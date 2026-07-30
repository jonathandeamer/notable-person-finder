"""Offline contract tests for the MediaWiki Action API adapter.

HTTP is served by a real ``HttpTransport`` over ``httpx.MockTransport``, so
status handling, pacing provider key, response limits, and Retry-After are
exercised rather than assumed. Fixtures under ``fixtures/`` are representative
Action API JSON shapes, not live captures of every field.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal

import httpx
import pytest

from notable_person_finder.config.models import MediaWikiConfig, TransportConfig
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.mediawiki import (
    HttpxMediaWikiClient,
    MediaWikiClient,
    MediaWikiSearchHit,
    MediaWikiSearchPage,
)
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
    build_transport,
)
from notable_person_finder.runs.clock import FakeClock

FIXTURES = Path(__file__).parent / "fixtures"

ENDPOINT = "https://en.wikipedia.org/w/api.php"
RESOLVER = StaticHostResolver({"en.wikipedia.org": ("208.80.154.224",)})


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def fixture_json(name: str) -> dict[str, Any]:
    return json.loads(fixture_bytes(name))


def streaming_body(payload: bytes) -> list[bytes]:
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def transport_for(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    config: TransportConfig | None = None,
) -> HttpTransport:
    return build_transport(
        config or TransportConfig(),
        version="0.1.0",
        resolver=RESOLVER,
        clock=FakeClock(),
        http_transport=httpx.MockTransport(handler),
    )


def client_for(
    payload: bytes,
    *,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    seen: list[httpx.Request] | None = None,
    mediawiki: MediaWikiConfig | None = None,
    srlimit: int = 10,
    max_extract_characters: int = 1200,
    max_categories_per_page: int = 20,
    transport_config: TransportConfig | None = None,
) -> HttpxMediaWikiClient:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(
            status,
            headers=dict(headers or {}),
            content=streaming_body(payload),
        )

    return HttpxMediaWikiClient(
        transport_for(handler, config=transport_config),
        config=mediawiki or MediaWikiConfig(),
        srlimit=srlimit,
        max_extract_characters=max_extract_characters,
        max_categories_per_page=max_categories_per_page,
    )


class RecordingTransport(HttpTransport):
    """Stand-in at the ``request`` seam; does not construct a real client."""

    def __init__(
        self,
        *,
        response: HttpResponse | None = None,
        error: BaseException | None = None,
    ) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        provider: str,
        operation: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str | int] | None = None,
        limit: ResponseLimit = ResponseLimit.API,
        profile: Literal["ordinary", "llm"] = "ordinary",
    ) -> HttpResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "provider": provider,
                "operation": operation,
                "headers": dict(headers) if headers is not None else None,
                "params": dict(params) if params is not None else None,
                "limit": limit,
                "profile": profile,
            }
        )
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


def http_response(
    payload: bytes,
    *,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
) -> HttpResponse:
    return HttpResponse(
        requested_url=ENDPOINT,
        final_url=ENDPOINT,
        redirect_chain=(),
        destination_host="en.wikipedia.org",
        status_code=status,
        headers=dict(headers or {}),
        content=payload,
        encoded_bytes=len(payload),
        decoded_bytes=len(payload),
    )


# --- Protocol surface --------------------------------------------------------


def test_httpx_client_satisfies_mediawiki_client_protocol() -> None:
    client: MediaWikiClient = client_for(fixture_bytes("mediawiki_search_page.json"))
    assert callable(client.search_pages)
    assert callable(client.get_page_facts)


# --- search_pages ------------------------------------------------------------


def test_search_pages_returns_ordered_hits_and_continuation() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("mediawiki_search_page.json"), seen=seen)

    result = client.search_pages("Ada Lovelace", continuation=None)

    assert isinstance(result, MediaWikiSearchPage)
    assert result.more_results is True
    assert result.continuation == "2"
    assert result.provider_total_hits == 42
    assert len(result.hits) == 2
    first = result.hits[0]
    assert first.page_id == 101
    assert first.title == "Ada Lovelace"
    assert first.snippet is not None
    assert "Ada" in first.snippet
    assert first.timestamp == "2026-01-15T12:00:00Z"
    assert isinstance(first, MediaWikiSearchHit)
    assert result.hits[1].page_id == 102
    assert result.hits[1].title == "Ada (programming language)"

    assert len(seen) == 1
    params = dict(seen[0].url.params)
    assert params["action"] == "query"
    assert params["list"] == "search"
    assert params["srsearch"] == "Ada Lovelace"
    assert params["srlimit"] == "10"
    assert params["maxlag"] == "5"
    assert params["format"] == "json"
    assert "sroffset" not in params
    assert seen[0].url.host == "en.wikipedia.org"
    assert seen[0].url.path == "/w/api.php"


def test_search_pages_sends_continuation_as_sroffset() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("mediawiki_search_empty.json"), seen=seen)

    client.search_pages("Ada", continuation="10")

    params = dict(seen[0].url.params)
    assert params["sroffset"] == "10"
    assert params["srsearch"] == "Ada"


def test_search_pages_empty_complete_search_is_returned_not_filtered() -> None:
    result = client_for(fixture_bytes("mediawiki_search_empty.json")).search_pages(
        "zzznobody", continuation=None
    )

    assert result.hits == ()
    assert result.more_results is False
    assert result.continuation is None
    assert result.provider_total_hits == 0


def test_search_pages_uses_configured_srlimit_and_maxlag() -> None:
    seen: list[httpx.Request] = []
    client = client_for(
        fixture_bytes("mediawiki_search_empty.json"),
        seen=seen,
        mediawiki=MediaWikiConfig(maxlag_seconds=7),
        srlimit=3,
    )

    client.search_pages("x", continuation=None)

    params = dict(seen[0].url.params)
    assert params["srlimit"] == "3"
    assert params["maxlag"] == "7"


def test_search_pages_exactly_one_http_call() -> None:
    transport = RecordingTransport(
        response=http_response(fixture_bytes("mediawiki_search_page.json"))
    )
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    client.search_pages("Ada", continuation=None)

    assert len(transport.calls) == 1
    assert transport.calls[0]["provider"] == "mediawiki"
    assert transport.calls[0]["operation"] == "search_pages"
    assert transport.calls[0]["limit"] == ResponseLimit.API
    assert transport.calls[0]["method"] == "GET"


# --- get_page_facts ----------------------------------------------------------


def test_get_page_facts_surfaces_redirects_namespaces_dabs_and_missing() -> None:
    seen: list[httpx.Request] = []
    page_ids = (31873, 534366, 2860127, 4, 999999999)
    client = client_for(fixture_bytes("mediawiki_page_facts.json"), seen=seen)

    batch = client.get_page_facts(page_ids)

    assert len(seen) == 1
    params = dict(seen[0].url.params)
    assert params["action"] == "query"
    assert params["pageids"] == "31873|534366|2860127|4|999999999"
    assert params["maxlag"] == "5"
    assert "info" in params["prop"]
    assert "pageprops" in params["prop"]
    assert "redirects" in params

    by_id = {page.page_id: page for page in batch.pages}

    redirect_source = by_id[31873]
    assert redirect_source.redirect_to_page_id == 3434750
    assert redirect_source.redirect_to_title == "United States"
    assert redirect_source.missing is False
    assert redirect_source.canonical_title == "USA"

    terminal = by_id[3434750]
    assert terminal.redirect_to_page_id is None
    assert terminal.canonical_title == "United States"
    assert terminal.namespace == 0
    assert terminal.description == "Country primarily in North America"
    assert terminal.extract is not None
    assert "United States" in terminal.extract
    assert "Category:United States" in terminal.categories

    biography = by_id[534366]
    assert biography.is_disambiguation is False
    assert biography.namespace == 0

    dab = by_id[2860127]
    assert dab.is_disambiguation is True
    assert dab.canonical_title == "USA (disambiguation)"

    project = by_id[4]
    assert project.namespace == 4
    assert project.is_disambiguation is False

    missing = by_id[999999999]
    assert missing.missing is True
    assert missing.page_id == 999999999


def test_get_page_facts_does_not_drop_non_main_or_dab_pages() -> None:
    """Adapter is a translator: namespace and dab filtering belong to domain."""
    batch = client_for(fixture_bytes("mediawiki_page_facts.json")).get_page_facts(
        (2860127, 4)
    )
    ids = {page.page_id for page in batch.pages}
    assert 2860127 in ids
    assert 4 in ids


def test_get_page_facts_bounds_extract_and_categories() -> None:
    client = client_for(
        fixture_bytes("mediawiki_page_facts.json"),
        max_extract_characters=12,
        max_categories_per_page=1,
    )
    batch = client.get_page_facts((534366,))
    page = next(p for p in batch.pages if p.page_id == 534366)
    assert page.extract is not None
    assert len(page.extract) <= 12
    assert len(page.categories) <= 1


def test_get_page_facts_exactly_one_http_call() -> None:
    transport = RecordingTransport(
        response=http_response(fixture_bytes("mediawiki_page_facts.json"))
    )
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    client.get_page_facts((1, 2, 3))

    assert len(transport.calls) == 1
    assert transport.calls[0]["provider"] == "mediawiki"
    assert transport.calls[0]["operation"] == "get_page_facts"
    assert transport.calls[0]["limit"] == ResponseLimit.API


def test_get_page_facts_rejects_empty_page_id_list_without_http() -> None:
    transport = RecordingTransport(
        response=http_response(fixture_bytes("mediawiki_page_facts.json"))
    )
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    with pytest.raises(ProviderFailure) as caught:
        client.get_page_facts(())

    assert caught.value.category == FailureCategory.CONFIGURATION
    assert transport.calls == []


# --- Failures ----------------------------------------------------------------


def test_maxlag_json_error_is_rate_limit_with_retry_after() -> None:
    client = client_for(
        fixture_bytes("mediawiki_maxlag_error.json"),
        headers={"Retry-After": "5"},
    )

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("Ada", continuation=None)

    assert caught.value.category == FailureCategory.RATE_LIMIT
    assert caught.value.retryable is True
    assert caught.value.provider == "mediawiki"
    assert caught.value.operation == "search_pages"
    assert caught.value.retry_after_ms == 5000
    assert caught.value.detail == "maxlag"
    assert "SECRET_BODY" not in str(caught.value)
    assert "SECRET_BODY" not in (caught.value.detail or "")


def test_rate_limit_json_error_does_not_leak_body_into_detail() -> None:
    client = client_for(fixture_bytes("mediawiki_rate_limit_error.json"))

    with pytest.raises(ProviderFailure) as caught:
        client.get_page_facts((1,))

    assert caught.value.category == FailureCategory.RATE_LIMIT
    assert caught.value.detail == "ratelimited"
    assert "sk-secret-value" not in str(caught.value)
    assert "sk-secret-value" not in (caught.value.detail or "")


def test_http_429_is_rate_limit() -> None:
    client = client_for(b"too many", status=429, headers={"Retry-After": "2"})

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("x", continuation=None)

    assert caught.value.category == FailureCategory.RATE_LIMIT
    assert caught.value.status_code == 429
    assert caught.value.retry_after_ms == 2000


def test_http_503_is_provider_unavailable() -> None:
    client = client_for(b"unavailable", status=503, headers={"Retry-After": "1"})

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("x", continuation=None)

    assert caught.value.category == FailureCategory.PROVIDER_UNAVAILABLE
    assert caught.value.status_code == 503


def test_malformed_json_is_malformed_response_without_body_in_detail() -> None:
    # Deliberately invalid JSON whose text includes a fake secret and query.
    toxic = b'{"not": "closed", "body": "LEAK_ME_QUERY=secret"'
    client = client_for(toxic)

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("Ada", continuation=None)

    assert caught.value.category == FailureCategory.MALFORMED_RESPONSE
    assert caught.value.detail in {"JSONDecodeError", "malformed_json"}
    assert "LEAK_ME" not in str(caught.value)
    assert "LEAK_ME" not in (caught.value.detail or "")


def test_missing_query_object_is_malformed_response() -> None:
    client = client_for(b'{"batchcomplete": true}')

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("Ada", continuation=None)

    assert caught.value.category == FailureCategory.MALFORMED_RESPONSE
    assert caught.value.detail == "missing_query"


def test_response_too_large_path_propagates_from_transport() -> None:
    transport = RecordingTransport(
        error=ProviderFailure(
            FailureCategory.RESPONSE_TOO_LARGE,
            provider="mediawiki",
            operation="search_pages",
            detail="decoded body exceeded 10 bytes",
        )
    )
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("Ada", continuation=None)

    assert caught.value.category == FailureCategory.RESPONSE_TOO_LARGE


def test_forced_error_with_body_does_not_appear_in_detail() -> None:
    """Positive control: a body-bearing fault is reduced to a type name."""

    class BodyBearingFault(Exception):
        def __str__(self) -> str:
            return "remote said: SECRET_RESPONSE_BODY and query=Ada+Lovelace"

    def handler(request: httpx.Request) -> httpx.Response:
        raise BodyBearingFault()

    # Build a transport whose send path raises; easiest: RecordingTransport.
    transport = RecordingTransport(error=BodyBearingFault())
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("Ada Lovelace", continuation=None)

    assert caught.value.category == FailureCategory.INTERNAL
    assert caught.value.detail == "BodyBearingFault"
    assert "SECRET_RESPONSE_BODY" not in str(caught.value)
    assert "Ada+Lovelace" not in str(caught.value)
    assert "SECRET_RESPONSE_BODY" not in (caught.value.detail or "")


def test_provider_failure_from_transport_is_not_rewrapped() -> None:
    original = ProviderFailure(
        FailureCategory.TIMEOUT,
        provider="mediawiki",
        operation="search_pages",
        detail="ReadTimeout",
    )
    transport = RecordingTransport(error=original)
    client = HttpxMediaWikiClient(transport, config=MediaWikiConfig())

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("x", continuation=None)

    assert caught.value is original


def test_search_does_not_put_query_text_in_failure_detail_on_api_error() -> None:
    body = json.dumps(
        {
            "error": {
                "code": "badvalue",
                "info": "Unrecognized value for parameter srsearch: supersecretquery",
            }
        }
    ).encode()
    client = client_for(body)

    with pytest.raises(ProviderFailure) as caught:
        client.search_pages("supersecretquery", continuation=None)

    assert caught.value.detail == "badvalue"
    assert "supersecretquery" not in (caught.value.detail or "")
    # str(ProviderFailure) interpolates detail only, not the original query.
    assert "supersecretquery" not in str(caught.value)


# --- Config wiring smoke for client ------------------------------------------


def test_client_uses_configured_endpoint_host() -> None:
    seen: list[httpx.Request] = []
    resolver = StaticHostResolver({"wiki.example": ("93.184.216.34",)})

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, content=streaming_body(fixture_bytes("mediawiki_search_empty.json"))
        )

    transport = build_transport(
        TransportConfig(),
        version="0.1.0",
        resolver=resolver,
        clock=FakeClock(),
        http_transport=httpx.MockTransport(handler),
    )
    client = HttpxMediaWikiClient(
        transport,
        config=MediaWikiConfig(endpoint="https://wiki.example/w/api.php"),
    )

    client.search_pages("x", continuation=None)

    assert seen[0].url.host == "wiki.example"
    assert seen[0].url.path == "/w/api.php"
