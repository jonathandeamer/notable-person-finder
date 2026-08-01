"""Offline contract tests for the Brave Web Search adapter.

HTTP is served by a real ``HttpTransport`` over ``httpx.MockTransport``, so
status handling, pacing provider key, response limits, auth header presence,
and Retry-After are exercised rather than assumed. Fixtures under ``fixtures/``
are representative Brave Web Search JSON shapes.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal

import httpx
import pytest

from notable_person_finder.config.models import BraveConfig, TransportConfig
from notable_person_finder.providers.brave import (
    OPERATION_SEARCH_WEB,
    PROVIDER,
    HttpxBraveWebSearchClient,
    SearchPage,
    SearchResult,
    WebSearchClient,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
    build_transport,
)
from notable_person_finder.runs.clock import FakeClock

FIXTURES = Path(__file__).parent / "fixtures"

ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
API_KEY = "test-brave-secret-token"
RESOLVER = StaticHostResolver({"api.search.brave.com": ("104.18.0.1",)})


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
    brave: BraveConfig | None = None,
    api_key: str = API_KEY,
    transport_config: TransportConfig | None = None,
) -> HttpxBraveWebSearchClient:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(
            status,
            headers=dict(headers or {}),
            content=streaming_body(payload),
        )

    return HttpxBraveWebSearchClient(
        transport_for(handler, config=transport_config),
        config=brave or BraveConfig(),
        api_key=api_key,
        clock=FakeClock(),
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
        destination_host="api.search.brave.com",
        status_code=status,
        headers=dict(headers or {}),
        content=payload,
        encoded_bytes=len(payload),
        decoded_bytes=len(payload),
    )


# --- Protocol surface --------------------------------------------------------


def test_httpx_client_satisfies_web_search_client_protocol() -> None:
    client: WebSearchClient = client_for(fixture_bytes("brave_search_page.json"))
    assert callable(client.search_web)


# --- search_web happy path ---------------------------------------------------


def test_search_web_returns_ordered_results_with_ranks_and_snippets() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("brave_search_page.json"), seen=seen)

    result = client.search_web("Ada Lovelace", count=10, offset=0)

    assert isinstance(result, SearchPage)
    assert result.query == "Ada Lovelace"
    assert result.altered_query is None
    assert result.offset == 0
    assert result.count == 10
    assert result.more_results is True
    assert len(result.results) == 2

    first = result.results[0]
    assert isinstance(first, SearchResult)
    assert first.rank == 1
    assert first.url == "https://en.wikipedia.org/wiki/Ada_Lovelace"
    assert first.title == "Ada Lovelace - Wikipedia"
    assert first.snippet is not None
    assert "mathematician" in first.snippet
    assert first.language == "en"
    assert first.provider_result_id == "brave-result-ada-lovelace-1"
    assert len(first.extra_snippets) == 2
    assert "first computer programmer" in first.extra_snippets[0]

    second = result.results[1]
    assert second.rank == 2
    assert second.url == "https://findingada.com/"
    assert second.extra_snippets == ()
    assert second.provider_result_id is None

    assert len(seen) == 1
    request = seen[0]
    assert request.method == "GET"
    assert request.url.host == "api.search.brave.com"
    assert request.url.path == "/res/v1/web/search"
    params = dict(request.url.params)
    assert params["q"] == "Ada Lovelace"
    assert params["count"] == "10"
    assert params["offset"] == "0"
    assert params["search_lang"] == "en"
    assert params["safesearch"] == "moderate"
    assert params["spellcheck"] == "false"
    assert params["country"] == "ALL"
    assert params["result_filter"] == "web"
    # I9: paid-plan only, so the default must not send it at all.
    assert "extra_snippets" not in params
    assert request.headers.get("X-Subscription-Token") == API_KEY


def test_search_web_empty_complete_page_is_returned() -> None:
    result = client_for(fixture_bytes("brave_search_empty.json")).search_web(
        "zzznobody12345", count=10, offset=0
    )

    assert result.results == ()
    assert result.more_results is False
    assert result.altered_query is None
    assert result.query == "zzznobody12345"


def test_search_web_captures_provider_altered_query() -> None:
    result = client_for(fixture_bytes("brave_search_altered.json")).search_web(
        "Ada Lovalace", count=5, offset=0
    )

    assert result.query == "Ada Lovalace"
    assert result.altered_query == "Ada Lovelace"
    assert result.more_results is False
    assert len(result.results) == 1
    assert result.results[0].rank == 1


def test_search_web_sends_pagination_offset() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("brave_search_empty.json"), seen=seen)

    page = client.search_web("Ada", count=10, offset=2)

    params = dict(seen[0].url.params)
    assert params["offset"] == "2"
    assert params["count"] == "10"
    assert page.offset == 2
    assert page.count == 10


def test_search_web_exactly_one_http_call() -> None:
    transport = RecordingTransport(
        response=http_response(fixture_bytes("brave_search_page.json"))
    )
    client = HttpxBraveWebSearchClient(
        transport,
        config=BraveConfig(),
        api_key=API_KEY,
        clock=FakeClock(),
    )

    client.search_web("Ada", count=10, offset=0)

    assert len(transport.calls) == 1
    call = transport.calls[0]
    assert call["provider"] == PROVIDER
    assert call["operation"] == OPERATION_SEARCH_WEB
    assert call["limit"] == ResponseLimit.API
    assert call["method"] == "GET"
    assert call["url"] == ENDPOINT
    headers = call["headers"]
    assert isinstance(headers, dict)
    assert headers.get("X-Subscription-Token") == API_KEY


def test_search_web_uses_configured_endpoint() -> None:
    # Distinct from the default so hardcoding ENDPOINT would fail this test.
    custom = "https://api.search.brave.com/res/v1/web/search-custom"
    assert custom != ENDPOINT
    transport = RecordingTransport(
        response=http_response(fixture_bytes("brave_search_empty.json"))
    )
    client = HttpxBraveWebSearchClient(
        transport,
        config=BraveConfig(endpoint=custom),
        api_key=API_KEY,
        clock=FakeClock(),
    )

    client.search_web("x", count=1, offset=0)

    assert len(transport.calls) == 1
    assert transport.calls[0]["url"] == custom


# --- Failures ----------------------------------------------------------------


def test_http_401_is_authentication_without_secret_in_detail() -> None:
    toxic_body = b'{"error":{"id":"x","status":401,"detail":"LEAK_ME_BODY"},"token":"'
    toxic_body += API_KEY.encode() + b'"}'
    client = client_for(toxic_body, status=401)

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.AUTHENTICATION
    assert failure.status_code == 401
    assert failure.provider == PROVIDER
    assert failure.operation == OPERATION_SEARCH_WEB
    assert API_KEY not in str(failure)
    assert API_KEY not in (failure.detail or "")
    assert "LEAK_ME_BODY" not in str(failure)
    assert "LEAK_ME_BODY" not in (failure.detail or "")


def test_http_429_is_rate_limit_with_retry_after() -> None:
    client = client_for(
        b'{"error":"slow down SECRET_RATE"}',
        status=429,
        headers={"Retry-After": "3"},
    )

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.RATE_LIMIT
    assert failure.status_code == 429
    assert failure.retryable is True
    assert failure.retry_after_ms == 3000
    assert "SECRET_RATE" not in str(failure)
    assert "SECRET_RATE" not in (failure.detail or "")
    assert API_KEY not in str(failure)


def test_http_503_is_provider_unavailable() -> None:
    client = client_for(
        b"unavailable body LEAK",
        status=503,
        headers={"Retry-After": "1"},
    )

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.PROVIDER_UNAVAILABLE
    assert failure.status_code == 503
    assert "LEAK" not in str(failure)
    assert "LEAK" not in (failure.detail or "")


def test_http_500_is_transient_server_error() -> None:
    client = client_for(b"server boom LEAK_500", status=500)

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.TRANSIENT_SERVER_ERROR
    assert failure.status_code == 500
    assert "LEAK_500" not in str(failure)


def test_malformed_json_is_malformed_response_without_body_in_detail() -> None:
    toxic = b'{"not": "closed", "body": "LEAK_ME_QUERY=secret", "token":"'
    toxic += API_KEY.encode() + b'"'
    client = client_for(toxic)

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.MALFORMED_RESPONSE
    assert failure.detail in {"JSONDecodeError", "malformed_json"}
    assert "LEAK_ME" not in str(failure)
    assert "LEAK_ME" not in (failure.detail or "")
    assert API_KEY not in str(failure)
    assert API_KEY not in (failure.detail or "")


def test_missing_web_object_is_malformed_response() -> None:
    client = client_for(b'{"type":"search","query":{"original":"x"}}')

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("x", count=10, offset=0)

    failure = caught.value
    assert failure.category == FailureCategory.MALFORMED_RESPONSE
    assert failure.detail == "missing_web"
    assert API_KEY not in str(failure)


def test_response_too_large_path_propagates_from_transport() -> None:
    transport = RecordingTransport(
        error=ProviderFailure(
            FailureCategory.RESPONSE_TOO_LARGE,
            provider=PROVIDER,
            operation=OPERATION_SEARCH_WEB,
            detail="decoded body exceeded 10 bytes",
        )
    )
    client = HttpxBraveWebSearchClient(
        transport,
        config=BraveConfig(),
        api_key=API_KEY,
        clock=FakeClock(),
    )

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("Ada", count=10, offset=0)

    assert caught.value.category == FailureCategory.RESPONSE_TOO_LARGE
    assert len(transport.calls) == 1


def test_auth_header_uses_api_key_and_never_puts_key_in_failure_detail() -> None:
    transport = RecordingTransport(
        error=ProviderFailure(
            FailureCategory.AUTHENTICATION,
            provider=PROVIDER,
            operation=OPERATION_SEARCH_WEB,
            status_code=401,
        )
    )
    secret = "super-secret-brave-key-xyz"
    client = HttpxBraveWebSearchClient(
        transport,
        config=BraveConfig(),
        api_key=secret,
        clock=FakeClock(),
    )

    with pytest.raises(ProviderFailure) as caught:
        client.search_web("q", count=1, offset=0)

    headers = transport.calls[0]["headers"]
    assert isinstance(headers, dict)
    assert headers["X-Subscription-Token"] == secret
    assert secret not in str(caught.value)
    assert secret not in (caught.value.detail or "")


def test_extra_snippets_is_opt_in_and_off_by_default() -> None:
    """I9: Brave rejects `extra_snippets` on the free and base tiers.

    The 4xx classifies as CONFIGURATION -- permanent -- so sending it
    unconditionally makes every `brave_web_search` fail on a first live run
    with a free-tier key.
    """
    assert BraveConfig().extra_snippets is False

    off_seen: list[httpx.Request] = []
    client_for(fixture_bytes("brave_search_page.json"), seen=off_seen).search_web(
        "Ada Lovelace", count=10, offset=0
    )
    assert "extra_snippets" not in dict(off_seen[0].url.params)

    # Positive control: the parameter is reachable when an operator opts in.
    on_seen: list[httpx.Request] = []
    client_for(
        fixture_bytes("brave_search_page.json"),
        seen=on_seen,
        brave=BraveConfig(extra_snippets=True),
    ).search_web("Ada Lovelace", count=10, offset=0)
    assert dict(on_seen[0].url.params)["extra_snippets"] == "true"
