from __future__ import annotations

import gzip
from collections.abc import Iterable

import httpx
import pytest

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import (
    HttpTransport,
    ResponseLimit,
    build_transport,
)
from notable_person_finder.runs.clock import FakeClock

RESOLVER = StaticHostResolver(
    {
        "example.com": ("93.184.216.34",),
        "other.example": ("93.184.216.35",),
        "internal.example": ("127.0.0.1",),
    }
)


def streaming_body(payload: bytes, *, parts: int = 3) -> Iterable[bytes]:
    """Split `payload` into multiple chunks for a MockTransport handler.

    `httpx.Response(status, content=<bytes>)` eagerly reads and closes its
    own stream during construction, which makes `iter_raw()` in the
    transport under test raise `StreamConsumed` before we ever see it.
    Passing a non-bytes *iterable* of chunks instead makes httpx treat the
    content as a genuine stream (`IteratorByteStream`), so `iter_raw()`
    yields the raw chunks the way it would for a real network response,
    across more than one iteration.
    """
    if not payload:
        return [payload]
    size = max(1, -(-len(payload) // max(parts, 1)))
    return [payload[start : start + size] for start in range(0, len(payload), size)]


def transport_for(handler, config: TransportConfig | None = None) -> HttpTransport:
    """Build a transport whose network layer is an in-process handler."""
    return build_transport(
        config or TransportConfig(),
        version="0.1.0",
        resolver=RESOLVER,
        clock=FakeClock(),
        http_transport=httpx.MockTransport(handler),
    )


def test_successful_request_returns_decoded_body_and_final_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=streaming_body(b"hello"),
            headers={"content-type": "text/plain"},
        )

    with transport_for(handler) as transport:
        response = transport.request(
            "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
        )
    assert response.status_code == 200
    assert response.content == b"hello"
    assert response.requested_url == "https://example.com/a"
    assert response.final_url == "https://example.com/a"
    assert response.redirect_chain == ()
    assert response.destination_host == "example.com"


def test_user_agent_is_sent_on_every_request() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["user-agent"])
        return httpx.Response(200, content=streaming_body(b"ok"))

    with transport_for(handler) as transport:
        transport.request("GET", "https://example.com/a", provider="feeds", operation="fetch_feed")
    assert seen == ["notable-person-finder/0.1.0 (+https://github.com/jonathandeamer/notable-person-finder)"]


def test_adapter_headers_are_merged_without_replacing_the_user_agent() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, content=streaming_body(b"ok"))

    with transport_for(handler) as transport:
        transport.request(
            "GET",
            "https://example.com/a",
            provider="brave",
            operation="search_web",
            headers={
                "X-Subscription-Token": "secret",
                "User-Agent": "untrusted-override",
                "Accept-Encoding": "br",
            },
        )
    assert seen["x-subscription-token"] == "secret"
    assert seen["user-agent"].startswith("notable-person-finder/")
    assert seen["accept-encoding"] == "gzip, deflate"


def test_redirects_are_followed_and_the_chain_is_recorded() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/a":
            return httpx.Response(301, headers={"location": "https://other.example/b"})
        return httpx.Response(200, content=streaming_body(b"final"))

    with transport_for(handler) as transport:
        response = transport.request(
            "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
        )
    assert response.redirect_chain == ("https://example.com/a",)
    assert response.final_url == "https://other.example/b"
    assert response.destination_host == "other.example"
    assert response.content == b"final"


def test_cross_origin_redirect_drops_credential_headers() -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.headers))
        if request.url.host == "example.com":
            return httpx.Response(302, headers={"location": "https://other.example/b"})
        return httpx.Response(200, content=streaming_body(b"final"))

    with transport_for(handler) as transport:
        transport.request(
            "GET",
            "https://example.com/a",
            provider="feeds",
            operation="fetch_feed",
            headers={
                "Authorization": "Bearer SECRET",
                "X-Subscription-Token": "TOKEN",
            },
        )
    assert len(seen) == 2
    assert seen[0]["authorization"] == "Bearer SECRET"
    assert seen[0]["x-subscription-token"] == "TOKEN"
    assert "authorization" not in seen[1]
    assert "x-subscription-token" not in seen[1]


def test_same_origin_redirect_keeps_credential_headers() -> None:
    seen: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.headers))
        if request.url.path == "/a":
            return httpx.Response(302, headers={"location": "https://example.com/b"})
        return httpx.Response(200, content=streaming_body(b"final"))

    with transport_for(handler) as transport:
        transport.request(
            "GET",
            "https://example.com/a",
            provider="feeds",
            operation="fetch_feed",
            headers={
                "Authorization": "Bearer SECRET",
                "X-Subscription-Token": "TOKEN",
            },
        )
    assert len(seen) == 2
    assert seen[0]["authorization"] == "Bearer SECRET"
    assert seen[1]["authorization"] == "Bearer SECRET"
    assert seen[1]["x-subscription-token"] == "TOKEN"


def test_redirect_to_a_private_address_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://internal.example/secrets"})

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is FailureCategory.CONFIGURATION
    assert "internal.example" not in str(raised.value)


def test_redirect_limit_is_enforced() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://example.com/loop"})

    with transport_for(handler, TransportConfig(max_redirects=2)) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is FailureCategory.MALFORMED_RESPONSE
    assert not raised.value.retryable


def test_decoded_body_over_the_limit_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=streaming_body(b"x" * 5000))

    config = TransportConfig(max_api_response_bytes=1000)
    with transport_for(handler, config) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET",
                "https://example.com/a",
                provider="feeds",
                operation="fetch_feed",
                limit=ResponseLimit.API,
            )
    assert raised.value.category is FailureCategory.RESPONSE_TOO_LARGE


def test_compression_cannot_smuggle_a_body_past_the_decoded_limit() -> None:
    payload = gzip.compress(b"x" * 200_000)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=streaming_body(payload, parts=3),
            headers={"content-encoding": "gzip"},
        )

    config = TransportConfig(max_api_response_bytes=1000)
    with transport_for(handler, config) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET",
                "https://example.com/a",
                provider="feeds",
                operation="fetch_feed",
                limit=ResponseLimit.API,
            )
    assert raised.value.category is FailureCategory.RESPONSE_TOO_LARGE


def test_article_limit_is_larger_than_the_api_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=streaming_body(b"x" * 4000))

    config = TransportConfig(max_api_response_bytes=1000, max_article_response_bytes=8000)
    with transport_for(handler, config) as transport:
        response = transport.request(
            "GET",
            "https://example.com/a",
            provider="articles",
            operation="fetch_article",
            limit=ResponseLimit.ARTICLE,
        )
    assert response.decoded_bytes == 4000


@pytest.mark.parametrize(
    ("status", "category", "retryable"),
    [
        (401, FailureCategory.AUTHENTICATION, False),
        (403, FailureCategory.ACCESS_DENIED, False),
        (404, FailureCategory.ACCESS_DENIED, False),
        (408, FailureCategory.TIMEOUT, True),
        (429, FailureCategory.RATE_LIMIT, True),
        (500, FailureCategory.TRANSIENT_SERVER_ERROR, True),
        (503, FailureCategory.PROVIDER_UNAVAILABLE, True),
        (400, FailureCategory.CONFIGURATION, False),
    ],
)
def test_http_statuses_map_to_typed_failures(
    status: int, category: FailureCategory, retryable: bool
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="body text")

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="brave", operation="search_web"
            )
    assert raised.value.category is category
    assert raised.value.retryable is retryable
    assert raised.value.status_code == status
    assert "body text" not in str(raised.value)


def test_retry_after_is_captured_from_a_rate_limited_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "7"})

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="brave", operation="search_web"
            )
    assert raised.value.retry_after_ms == 7000


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (httpx.ConnectTimeout("slow"), FailureCategory.TIMEOUT),
        (httpx.ReadTimeout("slow"), FailureCategory.TIMEOUT),
        (httpx.ConnectError("refused"), FailureCategory.NETWORK),
        (httpx.RemoteProtocolError("garbage"), FailureCategory.MALFORMED_RESPONSE),
    ],
)
def test_httpx_errors_are_translated(error: Exception, category: FailureCategory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is category


def test_no_httpx_exception_escapes_the_boundary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("proxy exploded")

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure):
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )


def test_llm_profile_uses_the_longer_read_timeout() -> None:
    config = TransportConfig(read_timeout_seconds=30.0, llm_read_timeout_seconds=300.0)
    with transport_for(lambda request: httpx.Response(200, text="ok"), config) as transport:
        assert transport._timeout_for(profile="llm").read == 300.0
        assert transport._timeout_for(profile="ordinary").read == 30.0
        assert transport._timeout_for(profile="ordinary").connect == 10.0


def test_client_level_automatic_retries_are_disabled() -> None:
    with transport_for(lambda request: httpx.Response(200, text="ok")) as transport:
        assert transport.automatic_retries_enabled() is False


class _BuildRequestRaisesUnicode(httpx.Client):
    def build_request(self, *args, **kwargs):
        raise UnicodeEncodeError(
            "ascii", "café", 0, 1, "ordinal not in range(128)"
        )


def test_unicode_encode_error_in_build_request_is_translated() -> None:
    """A non-ASCII header/value must not leak as a raw UnicodeEncodeError."""
    client = _BuildRequestRaisesUnicode(transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    transport = HttpTransport(
        client,
        config=TransportConfig(),
        user_agent="test",
        resolver=RESOLVER,
        clock=FakeClock(),
    )
    with pytest.raises(ProviderFailure) as captured:
        transport.request(
            "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
        )
    assert captured.value.category is FailureCategory.CONFIGURATION
    assert "non-ASCII" in captured.value.detail
