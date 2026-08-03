import httpx
import pytest

from notable.cache import Cache
from notable.config import TransportConfig
from notable.errors import ProviderFailure
from notable.http import Transport

CONFIG = TransportConfig(
    contact_url="https://example.com/c", per_host_min_interval_ms=0, initial_backoff_seconds=0
)


def _transport(tmp_path, handler):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return Transport(CONFIG, Cache(tmp_path), client=client, sleep=lambda _s: None)


def test_successful_response_is_returned_and_cached(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="hello")

    transport = _transport(tmp_path, handler)
    first = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    second = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert first.text == second.text == "hello"
    assert len(calls) == 1, "the second call must be served from cache"


def test_sends_fixed_headers_and_no_cookies(tmp_path):
    seen = []

    def handler(request):
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(
                200, text="ok", headers={"set-cookie": "session=abc; Path=/"}
            )
        return httpx.Response(200, text="ok")

    transport = _transport(tmp_path, handler)
    for _ in range(2):
        transport.request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None,
            bypass_cache=True,
        )
    assert "example.com/c" in seen[0].headers["user-agent"]
    assert seen[0].headers["accept-language"].startswith("en")
    assert "accept-encoding" in seen[0].headers
    assert "cookie" not in seen[1].headers


def test_server_errors_are_retried_then_fail(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503, text="down")

    with pytest.raises(ProviderFailure) as info:
        _transport(tmp_path, handler).request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    assert info.value.permanent is False
    assert len(calls) == CONFIG.max_attempts


def test_a_retry_that_succeeds_returns_the_success(tmp_path):
    responses = [httpx.Response(503, text="down"), httpx.Response(200, text="good")]

    def handler(request):
        return responses.pop(0)

    result = _transport(tmp_path, handler).request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert result.text == "good"


def test_client_errors_are_permanent_and_not_retried(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(404, text="nope")

    with pytest.raises(ProviderFailure) as info:
        _transport(tmp_path, handler).request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    assert info.value.permanent is True
    assert len(calls) == 1


def test_failures_are_never_cached(tmp_path):
    # A transport failure followed by a success must return the success.
    responses = [httpx.Response(500, text="x")] * CONFIG.max_attempts + [
        httpx.Response(200, text="recovered")
    ]

    def handler(request):
        return responses.pop(0)

    transport = _transport(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        transport.request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    result = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert result.text == "recovered"


def test_contact_url_is_part_of_the_request_profile(tmp_path):
    # User-Agent is sent on the wire and can affect a provider response, so it
    # must participate in the cache discriminator even though it is not secret.
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="ok")

    first = _transport(tmp_path, handler)
    first.request(provider="t", method="GET", url="https://a.test/x", ttl_seconds=None)
    second_client = httpx.Client(transport=httpx.MockTransport(handler))
    second = Transport(
        TransportConfig(contact_url="https://other.test/c", per_host_min_interval_ms=0),
        Cache(tmp_path),
        client=second_client,
        sleep=lambda _s: None,
    )
    second.request(provider="t", method="GET", url="https://a.test/x", ttl_seconds=None)
    assert len(calls) == 2


def test_a_cache_hit_is_flagged_so_callers_can_skip_charging_for_it(tmp_path):
    transport = _transport(tmp_path, lambda r: httpx.Response(200, text="ok"))
    kwargs = {"provider": "t", "method": "GET", "url": "https://a.test/x", "ttl_seconds": None}
    assert transport.request(**kwargs).from_cache is False
    assert transport.request(**kwargs).from_cache is True


def test_a_deferred_response_is_not_cached_until_commit(tmp_path):
    # The spec's "only validated successes are cached" rule. Validation lives
    # downstream, so the transport must not write on arrival.
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="hello")

    transport = _transport(tmp_path, handler)
    kwargs = {
        "provider": "t", "method": "GET", "url": "https://a.test/x",
        "ttl_seconds": None, "defer_cache": True,
    }
    transport.request(**kwargs)
    transport.request(**kwargs)
    assert len(calls) == 2, "an uncommitted response must not be served from cache"

    transport.request(**kwargs).commit()
    assert transport.request(**kwargs).from_cache is True
    assert len(calls) == 3


def test_committing_twice_is_harmless(tmp_path):
    transport = _transport(tmp_path, lambda r: httpx.Response(200, text="ok"))
    response = transport.request(
        provider="t", method="GET", url="https://a.test/x",
        ttl_seconds=None, defer_cache=True,
    )
    response.commit()
    response.commit()  # must not raise


def test_committing_a_cache_hit_is_a_no_op(tmp_path):
    transport = _transport(tmp_path, lambda r: httpx.Response(200, text="ok"))
    kwargs = {"provider": "t", "method": "GET", "url": "https://a.test/x", "ttl_seconds": None}
    transport.request(**kwargs)
    hit = transport.request(**kwargs)
    assert hit.from_cache is True
    hit.commit()  # must not raise or rewrite


def test_bypass_skips_the_read_but_still_stores(tmp_path):
    # What --fresh-feeds needs. ttl_seconds=None means "never expires", so it
    # cannot express this: it would make the entry more permanent, not less.
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text=f"body {len(calls)}")

    transport = _transport(tmp_path, handler)
    kwargs = {"provider": "t", "method": "GET", "url": "https://a.test/x", "ttl_seconds": 3600}
    assert transport.request(**kwargs).text == "body 1"
    assert transport.request(**kwargs).text == "body 1"      # cached
    assert transport.request(**kwargs | {"bypass_cache": True}).text == "body 2"
    assert transport.request(**kwargs).text == "body 2", "the bypass refreshed the entry"
    assert len(calls) == 2


def test_recovered_rate_limits_are_counted(tmp_path):
    # A 429 the retry loop survives raises nothing and logs nothing, so it is
    # invisible to the live gate unless it is counted here.
    responses = [httpx.Response(429, text="slow down"), httpx.Response(200, text="ok")]

    transport = _transport(tmp_path, lambda r: responses.pop(0))
    transport.request(provider="t", method="GET", url="https://a.test/x", ttl_seconds=None)
    assert transport.stats.rate_limited == 1
    assert transport.stats.retries == 1


def test_pacing_sleeps_between_calls_to_one_host(tmp_path):
    slept = []
    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text="ok"))
    )
    config = TransportConfig(contact_url="https://e.test/c", per_host_min_interval_ms=900)
    transport = Transport(config, Cache(tmp_path), client=client, sleep=slept.append)
    for path in ("a", "b"):
        transport.request(
            provider="t", method="GET", url=f"https://a.test/{path}", ttl_seconds=None
        )
    assert slept and slept[-1] > 0
