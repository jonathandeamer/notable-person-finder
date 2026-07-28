"""Tests for the feedparser-backed feed adapter.

Every test here is offline. Two fake transports are used, deliberately at
different depths:

* `transport_for` builds a *real* `HttpTransport` whose network layer is an
  `httpx.MockTransport`. That exercises the genuine status-code handling,
  redirect loop, and response-header lower-casing, so assertions about HTTP
  304 and about which request headers actually go out are evidence rather
  than assumption.
* `RecordingTransport` replaces `HttpTransport.request` outright. It is used
  only where the assertion is about the *call the adapter makes* (provider,
  operation, response limit) or where the transport has to raise something
  the real one never would.
"""

from __future__ import annotations

import io
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal

import feedparser
import httpx
import pytest

from notable_person_finder.config.models import FeedConfig, TransportConfig
from notable_person_finder.providers import feeds
from notable_person_finder.providers.failures import (
    RETRYABLE_CATEGORIES,
    FailureCategory,
    ProviderFailure,
)
from notable_person_finder.providers.feeds import (
    FeedClient,
    FeedparserClient,
    FeedValidators,
    Modified,
    NotModified,
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

RESOLVER = StaticHostResolver(
    {
        "example.com": ("93.184.216.34",),
        "other.example": ("93.184.216.35",),
    }
)

FEED = FeedConfig(
    key="culture", label="Example Culture Desk", url="https://example.com/feed.xml"
)

NO_VALIDATORS = FeedValidators(etag=None, last_modified=None)


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def streaming_body(payload: bytes) -> list[bytes]:
    """Hand `httpx.Response` a genuine stream rather than eager bytes.

    `httpx.Response(status, content=<bytes>)` reads and closes its own stream
    during construction, which makes the transport's `iter_raw()` raise
    `StreamConsumed`. A list of chunks is treated as a real stream instead.
    See `tests/run_engine/test_transport.py`, which does the same.
    """
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def transport_for(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    config: TransportConfig | None = None,
) -> HttpTransport:
    """A real transport whose only fake part is the network layer."""
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
) -> FeedparserClient:
    """A client whose single HTTP response serves `payload`."""

    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(
            status, headers=dict(headers or {}), content=streaming_body(payload)
        )

    return FeedparserClient(transport_for(handler))


class RecordingTransport(HttpTransport):
    """A transport that records one call and returns (or raises) on command.

    `HttpTransport.__init__` is deliberately not called: this stands in for
    the transport at the `request` seam only, and constructing the real one
    would require an `httpx.Client` that no test here wants. Nothing else on
    the base class is touched.
    """

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
                "params": params,
                "limit": limit,
                "profile": profile,
            }
        )
        if self.error is not None:
            raise self.error
        assert self.response is not None
        return self.response


def http_response(
    payload: bytes, *, status: int = 200, headers: Mapping[str, str] | None = None
) -> HttpResponse:
    return HttpResponse(
        requested_url=FEED.url,
        final_url=FEED.url,
        redirect_chain=(),
        destination_host="example.com",
        status_code=status,
        headers=dict(headers or {}),
        content=payload,
        encoded_bytes=len(payload),
        decoded_bytes=len(payload),
    )


# --------------------------------------------------------------------------
# Well-formed payloads
# --------------------------------------------------------------------------


def test_well_formed_rss_yields_modified_with_translated_entries() -> None:
    result = client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.feed_type == "rss20"
    assert result.source_title == "Example Culture Desk"
    assert result.source_link == "https://example.com/culture"
    assert result.source_updated_raw == "Mon, 20 Jul 2026 10:00:00 GMT"
    assert result.warnings == ()
    assert len(result.entries) == 2

    first = result.entries[0]
    assert first.entry_id == "tag:example.com,2026:culture/4711"
    assert first.title == "A composer nobody had written up"
    assert first.summary == "A short standfirst."
    assert first.content == "<p>The full body of the article.</p>"
    assert first.author == "Ada Reviewer"


def test_well_formed_atom_yields_modified_with_translated_entries() -> None:
    result = client_for(fixture_bytes("atom.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.feed_type == "atom10"
    assert result.source_title == "Example Atom Desk"
    assert result.warnings == ()
    assert [entry.entry_id for entry in result.entries] == [
        "urn:uuid:entry-0001",
        "urn:uuid:entry-0002",
    ]
    assert [entry.url for entry in result.entries] == [
        "https://example.com/atom/one",
        "https://example.com/atom/two",
    ]
    assert result.entries[0].content == "<p>Atom content body.</p>"
    assert result.entries[1].content is None
    assert result.entries[0].author == "Bo Author"
    assert result.entries[1].author is None


def test_adapter_decides_nothing_about_usability() -> None:
    """No URL normalization and no date parsing happen at this boundary.

    Task 10 owns both. If the adapter ever starts canonicalizing links or
    parsing `pubDate`, the tracking parameter would vanish and the raw date
    string would stop being a string -- so these two assertions are the
    boundary, not incidental detail.
    """
    result = client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert (
        result.entries[0].url == "https://example.com/culture/composer?utm_source=rss"
    )
    assert result.entries[0].published_raw == "Mon, 20 Jul 2026 09:30:00 GMT"


def test_response_bytes_records_the_decoded_payload_size() -> None:
    payload = fixture_bytes("rss20.xml")
    result = client_for(payload).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.response_bytes == len(payload)


# --------------------------------------------------------------------------
# Optionality the source controls
# --------------------------------------------------------------------------


def test_entries_without_ids_translate_to_none_rather_than_being_dropped() -> None:
    result = client_for(fixture_bytes("rss_missing_ids.xml")).fetch_feed(
        FEED, NO_VALIDATORS
    )

    assert isinstance(result, Modified)
    assert len(result.entries) == 2
    assert [entry.entry_id for entry in result.entries] == [None, None]
    assert [entry.url for entry in result.entries] == [
        "https://example.com/noids/one",
        "https://example.com/noids/two",
    ]


def test_entries_without_links_translate_to_none_rather_than_being_dropped() -> None:
    result = client_for(fixture_bytes("rss_missing_links.xml")).fetch_feed(
        FEED, NO_VALIDATORS
    )

    assert isinstance(result, Modified)
    assert len(result.entries) == 2
    assert [entry.url for entry in result.entries] == [None, None]
    assert result.entries[0].entry_id == "tag:example.com,2026:nolinks/1"
    assert result.entries[1].entry_id is None


# --------------------------------------------------------------------------
# Imperfect but recognizable payloads
# --------------------------------------------------------------------------


def test_imperfect_feed_yields_modified_with_recovered_entries() -> None:
    result = client_for(fixture_bytes("imperfect.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.feed_type == "rss20"
    assert len(result.entries) == 3
    assert [entry.title for entry in result.entries] == [
        "First, well formed",
        "Second, with a raw & ampersand",
        "Third, well formed",
    ]


def test_imperfect_feed_retains_its_warning() -> None:
    result = client_for(fixture_bytes("imperfect.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert len(result.warnings) == 1
    assert "SAXParseException" in result.warnings[0]


def test_warning_text_carries_no_payload_content() -> None:
    """A warning is diagnostic, not a transcript of the remote document."""
    result = client_for(fixture_bytes("imperfect.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    joined = " ".join(result.warnings)
    assert "ampersand" not in joined
    assert "Imperfect Desk" not in joined


# --------------------------------------------------------------------------
# Payloads that are not feeds
# --------------------------------------------------------------------------


def test_malformed_response_is_not_retryable_by_category_default() -> None:
    """Pins the premise the explicit `retryable=True` override exists for.

    If `MALFORMED_RESPONSE` were ever added to `RETRYABLE_CATEGORIES`, the
    override in `feeds.py` would become redundant -- and someone would be
    right to remove it. While this assertion holds, removing it is a bug.
    """
    assert FailureCategory.MALFORMED_RESPONSE not in RETRYABLE_CATEGORIES


def test_html_error_page_raises_retryable_malformed_response() -> None:
    with pytest.raises(ProviderFailure) as caught:
        client_for(fixture_bytes("error_page.html")).fetch_feed(FEED, NO_VALIDATORS)

    assert caught.value.category is FailureCategory.MALFORMED_RESPONSE
    assert caught.value.retryable is True


def test_empty_body_raises_retryable_malformed_response() -> None:
    with pytest.raises(ProviderFailure) as caught:
        client_for(fixture_bytes("empty.txt")).fetch_feed(FEED, NO_VALIDATORS)

    assert caught.value.category is FailureCategory.MALFORMED_RESPONSE
    assert caught.value.retryable is True


def test_malformed_response_detail_leaks_neither_body_nor_query_text() -> None:
    payload = fixture_bytes("error_page.html")
    feed = FeedConfig(
        key="culture",
        label="Example Culture Desk",
        url="https://example.com/feed.xml?key=s3cret",
    )
    with pytest.raises(ProviderFailure) as caught:
        client_for(payload).fetch_feed(feed, NO_VALIDATORS)

    rendered = str(caught.value)
    assert "s3cret" not in rendered
    assert "Bad Gateway" not in rendered
    assert "8c1f2a" not in rendered


# --------------------------------------------------------------------------
# Conditional requests
# --------------------------------------------------------------------------


def test_conditional_headers_are_sent_when_validators_are_present() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("rss20.xml"), seen=seen)

    client.fetch_feed(
        FEED,
        FeedValidators(etag='W/"abc"', last_modified="Mon, 20 Jul 2026 10:00:00 GMT"),
    )

    assert len(seen) == 1
    assert seen[0].headers["if-none-match"] == 'W/"abc"'
    assert seen[0].headers["if-modified-since"] == "Mon, 20 Jul 2026 10:00:00 GMT"


def test_each_conditional_header_is_omitted_when_its_validator_is_absent() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("rss20.xml"), seen=seen)

    client.fetch_feed(FEED, FeedValidators(etag='W/"abc"', last_modified=None))

    assert seen[0].headers["if-none-match"] == 'W/"abc"'
    assert "if-modified-since" not in seen[0].headers


def test_no_conditional_headers_are_sent_without_validators() -> None:
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("rss20.xml"), seen=seen)

    client.fetch_feed(FEED, NO_VALIDATORS)

    assert "if-none-match" not in seen[0].headers
    assert "if-modified-since" not in seen[0].headers


# --------------------------------------------------------------------------
# HTTP 304
# --------------------------------------------------------------------------


def test_http_304_yields_not_modified_rather_than_a_failure() -> None:
    result = client_for(b"", status=304).fetch_feed(
        FEED, FeedValidators(etag='W/"abc"', last_modified=None)
    )

    assert isinstance(result, NotModified)
    assert result.status_code == 304
    assert result.requested_url == FEED.url


def test_http_304_exposes_the_response_validators_when_it_carries_them() -> None:
    result = client_for(
        b"",
        status=304,
        headers={"ETag": 'W/"fresh"', "Last-Modified": "Tue, 21 Jul 2026 10:00:00 GMT"},
    ).fetch_feed(FEED, FeedValidators(etag='W/"stale"', last_modified=None))

    assert isinstance(result, NotModified)
    assert result.validators.etag == 'W/"fresh"'
    assert result.validators.last_modified == "Tue, 21 Jul 2026 10:00:00 GMT"


def test_http_304_without_validators_reports_none_rather_than_echoing_the_request() -> (
    None
):
    """A 304 usually omits `Last-Modified` and often `ETag`.

    Absent is reported as absent. Falling back to what was *sent* is the
    caller's decision, and the adapter must not pre-empt it by inventing
    values the response never carried.
    """
    result = client_for(b"", status=304).fetch_feed(
        FEED,
        FeedValidators(etag='W/"stale"', last_modified="Mon, 20 Jul 2026 10:00:00 GMT"),
    )

    assert isinstance(result, NotModified)
    assert result.validators.etag is None
    assert result.validators.last_modified is None


def test_modified_carries_the_response_validators() -> None:
    result = client_for(
        fixture_bytes("rss20.xml"),
        headers={"ETag": 'W/"v2"', "Last-Modified": "Tue, 21 Jul 2026 06:00:00 GMT"},
    ).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.validators.etag == 'W/"v2"'
    assert result.validators.last_modified == "Tue, 21 Jul 2026 06:00:00 GMT"


def test_modified_without_response_validators_reports_none() -> None:
    result = client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.validators.etag is None
    assert result.validators.last_modified is None


# --------------------------------------------------------------------------
# URLs and redirects
# --------------------------------------------------------------------------


def test_redirects_are_reported_as_requested_url_final_url_and_chain() -> None:
    payload = fixture_bytes("rss20.xml")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "example.com":
            return httpx.Response(
                301, headers={"location": "https://other.example/moved.xml"}
            )
        return httpx.Response(200, content=streaming_body(payload))

    result = FeedparserClient(transport_for(handler)).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.requested_url == FEED.url
    assert result.final_url == "https://other.example/moved.xml"
    assert result.redirect_chain == (FEED.url,)


# --------------------------------------------------------------------------
# The request the adapter makes
# --------------------------------------------------------------------------


def test_the_request_is_a_get_attributed_to_the_feeds_provider() -> None:
    transport = RecordingTransport(response=http_response(fixture_bytes("rss20.xml")))

    FeedparserClient(transport).fetch_feed(FEED, NO_VALIDATORS)

    call = transport.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == FEED.url
    assert call["provider"] == "feeds"
    assert call["operation"] == "fetch_feed"
    assert call["limit"] is ResponseLimit.API


def test_feedparser_is_handed_bytes_and_never_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """feedparser can fetch on its own; it must never be given the chance.

    A `str` or an `httpx.URL` argument would send feedparser to the network
    outside the shared transport, past every safety, pacing, and attempt
    guarantee. A stream is passed rather than raw bytes because
    `feedparser._open_resource` short-circuits on anything with `.read()` --
    raw bytes fall through to a `urlopen`/`open()` attempt on the payload.
    """
    captured: list[object] = []
    real_parse = feedparser.parse

    def spy(source: object, **kwargs: object) -> object:
        captured.append(source)
        return real_parse(source, **kwargs)

    monkeypatch.setattr(feeds.feedparser, "parse", spy)
    client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert len(captured) == 1
    assert isinstance(captured[0], io.BytesIO)


def test_feedparser_client_satisfies_the_feed_client_protocol() -> None:
    client: FeedClient = FeedparserClient(
        RecordingTransport(response=http_response(fixture_bytes("rss20.xml")))
    )

    assert isinstance(client.fetch_feed(FEED, NO_VALIDATORS), Modified)


# --------------------------------------------------------------------------
# Failure translation at the boundary
# --------------------------------------------------------------------------


def test_transport_provider_failures_pass_through_unchanged() -> None:
    original = ProviderFailure(
        FailureCategory.RATE_LIMIT,
        provider="feeds",
        operation="fetch_feed",
        status_code=429,
        retry_after_ms=30_000,
    )
    transport = RecordingTransport(error=original)

    with pytest.raises(ProviderFailure) as caught:
        FeedparserClient(transport).fetch_feed(FEED, NO_VALIDATORS)

    assert caught.value is original
    assert caught.value.category is FailureCategory.RATE_LIMIT
    assert caught.value.retryable is True


def test_an_unexpected_parse_error_becomes_a_provider_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing but `ProviderFailure` may leave `fetch_feed`.

    The run engine's execute phase does not isolate unexpected exceptions: a
    stray `ValueError` on a worker thread propagates out of the engine, leaves
    the run row `running`, and loses the digest for every sibling work item
    that succeeded. This boundary is where that is stopped.
    """

    def explode(source: object, **kwargs: object) -> object:
        raise ValueError("https://example.com/feed.xml?key=s3cret is not parseable")

    monkeypatch.setattr(feeds.feedparser, "parse", explode)

    with pytest.raises(ProviderFailure) as caught:
        client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    # The parse boundary's own translation, not the method-wide backstop: a
    # parser defeated by the payload is the malformed-payload case, and it
    # retries. Asserting the category is what distinguishes the two
    # boundaries, so that removing either one is visible here.
    assert caught.value.category is FailureCategory.MALFORMED_RESPONSE
    assert caught.value.retryable is True
    assert caught.value.detail == "ValueError"
    assert "s3cret" not in str(caught.value)


def test_an_unexpected_transport_error_becomes_a_provider_failure() -> None:
    transport = RecordingTransport(error=TypeError("unexpected keyword"))

    with pytest.raises(ProviderFailure) as caught:
        FeedparserClient(transport).fetch_feed(FEED, NO_VALIDATORS)

    # `INTERNAL`, not `MALFORMED_RESPONSE`: reaching the method-wide backstop
    # means a fault in this adapter or its inputs, and it must not be deferred
    # as though the publisher had served something bad.
    assert caught.value.category is FailureCategory.INTERNAL
    assert caught.value.retryable is False
    assert caught.value.detail == "TypeError"
    assert "unexpected keyword" not in str(caught.value)
