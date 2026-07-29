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
import warnings
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
    payload: bytes,
    *,
    status: int = 200,
    headers: Mapping[str, str] | None = None,
    encoded_bytes: int | None = None,
) -> HttpResponse:
    """A hand-built response.

    `encoded_bytes` can be set independently of the decoded length so that a
    compressed response -- where the two genuinely differ -- can be
    represented. Every fixture served over the wire in these tests is
    uncompressed, which makes the two values equal and would otherwise hide
    which one the adapter reports.
    """
    return HttpResponse(
        requested_url=FEED.url,
        final_url=FEED.url,
        redirect_chain=(),
        destination_host="example.com",
        status_code=status,
        headers=dict(headers or {}),
        content=payload,
        encoded_bytes=len(payload) if encoded_bytes is None else encoded_bytes,
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


def test_response_bytes_is_the_decoded_size_not_the_encoded_size() -> None:
    """The two differ only for a compressed response, so it is pinned here.

    Every fixture served over `httpx.MockTransport` in this module is
    uncompressed, which makes `encoded_bytes` and `decoded_bytes` equal and
    hides which of the two `response_bytes` reports. A hand-built response
    separates them.
    """
    payload = fixture_bytes("rss20.xml")
    transport = RecordingTransport(response=http_response(payload, encoded_bytes=17))

    result = FeedparserClient(transport).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.response_bytes == len(payload)
    assert result.response_bytes != 17


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


def test_atom_published_and_updated_are_kept_as_separate_dates() -> None:
    """RFC 4287 makes `atom:updated` mandatory and `atom:published` optional.

    A conforming Atom feed carrying only `<updated>` is common, and collapsing
    the two -- falling back from `published` to `updated` -- would assert they
    mean the same thing. They do not: one is when the publisher says the piece
    came out, the other merely when the document last changed. Deciding
    between them is a usability judgement that belongs to ingestion, and
    ingestion cannot recover a distinction this adapter has already thrown
    away. So both are exposed and neither substitutes for the other.
    """
    result = client_for(fixture_bytes("atom_updated_only.xml")).fetch_feed(
        FEED, NO_VALIDATORS
    )

    assert isinstance(result, Modified)
    entry = result.entries[0]
    assert entry.published_raw is None
    assert entry.updated_raw == "2026-07-19T08:00:00Z"


def test_atom_entry_with_both_dates_reports_both() -> None:
    result = client_for(fixture_bytes("atom.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.entries[0].published_raw == "2026-07-20T09:00:00Z"
    assert result.entries[0].updated_raw == "2026-07-20T09:05:00Z"


def test_rss_pubdate_populates_only_published_and_never_updated() -> None:
    """RSS has no per-item modification date, so `updated_raw` stays `None`.

    Reading this through `FeedParserDict`'s own lookup would report the
    `pubDate` value in *both* fields: for the key `updated`, feedparser falls
    back to `published` when `updated` is absent. That is the same conflation
    `FeedEntry` exists to prevent, just running in the opposite direction --
    it would have every RSS entry claim a modification date the publisher never
    wrote. It is also deprecated in feedparser and slated for removal, so
    depending on it would mean an upgrade silently changing what this adapter
    reports.

    The adapter therefore reads literally stored keys only. This test is what
    holds that: reinstating the aliasing lookup makes `updated_raw` equal
    `published_raw` here.
    """
    result = client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.entries[0].published_raw == "Mon, 20 Jul 2026 09:30:00 GMT"
    assert result.entries[0].updated_raw is None


def test_no_deprecated_feedparser_behaviour_is_relied_on() -> None:
    """Turn feedparser's own `DeprecationWarning` into a failure.

    feedparser warns when its temporary `updated` -> `published` fallback is
    used. Escalating warnings to errors across a full fetch of both a
    date-bearing RSS feed and an Atom feed is what keeps this adapter off that
    path -- reinstating the aliasing lookup raises here rather than merely
    changing a value.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        for name in ("rss20.xml", "atom.xml", "atom_updated_only.xml"):
            assert isinstance(
                client_for(fixture_bytes(name)).fetch_feed(FEED, NO_VALIDATORS),
                Modified,
            )


def test_entries_without_links_translate_to_none_rather_than_being_dropped() -> None:
    result = client_for(fixture_bytes("rss_missing_links.xml")).fetch_feed(
        FEED, NO_VALIDATORS
    )

    assert isinstance(result, Modified)
    assert len(result.entries) == 2
    assert [entry.url for entry in result.entries] == [None, None]
    assert result.entries[0].entry_id == "tag:example.com,2026:nolinks/1"
    assert result.entries[1].entry_id is None


def test_the_first_content_representation_is_the_one_reported() -> None:
    """Atom permits several `<content>` elements; feedparser keeps them all.

    The application needs one body candidate, and source order is the
    publisher's own preference. Choosing by media type or by length would be a
    usability decision, which belongs to ingestion. This is the only case
    `_first_content`'s loop exists for, so without a multi-content fixture the
    ordering is unpinned.
    """
    result = client_for(fixture_bytes("atom_multiple_content.xml")).fetch_feed(
        FEED, NO_VALIDATORS
    )

    assert isinstance(result, Modified)
    assert result.entries[0].content == "the first representation"


def test_relative_entry_links_pass_through_unresolved() -> None:
    """A `BytesIO` carries no base URI, so feedparser cannot resolve links.

    This is correct for the boundary -- resolving against the final URL would
    be a URL decision, and `canonicalize_article_url` in ingestion owns those
    -- but it means Task 10 will meet a bare path in `url` and must handle it
    rather than assuming an absolute URL.
    """
    payload = b"""<?xml version="1.0"?><rss version="2.0"><channel>
    <title>Relative Desk</title>
    <item><guid isPermaLink="false">tag:example.com,2026:rel/1</guid>
    <link>/relative/path</link></item>
    </channel></rss>"""

    result = client_for(payload).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.entries[0].url == "/relative/path"


def test_feedparsers_html_sanitiser_is_active() -> None:
    """Pinned so that losing the sanitiser becomes visible rather than silent.

    feedparser sanitises HTML in `summary` and `content` by default, stripping
    `<script>` elements and event-handler attributes. That is the behaviour to
    keep -- these strings are later rendered and fed to a model -- but it is a
    library default rather than something this module asks for, so a feedparser
    upgrade or a stray `sanitize_html=False` could remove it without any test
    noticing. It also means the values are not *literally* byte-for-byte what
    the publisher wrote, which the module docstring now says plainly.
    """
    payload = fixture_bytes("atom_unsafe_html.xml")

    # Positive control: the fixture really does carry a script element and an
    # event handler, so finding them absent below means they were stripped.
    assert b"script" in payload
    assert b"onclick" in payload

    result = client_for(payload).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    entry = result.entries[0]
    assert entry.summary is not None
    assert entry.content is not None

    for markup in (entry.summary, entry.content):
        assert "<script>" not in markup
        assert "onclick" not in markup
        assert "alert(" not in markup

    # The prose itself survives: sanitising removes the dangerous parts, it
    # does not discard the body.
    assert "the summary body" in entry.summary
    assert "the content body" in entry.content


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
    """Asserted positively and exactly, so the rendering itself is pinned.

    An earlier version of this test asserted only that the payload's own words
    were *absent* from the warning. That was vacuous: expat's message for this
    fixture is positional and quotes nothing from the document, so a naive
    `f"{type(error).__name__}: {error}"` would have satisfied it. The
    sanitisation claim is carried by
    `test_a_bogus_charset_declaration_is_warned_about_without_being_quoted`,
    which has a payload that can actually leak; this test pins the format.
    """
    result = client_for(fixture_bytes("imperfect.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.warnings == ("SAXParseException at line 13, column 33",)


def test_a_bogus_charset_declaration_is_warned_about_without_being_quoted() -> None:
    """The reachable warning leak, and the reason `_describe` drops messages.

    A payload declaring a charset feedparser cannot honour produces
    `CharacterEncodingOverride`, whose message is
    `document declared as <declared>, but parsed as utf-8` -- and `<declared>`
    is remote-controlled text. It arrives with `version == "rss20"` and its
    entries intact, so it becomes a `Modified` result and the warning is
    persisted to the database and rendered in the operator's digest.

    The `imperfect.xml` SAX case cannot demonstrate this: expat's message is
    positional (`<unknown>:13:33: not well-formed (invalid token)`) and quotes
    nothing from the document, so a naive
    `f"{type(error).__name__}: {error}"` would pass a negative assertion made
    against it. This fixture is the one that can tell the difference.
    """
    payload = fixture_bytes("bad_encoding.xml")

    # Positive control. A negative assertion is worthless unless the string it
    # looks for is genuinely reachable, so first prove that feedparser's own
    # exception message for this exact payload *does* quote the declared
    # charset. Without this, the assertions below could pass simply because
    # nothing ever put the charset anywhere.
    leaked = str(feedparser.parse(io.BytesIO(payload)).get("bozo_exception"))
    assert "x-leaky-charset-4f3a2b" in leaked

    result = client_for(payload).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    # The imperfection is reported, not swallowed ...
    assert len(result.warnings) == 1
    assert "CharacterEncodingOverride" in result.warnings[0]
    # ... and the entries are still delivered.
    assert len(result.entries) == 1
    # But the declared charset -- the remote-controlled part of the message --
    # never reaches the warning.
    assert "x-leaky-charset-4f3a2b" not in result.warnings[0]
    assert "declared as" not in result.warnings[0]


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


@pytest.mark.parametrize("version", ["cdf", "json1", "html", "", "rdf"])
def test_only_rss_and_atom_version_strings_are_accepted(
    version: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recognition is a positive allowlist, and entries do not override it.

    This is asserted against a fabricated parse result rather than a fixture
    on purpose: feedparser 6.0.12 cannot emit any of these version strings.
    `cdf` survives only in its `SUPPORTED_VERSIONS` table -- no code path sets
    it -- and `json1` does not exist in feedparser 6 at all; a CDF document
    and a JSON Feed both parse to `version == ""`. So no payload can express
    the rule, and a fixture-only test leaves "widen the accepted prefixes"
    unkillable. Fabricating the document pins the decision at the level where
    it is actually made.

    The fabricated document deliberately carries entries, so the rule cannot
    be satisfied by "it parsed something, so accept it".
    """
    document = {
        "version": version,
        "bozo": False,
        "feed": {"title": "Some Document"},
        "entries": [{"id": "x", "link": "https://example.com/x", "title": "X"}],
    }
    monkeypatch.setattr(feeds.feedparser, "parse", lambda source, **kwargs: document)

    with pytest.raises(ProviderFailure) as caught:
        client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert caught.value.category is FailureCategory.MALFORMED_RESPONSE
    assert caught.value.retryable is True


@pytest.mark.parametrize("version", ["rss20", "rss091n", "atom10", "atom03"])
def test_every_rss_and_atom_version_string_is_accepted(
    version: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The other half of the allowlist: prefix matching, not an exact set.

    Matching by prefix is what keeps a future feedparser release's new RSS or
    Atom revision working without a code change here.
    """
    document = {
        "version": version,
        "bozo": False,
        "feed": {"title": "Some Feed"},
        "entries": [{"id": "x", "link": "https://example.com/x", "title": "X"}],
    }
    monkeypatch.setattr(feeds.feedparser, "parse", lambda source, **kwargs: document)

    result = client_for(fixture_bytes("rss20.xml")).fetch_feed(FEED, NO_VALIDATORS)

    assert isinstance(result, Modified)
    assert result.feed_type == version


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

    # Positive controls: each string the assertions below look for is really
    # present in the material the adapter handled, so their absence from the
    # rendered failure is evidence rather than coincidence.
    assert "s3cret" in feed.url
    assert b"Bad Gateway" in payload
    assert b"8c1f2a" in payload

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
    """Both requests are made here so the control is inside the test.

    The first fetch proves these two header names do reach the wire when
    validators are supplied; only then does their absence from the second
    fetch mean the adapter omitted them, rather than the assertion looking for
    a header name that never appears under any circumstances.
    """
    seen: list[httpx.Request] = []
    client = client_for(fixture_bytes("rss20.xml"), seen=seen)

    client.fetch_feed(FEED, FeedValidators(etag='W/"a"', last_modified="Mon, 20 Jul"))
    assert "if-none-match" in seen[0].headers
    assert "if-modified-since" in seen[0].headers

    client.fetch_feed(FEED, NO_VALIDATORS)

    assert "if-none-match" not in seen[1].headers
    assert "if-modified-since" not in seen[1].headers


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

    message = "https://example.com/feed.xml?key=s3cret is not parseable"

    def explode(source: object, **kwargs: object) -> object:
        raise ValueError(message)

    # Positive control for the negative assertion at the end: the secret is
    # genuinely in the exception's own message, so `str(error)` would carry it.
    assert "s3cret" in message

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


class UnreadableBodyResponse(HttpResponse):
    """A response whose body cannot be materialised.

    The property shadows the frozen dataclass's `content` slot. A no-op setter
    is required because the generated `__init__` still assigns the field; the
    getter is the part under test.

    Replacing a `bytes` field with a property is exactly the kind of
    substitution Pyright is right to reject in production code, and this is the
    one place it is the point: the double exists to make a body read fail. The
    suppression is scoped to this single line and names the one rule, rather
    than silencing the file.
    """

    @property
    def content(self) -> bytes:
        raise MemoryError("body cannot be materialised")

    @content.setter
    def content(  # pyright: ignore[reportIncompatibleVariableOverride]
        self, value: bytes
    ) -> None:
        return None


def test_a_fault_reading_the_response_body_is_internal_not_the_publishers_fault() -> (
    None
):
    """Why `io.BytesIO(response.content)` sits outside the parse boundary.

    Reading the body off our own response object is our own work. Inside the
    parse `try` a fault there would be reported as a retryable
    `MALFORMED_RESPONSE` -- blaming the publisher for a bad payload and
    quietly deferring the item -- which inverts the exact distinction the two
    boundaries exist to maintain. Low-reachability, but the attribution is the
    whole point, so it is pinned rather than argued.
    """
    response = UnreadableBodyResponse(
        requested_url=FEED.url,
        final_url=FEED.url,
        redirect_chain=(),
        destination_host="example.com",
        status_code=200,
        headers={},
        content=b"",
        encoded_bytes=0,
        decoded_bytes=0,
    )

    with pytest.raises(ProviderFailure) as caught:
        FeedparserClient(RecordingTransport(response=response)).fetch_feed(
            FEED, NO_VALIDATORS
        )

    assert caught.value.category is FailureCategory.INTERNAL
    assert caught.value.retryable is False
    assert caught.value.detail == "MemoryError"


def test_an_unexpected_transport_error_becomes_a_provider_failure() -> None:
    error = TypeError("unexpected keyword")
    transport = RecordingTransport(error=error)

    # Positive control: the message really is on the exception, so its absence
    # from the rendered failure below means `detail` did not interpolate it.
    assert "unexpected keyword" in str(error)

    with pytest.raises(ProviderFailure) as caught:
        FeedparserClient(transport).fetch_feed(FEED, NO_VALIDATORS)

    # `INTERNAL`, not `MALFORMED_RESPONSE`: reaching the method-wide backstop
    # means a fault in this adapter or its inputs, and it must not be deferred
    # as though the publisher had served something bad.
    assert caught.value.category is FailureCategory.INTERNAL
    assert caught.value.retryable is False
    assert caught.value.detail == "TypeError"
    assert "unexpected keyword" not in str(caught.value)
