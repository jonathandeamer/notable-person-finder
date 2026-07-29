"""The RSS and Atom feed adapter: the first concrete provider in the rewrite.

This module is a *translator*, not a judge. It converts what a publisher
published -- external field names, external optionality -- into frozen
application values, and it decides nothing about whether any of it is usable.
It never validates a URL, never normalizes text, never parses a date, and
never assigns a skip reason. Ingestion owns every one of those decisions, so
that the rule for "is this article worth looking at" lives in exactly one
place rather than being partly pre-applied here where it cannot be reviewed
alongside the rest of the policy.

One caveat on "what the publisher published", because the module cannot
honestly claim byte-for-byte fidelity: feedparser sanitises HTML in `summary`
and `content` by default, stripping `<script>` elements and event-handler
attributes. That behaviour is kept deliberately -- these strings are later
rendered to an operator and fed to a model -- but it is a library default
rather than something this module requests, so
`test_feedparsers_html_sanitiser_is_active` pins it. If a feedparser upgrade
ever turns it off, that test is what makes the change visible instead of
silent. Nothing else is altered: text is not normalized, entities beyond the
sanitiser's work are not rewritten, and no field is dropped.

Two boundaries in here are load-bearing and are easy to mistake for
defensive habit:

1. `fetch_feed` never lets a non-`ProviderFailure` escape. The run engine's
   execute phase deliberately does *not* isolate unexpected exceptions from a
   work item: anything else escaping a worker thread propagates out of the
   engine, leaves the run row `running`, and writes no digest at all -- losing
   the results of every sibling work item that succeeded. `feedparser` and URL
   handling raise plain `ValueError` and `TypeError` readily, so the adapter
   boundary is where that has to stop. This mirrors what `transport.py` does
   at its own `build_request` and `send` boundaries.
2. Translated failure detail is always `type(error).__name__`, never
   `str(error)`. `ProviderFailure.__str__` interpolates `detail`, so an
   exception message would carry raw remote text -- a fragment of a response
   body, a URL complete with its query string and any key in it -- into logs
   and terminal output.
"""

from __future__ import annotations

import io
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol
from xml.sax import SAXParseException

import feedparser

from notable_person_finder.config.models import FeedConfig
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
)

PROVIDER = "feeds"
OPERATION = "fetch_feed"

# feedparser reports what it recognized in `version`: "rss20", "rss091n",
# "atom10", "atom03", and so on, with "" for "parsed something, but it was not
# a feed" and None for "nothing to parse". Matching by prefix rather than
# against an exhaustive set keeps a future feedparser release's new RSS or
# Atom revision working.
#
# This is a positive allowlist, and that is the whole rule: any other syntax
# is rejected because the milestone is scoped to RSS and Atom, and accepting a
# third one would ship a translation path no test covers. Do not read the
# allowlist as excluding some specific rival format -- in feedparser 6.0.12
# there is nothing else to exclude. "cdf" survives only in its
# `SUPPORTED_VERSIONS` table with no code path that sets it, and "json1" does
# not exist in feedparser 6 at all; a CDF document and a JSON Feed both parse
# to `version == ""`. Because no payload can express a widened allowlist,
# `test_only_rss_and_atom_version_strings_are_accepted` pins the rule against
# a fabricated parse result instead.
_RECOGNIZED_FEED_PREFIXES = ("rss", "atom")

_NOT_MODIFIED = 304


class FeedClient(Protocol):
    """The seam a run-engine handler depends on, so it can be faked in tests."""

    def fetch_feed(
        self, feed: FeedConfig, validators: FeedValidators
    ) -> FeedFetchResult: ...


@dataclass(frozen=True, slots=True)
class FeedValidators:
    """HTTP cache validators, in whichever direction they are travelling.

    On the way out these are what the last successful fetch stored. On the way
    back they are whatever *this* response carried, which may be nothing: a
    304 usually omits `Last-Modified` and often omits `ETag` too. Absent is
    reported as `None` and never backfilled from what was sent -- deciding to
    carry the previous validator forward is the caller's call to make, and
    inventing a value here would hide from it whether the server actually
    reaffirmed one.
    """

    etag: str | None
    last_modified: str | None


@dataclass(frozen=True, slots=True)
class FeedEntry:
    """One feed item, translated field for field and judged not at all.

    Every field is optional because every field is genuinely optional in the
    wild: RSS requires neither `guid` nor `link`, and plenty of real feeds omit
    one or both. `summary` and `content` have passed feedparser's HTML
    sanitiser; see the module docstring.

    Both dates are carried, unparsed, and neither substitutes for the other.
    RFC 4287 makes `atom:updated` mandatory and `atom:published` optional, so a
    conforming Atom feed with only `<updated>` is common -- and falling back
    from one to the other would assert that "when this was published" and "when
    this document last changed" are the same claim. They are not, and choosing
    between them is a usability judgement that belongs to ingestion, which
    cannot recover a distinction this adapter has already collapsed.

    RSS carries no per-item modification date, so an RSS entry reports
    `published_raw` and leaves `updated_raw` as `None`. Getting that right
    required reading literally stored keys rather than feedparser's own lookup,
    which would have echoed `pubDate` into both fields; see `_raw`.
    """

    entry_id: str | None
    url: str | None
    title: str | None
    summary: str | None
    content: str | None
    author: str | None
    published_raw: str | None
    updated_raw: str | None


@dataclass(frozen=True, slots=True)
class NotModified:
    """HTTP 304: the feed is unchanged. A success, not a failure."""

    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    status_code: int
    validators: FeedValidators


@dataclass(frozen=True, slots=True)
class Modified:
    """A payload feedparser recognized as RSS or Atom, with its entries.

    `warnings` is non-empty when feedparser set `bozo` -- it recovered entries
    from an imperfect document. Those entries are accepted, because a single
    unescaped ampersand should not cost a publisher a day of coverage, and the
    warnings travel alongside them so the imperfection is never silently
    erased.
    """

    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    status_code: int
    validators: FeedValidators
    feed_type: str
    source_title: str | None
    source_link: str | None
    source_updated_raw: str | None
    entries: tuple[FeedEntry, ...]
    warnings: tuple[str, ...]
    response_bytes: int


FeedFetchResult = NotModified | Modified


def _raw(container: dict[str, object], key: str) -> object:
    """Read the key that is literally stored, bypassing feedparser's aliasing.

    `FeedParserDict.__getitem__` is not a plain lookup. For `updated` it
    silently returns `published` when `updated` is absent -- with a
    `DeprecationWarning`, and the docstring in feedparser says the fallback
    "will be removed in a future version". Going through it would be wrong
    three times over:

    * it collapses `published` into `updated`, which is exactly the conflation
      `FeedEntry` exists to avoid, merely in the opposite direction, and it
      would make every RSS entry report a modification date it never carried;
    * the behaviour is deprecated, so an upgrade would change what this adapter
      reports with no code change and no failing test; and
    * it emits a `DeprecationWarning` for every entry read, which is noise in
      an operator's log for a value we did not want anyway.

    `dict.get` on the instance reaches the stored mapping directly. Every field
    is read this way, not just the dates: a translator must never resolve one
    external field name to a different external field's value. feedparser
    canonicalizes names on *write* (`guid` is stored as `id`), so the literal
    keys are the right ones to ask for.
    """
    return dict.get(container, key)


def _text(container: dict[str, object] | None, key: str) -> str | None:
    """Read `key` as a string, or `None` if it is missing or not a string.

    feedparser hands back a `dict` subclass with untyped, heterogeneous
    values: a key may be absent, may be a string, or may be a nested
    structure. Every value this module exposes passes through here or
    `_first_content`, so nothing untyped escapes into a frozen result.
    """
    if container is None:
        return None
    value = _raw(container, key)
    return value if isinstance(value, str) else None


def _mapping(value: object) -> dict[str, object] | None:
    return value if isinstance(value, dict) else None


def _first_content(entry: dict[str, object]) -> str | None:
    """The first `content` body a publisher supplied, if any.

    feedparser models `content` as a list, because Atom permits several
    representations of the same entry. The application needs one body
    candidate, and the first is the source's own preferred order -- choosing
    among them by type or length would be a usability decision, which belongs
    to ingestion rather than here.
    """
    values = _raw(entry, "content")
    if not isinstance(values, Sequence) or isinstance(values, str | bytes):
        return None
    for item in values:
        text = _text(_mapping(item), "value")
        if text is not None:
            return text
    return None


def _describe(error: object) -> str:
    """A warning string that is sanitized by construction.

    Only the exception type, and for a SAX error its position, are reported.
    The exception's own message is deliberately dropped, because a warning is
    persisted to the database and rendered in the operator's digest, and some
    of these messages quote remote-controlled text.

    The case that actually demonstrates this is `CharacterEncodingOverride`,
    whose message is `document declared as <declared>, but parsed as utf-8` --
    where `<declared>` is whatever charset string the remote document chose to
    write. Such a payload keeps `version == "rss20"` and its entries, so it
    becomes a `Modified` result and the warning travels onward.

    A SAX message from CPython's expat, by contrast, is positional and quotes
    nothing (`<unknown>:13:33: not well-formed (invalid token)`; even an
    undefined entity is reported as a bare `undefined entity`). Type plus
    position is enough to tell an operator a feed is broken and roughly where,
    without carrying any fragment of the document with it.
    """
    if error is None:
        return "unspecified parser warning"
    name = type(error).__name__
    if isinstance(error, SAXParseException):
        return (
            f"{name} at line {error.getLineNumber()}, column {error.getColumnNumber()}"
        )
    return name


def _malformed(detail: str) -> ProviderFailure:
    """A malformed feed payload, raised as a *retryable* failure.

    `retryable=True` is an explicit override and is not redundant:
    `MALFORMED_RESPONSE` is not in `RETRYABLE_CATEGORIES`, so the default here
    would be `False` and the engine would make no fresh attempt. For feeds, a
    malformed payload is often a self-healing CDN or origin error page, so the
    override buys one retry. The shared malformed-response ceiling still
    forecloses a second malformed result as `PERMANENT`: with one configured
    attempt the first result exhausts to `DEFERRED`; with the shipped default
    of three attempts, the second result settles `FAILED_PERMANENT` after two
    calls. A terminal item does not block the next ordinary run from seeding
    fresh work for the feed.
    """
    return ProviderFailure(
        FailureCategory.MALFORMED_RESPONSE,
        provider=PROVIDER,
        operation=OPERATION,
        retryable=True,
        detail=detail,
    )


class FeedparserClient:
    """Fetches a feed over the shared transport and parses it with feedparser.

    feedparser is used strictly as a parser of bytes already fetched. It is
    never given a URL: `feedparser.parse` will happily open one itself, which
    would bypass the transport's URL safety checks, pacing, response bounds,
    and per-attempt accounting entirely.
    """

    def __init__(self, transport: HttpTransport) -> None:
        self._transport = transport

    def fetch_feed(
        self, feed: FeedConfig, validators: FeedValidators
    ) -> FeedFetchResult:
        try:
            headers: dict[str, str] = {}
            if validators.etag:
                headers["If-None-Match"] = validators.etag
            if validators.last_modified:
                headers["If-Modified-Since"] = validators.last_modified

            response = self._transport.request(
                "GET",
                feed.url,
                provider=PROVIDER,
                operation=OPERATION,
                headers=headers or None,
                # A feed is an API-shaped document, so the API ceiling (5 MB by
                # default) applies rather than the larger article ceiling. That
                # is ample: a feed an order of magnitude past it is a
                # misconfiguration, not a publisher this tool should follow.
                limit=ResponseLimit.API,
            )

            # 304 arrives as an ordinary response: the transport treats only
            # 301/302/303/307/308 as redirects and only >= 400 as a failure.
            # So a conditional hit is detected by reading the status, not by
            # catching anything.
            if response.status_code == _NOT_MODIFIED:
                return NotModified(
                    requested_url=response.requested_url,
                    final_url=response.final_url,
                    redirect_chain=response.redirect_chain,
                    status_code=response.status_code,
                    validators=_response_validators(response),
                )

            return self._parse(response)
        except ProviderFailure:
            # Already translated and categorized by the transport (or by
            # `_parse`). Re-wrapping would discard its status code, its
            # `Retry-After`, and its retryability.
            raise
        except Exception as error:
            # Nothing else may leave this method; see the module docstring.
            # `INTERNAL` rather than `MALFORMED_RESPONSE`, because reaching
            # here means an unexpected fault in this adapter or its inputs
            # rather than a bad payload, and it should not be quietly deferred
            # as though the publisher were at fault.
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=OPERATION,
                detail=type(error).__name__,
            ) from error

    def _parse(self, response: HttpResponse) -> Modified:
        # A stream, not raw bytes: `feedparser._open_resource` returns
        # immediately for anything with `.read()`, whereas raw bytes fall
        # through to a `urlopen`/`open()` attempt on the payload itself.
        # Wrapping the body makes it structurally impossible for a remote
        # document to be treated as a URL or a filename.
        #
        # Built *outside* the try below on purpose. Reading `response.content`
        # or constructing the stream is our own work on our own object, so a
        # fault there is an internal fault; inside the try it would be
        # attributed to the publisher as a retryable malformed payload, which
        # inverts the exact distinction the two failure boundaries exist to
        # keep apart. The method-wide backstop in `fetch_feed` catches it as
        # `INTERNAL` instead.
        stream = io.BytesIO(response.content)
        try:
            parsed = feedparser.parse(stream)
        except Exception as error:
            # feedparser recovers from bad markup on its own, so an exception
            # escaping it means the payload defeated the parser outright --
            # which is the malformed-payload case, retryable like the others.
            raise _malformed(type(error).__name__) from error

        document = _mapping(parsed)
        if document is None:  # pragma: no cover - feedparser always returns a dict
            raise _malformed("parser returned no document")

        feed_type = _text(document, "version")
        if feed_type is None or not feed_type.startswith(_RECOGNIZED_FEED_PREFIXES):
            # An HTML error page, a JSON blob, an empty body, or plain text.
            # feedparser does not necessarily flag any of these as `bozo`; it
            # simply fails to recognize a feed, which is what `version`
            # reports. The detail names no part of the payload.
            raise _malformed("payload is not a recognized RSS or Atom feed")

        source = _mapping(_raw(document, "feed"))
        raw_entries = _raw(document, "entries")
        entries = raw_entries if isinstance(raw_entries, Sequence) else ()

        warnings: tuple[str, ...] = ()
        if _raw(document, "bozo"):
            warnings = (_describe(_raw(document, "bozo_exception")),)

        return Modified(
            requested_url=response.requested_url,
            final_url=response.final_url,
            redirect_chain=response.redirect_chain,
            status_code=response.status_code,
            validators=_response_validators(response),
            feed_type=feed_type,
            source_title=_text(source, "title"),
            source_link=_text(source, "link"),
            source_updated_raw=_text(source, "updated"),
            entries=tuple(
                _entry(mapping)
                for mapping in (_mapping(item) for item in entries)
                if mapping is not None
            ),
            warnings=warnings,
            response_bytes=response.decoded_bytes,
        )


def _entry(entry: dict[str, object]) -> FeedEntry:
    return FeedEntry(
        entry_id=_text(entry, "id"),
        url=_text(entry, "link"),
        title=_text(entry, "title"),
        summary=_text(entry, "summary"),
        content=_first_content(entry),
        author=_text(entry, "author"),
        published_raw=_text(entry, "published"),
        updated_raw=_text(entry, "updated"),
    )


def _response_validators(response: HttpResponse) -> FeedValidators:
    """Read the validators this response carried, if it carried any.

    Response header names arrive already lower-cased from the transport.
    """
    return FeedValidators(
        etag=response.headers.get("etag"),
        last_modified=response.headers.get("last-modified"),
    )
