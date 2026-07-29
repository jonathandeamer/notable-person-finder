"""Opt-in smoke coverage for the real feed-provider boundary.

The live smoke tests intentionally use the shared transport's real DNS and
HTTP implementation. They are marked ``live`` so the standard suite makes no
DNS or network call; invoke them explicitly with ``pytest -m live``. The
small cause-classifier tests are offline controls for that skip boundary.
"""

from __future__ import annotations

import errno
import socket
import ssl
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

import httpx
import pytest

from notable_person_finder.config.models import FeedConfig, FeedsConfig, TransportConfig
from notable_person_finder.ingestion.urls import canonicalize_article_url, publisher_key
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.feeds import (
    FeedparserClient,
    FeedValidators,
    Modified,
    NotModified,
)
from notable_person_finder.providers.safety import SystemHostResolver, UnsafeUrl
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.runs.clock import SystemClock

_ROOT = Path(__file__).parents[2]
_FEEDS_PATH = _ROOT / "config" / "discovery-feeds.example.toml"
_EMPTY_VALIDATORS = FeedValidators(etag=None, last_modified=None)
_UNREACHABLE_NETWORK_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH})


def _failure_caused_by(category: FailureCategory, cause: Exception) -> ProviderFailure:
    failure = ProviderFailure(category, provider="feeds", operation="fetch_feed")
    failure.__cause__ = cause
    return failure


def _connect_error_caused_by(cause: Exception) -> httpx.ConnectError:
    connect_error = httpx.ConnectError("connect failed")
    connect_error.__cause__ = cause
    return connect_error


@pytest.mark.parametrize(
    ("category", "cause"),
    [
        (FailureCategory.NETWORK, socket.gaierror(socket.EAI_NONAME, "no name")),
        (FailureCategory.CONFIGURATION, UnsafeUrl("host could not be resolved")),
        (
            FailureCategory.NETWORK,
            _connect_error_caused_by(OSError(errno.ENETUNREACH, "no route")),
        ),
        (FailureCategory.NETWORK, ConnectionRefusedError("refused")),
        (FailureCategory.TIMEOUT, httpx.ConnectTimeout("connect timed out")),
    ],
)
def test_environment_unavailability_has_a_skip_reason(
    category: FailureCategory, cause: Exception
) -> None:
    assert _skip_reason(_failure_caused_by(category, cause)) is not None


@pytest.mark.parametrize(
    ("category", "cause"),
    [
        (FailureCategory.NETWORK, httpx.ConnectError("no nested OS error")),
        (
            FailureCategory.NETWORK,
            _connect_error_caused_by(ssl.SSLError("TLS handshake failed")),
        ),
        (FailureCategory.TIMEOUT, httpx.ReadTimeout("read timed out")),
        (FailureCategory.MALFORMED_RESPONSE, httpx.RemoteProtocolError("bad HTTP")),
        (FailureCategory.CONFIGURATION, UnsafeUrl("unsafe request destination")),
    ],
)
def test_deterministic_transport_failures_have_no_skip_reason(
    category: FailureCategory, cause: Exception
) -> None:
    assert _skip_reason(_failure_caused_by(category, cause)) is None


def _configured_feed(key: str) -> FeedConfig:
    """Load a named feed from the tracked example, never a test-local URL."""
    configured = FeedsConfig.model_validate(tomllib.loads(_FEEDS_PATH.read_text()))
    return next(feed for feed in configured.feeds if feed.key == key)


def _cause_chain(error: BaseException) -> tuple[BaseException, ...]:
    """Return the explicit/contextual causes without looping on malformed chains."""
    causes: list[BaseException] = []
    current = error.__cause__ or error.__context__
    while current is not None and id(current) not in {id(item) for item in causes}:
        causes.append(current)
        current = current.__cause__ or current.__context__
    return tuple(causes)


def _skip_reason(error: ProviderFailure) -> str | None:
    """Name only environment failures that make a live assertion impossible.

    ``HttpTransport`` deliberately turns several HTTPX transport subclasses
    into the broad ``network`` and ``timeout`` categories. The live smoke must
    retain the original subtype here: a TLS, request, protocol, or read-timeout
    failure is a contract signal, while a missing route or failed connection
    establishment means the environment cannot conduct the smoke at all.
    """
    causes = _cause_chain(error)
    if error.category is FailureCategory.CONFIGURATION and any(
        isinstance(cause, UnsafeUrl) and "could not be resolved" in str(cause)
        for cause in causes
    ):
        return "live DNS unavailable"
    if error.category is FailureCategory.TIMEOUT and any(
        isinstance(cause, httpx.ConnectTimeout) for cause in causes
    ):
        return "live connection timed out"
    if error.category is not FailureCategory.NETWORK:
        return None
    if any(isinstance(cause, socket.gaierror) for cause in causes):
        return "live DNS unavailable"
    if any(isinstance(cause, ConnectionRefusedError) for cause in causes):
        return "live connection refused"
    if any(
        isinstance(cause, OSError) and cause.errno in _UNREACHABLE_NETWORK_ERRNOS
        for cause in causes
    ):
        return "live network unreachable"
    return None


def _fetch_live(
    feed: FeedConfig,
    validators: FeedValidators = _EMPTY_VALIDATORS,
    *,
    config: TransportConfig | None = None,
) -> Modified | NotModified:
    """Fetch once, skipping only when the live environment cannot connect."""
    resolver = SystemHostResolver()
    with build_transport(
        config or TransportConfig(),
        version="0.1.0",
        resolver=resolver,
        clock=SystemClock(),
    ) as transport:
        try:
            return FeedparserClient(transport).fetch_feed(feed, validators)
        except ProviderFailure as error:
            if reason := _skip_reason(error):
                pytest.skip(reason)
            raise


@pytest.mark.live
def test_artnet_conditional_fetch_reaches_a_real_not_modified_response() -> None:
    """Removing conditional headers makes this second real request return 200."""
    feed = _configured_feed("artnet-news")

    first = _fetch_live(feed)
    if isinstance(first, Modified):
        assert first.entries

    validators = first.validators
    assert validators.etag is not None or validators.last_modified is not None

    second = _fetch_live(feed, validators)

    assert isinstance(second, NotModified)
    assert second.status_code == 304


@pytest.mark.live
def test_hyperallergic_configured_feed_records_a_real_redirect() -> None:
    """Clearing the transport redirect chain makes this real-origin check fail."""
    feed = _configured_feed("hyperallergic")

    result = _fetch_live(feed)

    assert isinstance(result, Modified)
    assert result.entries
    assert result.requested_url == feed.url
    assert result.redirect_chain == (feed.url,)
    assert result.final_url == "https://hyperallergic.com/rss/"


@pytest.mark.live
def test_artnet_response_size_limit_rejects_a_real_feed_body() -> None:
    """Using the article response limit here would let this response through."""
    feed = _configured_feed("artnet-news")

    with pytest.raises(ProviderFailure) as raised:
        _fetch_live(feed, config=TransportConfig(max_api_response_bytes=1))

    assert raised.value.category is FailureCategory.RESPONSE_TOO_LARGE


@pytest.mark.live
def test_artnet_entries_have_plausible_canonical_articles_and_publisher_key() -> None:
    """Returning the full host from ``publisher_key`` breaks this assertion."""
    result = _fetch_live(_configured_feed("artnet-news"))

    assert isinstance(result, Modified)
    article_url = next(
        entry.url
        for entry in result.entries
        if entry.url is not None and urlsplit(entry.url).hostname == "news.artnet.com"
    )
    canonical_url = canonicalize_article_url(article_url)
    parsed = urlsplit(canonical_url)

    assert parsed.scheme == "https"
    assert parsed.hostname == "news.artnet.com"
    assert parsed.path.startswith("/") and parsed.path != "/feed"
    assert parsed.fragment == ""
    assert publisher_key(canonical_url) == "artnet.com"
