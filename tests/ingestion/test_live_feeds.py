"""Opt-in smoke coverage for the real feed-provider boundary.

These tests intentionally use the shared transport's real DNS and HTTP
implementation. They are marked ``live`` so the standard suite makes no DNS
or network call; invoke them explicitly with ``pytest -m live``.
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from urllib.parse import urlsplit

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


def _configured_feed(key: str) -> FeedConfig:
    """Load a named feed from the tracked example, never a test-local URL."""
    configured = FeedsConfig.model_validate(tomllib.loads(_FEEDS_PATH.read_text()))
    return next(feed for feed in configured.feeds if feed.key == key)


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
            if error.category in {FailureCategory.NETWORK, FailureCategory.TIMEOUT}:
                pytest.skip(f"live network unavailable: {error.category}")
            if (
                error.category is FailureCategory.CONFIGURATION
                and isinstance(error.__cause__, UnsafeUrl)
                and "could not be resolved" in str(error.__cause__)
            ):
                pytest.skip("live DNS unavailable")
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
