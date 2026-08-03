"""Feed fetching and normalization."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser

from notable.config import Config
from notable.errors import ProviderFailure
from notable.http import Transport
from notable.store import Store

logger = logging.getLogger(__name__)

_TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid")


@dataclass(frozen=True, slots=True)
class SourceItem:
    url: str
    title: str | None
    summary: str | None
    published_at: str | None
    feed_key: str
    publisher_label: str


def canonical_url(raw: str) -> str | None:
    """Normalize a link, or return None if it is not a usable http(s) URL."""
    if not raw:
        return None
    parts = urlsplit(raw.strip())
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return None
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith(_TRACKING_PREFIXES)
    ]
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path,
            urlencode(sorted(query)),
            "",
        )
    )


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def fetch_new(
    config: Config, transport: Transport, store: Store, *, fresh: bool = False
) -> Iterator[SourceItem]:
    """Yield eligible items from every configured feed.

    One feed failing must not stop the others: a single publisher's outage is
    not a reason to produce no digest.
    """
    emitted: set[str] = set()
    for feed in config.feeds:
        try:
            response = transport.request(
                provider="feed",
                method="GET",
                url=feed.url,
                ttl_seconds=config.cache.feed_ttl_seconds,
                # Not `ttl_seconds=None`: that means "never expires", so it
                # would make the cached feed *more* permanent. A bypass skips
                # the read and stores the fresh response over the stale one.
                bypass_cache=fresh,
                # A feed is not a success until feedparser can read it. Cached
                # on arrival, a truncated or malformed 200 becomes a twelve-
                # hour "success" that silently yields no items -- the same
                # defect as caching an unvalidated model response, and harder
                # to notice because it produces no error at all.
                defer_cache=True,
            )
        except ProviderFailure:
            logger.warning("feed fetch failed: %s", feed.key, exc_info=True)
            continue

        parsed = feedparser.parse(response.text)
        if parsed.bozo and not parsed.entries:
            # Bozo alone is not disqualifying -- real feeds carry minor XML
            # defects and parse fine. Bozo *and* nothing extracted means the
            # response was not usable, so it must not be stored.
            logger.warning(
                "feed did not parse and yielded no entries: %s (%s)",
                feed.key,
                parsed.get("bozo_exception"),
            )
            continue
        response.commit()

        for entry in parsed.entries:
            url = canonical_url(getattr(entry, "link", "") or "")
            if url is None or url in emitted:
                continue
            if not store.is_eligible(url, max_attempts=config.max_item_attempts):
                continue
            emitted.add(url)
            yield SourceItem(
                url=url,
                title=_text(getattr(entry, "title", None)),
                summary=_text(getattr(entry, "summary", None)),
                published_at=_text(getattr(entry, "published", None)),
                feed_key=feed.key,
                publisher_label=feed.label,
            )
