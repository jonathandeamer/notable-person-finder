"""Opt-in smoke coverage for the real article fetch and extraction boundary.

Marked ``live`` so the default suite makes no network call. Invoke with::

    uv run pytest tests/coverage -m live -v

No authentication secret is required. Fetches one stable public article,
confirms ``TrafilaturaArticleExtractor`` produces ordered blocks and a title,
and confirms raw HTML never survives into the returned extract object (K3:
raw HTML is dropped before ``execute`` returns and never persisted, logged,
or sent to a model). Skips only when the public endpoint is unreachable;
extraction failures raise.
"""

from __future__ import annotations

import errno
import json
import socket
from collections.abc import Callable

import pytest

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.articles import (
    OPERATION_FETCH_ARTICLE,
    ArticleFetchSuccess,
    HttpxArticleFetcher,
    TrafilaturaArticleExtractor,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import SystemHostResolver, UnsafeUrl
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.runs.clock import SystemClock

pytestmark = pytest.mark.live

_UNREACHABLE_NETWORK_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH})
# Stable, public, mainspace: unlikely to move or vanish.
_ARTICLE_URL = "https://en.wikipedia.org/wiki/Ada_Lovelace"


def _skip_reason(error: ProviderFailure) -> str | None:
    """Name only environment failures that make a live assertion impossible."""
    if error.category is FailureCategory.TIMEOUT:
        return "live article fetch connection timed out"
    if error.category is FailureCategory.CONFIGURATION and isinstance(
        error.__cause__, UnsafeUrl
    ):
        return "live DNS unavailable"
    if error.category is not FailureCategory.NETWORK:
        return None
    cause = error.__cause__
    if isinstance(cause, socket.gaierror):
        return "live DNS unavailable"
    if isinstance(cause, ConnectionRefusedError):
        return "live connection refused"
    if isinstance(cause, OSError) and getattr(cause, "errno", None) in (
        _UNREACHABLE_NETWORK_ERRNOS
    ):
        return "live network unreachable"
    return "live network unavailable"


def _call_or_skip[T](call: Callable[[], T]) -> T:
    try:
        return call()
    except ProviderFailure as error:
        reason = _skip_reason(error)
        if reason is not None:
            pytest.skip(reason)
        raise


@pytest.mark.live
def test_live_fetch_and_extract_stable_article() -> None:
    transport = build_transport(
        TransportConfig(),
        version="0.1.0-live",
        resolver=SystemHostResolver(),
        clock=SystemClock(),
    )
    fetcher = HttpxArticleFetcher(transport, clock=SystemClock())
    try:
        result = _call_or_skip(lambda: fetcher.fetch_article(_ARTICLE_URL))
    finally:
        closer = getattr(transport, "close", None)
        if callable(closer):
            closer()

    assert isinstance(result, ArticleFetchSuccess), (
        f"expected a fetchable HTML article, got {result!r}"
    )
    raw_html_text = result.html.decode("utf-8", errors="replace").lower()
    # Positive control: the raw fetch really does carry markup, so the
    # negative assertion below on the *extracted* object means something.
    assert "<html" in raw_html_text

    extractor = TrafilaturaArticleExtractor()
    extracted = extractor.extract_article(result.html)

    assert extracted.title is not None and extracted.title.strip()
    assert len(extracted.blocks) >= 1
    # Blocks are ordered b1, b2, ... (ArticleTextBlock.id contract).
    ordinals = [int(block.id[1:]) for block in extracted.blocks]
    assert ordinals == sorted(ordinals)
    assert ordinals == list(range(1, len(ordinals) + 1))

    # K3: raw HTML must never survive into the returned extract object.
    rendered = json.dumps(
        {
            "title": extracted.title,
            "dek": extracted.dek,
            "byline": extracted.byline,
            "blocks": [block.text for block in extracted.blocks],
        }
    )
    lowered = rendered.lower()
    assert "<html" not in lowered
    assert "<script" not in lowered
    assert "<div" not in lowered

    print(
        "live_article_fetch",
        {
            "provider": "article_http",
            "operation": OPERATION_FETCH_ARTICLE,
            "requested_url": result.requested_url,
            "final_url": result.final_url,
            "status_code": result.status_code,
            "quality": extracted.quality,
            "block_count": len(extracted.blocks),
            "outcome": "ok",
        },
    )
