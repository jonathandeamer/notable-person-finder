"""Opt-in smoke coverage for the real Brave Web Search boundary.

Marked ``live`` so the default suite makes no network call. Invoke with::

    BRAVE_API_KEY=... uv run pytest tests/coverage -m live -v

This is the only test that can catch a Brave contract break, including a
tier that rejects ``extra_snippets``: Brave's free and base tiers reject that
parameter with a 4xx, which the shared transport maps to
``FailureCategory.CONFIGURATION`` -- permanent. ``BraveConfig.extra_snippets``
defaults to ``False`` so a free-tier key still works; this smoke turns the
knob on deliberately so a tier that rejects it fails loudly here rather than
silently passing an unrelated assertion (finding I9). Only genuine
environment unavailability (DNS, unreachable network, connect timeout) skips;
every other failure -- including a tier rejection -- is allowed to raise.
"""

from __future__ import annotations

import errno
import os
import socket
from collections.abc import Callable

import pytest

from notable_person_finder.config.models import BraveConfig, TransportConfig
from notable_person_finder.providers.brave import (
    OPERATION_SEARCH_WEB,
    HttpxBraveWebSearchClient,
    SearchPage,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import SystemHostResolver, UnsafeUrl
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.runs.clock import SystemClock

pytestmark = pytest.mark.live

_API_KEY_ENV = "BRAVE_API_KEY"
_UNREACHABLE_NETWORK_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH})


def _require_key() -> str:
    key = os.environ.get(_API_KEY_ENV)
    if not key:
        pytest.skip(f"{_API_KEY_ENV} is unset; live Brave search smoke deselected")
    return key


def _skip_reason(error: ProviderFailure) -> str | None:
    """Name only environment failures that make a live assertion impossible.

    A tier rejection of ``extra_snippets`` (or any other configuration
    problem the operator's own account causes) is deliberately NOT named
    here: it must raise and fail the test, not disappear as a skip.
    """
    if error.category is FailureCategory.TIMEOUT:
        return "live Brave connection timed out"
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


def _live_client(*, api_key: str) -> tuple[HttpxBraveWebSearchClient, object]:
    transport = build_transport(
        TransportConfig(),
        version="0.1.0-live",
        resolver=SystemHostResolver(),
        clock=SystemClock(),
    )
    client = HttpxBraveWebSearchClient(
        transport,
        # extra_snippets=True deliberately: this is the parameter Brave's
        # free/base tiers reject (finding I9). A rejection must raise, not
        # be swallowed by _skip_reason above.
        config=BraveConfig(extra_snippets=True),
        api_key=api_key,
        clock=SystemClock(),
    )
    return client, transport


def _call_or_skip[T](call: Callable[[], T]) -> T:
    try:
        return call()
    except ProviderFailure as error:
        reason = _skip_reason(error)
        if reason is not None:
            pytest.skip(reason)
        raise


@pytest.mark.live
def test_live_search_web_returns_ordered_results_with_extra_snippets() -> None:
    key = _require_key()
    client, transport = _live_client(api_key=key)
    try:
        page: SearchPage = _call_or_skip(
            lambda: client.search_web("Ada Lovelace", count=5, offset=0)
        )
    finally:
        closer = getattr(transport, "close", None)
        if callable(closer):
            closer()

    assert isinstance(page.results, tuple)
    assert len(page.results) >= 1

    # Do not assert on which result, or on any specific title/URL -- only on
    # structure: at least one hit must carry a URL, a title, and a rank.
    qualifying = next(
        (result for result in page.results if result.title is not None), None
    )
    assert qualifying is not None, "no result carried a title"
    assert isinstance(qualifying.url, str) and qualifying.url
    assert isinstance(qualifying.rank, int) and qualifying.rank >= 1

    # Ranks are ordered 1..N within the page (SearchResult.rank contract).
    assert [result.rank for result in page.results] == list(
        range(1, len(page.results) + 1)
    )

    print(
        "live_brave_search",
        {
            "provider": "brave",
            "operation": OPERATION_SEARCH_WEB,
            "query": page.query,
            "altered_query": page.altered_query,
            "result_count": len(page.results),
            "more_results": page.more_results,
            "outcome": "ok",
        },
    )
