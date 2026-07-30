"""Opt-in smoke coverage for the real MediaWiki Action API boundary.

Marked ``live`` so the default suite makes no network call. Invoke with::

    uv run pytest tests/wikipedia -m live -v

No authentication secret is required. Skips only when the public endpoint is
unreachable; schema/parse failures raise.
"""

from __future__ import annotations

import errno
import socket
from collections.abc import Callable

import httpx
import pytest

from notable_person_finder.config.models import MediaWikiConfig, TransportConfig
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.mediawiki import (
    PAGE_FACTS_OPERATION,
    SEARCH_OPERATION,
    HttpxMediaWikiClient,
)
from notable_person_finder.providers.safety import SystemHostResolver, UnsafeUrl
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.runs.clock import SystemClock

_UNREACHABLE_NETWORK_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH})
_ENDPOINT = "https://en.wikipedia.org/w/api.php"


def _failure_caused_by(
    category: FailureCategory, cause: Exception, *, operation: str
) -> ProviderFailure:
    failure = ProviderFailure(category, provider="mediawiki", operation=operation)
    failure.__cause__ = cause
    return failure


def _connect_error_caused_by(cause: Exception) -> httpx.ConnectError:
    connect_error = httpx.ConnectError("connect failed")
    connect_error.__cause__ = cause
    return connect_error


def _skip_reason(error: ProviderFailure) -> str | None:
    """Name only environment failures that make a live assertion impossible."""
    if error.category is FailureCategory.TIMEOUT:
        if isinstance(error.__cause__, httpx.ReadTimeout):
            return None
        return "live MediaWiki connection timed out"
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
    current: BaseException | None = cause
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, socket.gaierror):
            return "live DNS unavailable"
        if isinstance(current, ConnectionRefusedError):
            return "live connection refused"
        if (
            isinstance(current, OSError)
            and getattr(current, "errno", None) in _UNREACHABLE_NETWORK_ERRNOS
        ):
            return "live network unreachable"
        current = current.__cause__ or current.__context__
    if isinstance(cause, httpx.ConnectError):
        return "live MediaWiki connection failed"
    return "live network unavailable"


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
    assert (
        _skip_reason(_failure_caused_by(category, cause, operation=SEARCH_OPERATION))
        is not None
    )


@pytest.mark.parametrize(
    ("category", "cause"),
    [
        (FailureCategory.TIMEOUT, httpx.ReadTimeout("read timed out")),
        (FailureCategory.MALFORMED_RESPONSE, httpx.RemoteProtocolError("bad HTTP")),
    ],
)
def test_deterministic_transport_failures_have_no_skip_reason(
    category: FailureCategory, cause: Exception
) -> None:
    assert (
        _skip_reason(_failure_caused_by(category, cause, operation=SEARCH_OPERATION))
        is None
    )


def _live_client() -> tuple[HttpxMediaWikiClient, object]:
    transport = build_transport(
        TransportConfig(),
        version="0.1.0-live",
        resolver=SystemHostResolver(),
        clock=SystemClock(),
    )
    client = HttpxMediaWikiClient(
        transport,
        config=MediaWikiConfig(endpoint=_ENDPOINT),
        srlimit=5,
        max_extract_characters=400,
        max_categories_per_page=10,
        clock=SystemClock(),
    )
    return client, transport


def _call_or_skip[T](operation: str, call: Callable[[], T]) -> T:
    del operation  # used only for documentation of the failure path
    try:
        return call()
    except ProviderFailure as error:
        reason = _skip_reason(error)
        if reason is not None:
            pytest.skip(reason)
        raise


@pytest.mark.live
def test_live_search_pages_returns_hits_or_empty() -> None:
    client, transport = _live_client()
    try:
        page = _call_or_skip(
            SEARCH_OPERATION,
            lambda: client.search_pages("Ada Lovelace", continuation=None),
        )
    finally:
        closer = getattr(transport, "close", None)
        if callable(closer):
            closer()
    assert page.provider_total_hits is None or page.provider_total_hits >= 0
    assert isinstance(page.hits, tuple)


@pytest.mark.live
def test_live_page_facts_for_known_page() -> None:
    """Ada Lovelace is stable mainspace page id 1012 on enwiki (public)."""
    client, transport = _live_client()
    try:
        batch = _call_or_skip(
            PAGE_FACTS_OPERATION,
            lambda: client.get_page_facts([1012]),
        )
    finally:
        closer = getattr(transport, "close", None)
        if callable(closer):
            closer()
    assert len(batch.pages) >= 1
    page = batch.pages[0]
    assert page.page_id == 1012
    assert page.missing is False
    assert page.namespace == 0
