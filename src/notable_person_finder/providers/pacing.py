from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from notable_person_finder.config.models import ConcurrencyConfig, PacingConfig
from notable_person_finder.runs.clock import Clock


class PacingGate:
    """Spaces provider request starts and caps concurrency per origin."""

    def __init__(
        self,
        *,
        intervals_ms: Mapping[str, int],
        per_origin: int,
        clock: Clock,
    ) -> None:
        self._intervals_ms = dict(intervals_ms)
        self._per_origin = per_origin
        self._clock = clock
        self._guard = threading.Lock()
        self._last_start: dict[str, float] = {}
        self._origin_slots: dict[str, threading.BoundedSemaphore] = {}

    def _semaphore(self, host: str) -> threading.BoundedSemaphore:
        with self._guard:
            slot = self._origin_slots.get(host)
            if slot is None:
                slot = threading.BoundedSemaphore(self._per_origin)
                self._origin_slots[host] = slot
            return slot

    def _wait_for_interval(self, provider: str) -> None:
        interval = self._intervals_ms.get(provider, 0) / 1000
        if interval <= 0:
            return
        with self._guard:
            previous = self._last_start.get(provider)
            now = self._clock.monotonic()
            delay = 0.0 if previous is None else interval - (now - previous)
            self._last_start[provider] = now + max(delay, 0.0)
        if delay > 0:
            self._clock.sleep(delay)

    @contextmanager
    def acquire(self, provider: str, host: str) -> Iterator[None]:
        slot = self._semaphore(host)
        slot.acquire()
        try:
            self._wait_for_interval(provider)
            yield
        finally:
            slot.release()


def build_pacing_gate(
    pacing: PacingConfig, concurrency: ConcurrencyConfig, *, clock: Clock
) -> PacingGate:
    return PacingGate(
        intervals_ms={
            "mediawiki": pacing.mediawiki_min_interval_ms,
            "brave": pacing.brave_min_interval_ms,
        },
        per_origin=concurrency.per_origin,
        clock=clock,
    )
