from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def now(self) -> datetime: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def now(self) -> datetime:
        return datetime.now(UTC)


class FakeClock:
    """Deterministic clock for tests; no test in this suite sleeps for real."""

    def __init__(self, start: datetime | None = None) -> None:
        self._start = start or datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)
        self._elapsed = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self._elapsed

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._elapsed += seconds

    def advance(self, seconds: float) -> None:
        self._elapsed += seconds

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self._elapsed)


def utc_timestamp(moment: datetime) -> str:
    """Render an aware datetime as the ISO-8601 Zulu text SQLite stores.

    Microseconds are always emitted, including on an exact whole second where
    `isoformat` would omit them. This is load-bearing, not cosmetic: SQLite
    stores these as TEXT and compares them as TEXT, so `eligible_at <= now`
    is a string comparison. `'...:01Z' <= '...:01.5Z'` is false because
    `'Z' > '.'`, so a whole-second timestamp reads as *later* than a `now`
    that genuinely followed it within the same second -- and a due work item
    would be unclaimable until the second rolled over.

    Emitting the fractional part unconditionally makes every timestamp
    comparable to every other by the same rule. Migration `0002`'s
    `CHECK (eligible_at GLOB '*Z')` accepts the longer form, so this needs no
    schema change.
    """
    rendered = moment.astimezone(UTC).isoformat(timespec="microseconds")
    return rendered.replace("+00:00", "Z")
