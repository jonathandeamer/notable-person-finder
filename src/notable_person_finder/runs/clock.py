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
    """Render an aware datetime as the ISO-8601 Zulu text SQLite stores."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")
