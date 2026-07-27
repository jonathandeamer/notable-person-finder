from __future__ import annotations

from datetime import UTC, datetime

from notable_person_finder.runs.clock import FakeClock, SystemClock, utc_timestamp


def test_utc_timestamp_is_normalized_iso_8601_zulu() -> None:
    moment = datetime(2026, 7, 25, 6, 0, 0, 123456, tzinfo=UTC)
    assert utc_timestamp(moment) == "2026-07-25T06:00:00.123456Z"


def test_system_clock_now_is_timezone_aware_utc() -> None:
    assert SystemClock().now().tzinfo == UTC


def test_fake_clock_sleep_advances_without_real_delay() -> None:
    clock = FakeClock(start=datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC))
    assert clock.monotonic() == 0.0
    clock.sleep(2.5)
    assert clock.monotonic() == 2.5
    assert clock.now() == datetime(2026, 7, 25, 6, 0, 2, 500000, tzinfo=UTC)
    assert clock.slept == [2.5]
