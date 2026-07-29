from __future__ import annotations

from datetime import UTC, datetime

from notable_person_finder.runs.clock import FakeClock, SystemClock, utc_timestamp


def test_utc_timestamp_is_normalized_iso_8601_zulu() -> None:
    moment = datetime(2026, 7, 25, 6, 0, 0, 123456, tzinfo=UTC)
    assert utc_timestamp(moment) == "2026-07-25T06:00:00.123456Z"


def test_utc_timestamp_emits_microseconds_on_an_exact_whole_second() -> None:
    """`isoformat` drops the fractional part when it is zero; this must not.

    A whole second is the one input where the format could vary in width, and
    a varying width is what breaks the TEXT comparisons these values are
    stored for.
    """
    moment = datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)
    assert utc_timestamp(moment) == "2026-07-25T06:00:00.000000Z"


def test_utc_timestamp_text_order_matches_chronological_order() -> None:
    """The invariant the whole fixed-width format exists to provide.

    SQLite stores these as TEXT and `eligible_at <= now` compares them as
    TEXT. Before microseconds were unconditional, a whole-second timestamp
    sorted *after* a later timestamp in the same second, because `'Z' > '.'`
    -- so a due work item read as not yet due and stayed unclaimable until
    the second rolled over. Sorting the rendered text must be the same as
    sorting the instants, with no exception at the second boundary.
    """
    moments = [
        datetime(2026, 7, 25, 6, 0, 0, 0, tzinfo=UTC),
        datetime(2026, 7, 25, 6, 0, 0, 1, tzinfo=UTC),
        datetime(2026, 7, 25, 6, 0, 0, 500000, tzinfo=UTC),
        datetime(2026, 7, 25, 6, 0, 0, 999999, tzinfo=UTC),
        datetime(2026, 7, 25, 6, 0, 1, 0, tzinfo=UTC),
    ]
    rendered = [utc_timestamp(moment) for moment in moments]
    assert rendered == sorted(rendered)
    # The specific comparison the claim predicate makes, spelled out: an item
    # eligible on the whole second is due at any later instant in that second.
    assert rendered[0] <= rendered[2]


def test_system_clock_now_is_timezone_aware_utc() -> None:
    assert SystemClock().now().tzinfo == UTC


def test_fake_clock_sleep_advances_without_real_delay() -> None:
    clock = FakeClock(start=datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC))
    assert clock.monotonic() == 0.0
    clock.sleep(2.5)
    assert clock.monotonic() == 2.5
    assert clock.now() == datetime(2026, 7, 25, 6, 0, 2, 500000, tzinfo=UTC)
    assert clock.slept == [2.5]
