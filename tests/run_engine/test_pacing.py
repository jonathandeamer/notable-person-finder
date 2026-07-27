from __future__ import annotations

import threading

import pytest

from notable_person_finder.config.models import ConcurrencyConfig, PacingConfig
from notable_person_finder.providers.pacing import build_pacing_gate
from notable_person_finder.runs.clock import FakeClock


def test_first_request_to_a_provider_does_not_sleep() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(PacingConfig(), ConcurrencyConfig(), clock=clock)
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == []


def test_second_request_waits_for_the_configured_interval() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900), ConcurrencyConfig(), clock=clock
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == [0.9]


def test_elapsed_time_is_credited_against_the_interval() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900), ConcurrencyConfig(), clock=clock
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    clock.advance(0.5)
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == [pytest.approx(0.4)]


def test_providers_are_paced_independently() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900, brave_min_interval_ms=1100),
        ConcurrencyConfig(),
        clock=clock,
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    with gate.acquire("brave", "api.search.brave.com"):
        pass
    assert clock.slept == []


def test_unpaced_providers_never_sleep() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(PacingConfig(), ConcurrencyConfig(), clock=clock)
    for _ in range(5):
        with gate.acquire("feeds", "example.com"):
            pass
    assert clock.slept == []


def test_per_origin_concurrency_is_capped() -> None:
    gate = build_pacing_gate(
        PacingConfig(), ConcurrencyConfig(per_origin=2), clock=FakeClock()
    )
    in_flight = 0
    peak = 0
    guard = threading.Lock()
    release = threading.Event()

    def worker() -> None:
        nonlocal in_flight, peak
        with gate.acquire("feeds", "example.com"):
            with guard:
                in_flight += 1
                peak = max(peak, in_flight)
            release.wait(timeout=2)
            with guard:
                in_flight -= 1

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    release.set()
    for thread in threads:
        thread.join(timeout=5)
    assert peak <= 2


def test_different_origins_do_not_share_a_slot() -> None:
    gate = build_pacing_gate(
        PacingConfig(), ConcurrencyConfig(per_origin=1), clock=FakeClock()
    )
    with gate.acquire("feeds", "a.example"), gate.acquire("feeds", "b.example"):
        pass
