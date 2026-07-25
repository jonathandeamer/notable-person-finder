from __future__ import annotations

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
)
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.retry import (
    AttemptRecord,
    RetryCoordinator,
    RetryExhausted,
)

NO_JITTER = RetryConfig(
    max_attempts=3,
    initial_backoff_seconds=1.0,
    max_backoff_seconds=30.0,
    backoff_multiplier=2.0,
    jitter_ratio=0.0,
)


def coordinator(config: RetryConfig = NO_JITTER, clock: FakeClock | None = None) -> RetryCoordinator:
    return RetryCoordinator(config, clock=clock or FakeClock())


def failure(category: FailureCategory, **kwargs: object) -> ProviderFailure:
    return ProviderFailure(category, provider="brave", operation="search_web", **kwargs)


def test_successful_call_records_one_attempt() -> None:
    records: list[AttemptRecord] = []
    result = coordinator().call(
        "brave", "search_web", lambda ordinal: "page", on_attempt=records.append
    )
    assert result == "page"
    assert [(r.ordinal, r.outcome) for r in records] == [(1, "succeeded")]


def test_transient_failure_is_retried_until_success() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        if ordinal < 3:
            raise failure(FailureCategory.TRANSIENT_SERVER_ERROR, status_code=500)
        return "page"

    records: list[AttemptRecord] = []
    clock = FakeClock()
    assert coordinator(clock=clock).call(
        "brave", "search_web", action, on_attempt=records.append
    ) == "page"
    assert calls == [1, 2, 3]
    assert [r.outcome for r in records] == ["failed", "failed", "succeeded"]
    assert clock.slept == [1.0, 2.0]


def test_backoff_is_capped() -> None:
    config = RetryConfig(
        max_attempts=5,
        initial_backoff_seconds=10.0,
        max_backoff_seconds=15.0,
        backoff_multiplier=10.0,
        jitter_ratio=0.0,
    )
    clock = FakeClock()

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT)

    with pytest.raises(RetryExhausted):
        coordinator(config, clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert clock.slept == [10.0, 15.0, 15.0, 15.0]


def test_retry_after_overrides_computed_backoff() -> None:
    clock = FakeClock()

    def action(ordinal: int) -> str:
        if ordinal == 1:
            raise failure(FailureCategory.RATE_LIMIT, status_code=429, retry_after_ms=7000)
        return "page"

    coordinator(clock=clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert clock.slept == [7.0]


@pytest.mark.parametrize(
    "category",
    [
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.ACCESS_DENIED,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.RESPONSE_TOO_LARGE,
    ],
)
def test_permanent_failures_are_not_retried(category: FailureCategory) -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise failure(category)

    clock = FakeClock()
    with pytest.raises(ProviderFailure) as raised:
        coordinator(clock=clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert raised.value.category is category
    assert calls == [1]
    assert clock.slept == []


def test_malformed_output_marked_retryable_gets_exactly_one_more_attempt() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        )

    with pytest.raises(ProviderFailure):
        coordinator().call(
            "openrouter", "generate_structured", action, on_attempt=lambda _: None
        )
    assert calls == [1, 2]


def test_exhaustion_raises_retry_exhausted_with_the_last_failure() -> None:
    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT, status_code=408)

    with pytest.raises(RetryExhausted) as raised:
        coordinator().call("brave", "search_web", action, on_attempt=lambda _: None)
    assert raised.value.attempts == 3
    assert raised.value.last_failure.category is FailureCategory.TIMEOUT


def test_provider_pauses_after_consecutive_exhaustions() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = coordinator(config)

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.PROVIDER_UNAVAILABLE)

    for _ in range(2):
        with pytest.raises(RetryExhausted):
            coordination.call("brave", "search_web", action, on_attempt=lambda _: None)

    assert coordination.is_paused("brave")
    assert coordination.paused_providers() == frozenset({"brave"})
    with pytest.raises(ProviderPaused):
        coordination.call("brave", "search_web", lambda ordinal: "page", on_attempt=lambda _: None)


def test_a_success_resets_the_consecutive_exhaustion_counter() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = coordinator(config)

    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    coordination.call("brave", "search_web", lambda ordinal: "ok", on_attempt=lambda _: None)
    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    assert not coordination.is_paused("brave")


def test_pausing_one_provider_does_not_pause_another() -> None:
    config = RetryConfig(
        max_attempts=1, jitter_ratio=0.0, provider_pause_after_consecutive_exhaustions=1
    )
    coordination = coordinator(config)
    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    assert coordination.is_paused("brave")
    assert not coordination.is_paused("mediawiki")
    assert coordination.call(
        "mediawiki", "search_pages", lambda ordinal: "ok", on_attempt=lambda _: None
    ) == "ok"


def test_jitter_stays_within_the_configured_ratio() -> None:
    import random

    config = RetryConfig(
        max_attempts=4,
        initial_backoff_seconds=10.0,
        max_backoff_seconds=100.0,
        backoff_multiplier=1.0,
        jitter_ratio=0.25,
    )
    clock = FakeClock()
    coordination = RetryCoordinator(config, clock=clock, random_source=random.Random(7))

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT)

    with pytest.raises(RetryExhausted):
        coordination.call("brave", "search_web", action, on_attempt=lambda _: None)
    assert len(clock.slept) == 3
    assert all(7.5 <= delay <= 12.5 for delay in clock.slept)


def test_attempt_records_carry_status_and_latency() -> None:
    clock = FakeClock()
    records: list[AttemptRecord] = []

    def action(ordinal: int) -> str:
        clock.advance(0.25)
        raise failure(FailureCategory.RATE_LIMIT, status_code=429, retry_after_ms=1000)

    with pytest.raises(RetryExhausted):
        coordinator(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock).call(
            "brave", "search_web", action, on_attempt=records.append
        )
    assert records[0].status_code == 429
    assert records[0].retry_after_ms == 1000
    assert records[0].latency_ms == 250
    assert records[0].failure_category is FailureCategory.RATE_LIMIT


def test_malformed_response_at_max_attempts_one_does_not_pause_the_provider() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        )

    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=1,
    )
    coordination = coordinator(config)
    with pytest.raises(RetryExhausted):
        coordination.call(
            "openrouter", "generate_structured", action, on_attempt=lambda _: None
        )
    assert calls == [1]
    assert not coordination.is_paused("openrouter")


def test_malformed_response_on_final_budget_slot_does_not_pause_the_provider() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        if ordinal < 3:
            raise failure(FailureCategory.TRANSIENT_SERVER_ERROR)
        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="brave",
            operation="search_web",
            retryable=True,
        )

    config = RetryConfig(
        max_attempts=3,
        initial_backoff_seconds=1.0,
        max_backoff_seconds=30.0,
        backoff_multiplier=2.0,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=1,
    )
    clock = FakeClock()
    coordination = coordinator(config, clock)
    with pytest.raises(RetryExhausted) as raised:
        coordination.call("brave", "search_web", action, on_attempt=lambda _: None)
    assert calls == [1, 2, 3]
    assert raised.value.last_failure.category is FailureCategory.MALFORMED_RESPONSE
    assert not coordination.is_paused("brave")


def test_starting_ordinal_with_a_retry_keeps_delays_keyed_to_retry_count() -> None:
    records: list[AttemptRecord] = []
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        if ordinal == 10:
            raise failure(FailureCategory.TRANSIENT_SERVER_ERROR)
        return "page"

    clock = FakeClock()
    result = coordinator(clock=clock).call(
        "brave",
        "search_web",
        action,
        on_attempt=records.append,
        starting_ordinal=10,
    )
    assert result == "page"
    assert calls == [10, 11]
    assert [r.ordinal for r in records] == [10, 11]
    assert clock.slept == [1.0]


def test_persisted_ordinals_can_continue_after_an_interrupted_attempt() -> None:
    records: list[AttemptRecord] = []
    calls: list[int] = []
    result = coordinator().call(
        "brave",
        "search_web",
        lambda ordinal: calls.append(ordinal) or "page",
        on_attempt=records.append,
        starting_ordinal=4,
    )
    assert result == "page"
    assert calls == [4]
    assert records[0].ordinal == 4
