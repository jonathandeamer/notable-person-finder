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
    RetryDecision,
    RetryExhausted,
    RetryPolicy,
)

NO_JITTER = RetryConfig(
    max_attempts=3,
    initial_backoff_seconds=1.0,
    max_backoff_seconds=30.0,
    backoff_multiplier=2.0,
    jitter_ratio=0.0,
)


def coordinator(
    config: RetryConfig = NO_JITTER, clock: FakeClock | None = None
) -> RetryCoordinator:
    return RetryCoordinator(config, clock=clock or FakeClock())


def failure(
    category: FailureCategory,
    *,
    retryable: bool | None = None,
    status_code: int | None = None,
    retry_after_ms: int | None = None,
    detail: str | None = None,
) -> ProviderFailure:
    return ProviderFailure(
        category,
        provider="brave",
        operation="search_web",
        retryable=retryable,
        status_code=status_code,
        retry_after_ms=retry_after_ms,
        detail=detail,
    )


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
    assert (
        coordinator(clock=clock).call(
            "brave", "search_web", action, on_attempt=records.append
        )
        == "page"
    )
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
        coordinator(config, clock).call(
            "brave", "search_web", action, on_attempt=lambda _: None
        )
    assert clock.slept == [10.0, 15.0, 15.0, 15.0]


def test_retry_after_overrides_computed_backoff() -> None:
    clock = FakeClock()

    def action(ordinal: int) -> str:
        if ordinal == 1:
            raise failure(
                FailureCategory.RATE_LIMIT, status_code=429, retry_after_ms=7000
            )
        return "page"

    coordinator(clock=clock).call(
        "brave", "search_web", action, on_attempt=lambda _: None
    )
    assert clock.slept == [7.0]


def test_retry_after_is_capped_by_the_operator_configured_maximum() -> None:
    """A provider's Retry-After header must not override an operator bound.

    `retry_after_ms` is filled from the remote's own `Retry-After` response
    header. A legal `Retry-After: 86400` on a 503 is remote input that, left
    uncapped, parks the coordinator for a day per retry while the OS mutation
    lock is held, blocking every subsequent `notable run`. The operator's
    `max_backoff_seconds` is a validated ceiling and must bind here too.
    """
    config = RetryConfig(
        max_attempts=3,
        initial_backoff_seconds=1.0,
        max_backoff_seconds=5.0,
        backoff_multiplier=2.0,
        jitter_ratio=0.0,
    )
    clock = FakeClock()
    records: list[AttemptRecord] = []

    def action(ordinal: int) -> str:
        raise failure(
            FailureCategory.TRANSIENT_SERVER_ERROR,
            status_code=503,
            retry_after_ms=86_400_000,
        )

    with pytest.raises(RetryExhausted):
        coordinator(config, clock).call(
            "brave", "search_web", action, on_attempt=records.append
        )
    assert clock.slept == [5.0, 5.0]
    assert max(clock.slept) <= config.max_backoff_seconds
    assert [record.retry_after_ms for record in records] == [86_400_000] * 3


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
        coordinator(clock=clock).call(
            "brave", "search_web", action, on_attempt=lambda _: None
        )
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
        coordination.call(
            "brave", "search_web", lambda ordinal: "page", on_attempt=lambda _: None
        )


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
    coordination.call(
        "brave", "search_web", lambda ordinal: "ok", on_attempt=lambda _: None
    )
    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    assert not coordination.is_paused("brave")


def test_a_permanent_failure_resets_the_counter_and_reraises_original() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = coordinator(config)

    def transient(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT)

    with pytest.raises(RetryExhausted):
        coordination.call("brave", "search_web", transient, on_attempt=lambda _: None)

    permanent = failure(FailureCategory.AUTHENTICATION)

    def denied(ordinal: int) -> str:
        raise permanent

    with pytest.raises(ProviderFailure) as raised:
        coordination.call("brave", "search_web", denied, on_attempt=lambda _: None)
    assert raised.value is permanent

    with pytest.raises(RetryExhausted):
        coordination.call("brave", "search_web", transient, on_attempt=lambda _: None)
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
    assert (
        coordination.call(
            "mediawiki", "search_pages", lambda ordinal: "ok", on_attempt=lambda _: None
        )
        == "ok"
    )


def test_jitter_uses_the_injected_random_source_within_configured_ratio() -> None:
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
    expected_random = random.Random(7)
    expected_sleeps = [expected_random.uniform(7.5, 12.5) for _ in range(3)]
    assert len(clock.slept) == 3
    assert all(7.5 <= delay <= 12.5 for delay in clock.slept)
    assert clock.slept == expected_sleeps


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


def test_second_malformed_on_final_budget_slot_raises_the_original_failure() -> None:
    """A second malformed response forecloses immediately, even when it also

    happens to be the last attempt the budget would have allowed. It must
    surface as the original `ProviderFailure` -- not `RetryExhausted` -- so
    the engine classifies it as a permanent failure rather than a deferral
    that would repeat a paid generation on a later run.
    """
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        )

    config = RetryConfig(max_attempts=2, jitter_ratio=0.0)
    with pytest.raises(ProviderFailure):
        coordinator(config).call(
            "openrouter", "generate_structured", action, on_attempt=lambda _: None
        )
    assert calls == [1, 2]


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


# --- RetryPolicy: pure decision object, no sleeping or I/O ---


def policy(
    config: RetryConfig = NO_JITTER, clock: FakeClock | None = None
) -> RetryPolicy:
    return RetryPolicy(config, clock=clock or FakeClock())


def test_policy_retryable_below_max_attempts_yields_retry_with_capped_delay() -> None:
    config = RetryConfig(
        max_attempts=5,
        initial_backoff_seconds=10.0,
        max_backoff_seconds=15.0,
        backoff_multiplier=10.0,
        jitter_ratio=0.0,
    )
    decision = policy(config).decide(
        "brave",
        failure=failure(FailureCategory.TIMEOUT),
        attempt_index=1,
        malformed_retries=0,
    )
    assert decision == RetryDecision(action="RETRY", delay_seconds=15.0)


def test_policy_non_retryable_failure_yields_permanent() -> None:
    decision = policy().decide(
        "brave",
        failure=failure(FailureCategory.AUTHENTICATION),
        attempt_index=0,
        malformed_retries=0,
    )
    assert decision == RetryDecision(action="PERMANENT", delay_seconds=None)


def test_policy_final_attempt_yields_exhausted() -> None:
    decision = policy().decide(
        "brave",
        failure=failure(FailureCategory.TIMEOUT),
        attempt_index=2,
        malformed_retries=0,
    )
    assert decision == RetryDecision(action="EXHAUSTED", delay_seconds=None)


def test_policy_second_malformed_response_yields_permanent_not_retry() -> None:
    decision = policy().decide(
        "openrouter",
        failure=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        ),
        attempt_index=0,
        malformed_retries=1,
    )
    assert decision == RetryDecision(action="PERMANENT", delay_seconds=None)


def test_policy_second_malformed_on_final_budget_slot_still_yields_permanent() -> None:
    """The malformed-foreclosure rule and budget exhaustion are independent,

    not mutually exclusive: a second malformed response on the very last
    attempt slot must still be reported as an immediate foreclosure
    (`PERMANENT`), not folded into `EXHAUSTED` just because the budget also
    happens to be spent on this attempt.
    """
    config = RetryConfig(max_attempts=2, jitter_ratio=0.0)
    decision = policy(config).decide(
        "openrouter",
        failure=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        ),
        attempt_index=config.max_attempts - 1,
        malformed_retries=1,
    )
    assert decision == RetryDecision(action="PERMANENT", delay_seconds=None)


def test_policy_first_malformed_response_yields_retry() -> None:
    config = RetryConfig(max_attempts=5, jitter_ratio=0.0)
    decision = policy(config).decide(
        "openrouter",
        failure=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        ),
        attempt_index=0,
        malformed_retries=0,
    )
    assert decision.action == "RETRY"


def test_policy_record_exhaustion_pauses_at_the_configured_threshold() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = policy(config)
    coordination.record_exhaustion("brave")
    assert not coordination.is_paused("brave")
    coordination.record_exhaustion("brave")
    assert coordination.is_paused("brave")
    assert coordination.paused_providers() == frozenset({"brave"})


def test_policy_malformed_exhaustion_does_not_pause() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=1,
    )
    coordination = policy(config)
    decision = coordination.decide(
        "openrouter",
        failure=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        ),
        attempt_index=0,
        malformed_retries=0,
    )
    assert decision.action == "EXHAUSTED"
    # The coordinator's `call` loop is responsible for distinguishing a
    # malformed-response exhaustion from a transient one and calling
    # `record_success` instead of `record_exhaustion` in that case -- the
    # policy itself does not decide which to call from `decide` alone.
    coordination.record_success("openrouter")
    assert not coordination.is_paused("openrouter")


def test_policy_start_raises_provider_paused_when_paused() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=1,
    )
    coordination = policy(config)
    coordination.record_exhaustion("brave")
    assert coordination.is_paused("brave")
    with pytest.raises(ProviderPaused):
        coordination.start("brave")
