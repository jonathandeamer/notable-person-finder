from __future__ import annotations

import random
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
)
from notable_person_finder.runs.clock import Clock


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    ordinal: int
    outcome: str
    latency_ms: int
    failure_category: FailureCategory | None = None
    status_code: int | None = None
    retry_after_ms: int | None = None
    detail: str | None = None


class RetryExhausted(Exception):
    """Every permitted attempt for one work item failed transiently."""

    def __init__(self, last_failure: ProviderFailure, *, attempts: int) -> None:
        self.last_failure = last_failure
        self.attempts = attempts
        super().__init__(
            f"{last_failure.provider}.{last_failure.operation} exhausted "
            f"{attempts} attempts: {last_failure.category}"
        )


@dataclass(frozen=True, slots=True)
class RetryDecision:
    action: Literal["RETRY", "PERMANENT", "EXHAUSTED"]
    delay_seconds: float | None


class RetryPolicy:
    """Pure retry-adjudication logic, synchronously callable with no sleeping or I/O.

    Decisions are made here; the sleeping, calling, and bookkeeping loop lives
    in `RetryCoordinator.call`. Callers own advancing `attempt_index` and
    `malformed_retries` across attempts and must apply the returned delay
    themselves before the next attempt.
    """

    def __init__(
        self,
        config: RetryConfig,
        *,
        clock: Clock,
        random_source: random.Random | None = None,
    ) -> None:
        # `clock` is accepted, not stored: it exists only so every call site
        # that builds a `RetryPolicy` -- `RetryCoordinator`, the CLI, and the
        # tests -- can pass the same clock it already has, without a special
        # case for this one pure, non-sleeping component. Jitter and delay
        # come from `self._random` and `config`, never from wall-clock or
        # monotonic time; `RetryCoordinator` is the one that sleeps and
        # measures elapsed time, and it keeps its own `self._clock` for that.
        self._config = config
        self._random = random_source or random.Random()
        self._guard = threading.Lock()
        self._consecutive_exhaustions: dict[str, int] = {}
        self._paused: set[str] = set()

    @property
    def max_attempts(self) -> int:
        return self._config.max_attempts

    def is_paused(self, provider: str) -> bool:
        with self._guard:
            return provider in self._paused

    def paused_providers(self) -> frozenset[str]:
        with self._guard:
            return frozenset(self._paused)

    def start(self, provider: str) -> None:
        if self.is_paused(provider):
            with self._guard:
                exhaustions = self._consecutive_exhaustions.get(provider, 0)
            raise ProviderPaused(provider, consecutive_exhaustions=exhaustions)

    def decide(
        self,
        provider: str,
        *,
        failure: ProviderFailure,
        attempt_index: int,
        malformed_retries: int,
    ) -> RetryDecision:
        if not failure.retryable:
            return RetryDecision(action="PERMANENT", delay_seconds=None)

        # Adjudicated: "at most one fresh attempt" for malformed
        # structured output is a ceiling, not a guarantee.
        # `max_attempts` remains the outer bound on total calls --
        # with max_attempts=1 a malformed response gets exactly
        # one attempt and stops, because the operator's
        # configured budget must bound the number of paid
        # generations. This branch never grants an attempt beyond
        # max_attempts; it only forecloses a second malformed
        # retry early when budget would otherwise allow one.
        #
        # This is reported as `PERMANENT`, not `EXHAUSTED`: the two rules
        # are independent, not mutually exclusive, so a second malformed
        # response that also happens to land on the final budget slot must
        # still surface as an immediate answer from the provider (the
        # coordinator resets the pause counter and re-raises the original
        # failure), never as a retry-budget exhaustion that the engine would
        # classify as a deferral and repeat on a later run.
        if (
            failure.category is FailureCategory.MALFORMED_RESPONSE
            and malformed_retries >= 1
        ):
            return RetryDecision(action="PERMANENT", delay_seconds=None)

        if attempt_index + 1 >= self._config.max_attempts:
            return RetryDecision(action="EXHAUSTED", delay_seconds=None)

        return RetryDecision(
            action="RETRY", delay_seconds=self._delay(attempt_index + 1, failure)
        )

    def record_success(self, provider: str) -> None:
        # A permanent failure is a real answer from the provider, so it does not
        # count towards the consecutive-exhaustion pause either.
        with self._guard:
            self._consecutive_exhaustions[provider] = 0

    def record_exhaustion(self, provider: str) -> None:
        with self._guard:
            count = self._consecutive_exhaustions.get(provider, 0) + 1
            self._consecutive_exhaustions[provider] = count
            if count >= self._config.provider_pause_after_consecutive_exhaustions:
                self._paused.add(provider)

    def _delay(self, retry_number: int, failure: ProviderFailure) -> float:
        if failure.retry_after_ms is not None:
            # `retry_after_ms` comes from the provider's own Retry-After
            # header, which is remote input. A legal `Retry-After: 86400`
            # would otherwise park the coordinator for a day per retry WHILE
            # THE OS MUTATION LOCK IS HELD, blocking every subsequent
            # `notable run`. Provider-controlled input must never override an
            # operator-configured bound, so honour the hint only up to
            # `max_backoff_seconds`.
            return min(failure.retry_after_ms / 1000, self._config.max_backoff_seconds)
        base = self._config.initial_backoff_seconds * (
            self._config.backoff_multiplier ** (retry_number - 1)
        )
        capped = min(base, self._config.max_backoff_seconds)
        if self._config.jitter_ratio == 0:
            return capped
        spread = capped * self._config.jitter_ratio
        return max(self._random.uniform(capped - spread, capped + spread), 0.0)


class RetryCoordinator:
    """The only component permitted to start a repeat provider request.

    No production caller since the batch-claim reshape: the engine calls
    `RetryPolicy` directly and owns its own sleep-and-retry loop across
    submissions, rather than routing a single call through `.call()`.
    Retained as a regression harness for the sleep/retry/pause bookkeeping
    this class still performs correctly on its own. Removal to be considered
    in pull request 2.
    """

    def __init__(
        self,
        config: RetryConfig,
        *,
        clock: Clock,
        random_source: random.Random | None = None,
    ) -> None:
        self._clock = clock
        self._policy = RetryPolicy(config, clock=clock, random_source=random_source)

    def is_paused(self, provider: str) -> bool:
        return self._policy.is_paused(provider)

    def paused_providers(self) -> frozenset[str]:
        return self._policy.paused_providers()

    def call[T](
        self,
        provider: str,
        operation: str,
        action: Callable[[int], T],
        *,
        on_attempt: Callable[[AttemptRecord], None],
        starting_ordinal: int = 1,
    ) -> T:
        if starting_ordinal < 1:
            raise ValueError("starting_ordinal must be at least 1")
        self._policy.start(provider)

        last_failure: ProviderFailure | None = None
        malformed_retries = 0
        for attempt_index in range(self._policy.max_attempts):
            ordinal = starting_ordinal + attempt_index
            started = self._clock.monotonic()
            try:
                result = action(ordinal)
            except ProviderFailure as failure:
                on_attempt(
                    AttemptRecord(
                        ordinal=ordinal,
                        outcome="failed",
                        latency_ms=self._elapsed_ms(started),
                        failure_category=failure.category,
                        status_code=failure.status_code,
                        retry_after_ms=failure.retry_after_ms,
                        detail=failure.detail,
                    )
                )
                decision = self._policy.decide(
                    provider,
                    failure=failure,
                    attempt_index=attempt_index,
                    malformed_retries=malformed_retries,
                )
                if decision.action == "PERMANENT":
                    # Covers both a non-retryable failure and a second
                    # malformed response (foreclosed regardless of remaining
                    # attempt budget): both are immediate answers from the
                    # provider, so the original failure is re-raised as-is.
                    self._policy.record_success(provider)
                    raise
                if failure.category is FailureCategory.MALFORMED_RESPONSE:
                    malformed_retries += 1
                last_failure = failure
                if decision.action == "EXHAUSTED":
                    break
                if decision.delay_seconds is None:
                    raise RuntimeError(
                        "RetryPolicy.decide returned action=RETRY with no delay_seconds"
                    ) from None
                self._clock.sleep(decision.delay_seconds)
                continue

            on_attempt(
                AttemptRecord(
                    ordinal=ordinal,
                    outcome="succeeded",
                    latency_ms=self._elapsed_ms(started),
                )
            )
            self._policy.record_success(provider)
            return result

        assert last_failure is not None
        # The pause counter tracks transient exhaustion only. A malformed
        # response is a real answer from the provider (it served us
        # schema-nonconforming content, not unavailability), so it must not
        # count toward pausing the provider even when it happens to be the
        # failure that used up the last attempt slot.
        if last_failure.category is FailureCategory.MALFORMED_RESPONSE:
            self._policy.record_success(provider)
        else:
            self._policy.record_exhaustion(provider)
        raise RetryExhausted(last_failure, attempts=self._policy.max_attempts)

    def _elapsed_ms(self, started: float) -> int:
        return max(int((self._clock.monotonic() - started) * 1000), 0)
