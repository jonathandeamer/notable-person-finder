from __future__ import annotations

import random
import threading
from collections.abc import Callable
from dataclasses import dataclass

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


class RetryCoordinator:
    """The only component permitted to start a repeat provider request."""

    def __init__(
        self,
        config: RetryConfig,
        *,
        clock: Clock,
        random_source: random.Random | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._random = random_source or random.Random()
        self._guard = threading.Lock()
        self._consecutive_exhaustions: dict[str, int] = {}
        self._paused: set[str] = set()

    def is_paused(self, provider: str) -> bool:
        with self._guard:
            return provider in self._paused

    def paused_providers(self) -> frozenset[str]:
        with self._guard:
            return frozenset(self._paused)

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
        if self.is_paused(provider):
            with self._guard:
                exhaustions = self._consecutive_exhaustions.get(provider, 0)
            raise ProviderPaused(provider, consecutive_exhaustions=exhaustions)

        last_failure: ProviderFailure | None = None
        malformed_retries = 0
        for attempt_index in range(self._config.max_attempts):
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
                if not failure.retryable:
                    self._record_success_or_permanent(provider)
                    raise
                if failure.category is FailureCategory.MALFORMED_RESPONSE:
                    if malformed_retries >= 1:
                        self._record_success_or_permanent(provider)
                        raise
                    malformed_retries += 1
                last_failure = failure
                if attempt_index + 1 < self._config.max_attempts:
                    self._clock.sleep(self._delay(attempt_index + 1, failure))
                continue

            on_attempt(
                AttemptRecord(
                    ordinal=ordinal,
                    outcome="succeeded",
                    latency_ms=self._elapsed_ms(started),
                )
            )
            self._record_success_or_permanent(provider)
            return result

        assert last_failure is not None
        self._record_exhaustion(provider)
        raise RetryExhausted(last_failure, attempts=self._config.max_attempts)

    def _elapsed_ms(self, started: float) -> int:
        return max(int((self._clock.monotonic() - started) * 1000), 0)

    def _delay(self, retry_number: int, failure: ProviderFailure) -> float:
        if failure.retry_after_ms is not None:
            return failure.retry_after_ms / 1000
        base = self._config.initial_backoff_seconds * (
            self._config.backoff_multiplier ** (retry_number - 1)
        )
        capped = min(base, self._config.max_backoff_seconds)
        if self._config.jitter_ratio == 0:
            return capped
        spread = capped * self._config.jitter_ratio
        return max(self._random.uniform(capped - spread, capped + spread), 0.0)

    def _record_success_or_permanent(self, provider: str) -> None:
        # A permanent failure is a real answer from the provider, so it does not
        # count towards the consecutive-exhaustion pause either.
        with self._guard:
            self._consecutive_exhaustions[provider] = 0

    def _record_exhaustion(self, provider: str) -> None:
        with self._guard:
            count = self._consecutive_exhaustions.get(provider, 0) + 1
            self._consecutive_exhaustions[provider] = count
            if count >= self._config.provider_pause_after_consecutive_exhaustions:
                self._paused.add(provider)
