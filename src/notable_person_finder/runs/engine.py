from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from notable_person_finder.obs.logging import EVENT_LOGGER_NAME, log_event
from notable_person_finder.providers.failures import ProviderFailure, ProviderPaused
from notable_person_finder.runs import repository
from notable_person_finder.runs.budget import BudgetExhausted
from notable_person_finder.runs.clock import Clock, utc_timestamp
from notable_person_finder.runs.models import (
    RunCounters,
    RunState,
    WorkItem,
    WorkState,
)
from notable_person_finder.runs.retry import (
    AttemptRecord,
    RetryCoordinator,
    RetryExhausted,
)
from notable_person_finder.runs.scheduler import BoundedScheduler

# A handler must hand back a state that takes the item out of the claimable
# set. Anything else (pending, running) would leave the item eligible and the
# engine's peek-and-settle loop would re-select it forever.
SETTLING_WORK_STATES = frozenset(
    {
        WorkState.SUCCEEDED,
        WorkState.DEFERRED,
        WorkState.FAILED_PERMANENT,
        WorkState.SUPERSEDED,
    }
)


class NonSettlingStateError(ValueError):
    """A handler returned a work state that does not settle the item."""

    def __init__(self, run_id: int, task_type: str, state: WorkState) -> None:
        self.run_id = run_id
        self.task_type = task_type
        self.state = state
        super().__init__(
            f"handler for {task_type!r} returned {state!r}, "
            "which is not a settling state"
        )


@dataclass(frozen=True, slots=True)
class TaskOutcome:
    state: WorkState
    reason: str | None
    response_bytes: int | None = None
    provider_request_id: str | None = None
    detail_json: str | None = None
    actual_nano_usd: int | None = None


@dataclass(frozen=True, slots=True)
class TaskHandler:
    task_type: str
    provider: str
    operation: str
    execute: Callable[[WorkItem, int], TaskOutcome]
    request_fingerprint: Callable[[WorkItem], str] | None = None
    reserved_nano_usd: int = 0


@dataclass(frozen=True, slots=True)
class ReportArtifact:
    path: str | None
    sha256: str | None
    markdown: str


@dataclass(frozen=True, slots=True)
class RunReport:
    # TODO(milestone 3): add budget fields (reserved_nano_usd, actual_nano_usd,
    # deferred_reason) so the digest and `notable status` can explain why work
    # was deferred. Currently unreachable because no handlers are registered.
    run_id: int
    state: RunState
    started_at: str
    finished_at: str
    timezone: str
    window_start: str
    window_end: str
    counters: RunCounters
    paused_providers: frozenset[str]
    interrupted_runs: tuple[int, ...] = ()
    failure_categories: Mapping[str, int] = field(default_factory=dict)

    @property
    def human_id(self) -> str:
        return f"run-{self.run_id}"


def derive_run_state(
    *,
    required_pending: int,
    required_deferred: int,
    required_failed_permanent: int,
    meaningful_results: bool,
    reporting_failed: bool,
) -> RunState:
    """Derive state from this run's durable outcomes and reporting result."""
    if reporting_failed:
        return RunState.FAILED
    if required_failed_permanent > 0:
        return RunState.PARTIAL if meaningful_results else RunState.FAILED
    if required_pending > 0 or required_deferred > 0:
        return RunState.PARTIAL
    return RunState.COMPLETE


class RunEngine:
    """Owns the run lifecycle: sweep, claim, execute, persist, and report."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        retry: RetryCoordinator,
        scheduler: BoundedScheduler,
        clock: Clock,
        timezone: str,
        window_start: str,
        budget_limit_nano_usd: int | None,
        snapshot_fingerprint: str,
        snapshot_json: str,
        reporter: Callable[[RunReport], ReportArtifact],
        logger: logging.Logger | None = None,
    ) -> None:
        self._connection = connection
        self._retry = retry
        # Held for milestones 3-6, which submit independent per-subject calls
        # through it. Milestone 2's synthetic handlers execute sequentially;
        # the scheduler's own bounded-concurrency contract is proven by its
        # own tests. Its pool belongs to whoever constructed it, so the engine
        # never closes it.
        self._scheduler = scheduler
        self._clock = clock
        self._timezone = timezone
        self._window_start = window_start
        self._budget_limit_nano_usd = budget_limit_nano_usd
        self._snapshot_fingerprint = snapshot_fingerprint
        self._snapshot_json = snapshot_json
        self._reporter = reporter
        self._logger = logger or logging.getLogger(EVENT_LOGGER_NAME)
        self._failure_categories_lock = threading.Lock()

    def _now(self) -> str:
        return utc_timestamp(self._clock.now())

    def execute(self, handlers: Mapping[str, TaskHandler]) -> RunReport:
        started_at = self._now()
        sweep = repository.sweep_interrupted(self._connection, now=started_at)

        snapshot_id = repository.store_snapshot(
            self._connection,
            fingerprint=self._snapshot_fingerprint,
            canonical_json=self._snapshot_json,
            now=started_at,
        )
        run_id = repository.create_run(
            self._connection,
            snapshot_id=snapshot_id,
            timezone=self._timezone,
            window_start=self._window_start,
            window_end=started_at,
            budget_limit_nano_usd=self._budget_limit_nano_usd,
            now=started_at,
        )
        log_event(
            self._logger,
            "run.started",
            run_id=run_id,
            window_start=self._window_start,
            window_end=started_at,
            swept_runs=list(sweep.runs),
            task_types=sorted(handlers),
        )

        failure_categories: dict[str, int] = {}
        while True:
            item = repository.next_eligible(
                self._connection,
                run_id=run_id,
                now=self._now(),
                task_types=handlers.keys(),
            )
            if item is None:
                break
            handler = handlers[item.task_type]
            self._perform(run_id, item, handler, failure_categories)

        finished_at = self._now()
        counters = repository.run_counters(
            self._connection, run_id=run_id, now=finished_at
        )
        meaningful_results = (
            counters.required_succeeded > 0 or counters.optional_succeeded > 0
        )
        state = derive_run_state(
            required_pending=counters.required_pending,
            required_deferred=counters.required_deferred,
            required_failed_permanent=counters.required_failed_permanent,
            meaningful_results=meaningful_results,
            reporting_failed=False,
        )
        record = repository.load_run(self._connection, run_id=run_id)
        report = RunReport(
            run_id=run_id,
            state=state,
            started_at=record.started_at,
            finished_at=finished_at,
            timezone=record.timezone,
            window_start=record.window_start,
            window_end=record.window_end,
            counters=counters,
            paused_providers=self._retry.paused_providers(),
            interrupted_runs=sweep.runs,
            failure_categories=dict(failure_categories),
        )
        try:
            artifact = self._reporter(report)
        except Exception as error:
            # The one terminal transition is recorded here, and only after the
            # reporter has resolved: a run whose digest never landed durably is
            # `failed`, whatever its work outcomes were. `derive_run_state`
            # stays the single authority for that, rather than a second
            # hard-coded RunState.FAILED in this branch.
            reporting_state = derive_run_state(
                required_pending=counters.required_pending,
                required_deferred=counters.required_deferred,
                required_failed_permanent=counters.required_failed_permanent,
                meaningful_results=meaningful_results,
                reporting_failed=True,
            )
            log_event(
                self._logger,
                "run.reporting_failed",
                severity=logging.ERROR,
                run_id=run_id,
                error_type=type(error).__name__,
            )
            repository.finish_run(
                self._connection,
                run_id=run_id,
                state=reporting_state,
                reason=f"reporting failed: {type(error).__name__}",
                digest_path=None,
                digest_sha256=None,
                now=self._now(),
            )
            raise
        repository.finish_run(
            self._connection,
            run_id=run_id,
            state=state,
            reason=None,
            digest_path=artifact.path,
            digest_sha256=artifact.sha256,
            now=finished_at,
        )
        log_event(
            self._logger,
            "run.finished",
            run_id=run_id,
            state=str(state),
            required_succeeded=counters.required_succeeded,
            required_pending=counters.required_pending,
            required_deferred=counters.required_deferred,
            required_failed_permanent=counters.required_failed_permanent,
            operational_failures=counters.operational_failures,
            paused_providers=sorted(report.paused_providers),
        )
        return report

    def _perform(
        self,
        run_id: int,
        work_item: WorkItem,
        handler: TaskHandler,
        failure_categories: dict[str, int],
    ) -> None:
        fingerprint = (
            handler.request_fingerprint(work_item)
            if handler.request_fingerprint is not None
            else work_item.fingerprint
        )
        attempt_ids: dict[int, int] = {}
        starting_ordinal = repository.next_attempt_ordinal(
            self._connection, work_item_id=work_item.id
        )

        last_outcome: TaskOutcome | None = None

        def action(ordinal: int) -> TaskOutcome:
            nonlocal last_outcome
            # Persist the attempt before the call so a crash leaves evidence.
            start = (
                repository.claim_and_start_attempt
                if ordinal == starting_ordinal
                else repository.start_attempt
            )
            attempt_ids[ordinal] = start(
                self._connection,
                run_id=run_id,
                work_item_id=work_item.id,
                provider=handler.provider,
                operation=handler.operation,
                ordinal=ordinal,
                request_fingerprint=fingerprint,
                destination_host=None,
                reserved_nano_usd=handler.reserved_nano_usd,
                now=self._now(),
            )
            # Every repository call above has committed, so no transaction is
            # open across the provider boundary.
            last_outcome = handler.execute(work_item, ordinal)
            return last_outcome

        def on_attempt(record: AttemptRecord) -> None:
            # A failed attempt has no outcome to persist; only a success does.
            outcome = last_outcome if record.outcome == "succeeded" else None
            if record.failure_category is not None:
                key = str(record.failure_category)
                with self._failure_categories_lock:
                    failure_categories[key] = failure_categories.get(key, 0) + 1
            repository.finish_attempt(
                self._connection,
                attempt_id=attempt_ids[record.ordinal],
                record=record,
                response_bytes=None if outcome is None else outcome.response_bytes,
                provider_request_id=None
                if outcome is None
                else outcome.provider_request_id,
                detail_json=None if outcome is None else outcome.detail_json,
                now=self._now(),
                # A failed attempt reports no actual cost, and passing None
                # deliberately RETAINS its reservation for the rest of the
                # run rather than releasing it. A failed call is not
                # necessarily a free call: tokens generated before a
                # malformed or truncated response, and a request billed
                # before a 5xx on the response path, are real spend the
                # provider never reported back. The cap therefore counts
                # every call the engine authorised, which is the
                # conservative direction for a spend limit -- the failure
                # mode is a run that stops early, not one that overspends.
                # `budget_actual_nano_usd` is untouched, so reported spend
                # never overstates real money; only headroom shrinks.
                actual_nano_usd=None if outcome is None else outcome.actual_nano_usd,
            )

        try:
            result = self._retry.call(
                handler.provider,
                handler.operation,
                action,
                on_attempt=on_attempt,
                starting_ordinal=starting_ordinal,
            )
        except ProviderPaused as paused:
            self._settle(
                run_id,
                work_item,
                handler,
                claimed=bool(attempt_ids),
                state=WorkState.DEFERRED,
                reason=f"{paused.provider} paused for this run",
            )
            return
        except RetryExhausted as exhausted:
            self._settle(
                run_id,
                work_item,
                handler,
                claimed=bool(attempt_ids),
                state=WorkState.DEFERRED,
                reason=(
                    f"exhausted transient failure: {exhausted.last_failure.category}"
                ),
            )
            return
        except BudgetExhausted:
            self._settle(
                run_id,
                work_item,
                handler,
                claimed=bool(attempt_ids),
                state=WorkState.DEFERRED,
                reason="not_evaluated_budget",
            )
            return
        except ProviderFailure as failure:
            self._settle(
                run_id,
                work_item,
                handler,
                claimed=bool(attempt_ids),
                state=WorkState.FAILED_PERMANENT,
                reason=str(failure.category),
            )
            return

        if result.state not in SETTLING_WORK_STATES:
            raise NonSettlingStateError(run_id, handler.task_type, result.state)
        self._settle(
            run_id,
            work_item,
            handler,
            claimed=True,
            state=result.state,
            reason=result.reason,
        )

    def _settle(
        self,
        run_id: int,
        work_item: WorkItem,
        handler: TaskHandler,
        *,
        claimed: bool,
        state: WorkState,
        reason: str | None,
    ) -> None:
        """Record one item's outcome and take it out of this run's peek.

        `claimed` distinguishes the two durable positions the item can be in.
        An item whose first attempt row was inserted is `running` and owned by
        this run, so `complete_work` settles it. An item refused before any
        attempt existed -- a paused provider, or a reservation refused inside
        `claim_and_start_attempt`'s own rolled-back transaction -- was never
        claimed, and only `defer_unclaimed` can record a decision about it.
        """
        now = self._now()
        if claimed:
            repository.complete_work(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                state=state,
                reason=reason,
                now=now,
            )
        elif state is WorkState.DEFERRED:
            repository.defer_unclaimed(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                reason=reason or "not evaluated",
                now=now,
            )
        else:
            raise ValueError(
                f"work item {work_item.id} was never claimed and can only be "
                f"deferred, not marked {state!r}"
            )
        log_event(
            self._logger,
            "run.work_settled",
            run_id=run_id,
            work_item_id=work_item.id,
            task_type=work_item.task_type,
            provider=handler.provider,
            operation=handler.operation,
            state=str(state),
            reason=reason,
            claimed=claimed,
        )
