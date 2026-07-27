from __future__ import annotations

import logging
import sqlite3
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from notable_person_finder.obs.logging import EVENT_LOGGER_NAME, log_event
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs import repository
from notable_person_finder.runs.budget import BudgetExhausted
from notable_person_finder.runs.clock import Clock, utc_timestamp
from notable_person_finder.runs.models import (
    RunCounters,
    RunState,
    WorkItem,
    WorkState,
)
from notable_person_finder.runs.retry import AttemptRecord, RetryPolicy
from notable_person_finder.runs.scheduler import BoundedScheduler, Completion

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
    # The handler's parsed result, carried from the worker thread back to the
    # application thread for `persist` to write. The engine never inspects it
    # and never stores it: only `persist` knows what it means.
    payload: object | None = None


@dataclass(frozen=True, slots=True)
class TaskHandler:
    """One task type's three phases, split by the thread each may run on.

    `prepare` runs on the application thread before submission and is the only
    phase that may read SQLite -- a stored ETag, a validator, an assembled
    prompt context. Its return value is handed to `execute`.

    `execute` runs on a scheduler worker. It performs exactly one external
    call and must never retry, never sleep, and never touch SQLite: the
    engine's connection belongs to the application thread, and the central
    retry coordination lives in the engine, not in a handler.

    `persist` runs on the application thread inside the same transaction that
    settles the work item, so a handler's domain writes and the settlement
    that justifies them commit or roll back together.
    """

    task_type: str
    provider: str
    operation: str
    execute: Callable[[WorkItem, int, object], TaskOutcome]
    request_fingerprint: Callable[[WorkItem], str] | None = None
    reserved_nano_usd: int = 0
    prepare: Callable[[WorkItem], object] | None = None
    persist: Callable[[WorkItem, TaskOutcome], None] | None = None
    destination_host: Callable[[WorkItem], str | None] | None = None


@dataclass(frozen=True, slots=True)
class _Submission:
    """Everything one external call needs, and deliberately nothing more.

    This is the whole interface between the application thread and a worker.
    It carries no connection and no engine, which is what makes "a worker
    never touches SQLite" a structural property rather than a convention.
    """

    work_item: WorkItem
    handler: TaskHandler
    ordinal: int
    attempt_id: int
    prepared: object
    clock: Clock


@dataclass(frozen=True, slots=True)
class _CallResult:
    """One call's answer, as observed on the worker thread.

    Exactly one of `outcome` and `failure` is set. `_call_provider` is the only
    constructor, and the engine checks `outcome` first, so neither field is
    ever read without being narrowed.
    """

    latency_ms: int
    outcome: TaskOutcome | None = None
    failure: ProviderFailure | None = None


def _call_provider(submission: _Submission) -> _CallResult:
    """Phase two: perform exactly one external call, off the application thread.

    Module-level and closure-free on purpose. A nested function could capture
    the engine, and through it the connection; this cannot. A `ProviderFailure`
    is returned rather than raised so its latency travels back with it, while
    any other exception propagates into `Completion.error` and the engine
    treats it as process-level breakage.
    """
    clock = submission.clock
    started = clock.monotonic()
    try:
        outcome = submission.handler.execute(
            submission.work_item, submission.ordinal, submission.prepared
        )
    except ProviderFailure as failure:
        return _CallResult(_elapsed_ms(clock, started), failure=failure)
    return _CallResult(_elapsed_ms(clock, started), outcome=outcome)


def _elapsed_ms(clock: Clock, started: float) -> int:
    return max(int((clock.monotonic() - started) * 1000), 0)


def _domain_writes(
    handler: TaskHandler, work_item: WorkItem, outcome: TaskOutcome | None
) -> Callable[[sqlite3.Connection], None] | None:
    """Bind `handler.persist` for the settling transaction to run, if there is one.

    There is nothing to persist for an item that never produced an outcome --
    a paused provider, a refused reservation, an exhausted retry -- so those
    settlements carry no domain writes.
    """
    persist = handler.persist
    if persist is None or outcome is None:
        return None

    def write(_connection: sqlite3.Connection) -> None:
        # The connection is handed over for symmetry with the repository's
        # other in-transaction helpers; the handler already holds its own.
        persist(work_item, outcome)

    return write


def _rearm_timestamp(moment: datetime) -> str:
    """Render a re-arm deadline so string comparison cannot strand the item.

    `eligible_at <= now` is a text comparison in SQL, and `utc_timestamp`
    omits the fractional part on a whole second. `'...:01Z' <= '...:01.5Z'` is
    false because `'Z' > '.'`, so a whole-second deadline could read as not
    yet due after its instant had passed -- and the loop would stop with the
    retry still parked. Always emitting microseconds makes the mismatch fall
    the safe way: a deadline sorts before any `now` in the same second, so a
    due item is claimable rather than stranded.
    """
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


@dataclass(slots=True)
class _ItemProgress:
    """One work item's retry position within this run.

    `max_attempts` bounds the calls a single run makes for one item, so this
    survives a re-arm: the item goes back to the queue, but its attempt count
    does not reset when the run re-claims it.
    """

    attempts: int = 0
    malformed_retries: int = 0


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
    """Owns the run lifecycle: sweep, claim, execute, persist, and report.

    Work moves through a per-item state machine so that SQLite stays on the
    application thread while external calls run concurrently on the scheduler's
    pool. One pass of the loop claims a batch no larger than the pool's cap,
    prepares and submits each item, then drains completions as they arrive.

    No transaction is ever open across an item's own external call: `prepare`
    and the attempt insert commit before submission, and the outcome is
    persisted only after the call has returned. Every transaction the engine
    opens is a short local write that never waits on a network response.
    """

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        retry: RetryPolicy,
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
        # Sizes every claim batch, and runs the calls. Its pool belongs to
        # whoever constructed it, so the engine never closes it.
        self._scheduler = scheduler
        self._clock = clock
        self._timezone = timezone
        self._window_start = window_start
        self._budget_limit_nano_usd = budget_limit_nano_usd
        self._snapshot_fingerprint = snapshot_fingerprint
        self._snapshot_json = snapshot_json
        self._reporter = reporter
        self._logger = logger or logging.getLogger(EVENT_LOGGER_NAME)
        # The tally is now built entirely on the application thread, so this
        # lock is defensive rather than load-bearing. It is retained because a
        # test pins its presence and because any future off-thread tally would
        # need it; dropping it is a separate, deliberate change.
        self._failure_categories_lock = threading.Lock()

    def _now(self) -> str:
        return utc_timestamp(self._clock.now())

    def execute(
        self,
        handlers: Mapping[str, TaskHandler],
        *,
        seed: Callable[[int], None] | None = None,
    ) -> RunReport:
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
        if seed is not None:
            # Seeding needs the run id, and its rows must exist before the
            # first claim, so this is the one hook between the two.
            seed(run_id)

        failure_categories: dict[str, int] = {}
        self._drain(run_id, handlers, failure_categories)

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

    def _drain(
        self,
        run_id: int,
        handlers: Mapping[str, TaskHandler],
        failure_categories: dict[str, int],
    ) -> None:
        """Claim, submit, and settle until nothing eligible remains.

        Everything in this method and everything it calls -- except
        `_call_provider` -- runs on the application thread.
        """
        progress: dict[int, _ItemProgress] = {}
        deadlines: dict[int, datetime] = {}
        while True:
            batch = repository.claim_batch(
                self._connection,
                run_id=run_id,
                now=self._now(),
                task_types=handlers.keys(),
                limit=self._scheduler.max_workers,
            )
            if batch:
                submissions: list[_Submission] = []
                for work_item in batch:
                    deadlines.pop(work_item.id, None)
                    submission = self._prepare(
                        run_id, work_item, handlers[work_item.task_type]
                    )
                    if submission is not None:
                        submissions.append(submission)
                for completion in self._scheduler.run(submissions, _call_provider):
                    self._resolve(
                        run_id, completion, progress, deadlines, failure_categories
                    )
                continue
            if not deadlines:
                return
            self._wait_for_the_earliest_deadline(run_id, deadlines)

    def _wait_for_the_earliest_deadline(
        self, run_id: int, deadlines: dict[int, datetime]
    ) -> None:
        """Park until the soonest re-armed item is due, rather than spinning.

        Only reached when nothing at all is claimable, so a backoff never
        delays work that is ready now. Every deadline whose instant has passed
        is dropped afterwards, which is what guarantees the loop terminates:
        an item that somehow stays unclaimable cannot be waited on twice.
        """
        earliest = min(deadlines.values())
        delay = (earliest - self._clock.now()).total_seconds()
        if delay > 0:
            log_event(
                self._logger,
                "run.backoff_wait",
                run_id=run_id,
                seconds=round(delay, 3),
                waiting_items=len(deadlines),
            )
            self._clock.sleep(delay)
        reached = self._clock.now()
        for work_item_id in [
            candidate
            for candidate, deadline in deadlines.items()
            if deadline <= reached
        ]:
            del deadlines[work_item_id]

    def _prepare(
        self, run_id: int, work_item: WorkItem, handler: TaskHandler
    ) -> _Submission | None:
        """Phase one: everything one call needs, read on the application thread.

        Returns None when the item was settled instead of submitted. The pause
        check comes first and writes no attempt row: an attempt row stands for
        exactly one external call, and a paused provider is never called. This
        used to be enforced inside `RetryCoordinator.call`, which the batch
        loop no longer routes through, so the engine owns it explicitly.
        """
        if self._retry.is_paused(handler.provider):
            self._settle(
                run_id,
                work_item,
                handler,
                state=WorkState.DEFERRED,
                reason=f"{handler.provider} paused for this run",
            )
            return None
        prepared = handler.prepare(work_item) if handler.prepare is not None else None
        fingerprint = (
            handler.request_fingerprint(work_item)
            if handler.request_fingerprint is not None
            else work_item.fingerprint
        )
        destination_host = (
            handler.destination_host(work_item)
            if handler.destination_host is not None
            else None
        )
        ordinal = repository.next_attempt_ordinal(
            self._connection, work_item_id=work_item.id
        )
        try:
            # Persisted before the call, so a crash leaves evidence that the
            # provider may already have been charged.
            attempt_id = repository.start_attempt(
                self._connection,
                run_id=run_id,
                work_item_id=work_item.id,
                provider=handler.provider,
                operation=handler.operation,
                ordinal=ordinal,
                request_fingerprint=fingerprint,
                destination_host=destination_host,
                reserved_nano_usd=handler.reserved_nano_usd,
                now=self._now(),
            )
        except BudgetExhausted:
            # The reservation and the attempt insert shared one transaction,
            # so nothing was written and no call is made.
            self._settle(
                run_id,
                work_item,
                handler,
                state=WorkState.DEFERRED,
                reason="not_evaluated_budget",
            )
            return None
        # Every repository call above has committed, so no transaction is open
        # across the call this submission is about to make.
        return _Submission(
            work_item=work_item,
            handler=handler,
            ordinal=ordinal,
            attempt_id=attempt_id,
            prepared=prepared,
            clock=self._clock,
        )

    def _resolve(
        self,
        run_id: int,
        completion: Completion[_Submission, _CallResult],
        progress: dict[int, _ItemProgress],
        deadlines: dict[int, datetime],
        failure_categories: dict[str, int],
    ) -> None:
        """Phase three: persist one call's answer on the application thread.

        `unwrap` re-raises anything the worker raised other than a
        `ProviderFailure`. That is deliberately not caught: an unexpected
        exception is process-level breakage, and leaving the attempt row open
        is exactly what lets the next run's sweep mark it `interrupted`.
        """
        call = completion.unwrap()
        submission = completion.item
        work_item = submission.work_item
        handler = submission.handler
        state = progress.setdefault(work_item.id, _ItemProgress())
        state.attempts += 1

        outcome = call.outcome
        if outcome is not None:
            self._finish_attempt(
                submission,
                AttemptRecord(
                    ordinal=submission.ordinal,
                    outcome="succeeded",
                    latency_ms=call.latency_ms,
                ),
                outcome,
                failure_categories,
            )
            self._retry.record_success(handler.provider)
            if outcome.state not in SETTLING_WORK_STATES:
                raise NonSettlingStateError(run_id, handler.task_type, outcome.state)
            self._settle(
                run_id,
                work_item,
                handler,
                state=outcome.state,
                reason=outcome.reason,
                outcome=outcome,
            )
            return

        failure = call.failure
        if failure is None:
            raise RuntimeError(
                "a call result carried neither an outcome nor a provider failure"
            )
        self._finish_attempt(
            submission,
            AttemptRecord(
                ordinal=submission.ordinal,
                outcome="failed",
                latency_ms=call.latency_ms,
                failure_category=failure.category,
                status_code=failure.status_code,
                retry_after_ms=failure.retry_after_ms,
                detail=failure.detail,
            ),
            None,
            failure_categories,
        )
        decision = self._retry.decide(
            handler.provider,
            failure=failure,
            attempt_index=state.attempts - 1,
            malformed_retries=state.malformed_retries,
        )
        if decision.action == "PERMANENT":
            # A non-retryable failure, or a second malformed response, which is
            # foreclosed regardless of remaining attempt budget. Both are real
            # answers from the provider, so neither counts towards pausing it,
            # and neither may be retried in a later run: repeating a paid call
            # that already answered is the failure mode this branch prevents.
            self._retry.record_success(handler.provider)
            self._settle(
                run_id,
                work_item,
                handler,
                state=WorkState.FAILED_PERMANENT,
                reason=str(failure.category),
            )
            return
        if failure.category is FailureCategory.MALFORMED_RESPONSE:
            state.malformed_retries += 1
        if decision.action == "EXHAUSTED":
            # The pause counter tracks transient exhaustion only. A malformed
            # response is a real answer from the provider, so it must not count
            # towards pausing even when it used up the last attempt slot.
            if failure.category is FailureCategory.MALFORMED_RESPONSE:
                self._retry.record_success(handler.provider)
            else:
                self._retry.record_exhaustion(handler.provider)
            self._settle(
                run_id,
                work_item,
                handler,
                state=WorkState.DEFERRED,
                reason=f"exhausted transient failure: {failure.category}",
            )
            return
        if decision.delay_seconds is None:
            raise RuntimeError(
                "RetryPolicy.decide returned action=RETRY with no delay_seconds"
            )
        self._rearm(
            run_id,
            work_item,
            handler,
            deadlines,
            failure=failure,
            delay_seconds=decision.delay_seconds,
        )

    def _finish_attempt(
        self,
        submission: _Submission,
        record: AttemptRecord,
        outcome: TaskOutcome | None,
        failure_categories: dict[str, int],
    ) -> None:
        if record.failure_category is not None:
            key = str(record.failure_category)
            with self._failure_categories_lock:
                failure_categories[key] = failure_categories.get(key, 0) + 1
        repository.finish_attempt(
            self._connection,
            attempt_id=submission.attempt_id,
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

    def _rearm(
        self,
        run_id: int,
        work_item: WorkItem,
        handler: TaskHandler,
        deadlines: dict[int, datetime],
        *,
        failure: ProviderFailure,
        delay_seconds: float,
    ) -> None:
        """Return an item to the queue for a retry later in this same run.

        `pending` with a future `eligible_at`, never `deferred`:
        `_CLAIMABLE_PREDICATE` admits a `deferred` item only from a different
        run, so deferring here would strand the retry until tomorrow. The
        backoff is a deadline on the row, not a sleep on the application
        thread, so other items keep moving while this one waits.

        The deadline is durable, so it can outlive the run that set it: if
        this run ends while the item is still deferred, a run starting within
        the backoff window will not pick it up yet. That is the intended
        reading of a backoff -- do not call this provider again for N seconds
        -- and it is bounded by `retry.max_backoff_seconds`.
        """
        deadline = self._clock.now() + timedelta(seconds=delay_seconds)
        repository.complete_work(
            self._connection,
            work_item_id=work_item.id,
            run_id=run_id,
            state=WorkState.PENDING,
            reason=f"retrying after {failure.category}",
            now=self._now(),
            eligible_at=_rearm_timestamp(deadline),
        )
        deadlines[work_item.id] = deadline
        log_event(
            self._logger,
            "run.work_rearmed",
            run_id=run_id,
            work_item_id=work_item.id,
            task_type=work_item.task_type,
            provider=handler.provider,
            operation=handler.operation,
            failure_category=str(failure.category),
            delay_seconds=round(delay_seconds, 3),
        )

    def _settle(
        self,
        run_id: int,
        work_item: WorkItem,
        handler: TaskHandler,
        *,
        state: WorkState,
        reason: str | None,
        outcome: TaskOutcome | None = None,
    ) -> None:
        """Record one item's outcome and take it out of this run's claims.

        Every item the engine sees has been claimed by `claim_batch`, so
        `complete_work` is the only settle path: it requires state `running`,
        which is proof this run owns the item. `handler.persist` runs inside
        that same transaction, so a handler's domain writes and the settlement
        that justifies them commit or roll back together.
        """
        repository.complete_work(
            self._connection,
            work_item_id=work_item.id,
            run_id=run_id,
            state=state,
            reason=reason,
            now=self._now(),
            domain_writes=_domain_writes(handler, work_item, outcome),
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
        )
