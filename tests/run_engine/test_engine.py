from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    NonSettlingStateError,
    ReportArtifact,
    RunEngine,
    RunReport,
    TaskHandler,
    TaskOutcome,
    derive_run_state,
)
from notable_person_finder.runs.models import RunState, WorkState
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

NOW = "2026-07-25T06:00:00Z"


# --- pure terminal-state rules -------------------------------------------------


def test_no_outstanding_required_work_is_complete() -> None:
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=0,
            meaningful_results=False,
            reporting_failed=False,
        )
        is RunState.COMPLETE
    )


def test_an_empty_run_is_complete_not_failed() -> None:
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=0,
            meaningful_results=False,
            reporting_failed=False,
        )
        is RunState.COMPLETE
    )


def test_deferred_required_work_makes_the_run_partial() -> None:
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=2,
            required_failed_permanent=0,
            meaningful_results=True,
            reporting_failed=False,
        )
        is RunState.PARTIAL
    )


def test_unevaluated_required_work_makes_the_run_partial() -> None:
    assert (
        derive_run_state(
            required_pending=3,
            required_deferred=0,
            required_failed_permanent=0,
            meaningful_results=True,
            reporting_failed=False,
        )
        is RunState.PARTIAL
    )


def test_a_reporting_failure_makes_the_run_failed() -> None:
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=0,
            meaningful_results=True,
            reporting_failed=True,
        )
        is RunState.FAILED
    )
    assert (
        derive_run_state(
            required_pending=1,
            required_deferred=1,
            required_failed_permanent=0,
            meaningful_results=True,
            reporting_failed=True,
        )
        is RunState.FAILED
    )


def test_required_permanent_failure_is_never_complete() -> None:
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=1,
            meaningful_results=True,
            reporting_failed=False,
        )
        is RunState.PARTIAL
    )
    assert (
        derive_run_state(
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=1,
            meaningful_results=False,
            reporting_failed=False,
        )
        is RunState.FAILED
    )


def test_a_meaningless_permanent_failure_outranks_outstanding_work() -> None:
    # Outstanding work alone is only PARTIAL, so this pins the precedence:
    # the permanent-failure rule is consulted before the outstanding-work rule
    # and can still reach FAILED when nothing meaningful was produced.
    assert (
        derive_run_state(
            required_pending=4,
            required_deferred=2,
            required_failed_permanent=1,
            meaningful_results=False,
            reporting_failed=False,
        )
        is RunState.FAILED
    )


# --- engine behaviour ----------------------------------------------------------


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, tmp_path / "backups")
    connection.close()
    return path


def memory_reporter(report) -> ReportArtifact:
    return ReportArtifact(path=None, sha256=None, markdown="")


def build_engine(
    connection: sqlite3.Connection,
    clock: FakeClock,
    *,
    budget_limit_nano_usd: int | None = None,
) -> RunEngine:
    return RunEngine(
        connection,
        retry=RetryCoordinator(
            RetryConfig(max_attempts=2, jitter_ratio=0.0), clock=clock
        ),
        scheduler=BoundedScheduler(max_workers=2),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=budget_limit_nano_usd,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=memory_reporter,
    )


def succeeding_handler(calls: list[int]) -> TaskHandler:
    def execute(work_item, ordinal: int) -> TaskOutcome:
        calls.append(work_item.id)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    return TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )


def schedule_probe(
    connection: sqlite3.Connection, fingerprint: str, *, required: bool = True
) -> int:
    return repository.schedule_work(
        connection,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint=fingerprint,
        required=required,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )


def test_a_run_with_no_work_is_complete(database: Path) -> None:
    connection = connect_database(database)
    report = build_engine(connection, FakeClock()).execute({})
    assert report.state is RunState.COMPLETE
    assert report.counters.required_succeeded == 0
    assert report.human_id == f"run-{report.run_id}"
    connection.close()


def test_eligible_work_is_executed_and_marked_succeeded(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "b" * 64)
    calls: list[int] = []
    handler = succeeding_handler(calls)

    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert calls == [work_id]
    assert report.state is RunState.COMPLETE
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.SUCCEEDED
    )
    connection.close()


def test_run_counters_do_not_include_historical_work_or_attempts(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "7" * 64)
    handler = succeeding_handler([])
    first = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    second = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    assert first.counters.required_succeeded == 1
    assert second.counters.required_succeeded == 0
    assert second.counters.operational_failures == 0
    connection.close()


def test_each_call_persists_one_attributed_attempt(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "c" * 64)
    handler = succeeding_handler([])
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    attempts = repository.attempts_for_run(connection, run_id=report.run_id)
    assert len(attempts) == 1
    assert attempts[0]["provider"] == "probe_provider"
    assert attempts[0]["outcome"] == "succeeded"
    assert attempts[0]["ordinal"] == 1
    connection.close()


def test_budget_is_reserved_before_the_call_and_actual_cost_is_reconciled(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "8" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        row = connection.execute(
            "SELECT budget_reserved_nano_usd FROM run ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["budget_reserved_nano_usd"] == 400
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None, actual_nano_usd=120)

    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=execute,
        reserved_nano_usd=400,
    )
    report = build_engine(connection, FakeClock(), budget_limit_nano_usd=1000).execute(
        {handler.task_type: handler}
    )
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (report.run_id,),
    ).fetchone()
    assert (row["budget_reserved_nano_usd"], row["budget_actual_nano_usd"]) == (
        120,
        120,
    )
    assert (
        connection.execute(
            "SELECT actual_nano_usd FROM attempt WHERE run_id = ?", (report.run_id,)
        ).fetchone()["actual_nano_usd"]
        == 120
    )
    connection.close()


def test_refused_budget_reservation_makes_no_external_call(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "9" * 64)
    calls: list[int] = []
    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=lambda work, ordinal: (
            calls.append(ordinal) or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        ),
        reserved_nano_usd=200,
    )
    report = build_engine(connection, FakeClock(), budget_limit_nano_usd=100).execute(
        {handler.task_type: handler}
    )
    assert calls == []
    assert report.state is RunState.PARTIAL
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.DEFERRED
    )
    assert repository.attempts_for_run(connection, run_id=report.run_id) == ()
    connection.close()


def test_a_transient_failure_is_retried_and_both_attempts_persist(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "d" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        if ordinal == 1:
            raise ProviderFailure(
                FailureCategory.TRANSIENT_SERVER_ERROR,
                provider="probe_provider",
                operation="probe_call",
                status_code=503,
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    outcomes = [
        row["outcome"]
        for row in repository.attempts_for_run(connection, run_id=report.run_id)
    ]
    assert outcomes == ["failed", "succeeded"]
    assert report.state is RunState.COMPLETE
    assert report.failure_categories == {"transient_server_error": 1}
    connection.close()


def test_attempt_ordinals_continue_across_runs_for_re_attempted_work(
    database: Path,
) -> None:
    """Ordinals are per work item and monotone, not per run.

    `attempt` carries UNIQUE (work_item_id, ordinal), and deferred work is
    re-attempted by the next run by design, so an engine that restarted
    numbering at 1 would not produce a soft wrong answer -- it would crash on
    the unique index the moment any item is retried across runs.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "a" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.TIMEOUT, provider="probe_provider", operation="probe_call"
        )

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    # max_attempts=2, so each run burns two ordinals and defers the item.
    first = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    second = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    rows = connection.execute(
        "SELECT ordinal, run_id FROM attempt WHERE work_item_id = ? ORDER BY id",
        (work_id,),
    ).fetchall()
    assert [row["ordinal"] for row in rows] == [1, 2, 3, 4]
    assert [row["run_id"] for row in rows] == [
        first.run_id,
        first.run_id,
        second.run_id,
        second.run_id,
    ]
    assert second.state is RunState.PARTIAL
    connection.close()


def test_exhausted_transient_work_is_deferred_and_the_run_is_partial(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "e" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.TIMEOUT, provider="probe_provider", operation="probe_call"
        )

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert report.state is RunState.PARTIAL
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.DEFERRED
    )
    connection.close()


def test_a_permanent_failure_without_useful_results_fails_the_run(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "f" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.AUTHENTICATION,
            provider="probe_provider",
            operation="probe_call",
            status_code=401,
        )

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.FAILED_PERMANENT
    )
    assert report.state is RunState.FAILED
    assert report.failure_categories == {"authentication": 1}
    connection.close()


def test_one_item_failure_does_not_stop_unrelated_work(database: Path) -> None:
    connection = connect_database(database)
    good = schedule_probe(connection, "1" * 64)
    bad = schedule_probe(connection, "2" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        if work_item.id == bad:
            raise ProviderFailure(
                FailureCategory.ACCESS_DENIED,
                provider="probe_provider",
                operation="probe_call",
                status_code=403,
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    states = {
        row["id"]: row["state"]
        for row in connection.execute("SELECT id, state FROM work_item")
    }
    assert states[good] == WorkState.SUCCEEDED
    assert states[bad] == WorkState.FAILED_PERMANENT
    connection.close()


def test_work_for_a_paused_provider_is_deferred(database: Path) -> None:
    connection = connect_database(database)
    for index in range(3):
        schedule_probe(connection, str(index) * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.PROVIDER_UNAVAILABLE,
            provider="probe_provider",
            operation="probe_call",
            status_code=503,
        )

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryCoordinator(
            RetryConfig(
                max_attempts=1,
                jitter_ratio=0.0,
                provider_pause_after_consecutive_exhaustions=1,
            ),
            clock=clock,
        ),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
    )
    report = engine.execute({handler.task_type: handler})

    assert report.paused_providers == frozenset({"probe_provider"})
    deferred = connection.execute(
        "SELECT COUNT(*) AS n FROM work_item WHERE state = 'deferred'"
    ).fetchone()["n"]
    assert deferred == 3
    # Only one provider request was made; the other two were deferred unmade.
    assert len(repository.attempts_for_run(connection, run_id=report.run_id)) == 1
    connection.close()


def test_the_engine_sweeps_an_abandoned_predecessor_first(database: Path) -> None:
    connection = connect_database(database)
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    abandoned = repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-23T06:00:00Z",
        window_end="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        now="2026-07-24T06:00:00Z",
    )
    report = build_engine(connection, FakeClock()).execute({})

    assert report.interrupted_runs == (abandoned,)
    assert (
        repository.load_run(connection, run_id=abandoned).state is RunState.INTERRUPTED
    )
    assert report.run_id != abandoned
    connection.close()


def test_no_transaction_is_open_while_a_handler_runs(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "3" * 64)
    observed: list[bool] = []

    def execute(work_item, ordinal: int) -> TaskOutcome:
        observed.append(connection.in_transaction)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    assert observed == [False]
    connection.close()


def test_work_without_a_registered_handler_stays_pending(database: Path) -> None:
    connection = connect_database(database)
    work_id = repository.schedule_work(
        connection,
        task_type="unknown_task",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="4" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )
    report = build_engine(connection, FakeClock()).execute({})
    assert report.state is RunState.PARTIAL
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.PENDING
    )
    connection.close()


def test_the_reporter_receives_the_run_report_and_its_artifact_is_recorded(
    database: Path,
) -> None:
    """The reporter is handed the report and hands back the durable artifact.

    The engine records the artifact's persisted identity on the run row and
    leaves `markdown` alone: the reporter has already written it, and it is
    carried for the CLI to render, not for the database.
    """
    connection = connect_database(database)
    seen: list[RunReport] = []
    markdown = "# run\n\nnothing to report\n"

    produced: list[ReportArtifact] = []

    def reporter(report: RunReport) -> ReportArtifact:
        seen.append(report)
        artifact = ReportArtifact(
            path="/digests/2026-07-25.md", sha256="c" * 64, markdown=markdown
        )
        produced.append(artifact)
        return artifact

    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryCoordinator(RetryConfig(max_attempts=1), clock=clock),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=reporter,
    )
    report = engine.execute({})

    assert len(seen) == 1
    handed = seen[0]
    assert handed.run_id == report.run_id
    assert handed.human_id == f"run-{report.run_id}"
    assert handed.state is RunState.COMPLETE

    row = connection.execute(
        "SELECT digest_path, digest_sha256 FROM run WHERE id = ?", (report.run_id,)
    ).fetchone()
    assert (row["digest_path"], row["digest_sha256"]) == (
        "/digests/2026-07-25.md",
        "c" * 64,
    )
    # The artifact carries the reporter's exact Markdown for the CLI to
    # render; only its persisted identity reaches the database.
    assert produced[0].markdown == markdown
    assert markdown not in str(tuple(row))
    connection.close()


def test_reporting_failure_is_the_only_terminal_transition(database: Path) -> None:
    connection = connect_database(database)

    def fail_reporting(report) -> ReportArtifact:
        raise OSError("digest root unavailable")

    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryCoordinator(RetryConfig(max_attempts=1), clock=clock),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=fail_reporting,
    )
    with pytest.raises(OSError, match="digest root"):
        engine.execute({})
    latest = repository.latest_run(connection)
    assert latest is not None and latest.state is RunState.FAILED
    transitions = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id",
            (latest.id,),
        )
    ]
    assert transitions == ["running", "failed"]
    connection.close()


# --- wiring decisions this task owns -------------------------------------------


def test_a_failed_attempt_keeps_its_reservation_and_can_refuse_the_retry(
    database: Path,
) -> None:
    """Pin the deliberate cost-reconciliation wiring.

    A failed attempt reports no actual cost, and the engine does NOT release
    its reservation: the run's cap counts every call the engine authorised.
    The retry therefore has only 40 of the 100 nano-USD cap left, cannot
    afford its own 60 nano-USD reservation, and is refused before any second
    provider request is made. Releasing the failed reservation instead would
    leave the full 100 available and let the retry proceed, so this test
    discriminates the two wirings.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "5" * 64)
    ordinals: list[int] = []

    def execute(work_item, ordinal: int) -> TaskOutcome:
        ordinals.append(ordinal)
        raise ProviderFailure(
            FailureCategory.TIMEOUT,
            provider="openrouter",
            operation="generate_structured",
        )

    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=execute,
        reserved_nano_usd=60,
    )
    report = build_engine(connection, FakeClock(), budget_limit_nano_usd=100).execute(
        {handler.task_type: handler}
    )

    assert ordinals == [1]
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (report.run_id,),
    ).fetchone()
    assert (row["budget_reserved_nano_usd"], row["budget_actual_nano_usd"]) == (60, 0)
    assert len(repository.attempts_for_run(connection, run_id=report.run_id)) == 1
    # The item was claimed by attempt 1, so this settles through the normal
    # claimed path rather than the never-claimed one.
    settled = connection.execute(
        "SELECT state, reason, completed_by_run_id FROM work_item WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert settled["state"] == WorkState.DEFERRED
    assert settled["reason"] == "not_evaluated_budget"
    assert settled["completed_by_run_id"] == report.run_id
    assert report.state is RunState.PARTIAL
    connection.close()


def test_a_handler_that_returns_a_non_settling_state_is_rejected(
    database: Path,
) -> None:
    """A handler may not hand back a state that would leave the item claimable.

    Without this guard the engine's peek-and-settle loop would re-select the
    same item forever instead of terminating.
    """
    connection = connect_database(database)
    schedule_probe(connection, "6" * 64)
    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=lambda work, ordinal: TaskOutcome(state=WorkState.PENDING, reason=None),
    )
    with pytest.raises(ValueError, match="settling state"):
        build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    connection.close()


def test_the_engine_logs_run_lifecycle_events_and_never_the_snapshot(
    database: Path,
) -> None:
    """Pin the lifecycle events and the fields the engine hands the log.

    The capture logger below has no redaction filter attached, so this does
    not exercise redaction -- Task 14's own tests do that. What it pins is
    what the engine *offers* the logger: the configuration snapshot, the only
    value the engine holds that could carry operator-supplied text, is never
    passed as a field, so redaction is never asked to save it.
    """
    connection = connect_database(database)
    schedule_probe(connection, "0" * 64)
    handler = succeeding_handler([])

    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    capture = Capture()
    logger = logging.getLogger("test.engine.events")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger.addHandler(capture)
    try:
        clock = FakeClock()
        engine = RunEngine(
            connection,
            retry=RetryCoordinator(RetryConfig(max_attempts=1), clock=clock),
            scheduler=BoundedScheduler(max_workers=1),
            clock=clock,
            timezone="Europe/Paris",
            window_start="2026-07-24T06:00:00Z",
            budget_limit_nano_usd=None,
            snapshot_fingerprint="a" * 64,
            snapshot_json="{}",
            reporter=memory_reporter,
            logger=logger,
        )
        report = engine.execute({handler.task_type: handler})

        events = [record.event for record in records]  # type: ignore[attr-defined]
        assert events == ["run.started", "run.work_settled", "run.finished"]
        finished = records[-1].fields  # type: ignore[attr-defined]
        assert finished["run_id"] == report.run_id
        assert finished["state"] == str(RunState.COMPLETE)
        for record in records:
            assert "{}" not in str(record.fields)  # type: ignore[attr-defined]
    finally:
        logger.removeHandler(capture)
    connection.close()


def test_non_settling_handler_state_raises(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "n" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        return TaskOutcome(state=WorkState.PENDING, reason="not settling")

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    engine = build_engine(connection, FakeClock())
    with pytest.raises(NonSettlingStateError) as captured:
        engine.execute({handler.task_type: handler})
    assert captured.value.task_type == "probe"
    assert captured.value.state is WorkState.PENDING
    connection.close()


def test_failure_categories_tally_uses_a_threading_lock(database: Path) -> None:
    """`on_attempt` mutates a shared tally; the engine must hold a lock for it."""
    connection = connect_database(database)
    engine = build_engine(connection, FakeClock())
    assert isinstance(engine._failure_categories_lock, threading.Lock)
    connection.close()
