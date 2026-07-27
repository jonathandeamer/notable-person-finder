from __future__ import annotations

import dataclasses
import inspect
import logging
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    RunReport,
    TaskHandler,
    TaskOutcome,
    derive_run_state,
)
from notable_person_finder.runs.models import RunState, WorkState
from notable_person_finder.runs.retry import RetryPolicy
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
    retry: RetryPolicy | None = None,
    scheduler: BoundedScheduler | None = None,
) -> RunEngine:
    return RunEngine(
        connection,
        retry=retry
        or RetryPolicy(RetryConfig(max_attempts=2, jitter_ratio=0.0), clock=clock),
        scheduler=scheduler or BoundedScheduler(max_workers=2),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=budget_limit_nano_usd,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=memory_reporter,
    )


def succeeding_handler(calls: list[int]) -> TaskHandler:
    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        # The call runs on a worker thread, so it must not use the engine's
        # connection. A second read-only handle observes only what is
        # COMMITTED, which is the stronger claim anyway: the reservation was
        # durable before the call, not merely pending in the caller's
        # transaction.
        reader = connect_database(database, readonly=True)
        try:
            row = reader.execute(
                "SELECT budget_reserved_nano_usd FROM run ORDER BY id DESC LIMIT 1"
            ).fetchone()
        finally:
            reader.close()
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
        execute=lambda work, ordinal, prepared: (
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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
    # The second run's clock starts after the first run finished, because the
    # first run's retry backoff pushes the item's `eligible_at` past its own
    # start instant: an intra-run backoff means "do not call this provider
    # again for N seconds", and that deadline outlives the run that set it.
    # Two runs cannot share one instant, and pretending they do would make the
    # second run unable to claim anything at all.
    first = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    second = build_engine(
        connection, FakeClock(start=datetime(2026, 7, 25, 6, 0, 30, tzinfo=UTC))
    ).execute({handler.task_type: handler})

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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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
        retry=RetryPolicy(
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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
        retry=RetryPolicy(RetryConfig(max_attempts=1), clock=clock),
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
        retry=RetryPolicy(RetryConfig(max_attempts=1), clock=clock),
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

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
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


def test_budget_exhaustion_is_reported_by_cap_reserved_and_deferral_reason(
    database: Path,
) -> None:
    """Reproduces the milestone-2 review gap with the real engine, cap, and
    digest inputs: three required items at 1 USD each against a 1.5 USD cap.
    Two must defer for budget, and `RunReport` must be able to say why --
    not just that "required work deferred: 2" happened, but the reason and
    the cap/reserved/actual figures that explain it.
    """
    connection = connect_database(database)
    one_usd = 1_000_000_000
    for fingerprint in ("1" * 64, "2" * 64, "3" * 64):
        schedule_probe(connection, fingerprint)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None, actual_nano_usd=one_usd
        )

    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=execute,
        reserved_nano_usd=one_usd,
    )
    report = build_engine(
        connection, FakeClock(), budget_limit_nano_usd=int(1.5 * one_usd)
    ).execute({handler.task_type: handler})

    assert report.counters.required_succeeded == 1
    assert report.counters.required_deferred == 2
    assert report.deferred_reasons == {"not_evaluated_budget": 2}
    assert report.budget_limit_nano_usd == int(1.5 * one_usd)
    assert report.budget_reserved_nano_usd == one_usd
    assert report.budget_actual_nano_usd == one_usd
    connection.close()


def test_a_handler_that_returns_a_non_settling_state_settles_as_failed_permanent(
    database: Path,
) -> None:
    """A handler may hand back a state that would otherwise leave the item
    claimable forever. The engine no longer lets that escape as a process
    crash: it settles just that item as `failed_permanent` with a diagnostic
    reason and lets the run finish normally.

    Without this guard the engine's peek-and-settle loop would re-select the
    same item forever instead of terminating.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "6" * 64)
    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.PENDING, reason=None
        ),
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT state, reason, completed_by_run_id FROM work_item WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row["state"] == WorkState.FAILED_PERMANENT
    assert row["completed_by_run_id"] == report.run_id
    assert row["reason"] is not None
    assert "settling state" in row["reason"]
    assert report.counters.required_failed_permanent == 1
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
            retry=RetryPolicy(RetryConfig(max_attempts=1), clock=clock),
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


def test_non_settling_handler_state_settles_permanently_and_does_not_raise(
    database: Path,
) -> None:
    """The engine used to raise `NonSettlingStateError` here. It no longer
    does: the one bad item is settled `failed_permanent` and the run reaches
    a terminal state instead of aborting."""
    connection = connect_database(database)
    work_id = schedule_probe(connection, "n" * 64)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.PENDING, reason="not settling")

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    engine = build_engine(connection, FakeClock())
    report = engine.execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.FAILED_PERMANENT
    assert "probe" in row["reason"]
    assert "pending" in row["reason"].lower()
    # No other required item succeeded this run, so the failure is not
    # meaningless-outranked: the run reports FAILED, not PARTIAL.
    assert report.state is RunState.FAILED
    connection.close()


def test_a_non_settling_item_does_not_stop_a_healthy_item_in_the_same_run(
    database: Path,
) -> None:
    """One bad handler must not abort the whole run: a healthy item scheduled
    alongside it still succeeds, and the run still finishes and reports,
    rather than aborting with no digest."""
    connection = connect_database(database)
    bad_id = schedule_probe(connection, "bad" * 21 + "1")
    good_id = schedule_probe(connection, "good" * 16)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        if work_item.id == bad_id:
            return TaskOutcome(state=WorkState.PENDING, reason=None)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    reported: list[RunReport] = []

    def spy_reporter(report: RunReport) -> ReportArtifact:
        reported.append(report)
        return ReportArtifact(path="fake-digest.md", sha256="d" * 64, markdown="x")

    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryPolicy(RetryConfig(max_attempts=2, jitter_ratio=0.0), clock=clock),
        scheduler=BoundedScheduler(max_workers=2),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=spy_reporter,
    )
    report = engine.execute({handler.task_type: handler})

    assert len(reported) == 1
    bad_row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (bad_id,)
    ).fetchone()
    good_row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (good_id,)
    ).fetchone()
    assert bad_row["state"] == WorkState.FAILED_PERMANENT
    assert good_row["state"] == WorkState.SUCCEEDED
    assert report.counters.required_succeeded == 1
    assert report.counters.required_failed_permanent == 1
    # The good item's success is meaningful, so the failed one is outranked
    # to PARTIAL rather than FAILED, and -- critically -- a digest was
    # recorded rather than the run aborting with none.
    assert report.state is RunState.PARTIAL
    run_row = connection.execute(
        "SELECT state, digest_path FROM run WHERE id = ?", (report.run_id,)
    ).fetchone()
    assert run_row["state"] == RunState.PARTIAL
    assert run_row["digest_path"] == "fake-digest.md"
    connection.close()


def test_failure_categories_tally_uses_a_threading_lock(database: Path) -> None:
    """`on_attempt` mutates a shared tally; the engine must hold a lock for it."""
    connection = connect_database(database)
    engine = build_engine(connection, FakeClock())
    assert isinstance(engine._failure_categories_lock, threading.Lock)
    connection.close()


# --- batch-claim state machine (task 3) ----------------------------------------


def probe_handler(
    execute,
    *,
    provider: str = "probe_provider",
    **extra,
) -> TaskHandler:
    return TaskHandler(
        task_type="probe",
        provider=provider,
        operation="probe_call",
        execute=execute,
        **extra,
    )


def test_every_repository_call_happens_on_the_application_thread(
    database: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No worker thread may touch SQLite, asserted at every repository call.

    Wrapping every public repository function is the dynamic half of the
    proof: if any of the engine's persistence moved onto a worker -- which is
    exactly the defect this reshape fixes -- the recorded thread set would
    grow beyond the application thread. `worker_threads` keeps the test
    honest: it fails if `execute` never actually ran off the main thread, so
    a fully sequential engine could not pass it vacuously.
    """
    connection = connect_database(database)
    for index in range(4):
        schedule_probe(connection, f"{index}a" * 32)

    application_thread = threading.get_ident()
    repository_threads: set[int] = set()
    repository_calls: list[str] = []
    guard = threading.Lock()

    def recording(name: str, function):
        def wrapper(*args: object, **kwargs: object) -> object:
            with guard:
                repository_threads.add(threading.get_ident())
                repository_calls.append(name)
            return function(*args, **kwargs)

        return wrapper

    for name in dir(repository):
        if name.startswith("_"):
            continue
        candidate = getattr(repository, name)
        if not inspect.isfunction(candidate):
            continue
        if candidate.__module__ != repository.__name__:
            continue
        monkeypatch.setattr(repository, name, recording(name, candidate))

    worker_threads: set[int] = set()

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        worker_threads.add(threading.get_ident())
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert repository_calls, "no repository call was observed at all"
    assert repository_threads == {application_thread}
    assert worker_threads and application_thread not in worker_threads
    connection.close()


class RecordingScheduler(BoundedScheduler):
    """Captures exactly what the engine hands the pool, before it runs."""

    def __init__(self, max_workers: int) -> None:
        super().__init__(max_workers)
        self.workers: list[object] = []
        self.submitted: list[object] = []

    def run(self, items, worker):  # type: ignore[override]
        materialized = list(items)
        self.workers.append(worker)
        self.submitted.extend(materialized)
        return super().run(materialized, worker)


def test_the_submitted_closure_never_captures_the_connection(database: Path) -> None:
    """The structural half: the worker cannot reach SQLite even in principle.

    A comment claiming "workers never touch the database" is worth nothing.
    This asserts the shape that makes it true: the submitted callable is a
    module-level function with no closure cells, so it cannot see the engine
    or its connection, and every non-callable field of the value it receives
    is checked for a connection. `handler` is exempt from that walk on
    purpose: `prepare` and `persist` are allowed to close over the connection
    because the engine only ever calls them on the application thread.
    """
    connection = connect_database(database)
    schedule_probe(connection, "1b" * 32)
    handler = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        )
    )
    scheduler = RecordingScheduler(max_workers=2)
    build_engine(connection, FakeClock(), scheduler=scheduler).execute(
        {handler.task_type: handler}
    )

    assert scheduler.workers, "the engine submitted nothing to the scheduler"
    assert scheduler.submitted, "the engine submitted no work item"
    for worker in scheduler.workers:
        assert inspect.isfunction(worker)
        assert worker.__closure__ is None
        assert getattr(worker, "__self__", None) is None
    for submission in scheduler.submitted:
        assert dataclasses.is_dataclass(submission)
        assert not isinstance(submission, type)
        for field in dataclasses.fields(submission):
            if field.name == "handler":
                continue
            value = getattr(submission, field.name)
            assert not isinstance(value, sqlite3.Connection)
            assert not isinstance(value, RunEngine)
    connection.close()


def test_a_paused_provider_defers_its_work_without_writing_an_attempt(
    database: Path,
) -> None:
    """A paused provider means no call was made, so no attempt row may exist.

    An attempt row stands for exactly one external call. The pause check
    therefore has to happen on the application thread before the attempt row
    is written, not inside the call path.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "2b" * 32)
    calls: list[int] = []
    clock = FakeClock()
    policy = RetryPolicy(
        RetryConfig(
            max_attempts=2,
            jitter_ratio=0.0,
            provider_pause_after_consecutive_exhaustions=1,
        ),
        clock=clock,
    )
    policy.record_exhaustion("probe_provider")
    assert policy.is_paused("probe_provider")

    handler = probe_handler(
        lambda work, ordinal, prepared: (
            calls.append(ordinal) or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        )
    )
    report = build_engine(connection, clock, retry=policy).execute(
        {handler.task_type: handler}
    )

    assert calls == []
    assert repository.attempts_for_run(connection, run_id=report.run_id) == ()
    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.DEFERRED
    assert "paused" in row["reason"]
    connection.close()


def test_at_most_http_workers_calls_are_ever_in_flight(database: Path) -> None:
    """Bounded concurrency, proven with a barrier rather than with timing.

    Four items and two workers. The barrier makes the test fail closed: a
    serializing engine never gets two closures to the barrier at once and
    every call times out, while an engine that submitted all four would push
    `peak` to 4. Only "exactly two in flight, twice" satisfies both
    assertions, and no assertion depends on how long anything takes.

    The attempt-row samples pin the *claim* as well as the concurrency. A pool
    of two caps execution however much the engine claims, so `peak` alone
    cannot tell a batch of two from a batch of ten. Counting committed attempt
    rows while the first pair is in flight can: an over-large batch authorises
    -- and budgets for -- calls it has not started, widening the crash window
    for every item queued behind the pool.
    """
    connection = connect_database(database)
    for index in range(4):
        schedule_probe(connection, f"{index}c" * 32)

    in_flight = 0
    peak = 0
    attempt_rows: list[int] = []
    guard = threading.Lock()
    paired = threading.Barrier(2)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        nonlocal in_flight, peak
        with guard:
            in_flight += 1
            peak = max(peak, in_flight)
        paired.wait(timeout=10)
        reader = connect_database(database, readonly=True)
        try:
            committed = reader.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()
        finally:
            reader.close()
        with guard:
            attempt_rows.append(int(committed["n"]))
            in_flight -= 1
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute)
    report = build_engine(
        connection, FakeClock(), scheduler=BoundedScheduler(max_workers=2)
    ).execute({handler.task_type: handler})

    assert peak == 2
    assert sorted(attempt_rows) == [2, 2, 4, 4]
    assert report.counters.required_succeeded == 4
    connection.close()


def test_backoff_never_sleeps_the_application_thread_while_work_is_ready(
    database: Path,
) -> None:
    """A re-armed item's backoff must not stall an item that is ready now.

    The re-armed item's deadline is 60 virtual seconds away. The second item's
    call is observed at monotonic 0.0, so it was made before that deadline;
    the retry is observed at 60.0, so the engine waited exactly once and only
    when nothing else was runnable. Virtual time makes this deterministic --
    no assertion reads a wall clock.
    """
    connection = connect_database(database)
    retried = schedule_probe(connection, "3c" * 32)
    ready = schedule_probe(connection, "4c" * 32)
    clock = FakeClock()
    observed: list[tuple[int, float]] = []
    guard = threading.Lock()

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        with guard:
            observed.append((work_item.id, clock.monotonic()))
        if work_item.id == retried and ordinal == 1:
            raise ProviderFailure(
                FailureCategory.TIMEOUT,
                provider="probe_provider",
                operation="probe_call",
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute)
    policy = RetryPolicy(
        RetryConfig(
            max_attempts=2,
            jitter_ratio=0.0,
            initial_backoff_seconds=60.0,
            max_backoff_seconds=60.0,
        ),
        clock=clock,
    )
    report = build_engine(connection, clock, retry=policy).execute(
        {handler.task_type: handler}
    )

    at_zero = {item for item, elapsed in observed if elapsed == 0.0}
    assert at_zero == {retried, ready}
    assert (retried, 60.0) in observed
    assert clock.slept == [60.0]
    assert report.state is RunState.COMPLETE
    connection.close()


def test_attempt_ordinals_stay_contiguous_across_re_arms(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "5c" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        if ordinal < 3:
            raise ProviderFailure(
                FailureCategory.TIMEOUT,
                provider="probe_provider",
                operation="probe_call",
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute)
    clock = FakeClock()
    policy = RetryPolicy(RetryConfig(max_attempts=3, jitter_ratio=0.0), clock=clock)
    report = build_engine(connection, clock, retry=policy).execute(
        {handler.task_type: handler}
    )

    rows = connection.execute(
        "SELECT ordinal, outcome FROM attempt WHERE work_item_id = ? ORDER BY id",
        (work_id,),
    ).fetchall()
    assert [row["ordinal"] for row in rows] == [1, 2, 3]
    assert [row["outcome"] for row in rows] == ["failed", "failed", "succeeded"]
    assert report.state is RunState.COMPLETE
    connection.close()


def test_a_re_armed_item_is_pending_with_a_future_deadline_never_deferred(
    database: Path,
) -> None:
    """Re-arming to `deferred` would strand the item for the rest of the run.

    `_CLAIMABLE_PREDICATE` admits a `deferred` item only from a *different*
    run, so a re-arm that used `deferred` would leave the retry unreachable
    until tomorrow. The item's state is sampled from a second connection
    while the retry is still waiting, which is the only moment the wrong
    state would be visible.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "6c" * 32)
    observed: list[tuple[str, str]] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        if ordinal == 1:
            raise ProviderFailure(
                FailureCategory.TIMEOUT,
                provider="probe_provider",
                operation="probe_call",
            )
        reader = connect_database(database, readonly=True)
        try:
            row = reader.execute(
                "SELECT state, eligible_at FROM work_item WHERE id = ?", (work_id,)
            ).fetchone()
        finally:
            reader.close()
        observed.append((row["state"], row["eligible_at"]))
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute)
    clock = FakeClock()
    build_engine(
        connection,
        clock,
        retry=RetryPolicy(RetryConfig(max_attempts=2, jitter_ratio=0.0), clock=clock),
    ).execute({handler.task_type: handler})

    # Sampled during the retry, so the row still carries the re-arm: running
    # (this run re-claimed it) and a deadline one backoff after the failure.
    assert observed == [(WorkState.RUNNING, "2026-07-25T06:00:01.000000Z")]
    connection.close()


def test_prepare_runs_on_the_application_thread_and_feeds_execute(
    database: Path,
) -> None:
    """`prepare` is the only place a handler may read SQLite.

    It runs on the application thread before submission, so it can use the
    engine's own connection -- which is exactly what a stored ETag lookup
    needs -- and hands its value to the call.
    """
    connection = connect_database(database)
    schedule_probe(connection, "7c" * 32)
    application_thread = threading.get_ident()
    prepare_threads: list[int] = []
    seen: list[object] = []

    def prepare(work_item) -> object:
        prepare_threads.append(threading.get_ident())
        row = connection.execute(
            "SELECT fingerprint FROM work_item WHERE id = ?", (work_item.id,)
        ).fetchone()
        return row["fingerprint"]

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        seen.append(prepared)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute, prepare=prepare)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert prepare_threads == [application_thread]
    assert seen == ["7c" * 32]
    connection.close()


def test_persist_commits_with_the_settlement_and_rolls_back_with_it(
    database: Path,
) -> None:
    """A handler's domain writes and its settlement are one transaction."""
    connection = connect_database(database)
    connection.execute("CREATE TABLE probe_note (id INTEGER PRIMARY KEY, note TEXT)")
    first = schedule_probe(connection, "8c" * 32)
    application_thread = threading.get_ident()
    persist_threads: list[int] = []

    def persist(work_item, outcome: TaskOutcome) -> None:
        persist_threads.append(threading.get_ident())
        connection.execute(
            "INSERT INTO probe_note (note) VALUES (?)", (str(outcome.payload),)
        )

    handler = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None, payload="carried"
        ),
        persist=persist,
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert persist_threads == [application_thread]
    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note")]
    assert notes == ["carried"]
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (first,)
        ).fetchone()["state"]
        == WorkState.SUCCEEDED
    )

    # And the other direction: a failing `persist` must take the settlement
    # down with it, leaving the item claimed for the sweep to recover.
    second = schedule_probe(connection, "9c" * 32)

    def failing_persist(work_item, outcome: TaskOutcome) -> None:
        connection.execute("INSERT INTO probe_note (note) VALUES ('doomed')")
        raise RuntimeError("domain write failed")

    doomed = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        ),
        persist=failing_persist,
    )
    with pytest.raises(RuntimeError, match="domain write failed"):
        build_engine(connection, FakeClock()).execute({doomed.task_type: doomed})

    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note")]
    assert notes == ["carried"]
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (second,)
        ).fetchone()["state"]
        == WorkState.RUNNING
    )
    connection.close()


def test_the_handler_supplies_the_attempts_destination_host(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "ac" * 32)
    handler = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        ),
        destination_host=lambda work: "feeds.example.com",
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    attempts = repository.attempts_for_run(connection, run_id=report.run_id)
    assert [row["destination_host"] for row in attempts] == ["feeds.example.com"]
    connection.close()


def test_the_seed_callback_runs_after_run_creation_and_before_any_claim(
    database: Path,
) -> None:
    """Seeding is what gives an ingestion run work to claim at all."""
    connection = connect_database(database)
    seeded: list[int] = []

    def seed(run_id: int) -> None:
        seeded.append(run_id)
        schedule_probe(connection, "bc" * 32)

    handler = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        )
    )
    report = build_engine(connection, FakeClock()).execute(
        {handler.task_type: handler}, seed=seed
    )

    assert seeded == [report.run_id]
    assert report.counters.required_succeeded == 1
    connection.close()


def malformed(retryable: bool = True) -> ProviderFailure:
    return ProviderFailure(
        FailureCategory.MALFORMED_RESPONSE,
        provider="probe_provider",
        operation="probe_call",
        retryable=retryable,
    )


def test_a_second_malformed_response_fails_permanently_and_is_never_retried(
    database: Path,
) -> None:
    """The foreclosed second malformed response is PERMANENT, not EXHAUSTED.

    `RetryPolicy` forecloses a second malformed retry regardless of remaining
    attempt budget, and reports it as `PERMANENT` because it is a real answer
    from the provider. Settling that as `DEFERRED` would leave the item
    claimable by tomorrow's run and repeat a paid generation for content the
    provider has already served twice. There is still attempt budget left
    here (max_attempts=3, two calls made), so nothing but the malformed rule
    can explain the outcome.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "cc" * 32)
    ordinals: list[int] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        ordinals.append(ordinal)
        raise malformed()

    handler = probe_handler(execute)
    clock = FakeClock()
    policy = RetryPolicy(RetryConfig(max_attempts=3, jitter_ratio=0.0), clock=clock)
    report = build_engine(connection, clock, retry=policy).execute(
        {handler.task_type: handler}
    )

    assert ordinals == [1, 2]
    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.FAILED_PERMANENT
    assert row["reason"] == str(FailureCategory.MALFORMED_RESPONSE)
    # A permanent answer never counts towards pausing the provider.
    assert report.paused_providers == frozenset()
    connection.close()


def test_a_first_malformed_response_that_runs_out_of_budget_is_deferred(
    database: Path,
) -> None:
    """The sibling case, which must NOT collapse into the one above.

    One attempt of budget, one malformed response: the retry budget is what
    stops this, not the malformed rule, so the policy reports `EXHAUSTED` and
    the item defers for a later run to try again. Collapsing the two branches
    in either direction is wrong -- this one must not fail permanently, and
    the second malformed response must not defer.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "dc" * 32)
    ordinals: list[int] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        ordinals.append(ordinal)
        raise malformed()

    handler = probe_handler(execute)
    clock = FakeClock()
    policy = RetryPolicy(
        RetryConfig(
            max_attempts=1,
            jitter_ratio=0.0,
            provider_pause_after_consecutive_exhaustions=1,
        ),
        clock=clock,
    )
    report = build_engine(connection, clock, retry=policy).execute(
        {handler.task_type: handler}
    )

    assert ordinals == [1]
    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.DEFERRED
    assert row["reason"] == (
        f"exhausted transient failure: {FailureCategory.MALFORMED_RESPONSE}"
    )
    # Exhausted, but on a real answer, so the provider is still not paused
    # even though one exhaustion would be enough to pause it.
    assert report.paused_providers == frozenset()
    connection.close()
