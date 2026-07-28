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
from notable_person_finder.runs.clock import FakeClock, utc_timestamp
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    RunReport,
    TaskHandler,
    TaskOutcome,
    derive_run_state,
)
from notable_person_finder.runs.models import RunState, WorkItem, WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import BoundedScheduler

# Derived from the production renderer rather than written out, so a change to
# the canonical timestamp format cannot leave the fixture seeding rows in a
# format the engine no longer produces. It previously read
# `"2026-07-25T06:00:00Z"`, which stopped matching once `utc_timestamp` began
# emitting microseconds unconditionally -- and because `eligible_at <= now` is
# a TEXT comparison, the mismatch made every seeded item unclaimable rather
# than failing an assertion outright.
NOW = utc_timestamp(FakeClock().now())


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


def test_a_failure_before_any_request_left_the_machine_releases_its_reservation(
    database: Path,
) -> None:
    """A `ProviderFailure` that HttpTransport raises before `client.send` --
    an unsafe-URL rejection or a request-construction failure -- carries
    `FailureCategory.CONFIGURATION` with no `status_code`, because no HTTP
    response was ever obtained. Such an attempt cannot have been charged, so
    the engine must record `actual_nano_usd = 0` for it and release the
    reservation, rather than retaining it the way an ordinary failure does.
    """
    connection = connect_database(database)
    schedule_probe(connection, "e" * 64)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.CONFIGURATION,
            provider="feeds",
            operation="fetch_feed",
            detail="unsafe request destination",
        )

    handler = TaskHandler(
        task_type="probe",
        provider="feeds",
        operation="fetch_feed",
        execute=execute,
        reserved_nano_usd=50,
    )
    report = build_engine(connection, FakeClock(), budget_limit_nano_usd=1000).execute(
        {handler.task_type: handler}
    )
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (report.run_id,),
    ).fetchone()
    assert (row["budget_reserved_nano_usd"], row["budget_actual_nano_usd"]) == (0, 0)
    attempts = repository.attempts_for_run(connection, run_id=report.run_id)
    assert len(attempts) == 1
    assert attempts[0]["actual_nano_usd"] == 0
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


def test_deferred_reasons_match_the_whole_queue_headline_across_two_runs(
    database: Path,
) -> None:
    """`required_deferred` is a deliberately whole-queue count (see
    `run_counters`'s docstring), so the reason breakdown must describe that
    same population -- not just the reasons the most recent run itself
    recorded.

    Run A defers 5 required items (3 of task type `probe_a`, 2 of
    `probe_b`) for budget. Run B only registers a handler for `probe_b`, so
    it reclaims and re-defers only those 2 -- the 3 `probe_a` items are still
    eligible-and-deferred, but were last settled by run A, not run B. If the
    breakdown were scoped to `completed_by_run_id = run B`, it would report
    only the 2 items run B touched, while `required_deferred` (whole-queue)
    still reports all 5 -- a total that does not sum to its own breakdown.
    """
    connection = connect_database(database)
    for fingerprint in ("a1" * 32, "a2" * 32, "a3" * 32):
        repository.schedule_work(
            connection,
            task_type="probe_a",
            subject_kind="synthetic",
            subject_id=None,
            fingerprint=fingerprint,
            required=True,
            priority=100,
            eligible_at=NOW,
            run_id=None,
            now=NOW,
        )
    for fingerprint in ("b1" * 32, "b2" * 32):
        repository.schedule_work(
            connection,
            task_type="probe_b",
            subject_kind="synthetic",
            subject_id=None,
            fingerprint=fingerprint,
            required=True,
            priority=100,
            eligible_at=NOW,
            run_id=None,
            now=NOW,
        )

    def always_refused(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler_a = TaskHandler(
        task_type="probe_a",
        provider="openrouter",
        operation="generate_structured",
        execute=always_refused,
        reserved_nano_usd=1,
    )
    handler_b = TaskHandler(
        task_type="probe_b",
        provider="openrouter",
        operation="generate_structured",
        execute=always_refused,
        reserved_nano_usd=1,
    )
    # A zero-USD cap refuses every reservation, so both task types defer all
    # five items in run A with the same "not_evaluated_budget" reason.
    run_a = build_engine(connection, FakeClock(), budget_limit_nano_usd=0).execute(
        {handler_a.task_type: handler_a, handler_b.task_type: handler_b}
    )
    assert run_a.counters.required_deferred == 5
    assert run_a.deferred_reasons == {"not_evaluated_budget": 5}

    # Run B only has a handler for probe_b, so claim_batch's task_types
    # filter means it can reclaim and re-defer only the 2 probe_b items --
    # the 3 probe_a items stay deferred, still attributed to run A.
    run_b = build_engine(
        connection,
        FakeClock(start=datetime(2026, 7, 25, 6, 0, 30, tzinfo=UTC)),
        budget_limit_nano_usd=0,
    ).execute({handler_b.task_type: handler_b})

    assert run_b.counters.required_deferred == 5
    # The whole-queue breakdown must sum to the whole-queue headline: 3
    # probe_a items still carrying run A's reason, plus the 2 probe_b items
    # run B just re-recorded under the same reason string.
    assert run_b.deferred_reasons == {"not_evaluated_budget": 5}
    rows = connection.execute(
        "SELECT task_type, completed_by_run_id FROM work_item "
        "WHERE state = 'deferred' ORDER BY task_type"
    ).fetchall()
    assert [row["task_type"] for row in rows] == [
        "probe_a",
        "probe_a",
        "probe_a",
        "probe_b",
        "probe_b",
    ]
    assert [row["completed_by_run_id"] for row in rows] == [
        run_a.run_id,
        run_a.run_id,
        run_a.run_id,
        run_b.run_id,
        run_b.run_id,
    ]
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
    """The tally is built on the application thread only; the lock is defensive.

    `on_attempt` no longer exists -- the batch-claim reshape moved tally
    construction entirely onto the application thread in `persist`, so nothing
    concurrent mutates it today. The lock is retained anyway, against a future
    off-thread tally reintroducing exactly the race it guards against, so this
    test pins its presence rather than asserting it is load-bearing right now.
    """
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

    # And the other direction: a failing `persist` rolls its own settlement
    # back (the domain write never lands), but the item is isolated rather
    # than left stranded -- it is settled a second time as
    # `failed_permanent` with no domain writes, and the run still finishes.
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
    build_engine(connection, FakeClock()).execute({doomed.task_type: doomed})

    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note")]
    assert notes == ["carried"]
    second_row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (second,)
    ).fetchone()
    assert second_row["state"] == WorkState.FAILED_PERMANENT
    assert "RuntimeError" in second_row["reason"]
    connection.close()


def test_a_handler_whose_prepare_raises_settles_that_item_and_lets_siblings_run(
    database: Path,
) -> None:
    """`prepare` runs before any call is made, so a raising `prepare` is
    unambiguous: nothing has been charged. The engine isolates it to the one
    item -- settled `failed_permanent` with a diagnostic reason naming the
    exception type -- rather than abandoning the whole batch, so a sibling
    item that already committed an attempt row and a budget reservation still
    gets its call made."""
    connection = connect_database(database)
    bad_id = schedule_probe(connection, "p1" * 32)
    good_id = schedule_probe(connection, "p2" * 32)

    def prepare(work_item) -> object:
        if work_item.id == bad_id:
            raise ValueError("prepare blew up")
        return None

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute, prepare=prepare)
    reported: list[RunReport] = []

    def spy_reporter(report: RunReport) -> ReportArtifact:
        reported.append(report)
        return ReportArtifact(path="fake-digest.md", sha256="e" * 64, markdown="x")

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

    bad_row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (bad_id,)
    ).fetchone()
    good_row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (good_id,)
    ).fetchone()
    assert bad_row["state"] == WorkState.FAILED_PERMANENT
    assert "ValueError" in bad_row["reason"]
    # The reason carries the exception TYPE and never its message. `reason`
    # reaches both the digest and the `work_item` table, and a handler that
    # interpolates remote text into its exception would leak it there. A
    # regression swapping `type(error).__name__` for `str(error)` passes
    # every other assertion in this test, so pin the absence explicitly.
    assert "blew up" not in bad_row["reason"]
    assert good_row["state"] == WorkState.SUCCEEDED
    assert report.counters.required_succeeded == 1
    # The failed item's contribution to the report is the operator-visible
    # half of this change: the digest must show it, not silently drop it.
    assert report.counters.required_failed_permanent == 1
    assert len(reported) == 1
    connection.close()


def test_a_handler_whose_persist_raises_settles_that_item_without_its_domain_writes(
    database: Path,
) -> None:
    """`persist` runs inside the settlement transaction, so a raising
    `persist` rolls that settlement back too -- but the call already
    happened, so leaving the item `running` forever would be wrong. The
    engine settles it a second time as `failed_permanent` with no domain
    writes, and a sibling item still succeeds and the run still reports."""
    connection = connect_database(database)
    connection.execute("CREATE TABLE probe_note2 (id INTEGER PRIMARY KEY, note TEXT)")
    bad_id = schedule_probe(connection, "p3" * 32)
    good_id = schedule_probe(connection, "p4" * 32)

    def persist(work_item, outcome: TaskOutcome) -> None:
        if work_item.id == bad_id:
            connection.execute(
                "INSERT INTO probe_note2 (note) VALUES ('should not land')"
            )
            raise RuntimeError("persist blew up")
        connection.execute("INSERT INTO probe_note2 (note) VALUES ('good')")

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = probe_handler(execute, persist=persist)
    reported: list[RunReport] = []

    def spy_reporter(report: RunReport) -> ReportArtifact:
        reported.append(report)
        return ReportArtifact(path="fake-digest.md", sha256="f" * 64, markdown="x")

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

    bad_row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (bad_id,)
    ).fetchone()
    good_row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (good_id,)
    ).fetchone()
    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note2")]
    assert bad_row["state"] == WorkState.FAILED_PERMANENT
    assert "RuntimeError" in bad_row["reason"]
    assert "blew up" not in bad_row["reason"]
    assert good_row["state"] == WorkState.SUCCEEDED
    assert notes == ["good"]
    assert report.counters.required_succeeded == 1
    assert report.counters.required_failed_permanent == 1
    assert len(reported) == 1
    connection.close()


def test_a_raising_execute_still_propagates_uncaught(database: Path) -> None:
    """Pins the ruling: unlike `prepare` and `persist`, `execute` is NOT
    isolated. A call that raised may already have left the machine and
    succeeded, so settling it here would risk recording a possibly-successful
    paid call as `failed_permanent` and never retrying it. The exception must
    propagate uncaught, leaving the item `running` for the next run's sweep
    to recover as `interrupted`."""
    connection = connect_database(database)
    work_id = schedule_probe(connection, "ex" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ValueError("execute blew up")

    handler = probe_handler(execute)
    with pytest.raises(ValueError, match="execute blew up"):
        build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.RUNNING
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


# --- constraining handler-supplied reason (pre-6b) ------------------------------


def test_a_handler_supplied_reason_with_embedded_newlines_is_collapsed(
    database: Path,
) -> None:
    """A newline in a settled reason is not cosmetic: the digest renders the
    reason inside a Markdown list item, so an embedded newline breaks the
    list structure and would let remote text inject its own lines. Every run
    of whitespace -- including tabs -- collapses to one space.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "10" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(
            state=WorkState.DEFERRED,
            reason="line one\nline two\ttab   three",
        )

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "line one line two tab three"
    connection.close()


def test_a_handler_supplied_reason_with_non_whitespace_control_characters_is_stripped(
    database: Path,
) -> None:
    """Control characters that are not whitespace (a bell, an escape) are
    dropped outright rather than collapsed into a space: neither has a
    legitimate place in a digest bullet or a `GROUP BY` key, and unlike a
    newline or tab they carry no argument for being rendered as a space
    either.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "14" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason="bad\x07bell\x1bescape")

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "badbellescape"
    connection.close()


def test_a_handler_supplied_reason_far_over_120_characters_is_truncated_visibly(
    database: Path,
) -> None:
    """The stored reason is at most 120 characters, with the truncation
    itself visible -- a trailing `...` inside the 120, not appended past it --
    so an operator reading the digest can tell the value was cut rather than
    reading a suspiciously round classification token.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "11" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason="x" * 200)

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "x" * 117 + "..."
    assert len(row["reason"]) == 120
    connection.close()


def test_a_reason_at_exactly_120_characters_survives_byte_identical(
    database: Path,
) -> None:
    """Pins the truncation boundary from the other side: `>` rather than
    `>=` at 120 must not be a fluke the 200-character test alone can't catch.
    A reason of exactly 120 characters must survive untouched -- no marker,
    no shortening -- or a stray `>=` here would silently truncate a
    legitimate reason with no test failing.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "15" * 32)
    exactly_120 = "y" * 120

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason=exactly_120)

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == exactly_120
    connection.close()


def test_a_reason_at_121_characters_is_truncated_to_120(database: Path) -> None:
    """The sibling of the 120-character pin: one character over the bound
    must truncate, with the marker landing inside the 120, not past it.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "16" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason="z" * 121)

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "z" * 117 + "..."
    assert len(row["reason"]) == 120
    connection.close()


def test_a_handler_deferring_with_no_reason_still_counts_in_the_breakdown(
    database: Path,
) -> None:
    """`TaskOutcome(state=DEFERRED, reason=None)` type-checks -- nothing stops
    a handler from returning it -- but `_sanitize_reason` leaves `None` as
    `None` on purpose, so the door has to be closed in `_settle` itself, and
    only for `DEFERRED`: `repository.deferred_reasons` filters `reason IS NOT
    NULL` while `required_deferred` does not, so a `NULL` reason on a
    deferred item would reopen the exact sum mismatch Task 4's fix round
    closed.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "17" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason=None)

    handler = probe_handler(execute)
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "unspecified"
    assert report.deferred_reasons == {"unspecified": 1}
    connection.close()


def test_a_succeeded_item_with_no_reason_stays_null(database: Path) -> None:
    """The fix for a `None` reason on a deferred item is deliberately scoped
    to `DEFERRED`: a succeeded item with no reason is ordinary, and `NULL` is
    the correct, unchanged value there -- this must not regress into
    `"unspecified"` for every settling state.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "18" * 32)
    handler = succeeding_handler([])

    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] is None
    connection.close()


def test_a_zero_width_only_reason_becomes_unspecified(database: Path) -> None:
    """A reason made of only zero-width or bidi-override characters is not
    empty by `str.strip()` -- so without widening the control-character class
    beyond ASCII, this would survive as a blank-looking digest bullet and
    mint its own `GROUP BY` key instead of becoming `"unspecified"`.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "19" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason="​​")

    handler = probe_handler(execute)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "unspecified"
    connection.close()


def test_a_whitespace_only_reason_becomes_unspecified_and_still_counts_in_the_breakdown(
    database: Path,
) -> None:
    """An empty result after sanitisation must become the stable literal
    `"unspecified"`, never `None`: `repository.deferred_reasons` filters
    `reason IS NOT NULL` while the `required_deferred` headline does not, so a
    `NULL` reason on a deferred item would make the breakdown stop summing to
    its own headline -- the exact mismatch Task 4's fix round closed.
    """
    connection = connect_database(database)
    work_id = schedule_probe(connection, "12" * 32)

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.DEFERRED, reason="   ")

    handler = probe_handler(execute)
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    row = connection.execute(
        "SELECT reason FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["reason"] == "unspecified"
    assert report.deferred_reasons == {"unspecified": 1}
    connection.close()


def test_the_engines_own_pause_and_exhaustion_literals_round_trip_byte_identical(
    database: Path,
) -> None:
    """The sanitiser must not mangle the stable grouping keys the engine
    authors for itself. This exercises two of them in one run: the pause
    token `_prepare` deliberately keeps stable across providers, and the
    `"exhausted transient failure: {category}"` token `_resolve` produces --
    both must reach `work_item.reason` exactly as written.
    """
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
    engine.execute({handler.task_type: handler})

    reasons = {
        row["reason"]
        for row in connection.execute("SELECT DISTINCT reason FROM work_item")
    }
    assert reasons == {
        f"exhausted transient failure: {FailureCategory.PROVIDER_UNAVAILABLE}",
        "provider paused for this run",
    }
    connection.close()


# --- persist_failure: domain writes for a settlement with no outcome ----------


def test_persist_failure_runs_for_a_permanent_failure_inside_its_settlement(
    database: Path,
) -> None:
    """A non-retryable failure settles with no `TaskOutcome`, so `persist`
    cannot run -- yet the attempt genuinely happened and a handler owning
    domain history needs to record it. `persist_failure` is that seam, and it
    runs inside the settling transaction exactly as `persist` does.

    `access_denied` is not in `RETRYABLE_CATEGORIES`, so the retry policy
    forecloses it as PERMANENT on the first attempt.
    """
    connection = connect_database(database)
    connection.execute("CREATE TABLE probe_note (id INTEGER PRIMARY KEY, note TEXT)")
    item = schedule_probe(connection, "d1" * 32)
    application_thread = threading.get_ident()
    seen: list[tuple[int, str, str | None]] = []

    def persist_failure(work_item, failure: ProviderFailure) -> None:
        seen.append((threading.get_ident(), str(failure.category), failure.detail))
        connection.execute(
            "INSERT INTO probe_note (note) VALUES (?)", (str(failure.category),)
        )

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(execute, persist_failure=persist_failure)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert seen == [
        (application_thread, str(FailureCategory.ACCESS_DENIED), "forbidden")
    ]
    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note")]
    assert notes == [str(FailureCategory.ACCESS_DENIED)]
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (item,)
        ).fetchone()["state"]
        == WorkState.FAILED_PERMANENT
    )
    connection.close()


def test_persist_failure_runs_once_when_a_transient_failure_exhausts(
    database: Path,
) -> None:
    """The EXHAUSTED settlement is the other outcome-less path, and it is the
    one that matters operationally: a feed down all week defers every day and
    would otherwise leave no domain trace at all.

    Called once per *settlement*, not once per attempt -- `max_attempts=2`
    makes two calls here and the hook must still fire exactly once, or a
    caller counting rows would double-count every retry.
    """
    connection = connect_database(database)
    item = schedule_probe(connection, "d2" * 32)
    calls: list[int] = []
    failures: list[str] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        calls.append(ordinal)
        raise ProviderFailure(
            FailureCategory.NETWORK,
            provider="probe_provider",
            operation="probe_call",
            detail="connection reset",
        )

    handler = probe_handler(
        execute,
        persist_failure=lambda work_item, failure: failures.append(
            str(failure.category)
        ),
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert calls == [1, 2]
    assert failures == [str(FailureCategory.NETWORK)]
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (item,)
        ).fetchone()["state"]
        == WorkState.DEFERRED
    )
    connection.close()


def test_persist_failure_rolls_back_with_its_settlement_and_isolates_the_item(
    database: Path,
) -> None:
    """Same transactional contract as `persist`, and the same isolation when it
    raises: the domain write never lands, and the item is settled a second time
    as `failed_permanent` rather than stranded `running` for every future run
    to reclaim and fail identically."""
    connection = connect_database(database)
    connection.execute("CREATE TABLE probe_note (id INTEGER PRIMARY KEY, note TEXT)")
    item = schedule_probe(connection, "d3" * 32)

    def failing_persist_failure(work_item, failure: ProviderFailure) -> None:
        connection.execute("INSERT INTO probe_note (note) VALUES ('doomed')")
        raise RuntimeError("domain write failed")

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(execute, persist_failure=failing_persist_failure)
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    notes = [row["note"] for row in connection.execute("SELECT note FROM probe_note")]
    assert notes == []
    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (item,)
    ).fetchone()
    assert row["state"] == WorkState.FAILED_PERMANENT
    assert "RuntimeError" in row["reason"]
    connection.close()


def test_persist_failure_does_not_run_when_the_handler_returned_an_outcome(
    database: Path,
) -> None:
    """The two hooks are disjoint by construction. A handler that translates a
    failure into a settling `TaskOutcome` itself -- which is exactly what the
    feed handler does for `response_too_large` -- must get `persist`, not
    `persist_failure`, or its refusal would be recorded twice under two
    different shapes.

    Both items run through one engine pass on one handler that has *both*
    hooks bound, so the routing is decided per settlement rather than per
    handler. Each list is the other's positive control: neither empty
    assertion can pass vacuously, because the same run makes the other fire.
    """
    connection = connect_database(database)
    translated = schedule_probe(connection, "d4" * 32)
    raising = schedule_probe(connection, "d5" * 32)
    persisted: list[str] = []
    failed: list[str] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        if work_item.id == translated:
            return TaskOutcome(
                state=WorkState.DEFERRED, reason="translated", payload="carried"
            )
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(
        execute,
        persist=lambda work_item, outcome: persisted.append(str(outcome.payload)),
        persist_failure=lambda work_item, failure: failed.append(str(failure.category)),
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    # The translated deferral took `persist` and nothing else; the untranslated
    # failure took `persist_failure` and nothing else.
    assert persisted == ["carried"]
    assert failed == [str(FailureCategory.ACCESS_DENIED)]
    states = {
        row["id"]: row["state"]
        for row in connection.execute("SELECT id, state FROM work_item")
    }
    assert states[translated] == WorkState.DEFERRED
    assert states[raising] == WorkState.FAILED_PERMANENT
    connection.close()


def test_persist_failure_does_not_run_for_a_settlement_with_no_failure(
    database: Path,
) -> None:
    """Budget refusal: `reserved_nano_usd` exceeds the run's limit, so
    `start_attempt` raises `BudgetExhausted` before any call is made. No call
    means no failed attempt to record, and a handler told otherwise would write
    a fetch row for a request that never left the machine -- worse than the gap
    the hook was added to close.

    This is one of *four* outcome-less settlements; the siblings below cover a
    paused provider, a raising `prepare`, and a non-settling handler state.
    Each is tested separately rather than in one case, because a single test
    naming four paths and exercising one is exactly how a rule ends up asserted
    in prose and unenforced in fact.

    The second half is the positive control: the identical handler, given a
    budget, reaches its provider and fires the hook -- so the empty assertion
    cannot pass merely because the hook was never wired up.
    """
    connection = connect_database(database)
    refused = schedule_probe(connection, "e1" * 32)
    failed: list[str] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(
        execute,
        reserved_nano_usd=500,
        persist_failure=lambda work_item, failure: failed.append(str(failure.category)),
    )
    build_engine(connection, FakeClock(), budget_limit_nano_usd=0).execute(
        {handler.task_type: handler}
    )

    row = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?", (refused,)
    ).fetchone()
    assert row["state"] == WorkState.DEFERRED
    assert row["reason"] == "not_evaluated_budget"
    assert failed == []

    # Positive control: same handler, same hook, a budget that permits the call.
    build_engine(connection, FakeClock(), budget_limit_nano_usd=10_000).execute(
        {handler.task_type: handler}
    )
    assert failed == [str(FailureCategory.ACCESS_DENIED)]
    connection.close()


def test_a_settlement_may_not_carry_both_an_outcome_and_a_failure(
    database: Path,
) -> None:
    """`_settle` picks the domain write from whichever of the two it was given.
    Passing both would make that choice depend on argument order, so it is
    refused outright rather than silently resolved. No engine path does this
    today; the guard exists so that a future one cannot introduce it quietly.

    The guard is checked before any database access, so this needs no run and
    no claimed item -- which is the point: it isolates the rule from every
    other reason a settlement can fail.
    """
    connection = connect_database(database)
    handler = probe_handler(
        lambda work, ordinal, prepared: TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        )
    )
    engine = build_engine(connection, FakeClock())
    work_item = WorkItem(
        id=1,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="e2" * 32,
        required=True,
        priority=100,
        state=WorkState.RUNNING,
    )

    with pytest.raises(RuntimeError, match="both an outcome and a provider failure"):
        engine._settle(  # pyright: ignore[reportPrivateUsage]
            1,
            work_item,
            handler,
            state=WorkState.FAILED_PERMANENT,
            reason="both",
            outcome=TaskOutcome(state=WorkState.SUCCEEDED, reason=None),
            failure=ProviderFailure(
                FailureCategory.ACCESS_DENIED,
                provider="probe_provider",
                operation="probe_call",
                detail="forbidden",
            ),
        )
    connection.close()


def test_persist_failure_does_not_run_when_the_provider_is_paused(
    database: Path,
) -> None:
    """A paused provider is never called, so there is nothing to record.

    The pause check sits ahead of the attempt row for exactly this reason: an
    attempt row stands for one external call. `provider_pause_after_...=1`
    means the first item's exhaustion pauses the provider, so the second and
    third items settle through the pause branch without ever reaching a worker.

    The first item is the positive control -- it *does* fire the hook -- so the
    count below pins "once, for the one item that was actually called" rather
    than the weaker "not more than once".
    """
    connection = connect_database(database)
    for index in range(3):
        schedule_probe(connection, f"{index}f" * 32)
    calls: list[int] = []
    failed: list[str] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        calls.append(work_item.id)
        raise ProviderFailure(
            FailureCategory.NETWORK,
            provider="probe_provider",
            operation="probe_call",
            detail="connection reset",
        )

    handler = probe_handler(
        execute,
        persist_failure=lambda work_item, failure: failed.append(str(failure.category)),
    )
    clock = FakeClock()
    build_engine(
        connection,
        clock,
        retry=RetryPolicy(
            RetryConfig(
                max_attempts=1,
                jitter_ratio=0.0,
                provider_pause_after_consecutive_exhaustions=1,
            ),
            clock=clock,
        ),
        scheduler=BoundedScheduler(max_workers=1),
    ).execute({handler.task_type: handler})

    # One item was called and recorded; the two the pause caught were not.
    assert len(calls) == 1
    assert failed == [str(FailureCategory.NETWORK)]
    reasons = [
        row["reason"]
        for row in connection.execute(
            "SELECT reason FROM work_item WHERE reason = 'provider paused for this run'"
        )
    ]
    assert len(reasons) == 2
    connection.close()


def test_persist_failure_does_not_run_when_prepare_raises(database: Path) -> None:
    """A raising `prepare` settles the item before any call is made.

    The sibling item is the positive control: the same handler, same hook, an
    item whose `prepare` returns normally reaches the provider and records its
    failure -- so `failed` having exactly one entry distinguishes "the hook
    skipped the prepare-raised item" from "the hook never ran at all".
    """
    connection = connect_database(database)
    bad = schedule_probe(connection, "g1" * 32)
    good = schedule_probe(connection, "g2" * 32)
    failed: list[int] = []

    def prepare(work_item) -> object:
        if work_item.id == bad:
            raise ValueError("prepare blew up")
        return None

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(
        execute,
        prepare=prepare,
        persist_failure=lambda work_item, failure: failed.append(work_item.id),
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert failed == [good]
    states = {
        row["id"]: row["state"]
        for row in connection.execute("SELECT id, state FROM work_item")
    }
    assert states[bad] == WorkState.FAILED_PERMANENT
    assert states[good] == WorkState.FAILED_PERMANENT
    connection.close()


def test_persist_failure_does_not_run_for_a_non_settling_handler_state(
    database: Path,
) -> None:
    """A handler returning a non-settling state has an outcome, but the engine
    refuses to honour it and settles the item `failed_permanent` itself,
    deliberately discarding the payload (`engine.py:817-827`).

    That settlement therefore carries neither an outcome nor a failure, and it
    must not fire this hook either: a state the engine refused to trust is not
    a trustworthy instruction to write domain data. The call *did* happen here,
    unlike the other three paths, so this is the one case where "no hook" costs
    a real record -- see the known gap noted for Task 10.

    The sibling is the positive control.
    """
    connection = connect_database(database)
    rogue = schedule_probe(connection, "h1" * 32)
    honest = schedule_probe(connection, "h2" * 32)
    failed: list[int] = []

    def execute(work_item, ordinal: int, prepared: object) -> TaskOutcome:
        if work_item.id == rogue:
            # `running` is not a settling state.
            return TaskOutcome(state=WorkState.RUNNING, reason=None, payload="ignored")
        raise ProviderFailure(
            FailureCategory.ACCESS_DENIED,
            provider="probe_provider",
            operation="probe_call",
            detail="forbidden",
        )

    handler = probe_handler(
        execute,
        persist=lambda work_item, outcome: failed.append(-work_item.id),
        persist_failure=lambda work_item, failure: failed.append(work_item.id),
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    # Neither hook ran for the rogue item; the honest sibling recorded normally.
    assert failed == [honest]
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (rogue,)
        ).fetchone()["state"]
        == WorkState.FAILED_PERMANENT
    )
    connection.close()
