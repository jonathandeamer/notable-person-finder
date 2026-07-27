from __future__ import annotations

import signal
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
)
from notable_person_finder.runs.models import RunState, WorkItem, WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import BoundedScheduler

NOW = "2026-07-25T06:00:00Z"


class SimulatedCrash(BaseException):
    """Stands in for process death: not catchable as an ordinary Exception."""


def make_database(directory: Path) -> Path:
    path = directory / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, directory / "backups")
    connection.close()
    return path


@pytest.fixture
def database(tmp_path: Path) -> Path:
    return make_database(tmp_path)


def engine_for(connection: sqlite3.Connection) -> RunEngine:
    clock = FakeClock()
    return RunEngine(
        connection,
        retry=RetryPolicy(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock=clock),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
    )


def schedule(connection: sqlite3.Connection) -> int:
    return repository.schedule_work(
        connection,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="b" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )


def durable_state(database: Path) -> dict[str, list[tuple[object, ...]]]:
    """Everything a recovering process can see, read through a fresh handle.

    Deliberately reopens the file rather than reusing the connection that
    "crashed", so the result is what the last COMMITTED transaction left on
    disk and nothing that only exists in a live connection's memory.

    Every column of every row is compared, not a hand-picked projection: a
    projection would silently exempt whatever it left out -- reservations,
    actual cost, request fingerprints, digest paths -- from the claim that two
    crashes leave the same state.
    """
    inspect = connect_database(database, readonly=True)
    try:
        return {
            table: [
                tuple(row)
                for row in inspect.execute(f"SELECT * FROM {table} ORDER BY id")
            ]
            for table in ("run", "work_item", "attempt", "run_transition")
        }
    finally:
        inspect.close()


def shape(database: Path) -> dict[str, list[tuple[object, ...]]]:
    """The few columns a human reads when describing an on-disk crash shape."""
    inspect = connect_database(database, readonly=True)
    try:
        return {
            "run": [
                (row["id"], row["state"], row["finished_at"])
                for row in inspect.execute("SELECT * FROM run ORDER BY id")
            ],
            "work_item": [
                (
                    row["id"],
                    row["state"],
                    row["claimed_by_run_id"],
                    row["completed_by_run_id"],
                )
                for row in inspect.execute("SELECT * FROM work_item ORDER BY id")
            ],
            "attempt": [
                (row["id"], row["run_id"], row["ordinal"], row["outcome"])
                for row in inspect.execute("SELECT * FROM attempt ORDER BY id")
            ],
        }
    finally:
        inspect.close()


def crash_in_flight(database: Path) -> None:
    """Run one work item and die inside the provider call, in this process."""
    connection = connect_database(database)

    def crashing(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        # The provider has accepted and charged for the request at this point.
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=crashing,
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    # No transaction may be open across the provider boundary; if one were,
    # the evidence of the in-flight call would die with the process instead of
    # surviving on disk. Asserted before the close so the close cannot hide it.
    assert connection.in_transaction is False
    connection.close()


def crash_before_settle(database: Path) -> None:
    """Die between the two transactions of step 3, in this process.

    The provider answered and `finish_attempt` committed the attempt as
    `succeeded`; the process then dies before `complete_work` commits the work
    item's outcome. Patching `repository.complete_work` is the seam because the
    engine resolves it as a module attribute at call time, so the substitution
    is exactly the call the engine would have made.
    """
    connection = connect_database(database)

    def succeeding(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    def dying(*args: object, **kwargs: object) -> None:
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=succeeding,
    )
    original = repository.complete_work
    repository.complete_work = dying  # type: ignore[assignment]
    try:
        with pytest.raises(SimulatedCrash):
            engine_for(connection).execute({handler.task_type: handler})
    finally:
        repository.complete_work = original  # type: ignore[assignment]
    assert connection.in_transaction is False
    connection.close()


# Executed in a real child process that kills itself with SIGKILL. `sys.argv[3]`
# selects the kill point: "provider" kills inside the external call, "settle"
# kills between the two transactions of step 3. Nothing unwinds: no `finally`,
# no `__del__`, no implicit rollback, no buffered write flushed on the way out.
_SIGKILL_CHILD = """
import os
import signal
import sys
from pathlib import Path

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
)
from notable_person_finder.runs.models import WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import BoundedScheduler

database = Path(sys.argv[1])
marker = Path(sys.argv[2])
kill_point = sys.argv[3] if len(sys.argv) > 3 else "provider"
connection = connect_database(database)


def die():
    os.kill(os.getpid(), signal.SIGKILL)
    raise AssertionError("SIGKILL did not end the process")


def execute(work_item, ordinal, prepared):
    marker.write_text(str(ordinal), encoding="utf-8")
    if kill_point == "provider":
        die()
    return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)


if kill_point == "settle":
    def _dying_complete_work(*args, **kwargs):
        die()

    repository.complete_work = _dying_complete_work


handler = TaskHandler(
    task_type="probe",
    provider="probe_provider",
    operation="probe_call",
    execute=execute,
)
clock = FakeClock()
RunEngine(
    connection,
    retry=RetryPolicy(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock=clock),
    scheduler=BoundedScheduler(max_workers=1),
    clock=clock,
    timezone="Europe/Paris",
    window_start="2026-07-24T06:00:00Z",
    budget_limit_nano_usd=None,
    snapshot_fingerprint="a" * 64,
    snapshot_json="{}",
    reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
).execute({handler.task_type: handler})
"""


def sigkill_at(database: Path, workspace: Path, kill_point: str) -> None:
    """Run one work item and SIGKILL the process at `kill_point`."""
    script = workspace / f"sigkill_child_{kill_point}.py"
    script.write_text(_SIGKILL_CHILD, encoding="utf-8")
    marker = workspace / f"provider-called-{kill_point}"
    finished = subprocess.run(
        [sys.executable, str(script), str(database), str(marker), kill_point],
        capture_output=True,
        timeout=120,
    )
    assert finished.returncode == -signal.SIGKILL, finished.stderr.decode()
    # The kill really happened after the provider was called, not before.
    assert marker.read_text(encoding="utf-8") == "1"


def sigkill_in_flight(database: Path, workspace: Path) -> None:
    """Run one work item and SIGKILL the process inside the provider call."""
    sigkill_at(database, workspace, "provider")


def test_a_crash_after_the_provider_accepted_leaves_an_in_flight_attempt(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule(connection)
    provider_calls: list[int] = []

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        provider_calls.append(ordinal)
        # The provider has accepted and charged for the request at this point.
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    assert connection.in_transaction is False
    connection.close()

    # The process died: the attempt row exists with no outcome, and the run is
    # still 'running' because no terminal state was ever written.
    inspect = connect_database(database, readonly=True)
    attempt = inspect.execute("SELECT outcome, finished_at FROM attempt").fetchone()
    assert attempt["outcome"] is None
    assert attempt["finished_at"] is None
    assert inspect.execute("SELECT state FROM run").fetchone()["state"] == "running"
    # The claim is durable too, so the item cannot be picked up by anything
    # other than the sweep.
    work = inspect.execute("SELECT state, claimed_by_run_id FROM work_item").fetchone()
    assert work["state"] == WorkState.RUNNING
    assert work["claimed_by_run_id"] == 1
    inspect.close()
    assert provider_calls == [1]
    assert work_id > 0


def test_a_real_sigkill_leaves_exactly_what_the_simulated_crash_leaves(
    tmp_path: Path,
) -> None:
    """The simulation is only worth anything if it matches process death.

    A `SIGKILL` inside the provider call cannot be caught, cannot run a
    `finally`, and cannot flush or roll back anything. If the in-process
    simulation left even one extra committed row -- a settled work item, a
    finished attempt, a terminal run transition -- the two dumps would differ.
    """
    simulated_root = tmp_path / "simulated"
    simulated_root.mkdir()
    simulated = make_database(simulated_root)
    connection = connect_database(simulated)
    schedule(connection)
    connection.close()
    crash_in_flight(simulated)

    killed_root = tmp_path / "killed"
    killed_root.mkdir()
    killed = make_database(killed_root)
    connection = connect_database(killed)
    schedule(connection)
    connection.close()
    sigkill_in_flight(killed, killed_root)

    assert durable_state(simulated) == durable_state(killed)
    # Equality of two empty dumps would prove nothing.
    assert all(rows for rows in durable_state(killed).values())
    # And that shared state is the in-flight one, not an orderly shutdown: two
    # clean runs could never satisfy these.
    assert shape(killed) == {
        "run": [(1, RunState.RUNNING, None)],
        "work_item": [(1, WorkState.RUNNING, 1, None)],
        "attempt": [(1, 1, 1, None)],
    }


def test_the_next_run_after_a_real_sigkill_recovers_the_work(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    connection = connect_database(database)
    schedule(connection)
    connection.close()
    sigkill_in_flight(database, tmp_path)

    connection = connect_database(database)
    calls: list[int] = []

    def succeeding(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=succeeding,
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    assert calls == [2]
    assert report.interrupted_runs == (1,)
    assert report.state is RunState.COMPLETE
    outcomes = [
        row["outcome"]
        for row in connection.execute("SELECT outcome FROM attempt ORDER BY id")
    ]
    assert outcomes == ["interrupted", "succeeded"]
    connection.close()


def test_the_next_run_records_the_interruption_and_repeats_the_call(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule(connection)
    connection.close()
    crash_in_flight(database)

    # A fresh process starts: no resume mode, just another ordinary run.
    connection = connect_database(database)
    second_calls: list[int] = []

    def succeeding(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        second_calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=succeeding,
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    # This is the documented at-least-once window: the request is made again.
    assert second_calls == [2]
    assert report.state is RunState.COMPLETE
    assert report.interrupted_runs == (1,)

    outcomes = [
        row["outcome"]
        for row in connection.execute("SELECT outcome FROM attempt ORDER BY id")
    ]
    assert outcomes == ["interrupted", "succeeded"]
    # The observed transition sequence matches the one seen empirically for a
    # real SIGINT during the digest write: running, interrupted, running,
    # complete.
    transitions = [
        row["state"]
        for row in connection.execute("SELECT state FROM run_transition ORDER BY id")
    ]
    assert transitions == ["running", "interrupted", "running", "complete"]
    connection.close()


def test_a_persisted_result_is_never_repeated(database: Path) -> None:
    connection = connect_database(database)
    schedule(connection)
    calls: list[int] = []

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )
    engine_for(connection).execute({handler.task_type: handler})
    engine_for(connection).execute({handler.task_type: handler})

    assert calls == [1]  # the second run found no eligible work
    # One call means exactly one attempt row; a repeat would add a second.
    assert [
        (row["ordinal"], row["outcome"])
        for row in connection.execute(
            "SELECT ordinal, outcome FROM attempt ORDER BY id"
        )
    ] == [(1, "succeeded")]
    connection.close()


def test_a_persisted_result_survives_a_later_crash_and_is_not_repeated(
    database: Path,
) -> None:
    """Reuse must hold across the crash boundary, not just across clean runs.

    Two items are scheduled. The first succeeds and is committed; the process
    then dies inside the second item's provider call. The recovery run must
    repeat only the interrupted item.
    """
    connection = connect_database(database)
    first = schedule(connection)
    second = repository.schedule_work(
        connection,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="c" * 64,
        required=True,
        priority=200,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )
    calls: list[int] = []

    def crash_on_second(
        work_item: WorkItem, ordinal: int, prepared: object
    ) -> TaskOutcome:
        calls.append(work_item.id)
        if work_item.id == second:
            raise SimulatedCrash
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=crash_on_second,
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    assert connection.in_transaction is False
    connection.close()
    assert calls == [first, second]

    connection = connect_database(database)
    recovered: list[int] = []

    def succeeding(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        recovered.append(work_item.id)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    engine_for(connection).execute(
        {
            "probe": TaskHandler(
                task_type="probe",
                provider="probe_provider",
                operation="probe_call",
                execute=succeeding,
            )
        }
    )
    assert recovered == [second]
    connection.close()


def test_an_interrupted_run_is_not_reported_as_complete(database: Path) -> None:
    connection = connect_database(database)
    schedule(connection)
    connection.close()
    crash_in_flight(database)

    connection = connect_database(database)
    engine_for(connection).execute({})
    states = [
        row["state"] for row in connection.execute("SELECT state FROM run ORDER BY id")
    ]
    assert states[0] == RunState.INTERRUPTED
    connection.close()


def test_abandoned_work_returns_to_pending_not_to_deferred(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule(connection)
    connection.close()
    crash_in_flight(database)

    connection = connect_database(database)
    swept = repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert swept.runs == (1,)
    assert swept.work_items == 1
    assert swept.attempts == 1
    row = connection.execute(
        "SELECT state, claimed_by_run_id, completed_by_run_id FROM work_item "
        "WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row["state"] == WorkState.PENDING
    # Pending, not deferred: a deferral would stamp completed_by_run_id and
    # keep the item out of the very next run's peek, which would silently
    # convert "retry the interrupted call" into "skip it".
    assert row["completed_by_run_id"] is None
    # The sweep only unclaims rows whose claimed_by_run_id names an abandoned
    # run, which is safe only because the claim writes state and
    # claimed_by_run_id in one statement. No running row may have a NULL claim.
    assert row["claimed_by_run_id"] is None
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE state = 'running' "
            "AND claimed_by_run_id IS NULL"
        ).fetchone()["n"]
        == 0
    )
    connection.close()


def test_a_crash_between_the_two_step_three_transactions_leaves_a_settled_attempt(
    tmp_path: Path,
) -> None:
    """Step 3 is two transactions, and the gap between them is a second window.

    `finish_attempt` and `complete_work` each open their own `BEGIN IMMEDIATE`.
    A crash between them commits the attempt as `succeeded` while leaving the
    work item `running` and claimed. The sweep only touches attempts whose
    `outcome IS NULL`, so this attempt is never marked `interrupted`: the
    duplicate call the next run makes is traceable ONLY through the predecessor
    run's `interrupted` state, never through the attempt rows.

    If this test ever fails because step 3 became one transaction, that is a
    fix, not a regression -- but `docs/architecture/at-least-once-execution.md`
    must be corrected in the same change, because this test is what makes that
    document's "what a crash looks like on disk" section true.
    """
    simulated_root = tmp_path / "simulated"
    simulated_root.mkdir()
    simulated = make_database(simulated_root)
    connection = connect_database(simulated)
    schedule(connection)
    connection.close()
    crash_before_settle(simulated)

    killed_root = tmp_path / "killed"
    killed_root.mkdir()
    killed = make_database(killed_root)
    connection = connect_database(killed)
    schedule(connection)
    connection.close()
    sigkill_at(killed, killed_root, "settle")

    # Real process death and the in-process simulation agree here too.
    assert durable_state(simulated) == durable_state(killed)
    assert all(rows for rows in durable_state(killed).values())
    assert shape(killed) == {
        "run": [(1, RunState.RUNNING, None)],
        # Settled attempt, UNSETTLED work item: the shape that distinguishes
        # this window from the in-flight one.
        "work_item": [(1, WorkState.RUNNING, 1, None)],
        "attempt": [(1, 1, 1, "succeeded")],
    }


def test_the_next_run_repeats_a_call_whose_attempt_was_already_settled(
    tmp_path: Path,
) -> None:
    """The recovery is correct, and the duplicate is invisible in `attempt`."""
    database = make_database(tmp_path)
    connection = connect_database(database)
    schedule(connection)
    connection.close()
    sigkill_at(database, tmp_path, "settle")

    connection = connect_database(database)
    calls: list[int] = []

    def succeeding(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=succeeding,
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    # At-least-once holds: no work is lost, and the sweep did recover the item.
    assert calls == [2]
    assert report.interrupted_runs == (1,)
    assert report.state is RunState.COMPLETE
    assert (
        connection.execute("SELECT state FROM work_item").fetchone()["state"]
        == WorkState.SUCCEEDED
    )

    # But nothing in the attempt rows marks the first call as lost. Two
    # succeeded attempts against the same request fingerprint, and no
    # 'interrupted' row anywhere -- this is the honest, and more dangerous,
    # difference from the in-flight window.
    attempts = list(
        connection.execute(
            "SELECT run_id, ordinal, outcome, provider, operation, request_fingerprint "
            "FROM attempt ORDER BY id"
        )
    )
    assert [row["outcome"] for row in attempts] == ["succeeded", "succeeded"]
    assert [row["run_id"] for row in attempts] == [1, 2]
    assert len({row["request_fingerprint"] for row in attempts}) == 1
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE outcome = 'interrupted'"
        ).fetchone()["n"]
        == 0
    )

    # The only durable trace of the duplicate is the predecessor run's state.
    assert (
        connection.execute("SELECT state FROM run WHERE id = 1").fetchone()["state"]
        == RunState.INTERRUPTED
    )
    connection.close()
