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
from notable_person_finder.runs.retry import RetryCoordinator
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
        retry=RetryCoordinator(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock=clock),
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
    """
    inspect = connect_database(database, readonly=True)
    try:
        return {
            "run": [
                (row["id"], row["state"], row["started_at"], row["finished_at"])
                for row in inspect.execute("SELECT * FROM run ORDER BY id")
            ],
            "work_item": [
                (row["id"], row["state"], row["claimed_by_run_id"], row["completed_by_run_id"])
                for row in inspect.execute("SELECT * FROM work_item ORDER BY id")
            ],
            "attempt": [
                (
                    row["id"],
                    row["run_id"],
                    row["ordinal"],
                    row["outcome"],
                    row["finished_at"],
                    row["started_at"],
                )
                for row in inspect.execute("SELECT * FROM attempt ORDER BY id")
            ],
            "run_transition": [
                (row["run_id"], row["state"]) for row in inspect.execute(
                    "SELECT * FROM run_transition ORDER BY id"
                )
            ],
        }
    finally:
        inspect.close()


def crash_in_flight(database: Path) -> None:
    """Run one work item and die inside the provider call, in this process."""
    connection = connect_database(database)

    def crashing(work_item: WorkItem, ordinal: int) -> TaskOutcome:
        # The provider has accepted and charged for the request at this point.
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=crashing
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    # No transaction may be open across the provider boundary; if one were,
    # the evidence of the in-flight call would die with the process instead of
    # surviving on disk. Asserted before the close so the close cannot hide it.
    assert connection.in_transaction is False
    connection.close()


# Executed in a real child process that kills itself with SIGKILL from inside
# the provider call. Nothing unwinds: no `finally`, no `__del__`, no implicit
# rollback, no buffered write flushed on the way out.
_SIGKILL_CHILD = '''
import os
import signal
import sys
from pathlib import Path

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
)
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

database = Path(sys.argv[1])
marker = Path(sys.argv[2])
connection = connect_database(database)


def execute(work_item, ordinal):
    marker.write_text(str(ordinal), encoding="utf-8")
    os.kill(os.getpid(), signal.SIGKILL)
    raise AssertionError("SIGKILL did not end the process")


handler = TaskHandler(
    task_type="probe",
    provider="probe_provider",
    operation="probe_call",
    execute=execute,
)
clock = FakeClock()
RunEngine(
    connection,
    retry=RetryCoordinator(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock=clock),
    scheduler=BoundedScheduler(max_workers=1),
    clock=clock,
    timezone="Europe/Paris",
    window_start="2026-07-24T06:00:00Z",
    budget_limit_nano_usd=None,
    snapshot_fingerprint="a" * 64,
    snapshot_json="{}",
    reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
).execute({handler.task_type: handler})
'''


def sigkill_in_flight(database: Path, workspace: Path) -> None:
    """Run one work item and SIGKILL the process inside the provider call."""
    script = workspace / "sigkill_child.py"
    script.write_text(_SIGKILL_CHILD, encoding="utf-8")
    marker = workspace / "provider-called"
    finished = subprocess.run(
        [sys.executable, str(script), str(database), str(marker)],
        capture_output=True,
        timeout=120,
    )
    assert finished.returncode == -signal.SIGKILL, finished.stderr.decode()
    assert marker.read_text(encoding="utf-8") == "1"


def test_a_crash_after_the_provider_accepted_leaves_an_in_flight_attempt(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule(connection)
    provider_calls: list[int] = []

    def execute(work_item: WorkItem, ordinal: int) -> TaskOutcome:
        provider_calls.append(ordinal)
        # The provider has accepted and charged for the request at this point.
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
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
    # And that shared state is the in-flight one, not an orderly shutdown.
    assert durable_state(killed)["run"] == [(1, RunState.RUNNING, NOW, None)]
    assert durable_state(killed)["attempt"] == [(1, 1, 1, None, None, NOW)]


def test_the_next_run_after_a_real_sigkill_recovers_the_work(tmp_path: Path) -> None:
    database = make_database(tmp_path)
    connection = connect_database(database)
    schedule(connection)
    connection.close()
    sigkill_in_flight(database, tmp_path)

    connection = connect_database(database)
    calls: list[int] = []

    def succeeding(work_item: WorkItem, ordinal: int) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=succeeding
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    assert calls == [2]
    assert report.interrupted_runs == (1,)
    assert report.state is RunState.COMPLETE
    outcomes = [
        row["outcome"] for row in connection.execute("SELECT outcome FROM attempt ORDER BY id")
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

    def succeeding(work_item: WorkItem, ordinal: int) -> TaskOutcome:
        second_calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=succeeding
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    # This is the documented at-least-once window: the request is made again.
    assert second_calls == [2]
    assert report.state is RunState.COMPLETE
    assert report.interrupted_runs == (1,)

    outcomes = [
        row["outcome"] for row in connection.execute("SELECT outcome FROM attempt ORDER BY id")
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

    def execute(work_item: WorkItem, ordinal: int) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    engine_for(connection).execute({handler.task_type: handler})
    engine_for(connection).execute({handler.task_type: handler})

    assert calls == [1]  # the second run found no eligible work
    # One call means exactly one attempt row; a repeat would add a second.
    assert [
        (row["ordinal"], row["outcome"])
        for row in connection.execute("SELECT ordinal, outcome FROM attempt ORDER BY id")
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

    def crash_on_second(work_item: WorkItem, ordinal: int) -> TaskOutcome:
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

    def succeeding(work_item: WorkItem, ordinal: int) -> TaskOutcome:
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
    states = [row["state"] for row in connection.execute("SELECT state FROM run ORDER BY id")]
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
        "SELECT state, claimed_by_run_id, completed_by_run_id FROM work_item WHERE id = ?",
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
