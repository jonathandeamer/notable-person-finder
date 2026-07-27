from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.models import RunState, WorkState

WINDOW = ("2026-07-24T06:00:00Z", "2026-07-25T06:00:00Z")


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


def new_run(connection: sqlite3.Connection, *, at: str = "2026-07-25T06:00:00Z") -> int:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=at
    )
    return repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start=WINDOW[0],
        window_end=WINDOW[1],
        budget_limit_nano_usd=None,
        now=at,
    )


def add_pending_work(connection: sqlite3.Connection, *, run_id: int, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 100,
                '2026-07-25T06:00:00Z', 'pending', ?,
                '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (fingerprint, run_id),
    )
    connection.commit()
    return int(cursor.lastrowid)


def test_snapshot_is_stored_once_per_fingerprint(connection: sqlite3.Connection) -> None:
    first = repository.store_snapshot(
        connection, fingerprint="b" * 64, canonical_json="{}", now="2026-07-25T06:00:00Z"
    )
    second = repository.store_snapshot(
        connection, fingerprint="b" * 64, canonical_json="{}", now="2026-07-25T07:00:00Z"
    )
    assert first == second


def test_creating_a_run_records_the_running_transition(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    record = repository.load_run(connection, run_id=run_id)
    assert record.state is RunState.RUNNING
    assert record.finished_at is None
    assert record.human_id == f"run-{run_id}"
    states = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id", (run_id,)
        )
    ]
    assert states == ["running"]


def test_every_invocation_creates_a_new_run_on_the_same_day(connection: sqlite3.Connection) -> None:
    first = new_run(connection)
    second = new_run(connection)
    assert first != second


def test_finishing_a_run_sets_state_time_and_transition(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path="/tmp/2026-07-25-run-1.md",
        digest_sha256="c" * 64,
        now="2026-07-25T06:30:00Z",
    )
    record = repository.load_run(connection, run_id=run_id)
    assert record.state is RunState.COMPLETE
    assert record.finished_at == "2026-07-25T06:30:00Z"
    assert record.digest_path == "/tmp/2026-07-25-run-1.md"
    states = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id", (run_id,)
        )
    ]
    assert states == ["running", "complete"]


def test_sweep_marks_an_abandoned_run_interrupted(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.runs == (abandoned,)
    record = repository.load_run(connection, run_id=abandoned)
    assert record.state is RunState.INTERRUPTED
    assert record.finished_at == "2026-07-25T07:00:00Z"


def test_sweep_returns_abandoned_running_work_to_pending(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    work_id = add_pending_work(connection, run_id=abandoned, fingerprint="d" * 64)
    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (abandoned, work_id),
    )
    connection.commit()

    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.work_items == 1
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.PENDING
    assert row["claimed_by_run_id"] is None


def test_sweep_marks_in_flight_attempts_interrupted(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    work_id = add_pending_work(connection, run_id=abandoned, fingerprint="e" * 64)
    connection.execute(
        """
        INSERT INTO attempt (run_id, work_item_id, provider, operation, ordinal,
                             started_at, request_fingerprint)
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (abandoned, work_id, "f" * 64),
    )
    connection.commit()

    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.attempts == 1
    row = connection.execute("SELECT outcome, finished_at FROM attempt").fetchone()
    assert row["outcome"] == "interrupted"
    assert row["finished_at"] == "2026-07-25T07:00:00Z"


def test_sweep_leaves_completed_runs_alone(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path=None,
        digest_sha256=None,
        now="2026-07-25T06:30:00Z",
    )
    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.runs == ()
    assert repository.load_run(connection, run_id=run_id).state is RunState.COMPLETE


def test_latest_run_returns_the_most_recent(connection: sqlite3.Connection) -> None:
    new_run(connection)
    second = new_run(connection)
    assert repository.latest_run(connection).id == second


def test_latest_run_is_none_on_an_empty_database(connection: sqlite3.Connection) -> None:
    assert repository.latest_run(connection) is None


def test_a_run_cannot_be_finished_twice(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path=None,
        digest_sha256=None,
        now="2026-07-25T06:30:00Z",
    )
    with pytest.raises(RuntimeError, match="already terminal"):
        repository.finish_run(
            connection,
            run_id=run_id,
            state=RunState.FAILED,
            reason="late reporting failure",
            digest_path=None,
            digest_sha256=None,
            now="2026-07-25T06:31:00Z",
        )
