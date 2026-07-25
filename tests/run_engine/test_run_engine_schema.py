from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


def snapshot_id(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, '{}', '2026-07-25T00:00:00Z')
        """,
        ("a" * 64,),
    )
    return int(cursor.lastrowid)


def start_run(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at
        )
        VALUES ('running', ?, 'Europe/Paris',
                '2026-07-24T00:00:00Z', '2026-07-25T00:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (snapshot_id(connection),),
    )
    return int(cursor.lastrowid)


def add_work(connection: sqlite3.Connection, *, state: str, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1,
                100, '2026-07-25T06:00:00Z', ?, '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (fingerprint, state),
    )
    return int(cursor.lastrowid)


def test_migration_0002_is_applied(connection: sqlite3.Connection) -> None:
    versions = {
        row["version"] for row in connection.execute("SELECT version FROM schema_migration")
    }
    assert {1, 2} <= versions


def test_operations_tables_exist(connection: sqlite3.Connection) -> None:
    names = {
        row["name"]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert {"run", "run_transition", "work_item", "attempt"} <= names


def test_run_state_vocabulary_is_constrained(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE run SET state = 'succeeded' WHERE id = ?", (run_id,))


def test_running_run_must_not_have_a_finish_time(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE run SET finished_at = '2026-07-25T07:00:00Z' WHERE id = ?", (run_id,)
        )


def test_terminal_run_requires_a_finish_time(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE run SET state = 'complete' WHERE id = ?", (run_id,))
    connection.execute(
        "UPDATE run SET state = 'complete', finished_at = '2026-07-25T07:00:00Z' WHERE id = ?",
        (run_id,),
    )


def test_one_active_work_item_per_task_and_fingerprint(connection: sqlite3.Connection) -> None:
    add_work(connection, state="pending", fingerprint="b" * 64)
    with pytest.raises(sqlite3.IntegrityError):
        add_work(connection, state="running", fingerprint="b" * 64)


def test_completed_work_does_not_block_a_new_active_item(connection: sqlite3.Connection) -> None:
    first = add_work(connection, state="pending", fingerprint="c" * 64)
    connection.execute("UPDATE work_item SET state = 'superseded' WHERE id = ?", (first,))
    add_work(connection, state="pending", fingerprint="c" * 64)


def test_attempt_ordinal_is_unique_within_a_work_item(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="d" * 64)
    for ordinal in (1, 2):
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, request_fingerprint
            )
            VALUES (?, ?, 'mediawiki', 'search_pages', ?, '2026-07-25T06:00:00Z', ?)
            """,
            (run_id, work_id, ordinal, "e" * 64),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, request_fingerprint
            )
            VALUES (?, ?, 'mediawiki', 'search_pages', 1, '2026-07-25T06:00:00Z', ?)
            """,
            (run_id, work_id, "e" * 64),
        )


def test_failed_attempt_requires_a_failure_category(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="f" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint
        )
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (run_id, work_id, "0" * 64),
    )
    attempt_id = int(cursor.lastrowid)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE attempt SET outcome = 'failed', finished_at = '2026-07-25T06:00:01Z' WHERE id = ?",
            (attempt_id,),
        )
    connection.execute(
        """
        UPDATE attempt
           SET outcome = 'failed',
               failure_category = 'rate_limit',
               finished_at = '2026-07-25T06:00:01Z'
         WHERE id = ?
        """,
        (attempt_id,),
    )


def test_finished_attempt_requires_an_outcome(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="2" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint
        )
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (run_id, work_id, "3" * 64),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE attempt SET finished_at = '2026-07-25T06:00:01Z' WHERE id = ?",
            (int(cursor.lastrowid),),
        )


def test_money_columns_reject_negative_values(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE run SET budget_reserved_nano_usd = -1 WHERE id = ?", (run_id,)
        )


def test_work_item_requires_an_existing_run_reference(connection: sqlite3.Connection) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_at, updated_at, created_by_run_id
            )
            VALUES ('detect_people', 'source_item', 1, ?, 1, 100,
                    '2026-07-25T06:00:00Z', 'pending',
                    '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z', 9999)
            """,
            ("1" * 64,),
        )
