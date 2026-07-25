from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs.budget import (
    BudgetExhausted,
    reconcile,
    remaining,
    reserve,
)
from tests.run_engine.test_run_engine_schema import add_work, snapshot_id


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, tmp_path / "backups")
    connection.close()
    return path


def make_run(connection: sqlite3.Connection, limit_nano_usd: int | None) -> int:
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at, budget_limit_nano_usd
        )
        VALUES ('running', ?, 'Europe/Paris',
                '2026-07-24T00:00:00Z', '2026-07-25T00:00:00Z',
                '2026-07-25T06:00:00Z', ?)
        """,
        (snapshot_id(connection), limit_nano_usd),
    )
    connection.commit()
    return int(cursor.lastrowid)


def test_reservation_reduces_the_remaining_allowance(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    reserve(connection, run_id=run_id, nano_usd=400_000_000)
    assert remaining(connection, run_id=run_id) == 600_000_000
    connection.close()


def test_reservation_beyond_the_cap_is_refused(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    reserve(connection, run_id=run_id, nano_usd=900_000_000)
    with pytest.raises(BudgetExhausted):
        reserve(connection, run_id=run_id, nano_usd=200_000_000)
    assert remaining(connection, run_id=run_id) == 100_000_000
    connection.close()


def test_a_refused_reservation_leaves_no_partial_state(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    with pytest.raises(BudgetExhausted):
        reserve(connection, run_id=run_id, nano_usd=2_000_000_000)
    assert remaining(connection, run_id=run_id) == 1_000_000_000
    assert not connection.in_transaction
    connection.close()


def test_no_cap_means_unlimited_reservations(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, None)
    reserve(connection, run_id=run_id, nano_usd=999_999_999_999)
    assert remaining(connection, run_id=run_id) is None
    connection.close()


def test_reconciliation_replaces_the_reservation_with_the_actual_cost(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="a" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500000000)
        """,
        (run_id, work_id, "b" * 64),
    )
    attempt_id = int(cursor.lastrowid)
    connection.commit()
    reserve(connection, run_id=run_id, nano_usd=500_000_000)

    reconcile(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        reserved_nano_usd=500_000_000,
        actual_nano_usd=120_000_000,
    )

    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 120_000_000
    assert row["budget_actual_nano_usd"] == 120_000_000
    assert (
        connection.execute(
            "SELECT actual_nano_usd FROM attempt WHERE id = ?", (attempt_id,)
        ).fetchone()["actual_nano_usd"]
        == 120_000_000
    )
    connection.close()


def test_reconciling_an_unreported_cost_keeps_the_reservation(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="c" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500000000)
        """,
        (run_id, work_id, "d" * 64),
    )
    connection.commit()
    reserve(connection, run_id=run_id, nano_usd=500_000_000)
    reconcile(
        connection,
        run_id=run_id,
        attempt_id=int(cursor.lastrowid),
        reserved_nano_usd=500_000_000,
        actual_nano_usd=None,
    )
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 500_000_000
    assert row["budget_actual_nano_usd"] == 0
    connection.close()


def test_concurrent_reservations_cannot_oversubscribe(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.close()

    granted = 0
    guard = threading.Lock()
    start = threading.Event()

    def worker() -> None:
        nonlocal granted
        own = connect_database(database)
        try:
            start.wait(timeout=5)
            reserve(own, run_id=run_id, nano_usd=300_000_000)
        except BudgetExhausted:
            return
        else:
            with guard:
                granted += 1
        finally:
            own.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    start.set()
    for thread in threads:
        thread.join(timeout=10)

    assert granted == 3
    verify = connect_database(database, readonly=True)
    assert verify.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 900_000_000
    verify.close()


def test_negative_reservation_is_rejected(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    with pytest.raises(ValueError):
        reserve(connection, run_id=run_id, nano_usd=-1)
    connection.close()


def test_reserve_refuses_to_reuse_a_connection_already_mid_transaction(
    database: Path,
) -> None:
    """`reserve` must always take its own BEGIN IMMEDIATE lock.

    If it silently reused an already-open transaction instead, the read of
    the remaining allowance could happen without the write lock BEGIN
    IMMEDIATE guarantees, which is exactly the race this module exists to
    close. A connection left mid-transaction must make `reserve` fail loudly
    rather than adapt to it.
    """
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.execute("UPDATE run SET budget_reserved_nano_usd = 0 WHERE id = ?", (run_id,))
    assert connection.in_transaction
    with pytest.raises(sqlite3.OperationalError):
        reserve(connection, run_id=run_id, nano_usd=100_000_000)
    connection.rollback()
    connection.close()
