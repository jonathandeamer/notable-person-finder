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
    reconcile_in_transaction,
    remaining,
    reservation_nano_usd,
    reserve,
    reserve_in_transaction,
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
    assert cursor.lastrowid is not None
    return cursor.lastrowid


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
    # `remaining() is None` alone would also pass if `reserve` were a no-op
    # under a NULL limit; confirm the reservation actually accumulated.
    row = connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 999_999_999_999
    connection.close()


def test_reconciliation_replaces_the_reservation_with_the_actual_cost(
    database: Path,
) -> None:
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
    assert cursor.lastrowid is not None
    attempt_id = cursor.lastrowid
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
    assert cursor.lastrowid is not None
    attempt_id = cursor.lastrowid
    connection.commit()
    reserve(connection, run_id=run_id, nano_usd=500_000_000)
    reconcile(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
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


def test_reconciling_the_same_attempt_twice_is_refused(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="e" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500000000)
        """,
        (run_id, work_id, "e" * 64),
    )
    assert cursor.lastrowid is not None
    attempt_id = cursor.lastrowid
    connection.commit()
    reserve(connection, run_id=run_id, nano_usd=500_000_000)

    reconcile(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        reserved_nano_usd=500_000_000,
        actual_nano_usd=120_000_000,
    )

    with pytest.raises(RuntimeError):
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
    connection.close()


def test_reconciling_with_no_prior_reserve_is_refused(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="f" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500000000)
        """,
        (run_id, work_id, "f" * 64),
    )
    assert cursor.lastrowid is not None
    attempt_id = cursor.lastrowid
    connection.commit()

    with pytest.raises(RuntimeError):
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
    assert row["budget_reserved_nano_usd"] == 0
    assert row["budget_actual_nano_usd"] == 0
    connection.close()


def test_reserve_in_transaction_refuses_a_connection_without_one(
    database: Path,
) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    assert not connection.in_transaction
    with pytest.raises(RuntimeError, match="active transaction"):
        reserve_in_transaction(connection, run_id=run_id, nano_usd=100_000_000)
    connection.close()


def test_reserve_in_transaction_succeeds_inside_a_caller_owned_transaction(
    database: Path,
) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.execute("BEGIN IMMEDIATE")
    reserve_in_transaction(connection, run_id=run_id, nano_usd=400_000_000)
    connection.commit()
    assert remaining(connection, run_id=run_id) == 600_000_000
    connection.close()


def test_reconcile_in_transaction_refuses_a_connection_without_one(
    database: Path,
) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    assert not connection.in_transaction
    with pytest.raises(RuntimeError, match="active transaction"):
        reconcile_in_transaction(
            connection,
            run_id=run_id,
            reserved_nano_usd=100_000_000,
            actual_nano_usd=50_000_000,
        )
    connection.close()


def test_reconcile_in_transaction_succeeds_inside_a_caller_owned_transaction(
    database: Path,
) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.execute("BEGIN IMMEDIATE")
    reserve_in_transaction(connection, run_id=run_id, nano_usd=500_000_000)
    reconcile_in_transaction(
        connection,
        run_id=run_id,
        reserved_nano_usd=500_000_000,
        actual_nano_usd=120_000_000,
    )
    connection.commit()
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 120_000_000
    assert row["budget_actual_nano_usd"] == 120_000_000
    connection.close()


def test_concurrent_reservations_cannot_oversubscribe(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.close()

    outcomes: list[str | BaseException] = []
    guard = threading.Lock()
    start = threading.Event()

    def worker() -> None:
        own = connect_database(database)
        try:
            start.wait(timeout=5)
            reserve(own, run_id=run_id, nano_usd=300_000_000)
        except BaseException as exc:  # noqa: BLE001 - recorded, not swallowed
            with guard:
                outcomes.append(exc)
        else:
            with guard:
                outcomes.append("granted")
        finally:
            own.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    start.set()
    for thread in threads:
        thread.join(timeout=10)

    assert len(outcomes) == 8, "a worker thread never reported an outcome"
    granted = [outcome for outcome in outcomes if outcome == "granted"]
    refused = [outcome for outcome in outcomes if outcome != "granted"]
    assert len(granted) == 3
    # Every non-granted thread must have been refused specifically by the
    # budget cap. If BEGIN IMMEDIATE contention instead raised
    # sqlite3.OperationalError, catching only BudgetExhausted would hide that
    # as a passing test while granted still happened to equal 3.
    assert len(refused) == 5
    assert all(isinstance(outcome, BudgetExhausted) for outcome in refused), refused

    verify = connect_database(database, readonly=True)
    assert (
        verify.execute(
            "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
        ).fetchone()["budget_reserved_nano_usd"]
        == 900_000_000
    )
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
    connection.execute(
        "UPDATE run SET budget_reserved_nano_usd = 0 WHERE id = ?", (run_id,)
    )
    assert connection.in_transaction
    with pytest.raises(sqlite3.OperationalError, match="within a transaction"):
        reserve(connection, run_id=run_id, nano_usd=100_000_000)
    connection.rollback()
    connection.close()


def test_reservation_uses_supplied_input_tokens() -> None:
    """Kills a reservation that ignores the request size it is handed.

    The input side must scale with `input_tokens`. A helper that returned a
    fixed figure -- or that reached past its argument for a configured
    ceiling -- would make every request reserve the same amount, which is the
    defect this whole change exists to remove.
    """
    prices = {
        "prompt_unit_price_nano_usd": 150,
        "completion_unit_price_nano_usd": 600,
        "max_completion_tokens": 512,
    }
    small = reservation_nano_usd(input_tokens=3_400, **prices)
    large = reservation_nano_usd(input_tokens=65_536, **prices)

    assert small < large
    assert small == 150 * 3_400 + 600 * 512
    assert large == 150 * 65_536 + 600 * 512


def test_reservation_keeps_the_completion_side_at_the_configured_ceiling() -> None:
    """Pins K3: output length is not knowable before the call.

    Positive control for the test above -- it proves the completion term is
    actually reachable, so `small < large` there is about the input side and
    not about a completion term that never contributes.
    """
    prices = {
        "prompt_unit_price_nano_usd": 150,
        "completion_unit_price_nano_usd": 600,
        "input_tokens": 3_400,
    }
    assert reservation_nano_usd(max_completion_tokens=512, **prices) < (
        reservation_nano_usd(max_completion_tokens=4_096, **prices)
    )
