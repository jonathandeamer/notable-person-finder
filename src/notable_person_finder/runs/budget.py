from __future__ import annotations

import sqlite3


class BudgetExhausted(Exception):
    """The configured hard per-run cap cannot cover this reservation."""

    def __init__(self, *, requested_nano_usd: int, remaining_nano_usd: int) -> None:
        self.requested_nano_usd = requested_nano_usd
        self.remaining_nano_usd = remaining_nano_usd
        super().__init__(
            f"reservation of {requested_nano_usd} nano-USD exceeds the "
            f"{remaining_nano_usd} nano-USD remaining in the run budget"
        )


def remaining(connection: sqlite3.Connection, *, run_id: int) -> int | None:
    row = connection.execute(
        "SELECT budget_limit_nano_usd, budget_reserved_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} does not exist")
    if row["budget_limit_nano_usd"] is None:
        return None
    return row["budget_limit_nano_usd"] - row["budget_reserved_nano_usd"]


def reserve(connection: sqlite3.Connection, *, run_id: int, nano_usd: int) -> None:
    """Reserve a conservative maximum cost, or refuse the call entirely.

    Opens its own BEGIN IMMEDIATE transaction so that a concurrently scheduled
    external call cannot read the same remaining allowance and spend it twice.
    """
    if nano_usd < 0:
        raise ValueError("a budget reservation must not be negative")

    connection.execute("BEGIN IMMEDIATE")
    try:
        reserve_in_transaction(connection, run_id=run_id, nano_usd=nano_usd)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def reserve_in_transaction(
    connection: sqlite3.Connection, *, run_id: int, nano_usd: int
) -> None:
    """Reserve within the caller's existing write transaction."""
    if nano_usd < 0:
        raise ValueError("a budget reservation must not be negative")
    if not connection.in_transaction:
        raise RuntimeError("reserve_in_transaction requires an active transaction")
    row = connection.execute(
        "SELECT budget_limit_nano_usd, budget_reserved_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} does not exist")
    limit = row["budget_limit_nano_usd"]
    reserved = row["budget_reserved_nano_usd"]
    if limit is not None and reserved + nano_usd > limit:
        raise BudgetExhausted(
            requested_nano_usd=nano_usd, remaining_nano_usd=limit - reserved
        )
    connection.execute(
        "UPDATE run SET budget_reserved_nano_usd = ? WHERE id = ?",
        (reserved + nano_usd, run_id),
    )


def reconcile(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    reserved_nano_usd: int,
    actual_nano_usd: int | None,
) -> None:
    """Replace a reservation with the provider-reported actual cost.

    When the provider reports no cost the reservation is retained: releasing it
    would let an unmeasured call escape the cap.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        reconcile_in_transaction(
            connection,
            run_id=run_id,
            reserved_nano_usd=reserved_nano_usd,
            actual_nano_usd=actual_nano_usd,
        )
        connection.execute(
            "UPDATE attempt SET actual_nano_usd = ? WHERE id = ?",
            (actual_nano_usd, attempt_id),
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def reconcile_in_transaction(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    reserved_nano_usd: int,
    actual_nano_usd: int | None,
) -> None:
    """Replace a reservation inside the caller's finishing transaction."""
    if not connection.in_transaction:
        raise RuntimeError("reconcile_in_transaction requires an active transaction")
    if actual_nano_usd is None:
        return
    connection.execute(
        """
        UPDATE run
           SET budget_reserved_nano_usd =
                   MAX(budget_reserved_nano_usd - ? + ?, 0),
               budget_actual_nano_usd = budget_actual_nano_usd + ?
         WHERE id = ?
        """,
        (reserved_nano_usd, actual_nano_usd, actual_nano_usd, run_id),
    )
