from __future__ import annotations

import sqlite3
from decimal import ROUND_CEILING, Decimal, InvalidOperation

NEGATIVE_PRICING_DETAIL = "unit pricing must not be negative"
OVERFLOW_PRICING_DETAIL = "worst-case reservation overflows integer nano-USD"

# SQLite INTEGER / attempt reservation bound (signed 64-bit).
_MAX_SQLITE_INTEGER = (1 << 63) - 1

# These reserve/reconcile helpers are driven by `RunEngine.execute` through
# `repository.start_attempt`/`finish_attempt` on every attempt -- see
# tests/run_engine/test_engine.py's budget-reservation and
# budget-exhaustion-reporting tests -- and no longer wait on a provider
# adapter to exercise them.


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
    # Deliberately duplicated with the check in `reserve_in_transaction`: this
    # one rejects before opening a transaction at all, so a bad call never
    # takes the write lock. Do not "de-duplicate" this away.
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
    # Deliberately duplicated with the check in `reserve`: this call site has
    # no wrapper to catch it, so it must validate independently. Do not
    # "de-duplicate" this away.
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
    if reserved_nano_usd < 0:
        raise ValueError("a reconciled reservation must not be negative")
    if actual_nano_usd is not None and actual_nano_usd < 0:
        raise ValueError("a reconciled actual cost must not be negative")
    if actual_nano_usd is None:
        return

    row = connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} does not exist")
    current_reserved = row["budget_reserved_nano_usd"]
    if reserved_nano_usd > current_reserved:
        # Refuse rather than clamp to zero: clamping would silently absorb an
        # inconsistent reconciliation (a double reconcile of the same
        # attempt, or a reconcile with no matching prior reserve) and let
        # real spend vanish from budget_reserved_nano_usd, the only figure
        # `remaining()` reads. That is precisely backwards for a spend cap,
        # so this is a caller-bug RuntimeError, not a clamp.
        raise RuntimeError(
            f"reconciliation for run {run_id} releases {reserved_nano_usd} "
            f"nano-USD but only {current_reserved} nano-USD is currently "
            "reserved"
        )
    connection.execute(
        """
        UPDATE run
           SET budget_reserved_nano_usd = budget_reserved_nano_usd - ? + ?,
               budget_actual_nano_usd = budget_actual_nano_usd + ?
         WHERE id = ?
        """,
        (reserved_nano_usd, actual_nano_usd, actual_nano_usd, run_id),
    )


def _checked_product(unit_price_nano_usd: int, tokens: int) -> int:
    """Ceiling unit_price * tokens as a non-negative SQLite integer."""
    if unit_price_nano_usd < 0:
        raise ValueError(NEGATIVE_PRICING_DETAIL)
    if tokens < 0:
        raise ValueError("token bound must not be negative")
    try:
        product = (Decimal(unit_price_nano_usd) * Decimal(tokens)).to_integral_value(
            rounding=ROUND_CEILING
        )
    except InvalidOperation as error:
        raise ValueError(OVERFLOW_PRICING_DETAIL) from error
    if product < 0 or product > _MAX_SQLITE_INTEGER:
        raise ValueError(OVERFLOW_PRICING_DETAIL)
    return int(product)


def reservation_nano_usd(
    *,
    prompt_unit_price_nano_usd: int,
    completion_unit_price_nano_usd: int,
    input_tokens: int,
    max_completion_tokens: int,
) -> int:
    """Budget to reserve for one generation, before the call is made.

    ``input_tokens`` is this specific request's measured size, taken from the
    render's ``worst_case_input_tokens`` -- not the task's configured
    ``max_input_tokens`` ceiling. Charging the ceiling reserved roughly 31x
    real spend on the first ten-feed run and deferred 219 of 236 items at 5%
    real cap usage; see
    docs/superpowers/specs/2026-08-02-budget-reservation-sizing-design.md.

    The measured size is a UTF-8 **byte** count charged as a token count, which
    leaves roughly a 4x margin over real token cost. That margin is deliberate
    (K2): it needs no tokenizer, so reservation stays deterministic and
    offline, and over-reserving against a known request size is the safe
    direction for a spend cap. Reconciliation replaces the reserve with actual
    cost, so the margin costs in-run headroom only.

    The completion side stays worst-case (K3): output length is not knowable
    before the call.
    """
    prompt_cost = _checked_product(prompt_unit_price_nano_usd, input_tokens)
    completion_cost = _checked_product(
        completion_unit_price_nano_usd, max_completion_tokens
    )
    total = prompt_cost + completion_cost
    if total > _MAX_SQLITE_INTEGER:
        raise ValueError(OVERFLOW_PRICING_DETAIL)
    return total
