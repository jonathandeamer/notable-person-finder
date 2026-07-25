from __future__ import annotations

import sqlite3

from notable_person_finder.runs.models import (
    RunRecord,
    RunState,
    SweepResult,
    WorkState,
)


def store_snapshot(
    connection: sqlite3.Connection, *, fingerprint: str, canonical_json: str, now: str
) -> int:
    """Insert the redacted configuration snapshot, reusing an identical one."""
    connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(fingerprint) DO NOTHING
        """,
        (fingerprint, canonical_json, now),
    )
    connection.commit()
    row = connection.execute(
        "SELECT id FROM configuration_snapshot WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()
    return int(row["id"])


def create_run(
    connection: sqlite3.Connection,
    *,
    snapshot_id: int,
    timezone: str,
    window_start: str,
    window_end: str,
    budget_limit_nano_usd: int | None,
    now: str,
) -> int:
    connection.execute("BEGIN IMMEDIATE")
    try:
        cursor = connection.execute(
            """
            INSERT INTO run (
                state, configuration_snapshot_id, timezone,
                window_start, window_end, started_at, budget_limit_nano_usd
            )
            VALUES ('running', ?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, timezone, window_start, window_end, now, budget_limit_nano_usd),
        )
        run_id = int(cursor.lastrowid)
        _insert_transition(connection, run_id=run_id, state=RunState.RUNNING, reason=None, now=now)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return run_id


def _insert_transition(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    state: RunState,
    reason: str | None,
    now: str,
) -> None:
    connection.execute(
        """
        INSERT INTO run_transition (run_id, state, reason, occurred_at)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, str(state), reason, now),
    )


def finish_run(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    state: RunState,
    reason: str | None,
    digest_path: str | None,
    digest_sha256: str | None,
    now: str,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE run
               SET state = ?, finished_at = ?, digest_path = ?, digest_sha256 = ?
             WHERE id = ? AND state = 'running'
            """,
            (str(state), now, digest_path, digest_sha256, run_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"run-{run_id} is already terminal or does not exist")
        _insert_transition(connection, run_id=run_id, state=state, reason=reason, now=now)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def sweep_interrupted(connection: sqlite3.Connection, *, now: str) -> SweepResult:
    """Record abandoned runs, work, and attempts left by a crashed process.

    Runs before any provider call in a new invocation, under the mutation lock,
    so no live run can be swept.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        abandoned = tuple(
            int(row["id"])
            for row in connection.execute("SELECT id FROM run WHERE state = 'running'")
        )
        if not abandoned:
            connection.rollback()
            return SweepResult(runs=(), work_items=0, attempts=0)

        placeholders = ",".join("?" for _ in abandoned)
        attempts = connection.execute(
            f"""
            UPDATE attempt
               SET outcome = 'interrupted', finished_at = ?
             WHERE outcome IS NULL AND run_id IN ({placeholders})
            """,
            (now, *abandoned),
        ).rowcount
        work_items = connection.execute(
            f"""
            UPDATE work_item
               SET state = 'pending', claimed_by_run_id = NULL, updated_at = ?
             WHERE state = 'running' AND claimed_by_run_id IN ({placeholders})
            """,
            (now, *abandoned),
        ).rowcount
        connection.execute(
            f"UPDATE run SET state = 'interrupted', finished_at = ? WHERE id IN ({placeholders})",
            (now, *abandoned),
        )
        for run_id in abandoned:
            _insert_transition(
                connection,
                run_id=run_id,
                state=RunState.INTERRUPTED,
                reason="process ended before a terminal state was recorded",
                now=now,
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return SweepResult(runs=abandoned, work_items=work_items, attempts=attempts)


def _to_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        id=int(row["id"]),
        state=RunState(row["state"]),
        timezone=row["timezone"],
        window_start=row["window_start"],
        window_end=row["window_end"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        budget_limit_nano_usd=row["budget_limit_nano_usd"],
        budget_reserved_nano_usd=row["budget_reserved_nano_usd"],
        budget_actual_nano_usd=row["budget_actual_nano_usd"],
        digest_path=row["digest_path"],
        digest_sha256=row["digest_sha256"],
    )


def load_run(connection: sqlite3.Connection, *, run_id: int) -> RunRecord:
    row = connection.execute("SELECT * FROM run WHERE id = ?", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run-{run_id} does not exist")
    return _to_record(row)


def latest_run(connection: sqlite3.Connection) -> RunRecord | None:
    row = connection.execute("SELECT * FROM run ORDER BY id DESC LIMIT 1").fetchone()
    return None if row is None else _to_record(row)
