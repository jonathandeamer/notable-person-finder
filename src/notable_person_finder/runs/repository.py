from __future__ import annotations

import sqlite3
from collections.abc import Collection

from notable_person_finder.runs.models import (
    RunCounters,
    RunRecord,
    RunState,
    SweepResult,
    WorkItem,
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


def schedule_work(
    connection: sqlite3.Connection,
    *,
    task_type: str,
    subject_kind: str,
    subject_id: int | None,
    fingerprint: str,
    required: bool,
    priority: int,
    eligible_at: str,
    run_id: int | None,
    now: str,
) -> int:
    """Create the work item, or return the identity of the existing active one.

    Duplicate active scheduling is prevented by `work_item_active_identity`, so
    a caller that rediscovers the same input is a no-op rather than an error.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = connection.execute(
            """
            SELECT id FROM work_item
             WHERE task_type = ? AND fingerprint = ?
               AND state IN ('pending', 'running', 'deferred')
            """,
            (task_type, fingerprint),
        ).fetchone()
        if existing is not None:
            work_id = int(existing["id"])
        else:
            cursor = connection.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    task_type,
                    subject_kind,
                    subject_id,
                    fingerprint,
                    1 if required else 0,
                    priority,
                    eligible_at,
                    run_id,
                    now,
                    now,
                ),
            )
            work_id = int(cursor.lastrowid)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return work_id


def supersede_work(
    connection: sqlite3.Connection,
    *,
    task_type: str,
    fingerprint: str,
    now: str,
    reason: str,
) -> int:
    """Retire active work whose material input has changed."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'superseded', reason = ?, updated_at = ?
             WHERE task_type = ? AND fingerprint = ?
               AND state IN ('pending', 'deferred')
            """,
            (reason, now, task_type, fingerprint),
        ).rowcount
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return changed


def _work_item(row: sqlite3.Row, *, state: WorkState | None = None) -> WorkItem:
    return WorkItem(
        id=int(row["id"]),
        task_type=row["task_type"],
        subject_kind=row["subject_kind"],
        subject_id=row["subject_id"],
        fingerprint=row["fingerprint"],
        required=bool(row["required"]),
        priority=int(row["priority"]),
        state=state or WorkState(row["state"]),
    )


def next_eligible(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Inspect the next eligible item without mutating it."""
    if task_types is not None and not task_types:
        return None
    task_filter = ""
    parameters: list[object] = [run_id, now]
    if task_types is not None:
        ordered = sorted(task_types)
        task_filter = f" AND task_type IN ({','.join('?' for _ in ordered)})"
        parameters.extend(ordered)
    row = connection.execute(
        f"""
        SELECT * FROM work_item
         WHERE (
                   state = 'pending'
                OR (state = 'deferred' AND COALESCE(completed_by_run_id, -1) <> ?)
               )
           AND eligible_at <= ?
           {task_filter}
         ORDER BY priority ASC, id ASC
         LIMIT 1
        """,
        parameters,
    ).fetchone()
    return None if row is None else _work_item(row)


def claim_next(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Claim deterministic work; external calls use claim_and_start_attempt."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        item = next_eligible(
            connection, run_id=run_id, now=now, task_types=task_types
        )
        if item is None:
            connection.rollback()
            return None
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'running', claimed_by_run_id = ?, updated_at = ?
             WHERE id = ? AND state IN ('pending', 'deferred')
            """,
            (run_id, now, item.id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"work item {item.id} was no longer claimable")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return WorkItem(
        id=item.id,
        task_type=item.task_type,
        subject_kind=item.subject_kind,
        subject_id=item.subject_id,
        fingerprint=item.fingerprint,
        required=item.required,
        priority=item.priority,
        state=WorkState.RUNNING,
    )


def complete_work(
    connection: sqlite3.Connection,
    *,
    work_item_id: int,
    run_id: int,
    state: WorkState,
    reason: str | None,
    now: str,
    eligible_at: str | None = None,
) -> None:
    """Record one item's terminal or deferred outcome; failures are isolated."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            UPDATE work_item
               SET state = ?,
                   reason = ?,
                   completed_by_run_id = ?,
                   claimed_by_run_id = NULL,
                   eligible_at = COALESCE(?, eligible_at),
                   updated_at = ?
             WHERE id = ?
            """,
            (str(state), reason, run_id, eligible_at, now, work_item_id),
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def pending_required(
    connection: sqlite3.Connection, *, now: str | None = None
) -> int:
    eligibility = "" if now is None else " AND eligible_at <= ?"
    parameters = () if now is None else (now,)
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*) AS n FROM work_item
             WHERE required = 1 AND state IN ('pending', 'running'){eligibility}
            """,
            parameters,
        ).fetchone()["n"]
    )


def deferred_required(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE required = 1 AND state = 'deferred'"
        ).fetchone()["n"]
    )


def operational_failures_for_run(
    connection: sqlite3.Connection, *, run_id: int
) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE run_id = ? AND outcome = 'failed'",
            (run_id,),
        ).fetchone()["n"]
    )


def run_counters(
    connection: sqlite3.Connection, *, run_id: int, now: str
) -> RunCounters:
    tally = {
        (row["required"], row["state"]): int(row["n"])
        for row in connection.execute(
            """
            SELECT required, state, COUNT(*) AS n
              FROM work_item
             WHERE completed_by_run_id = ?
             GROUP BY required, state
            """,
            (run_id,),
        )
    }
    required_deferred = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE required = 1 AND state = 'deferred' AND eligible_at <= ?
            """,
            (now,),
        ).fetchone()["n"]
    )
    return RunCounters(
        required_succeeded=tally.get((1, "succeeded"), 0),
        required_pending=pending_required(connection, now=now),
        required_deferred=required_deferred,
        required_failed_permanent=tally.get((1, "failed_permanent"), 0),
        optional_succeeded=tally.get((0, "succeeded"), 0),
        optional_skipped=tally.get((0, "superseded"), 0),
        operational_failures=operational_failures_for_run(connection, run_id=run_id),
    )
