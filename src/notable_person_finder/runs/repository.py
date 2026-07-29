from __future__ import annotations

import dataclasses
import sqlite3
from collections.abc import Callable, Collection, Mapping

from notable_person_finder.runs.budget import (
    reconcile_in_transaction,
    reserve_in_transaction,
)
from notable_person_finder.runs.models import (
    RunCounters,
    RunRecord,
    RunState,
    SweepResult,
    WorkItem,
    WorkState,
)
from notable_person_finder.runs.retry import AttemptRecord


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    """The row id of a just-completed INSERT.

    `sqlite3` types `lastrowid` as optional because it is None before any
    INSERT on the cursor. Every call site here has just inserted exactly one
    row, so None means the statement did not do what the caller assumed and
    must fail loudly rather than propagate a bad id.
    """
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT did not produce a row id")
    return row_id


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
            (
                snapshot_id,
                timezone,
                window_start,
                window_end,
                now,
                budget_limit_nano_usd,
            ),
        )
        run_id = _last_row_id(cursor)
        _insert_transition(
            connection, run_id=run_id, state=RunState.RUNNING, reason=None, now=now
        )
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
        _insert_transition(
            connection, run_id=run_id, state=state, reason=reason, now=now
        )
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
            "UPDATE run SET state = 'interrupted', finished_at = ? "
            f"WHERE id IN ({placeholders})",
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
            work_id = _last_row_id(cursor)
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
    run_id: int,
    now: str,
    reason: str,
) -> int:
    """Retire active work whose material input has changed."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ? AND fingerprint = ?
               AND state IN ('pending', 'deferred')
            """,
            (reason, run_id, now, task_type, fingerprint),
        ).rowcount
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return changed


def supersede_work_for_subject(
    connection: sqlite3.Connection,
    *,
    task_type: str,
    subject_kind: str,
    subject_id: int,
    run_id: int,
    now: str,
    reason: str,
) -> int:
    """Retire every active item for one subject, across fingerprints.

    Use when the subject itself is no longer wanted (for example a feed that
    left configuration). A URL move leaves multiple active fingerprints for the
    same identity; fingerprint-only supersession would miss the orphans.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ? AND subject_kind = ? AND subject_id = ?
               AND state IN ('pending', 'deferred')
            """,
            (reason, run_id, now, task_type, subject_kind, subject_id),
        ).rowcount
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return changed


# Shared by `claim_batch` -- the engine's production claim path -- and by
# `next_eligible`, `claim_next`, and `claim_and_start_attempt`, so no claim
# path can silently diverge on what "claimable" means. Binds two
# placeholders, in this order: run_id (for the same-run deferred check), then
# now (for the eligibility check).
_CLAIMABLE_PREDICATE = """(
    state = 'pending'
    OR (state = 'deferred' AND COALESCE(completed_by_run_id, -1) <> ?)
) AND eligible_at <= ?"""


def _work_item(row: sqlite3.Row) -> WorkItem:
    return WorkItem(
        id=int(row["id"]),
        task_type=row["task_type"],
        subject_kind=row["subject_kind"],
        subject_id=row["subject_id"],
        fingerprint=row["fingerprint"],
        required=bool(row["required"]),
        priority=int(row["priority"]),
        state=WorkState(row["state"]),
    )


def next_eligible(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Inspect the next eligible item without mutating it.

    No production caller since the batch-claim reshape (the engine calls
    `claim_batch` directly); retained as a regression harness for
    `_CLAIMABLE_PREDICATE`'s peek semantics. Removal to be considered in pull
    request 2.
    """
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
         WHERE {_CLAIMABLE_PREDICATE}
           {task_filter}
         ORDER BY priority ASC, id ASC
         LIMIT 1
        """,
        parameters,
    ).fetchone()
    return None if row is None else _work_item(row)


def claim_batch(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
    limit: int,
) -> tuple[WorkItem, ...]:
    """Claim up to `limit` eligible items in one transaction.

    Ordering matches `next_eligible` exactly: `priority ASC, id ASC`.
    `eligible_at` is not a sort key; it is enforced by `_CLAIMABLE_PREDICATE`,
    the same predicate `next_eligible` and `claim_and_start_attempt` use, so
    the claim paths cannot diverge on what "claimable" means. The select and
    the update run inside one transaction, so a crash between them leaves
    every item in the batch `pending` (or `deferred`), never partially
    `running`.
    """
    if limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}")
    if task_types is not None and not task_types:
        return ()
    task_filter = ""
    parameters: list[object] = [run_id, now]
    if task_types is not None:
        ordered = sorted(task_types)
        task_filter = f" AND task_type IN ({','.join('?' for _ in ordered)})"
        parameters.extend(ordered)
    connection.execute("BEGIN IMMEDIATE")
    try:
        rows = connection.execute(
            f"""
            SELECT * FROM work_item
             WHERE {_CLAIMABLE_PREDICATE}
               {task_filter}
             ORDER BY priority ASC, id ASC
             LIMIT ?
            """,
            (*parameters, limit),
        ).fetchall()
        if not rows:
            connection.rollback()
            return ()
        items = [_work_item(row) for row in rows]
        ids = [item.id for item in items]
        placeholders = ",".join("?" for _ in ids)
        changed = connection.execute(
            f"""
            UPDATE work_item
               SET state = 'running', claimed_by_run_id = ?, updated_at = ?
             WHERE id IN ({placeholders}) AND {_CLAIMABLE_PREDICATE}
            """,
            (run_id, now, *ids, run_id, now),
        ).rowcount
        if changed != len(ids):
            raise RuntimeError(
                f"batch claim for run-{run_id} raced with a concurrent claim"
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return tuple(dataclasses.replace(item, state=WorkState.RUNNING) for item in items)


def claim_next(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Claim deterministic work; external calls use claim_and_start_attempt.

    No production caller since the batch-claim reshape (the engine claims in
    batches of more than one via `claim_batch` directly); retained as a
    regression harness for single-item claim semantics. Removal to be
    considered in pull request 2.
    """
    claimed = claim_batch(
        connection, run_id=run_id, now=now, task_types=task_types, limit=1
    )
    return claimed[0] if claimed else None


def complete_work(
    connection: sqlite3.Connection,
    *,
    work_item_id: int,
    run_id: int,
    state: WorkState,
    reason: str | None,
    now: str,
    eligible_at: str | None = None,
    domain_writes: Callable[[sqlite3.Connection], None] | None = None,
) -> None:
    """Record one item's outcome; failures are isolated.

    Also the re-arm path: passing `state='pending'` with a later `eligible_at`
    returns a claimed item to the queue for a retry inside the same run.

    `domain_writes` runs inside this transaction, after the state change and
    before the commit, so a handler's domain rows and the settlement that
    justifies them commit or roll back together. A handler that raises there
    leaves the item `running` and claimed as far as *this* function is
    concerned -- the rollback is total, so there is never a settled item whose
    domain writes vanished by accident. The engine then deliberately settles
    that item a second time as `failed_permanent` with no domain writes, so
    the durable end state is a settled item that correctly has none; see
    `RunEngine._settle`. Only if the engine did not do that would the next
    run's sweep be what recovers the item.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = ?,
                   reason = ?,
                   completed_by_run_id = ?,
                   claimed_by_run_id = NULL,
                   eligible_at = COALESCE(?, eligible_at),
                   updated_at = ?
             WHERE id = ? AND state = 'running'
            """,
            (str(state), reason, run_id, eligible_at, now, work_item_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(
                f"work item {work_item_id} is not completable by run-{run_id}"
            )
        if domain_writes is not None:
            domain_writes(connection)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def defer_unclaimed(
    connection: sqlite3.Connection,
    *,
    work_item_id: int,
    run_id: int,
    reason: str,
    now: str,
) -> None:
    """Defer work this run peeked at but never claimed.

    No production caller since the batch-claim reshape: every item the engine
    sees has already been claimed by `claim_batch` before any check runs
    (including the pause check in `_prepare`), so `complete_work` -- which
    requires state `running` as proof of that claim -- is the engine's only
    settle path now, via `_settle`. There is no longer an engine path that
    refuses an item *before* a claim exists for this function to serve.
    Retained as a regression harness for the peek-then-defer-without-claiming
    shape `next_eligible` also exercises. Removal to be considered in pull
    request 2.

    `completed_by_run_id` is stamped for exactly that reason: it is what
    `_CLAIMABLE_PREDICATE` reads to keep a same-run deferral out of the next
    peek.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'deferred',
                   reason = ?,
                   completed_by_run_id = ?,
                   updated_at = ?
             WHERE id = ?
               AND state IN ('pending', 'deferred')
               AND claimed_by_run_id IS NULL
            """,
            (reason, run_id, now, work_item_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(
                f"work item {work_item_id} is not unclaimed work that "
                f"run-{run_id} may defer"
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def pending_required(connection: sqlite3.Connection, *, now: str | None = None) -> int:
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
            "SELECT COUNT(*) AS n FROM work_item "
            "WHERE required = 1 AND state = 'deferred'"
        ).fetchone()["n"]
    )


def operational_failures_for_run(connection: sqlite3.Connection, *, run_id: int) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE run_id = ? AND outcome = 'failed'",
            (run_id,),
        ).fetchone()["n"]
    )


def run_counters(
    connection: sqlite3.Connection, *, run_id: int, now: str
) -> RunCounters:
    """Summarize one run's work for the digest.

    Deliberately mixes scopes: `required_succeeded`, `required_failed_permanent`,
    `optional_succeeded`, and `optional_skipped` are attributed to this run
    (`completed_by_run_id = run_id`), while `required_pending` and
    `required_deferred` are whole-queue counts that are not run-attributed,
    since pending and eligible-deferred work is not "owned" by any one run.
    """
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


def deferred_reasons(connection: sqlite3.Connection, *, now: str) -> Mapping[str, int]:
    """Count all outstanding due-and-deferred required work, grouped by reason.

    Matches `run_counters`'s `required_deferred` exactly: whole-queue and
    `eligible_at <= now`, not `completed_by_run_id`-scoped. `required_deferred`
    is a deliberately whole-queue count -- it includes carryover a previous
    run deferred and this run never re-touched -- so the reason breakdown must
    describe that same population, or the two numbers disagree the moment a
    later run only reclaims and re-defers part of an earlier run's backlog.
    The reason text a prior run recorded persists on those older rows, so a
    whole-queue breakdown can still explain them.
    """
    return {
        str(row["reason"]): int(row["n"])
        for row in connection.execute(
            """
            SELECT reason, COUNT(*) AS n
              FROM work_item
             WHERE required = 1 AND state = 'deferred' AND eligible_at <= ?
               AND reason IS NOT NULL
             GROUP BY reason
            """,
            (now,),
        )
    }


def next_attempt_ordinal(connection: sqlite3.Connection, *, work_item_id: int) -> int:
    """Advisory next ordinal for a work item.

    Computed outside the transaction that will consume it, so it is a hint,
    not a reservation: the `UNIQUE (work_item_id, ordinal)` index is the real
    arbiter and turns a concurrent race into a rolled-back IntegrityError
    rather than a silently overwritten attempt.
    """
    row = connection.execute(
        "SELECT COALESCE(MAX(ordinal), 0) + 1 AS ordinal FROM attempt "
        "WHERE work_item_id = ?",
        (work_item_id,),
    ).fetchone()
    return int(row["ordinal"])


def _insert_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, destination_host, reserved_nano_usd
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            work_item_id,
            provider,
            operation,
            ordinal,
            now,
            request_fingerprint,
            destination_host,
            reserved_nano_usd,
        ),
    )
    return _last_row_id(cursor)


def claim_and_start_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    """Atomically claim work, reserve cost, and persist the first attempt.

    No production caller since the batch-claim reshape (the engine claims via
    `claim_batch`, then reserves and inserts the attempt as separate steps in
    `prepare`); retained as a regression harness for the combined claim/
    reserve/insert transaction. Removal to be considered in pull request 2.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            f"""
            UPDATE work_item
               SET state = 'running', claimed_by_run_id = ?, updated_at = ?
             WHERE id = ? AND {_CLAIMABLE_PREDICATE}
            """,
            (run_id, now, work_item_id, run_id, now),
        ).rowcount
        if changed != 1:
            raise RuntimeError(
                f"work item {work_item_id} is not claimable by run-{run_id}"
            )
        reserve_in_transaction(
            connection,
            run_id=run_id,
            nano_usd=reserved_nano_usd,
        )
        attempt_id = _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            provider=provider,
            operation=operation,
            ordinal=ordinal,
            request_fingerprint=request_fingerprint,
            destination_host=destination_host,
            reserved_nano_usd=reserved_nano_usd,
            now=now,
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return attempt_id


def start_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    """Atomically reserve and persist a retry for already-running work."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        owner = connection.execute(
            "SELECT claimed_by_run_id FROM work_item "
            "WHERE id = ? AND state = 'running'",
            (work_item_id,),
        ).fetchone()
        if owner is None or owner["claimed_by_run_id"] != run_id:
            raise RuntimeError(
                f"work item {work_item_id} is not a running attempt "
                f"claimed by run-{run_id}"
            )
        reserve_in_transaction(
            connection,
            run_id=run_id,
            nano_usd=reserved_nano_usd,
        )
        attempt_id = _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            provider=provider,
            operation=operation,
            ordinal=ordinal,
            request_fingerprint=request_fingerprint,
            destination_host=destination_host,
            reserved_nano_usd=reserved_nano_usd,
            now=now,
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return attempt_id


def finish_attempt(
    connection: sqlite3.Connection,
    *,
    attempt_id: int,
    record: AttemptRecord,
    response_bytes: int | None,
    provider_request_id: str | None,
    detail_json: str | None,
    now: str,
    actual_nano_usd: int | None = None,
) -> None:
    """Close one immutable attempt. Retries insert new rows; nothing is rewritten."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        attempt = connection.execute(
            "SELECT run_id, reserved_nano_usd, ordinal FROM attempt "
            "WHERE id = ? AND outcome IS NULL",
            (attempt_id,),
        ).fetchone()
        if attempt is None:
            raise RuntimeError(
                f"attempt {attempt_id} is already finished or does not exist"
            )
        if int(attempt["ordinal"]) != record.ordinal:
            raise RuntimeError(
                f"attempt {attempt_id} has ordinal {attempt['ordinal']}, "
                f"but record is for ordinal {record.ordinal}"
            )
        reconcile_in_transaction(
            connection,
            run_id=int(attempt["run_id"]),
            reserved_nano_usd=int(attempt["reserved_nano_usd"]),
            actual_nano_usd=actual_nano_usd,
        )
        changed = connection.execute(
            """
            UPDATE attempt
               SET outcome = ?, failure_category = ?, provider_status = ?,
                   retry_after_ms = ?, latency_ms = ?, response_bytes = ?,
                   provider_request_id = ?, detail_json = ?, actual_nano_usd = ?,
                   finished_at = ?
             WHERE id = ? AND outcome IS NULL
            """,
            (
                record.outcome,
                None
                if record.failure_category is None
                else str(record.failure_category),
                record.status_code,
                record.retry_after_ms,
                record.latency_ms,
                response_bytes,
                provider_request_id,
                detail_json,
                actual_nano_usd,
                now,
                attempt_id,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"attempt {attempt_id} could not be finished")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def attempts_for_run(
    connection: sqlite3.Connection, *, run_id: int
) -> tuple[sqlite3.Row, ...]:
    return tuple(
        connection.execute(
            "SELECT * FROM attempt WHERE run_id = ? ORDER BY id", (run_id,)
        )
    )
