"""Read-only SQL for `notable audit run`.

Every query here degrades rather than raises against a database that
predates the table it needs (K3): the caller learns which sections were
unavailable instead of getting a traceback.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping

from notable_person_finder.audit.models import (
    AttemptAudit,
    AttemptLine,
    BudgetSummary,
    ConfigurationProvenance,
    FailureGroup,
    ReportingResult,
    RunAudit,
    RunHeader,
    Transition,
    WorkItemLine,
    WorkOutcomes,
)
from notable_person_finder.audit.registry import ResultBinding, binding_for

# K9: `feed_fetch` records no `attempt_id`, and `attempt` records no URL, so
# a `fetch_feed` work item that retried within one run cannot be attributed
# to a single attempt. Every `feed_fetch` row for the owning feed and run is
# shown instead of guessing by ordinal position.
_FETCH_FEED_CAVEAT = (
    "feed_fetch records no attempt_id; all fetch rows for this feed and run "
    "are shown, and cannot be attributed to a single attempt"
)


class AttemptScopeError(Exception):
    """The attempt does not exist, or belongs to a different run (K8)."""


def _table_present(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


def _load_configuration(
    connection: sqlite3.Connection,
    snapshot_id: int,
    unavailable: list[str],
) -> ConfigurationProvenance | None:
    if not _table_present(connection, "configuration_snapshot"):
        unavailable.append("configuration")
        return None
    row = connection.execute(
        "SELECT id, fingerprint, canonical_json, created_at "
        "FROM configuration_snapshot WHERE id = ?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return None
    return ConfigurationProvenance(
        snapshot_id=int(row["id"]),
        fingerprint=row["fingerprint"],
        created_at=row["created_at"],
        canonical_json=row["canonical_json"],
    )


def _load_transitions(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> tuple[Transition, ...]:
    if not _table_present(connection, "run_transition"):
        unavailable.append("transitions")
        return ()
    rows = connection.execute(
        "SELECT state, reason, occurred_at FROM run_transition "
        "WHERE run_id = ? ORDER BY id",
        (run_id,),
    ).fetchall()
    return tuple(
        Transition(
            state=row["state"], reason=row["reason"], occurred_at=row["occurred_at"]
        )
        for row in rows
    )


def _work_item_lines(
    connection: sqlite3.Connection, run_id: int, *, state: str
) -> tuple[WorkItemLine, ...]:
    rows = connection.execute(
        """
        SELECT id, task_type, subject_kind, subject_id, fingerprint, state, reason
        FROM work_item
        WHERE (
            claimed_by_run_id = ? OR completed_by_run_id = ? OR created_by_run_id = ?
        )
          AND state = ?
        ORDER BY id
        """,
        (run_id, run_id, run_id, state),
    ).fetchall()
    return tuple(
        WorkItemLine(
            id=int(row["id"]),
            task_type=row["task_type"],
            subject_kind=row["subject_kind"],
            subject_id=(None if row["subject_id"] is None else int(row["subject_id"])),
            fingerprint=row["fingerprint"],
            state=row["state"],
            reason=row["reason"],
        )
        for row in rows
    )


def _load_work_outcomes(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> WorkOutcomes:
    if not _table_present(connection, "work_item"):
        unavailable.append("work outcomes")
        return WorkOutcomes(counts=(), failed_permanent=(), deferred=())
    count_rows = connection.execute(
        """
        SELECT task_type, state, COUNT(*) AS n
        FROM work_item
        WHERE claimed_by_run_id = ? OR completed_by_run_id = ? OR created_by_run_id = ?
        GROUP BY task_type, state
        ORDER BY task_type, state
        """,
        (run_id, run_id, run_id),
    ).fetchall()
    counts = tuple(
        (row["task_type"], row["state"], int(row["n"])) for row in count_rows
    )
    return WorkOutcomes(
        counts=counts,
        failed_permanent=_work_item_lines(connection, run_id, state="failed_permanent"),
        deferred=_work_item_lines(connection, run_id, state="deferred"),
    )


def _attempt_line_from_row(row: sqlite3.Row) -> AttemptLine:
    return AttemptLine(
        id=int(row["id"]),
        work_item_id=int(row["work_item_id"]),
        provider=row["provider"],
        operation=row["operation"],
        ordinal=int(row["ordinal"]),
        outcome=row["outcome"],
        failure_category=row["failure_category"],
        provider_status=(
            None if row["provider_status"] is None else int(row["provider_status"])
        ),
        latency_ms=(None if row["latency_ms"] is None else int(row["latency_ms"])),
        response_bytes=(
            None if row["response_bytes"] is None else int(row["response_bytes"])
        ),
        destination_host=row["destination_host"],
        reserved_nano_usd=int(row["reserved_nano_usd"]),
        actual_nano_usd=(
            None if row["actual_nano_usd"] is None else int(row["actual_nano_usd"])
        ),
    )


_ATTEMPT_COLUMNS = (
    "id, work_item_id, provider, operation, ordinal, outcome, "
    "failure_category, provider_status, latency_ms, response_bytes, "
    "destination_host, reserved_nano_usd, actual_nano_usd"
)


def _load_attempts(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> tuple[AttemptLine, ...]:
    if not _table_present(connection, "attempt"):
        unavailable.append("attempts")
        return ()
    rows = connection.execute(
        f"""
        SELECT {_ATTEMPT_COLUMNS}
        FROM attempt
        WHERE run_id = ?
        ORDER BY id
        """,
        (run_id,),
    ).fetchall()
    return tuple(_attempt_line_from_row(row) for row in rows)


def _load_failures(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> tuple[FailureGroup, ...]:
    if not _table_present(connection, "attempt"):
        unavailable.append("failures")
        return ()
    # `failure_category` is NULL exactly on `interrupted` attempts (the
    # schema forbids it being NULL on `failed` ones and forbids it being
    # non-NULL on any other outcome), so grouping on
    # `COALESCE(failure_category, 'interrupted')` buckets every interrupted
    # attempt together without colliding with a real failure category.
    rows = connection.execute(
        """
        SELECT COALESCE(failure_category, 'interrupted') AS category,
               COUNT(*) AS n, provider, operation,
               GROUP_CONCAT(DISTINCT outcome) AS outcomes
        FROM attempt
        WHERE run_id = ? AND outcome IN ('failed', 'interrupted')
        GROUP BY category
        ORDER BY category
        """,
        (run_id,),
    ).fetchall()
    return tuple(
        FailureGroup(
            failure_category=row["category"],
            count=int(row["n"]),
            example_provider=row["provider"],
            example_operation=row["operation"],
            outcomes=tuple((row["outcomes"] or "").split(",")),
        )
        for row in rows
    )


def _load_budget(
    connection: sqlite3.Connection,
    run_id: int,
    run_row: sqlite3.Row,
    unavailable: list[str],
) -> BudgetSummary:
    if not _table_present(connection, "attempt"):
        unavailable.append("budget")
        attempt_sum = 0
    else:
        attempt_sum = connection.execute(
            "SELECT COALESCE(SUM(actual_nano_usd), 0) FROM attempt WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    limit_value = run_row["budget_limit_nano_usd"]
    return BudgetSummary(
        limit_nano_usd=(None if limit_value is None else int(limit_value)),
        reserved_nano_usd=int(run_row["budget_reserved_nano_usd"]),
        actual_nano_usd=int(run_row["budget_actual_nano_usd"]),
        attempt_actual_sum_nano_usd=int(attempt_sum),
    )


def _load_reporting(
    connection: sqlite3.Connection, run_id: int, run_row: sqlite3.Row
) -> ReportingResult | None:
    if _table_present(connection, "digest"):
        row = connection.execute(
            "SELECT id, file_path, content_hash, run_state FROM digest "
            "WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is not None:
            entry_count: int | None = None
            if _table_present(connection, "digest_entry"):
                entry_count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM digest_entry WHERE digest_id = ?",
                        (row["id"],),
                    ).fetchone()[0]
                )
            return ReportingResult(
                digest_path=row["file_path"],
                digest_sha256=row["content_hash"],
                run_state=row["run_state"],
                entry_count=entry_count,
            )

    file_path = run_row["digest_path"]
    content_hash = run_row["digest_sha256"]
    if file_path is None and content_hash is None:
        return None
    # Pre-0008 fallback: the `digest` table (and its entry count) do not
    # exist yet, so the best available `run_state` is the run's current
    # state rather than the state recorded at write time.
    return ReportingResult(
        digest_path=file_path,
        digest_sha256=content_hash,
        run_state=run_row["state"],
        entry_count=None,
    )


def load_run_audit(connection: sqlite3.Connection, *, run_id: int) -> RunAudit | None:
    if not _table_present(connection, "run"):
        return None

    run_row = connection.execute(
        """
        SELECT id, state, configuration_snapshot_id, timezone, window_start,
               window_end, started_at, finished_at, budget_limit_nano_usd,
               budget_reserved_nano_usd, budget_actual_nano_usd, digest_path,
               digest_sha256
        FROM run WHERE id = ?
        """,
        (run_id,),
    ).fetchone()
    if run_row is None:
        return None

    unavailable: list[str] = []

    run = RunHeader(
        id=int(run_row["id"]),
        state=run_row["state"],
        started_at=run_row["started_at"],
        finished_at=run_row["finished_at"],
        timezone=run_row["timezone"],
        window_start=run_row["window_start"],
        window_end=run_row["window_end"],
    )

    configuration = _load_configuration(
        connection, int(run_row["configuration_snapshot_id"]), unavailable
    )
    transitions = _load_transitions(connection, run_id, unavailable)
    work_outcomes = _load_work_outcomes(connection, run_id, unavailable)
    attempts = _load_attempts(connection, run_id, unavailable)
    failures = _load_failures(connection, run_id, unavailable)
    budget = _load_budget(connection, run_id, run_row, unavailable)
    reporting = _load_reporting(connection, run_id, run_row)

    return RunAudit(
        run=run,
        configuration=configuration,
        transitions=transitions,
        work_outcomes=work_outcomes,
        attempts=attempts,
        failures=failures,
        budget=budget,
        reporting=reporting,
        unavailable_sections=tuple(unavailable),
    )


def _work_item_line(row: sqlite3.Row) -> WorkItemLine:
    return WorkItemLine(
        id=int(row["id"]),
        task_type=row["task_type"],
        subject_kind=row["subject_kind"],
        subject_id=(None if row["subject_id"] is None else int(row["subject_id"])),
        fingerprint=row["fingerprint"],
        state=row["state"],
        reason=row["reason"],
    )


def _feed_fetch_rows(
    connection: sqlite3.Connection,
    binding: ResultBinding,
    *,
    feed_identity_id: int | None,
    run_id: int,
) -> tuple[Mapping[str, object], ...]:
    # The table name comes off the registry binding rather than a hardcoded
    # literal, so `fetch_feed`'s result table has exactly one source of
    # truth: `audit/registry.py`.
    if not _table_present(connection, binding.result_table):
        return ()
    rows = connection.execute(
        f"SELECT * FROM {binding.result_table}"  # noqa: S608
        " WHERE feed_identity_id = ? AND run_id = ? ORDER BY requested_at",
        (feed_identity_id, run_id),
    ).fetchall()
    return tuple(dict(row) for row in rows)


def _result_rows_for_binding(
    connection: sqlite3.Connection,
    binding: ResultBinding,
    *,
    attempt_id: int,
    run_id: int,
) -> tuple[Mapping[str, object], ...]:
    if not _table_present(connection, binding.result_table):
        return ()
    # Every join column used by a registered external binding is one of
    # these two; a binding needing anything else is out of scope for this
    # generic path (fetch_feed, the one exception, is handled separately
    # because it joins on the work item's subject, not the attempt).
    value_map = {"attempt_id": attempt_id, "run_id": run_id}
    if not all(column in value_map for column in binding.join_columns):
        return ()
    where_clause = " AND ".join(f"{column} = ?" for column in binding.join_columns)
    params = [value_map[column] for column in binding.join_columns]
    rows = connection.execute(
        f"SELECT * FROM {binding.result_table} WHERE {where_clause}",  # noqa: S608
        params,
    ).fetchall()
    return tuple(dict(row) for row in rows)


def load_attempt_audit(
    connection: sqlite3.Connection, *, run_id: int, attempt_id: int
) -> AttemptAudit:
    """Drill down into one attempt: its retry siblings, owning work item, and
    persisted result (K8: raises rather than silently answering with another
    run's data when the attempt belongs elsewhere or does not exist)."""
    if not _table_present(connection, "attempt"):
        raise AttemptScopeError(f"attempt {attempt_id} not found")

    attempt_row = connection.execute(
        f"SELECT {_ATTEMPT_COLUMNS}, run_id FROM attempt WHERE id = ?",
        (attempt_id,),
    ).fetchone()
    if attempt_row is None or int(attempt_row["run_id"]) != run_id:
        raise AttemptScopeError(f"attempt {attempt_id} does not belong to run {run_id}")
    attempt = _attempt_line_from_row(attempt_row)

    if not _table_present(connection, "work_item"):
        raise AttemptScopeError(f"attempt {attempt_id} not found")
    work_item_row = connection.execute(
        "SELECT id, task_type, subject_kind, subject_id, fingerprint, state, reason "
        "FROM work_item WHERE id = ?",
        (attempt.work_item_id,),
    ).fetchone()
    if work_item_row is None:
        raise AttemptScopeError(f"attempt {attempt_id} not found")
    work_item = _work_item_line(work_item_row)

    retry_rows = connection.execute(
        f"SELECT {_ATTEMPT_COLUMNS} FROM attempt "
        "WHERE work_item_id = ? ORDER BY ordinal",
        (work_item.id,),
    ).fetchall()
    retry_history = tuple(_attempt_line_from_row(row) for row in retry_rows)

    binding = binding_for(work_item.task_type)
    caveat: str | None = None
    result_rows: tuple[Mapping[str, object], ...] = ()
    no_result_row = False

    if binding is None:
        no_result_row = True
    elif not binding.external:
        # K10: an attempt exists for a task type that should never make an
        # external call. `render_attempt_audit` reports this inconsistency
        # straight from `binding.external`; no result is rendered here.
        pass
    elif work_item.task_type == "fetch_feed":
        caveat = _FETCH_FEED_CAVEAT
        result_rows = _feed_fetch_rows(
            connection, binding, feed_identity_id=work_item.subject_id, run_id=run_id
        )
        no_result_row = not result_rows
    else:
        result_rows = _result_rows_for_binding(
            connection, binding, attempt_id=attempt.id, run_id=run_id
        )
        no_result_row = not result_rows

    return AttemptAudit(
        attempt=attempt,
        work_item=work_item,
        retry_history=retry_history,
        result_rows=result_rows,
        binding=binding,
        caveat=caveat,
        no_result_row=no_result_row,
    )
