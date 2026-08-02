"""Read-only SQL for `notable audit run`.

Every query here degrades rather than raises against a database that
predates the table it needs (K3): the caller learns which sections were
unavailable instead of getting a traceback.
"""

from __future__ import annotations

import sqlite3

from notable_person_finder.audit.models import (
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
        "SELECT id, fingerprint, created_at FROM configuration_snapshot WHERE id = ?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return None
    return ConfigurationProvenance(
        snapshot_id=int(row["id"]),
        fingerprint=row["fingerprint"],
        created_at=row["created_at"],
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


def _load_attempts(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> tuple[AttemptLine, ...]:
    if not _table_present(connection, "attempt"):
        unavailable.append("attempts")
        return ()
    rows = connection.execute(
        """
        SELECT id, work_item_id, provider, operation, outcome, failure_category,
               reserved_nano_usd, actual_nano_usd
        FROM attempt
        WHERE run_id = ?
        ORDER BY id
        """,
        (run_id,),
    ).fetchall()
    return tuple(
        AttemptLine(
            id=int(row["id"]),
            work_item_id=int(row["work_item_id"]),
            provider=row["provider"],
            operation=row["operation"],
            outcome=row["outcome"],
            failure_category=row["failure_category"],
            reserved_nano_usd=int(row["reserved_nano_usd"]),
            actual_nano_usd=(
                None if row["actual_nano_usd"] is None else int(row["actual_nano_usd"])
            ),
        )
        for row in rows
    )


def _load_failures(
    connection: sqlite3.Connection, run_id: int, unavailable: list[str]
) -> tuple[FailureGroup, ...]:
    if not _table_present(connection, "attempt"):
        unavailable.append("failures")
        return ()
    rows = connection.execute(
        """
        SELECT failure_category, COUNT(*) AS n, provider, operation
        FROM attempt
        WHERE run_id = ? AND outcome = 'failed'
        GROUP BY failure_category
        ORDER BY failure_category
        """,
        (run_id,),
    ).fetchall()
    return tuple(
        FailureGroup(
            failure_category=row["failure_category"] or "unknown",
            count=int(row["n"]),
            example_provider=row["provider"],
            example_operation=row["operation"],
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
            "SELECT file_path, content_hash FROM digest "
            "WHERE run_id = ? ORDER BY id DESC LIMIT 1",
            (run_id,),
        ).fetchone()
        if row is not None:
            return ReportingResult(
                digest_path=row["file_path"], digest_sha256=row["content_hash"]
            )

    file_path = run_row["digest_path"]
    content_hash = run_row["digest_sha256"]
    if file_path is None and content_hash is None:
        return None
    return ReportingResult(digest_path=file_path, digest_sha256=content_hash)


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
