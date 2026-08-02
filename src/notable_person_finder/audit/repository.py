"""Read-only SQL for `notable audit run`.

Every query here degrades rather than raises against a database that
predates the table it needs (K3): the caller learns which sections were
unavailable instead of getting a traceback.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping

from notable_person_finder.audit.models import (
    ArticleTargetLine,
    AssessmentLine,
    AssessmentSignalLine,
    AttemptAudit,
    AttemptLine,
    BraveSearchObservationLine,
    BudgetSummary,
    ConfigurationProvenance,
    CoverageEvidence,
    CoveragePlanLine,
    CoverageQueryFormLine,
    DigestEntryLine,
    FailureGroup,
    LeadLine,
    MentionLine,
    PersonAudit,
    PersonIdentity,
    QueueEvidence,
    QueueRowLine,
    QueueTransitionLine,
    RelationLine,
    ReportingResult,
    ResolutionLine,
    RunAudit,
    RunHeader,
    ScreeningLine,
    SourcedNameLine,
    Transition,
    WikipediaEvidence,
    WikipediaIdentityObservationLine,
    WikipediaPageFactsBatchLine,
    WikipediaPlanLine,
    WikipediaQueryFormLine,
    WikipediaSearchObservationLine,
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


# --- notable audit person -------------------------------------------------


def _load_person_identity(
    connection: sqlite3.Connection, person_id: int
) -> PersonIdentity | None:
    # `current_wikipedia_identity_observation_id` and `current_lead_assessment_id`
    # are added to `person` by ALTER TABLE in migrations 0006 and 0008
    # respectively, in the same transaction that creates
    # `wikipedia_identity_observation` and `lead_assessment`. Table presence
    # is therefore a safe proxy for column presence, consistent with the K3
    # guard style used throughout this module.
    has_wikipedia = _table_present(connection, "wikipedia_identity_observation")
    has_leads = _table_present(connection, "lead_assessment")
    columns = [
        "id",
        "display_name",
        "identity_fingerprint",
        "created_at",
        "created_by_run_id",
        "merged_into_person_id",
    ]
    if has_wikipedia:
        columns.append("current_wikipedia_identity_observation_id")
    if has_leads:
        columns.append("current_lead_assessment_id")
    row = connection.execute(
        f"SELECT {', '.join(columns)} FROM person WHERE id = ?",  # noqa: S608
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    return PersonIdentity(
        id=int(row["id"]),
        display_name=row["display_name"],
        identity_fingerprint=row["identity_fingerprint"],
        created_at=row["created_at"],
        created_by_run_id=int(row["created_by_run_id"]),
        merged_into_person_id=(
            None
            if row["merged_into_person_id"] is None
            else int(row["merged_into_person_id"])
        ),
        current_wikipedia_identity_observation_id=(
            int(row["current_wikipedia_identity_observation_id"])
            if has_wikipedia
            and row["current_wikipedia_identity_observation_id"] is not None
            else None
        ),
        current_lead_assessment_id=(
            int(row["current_lead_assessment_id"])
            if has_leads and row["current_lead_assessment_id"] is not None
            else None
        ),
    )


def _merged_by_run_id(
    connection: sqlite3.Connection, *, merged_id: int, survivor_id: int
) -> int | None:
    if not _table_present(connection, "person_relation"):
        return None
    row = connection.execute(
        """
        SELECT created_by_run_id FROM person_relation
        WHERE kind = 'merge' AND status = 'active'
          AND (
              (person_id_a = ? AND person_id_b = ?)
              OR (person_id_a = ? AND person_id_b = ?)
          )
        ORDER BY id DESC LIMIT 1
        """,
        (merged_id, survivor_id, survivor_id, merged_id),
    ).fetchone()
    return None if row is None else int(row["created_by_run_id"])


def _load_sourced_names(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[SourcedNameLine, ...]:
    if not _table_present(connection, "sourced_name"):
        unavailable.append("sourced names")
        return ()
    rows = connection.execute(
        """
        SELECT id, exact_name, search_name, match_key, kind, origin_kind,
               origin_mention_id, first_observed_at, last_observed_at
        FROM sourced_name WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    return tuple(
        SourcedNameLine(
            id=int(row["id"]),
            exact_name=row["exact_name"],
            search_name=row["search_name"],
            match_key=row["match_key"],
            kind=row["kind"],
            origin_kind=row["origin_kind"],
            origin_mention_id=(
                None
                if row["origin_mention_id"] is None
                else int(row["origin_mention_id"])
            ),
            first_observed_at=row["first_observed_at"],
            last_observed_at=row["last_observed_at"],
        )
        for row in rows
    )


def _load_relations(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[RelationLine, ...]:
    if not _table_present(connection, "person_relation"):
        unavailable.append("relations")
        return ()
    rows = connection.execute(
        """
        SELECT id, kind, person_id_a, person_id_b, status, created_at,
               created_by_run_id, created_by_observation_id, closed_at,
               closed_by_observation_id
        FROM person_relation
        WHERE person_id_a = ? OR person_id_b = ?
        ORDER BY id
        """,
        (person_id, person_id),
    ).fetchall()
    lines: list[RelationLine] = []
    for row in rows:
        other = (
            int(row["person_id_b"])
            if int(row["person_id_a"]) == person_id
            else int(row["person_id_a"])
        )
        lines.append(
            RelationLine(
                id=int(row["id"]),
                other_person_id=other,
                kind=row["kind"],
                status=row["status"],
                created_at=row["created_at"],
                created_by_run_id=int(row["created_by_run_id"]),
                created_by_observation_id=(
                    None
                    if row["created_by_observation_id"] is None
                    else int(row["created_by_observation_id"])
                ),
                closed_at=row["closed_at"],
                closed_by_observation_id=(
                    None
                    if row["closed_by_observation_id"] is None
                    else int(row["closed_by_observation_id"])
                ),
            )
        )
    return tuple(lines)


def _load_mentions(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[MentionLine, ...]:
    # `person_mention.person_id` and `.current_entity_resolution_observation_id`
    # are added by ALTER TABLE in migration 0005 -- not present on the
    # 0004-only CREATE TABLE. `entity_resolution_observation` is created in
    # that same 0005 transaction, so its presence is a safe proxy for both
    # ALTER'd columns existing (K3).
    if not (
        _table_present(connection, "person_mention")
        and _table_present(connection, "triage_observation")
        and _table_present(connection, "entity_resolution_observation")
    ):
        unavailable.append("mentions")
        return ()
    # K11: joined directly on `person_mention.person_id` -- the durable,
    # current association -- never derived from
    # `entity_resolution_observation.selected_person_id`, which only records
    # a point-in-time historical decision that a later observation may
    # supersede.
    rows = connection.execute(
        """
        SELECT pm.id, pm.triage_observation_id, ti.source_item_id, pm.ordinal,
               pm.exact_name, pm.search_name, pm.outcome, pm.rationale,
               pm.current_entity_resolution_observation_id,
               ero.semantic_outcome AS current_semantic_outcome
        FROM person_mention pm
        JOIN triage_observation ti ON ti.id = pm.triage_observation_id
        LEFT JOIN entity_resolution_observation ero
            ON ero.id = pm.current_entity_resolution_observation_id
        WHERE pm.person_id = ?
        ORDER BY pm.id
        """,
        (person_id,),
    ).fetchall()
    return tuple(
        MentionLine(
            id=int(row["id"]),
            triage_observation_id=int(row["triage_observation_id"]),
            source_item_id=int(row["source_item_id"]),
            ordinal=int(row["ordinal"]),
            exact_name=row["exact_name"],
            search_name=row["search_name"],
            outcome=row["outcome"],
            rationale=row["rationale"],
            current_entity_resolution_observation_id=(
                None
                if row["current_entity_resolution_observation_id"] is None
                else int(row["current_entity_resolution_observation_id"])
            ),
            current_semantic_outcome=row["current_semantic_outcome"],
        )
        for row in rows
    )


def _load_resolutions(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[ResolutionLine, ...]:
    if not _table_present(connection, "entity_resolution_observation"):
        unavailable.append("entity resolution")
        return ()
    # Every observation that bears on this person: first-pass/reconsider
    # observations on a mention or relation currently associated with this
    # person, plus any observation that selected or created this person
    # (its subject mention or relation may since have moved on).
    rows = connection.execute(
        """
        SELECT DISTINCT ero.id, ero.person_mention_id, ero.person_relation_id,
               ero.disposition, ero.semantic_outcome,
               ero.candidate_person_ids_json, ero.selected_person_id,
               ero.created_person_id, ero.prompt_hash, ero.schema_version,
               ero.task_fingerprint, ero.rationale
        FROM entity_resolution_observation ero
        LEFT JOIN person_mention pm ON pm.id = ero.person_mention_id
        LEFT JOIN person_relation pr ON pr.id = ero.person_relation_id
        WHERE pm.person_id = ?
           OR ero.selected_person_id = ?
           OR ero.created_person_id = ?
           OR pr.person_id_a = ?
           OR pr.person_id_b = ?
        ORDER BY ero.id
        """,
        (person_id, person_id, person_id, person_id, person_id),
    ).fetchall()
    return tuple(
        ResolutionLine(
            id=int(row["id"]),
            person_mention_id=(
                None
                if row["person_mention_id"] is None
                else int(row["person_mention_id"])
            ),
            person_relation_id=(
                None
                if row["person_relation_id"] is None
                else int(row["person_relation_id"])
            ),
            disposition=row["disposition"],
            semantic_outcome=row["semantic_outcome"],
            candidate_person_ids_json=row["candidate_person_ids_json"],
            selected_person_id=(
                None
                if row["selected_person_id"] is None
                else int(row["selected_person_id"])
            ),
            created_person_id=(
                None
                if row["created_person_id"] is None
                else int(row["created_person_id"])
            ),
            prompt_hash=row["prompt_hash"],
            schema_version=(
                None if row["schema_version"] is None else int(row["schema_version"])
            ),
            task_fingerprint=row["task_fingerprint"],
            rationale=row["rationale"],
        )
        for row in rows
    )


def _load_wikipedia(
    connection: sqlite3.Connection,
    person_id: int,
    current_observation_id: int | None,
    unavailable: list[str],
) -> WikipediaEvidence:
    if not _table_present(connection, "wikipedia_identity_plan"):
        unavailable.append("wikipedia")
        return WikipediaEvidence(
            plans=(),
            query_forms=(),
            search_observations=(),
            page_facts_batches=(),
            identity_observations=(),
        )
    plan_rows = connection.execute(
        """
        SELECT id, status, created_at, completed_at, failure_category
        FROM wikipedia_identity_plan WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    plans = tuple(
        WikipediaPlanLine(
            id=int(row["id"]),
            status=row["status"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
            failure_category=row["failure_category"],
        )
        for row in plan_rows
    )
    plan_ids = tuple(plan.id for plan in plans)

    query_forms: tuple[WikipediaQueryFormLine, ...] = ()
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        query_form_rows = connection.execute(
            f"""
            SELECT id, plan_id, ordinal, variant_kind, query_text, status, hit_count
            FROM wikipedia_query_form WHERE plan_id IN ({placeholders})
            ORDER BY plan_id, ordinal
            """,  # noqa: S608
            plan_ids,
        ).fetchall()
        query_forms = tuple(
            WikipediaQueryFormLine(
                id=int(row["id"]),
                plan_id=int(row["plan_id"]),
                ordinal=int(row["ordinal"]),
                variant_kind=row["variant_kind"],
                query_text=row["query_text"],
                status=row["status"],
                hit_count=(None if row["hit_count"] is None else int(row["hit_count"])),
            )
            for row in query_form_rows
        )
    query_form_ids = tuple(form.id for form in query_forms)

    search_observations: tuple[WikipediaSearchObservationLine, ...] = ()
    if query_form_ids:
        placeholders = ",".join("?" for _ in query_form_ids)
        search_rows = connection.execute(
            f"""
            SELECT id, query_form_id, query_text, hit_count, truncated,
                   response_complete
            FROM mediawiki_search_observation WHERE query_form_id IN ({placeholders})
            ORDER BY id
            """,  # noqa: S608
            query_form_ids,
        ).fetchall()
        search_observations = tuple(
            WikipediaSearchObservationLine(
                id=int(row["id"]),
                query_form_id=int(row["query_form_id"]),
                query_text=row["query_text"],
                hit_count=int(row["hit_count"]),
                truncated=bool(row["truncated"]),
                response_complete=bool(row["response_complete"]),
            )
            for row in search_rows
        )

    page_facts_batches: tuple[WikipediaPageFactsBatchLine, ...] = ()
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        batch_rows = connection.execute(
            f"""
            SELECT id, plan_id, ordinal, status, wave
            FROM wikipedia_page_facts_batch WHERE plan_id IN ({placeholders})
            ORDER BY plan_id, ordinal
            """,  # noqa: S608
            plan_ids,
        ).fetchall()
        page_facts_batches = tuple(
            WikipediaPageFactsBatchLine(
                id=int(row["id"]),
                plan_id=int(row["plan_id"]),
                ordinal=int(row["ordinal"]),
                status=row["status"],
                wave=int(row["wave"]),
            )
            for row in batch_rows
        )

    obs_rows = connection.execute(
        """
        SELECT id, disposition, semantic_outcome, matched_mediawiki_page_id,
               task_fingerprint, rationale
        FROM wikipedia_identity_observation WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    identity_observations = tuple(
        WikipediaIdentityObservationLine(
            id=int(row["id"]),
            disposition=row["disposition"],
            semantic_outcome=row["semantic_outcome"],
            matched_mediawiki_page_id=(
                None
                if row["matched_mediawiki_page_id"] is None
                else int(row["matched_mediawiki_page_id"])
            ),
            task_fingerprint=row["task_fingerprint"],
            rationale=row["rationale"],
            is_current=(
                current_observation_id is not None
                and int(row["id"]) == current_observation_id
            ),
        )
        for row in obs_rows
    )

    return WikipediaEvidence(
        plans=plans,
        query_forms=query_forms,
        search_observations=search_observations,
        page_facts_batches=page_facts_batches,
        identity_observations=identity_observations,
    )


def _load_coverage(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> CoverageEvidence:
    if not _table_present(connection, "person_coverage_plan"):
        unavailable.append("coverage")
        return CoverageEvidence(
            plans=(),
            query_forms=(),
            brave_observations=(),
            screenings=(),
            article_targets=(),
        )
    plan_rows = connection.execute(
        """
        SELECT id, status, created_at, completed_at, failure_category
        FROM person_coverage_plan WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    plans = tuple(
        CoveragePlanLine(
            id=int(row["id"]),
            status=row["status"],
            created_at=row["created_at"],
            completed_at=row["completed_at"],
            failure_category=row["failure_category"],
        )
        for row in plan_rows
    )
    plan_ids = tuple(plan.id for plan in plans)

    query_forms: tuple[CoverageQueryFormLine, ...] = ()
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        query_form_rows = connection.execute(
            f"""
            SELECT id, plan_id, ordinal, stage, variant_kind, query_text,
                   status, result_count
            FROM coverage_query_form WHERE plan_id IN ({placeholders})
            ORDER BY plan_id, ordinal
            """,  # noqa: S608
            plan_ids,
        ).fetchall()
        query_forms = tuple(
            CoverageQueryFormLine(
                id=int(row["id"]),
                plan_id=int(row["plan_id"]),
                ordinal=int(row["ordinal"]),
                stage=int(row["stage"]),
                variant_kind=row["variant_kind"],
                query_text=row["query_text"],
                status=row["status"],
                result_count=(
                    None if row["result_count"] is None else int(row["result_count"])
                ),
            )
            for row in query_form_rows
        )
    query_form_ids = tuple(form.id for form in query_forms)

    brave_observations: tuple[BraveSearchObservationLine, ...] = ()
    if query_form_ids:
        placeholders = ",".join("?" for _ in query_form_ids)
        brave_rows = connection.execute(
            f"""
            SELECT id, query_form_id, query_text, result_count, truncated
            FROM brave_search_observation WHERE query_form_id IN ({placeholders})
            ORDER BY id
            """,  # noqa: S608
            query_form_ids,
        ).fetchall()
        brave_observations = tuple(
            BraveSearchObservationLine(
                id=int(row["id"]),
                query_form_id=int(row["query_form_id"]),
                query_text=row["query_text"],
                result_count=int(row["result_count"]),
                truncated=bool(row["truncated"]),
            )
            for row in brave_rows
        )

    screenings: tuple[ScreeningLine, ...] = ()
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        screening_rows = connection.execute(
            f"""
            SELECT id, plan_id, url, publisher_key, rule_id, rule_status,
                   source_policy_fingerprint
            FROM source_screening WHERE plan_id IN ({placeholders})
            ORDER BY id
            """,  # noqa: S608
            plan_ids,
        ).fetchall()
        screenings = tuple(
            ScreeningLine(
                id=int(row["id"]),
                plan_id=(None if row["plan_id"] is None else int(row["plan_id"])),
                url=row["url"],
                publisher_key=row["publisher_key"],
                rule_id=row["rule_id"],
                rule_status=row["rule_status"],
                source_policy_fingerprint=row["source_policy_fingerprint"],
            )
            for row in screening_rows
        )

    article_targets: tuple[ArticleTargetLine, ...] = ()
    if plan_ids:
        placeholders = ",".join("?" for _ in plan_ids)
        target_rows = connection.execute(
            f"""
            SELECT id, plan_id, canonical_article_id, request_url,
                   selection_reason, status
            FROM coverage_article_target WHERE plan_id IN ({placeholders})
            ORDER BY id
            """,  # noqa: S608
            plan_ids,
        ).fetchall()
        article_targets = tuple(
            ArticleTargetLine(
                id=int(row["id"]),
                plan_id=int(row["plan_id"]),
                canonical_article_id=int(row["canonical_article_id"]),
                request_url=row["request_url"],
                selection_reason=row["selection_reason"],
                status=row["status"],
            )
            for row in target_rows
        )

    return CoverageEvidence(
        plans=plans,
        query_forms=query_forms,
        brave_observations=brave_observations,
        screenings=screenings,
        article_targets=article_targets,
    )


def _load_assessments(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[AssessmentLine, ...]:
    if not _table_present(connection, "person_article_assessment"):
        unavailable.append("assessments")
        return ()
    rows = connection.execute(
        """
        SELECT id, canonical_article_id, disposition, person_relation,
               coverage_depth, subject_relationship, content_types_json,
               rationale, failure_category
        FROM person_article_assessment WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    has_signals = _table_present(connection, "article_assessment_signal")
    assessments: list[AssessmentLine] = []
    for row in rows:
        signal_rows = ()
        if has_signals:
            signal_rows = connection.execute(
                """
                SELECT id, signal_kind, category, claim,
                       supporting_passage_ids_json, ordinal
                FROM article_assessment_signal
                WHERE assessment_id = ? ORDER BY signal_kind, ordinal
                """,
                (int(row["id"]),),
            ).fetchall()
        signals = tuple(
            AssessmentSignalLine(
                id=int(signal_row["id"]),
                signal_kind=signal_row["signal_kind"],
                category=signal_row["category"],
                claim=signal_row["claim"],
                supporting_passage_ids_json=signal_row["supporting_passage_ids_json"],
                ordinal=int(signal_row["ordinal"]),
            )
            for signal_row in signal_rows
        )
        assessments.append(
            AssessmentLine(
                id=int(row["id"]),
                canonical_article_id=int(row["canonical_article_id"]),
                disposition=row["disposition"],
                person_relation=row["person_relation"],
                coverage_depth=row["coverage_depth"],
                subject_relationship=row["subject_relationship"],
                content_types_json=row["content_types_json"],
                rationale=row["rationale"],
                failure_category=row["failure_category"],
                signals=signals,
            )
        )
    return tuple(assessments)


def _load_leads(
    connection: sqlite3.Connection,
    person_id: int,
    current_lead_id: int | None,
    unavailable: list[str],
) -> tuple[LeadLine, ...]:
    if not _table_present(connection, "lead_assessment"):
        unavailable.append("lead history")
        return ()
    rows = connection.execute(
        """
        SELECT id, outcome, qualifying_domain_count, incompleteness_reason,
               decided_at
        FROM lead_assessment WHERE person_id = ? ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    return tuple(
        LeadLine(
            id=int(row["id"]),
            outcome=row["outcome"],
            qualifying_domain_count=int(row["qualifying_domain_count"]),
            incompleteness_reason=row["incompleteness_reason"],
            decided_at=row["decided_at"],
            is_current=(
                current_lead_id is not None and int(row["id"]) == current_lead_id
            ),
        )
        for row in rows
    )


def _load_queue(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> QueueEvidence:
    if not _table_present(connection, "digest_queue"):
        unavailable.append("queue history")
        return QueueEvidence(row=None, transitions=())
    row = connection.execute(
        """
        SELECT status, tier, eligibility_reason, lead_assessment_id,
               first_pending_at, last_material_change_at, removed_reason
        FROM digest_queue WHERE person_id = ?
        """,
        (person_id,),
    ).fetchone()
    queue_row = (
        None
        if row is None
        else QueueRowLine(
            status=row["status"],
            tier=row["tier"],
            eligibility_reason=row["eligibility_reason"],
            lead_assessment_id=int(row["lead_assessment_id"]),
            first_pending_at=row["first_pending_at"],
            last_material_change_at=row["last_material_change_at"],
            removed_reason=row["removed_reason"],
        )
    )
    transition_rows = ()
    if _table_present(connection, "queue_transition"):
        transition_rows = connection.execute(
            """
            SELECT id, run_id, from_status, to_status, tier, reason, occurred_at
            FROM queue_transition WHERE person_id = ? ORDER BY id
            """,
            (person_id,),
        ).fetchall()
    transitions = tuple(
        QueueTransitionLine(
            id=int(row["id"]),
            run_id=int(row["run_id"]),
            from_status=row["from_status"],
            to_status=row["to_status"],
            tier=row["tier"],
            reason=row["reason"],
            occurred_at=row["occurred_at"],
        )
        for row in transition_rows
    )
    return QueueEvidence(row=queue_row, transitions=transitions)


def _load_digest_entries(
    connection: sqlite3.Connection, person_id: int, unavailable: list[str]
) -> tuple[DigestEntryLine, ...]:
    if not (
        _table_present(connection, "digest_entry")
        and _table_present(connection, "digest")
    ):
        unavailable.append("digest history")
        return ()
    rows = connection.execute(
        """
        SELECT de.digest_id, d.run_id, de.ordinal, de.lead_assessment_id,
               d.file_path
        FROM digest_entry de
        JOIN digest d ON d.id = de.digest_id
        WHERE de.person_id = ?
        ORDER BY de.id
        """,
        (person_id,),
    ).fetchall()
    return tuple(
        DigestEntryLine(
            digest_id=int(row["digest_id"]),
            run_id=int(row["run_id"]),
            ordinal=int(row["ordinal"]),
            lead_assessment_id=int(row["lead_assessment_id"]),
            file_path=row["file_path"],
        )
        for row in rows
    )


def load_person_audit(
    connection: sqlite3.Connection, *, person_id: int
) -> PersonAudit | None:
    """The full lifecycle-ordered evidence chain for one person (K11).

    Every section is guarded independently on table presence (K3), because a
    database migrated only partway through the rewrite programme may have
    `person` and `entity_resolution_observation` but lack
    `person_coverage_plan` or `lead_assessment` entirely.
    """
    if not _table_present(connection, "person"):
        return None
    identity = _load_person_identity(connection, person_id)
    if identity is None:
        return None

    unavailable: list[str] = []

    merged_into: PersonIdentity | None = None
    merged_by_run_id: int | None = None
    if identity.merged_into_person_id is not None:
        merged_into = _load_person_identity(connection, identity.merged_into_person_id)
        merged_by_run_id = _merged_by_run_id(
            connection,
            merged_id=identity.id,
            survivor_id=identity.merged_into_person_id,
        )

    sourced_names = _load_sourced_names(connection, person_id, unavailable)
    relations = _load_relations(connection, person_id, unavailable)
    mentions = _load_mentions(connection, person_id, unavailable)
    resolutions = _load_resolutions(connection, person_id, unavailable)
    wikipedia = _load_wikipedia(
        connection,
        person_id,
        identity.current_wikipedia_identity_observation_id,
        unavailable,
    )
    coverage = _load_coverage(connection, person_id, unavailable)
    assessments = _load_assessments(connection, person_id, unavailable)
    leads = _load_leads(
        connection, person_id, identity.current_lead_assessment_id, unavailable
    )
    queue = _load_queue(connection, person_id, unavailable)
    digest_entries = _load_digest_entries(connection, person_id, unavailable)

    return PersonAudit(
        identity=identity,
        merged_into=merged_into,
        merged_by_run_id=merged_by_run_id,
        sourced_names=sourced_names,
        relations=relations,
        mentions=mentions,
        resolutions=resolutions,
        wikipedia=wikipedia,
        coverage=coverage,
        assessments=assessments,
        leads=leads,
        queue=queue,
        digest_entries=digest_entries,
        unavailable_sections=tuple(unavailable),
    )
