"""Transaction-neutral persistence for coverage evidence rows.

Writers neither begin, commit, nor roll back: they join the caller's open
transaction so domain rows and the settlement that justifies them commit or
roll back together.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from notable_person_finder.coverage.models import (
    ArticleViewRecord,
    BraveSearchObservationRecord,
    CoverageArticleTargetRecord,
    CoverageDiscoveryArticleRecord,
    CoverageQueryFormRecord,
    PersonArticleAssessmentRecord,
    PersonArticleRecord,
    PersonCoveragePlanRecord,
    SourceScreeningRecord,
)


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT did not produce a row id")
    return row_id


def _require_transaction(connection: sqlite3.Connection, name: str) -> None:
    if not connection.in_transaction:
        raise RuntimeError(f"{name} requires an active transaction")


def _as_bool_int(value: bool) -> int:
    return 1 if value else 0


def _plan_record(row: sqlite3.Row) -> PersonCoveragePlanRecord:
    return PersonCoveragePlanRecord(
        id=int(row["id"]),
        person_id=int(row["person_id"]),
        run_id=int(row["run_id"]),
        material_fingerprint=str(row["material_fingerprint"]),
        status=str(row["status"]),
        refresh_of_plan_id=(
            int(row["refresh_of_plan_id"])
            if row["refresh_of_plan_id"] is not None
            else None
        ),
        source_policy_fingerprint=str(row["source_policy_fingerprint"]),
        truncated_unsafe=bool(row["truncated_unsafe"]),
        partial_retrieval=bool(row["partial_retrieval"]),
        retrieval_target=int(row["retrieval_target"]),
        eligible_selected_count=int(row["eligible_selected_count"]),
        created_at=str(row["created_at"]),
        completed_at=(
            str(row["completed_at"]) if row["completed_at"] is not None else None
        ),
        failure_category=(
            str(row["failure_category"])
            if row["failure_category"] is not None
            else None
        ),
    )


def _query_form_record(row: sqlite3.Row) -> CoverageQueryFormRecord:
    return CoverageQueryFormRecord(
        id=int(row["id"]),
        plan_id=int(row["plan_id"]),
        ordinal=int(row["ordinal"]),
        stage=int(row["stage"]),
        variant_kind=str(row["variant_kind"]),
        query_text=str(row["query_text"]),
        status=str(row["status"]),
        offsets_used=int(row["offsets_used"]),
        result_count=(
            int(row["result_count"]) if row["result_count"] is not None else None
        ),
        truncated=bool(row["truncated"]),
        failure_category=(
            str(row["failure_category"])
            if row["failure_category"] is not None
            else None
        ),
    )


def _screening_record(row: sqlite3.Row) -> SourceScreeningRecord:
    return SourceScreeningRecord(
        id=int(row["id"]),
        canonical_article_id=(
            int(row["canonical_article_id"])
            if row["canonical_article_id"] is not None
            else None
        ),
        url=str(row["url"]),
        publisher_key=(
            str(row["publisher_key"]) if row["publisher_key"] is not None else None
        ),
        rule_id=str(row["rule_id"]),
        rule_status=str(row["rule_status"]),
        source_policy_fingerprint=str(row["source_policy_fingerprint"]),
        decided_at=str(row["decided_at"]),
        plan_id=int(row["plan_id"]) if row["plan_id"] is not None else None,
        source_item_id=(
            int(row["source_item_id"]) if row["source_item_id"] is not None else None
        ),
        person_mention_id=(
            int(row["person_mention_id"])
            if row["person_mention_id"] is not None
            else None
        ),
    )


def _person_article_record(row: sqlite3.Row) -> PersonArticleRecord:
    return PersonArticleRecord(
        id=int(row["id"]),
        person_id=int(row["person_id"]),
        canonical_article_id=int(row["canonical_article_id"]),
        first_plan_id=(
            int(row["first_plan_id"]) if row["first_plan_id"] is not None else None
        ),
        current_assessment_id=(
            int(row["current_assessment_id"])
            if row["current_assessment_id"] is not None
            else None
        ),
    )


def _assessment_record(row: sqlite3.Row) -> PersonArticleAssessmentRecord:
    return PersonArticleAssessmentRecord(
        id=int(row["id"]),
        person_article_id=int(row["person_article_id"]),
        person_id=int(row["person_id"]),
        canonical_article_id=int(row["canonical_article_id"]),
        plan_id=int(row["plan_id"]) if row["plan_id"] is not None else None,
        article_view_id=int(row["article_view_id"]),
        run_id=int(row["run_id"]),
        attempt_id=int(row["attempt_id"]) if row["attempt_id"] is not None else None,
        model_inspection_id=(
            int(row["model_inspection_id"])
            if row["model_inspection_id"] is not None
            else None
        ),
        disposition=str(row["disposition"]),
        person_relation=(
            str(row["person_relation"]) if row["person_relation"] is not None else None
        ),
        coverage_depth=(
            str(row["coverage_depth"]) if row["coverage_depth"] is not None else None
        ),
        content_types_json=(
            str(row["content_types_json"])
            if row["content_types_json"] is not None
            else None
        ),
        subject_relationship=(
            str(row["subject_relationship"])
            if row["subject_relationship"] is not None
            else None
        ),
        screening_rule_id=str(row["screening_rule_id"]),
        screening_rule_status=str(row["screening_rule_status"]),
        source_policy_fingerprint=str(row["source_policy_fingerprint"]),
        canonical_supplied_input_json=str(row["canonical_supplied_input_json"]),
        validated_output_json=(
            str(row["validated_output_json"])
            if row["validated_output_json"] is not None
            else None
        ),
        prompt_hash=(
            str(row["prompt_hash"]) if row["prompt_hash"] is not None else None
        ),
        schema_hash=(
            str(row["schema_hash"]) if row["schema_hash"] is not None else None
        ),
        schema_version=(
            int(row["schema_version"]) if row["schema_version"] is not None else None
        ),
        task_fingerprint=str(row["task_fingerprint"]),
        rationale=str(row["rationale"]),
        failure_category=(
            str(row["failure_category"])
            if row["failure_category"] is not None
            else None
        ),
        observed_at=str(row["observed_at"]),
    )


def open_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    source_policy_fingerprint: str,
    retrieval_target: int,
    created_at: str,
    refresh_of_plan_id: int | None = None,
    truncated_unsafe: bool = False,
    partial_retrieval: bool = False,
) -> int:
    """Open a retrieving coverage plan for a person material fingerprint.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "open_plan")
    cursor = connection.execute(
        """
        INSERT INTO person_coverage_plan (
            person_id, run_id, material_fingerprint, status,
            refresh_of_plan_id, source_policy_fingerprint,
            truncated_unsafe, partial_retrieval, retrieval_target,
            eligible_selected_count, created_at
        ) VALUES (?, ?, ?, 'retrieving', ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            person_id,
            run_id,
            material_fingerprint,
            refresh_of_plan_id,
            source_policy_fingerprint,
            _as_bool_int(truncated_unsafe),
            _as_bool_int(partial_retrieval),
            retrieval_target,
            created_at,
        ),
    )
    return _last_row_id(cursor)


def load_active_plan_for_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
) -> PersonCoveragePlanRecord | None:
    """Load the active (retrieving/selecting/assessing) plan for a fingerprint."""
    row = connection.execute(
        """
        SELECT *
          FROM person_coverage_plan
         WHERE person_id = ?
           AND material_fingerprint = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
         ORDER BY id
         LIMIT 1
        """,
        (person_id, material_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _plan_record(row)


def load_plan(
    connection: sqlite3.Connection, *, plan_id: int
) -> PersonCoveragePlanRecord | None:
    row = connection.execute(
        "SELECT * FROM person_coverage_plan WHERE id = ?",
        (plan_id,),
    ).fetchone()
    if row is None:
        return None
    return _plan_record(row)


def supersede_plan(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    completed_at: str,
) -> None:
    """Mark a plan superseded (frees the active partial-unique slot).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "supersede_plan")
    changed = connection.execute(
        """
        UPDATE person_coverage_plan
           SET status = 'superseded',
               completed_at = ?
         WHERE id = ?
        """,
        (completed_at, plan_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person_coverage_plan {plan_id} is missing; cannot supersede"
        )


def update_plan_status(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    status: str,
    completed_at: str | None = None,
    failure_category: str | None = None,
    truncated_unsafe: bool | None = None,
    partial_retrieval: bool | None = None,
    eligible_selected_count: int | None = None,
) -> None:
    """Update plan status and optional terminal fields.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "update_plan_status")
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None:
        raise RuntimeError(f"person_coverage_plan {plan_id} is missing")
    next_truncated = (
        plan.truncated_unsafe if truncated_unsafe is None else truncated_unsafe
    )
    next_partial = (
        plan.partial_retrieval if partial_retrieval is None else partial_retrieval
    )
    next_selected = (
        plan.eligible_selected_count
        if eligible_selected_count is None
        else eligible_selected_count
    )
    changed = connection.execute(
        """
        UPDATE person_coverage_plan
           SET status = ?,
               completed_at = ?,
               failure_category = ?,
               truncated_unsafe = ?,
               partial_retrieval = ?,
               eligible_selected_count = ?
         WHERE id = ?
        """,
        (
            status,
            completed_at,
            failure_category,
            _as_bool_int(next_truncated),
            _as_bool_int(next_partial),
            next_selected,
            plan_id,
        ),
    ).rowcount
    if changed != 1:
        raise RuntimeError(f"person_coverage_plan {plan_id} is missing; cannot update")


def insert_query_forms(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    forms: Sequence[Mapping[str, Any]],
) -> tuple[int, ...]:
    """Insert pending query forms for a plan; returns new form ids in order.

    Each mapping requires ``ordinal``, ``stage``, ``variant_kind``, and
    ``query_text``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_query_forms")
    ids: list[int] = []
    for form in forms:
        cursor = connection.execute(
            """
            INSERT INTO coverage_query_form (
                plan_id, ordinal, stage, variant_kind, query_text, status,
                offsets_used, truncated
            ) VALUES (?, ?, ?, ?, ?, 'pending', 0, 0)
            """,
            (
                plan_id,
                int(form["ordinal"]),
                int(form["stage"]),
                str(form["variant_kind"]),
                str(form["query_text"]),
            ),
        )
        ids.append(_last_row_id(cursor))
    return tuple(ids)


def load_query_form(
    connection: sqlite3.Connection, *, form_id: int
) -> CoverageQueryFormRecord | None:
    row = connection.execute(
        "SELECT * FROM coverage_query_form WHERE id = ?",
        (form_id,),
    ).fetchone()
    if row is None:
        return None
    return _query_form_record(row)


def list_query_forms_for_plan(
    connection: sqlite3.Connection, *, plan_id: int
) -> tuple[CoverageQueryFormRecord, ...]:
    rows = connection.execute(
        """
        SELECT *
          FROM coverage_query_form
         WHERE plan_id = ?
         ORDER BY ordinal
        """,
        (plan_id,),
    ).fetchall()
    return tuple(_query_form_record(row) for row in rows)


def mark_query_form_completed(
    connection: sqlite3.Connection,
    *,
    form_id: int,
    offsets_used: int,
    result_count: int,
    truncated: bool,
) -> None:
    """Mark a query form completed after its search work finishes.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "mark_query_form_completed")
    changed = connection.execute(
        """
        UPDATE coverage_query_form
           SET status = 'completed',
               offsets_used = ?,
               result_count = ?,
               truncated = ?
         WHERE id = ?
        """,
        (offsets_used, result_count, _as_bool_int(truncated), form_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(f"coverage_query_form {form_id} is missing; cannot complete")


def mark_query_form_failed(
    connection: sqlite3.Connection,
    *,
    form_id: int,
    failure_category: str,
    offsets_used: int | None = None,
    result_count: int | None = None,
    truncated: bool | None = None,
) -> None:
    """Mark a query form permanently failed.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "mark_query_form_failed")
    form = load_query_form(connection, form_id=form_id)
    if form is None:
        raise RuntimeError(f"coverage_query_form {form_id} is missing; cannot fail")
    next_offsets = form.offsets_used if offsets_used is None else offsets_used
    next_count = form.result_count if result_count is None else result_count
    next_truncated = form.truncated if truncated is None else truncated
    changed = connection.execute(
        """
        UPDATE coverage_query_form
           SET status = 'failed',
               failure_category = ?,
               offsets_used = ?,
               result_count = ?,
               truncated = ?
         WHERE id = ?
        """,
        (
            failure_category,
            next_offsets,
            next_count,
            _as_bool_int(next_truncated),
            form_id,
        ),
    ).rowcount
    if changed != 1:
        raise RuntimeError(f"coverage_query_form {form_id} is missing; cannot fail")


def mark_query_form_progress(
    connection: sqlite3.Connection,
    *,
    form_id: int,
    offsets_used: int,
    result_count: int,
    truncated: bool,
) -> None:
    """Update form counters while the form remains pending (more offsets).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "mark_query_form_progress")
    changed = connection.execute(
        """
        UPDATE coverage_query_form
           SET offsets_used = ?,
               result_count = ?,
               truncated = ?
         WHERE id = ? AND status = 'pending'
        """,
        (offsets_used, result_count, _as_bool_int(truncated), form_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"coverage_query_form {form_id} is missing or not pending; cannot progress"
        )


def insert_or_load_brave_search_observation_by_attempt(
    connection: sqlite3.Connection,
    *,
    query_form_id: int,
    run_id: int,
    attempt_id: int,
    query_text: str,
    altered_query: str | None,
    offset_in: int,
    count_requested: int,
    result_count: int,
    truncated: bool,
    response_complete: bool,
    observed_at: str,
) -> int:
    """Insert a Brave search observation, or load the existing row for attempt.

    Never double-inserts for one attempt (UNIQUE(attempt_id)). Callers must not
    re-insert occurrences when this returns an existing id.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(
        connection, "insert_or_load_brave_search_observation_by_attempt"
    )
    existing = connection.execute(
        """
        SELECT id
          FROM brave_search_observation
         WHERE attempt_id = ?
        """,
        (attempt_id,),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO brave_search_observation (
            query_form_id, run_id, attempt_id, query_text, altered_query,
            offset_in, count_requested, result_count, truncated,
            response_complete, observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            query_form_id,
            run_id,
            attempt_id,
            query_text,
            altered_query,
            offset_in,
            count_requested,
            result_count,
            _as_bool_int(truncated),
            _as_bool_int(response_complete),
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def load_brave_search_observation(
    connection: sqlite3.Connection, *, observation_id: int
) -> BraveSearchObservationRecord | None:
    row = connection.execute(
        "SELECT * FROM brave_search_observation WHERE id = ?",
        (observation_id,),
    ).fetchone()
    if row is None:
        return None
    return BraveSearchObservationRecord(
        id=int(row["id"]),
        query_form_id=int(row["query_form_id"]),
        run_id=int(row["run_id"]),
        attempt_id=int(row["attempt_id"]),
        query_text=str(row["query_text"]),
        altered_query=(
            str(row["altered_query"]) if row["altered_query"] is not None else None
        ),
        offset_in=int(row["offset_in"]),
        count_requested=int(row["count_requested"]),
        result_count=int(row["result_count"]),
        truncated=bool(row["truncated"]),
        response_complete=bool(row["response_complete"]),
        observed_at=str(row["observed_at"]),
    )


def insert_search_result_occurrences(
    connection: sqlite3.Connection,
    *,
    search_observation_id: int,
    occurrences: Sequence[Mapping[str, Any]],
) -> None:
    """Insert ranked Brave search result occurrences for one observation.

    Each mapping requires ``rank`` and ``url``. Optional: title, snippet,
    extra_snippet, language, provider_result_id, canonical_article_id,
    screening_id.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_search_result_occurrences")
    for occurrence in occurrences:
        connection.execute(
            """
            INSERT INTO brave_search_result_occurrence (
                search_observation_id, rank, url, title, snippet, extra_snippet,
                language, provider_result_id, canonical_article_id, screening_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                search_observation_id,
                int(occurrence["rank"]),
                str(occurrence["url"]),
                occurrence.get("title"),
                occurrence.get("snippet"),
                occurrence.get("extra_snippet"),
                occurrence.get("language"),
                occurrence.get("provider_result_id"),
                occurrence.get("canonical_article_id"),
                occurrence.get("screening_id"),
            ),
        )


def insert_source_screening(
    connection: sqlite3.Connection,
    *,
    canonical_article_id: int | None,
    url: str,
    publisher_key: str | None,
    rule_id: str,
    rule_status: str,
    source_policy_fingerprint: str,
    decided_at: str,
    plan_id: int | None = None,
    source_item_id: int | None = None,
    person_mention_id: int | None = None,
) -> int:
    """Insert one source screening decision row.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_source_screening")
    cursor = connection.execute(
        """
        INSERT INTO source_screening (
            canonical_article_id, url, publisher_key, rule_id, rule_status,
            source_policy_fingerprint, decided_at, plan_id,
            source_item_id, person_mention_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            canonical_article_id,
            url,
            publisher_key,
            rule_id,
            rule_status,
            source_policy_fingerprint,
            decided_at,
            plan_id,
            source_item_id,
            person_mention_id,
        ),
    )
    return _last_row_id(cursor)


def load_source_screening(
    connection: sqlite3.Connection, *, screening_id: int
) -> SourceScreeningRecord | None:
    row = connection.execute(
        "SELECT * FROM source_screening WHERE id = ?",
        (screening_id,),
    ).fetchone()
    if row is None:
        return None
    return _screening_record(row)


def insert_coverage_discovery_article(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    canonical_article_id: int,
    source_item_id: int,
    screening_id: int,
    person_mention_id: int | None = None,
) -> int:
    """Insert a usable discovery article row (screening_id required — K10).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_coverage_discovery_article")
    cursor = connection.execute(
        """
        INSERT INTO coverage_discovery_article (
            plan_id, canonical_article_id, source_item_id, person_mention_id,
            screening_id
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            plan_id,
            canonical_article_id,
            source_item_id,
            person_mention_id,
            screening_id,
        ),
    )
    return _last_row_id(cursor)


def load_coverage_discovery_article(
    connection: sqlite3.Connection, *, discovery_id: int
) -> CoverageDiscoveryArticleRecord | None:
    row = connection.execute(
        "SELECT * FROM coverage_discovery_article WHERE id = ?",
        (discovery_id,),
    ).fetchone()
    if row is None:
        return None
    return CoverageDiscoveryArticleRecord(
        id=int(row["id"]),
        plan_id=int(row["plan_id"]),
        canonical_article_id=int(row["canonical_article_id"]),
        source_item_id=int(row["source_item_id"]),
        person_mention_id=(
            int(row["person_mention_id"])
            if row["person_mention_id"] is not None
            else None
        ),
        screening_id=int(row["screening_id"]),
    )


def insert_coverage_article_target(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    canonical_article_id: int,
    request_url: str,
    selection_reason: str,
    status: str = "pending",
) -> int:
    """Insert a fetch/assess target for a plan and canonical article.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_coverage_article_target")
    cursor = connection.execute(
        """
        INSERT INTO coverage_article_target (
            plan_id, canonical_article_id, request_url, selection_reason, status
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (plan_id, canonical_article_id, request_url, selection_reason, status),
    )
    return _last_row_id(cursor)


def load_coverage_article_target(
    connection: sqlite3.Connection, *, target_id: int
) -> CoverageArticleTargetRecord | None:
    row = connection.execute(
        "SELECT * FROM coverage_article_target WHERE id = ?",
        (target_id,),
    ).fetchone()
    if row is None:
        return None
    return CoverageArticleTargetRecord(
        id=int(row["id"]),
        plan_id=int(row["plan_id"]),
        canonical_article_id=int(row["canonical_article_id"]),
        request_url=str(row["request_url"]),
        selection_reason=str(row["selection_reason"]),
        status=str(row["status"]),
        article_view_id=(
            int(row["article_view_id"]) if row["article_view_id"] is not None else None
        ),
        attempt_id=int(row["attempt_id"]) if row["attempt_id"] is not None else None,
        failure_category=(
            str(row["failure_category"])
            if row["failure_category"] is not None
            else None
        ),
    )


def update_coverage_article_target(
    connection: sqlite3.Connection,
    *,
    target_id: int,
    status: str,
    article_view_id: int | None = None,
    attempt_id: int | None = None,
    failure_category: str | None = None,
) -> None:
    """Update target status and optional view/attempt linkage.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "update_coverage_article_target")
    changed = connection.execute(
        """
        UPDATE coverage_article_target
           SET status = ?,
               article_view_id = COALESCE(?, article_view_id),
               attempt_id = COALESCE(?, attempt_id),
               failure_category = ?
         WHERE id = ?
        """,
        (status, article_view_id, attempt_id, failure_category, target_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"coverage_article_target {target_id} is missing; cannot update"
        )


def insert_article_view(
    connection: sqlite3.Connection,
    *,
    canonical_article_id: int,
    run_id: int,
    attempt_id: int | None,
    access_kind: str,
    requested_url: str | None,
    final_url: str | None,
    title: str | None,
    dek: str | None,
    byline: str | None,
    published_at: str | None,
    editorial_labels_json: str,
    main_text_blocks_json: str,
    snippets_json: str,
    extraction_quality: str | None,
    extractor_version: int,
    observed_at: str,
) -> int:
    """Insert a cleaned article view (never HTML — K3).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_article_view")
    cursor = connection.execute(
        """
        INSERT INTO article_view (
            canonical_article_id, run_id, attempt_id, access_kind,
            requested_url, final_url, title, dek, byline, published_at,
            editorial_labels_json, main_text_blocks_json, snippets_json,
            extraction_quality, extractor_version, observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            canonical_article_id,
            run_id,
            attempt_id,
            access_kind,
            requested_url,
            final_url,
            title,
            dek,
            byline,
            published_at,
            editorial_labels_json,
            main_text_blocks_json,
            snippets_json,
            extraction_quality,
            extractor_version,
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def load_article_view(
    connection: sqlite3.Connection, *, view_id: int
) -> ArticleViewRecord | None:
    row = connection.execute(
        "SELECT * FROM article_view WHERE id = ?",
        (view_id,),
    ).fetchone()
    if row is None:
        return None
    return ArticleViewRecord(
        id=int(row["id"]),
        canonical_article_id=int(row["canonical_article_id"]),
        run_id=int(row["run_id"]),
        attempt_id=int(row["attempt_id"]) if row["attempt_id"] is not None else None,
        access_kind=str(row["access_kind"]),
        requested_url=(
            str(row["requested_url"]) if row["requested_url"] is not None else None
        ),
        final_url=str(row["final_url"]) if row["final_url"] is not None else None,
        title=str(row["title"]) if row["title"] is not None else None,
        dek=str(row["dek"]) if row["dek"] is not None else None,
        byline=str(row["byline"]) if row["byline"] is not None else None,
        published_at=(
            str(row["published_at"]) if row["published_at"] is not None else None
        ),
        editorial_labels_json=str(row["editorial_labels_json"]),
        main_text_blocks_json=str(row["main_text_blocks_json"]),
        snippets_json=str(row["snippets_json"]),
        extraction_quality=(
            str(row["extraction_quality"])
            if row["extraction_quality"] is not None
            else None
        ),
        extractor_version=int(row["extractor_version"]),
        observed_at=str(row["observed_at"]),
    )


def upsert_person_article(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    canonical_article_id: int,
    first_plan_id: int | None = None,
) -> int:
    """Insert or load the person–article relation (K31).

    On conflict, keeps the existing first_plan_id when already set.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "upsert_person_article")
    cursor = connection.execute(
        """
        INSERT INTO person_article (
            person_id, canonical_article_id, first_plan_id
        ) VALUES (?, ?, ?)
        ON CONFLICT(person_id, canonical_article_id) DO UPDATE SET
            first_plan_id = COALESCE(
                person_article.first_plan_id,
                excluded.first_plan_id
            )
        RETURNING id
        """,
        (person_id, canonical_article_id, first_plan_id),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("upsert_person_article did not return an id")
    return int(row["id"] if isinstance(row, sqlite3.Row) else row[0])


def load_person_article(
    connection: sqlite3.Connection, *, person_article_id: int
) -> PersonArticleRecord | None:
    row = connection.execute(
        "SELECT * FROM person_article WHERE id = ?",
        (person_article_id,),
    ).fetchone()
    if row is None:
        return None
    return _person_article_record(row)


def load_person_article_by_pair(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    canonical_article_id: int,
) -> PersonArticleRecord | None:
    row = connection.execute(
        """
        SELECT *
          FROM person_article
         WHERE person_id = ? AND canonical_article_id = ?
        """,
        (person_id, canonical_article_id),
    ).fetchone()
    if row is None:
        return None
    return _person_article_record(row)


def insert_person_article_assessment(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    person_id: int,
    canonical_article_id: int,
    plan_id: int | None,
    article_view_id: int,
    run_id: int,
    attempt_id: int | None,
    model_inspection_id: int | None,
    disposition: str,
    person_relation: str | None,
    coverage_depth: str | None,
    content_types_json: str | None,
    subject_relationship: str | None,
    screening_rule_id: str,
    screening_rule_status: str,
    source_policy_fingerprint: str,
    canonical_supplied_input_json: str,
    validated_output_json: str | None,
    prompt_hash: str | None,
    schema_hash: str | None,
    schema_version: int | None,
    task_fingerprint: str,
    rationale: str,
    failure_category: str | None,
    observed_at: str,
) -> int:
    """Insert one assessment, or reuse the material fingerprint row.

    Reuse is keyed by ``UNIQUE (person_article_id, task_fingerprint)``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_person_article_assessment")
    existing = connection.execute(
        """
        SELECT id
          FROM person_article_assessment
         WHERE person_article_id = ? AND task_fingerprint = ?
        """,
        (person_article_id, task_fingerprint),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO person_article_assessment (
            person_article_id, person_id, canonical_article_id, plan_id,
            article_view_id, run_id, attempt_id, model_inspection_id,
            disposition, person_relation, coverage_depth, content_types_json,
            subject_relationship, screening_rule_id, screening_rule_status,
            source_policy_fingerprint, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, rationale, failure_category, observed_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?
        )
        """,
        (
            person_article_id,
            person_id,
            canonical_article_id,
            plan_id,
            article_view_id,
            run_id,
            attempt_id,
            model_inspection_id,
            disposition,
            person_relation,
            coverage_depth,
            content_types_json,
            subject_relationship,
            screening_rule_id,
            screening_rule_status,
            source_policy_fingerprint,
            canonical_supplied_input_json,
            validated_output_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            rationale,
            failure_category,
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def load_person_article_assessment(
    connection: sqlite3.Connection, *, assessment_id: int
) -> PersonArticleAssessmentRecord | None:
    row = connection.execute(
        "SELECT * FROM person_article_assessment WHERE id = ?",
        (assessment_id,),
    ).fetchone()
    if row is None:
        return None
    return _assessment_record(row)


def insert_assessment_signals(
    connection: sqlite3.Connection,
    *,
    assessment_id: int,
    signals: Sequence[Mapping[str, Any]],
) -> None:
    """Insert attention/caution signals for one assessment.

    Each mapping requires ``signal_kind``, ``category``, ``claim``,
    ``supporting_passage_ids_json``, and ``ordinal``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_assessment_signals")
    for signal in signals:
        connection.execute(
            """
            INSERT INTO article_assessment_signal (
                assessment_id, signal_kind, category, claim,
                supporting_passage_ids_json, ordinal
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                assessment_id,
                str(signal["signal_kind"]),
                str(signal["category"]),
                str(signal["claim"]),
                str(signal["supporting_passage_ids_json"]),
                int(signal["ordinal"]),
            ),
        )


def point_person_article_current_assessment(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    assessment_id: int,
) -> None:
    """Point a person_article at a completed assessment (K25).

    Ownership triggers reject failed assessments and wrong-relation rows.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "point_person_article_current_assessment")
    changed = connection.execute(
        """
        UPDATE person_article
           SET current_assessment_id = ?
         WHERE id = ?
        """,
        (assessment_id, person_article_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person_article {person_article_id} is missing; "
            "cannot set current assessment"
        )


def supersede_pending_targets_for_plan(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
) -> int:
    """Mark pending targets on a plan as superseded. Returns rows changed.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "supersede_pending_targets_for_plan")
    return int(
        connection.execute(
            """
            UPDATE coverage_article_target
               SET status = 'superseded'
             WHERE plan_id = ?
               AND status = 'pending'
            """,
            (plan_id,),
        ).rowcount
        or 0
    )
