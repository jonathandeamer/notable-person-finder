"""Transaction-neutral persistence for Wikipedia identity rows.

Writers neither begin, commit, nor roll back: they join the caller's open
transaction so domain rows and the settlement that justifies them commit or
roll back together.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping, Sequence
from typing import Any

from notable_person_finder.wikipedia.models import (
    WikipediaIdentityObservationRecord,
    WikipediaIdentityPlanRecord,
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


def _plan_record(row: sqlite3.Row) -> WikipediaIdentityPlanRecord:
    return WikipediaIdentityPlanRecord(
        id=int(row["id"]),
        person_id=int(row["person_id"]),
        run_id=int(row["run_id"]),
        material_fingerprint=str(row["material_fingerprint"]),
        status=str(row["status"]),
        refresh_of_observation_id=(
            int(row["refresh_of_observation_id"])
            if row["refresh_of_observation_id"] is not None
            else None
        ),
        truncated_unsafe_for_negative=bool(row["truncated_unsafe_for_negative"]),
        partial_retrieval=bool(row["partial_retrieval"]),
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


def _observation_record(row: sqlite3.Row) -> WikipediaIdentityObservationRecord:
    return WikipediaIdentityObservationRecord(
        id=int(row["id"]),
        person_id=int(row["person_id"]),
        plan_id=int(row["plan_id"]) if row["plan_id"] is not None else None,
        run_id=int(row["run_id"]),
        attempt_id=int(row["attempt_id"]) if row["attempt_id"] is not None else None,
        model_inspection_id=(
            int(row["model_inspection_id"])
            if row["model_inspection_id"] is not None
            else None
        ),
        disposition=str(row["disposition"]),
        semantic_outcome=(
            str(row["semantic_outcome"])
            if row["semantic_outcome"] is not None
            else None
        ),
        matched_mediawiki_page_id=(
            int(row["matched_mediawiki_page_id"])
            if row["matched_mediawiki_page_id"] is not None
            else None
        ),
        candidate_page_ids_json=str(row["candidate_page_ids_json"]),
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
        supporting_fact_ids_json=(
            str(row["supporting_fact_ids_json"])
            if row["supporting_fact_ids_json"] is not None
            else None
        ),
        conflicting_fact_ids_json=(
            str(row["conflicting_fact_ids_json"])
            if row["conflicting_fact_ids_json"] is not None
            else None
        ),
        rationale=str(row["rationale"]),
        failure_category=(
            str(row["failure_category"])
            if row["failure_category"] is not None
            else None
        ),
        observed_at=str(row["observed_at"]),
    )


def upsert_mediawiki_page(
    connection: sqlite3.Connection,
    *,
    wiki_id: str,
    page_id: int,
    canonical_title: str,
    canonical_url: str,
    namespace: int,
    is_disambiguation: bool,
    is_missing: bool,
    redirect_to_page_id: int | None,
    description: str | None,
    extract: str | None,
    categories_json: str,
    last_observed_at: str,
    last_attempt_id: int | None,
) -> int:
    """Insert or refresh a MediaWiki page row keyed by ``(wiki_id, page_id)``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "upsert_mediawiki_page")
    cursor = connection.execute(
        """
        INSERT INTO mediawiki_page (
            wiki_id, page_id, canonical_title, canonical_url, namespace,
            is_disambiguation, is_missing, redirect_to_page_id, description,
            extract, categories_json, last_observed_at, last_attempt_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wiki_id, page_id) DO UPDATE SET
            canonical_title = excluded.canonical_title,
            canonical_url = excluded.canonical_url,
            namespace = excluded.namespace,
            is_disambiguation = excluded.is_disambiguation,
            is_missing = excluded.is_missing,
            redirect_to_page_id = excluded.redirect_to_page_id,
            description = excluded.description,
            extract = excluded.extract,
            categories_json = excluded.categories_json,
            last_observed_at = excluded.last_observed_at,
            last_attempt_id = excluded.last_attempt_id
        RETURNING id
        """,
        (
            wiki_id,
            page_id,
            canonical_title,
            canonical_url,
            namespace,
            _as_bool_int(is_disambiguation),
            _as_bool_int(is_missing),
            redirect_to_page_id,
            description,
            extract,
            categories_json,
            last_observed_at,
            last_attempt_id,
        ),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("upsert_mediawiki_page did not return an id")
    return int(row["id"] if isinstance(row, sqlite3.Row) else row[0])


def open_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    created_at: str,
    refresh_of_observation_id: int | None = None,
    truncated_unsafe_for_negative: bool = False,
    partial_retrieval: bool = False,
) -> int:
    """Open a retrieving Wikipedia identity plan for a person fingerprint.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "open_plan")
    cursor = connection.execute(
        """
        INSERT INTO wikipedia_identity_plan (
            person_id, run_id, material_fingerprint, status,
            refresh_of_observation_id, truncated_unsafe_for_negative,
            partial_retrieval, created_at
        ) VALUES (?, ?, ?, 'retrieving', ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            material_fingerprint,
            refresh_of_observation_id,
            _as_bool_int(truncated_unsafe_for_negative),
            _as_bool_int(partial_retrieval),
            created_at,
        ),
    )
    return _last_row_id(cursor)


def load_active_plan_for_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
) -> WikipediaIdentityPlanRecord | None:
    """Load the active (retrieving/ready_for_match) plan for a fingerprint."""
    row = connection.execute(
        """
        SELECT *
          FROM wikipedia_identity_plan
         WHERE person_id = ?
           AND material_fingerprint = ?
           AND status IN ('retrieving', 'ready_for_match')
         ORDER BY id
         LIMIT 1
        """,
        (person_id, material_fingerprint),
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
        UPDATE wikipedia_identity_plan
           SET status = 'superseded',
               completed_at = ?
         WHERE id = ?
        """,
        (completed_at, plan_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"wikipedia_identity_plan {plan_id} is missing; cannot supersede"
        )


def insert_query_forms(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    forms: Sequence[Mapping[str, Any]],
) -> tuple[int, ...]:
    """Insert pending query forms for a plan; returns new form ids in order.

    Each mapping requires ``ordinal``, ``variant_kind``, and ``query_text``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_query_forms")
    ids: list[int] = []
    for form in forms:
        cursor = connection.execute(
            """
            INSERT INTO wikipedia_query_form (
                plan_id, ordinal, variant_kind, query_text, status,
                continuations_used, truncated
            ) VALUES (?, ?, ?, ?, 'pending', 0, 0)
            """,
            (
                plan_id,
                int(form["ordinal"]),
                str(form["variant_kind"]),
                str(form["query_text"]),
            ),
        )
        ids.append(_last_row_id(cursor))
    return tuple(ids)


def insert_page_facts_batch(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    ordinal: int,
    page_ids_json: str,
    wave: int,
    created_at: str,
) -> int:
    """Insert a pending page-facts batch subject for ``mediawiki_page_facts``.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_page_facts_batch")
    cursor = connection.execute(
        """
        INSERT INTO wikipedia_page_facts_batch (
            plan_id, ordinal, page_ids_json, status, wave, created_at
        ) VALUES (?, ?, ?, 'pending', ?, ?)
        """,
        (plan_id, ordinal, page_ids_json, wave, created_at),
    )
    return _last_row_id(cursor)


def mark_batch_completed(
    connection: sqlite3.Connection,
    *,
    batch_id: int,
    attempt_id: int,
    completed_at: str,
) -> None:
    """Mark a facts batch completed and bind its unique attempt_id.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "mark_batch_completed")
    changed = connection.execute(
        """
        UPDATE wikipedia_page_facts_batch
           SET status = 'completed',
               attempt_id = ?,
               completed_at = ?
         WHERE id = ?
        """,
        (attempt_id, completed_at, batch_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"wikipedia_page_facts_batch {batch_id} is missing; cannot complete"
        )


def insert_or_load_search_observation_by_attempt(
    connection: sqlite3.Connection,
    *,
    query_form_id: int,
    run_id: int,
    attempt_id: int,
    query_text: str,
    continuation_in: str | None,
    continuation_out: str | None,
    srlimit: int,
    hit_count: int,
    truncated: bool,
    response_complete: bool,
    observed_at: str,
) -> int:
    """Insert a search observation, or load the existing row for ``attempt_id``.

    Never double-inserts for one attempt (UNIQUE(attempt_id)). Callers must not
    re-insert hits when this returns an existing id.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_or_load_search_observation_by_attempt")
    existing = connection.execute(
        """
        SELECT id
          FROM mediawiki_search_observation
         WHERE attempt_id = ?
        """,
        (attempt_id,),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO mediawiki_search_observation (
            query_form_id, run_id, attempt_id, query_text, continuation_in,
            continuation_out, srlimit, hit_count, truncated, response_complete,
            observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            query_form_id,
            run_id,
            attempt_id,
            query_text,
            continuation_in,
            continuation_out,
            srlimit,
            hit_count,
            _as_bool_int(truncated),
            _as_bool_int(response_complete),
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def insert_search_hits(
    connection: sqlite3.Connection,
    *,
    search_observation_id: int,
    hits: Sequence[Mapping[str, Any]],
) -> None:
    """Insert ranked search hits for one observation.

    Each mapping requires ``rank`` and ``title``; ``page_id`` may be null.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_search_hits")
    for hit in hits:
        connection.execute(
            """
            INSERT INTO mediawiki_search_hit (
                search_observation_id, rank, page_id, title
            ) VALUES (?, ?, ?, ?)
            """,
            (
                search_observation_id,
                int(hit["rank"]),
                hit.get("page_id"),
                str(hit["title"]),
            ),
        )


def load_wikipedia_identity_observation_by_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    task_fingerprint: str,
) -> WikipediaIdentityObservationRecord | None:
    """Load the observation for a person and material fingerprint, if any."""
    row = connection.execute(
        """
        SELECT *
          FROM wikipedia_identity_observation
         WHERE person_id = ? AND task_fingerprint = ?
        """,
        (person_id, task_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _observation_record(row)


def insert_wikipedia_identity_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    plan_id: int | None,
    run_id: int,
    attempt_id: int | None,
    model_inspection_id: int | None,
    disposition: str,
    semantic_outcome: str | None,
    matched_mediawiki_page_id: int | None,
    candidate_page_ids_json: str,
    canonical_supplied_input_json: str,
    validated_output_json: str | None,
    prompt_hash: str | None,
    schema_hash: str | None,
    schema_version: int | None,
    task_fingerprint: str,
    rationale: str,
    failure_category: str | None,
    observed_at: str,
    supporting_fact_ids_json: str | None = None,
    conflicting_fact_ids_json: str | None = None,
) -> int:
    """Insert one Wikipedia identity observation, or reuse the material row.

    Reuse is keyed by ``UNIQUE (person_id, task_fingerprint)``. Disposition
    combinations must satisfy the outcome truth-table CHECK.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_wikipedia_identity_observation")
    existing = load_wikipedia_identity_observation_by_fingerprint(
        connection,
        person_id=person_id,
        task_fingerprint=task_fingerprint,
    )
    if existing is not None:
        return existing.id

    cursor = connection.execute(
        """
        INSERT INTO wikipedia_identity_observation (
            person_id, plan_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, matched_mediawiki_page_id,
            candidate_page_ids_json, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, supporting_fact_ids_json,
            conflicting_fact_ids_json, rationale, failure_category, observed_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            person_id,
            plan_id,
            run_id,
            attempt_id,
            model_inspection_id,
            disposition,
            semantic_outcome,
            matched_mediawiki_page_id,
            candidate_page_ids_json,
            canonical_supplied_input_json,
            validated_output_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            supporting_fact_ids_json,
            conflicting_fact_ids_json,
            rationale,
            failure_category,
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def point_person_current_wikipedia_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    observation_id: int,
) -> None:
    """Point a person at a completed Wikipedia identity observation (K25).

    Ownership triggers reject failed observations and wrong-person rows.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "point_person_current_wikipedia_observation")
    changed = connection.execute(
        """
        UPDATE person
           SET current_wikipedia_identity_observation_id = ?
         WHERE id = ?
        """,
        (observation_id, person_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person {person_id} is missing; cannot set current wikipedia identity"
        )
