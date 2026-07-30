"""Transaction-neutral Wikipedia identity repository writers."""

from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.wikipedia.repository import (
    insert_or_load_search_observation_by_attempt,
    insert_page_facts_batch,
    insert_query_forms,
    insert_search_hits,
    insert_wikipedia_identity_observation,
    load_active_plan_for_fingerprint,
    mark_batch_completed,
    open_plan,
    point_person_current_wikipedia_observation,
    supersede_plan,
    upsert_mediawiki_page,
)
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_PROMPT_HASH = "d" * 64
_SCHEMA_HASH = "e" * 64


def _work_item(connection: sqlite3.Connection, *, run_id: int) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('mediawiki_search', 'person', 1, ?, 1, 50, ?, 'running', ?, ?, ?)
        """,
        (_HASH, moment(), run_id, moment(), moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    provider: str = "mediawiki",
    operation: str = "search_pages",
    fingerprint: str = _HASH,
) -> int:
    work_item_id = _work_item(connection, run_id=run_id)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, ?, ?, 1, ?, ?, 'succeeded', ?)
        """,
        (
            run_id,
            work_item_id,
            provider,
            operation,
            moment(),
            moment(1),
            fingerprint,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _inspection(connection: sqlite3.Connection, *, run_id: int, attempt_id: int) -> int:
    cursor = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, _HASH, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _person(connection: sqlite3.Connection, *, run_id: int) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, 'Alex Smith', ?)
        """,
        (moment(), run_id, _HASH),
    )
    assert cursor.lastrowid is not None
    # Commit so the next BEGIN IMMEDIATE (domain-writer shape) is not nested.
    connection.commit()
    return cursor.lastrowid


def test_writers_require_open_transaction(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with pytest.raises(RuntimeError, match="active transaction"):
        open_plan(
            connection,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )


def test_upsert_mediawiki_page_inserts_and_updates(
    connection: sqlite3.Connection,
) -> None:
    attempt_id = None
    with immediate(connection) as conn:
        page_id = upsert_mediawiki_page(
            conn,
            wiki_id="enwiki",
            page_id=42,
            canonical_title="Alex Smith",
            canonical_url="https://en.wikipedia.org/wiki/Alex_Smith",
            namespace=0,
            is_disambiguation=False,
            is_missing=False,
            redirect_to_page_id=None,
            description="writer",
            extract="extract one",
            categories_json='["People"]',
            last_observed_at=moment(),
            last_attempt_id=attempt_id,
        )
        same_id = upsert_mediawiki_page(
            conn,
            wiki_id="enwiki",
            page_id=42,
            canonical_title="Alex Smith",
            canonical_url="https://en.wikipedia.org/wiki/Alex_Smith",
            namespace=0,
            is_disambiguation=False,
            is_missing=False,
            redirect_to_page_id=99,
            description="updated",
            extract="extract two",
            categories_json='["Writers"]',
            last_observed_at=moment(1),
            last_attempt_id=None,
        )
        assert page_id == same_id
        row = conn.execute(
            "SELECT description, extract, redirect_to_page_id FROM mediawiki_page"
            " WHERE id = ?",
            (page_id,),
        ).fetchone()
        assert tuple(row) == ("updated", "extract two", 99)


def test_open_plan_and_active_load_and_supersede(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        active = load_active_plan_for_fingerprint(
            conn, person_id=person_id, material_fingerprint=_HASH
        )
        assert active is not None
        assert active.id == plan_id
        assert active.status == "retrieving"

        forms = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {"ordinal": 1, "variant_kind": "exact", "query_text": "Alex Smith"},
                {
                    "ordinal": 2,
                    "variant_kind": "comma_swap",
                    "query_text": "Smith, Alex",
                },
            ),
        )
        assert forms == (1, 2) or len(forms) == 2

        supersede_plan(conn, plan_id=plan_id, completed_at=moment(2))
        assert (
            load_active_plan_for_fingerprint(
                conn, person_id=person_id, material_fingerprint=_HASH
            )
            is None
        )
        status = conn.execute(
            "SELECT status FROM wikipedia_identity_plan WHERE id = ?",
            (plan_id,),
        ).fetchone()[0]
        assert status == "superseded"


def test_insert_or_load_search_observation_is_idempotent_by_attempt(
    connection: sqlite3.Connection,
) -> None:
    """Double-insert same attempt_id loads one row; hits not duplicated."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {"ordinal": 1, "variant_kind": "exact", "query_text": "Alex Smith"},
            ),
        )
        form_id = form_ids[0]
        attempt_id = _attempt(connection, run_id=run_id)

        first = insert_or_load_search_observation_by_attempt(
            conn,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text="Alex Smith",
            continuation_in=None,
            continuation_out=None,
            srlimit=10,
            hit_count=2,
            truncated=False,
            response_complete=True,
            observed_at=moment(),
        )
        insert_search_hits(
            conn,
            search_observation_id=first,
            hits=(
                {"rank": 1, "page_id": 100, "title": "Alex Smith"},
                {"rank": 2, "page_id": 200, "title": "Alex Smith (disambiguation)"},
            ),
        )
        second = insert_or_load_search_observation_by_attempt(
            conn,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text="Alex Smith",
            continuation_in=None,
            continuation_out=None,
            srlimit=10,
            hit_count=99,
            truncated=True,
            response_complete=False,
            observed_at=moment(5),
        )
        assert first == second
        count = conn.execute(
            "SELECT COUNT(*) AS n FROM mediawiki_search_observation"
            " WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()["n"]
        assert count == 1
        hit_count = conn.execute(
            "SELECT COUNT(*) AS n FROM mediawiki_search_hit"
            " WHERE search_observation_id = ?",
            (first,),
        ).fetchone()["n"]
        assert hit_count == 2


def test_page_facts_batch_mark_completed(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        batch_id = insert_page_facts_batch(
            conn,
            plan_id=plan_id,
            ordinal=1,
            page_ids_json="[100,200]",
            wave=1,
            created_at=moment(),
        )
        attempt_id = _attempt(connection, run_id=run_id, operation="get_page_facts")
        mark_batch_completed(
            conn,
            batch_id=batch_id,
            attempt_id=attempt_id,
            completed_at=moment(1),
        )
        row = conn.execute(
            "SELECT status, attempt_id FROM wikipedia_page_facts_batch WHERE id = ?",
            (batch_id,),
        ).fetchone()
        assert tuple(row) == ("completed", attempt_id)


def test_insert_wikipedia_identity_observation_deterministic_and_point(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="no_matching_page_found",
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_HASH,
            rationale="empty complete search",
            failure_category=None,
            observed_at=moment(),
        )
        reused = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="no_matching_page_found",
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_HASH,
            rationale="empty complete search",
            failure_category=None,
            observed_at=moment(1),
        )
        assert reused == obs_id
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )
        pointed = conn.execute(
            "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
            (person_id,),
        ).fetchone()[0]
        assert pointed == obs_id


def test_insert_model_path_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        page_id = upsert_mediawiki_page(
            conn,
            wiki_id="enwiki",
            page_id=1001,
            canonical_title="Alex Smith",
            canonical_url="https://en.wikipedia.org/wiki/Alex_Smith",
            namespace=0,
            is_disambiguation=False,
            is_missing=False,
            redirect_to_page_id=None,
            description=None,
            extract="bio",
            categories_json="[]",
            last_observed_at=moment(),
            last_attempt_id=None,
        )
        attempt_id = _attempt(
            connection,
            run_id=run_id,
            provider="openrouter",
            operation="generate_structured",
            fingerprint=_OTHER_HASH,
        )
        inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="matching_page_found",
            matched_mediawiki_page_id=page_id,
            candidate_page_ids_json="[1001]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"matching_page"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_OTHER_HASH,
            rationale="same person as page",
            failure_category=None,
            observed_at=moment(),
        )
        assert obs_id > 0
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )


def test_point_rejects_failed_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=moment(),
        )
        failed_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="failed",
            semantic_outcome=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_HASH,
            rationale="unsafe truncation",
            failure_category="unsafe_truncation",
            observed_at=moment(),
        )
        with pytest.raises(sqlite3.IntegrityError):
            point_person_current_wikipedia_observation(
                conn, person_id=person_id, observation_id=failed_id
            )
