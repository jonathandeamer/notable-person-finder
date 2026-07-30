"""Transaction-neutral coverage evidence repository writers."""

from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.coverage.repository import (
    insert_article_view,
    insert_assessment_signals,
    insert_coverage_article_target,
    insert_coverage_discovery_article,
    insert_or_load_brave_search_observation_by_attempt,
    insert_person_article_assessment,
    insert_query_forms,
    insert_search_result_occurrences,
    insert_source_screening,
    load_active_plan_for_fingerprint,
    open_plan,
    point_person_article_current_assessment,
    supersede_plan,
    upsert_person_article,
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
        VALUES (
            'brave_web_search', 'coverage_query_form', 1, ?, 1, 60, ?,
            'running', ?, ?, ?
        )
        """,
        (_HASH, moment(), run_id, moment(), moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    provider: str = "brave",
    operation: str = "search_web",
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
    connection.commit()
    return cursor.lastrowid


def _canonical_article(
    connection: sqlite3.Connection,
    *,
    url: str = "https://example.com/a",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
        VALUES (?, 'example.com', ?)
        """,
        (url, moment()),
    )
    assert cursor.lastrowid is not None
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
            source_policy_fingerprint=_OTHER_HASH,
            retrieval_target=5,
            created_at=moment(),
        )


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
            source_policy_fingerprint=_OTHER_HASH,
            retrieval_target=5,
            created_at=moment(),
        )
        active = load_active_plan_for_fingerprint(
            conn, person_id=person_id, material_fingerprint=_HASH
        )
        assert active is not None
        assert active.id == plan_id
        assert active.status == "retrieving"
        assert active.retrieval_target == 5

        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": "Alex Smith",
                },
                {
                    "ordinal": 2,
                    "stage": 1,
                    "variant_kind": "exact_obituary",
                    "query_text": "Alex Smith obituary",
                },
            ),
        )
        assert len(form_ids) == 2

        supersede_plan(conn, plan_id=plan_id, completed_at=moment(2))
        assert (
            load_active_plan_for_fingerprint(
                conn, person_id=person_id, material_fingerprint=_HASH
            )
            is None
        )
        status = conn.execute(
            "SELECT status FROM person_coverage_plan WHERE id = ?",
            (plan_id,),
        ).fetchone()[0]
        assert status == "superseded"


def test_insert_or_load_brave_search_observation_is_idempotent_by_attempt(
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
            source_policy_fingerprint=_OTHER_HASH,
            retrieval_target=3,
            created_at=moment(),
        )
        form_id = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": "Alex Smith",
                },
            ),
        )[0]
        attempt_id = _attempt(connection, run_id=run_id)

        first = insert_or_load_brave_search_observation_by_attempt(
            conn,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text="Alex Smith",
            altered_query=None,
            offset_in=0,
            count_requested=10,
            result_count=1,
            truncated=False,
            response_complete=True,
            observed_at=moment(),
        )
        second = insert_or_load_brave_search_observation_by_attempt(
            conn,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text="Alex Smith",
            altered_query="ignored on reuse",
            offset_in=0,
            count_requested=10,
            result_count=99,
            truncated=True,
            response_complete=False,
            observed_at=moment(1),
        )
        assert first == second
        count = conn.execute(
            "SELECT COUNT(*) FROM brave_search_observation WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()[0]
        assert count == 1

        article_id = conn.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES ('https://example.com/hit', 'example.com', ?)
            """,
            (moment(),),
        ).lastrowid
        assert article_id is not None
        screening_id = insert_source_screening(
            conn,
            canonical_article_id=article_id,
            url="https://example.com/hit",
            publisher_key="example.com",
            rule_id="rule.a",
            rule_status="curated_eligible",
            source_policy_fingerprint=_OTHER_HASH,
            decided_at=moment(),
            plan_id=plan_id,
        )
        insert_search_result_occurrences(
            conn,
            search_observation_id=first,
            occurrences=(
                {
                    "rank": 1,
                    "url": "https://example.com/hit",
                    "title": "Hit",
                    "snippet": "snip",
                    "extra_snippet": None,
                    "language": "en",
                    "provider_result_id": "p1",
                    "canonical_article_id": article_id,
                    "screening_id": screening_id,
                },
            ),
        )
        # Re-insert same rank is rejected (UNIQUE observation, rank).
        with pytest.raises(sqlite3.IntegrityError):
            insert_search_result_occurrences(
                conn,
                search_observation_id=first,
                occurrences=(
                    {
                        "rank": 1,
                        "url": "https://example.com/hit",
                        "title": "dup",
                    },
                ),
            )


def test_discovery_screening_target_view_assessment_pipeline(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            source_policy_fingerprint=_OTHER_HASH,
            retrieval_target=2,
            created_at=moment(),
        )
        screening_id = insert_source_screening(
            conn,
            canonical_article_id=article_id,
            url="https://example.com/a",
            publisher_key="example.com",
            rule_id="rule.a",
            rule_status="curated_eligible",
            source_policy_fingerprint=_OTHER_HASH,
            decided_at=moment(),
            plan_id=plan_id,
            source_item_id=42,
            person_mention_id=7,
        )
        discovery_id = insert_coverage_discovery_article(
            conn,
            plan_id=plan_id,
            canonical_article_id=article_id,
            source_item_id=42,
            person_mention_id=7,
            screening_id=screening_id,
        )
        assert discovery_id > 0

        person_article_id = upsert_person_article(
            conn,
            person_id=person_id,
            canonical_article_id=article_id,
            first_plan_id=plan_id,
        )
        same_pa = upsert_person_article(
            conn,
            person_id=person_id,
            canonical_article_id=article_id,
            first_plan_id=plan_id,
        )
        assert person_article_id == same_pa

        target_id = insert_coverage_article_target(
            conn,
            plan_id=plan_id,
            canonical_article_id=article_id,
            request_url="https://example.com/a",
            selection_reason="discovery_curated_eligible",
        )
        assert target_id > 0

        view_id = insert_article_view(
            conn,
            canonical_article_id=article_id,
            run_id=run_id,
            attempt_id=None,
            access_kind="snippets",
            requested_url="https://example.com/a",
            final_url=None,
            title="Title",
            dek=None,
            byline=None,
            published_at=None,
            editorial_labels_json="[]",
            main_text_blocks_json="[]",
            snippets_json='["snippet"]',
            extraction_quality=None,
            extractor_version=1,
            observed_at=moment(),
        )
        gen_attempt = _attempt(
            connection,
            run_id=run_id,
            provider="openrouter",
            operation="generate_structured",
            fingerprint=_OTHER_HASH,
        )
        inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
        assessment_id = insert_person_article_assessment(
            conn,
            person_article_id=person_article_id,
            person_id=person_id,
            canonical_article_id=article_id,
            plan_id=plan_id,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=gen_attempt,
            model_inspection_id=inspection_id,
            disposition="completed",
            person_relation="same_person",
            coverage_depth="significant",
            content_types_json='["reporting"]',
            subject_relationship="editorially_independent",
            screening_rule_id="rule.a",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=_OTHER_HASH,
            canonical_supplied_input_json="{}",
            validated_output_json='{"ok":true}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            rationale="enough evidence",
            failure_category=None,
            observed_at=moment(),
        )
        insert_assessment_signals(
            conn,
            assessment_id=assessment_id,
            signals=(
                {
                    "signal_kind": "attention",
                    "category": "awards",
                    "claim": "won a prize",
                    "supporting_passage_ids_json": '["t0"]',
                    "ordinal": 1,
                },
            ),
        )
        point_person_article_current_assessment(
            conn,
            person_article_id=person_article_id,
            assessment_id=assessment_id,
        )
        current = conn.execute(
            "SELECT current_assessment_id FROM person_article WHERE id = ?",
            (person_article_id,),
        ).fetchone()[0]
        assert current == assessment_id


def test_unusable_screening_without_discovery_row(
    connection: sqlite3.Connection,
) -> None:
    """K10: unusable discovery URLs write screening only (no discovery row)."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            source_policy_fingerprint=_OTHER_HASH,
            retrieval_target=1,
            created_at=moment(),
        )
        screening_id = insert_source_screening(
            conn,
            canonical_article_id=None,
            url="",
            publisher_key=None,
            rule_id="unusable",
            rule_status="unusable",
            source_policy_fingerprint=_OTHER_HASH,
            decided_at=moment(),
            plan_id=plan_id,
            source_item_id=99,
            person_mention_id=None,
        )
        assert screening_id > 0
        discovery_count = conn.execute(
            "SELECT COUNT(*) FROM coverage_discovery_article WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()[0]
        assert discovery_count == 0
