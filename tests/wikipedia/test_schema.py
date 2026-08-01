"""Schema tests for migration 0006 Wikipedia identity DDL."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations, load_migrations
from tests.ingestion.helpers import insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_THIRD_HASH = "c" * 64
_PROMPT_HASH = "d" * 64
_SCHEMA_HASH = "e" * 64

_EXPECTED_COLUMNS = {
    "mediawiki_page": {
        "id",
        "wiki_id",
        "page_id",
        "canonical_title",
        "canonical_url",
        "namespace",
        "is_disambiguation",
        "is_missing",
        "redirect_to_page_id",
        "description",
        "extract",
        "categories_json",
        "last_observed_at",
        "last_attempt_id",
    },
    "wikipedia_identity_plan": {
        "id",
        "person_id",
        "run_id",
        "material_fingerprint",
        "status",
        "refresh_of_observation_id",
        "truncated_unsafe_for_negative",
        "partial_retrieval",
        "created_at",
        "completed_at",
        "failure_category",
    },
    "wikipedia_query_form": {
        "id",
        "plan_id",
        "ordinal",
        "variant_kind",
        "query_text",
        "status",
        "continuations_used",
        "hit_count",
        "truncated",
        "failure_category",
    },
    "wikipedia_page_facts_batch": {
        "id",
        "plan_id",
        "ordinal",
        "page_ids_json",
        "status",
        "wave",
        "attempt_id",
        "failure_category",
        "created_at",
        "completed_at",
    },
    "mediawiki_search_observation": {
        "id",
        "query_form_id",
        "run_id",
        "attempt_id",
        "query_text",
        "continuation_in",
        "continuation_out",
        "srlimit",
        "hit_count",
        "truncated",
        "response_complete",
        "observed_at",
    },
    "mediawiki_search_hit": {
        "id",
        "search_observation_id",
        "rank",
        "page_id",
        "title",
    },
    "wikipedia_identity_observation": {
        "id",
        "person_id",
        "plan_id",
        "run_id",
        "attempt_id",
        "model_inspection_id",
        "disposition",
        "semantic_outcome",
        "matched_mediawiki_page_id",
        "candidate_page_ids_json",
        "canonical_supplied_input_json",
        "validated_output_json",
        "prompt_hash",
        "schema_hash",
        "schema_version",
        "task_fingerprint",
        "supporting_fact_ids_json",
        "conflicting_fact_ids_json",
        "rationale",
        "failure_category",
        "observed_at",
    },
}


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
        (run_id, work_item_id, provider, operation, moment(), moment(1), _HASH),
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
    return cursor.lastrowid


def _page(connection: sqlite3.Connection, *, page_id: int = 1001) -> int:
    cursor = connection.execute(
        """
        INSERT INTO mediawiki_page (
            wiki_id, page_id, canonical_title, canonical_url, namespace,
            is_disambiguation, is_missing, categories_json, last_observed_at
        ) VALUES (
            'enwiki', ?, 'Alex Smith', 'https://en.wikipedia.org/wiki/Alex_Smith',
            0, 0, 0, '[]', ?
        )
        """,
        (page_id, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str = _HASH,
    status: str = "retrieving",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO wikipedia_identity_plan (
            person_id, run_id, material_fingerprint, status,
            truncated_unsafe_for_negative, partial_retrieval, created_at
        ) VALUES (?, ?, ?, ?, 0, 0, ?)
        """,
        (person_id, run_id, material_fingerprint, status, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _query_form(connection: sqlite3.Connection, *, plan_id: int) -> int:
    cursor = connection.execute(
        """
        INSERT INTO wikipedia_query_form (
            plan_id, ordinal, variant_kind, query_text, status,
            continuations_used, truncated
        ) VALUES (?, 1, 'exact', 'Alex Smith', 'pending', 0, 0)
        """,
        (plan_id,),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _insert_wiki_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    plan_id: int | None,
    disposition: str,
    semantic_outcome: str | None,
    attempt_id: int | None,
    model_inspection_id: int | None,
    matched_mediawiki_page_id: int | None,
    candidate_page_ids_json: str,
    validated_output_json: str | None,
    task_fingerprint: str = _HASH,
    prompt_hash: str | None = _PROMPT_HASH,
    schema_hash: str | None = _SCHEMA_HASH,
    schema_version: int | None = 1,
    failure_category: str | None = None,
    rationale: str = "reason",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO wikipedia_identity_observation (
            person_id, plan_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, matched_mediawiki_page_id,
            candidate_page_ids_json, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, rationale, failure_category, observed_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, ?, ?, ?, ?, ?
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
            validated_output_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            rationale,
            failure_category,
            moment(),
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def test_migration_0006_applies_to_a_fresh_database(
    connection: sqlite3.Connection,
) -> None:
    versions = tuple(
        row["version"]
        for row in connection.execute(
            "SELECT version FROM schema_migration ORDER BY version"
        )
    )
    assert versions == (1, 2, 3, 4, 5, 6, 7)


@pytest.mark.parametrize("retained_version", [0, 1, 2, 3, 4, 5])
def test_migration_0006_upgrades_every_retained_schema_version(
    tmp_path: Path, retained_version: int
) -> None:
    database = tmp_path / f"v{retained_version}.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        retained = tuple(
            migration
            for migration in load_migrations()
            if migration.version <= retained_version
        )
        if retained:
            apply_migrations(connection, database, backups, retained)
        result = apply_migrations(connection, database, backups)
        assert result.applied_versions == tuple(range(retained_version + 1, 8))
        assert result.backup_path is not None and result.backup_path.exists()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()


def test_migration_0006_preserves_existing_person_rows_from_0005(
    tmp_path: Path,
) -> None:
    database = tmp_path / "v5-with-data.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        retained = tuple(
            migration for migration in load_migrations() if migration.version <= 5
        )
        apply_migrations(connection, database, backups, retained)
        run_id = insert_run(connection)
        person_id = connection.execute(
            """
            INSERT INTO person (
                created_at, created_by_run_id, display_name, identity_fingerprint
            ) VALUES (?, ?, 'Alex Smith', ?)
            """,
            (moment(), run_id, _HASH),
        ).lastrowid
        assert person_id is not None
        connection.commit()

        result = apply_migrations(connection, database, backups)

        assert result.applied_versions == (6, 7)
        row = connection.execute(
            """
            SELECT display_name, current_wikipedia_identity_observation_id
              FROM person WHERE id = ?
            """,
            (person_id,),
        ).fetchone()
        assert tuple(row) == ("Alex Smith", None)
        tables = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "wikipedia_page_facts_batch" in tables
        assert "mediawiki_search_observation" in tables
    finally:
        connection.close()


def test_wikipedia_tables_have_expected_shape(connection: sqlite3.Connection) -> None:
    table_names = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert set(_EXPECTED_COLUMNS) <= table_names
    for table, expected in _EXPECTED_COLUMNS.items():
        actual = {
            row["name"] for row in connection.execute(f"PRAGMA table_info({table})")
        }
        assert actual == expected, f"{table}: {actual ^ expected}"

    person_columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(person)")
    }
    assert "current_wikipedia_identity_observation_id" in person_columns


def test_schema_has_indexes_for_uniques_and_lookups(
    connection: sqlite3.Connection,
) -> None:
    indexes = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
    }
    assert {
        "mediawiki_page_by_wiki_page",
        "wikipedia_identity_plan_active",
        "wikipedia_query_form_by_plan_ordinal",
        "wikipedia_page_facts_batch_by_plan_ordinal",
        "wikipedia_page_facts_batch_by_attempt",
        "mediawiki_search_observation_by_attempt",
        "mediawiki_search_hit_by_observation_rank",
        "wikipedia_identity_observation_by_person_fingerprint",
    } <= indexes


def test_migration_0006_sql_contains_no_pragma_foreign_keys() -> None:
    migration = next(m for m in load_migrations() if m.version == 6)
    assert "PRAGMA foreign_keys" not in migration.sql
    assert "ADD FOREIGN KEY" not in migration.sql.upper()
    assert "ADD CONSTRAINT" not in migration.sql.upper()


def test_search_observation_attempt_id_is_unique(
    connection: sqlite3.Connection,
) -> None:
    """Named rule: UNIQUE(attempt_id) on mediawiki_search_observation."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    form_id = _query_form(connection, plan_id=plan_id)
    attempt_id = _attempt(connection, run_id=run_id)

    connection.execute(
        """
        INSERT INTO mediawiki_search_observation (
            query_form_id, run_id, attempt_id, query_text, srlimit,
            hit_count, truncated, response_complete, observed_at
        ) VALUES (?, ?, ?, 'Alex Smith', 10, 0, 0, 1, ?)
        """,
        (form_id, run_id, attempt_id, moment()),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO mediawiki_search_observation (
                query_form_id, run_id, attempt_id, query_text, srlimit,
                hit_count, truncated, response_complete, observed_at
            ) VALUES (?, ?, ?, 'Alex Smith', 10, 0, 0, 1, ?)
            """,
            (form_id, run_id, attempt_id, moment(1)),
        )


def test_page_facts_batch_attempt_id_is_unique_when_set(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id, operation="get_page_facts")
    connection.execute(
        """
        INSERT INTO wikipedia_page_facts_batch (
            plan_id, ordinal, page_ids_json, status, wave, attempt_id, created_at
        ) VALUES (?, 1, '[1]', 'completed', 1, ?, ?)
        """,
        (plan_id, attempt_id, moment()),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO wikipedia_page_facts_batch (
                plan_id, ordinal, page_ids_json, status, wave, attempt_id, created_at
            ) VALUES (?, 2, '[2]', 'completed', 1, ?, ?)
            """,
            (plan_id, attempt_id, moment()),
        )
    # Multiple NULL attempt_ids remain legal (pending batches).
    connection.execute(
        """
        INSERT INTO wikipedia_page_facts_batch (
            plan_id, ordinal, page_ids_json, status, wave, created_at
        ) VALUES (?, 3, '[3]', 'pending', 1, ?)
        """,
        (plan_id, moment()),
    )
    connection.execute(
        """
        INSERT INTO wikipedia_page_facts_batch (
            plan_id, ordinal, page_ids_json, status, wave, created_at
        ) VALUES (?, 4, '[4]', 'pending', 1, ?)
        """,
        (plan_id, moment()),
    )


def test_active_plan_partial_unique_rejects_second_retrieving_same_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=_HASH,
        status="retrieving",
    )
    with pytest.raises(sqlite3.IntegrityError):
        _plan(
            connection,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            status="ready_for_match",
        )
    # Terminal plans free the active slot.
    completed = _plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=_OTHER_HASH,
        status="completed",
    )
    assert completed > 0
    second_completed = _plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=_OTHER_HASH,
        status="failed",
    )
    assert second_completed > completed


def test_completed_outcome_truth_table_accepts_valid_rows(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    page_id = _page(connection)
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)

    # Deterministic empty complete search → no_matching_page_found, no attempt.
    det_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json='{"outcome":"no_matching_page_found"}',
        task_fingerprint=_HASH,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
    )
    assert det_id > 0

    # Model matching_page_found
    match_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="matching_page_found",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        matched_mediawiki_page_id=page_id,
        candidate_page_ids_json="[1001]",
        validated_output_json='{"outcome":"matching_page"}',
        task_fingerprint=_OTHER_HASH,
    )
    assert match_id > det_id

    # Model no_matching_page_found with non-empty candidates
    no_match_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[1001]",
        validated_output_json='{"outcome":"no_matching_page"}',
        task_fingerprint=_THIRD_HASH,
    )
    assert no_match_id > match_id

    # Model uncertain_identity
    uncertain_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="uncertain_identity",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[1001]",
        validated_output_json='{"outcome":"uncertain"}',
        task_fingerprint="f" * 64,
    )
    assert uncertain_id > no_match_id


@pytest.mark.parametrize(
    (
        "semantic_outcome",
        "attempt",
        "inspection",
        "matched",
        "candidates",
        "validated",
        "prompt",
        "schema",
        "version",
    ),
    [
        # completed with null outcome
        (None, True, True, False, "[1]", "{}", True, True, 1),
        # matching without matched page
        ("matching_page_found", True, True, False, "[1]", "{}", True, True, 1),
        # matching with empty candidates
        ("matching_page_found", True, True, True, "[]", "{}", True, True, 1),
        # model no-match without attempt
        ("no_matching_page_found", False, False, False, "[1]", "{}", True, True, 1),
        # deterministic no-match with non-empty candidates
        (
            "no_matching_page_found",
            False,
            False,
            False,
            "[1]",
            "{}",
            False,
            False,
            None,
        ),
        # uncertain without inspection
        ("uncertain_identity", True, False, False, "[1]", "{}", True, True, 1),
    ],
)
def test_completed_outcome_truth_table_rejects_invalid_rows(
    connection: sqlite3.Connection,
    semantic_outcome: str | None,
    attempt: bool,
    inspection: bool,
    matched: bool,
    candidates: str,
    validated: str | None,
    prompt: bool,
    schema: bool,
    version: int | None,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    page_row = _page(connection) if matched else None
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = (
        _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
        if inspection
        else None
    )
    with pytest.raises(sqlite3.IntegrityError):
        _insert_wiki_observation(
            connection,
            person_id=person_id,
            run_id=run_id,
            plan_id=plan_id,
            disposition="completed",
            semantic_outcome=semantic_outcome,
            attempt_id=gen_attempt if attempt else None,
            model_inspection_id=inspection_id,
            matched_mediawiki_page_id=page_row,
            candidate_page_ids_json=candidates,
            validated_output_json=validated,
            prompt_hash=_PROMPT_HASH if prompt else None,
            schema_hash=_SCHEMA_HASH if schema else None,
            schema_version=version,
        )


def test_completed_rejects_failure_category(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_wiki_observation(
            connection,
            person_id=person_id,
            run_id=run_id,
            plan_id=plan_id,
            disposition="completed",
            semantic_outcome="no_matching_page_found",
            attempt_id=None,
            model_inspection_id=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            validated_output_json="{}",
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            failure_category="unsafe_truncation",
        )


@pytest.mark.parametrize(
    "failure_category",
    [
        "unsafe_truncation",
        "partial_retrieval_empty",
        "redirect_budget_exhausted",
    ],
)
def test_failed_local_domain_categories_allow_null_attempt(
    connection: sqlite3.Connection,
    failure_category: str,
) -> None:
    """Named rule: failed CHECK allows null attempt only for local domain categories."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    obs_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="failed",
        semantic_outcome=None,
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json=None,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        failure_category=failure_category,
        task_fingerprint=_HASH
        if failure_category == "unsafe_truncation"
        else (
            _OTHER_HASH
            if failure_category == "partial_retrieval_empty"
            else _THIRD_HASH
        ),
    )
    assert obs_id > 0


def test_failed_permanent_provider_rejects_null_attempt(
    connection: sqlite3.Connection,
) -> None:
    """Named rule: permanent_provider without attempt fails CHECK."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_wiki_observation(
            connection,
            person_id=person_id,
            run_id=run_id,
            plan_id=plan_id,
            disposition="failed",
            semantic_outcome=None,
            attempt_id=None,
            model_inspection_id=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            failure_category="permanent_provider",
        )


def test_failed_with_attempt_accepts_provider_and_preflight(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)

    provider_fail = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="failed",
        semantic_outcome=None,
        attempt_id=attempt_id,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json=None,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        failure_category="permanent_provider",
        task_fingerprint=_HASH,
    )
    preflight_fail = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="failed",
        semantic_outcome=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json=None,
        failure_category="permanent_preflight",
        task_fingerprint=_OTHER_HASH,
    )
    assert provider_fail > 0
    assert preflight_fail > provider_fail


def test_current_pointer_rejects_failed_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    failed_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="failed",
        semantic_outcome=None,
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json=None,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        failure_category="unsafe_truncation",
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            UPDATE person
               SET current_wikipedia_identity_observation_id = ?
             WHERE id = ?
            """,
            (failed_id, person_id),
        )


def test_current_pointer_rejects_wrong_person(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_a = _person(connection, run_id=run_id)
    person_b = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, 'Blake Jones', ?)
        """,
        (moment(), run_id, _OTHER_HASH),
    ).lastrowid
    assert person_b is not None
    plan_id = _plan(connection, person_id=person_a, run_id=run_id)
    obs_id = _insert_wiki_observation(
        connection,
        person_id=person_a,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json="{}",
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            UPDATE person
               SET current_wikipedia_identity_observation_id = ?
             WHERE id = ?
            """,
            (obs_id, person_b),
        )


def test_current_pointer_accepts_completed_for_same_person(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    obs_id = _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json="{}",
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
    )
    connection.execute(
        """
        UPDATE person
           SET current_wikipedia_identity_observation_id = ?
         WHERE id = ?
        """,
        (obs_id, person_id),
    )
    pointed = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()[0]
    assert pointed == obs_id


def test_mediawiki_page_unique_wiki_page_id(
    connection: sqlite3.Connection,
) -> None:
    _page(connection, page_id=42)
    with pytest.raises(sqlite3.IntegrityError):
        _page(connection, page_id=42)


def test_observation_person_task_fingerprint_unique(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    _insert_wiki_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        plan_id=plan_id,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        attempt_id=None,
        model_inspection_id=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        validated_output_json="{}",
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        task_fingerprint=_HASH,
    )
    with pytest.raises(sqlite3.IntegrityError):
        _insert_wiki_observation(
            connection,
            person_id=person_id,
            run_id=run_id,
            plan_id=plan_id,
            disposition="failed",
            semantic_outcome=None,
            attempt_id=None,
            model_inspection_id=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            failure_category="unsafe_truncation",
            task_fingerprint=_HASH,
        )
