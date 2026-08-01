"""Schema tests for migration 0007 coverage evidence DDL."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations, load_migrations
from tests.ingestion.helpers import insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_PROMPT_HASH = "d" * 64
_SCHEMA_HASH = "e" * 64

_EXPECTED_COLUMNS = {
    "person_coverage_plan": {
        "id",
        "person_id",
        "run_id",
        "material_fingerprint",
        "status",
        "refresh_of_plan_id",
        "source_policy_fingerprint",
        "truncated_unsafe",
        "partial_retrieval",
        "retrieval_target",
        "eligible_selected_count",
        "created_at",
        "completed_at",
        "failure_category",
    },
    "coverage_query_form": {
        "id",
        "plan_id",
        "ordinal",
        "stage",
        "variant_kind",
        "query_text",
        "status",
        "offsets_used",
        "result_count",
        "truncated",
        "failure_category",
    },
    "brave_search_observation": {
        "id",
        "query_form_id",
        "run_id",
        "attempt_id",
        "query_text",
        "altered_query",
        "offset_in",
        "count_requested",
        "result_count",
        "truncated",
        "response_complete",
        "observed_at",
    },
    "brave_search_result_occurrence": {
        "id",
        "search_observation_id",
        "rank",
        "url",
        "title",
        "snippet",
        "extra_snippet",
        "language",
        "provider_result_id",
        "canonical_article_id",
        "screening_id",
    },
    "source_screening": {
        "id",
        "canonical_article_id",
        "url",
        "publisher_key",
        "rule_id",
        "rule_status",
        "source_policy_fingerprint",
        "decided_at",
        "plan_id",
        "source_item_id",
        "person_mention_id",
    },
    "coverage_discovery_article": {
        "id",
        "plan_id",
        "canonical_article_id",
        "source_item_id",
        "person_mention_id",
        "screening_id",
    },
    "coverage_article_target": {
        "id",
        "plan_id",
        "canonical_article_id",
        "request_url",
        "selection_reason",
        "status",
        "article_view_id",
        "attempt_id",
        "failure_category",
    },
    "article_view": {
        "id",
        "canonical_article_id",
        "run_id",
        "attempt_id",
        "access_kind",
        "requested_url",
        "final_url",
        "title",
        "dek",
        "byline",
        "published_at",
        "editorial_labels_json",
        "main_text_blocks_json",
        "snippets_json",
        "extraction_quality",
        "extractor_version",
        "observed_at",
    },
    "person_article": {
        "id",
        "person_id",
        "canonical_article_id",
        "first_plan_id",
        "current_assessment_id",
    },
    "person_article_assessment": {
        "id",
        "person_article_id",
        "person_id",
        "canonical_article_id",
        "plan_id",
        "article_view_id",
        "run_id",
        "attempt_id",
        "model_inspection_id",
        "disposition",
        "person_relation",
        "coverage_depth",
        "content_types_json",
        "subject_relationship",
        "screening_rule_id",
        "screening_rule_status",
        "source_policy_fingerprint",
        "canonical_supplied_input_json",
        "validated_output_json",
        "prompt_hash",
        "schema_hash",
        "schema_version",
        "task_fingerprint",
        "rationale",
        "failure_category",
        "observed_at",
    },
    "article_assessment_signal": {
        "id",
        "assessment_id",
        "signal_kind",
        "category",
        "claim",
        "supporting_passage_ids_json",
        "ordinal",
    },
}


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
    return cursor.lastrowid


def _canonical_article(
    connection: sqlite3.Connection,
    *,
    url: str = "https://example.com/a",
    publisher_key: str = "example.com",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
        VALUES (?, ?, ?)
        """,
        (url, publisher_key, moment()),
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
    source_policy_fingerprint: str = _OTHER_HASH,
    retrieval_target: int = 5,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person_coverage_plan (
            person_id, run_id, material_fingerprint, status,
            source_policy_fingerprint, truncated_unsafe, partial_retrieval,
            retrieval_target, eligible_selected_count, created_at
        ) VALUES (?, ?, ?, ?, ?, 0, 0, ?, 0, ?)
        """,
        (
            person_id,
            run_id,
            material_fingerprint,
            status,
            source_policy_fingerprint,
            retrieval_target,
            moment(),
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _query_form(connection: sqlite3.Connection, *, plan_id: int) -> int:
    cursor = connection.execute(
        """
        INSERT INTO coverage_query_form (
            plan_id, ordinal, stage, variant_kind, query_text, status,
            offsets_used, truncated
        ) VALUES (?, 1, 1, 'exact', 'Alex Smith', 'pending', 0, 0)
        """,
        (plan_id,),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _screening(
    connection: sqlite3.Connection,
    *,
    plan_id: int | None,
    rule_status: str = "curated_eligible",
    canonical_article_id: int | None = None,
    url: str = "https://example.com/a",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO source_screening (
            canonical_article_id, url, publisher_key, rule_id, rule_status,
            source_policy_fingerprint, decided_at, plan_id
        ) VALUES (?, ?, 'example.com', 'rule.a', ?, ?, ?, ?)
        """,
        (
            canonical_article_id,
            url,
            rule_status,
            _OTHER_HASH,
            moment(),
            plan_id,
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _article_view(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    canonical_article_id: int,
    attempt_id: int | None = None,
    access_kind: str = "full",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO article_view (
            canonical_article_id, run_id, attempt_id, access_kind,
            requested_url, final_url, title, dek, byline, published_at,
            editorial_labels_json, main_text_blocks_json, snippets_json,
            extraction_quality, extractor_version, observed_at
        ) VALUES (
            ?, ?, ?, ?, 'https://example.com/a', 'https://example.com/a',
            'Title', NULL, NULL, NULL, '[]', '[{"id":"b1","text":"body"}]',
            '[]', 'full', 1, ?
        )
        """,
        (canonical_article_id, run_id, attempt_id, access_kind, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _person_article(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    canonical_article_id: int,
    first_plan_id: int | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person_article (
            person_id, canonical_article_id, first_plan_id
        ) VALUES (?, ?, ?)
        """,
        (person_id, canonical_article_id, first_plan_id),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _insert_assessment(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    person_id: int,
    canonical_article_id: int,
    article_view_id: int,
    run_id: int,
    disposition: str,
    attempt_id: int | None,
    model_inspection_id: int | None,
    plan_id: int | None = None,
    person_relation: str | None = None,
    coverage_depth: str | None = None,
    content_types_json: str | None = None,
    subject_relationship: str | None = None,
    validated_output_json: str | None = None,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
    task_fingerprint: str = _HASH,
    failure_category: str | None = None,
    rationale: str = "reason",
) -> int:
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
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'rule.a', 'curated_eligible',
            ?, '{}', ?, ?, ?, ?, ?, ?, ?, ?
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
            _OTHER_HASH,
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


def test_migration_0007_applies_to_a_fresh_database(
    connection: sqlite3.Connection,
) -> None:
    versions = tuple(
        row["version"]
        for row in connection.execute(
            "SELECT version FROM schema_migration ORDER BY version"
        )
    )
    assert versions == (1, 2, 3, 4, 5, 6, 7, 8)


@pytest.mark.parametrize("retained_version", [0, 1, 2, 3, 4, 5, 6])
def test_migration_0007_upgrades_every_retained_schema_version(
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
        assert result.applied_versions == tuple(range(retained_version + 1, 9))
        assert result.backup_path is not None and result.backup_path.exists()
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        connection.close()


def test_migration_0007_preserves_existing_aliases_and_adds_search_result(
    tmp_path: Path,
) -> None:
    database = tmp_path / "v6-with-aliases.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        retained = tuple(
            migration for migration in load_migrations() if migration.version <= 6
        )
        apply_migrations(connection, database, backups, retained)
        article_id = connection.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES ('https://example.com/old', 'example.com', ?)
            """,
            (moment(),),
        ).lastrowid
        assert article_id is not None
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            ) VALUES (?, 'https://example.com/old', 'feed_original', ?)
            """,
            (article_id, moment()),
        )
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            ) VALUES (?, 'https://example.com/redir', 'redirect_destination', ?)
            """,
            (article_id, moment(1)),
        )
        connection.commit()

        result = apply_migrations(connection, database, backups)
        assert result.applied_versions == (7, 8)

        kinds = {
            row["kind"]
            for row in connection.execute("SELECT kind FROM article_url_alias")
        }
        assert kinds == {"feed_original", "redirect_destination"}

        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            ) VALUES (?, 'https://example.com/search', 'search_result', ?)
            """,
            (article_id, moment(2)),
        )
        assert (
            connection.execute(
                "SELECT kind FROM article_url_alias WHERE url = ?",
                ("https://example.com/search",),
            ).fetchone()[0]
            == "search_result"
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO article_url_alias (
                    canonical_article_id, url, kind, first_seen_at
                ) VALUES (?, 'https://example.com/bad', 'discovery', ?)
                """,
                (article_id, moment(3)),
            )
    finally:
        connection.close()


def test_coverage_tables_have_expected_shape(connection: sqlite3.Connection) -> None:
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


def test_article_view_has_no_html_column(connection: sqlite3.Connection) -> None:
    columns = {
        row["name"].lower()
        for row in connection.execute("PRAGMA table_info(article_view)")
    }
    forbidden = {name for name in columns if "html" in name or name in {"body", "raw"}}
    assert forbidden == set()


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
        "person_coverage_plan_active",
        "coverage_query_form_by_plan_ordinal",
        "brave_search_observation_by_attempt",
        "brave_search_result_occurrence_by_observation_rank",
        "coverage_discovery_article_by_plan_article",
        "coverage_article_target_by_plan_article",
        "article_view_by_attempt",
        "person_article_by_person_article",
        "person_article_assessment_by_relation_fingerprint",
    } <= indexes


def test_migration_0007_sql_contains_no_pragma_foreign_keys() -> None:
    migration = next(m for m in load_migrations() if m.version == 7)
    assert "PRAGMA foreign_keys" not in migration.sql
    assert "ADD FOREIGN KEY" not in migration.sql.upper()
    assert "ADD CONSTRAINT" not in migration.sql.upper()
    # Column list must not declare an HTML/body field (comments may say "no HTML").
    create_block = migration.sql.lower().split("create table article_view")[1]
    create_block = create_block.split(");")[0]
    for forbidden in (" html ", "\thtml ", " raw_html", " body_html", " html_"):
        assert forbidden not in create_block


def test_active_plan_partial_unique_rejects_second_active_same_fingerprint(
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
            status="selecting",
        )
    # Terminal statuses free the active slot (multiple completed allowed).
    completed = _plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=_OTHER_HASH,
        status="completed",
    )
    second = _plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=_OTHER_HASH,
        status="failed",
    )
    assert second > completed


def test_brave_search_observation_attempt_id_is_unique(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    form_id = _query_form(connection, plan_id=plan_id)
    attempt_id = _attempt(connection, run_id=run_id)

    connection.execute(
        """
        INSERT INTO brave_search_observation (
            query_form_id, run_id, attempt_id, query_text, offset_in,
            count_requested, result_count, truncated, response_complete, observed_at
        ) VALUES (?, ?, ?, 'Alex Smith', 0, 10, 0, 0, 1, ?)
        """,
        (form_id, run_id, attempt_id, moment()),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO brave_search_observation (
                query_form_id, run_id, attempt_id, query_text, offset_in,
                count_requested, result_count, truncated, response_complete,
                observed_at
            ) VALUES (?, ?, ?, 'Alex Smith', 0, 10, 0, 0, 1, ?)
            """,
            (form_id, run_id, attempt_id, moment(1)),
        )


def test_article_view_attempt_id_unique_when_set_allows_multiple_null(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    article_id = _canonical_article(connection)
    attempt_id = _attempt(
        connection, run_id=run_id, provider="article_http", operation="fetch_article"
    )
    _article_view(
        connection,
        run_id=run_id,
        canonical_article_id=article_id,
        attempt_id=attempt_id,
    )
    with pytest.raises(sqlite3.IntegrityError):
        _article_view(
            connection,
            run_id=run_id,
            canonical_article_id=article_id,
            attempt_id=attempt_id,
        )
    # Multiple snippets-only views with NULL attempt_id are legal.
    first_null = _article_view(
        connection,
        run_id=run_id,
        canonical_article_id=article_id,
        attempt_id=None,
        access_kind="snippets",
    )
    second_null = _article_view(
        connection,
        run_id=run_id,
        canonical_article_id=article_id,
        attempt_id=None,
        access_kind="snippets",
    )
    assert second_null > first_null


def test_discovery_article_requires_screening_id_not_null(
    connection: sqlite3.Connection,
) -> None:
    """K10: screening_id NOT NULL — must not be masked by UNIQUE(plan, article)."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    article_id = _canonical_article(connection, url="https://example.com/a")
    other_article_id = _canonical_article(
        connection, url="https://example.com/other-discovery"
    )
    screening_id = _screening(
        connection,
        plan_id=plan_id,
        canonical_article_id=article_id,
        rule_status="curated_eligible",
    )
    # Positive control: with screening_id the row is accepted.
    connection.execute(
        """
        INSERT INTO coverage_discovery_article (
            plan_id, canonical_article_id, source_item_id, screening_id
        ) VALUES (?, ?, 1, ?)
        """,
        (plan_id, article_id, screening_id),
    )
    # Named rule: screening_id NOT NULL (K10). Use a *different* canonical
    # article so UNIQUE (plan_id, canonical_article_id) cannot produce the error.
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO coverage_discovery_article (
                plan_id, canonical_article_id, source_item_id, screening_id
            ) VALUES (?, ?, 2, NULL)
            """,
            (plan_id, other_article_id),
        )


def test_source_screening_null_canonical_only_when_unusable(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    article_id = _canonical_article(connection)

    # Unusable may (must) have null canonical_article_id.
    unusable_id = _screening(
        connection,
        plan_id=plan_id,
        rule_status="unusable",
        canonical_article_id=None,
        url="not-a-url",
    )
    assert unusable_id > 0

    with pytest.raises(sqlite3.IntegrityError):
        _screening(
            connection,
            plan_id=plan_id,
            rule_status="curated_eligible",
            canonical_article_id=None,
            url="https://example.com/missing-article",
        )
    with pytest.raises(sqlite3.IntegrityError):
        _screening(
            connection,
            plan_id=plan_id,
            rule_status="unusable",
            canonical_article_id=article_id,
            url="https://example.com/should-be-null",
        )


def test_selection_reason_four_value_enum_rejects_bare_discovery(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    article_id = _canonical_article(connection)

    for reason in (
        "discovery_curated_eligible",
        "discovery_unclassified_fallback",
        "search_curated_eligible",
        "search_unclassified_fallback",
    ):
        connection.execute(
            """
            INSERT INTO coverage_article_target (
                plan_id, canonical_article_id, request_url, selection_reason, status
            ) VALUES (?, ?, 'https://example.com/a', ?, 'pending')
            """,
            (plan_id, article_id, reason),
        )
        # Free the unique (plan_id, canonical_article_id) for the next reason.
        connection.execute("DELETE FROM coverage_article_target")

    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO coverage_article_target (
                plan_id, canonical_article_id, request_url, selection_reason, status
            ) VALUES (?, ?, 'https://example.com/a', 'discovery', 'pending')
            """,
            (plan_id, article_id),
        )


def test_assessment_completed_truth_table_accepts_valid_row(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection,
        person_id=person_id,
        canonical_article_id=article_id,
        first_plan_id=plan_id,
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
        fingerprint=_OTHER_HASH,
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection,
        run_id=run_id,
        canonical_article_id=article_id,
        attempt_id=None,
        access_kind="snippets",
    )
    assessment_id = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        plan_id=plan_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="completed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        person_relation="same_person",
        coverage_depth="significant",
        content_types_json='["reporting"]',
        subject_relationship="editorially_independent",
        validated_output_json='{"ok":true}',
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
    )
    assert assessment_id > 0


@pytest.mark.parametrize(
    "override",
    [
        {"person_relation": None},
        {"coverage_depth": None},
        {"subject_relationship": None},
        {"content_types_json": None},
        {"validated_output_json": None},
        {"attempt_id": None},
        {"model_inspection_id": None},
        {"prompt_hash": None},
        {"schema_hash": None},
        {"schema_version": None},
        {"failure_category": "permanent_provider"},
    ],
    ids=[
        "null_person_relation",
        "null_coverage_depth",
        "null_subject_relationship",
        "null_content_types_json",
        "null_validated_output_json",
        "null_attempt_id",
        "null_model_inspection_id",
        "null_prompt_hash",
        "null_schema_hash",
        "null_schema_version",
        "failure_category_set",
    ],
)
def test_assessment_completed_truth_table_rejects_illegal_fields(
    connection: sqlite3.Connection,
    override: dict[str, object],
) -> None:
    """Each completed CHECK clause is independently rejected (one field wrong)."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection, person_id=person_id, canonical_article_id=article_id
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection, run_id=run_id, canonical_article_id=article_id, attempt_id=None
    )
    kwargs: dict[str, object] = {
        "person_article_id": pa_id,
        "person_id": person_id,
        "canonical_article_id": article_id,
        "article_view_id": view_id,
        "run_id": run_id,
        "disposition": "completed",
        "attempt_id": gen_attempt,
        "model_inspection_id": inspection_id,
        "person_relation": "same_person",
        "coverage_depth": "significant",
        "content_types_json": '["reporting"]',
        "subject_relationship": "editorially_independent",
        "validated_output_json": '{"ok":true}',
        "prompt_hash": _PROMPT_HASH,
        "schema_hash": _SCHEMA_HASH,
        "schema_version": 1,
        "failure_category": None,
    }
    kwargs.update(override)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_assessment(connection, **kwargs)  # type: ignore[arg-type]


def test_assessment_failed_truth_table_paths(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection, person_id=person_id, canonical_article_id=article_id
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
        fingerprint=_OTHER_HASH,
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection, run_id=run_id, canonical_article_id=article_id, attempt_id=None
    )

    # Permanent provider failure with attempt.
    failed_provider = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="failed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        failure_category="permanent_provider",
        task_fingerprint=_HASH,
    )
    assert failed_provider > 0

    # Local refuse / superseded may have null attempt.
    failed_local = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="failed",
        attempt_id=None,
        model_inspection_id=None,
        failure_category="local_refuse",
        task_fingerprint=_OTHER_HASH,
    )
    assert failed_local > failed_provider

    # Failed without attempt for non-local category is illegal.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_assessment(
            connection,
            person_article_id=pa_id,
            person_id=person_id,
            canonical_article_id=article_id,
            article_view_id=view_id,
            run_id=run_id,
            disposition="failed",
            attempt_id=None,
            model_inspection_id=None,
            failure_category="permanent_provider",
            task_fingerprint="c" * 64,
        )

    # Failed may not carry completed semantic fields.
    with pytest.raises(sqlite3.IntegrityError):
        _insert_assessment(
            connection,
            person_article_id=pa_id,
            person_id=person_id,
            canonical_article_id=article_id,
            article_view_id=view_id,
            run_id=run_id,
            disposition="failed",
            attempt_id=gen_attempt,
            model_inspection_id=inspection_id,
            person_relation="same_person",
            failure_category="permanent_provider",
            task_fingerprint="f" * 64,
        )


def test_person_article_current_assessment_fk_rejects_delete_of_pointed_assessment(
    connection: sqlite3.Connection,
) -> None:
    """SQL FK on current_assessment_id blocks DELETE of the pointed assessment."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection, person_id=person_id, canonical_article_id=article_id
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection, run_id=run_id, canonical_article_id=article_id, attempt_id=None
    )
    completed_id = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="completed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        person_relation="same_person",
        coverage_depth="passing",
        content_types_json='["profile"]',
        subject_relationship="uncertain",
        validated_output_json='{"ok":true}',
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
        task_fingerprint=_HASH,
    )
    connection.execute(
        "UPDATE person_article SET current_assessment_id = ? WHERE id = ?",
        (completed_id, pa_id),
    )
    # foreign_keys is ON via connect_database; pointed assessment cannot be deleted.
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "DELETE FROM person_article_assessment WHERE id = ?",
            (completed_id,),
        )


def test_person_article_current_assessment_must_be_completed_owner(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection, person_id=person_id, canonical_article_id=article_id
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection, run_id=run_id, canonical_article_id=article_id, attempt_id=None
    )
    completed_id = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="completed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        person_relation="same_person",
        coverage_depth="passing",
        content_types_json='["profile"]',
        subject_relationship="uncertain",
        validated_output_json='{"ok":true}',
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
        task_fingerprint=_HASH,
    )
    failed_id = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="failed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        failure_category="invalid_model_output",
        task_fingerprint=_OTHER_HASH,
    )

    connection.execute(
        "UPDATE person_article SET current_assessment_id = ? WHERE id = ?",
        (completed_id, pa_id),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE person_article SET current_assessment_id = ? WHERE id = ?",
            (failed_id, pa_id),
        )


def test_query_form_variant_and_status_checks(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _plan(connection, person_id=person_id, run_id=run_id)
    for ordinal, variant in enumerate(
        ("exact", "exact_obituary", "alias", "context"), start=1
    ):
        connection.execute(
            """
            INSERT INTO coverage_query_form (
                plan_id, ordinal, stage, variant_kind, query_text, status,
                offsets_used, truncated
            ) VALUES (?, ?, 1, ?, 'q', 'pending', 0, 0)
            """,
            (plan_id, ordinal, variant),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO coverage_query_form (
                plan_id, ordinal, stage, variant_kind, query_text, status,
                offsets_used, truncated
            ) VALUES (?, 99, 1, 'news', 'q', 'pending', 0, 0)
            """,
            (plan_id,),
        )


def test_article_assessment_signal_kind_and_nonempty_category(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    article_id = _canonical_article(connection)
    pa_id = _person_article(
        connection, person_id=person_id, canonical_article_id=article_id
    )
    gen_attempt = _attempt(
        connection,
        run_id=run_id,
        provider="openrouter",
        operation="generate_structured",
    )
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=gen_attempt)
    view_id = _article_view(
        connection, run_id=run_id, canonical_article_id=article_id, attempt_id=None
    )
    assessment_id = _insert_assessment(
        connection,
        person_article_id=pa_id,
        person_id=person_id,
        canonical_article_id=article_id,
        article_view_id=view_id,
        run_id=run_id,
        disposition="completed",
        attempt_id=gen_attempt,
        model_inspection_id=inspection_id,
        person_relation="same_person",
        coverage_depth="significant",
        content_types_json='["reporting"]',
        subject_relationship="editorially_independent",
        validated_output_json='{"ok":true}',
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
    )
    connection.execute(
        """
        INSERT INTO article_assessment_signal (
            assessment_id, signal_kind, category, claim,
            supporting_passage_ids_json, ordinal
        ) VALUES (?, 'attention', 'awards', 'won a prize', '["b1"]', 1)
        """,
        (assessment_id,),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_assessment_signal (
                assessment_id, signal_kind, category, claim,
                supporting_passage_ids_json, ordinal
            ) VALUES (?, 'attention', '', 'empty category', '["b1"]', 2)
            """,
            (assessment_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_assessment_signal (
                assessment_id, signal_kind, category, claim,
                supporting_passage_ids_json, ordinal
            ) VALUES (?, 'warning', 'x', 'bad kind', '["b1"]', 3)
            """,
            (assessment_id,),
        )
