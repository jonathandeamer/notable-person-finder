from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations, load_migrations
from tests.ingestion.helpers import insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64

_EXPECTED_COLUMNS = {
    "model_inspection": {
        "id",
        "run_id",
        "attempt_id",
        "configured_model_id",
        "resolved_model_id",
        "routing_fingerprint",
        "supported_parameters_json",
        "supports_strict_structured_output",
        "pricing_usable",
        "prompt_unit_price_nano_usd",
        "completion_unit_price_nano_usd",
        "compatibility",
        "inspected_at",
    },
    "triage_observation": {
        "id",
        "source_item_id",
        "run_id",
        "attempt_id",
        "model_inspection_id",
        "disposition",
        "semantic_outcome",
        "canonical_supplied_input_json",
        "validated_output_json",
        "prompt_hash",
        "schema_hash",
        "schema_version",
        "task_fingerprint",
        "input_truncated",
        "overflow",
        "rationale",
        "failure_category",
        "observed_at",
    },
    "person_mention": {
        "id",
        "triage_observation_id",
        "ordinal",
        "exact_name",
        "search_name",
        "outcome",
        "supporting_passage_ids_json",
        "rationale",
        "person_id",
        "current_entity_resolution_observation_id",
    },
    "mention_identity_fact": {
        "id",
        "person_mention_id",
        "local_id",
        "kind",
        "value",
        "supporting_passage_ids_json",
    },
    "mention_signal": {
        "id",
        "person_mention_id",
        "ordinal",
        "kind",
        "category",
        "claim",
        "supporting_passage_ids_json",
        "grounding",
    },
    "person": {
        "id",
        "created_at",
        "created_by_run_id",
        "display_name",
        "identity_fingerprint",
        "merged_into_person_id",
        "current_wikipedia_identity_observation_id",
    },
    "sourced_name": {
        "id",
        "person_id",
        "exact_name",
        "search_name",
        "match_key",
        "kind",
        "origin_kind",
        "origin_mention_id",
        "first_observed_at",
        "last_observed_at",
    },
    "person_relation": {
        "id",
        "kind",
        "person_id_a",
        "person_id_b",
        "status",
        "created_at",
        "created_by_run_id",
        "created_by_observation_id",
        "closed_at",
        "closed_by_observation_id",
    },
    "entity_resolution_observation": {
        "id",
        "person_mention_id",
        "person_relation_id",
        "run_id",
        "attempt_id",
        "model_inspection_id",
        "disposition",
        "semantic_outcome",
        "selected_person_id",
        "created_person_id",
        "candidate_person_ids_json",
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
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (_HASH, moment(), run_id, moment(), moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _attempt(connection: sqlite3.Connection, *, run_id: int) -> int:
    work_item_id = _work_item(connection, run_id=run_id)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_item_id, moment(), moment(1), _HASH),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _source_item(connection: sqlite3.Connection, *, run_id: int) -> int:
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES ('feed-a', 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (moment(), moment()),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, moment()),
    ).lastrowid
    assert fetch is not None
    item = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, 'entry-a', 'Alex Smith wins award', ?)
        """,
        (feed, fetch, run_id, moment()),
    ).lastrowid
    assert item is not None
    return item


def _additional_source_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    source_entry_id: str,
) -> int:
    provenance = connection.execute(
        "SELECT feed_identity_id, discovered_by_fetch_id FROM source_item ORDER BY id"
    ).fetchone()
    item = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'A different article', ?)
        """,
        (
            provenance["feed_identity_id"],
            provenance["discovered_by_fetch_id"],
            run_id,
            source_entry_id,
            moment(),
        ),
    ).lastrowid
    assert item is not None
    return item


def _inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    routing_fingerprint: str = _HASH,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test-2026', ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, routing_fingerprint, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _observation(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int,
    task_fingerprint: str = _HASH,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded result', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            model_inspection_id,
            _HASH,
            _OTHER_HASH,
            task_fingerprint,
            moment(),
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _replace_observation(
    connection: sqlite3.Connection,
    *,
    observation_id: int,
    source_item_id: int,
    task_fingerprint: str,
) -> None:
    connection.execute(
        """
        INSERT OR REPLACE INTO triage_observation (
            id, source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale,
            failure_category, observed_at
        )
        SELECT id, ?, run_id, attempt_id, model_inspection_id,
               disposition, semantic_outcome, canonical_supplied_input_json,
               validated_output_json, prompt_hash, schema_hash, schema_version,
               ?, input_truncated, overflow, rationale, failure_category,
               observed_at
        FROM triage_observation
        WHERE id = ?
        """,
        (source_item_id, task_fingerprint, observation_id),
    )


def _valid_graph(connection: sqlite3.Connection) -> tuple[int, int, int, int]:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    source_item_id = _source_item(connection, run_id=run_id)
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)
    observation_id = _observation(
        connection,
        source_item_id=source_item_id,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
    )
    return run_id, attempt_id, source_item_id, observation_id


def test_migration_0005_applies_to_a_fresh_database(
    connection: sqlite3.Connection,
) -> None:
    versions = tuple(
        row["version"]
        for row in connection.execute(
            "SELECT version FROM schema_migration ORDER BY version"
        )
    )
    assert versions == (1, 2, 3, 4, 5, 6, 7)


@pytest.mark.parametrize("retained_version", [0, 1, 2, 3, 4])
def test_migration_0005_upgrades_every_retained_schema_version(
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


def test_migration_0005_preserves_existing_source_items_and_mentions_from_0004(
    tmp_path: Path,
) -> None:
    database = tmp_path / "v4-with-data.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        retained = tuple(
            migration for migration in load_migrations() if migration.version <= 4
        )
        apply_migrations(connection, database, backups, retained)
        run_id, _attempt_id, source_item_id, observation_id = _valid_graph(connection)
        mention_id = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name,
                outcome, supporting_passage_ids_json, rationale
            ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
            """,
            (observation_id,),
        ).lastrowid
        assert mention_id is not None
        connection.commit()

        result = apply_migrations(connection, database, backups)

        assert result.applied_versions == (5, 6, 7)
        row = connection.execute(
            """
            SELECT title_text, current_triage_observation_id
            FROM source_item
            WHERE id = ?
            """,
            (source_item_id,),
        ).fetchone()
        assert tuple(row) == ("Alex Smith wins award", None)
        mention = connection.execute(
            """
            SELECT exact_name, person_id, current_entity_resolution_observation_id
              FROM person_mention WHERE id = ?
            """,
            (mention_id,),
        ).fetchone()
        assert tuple(mention) == ("Alex Smith", None, None)
        assert run_id > 0
    finally:
        connection.close()


def test_people_detection_and_identity_tables_have_expected_shape(
    connection: sqlite3.Connection,
) -> None:
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
        assert actual == expected

    source_columns = {
        row["name"] for row in connection.execute("PRAGMA table_info(source_item)")
    }
    assert "current_triage_observation_id" in source_columns
    assert "person_id" not in source_columns
    assert "person_id" in _EXPECTED_COLUMNS["person_mention"]
    assert "person" in table_names


def test_schema_has_indexes_for_provenance_lookup_and_current_status(
    connection: sqlite3.Connection,
) -> None:
    indexes = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index'"
        )
    }
    assert {
        "model_inspection_by_run_model_routing",
        "model_inspection_by_attempt",
        "triage_observation_by_source_item",
        "triage_observation_by_run",
        "triage_observation_by_attempt",
        "triage_observation_by_status",
        "source_item_by_current_triage",
        "person_by_canonical",
        "person_by_merged_into",
        "sourced_name_by_match_key",
        "sourced_name_by_person",
        "sourced_name_by_exact",
        "person_relation_active_possible",
        "entity_resolution_mention_material",
        "entity_resolution_relation_material",
        "person_mention_by_person",
    } <= indexes


def test_migration_0005_sql_contains_no_pragma_foreign_keys() -> None:
    migration = next(m for m in load_migrations() if m.version == 5)
    assert "PRAGMA foreign_keys" not in migration.sql
    assert "ADD FOREIGN KEY" not in migration.sql.upper()
    assert "ADD CONSTRAINT" not in migration.sql.upper()


@pytest.mark.parametrize("compatibility", ["unknown", "failed", "COMPATIBLE"])
def test_model_inspection_rejects_unknown_compatibility(
    connection: sqlite3.Connection, compatibility: str
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO model_inspection (
                run_id, attempt_id, configured_model_id, resolved_model_id,
                routing_fingerprint, supported_parameters_json,
                supports_strict_structured_output, pricing_usable,
                compatibility, inspected_at
            ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?, '[]',
                      1, 0, ?, ?)
            """,
            (run_id, attempt_id, _HASH, compatibility, moment()),
        )


@pytest.mark.parametrize(
    ("structured", "pricing_usable", "prompt_price", "completion_price"),
    [
        (2, 0, None, None),
        (1, 2, 1, 1),
        (1, 1, None, 1),
        (1, 1, 1, None),
        (1, 0, 1, None),
        (1, 0, None, 1),
        (1, 1, -1, 1),
        (1, 1, 1, -1),
    ],
)
def test_model_inspection_rejects_invalid_boolean_or_pricing_coupling(
    connection: sqlite3.Connection,
    structured: int,
    pricing_usable: int,
    prompt_price: int | None,
    completion_price: int | None,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO model_inspection (
                run_id, attempt_id, configured_model_id, resolved_model_id,
                routing_fingerprint, supported_parameters_json,
                supports_strict_structured_output, pricing_usable,
                prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
                compatibility, inspected_at
            ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?, '[]',
                      ?, ?, ?, ?, 'compatible', ?)
            """,
            (
                run_id,
                attempt_id,
                _HASH,
                structured,
                pricing_usable,
                prompt_price,
                completion_price,
                moment(),
            ),
        )


def test_model_inspection_requires_attempt_from_the_same_run(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    second_run = insert_run(connection)
    attempt_id = _attempt(connection, run_id=first_run)
    with pytest.raises(sqlite3.IntegrityError):
        _inspection(connection, run_id=second_run, attempt_id=attempt_id)


def test_model_inspection_identity_is_unique_within_run_and_reusable_across_runs(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    first_attempt = _attempt(connection, run_id=first_run)
    _inspection(connection, run_id=first_run, attempt_id=first_attempt)
    with pytest.raises(sqlite3.IntegrityError):
        _inspection(connection, run_id=first_run, attempt_id=first_attempt)

    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_run = insert_run(connection)
    second_attempt = _attempt(connection, run_id=second_run)
    _inspection(connection, run_id=second_run, attempt_id=second_attempt)


def test_incompatible_inspection_accepts_unavailable_pricing(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable, compatibility,
            inspected_at
        ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?, '[]', 0, 0,
                  'incompatible', ?)
        """,
        (run_id, attempt_id, _HASH, moment()),
    )


def test_compatible_inspection_requires_strict_structured_output(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO model_inspection (
                run_id, attempt_id, configured_model_id, resolved_model_id,
                routing_fingerprint, supported_parameters_json,
                supports_strict_structured_output, pricing_usable, compatibility,
                inspected_at
            ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?, '[]', 0, 0,
                      'compatible', ?)
            """,
            (run_id, attempt_id, _HASH, moment()),
        )


@pytest.mark.parametrize(
    (
        "disposition",
        "semantic_outcome",
        "with_attempt",
        "validated_output_json",
        "overflow",
        "failure_category",
    ),
    [
        ("other", None, False, None, None, None),
        ("completed", None, True, "{}", 0, None),
        ("completed", "research_people", False, "{}", 0, None),
        ("completed", "research_people", True, None, 0, None),
        ("completed", "research_people", True, "{}", None, None),
        ("completed", "research_people", True, "{}", 0, "internal"),
        ("insufficient_input", "do_not_research", False, None, None, None),
        ("insufficient_input", None, True, None, None, None),
        ("insufficient_input", None, False, "{}", None, None),
        ("insufficient_input", None, False, None, 0, None),
        ("insufficient_input", None, False, None, None, "internal"),
        ("failed", "uncertain", True, None, None, "timeout"),
        ("failed", None, False, None, None, "timeout"),
        ("failed", None, True, "{}", None, "timeout"),
        ("failed", None, True, None, 0, "timeout"),
        ("failed", None, True, None, None, None),
    ],
)
def test_triage_observation_rejects_invalid_disposition_coupling(
    connection: sqlite3.Connection,
    disposition: str,
    semantic_outcome: str | None,
    with_attempt: bool,
    validated_output_json: str | None,
    overflow: int | None,
    failure_category: str | None,
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    provenance_attempt_id = (
        _attempt(connection, run_id=run_id)
        if with_attempt or disposition == "completed"
        else None
    )
    attempt_id = provenance_attempt_id if with_attempt else None
    model_inspection_id = (
        _inspection(
            connection,
            run_id=run_id,
            attempt_id=provenance_attempt_id,
        )
        if disposition == "completed" and provenance_attempt_id is not None
        else None
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, attempt_id, model_inspection_id,
                disposition, semantic_outcome, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, input_truncated, overflow, rationale,
                failure_category, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, 1, ?, 0, ?, 'reason', ?, ?)
            """,
            (
                source_item_id,
                run_id,
                attempt_id,
                model_inspection_id,
                disposition,
                semantic_outcome,
                validated_output_json,
                _HASH,
                _OTHER_HASH,
                _HASH,
                overflow,
                failure_category,
                moment(),
            ),
        )


@pytest.mark.parametrize("semantic_outcome", ["research", "failed", "maybe"])
def test_completed_triage_rejects_unknown_semantic_outcome(
    connection: sqlite3.Connection, semantic_outcome: str
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, attempt_id, model_inspection_id,
                disposition, semantic_outcome, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, input_truncated, overflow, rationale, observed_at
            ) VALUES (?, ?, ?, ?, 'completed', ?, '{}', '{}', ?, ?, 1, ?, 0, 0,
                      'reason', ?)
            """,
            (
                source_item_id,
                run_id,
                attempt_id,
                inspection_id,
                semantic_outcome,
                _HASH,
                _OTHER_HASH,
                _HASH,
                moment(),
            ),
        )


def test_triage_rejects_unknown_failure_category_and_invalid_scalar_facts(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    for overrides in (
        {"failure": "made_up"},
        {"truncated": 2},
        {"schema_version": 0},
        {"prompt_hash": "short"},
        {"schema_hash": "short"},
        {"task_fingerprint": "short"},
        {"observed_at": "2026-07-29 12:00:00"},
    ):
        values: dict[str, object] = {
            "failure": "internal",
            "truncated": 0,
            "schema_version": 1,
            "prompt_hash": _HASH,
            "schema_hash": _OTHER_HASH,
            "task_fingerprint": _HASH,
            "observed_at": moment(),
        }
        values.update(overrides)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO triage_observation (
                    source_item_id, run_id, attempt_id, disposition,
                    canonical_supplied_input_json, prompt_hash, schema_hash,
                    schema_version, task_fingerprint, input_truncated, rationale,
                    failure_category, observed_at
                ) VALUES (?, ?, ?, 'failed', '{}', ?, ?, ?, ?, ?, 'reason', ?, ?)
                """,
                (
                    source_item_id,
                    run_id,
                    attempt_id,
                    values["prompt_hash"],
                    values["schema_hash"],
                    values["schema_version"],
                    values["task_fingerprint"],
                    values["truncated"],
                    values["failure"],
                    values["observed_at"],
                ),
            )


def test_triage_attempt_must_belong_to_observation_run(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    second_run = insert_run(connection)
    source_item_id = _source_item(connection, run_id=second_run)
    first_attempt_id = _attempt(connection, run_id=first_run)
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_attempt_id = _attempt(connection, run_id=second_run)
    inspection_id = _inspection(
        connection, run_id=second_run, attempt_id=second_attempt_id
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, attempt_id, model_inspection_id,
                disposition, semantic_outcome, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, input_truncated, overflow, rationale, observed_at
            ) VALUES (?, ?, ?, ?, 'completed', 'uncertain', '{}', '{}', ?, ?, 1,
                      ?, 0, 0, 'reason', ?)
            """,
            (
                source_item_id,
                second_run,
                first_attempt_id,
                inspection_id,
                _HASH,
                _OTHER_HASH,
                _HASH,
                moment(),
            ),
        )


def test_triage_model_inspection_must_belong_to_observation_run(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    first_attempt = _attempt(connection, run_id=first_run)
    inspection_id = _inspection(connection, run_id=first_run, attempt_id=first_attempt)
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_run = insert_run(connection)
    source_item_id = _source_item(connection, run_id=second_run)
    second_attempt = _attempt(connection, run_id=second_run)
    with pytest.raises(sqlite3.IntegrityError):
        _observation(
            connection,
            source_item_id=source_item_id,
            run_id=second_run,
            attempt_id=second_attempt,
            model_inspection_id=inspection_id,
        )


def test_completed_triage_requires_model_inspection(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, attempt_id, disposition, semantic_outcome,
                canonical_supplied_input_json, validated_output_json, prompt_hash,
                schema_hash, schema_version, task_fingerprint, input_truncated,
                overflow, rationale, observed_at
            ) VALUES (?, ?, ?, 'completed', 'research_people', '{}', '{}', ?, ?,
                      1, ?, 0, 0, 'reason', ?)
            """,
            (
                source_item_id,
                run_id,
                attempt_id,
                _HASH,
                _OTHER_HASH,
                _HASH,
                moment(),
            ),
        )


@pytest.mark.parametrize("write_path", ["insert", "observation_reassignment", "update"])
def test_source_item_current_observation_must_belong_to_same_source_item(
    connection: sqlite3.Connection,
    write_path: str,
) -> None:
    run_id, attempt_id, first_source_id, _ = _valid_graph(connection)
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    second_source_id = _additional_source_item(
        connection, run_id=run_id, source_entry_id="entry-b"
    )
    second_observation_id = _observation(
        connection,
        source_item_id=second_source_id,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        task_fingerprint=_OTHER_HASH,
    )

    with pytest.raises(sqlite3.IntegrityError):
        if write_path == "update":
            connection.execute(
                """
                UPDATE source_item
                SET current_triage_observation_id = ?
                WHERE id = ?
                """,
                (second_observation_id, first_source_id),
            )
        elif write_path == "insert":
            provenance = connection.execute(
                """
                SELECT feed_identity_id, discovered_by_fetch_id
                FROM source_item
                WHERE id = ?
                """,
                (first_source_id,),
            ).fetchone()
            connection.execute(
                """
                INSERT INTO source_item (
                    id, feed_identity_id, discovered_by_fetch_id,
                    discovered_by_run_id, source_entry_id, title_text,
                    discovered_at, current_triage_observation_id
                ) VALUES (999, ?, ?, ?, 'entry-c', 'A third article', ?, ?)
                """,
                (
                    provenance["feed_identity_id"],
                    provenance["discovered_by_fetch_id"],
                    run_id,
                    moment(),
                    second_observation_id,
                ),
            )
        else:
            connection.execute(
                """
                UPDATE source_item
                SET current_triage_observation_id = ?
                WHERE id = ?
                """,
                (second_observation_id, second_source_id),
            )
            connection.execute(
                """
                UPDATE triage_observation
                SET source_item_id = ?
                WHERE id = ?
                """,
                (first_source_id, second_observation_id),
            )


def test_source_item_rowid_replacement_cannot_break_current_observation_ownership(
    connection: sqlite3.Connection,
) -> None:
    run_id, _, source_item_id, observation_id = _valid_graph(connection)
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, source_item_id),
    )
    provenance = connection.execute(
        """
        SELECT feed_identity_id, discovered_by_fetch_id
        FROM source_item
        WHERE id = ?
        """,
        (source_item_id,),
    ).fetchone()
    connection.commit()
    connection.execute("PRAGMA defer_foreign_keys = ON")
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert connection.execute("PRAGMA defer_foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError), connection:
        connection.execute(
            "UPDATE source_item SET rowid = 1000 WHERE id = ?",
            (source_item_id,),
        )
        connection.execute(
            """
            INSERT INTO source_item (
                id, feed_identity_id, discovered_by_fetch_id,
                discovered_by_run_id, source_entry_id, title_text, discovered_at
            ) VALUES (?, ?, ?, ?, 'replacement-entry', 'Replacement row', ?)
            """,
            (
                source_item_id,
                provenance["feed_identity_id"],
                provenance["discovered_by_fetch_id"],
                run_id,
                moment(10),
            ),
        )

    assert (
        connection.execute(
            "SELECT id FROM source_item WHERE id = ?", (source_item_id,)
        ).fetchone()[0]
        == source_item_id
    )
    assert (
        connection.execute("SELECT 1 FROM source_item WHERE id = 1000").fetchone()
        is None
    )


def test_observation_rowid_replacement_cannot_break_current_source_ownership(
    connection: sqlite3.Connection,
) -> None:
    run_id, _, source_item_id, observation_id = _valid_graph(connection)
    second_source_id = _additional_source_item(
        connection, run_id=run_id, source_entry_id="entry-b"
    )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, source_item_id),
    )
    connection.commit()
    connection.execute("PRAGMA defer_foreign_keys = ON")
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert connection.execute("PRAGMA defer_foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError), connection:
        connection.execute(
            "UPDATE triage_observation SET rowid = 1000 WHERE id = ?",
            (observation_id,),
        )
        connection.execute(
            """
            INSERT INTO triage_observation (
                id, source_item_id, run_id, attempt_id, model_inspection_id,
                disposition, semantic_outcome, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, input_truncated, overflow, rationale,
                failure_category, observed_at
            )
            SELECT ?, ?, run_id, attempt_id, model_inspection_id,
                   disposition, semantic_outcome, canonical_supplied_input_json,
                   validated_output_json, prompt_hash, schema_hash, schema_version,
                   ?, input_truncated, overflow, rationale, failure_category,
                   observed_at
            FROM triage_observation
            WHERE id = 1000
            """,
            (observation_id, second_source_id, _OTHER_HASH),
        )

    row = connection.execute(
        "SELECT id, source_item_id FROM triage_observation WHERE id = ?",
        (observation_id,),
    ).fetchone()
    assert tuple(row) == (observation_id, source_item_id)
    assert (
        connection.execute(
            "SELECT 1 FROM triage_observation WHERE id = 1000"
        ).fetchone()
        is None
    )


def test_observation_replace_cannot_change_current_source_ownership(
    connection: sqlite3.Connection,
) -> None:
    run_id, _, source_item_id, observation_id = _valid_graph(connection)
    second_source_id = _additional_source_item(
        connection, run_id=run_id, source_entry_id="entry-b"
    )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, source_item_id),
    )
    connection.commit()
    connection.execute("PRAGMA defer_foreign_keys = ON")
    assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
    assert connection.execute("PRAGMA defer_foreign_keys").fetchone()[0] == 1

    with pytest.raises(sqlite3.IntegrityError), connection:
        _replace_observation(
            connection,
            observation_id=observation_id,
            source_item_id=second_source_id,
            task_fingerprint=_OTHER_HASH,
        )

    row = connection.execute(
        "SELECT id, source_item_id FROM triage_observation WHERE id = ?",
        (observation_id,),
    ).fetchone()
    assert tuple(row) == (observation_id, source_item_id)


def test_observation_replace_preserves_a_matching_current_owner(
    connection: sqlite3.Connection,
) -> None:
    _, _, source_item_id, observation_id = _valid_graph(connection)
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, source_item_id),
    )

    with connection:
        _replace_observation(
            connection,
            observation_id=observation_id,
            source_item_id=source_item_id,
            task_fingerprint=_HASH,
        )

    row = connection.execute(
        "SELECT id, source_item_id FROM triage_observation WHERE id = ?",
        (observation_id,),
    ).fetchone()
    assert tuple(row) == (observation_id, source_item_id)
    assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_current_observation_can_move_by_clear_reassign_then_adopt(
    connection: sqlite3.Connection,
) -> None:
    run_id, _, first_source_id, observation_id = _valid_graph(connection)
    second_source_id = _additional_source_item(
        connection, run_id=run_id, source_entry_id="entry-b"
    )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, first_source_id),
    )

    with connection:
        connection.execute(
            "UPDATE source_item SET current_triage_observation_id = NULL WHERE id = ?",
            (first_source_id,),
        )
        connection.execute(
            "UPDATE triage_observation SET source_item_id = ? WHERE id = ?",
            (second_source_id, observation_id),
        )
        connection.execute(
            "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
            (observation_id, second_source_id),
        )

    assert (
        connection.execute(
            "SELECT current_triage_observation_id FROM source_item WHERE id = ?",
            (first_source_id,),
        ).fetchone()[0]
        is None
    )
    assert (
        connection.execute(
            "SELECT current_triage_observation_id FROM source_item WHERE id = ?",
            (second_source_id,),
        ).fetchone()[0]
        == observation_id
    )


def test_source_item_current_observation_pointer_is_nullable_and_foreign_keyed(
    connection: sqlite3.Connection,
) -> None:
    run_id, _, source_item_id, observation_id = _valid_graph(connection)
    assert run_id > 0
    assert (
        connection.execute(
            "SELECT current_triage_observation_id FROM source_item WHERE id = ?",
            (source_item_id,),
        ).fetchone()[0]
        is None
    )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (observation_id, source_item_id),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE source_item SET current_triage_observation_id = -1 WHERE id = ?",
            (source_item_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "DELETE FROM triage_observation WHERE id = ?", (observation_id,)
        )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = NULL WHERE id = ?",
        (source_item_id,),
    )
    connection.execute("DELETE FROM triage_observation WHERE id = ?", (observation_id,))


def test_observation_history_is_insert_only_in_shape_and_allows_replacement(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, source_item_id, first_id = _valid_graph(connection)
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    second_id = _observation(
        connection,
        source_item_id=source_item_id,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        task_fingerprint=_OTHER_HASH,
    )
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = ? WHERE id = ?",
        (second_id, source_item_id),
    )
    rows = connection.execute(
        "SELECT id, task_fingerprint FROM triage_observation ORDER BY id"
    ).fetchall()
    assert [(row["id"], row["task_fingerprint"]) for row in rows] == [
        (first_id, _HASH),
        (second_id, _OTHER_HASH),
    ]


def test_material_fingerprint_is_unique_only_within_source_item(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, source_item_id, _ = _valid_graph(connection)
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError):
        _observation(
            connection,
            source_item_id=source_item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
        )

    second_source = _additional_source_item(
        connection, run_id=run_id, source_entry_id="entry-b"
    )
    _observation(
        connection,
        source_item_id=second_source,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
    )


@pytest.mark.parametrize("outcome", ["research_people", "do_not_research", "uncertain"])
def test_completed_triage_accepts_every_semantic_outcome(
    connection: sqlite3.Connection, outcome: str
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)
    connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id, disposition,
            semantic_outcome, canonical_supplied_input_json, validated_output_json,
            prompt_hash, schema_hash, schema_version, task_fingerprint,
            input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', ?, '{}', '{}', ?, ?, 1, ?, 0, 0,
                  'reason', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            outcome,
            _HASH,
            _OTHER_HASH,
            _HASH,
            moment(),
        ),
    )


def test_insufficient_input_is_the_only_no_attempt_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    source_item_id = _source_item(connection, run_id=run_id)
    connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, disposition, canonical_supplied_input_json,
            prompt_hash, schema_hash, schema_version, task_fingerprint,
            input_truncated, rationale, observed_at
        ) VALUES (?, ?, 'insufficient_input', '{}', ?, ?, 1, ?, 0,
                  'No normalized title or summary', ?)
        """,
        (source_item_id, run_id, _HASH, _OTHER_HASH, _HASH, moment()),
    )


@pytest.mark.parametrize("outcome", ["research", "do_not_research", "uncertain"])
def test_person_mention_accepts_every_outcome(
    connection: sqlite3.Connection, outcome: str
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name, outcome,
            supporting_passage_ids_json, rationale
        ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', ?, '[]', 'reason')
        """,
        (observation_id, outcome),
    )


def test_person_mention_rejects_nonpositive_ordinal(
    connection: sqlite3.Connection,
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, 0, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
            """,
            (observation_id,),
        )


def test_namesakes_and_owner_local_mention_ordinals_coexist(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, source_item_id, first_observation = _valid_graph(connection)
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    second_observation = _observation(
        connection,
        source_item_id=source_item_id,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        task_fingerprint=_OTHER_HASH,
    )
    for observation_id in (first_observation, second_observation):
        connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '["p1"]',
                      'Meaningful focus')
            """,
            (observation_id,),
        )
    assert (
        connection.execute(
            "SELECT count(*) FROM person_mention WHERE exact_name = 'Alex Smith'"
        ).fetchone()[0]
        == 2
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, 1, 'Other Person', 'Other Person', 'uncertain', '[]',
                      'Possible focus')
            """,
            (first_observation,),
        )


@pytest.mark.parametrize("outcome", ["other", "research_people", "failed"])
def test_person_mention_rejects_unknown_outcome(
    connection: sqlite3.Connection, outcome: str
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', ?, '[]', 'reason')
            """,
            (observation_id, outcome),
        )


@pytest.mark.parametrize(
    "kind",
    [
        "name",
        "profession_or_role",
        "place",
        "nationality",
        "era_or_date",
        "work",
        "affiliation",
        "other",
    ],
)
def test_identity_fact_accepts_every_kind(
    connection: sqlite3.Connection, kind: str
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name, outcome,
            supporting_passage_ids_json, rationale
        ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
        """,
        (observation_id,),
    ).lastrowid
    assert mention_id is not None
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value, supporting_passage_ids_json
        ) VALUES (?, 'fact-1', ?, 'literal supplied value', '["p1"]')
        """,
        (mention_id, kind),
    )


def test_identity_fact_local_id_is_unique_only_within_mention(
    connection: sqlite3.Connection,
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    mention_ids: list[int] = []
    for ordinal in (1, 2):
        mention_id = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, ?, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
            """,
            (observation_id, ordinal),
        ).lastrowid
        assert mention_id is not None
        mention_ids.append(mention_id)
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, 'fact-1', 'name', 'Alex Smith', '["p1"]')
            """,
            (mention_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, 'fact-1', 'other', 'duplicate', '[]')
            """,
            (mention_ids[0],),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, 'fact-2', 'invented', 'bad', '[]')
            """,
            (mention_ids[0],),
        )


@pytest.mark.parametrize("kind", ["attention", "caution"])
@pytest.mark.parametrize("grounding", ["source_text", "domain_profile"])
def test_signal_accepts_each_kind_and_grounding(
    connection: sqlite3.Connection, kind: str, grounding: str
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name, outcome,
            supporting_passage_ids_json, rationale
        ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
        """,
        (observation_id,),
    ).lastrowid
    assert mention_id is not None
    connection.execute(
        """
        INSERT INTO mention_signal (
            person_mention_id, ordinal, kind, category, claim,
            supporting_passage_ids_json, grounding
        ) VALUES (?, 1, ?, 'significant_recognition', 'Won a supplied award',
                  '["p1"]', ?)
        """,
        (mention_id, kind, grounding),
    )


@pytest.mark.parametrize(
    ("kind", "grounding", "ordinal"),
    [
        ("other", "source_text", 1),
        ("attention", "memory", 1),
        ("attention", "source_text", 0),
    ],
)
def test_signal_rejects_unknown_enums_and_nonpositive_ordinal(
    connection: sqlite3.Connection, kind: str, grounding: str, ordinal: int
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name, outcome,
            supporting_passage_ids_json, rationale
        ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
        """,
        (observation_id,),
    ).lastrowid
    assert mention_id is not None
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO mention_signal (
                person_mention_id, ordinal, kind, category, claim,
                supporting_passage_ids_json, grounding
            ) VALUES (?, ?, ?, 'category', 'claim', '[]', ?)
            """,
            (mention_id, ordinal, kind, grounding),
        )


def test_signal_ordinal_is_unique_only_within_mention(
    connection: sqlite3.Connection,
) -> None:
    _, _, _, observation_id = _valid_graph(connection)
    mention_ids: list[int] = []
    for mention_ordinal in (1, 2):
        mention_id = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name, outcome,
                supporting_passage_ids_json, rationale
            ) VALUES (?, ?, 'Alex Smith', 'Alex Smith', 'research', '[]', 'reason')
            """,
            (observation_id, mention_ordinal),
        ).lastrowid
        assert mention_id is not None
        mention_ids.append(mention_id)
        connection.execute(
            """
            INSERT INTO mention_signal (
                person_mention_id, ordinal, kind, category, claim,
                supporting_passage_ids_json, grounding
            ) VALUES (?, 1, 'attention', 'significant_recognition', 'claim',
                      '["p1"]', 'source_text')
            """,
            (mention_id,),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO mention_signal (
                person_mention_id, ordinal, kind, category, claim,
                supporting_passage_ids_json, grounding
            ) VALUES (?, 1, 'caution', 'sparse_context', 'claim', '[]',
                      'source_text')
            """,
            (mention_ids[0],),
        )


def test_child_foreign_key_failure_rolls_back_observation_and_all_children(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    source_item_id = _source_item(connection, run_id=run_id)
    inspection_id = _inspection(connection, run_id=run_id, attempt_id=attempt_id)

    with pytest.raises(sqlite3.IntegrityError), connection:
        observation_id = _observation(
            connection,
            source_item_id=source_item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
        )
        mention_id = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name,
                outcome, supporting_passage_ids_json, rationale
            ) VALUES (?, 1, 'Alex Smith', 'Alex Smith', 'research', '[]',
                      'reason')
            """,
            (observation_id,),
        ).lastrowid
        assert mention_id is not None
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (-1, 'fact-1', 'name', 'Alex Smith', '[]')
            """
        )

    assert (
        connection.execute("SELECT count(*) FROM triage_observation").fetchone()[0] == 0
    )
    assert connection.execute("SELECT count(*) FROM person_mention").fetchone()[0] == 0
    assert (
        connection.execute("SELECT count(*) FROM mention_identity_fact").fetchone()[0]
        == 0
    )


def _person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    display_name: str = "Alex Smith",
    fingerprint: str = _HASH,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, ?, ?)
        """,
        (moment(), run_id, display_name, fingerprint),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _mention_for_observation(
    connection: sqlite3.Connection,
    *,
    observation_id: int,
    exact_name: str = "Alex Smith",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, 'research', '[]', 'reason')
        """,
        (observation_id, exact_name, exact_name),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _er_base_kwargs(
    *,
    run_id: int,
    fingerprint: str = _HASH,
) -> dict[str, object]:
    return {
        "run_id": run_id,
        "prompt_hash": _HASH,
        "schema_hash": _OTHER_HASH,
        "schema_version": 1,
        "task_fingerprint": fingerprint,
        "rationale": "reason",
        "observed_at": moment(),
        "canonical_supplied_input_json": "{}",
    }


def _insert_er(
    connection: sqlite3.Connection,
    **fields: object,
) -> int:
    columns = list(fields)
    placeholders = ", ".join("?" for _ in columns)
    cursor = connection.execute(
        f"""
        INSERT INTO entity_resolution_observation (
            {", ".join(columns)}
        ) VALUES ({placeholders})
        """,
        tuple(fields[c] for c in columns),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def test_mutual_references_between_relation_and_er_exist(
    connection: sqlite3.Connection,
) -> None:
    relation_fks = {
        (row["table"], row["from"])
        for row in connection.execute("PRAGMA foreign_key_list(person_relation)")
    }
    er_fks = {
        (row["table"], row["from"])
        for row in connection.execute(
            "PRAGMA foreign_key_list(entity_resolution_observation)"
        )
    }
    assert (
        "entity_resolution_observation",
        "created_by_observation_id",
    ) in relation_fks
    assert ("entity_resolution_observation", "closed_by_observation_id") in relation_fks
    assert ("person_relation", "person_relation_id") in er_fks


def test_first_pass_er_then_relation_insert_protocol(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_id = _person(connection, run_id=run_id)
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]

    er_id = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=person_id,
        candidate_person_ids_json="[1]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id),
    )
    peer = _person(
        connection, run_id=run_id, display_name="Alex Other", fingerprint=_OTHER_HASH
    )
    a, b = sorted((person_id, peer))
    relation_id = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at, created_by_run_id,
            created_by_observation_id
        ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, ?)
        """,
        (a, b, moment(), run_id, er_id),
    ).lastrowid
    assert relation_id is not None

    orphan_peer = _person(
        connection,
        run_id=run_id,
        display_name="Orphan Peer",
        fingerprint="c" * 64,
    )
    oa, ob = sorted((person_id, orphan_peer))
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO person_relation (
                kind, person_id_a, person_id_b, status, created_at,
                created_by_run_id, created_by_observation_id
            ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, 999999)
            """,
            (oa, ob, moment(), run_id),
        )


def test_reconsider_er_insert_protocol_after_relation_exists(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_a = _person(connection, run_id=run_id)
    person_b = _person(
        connection, run_id=run_id, display_name="Alex Other", fingerprint=_OTHER_HASH
    )
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    first_pass = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=person_a,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id, fingerprint=_HASH),
    )
    a, b = sorted((person_a, person_b))
    relation_id = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at, created_by_run_id,
            created_by_observation_id
        ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, ?)
        """,
        (a, b, moment(), run_id, first_pass),
    ).lastrowid
    assert relation_id is not None

    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_attempt = _attempt(connection, run_id=run_id)
    second_inspection = _inspection(
        connection,
        run_id=run_id,
        attempt_id=second_attempt,
        routing_fingerprint=_OTHER_HASH,
    )
    reconsider_id = _insert_er(
        connection,
        person_mention_id=None,
        person_relation_id=relation_id,
        attempt_id=second_attempt,
        model_inspection_id=second_inspection,
        disposition="completed",
        semantic_outcome="different_people",
        selected_person_id=None,
        created_person_id=None,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"different_people"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id, fingerprint=_OTHER_HASH),
    )
    assert reconsider_id > 0


def test_namesakes_share_exact_name_and_match_key(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    first = _person(connection, run_id=run_id)
    second = _person(
        connection, run_id=run_id, display_name="Alex Smith", fingerprint=_OTHER_HASH
    )
    for person_id in (first, second):
        connection.execute(
            """
            INSERT INTO sourced_name (
                person_id, exact_name, search_name, match_key, kind,
                origin_kind, first_observed_at, last_observed_at
            ) VALUES (?, 'Alex Smith', 'Alex Smith', 'alex smith', 'display',
                      'manual', ?, ?)
            """,
            (person_id, moment(), moment()),
        )
    rows = connection.execute(
        """
        SELECT person_id
          FROM sourced_name
         WHERE match_key = 'alex smith'
         ORDER BY person_id
        """
    ).fetchall()
    assert [int(r["person_id"]) for r in rows] == sorted([first, second])


def test_mention_cannot_point_current_er_at_relation_scoped_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_a = _person(connection, run_id=run_id)
    person_b = _person(
        connection, run_id=run_id, display_name="Alex Other", fingerprint=_OTHER_HASH
    )
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    first_pass = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=person_a,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id),
    )
    a, b = sorted((person_a, person_b))
    relation_id = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at, created_by_run_id,
            created_by_observation_id
        ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, ?)
        """,
        (a, b, moment(), run_id, first_pass),
    ).lastrowid
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_attempt = _attempt(connection, run_id=run_id)
    second_inspection = _inspection(
        connection,
        run_id=run_id,
        attempt_id=second_attempt,
        routing_fingerprint=_OTHER_HASH,
    )
    relation_er = _insert_er(
        connection,
        person_mention_id=None,
        person_relation_id=relation_id,
        attempt_id=second_attempt,
        model_inspection_id=second_inspection,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=None,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id, fingerprint=_OTHER_HASH),
    )

    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            UPDATE person_mention
               SET current_entity_resolution_observation_id = ?
             WHERE id = ?
            """,
            (relation_er, mention_id),
        )

    # Positive control: mention-scoped ER is accepted as current pointer.
    connection.execute(
        """
        UPDATE person_mention
           SET current_entity_resolution_observation_id = ?
         WHERE id = ?
        """,
        (first_pass, mention_id),
    )
    assert (
        connection.execute(
            """
            SELECT current_entity_resolution_observation_id
              FROM person_mention
             WHERE id = ?
            """,
            (mention_id,),
        ).fetchone()[0]
        == first_pass
    )


@pytest.mark.parametrize(
    ("semantic_outcome", "candidates", "selected", "created", "attempted"),
    [
        ("created_new", "[]", None, True, False),
        ("same_person", "[1]", True, False, True),
        ("different_people", "[1]", False, True, True),
        ("uncertain", "[1]", False, True, True),
    ],
)
def test_k22_first_pass_completed_outcomes_insert(
    connection: sqlite3.Connection,
    semantic_outcome: str,
    candidates: str,
    selected: bool | None,
    created: bool,
    attempted: bool,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_id = _person(connection, run_id=run_id)
    peer = _person(
        connection, run_id=run_id, display_name="Peer", fingerprint=_OTHER_HASH
    )
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    selected_id = peer if selected else None
    created_id = person_id if created else None
    er_attempt = attempt_id if attempted else None
    er_inspection = inspection_id if attempted else None
    if candidates == "[1]":
        candidates = f"[{peer}]"
    er_id = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=er_attempt,
        model_inspection_id=er_inspection,
        disposition="completed",
        semantic_outcome=semantic_outcome,
        selected_person_id=selected_id,
        created_person_id=created_id,
        candidate_person_ids_json=candidates,
        validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id),
    )
    assert er_id > 0


def test_k22_first_pass_skipped_and_failed_insert(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    skipped = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=None,
        model_inspection_id=None,
        disposition="skipped",
        semantic_outcome=None,
        selected_person_id=None,
        created_person_id=None,
        candidate_person_ids_json="[]",
        validated_output_json=None,
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id, fingerprint=_HASH),
    )
    assert skipped > 0
    failed = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=None,
        disposition="failed",
        semantic_outcome=None,
        selected_person_id=None,
        created_person_id=None,
        candidate_person_ids_json="[]",
        validated_output_json=None,
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category="malformed_response",
        **_er_base_kwargs(run_id=run_id, fingerprint=_OTHER_HASH),
    )
    assert failed > 0


@pytest.mark.parametrize(
    "semantic_outcome",
    ["same_person", "different_people", "uncertain"],
)
def test_k22_reconsider_completed_outcomes_require_null_created_person(
    connection: sqlite3.Connection,
    semantic_outcome: str,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_a = _person(connection, run_id=run_id)
    person_b = _person(
        connection, run_id=run_id, display_name="Alex Other", fingerprint=_OTHER_HASH
    )
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    first_pass = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=person_a,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id),
    )
    a, b = sorted((person_a, person_b))
    relation_id = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at, created_by_run_id,
            created_by_observation_id
        ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, ?)
        """,
        (a, b, moment(), run_id, first_pass),
    ).lastrowid
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_attempt = _attempt(connection, run_id=run_id)
    second_inspection = _inspection(
        connection,
        run_id=run_id,
        attempt_id=second_attempt,
        routing_fingerprint=_OTHER_HASH,
    )
    selected = person_a if semantic_outcome == "same_person" else None
    er_id = _insert_er(
        connection,
        person_mention_id=None,
        person_relation_id=relation_id,
        attempt_id=second_attempt,
        model_inspection_id=second_inspection,
        disposition="completed",
        semantic_outcome=semantic_outcome,
        selected_person_id=selected,
        created_person_id=None,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id, fingerprint=_OTHER_HASH),
    )
    assert er_id > 0


def test_k22_rejects_created_person_id_on_relation_scoped_completed_row(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _source_item_id, observation_id = _valid_graph(connection)
    mention_id = _mention_for_observation(connection, observation_id=observation_id)
    person_a = _person(connection, run_id=run_id)
    person_b = _person(
        connection, run_id=run_id, display_name="Alex Other", fingerprint=_OTHER_HASH
    )
    inspection_id = connection.execute("SELECT id FROM model_inspection").fetchone()[0]
    first_pass = _insert_er(
        connection,
        person_mention_id=mention_id,
        person_relation_id=None,
        attempt_id=attempt_id,
        model_inspection_id=inspection_id,
        disposition="completed",
        semantic_outcome="uncertain",
        selected_person_id=None,
        created_person_id=person_a,
        candidate_person_ids_json=f"[{person_b}]",
        validated_output_json='{"outcome":"uncertain"}',
        supporting_fact_ids_json=None,
        conflicting_fact_ids_json=None,
        failure_category=None,
        **_er_base_kwargs(run_id=run_id),
    )
    a, b = sorted((person_a, person_b))
    relation_id = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at, created_by_run_id,
            created_by_observation_id
        ) VALUES ('possible_same_person', ?, ?, 'active', ?, ?, ?)
        """,
        (a, b, moment(), run_id, first_pass),
    ).lastrowid
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    second_attempt = _attempt(connection, run_id=run_id)
    second_inspection = _inspection(
        connection,
        run_id=run_id,
        attempt_id=second_attempt,
        routing_fingerprint=_OTHER_HASH,
    )
    with pytest.raises(sqlite3.IntegrityError):
        _insert_er(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=second_attempt,
            model_inspection_id=second_inspection,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json=f"[{person_b}]",
            validated_output_json='{"outcome":"different_people"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_base_kwargs(run_id=run_id, fingerprint=_OTHER_HASH),
        )
