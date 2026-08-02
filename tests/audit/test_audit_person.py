from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations, load_migrations
from tests.audit.helpers import insert_run, migrated_database
from tests.people.test_run_cli import _single_feed, write_people_graph


def _person(connection, *, run_id: int, name: str, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES ('2026-08-02T00:00:00Z', ?, ?, ?)
        """,
        (run_id, name, fingerprint),
    )
    person_id = cursor.lastrowid
    assert person_id is not None
    return person_id


def _section(out: str, heading: str) -> str:
    """The lines of one rendered section, from its heading to the next blank
    line. See tests/audit/test_audit_run.py's identical helper: scoping an
    assertion to this slice (rather than the whole of `out`) is essential
    because several sections can legitimately repeat the same raw values.
    """
    lines = out.splitlines()
    start = lines.index(heading)
    end = start + 1
    while end < len(lines) and lines[end] != "":
        end += 1
    return "\n".join(lines[start:end])


def test_prints_every_section_heading(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Ada Example", fingerprint="1" * 64
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(
        config_file, person_id_argument=str(person_id)
    )

    out = capsys.readouterr().out.lower()
    assert status == cli_main.EXIT_OK
    for heading in (
        "identity",
        "sourced names",
        "relations",
        "mentions",
        "entity resolution",
        "wikipedia",
        "coverage",
        "assessments",
        "lead history",
        "queue history",
        "digest history",
    ):
        assert heading in out


def test_empty_sections_are_marked_not_omitted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Empty Person", fingerprint="2" * 64
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_person(config_file, person_id_argument=str(person_id))

    out = capsys.readouterr().out
    # A person with no coverage must still show the heading with an explicit
    # marker; a silently omitted section reads as "not queried".
    assert "none" in out.lower()
    coverage_section = _section(out, "Coverage")
    assert "none" in coverage_section.lower()


def test_mentions_use_the_current_person_id_not_resolution_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """K11: person_mention.person_id is the durable association.

    Seeds a mention whose *current* person_id is B while an entity-resolution
    observation still records selected_person_id = A. Deriving mentions from
    the observation would attribute the mention to A and miss it under B.

    The `source_item`, `triage_observation`, and
    `entity_resolution_observation` seeds below deviate from the task brief:
    the brief's column lists (`external_id`, `run_id`, `title`, ... on
    source_item; `outcome`, `model`, `decided_at` on triage_observation;
    `decided_at` on entity_resolution_observation, and a `same_person` row
    with no attempt/model_inspection and an empty candidate list) do not
    match the shipped schema and its CHECK constraints in
    db/migrations/0003_ingestion.sql, 0004_people_detection.sql, and
    0005_people_identity.sql. Adjusted to the real column lists: the
    `triage_observation` row uses `disposition = 'insufficient_input'`
    (there is no 'skipped' disposition on this table -- that value belongs
    to entity_resolution_observation -- and the row only needs to exist for
    person_mention's FK, not to represent a completed research decision);
    the `entity_resolution_observation` row's real CHECK for
    `semantic_outcome = 'same_person'` requires a non-NULL `attempt_id` and
    `model_inspection_id` and a non-empty `candidate_person_ids_json`, so a
    minimal work_item/attempt/model_inspection chain is seeded to satisfy
    it. The surrounding assertion -- the divergence between the mention's
    current person_id and this observation's historical
    selected_person_id -- is unchanged from the brief.
    """
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_a = _person(
            connection, run_id=run_id, name="Person A", fingerprint="3" * 64
        )
        person_b = _person(
            connection, run_id=run_id, name="Person B", fingerprint="4" * 64
        )
        feed_id = connection.execute(
            """
            INSERT INTO feed_identity (
                key, current_label, current_url, first_seen_at, last_seen_at
            ) VALUES ('feed-1', 'Feed', 'https://example.test/feed',
                      '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')
            """
        ).lastrowid
        fetch_id = connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            ) VALUES (?, ?, '2026-08-02T00:00:00Z', 'https://example.test/feed',
                      'modified')
            """,
            (feed_id, run_id),
        ).lastrowid
        source_item_id = connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                source_entry_id, title_text, summary_text, discovered_at
            ) VALUES (?, ?, ?, 'ext-1', 'Title', 'Summary',
                      '2026-08-02T00:00:00Z')
            """,
            (feed_id, fetch_id, run_id),
        ).lastrowid
        triage_id = connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, disposition,
                canonical_supplied_input_json, prompt_hash, schema_hash,
                schema_version, task_fingerprint, input_truncated, rationale,
                observed_at
            ) VALUES (?, ?, 'insufficient_input', '{}', ?, ?, 1, ?, 0,
                      'seeded', '2026-08-02T00:00:00Z')
            """,
            (source_item_id, run_id, "a" * 64, "b" * 64, "c" * 64),
        ).lastrowid
        mention_id = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name,
                outcome, supporting_passage_ids_json, rationale, person_id
            ) VALUES (?, 1, 'Reassigned Name', 'reassigned name', 'research',
                      '[]', 'seeded', ?)
            """,
            (triage_id, person_b),
        ).lastrowid
        work_item_id = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES ('resolve_person_entity', 'person_mention', ?, ?, 1, 10,
                      '2026-08-02T00:00:00Z', 'succeeded', ?,
                      '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')
            """,
            (mention_id, "g" * 64, run_id),
        ).lastrowid
        attempt_id = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, request_fingerprint,
                destination_host, reserved_nano_usd, actual_nano_usd
            ) VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                      '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z',
                      'succeeded', ?, 'openrouter.ai', 0, 0)
            """,
            (run_id, work_item_id, "h" * 64),
        ).lastrowid
        inspection_id = connection.execute(
            """
            INSERT INTO model_inspection (
                run_id, attempt_id, configured_model_id, resolved_model_id,
                routing_fingerprint, supported_parameters_json,
                supports_strict_structured_output, pricing_usable,
                prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
                compatibility, inspected_at
            ) VALUES (?, ?, 'm', 'm', ?, '[]', 1, 1, 1, 1, 'compatible',
                      '2026-08-02T00:00:00Z')
            """,
            (run_id, attempt_id, "i" * 64),
        ).lastrowid
        connection.execute(
            """
            INSERT INTO entity_resolution_observation (
                person_mention_id, run_id, attempt_id, model_inspection_id,
                disposition, semantic_outcome, selected_person_id,
                candidate_person_ids_json, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash,
                schema_version, task_fingerprint, rationale, observed_at
            ) VALUES (?, ?, ?, ?, 'completed', 'same_person', ?, ?, '{}',
                      '{}', ?, ?, 1, ?, 'historical', '2026-08-02T00:00:00Z')
            """,
            (
                mention_id,
                run_id,
                attempt_id,
                inspection_id,
                person_a,
                f"[{person_a}]",
                "d" * 64,
                "e" * 64,
                "f" * 64,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_person(config_file, person_id_argument=str(person_b))
    out_b = capsys.readouterr().out

    mentions_section = _section(out_b, "Mentions")
    assert "Reassigned Name" in mentions_section


def test_merged_away_person_shows_a_banner_and_its_own_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        survivor = _person(
            connection, run_id=run_id, name="Survivor", fingerprint="5" * 64
        )
        merged = _person(
            connection, run_id=run_id, name="Merged Away", fingerprint="6" * 64
        )
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, merged),
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument=str(merged))

    out = capsys.readouterr().out
    assert status == cli_main.EXIT_OK
    # Must NOT redirect: the merged person's own name and its own Identity
    # section both appear, because a merge is exactly what the operator came
    # to understand.
    assert "Merged Away" in out
    # Discriminating banner assertions (I3): a bare `str(survivor) in out`
    # is satisfied by unrelated single-digit numbers elsewhere in the
    # rendered output (e.g. `created_by_run_id: 1`) and survives deleting
    # the banner block entirely. The literal marker text and the full
    # `notable audit person <id>` pointer string cannot appear by accident.
    assert "MERGED AWAY" in out
    assert f"notable audit person {survivor}" in out


def test_unknown_person_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument="4242")

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_malformed_person_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument="person-1")

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""


def test_coverage_section_degrades_on_a_database_migrated_only_through_0006(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """K3: a database that predates milestone 5 has `wikipedia_identity_plan`
    (migration 0006) but no `person_coverage_plan` or `lead_assessment`
    (0007/0008). The Coverage section must degrade with the "section
    unavailable" marker instead of raising `sqlite3.OperationalError` from a
    query against a table that does not exist yet.
    """
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    partial_migrations = [
        migration for migration in load_migrations() if migration.version <= 6
    ]
    apply_migrations(connection, database, loaded.paths.backups, partial_migrations)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Partial Schema", fingerprint="7" * 64
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(
        config_file, person_id_argument=str(person_id)
    )

    out = capsys.readouterr().out
    assert status == cli_main.EXIT_OK
    coverage_section = _section(out, "Coverage")
    assert "section unavailable" in coverage_section.lower()


def test_wikipedia_section_degrades_on_a_database_migrated_only_through_0005(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """K3: a database that predates milestone 4 has `person` and
    `entity_resolution_observation` (migration 0005) but no
    `wikipedia_identity_plan` (0006). The Wikipedia section must degrade with
    the "section unavailable" marker instead of raising
    `sqlite3.OperationalError` from a query against a table that does not
    exist yet. This is a distinct guard from the Coverage-section test above:
    that fixture is migrated through 0006, where `wikipedia_identity_plan`
    already exists, so it cannot exercise this earlier guard.
    """
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    partial_migrations = [
        migration for migration in load_migrations() if migration.version <= 5
    ]
    apply_migrations(connection, database, loaded.paths.backups, partial_migrations)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Earlier Schema", fingerprint="8" * 64
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(
        config_file, person_id_argument=str(person_id)
    )

    out = capsys.readouterr().out
    assert status == cli_main.EXIT_OK
    wikipedia_section = _section(out, "Wikipedia")
    assert "section unavailable" in wikipedia_section.lower()
