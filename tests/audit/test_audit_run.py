from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_digest_row, insert_run, migrated_database


def _section(out: str, heading: str) -> str:
    """The lines of one rendered section, from its heading to the next blank
    line. Scoping an assertion to this slice (rather than the whole of
    `out`) is essential: several sections legitimately repeat the same raw
    numbers (e.g. an attempt's actual cost also feeds the budget cross-check),
    so an unscoped `"3000" in out` assertion can pass because a *different*
    section printed the value, hiding a defect in the section under test.
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
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    for heading in (
        "run",
        "configuration",
        "transitions",
        "work outcomes",
        "attempts",
        "failures",
        "budget",
        "reporting",
    ):
        assert heading in captured.out.lower()


def test_deferred_work_items_show_their_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, reason, created_by_run_id,
                claimed_by_run_id, created_at, updated_at
            ) VALUES (
                'assess_article', 'person_article', 7, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'deferred', 'budget_exhausted', ?, ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("a" * 64, run_id, run_id),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    # The reason is the whole point of this section: status cannot explain
    # why work deferred, and this is where that answer lives.
    assert "budget_exhausted" in out
    assert "assess_article" in out


def test_budget_divergence_between_run_total_and_attempt_sum_is_shown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            "UPDATE run SET budget_actual_nano_usd = 5000 WHERE id = ?", (run_id,)
        )
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'detect_people', 'source_item', 1, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'succeeded', ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("b" * 64, run_id),
        )
        work_item_id = connection.execute(
            "SELECT id FROM work_item WHERE fingerprint = ?", ("b" * 64,)
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, request_fingerprint,
                reserved_nano_usd, actual_nano_usd
            ) VALUES (
                ?, ?, 'openrouter', 'generate_structured', 1,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z', 'succeeded',
                ?, 0, 3000
            )
            """,
            (run_id, work_item_id, "c" * 64),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    budget_section = _section(out, "Budget")
    # 5000 recorded on the run, 3000 summed from attempts. Both must appear
    # *within the Budget section itself*: hiding the divergence there would
    # defeat the cross-check, even though the Attempts section legitimately
    # also shows the same attempt's actual cost.
    assert "5000" in budget_section.replace(",", "") or "0.000005" in budget_section
    assert "3000" in budget_section.replace(",", "") or "0.000003" in budget_section


def test_attempts_section_shows_every_field(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'assess_article', 'person_article', 9, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'succeeded', ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("f" * 64, run_id),
        )
        work_item_id = connection.execute(
            "SELECT id FROM work_item WHERE fingerprint = ?", ("f" * 64,)
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, failure_category,
                provider_status, retry_after_ms, request_fingerprint,
                destination_host, response_bytes, latency_ms,
                reserved_nano_usd, actual_nano_usd
            ) VALUES (
                ?, ?, 'brave', 'web_search', 3,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z', 'failed',
                'rate_limited', 503, NULL, ?,
                'fixture.invalid', 5678, 1234,
                2000, 3000
            )
            """,
            (run_id, work_item_id, "g" * 64),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    attempts_section = _section(out, "Attempts")
    # Every field the spec (K7 point 5) requires, each a distinct,
    # individually identifiable value: a dropped or swapped field cannot
    # hide behind an equal-to-something-else coincidence.
    for expected in (
        "brave",
        "web_search",
        "ordinal=3",
        "failed",
        "rate_limited",
        "503",
        "1234",
        "5678",
        "fixture.invalid",
        "2000",
        "3000",
    ):
        assert expected in attempts_section, (
            f"{expected!r} missing from Attempts section"
        )


def test_failures_section_includes_interrupted_attempts(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'mediawiki_search', 'person', 4, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'deferred', ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("h" * 64, run_id),
        )
        work_item_id = connection.execute(
            "SELECT id FROM work_item WHERE fingerprint = ?", ("h" * 64,)
        ).fetchone()[0]
        # `interrupted` attempts carry no failure_category (the schema
        # forbids one outside outcome='failed') -- exactly the crash-window
        # evidence an operator audits a broken run to find.
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, request_fingerprint,
                reserved_nano_usd, actual_nano_usd
            ) VALUES (
                ?, ?, 'mediawiki', 'search_pages', 1,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z', 'interrupted',
                ?, 0, NULL
            )
            """,
            (run_id, work_item_id, "i" * 64),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    failures_section = _section(out, "Failures")
    assert "interrupted" in failures_section
    assert "mediawiki" in failures_section
    assert "search_pages" in failures_section


def test_reporting_section_shows_digest_details(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        digest_id = insert_digest_row(
            connection,
            run_id=run_id,
            file_path="/fixture/digests/2026-08-02.md",
            content_hash="j" * 64,
            run_state="partial",
        )
        connection.execute(
            """
            INSERT INTO person (
                id, created_at, created_by_run_id, display_name,
                identity_fingerprint
            ) VALUES (101, '2026-08-02T00:00:00Z', ?, 'Fixture Person', ?)
            """,
            (run_id, "k" * 64),
        )
        connection.execute(
            """
            INSERT INTO lead_assessment (
                id, person_id, run_id, outcome, ordering_factors_json,
                lead_policy_fingerprint, decided_at
            ) VALUES (
                201, 101, ?, 'promising_lead', '{}', ?, '2026-08-02T00:00:00Z'
            )
            """,
            (run_id, "l" * 64),
        )
        connection.execute(
            """
            INSERT INTO queue_transition (
                id, person_id, run_id, lead_assessment_id, tier, from_status,
                to_status, reason, occurred_at
            ) VALUES (
                301, 101, ?, 201, 'promising_lead', NULL, 'pending',
                'new lead', '2026-08-02T00:00:00Z'
            )
            """,
            (run_id,),
        )
        connection.execute(
            """
            INSERT INTO digest_entry (
                digest_id, person_id, lead_assessment_id, queue_transition_id,
                ordinal
            ) VALUES (?, 101, 201, 301, 1)
            """,
            (digest_id,),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    reporting_section = _section(out, "Reporting")
    # Path, hash, write-time run_state, entry count, and the pointer to
    # `digest show` (K7 point 8) -- each a distinct value so a dropped or
    # swapped field cannot hide behind a coincidental match.
    assert "/fixture/digests/2026-08-02.md" in reporting_section
    assert "j" * 64 in reporting_section
    assert "partial" in reporting_section
    assert "  entry_count: 1" in reporting_section
    assert f"notable digest show {run_id}" in reporting_section


def test_unknown_run_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id + 999), attempt_id_argument=None
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_malformed_run_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument="bogus", attempt_id_argument=None
    )

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""


def _seed_attempt(connection, *, run_id: int, task_type: str = "detect_people") -> int:
    connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at,
            updated_at
        ) VALUES (?, 'source_item', 1, ?, 1, 10, '2026-08-02T00:00:00Z',
                  'succeeded', ?, '2026-08-02T00:00:00Z',
                  '2026-08-02T00:00:00Z')
        """,
        (task_type, "d" * 64, run_id),
    )
    work_item_id = connection.execute(
        "SELECT id FROM work_item WHERE fingerprint = ?", ("d" * 64,)
    ).fetchone()[0]
    connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint, reserved_nano_usd,
            actual_nano_usd
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                  '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z',
                  'succeeded', ?, 0, 100)
        """,
        (run_id, work_item_id, "e" * 64),
    )
    attempt_id = connection.execute(
        "SELECT id FROM attempt WHERE request_fingerprint = ?", ("e" * 64,)
    ).fetchone()[0]
    return attempt_id


def test_attempt_from_another_run_is_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_a = insert_run(connection)
        run_b = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_a)
        connection.commit()
    finally:
        connection.close()

    # The attempt exists, but not in run_b. Answering with run_a's data
    # would be a correctness bug dressed up as convenience.
    status = cli_main.command_audit_run(
        config_file,
        run_id_argument=str(run_b),
        attempt_id_argument=str(attempt_id),
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""
    assert str(attempt_id) in captured.err


def test_attempt_without_a_result_row_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_id)
        connection.commit()  # no triage_observation inserted
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    result_section = _section(out, "Result")
    assert status == cli_main.EXIT_OK
    assert "no persisted result row" in result_section


def test_fetch_feed_attempt_carries_the_unattributable_caveat(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_id, task_type="fetch_feed")
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    result_section = _section(out, "Result")
    # feed_fetch records no attempt_id, so per-attempt attribution is
    # impossible and must be disclosed rather than guessed by position.
    assert "cannot be attributed to a single attempt" in result_section


def test_malformed_attempt_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument="nope"
    )

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""


def _seed_work_item_only(
    connection, *, run_id: int, fingerprint: str, task_type: str = "detect_people"
) -> int:
    connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at,
            updated_at
        ) VALUES (?, 'source_item', 1, ?, 1, 10, '2026-08-02T00:00:00Z',
                  'succeeded', ?, '2026-08-02T00:00:00Z',
                  '2026-08-02T00:00:00Z')
        """,
        (task_type, fingerprint, run_id),
    )
    work_item_id = connection.execute(
        "SELECT id FROM work_item WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()[0]
    return work_item_id


def _insert_attempt(
    connection,
    *,
    run_id: int,
    work_item_id: int,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint, destination_host,
            reserved_nano_usd, actual_nano_usd
        ) VALUES (?, ?, 'openrouter', 'generate_structured', ?,
                  '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z', 'succeeded',
                  ?, ?, 0, 0)
        """,
        (run_id, work_item_id, ordinal, request_fingerprint, destination_host),
    )


def _seed_completed_detect_people_result(
    connection,
    *,
    run_id: int,
    attempt_id: int,
    source_item_key: str,
    feed_key: str,
    rationale: str,
    validated_output_json: str,
    schema_version: int,
    routing_fingerprint: str,
) -> None:
    """A real `triage_observation` row (K7/K9's registered result table for
    `detect_people`) joined on `(attempt_id, run_id)`, satisfying the
    `disposition = 'completed'` CHECK constraint in
    `db/migrations/0004_people_detection.sql` (non-NULL attempt_id,
    model_inspection_id, semantic_outcome, validated_output_json, overflow;
    NULL failure_category)."""
    feed_id = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed', ?, '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z')
        """,
        (feed_key, f"https://example.com/{feed_key}"),
    ).lastrowid
    fetch_id = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, '2026-08-02T00:00:00Z', ?, 'modified')
        """,
        (feed_id, run_id, f"https://example.com/{feed_key}"),
    ).lastrowid
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'Title', 'Summary', '2026-08-02T00:00:00Z')
        """,
        (feed_id, fetch_id, run_id, source_item_key),
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
        (run_id, attempt_id, routing_fingerprint),
    ).lastrowid
    connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', ?,
                  ?, ?, ?, ?, 0, 0, ?, '2026-08-02T00:00:00Z')
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            validated_output_json,
            "n" * 64,
            "o" * 64,
            schema_version,
            "q" * 64,
            rationale,
        ),
    )


def test_result_row_from_a_matching_result_table_is_rendered(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_id)

        # Decoy: a second attempt, on a second work item, in the SAME run.
        # Its triage_observation shares `run_id` with the target but has a
        # different `attempt_id`. If the generic result join ever dropped
        # the `attempt_id` column (matching on `run_id` alone), this decoy's
        # markers would leak into the target attempt's rendered result.
        decoy_work_item_id = _seed_work_item_only(
            connection, run_id=run_id, fingerprint="v" * 64
        )
        _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=decoy_work_item_id,
            ordinal=1,
            request_fingerprint="w" * 64,
        )
        decoy_attempt_id = connection.execute(
            "SELECT id FROM attempt WHERE request_fingerprint = ?", ("w" * 64,)
        ).fetchone()[0]
        _seed_completed_detect_people_result(
            connection,
            run_id=run_id,
            attempt_id=decoy_attempt_id,
            source_item_key="decoy-entry",
            feed_key="decoy-feed",
            rationale="DECOY_RATIONALE_11223",
            validated_output_json='{"marker": "DECOY_VALIDATED_44556"}',
            schema_version=99,
            routing_fingerprint="m" * 64,
        )

        _seed_completed_detect_people_result(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            source_item_key="target-entry",
            feed_key="target-feed",
            rationale="TARGET_RATIONALE_78901",
            validated_output_json='{"marker": "TARGET_VALIDATED_23457"}',
            schema_version=42,
            routing_fingerprint="n" * 64,
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    result_section = _section(out, "Result")
    assert status == cli_main.EXIT_OK
    # The real persisted result: the whole point of this command.
    assert "TARGET_RATIONALE_78901" in result_section
    assert "TARGET_VALIDATED_23457" in result_section
    assert "42" in result_section
    assert "no persisted result row" not in result_section
    # A wrong (or dropped) join column would leak the decoy's row in
    # alongside -- or instead of -- the target's.
    assert "DECOY_RATIONALE_11223" not in result_section
    assert "DECOY_VALIDATED_44556" not in result_section


def test_local_handler_attempt_is_reported_as_a_data_inconsistency(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        # `aggregate_person_lead` is the one registered task type that never
        # makes an external call (`external=False`); an attempt existing for
        # it anyway is a data inconsistency, not a missing result.
        attempt_id = _seed_attempt(
            connection, run_id=run_id, task_type="aggregate_person_lead"
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    result_section = _section(out, "Result")
    assert status == cli_main.EXIT_OK
    assert "aggregate_person_lead" in result_section
    assert "external" in result_section
    # This is a distinct failure mode from "no persisted result row": naming
    # the inconsistency, not claiming the attempt is simply unevidenced.
    assert "no persisted result row" not in result_section


def test_retry_history_renders_in_ordinal_order_even_when_inserted_out_of_order(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        work_item_id = _seed_work_item_only(
            connection, run_id=run_id, fingerprint="r" * 64
        )
        # Inserted out of ordinal order (3, then 1, then 2). A query that
        # relied on insertion/rowid order instead of `ORDER BY ordinal`
        # would still pass a same-order seed; this seed is designed to fail
        # under exactly that regression.
        _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            ordinal=3,
            request_fingerprint="s" * 64,
            destination_host="host-three",
        )
        _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            ordinal=1,
            request_fingerprint="t" * 64,
            destination_host="host-one",
        )
        _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            ordinal=2,
            request_fingerprint="u" * 64,
            destination_host="host-two",
        )
        attempt_id = connection.execute(
            "SELECT id FROM attempt WHERE request_fingerprint = ?", ("t" * 64,)
        ).fetchone()[0]
        # NOTE (see task-4-report.md fix-round mutation evidence): `attempt`
        # carries `UNIQUE (work_item_id, ordinal)`, and SQLite backs a
        # UNIQUE constraint with its own autoindex that cannot be dropped
        # (`DROP INDEX` on it raises "index associated with UNIQUE or
        # PRIMARY KEY constraint cannot be dropped"). `EXPLAIN QUERY PLAN`
        # confirms the retry-history query always resolves via a COVERING
        # INDEX keyed `(work_item_id, ordinal)`, so it returns ordinal order
        # even with no `ORDER BY` in the SQL at all. No seed can defeat that:
        # this test still pins the real, operator-visible contract (retry
        # history renders in ordinal order), it just cannot be used as
        # standalone proof that the query's own `ORDER BY ordinal` clause is
        # what produces it on this schema.
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    retry_section = _section(out, "Retry history")
    assert status == cli_main.EXIT_OK
    first = retry_section.index("host-one")
    second = retry_section.index("host-two")
    third = retry_section.index("host-three")
    assert first < second < third
