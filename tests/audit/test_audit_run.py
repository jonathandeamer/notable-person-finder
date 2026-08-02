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
