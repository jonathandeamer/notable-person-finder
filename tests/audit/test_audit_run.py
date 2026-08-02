from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_run, migrated_database


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
    # 5000 recorded on the run, 3000 summed from attempts. Both must appear:
    # hiding the divergence would defeat the cross-check.
    assert "5000" in out.replace(",", "") or "0.000005" in out
    assert "3000" in out.replace(",", "") or "0.000003" in out


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
