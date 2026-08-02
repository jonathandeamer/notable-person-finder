from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_digest_row, insert_run, migrated_database

BODY = "# Digest\n\nreal persisted content\n"


def _seed(tmp_path: Path, *, body: str = BODY) -> tuple[Path, Path, int]:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        digest_file = tmp_path / "digest.md"
        digest_file.write_text(body, encoding="utf-8")
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        insert_digest_row(
            connection,
            run_id=run_id,
            file_path=str(digest_file),
            content_hash=digest,
        )
        connection.commit()
    finally:
        connection.close()
    return config_file, digest_file, run_id


def test_prints_verified_digest_verbatim_and_nothing_else(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    # Verbatim: stdout must hash back to the recorded content_hash. Any
    # banner, header, or trailing summary breaks this equality, which is
    # what makes it a real assertion of K6 rather than a substring check.
    assert captured.out == BODY


def test_hash_mismatch_writes_nothing_to_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, digest_file, run_id = _seed(tmp_path)
    digest_file.write_text("tampered\n", encoding="utf-8")

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    # The point of the check: a piped invocation must forward nothing.
    assert captured.out == ""
    assert "tampered" not in captured.err
    assert "hash" in captured.err.lower()


def test_missing_file_reports_and_prints_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, digest_file, run_id = _seed(tmp_path)
    digest_file.unlink()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""
    assert str(digest_file) in captured.err


def test_defaults_to_the_highest_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        older_body = "# older\n"
        newer_body = "# newer\n"
        for body in (older_body, newer_body):
            run_id = insert_run(connection)
            path = tmp_path / f"digest-{run_id}.md"
            path.write_text(body, encoding="utf-8")
            insert_digest_row(
                connection,
                run_id=run_id,
                file_path=str(path),
                content_hash=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=None)

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    assert captured.out == newer_body


def test_accepts_the_human_id_form(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(config_file, run_id_argument=f"run-{run_id}")

    assert status == cli_main.EXIT_OK
    assert capsys.readouterr().out == BODY


def test_malformed_run_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, _ = _seed(tmp_path)

    status = cli_main.command_digest_show(config_file, run_id_argument="not-a-run")

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_USAGE
    assert captured.out == ""


def test_unknown_run_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(
        config_file, run_id_argument=str(run_id + 999)
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_falls_back_to_run_columns_when_no_digest_row(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        path = tmp_path / "legacy.md"
        path.write_text(BODY, encoding="utf-8")
        connection.execute(
            "UPDATE run SET digest_path = ?, digest_sha256 = ? WHERE id = ?",
            (str(path), hashlib.sha256(BODY.encode("utf-8")).hexdigest(), run_id),
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    assert status == cli_main.EXIT_OK
    assert capsys.readouterr().out == BODY


def test_falls_back_to_run_columns_when_digest_table_absent(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The `digest` table guard (K3) is real: a database migrated before it

    existed must still work via the `run.digest_path`/`digest_sha256`
    fallback, not raise sqlite3.OperationalError.
    """
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        path = tmp_path / "legacy.md"
        path.write_text(BODY, encoding="utf-8")
        connection.execute(
            "UPDATE run SET digest_path = ?, digest_sha256 = ? WHERE id = ?",
            (str(path), hashlib.sha256(BODY.encode("utf-8")).hexdigest(), run_id),
        )
        connection.commit()
        connection.execute("DROP TABLE digest_entry")
        connection.execute("DROP TABLE digest")
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    assert captured.out == BODY


def test_run_with_no_digest_reports_failure_and_prints_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A run that exists but never produced a digest (no `digest` row and no

    `run.digest_path`) must report failure, not fall through silently. This
    differs from test_unknown_run_reports_failure, which uses a run id that
    does not exist at all and takes the earlier "no run found" branch.
    """
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""
