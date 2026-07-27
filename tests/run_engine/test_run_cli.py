from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from tests.run_engine.helpers import ENVIRONMENT, write_graph


def run_notable(
    config_file: Path, *arguments: str, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, **ENVIRONMENT, **(environment or {})}
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "notable_person_finder",
            "--config",
            str(config_file),
            *arguments,
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_graph(tmp_path)


def test_run_on_a_fresh_database_is_complete(config_file: Path) -> None:
    completed = run_notable(config_file, "run")
    assert completed.returncode == 0, completed.stderr
    assert "# Notable Person Finder" in completed.stdout
    assert "**State:** complete" in completed.stdout


def test_run_stdout_is_byte_for_byte_the_persisted_digest(
    config_file: Path, tmp_path: Path
) -> None:
    completed = run_notable(config_file, "run")
    digests = tmp_path / "portable" / "data" / "digests"
    written = next(path for path in digests.iterdir() if path.name != "latest.md")
    assert completed.stdout == written.read_text(encoding="utf-8")


def test_run_creates_the_database_and_applies_migrations(
    config_file: Path, tmp_path: Path
) -> None:
    run_notable(config_file, "run")
    assert (tmp_path / "portable" / "data" / "notable.sqlite3").is_file()


def test_a_second_same_day_run_is_allowed_and_creates_another_run(
    config_file: Path, tmp_path: Path
) -> None:
    assert run_notable(config_file, "run").returncode == 0
    assert run_notable(config_file, "run").returncode == 0
    digests = tmp_path / "portable" / "data" / "digests"
    dated = [path for path in digests.iterdir() if path.name != "latest.md"]
    assert len(dated) == 2


def test_invalid_configuration_fails_with_status_one_and_no_traceback(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path, operational="[concurrency]\nhttp_workers = 0\n")
    completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    assert "Traceback" not in completed.stderr
    assert "http_workers" in completed.stderr


def test_a_missing_secret_names_the_variable_but_not_its_value(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "notable_person_finder",
            "--config",
            str(config_file),
            "run",
        ],
        capture_output=True,
        text=True,
        env={
            key: value
            for key, value in os.environ.items()
            if key not in {"TEST_OPENROUTER", "TEST_BRAVE"}
        },
        timeout=120,
    )
    assert completed.returncode == 1
    assert "TEST_OPENROUTER" in completed.stderr
    # No value-absence assertion belongs here: this process never held
    # `or-secret-value`, so such an assertion could not fail whatever the code
    # did. `test_a_secret_that_is_present_is_not_echoed_when_another_is_missing`
    # owns that property, in a process that genuinely holds the value.


def test_no_secret_value_appears_in_output_or_digest(
    config_file: Path, tmp_path: Path
) -> None:
    completed = run_notable(config_file, "run")
    combined = completed.stdout + completed.stderr
    assert "or-secret-value" not in combined
    assert "brave-secret-value" not in combined
    for path in (tmp_path / "portable" / "data" / "digests").iterdir():
        assert "or-secret-value" not in path.read_text(encoding="utf-8")


def test_an_overlapping_run_fails_immediately_without_creating_a_run(
    config_file: Path, tmp_path: Path
) -> None:
    import portalocker

    lock_file = tmp_path / "portable" / "data" / "notable.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    # Assert the contention message itself. "lock" alone would be satisfied by
    # the substring inside the path `notable.lock`, so it would still pass if
    # the message stopped explaining that another process holds the lock.
    # The owner detail is deliberately not asserted: lock file contents are
    # diagnostic only and never determine ownership, so this test holds the
    # lock without writing them.
    assert "another mutation is using" in completed.stderr
    assert str(lock_file) in completed.stderr


def test_usage_error_returns_sixty_four(config_file: Path) -> None:
    completed = run_notable(config_file, "nonsense")
    assert completed.returncode == 64


def test_sigint_during_a_run_returns_one_hundred_thirty(
    config_file: Path, tmp_path: Path
) -> None:
    # Raise KeyboardInterrupt from inside the run, exactly as SIGINT would, and
    # assert the documented status rather than racing a real signal.
    script = textwrap.dedent(
        f"""
        import sys
        from notable_person_finder.cli import main as cli
        from notable_person_finder.runs.engine import RunEngine

        def interrupted(self, handlers):
            raise KeyboardInterrupt

        RunEngine.execute = interrupted
        sys.exit(cli.main(["--config", {str(config_file)!r}, "run"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 130
    assert "Traceback" not in completed.stderr


# Recording an interrupted run in durable state is covered directly by
# tests/run_engine/test_crash_boundary.py; this case owns the exit status only.


def test_reporting_failure_exits_one_and_persists_failed_state(
    config_file: Path, tmp_path: Path
) -> None:
    script = textwrap.dedent(
        f"""
        import sys
        from notable_person_finder.cli import main as cli
        from notable_person_finder.reporting.digest import DigestWriteError

        def fail(*args, **kwargs):
            raise DigestWriteError("simulated digest failure")

        cli.write_digest = fail
        sys.exit(cli.main(["--config", {str(config_file)!r}, "run"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 1
    database = tmp_path / "portable" / "data" / "notable.sqlite3"
    connection = connect_database(database)
    assert connection.execute("SELECT state FROM run").fetchone()[0] == "failed"
    assert [
        row[0]
        for row in connection.execute("SELECT state FROM run_transition ORDER BY id")
    ] == ["running", "failed"]
    connection.close()


def test_status_reports_the_latest_run_without_locking(
    config_file: Path, tmp_path: Path
) -> None:
    run_notable(config_file, "run")
    completed = run_notable(config_file, "status")
    assert completed.returncode == 0
    assert "run-1" in completed.stdout
    assert "complete" in completed.stdout


def test_status_can_inspect_committed_state_while_the_lock_is_held(
    config_file: Path, tmp_path: Path
) -> None:
    import portalocker

    run_notable(config_file, "run")
    lock_file = tmp_path / "portable" / "data" / "notable.lock"
    with open(lock_file, "a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        completed = run_notable(config_file, "status")
    assert completed.returncode == 0


def test_status_before_any_run_reports_that_none_exists(config_file: Path) -> None:
    completed = run_notable(config_file, "status")
    assert completed.returncode == 0
    assert "no run" in completed.stdout.lower()


def test_run_writes_a_latest_copy(config_file: Path, tmp_path: Path) -> None:
    run_notable(config_file, "run")
    latest = tmp_path / "portable" / "data" / "digests" / "latest.md"
    assert latest.is_file()


def test_run_writes_structured_logs(config_file: Path, tmp_path: Path) -> None:
    run_notable(config_file, "run")
    log_file = tmp_path / "portable" / "logs" / "notable.jsonl"
    assert log_file.is_file()
    assert "run_started" in log_file.read_text(encoding="utf-8")


# --- Supplementary coverage -------------------------------------------------
#
# The cases above come from the task brief. The cases below close gaps the
# brief leaves open: exit status 2 has no brief case at all, and several brief
# cases would still pass against implementations that get the load-bearing
# property wrong (a `status` that takes the lock, an error path that echoes a
# secret it actually holds).


def test_a_run_with_unhandled_required_work_returns_exactly_two(
    config_file: Path, tmp_path: Path
) -> None:
    """Exit status 2 means partial, and nothing else.

    This milestone registers no task handlers, so required work that no
    handler claims stays pending and the run is partial by derivation.
    """
    from notable_person_finder.db.connection import connect_database
    from notable_person_finder.runs import repository

    assert run_notable(config_file, "run").returncode == 0

    connection = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        repository.schedule_work(
            connection,
            task_type="unhandled.task",
            subject_kind="candidate",
            subject_id=None,
            fingerprint="a" * 64,
            required=True,
            priority=100,
            eligible_at="2000-01-01T00:00:00Z",
            run_id=None,
            now="2000-01-01T00:00:00Z",
        )
    finally:
        connection.close()

    completed = run_notable(config_file, "run")
    assert completed.returncode == 2, completed.stderr
    assert "**State:** partial" in completed.stdout


def test_an_overlapping_run_creates_no_database_and_no_run(
    config_file: Path, tmp_path: Path
) -> None:
    """`run` must acquire the lock before it touches durable state."""
    import portalocker

    lock_file = tmp_path / "portable" / "data" / "notable.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    assert not (tmp_path / "portable" / "data" / "notable.sqlite3").exists()
    assert not (tmp_path / "portable" / "data" / "digests").exists()


def test_status_never_creates_the_lock_file(config_file: Path, tmp_path: Path) -> None:
    """`status` performs no work and takes no lock, so it leaves no lock file.

    `MutationLock` creates its file on entry, so a `status` that acquired the
    lock -- even successfully -- would leave one behind.
    """
    completed = run_notable(config_file, "status")
    assert completed.returncode == 0
    assert not (tmp_path / "portable" / "data" / "notable.lock").exists()


def test_a_secret_that_is_present_is_not_echoed_when_another_is_missing(
    tmp_path: Path,
) -> None:
    """The failing process holds one real secret; it must not appear anywhere.

    The brief's missing-secret case removes both variables from the child
    environment, so its value could not have leaked whatever the code did.
    Here the value is genuinely available to the process that fails.
    """
    config_file = write_graph(tmp_path)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "notable_person_finder",
            "--config",
            str(config_file),
            "run",
        ],
        capture_output=True,
        text=True,
        env={
            **{
                key: value
                for key, value in os.environ.items()
                if key not in {"TEST_OPENROUTER", "TEST_BRAVE"}
            },
            "TEST_OPENROUTER": "or-secret-value",
        },
        timeout=120,
    )
    assert completed.returncode == 1
    assert "TEST_BRAVE" in completed.stderr
    combined = completed.stdout + completed.stderr
    assert "or-secret-value" not in combined


def test_a_configuration_error_does_not_echo_secret_values(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path, operational="[concurrency]\nhttp_workers = 0\n")
    completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    combined = completed.stdout + completed.stderr
    assert "or-secret-value" not in combined
    assert "brave-secret-value" not in combined


def test_the_digest_is_dated_from_the_same_instant_as_the_window(
    config_file: Path, tmp_path: Path
) -> None:
    """One clock reading dates both the window and the digest file name.

    The clock crosses local midnight between its first and second observation.
    An implementation that reads it once per use files the digest under the
    following day while the run's own window belongs to the previous one.
    """
    script = textwrap.dedent(
        f"""
        import sys
        from datetime import UTC, datetime
        from notable_person_finder.cli import main as cli
        from notable_person_finder.runs.clock import SystemClock

        # In Europe/Paris (UTC+1 in January) the first instant is
        # 2026-01-01 23:59:59.9 local and every later one is 2026-01-02.
        first = [datetime(2026, 1, 1, 22, 59, 59, 900000, tzinfo=UTC)]
        later = datetime(2026, 1, 1, 23, 0, 0, 100000, tzinfo=UTC)

        SystemClock.now = lambda self: first.pop() if first else later
        sys.exit(cli.main(["--config", {str(config_file)!r}, "run"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr
    digests = tmp_path / "portable" / "data" / "digests"
    assert (digests / "2026-01-01-run-1.md").is_file()
    assert not (digests / "2026-01-02-run-1.md").exists()


def test_status_against_an_unmigrated_database_is_actionable(
    config_file: Path, tmp_path: Path
) -> None:
    """A half-migrated data root must not read as an empty, healthy one."""
    from notable_person_finder.db.connection import connect_database

    connect_database(tmp_path / "portable" / "data" / "notable.sqlite3").close()

    completed = run_notable(config_file, "status")
    assert completed.returncode == 1
    assert "no such table" not in completed.stderr
    assert "notable db migrate" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_the_cli_wires_the_resolved_secrets_into_the_log_redaction_filter(
    config_file: Path, tmp_path: Path
) -> None:
    """`command_run` must hand the resolved credentials to `configure_logging`.

    An absence assertion cannot guard this wire: this milestone never logs a
    secret of its own, so `secret not in notable.jsonl` passes even against a
    `secrets=()` that cuts the redaction wire entirely. So log a
    secret-bearing field THROUGH the logger the CLI configured, and require
    the redaction marker to be there. If the CLI stops passing the
    credentials the filter has nothing to match and the plaintext lands in
    `notable.jsonl`.
    """
    script = textwrap.dedent(
        f"""
        import logging
        import os
        import sys
        from notable_person_finder.cli import main as cli
        from notable_person_finder.obs.logging import EVENT_LOGGER_NAME, log_event

        status = cli.main(["--config", {str(config_file)!r}, "run"])
        # The logger `command_run` configured is still installed, filter and
        # handler included. Nothing is reconfigured here.
        log_event(
            logging.getLogger(EVENT_LOGGER_NAME),
            "probe_of_the_cli_configured_logger",
            detail="openrouter=" + os.environ["TEST_OPENROUTER"],
            other="brave=" + os.environ["TEST_BRAVE"],
        )
        logging.shutdown()
        sys.exit(status)
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 0, completed.stderr

    log_file = tmp_path / "portable" / "logs" / "notable.jsonl"
    lines = [
        line
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if "probe_of_the_cli_configured_logger" in line
    ]
    # Without this the two assertions below could both hold on an empty file.
    assert len(lines) == 1, log_file.read_text(encoding="utf-8")
    assert '"detail":"openrouter=[redacted]"' in lines[0]
    assert '"other":"brave=[redacted]"' in lines[0]
    assert "or-secret-value" not in log_file.read_text(encoding="utf-8")
    assert "brave-secret-value" not in log_file.read_text(encoding="utf-8")


def test_the_persisted_configuration_snapshot_holds_no_secret_value(
    config_file: Path, tmp_path: Path
) -> None:
    """`command_run` passes `loaded.snapshot_json` straight into the run row.

    Configuration is the one structure that holds the resolved credentials, so
    what actually lands in `configuration_snapshot.canonical_json` is the
    assertion that matters -- not what the loader intends to build.
    """
    assert run_notable(config_file, "run").returncode == 0

    connection = connect_database(tmp_path / "portable" / "data" / "notable.sqlite3")
    try:
        rows = connection.execute(
            "SELECT canonical_json FROM configuration_snapshot"
        ).fetchall()
    finally:
        connection.close()

    assert len(rows) == 1
    canonical_json = rows[0][0]
    # A snapshot that is empty or absent would satisfy the absence assertions
    # below without recording anything, so pin that it holds real content.
    assert "Europe/Paris" in canonical_json
    assert "or-secret-value" not in canonical_json
    assert "brave-secret-value" not in canonical_json


def test_verbose_names_the_digest_without_revealing_secrets(
    config_file: Path, tmp_path: Path
) -> None:
    completed = run_notable(config_file, "--verbose", "run")
    assert completed.returncode == 0, completed.stderr
    assert "digest:" in completed.stderr
    assert str(tmp_path / "portable" / "data" / "digests") in completed.stderr
    combined = completed.stdout + completed.stderr
    assert "or-secret-value" not in combined
    assert "brave-secret-value" not in combined
