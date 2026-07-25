from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from notable_person_finder.runs.lock import MutationLock
from tests.foundation.helpers import write_graph


def run_cli(config_file: Path, *arguments: str, env: dict[str, str] | None = None):
    command_env = os.environ.copy()
    command_env.update(env or {})
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "notable_person_finder",
            "--config",
            str(config_file),
            *arguments,
        ],
        check=False,
        capture_output=True,
        text=True,
        env=command_env,
    )


def test_config_validate_reports_fingerprint_without_secrets(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    completed = run_cli(
        config_file,
        "config",
        "validate",
        env={"TEST_OPENROUTER": "openrouter-secret", "TEST_BRAVE": "brave-secret"},
    )

    assert completed.returncode == 0
    assert "configuration valid" in completed.stdout
    assert "fingerprint:" in completed.stdout
    assert f"main: {config_file}" in completed.stdout
    assert "openrouter-secret" not in completed.stdout + completed.stderr
    assert "brave-secret" not in completed.stdout + completed.stderr


def test_paths_reports_every_resolved_path_without_secrets(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    completed = run_cli(config_file, "paths")

    portable = tmp_path / "portable"
    expected_paths = {
        "config_file": config_file,
        "data_root": portable / "data",
        "database": portable / "data" / "notable.sqlite3",
        "backups": portable / "data" / "backups",
        "digests": portable / "data" / "digests",
        "log_file": portable / "logs" / "notable.jsonl",
        "cache_root": portable / "cache",
        "lock_file": portable / "data" / "notable.lock",
    }

    assert completed.returncode == 0
    assert completed.stderr == ""
    assert completed.stdout.splitlines() == [
        f"{name}: {path}" for name, path in expected_paths.items()
    ]


def test_db_migrate_creates_database_under_lock(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    completed = run_cli(config_file, "db", "migrate")

    assert completed.returncode == 0
    assert "applied migrations: 1" in completed.stdout
    assert (tmp_path / "portable" / "data" / "notable.sqlite3").exists()


def test_db_migrate_reports_lock_contention_before_creating_database(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)
    database = tmp_path / "portable" / "data" / "notable.sqlite3"
    lock_file = tmp_path / "portable" / "data" / "notable.lock"

    with MutationLock(lock_file):
        completed = run_cli(config_file, "db", "migrate")

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert "another mutation is using" in completed.stderr
    assert not database.exists()


def test_invalid_configuration_uses_exit_one_and_stderr(tmp_path: Path) -> None:
    config_file = tmp_path / "notable.toml"
    config_file.write_text("schema_version = 1\n", encoding="utf-8")
    completed = run_cli(config_file, "config", "validate")

    assert completed.returncode == 1
    assert completed.stdout == ""
    assert "configuration is invalid" in completed.stderr


def test_invalid_usage_returns_64() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "notable_person_finder", "unknown"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 64
    assert "usage:" in completed.stderr


def test_checked_in_example_configuration_is_structurally_valid(
    tmp_path: Path,
) -> None:
    import shutil

    config_root = tmp_path / "config"
    profile_root = config_root / "discovery_profiles"
    profile_root.mkdir(parents=True)
    shutil.copyfile("config/notable.example.toml", config_root / "notable.toml")
    shutil.copyfile(
        "config/discovery-feeds.example.toml",
        config_root / "discovery-feeds.toml",
    )
    shutil.copyfile(
        "config/discovery_profiles/art.example.toml",
        profile_root / "art.toml",
    )
    completed = run_cli(
        config_root / "notable.toml",
        "config",
        "validate",
        env={"OPENROUTER_API_KEY": "test", "BRAVE_API_KEY": "test"},
    )

    assert completed.returncode == 0, completed.stderr
