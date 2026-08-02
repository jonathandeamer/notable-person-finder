"""Shared fixture builders for the audit tests."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from pathlib import Path

from notable_person_finder.config.loader import load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from tests.ingestion.helpers import insert_configuration_snapshot, moment
from tests.people.test_run_cli import _single_feed, write_people_graph


def migrated_database(tmp_path: Path) -> tuple[Path, sqlite3.Connection]:
    """Return (config_file, open read-write connection) on a migrated db."""
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    apply_migrations(connection, database, loaded.paths.backups)
    return config_file, connection


def insert_run(
    connection: sqlite3.Connection,
    *,
    state: str = "complete",
    started_at: str | None = None,
    finished_at: str | None = None,
    configuration_snapshot_id: int | None = None,
) -> int:
    snapshot_id = configuration_snapshot_id or insert_configuration_snapshot(connection)
    started = started_at or moment()
    finished = finished_at if finished_at is not None else moment()
    if state == "running":
        finished = None
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone, window_start,
            window_end, started_at, finished_at, budget_limit_nano_usd,
            budget_reserved_nano_usd, budget_actual_nano_usd
        ) VALUES (?, ?, 'UTC', ?, ?, ?, ?, NULL, 0, 0)
        """,
        (state, snapshot_id, started, started, started, finished),
    )
    run_id = cursor.lastrowid
    assert run_id is not None
    return run_id


def insert_real_configuration_snapshot(
    connection: sqlite3.Connection,
    config_file: Path,
    *,
    environ: Mapping[str, str],
) -> int:
    """Insert a `configuration_snapshot` row built by the real `load_config`.

    `tests.ingestion.helpers.insert_configuration_snapshot` always writes the
    literal string `'{}'` as `canonical_json` -- fine for tests that only
    need a row to satisfy a foreign key, but it means secrets set in
    `environ` can never reach the database through it, which makes a
    redaction assertion built on top of it vacuous (I4). This instead runs
    the production snapshot-building path (`load_config` -> `_snapshot`),
    which folds credentials in only as `secret_availability` booleans, and
    persists that real `snapshot_json`/`fingerprint` pair -- so a secret set
    in `environ` genuinely is reachable into `canonical_json` if the
    redaction logic that keeps it out ever regresses.
    """
    resolved = load_config(config_file, environ=environ, require_secrets=False)
    cursor = connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, ?, ?)
        """,
        (resolved.fingerprint, resolved.snapshot_json, moment()),
    )
    snapshot_id = cursor.lastrowid
    assert snapshot_id is not None
    connection.commit()
    return snapshot_id


def insert_digest_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    content_hash: str,
    run_state: str = "complete",
) -> int:
    now = moment()
    cursor = connection.execute(
        """
        INSERT INTO digest (
            run_id, file_path, timezone, window_start, window_end,
            run_state, content_hash, created_at
        ) VALUES (?, ?, 'UTC', ?, ?, ?, ?, ?)
        """,
        (run_id, file_path, now, now, run_state, content_hash, now),
    )
    digest_id = cursor.lastrowid
    assert digest_id is not None
    return digest_id
