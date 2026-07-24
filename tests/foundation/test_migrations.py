from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations


class _BackupInspectingConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.backup_pragmas: tuple[int, str, int, int] | None = None

    def __getattr__(self, name: str) -> object:
        return getattr(self._connection, name)

    def backup(self, destination: sqlite3.Connection) -> None:
        self.backup_pragmas = (
            destination.execute("PRAGMA foreign_keys").fetchone()[0],
            destination.execute("PRAGMA journal_mode").fetchone()[0],
            destination.execute("PRAGMA synchronous").fetchone()[0],
            destination.execute("PRAGMA busy_timeout").fetchone()[0],
        )
        self._connection.backup(destination)


def test_migrations_apply_once_and_create_backup_before_upgrade(tmp_path: Path) -> None:
    database = tmp_path / "notable.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        first = apply_migrations(connection, database, backups)
        second = apply_migrations(connection, database, backups)
    finally:
        connection.close()

    assert first.applied_versions == (1,)
    assert first.backup_path is not None and first.backup_path.exists()
    assert second.applied_versions == ()
    assert second.backup_path is None


def test_migration_backup_destination_uses_writable_configuration(
    tmp_path: Path,
) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = _BackupInspectingConnection(connect_database(database))
    try:
        apply_migrations(connection, database, tmp_path / "backups")
    finally:
        connection.close()

    assert connection.backup_pragmas == (1, "wal", 2, 5_000)


def test_changed_applied_migration_checksum_fails(tmp_path: Path) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    try:
        apply_migrations(connection, database, tmp_path / "backups")
        connection.execute(
            "UPDATE schema_migration SET checksum = ? WHERE version = 1",
            ("0" * 64,),
        )
        connection.commit()
        with pytest.raises(MigrationError, match="checksum"):
            apply_migrations(connection, database, tmp_path / "backups")
    finally:
        connection.close()
