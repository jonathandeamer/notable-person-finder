from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, cast

import pytest

from notable_person_finder.db import migrate as migrate_module
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations


class _BackupInspectingConnection:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection
        self.backup_pragmas: tuple[int, str, int, int] | None = None

    def __getattr__(self, name: str) -> Any:
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
        apply_migrations(
            cast(sqlite3.Connection, connection), database, tmp_path / "backups"
        )
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


def test_backup_failure_leaves_existing_database_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    try:
        connection.execute("CREATE TABLE legacy_record (value TEXT NOT NULL)")
        connection.execute("INSERT INTO legacy_record VALUES ('preserve me')")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        database_bytes = database.read_bytes()
        wal_path = database.with_name(f"{database.name}-wal")
        wal_bytes = wal_path.read_bytes()

        def fail_backup(
            connection: sqlite3.Connection,
            database_path: Path,
            backup_dir: Path,
        ) -> Path:
            raise MigrationError("injected backup failure")

        monkeypatch.setattr(migrate_module, "_create_backup", fail_backup)

        with pytest.raises(MigrationError, match="injected backup failure"):
            apply_migrations(connection, database, tmp_path / "backups")

        assert database.read_bytes() == database_bytes
        assert wal_path.read_bytes() == wal_bytes
        assert [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        ] == ["legacy_record"]
        assert [
            row[0] for row in connection.execute("SELECT value FROM legacy_record")
        ] == ["preserve me"]
    finally:
        connection.close()


def test_pre_migration_backup_precedes_metadata_bootstrap(tmp_path: Path) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    try:
        connection.execute("CREATE TABLE legacy_record (value TEXT NOT NULL)")
        connection.execute("INSERT INTO legacy_record VALUES ('before migration')")
        connection.commit()
        result = apply_migrations(connection, database, tmp_path / "backups")
    finally:
        connection.close()

    assert result.backup_path is not None
    backup = sqlite3.connect(result.backup_path)
    try:
        tables = {
            row[0]
            for row in backup.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "legacy_record" in tables
        assert "schema_migration" not in tables
        assert "configuration_snapshot" not in tables
        assert backup.execute("SELECT value FROM legacy_record").fetchall() == [
            ("before migration",)
        ]
    finally:
        backup.close()


def test_migrations_reject_caller_owned_transaction_without_committing_it(
    tmp_path: Path,
) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    try:
        connection.execute("CREATE TABLE caller_work (value TEXT NOT NULL)")
        connection.commit()
        connection.execute("INSERT INTO caller_work VALUES ('uncommitted')")

        with pytest.raises(MigrationError, match="active transaction"):
            apply_migrations(connection, database, tmp_path / "backups")

        assert connection.in_transaction
        assert [
            row[0] for row in connection.execute("SELECT value FROM caller_work")
        ] == ["uncommitted"]
        connection.rollback()
        assert list(connection.execute("SELECT value FROM caller_work")) == []
    finally:
        connection.close()
