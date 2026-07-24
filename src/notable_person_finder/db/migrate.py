from __future__ import annotations

import hashlib
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Iterable

_MIGRATION_FILENAME = re.compile(
    r"^(?P<version>[0-9]{4})_(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\.sql$"
)


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    sql: str
    checksum: str


@dataclass(frozen=True, slots=True)
class MigrationResult:
    applied_versions: tuple[int, ...]
    backup_path: Path | None


class MigrationError(Exception):
    pass


def load_migrations() -> tuple[Migration, ...]:
    migration_root = resources.files("notable_person_finder.db").joinpath("migrations")
    discovered: list[Migration] = []

    for resource in migration_root.iterdir():
        if not resource.is_file() or not resource.name.endswith(".sql"):
            continue
        match = _MIGRATION_FILENAME.fullmatch(resource.name)
        if match is None:
            raise MigrationError(f"malformed migration filename: {resource.name}")

        sql_bytes = resource.read_bytes()
        try:
            sql = sql_bytes.decode("utf-8")
        except UnicodeDecodeError as error:
            raise MigrationError(
                f"migration {resource.name} is not valid UTF-8"
            ) from error

        discovered.append(
            Migration(
                version=int(match.group("version")),
                name=match.group("name"),
                sql=sql,
                checksum=hashlib.sha256(sql_bytes).hexdigest(),
            )
        )

    return _ordered_migrations(discovered)


def apply_migrations(
    connection: sqlite3.Connection,
    database_path: Path,
    backup_dir: Path,
    migrations: Iterable[Migration] | None = None,
) -> MigrationResult:
    available = (
        load_migrations() if migrations is None else _ordered_migrations(migrations)
    )
    available_by_version = {migration.version: migration for migration in available}

    _ensure_migration_table(connection)
    applied = _read_applied_migrations(connection)
    _verify_applied_migrations(applied, available_by_version)

    applied_versions = {version for version, _, _ in applied}
    pending = tuple(
        migration
        for migration in available
        if migration.version not in applied_versions
    )
    if not pending:
        return MigrationResult(applied_versions=(), backup_path=None)

    backup_path = _create_backup(connection, database_path, backup_dir)
    newly_applied: list[int] = []
    for migration in pending:
        try:
            connection.execute("BEGIN")
            for statement in _split_statements(migration.sql):
                connection.execute(statement)
            connection.execute(
                """
                INSERT INTO schema_migration (version, name, checksum, applied_at)
                VALUES (?, ?, ?, ?)
                """,
                (
                    migration.version,
                    migration.name,
                    migration.checksum,
                    _utc_timestamp(),
                ),
            )
            connection.commit()
        except (MigrationError, sqlite3.Error) as error:
            connection.rollback()
            raise MigrationError(
                f"migration {migration.version} failed; "
                f"backup: {backup_path}: {error}"
            ) from error
        newly_applied.append(migration.version)

    return MigrationResult(
        applied_versions=tuple(newly_applied),
        backup_path=backup_path,
    )


def _ordered_migrations(migrations: Iterable[Migration]) -> tuple[Migration, ...]:
    ordered = tuple(sorted(migrations, key=lambda migration: migration.version))
    versions = tuple(migration.version for migration in ordered)

    if any(version == 0 for version in versions):
        raise MigrationError("migration version zero is not allowed")
    if any(version < 0 for version in versions):
        raise MigrationError("migration versions must be positive")
    if len(versions) != len(set(versions)):
        raise MigrationError("duplicate migration version")

    expected = tuple(range(1, len(versions) + 1))
    if versions != expected:
        missing = sorted(set(expected) - set(versions))
        detail = f"; missing {missing}" if missing else ""
        raise MigrationError(
            f"migration versions must be a sequence starting at 1{detail}"
        )
    return ordered


def _ensure_migration_table(connection: sqlite3.Connection) -> None:
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migration (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
    except sqlite3.Error as error:
        connection.rollback()
        raise MigrationError(f"could not create schema_migration: {error}") from error


def _read_applied_migrations(
    connection: sqlite3.Connection,
) -> tuple[tuple[int, str, str], ...]:
    try:
        rows = connection.execute(
            "SELECT version, name, checksum FROM schema_migration ORDER BY version"
        ).fetchall()
    except sqlite3.Error as error:
        raise MigrationError(f"could not read schema_migration: {error}") from error
    return tuple((row[0], row[1], row[2]) for row in rows)


def _verify_applied_migrations(
    applied: tuple[tuple[int, str, str], ...],
    available_by_version: dict[int, Migration],
) -> None:
    for version, name, checksum in applied:
        migration = available_by_version.get(version)
        if migration is None:
            raise MigrationError(
                f"database contains unexpected migration version {version}"
            )
        if migration.name != name:
            raise MigrationError(
                f"migration {version} name does not match packaged migration"
            )
        if migration.checksum != checksum:
            raise MigrationError(
                f"migration {version} checksum does not match packaged migration"
            )

    versions = tuple(version for version, _, _ in applied)
    if versions:
        expected = tuple(range(1, versions[-1] + 1))
        if versions != expected:
            missing = sorted(set(expected) - set(versions))
            raise MigrationError(
                f"database is missing applied migration versions {missing}"
            )


def _create_backup(
    connection: sqlite3.Connection,
    database_path: Path,
    backup_dir: Path,
) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    backup_path = backup_dir / f"pre-migration-{timestamp}.sqlite3"
    backup_connection: sqlite3.Connection | None = None
    try:
        backup_connection = sqlite3.connect(backup_path)
        connection.backup(backup_connection)
    except sqlite3.Error as error:
        raise MigrationError(
            f"could not back up {database_path} to {backup_path}: {error}"
        ) from error
    finally:
        if backup_connection is not None:
            backup_connection.close()
    return backup_path


def _split_statements(sql: str) -> tuple[str, ...]:
    statements: list[str] = []
    buffer: list[str] = []

    for character in sql:
        buffer.append(character)
        if character == ";":
            candidate = "".join(buffer)
            if sqlite3.complete_statement(candidate):
                statements.append(candidate)
                buffer.clear()

    tail = "".join(buffer)
    if tail.strip():
        raise MigrationError("migration contains an incomplete SQL statement")
    return tuple(statements)


def _utc_timestamp() -> str:
    timestamp = datetime.now(UTC).isoformat(timespec="microseconds")
    return timestamp.replace("+00:00", "Z")
