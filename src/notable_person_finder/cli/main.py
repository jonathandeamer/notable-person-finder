from __future__ import annotations

import argparse
import sqlite3
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import NoReturn

from notable_person_finder.config.loader import ConfigLoadError, load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations
from notable_person_finder.runs.lock import LockUnavailable, MutationLock


class UsageError(Exception):
    pass


class NotableArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise UsageError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = NotableArgumentParser(prog="notable")
    parser.add_argument("--config", type=Path, help="path to notable.toml")
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    config = commands.add_parser("config")
    config.add_subparsers(dest="config_command", required=True).add_parser("validate")
    commands.add_parser("paths")
    db = commands.add_parser("db")
    db.add_subparsers(dest="db_command", required=True).add_parser("migrate")
    return parser


def command_config_validate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=True)
    print("configuration valid")
    print(f"fingerprint: {loaded.fingerprint}")
    print(f"main: {loaded.paths.config_file}")
    return 0


def command_paths(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    for name in (
        "config_file",
        "data_root",
        "database",
        "backups",
        "digests",
        "log_file",
        "cache_root",
        "lock_file",
    ):
        print(f"{name}: {getattr(loaded.paths, name)}")
    return 0


def command_db_migrate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    with MutationLock(loaded.paths.lock_file):
        connection = connect_database(loaded.paths.database)
        try:
            result = apply_migrations(
                connection,
                loaded.paths.database,
                loaded.paths.backups,
            )
        finally:
            connection.close()
    versions = ", ".join(str(version) for version in result.applied_versions) or "none"
    print(f"applied migrations: {versions}")
    if result.backup_path is not None:
        print(f"backup: {result.backup_path}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        arguments = parser.parse_args(argv)
        if arguments.command == "config" and arguments.config_command == "validate":
            return command_config_validate(arguments.config)
        if arguments.command == "paths":
            return command_paths(arguments.config)
        if arguments.command == "db" and arguments.db_command == "migrate":
            return command_db_migrate(arguments.config)
        raise UsageError("command is not implemented")
    except UsageError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return 64
    except (
        ConfigLoadError,
        MigrationError,
        LockUnavailable,
        OSError,
        sqlite3.Error,
    ) as error:
        print(error, file=sys.stderr)
        return 1


def entrypoint() -> NoReturn:
    raise SystemExit(main())
