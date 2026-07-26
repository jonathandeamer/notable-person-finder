from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import NoReturn
from zoneinfo import ZoneInfo

from notable_person_finder.config.loader import ConfigLoadError, ResolvedConfig, load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations
from notable_person_finder.obs.logging import configure_logging, log_event
from notable_person_finder.reporting.digest import (
    DigestRecord,
    DigestWriteError,
    write_digest,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import SystemClock, utc_timestamp
from notable_person_finder.runs.engine import (
    NonSettlingStateError,
    ReportArtifact,
    RunEngine,
    RunReport,
)
from notable_person_finder.runs.lock import LockUnavailable, MutationLock
from notable_person_finder.runs.models import RunState
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 2
EXIT_USAGE = 64
EXIT_INTERRUPTED = 130

_EXIT_BY_STATE = {
    RunState.COMPLETE: EXIT_OK,
    RunState.PARTIAL: EXIT_PARTIAL,
    RunState.FAILED: EXIT_FAILED,
    RunState.INTERRUPTED: EXIT_INTERRUPTED,
}


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
    commands.add_parser("run")
    commands.add_parser("status")
    db = commands.add_parser("db")
    db.add_subparsers(dest="db_command", required=True).add_parser("migrate")
    return parser


def command_config_validate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=True)
    print("configuration valid")
    print(f"fingerprint: {loaded.fingerprint}")
    print(f"main: {loaded.paths.config_file}")
    return EXIT_OK


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
    return EXIT_OK


def command_db_migrate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    with MutationLock(loaded.paths.lock_file):
        connection = connect_database(loaded.paths.database)
        try:
            result = apply_migrations(
                connection, loaded.paths.database, loaded.paths.backups
            )
        finally:
            connection.close()
    versions = ", ".join(str(version) for version in result.applied_versions) or "none"
    print(f"applied migrations: {versions}")
    if result.backup_path is not None:
        print(f"backup: {result.backup_path}")
    return EXIT_OK


def _window_start(loaded: ResolvedConfig, now: datetime) -> str:
    """The most recent local midnight, as the UTC text the run table stores."""
    local = now.astimezone(ZoneInfo(loaded.main.timezone))
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return utc_timestamp(start)


def command_run(config_file: Path | None, *, verbose: bool) -> int:
    loaded = load_config(config_file, require_secrets=True)
    clock = SystemClock()
    logger = configure_logging(
        loaded.paths.log_file,
        loaded.main.logging,
        secrets=(
            loaded.credentials.openrouter_api_key,
            loaded.credentials.brave_api_key,
        ),
    )

    with MutationLock(loaded.paths.lock_file):
        connection = connect_database(loaded.paths.database)
        try:
            apply_migrations(connection, loaded.paths.database, loaded.paths.backups)

            # One observation of the clock dates both the observation window
            # and the digest. Reading the clock twice would let a run that
            # starts a few milliseconds before local midnight file its digest
            # under the following day, and the digest file name is immutable.
            now = clock.now()
            window_start = _window_start(loaded, now)
            local_date = now.astimezone(ZoneInfo(loaded.main.timezone)).date().isoformat()
            # The transport and pacing gate will be built by the first adapter
            # milestone that has a provider to pace; nothing in this milestone
            # makes requests, so no transport is constructed yet.
            written: DigestRecord | None = None

            def report_run(report: RunReport) -> ReportArtifact:
                nonlocal written
                try:
                    written = write_digest(
                        loaded.paths.digests,
                        report,
                        local_date=local_date,
                        config=loaded.main.digest,
                    )
                except DigestWriteError:
                    log_event(logger, "run_reporting_failed", run_id=report.run_id)
                    raise
                return ReportArtifact(
                    path=str(written.path),
                    sha256=written.sha256,
                    markdown=written.markdown,
                )

            engine = RunEngine(
                connection,
                retry=RetryCoordinator(loaded.main.retry, clock=clock),
                scheduler=BoundedScheduler(loaded.main.concurrency.http_workers),
                clock=clock,
                timezone=loaded.main.timezone,
                window_start=window_start,
                budget_limit_nano_usd=loaded.main.budget.openrouter_nano_usd_per_run(),
                snapshot_fingerprint=loaded.fingerprint,
                snapshot_json=loaded.snapshot_json,
                reporter=report_run,
            )
            log_event(logger, "run_started", fingerprint=loaded.fingerprint)
            try:
                # No provider adapters exist in this milestone, so no task
                # handlers are registered. Milestones 3-6 supply them.
                report = engine.execute({})
            except NonSettlingStateError as error:
                repository.finish_run(
                    connection,
                    run_id=error.run_id,
                    state=RunState.INTERRUPTED,
                    reason=(
                        f"handler for {error.task_type!r} returned "
                        f"non-settling state {error.state!r}"
                    ),
                    digest_path=None,
                    digest_sha256=None,
                    now=utc_timestamp(clock.now()),
                )
                log_event(
                    logger,
                    "run_non_settling_state",
                    run_id=error.run_id,
                    task_type=error.task_type,
                    state=str(error.state),
                )
                return EXIT_FAILED

            if written is None:
                # The engine always calls the reporter before returning, so
                # this is unreachable today. It is an explicit raise rather
                # than an assert because `python -O` strips asserts, and the
                # stripped version would fail with an AttributeError on the
                # next line instead of a handled reporting failure.
                raise DigestWriteError("the run finished without writing a digest")

            log_event(
                logger,
                "run_finished",
                run_id=report.run_id,
                state=str(report.state),
                required_succeeded=report.counters.required_succeeded,
                required_deferred=report.counters.required_deferred,
                operational_failures=report.counters.operational_failures,
            )
            sys.stdout.write(written.markdown)
            if verbose:
                print(f"digest: {written.path}", file=sys.stderr)
            return _EXIT_BY_STATE[report.state]
        finally:
            connection.close()


def _run_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'run'"
        ).fetchone()
        is not None
    )


def command_status(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    if not loaded.paths.database.exists():
        print("no run has been recorded yet")
        return EXIT_OK

    connection = connect_database(loaded.paths.database, readonly=True)
    try:
        if not _run_schema_present(connection):
            # Reachable when a migration failed partway: the file exists but
            # the run tables do not. Reporting "no run yet" would read as an
            # empty but healthy data root, so name the fault and the fix.
            print(
                f"{loaded.paths.database} exists but has no run schema; "
                "apply migrations with 'notable db migrate'",
                file=sys.stderr,
            )
            return EXIT_FAILED
        record = repository.latest_run(connection)
        if record is None:
            print("no run has been recorded yet")
            return EXIT_OK
        failures = repository.operational_failures_for_run(
            connection, run_id=record.id
        )
        print(f"latest run: {record.human_id} ({record.state})")
        print(f"started: {record.started_at}")
        print(f"finished: {record.finished_at or '-'}")
        print(f"digest: {record.digest_path or '-'}")
        print(f"required work pending: {repository.pending_required(connection)}")
        print(f"required work deferred: {repository.deferred_required(connection)}")
        print(f"operational failures: {failures}")
        # Digest backlog, queue tiers, and the oldest pending candidate arrive
        # with the digest queue in the lead-assessment milestone.
        return EXIT_OK
    finally:
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        arguments = parser.parse_args(argv)
        if arguments.command == "config" and arguments.config_command == "validate":
            return command_config_validate(arguments.config)
        if arguments.command == "paths":
            return command_paths(arguments.config)
        if arguments.command == "run":
            return command_run(arguments.config, verbose=arguments.verbose)
        if arguments.command == "status":
            return command_status(arguments.config)
        if arguments.command == "db" and arguments.db_command == "migrate":
            return command_db_migrate(arguments.config)
        raise UsageError("command is not implemented")
    except UsageError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except (
        ConfigLoadError,
        MigrationError,
        LockUnavailable,
        DigestWriteError,
        OSError,
        sqlite3.Error,
    ) as error:
        print(error, file=sys.stderr)
        return EXIT_FAILED


def entrypoint() -> NoReturn:
    signal.signal(signal.SIGINT, signal.default_int_handler)
    raise SystemExit(main())
