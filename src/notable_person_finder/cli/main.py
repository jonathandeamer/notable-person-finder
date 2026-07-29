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

from notable_person_finder import __version__
from notable_person_finder.config.loader import (
    ConfigLoadError,
    ResolvedConfig,
    load_config,
)
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations
from notable_person_finder.ingestion.repository import source_item_counts
from notable_person_finder.ingestion.service import (
    FETCH_FEED_TASK_TYPE,
    build_fetch_handler,
    build_seed_hook,
)
from notable_person_finder.obs.logging import configure_logging, log_event
from notable_person_finder.providers.feeds import FeedparserClient
from notable_person_finder.providers.safety import SystemHostResolver
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.reporting.digest import (
    DigestRecord,
    DigestWriteError,
    IngestionSummary,
    write_digest,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import SystemClock, utc_timestamp
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    RunReport,
)
from notable_person_finder.runs.lock import LockUnavailable, MutationLock
from notable_person_finder.runs.models import RunState
from notable_person_finder.runs.retry import RetryPolicy
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
            local_date = (
                now.astimezone(ZoneInfo(loaded.main.timezone)).date().isoformat()
            )
            # The transport is owned by this command and closes before the
            # scheduler and database connection leave their scopes.
            written: DigestRecord | None = None

            def report_run(report: RunReport) -> ReportArtifact:
                nonlocal written
                ingestion = (
                    _ingestion_summary(connection, report.run_id)
                    if _ingestion_schema_present(connection)
                    else None
                )
                try:
                    written = write_digest(
                        loaded.paths.digests,
                        report,
                        local_date=local_date,
                        config=loaded.main.digest,
                        ingestion=ingestion,
                    )
                except DigestWriteError:
                    # `cli.` prefix, not `run.`: the engine already emits
                    # `run.reporting_failed` for the same failure, with a
                    # different field set (error_type, no run_id-only
                    # shape). Two emitters sharing one dotted name would
                    # make the name insufficient to know which schema
                    # applies without inspecting the payload -- so every
                    # CLI-originated event in this function is under its
                    # own `cli.` namespace instead.
                    log_event(logger, "cli.run_reporting_failed", run_id=report.run_id)
                    raise
                return ReportArtifact(
                    path=str(written.path),
                    sha256=written.sha256,
                    markdown=written.markdown,
                )

            # Scoped tightly around engine construction and execution -- the
            # only place the scheduler's worker pool is used -- so the pool
            # is always shut down before `connection.close()` runs in the
            # outer `finally`, on both the success and the exception path.
            # Closing the connection first would let a worker thread still
            # inside an HTTP call outlive the SQLite connection it will need
            # for `persist`.
            with (
                BoundedScheduler(loaded.main.concurrency.http_workers) as scheduler,
                build_transport(
                    loaded.main.transport,
                    version=__version__,
                    resolver=SystemHostResolver(),
                    clock=clock,
                ) as transport,
            ):
                client = FeedparserClient(transport)
                handlers = {
                    FETCH_FEED_TASK_TYPE: build_fetch_handler(
                        connection, client=client, feeds=loaded.feeds
                    )
                }
                seed = build_seed_hook(connection, feeds=loaded.feeds, clock=clock)

                engine = RunEngine(
                    connection,
                    retry=RetryPolicy(loaded.main.retry, clock=clock),
                    scheduler=scheduler,
                    clock=clock,
                    timezone=loaded.main.timezone,
                    window_start=window_start,
                    budget_limit_nano_usd=(
                        loaded.main.budget.openrouter_nano_usd_per_run()
                    ),
                    snapshot_fingerprint=loaded.fingerprint,
                    snapshot_json=loaded.snapshot_json,
                    reporter=report_run,
                )
                # `cli.run_started`, not `run.started`: the engine emits its own
                # `run.started` with a disjoint field set (run_id, window_start,
                # window_end, swept_runs, task_types). Reusing that name here
                # would give one dotted event name two schemas in the same log,
                # which is the defect the dotted rename was meant to remove, not
                # reintroduce in a different shape.
                log_event(logger, "cli.run_started", fingerprint=loaded.fingerprint)
                report = engine.execute(handlers, seed=seed)

            if written is None:
                # The engine always calls the reporter before returning, so
                # this is unreachable today. It is an explicit raise rather
                # than an assert because `python -O` strips asserts, and the
                # stripped version would fail with an AttributeError on the
                # next line instead of a handled reporting failure.
                raise DigestWriteError("the run finished without writing a digest")

            # `cli.run_finished`, not `run.finished`: same reasoning as
            # `cli.run_started` above -- the engine already emits
            # `run.finished` with its own field set for the same moment.
            log_event(
                logger,
                "cli.run_finished",
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


def _ingestion_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'feed_identity'"
        ).fetchone()
        is not None
    )


def _ingestion_summary(
    connection: sqlite3.Connection, run_id: int
) -> IngestionSummary | None:
    """Counts for the digest's ingestion section, or None before migration 0003."""
    if not _ingestion_schema_present(connection):
        return None
    # A feed can have more than one work item in a run (for example after a
    # URL change). The digest is one final outcome per feed identity, so `id`
    # -- the settlement order -- selects the newest row without relying on
    # timestamps that may tie.
    row = connection.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN outcome = 'modified' THEN 1 ELSE 0 END), 0)
                AS modified,
            COALESCE(SUM(CASE WHEN outcome = 'not_modified' THEN 1 ELSE 0 END), 0)
                AS not_modified,
            COALESCE(SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END), 0) AS failed
        FROM feed_fetch
        WHERE run_id = ?
          AND id IN (
              SELECT MAX(id)
              FROM feed_fetch
              WHERE run_id = ?
              GROUP BY feed_identity_id
          )
        """,
        (run_id, run_id),
    ).fetchone()
    counts = source_item_counts(connection, run_id=run_id)
    # `canonical_article` has no run column. An article is first associated
    # with its only source item in the same settlement transaction that
    # creates it, so this association identifies the current run's creates
    # without presenting the corpus-wide article count as a delta.
    articles_created = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM canonical_article article
            WHERE EXISTS (
                SELECT 1
                FROM source_item item
                WHERE item.canonical_article_id = article.id
                  AND item.discovered_by_run_id = ?
            )
            """,
            (run_id,),
        ).fetchone()["n"]
    )
    return IngestionSummary(
        feeds_fetched=int(row["modified"]),
        feeds_not_modified=int(row["not_modified"]),
        feeds_failed=int(row["failed"]),
        source_items_created=counts.created_in_run,
        articles_created=articles_created,
    )


def _latest_successful_fetches(
    connection: sqlite3.Connection,
) -> list[tuple[str, str | None]]:
    """The latest non-failed fetch time for every configured feed identity."""
    rows = connection.execute(
        """
        SELECT fi.key, MAX(ff.requested_at) AS latest_requested_at
        FROM feed_identity fi
        LEFT JOIN feed_fetch ff
            ON ff.feed_identity_id = fi.id AND ff.outcome <> 'failed'
        GROUP BY fi.key
        ORDER BY fi.key
        """
    ).fetchall()
    return [(row["key"], row["latest_requested_at"]) for row in rows]


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
        failures = repository.operational_failures_for_run(connection, run_id=record.id)
        print(f"latest run: {record.human_id} ({record.state})")
        print(f"started: {record.started_at}")
        print(f"finished: {record.finished_at or '-'}")
        print(f"digest: {record.digest_path or '-'}")
        print(f"required work pending: {repository.pending_required(connection)}")
        print(f"required work deferred: {repository.deferred_required(connection)}")
        print(f"operational failures: {failures}")
        if _ingestion_schema_present(connection):
            counts = source_item_counts(connection, run_id=record.id)
            print(f"source items: {counts.total}")
            articles = int(
                connection.execute(
                    "SELECT COUNT(*) AS n FROM canonical_article"
                ).fetchone()["n"]
            )
            print(f"articles: {articles}")
            fetches = _latest_successful_fetches(connection)
            if fetches:
                print("latest successful fetch:")
                for key, latest in fetches:
                    print(f"  {key}: {latest or '-'}")
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
