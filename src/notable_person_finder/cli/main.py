from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
from collections.abc import Callable, Sequence
from datetime import datetime
from pathlib import Path
from typing import NoReturn
from zoneinfo import ZoneInfo

from notable_person_finder import __version__
from notable_person_finder.audit.digest_show import (
    DigestLookupError,
    locate_digest,
    read_verified_digest,
)
from notable_person_finder.audit.render import render_run_audit
from notable_person_finder.audit.repository import load_run_audit
from notable_person_finder.config.loader import (
    ConfigLoadError,
    ResolvedConfig,
    load_config,
)
from notable_person_finder.config.models import DomainProfileConfig, MainConfig
from notable_person_finder.coverage.repository import (
    coverage_corpus_counts,
    coverage_run_counts,
)
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.coverage.service import (
    ASSESS_ARTICLE_TASK_TYPE,
    BRAVE_WEB_SEARCH_TASK_TYPE,
    FETCH_ARTICLE_TASK_TYPE,
    build_assess_article_handler,
    build_brave_web_search_handler,
    build_fetch_article_handler,
    count_coverage_research_eligible,
    seed_coverage_research,
)
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations
from notable_person_finder.ingestion.repository import source_item_counts
from notable_person_finder.ingestion.service import (
    FETCH_FEED_TASK_TYPE,
    build_fetch_handler,
    build_seed_hook,
)
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.ranking import QueueEntry, rank_key
from notable_person_finder.leads.repository import (
    ShortlistCandidate,
    compensate_queue_flow_for_pending_emission,
    count_attention_signals_for_lead,
    emit_shortlist_entries,
    fetch_pending_shortlist_candidates,
    fetch_qualifying_sources_for_lead,
    fetch_queue_flow_counts,
    fetch_signal_claims_for_person,
    record_digest_with_entries,
    shift_utc_days,
)
from notable_person_finder.leads.service import (
    AGGREGATE_PERSON_LEAD_TASK_TYPE,
    build_aggregate_person_lead_handler,
    seed_lead_aggregation,
)
from notable_person_finder.obs.logging import configure_logging, log_event
from notable_person_finder.people.repository import (
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    identity_corpus_counts,
    identity_run_counts,
    triage_corpus_counts,
    triage_run_counts,
)
from notable_person_finder.people.service import (
    DETECT_PEOPLE_TASK_TYPE,
    INSPECT_MODEL_TASK_TYPE,
    build_detection_handler,
    build_inspection_handler,
    build_reconsideration_handler,
    build_resolution_handler,
    count_resolution_eligible_mentions,
    ensure_model_inspections_for_run,
    schedule_source_items,
    seed_unresolved_mentions,
    seed_untriaged,
)
from notable_person_finder.providers.articles import (
    HttpxArticleFetcher,
    TrafilaturaArticleExtractor,
)
from notable_person_finder.providers.brave import HttpxBraveWebSearchClient
from notable_person_finder.providers.feeds import FeedparserClient
from notable_person_finder.providers.mediawiki import HttpxMediaWikiClient
from notable_person_finder.providers.openrouter import OpenRouterClient
from notable_person_finder.providers.pacing import build_pacing_gate
from notable_person_finder.providers.safety import SystemHostResolver
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.reporting.digest import (
    CoverageSummary,
    DigestRecord,
    DigestWriteError,
    IdentityRunSummary,
    IngestionSummary,
    PeopleRunSummary,
    QueueFlowSummary,
    ShortlistEntry,
    WikipediaRunSummary,
    write_digest,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import Clock, SystemClock, utc_timestamp
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    RunReport,
)
from notable_person_finder.runs.lock import LockUnavailable, MutationLock
from notable_person_finder.runs.models import RunState, WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import (
    BoundedScheduler,
    SchedulerSet,
    WorkerPool,
)
from notable_person_finder.wikipedia.repository import (
    wikipedia_corpus_counts,
    wikipedia_run_counts,
)
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
    MEDIAWIKI_SEARCH_TASK_TYPE,
    build_match_wikipedia_handler,
    build_mediawiki_page_facts_handler,
    build_mediawiki_search_handler,
    count_wikipedia_match_eligible,
    seed_wikipedia_identity,
)

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
    digest = commands.add_parser("digest")
    digest_show = digest.add_subparsers(
        dest="digest_command", required=True
    ).add_parser("show")
    digest_show.add_argument("run_id", nargs="?", default=None)
    audit = commands.add_parser("audit")
    audit_commands = audit.add_subparsers(dest="audit_command", required=True)
    audit_run = audit_commands.add_parser("run")
    audit_run.add_argument("run_id")
    audit_run.add_argument("--attempt", dest="attempt_id", default=None)
    return parser


def _parse_run_id_argument(value: str) -> int:
    """Accept `12` or `run-12`. Raises ValueError otherwise."""
    text = value.removeprefix("run-")
    return int(text)


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


def _compose_seed(
    connection: sqlite3.Connection,
    *,
    loaded: ResolvedConfig,
    policy: SourcePolicy,
    clock: Clock,
) -> Callable[[int], None]:
    """Feed seeding plus People backfill of every still-untriaged source item."""
    feed_seed = build_seed_hook(connection, feeds=loaded.feeds, clock=clock)
    profile = loaded.domain_profile
    config = loaded.main

    def seed(run_id: int) -> None:
        now = utc_timestamp(clock.now())
        feed_seed(run_id)
        seed_untriaged(
            connection,
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
        )
        seed_unresolved_mentions(
            connection,
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
        )
        seed_wikipedia_identity(
            connection,
            run_id=run_id,
            config=config,
            now=now,
        )
        seed_coverage_research(
            connection,
            run_id=run_id,
            config=config,
            policy=policy,
            now=now,
        )
        seed_lead_aggregation(
            connection,
            run_id=run_id,
            config=config,
            now=now,
        )
        ensure_model_inspections_for_run(
            connection,
            run_id=run_id,
            config=config,
            now=now,
        )

    return seed


def _on_source_items_callback(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> Callable[[tuple[int, ...], int, str], None]:
    """Inject schedule_source_items into ingestion without people importing feeds."""

    def on_source_items(
        source_item_ids: tuple[int, ...], run_id: int, now: str
    ) -> None:
        schedule_source_items(
            connection,
            source_item_ids=source_item_ids,
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
        )

    return on_source_items


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
            # Provider clients are entered before the dual schedulers so reverse
            # context-manager exit drains every in-flight HTTP and LLM worker
            # before either client or SQLite closes.
            written: DigestRecord | None = None

            def report_run(report: RunReport) -> ReportArtifact:
                nonlocal written
                ingestion = (
                    _ingestion_summary(connection, report.run_id)
                    if _ingestion_schema_present(connection)
                    else None
                )
                people = (
                    _people_summary(connection, report)
                    if _people_schema_present(connection)
                    else None
                )
                identity = (
                    _identity_summary(connection, report, config=loaded.main)
                    if _identity_schema_present(connection)
                    else None
                )
                wikipedia = (
                    _wikipedia_summary(
                        connection,
                        report,
                        config=loaded.main,
                        now=utc_timestamp(clock.now()),
                    )
                    if _wikipedia_schema_present(connection)
                    else None
                )
                coverage = (
                    _coverage_summary(
                        connection,
                        report,
                        config=loaded.main,
                        policy=loaded.source_policy,
                        now=utc_timestamp(clock.now()),
                    )
                    if _coverage_schema_present(connection)
                    else None
                )
                leads_now = utc_timestamp(clock.now())
                leads = (
                    _leads_summary(
                        connection,
                        report,
                        config=loaded.main,
                        now=leads_now,
                    )
                    if _leads_schema_present(connection)
                    else None
                )
                shortlist_entries, queue_flow, limited_candidates = (
                    leads
                    if leads is not None
                    else (
                        None,
                        None,
                        None,
                    )
                )
                try:
                    written = write_digest(
                        loaded.paths.digests,
                        report,
                        local_date=local_date,
                        config=loaded.main.digest,
                        ingestion=ingestion,
                        people=people,
                        identity=identity,
                        wikipedia=wikipedia,
                        coverage=coverage,
                        shortlist_entries=shortlist_entries,
                        queue_flow=queue_flow,
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
                if leads is not None:
                    # Deliberately after write_digest succeeds, and in one
                    # transaction: emitting the shortlist's pending ->
                    # emitted transition BEFORE the digest file was durably
                    # written left a crash window where those digest_queue
                    # rows were already committed 'emitted' with no digest
                    # file or `digest`/`digest_entry` row ever recording
                    # them, so they vanished from the queue forever. Doing
                    # both in one committed transaction here collapses that
                    # window to a single commit; on a crash between
                    # write_digest and this commit, nothing is marked
                    # emitted and nothing is lost -- the entries simply
                    # remain pending for the next run. Always runs when the
                    # leads schema is present, even with an empty
                    # shortlist (`limited_candidates == []`), so `digest` is
                    # a complete history of every digest actually written,
                    # not only the ones with a non-empty shortlist.
                    assert limited_candidates is not None
                    connection.execute("BEGIN IMMEDIATE")
                    try:
                        emitted = emit_shortlist_entries(
                            connection,
                            run_id=report.run_id,
                            occurred_at=leads_now,
                            candidates=limited_candidates,
                        )
                        record_digest_with_entries(
                            connection,
                            run_id=report.run_id,
                            file_path=str(written.path),
                            timezone=loaded.main.timezone,
                            window_start=report.window_start,
                            window_end=report.window_end,
                            run_state=str(report.state),
                            content_hash=written.sha256,
                            created_at=leads_now,
                            emitted=emitted,
                        )
                    except BaseException:
                        connection.rollback()
                        raise
                    else:
                        connection.commit()
                return ReportArtifact(
                    path=str(written.path),
                    sha256=written.sha256,
                    markdown=written.markdown,
                )

            # Scoped tightly around engine construction and execution so both
            # pools always shut down before provider clients and before
            # `connection.close()` in the outer `finally`, on success and
            # exception paths alike.
            pacing_gate = build_pacing_gate(
                loaded.main.pacing,
                loaded.main.concurrency,
                clock=clock,
            )
            openrouter_key = loaded.credentials.openrouter_api_key
            if openrouter_key is None:
                # require_secrets=True already refuses a missing key; this
                # narrows the type for the client constructor.
                raise ConfigLoadError(("openrouter_api_key is required for run",))
            brave_key = loaded.credentials.brave_api_key
            if brave_key is None:
                raise ConfigLoadError(("brave_api_key is required for run",))
            # Task 6b: Brave search + fetch_article. assess_article / coverage
            # seed land in Tasks 7–8.
            with (
                build_transport(
                    loaded.main.transport,
                    version=__version__,
                    resolver=SystemHostResolver(),
                    clock=clock,
                    pacing_gate=pacing_gate,
                ) as transport,
                OpenRouterClient(
                    api_key=openrouter_key,
                    endpoint=loaded.main.openrouter.endpoint,
                    routing=loaded.main.openrouter.routing,
                    timeout_seconds=loaded.main.transport.llm_read_timeout_seconds,
                    clock=clock,
                ) as llm_client,
                SchedulerSet(
                    {
                        WorkerPool.HTTP: BoundedScheduler(
                            loaded.main.concurrency.http_workers
                        ),
                        WorkerPool.LLM: BoundedScheduler(
                            loaded.main.concurrency.llm_workers
                        ),
                    }
                ) as scheduler,
            ):
                feed_client = FeedparserClient(transport)
                match_cfg = loaded.main.tasks.match_wikipedia_identity
                mediawiki_client = HttpxMediaWikiClient(
                    transport,
                    config=loaded.main.mediawiki,
                    srlimit=match_cfg.search_srlimit,
                    max_extract_characters=match_cfg.max_extract_characters,
                    max_categories_per_page=match_cfg.max_categories_per_page,
                    clock=clock,
                )
                brave_client = HttpxBraveWebSearchClient(
                    transport,
                    config=loaded.main.brave,
                    api_key=brave_key,
                    clock=clock,
                )
                article_fetcher = HttpxArticleFetcher(transport, clock=clock)
                article_extractor = TrafilaturaArticleExtractor()
                on_source_items = _on_source_items_callback(
                    connection,
                    config=loaded.main,
                    profile=loaded.domain_profile,
                )
                handlers = {
                    FETCH_FEED_TASK_TYPE: build_fetch_handler(
                        connection,
                        client=feed_client,
                        feeds=loaded.feeds,
                        on_source_items=on_source_items,
                    ),
                    MEDIAWIKI_SEARCH_TASK_TYPE: build_mediawiki_search_handler(
                        connection,
                        client=mediawiki_client,
                        config=loaded.main,
                    ),
                    MEDIAWIKI_PAGE_FACTS_TASK_TYPE: build_mediawiki_page_facts_handler(
                        connection,
                        client=mediawiki_client,
                        config=loaded.main,
                    ),
                    BRAVE_WEB_SEARCH_TASK_TYPE: build_brave_web_search_handler(
                        connection,
                        client=brave_client,
                        config=loaded.main,
                        policy=loaded.source_policy,
                    ),
                    FETCH_ARTICLE_TASK_TYPE: build_fetch_article_handler(
                        connection,
                        fetcher=article_fetcher,
                        extractor=article_extractor,
                        config=loaded.main,
                        policy=loaded.source_policy,
                    ),
                    INSPECT_MODEL_TASK_TYPE: build_inspection_handler(
                        connection, client=llm_client, config=loaded.main
                    ),
                    ASSESS_ARTICLE_TASK_TYPE: build_assess_article_handler(
                        connection,
                        client=llm_client,
                        config=loaded.main,
                        profile=loaded.domain_profile,
                    ),
                    DETECT_PEOPLE_TASK_TYPE: build_detection_handler(
                        connection,
                        client=llm_client,
                        config=loaded.main,
                        profile=loaded.domain_profile,
                    ),
                    RESOLVE_PERSON_ENTITY_TASK_TYPE: build_resolution_handler(
                        connection,
                        client=llm_client,
                        config=loaded.main,
                        profile=loaded.domain_profile,
                    ),
                    RECONSIDER_PERSON_ENTITY_TASK_TYPE: build_reconsideration_handler(
                        connection,
                        client=llm_client,
                        config=loaded.main,
                        profile=loaded.domain_profile,
                    ),
                    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE: build_match_wikipedia_handler(
                        connection,
                        client=llm_client,
                        config=loaded.main,
                    ),
                    AGGREGATE_PERSON_LEAD_TASK_TYPE: (
                        build_aggregate_person_lead_handler(
                            connection,
                            config=loaded.main,
                            policy=loaded.source_policy,
                        )
                    ),
                }
                seed = _compose_seed(
                    connection, loaded=loaded, policy=loaded.source_policy, clock=clock
                )

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


def _people_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'triage_observation'"
        ).fetchone()
        is not None
    )


def _identity_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'person'"
        ).fetchone()
        is not None
    )


def _wikipedia_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'wikipedia_identity_observation'"
        ).fetchone()
        is not None
    )


def _coverage_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'person_coverage_plan'"
        ).fetchone()
        is not None
    )


def _leads_schema_present(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'digest_queue'"
        ).fetchone()
        is not None
    )


def _leads_summary(
    connection: sqlite3.Connection,
    report: RunReport,
    *,
    config: MainConfig,
    now: str,
) -> tuple[list[ShortlistEntry], QueueFlowSummary, list[ShortlistCandidate]] | None:
    """Ranked shortlist entries, queue-flow counters, and the candidates
    selected into this digest's shortlist (still `pending`), or None pre-0008.

    This function only ranks and reads -- it does NOT transition any
    `digest_queue` row `pending -> emitted`. That transition, together with
    recording the `digest`/`digest_entry` rows, is the caller's
    (`command_run`'s `report_run`) job, and deliberately happens only after
    `write_digest` has durably written the digest file: emitting first and
    writing the file second left a crash window where entries were
    committed `emitted` with no digest file or `digest` row ever recording
    them, so they vanished from the queue forever. Deferring emission here
    means this run's own emission has not yet been committed when
    `fetch_queue_flow_counts` runs below, so the returned `QueueFlowSummary`
    adds `len(limited)` to the DB-read "emitted" count locally so the
    rendered "Emitted" line still reflects this run's own shortlist.
    """
    if not _leads_schema_present(connection):
        return None

    lead_cfg = config.tasks.aggregate_lead
    starvation_cutoff = shift_utc_days(now, -lead_cfg.starvation_days)

    candidates = fetch_pending_shortlist_candidates(connection)
    ranked: list[
        tuple[
            tuple[object, ...],
            ShortlistCandidate,
            list[tuple[str, str, str, str, str, str]],
        ]
    ] = []
    for candidate in candidates:
        sources = fetch_qualifying_sources_for_lead(
            connection, lead_assessment_id=candidate.lead_assessment_id
        )
        positive_signal_count = count_attention_signals_for_lead(
            connection, lead_assessment_id=candidate.lead_assessment_id
        )
        # Best (lowest-rank) access_kind among this lead's qualifying
        # sources; "snippets" (the plural CHECK value) falls through to
        # rank_key's unrecognized-visibility default, same as any other
        # value ranking.py's _EVIDENCE_VISIBILITY_RANK does not name.
        visibilities = [source[5] for source in sources]
        best_visibility = min(
            visibilities,
            key=lambda v: {"full": 0, "partial": 1, "snippet": 2}.get(v, 3),
            default="",
        )
        freshest_at = max((source[2] for source in sources), default=None)
        if freshest_at == "-":
            freshest_at = None
        entry_key = QueueEntry(
            person_id=candidate.person_id,
            eligibility_reason=candidate.eligibility_reason,
            first_pending_at=candidate.first_pending_at,
        )
        lead_outcome = LeadOutcome(
            outcome=candidate.outcome,
            qualifying_domain_count=candidate.qualifying_domain_count,
            qualifying_articles=(),
            contributing_signals=(),
            wikipedia_outcome=candidate.wikipedia_outcome,
        )
        key = rank_key(
            entry_key,
            lead_outcome,
            positive_signal_count=positive_signal_count,
            best_evidence_visibility=best_visibility,
            freshest_qualifying_article_at=freshest_at,
            starvation_cutoff=starvation_cutoff,
        )
        ranked.append((key, candidate, sources))

    ranked.sort(key=lambda item: item[0])
    limited = ranked[: lead_cfg.digest_limit]
    limited_candidates = [candidate for _key, candidate, _sources in limited]

    # Entries ranked beyond digest_limit are never in `limited` and so keep
    # their 'pending' status untouched, preserving the existing digest-
    # limit-omission-retains-eligibility rule. The actual 'pending' ->
    # 'emitted' transition for `limited_candidates` happens in the caller,
    # after `write_digest` succeeds (see this function's docstring).

    shortlist_entries: list[ShortlistEntry] = []
    for _key, candidate, sources in limited:
        attention = fetch_signal_claims_for_person(
            connection, person_id=candidate.person_id, signal_kind="attention"
        )
        caution = fetch_signal_claims_for_person(
            connection, person_id=candidate.person_id, signal_kind="caution"
        )
        shortlist_entries.append(
            ShortlistEntry(
                person_id=candidate.person_id,
                display_name=candidate.display_name,
                outcome=candidate.outcome,
                eligibility_reason=candidate.eligibility_reason,
                wikipedia_outcome=candidate.wikipedia_outcome,
                qualifying_domain_count=candidate.qualifying_domain_count,
                qualifying_sources=tuple(sources),
                attention_signals=tuple(attention),
                caution_signals=tuple(caution),
                unresolved_issues=(),
            )
        )

    counts = fetch_queue_flow_counts(connection, run_id=report.run_id, now=now)
    # This run's own emission is deliberately not yet committed at this
    # point (see docstring), so `counts.emitted` -- read from already-
    # committed `queue_transition` rows -- does not yet include it. Add
    # `len(limited_candidates)` locally so the rendered "Emitted" line for
    # this run's own digest still reflects the entries this digest is about
    # to emit.
    emitted_for_render = counts.emitted + len(limited_candidates)
    # For the same reason, `counts.ending_backlog_*` and
    # `counts.oldest_pending_days` were read while every candidate in
    # `limited_candidates` was still `status = 'pending'`, so they overstate
    # the backlog this digest actually leaves behind (and may report the
    # about-to-be-emitted top-ranked candidate as the "oldest pending"
    # entry). Compensate locally using the already-known shortlist, the same
    # way `emitted_for_render` compensates the emission count above.
    counts = compensate_queue_flow_for_pending_emission(
        counts,
        all_pending_candidates=candidates,
        limited_candidates=limited_candidates,
        now=now,
    )
    net_queue_growth = (
        counts.newly_queued - emitted_for_render - counts.removed_matching_wikipedia
    )
    # Conservative by design (Global Constraint, K-series lead spec): only
    # project a clear time when there is a nonzero trailing emission rate and
    # an actual backlog to clear against it. Any other case -- no rate
    # history, a zero rate, or an empty backlog -- reports "not clearing"
    # rather than fabricating a number.
    backlog_total = counts.ending_backlog_promising + counts.ending_backlog_possible
    estimated_clear_days: int | None = None
    if (
        counts.emission_rate_7d is not None
        and counts.emission_rate_7d > 0
        and backlog_total > 0
    ):
        estimated_clear_days = round(backlog_total / counts.emission_rate_7d)

    queue_flow = QueueFlowSummary(
        newly_queued=counts.newly_queued,
        emitted=emitted_for_render,
        removed_matching_wikipedia=counts.removed_matching_wikipedia,
        ending_backlog_promising=counts.ending_backlog_promising,
        ending_backlog_possible=counts.ending_backlog_possible,
        arrival_rate_7d=counts.arrival_rate_7d,
        emission_rate_7d=counts.emission_rate_7d,
        net_queue_growth=net_queue_growth,
        oldest_pending_days=counts.oldest_pending_days,
        estimated_clear_days=estimated_clear_days,
    )
    return shortlist_entries, queue_flow, limited_candidates


def _model_work_counts(
    connection: sqlite3.Connection, *, run_id: int
) -> tuple[int, int]:
    """Deferred and permanently failed LLM work settled by this run.

    ``complete_work`` always clears ``claimed_by_run_id`` and stamps
    ``completed_by_run_id`` for every terminal state including deferred, so
    both arms attribute via ``completed_by_run_id``.
    """
    deferred = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM work_item
             WHERE task_type IN (?, ?)
               AND state = ?
               AND completed_by_run_id = ?
            """,
            (
                INSPECT_MODEL_TASK_TYPE,
                DETECT_PEOPLE_TASK_TYPE,
                str(WorkState.DEFERRED),
                run_id,
            ),
        ).fetchone()["n"]
    )
    failed = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM work_item
             WHERE task_type IN (?, ?)
               AND state = ?
               AND completed_by_run_id = ?
            """,
            (
                INSPECT_MODEL_TASK_TYPE,
                DETECT_PEOPLE_TASK_TYPE,
                str(WorkState.FAILED_PERMANENT),
                run_id,
            ),
        ).fetchone()["n"]
    )
    return deferred, failed


def _people_summary(
    connection: sqlite3.Connection, report: RunReport
) -> PeopleRunSummary | None:
    """Counts for the digest's person-detection section, or None pre-migration."""
    if not _people_schema_present(connection):
        return None
    counts = triage_run_counts(connection, run_id=report.run_id)
    model_deferred, model_failed = _model_work_counts(connection, run_id=report.run_id)
    return PeopleRunSummary(
        source_items_triaged=counts.observations,
        research_people=counts.research_people,
        do_not_research=counts.do_not_research,
        uncertain=counts.uncertain,
        research_or_uncertain_mentions=counts.research_or_uncertain_mentions,
        overflow=counts.overflow,
        insufficient_input=counts.insufficient_input,
        model_deferred=model_deferred,
        model_failed=model_failed,
        model_failed_by_category=dict(counts.failed_by_category),
        budget_limit_nano_usd=report.budget_limit_nano_usd,
        budget_reserved_nano_usd=report.budget_reserved_nano_usd,
        budget_actual_nano_usd=report.budget_actual_nano_usd,
    )


def _identity_summary(
    connection: sqlite3.Connection,
    report: RunReport,
    *,
    config: MainConfig,
) -> IdentityRunSummary | None:
    """Counts for the digest's person-identity section, or None pre-migration."""
    if not _identity_schema_present(connection):
        return None
    counts = identity_run_counts(connection, run_id=report.run_id)
    unresolved_eligible = count_resolution_eligible_mentions(connection, config=config)
    return IdentityRunSummary(
        people_created=counts.people_created,
        mentions_resolved=counts.mentions_resolved,
        linked_same_person=counts.linked_same_person,
        created_via_different_people=counts.created_via_different_people,
        created_via_created_new=counts.created_via_created_new,
        uncertain=counts.uncertain,
        unresolved_eligible_mentions=unresolved_eligible,
        active_possible_same_person=counts.active_possible_same_person,
        confirmed_merges=counts.confirmed_merges,
        resolution_model_deferred=counts.resolution_model_deferred,
        resolution_model_failed=counts.resolution_model_failed,
    )


def _wikipedia_summary(
    connection: sqlite3.Connection,
    report: RunReport,
    *,
    config: MainConfig,
    now: str,
) -> WikipediaRunSummary | None:
    """Counts for the digest's Wikipedia identity section, or None pre-0006."""
    if not _wikipedia_schema_present(connection):
        return None
    run_counts = wikipedia_run_counts(connection, run_id=report.run_id)
    corpus = wikipedia_corpus_counts(connection)
    eligible = count_wikipedia_match_eligible(connection, config=config, now=now)
    return WikipediaRunSummary(
        matching_this_run=run_counts.matching_this_run,
        no_match_this_run=run_counts.no_match_this_run,
        uncertain_this_run=run_counts.uncertain_this_run,
        deterministic_no_match_this_run=run_counts.deterministic_no_match_this_run,
        current_matching=corpus.current_matching,
        current_no_match=corpus.current_no_match,
        current_uncertain=corpus.current_uncertain,
        eligible_remaining=eligible,
        without_pointer=corpus.without_pointer,
        mediawiki_deferred=run_counts.mediawiki_deferred,
        mediawiki_failed=run_counts.mediawiki_failed,
        match_model_deferred=run_counts.match_model_deferred,
        match_model_failed=run_counts.match_model_failed,
    )


def _coverage_summary(
    connection: sqlite3.Connection,
    report: RunReport,
    *,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> CoverageSummary | None:
    if not _coverage_schema_present(connection):
        return None
    corpus = coverage_corpus_counts(connection)
    run = coverage_run_counts(connection, run_id=report.run_id)
    eligible = count_coverage_research_eligible(
        connection, config=config, policy=policy, now=now
    )
    return CoverageSummary(
        plans_completed=run.plans_completed,
        plans_incomplete=run.plans_incomplete,
        plans_failed=run.plans_failed,
        assessments_completed_this_run=run.assessments_completed_this_run,
        people_with_completed_assessment=corpus.people_with_completed_assessment,
        eligible_remaining=eligible,
        stopped_matching_wikipedia=corpus.stopped_matching_wikipedia,
        model_deferred=run.model_deferred,
        model_failed=run.model_failed,
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
        if _people_schema_present(connection):
            triage = triage_corpus_counts(connection)
            print(f"source items triaged: {triage.triaged}")
            print(f"source items untriaged: {triage.untriaged}")
            print(f"research: {triage.research_people}")
            print(f"uncertain: {triage.uncertain}")
            print(f"do not research: {triage.do_not_research}")
            print(f"insufficient input: {triage.insufficient_input}")
            print(f"failed triage: {triage.failed}")
            print(
                "unresolved research or uncertain mentions: "
                f"{triage.research_or_uncertain_mentions}"
            )
        if _identity_schema_present(connection):
            identity = identity_corpus_counts(connection)
            print(f"canonical people: {identity.canonical_people}")
            print(f"merged-away people: {identity.merged_away_people}")
            eligible = count_resolution_eligible_mentions(
                connection, config=loaded.main
            )
            print(f"unresolved eligible mentions: {eligible}")
            print(
                "active possible_same_person relations: "
                f"{identity.active_possible_same_person}"
            )
            print(f"mentions linked to people: {identity.mentions_linked_to_people}")
        if _wikipedia_schema_present(connection):
            wiki = wikipedia_corpus_counts(connection)
            print(f"people with current matching page: {wiki.current_matching}")
            print(f"people with current no-match: {wiki.current_no_match}")
            print(f"people with current uncertain identity: {wiki.current_uncertain}")
            wiki_eligible = count_wikipedia_match_eligible(
                connection,
                config=loaded.main,
                now=utc_timestamp(SystemClock().now()),
            )
            print(f"wikipedia eligible remaining: {wiki_eligible}")
            print(f"canonical people without wikipedia pointer: {wiki.without_pointer}")
        if _coverage_schema_present(connection):
            cov_corpus = coverage_corpus_counts(connection)
            cov_eligible = count_coverage_research_eligible(
                connection,
                config=loaded.main,
                policy=loaded.source_policy,
                now=utc_timestamp(SystemClock().now()),
            )
            print(
                "people with completed assessment (corpus): "
                f"{cov_corpus.people_with_completed_assessment}"
            )
            print(f"coverage eligible remaining: {cov_eligible}")
            print(
                f"stopped matching wikipedia: {cov_corpus.stopped_matching_wikipedia}"
            )
        if _leads_schema_present(connection):
            backlog_rows = connection.execute(
                """
                SELECT tier, COUNT(*)
                FROM digest_queue
                WHERE status = 'pending'
                GROUP BY tier
                """
            ).fetchall()
            backlog_by_tier = {tier: count for tier, count in backlog_rows}
            promising = backlog_by_tier.get("promising_lead", 0)
            possible = backlog_by_tier.get("possible_lead", 0)
            print(
                f"digest backlog: {promising} promising_lead, {possible} possible_lead"
            )
            oldest_pending = connection.execute(
                "SELECT MIN(first_pending_at) FROM digest_queue "
                "WHERE status = 'pending'"
            ).fetchone()[0]
            if oldest_pending is None:
                print("oldest pending candidate: none")
            else:
                print(f"oldest pending candidate: {oldest_pending}")
        return EXIT_OK
    finally:
        connection.close()


def command_digest_show(
    config_file: Path | None, *, run_id_argument: str | None
) -> int:
    loaded = load_config(config_file, require_secrets=False)
    if run_id_argument is not None:
        try:
            run_id = _parse_run_id_argument(run_id_argument)
        except ValueError:
            print(f"invalid run id: {run_id_argument}", file=sys.stderr)
            return EXIT_USAGE
    else:
        run_id = None
    if not loaded.paths.database.exists():
        print("no run has been recorded yet", file=sys.stderr)
        return EXIT_FAILED
    connection = connect_database(loaded.paths.database, readonly=True)
    try:
        location = locate_digest(connection, run_id=run_id)
        body = read_verified_digest(location)
    except DigestLookupError as error:
        print(str(error), file=sys.stderr)
        return EXIT_FAILED
    finally:
        connection.close()
    sys.stdout.write(body.decode("utf-8"))
    return EXIT_OK


def command_audit_run(
    config_file: Path | None,
    *,
    run_id_argument: str,
    attempt_id_argument: str | None,
) -> int:
    # attempt_id_argument is accepted now and ignored; Task 4 implements the
    # `--attempt` drill-down without changing this signature.
    del attempt_id_argument
    loaded = load_config(config_file, require_secrets=False)
    try:
        run_id = _parse_run_id_argument(run_id_argument)
    except ValueError:
        print(f"invalid run id: {run_id_argument}", file=sys.stderr)
        return EXIT_USAGE
    if not loaded.paths.database.exists():
        print("no run has been recorded yet", file=sys.stderr)
        return EXIT_FAILED
    connection = connect_database(loaded.paths.database, readonly=True)
    try:
        audit = load_run_audit(connection, run_id=run_id)
    finally:
        connection.close()
    if audit is None:
        print(f"no run found with id {run_id}", file=sys.stderr)
        return EXIT_FAILED
    sys.stdout.write(render_run_audit(audit))
    return EXIT_OK


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
        if arguments.command == "digest" and arguments.digest_command == "show":
            return command_digest_show(
                arguments.config, run_id_argument=arguments.run_id
            )
        if arguments.command == "audit" and arguments.audit_command == "run":
            return command_audit_run(
                arguments.config,
                run_id_argument=arguments.run_id,
                attempt_id_argument=arguments.attempt_id,
            )
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
