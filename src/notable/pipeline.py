"""The whole loop, readable top to bottom."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from notable import coverage, detect, digest, feeds, rank, wiki
from notable.config import Config
from notable.errors import BudgetExceeded, Incomplete
from notable.http import Transport
from notable.llm import LlmClient
from notable.store import RunSummary, Store


@dataclass(frozen=True, slots=True)
class Providers:
    transport: Transport
    llm: LlmClient


@dataclass(frozen=True, slots=True)
class RunResult:
    """This run's outputs. The summary is returned rather than re-read from
    the `run` table, because that table is a best-effort log whose writes are
    allowed to fail silently."""

    digest_path: Path
    summary: RunSummary


def run(
    config: Config,
    store: Store,
    providers: Providers,
    *,
    fresh_feeds: bool = False,
) -> RunResult:
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    entries: list[rank.Lead] = []
    settled: list[str] = []
    incomplete: list[str] = []
    capped = False
    try:
        for item in feeds.fetch_new(
            config, providers.transport, store, fresh=fresh_feeds
        ):
            item_entries: list[rank.Lead] = []
            try:
                for mention in detect.people_in(item, config, providers.llm):
                    if not mention.research_worthy:
                        continue
                    verdict = wiki.match(
                        mention, config, providers.transport, providers.llm
                    )
                    if verdict.has_page:
                        continue
                    assessments = coverage.research(
                        mention, config, providers.transport, providers.llm
                    )
                    item_entries.append(
                        rank.assess(mention, verdict, assessments, config, item)
                    )
            except Incomplete:
                incomplete.append(item.url)
                continue
            entries.extend(item_entries)
            settled.append(item.url)
    except BudgetExceeded:
        capped = True

    summary = RunSummary(settled, incomplete, capped, providers.llm.spend(), started_at)
    shortlist, surfaced_keys = rank.shortlist(entries, store, config)
    written = digest.write(
        shortlist,
        config.digest_dir,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        status=summary.status,
        cost_usd=summary.cost_usd,
        n_settled=len(settled),
        n_incomplete=len(incomplete),
    )
    store.commit(settled, incomplete, surfaced_keys)
    store.log(
        summary,
        [rank.lead_to_log_dict(lead) for lead in entries],
        str(written),
    )
    return RunResult(digest_path=written, summary=summary)
