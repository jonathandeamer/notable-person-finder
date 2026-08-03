"""The whole loop, readable top to bottom."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from notable import detect, digest, feeds
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
    entries: list[digest.DigestEntry] = []
    settled: list[str] = []
    incomplete: list[str] = []
    capped = False
    try:
        for item in feeds.fetch_new(
            config, providers.transport, store, fresh=fresh_feeds
        ):
            item_entries = []
            try:
                for mention in detect.people_in(item, config, providers.llm):
                    if not mention.research_worthy:
                        continue
                    item_entries.append(
                        digest.DigestEntry(
                            identity_key=digest.identity_key(mention.exact_name),
                            display_name=mention.exact_name,
                            source_url=item.url,
                            publisher_label=item.publisher_label,
                            rationale=mention.rationale,
                        )
                    )
            except Incomplete:
                incomplete.append(item.url)  # retried next run, up to a cap
                continue
            entries.extend(item_entries)
            settled.append(item.url)
    except BudgetExceeded:
        capped = True  # render what finished

    summary = RunSummary(settled, incomplete, capped, providers.llm.spend(), started_at)
    # Phase 1 has no ranking or shortlist yet: the detection digest must retain
    # every research-worthy mention so the fixture exercises the full corpus.
    # Phase 4 applies digest_size after ranking and duplicate collapse.
    written = digest.write(
        entries,
        config.digest_dir,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        status=summary.status,
        cost_usd=summary.cost_usd,
        n_settled=len(settled),
        n_incomplete=len(incomplete),
    )
    # Nothing durable is written until the digest file exists. Phase 4 passes
    # surfaced keys and lead rows; both parameters exist now so the signatures
    # do not move.
    store.commit(settled, incomplete, [])
    store.log(summary, [], str(written))
    return RunResult(digest_path=written, summary=summary)
