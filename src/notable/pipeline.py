"""The whole loop, readable top to bottom."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from notable import coverage, detect, digest, feeds, rank, wiki
from notable.config import Config
import dataclasses
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
        item_mentions = {}
        canonical_mentions = {}
        for item in feeds.fetch_new(
            config, providers.transport, store, fresh=fresh_feeds
        ):
            try:
                mentions = list(detect.people_in(item, config, providers.llm))
                item_mentions[item.url] = (item, mentions)
                for mention in mentions:
                    if not mention.research_worthy:
                        continue
                    key = rank.identity_key(mention.canonical_name)
                    if key in canonical_mentions:
                        existing = canonical_mentions[key]
                        canonical_mentions[key] = existing.model_copy(
                            update={
                                "identity_facts": existing.identity_facts
                                + mention.identity_facts,
                                "signals": existing.signals + mention.signals,
                            }
                        )
                    else:
                        canonical_mentions[key] = mention
            except Incomplete:
                incomplete.append(item.url)
    except BudgetExceeded:
        capped = True

    # A single item might mention the same person multiple times (e.g. differently
    # capitalized). The identity key de-duplicates these: we assess and research
    # each distinct canonical name exactly once per run.
    # Verdict/assessments per identity:
    identity_leads: dict[
        str, tuple[wiki.MatchVerdict, tuple[coverage.Article, ...]]
    ] = {}
    suppressed: set[str] = set()
    failed_identities = set()
    new_research = {}
    for key, mention in canonical_mentions.items():
        try:
            cached = store.cached_research(key)
            if cached:
                verdict = wiki.MatchVerdict(**cached)
            else:
                verdict = wiki.match(
                    mention,
                    config,
                    providers.transport,
                    providers.llm,
                )
                if verdict.outcome in ("matching_page", "no_matching_page"):
                    cached = dataclasses.asdict(verdict)
                    cached["outcome"] = verdict.outcome
                    new_research[key] = cached

            if verdict.outcome == "matching_page":
                suppressed.add(key)
                continue
            assessments = coverage.research(
                mention, config, providers.transport, providers.llm
            )
            identity_leads[key] = (verdict, assessments)
        except Incomplete:
            failed_identities.add(key)
        except BudgetExceeded:
            capped = True
            break

    for url, (item, mentions) in item_mentions.items():
        item_is_settled = True
        # Write rank outputs
        item_entries = []
        for mention in mentions:
            if not mention.research_worthy:
                continue
            key = rank.identity_key(mention.canonical_name)
            if key in failed_identities or (
                key not in identity_leads and key not in suppressed
            ):
                item_is_settled = False
                break

            if key in suppressed:
                continue

            lead_data = identity_leads.get(key)
            if lead_data is not None:
                verdict, assessments = lead_data
                # We pass `canonical_mentions[key]` to `assess` so it uses the
                # accumulated signals/facts
                item_entries.append(
                    rank.assess(
                        canonical_mentions[key], verdict, assessments, config, item
                    )
                )

        if item_is_settled:
            settled.append(url)
            entries.extend(item_entries)
        else:
            incomplete.append(url)

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
    store.commit(settled, incomplete, surfaced_keys, new_research)
    store.log(
        summary,
        [rank.lead_to_log_dict(lead) for lead in entries],
        str(written),
    )
    return RunResult(digest_path=written, summary=summary)
