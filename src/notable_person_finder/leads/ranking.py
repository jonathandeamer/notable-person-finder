"""Pure ranking of digest-queue entries (capability 5's rank_key, capability
6's starvation guard). See design spec Ranking section for K11 rationale."""

from __future__ import annotations

from dataclasses import dataclass

from notable_person_finder.leads.aggregation import LeadOutcome

_OUTCOME_RANK = {"promising_lead": 0, "possible_lead": 1}
_ELIGIBILITY_RANK = {"new": 0, "promoted": 0, "strengthened": 0, "reminder": 1}
_WIKIPEDIA_RANK = {"no_matching_page_found": 0, "uncertain_identity": 1}
_EVIDENCE_VISIBILITY_RANK = {"full": 0, "partial": 1, "snippet": 2}


@dataclass(frozen=True, slots=True)
class QueueEntry:
    person_id: int
    eligibility_reason: str  # 'new' | 'promoted' | 'strengthened' | 'reminder'
    first_pending_at: str


def _freshness_days(freshest_qualifying_article_at: str | None) -> int:
    if freshest_qualifying_article_at is None:
        return 0
    # Lexicographic ISO-8601 UTC timestamps sort chronologically; days-since
    # is not needed for ordering, only a monotone proxy is. Callers passing
    # the raw timestamp already yield correct ordering when negated below,
    # so this returns an integer derived from the timestamp for typing only.
    return int(
        freshest_qualifying_article_at.replace("-", "")
        .replace(":", "")
        .replace("T", "")
        .rstrip("Z")[:14]
        or 0
    )


def rank_key(
    entry: QueueEntry,
    lead: LeadOutcome,
    *,
    positive_signal_count: int,
    best_evidence_visibility: str,
    freshest_qualifying_article_at: str | None,
    starvation_cutoff: str,
) -> tuple[object, ...]:
    is_starved = entry.first_pending_at < starvation_cutoff
    return (
        _OUTCOME_RANK[lead.outcome],
        0 if is_starved else 1,
        _ELIGIBILITY_RANK[entry.eligibility_reason],
        _WIKIPEDIA_RANK.get(lead.wikipedia_outcome or "", 2),
        -lead.qualifying_domain_count,
        -positive_signal_count,
        _EVIDENCE_VISIBILITY_RANK.get(best_evidence_visibility, 3),
        -_freshness_days(freshest_qualifying_article_at),
        entry.person_id,
    )
