"""Deterministic coverage article selection (K11 / K34).

Provisional and final selection share ordering. Curated-ineligible and unusable
articles never become fetch targets. ``selection_reason`` is the four-value
source × tier enum (never a bare ``discovery`` string).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

SelectionReason = Literal[
    "discovery_curated_eligible",
    "discovery_unclassified_fallback",
    "search_curated_eligible",
    "search_unclassified_fallback",
]

_ELIGIBLE = "curated_eligible"
_UNCLASSIFIED = "unclassified"
_INELIGIBLE = frozenset({"curated_ineligible", "unusable"})


@dataclass(frozen=True, slots=True)
class SelectionCandidate:
    """One distinct canonical article considered for selection.

    When the same article appears on both discovery and search, callers should
    pass a single candidate with ``from_discovery=True`` (prefer discovery for
    ``selection_reason``). Ordering uses ``stage_ordinal`` then ``rank`` then
    ``canonical_article_id``.
    """

    canonical_article_id: int
    rule_status: str
    from_discovery: bool
    stage_ordinal: int
    rank: int
    request_url: str


@dataclass(frozen=True, slots=True)
class SelectedArticle:
    canonical_article_id: int
    selection_reason: SelectionReason
    request_url: str
    rule_status: str
    from_discovery: bool


def eligible_selected_count(
    candidates: Sequence[SelectionCandidate],
    *,
    max_eligible_fetches: int,
) -> int:
    """Count curated-eligible articles under provisional selection order (K6).

    Unclassified fallback does **not** count toward the retrieval target.
    """
    if max_eligible_fetches < 1:
        return 0
    eligible = [
        candidate for candidate in candidates if candidate.rule_status == _ELIGIBLE
    ]
    ordered = _order(eligible)
    seen: set[int] = set()
    count = 0
    for candidate in ordered:
        if candidate.canonical_article_id in seen:
            continue
        seen.add(candidate.canonical_article_id)
        count += 1
        if count >= max_eligible_fetches:
            break
    return count


def final_selection(
    candidates: Sequence[SelectionCandidate],
    *,
    retrieval_target: int,
    max_eligible_fetches: int,
    max_unclassified_fetches: int,
) -> tuple[SelectedArticle, ...]:
    """Select fetch targets with K34 selection_reason values (K11).

    1. Curated-eligible up to ``max_eligible_fetches`` (discovery stage 0 first).
    2. If eligible unique count < ``retrieval_target``, add up to
       ``max_unclassified_fetches`` unclassified fallbacks.
    3. Never select curated-ineligible or unusable.
    """
    if max_eligible_fetches < 0:
        raise ValueError("max_eligible_fetches must be >= 0")
    if max_unclassified_fetches < 0:
        raise ValueError("max_unclassified_fetches must be >= 0")

    eligible = [c for c in candidates if c.rule_status == _ELIGIBLE]
    unclassified = [c for c in candidates if c.rule_status == _UNCLASSIFIED]
    # Explicitly ignore ineligible / unusable — and any unknown status.
    _ = [c for c in candidates if c.rule_status in _INELIGIBLE]

    selected: list[SelectedArticle] = []
    seen: set[int] = set()

    for candidate in _order(eligible):
        if candidate.canonical_article_id in seen:
            continue
        if len(selected) >= max_eligible_fetches:
            break
        seen.add(candidate.canonical_article_id)
        selected.append(
            SelectedArticle(
                canonical_article_id=candidate.canonical_article_id,
                selection_reason=_reason(
                    from_discovery=candidate.from_discovery,
                    tier="eligible",
                ),
                request_url=candidate.request_url,
                rule_status=candidate.rule_status,
                from_discovery=candidate.from_discovery,
            )
        )

    eligible_unique = len(selected)
    if eligible_unique < retrieval_target and max_unclassified_fetches > 0:
        fallback_slots = max_unclassified_fetches
        for candidate in _order(unclassified):
            if candidate.canonical_article_id in seen:
                continue
            if fallback_slots <= 0:
                break
            seen.add(candidate.canonical_article_id)
            fallback_slots -= 1
            selected.append(
                SelectedArticle(
                    canonical_article_id=candidate.canonical_article_id,
                    selection_reason=_reason(
                        from_discovery=candidate.from_discovery,
                        tier="unclassified",
                    ),
                    request_url=candidate.request_url,
                    rule_status=candidate.rule_status,
                    from_discovery=candidate.from_discovery,
                )
            )

    return tuple(selected)


def _order(candidates: Sequence[SelectionCandidate]) -> list[SelectionCandidate]:
    return sorted(
        candidates,
        key=lambda c: (c.stage_ordinal, c.rank, c.canonical_article_id),
    )


def _reason(
    *,
    from_discovery: bool,
    tier: Literal["eligible", "unclassified"],
) -> SelectionReason:
    if from_discovery and tier == "eligible":
        return "discovery_curated_eligible"
    if from_discovery and tier == "unclassified":
        return "discovery_unclassified_fallback"
    if not from_discovery and tier == "eligible":
        return "search_curated_eligible"
    return "search_unclassified_fallback"
