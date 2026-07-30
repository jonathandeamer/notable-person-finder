"""Coverage research eligibility: Wikipedia gate (K5) and pure matrix (K19).

The full ``is_coverage_research_eligible`` path that consults plan fingerprints
and refresh intervals lands with service/seed work. This module owns the pure
Wikipedia gate and a pure state evaluator so Task 4 can lock K5 without HTTP
or material-fingerprint machinery.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

WikipediaSemanticOutcome = Literal[
    "matching_page_found",
    "no_matching_page_found",
    "uncertain_identity",
]

_ELIGIBLE_OUTCOMES = frozenset({"no_matching_page_found", "uncertain_identity"})


@dataclass(frozen=True, slots=True)
class WikipediaIdentityView:
    """Faked or loaded current Wikipedia observation fields for the gate."""

    observation_id: int | None
    disposition: str | None
    semantic_outcome: str | None


@dataclass(frozen=True, slots=True)
class CoverageEligibilityView:
    """Person-level inputs for the pure eligibility matrix (K5 core + guards).

    Plan/refresh branches of K19 are represented by ``has_blocking_active_plan``
    and ``has_blocking_terminal_plan`` so unit tests can assert those outcomes
    without implementing material fingerprints yet.
    """

    merged_into_person_id: int | None
    has_operational_match_key: bool
    wikipedia: WikipediaIdentityView
    has_blocking_active_plan: bool = False
    has_blocking_terminal_plan: bool = False


def wikipedia_allows_coverage_research(wikipedia: WikipediaIdentityView) -> bool:
    """K5 Wikipedia gate alone.

    - null pointer → false (in-progress or none; do not invent no-match)
    - non-completed disposition → false
    - ``matching_page_found`` → false (stop research) — explicit before allowlist
    - ``no_matching_page_found`` / ``uncertain_identity`` → true
    - any other semantic outcome → false
    """
    if wikipedia.observation_id is None:
        return False
    if wikipedia.disposition != "completed":
        return False
    # Explicit stop before the allowlist so matching cannot unlock coverage
    # even if `_ELIGIBLE_OUTCOMES` is expanded by mistake.
    if wikipedia.semantic_outcome == "matching_page_found":
        return False
    return wikipedia.semantic_outcome in _ELIGIBLE_OUTCOMES


def is_coverage_research_eligible_view(view: CoverageEligibilityView) -> bool:
    """Pure K5/K19 core eligibility matrix used by unit tests and later service.

    Returns false when the person is merged away, lacks an operational name
    match_key, fails the Wikipedia gate, or has a blocking active/terminal
    plan under live material (flags supplied by the caller until fingerprint
    recompute lives in service).
    """
    if view.merged_into_person_id is not None:
        return False
    if not view.has_operational_match_key:
        return False
    if not wikipedia_allows_coverage_research(view.wikipedia):
        return False
    return not (view.has_blocking_active_plan or view.has_blocking_terminal_plan)


# Public alias matching design naming for the pure matrix entry point.
is_coverage_research_eligible = is_coverage_research_eligible_view
