"""K5 Wikipedia gate and pure coverage eligibility matrix (K19 core)."""

from __future__ import annotations

import pytest

from notable_person_finder.coverage.eligibility import (
    CoverageEligibilityView,
    WikipediaIdentityView,
    is_coverage_research_eligible,
    wikipedia_allows_coverage_research,
)


def _wiki(
    *,
    observation_id: int | None,
    disposition: str | None = "completed",
    semantic_outcome: str | None,
) -> WikipediaIdentityView:
    return WikipediaIdentityView(
        observation_id=observation_id,
        disposition=disposition,
        semantic_outcome=semantic_outcome,
    )


def _view(
    *,
    wiki: WikipediaIdentityView,
    merged: int | None = None,
    has_name: bool = True,
    active_plan: bool = False,
    terminal_plan: bool = False,
) -> CoverageEligibilityView:
    return CoverageEligibilityView(
        merged_into_person_id=merged,
        has_operational_match_key=has_name,
        wikipedia=wiki,
        has_blocking_active_plan=active_plan,
        has_blocking_terminal_plan=terminal_plan,
    )


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("matching_page_found", False),
        ("no_matching_page_found", True),
        ("uncertain_identity", True),
        ("something_else", False),
        (None, False),
    ],
)
def test_wikipedia_gate_semantic_outcomes(outcome: str | None, expected: bool) -> None:
    wiki = _wiki(observation_id=1, semantic_outcome=outcome)
    assert wikipedia_allows_coverage_research(wiki) is expected
    assert is_coverage_research_eligible(_view(wiki=wiki)) is expected


def test_null_wikipedia_pointer_is_not_eligible() -> None:
    wiki = _wiki(observation_id=None, disposition=None, semantic_outcome=None)
    assert wikipedia_allows_coverage_research(wiki) is False
    assert is_coverage_research_eligible(_view(wiki=wiki)) is False


def test_failed_or_non_completed_wikipedia_is_not_eligible() -> None:
    failed = _wiki(
        observation_id=2,
        disposition="failed",
        semantic_outcome="no_matching_page_found",
    )
    assert wikipedia_allows_coverage_research(failed) is False
    assert is_coverage_research_eligible(_view(wiki=failed)) is False

    pending = _wiki(
        observation_id=3,
        disposition="pending",
        semantic_outcome=None,
    )
    assert wikipedia_allows_coverage_research(pending) is False


def test_matching_page_found_stops_even_when_other_gates_pass() -> None:
    view = _view(
        wiki=_wiki(observation_id=9, semantic_outcome="matching_page_found"),
        merged=None,
        has_name=True,
    )
    assert is_coverage_research_eligible(view) is False


def test_no_match_and_uncertain_eligible_when_other_gates_permit() -> None:
    for outcome in ("no_matching_page_found", "uncertain_identity"):
        view = _view(
            wiki=_wiki(observation_id=4, semantic_outcome=outcome),
            merged=None,
            has_name=True,
            active_plan=False,
            terminal_plan=False,
        )
        assert is_coverage_research_eligible(view) is True


def test_merged_person_never_eligible() -> None:
    view = _view(
        wiki=_wiki(observation_id=1, semantic_outcome="no_matching_page_found"),
        merged=99,
        has_name=True,
    )
    assert is_coverage_research_eligible(view) is False


def test_missing_operational_match_key_not_eligible() -> None:
    view = _view(
        wiki=_wiki(observation_id=1, semantic_outcome="no_matching_page_found"),
        has_name=False,
    )
    assert is_coverage_research_eligible(view) is False


def test_blocking_active_or_terminal_plan_not_eligible() -> None:
    base_wiki = _wiki(observation_id=1, semantic_outcome="uncertain_identity")
    assert (
        is_coverage_research_eligible(_view(wiki=base_wiki, active_plan=True)) is False
    )
    assert (
        is_coverage_research_eligible(_view(wiki=base_wiki, terminal_plan=True))
        is False
    )


def test_positive_control_each_outcome_reachable() -> None:
    """Positive controls: each Wikipedia outcome path is observable."""
    outcomes = {
        "matching_page_found": False,
        "no_matching_page_found": True,
        "uncertain_identity": True,
    }
    observed: dict[str, bool] = {}
    for outcome, expected in outcomes.items():
        result = is_coverage_research_eligible(
            _view(wiki=_wiki(observation_id=1, semantic_outcome=outcome))
        )
        observed[outcome] = result
        assert result is expected
    # Ensure the matrix distinguishes matching (false) from the two true paths.
    assert observed["matching_page_found"] is False
    assert observed["no_matching_page_found"] is True
    assert observed["uncertain_identity"] is True
