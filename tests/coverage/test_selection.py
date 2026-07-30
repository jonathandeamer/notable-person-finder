"""Deterministic selection and K34 selection_reason matrix (K11/K34)."""

from __future__ import annotations

from notable_person_finder.coverage.selection import (
    SelectionCandidate,
    eligible_selected_count,
    final_selection,
)

_K34 = frozenset(
    {
        "discovery_curated_eligible",
        "discovery_unclassified_fallback",
        "search_curated_eligible",
        "search_unclassified_fallback",
    }
)


def _c(
    article_id: int,
    *,
    status: str,
    discovery: bool = False,
    stage: int = 1,
    rank: int = 1,
    url: str | None = None,
) -> SelectionCandidate:
    return SelectionCandidate(
        canonical_article_id=article_id,
        rule_status=status,
        from_discovery=discovery,
        stage_ordinal=0 if discovery else stage,
        rank=0 if discovery else rank,
        request_url=url or f"https://example.com/{article_id}",
    )


def test_k34_four_reasons_matrix() -> None:
    selected = final_selection(
        (
            _c(1, status="curated_eligible", discovery=True),
            _c(2, status="unclassified", discovery=True),
            _c(3, status="curated_eligible", discovery=False, stage=1, rank=1),
            _c(4, status="unclassified", discovery=False, stage=1, rank=2),
        ),
        retrieval_target=5,
        max_eligible_fetches=8,
        max_unclassified_fetches=2,
    )
    reasons = {item.canonical_article_id: item.selection_reason for item in selected}
    assert reasons[1] == "discovery_curated_eligible"
    assert reasons[3] == "search_curated_eligible"
    assert reasons[2] == "discovery_unclassified_fallback"
    assert reasons[4] == "search_unclassified_fallback"
    assert set(reasons.values()) <= _K34
    assert "discovery" not in reasons.values()


def test_curated_ineligible_never_selected() -> None:
    selected = final_selection(
        (
            _c(10, status="curated_ineligible", discovery=False, rank=1),
            _c(11, status="unusable", discovery=False, rank=2),
            _c(12, status="curated_eligible", discovery=False, rank=3),
        ),
        retrieval_target=5,
        max_eligible_fetches=8,
        max_unclassified_fetches=2,
    )
    ids = {item.canonical_article_id for item in selected}
    assert ids == {12}
    assert all(item.selection_reason in _K34 for item in selected)


def test_ordering_discovery_before_search_then_rank() -> None:
    selected = final_selection(
        (
            _c(30, status="curated_eligible", discovery=False, stage=1, rank=2),
            _c(20, status="curated_eligible", discovery=False, stage=1, rank=1),
            _c(10, status="curated_eligible", discovery=True),
            _c(40, status="curated_eligible", discovery=False, stage=2, rank=1),
        ),
        retrieval_target=5,
        max_eligible_fetches=8,
        max_unclassified_fetches=0,
    )
    assert [item.canonical_article_id for item in selected] == [10, 20, 30, 40]


def test_max_eligible_fetches_cap() -> None:
    candidates = tuple(
        _c(i, status="curated_eligible", discovery=False, rank=i) for i in range(1, 6)
    )
    selected = final_selection(
        candidates,
        retrieval_target=5,
        max_eligible_fetches=2,
        max_unclassified_fetches=2,
    )
    assert len(selected) == 2
    assert [item.canonical_article_id for item in selected] == [1, 2]


def test_unclassified_fallback_only_when_below_retrieval_target() -> None:
    # Two eligible already meet target=2 → no unclassified.
    selected = final_selection(
        (
            _c(1, status="curated_eligible", discovery=False, rank=1),
            _c(2, status="curated_eligible", discovery=False, rank=2),
            _c(9, status="unclassified", discovery=False, rank=1),
        ),
        retrieval_target=2,
        max_eligible_fetches=8,
        max_unclassified_fetches=2,
    )
    assert {item.canonical_article_id for item in selected} == {1, 2}

    # One eligible below target=2 → add unclassified.
    selected_fallback = final_selection(
        (
            _c(1, status="curated_eligible", discovery=False, rank=1),
            _c(9, status="unclassified", discovery=False, rank=1),
        ),
        retrieval_target=2,
        max_eligible_fetches=8,
        max_unclassified_fetches=2,
    )
    assert {item.canonical_article_id for item in selected_fallback} == {1, 9}
    assert selected_fallback[1].selection_reason == "search_unclassified_fallback"


def test_max_unclassified_fetches_caps_surplus_fallback() -> None:
    """K11: surplus unclassified truncated to max_unclassified_fetches."""
    candidates = (
        _c(1, status="curated_eligible", discovery=False, rank=1),
        _c(10, status="unclassified", discovery=False, rank=1),
        _c(11, status="unclassified", discovery=False, rank=2),
        _c(12, status="unclassified", discovery=False, rank=3),
        _c(13, status="unclassified", discovery=False, rank=4),
    )
    selected = final_selection(
        candidates,
        retrieval_target=5,
        max_eligible_fetches=8,
        max_unclassified_fetches=2,
    )
    # 1 eligible + 2 unclassified (cap), not all four unclassified.
    assert len(selected) == 3
    assert [item.canonical_article_id for item in selected] == [1, 10, 11]
    assert selected[0].selection_reason == "search_curated_eligible"
    assert selected[1].selection_reason == "search_unclassified_fallback"
    assert selected[2].selection_reason == "search_unclassified_fallback"
    assert all(item.selection_reason in _K34 for item in selected)
    assert 12 not in {item.canonical_article_id for item in selected}
    assert 13 not in {item.canonical_article_id for item in selected}


def test_eligible_selected_count_ignores_unclassified() -> None:
    candidates = (
        _c(1, status="curated_eligible", discovery=True),
        _c(2, status="unclassified", discovery=False, rank=1),
        _c(3, status="curated_eligible", discovery=False, rank=1),
        _c(4, status="curated_ineligible", discovery=False, rank=1),
    )
    assert eligible_selected_count(candidates, max_eligible_fetches=8) == 2
    assert eligible_selected_count(candidates, max_eligible_fetches=1) == 1


def test_dedupe_prefers_discovery_reason() -> None:
    # Same article on discovery and search: caller should pass one candidate
    # with from_discovery=True (design: prefer discovery in selection_reason).
    selected = final_selection(
        (_c(7, status="curated_eligible", discovery=True),),
        retrieval_target=5,
        max_eligible_fetches=8,
        max_unclassified_fetches=0,
    )
    assert len(selected) == 1
    assert selected[0].selection_reason == "discovery_curated_eligible"
