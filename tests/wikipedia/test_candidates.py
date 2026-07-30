"""Pure biography candidate assembly (K5/K7/K8/K24).

Fixture page and search rows only — no HTTP, no SQLite. Names/similarity never
establish identity; dab and non-main-ns never become candidates.
"""

from __future__ import annotations

import pytest

from notable_person_finder.wikipedia.candidates import (
    AssemblyPage,
    AssemblyResult,
    AssemblySearchHit,
    BiographyCandidate,
    assemble_biography_candidates,
)


def page(
    page_id: int,
    *,
    title: str | None = None,
    namespace: int = 0,
    is_disambiguation: bool = False,
    is_missing: bool = False,
    redirect_to: int | None = None,
    description: str | None = None,
    extract: str | None = None,
    categories: tuple[str, ...] = (),
    url: str | None = None,
) -> AssemblyPage:
    resolved_title = title if title is not None else f"Page {page_id}"
    default_url = "https://en.wikipedia.org/wiki/" + resolved_title.replace(" ", "_")
    return AssemblyPage(
        page_id=page_id,
        canonical_title=resolved_title,
        canonical_url=url or default_url,
        namespace=namespace,
        is_disambiguation=is_disambiguation,
        is_missing=is_missing,
        redirect_to_page_id=redirect_to,
        description=description,
        extract=extract,
        categories=categories,
    )


def hit(page_id: int, rank: int) -> AssemblySearchHit:
    return AssemblySearchHit(page_id=page_id, rank=rank)


def assemble(
    hits: list[AssemblySearchHit] | tuple[AssemblySearchHit, ...],
    pages: list[AssemblyPage] | tuple[AssemblyPage, ...],
    *,
    max_candidates: int = 8,
    max_redirect_hops: int = 3,
    search_incomplete: bool = False,
    redirect_budget_exhausted: bool = False,
    partial_retrieval: bool = False,
) -> AssemblyResult:
    return assemble_biography_candidates(
        hits,
        {p.page_id: p for p in pages},
        max_candidates=max_candidates,
        max_redirect_hops=max_redirect_hops,
        search_incomplete=search_incomplete,
        redirect_budget_exhausted=redirect_budget_exhausted,
        partial_retrieval=partial_retrieval,
    )


def test_drops_disambiguation_pages() -> None:
    result = assemble(
        [hit(1, 0), hit(2, 1)],
        [
            page(1, title="Ada (disambiguation)", is_disambiguation=True),
            page(2, title="Ada Lovelace", description="mathematician"),
        ],
    )
    assert [c.page_id for c in result.candidates] == [2]
    assert result.uncapped_count == 1


def test_drops_non_main_namespace() -> None:
    result = assemble(
        [hit(10, 0), hit(11, 1)],
        [
            page(10, title="Talk:Ada", namespace=1),
            page(11, title="Ada Lovelace", namespace=0),
        ],
    )
    assert [c.page_id for c in result.candidates] == [11]


def test_drops_missing_terminals() -> None:
    result = assemble(
        [hit(5, 0), hit(6, 1)],
        [
            page(5, title="Gone", is_missing=True),
            page(6, title="Present"),
        ],
    )
    assert [c.page_id for c in result.candidates] == [6]


def test_redirect_chain_length_one() -> None:
    result = assemble(
        [hit(100, 0)],
        [
            page(100, title="A. Lovelace", redirect_to=200),
            page(200, title="Ada Lovelace", extract="English mathematician"),
        ],
    )
    assert len(result.candidates) == 1
    c = result.candidates[0]
    assert c.page_id == 200
    assert c.canonical_title == "Ada Lovelace"
    assert c.extract == "English mathematician"
    assert c.best_search_rank == 0


def test_redirect_chain_length_two() -> None:
    result = assemble(
        [hit(1, 0)],
        [
            page(1, title="A", redirect_to=2),
            page(2, title="B", redirect_to=3),
            page(3, title="Terminal bio"),
        ],
        max_redirect_hops=3,
    )
    assert [c.page_id for c in result.candidates] == [3]
    assert result.candidates[0].canonical_title == "Terminal bio"


def test_redirect_hop_budget_drops_trail() -> None:
    # Chain of 3 hops needs max_redirect_hops >= 3 to reach page 4.
    pages = [
        page(1, title="A", redirect_to=2),
        page(2, title="B", redirect_to=3),
        page(3, title="C", redirect_to=4),
        page(4, title="D terminal"),
    ]
    short = assemble([hit(1, 0)], pages, max_redirect_hops=2)
    assert short.candidates == ()
    assert short.uncapped_count == 0

    long = assemble([hit(1, 0)], pages, max_redirect_hops=3)
    assert [c.page_id for c in long.candidates] == [4]


def test_unfetched_redirect_target_drops_trail() -> None:
    result = assemble(
        [hit(1, 0)],
        [page(1, title="A", redirect_to=99)],
        redirect_budget_exhausted=True,
    )
    assert result.candidates == ()
    assert result.truncated_unsafe_for_negative is True
    assert result.failure_category_if_empty == "redirect_budget_exhausted"


def test_dedupe_by_terminal_page_id_keeps_best_rank() -> None:
    # Two hits (ranks 5 and 1) redirect or point to the same terminal.
    result = assemble(
        [hit(10, 5), hit(20, 1), hit(30, 3)],
        [
            page(10, title="Alias A", redirect_to=99),
            page(20, title="Alias B", redirect_to=99),
            page(30, title="Direct", redirect_to=99),
            page(99, title="Canonical"),
        ],
    )
    assert len(result.candidates) == 1
    assert result.candidates[0].page_id == 99
    assert result.candidates[0].best_search_rank == 1
    assert result.uncapped_count == 1


def test_rank_by_best_search_rank_then_lower_page_id() -> None:
    result = assemble(
        [hit(50, 2), hit(40, 2), hit(30, 0)],
        [
            page(50, title="C"),
            page(40, title="B"),
            page(30, title="A"),
        ],
        max_candidates=8,
    )
    assert [c.page_id for c in result.candidates] == [30, 40, 50]
    assert [c.best_search_rank for c in result.candidates] == [0, 2, 2]


def test_max_candidates_cap_and_uncapped_sets_truncated_unsafe() -> None:
    pages = [page(i, title=f"P{i}") for i in range(1, 6)]
    hits = [hit(i, i - 1) for i in range(1, 6)]
    result = assemble(hits, pages, max_candidates=2)
    assert [c.page_id for c in result.candidates] == [1, 2]
    assert result.uncapped_count == 5
    assert result.truncated_unsafe_for_negative is True
    # Non-empty: failure_category_if_empty is only for empty assembly.
    assert result.failure_category_if_empty is None


def test_uncapped_equal_max_candidates_is_safe() -> None:
    pages = [page(1), page(2)]
    result = assemble([hit(1, 0), hit(2, 1)], pages, max_candidates=2)
    assert result.uncapped_count == 2
    assert len(result.candidates) == 2
    assert result.truncated_unsafe_for_negative is False


def test_search_incomplete_sets_truncated_unsafe_even_when_empty() -> None:
    result = assemble(
        [],
        [],
        search_incomplete=True,
    )
    assert result.candidates == ()
    assert result.truncated_unsafe_for_negative is True
    assert result.failure_category_if_empty == "unsafe_truncation"


def test_partial_retrieval_flag_plumbing() -> None:
    # Usable candidates still surface; flag blocks safe negatives later.
    result = assemble(
        [hit(1, 0)],
        [page(1, title="Bio")],
        partial_retrieval=True,
    )
    assert [c.page_id for c in result.candidates] == [1]
    assert result.partial_retrieval is True
    assert result.truncated_unsafe_for_negative is True
    assert result.failure_category_if_empty is None


def test_partial_retrieval_empty_failure_category() -> None:
    result = assemble(
        [hit(1, 0)],
        [page(1, title="Talk", namespace=1)],
        partial_retrieval=True,
    )
    assert result.candidates == ()
    assert result.partial_retrieval is True
    assert result.truncated_unsafe_for_negative is True
    assert result.failure_category_if_empty == "partial_retrieval_empty"


def test_safe_empty_has_no_failure_category() -> None:
    result = assemble([], [])
    assert result == AssemblyResult(
        candidates=(),
        uncapped_count=0,
        truncated_unsafe_for_negative=False,
        partial_retrieval=False,
        failure_category_if_empty=None,
    )


def test_redirect_budget_exhausted_with_other_candidates_still_unsafe() -> None:
    result = assemble(
        [hit(1, 0), hit(2, 1)],
        [
            page(1, title="A", redirect_to=999),  # unresolved
            page(2, title="Ok bio"),
        ],
        redirect_budget_exhausted=True,
    )
    assert [c.page_id for c in result.candidates] == [2]
    assert result.truncated_unsafe_for_negative is True
    assert result.failure_category_if_empty is None


def test_no_biography_category_score_gate() -> None:
    """Categories never filter candidates (K7); non-bio categories still admit."""
    result = assemble(
        [hit(1, 0)],
        [
            page(
                1,
                title="Someone",
                categories=("Category:Living people", "Category:2020s albums"),
            )
        ],
    )
    assert len(result.candidates) == 1
    assert result.candidates[0].categories == (
        "Category:Living people",
        "Category:2020s albums",
    )


def test_hit_without_page_facts_is_dropped() -> None:
    result = assemble([hit(42, 0)], [])
    assert result.candidates == ()
    assert result.uncapped_count == 0


def test_max_candidates_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="max_candidates"):
        assemble([hit(1, 0)], [page(1)], max_candidates=0)


def test_max_redirect_hops_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="max_redirect_hops"):
        assemble([hit(1, 0)], [page(1)], max_redirect_hops=0)


def test_candidate_payload_carries_context_fields() -> None:
    result = assemble(
        [hit(7, 4)],
        [
            page(
                7,
                title="Ada Lovelace",
                description="English mathematician",
                extract="Ada Lovelace was…",
                categories=("Category:1815 births",),
            )
        ],
    )
    c = result.candidates[0]
    assert isinstance(c, BiographyCandidate)
    assert c.page_id == 7
    assert c.canonical_title == "Ada Lovelace"
    assert c.description == "English mathematician"
    assert c.extract == "Ada Lovelace was…"
    assert c.categories == ("Category:1815 births",)
    assert c.best_search_rank == 4
    assert "Ada_Lovelace" in c.canonical_url
