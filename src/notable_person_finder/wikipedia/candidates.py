"""Assemble biography candidates from search hits and page facts (K5/K8/K24).

Pure deterministic logic: union hits, walk redirect edges within hop budget,
drop missing / non-main-namespace / disambiguation terminals, rank and cap.
Names and category “biography scores” never establish identity (K7). Redirect
multi-wave fetching is owned by handlers; this module consumes terminal pages
already present in the page map.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class AssemblySearchHit:
    """One search hit root identified by MediaWiki page id and rank."""

    page_id: int
    rank: int  # lower is better


@dataclass(frozen=True, slots=True)
class AssemblyPage:
    """Fetched MediaWiki page facts needed for assembly."""

    page_id: int
    canonical_title: str
    canonical_url: str
    namespace: int
    is_disambiguation: bool
    is_missing: bool
    redirect_to_page_id: int | None
    description: str | None
    extract: str | None
    categories: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BiographyCandidate:
    """One main-namespace non-dab terminal page eligible for model match."""

    page_id: int
    canonical_title: str
    canonical_url: str
    description: str | None
    extract: str | None
    categories: tuple[str, ...]
    best_search_rank: int


@dataclass(frozen=True, slots=True)
class AssemblyResult:
    """Capped candidate set plus K5 truncation / K22 partial flags."""

    candidates: tuple[BiographyCandidate, ...]
    uncapped_count: int
    truncated_unsafe_for_negative: bool
    partial_retrieval: bool
    failure_category_if_empty: str | None


def assemble_biography_candidates(
    hits: Sequence[AssemblySearchHit],
    pages_by_id: Mapping[int, AssemblyPage],
    *,
    max_candidates: int,
    max_redirect_hops: int,
    search_incomplete: bool = False,
    redirect_budget_exhausted: bool = False,
    partial_retrieval: bool = False,
) -> AssemblyResult:
    """Filter, resolve redirects, rank, and cap biography candidates.

    ``search_incomplete`` covers pagination / hit-cap truncation on forms.
    ``redirect_budget_exhausted`` is set by the caller when hop or fact-page
    budgets left unresolved trails (K24). ``partial_retrieval`` is K22 form
    permanent-fail plumbing.
    """
    if max_candidates < 1:
        raise ValueError("max_candidates must be >= 1")
    if max_redirect_hops < 1:
        raise ValueError("max_redirect_hops must be >= 1")

    # Best (lowest) rank per hit root page id.
    best_rank_by_root: dict[int, int] = {}
    for hit in hits:
        previous = best_rank_by_root.get(hit.page_id)
        if previous is None or hit.rank < previous:
            best_rank_by_root[hit.page_id] = hit.rank

    # Terminal page id → best rank among roots that resolve to it.
    best_rank_by_terminal: dict[int, int] = {}
    terminal_pages: dict[int, AssemblyPage] = {}

    for root_id, root_rank in best_rank_by_root.items():
        terminal = _resolve_terminal(
            root_id,
            pages_by_id,
            max_redirect_hops=max_redirect_hops,
        )
        if terminal is None:
            continue
        if terminal.is_missing:
            continue
        if terminal.namespace != 0:
            continue
        if terminal.is_disambiguation:
            continue

        previous = best_rank_by_terminal.get(terminal.page_id)
        if previous is None or root_rank < previous:
            best_rank_by_terminal[terminal.page_id] = root_rank
            terminal_pages[terminal.page_id] = terminal

    ranked_ids = sorted(
        best_rank_by_terminal.keys(),
        key=lambda page_id: (best_rank_by_terminal[page_id], page_id),
    )
    uncapped_count = len(ranked_ids)
    kept_ids = ranked_ids[:max_candidates]

    candidates = tuple(
        BiographyCandidate(
            page_id=page_id,
            canonical_title=terminal_pages[page_id].canonical_title,
            canonical_url=terminal_pages[page_id].canonical_url,
            description=terminal_pages[page_id].description,
            extract=terminal_pages[page_id].extract,
            categories=terminal_pages[page_id].categories,
            best_search_rank=best_rank_by_terminal[page_id],
        )
        for page_id in kept_ids
    )

    capped = uncapped_count > max_candidates
    truncated_unsafe = (
        search_incomplete or capped or redirect_budget_exhausted or partial_retrieval
    )

    failure_category: str | None = None
    if not candidates:
        if partial_retrieval:
            failure_category = "partial_retrieval_empty"
        elif redirect_budget_exhausted:
            failure_category = "redirect_budget_exhausted"
        elif truncated_unsafe:
            failure_category = "unsafe_truncation"
        else:
            failure_category = None

    return AssemblyResult(
        candidates=candidates,
        uncapped_count=uncapped_count,
        truncated_unsafe_for_negative=truncated_unsafe,
        partial_retrieval=partial_retrieval,
        failure_category_if_empty=failure_category,
    )


def _resolve_terminal(
    root_page_id: int,
    pages_by_id: Mapping[int, AssemblyPage],
    *,
    max_redirect_hops: int,
) -> AssemblyPage | None:
    """Walk redirect edges up to ``max_redirect_hops``; return terminal or None.

    Hop count is the number of redirect edges followed from the search-hit root.
    A non-redirect page at the root uses zero hops. Missing map entries and
    cycles drop the trail.
    """
    current_id = root_page_id
    seen: set[int] = set()
    hops = 0

    while True:
        if current_id in seen:
            return None
        seen.add(current_id)

        page = pages_by_id.get(current_id)
        if page is None:
            return None

        target = page.redirect_to_page_id
        if target is None:
            return page

        if hops >= max_redirect_hops:
            return None
        hops += 1
        current_id = target
