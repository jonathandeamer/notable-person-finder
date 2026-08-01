"""Fake WebSearchClient surfaces for coverage handler tests.

The real Brave HTTP path is exercised only in ``test_brave.py``. Downstream
handlers depend on this seam so they never need MockTransport or fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from notable_person_finder.providers.brave import SearchPage, SearchResult
from notable_person_finder.providers.failures import ProviderFailure


@dataclass
class FakeWebSearchClient:
    """Queue-driven ``WebSearchClient`` for plan/handler unit tests."""

    pages: list[SearchPage | ProviderFailure] = field(default_factory=list)
    calls: list[dict[str, object]] = field(default_factory=list)

    def search_web(self, query: str, *, count: int, offset: int) -> SearchPage:
        self.calls.append({"query": query, "count": count, "offset": offset})
        if not self.pages:
            raise AssertionError("FakeWebSearchClient has no queued pages")
        next_item = self.pages.pop(0)
        if isinstance(next_item, ProviderFailure):
            raise next_item
        return next_item


def sample_search_page(
    *,
    query: str = "Ada Lovelace",
    altered_query: str | None = None,
    offset: int = 0,
    count: int = 10,
    more_results: bool = False,
    results: tuple[SearchResult, ...] | None = None,
) -> SearchPage:
    if results is None:
        results = (
            SearchResult(
                rank=1,
                url="https://en.wikipedia.org/wiki/Ada_Lovelace",
                title="Ada Lovelace - Wikipedia",
                snippet="English mathematician.",
                extra_snippets=(),
                language="en",
                provider_result_id=None,
            ),
        )
    return SearchPage(
        query=query,
        altered_query=altered_query,
        offset=offset,
        count=count,
        estimated_total=None,
        more_results=more_results,
        results=results,
    )
