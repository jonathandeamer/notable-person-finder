"""Fake ArticleFetcher / ArticleExtractor surfaces for coverage handler tests.

The real HTTP and Trafilatura paths are exercised only in ``test_articles.py``.
Downstream handlers depend on these seams so they never need MockTransport or
HTML fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from notable_person_finder.providers.articles import (
    ArticleAccessDenied,
    ArticleAccessKind,
    ArticleFetchResult,
    ArticleFetchSuccess,
    ArticleTextBlock,
    ExtractedArticle,
    ExtractionQuality,
)
from notable_person_finder.providers.failures import ProviderFailure


@dataclass
class FakeArticleFetcher:
    """Queue-driven ``ArticleFetcher`` for fetch-handler unit tests."""

    results: list[ArticleFetchResult | ProviderFailure] = field(default_factory=list)
    calls: list[str] = field(default_factory=list)

    def fetch_article(self, url: str) -> ArticleFetchResult:
        self.calls.append(url)
        if not self.results:
            raise AssertionError("FakeArticleFetcher has no queued results")
        next_item = self.results.pop(0)
        if isinstance(next_item, ProviderFailure):
            raise next_item
        return next_item


@dataclass
class FakeArticleExtractor:
    """Queue-driven ``ArticleExtractor`` for fetch-handler unit tests."""

    results: list[ExtractedArticle] = field(default_factory=list)
    calls: list[bytes | str] = field(default_factory=list)

    def extract_article(self, html: bytes | str) -> ExtractedArticle:
        self.calls.append(html)
        if not self.results:
            raise AssertionError("FakeArticleExtractor has no queued results")
        return self.results.pop(0)


def sample_fetch_success(
    *,
    requested_url: str = "https://example.com/article",
    final_url: str | None = None,
    html: bytes = b"<html><body><p>hi</p></body></html>",
) -> ArticleFetchSuccess:
    final = final_url if final_url is not None else requested_url
    return ArticleFetchSuccess(
        requested_url=requested_url,
        final_url=final,
        redirect_chain=(),
        status_code=200,
        content_type="text/html; charset=utf-8",
        html=html,
        response_bytes=len(html),
    )


def sample_access_denied(
    *,
    kind: ArticleAccessKind = ArticleAccessKind.NOT_FOUND,
    requested_url: str = "https://example.com/missing",
    status_code: int | None = 404,
) -> ArticleAccessDenied:
    return ArticleAccessDenied(
        kind=kind,
        requested_url=requested_url,
        final_url=None,
        redirect_chain=(),
        status_code=status_code,
        detail=kind.value,
    )


def sample_extracted(
    *,
    title: str | None = "Sample title",
    quality: ExtractionQuality = ExtractionQuality.FULL,
    blocks: tuple[ArticleTextBlock, ...] | None = None,
) -> ExtractedArticle:
    if blocks is None:
        blocks = (
            ArticleTextBlock(
                id="b1",
                text=(
                    "First substantial paragraph of cleaned article body text "
                    "used as a default fake extraction payload."
                ),
            ),
            ArticleTextBlock(
                id="b2",
                text=(
                    "Second paragraph continues so quality stays full under "
                    "the default length thresholds used in tests."
                ),
            ),
        )
    return ExtractedArticle(
        title=title,
        dek=None,
        byline=None,
        published_at=None,
        editorial_labels=(),
        blocks=blocks,
        quality=quality,
        warnings=(),
    )
