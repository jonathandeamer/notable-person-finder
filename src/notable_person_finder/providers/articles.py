"""Article HTTP fetch and Trafilatura extraction adapters.

This module is a translator. ``HttpxArticleFetcher`` performs exactly one
ordinary GET per ``fetch_article`` over the shared transport (provider key
``article_http``, ``ResponseLimit.ARTICLE``), maps expected access outcomes
into typed values, and never extracts text or judges significance.

``TrafilaturaArticleExtractor`` is pure and network-free: it accepts HTML
bytes or text and returns cleaned metadata plus ordered main-text blocks.
Returned extract DTOs never carry HTML (K3). Failure detail is always a
compact code or ``type(error).__name__`` — never a response body.

No publisher-specific scrapers: cleanup is generic Trafilatura plus
deterministic block splitting.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import trafilatura

from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
)
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
)
from notable_person_finder.runs.clock import Clock, SystemClock

ARTICLE_PROVIDER = "article_http"
OPERATION_FETCH_ARTICLE = "fetch_article"
EXTRACTOR_VERSION = 1

# Drop nav/footer crumbs shorter than this before quality is judged.
_MIN_BLOCK_CHARS = 40
# Total cleaned body under this with no substantial block → empty, not partial.
_EMPTY_BODY_CHAR_LIMIT = 40
# Body length under which a non-empty extract is still only partial evidence.
_PARTIAL_BODY_CHAR_LIMIT = 250

_HTML_MEDIA_TYPES = frozenset({"text/html", "application/xhtml+xml"})


class ArticleAccessKind(StrEnum):
    """Expected inaccessible or non-HTML outcomes (not operational failures)."""

    NOT_FOUND = "not_found"
    AUTHENTICATION_REQUIRED = "authentication_required"
    ACCESS_DENIED = "access_denied"
    UNSUPPORTED_CONTENT = "unsupported_content"
    RESPONSE_TOO_LARGE = "response_too_large"


class ExtractionQuality(StrEnum):
    FULL = "full"
    PARTIAL = "partial"
    EMPTY = "empty"


@dataclass(frozen=True, slots=True)
class ArticleFetchSuccess:
    """Successful GET of HTML-ish bytes held only for the current attempt."""

    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    status_code: int
    content_type: str | None
    html: bytes
    response_bytes: int


@dataclass(frozen=True, slots=True)
class ArticleAccessDenied:
    """Typed expected alternative: inaccessible, unsupported, or too large.

    ``detail`` is compact and never includes HTML or response bodies.
    """

    kind: ArticleAccessKind
    requested_url: str
    final_url: str | None = None
    redirect_chain: tuple[str, ...] = ()
    status_code: int | None = None
    detail: str | None = None


ArticleFetchResult = ArticleFetchSuccess | ArticleAccessDenied


@dataclass(frozen=True, slots=True)
class ArticleTextBlock:
    """One ordered main-text block with a stable id (``b1``, ``b2``, …)."""

    id: str
    text: str


@dataclass(frozen=True, slots=True)
class ExtractedArticle:
    """Cleaned article view after the extract boundary — never carries HTML."""

    title: str | None
    dek: str | None
    byline: str | None
    published_at: str | None
    editorial_labels: tuple[str, ...]
    blocks: tuple[ArticleTextBlock, ...]
    quality: ExtractionQuality
    warnings: tuple[str, ...]


class ArticleFetcher(Protocol):
    """The seam coverage handlers depend on so tests can supply a fake."""

    def fetch_article(self, url: str) -> ArticleFetchResult: ...


class ArticleExtractor(Protocol):
    """Pure extract seam; must not perform network I/O."""

    def extract_article(self, html: bytes | str) -> ExtractedArticle: ...


class HttpxArticleFetcher:
    """One ordinary article GET per call; typed access states; no extract."""

    def __init__(
        self,
        transport: HttpTransport,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._transport = transport
        self._clock = clock if clock is not None else SystemClock()

    def fetch_article(self, url: str) -> ArticleFetchResult:
        try:
            response = self._transport.request(
                "GET",
                url,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                limit=ResponseLimit.ARTICLE,
            )
            return self._translate_success(response)
        except ProviderFailure as failure:
            mapped = self._map_expected_access(failure, requested_url=url)
            if mapped is not None:
                return mapped
            raise
        except Exception as error:
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                detail=type(error).__name__,
            ) from error

    def _translate_success(self, response: HttpResponse) -> ArticleFetchResult:
        content_type = _header_content_type(response.headers)
        if not _is_html_content(content_type, response.content):
            return ArticleAccessDenied(
                kind=ArticleAccessKind.UNSUPPORTED_CONTENT,
                requested_url=response.requested_url,
                final_url=response.final_url,
                redirect_chain=response.redirect_chain,
                status_code=response.status_code,
                detail="unsupported_content_type",
            )
        return ArticleFetchSuccess(
            requested_url=response.requested_url,
            final_url=response.final_url,
            redirect_chain=response.redirect_chain,
            status_code=response.status_code,
            content_type=content_type,
            html=response.content,
            response_bytes=response.decoded_bytes,
        )

    def _map_expected_access(
        self, failure: ProviderFailure, *, requested_url: str
    ) -> ArticleAccessDenied | None:
        """Map expected access outcomes; leave operational failures to raise."""
        status = failure.status_code
        if failure.category is FailureCategory.RESPONSE_TOO_LARGE:
            return ArticleAccessDenied(
                kind=ArticleAccessKind.RESPONSE_TOO_LARGE,
                requested_url=requested_url,
                status_code=status,
                detail="response_too_large",
            )
        if status in {404, 410}:
            return ArticleAccessDenied(
                kind=ArticleAccessKind.NOT_FOUND,
                requested_url=requested_url,
                status_code=status,
                detail="not_found",
            )
        if status == 401 or failure.category is FailureCategory.AUTHENTICATION:
            return ArticleAccessDenied(
                kind=ArticleAccessKind.AUTHENTICATION_REQUIRED,
                requested_url=requested_url,
                status_code=status,
                detail="authentication_required",
            )
        if status in {402, 403, 451} or (
            failure.category is FailureCategory.ACCESS_DENIED
            and status not in {404, 410}
        ):
            return ArticleAccessDenied(
                kind=ArticleAccessKind.ACCESS_DENIED,
                requested_url=requested_url,
                status_code=status,
                detail="access_denied",
            )
        return None


class TrafilaturaArticleExtractor:
    """Network-free Trafilatura extract + deterministic block cleanup."""

    def extract_article(self, html: bytes | str) -> ExtractedArticle:
        document = _as_text(html)
        try:
            result = trafilatura.bare_extraction(
                document,
                include_comments=False,
                include_tables=True,
                include_images=False,
                include_links=False,
                with_metadata=True,
                favor_recall=False,
            )
        except Exception:
            # Never leak markup through exception paths into domain code.
            return ExtractedArticle(
                title=None,
                dek=None,
                byline=None,
                published_at=None,
                editorial_labels=(),
                blocks=(),
                quality=ExtractionQuality.EMPTY,
                warnings=("extract_failed", "empty_body"),
            )

        if result is None:
            return ExtractedArticle(
                title=None,
                dek=None,
                byline=None,
                published_at=None,
                editorial_labels=(),
                blocks=(),
                quality=ExtractionQuality.EMPTY,
                warnings=("empty_body",),
            )

        title = _optional_str(getattr(result, "title", None))
        dek = _optional_str(getattr(result, "description", None))
        byline = _optional_str(getattr(result, "author", None))
        published_at = _optional_str(getattr(result, "date", None))
        labels = _labels_from_document(result)
        raw_text = _optional_str(getattr(result, "text", None)) or _optional_str(
            getattr(result, "raw_text", None)
        )
        blocks = _blocks_from_text(
            raw_text,
            title=title,
            dek=dek,
            byline=byline,
        )
        quality, warnings = _quality_for(blocks)
        return ExtractedArticle(
            title=title,
            dek=dek,
            byline=byline,
            published_at=published_at,
            editorial_labels=labels,
            blocks=blocks,
            quality=quality,
            warnings=warnings,
        )


def _as_text(html: bytes | str) -> str:
    if isinstance(html, bytes):
        return html.decode("utf-8", errors="replace")
    return html


def _optional_str(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _labels_from_document(document: object) -> tuple[str, ...]:
    labels: list[str] = []
    for attr in ("tags", "categories"):
        raw = getattr(document, attr, None)
        if raw is None:
            continue
        if isinstance(raw, str):
            piece = raw.strip()
            if piece:
                labels.append(piece)
            continue
        if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
            for item in raw:
                if isinstance(item, str):
                    piece = item.strip()
                    if piece and piece not in labels:
                        labels.append(piece)
    return tuple(labels)


def _normalize_line(text: str) -> str:
    return " ".join(text.split()).casefold()


def _blocks_from_text(
    text: str | None,
    *,
    title: str | None,
    dek: str | None,
    byline: str | None,
) -> tuple[ArticleTextBlock, ...]:
    if not text:
        return ()
    skip = {
        _normalize_line(value)
        for value in (title, dek, byline)
        if value is not None and value.strip()
    }
    # Also skip a common "By Author" echo of the byline.
    if byline is not None:
        skip.add(_normalize_line(f"By {byline}"))

    candidates: list[str] = []
    for paragraph in text.replace("\r\n", "\n").split("\n"):
        piece = " ".join(paragraph.split()).strip()
        if not piece:
            continue
        if _normalize_line(piece) in skip:
            continue
        # Generic short-line drop: nav labels, lone links, cookie crumbs.
        if len(piece) < _MIN_BLOCK_CHARS:
            continue
        candidates.append(piece)

    return tuple(
        ArticleTextBlock(id=f"b{index}", text=piece)
        for index, piece in enumerate(candidates, start=1)
    )


def _quality_for(
    blocks: tuple[ArticleTextBlock, ...],
) -> tuple[ExtractionQuality, tuple[str, ...]]:
    if not blocks:
        return ExtractionQuality.EMPTY, ("empty_body",)
    total = sum(len(block.text) for block in blocks)
    if total < _EMPTY_BODY_CHAR_LIMIT:
        return ExtractionQuality.EMPTY, ("empty_body",)
    if total < _PARTIAL_BODY_CHAR_LIMIT:
        return ExtractionQuality.PARTIAL, ("short_body",)
    return ExtractionQuality.FULL, ()


def _header_content_type(headers: object) -> str | None:
    if not isinstance(headers, dict):
        try:
            value = headers.get("content-type")  # type: ignore[attr-defined]
        except Exception:
            return None
    else:
        value = headers.get("content-type")
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def _media_type(content_type: str | None) -> str | None:
    if content_type is None:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def _looks_like_html(body: bytes) -> bool:
    sample = body[:2048].lstrip().lower()
    if sample.startswith((b"<!doctype html", b"<html")):
        return True
    # Tolerant: some servers omit doctype.
    return b"<html" in sample[:512] or b"<head" in sample[:512]


def _is_html_content(content_type: str | None, body: bytes) -> bool:
    media = _media_type(content_type)
    if media in _HTML_MEDIA_TYPES:
        return True
    if media is None:
        return _looks_like_html(body)
    return media.startswith("text/") and _looks_like_html(body)
