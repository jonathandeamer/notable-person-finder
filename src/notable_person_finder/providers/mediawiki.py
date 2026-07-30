"""MediaWiki Action API adapter: search pages and page-fact batches.

This module is a translator. It performs exactly one HTTP request per method
call over the shared transport, maps JSON into frozen application values, and
decides nothing about identity, namespaces, disambiguation, or whether a search
is complete enough for a negative judgment. Domain code owns those rules.

Failure detail is always a compact code or ``type(error).__name__`` — never a
response body, never the full query string — matching the feeds adapter
precedent. There is no MediaWiki authentication secret.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote, urlsplit

from notable_person_finder.config.models import MediaWikiConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    parse_retry_after,
)
from notable_person_finder.providers.transport import (
    HttpResponse,
    HttpTransport,
    ResponseLimit,
)
from notable_person_finder.runs.clock import Clock, SystemClock

PROVIDER = "mediawiki"
SEARCH_OPERATION = "search_pages"
PAGE_FACTS_OPERATION = "get_page_facts"

_DEFAULT_SRLIMIT = 10
_DEFAULT_MAX_EXTRACT_CHARACTERS = 1200
_DEFAULT_MAX_CATEGORIES_PER_PAGE = 20

# JSON-body error codes MediaWiki returns with HTTP 200 (maxlag is the common
# case; it also sets Retry-After). Map only known compact codes — unknown codes
# become configuration/malformed rather than leaking ``info`` text.
_JSON_ERROR_CATEGORIES: Mapping[str, FailureCategory] = {
    "maxlag": FailureCategory.RATE_LIMIT,
    "ratelimited": FailureCategory.RATE_LIMIT,
    "readonly": FailureCategory.PROVIDER_UNAVAILABLE,
    "internal_api_error_dbqueryerror": FailureCategory.TRANSIENT_SERVER_ERROR,
    "internal_api_error_dbconnectionerror": FailureCategory.TRANSIENT_SERVER_ERROR,
}


@dataclass(frozen=True, slots=True)
class MediaWikiSearchHit:
    page_id: int | None
    title: str
    snippet: str | None = None
    timestamp: str | None = None


@dataclass(frozen=True, slots=True)
class MediaWikiSearchPage:
    hits: tuple[MediaWikiSearchHit, ...]
    continuation: str | None
    more_results: bool
    provider_total_hits: int | None


@dataclass(frozen=True, slots=True)
class MediaWikiPageFact:
    page_id: int
    requested_title: str | None
    canonical_title: str
    namespace: int
    missing: bool
    redirect_to_page_id: int | None
    redirect_to_title: str | None
    is_disambiguation: bool
    canonical_url: str
    description: str | None
    extract: str | None
    categories: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MediaWikiPageFactsBatch:
    pages: tuple[MediaWikiPageFact, ...]


class MediaWikiClient(Protocol):
    """The seam Wikipedia handlers depend on so tests can supply a fake."""

    def search_pages(
        self, query: str, *, continuation: str | None
    ) -> MediaWikiSearchPage: ...

    def get_page_facts(self, page_ids: Sequence[int]) -> MediaWikiPageFactsBatch: ...


class HttpxMediaWikiClient:
    """One Action API request per method; no retries; no workflow decisions."""

    def __init__(
        self,
        transport: HttpTransport,
        *,
        config: MediaWikiConfig,
        srlimit: int = _DEFAULT_SRLIMIT,
        max_extract_characters: int = _DEFAULT_MAX_EXTRACT_CHARACTERS,
        max_categories_per_page: int = _DEFAULT_MAX_CATEGORIES_PER_PAGE,
        clock: Clock | None = None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._srlimit = srlimit
        self._max_extract_characters = max_extract_characters
        self._max_categories_per_page = max_categories_per_page
        self._clock = clock if clock is not None else SystemClock()
        self._wiki_origin = _wiki_origin(config.endpoint)

    def search_pages(
        self, query: str, *, continuation: str | None
    ) -> MediaWikiSearchPage:
        params: dict[str, str | int] = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "search",
            "srsearch": query,
            "srlimit": self._srlimit,
            "srprop": "snippet|timestamp|size|wordcount",
            "maxlag": self._config.maxlag_seconds,
        }
        if continuation is not None:
            params["sroffset"] = continuation

        try:
            response = self._request(
                params,
                operation=SEARCH_OPERATION,
            )
            data = self._parse_json(response, operation=SEARCH_OPERATION)
            self._raise_if_api_error(data, response, operation=SEARCH_OPERATION)
            return self._translate_search(data)
        except ProviderFailure:
            raise
        except Exception as error:
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=SEARCH_OPERATION,
                detail=type(error).__name__,
            ) from error

    def get_page_facts(self, page_ids: Sequence[int]) -> MediaWikiPageFactsBatch:
        if not page_ids:
            raise ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=PROVIDER,
                operation=PAGE_FACTS_OPERATION,
                detail="empty_page_ids",
            )

        # Preserve caller order; drop duplicate ids so the request stays bounded.
        ordered_ids: list[int] = []
        seen: set[int] = set()
        for page_id in page_ids:
            if page_id in seen:
                continue
            seen.add(page_id)
            ordered_ids.append(page_id)

        params: dict[str, str | int] = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "pageids": "|".join(str(page_id) for page_id in ordered_ids),
            "prop": "info|pageprops|description|extracts|categories",
            "inprop": "url",
            "exintro": 1,
            "explaintext": 1,
            "exlimit": "max",
            "cllimit": self._max_categories_per_page,
            "redirects": 1,
            "maxlag": self._config.maxlag_seconds,
        }

        try:
            response = self._request(
                params,
                operation=PAGE_FACTS_OPERATION,
            )
            data = self._parse_json(response, operation=PAGE_FACTS_OPERATION)
            self._raise_if_api_error(data, response, operation=PAGE_FACTS_OPERATION)
            return self._translate_page_facts(data, requested_ids=ordered_ids)
        except ProviderFailure:
            raise
        except Exception as error:
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=PAGE_FACTS_OPERATION,
                detail=type(error).__name__,
            ) from error

    def _request(
        self,
        params: Mapping[str, str | int],
        *,
        operation: str,
    ) -> HttpResponse:
        return self._transport.request(
            "GET",
            self._config.endpoint,
            provider=PROVIDER,
            operation=operation,
            params=params,
            limit=ResponseLimit.API,
        )

    def _parse_json(self, response: HttpResponse, *, operation: str) -> dict[str, Any]:
        try:
            payload = json.loads(response.content)
        except json.JSONDecodeError as error:
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=operation,
                detail="JSONDecodeError",
            ) from error
        if not isinstance(payload, dict):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=operation,
                detail="response_not_object",
            )
        return payload

    def _raise_if_api_error(
        self,
        data: Mapping[str, Any],
        response: HttpResponse,
        *,
        operation: str,
    ) -> None:
        error = data.get("error")
        if error is None:
            return
        code = "error"
        if isinstance(error, Mapping):
            raw_code = error.get("code")
            if isinstance(raw_code, str) and raw_code:
                code = raw_code
        category = _JSON_ERROR_CATEGORIES.get(code, FailureCategory.CONFIGURATION)
        # Prefer the HTTP header when present (maxlag always sets it).
        retry_after_ms = parse_retry_after(
            response.headers.get("retry-after"),
            now=self._clock.now(),
        )
        raise ProviderFailure(
            category,
            provider=PROVIDER,
            operation=operation,
            status_code=response.status_code if response.status_code >= 400 else None,
            retry_after_ms=retry_after_ms,
            detail=code,
        )

    def _translate_search(self, data: Mapping[str, Any]) -> MediaWikiSearchPage:
        query = data.get("query")
        if not isinstance(query, Mapping):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=SEARCH_OPERATION,
                detail="missing_query",
            )

        raw_hits = query.get("search")
        if raw_hits is None:
            raw_hits = ()
        if not isinstance(raw_hits, Sequence) or isinstance(raw_hits, (str, bytes)):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=SEARCH_OPERATION,
                detail="malformed_search",
            )

        hits: list[MediaWikiSearchHit] = []
        for item in raw_hits:
            if not isinstance(item, Mapping):
                continue
            title = item.get("title")
            if not isinstance(title, str) or not title:
                continue
            page_id = item.get("pageid")
            hits.append(
                MediaWikiSearchHit(
                    page_id=page_id if isinstance(page_id, int) else None,
                    title=title,
                    snippet=_optional_str(item.get("snippet")),
                    timestamp=_optional_str(item.get("timestamp")),
                )
            )

        total: int | None = None
        searchinfo = query.get("searchinfo")
        if isinstance(searchinfo, Mapping):
            raw_total = searchinfo.get("totalhits")
            if isinstance(raw_total, int):
                total = raw_total

        continuation: str | None = None
        cont = data.get("continue")
        if isinstance(cont, Mapping):
            offset = cont.get("sroffset")
            if isinstance(offset, int):
                continuation = str(offset)
            elif isinstance(offset, str) and offset:
                continuation = offset

        return MediaWikiSearchPage(
            hits=tuple(hits),
            continuation=continuation,
            more_results=continuation is not None,
            provider_total_hits=total,
        )

    def _translate_page_facts(
        self,
        data: Mapping[str, Any],
        *,
        requested_ids: Sequence[int],
    ) -> MediaWikiPageFactsBatch:
        query = data.get("query")
        if not isinstance(query, Mapping):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=PAGE_FACTS_OPERATION,
                detail="missing_query",
            )

        raw_pages = query.get("pages")
        pages_list = _as_page_list(raw_pages)
        redirects = query.get("redirects")
        redirect_entries: list[Mapping[str, Any]] = []
        if isinstance(redirects, Sequence) and not isinstance(redirects, (str, bytes)):
            redirect_entries = [
                entry for entry in redirects if isinstance(entry, Mapping)
            ]

        result_pages: list[MediaWikiPageFact] = []
        pages_by_title: dict[str, MediaWikiPageFact] = {}
        result_ids: set[int] = set()

        for raw in pages_list:
            fact = self._page_to_fact(raw, requested_title=None)
            result_pages.append(fact)
            result_ids.add(fact.page_id)
            if not fact.missing:
                pages_by_title[fact.canonical_title] = fact

        # Requested ids absent from the result set are redirect sources (missing
        # pages still appear under their requested id with missing=true).
        redirected_source_ids = [
            page_id for page_id in requested_ids if page_id not in result_ids
        ]
        unassigned_sources = list(redirected_source_ids)
        entries_without_fromid = [
            entry
            for entry in redirect_entries
            if _optional_int(entry.get("fromid")) is None
        ]

        for entry in redirect_entries:
            from_title = _optional_str(entry.get("from"))
            to_title = _optional_str(entry.get("to"))
            if from_title is None or to_title is None:
                continue
            source_id = _optional_int(entry.get("fromid"))
            unambiguous = (
                len(unassigned_sources) == 1 and len(entries_without_fromid) == 1
            )
            if source_id is None and unambiguous:
                source_id = unassigned_sources[0]
            if source_id is None:
                continue
            if source_id in unassigned_sources:
                unassigned_sources.remove(source_id)
            target = pages_by_title.get(to_title)
            result_pages.append(
                MediaWikiPageFact(
                    page_id=source_id,
                    requested_title=from_title,
                    canonical_title=from_title,
                    namespace=0,
                    missing=False,
                    redirect_to_page_id=target.page_id if target is not None else None,
                    redirect_to_title=to_title,
                    is_disambiguation=False,
                    canonical_url=_title_url(self._wiki_origin, from_title),
                    description=None,
                    extract=None,
                    categories=(),
                )
            )

        return MediaWikiPageFactsBatch(pages=tuple(result_pages))

    def _page_to_fact(
        self, raw: Mapping[str, Any], *, requested_title: str | None
    ) -> MediaWikiPageFact:
        missing = bool(raw.get("missing"))
        page_id = raw.get("pageid")
        if not isinstance(page_id, int):
            # formatversion=1 missing pages use negative keys; v2 always has pageid.
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=PAGE_FACTS_OPERATION,
                detail="missing_pageid",
            )

        title = _optional_str(raw.get("title"))
        if title is None:
            title = f"#{page_id}"

        namespace = raw.get("ns")
        if not isinstance(namespace, int):
            namespace = -1 if missing else 0

        pageprops = raw.get("pageprops")
        is_disambiguation = isinstance(pageprops, Mapping) and (
            "disambiguation" in pageprops
        )

        canonical_url = _optional_str(raw.get("canonicalurl")) or _optional_str(
            raw.get("fullurl")
        )
        if canonical_url is None:
            canonical_url = (
                f"{self._wiki_origin}/?curid={page_id}"
                if missing
                else _title_url(self._wiki_origin, title)
            )

        description = _optional_str(raw.get("description"))
        extract = _optional_str(raw.get("extract"))
        if extract is not None and len(extract) > self._max_extract_characters:
            extract = extract[: self._max_extract_characters]
        if description is not None and len(description) > self._max_extract_characters:
            description = description[: self._max_extract_characters]

        categories = _category_titles(
            raw.get("categories"),
            limit=self._max_categories_per_page,
        )

        # Unfollowed redirect pages (redirect true / "") without a target in
        # this response leave redirect fields null — callers still see the page.
        # When redirects=1 was used, sources are synthesised separately.
        return MediaWikiPageFact(
            page_id=page_id,
            requested_title=requested_title,
            canonical_title=title,
            namespace=namespace,
            missing=missing,
            redirect_to_page_id=None,
            redirect_to_title=None,
            is_disambiguation=is_disambiguation,
            canonical_url=canonical_url,
            description=description,
            extract=extract,
            categories=categories,
        )


def _wiki_origin(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    # endpoint is https://en.wikipedia.org/w/api.php → origin without path
    return f"{parsed.scheme}://{parsed.netloc}"


def _title_url(origin: str, title: str) -> str:
    # MediaWiki titles use underscores in path segments.
    return f"{origin}/wiki/{quote(title.replace(' ', '_'), safe="/:()!$&'*+,;=:@")}"


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) else None


def _as_page_list(raw_pages: object) -> list[Mapping[str, Any]]:
    if raw_pages is None:
        return []
    if isinstance(raw_pages, Mapping):
        return [page for page in raw_pages.values() if isinstance(page, Mapping)]
    if isinstance(raw_pages, Sequence) and not isinstance(raw_pages, (str, bytes)):
        return [page for page in raw_pages if isinstance(page, Mapping)]
    return []


def _category_titles(raw: object, *, limit: int) -> tuple[str, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    titles: list[str] = []
    for item in raw:
        if len(titles) >= limit:
            break
        if not isinstance(item, Mapping):
            continue
        title = item.get("title")
        if isinstance(title, str) and title:
            titles.append(title)
    return tuple(titles)
