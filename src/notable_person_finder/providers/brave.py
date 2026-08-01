"""Brave Web Search adapter: one ``search_web`` request, typed ``SearchPage``.

This module is a translator. It performs exactly one Brave Web Search HTTP
request per ``search_web`` call over the shared transport, maps JSON into
frozen application values, and decides nothing about query plans, screening,
reliability, or follow-up queries. Domain code owns those rules (K7).

Product settings are fixed: global web results, English language, moderate
SafeSearch, silent spellcheck disabled when the API permits it. News is never
requested. Auth is the ``X-Subscription-Token`` header from the env-supplied
API key — never logged, never placed in ``ProviderFailure.detail``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from notable_person_finder.config.models import BraveConfig
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

PROVIDER = "brave"
OPERATION_SEARCH_WEB = "search_web"

_AUTH_HEADER = "X-Subscription-Token"

# Fixed product settings (design §Brave WebSearchClient). Not config knobs.
_COUNTRY = "ALL"
_SEARCH_LANG = "en"
_SAFESEARCH = "moderate"
_RESULT_FILTER = "web"


@dataclass(frozen=True, slots=True)
class SearchResult:
    """One ranked web hit on a single Brave page (1-based rank within the page)."""

    rank: int
    url: str
    title: str | None
    snippet: str | None
    extra_snippets: tuple[str, ...]
    language: str | None
    provider_result_id: str | None


@dataclass(frozen=True, slots=True)
class SearchPage:
    """One Brave Web Search response for a single ``search_web`` call."""

    query: str
    altered_query: str | None
    offset: int
    count: int
    estimated_total: int | None
    more_results: bool
    results: tuple[SearchResult, ...]


class WebSearchClient(Protocol):
    """The seam coverage handlers depend on so tests can supply a fake."""

    def search_web(self, query: str, *, count: int, offset: int) -> SearchPage: ...


class HttpxBraveWebSearchClient:
    """One Brave Web Search request per call; no retries; no workflow decisions."""

    def __init__(
        self,
        transport: HttpTransport,
        *,
        config: BraveConfig,
        api_key: str,
        clock: Clock | None = None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._api_key = api_key
        self._clock = clock if clock is not None else SystemClock()

    def search_web(self, query: str, *, count: int, offset: int) -> SearchPage:
        params: dict[str, str | int] = {
            "q": query,
            "count": count,
            "offset": offset,
            "country": _COUNTRY,
            "search_lang": _SEARCH_LANG,
            "safesearch": _SAFESEARCH,
            "spellcheck": "false",
            "result_filter": _RESULT_FILTER,
        }
        if self._config.extra_snippets:
            # Paid-plan only; see BraveConfig.
            params["extra_snippets"] = "true"
        headers = {_AUTH_HEADER: self._api_key}

        try:
            response = self._transport.request(
                "GET",
                self._config.endpoint,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                headers=headers,
                params=params,
                limit=ResponseLimit.API,
            )
            data = self._parse_json(response)
            return self._translate(data, query=query, count=count, offset=offset)
        except ProviderFailure:
            raise
        except Exception as error:
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail=type(error).__name__,
            ) from error

    def _parse_json(self, response: HttpResponse) -> dict[str, Any]:
        try:
            payload = json.loads(response.content)
        except json.JSONDecodeError as error:
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="JSONDecodeError",
            ) from error
        if not isinstance(payload, dict):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="response_not_object",
            )
        return payload

    def _translate(
        self,
        data: Mapping[str, Any],
        *,
        query: str,
        count: int,
        offset: int,
    ) -> SearchPage:
        query_block = data.get("query")
        altered: str | None = None
        more_results = False
        if isinstance(query_block, Mapping):
            raw_altered = query_block.get("altered")
            if isinstance(raw_altered, str) and raw_altered:
                altered = raw_altered
            more_flag = query_block.get("more_results_available")
            if isinstance(more_flag, bool):
                more_results = more_flag

        web = data.get("web")
        if web is None:
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="missing_web",
            )
        if not isinstance(web, Mapping):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="malformed_web",
            )

        raw_results = web.get("results")
        if raw_results is None:
            raw_results = ()
        if not isinstance(raw_results, Sequence) or isinstance(
            raw_results, (str, bytes)
        ):
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="malformed_results",
            )

        results: list[SearchResult] = []
        for index, item in enumerate(raw_results):
            if not isinstance(item, Mapping):
                continue
            url = item.get("url")
            if not isinstance(url, str) or not url:
                continue
            results.append(
                SearchResult(
                    rank=index + 1,
                    url=url,
                    title=_optional_str(item.get("title")),
                    snippet=_optional_str(item.get("description")),
                    extra_snippets=_string_tuple(item.get("extra_snippets")),
                    language=_optional_str(item.get("language")),
                    provider_result_id=_optional_str(item.get("id")),
                )
            )

        return SearchPage(
            query=query,
            altered_query=altered,
            offset=offset,
            count=count,
            estimated_total=None,
            more_results=more_results,
            results=tuple(results),
        )


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    items: list[str] = []
    for entry in value:
        if isinstance(entry, str) and entry:
            items.append(entry)
    return tuple(items)
