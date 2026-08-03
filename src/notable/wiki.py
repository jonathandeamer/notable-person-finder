"""One MediaWiki wave plus at most one model call per mention: does an
English Wikipedia biography already describe this person?"""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources
from typing import Any

from notable.config import Config
from notable.detect_contract import DetectedMention
from notable.errors import Incomplete, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient
from notable.wiki_contract import (
    Candidate,
    MatchInvalid,
    MatchOutput,
    MatchVerdict,
    PageFact,
    build_candidates,
    match_schema,
    validate_match,
)

logger = logging.getLogger(__name__)


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("match_wikipedia_identity.md")
        .read_text(encoding="utf-8")
    )


def match(
    mention: DetectedMention, config: Config, transport: Transport, llm: LlmClient
) -> MatchVerdict:
    """Decide whether `mention` already has a current English Wikipedia page.

    Raises `Incomplete` if a MediaWiki call, the model call, or its
    validation fails: the mention's item retries on a later run. Never
    convert a technical failure into a semantic verdict. `BudgetExceeded`
    propagates unchanged -- it ends the whole pass, not just this mention.
    """
    try:
        hit_ids, truncated = _search(mention.exact_name, config, transport)
        pages = _facts(config, transport, hit_ids)
        redirect_ids = tuple(
            page.page_id for page in pages if page.is_redirect and not page.missing
        )
        if redirect_ids:
            resolved = _facts(config, transport, redirect_ids, follow_redirects=True)
            pages = tuple(page for page in pages if not page.is_redirect) + resolved
    except ProviderFailure as error:
        logger.warning(
            "wikipedia retrieval failed for %r: %s", mention.exact_name, error
        )
        raise Incomplete(
            f"wikipedia retrieval failed for {mention.exact_name!r}"
        ) from error

    candidates = build_candidates(
        pages,
        max_extract_characters=config.mediawiki.max_extract_characters,
        max_categories_per_page=config.mediawiki.max_categories_per_page,
    )

    if not candidates:
        # Deterministic short-circuits: no model call is made in either
        # case. See the design doc's "Empty-candidate short-circuit"
        # section -- zero-and-truncated is `uncertain`, never `Incomplete`,
        # because the search is cached and a later run would hit the
        # identical dead end and abandon the item without ever reaching
        # coverage research.
        if truncated:
            return MatchVerdict(
                outcome="uncertain",
                selected_page_id=None,
                rationale="search was truncated and left no safe candidates",
            )
        return MatchVerdict(
            outcome="no_matching_page",
            selected_page_id=None,
            rationale="search returned no biography candidates",
        )

    return _ask_model(mention, candidates, truncated, config, llm)


def _search(
    name: str, config: Config, transport: Transport
) -> tuple[tuple[int, ...], bool]:
    response = transport.request(
        provider="mediawiki",
        method="GET",
        url=config.mediawiki.endpoint,
        params={
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "search",
            "srsearch": name,
            "srlimit": config.mediawiki.max_candidates,
            "maxlag": config.mediawiki.maxlag_seconds,
        },
        ttl_seconds=config.cache.discovery_ttl_seconds,
    )
    data = response.json()
    hits = tuple(hit["pageid"] for hit in data.get("query", {}).get("search", []))
    return hits, "continue" in data


def _facts(
    config: Config,
    transport: Transport,
    page_ids: tuple[int, ...],
    *,
    follow_redirects: bool = False,
) -> tuple[PageFact, ...]:
    if not page_ids:
        return ()
    params: dict[str, Any] = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "pageids": "|".join(str(page_id) for page_id in page_ids),
        "prop": "info|description|extracts|categories|pageprops",
        "explaintext": 1,
        "exchars": config.mediawiki.max_extract_characters,
        "cllimit": config.mediawiki.max_categories_per_page,
        "ppprop": "disambiguation",
        "maxlag": config.mediawiki.maxlag_seconds,
    }
    if follow_redirects:
        params["redirects"] = 1
    response = transport.request(
        provider="mediawiki",
        method="GET",
        url=config.mediawiki.endpoint,
        params=params,
        ttl_seconds=config.cache.discovery_ttl_seconds,
    )
    data = response.json()
    return tuple(_parse_page(page) for page in data.get("query", {}).get("pages", []))


def _parse_page(page: dict[str, Any]) -> PageFact:
    categories = tuple(
        category["title"].removeprefix("Category:")
        for category in page.get("categories", [])
    )
    return PageFact(
        page_id=page["pageid"],
        title=page.get("title", ""),
        namespace=page.get("ns", -1),
        missing=bool(page.get("missing", False)),
        is_redirect=bool(page.get("redirect", False)),
        is_disambiguation="disambiguation" in page.get("pageprops", {}),
        description=page.get("description"),
        extract=page.get("extract") or None,
        categories=categories,
    )


def _ask_model(
    mention: DetectedMention,
    candidates: tuple[Candidate, ...],
    truncated: bool,
    config: Config,
    llm: LlmClient,
) -> MatchVerdict:
    payload = {
        "task": "match_wikipedia_identity",
        "mention_name": mention.exact_name,
        "identity_facts": [
            {"kind": fact.kind, "value": fact.value} for fact in mention.identity_facts
        ],
        "candidates": [
            {
                "page_id": candidate.page_id,
                "title": candidate.title,
                "facts": [
                    {"id": fact.id, "field": fact.field, "text": fact.text}
                    for fact in candidate.facts
                ],
            }
            for candidate in candidates
        ],
    }

    held: list[MatchOutput] = []

    def _validate(raw: dict[str, Any]) -> None:
        held.append(validate_match(raw, candidates=candidates, truncated=truncated))

    try:
        llm.structured(
            task="match_wikipedia_identity",
            model=config.match.model,
            system=_system_prompt(),
            user_payload=payload,
            schema=match_schema(),
            max_completion_tokens=config.match.max_completion_tokens,
            reasoning_effort=config.match.reasoning_effort,
            timeout=config.transport.llm_read_timeout_seconds,
            validate=_validate,
        )
    except ProviderFailure as error:
        logger.warning(
            "match_wikipedia_identity failed for %r: %s", mention.exact_name, error
        )
        raise Incomplete(
            f"match_wikipedia_identity failed for {mention.exact_name!r}"
        ) from error
    except MatchInvalid as error:
        logger.warning(
            "match_wikipedia_identity output rejected for %r: %s",
            mention.exact_name,
            error,
        )
        raise Incomplete(
            f"match_wikipedia_identity output rejected for {mention.exact_name!r}"
        ) from error

    output = held[0]
    return MatchVerdict(
        outcome=output.outcome,
        selected_page_id=output.selected_page_id,
        rationale=output.rationale,
    )
