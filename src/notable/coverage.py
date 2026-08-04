"""Brave search, publisher-policy screening, article fetch + extract, and
the assess_article call. Not a lead outcome yet -- Phase 4's rank.assess
consumes this module's output."""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources
from typing import Any, Literal

import trafilatura

from notable import policy
from notable.config import Config
from notable.coverage_contract import (
    ArticleAssessment,
    AssessInvalid,
    assessment_schema,
    build_article_passage,
    validate_assessment,
)
from notable.detect_contract import DetectedMention
from notable.errors import Incomplete, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient

logger = logging.getLogger(__name__)

_HTML_CONTENT_TYPES = ("text/html", "application/xhtml+xml")


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("assess_article.md")
        .read_text(encoding="utf-8")
    )


def research(
    mention: DetectedMention, config: Config, transport: Transport, llm: LlmClient
) -> tuple[ArticleAssessment, ...]:
    """Search, screen, fetch, and assess English-language coverage of
    `mention`.

    Raises `Incomplete` if the Brave search fails, an assess_article call
    fails, or its output is domain-rejected -- the whole mention retries on
    a later run. A single article's fetch, extraction, or content-type
    failure just drops that article and continues; a dead search result is
    not evidence of a technical failure in this system. Zero surviving
    articles returns `()` with zero model calls, never `Incomplete` -- an
    empty result honestly means the search ran and found nothing usable.
    `BudgetExceeded` propagates unchanged.
    """
    try:
        hits = _search_brave(mention.exact_name, config, transport)
    except ProviderFailure as error:
        logger.warning("brave search failed for %r: %s", mention.exact_name, error)
        raise Incomplete(f"brave search failed for {mention.exact_name!r}") from error

    survivors: list[tuple[str, Literal["curated_eligible", "unclassified"]]] = []
    for url in hits:
        status = policy.classify(url, config.coverage.source_policy_path)
        if status == "curated_ineligible":
            continue
        survivors.append((url, status))
        if len(survivors) >= config.brave.max_articles_per_mention:
            break

    assessments: list[ArticleAssessment] = []
    for url, status in survivors:
        text = _fetch_and_extract(url, config, transport)
        if text is None:
            continue
        passage = build_article_passage(
            text, max_characters=config.coverage.max_article_characters
        )
        assessments.append(_assess_article(mention, url, status, passage, config, llm))
    return tuple(assessments)


def _parsed_json(response: Any, *, context: str) -> dict[str, Any]:
    try:
        data = response.json()
    except (ValueError, TypeError) as error:
        raise ProviderFailure(
            f"unreadable Brave {context} response: {error}", permanent=False
        ) from error
    if not isinstance(data, dict):
        raise ProviderFailure(
            f"unreadable Brave {context} response: not an object", permanent=False
        )
    return data


def _search_brave(name: str, config: Config, transport: Transport) -> tuple[str, ...]:
    response = transport.request(
        provider="brave",
        method="GET",
        url=config.brave.endpoint,
        params={"q": name, "count": config.brave.max_search_results},
        ttl_seconds=config.cache.discovery_ttl_seconds,
        auth_token=config.brave_api_key,
        auth_header="X-Subscription-Token",
    )
    data = _parsed_json(response, context="search")
    try:
        results = data.get("web", {}).get("results", [])
        return tuple(result["url"] for result in results)
    except (KeyError, TypeError, AttributeError) as error:
        raise ProviderFailure(
            f"malformed Brave search response: {error}", permanent=False
        ) from error


def _fetch_and_extract(url: str, config: Config, transport: Transport) -> str | None:
    try:
        response = transport.request(
            provider="article",
            method="GET",
            url=url,
            ttl_seconds=None,  # Stable class: permanent, per the master spec.
            max_bytes=config.coverage.max_article_bytes,
        )
    except ProviderFailure as error:
        logger.info("article fetch failed for %s: %s", url, error)
        return None

    content_type = (response.content_type or "").split(";")[0].strip().lower()
    if content_type not in _HTML_CONTENT_TYPES:
        logger.info("skipping non-HTML article %s (%r)", url, content_type)
        return None

    extracted = trafilatura.extract(
        response.text,
        favor_precision=True,
        include_comments=False,
        include_tables=False,
    )
    if not extracted:
        logger.info("extraction produced no usable text for %s", url)
        return None
    return extracted


def _assess_article(
    mention: DetectedMention,
    url: str,
    status: Literal["curated_eligible", "unclassified"],
    passage: Any,
    config: Config,
    llm: LlmClient,
) -> ArticleAssessment:
    payload = {
        "task": "assess_article",
        "mention_name": mention.exact_name,
        "identity_facts": [
            {"kind": fact.kind, "value": fact.value} for fact in mention.identity_facts
        ],
        "screening_status": status,
        "article_url": url,
        "passages": [
            {"id": passage.id, "text": passage.text, "truncated": passage.truncated}
        ],
    }

    held: list[Any] = []

    def _validate(raw: dict[str, Any]) -> None:
        held.append(validate_assessment(raw))

    try:
        llm.structured(
            task="assess_article",
            model=config.assess.model,
            system=_system_prompt(),
            user_payload=payload,
            schema=assessment_schema(),
            max_completion_tokens=config.assess.max_completion_tokens,
            reasoning_effort=config.assess.reasoning_effort,
            timeout=config.transport.llm_read_timeout_seconds,
            validate=_validate,
        )
    except ProviderFailure as error:
        logger.warning(
            "assess_article failed for %s (%r): %s", url, mention.exact_name, error
        )
        raise Incomplete(f"assess_article failed for {url}") from error
    except AssessInvalid as error:
        logger.warning(
            "assess_article output rejected for %s (%r): %s",
            url,
            mention.exact_name,
            error,
        )
        raise Incomplete(f"assess_article output rejected for {url}") from error

    output = held[0]
    return ArticleAssessment(
        url=url,
        screening_status=status,
        person_relation=output.person_relation,
        coverage_depth=output.coverage_depth,
        content_types=output.content_types,
        subject_relationship=output.subject_relationship,
        signals=output.signals,
        rationale=output.rationale,
    )
