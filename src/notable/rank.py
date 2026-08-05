"""Lead outcome, selection, and ordering. Pure policy; no I/O."""

from __future__ import annotations

import unicodedata
from dataclasses import asdict, dataclass, is_dataclass, replace
from typing import Any

from notable.policy import canonical_domain

DISQUALIFYING = frozenset({"listing", "announcement", "press_release", "sponsored"})


def identity_key(name: str) -> str:
    """Opaque scalar: normalized name in the MVP. Rank owns construction."""
    return " ".join(unicodedata.normalize("NFKC", name).casefold().split())


@dataclass(frozen=True, slots=True)
class Lead:
    identity_key: str
    display_name: str
    source_url: str
    publisher_label: str
    wikipedia_verdict: Any
    outcome: str
    article_assessments: tuple[Any, ...]
    rank_tuple: tuple[Any, ...] | None = None
    namesake_urls: tuple[str, ...] = ()
    rationale: str = ""
    qualifying_domains: tuple[str, ...] = ()
    explanation: str = ""


def _content_ok(article: Any) -> bool:
    return not any(ct in DISQUALIFYING for ct in getattr(article, "content_types", ()))


def is_qualifying_article(article: Any) -> bool:
    return (
        getattr(article, "person_relation", "") == "same_person"
        and getattr(article, "coverage_depth", "") == "significant"
        and getattr(article, "screening_status", "") == "curated_eligible"
        and getattr(article, "subject_relationship", "") == "editorially_independent"
        and _content_ok(article)
    )


def is_possible_fallback(article: Any) -> bool:
    """possible_lead reasons 2–3. Fully qualifying articles are excluded here."""
    if getattr(
        article, "person_relation", ""
    ) != "same_person" or is_qualifying_article(article):
        return False
    depth = getattr(article, "coverage_depth", "")
    if (
        depth == "significant"
        and getattr(article, "screening_status", "") == "unclassified"
    ):
        return True
    return depth in ("significant", "passing") and _content_ok(article)


def assess(
    mention: Any,
    wikipedia_verdict: Any,
    articles: tuple[Any, ...],
    config: Any,
    item: Any,
) -> Lead:
    for a in articles:
        if (
            canonical_domain(getattr(a, "url", "")).endswith("wikipedia.org")
            and getattr(a, "person_relation", "") == "same_person"
        ):
            key = identity_key(mention.canonical_name)
            return Lead(
                identity_key=key,
                display_name=mention.canonical_name,
                source_url=getattr(item, "url", ""),
                publisher_label=getattr(item, "publisher_label", ""),
                wikipedia_verdict=wikipedia_verdict,
                outcome="insufficient_evidence",
                article_assessments=articles,
                rationale=getattr(mention, "rationale", ""),
                explanation="insufficient_evidence; Wikipedia page discovered during coverage research",
            )

    qualifying = [a for a in articles if is_qualifying_article(a)]
    domains = sorted(
        {d for a in qualifying if (d := canonical_domain(getattr(a, "url", "")))}
    )
    if qualifying:
        outcome = (
            "promising_lead"
            if len(domains) >= config.promising_domain_threshold
            else "possible_lead"
        )
    elif any(is_possible_fallback(a) for a in articles):
        outcome = "possible_lead"
    else:
        outcome = "insufficient_evidence"
    key = identity_key(mention.canonical_name)
    rank_tuple = None
    if outcome != "insufficient_evidence":
        wiki = (
            0 if getattr(wikipedia_verdict, "outcome", "") == "no_matching_page" else 1
        )
        rank_tuple = (0 if outcome == "promising_lead" else 1, wiki, -len(domains), key)
    return Lead(
        identity_key=key,
        display_name=mention.canonical_name,
        source_url=getattr(item, "url", ""),
        publisher_label=getattr(item, "publisher_label", ""),
        wikipedia_verdict=wikipedia_verdict,
        outcome=outcome,
        article_assessments=articles,
        rank_tuple=rank_tuple,
        rationale=getattr(mention, "rationale", ""),
        qualifying_domains=tuple(domains),
        explanation=f"{outcome}; {len(domains)} qualifying domain(s)",
    )


def shortlist(
    leads: list[Lead], store: Any, config: Any
) -> tuple[list[Lead], list[str]]:
    valid = [
        lead
        for lead in leads
        if lead.outcome != "insufficient_evidence"
        and not store.is_suppressed(
            lead.identity_key, max_days=config.resurface_after_days
        )
    ]
    valid.sort(key=lambda lead: lead.rank_tuple or ())
    best: dict[str, Lead] = {}
    extras: dict[str, list[str]] = {}
    order: list[str] = []
    for lead in valid:
        key = lead.identity_key
        if key not in best:
            best[key], extras[key] = lead, []
            order.append(key)
        elif lead.source_url and lead.source_url != best[key].source_url:
            if lead.source_url not in extras[key]:
                extras[key].append(lead.source_url)
    final = [
        replace(best[k], namesake_urls=tuple(extras[k])) if extras[k] else best[k]
        for k in order
    ][: config.digest_size]
    return final, [lead.identity_key for lead in final]


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if is_dataclass(value) and not isinstance(value, type):
        return {k: _jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def lead_to_log_dict(lead: Lead) -> dict[str, Any]:
    """One terminal lead for store.log. The pipeline never reads this back."""
    return {
        "identity_key": lead.identity_key,
        "display_name": lead.display_name,
        "outcome": lead.outcome,
        "rank_key": list(lead.rank_tuple) if lead.rank_tuple is not None else [],
        "detail": {
            "source_url": lead.source_url,
            "publisher_label": lead.publisher_label,
            "rationale": lead.rationale,
            "explanation": lead.explanation,
            "qualifying_domains": list(lead.qualifying_domains),
            "wikipedia": _jsonable(lead.wikipedia_verdict),
            "articles": [_jsonable(a) for a in lead.article_assessments],
        },
    }
