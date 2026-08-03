"""The match_wikipedia_identity contract: models, wire schema, and domain
validation, plus the pure MediaWiki-facts domain rules (namespace and
disambiguation filtering) that decide what counts as a candidate.

Pure: no I/O, no network client. Every rule here traces to
docs/superpowers/specs/2026-08-03-wikipedia-match-design.md and
docs/findings.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

MATCH_OUTCOMES = ("matching_page", "no_matching_page", "uncertain")
FACT_FIELDS = ("description", "extract", "category")

_MAIN_NAMESPACE = 0


class MatchInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, slots=True)
class PageFact:
    """One MediaWiki page as retrieved: not yet judged a candidate."""

    page_id: int
    title: str
    namespace: int
    missing: bool
    is_redirect: bool
    is_disambiguation: bool
    description: str | None
    extract: str | None
    categories: tuple[str, ...]


def is_biography_candidate(page: PageFact) -> bool:
    """Main namespace, not missing, not a disambiguation page, not a
    redirect. A redirect that survived one resolution pass and is still a
    redirect (or resolved onto a disambiguation / non-main-namespace page)
    is dropped rather than chased further -- see the design doc's "one
    bounded redirect-resolution pass"."""
    return (
        not page.missing
        and page.namespace == _MAIN_NAMESPACE
        and not page.is_disambiguation
        and not page.is_redirect
    )


class CandidateFact(_Strict):
    id: str
    page_id: int
    field: Literal["description", "extract", "category"]
    text: str = Field(min_length=1)


class Candidate(_Strict):
    page_id: int
    title: str = Field(min_length=1, max_length=300)
    facts: tuple[CandidateFact, ...]


def build_candidates(
    pages: tuple[PageFact, ...],
    *,
    max_extract_characters: int,
    max_categories_per_page: int,
) -> tuple[Candidate, ...]:
    """Assemble the final candidate list from every retrieved page.

    Deduplicates by page id -- a redirect's terminal page can be reached
    twice if two different search hits redirected to the same article.
    """
    candidates: list[Candidate] = []
    seen: set[int] = set()
    fact_counter = 0
    for page in pages:
        if not is_biography_candidate(page) or page.page_id in seen:
            continue
        seen.add(page.page_id)
        facts: list[CandidateFact] = []
        if page.description:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="description",
                    text=page.description,
                )
            )
        if page.extract:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="extract",
                    text=page.extract[:max_extract_characters],
                )
            )
        for category in page.categories[:max_categories_per_page]:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="category",
                    text=category,
                )
            )
        candidates.append(
            Candidate(page_id=page.page_id, title=page.title, facts=tuple(facts))
        )
    return tuple(candidates)


class MatchOutput(_Strict):
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True, slots=True)
class MatchVerdict:
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None
    rationale: str

    @property
    def has_page(self) -> bool:
        return self.outcome == "matching_page"


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def match_schema() -> dict[str, object]:
    """The wire schema. Root is a plain object -- a root-level `anyOf` is
    rejected under strict mode (docs/findings.md)."""
    return _object(
        {
            "outcome": _string(enum=list(MATCH_OUTCOMES)),
            "selected_page_id": {"type": ["integer", "null"]},
            "supporting_fact_ids": {"type": "array", "items": _string()},
            "conflicting_fact_ids": {"type": "array", "items": _string()},
            "rationale": _string(maxLength=1000),
        }
    )


def validate_match(
    raw: dict[str, Any], *, candidates: tuple[Candidate, ...], truncated: bool
) -> MatchOutput:
    """Parse and apply the rules the wire schema cannot express."""
    try:
        output = MatchOutput.model_validate(raw)
    except ValidationError as error:
        raise MatchInvalid(f"match output failed validation: {error}") from error

    page_ids = {candidate.page_id for candidate in candidates}
    fact_ids = {fact.id for candidate in candidates for fact in candidate.facts}

    # Not expressible in the schema: it relates two root-level properties,
    # and strict mode has no if/then and rejects a root-level union
    # (docs/findings.md) -- the same reason detection's overflow/item_outcome
    # pairing is a validator rule rather than a schema one.
    if output.outcome == "matching_page":
        if output.selected_page_id is None or output.selected_page_id not in page_ids:
            raise MatchInvalid(
                "matching_page requires selected_page_id naming a supplied candidate"
            )
    elif output.selected_page_id is not None:
        raise MatchInvalid(f"{output.outcome} must leave selected_page_id null")

    # A truncated search saw an incomplete candidate universe: "no page
    # exists" would be a false negative manufactured by max_candidates, not
    # by evidence. See the design doc's truncation rule.
    if output.outcome == "no_matching_page" and truncated:
        raise MatchInvalid(
            "no_matching_page is not valid when the search was truncated"
        )

    unknown = (
        set(output.supporting_fact_ids) | set(output.conflicting_fact_ids)
    ) - fact_ids
    if unknown:
        raise MatchInvalid(f"unknown fact reference: {sorted(unknown)}")

    return output
