"""The assess_article contract: models and wire schema.

Pure: no I/O, no network client, no HTML parsing. Every rule here traces to
docs/superpowers/specs/2026-08-04-coverage-research-design.md.

Unlike detect_people and match_wikipedia_identity, this contract needs no
domain validator beyond parsing: every rule the ported prompt states
(content-type dedup/bound, the fixed single passage id, the closed enums) is
directly schema-expressible, so there is no cross-field rule left for code to
check.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

PERSON_RELATIONS = ("same_person", "different_person", "uncertain")
COVERAGE_DEPTHS = ("significant", "passing", "uncertain")
CONTENT_TYPES = (
    "reporting",
    "profile",
    "review",
    "interview",
    "obituary",
    "listing",
    "announcement",
    "press_release",
    "sponsored",
    "other",
)
SUBJECT_RELATIONSHIPS = (
    "editorially_independent",
    "affiliated",
    "self_published",
    "uncertain",
)
ARTICLE_PASSAGE_ID = "p1"


class AssessInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, slots=True)
class ArticlePassage:
    id: Literal["p1"]
    text: str
    truncated: bool


def build_article_passage(text: str, *, max_characters: int) -> ArticlePassage:
    bounded = text[:max_characters]
    return ArticlePassage(id="p1", text=bounded, truncated=bounded != text)


class GroundedSignal(_Strict):
    kind: Literal["attention", "caution"]
    category: str = Field(min_length=1, max_length=100)
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[Literal["p1"], ...] = Field(min_length=1)
    grounding: Literal["source_text"]


class AssessmentOutput(_Strict):
    person_relation: Literal["same_person", "different_person", "uncertain"]
    coverage_depth: Literal["significant", "passing", "uncertain"]
    content_types: tuple[
        Literal[
            "reporting",
            "profile",
            "review",
            "interview",
            "obituary",
            "listing",
            "announcement",
            "press_release",
            "sponsored",
            "other",
        ],
        ...,
    ] = Field(min_length=1, max_length=3)
    subject_relationship: Literal[
        "editorially_independent", "affiliated", "self_published", "uncertain"
    ]
    signals: tuple[GroundedSignal, ...]
    supporting_passage_ids: tuple[Literal["p1"], ...] = Field(min_length=1)
    rationale: str = Field(min_length=1, max_length=1000)

    @field_validator("content_types")
    @classmethod
    def _no_duplicate_content_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("content_types must not contain duplicates")
        return value


@dataclass(frozen=True, slots=True)
class ArticleAssessment:
    """One (mention, article) judgment: the full assess_article output plus
    the screening status coverage.py already knew -- the model never
    re-derives it."""

    url: str
    screening_status: Literal["curated_eligible", "unclassified"]
    person_relation: Literal["same_person", "different_person", "uncertain"]
    coverage_depth: Literal["significant", "passing", "uncertain"]
    content_types: tuple[str, ...]
    subject_relationship: Literal[
        "editorially_independent", "affiliated", "self_published", "uncertain"
    ]
    signals: tuple[GroundedSignal, ...]
    rationale: str


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def assessment_schema() -> dict[str, Any]:
    """The wire schema. Root is a plain object -- a root-level `anyOf` is
    rejected under strict mode (docs/findings.md). Every rule the prompt
    states is expressed here directly: content_types is bounded and
    deduplicated by minItems/maxItems/uniqueItems, and every passage-id field
    is enum-restricted to the one passage this contract ever supplies -- so a
    model cannot cite a passage that doesn't exist."""
    passage_ids = {
        "type": "array",
        "items": _string(enum=[ARTICLE_PASSAGE_ID]),
        "minItems": 1,
    }
    return _object(
        {
            "person_relation": _string(enum=list(PERSON_RELATIONS)),
            "coverage_depth": _string(enum=list(COVERAGE_DEPTHS)),
            "content_types": {
                "type": "array",
                "items": _string(enum=list(CONTENT_TYPES)),
                "minItems": 1,
                "maxItems": 3,
                "uniqueItems": True,
            },
            "subject_relationship": _string(enum=list(SUBJECT_RELATIONSHIPS)),
            "signals": {
                "type": "array",
                "items": _object(
                    {
                        "kind": _string(enum=["attention", "caution"]),
                        "category": _string(maxLength=100),
                        "claim": _string(maxLength=1000),
                        "supporting_passage_ids": passage_ids,
                        "grounding": _string(enum=["source_text"]),
                    }
                ),
            },
            "supporting_passage_ids": passage_ids,
            "rationale": _string(maxLength=1000),
        }
    )


def validate_assessment(raw: dict[str, Any]) -> AssessmentOutput:
    """Parse and validate. Every rule the ported prompt states is
    schema-expressible (see `assessment_schema`), so this is parsing plus
    error-type translation, not cross-field checking."""
    try:
        return AssessmentOutput.model_validate(raw)
    except ValidationError as error:
        raise AssessInvalid(f"assessment output failed validation: {error}") from error
