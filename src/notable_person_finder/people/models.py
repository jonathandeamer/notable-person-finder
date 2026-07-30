from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field


class _StrictBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ItemOutcome(StrEnum):
    RESEARCH_PEOPLE = "research_people"
    DO_NOT_RESEARCH = "do_not_research"
    UNCERTAIN = "uncertain"


class MentionOutcome(StrEnum):
    RESEARCH = "research"
    DO_NOT_RESEARCH = "do_not_research"
    UNCERTAIN = "uncertain"


class IdentityFactKind(StrEnum):
    NAME = "name"
    PROFESSION_OR_ROLE = "profession_or_role"
    PLACE = "place"
    NATIONALITY = "nationality"
    ERA_OR_DATE = "era_or_date"
    WORK = "work"
    AFFILIATION = "affiliation"
    OTHER = "other"


class SignalKind(StrEnum):
    ATTENTION = "attention"
    CAUTION = "caution"


class AttentionCategory(StrEnum):
    SIGNIFICANT_RECOGNITION = "significant_recognition"
    ENDURING_CONTRIBUTION = "enduring_contribution"
    SIGNIFICANT_WORK = "significant_work"
    INSTITUTIONAL_RECOGNITION = "institutional_recognition"
    SUSTAINED_FIELD_ATTENTION = "sustained_field_attention"
    MAJOR_ACHIEVEMENT = "major_achievement"
    INFLUENTIAL_ROLE = "influential_role"


class CautionCategory(StrEnum):
    SINGLE_EVENT_ONLY = "single_event_only"
    INHERITED_ASSOCIATION = "inherited_association"
    ROUTINE_ROLE_OR_LISTING = "routine_role_or_listing"
    PRIMARY_OR_PROMOTIONAL = "primary_or_promotional"
    SIGNIFICANCE_UNCLEAR = "significance_unclear"


class SignalGrounding(StrEnum):
    SOURCE_TEXT = "source_text"
    DOMAIN_PROFILE = "domain_profile"


class DomainProfileEvidenceExample(_StrictBoundaryModel):
    category: AttentionCategory
    examples: tuple[str, ...]


class DomainProfileEvidence(_StrictBoundaryModel):
    version: int = Field(ge=1)
    key: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=120)
    language: Literal["en"]
    attention_examples: tuple[DomainProfileEvidenceExample, ...]


class DetectionPassage(_StrictBoundaryModel):
    id: Literal["p1", "p2"]
    field: Literal["title", "summary"]
    text: str = Field(min_length=1)
    truncated: bool


class DetectionView(_StrictBoundaryModel):
    kind: Literal["feed_metadata"]
    title_available: bool
    summary_available: bool
    title_truncated: bool
    summary_truncated: bool
    input_truncated: bool


class DetectionInput(_StrictBoundaryModel):
    task: Literal["detect_people"]
    source_item_id: int = Field(ge=1)
    feed_id: int = Field(ge=1)
    feed_key: str = Field(min_length=1, max_length=64)
    publisher_label: str = Field(min_length=1, max_length=120)
    title: str | None
    summary: str | None
    passages: tuple[DetectionPassage, ...]
    canonical_article_id: int | None = Field(default=None, ge=1)
    original_url: str | None
    published_at: str | None
    published_issue: Literal["missing", "unparseable", "implausible"] | None
    url_issue: Literal["missing", "not_http", "unsafe", "unusable"] | None
    view: DetectionView
    domain_profile: DomainProfileEvidence
    max_people: int = Field(ge=1, le=32)
    max_input_tokens: int = Field(ge=1)


PassageReference = Annotated[str, Field(pattern=r"^p[1-9][0-9]{0,5}$")]


class IdentityFact(_StrictBoundaryModel):
    local_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    kind: IdentityFactKind
    value: str = Field(min_length=1, max_length=500)
    supporting_passage_ids: tuple[PassageReference, ...] = Field(min_length=1)


class GroundedSignal(_StrictBoundaryModel):
    kind: SignalKind
    category: AttentionCategory | CautionCategory
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[PassageReference, ...] = Field(min_length=1)
    grounding: SignalGrounding


class DetectedMention(_StrictBoundaryModel):
    exact_name: str = Field(min_length=1, max_length=300)
    outcome: MentionOutcome
    supporting_passage_ids: tuple[PassageReference, ...] = Field(min_length=1)
    identity_facts: tuple[IdentityFact, ...]
    signals: tuple[GroundedSignal, ...]
    rationale: str = Field(min_length=1, max_length=1000)


class DetectionOutput(_StrictBoundaryModel):
    item_outcome: ItemOutcome
    mentions: tuple[DetectedMention, ...]
    overflow: bool
    rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True, slots=True)
class RenderedDetectionRequest:
    task: Literal["detect_people"]
    system_prompt: str
    user_input_json: str
    schema: dict[str, object]
    canonical_schema_json: str
    schema_version: int
    prompt_hash: str
    schema_hash: str
    token_bearing_utf8_bytes: int
    chat_framing_token_allowance: int
    worst_case_input_tokens: int

    @property
    def canonical_input_json(self) -> str:
        return self.user_input_json


SourcedNameKind = Literal["professional", "display", "alias", "other", "mononym"]
CandidateFactLocalId = Annotated[
    str, Field(pattern=r"^c[1-9][0-9]{0,17}-f[1-9][0-9]{0,5}$")
]


class ResolveCandidateName(_StrictBoundaryModel):
    exact_name: str = Field(min_length=1, max_length=300)
    search_name: str = Field(min_length=1, max_length=300)
    match_key: str = Field(min_length=1, max_length=300)
    kind: SourcedNameKind


class ResolveCandidateFact(_StrictBoundaryModel):
    local_id: CandidateFactLocalId
    kind: IdentityFactKind
    value: str = Field(min_length=1, max_length=500)


class ResolveCandidate(_StrictBoundaryModel):
    person_id: int = Field(ge=1)
    display_name: str = Field(min_length=1, max_length=300)
    names: tuple[ResolveCandidateName, ...]
    identity_facts: tuple[ResolveCandidateFact, ...]


class ResolveView(_StrictBoundaryModel):
    kind: Literal["mention_candidates"]
    candidate_count: int = Field(ge=1, le=16)
    passages_truncated: bool


class ResolvePersonEntityInput(_StrictBoundaryModel):
    task: Literal["resolve_person_entity"]
    person_mention_id: int = Field(ge=1)
    source_item_id: int = Field(ge=1)
    exact_name: str = Field(min_length=1, max_length=300)
    search_name: str = Field(min_length=1, max_length=300)
    mention_outcome: Literal["research", "uncertain"]
    passages: tuple[DetectionPassage, ...]
    identity_facts: tuple[IdentityFact, ...]
    signals: tuple[GroundedSignal, ...]
    candidates: tuple[ResolveCandidate, ...] = Field(min_length=1)
    max_candidates: int = Field(ge=1, le=16)
    view: ResolveView
    max_input_tokens: int = Field(ge=1)


class ResolvePersonEntityOutput(_StrictBoundaryModel):
    outcome: Literal["same_person", "different_people", "uncertain"]
    selected_person_id: int | None = Field(default=None, ge=1)
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True, slots=True)
class RenderedResolutionRequest:
    task: Literal["resolve_person_entity"]
    system_prompt: str
    user_input_json: str
    schema: dict[str, object]
    canonical_schema_json: str
    schema_version: int
    prompt_hash: str
    schema_hash: str
    token_bearing_utf8_bytes: int
    chat_framing_token_allowance: int
    worst_case_input_tokens: int

    @property
    def canonical_input_json(self) -> str:
        return self.user_input_json
