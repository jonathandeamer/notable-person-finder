"""Domain DTOs and match_wikipedia_identity boundary models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from notable_person_finder.people.models import IdentityFactKind, SourcedNameKind


class _StrictBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


@dataclass(frozen=True, slots=True)
class MediaWikiPageRecord:
    id: int
    wiki_id: str
    page_id: int
    canonical_title: str
    canonical_url: str
    namespace: int
    is_disambiguation: bool
    is_missing: bool
    redirect_to_page_id: int | None
    description: str | None
    extract: str | None
    categories_json: str
    last_observed_at: str
    last_attempt_id: int | None


@dataclass(frozen=True, slots=True)
class WikipediaIdentityPlanRecord:
    id: int
    person_id: int
    run_id: int
    material_fingerprint: str
    status: str
    refresh_of_observation_id: int | None
    truncated_unsafe_for_negative: bool
    partial_retrieval: bool
    created_at: str
    completed_at: str | None
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class WikipediaQueryFormRecord:
    id: int
    plan_id: int
    ordinal: int
    variant_kind: str
    query_text: str
    status: str
    continuations_used: int
    hit_count: int | None
    truncated: bool
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class WikipediaPageFactsBatchRecord:
    id: int
    plan_id: int
    ordinal: int
    page_ids_json: str
    status: str
    wave: int
    attempt_id: int | None
    failure_category: str | None
    created_at: str
    completed_at: str | None


@dataclass(frozen=True, slots=True)
class MediaWikiSearchObservationRecord:
    id: int
    query_form_id: int
    run_id: int
    attempt_id: int
    query_text: str
    continuation_in: str | None
    continuation_out: str | None
    srlimit: int
    hit_count: int
    truncated: bool
    response_complete: bool
    observed_at: str


@dataclass(frozen=True, slots=True)
class WikipediaIdentityObservationRecord:
    id: int
    person_id: int
    plan_id: int | None
    run_id: int
    attempt_id: int | None
    model_inspection_id: int | None
    disposition: str
    semantic_outcome: str | None
    matched_mediawiki_page_id: int | None
    candidate_page_ids_json: str
    canonical_supplied_input_json: str
    validated_output_json: str | None
    prompt_hash: str | None
    schema_hash: str | None
    schema_version: int | None
    task_fingerprint: str
    supporting_fact_ids_json: str | None
    conflicting_fact_ids_json: str | None
    rationale: str
    failure_category: str | None
    observed_at: str


MatchFactLocalId = Annotated[str, Field(pattern=r"^f[1-9][0-9]{0,5}$")]


class MatchName(_StrictBoundaryModel):
    exact_name: str = Field(min_length=1, max_length=300)
    search_name: str = Field(min_length=1, max_length=300)
    match_key: str = Field(min_length=1, max_length=300)
    kind: SourcedNameKind


class MatchFact(_StrictBoundaryModel):
    local_id: MatchFactLocalId
    kind: IdentityFactKind
    value: str = Field(min_length=1, max_length=500)


class MatchWikiCandidate(_StrictBoundaryModel):
    page_id: int = Field(ge=1)
    title: str = Field(min_length=1, max_length=500)
    canonical_url: str = Field(min_length=1, max_length=2000)
    namespace: int = Field(ge=0)
    is_disambiguation: bool
    description: str | None = Field(default=None, max_length=2000)
    extract: str | None = Field(default=None, max_length=20_000)
    categories: tuple[str, ...]
    redirect_trail: tuple[str, ...]


class MatchView(_StrictBoundaryModel):
    kind: Literal["wikipedia_candidates"]
    candidate_count: int = Field(ge=1, le=16)
    truncated_unsafe_for_negative: bool
    partial_retrieval: bool


class MatchWikipediaIdentityInput(_StrictBoundaryModel):
    task: Literal["match_wikipedia_identity"]
    person_id: int = Field(ge=1)
    display_name: str = Field(min_length=1, max_length=300)
    sourced_names: tuple[MatchName, ...]
    identity_facts: tuple[MatchFact, ...]
    candidates: tuple[MatchWikiCandidate, ...] = Field(min_length=1)
    max_candidates: int = Field(ge=1, le=16)
    view: MatchView
    max_input_tokens: int = Field(ge=1)


class MatchWikipediaIdentityOutput(_StrictBoundaryModel):
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None = Field(default=None, ge=1)
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True, slots=True)
class RenderedMatchRequest:
    task: Literal["match_wikipedia_identity"]
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


@dataclass(frozen=True, slots=True)
class WikipediaPersonMaterialView:
    """Person fields needed for Wikipedia material fingerprints (K17)."""

    person_id: int
    identity_fingerprint: str
