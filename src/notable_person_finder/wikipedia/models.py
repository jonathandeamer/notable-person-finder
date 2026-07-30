"""Row DTOs for Wikipedia identity persistence."""

from __future__ import annotations

from dataclasses import dataclass


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
