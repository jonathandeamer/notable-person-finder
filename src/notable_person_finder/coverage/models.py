"""Domain DTOs for coverage evidence rows."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PersonCoveragePlanRecord:
    id: int
    person_id: int
    run_id: int
    material_fingerprint: str
    status: str
    refresh_of_plan_id: int | None
    source_policy_fingerprint: str
    truncated_unsafe: bool
    partial_retrieval: bool
    retrieval_target: int
    eligible_selected_count: int
    created_at: str
    completed_at: str | None
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class CoverageQueryFormRecord:
    id: int
    plan_id: int
    ordinal: int
    stage: int
    variant_kind: str
    query_text: str
    status: str
    offsets_used: int
    result_count: int | None
    truncated: bool
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class BraveSearchObservationRecord:
    id: int
    query_form_id: int
    run_id: int
    attempt_id: int
    query_text: str
    altered_query: str | None
    offset_in: int
    count_requested: int
    result_count: int
    truncated: bool
    response_complete: bool
    observed_at: str


@dataclass(frozen=True, slots=True)
class SourceScreeningRecord:
    id: int
    canonical_article_id: int | None
    url: str
    publisher_key: str | None
    rule_id: str
    rule_status: str
    source_policy_fingerprint: str
    decided_at: str
    plan_id: int | None
    source_item_id: int | None
    person_mention_id: int | None


@dataclass(frozen=True, slots=True)
class CoverageDiscoveryArticleRecord:
    id: int
    plan_id: int
    canonical_article_id: int
    source_item_id: int
    person_mention_id: int | None
    screening_id: int


@dataclass(frozen=True, slots=True)
class CoverageArticleTargetRecord:
    id: int
    plan_id: int
    canonical_article_id: int
    request_url: str
    selection_reason: str
    status: str
    article_view_id: int | None
    attempt_id: int | None
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class ArticleViewRecord:
    id: int
    canonical_article_id: int
    run_id: int
    attempt_id: int | None
    access_kind: str
    requested_url: str | None
    final_url: str | None
    title: str | None
    dek: str | None
    byline: str | None
    published_at: str | None
    editorial_labels_json: str
    main_text_blocks_json: str
    snippets_json: str
    extraction_quality: str | None
    extractor_version: int
    observed_at: str


@dataclass(frozen=True, slots=True)
class PersonArticleRecord:
    id: int
    person_id: int
    canonical_article_id: int
    first_plan_id: int | None
    current_assessment_id: int | None


@dataclass(frozen=True, slots=True)
class PersonArticleAssessmentRecord:
    id: int
    person_article_id: int
    person_id: int
    canonical_article_id: int
    plan_id: int | None
    article_view_id: int
    run_id: int
    attempt_id: int | None
    model_inspection_id: int | None
    disposition: str
    person_relation: str | None
    coverage_depth: str | None
    content_types_json: str | None
    subject_relationship: str | None
    screening_rule_id: str
    screening_rule_status: str
    source_policy_fingerprint: str
    canonical_supplied_input_json: str
    validated_output_json: str | None
    prompt_hash: str | None
    schema_hash: str | None
    schema_version: int | None
    task_fingerprint: str
    rationale: str
    failure_category: str | None
    observed_at: str
