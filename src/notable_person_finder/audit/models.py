"""Read-only audit models.

`audit/` may import only from `config/`, `db/`, and `obs/` within this
package; it reads every other package's tables with its own SQL rather than
importing them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from notable_person_finder.audit.registry import ResultBinding


@dataclass(frozen=True, slots=True)
class DigestLocation:
    run_id: int
    file_path: str
    content_hash: str
    timezone: str | None  # None on the pre-0008 run-column fallback
    window_start: str | None
    window_end: str | None
    run_state: str | None
    created_at: str | None
    source: str  # "digest_table" | "run_columns"


@dataclass(frozen=True, slots=True)
class RunHeader:
    id: int
    state: str
    started_at: str
    finished_at: str | None
    timezone: str
    window_start: str
    window_end: str


@dataclass(frozen=True, slots=True)
class ConfigurationProvenance:
    snapshot_id: int
    fingerprint: str
    created_at: str
    canonical_json: str


@dataclass(frozen=True, slots=True)
class Transition:
    state: str
    reason: str | None
    occurred_at: str


@dataclass(frozen=True, slots=True)
class WorkItemLine:
    id: int
    task_type: str
    subject_kind: str
    subject_id: int | None
    fingerprint: str
    state: str
    reason: str | None


@dataclass(frozen=True, slots=True)
class WorkOutcomes:
    counts: tuple[tuple[str, str, int], ...]
    failed_permanent: tuple[WorkItemLine, ...]
    deferred: tuple[WorkItemLine, ...]


@dataclass(frozen=True, slots=True)
class AttemptLine:
    id: int
    work_item_id: int
    provider: str
    operation: str
    ordinal: int
    outcome: str | None
    failure_category: str | None
    provider_status: int | None
    latency_ms: int | None
    response_bytes: int | None
    destination_host: str | None
    reserved_nano_usd: int
    actual_nano_usd: int | None


@dataclass(frozen=True, slots=True)
class FailureGroup:
    failure_category: str
    count: int
    example_provider: str
    example_operation: str
    outcomes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BudgetSummary:
    limit_nano_usd: int | None
    reserved_nano_usd: int
    actual_nano_usd: int
    attempt_actual_sum_nano_usd: int


@dataclass(frozen=True, slots=True)
class ReportingResult:
    digest_path: str | None
    digest_sha256: str | None
    run_state: str | None
    entry_count: int | None  # None when unknown (pre-0008 run-column fallback)


@dataclass(frozen=True, slots=True)
class RunAudit:
    run: RunHeader
    configuration: ConfigurationProvenance | None
    transitions: tuple[Transition, ...]
    work_outcomes: WorkOutcomes
    attempts: tuple[AttemptLine, ...]
    failures: tuple[FailureGroup, ...]
    budget: BudgetSummary
    reporting: ReportingResult | None
    unavailable_sections: tuple[str, ...]  # K3 schema guards


@dataclass(frozen=True, slots=True)
class AttemptAudit:
    attempt: AttemptLine
    work_item: WorkItemLine
    retry_history: tuple[AttemptLine, ...]
    result_rows: tuple[Mapping[str, object], ...]
    binding: ResultBinding | None
    caveat: str | None  # set for fetch_feed
    no_result_row: bool


# --- notable audit person -------------------------------------------------


@dataclass(frozen=True, slots=True)
class PersonIdentity:
    id: int
    display_name: str
    identity_fingerprint: str
    created_at: str
    created_by_run_id: int
    merged_into_person_id: int | None
    current_wikipedia_identity_observation_id: int | None
    current_lead_assessment_id: int | None


@dataclass(frozen=True, slots=True)
class SourcedNameLine:
    id: int
    exact_name: str
    search_name: str
    match_key: str
    kind: str
    origin_kind: str
    origin_mention_id: int | None
    first_observed_at: str
    last_observed_at: str


@dataclass(frozen=True, slots=True)
class RelationLine:
    id: int
    other_person_id: int
    kind: str
    status: str
    created_at: str
    created_by_run_id: int
    created_by_observation_id: int | None
    closed_at: str | None
    closed_by_observation_id: int | None


@dataclass(frozen=True, slots=True)
class MentionLine:
    id: int
    triage_observation_id: int
    source_item_id: int
    ordinal: int
    exact_name: str
    search_name: str
    outcome: str
    rationale: str
    current_entity_resolution_observation_id: int | None
    current_semantic_outcome: str | None


@dataclass(frozen=True, slots=True)
class ResolutionLine:
    id: int
    person_mention_id: int | None
    person_relation_id: int | None
    disposition: str
    semantic_outcome: str | None
    candidate_person_ids_json: str
    selected_person_id: int | None
    created_person_id: int | None
    prompt_hash: str | None
    schema_version: int | None
    task_fingerprint: str
    rationale: str


@dataclass(frozen=True, slots=True)
class WikipediaPlanLine:
    id: int
    status: str
    created_at: str
    completed_at: str | None
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class WikipediaQueryFormLine:
    id: int
    plan_id: int
    ordinal: int
    variant_kind: str
    query_text: str
    status: str
    hit_count: int | None


@dataclass(frozen=True, slots=True)
class WikipediaSearchObservationLine:
    id: int
    query_form_id: int
    query_text: str
    hit_count: int
    truncated: bool
    response_complete: bool


@dataclass(frozen=True, slots=True)
class WikipediaPageFactsBatchLine:
    id: int
    plan_id: int
    ordinal: int
    status: str
    wave: int


@dataclass(frozen=True, slots=True)
class WikipediaIdentityObservationLine:
    id: int
    disposition: str
    semantic_outcome: str | None
    matched_mediawiki_page_id: int | None
    task_fingerprint: str
    rationale: str
    is_current: bool


@dataclass(frozen=True, slots=True)
class WikipediaEvidence:
    plans: tuple[WikipediaPlanLine, ...]
    query_forms: tuple[WikipediaQueryFormLine, ...]
    search_observations: tuple[WikipediaSearchObservationLine, ...]
    page_facts_batches: tuple[WikipediaPageFactsBatchLine, ...]
    identity_observations: tuple[WikipediaIdentityObservationLine, ...]


@dataclass(frozen=True, slots=True)
class CoveragePlanLine:
    id: int
    status: str
    created_at: str
    completed_at: str | None
    failure_category: str | None


@dataclass(frozen=True, slots=True)
class CoverageQueryFormLine:
    id: int
    plan_id: int
    ordinal: int
    stage: int
    variant_kind: str
    query_text: str
    status: str
    result_count: int | None


@dataclass(frozen=True, slots=True)
class BraveSearchObservationLine:
    id: int
    query_form_id: int
    query_text: str
    result_count: int
    truncated: bool


@dataclass(frozen=True, slots=True)
class ScreeningLine:
    id: int
    plan_id: int | None
    url: str
    publisher_key: str | None
    rule_id: str
    rule_status: str
    source_policy_fingerprint: str


@dataclass(frozen=True, slots=True)
class ArticleTargetLine:
    id: int
    plan_id: int
    canonical_article_id: int
    request_url: str
    selection_reason: str
    status: str


@dataclass(frozen=True, slots=True)
class CoverageEvidence:
    plans: tuple[CoveragePlanLine, ...]
    query_forms: tuple[CoverageQueryFormLine, ...]
    brave_observations: tuple[BraveSearchObservationLine, ...]
    screenings: tuple[ScreeningLine, ...]
    article_targets: tuple[ArticleTargetLine, ...]


@dataclass(frozen=True, slots=True)
class AssessmentSignalLine:
    id: int
    signal_kind: str
    category: str
    claim: str
    supporting_passage_ids_json: str
    ordinal: int


@dataclass(frozen=True, slots=True)
class AssessmentLine:
    id: int
    canonical_article_id: int
    disposition: str
    person_relation: str | None
    coverage_depth: str | None
    subject_relationship: str | None
    content_types_json: str | None
    rationale: str
    failure_category: str | None
    signals: tuple[AssessmentSignalLine, ...]


@dataclass(frozen=True, slots=True)
class LeadLine:
    id: int
    outcome: str
    qualifying_domain_count: int
    incompleteness_reason: str | None
    decided_at: str
    is_current: bool


@dataclass(frozen=True, slots=True)
class QueueTransitionLine:
    id: int
    run_id: int
    from_status: str | None
    to_status: str
    tier: str
    reason: str
    occurred_at: str


@dataclass(frozen=True, slots=True)
class QueueRowLine:
    status: str
    tier: str
    eligibility_reason: str
    lead_assessment_id: int
    first_pending_at: str
    last_material_change_at: str
    removed_reason: str | None


@dataclass(frozen=True, slots=True)
class QueueEvidence:
    row: QueueRowLine | None
    transitions: tuple[QueueTransitionLine, ...]


@dataclass(frozen=True, slots=True)
class DigestEntryLine:
    digest_id: int
    run_id: int
    ordinal: int
    lead_assessment_id: int
    file_path: str


@dataclass(frozen=True, slots=True)
class PersonAudit:
    identity: PersonIdentity
    merged_into: PersonIdentity | None
    merged_by_run_id: int | None
    sourced_names: tuple[SourcedNameLine, ...]
    relations: tuple[RelationLine, ...]
    mentions: tuple[MentionLine, ...]
    resolutions: tuple[ResolutionLine, ...]
    wikipedia: WikipediaEvidence
    coverage: CoverageEvidence
    assessments: tuple[AssessmentLine, ...]
    leads: tuple[LeadLine, ...]
    queue: QueueEvidence
    digest_entries: tuple[DigestEntryLine, ...]
    unavailable_sections: tuple[str, ...]
