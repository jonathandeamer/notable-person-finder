"""Coverage plan lifecycle, Brave search, fetch, and ``assess_article`` handlers.

Plan open attaches discovery (K10), schedules exact-name forms (K6), and
stages alias/context after exact terminal. ``brave_web_search`` performs
exactly one Brave ``search_web`` call per execute. ``fetch_article`` performs
exactly one GET, extracts in-process (K3), and persists cleaned article views
with snippets fallback (K28). ``assess_article`` runs one OpenRouter structured
generation with PassageSelector input and completed-only current pointers
(K25). Workers never open SQLite; domain settlement and
``maybe_advance_coverage_plan`` run on the application thread inside the engine
settlement transaction.

Terminal truth table T1–T11 applies once search/targets/assess are quiescent.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import urlsplit

from notable_person_finder.config.models import (
    AssessArticleConfig,
    DomainProfileConfig,
    MainConfig,
)
from notable_person_finder.coverage.assessment import (
    AssessArticleInput,
    AssessArticleOutput,
    AssessFact,
    AssessName,
    AssessValidationError,
    CoveragePersonMaterialView,
    assess_task_fingerprint,
    build_assess_input,
    coverage_material_fingerprint,
    render_assess_request,
    validate_assess_output,
)
from notable_person_finder.coverage.models import (
    CoverageQueryFormRecord,
    PersonCoveragePlanRecord,
)  # PersonCoveragePlanRecord used by eligibility/ensure
from notable_person_finder.coverage.passages import (
    ArticleViewLike,
    TextBlock,
    select_passages,
)
from notable_person_finder.coverage.queries import (
    generate_alias_forms,
    generate_context_form,
    generate_exact_forms,
)
from notable_person_finder.coverage.repository import (
    insert_article_view,
    insert_assessment_signals,
    insert_coverage_article_target,
    insert_coverage_discovery_article,
    insert_or_load_brave_search_observation_by_attempt,
    insert_person_article_assessment,
    insert_query_forms,
    insert_search_result_occurrences,
    insert_source_screening,
    list_coverage_article_targets_for_plan,
    list_coverage_discovery_articles_for_plan,
    list_query_forms_for_plan,
    load_active_plan_for_fingerprint,
    load_article_view,
    load_coverage_article_target,
    load_person_article,
    load_person_article_assessment_by_fingerprint,
    load_person_article_by_pair,
    load_plan,
    load_query_form,
    load_source_screening,
    mark_query_form_completed,
    mark_query_form_failed,
    mark_query_form_progress,
    open_plan,
    point_person_article_current_assessment,
    supersede_pending_targets_for_plan,
    supersede_plan,
    update_coverage_article_target,
    update_plan_status,
    upsert_person_article,
)
from notable_person_finder.coverage.screening import (
    DiscoveryAttachKind,
    SourcePolicy,
    attach_discovery_url,
    screen_url,
)
from notable_person_finder.coverage.selection import (
    SelectionCandidate,
    eligible_selected_count,
    final_selection,
)
from notable_person_finder.ingestion.repository import record_alias, upsert_article
from notable_person_finder.ingestion.urls import publisher_key
from notable_person_finder.people.identity import (
    canonical_person_id,
    mentions_for_canonical_person,
    person_id_closure_for_canonical,
    select_display_name,
)
from notable_person_finder.people.models import (
    AttentionCategory,
    DomainProfileEvidence,
    DomainProfileEvidenceExample,
    IdentityFactKind,
)
from notable_person_finder.people.repository import load_model_inspection
from notable_person_finder.people.service import (
    _worst_case_reservation_nano_usd,
    inspection_ready,
    routing_fingerprint,
)
from notable_person_finder.providers.article_versions import EXTRACTOR_VERSION
from notable_person_finder.providers.articles import (
    ARTICLE_PROVIDER,
    OPERATION_FETCH_ARTICLE,
    ArticleAccessDenied,
    ArticleExtractor,
    ArticleFetcher,
    ArticleFetchSuccess,
    ExtractedArticle,
    ExtractionQuality,
)
from notable_person_finder.providers.brave import (
    OPERATION_SEARCH_WEB,
    SearchPage,
    WebSearchClient,
)
from notable_person_finder.providers.brave import (
    PROVIDER as BRAVE_PROVIDER,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    LlmClient,
    StructuredGenerationRequest,
)
from notable_person_finder.providers.openrouter import (
    PROVIDER as OPENROUTER_PROVIDER,
)
from notable_person_finder.runs import repository as runs_repository
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome, TaskPreparation
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import WorkerPool

BRAVE_WEB_SEARCH_TASK_TYPE = "brave_web_search"
FETCH_ARTICLE_TASK_TYPE = "fetch_article"
ASSESS_ARTICLE_TASK_TYPE = "assess_article"

SUBJECT_KIND_COVERAGE_QUERY_FORM = "coverage_query_form"
SUBJECT_KIND_COVERAGE_ARTICLE_TARGET = "coverage_article_target"
SUBJECT_KIND_PERSON_ARTICLE = "person_article"

BRAVE_SEARCH_PRIORITY = 60
FETCH_ARTICLE_PRIORITY = 65
ASSESS_ARTICLE_PRIORITY = 70

COVERAGE_SUPERSEDED_REASON = "coverage_superseded"
MATCHING_WIKIPEDIA_REASON = "matching_wikipedia_page"
MERGED_AWAY_COVERAGE_REASON = "merged_away"

_TERMINAL_PLAN_STATUSES = frozenset({"superseded", "completed", "failed", "incomplete"})
_ACTIVE_PLAN_STATUSES = frozenset({"retrieving", "selecting", "assessing"})
_ELIGIBLE_WIKI_OUTCOMES = frozenset({"no_matching_page_found", "uncertain_identity"})
_NAME_KIND_ORDER: dict[str, int] = {
    "professional": 0,
    "display": 1,
    "alias": 2,
    "other": 3,
    "mononym": 4,
}
_DEATH_FACT_PATTERN = re.compile(
    r"\b(obituar(?:y|ies)|died|death|deceased|passed away|funeral)\b",
    re.IGNORECASE,
)
_CONTEXT_FACT_KINDS = frozenset(
    {
        "profession_or_role",
        "place",
        "nationality",
        "work",
        "affiliation",
        "era_or_date",
    }
)
_ALTERED_QUERY_CATEGORY = "altered_query"
_UNSAFE_TRUNCATION_CATEGORY = "unsafe_truncation"
_PARTIAL_RETRIEVAL_EMPTY_CATEGORY = "partial_retrieval_empty"
_PERMANENT_PROVIDER_CATEGORY = "permanent_provider"
_INVALID_MODEL_OUTPUT_CATEGORY = "invalid_model_output"
ASSESS_SCHEMA_NAME = "assess_article"
MALFORMED_ASSESS_DETAIL = "invalid assess output"
MISSING_INSPECTION_DETAIL = "compatible model inspection is missing"
MISSING_PRICING_DETAIL = "usable model pricing is missing"
MISSING_PERSON_ARTICLE_DETAIL = "person_article is missing"
ASSESS_PREPARE_REFUSED_PREFIX = "assess_prepare_refused:"
ASSESS_FAILED_SUPPLIED_INPUT_JSON = "{}"
_ACTIVE_COVERAGE_PLAN_STATUSES = frozenset({"retrieving", "selecting", "assessing"})


@dataclass(frozen=True, slots=True)
class _BraveSearchCall:
    form_id: int
    plan_id: int
    query_text: str
    offset_in: int
    offsets_used_before: int
    material_fingerprint: str
    search_count: int
    max_offsets_per_form: int
    max_results_per_form: int
    reject_altered_query: bool


@dataclass(frozen=True, slots=True)
class _FetchArticleCall:
    target_id: int
    plan_id: int
    person_id: int
    canonical_article_id: int
    request_url: str
    material_fingerprint: str
    person_article_id: int | None


@dataclass(frozen=True, slots=True)
class _CleanBlock:
    id: str
    text: str


@dataclass(frozen=True, slots=True)
class _FetchArticlePayload:
    """Cleaned fetch result only — never carries HTML (K3)."""

    kind: Literal["extracted", "access_denied"]
    requested_url: str
    final_url: str | None
    status_code: int | None
    access_kind: Literal["full", "partial", "snippets"]
    title: str | None
    dek: str | None
    byline: str | None
    published_at: str | None
    editorial_labels: tuple[str, ...]
    blocks: tuple[_CleanBlock, ...]
    extraction_quality: str | None
    access_denied_kind: str | None
    detail: str | None


def brave_web_search_fingerprint(
    *,
    material_fingerprint: str,
    query_form_id: int,
    offset_in: int,
) -> str:
    """Work-item fingerprint for one Brave page (form + offset)."""
    return _sha256(
        _canonical_json(
            {
                "task": BRAVE_WEB_SEARCH_TASK_TYPE,
                "material_fingerprint": material_fingerprint,
                "query_form_id": query_form_id,
                "offset_in": offset_in,
            }
        )
    )


def fetch_article_fingerprint(
    *,
    material_fingerprint: str,
    coverage_article_target_id: int,
) -> str:
    """Work-item fingerprint for one article fetch target."""
    return _sha256(
        _canonical_json(
            {
                "task": FETCH_ARTICLE_TASK_TYPE,
                "material_fingerprint": material_fingerprint,
                "coverage_article_target_id": coverage_article_target_id,
            }
        )
    )


def schedule_brave_web_search(
    connection: sqlite3.Connection,
    *,
    form_id: int,
    material_fingerprint: str,
    offset_in: int,
    run_id: int,
    now: str,
) -> int:
    """Schedule one ``brave_web_search`` work item for a form offset page."""
    fingerprint = brave_web_search_fingerprint(
        material_fingerprint=material_fingerprint,
        query_form_id=form_id,
        offset_in=offset_in,
    )
    return runs_repository.schedule_work(
        connection,
        task_type=BRAVE_WEB_SEARCH_TASK_TYPE,
        subject_kind=SUBJECT_KIND_COVERAGE_QUERY_FORM,
        subject_id=form_id,
        fingerprint=fingerprint,
        required=True,
        priority=BRAVE_SEARCH_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def schedule_fetch_article(
    connection: sqlite3.Connection,
    *,
    target_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
) -> int:
    """Schedule one ``fetch_article`` work item for a coverage target."""
    fingerprint = fetch_article_fingerprint(
        material_fingerprint=material_fingerprint,
        coverage_article_target_id=target_id,
    )
    return runs_repository.schedule_work(
        connection,
        task_type=FETCH_ARTICLE_TASK_TYPE,
        subject_kind=SUBJECT_KIND_COVERAGE_ARTICLE_TARGET,
        subject_id=target_id,
        fingerprint=fingerprint,
        required=True,
        priority=FETCH_ARTICLE_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def schedule_assess_article(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    article_view_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
    config: MainConfig,
) -> int:
    """Schedule one ``assess_article`` work item when a view is ready.

    Mid-run inspect arming (K20): ensures the assess model is inspected once
    assess work exists, so cold-start plan/HTTP-only seeds unlock preflight.
    """
    fingerprint = assess_task_fingerprint(
        material_fingerprint=material_fingerprint,
        person_article_id=person_article_id,
        article_view_id=article_view_id,
    )
    work_id = runs_repository.schedule_work(
        connection,
        task_type=ASSESS_ARTICLE_TASK_TYPE,
        subject_kind=SUBJECT_KIND_PERSON_ARTICLE,
        subject_id=person_article_id,
        fingerprint=fingerprint,
        required=True,
        priority=ASSESS_ARTICLE_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )
    # Lazy import: people.service.models_needed_for_run imports assess_model_needed.
    from notable_person_finder.people.service import ensure_model_inspections_for_run

    ensure_model_inspections_for_run(
        connection,
        run_id=run_id,
        config=config,
        now=now,
    )
    return work_id


def assess_model_needed(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    run_id: int | None = None,
    policy: SourcePolicy | None = None,
    now: str | None = None,
) -> bool:
    """K20: whether the assess model needs current-run inspection.

    Arms when active assess work exists, an active coverage plan is
    retrieving/selecting/assessing, Brave/fetch HTTP work is active, or
    (when ``policy`` is supplied) any person is K19-eligible.
    """
    del run_id
    if (
        _has_active_assess_article_work(connection)
        or _has_active_coverage_plan(connection)
        or _has_active_coverage_http_work(connection)
    ):
        return True
    if policy is None:
        return False
    effective_now = now if now is not None else utc_timestamp(datetime.now(tz=UTC))
    return has_coverage_research_eligible_people(
        connection, config=config, policy=policy, now=effective_now
    )


def has_coverage_research_eligible_people(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> bool:
    """True when any canonical person is K19-eligible at ``now``."""
    rows = connection.execute(
        """
        SELECT id
          FROM person
         WHERE merged_into_person_id IS NULL
         ORDER BY id
        """
    ).fetchall()
    for row in rows:
        if is_coverage_research_eligible(
            connection,
            person_id=int(row["id"]),
            config=config,
            policy=policy,
            now=now,
        ):
            return True
    return False


def plan_matches_live_material(
    plan: PersonCoveragePlanRecord,
    person: CoveragePersonMaterialView,
    config: AssessArticleConfig,
    *,
    source_policy_fingerprint: str,
) -> bool:
    """Forward-only material match (K30): recompute fingerprint with plan's refresh."""
    expected = coverage_material_fingerprint(
        person,
        config,
        source_policy_fingerprint=source_policy_fingerprint,
        refresh_of_plan_id=plan.refresh_of_plan_id,
    )
    return plan.material_fingerprint == expected


def is_coverage_research_eligible(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> bool:
    """Shared K5/K19 eligibility used by seed, digest, and status."""
    person_row = connection.execute(
        """
        SELECT id, merged_into_person_id, identity_fingerprint,
               current_wikipedia_identity_observation_id
          FROM person
         WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    if person_row is None:
        return False
    if person_row["merged_into_person_id"] is not None:
        return False
    canonical_id = int(person_row["id"])
    if not _has_operational_match_key_name(connection, person_id=canonical_id):
        return False

    wiki_id = person_row["current_wikipedia_identity_observation_id"]
    if wiki_id is None:
        return False
    wiki = connection.execute(
        """
        SELECT disposition, semantic_outcome
          FROM wikipedia_identity_observation
         WHERE id = ?
        """,
        (int(wiki_id),),
    ).fetchone()
    if wiki is None or wiki["disposition"] != "completed":
        return False
    if wiki["semantic_outcome"] == "matching_page_found":
        return False
    if wiki["semantic_outcome"] not in _ELIGIBLE_WIKI_OUTCOMES:
        return False

    assess = config.tasks.assess_article
    person_view = CoveragePersonMaterialView(
        person_id=canonical_id,
        identity_fingerprint=str(person_row["identity_fingerprint"]),
    )
    policy_fp = policy.fingerprint
    base_fp = coverage_material_fingerprint(
        person_view,
        assess,
        source_policy_fingerprint=policy_fp,
        refresh_of_plan_id=None,
    )

    cur_plan = _latest_terminal_plan_matching_live(
        connection,
        person_id=canonical_id,
        person_view=person_view,
        assess=assess,
        source_policy_fingerprint=policy_fp,
    )
    if cur_plan is not None:
        if cur_plan.status == "failed":
            return False
        if cur_plan.status in ("completed", "incomplete"):
            completed_at = cur_plan.completed_at
            if completed_at is None:
                return False
            if not _refresh_interval_elapsed(
                completed_at=completed_at,
                now=now,
                hours=assess.coverage_refresh_interval_hours,
            ):
                return False
            live_fp = coverage_material_fingerprint(
                person_view,
                assess,
                source_policy_fingerprint=policy_fp,
                refresh_of_plan_id=cur_plan.id,
            )
            return not _has_terminal_plan_fp(
                connection, person_id=canonical_id, material_fingerprint=live_fp
            ) and not _has_active_plan_fp(
                connection, person_id=canonical_id, material_fingerprint=live_fp
            )
        return False

    terminal_base = _terminal_plan_for_fingerprint(
        connection, person_id=canonical_id, material_fingerprint=base_fp
    )
    if terminal_base is not None:
        if terminal_base.status == "failed":
            return False
        if terminal_base.status in ("completed", "incomplete"):
            completed_at = terminal_base.completed_at
            if completed_at is None:
                return False
            if not _refresh_interval_elapsed(
                completed_at=completed_at,
                now=now,
                hours=assess.coverage_refresh_interval_hours,
            ):
                return False
            live_fp = coverage_material_fingerprint(
                person_view,
                assess,
                source_policy_fingerprint=policy_fp,
                refresh_of_plan_id=terminal_base.id,
            )
            return not _has_terminal_plan_fp(
                connection, person_id=canonical_id, material_fingerprint=live_fp
            ) and not _has_active_plan_fp(
                connection, person_id=canonical_id, material_fingerprint=live_fp
            )
        return False

    return not _has_active_plan_fp(
        connection, person_id=canonical_id, material_fingerprint=base_fp
    )


def ensure_coverage_research(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> str:
    """Open coverage research for an eligible person, or no-op.

    Returns ``scheduled``, ``reused``, ``ineligible``, or ``stopped_matching``.
    Joins an open caller transaction when present; otherwise opens a brief
    ``BEGIN IMMEDIATE``.
    """
    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        result = _ensure_coverage_research_body(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=now,
        )
    except BaseException:
        if owns_transaction:
            connection.rollback()
        raise
    else:
        if owns_transaction:
            connection.commit()
        return result


def schedule_coverage_after_wikipedia_ready(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    """After completed Wikipedia identity: supersede on matching, else ensure.

    Mandatory on every completed Wikipedia settle path (including matching) so
    mid-flight coverage work is cancelled when a page match is found (K5).
    """
    person_row = connection.execute(
        """
        SELECT id, merged_into_person_id, current_wikipedia_identity_observation_id
          FROM person
         WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    if person_row is None:
        return
    if person_row["merged_into_person_id"] is not None:
        supersede_coverage_work_for_person(
            connection,
            person_id=int(person_row["id"]),
            run_id=run_id,
            now=now,
            reason=MERGED_AWAY_COVERAGE_REASON,
        )
        return
    wiki_id = person_row["current_wikipedia_identity_observation_id"]
    if wiki_id is None:
        return
    wiki = connection.execute(
        """
        SELECT disposition, semantic_outcome
          FROM wikipedia_identity_observation
         WHERE id = ?
        """,
        (int(wiki_id),),
    ).fetchone()
    if wiki is None or wiki["disposition"] != "completed":
        return
    if wiki["semantic_outcome"] == "matching_page_found":
        supersede_coverage_work_for_person(
            connection,
            person_id=int(person_row["id"]),
            run_id=run_id,
            now=now,
            reason=MATCHING_WIKIPEDIA_REASON,
        )
        return
    if wiki["semantic_outcome"] in _ELIGIBLE_WIKI_OUTCOMES:
        ensure_coverage_research(
            connection,
            person_id=int(person_row["id"]),
            run_id=run_id,
            config=config,
            policy=policy,
            now=now,
        )


def seed_coverage_research(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> int:
    """Backfill coverage for every canonical person after Wikipedia seed.

    Matching current Wikipedia → supersede mid-flight work. Else ensure when
    eligible. Returns the count of people for which ensure returned a non-
    ineligible result (``scheduled``, ``reused``, or ``stopped_matching``),
    plus people whose matching path superseded active coverage.
    """
    rows = connection.execute(
        """
        SELECT id
          FROM person
         WHERE merged_into_person_id IS NULL
         ORDER BY id
        """
    ).fetchall()
    acted = 0
    for row in rows:
        person_id = int(row["id"])
        wiki = _current_wikipedia_row(connection, person_id=person_id)
        if wiki is not None and wiki["semantic_outcome"] == "matching_page_found":
            had_active = _person_has_active_coverage(connection, person_id=person_id)
            supersede_coverage_work_for_person(
                connection,
                person_id=person_id,
                run_id=run_id,
                now=now,
                reason=MATCHING_WIKIPEDIA_REASON,
            )
            if had_active:
                acted += 1
            continue
        result = ensure_coverage_research(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=now,
        )
        if result != "ineligible":
            acted += 1
    return acted


def supersede_coverage_work_for_person(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    now: str,
    reason: str = COVERAGE_SUPERSEDED_REASON,
) -> None:
    """Supersede active coverage plans and work for a person (K5 / K18).

    Does not delete historical assessments, occurrences, or views.
    Does not open a new plan.

    **The caller must already hold an open transaction** when one is required
    by surrounding settlement; this helper also joins or opens one briefly.
    """
    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        _supersede_coverage_work_for_person_body(
            connection,
            person_id=person_id,
            run_id=run_id,
            now=now,
            reason=reason,
        )
    except BaseException:
        if owns_transaction:
            connection.rollback()
        raise
    else:
        if owns_transaction:
            connection.commit()


def _ensure_coverage_research_body(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> str:
    person_row = connection.execute(
        """
        SELECT id, merged_into_person_id, identity_fingerprint,
               current_wikipedia_identity_observation_id
          FROM person
         WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    if person_row is None:
        return "ineligible"
    if person_row["merged_into_person_id"] is not None:
        supersede_coverage_work_for_person(
            connection,
            person_id=int(person_row["id"]),
            run_id=run_id,
            now=now,
            reason=MERGED_AWAY_COVERAGE_REASON,
        )
        return "ineligible"

    canonical_id = int(person_row["id"])
    wiki_id = person_row["current_wikipedia_identity_observation_id"]
    if wiki_id is None:
        return "ineligible"
    wiki = connection.execute(
        """
        SELECT disposition, semantic_outcome
          FROM wikipedia_identity_observation
         WHERE id = ?
        """,
        (int(wiki_id),),
    ).fetchone()
    if wiki is None or wiki["disposition"] != "completed":
        return "ineligible"
    if wiki["semantic_outcome"] == "matching_page_found":
        supersede_coverage_work_for_person(
            connection,
            person_id=canonical_id,
            run_id=run_id,
            now=now,
            reason=MATCHING_WIKIPEDIA_REASON,
        )
        return "stopped_matching"
    if wiki["semantic_outcome"] not in _ELIGIBLE_WIKI_OUTCOMES:
        return "ineligible"
    if not _has_operational_match_key_name(connection, person_id=canonical_id):
        return "ineligible"

    target = _resolve_coverage_live_target(
        connection,
        person_id=canonical_id,
        identity_fingerprint=str(person_row["identity_fingerprint"]),
        config=config,
        policy=policy,
        now=now,
    )
    if target is None:
        return "ineligible"
    live_fp, refresh_of, _ = target

    active = load_active_plan_for_fingerprint(
        connection,
        person_id=canonical_id,
        material_fingerprint=live_fp,
    )
    if active is not None:
        return "reused"
    if _has_terminal_plan_fp(
        connection, person_id=canonical_id, material_fingerprint=live_fp
    ):
        return "reused"

    open_coverage_plan(
        connection,
        person_id=canonical_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=now,
        material_fingerprint=live_fp,
        refresh_of_plan_id=refresh_of,
    )
    return "scheduled"


def _resolve_coverage_live_target(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    identity_fingerprint: str,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> tuple[str, int | None, int] | None:
    """Return ``(live_fp, refresh_of_plan_id, person_id)`` for ensure/open."""
    assess = config.tasks.assess_article
    person_view = CoveragePersonMaterialView(
        person_id=person_id,
        identity_fingerprint=identity_fingerprint,
    )
    policy_fp = policy.fingerprint
    base_fp = coverage_material_fingerprint(
        person_view,
        assess,
        source_policy_fingerprint=policy_fp,
        refresh_of_plan_id=None,
    )

    cur_plan = _latest_terminal_plan_matching_live(
        connection,
        person_id=person_id,
        person_view=person_view,
        assess=assess,
        source_policy_fingerprint=policy_fp,
    )
    if cur_plan is not None:
        if cur_plan.status == "failed":
            return None
        if cur_plan.status in ("completed", "incomplete"):
            completed_at = cur_plan.completed_at
            if completed_at is None:
                return None
            if not _refresh_interval_elapsed(
                completed_at=completed_at,
                now=now,
                hours=assess.coverage_refresh_interval_hours,
            ):
                return None
            live_fp = coverage_material_fingerprint(
                person_view,
                assess,
                source_policy_fingerprint=policy_fp,
                refresh_of_plan_id=cur_plan.id,
            )
            return live_fp, cur_plan.id, person_id
        return None

    terminal_base = _terminal_plan_for_fingerprint(
        connection, person_id=person_id, material_fingerprint=base_fp
    )
    if terminal_base is not None:
        if terminal_base.status == "failed":
            return None
        if terminal_base.status in ("completed", "incomplete"):
            completed_at = terminal_base.completed_at
            if completed_at is None:
                return None
            if not _refresh_interval_elapsed(
                completed_at=completed_at,
                now=now,
                hours=assess.coverage_refresh_interval_hours,
            ):
                return None
            live_fp = coverage_material_fingerprint(
                person_view,
                assess,
                source_policy_fingerprint=policy_fp,
                refresh_of_plan_id=terminal_base.id,
            )
            return live_fp, terminal_base.id, person_id
        return None

    return base_fp, None, person_id


def _supersede_coverage_work_for_person_body(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    now: str,
    reason: str,
) -> None:
    plan_rows = connection.execute(
        """
        SELECT id, material_fingerprint
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
         ORDER BY id
        """,
        (person_id,),
    ).fetchall()
    for row in plan_rows:
        plan_id = int(row["id"])
        _supersede_coverage_plan_and_work(
            connection,
            plan_id=plan_id,
            person_id=person_id,
            run_id=run_id,
            now=now,
            reason=reason,
        )

    # Assess work may still name person_article rows for this person.
    pa_ids = [
        int(row["id"])
        for row in connection.execute(
            "SELECT id FROM person_article WHERE person_id = ?",
            (person_id,),
        )
    ]
    if pa_ids:
        placeholders = ",".join("?" for _ in pa_ids)
        connection.execute(
            f"""
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id IN ({placeholders})
               AND state IN ('pending', 'deferred')
            """,
            (
                reason,
                run_id,
                now,
                ASSESS_ARTICLE_TASK_TYPE,
                SUBJECT_KIND_PERSON_ARTICLE,
                *pa_ids,
            ),
        )


def _supersede_coverage_plan_and_work(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    person_id: int,
    run_id: int,
    now: str,
    reason: str,
) -> None:
    del person_id  # reserved for future person-scoped work kinds
    form_ids = [
        int(row["id"])
        for row in connection.execute(
            "SELECT id FROM coverage_query_form WHERE plan_id = ?",
            (plan_id,),
        )
    ]
    if form_ids:
        placeholders = ",".join("?" for _ in form_ids)
        connection.execute(
            f"""
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id IN ({placeholders})
               AND state IN ('pending', 'deferred')
            """,
            (
                reason,
                run_id,
                now,
                BRAVE_WEB_SEARCH_TASK_TYPE,
                SUBJECT_KIND_COVERAGE_QUERY_FORM,
                *form_ids,
            ),
        )

    target_ids = [
        int(row["id"])
        for row in connection.execute(
            "SELECT id FROM coverage_article_target WHERE plan_id = ?",
            (plan_id,),
        )
    ]
    if target_ids:
        placeholders = ",".join("?" for _ in target_ids)
        connection.execute(
            f"""
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id IN ({placeholders})
               AND state IN ('pending', 'deferred')
            """,
            (
                reason,
                run_id,
                now,
                FETCH_ARTICLE_TASK_TYPE,
                SUBJECT_KIND_COVERAGE_ARTICLE_TARGET,
                *target_ids,
            ),
        )

    supersede_pending_targets_for_plan(connection, plan_id=plan_id)
    plan = load_plan(connection, plan_id=plan_id)
    if plan is not None and plan.status in _ACTIVE_PLAN_STATUSES:
        supersede_plan(connection, plan_id=plan_id, completed_at=now)


def _latest_terminal_plan_matching_live(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    person_view: CoveragePersonMaterialView,
    assess: AssessArticleConfig,
    source_policy_fingerprint: str,
) -> PersonCoveragePlanRecord | None:
    rows = connection.execute(
        """
        SELECT *
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('completed', 'failed', 'incomplete')
         ORDER BY id DESC
        """,
        (person_id,),
    ).fetchall()
    for row in rows:
        plan = load_plan(connection, plan_id=int(row["id"]))
        if plan is None:
            continue
        if plan_matches_live_material(
            plan,
            person_view,
            assess,
            source_policy_fingerprint=source_policy_fingerprint,
        ):
            return plan
    return None


def _terminal_plan_for_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
) -> PersonCoveragePlanRecord | None:
    row = connection.execute(
        """
        SELECT *
          FROM person_coverage_plan
         WHERE person_id = ?
           AND material_fingerprint = ?
           AND status IN ('completed', 'failed', 'incomplete')
         ORDER BY id DESC
         LIMIT 1
        """,
        (person_id, material_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return load_plan(connection, plan_id=int(row["id"]))


def _has_terminal_plan_fp(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
) -> bool:
    return (
        _terminal_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=material_fingerprint,
        )
        is not None
    )


def _has_active_plan_fp(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
) -> bool:
    return (
        load_active_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=material_fingerprint,
        )
        is not None
    )


def _has_operational_match_key_name(
    connection: sqlite3.Connection, *, person_id: int
) -> bool:
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return False
    placeholders = ",".join("?" for _ in closure)
    row = connection.execute(
        f"""
        SELECT 1 AS present
          FROM sourced_name
         WHERE person_id IN ({placeholders})
           AND length(match_key) > 0
         LIMIT 1
        """,
        closure,
    ).fetchone()
    return row is not None


def _refresh_interval_elapsed(*, completed_at: str, now: str, hours: int) -> bool:
    completed_dt = _parse_utc_timestamp(completed_at)
    now_dt = _parse_utc_timestamp(now)
    return now_dt - completed_dt >= timedelta(hours=hours)


def _parse_utc_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _current_wikipedia_row(
    connection: sqlite3.Connection, *, person_id: int
) -> sqlite3.Row | None:
    person = connection.execute(
        """
        SELECT current_wikipedia_identity_observation_id
          FROM person
         WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    if person is None or person["current_wikipedia_identity_observation_id"] is None:
        return None
    return connection.execute(
        """
        SELECT disposition, semantic_outcome
          FROM wikipedia_identity_observation
         WHERE id = ?
        """,
        (int(person["current_wikipedia_identity_observation_id"]),),
    ).fetchone()


def _person_has_active_coverage(
    connection: sqlite3.Connection, *, person_id: int
) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
         LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    return row is not None


@dataclass(frozen=True, slots=True)
class AssessWorkContext:
    """Resolved plan/view/screening material for one assess work fingerprint."""

    plan_id: int | None
    material_fingerprint: str
    article_view_id: int
    screening_rule_id: str
    screening_rule_status: str
    source_policy_fingerprint: str
    plan_status: str | None


def resolve_assess_work_context(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    task_fingerprint: str,
) -> AssessWorkContext | None:
    """Recover plan/view/screening for an assess work-item fingerprint.

    The work fingerprint embeds material + person_article_id + article_view_id;
    subject_id alone is the person_article. Used by prepare and K23 preflight.
    """
    person_article = load_person_article(
        connection, person_article_id=person_article_id
    )
    if person_article is None:
        return None
    rows = connection.execute(
        """
        SELECT av.id AS article_view_id,
               p.id AS plan_id,
               p.material_fingerprint AS material_fingerprint,
               p.status AS plan_status,
               p.source_policy_fingerprint AS source_policy_fingerprint
          FROM article_view AS av
          JOIN coverage_article_target AS t
            ON t.article_view_id = av.id
          JOIN person_coverage_plan AS p
            ON p.id = t.plan_id
         WHERE av.canonical_article_id = ?
           AND p.person_id = ?
         ORDER BY av.id, p.id
        """,
        (person_article.canonical_article_id, person_article.person_id),
    ).fetchall()
    for row in rows:
        material = str(row["material_fingerprint"])
        view_id = int(row["article_view_id"])
        if (
            assess_task_fingerprint(
                material_fingerprint=material,
                person_article_id=person_article_id,
                article_view_id=view_id,
            )
            != task_fingerprint
        ):
            continue
        screening = _screening_for_article_on_plan(
            connection,
            plan_id=int(row["plan_id"]),
            canonical_article_id=person_article.canonical_article_id,
        )
        if screening is None:
            screening = (
                "unknown",
                "unclassified",
                str(row["source_policy_fingerprint"]),
            )
        return AssessWorkContext(
            plan_id=int(row["plan_id"]),
            material_fingerprint=material,
            article_view_id=view_id,
            screening_rule_id=screening[0],
            screening_rule_status=screening[1],
            source_policy_fingerprint=screening[2],
            plan_status=str(row["plan_status"]),
        )
    return None


def open_coverage_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
    material_fingerprint: str,
    refresh_of_plan_id: int | None = None,
) -> int:
    """Open a retrieving plan: discovery attach (K10) + stage-1 exact forms.

    Exact-name forms always open even when discovery already meets the
    retrieval target. Alias/context are deferred to ``maybe_advance_coverage_plan``.

    **The caller must already hold an open transaction.**
    """
    assess = config.tasks.assess_article
    plan_id = open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=material_fingerprint,
        source_policy_fingerprint=policy.fingerprint,
        retrieval_target=assess.retrieval_target,
        created_at=now,
        refresh_of_plan_id=refresh_of_plan_id,
    )
    attach_discovery_at_plan_open(
        connection,
        plan_id=plan_id,
        person_id=person_id,
        policy=policy,
        now=now,
    )
    eligible = _recompute_eligible_selected_count(
        connection,
        plan_id=plan_id,
        max_eligible_fetches=assess.max_eligible_fetches,
    )
    update_plan_status(
        connection,
        plan_id=plan_id,
        status="retrieving",
        eligible_selected_count=eligible,
    )

    name_texts = _operational_name_texts(connection, person_id=person_id)
    death_supported = _death_supported(connection, person_id=person_id)
    exact_specs = generate_exact_forms(
        name_texts,
        max_exact_forms=assess.max_exact_forms,
        death_supported=death_supported,
    )
    if exact_specs:
        form_ids = insert_query_forms(
            connection,
            plan_id=plan_id,
            forms=tuple(
                {
                    "ordinal": index,
                    "stage": 1,
                    "variant_kind": spec.variant_kind,
                    "query_text": spec.query_text,
                }
                for index, spec in enumerate(exact_specs, start=1)
            ),
        )
        for form_id in form_ids:
            schedule_brave_web_search(
                connection,
                form_id=form_id,
                material_fingerprint=material_fingerprint,
                offset_in=0,
                run_id=run_id,
                now=now,
            )
    else:
        # No exact forms possible: still advance (may terminalize via discovery).
        maybe_advance_coverage_plan(
            connection,
            plan_id=plan_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=now,
        )
    return plan_id


def attach_discovery_at_plan_open(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    person_id: int,
    policy: SourcePolicy,
    now: str,
) -> None:
    """K10 discovery attach — stage 0 only at plan open (not in maybe_advance).

    Uses ``attach_discovery_url`` / ``canonicalize_article_url`` (via screening);
    does not invent a second canonicalize function.
    """
    for mention_id, source_item_id, original_url in _discovery_mention_urls(
        connection, person_id=person_id
    ):
        decision = attach_discovery_url(policy, url=original_url)
        if decision.kind is DiscoveryAttachKind.NEITHER:
            continue
        if decision.kind is DiscoveryAttachKind.SCREENING_ONLY:
            assert decision.screening is not None
            insert_source_screening(
                connection,
                canonical_article_id=None,
                url=decision.screening.raw_url,
                publisher_key=None,
                rule_id=decision.screening.rule_id,
                rule_status=decision.screening.rule_status,
                source_policy_fingerprint=policy.fingerprint,
                decided_at=now,
                plan_id=plan_id,
                source_item_id=source_item_id,
                person_mention_id=mention_id,
            )
            continue

        # Usable URL: upsert article + feed alias, screen, discovery row.
        assert decision.canonical_url is not None
        assert decision.screening is not None
        article_id = upsert_article(
            connection,
            canonical_url=decision.canonical_url,
            publisher_key=publisher_key(decision.canonical_url),
            now=now,
        )
        # Prefer original discovery URL as feed_original alias when present.
        if original_url:
            record_alias(
                connection,
                canonical_article_id=article_id,
                url=original_url,
                kind="feed_original",
                now=now,
            )
        screening = decision.screening
        screening_id = insert_source_screening(
            connection,
            canonical_article_id=article_id,
            url=screening.raw_url,
            publisher_key=screening.publisher_key,
            rule_id=screening.rule_id,
            rule_status=screening.rule_status,
            source_policy_fingerprint=policy.fingerprint,
            decided_at=now,
            plan_id=plan_id,
            source_item_id=source_item_id,
            person_mention_id=mention_id,
        )
        # UNIQUE(plan_id, canonical_article_id): keep first discovery row.
        existing = connection.execute(
            """
            SELECT id FROM coverage_discovery_article
             WHERE plan_id = ? AND canonical_article_id = ?
            """,
            (plan_id, article_id),
        ).fetchone()
        if existing is None:
            insert_coverage_discovery_article(
                connection,
                plan_id=plan_id,
                canonical_article_id=article_id,
                source_item_id=source_item_id,
                screening_id=screening_id,
                person_mention_id=mention_id,
            )


def maybe_advance_coverage_plan(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    """Join-point after search/target/assess settlement (design steps 1–11).

    Discovery is attached only at plan open (step 3: do not re-attach here).
    **The caller must already hold an open transaction.**
    """
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None:
        return
    if plan.status in _TERMINAL_PLAN_STATUSES:
        return

    assess = config.tasks.assess_article
    forms = list(list_query_forms_for_plan(connection, plan_id=plan_id))

    # 2. Any form still pending → wait.
    if any(form.status == "pending" for form in forms):
        return

    # 4. Recompute eligible_selected_count (includes discovery).
    eligible = _recompute_eligible_selected_count(
        connection,
        plan_id=plan_id,
        max_eligible_fetches=assess.max_eligible_fetches,
    )
    if eligible != plan.eligible_selected_count:
        update_plan_status(
            connection,
            plan_id=plan_id,
            status=plan.status,
            eligible_selected_count=eligible,
        )
        plan = load_plan(connection, plan_id=plan_id)
        if plan is None or plan.status in _TERMINAL_PLAN_STATUSES:
            return

    # 5. Stage-1 terminal and target unmet → insert alias forms once.
    if (
        _stage_terminal(forms, stage=1)
        and eligible < plan.retrieval_target
        and not any(form.stage == 2 for form in forms)
    ):
        if _insert_alias_forms_if_needed(
            connection,
            plan=plan,
            forms=forms,
            assess=assess,
            run_id=run_id,
            now=now,
        ):
            return
        forms = list(list_query_forms_for_plan(connection, plan_id=plan_id))

    # 6. Alias terminal (or skipped) and target unmet → at most one context form.
    stage2_forms = [f for f in forms if f.stage == 2]
    stage2_done = not stage2_forms or all(
        f.status in {"completed", "failed"} for f in stage2_forms
    )
    if (
        _stage_terminal(forms, stage=1)
        and stage2_done
        and eligible < plan.retrieval_target
        and not any(form.stage == 3 for form in forms)
    ):
        if _insert_context_form_if_needed(
            connection,
            plan=plan,
            forms=forms,
            assess=assess,
            run_id=run_id,
            now=now,
        ):
            return
        forms = list(list_query_forms_for_plan(connection, plan_id=plan_id))

    # Still waiting on newly scheduled forms?
    if any(form.status == "pending" for form in forms):
        return

    # 7. All search stages that will run are terminal → final selection.
    targets = list(list_coverage_article_targets_for_plan(connection, plan_id=plan_id))
    if not targets and plan.status in {"retrieving", "selecting"}:
        _apply_final_selection(
            connection,
            plan=plan,
            assess=assess,
            policy=policy,
            run_id=run_id,
            now=now,
        )
        targets = list(
            list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
        )
        plan = load_plan(connection, plan_id=plan_id)
        if plan is None or plan.status in _TERMINAL_PLAN_STATUSES:
            return

    # 8. Pending fetch targets → wait.
    if any(t.status == "pending" for t in targets):
        if plan is not None and plan.status == "retrieving":
            update_plan_status(connection, plan_id=plan_id, status="selecting")
        return

    # 9–10. Assess pending / views (design steps 9–10). Active assess work
    # blocks terminalize. Targets that are ``fetched`` without a terminal
    # assessment still need assess (Task 7); do not complete the plan yet.
    if _has_active_assess_work(connection, plan_id=plan_id):
        if plan is not None and plan.status in {"retrieving", "selecting"}:
            update_plan_status(connection, plan_id=plan_id, status="assessing")
        return

    if targets and not _selected_paths_ready_to_terminalize(
        connection, plan_id=plan_id, person_id=plan.person_id, targets=targets
    ):
        # Mid-flight after fetch: stay selecting/assessing until assess lands
        # or the path is permanently dead without a view (failed/snippets_only).
        if (
            any(t.status == "fetched" for t in targets)
            and plan is not None
            and plan.status in {"retrieving", "selecting"}
        ):
            update_plan_status(connection, plan_id=plan_id, status="assessing")
        return

    # 11. Apply terminal truth table.
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None or plan.status in _TERMINAL_PLAN_STATUSES:
        return
    forms = list(list_query_forms_for_plan(connection, plan_id=plan_id))
    targets = list(list_coverage_article_targets_for_plan(connection, plan_id=plan_id))
    _terminalize_plan(
        connection,
        plan=plan,
        forms=forms,
        targets=targets,
        now=now,
    )


def build_brave_web_search_handler(
    connection: sqlite3.Connection,
    *,
    client: WebSearchClient,
    config: MainConfig,
    policy: SourcePolicy,
) -> TaskHandler:
    """HTTP handler: one ``search_web`` call per execute; no OpenRouter budget."""

    def prepare(work_item: WorkItem) -> TaskPreparation:
        form_id = work_item.subject_id
        if form_id is None:
            raise ValueError("brave_web_search work item names no query form")
        form = load_query_form(connection, form_id=form_id)
        if form is None:
            raise ValueError(f"coverage_query_form {form_id} is missing")
        plan = load_plan(connection, plan_id=form.plan_id)
        if plan is None:
            raise ValueError(f"person_coverage_plan {form.plan_id} is missing")
        if plan.status == "superseded":
            raise ValueError("coverage plan is superseded")
        if form.status != "pending":
            raise ValueError(f"coverage_query_form {form_id} is not pending")

        assess = config.tasks.assess_article
        offset_in = _resolve_offset_in(
            connection,
            fingerprint=work_item.fingerprint,
            material_fingerprint=plan.material_fingerprint,
            query_form_id=form_id,
            search_count=assess.search_count,
        )
        return TaskPreparation(
            payload=_BraveSearchCall(
                form_id=form_id,
                plan_id=form.plan_id,
                query_text=form.query_text,
                offset_in=offset_in,
                offsets_used_before=form.offsets_used,
                material_fingerprint=plan.material_fingerprint,
                search_count=assess.search_count,
                max_offsets_per_form=assess.max_offsets_per_form,
                max_results_per_form=assess.max_results_per_form,
                reject_altered_query=assess.reject_altered_query,
            )
        )

    def destination_host(_work_item: WorkItem) -> str | None:
        try:
            return urlsplit(config.brave.endpoint).hostname
        except Exception:
            return None

    def request_fingerprint(work_item: WorkItem) -> str:
        return work_item.fingerprint

    return TaskHandler(
        task_type=BRAVE_WEB_SEARCH_TASK_TYPE,
        provider=BRAVE_PROVIDER,
        operation=OPERATION_SEARCH_WEB,
        execute=_execute_brave_for(client),
        request_fingerprint=request_fingerprint,
        reserved_nano_usd=0,
        prepare=prepare,
        persist=_persist_brave_for(connection, config=config, policy=policy),
        persist_failure=_persist_brave_failure_for(
            connection, config=config, policy=policy
        ),
        destination_host=destination_host,
        pool=WorkerPool.HTTP,
    )


def build_fetch_article_handler(
    connection: sqlite3.Connection,
    *,
    fetcher: ArticleFetcher,
    extractor: ArticleExtractor,
    config: MainConfig,
    policy: SourcePolicy,
) -> TaskHandler:
    """HTTP handler: one article GET + in-process extract; no HTML on return (K3)."""

    def prepare(work_item: WorkItem) -> TaskPreparation:
        target_id = work_item.subject_id
        if target_id is None:
            raise ValueError("fetch_article work item names no coverage target")
        target = load_coverage_article_target(connection, target_id=target_id)
        if target is None:
            raise ValueError(f"coverage_article_target {target_id} is missing")
        if target.status != "pending":
            raise ValueError(
                f"coverage_article_target {target_id} is not pending ({target.status})"
            )
        plan = load_plan(connection, plan_id=target.plan_id)
        if plan is None:
            raise ValueError(f"person_coverage_plan {target.plan_id} is missing")
        if plan.status == "superseded":
            raise ValueError("coverage plan is superseded")
        person_article = load_person_article_by_pair(
            connection,
            person_id=plan.person_id,
            canonical_article_id=target.canonical_article_id,
        )
        return TaskPreparation(
            payload=_FetchArticleCall(
                target_id=target.id,
                plan_id=plan.id,
                person_id=plan.person_id,
                canonical_article_id=target.canonical_article_id,
                request_url=target.request_url,
                material_fingerprint=plan.material_fingerprint,
                person_article_id=person_article.id if person_article else None,
            )
        )

    def destination_host(work_item: WorkItem) -> str | None:
        target_id = work_item.subject_id
        if target_id is None:
            return None
        target = load_coverage_article_target(connection, target_id=int(target_id))
        if target is None:
            return None
        try:
            return urlsplit(target.request_url).hostname
        except Exception:
            return None

    def request_fingerprint(work_item: WorkItem) -> str:
        return work_item.fingerprint

    return TaskHandler(
        task_type=FETCH_ARTICLE_TASK_TYPE,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
        execute=_execute_fetch_for(fetcher, extractor),
        request_fingerprint=request_fingerprint,
        reserved_nano_usd=0,
        prepare=prepare,
        persist=_persist_fetch_for(connection, config=config, policy=policy),
        persist_failure=_persist_fetch_failure_for(
            connection, config=config, policy=policy
        ),
        destination_host=destination_host,
        pool=WorkerPool.HTTP,
    )


# ---------------------------------------------------------------------------
# Execute / persist
# ---------------------------------------------------------------------------


def _execute_brave_for(
    client: WebSearchClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _BraveSearchCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=BRAVE_PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        page = client.search_web(
            prepared.query_text,
            count=prepared.search_count,
            offset=prepared.offset_in,
        )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None, payload=page)

    return execute


def _persist_brave_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        if not isinstance(outcome.payload, SearchPage):
            raise RuntimeError(
                f"unexpected brave payload type: {type(outcome.payload).__name__}"
            )
        page = outcome.payload
        form_id = work_item.subject_id
        if form_id is None:
            raise RuntimeError("brave work item missing subject_id")
        form = load_query_form(connection, form_id=form_id)
        if form is None:
            return
        plan = load_plan(connection, plan_id=form.plan_id)
        if plan is None or plan.status == "superseded":
            return
        if form.status != "pending":
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        assess = config.tasks.assess_article
        offset_in = _resolve_offset_in(
            connection,
            fingerprint=work_item.fingerprint,
            material_fingerprint=plan.material_fingerprint,
            query_form_id=form_id,
            search_count=assess.search_count,
        )

        prior_obs = connection.execute(
            "SELECT id FROM brave_search_observation WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        is_new = prior_obs is None

        observation_id = insert_or_load_brave_search_observation_by_attempt(
            connection,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text=form.query_text,
            altered_query=page.altered_query,
            offset_in=offset_in,
            count_requested=page.count,
            result_count=len(page.results),
            truncated=bool(page.more_results),
            response_complete=not page.more_results,
            observed_at=observed_at,
        )
        if not is_new:
            maybe_advance_coverage_plan(
                connection,
                plan_id=form.plan_id,
                run_id=run_id,
                config=config,
                policy=policy,
                now=observed_at,
            )
            return

        # Altered-query reject: store observation, fail form, no page advancement.
        if assess.reject_altered_query and page.altered_query:
            mark_query_form_failed(
                connection,
                form_id=form_id,
                failure_category=_ALTERED_QUERY_CATEGORY,
                offsets_used=form.offsets_used,
                result_count=(form.result_count or 0) + len(page.results),
                truncated=bool(page.more_results),
            )
            update_plan_status(
                connection,
                plan_id=form.plan_id,
                status=plan.status,
                partial_retrieval=True,
            )
            maybe_advance_coverage_plan(
                connection,
                plan_id=form.plan_id,
                run_id=run_id,
                config=config,
                policy=policy,
                now=observed_at,
            )
            return

        # Cap stored results by remaining form budget.
        prior_count = form.result_count or 0
        remaining = max(0, assess.max_results_per_form - prior_count)
        stored = page.results[:remaining]
        hit_cap = len(page.results) > remaining

        occurrences = _screen_search_results(
            connection,
            plan_id=form.plan_id,
            policy=policy,
            results=stored,
            now=observed_at,
        )
        insert_search_result_occurrences(
            connection,
            search_observation_id=observation_id,
            occurrences=occurrences,
        )

        new_total = prior_count + len(stored)
        # offsets_used counts additional pages after the first.
        new_offsets_used = form.offsets_used
        if offset_in > 0:
            new_offsets_used = form.offsets_used + 1

        more = bool(page.more_results)
        results_budget_left = new_total < assess.max_results_per_form
        can_continue = (
            more
            and results_budget_left
            and new_offsets_used < assess.max_offsets_per_form
        )
        truncated = bool(
            (more and not can_continue)
            or hit_cap
            or (new_total >= assess.max_results_per_form and more)
        )

        if can_continue:
            mark_query_form_progress(
                connection,
                form_id=form_id,
                offsets_used=new_offsets_used,
                result_count=new_total,
                truncated=False,
            )
            next_offset = offset_in + assess.search_count
            schedule_brave_web_search(
                connection,
                form_id=form_id,
                material_fingerprint=plan.material_fingerprint,
                offset_in=next_offset,
                run_id=run_id,
                now=observed_at,
            )
            return

        mark_query_form_completed(
            connection,
            form_id=form_id,
            offsets_used=new_offsets_used,
            result_count=new_total,
            truncated=truncated,
        )
        if truncated:
            update_plan_status(
                connection,
                plan_id=form.plan_id,
                status=plan.status,
                truncated_unsafe=True,
            )
        maybe_advance_coverage_plan(
            connection,
            plan_id=form.plan_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=observed_at,
        )

    return persist


def _persist_brave_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        state_row = connection.execute(
            "SELECT state, subject_id FROM work_item WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        if state_row is None or state_row["state"] != "failed_permanent":
            return
        form_id = state_row["subject_id"]
        if form_id is None:
            return
        form = load_query_form(connection, form_id=int(form_id))
        if form is None or form.status != "pending":
            return
        plan = load_plan(connection, plan_id=form.plan_id)
        if plan is None or plan.status == "superseded":
            return

        _attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        del _attempt_id
        mark_query_form_failed(
            connection,
            form_id=int(form_id),
            failure_category=str(failure.category),
        )
        update_plan_status(
            connection,
            plan_id=form.plan_id,
            status=plan.status,
            partial_retrieval=True,
        )
        maybe_advance_coverage_plan(
            connection,
            plan_id=form.plan_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=observed_at,
        )

    return persist_failure


def _execute_fetch_for(
    fetcher: ArticleFetcher,
    extractor: ArticleExtractor,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _FetchArticleCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = fetcher.fetch_article(prepared.request_url)
        if isinstance(result, ArticleAccessDenied):
            # Typed access outcome: no HTML ever existed on this path.
            return TaskOutcome(
                state=WorkState.SUCCEEDED,
                reason=None,
                payload=_FetchArticlePayload(
                    kind="access_denied",
                    requested_url=result.requested_url,
                    final_url=result.final_url,
                    status_code=result.status_code,
                    access_kind="snippets",
                    title=None,
                    dek=None,
                    byline=None,
                    published_at=None,
                    editorial_labels=(),
                    blocks=(),
                    extraction_quality=None,
                    access_denied_kind=str(result.kind),
                    detail=result.detail,
                ),
            )
        if not isinstance(result, ArticleFetchSuccess):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                detail=f"unexpected fetch result {type(result).__name__}",
            )

        # Extract in-process on the worker; drop HTML before return (K3).
        html = result.html
        extracted = extractor.extract_article(html)
        # Explicitly drop HTML references so the payload cannot retain them.
        del html
        payload = _payload_from_extracted(
            extracted,
            requested_url=result.requested_url,
            final_url=result.final_url,
            status_code=result.status_code,
        )
        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=payload,
            response_bytes=result.response_bytes,
            # Compact only — never HTML or full bodies.
            detail_json=None,
        )

    return execute


def _payload_from_extracted(
    extracted: ExtractedArticle,
    *,
    requested_url: str,
    final_url: str | None,
    status_code: int | None,
) -> _FetchArticlePayload:
    quality = extracted.quality
    if quality is ExtractionQuality.EMPTY:
        access_kind: Literal["full", "partial", "snippets"] = "snippets"
        quality_value: str | None = "empty"
        blocks: tuple[_CleanBlock, ...] = ()
    elif quality is ExtractionQuality.PARTIAL:
        access_kind = "partial"
        quality_value = "partial"
        blocks = tuple(_CleanBlock(id=b.id, text=b.text) for b in extracted.blocks)
    else:
        access_kind = "full"
        quality_value = "full"
        blocks = tuple(_CleanBlock(id=b.id, text=b.text) for b in extracted.blocks)
    return _FetchArticlePayload(
        kind="extracted",
        requested_url=requested_url,
        final_url=final_url,
        status_code=status_code,
        access_kind=access_kind,
        title=extracted.title,
        dek=extracted.dek,
        byline=extracted.byline,
        published_at=extracted.published_at,
        editorial_labels=tuple(extracted.editorial_labels),
        blocks=blocks,
        extraction_quality=quality_value,
        access_denied_kind=None,
        detail=None,
    )


def _persist_fetch_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        if not isinstance(outcome.payload, _FetchArticlePayload):
            raise RuntimeError(
                f"unexpected fetch payload type: {type(outcome.payload).__name__}"
            )
        payload = outcome.payload
        target_id = work_item.subject_id
        if target_id is None:
            raise RuntimeError("fetch work item missing subject_id")
        target = load_coverage_article_target(connection, target_id=int(target_id))
        if target is None or target.status != "pending":
            return
        plan = load_plan(connection, plan_id=target.plan_id)
        if plan is None or plan.status == "superseded":
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)

        # Idempotent: one view per external attempt.
        existing = connection.execute(
            "SELECT id FROM article_view WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        if existing is not None:
            maybe_advance_coverage_plan(
                connection,
                plan_id=plan.id,
                run_id=run_id,
                config=config,
                policy=policy,
                now=observed_at,
            )
            return

        person_article_id = upsert_person_article(
            connection,
            person_id=plan.person_id,
            canonical_article_id=target.canonical_article_id,
            first_plan_id=plan.id,
        )

        title, snippets = _title_and_snippets_for_article(
            connection,
            plan_id=plan.id,
            canonical_article_id=target.canonical_article_id,
        )
        if payload.access_kind in {"full", "partial"}:
            view_title = payload.title if payload.title else title
            view_dek = payload.dek
            view_byline = payload.byline
            view_published = payload.published_at
            labels_json = _canonical_json(list(payload.editorial_labels))
            blocks_json = _canonical_json(
                [{"id": b.id, "text": b.text} for b in payload.blocks]
            )
            snippets_json = _canonical_json(list(snippets))
            extraction_quality = payload.extraction_quality
            target_status = "fetched"
            failure_category = None
        else:
            # Snippets path (K28): access denied or empty extract.
            view_title = payload.title if payload.title else title
            view_dek = payload.dek
            view_byline = payload.byline
            view_published = payload.published_at
            labels_json = _canonical_json(list(payload.editorial_labels))
            blocks_json = "[]"
            snippets_json = _canonical_json(list(snippets))
            extraction_quality = (
                payload.extraction_quality if payload.kind == "extracted" else None
            )
            target_status = "snippets_only"
            failure_category = (
                payload.access_denied_kind
                if payload.kind == "access_denied"
                else "empty_extract"
            )

        view_id = insert_article_view(
            connection,
            canonical_article_id=target.canonical_article_id,
            run_id=run_id,
            attempt_id=attempt_id,
            access_kind=payload.access_kind,
            requested_url=payload.requested_url,
            final_url=payload.final_url,
            title=view_title,
            dek=view_dek,
            byline=view_byline,
            published_at=view_published,
            editorial_labels_json=labels_json,
            main_text_blocks_json=blocks_json,
            snippets_json=snippets_json,
            extraction_quality=extraction_quality,
            extractor_version=EXTRACTOR_VERSION,
            observed_at=observed_at,
        )
        update_coverage_article_target(
            connection,
            target_id=target.id,
            status=target_status,
            article_view_id=view_id,
            attempt_id=attempt_id,
            failure_category=failure_category,
        )

        if payload.final_url and payload.final_url != target.request_url:
            record_alias(
                connection,
                canonical_article_id=target.canonical_article_id,
                url=payload.final_url,
                kind="redirect_destination",
                now=observed_at,
            )

        schedule_assess_article(
            connection,
            person_article_id=person_article_id,
            article_view_id=view_id,
            material_fingerprint=plan.material_fingerprint,
            run_id=run_id,
            now=observed_at,
            config=config,
        )
        maybe_advance_coverage_plan(
            connection,
            plan_id=plan.id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=observed_at,
        )

    return persist


def _persist_fetch_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        state_row = connection.execute(
            "SELECT state, subject_id FROM work_item WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        if state_row is None or state_row["state"] != "failed_permanent":
            return
        target_id = state_row["subject_id"]
        if target_id is None:
            return
        target = load_coverage_article_target(connection, target_id=int(target_id))
        if target is None or target.status != "pending":
            return
        plan = load_plan(connection, plan_id=target.plan_id)
        if plan is None or plan.status == "superseded":
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        person_article_id = upsert_person_article(
            connection,
            person_id=plan.person_id,
            canonical_article_id=target.canonical_article_id,
            first_plan_id=plan.id,
        )
        title, snippets = _title_and_snippets_for_article(
            connection,
            plan_id=plan.id,
            canonical_article_id=target.canonical_article_id,
        )

        # K28: still materialize a snippets view when search/discovery text exists.
        view_id: int | None = None
        if title is not None or snippets:
            existing = connection.execute(
                "SELECT id FROM article_view WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
            if existing is not None:
                view_id = int(existing["id"])
            else:
                view_id = insert_article_view(
                    connection,
                    canonical_article_id=target.canonical_article_id,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    access_kind="snippets",
                    requested_url=target.request_url,
                    final_url=None,
                    title=title,
                    dek=None,
                    byline=None,
                    published_at=None,
                    editorial_labels_json="[]",
                    main_text_blocks_json="[]",
                    snippets_json=_canonical_json(list(snippets)),
                    extraction_quality=None,
                    extractor_version=EXTRACTOR_VERSION,
                    observed_at=observed_at,
                )

        if view_id is not None:
            update_coverage_article_target(
                connection,
                target_id=target.id,
                status="snippets_only",
                article_view_id=view_id,
                attempt_id=attempt_id,
                failure_category=str(failure.category),
            )
            schedule_assess_article(
                connection,
                person_article_id=person_article_id,
                article_view_id=view_id,
                material_fingerprint=plan.material_fingerprint,
                run_id=run_id,
                now=observed_at,
                config=config,
            )
        else:
            update_coverage_article_target(
                connection,
                target_id=target.id,
                status="failed",
                attempt_id=attempt_id,
                failure_category=str(failure.category),
            )

        maybe_advance_coverage_plan(
            connection,
            plan_id=plan.id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=observed_at,
        )

    return persist_failure


def _title_and_snippets_for_article(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    canonical_article_id: int,
) -> tuple[str | None, list[str]]:
    """Collect search/feed title and snippets for K28 snippets-only views."""
    title: str | None = None
    snippets: list[str] = []
    seen: set[str] = set()

    rows = connection.execute(
        """
        SELECT o.title, o.snippet, o.extra_snippet
          FROM brave_search_result_occurrence AS o
          JOIN brave_search_observation AS obs
            ON obs.id = o.search_observation_id
          JOIN coverage_query_form AS f
            ON f.id = obs.query_form_id
         WHERE f.plan_id = ?
           AND o.canonical_article_id = ?
         ORDER BY f.stage, o.rank, o.id
        """,
        (plan_id, canonical_article_id),
    ).fetchall()
    for row in rows:
        if title is None and row["title"]:
            title = str(row["title"]).strip() or None
        for field in ("snippet", "extra_snippet"):
            raw = row[field]
            if not raw:
                continue
            text = str(raw).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            snippets.append(text)

    discovery = connection.execute(
        """
        SELECT si.title_text, si.summary_text
          FROM coverage_discovery_article AS d
          JOIN source_item AS si ON si.id = d.source_item_id
         WHERE d.plan_id = ?
           AND d.canonical_article_id = ?
         LIMIT 1
        """,
        (plan_id, canonical_article_id),
    ).fetchone()
    if discovery is not None:
        if title is None and discovery["title_text"]:
            title = str(discovery["title_text"]).strip() or None
        summary = discovery["summary_text"]
        if summary:
            text = str(summary).strip()
            if text and text not in seen:
                seen.add(text)
                snippets.append(text)

    return title, snippets


def _screen_search_results(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    policy: SourcePolicy,
    results: Sequence[Any],
    now: str,
) -> list[dict[str, Any]]:
    """Canonicalize + screen each Brave result URL (K8). Uses shared canonicalize."""
    out: list[dict[str, Any]] = []
    for result in results:
        url = str(result.url)
        decision = screen_url(policy, url=url)
        if decision.rule_status == "unusable" or decision.canonical_url is None:
            screening_id = insert_source_screening(
                connection,
                canonical_article_id=None,
                url=url,
                publisher_key=None,
                rule_id=decision.rule_id,
                rule_status="unusable",
                source_policy_fingerprint=policy.fingerprint,
                decided_at=now,
                plan_id=plan_id,
            )
            out.append(
                {
                    "rank": int(result.rank),
                    "url": url,
                    "title": result.title,
                    "snippet": result.snippet,
                    "extra_snippet": (
                        result.extra_snippets[0] if result.extra_snippets else None
                    ),
                    "language": result.language,
                    "provider_result_id": result.provider_result_id,
                    "canonical_article_id": None,
                    "screening_id": screening_id,
                }
            )
            continue

        article_id = upsert_article(
            connection,
            canonical_url=decision.canonical_url,
            publisher_key=publisher_key(decision.canonical_url),
            now=now,
        )
        record_alias(
            connection,
            canonical_article_id=article_id,
            url=url,
            kind="search_result",
            now=now,
        )
        screening_id = insert_source_screening(
            connection,
            canonical_article_id=article_id,
            url=url,
            publisher_key=decision.publisher_key,
            rule_id=decision.rule_id,
            rule_status=decision.rule_status,
            source_policy_fingerprint=policy.fingerprint,
            decided_at=now,
            plan_id=plan_id,
        )
        out.append(
            {
                "rank": int(result.rank),
                "url": url,
                "title": result.title,
                "snippet": result.snippet,
                "extra_snippet": (
                    result.extra_snippets[0] if result.extra_snippets else None
                ),
                "language": result.language,
                "provider_result_id": result.provider_result_id,
                "canonical_article_id": article_id,
                "screening_id": screening_id,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Stage scheduling helpers
# ---------------------------------------------------------------------------


def _insert_alias_forms_if_needed(
    connection: sqlite3.Connection,
    *,
    plan: PersonCoveragePlanRecord,
    forms: Sequence[CoverageQueryFormRecord],
    assess: AssessArticleConfig,
    run_id: int,
    now: str,
) -> bool:
    if assess.max_alias_forms <= 0:
        return False
    alias_names = _operational_alias_name_texts(connection, person_id=plan.person_id)
    if not alias_names:
        return False
    existing = {form.query_text for form in forms}
    specs = generate_alias_forms(
        alias_names,
        existing_query_texts=existing,
        max_alias_forms=assess.max_alias_forms,
    )
    if not specs:
        return False
    next_ordinal = max((form.ordinal for form in forms), default=0) + 1
    form_ids = insert_query_forms(
        connection,
        plan_id=plan.id,
        forms=tuple(
            {
                "ordinal": next_ordinal + index,
                "stage": 2,
                "variant_kind": spec.variant_kind,
                "query_text": spec.query_text,
            }
            for index, spec in enumerate(specs)
        ),
    )
    for form_id in form_ids:
        schedule_brave_web_search(
            connection,
            form_id=form_id,
            material_fingerprint=plan.material_fingerprint,
            offset_in=0,
            run_id=run_id,
            now=now,
        )
    return True


def _insert_context_form_if_needed(
    connection: sqlite3.Connection,
    *,
    plan: PersonCoveragePlanRecord,
    forms: Sequence[CoverageQueryFormRecord],
    assess: AssessArticleConfig,
    run_id: int,
    now: str,
) -> bool:
    if assess.max_context_forms <= 0:
        return False
    try:
        display = select_display_name(connection, plan.person_id)
    except LookupError:
        display = _operational_name_texts(connection, person_id=plan.person_id)
        display = display[0] if display else ""
    terms = _operational_context_terms(connection, person_id=plan.person_id)
    existing = {form.query_text for form in forms}
    spec = generate_context_form(display, terms, existing_query_texts=existing)
    if spec is None:
        return False
    next_ordinal = max((form.ordinal for form in forms), default=0) + 1
    form_id = insert_query_forms(
        connection,
        plan_id=plan.id,
        forms=(
            {
                "ordinal": next_ordinal,
                "stage": 3,
                "variant_kind": spec.variant_kind,
                "query_text": spec.query_text,
            },
        ),
    )[0]
    schedule_brave_web_search(
        connection,
        form_id=form_id,
        material_fingerprint=plan.material_fingerprint,
        offset_in=0,
        run_id=run_id,
        now=now,
    )
    return True


def _apply_final_selection(
    connection: sqlite3.Connection,
    *,
    plan: PersonCoveragePlanRecord,
    assess: AssessArticleConfig,
    policy: SourcePolicy,
    run_id: int,
    now: str,
) -> None:
    del policy  # screening already durable; candidates load from rows
    candidates = _selection_candidates(connection, plan_id=plan.id)
    selected = final_selection(
        candidates,
        retrieval_target=plan.retrieval_target,
        max_eligible_fetches=assess.max_eligible_fetches,
        max_unclassified_fetches=assess.max_unclassified_fetches,
    )
    eligible = eligible_selected_count(
        candidates, max_eligible_fetches=assess.max_eligible_fetches
    )
    if not selected:
        update_plan_status(
            connection,
            plan_id=plan.id,
            status=plan.status,
            eligible_selected_count=eligible,
        )
        return

    for article in selected:
        upsert_person_article(
            connection,
            person_id=plan.person_id,
            canonical_article_id=article.canonical_article_id,
            first_plan_id=plan.id,
        )
        existing = connection.execute(
            """
            SELECT id FROM coverage_article_target
             WHERE plan_id = ? AND canonical_article_id = ?
            """,
            (plan.id, article.canonical_article_id),
        ).fetchone()
        if existing is not None:
            continue
        target_id = insert_coverage_article_target(
            connection,
            plan_id=plan.id,
            canonical_article_id=article.canonical_article_id,
            request_url=article.request_url,
            selection_reason=article.selection_reason,
            status="pending",
        )
        schedule_fetch_article(
            connection,
            target_id=target_id,
            material_fingerprint=plan.material_fingerprint,
            run_id=run_id,
            now=now,
        )
    update_plan_status(
        connection,
        plan_id=plan.id,
        status="selecting",
        eligible_selected_count=eligible,
    )


# ---------------------------------------------------------------------------
# Terminal truth table (T1–T11)
# ---------------------------------------------------------------------------


def _terminalize_plan(
    connection: sqlite3.Connection,
    *,
    plan: PersonCoveragePlanRecord,
    forms: Sequence[CoverageQueryFormRecord],
    targets: Sequence[Any],
    now: str,
) -> None:
    assessments = _assessment_stats(connection, plan_id=plan.id)
    completed_assess = assessments["completed"]
    failed_assess = assessments["failed"]
    any_assess = completed_assess + failed_assess > 0

    usable_search = _usable_search_occurrence_count(connection, plan_id=plan.id) > 0
    selected_count = len(targets)

    # Required scheduled = all forms that were inserted (conditional stages that
    # never opened are not required).
    scheduled = list(forms)
    stage1 = [f for f in forms if f.stage == 1]
    any_form_failed = any(f.status == "failed" for f in scheduled)
    all_scheduled_succeeded = bool(scheduled) and all(
        f.status == "completed" for f in scheduled
    )
    # Empty plan with zero forms (no names): treat as succeeded-empty for T1-ish.
    no_forms = not scheduled
    all_stage1_failed = bool(stage1) and all(f.status == "failed" for f in stage1)
    some_stage1_failed = any(f.status == "failed" for f in stage1)
    some_form_completed = any(f.status == "completed" for f in scheduled)

    truncated_unsafe = plan.truncated_unsafe
    partial_retrieval = plan.partial_retrieval

    # Evaluation order: T9 superseded (already returned); T3/T4 truncated;
    # T10 discovery salvage; T5/T8 partial success; T6/T7 death; T11 mixed empty;
    # T1/T2 complete-safe.

    # T3 / T4
    if truncated_unsafe:
        if completed_assess >= 1:
            # T4
            update_plan_status(
                connection,
                plan_id=plan.id,
                status="completed",
                completed_at=now,
                failure_category=None,
                truncated_unsafe=True,
                partial_retrieval=partial_retrieval,
            )
            return
        # T3
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="incomplete",
            completed_at=now,
            failure_category=_UNSAFE_TRUNCATION_CATEGORY,
            truncated_unsafe=True,
            partial_retrieval=partial_retrieval,
        )
        return

    # Selected paths must already be ready (caller). ``fetched`` without an
    # assessment never reaches here.

    # T10: no usable search occurrences; discovery/unclassified path terminal
    if (
        not usable_search
        and selected_count >= 1
        and all(t.status != "pending" for t in targets)
        and (
            any_assess or all(t.status in {"failed", "snippets_only"} for t in targets)
        )
    ):
        # Discovery-only salvage (or unclassified-only salvage) completes with
        # partial_retrieval — evidence may exist; not empty-complete.
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="completed",
            completed_at=now,
            failure_category=None,
            partial_retrieval=True,
        )
        return

    # T5 / T8: partial form failure with selections and terminal assess paths
    # (or permanent fetch death without assess: failed/snippets_only).
    if (
        any_form_failed
        and selected_count >= 1
        and (
            any_assess or all(t.status in {"failed", "snippets_only"} for t in targets)
        )
    ):
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="completed",
            completed_at=now,
            failure_category=None,
            partial_retrieval=True,
        )
        return

    # T6 / T7: total stage-1 death, zero discovery salvage
    if all_stage1_failed and selected_count == 0 and not usable_search:
        # T7 if no successful observation at all; T6 otherwise — same terminal.
        failure_cat = (
            _worst_form_failure_category(forms) or _PERMANENT_PROVIDER_CATEGORY
        )
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="failed",
            completed_at=now,
            failure_category=failure_cat,
            partial_retrieval=True,
        )
        return

    # T11: mixed form failure + empty selection
    if (
        some_stage1_failed
        and some_form_completed
        and not truncated_unsafe
        and not usable_search
        and selected_count == 0
    ):
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="failed",
            completed_at=now,
            failure_category=_PARTIAL_RETRIEVAL_EMPTY_CATEGORY,
            partial_retrieval=True,
        )
        return

    # Also T11 when any required scheduled form failed + another completed + empty
    if (
        any_form_failed
        and some_form_completed
        and selected_count == 0
        and not usable_search
        and not truncated_unsafe
    ):
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="failed",
            completed_at=now,
            failure_category=_PARTIAL_RETRIEVAL_EMPTY_CATEGORY,
            partial_retrieval=True,
        )
        return

    # T1 / T2
    complete_safe = (
        (all_scheduled_succeeded or no_forms)
        and not truncated_unsafe
        and not any_form_failed
    )
    if complete_safe:
        if selected_count == 0 and not any_assess:
            # T1 empty complete
            update_plan_status(
                connection,
                plan_id=plan.id,
                status="completed",
                completed_at=now,
                failure_category=None,
                partial_retrieval=False,
                truncated_unsafe=False,
            )
            return
        # T2: selected paths finished with assessments (caller enforced readiness).
        if selected_count >= 1 and any_assess:
            update_plan_status(
                connection,
                plan_id=plan.id,
                status="completed",
                completed_at=now,
                failure_category=None,
            )
            return
        # Selected but no assessments yet (e.g. only failed targets without
        # assess rows): still complete when every path is permanent fetch death.
        if selected_count >= 1 and all(
            t.status in {"failed", "snippets_only"} for t in targets
        ):
            update_plan_status(
                connection,
                plan_id=plan.id,
                status="completed",
                completed_at=now,
                failure_category=None,
                partial_retrieval=True,
            )
            return
        return

    # Fallback: prefer failed if any form failed, else completed/incomplete.
    if any_form_failed and selected_count == 0:
        update_plan_status(
            connection,
            plan_id=plan.id,
            status="failed",
            completed_at=now,
            failure_category=(
                _worst_form_failure_category(forms) or _PERMANENT_PROVIDER_CATEGORY
            ),
            partial_retrieval=True,
        )
        return

    update_plan_status(
        connection,
        plan_id=plan.id,
        status="completed",
        completed_at=now,
        failure_category=None,
        partial_retrieval=partial_retrieval or any_form_failed,
    )


# ---------------------------------------------------------------------------
# Selection / candidate assembly
# ---------------------------------------------------------------------------


def _selection_candidates(
    connection: sqlite3.Connection, *, plan_id: int
) -> list[SelectionCandidate]:
    """Build selection candidates from discovery + search occurrences."""
    by_article: dict[int, SelectionCandidate] = {}

    for discovery in list_coverage_discovery_articles_for_plan(
        connection, plan_id=plan_id
    ):
        screening = load_source_screening(
            connection, screening_id=discovery.screening_id
        )
        if screening is None:
            continue
        article_row = connection.execute(
            "SELECT canonical_url FROM canonical_article WHERE id = ?",
            (discovery.canonical_article_id,),
        ).fetchone()
        request_url = (
            str(article_row["canonical_url"])
            if article_row is not None
            else screening.url
        )
        by_article[discovery.canonical_article_id] = SelectionCandidate(
            canonical_article_id=discovery.canonical_article_id,
            rule_status=screening.rule_status,
            from_discovery=True,
            stage_ordinal=0,
            rank=0,
            request_url=request_url,
        )

    rows = connection.execute(
        """
        SELECT o.canonical_article_id,
               o.rank,
               o.url,
               s.rule_status,
               f.stage,
               f.ordinal AS form_ordinal
          FROM brave_search_result_occurrence AS o
          JOIN brave_search_observation AS obs
            ON obs.id = o.search_observation_id
          JOIN coverage_query_form AS f
            ON f.id = obs.query_form_id
          JOIN source_screening AS s
            ON s.id = o.screening_id
         WHERE f.plan_id = ?
           AND o.canonical_article_id IS NOT NULL
         ORDER BY f.stage, o.rank, o.canonical_article_id
        """,
        (plan_id,),
    ).fetchall()
    for row in rows:
        article_id = int(row["canonical_article_id"])
        existing = by_article.get(article_id)
        if existing is not None and existing.from_discovery:
            # Prefer discovery for selection_reason; keep discovery ordering.
            continue
        stage = int(row["stage"])
        candidate = SelectionCandidate(
            canonical_article_id=article_id,
            rule_status=str(row["rule_status"]),
            from_discovery=False,
            stage_ordinal=stage,
            rank=int(row["rank"]),
            request_url=str(row["url"]),
        )
        prior = by_article.get(article_id)
        if prior is None or (stage, int(row["rank"])) < (
            prior.stage_ordinal,
            prior.rank,
        ):
            by_article[article_id] = candidate

    return list(by_article.values())


def _recompute_eligible_selected_count(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    max_eligible_fetches: int,
) -> int:
    candidates = _selection_candidates(connection, plan_id=plan_id)
    return eligible_selected_count(
        candidates, max_eligible_fetches=max_eligible_fetches
    )


# ---------------------------------------------------------------------------
# Operational projection helpers
# ---------------------------------------------------------------------------


def _discovery_mention_urls(
    connection: sqlite3.Connection, *, person_id: int
) -> list[tuple[int, int, str | None]]:
    """(mention_id, source_item_id, original_url) for operational mentions."""
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return []
    placeholders = ",".join("?" for _ in closure)
    rows = connection.execute(
        f"""
        SELECT m.id AS mention_id,
               t.source_item_id AS source_item_id,
               si.original_url
          FROM person_mention AS m
          JOIN triage_observation AS t ON t.id = m.triage_observation_id
          JOIN source_item AS si ON si.id = t.source_item_id
         WHERE m.person_id IN ({placeholders})
         ORDER BY m.id
        """,
        closure,
    ).fetchall()
    return [
        (
            int(row["mention_id"]),
            int(row["source_item_id"]),
            str(row["original_url"]) if row["original_url"] is not None else None,
        )
        for row in rows
    ]


def _operational_name_texts(
    connection: sqlite3.Connection, *, person_id: int
) -> list[str]:
    """Stage-1 exact-name inputs: non-alias operational sourced names (K6).

    Alias kinds are reserved for stage-2 ``generate_alias_forms`` so they are
    only scheduled when exact (+ discovery) remains under the retrieval target.
    """
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return []
    placeholders = ",".join("?" for _ in closure)
    rows = connection.execute(
        f"""
        SELECT id, exact_name, match_key, kind
          FROM sourced_name
         WHERE person_id IN ({placeholders})
           AND length(match_key) > 0
           AND kind != 'alias'
         ORDER BY id
        """,
        closure,
    ).fetchall()
    sorted_rows = sorted(
        rows,
        key=lambda row: (
            _NAME_KIND_ORDER.get(str(row["kind"]), 99),
            int(row["id"]),
        ),
    )
    texts: list[str] = []
    seen: set[str] = set()
    for row in sorted_rows:
        exact = str(row["exact_name"])
        if not exact or exact in seen:
            continue
        seen.add(exact)
        texts.append(exact)
    return texts


def _operational_alias_name_texts(
    connection: sqlite3.Connection, *, person_id: int
) -> list[str]:
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return []
    placeholders = ",".join("?" for _ in closure)
    rows = connection.execute(
        f"""
        SELECT id, exact_name
          FROM sourced_name
         WHERE person_id IN ({placeholders})
           AND kind = 'alias'
           AND length(match_key) > 0
         ORDER BY id
        """,
        closure,
    ).fetchall()
    texts: list[str] = []
    seen: set[str] = set()
    for row in rows:
        exact = str(row["exact_name"])
        if not exact or exact in seen:
            continue
        seen.add(exact)
        texts.append(exact)
    return texts


def _operational_context_terms(
    connection: sqlite3.Connection, *, person_id: int
) -> list[str]:
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return []
    placeholders = ",".join("?" for _ in closure)
    rows = connection.execute(
        f"""
        SELECT f.kind, f.value
          FROM person_mention AS m
          JOIN mention_identity_fact AS f ON f.person_mention_id = m.id
         WHERE m.person_id IN ({placeholders})
           AND f.kind IN ({",".join("?" for _ in _CONTEXT_FACT_KINDS)})
         ORDER BY m.id, f.id
        """,
        (*closure, *_CONTEXT_FACT_KINDS),
    ).fetchall()
    terms: list[str] = []
    seen: set[str] = set()
    for row in rows:
        value = str(row["value"]).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        terms.append(value)
    return terms


def _death_supported(connection: sqlite3.Connection, *, person_id: int) -> bool:
    """True only when operational facts explicitly support death/obituary."""
    closure = person_id_closure_for_canonical(connection, person_id)
    if not closure:
        return False
    placeholders = ",".join("?" for _ in closure)
    rows = connection.execute(
        f"""
        SELECT f.value
          FROM person_mention AS m
          JOIN mention_identity_fact AS f ON f.person_mention_id = m.id
         WHERE m.person_id IN ({placeholders})
        """,
        closure,
    ).fetchall()
    return any(_DEATH_FACT_PATTERN.search(str(row["value"])) for row in rows)


# ---------------------------------------------------------------------------
# Plan / form stats
# ---------------------------------------------------------------------------


def _stage_terminal(forms: Sequence[CoverageQueryFormRecord], *, stage: int) -> bool:
    stage_forms = [f for f in forms if f.stage == stage]
    if not stage_forms:
        # Stage never opened: only "terminal" for stage 1 if truly none exist;
        # for gating alias after stage 1, empty stage 1 means nothing to wait on.
        return True
    return all(f.status in {"completed", "failed"} for f in stage_forms)


def _usable_search_occurrence_count(
    connection: sqlite3.Connection, *, plan_id: int
) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM brave_search_result_occurrence AS o
          JOIN brave_search_observation AS obs
            ON obs.id = o.search_observation_id
          JOIN coverage_query_form AS f ON f.id = obs.query_form_id
         WHERE f.plan_id = ?
           AND o.canonical_article_id IS NOT NULL
        """,
        (plan_id,),
    ).fetchone()
    return int(row["n"]) if row is not None else 0


def _assessment_stats(
    connection: sqlite3.Connection, *, plan_id: int
) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT disposition, COUNT(*) AS n
          FROM person_article_assessment
         WHERE plan_id = ?
         GROUP BY disposition
        """,
        (plan_id,),
    ).fetchall()
    stats = {"completed": 0, "failed": 0}
    for row in rows:
        disposition = str(row["disposition"])
        if disposition in stats:
            stats[disposition] = int(row["n"])
    return stats


def _target_has_terminal_assessment(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    person_id: int,
    canonical_article_id: int,
) -> bool:
    """True when a completed/failed assessment exists for this person–article."""
    row = connection.execute(
        """
        SELECT 1
          FROM person_article_assessment AS a
          JOIN person_article AS pa ON pa.id = a.person_article_id
         WHERE a.plan_id = ?
           AND pa.person_id = ?
           AND pa.canonical_article_id = ?
           AND a.disposition IN ('completed', 'failed')
         LIMIT 1
        """,
        (plan_id, person_id, canonical_article_id),
    ).fetchone()
    return row is not None


def _selected_paths_ready_to_terminalize(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    person_id: int,
    targets: Sequence[Any],
) -> bool:
    """Design steps 9–11: every selected path is assess-terminal or fetch-dead.

    - ``pending``: not ready (caller should have returned earlier).
    - ``fetched``: requires completed/failed assessment (view alone is not enough).
    - ``failed`` / ``snippets_only``: interim permanent fetch death without assess
      (allowed so T10 salvage can close before Task 7 wires snippets→assess).
    - other non-pending: require terminal assessment.
    """
    if not targets:
        return True
    for target in targets:
        status = str(target.status)
        if status == "pending":
            return False
        if status in {"failed", "snippets_only", "superseded"}:
            continue
        if status == "fetched":
            if not _target_has_terminal_assessment(
                connection,
                plan_id=plan_id,
                person_id=person_id,
                canonical_article_id=int(target.canonical_article_id),
            ):
                return False
            continue
        # Unknown / future statuses: require assessment.
        if not _target_has_terminal_assessment(
            connection,
            plan_id=plan_id,
            person_id=person_id,
            canonical_article_id=int(target.canonical_article_id),
        ):
            return False
    return True


def _has_active_assess_work(connection: sqlite3.Connection, *, plan_id: int) -> bool:
    """True when assess work still queues on this plan (design step 10).

    Only ``pending``/``deferred`` block terminalize. The item under settlement
    remains ``running`` through ``persist`` (engine settles state after), so
    including ``running`` would wedge the last assess path forever.
    """
    # Assess subjects are person_article; join via targets on this plan.
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM work_item AS w
          JOIN person_article AS pa ON pa.id = w.subject_id
          JOIN coverage_article_target AS t
            ON t.canonical_article_id = pa.canonical_article_id
           AND t.plan_id = ?
         WHERE w.task_type = ?
           AND w.subject_kind = ?
           AND w.state IN ('pending', 'deferred')
        """,
        (plan_id, ASSESS_ARTICLE_TASK_TYPE, SUBJECT_KIND_PERSON_ARTICLE),
    ).fetchone()
    return bool(row and int(row["n"]) > 0)


def _worst_form_failure_category(
    forms: Sequence[CoverageQueryFormRecord],
) -> str | None:
    for form in forms:
        if form.status == "failed" and form.failure_category:
            return form.failure_category
    return None


def _resolve_offset_in(
    connection: sqlite3.Connection,
    *,
    fingerprint: str,
    material_fingerprint: str,
    query_form_id: int,
    search_count: int,
) -> int:
    """Derive offset_in from the work fingerprint (0, count, 2*count, ...)."""
    del connection
    # Prefer exact match against known offsets for the form fingerprint family.
    for page_index in range(0, 16):
        offset = page_index * search_count
        if (
            brave_web_search_fingerprint(
                material_fingerprint=material_fingerprint,
                query_form_id=query_form_id,
                offset_in=offset,
            )
            == fingerprint
        ):
            return offset
    return 0


def _attempt_context(
    connection: sqlite3.Connection, work_item_id: int
) -> tuple[int, int, str]:
    row = connection.execute(
        """
        SELECT id, run_id, started_at
          FROM attempt
         WHERE work_item_id = ?
         ORDER BY id DESC
         LIMIT 1
        """,
        (work_item_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"no attempt row for work item {work_item_id}; cannot persist"
        )
    return int(row["id"]), int(row["run_id"]), str(row["started_at"])


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# assess_article handler (K20/K22/K25)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _AssessCall:
    request: StructuredGenerationRequest
    assess_input: AssessArticleInput
    person_article_id: int
    person_id: int
    canonical_article_id: int
    plan_id: int | None
    article_view_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    screening_rule_id: str
    screening_rule_status: str
    source_policy_fingerprint: str


@dataclass(frozen=True, slots=True)
class _AssessPersist:
    output: AssessArticleOutput
    person_article_id: int
    person_id: int
    canonical_article_id: int
    plan_id: int | None
    article_view_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    screening_rule_id: str
    screening_rule_status: str
    source_policy_fingerprint: str


def build_assess_article_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> TaskHandler:
    """The ``assess_article`` handler: one structured generation (Task 4)."""
    assess_config = config.tasks.assess_article
    model_id = assess_config.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname
    parameters = assess_config.parameters

    def ready(claimed_run_id: int) -> bool:
        return inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=model_id,
        )

    def prepare(work_item: WorkItem) -> TaskPreparation:
        if work_item.subject_id is None:
            raise ValueError(MISSING_PERSON_ARTICLE_DETAIL)
        person_article_id = int(work_item.subject_id)
        task_fingerprint = work_item.fingerprint
        run_id = _claimed_run_id(connection, work_item.id)

        existing = load_person_article_assessment_by_fingerprint(
            connection,
            person_article_id=person_article_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            _prepare_reuse_existing_assessment(
                connection,
                person_article_id=person_article_id,
                existing_id=existing.id,
                disposition=existing.disposition,
                plan_id=existing.plan_id,
                work_item_id=work_item.id,
            )
            raise ValueError(
                f"{ASSESS_PREPARE_REFUSED_PREFIX}already_settled "
                f"person_article {person_article_id}"
            )

        person_article = load_person_article(
            connection, person_article_id=person_article_id
        )
        if person_article is None:
            raise ValueError(MISSING_PERSON_ARTICLE_DETAIL)

        context = resolve_assess_work_context(
            connection,
            person_article_id=person_article_id,
            task_fingerprint=task_fingerprint,
        )
        if context is None:
            raise ValueError(
                f"{ASSESS_PREPARE_REFUSED_PREFIX}unresolved_context "
                f"person_article {person_article_id}"
            )
        if context.plan_status == "superseded":
            raise ValueError(
                f"{ASSESS_PREPARE_REFUSED_PREFIX}superseded_plan "
                f"person_article {person_article_id}"
            )

        view = load_article_view(connection, view_id=context.article_view_id)
        if view is None:
            raise ValueError(
                f"{ASSESS_PREPARE_REFUSED_PREFIX}missing_view "
                f"article_view {context.article_view_id}"
            )

        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=model_id,
            routing_fingerprint=routing_fp,
        )
        if inspection is None or inspection.compatibility != "compatible":
            raise ValueError(MISSING_INSPECTION_DETAIL)

        display_name, sourced_names, identity_facts, person_name_texts = (
            _operational_assess_material(connection, person_id=person_article.person_id)
        )
        article_view_like = _article_view_like(view)
        passage_view = select_passages(
            person_name_texts,
            article_view_like,
            assess_config,
        )
        access_kind = view.access_kind
        if access_kind not in {"full", "partial", "snippets"}:
            access_kind = "snippets"
        domain_profile = _domain_profile_evidence(profile)
        assess_input = build_assess_input(
            person_id=person_article.person_id,
            display_name=display_name,
            sourced_names=sourced_names,
            identity_facts=identity_facts,
            person_article_id=person_article_id,
            canonical_article_id=person_article.canonical_article_id,
            article_view_id=view.id,
            screening_rule_id=context.screening_rule_id,
            screening_rule_status=context.screening_rule_status,
            title=view.title,
            dek=view.dek,
            byline=view.byline,
            published_at=view.published_at,
            editorial_labels=_parse_string_list(view.editorial_labels_json),
            passage_view=passage_view,
            access_kind=access_kind,  # type: ignore[arg-type]
            extraction_quality=view.extraction_quality,
            domain_profile=domain_profile,
            config=assess_config,
        )
        rendered = render_assess_request(assess_input)
        request = StructuredGenerationRequest(
            model_id=model_id,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name=ASSESS_SCHEMA_NAME,
            max_completion_tokens=assess_config.max_completion_tokens,
            temperature=parameters.temperature,
            top_p=parameters.top_p,
            reasoning_effort=parameters.reasoning_effort,
        )
        call = _AssessCall(
            request=request,
            assess_input=assess_input,
            person_article_id=person_article_id,
            person_id=person_article.person_id,
            canonical_article_id=person_article.canonical_article_id,
            plan_id=context.plan_id,
            article_view_id=view.id,
            model_inspection_id=inspection.id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=rendered.prompt_hash,
            schema_hash=rendered.schema_hash,
            schema_version=rendered.schema_version,
            task_fingerprint=task_fingerprint,
            screening_rule_id=context.screening_rule_id,
            screening_rule_status=context.screening_rule_status,
            source_policy_fingerprint=context.source_policy_fingerprint,
        )
        if not hard_budget:
            return TaskPreparation(payload=call, reserved_nano_usd=0)

        prompt_price = inspection.prompt_unit_price_nano_usd
        completion_price = inspection.completion_unit_price_nano_usd
        if (
            not inspection.pricing_usable
            or prompt_price is None
            or completion_price is None
        ):
            raise ValueError(MISSING_PRICING_DETAIL)
        reserved = _worst_case_reservation_nano_usd(
            prompt_unit_price_nano_usd=prompt_price,
            completion_unit_price_nano_usd=completion_price,
            max_input_tokens=assess_config.max_input_tokens,
            max_completion_tokens=assess_config.max_completion_tokens,
        )
        return TaskPreparation(payload=call, reserved_nano_usd=reserved)

    def destination_host(_work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=ASSESS_ARTICLE_TASK_TYPE,
        provider=OPENROUTER_PROVIDER,
        operation=GENERATE_OPERATION,
        execute=_execute_assess_for(client),
        prepare=prepare,
        persist=_persist_assess_for(connection, config=config),
        persist_failure=_persist_assess_failure_for(connection, config=config),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )


def _execute_assess_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _AssessCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=OPENROUTER_PROVIDER,
                operation=GENERATE_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.generate_structured(prepared.request)
        raw_text = result.raw_text
        if result.refusal:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=OPENROUTER_PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_ASSESS_DETAIL,
            )
        try:
            output = validate_assess_output(raw_text, prepared.assess_input)
        except AssessValidationError:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=OPENROUTER_PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_ASSESS_DETAIL,
            ) from None
        del raw_text
        payload = _AssessPersist(
            output=output,
            person_article_id=prepared.person_article_id,
            person_id=prepared.person_id,
            canonical_article_id=prepared.canonical_article_id,
            plan_id=prepared.plan_id,
            article_view_id=prepared.article_view_id,
            model_inspection_id=prepared.model_inspection_id,
            canonical_supplied_input_json=prepared.canonical_supplied_input_json,
            prompt_hash=prepared.prompt_hash,
            schema_hash=prepared.schema_hash,
            schema_version=prepared.schema_version,
            task_fingerprint=prepared.task_fingerprint,
            screening_rule_id=prepared.screening_rule_id,
            screening_rule_status=prepared.screening_rule_status,
            source_policy_fingerprint=prepared.source_policy_fingerprint,
        )
        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=payload,
            response_bytes=len(result.raw_text.encode("utf-8")),
            provider_request_id=result.provider_request_id,
            actual_nano_usd=result.actual_nano_usd,
        )

    return execute


def _persist_assess_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    del config  # reserved: full maybe_advance with SourcePolicy if needed later

    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _AssessPersist):
            raise RuntimeError(
                f"unexpected assess payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        existing = load_person_article_assessment_by_fingerprint(
            connection,
            person_article_id=payload.person_article_id,
            task_fingerprint=payload.task_fingerprint,
        )
        if existing is not None:
            if existing.disposition == "completed":
                point_person_article_current_assessment(
                    connection,
                    person_article_id=payload.person_article_id,
                    assessment_id=existing.id,
                )
                if payload.plan_id is not None:
                    advance_coverage_plan_after_assess(
                        connection,
                        plan_id=payload.plan_id,
                        now=observed_at,
                    )
            return

        if payload.plan_id is not None:
            plan = load_plan(connection, plan_id=payload.plan_id)
            if plan is not None and plan.status == "superseded":
                return

        output = payload.output
        assessment_id = insert_person_article_assessment(
            connection,
            person_article_id=payload.person_article_id,
            person_id=payload.person_id,
            canonical_article_id=payload.canonical_article_id,
            plan_id=payload.plan_id,
            article_view_id=payload.article_view_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=payload.model_inspection_id,
            disposition="completed",
            person_relation=output.person_relation,
            coverage_depth=output.coverage_depth,
            content_types_json=_canonical_json(list(output.content_types)),
            subject_relationship=output.subject_relationship,
            screening_rule_id=payload.screening_rule_id,
            screening_rule_status=payload.screening_rule_status,
            source_policy_fingerprint=payload.source_policy_fingerprint,
            canonical_supplied_input_json=payload.canonical_supplied_input_json,
            validated_output_json=output.model_dump_json(),
            prompt_hash=payload.prompt_hash,
            schema_hash=payload.schema_hash,
            schema_version=payload.schema_version,
            task_fingerprint=payload.task_fingerprint,
            rationale=output.person_relation_rationale,
            failure_category=None,
            observed_at=observed_at,
        )
        if output.signals:
            insert_assessment_signals(
                connection,
                assessment_id=assessment_id,
                signals=tuple(
                    {
                        "signal_kind": signal.kind,
                        "category": signal.category,
                        "claim": signal.claim,
                        "supporting_passage_ids_json": _canonical_json(
                            list(signal.supporting_passage_ids)
                        ),
                        "ordinal": index,
                    }
                    for index, signal in enumerate(output.signals, start=1)
                ),
            )
        # K25: current pointer only for completed assessments.
        point_person_article_current_assessment(
            connection,
            person_article_id=payload.person_article_id,
            assessment_id=assessment_id,
        )
        if payload.plan_id is not None:
            advance_coverage_plan_after_assess(
                connection,
                plan_id=payload.plan_id,
                now=observed_at,
            )

    return persist


def _persist_assess_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        state_row = connection.execute(
            "SELECT state, subject_id, fingerprint FROM work_item WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        if state_row is None or state_row["state"] != "failed_permanent":
            return
        if state_row["subject_id"] is None:
            return
        person_article_id = int(state_row["subject_id"])
        task_fingerprint = str(state_row["fingerprint"])
        existing = load_person_article_assessment_by_fingerprint(
            connection,
            person_article_id=person_article_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            return

        person_article = load_person_article(
            connection, person_article_id=person_article_id
        )
        if person_article is None:
            return
        context = resolve_assess_work_context(
            connection,
            person_article_id=person_article_id,
            task_fingerprint=task_fingerprint,
        )
        if context is None:
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        routing_fp = routing_fingerprint(config.openrouter.routing)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=config.tasks.assess_article.model,
            routing_fingerprint=routing_fp,
        )
        model_inspection_id = None if inspection is None else inspection.id
        category = (
            _INVALID_MODEL_OUTPUT_CATEGORY
            if failure.category is FailureCategory.MALFORMED_RESPONSE
            else str(failure.category)
        )
        insert_person_article_assessment(
            connection,
            person_article_id=person_article_id,
            person_id=person_article.person_id,
            canonical_article_id=person_article.canonical_article_id,
            plan_id=context.plan_id,
            article_view_id=context.article_view_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=model_inspection_id,
            disposition="failed",
            person_relation=None,
            coverage_depth=None,
            content_types_json=None,
            subject_relationship=None,
            screening_rule_id=context.screening_rule_id,
            screening_rule_status=context.screening_rule_status,
            source_policy_fingerprint=context.source_policy_fingerprint,
            canonical_supplied_input_json=ASSESS_FAILED_SUPPLIED_INPUT_JSON,
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=task_fingerprint,
            rationale=str(failure.category),
            failure_category=category,
            observed_at=observed_at,
        )
        # K25: do not move current pointer on permanent failure.
        if context.plan_id is not None:
            advance_coverage_plan_after_assess(
                connection,
                plan_id=context.plan_id,
                now=observed_at,
            )

    return persist_failure


def advance_coverage_plan_after_assess(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    now: str,
) -> None:
    """Terminalize a coverage plan when selected assess paths are settled.

    Used by assess handler persist/persist_failure and the K23 permanent
    preflight settler. Does not re-screen (no SourcePolicy); only advances
    when forms/targets/assess work are already quiescent and every selected
    path is assess-terminal or fetch-dead (T1–T11 via ``_terminalize_plan``).
    """
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None or plan.status in _TERMINAL_PLAN_STATUSES:
        return
    if _has_active_assess_work(connection, plan_id=plan_id):
        if plan.status in {"retrieving", "selecting"}:
            update_plan_status(connection, plan_id=plan_id, status="assessing")
        return
    targets = list(list_coverage_article_targets_for_plan(connection, plan_id=plan_id))
    if targets and not _selected_paths_ready_to_terminalize(
        connection, plan_id=plan_id, person_id=plan.person_id, targets=targets
    ):
        if plan.status in {"retrieving", "selecting"}:
            update_plan_status(connection, plan_id=plan_id, status="assessing")
        return
    forms = list(list_query_forms_for_plan(connection, plan_id=plan_id))
    if any(form.status == "pending" for form in forms):
        return
    if any(t.status == "pending" for t in targets):
        return
    _terminalize_plan(
        connection,
        plan=plan,
        forms=forms,
        targets=targets,
        now=now,
    )


def _prepare_reuse_existing_assessment(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    existing_id: int,
    disposition: str,
    plan_id: int | None,
    work_item_id: int,
) -> None:
    """Point current for an existing completed assessment (prepare-time)."""
    if disposition != "completed":
        return
    owns = not connection.in_transaction
    if owns:
        connection.execute("BEGIN IMMEDIATE")
    try:
        point_person_article_current_assessment(
            connection,
            person_article_id=person_article_id,
            assessment_id=existing_id,
        )
        del plan_id, work_item_id
    except BaseException:
        if owns:
            connection.rollback()
        raise
    else:
        if owns:
            connection.commit()


def _claimed_run_id(connection: sqlite3.Connection, work_item_id: int) -> int:
    row = connection.execute(
        "SELECT claimed_by_run_id FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is None or row["claimed_by_run_id"] is None:
        attempt = connection.execute(
            """
            SELECT run_id FROM attempt
             WHERE work_item_id = ?
             ORDER BY id DESC LIMIT 1
            """,
            (work_item_id,),
        ).fetchone()
        if attempt is not None:
            return int(attempt["run_id"])
        raise ValueError(f"work item {work_item_id} is not claimed by a run")
    return int(row["claimed_by_run_id"])


def _has_active_assess_article_work(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred', 'running')
         LIMIT 1
        """,
        (ASSESS_ARTICLE_TASK_TYPE,),
    ).fetchone()
    return row is not None


def _has_active_coverage_plan(connection: sqlite3.Connection) -> bool:
    """Active plan arm (retrieving | selecting | assessing). K20 cold-start."""
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM person_coverage_plan
         WHERE status IN ('retrieving', 'selecting', 'assessing')
         LIMIT 1
        """
    ).fetchone()
    return row is not None


def _has_active_coverage_http_work(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM work_item
         WHERE task_type IN (?, ?)
           AND state IN ('pending', 'deferred', 'running')
         LIMIT 1
        """,
        (BRAVE_WEB_SEARCH_TASK_TYPE, FETCH_ARTICLE_TASK_TYPE),
    ).fetchone()
    return row is not None


def _screening_for_article_on_plan(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    canonical_article_id: int,
) -> tuple[str, str, str] | None:
    """Return (rule_id, rule_status, policy_fp) for discovery or search path."""
    discovery = connection.execute(
        """
        SELECT s.rule_id, s.rule_status, s.source_policy_fingerprint
          FROM coverage_discovery_article AS d
          JOIN source_screening AS s ON s.id = d.screening_id
         WHERE d.plan_id = ? AND d.canonical_article_id = ?
         LIMIT 1
        """,
        (plan_id, canonical_article_id),
    ).fetchone()
    if discovery is not None:
        return (
            str(discovery["rule_id"]),
            str(discovery["rule_status"]),
            str(discovery["source_policy_fingerprint"]),
        )
    search = connection.execute(
        """
        SELECT s.rule_id, s.rule_status, s.source_policy_fingerprint
          FROM brave_search_result_occurrence AS o
          JOIN brave_search_observation AS obs
            ON obs.id = o.search_observation_id
          JOIN coverage_query_form AS f ON f.id = obs.query_form_id
          JOIN source_screening AS s ON s.id = o.screening_id
         WHERE f.plan_id = ?
           AND o.canonical_article_id = ?
         ORDER BY o.rank, o.id
         LIMIT 1
        """,
        (plan_id, canonical_article_id),
    ).fetchone()
    if search is not None:
        return (
            str(search["rule_id"]),
            str(search["rule_status"]),
            str(search["source_policy_fingerprint"]),
        )
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None:
        return None
    return (
        "unknown",
        "unclassified",
        plan.source_policy_fingerprint,
    )


def _operational_assess_material(
    connection: sqlite3.Connection,
    *,
    person_id: int,
) -> tuple[str, tuple[AssessName, ...], tuple[AssessFact, ...], list[str]]:
    """K27: names and facts from the operational projection for assess input."""
    canonical_id = canonical_person_id(connection, person_id)
    try:
        display_name = select_display_name(connection, canonical_id)
    except LookupError:
        row = connection.execute(
            "SELECT display_name FROM person WHERE id = ?",
            (canonical_id,),
        ).fetchone()
        if row is None:
            raise ValueError(MISSING_PERSON_ARTICLE_DETAIL) from None
        display_name = str(row["display_name"])

    closure = person_id_closure_for_canonical(connection, canonical_id)
    if not closure:
        raise ValueError(MISSING_PERSON_ARTICLE_DETAIL)
    placeholders = ",".join("?" for _ in closure)
    name_rows = connection.execute(
        f"""
        SELECT id, exact_name, match_key, kind
          FROM sourced_name
         WHERE person_id IN ({placeholders})
         ORDER BY id
        """,
        closure,
    ).fetchall()
    names: list[AssessName] = []
    name_texts: list[str] = []
    seen_keys: set[str] = set()
    sorted_names = sorted(
        name_rows,
        key=lambda row: (
            _NAME_KIND_ORDER.get(str(row["kind"]), 99),
            int(row["id"]),
        ),
    )
    for row in sorted_names:
        key = str(row["match_key"])
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        exact = str(row["exact_name"])
        names.append(
            AssessName(
                exact_name=exact,
                match_key=key,
                kind=str(row["kind"]),
            )
        )
        name_texts.append(exact)
        if len(names) >= 8:
            break

    if not names:
        names.append(
            AssessName(
                exact_name=display_name,
                match_key=display_name.casefold() or "unknown",
                kind="display",
            )
        )
        name_texts.append(display_name)

    mentions = mentions_for_canonical_person(connection, canonical_id)
    facts: list[AssessFact] = []
    fact_index = 1
    for mention in mentions:
        for fact in mention.identity_facts:
            kind_raw = str(fact.kind)
            try:
                kind_enum = IdentityFactKind(kind_raw)
            except ValueError:
                kind_enum = IdentityFactKind.OTHER
            facts.append(
                AssessFact(
                    local_id=f"f{fact_index}",
                    kind=kind_enum,
                    value=str(fact.value)[:500] or str(fact.value)[:1],
                )
            )
            fact_index += 1
            if len(facts) >= 16:
                break
        if len(facts) >= 16:
            break

    return display_name, tuple(names), tuple(facts), name_texts


def _article_view_like(view: Any) -> ArticleViewLike:
    blocks_raw = json.loads(view.main_text_blocks_json or "[]")
    blocks: list[TextBlock] = []
    if isinstance(blocks_raw, list):
        for index, item in enumerate(blocks_raw, start=1):
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if not isinstance(text, str) or not text.strip():
                continue
            block_id = item.get("id")
            if not isinstance(block_id, str) or not block_id:
                block_id = f"b{index}"
            blocks.append(TextBlock(id=block_id, text=text))
    snippets_raw = json.loads(view.snippets_json or "[]")
    snippets: list[str] = []
    if isinstance(snippets_raw, list):
        for item in snippets_raw:
            if isinstance(item, str) and item.strip():
                snippets.append(item)
    return ArticleViewLike(
        title=view.title,
        dek=view.dek,
        main_text_blocks=tuple(blocks),
        snippets=tuple(snippets),
    )


def _domain_profile_evidence(profile: DomainProfileConfig) -> DomainProfileEvidence:
    examples = tuple(
        DomainProfileEvidenceExample(
            category=AttentionCategory(category),
            examples=tuple(values),
        )
        for category, values in sorted(profile.attention_examples.items())
    )
    return DomainProfileEvidence(
        version=profile.schema_version,
        key=profile.key,
        label=profile.label,
        language=profile.language,
        attention_examples=examples,
    )


def _parse_string_list(raw: str) -> tuple[str, ...]:
    try:
        parsed = json.loads(raw or "[]")
    except json.JSONDecodeError:
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(item) for item in parsed if isinstance(item, str))
