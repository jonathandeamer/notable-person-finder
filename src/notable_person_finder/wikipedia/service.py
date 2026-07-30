"""Wikipedia identity HTTP handlers, match generation, and plan advancement.

``mediawiki_search`` and ``mediawiki_page_facts`` each perform exactly one
MediaWiki call per ``execute``. ``match_wikipedia_identity`` performs exactly
one OpenRouter ``generate_structured`` call. Workers never open SQLite. Domain
settlement and ``maybe_advance_plan`` run on the application thread inside the
engine's settlement transaction.

When match work is scheduled, ``ensure_model_inspections_for_run`` re-arms
multi-model inspection (K21b) so cold-start HTTP-only seeds unlock readiness.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

from notable_person_finder.config.models import (
    MainConfig,
    MatchWikipediaIdentityConfig,
)
from notable_person_finder.people.identity import (
    canonical_person_id,
    mentions_for_canonical_person,
    person_id_closure_for_canonical,
    select_display_name,
)
from notable_person_finder.people.models import IdentityFactKind, SourcedNameKind
from notable_person_finder.people.repository import load_model_inspection
from notable_person_finder.people.service import (
    _worst_case_reservation_nano_usd,
    ensure_model_inspections_for_run,
    inspection_ready,
    routing_fingerprint,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.mediawiki import (
    PAGE_FACTS_OPERATION,
    SEARCH_OPERATION,
    MediaWikiClient,
    MediaWikiPageFact,
    MediaWikiPageFactsBatch,
    MediaWikiSearchPage,
)
from notable_person_finder.providers.mediawiki import (
    PROVIDER as MEDIAWIKI_PROVIDER,
)
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
from notable_person_finder.wikipedia.candidates import (
    AssemblyPage,
    AssemblySearchHit,
    BiographyCandidate,
    assemble_biography_candidates,
)
from notable_person_finder.wikipedia.matching import (
    MatchValidationError,
    build_match_input,
    map_model_outcome_to_semantic,
    match_prompt_and_schema_hashes,
    render_match_request,
    validate_match_output,
)
from notable_person_finder.wikipedia.models import (
    MatchFact,
    MatchName,
    MatchWikiCandidate,
    MatchWikipediaIdentityInput,
    MatchWikipediaIdentityOutput,
)
from notable_person_finder.wikipedia.queries import generate_accent_fallback_forms
from notable_person_finder.wikipedia.repository import (
    covered_fact_page_ids,
    insert_or_load_search_observation_by_attempt,
    insert_page_facts_batch,
    insert_query_forms,
    insert_search_hits,
    insert_wikipedia_identity_observation,
    list_mediawiki_pages_by_page_ids,
    list_page_facts_batches_for_plan,
    list_query_forms_for_plan,
    list_search_hit_page_ids_for_completed_forms,
    load_active_plan_for_fingerprint,
    load_page_facts_batch,
    load_plan,
    load_query_form,
    load_wikipedia_identity_observation_by_fingerprint,
    mark_batch_completed,
    mark_batch_failed,
    mark_plan_status,
    mark_query_form_completed,
    mark_query_form_failed,
    mark_query_form_progress,
    next_batch_ordinal,
    next_form_ordinal,
    point_person_current_wikipedia_observation,
    update_plan_retrieval_flags,
    upsert_mediawiki_page,
)

MEDIAWIKI_SEARCH_TASK_TYPE = "mediawiki_search"
MEDIAWIKI_PAGE_FACTS_TASK_TYPE = "mediawiki_page_facts"
MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE = "match_wikipedia_identity"

SUBJECT_KIND_WIKIPEDIA_QUERY_FORM = "wikipedia_query_form"
SUBJECT_KIND_WIKIPEDIA_PAGE_FACTS_BATCH = "wikipedia_page_facts_batch"
SUBJECT_KIND_PERSON = "person"

MEDIAWIKI_HTTP_PRIORITY = 50
MATCH_WIKIPEDIA_PRIORITY = 55

DEFAULT_WIKI_ID = "enwiki"

NO_MATCH_VALIDATED_OUTPUT_JSON = '{"outcome":"no_matching_page_found"}'
NO_MATCH_RATIONALE = "empty complete search"
EMPTY_COMPLETE_SUPPLIED_INPUT_JSON = (
    '{"task":"wikipedia_identity","path":"empty_complete_search",'
    '"candidate_page_ids":[]}'
)
LOCAL_FAILED_SUPPLIED_INPUT_JSON = (
    '{"task":"wikipedia_identity","path":"local_assembly_failed",'
    '"candidate_page_ids":[]}'
)
MATCH_FAILED_SUPPLIED_INPUT_JSON = (
    '{"task":"wikipedia_identity","path":"match_failed","candidate_page_ids":[]}'
)

MATCH_SCHEMA_NAME = "match_wikipedia_identity"
MALFORMED_MATCH_DETAIL = "invalid match output"
MISSING_PERSON_DETAIL = "person is missing"
MISSING_INSPECTION_DETAIL = "compatible model inspection is missing"
MISSING_PRICING_DETAIL = "usable unit pricing is required under a hard budget"
MATCH_PREPARE_REFUSED_PREFIX = "match_prepare_refused:"
INVALID_MODEL_OUTPUT_CATEGORY = "invalid_model_output"
PERMANENT_PREFLIGHT_CATEGORY = "permanent_preflight"

_PRIMARY_VARIANT_KINDS = frozenset({"exact", "comma_swap"})
_ACCENT_VARIANT_KIND = "accent_fallback"
_TERMINAL_PLAN_STATUSES = frozenset({"superseded", "completed", "failed"})
_NAME_KIND_ORDER: dict[str, int] = {
    "professional": 0,
    "display": 1,
    "alias": 2,
    "other": 3,
    "mononym": 4,
}


@dataclass(frozen=True, slots=True)
class _SearchCall:
    form_id: int
    plan_id: int
    query_text: str
    continuation_in: str | None
    continuations_used_before: int
    material_fingerprint: str


@dataclass(frozen=True, slots=True)
class _FactsCall:
    batch_id: int
    plan_id: int
    page_ids: tuple[int, ...]
    wave: int
    material_fingerprint: str


def mediawiki_search_fingerprint(
    *,
    material_fingerprint: str,
    query_form_id: int,
    continuation_in: str | None,
) -> str:
    """Work-item fingerprint for one search call (form + continuation step)."""
    return _sha256(
        _canonical_json(
            {
                "task": MEDIAWIKI_SEARCH_TASK_TYPE,
                "material_fingerprint": material_fingerprint,
                "query_form_id": query_form_id,
                "continuation_in": continuation_in,
            }
        )
    )


def mediawiki_page_facts_fingerprint(
    *,
    material_fingerprint: str,
    batch_id: int,
) -> str:
    """Work-item fingerprint for one facts batch."""
    return _sha256(
        _canonical_json(
            {
                "task": MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
                "material_fingerprint": material_fingerprint,
                "batch_id": batch_id,
            }
        )
    )


def match_wikipedia_identity_fingerprint(*, material_fingerprint: str) -> str:
    """Work-item fingerprint for match (person subject; material only)."""
    return material_fingerprint


def schedule_mediawiki_search(
    connection: sqlite3.Connection,
    *,
    form_id: int,
    material_fingerprint: str,
    continuation_in: str | None,
    run_id: int,
    now: str,
) -> int:
    """Schedule one ``mediawiki_search`` work item for a form continuation."""
    fingerprint = mediawiki_search_fingerprint(
        material_fingerprint=material_fingerprint,
        query_form_id=form_id,
        continuation_in=continuation_in,
    )
    return runs_repository.schedule_work(
        connection,
        task_type=MEDIAWIKI_SEARCH_TASK_TYPE,
        subject_kind=SUBJECT_KIND_WIKIPEDIA_QUERY_FORM,
        subject_id=form_id,
        fingerprint=fingerprint,
        required=True,
        priority=MEDIAWIKI_HTTP_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def schedule_mediawiki_page_facts(
    connection: sqlite3.Connection,
    *,
    batch_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
) -> int:
    fingerprint = mediawiki_page_facts_fingerprint(
        material_fingerprint=material_fingerprint,
        batch_id=batch_id,
    )
    return runs_repository.schedule_work(
        connection,
        task_type=MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
        subject_kind=SUBJECT_KIND_WIKIPEDIA_PAGE_FACTS_BATCH,
        subject_id=batch_id,
        fingerprint=fingerprint,
        required=True,
        priority=MEDIAWIKI_HTTP_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def schedule_match_wikipedia_identity(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
) -> int:
    fingerprint = match_wikipedia_identity_fingerprint(
        material_fingerprint=material_fingerprint
    )
    return runs_repository.schedule_work(
        connection,
        task_type=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
        subject_kind=SUBJECT_KIND_PERSON,
        subject_id=person_id,
        fingerprint=fingerprint,
        required=True,
        priority=MATCH_WIKIPEDIA_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def wikipedia_match_model_needed(
    connection: sqlite3.Connection, *, config: MainConfig
) -> bool:
    """K21: whether the match model needs current-run inspection.

    Arms when K17-eligible people exist, active match work exists, an active
    Wikipedia plan is retrieving/ready_for_match, or MediaWiki HTTP work for
    a Wikipedia plan is active. Full K17 eligibility lands in Task 7; until
    then the eligible arm is a thin provisional scan.
    """
    del config  # reserved for full is_wikipedia_match_eligible (Task 7)
    return (
        _has_wikipedia_match_eligible_people_provisional(connection)
        or _has_active_match_wikipedia_work(connection)
        or _has_active_wikipedia_plan(connection)
        or _has_active_mediawiki_wikipedia_http_work(connection)
    )


def _has_wikipedia_match_eligible_people_provisional(
    connection: sqlite3.Connection,
) -> bool:
    """Provisional K17 arm until Task 7 ships ``is_wikipedia_match_eligible``.

    True when a canonical person has sourced names and no current Wikipedia
    pointer and no terminal observation yet — enough for multi-model inspect
    when only a Wikipedia backlog remains before seed opens plans.
    """
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM person AS p
         WHERE p.merged_into_person_id IS NULL
           AND p.current_wikipedia_identity_observation_id IS NULL
           AND EXISTS (
                SELECT 1
                  FROM sourced_name AS sn
                 WHERE sn.person_id = p.id
                   AND length(sn.match_key) > 0
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM wikipedia_identity_observation AS o
                 WHERE o.person_id = p.id
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM wikipedia_identity_plan AS pl
                 WHERE pl.person_id = p.id
                   AND pl.status IN ('retrieving', 'ready_for_match')
           )
         LIMIT 1
        """
    ).fetchone()
    return row is not None


def _has_active_match_wikipedia_work(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred', 'running')
         LIMIT 1
        """,
        (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
    ).fetchone()
    return row is not None


def _has_active_wikipedia_plan(connection: sqlite3.Connection) -> bool:
    """Active plan arm (retrieving | ready_for_match). Mutation-sensitive (K21)."""
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM wikipedia_identity_plan
         WHERE status IN ('retrieving', 'ready_for_match')
         LIMIT 1
        """
    ).fetchone()
    return row is not None


def _has_active_mediawiki_wikipedia_http_work(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM work_item
         WHERE task_type IN (?, ?)
           AND state IN ('pending', 'deferred', 'running')
         LIMIT 1
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE, MEDIAWIKI_PAGE_FACTS_TASK_TYPE),
    ).fetchone()
    return row is not None


def maybe_advance_plan(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    run_id: int,
    config: MainConfig,
    now: str,
) -> None:
    """Join-point after form/batch settlement: facts waves, accent, assemble.

    Ordering is load-bearing (design §maybe_advance_plan steps 1–7).
    **The caller must already hold an open transaction** when settling.
    """
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None:
        return
    if plan.status in _TERMINAL_PLAN_STATUSES:
        return

    match_config = config.tasks.match_wikipedia_identity
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)

    # 2. Any form still pending → wait.
    if any(form.status == "pending" for form in forms):
        return

    # 3. Facts for known hits on completed forms (wave 1) if needed.
    if _schedule_wave1_facts_if_needed(
        connection,
        plan_id=plan_id,
        material_fingerprint=plan.material_fingerprint,
        match_config=match_config,
        run_id=run_id,
        now=now,
    ):
        return

    # 4. Any facts batch pending → wait.
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    if any(batch.status == "pending" for batch in batches):
        return

    # 5. Redirect waves (K24); budget exhaust → truncated_unsafe.
    if _schedule_redirect_wave_if_needed(
        connection,
        plan_id=plan_id,
        material_fingerprint=plan.material_fingerprint,
        match_config=match_config,
        run_id=run_id,
        now=now,
    ):
        return

    # Re-load plan flags after possible redirect-budget flag write.
    plan = load_plan(connection, plan_id=plan_id)
    if plan is None or plan.status in _TERMINAL_PLAN_STATUSES:
        return
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)

    # 6. Accent phase (K23) — primary forms terminal, primary facts quiescent.
    if _maybe_run_accent_phase(
        connection,
        plan_id=plan_id,
        forms=forms,
        batches=batches,
        material_fingerprint=plan.material_fingerprint,
        match_config=match_config,
        run_id=run_id,
        now=now,
    ):
        return

    # 7. All forms terminal + facts quiescent → assemble.
    if any(form.status == "pending" for form in forms):
        return
    if any(batch.status == "pending" for batch in batches):
        return

    _assemble_and_terminalize(
        connection,
        plan_id=plan_id,
        plan=plan,
        forms=forms,
        run_id=run_id,
        config=config,
        match_config=match_config,
        now=now,
    )


def build_mediawiki_search_handler(
    connection: sqlite3.Connection,
    *,
    client: MediaWikiClient,
    config: MainConfig,
) -> TaskHandler:
    """HTTP handler: one ``search_pages`` call per execute; no OpenRouter budget."""

    def prepare(work_item: WorkItem) -> TaskPreparation:
        form_id = work_item.subject_id
        if form_id is None:
            raise ValueError("mediawiki_search work item names no query form")
        form = load_query_form(connection, form_id=form_id)
        if form is None:
            raise ValueError(f"wikipedia_query_form {form_id} is missing")
        plan = load_plan(connection, plan_id=form.plan_id)
        if plan is None:
            raise ValueError(f"wikipedia_identity_plan {form.plan_id} is missing")
        if plan.status == "superseded":
            raise ValueError("wikipedia plan is superseded")
        if form.status != "pending":
            raise ValueError(f"wikipedia_query_form {form_id} is not pending")

        continuation_in = _resolve_continuation_in(
            connection,
            fingerprint=work_item.fingerprint,
            material_fingerprint=plan.material_fingerprint,
            query_form_id=form_id,
        )

        return TaskPreparation(
            payload=_SearchCall(
                form_id=form_id,
                plan_id=form.plan_id,
                query_text=form.query_text,
                continuation_in=continuation_in,
                continuations_used_before=form.continuations_used,
                material_fingerprint=plan.material_fingerprint,
            )
        )

    def destination_host(_work_item: WorkItem) -> str | None:
        try:
            return urlsplit(config.mediawiki.endpoint).hostname
        except Exception:
            return None

    def request_fingerprint(work_item: WorkItem) -> str:
        return work_item.fingerprint

    persist = _persist_search_for(connection, config=config)
    persist_failure = _persist_search_failure_for(connection, config=config)

    return TaskHandler(
        task_type=MEDIAWIKI_SEARCH_TASK_TYPE,
        provider=MEDIAWIKI_PROVIDER,
        operation=SEARCH_OPERATION,
        execute=_execute_search_for(client),
        request_fingerprint=request_fingerprint,
        reserved_nano_usd=0,
        prepare=prepare,
        persist=persist,
        persist_failure=persist_failure,
        destination_host=destination_host,
        pool=WorkerPool.HTTP,
    )


def build_mediawiki_page_facts_handler(
    connection: sqlite3.Connection,
    *,
    client: MediaWikiClient,
    config: MainConfig,
) -> TaskHandler:
    """HTTP handler: one ``get_page_facts`` call per execute; no OpenRouter budget."""

    def prepare(work_item: WorkItem) -> TaskPreparation:
        batch_id = work_item.subject_id
        if batch_id is None:
            raise ValueError("mediawiki_page_facts work item names no facts batch")
        batch = load_page_facts_batch(connection, batch_id=batch_id)
        if batch is None:
            raise ValueError(f"wikipedia_page_facts_batch {batch_id} is missing")
        plan = load_plan(connection, plan_id=batch.plan_id)
        if plan is None:
            raise ValueError(f"wikipedia_identity_plan {batch.plan_id} is missing")
        if plan.status == "superseded":
            raise ValueError("wikipedia plan is superseded")
        if batch.status != "pending":
            raise ValueError(f"wikipedia_page_facts_batch {batch_id} is not pending")

        page_ids = _parse_page_ids_json(batch.page_ids_json)
        return TaskPreparation(
            payload=_FactsCall(
                batch_id=batch_id,
                plan_id=batch.plan_id,
                page_ids=page_ids,
                wave=batch.wave,
                material_fingerprint=plan.material_fingerprint,
            )
        )

    def destination_host(_work_item: WorkItem) -> str | None:
        try:
            return urlsplit(config.mediawiki.endpoint).hostname
        except Exception:
            return None

    def request_fingerprint(work_item: WorkItem) -> str:
        return work_item.fingerprint

    return TaskHandler(
        task_type=MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
        provider=MEDIAWIKI_PROVIDER,
        operation=PAGE_FACTS_OPERATION,
        execute=_execute_facts_for(client),
        request_fingerprint=request_fingerprint,
        reserved_nano_usd=0,
        prepare=prepare,
        persist=_persist_facts_for(connection, config=config),
        persist_failure=_persist_facts_failure_for(connection, config=config),
        destination_host=destination_host,
        pool=WorkerPool.HTTP,
    )


# ---------------------------------------------------------------------------
# Execute factories (no SQLite in scope)
# ---------------------------------------------------------------------------


def _execute_search_for(
    client: MediaWikiClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _SearchCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=MEDIAWIKI_PROVIDER,
                operation=SEARCH_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.search_pages(
            prepared.query_text,
            continuation=prepared.continuation_in,
        )
        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=result,
        )

    return execute


def _execute_facts_for(
    client: MediaWikiClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _FactsCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=MEDIAWIKI_PROVIDER,
                operation=PAGE_FACTS_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.get_page_facts(prepared.page_ids)
        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=result,
        )

    return execute


# ---------------------------------------------------------------------------
# Persist factories (application thread)
# ---------------------------------------------------------------------------


def _persist_search_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    match_config = config.tasks.match_wikipedia_identity

    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        if not isinstance(outcome.payload, MediaWikiSearchPage):
            raise RuntimeError(
                f"unexpected search payload type: {type(outcome.payload).__name__}"
            )
        result = outcome.payload
        form_id = work_item.subject_id
        if form_id is None:
            raise RuntimeError("search work item missing subject_id")
        form = load_query_form(connection, form_id=form_id)
        if form is None:
            return
        plan = load_plan(connection, plan_id=form.plan_id)
        if plan is None or plan.status == "superseded":
            # In-flight complete after supersede: no domain writes (K19).
            return
        if form.status != "pending":
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        continuation_in = _resolve_continuation_in(
            connection,
            fingerprint=work_item.fingerprint,
            material_fingerprint=plan.material_fingerprint,
            query_form_id=form_id,
        )

        # Cap stored hits for this response page by remaining form budget.
        prior_hits = form.hit_count or 0
        remaining_slots = max(0, match_config.max_search_hits_per_form - prior_hits)
        stored_hits = result.hits[:remaining_slots]
        hit_cap_truncated = len(result.hits) > remaining_slots

        # Detect insert-or-load: existing attempt reuses the observation id.
        prior_obs = connection.execute(
            "SELECT id FROM mediawiki_search_observation WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()
        is_new_observation = prior_obs is None

        observation_id = insert_or_load_search_observation_by_attempt(
            connection,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=attempt_id,
            query_text=form.query_text,
            continuation_in=continuation_in,
            continuation_out=result.continuation,
            srlimit=match_config.search_srlimit,
            hit_count=len(stored_hits),
            truncated=bool(result.more_results or hit_cap_truncated),
            response_complete=not result.more_results,
            observed_at=observed_at,
        )
        if is_new_observation:
            # Ranks are global within the form across continuation pages.
            insert_search_hits(
                connection,
                search_observation_id=observation_id,
                hits=tuple(
                    {
                        "rank": prior_hits + index,
                        "page_id": hit.page_id,
                        "title": hit.title,
                    }
                    for index, hit in enumerate(stored_hits, start=1)
                ),
            )
        else:
            # Idempotent re-settle: do not duplicate hits or re-bump counters.
            maybe_advance_plan(
                connection,
                plan_id=form.plan_id,
                run_id=run_id,
                config=config,
                now=observed_at,
            )
            return

        new_total = prior_hits + len(stored_hits)
        # ``continuations_used`` counts completed follow-up pages (not page 0).
        new_continuations_used = form.continuations_used
        if continuation_in is not None:
            new_continuations_used = form.continuations_used + 1

        more = bool(result.more_results and result.continuation is not None)
        hit_budget_left = new_total < match_config.max_search_hits_per_form
        # After this page settles, may we schedule another continuation?
        can_continue = (
            more
            and hit_budget_left
            and new_continuations_used < match_config.max_continuations_per_form
        )
        truncated = bool(
            (more and not can_continue)
            or hit_cap_truncated
            or (new_total >= match_config.max_search_hits_per_form and more)
        )

        if can_continue:
            mark_query_form_progress(
                connection,
                form_id=form_id,
                continuations_used=new_continuations_used,
                hit_count=new_total,
                truncated=False,
            )
            schedule_mediawiki_search(
                connection,
                form_id=form_id,
                material_fingerprint=plan.material_fingerprint,
                continuation_in=result.continuation,
                run_id=run_id,
                now=observed_at,
            )
            return

        mark_query_form_completed(
            connection,
            form_id=form_id,
            continuations_used=new_continuations_used,
            hit_count=new_total,
            truncated=truncated,
        )
        maybe_advance_plan(
            connection,
            plan_id=form.plan_id,
            run_id=run_id,
            config=config,
            now=observed_at,
        )

    return persist


def _persist_search_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
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

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        del attempt_id
        mark_query_form_failed(
            connection,
            form_id=int(form_id),
            failure_category=str(failure.category),
        )
        update_plan_retrieval_flags(
            connection,
            plan_id=form.plan_id,
            partial_retrieval=True,
        )
        maybe_advance_plan(
            connection,
            plan_id=form.plan_id,
            run_id=run_id,
            config=config,
            now=observed_at,
        )

    return persist_failure


def _persist_facts_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        if not isinstance(outcome.payload, MediaWikiPageFactsBatch):
            raise RuntimeError(
                f"unexpected facts payload type: {type(outcome.payload).__name__}"
            )
        result = outcome.payload
        batch_id = work_item.subject_id
        if batch_id is None:
            raise RuntimeError("facts work item missing subject_id")
        batch = load_page_facts_batch(connection, batch_id=batch_id)
        if batch is None:
            return
        plan = load_plan(connection, plan_id=batch.plan_id)
        if plan is None or plan.status == "superseded":
            return
        if batch.status != "pending":
            # Already settled (at-least-once): still try advance.
            attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
            del attempt_id
            maybe_advance_plan(
                connection,
                plan_id=batch.plan_id,
                run_id=run_id,
                config=config,
                now=observed_at,
            )
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        for page in result.pages:
            _upsert_page_from_fact(
                connection,
                page=page,
                observed_at=observed_at,
                attempt_id=attempt_id,
            )
        mark_batch_completed(
            connection,
            batch_id=batch_id,
            attempt_id=attempt_id,
            completed_at=observed_at,
        )
        maybe_advance_plan(
            connection,
            plan_id=batch.plan_id,
            run_id=run_id,
            config=config,
            now=observed_at,
        )

    return persist


def _persist_facts_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        state_row = connection.execute(
            "SELECT state, subject_id FROM work_item WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        if state_row is None or state_row["state"] != "failed_permanent":
            return
        batch_id = state_row["subject_id"]
        if batch_id is None:
            return
        batch = load_page_facts_batch(connection, batch_id=int(batch_id))
        if batch is None or batch.status != "pending":
            return
        plan = load_plan(connection, plan_id=batch.plan_id)
        if plan is None or plan.status == "superseded":
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        mark_batch_failed(
            connection,
            batch_id=int(batch_id),
            failure_category=str(failure.category),
            completed_at=observed_at,
            attempt_id=attempt_id,
        )
        # A failed facts batch may leave candidates incomplete; advance decides.
        update_plan_retrieval_flags(
            connection,
            plan_id=batch.plan_id,
            truncated_unsafe_for_negative=True,
        )
        maybe_advance_plan(
            connection,
            plan_id=batch.plan_id,
            run_id=run_id,
            config=config,
            now=observed_at,
        )

    return persist_failure


# ---------------------------------------------------------------------------
# Plan advancement helpers
# ---------------------------------------------------------------------------


def _schedule_wave1_facts_if_needed(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    material_fingerprint: str,
    match_config: MatchWikipediaIdentityConfig,
    run_id: int,
    now: str,
) -> bool:
    """Create wave-1 batches for unfetched search-hit page ids. Return if scheduled."""
    hits = list_search_hit_page_ids_for_completed_forms(connection, plan_id=plan_id)
    if not hits:
        return False
    hit_ids = [page_id for page_id, _rank in hits]
    covered = covered_fact_page_ids(connection, plan_id=plan_id)
    missing = sorted({page_id for page_id in hit_ids if page_id not in covered})
    if not missing:
        return False

    # Respect fact-page budget from the start.
    budget = match_config.max_fact_pages_per_plan - len(covered)
    if budget <= 0:
        update_plan_retrieval_flags(
            connection,
            plan_id=plan_id,
            truncated_unsafe_for_negative=True,
        )
        return False

    to_fetch = missing[:budget]
    if len(to_fetch) < len(missing):
        update_plan_retrieval_flags(
            connection,
            plan_id=plan_id,
            truncated_unsafe_for_negative=True,
        )

    _insert_and_schedule_batches(
        connection,
        plan_id=plan_id,
        page_ids=to_fetch,
        wave=1,
        material_fingerprint=material_fingerprint,
        match_config=match_config,
        run_id=run_id,
        now=now,
    )
    return True


def _schedule_redirect_wave_if_needed(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    material_fingerprint: str,
    match_config: MatchWikipediaIdentityConfig,
    run_id: int,
    now: str,
) -> bool:
    """Schedule next redirect-target wave; set unsafe on budget exhaust."""
    hits = list_search_hit_page_ids_for_completed_forms(connection, plan_id=plan_id)
    root_ids = {page_id for page_id, _rank in hits}
    if not root_ids:
        return False

    covered = covered_fact_page_ids(connection, plan_id=plan_id)
    pages = list_mediawiki_pages_by_page_ids(
        connection,
        wiki_id=DEFAULT_WIKI_ID,
        page_ids=sorted(covered),
    )
    hop_by_page = _min_hops_from_roots(root_ids, pages)

    targets: list[int] = []
    unresolved_over_budget = False
    for page_id, page in pages.items():
        target = page.redirect_to_page_id
        if target is None:
            continue
        if target in covered or target in targets:
            continue
        hop = hop_by_page.get(page_id)
        if hop is None:
            continue
        next_hop = hop + 1
        if next_hop > match_config.max_redirect_hops:
            unresolved_over_budget = True
            continue
        targets.append(target)

    if not targets:
        if unresolved_over_budget:
            update_plan_retrieval_flags(
                connection,
                plan_id=plan_id,
                truncated_unsafe_for_negative=True,
            )
        return False

    remaining = match_config.max_fact_pages_per_plan - len(covered)
    if remaining <= 0:
        update_plan_retrieval_flags(
            connection,
            plan_id=plan_id,
            truncated_unsafe_for_negative=True,
        )
        return False

    if len(targets) > remaining:
        unresolved_over_budget = True
        targets = targets[:remaining]

    if unresolved_over_budget:
        update_plan_retrieval_flags(
            connection,
            plan_id=plan_id,
            truncated_unsafe_for_negative=True,
        )

    # Wave = max existing wave + 1 for redirect targets.
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    wave = max((batch.wave for batch in batches), default=1) + 1
    _insert_and_schedule_batches(
        connection,
        plan_id=plan_id,
        page_ids=sorted(set(targets)),
        wave=wave,
        material_fingerprint=material_fingerprint,
        match_config=match_config,
        run_id=run_id,
        now=now,
    )
    return True


def _maybe_run_accent_phase(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    forms: Sequence[Any],
    batches: Sequence[Any],
    material_fingerprint: str,
    match_config: MatchWikipediaIdentityConfig,
    run_id: int,
    now: str,
) -> bool:
    """Insert accent forms when primaries lack main-ns non-dab hits.

    Returns True when accent search work was scheduled.
    """
    if any(form.variant_kind == _ACCENT_VARIANT_KIND for form in forms):
        # Accent already decided (forms exist).
        return False

    primary_forms = [
        form for form in forms if form.variant_kind in _PRIMARY_VARIANT_KINDS
    ]
    if not primary_forms:
        return False
    if any(form.status == "pending" for form in primary_forms):
        return False
    # Only evaluate accent when primary facts are quiescent (no pending batches
    # covering primary hits — all plan batches are quiescent by step 4/5).
    if any(batch.status == "pending" for batch in batches):
        return False

    if _has_main_ns_non_dab_reachable(
        connection, plan_id=plan_id, match_config=match_config
    ):
        return False  # skip accent; fall through to assemble

    remaining = match_config.max_query_forms - len(forms)
    if remaining < 1:
        return False

    primary_texts = [form.query_text for form in primary_forms]
    existing = {form.query_text for form in forms}
    accent_specs = generate_accent_fallback_forms(
        primary_texts,
        existing_query_texts=existing,
        remaining_budget=remaining,
    )
    if not accent_specs:
        return False

    start_ordinal = next_form_ordinal(connection, plan_id=plan_id)
    form_payloads = [
        {
            "ordinal": start_ordinal + index,
            "variant_kind": spec.variant_kind,
            "query_text": spec.query_text,
        }
        for index, spec in enumerate(accent_specs)
    ]
    form_ids = insert_query_forms(
        connection,
        plan_id=plan_id,
        forms=form_payloads,
    )
    for form_id in form_ids:
        schedule_mediawiki_search(
            connection,
            form_id=form_id,
            material_fingerprint=material_fingerprint,
            continuation_in=None,
            run_id=run_id,
            now=now,
        )
    return True


def _assemble_and_terminalize(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    plan: Any,
    forms: Sequence[Any],
    run_id: int,
    config: MainConfig,
    match_config: MatchWikipediaIdentityConfig,
    now: str,
) -> None:
    hits_raw = list_search_hit_page_ids_for_completed_forms(connection, plan_id=plan_id)
    hits = tuple(
        AssemblySearchHit(page_id=page_id, rank=rank) for page_id, rank in hits_raw
    )

    covered = covered_fact_page_ids(connection, plan_id=plan_id)
    # Also load any redirect targets we may need that are in the page table
    # even if not in covered (should not happen if waves completed).
    needed_ids = set(covered)
    for page_id, _rank in hits_raw:
        needed_ids.add(page_id)
    pages_records = list_mediawiki_pages_by_page_ids(
        connection,
        wiki_id=DEFAULT_WIKI_ID,
        page_ids=sorted(needed_ids),
    )
    # Expand once more via known redirects so terminal pages are present.
    for _ in range(match_config.max_redirect_hops + 1):
        extra: set[int] = set()
        for page in pages_records.values():
            if (
                page.redirect_to_page_id is not None
                and page.redirect_to_page_id not in pages_records
            ):
                extra.add(page.redirect_to_page_id)
        if not extra:
            break
        more = list_mediawiki_pages_by_page_ids(
            connection,
            wiki_id=DEFAULT_WIKI_ID,
            page_ids=sorted(extra),
        )
        pages_records.update(more)

    pages_by_id = {
        page_id: AssemblyPage(
            page_id=rec.page_id,
            canonical_title=rec.canonical_title,
            canonical_url=rec.canonical_url,
            namespace=rec.namespace,
            is_disambiguation=rec.is_disambiguation,
            is_missing=rec.is_missing,
            redirect_to_page_id=rec.redirect_to_page_id,
            description=rec.description,
            extract=rec.extract,
            categories=_parse_categories(rec.categories_json),
        )
        for page_id, rec in pages_records.items()
    }

    any_form_failed = any(form.status == "failed" for form in forms)
    any_form_truncated = any(
        form.status == "completed" and form.truncated for form in forms
    )
    # Failed facts batches also make retrieval incomplete for negatives.
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    any_batch_failed = any(batch.status == "failed" for batch in batches)
    # Unresolved redirect trails only (hop/page budget left targets unfetched).
    # Do not treat plan.truncated_unsafe alone as redirect budget — batch failure
    # also sets that flag and must surface as unsafe_truncation (not redirect).
    unresolved_redirect = any(
        page.redirect_to_page_id is not None
        and page.redirect_to_page_id not in pages_records
        for page in pages_records.values()
    )

    partial_retrieval = any_form_failed or bool(plan.partial_retrieval)
    search_incomplete = (
        any_form_truncated
        or any_batch_failed
        or (bool(plan.truncated_unsafe_for_negative) and not unresolved_redirect)
    )

    assembly = assemble_biography_candidates(
        hits,
        pages_by_id,
        max_candidates=match_config.max_candidates,
        max_redirect_hops=match_config.max_redirect_hops,
        search_incomplete=search_incomplete,
        redirect_budget_exhausted=unresolved_redirect,
        partial_retrieval=partial_retrieval,
    )

    truncated_unsafe = assembly.truncated_unsafe_for_negative
    if not assembly.candidates:
        if (
            not truncated_unsafe
            and not partial_retrieval
            and not unresolved_redirect
            and not any_form_failed
            and not any_batch_failed
        ):
            # K4: empty complete safe → deterministic no_match.
            _write_empty_no_match(
                connection,
                person_id=plan.person_id,
                plan_id=plan_id,
                material_fingerprint=plan.material_fingerprint,
                run_id=run_id,
                now=now,
            )
            return

        # Prefer explicit domain categories: partial > real redirect trails >
        # generic unsafe (batch fail, form truncation, fact-page cap, etc.).
        failure_category = assembly.failure_category_if_empty
        if failure_category is None:
            if partial_retrieval or any_form_failed:
                failure_category = "partial_retrieval_empty"
            elif unresolved_redirect:
                failure_category = "redirect_budget_exhausted"
            else:
                failure_category = "unsafe_truncation"
        elif (
            failure_category == "redirect_budget_exhausted" and not unresolved_redirect
        ):
            # Assembly may inherit redirect_budget from a stale flag; reclassify.
            if partial_retrieval or any_form_failed:
                failure_category = "partial_retrieval_empty"
            else:
                failure_category = "unsafe_truncation"
        _write_local_failed(
            connection,
            person_id=plan.person_id,
            plan_id=plan_id,
            material_fingerprint=plan.material_fingerprint,
            run_id=run_id,
            failure_category=failure_category,
            truncated_unsafe=truncated_unsafe or unresolved_redirect,
            partial_retrieval=partial_retrieval,
            now=now,
        )
        return

    # Non-empty → ready_for_match; schedule match; ensure inspections (K21b).
    mark_plan_status(
        connection,
        plan_id=plan_id,
        status="ready_for_match",
        truncated_unsafe_for_negative=truncated_unsafe,
        partial_retrieval=partial_retrieval,
    )
    schedule_match_wikipedia_identity(
        connection,
        person_id=plan.person_id,
        material_fingerprint=plan.material_fingerprint,
        run_id=run_id,
        now=now,
    )
    # Mid-run inspect arming (K21b). Until Task 6 extends models_needed_for_run,
    # this is a no-op for the match model when only Wikipedia work is active.
    ensure_model_inspections_for_run(
        connection,
        run_id=run_id,
        config=config,
        now=now,
    )


def _write_empty_no_match(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    plan_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
) -> None:
    observation_id = insert_wikipedia_identity_observation(
        connection,
        person_id=person_id,
        plan_id=plan_id,
        run_id=run_id,
        attempt_id=None,
        model_inspection_id=None,
        disposition="completed",
        semantic_outcome="no_matching_page_found",
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        canonical_supplied_input_json=EMPTY_COMPLETE_SUPPLIED_INPUT_JSON,
        validated_output_json=NO_MATCH_VALIDATED_OUTPUT_JSON,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        task_fingerprint=material_fingerprint,
        rationale=NO_MATCH_RATIONALE,
        failure_category=None,
        observed_at=now,
    )
    point_person_current_wikipedia_observation(
        connection,
        person_id=person_id,
        observation_id=observation_id,
    )
    mark_plan_status(
        connection,
        plan_id=plan_id,
        status="completed",
        completed_at=now,
        truncated_unsafe_for_negative=False,
        partial_retrieval=False,
    )


def _write_local_failed(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    plan_id: int,
    material_fingerprint: str,
    run_id: int,
    failure_category: str,
    truncated_unsafe: bool,
    partial_retrieval: bool,
    now: str,
) -> None:
    insert_wikipedia_identity_observation(
        connection,
        person_id=person_id,
        plan_id=plan_id,
        run_id=run_id,
        attempt_id=None,
        model_inspection_id=None,
        disposition="failed",
        semantic_outcome=None,
        matched_mediawiki_page_id=None,
        candidate_page_ids_json="[]",
        canonical_supplied_input_json=LOCAL_FAILED_SUPPLIED_INPUT_JSON,
        validated_output_json=None,
        prompt_hash=None,
        schema_hash=None,
        schema_version=None,
        task_fingerprint=material_fingerprint,
        rationale=failure_category,
        failure_category=failure_category,
        observed_at=now,
    )
    # K25: do not move current pointer.
    mark_plan_status(
        connection,
        plan_id=plan_id,
        status="failed",
        completed_at=now,
        failure_category=failure_category,
        truncated_unsafe_for_negative=truncated_unsafe,
        partial_retrieval=partial_retrieval,
    )


def _has_main_ns_non_dab_reachable(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    match_config: MatchWikipediaIdentityConfig,
) -> bool:
    hits_raw = list_search_hit_page_ids_for_completed_forms(connection, plan_id=plan_id)
    if not hits_raw:
        return False
    hits = tuple(
        AssemblySearchHit(page_id=page_id, rank=rank) for page_id, rank in hits_raw
    )
    covered = covered_fact_page_ids(connection, plan_id=plan_id)
    needed = set(covered) | {page_id for page_id, _ in hits_raw}
    pages_records = list_mediawiki_pages_by_page_ids(
        connection,
        wiki_id=DEFAULT_WIKI_ID,
        page_ids=sorted(needed),
    )
    for _ in range(match_config.max_redirect_hops + 1):
        extra: set[int] = set()
        for page in pages_records.values():
            if (
                page.redirect_to_page_id is not None
                and page.redirect_to_page_id not in pages_records
            ):
                extra.add(page.redirect_to_page_id)
        if not extra:
            break
        pages_records.update(
            list_mediawiki_pages_by_page_ids(
                connection,
                wiki_id=DEFAULT_WIKI_ID,
                page_ids=sorted(extra),
            )
        )
    pages_by_id = {
        page_id: AssemblyPage(
            page_id=rec.page_id,
            canonical_title=rec.canonical_title,
            canonical_url=rec.canonical_url,
            namespace=rec.namespace,
            is_disambiguation=rec.is_disambiguation,
            is_missing=rec.is_missing,
            redirect_to_page_id=rec.redirect_to_page_id,
            description=rec.description,
            extract=rec.extract,
            categories=_parse_categories(rec.categories_json),
        )
        for page_id, rec in pages_records.items()
    }
    assembly = assemble_biography_candidates(
        hits,
        pages_by_id,
        max_candidates=match_config.max_candidates,
        max_redirect_hops=match_config.max_redirect_hops,
    )
    return len(assembly.candidates) > 0


def _insert_and_schedule_batches(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    page_ids: Sequence[int],
    wave: int,
    material_fingerprint: str,
    match_config: MatchWikipediaIdentityConfig,
    run_id: int,
    now: str,
) -> None:
    chunk_size = match_config.max_page_ids_per_facts_request
    for offset in range(0, len(page_ids), chunk_size):
        chunk = list(page_ids[offset : offset + chunk_size])
        ordinal = next_batch_ordinal(connection, plan_id=plan_id)
        page_ids_json = _canonical_json(sorted(chunk))
        batch_id = insert_page_facts_batch(
            connection,
            plan_id=plan_id,
            ordinal=ordinal,
            page_ids_json=page_ids_json,
            wave=wave,
            created_at=now,
        )
        schedule_mediawiki_page_facts(
            connection,
            batch_id=batch_id,
            material_fingerprint=material_fingerprint,
            run_id=run_id,
            now=now,
        )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


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


def _upsert_page_from_fact(
    connection: sqlite3.Connection,
    *,
    page: MediaWikiPageFact,
    observed_at: str,
    attempt_id: int,
) -> None:
    categories_json = _canonical_json(list(page.categories))
    upsert_mediawiki_page(
        connection,
        wiki_id=DEFAULT_WIKI_ID,
        page_id=page.page_id,
        canonical_title=(
            page.canonical_title or page.requested_title or f"page-{page.page_id}"
        ),
        canonical_url=(
            page.canonical_url or f"https://en.wikipedia.org/?curid={page.page_id}"
        ),
        namespace=page.namespace,
        is_disambiguation=page.is_disambiguation,
        is_missing=page.missing,
        redirect_to_page_id=page.redirect_to_page_id,
        description=page.description,
        extract=page.extract,
        categories_json=categories_json,
        last_observed_at=observed_at,
        last_attempt_id=attempt_id,
    )


def _min_hops_from_roots(
    root_ids: set[int],
    pages: dict[int, Any],
) -> dict[int, int]:
    """BFS hop distance from search-hit roots along known redirect edges."""
    hops: dict[int, int] = {root: 0 for root in root_ids}
    changed = True
    while changed:
        changed = False
        for page_id, page in pages.items():
            if page_id not in hops:
                continue
            target = getattr(page, "redirect_to_page_id", None)
            if target is None:
                continue
            candidate = hops[page_id] + 1
            previous = hops.get(target)
            if previous is None or candidate < previous:
                hops[target] = candidate
                changed = True
    return hops


def _resolve_continuation_in(
    connection: sqlite3.Connection,
    *,
    fingerprint: str,
    material_fingerprint: str,
    query_form_id: int,
) -> str | None:
    """Recover the continuation token hashed into a search work-item fingerprint."""
    candidates: list[str | None] = [None]
    rows = connection.execute(
        """
        SELECT continuation_out, continuation_in
          FROM mediawiki_search_observation
         WHERE query_form_id = ?
        """,
        (query_form_id,),
    ).fetchall()
    for row in rows:
        if row["continuation_out"] is not None:
            candidates.append(str(row["continuation_out"]))
        if row["continuation_in"] is not None:
            candidates.append(str(row["continuation_in"]))
    # Also accept tokens already scheduled as active work fingerprints.
    for token in dict.fromkeys(candidates):
        if (
            mediawiki_search_fingerprint(
                material_fingerprint=material_fingerprint,
                query_form_id=query_form_id,
                continuation_in=token,
            )
            == fingerprint
        ):
            return token
    raise ValueError(
        f"cannot resolve continuation for search fingerprint {fingerprint[:12]}…"
    )


def _parse_page_ids_json(raw: str) -> tuple[int, ...]:
    data = json.loads(raw)
    if not isinstance(data, list):
        raise ValueError("page_ids_json must be a JSON array")
    return tuple(int(item) for item in data)


def _parse_categories(raw: str) -> tuple[str, ...]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return ()
    if not isinstance(data, list):
        return ()
    return tuple(str(item) for item in data if isinstance(item, str))


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# match_wikipedia_identity (OpenRouter LLM)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _MatchCall:
    """Application-thread inputs for the worker-thread match generation."""

    request: StructuredGenerationRequest
    match_input: MatchWikipediaIdentityInput
    person_id: int
    plan_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    truncated_unsafe_for_negative: bool
    candidate_page_ids: tuple[int, ...]
    mediawiki_row_id_by_page_id: dict[int, int]


@dataclass(frozen=True, slots=True)
class _MatchPersist:
    """Prevalidated match output and provenance for application-thread persist."""

    output: MatchWikipediaIdentityOutput
    person_id: int
    plan_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    candidate_page_ids: tuple[int, ...]
    mediawiki_row_id_by_page_id: dict[int, int]


def build_match_wikipedia_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
) -> TaskHandler:
    """The ``match_wikipedia_identity`` handler: one structured generation."""
    match_config = config.tasks.match_wikipedia_identity
    model_id = match_config.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname
    parameters = match_config.parameters

    def ready(claimed_run_id: int) -> bool:
        return inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=model_id,
        )

    def prepare(work_item: WorkItem) -> TaskPreparation:
        if work_item.subject_id is None:
            raise ValueError(MISSING_PERSON_DETAIL)
        person_id = int(work_item.subject_id)
        task_fingerprint = work_item.fingerprint
        run_id = _claimed_run_id(connection, work_item.id)

        existing = load_wikipedia_identity_observation_by_fingerprint(
            connection,
            person_id=person_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            _prepare_reuse_existing_observation(
                connection,
                person_id=person_id,
                task_fingerprint=task_fingerprint,
                existing_id=existing.id,
                disposition=existing.disposition,
                work_item_id=work_item.id,
            )
            raise ValueError(
                f"{MATCH_PREPARE_REFUSED_PREFIX}already_settled person {person_id}"
            )

        plan = load_active_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=task_fingerprint,
        )
        if plan is None:
            raise ValueError(
                f"{MATCH_PREPARE_REFUSED_PREFIX}no_active_plan person {person_id}"
            )
        if plan.status == "superseded":
            raise ValueError(
                f"{MATCH_PREPARE_REFUSED_PREFIX}superseded_plan person {person_id}"
            )

        candidates, page_row_ids = _assemble_match_candidates(
            connection,
            plan_id=plan.id,
            match_config=match_config,
            truncated_unsafe_for_negative=plan.truncated_unsafe_for_negative,
            partial_retrieval=plan.partial_retrieval,
        )
        if not candidates:
            _prepare_empty_candidates_path(
                connection,
                person_id=person_id,
                plan=plan,
                task_fingerprint=task_fingerprint,
                run_id=run_id,
                work_item_id=work_item.id,
            )
            raise ValueError(
                f"{MATCH_PREPARE_REFUSED_PREFIX}empty_candidates person {person_id}"
            )

        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=model_id,
            routing_fingerprint=routing_fp,
        )
        if inspection is None or inspection.compatibility != "compatible":
            raise ValueError(MISSING_INSPECTION_DETAIL)

        display_name, sourced_names, identity_facts = _operational_match_material(
            connection,
            person_id=person_id,
            match_config=match_config,
        )
        match_input = build_match_input(
            person_id=person_id,
            display_name=display_name,
            sourced_names=sourced_names,
            identity_facts=identity_facts,
            candidates=candidates,
            config=match_config,
            truncated_unsafe_for_negative=plan.truncated_unsafe_for_negative,
            partial_retrieval=plan.partial_retrieval,
        )
        rendered = render_match_request(match_input)
        request = StructuredGenerationRequest(
            model_id=model_id,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name=MATCH_SCHEMA_NAME,
            max_completion_tokens=match_config.max_completion_tokens,
            temperature=parameters.temperature,
            top_p=parameters.top_p,
            reasoning_effort=parameters.reasoning_effort,
        )
        call = _MatchCall(
            request=request,
            match_input=match_input,
            person_id=person_id,
            plan_id=plan.id,
            model_inspection_id=inspection.id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=rendered.prompt_hash,
            schema_hash=rendered.schema_hash,
            schema_version=rendered.schema_version,
            task_fingerprint=task_fingerprint,
            truncated_unsafe_for_negative=plan.truncated_unsafe_for_negative,
            candidate_page_ids=tuple(c.page_id for c in candidates),
            mediawiki_row_id_by_page_id=page_row_ids,
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
            max_input_tokens=match_config.max_input_tokens,
            max_completion_tokens=match_config.max_completion_tokens,
        )
        return TaskPreparation(payload=call, reserved_nano_usd=reserved)

    def destination_host(_work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
        provider=OPENROUTER_PROVIDER,
        operation=GENERATE_OPERATION,
        execute=_execute_match_for(client),
        prepare=prepare,
        persist=_persist_match_for(connection),
        persist_failure=_persist_match_failure_for(connection, config=config),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )


def _execute_match_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        del work_item, ordinal
        if not isinstance(prepared, _MatchCall):
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
                detail=MALFORMED_MATCH_DETAIL,
            )
        try:
            output = validate_match_output(
                raw_text,
                prepared.match_input,
                truncated_unsafe_for_negative=prepared.truncated_unsafe_for_negative,
            )
        except MatchValidationError:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=OPENROUTER_PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_MATCH_DETAIL,
            ) from None
        del raw_text
        payload = _MatchPersist(
            output=output,
            person_id=prepared.person_id,
            plan_id=prepared.plan_id,
            model_inspection_id=prepared.model_inspection_id,
            canonical_supplied_input_json=prepared.canonical_supplied_input_json,
            prompt_hash=prepared.prompt_hash,
            schema_hash=prepared.schema_hash,
            schema_version=prepared.schema_version,
            task_fingerprint=prepared.task_fingerprint,
            candidate_page_ids=prepared.candidate_page_ids,
            mediawiki_row_id_by_page_id=prepared.mediawiki_row_id_by_page_id,
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


def _persist_match_for(
    connection: sqlite3.Connection,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _MatchPersist):
            raise RuntimeError(
                f"unexpected match payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        existing = load_wikipedia_identity_observation_by_fingerprint(
            connection,
            person_id=payload.person_id,
            task_fingerprint=payload.task_fingerprint,
        )
        if existing is not None:
            if existing.disposition == "completed":
                point_person_current_wikipedia_observation(
                    connection,
                    person_id=payload.person_id,
                    observation_id=existing.id,
                )
                plan = load_plan(connection, plan_id=payload.plan_id)
                if plan is not None and plan.status not in _TERMINAL_PLAN_STATUSES:
                    mark_plan_status(
                        connection,
                        plan_id=payload.plan_id,
                        status="completed",
                        completed_at=observed_at,
                    )
            return

        plan = load_plan(connection, plan_id=payload.plan_id)
        if plan is None or plan.status == "superseded":
            return

        output = payload.output
        semantic = map_model_outcome_to_semantic(output.outcome)
        matched_row_id: int | None = None
        if output.outcome == "matching_page":
            if output.selected_page_id is None:
                raise RuntimeError("matching_page output missing selected_page_id")
            matched_row_id = payload.mediawiki_row_id_by_page_id.get(
                output.selected_page_id
            )
            if matched_row_id is None:
                raise RuntimeError(
                    f"selected page {output.selected_page_id} has no mediawiki_page row"
                )

        observation_id = insert_wikipedia_identity_observation(
            connection,
            person_id=payload.person_id,
            plan_id=payload.plan_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=payload.model_inspection_id,
            disposition="completed",
            semantic_outcome=semantic,
            matched_mediawiki_page_id=matched_row_id,
            candidate_page_ids_json=_candidate_page_ids_json(
                payload.candidate_page_ids
            ),
            canonical_supplied_input_json=payload.canonical_supplied_input_json,
            validated_output_json=output.model_dump_json(),
            prompt_hash=payload.prompt_hash,
            schema_hash=payload.schema_hash,
            schema_version=payload.schema_version,
            task_fingerprint=payload.task_fingerprint,
            supporting_fact_ids_json=json.dumps(
                list(output.supporting_fact_ids), separators=(",", ":")
            ),
            conflicting_fact_ids_json=json.dumps(
                list(output.conflicting_fact_ids), separators=(",", ":")
            ),
            rationale=output.rationale,
            failure_category=None,
            observed_at=observed_at,
        )
        # K25: set pointer on completed model outcomes (including uncertain).
        point_person_current_wikipedia_observation(
            connection,
            person_id=payload.person_id,
            observation_id=observation_id,
        )
        mark_plan_status(
            connection,
            plan_id=payload.plan_id,
            status="completed",
            completed_at=observed_at,
        )

    return persist


def _persist_match_failure_for(
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
        person_id = int(state_row["subject_id"])
        task_fingerprint = str(state_row["fingerprint"])
        existing = load_wikipedia_identity_observation_by_fingerprint(
            connection,
            person_id=person_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            # Completed exists → no-op (re-armed empty-at-prepare race).
            # Failed exists → idempotent no-op.
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        routing_fp = routing_fingerprint(config.openrouter.routing)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=config.tasks.match_wikipedia_identity.model,
            routing_fingerprint=routing_fp,
        )
        model_inspection_id = None if inspection is None else inspection.id
        prompt_hash, schema_hash, schema_version = match_prompt_and_schema_hashes()
        plan = load_active_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=task_fingerprint,
        )
        plan_id = None if plan is None else plan.id
        category = (
            INVALID_MODEL_OUTPUT_CATEGORY
            if failure.category is FailureCategory.MALFORMED_RESPONSE
            else str(failure.category)
        )
        insert_wikipedia_identity_observation(
            connection,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=model_inspection_id,
            disposition="failed",
            semantic_outcome=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json=MATCH_FAILED_SUPPLIED_INPUT_JSON,
            validated_output_json=None,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=task_fingerprint,
            rationale=str(failure.category),
            failure_category=category,
            observed_at=observed_at,
        )
        # K25: do not move current pointer.
        if plan is not None and plan.status not in _TERMINAL_PLAN_STATUSES:
            mark_plan_status(
                connection,
                plan_id=plan.id,
                status="failed",
                completed_at=observed_at,
                failure_category=category,
            )

    return persist_failure


def _prepare_reuse_existing_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    task_fingerprint: str,
    existing_id: int,
    disposition: str,
    work_item_id: int,
) -> None:
    """Point / complete plan for an existing completed obs (prepare-time)."""
    if disposition != "completed":
        return
    owns = not connection.in_transaction
    if owns:
        connection.execute("BEGIN IMMEDIATE")
    try:
        point_person_current_wikipedia_observation(
            connection,
            person_id=person_id,
            observation_id=existing_id,
        )
        plan = load_active_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=task_fingerprint,
        )
        if plan is not None:
            mark_plan_status(
                connection,
                plan_id=plan.id,
                status="completed",
                completed_at=_observed_at_for_prepare(connection, work_item_id),
            )
    except BaseException:
        if owns:
            connection.rollback()
        raise
    else:
        if owns:
            connection.commit()


def _prepare_empty_candidates_path(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    plan: Any,
    task_fingerprint: str,
    run_id: int,
    work_item_id: int,
) -> None:
    """Deterministic empty race settlement owned by prepare (brief txn)."""
    now = _observed_at_for_prepare(connection, work_item_id)
    owns = not connection.in_transaction
    if owns:
        connection.execute("BEGIN IMMEDIATE")
    try:
        if not plan.truncated_unsafe_for_negative and not plan.partial_retrieval:
            _write_empty_no_match(
                connection,
                person_id=person_id,
                plan_id=plan.id,
                material_fingerprint=task_fingerprint,
                run_id=run_id,
                now=now,
            )
        else:
            _write_local_failed(
                connection,
                person_id=person_id,
                plan_id=plan.id,
                material_fingerprint=task_fingerprint,
                run_id=run_id,
                failure_category="unsafe_truncation",
                truncated_unsafe=plan.truncated_unsafe_for_negative,
                partial_retrieval=plan.partial_retrieval,
                now=now,
            )
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
        # Fixture paths may not claim; fall back to latest attempt run.
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


def _observed_at_for_prepare(connection: sqlite3.Connection, work_item_id: int) -> str:
    row = connection.execute(
        "SELECT updated_at FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is not None and row["updated_at"]:
        return str(row["updated_at"])
    return utc_timestamp(datetime.now(tz=UTC))


def _candidate_page_ids_json(page_ids: Sequence[int]) -> str:
    return json.dumps(list(page_ids), separators=(",", ":"))


def _operational_match_material(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    match_config: MatchWikipediaIdentityConfig,
) -> tuple[str, tuple[MatchName, ...], tuple[MatchFact, ...]]:
    """K27: names and facts from the operational projection."""
    canonical_id = canonical_person_id(connection, person_id)
    try:
        display_name = select_display_name(connection, canonical_id)
    except LookupError:
        row = connection.execute(
            "SELECT display_name FROM person WHERE id = ?",
            (canonical_id,),
        ).fetchone()
        if row is None:
            raise ValueError(MISSING_PERSON_DETAIL) from None
        display_name = str(row["display_name"])

    closure = person_id_closure_for_canonical(connection, canonical_id)
    if not closure:
        raise ValueError(MISSING_PERSON_DETAIL)
    placeholders = ",".join("?" for _ in closure)
    name_rows = connection.execute(
        f"""
        SELECT id, exact_name, search_name, match_key, kind
          FROM sourced_name
         WHERE person_id IN ({placeholders})
         ORDER BY id
        """,
        closure,
    ).fetchall()
    names: list[MatchName] = []
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
        kind_raw = str(row["kind"])
        kind: SourcedNameKind
        if kind_raw in {"professional", "display", "alias", "other", "mononym"}:
            kind = kind_raw  # type: ignore[assignment]
        else:
            kind = "other"
        names.append(
            MatchName(
                exact_name=str(row["exact_name"]),
                search_name=str(row["search_name"]),
                match_key=key,
                kind=kind,
            )
        )
        if len(names) >= match_config.max_names_in_prompt:
            break

    if not names:
        names.append(
            MatchName(
                exact_name=display_name,
                search_name=display_name,
                match_key=display_name.casefold() or "unknown",
                kind="display",
            )
        )

    mentions = mentions_for_canonical_person(connection, canonical_id)
    facts: list[MatchFact] = []
    fact_index = 1
    for mention in mentions:
        for fact in mention.identity_facts:
            kind_raw = str(fact.kind)
            try:
                kind_enum = IdentityFactKind(kind_raw)
            except ValueError:
                kind_enum = IdentityFactKind.OTHER
            facts.append(
                MatchFact(
                    local_id=f"f{fact_index}",
                    kind=kind_enum,
                    value=str(fact.value)[:500] or str(fact.value)[:1],
                )
            )
            fact_index += 1
            if len(facts) >= match_config.max_facts_in_prompt:
                break
        if len(facts) >= match_config.max_facts_in_prompt:
            break

    return display_name, tuple(names), tuple(facts)


def _assemble_match_candidates(
    connection: sqlite3.Connection,
    *,
    plan_id: int,
    match_config: MatchWikipediaIdentityConfig,
    truncated_unsafe_for_negative: bool,
    partial_retrieval: bool,
) -> tuple[tuple[MatchWikiCandidate, ...], dict[int, int]]:
    """Rebuild code-selected candidates for the match model from plan state."""
    hits_raw = list_search_hit_page_ids_for_completed_forms(connection, plan_id=plan_id)
    hits = tuple(
        AssemblySearchHit(page_id=page_id, rank=rank) for page_id, rank in hits_raw
    )
    covered = covered_fact_page_ids(connection, plan_id=plan_id)
    needed_ids = set(covered) | {page_id for page_id, _ in hits_raw}
    pages_records = list_mediawiki_pages_by_page_ids(
        connection,
        wiki_id=DEFAULT_WIKI_ID,
        page_ids=sorted(needed_ids),
    )
    for _ in range(match_config.max_redirect_hops + 1):
        extra: set[int] = set()
        for page in pages_records.values():
            if (
                page.redirect_to_page_id is not None
                and page.redirect_to_page_id not in pages_records
            ):
                extra.add(page.redirect_to_page_id)
        if not extra:
            break
        pages_records.update(
            list_mediawiki_pages_by_page_ids(
                connection,
                wiki_id=DEFAULT_WIKI_ID,
                page_ids=sorted(extra),
            )
        )
    pages_by_id = {
        page_id: AssemblyPage(
            page_id=rec.page_id,
            canonical_title=rec.canonical_title,
            canonical_url=rec.canonical_url,
            namespace=rec.namespace,
            is_disambiguation=rec.is_disambiguation,
            is_missing=rec.is_missing,
            redirect_to_page_id=rec.redirect_to_page_id,
            description=rec.description,
            extract=rec.extract,
            categories=_parse_categories(rec.categories_json),
        )
        for page_id, rec in pages_records.items()
    }
    assembly = assemble_biography_candidates(
        hits,
        pages_by_id,
        max_candidates=match_config.max_candidates,
        max_redirect_hops=match_config.max_redirect_hops,
        search_incomplete=truncated_unsafe_for_negative,
        partial_retrieval=partial_retrieval,
    )
    page_row_ids = {
        rec.page_id: rec.id for rec in pages_records.values() if not rec.is_missing
    }
    candidates = tuple(
        _to_match_wiki_candidate(
            candidate,
            pages_by_id=pages_by_id,
            hits_raw=hits_raw,
            match_config=match_config,
        )
        for candidate in assembly.candidates
    )
    return candidates, page_row_ids


def _to_match_wiki_candidate(
    candidate: BiographyCandidate,
    *,
    pages_by_id: dict[int, AssemblyPage],
    hits_raw: Sequence[tuple[int, int]],
    match_config: MatchWikipediaIdentityConfig,
) -> MatchWikiCandidate:
    trail = _redirect_trail_titles(
        terminal_page_id=candidate.page_id,
        pages_by_id=pages_by_id,
        hits_raw=hits_raw,
        max_redirect_hops=match_config.max_redirect_hops,
    )
    extract = candidate.extract
    if extract is not None and len(extract) > match_config.max_extract_characters:
        extract = extract[: match_config.max_extract_characters]
    categories = candidate.categories[: match_config.max_categories_per_page]
    title = candidate.canonical_title
    if len(title) > match_config.max_title_characters:
        title = title[: match_config.max_title_characters]
    return MatchWikiCandidate(
        page_id=candidate.page_id,
        title=title,
        canonical_url=candidate.canonical_url,
        namespace=0,
        is_disambiguation=False,
        description=candidate.description,
        extract=extract,
        categories=categories,
        redirect_trail=trail,
    )


def _redirect_trail_titles(
    *,
    terminal_page_id: int,
    pages_by_id: dict[int, AssemblyPage],
    hits_raw: Sequence[tuple[int, int]],
    max_redirect_hops: int,
) -> tuple[str, ...]:
    """Titles of redirect intermediates from the best root to the terminal."""
    best_root: int | None = None
    best_rank: int | None = None
    for root_id, rank in hits_raw:
        terminal = _walk_terminal(
            root_id, pages_by_id, max_redirect_hops=max_redirect_hops
        )
        if terminal != terminal_page_id:
            continue
        if best_rank is None or rank < best_rank:
            best_rank = rank
            best_root = root_id
    if best_root is None or best_root == terminal_page_id:
        return ()
    titles: list[str] = []
    current = best_root
    seen: set[int] = set()
    while current != terminal_page_id and current not in seen:
        seen.add(current)
        page = pages_by_id.get(current)
        if page is None or page.redirect_to_page_id is None:
            break
        titles.append(page.canonical_title)
        current = page.redirect_to_page_id
    return tuple(titles)


def _walk_terminal(
    root_id: int,
    pages_by_id: dict[int, AssemblyPage],
    *,
    max_redirect_hops: int,
) -> int | None:
    current = root_id
    seen: set[int] = set()
    hops = 0
    while True:
        if current in seen:
            return None
        seen.add(current)
        page = pages_by_id.get(current)
        if page is None:
            return None
        if page.redirect_to_page_id is None:
            return current
        hops += 1
        if hops > max_redirect_hops:
            return None
        current = page.redirect_to_page_id
