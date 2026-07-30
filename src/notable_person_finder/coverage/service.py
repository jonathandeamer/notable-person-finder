"""Coverage plan lifecycle and ``brave_web_search`` handler (Task 6a).

Plan open attaches discovery (K10), schedules exact-name forms (K6), and
stages alias/context after exact terminal. ``brave_web_search`` performs
exactly one Brave ``search_web`` call per execute. Workers never open SQLite;
domain settlement and ``maybe_advance_coverage_plan`` run on the application
thread inside the engine settlement transaction.

Fetch and assess handlers land in Tasks 6b/7; this module still final-selects
targets and schedules ``fetch_article`` so those handlers can settle them.
Terminal truth table T1–T11 applies once search/targets/assess are quiescent.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from notable_person_finder.config.models import AssessArticleConfig, MainConfig
from notable_person_finder.coverage.models import (
    CoverageQueryFormRecord,
    PersonCoveragePlanRecord,
)
from notable_person_finder.coverage.queries import (
    generate_alias_forms,
    generate_context_form,
    generate_exact_forms,
)
from notable_person_finder.coverage.repository import (
    insert_coverage_article_target,
    insert_coverage_discovery_article,
    insert_or_load_brave_search_observation_by_attempt,
    insert_query_forms,
    insert_search_result_occurrences,
    insert_source_screening,
    list_coverage_article_targets_for_plan,
    list_coverage_discovery_articles_for_plan,
    list_query_forms_for_plan,
    load_plan,
    load_query_form,
    load_source_screening,
    mark_query_form_completed,
    mark_query_form_failed,
    mark_query_form_progress,
    open_plan,
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
    person_id_closure_for_canonical,
    select_display_name,
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
from notable_person_finder.runs import repository as runs_repository
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

_TERMINAL_PLAN_STATUSES = frozenset({"superseded", "completed", "failed", "incomplete"})
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
    """Schedule one ``fetch_article`` work item (handler lands in Task 6b)."""
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
           AND w.state IN ('pending', 'running', 'deferred')
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
