"""Model inspection seeding, detection scheduling, and LLM task handlers.

This milestone schedules current-run ``inspect_model`` work when usable
untriaged source items exist, gates detection readiness on a compatible
inspection, schedules ``detect_people`` work (or schedule-time
``insufficient_input`` terminals), and runs generation on the LLM pool.

**Thread split (same contract as feed handlers):**

* ``prepare`` / ``persist`` / ``persist_failure`` run on the application thread
  and may touch SQLite.
* ``execute`` runs on a scheduler worker, performs exactly one external call,
  and never opens SQLite. It is built by a module-level factory so no
  connection is in scope where it is defined.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from importlib import resources
from urllib.parse import urlsplit

from notable_person_finder.config.models import (
    DomainProfileConfig,
    FeedConfig,
    MainConfig,
    ProviderRoutingConfig,
)
from notable_person_finder.people.candidates import (
    CandidatePerson,
    retrieve_candidates,
    scan_name_matched_peers,
)
from notable_person_finder.people.detection import (
    DETECTION_SCHEMA_VERSION,
    DetectionValidationError,
    build_detection_input,
    detection_schema,
    render_detection_request,
    validate_detection_output,
)
from notable_person_finder.people.identity import (
    canonical_person_id,
    create_person_for_mention,
    mentions_for_canonical_person,
    recompute_identity_fingerprint,
    select_display_name,
    upsert_sourced_names_for_mention,
)
from notable_person_finder.people.merge import confirm_person_merge
from notable_person_finder.people.models import (
    AttentionCategory,
    CautionCategory,
    DetectionInput,
    DetectionOutput,
    DetectionPassage,
    GroundedSignal,
    IdentityFact,
    IdentityFactKind,
    ResolveCandidate,
    ResolveCandidateFact,
    ResolveCandidateName,
    ResolvePersonEntityInput,
    ResolvePersonEntityOutput,
    SignalGrounding,
    SignalKind,
    SourcedNameKind,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    SourceItemRecord,
    dismiss_relation,
    insert_completed_observation,
    insert_entity_resolution_observation,
    insert_failed_observation,
    insert_insufficient_input_observation,
    insert_model_inspection,
    list_active_possible_same_person_for,
    list_untriaged_source_item_ids,
    load_current_triage_observation,
    load_er_by_mention_fingerprint,
    load_er_by_relation_fingerprint,
    load_model_inspection,
    load_person_mentions,
    load_source_item_record,
    load_triage_observation_by_fingerprint,
    match_key,
    point_mention_current_er,
    settle_active_tasks_after_permanent_preflight,
    upsert_active_possible_same_person,
)
from notable_person_finder.people.resolution import (
    ResolutionValidationError,
    build_resolve_input,
    first_pass_canonical_supplied_input_json,
    first_pass_task_fingerprint,
    reconsider_task_fingerprint,
    render_resolution_request,
    resolution_prompt_and_schema_hashes,
    validate_resolution_output,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    INSPECT_OPERATION,
    PROVIDER,
    LlmClient,
    ModelInspectionRequest,
    ModelInspectionResult,
    StructuredGenerationRequest,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome, TaskPreparation
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import WorkerPool

INSPECT_MODEL_TASK_TYPE = "inspect_model"
INSPECT_MODEL_PRIORITY = 20
SUBJECT_KIND_MODEL = "model"
SUBJECT_KIND_SOURCE_ITEM = "source_item"
SUBJECT_KIND_PERSON_MENTION = "person_mention"
SUBJECT_KIND_PERSON_RELATION = "person_relation"

DETECT_PEOPLE_PRIORITY = 30
DETECTION_SCHEMA_NAME = "detect_people"

RESOLVE_PERSON_PRIORITY = 40
RECONSIDER_PERSON_PRIORITY = 40
RESOLUTION_SCHEMA_NAME = "resolve_person_entity"
CREATED_NEW_VALIDATED_OUTPUT_JSON = '{"outcome":"created_new"}'
SKIPPED_MATCH_KEY_RATIONALE = "insufficient_identity_match_key"
MALFORMED_RESOLUTION_DETAIL = "invalid resolution output"
MISSING_MENTION_DETAIL = "person mention is missing"
MISSING_RELATION_DETAIL = "person relation is missing"
RESOLVE_PREPARE_REFUSED_PREFIX = "resolve_prepare_refused:"
SUPERSEDED_RECONSIDER_REASON = "stale reconsideration material"
MERGE_GUARDRAILS_FAILED_NOTE = " [merge_guardrails_failed]"
READY_TO_MERGE_NOTE = " [ready_to_merge]"

# Re-export resolution task type constants for handlers and tests.
assert RESOLVE_PERSON_ENTITY_TASK_TYPE == "resolve_person_entity"
assert RECONSIDER_PERSON_ENTITY_TASK_TYPE == "reconsider_person_entity"

# Bumped when inspection request identity or capability adjudication changes.
ADAPTER_VERSION = 1
CAPABILITY_CONTRACT_VERSION = 1

# Bumped when detection material identity or generation request identity changes.
DETECTION_ADAPTER_VERSION = 1

SUPERSEDED_REASON = "stale model inspection"
SUPERSEDED_DETECTION_REASON = "stale person detection material"
UNSUPPORTED_STRICT_REASON = "unsupported strict structured output"
PRICING_REQUIRED_REASON = "pricing required under hard budget"
INSUFFICIENT_INPUT_RATIONALE = "empty title and summary"
MALFORMED_DETECTION_DETAIL = "invalid detection output"
MISSING_INSPECTION_DETAIL = "compatible model inspection is missing"
MISSING_PRICING_DETAIL = "usable unit pricing is required under a hard budget"
NEGATIVE_PRICING_DETAIL = "unit pricing must not be negative"
OVERFLOW_PRICING_DETAIL = "worst-case reservation overflows integer nano-USD"
MISSING_SOURCE_DETAIL = "source item is missing"
EMPTY_SOURCE_DETAIL = "source item has empty title and summary"

# SQLite INTEGER / attempt reservation bound (signed 64-bit).
_MAX_SQLITE_INTEGER = (1 << 63) - 1

_PERMANENT_PREFLIGHT_CATEGORIES = frozenset(
    {
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.ACCESS_DENIED,
    }
)

# Re-export the repository constant so handlers and tests share one name.
assert DETECT_PEOPLE_TASK_TYPE == "detect_people"


def routing_fingerprint(routing: ProviderRoutingConfig) -> str:
    """SHA-256 of the routing/privacy policy that generations must honour."""
    canonical = json.dumps(
        {
            "allow_fallbacks": routing.allow_fallbacks,
            "data_collection": routing.data_collection,
            "require_parameters": True,
            "zdr": routing.zdr,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _inspection_work_fingerprint(
    *,
    run_id: int,
    model_id: str,
    routing_fp: str,
) -> str:
    canonical = json.dumps(
        {
            "adapter_version": ADAPTER_VERSION,
            "capability_contract_version": CAPABILITY_CONTRACT_VERSION,
            "model": model_id,
            "routing_fingerprint": routing_fp,
            "run_id": run_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _has_usable_untriaged_source_item(connection: sqlite3.Connection) -> bool:
    """True when at least one source item needs generation, not empty-input."""
    row = connection.execute(
        """
        SELECT 1 AS present
          FROM source_item
         WHERE current_triage_observation_id IS NULL
           AND (
                (title_text IS NOT NULL AND length(title_text) > 0)
             OR (summary_text IS NOT NULL AND length(summary_text) > 0)
           )
         LIMIT 1
        """
    ).fetchone()
    return row is not None


def _has_active_work(
    connection: sqlite3.Connection, *, task_types: tuple[str, ...]
) -> bool:
    if not task_types:
        return False
    placeholders = ",".join("?" for _ in task_types)
    row = connection.execute(
        f"""
        SELECT 1 AS present
          FROM work_item
         WHERE task_type IN ({placeholders})
           AND state IN ('pending', 'deferred', 'running')
         LIMIT 1
        """,
        task_types,
    ).fetchone()
    return row is not None


def _has_active_detect_work(connection: sqlite3.Connection) -> bool:
    return _has_active_work(connection, task_types=(DETECT_PEOPLE_TASK_TYPE,))


def _has_active_resolve_or_reconsider_work(connection: sqlite3.Connection) -> bool:
    return _has_active_work(
        connection,
        task_types=(
            RESOLVE_PERSON_ENTITY_TASK_TYPE,
            RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        ),
    )


def _passage_ids_from_json(raw: str) -> tuple[str, ...]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return ()
    if not isinstance(parsed, list):
        return ()
    return tuple(str(item) for item in parsed if isinstance(item, str) and item)


def _signal_category(raw: str) -> AttentionCategory | CautionCategory:
    try:
        return AttentionCategory(raw)
    except ValueError:
        return CautionCategory(raw)


def _identity_facts_for_mention(
    connection: sqlite3.Connection, *, person_mention_id: int
) -> tuple[IdentityFact, ...]:
    facts: list[IdentityFact] = []
    for row in connection.execute(
        """
        SELECT local_id, kind, value, supporting_passage_ids_json
          FROM mention_identity_fact
         WHERE person_mention_id = ?
         ORDER BY local_id
        """,
        (person_mention_id,),
    ):
        passages = _passage_ids_from_json(row["supporting_passage_ids_json"])
        if not passages:
            continue
        try:
            facts.append(
                IdentityFact(
                    local_id=row["local_id"],
                    kind=IdentityFactKind(row["kind"]),
                    value=row["value"],
                    supporting_passage_ids=passages,  # type: ignore[arg-type]
                )
            )
        except (TypeError, ValueError):
            continue
    return tuple(facts)


def _signals_for_mention(
    connection: sqlite3.Connection, *, person_mention_id: int
) -> tuple[GroundedSignal, ...]:
    signals: list[GroundedSignal] = []
    for row in connection.execute(
        """
        SELECT kind, category, claim, supporting_passage_ids_json, grounding
          FROM mention_signal
         WHERE person_mention_id = ?
         ORDER BY ordinal
        """,
        (person_mention_id,),
    ):
        passages = _passage_ids_from_json(row["supporting_passage_ids_json"])
        if not passages:
            continue
        try:
            signals.append(
                GroundedSignal(
                    kind=SignalKind(row["kind"]),
                    category=_signal_category(str(row["category"])),
                    claim=row["claim"],
                    supporting_passage_ids=passages,  # type: ignore[arg-type]
                    grounding=SignalGrounding(row["grounding"]),
                )
            )
        except (TypeError, ValueError):
            continue
    return tuple(signals)


def is_resolution_eligible_mention(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> bool:
    """K24: research/uncertain, usable match_key, no ER at current fingerprint.

    Shared by seed, ``models_needed_for_run``, digest, and status. Mentions with
    only ``person_id IS NULL`` are not sufficient — skipped and permanently
    failed rows keep ``person_id`` null forever.
    """
    del profile  # reserved for ensure_resolution_for_mention parity
    row = connection.execute(
        """
        SELECT exact_name, search_name, outcome
          FROM person_mention
         WHERE id = ?
        """,
        (person_mention_id,),
    ).fetchone()
    if row is None:
        return False
    outcome = row["outcome"]
    if outcome not in {"research", "uncertain"}:
        return False
    exact_name = row["exact_name"]
    if exact_name is None or not str(exact_name).strip():
        return False
    if not match_key(str(exact_name)):
        return False
    search_name = row["search_name"] if row["search_name"] is not None else exact_name
    fingerprint = first_pass_task_fingerprint(
        person_mention_id=person_mention_id,
        mention_outcome=str(outcome),
        exact_name=str(exact_name),
        search_name=str(search_name),
        identity_facts=_identity_facts_for_mention(
            connection, person_mention_id=person_mention_id
        ),
        signals=_signals_for_mention(connection, person_mention_id=person_mention_id),
        config=config.tasks.resolve_person_entity,
    )
    existing = load_er_by_mention_fingerprint(
        connection,
        person_mention_id=person_mention_id,
        task_fingerprint=fingerprint,
    )
    return existing is None


def _has_resolution_eligible_mentions(
    connection: sqlite3.Connection, *, config: MainConfig
) -> bool:
    """True when any research/uncertain mention still lacks current-fingerprint ER.

    Uses the same material filters as :func:`is_resolution_eligible_mention`
    without requiring a domain profile (fingerprint does not include profile).
    """
    rows = connection.execute(
        """
        SELECT id
          FROM person_mention
         WHERE outcome IN ('research', 'uncertain')
           AND length(trim(exact_name)) > 0
         ORDER BY id
        """
    ).fetchall()
    # Minimal profile stand-in is unused by the predicate body.
    for row in rows:
        if is_resolution_eligible_mention(
            connection,
            person_mention_id=int(row["id"]),
            config=config,
            profile=_unused_profile_for_eligibility(),
        ):
            return True
    return False


def count_resolution_eligible_mentions(
    connection: sqlite3.Connection, *, config: MainConfig
) -> int:
    """Corpus count of mentions still K24-eligible for first-pass resolution.

    Skipped and permanently failed ERs close eligibility for their fingerprint;
    they are not counted even when ``person_id`` remains NULL.
    """
    rows = connection.execute(
        """
        SELECT id
          FROM person_mention
         WHERE outcome IN ('research', 'uncertain')
           AND length(trim(exact_name)) > 0
         ORDER BY id
        """
    ).fetchall()
    profile = _unused_profile_for_eligibility()
    total = 0
    for row in rows:
        if is_resolution_eligible_mention(
            connection,
            person_mention_id=int(row["id"]),
            config=config,
            profile=profile,
        ):
            total += 1
    return total


def _unused_profile_for_eligibility() -> DomainProfileConfig:
    """Placeholder profile: K24 fingerprint does not read domain profile fields."""
    return DomainProfileConfig(
        schema_version=1,
        key="eligibility-check",
        label="Eligibility check",
        language="en",
        attention_examples={},
    )


def models_needed_for_run(
    connection: sqlite3.Connection, run_id: int, config: MainConfig
) -> tuple[str, ...]:
    """Exact models that have dependent work this run (detect/resolve/match)."""
    del run_id  # reserved: active-work queries are run-global for pending state
    needed: list[str] = []
    detect_model = config.tasks.detect_people.model
    resolve_model = config.tasks.resolve_person_entity.model
    match_model = config.tasks.match_wikipedia_identity.model
    if _has_usable_untriaged_source_item(connection) or _has_active_detect_work(
        connection
    ):
        needed.append(detect_model)
    if _has_resolution_eligible_mentions(
        connection, config=config
    ) or _has_active_resolve_or_reconsider_work(connection):
        needed.append(resolve_model)
    if _wikipedia_match_model_needed(connection, config=config):
        needed.append(match_model)
    return tuple(dict.fromkeys(needed))


def task_types_for_model(config: MainConfig, model_id: str) -> tuple[str, ...]:
    """Task types that depend on a successful inspection of ``model_id``."""
    types: list[str] = []
    if config.tasks.detect_people.model == model_id:
        types.append(DETECT_PEOPLE_TASK_TYPE)
    if config.tasks.resolve_person_entity.model == model_id:
        types.append(RESOLVE_PERSON_ENTITY_TASK_TYPE)
        types.append(RECONSIDER_PERSON_ENTITY_TASK_TYPE)
    if config.tasks.match_wikipedia_identity.model == model_id:
        # HTTP MediaWiki kinds have no model ready gate (K21).
        types.append(MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE)
    return tuple(types)


def _wikipedia_match_model_needed(
    connection: sqlite3.Connection, *, config: MainConfig
) -> bool:
    """K21 match-model inspection arming (lazy import avoids package cycle)."""
    # Import inside the call: wikipedia.service imports ensure_model_inspections.
    from notable_person_finder.wikipedia.service import wikipedia_match_model_needed

    return wikipedia_match_model_needed(connection, config=config)


def ensure_model_inspections_for_run(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    now: str,
) -> int:
    """Schedule current-run ``inspect_model`` work for every needed model.

    One work item per ``(run_id, model_id, routing_fingerprint)``. Supersedes
    active inspect work whose fingerprint is not among the currently needed
    set so prior-run or stale-model preflights do not linger. Returns the
    number of models for which inspection work is ensured.
    """
    needed = models_needed_for_run(connection, run_id, config)
    if not needed:
        return 0

    routing_fp = routing_fingerprint(config.openrouter.routing)
    needed_fingerprints = {
        _inspection_work_fingerprint(
            run_id=run_id, model_id=model_id, routing_fp=routing_fp
        )
        for model_id in needed
    }

    stale = connection.execute(
        """
        SELECT fingerprint
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchall()
    for row in stale:
        if row["fingerprint"] in needed_fingerprints:
            continue
        repository.supersede_work(
            connection,
            task_type=INSPECT_MODEL_TASK_TYPE,
            fingerprint=row["fingerprint"],
            run_id=run_id,
            now=now,
            reason=SUPERSEDED_REASON,
        )

    for model_id in needed:
        fingerprint = _inspection_work_fingerprint(
            run_id=run_id, model_id=model_id, routing_fp=routing_fp
        )
        repository.schedule_work(
            connection,
            task_type=INSPECT_MODEL_TASK_TYPE,
            subject_kind=SUBJECT_KIND_MODEL,
            subject_id=None,
            fingerprint=fingerprint,
            required=True,
            priority=INSPECT_MODEL_PRIORITY,
            eligible_at=now,
            run_id=run_id,
            now=now,
        )
    return len(needed)


def ensure_model_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    now: str,
) -> int:
    """Schedule inspections for every model that currently has dependent work.

    Backward-compatible entry point used by detection scheduling. Delegates to
    :func:`ensure_model_inspections_for_run` so a fully-triaged resolve backlog
    still receives resolve-model preflight when this helper is the only seed.
    """
    return ensure_model_inspections_for_run(
        connection, run_id=run_id, config=config, now=now
    )


def inspection_ready(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    model_id: str | None = None,
) -> bool:
    """True when this run has a compatible inspection for ``model_id``.

    When ``model_id`` is omitted, the detect-people model is used (3b1 callers).
    Under a hard OpenRouter budget, usable unit pricing is also required so
    generation prepare can compute a worst-case reservation.
    """
    resolved_model_id = (
        config.tasks.detect_people.model if model_id is None else model_id
    )
    routing_fp = routing_fingerprint(config.openrouter.routing)
    inspection = load_model_inspection(
        connection,
        run_id=run_id,
        configured_model_id=resolved_model_id,
        routing_fingerprint=routing_fp,
    )
    if inspection is None:
        return False
    if inspection.compatibility != "compatible":
        return False
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    return not (hard_budget and not inspection.pricing_usable)


@dataclass(frozen=True, slots=True)
class _InspectionCall:
    """Application-thread inputs for the worker-thread inspect call."""

    model_id: str
    hard_budget: bool


@dataclass(frozen=True, slots=True)
class _InspectionPersist:
    """Parsed inspection facts carried from execute to persist."""

    result: ModelInspectionResult
    compatibility: str
    pricing_usable: bool
    failure_category: str | None
    reason: str | None


def _pricing_usable(result: ModelInspectionResult) -> bool:
    return (
        result.prompt_unit_price_nano_usd is not None
        and result.completion_unit_price_nano_usd is not None
    )


def _adjudicate(
    result: ModelInspectionResult, *, hard_budget: bool
) -> _InspectionPersist:
    pricing_usable = _pricing_usable(result)
    if not result.supports_strict_structured_output:
        return _InspectionPersist(
            result=result,
            compatibility="incompatible",
            pricing_usable=pricing_usable,
            failure_category=str(FailureCategory.UNSUPPORTED_CAPABILITY),
            reason=UNSUPPORTED_STRICT_REASON,
        )
    if hard_budget and not pricing_usable:
        return _InspectionPersist(
            result=result,
            compatibility="incompatible",
            pricing_usable=False,
            failure_category=str(FailureCategory.UNSUPPORTED_CAPABILITY),
            reason=PRICING_REQUIRED_REASON,
        )
    return _InspectionPersist(
        result=result,
        compatibility="compatible",
        pricing_usable=pricing_usable,
        failure_category=None,
        reason=None,
    )


def _detection_prompt_and_schema_hashes() -> tuple[str, str, int]:
    """Stable hashes for permanent-preflight failed triage observations."""
    prompt = (
        resources.files("notable_person_finder.people")
        .joinpath("prompts", "detect_people.md")
        .read_text(encoding="utf-8")
    )
    schema_envelope = json.dumps(
        {
            "schema": detection_schema(),
            "schema_version": DETECTION_SCHEMA_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    schema_hash = hashlib.sha256(schema_envelope.encode("utf-8")).hexdigest()
    return prompt_hash, schema_hash, DETECTION_SCHEMA_VERSION


def _attempt_context(
    connection: sqlite3.Connection, work_item_id: int
) -> tuple[int, int, str]:
    row = connection.execute(
        """
        SELECT id, run_id, started_at FROM attempt
         WHERE work_item_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (work_item_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"no attempt row for work item {work_item_id}; cannot persist inspection"
        )
    return int(row["id"]), int(row["run_id"]), row["started_at"]


def _stored_prices(
    result: ModelInspectionResult, *, pricing_usable: bool
) -> tuple[int | None, int | None]:
    if not pricing_usable:
        return None, None
    return result.prompt_unit_price_nano_usd, result.completion_unit_price_nano_usd


def _insert_inspection_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    routing_fp: str,
    payload: _InspectionPersist,
    inspected_at: str,
) -> int:
    prompt_price, completion_price = _stored_prices(
        payload.result, pricing_usable=payload.pricing_usable
    )
    return insert_model_inspection(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        configured_model_id=payload.result.configured_model_id,
        resolved_model_id=payload.result.resolved_model_id,
        routing_fingerprint=routing_fp,
        supported_parameters_json=json.dumps(
            list(payload.result.supported_parameters),
            separators=(",", ":"),
        ),
        supports_strict_structured_output=(
            payload.result.supports_strict_structured_output
        ),
        pricing_usable=payload.pricing_usable,
        prompt_unit_price_nano_usd=prompt_price,
        completion_unit_price_nano_usd=completion_price,
        compatibility=payload.compatibility,
        inspected_at=inspected_at,
    )


def _settle_dependents(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_id: str,
    config: MainConfig,
    failure_category: str,
    rationale: str,
    now: str,
    model_inspection_id: int | None = None,
) -> None:
    task_types = task_types_for_model(config, model_id)
    if not task_types:
        return
    detect_prompt_hash: str | None = None
    detect_schema_hash: str | None = None
    detect_schema_version: int | None = None
    resolve_prompt_hash: str | None = None
    resolve_schema_hash: str | None = None
    resolve_schema_version: int | None = None
    if DETECT_PEOPLE_TASK_TYPE in task_types:
        detect_prompt_hash, detect_schema_hash, detect_schema_version = (
            _detection_prompt_and_schema_hashes()
        )
    if (
        RESOLVE_PERSON_ENTITY_TASK_TYPE in task_types
        or RECONSIDER_PERSON_ENTITY_TASK_TYPE in task_types
    ):
        resolve_prompt_hash, resolve_schema_hash, resolve_schema_version = (
            resolution_prompt_and_schema_hashes()
        )
    settle_active_tasks_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=model_inspection_id,
        model_id=model_id,
        failure_category=failure_category,
        rationale=rationale,
        task_types=task_types,
        now=now,
        detect_prompt_hash=detect_prompt_hash,
        detect_schema_hash=detect_schema_hash,
        detect_schema_version=detect_schema_version,
        resolve_prompt_hash=resolve_prompt_hash,
        resolve_schema_hash=resolve_schema_hash,
        resolve_schema_version=resolve_schema_version,
    )


def _model_id_for_inspection_work(
    work_item: WorkItem,
    *,
    run_id: int,
    config: MainConfig,
) -> str:
    """Match the work-item fingerprint to a configured model for this run."""
    routing_fp = routing_fingerprint(config.openrouter.routing)
    candidates = dict.fromkeys(
        (
            config.tasks.detect_people.model,
            config.tasks.resolve_person_entity.model,
            config.tasks.match_wikipedia_identity.model,
        )
    )
    for model_id in candidates:
        expected = _inspection_work_fingerprint(
            run_id=run_id, model_id=model_id, routing_fp=routing_fp
        )
        if work_item.fingerprint == expected:
            return model_id
    raise ValueError(
        f"inspect_model work item {work_item.id} fingerprint does not match "
        f"any configured model for run {run_id}"
    )


def _execute_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    """Worker-thread phase in a scope that holds no connection."""

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        if not isinstance(prepared, _InspectionCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=INSPECT_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.inspect_model(
            ModelInspectionRequest(model_id=prepared.model_id)
        )
        payload = _adjudicate(result, hard_budget=prepared.hard_budget)
        if payload.compatibility == "compatible":
            return TaskOutcome(
                state=WorkState.SUCCEEDED,
                reason=None,
                payload=payload,
            )
        return TaskOutcome(
            state=WorkState.FAILED_PERMANENT,
            reason=payload.reason,
            payload=payload,
        )

    return execute


def _persist_for(
    connection: sqlite3.Connection,
    *,
    routing_fp: str,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _InspectionPersist):
            raise RuntimeError(
                f"unexpected inspection payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, inspected_at = _attempt_context(connection, work_item.id)
        inspection_id = _insert_inspection_row(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            routing_fp=routing_fp,
            payload=payload,
            inspected_at=inspected_at,
        )
        if (
            payload.compatibility == "incompatible"
            and payload.failure_category is not None
            and payload.reason is not None
        ):
            _settle_dependents(
                connection,
                run_id=run_id,
                attempt_id=attempt_id,
                model_id=payload.result.configured_model_id,
                config=config,
                failure_category=payload.failure_category,
                rationale=payload.reason,
                now=inspected_at,
                model_inspection_id=inspection_id,
            )

    return persist


def _persist_failure_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        # Transient exhaustion leaves dependents pending and unclaimable via
        # readiness; only permanent preflight categories settle them here.
        if failure.category not in _PERMANENT_PREFLIGHT_CATEGORIES:
            return
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        model_id = _model_id_for_inspection_work(
            work_item, run_id=run_id, config=config
        )
        _settle_dependents(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            model_id=model_id,
            config=config,
            failure_category=str(failure.category),
            rationale=str(failure.category),
            now=observed_at,
            model_inspection_id=None,
        )

    return persist_failure


def build_inspection_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
) -> TaskHandler:
    """The ``inspect_model`` handler: one capability preflight, two threads.

    ``prepare`` resolves the model id from the work-item fingerprint so one
    handler instance can preflight both the detect and resolve models.
    """
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname

    def prepare(work_item: WorkItem) -> TaskPreparation:
        run_id = _claimed_run_id(connection, work_item.id)
        model_id = _model_id_for_inspection_work(
            work_item, run_id=run_id, config=config
        )
        return TaskPreparation(
            payload=_InspectionCall(model_id=model_id, hard_budget=hard_budget),
            reserved_nano_usd=0,
        )

    def destination_host(work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=INSPECT_MODEL_TASK_TYPE,
        provider=PROVIDER,
        operation=INSPECT_OPERATION,
        execute=_execute_for(client),
        prepare=prepare,
        persist=_persist_for(connection, routing_fp=routing_fp, config=config),
        persist_failure=_persist_failure_for(connection, config=config),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=None,
    )


# ---------------------------------------------------------------------------
# Detection scheduling and generation handler
# ---------------------------------------------------------------------------


def _normalized_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _has_usable_text(record: SourceItemRecord) -> bool:
    return (
        _normalized_text(record.title_text) is not None
        or _normalized_text(record.summary_text) is not None
    )


def _feed_from_record(record: SourceItemRecord) -> FeedConfig:
    # Detection input only consumes key and label; a stable public URL satisfies
    # FeedConfig validation without contacting the network.
    return FeedConfig(
        key=record.feed_key,
        label=record.feed_label,
        url="https://example.com/feed",
    )


def _detection_material_fingerprint(
    record: SourceItemRecord,
    *,
    config: MainConfig,
    profile: DomainProfileConfig,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
) -> str:
    detect = config.tasks.detect_people
    parameters = detect.parameters
    attention = {
        category: list(examples)
        for category, examples in sorted(profile.attention_examples.items())
    }
    canonical = json.dumps(
        {
            "adapter_version": DETECTION_ADAPTER_VERSION,
            "domain_profile_key": profile.key,
            "domain_profile_label": profile.label,
            "domain_profile_language": profile.language,
            "domain_profile_version": profile.schema_version,
            "attention_examples": attention,
            "max_completion_tokens": detect.max_completion_tokens,
            "max_input_tokens": detect.max_input_tokens,
            "max_people": detect.max_people,
            "max_summary_characters": detect.max_summary_characters,
            "max_title_characters": detect.max_title_characters,
            "model": detect.model,
            "parameters": {
                "reasoning_effort": parameters.reasoning_effort,
                "temperature": parameters.temperature,
                "top_p": parameters.top_p,
            },
            "prompt_hash": prompt_hash,
            "schema_hash": schema_hash,
            "schema_version": schema_version,
            "source_item_id": record.id,
            "summary_text": record.summary_text,
            "task": DETECT_PEOPLE_TASK_TYPE,
            "title_text": record.title_text,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _point_source_item_current(
    connection: sqlite3.Connection, *, source_item_id: int, observation_id: int
) -> None:
    changed = connection.execute(
        """
        UPDATE source_item
           SET current_triage_observation_id = ?
         WHERE id = ?
        """,
        (observation_id, source_item_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"source item {source_item_id} is missing; cannot set current triage"
        )


def _supersede_stale_detection_work(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    fingerprint: str,
    run_id: int,
    now: str,
) -> None:
    stale = connection.execute(
        """
        SELECT fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id = ?
           AND state IN ('pending', 'deferred')
           AND fingerprint != ?
         ORDER BY id
        """,
        (
            DETECT_PEOPLE_TASK_TYPE,
            SUBJECT_KIND_SOURCE_ITEM,
            source_item_id,
            fingerprint,
        ),
    ).fetchall()
    for row in stale:
        repository.supersede_work(
            connection,
            task_type=DETECT_PEOPLE_TASK_TYPE,
            fingerprint=row["fingerprint"],
            run_id=run_id,
            now=now,
            reason=SUPERSEDED_DETECTION_REASON,
        )


def _write_insufficient_input(
    connection: sqlite3.Connection,
    *,
    record: SourceItemRecord,
    run_id: int,
    fingerprint: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    now: str,
    profile: DomainProfileConfig,
    config: MainConfig,
) -> None:
    detection_input = build_detection_input(
        record,
        _feed_from_record(record),
        profile,
        config.tasks.detect_people,
    )
    rendered = render_detection_request(detection_input)
    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        insert_insufficient_input_observation(
            connection,
            source_item_id=record.id,
            run_id=run_id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=fingerprint,
            observed_at=now,
            rationale=INSUFFICIENT_INPUT_RATIONALE,
        )
    except BaseException:
        if owns_transaction:
            connection.rollback()
        raise
    else:
        if owns_transaction:
            connection.commit()


def _schedule_one_source_item(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
) -> bool:
    """Schedule detection or write insufficient_input. Returns True if usable."""
    record = load_source_item_record(connection, source_item_id=source_item_id)
    if record is None:
        raise LookupError(f"source item {source_item_id} is missing")

    fingerprint = _detection_material_fingerprint(
        record,
        config=config,
        profile=profile,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
    )

    existing = load_triage_observation_by_fingerprint(
        connection,
        source_item_id=source_item_id,
        task_fingerprint=fingerprint,
    )
    if existing is not None:
        # Reuse must still retire other active fingerprints. A material
        # flip-flop can leave pending/deferred work for a different fingerprint
        # claimable even when the current material already has a durable
        # observation; prepare rebuilds from current text but stamps the work
        # item's (stale) fingerprint.
        _supersede_stale_detection_work(
            connection,
            source_item_id=source_item_id,
            fingerprint=fingerprint,
            run_id=run_id,
            now=now,
        )
        if record.current_triage_observation_id != existing.id:
            owns_transaction = not connection.in_transaction
            if owns_transaction:
                connection.execute("BEGIN IMMEDIATE")
            try:
                _point_source_item_current(
                    connection,
                    source_item_id=source_item_id,
                    observation_id=existing.id,
                )
            except BaseException:
                if owns_transaction:
                    connection.rollback()
                raise
            else:
                if owns_transaction:
                    connection.commit()
        return False

    if not _has_usable_text(record):
        _write_insufficient_input(
            connection,
            record=record,
            run_id=run_id,
            fingerprint=fingerprint,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            now=now,
            profile=profile,
            config=config,
        )
        return False

    _supersede_stale_detection_work(
        connection,
        source_item_id=source_item_id,
        fingerprint=fingerprint,
        run_id=run_id,
        now=now,
    )
    repository.schedule_work(
        connection,
        task_type=DETECT_PEOPLE_TASK_TYPE,
        subject_kind=SUBJECT_KIND_SOURCE_ITEM,
        subject_id=source_item_id,
        fingerprint=fingerprint,
        required=True,
        priority=DETECT_PEOPLE_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )
    return True


def schedule_source_items(
    connection: sqlite3.Connection,
    *,
    source_item_ids: Sequence[int],
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
) -> None:
    """Schedule detection (or insufficient_input) for the named source items.

    Empty normalized title and summary become durable ``insufficient_input``
    observations with no work item and no attempt. Usable items schedule
    required ``detect_people`` work idempotently under a material fingerprint
    and ensure current-run model inspection exists.
    """
    prompt_hash, schema_hash, schema_version = _detection_prompt_and_schema_hashes()
    needs_inspection = False
    for source_item_id in source_item_ids:
        if _schedule_one_source_item(
            connection,
            source_item_id=source_item_id,
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
        ):
            needs_inspection = True
    if needs_inspection:
        ensure_model_inspection(connection, run_id=run_id, config=config, now=now)


def seed_untriaged(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
) -> None:
    """Backfill every source item that still lacks a current triage observation."""
    schedule_source_items(
        connection,
        source_item_ids=list_untriaged_source_item_ids(connection),
        run_id=run_id,
        config=config,
        profile=profile,
        now=now,
    )


def _checked_product(unit_price_nano_usd: int, tokens: int) -> int:
    """Ceiling unit_price * tokens as a non-negative SQLite integer."""
    if unit_price_nano_usd < 0:
        raise ValueError(NEGATIVE_PRICING_DETAIL)
    if tokens < 0:
        raise ValueError("token bound must not be negative")
    try:
        product = (Decimal(unit_price_nano_usd) * Decimal(tokens)).to_integral_value(
            rounding=ROUND_CEILING
        )
    except InvalidOperation as error:
        raise ValueError(OVERFLOW_PRICING_DETAIL) from error
    if product < 0 or product > _MAX_SQLITE_INTEGER:
        raise ValueError(OVERFLOW_PRICING_DETAIL)
    return int(product)


def _worst_case_reservation_nano_usd(
    *,
    prompt_unit_price_nano_usd: int,
    completion_unit_price_nano_usd: int,
    max_input_tokens: int,
    max_completion_tokens: int,
) -> int:
    prompt_cost = _checked_product(prompt_unit_price_nano_usd, max_input_tokens)
    completion_cost = _checked_product(
        completion_unit_price_nano_usd, max_completion_tokens
    )
    total = prompt_cost + completion_cost
    if total > _MAX_SQLITE_INTEGER:
        raise ValueError(OVERFLOW_PRICING_DETAIL)
    return total


@dataclass(frozen=True, slots=True)
class _DetectionCall:
    """Application-thread inputs for the worker-thread generation call."""

    request: StructuredGenerationRequest
    detection_input: DetectionInput
    source_item_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    input_truncated: bool


@dataclass(frozen=True, slots=True)
class _DetectionPersist:
    """Prevalidated detection output and provenance for application-thread persist."""

    output: DetectionOutput
    source_item_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    input_truncated: bool


def _claimed_run_id(connection: sqlite3.Connection, work_item_id: int) -> int:
    row = connection.execute(
        "SELECT claimed_by_run_id FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is None or row["claimed_by_run_id"] is None:
        raise RuntimeError(
            f"work item {work_item_id} has no claimed run; cannot prepare detection"
        )
    return int(row["claimed_by_run_id"])


def _execute_detection_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    """Worker-thread generation + domain validation; no SQLite in freevars."""

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        if not isinstance(prepared, _DetectionCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.generate_structured(prepared.request)
        raw_text = result.raw_text
        if result.refusal:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_DETECTION_DETAIL,
            )
        try:
            output = validate_detection_output(raw_text, prepared.detection_input)
        except DetectionValidationError:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_DETECTION_DETAIL,
            ) from None
        del raw_text
        payload = _DetectionPersist(
            output=output,
            source_item_id=prepared.source_item_id,
            model_inspection_id=prepared.model_inspection_id,
            canonical_supplied_input_json=prepared.canonical_supplied_input_json,
            prompt_hash=prepared.prompt_hash,
            schema_hash=prepared.schema_hash,
            schema_version=prepared.schema_version,
            task_fingerprint=prepared.task_fingerprint,
            input_truncated=prepared.input_truncated,
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


def _persist_detection_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _DetectionPersist):
            raise RuntimeError(
                f"unexpected detection payload type: {type(payload).__name__}"
            )
        # Persist only prevalidated output. Domain validation must have run in
        # execute; re-validating here would be a second paid-call gate.
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        observation_id = insert_completed_observation(
            connection,
            source_item_id=payload.source_item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=payload.model_inspection_id,
            output=payload.output,
            canonical_supplied_input_json=payload.canonical_supplied_input_json,
            prompt_hash=payload.prompt_hash,
            schema_hash=payload.schema_hash,
            schema_version=payload.schema_version,
            task_fingerprint=payload.task_fingerprint,
            input_truncated=payload.input_truncated,
            observed_at=observed_at,
        )
        # Same settlement transaction: failure rolls back triage + identity.
        schedule_resolution_for_observation(
            connection,
            observation_id=observation_id,
            run_id=run_id,
            config=config,
            profile=profile,
            now=observed_at,
        )

    return persist


def _persist_detection_failure_for(
    connection: sqlite3.Connection,
    *,
    profile: DomainProfileConfig,
    config: MainConfig,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        # complete_work updates state before domain_writes; only permanent
        # settlements mint a failed triage observation. Transient re-arm uses
        # pending without this callback; deferred exhaustion must not invent
        # a semantic observation.
        state_row = connection.execute(
            "SELECT state, subject_id, fingerprint FROM work_item WHERE id = ?",
            (work_item.id,),
        ).fetchone()
        if state_row is None or state_row["state"] != "failed_permanent":
            return
        if state_row["subject_id"] is None:
            return
        source_item_id = int(state_row["subject_id"])
        task_fingerprint = state_row["fingerprint"]
        existing = load_triage_observation_by_fingerprint(
            connection,
            source_item_id=source_item_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            current = load_current_triage_observation(
                connection, source_item_id=source_item_id
            )
            if current is None or current.id != existing.id:
                _point_source_item_current(
                    connection,
                    source_item_id=source_item_id,
                    observation_id=existing.id,
                )
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        record = load_source_item_record(connection, source_item_id=source_item_id)
        if record is None:
            raise RuntimeError(
                f"source item {source_item_id} missing during detection failure persist"
            )
        prompt_hash, schema_hash, schema_version = _detection_prompt_and_schema_hashes()
        try:
            detection_input = build_detection_input(
                record,
                _feed_from_record(record),
                profile,
                config.tasks.detect_people,
            )
            rendered = render_detection_request(detection_input)
            canonical = rendered.canonical_input_json
            prompt_hash = rendered.prompt_hash
            schema_hash = rendered.schema_hash
            schema_version = rendered.schema_version
            input_truncated = detection_input.view.input_truncated
        except Exception:
            canonical = "{}"
            input_truncated = False

        routing_fp = routing_fingerprint(config.openrouter.routing)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=config.tasks.detect_people.model,
            routing_fingerprint=routing_fp,
        )
        model_inspection_id = None if inspection is None else inspection.id
        insert_failed_observation(
            connection,
            source_item_id=source_item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=model_inspection_id,
            failure_category=str(failure.category),
            canonical_supplied_input_json=canonical,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=task_fingerprint,
            input_truncated=input_truncated,
            observed_at=observed_at,
            rationale=str(failure.category),
        )

    return persist_failure


def build_detection_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> TaskHandler:
    """The ``detect_people`` handler: one structured generation, two threads."""
    detect = config.tasks.detect_people
    model_id = detect.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname
    parameters = detect.parameters

    def ready(claimed_run_id: int) -> bool:
        return inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=model_id,
        )

    def prepare(work_item: WorkItem) -> TaskPreparation:
        if work_item.subject_id is None:
            raise ValueError(MISSING_SOURCE_DETAIL)
        source_item_id = work_item.subject_id
        record = load_source_item_record(connection, source_item_id=source_item_id)
        if record is None:
            raise ValueError(MISSING_SOURCE_DETAIL)
        if not _has_usable_text(record):
            # Schedule-time should have terminalled these; refuse the attempt.
            raise ValueError(EMPTY_SOURCE_DETAIL)

        run_id = _claimed_run_id(connection, work_item.id)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=model_id,
            routing_fingerprint=routing_fp,
        )
        if inspection is None or inspection.compatibility != "compatible":
            raise ValueError(MISSING_INSPECTION_DETAIL)

        detection_input = build_detection_input(
            record,
            _feed_from_record(record),
            profile,
            detect,
        )
        rendered = render_detection_request(detection_input)
        request = StructuredGenerationRequest(
            model_id=model_id,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name=DETECTION_SCHEMA_NAME,
            max_completion_tokens=detect.max_completion_tokens,
            temperature=parameters.temperature,
            top_p=parameters.top_p,
            reasoning_effort=parameters.reasoning_effort,
        )
        call = _DetectionCall(
            request=request,
            detection_input=detection_input,
            source_item_id=source_item_id,
            model_inspection_id=inspection.id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=rendered.prompt_hash,
            schema_hash=rendered.schema_hash,
            schema_version=rendered.schema_version,
            task_fingerprint=work_item.fingerprint,
            input_truncated=detection_input.view.input_truncated,
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
            max_input_tokens=detect.max_input_tokens,
            max_completion_tokens=detect.max_completion_tokens,
        )
        return TaskPreparation(payload=call, reserved_nano_usd=reserved)

    def destination_host(work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=_execute_detection_for(client),
        prepare=prepare,
        persist=_persist_detection_for(connection, config=config, profile=profile),
        persist_failure=_persist_detection_failure_for(
            connection, profile=profile, config=config
        ),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )


# ---------------------------------------------------------------------------
# First-pass entity resolution (created_new + model path)
# ---------------------------------------------------------------------------


def _retrieve_candidates_for_mention(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    config: MainConfig,
) -> tuple[CandidatePerson, ...]:
    resolve = config.tasks.resolve_person_entity
    return retrieve_candidates(
        connection,
        person_mention_id=person_mention_id,
        max_candidates=resolve.max_candidates,
        max_facts_per_candidate=resolve.max_facts_per_candidate,
        max_names_per_candidate=resolve.max_names_per_candidate,
    )


def _candidate_ids_json(candidate_ids: Sequence[int]) -> str:
    return json.dumps(list(candidate_ids), separators=(",", ":"))


def _to_resolve_candidates(
    candidates: Sequence[CandidatePerson],
) -> tuple[ResolveCandidate, ...]:
    result: list[ResolveCandidate] = []
    for candidate in candidates:
        names: list[ResolveCandidateName] = []
        for name in candidate.names:
            kind: SourcedNameKind
            if name.kind in {"professional", "display", "alias", "other", "mononym"}:
                kind = name.kind  # type: ignore[assignment]
            else:
                kind = "other"
            names.append(
                ResolveCandidateName(
                    exact_name=name.exact_name,
                    search_name=name.search_name,
                    match_key=name.match_key,
                    kind=kind,
                )
            )
        facts: list[ResolveCandidateFact] = []
        for fact in candidate.facts:
            try:
                kind_enum = IdentityFactKind(fact.kind)
            except ValueError:
                kind_enum = IdentityFactKind.OTHER
            facts.append(
                ResolveCandidateFact(
                    local_id=fact.local_id,
                    kind=kind_enum,
                    value=fact.value,
                )
            )
        result.append(
            ResolveCandidate(
                person_id=candidate.person_id,
                display_name=candidate.display_name,
                names=tuple(names),
                identity_facts=tuple(facts),
            )
        )
    return tuple(result)


def _mention_identity_fact_name_values(
    connection: sqlite3.Connection, *, person_mention_id: int
) -> tuple[str, ...]:
    return tuple(
        str(row["value"])
        for row in connection.execute(
            """
            SELECT value
              FROM mention_identity_fact
             WHERE person_mention_id = ? AND kind = 'name'
             ORDER BY local_id
            """,
            (person_mention_id,),
        )
    )


def _source_item_id_for_mention(
    connection: sqlite3.Connection, *, person_mention_id: int
) -> int:
    row = connection.execute(
        """
        SELECT t.source_item_id
          FROM person_mention AS m
          JOIN triage_observation AS t ON t.id = m.triage_observation_id
         WHERE m.id = ?
        """,
        (person_mention_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"person_mention {person_mention_id} is missing")
    return int(row["source_item_id"])


def _passages_for_source_item(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    config: MainConfig,
) -> tuple[DetectionPassage, ...]:
    record = load_source_item_record(connection, source_item_id=source_item_id)
    if record is None:
        return ()
    resolve = config.tasks.resolve_person_entity
    passages: list[DetectionPassage] = []
    title = _normalized_text(record.title_text)
    if title is not None:
        bounded = title[: resolve.max_title_characters] or title[:1]
        passages.append(
            DetectionPassage(
                id="p1",
                field="title",
                text=bounded,
                truncated=bounded != title,
            )
        )
    summary = _normalized_text(record.summary_text)
    if summary is not None:
        bounded = summary[: resolve.max_summary_characters] or summary[:1]
        passages.append(
            DetectionPassage(
                id="p2",
                field="summary",
                text=bounded,
                truncated=bounded != summary,
            )
        )
    return tuple(passages)


def _apply_peer_edges(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    reject_set: set[int],
    creating_mention_id: int,
    run_id: int,
    observation_id: int,
    config: MainConfig,
    now: str,
) -> None:
    """Open K17 name-matched peer edges after a person create (not reconsider)."""
    peers = scan_name_matched_peers(
        connection,
        person_id=person_id,
        reject_set=reject_set,
        max_candidates=config.tasks.resolve_person_entity.max_candidates,
        creating_mention_id=creating_mention_id,
    )
    for peer_id in peers:
        upsert_active_possible_same_person(
            connection,
            person_id_a=person_id,
            person_id_b=peer_id,
            run_id=run_id,
            created_by_observation_id=observation_id,
            now=now,
        )


def _supersede_stale_resolve_work(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    fingerprint: str,
    run_id: int,
    now: str,
) -> None:
    stale = connection.execute(
        """
        SELECT fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id = ?
           AND state IN ('pending', 'deferred')
           AND fingerprint != ?
         ORDER BY id
        """,
        (
            RESOLVE_PERSON_ENTITY_TASK_TYPE,
            SUBJECT_KIND_PERSON_MENTION,
            person_mention_id,
            fingerprint,
        ),
    ).fetchall()
    for row in stale:
        repository.supersede_work(
            connection,
            task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
            fingerprint=row["fingerprint"],
            run_id=run_id,
            now=now,
            reason=SUPERSEDED_DETECTION_REASON,
        )


def _linked_person_id_from_er(
    *,
    created_person_id: int | None,
    selected_person_id: int | None,
) -> int | None:
    if created_person_id is not None:
        return created_person_id
    if selected_person_id is not None:
        return selected_person_id
    return None


def _schedule_resolve_work(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    fingerprint: str,
    run_id: int,
    now: str,
) -> None:
    _supersede_stale_resolve_work(
        connection,
        person_mention_id=person_mention_id,
        fingerprint=fingerprint,
        run_id=run_id,
        now=now,
    )
    repository.schedule_work(
        connection,
        task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        subject_kind=SUBJECT_KIND_PERSON_MENTION,
        subject_id=person_mention_id,
        fingerprint=fingerprint,
        required=True,
        priority=RESOLVE_PERSON_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def _write_created_new(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    run_id: int,
    fingerprint: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    material_json: str,
    config: MainConfig,
    now: str,
) -> None:
    person_id = create_person_for_mention(
        connection,
        run_id=run_id,
        mention_id=person_mention_id,
        now=now,
    )
    er_id = insert_entity_resolution_observation(
        connection,
        person_mention_id=person_mention_id,
        person_relation_id=None,
        run_id=run_id,
        attempt_id=None,
        model_inspection_id=None,
        disposition="completed",
        semantic_outcome="created_new",
        selected_person_id=None,
        created_person_id=person_id,
        candidate_person_ids_json="[]",
        canonical_supplied_input_json=material_json,
        validated_output_json=CREATED_NEW_VALIDATED_OUTPUT_JSON,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
        task_fingerprint=fingerprint,
        rationale="empty candidate set",
        failure_category=None,
        observed_at=now,
    )
    point_mention_current_er(
        connection,
        person_mention_id=person_mention_id,
        observation_id=er_id,
        person_id=person_id,
    )
    # New person: no pre-existing edges (K21 — peer edges opened below do not
    # schedule reconsider in this settlement).
    pre_existing_relation_ids: frozenset[int] = frozenset()
    _apply_peer_edges(
        connection,
        person_id=person_id,
        reject_set=set(),
        creating_mention_id=person_mention_id,
        run_id=run_id,
        observation_id=er_id,
        config=config,
        now=now,
    )
    maybe_schedule_reconsideration_for_person(
        connection,
        person_id=person_id,
        pre_existing_relation_ids=pre_existing_relation_ids,
        run_id=run_id,
        config=config,
        now=now,
    )


def ensure_resolution_for_mention(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
) -> str:
    """Schedule-time first-pass resolution for one mention.

    Returns one of: ``created_new``, ``scheduled``, ``reused``, ``skipped``,
    ``ineligible``. Caller may hold an open transaction (detection persist);
    seed and prepare open a brief ``BEGIN IMMEDIATE`` when none is open.
    """
    del profile  # reserved for future domain-profile bounds
    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        result = _ensure_resolution_for_mention_body(
            connection,
            person_mention_id=person_mention_id,
            run_id=run_id,
            config=config,
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


def _ensure_resolution_for_mention_body(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    run_id: int,
    config: MainConfig,
    now: str,
) -> str:
    row = connection.execute(
        """
        SELECT exact_name, search_name, outcome, person_id
          FROM person_mention
         WHERE id = ?
        """,
        (person_mention_id,),
    ).fetchone()
    if row is None:
        return "ineligible"
    outcome = str(row["outcome"])
    if outcome not in {"research", "uncertain"}:
        return "ineligible"
    exact_name = row["exact_name"]
    if exact_name is None or not str(exact_name).strip():
        return "ineligible"
    exact_name_str = str(exact_name)
    search_name_str = (
        str(row["search_name"]) if row["search_name"] is not None else exact_name_str
    )

    resolve = config.tasks.resolve_person_entity
    prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
    identity_facts = _identity_facts_for_mention(
        connection, person_mention_id=person_mention_id
    )
    signals = _signals_for_mention(connection, person_mention_id=person_mention_id)
    fingerprint = first_pass_task_fingerprint(
        person_mention_id=person_mention_id,
        mention_outcome=outcome,
        exact_name=exact_name_str,
        search_name=search_name_str,
        identity_facts=identity_facts,
        signals=signals,
        config=resolve,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
    )
    material_json = first_pass_canonical_supplied_input_json(
        person_mention_id=person_mention_id,
        mention_outcome=outcome,
        exact_name=exact_name_str,
        search_name=search_name_str,
        identity_facts=identity_facts,
        signals=signals,
        config=resolve,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
    )

    existing = load_er_by_mention_fingerprint(
        connection,
        person_mention_id=person_mention_id,
        task_fingerprint=fingerprint,
    )
    if existing is not None:
        linked = _linked_person_id_from_er(
            created_person_id=existing.created_person_id,
            selected_person_id=existing.selected_person_id,
        )
        current = connection.execute(
            """
            SELECT current_entity_resolution_observation_id, person_id
              FROM person_mention
             WHERE id = ?
            """,
            (person_mention_id,),
        ).fetchone()
        if current is not None and (
            current["current_entity_resolution_observation_id"] != existing.id
            or (linked is not None and current["person_id"] != linked)
        ):
            point_mention_current_er(
                connection,
                person_mention_id=person_mention_id,
                observation_id=existing.id,
                person_id=linked,
            )
        return "reused"

    if not match_key(exact_name_str):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=person_mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="skipped",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json=material_json,
            validated_output_json=None,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=fingerprint,
            rationale=SKIPPED_MATCH_KEY_RATIONALE,
            failure_category=None,
            observed_at=now,
        )
        point_mention_current_er(
            connection,
            person_mention_id=person_mention_id,
            observation_id=er_id,
            person_id=None,
        )
        return "skipped"

    candidates = _retrieve_candidates_for_mention(
        connection, person_mention_id=person_mention_id, config=config
    )
    if not candidates:
        # K17 txn re-check before deterministic create.
        candidates = _retrieve_candidates_for_mention(
            connection, person_mention_id=person_mention_id, config=config
        )
    if not candidates:
        _write_created_new(
            connection,
            person_mention_id=person_mention_id,
            run_id=run_id,
            fingerprint=fingerprint,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            material_json=material_json,
            config=config,
            now=now,
        )
        return "created_new"

    _schedule_resolve_work(
        connection,
        person_mention_id=person_mention_id,
        fingerprint=fingerprint,
        run_id=run_id,
        now=now,
    )
    return "scheduled"


def schedule_resolution_for_observation(
    connection: sqlite3.Connection,
    *,
    observation_id: int,
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
) -> int:
    """Txn-neutral: ensure resolution for every mention on a triage observation.

    Must not call BEGIN/COMMIT; joins the caller's settlement transaction.
    """
    mentions = load_person_mentions(connection, triage_observation_id=observation_id)
    acted = 0
    for mention in mentions:
        result = ensure_resolution_for_mention(
            connection,
            person_mention_id=mention.id,
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
        )
        if result != "ineligible":
            acted += 1
    return acted


def seed_unresolved_mentions(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
    now: str,
) -> int:
    """Backfill first-pass resolution for already-triaged unresolved mentions.

    Calls :func:`ensure_resolution_for_mention` for every research/uncertain
    mention with a non-empty exact_name. Ensure is idempotent (reuse / skip /
    create / schedule). Empty ``match_key`` mentions receive disposition
    ``skipped`` here even though they are not K24-eligible for model inspect.
    """
    rows = connection.execute(
        """
        SELECT id
          FROM person_mention
         WHERE outcome IN ('research', 'uncertain')
           AND length(trim(exact_name)) > 0
         ORDER BY id
        """
    ).fetchall()
    acted = 0
    for row in rows:
        result = ensure_resolution_for_mention(
            connection,
            person_mention_id=int(row["id"]),
            run_id=run_id,
            config=config,
            profile=profile,
            now=now,
        )
        if result != "ineligible":
            acted += 1
    return acted


@dataclass(frozen=True, slots=True)
class _ResolutionCall:
    """Application-thread inputs for the worker-thread resolve generation."""

    request: StructuredGenerationRequest
    resolve_input: ResolvePersonEntityInput
    person_mention_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    candidate_person_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _ResolutionPersist:
    """Prevalidated resolve output and provenance for application-thread persist."""

    output: ResolvePersonEntityOutput
    person_mention_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    candidate_person_ids: tuple[int, ...]


def _execute_resolution_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    """Worker-thread generation + domain validation; no SQLite in freevars."""

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        if not isinstance(prepared, _ResolutionCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.generate_structured(prepared.request)
        raw_text = result.raw_text
        if result.refusal:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_RESOLUTION_DETAIL,
            )
        try:
            output = validate_resolution_output(raw_text, prepared.resolve_input)
        except ResolutionValidationError:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_RESOLUTION_DETAIL,
            ) from None
        del raw_text
        payload = _ResolutionPersist(
            output=output,
            person_mention_id=prepared.person_mention_id,
            model_inspection_id=prepared.model_inspection_id,
            canonical_supplied_input_json=prepared.canonical_supplied_input_json,
            prompt_hash=prepared.prompt_hash,
            schema_hash=prepared.schema_hash,
            schema_version=prepared.schema_version,
            task_fingerprint=prepared.task_fingerprint,
            candidate_person_ids=prepared.candidate_person_ids,
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


def _persist_resolution_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _ResolutionPersist):
            raise RuntimeError(
                f"unexpected resolution payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        existing = load_er_by_mention_fingerprint(
            connection,
            person_mention_id=payload.person_mention_id,
            task_fingerprint=payload.task_fingerprint,
        )
        if existing is not None:
            linked = _linked_person_id_from_er(
                created_person_id=existing.created_person_id,
                selected_person_id=existing.selected_person_id,
            )
            point_mention_current_er(
                connection,
                person_mention_id=payload.person_mention_id,
                observation_id=existing.id,
                person_id=linked,
            )
            return

        candidate_ids = list(payload.candidate_person_ids)
        candidates_json = _candidate_ids_json(candidate_ids)
        output = payload.output
        validated_json = output.model_dump_json()
        supporting_json = json.dumps(
            list(output.supporting_fact_ids), separators=(",", ":")
        )
        conflicting_json = json.dumps(
            list(output.conflicting_fact_ids), separators=(",", ":")
        )

        if output.outcome == "same_person":
            if output.selected_person_id is None:
                raise RuntimeError("same_person output missing selected_person_id")
            selected = canonical_person_id(connection, output.selected_person_id)
            mention_row = connection.execute(
                "SELECT exact_name FROM person_mention WHERE id = ?",
                (payload.person_mention_id,),
            ).fetchone()
            if mention_row is None:
                raise RuntimeError(
                    f"person_mention {payload.person_mention_id} "
                    "missing in resolve persist"
                )
            upsert_sourced_names_for_mention(
                connection,
                person_id=selected,
                mention_id=payload.person_mention_id,
                exact_name=str(mention_row["exact_name"]),
                identity_fact_names=_mention_identity_fact_name_values(
                    connection, person_mention_id=payload.person_mention_id
                ),
                observed_at=observed_at,
            )
            er_id = insert_entity_resolution_observation(
                connection,
                person_mention_id=payload.person_mention_id,
                person_relation_id=None,
                run_id=run_id,
                attempt_id=attempt_id,
                model_inspection_id=payload.model_inspection_id,
                disposition="completed",
                semantic_outcome="same_person",
                selected_person_id=selected,
                created_person_id=None,
                candidate_person_ids_json=candidates_json,
                canonical_supplied_input_json=payload.canonical_supplied_input_json,
                validated_output_json=validated_json,
                prompt_hash=payload.prompt_hash,
                schema_hash=payload.schema_hash,
                schema_version=payload.schema_version,
                task_fingerprint=payload.task_fingerprint,
                supporting_fact_ids_json=supporting_json,
                conflicting_fact_ids_json=conflicting_json,
                rationale=output.rationale,
                failure_category=None,
                observed_at=observed_at,
            )
            # Link person_id before fingerprint recompute: operational projection
            # reads non-name facts only from mentions with person_id set (C1).
            point_mention_current_er(
                connection,
                person_mention_id=payload.person_mention_id,
                observation_id=er_id,
                person_id=selected,
            )
            display_name = select_display_name(connection, selected)
            fingerprint = recompute_identity_fingerprint(connection, selected)
            connection.execute(
                """
                UPDATE person
                   SET display_name = ?, identity_fingerprint = ?
                 WHERE id = ?
                """,
                (display_name, fingerprint, selected),
            )
            # Capture edges that pre-exist peer scan so same-settlement edges
            # are not reconsidered (K21).
            pre_existing_relation_ids = frozenset(
                relation.id
                for relation in list_active_possible_same_person_for(
                    connection, selected
                )
            )
            # K17: non-name material attach re-opens name-matched peer edges.
            attaching_has_non_name = (
                connection.execute(
                    """
                    SELECT 1
                      FROM mention_identity_fact
                     WHERE person_mention_id = ?
                       AND kind != 'name'
                     LIMIT 1
                    """,
                    (payload.person_mention_id,),
                ).fetchone()
                is not None
            )
            if attaching_has_non_name:
                _apply_peer_edges(
                    connection,
                    person_id=selected,
                    reject_set=set(),
                    creating_mention_id=payload.person_mention_id,
                    run_id=run_id,
                    observation_id=er_id,
                    config=config,
                    now=observed_at,
                )
            maybe_schedule_reconsideration_for_person(
                connection,
                person_id=selected,
                pre_existing_relation_ids=pre_existing_relation_ids,
                run_id=run_id,
                config=config,
                now=observed_at,
            )
            return

        # different_people or uncertain: create a new person.
        person_id = create_person_for_mention(
            connection,
            run_id=run_id,
            mention_id=payload.person_mention_id,
            now=observed_at,
        )
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=payload.person_mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=payload.model_inspection_id,
            disposition="completed",
            semantic_outcome=output.outcome,
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json=candidates_json,
            canonical_supplied_input_json=payload.canonical_supplied_input_json,
            validated_output_json=validated_json,
            prompt_hash=payload.prompt_hash,
            schema_hash=payload.schema_hash,
            schema_version=payload.schema_version,
            task_fingerprint=payload.task_fingerprint,
            supporting_fact_ids_json=supporting_json,
            conflicting_fact_ids_json=conflicting_json,
            rationale=output.rationale,
            failure_category=None,
            observed_at=observed_at,
        )
        point_mention_current_er(
            connection,
            person_mention_id=payload.person_mention_id,
            observation_id=er_id,
            person_id=person_id,
        )

        # New person has no edges before this settlement (K21).
        pre_existing_relation_ids: frozenset[int] = frozenset()
        reject_set: set[int] = set()
        if output.outcome == "uncertain":
            # K4: edges to all code-supplied candidates; peer scan reject empty.
            for candidate_id in candidate_ids:
                upsert_active_possible_same_person(
                    connection,
                    person_id_a=person_id,
                    person_id_b=candidate_id,
                    run_id=run_id,
                    created_by_observation_id=er_id,
                    now=observed_at,
                )
            reject_set = set()
        else:
            # different_people: no edges to rejected candidates.
            reject_set = set(candidate_ids)

        _apply_peer_edges(
            connection,
            person_id=person_id,
            reject_set=reject_set,
            creating_mention_id=payload.person_mention_id,
            run_id=run_id,
            observation_id=er_id,
            config=config,
            now=observed_at,
        )
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_id,
            pre_existing_relation_ids=pre_existing_relation_ids,
            run_id=run_id,
            config=config,
            now=observed_at,
        )

    return persist


def _persist_resolution_failure_for(
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
        person_mention_id = int(state_row["subject_id"])
        task_fingerprint = state_row["fingerprint"]
        existing = load_er_by_mention_fingerprint(
            connection,
            person_mention_id=person_mention_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            # Re-armed empty-at-prepare / ensure already wrote domain state.
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
        routing_fp = routing_fingerprint(config.openrouter.routing)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=config.tasks.resolve_person_entity.model,
            routing_fingerprint=routing_fp,
        )
        model_inspection_id = None if inspection is None else inspection.id
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=person_mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=model_inspection_id,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=task_fingerprint,
            rationale=str(failure.category),
            failure_category=str(failure.category),
            observed_at=observed_at,
        )
        point_mention_current_er(
            connection,
            person_mention_id=person_mention_id,
            observation_id=er_id,
            person_id=None,
        )

    return persist_failure


def build_resolution_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> TaskHandler:
    """The ``resolve_person_entity`` handler: model path only (K3 empty is ensure)."""
    resolve = config.tasks.resolve_person_entity
    model_id = resolve.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname
    parameters = resolve.parameters

    def ready(claimed_run_id: int) -> bool:
        return inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=model_id,
        )

    def prepare(work_item: WorkItem) -> TaskPreparation:
        if work_item.subject_id is None:
            raise ValueError(MISSING_MENTION_DETAIL)
        person_mention_id = work_item.subject_id
        mention = connection.execute(
            """
            SELECT exact_name, search_name, outcome, person_id
              FROM person_mention
             WHERE id = ?
            """,
            (person_mention_id,),
        ).fetchone()
        if mention is None:
            raise ValueError(MISSING_MENTION_DETAIL)

        run_id = _claimed_run_id(connection, work_item.id)
        candidates = _retrieve_candidates_for_mention(
            connection, person_mention_id=person_mention_id, config=config
        )
        existing_er = load_er_by_mention_fingerprint(
            connection,
            person_mention_id=person_mention_id,
            task_fingerprint=work_item.fingerprint,
        )
        already_linked = mention["person_id"] is not None
        if not candidates or already_linked or existing_er is not None:
            ensure_resolution_for_mention(
                connection,
                person_mention_id=person_mention_id,
                run_id=run_id,
                config=config,
                profile=profile,
                now=_observed_at_for_prepare(connection, work_item.id),
            )
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}empty_or_settled mention "
                f"{person_mention_id}"
            )

        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=model_id,
            routing_fingerprint=routing_fp,
        )
        if inspection is None or inspection.compatibility != "compatible":
            raise ValueError(MISSING_INSPECTION_DETAIL)

        outcome = str(mention["outcome"])
        exact_name = str(mention["exact_name"])
        search_name = (
            str(mention["search_name"])
            if mention["search_name"] is not None
            else exact_name
        )
        source_item_id = _source_item_id_for_mention(
            connection, person_mention_id=person_mention_id
        )
        resolve_input = build_resolve_input(
            person_mention_id=person_mention_id,
            source_item_id=source_item_id,
            exact_name=exact_name,
            search_name=search_name,
            mention_outcome=outcome,
            passages=_passages_for_source_item(
                connection, source_item_id=source_item_id, config=config
            ),
            identity_facts=_identity_facts_for_mention(
                connection, person_mention_id=person_mention_id
            ),
            signals=_signals_for_mention(
                connection, person_mention_id=person_mention_id
            ),
            candidates=_to_resolve_candidates(candidates),
            config=resolve,
        )
        rendered = render_resolution_request(resolve_input)
        request = StructuredGenerationRequest(
            model_id=model_id,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name=RESOLUTION_SCHEMA_NAME,
            max_completion_tokens=resolve.max_completion_tokens,
            temperature=parameters.temperature,
            top_p=parameters.top_p,
            reasoning_effort=parameters.reasoning_effort,
        )
        call = _ResolutionCall(
            request=request,
            resolve_input=resolve_input,
            person_mention_id=person_mention_id,
            model_inspection_id=inspection.id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=rendered.prompt_hash,
            schema_hash=rendered.schema_hash,
            schema_version=rendered.schema_version,
            task_fingerprint=work_item.fingerprint,
            candidate_person_ids=tuple(c.person_id for c in candidates),
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
            max_input_tokens=resolve.max_input_tokens,
            max_completion_tokens=resolve.max_completion_tokens,
        )
        return TaskPreparation(payload=call, reserved_nano_usd=reserved)

    def destination_host(work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=_execute_resolution_for(client),
        prepare=prepare,
        persist=_persist_resolution_for(connection, config=config),
        persist_failure=_persist_resolution_failure_for(connection, config=config),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )


def _observed_at_for_prepare(connection: sqlite3.Connection, work_item_id: int) -> str:
    """Best-effort observed_at for prepare-time ensure (work item updated_at)."""
    row = connection.execute(
        "SELECT updated_at FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is not None and row["updated_at"]:
        return str(row["updated_at"])
    return utc_timestamp(datetime.now(tz=UTC))


# ---------------------------------------------------------------------------
# Reconsideration of possible_same_person edges (K18 / K21 / K22)
# ---------------------------------------------------------------------------


def _person_identity_fingerprint(
    connection: sqlite3.Connection, person_id: int
) -> str | None:
    row = connection.execute(
        "SELECT identity_fingerprint FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    return str(row["identity_fingerprint"])


def _latest_linked_mention_id(
    connection: sqlite3.Connection, *, person_id: int
) -> int | None:
    mentions = mentions_for_canonical_person(connection, person_id)
    if not mentions:
        return None
    return max(mention.id for mention in mentions)


def _peer_endpoint(person_id_a: int, person_id_b: int, person_id: int) -> int:
    if person_id == person_id_a:
        return person_id_b
    if person_id == person_id_b:
        return person_id_a
    raise ValueError(f"person {person_id} is not an endpoint of the relation")


def _canonical_has_non_name_identity_fact(
    connection: sqlite3.Connection, person_id: int
) -> bool:
    for mention in mentions_for_canonical_person(connection, person_id):
        for fact in mention.identity_facts:
            if fact.kind != "name":
                return True
    return False


def merge_guardrails_pass(
    connection: sqlite3.Connection,
    *,
    relation_id: int,
    subject_person_id: int,
    peer_person_id: int,
    output: ResolvePersonEntityOutput,
    resolve_input: ResolvePersonEntityInput,
) -> bool:
    """K18 automatic-merge guardrails. All must hold.

    Does not perform the merge; ``confirm_person_merge`` owns the write path.
    """
    relation = connection.execute(
        """
        SELECT kind, status, person_id_a, person_id_b
          FROM person_relation
         WHERE id = ?
        """,
        (relation_id,),
    ).fetchone()
    if relation is None:
        return False
    if relation["kind"] != "possible_same_person" or relation["status"] != "active":
        return False

    subject_canonical = canonical_person_id(connection, subject_person_id)
    peer_canonical = canonical_person_id(connection, peer_person_id)
    if subject_canonical != subject_person_id or peer_canonical != peer_person_id:
        # Endpoints must still be canonical (merged_into IS NULL).
        return False
    if subject_canonical == peer_canonical:
        return False

    if output.selected_person_id is None:
        return False
    selected_canonical = canonical_person_id(connection, output.selected_person_id)
    if selected_canonical != peer_canonical:
        return False

    if not output.supporting_fact_ids:
        return False
    allowed_fact_ids = {fact.local_id for fact in resolve_input.identity_facts}
    for candidate in resolve_input.candidates:
        allowed_fact_ids.update(fact.local_id for fact in candidate.identity_facts)
    for fact_id in output.supporting_fact_ids:
        if fact_id not in allowed_fact_ids:
            return False

    if not _canonical_has_non_name_identity_fact(connection, subject_canonical):
        return False
    return _canonical_has_non_name_identity_fact(connection, peer_canonical)


def maybe_schedule_reconsideration_for_person(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    pre_existing_relation_ids: frozenset[int] | set[int] | Sequence[int],
    run_id: int,
    config: MainConfig,
    now: str,
) -> int:
    """Schedule reconsider work for pre-existing active edges involving person_id.

    **K21:** Only relation ids present in ``pre_existing_relation_ids`` are
    eligible. Edges opened in the same settlement must be omitted from that set.

    Returns the number of work items scheduled (including idempotent reuses of
    already-active work with the same fingerprint).
    """
    allowed = frozenset(pre_existing_relation_ids)
    if not allowed:
        return 0

    resolve = config.tasks.resolve_person_entity
    prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
    subject_fp = _person_identity_fingerprint(connection, person_id)
    if subject_fp is None:
        return 0

    scheduled = 0
    for relation in list_active_possible_same_person_for(connection, person_id):
        if relation.id not in allowed:
            continue
        try:
            peer_id = _peer_endpoint(
                relation.person_id_a, relation.person_id_b, person_id
            )
        except ValueError:
            continue
        peer_fp = _person_identity_fingerprint(connection, peer_id)
        if peer_fp is None:
            continue
        fingerprint = reconsider_task_fingerprint(
            person_relation_id=relation.id,
            subject_identity_fingerprint=subject_fp,
            peer_identity_fingerprint=peer_fp,
            config=resolve,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
        )
        existing_er = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=relation.id,
            task_fingerprint=fingerprint,
        )
        if existing_er is not None:
            continue

        # Supersede other active reconsider fingerprints for this relation.
        stale = connection.execute(
            """
            SELECT fingerprint
              FROM work_item
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id = ?
               AND state IN ('pending', 'deferred')
               AND fingerprint != ?
             ORDER BY id
            """,
            (
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                SUBJECT_KIND_PERSON_RELATION,
                relation.id,
                fingerprint,
            ),
        ).fetchall()
        for row in stale:
            repository.supersede_work(
                connection,
                task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                fingerprint=row["fingerprint"],
                run_id=run_id,
                now=now,
                reason=SUPERSEDED_RECONSIDER_REASON,
            )

        repository.schedule_work(
            connection,
            task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
            subject_kind=SUBJECT_KIND_PERSON_RELATION,
            subject_id=relation.id,
            fingerprint=fingerprint,
            required=True,
            priority=RECONSIDER_PERSON_PRIORITY,
            eligible_at=now,
            run_id=run_id,
            now=now,
        )
        scheduled += 1
    return scheduled


def _load_candidate_person(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    config: MainConfig,
) -> CandidatePerson:
    resolve = config.tasks.resolve_person_entity
    # Import the shared projection builder without expanding candidates.py API.
    from notable_person_finder.people.candidates import (  # noqa: PLC0415
        _build_candidate,
    )

    return _build_candidate(
        connection,
        person_id=person_id,
        score=0,
        max_facts=resolve.max_facts_per_candidate,
        max_names=resolve.max_names_per_candidate,
    )


def _resolve_reconsider_sides_for_relation(
    connection: sqlite3.Connection,
    *,
    relation_id: int,
    person_id_a: int,
    person_id_b: int,
    work_fingerprint: str,
    config: MainConfig,
) -> tuple[int, int]:
    """Return (subject_person_id, peer_person_id) matching the work fingerprint.

    Prefer the ordering whose identity fingerprints reproduce the scheduled
    fingerprint (subject = fingerprint-changed side at schedule time). When both
    orderings match (identical fingerprints) or neither does, use the lower
    person id as subject (design determinism rule).
    """
    resolve = config.tasks.resolve_person_entity
    prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
    lower, higher = sorted((person_id_a, person_id_b))
    matched: list[tuple[int, int]] = []
    for subject_id, peer_id in ((lower, higher), (higher, lower)):
        subject_fp = _person_identity_fingerprint(connection, subject_id)
        peer_fp = _person_identity_fingerprint(connection, peer_id)
        if subject_fp is None or peer_fp is None:
            continue
        candidate_fp = reconsider_task_fingerprint(
            person_relation_id=relation_id,
            subject_identity_fingerprint=subject_fp,
            peer_identity_fingerprint=peer_fp,
            config=resolve,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
        )
        if candidate_fp == work_fingerprint:
            matched.append((subject_id, peer_id))
    if len(matched) == 1:
        return matched[0]
    # Both or neither: lower person id is subject.
    return lower, higher


@dataclass(frozen=True, slots=True)
class _ReconsiderCall:
    """Application-thread inputs for worker-thread reconsider generation."""

    request: StructuredGenerationRequest
    resolve_input: ResolvePersonEntityInput
    person_relation_id: int
    subject_person_id: int
    peer_person_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str


@dataclass(frozen=True, slots=True)
class _ReconsiderPersist:
    """Prevalidated reconsider output and provenance for application-thread persist."""

    output: ResolvePersonEntityOutput
    person_relation_id: int
    subject_person_id: int
    peer_person_id: int
    model_inspection_id: int
    canonical_supplied_input_json: str
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    resolve_input: ResolvePersonEntityInput


def _execute_reconsideration_for(
    client: LlmClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    """Worker-thread generation + domain validation; no SQLite in freevars."""

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        if not isinstance(prepared, _ReconsiderCall):
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )
        result = client.generate_structured(prepared.request)
        raw_text = result.raw_text
        if result.refusal:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_RESOLUTION_DETAIL,
            )
        try:
            output = validate_resolution_output(raw_text, prepared.resolve_input)
        except ResolutionValidationError:
            del raw_text, result
            raise ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=True,
                detail=MALFORMED_RESOLUTION_DETAIL,
            ) from None
        del raw_text
        payload = _ReconsiderPersist(
            output=output,
            person_relation_id=prepared.person_relation_id,
            subject_person_id=prepared.subject_person_id,
            peer_person_id=prepared.peer_person_id,
            model_inspection_id=prepared.model_inspection_id,
            canonical_supplied_input_json=prepared.canonical_supplied_input_json,
            prompt_hash=prepared.prompt_hash,
            schema_hash=prepared.schema_hash,
            schema_version=prepared.schema_version,
            task_fingerprint=prepared.task_fingerprint,
            resolve_input=prepared.resolve_input,
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


def _attempt_confirm_person_merge(
    connection: sqlite3.Connection,
    *,
    loser_id: int,
    survivor_id: int,
    run_id: int,
    observation_id: int,
    now: str,
) -> bool:
    """Run ``confirm_person_merge`` and report that a merge was attempted."""
    confirm_person_merge(
        connection,
        loser_id=loser_id,
        survivor_id=survivor_id,
        run_id=run_id,
        observation_id=observation_id,
        now=now,
    )
    return True


def _persist_reconsideration_for(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _ReconsiderPersist):
            raise RuntimeError(
                f"unexpected reconsideration payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        existing = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=payload.person_relation_id,
            task_fingerprint=payload.task_fingerprint,
        )
        if existing is not None:
            return

        output = payload.output
        validated_json = output.model_dump_json()
        supporting_json = json.dumps(
            list(output.supporting_fact_ids), separators=(",", ":")
        )
        conflicting_json = json.dumps(
            list(output.conflicting_fact_ids), separators=(",", ":")
        )
        candidates_json = _candidate_ids_json((payload.peer_person_id,))
        selected: int | None = None
        rationale = output.rationale

        if output.outcome == "same_person":
            if output.selected_person_id is None:
                raise RuntimeError("same_person output missing selected_person_id")
            selected = canonical_person_id(connection, output.selected_person_id)
            guardrails_ok = merge_guardrails_pass(
                connection,
                relation_id=payload.person_relation_id,
                subject_person_id=payload.subject_person_id,
                peer_person_id=payload.peer_person_id,
                output=output,
                resolve_input=payload.resolve_input,
            )
            if not guardrails_ok:
                rationale = f"{rationale}{MERGE_GUARDRAILS_FAILED_NOTE}"
            else:
                rationale = f"{rationale}{READY_TO_MERGE_NOTE}"

        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=payload.person_relation_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=payload.model_inspection_id,
            disposition="completed",
            semantic_outcome=output.outcome,
            selected_person_id=selected,
            created_person_id=None,
            candidate_person_ids_json=candidates_json,
            canonical_supplied_input_json=payload.canonical_supplied_input_json,
            validated_output_json=validated_json,
            prompt_hash=payload.prompt_hash,
            schema_hash=payload.schema_hash,
            schema_version=payload.schema_version,
            task_fingerprint=payload.task_fingerprint,
            supporting_fact_ids_json=supporting_json,
            conflicting_fact_ids_json=conflicting_json,
            rationale=rationale,
            failure_category=None,
            observed_at=observed_at,
        )

        if output.outcome == "different_people":
            dismiss_relation(
                connection,
                relation_id=payload.person_relation_id,
                closed_by_observation_id=er_id,
                closed_at=observed_at,
            )
            return

        if output.outcome == "uncertain":
            # Leave edge active; no reschedule until fingerprint changes.
            return

        # same_person: confirm merge when guardrails pass (K7 lower-id survivor).
        if MERGE_GUARDRAILS_FAILED_NOTE in rationale:
            return
        subject = payload.subject_person_id
        peer = payload.peer_person_id
        survivor_id = min(subject, peer)
        loser_id = max(subject, peer)
        merged = _attempt_confirm_person_merge(
            connection,
            loser_id=loser_id,
            survivor_id=survivor_id,
            run_id=run_id,
            observation_id=er_id,
            now=observed_at,
        )
        if not merged:
            # Edge stays active if merge is unavailable; fingerprint change
            # re-arms reconsideration later.
            return

    return persist


def _persist_reconsideration_failure_for(
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
        person_relation_id = int(state_row["subject_id"])
        task_fingerprint = state_row["fingerprint"]
        existing = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=person_relation_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            return

        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
        routing_fp = routing_fingerprint(config.openrouter.routing)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=config.tasks.resolve_person_entity.model,
            routing_fingerprint=routing_fp,
        )
        model_inspection_id = None if inspection is None else inspection.id
        insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=person_relation_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=model_inspection_id,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=prompt_hash,
            schema_hash=schema_hash,
            schema_version=schema_version,
            task_fingerprint=task_fingerprint,
            rationale=str(failure.category),
            failure_category=str(failure.category),
            observed_at=observed_at,
        )
        # Edge stays active (design: leave relation status unchanged on failed).

    return persist_failure


def build_reconsideration_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> TaskHandler:
    """The ``reconsider_person_entity`` handler (relation-scoped; K18/K21/K22)."""
    del profile  # reserved for future domain-profile bounds
    resolve = config.tasks.resolve_person_entity
    model_id = resolve.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname
    parameters = resolve.parameters

    def ready(claimed_run_id: int) -> bool:
        return inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=model_id,
        )

    def prepare(work_item: WorkItem) -> TaskPreparation:
        if work_item.subject_id is None:
            raise ValueError(MISSING_RELATION_DETAIL)
        person_relation_id = work_item.subject_id
        relation = connection.execute(
            """
            SELECT id, kind, status, person_id_a, person_id_b
              FROM person_relation
             WHERE id = ?
            """,
            (person_relation_id,),
        ).fetchone()
        if relation is None:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}missing_relation {person_relation_id}"
            )
        if relation["kind"] != "possible_same_person" or relation["status"] != "active":
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}inactive_relation "
                f"{person_relation_id}"
            )

        person_id_a = int(relation["person_id_a"])
        person_id_b = int(relation["person_id_b"])

        # Resolve endpoints to canonical form; refuse if either side is gone
        # or both collapse to the same canonical person mid-flight.
        try:
            canon_a = canonical_person_id(connection, person_id_a)
            canon_b = canonical_person_id(connection, person_id_b)
        except LookupError as exc:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}missing_peer {person_relation_id}"
            ) from exc
        if canon_a == canon_b:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}merged_endpoints {person_relation_id}"
            )

        # Prefer stored endpoints when still canonical; otherwise use canons.
        endpoint_a = person_id_a if canon_a == person_id_a else canon_a
        endpoint_b = person_id_b if canon_b == person_id_b else canon_b

        existing_er = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=person_relation_id,
            task_fingerprint=work_item.fingerprint,
        )
        if existing_er is not None:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}already_settled_relation "
                f"{person_relation_id}"
            )

        subject_person_id, peer_person_id = _resolve_reconsider_sides_for_relation(
            connection,
            relation_id=person_relation_id,
            person_id_a=endpoint_a,
            person_id_b=endpoint_b,
            work_fingerprint=work_item.fingerprint,
            config=config,
        )

        subject_mention_id = _latest_linked_mention_id(
            connection, person_id=subject_person_id
        )
        if subject_mention_id is None:
            # Fallback: lower-id side (design).
            fallback_subject = min(endpoint_a, endpoint_b)
            fallback_peer = max(endpoint_a, endpoint_b)
            subject_person_id = fallback_subject
            peer_person_id = fallback_peer
            subject_mention_id = _latest_linked_mention_id(
                connection, person_id=subject_person_id
            )
        if subject_mention_id is None:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}missing_subject_mention "
                f"{person_relation_id}"
            )

        mention = connection.execute(
            """
            SELECT exact_name, search_name, outcome, person_id
              FROM person_mention
             WHERE id = ?
            """,
            (subject_mention_id,),
        ).fetchone()
        if mention is None:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}missing_subject_mention "
                f"{person_relation_id}"
            )
        outcome = str(mention["outcome"])
        if outcome not in {"research", "uncertain"}:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}ineligible_subject_mention "
                f"{person_relation_id}"
            )

        run_id = _claimed_run_id(connection, work_item.id)
        inspection = load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=model_id,
            routing_fingerprint=routing_fp,
        )
        if inspection is None or inspection.compatibility != "compatible":
            raise ValueError(MISSING_INSPECTION_DETAIL)

        peer_row = connection.execute(
            "SELECT id FROM person WHERE id = ?",
            (peer_person_id,),
        ).fetchone()
        if peer_row is None:
            raise ValueError(
                f"{RESOLVE_PREPARE_REFUSED_PREFIX}missing_peer {person_relation_id}"
            )

        exact_name = str(mention["exact_name"])
        search_name = (
            str(mention["search_name"])
            if mention["search_name"] is not None
            else exact_name
        )
        source_item_id = _source_item_id_for_mention(
            connection, person_mention_id=subject_mention_id
        )
        peer_candidate = _load_candidate_person(
            connection, person_id=peer_person_id, config=config
        )
        resolve_input = build_resolve_input(
            person_mention_id=subject_mention_id,
            source_item_id=source_item_id,
            exact_name=exact_name,
            search_name=search_name,
            mention_outcome=outcome,
            passages=_passages_for_source_item(
                connection, source_item_id=source_item_id, config=config
            ),
            identity_facts=_identity_facts_for_mention(
                connection, person_mention_id=subject_mention_id
            ),
            signals=_signals_for_mention(
                connection, person_mention_id=subject_mention_id
            ),
            candidates=_to_resolve_candidates((peer_candidate,)),
            config=resolve,
        )
        rendered = render_resolution_request(resolve_input)
        request = StructuredGenerationRequest(
            model_id=model_id,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name=RESOLUTION_SCHEMA_NAME,
            max_completion_tokens=resolve.max_completion_tokens,
            temperature=parameters.temperature,
            top_p=parameters.top_p,
            reasoning_effort=parameters.reasoning_effort,
        )
        call = _ReconsiderCall(
            request=request,
            resolve_input=resolve_input,
            person_relation_id=person_relation_id,
            subject_person_id=subject_person_id,
            peer_person_id=peer_person_id,
            model_inspection_id=inspection.id,
            canonical_supplied_input_json=rendered.canonical_input_json,
            prompt_hash=rendered.prompt_hash,
            schema_hash=rendered.schema_hash,
            schema_version=rendered.schema_version,
            task_fingerprint=work_item.fingerprint,
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
            max_input_tokens=resolve.max_input_tokens,
            max_completion_tokens=resolve.max_completion_tokens,
        )
        return TaskPreparation(payload=call, reserved_nano_usd=reserved)

    def destination_host(work_item: WorkItem) -> str | None:
        return endpoint_host

    return TaskHandler(
        task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=_execute_reconsideration_for(client),
        prepare=prepare,
        persist=_persist_reconsideration_for(connection, config=config),
        persist_failure=_persist_reconsideration_failure_for(connection, config=config),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )
