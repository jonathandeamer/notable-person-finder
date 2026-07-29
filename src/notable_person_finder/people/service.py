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
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from importlib import resources
from urllib.parse import urlsplit

from notable_person_finder.config.models import (
    DomainProfileConfig,
    FeedConfig,
    MainConfig,
    ProviderRoutingConfig,
)
from notable_person_finder.people.detection import (
    DETECTION_SCHEMA_VERSION,
    DetectionValidationError,
    build_detection_input,
    detection_schema,
    render_detection_request,
    validate_detection_output,
)
from notable_person_finder.people.models import DetectionInput, DetectionOutput
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    SourceItemRecord,
    insert_completed_observation,
    insert_failed_observation,
    insert_insufficient_input_observation,
    insert_model_inspection,
    list_untriaged_source_item_ids,
    load_current_triage_observation,
    load_model_inspection,
    load_source_item_record,
    load_triage_observation_by_fingerprint,
    settle_active_detect_people_after_permanent_preflight,
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
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome, TaskPreparation
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import WorkerPool

INSPECT_MODEL_TASK_TYPE = "inspect_model"
INSPECT_MODEL_PRIORITY = 20
SUBJECT_KIND_MODEL = "model"
SUBJECT_KIND_SOURCE_ITEM = "source_item"

DETECT_PEOPLE_PRIORITY = 30
DETECTION_SCHEMA_NAME = "detect_people"

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


def ensure_model_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    config: MainConfig,
    now: str,
) -> int:
    """Schedule current-run model inspection when usable untriaged work exists.

    Returns ``1`` when inspection work is ensured for this run's model, else
    ``0``. Supersedes older active ``inspect_model`` work so every run collects
    fresh capability evidence (no cross-run reuse).
    """
    if not _has_usable_untriaged_source_item(connection):
        return 0

    model_id = config.tasks.detect_people.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    fingerprint = _inspection_work_fingerprint(
        run_id=run_id, model_id=model_id, routing_fp=routing_fp
    )

    stale = connection.execute(
        """
        SELECT fingerprint
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred')
           AND fingerprint != ?
         ORDER BY id
        """,
        (INSPECT_MODEL_TASK_TYPE, fingerprint),
    ).fetchall()
    for row in stale:
        repository.supersede_work(
            connection,
            task_type=INSPECT_MODEL_TASK_TYPE,
            fingerprint=row["fingerprint"],
            run_id=run_id,
            now=now,
            reason=SUPERSEDED_REASON,
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
    return 1


def inspection_ready(
    connection: sqlite3.Connection, *, run_id: int, config: MainConfig
) -> bool:
    """True when this run has a compatible inspection for the configured model.

    Under a hard OpenRouter budget, usable unit pricing is also required so
    generation prepare can compute a worst-case reservation.
    """
    model_id = config.tasks.detect_people.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    inspection = load_model_inspection(
        connection,
        run_id=run_id,
        configured_model_id=model_id,
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
    failure_category: str,
    rationale: str,
    now: str,
) -> None:
    prompt_hash, schema_hash, schema_version = _detection_prompt_and_schema_hashes()
    settle_active_detect_people_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        failure_category=failure_category,
        rationale=rationale,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
        now=now,
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
) -> Callable[[WorkItem, TaskOutcome], None]:
    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        payload = outcome.payload
        if not isinstance(payload, _InspectionPersist):
            raise RuntimeError(
                f"unexpected inspection payload type: {type(payload).__name__}"
            )
        attempt_id, run_id, inspected_at = _attempt_context(connection, work_item.id)
        _insert_inspection_row(
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
                failure_category=payload.failure_category,
                rationale=payload.reason,
                now=inspected_at,
            )

    return persist


def _persist_failure_for(
    connection: sqlite3.Connection,
) -> Callable[[WorkItem, ProviderFailure], None]:
    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        # Transient exhaustion leaves dependents pending and unclaimable via
        # readiness; only permanent preflight categories settle them here.
        if failure.category not in _PERMANENT_PREFLIGHT_CATEGORIES:
            return
        attempt_id, run_id, observed_at = _attempt_context(connection, work_item.id)
        _settle_dependents(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            failure_category=str(failure.category),
            rationale=str(failure.category),
            now=observed_at,
        )

    return persist_failure


def build_inspection_handler(
    connection: sqlite3.Connection,
    *,
    client: LlmClient,
    config: MainConfig,
) -> TaskHandler:
    """The ``inspect_model`` handler: one capability preflight, two threads."""
    model_id = config.tasks.detect_people.model
    routing_fp = routing_fingerprint(config.openrouter.routing)
    hard_budget = config.budget.openrouter_nano_usd_per_run() is not None
    endpoint_host = urlsplit(config.openrouter.endpoint).hostname

    def prepare(work_item: WorkItem) -> TaskPreparation:
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
        persist=_persist_for(connection, routing_fp=routing_fp),
        persist_failure=_persist_failure_for(connection),
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
        insert_completed_observation(
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
        return inspection_ready(connection, run_id=claimed_run_id, config=config)

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
        persist=_persist_detection_for(connection),
        persist_failure=_persist_detection_failure_for(
            connection, profile=profile, config=config
        ),
        destination_host=destination_host,
        reserved_nano_usd=0,
        pool=WorkerPool.LLM,
        ready=ready,
    )
