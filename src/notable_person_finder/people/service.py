"""Model inspection seeding and the preflight handler that gates detection.

This milestone schedules one current-run ``inspect_model`` work item when at
least one usable untriaged source item exists, executes it on the LLM pool,
and unlocks detection readiness only after a compatible inspection is
persisted for the exact configured model and routing policy.

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
from collections.abc import Callable
from dataclasses import dataclass
from importlib import resources
from urllib.parse import urlsplit

from notable_person_finder.config.models import MainConfig, ProviderRoutingConfig
from notable_person_finder.people.detection import (
    DETECTION_SCHEMA_VERSION,
    detection_schema,
)
from notable_person_finder.people.repository import (
    insert_model_inspection,
    load_model_inspection,
    settle_active_detect_people_after_permanent_preflight,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    INSPECT_OPERATION,
    PROVIDER,
    LlmClient,
    ModelInspectionRequest,
    ModelInspectionResult,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome, TaskPreparation
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import WorkerPool

INSPECT_MODEL_TASK_TYPE = "inspect_model"
INSPECT_MODEL_PRIORITY = 20
SUBJECT_KIND = "model"

# Bumped when inspection request identity or capability adjudication changes.
ADAPTER_VERSION = 1
CAPABILITY_CONTRACT_VERSION = 1

SUPERSEDED_REASON = "stale model inspection"
UNSUPPORTED_STRICT_REASON = "unsupported strict structured output"
PRICING_REQUIRED_REASON = "pricing required under hard budget"

_PERMANENT_PREFLIGHT_CATEGORIES = frozenset(
    {
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.ACCESS_DENIED,
    }
)


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
        subject_kind=SUBJECT_KIND,
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
