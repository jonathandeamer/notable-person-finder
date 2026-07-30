"""Reconsideration of possible_same_person edges: K18/K21/K22."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest

from notable_person_finder.config.models import (
    BudgetConfig,
    DetectPeopleConfig,
    DomainProfileConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    ResolvePersonEntityConfig,
    RetryConfig,
    TasksConfig,
)
from notable_person_finder.people.identity import (
    insert_person,
    recompute_identity_fingerprint,
    upsert_sourced_name,
)
from notable_person_finder.people.repository import (
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    insert_entity_resolution_observation,
    mechanical_search_name,
    upsert_active_possible_same_person,
)
from notable_person_finder.people.resolution import reconsider_task_fingerprint
from notable_person_finder.people.service import (
    INSPECT_MODEL_TASK_TYPE,
    MERGE_GUARDRAILS_FAILED_NOTE,
    READY_TO_MERGE_NOTE,
    RECONSIDER_PERSON_PRIORITY,
    RESOLVE_PREPARE_REFUSED_PREFIX,
    build_inspection_handler,
    build_reconsideration_handler,
    build_resolution_handler,
    ensure_model_inspections_for_run,
    ensure_resolution_for_mention,
    maybe_schedule_reconsideration_for_person,
    routing_fingerprint,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    PROVIDER,
    ModelInspectionRequest,
    ModelInspectionResult,
    StructuredGenerationRequest,
    StructuredGenerationResult,
    TokenUsage,
)
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import ReportArtifact, RunEngine
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import (
    BoundedScheduler,
    SchedulerSet,
    WorkerPool,
)
from tests.ingestion.helpers import immediate, insert_run, moment

MODEL = "openai/gpt-test"
NOW = moment()
_HASH = "a" * 64
_OTHER = "b" * 64
_PROMPT = "p" * 64
_SCHEMA = "s" * 64


def _main_config(*, resolve_model: str = MODEL) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(model=MODEL),
            resolve_person_entity=ResolvePersonEntityConfig(model=resolve_model),
        ),
    )


def _profile() -> DomainProfileConfig:
    return DomainProfileConfig(
        schema_version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples={"significant_recognition": ("major art prize",)},
    )


def _seed_mention_graph(
    connection: sqlite3.Connection,
    *,
    exact_name: str = "Alex Smith",
    outcome: str = "research",
    non_name_facts: tuple[tuple[str, str], ...] = (),
    title: str = "Alex Smith wins award",
    summary: str = "A prize ceremony.",
    entry_id: str = "entry-a",
) -> tuple[int, int, int, int]:
    """Return (run_id, attempt_id, inspection_id, mention_id)."""
    run_id = insert_run(connection)
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (_HASH, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert work is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work, NOW, moment(1), _HASH),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES ('feed-a', 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (NOW, NOW),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, NOW),
    ).lastrowid
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (feed, fetch, run_id, entry_id, title, summary, NOW),
    ).lastrowid
    assert source_item_id is not None
    inspection_id = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, MODEL, MODEL, _HASH, NOW),
    ).lastrowid
    assert inspection_id is not None
    observation_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded result', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT,
            _SCHEMA,
            _HASH,
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    search = mechanical_search_name(exact_name)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, ?, '["p1"]', 'reason')
        """,
        (observation_id, exact_name, search, outcome),
    ).lastrowid
    assert mention_id is not None
    for index, (kind, value) in enumerate(non_name_facts, start=1):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, ?, ?, ?, '["p1"]')
            """,
            (mention_id, f"fact-{index}", kind, value),
        )
    connection.commit()
    return run_id, attempt_id, inspection_id, mention_id


def _add_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    inspection_id: int,
    exact_name: str,
    non_name_facts: tuple[tuple[str, str], ...] = (),
    entry_id: str = "entry-b",
    title: str = "More on Alex Smith",
    summary: str = "Additional coverage.",
) -> int:
    feed = connection.execute(
        "SELECT id FROM feed_identity ORDER BY id LIMIT 1"
    ).fetchone()
    assert feed is not None
    fetch = connection.execute(
        "SELECT id FROM feed_fetch ORDER BY id LIMIT 1"
    ).fetchone()
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (feed["id"], fetch["id"], run_id, entry_id, title, summary, NOW),
    ).lastrowid
    assert source_item_id is not None
    observation_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded result', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT,
            _SCHEMA,
            ("c" * 64),
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    search = mechanical_search_name(exact_name)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, 'research', '["p1"]', 'reason')
        """,
        (observation_id, exact_name, search),
    ).lastrowid
    assert mention_id is not None
    for index, (kind, value) in enumerate(non_name_facts, start=1):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, ?, ?, ?, '["p1"]')
            """,
            (mention_id, f"fact-{index}", kind, value),
        )
    connection.commit()
    return int(mention_id)


@dataclass
class ScriptedLlmClient:
    inspection: ModelInspectionResult | None = None
    generate_results: list[StructuredGenerationResult | ProviderFailure] = field(
        default_factory=list
    )
    inspect_calls: list[ModelInspectionRequest] = field(default_factory=list)
    generate_calls: list[StructuredGenerationRequest] = field(default_factory=list)
    _generate_index: int = 0

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        self.inspect_calls.append(request)
        if self.inspection is None:
            raise RuntimeError("inspection not configured")
        return self.inspection

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        self.generate_calls.append(request)
        if self._generate_index >= len(self.generate_results):
            raise RuntimeError("no further generate results")
        result = self.generate_results[self._generate_index]
        self._generate_index += 1
        if isinstance(result, ProviderFailure):
            raise result
        return result


def _compatible_inspection(*, model_id: str = MODEL) -> ModelInspectionResult:
    return ModelInspectionResult(
        configured_model_id=model_id,
        resolved_model_id=f"{model_id}-resolved",
        supported_parameters=("response_format", "structured_outputs"),
        supports_strict_structured_output=True,
        prompt_unit_price_nano_usd=150,
        completion_unit_price_nano_usd=600,
        latency_ms=12,
    )


def _generation_result(content: str) -> StructuredGenerationResult:
    return StructuredGenerationResult(
        raw_text=content,
        configured_model_id=MODEL,
        resolved_model_id=f"{MODEL}-resolved",
        serving_provider="OpenAI",
        finish_reason="stop",
        refusal=None,
        usage=TokenUsage(prompt_tokens=100, completion_tokens=40, total_tokens=140),
        latency_ms=20,
        provider_request_id="gen-reconsider-1",
        actual_nano_usd=12_000,
    )


def _resolve_output(
    *,
    outcome: str,
    selected_person_id: int | None = None,
    supporting: tuple[str, ...] = (),
    rationale: str = "Evidence-grounded resolution decision.",
) -> str:
    return json.dumps(
        {
            "outcome": outcome,
            "selected_person_id": selected_person_id,
            "supporting_fact_ids": list(supporting),
            "conflicting_fact_ids": [],
            "rationale": rationale,
        },
        separators=(",", ":"),
    )


def _handlers(
    connection: sqlite3.Connection,
    client: ScriptedLlmClient,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> dict:
    return {
        INSPECT_MODEL_TASK_TYPE: build_inspection_handler(
            connection, client=client, config=config
        ),
        RESOLVE_PERSON_ENTITY_TASK_TYPE: build_resolution_handler(
            connection, client=client, config=config, profile=profile
        ),
        RECONSIDER_PERSON_ENTITY_TASK_TYPE: build_reconsideration_handler(
            connection, client=client, config=config, profile=profile
        ),
    }


def _run_engine(
    connection: sqlite3.Connection,
    handlers: dict,
    *,
    seed: Callable[[int], None] | None = None,
    max_attempts: int = 2,
) -> int:
    clock = FakeClock()
    with SchedulerSet({WorkerPool.LLM: BoundedScheduler(max_workers=1)}) as pools:
        engine = RunEngine(
            connection,
            retry=RetryPolicy(
                RetryConfig(max_attempts=max_attempts, jitter_ratio=0.0), clock=clock
            ),
            scheduler=pools,
            clock=clock,
            timezone="Europe/Paris",
            window_start="2026-07-24T06:00:00Z",
            budget_limit_nano_usd=None,
            snapshot_fingerprint="a" * 64,
            snapshot_json="{}",
            reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
        )
        report = engine.execute(handlers, seed=seed)
    return report.run_id


def _count_work(
    connection: sqlite3.Connection, *, task_type: str, subject_id: int | None = None
) -> int:
    if subject_id is None:
        row = connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (task_type,),
        ).fetchone()
    else:
        row = connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE task_type = ? AND subject_id = ?
            """,
            (task_type, subject_id),
        ).fetchone()
    assert row is not None
    return int(row["n"])


def _link_mention_to_person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    mention_id: int,
    person_id: int,
    attempt_id: int,
    inspection_id: int,
) -> int:
    """First-pass completed ER + point mention at person. Returns er_id."""
    del attempt_id, inspection_id  # created_new is schedule-time (no attempt)
    fingerprint = f"{mention_id:064x}"
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"created_new"}',
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint=fingerprint,
            rationale="test link",
            failure_category=None,
            observed_at=NOW,
        )
        connection.execute(
            """
            UPDATE person_mention
               SET person_id = ?,
                   current_entity_resolution_observation_id = ?
             WHERE id = ?
            """,
            (person_id, er_id, mention_id),
        )
        recompute_identity_fingerprint(connection, person_id)
    return er_id


def _open_edge_between_people(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    person_a: int,
    person_b: int,
    er_id: int,
) -> int:
    with immediate(connection):
        return upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=NOW,
        )


def _two_people_with_edge(
    connection: sqlite3.Connection,
    *,
    name: str = "Alex Smith",
    a_non_name: tuple[tuple[str, str], ...] = (("profession_or_role", "sculptor"),),
    b_non_name: tuple[tuple[str, str], ...] = (("profession_or_role", "painter"),),
) -> tuple[int, int, int, int, int, int, int]:
    """Return run, attempt, inspection, mention_a, person_a, person_b, relation."""
    run_id, attempt_id, inspection_id, mention_a = _seed_mention_graph(
        connection,
        exact_name=name,
        non_name_facts=a_non_name,
        entry_id="entry-a",
    )
    mention_b = _add_mention(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
        exact_name=name,
        non_name_facts=b_non_name,
        entry_id="entry-b",
    )
    with immediate(connection):
        person_a = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_a,
            exact_name=name,
            kind="display",
            origin_kind="person_mention",
            origin_mention_id=mention_a,
            observed_at=NOW,
        )
        person_b = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=_OTHER,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_b,
            exact_name=name,
            kind="display",
            origin_kind="person_mention",
            origin_mention_id=mention_b,
            observed_at=NOW,
        )
    er_a = _link_mention_to_person(
        connection,
        run_id=run_id,
        mention_id=mention_a,
        person_id=person_a,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
    )
    _link_mention_to_person(
        connection,
        run_id=run_id,
        mention_id=mention_b,
        person_id=person_b,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
    )
    relation_id = _open_edge_between_people(
        connection,
        run_id=run_id,
        person_a=person_a,
        person_b=person_b,
        er_id=er_a,
    )
    return (
        run_id,
        attempt_id,
        inspection_id,
        mention_a,
        person_a,
        person_b,
        relation_id,
    )


def test_k21_first_pass_uncertain_n_edges_zero_reconsider(
    connection: sqlite3.Connection,
) -> None:
    """K21: first-pass uncertain with N candidates ⇒ N edges, zero reconsider work."""
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Alex Smith"
    )
    candidate_ids: list[int] = []
    with immediate(connection):
        for index in range(2):
            peer = insert_person(
                connection,
                run_id=run_id,
                display_name="Alex Smith",
                identity_fingerprint=f"{index:064x}",
                created_at=NOW,
            )
            upsert_sourced_name(
                connection,
                person_id=peer,
                exact_name="Alex Smith",
                kind="display",
                origin_kind="manual",
                origin_mention_id=None,
                observed_at=NOW,
            )
            candidate_ids.append(peer)

    config = _main_config()
    profile = _profile()
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert result == "scheduled"

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(_resolve_output(outcome="uncertain"))],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    edges = connection.execute(
        """
        SELECT COUNT(*) AS n FROM person_relation
         WHERE kind = 'possible_same_person' AND status = 'active'
        """
    ).fetchone()
    assert edges is not None
    assert int(edges["n"]) == len(candidate_ids)
    assert _count_work(connection, task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE) == 0


def test_later_non_name_material_change_schedules_reconsider(
    connection: sqlite3.Connection,
) -> None:
    """After an edge exists, attaching material to one side schedules reconsider."""
    (
        run_id,
        _attempt_id,
        _inspection_id,
        _mention_a,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del person_b

    pre_existing = frozenset({relation_id})
    # Simulate material attach settlement: recompute fingerprint, then schedule.
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        n = maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=pre_existing,
            run_id=run_id,
            config=_main_config(),
            now=NOW,
        )
    assert n == 1
    work = connection.execute(
        """
        SELECT task_type, subject_kind, subject_id, priority, state
          FROM work_item
         WHERE task_type = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE,),
    ).fetchone()
    assert work is not None
    assert work["subject_kind"] == "person_relation"
    assert work["subject_id"] == relation_id
    assert work["priority"] == RECONSIDER_PERSON_PRIORITY
    assert work["state"] == "pending"


def test_same_settlement_edges_excluded_from_pre_existing(
    connection: sqlite3.Connection,
) -> None:
    """K21: edges not in pre_existing_relation_ids are not scheduled."""
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        _person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    with immediate(connection):
        n = maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset(),  # same-settlement: empty
            run_id=run_id,
            config=_main_config(),
            now=NOW,
        )
    assert n == 0
    assert _count_work(connection, task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE) == 0
    del relation_id


def test_subject_is_fingerprint_changed_side(
    connection: sqlite3.Connection,
) -> None:
    """Prepare input from fingerprint-changed side; peer is sole candidate."""
    (
        run_id,
        attempt_id,
        inspection_id,
        mention_a,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del inspection_id
    # person_a is the lower id when inserted first; schedule with A as changed side.
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_resolve_output(outcome="uncertain")),
        ],
    )
    handler = build_reconsideration_handler(
        connection, client=client, config=config, profile=profile
    )
    work_row = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert work_row is not None
    # Insert a routing-matched compatible inspection for the claimed run.
    routing_fp = routing_fingerprint(config.openrouter.routing)
    connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, MODEL, MODEL, routing_fp, NOW),
    )
    # Claim as if engine did.
    connection.execute(
        """
        UPDATE work_item
           SET state = 'running', claimed_by_run_id = ?, updated_at = ?
         WHERE id = ?
        """,
        (run_id, NOW, work_row["id"]),
    )
    connection.commit()
    work_item = WorkItem(
        id=int(work_row["id"]),
        task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_relation",
        subject_id=relation_id,
        fingerprint=work_row["fingerprint"],
        required=True,
        priority=RECONSIDER_PERSON_PRIORITY,
        state=WorkState.RUNNING,
    )
    assert handler.prepare is not None
    preparation = handler.prepare(work_item)
    prepared = cast(Any, preparation.payload)
    assert prepared is not None
    assert prepared.subject_person_id == person_a
    assert prepared.peer_person_id == person_b
    assert prepared.resolve_input.person_mention_id == mention_a
    assert len(prepared.resolve_input.candidates) == 1
    assert prepared.resolve_input.candidates[0].person_id == person_b


def test_subject_is_higher_person_when_higher_is_trigger(
    connection: sqlite3.Connection,
) -> None:
    """Discriminating case: higher person_id is the fingerprint-changed side.

    Killing this would leave only the lower-id default and pass the lower-trigger
    test without implementing subject = changed side.
    """
    (
        run_id,
        attempt_id,
        inspection_id,
        mention_a,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del inspection_id
    assert person_b > person_a
    mention_b = connection.execute(
        """
        SELECT id FROM person_mention
         WHERE person_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (person_b,),
    ).fetchone()
    assert mention_b is not None
    mention_b_id = int(mention_b["id"])
    assert mention_b_id != mention_a

    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        # Trigger side is the *higher* person id.
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_b,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    handler = build_reconsideration_handler(
        connection,
        client=ScriptedLlmClient(inspection=_compatible_inspection()),
        config=config,
        profile=profile,
    )
    work_row = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert work_row is not None
    # Fingerprint must encode higher as subject (not lower-id default).
    expected_fp = reconsider_task_fingerprint(
        person_relation_id=relation_id,
        subject_identity_fingerprint=connection.execute(
            "SELECT identity_fingerprint FROM person WHERE id = ?",
            (person_b,),
        ).fetchone()["identity_fingerprint"],
        peer_identity_fingerprint=connection.execute(
            "SELECT identity_fingerprint FROM person WHERE id = ?",
            (person_a,),
        ).fetchone()["identity_fingerprint"],
        config=config.tasks.resolve_person_entity,
    )
    assert work_row["fingerprint"] == expected_fp

    routing_fp = routing_fingerprint(config.openrouter.routing)
    connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, MODEL, MODEL, routing_fp, NOW),
    )
    connection.execute(
        """
        UPDATE work_item
           SET state = 'running', claimed_by_run_id = ?, updated_at = ?
         WHERE id = ?
        """,
        (run_id, NOW, work_row["id"]),
    )
    connection.commit()
    work_item = WorkItem(
        id=int(work_row["id"]),
        task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_relation",
        subject_id=relation_id,
        fingerprint=work_row["fingerprint"],
        required=True,
        priority=RECONSIDER_PERSON_PRIORITY,
        state=WorkState.RUNNING,
    )
    assert handler.prepare is not None
    prepared = cast(Any, handler.prepare(work_item).payload)
    assert prepared is not None
    assert prepared.subject_person_id == person_b
    assert prepared.peer_person_id == person_a
    assert prepared.resolve_input.person_mention_id == mention_b_id
    assert len(prepared.resolve_input.candidates) == 1
    assert prepared.resolve_input.candidates[0].person_id == person_a


def test_dismiss_outcome_dismisses_edge(connection: sqlite3.Connection) -> None:
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del person_b
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_resolve_output(outcome="different_people"))
        ],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    relation = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()
    assert relation is not None
    assert relation["status"] == "dismissed"
    er = connection.execute(
        """
        SELECT disposition, semantic_outcome, created_person_id, person_mention_id,
               person_relation_id
          FROM entity_resolution_observation
         WHERE person_relation_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    assert er is not None
    assert er["disposition"] == "completed"
    assert er["semantic_outcome"] == "different_people"
    assert er["created_person_id"] is None
    assert er["person_mention_id"] is None
    assert er["person_relation_id"] == relation_id


def test_uncertain_outcome_leaves_edge_active(
    connection: sqlite3.Connection,
) -> None:
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        _person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(_resolve_output(outcome="uncertain"))],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    relation = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()
    assert relation is not None
    assert relation["status"] == "active"
    er = connection.execute(
        """
        SELECT semantic_outcome, created_person_id
          FROM entity_resolution_observation
         WHERE person_relation_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    assert er is not None
    assert er["semantic_outcome"] == "uncertain"
    assert er["created_person_id"] is None


def test_same_person_guardrails_fail_leaves_edge_active(
    connection: sqlite3.Connection,
) -> None:
    """Name-only pair never auto-merges; K22 ER has created_person_id NULL."""
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(
        connection,
        a_non_name=(),  # name-only; facts stripped below
        b_non_name=(),
    )
    # Rebuild as name-only: strip non-name facts from both mentions.
    connection.execute("DELETE FROM mention_identity_fact")
    connection.commit()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=_main_config(),
            now=NOW,
        )

    config = _main_config()
    profile = _profile()
    # same_person without supporting facts → guardrails fail.
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(
                    outcome="same_person",
                    selected_person_id=person_b,
                    supporting=(),
                )
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    relation = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()
    assert relation is not None
    assert relation["status"] == "active"
    er = connection.execute(
        """
        SELECT semantic_outcome, created_person_id, rationale
          FROM entity_resolution_observation
         WHERE person_relation_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    assert er is not None
    assert er["semantic_outcome"] == "same_person"
    assert er["created_person_id"] is None
    assert MERGE_GUARDRAILS_FAILED_NOTE.strip() in er["rationale"]
    assert READY_TO_MERGE_NOTE not in er["rationale"]
    # No merge rows.
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM person_relation WHERE kind = 'merge'"
        ).fetchone()["n"]
        == 0
    )


def test_same_person_guardrails_pass_ready_to_merge(
    connection: sqlite3.Connection,
) -> None:
    """K18 positive path: guardrails pass ⇒ ready-to-merge note; edge stays active.

    Both sides have non-name facts; model returns same_person for the peer with
    a validated supporting fact id. Task 8 merge is not present, so no merge
    relation is written and created_person_id stays NULL (K22).
    """
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    # Subject mention non-name fact local_id is "fact-1" (see _two_people_with_edge).
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(
                    outcome="same_person",
                    selected_person_id=person_b,
                    supporting=("fact-1",),
                )
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    relation = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()
    assert relation is not None
    assert relation["status"] == "active"
    er = connection.execute(
        """
        SELECT semantic_outcome, created_person_id, rationale, selected_person_id,
               person_mention_id
          FROM entity_resolution_observation
         WHERE person_relation_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    assert er is not None
    assert er["semantic_outcome"] == "same_person"
    assert er["created_person_id"] is None
    assert er["person_mention_id"] is None
    assert er["selected_person_id"] == person_b
    assert READY_TO_MERGE_NOTE.strip() in er["rationale"]
    assert MERGE_GUARDRAILS_FAILED_NOTE not in er["rationale"]
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM person_relation WHERE kind = 'merge'"
        ).fetchone()["n"]
        == 0
    )
    # Both people remain canonical (no merge applied).
    for person_id in (person_a, person_b):
        row = connection.execute(
            "SELECT merged_into_person_id FROM person WHERE id = ?",
            (person_id,),
        ).fetchone()
        assert row is not None
        assert row["merged_into_person_id"] is None


def test_missing_peer_prepare_refuse(connection: sqlite3.Connection) -> None:
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    # Dismiss the relation so prepare sees inactive edge (missing/unusable peer path).
    connection.execute(
        "UPDATE person_relation SET status = 'dismissed' WHERE id = ?",
        (relation_id,),
    )
    connection.commit()

    client = ScriptedLlmClient(inspection=_compatible_inspection())

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)
    assert client.generate_calls == []
    work = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert work is not None
    assert work["state"] == "failed_permanent"
    # No relation-scoped ER from prepare refuse (first attempt, no persist_failure).
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM entity_resolution_observation
             WHERE person_relation_id = ?
            """,
            (relation_id,),
        ).fetchone()["n"]
        == 0
    )


def test_missing_peer_person_prepare_refuse(
    connection: sqlite3.Connection,
) -> None:
    """Peer person row deleted mid-flight ⇒ prepare refuse, no generation."""
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    # Break the peer: clear FKs that block delete, then remove person_b.
    connection.execute(
        "UPDATE person_mention SET person_id = NULL WHERE person_id = ?",
        (person_b,),
    )
    connection.execute(
        "UPDATE sourced_name SET origin_mention_id = NULL WHERE person_id = ?",
        (person_b,),
    )
    # Relation still references person_b; prepare loads endpoints then looks up peer.
    # Simulate missing peer by pointing relation at a non-existent id via raw update
    # after disabling FK briefly is not allowed; instead null out and use
    # prepare on a work item whose relation endpoints include a deleted id.
    # Safer approach: call prepare directly with relation after deleting peer
    # if FK permits. person_relation has FK to person — delete will fail.
    # Call prepare and force missing peer by monkeypatching canonical lookup.
    work_row = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert work_row is not None
    connection.execute(
        """
        UPDATE work_item
           SET state = 'running', claimed_by_run_id = ?, updated_at = ?
         WHERE id = ?
        """,
        (run_id, NOW, work_row["id"]),
    )
    connection.commit()

    handler = build_reconsideration_handler(
        connection,
        client=ScriptedLlmClient(inspection=_compatible_inspection()),
        config=config,
        profile=profile,
    )
    work_item = WorkItem(
        id=int(work_row["id"]),
        task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_relation",
        subject_id=relation_id,
        fingerprint=work_row["fingerprint"],
        required=True,
        priority=RECONSIDER_PERSON_PRIORITY,
        state=WorkState.RUNNING,
    )

    def boom(_connection: sqlite3.Connection, person_id: int) -> int:
        if person_id == person_b:
            raise LookupError(f"person {person_id} does not exist")
        return person_id

    import notable_person_finder.people.service as service_mod

    original = service_mod.canonical_person_id
    service_mod.canonical_person_id = boom  # type: ignore[assignment]
    try:
        with pytest.raises(ValueError, match=RESOLVE_PREPARE_REFUSED_PREFIX) as exc:
            assert handler.prepare is not None
            handler.prepare(work_item)
        assert "missing_peer" in str(exc.value)
    finally:
        service_mod.canonical_person_id = original


def test_relation_scoped_failed_er(connection: sqlite3.Connection) -> None:
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del person_b
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        recompute_identity_fingerprint(connection, person_a)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            ProviderFailure(
                FailureCategory.AUTHENTICATION,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=False,
                detail="auth failed",
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=seed,
        max_attempts=1,
    )

    er = connection.execute(
        """
        SELECT disposition, person_mention_id, person_relation_id,
               created_person_id, failure_category, attempt_id
          FROM entity_resolution_observation
         WHERE person_relation_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (relation_id,),
    ).fetchone()
    assert er is not None
    assert er["disposition"] == "failed"
    assert er["person_mention_id"] is None
    assert er["person_relation_id"] == relation_id
    assert er["created_person_id"] is None
    assert er["failure_category"] == "authentication"
    assert er["attempt_id"] is not None
    # Edge remains active.
    relation = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()
    assert relation is not None
    assert relation["status"] == "active"


def test_first_pass_same_person_attach_schedules_preexisting_edge(
    connection: sqlite3.Connection,
) -> None:
    """Integration: first-pass same_person material attach on a person with edges."""
    (
        run_id,
        attempt_id,
        inspection_id,
        _mention_a,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    del attempt_id, inspection_id, person_b
    # New unresolved mention that name-matches person_a (and person_b).
    new_mention = _add_mention(
        connection,
        run_id=run_id,
        attempt_id=connection.execute(
            "SELECT id FROM attempt ORDER BY id LIMIT 1"
        ).fetchone()["id"],
        inspection_id=connection.execute(
            "SELECT id FROM model_inspection ORDER BY id LIMIT 1"
        ).fetchone()["id"],
        exact_name="Alex Smith",
        non_name_facts=(("place", "Paris"),),
        entry_id="entry-c",
        title="Alex Smith in Paris",
    )
    config = _main_config()
    profile = _profile()
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=new_mention,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert result == "scheduled"

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=person_a)
            ),
            # Reconsider of the pre-existing edge (may run in the same drain).
            _generation_result(_resolve_output(outcome="uncertain")),
        ],
    )

    def seed(engine_run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (new_mention,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] == person_a
    # Pre-existing edge must have reconsider work (not opened in this settlement).
    reconsider = connection.execute(
        """
        SELECT subject_id, state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert reconsider is not None
    assert reconsider["state"] in {
        "pending",
        "succeeded",
        "failed_permanent",
        "running",
        "deferred",
    }
    # At least one generate call for first-pass same_person; reconsider may follow.
    assert len(client.generate_calls) >= 1


def test_cli_registers_reconsider_handler() -> None:
    import inspect

    from notable_person_finder.cli import main as cli_main

    run_source = inspect.getsource(cli_main.command_run)
    assert "build_reconsideration_handler" in run_source
    assert "RECONSIDER_PERSON_ENTITY_TASK_TYPE" in run_source


def test_reconsider_fingerprint_includes_relation_and_both_sides(
    connection: sqlite3.Connection,
) -> None:
    (
        run_id,
        _a,
        _i,
        _m,
        person_a,
        person_b,
        relation_id,
    ) = _two_people_with_edge(connection)
    config = _main_config()
    with immediate(connection):
        fp_a = recompute_identity_fingerprint(connection, person_a)
        fp_b = recompute_identity_fingerprint(connection, person_b)
        maybe_schedule_reconsideration_for_person(
            connection,
            person_id=person_a,
            pre_existing_relation_ids=frozenset({relation_id}),
            run_id=run_id,
            config=config,
            now=NOW,
        )
    work = connection.execute(
        "SELECT fingerprint FROM work_item WHERE task_type = ?",
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE,),
    ).fetchone()
    assert work is not None
    expected = reconsider_task_fingerprint(
        person_relation_id=relation_id,
        subject_identity_fingerprint=fp_a,
        peer_identity_fingerprint=fp_b,
        config=config.tasks.resolve_person_entity,
    )
    assert work["fingerprint"] == expected
    # Swapped sides differ when fingerprints differ.
    swapped = reconsider_task_fingerprint(
        person_relation_id=relation_id,
        subject_identity_fingerprint=fp_b,
        peer_identity_fingerprint=fp_a,
        config=config.tasks.resolve_person_entity,
    )
    assert swapped != expected
