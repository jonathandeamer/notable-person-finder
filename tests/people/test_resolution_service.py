"""First-pass resolution: ensure, seed, handler model path, K3/K17/K21."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

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
    compute_identity_fingerprint,
    insert_person,
    upsert_sourced_name,
)
from notable_person_finder.people.repository import (
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    insert_entity_resolution_observation,
    mechanical_search_name,
    upsert_active_possible_same_person,
)
from notable_person_finder.people.resolution import first_pass_task_fingerprint
from notable_person_finder.people.service import (
    CREATED_NEW_VALIDATED_OUTPUT_JSON,
    INSPECT_MODEL_TASK_TYPE,
    RESOLVE_PERSON_PRIORITY,
    SKIPPED_MATCH_KEY_RATIONALE,
    _identity_facts_for_mention,
    _signals_for_mention,
    build_inspection_handler,
    build_resolution_handler,
    ensure_resolution_for_mention,
    schedule_resolution_for_observation,
    seed_unresolved_mentions,
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
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import ReportArtifact, RunEngine
from notable_person_finder.runs.models import WorkState
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
    feed_key: str = "feed-a",
    source_entry_id: str = "entry-a",
    material_fingerprint: str = _HASH,
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
        (material_fingerprint, NOW, run_id, NOW, NOW),
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
        (run_id, work, NOW, moment(1), material_fingerprint),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (feed_key, NOW, NOW),
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
        (feed, fetch, run_id, source_entry_id, title, summary, NOW),
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
        (run_id, attempt_id, MODEL, MODEL, material_fingerprint, NOW),
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
            material_fingerprint,
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
        provider_request_id="gen-resolve-1",
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


def _count_attempts(connection: sqlite3.Connection) -> int:
    row = connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()
    assert row is not None
    return int(row["n"])


def test_empty_candidates_created_new_zero_work_and_attempts(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    attempts_before = _count_attempts(connection)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(inspection=_compatible_inspection())

    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )

    assert result == "created_new"
    assert _count_work(connection, task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE) == 0
    assert _count_attempts(connection) == attempts_before
    assert client.generate_calls == []

    mention = connection.execute(
        """
        SELECT person_id, current_entity_resolution_observation_id
          FROM person_mention WHERE id = ?
        """,
        (mention_id,),
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is not None
    er = connection.execute(
        "SELECT * FROM entity_resolution_observation WHERE id = ?",
        (mention["current_entity_resolution_observation_id"],),
    ).fetchone()
    assert er is not None
    assert er["disposition"] == "completed"
    assert er["semantic_outcome"] == "created_new"
    assert er["attempt_id"] is None
    assert er["validated_output_json"] == CREATED_NEW_VALIDATED_OUTPUT_JSON
    assert er["candidate_person_ids_json"] == "[]"
    assert er["created_person_id"] == mention["person_id"]


def test_do_not_research_ineligible(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, outcome="do_not_research"
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert result == "ineligible"
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM entity_resolution_observation"
        ).fetchone()["n"]
        == 0
    )
    assert connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()["n"] == 0


def test_mononym_research_is_eligible_created_new(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Madonna", title="Madonna performs"
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert result == "created_new"
    person = connection.execute(
        "SELECT display_name FROM person ORDER BY id DESC LIMIT 1"
    ).fetchone()
    assert person is not None
    assert person["display_name"] == "Madonna"


def test_empty_match_key_skipped(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection, exact_name="Alex")
    monkeypatch.setattr(
        "notable_person_finder.people.service.match_key", lambda _value: ""
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert result == "skipped"
    er = connection.execute(
        """
        SELECT disposition, rationale, person_mention_id
          FROM entity_resolution_observation
        """
    ).fetchone()
    assert er is not None
    assert er["disposition"] == "skipped"
    assert er["rationale"] == SKIPPED_MATCH_KEY_RATIONALE
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is None


def test_count_resolution_eligible_excludes_skipped_and_resolved(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K24 digest/status counter: skipped and resolved mentions are not eligible."""
    from notable_person_finder.people.repository import (
        insert_entity_resolution_observation,
        point_mention_current_er,
    )
    from notable_person_finder.people.resolution import first_pass_task_fingerprint
    from notable_person_finder.people.service import (
        _identity_facts_for_mention,
        _signals_for_mention,
        count_resolution_eligible_mentions,
        is_resolution_eligible_mention,
    )
    from tests.ingestion.helpers import immediate

    run_id, _a, _i, open_mention = _seed_mention_graph(
        connection,
        exact_name="Open Person",
        title="Open Person wins",
        material_fingerprint="1" * 64,
    )
    run2, _a2, _i2, skip_mention = _seed_mention_graph(
        connection,
        exact_name="Skip Person",
        title="Skip Person appears",
        source_entry_id="entry-skip",
        feed_key="feed-skip",
        material_fingerprint="2" * 64,
    )
    config = _main_config()
    profile = _profile()
    assert count_resolution_eligible_mentions(connection, config=config) == 2
    ensure_resolution_for_mention(
        connection,
        person_mention_id=open_mention,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    # Closed via created_new: must not remain K24-eligible.
    assert not is_resolution_eligible_mention(
        connection,
        person_mention_id=open_mention,
        config=config,
        profile=profile,
    )
    # Skipped ER for the second mention (empty match_key path).
    monkeypatch.setattr(
        "notable_person_finder.people.service.match_key", lambda _value: ""
    )
    skip_result = ensure_resolution_for_mention(
        connection,
        person_mention_id=skip_mention,
        run_id=run2,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert skip_result == "skipped"
    monkeypatch.undo()
    # After restoring match_key, skipped disposition still closes eligibility.
    assert not is_resolution_eligible_mention(
        connection,
        person_mention_id=skip_mention,
        config=config,
        profile=profile,
    )
    assert count_resolution_eligible_mentions(connection, config=config) == 0
    # Positive control: a failed ER also closes eligibility for its fingerprint.
    run3, attempt_id, inspection_id, failed_mention = _seed_mention_graph(
        connection,
        exact_name="Failed Person",
        title="Failed Person noted",
        source_entry_id="entry-fail",
        feed_key="feed-fail",
        material_fingerprint="3" * 64,
    )
    assert is_resolution_eligible_mention(
        connection,
        person_mention_id=failed_mention,
        config=config,
        profile=profile,
    )
    row = connection.execute(
        "SELECT exact_name, search_name, outcome FROM person_mention WHERE id = ?",
        (failed_mention,),
    ).fetchone()
    fp = first_pass_task_fingerprint(
        person_mention_id=failed_mention,
        mention_outcome=str(row["outcome"]),
        exact_name=str(row["exact_name"]),
        search_name=str(row["search_name"] or row["exact_name"]),
        identity_facts=_identity_facts_for_mention(
            connection, person_mention_id=failed_mention
        ),
        signals=_signals_for_mention(connection, person_mention_id=failed_mention),
        config=config.tasks.resolve_person_entity,
    )
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=failed_mention,
            person_relation_id=None,
            run_id=run3,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint=fp,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            rationale="permanent failure",
            failure_category="malformed_response",
            observed_at=NOW,
        )
        point_mention_current_er(
            connection,
            person_mention_id=failed_mention,
            observation_id=er_id,
            person_id=None,
        )
    assert not is_resolution_eligible_mention(
        connection,
        person_mention_id=failed_mention,
        config=config,
        profile=profile,
    )
    assert count_resolution_eligible_mentions(connection, config=config) == 0


def test_fingerprint_reuse(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    config = _main_config()
    profile = _profile()
    first = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert first == "created_new"
    people_before = connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()[
        "n"
    ]
    second = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert second == "reused"
    people_after = connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()[
        "n"
    ]
    assert people_after == people_before


def test_name_equality_with_candidates_schedules_model(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Alex Smith"
    )
    with immediate(connection):
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER,
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

    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert result == "scheduled"
    work = connection.execute(
        """
        SELECT task_type, priority, state, subject_kind, subject_id
          FROM work_item WHERE task_type = ?
        """,
        (RESOLVE_PERSON_ENTITY_TASK_TYPE,),
    ).fetchone()
    assert work is not None
    assert work["priority"] == RESOLVE_PERSON_PRIORITY
    assert work["state"] == "pending"
    assert work["subject_kind"] == "person_mention"
    assert work["subject_id"] == mention_id
    assert connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()["n"] == 1


def test_txn_recheck_schedules_when_peer_appears(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    config = _main_config()
    profile = _profile()
    calls = {"n": 0}
    real_retrieve = __import__(
        "notable_person_finder.people.service",
        fromlist=["_retrieve_candidates_for_mention"],
    )._retrieve_candidates_for_mention

    def flaky_retrieve(connection: sqlite3.Connection, **kwargs: object) -> tuple:
        calls["n"] += 1
        if calls["n"] == 1:
            return ()
        # Second call (re-check): inject a peer inside ensure's open txn.
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER,
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
        return real_retrieve(connection, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        "notable_person_finder.people.service._retrieve_candidates_for_mention",
        flaky_retrieve,
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert result == "scheduled"
    assert _count_work(connection, task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE) == 1
    # Peer exists; mention not linked via created_new.
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is None


def test_created_new_alone_has_zero_peer_edges(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection,
        non_name_facts=(("profession_or_role", "sculptor"),),
    )
    ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'possible_same_person'
            """
        ).fetchone()["n"]
        == 0
    )


def test_peer_edge_after_create_when_peer_has_non_name(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Force empty retrieve then inject peer before create so re-check still empty once.

    Simulate peer appearing after re-check by patching retrieve to always empty
    while a name-matched peer exists — proves peer-edge scan opens edges when
    create proceeds (race where re-check is imperfect / eligibility differs).
    """
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection,
        exact_name="Alex Smith",
        non_name_facts=(("profession_or_role", "sculptor"),),
    )
    with immediate(connection):
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER,
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

    monkeypatch.setattr(
        "notable_person_finder.people.service._retrieve_candidates_for_mention",
        lambda *_a, **_k: (),
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert result == "created_new"
    edges = connection.execute(
        """
        SELECT person_id_a, person_id_b, status
          FROM person_relation
         WHERE kind = 'possible_same_person' AND status = 'active'
        """
    ).fetchall()
    assert len(edges) == 1
    ends = {edges[0]["person_id_a"], edges[0]["person_id_b"]}
    created = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert created is not None
    assert peer in ends
    assert created["person_id"] in ends


def test_peer_edge_skips_name_only_pair(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Alex Smith"
    )  # no non-name facts
    with immediate(connection):
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER,
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
    monkeypatch.setattr(
        "notable_person_finder.people.service._retrieve_candidates_for_mention",
        lambda *_a, **_k: (),
    )
    ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=_main_config(),
        profile=_profile(),
        now=NOW,
    )
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'possible_same_person'
            """
        ).fetchone()["n"]
        == 0
    )


def _prepare_resolve_work(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    mention_id: int,
    candidate_name: str = "Alex Smith",
) -> int:
    """Create a name-matched peer and schedule resolve work; return peer person_id."""
    with immediate(connection):
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name=candidate_name,
            identity_fingerprint=_OTHER,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=peer,
            exact_name=candidate_name,
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
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
    return peer


def test_model_same_person_links_selected(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, inspection_id, mention_id = _seed_mention_graph(connection)
    del inspection_id
    peer = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=peer)
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        # Resolve work already scheduled under bootstrap run; ensure inspect.
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] == peer
    er = connection.execute(
        """
        SELECT semantic_outcome, selected_person_id, created_person_id, attempt_id
          FROM entity_resolution_observation
         WHERE person_mention_id = ? AND disposition = 'completed'
         ORDER BY id DESC LIMIT 1
        """,
        (mention_id,),
    ).fetchone()
    assert er is not None
    assert er["semantic_outcome"] == "same_person"
    assert er["selected_person_id"] == peer
    assert er["created_person_id"] is None
    assert er["attempt_id"] is not None
    assert len(client.generate_calls) == 1


def test_same_person_non_name_attach_changes_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    """C1: same_person recompute must see the linked mention's non-name facts.

    Operational fingerprint reads facts only from mentions with person_id set.
    Recompute-before-link would leave the fingerprint at name-only and this
    assertion fails.
    """
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection,
        exact_name="Alex Smith",
        non_name_facts=(("profession_or_role", "sculptor"),),
    )
    peer = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    with immediate(connection):
        fp_before = compute_identity_fingerprint(connection, peer)
        connection.execute(
            "UPDATE person SET identity_fingerprint = ? WHERE id = ?",
            (fp_before, peer),
        )
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=peer)
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)
    person = connection.execute(
        "SELECT identity_fingerprint FROM person WHERE id = ?", (peer,)
    ).fetchone()
    assert person is not None
    assert person["identity_fingerprint"] != fp_before
    with immediate(connection):
        # Linked mention must contribute the non-name fact to the projection.
        assert (
            compute_identity_fingerprint(connection, peer)
            == person["identity_fingerprint"]
        )
        mention = connection.execute(
            "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
        ).fetchone()
    assert mention is not None
    assert mention["person_id"] == peer


def test_same_person_non_name_with_preexisting_edge_schedules_reconsider(
    connection: sqlite3.Connection,
) -> None:
    """C1+K21: non-name attach changes fingerprint and re-arms pre-existing edges."""
    run_id, attempt_id, inspection_id, seed_mention = _seed_mention_graph(
        connection,
        exact_name="Alex Smith",
        source_entry_id="entry-seed",
    )
    # Attaching mention carries a new non-name fact not yet on selected.
    attach_mention = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        )
        SELECT triage_observation_id, 2, exact_name, search_name,
               'research', '["p1"]', 'attach'
          FROM person_mention WHERE id = ?
        """,
        (seed_mention,),
    ).lastrowid
    assert attach_mention is not None
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value,
            supporting_passage_ids_json
        ) VALUES (?, 'f-place', 'place', 'Paris', '["p1"]')
        """,
        (attach_mention,),
    )
    connection.commit()

    with immediate(connection):
        selected = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=selected,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="person_mention",
            origin_mention_id=seed_mention,
            observed_at=NOW,
        )
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER,
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
        del attempt_id, inspection_id  # created_new is schedule-time (no attempt)
        er_seed = insert_entity_resolution_observation(
            connection,
            person_mention_id=seed_mention,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=selected,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=CREATED_NEW_VALIDATED_OUTPUT_JSON,
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint=_HASH,
            rationale="seed link",
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
            (selected, er_seed, seed_mention),
        )
        relation_id = upsert_active_possible_same_person(
            connection,
            person_id_a=selected,
            person_id_b=peer,
            run_id=run_id,
            created_by_observation_id=er_seed,
            now=NOW,
        )
        fp_before = compute_identity_fingerprint(connection, selected)
        connection.execute(
            "UPDATE person SET identity_fingerprint = ? WHERE id = ?",
            (fp_before, selected),
        )

    config = _main_config()
    profile = _profile()
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=attach_mention,
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
                _resolve_output(outcome="same_person", selected_person_id=selected)
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    person = connection.execute(
        "SELECT identity_fingerprint FROM person WHERE id = ?", (selected,)
    ).fetchone()
    assert person is not None
    assert person["identity_fingerprint"] != fp_before
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


def test_same_person_name_only_attach_does_not_peer_scan_name_only_peers(
    connection: sqlite3.Connection,
) -> None:
    """I1: name-only same_person attach must not open edges to name-only peers."""
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Alex Smith"
    )  # name-only attach
    selected = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    with immediate(connection):
        name_only_peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint="c" * 64,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=name_only_peer,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=selected)
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

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
    assert edges["n"] == 0


def test_same_person_non_name_attach_opens_peer_edges(
    connection: sqlite3.Connection,
) -> None:
    """I1: non-name same_person attach peer-scans name-matched eligible peers."""
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection,
        exact_name="Alex Smith",
        non_name_facts=(("profession_or_role", "sculptor"),),
    )
    selected = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    with immediate(connection):
        # Name-matched peer that is not the selected person; creating mention
        # has non-name so K17 allows the edge even if peer is name-only.
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint="c" * 64,
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
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=selected)
            )
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)
    edges = connection.execute(
        """
        SELECT person_id_a, person_id_b FROM person_relation
         WHERE kind = 'possible_same_person' AND status = 'active'
        """
    ).fetchall()
    assert len(edges) == 1
    ends = {int(edges[0]["person_id_a"]), int(edges[0]["person_id_b"])}
    assert ends == {selected, peer}
    # K21: peer edges opened in this settlement must not schedule reconsider.
    assert _count_work(connection, task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE) == 0


def test_model_different_people_creates_without_edges(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, non_name_facts=(("profession_or_role", "painter"),)
    )
    peer = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_resolve_output(outcome="different_people"))
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is not None
    assert mention["person_id"] != peer
    # No edge to rejected candidate.
    edge_to_peer = connection.execute(
        """
        SELECT COUNT(*) AS n FROM person_relation
         WHERE kind = 'possible_same_person'
           AND ((person_id_a = ? AND person_id_b = ?)
             OR (person_id_a = ? AND person_id_b = ?))
        """,
        (peer, mention["person_id"], mention["person_id"], peer),
    ).fetchone()
    assert edge_to_peer is not None
    assert edge_to_peer["n"] == 0


def test_model_uncertain_k4_edges_all_candidates_name_only_zero_reconsider(
    connection: sqlite3.Connection,
) -> None:
    """K4: uncertain opens edges to every supplied candidate even when peer scan cannot.

    Name-only mention + name-only candidates: K17 peer scan must not open edges
    (name-only↔name-only). Edges that appear must come from the K4 uncertain
    loop. N≥2 candidates so deleting the loop cannot leave a single peer-scan
    edge as a false positive.
    """
    run_id, _a, _i, mention_id = _seed_mention_graph(
        connection, exact_name="Alex Smith"
    )  # no non-name facts
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
    assert len(candidate_ids) >= 2

    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(_resolve_output(outcome="uncertain"))],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(connection, _handlers(connection, client, config, profile), seed=seed)

    created = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert created is not None
    created_id = int(created["person_id"])
    assert created_id not in candidate_ids

    edges = connection.execute(
        """
        SELECT person_id_a, person_id_b FROM person_relation
         WHERE kind = 'possible_same_person' AND status = 'active'
         ORDER BY person_id_a, person_id_b
        """
    ).fetchall()
    # Exactly N edges — one per code-supplied candidate (K4); peer scan adds none.
    assert len(edges) == len(candidate_ids)
    edge_peers: set[int] = set()
    for row in edges:
        ends = {int(row["person_id_a"]), int(row["person_id_b"])}
        assert created_id in ends
        peer = next(person_id for person_id in ends if person_id != created_id)
        edge_peers.add(peer)
    assert edge_peers == set(candidate_ids)
    # K21: zero reconsider work in this settlement.
    assert _count_work(connection, task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE) == 0


def test_unseen_selected_id_is_malformed(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=999_999)
            ),
            _generation_result(
                _resolve_output(outcome="same_person", selected_person_id=999_999)
            ),
        ],
    )

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

        ensure_model_inspections_for_run(
            connection, run_id=engine_run_id, config=config, now=NOW
        )

    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=seed,
        max_attempts=2,
    )
    work = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (RESOLVE_PERSON_ENTITY_TASK_TYPE, mention_id),
    ).fetchone()
    assert work is not None
    assert work["state"] == "failed_permanent"
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is None


def test_empty_at_prepare_ensure_then_value_error(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    peer = _prepare_resolve_work(connection, run_id=run_id, mention_id=mention_id)
    del peer
    config = _main_config()
    profile = _profile()
    # At prepare time candidates become empty (peer deleted).
    monkeypatch.setattr(
        "notable_person_finder.people.service._retrieve_candidates_for_mention",
        lambda *_a, **_k: (),
    )
    client = ScriptedLlmClient(inspection=_compatible_inspection())

    def seed(engine_run_id: int) -> None:
        from notable_person_finder.people.service import (
            ensure_model_inspections_for_run,
        )

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
        (RESOLVE_PERSON_ENTITY_TASK_TYPE, mention_id),
    ).fetchone()
    assert work is not None
    assert work["state"] == "failed_permanent"
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is not None  # ensure created_new
    er = connection.execute(
        """
        SELECT disposition, semantic_outcome FROM entity_resolution_observation
         WHERE person_mention_id = ?
        """,
        (mention_id,),
    ).fetchone()
    assert er is not None
    assert er["semantic_outcome"] == "created_new"


def test_persist_failure_noops_when_er_exists(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _i, mention_id = _seed_mention_graph(connection)
    config = _main_config()
    profile = _profile()
    ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    # Build fingerprint and work item as if re-armed after a real call.
    facts = _identity_facts_for_mention(connection, person_mention_id=mention_id)
    signals = _signals_for_mention(connection, person_mention_id=mention_id)
    row = connection.execute(
        "SELECT exact_name, search_name, outcome FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert row is not None
    fingerprint = first_pass_task_fingerprint(
        person_mention_id=mention_id,
        mention_outcome=str(row["outcome"]),
        exact_name=str(row["exact_name"]),
        search_name=str(row["search_name"]),
        identity_facts=facts,
        signals=signals,
        config=config.tasks.resolve_person_entity,
    )
    work_id = repository.schedule_work(
        connection,
        task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_mention",
        subject_id=mention_id,
        fingerprint=fingerprint,
        required=True,
        priority=RESOLVE_PERSON_PRIORITY,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )
    connection.execute(
        """
        UPDATE work_item SET state = 'failed_permanent', claimed_by_run_id = ?
         WHERE id = ?
        """,
        (run_id, work_id),
    )
    # Attach a failed attempt for the work item (re-armed path).
    connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, failure_category, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?,
                  'failed', 'malformed_response', ?)
        """,
        (run_id, work_id, NOW, moment(1), fingerprint),
    )
    connection.commit()

    handler = build_resolution_handler(
        connection, client=ScriptedLlmClient(), config=config, profile=profile
    )
    from notable_person_finder.runs.models import WorkItem

    work_item = WorkItem(
        id=work_id,
        task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_mention",
        subject_id=mention_id,
        fingerprint=fingerprint,
        required=True,
        priority=RESOLVE_PERSON_PRIORITY,
        state=WorkState.FAILED_PERMANENT,
    )
    er_before = connection.execute(
        """
        SELECT COUNT(*) AS n FROM entity_resolution_observation
         WHERE person_mention_id = ?
        """,
        (mention_id,),
    ).fetchone()["n"]
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work_item,
            ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail="invalid",
            ),
        )
    er_after = connection.execute(
        """
        SELECT COUNT(*) AS n FROM entity_resolution_observation
         WHERE person_mention_id = ?
        """,
        (mention_id,),
    ).fetchone()["n"]
    assert er_after == er_before
    del attempt_id


def test_seed_unresolved_mentions_backfill(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    config = _main_config()
    profile = _profile()
    acted = seed_unresolved_mentions(
        connection, run_id=run_id, config=config, profile=profile, now=NOW
    )
    assert acted == 1
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention is not None
    assert mention["person_id"] is not None
    # Idempotent second seed.
    acted2 = seed_unresolved_mentions(
        connection, run_id=run_id, config=config, profile=profile, now=NOW
    )
    assert acted2 == 1  # reuse still counts as non-ineligible
    assert connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()["n"] == 1


def test_schedule_resolution_for_observation_txn_neutral(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id = _seed_mention_graph(connection)
    observation_id = connection.execute(
        "SELECT triage_observation_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()["triage_observation_id"]
    config = _main_config()
    profile = _profile()
    with immediate(connection):
        n = schedule_resolution_for_observation(
            connection,
            observation_id=observation_id,
            run_id=run_id,
            config=config,
            profile=profile,
            now=NOW,
        )
    assert n == 1
    assert connection.in_transaction is False


def test_cli_registers_resolve_handler_and_seed_order() -> None:
    import inspect

    from notable_person_finder.cli import main as cli_main

    source = inspect.getsource(cli_main._compose_seed)
    assert "seed_untriaged" in source
    assert "seed_unresolved_mentions" in source
    assert "ensure_model_inspections_for_run" in source
    # Order: untriaged before unresolved before inspections.
    assert source.index("seed_untriaged") < source.index("seed_unresolved_mentions")
    assert source.index("seed_unresolved_mentions") < source.index(
        "ensure_model_inspections_for_run"
    )
    run_source = inspect.getsource(cli_main.command_run)
    assert "build_resolution_handler" in run_source
    assert "RESOLVE_PERSON_ENTITY_TASK_TYPE" in run_source
