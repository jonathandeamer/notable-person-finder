"""match_wikipedia_identity handler, K21 inspect arming, K18 preflight settler."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from notable_person_finder.config.models import (
    BudgetConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    MediaWikiConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.people.repository import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE as REPO_MATCH_TYPE,
)
from notable_person_finder.people.repository import (
    insert_model_inspection,
    settle_active_tasks_after_permanent_preflight,
)
from notable_person_finder.people.service import (
    INSPECT_MODEL_TASK_TYPE,
    _inspection_work_fingerprint,
    ensure_model_inspections_for_run,
    inspection_ready,
    models_needed_for_run,
    routing_fingerprint,
    task_types_for_model,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.mediawiki import (
    MediaWikiPageFactsBatch,
    MediaWikiSearchPage,
)
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    PROVIDER,
    ModelInspectionRequest,
    ModelInspectionResult,
    StructuredGenerationRequest,
    StructuredGenerationResult,
)
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import WorkerPool
from notable_person_finder.wikipedia.matching import match_prompt_and_schema_hashes
from notable_person_finder.wikipedia.repository import (
    insert_query_forms,
    insert_wikipedia_identity_observation,
    list_page_facts_batches_for_plan,
    load_plan,
    load_wikipedia_identity_observation_by_fingerprint,
    mark_plan_status,
    mark_query_form_completed,
    open_plan,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MATCH_WIKIPEDIA_PRIORITY,
    build_match_wikipedia_handler,
    schedule_match_wikipedia_identity,
    wikipedia_match_model_needed,
)
from tests.ingestion.helpers import immediate, insert_run, moment
from tests.wikipedia.test_http_service import (
    ScriptedMediaWikiClient,
    _open_plan_with_forms,
    _page,
    _run_facts,
    _run_search,
    _search_page,
)

_HASH = "a" * 64
_OTHER = "b" * 64
NOW = moment()
MATCH_MODEL = "openai/gpt-match"
FIXTURES = Path(__file__).with_name("fixtures")


def _main_config(
    *,
    match_model: str = MATCH_MODEL,
    hard_budget: bool = False,
    max_candidates: int = 8,
) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run="1.00" if hard_budget else None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model=match_model,
                max_candidates=max_candidates,
                max_input_tokens=8192,
            )
        ),
    )


@dataclass
class ScriptedLlmClient:
    results: list[StructuredGenerationResult | ProviderFailure] = field(
        default_factory=list
    )
    generate_calls: list[StructuredGenerationRequest] = field(default_factory=list)
    _i: int = 0

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        self.generate_calls.append(request)
        if self._i >= len(self.results):
            raise RuntimeError("no further generation results scripted")
        result = self.results[self._i]
        self._i += 1
        if isinstance(result, ProviderFailure):
            raise result
        return result

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        raise AssertionError("match handler must not call inspect_model")


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _generation(raw: str, *, nano: int = 100) -> StructuredGenerationResult:
    return StructuredGenerationResult(
        raw_text=raw,
        configured_model_id=MATCH_MODEL,
        resolved_model_id=f"{MATCH_MODEL}-resolved",
        serving_provider=None,
        finish_reason="stop",
        refusal=None,
        usage=None,
        latency_ms=10,
        provider_request_id="req-1",
        actual_nano_usd=nano,
    )


def _person_with_name(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    name: str = "Alex Smith",
    fingerprint: str = _HASH,
) -> int:
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=fingerprint,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name=name,
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    return person_id


def _seed_ready_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str = _HASH,
    page_id: int = 101,
    title: str = "Alex Smith",
    truncated_unsafe: bool = False,
    partial_retrieval: bool = False,
    extract: str = "British sculptor known for public monuments.",
) -> int:
    """Open plan, completed form, page facts, ready_for_match."""
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=NOW,
        )
        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "variant_kind": "exact",
                    "query_text": title,
                },
            ),
        )
        form_id = form_ids[0]
        # Synthetic work+attempt so search observation FK is satisfied.
        search_work = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'mediawiki_search', 'wikipedia_query_form', ?, ?, 1, 50, ?,
                'succeeded', ?, ?, ?
            )
            """,
            (form_id, "c" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert search_work is not None
        search_attempt = conn.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (
                ?, ?, 'mediawiki', 'search_pages', 1, ?, ?, 'succeeded', ?
            )
            """,
            (run_id, search_work, NOW, moment(1), "c" * 64),
        ).lastrowid
        assert search_attempt is not None
        obs = conn.execute(
            """
            INSERT INTO mediawiki_search_observation (
                query_form_id, run_id, attempt_id, query_text, continuation_in,
                continuation_out, srlimit, hit_count, truncated, response_complete,
                observed_at
            ) VALUES (?, ?, ?, ?, NULL, NULL, 10, 1, 0, 1, ?)
            """,
            (form_id, run_id, search_attempt, title, NOW),
        ).lastrowid
        assert obs is not None
        conn.execute(
            """
            INSERT INTO mediawiki_search_hit (
                search_observation_id, rank, page_id, title
            ) VALUES (?, 1, ?, ?)
            """,
            (obs, page_id, title),
        )
        mark_query_form_completed(
            conn,
            form_id=form_id,
            continuations_used=0,
            hit_count=1,
            truncated=False,
        )
        upsert_mediawiki_page(
            conn,
            wiki_id="enwiki",
            page_id=page_id,
            canonical_title=title,
            canonical_url=f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
            namespace=0,
            is_disambiguation=False,
            is_missing=False,
            redirect_to_page_id=None,
            description="sculptor",
            extract=extract,
            categories_json='["British sculptors"]',
            last_observed_at=NOW,
            last_attempt_id=None,
        )
        batch = conn.execute(
            """
            INSERT INTO wikipedia_page_facts_batch (
                plan_id, ordinal, page_ids_json, status, wave, created_at,
                completed_at
            ) VALUES (?, 1, ?, 'completed', 1, ?, ?)
            """,
            (plan_id, json.dumps([page_id]), NOW, NOW),
        ).lastrowid
        assert batch is not None
        mark_plan_status(
            conn,
            plan_id=plan_id,
            status="ready_for_match",
            truncated_unsafe_for_negative=truncated_unsafe,
            partial_retrieval=partial_retrieval,
        )
    return plan_id


def _insert_compatible_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    model_id: str = MATCH_MODEL,
    prompt_price: int | None = 150,
    completion_price: int | None = 600,
) -> int:
    with immediate(connection):
        work = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'inspect_model', 'model', NULL, ?, 1, 20, ?, 'succeeded',
                ?, ?, ?
            )
            """,
            ("1" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert work is not None
        attempt = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (
                ?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'succeeded', ?
            )
            """,
            (run_id, work, NOW, moment(1), "1" * 64),
        ).lastrowid
        assert attempt is not None
        config = _main_config(match_model=model_id)
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt,
            configured_model_id=model_id,
            resolved_model_id=f"{model_id}-resolved",
            routing_fingerprint=routing_fingerprint(config.openrouter.routing),
            supported_parameters_json='["response_format"]',
            supports_strict_structured_output=True,
            pricing_usable=prompt_price is not None and completion_price is not None,
            prompt_unit_price_nano_usd=prompt_price,
            completion_unit_price_nano_usd=completion_price,
            compatibility="compatible",
            inspected_at=NOW,
        )
    return inspection_id


def _schedule_match(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str = _HASH,
) -> int:
    with immediate(connection) as conn:
        return schedule_match_wikipedia_identity(
            conn,
            person_id=person_id,
            material_fingerprint=material_fingerprint,
            run_id=run_id,
            now=NOW,
        )


def _work_item(connection: sqlite3.Connection, work_id: int) -> WorkItem:
    row = connection.execute(
        "SELECT * FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row is not None
    return WorkItem(
        id=int(row["id"]),
        task_type=str(row["task_type"]),
        subject_kind=str(row["subject_kind"]),
        subject_id=int(row["subject_id"]) if row["subject_id"] is not None else None,
        fingerprint=str(row["fingerprint"]),
        required=bool(row["required"]),
        priority=int(row["priority"]),
        state=WorkState(str(row["state"])),
    )


def _claim_and_attempt(
    connection: sqlite3.Connection,
    *,
    work_id: int,
    run_id: int,
) -> int:
    connection.execute(
        """
        UPDATE work_item
           SET state = 'running', claimed_by_run_id = ?, updated_at = ?
         WHERE id = ?
        """,
        (run_id, NOW, work_id),
    )
    row = connection.execute(
        "SELECT fingerprint FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row is not None
    attempt = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, ?, ?, 1, ?, NULL, NULL, ?)
        """,
        (run_id, work_id, PROVIDER, GENERATE_OPERATION, NOW, row["fingerprint"]),
    ).lastrowid
    connection.commit()
    assert attempt is not None
    return attempt


def _run_match(
    connection: sqlite3.Connection,
    *,
    client: ScriptedLlmClient,
    config: MainConfig,
    work_id: int,
    run_id: int,
    expect_execute: bool = True,
) -> tuple[Exception | None, ProviderFailure | None]:
    """Prepare → execute → persist. Returns (prepare_error, execute_error)."""
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_match_wikipedia_handler(connection, client=client, config=config)
    assert handler.prepare is not None
    assert handler.persist is not None
    assert handler.persist_failure is not None
    try:
        prepared = handler.prepare(work)
    except Exception as error:
        return error, None
    if not expect_execute:
        return None, None
    try:
        outcome = handler.execute(work, 1, prepared.payload)
    except ProviderFailure as error:
        connection.execute(
            """
            UPDATE work_item
               SET state = 'failed_permanent', reason = ?, updated_at = ?
             WHERE id = ?
            """,
            (str(error.category), NOW, work_id),
        )
        connection.execute(
            """
            UPDATE attempt
               SET finished_at = ?, outcome = 'failed', failure_category = ?
             WHERE work_item_id = ? AND finished_at IS NULL
            """,
            (moment(1), str(error.category), work_id),
        )
        connection.commit()
        with immediate(connection):
            handler.persist_failure(work, error)
        return None, error
    with immediate(connection):
        handler.persist(work, outcome)
    connection.execute(
        """
        UPDATE work_item SET state = 'succeeded', updated_at = ? WHERE id = ?
        """,
        (NOW, work_id),
    )
    connection.execute(
        """
        UPDATE attempt
           SET finished_at = ?, outcome = 'succeeded'
         WHERE work_item_id = ? AND finished_at IS NULL
        """,
        (moment(1), work_id),
    )
    connection.commit()
    return None, None


def _person_pointer(connection: sqlite3.Connection, person_id: int) -> int | None:
    row = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    assert row is not None
    value = row["current_wikipedia_identity_observation_id"]
    return None if value is None else int(value)


# ---------------------------------------------------------------------------
# Handler contract
# ---------------------------------------------------------------------------


def test_match_handler_pool_priority_provider_operation(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    handler = build_match_wikipedia_handler(
        connection, client=ScriptedLlmClient(), config=config
    )
    assert handler.task_type == MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE
    assert handler.provider == PROVIDER
    assert handler.operation == GENERATE_OPERATION
    assert handler.pool is WorkerPool.LLM
    assert handler.reserved_nano_usd == 0
    assert handler.ready is not None
    assert MATCH_WIKIPEDIA_PRIORITY == 55


def _model_output(
    outcome: str,
    *,
    selected_page_id: int | None = None,
    rationale: str = "Supplied biography extract aligns with the person.",
) -> str:
    return json.dumps(
        {
            "outcome": outcome,
            "selected_page_id": selected_page_id,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": rationale,
        }
    )


@pytest.mark.parametrize(
    ("outcome", "semantic", "selected"),
    [
        ("matching_page", "matching_page_found", 101),
        ("no_matching_page", "no_matching_page_found", None),
        ("uncertain", "uncertain_identity", None),
    ],
)
def test_three_model_outcomes_set_pointer_and_store_rationale(
    connection: sqlite3.Connection,
    outcome: str,
    semantic: str,
    selected: int | None,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    client = ScriptedLlmClient(
        results=[_generation(_model_output(outcome, selected_page_id=selected))]
    )
    config = _main_config()

    prep_err, exec_err = _run_match(
        connection, client=client, config=config, work_id=work_id, run_id=run_id
    )
    assert prep_err is None
    assert exec_err is None
    assert len(client.generate_calls) == 1

    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.disposition == "completed"
    assert obs.semantic_outcome == semantic
    assert obs.rationale
    assert _person_pointer(connection, person_id) == obs.id
    plan = load_plan(
        connection,
        plan_id=int(obs.plan_id) if obs.plan_id is not None else -1,
    )
    assert plan is not None
    assert plan.status == "completed"


def test_uncertain_sets_pointer_never_suppresses(
    connection: sqlite3.Connection,
) -> None:
    """Uncertain is a completed judgment with pointer (never a suppress claim)."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    client = ScriptedLlmClient(results=[_generation(_model_output("uncertain"))])
    prep_err, exec_err = _run_match(
        connection,
        client=client,
        config=_main_config(),
        work_id=work_id,
        run_id=run_id,
    )
    assert prep_err is None and exec_err is None
    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.semantic_outcome == "uncertain_identity"
    assert _person_pointer(connection, person_id) == obs.id


def test_unseen_page_id_rejected_as_malformed(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    client = ScriptedLlmClient(
        results=[_generation(_model_output("matching_page", selected_page_id=99999))]
    )
    prep_err, exec_err = _run_match(
        connection,
        client=client,
        config=_main_config(),
        work_id=work_id,
        run_id=run_id,
    )
    assert prep_err is None
    assert exec_err is not None
    assert exec_err.category is FailureCategory.MALFORMED_RESPONSE
    assert exec_err.retryable is True

    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.disposition == "failed"
    assert obs.failure_category == "invalid_model_output"
    assert _person_pointer(connection, person_id) is None
    plan_rows = connection.execute(
        "SELECT status FROM wikipedia_identity_plan WHERE person_id = ?",
        (person_id,),
    ).fetchone()
    assert plan_rows["status"] == "failed"


def test_truncated_unsafe_rejects_model_no_matching_page(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(
        connection, person_id=person_id, run_id=run_id, truncated_unsafe=True
    )
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    client = ScriptedLlmClient(results=[_generation(_model_output("no_matching_page"))])
    prep_err, exec_err = _run_match(
        connection,
        client=client,
        config=_main_config(),
        work_id=work_id,
        run_id=run_id,
    )
    assert prep_err is None
    assert exec_err is not None
    assert exec_err.category is FailureCategory.MALFORMED_RESPONSE
    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.disposition == "failed"
    assert _person_pointer(connection, person_id) is None


def test_empty_at_prepare_race_writes_deterministic_then_refuses(
    connection: sqlite3.Connection,
) -> None:
    """Empty candidates at prepare → deterministic path + ValueError refuse."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    # Ready plan with no hits / no pages → empty assembly.
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=NOW,
        )
        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=({"ordinal": 1, "variant_kind": "exact", "query_text": "Nobody"},),
        )
        mark_query_form_completed(
            conn,
            form_id=form_ids[0],
            continuations_used=0,
            hit_count=0,
            truncated=False,
        )
        mark_plan_status(conn, plan_id=plan_id, status="ready_for_match")
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    client = ScriptedLlmClient()
    prep_err, exec_err = _run_match(
        connection,
        client=client,
        config=_main_config(),
        work_id=work_id,
        run_id=run_id,
        expect_execute=False,
    )
    assert prep_err is not None
    assert "empty_candidates" in str(prep_err)
    assert exec_err is None
    assert client.generate_calls == []
    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.disposition == "completed"
    assert obs.semantic_outcome == "no_matching_page_found"
    assert obs.attempt_id is None


def test_persist_failure_noop_when_completed_exists(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id = _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    with immediate(connection):
        insert_wikipedia_identity_observation(
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
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_HASH,
            rationale="prior",
            failure_category=None,
            observed_at=NOW,
        )
        # Point via completed empty path shape: K4 no_match allows null attempt.
        obs = load_wikipedia_identity_observation_by_fingerprint(
            connection, person_id=person_id, task_fingerprint=_HASH
        )
        assert obs is not None
        point_person_current_wikipedia_observation(
            connection, person_id=person_id, observation_id=obs.id
        )
        mark_plan_status(
            connection, plan_id=plan_id, status="completed", completed_at=NOW
        )
    prior_pointer = _person_pointer(connection, person_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    connection.execute(
        """
        UPDATE work_item SET state = 'failed_permanent', reason = 'x', updated_at = ?
         WHERE id = ?
        """,
        (NOW, work_id),
    )
    connection.commit()
    handler = build_match_wikipedia_handler(
        connection, client=ScriptedLlmClient(), config=_main_config()
    )
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.MALFORMED_RESPONSE,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                retryable=False,
                detail="should no-op",
            ),
        )
    assert _person_pointer(connection, person_id) == prior_pointer
    count = connection.execute(
        """
        SELECT COUNT(*) AS n FROM wikipedia_identity_observation
         WHERE person_id = ? AND task_fingerprint = ?
        """,
        (person_id, _HASH),
    ).fetchone()["n"]
    assert count == 1
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"


def test_budget_reservation_under_hard_cap(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    _insert_compatible_inspection(
        connection, run_id=run_id, prompt_price=100, completion_price=200
    )
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    config = _main_config(hard_budget=True)
    handler = build_match_wikipedia_handler(
        connection, client=ScriptedLlmClient(), config=config
    )
    assert handler.prepare is not None
    prepared = handler.prepare(work)
    assert prepared.reserved_nano_usd is not None
    assert prepared.reserved_nano_usd > 0
    # MediaWiki handlers stay at zero reservation.
    from notable_person_finder.wikipedia.service import (
        build_mediawiki_search_handler,
    )

    class _Dummy:
        def search_pages(
            self, query: str, *, continuation: str | None
        ) -> MediaWikiSearchPage:
            raise AssertionError

        def get_page_facts(self, page_ids: object) -> MediaWikiPageFactsBatch:
            raise AssertionError

    search = build_mediawiki_search_handler(
        connection,
        client=_Dummy(),
        config=config,  # type: ignore[arg-type]
    )
    assert search.reserved_nano_usd == 0


# ---------------------------------------------------------------------------
# K21 inspection arming
# ---------------------------------------------------------------------------


def test_active_plan_arms_match_model_inspect(connection: sqlite3.Connection) -> None:
    """Cold-start K21: retrieving plan alone arms match-model inspection."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    with immediate(connection) as conn:
        open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=NOW,
        )
    config = _main_config()
    assert wikipedia_match_model_needed(connection, config=config) is True
    assert models_needed_for_run(connection, run_id, config) == (MATCH_MODEL,)
    ensured = ensure_model_inspections_for_run(
        connection, run_id=run_id, config=config, now=NOW
    )
    assert ensured == 1
    rows = connection.execute(
        "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ? AND state = 'pending'",
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()
    assert rows["n"] == 1


def test_mid_run_facts_settlement_schedules_match_and_match_model_inspect(
    connection: sqlite3.Connection,
) -> None:
    """K21b: search→facts→maybe_advance_plan arms match work + match inspect.

    Cold-start path opens only MediaWiki HTTP; after facts complete, both
    pending match_wikipedia_identity (priority 55) and pending inspect_model
    for the match model must exist without a separate seed-time ensure call.
    """
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"),),
        material_fingerprint=_HASH,
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((101, "Alex Smith"),))],
        facts_results=[MediaWikiPageFactsBatch(pages=(_page(101, "Alex Smith"),))],
    )
    # Before any HTTP: no match work, and seed did not open inspect.
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
        ).fetchone()["n"]
        == 0
    )
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (INSPECT_MODEL_TASK_TYPE,),
        ).fetchone()["n"]
        == 0
    )

    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    assert batches[0].status == "pending"
    # Facts still pending: match not yet scheduled.
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE task_type = ? AND state IN ('pending', 'deferred')
            """,
            (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
        ).fetchone()["n"]
        == 0
    )

    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )

    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"

    match_rows = connection.execute(
        """
        SELECT id, state, priority, required, subject_kind, subject_id
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
    ).fetchall()
    assert len(match_rows) == 1
    assert match_rows[0]["state"] == "pending"
    assert match_rows[0]["priority"] == MATCH_WIKIPEDIA_PRIORITY == 55
    assert match_rows[0]["required"] == 1
    assert match_rows[0]["subject_kind"] == "person"
    assert int(match_rows[0]["subject_id"]) == person_id

    expected_inspect_fp = _inspection_work_fingerprint(
        run_id=run_id,
        model_id=MATCH_MODEL,
        routing_fp=routing_fingerprint(config.openrouter.routing),
    )
    inspect_rows = connection.execute(
        """
        SELECT fingerprint, state, priority
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchall()
    assert len(inspect_rows) == 1
    assert inspect_rows[0]["state"] == "pending"
    assert inspect_rows[0]["fingerprint"] == expected_inspect_fp
    # Match model is the sole needed model on this Wikipedia-only path.
    assert models_needed_for_run(connection, run_id, config) == (MATCH_MODEL,)


def test_cold_start_after_facts_match_and_inspect_present(
    connection: sqlite3.Connection,
) -> None:
    """After ready_for_match + schedule match + ensure, match is inspection-ready."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)
    config = _main_config()
    ensure_model_inspections_for_run(connection, run_id=run_id, config=config, now=NOW)
    # Simulate successful inspect.
    _insert_compatible_inspection(connection, run_id=run_id)

    match_work = connection.execute(
        "SELECT state, priority FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert match_work["state"] == "pending"
    assert match_work["priority"] == MATCH_WIKIPEDIA_PRIORITY == 55
    assert inspection_ready(
        connection, run_id=run_id, config=config, model_id=MATCH_MODEL
    )
    handler = build_match_wikipedia_handler(
        connection, client=ScriptedLlmClient(), config=config
    )
    assert handler.ready is not None
    assert handler.ready(run_id) is True


def test_multi_model_inspect_when_only_wikipedia_backlog(
    connection: sqlite3.Connection,
) -> None:
    """Only Wikipedia active work ⇒ only match model in models_needed."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    with immediate(connection) as conn:
        open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            created_at=NOW,
        )
    config = _main_config()
    needed = models_needed_for_run(connection, run_id, config)
    assert needed == (MATCH_MODEL,)
    assert task_types_for_model(config, MATCH_MODEL) == (
        MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    )
    assert task_types_for_model(config, MATCH_MODEL) == (REPO_MATCH_TYPE,)


def test_eligible_person_without_plan_arms_inspect(
    connection: sqlite3.Connection,
) -> None:
    """Provisional K17 arm: bare eligible person forces match model inspect."""
    run_id = insert_run(connection)
    _person_with_name(connection, run_id=run_id)
    config = _main_config()
    assert wikipedia_match_model_needed(connection, config=config) is True
    assert models_needed_for_run(connection, run_id, config) == (MATCH_MODEL,)


# ---------------------------------------------------------------------------
# K18 permanent preflight
# ---------------------------------------------------------------------------


def test_permanent_match_preflight_failed_obs_plan_failed_pointer_unchanged(
    connection: sqlite3.Connection,
) -> None:
    """K18 + K25: inspection attempt_id, permanent_preflight, no pointer move."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id = _seed_ready_plan(connection, person_id=person_id, run_id=run_id)
    # Prior completed pointer on a different fingerprint (must stay).
    with immediate(connection):
        prior = insert_wikipedia_identity_observation(
            connection,
            person_id=person_id,
            plan_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="no_matching_page_found",
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_OTHER,
            rationale="prior completed",
            failure_category=None,
            observed_at=NOW,
        )
        point_person_current_wikipedia_observation(
            connection, person_id=person_id, observation_id=prior
        )

    work_id = _schedule_match(connection, person_id=person_id, run_id=run_id)

    inspect_work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES (
            'inspect_model', 'model', NULL, ?, 1, 20, ?, 'failed_permanent',
            ?, ?, ?
        )
        """,
        ("9" * 64, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert inspect_work is not None
    inspection_attempt = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint, failure_category
        ) VALUES (
            ?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'failed', ?,
            'authentication'
        )
        """,
        (run_id, inspect_work, NOW, moment(1), "9" * 64),
    ).lastrowid
    assert inspection_attempt is not None
    connection.commit()

    hashes = match_prompt_and_schema_hashes()
    settled = settle_active_tasks_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=inspection_attempt,
        model_inspection_id=None,
        model_id=MATCH_MODEL,
        failure_category="authentication",
        rationale="authentication",
        task_types=(MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
        now=moment(5),
    )
    assert settled == 1
    del hashes

    state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"]
    assert state == "failed_permanent"

    obs = load_wikipedia_identity_observation_by_fingerprint(
        connection, person_id=person_id, task_fingerprint=_HASH
    )
    assert obs is not None
    assert obs.disposition == "failed"
    assert obs.attempt_id == inspection_attempt
    assert obs.failure_category == "permanent_preflight"
    assert _person_pointer(connection, person_id) == prior  # K25 unchanged
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    # No generate_structured attempts invented.
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )


def test_task_types_for_model_maps_match_only() -> None:
    config = _main_config()
    assert task_types_for_model(config, MATCH_MODEL) == (
        MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    )
    assert task_types_for_model(config, "openai/gpt-other") == ()
