"""assess_article handler, K20 inspect arming, K23 preflight settler, K25 pointer."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from notable_person_finder.config.models import (
    AssessArticleConfig,
    BraveConfig,
    BudgetConfig,
    DomainProfileConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage.repository import (
    insert_article_view,
    insert_coverage_article_target,
    insert_person_article_assessment,
    insert_source_screening,
    load_person_article,
    load_person_article_assessment_by_fingerprint,
    load_plan,
    open_plan,
    point_person_article_current_assessment,
    update_plan_status,
    upsert_person_article,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import (
    ASSESS_ARTICLE_PRIORITY,
    ASSESS_ARTICLE_TASK_TYPE,
    assess_model_needed,
    build_assess_article_handler,
    schedule_assess_article,
    sweep_stalled_coverage_plans,
)
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.people.repository import (
    ASSESS_ARTICLE_TASK_TYPE as REPO_ASSESS_TYPE,
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
from notable_person_finder.providers.article_versions import EXTRACTOR_VERSION
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
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
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_POLICY = "b" * 64
_OTHER = "c" * 64
NOW = moment()
ASSESS_MODEL = "openai/gpt-assess"
MATCH_MODEL = "openai/gpt-match"
_REPO_ROOT = Path(__file__).resolve().parents[2]
_VISUAL_ARTS_POLICY = _REPO_ROOT / "config" / "source_policies" / "visual_arts.toml"


def _main_config(
    *,
    assess_model: str = ASSESS_MODEL,
    match_model: str = MATCH_MODEL,
    hard_budget: bool = False,
    source_policy_file: Path = _VISUAL_ARTS_POLICY,
) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=source_policy_file,
        budget=BudgetConfig(openrouter_usd_per_run="1.00" if hard_budget else None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        brave=BraveConfig(),
        tasks=TasksConfig(
            # Distinct match model so coverage-only arms do not collapse with
            # Wikipedia K17 arming on bare named people.
            match_wikipedia_identity=MatchWikipediaIdentityConfig(model=match_model),
            # No max_input_tokens override: the shipped default must be able
            # to assess a real article.
            assess_article=AssessArticleConfig(
                model=assess_model,
                max_completion_tokens=1024,
            ),
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
        raise AssertionError("assess handler must not call inspect_model")


def _generation(raw: str, *, nano: int = 100) -> StructuredGenerationResult:
    return StructuredGenerationResult(
        raw_text=raw,
        configured_model_id=ASSESS_MODEL,
        resolved_model_id=f"{ASSESS_MODEL}-resolved",
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


def _seed_assessable(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str = _HASH,
    title: str = "Alex Smith retrospective",
    body: str = "Alex Smith presents new bronzes in Paris.",
) -> tuple[int, int, int, int]:
    """Return (plan_id, person_article_id, article_view_id, canonical_article_id)."""
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            source_policy_fingerprint=_POLICY,
            retrieval_target=5,
            created_at=NOW,
        )
        article = conn.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES (?, 'example.com', ?)
            """,
            ("https://example.com/alex-smith", NOW),
        ).lastrowid
        assert article is not None
        screening_id = insert_source_screening(
            conn,
            canonical_article_id=int(article),
            url="https://example.com/alex-smith",
            publisher_key="example.com",
            rule_id="eligible.example.com",
            rule_status="curated_eligible",
            source_policy_fingerprint=_POLICY,
            decided_at=NOW,
            plan_id=plan_id,
        )
        del screening_id
        person_article_id = upsert_person_article(
            conn,
            person_id=person_id,
            canonical_article_id=int(article),
            first_plan_id=plan_id,
        )
        # Synthetic fetch attempt so article_view FK is satisfied.
        fetch_work = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'fetch_article', 'coverage_article_target', 1, ?, 1, 65, ?,
                'succeeded', ?, ?, ?
            )
            """,
            ("f" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert fetch_work is not None
        fetch_attempt = conn.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (
                ?, ?, 'article', 'fetch_article', 1, ?, ?, 'succeeded', ?
            )
            """,
            (run_id, fetch_work, NOW, moment(1), "f" * 64),
        ).lastrowid
        assert fetch_attempt is not None
        view_id = insert_article_view(
            conn,
            canonical_article_id=int(article),
            run_id=run_id,
            attempt_id=fetch_attempt,
            access_kind="full",
            requested_url="https://example.com/alex-smith",
            final_url="https://example.com/alex-smith",
            title=title,
            dek="A survey of sculpture.",
            byline="Staff",
            published_at="2026-01-01",
            editorial_labels_json='["Exhibition"]',
            main_text_blocks_json=json.dumps(
                [{"id": "b1", "text": body}], separators=(",", ":")
            ),
            snippets_json="[]",
            extraction_quality="full",
            extractor_version=EXTRACTOR_VERSION,
            observed_at=NOW,
        )
        insert_coverage_article_target(
            conn,
            plan_id=plan_id,
            canonical_article_id=int(article),
            request_url="https://example.com/alex-smith",
            selection_reason="search_curated_eligible",
            status="fetched",
        )
        conn.execute(
            """
            UPDATE coverage_article_target
               SET article_view_id = ?, attempt_id = ?
             WHERE plan_id = ? AND canonical_article_id = ?
            """,
            (view_id, fetch_attempt, plan_id, int(article)),
        )
        # Screening row linked for assess prepare (search path may be empty).
        insert_source_screening(
            conn,
            canonical_article_id=int(article),
            url="https://example.com/alex-smith",
            publisher_key="example.com",
            rule_id="eligible.example.com",
            rule_status="curated_eligible",
            source_policy_fingerprint=_POLICY,
            decided_at=NOW,
            plan_id=plan_id,
        )
    return plan_id, person_article_id, view_id, int(article)


def _insert_compatible_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    model_id: str = ASSESS_MODEL,
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
        config = _main_config(assess_model=model_id)
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


def _schedule_assess(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    article_view_id: int,
    run_id: int,
    material_fingerprint: str = _HASH,
    config: MainConfig | None = None,
) -> int:
    with immediate(connection) as conn:
        return schedule_assess_article(
            conn,
            person_article_id=person_article_id,
            article_view_id=article_view_id,
            material_fingerprint=material_fingerprint,
            run_id=run_id,
            now=NOW,
            config=config or _main_config(),
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


def _valid_output(*, passage_ids: list[str] | None = None) -> str:
    ids = passage_ids if passage_ids is not None else ["p1"]
    return json.dumps(
        {
            "person_relation": "same_person",
            "person_relation_passage_ids": ids,
            "person_relation_rationale": "Name and role match the supplied person.",
            "coverage_depth": "significant",
            "coverage_depth_passage_ids": ids,
            "coverage_depth_rationale": "Extended treatment of the career.",
            "content_types": ["profile", "review"],
            "content_types_passage_ids": ids,
            "content_types_rationale": "Exhibition profile with critical review.",
            "subject_relationship": "editorially_independent",
            "subject_relationship_passage_ids": ids,
            "subject_relationship_rationale": "Third-party critical coverage.",
            "signals": [
                {
                    "kind": "attention",
                    "category": "significant_recognition",
                    "claim": "Major retrospective survey.",
                    "supporting_passage_ids": ids,
                }
            ],
        }
    )


def _run_assess(
    connection: sqlite3.Connection,
    *,
    client: ScriptedLlmClient,
    config: MainConfig,
    work_id: int,
    run_id: int,
    expect_execute: bool = True,
) -> tuple[object | None, object | None]:
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_assess_article_handler(
        connection, client=client, config=config, profile=_profile()
    )
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


def _person_article_pointer(
    connection: sqlite3.Connection, person_article_id: int
) -> int | None:
    row = connection.execute(
        "SELECT current_assessment_id FROM person_article WHERE id = ?",
        (person_article_id,),
    ).fetchone()
    assert row is not None
    value = row["current_assessment_id"]
    return None if value is None else int(value)


# ---------------------------------------------------------------------------
# Handler contract
# ---------------------------------------------------------------------------


def test_assess_handler_pool_priority_provider_operation(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    handler = build_assess_article_handler(
        connection, client=ScriptedLlmClient(), config=config, profile=_profile()
    )
    assert handler.task_type == ASSESS_ARTICLE_TASK_TYPE
    assert handler.provider == PROVIDER
    assert handler.operation == GENERATE_OPERATION
    assert handler.pool is WorkerPool.LLM
    assert handler.reserved_nano_usd == 0
    assert handler.ready is not None
    assert ASSESS_ARTICLE_PRIORITY == 70


def test_assess_reservation_uses_the_rendered_request_not_the_ceiling(
    connection: sqlite3.Connection,
) -> None:
    """Kills reserving `assess_article.max_input_tokens` at this call site.

    Four other task types share the same reservation helper, so a per-site
    test is the only thing that catches one site reverting on its own.
    """
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    _insert_compatible_inspection(
        connection, run_id=run_id, prompt_price=150, completion_price=600
    )
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    config = _main_config(hard_budget=True)
    handler = build_assess_article_handler(
        connection, client=ScriptedLlmClient(), config=config, profile=_profile()
    )
    assert handler.prepare is not None
    prepared = handler.prepare(work)

    assert prepared.reserved_nano_usd is not None
    assert prepared.reserved_nano_usd > 0
    assess_config = config.tasks.assess_article
    ceiling_reservation = (
        150 * assess_config.max_input_tokens + 600 * assess_config.max_completion_tokens
    )
    assert prepared.reserved_nano_usd < ceiling_reservation


def test_happy_assess_path_sets_pointer_and_signals(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    client = ScriptedLlmClient(results=[_generation(_valid_output())])
    config = _main_config()

    prep_err, exec_err = _run_assess(
        connection, client=client, config=config, work_id=work_id, run_id=run_id
    )
    assert prep_err is None
    assert exec_err is None
    assert len(client.generate_calls) == 1
    assert client.generate_calls[0].schema_name == "assess_article"

    work = _work_item(connection, work_id)
    assessment = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=work.fingerprint,
    )
    assert assessment is not None
    assert assessment.disposition == "completed"
    assert assessment.person_relation == "same_person"
    assert assessment.coverage_depth == "significant"
    assert assessment.validated_output_json is not None
    assert _person_article_pointer(connection, person_article_id) == assessment.id

    signals = connection.execute(
        """
        SELECT signal_kind, category, claim
          FROM article_assessment_signal
         WHERE assessment_id = ?
         ORDER BY ordinal
        """,
        (assessment.id,),
    ).fetchall()
    assert len(signals) == 1
    assert signals[0]["signal_kind"] == "attention"
    assert signals[0]["category"] == "significant_recognition"

    # Last assess path terminal → plan closes (T10: no usable search; any assess).
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert plan.completed_at is not None
    assert plan.partial_retrieval is True


def test_bad_passage_id_malformed_then_permanent_failed_no_pointer(
    connection: sqlite3.Connection,
) -> None:
    """Invalid passage refs → malformed retry, then permanent failed assessment."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    bad = _valid_output(passage_ids=["p99"])
    client = ScriptedLlmClient(results=[_generation(bad)])
    config = _main_config()

    prep_err, exec_err = _run_assess(
        connection, client=client, config=config, work_id=work_id, run_id=run_id
    )
    assert prep_err is None
    assert exec_err is not None
    assert isinstance(exec_err, ProviderFailure)
    assert exec_err.category is FailureCategory.MALFORMED_RESPONSE
    assert exec_err.retryable is True

    work = _work_item(connection, work_id)
    assessment = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=work.fingerprint,
    )
    assert assessment is not None
    assert assessment.disposition == "failed"
    assert assessment.failure_category == "invalid_model_output"
    assert assessment.person_relation is None
    assert _person_article_pointer(connection, person_article_id) is None

    # Permanent invalid-output is assess-terminal → plan advances (T10).
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert plan.completed_at is not None
    assert plan.partial_retrieval is True


def test_completed_only_sets_pointer_failed_does_not(
    connection: sqlite3.Connection,
) -> None:
    """K25: current person–article pointer only for completed assessments."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _plan_id, person_article_id, view_id, article_id = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    # Prior failed assessment must not set the pointer.
    with immediate(connection):
        prior_work = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'assess_article', 'person_article', ?, ?, 1, 70, ?,
                'failed_permanent', ?, ?, ?
            )
            """,
            (person_article_id, _OTHER, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert prior_work is not None
        prior_attempt = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint, failure_category
            ) VALUES (
                ?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'failed', ?,
                'timeout'
            )
            """,
            (run_id, prior_work, NOW, moment(1), _OTHER),
        ).lastrowid
        assert prior_attempt is not None
        insert_person_article_assessment(
            connection,
            person_article_id=person_article_id,
            person_id=person_id,
            canonical_article_id=article_id,
            plan_id=None,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=prior_attempt,
            model_inspection_id=None,
            disposition="failed",
            person_relation=None,
            coverage_depth=None,
            content_types_json=None,
            subject_relationship=None,
            screening_rule_id="eligible.example.com",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=_POLICY,
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=_OTHER,
            rationale="prior failed",
            failure_category="permanent_provider",
            observed_at=NOW,
        )
    assert _person_article_pointer(connection, person_article_id) is None

    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    client = ScriptedLlmClient(results=[_generation(_valid_output())])
    prep_err, exec_err = _run_assess(
        connection,
        client=client,
        config=_main_config(),
        work_id=work_id,
        run_id=run_id,
    )
    assert prep_err is None and exec_err is None
    work = _work_item(connection, work_id)
    assessment = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=work.fingerprint,
    )
    assert assessment is not None
    assert _person_article_pointer(connection, person_article_id) == assessment.id


# ---------------------------------------------------------------------------
# K20 inspection arming
# ---------------------------------------------------------------------------


def test_active_plan_arms_assess_model_inspect(connection: sqlite3.Connection) -> None:
    """Cold-start K20: retrieving plan alone arms assess-model inspection."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    with immediate(connection) as conn:
        open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            source_policy_fingerprint=_POLICY,
            retrieval_target=5,
            created_at=NOW,
        )
    config = _main_config()
    assert assess_model_needed(connection, config=config) is True
    needed = models_needed_for_run(connection, run_id, config)
    assert ASSESS_MODEL in needed
    ensured = ensure_model_inspections_for_run(
        connection, run_id=run_id, config=config, now=NOW
    )
    assert ensured >= 1
    assess_fp = _inspection_work_fingerprint(
        run_id=run_id,
        model_id=ASSESS_MODEL,
        routing_fp=routing_fingerprint(config.openrouter.routing),
    )
    rows = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending' AND fingerprint = ?
        """,
        (INSPECT_MODEL_TASK_TYPE, assess_fp),
    ).fetchone()
    assert rows["n"] == 1


def test_schedule_assess_mid_run_ensures_inspect(
    connection: sqlite3.Connection,
) -> None:
    """K20b: scheduling assess_article arms inspect_model for the assess model."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    config = _main_config()
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (INSPECT_MODEL_TASK_TYPE,),
        ).fetchone()["n"]
        == 0
    )
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
        config=config,
    )
    assert work_id > 0
    expected_fp = _inspection_work_fingerprint(
        run_id=run_id,
        model_id=ASSESS_MODEL,
        routing_fp=routing_fingerprint(config.openrouter.routing),
    )
    inspect_rows = connection.execute(
        """
        SELECT fingerprint, state, priority
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred')
           AND fingerprint = ?
         ORDER BY id
        """,
        (INSPECT_MODEL_TASK_TYPE, expected_fp),
    ).fetchall()
    assert len(inspect_rows) == 1
    assert inspect_rows[0]["fingerprint"] == expected_fp
    assert ASSESS_MODEL in models_needed_for_run(connection, run_id, config)


def test_multi_model_inspect_when_only_coverage_backlog(
    connection: sqlite3.Connection,
) -> None:
    """Active coverage plan arms assess model (may co-arm Wikipedia K17)."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    with immediate(connection) as conn:
        open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=_HASH,
            source_policy_fingerprint=_POLICY,
            retrieval_target=5,
            created_at=NOW,
        )
    config = _main_config()
    needed = models_needed_for_run(connection, run_id, config)
    assert ASSESS_MODEL in needed
    assert task_types_for_model(config, ASSESS_MODEL) == (ASSESS_ARTICLE_TASK_TYPE,)
    assert task_types_for_model(config, ASSESS_MODEL) == (REPO_ASSESS_TYPE,)


def test_assess_ready_after_compatible_inspection(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    _plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    config = _main_config()
    ensure_model_inspections_for_run(connection, run_id=run_id, config=config, now=NOW)
    _insert_compatible_inspection(connection, run_id=run_id)

    assert inspection_ready(
        connection, run_id=run_id, config=config, model_id=ASSESS_MODEL
    )
    handler = build_assess_article_handler(
        connection, client=ScriptedLlmClient(), config=config, profile=_profile()
    )
    assert handler.ready is not None
    assert handler.ready(run_id) is True
    work = connection.execute(
        "SELECT state, priority FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert work["state"] == "pending"
    assert work["priority"] == ASSESS_ARTICLE_PRIORITY == 70


# ---------------------------------------------------------------------------
# K23 permanent preflight
# ---------------------------------------------------------------------------


def test_permanent_assess_preflight_failed_assessment_pointer_unchanged(
    connection: sqlite3.Connection,
) -> None:
    """K23 + K25: inspection attempt_id, permanent_preflight, no pointer move."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, person_article_id, view_id, article_id = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    # Prior completed pointer must stay.
    with immediate(connection):
        # Need a separate completed-compatible attempt for the prior row.
        prior_work = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'assess_article', 'person_article', ?, ?, 1, 70, ?,
                'succeeded', ?, ?, ?
            )
            """,
            (person_article_id, _OTHER, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert prior_work is not None
        prior_attempt = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (
                ?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?
            )
            """,
            (run_id, prior_work, NOW, moment(1), _OTHER),
        ).lastrowid
        assert prior_attempt is not None
        inspect_work = connection.execute(
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
            ("2" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert inspect_work is not None
        inspect_attempt = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (
                ?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'succeeded', ?
            )
            """,
            (run_id, inspect_work, NOW, moment(1), "2" * 64),
        ).lastrowid
        assert inspect_attempt is not None
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=inspect_attempt,
            configured_model_id=ASSESS_MODEL,
            resolved_model_id=f"{ASSESS_MODEL}-resolved",
            routing_fingerprint=routing_fingerprint(_main_config().openrouter.routing),
            supported_parameters_json='["response_format"]',
            supports_strict_structured_output=True,
            pricing_usable=True,
            prompt_unit_price_nano_usd=150,
            completion_unit_price_nano_usd=600,
            compatibility="compatible",
            inspected_at=NOW,
        )
        prior = insert_person_article_assessment(
            connection,
            person_article_id=person_article_id,
            person_id=person_id,
            canonical_article_id=article_id,
            plan_id=plan_id,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=prior_attempt,
            model_inspection_id=inspection_id,
            disposition="completed",
            person_relation="same_person",
            coverage_depth="passing",
            content_types_json='["profile"]',
            subject_relationship="uncertain",
            screening_rule_id="eligible.example.com",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=_POLICY,
            canonical_supplied_input_json="{}",
            validated_output_json='{"person_relation":"same_person"}',
            prompt_hash="d" * 64,
            schema_hash="e" * 64,
            schema_version=1,
            task_fingerprint=_OTHER,
            rationale="prior completed",
            failure_category=None,
            observed_at=NOW,
        )
        point_person_article_current_assessment(
            connection,
            person_article_id=person_article_id,
            assessment_id=prior,
        )

    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    # Simulate permanent inspect failure for the assess model.
    fail_work = connection.execute(
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
    assert fail_work is not None
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
        (run_id, fail_work, NOW, moment(1), "9" * 64),
    ).lastrowid
    assert inspection_attempt is not None
    connection.commit()

    settled = settle_active_tasks_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=inspection_attempt,
        model_inspection_id=None,
        model_id=ASSESS_MODEL,
        failure_category="authentication",
        rationale="authentication",
        task_types=(ASSESS_ARTICLE_TASK_TYPE,),
        now=moment(5),
    )
    assert settled == 1

    state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"]
    assert state == "failed_permanent"

    work = _work_item(connection, work_id)
    assessment = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=work.fingerprint,
    )
    assert assessment is not None
    assert assessment.disposition == "failed"
    assert assessment.attempt_id == inspection_attempt
    assert assessment.failure_category == "permanent_preflight"
    assert _person_article_pointer(connection, person_article_id) == prior  # K25
    # No generate_structured attempts invented by the settler.
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM attempt
             WHERE operation = ? AND work_item_id = ?
            """,
            (GENERATE_OPERATION, work_id),
        ).fetchone()["n"]
        == 0
    )
    # person_article still loads.
    assert load_person_article(connection, person_article_id=person_article_id)
    # K23: settler advances plan when selected assess paths are terminal (T10).
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert plan.completed_at is not None
    assert plan.partial_retrieval is True


def test_task_types_for_model_maps_assess_only() -> None:
    config = _main_config()
    assert task_types_for_model(config, ASSESS_MODEL) == (ASSESS_ARTICLE_TASK_TYPE,)
    assert task_types_for_model(config, "openai/gpt-other") == ()


def _oversized_profile() -> DomainProfileConfig:
    """An operator profile far larger than the shipped one.

    `AssessArticleConfig` bounds passages, title, and dek, but nothing bounds
    the domain profile, so this is the overflow route that survives the
    configuration validator and must be handled at render time.
    """
    return DomainProfileConfig(
        schema_version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples={
            "significant_recognition": tuple(
                "major art prize awarded by a national institution " * 20
                for _ in range(60)
            )
        },
    )


def test_assess_input_overflow_records_local_refuse_and_advances_plan(
    connection: sqlite3.Connection,
) -> None:
    """C1(c): an input that cannot fit must not wedge the plan.

    A raising `prepare` settles the item `failed_permanent` with no failure, so
    `persist_failure` never runs. Without a domain row the target stays
    `fetched`, the plan stays `assessing`, and the person is excluded from
    coverage eligibility forever.
    """
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    _insert_compatible_inspection(connection, run_id=run_id)
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    work = _work_item(connection, work_id)
    _claim_and_attempt(connection, work_id=work_id, run_id=run_id)
    client = ScriptedLlmClient()
    handler = build_assess_article_handler(
        connection,
        client=client,
        config=_main_config(),
        profile=_oversized_profile(),
    )
    assert handler.prepare is not None

    with pytest.raises(ValueError) as error:
        handler.prepare(work)
    assert "input_too_large" in str(error.value)
    assert client.generate_calls == []

    assessment = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=work.fingerprint,
    )
    assert assessment is not None
    assert assessment.disposition == "failed"
    assert assessment.failure_category == "local_refuse"
    assert assessment.attempt_id is None
    # K25: a failed assessment never becomes the current pointer.
    assert _person_article_pointer(connection, person_article_id) is None

    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status in {"completed", "incomplete", "failed"}
    assert plan.completed_at is not None

    aggregate_work = connection.execute(
        """
        SELECT subject_kind, subject_id
          FROM work_item
         WHERE task_type = 'aggregate_person_lead'
        """
    ).fetchall()
    assert len(aggregate_work) == 1
    assert aggregate_work[0]["subject_kind"] == "person"
    assert aggregate_work[0]["subject_id"] == person_id


def test_sweep_terminalizes_a_plan_whose_assess_work_died_without_a_row(
    connection: sqlite3.Connection,
) -> None:
    """C2: a `fetched` path whose assess work is permanently dead is terminal.

    `prepare` exits such as `unresolved_context` and `missing_view` raise
    before any domain write, so the target keeps `fetched` and no assessment
    row exists. Without treating the dead work item as terminal the plan can
    never leave `assessing`.
    """
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    plan_id, person_article_id, view_id, _article = _seed_assessable(
        connection, person_id=person_id, run_id=run_id
    )
    work_id = _schedule_assess(
        connection,
        person_article_id=person_article_id,
        article_view_id=view_id,
        run_id=run_id,
    )
    with immediate(connection) as conn:
        update_plan_status(conn, plan_id=plan_id, status="assessing")
        conn.execute(
            """
            UPDATE work_item
               SET state = 'failed_permanent', reason = 'prepare raised ValueError',
                   completed_by_run_id = ?, updated_at = ?
             WHERE id = ?
            """,
            (run_id, NOW, work_id),
        )
    assert (
        load_person_article_assessment_by_fingerprint(
            connection,
            person_article_id=person_article_id,
            task_fingerprint=_work_item(connection, work_id).fingerprint,
        )
        is None
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "assessing"

    swept = sweep_stalled_coverage_plans(
        connection,
        run_id=run_id,
        config=_main_config(),
        policy=source_policy_from_mapping(
            {
                "schema_version": 1,
                "key": "test-sources",
                "label": "Test policy",
                "rules": [],
            }
        ),
        now=moment(2),
    )
    assert swept == 1
    recovered = load_person_article_assessment_by_fingerprint(
        connection,
        person_article_id=person_article_id,
        task_fingerprint=_work_item(connection, work_id).fingerprint,
    )
    assert recovered is not None
    assert recovered.disposition == "failed"
    assert recovered.failure_category == "local_refuse"
    assert recovered.attempt_id is None
    assert _person_article_pointer(connection, person_article_id) is None
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status in {"completed", "incomplete", "failed"}
    assert plan.completed_at is not None
