"""Detection scheduling, handler prepare/execute/persist, and engine seams."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from notable_person_finder.config.models import (
    BudgetConfig,
    DetectPeopleConfig,
    DomainProfileConfig,
    FeedConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    RetryConfig,
    TasksConfig,
)
from notable_person_finder.people.detection import (
    DetectionValidationError,
    build_detection_input,
    render_detection_request,
)
from notable_person_finder.people.models import (
    AttentionCategory,
    CautionCategory,
    DetectedMention,
    DetectionOutput,
    GroundedSignal,
    IdentityFact,
    IdentityFactKind,
    ItemOutcome,
    MentionOutcome,
    SignalGrounding,
    SignalKind,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    load_current_triage_observation,
    load_person_mentions,
    load_source_item_record,
    load_triage_observation_by_fingerprint,
)
from notable_person_finder.people.service import (
    DETECT_PEOPLE_PRIORITY,
    INSPECT_MODEL_TASK_TYPE,
    MALFORMED_DETECTION_DETAIL,
    _DetectionCall,
    _execute_detection_for,
    build_detection_handler,
    build_inspection_handler,
    ensure_model_inspection,
    inspection_ready,
    schedule_source_items,
    seed_untriaged,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    INSPECT_OPERATION,
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
from tests.ingestion.helpers import insert_run, moment

MODEL = "openai/gpt-test"
NOW = moment()
FIXTURES = Path(__file__).with_name("fixtures")


def _main_config(
    *,
    model: str = MODEL,
    hard_budget: bool = False,
    budget_usd: str | None = None,
    max_input_tokens: int = 4428,
    max_completion_tokens: int = 512,
    max_people: int = 3,
) -> MainConfig:
    openrouter_usd = budget_usd
    if openrouter_usd is None and hard_budget:
        openrouter_usd = "1.00"
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=openrouter_usd),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(
                model=model,
                max_input_tokens=max_input_tokens,
                max_completion_tokens=max_completion_tokens,
                max_people=max_people,
            )
        ),
    )


def _profile() -> DomainProfileConfig:
    return DomainProfileConfig(
        schema_version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples={
            "significant_recognition": ("major art prize",),
            "institutional_recognition": ("permanent museum collection",),
        },
    )


def _seed_source_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    source_entry_id: str = "entry-a",
    title_text: str | None = "Élodie N'Diaye wins the Prix Exemple",
    summary_text: str | None = "The sculptor was honoured in Paris.",
    key: str = "feed-a",
) -> int:
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Arts News', 'https://example.com/feed', ?, ?)
        """,
        (key, moment(), moment()),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, moment()),
    ).lastrowid
    assert fetch is not None
    item = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, original_url,
            published_at, published_issue, url_issue, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'https://example.com/a', ?, NULL, NULL, ?)
        """,
        (
            feed,
            fetch,
            run_id,
            source_entry_id,
            title_text,
            summary_text,
            moment(),
            moment(),
        ),
    ).lastrowid
    assert item is not None
    connection.commit()
    return item


@dataclass
class ScriptedLlmClient:
    """Inspection + generation client with scripted generate responses."""

    inspection: ModelInspectionResult | None = None
    inspect_error: ProviderFailure | None = None
    generate_results: list[StructuredGenerationResult | ProviderFailure] = field(
        default_factory=list
    )
    inspect_calls: list[ModelInspectionRequest] = field(default_factory=list)
    generate_calls: list[StructuredGenerationRequest] = field(default_factory=list)
    _generate_index: int = 0

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        self.inspect_calls.append(request)
        if self.inspect_error is not None:
            raise self.inspect_error
        if self.inspection is None:
            raise RuntimeError("ScriptedLlmClient.inspection is not configured")
        return self.inspection

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        self.generate_calls.append(request)
        if self._generate_index >= len(self.generate_results):
            raise RuntimeError("ScriptedLlmClient has no further generate results")
        result = self.generate_results[self._generate_index]
        self._generate_index += 1
        if isinstance(result, ProviderFailure):
            raise result
        return result


def _compatible_inspection(
    *,
    model_id: str = MODEL,
    prompt_price: int | None = 150,
    completion_price: int | None = 600,
) -> ModelInspectionResult:
    return ModelInspectionResult(
        configured_model_id=model_id,
        resolved_model_id=f"{model_id}-resolved",
        supported_parameters=("response_format", "structured_outputs"),
        supports_strict_structured_output=True,
        prompt_unit_price_nano_usd=prompt_price,
        completion_unit_price_nano_usd=completion_price,
        latency_ms=12,
    )


def _generation_result(
    content: str,
    *,
    cost: float | None = 0.000012,
    request_id: str = "gen-test-1",
) -> StructuredGenerationResult:
    return StructuredGenerationResult(
        raw_text=content,
        configured_model_id=MODEL,
        resolved_model_id=f"{MODEL}-resolved",
        serving_provider="OpenAI",
        finish_reason="stop",
        refusal=None,
        usage=TokenUsage(prompt_tokens=100, completion_tokens=40, total_tokens=140),
        latency_ms=20,
        provider_request_id=request_id,
        actual_nano_usd=None if cost is None else int(round(cost * 1_000_000_000)),
    )


def _zero_mentions_output() -> DetectionOutput:
    return DetectionOutput(
        item_outcome=ItemOutcome.DO_NOT_RESEARCH,
        mentions=(),
        overflow=False,
        rationale="No person is named in the supplied passages.",
    )


def _research_mention(name: str = "Élodie N'Diaye") -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        outcome=MentionOutcome.RESEARCH,
        supporting_passage_ids=("p1",),
        identity_facts=(
            IdentityFact(
                local_id="fact-name",
                kind=IdentityFactKind.NAME,
                value=name,
                supporting_passage_ids=("p1",),
            ),
        ),
        signals=(
            GroundedSignal(
                kind=SignalKind.ATTENTION,
                category=AttentionCategory.SIGNIFICANT_RECOGNITION,
                claim="The title reports a prize.",
                supporting_passage_ids=("p1",),
                grounding=SignalGrounding.DOMAIN_PROFILE,
            ),
        ),
        rationale=f"{name} is the clear subject.",
    )


def _multiple_mentions_output() -> DetectionOutput:
    second = DetectedMention(
        exact_name="sculptor",
        outcome=MentionOutcome.UNCERTAIN,
        supporting_passage_ids=("p2",),
        identity_facts=(),
        signals=(
            GroundedSignal(
                kind=SignalKind.CAUTION,
                category=CautionCategory.SIGNIFICANCE_UNCLEAR,
                claim="The summary uses a professional reference only.",
                supporting_passage_ids=("p2",),
                grounding=SignalGrounding.SOURCE_TEXT,
            ),
        ),
        rationale="Professional mononym with uncertain identity.",
    )
    return DetectionOutput(
        item_outcome=ItemOutcome.RESEARCH_PEOPLE,
        mentions=(_research_mention(), second),
        overflow=False,
        rationale="One clear subject and one uncertain reference.",
    )


def _uncertain_only_output() -> DetectionOutput:
    mention = DetectedMention(
        exact_name="sculptor",
        outcome=MentionOutcome.UNCERTAIN,
        supporting_passage_ids=("p2",),
        identity_facts=(),
        signals=(
            GroundedSignal(
                kind=SignalKind.CAUTION,
                category=CautionCategory.SIGNIFICANCE_UNCLEAR,
                claim="Only a professional label appears.",
                supporting_passage_ids=("p2",),
                grounding=SignalGrounding.SOURCE_TEXT,
            ),
        ),
        rationale="Uncertain identity.",
    )
    return DetectionOutput(
        item_outcome=ItemOutcome.UNCERTAIN,
        mentions=(mention,),
        overflow=False,
        rationale="Only uncertain references appear.",
    )


def _run_engine(
    connection: sqlite3.Connection,
    handlers: dict,
    *,
    seed: Callable[[int], None] | None = None,
    hard_budget_limit: int | None = None,
    max_attempts: int = 2,
    snapshot_fingerprint: str = "a" * 64,
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
            budget_limit_nano_usd=hard_budget_limit,
            snapshot_fingerprint=snapshot_fingerprint,
            snapshot_json="{}",
            reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
        )
        report = engine.execute(handlers, seed=seed)
    return report.run_id


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
        DETECT_PEOPLE_TASK_TYPE: build_detection_handler(
            connection, client=client, config=config, profile=profile
        ),
    }


def _people_seed(
    connection: sqlite3.Connection,
    config: MainConfig,
    profile: DomainProfileConfig,
    *,
    source_item_ids: list[int] | None = None,
    backfill: bool = False,
) -> Callable[[int], None]:
    def seed(run_id: int) -> None:
        now = moment()
        if backfill:
            seed_untriaged(
                connection, run_id=run_id, config=config, profile=profile, now=now
            )
        elif source_item_ids is not None:
            schedule_source_items(
                connection,
                source_item_ids=source_item_ids,
                run_id=run_id,
                config=config,
                profile=profile,
                now=now,
            )
        else:
            ensure_model_inspection(connection, run_id=run_id, config=config, now=now)

    return seed


def _detect_work_rows(
    connection: sqlite3.Connection, *, source_item_id: int | None = None
) -> list[sqlite3.Row]:
    if source_item_id is None:
        return connection.execute(
            """
            SELECT * FROM work_item WHERE task_type = ? ORDER BY id
            """,
            (DETECT_PEOPLE_TASK_TYPE,),
        ).fetchall()
    return connection.execute(
        """
        SELECT * FROM work_item
         WHERE task_type = ? AND subject_id = ?
         ORDER BY id
        """,
        (DETECT_PEOPLE_TASK_TYPE, source_item_id),
    ).fetchall()


# ---------------------------------------------------------------------------
# Scheduling
# ---------------------------------------------------------------------------


def test_seed_untriaged_backfills_usable_items(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    first = _seed_source_item(connection, run_id=run_id, source_entry_id="e1")
    second = _seed_source_item(
        connection, run_id=run_id, source_entry_id="e2", key="feed-b"
    )
    config = _main_config()
    profile = _profile()

    seed_untriaged(connection, run_id=run_id, config=config, profile=profile, now=NOW)

    rows = _detect_work_rows(connection)
    assert len(rows) == 2
    assert {row["subject_id"] for row in rows} == {first, second}
    assert all(row["state"] == "pending" for row in rows)
    assert all(row["priority"] == DETECT_PEOPLE_PRIORITY for row in rows)
    assert all(row["required"] == 1 for row in rows)
    inspect_count = connection.execute(
        "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["n"]
    assert inspect_count == 1


def test_schedule_source_items_for_newly_named_ids(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    item = _seed_source_item(connection, run_id=run_id)
    config = _main_config()
    profile = _profile()

    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )

    rows = _detect_work_rows(connection, source_item_id=item)
    assert len(rows) == 1
    assert rows[0]["state"] == "pending"
    assert rows[0]["subject_kind"] == "source_item"
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (INSPECT_MODEL_TASK_TYPE,),
        ).fetchone()["n"]
        == 1
    )


def test_schedule_detection_is_idempotent(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    item = _seed_source_item(connection, run_id=run_id)
    config = _main_config()
    profile = _profile()

    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    first = _detect_work_rows(connection, source_item_id=item)
    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    second = _detect_work_rows(connection, source_item_id=item)
    assert len(first) == 1
    assert len(second) == 1
    assert first[0]["id"] == second[0]["id"]
    assert first[0]["fingerprint"] == second[0]["fingerprint"]


def test_changed_material_fingerprint_schedules_replacement(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    item = _seed_source_item(connection, run_id=run_id)
    config = _main_config()
    profile = _profile()

    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    original = _detect_work_rows(connection, source_item_id=item)
    assert len(original) == 1
    original_fp = original[0]["fingerprint"]

    connection.execute(
        "UPDATE source_item SET title_text = ? WHERE id = ?",
        ("Renamed subject wins another prize", item),
    )
    connection.commit()

    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    rows = _detect_work_rows(connection, source_item_id=item)
    assert len(rows) == 2
    by_state = {row["state"]: row for row in rows}
    assert by_state["superseded"]["fingerprint"] == original_fp
    assert by_state["pending"]["fingerprint"] != original_fp


def test_reuse_supersedes_stale_active_fingerprint_after_material_flip_flop(
    connection: sqlite3.Connection,
) -> None:
    """Reuse of material A must retire pending work for material B.

    Sequence: complete A → schedule B (pending) → restore A text and re-schedule.
    Without supersede on the reuse branch, B stays claimable under a fingerprint
    that no longer matches the current source text prepare would send.
    """
    bootstrap = insert_run(connection)
    title_a = "Élodie N'Diaye wins the Prix Exemple"
    summary = "The sculptor was honoured in Paris."
    item = _seed_source_item(
        connection,
        run_id=bootstrap,
        title_text=title_a,
        summary_text=summary,
    )
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        snapshot_fingerprint="flip-a" + "0" * 58,
    )
    observation_a = load_current_triage_observation(connection, source_item_id=item)
    assert observation_a is not None
    assert observation_a.disposition == "completed"
    fingerprint_a = observation_a.task_fingerprint

    connection.execute(
        "UPDATE source_item SET title_text = ? WHERE id = ?",
        ("Completely different subject receives an award", item),
    )
    connection.commit()
    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=bootstrap,
        config=config,
        profile=profile,
        now=moment(20),
    )
    pending_b = [
        row
        for row in _detect_work_rows(connection, source_item_id=item)
        if row["state"] == "pending"
    ]
    assert len(pending_b) == 1
    fingerprint_b = pending_b[0]["fingerprint"]
    assert fingerprint_b != fingerprint_a

    connection.execute(
        "UPDATE source_item SET title_text = ? WHERE id = ?",
        (title_a, item),
    )
    connection.commit()
    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=bootstrap,
        config=config,
        profile=profile,
        now=moment(30),
    )

    rows = _detect_work_rows(connection, source_item_id=item)
    b_rows = [row for row in rows if row["fingerprint"] == fingerprint_b]
    assert len(b_rows) == 1
    assert b_rows[0]["state"] == "superseded"
    active = [row for row in rows if row["state"] in ("pending", "deferred", "running")]
    assert active == []
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.id == observation_a.id
    assert current.task_fingerprint == fingerprint_a


def test_schedule_time_empty_input_writes_insufficient_without_work(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    item = _seed_source_item(
        connection,
        run_id=run_id,
        title_text=None,
        summary_text="   ",
    )
    config = _main_config()
    profile = _profile()

    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )

    assert _detect_work_rows(connection, source_item_id=item) == []
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "insufficient_input"
    assert current.attempt_id is None
    assert current.failure_category is None
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
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


def test_schedule_ensures_inspection_for_usable_items(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    item = _seed_source_item(connection, run_id=run_id)
    config = _main_config()
    profile = _profile()
    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    inspect = connection.execute(
        """
        SELECT task_type, state, priority FROM work_item WHERE task_type = ?
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()
    assert inspect is not None
    assert inspect["state"] == "pending"
    assert inspect["priority"] == 20


# ---------------------------------------------------------------------------
# Handler / engine
# ---------------------------------------------------------------------------


def test_detection_prepare_uses_exact_source_context(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    valid = _multiple_mentions_output().model_dump_json()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(valid)],
    )
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )

    assert len(client.generate_calls) == 1
    request = client.generate_calls[0]
    assert request.model_id == MODEL
    assert request.schema_name == "detect_people"
    record = load_source_item_record(connection, source_item_id=item)
    assert record is not None
    expected = render_detection_request(
        build_detection_input(
            record,
            FeedConfig(
                key=record.feed_key,
                label=record.feed_label,
                url="https://example.com/feed",
            ),
            profile,
            config.tasks.detect_people,
        )
    )
    assert request.system_prompt == expected.system_prompt
    assert request.user_content == expected.user_input_json
    assert request.json_schema == expected.schema
    assert (
        request.max_completion_tokens
        == config.tasks.detect_people.max_completion_tokens
    )
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "completed"
    assert current.run_id == run_id


def test_reservation_tracks_the_rendered_request_not_the_configured_ceiling(
    connection: sqlite3.Connection,
) -> None:
    """Kills reserving the configured `max_input_tokens` ceiling.

    Two items with materially different summary lengths render to materially
    different request sizes under one configuration. Their reservations must
    differ in the same direction. Reserving the ceiling makes both identical,
    which is the ~31x over-reservation measured on the first ten-feed run.
    """
    bootstrap = insert_run(connection)
    config = _main_config(
        hard_budget=True,
        max_input_tokens=65_536,
        max_completion_tokens=512,
    )
    profile = _profile()
    prompt_price = 150
    completion_price = 600

    reservations: list[int] = []
    token_counts: list[int] = []
    for entry, summary in (
        ("entry-small", "Short."),
        ("entry-large", "A far longer summary. " * 200),
    ):
        item = _seed_source_item(
            connection,
            run_id=bootstrap,
            source_entry_id=entry,
            summary_text=summary,
            key=f"feed-{entry}",
        )
        token_counts.append(
            _rendered_input_tokens(
                connection, source_item_id=item, config=config, profile=profile
            )
        )
        client = ScriptedLlmClient(
            inspection=_compatible_inspection(
                prompt_price=prompt_price, completion_price=completion_price
            ),
            generate_results=[
                _generation_result(_zero_mentions_output().model_dump_json())
            ],
        )
        run_id = _run_engine(
            connection,
            _handlers(connection, client, config, profile),
            seed=_people_seed(connection, config, profile, source_item_ids=[item]),
            hard_budget_limit=10_000_000_000,
        )
        row = connection.execute(
            """
            SELECT reserved_nano_usd FROM attempt
             WHERE run_id = ? AND operation = ?
            """,
            (run_id, GENERATE_OPERATION),
        ).fetchone()
        assert row is not None
        reservations.append(row["reserved_nano_usd"])

    # The fixture must actually discriminate, or the assertion below is vacuous.
    assert token_counts[0] < token_counts[1]
    assert reservations[0] < reservations[1]
    ceiling_reservation = prompt_price * 65_536 + completion_price * 512
    assert reservations[1] < ceiling_reservation


def test_reservation_never_exceeds_the_configured_ceiling_reservation(
    connection: sqlite3.Connection,
) -> None:
    """Pins K5: the change can only lower a reservation, never raise one.

    Every render refuses an input larger than `max_input_tokens`, so the
    rendered size is bounded by the ceiling by construction. That is what
    guarantees no run passing under a cap today starts failing on budget.
    """
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config(
        hard_budget=True,
        max_input_tokens=65_536,
        max_completion_tokens=512,
    )
    profile = _profile()
    input_tokens = _rendered_input_tokens(
        connection, source_item_id=item, config=config, profile=profile
    )
    assert input_tokens <= config.tasks.detect_people.max_input_tokens


def _rendered_input_tokens(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    config: MainConfig,
    profile: DomainProfileConfig,
) -> int:
    """This item's own rendered request size, as the handler computes it.

    Derived, never hardcoded: a constant here would keep passing if a call
    site reverted to reserving the configured ``max_input_tokens`` ceiling.
    """
    record = load_source_item_record(connection, source_item_id=source_item_id)
    assert record is not None
    rendered = render_detection_request(
        build_detection_input(
            record,
            FeedConfig(
                key=record.feed_key,
                label=record.feed_label,
                url="https://example.com/feed",
            ),
            profile,
            config.tasks.detect_people,
        )
    )
    return rendered.worst_case_input_tokens


def test_dynamic_reservation_under_hard_budget(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config(
        hard_budget=True,
        max_input_tokens=4428,
        max_completion_tokens=512,
    )
    profile = _profile()
    prompt_price = 150
    completion_price = 600
    input_tokens = _rendered_input_tokens(
        connection, source_item_id=item, config=config, profile=profile
    )
    # The rendered request must be strictly smaller than the ceiling, or this
    # test cannot tell the two apart.
    assert input_tokens < 4428
    expected_reservation = prompt_price * input_tokens + completion_price * 512
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(
            prompt_price=prompt_price, completion_price=completion_price
        ),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        hard_budget_limit=10_000_000_000,
    )
    gen = connection.execute(
        """
        SELECT reserved_nano_usd, operation FROM attempt
         WHERE run_id = ? AND operation = ?
        """,
        (run_id, GENERATE_OPERATION),
    ).fetchone()
    assert gen is not None
    assert gen["reserved_nano_usd"] == expected_reservation
    inspect = connection.execute(
        """
        SELECT reserved_nano_usd FROM attempt
         WHERE run_id = ? AND operation = ?
        """,
        (run_id, INSPECT_OPERATION),
    ).fetchone()
    assert inspect["reserved_nano_usd"] == 0


def test_no_hard_budget_reserves_zero(connection: sqlite3.Connection) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config(hard_budget=False)
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )
    gen = connection.execute(
        """
        SELECT reserved_nano_usd FROM attempt
         WHERE run_id = ? AND operation = ?
        """,
        (run_id, GENERATE_OPERATION),
    ).fetchone()
    assert gen["reserved_nano_usd"] == 0


def test_valid_zero_mentions_result(connection: sqlite3.Connection) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "completed"
    assert current.semantic_outcome == "do_not_research"
    assert load_person_mentions(connection, triage_observation_id=current.id) == ()


def test_valid_multiple_and_uncertain_results(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    multi = _seed_source_item(
        connection, run_id=bootstrap, source_entry_id="multi", key="feed-multi"
    )
    uncertain = _seed_source_item(
        connection,
        run_id=bootstrap,
        source_entry_id="unc",
        key="feed-unc",
        title_text="A sculptor spoke",
        summary_text="The sculptor was honoured in Paris.",
    )
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_multiple_mentions_output().model_dump_json()),
            _generation_result(_uncertain_only_output().model_dump_json()),
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(
            connection, config, profile, source_item_ids=[multi, uncertain]
        ),
    )
    multi_obs = load_current_triage_observation(connection, source_item_id=multi)
    assert multi_obs is not None
    assert multi_obs.semantic_outcome == "research_people"
    multi_mentions = load_person_mentions(
        connection, triage_observation_id=multi_obs.id
    )
    assert len(multi_mentions) == 2
    assert multi_mentions[0].exact_name == "Élodie N'Diaye"
    assert multi_mentions[1].outcome == "uncertain"

    unc_obs = load_current_triage_observation(connection, source_item_id=uncertain)
    assert unc_obs is not None
    assert unc_obs.semantic_outcome == "uncertain"
    assert unc_obs.disposition == "completed"


def test_malformed_validation_failure_detail_excludes_raw_output() -> None:
    """ProviderFailure.detail must be the safe token, not the model body.

    Engine failed attempts store detail_json=None, so attempt-row checks cannot
    kill a mutation that sets detail=raw_text. Assert the failure channel
    directly on the worker execute callback.
    """
    config = _main_config()
    profile = _profile()
    record_fields = {
        "id": 1,
        "feed_identity_id": 1,
        "feed_key": "arts-news",
        "feed_label": "Arts News",
        "title_text": "Élodie N'Diaye wins the Prix Exemple",
        "summary_text": "The sculptor was honoured in Paris.",
        "canonical_article_id": None,
        "original_url": "https://example.com/a",
        "published_at": moment(),
        "published_issue": None,
        "url_issue": None,
        "current_triage_observation_id": None,
    }
    detection_input = build_detection_input(
        record_fields,
        FeedConfig(
            key="arts-news",
            label="Arts News",
            url="https://example.com/feed",
        ),
        profile,
        config.tasks.detect_people,
    )
    rendered = render_detection_request(detection_input)
    sentinel = '{"raw":"SECRET_MODEL_BODY_SHOULD_NOT_LEAK"}'
    client = ScriptedLlmClient(
        generate_results=[_generation_result(sentinel)],
    )
    prepared = _DetectionCall(
        request=StructuredGenerationRequest(
            model_id=MODEL,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name="detect_people",
            max_completion_tokens=config.tasks.detect_people.max_completion_tokens,
            temperature=0.0,
            top_p=1.0,
            reasoning_effort=None,
        ),
        detection_input=detection_input,
        source_item_id=1,
        model_inspection_id=1,
        canonical_supplied_input_json=rendered.canonical_input_json,
        prompt_hash=rendered.prompt_hash,
        schema_hash=rendered.schema_hash,
        schema_version=rendered.schema_version,
        task_fingerprint="f" * 64,
        input_truncated=False,
    )
    work_item = WorkItem(
        id=1,
        task_type=DETECT_PEOPLE_TASK_TYPE,
        subject_kind="source_item",
        subject_id=1,
        fingerprint="f" * 64,
        required=True,
        priority=DETECT_PEOPLE_PRIORITY,
        state=WorkState.RUNNING,
    )
    execute = _execute_detection_for(client)
    with pytest.raises(ProviderFailure) as raised:
        execute(work_item, 1, prepared)
    failure = raised.value
    assert failure.category is FailureCategory.MALFORMED_RESPONSE
    assert failure.retryable is True
    # The reason is appended (A0), but the payload must never be.
    assert (failure.detail or "").startswith(MALFORMED_DETECTION_DETAIL)
    assert sentinel not in (failure.detail or "")
    assert "SECRET_MODEL_BODY" not in (failure.detail or "")
    assert "SECRET_MODEL_BODY" not in str(failure)


def test_one_malformed_retry_then_success(connection: sqlite3.Connection) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result("{not-json"),
            _generation_result(_zero_mentions_output().model_dump_json()),
        ],
    )
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=3,
    )
    attempts = connection.execute(
        """
        SELECT ordinal, outcome, failure_category, detail_json
          FROM attempt
         WHERE run_id = ? AND operation = ?
         ORDER BY ordinal
        """,
        (run_id, GENERATE_OPERATION),
    ).fetchall()
    assert len(attempts) == 2
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["failure_category"] == "malformed_response"
    # Failure detail must never retain raw model output (engine may leave
    # detail_json null for failed attempts; either way the body is absent).
    assert "{not-json" not in (attempts[0]["detail_json"] or "")
    assert attempts[1]["outcome"] == "succeeded"
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "completed"


def test_second_malformed_is_permanent(connection: sqlite3.Connection) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result("{bad"),
            _generation_result('{"item_outcome":"nope"}'),
        ],
    )
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=3,
    )
    attempts = connection.execute(
        """
        SELECT ordinal, outcome, failure_category
          FROM attempt
         WHERE run_id = ? AND operation = ?
         ORDER BY ordinal
        """,
        (run_id, GENERATE_OPERATION),
    ).fetchall()
    assert len(attempts) == 2
    assert all(row["failure_category"] == "malformed_response" for row in attempts)
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "failed_permanent"
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "failed"
    assert current.failure_category == "malformed_response"
    assert current.validated_output_json is None


def test_unseen_references_are_malformed_not_persisted(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    invalid = (FIXTURES / "detect_people_invalid_references.json").read_text(
        encoding="utf-8"
    )
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(invalid),
            _generation_result(invalid),
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=3,
    )
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "failed"
    assert current.validated_output_json is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM person_mention").fetchone()["n"]
        == 0
    )


def test_budget_refusal_defers_without_generation(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    # Tiny hard budget: inspection free (0), generation reservation huge.
    config = _main_config(
        hard_budget=True,
        budget_usd="0.000001",
        max_input_tokens=4428,
        max_completion_tokens=1024,
    )
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(prompt_price=150, completion_price=600),
        generate_results=[],
    )
    hard_limit = config.budget.openrouter_nano_usd_per_run()
    assert hard_limit is not None
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        hard_budget_limit=hard_limit,
    )
    assert client.generate_calls == []
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "deferred"
    assert work["reason"] == "not_evaluated_budget"
    assert load_current_triage_observation(connection, source_item_id=item) is None
    assert inspection_ready(connection, run_id=run_id, config=config) is True


def test_detection_not_claimed_until_inspection_ready(
    connection: sqlite3.Connection,
) -> None:
    """Detection ready must gate claims while inspection is still pending."""
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspect_error=ProviderFailure(
            FailureCategory.TIMEOUT,
            provider=PROVIDER,
            operation=INSPECT_OPERATION,
            detail="timeout",
        ),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=1,
    )
    assert client.generate_calls == []
    detect = _detect_work_rows(connection, source_item_id=item)[0]
    assert detect["state"] == "pending"
    assert detect["claimed_by_run_id"] is None
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )


def test_worker_closure_contains_no_connection(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(inspection=_compatible_inspection())
    handler = build_detection_handler(
        connection, client=client, config=config, profile=profile
    )
    freevars = handler.execute.__code__.co_freevars
    assert "connection" not in freevars
    for cell in handler.execute.__closure__ or ():
        assert not isinstance(cell.cell_contents, sqlite3.Connection)


def test_detection_persist_rolls_back_when_resolution_schedule_fails(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Forced schedule_resolution failure rolls back triage observation too."""
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()

    def boom(*_args: object, **_kwargs: object) -> int:
        raise RuntimeError("forced resolution schedule failure")

    monkeypatch.setattr(
        "notable_person_finder.people.service.schedule_resolution_for_observation",
        boom,
    )
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_multiple_mentions_output().model_dump_json())
        ],
    )
    # Engine catches persist failures, rolls back domain writes, and
    # re-settles the work item failed_permanent without re-running persist.
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )
    assert load_current_triage_observation(connection, source_item_id=item) is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM triage_observation").fetchone()[
            "n"
        ]
        == 0
    )
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM person_mention").fetchone()["n"]
        == 0
    )
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE task_type = 'resolve_person_entity'
            """
        ).fetchone()["n"]
        == 0
    )
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "failed_permanent"


def test_atomic_persistence_with_settlement(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_multiple_mentions_output().model_dump_json())
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "succeeded"
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "completed"
    mentions = load_person_mentions(connection, triage_observation_id=current.id)
    assert len(mentions) == 2
    # Observation and work settlement share material fingerprint identity.
    assert current.task_fingerprint == work["fingerprint"]


def test_interruption_leaves_no_partial_observation(
    connection: sqlite3.Connection,
) -> None:
    """A generation that raises mid-call leaves no triage observation."""
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()

    class Boom(Exception):
        pass

    client = ScriptedLlmClient(inspection=_compatible_inspection())

    def boom_generate(
        request: StructuredGenerationRequest,
    ) -> StructuredGenerationResult:
        client.generate_calls.append(request)
        raise Boom("worker crashed after network")

    client.generate_structured = boom_generate  # type: ignore[method-assign]

    with pytest.raises(Boom):
        _run_engine(
            connection,
            _handlers(connection, client, config, profile),
            seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        )

    assert load_current_triage_observation(connection, source_item_id=item) is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM triage_observation").fetchone()[
            "n"
        ]
        == 0
    )
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "running"


def test_reuse_after_successful_persistence(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        snapshot_fingerprint="r" * 64,
    )
    first = load_current_triage_observation(connection, source_item_id=item)
    assert first is not None
    first_id = first.id
    fingerprint = first.task_fingerprint
    generate_count_after_first = len(client.generate_calls)

    # Re-schedule the same material: must reuse observation, not mint work.
    schedule_source_items(
        connection,
        source_item_ids=[item],
        run_id=bootstrap,
        config=config,
        profile=profile,
        now=moment(10),
    )
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ?
           AND state IN ('pending', 'deferred', 'running')
        """,
        (DETECT_PEOPLE_TASK_TYPE, item),
    ).fetchone()["n"]
    assert pending == 0
    reused = load_triage_observation_by_fingerprint(
        connection, source_item_id=item, task_fingerprint=fingerprint
    )
    assert reused is not None
    assert reused.id == first_id
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.id == first_id
    assert len(client.generate_calls) == generate_count_after_first


def test_transient_generation_exhaustion_writes_no_observation(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            ProviderFailure(
                FailureCategory.TIMEOUT,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail="timeout",
            ),
            ProviderFailure(
                FailureCategory.TIMEOUT,
                provider=PROVIDER,
                operation=GENERATE_OPERATION,
                detail="timeout",
            ),
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=2,
    )
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "deferred"
    assert load_current_triage_observation(connection, source_item_id=item) is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM triage_observation").fetchone()[
            "n"
        ]
        == 0
    )


def test_seed_untriaged_skips_already_triaged(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    empty = _seed_source_item(
        connection,
        run_id=run_id,
        source_entry_id="empty",
        title_text="",
        summary_text=None,
        key="feed-empty",
    )
    usable = _seed_source_item(
        connection, run_id=run_id, source_entry_id="usable", key="feed-usable"
    )
    config = _main_config()
    profile = _profile()
    seed_untriaged(connection, run_id=run_id, config=config, profile=profile, now=NOW)
    assert load_current_triage_observation(connection, source_item_id=empty) is not None
    assert _detect_work_rows(connection, source_item_id=empty) == []
    assert len(_detect_work_rows(connection, source_item_id=usable)) == 1

    seed_untriaged(connection, run_id=run_id, config=config, profile=profile, now=NOW)
    assert len(_detect_work_rows(connection, source_item_id=usable)) == 1


def test_validation_detail_names_the_rule_that_rejected_the_response() -> None:
    """Kills discarding the domain-validation reason (A0).

    Before this, `detail_json` was empty and the log carried only
    `failure_category`, so diagnosing why a response was rejected cost a live
    replay of every failing item.
    """
    from notable_person_finder.providers.failures import validation_detail

    detail = validation_detail(
        MALFORMED_DETECTION_DETAIL,
        DetectionValidationError("mention[1] f2: value is not grounded"),
    )
    assert detail == ("invalid detection output: mention[1] f2: value is not grounded")


def test_validation_detail_carries_no_supplied_or_model_prose() -> None:
    """Positive control for the test above.

    The reason may be persisted only because every variable part is either
    application-supplied or a pydantic-bounded `local_id`. If a message ever
    carried article text or a secret, persisting it would be a leak.
    """
    from notable_person_finder.providers.failures import validation_detail

    secret = "SECRET_MODEL_BODY_SHOULD_NOT_LEAK"
    detail = validation_detail(
        MALFORMED_DETECTION_DETAIL,
        DetectionValidationError("mention[1] f2: value is not grounded"),
    )
    assert secret not in detail
    assert "sculpture" not in detail
    # An empty reason must not leave a dangling separator.
    assert validation_detail(MALFORMED_DETECTION_DETAIL, ValueError("")) == (
        MALFORMED_DETECTION_DETAIL
    )
