"""Model inspection seeding, handler settlement, and detection readiness."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from notable_person_finder.config.models import (
    BudgetConfig,
    DetectPeopleConfig,
    DomainProfileConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    MediaWikiConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    ResolvePersonEntityConfig,
    RetryConfig,
    TasksConfig,
)
from notable_person_finder.people.identity import insert_person
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    insert_entity_resolution_observation,
    insert_failed_observation,
    load_current_triage_observation,
    load_er_by_mention_fingerprint,
    load_er_by_relation_fingerprint,
    load_model_inspection,
    match_key,
    settle_active_tasks_after_permanent_preflight,
    upsert_active_possible_same_person,
)
from notable_person_finder.people.resolution import (
    first_pass_task_fingerprint,
    resolution_prompt_and_schema_hashes,
)
from notable_person_finder.people.service import (
    INSPECT_MODEL_PRIORITY,
    INSPECT_MODEL_TASK_TYPE,
    _inspection_work_fingerprint,
    build_inspection_handler,
    ensure_model_inspection,
    ensure_model_inspections_for_run,
    inspection_ready,
    is_resolution_eligible_mention,
    models_needed_for_run,
    routing_fingerprint,
    task_types_for_model,
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
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
    TaskPreparation,
)
from notable_person_finder.runs.models import WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import (
    BoundedScheduler,
    SchedulerSet,
    WorkerPool,
)
from tests.ingestion.helpers import immediate, insert_run, moment

MODEL = "openai/gpt-test"
RESOLVE_MODEL = "openai/gpt-resolve"
NOW = moment()
_PROMPT_HASH = "p" * 64
_SCHEMA_HASH = "s" * 64
_TASK_FINGERPRINT = "t" * 64
_HASH = "a" * 64
_OTHER_HASH = "b" * 64


def _main_config(
    *,
    model: str = MODEL,
    resolve_model: str | None = None,
    hard_budget: bool = False,
    routing: ProviderRoutingConfig | None = None,
) -> MainConfig:
    resolve = (
        ResolvePersonEntityConfig(model=resolve_model)
        if resolve_model is not None
        else ResolvePersonEntityConfig(model=model)
    )
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        budget=BudgetConfig(openrouter_usd_per_run="1.00" if hard_budget else None),
        openrouter=OpenRouterConfig(
            routing=routing if routing is not None else ProviderRoutingConfig()
        ),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(model=model),
            resolve_person_entity=resolve,
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
        },
    )


def _seed_source_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    source_entry_id: str = "entry-a",
    title_text: str | None = "Alex Smith wins award",
    summary_text: str | None = "A prize ceremony.",
    key: str = "feed-a",
) -> int:
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed A', 'https://example.com/feed', ?, ?)
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


def _schedule_detect(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    source_item_id: int,
    fingerprint: str = "d" * 64,
) -> int:
    return repository.schedule_work(
        connection,
        task_type=DETECT_PEOPLE_TASK_TYPE,
        subject_kind="source_item",
        subject_id=source_item_id,
        fingerprint=fingerprint,
        required=True,
        priority=30,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )


@dataclass
class FakeLlmClient:
    """Minimal LlmClient for inspection handler tests."""

    inspection: ModelInspectionResult | None = None
    error: ProviderFailure | None = None
    inspect_calls: list[ModelInspectionRequest] = field(default_factory=list)
    generate_calls: list[object] = field(default_factory=list)

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        self.inspect_calls.append(request)
        if self.error is not None:
            raise self.error
        if self.inspection is None:
            raise RuntimeError("FakeLlmClient.inspection is not configured")
        return self.inspection

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        self.generate_calls.append(request)
        raise AssertionError("generation must not run before inspection readiness")


def _compatible_inspection(
    *,
    model_id: str = MODEL,
    prompt_price: int | None = 150,
    completion_price: int | None = 600,
    supports_strict: bool = True,
) -> ModelInspectionResult:
    return ModelInspectionResult(
        configured_model_id=model_id,
        resolved_model_id=f"{model_id}-resolved",
        supported_parameters=(
            ("response_format", "structured_outputs")
            if supports_strict
            else ("temperature",)
        ),
        supports_strict_structured_output=supports_strict,
        prompt_unit_price_nano_usd=prompt_price,
        completion_unit_price_nano_usd=completion_price,
        latency_ms=12,
    )


def _run_engine(
    connection: sqlite3.Connection,
    handlers: dict[str, TaskHandler],
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


def _inspection_seed(
    connection: sqlite3.Connection,
    config: MainConfig,
    *,
    also_detect: list[int] | None = None,
) -> Callable[[int], None]:
    def seed(run_id: int) -> None:
        now = moment()
        ensure_model_inspection(connection, run_id=run_id, config=config, now=now)
        for source_item_id in also_detect or ():
            _schedule_detect(
                connection,
                run_id=run_id,
                source_item_id=source_item_id,
            )

    return seed


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------


def test_one_inspection_per_needed_model_and_run(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _seed_source_item(connection, run_id=run_id)
    config = _main_config()

    first = ensure_model_inspection(connection, run_id=run_id, config=config, now=NOW)
    second = ensure_model_inspection(connection, run_id=run_id, config=config, now=NOW)
    assert first == 1
    assert second == 1

    rows = connection.execute(
        """
        SELECT task_type, state, priority, required, subject_kind
          FROM work_item WHERE task_type = ?
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["task_type"] == INSPECT_MODEL_TASK_TYPE
    assert rows[0]["state"] == "pending"
    assert rows[0]["priority"] == INSPECT_MODEL_PRIORITY
    assert rows[0]["required"] == 1
    assert rows[0]["subject_kind"] == "model"


def test_no_inspection_when_no_usable_untriaged_item(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _seed_source_item(
        connection,
        run_id=run_id,
        title_text=None,
        summary_text=None,
    )
    config = _main_config()
    assert (
        ensure_model_inspection(connection, run_id=run_id, config=config, now=NOW) == 0
    )
    count = connection.execute(
        "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["n"]
    assert count == 0


def test_no_inspection_when_only_triaged_usable_items_exist(
    connection: sqlite3.Connection,
) -> None:
    """Usable text alone must not seed inspection once the item is triaged.

    Kills dropping ``current_triage_observation_id IS NULL`` from the seed
    predicate: a fully triaged item with non-empty title/summary would then
    still schedule inspect_model work.
    """
    run_id = insert_run(connection)
    item_id = _seed_source_item(connection, run_id=run_id)
    work_item_id = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('inspect_model', 'model', NULL, ?, 1, 20, ?, 'succeeded', ?, ?, ?)
        """,
        ("e" * 64, moment(), run_id, moment(), moment()),
    ).lastrowid
    assert work_item_id is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_item_id, moment(), moment(1), "f" * 64),
    ).lastrowid
    assert attempt_id is not None
    connection.commit()
    with immediate(connection):
        insert_failed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=None,
            failure_category="authentication",
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_TASK_FINGERPRINT,
            input_truncated=False,
            observed_at=moment(),
            rationale="Already triaged; no further inspection seed.",
        )

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    config = _main_config()
    assert (
        ensure_model_inspection(connection, run_id=run_id, config=config, now=NOW) == 0
    )
    # Only the historical succeeded inspect work may exist; no new pending row.
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state IN ('pending', 'deferred', 'running')
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["n"]
    assert pending == 0


def test_no_cross_run_freshness_reuse(connection: sqlite3.Connection) -> None:
    first_run = insert_run(connection)
    _seed_source_item(connection, run_id=first_run)
    config = _main_config()
    ensure_model_inspection(connection, run_id=first_run, config=config, now=NOW)
    first_fp = connection.execute(
        "SELECT fingerprint FROM work_item WHERE task_type = ?",
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["fingerprint"]

    second_run = insert_run(connection)
    _seed_source_item(
        connection, run_id=second_run, source_entry_id="entry-b", key="feed-b"
    )
    ensure_model_inspection(connection, run_id=second_run, config=config, now=NOW)

    rows = connection.execute(
        """
        SELECT fingerprint, state, created_by_run_id
          FROM work_item
         WHERE task_type = ?
         ORDER BY id
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["fingerprint"] == first_fp
    assert rows[0]["state"] == "superseded"
    assert rows[0]["created_by_run_id"] == first_run
    assert rows[1]["fingerprint"] != first_fp
    assert rows[1]["state"] == "pending"
    assert rows[1]["created_by_run_id"] == second_run


# ---------------------------------------------------------------------------
# Readiness and handler settlement
# ---------------------------------------------------------------------------


def test_successful_inspection_unlocks_readiness(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    client = FakeLlmClient(inspection=_compatible_inspection())
    handler = build_inspection_handler(connection, client=client, config=config)

    run_id = _run_engine(
        connection,
        {INSPECT_MODEL_TASK_TYPE: handler},
        seed=_inspection_seed(connection, config),
    )

    assert len(client.inspect_calls) == 1
    assert client.inspect_calls[0].model_id == MODEL
    assert inspection_ready(connection, run_id=run_id, config=config) is True
    routing_fp = routing_fingerprint(config.openrouter.routing)
    loaded = load_model_inspection(
        connection,
        run_id=run_id,
        configured_model_id=MODEL,
        routing_fingerprint=routing_fp,
    )
    assert loaded is not None
    assert loaded.compatibility == "compatible"
    assert loaded.supports_strict_structured_output is True
    assert loaded.pricing_usable is True
    assert loaded.resolved_model_id == f"{MODEL}-resolved"

    attempts = connection.execute(
        """
        SELECT operation, outcome, reserved_nano_usd FROM attempt WHERE run_id = ?
        """,
        (run_id,),
    ).fetchall()
    assert len(attempts) == 1
    assert attempts[0]["operation"] == INSPECT_OPERATION
    assert attempts[0]["outcome"] == "succeeded"
    assert attempts[0]["reserved_nano_usd"] == 0


def test_transient_deferral_leaves_detection_active_and_unclaimable(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item_id = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    client = FakeLlmClient(
        error=ProviderFailure(
            FailureCategory.TIMEOUT,
            provider=PROVIDER,
            operation=INSPECT_OPERATION,
            detail="timeout",
        )
    )
    inspection_handler = build_inspection_handler(
        connection, client=client, config=config
    )
    generation_calls: list[int] = []

    def detect_execute(
        work_item: object, ordinal: int, prepared: object
    ) -> TaskOutcome:
        generation_calls.append(1)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=detect_execute,
        pool=WorkerPool.LLM,
        ready=lambda claimed_run_id: inspection_ready(
            connection, run_id=claimed_run_id, config=config
        ),
        reserved_nano_usd=0,
    )

    run_id = _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: inspection_handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=_inspection_seed(connection, config, also_detect=[item_id]),
    )

    assert generation_calls == []
    assert client.generate_calls == []
    assert inspection_ready(connection, run_id=run_id, config=config) is False
    inspect_state = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND created_by_run_id = ?
        """,
        (INSPECT_MODEL_TASK_TYPE, run_id),
    ).fetchone()["state"]
    assert inspect_state == "deferred"
    detect_state = connection.execute(
        """
        SELECT state, claimed_by_run_id FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, item_id),
    ).fetchone()
    assert detect_state["state"] == "pending"
    assert detect_state["claimed_by_run_id"] is None
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )
    assert (
        load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id=MODEL,
            routing_fingerprint=routing_fingerprint(config.openrouter.routing),
        )
        is None
    )


def test_permanent_propagation_settles_detection_without_generation(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item_id = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    client = FakeLlmClient(
        error=ProviderFailure(
            FailureCategory.AUTHENTICATION,
            provider=PROVIDER,
            operation=INSPECT_OPERATION,
            detail="unauthorized",
        )
    )
    inspection_handler = build_inspection_handler(
        connection, client=client, config=config
    )
    generation_calls: list[int] = []

    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=lambda work, ordinal, prepared: (
            generation_calls.append(1)
            or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        ),
        pool=WorkerPool.LLM,
        ready=lambda claimed_run_id: inspection_ready(
            connection, run_id=claimed_run_id, config=config
        ),
    )

    run_id = _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: inspection_handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=_inspection_seed(connection, config, also_detect=[item_id]),
    )

    assert generation_calls == []
    detect_row = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, item_id),
    ).fetchone()
    assert detect_row["state"] == "failed_permanent"
    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.disposition == "failed"
    assert current.failure_category == "authentication"
    assert current.model_inspection_id is None
    assert current.run_id == run_id
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )
    assert inspection_ready(connection, run_id=run_id, config=config) is False


def test_pricing_required_only_under_hard_budget(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    soft_item = _seed_source_item(
        connection, run_id=bootstrap, source_entry_id="soft", key="feed-soft"
    )
    soft_config = _main_config(hard_budget=False)
    soft_client = FakeLlmClient(
        inspection=_compatible_inspection(prompt_price=None, completion_price=None)
    )
    soft_handler = build_inspection_handler(
        connection, client=soft_client, config=soft_config
    )
    soft_run = _run_engine(
        connection,
        {INSPECT_MODEL_TASK_TYPE: soft_handler},
        seed=_inspection_seed(connection, soft_config),
        snapshot_fingerprint="s" * 64,
    )
    assert inspection_ready(connection, run_id=soft_run, config=soft_config) is True
    soft_loaded = load_model_inspection(
        connection,
        run_id=soft_run,
        configured_model_id=MODEL,
        routing_fingerprint=routing_fingerprint(soft_config.openrouter.routing),
    )
    assert soft_loaded is not None
    assert soft_loaded.compatibility == "compatible"
    assert soft_loaded.pricing_usable is False

    hard_item = _seed_source_item(
        connection, run_id=bootstrap, source_entry_id="hard", key="feed-hard"
    )
    # Soft item already has no triage; mark it triaged so hard run still needs
    # inspection via hard_item only. Actually both are untriaged usable — fine.
    del soft_item
    hard_config = _main_config(hard_budget=True)
    hard_client = FakeLlmClient(
        inspection=_compatible_inspection(prompt_price=None, completion_price=None)
    )
    hard_handler = build_inspection_handler(
        connection, client=hard_client, config=hard_config
    )
    generation_calls: list[int] = []
    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=lambda work, ordinal, prepared: (
            generation_calls.append(1)
            or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        ),
        pool=WorkerPool.LLM,
        ready=lambda claimed_run_id: inspection_ready(
            connection, run_id=claimed_run_id, config=hard_config
        ),
    )
    hard_run = _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: hard_handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=_inspection_seed(connection, hard_config, also_detect=[hard_item]),
        hard_budget_limit=1_000_000_000,
        snapshot_fingerprint="h" * 64,
    )

    assert generation_calls == []
    assert inspection_ready(connection, run_id=hard_run, config=hard_config) is False
    hard_loaded = load_model_inspection(
        connection,
        run_id=hard_run,
        configured_model_id=MODEL,
        routing_fingerprint=routing_fingerprint(hard_config.openrouter.routing),
    )
    assert hard_loaded is not None
    assert hard_loaded.compatibility == "incompatible"
    assert hard_loaded.pricing_usable is False
    detect_state = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, hard_item),
    ).fetchone()["state"]
    assert detect_state == "failed_permanent"


def test_unsupported_strict_output_is_permanent_preflight(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    item_id = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    client = FakeLlmClient(inspection=_compatible_inspection(supports_strict=False))
    handler = build_inspection_handler(connection, client=client, config=config)
    generation_calls: list[int] = []
    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=lambda work, ordinal, prepared: (
            generation_calls.append(1)
            or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        ),
        pool=WorkerPool.LLM,
        ready=lambda claimed_run_id: inspection_ready(
            connection, run_id=claimed_run_id, config=config
        ),
    )
    run_id = _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=_inspection_seed(connection, config, also_detect=[item_id]),
    )

    assert generation_calls == []
    assert inspection_ready(connection, run_id=run_id, config=config) is False
    loaded = load_model_inspection(
        connection,
        run_id=run_id,
        configured_model_id=MODEL,
        routing_fingerprint=routing_fingerprint(config.openrouter.routing),
    )
    assert loaded is not None
    assert loaded.compatibility == "incompatible"
    assert loaded.supports_strict_structured_output is False
    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.disposition == "failed"
    assert current.failure_category == "unsupported_capability"
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 0
    )


def test_zero_generation_attempts_before_readiness(
    connection: sqlite3.Connection,
) -> None:
    """Positive control: generation runs only after readiness becomes true."""
    bootstrap = insert_run(connection)
    item_id = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    client = FakeLlmClient(inspection=_compatible_inspection())
    inspection_handler = build_inspection_handler(
        connection, client=client, config=config
    )
    generation_calls: list[int] = []
    readiness_seen: list[bool] = []

    def ready(claimed_run_id: int) -> bool:
        is_ready = inspection_ready(connection, run_id=claimed_run_id, config=config)
        readiness_seen.append(is_ready)
        return is_ready

    def detect_execute(
        work_item: object, ordinal: int, prepared: object
    ) -> TaskOutcome:
        generation_calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    def detect_prepare(work_item: object) -> TaskPreparation:
        return TaskPreparation(payload=None, reserved_nano_usd=0)

    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=detect_execute,
        prepare=detect_prepare,
        pool=WorkerPool.LLM,
        ready=ready,
        reserved_nano_usd=0,
    )

    _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: inspection_handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=_inspection_seed(connection, config, also_detect=[item_id]),
    )

    assert False in readiness_seen
    assert True in readiness_seen
    assert generation_calls == [1]
    assert client.generate_calls == []
    gen_attempts = connection.execute(
        "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
        (GENERATE_OPERATION,),
    ).fetchone()["n"]
    assert gen_attempts == 1
    inspect_attempts = connection.execute(
        "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
        (INSPECT_OPERATION,),
    ).fetchone()["n"]
    assert inspect_attempts == 1


def test_inspection_handler_reserves_zero_and_uses_llm_pool(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    client = FakeLlmClient(inspection=_compatible_inspection())
    handler = build_inspection_handler(connection, client=client, config=config)
    assert handler.task_type == INSPECT_MODEL_TASK_TYPE
    assert handler.provider == PROVIDER
    assert handler.operation == INSPECT_OPERATION
    assert handler.pool is WorkerPool.LLM
    assert handler.reserved_nano_usd == 0
    assert handler.ready is None


# ---------------------------------------------------------------------------
# Multi-model inspection (K23 / K24)
# ---------------------------------------------------------------------------


def _seed_triaged_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    exact_name: str = "Alex Smith",
    outcome: str = "research",
    source_entry_id: str = "entry-mention",
    key: str = "feed-mention",
    work_fingerprint: str | None = None,
    routing_fingerprint_value: str | None = None,
) -> tuple[int, int]:
    """Return (source_item_id, person_mention_id) with a completed triage row."""
    item_id = _seed_source_item(
        connection,
        run_id=run_id,
        source_entry_id=source_entry_id,
        key=key,
        title_text=f"{exact_name} wins award",
        summary_text="A prize ceremony.",
    )
    fingerprint = work_fingerprint or (_HASH[:56] + f"{item_id:08d}")
    routing_fp = routing_fingerprint_value or (_HASH[:56] + f"{item_id:08d}")
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', ?, ?, 1, 30, ?, 'succeeded', ?, ?, ?)
        """,
        (item_id, fingerprint, NOW, run_id, NOW, NOW),
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
        (run_id, work, NOW, moment(1), fingerprint),
    ).lastrowid
    assert attempt_id is not None
    # Reuse a run-scoped inspection when one already exists for this model.
    existing_inspection = connection.execute(
        """
        SELECT id FROM model_inspection
         WHERE run_id = ? AND configured_model_id = ?
         LIMIT 1
        """,
        (run_id, MODEL),
    ).fetchone()
    if existing_inspection is None:
        inspection_id = connection.execute(
            """
            INSERT INTO model_inspection (
                run_id, attempt_id, configured_model_id, resolved_model_id,
                routing_fingerprint, supported_parameters_json,
                supports_strict_structured_output, pricing_usable,
                prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
                compatibility, inspected_at
            ) VALUES (?, ?, ?, ?, ?, '["response_format"]', 1, 1, 100, 200,
                      'compatible', ?)
            """,
            (run_id, attempt_id, MODEL, MODEL, routing_fp, NOW),
        ).lastrowid
        assert inspection_id is not None
    else:
        inspection_id = int(existing_inspection["id"])
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
            item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT_HASH,
            _SCHEMA_HASH,
            fingerprint,
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    connection.execute(
        """
        UPDATE source_item
           SET current_triage_observation_id = ?
         WHERE id = ?
        """,
        (observation_id, item_id),
    )
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, ?, '["p1"]', 'reason')
        """,
        (observation_id, exact_name, exact_name, outcome),
    ).lastrowid
    assert mention_id is not None
    connection.commit()
    return item_id, mention_id


def _mention_fingerprint(
    *,
    person_mention_id: int,
    exact_name: str,
    config: MainConfig,
    outcome: str = "research",
) -> str:
    return first_pass_task_fingerprint(
        person_mention_id=person_mention_id,
        mention_outcome=outcome,
        exact_name=exact_name,
        search_name=exact_name,
        identity_facts=(),
        signals=(),
        config=config.tasks.resolve_person_entity,
    )


def test_fully_triaged_eligible_mentions_schedule_resolve_model_inspect(
    connection: sqlite3.Connection,
) -> None:
    """Fully triaged corpus + K24-eligible mentions ⇒ resolve-model inspect.

    Kills early-return solely on untriaged source items: a resolve backlog
    would never unlock readiness.
    """
    run_id = insert_run(connection)
    _item_id, mention_id = _seed_triaged_mention(connection, run_id=run_id)
    config = _main_config(resolve_model=RESOLVE_MODEL)
    profile = _profile()

    assert (
        is_resolution_eligible_mention(
            connection,
            person_mention_id=mention_id,
            config=config,
            profile=profile,
        )
        is True
    )
    assert models_needed_for_run(connection, run_id, config) == (RESOLVE_MODEL,)

    ensured = ensure_model_inspections_for_run(
        connection, run_id=run_id, config=config, now=NOW
    )
    assert ensured == 1

    rows = connection.execute(
        """
        SELECT fingerprint, state FROM work_item
         WHERE task_type = ? AND state = 'pending'
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["fingerprint"] == _inspection_work_fingerprint(
        run_id=run_id,
        model_id=RESOLVE_MODEL,
        routing_fp=routing_fingerprint(config.openrouter.routing),
    )


def test_skipped_and_failed_only_corpus_skips_resolve_model_inspect(
    connection: sqlite3.Connection,
) -> None:
    """Skipped + failed ER mentions must not force perpetual resolve inspect (K24)."""
    run_id = insert_run(connection)
    config = _main_config(resolve_model=RESOLVE_MODEL)
    profile = _profile()
    resolve_hashes = resolution_prompt_and_schema_hashes()

    _item_a, skipped_mention = _seed_triaged_mention(
        connection,
        run_id=run_id,
        exact_name="!!!",  # empty match_key after normalize
        source_entry_id="skip",
        key="feed-skip",
    )
    # Use a name with empty match_key... "!!!" may still produce something.
    # Instead write an explicit skipped ER for a normal research mention.
    _item_b, failed_mention = _seed_triaged_mention(
        connection,
        run_id=run_id,
        exact_name="Blair Failed",
        source_entry_id="fail",
        key="feed-fail",
    )
    _item_c, skipped_named = _seed_triaged_mention(
        connection,
        run_id=run_id,
        exact_name="Casey Skipped",
        source_entry_id="skip2",
        key="feed-skip2",
    )

    skipped_fp = _mention_fingerprint(
        person_mention_id=skipped_named, exact_name="Casey Skipped", config=config
    )
    failed_fp = _mention_fingerprint(
        person_mention_id=failed_mention, exact_name="Blair Failed", config=config
    )
    with immediate(connection):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=skipped_named,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="skipped",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=resolve_hashes[0],
            schema_hash=resolve_hashes[1],
            schema_version=resolve_hashes[2],
            task_fingerprint=skipped_fp,
            rationale="empty match_key",
            failure_category=None,
            observed_at=NOW,
        )
        # Failed ER requires a real attempt FK.
        work = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            )
            VALUES (
                'inspect_model', 'model', NULL, ?, 1, 20, ?, 'failed_permanent',
                ?, ?, ?
            )
            """,
            (_OTHER_HASH, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert work is not None
        attempt_id = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint, failure_category
            )
            VALUES (
                ?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'failed', ?,
                'authentication'
            )
            """,
            (run_id, work, NOW, moment(1), _OTHER_HASH),
        ).lastrowid
        assert attempt_id is not None
        insert_entity_resolution_observation(
            connection,
            person_mention_id=failed_mention,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=None,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json=None,
            prompt_hash=resolve_hashes[0],
            schema_hash=resolve_hashes[1],
            schema_version=resolve_hashes[2],
            task_fingerprint=failed_fp,
            rationale="authentication",
            failure_category="authentication",
            observed_at=NOW,
        )

    # The "!!!" mention is only eligible if match_key is non-empty; close it
    # with a skipped ER at the current material fingerprint when so.
    if match_key("!!!"):
        bang_fp = _mention_fingerprint(
            person_mention_id=skipped_mention, exact_name="!!!", config=config
        )
        with immediate(connection):
            insert_entity_resolution_observation(
                connection,
                person_mention_id=skipped_mention,
                person_relation_id=None,
                run_id=run_id,
                attempt_id=None,
                model_inspection_id=None,
                disposition="skipped",
                semantic_outcome=None,
                selected_person_id=None,
                created_person_id=None,
                candidate_person_ids_json="[]",
                canonical_supplied_input_json="{}",
                validated_output_json=None,
                prompt_hash=resolve_hashes[0],
                schema_hash=resolve_hashes[1],
                schema_version=resolve_hashes[2],
                task_fingerprint=bang_fp,
                rationale="unusable name",
                failure_category=None,
                observed_at=NOW,
            )

    for mention_id in (skipped_named, failed_mention, skipped_mention):
        assert (
            is_resolution_eligible_mention(
                connection,
                person_mention_id=mention_id,
                config=config,
                profile=profile,
            )
            is False
        )

    assert models_needed_for_run(connection, run_id, config) == ()
    assert (
        ensure_model_inspections_for_run(
            connection, run_id=run_id, config=config, now=NOW
        )
        == 0
    )
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state IN ('pending', 'deferred', 'running')
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["n"]
    assert pending == 0


def test_task_types_for_model_maps_detect_and_resolve_separately() -> None:
    """Kills dropping resolve/reconsider from the task→model map (K23 wiring)."""
    shared = _main_config(model=MODEL, resolve_model=MODEL)
    assert task_types_for_model(shared, MODEL) == (
        DETECT_PEOPLE_TASK_TYPE,
        RESOLVE_PERSON_ENTITY_TASK_TYPE,
        RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    )

    split = _main_config(model=MODEL, resolve_model=RESOLVE_MODEL)
    assert task_types_for_model(split, MODEL) == (DETECT_PEOPLE_TASK_TYPE,)
    assert task_types_for_model(split, RESOLVE_MODEL) == (
        RESOLVE_PERSON_ENTITY_TASK_TYPE,
        RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    )
    assert task_types_for_model(split, "openai/gpt-other") == ()


def test_multi_model_inspect_when_only_wikipedia_backlog(
    connection: sqlite3.Connection,
) -> None:
    """K21: active Wikipedia plan arms only the match model (no detect/resolve)."""
    from notable_person_finder.people.identity import insert_person, upsert_sourced_name
    from notable_person_finder.wikipedia.repository import open_plan
    from notable_person_finder.wikipedia.service import (
        MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    )

    match_model = "openai/gpt-match-only"
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint="c" * 64,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Smith",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
        open_plan(
            connection,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint="d" * 64,
            created_at=NOW,
        )
    config = MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(model=MODEL),
            resolve_person_entity=ResolvePersonEntityConfig(model=RESOLVE_MODEL),
            match_wikipedia_identity=MatchWikipediaIdentityConfig(model=match_model),
        ),
    )
    assert models_needed_for_run(connection, run_id, config) == (match_model,)
    ensured = ensure_model_inspections_for_run(
        connection, run_id=run_id, config=config, now=NOW
    )
    assert ensured == 1
    assert task_types_for_model(config, match_model) == (
        MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    )
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending'
        """,
        (INSPECT_MODEL_TASK_TYPE,),
    ).fetchone()["n"]
    assert pending == 1


def test_permanent_resolve_preflight_writes_failed_er_with_inspection_attempt(
    connection: sqlite3.Connection,
) -> None:
    """K23: permanent resolve-model preflight settles resolve/reconsider work."""
    run_id = insert_run(connection)
    config = _main_config(resolve_model=RESOLVE_MODEL)
    _item_id, mention_id = _seed_triaged_mention(connection, run_id=run_id)
    resolve_fp = _mention_fingerprint(
        person_mention_id=mention_id, exact_name="Alex Smith", config=config
    )
    resolve_hashes = resolution_prompt_and_schema_hashes()

    # Active resolve work for the eligible mention.
    repository.schedule_work(
        connection,
        task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_mention",
        subject_id=mention_id,
        fingerprint=resolve_fp,
        required=True,
        priority=40,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )

    # Active reconsider work on a synthetic relation (needs two people + ER).
    with immediate(connection):
        person_a = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint="c" * 64,
            created_at=NOW,
        )
        person_b = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Other",
            identity_fingerprint="d" * 64,
            created_at=NOW,
        )
        # First-pass ER required before relation FK protocol for created_by.
        first_pass_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"created_new"}',
            prompt_hash=resolve_hashes[0],
            schema_hash=resolve_hashes[1],
            schema_version=resolve_hashes[2],
            task_fingerprint="e" * 64,  # different fingerprint so resolve work stays
            rationale="created",
            observed_at=NOW,
        )
        relation_id = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=first_pass_er,
            now=NOW,
        )
    reconsider_fp = "f" * 64
    repository.schedule_work(
        connection,
        task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        subject_kind="person_relation",
        subject_id=relation_id,
        fingerprint=reconsider_fp,
        required=True,
        priority=40,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )

    # Permanent inspect attempt (inspection work + attempt rows).
    inspect_work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES (
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
        )
        VALUES (
            ?, ?, 'openrouter', 'inspect_model', 1, ?, ?, 'failed', ?,
            'authentication'
        )
        """,
        (run_id, inspect_work, NOW, moment(1), "9" * 64),
    ).lastrowid
    assert inspection_attempt is not None
    connection.commit()

    settled = settle_active_tasks_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=inspection_attempt,
        model_inspection_id=None,
        model_id=RESOLVE_MODEL,
        failure_category="authentication",
        rationale="authentication",
        task_types=(
            RESOLVE_PERSON_ENTITY_TASK_TYPE,
            RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        ),
        now=moment(5),
        resolve_prompt_hash=resolve_hashes[0],
        resolve_schema_hash=resolve_hashes[1],
        resolve_schema_version=resolve_hashes[2],
    )
    assert settled == 2

    resolve_state = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RESOLVE_PERSON_ENTITY_TASK_TYPE, mention_id),
    ).fetchone()["state"]
    assert resolve_state == "failed_permanent"
    reconsider_state = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()["state"]
    assert reconsider_state == "failed_permanent"

    er = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=resolve_fp
    )
    assert er is not None
    assert er.disposition == "failed"
    assert er.attempt_id == inspection_attempt
    assert er.failure_category == "authentication"

    rel_er = load_er_by_relation_fingerprint(
        connection, person_relation_id=relation_id, task_fingerprint=reconsider_fp
    )
    assert rel_er is not None
    assert rel_er.disposition == "failed"
    assert rel_er.attempt_id == inspection_attempt

    # K24 closed for the resolve fingerprint.
    assert (
        is_resolution_eligible_mention(
            connection,
            person_mention_id=mention_id,
            config=config,
            profile=_profile(),
        )
        is False
    )
    # No generate_structured attempts invented by the settler.
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE operation = ?",
            (GENERATE_OPERATION,),
        ).fetchone()["n"]
        == 1  # the historical detection attempt from _seed_triaged_mention
    )


def test_handler_permanent_resolve_preflight_settles_resolve_not_detect(
    connection: sqlite3.Connection,
) -> None:
    """K23 through real inspect handler: resolve model fail must not settle detect.

    Kills regressions that drop resolve types from ``task_types_for_model`` or
    ``_settle_dependents`` while still calling the repository settler in unit
    tests — resolve work would stay pending forever under permanent preflight.
    """
    bootstrap = insert_run(connection)
    detect_item_id = _seed_source_item(
        connection,
        run_id=bootstrap,
        source_entry_id="detect-live",
        key="feed-detect-live",
        title_text="Untriaged item still needs detection",
        summary_text="Keep detect model in models_needed.",
    )
    _triaged_item, mention_id = _seed_triaged_mention(
        connection,
        run_id=bootstrap,
        exact_name="Alex Smith",
        source_entry_id="resolve-subject",
        key="feed-resolve-subject",
    )
    config = _main_config(resolve_model=RESOLVE_MODEL)
    resolve_fp = _mention_fingerprint(
        person_mention_id=mention_id, exact_name="Alex Smith", config=config
    )
    resolve_hashes = resolution_prompt_and_schema_hashes()

    with immediate(connection):
        person_a = insert_person(
            connection,
            run_id=bootstrap,
            display_name="Alex Smith",
            identity_fingerprint="c" * 64,
            created_at=NOW,
        )
        person_b = insert_person(
            connection,
            run_id=bootstrap,
            display_name="Alex Other",
            identity_fingerprint="d" * 64,
            created_at=NOW,
        )
        first_pass_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            run_id=bootstrap,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"created_new"}',
            prompt_hash=resolve_hashes[0],
            schema_hash=resolve_hashes[1],
            schema_version=resolve_hashes[2],
            task_fingerprint="e" * 64,
            rationale="created",
            observed_at=NOW,
        )
        relation_id = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=bootstrap,
            created_by_observation_id=first_pass_er,
            now=NOW,
        )
    reconsider_fp = "f" * 64

    class SelectiveInspectClient(FakeLlmClient):
        def inspect_model(
            self, request: ModelInspectionRequest
        ) -> ModelInspectionResult:
            self.inspect_calls.append(request)
            if request.model_id == RESOLVE_MODEL:
                raise ProviderFailure(
                    FailureCategory.AUTHENTICATION,
                    provider=PROVIDER,
                    operation=INSPECT_OPERATION,
                    detail="resolve model unauthorized",
                )
            return _compatible_inspection(model_id=request.model_id)

    client = SelectiveInspectClient()
    inspection_handler = build_inspection_handler(
        connection, client=client, config=config
    )
    generation_calls: list[int] = []

    detection_handler = TaskHandler(
        task_type=DETECT_PEOPLE_TASK_TYPE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        execute=lambda work, ordinal, prepared: (
            generation_calls.append(1)
            or TaskOutcome(state=WorkState.SUCCEEDED, reason=None)
        ),
        pool=WorkerPool.LLM,
        ready=lambda claimed_run_id: inspection_ready(
            connection,
            run_id=claimed_run_id,
            config=config,
            model_id=MODEL,
        ),
        reserved_nano_usd=0,
    )

    def seed(run_id: int) -> None:
        now = moment()
        # Domain work first so ensure sees both detect and resolve dependents.
        _schedule_detect(
            connection,
            run_id=run_id,
            source_item_id=detect_item_id,
            fingerprint="1" * 64,
        )
        repository.schedule_work(
            connection,
            task_type=RESOLVE_PERSON_ENTITY_TASK_TYPE,
            subject_kind="person_mention",
            subject_id=mention_id,
            fingerprint=resolve_fp,
            required=True,
            priority=40,
            eligible_at=now,
            run_id=run_id,
            now=now,
        )
        repository.schedule_work(
            connection,
            task_type=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
            subject_kind="person_relation",
            subject_id=relation_id,
            fingerprint=reconsider_fp,
            required=True,
            priority=40,
            eligible_at=now,
            run_id=run_id,
            now=now,
        )
        ensure_model_inspections_for_run(
            connection, run_id=run_id, config=config, now=now
        )

    run_id = _run_engine(
        connection,
        {
            INSPECT_MODEL_TASK_TYPE: inspection_handler,
            DETECT_PEOPLE_TASK_TYPE: detection_handler,
        },
        seed=seed,
        snapshot_fingerprint="r" * 64,
    )

    inspected = {call.model_id for call in client.inspect_calls}
    assert MODEL in inspected
    assert RESOLVE_MODEL in inspected

    resolve_row = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RESOLVE_PERSON_ENTITY_TASK_TYPE, mention_id),
    ).fetchone()
    assert resolve_row["state"] == "failed_permanent"

    reconsider_row = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, relation_id),
    ).fetchone()
    assert reconsider_row["state"] == "failed_permanent"

    detect_row = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, detect_item_id),
    ).fetchone()
    assert detect_row["state"] != "failed_permanent"

    inspection_attempt = connection.execute(
        """
        SELECT a.id AS attempt_id
          FROM attempt AS a
          JOIN work_item AS w ON w.id = a.work_item_id
         WHERE a.run_id = ?
           AND a.operation = ?
           AND a.outcome = 'failed'
           AND a.failure_category = 'authentication'
         ORDER BY a.id DESC
         LIMIT 1
        """,
        (run_id, INSPECT_OPERATION),
    ).fetchone()
    assert inspection_attempt is not None
    inspection_attempt_id = int(inspection_attempt["attempt_id"])

    er = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=resolve_fp
    )
    assert er is not None
    assert er.disposition == "failed"
    assert er.attempt_id == inspection_attempt_id
    assert er.failure_category == "authentication"

    rel_er = load_er_by_relation_fingerprint(
        connection, person_relation_id=relation_id, task_fingerprint=reconsider_fp
    )
    assert rel_er is not None
    assert rel_er.disposition == "failed"
    assert rel_er.attempt_id == inspection_attempt_id

    # Detect model preflight succeeded; generation may have run.
    assert inspection_ready(connection, run_id=run_id, config=config, model_id=MODEL)
    assert not inspection_ready(
        connection, run_id=run_id, config=config, model_id=RESOLVE_MODEL
    )
    # No generate_structured for resolve/reconsider (no handlers registered).
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM attempt
             WHERE operation = ? AND work_item_id IN (
                SELECT id FROM work_item
                 WHERE task_type IN (?, ?)
             )
            """,
            (
                GENERATE_OPERATION,
                RESOLVE_PERSON_ENTITY_TASK_TYPE,
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
            ),
        ).fetchone()["n"]
        == 0
    )


def test_inspection_ready_parameterized_by_model_id(
    connection: sqlite3.Connection,
) -> None:
    bootstrap = insert_run(connection)
    _seed_source_item(connection, run_id=bootstrap)
    config = _main_config(resolve_model=RESOLVE_MODEL)

    # Also seed an eligible mention so both models are needed when multi-seeded.
    run_for_mention = insert_run(connection)
    _seed_triaged_mention(connection, run_id=run_for_mention)

    # Manual seed of both inspect fingerprints for one engine run.
    def seed(run_id: int) -> None:
        ensure_model_inspections_for_run(
            connection, run_id=run_id, config=config, now=moment()
        )

    # Fake client returns compatible only for detect model; resolve would need
    # a second call — use a side-effect list of results.
    results = {
        MODEL: _compatible_inspection(model_id=MODEL),
        RESOLVE_MODEL: _compatible_inspection(model_id=RESOLVE_MODEL),
    }

    class MultiModelClient(FakeLlmClient):
        def inspect_model(
            self, request: ModelInspectionRequest
        ) -> ModelInspectionResult:
            self.inspect_calls.append(request)
            return results[request.model_id]

    client = MultiModelClient()
    handler = build_inspection_handler(connection, client=client, config=config)
    run_id = _run_engine(
        connection,
        {INSPECT_MODEL_TASK_TYPE: handler},
        seed=seed,
        snapshot_fingerprint="m" * 64,
    )

    assert {call.model_id for call in client.inspect_calls} == {MODEL, RESOLVE_MODEL}
    assert (
        inspection_ready(connection, run_id=run_id, config=config, model_id=MODEL)
        is True
    )
    assert (
        inspection_ready(
            connection, run_id=run_id, config=config, model_id=RESOLVE_MODEL
        )
        is True
    )
    # Backward-compatible default still names the detect model.
    assert inspection_ready(connection, run_id=run_id, config=config) is True
