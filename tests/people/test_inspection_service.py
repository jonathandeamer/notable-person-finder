"""Model inspection seeding, handler settlement, and detection readiness."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from notable_person_finder.config.models import (
    BudgetConfig,
    DetectPeopleConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    RetryConfig,
    TasksConfig,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    insert_failed_observation,
    load_current_triage_observation,
    load_model_inspection,
)
from notable_person_finder.people.service import (
    INSPECT_MODEL_PRIORITY,
    INSPECT_MODEL_TASK_TYPE,
    build_inspection_handler,
    ensure_model_inspection,
    inspection_ready,
    routing_fingerprint,
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
NOW = moment()
_PROMPT_HASH = "p" * 64
_SCHEMA_HASH = "s" * 64
_TASK_FINGERPRINT = "t" * 64


def _main_config(
    *,
    model: str = MODEL,
    hard_budget: bool = False,
    routing: ProviderRoutingConfig | None = None,
) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        budget=BudgetConfig(openrouter_usd_per_run="1.00" if hard_budget else None),
        openrouter=OpenRouterConfig(
            routing=routing if routing is not None else ProviderRoutingConfig()
        ),
        tasks=TasksConfig(detect_people=DetectPeopleConfig(model=model)),
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
