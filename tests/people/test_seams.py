"""Executable ownership tests for model-gateway and detection cross-component seams.

Each test names the source-level break it is intended to catch. Discrimination
is proven by mutating production source, clearing ``__pycache__``, observing
the named test fail, restoring via ``cp`` + ``diff``, and re-running green.
See ``.superpowers/sdd/.../task-13-report.md`` for the mutation mapping.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

import pytest
from openrouter.utils.retries import RetryConfig

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import FeedConfig
from notable_person_finder.people.detection import (
    DetectionValidationError,
    build_detection_input,
    render_detection_request,
    validate_detection_output,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    load_current_triage_observation,
    load_person_mentions,
    load_source_item_record,
)
from notable_person_finder.people.service import (
    INSPECT_MODEL_TASK_TYPE,
    MALFORMED_DETECTION_DETAIL,
    _DetectionCall,
    _execute_detection_for,
    build_detection_handler,
    build_inspection_handler,
    schedule_source_items,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    GENERATE_OPERATION,
    INSPECT_OPERATION,
    PROVIDER,
    ModelInspectionRequest,
    OpenRouterClient,
    StructuredGenerationRequest,
)
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.runs.scheduler import BoundedScheduler, WorkerPool
from tests.ingestion.helpers import insert_run, moment
from tests.people.fakes_openrouter import (
    FakeOpenRouterSdk,
    default_chat_result,
    default_model_response,
)
from tests.people.test_detection_service import (
    MODEL,
    ScriptedLlmClient,
    _compatible_inspection,
    _detect_work_rows,
    _generation_result,
    _handlers,
    _main_config,
    _people_seed,
    _profile,
    _run_engine,
    _seed_source_item,
    _uncertain_only_output,
    _zero_mentions_output,
)
from tests.people.test_openrouter import MODEL_ID, _client, _generation_request
from tests.people.test_run_cli import (
    ASSESS_JSON,
    RESEARCH_FEED,
    RESEARCH_JSON,
    ScriptedOpenRouterClient,
    _latest_digest,
    _single_feed,
    _wire,
    write_people_graph,
)
from tests.run_engine.helpers import ENVIRONMENT

FIXTURES = Path(__file__).with_name("fixtures")


# ---------------------------------------------------------------------------
# 1. Inspection readiness gates detection claim routing
# ---------------------------------------------------------------------------


def test_detection_claim_routing_requires_inspection_readiness(
    connection: sqlite3.Connection,
) -> None:
    """Kills removing ``ready=inspection_ready`` from detection claim routing.

    While inspection has not produced a compatible row, required detect_people
    work must stay unclaimed and generation must not run.
    """
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
    handler = build_detection_handler(
        connection, client=client, config=config, profile=profile
    )
    assert handler.ready is not None
    assert handler.ready(bootstrap) is False


# ---------------------------------------------------------------------------
# 2. Prepare-returned pricing, not the fixed handler reservation
# ---------------------------------------------------------------------------


def test_detection_uses_prepare_returned_pricing_not_handler_default(
    connection: sqlite3.Connection,
) -> None:
    """Kills using the fixed handler reservation instead of prepare pricing.

    Under a hard budget the generation attempt must reserve the worst-case
    amount derived from inspected unit prices and configured token ceilings,
    not the handler's static ``reserved_nano_usd=0``.
    """
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    max_input = 4415
    max_completion = 512
    config = _main_config(
        hard_budget=True,
        max_input_tokens=max_input,
        max_completion_tokens=max_completion,
    )
    profile = _profile()
    prompt_price = 150
    completion_price = 600
    record = load_source_item_record(connection, source_item_id=item)
    assert record is not None
    rendered = render_detection_request(
        build_detection_input(
            record,
            FeedConfig(key="feed-a", label="Arts News", url="https://example.com/feed"),
            profile,
            config.tasks.detect_people,
        )
    )
    # Derived from this item's own render, never a constant: a hardcoded
    # number would keep passing if the call site reverted to reserving the
    # configured `max_input_tokens` ceiling.
    input_tokens = rendered.worst_case_input_tokens
    assert input_tokens < max_input
    expected = prompt_price * input_tokens + completion_price * max_completion
    assert expected != 0
    assert expected != prompt_price * max_input + completion_price * max_completion
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(
            prompt_price=prompt_price, completion_price=completion_price
        ),
        generate_results=[
            _generation_result(_zero_mentions_output().model_dump_json())
        ],
    )
    handler = build_detection_handler(
        connection, client=client, config=config, profile=profile
    )
    assert handler.reserved_nano_usd == 0
    run_id = _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        hard_budget_limit=10_000_000_000,
    )
    gen = connection.execute(
        """
        SELECT reserved_nano_usd FROM attempt
         WHERE run_id = ? AND operation = ?
        """,
        (run_id, GENERATE_OPERATION),
    ).fetchone()
    assert gen is not None
    assert gen["reserved_nano_usd"] == expected
    assert gen["reserved_nano_usd"] != handler.reserved_nano_usd


# ---------------------------------------------------------------------------
# 3. OpenRouter work uses the LLM pool; pools sized independently
# ---------------------------------------------------------------------------


def test_openrouter_handlers_use_llm_pool_not_http(
    connection: sqlite3.Connection,
) -> None:
    """Kills routing inspection or generation onto the HTTP pool."""
    config = _main_config()
    profile = _profile()
    client = ScriptedLlmClient(inspection=_compatible_inspection())
    inspect = build_inspection_handler(connection, client=client, config=config)
    detect = build_detection_handler(
        connection, client=client, config=config, profile=profile
    )
    assert inspect.pool is WorkerPool.LLM
    assert detect.pool is WorkerPool.LLM
    assert inspect.pool is not WorkerPool.HTTP
    assert detect.pool is not WorkerPool.HTTP


def test_cli_sizes_http_and_llm_pools_from_distinct_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills sizing both pools from one concurrency setting."""
    operational = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 2
initial_backoff_seconds = 0.001
max_backoff_seconds = 0.001
backoff_multiplier = 1.0
jitter_ratio = 0.0

[concurrency]
http_workers = 3
llm_workers = 1

[budget]
openrouter_usd_per_run = "2.50"

[tasks.detect_people]
model = "openai/gpt-test"
max_input_tokens = 4415
max_completion_tokens = 512
max_people = 3
"""
    config = write_people_graph(tmp_path, feeds=_single_feed(), operational=operational)
    sizes: list[int] = []

    class _RecordingScheduler(BoundedScheduler):
        def __init__(self, max_workers: int) -> None:
            sizes.append(max_workers)
            super().__init__(max_workers)

    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            generate_contents=(RESEARCH_JSON,),
            content_by_substring={"assess_article": ASSESS_JSON},
        ),
    )
    monkeypatch.setattr(cli_main, "BoundedScheduler", _RecordingScheduler)

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert sizes == [3, 1]


# ---------------------------------------------------------------------------
# 4. SDK default retries stay disabled for inspection and generation
# ---------------------------------------------------------------------------


def test_sdk_retries_disabled_for_inspection_and_generation() -> None:
    """Kills enabling SDK default retries on inspect_model or generate_structured."""
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(),
        chat_result=default_chat_result(),
    )
    client = _client(sdk)
    client.inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    client.generate_structured(_generation_request())
    assert len(sdk.models_get_calls) == 1
    assert len(sdk.chat_send_calls) == 1
    for call in (*sdk.models_get_calls, *sdk.chat_send_calls):
        assert "retries" in call
        retries = call["retries"]
        assert isinstance(retries, RetryConfig)
        assert retries.strategy == "none"
        assert retries.retry_connection_errors is False
    # Client construction also pins strategy=none so a generated default
    # cannot re-enable retries when the per-call override is dropped.
    constructed = OpenRouterClient(
        api_key="test-key-not-a-secret-for-production",
        sdk=FakeOpenRouterSdk(model_response=default_model_response()),
    )
    assert constructed._retry.strategy == "none"  # noqa: SLF001


# ---------------------------------------------------------------------------
# 5. Unseen references validated in execute (not only persist)
# ---------------------------------------------------------------------------


def test_unseen_references_rejected_in_execute_not_only_persist(
    connection: sqlite3.Connection,
) -> None:
    """Kills moving unseen-reference validation solely into ``persist``.

    Domain validation must fail on the worker thread as a retryable
    ``MALFORMED_RESPONSE`` so the engine can retry; a succeed-then-validate
    persist path would commit a paid success without durable domain checks.
    """
    bootstrap = insert_run(connection)
    item = _seed_source_item(connection, run_id=bootstrap)
    config = _main_config()
    profile = _profile()
    invalid = (FIXTURES / "detect_people_invalid_references.json").read_text(
        encoding="utf-8"
    )
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(invalid)],
    )
    # Direct execute path: invalid refs must raise before any persist.
    record_fields = {
        "id": item,
        "feed_identity_id": 1,
        "feed_key": "feed-a",
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
        FeedConfig(key="feed-a", label="Arts News", url="https://example.com/feed"),
        profile,
        config.tasks.detect_people,
    )
    rendered = render_detection_request(detection_input)
    with pytest.raises(DetectionValidationError):
        validate_detection_output(invalid, detection_input)

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
        source_item_id=item,
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
        subject_id=item,
        fingerprint="f" * 64,
        required=True,
        priority=30,
        state=WorkState.RUNNING,
    )
    execute = _execute_detection_for(client)
    with pytest.raises(ProviderFailure) as raised:
        execute(work_item, 1, prepared)
    assert raised.value.category is FailureCategory.MALFORMED_RESPONSE
    assert raised.value.retryable is True
    assert raised.value.detail == MALFORMED_DETECTION_DETAIL
    # Persist must not re-validate: it only accepts prevalidated payloads.
    # Engine path: permanent failure after retries, no completed observation body.
    client2 = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[
            _generation_result(invalid),
            _generation_result(invalid),
        ],
    )
    _run_engine(
        connection,
        _handlers(connection, client2, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
        max_attempts=2,
    )
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "failed"
    assert current.validated_output_json is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM person_mention").fetchone()["n"]
        == 0
    )


# ---------------------------------------------------------------------------
# 6. Valid uncertain is a success, not MALFORMED_RESPONSE
# ---------------------------------------------------------------------------


def test_valid_uncertain_is_not_malformed_response(
    connection: sqlite3.Connection,
) -> None:
    """Kills converting a valid ``uncertain`` result into ``MALFORMED_RESPONSE``."""
    bootstrap = insert_run(connection)
    item = _seed_source_item(
        connection,
        run_id=bootstrap,
        title_text="A sculptor spoke",
        summary_text="The sculptor was honoured in Paris.",
    )
    config = _main_config()
    profile = _profile()
    raw = _uncertain_only_output().model_dump_json()
    client = ScriptedLlmClient(
        inspection=_compatible_inspection(),
        generate_results=[_generation_result(raw)],
    )
    _run_engine(
        connection,
        _handlers(connection, client, config, profile),
        seed=_people_seed(connection, config, profile, source_item_ids=[item]),
    )
    assert len(client.generate_calls) == 1
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "completed"
    assert current.semantic_outcome == "uncertain"
    assert current.failure_category is None
    mentions = load_person_mentions(connection, triage_observation_id=current.id)
    assert len(mentions) == 1
    assert mentions[0].outcome == "uncertain"
    # Permanent-malformed attempts would leave failed_permanent work.
    work = _detect_work_rows(connection, source_item_id=item)[0]
    assert work["state"] == "succeeded"


# ---------------------------------------------------------------------------
# 7. Empty input is insufficient_input without a fake attempt
# ---------------------------------------------------------------------------


def test_empty_input_is_insufficient_without_attempt_or_do_not_research(
    connection: sqlite3.Connection,
) -> None:
    """Kills persisting empty input as ``do_not_research`` or inventing an attempt."""
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
        now=moment(),
    )
    assert _detect_work_rows(connection, source_item_id=item) == []
    current = load_current_triage_observation(connection, source_item_id=item)
    assert current is not None
    assert current.disposition == "insufficient_input"
    assert current.semantic_outcome is None
    assert current.attempt_id is None
    assert current.failure_category is None
    assert connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()["n"] == 0
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (INSPECT_MODEL_TASK_TYPE,),
        ).fetchone()["n"]
        == 0
    )
    # Positive control: do_not_research is a distinct completed disposition.
    assert current.disposition != "completed"
    assert current.semantic_outcome != "do_not_research"


# ---------------------------------------------------------------------------
# 8. Ingestion downstream callback schedules detection
# ---------------------------------------------------------------------------


def test_ingestion_downstream_callback_schedules_detection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills omitting the ingestion ``on_source_items`` downstream callback.

    Without the callback, a successful feed fetch still inserts source items
    but never schedules detect_people or inspection work in the same run.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            generate_contents=(RESEARCH_JSON,),
            content_by_substring={"assess_article": ASSESS_JSON},
        ),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    digest = _latest_digest(config)
    assert "Source items triaged: 1" in digest
    assert "Research: 1" in digest
    database = config.parent / "portable" / "data" / "notable.sqlite3"
    connection = sqlite3.connect(database)
    connection.row_factory = sqlite3.Row
    try:
        detect = connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (DETECT_PEOPLE_TASK_TYPE,),
        ).fetchone()["n"]
        assert detect >= 1
        completed = connection.execute(
            """
            SELECT COUNT(*) AS n FROM triage_observation
             WHERE disposition = 'completed'
            """
        ).fetchone()["n"]
        assert completed == 1
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# 9. Provider clients close only after pools drain
# ---------------------------------------------------------------------------


def test_provider_clients_close_only_after_pools_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills closing either provider client while its pool still has a worker."""
    config = write_people_graph(tmp_path, feeds=_single_feed())
    llm = ScriptedOpenRouterClient(
        generate_contents=(RESEARCH_JSON,),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    _wire(monkeypatch, payload=RESEARCH_FEED.encode(), llm=llm)
    pool_closed = threading.Event()
    llm._pool_closed = pool_closed
    original_close = BoundedScheduler.close

    def close_and_signal(self: BoundedScheduler) -> None:
        pool_closed.set()
        original_close(self)

    monkeypatch.setattr(BoundedScheduler, "close", close_and_signal)

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    instance = llm.instances[-1]
    assert instance.entered is True
    assert instance.exited is True
    assert instance.closed_after_pools is True


# ---------------------------------------------------------------------------
# 10. Raw model output and synthetic secrets stay out of failure detail / logs
# ---------------------------------------------------------------------------


def test_failure_detail_and_logs_exclude_raw_model_output_and_secrets(
    connection: sqlite3.Connection,
    caplog: pytest.LogCaptureFixture,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Kills including raw model output or a synthetic secret in logs/detail."""
    sentinel_body = '{"raw":"SECRET_MODEL_BODY_SHOULD_NOT_LEAK"}'
    secret_token = "or-secret-value-seam-test"
    config_cfg = _main_config()
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
        FeedConfig(key="arts-news", label="Arts News", url="https://example.com/feed"),
        profile,
        config_cfg.tasks.detect_people,
    )
    rendered = render_detection_request(detection_input)
    client = ScriptedLlmClient(
        generate_results=[_generation_result(sentinel_body)],
    )
    prepared = _DetectionCall(
        request=StructuredGenerationRequest(
            model_id=MODEL,
            system_prompt=rendered.system_prompt,
            user_content=rendered.user_input_json,
            json_schema=rendered.schema,
            schema_name="detect_people",
            max_completion_tokens=config_cfg.tasks.detect_people.max_completion_tokens,
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
        priority=30,
        state=WorkState.RUNNING,
    )
    with caplog.at_level(logging.DEBUG):
        execute = _execute_detection_for(client)
        with pytest.raises(ProviderFailure) as raised:
            execute(work_item, 1, prepared)
    failure = raised.value
    assert failure.detail == MALFORMED_DETECTION_DETAIL
    assert sentinel_body not in (failure.detail or "")
    assert "SECRET_MODEL_BODY" not in (failure.detail or "")
    assert "SECRET_MODEL_BODY" not in str(failure)
    assert "SECRET_MODEL_BODY" not in caplog.text

    # CLI diagnostics must not echo a synthetic provider secret either.
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            inspect_error=ProviderFailure(
                FailureCategory.AUTHENTICATION,
                provider=PROVIDER,
                operation=INSPECT_OPERATION,
                detail=f"auth failed with {secret_token}",
            )
        ),
    )
    exit_code = cli_main.command_run(config, verbose=False)
    assert exit_code in {cli_main.EXIT_PARTIAL, cli_main.EXIT_FAILED}
    digest = _latest_digest(config)
    assert secret_token not in digest
    assert ENVIRONMENT["TEST_OPENROUTER"] not in digest
    log_path = config.parent / "portable" / "data" / "logs" / "notable.log"
    if log_path.exists():
        assert secret_token not in log_path.read_text(encoding="utf-8")
        assert ENVIRONMENT["TEST_OPENROUTER"] not in log_path.read_text(
            encoding="utf-8"
        )
