"""CLI integration tests for the ingestion milestone.

Every test is offline: `build_transport` is monkey-patched to return a real
`HttpTransport` whose network layer is an `httpx.MockTransport`. The resolver is
replaced with `StaticHostResolver` so no DNS lookup ever happens. OpenRouter is
replaced with a scripted offline client so person-detection handlers never
reach the network when feed items are triaged.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from notable_person_finder.config.models import FeedsConfig, TransportConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.ingestion.service import seed_feeds
from notable_person_finder.providers.openrouter import (
    ModelInspectionRequest,
    ModelInspectionResult,
    StructuredGenerationRequest,
    StructuredGenerationResult,
    TokenUsage,
)
from notable_person_finder.providers.pacing import PacingGate
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.runs.clock import SystemClock, utc_timestamp
from tests.run_engine.helpers import ENVIRONMENT

FIXTURES = Path(__file__).parent / "fixtures"
RESOLVER = StaticHostResolver({"example.com": ("93.184.216.34",)})

_ZERO_MENTIONS = json.dumps(
    {
        "item_outcome": "do_not_research",
        "mentions": [],
        "overflow": False,
        "rationale": "No person is named in the supplied passages.",
    }
)

OPERATIONAL_SECTIONS = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 3
initial_backoff_seconds = 0.001
max_backoff_seconds = 0.001
backoff_multiplier = 1.0
jitter_ratio = 0.0

[concurrency]
http_workers = 4
llm_workers = 2

[budget]
openrouter_usd_per_run = "2.50"

[tasks.detect_people]
model = "openai/gpt-test"
"""


@dataclass
class _OfflineOpenRouter:
    """Minimal LlmClient + context manager for offline ingestion CLI runs."""

    entered: bool = False
    exited: bool = False
    generate_calls: list[StructuredGenerationRequest] = field(default_factory=list)

    def __enter__(self) -> _OfflineOpenRouter:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.exited = True
        return None

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        return ModelInspectionResult(
            configured_model_id=request.model_id,
            resolved_model_id=f"{request.model_id}-resolved",
            supported_parameters=("response_format", "structured_outputs"),
            supports_strict_structured_output=True,
            prompt_unit_price_nano_usd=150,
            completion_unit_price_nano_usd=600,
            latency_ms=1,
        )

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        self.generate_calls.append(request)
        return StructuredGenerationResult(
            raw_text=_ZERO_MENTIONS,
            configured_model_id=request.model_id,
            resolved_model_id=f"{request.model_id}-resolved",
            serving_provider="OpenAI",
            finish_reason="stop",
            refusal=None,
            usage=TokenUsage(prompt_tokens=10, completion_tokens=5, total_tokens=15),
            latency_ms=1,
            provider_request_id="gen-ingestion-offline",
            actual_nano_usd=1_000,
        )


def _offline_openrouter(**kwargs: Any) -> _OfflineOpenRouter:
    return _OfflineOpenRouter()


def streaming_body(payload: bytes) -> list[bytes]:
    """Hand `httpx.Response` a genuine stream rather than eager bytes.

    `httpx.Response(status, content=<bytes>)` eagerly reads and closes its own
    stream during construction, which makes the transport's `iter_raw()` raise
    `StreamConsumed`. A list of chunks is treated as a real stream instead.
    """
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def fixture_bytes(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


def write_ingestion_graph(
    root: Path,
    *,
    feeds: str,
    operational: str = OPERATIONAL_SECTIONS,
) -> Path:
    """Write a complete configuration tree with enabled feed(s)."""
    config_file = root / "notable.toml"
    config_file.write_text(
        """\
schema_version = 1
timezone = "Europe/Paris"
feeds_file = "feeds.toml"
domain_profile_file = "profiles/art.toml"
source_policy_file = "source_policies/visual_arts.toml"

[paths]
root = "portable"

[secrets]
openrouter_api_key = "TEST_OPENROUTER"
brave_api_key = "TEST_BRAVE"
"""
        + operational,
        encoding="utf-8",
    )
    (root / "profiles").mkdir()
    (root / "source_policies").mkdir()
    (root / "feeds.toml").write_text(feeds, encoding="utf-8")
    (root / "profiles" / "art.toml").write_text(
        """\
schema_version = 1
key = "visual-arts-en"
label = "English visual arts"
language = "en"
[attention_examples]
significant_recognition = ["major art prize"]
""",
        encoding="utf-8",
    )
    (root / "source_policies" / "visual_arts.toml").write_text(
        """\
schema_version = 1
key = "visual-arts-en-sources"
label = "English visual arts publisher policy"
[[rules]]
id = "eligible.example"
status = "curated_eligible"
match = { host_suffix = "example.com" }
rationale = "test eligible"
review_date = "2026-07-24"
""",
        encoding="utf-8",
    )
    return config_file


def _build_transport_patch(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[..., HttpTransport]:
    """Factory for a `build_transport` replacement that never touches the network."""

    def _patch(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
        pacing_gate: PacingGate | None = None,
    ) -> HttpTransport:
        return build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=httpx.MockTransport(handler),
            pacing_gate=pacing_gate,
        )

    return _patch


def _single_feed_handler(
    payload: bytes,
    *,
    status: int = 200,
    seen: list[httpx.Request] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        if seen is not None:
            seen.append(request)
        return httpx.Response(status, content=streaming_body(payload))

    return handler


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
""",
    )


@pytest.fixture
def monkeypatch_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])
    monkeypatch.setattr(cli_main, "OpenRouterClient", _offline_openrouter)


def test_run_ingests_feed_and_renders_counts(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_single_feed_handler(payload)),
    )

    exit_code = cli_main.command_run(config_file, verbose=False)

    assert exit_code == cli_main.EXIT_OK
    digests = config_file.parent / "portable" / "data" / "digests"
    latest = (digests / "latest.md").read_text(encoding="utf-8")
    assert "### Ingestion" in latest
    assert "Feeds fetched: 1" in latest
    assert "Feeds not modified: 0" in latest
    assert "Feeds failed: 0" in latest
    assert "Source items created: 2" in latest
    assert "Articles created: 2" in latest


def test_run_enforces_configured_same_origin_concurrency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    """The run's configured per-origin cap bounds actual HTTP calls."""
    config_file = write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "one"
label = "One"
url = "https://example.com/one.xml"
[[feeds]]
key = "two"
label = "Two"
url = "https://example.com/two.xml"
[[feeds]]
key = "three"
label = "Three"
url = "https://example.com/three.xml"
""",
        operational=OPERATIONAL_SECTIONS.replace(
            "http_workers = 4", "http_workers = 3\nper_origin = 1"
        ),
    )
    payload = fixture_bytes("rss20.xml")
    in_flight = 0
    peak = 0
    guard = threading.Lock()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal in_flight, peak
        with guard:
            in_flight += 1
            peak = max(peak, in_flight)
        try:
            # Long enough for all three scheduler workers to reach the fake
            # network layer when the configured gate is absent.
            time.sleep(0.05)
            return httpx.Response(200, content=streaming_body(payload))
        finally:
            with guard:
                in_flight -= 1

    monkeypatch.setattr(cli_main, "build_transport", _build_transport_patch(handler))

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    assert peak == 1


def test_second_run_reports_zero_new_source_items(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")

    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                headers={"ETag": '"fixture-etag"'},
                content=streaming_body(payload),
            )
        return httpx.Response(304, content=streaming_body(b""))

    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(handler),
    )

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK

    digests = config_file.parent / "portable" / "data" / "digests"
    dated = [path for path in digests.iterdir() if path.name != "latest.md"]
    assert len(dated) == 2
    latest = (digests / "latest.md").read_text(encoding="utf-8")
    assert "Feeds fetched: 0" in latest
    assert "Feeds not modified: 1" in latest
    assert "Source items created: 0" in latest
    assert "Articles created: 0" in latest


def test_failing_feed_still_reports_successful_ones(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    config_file = write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "good-feed"
label = "Good Feed"
url = "https://example.com/good.xml"
[[feeds]]
key = "bad-feed"
label = "Bad Feed"
url = "https://example.com/bad.xml"
[[feeds]]
key = "also-bad-feed"
label = "Also Bad Feed"
url = "https://example.com/also-bad.xml"
""",
    )
    good_payload = fixture_bytes("rss20.xml")

    def handler(request: httpx.Request) -> httpx.Response:
        if "/good.xml" in str(request.url):
            return httpx.Response(200, content=streaming_body(good_payload))
        return httpx.Response(401, content=streaming_body(b"authentication required"))

    monkeypatch.setattr(cli_main, "build_transport", _build_transport_patch(handler))

    exit_code = cli_main.command_run(config_file, verbose=False)

    assert exit_code == cli_main.EXIT_PARTIAL
    latest = (
        config_file.parent / "portable" / "data" / "digests" / "latest.md"
    ).read_text(encoding="utf-8")
    assert "Feeds fetched: 1" in latest
    assert "Feeds failed: 2" in latest


def test_latest_feed_settlement_controls_duplicate_fetch_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    """A URL move schedules duplicate work for one feed, settled in order."""
    config_file = write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
""",
        operational=OPERATIONAL_SECTIONS.replace(
            "http_workers = 4", "http_workers = 1"
        ),
    )
    payload = fixture_bytes("rss20.xml")
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, content=streaming_body(payload))
        return httpx.Response(304, content=streaming_body(b""))

    def duplicate_seed_hook(
        connection: sqlite3.Connection, *, feeds: FeedsConfig, clock: SystemClock
    ) -> Callable[[int], None]:
        moved_feed = feeds.feeds[0].model_copy(
            update={"url": "https://example.com/moved-feed.xml"}
        )
        moved_feeds = FeedsConfig(schema_version=1, feeds=(moved_feed,))

        def seed(run_id: int) -> None:
            now = utc_timestamp(clock.now())
            seed_feeds(connection, feeds=feeds, run_id=run_id, now=now)
            seed_feeds(connection, feeds=moved_feeds, run_id=run_id, now=now)

        return seed

    monkeypatch.setattr(cli_main, "build_transport", _build_transport_patch(handler))
    monkeypatch.setattr(cli_main, "build_seed_hook", duplicate_seed_hook)

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    assert calls == 2

    latest = (
        config_file.parent / "portable" / "data" / "digests" / "latest.md"
    ).read_text(encoding="utf-8")
    assert "Feeds fetched: 0" in latest
    assert "Feeds not modified: 1" in latest
    assert "Feeds failed: 0" in latest

    database = load_config(config_file, require_secrets=False).paths.database
    connection = connect_database(database, readonly=True)
    try:
        outcomes = connection.execute(
            "SELECT outcome FROM feed_fetch ORDER BY id"
        ).fetchall()
    finally:
        connection.close()
    assert [row["outcome"] for row in outcomes] == ["modified", "not_modified"]


def test_status_shows_source_items_and_latest_fetch(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = fixture_bytes("rss20.xml")
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_single_feed_handler(payload)),
    )

    assert cli_main.command_run(config_file, verbose=False) == cli_main.EXIT_OK
    capsys.readouterr()

    exit_code = cli_main.command_status(config_file)
    assert exit_code == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "source items: 2" in output
    assert "articles: 2" in output
    assert "latest successful fetch:" in output
    assert re.search(r"art-news: \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z", output)


def test_transport_is_closed_when_run_raises(
    config_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    payload = fixture_bytes("rss20.xml")
    built_transport: HttpTransport | None = None

    def capturing_patch(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
        pacing_gate: PacingGate | None = None,
    ) -> HttpTransport:
        nonlocal built_transport
        transport = build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=httpx.MockTransport(_single_feed_handler(payload)),
            pacing_gate=pacing_gate,
        )
        built_transport = transport
        transport.closed = False  # type: ignore[attr-defined]
        original_close = transport.close

        def _close() -> None:
            transport.closed = True  # type: ignore[attr-defined]
            original_close()

        transport.close = _close  # type: ignore[method-assign]
        return transport

    monkeypatch.setattr(cli_main, "build_transport", capturing_patch)

    def _boom(self: object, handlers: object, *, seed: object = None) -> None:
        raise RuntimeError("synthetic failure for transport-close test")

    monkeypatch.setattr(cli_main.RunEngine, "execute", _boom)

    with pytest.raises(RuntimeError, match="synthetic failure"):
        cli_main.command_run(config_file, verbose=False)

    assert built_transport is not None
    assert built_transport.closed is True  # type: ignore[attr-defined]


class _AbortRun(BaseException):
    """An unexpected worker failure that crosses both provider boundaries."""


class _InFlightTransport(httpx.BaseTransport):
    """Coordinate two real HTTP calls and observe transport shutdown safety."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.sibling_started = threading.Event()
        self.sibling_exited = threading.Event()
        self._release_sibling = threading.Event()
        self.closed_after_sibling: bool | None = None

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/fail.xml":
            if not self.sibling_started.wait(timeout=5):
                raise RuntimeError("sibling HTTP call did not start")
            raise _AbortRun("abort while sibling is in HTTP")

        self.sibling_started.set()
        try:
            self._release_sibling.wait(timeout=0.25)
            return httpx.Response(200, content=streaming_body(self._payload))
        finally:
            self.sibling_exited.set()

    def close(self) -> None:
        self.closed_after_sibling = self.sibling_exited.is_set()
        # Keep a broken exit order from making the regression wait for the
        # sibling's timeout after it has already observed the unsafe close.
        self._release_sibling.set()


def test_exceptional_unwind_waits_for_in_flight_http_before_closing_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monkeypatch_env: None,
) -> None:
    """Scheduler shutdown drains a sibling call before transport teardown.

    This exercises the real nested context managers, thread pool, engine, feed
    adapter, and HTTP transport. One HTTP call raises an unexpected
    ``BaseException`` only after its sibling has entered the network layer;
    the transport records the sibling's actual exit state when ``close`` runs.
    """
    config_file = write_ingestion_graph(
        tmp_path,
        feeds="""\
schema_version = 1
[[feeds]]
key = "fail"
label = "Fail"
url = "https://example.com/fail.xml"
[[feeds]]
key = "sibling"
label = "Sibling"
url = "https://example.com/sibling.xml"
""",
        operational=OPERATIONAL_SECTIONS.replace(
            "http_workers = 4", "http_workers = 2"
        ),
    )
    network = _InFlightTransport(fixture_bytes("rss20.xml"))

    def build_coordinated_transport(
        config: TransportConfig,
        *,
        version: str,
        resolver: object,
        clock: SystemClock,
        http_transport: httpx.BaseTransport | None = None,
        pacing_gate: PacingGate | None = None,
    ) -> HttpTransport:
        return build_transport(
            config,
            version=version,
            resolver=RESOLVER,
            clock=clock,
            http_transport=network,
            pacing_gate=pacing_gate,
        )

    monkeypatch.setattr(cli_main, "build_transport", build_coordinated_transport)

    with pytest.raises(_AbortRun, match="abort while sibling"):
        cli_main.command_run(config_file, verbose=False)

    assert network.sibling_started.is_set()
    assert network.sibling_exited.is_set()
    assert network.closed_after_sibling is True
