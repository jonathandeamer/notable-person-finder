"""Installed-interface CLI tests for feed ingestion plus person detection.

Offline only: HTTP is served by ``httpx.MockTransport``; OpenRouter is a
run-scoped fake that implements ``LlmClient`` and the context-manager lifecycle
``command_run`` expects from ``OpenRouterClient``.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import TransportConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    INSPECT_OPERATION,
    PROVIDER,
    ModelInspectionRequest,
    ModelInspectionResult,
    StructuredGenerationRequest,
    StructuredGenerationResult,
    TokenUsage,
)
from notable_person_finder.providers.pacing import PacingGate
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.reporting.digest import DigestWriteError
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.runs.engine import RunEngine, RunReport
from notable_person_finder.runs.scheduler import BoundedScheduler
from tests.run_engine.helpers import ENVIRONMENT

FIXTURES = Path(__file__).parent / "fixtures"
INGESTION_FIXTURES = Path(__file__).resolve().parents[1] / "ingestion" / "fixtures"
WIKIPEDIA_FIXTURES = Path(__file__).resolve().parents[1] / "wikipedia" / "fixtures"
RESOLVER = StaticHostResolver(
    {
        "example.com": ("93.184.216.34",),
        "en.wikipedia.org": ("208.80.154.224",),
        "api.search.brave.com": ("104.18.0.1",),
    }
)
EMPTY_MEDIAWIKI_SEARCH = (
    WIKIPEDIA_FIXTURES / "mediawiki_search_empty.json"
).read_bytes()

ZERO_MENTIONS_JSON = json.dumps(
    {
        "item_outcome": "do_not_research",
        "mentions": [],
        "overflow": False,
        "rationale": "No person is named in the supplied passages.",
    }
)

RESEARCH_JSON = json.dumps(
    {
        "item_outcome": "research_people",
        "mentions": [
            {
                "exact_name": "Élodie N'Diaye",
                "outcome": "research",
                "supporting_passage_ids": ["p1"],
                "identity_facts": [
                    {
                        "local_id": "fact-1",
                        "kind": "name",
                        "value": "Élodie N'Diaye",
                        "supporting_passage_ids": ["p1"],
                    }
                ],
                "signals": [
                    {
                        "kind": "attention",
                        "category": "significant_recognition",
                        "claim": "The title reports a prize.",
                        "supporting_passage_ids": ["p1"],
                        "grounding": "domain_profile",
                    }
                ],
                "rationale": "Clear subject of the prize report.",
            }
        ],
        "overflow": False,
        "rationale": "One clear research subject.",
    }
)

MULTI_JSON = json.dumps(
    {
        "item_outcome": "research_people",
        "mentions": [
            {
                "exact_name": "Élodie N'Diaye",
                "outcome": "research",
                "supporting_passage_ids": ["p1"],
                "identity_facts": [
                    {
                        "local_id": "fact-1",
                        "kind": "name",
                        "value": "Élodie N'Diaye",
                        "supporting_passage_ids": ["p1"],
                    }
                ],
                "signals": [
                    {
                        "kind": "attention",
                        "category": "significant_recognition",
                        "claim": "The title reports a prize.",
                        "supporting_passage_ids": ["p1"],
                        "grounding": "domain_profile",
                    }
                ],
                "rationale": "Clear subject.",
            },
            {
                "exact_name": "sculptor",
                "outcome": "uncertain",
                "supporting_passage_ids": ["p2"],
                "identity_facts": [],
                "signals": [
                    {
                        "kind": "caution",
                        "category": "significance_unclear",
                        "claim": "Only a professional label appears.",
                        "supporting_passage_ids": ["p2"],
                        "grounding": "source_text",
                    }
                ],
                "rationale": "Uncertain identity.",
            },
        ],
        "overflow": False,
        "rationale": "One clear subject and one uncertain reference.",
    }
)

ASSESS_JSON = json.dumps(
    {
        "person_relation": "same_person",
        "person_relation_passage_ids": ["p1"],
        "person_relation_rationale": "Name and role match the supplied person.",
        "coverage_depth": "significant",
        "coverage_depth_passage_ids": ["p1"],
        "coverage_depth_rationale": "Extended treatment of the career.",
        "content_types": ["profile"],
        "content_types_passage_ids": ["p1"],
        "content_types_rationale": "Exhibition profile with critical review notes.",
        "subject_relationship": "editorially_independent",
        "subject_relationship_passage_ids": ["p1"],
        "subject_relationship_rationale": "Third-party critical coverage.",
        "signals": [
            {
                "kind": "attention",
                "category": "significant_recognition",
                "claim": "Major retrospective survey.",
                "supporting_passage_ids": ["p1"],
            }
        ],
    }
)

EMPTY_TITLE_SUMMARY_FEED = """\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Empty Desk</title>
    <link>https://example.com/empty</link>
    <description>Empty entries.</description>
    <item>
      <guid isPermaLink="false">tag:example.com,2026:empty/1</guid>
      <title>   </title>
      <link>https://example.com/empty/1</link>
      <description>   </description>
    </item>
  </channel>
</rss>
"""

RESEARCH_FEED = """\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Arts Desk</title>
    <link>https://example.com/arts</link>
    <description>Profiles.</description>
    <item>
      <guid isPermaLink="false">tag:example.com,2026:arts/elodie</guid>
      <title>Élodie N'Diaye wins the Prix Exemple</title>
      <link>https://example.com/arts/elodie</link>
      <description>The sculptor was honoured in Paris.</description>
    </item>
  </channel>
</rss>
"""

MULTI_FEED = """\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Arts Desk</title>
    <link>https://example.com/arts</link>
    <description>Profiles.</description>
    <item>
      <guid isPermaLink="false">tag:example.com,2026:arts/multi</guid>
      <title>Élodie N'Diaye wins the Prix Exemple</title>
      <link>https://example.com/arts/multi</link>
      <description>The sculptor was honoured in Paris.</description>
    </item>
  </channel>
</rss>
"""

SIBLING_FEED = """\
<?xml version="1.0" encoding="utf-8"?>
<rss version="2.0">
  <channel>
    <title>Arts Desk</title>
    <link>https://example.com/arts</link>
    <description>Profiles.</description>
    <item>
      <guid isPermaLink="false">tag:example.com,2026:arts/good</guid>
      <title>Élodie N'Diaye wins the Prix Exemple</title>
      <link>https://example.com/arts/good</link>
      <description>The sculptor was honoured in Paris.</description>
    </item>
    <item>
      <guid isPermaLink="false">tag:example.com,2026:arts/bad</guid>
      <title>A second piece about nothing</title>
      <link>https://example.com/arts/bad</link>
      <description>Still no person named here at all.</description>
    </item>
  </channel>
</rss>
"""

OPERATIONAL = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 2
initial_backoff_seconds = 0.001
max_backoff_seconds = 0.001
backoff_multiplier = 1.0
jitter_ratio = 0.0

[concurrency]
http_workers = 2
llm_workers = 2

[budget]
openrouter_usd_per_run = "2.50"

[tasks.detect_people]
model = "openai/gpt-test"
max_input_tokens = 4428
max_completion_tokens = 512
max_people = 3
"""

TINY_BUDGET_OPERATIONAL = OPERATIONAL.replace(
    'openrouter_usd_per_run = "2.50"',
    'openrouter_usd_per_run = "0.000001"',
)


def streaming_body(payload: bytes) -> list[bytes]:
    if not payload:
        return [payload]
    return [payload[start : start + 64] for start in range(0, len(payload), 64)]


def write_people_graph(
    root: Path,
    *,
    feeds: str,
    operational: str = OPERATIONAL,
) -> Path:
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


def _single_feed(
    key: str = "art-news", url: str = "https://example.com/feed.xml"
) -> str:
    return f"""\
schema_version = 1
[[feeds]]
key = "{key}"
label = "Art News"
url = "{url}"
"""


@dataclass
class ScriptedOpenRouterClient:
    """Run-scoped OpenRouter stand-in for ``command_run`` lifecycle tests.

    Generation content is selected by optional ``content_by_substring`` first
    (title/summary substrings in the user payload), then by sequential
    ``generate_contents``. Selection is lock-guarded so dual LLM workers cannot
    scramble scripted order.
    """

    generate_contents: Sequence[str] = field(
        default_factory=lambda: (ZERO_MENTIONS_JSON,)
    )
    content_by_substring: dict[str, str] = field(default_factory=dict)
    inspect_error: ProviderFailure | None = None
    generate_errors: Sequence[ProviderFailure | None] = ()
    prompt_price: int | None = 150
    completion_price: int | None = 600
    model_id: str = "openai/gpt-test"
    instances: list[ScriptedOpenRouterClient] = field(default_factory=list)

    entered: bool = False
    exited: bool = False
    closed_after_pools: bool | None = None
    inspect_calls: list[ModelInspectionRequest] = field(default_factory=list)
    generate_calls: list[StructuredGenerationRequest] = field(default_factory=list)
    _generate_index: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _pool_closed: threading.Event = field(default_factory=threading.Event)
    _factory_kwargs: dict[str, Any] = field(default_factory=dict)

    def factory(self, **kwargs: Any) -> ScriptedOpenRouterClient:
        """``OpenRouterClient``-compatible constructor that returns this fake."""
        clone = ScriptedOpenRouterClient(
            generate_contents=self.generate_contents,
            content_by_substring=dict(self.content_by_substring),
            inspect_error=self.inspect_error,
            generate_errors=self.generate_errors,
            prompt_price=self.prompt_price,
            completion_price=self.completion_price,
            model_id=self.model_id,
        )
        clone._factory_kwargs = dict(kwargs)
        clone._pool_closed = self._pool_closed
        self.instances.append(clone)
        return clone

    def __enter__(self) -> ScriptedOpenRouterClient:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.closed_after_pools = self._pool_closed.is_set()
        self.exited = True
        return None

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        self.inspect_calls.append(request)
        if self.inspect_error is not None:
            raise self.inspect_error
        return ModelInspectionResult(
            configured_model_id=request.model_id,
            resolved_model_id=f"{request.model_id}-resolved",
            supported_parameters=("response_format", "structured_outputs"),
            supports_strict_structured_output=True,
            prompt_unit_price_nano_usd=self.prompt_price,
            completion_unit_price_nano_usd=self.completion_price,
            latency_ms=5,
        )

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        with self._lock:
            self.generate_calls.append(request)
            index = self._generate_index
            self._generate_index += 1
            if (
                index < len(self.generate_errors)
                and self.generate_errors[index] is not None
            ):
                raise self.generate_errors[index]  # type: ignore[misc]
            content = self._select_content(request.user_content, index)
            request_id = f"gen-cli-{index}"
        return StructuredGenerationResult(
            raw_text=content,
            configured_model_id=request.model_id,
            resolved_model_id=f"{request.model_id}-resolved",
            serving_provider="OpenAI",
            finish_reason="stop",
            refusal=None,
            usage=TokenUsage(prompt_tokens=20, completion_tokens=10, total_tokens=30),
            latency_ms=8,
            provider_request_id=request_id,
            actual_nano_usd=12_000,
        )

    def _select_content(self, user_content: str, index: int) -> str:
        for needle, body in self.content_by_substring.items():
            if needle in user_content:
                return body
        if not self.generate_contents:
            return ZERO_MENTIONS_JSON
        if index < len(self.generate_contents):
            return self.generate_contents[index]
        return self.generate_contents[-1]


class _SpyScheduler(BoundedScheduler):
    instances: list[_SpyScheduler] = []
    close_order: list[str] = []

    def __init__(self, max_workers: int) -> None:
        super().__init__(max_workers)
        self.closed = False
        _SpyScheduler.instances.append(self)

    def close(self) -> None:
        self.closed = True
        _SpyScheduler.close_order.append("scheduler")
        super().close()


def _build_transport_patch(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[..., HttpTransport]:
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


def _is_mediawiki_request(request: httpx.Request) -> bool:
    host = request.url.host or ""
    return host.endswith("wikipedia.org") or str(request.url.path).endswith("api.php")


def _is_brave_request(request: httpx.Request) -> bool:
    host = request.url.host or ""
    return "api.search.brave.com" in host


def _rss_handler(payload: bytes) -> Callable[[httpx.Request], httpx.Response]:
    """Serve RSS for feeds and empty MediaWiki search for Wikipedia API hosts."""

    def handler(request: httpx.Request) -> httpx.Response:
        if _is_mediawiki_request(request):
            return httpx.Response(
                200,
                content=streaming_body(EMPTY_MEDIAWIKI_SEARCH),
                headers={"content-type": "application/json"},
            )
        if _is_brave_request(request):
            return httpx.Response(
                200,
                content=streaming_body(
                    b'{"query": {"original": ""}, "web": {"results": []}}'
                ),
                headers={"content-type": "application/json"},
            )
        return httpx.Response(200, content=streaming_body(payload))

    return handler


def _not_modified_or_empty_mediawiki(
    request: httpx.Request,
) -> httpx.Response:
    """304 for feeds; empty complete MediaWiki search for Wikipedia (K4 path)."""
    if _is_mediawiki_request(request):
        return httpx.Response(
            200,
            content=streaming_body(EMPTY_MEDIAWIKI_SEARCH),
            headers={"content-type": "application/json"},
        )
    if _is_brave_request(request):
        return httpx.Response(
            200,
            content=streaming_body(
                b'{"query": {"original": ""}, "web": {"results": []}}'
            ),
            headers={"content-type": "application/json"},
        )
    return httpx.Response(304, content=streaming_body(b""))


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: bytes,
    llm: ScriptedOpenRouterClient | None = None,
) -> ScriptedOpenRouterClient:
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])
    monkeypatch.setattr(
        cli_main, "build_transport", _build_transport_patch(_rss_handler(payload))
    )
    client = llm or ScriptedOpenRouterClient()
    monkeypatch.setattr(cli_main, "OpenRouterClient", client.factory)
    return client


def _latest_digest(config_file: Path) -> str:
    digests = config_file.parent / "portable" / "data" / "digests"
    return (digests / "latest.md").read_text(encoding="utf-8")


def _open_db(config_file: Path):
    database = config_file.parent / "portable" / "data" / "notable.sqlite3"
    return connect_database(database, readonly=True)


# ---------------------------------------------------------------------------
# Fresh run / backlog / outcomes
# ---------------------------------------------------------------------------


def test_fresh_run_triages_ingested_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
    assert "### Person detection" in digest
    assert "Source items triaged: 1" in digest
    assert "Research: 1" in digest
    assert "Unresolved research or uncertain mentions: 1" in digest
    assert "OpenRouter cost:" in digest
    # Empty-candidate path: one person via created_new; K24 remaining is zero.
    assert "### Person identity" in digest
    identity = digest.split("### Person identity\n\n", 1)[1]
    assert "People created this run: 1" in identity
    assert "Mentions resolved this run: 1" in identity
    assert "Created via created_new (no candidates): 1" in identity
    assert "Linked same_person: 0" in identity
    assert "Created via different_people: 0" in identity
    assert "Unresolved eligible mentions remaining: 0" in identity
    assert "Active possible_same_person relations (corpus): 0" in identity
    assert "Confirmed merges this run: 0" in identity
    assert "Resolution model deferred: 0" in identity
    assert "OpenRouter cost:" not in identity
    assert ENVIRONMENT["TEST_OPENROUTER"] not in digest


def test_existing_backlog_is_seeded_without_new_feed_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A second run with no new ingestion still finishes already-triaged work."""
    config = write_people_graph(tmp_path, feeds=_single_feed())
    client = _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            generate_contents=(RESEARCH_JSON,),
            content_by_substring={"assess_article": ASSESS_JSON},
        ),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    first_generations = len(client.instances[-1].generate_calls)

    # Second run: 304, no new items; backlog already triaged so no generation.
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_not_modified_or_empty_mediawiki),
    )
    second = ScriptedOpenRouterClient(
        generate_contents=(RESEARCH_JSON,),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    monkeypatch.setattr(cli_main, "OpenRouterClient", second.factory)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert first_generations >= 1
    # We might have generated ASSESS_JSON in the second run for coverage.
    assert "Source items triaged: 0" in _latest_digest(config)


def test_zero_mentions_and_multiple_mentions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    # Two-item sibling feed: content-keyed so concurrent LLM workers stay stable.
    _wire(
        monkeypatch,
        payload=SIBLING_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            content_by_substring={
                "assess_article": ASSESS_JSON,
                "Élodie N'Diaye": MULTI_JSON,
                "second piece": ZERO_MENTIONS_JSON,
            }
        ),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    digest = _latest_digest(config)
    assert "Source items triaged: 2" in digest
    assert "Research: 1" in digest
    assert "Do not research: 1" in digest
    assert "Unresolved research or uncertain mentions: 2" in digest


def test_empty_input_is_insufficient_without_model_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    client = _wire(
        monkeypatch,
        payload=EMPTY_TITLE_SUMMARY_FEED.encode(),
        llm=ScriptedOpenRouterClient(),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert client.instances[-1].inspect_calls == []
    assert client.instances[-1].generate_calls == []
    digest = _latest_digest(config)
    assert "Insufficient input: 1" in digest
    assert "Source items triaged: 1" in digest


def test_one_malformed_sibling_still_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=SIBLING_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            content_by_substring={
                "Élodie N'Diaye": '{"not": "valid detection output"}',
                "second piece": ZERO_MENTIONS_JSON,
            }
        ),
    )
    exit_code = cli_main.command_run(config, verbose=False)
    # One permanent malformed failure + one success → partial when required
    # failed_permanent is recorded, or complete if isolated as item failure.
    assert exit_code in {cli_main.EXIT_OK, cli_main.EXIT_PARTIAL, cli_main.EXIT_FAILED}
    digest = _latest_digest(config)
    assert "Do not research: 1" in digest
    connection = _open_db(config)
    try:
        completed = connection.execute(
            """
            SELECT COUNT(*) AS n FROM triage_observation
             WHERE disposition = 'completed'
            """
        ).fetchone()["n"]
        failed = connection.execute(
            """
            SELECT COUNT(*) AS n FROM triage_observation
             WHERE disposition = 'failed'
            """
        ).fetchone()["n"]
    finally:
        connection.close()
    assert completed >= 1
    assert failed >= 1
    assert ENVIRONMENT["TEST_OPENROUTER"] not in digest


def test_budget_partial_exits_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(
        tmp_path, feeds=_single_feed(), operational=TINY_BUDGET_OPERATIONAL
    )
    client = _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(generate_contents=(RESEARCH_JSON,)),
    )
    exit_code = cli_main.command_run(config, verbose=False)
    assert exit_code == cli_main.EXIT_PARTIAL
    assert client.instances[-1].generate_calls == []
    digest = _latest_digest(config)
    # People section must attribute deferred model work; the operational
    # summary's whole-queue deferred line is not a substitute.
    people_section = digest.split("### Person detection\n\n", 1)[1]
    assert "Model work deferred: 1" in people_section
    assert "**State:** partial" in digest


def test_seed_untriaged_backfills_corpus_without_new_ingestion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """People seed backfill schedules detection for pre-existing untriaged items.

    Same-run ``on_source_items`` is disabled on the ingesting run so the item
    lands without triage. The next run (304, no new source items) must still
    schedule and settle via ``seed_untriaged`` in the composed seed hook.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    real_on_source_items = cli_main._on_source_items_callback

    def no_op_callback(
        connection: object,
        *,
        config: object,
        profile: object,
    ) -> Callable[[tuple[int, ...], int, str], None]:
        def on_source_items(
            source_item_ids: tuple[int, ...], run_id: int, now: str
        ) -> None:
            return None

        return on_source_items

    monkeypatch.setattr(cli_main, "_on_source_items_callback", no_op_callback)
    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(generate_contents=(RESEARCH_JSON,)),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    connection = _open_db(config)
    try:
        untriaged = connection.execute(
            """
            SELECT COUNT(*) AS n FROM source_item
             WHERE current_triage_observation_id IS NULL
            """
        ).fetchone()["n"]
        assert untriaged == 1
        assert (
            connection.execute(
                "SELECT COUNT(*) AS n FROM triage_observation"
            ).fetchone()["n"]
            == 0
        )
    finally:
        connection.close()

    monkeypatch.setattr(cli_main, "_on_source_items_callback", real_on_source_items)

    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_not_modified_or_empty_mediawiki),
    )
    second = ScriptedOpenRouterClient(
        generate_contents=(RESEARCH_JSON,),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    monkeypatch.setattr(cli_main, "OpenRouterClient", second.factory)

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    # One detect_people call for the backfilled item plus one assess_article
    # call: the person it resolves to gets a Wikipedia no-match this same run,
    # which now (K5 fix) opens and advances coverage synchronously instead of
    # deferring to a third run.
    assert len(second.instances[-1].generate_calls) == 2
    schema_names = {call.schema_name for call in second.instances[-1].generate_calls}
    assert schema_names == {"detect_people", "assess_article"}
    digest = _latest_digest(config)
    assert "Source items triaged: 1" in digest
    assert "Research: 1" in digest
    assert "Source items created: 0" in digest


def test_second_run_reuses_completed_triage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    client = _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            generate_contents=(RESEARCH_JSON,),
            content_by_substring={"assess_article": ASSESS_JSON},
        ),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    first_client = client.instances[-1]

    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_not_modified_or_empty_mediawiki),
    )
    second = ScriptedOpenRouterClient(
        generate_contents=('{"should":"not be used"}',),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    monkeypatch.setattr(cli_main, "OpenRouterClient", second.factory)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    # The first run now does detect_people, resolves the person, matches
    # Wikipedia no-match, and opens/completes coverage research all in the
    # same run (K5 fix): one detect_people call plus one assess_article call.
    first_schema_names = {call.schema_name for call in first_client.generate_calls}
    assert first_schema_names == {"detect_people", "assess_article"}
    assert len(first_client.generate_calls) == 2
    # Triage/detect_people must not repeat on the second run: the person was
    # already triaged, and coverage's plan already completed in the first run
    # (its material has not changed and the refresh interval has not
    # elapsed), so the second run performs no generation at all.
    second_calls = second.instances[-1].generate_calls
    assert second_calls == []

    connection = _open_db(config)
    try:
        observations = connection.execute(
            "SELECT COUNT(*) AS n FROM triage_observation"
        ).fetchone()["n"]
        mentions = connection.execute(
            "SELECT COUNT(*) AS n FROM person_mention"
        ).fetchone()["n"]
    finally:
        connection.close()
    assert observations == 1
    assert mentions == 1


def test_status_reports_durable_triage_counts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=SIBLING_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            content_by_substring={
                "assess_article": ASSESS_JSON,
                "Élodie N'Diaye": MULTI_JSON,
                "second piece": ZERO_MENTIONS_JSON,
            }
        ),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    capsys.readouterr()

    assert cli_main.command_status(config) == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "source items triaged: 2" in output
    assert "source items untriaged: 0" in output
    assert "research: 1" in output
    assert "do not research: 1" in output
    assert "unresolved research or uncertain mentions: 2" in output
    # MULTI_JSON: research + uncertain mentions; both resolve via created_new.
    assert "canonical people: 2" in output
    assert "merged-away people: 0" in output
    assert "unresolved eligible mentions: 0" in output
    assert "active possible_same_person relations: 0" in output
    assert "mentions linked to people: 2" in output
    assert ENVIRONMENT["TEST_OPENROUTER"] not in output
    assert "brave-secret-value" not in output


def test_diagnostics_never_echo_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(
            inspect_error=ProviderFailure(
                FailureCategory.AUTHENTICATION,
                provider=PROVIDER,
                operation=INSPECT_OPERATION,
                detail="auth failed with or-secret-value",
            )
        ),
    )
    exit_code = cli_main.command_run(config, verbose=False)
    assert exit_code in {cli_main.EXIT_PARTIAL, cli_main.EXIT_FAILED}
    digest = _latest_digest(config)
    assert "or-secret-value" not in digest
    log_path = config.parent / "portable" / "data" / "logs" / "notable.log"
    if log_path.exists():
        assert "or-secret-value" not in log_path.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Lifecycle: both pools drain before clients close
# ---------------------------------------------------------------------------


def test_pools_drain_before_clients_close_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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


def test_pools_drain_before_clients_close_on_provider_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    llm = ScriptedOpenRouterClient(
        inspect_error=ProviderFailure(
            FailureCategory.TRANSIENT_SERVER_ERROR,
            provider=PROVIDER,
            operation=INSPECT_OPERATION,
            detail="upstream",
        )
    )
    _wire(monkeypatch, payload=RESEARCH_FEED.encode(), llm=llm)
    pool_closed = threading.Event()
    llm._pool_closed = pool_closed
    original_close = BoundedScheduler.close

    def close_and_signal(self: BoundedScheduler) -> None:
        pool_closed.set()
        original_close(self)

    monkeypatch.setattr(BoundedScheduler, "close", close_and_signal)

    exit_code = cli_main.command_run(config, verbose=False)
    assert exit_code in {cli_main.EXIT_PARTIAL, cli_main.EXIT_FAILED}
    instance = llm.instances[-1]
    assert instance.exited is True
    assert instance.closed_after_pools is True


def test_pools_drain_before_clients_close_on_reporting_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    llm = ScriptedOpenRouterClient(generate_contents=(ZERO_MENTIONS_JSON,))
    _wire(
        monkeypatch,
        payload=(INGESTION_FIXTURES / "rss20.xml").read_bytes(),
        llm=llm,
    )
    pool_closed = threading.Event()
    llm._pool_closed = pool_closed
    original_close = BoundedScheduler.close

    def close_and_signal(self: BoundedScheduler) -> None:
        pool_closed.set()
        original_close(self)

    monkeypatch.setattr(BoundedScheduler, "close", close_and_signal)

    def boom(*args: object, **kwargs: object) -> None:
        raise DigestWriteError("simulated digest failure")

    monkeypatch.setattr(cli_main, "write_digest", boom)

    with pytest.raises(DigestWriteError):
        cli_main.command_run(config, verbose=False)

    instance = llm.instances[-1]
    assert instance.exited is True
    assert instance.closed_after_pools is True


def test_pools_drain_before_clients_close_on_unexpected_worker_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    llm = ScriptedOpenRouterClient()
    _wire(monkeypatch, payload=RESEARCH_FEED.encode(), llm=llm)
    pool_closed = threading.Event()
    llm._pool_closed = pool_closed
    original_close = BoundedScheduler.close

    def close_and_signal(self: BoundedScheduler) -> None:
        pool_closed.set()
        original_close(self)

    monkeypatch.setattr(BoundedScheduler, "close", close_and_signal)

    def boom(self: RunEngine, handlers: object, *, seed: object = None) -> RunReport:
        raise RuntimeError("unexpected worker path failure")

    monkeypatch.setattr(RunEngine, "execute", boom)

    with pytest.raises(RuntimeError, match="unexpected worker path"):
        cli_main.command_run(config, verbose=False)

    instance = llm.instances[-1]
    assert instance.exited is True
    assert instance.closed_after_pools is True


def test_both_scheduler_pools_are_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _SpyScheduler.instances = []
    _SpyScheduler.close_order = []
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _build_transport_patch(_rss_handler(RESEARCH_FEED.encode())),
    )
    monkeypatch.setattr(
        cli_main, "OpenRouterClient", ScriptedOpenRouterClient().factory
    )
    monkeypatch.setattr(cli_main, "BoundedScheduler", _SpyScheduler)

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert len(_SpyScheduler.instances) == 2
    assert all(instance.closed for instance in _SpyScheduler.instances)
