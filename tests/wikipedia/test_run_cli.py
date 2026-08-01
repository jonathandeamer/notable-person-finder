"""Installed-interface CLI tests for Wikipedia identity handlers and surfaces.

Offline only: feed HTTP and MediaWiki share ``httpx.MockTransport``; OpenRouter
is a run-scoped fake. Empty complete MediaWiki search yields deterministic
no-match without match-model generations (K4).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from notable_person_finder.config.models import TransportConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.pacing import PacingGate
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.runs.scheduler import WorkerPool
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
    MEDIAWIKI_SEARCH_TASK_TYPE,
    build_match_wikipedia_handler,
    build_mediawiki_page_facts_handler,
    build_mediawiki_search_handler,
)
from tests.people.test_run_cli import (
    ASSESS_JSON,
    RESEARCH_JSON,
    ScriptedOpenRouterClient,
    _single_feed,
    streaming_body,
    write_people_graph,
)
from tests.run_engine.helpers import ENVIRONMENT
from tests.wikipedia.test_mediawiki import client_for

WIKIPEDIA_FIXTURES = Path(__file__).parent / "fixtures"
EMPTY_MEDIAWIKI_SEARCH = (
    WIKIPEDIA_FIXTURES / "mediawiki_search_empty.json"
).read_bytes()
RESOLVER = StaticHostResolver(
    {
        "example.com": ("93.184.216.34",),
        "en.wikipedia.org": ("208.80.154.224",),
        "api.search.brave.com": ("104.18.0.1",),
    }
)


def _transport_patch(
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


def _http_handler(rss: bytes) -> Callable[[httpx.Request], httpx.Response]:
    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host or ""
        if host.endswith("wikipedia.org") or str(request.url.path).endswith("api.php"):
            return httpx.Response(
                200,
                content=streaming_body(EMPTY_MEDIAWIKI_SEARCH),
                headers={"content-type": "application/json"},
            )
        if "api.search.brave.com" in host:
            # K6's exact-name Brave forms schedule regardless of whether
            # discovery already satisfies the retrieval target, so a plan
            # opened this run always issues at least one real search call.
            return httpx.Response(
                200,
                content=streaming_body(
                    b'{"query": {"original": ""}, "web": {"results": []}}'
                ),
                headers={"content-type": "application/json"},
            )
        return httpx.Response(200, content=streaming_body(rss))

    return handler


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    payload: bytes | None = None,
    llm: ScriptedOpenRouterClient | None = None,
) -> ScriptedOpenRouterClient:
    from tests.people.test_run_cli import RESEARCH_FEED

    body = payload if payload is not None else RESEARCH_FEED.encode()
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])
    monkeypatch.setattr(
        cli_main, "build_transport", _transport_patch(_http_handler(body))
    )
    client = llm or ScriptedOpenRouterClient(
        generate_contents=(RESEARCH_JSON,),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    monkeypatch.setattr(cli_main, "OpenRouterClient", client.factory)
    return client


def _latest_digest(config_file: Path) -> str:
    digests = config_file.parent / "portable" / "data" / "digests"
    return (digests / "latest.md").read_text(encoding="utf-8")


def test_run_registers_three_wikipedia_handlers_and_settles_empty_search(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    client = _wire(monkeypatch)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    digest = _latest_digest(config)

    assert "### Wikipedia identity" in digest
    wiki = digest.split("### Wikipedia identity\n\n", 1)[1]
    if "### " in wiki:
        wiki = wiki.split("### ", 1)[0]
    assert "Deterministic no-match (empty complete search): 1" in wiki
    assert "No matching page (this run): 1" in wiki
    assert "People with current no-match (corpus): 1" in wiki
    assert "Wikipedia eligible remaining: 0" in wiki
    assert "Canonical people without Wikipedia pointer: 0" in wiki
    # K4: empty complete search never schedules match-model generation.
    # Detect generation still happens once. The no-match outcome also makes
    # this person coverage-eligible this same run (K5 fix): its originating
    # article is curated_eligible in the test source policy, so coverage opens
    # and completes with one assess_article generation alongside detect.
    generate_calls = client.instances[-1].generate_calls
    assert len(generate_calls) == 2
    assert {call.schema_name for call in generate_calls} == {
        "detect_people",
        "assess_article",
    }

    database = config.parent / "portable" / "data" / "notable.sqlite3"
    connection = connect_database(database, readonly=True)
    try:
        search_work = connection.execute(
            """
            SELECT state, COUNT(*) AS n FROM work_item
             WHERE task_type = ?
             GROUP BY state
            """,
            (MEDIAWIKI_SEARCH_TASK_TYPE,),
        ).fetchall()
        by_state = {str(row["state"]): int(row["n"]) for row in search_work}
        assert by_state.get("succeeded", 0) >= 1
        assert by_state.get("pending", 0) == 0
        match_pending = connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE task_type = ? AND state = 'pending'
            """,
            (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
        ).fetchone()
        assert match_pending is not None
        assert int(match_pending["n"]) == 0
        facts_pending = connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE task_type = ? AND state = 'pending'
            """,
            (MEDIAWIKI_PAGE_FACTS_TASK_TYPE,),
        ).fetchone()
        assert facts_pending is not None
        assert int(facts_pending["n"]) == 0
    finally:
        connection.close()


def test_status_reports_wikipedia_corpus_counters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    capsys.readouterr()

    assert cli_main.command_status(config) == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "people with current matching page: 0" in output
    assert "people with current no-match: 1" in output
    assert "people with current uncertain identity: 0" in output
    assert "wikipedia eligible remaining: 0" in output
    assert "canonical people without wikipedia pointer: 0" in output
    assert ENVIRONMENT["TEST_OPENROUTER"] not in output
    forbidden = "coverage skipped"
    assert forbidden not in output.lower()
    assert forbidden in (output + f"\n{forbidden}\n").lower()


def test_handlers_map_includes_wikipedia_on_correct_pools(
    tmp_path: Path,
) -> None:
    """Three kinds registered on HTTP (search/facts) and LLM (match) pools."""
    config_path = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_path, require_secrets=False)
    database = tmp_path / "wiki.sqlite3"
    connection = connect_database(database)
    try:
        apply_migrations(connection, database, tmp_path / "backups")
        mw = client_for(EMPTY_MEDIAWIKI_SEARCH)
        search = build_mediawiki_search_handler(
            connection, client=mw, config=loaded.main
        )
        facts = build_mediawiki_page_facts_handler(
            connection, client=mw, config=loaded.main
        )
        match = build_match_wikipedia_handler(
            connection, client=ScriptedOpenRouterClient(), config=loaded.main
        )
        assert search.pool is WorkerPool.HTTP
        assert facts.pool is WorkerPool.HTTP
        assert match.pool is WorkerPool.LLM
        assert search.task_type == MEDIAWIKI_SEARCH_TASK_TYPE
        assert facts.task_type == MEDIAWIKI_PAGE_FACTS_TASK_TYPE
        assert match.task_type == MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE
        assert search.reserved_nano_usd == 0
        assert facts.reserved_nano_usd == 0
    finally:
        connection.close()


def test_command_run_registers_three_wikipedia_handlers_on_engine_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """I1: ``command_run`` passes all three Wikipedia task types to the engine.

    Structural assert on the handlers mapping actually handed to
    ``RunEngine.execute`` (not a side-constructed builder). Kills omitting any
    of the three keys from the CLI map.
    """
    from notable_person_finder.runs.engine import RunEngine
    from notable_person_finder.runs.scheduler import WorkerPool as Pool

    captured: dict[str, object] = {}
    original_execute = RunEngine.execute

    def spy_execute(self: RunEngine, handlers: object, seed: object = None) -> object:
        assert isinstance(handlers, dict)
        captured["keys"] = frozenset(handlers.keys())
        captured["handlers"] = handlers
        return original_execute(self, handlers, seed=seed)

    monkeypatch.setattr(RunEngine, "execute", spy_execute)
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    keys = captured["keys"]
    assert isinstance(keys, frozenset)
    for task_type in (
        MEDIAWIKI_SEARCH_TASK_TYPE,
        MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
        MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    ):
        assert task_type in keys, f"missing handler key {task_type!r}"

    handlers = captured["handlers"]
    assert isinstance(handlers, dict)
    assert handlers[MEDIAWIKI_SEARCH_TASK_TYPE].pool is Pool.HTTP
    assert handlers[MEDIAWIKI_PAGE_FACTS_TASK_TYPE].pool is Pool.HTTP
    assert handlers[MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE].pool is Pool.LLM
    assert handlers[MEDIAWIKI_SEARCH_TASK_TYPE].reserved_nano_usd == 0
    assert handlers[MEDIAWIKI_PAGE_FACTS_TASK_TYPE].reserved_nano_usd == 0
