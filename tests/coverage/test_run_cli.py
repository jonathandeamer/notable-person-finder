"""Installed-interface CLI tests for coverage evidence registration and runs.

Offline only: feed, MediaWiki, and Brave HTTP share ``httpx.MockTransport``;
OpenRouter is a run-scoped fake. Mirrors the reviewed house style in
``tests/wikipedia/test_run_cli.py``.

This file replaces a two-line stub (``def
test_run_cli_coverage_registered(): pass``) that reported green in every gate
while testing nothing about coverage CLI registration, seeding order,
eligibility, or the Wikipedia-match stop/supersede path (K5).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

import httpx
import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import TransportConfig
from notable_person_finder.coverage.repository import insert_query_forms, open_plan
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.coverage.service import (
    ASSESS_ARTICLE_TASK_TYPE,
    BRAVE_WEB_SEARCH_TASK_TYPE,
    FETCH_ARTICLE_TASK_TYPE,
)
from notable_person_finder.db.connection import connect_database
from notable_person_finder.providers.pacing import PacingGate
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import HttpTransport, build_transport
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from tests.ingestion.helpers import immediate, moment
from tests.people.test_run_cli import (
    ASSESS_JSON,
    RESEARCH_FEED,
    RESEARCH_JSON,
    ScriptedOpenRouterClient,
    _single_feed,
    streaming_body,
    write_people_graph,
)
from tests.run_engine.helpers import ENVIRONMENT
from tests.wikipedia.test_run_cli import EMPTY_MEDIAWIKI_SEARCH

ZERO_RESULTS_BRAVE = b'{"query": {"original": ""}, "web": {"results": []}}'


def _is_mediawiki_request(request: httpx.Request) -> bool:
    host = request.url.host or ""
    return host.endswith("wikipedia.org") or str(request.url.path).endswith("api.php")


def _is_brave_request(request: httpx.Request) -> bool:
    return "api.search.brave.com" in (request.url.host or "")


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


def _handler(
    *,
    rss: bytes | None,
    brave_calls: list[httpx.Request] | None = None,
) -> Callable[[httpx.Request], httpx.Response]:
    """Serve RSS/304, deterministic empty MediaWiki search, and zero-result Brave.

    ``rss`` of ``None`` returns 304 (unchanged feed material) for non-MediaWiki,
    non-Brave hosts, matching the "second run over unchanged material" shape
    used elsewhere in this suite.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        if _is_mediawiki_request(request):
            return httpx.Response(
                200,
                content=streaming_body(EMPTY_MEDIAWIKI_SEARCH),
                headers={"content-type": "application/json"},
            )
        if _is_brave_request(request):
            if brave_calls is not None:
                brave_calls.append(request)
            return httpx.Response(
                200,
                content=streaming_body(ZERO_RESULTS_BRAVE),
                headers={"content-type": "application/json"},
            )
        if rss is None:
            return httpx.Response(304, content=streaming_body(b""))
        return httpx.Response(200, content=streaming_body(rss))

    return handler


def _wire(
    monkeypatch: pytest.MonkeyPatch,
    *,
    rss: bytes | None,
    brave_calls: list[httpx.Request] | None = None,
    llm: ScriptedOpenRouterClient | None = None,
) -> ScriptedOpenRouterClient:
    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", ENVIRONMENT["TEST_BRAVE"])
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _transport_patch(_handler(rss=rss, brave_calls=brave_calls)),
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


def _database_path(config_file: Path) -> Path:
    return config_file.parent / "portable" / "data" / "notable.sqlite3"


def _sole_person_id(config_file: Path) -> int:
    connection = connect_database(_database_path(config_file), readonly=True)
    try:
        rows = connection.execute(
            "SELECT id FROM person WHERE merged_into_person_id IS NULL"
        ).fetchall()
        assert len(rows) == 1, f"expected exactly one canonical person, got {rows!r}"
        return int(rows[0]["id"])
    finally:
        connection.close()


def _plan_rows(config_file: Path, *, person_id: int) -> list[tuple[int, str]]:
    connection = connect_database(_database_path(config_file), readonly=True)
    try:
        rows = connection.execute(
            """
            SELECT id, status FROM person_coverage_plan
             WHERE person_id = ?
             ORDER BY id
            """,
            (person_id,),
        ).fetchall()
        return [(int(row["id"]), str(row["status"])) for row in rows]
    finally:
        connection.close()


def _current_wikipedia_semantic_outcome(
    config_file: Path, *, person_id: int
) -> str | None:
    connection = connect_database(_database_path(config_file), readonly=True)
    try:
        row = connection.execute(
            """
            SELECT o.semantic_outcome AS outcome
              FROM person p
              JOIN wikipedia_identity_observation o
                ON o.id = p.current_wikipedia_identity_observation_id
             WHERE p.id = ?
            """,
            (person_id,),
        ).fetchone()
        return None if row is None else str(row["outcome"])
    finally:
        connection.close()


def _force_matching_wikipedia_page(config_file: Path, *, person_id: int) -> None:
    """Directly overwrite the person's current Wikipedia pointer to a match.

    Simulates a later Wikipedia re-match finding a page, without driving the
    full MediaWiki search/facts/match pipeline through the CLI a second time
    (that pipeline is exercised elsewhere in ``tests/wikipedia``). This is the
    exact durable shape ``point_person_current_wikipedia_observation`` leaves
    behind (with satisfying attempt/inspection/page rows, per migration 0006's
    outcome truth-table CHECK), so ``seed_coverage_research`` sees the same
    state a real match would produce.
    """
    connection = connect_database(_database_path(config_file))
    try:
        with immediate(connection) as conn:
            run_id = conn.execute("SELECT MAX(id) AS n FROM run").fetchone()["n"]
            work_item_id = conn.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    created_at, updated_at
                ) VALUES ('match_wikipedia_identity', 'person', ?, ?, 1, 55, ?,
                          'succeeded', ?, ?, ?)
                """,
                (person_id, "d" * 64, moment(), run_id, moment(), moment()),
            ).lastrowid
            attempt_id = conn.execute(
                """
                INSERT INTO attempt (
                    run_id, work_item_id, provider, operation, ordinal,
                    started_at, finished_at, outcome, request_fingerprint
                ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?,
                          'succeeded', ?)
                """,
                (run_id, work_item_id, moment(), moment(1), "e" * 64),
            ).lastrowid
            inspection_id = conn.execute(
                """
                INSERT INTO model_inspection (
                    run_id, attempt_id, configured_model_id, resolved_model_id,
                    routing_fingerprint, supported_parameters_json,
                    supports_strict_structured_output, pricing_usable,
                    prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
                    compatibility, inspected_at
                ) VALUES (?, ?, 'openai/gpt-match', 'openai/gpt-match', ?,
                          '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
                """,
                (run_id, attempt_id, "f" * 64, moment()),
            ).lastrowid
            page_id = upsert_mediawiki_page(
                conn,
                wiki_id="enwiki",
                page_id=9001,
                canonical_title="Test Person",
                canonical_url="https://en.wikipedia.org/wiki/Test_Person",
                namespace=0,
                is_disambiguation=False,
                is_missing=False,
                redirect_to_page_id=None,
                description=None,
                extract="bio",
                categories_json="[]",
                last_observed_at=moment(),
                last_attempt_id=None,
            )
            obs_id = insert_wikipedia_identity_observation(
                conn,
                person_id=person_id,
                plan_id=None,
                run_id=int(run_id),
                attempt_id=attempt_id,
                model_inspection_id=inspection_id,
                disposition="completed",
                semantic_outcome="matching_page_found",
                matched_mediawiki_page_id=page_id,
                candidate_page_ids_json="[9001]",
                canonical_supplied_input_json="{}",
                validated_output_json='{"outcome":"matching_page_found"}',
                prompt_hash="a" * 64,
                schema_hash="b" * 64,
                schema_version=1,
                task_fingerprint="f" * 64,
                rationale="forced match for test",
                failure_category=None,
                observed_at=moment(),
            )
            point_person_current_wikipedia_observation(
                conn, person_id=person_id, observation_id=obs_id
            )
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Rule 1: handler registration
# ---------------------------------------------------------------------------


def test_command_run_registers_three_coverage_handlers_on_engine_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K2: ``command_run`` hands the engine all three coverage work kinds.

    Structural assert on the handlers mapping actually passed to
    ``RunEngine.execute`` (not a side-constructed builder), matching the
    Wikipedia equivalent. Kills omitting any of the three keys from the CLI
    map.
    """
    from notable_person_finder.runs.engine import RunEngine, RunReport, TaskHandler

    captured: dict[str, object] = {}
    original_execute = RunEngine.execute

    def spy_execute(
        self: RunEngine,
        handlers: Mapping[str, TaskHandler],
        *,
        seed: Callable[[int], None] | None = None,
    ) -> RunReport:
        assert isinstance(handlers, dict)
        captured["keys"] = frozenset(handlers.keys())
        return original_execute(self, handlers, seed=seed)

    monkeypatch.setattr(RunEngine, "execute", spy_execute)
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    keys = captured["keys"]
    assert isinstance(keys, frozenset)
    for task_type in (
        BRAVE_WEB_SEARCH_TASK_TYPE,
        FETCH_ARTICLE_TASK_TYPE,
        ASSESS_ARTICLE_TASK_TYPE,
    ):
        assert task_type in keys, f"missing handler key {task_type!r}"


# ---------------------------------------------------------------------------
# Rule 2: seed ordering
# ---------------------------------------------------------------------------


def test_seed_runs_coverage_research_between_wikipedia_and_model_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Seed order: Wikipedia identity, then coverage research, then inspection."""
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())

    import sqlite3

    from notable_person_finder.config.models import MainConfig

    order: list[str] = []
    real_wikipedia = cli_main.seed_wikipedia_identity
    real_coverage = cli_main.seed_coverage_research
    real_ensure = cli_main.ensure_model_inspections_for_run

    def spy_wikipedia(
        connection: sqlite3.Connection, *, run_id: int, config: MainConfig, now: str
    ) -> int:
        order.append("wikipedia")
        return real_wikipedia(connection, run_id=run_id, config=config, now=now)

    def spy_coverage(
        connection: sqlite3.Connection,
        *,
        run_id: int,
        config: MainConfig,
        policy: SourcePolicy,
        now: str,
    ) -> int:
        order.append("coverage")
        return real_coverage(
            connection, run_id=run_id, config=config, policy=policy, now=now
        )

    def spy_ensure(
        connection: sqlite3.Connection, *, run_id: int, config: MainConfig, now: str
    ) -> int:
        order.append("ensure_model_inspections")
        return real_ensure(connection, run_id=run_id, config=config, now=now)

    monkeypatch.setattr(cli_main, "seed_wikipedia_identity", spy_wikipedia)
    monkeypatch.setattr(cli_main, "seed_coverage_research", spy_coverage)
    monkeypatch.setattr(cli_main, "ensure_model_inspections_for_run", spy_ensure)

    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    assert order == ["wikipedia", "coverage", "ensure_model_inspections"]


# ---------------------------------------------------------------------------
# Rules 3, 4, 5: eligibility, stop/supersede, and no double work
# ---------------------------------------------------------------------------


def test_no_matching_page_found_opens_exactly_one_coverage_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A person whose current Wikipedia observation is a no-match opens one plan.

    Two CLI runs are taken rather than asserting mid-first-run state: the first
    run resolves the person's Wikipedia identity; a following run (unchanged
    material) is where ``seed_coverage_research``'s top-of-run batch backfill
    is guaranteed to observe the now-completed Wikipedia pointer and act on it
    (see the reported finding about the same-run inline hook). This shape is
    robust to that finding being fixed later: if the plan already opened
    inline during the first run, the second run's reuse guard simply leaves
    the count at one.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    person_id = _sole_person_id(config)
    assert (
        _current_wikipedia_semantic_outcome(config, person_id=person_id)
        == "no_matching_page_found"
    )

    _wire(monkeypatch, rss=None, llm=ScriptedOpenRouterClient())
    cli_main.command_run(config, verbose=False)

    plans = _plan_rows(config, person_id=person_id)
    assert len(plans) == 1


def test_matching_wikipedia_page_opens_no_plan_and_supersedes_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """K5: a current ``matching_page_found`` stops research and supersedes work.

    An active plan with pending Brave work is inserted directly (the same
    shape ``tests/coverage/test_seed_and_hooks.py`` uses for this precondition)
    so the "opens no plan and supersedes existing work" assertions below have
    real in-flight work to act on, rather than starting from nothing. The
    plan's ``retrieving`` status and the work item's ``pending`` state are
    asserted before the run as the positive control: both are reachable, and
    real work would use exactly this shape.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    person_id = _sole_person_id(config)

    database = _database_path(config)
    connection = connect_database(database)
    try:
        with immediate(connection) as conn:
            run_id = conn.execute("SELECT MAX(id) AS n FROM run").fetchone()["n"]
            plan_id = open_plan(
                conn,
                person_id=person_id,
                run_id=int(run_id),
                material_fingerprint="a" * 64,
                source_policy_fingerprint="b" * 64,
                retrieval_target=5,
                created_at=moment(),
            )
            (form_id,) = insert_query_forms(
                conn,
                plan_id=plan_id,
                forms=(
                    {
                        "ordinal": 1,
                        "stage": 1,
                        "variant_kind": "exact",
                        "query_text": "Test Person",
                    },
                ),
            )
            work_id = conn.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    created_at, updated_at
                ) VALUES (?, 'coverage_query_form', ?, ?, 1, 60, ?, 'pending',
                          ?, ?, ?)
                """,
                (
                    BRAVE_WEB_SEARCH_TASK_TYPE,
                    form_id,
                    "c" * 64,
                    moment(),
                    int(run_id),
                    moment(),
                    moment(),
                ),
            ).lastrowid
    finally:
        connection.close()

    # Positive control: the plan and its Brave work are active/pending before
    # the run that is meant to supersede them. The first run's own
    # no-match-then-coverage plan is also present now (K5 fires in-run), and
    # already terminalized to "completed"; only the freshly inserted plan is
    # still active.
    before = _plan_rows(config, person_id=person_id)
    assert before[-1] == (plan_id, "retrieving")
    assert all(status == "completed" for _, status in before[:-1])
    connection = connect_database(database, readonly=True)
    try:
        row = connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()
        assert row is not None
        assert row["state"] == "pending"
    finally:
        connection.close()

    _force_matching_wikipedia_page(config, person_id=person_id)

    brave_calls: list[httpx.Request] = []
    _wire(monkeypatch, rss=None, brave_calls=brave_calls)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    after = _plan_rows(config, person_id=person_id)
    # The run's own earlier plan stays "completed" (already terminal); only
    # the manually inserted active plan is superseded — no new plan opens.
    assert after[-1] == (plan_id, "superseded")
    assert after[:-1] == before[:-1]
    connection = connect_database(database, readonly=True)
    try:
        row = connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()
        assert row is not None
        assert row["state"] == "superseded"
    finally:
        connection.close()
    assert not brave_calls, "matching Wikipedia page must not trigger a Brave call"


def test_second_run_unchanged_material_opens_no_second_plan_or_brave_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later run over unchanged material must not duplicate plan or call."""
    config = write_people_graph(tmp_path, feeds=_single_feed())
    # With the K5 same-run hook now actually firing, this very first run's
    # top-of-run seed batch is the one that opens and drives the plan to a
    # terminal state: the person's Wikipedia no-match settles this run, which
    # makes it coverage-eligible in the same run.
    brave_calls_open: list[httpx.Request] = []
    _wire(monkeypatch, rss=RESEARCH_FEED.encode(), brave_calls=brave_calls_open)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    person_id = _sole_person_id(config)
    first_plans = _plan_rows(config, person_id=person_id)
    assert len(first_plans) == 1
    # Positive control: opening the plan really did call Brave at least once,
    # so the "no second call" assertion below is not vacuously true.
    assert brave_calls_open

    brave_calls_second: list[httpx.Request] = []
    _wire(
        monkeypatch,
        rss=None,
        brave_calls=brave_calls_second,
        llm=ScriptedOpenRouterClient(),
    )
    cli_main.command_run(config, verbose=False)

    second_plans = _plan_rows(config, person_id=person_id)
    assert second_plans == first_plans
    assert not brave_calls_second, (
        "unchanged material must not trigger a second Brave call"
    )


# ---------------------------------------------------------------------------
# Rule 6: digest section
# ---------------------------------------------------------------------------


def test_command_run_writes_digest_with_coverage_evidence_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    digest = _latest_digest(config)
    assert "### Coverage evidence" in digest


# ---------------------------------------------------------------------------
# Rule 7: secret redaction with a positive (reachability) control
# ---------------------------------------------------------------------------


def test_brave_secret_never_reaches_digest_or_output_but_is_in_the_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    secret = ENVIRONMENT["TEST_BRAVE"]
    seen_tokens: list[str | None] = []

    def make_handler(*, rss: bytes | None) -> Callable[[httpx.Request], httpx.Response]:
        def handler(request: httpx.Request) -> httpx.Response:
            if _is_mediawiki_request(request):
                return httpx.Response(
                    200,
                    content=streaming_body(EMPTY_MEDIAWIKI_SEARCH),
                    headers={"content-type": "application/json"},
                )
            if _is_brave_request(request):
                seen_tokens.append(request.headers.get("X-Subscription-Token"))
                return httpx.Response(
                    200,
                    content=streaming_body(ZERO_RESULTS_BRAVE),
                    headers={"content-type": "application/json"},
                )
            if rss is None:
                return httpx.Response(304, content=streaming_body(b""))
            return httpx.Response(200, content=streaming_body(rss))

        return handler

    monkeypatch.setenv("TEST_OPENROUTER", ENVIRONMENT["TEST_OPENROUTER"])
    monkeypatch.setenv("TEST_BRAVE", secret)
    monkeypatch.setattr(
        cli_main,
        "build_transport",
        _transport_patch(make_handler(rss=RESEARCH_FEED.encode())),
    )
    client = ScriptedOpenRouterClient(
        generate_contents=(RESEARCH_JSON,),
        content_by_substring={"assess_article": ASSESS_JSON},
    )
    monkeypatch.setattr(cli_main, "OpenRouterClient", client.factory)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    # With the K5 same-run hook now actually firing, the person's Wikipedia
    # no-match this run already makes coverage eligible and opens/advances a
    # plan in the same run, so Brave is already called during this first run.
    # A second run over unchanged material is still taken to prove no repeat
    # Brave call happens once the plan has terminalized.
    monkeypatch.setattr(
        cli_main, "build_transport", _transport_patch(make_handler(rss=None))
    )
    monkeypatch.setattr(
        cli_main, "OpenRouterClient", ScriptedOpenRouterClient().factory
    )
    cli_main.command_run(config, verbose=False)

    # Positive control: the secret really is reachable in the outgoing
    # request path, so its absence below is not vacuous.
    assert seen_tokens, "Brave must have been called at least once"
    assert secret in seen_tokens

    digest = _latest_digest(config)
    assert secret not in digest
    output = capsys.readouterr().out
    assert secret not in output
    log_path = config.parent / "portable" / "data" / "logs" / "notable.log"
    if log_path.exists():
        assert secret not in log_path.read_text(encoding="utf-8")
