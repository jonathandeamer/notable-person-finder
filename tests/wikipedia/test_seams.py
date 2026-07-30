"""Cross-component seams for Wikipedia CLI wire and digest reporting."""

from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.db.connection import connect_database
from notable_person_finder.wikipedia.service import (
    MEDIAWIKI_SEARCH_TASK_TYPE,
    seed_wikipedia_identity,
)
from tests.people.test_run_cli import (
    RESEARCH_FEED,
    RESEARCH_JSON,
    ScriptedOpenRouterClient,
    _single_feed,
    write_people_graph,
)
from tests.wikipedia.test_run_cli import _latest_digest, _wire


def test_seed_order_includes_wikipedia_before_inspections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Kills omitting seed_wikipedia_identity for pre-existing corpus people.

    Person-ready hooks cover in-run creates; this path backfills people that
    already exist before the run starts (seed composition after unresolved
    mentions, before inspections).
    """
    from notable_person_finder.config.loader import load_config
    from notable_person_finder.db.migrate import apply_migrations
    from notable_person_finder.people.identity import insert_person, upsert_sourced_name
    from notable_person_finder.runs.clock import SystemClock, utc_timestamp
    from tests.ingestion.helpers import immediate, insert_run
    from tests.people.test_run_cli import ZERO_MENTIONS_JSON

    config = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    try:
        apply_migrations(connection, database, loaded.paths.backups)
        now = utc_timestamp(SystemClock().now())
        run_id = insert_run(connection)
        with immediate(connection):
            person_id = insert_person(
                connection,
                run_id=int(run_id),
                display_name="Pre Existing",
                identity_fingerprint="9" * 64,
                created_at=now,
            )
            upsert_sourced_name(
                connection,
                person_id=person_id,
                exact_name="Pre Existing",
                kind="professional",
                origin_kind="manual",
                origin_mention_id=None,
                observed_at=now,
            )
        connection.commit()
    finally:
        connection.close()

    # Feed has no research people; only seed can open Wikipedia for pre-existing.
    _wire(
        monkeypatch,
        llm=ScriptedOpenRouterClient(generate_contents=(ZERO_MENTIONS_JSON,)),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    connection = connect_database(database, readonly=True)
    try:
        plans = connection.execute(
            """
            SELECT COUNT(*) AS n FROM wikipedia_identity_plan
             WHERE person_id = (
                 SELECT id FROM person WHERE display_name = 'Pre Existing'
             )
            """
        ).fetchone()
        assert plans is not None
        assert int(plans["n"]) >= 1
        n = connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE task_type = ?",
            (MEDIAWIKI_SEARCH_TASK_TYPE,),
        ).fetchone()
        assert n is not None
        assert int(n["n"]) >= 1
    finally:
        connection.close()


def test_digest_reports_wikipedia_after_person_create(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(
        monkeypatch,
        payload=RESEARCH_FEED.encode(),
        llm=ScriptedOpenRouterClient(generate_contents=(RESEARCH_JSON,)),
    )
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK
    digest = _latest_digest(config)
    assert "### Person identity" in digest
    assert "### Wikipedia identity" in digest
    # Person created then settled via empty MediaWiki search.
    assert "People created this run: 1" in digest
    assert "Deterministic no-match (empty complete search): 1" in digest


def test_seed_wikipedia_is_idempotent_for_settled_people(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch)
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    from notable_person_finder.config.loader import load_config
    from notable_person_finder.runs.clock import SystemClock, utc_timestamp

    loaded = load_config(config, require_secrets=False)
    database = config.parent / "portable" / "data" / "notable.sqlite3"
    connection = connect_database(database)
    try:
        run_id = int(
            connection.execute("SELECT MAX(id) AS id FROM run").fetchone()["id"]
        )
        acted = seed_wikipedia_identity(
            connection,
            run_id=run_id,
            config=loaded.main,
            now=utc_timestamp(SystemClock().now()),
        )
        assert acted == 0
    finally:
        connection.close()
