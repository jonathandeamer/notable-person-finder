"""Seed, person-ready hooks, and mandatory call sites for Wikipedia identity."""

from __future__ import annotations

import sqlite3
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
    TasksConfig,
)
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.people.models import ResolvePersonEntityOutput
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.people.service import (
    _persist_resolution_for,
    _ResolutionPersist,
    ensure_resolution_for_mention,
    seed_unresolved_mentions,
)
from notable_person_finder.runs.engine import TaskOutcome
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.wikipedia.service import (
    MEDIAWIKI_SEARCH_TASK_TYPE,
    ensure_wikipedia_identity,
    schedule_wikipedia_after_person_ready,
    seed_wikipedia_identity,
)
from tests.ingestion.helpers import immediate, insert_run, moment

MODEL = "openai/gpt-test"
MATCH_MODEL = "openai/gpt-match"
NOW = moment()
_HASH = "a" * 64
_PROMPT = "p" * 64
_SCHEMA = "s" * 64


def _main_config() -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(model=MODEL),
            resolve_person_entity=ResolvePersonEntityConfig(model=MODEL),
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model=MATCH_MODEL,
                refresh_interval_hours=24,
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


def _seed_mention_graph(
    connection: sqlite3.Connection,
    *,
    exact_name: str = "Alex Smith",
    outcome: str = "research",
    material_fingerprint: str = _HASH,
    feed_key: str = "feed-seed",
    source_entry_id: str = "entry-1",
) -> tuple[int, int, int]:
    """Return (run_id, inspection_id, mention_id)."""
    run_id = insert_run(connection)
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (material_fingerprint, NOW, run_id, NOW, NOW),
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
        (run_id, work, NOW, moment(1), material_fingerprint),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed', 'https://example.com/feed', ?, ?)
        """,
        (feed_key, NOW, NOW),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, NOW),
    ).lastrowid
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'Title', 'Summary text.', ?)
        """,
        (feed, fetch, run_id, source_entry_id, NOW),
    ).lastrowid
    assert source_item_id is not None
    inspection_id = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, MODEL, MODEL, material_fingerprint, NOW),
    ).lastrowid
    assert inspection_id is not None
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
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT,
            _SCHEMA,
            material_fingerprint,
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    search = mechanical_search_name(exact_name)
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, ?, '["p1"]', 'reason')
        """,
        (observation_id, exact_name, search, outcome),
    ).lastrowid
    assert mention_id is not None
    connection.commit()
    return run_id, inspection_id, mention_id


def _wiki_search_work_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'deferred', 'running')
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE,),
    ).fetchone()
    assert row is not None
    return int(row["n"])


def _active_plan_count(connection: sqlite3.Connection, *, person_id: int) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM wikipedia_identity_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'ready_for_match')
        """,
        (person_id,),
    ).fetchone()
    assert row is not None
    return int(row["n"])


def test_created_new_hook_schedules_wikipedia(connection: sqlite3.Connection) -> None:
    config = _main_config()
    profile = _profile()
    run_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert result == "created_new"
    person_row = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert person_row is not None and person_row["person_id"] is not None
    assert _active_plan_count(connection, person_id=int(person_row["person_id"])) == 1
    assert _wiki_search_work_count(connection) >= 1


def test_seed_wikipedia_backfill(connection: sqlite3.Connection) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Blake Jones",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Blake Jones",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    acted = seed_wikipedia_identity(connection, run_id=run_id, config=config, now=NOW)
    assert acted == 1
    assert _active_plan_count(connection, person_id=person_id) == 1
    acted2 = seed_wikipedia_identity(connection, run_id=run_id, config=config, now=NOW)
    assert acted2 == 1
    assert _active_plan_count(connection, person_id=person_id) == 1


def test_fingerprint_unchanged_ensure_is_noop(connection: sqlite3.Connection) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Casey Lee",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Casey Lee",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    first = ensure_wikipedia_identity(
        connection, person_id=person_id, run_id=run_id, config=config, now=NOW
    )
    assert first == "scheduled"
    plans_before = connection.execute(
        "SELECT COUNT(*) AS n FROM wikipedia_identity_plan WHERE person_id = ?",
        (person_id,),
    ).fetchone()
    assert plans_before is not None
    second = ensure_wikipedia_identity(
        connection, person_id=person_id, run_id=run_id, config=config, now=NOW
    )
    assert second == "reused"
    plans_after = connection.execute(
        "SELECT COUNT(*) AS n FROM wikipedia_identity_plan WHERE person_id = ?",
        (person_id,),
    ).fetchone()
    assert plans_after is not None
    assert int(plans_after["n"]) == int(plans_before["n"])


def test_do_not_research_without_person_does_not_schedule(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    profile = _profile()
    run_id, _inspection_id, mention_id = _seed_mention_graph(
        connection, exact_name="Nobody Famous", outcome="do_not_research"
    )
    result = ensure_resolution_for_mention(
        connection,
        person_mention_id=mention_id,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    assert result == "ineligible"
    assert _wiki_search_work_count(connection) == 0
    plans = connection.execute(
        "SELECT COUNT(*) AS n FROM wikipedia_identity_plan"
    ).fetchone()
    assert plans is not None
    assert int(plans["n"]) == 0


def _persist_model_resolution(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    run_id: int,
    inspection_id: int,
    mention_id: int,
    output: ResolvePersonEntityOutput,
    candidate_person_ids: tuple[int, ...],
    fingerprint: str = "f" * 64,
) -> None:
    """Drive first-pass model persist (not empty-candidate ensure)."""
    work_id = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES (
            'resolve_person_entity', 'person_mention', ?, ?, 1, 40, ?,
            'running', ?, ?, ?
        )
        """,
        (mention_id, fingerprint, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert work_id is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_id, NOW, moment(1), fingerprint),
    ).lastrowid
    assert attempt_id is not None
    connection.commit()

    payload = _ResolutionPersist(
        output=output,
        person_mention_id=mention_id,
        model_inspection_id=inspection_id,
        canonical_supplied_input_json="{}",
        prompt_hash=_PROMPT,
        schema_hash=_SCHEMA,
        schema_version=1,
        task_fingerprint=fingerprint,
        candidate_person_ids=candidate_person_ids,
    )
    work_item = WorkItem(
        id=work_id,
        task_type="resolve_person_entity",
        subject_kind="person_mention",
        subject_id=mention_id,
        fingerprint=fingerprint,
        required=True,
        priority=40,
        state=WorkState.RUNNING,
    )
    persist = _persist_resolution_for(connection, config=config)
    with immediate(connection):
        persist(
            work_item,
            TaskOutcome(state=WorkState.SUCCEEDED, reason=None, payload=payload),
        )


def test_same_person_link_hook_schedules_wikipedia(
    connection: sqlite3.Connection,
) -> None:
    """First-pass same_person path schedules Wikipedia after link + fingerprint."""
    config = _main_config()
    run_id, inspection_id, mention_id = _seed_mention_graph(
        connection, exact_name="Dana Park"
    )
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Dana Park",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Dana Park",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )

    _persist_model_resolution(
        connection,
        config=config,
        run_id=run_id,
        inspection_id=inspection_id,
        mention_id=mention_id,
        output=ResolvePersonEntityOutput(
            outcome="same_person",
            selected_person_id=person_id,
            supporting_fact_ids=(),
            conflicting_fact_ids=(),
            rationale="same individual",
        ),
        candidate_person_ids=(person_id,),
    )

    linked = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert linked is not None
    assert int(linked["person_id"]) == person_id
    assert _active_plan_count(connection, person_id=person_id) == 1
    assert _wiki_search_work_count(connection) >= 1


def test_different_people_create_hook_schedules_wikipedia(
    connection: sqlite3.Connection,
) -> None:
    """Model-path different_people create (not empty ensure) arms Wikipedia."""
    config = _main_config()
    run_id, inspection_id, mention_id = _seed_mention_graph(
        connection,
        exact_name="Gina Vale",
        feed_key="feed-dp",
        source_entry_id="entry-dp",
        material_fingerprint="d" * 64,
    )
    with immediate(connection):
        candidate_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Gina Other",
            identity_fingerprint="e" * 64,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=candidate_id,
            exact_name="Gina Other",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )

    _persist_model_resolution(
        connection,
        config=config,
        run_id=run_id,
        inspection_id=inspection_id,
        mention_id=mention_id,
        output=ResolvePersonEntityOutput(
            outcome="different_people",
            selected_person_id=None,
            supporting_fact_ids=(),
            conflicting_fact_ids=(),
            rationale="distinct individuals",
        ),
        candidate_person_ids=(candidate_id,),
        fingerprint="1" * 64,
    )

    person_row = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert person_row is not None and person_row["person_id"] is not None
    created_id = int(person_row["person_id"])
    assert created_id != candidate_id
    assert _active_plan_count(connection, person_id=created_id) == 1
    assert _wiki_search_work_count(connection) >= 1


def test_uncertain_create_hook_schedules_wikipedia(
    connection: sqlite3.Connection,
) -> None:
    """Model-path uncertain create (not empty ensure) arms Wikipedia."""
    config = _main_config()
    run_id, inspection_id, mention_id = _seed_mention_graph(
        connection,
        exact_name="Holly Quinn",
        feed_key="feed-unc",
        source_entry_id="entry-unc",
        material_fingerprint="u" * 64,
    )
    with immediate(connection):
        candidate_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Holly Peer",
            identity_fingerprint="v" * 64,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=candidate_id,
            exact_name="Holly Peer",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )

    _persist_model_resolution(
        connection,
        config=config,
        run_id=run_id,
        inspection_id=inspection_id,
        mention_id=mention_id,
        output=ResolvePersonEntityOutput(
            outcome="uncertain",
            selected_person_id=None,
            supporting_fact_ids=(),
            conflicting_fact_ids=(),
            rationale="insufficient evidence",
        ),
        candidate_person_ids=(candidate_id,),
        fingerprint="2" * 64,
    )

    person_row = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert person_row is not None and person_row["person_id"] is not None
    created_id = int(person_row["person_id"])
    assert created_id != candidate_id
    assert _active_plan_count(connection, person_id=created_id) == 1
    assert _wiki_search_work_count(connection) >= 1


def test_schedule_wikipedia_after_person_ready_direct(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Evan Wu",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Evan Wu",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
        schedule_wikipedia_after_person_ready(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
    assert _active_plan_count(connection, person_id=person_id) == 1


def test_seed_unresolved_then_seed_wikipedia_composition(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    profile = _profile()
    run_id, _inspection_id, _mention_id = _seed_mention_graph(
        connection, exact_name="Fran Ortiz"
    )
    seed_unresolved_mentions(
        connection,
        run_id=run_id,
        config=config,
        profile=profile,
        now=NOW,
    )
    acted = seed_wikipedia_identity(connection, run_id=run_id, config=config, now=NOW)
    assert acted >= 1
    person = connection.execute(
        "SELECT id FROM person WHERE display_name = 'Fran Ortiz'"
    ).fetchone()
    assert person is not None
    assert _active_plan_count(connection, person_id=int(person["id"])) == 1
