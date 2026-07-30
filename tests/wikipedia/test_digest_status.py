"""Digest and status counter alignment for Wikipedia identity (K11/K17)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from notable_person_finder.config.models import (
    BudgetConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    MediaWikiConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.reporting.digest import WikipediaRunSummary, render_digest
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState
from notable_person_finder.wikipedia.matching import base_material_fingerprint
from notable_person_finder.wikipedia.models import WikipediaPersonMaterialView
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    open_plan,
    point_person_current_wikipedia_observation,
    wikipedia_corpus_counts,
    wikipedia_run_counts,
)
from notable_person_finder.wikipedia.service import (
    count_wikipedia_match_eligible,
    is_wikipedia_match_eligible,
)
from tests.ingestion.helpers import immediate, insert_run, moment
from tests.run_engine.test_digest import report

NOW = moment()
_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64


def _config(*, refresh_hours: int = 720) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model="openai/gpt-match",
                refresh_interval_hours=refresh_hours,
            )
        ),
    )


def _person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    name: str,
    fingerprint: str,
) -> int:
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=fingerprint,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name=name,
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    return person_id


def _deterministic_no_match(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    fingerprint: str,
    point_current: bool,
    observed_at: str = NOW,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=fingerprint,
            created_at=observed_at,
        )
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="no_matching_page_found",
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json='{"task":"wikipedia_identity"}',
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=fingerprint,
            rationale="test empty complete",
            failure_category=None,
            observed_at=observed_at,
        )
        conn.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'completed', completed_at = ?
             WHERE id = ?
            """,
            (observed_at, plan_id),
        )
        if point_current:
            point_person_current_wikipedia_observation(
                conn, person_id=person_id, observation_id=obs_id
            )
    return obs_id


def test_wikipedia_run_and_corpus_counts_align_with_rows(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    config = _config()

    eligible_person = _person(
        connection, run_id=run_id, name="Eligible One", fingerprint=_HASH_A
    )
    no_match_person = _person(
        connection, run_id=run_id, name="No Match", fingerprint=_HASH_B
    )
    material_b = base_material_fingerprint(
        WikipediaPersonMaterialView(
            person_id=no_match_person, identity_fingerprint=_HASH_B
        ),
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=None,
    )
    _deterministic_no_match(
        connection,
        person_id=no_match_person,
        run_id=run_id,
        fingerprint=material_b,
        point_current=True,
    )

    corpus = wikipedia_corpus_counts(connection)
    assert corpus.current_matching == 0
    assert corpus.current_no_match == 1
    assert corpus.current_uncertain == 0
    assert corpus.without_pointer == 1  # eligible_person only

    run_counts = wikipedia_run_counts(connection, run_id=run_id)
    assert run_counts.no_match_this_run == 1
    assert run_counts.deterministic_no_match_this_run == 1
    assert run_counts.matching_this_run == 0
    assert run_counts.uncertain_this_run == 0
    assert run_counts.mediawiki_deferred == 0
    assert run_counts.match_model_failed == 0

    eligible = count_wikipedia_match_eligible(connection, config=config, now=NOW)
    assert eligible == 1
    assert is_wikipedia_match_eligible(
        connection, person_id=eligible_person, config=config, now=NOW
    )
    assert not is_wikipedia_match_eligible(
        connection, person_id=no_match_person, config=config, now=NOW
    )


def test_without_pointer_excludes_merged_away(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Survivor", fingerprint=_HASH_C)
    loser = _person(connection, run_id=run_id, name="Loser", fingerprint=_HASH_D)
    with immediate(connection):
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, loser),
        )
    corpus = wikipedia_corpus_counts(connection)
    assert corpus.without_pointer == 1


def test_eligible_remaining_counts_refresh_due_with_current_pointer(
    connection: sqlite3.Connection,
) -> None:
    """Eligible remaining includes refresh-due people who already have current obs."""
    run_id = insert_run(connection)
    config = _config(refresh_hours=1)
    person_id = _person(
        connection, run_id=run_id, name="Refresh Me", fingerprint=_HASH_A
    )
    material = base_material_fingerprint(
        WikipediaPersonMaterialView(person_id=person_id, identity_fingerprint=_HASH_A),
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=None,
    )
    _deterministic_no_match(
        connection,
        person_id=person_id,
        run_id=run_id,
        fingerprint=material,
        point_current=True,
        observed_at=NOW,
    )

    later = utc_timestamp(
        datetime.fromisoformat(NOW.replace("Z", "+00:00")) + timedelta(hours=2)
    )
    assert count_wikipedia_match_eligible(connection, config=config, now=later) == 1
    assert wikipedia_corpus_counts(connection).current_no_match == 1
    assert wikipedia_corpus_counts(connection).without_pointer == 0


def test_digest_wikipedia_lines_and_no_coverage_skipped() -> None:
    wikipedia = WikipediaRunSummary(
        matching_this_run=1,
        no_match_this_run=2,
        uncertain_this_run=0,
        deterministic_no_match_this_run=2,
        current_matching=1,
        current_no_match=2,
        current_uncertain=0,
        eligible_remaining=3,
        without_pointer=4,
        mediawiki_deferred=0,
        mediawiki_failed=1,
        match_model_deferred=0,
        match_model_failed=0,
    )
    markdown = render_digest(
        RunReport(
            run_id=1,
            state=RunState.COMPLETE,
            started_at="2026-07-30T00:00:00Z",
            finished_at="2026-07-30T00:01:00Z",
            timezone="UTC",
            window_start="2026-07-30T00:00:00Z",
            window_end="2026-07-31T00:00:00Z",
            counters=RunCounters(
                required_succeeded=1,
                required_pending=0,
                required_deferred=0,
                required_failed_permanent=0,
                optional_succeeded=0,
                optional_skipped=0,
                operational_failures=0,
            ),
            paused_providers=frozenset(),
            interrupted_runs=(),
            failure_categories={},
            budget_limit_nano_usd=None,
            budget_reserved_nano_usd=0,
            budget_actual_nano_usd=0,
            deferred_reasons={},
        ),
        local_date="2026-07-30",
        wikipedia=wikipedia,
    )
    assert "### Wikipedia identity" in markdown
    section = markdown.split("### Wikipedia identity\n\n", 1)[1]
    assert "- Matching page found (this run): 1" in section
    assert "- No matching page (this run): 2" in section
    assert "- Deterministic no-match (empty complete search): 2" in section
    assert "- Wikipedia eligible remaining: 3" in section
    assert "- Canonical people without Wikipedia pointer: 4" in section
    assert "- MediaWiki work permanently failed: 1" in section
    # Positive control: forbidden wording is detectable when injected (K11).
    forbidden = "coverage skipped because wikipedia match"
    polluted = section + f"- {forbidden}: 9\n"
    assert forbidden in polluted.lower()
    assert forbidden not in section.lower()
    assert "coverage" not in section.lower()


def test_render_digest_helper_matches_report_fixture() -> None:
    markdown = render_digest(report(), local_date="2026-07-30")
    assert "### Wikipedia identity" not in markdown
