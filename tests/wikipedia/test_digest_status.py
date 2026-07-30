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
    upsert_mediawiki_page,
    wikipedia_corpus_counts,
    wikipedia_run_counts,
)
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
    MEDIAWIKI_SEARCH_TASK_TYPE,
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
_HASH_E = "e" * 64
_HASH_F = "f" * 64
_PROMPT_HASH = "1" * 64
_SCHEMA_HASH = "2" * 64


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


def _work_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    task_type: str,
    state: str,
    fingerprint: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, completed_by_run_id,
            created_at, updated_at
        )
        VALUES (?, 'person', 1, ?, 1, 50, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_type,
            fingerprint,
            NOW,
            state,
            run_id,
            run_id,
            NOW,
            NOW,
        ),
    )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _attempt_and_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    fingerprint: str,
) -> tuple[int, int]:
    work_id = _work_item(
        connection,
        run_id=run_id,
        task_type=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
        state="succeeded",
        fingerprint=fingerprint,
    )
    attempt = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_id, NOW, NOW, fingerprint),
    )
    assert attempt.lastrowid is not None
    attempt_id = int(attempt.lastrowid)
    inspection = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, attempt_id, fingerprint, NOW),
    )
    assert inspection.lastrowid is not None
    return attempt_id, int(inspection.lastrowid)


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


def _model_path_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    fingerprint: str,
    semantic_outcome: str,
    point_current: bool,
    provider_page_id: int,
) -> int:
    """Insert a completed model-path observation (matching / no-match / uncertain)."""
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=fingerprint,
            created_at=NOW,
        )
        attempt_id, inspection_id = _attempt_and_inspection(
            conn, run_id=run_id, fingerprint=fingerprint
        )
        matched_page_row_id: int | None = None
        candidates = f"[{provider_page_id}]"
        if semantic_outcome == "matching_page_found":
            matched_page_row_id = upsert_mediawiki_page(
                conn,
                wiki_id="enwiki",
                page_id=provider_page_id,
                canonical_title=f"Page {provider_page_id}",
                canonical_url=f"https://en.wikipedia.org/wiki/P{provider_page_id}",
                namespace=0,
                is_disambiguation=False,
                is_missing=False,
                redirect_to_page_id=None,
                description=None,
                extract="bio",
                categories_json="[]",
                last_observed_at=NOW,
                last_attempt_id=None,
            )
        elif semantic_outcome == "no_matching_page_found":
            # Model no-match requires non-empty candidates and non-null hashes.
            pass
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome=semantic_outcome,
            matched_mediawiki_page_id=matched_page_row_id,
            candidate_page_ids_json=candidates,
            canonical_supplied_input_json='{"task":"wikipedia_identity"}',
            validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=fingerprint,
            rationale="model path",
            failure_category=None,
            observed_at=NOW,
        )
        conn.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'completed', completed_at = ?
             WHERE id = ?
            """,
            (NOW, plan_id),
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


def test_corpus_and_run_counters_positive_for_each_outcome_and_work_state(
    connection: sqlite3.Connection,
) -> None:
    """I2: non-zero SQL controls for matching/no-match/uncertain and work states.

    Proves counters are not stuck at zero by construction: each corpus outcome
    and each deferred/failed MediaWiki and match counter is exercised with a
    real row and a non-zero expected value.
    """
    run_id = insert_run(connection)
    config = _config()

    match_person = _person(
        connection, run_id=run_id, name="Match Person", fingerprint=_HASH_A
    )
    no_match_person = _person(
        connection, run_id=run_id, name="No Match Person", fingerprint=_HASH_B
    )
    uncertain_person = _person(
        connection, run_id=run_id, name="Uncertain Person", fingerprint=_HASH_C
    )
    # Extra without-pointer control (eligible, no current obs).
    _person(connection, run_id=run_id, name="Bare Person", fingerprint=_HASH_D)

    material_match = base_material_fingerprint(
        WikipediaPersonMaterialView(
            person_id=match_person, identity_fingerprint=_HASH_A
        ),
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=None,
    )
    material_no = base_material_fingerprint(
        WikipediaPersonMaterialView(
            person_id=no_match_person, identity_fingerprint=_HASH_B
        ),
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=None,
    )
    material_unc = base_material_fingerprint(
        WikipediaPersonMaterialView(
            person_id=uncertain_person, identity_fingerprint=_HASH_C
        ),
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=None,
    )

    _model_path_observation(
        connection,
        person_id=match_person,
        run_id=run_id,
        fingerprint=material_match,
        semantic_outcome="matching_page_found",
        point_current=True,
        provider_page_id=9001,
    )
    _deterministic_no_match(
        connection,
        person_id=no_match_person,
        run_id=run_id,
        fingerprint=material_no,
        point_current=True,
    )
    _model_path_observation(
        connection,
        person_id=uncertain_person,
        run_id=run_id,
        fingerprint=material_unc,
        semantic_outcome="uncertain_identity",
        point_current=True,
        provider_page_id=9002,
    )

    # Positive work-state controls (completed_by_run_id attribution).
    with immediate(connection):
        _work_item(
            connection,
            run_id=run_id,
            task_type=MEDIAWIKI_SEARCH_TASK_TYPE,
            state="deferred",
            fingerprint=_HASH_E,
        )
        _work_item(
            connection,
            run_id=run_id,
            task_type=MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
            state="failed_permanent",
            fingerprint=_HASH_F,
        )
        _work_item(
            connection,
            run_id=run_id,
            task_type=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
            state="deferred",
            fingerprint="3" * 64,
        )
        _work_item(
            connection,
            run_id=run_id,
            task_type=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
            state="failed_permanent",
            fingerprint="4" * 64,
        )

    corpus = wikipedia_corpus_counts(connection)
    assert corpus.current_matching == 1
    assert corpus.current_no_match == 1
    assert corpus.current_uncertain == 1
    assert corpus.without_pointer == 1

    run_counts = wikipedia_run_counts(connection, run_id=run_id)
    assert run_counts.matching_this_run == 1
    assert run_counts.no_match_this_run == 1
    assert run_counts.uncertain_this_run == 1
    assert run_counts.deterministic_no_match_this_run == 1
    assert run_counts.mediawiki_deferred == 1
    assert run_counts.mediawiki_failed == 1
    assert run_counts.match_model_deferred == 1
    assert run_counts.match_model_failed == 1

    # Positive control: a different run_id must not pick up these work rows.
    other_run = insert_run(connection)
    other = wikipedia_run_counts(connection, run_id=other_run)
    assert other.mediawiki_deferred == 0
    assert other.mediawiki_failed == 0
    assert other.match_model_deferred == 0
    assert other.match_model_failed == 0
    assert other.matching_this_run == 0


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
