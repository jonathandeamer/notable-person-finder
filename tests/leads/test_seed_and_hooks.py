"""Cross-module scheduling-hook seams for lead aggregation.

Verifies that:
1. Coverage plan completion schedules an aggregate_person_lead work item.
2. Wikipedia matching_page_found observation schedules an
   aggregate_person_lead work item.
3. The top-of-run seed sweep catches a person whose settlement hook was
   missed by a crash window.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Generator
from pathlib import Path

import pytest

import notable_person_finder
from notable_person_finder.config.models import (
    AssessArticleConfig,
    BraveConfig,
    BudgetConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    MediaWikiConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage.repository import (
    insert_query_forms,
    mark_query_form_completed,
    open_plan,
)
from notable_person_finder.coverage.service import advance_coverage_plan_after_assess
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.service import seed_lead_aggregation
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from notable_person_finder.wikipedia.repository import (
    open_plan as open_wiki_plan,
)
from notable_person_finder.wikipedia.service import (
    _schedule_coverage_after_wikipedia_settled,
)
from tests.ingestion.helpers import immediate, insert_run, moment
from tests.leads.test_service import _seed_person_and_articles

NOW = moment()
_PROMPT = "p" * 64
_SCHEMA = "s" * 64

_REPO_ROOT = Path(notable_person_finder.__file__).resolve().parents[2]
_VISUAL_ARTS_POLICY = _REPO_ROOT / "config" / "source_policies" / "visual_arts.toml"


@pytest.fixture
def connection(tmp_path: Path) -> Generator[sqlite3.Connection]:
    db_path = tmp_path / "test.db"
    conn = connect_database(db_path)
    apply_migrations(conn, db_path, tmp_path / "backups")
    yield conn
    conn.close()


def _config(*, source_policy_file: Path = _VISUAL_ARTS_POLICY) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=source_policy_file,
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        brave=BraveConfig(),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            assess_article=AssessArticleConfig(
                model="openai/gpt-test",
                max_alias_forms=0,
                max_context_forms=0,
            ),
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model="openai/gpt-match",
                refresh_interval_hours=24,
            ),
        ),
    )


def _person(
    connection: sqlite3.Connection, *, run_id: int, name: str, fingerprint: str
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, ?, ?)
        """,
        (NOW, run_id, name, fingerprint),
    )
    assert cursor.lastrowid is not None
    person_id = int(cursor.lastrowid)
    search = mechanical_search_name(name)
    connection.execute(
        """
        INSERT INTO sourced_name (
            person_id, exact_name, search_name, match_key, kind, origin_kind,
            first_observed_at, last_observed_at
        ) VALUES (?, ?, ?, ?, 'professional', 'manual', ?, ?)
        """,
        (person_id, name, search, match_key(name), NOW, NOW),
    )
    connection.commit()
    return person_id


def test_coverage_plan_completion_schedules_aggregate_person_lead(
    connection: sqlite3.Connection,
) -> None:
    """When a coverage plan reaches 'completed', an aggregate_person_lead work item
    is enqueued in the same run (K5 pattern)."""
    config = _config()
    run_id = insert_run(connection)
    person_id = _person(
        connection, run_id=run_id, name="Test Subject", fingerprint="a" * 64
    )

    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint="m" * 64,
            source_policy_fingerprint="p" * 64,
            retrieval_target=5,
            created_at=NOW,
        )
        (form_id,) = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": "Test Subject",
                },
            ),
        )
        mark_query_form_completed(
            conn, form_id=form_id, offsets_used=1, result_count=1, truncated=False
        )

    # Trigger plan terminalization / completion
    with immediate(connection):
        advance_coverage_plan_after_assess(
            connection, plan_id=plan_id, now=NOW, config=config
        )

    # Verify plan reached completed
    plan_row = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert plan_row is not None and plan_row["status"] == "completed"

    # Verify aggregate_person_lead work item was scheduled
    work_items = connection.execute(
        "SELECT task_type, subject_kind, subject_id FROM work_item "
        "WHERE task_type = 'aggregate_person_lead'"
    ).fetchall()
    assert len(work_items) == 1
    assert work_items[0]["subject_kind"] == "person"
    assert work_items[0]["subject_id"] == person_id


def test_seed_lead_aggregation_catches_missed_settlement_hook(
    connection: sqlite3.Connection,
) -> None:
    """A person with completed coverage evidence but no scheduling hook call
    (simulating a crash between settlement and hook-firing) must be swept at
    the top of the next run, same at-least-once posture as every other
    milestone."""
    config = _config()
    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    seed_lead_aggregation(connection, run_id=run_id, config=config, now=NOW)

    work_items = connection.execute(
        "SELECT task_type, subject_kind, subject_id FROM work_item "
        "WHERE task_type = 'aggregate_person_lead'"
    ).fetchall()
    assert len(work_items) == 1
    assert work_items[0]["subject_kind"] == "person"
    assert work_items[0]["subject_id"] == person_id


def test_seed_lead_aggregation_is_idempotent_via_fingerprint_dedup(
    connection: sqlite3.Connection,
) -> None:
    """Sweeping twice in a row (e.g. two runs before the item is claimed)
    must not create a second active work item for the same evidence."""
    config = _config()
    run_id = insert_run(connection)
    _person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    seed_lead_aggregation(connection, run_id=run_id, config=config, now=NOW)
    seed_lead_aggregation(connection, run_id=run_id, config=config, now=NOW)

    work_items = connection.execute(
        "SELECT id FROM work_item WHERE task_type = 'aggregate_person_lead'"
    ).fetchall()
    assert len(work_items) == 1


def _complete_wikipedia(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    semantic_outcome: str,
    task_fingerprint: str,
    page_id: int,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_wiki_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=task_fingerprint,
            created_at=NOW,
        )
        conn.execute(
            "UPDATE wikipedia_identity_plan SET status = 'completed', "
            "completed_at = ? WHERE id = ?",
            (NOW, plan_id),
        )
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id,
                created_at, updated_at
            ) VALUES ('match_wikipedia_identity', 'person', ?, ?, 1, 55, ?,
                      'succeeded', ?, ?, ?)
            """,
            (person_id, f"w{page_id:03d}".ljust(64, "0"), NOW, run_id, NOW, NOW),
        ).lastrowid
        attempt_id = conn.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, request_fingerprint
            ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?,
                      'succeeded', ?)
            """,
            (run_id, work_id, NOW, NOW, f"r{page_id:03d}".ljust(64, "0")),
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
            (run_id, attempt_id, f"i{page_id:03d}".ljust(64, "0"), NOW),
        ).lastrowid
        matched_row = upsert_mediawiki_page(
            conn,
            wiki_id="enwiki",
            page_id=page_id,
            canonical_title=f"Page {page_id}",
            canonical_url=f"https://en.wikipedia.org/wiki/P{page_id}",
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
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome=semantic_outcome,
            matched_mediawiki_page_id=matched_row,
            candidate_page_ids_json=f"[{page_id}]",
            canonical_supplied_input_json='{"task":"wikipedia_identity"}',
            validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint=task_fingerprint,
            rationale="test complete",
            failure_category=None,
            observed_at=NOW,
        )
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )
    return int(obs_id)


def test_wikipedia_matching_page_found_schedules_aggregate_person_lead(
    connection: sqlite3.Connection,
) -> None:
    """A matching_page_found Wikipedia observation schedules aggregate_person_lead."""
    config = _config()
    run_id = insert_run(connection)
    person_id = _person(
        connection, run_id=run_id, name="Wiki Match Subject", fingerprint="b" * 64
    )

    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="matching_page_found",
        task_fingerprint="t" * 64,
        page_id=999,
    )

    # Call wikipedia settlement hook
    with immediate(connection):
        _schedule_coverage_after_wikipedia_settled(
            connection, person_id=person_id, run_id=run_id, config=config, now=NOW
        )

    # Verify aggregate_person_lead work item was scheduled for matching_page_found
    work_items = connection.execute(
        "SELECT task_type, subject_kind, subject_id FROM work_item "
        "WHERE task_type = 'aggregate_person_lead'"
    ).fetchall()
    assert len(work_items) == 1
    assert work_items[0]["subject_kind"] == "person"
    assert work_items[0]["subject_id"] == person_id
