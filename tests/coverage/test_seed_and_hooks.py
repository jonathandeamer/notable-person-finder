"""Seed, Wikipedia-ready hooks, supersede mid-flight, multi-refresh (K5/K19/K30)."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from notable_person_finder.config.models import (
    AssessArticleConfig,
    BraveConfig,
    BudgetConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage.assessment import (
    CoveragePersonMaterialView,
    coverage_material_fingerprint,
)
from notable_person_finder.coverage.repository import (
    insert_query_forms,
    open_plan,
    update_plan_status,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import (
    BRAVE_WEB_SEARCH_TASK_TYPE,
    ensure_coverage_research,
    is_coverage_research_eligible,
    schedule_coverage_after_wikipedia_ready,
    seed_coverage_research,
    supersede_coverage_work_for_person,
)
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from notable_person_finder.wikipedia.repository import (
    open_plan as open_wiki_plan,
)
from tests.ingestion.helpers import immediate, insert_run, moment

NOW = moment()
_HASH = "a" * 64
_PROMPT = "p" * 64
_SCHEMA = "s" * 64
REFRESH_HOURS = 24
MODEL = "openai/gpt-test"
_PAGE_COUNTER = 10_000


def _policy():
    return source_policy_from_mapping(
        {
            "schema_version": 1,
            "key": "test-sources",
            "label": "Test policy",
            "rules": [
                {
                    "id": "eligible.example.com",
                    "status": "curated_eligible",
                    "match": {"host_suffix": "example.com"},
                    "rationale": "test",
                    "review_date": "2026-07-24",
                }
            ],
        }
    )


def _config(*, refresh_hours: int = REFRESH_HOURS) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        brave=BraveConfig(),
        tasks=TasksConfig(
            assess_article=AssessArticleConfig(
                model=MODEL,
                coverage_refresh_interval_hours=refresh_hours,
                max_alias_forms=0,
                max_context_forms=0,
            )
        ),
    )


def _hours_after(base: str, hours: float) -> str:
    dt = datetime.fromisoformat(base.replace("Z", "+00:00"))
    return utc_timestamp(dt + timedelta(hours=hours))


def _person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    name: str = "Alex Smith",
    identity_fingerprint: str = _HASH,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, ?, ?)
        """,
        (NOW, run_id, name, identity_fingerprint),
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


def _attempt_and_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    fingerprint: str,
) -> tuple[int, int]:
    work_id = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES (
            'match_wikipedia_identity', 'person', 1, ?, 1, 55, ?,
            'succeeded', ?, ?, ?
        )
        """,
        (fingerprint, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert work_id is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_id, NOW, NOW, fingerprint),
    ).lastrowid
    assert attempt_id is not None
    inspection_id = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?, '[]', 1, 1, 1, 1, 'compatible', ?)
        """,
        (run_id, attempt_id, MODEL, MODEL, fingerprint, NOW),
    ).lastrowid
    assert inspection_id is not None
    return int(attempt_id), int(inspection_id)


def _complete_wikipedia(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    semantic_outcome: str,
    material_fingerprint: str = "w" * 64,
    observed_at: str = NOW,
) -> int:
    """Insert a valid completed Wikipedia observation and set the current pointer."""
    global _PAGE_COUNTER
    with immediate(connection) as conn:
        plan_id = open_wiki_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=observed_at,
        )
        conn.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'completed', completed_at = ?
             WHERE id = ?
            """,
            (observed_at, plan_id),
        )
        if semantic_outcome == "no_matching_page_found":
            # Deterministic empty-complete path (K4).
            obs_id = insert_wikipedia_identity_observation(
                conn,
                person_id=person_id,
                plan_id=plan_id,
                run_id=run_id,
                attempt_id=None,
                model_inspection_id=None,
                disposition="completed",
                semantic_outcome=semantic_outcome,
                matched_mediawiki_page_id=None,
                candidate_page_ids_json="[]",
                canonical_supplied_input_json='{"task":"wikipedia_identity"}',
                validated_output_json='{"outcome":"no_matching_page_found"}',
                prompt_hash=None,
                schema_hash=None,
                schema_version=None,
                task_fingerprint=material_fingerprint,
                rationale="test complete",
                failure_category=None,
                observed_at=observed_at,
            )
        else:
            attempt_id, inspection_id = _attempt_and_inspection(
                conn, run_id=run_id, fingerprint=material_fingerprint
            )
            _PAGE_COUNTER += 1
            provider_page_id = _PAGE_COUNTER
            matched_row: int | None = None
            if semantic_outcome == "matching_page_found":
                matched_row = upsert_mediawiki_page(
                    conn,
                    wiki_id="enwiki",
                    page_id=provider_page_id,
                    canonical_title=f"Page {provider_page_id}",
                    canonical_url=(
                        f"https://en.wikipedia.org/wiki/P{provider_page_id}"
                    ),
                    namespace=0,
                    is_disambiguation=False,
                    is_missing=False,
                    redirect_to_page_id=None,
                    description=None,
                    extract="bio",
                    categories_json="[]",
                    last_observed_at=observed_at,
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
                candidate_page_ids_json=f"[{provider_page_id}]",
                canonical_supplied_input_json='{"task":"wikipedia_identity"}',
                validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
                prompt_hash=_PROMPT,
                schema_hash=_SCHEMA,
                schema_version=1,
                task_fingerprint=material_fingerprint,
                rationale="model path",
                failure_category=None,
                observed_at=observed_at,
            )
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )
    return int(obs_id)


def _open_active_coverage_with_brave_work(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    policy_fp: str,
) -> tuple[int, int, int]:
    """Return (plan_id, form_id, brave_work_id)."""
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            source_policy_fingerprint=policy_fp,
            retrieval_target=5,
            created_at=NOW,
        )
        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": "Alex Smith",
                },
            ),
        )
        form_id = form_ids[0]
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'coverage_query_form', ?, ?, 1, 60, ?, 'pending', ?, ?, ?)
            """,
            (
                BRAVE_WEB_SEARCH_TASK_TYPE,
                form_id,
                "1" * 64,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert work_id is not None
    return plan_id, form_id, int(work_id)


def _complete_coverage_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    policy_fp: str,
    completed_at: str,
    refresh_of_plan_id: int | None = None,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            source_policy_fingerprint=policy_fp,
            retrieval_target=5,
            created_at=completed_at,
            refresh_of_plan_id=refresh_of_plan_id,
        )
        update_plan_status(
            conn,
            plan_id=plan_id,
            status="completed",
            completed_at=completed_at,
        )
    return plan_id


def _active_plan_count(connection: sqlite3.Connection, *, person_id: int) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
        """,
        (person_id,),
    ).fetchone()
    assert row is not None
    return int(row["n"])


def test_null_wikipedia_pointer_does_not_open_coverage(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    result = ensure_coverage_research(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=NOW,
    )
    assert result == "ineligible"
    assert _active_plan_count(connection, person_id=person_id) == 0
    schedule_coverage_after_wikipedia_ready(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=NOW,
    )
    assert _active_plan_count(connection, person_id=person_id) == 0


def test_matching_supersedes_mid_flight_brave_and_stops(
    connection: sqlite3.Connection,
) -> None:
    """K5: matching settle must supersede active Brave work (not eligibility alone)."""
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, _form_id, brave_work = _open_active_coverage_with_brave_work(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint="c" * 64,
        policy_fp=policy.fingerprint,
    )
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="matching_page_found",
    )

    schedule_coverage_after_wikipedia_ready(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=NOW,
    )

    plan = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?",
        (plan_id,),
    ).fetchone()
    assert plan is not None
    assert plan["status"] == "superseded"
    work = connection.execute(
        "SELECT state, reason FROM work_item WHERE id = ?",
        (brave_work,),
    ).fetchone()
    assert work is not None
    assert work["state"] == "superseded"
    assert _active_plan_count(connection, person_id=person_id) == 0
    # ensure alone would only skip; schedule must have cancelled mid-flight.
    assert (
        ensure_coverage_research(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )
        == "stopped_matching"
    )


def test_no_match_and_uncertain_ensure_open_plan(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    for outcome, name in (
        ("no_matching_page_found", "No Match"),
        ("uncertain_identity", "Uncertain One"),
    ):
        person_id = _person(
            connection,
            run_id=run_id,
            name=name,
            identity_fingerprint=match_key(name).ljust(64, "0")[:64],
        )
        _complete_wikipedia(
            connection,
            person_id=person_id,
            run_id=run_id,
            semantic_outcome=outcome,
            material_fingerprint=match_key(name + "w").ljust(64, "f")[:64],
        )
        result = schedule_coverage_after_wikipedia_ready(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )
        assert result is None
        assert _active_plan_count(connection, person_id=person_id) == 1
        assert (
            ensure_coverage_research(
                connection,
                person_id=person_id,
                run_id=run_id,
                config=config,
                policy=policy,
                now=NOW,
            )
            == "reused"
        )


def test_seed_matching_supersedes_eligible_ensures(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    eligible = _person(
        connection, run_id=run_id, name="Eligible Person", identity_fingerprint="1" * 64
    )
    matching = _person(
        connection, run_id=run_id, name="Matched Person", identity_fingerprint="2" * 64
    )
    no_wiki = _person(
        connection, run_id=run_id, name="No Wiki Yet", identity_fingerprint="3" * 64
    )
    _complete_wikipedia(
        connection,
        person_id=eligible,
        run_id=run_id,
        semantic_outcome="no_matching_page_found",
        material_fingerprint="4" * 64,
    )
    plan_id, _form_id, brave_work = _open_active_coverage_with_brave_work(
        connection,
        person_id=matching,
        run_id=run_id,
        material_fingerprint="5" * 64,
        policy_fp=policy.fingerprint,
    )
    _complete_wikipedia(
        connection,
        person_id=matching,
        run_id=run_id,
        semantic_outcome="matching_page_found",
        material_fingerprint="6" * 64,
    )

    acted = seed_coverage_research(
        connection, run_id=run_id, config=config, policy=policy, now=NOW
    )
    assert acted >= 1
    assert _active_plan_count(connection, person_id=eligible) == 1
    assert _active_plan_count(connection, person_id=matching) == 0
    assert _active_plan_count(connection, person_id=no_wiki) == 0
    plan = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert plan is not None and plan["status"] == "superseded"
    work = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (brave_work,)
    ).fetchone()
    assert work is not None and work["state"] == "superseded"


def test_multi_refresh_o0_r1_r2_clock(
    connection: sqlite3.Connection,
) -> None:
    """K30: O0 complete → R1(refresh_of=O0) → R2(refresh_of=R1) under fixed material."""
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="uncertain_identity",
    )
    person_view = CoveragePersonMaterialView(
        person_id=person_id,
        identity_fingerprint=_HASH,
    )
    assess = config.tasks.assess_article
    base_fp = coverage_material_fingerprint(
        person_view,
        assess,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=None,
    )

    o0 = _complete_coverage_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        policy_fp=policy.fingerprint,
        completed_at=NOW,
    )

    soon = _hours_after(NOW, REFRESH_HOURS - 1)
    assert not is_coverage_research_eligible(
        connection, person_id=person_id, config=config, policy=policy, now=soon
    )

    t1 = _hours_after(NOW, REFRESH_HOURS)
    assert is_coverage_research_eligible(
        connection, person_id=person_id, config=config, policy=policy, now=t1
    )
    assert (
        ensure_coverage_research(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=t1,
        )
        == "scheduled"
    )
    r1_plan = connection.execute(
        """
        SELECT id, refresh_of_plan_id, material_fingerprint, status
          FROM person_coverage_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert r1_plan is not None
    assert int(r1_plan["refresh_of_plan_id"]) == o0
    r1_fp = coverage_material_fingerprint(
        person_view,
        assess,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=o0,
    )
    assert r1_plan["material_fingerprint"] == r1_fp

    # Complete R1 as terminal under its fingerprint.
    with immediate(connection):
        update_plan_status(
            connection,
            plan_id=int(r1_plan["id"]),
            status="completed",
            completed_at=t1,
        )
    r1_id = int(r1_plan["id"])

    t2 = _hours_after(t1, REFRESH_HOURS)
    assert is_coverage_research_eligible(
        connection, person_id=person_id, config=config, policy=policy, now=t2
    )
    assert (
        ensure_coverage_research(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=t2,
        )
        == "scheduled"
    )
    r2_plan = connection.execute(
        """
        SELECT id, refresh_of_plan_id, material_fingerprint
          FROM person_coverage_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert r2_plan is not None
    assert int(r2_plan["refresh_of_plan_id"]) == r1_id
    # Must re-anchor on R1, not O0 alone.
    assert int(r2_plan["refresh_of_plan_id"]) != o0
    r2_fp = coverage_material_fingerprint(
        person_view,
        assess,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=r1_id,
    )
    assert r2_plan["material_fingerprint"] == r2_fp


def test_supersede_coverage_work_for_person_cancels_active_only(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, _form_id, brave_work = _open_active_coverage_with_brave_work(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint="7" * 64,
        policy_fp=policy.fingerprint,
    )
    with immediate(connection):
        supersede_coverage_work_for_person(
            connection, person_id=person_id, run_id=run_id, now=NOW
        )
    plan = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert plan is not None and plan["status"] == "superseded"
    work = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (brave_work,)
    ).fetchone()
    assert work is not None and work["state"] == "superseded"


def test_failed_terminal_does_not_time_refresh(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="no_matching_page_found",
    )
    person_view = CoveragePersonMaterialView(
        person_id=person_id, identity_fingerprint=_HASH
    )
    base_fp = coverage_material_fingerprint(
        person_view,
        config.tasks.assess_article,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=None,
    )
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=base_fp,
            source_policy_fingerprint=policy.fingerprint,
            retrieval_target=5,
            created_at=NOW,
        )
        update_plan_status(conn, plan_id=plan_id, status="failed", completed_at=NOW)
    far = _hours_after(NOW, REFRESH_HOURS * 10)
    assert not is_coverage_research_eligible(
        connection, person_id=person_id, config=config, policy=policy, now=far
    )
    assert (
        ensure_coverage_research(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=far,
        )
        == "ineligible"
    )


def _plan_status(connection: sqlite3.Connection, *, plan_id: int) -> str:
    row = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert row is not None
    return str(row["status"])


def test_seed_recovers_a_plan_wedged_by_a_settlement_that_skipped_persist(
    connection: sqlite3.Connection,
) -> None:
    """C2: `maybe_advance_coverage_plan` only runs from the persist paths.

    A raising `prepare` settles the work item `failed_permanent` with no
    failure, so `persist_failure` never runs and the form keeps its `pending`
    status. Nothing then advances the plan: it stays active forever,
    `ensure_coverage_research` answers `reused`, and the person vanishes from
    "coverage eligible remaining". The next run must sweep it.
    """
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="no_matching_page_found",
    )
    config = _config()
    policy = _policy()
    material_fp = coverage_material_fingerprint(
        CoveragePersonMaterialView(person_id=person_id, identity_fingerprint=_HASH),
        config.tasks.assess_article,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=None,
    )
    plan_id, form_id, work_id = _open_active_coverage_with_brave_work(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=material_fp,
        policy_fp=policy.fingerprint,
    )
    # Exactly what the engine leaves behind for a raising `prepare`.
    with immediate(connection) as conn:
        conn.execute(
            """
            UPDATE work_item
               SET state = 'failed_permanent', reason = 'prepare raised ValueError',
                   completed_by_run_id = ?, updated_at = ?
             WHERE id = ?
            """,
            (run_id, NOW, work_id),
        )
    assert _plan_status(connection, plan_id=plan_id) == "retrieving"
    form_before = connection.execute(
        "SELECT status FROM coverage_query_form WHERE id = ?", (form_id,)
    ).fetchone()
    assert form_before is not None
    assert form_before["status"] == "pending"

    next_run_id = insert_run(connection)
    later = _hours_after(NOW, 1)
    seed_coverage_research(
        connection,
        run_id=next_run_id,
        config=config,
        policy=policy,
        now=later,
    )

    assert _plan_status(connection, plan_id=plan_id) in {
        "completed",
        "incomplete",
        "failed",
    }
    assert _active_plan_count(connection, person_id=person_id) == 0
    form_after = connection.execute(
        "SELECT status, failure_category FROM coverage_query_form WHERE id = ?",
        (form_id,),
    ).fetchone()
    assert form_after is not None
    assert form_after["status"] == "failed"
    assert form_after["failure_category"] == "work_item_failed_permanent"


def test_sweep_leaves_a_plan_alone_while_its_work_can_still_run(
    connection: sqlite3.Connection,
) -> None:
    """Positive control for the sweep: a live work item is not swept."""
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="no_matching_page_found",
    )
    config = _config()
    policy = _policy()
    material_fp = coverage_material_fingerprint(
        CoveragePersonMaterialView(person_id=person_id, identity_fingerprint=_HASH),
        config.tasks.assess_article,
        source_policy_fingerprint=policy.fingerprint,
        refresh_of_plan_id=None,
    )
    plan_id, form_id, _work_id = _open_active_coverage_with_brave_work(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=material_fp,
        policy_fp=policy.fingerprint,
    )

    next_run_id = insert_run(connection)
    seed_coverage_research(
        connection,
        run_id=next_run_id,
        config=config,
        policy=policy,
        now=_hours_after(NOW, 1),
    )

    assert _plan_status(connection, plan_id=plan_id) == "retrieving"
    row = connection.execute(
        "SELECT status FROM coverage_query_form WHERE id = ?", (form_id,)
    ).fetchone()
    assert row is not None
    assert row["status"] == "pending"
