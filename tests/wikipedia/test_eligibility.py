"""K17 eligibility: branches, double/triple refresh, failed no time-refresh."""

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
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.wikipedia.matching import base_material_fingerprint
from notable_person_finder.wikipedia.models import WikipediaPersonMaterialView
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    open_plan,
    point_person_current_wikipedia_observation,
)
from notable_person_finder.wikipedia.service import (
    ensure_wikipedia_identity,
    is_wikipedia_match_eligible,
)
from tests.ingestion.helpers import immediate, insert_run, moment

NOW = moment()
REFRESH_HOURS = 24
_HASH = "a" * 64


def _config(*, refresh_hours: int = REFRESH_HOURS) -> MainConfig:
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
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model="openai/gpt-match",
                refresh_interval_hours=refresh_hours,
            )
        ),
    )


def _hours_after(base: str, hours: float) -> str:
    dt = datetime.fromisoformat(base.replace("Z", "+00:00"))
    return utc_timestamp(dt + timedelta(hours=hours))


def _person_with_name(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    name: str = "Alex Smith",
    fingerprint: str = _HASH,
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


def _person_view(
    connection: sqlite3.Connection, person_id: int
) -> WikipediaPersonMaterialView:
    row = connection.execute(
        "SELECT id, identity_fingerprint FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    assert row is not None
    return WikipediaPersonMaterialView(
        person_id=int(row["id"]),
        identity_fingerprint=str(row["identity_fingerprint"]),
    )


def _complete_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    observed_at: str,
    refresh_of: int | None = None,
    point_current: bool = True,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=observed_at,
            refresh_of_observation_id=refresh_of,
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
            task_fingerprint=material_fingerprint,
            rationale="test complete",
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
                conn,
                person_id=person_id,
                observation_id=obs_id,
            )
    return obs_id


def _failed_observation(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    observed_at: str,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=observed_at,
        )
        obs_id = insert_wikipedia_identity_observation(
            conn,
            person_id=person_id,
            plan_id=plan_id,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="failed",
            semantic_outcome=None,
            matched_mediawiki_page_id=None,
            candidate_page_ids_json="[]",
            canonical_supplied_input_json='{"task":"wikipedia_identity"}',
            validated_output_json=None,
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=material_fingerprint,
            rationale="test fail",
            failure_category="unsafe_truncation",
            observed_at=observed_at,
        )
        conn.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'failed', completed_at = ?, failure_category = ?
             WHERE id = ?
            """,
            (observed_at, "unsafe_truncation", plan_id),
        )
    return obs_id


def test_merged_away_not_eligible(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    survivor = _person_with_name(connection, run_id=run_id, name="A")
    loser = _person_with_name(connection, run_id=run_id, name="B", fingerprint="b" * 64)
    with immediate(connection):
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, loser),
        )
    config = _config()
    assert (
        is_wikipedia_match_eligible(connection, person_id=loser, config=config, now=NOW)
        is False
    )


def test_no_match_key_name_not_eligible(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="???",
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
    config = _config()
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=NOW
        )
        is False
    )


def test_fresh_person_with_name_eligible(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=NOW
        )
        is True
    )


def test_fresh_completed_within_interval_not_eligible(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )
    later = _hours_after(NOW, REFRESH_HOURS - 1)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=later
        )
        is False
    )


def test_completed_after_interval_eligible_with_refresh_of_current(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    o0 = _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )
    due = _hours_after(NOW, REFRESH_HOURS)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=due
        )
        is True
    )
    result = ensure_wikipedia_identity(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        now=due,
    )
    assert result == "scheduled"
    plan = connection.execute(
        """
        SELECT refresh_of_observation_id, material_fingerprint, status
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC
         LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert plan is not None
    assert int(plan["refresh_of_observation_id"]) == o0
    expected_fp = base_material_fingerprint(
        view,
        config.tasks.match_wikipedia_identity,
        refresh_of_observation_id=o0,
    )
    assert plan["material_fingerprint"] == expected_fp


def test_double_and_triple_refresh_reanchors_on_current(
    connection: sqlite3.Connection,
) -> None:
    """O0 → R1(refresh_of=O0) → R2(refresh_of=R1) under fixed person+config."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    match = config.tasks.match_wikipedia_identity

    base_fp = base_material_fingerprint(view, match, refresh_of_observation_id=None)
    o0 = _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )

    t1 = _hours_after(NOW, REFRESH_HOURS)
    assert is_wikipedia_match_eligible(
        connection, person_id=person_id, config=config, now=t1
    )
    ensure_wikipedia_identity(
        connection, person_id=person_id, run_id=run_id, config=config, now=t1
    )
    r1_fp = base_material_fingerprint(view, match, refresh_of_observation_id=o0)
    # Supersede the retrieving plan and complete R1 as current.
    with immediate(connection):
        connection.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'superseded', completed_at = ?
             WHERE person_id = ? AND status = 'retrieving'
            """,
            (t1, person_id),
        )
    r1 = _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=r1_fp,
        observed_at=t1,
        refresh_of=o0,
    )

    t2 = _hours_after(t1, REFRESH_HOURS)
    assert is_wikipedia_match_eligible(
        connection, person_id=person_id, config=config, now=t2
    )
    ensure_wikipedia_identity(
        connection, person_id=person_id, run_id=run_id, config=config, now=t2
    )
    plan_r2 = connection.execute(
        """
        SELECT refresh_of_observation_id, material_fingerprint
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert plan_r2 is not None
    assert int(plan_r2["refresh_of_observation_id"]) == r1
    r2_fp = base_material_fingerprint(view, match, refresh_of_observation_id=r1)
    assert plan_r2["material_fingerprint"] == r2_fp
    # Must not re-anchor on O0.
    assert int(plan_r2["refresh_of_observation_id"]) != o0

    with immediate(connection):
        connection.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'superseded', completed_at = ?
             WHERE person_id = ? AND status = 'retrieving'
            """,
            (t2, person_id),
        )
    r2 = _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=r2_fp,
        observed_at=t2,
        refresh_of=r1,
    )

    t3 = _hours_after(t2, REFRESH_HOURS)
    assert is_wikipedia_match_eligible(
        connection, person_id=person_id, config=config, now=t3
    )
    ensure_wikipedia_identity(
        connection, person_id=person_id, run_id=run_id, config=config, now=t3
    )
    plan_r3 = connection.execute(
        """
        SELECT refresh_of_observation_id
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert plan_r3 is not None
    assert int(plan_r3["refresh_of_observation_id"]) == r2


def test_branch2_completed_without_pointer_time_refresh(
    connection: sqlite3.Connection,
) -> None:
    """Completed base_fp without current pointer: interval then refresh_of=that id."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    obs_id = _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
        point_current=False,
    )
    ptr = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    assert ptr is not None
    assert ptr["current_wikipedia_identity_observation_id"] is None

    soon = _hours_after(NOW, REFRESH_HOURS - 1)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=soon
        )
        is False
    )
    due = _hours_after(NOW, REFRESH_HOURS)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=due
        )
        is True
    )
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=due,
        )
        == "scheduled"
    )
    plan = connection.execute(
        """
        SELECT refresh_of_observation_id
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert plan is not None
    assert int(plan["refresh_of_observation_id"]) == obs_id


def test_k19_stale_active_plan_superseded_on_material_change(
    connection: sqlite3.Connection,
) -> None:
    """Ensure under new live_fp supersedes active plan for a different fingerprint."""
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
        == "scheduled"
    )
    old_plan = connection.execute(
        """
        SELECT id, material_fingerprint
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
        """,
        (person_id,),
    ).fetchone()
    assert old_plan is not None

    with immediate(connection):
        connection.execute(
            """
            UPDATE person
               SET identity_fingerprint = ?
             WHERE id = ?
            """,
            ("9" * 64, person_id),
        )
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
        == "scheduled"
    )
    old_status = connection.execute(
        "SELECT status FROM wikipedia_identity_plan WHERE id = ?",
        (int(old_plan["id"]),),
    ).fetchone()
    assert old_status is not None
    assert old_status["status"] == "superseded"
    new_plan = connection.execute(
        """
        SELECT material_fingerprint
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert new_plan is not None
    assert new_plan["material_fingerprint"] != old_plan["material_fingerprint"]


def test_failed_terminal_does_not_time_refresh(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    _failed_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )
    far_future = _hours_after(NOW, REFRESH_HOURS * 10)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=far_future
        )
        is False
    )


def test_material_change_rearms_base_fp(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )
    # Identity material changes → current no longer matches live → new base_fp.
    with immediate(connection):
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alexandra Smith",
            kind="alias",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
        connection.execute(
            """
            UPDATE person
               SET identity_fingerprint = ?
             WHERE id = ?
            """,
            ("c" * 64, person_id),
        )
    # Even within refresh interval of old obs, new material is eligible.
    soon = _hours_after(NOW, 1)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=soon
        )
        is True
    )
    result = ensure_wikipedia_identity(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        now=soon,
    )
    assert result == "scheduled"
    plan = connection.execute(
        """
        SELECT refresh_of_observation_id, material_fingerprint
          FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    assert plan is not None
    assert plan["refresh_of_observation_id"] is None
    new_view = _person_view(connection, person_id)
    expected = base_material_fingerprint(
        new_view, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    assert plan["material_fingerprint"] == expected


def test_namesake_people_do_not_share_mapping(connection: sqlite3.Connection) -> None:
    """K9: same display name, different person → never reuse observation."""
    run_id = insert_run(connection)
    a = _person_with_name(
        connection, run_id=run_id, name="Alex Smith", fingerprint="1" * 64
    )
    b = _person_with_name(
        connection, run_id=run_id, name="Alex Smith", fingerprint="2" * 64
    )
    config = _config()
    view_a = _person_view(connection, a)
    base_a = base_material_fingerprint(
        view_a, config.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    _complete_observation(
        connection,
        person_id=a,
        run_id=run_id,
        material_fingerprint=base_a,
        observed_at=NOW,
    )
    # B is still eligible (no shared mapping by name).
    assert (
        is_wikipedia_match_eligible(connection, person_id=b, config=config, now=NOW)
        is True
    )
    # A's terminal does not block B ensure.
    result = ensure_wikipedia_identity(
        connection, person_id=b, run_id=run_id, config=config, now=NOW
    )
    assert result == "scheduled"
    # Observation for A is not current for B.
    b_ptr = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (b,),
    ).fetchone()
    assert b_ptr is not None
    assert b_ptr["current_wikipedia_identity_observation_id"] is None


def test_active_plan_blocks_eligibility_but_ensure_reuses(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config = _config()
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
        == "scheduled"
    )
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config, now=NOW
        )
        is False
    )
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
        == "reused"
    )


def test_config_change_rearms_base_fp(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    person_id = _person_with_name(connection, run_id=run_id)
    config_a = _config()
    view = _person_view(connection, person_id)
    base_fp = base_material_fingerprint(
        view, config_a.tasks.match_wikipedia_identity, refresh_of_observation_id=None
    )
    _complete_observation(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint=base_fp,
        observed_at=NOW,
    )
    config_b = MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        mediawiki=MediaWikiConfig(),
        tasks=TasksConfig(
            match_wikipedia_identity=MatchWikipediaIdentityConfig(
                model="openai/gpt-match",
                max_candidates=4,  # bound change → new fingerprint
                refresh_interval_hours=REFRESH_HOURS,
            )
        ),
    )
    soon = _hours_after(NOW, 1)
    assert (
        is_wikipedia_match_eligible(
            connection, person_id=person_id, config=config_b, now=soon
        )
        is True
    )
