"""K16 merge reconcile: supersede loser work, ensure survivor, no pointer copy."""

from __future__ import annotations

import sqlite3
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
from notable_person_finder.people.identity import (
    insert_person,
    mentions_for_canonical_person,
    upsert_sourced_name,
)
from notable_person_finder.people.merge import confirm_person_merge
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.wikipedia.merge_hooks import reconcile_on_merge
from notable_person_finder.wikipedia.repository import (
    insert_page_facts_batch,
    insert_query_forms,
    insert_wikipedia_identity_observation,
    open_plan,
    point_person_current_wikipedia_observation,
)
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
    MEDIAWIKI_SEARCH_TASK_TYPE,
    _operational_match_material,
    ensure_wikipedia_identity,
)
from tests.ingestion.helpers import immediate, insert_run, moment

NOW = moment()
_HASH = "a" * 64
MATCH_MODEL = "openai/gpt-match"


def _config() -> MainConfig:
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
                model=MATCH_MODEL,
                refresh_interval_hours=24,
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


def _schedule_wiki_plan_with_work(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
) -> tuple[int, int, int, int]:
    """Return (plan_id, form_id, batch_id, match_work_id)."""
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=NOW,
        )
        form_ids = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=({"ordinal": 1, "variant_kind": "exact", "query_text": "Name"},),
        )
        form_id = form_ids[0]
        batch_id = insert_page_facts_batch(
            conn,
            plan_id=plan_id,
            ordinal=1,
            page_ids_json="[1]",
            wave=1,
            created_at=NOW,
        )
        search_work = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'wikipedia_query_form', ?, ?, 1, 50, ?, 'pending', ?, ?, ?)
            """,
            (
                MEDIAWIKI_SEARCH_TASK_TYPE,
                form_id,
                "1" * 64,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert search_work is not None
        facts_work = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'wikipedia_page_facts_batch', ?, ?, 1, 50, ?, 'pending',
                      ?, ?, ?)
            """,
            (
                MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
                batch_id,
                "2" * 64,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert facts_work is not None
        match_work = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'person', ?, ?, 1, 55, ?, 'pending', ?, ?, ?)
            """,
            (
                MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
                person_id,
                material_fingerprint,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert match_work is not None
    return plan_id, form_id, batch_id, match_work


def _complete_obs_and_point(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=NOW,
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
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"no_matching_page_found"}',
            prompt_hash=None,
            schema_hash=None,
            schema_version=None,
            task_fingerprint=material_fingerprint,
            rationale="no match",
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
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )
    return obs_id


def test_reconcile_supersedes_loser_work_and_ensures_survivor(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Ada", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Ada B", fingerprint="2" * 64)
    plan_id, _form_id, _batch_id, match_work = _schedule_wiki_plan_with_work(
        connection,
        person_id=loser,
        run_id=run_id,
        material_fingerprint="3" * 64,
    )
    with immediate(connection):
        reconcile_on_merge(
            connection,
            survivor_id=survivor,
            loser_id=loser,
            run_id=run_id,
            config=config,
            now=NOW,
        )

    plan = connection.execute(
        "SELECT status FROM wikipedia_identity_plan WHERE id = ?",
        (plan_id,),
    ).fetchone()
    assert plan is not None
    assert plan["status"] == "superseded"

    match_state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?",
        (match_work,),
    ).fetchone()
    assert match_state is not None
    assert match_state["state"] == "superseded"

    search_states = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND state != 'superseded'
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE,),
    ).fetchall()
    # Loser search superseded; survivor ensure may have scheduled new search.
    loser_form_work = connection.execute(
        """
        SELECT wi.state
          FROM work_item AS wi
          JOIN wikipedia_query_form AS qf ON qf.id = wi.subject_id
         WHERE wi.task_type = ?
           AND qf.plan_id = ?
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE, plan_id),
    ).fetchone()
    assert loser_form_work is not None
    assert loser_form_work["state"] == "superseded"

    loser_facts_work = connection.execute(
        """
        SELECT wi.state
          FROM work_item AS wi
          JOIN wikipedia_page_facts_batch AS b ON b.id = wi.subject_id
         WHERE wi.task_type = ?
           AND b.plan_id = ?
        """,
        (MEDIAWIKI_PAGE_FACTS_TASK_TYPE, plan_id),
    ).fetchone()
    assert loser_facts_work is not None
    assert loser_facts_work["state"] == "superseded"

    survivor_plan = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM wikipedia_identity_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'ready_for_match')
        """,
        (survivor,),
    ).fetchone()
    assert survivor_plan is not None
    assert int(survivor_plan["n"]) == 1
    del search_states


def test_merge_does_not_copy_loser_pointer(connection: sqlite3.Connection) -> None:
    config = _config()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Bea", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Bea C", fingerprint="2" * 64)
    loser_obs = _complete_obs_and_point(
        connection,
        person_id=loser,
        run_id=run_id,
        material_fingerprint="4" * 64,
    )
    survivor_before = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (survivor,),
    ).fetchone()
    assert survivor_before is not None
    assert survivor_before["current_wikipedia_identity_observation_id"] is None

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser,
            survivor_id=survivor,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
        )

    survivor_after = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (survivor,),
    ).fetchone()
    assert survivor_after is not None
    # K16: must not adopt loser's observation id.
    assert survivor_after["current_wikipedia_identity_observation_id"] != loser_obs
    assert survivor_after["current_wikipedia_identity_observation_id"] is None

    loser_after = connection.execute(
        """
        SELECT merged_into_person_id, current_wikipedia_identity_observation_id
          FROM person WHERE id = ?
        """,
        (loser,),
    ).fetchone()
    assert loser_after is not None
    assert int(loser_after["merged_into_person_id"]) == survivor
    # Historical loser pointer may remain on loser row.
    assert loser_after["current_wikipedia_identity_observation_id"] == loser_obs


def test_post_merge_projection_includes_loser_mention_facts(
    connection: sqlite3.Connection,
) -> None:
    """K27: match material assembly includes facts from loser-linked mentions."""
    config = _config()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Cara", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Cara D", fingerprint="2" * 64)

    # Attach a mention with a non-name fact still FK'd to the loser person id.
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (_HASH, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert work is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work, NOW, moment(1), _HASH),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES ('feed-m', 'Feed', 'https://example.com/f', ?, ?)
        """,
        (NOW, NOW),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/f', 'modified')
        """,
        (feed, run_id, NOW),
    ).lastrowid
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, 'e1', 'Title', 'Summary', ?)
        """,
        (feed, fetch, run_id, NOW),
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
        ) VALUES (?, ?, 'm', 'm', ?, '[]', 1, 1, 1, 1, 'compatible', ?)
        """,
        (run_id, attempt_id, _HASH, NOW),
    ).lastrowid
    assert inspection_id is not None
    triage_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'ok', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            "p" * 64,
            "s" * 64,
            _HASH,
            NOW,
        ),
    ).lastrowid
    assert triage_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale, person_id
        ) VALUES (?, 1, 'Cara D', ?, 'research', '["p1"]', 'r', ?)
        """,
        (triage_id, mechanical_search_name("Cara D"), loser),
    ).lastrowid
    assert mention_id is not None
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value, supporting_passage_ids_json
        ) VALUES (?, 'f1', 'profession_or_role', 'sculptor', '["p1"]')
        """,
        (mention_id,),
    )
    connection.commit()

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser,
            survivor_id=survivor,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
        )

    mentions = mentions_for_canonical_person(connection, survivor)
    assert any(m.id == mention_id for m in mentions)

    _display, _names, facts = _operational_match_material(
        connection,
        person_id=survivor,
        match_config=config.tasks.match_wikipedia_identity,
    )
    assert any(fact.value == "sculptor" for fact in facts)


def test_ensure_survivor_under_combined_identity(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Dee", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Dee E", fingerprint="2" * 64)
    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser,
            survivor_id=survivor,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
        )
    # Survivor should have an active plan after merge ensure.
    plan = connection.execute(
        """
        SELECT status FROM wikipedia_identity_plan
         WHERE person_id = ? AND status = 'retrieving'
        """,
        (survivor,),
    ).fetchone()
    assert plan is not None
    # Re-ensure is reuse.
    assert (
        ensure_wikipedia_identity(
            connection,
            person_id=survivor,
            run_id=run_id,
            config=config,
            now=NOW,
        )
        == "reused"
    )
