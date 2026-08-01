"""K18 merge reconcile: supersede loser, reassign person_article, no blind pointers."""

from __future__ import annotations

import sqlite3
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
from notable_person_finder.coverage.merge_hooks import reconcile_on_merge
from notable_person_finder.coverage.repository import (
    insert_person_article_assessment,
    insert_query_forms,
    open_plan,
    upsert_person_article,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import (
    ASSESS_ARTICLE_TASK_TYPE,
    BRAVE_WEB_SEARCH_TASK_TYPE,
    SUBJECT_KIND_PERSON_ARTICLE,
)
from notable_person_finder.ingestion.repository import upsert_article
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.merge import confirm_person_merge
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
)
from notable_person_finder.wikipedia.repository import (
    open_plan as open_wiki_plan,
)
from tests.ingestion.helpers import immediate, insert_run, moment

NOW = moment()
_HASH = "a" * 64
_PROMPT = "p" * 64
_SCHEMA = "s" * 64
MODEL = "openai/gpt-test"


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


def _config() -> MainConfig:
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
                max_alias_forms=0,
                max_context_forms=0,
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


def _complete_wikipedia_no_match(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_wiki_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            created_at=NOW,
        )
        conn.execute(
            """
            UPDATE wikipedia_identity_plan
               SET status = 'completed', completed_at = ?
             WHERE id = ?
            """,
            (NOW, plan_id),
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
            observed_at=NOW,
        )
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )
    return int(obs_id)


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
            'assess_article', 'person_article', 1, ?, 1, 70, ?,
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


def _active_coverage_with_work(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    policy_fp: str,
) -> tuple[int, int]:
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
                    "query_text": "Name",
                },
            ),
        )
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'coverage_query_form', ?, ?, 1, 60, ?, 'pending', ?, ?, ?)
            """,
            (
                BRAVE_WEB_SEARCH_TASK_TYPE,
                form_ids[0],
                "1" * 64,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert work_id is not None
    return plan_id, int(work_id)


def _article(connection: sqlite3.Connection, *, url: str) -> int:
    with immediate(connection) as conn:
        return upsert_article(
            conn,
            canonical_url=url,
            publisher_key="example.com",
            now=NOW,
        )


def _view(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    canonical_article_id: int,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO article_view (
            canonical_article_id, run_id, attempt_id, access_kind,
            requested_url, final_url, title, dek, byline, published_at,
            editorial_labels_json, main_text_blocks_json, snippets_json,
            extraction_quality, extractor_version, observed_at
        ) VALUES (
            ?, ?, NULL, 'full', 'https://example.com/a', 'https://example.com/a',
            'Title', NULL, NULL, NULL, '[]', '[{"id":"b1","text":"Body"}]',
            '[]', 'full', 1, ?
        )
        """,
        (canonical_article_id, run_id, NOW),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return int(cursor.lastrowid)


def test_reconcile_supersedes_loser_and_ensures_survivor(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Ada", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Ada B", fingerprint="2" * 64)
    plan_id, work_id = _active_coverage_with_work(
        connection,
        person_id=loser,
        run_id=run_id,
        material_fingerprint="3" * 64,
        policy_fp=policy.fingerprint,
    )
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="4" * 64,
    )

    with immediate(connection):
        reconcile_on_merge(
            connection,
            survivor_id=survivor,
            loser_id=loser,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )

    plan = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert plan is not None and plan["status"] == "superseded"
    work = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert work is not None and work["state"] == "superseded"

    survivor_plans = connection.execute(
        """
        SELECT COUNT(*) AS n FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
        """,
        (survivor,),
    ).fetchone()
    assert survivor_plans is not None
    assert int(survivor_plans["n"]) == 1


def test_person_article_reassign_without_conflict(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Bea", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Bea C", fingerprint="2" * 64)
    article_id = _article(connection, url="https://example.com/bea")
    with immediate(connection) as conn:
        pa_id = upsert_person_article(
            conn,
            person_id=loser,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="5" * 64,
    )

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser,
            survivor_id=survivor,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
            coverage_policy=policy,
        )

    row = connection.execute(
        "SELECT person_id FROM person_article WHERE id = ?", (pa_id,)
    ).fetchone()
    assert row is not None
    assert int(row["person_id"]) == survivor


def test_person_article_conflict_keeps_lower_id_and_immutable_assessment_person(
    connection: sqlite3.Connection,
) -> None:
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Cara", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Cara D", fingerprint="2" * 64)
    article_id = _article(connection, url="https://example.com/cara")
    view_id = _view(connection, run_id=run_id, canonical_article_id=article_id)

    with immediate(connection) as conn:
        keeper_id = upsert_person_article(
            conn,
            person_id=survivor,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
        retiree_id = upsert_person_article(
            conn,
            person_id=loser,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
        attempt_id, inspection_id = _attempt_and_inspection(
            conn, run_id=run_id, fingerprint="t" * 64
        )
        # Completed assessment on retiree only — pointer should move to keeper.
        assessment_id = insert_person_article_assessment(
            conn,
            person_article_id=retiree_id,
            person_id=loser,
            canonical_article_id=article_id,
            plan_id=None,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            person_relation="same_person",
            coverage_depth="significant",
            content_types_json='["reporting"]',
            subject_relationship="editorially_independent",
            screening_rule_id="eligible.example.com",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=policy.fingerprint,
            canonical_supplied_input_json="{}",
            validated_output_json="{}",
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint="t" * 64,
            rationale="ok",
            failure_category=None,
            observed_at=NOW,
        )
        conn.execute(
            """
            UPDATE person_article
               SET current_assessment_id = ?
             WHERE id = ?
            """,
            (assessment_id, retiree_id),
        )

    assert keeper_id < retiree_id
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="6" * 64,
    )

    with immediate(connection):
        reconcile_on_merge(
            connection,
            survivor_id=survivor,
            loser_id=loser,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )

    # Retiree gone; keeper remains.
    assert (
        connection.execute(
            "SELECT id FROM person_article WHERE id = ?", (retiree_id,)
        ).fetchone()
        is None
    )
    keeper = connection.execute(
        """
        SELECT person_id, current_assessment_id
          FROM person_article WHERE id = ?
        """,
        (keeper_id,),
    ).fetchone()
    assert keeper is not None
    assert int(keeper["person_id"]) == survivor
    assert int(keeper["current_assessment_id"]) == assessment_id

    assessment = connection.execute(
        """
        SELECT person_id, person_article_id
          FROM person_article_assessment WHERE id = ?
        """,
        (assessment_id,),
    ).fetchone()
    assert assessment is not None
    # Observation-time person_id is immutable (still loser).
    assert int(assessment["person_id"]) == loser
    assert int(assessment["person_article_id"]) == keeper_id


def test_merge_does_not_blind_copy_coverage_pointers(
    connection: sqlite3.Connection,
) -> None:
    """Never invent survivor current assessment from loser without conflict rules."""
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Dee", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Dee E", fingerprint="2" * 64)
    article_id = _article(connection, url="https://example.com/dee")
    view_id = _view(connection, run_id=run_id, canonical_article_id=article_id)
    with immediate(connection) as conn:
        pa_id = upsert_person_article(
            conn,
            person_id=loser,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
        attempt_id, inspection_id = _attempt_and_inspection(
            conn, run_id=run_id, fingerprint="u" * 64
        )
        assessment_id = insert_person_article_assessment(
            conn,
            person_article_id=pa_id,
            person_id=loser,
            canonical_article_id=article_id,
            plan_id=None,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            person_relation="same_person",
            coverage_depth="passing",
            content_types_json='["profile"]',
            subject_relationship="uncertain",
            screening_rule_id="eligible.example.com",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=policy.fingerprint,
            canonical_supplied_input_json="{}",
            validated_output_json="{}",
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint="u" * 64,
            rationale="ok",
            failure_category=None,
            observed_at=NOW,
        )
        conn.execute(
            """
            UPDATE person_article
               SET current_assessment_id = ?
             WHERE id = ?
            """,
            (assessment_id, pa_id),
        )
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="7" * 64,
    )

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser,
            survivor_id=survivor,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
            coverage_policy=policy,
        )

    # Reassigned row keeps its own current pointer (not a Wikipedia-style blind set
    # inventing a different survivor-only pointer row).
    row = connection.execute(
        """
        SELECT person_id, current_assessment_id
          FROM person_article WHERE id = ?
        """,
        (pa_id,),
    ).fetchone()
    assert row is not None
    assert int(row["person_id"]) == survivor
    assert int(row["current_assessment_id"]) == assessment_id
    assessment = connection.execute(
        "SELECT person_id FROM person_article_assessment WHERE id = ?",
        (assessment_id,),
    ).fetchone()
    assert assessment is not None
    assert int(assessment["person_id"]) == loser


def _assess_work(
    connection: sqlite3.Connection,
    *,
    person_article_id: int,
    run_id: int,
    fingerprint: str,
) -> int:
    with immediate(connection) as conn:
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, 1, 70, ?, 'pending', ?, ?, ?)
            """,
            (
                ASSESS_ARTICLE_TASK_TYPE,
                SUBJECT_KIND_PERSON_ARTICLE,
                person_article_id,
                fingerprint,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
        assert work_id is not None
    return int(work_id)


def _work_state(connection: sqlite3.Connection, work_id: int) -> str:
    row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row is not None
    return str(row["state"])


def test_merge_supersedes_assess_work_naming_the_retired_relation(
    connection: sqlite3.Connection,
) -> None:
    """I3: the retiree may be the *survivor's* person_article row.

    The keeper is the lower `person_article.id`, not the survivor's. When
    `survivor_pa_id > loser_pa_id` the survivor's row is deleted, and pending
    assess work naming it is left with a dangling `subject_id` -- which
    `prepare` then raises on, into a settlement that writes nothing.
    """
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    # The loser is created first, so its person_article id is the lower one and
    # the survivor's is the row that retires.
    loser = _person(connection, run_id=run_id, name="Fay G", fingerprint="2" * 64)
    survivor = _person(connection, run_id=run_id, name="Fay", fingerprint="1" * 64)
    article_id = _article(connection, url="https://example.com/fay")
    with immediate(connection) as conn:
        loser_pa_id = upsert_person_article(
            conn,
            person_id=loser,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
        survivor_pa_id = upsert_person_article(
            conn,
            person_id=survivor,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
    assert survivor_pa_id > loser_pa_id

    survivor_work = _assess_work(
        connection,
        person_article_id=survivor_pa_id,
        run_id=run_id,
        fingerprint="9" * 64,
    )
    loser_work = _assess_work(
        connection,
        person_article_id=loser_pa_id,
        run_id=run_id,
        fingerprint="8" * 64,
    )
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="7" * 64,
    )

    with immediate(connection):
        reconcile_on_merge(
            connection,
            survivor_id=survivor,
            loser_id=loser,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )

    assert (
        connection.execute(
            "SELECT id FROM person_article WHERE id = ?", (survivor_pa_id,)
        ).fetchone()
        is None
    )
    # No work item may still name a person_article that no longer exists.
    dangling = connection.execute(
        """
        SELECT w.id
          FROM work_item AS w
          LEFT JOIN person_article AS pa ON pa.id = w.subject_id
         WHERE w.task_type = ?
           AND w.subject_kind = ?
           AND w.state IN ('pending', 'deferred')
           AND pa.id IS NULL
        """,
        (ASSESS_ARTICLE_TASK_TYPE, SUBJECT_KIND_PERSON_ARTICLE),
    ).fetchall()
    assert dangling == []
    assert _work_state(connection, survivor_work) == "superseded"
    assert _work_state(connection, loser_work) == "superseded"


def test_merge_supersedes_the_survivors_own_stale_plan_before_opening_a_new_one(
    connection: sqlite3.Connection,
) -> None:
    """I4: the survivor's fingerprint changes, so a second plan would open.

    Leaving the old plan active means its Brave, fetch, and assess work keeps
    running alongside the new plan's -- duplicate paid calls for one person.
    """
    config = _config()
    policy = _policy()
    run_id = insert_run(connection)
    survivor = _person(connection, run_id=run_id, name="Hal", fingerprint="1" * 64)
    loser = _person(connection, run_id=run_id, name="Hal I", fingerprint="2" * 64)
    _complete_wikipedia_no_match(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="5" * 64,
    )
    stale_plan_id, stale_work_id = _active_coverage_with_work(
        connection,
        person_id=survivor,
        run_id=run_id,
        material_fingerprint="3" * 64,
        policy_fp=policy.fingerprint,
    )

    with immediate(connection):
        reconcile_on_merge(
            connection,
            survivor_id=survivor,
            loser_id=loser,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
        )

    stale = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (stale_plan_id,)
    ).fetchone()
    assert stale is not None
    assert stale["status"] == "superseded"
    assert _work_state(connection, stale_work_id) == "superseded"
    active = connection.execute(
        """
        SELECT COUNT(*) AS n
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('retrieving', 'selecting', 'assessing')
        """,
        (survivor,),
    ).fetchone()
    assert active is not None
    # At most one plan may be active for the survivor after the merge.
    assert int(active["n"]) <= 1
