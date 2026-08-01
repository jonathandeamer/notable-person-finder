"""Digest and status counter alignment for coverage evidence (milestone 5).

Replaces a 22-line stub that asserted nine zero-counts against a freshly
migrated, *empty* database and never rendered anything -- a review confirmed
it passed even with the entire "### Coverage evidence" digest section
deleted. This file builds populated fixtures (real plans, targets,
assessments, merges, and cross-run settlements) so every digest counter and
``notable status`` line is exercised against a nonzero, independently
computable value, mirroring the house style in
``tests/wikipedia/test_digest_status.py``.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from notable_person_finder.config.models import (
    AssessArticleConfig,
    BudgetConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage.repository import (
    coverage_corpus_counts,
    coverage_run_counts,
    insert_article_view,
    insert_or_load_brave_search_observation_by_attempt,
    insert_query_forms,
    insert_search_result_occurrences,
    open_plan,
    update_plan_status,
    upsert_person_article,
)
from notable_person_finder.coverage.repository import (
    insert_person_article_assessment as _insert_assessment,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import count_coverage_research_eligible
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.ingestion.repository import upsert_article
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.reporting.digest import CoverageSummary, render_digest
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from notable_person_finder.wikipedia.repository import (
    open_plan as open_wiki_plan,
)
from tests.ingestion.helpers import (
    immediate,
    insert_configuration_snapshot,
    moment,
)
from tests.people.test_run_cli import _single_feed, write_people_graph

NOW = moment()
_PROMPT_HASH = "1" * 64
_SCHEMA_HASH = "2" * 64


def _fp(tag: str, index: int) -> str:
    """A distinct, schema-valid (64-char) fingerprint for loop-built fixtures."""
    raw = f"{tag}{index}"
    return raw.ljust(64, "0")[:64]


def _config() -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        tasks=TasksConfig(assess_article=AssessArticleConfig(model="openai/gpt-test")),
    )


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


def _insert_run(connection: sqlite3.Connection, *, started_at: str) -> int:
    """Seed a `run` row at an explicit `started_at`.

    `tests.ingestion.helpers.insert_run` always stamps the same fixed base
    moment, so two calls collide on `started_at`. Rule 5 (I5) needs two runs
    strictly ordered in time -- a plan that opens in run N and settles in run
    N+1 -- so this mirrors that helper's INSERT with an explicit timestamp
    instead.
    """
    snapshot_id = insert_configuration_snapshot(connection)
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at
        )
        VALUES ('running', ?, 'Europe/Paris', ?, ?, ?)
        """,
        (snapshot_id, started_at, started_at, started_at),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return int(cursor.lastrowid)


def _person(
    connection: sqlite3.Connection, *, run_id: int, name: str, fingerprint: str
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


def _set_wikipedia_outcome(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    semantic_outcome: str,
    fingerprint: str,
    page_id: int = 9001,
) -> None:
    """Give a person a completed, current Wikipedia identity observation.

    `semantic_outcome` of `no_matching_page_found` makes the person coverage
    eligible (deterministic empty complete search, K4: no attempt/model
    inspection); `matching_page_found` stops research and needs a real
    attempt/inspection/matched page to satisfy migration 0006's outcome
    truth-table CHECK.
    """
    with immediate(connection) as conn:
        plan_id = open_wiki_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=fingerprint,
            created_at=NOW,
        )
        attempt_id: int | None = None
        inspection_id: int | None = None
        matched_page_row_id: int | None = None
        candidates = "[]"
        if semantic_outcome != "no_matching_page_found":
            work_id = conn.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    completed_by_run_id, created_at, updated_at
                ) VALUES ('match_wikipedia_identity', 'person', ?, ?, 1, 55, ?,
                          'succeeded', ?, ?, ?, ?)
                """,
                (person_id, f"w{fingerprint}"[:64], NOW, run_id, run_id, NOW, NOW),
            ).lastrowid
            attempt_id = conn.execute(
                """
                INSERT INTO attempt (
                    run_id, work_item_id, provider, operation, ordinal,
                    started_at, finished_at, outcome, request_fingerprint
                ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?,
                          'succeeded', ?)
                """,
                (run_id, work_id, NOW, NOW, f"r{fingerprint}"[:64]),
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
                (run_id, attempt_id, f"i{fingerprint}"[:64], NOW),
            ).lastrowid
            candidates = f"[{page_id}]"
            if semantic_outcome == "matching_page_found":
                matched_page_row_id = upsert_mediawiki_page(
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
            matched_mediawiki_page_id=matched_page_row_id,
            candidate_page_ids_json=candidates,
            canonical_supplied_input_json='{"task":"wikipedia_identity"}',
            validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
            prompt_hash=(_PROMPT_HASH if attempt_id is not None else None),
            schema_hash=(_SCHEMA_HASH if attempt_id is not None else None),
            schema_version=(1 if attempt_id is not None else None),
            task_fingerprint=fingerprint,
            rationale="test fixture",
            failure_category=None,
            observed_at=NOW,
        )
        conn.execute(
            "UPDATE wikipedia_identity_plan SET status = 'completed', "
            "completed_at = ? WHERE id = ?",
            (NOW, plan_id),
        )
        point_person_current_wikipedia_observation(
            conn, person_id=person_id, observation_id=obs_id
        )


def _attempt_and_inspection(
    connection: sqlite3.Connection, *, run_id: int, fingerprint: str
) -> tuple[int, int]:
    with immediate(connection) as conn:
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id,
                completed_by_run_id, created_at, updated_at
            )
            VALUES ('assess_article', 'person_article', 1, ?, 1, 70, ?,
                    'succeeded', ?, ?, ?, ?)
            """,
            (fingerprint, NOW, run_id, run_id, NOW, NOW),
        ).lastrowid
        attempt_id = conn.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            )
            VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?,
                    'succeeded', ?)
            """,
            (run_id, work_id, NOW, NOW, fingerprint),
        ).lastrowid
        inspection_id = conn.execute(
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
        ).lastrowid
    assert attempt_id is not None and inspection_id is not None
    return int(attempt_id), int(inspection_id)


def _completed_assessment(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    policy_fingerprint: str,
    article_url: str,
    task_fingerprint: str,
    article_title: str = "Test Article",
) -> int:
    """Build one full completed person-article-assessment chain."""
    with immediate(connection) as conn:
        article_id = upsert_article(
            conn, canonical_url=article_url, publisher_key="example.com", now=NOW
        )
        person_article_id = upsert_person_article(
            conn, person_id=person_id, canonical_article_id=article_id
        )
        view_id = insert_article_view(
            conn,
            canonical_article_id=article_id,
            run_id=run_id,
            attempt_id=None,
            access_kind="full",
            requested_url=article_url,
            final_url=article_url,
            title=article_title,
            dek=None,
            byline=None,
            published_at=None,
            editorial_labels_json="[]",
            main_text_blocks_json="[]",
            snippets_json="[]",
            extraction_quality="full",
            extractor_version=1,
            observed_at=NOW,
        )
    attempt_id, inspection_id = _attempt_and_inspection(
        connection, run_id=run_id, fingerprint=task_fingerprint
    )
    with immediate(connection) as conn:
        assessment_id = _insert_assessment(
            conn,
            person_article_id=person_article_id,
            person_id=person_id,
            canonical_article_id=article_id,
            plan_id=None,
            article_view_id=view_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            person_relation="same_person",
            coverage_depth="passing",
            content_types_json="[]",
            subject_relationship="editorially_independent",
            screening_rule_id="eligible.example.com",
            screening_rule_status="curated_eligible",
            source_policy_fingerprint=policy_fingerprint,
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"assessed"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=task_fingerprint,
            rationale="test assessment",
            failure_category=None,
            observed_at=NOW,
        )
    return int(assessment_id)


def _open_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    material_fingerprint: str,
    policy_fingerprint: str,
) -> int:
    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint=material_fingerprint,
            source_policy_fingerprint=policy_fingerprint,
            retrieval_target=5,
            created_at=NOW,
        )
    return plan_id


def _settle_plan(
    connection: sqlite3.Connection, *, plan_id: int, status: str, completed_at: str
) -> None:
    with immediate(connection) as conn:
        update_plan_status(
            conn, plan_id=plan_id, status=status, completed_at=completed_at
        )


def _work_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    state: str,
    fingerprint: str,
) -> int:
    with immediate(connection) as conn:
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id,
                completed_by_run_id, created_at, updated_at
            )
            VALUES ('assess_article', 'person_article', 1, ?, 1, 70, ?, ?,
                    ?, ?, ?, ?)
            """,
            (fingerprint, NOW, state, run_id, run_id, NOW, NOW),
        ).lastrowid
    assert work_id is not None
    return int(work_id)


def _report(run_id: int) -> RunReport:
    return RunReport(
        run_id=run_id,
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
    )


# ---------------------------------------------------------------------------
# Rule 1: every corpus/run counter is correct against a populated database
# ---------------------------------------------------------------------------


def test_corpus_and_run_counters_positive_for_populated_database(
    connection: sqlite3.Connection,
) -> None:
    """Every counter gets its own distinct value from real repository rows.

    A first version of this test gave every counter the value 1. That made it
    blind to the wrong-column / swapped-field defect class: a review proved
    that swapping `model_deferred` and `model_failed` in `CoverageRunCounts`'s
    return survived the entire file. So each of the nine digest counters here
    is built from a different *count* of real rows (1 through 9) -- plans
    completed/incomplete/failed, this-run assessments, corpus-wide assessed
    people, eligible-remaining people, stopped-matching people, and deferred/
    failed assess work -- so a swapped pair of fields produces a wrong number
    that this test actually notices, not two coincidentally-equal 1s.
    """
    run_id = _insert_run(connection, started_at=moment())
    policy = _policy()

    # plans_completed = 1
    for i in range(1):
        person = _person(
            connection,
            run_id=run_id,
            name=f"Completed Plan {i}",
            fingerprint=_fp("cp", i),
        )
        plan_id = _open_plan(
            connection,
            person_id=person,
            run_id=run_id,
            material_fingerprint=_fp("cpf", i),
            policy_fingerprint=policy.fingerprint,
        )
        _settle_plan(connection, plan_id=plan_id, status="completed", completed_at=NOW)

    # plans_incomplete = 2
    for i in range(2):
        person = _person(
            connection,
            run_id=run_id,
            name=f"Incomplete Plan {i}",
            fingerprint=_fp("ip", i),
        )
        plan_id = _open_plan(
            connection,
            person_id=person,
            run_id=run_id,
            material_fingerprint=_fp("ipf", i),
            policy_fingerprint=policy.fingerprint,
        )
        _settle_plan(connection, plan_id=plan_id, status="incomplete", completed_at=NOW)

    # plans_failed = 3
    for i in range(3):
        person = _person(
            connection, run_id=run_id, name=f"Failed Plan {i}", fingerprint=_fp("fp", i)
        )
        plan_id = _open_plan(
            connection,
            person_id=person,
            run_id=run_id,
            material_fingerprint=_fp("fpf", i),
            policy_fingerprint=policy.fingerprint,
        )
        _settle_plan(connection, plan_id=plan_id, status="failed", completed_at=NOW)

    # assessments_completed_this_run = 4 (four distinct people, assessed now)
    for i in range(4):
        person = _person(
            connection,
            run_id=run_id,
            name=f"Assessed This Run {i}",
            fingerprint=_fp("ar", i),
        )
        _completed_assessment(
            connection,
            person_id=person,
            run_id=run_id,
            policy_fingerprint=policy.fingerprint,
            article_url=f"https://example.com/assessed-this-run-{i}",
            task_fingerprint=_fp("atf", i),
        )

    # A fifth assessed person, completed in a *different* run, so corpus-wide
    # `people_with_completed_assessment` (5) is distinct from this-run
    # `assessments_completed_this_run` (4): the corpus counter is not scoped
    # to any run, so this person's earlier assessment still counts in it.
    other_assessment_run = _insert_run(connection, started_at=moment(-100))
    extra_assessed_person = _person(
        connection,
        run_id=other_assessment_run,
        name="Assessed Other Run",
        fingerprint=_fp("aor", 0),
    )
    _completed_assessment(
        connection,
        person_id=extra_assessed_person,
        run_id=other_assessment_run,
        policy_fingerprint=policy.fingerprint,
        article_url="https://example.com/assessed-other-run",
        task_fingerprint=_fp("aotf", 0),
    )

    # eligible_remaining = 6
    for i in range(6):
        person = _person(
            connection, run_id=run_id, name=f"Eligible {i}", fingerprint=_fp("el", i)
        )
        _set_wikipedia_outcome(
            connection,
            person_id=person,
            run_id=run_id,
            semantic_outcome="no_matching_page_found",
            fingerprint=_fp("elf", i),
        )

    # stopped_matching_wikipedia = 7
    for i in range(7):
        person = _person(
            connection, run_id=run_id, name=f"Stopped {i}", fingerprint=_fp("st", i)
        )
        _set_wikipedia_outcome(
            connection,
            person_id=person,
            run_id=run_id,
            semantic_outcome="matching_page_found",
            fingerprint=_fp("stf", i),
            page_id=9100 + i,
        )

    # model_deferred = 8
    for i in range(8):
        _work_item(
            connection, run_id=run_id, state="deferred", fingerprint=_fp("md", i)
        )

    # model_failed = 9
    for i in range(9):
        _work_item(
            connection,
            run_id=run_id,
            state="failed_permanent",
            fingerprint=_fp("mf", i),
        )

    corpus = coverage_corpus_counts(connection)
    assert corpus.assessments_completed == 5, (
        "the extra other-run assessment must not be dropped"
    )
    assert corpus.people_with_completed_assessment == 5
    assert corpus.stopped_matching_wikipedia == 7

    run_counts = coverage_run_counts(connection, run_id=run_id)
    assert run_counts.plans_completed == 1
    assert run_counts.plans_incomplete == 2
    assert run_counts.plans_failed == 3
    assert run_counts.assessments_completed_this_run == 4
    assert run_counts.model_deferred == 8
    assert run_counts.model_failed == 9

    config = _config()
    eligible = count_coverage_research_eligible(
        connection, config=config, policy=policy, now=NOW
    )
    assert eligible == 6

    # Positive control: a different run_id must not pick up this run's work.
    other_run = _insert_run(connection, started_at=moment(120))
    other = coverage_run_counts(connection, run_id=other_run)
    assert other.model_deferred == 0
    assert other.model_failed == 0
    assert other.assessments_completed_this_run == 0

    # Render the full digest section from these exact corpus/run numbers and
    # confirm every one of the nine counters lands in the rendered Markdown --
    # the specific hole this task closes (the stub never rendered anything).
    # Every value below is distinct (1..9), so a swapped-field defect such as
    # model_deferred <-> model_failed changes what this section says.
    summary = CoverageSummary(
        plans_completed=run_counts.plans_completed,
        plans_incomplete=run_counts.plans_incomplete,
        plans_failed=run_counts.plans_failed,
        assessments_completed_this_run=run_counts.assessments_completed_this_run,
        people_with_completed_assessment=corpus.people_with_completed_assessment,
        eligible_remaining=eligible,
        stopped_matching_wikipedia=corpus.stopped_matching_wikipedia,
        model_deferred=run_counts.model_deferred,
        model_failed=run_counts.model_failed,
    )
    markdown = render_digest(_report(run_id), local_date="2026-07-30", coverage=summary)
    assert "### Coverage evidence" in markdown
    section = markdown.split("### Coverage evidence\n\n", 1)[1]
    assert "- Plans completed this run: 1" in section
    assert "- Plans incomplete this run: 2" in section
    assert "- Plans permanently failed this run: 3" in section
    assert "- Assessments completed this run: 4" in section
    assert "- People with completed assessment (corpus): 5" in section
    assert "- Coverage eligible remaining: 6" in section
    assert "- Stopped matching Wikipedia: 7" in section
    assert "- Assess model deferred: 8" in section
    assert "- Assess model permanently failed: 9" in section


# ---------------------------------------------------------------------------
# Rule 2: exact heading and label wording
# ---------------------------------------------------------------------------


def test_digest_coverage_labels_render_with_exact_wording() -> None:
    """Distinct field values per label so no assertion could pass by accident
    (e.g. two labels sharing a coincidental count)."""
    summary = CoverageSummary(
        plans_completed=11,
        plans_incomplete=22,
        plans_failed=33,
        assessments_completed_this_run=44,
        people_with_completed_assessment=55,
        eligible_remaining=66,
        stopped_matching_wikipedia=77,
        model_deferred=88,
        model_failed=99,
    )
    markdown = render_digest(
        _report(run_id=1), local_date="2026-07-30", coverage=summary
    )
    assert "### Coverage evidence" in markdown
    section = markdown.split("### Coverage evidence\n\n", 1)[1]
    assert "- Plans completed this run: 11" in section
    assert "- Plans incomplete this run: 22" in section
    assert "- Plans permanently failed this run: 33" in section
    assert "- Assessments completed this run: 44" in section
    assert "- People with completed assessment (corpus): 55" in section
    assert "- Coverage eligible remaining: 66" in section
    assert "- Stopped matching Wikipedia: 77" in section
    assert "- Assess model deferred: 88" in section
    assert "- Assess model permanently failed: 99" in section


# ---------------------------------------------------------------------------
# Rule 4 (I7): people_with_completed_assessment excludes merged-away people
# ---------------------------------------------------------------------------


def test_people_with_completed_assessment_excludes_merged_away_person(
    connection: sqlite3.Connection,
) -> None:
    """I7 regression: a merged-away person's own assessment row is not
    double-counted as a second distinct person.

    Assessment rows deliberately keep the observation-time `person_id` (K18),
    so both rows survive the merge; only the *people* count must resolve to
    canonical survivors. `assessments_completed` (raw row count) staying at 2
    while `people_with_completed_assessment` drops to 1 is the positive
    control proving the merge did not delete evidence -- only the per-person
    count changed.
    """
    run_id = _insert_run(connection, started_at=moment())
    policy = _policy()

    survivor_id = _person(
        connection, run_id=run_id, name="Survivor", fingerprint="a" * 64
    )
    loser_id = _person(connection, run_id=run_id, name="Loser", fingerprint="b" * 64)

    _completed_assessment(
        connection,
        person_id=survivor_id,
        run_id=run_id,
        policy_fingerprint=policy.fingerprint,
        article_url="https://example.com/survivor",
        task_fingerprint="c" * 64,
    )
    _completed_assessment(
        connection,
        person_id=loser_id,
        run_id=run_id,
        policy_fingerprint=policy.fingerprint,
        article_url="https://example.com/loser",
        task_fingerprint="d" * 64,
    )

    with immediate(connection):
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor_id, loser_id),
        )

    corpus = coverage_corpus_counts(connection)
    assert corpus.assessments_completed == 2, (
        "merging must not delete either assessment row"
    )
    assert corpus.people_with_completed_assessment == 1


# ---------------------------------------------------------------------------
# Rule 5 (I5): plan counters credit the settling run, not the opening run
# ---------------------------------------------------------------------------


def test_plan_counters_credit_settling_run_not_opening_run(
    connection: sqlite3.Connection,
) -> None:
    """I5 regression: a plan opened in run N and settled in run N+1 counts in
    run N+1's digest, not run N's.

    Plans normally open in run N and terminalize in run N+1 (the seed batch
    that opens the plan is not the same run that drains its work to a
    terminal state), so crediting the opening run would put every plan in a
    digest that cannot yet report its outcome, and in no later one.
    """
    run_n = _insert_run(connection, started_at=moment())
    person_id = _person(
        connection, run_id=run_n, name="Settling Run Person", fingerprint="a" * 64
    )
    policy = _policy()

    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_n,
        material_fingerprint="b" * 64,
        policy_fingerprint=policy.fingerprint,
    )

    run_n_plus_1 = _insert_run(connection, started_at=moment(3600))
    # Settle strictly after run N+1 started, so the settlement is attributed
    # to run N+1, never to the opening run N.
    _settle_plan(
        connection, plan_id=plan_id, status="completed", completed_at=moment(3700)
    )

    opening_run_counts = coverage_run_counts(connection, run_id=run_n)
    settling_run_counts = coverage_run_counts(connection, run_id=run_n_plus_1)
    assert opening_run_counts.plans_completed == 0, (
        "the opening run must not claim a plan it had not yet settled"
    )
    assert settling_run_counts.plans_completed == 1


# ---------------------------------------------------------------------------
# Rule 6: the digest omits the coverage section when coverage is None, with a
# positive control proving the section appears when it is supplied
# ---------------------------------------------------------------------------


def test_digest_omits_coverage_section_when_none_but_renders_when_supplied() -> None:
    without = render_digest(_report(run_id=1), local_date="2026-07-30", coverage=None)
    assert "### Coverage evidence" not in without

    # Positive control: the exact same report, only `coverage` supplied,
    # really does render the section -- proving its absence above is not
    # simply because the heading is unreachable.
    with_coverage = render_digest(
        _report(run_id=1),
        local_date="2026-07-30",
        coverage=CoverageSummary(
            plans_completed=1,
            plans_incomplete=0,
            plans_failed=0,
            assessments_completed_this_run=1,
            people_with_completed_assessment=1,
            eligible_remaining=0,
            stopped_matching_wikipedia=0,
            model_deferred=0,
            model_failed=0,
        ),
    )
    assert "### Coverage evidence" in with_coverage


def test_render_digest_helper_matches_report_fixture_omits_coverage() -> None:
    """Mirrors the Wikipedia sibling: the shared report fixture (no coverage
    kwarg at all) never mentions the section."""
    from tests.run_engine.test_digest import report

    markdown = render_digest(report(), local_date="2026-07-30")
    assert "### Coverage evidence" not in markdown


# ---------------------------------------------------------------------------
# Rule 7: no query text, result URL, or article title leaks into the digest,
# with a positive control proving each string is really present in the
# fixture database
# ---------------------------------------------------------------------------


def test_digest_coverage_section_never_leaks_query_url_or_title(
    connection: sqlite3.Connection,
) -> None:
    run_id = _insert_run(connection, started_at=moment())
    policy = _policy()
    person_id = _person(
        connection, run_id=run_id, name="Secret Bearer", fingerprint="a" * 64
    )

    distinctive_query = "Distinctive Unlikely Query Text Marker 12345"
    distinctive_result_url = "https://example.com/distinctive-result-url-marker"
    distinctive_title = "Distinctive Unlikely Article Title Marker"

    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        material_fingerprint="b" * 64,
        policy_fingerprint=policy.fingerprint,
    )
    with immediate(connection) as conn:
        (form_id,) = insert_query_forms(
            conn,
            plan_id=plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": distinctive_query,
                },
            ),
        )
        work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id,
                completed_by_run_id, created_at, updated_at
            ) VALUES ('brave_web_search', 'coverage_query_form', ?, ?, 1, 60,
                      ?, 'succeeded', ?, ?, ?, ?)
            """,
            (form_id, "c" * 64, NOW, run_id, run_id, NOW, NOW),
        ).lastrowid
        attempt_id = conn.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (?, ?, 'brave', 'web_search', 1, ?, ?, 'succeeded', ?)
            """,
            (run_id, work_id, NOW, NOW, "d" * 64),
        ).lastrowid
        assert attempt_id is not None
        observation_id = insert_or_load_brave_search_observation_by_attempt(
            conn,
            query_form_id=form_id,
            run_id=run_id,
            attempt_id=int(attempt_id),
            query_text=distinctive_query,
            altered_query=None,
            offset_in=0,
            count_requested=10,
            result_count=1,
            truncated=False,
            response_complete=True,
            observed_at=NOW,
        )
        insert_search_result_occurrences(
            conn,
            search_observation_id=observation_id,
            occurrences=(
                {"rank": 1, "url": distinctive_result_url, "title": distinctive_title},
            ),
        )
        article_id = upsert_article(
            conn,
            canonical_url=distinctive_result_url,
            publisher_key="example.com",
            now=NOW,
        )
        insert_article_view(
            conn,
            canonical_article_id=article_id,
            run_id=run_id,
            attempt_id=None,
            access_kind="full",
            requested_url=distinctive_result_url,
            final_url=distinctive_result_url,
            title=distinctive_title,
            dek=None,
            byline=None,
            published_at=None,
            editorial_labels_json="[]",
            main_text_blocks_json="[]",
            snippets_json="[]",
            extraction_quality="full",
            extractor_version=1,
            observed_at=NOW,
        )

    # Positive control: each distinctive string really is present in the
    # fixture database, so its absence from the digest below is not vacuous.
    stored_query = connection.execute(
        "SELECT query_text FROM coverage_query_form WHERE id = ?", (form_id,)
    ).fetchone()["query_text"]
    assert stored_query == distinctive_query
    stored_url = connection.execute(
        "SELECT url FROM brave_search_result_occurrence LIMIT 1"
    ).fetchone()["url"]
    assert stored_url == distinctive_result_url
    stored_title = connection.execute(
        "SELECT title FROM article_view WHERE canonical_article_id = ?",
        (article_id,),
    ).fetchone()["title"]
    assert stored_title == distinctive_title

    corpus = coverage_corpus_counts(connection)
    run_counts = coverage_run_counts(connection, run_id=run_id)
    config = _config()
    eligible = count_coverage_research_eligible(
        connection, config=config, policy=policy, now=NOW
    )
    summary = CoverageSummary(
        plans_completed=run_counts.plans_completed,
        plans_incomplete=run_counts.plans_incomplete,
        plans_failed=run_counts.plans_failed,
        assessments_completed_this_run=run_counts.assessments_completed_this_run,
        people_with_completed_assessment=corpus.people_with_completed_assessment,
        eligible_remaining=eligible,
        stopped_matching_wikipedia=corpus.stopped_matching_wikipedia,
        model_deferred=run_counts.model_deferred,
        model_failed=run_counts.model_failed,
    )
    markdown = render_digest(_report(run_id), local_date="2026-07-30", coverage=summary)
    section = markdown.split("### Coverage evidence\n\n", 1)[1]
    assert distinctive_query not in section
    assert distinctive_result_url not in section
    assert distinctive_title not in section


# ---------------------------------------------------------------------------
# Rule 3: `notable status` prints its three coverage lines with correct values
# ---------------------------------------------------------------------------


def test_notable_status_prints_coverage_lines_with_correct_values(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)

    connection = connect_database(database)
    try:
        apply_migrations(connection, database, loaded.paths.backups)
        run_id = _insert_run(connection, started_at=moment())
        policy = _policy()

        eligible_person = _person(
            connection, run_id=run_id, name="Status Eligible", fingerprint="a" * 64
        )
        _set_wikipedia_outcome(
            connection,
            person_id=eligible_person,
            run_id=run_id,
            semantic_outcome="no_matching_page_found",
            fingerprint="b" * 64,
        )

        stopped_person = _person(
            connection, run_id=run_id, name="Status Stopped", fingerprint="c" * 64
        )
        _set_wikipedia_outcome(
            connection,
            person_id=stopped_person,
            run_id=run_id,
            semantic_outcome="matching_page_found",
            fingerprint="d" * 64,
        )

        assessed_person = _person(
            connection, run_id=run_id, name="Status Assessed", fingerprint="e" * 64
        )
        _completed_assessment(
            connection,
            person_id=assessed_person,
            run_id=run_id,
            policy_fingerprint=policy.fingerprint,
            article_url="https://example.com/status-assessed",
            task_fingerprint="f" * 64,
        )
    finally:
        connection.close()

    assert cli_main.command_status(config_file) == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "people with completed assessment (corpus): 1" in output
    assert "coverage eligible remaining: 1" in output
    assert "stopped matching wikipedia: 1" in output
