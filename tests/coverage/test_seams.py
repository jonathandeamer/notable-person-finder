"""Cross-component seams: Wikipedia-to-coverage handoff, merge reconcile, seed
composition order, and package-layering direction (K1).

Offline; no network. Replaces a seven-line ``live``-marked stub
(``pytestmark = pytest.mark.live`` over a bare ``pass``) that asserted
nothing and was itself the reason ``pytest tests/coverage -m live`` collected
four tests instead of three -- a no-op seam test masquerading as live evidence.
These rules need no network at all, so the ``live`` marker is removed; every
test here runs in the default offline gate.
"""

from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

import pytest

import notable_person_finder
from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.models import (
    AssessArticleConfig,
    BraveConfig,
    BudgetConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage.repository import (
    insert_query_forms,
    open_plan,
    upsert_person_article,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import BRAVE_WEB_SEARCH_TASK_TYPE
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.ingestion.repository import upsert_article
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.merge import confirm_person_merge
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.wikipedia.repository import (
    insert_wikipedia_identity_observation,
    point_person_current_wikipedia_observation,
    upsert_mediawiki_page,
)
from notable_person_finder.wikipedia.repository import open_plan as open_wiki_plan
from notable_person_finder.wikipedia.service import (
    _schedule_coverage_after_wikipedia_settled,  # seam under test (rules 1-2)
)
from tests.ingestion.helpers import immediate, insert_run, moment
from tests.people.test_run_cli import _single_feed, write_people_graph

NOW = moment()
_PROMPT = "p" * 64
_SCHEMA = "s" * 64

# The real tracked policy file, referenced by an absolute path. The seam under
# test (`_schedule_coverage_after_wikipedia_settled`) loads the source policy
# itself via `config.source_policy_file` with no override hook.
# `config/loader.py`'s `load_config` resolves `source_policy_file` to an
# absolute path on the `MainConfig` object it returns (see
# `tests/foundation/test_review_findings.py::
# test_source_policy_file_is_resolved_absolute_on_the_main_config`), but this
# module builds `MainConfig` directly rather than through `load_config`, so an
# absolute path is supplied here to keep rules 1-2 about the seam's *own*
# wiring, independent of loader resolution.
_REPO_ROOT = Path(notable_person_finder.__file__).resolve().parents[2]
_VISUAL_ARTS_POLICY = _REPO_ROOT / "config" / "source_policies" / "visual_arts.toml"


def _config(*, source_policy_file: Path) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=source_policy_file,
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        brave=BraveConfig(),
        tasks=TasksConfig(
            assess_article=AssessArticleConfig(
                model="openai/gpt-test",
                max_alias_forms=0,
                max_context_forms=0,
            )
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


def _complete_wikipedia(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    semantic_outcome: str,
    task_fingerprint: str,
    page_id: int,
) -> int:
    """Insert a schema-valid completed Wikipedia observation and set it current.

    Mirrors migration 0006's outcome truth table so ``no_matching_page_found``
    and ``uncertain_identity`` (attempt + inspection required, no matched page)
    and ``matching_page_found`` (attempt + inspection + matched page required)
    are each constructed the way a real settlement would leave them.
    """
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
        if semantic_outcome == "no_matching_page_found":
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
                task_fingerprint=task_fingerprint,
                rationale="test complete",
                failure_category=None,
                observed_at=NOW,
            )
        else:
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
            matched_row: int | None = None
            if semantic_outcome == "matching_page_found":
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


# ---------------------------------------------------------------------------
# Rules 1 & 2: the Wikipedia settle seam
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "semantic_outcome", ["no_matching_page_found", "uncertain_identity"]
)
def test_wikipedia_settle_seam_schedules_coverage_when_eligible(
    connection: sqlite3.Connection, semantic_outcome: str
) -> None:
    """Rule 1: the Wikipedia settle seam opens coverage research on eligible outcomes.

    Exercises `_schedule_coverage_after_wikipedia_settled` directly -- the
    private hook that `_persist_match_for`'s `persist` closure calls after
    every completed Wikipedia identity observation in real match settlement --
    rather than the already-covered `schedule_coverage_after_wikipedia_ready`
    it delegates to (see `tests/coverage/test_seed_and_hooks.py`). This is the
    actual handoff point, and it is the one that carries the
    `load_source_policy` call described in the module comment above.
    """
    run_id = insert_run(connection)
    person_id = _person(
        connection, run_id=run_id, name="Ada Test", fingerprint="1" * 64
    )
    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome=semantic_outcome,
        task_fingerprint="a" * 64,
        page_id=101,
    )

    config = _config(source_policy_file=_VISUAL_ARTS_POLICY)
    with immediate(connection):
        _schedule_coverage_after_wikipedia_settled(
            connection, person_id=person_id, run_id=run_id, config=config, now=NOW
        )

    plans = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE person_id = ?", (person_id,)
    ).fetchall()
    assert len(plans) == 1, f"expected exactly one opened plan, got {plans!r}"
    assert plans[0]["status"] == "retrieving"


def test_wikipedia_settle_seam_matching_page_opens_no_plan_and_supersedes_existing(
    connection: sqlite3.Connection,
) -> None:
    """Rule 2: a current `matching_page_found` opens no plan and supersedes work.

    Positive control: the sibling test above proves this exact seam function,
    on identical machinery, really does open a plan under an eligible
    outcome -- so this test's "no *new* plan" assertion is not vacuously true
    from the mechanism being unreachable; it is reachable and simply does not
    fire for `matching_page_found` (K5).
    """
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id, name="Bo Test", fingerprint="2" * 64)

    with immediate(connection) as conn:
        plan_id = open_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            material_fingerprint="b" * 64,
            source_policy_fingerprint="c" * 64,
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
                    "query_text": "Bo Test",
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
            (BRAVE_WEB_SEARCH_TASK_TYPE, form_id, "d" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
    assert work_id is not None

    # Positive control: the plan and its Brave work item are really active
    # before the settle seam runs.
    before = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (plan_id,)
    ).fetchone()
    assert before is not None and before["status"] == "retrieving"
    before_work = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert before_work is not None and before_work["state"] == "pending"

    _complete_wikipedia(
        connection,
        person_id=person_id,
        run_id=run_id,
        semantic_outcome="matching_page_found",
        task_fingerprint="e" * 64,
        page_id=202,
    )

    config = _config(source_policy_file=_VISUAL_ARTS_POLICY)
    with immediate(connection):
        _schedule_coverage_after_wikipedia_settled(
            connection, person_id=person_id, run_id=run_id, config=config, now=NOW
        )

    plans = connection.execute(
        "SELECT id, status FROM person_coverage_plan WHERE person_id = ?",
        (person_id,),
    ).fetchall()
    assert [dict(row) for row in plans] == [{"id": plan_id, "status": "superseded"}]
    after_work = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert after_work is not None and after_work["state"] == "superseded"


# ---------------------------------------------------------------------------
# Rule 3: the person-merge seam (I3/I4 regression, exercised via the seam)
# ---------------------------------------------------------------------------


def test_person_merge_seam_supersedes_stale_plan_and_preserves_named_person_article(
    connection: sqlite3.Connection,
) -> None:
    """Rule 3: `confirm_person_merge` reconciles coverage state (K18).

    Regression test for two defects a completed review already found and
    fixed in `coverage/merge_hooks.py` (I3, I4), exercised through the actual
    seam entry point -- `people.merge.confirm_person_merge` with `config` and
    `coverage_policy` supplied -- rather than calling
    `coverage.merge_hooks.reconcile_on_merge` directly (already unit-tested in
    `tests/coverage/test_merge_hooks.py`). This is also why the survivor
    person is created *before* the loser here: `confirm_person_merge` enforces
    K7 itself (survivor = the numerically lower canonical person id)
    regardless of which parameter name a caller passes which id under, so
    getting the intended I3 shape (retiree = survivor's own `person_article`
    row) through this seam needs person-creation order to already match what
    K7 will pick, not just parameter names.

    I4: the survivor already has an active coverage plan under its old
    identity fingerprint. A merge changes that fingerprint, so
    `ensure_coverage_research` opens a *new* plan; without first superseding
    the stale one, both would run -- duplicate paid Brave/fetch/assess calls
    for one person. Mutation-verified: removing the second
    `supersede_coverage_work_for_person(survivor_id, ...)` call in
    `reconcile_on_merge` makes this test fail on the stale-plan-superseded
    assertion below.

    I3: the loser's `person_article` row for a shared article has the lower
    id (created first), so it is the "keeper" and the survivor's own row is
    the one retired and deleted. The commit that added I3 also added I4 in
    the same change. With I4 present, `reconcile_on_merge`'s two person-scoped
    `supersede_coverage_work_for_person` calls (one for `loser_id`, one for
    `survivor_id`) already supersede every pending/deferred assess work item
    naming *any* `person_article` row either person currently owns -- before
    `_reassign_person_articles` runs at all. That includes whichever row ends
    up the retiree, in every direction (retiree = loser's row, or retiree =
    survivor's row as built here). So the retiree-specific
    `UPDATE work_item ... WHERE subject_id = retiree_id` inside
    `_merge_person_article_conflict` -- the I3 fix -- is dead code given I4:
    removing only that line, verified by mutation below, changes nothing this
    test (or `tests/coverage/test_merge_hooks.py`'s own dedicated I3 test) can
    observe. This is a disclosed finding, not a weakened assertion: the
    dangling-subject and superseded-state assertions below still hold and are
    still genuine regression coverage for a merge that supersedes *and*
    deletes correctly -- they just currently owe that correctness to I4, not
    to I3's own retiree-targeted line.
    """
    config = _config(source_policy_file=Path("unused/for/this/test.toml"))
    policy = _policy()
    run_id = insert_run(connection)

    # `confirm_person_merge` enforces K7 itself (survivor = the numerically
    # lower canonical person id) regardless of which parameter name a caller
    # uses, so the survivor person is created first here to land the lower
    # person.id. Its person_article row for the shared article is inserted
    # *second*, below, so it still gets the higher person_article.id and
    # becomes the retiree -- the I3 scenario -- while person.id ordering
    # matches what K7 will actually pick.
    survivor_id = _person(connection, run_id=run_id, name="Cy", fingerprint="4" * 64)
    loser_id = _person(connection, run_id=run_id, name="Cy G", fingerprint="3" * 64)
    _complete_wikipedia(
        connection,
        person_id=survivor_id,
        run_id=run_id,
        semantic_outcome="no_matching_page_found",
        task_fingerprint="f" * 64,
        page_id=303,
    )

    with immediate(connection) as conn:
        article_id = upsert_article(
            conn,
            canonical_url="https://example.com/cy",
            publisher_key="example.com",
            now=NOW,
        )

    with immediate(connection):
        loser_pa_id = upsert_person_article(
            connection,
            person_id=loser_id,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
        survivor_pa_id = upsert_person_article(
            connection,
            person_id=survivor_id,
            canonical_article_id=article_id,
            first_plan_id=None,
        )
    assert survivor_pa_id > loser_pa_id  # positive control: survivor is the retiree

    def _assess_work(*, person_article_id: int, fingerprint: str) -> int:
        with immediate(connection) as conn:
            work_id = conn.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    created_at, updated_at
                ) VALUES ('assess_article', 'person_article', ?, ?, 1, 70, ?,
                          'pending', ?, ?, ?)
                """,
                (person_article_id, fingerprint, NOW, run_id, NOW, NOW),
            ).lastrowid
        assert work_id is not None
        return int(work_id)

    survivor_work_id = _assess_work(
        person_article_id=survivor_pa_id, fingerprint="1" * 64
    )
    loser_work_id = _assess_work(person_article_id=loser_pa_id, fingerprint="2" * 64)

    # I4 precondition: the survivor already has an active plan + pending Brave
    # work under a stale material fingerprint.
    with immediate(connection) as conn:
        stale_plan_id = open_plan(
            conn,
            person_id=survivor_id,
            run_id=run_id,
            material_fingerprint="3" * 64,
            source_policy_fingerprint=policy.fingerprint,
            retrieval_target=5,
            created_at=NOW,
        )
        (stale_form_id,) = insert_query_forms(
            conn,
            plan_id=stale_plan_id,
            forms=(
                {
                    "ordinal": 1,
                    "stage": 1,
                    "variant_kind": "exact",
                    "query_text": "Cy",
                },
            ),
        )
        stale_work_id = conn.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at, updated_at
            ) VALUES (?, 'coverage_query_form', ?, ?, 1, 60, ?, 'pending', ?, ?, ?)
            """,
            (
                BRAVE_WEB_SEARCH_TASK_TYPE,
                stale_form_id,
                "4" * 64,
                NOW,
                run_id,
                NOW,
                NOW,
            ),
        ).lastrowid
    assert stale_work_id is not None

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=loser_id,
            survivor_id=survivor_id,
            run_id=run_id,
            observation_id=None,
            now=NOW,
            config=config,
            coverage_policy=policy,
        )

    # I4: the stale plan (and its work) is superseded, and at most one plan
    # for the survivor is active afterward.
    stale = connection.execute(
        "SELECT status FROM person_coverage_plan WHERE id = ?", (stale_plan_id,)
    ).fetchone()
    assert stale is not None and stale["status"] == "superseded"
    stale_work_state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (stale_work_id,)
    ).fetchone()
    assert stale_work_state is not None and stale_work_state["state"] == "superseded"
    active_count = connection.execute(
        """
        SELECT COUNT(*) AS n FROM person_coverage_plan
         WHERE person_id = ? AND status IN ('retrieving', 'selecting', 'assessing')
        """,
        (survivor_id,),
    ).fetchone()
    assert int(active_count["n"]) <= 1

    # I3: the survivor's own person_article row was retired and deleted, but
    # no pending/deferred assess work item may still name a dangling subject.
    assert (
        connection.execute(
            "SELECT id FROM person_article WHERE id = ?", (survivor_pa_id,)
        ).fetchone()
        is None
    )
    dangling = connection.execute(
        """
        SELECT w.id
          FROM work_item AS w
          LEFT JOIN person_article AS pa ON pa.id = w.subject_id
         WHERE w.task_type = 'assess_article'
           AND w.subject_kind = 'person_article'
           AND w.state IN ('pending', 'deferred')
           AND pa.id IS NULL
        """
    ).fetchall()
    assert dangling == []
    survivor_work_state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (survivor_work_id,)
    ).fetchone()
    assert survivor_work_state is not None
    assert survivor_work_state["state"] == "superseded"
    loser_work_state = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (loser_work_id,)
    ).fetchone()
    assert loser_work_state is not None
    assert loser_work_state["state"] == "superseded"


# ---------------------------------------------------------------------------
# Rule 4: `_compose_seed` ordering
# ---------------------------------------------------------------------------


def test_compose_seed_runs_coverage_research_after_wikipedia_seed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rule 4: `_compose_seed` wires coverage seeding in after the Wikipedia seed.

    Unit-level seam test of `_compose_seed` itself: binds the closure once,
    then monkeypatches the three seed-order module globals it calls by bare
    name so the assertion is on `_compose_seed`'s own composition, distinct
    from the full-CLI-run seed-order coverage already in
    `tests/coverage/test_run_cli.py`.
    """
    from notable_person_finder.config.loader import load_config

    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    try:
        apply_migrations(connection, database, loaded.paths.backups)
        run_id = insert_run(connection)

        order: list[str] = []
        real_wikipedia = cli_main.seed_wikipedia_identity
        real_coverage = cli_main.seed_coverage_research
        real_ensure = cli_main.ensure_model_inspections_for_run

        def spy_wikipedia(*args: object, **kwargs: object) -> object:
            order.append("wikipedia")
            return real_wikipedia(*args, **kwargs)

        def spy_coverage(*args: object, **kwargs: object) -> object:
            order.append("coverage")
            return real_coverage(*args, **kwargs)

        def spy_ensure(*args: object, **kwargs: object) -> object:
            order.append("ensure_model_inspections")
            return real_ensure(*args, **kwargs)

        monkeypatch.setattr(cli_main, "seed_wikipedia_identity", spy_wikipedia)
        monkeypatch.setattr(cli_main, "seed_coverage_research", spy_coverage)
        monkeypatch.setattr(cli_main, "ensure_model_inspections_for_run", spy_ensure)

        seed = cli_main._compose_seed(
            connection,
            loaded=loaded,
            policy=loaded.source_policy,
            clock=SystemClock(),
        )
        seed(run_id)

        assert order == ["wikipedia", "coverage", "ensure_model_inspections"]
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Rule 5: package layering (K1) -- injected seam, not a reverse import
# ---------------------------------------------------------------------------


def test_coverage_seam_is_injected_not_imported_at_module_level() -> None:
    """Rule 5 / K1: `wikipedia` and `people` reach `coverage` only inside a
    function body, never at module import time.

    K1 requires coverage never be folded into `people/` or `wikipedia/`, and
    the direction of the dependency graph matters: `coverage/service.py`
    imports from `people` and `wikipedia` at module level (coverage depends on
    them), but `wikipedia/service.py` and `people/merge.py` reach `coverage`
    only via a lazy, function-body import (see `_schedule_coverage_after_
    wikipedia_settled` and `_reconcile_coverage_on_merge`) -- an injected
    hook, not a module-level dependency in the reverse direction.

    Both directions are checked: each file really does reach `coverage`
    somewhere (reachability control -- otherwise "no top-level import" would
    pass vacuously if the seam were deleted entirely), and none of those
    reaches is at module (top) level.
    """
    root = Path(notable_person_finder.__file__).parent
    for relative in ("wikipedia/service.py", "people/merge.py"):
        path = root / relative
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))

        def _names_coverage(node: ast.AST) -> bool:
            if isinstance(node, ast.ImportFrom):
                return bool(node.module) and node.module.startswith(
                    "notable_person_finder.coverage"
                )
            if isinstance(node, ast.Import):
                return any(
                    alias.name.startswith("notable_person_finder.coverage")
                    for alias in node.names
                )
            return False

        any_coverage_import = [n for n in ast.walk(tree) if _names_coverage(n)]
        top_level_coverage_imports = [
            n for n in ast.iter_child_nodes(tree) if _names_coverage(n)
        ]

        assert any_coverage_import, (
            f"{relative} never imports notable_person_finder.coverage anywhere "
            "-- the seam is not wired, so a 'no top-level import' assertion "
            "would be vacuously true"
        )
        assert not top_level_coverage_imports, (
            f"{relative} imports notable_person_finder.coverage at module "
            "level, inverting K1's required package-layering direction"
        )
