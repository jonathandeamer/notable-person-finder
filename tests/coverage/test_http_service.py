"""Plan lifecycle + brave_web_search: discovery, offsets, T1/T3/T10/T11."""

from __future__ import annotations

import inspect
import sqlite3
import uuid
from pathlib import Path

import pytest

from notable_person_finder.config.models import (
    AssessArticleConfig,
    BraveConfig,
    BudgetConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.coverage import service as coverage_service
from notable_person_finder.coverage.repository import (
    list_coverage_article_targets_for_plan,
    list_coverage_discovery_articles_for_plan,
    list_query_forms_for_plan,
    load_plan,
    update_coverage_article_target,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import (
    BRAVE_WEB_SEARCH_TASK_TYPE,
    FETCH_ARTICLE_TASK_TYPE,
    brave_web_search_fingerprint,
    build_brave_web_search_handler,
    maybe_advance_coverage_plan,
    open_coverage_plan,
    schedule_brave_web_search,
)
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.providers.brave import (
    OPERATION_SEARCH_WEB,
    PROVIDER,
    SearchResult,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs.models import WorkItem, WorkState
from tests.coverage.fakes_brave import FakeWebSearchClient, sample_search_page
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
NOW = moment()
MODEL = "openai/gpt-test"


def _policy(
    *,
    eligible_hosts: tuple[str, ...] = ("example.com", "artnews.com"),
    ineligible_hosts: tuple[str, ...] = ("twitter.com",),
):
    rules: list[dict[str, object]] = []
    for host in eligible_hosts:
        rules.append(
            {
                "id": f"eligible.{host}",
                "status": "curated_eligible",
                "match": {"host_suffix": host},
                "rationale": "test eligible",
                "review_date": "2026-07-24",
            }
        )
    for host in ineligible_hosts:
        rules.append(
            {
                "id": f"ineligible.{host}",
                "status": "curated_ineligible",
                "match": {"host_suffix": host},
                "rationale": "test ineligible",
                "review_date": "2026-07-24",
            }
        )
    return source_policy_from_mapping(
        {
            "schema_version": 1,
            "key": "test-sources",
            "label": "Test policy",
            "rules": rules,
        }
    )


def _main_config(
    *,
    max_exact_forms: int = 4,
    max_alias_forms: int = 0,
    max_context_forms: int = 0,
    search_count: int = 10,
    max_offsets_per_form: int = 0,
    max_results_per_form: int = 20,
    retrieval_target: int = 5,
    max_eligible_fetches: int = 8,
    max_unclassified_fetches: int = 2,
    reject_altered_query: bool = False,
) -> MainConfig:
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
                max_exact_forms=max_exact_forms,
                max_alias_forms=max_alias_forms,
                max_context_forms=max_context_forms,
                search_count=search_count,
                max_offsets_per_form=max_offsets_per_form,
                max_results_per_form=max_results_per_form,
                retrieval_target=retrieval_target,
                max_eligible_fetches=max_eligible_fetches,
                max_unclassified_fetches=max_unclassified_fetches,
                reject_altered_query=reject_altered_query,
            )
        ),
    )


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
    person_id = cursor.lastrowid
    search = mechanical_search_name(name)
    connection.execute(
        """
        INSERT INTO sourced_name (
            person_id, exact_name, search_name, match_key, kind, origin_kind,
            first_observed_at, last_observed_at
        ) VALUES (?, ?, ?, ?, 'display', 'person_mention', ?, ?)
        """,
        (person_id, name, search, match_key(name), NOW, NOW),
    )
    connection.commit()
    return person_id


def _add_alias(connection: sqlite3.Connection, *, person_id: int, alias: str) -> None:
    search = mechanical_search_name(alias)
    connection.execute(
        """
        INSERT INTO sourced_name (
            person_id, exact_name, search_name, match_key, kind, origin_kind,
            first_observed_at, last_observed_at
        ) VALUES (?, ?, ?, ?, 'alias', 'person_mention', ?, ?)
        """,
        (person_id, alias, search, match_key(alias), NOW, NOW),
    )
    connection.commit()


def _seed_mention_with_url(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    person_id: int,
    original_url: str | None,
    exact_name: str = "Alex Smith",
    death_fact: str | None = None,
) -> int:
    feed_key = f"feed-{person_id}-{run_id}-{uuid.uuid4().hex[:12]}"
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Test Feed', 'https://example.com/feed', ?, ?)
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
            source_entry_id, original_url, title_text, discovered_at
        ) VALUES (?, ?, ?, ?, ?, 'Article', ?)
        """,
        (feed, fetch, run_id, f"entry-{feed}", original_url, NOW),
    ).lastrowid
    assert source_item_id is not None
    detect_fp = uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES (
            'detect_people', 'source_item', ?, ?, 1, 40, ?,
            'succeeded', ?, ?, ?
        )
        """,
        (source_item_id, detect_fp, NOW, run_id, NOW, NOW),
    ).lastrowid
    assert work is not None
    attempt = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work, NOW, moment(1), detect_fp),
    ).lastrowid
    assert attempt is not None
    routing_fp = uuid.uuid4().hex + uuid.uuid4().hex
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
        (run_id, attempt, routing_fp, NOW),
    ).lastrowid
    assert inspection is not None
    task_fp = uuid.uuid4().hex + uuid.uuid4().hex  # 64 hex
    observation_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt,
            inspection,
            _HASH,
            _OTHER_HASH,
            task_fp,
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale, person_id
        ) VALUES (?, 1, ?, ?, 'research', '[]', 'reason', ?)
        """,
        (
            observation_id,
            exact_name,
            mechanical_search_name(exact_name),
            person_id,
        ),
    ).lastrowid
    assert mention_id is not None
    if death_fact is not None:
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, 'death-1', 'era_or_date', ?, '[]')
            """,
            (mention_id, death_fact),
        )
    connection.commit()
    return mention_id


def _work_item_row(connection: sqlite3.Connection, *, work_id: int) -> WorkItem:
    row = connection.execute(
        "SELECT * FROM work_item WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row is not None
    return WorkItem(
        id=int(row["id"]),
        task_type=str(row["task_type"]),
        subject_kind=str(row["subject_kind"]),
        subject_id=int(row["subject_id"]) if row["subject_id"] is not None else None,
        fingerprint=str(row["fingerprint"]),
        required=bool(row["required"]),
        priority=int(row["priority"]),
        state=WorkState(str(row["state"])),
    )


def _insert_running_attempt(
    connection: sqlite3.Connection,
    *,
    work_id: int,
    run_id: int,
) -> int:
    row = connection.execute(
        "SELECT fingerprint FROM work_item WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row is not None
    fingerprint = str(row["fingerprint"])
    connection.execute(
        """
        UPDATE work_item
           SET state = 'running', updated_at = ?
         WHERE id = ?
        """,
        (NOW, work_id),
    )
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, ?, ?, 1, ?, NULL, NULL, ?)
        """,
        (run_id, work_id, PROVIDER, OPERATION_SEARCH_WEB, NOW, fingerprint),
    )
    connection.commit()
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _settle_work_succeeded(connection: sqlite3.Connection, *, work_id: int) -> None:
    connection.execute(
        """
        UPDATE work_item
           SET state = 'succeeded', updated_at = ?
         WHERE id = ?
        """,
        (NOW, work_id),
    )
    connection.execute(
        """
        UPDATE attempt
           SET finished_at = ?, outcome = 'succeeded'
         WHERE work_item_id = ? AND finished_at IS NULL
        """,
        (moment(1), work_id),
    )
    connection.commit()


def _settle_work_failed_permanent(
    connection: sqlite3.Connection, *, work_id: int
) -> None:
    connection.execute(
        """
        UPDATE work_item
           SET state = 'failed_permanent', reason = 'permanent', updated_at = ?
         WHERE id = ?
        """,
        (NOW, work_id),
    )
    connection.execute(
        """
        UPDATE attempt
           SET finished_at = ?, outcome = 'failed', failure_category = 'configuration'
         WHERE work_item_id = ? AND finished_at IS NULL
        """,
        (moment(1), work_id),
    )
    connection.commit()


def _run_brave(
    connection: sqlite3.Connection,
    *,
    client: FakeWebSearchClient,
    config: MainConfig,
    policy,
    form_id: int,
    material_fingerprint: str,
    run_id: int,
    offset_in: int = 0,
) -> WorkItem:
    with immediate(connection) as conn:
        work_id = schedule_brave_web_search(
            conn,
            form_id=form_id,
            material_fingerprint=material_fingerprint,
            offset_in=offset_in,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_brave_web_search_handler(
        connection, client=client, config=config, policy=policy
    )
    assert handler.prepare is not None
    assert handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(work, outcome)
    _settle_work_succeeded(connection, work_id=work_id)
    return work


def _pending_brave_form_ids(connection: sqlite3.Connection) -> list[int]:
    rows = connection.execute(
        """
        SELECT subject_id FROM work_item
         WHERE task_type = ?
           AND state = 'pending'
         ORDER BY id
        """,
        (BRAVE_WEB_SEARCH_TASK_TYPE,),
    ).fetchall()
    return [int(r["subject_id"]) for r in rows]


def _count_lead_rows(connection: sqlite3.Connection) -> int:
    # m5 must never invent lead outcomes; table may not exist yet.
    try:
        row = connection.execute("SELECT COUNT(*) AS n FROM person_lead").fetchone()
    except sqlite3.OperationalError:
        return 0
    return int(row["n"]) if row is not None else 0


def _open_plan(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy,
    material_fingerprint: str = _HASH,
) -> int:
    with immediate(connection) as conn:
        return open_coverage_plan(
            conn,
            person_id=person_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=NOW,
            material_fingerprint=material_fingerprint,
        )


# ---------------------------------------------------------------------------
# Discovery matrix (K10)
# ---------------------------------------------------------------------------


def test_plan_open_discovery_usable_creates_discovery_and_screening(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url="https://www.artnews.com/article/alex",
    )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    discovery = list_coverage_discovery_articles_for_plan(connection, plan_id=plan_id)
    assert len(discovery) == 1
    assert discovery[0].screening_id is not None
    screening = connection.execute(
        "SELECT * FROM source_screening WHERE id = ?",
        (discovery[0].screening_id,),
    ).fetchone()
    assert screening is not None
    assert screening["rule_status"] == "curated_eligible"
    assert screening["canonical_article_id"] is not None
    # Exact form still scheduled even when discovery meets a low target.
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert any(f.variant_kind == "exact" for f in forms)


def test_plan_open_discovery_unusable_url_screening_only_no_discovery(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url="ftp://bad.example/path",
    )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    assert list_coverage_discovery_articles_for_plan(connection, plan_id=plan_id) == ()
    unusable = connection.execute(
        """
        SELECT * FROM source_screening
         WHERE plan_id = ? AND rule_status = 'unusable'
        """,
        (plan_id,),
    ).fetchall()
    assert len(unusable) == 1
    assert unusable[0]["canonical_article_id"] is None
    assert unusable[0]["source_item_id"] is not None


def test_plan_open_discovery_empty_url_neither_row(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url=None,
    )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    assert list_coverage_discovery_articles_for_plan(connection, plan_id=plan_id) == ()
    count = connection.execute(
        "SELECT COUNT(*) AS n FROM source_screening WHERE plan_id = ?",
        (plan_id,),
    ).fetchone()
    assert count is not None
    assert int(count["n"]) == 0


def test_exact_always_even_when_discovery_meets_target(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(retrieval_target=1, max_eligible_fetches=8)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    for index in range(2):
        _seed_mention_with_url(
            connection,
            run_id=run_id,
            person_id=person_id,
            original_url=f"https://www.artnews.com/article/{index}",
            exact_name="Alex Smith",
        )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.eligible_selected_count >= 1
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert any(f.variant_kind == "exact" and f.status == "pending" for f in forms)
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending'
        """,
        (BRAVE_WEB_SEARCH_TASK_TYPE,),
    ).fetchone()
    assert pending is not None
    assert int(pending["n"]) >= 1


# ---------------------------------------------------------------------------
# Brave handler: one call, UNIQUE attempt, offsets
# ---------------------------------------------------------------------------


def test_one_search_web_per_execute_and_unique_attempt(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form_id = forms[0].id
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=forms[0].query_text,
                results=(
                    SearchResult(
                        rank=1,
                        url="https://example.com/a",
                        title="A",
                        snippet="s",
                        extra_snippets=(),
                        language="en",
                        provider_result_id=None,
                    ),
                ),
            )
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form_id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    assert len(client.calls) == 1
    assert client.calls[0]["offset"] == 0
    obs_count = connection.execute(
        "SELECT COUNT(*) AS n FROM brave_search_observation"
    ).fetchone()
    assert obs_count is not None
    assert int(obs_count["n"]) == 1
    # Re-persist with same attempt must not double insert (handler path for
    # idempotent re-settle is covered via insert_or_load UNIQUE).
    attempts = connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()
    assert attempts is not None
    assert int(attempts["n"]) == 1


def test_max_offsets_zero_is_single_page_only(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(max_offsets_per_form=0, search_count=10)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form_id = forms[0].id
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=forms[0].query_text,
                more_results=True,
                results=(),
            )
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form_id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "completed"
    assert form.offsets_used == 0
    assert form.truncated is True
    # No second page scheduled.
    pending = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending'
        """,
        (BRAVE_WEB_SEARCH_TASK_TYPE,),
    ).fetchone()
    assert pending is not None
    assert int(pending["n"]) == 0


def test_max_offsets_one_schedules_second_page(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(max_offsets_per_form=1, search_count=10)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form_id = forms[0].id
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=forms[0].query_text,
                more_results=True,
                results=(),
            ),
            sample_search_page(
                query=forms[0].query_text,
                offset=10,
                more_results=False,
                results=(),
            ),
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form_id,
        material_fingerprint=_HASH,
        run_id=run_id,
        offset_in=0,
    )
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "pending"
    cont = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
        """,
        (BRAVE_WEB_SEARCH_TASK_TYPE, form_id),
    ).fetchone()
    assert cont is not None
    expected = brave_web_search_fingerprint(
        material_fingerprint=_HASH,
        query_form_id=form_id,
        offset_in=10,
    )
    assert cont["fingerprint"] == expected
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form_id,
        material_fingerprint=_HASH,
        run_id=run_id,
        offset_in=10,
    )
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "completed"
    assert form.offsets_used == 1
    assert form.truncated is False
    assert len(client.calls) == 2
    assert client.calls[1]["offset"] == 10


# ---------------------------------------------------------------------------
# Terminal truth table rows
# ---------------------------------------------------------------------------


def test_t1_empty_complete_completed_zero_leads_zero_assess(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(max_alias_forms=0, max_context_forms=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=f.query_text, results=()) for f in forms]
    )
    for form in forms:
        _run_brave(
            connection,
            client=client,
            config=config,
            policy=policy,
            form_id=form.id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert plan.truncated_unsafe is False
    assert plan.partial_retrieval is False
    assert plan.failure_category is None
    assert plan.completed_at is not None
    assert list_coverage_article_targets_for_plan(connection, plan_id=plan_id) == ()
    assess_work = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = 'assess_article'
        """
    ).fetchone()
    assert assess_work is not None
    assert int(assess_work["n"]) == 0
    assert _count_lead_rows(connection) == 0


def test_t3_truncated_empty_is_incomplete_not_empty_claim(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(
        max_offsets_per_form=0,
        max_alias_forms=0,
        max_context_forms=0,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(query=f.query_text, more_results=True, results=())
            for f in forms
        ]
    )
    for form in forms:
        _run_brave(
            connection,
            client=client,
            config=config,
            policy=policy,
            form_id=form.id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "incomplete"
    assert plan.truncated_unsafe is True
    assert plan.failure_category == "unsafe_truncation"
    assert _count_lead_rows(connection) == 0


def test_t11_mixed_form_failure_empty_selection(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(max_exact_forms=2, max_alias_forms=0, max_context_forms=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id, name="Alex Smith")
    # Force two exact forms via second display-ish name.
    connection.execute(
        """
        INSERT INTO sourced_name (
            person_id, exact_name, search_name, match_key, kind, origin_kind,
            first_observed_at, last_observed_at
        ) VALUES (?, 'Alexandra Smith', ?, ?, 'professional', 'person_mention', ?, ?)
        """,
        (
            person_id,
            mechanical_search_name("Alexandra Smith"),
            match_key("Alexandra Smith"),
            NOW,
            NOW,
        ),
    )
    connection.commit()
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert len(forms) >= 2
    # Complete first form empty; permanently fail second.
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=forms[0].query_text, results=())]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=forms[0].id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    # Fail second form permanently via handler failure path.
    with immediate(connection) as conn:
        work_id = schedule_brave_web_search(
            conn,
            form_id=forms[1].id,
            material_fingerprint=_HASH,
            offset_in=0,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_brave_web_search_handler(
        connection,
        client=FakeWebSearchClient(
            pages=[
                ProviderFailure(
                    FailureCategory.CONFIGURATION,
                    provider=PROVIDER,
                    operation=OPERATION_SEARCH_WEB,
                    detail="bad key",
                )
            ]
        ),
        config=config,
        policy=policy,
    )
    assert handler.prepare is not None
    assert handler.persist_failure is not None
    prepared = handler.prepare(work)
    with pytest.raises(ProviderFailure):
        handler.execute(work, 1, prepared.payload)
    _settle_work_failed_permanent(connection, work_id=work_id)
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=PROVIDER,
                operation=OPERATION_SEARCH_WEB,
                detail="bad key",
            ),
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert plan.failure_category == "partial_retrieval_empty"
    assert plan.partial_retrieval is True
    assert plan.truncated_unsafe is False


def test_t10_discovery_salvage_completed_partial_retrieval(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(
        retrieval_target=5,
        max_alias_forms=0,
        max_context_forms=0,
        max_eligible_fetches=8,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url="https://www.artnews.com/article/discovery-only",
    )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    # Permanent-fail exact forms (no usable search occurrences).
    for form in forms:
        with immediate(connection) as conn:
            work_id = schedule_brave_web_search(
                conn,
                form_id=form.id,
                material_fingerprint=_HASH,
                offset_in=0,
                run_id=run_id,
                now=NOW,
            )
        work = _work_item_row(connection, work_id=work_id)
        _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
        handler = build_brave_web_search_handler(
            connection,
            client=FakeWebSearchClient(
                pages=[
                    ProviderFailure(
                        FailureCategory.AUTHENTICATION,
                        provider=PROVIDER,
                        operation=OPERATION_SEARCH_WEB,
                        detail="auth",
                    )
                ]
            ),
            config=config,
            policy=policy,
        )
        assert handler.prepare is not None
        assert handler.persist_failure is not None
        prepared = handler.prepare(work)
        with pytest.raises(ProviderFailure):
            handler.execute(work, 1, prepared.payload)
        _settle_work_failed_permanent(connection, work_id=work_id)
        with immediate(connection):
            handler.persist_failure(
                work,
                ProviderFailure(
                    FailureCategory.AUTHENTICATION,
                    provider=PROVIDER,
                    operation=OPERATION_SEARCH_WEB,
                    detail="auth",
                ),
            )

    # Final selection should create discovery target + fetch work.
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) >= 1
    assert all(t.selection_reason.startswith("discovery_") for t in targets), [
        t.selection_reason for t in targets
    ]
    # Simulate fetch/assess terminal without Task 6b/7 handlers.
    with immediate(connection) as conn:
        for target in targets:
            update_coverage_article_target(
                conn, target_id=target.id, status="failed", failure_category="timeout"
            )
        maybe_advance_coverage_plan(
            conn,
            plan_id=plan_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=moment(2),
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert plan.partial_retrieval is True
    assert plan.failure_category is None
    assert _count_lead_rows(connection) == 0


def test_t6_all_exact_failed_no_discovery_is_failed(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(max_alias_forms=0, max_context_forms=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    for form in forms:
        with immediate(connection) as conn:
            work_id = schedule_brave_web_search(
                conn,
                form_id=form.id,
                material_fingerprint=_HASH,
                offset_in=0,
                run_id=run_id,
                now=NOW,
            )
        work = _work_item_row(connection, work_id=work_id)
        _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
        handler = build_brave_web_search_handler(
            connection,
            client=FakeWebSearchClient(
                pages=[
                    ProviderFailure(
                        FailureCategory.AUTHENTICATION,
                        provider=PROVIDER,
                        operation=OPERATION_SEARCH_WEB,
                        detail="auth",
                    )
                ]
            ),
            config=config,
            policy=policy,
        )
        assert handler.prepare is not None
        assert handler.persist_failure is not None
        prepared = handler.prepare(work)
        with pytest.raises(ProviderFailure):
            handler.execute(work, 1, prepared.payload)
        _settle_work_failed_permanent(connection, work_id=work_id)
        with immediate(connection):
            handler.persist_failure(
                work,
                ProviderFailure(
                    FailureCategory.AUTHENTICATION,
                    provider=PROVIDER,
                    operation=OPERATION_SEARCH_WEB,
                    detail="auth",
                ),
            )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert plan.partial_retrieval is True
    assert list_coverage_article_targets_for_plan(connection, plan_id=plan_id) == ()


def test_reject_altered_query_fails_form(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config(
        reject_altered_query=True,
        max_alias_forms=0,
        max_context_forms=0,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form = forms[0]
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=form.query_text,
                altered_query="alex smith",
                results=(),
            )
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    form_row = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form_row.status == "failed"
    assert form_row.failure_category == "altered_query"
    obs = connection.execute(
        "SELECT altered_query FROM brave_search_observation WHERE query_form_id = ?",
        (form.id,),
    ).fetchone()
    assert obs is not None
    assert obs["altered_query"] == "alex smith"


def test_alias_gated_when_exact_meets_target_via_discovery(
    connection: sqlite3.Connection,
) -> None:
    """Alias not scheduled when discovery already meets retrieval_target after exact."""
    policy = _policy()
    config = _main_config(
        retrieval_target=1,
        max_alias_forms=4,
        max_context_forms=0,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _add_alias(connection, person_id=person_id, alias="A. Smith")
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url="https://www.artnews.com/article/enough",
    )
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    # Only stage-1 exact at open (alias deferred).
    assert all(f.stage == 1 for f in forms)
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=f.query_text, results=()) for f in forms]
    )
    for form in forms:
        _run_brave(
            connection,
            client=client,
            config=config,
            policy=policy,
            form_id=form.id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )
    forms_after = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert not any(f.variant_kind == "alias" for f in forms_after)


def test_alias_scheduled_when_exact_and_discovery_under_target(
    connection: sqlite3.Connection,
) -> None:
    """Positive gate: under retrieval_target after empty exact → stage-2 alias."""
    policy = _policy()
    config = _main_config(
        retrieval_target=5,
        max_alias_forms=2,
        max_context_forms=0,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _add_alias(connection, person_id=person_id, alias="A. Smith")
    # No discovery URL → eligible_selected_count stays 0 after empty exact.
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert all(f.stage == 1 for f in forms)
    assert not any(f.variant_kind == "alias" for f in forms)
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=f.query_text, results=()) for f in forms]
    )
    for form in forms:
        _run_brave(
            connection,
            client=client,
            config=config,
            policy=policy,
            form_id=form.id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )
    forms_after = list_query_forms_for_plan(connection, plan_id=plan_id)
    alias_forms = [f for f in forms_after if f.variant_kind == "alias"]
    assert alias_forms, "alias forms must open when under retrieval_target"
    assert all(f.stage == 2 and f.status == "pending" for f in alias_forms)
    pending_alias_work = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending'
           AND subject_id IN ({})
        """.format(",".join("?" for _ in alias_forms)),
        (BRAVE_WEB_SEARCH_TASK_TYPE, *(f.id for f in alias_forms)),
    ).fetchone()
    assert pending_alias_work is not None
    assert int(pending_alias_work["n"]) == len(alias_forms)
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "retrieving"


def test_context_scheduled_when_still_under_target_without_aliases(
    connection: sqlite3.Connection,
) -> None:
    """Positive gate: no aliases, empty exact → one context form when facts exist."""
    policy = _policy()
    config = _main_config(
        retrieval_target=5,
        max_alias_forms=0,
        max_context_forms=1,
    )
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    # Context terms come from operational identity facts (profession).
    _seed_mention_with_url(
        connection,
        run_id=run_id,
        person_id=person_id,
        original_url=None,
        exact_name="Alex Smith",
    )
    mention_id = connection.execute(
        "SELECT id FROM person_mention WHERE person_id = ? ORDER BY id LIMIT 1",
        (person_id,),
    ).fetchone()
    assert mention_id is not None
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value,
            supporting_passage_ids_json
        ) VALUES (?, 'prof-1', 'profession_or_role', 'painter', '[]')
        """,
        (int(mention_id["id"]),),
    )
    connection.commit()
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert all(f.stage == 1 for f in forms)
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=f.query_text, results=()) for f in forms]
    )
    for form in forms:
        _run_brave(
            connection,
            client=client,
            config=config,
            policy=policy,
            form_id=form.id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )
    forms_after = list_query_forms_for_plan(connection, plan_id=plan_id)
    context_forms = [f for f in forms_after if f.variant_kind == "context"]
    assert len(context_forms) == 1
    assert context_forms[0].stage == 3
    assert context_forms[0].status == "pending"
    assert "painter" in context_forms[0].query_text


def test_fetched_target_without_assessment_does_not_terminalize(
    connection: sqlite3.Connection,
) -> None:
    """Design 9–11: ``fetched`` alone is not path-terminal (assess still required)."""
    policy = _policy()
    config = _main_config(max_alias_forms=0, max_context_forms=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form = forms[0]
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=form.query_text,
                results=(
                    SearchResult(
                        rank=1,
                        url="https://example.com/needs-assess",
                        title="Hit",
                        snippet="s",
                        extra_snippets=(),
                        language="en",
                        provider_result_id=None,
                    ),
                ),
            )
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) == 1
    with immediate(connection) as conn:
        update_coverage_article_target(conn, target_id=targets[0].id, status="fetched")
        maybe_advance_coverage_plan(
            conn,
            plan_id=plan_id,
            run_id=run_id,
            config=config,
            policy=policy,
            now=moment(2),
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status not in {"completed", "failed", "incomplete", "superseded"}
    assert plan.status in {"selecting", "assessing", "retrieving"}
    assert plan.completed_at is None


def test_persist_idempotent_on_same_attempt_does_not_double_occurrences(
    connection: sqlite3.Connection,
) -> None:
    """UNIQUE(attempt_id): re-persist same attempt does not re-insert occurrences."""
    policy = _policy()
    config = _main_config(max_alias_forms=0, max_context_forms=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form = forms[0]
    page = sample_search_page(
        query=form.query_text,
        results=(
            SearchResult(
                rank=1,
                url="https://example.com/once",
                title="Once",
                snippet="s",
                extra_snippets=(),
                language="en",
                provider_result_id=None,
            ),
        ),
    )
    client = FakeWebSearchClient(pages=[page])
    with immediate(connection) as conn:
        work_id = schedule_brave_web_search(
            conn,
            form_id=form.id,
            material_fingerprint=_HASH,
            offset_in=0,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_brave_web_search_handler(
        connection, client=client, config=config, policy=policy
    )
    assert handler.prepare is not None
    assert handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(work, outcome)
    _settle_work_succeeded(connection, work_id=work_id)
    first_count = connection.execute(
        "SELECT COUNT(*) AS n FROM brave_search_result_occurrence"
    ).fetchone()
    assert first_count is not None
    assert int(first_count["n"]) == 1
    # Re-settle with the same attempt (at-least-once window): no second page call,
    # no second occurrence row.
    with immediate(connection):
        handler.persist(work, outcome)
    second_count = connection.execute(
        "SELECT COUNT(*) AS n FROM brave_search_result_occurrence"
    ).fetchone()
    assert second_count is not None
    assert int(second_count["n"]) == 1
    assert len(client.calls) == 1


def test_service_does_not_define_second_canonicalize(
    connection: sqlite3.Connection,
) -> None:
    del connection
    source = inspect.getsource(coverage_service)
    # Must not invent a second URL canonicalize; only import/use ingestion.urls
    # via screening / upsert path.
    assert "def canonicalize" not in source
    assert "UnusableArticleUrl" not in source or "attach_discovery_url" in source


def test_search_result_alias_kind_written(
    connection: sqlite3.Connection,
) -> None:
    policy = _policy()
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id = _open_plan(
        connection,
        person_id=person_id,
        run_id=run_id,
        config=config,
        policy=policy,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    form = forms[0]
    client = FakeWebSearchClient(
        pages=[
            sample_search_page(
                query=form.query_text,
                results=(
                    SearchResult(
                        rank=1,
                        url="https://example.com/from-search",
                        title="Hit",
                        snippet="s",
                        extra_snippets=(),
                        language="en",
                        provider_result_id=None,
                    ),
                ),
            )
        ]
    )
    _run_brave(
        connection,
        client=client,
        config=config,
        policy=policy,
        form_id=form.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    alias = connection.execute(
        """
        SELECT kind FROM article_url_alias
         WHERE url = 'https://example.com/from-search'
        """
    ).fetchone()
    assert alias is not None
    assert alias["kind"] == "search_result"
    # Plan advanced to selecting with a fetch target.
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) == 1
    assert targets[0].selection_reason == "search_curated_eligible"
    fetch_work = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND state = 'pending'
        """,
        (FETCH_ARTICLE_TASK_TYPE,),
    ).fetchone()
    assert fetch_work is not None
    assert int(fetch_work["n"]) == 1
