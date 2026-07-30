"""HTTP handlers + maybe_advance_plan: search, facts, K4/K5/K22/K23/K24."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from notable_person_finder.config.models import (
    BudgetConfig,
    MainConfig,
    MatchWikipediaIdentityConfig,
    MediaWikiConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.mediawiki import (
    PAGE_FACTS_OPERATION,
    PROVIDER,
    SEARCH_OPERATION,
    MediaWikiPageFact,
    MediaWikiPageFactsBatch,
    MediaWikiSearchHit,
    MediaWikiSearchPage,
)
from notable_person_finder.runs.models import WorkItem, WorkState
from notable_person_finder.wikipedia import service as wikipedia_service
from notable_person_finder.wikipedia.repository import (
    insert_query_forms,
    list_page_facts_batches_for_plan,
    list_query_forms_for_plan,
    load_plan,
    open_plan,
)
from notable_person_finder.wikipedia.service import (
    MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
    MEDIAWIKI_PAGE_FACTS_TASK_TYPE,
    MEDIAWIKI_SEARCH_TASK_TYPE,
    NO_MATCH_VALIDATED_OUTPUT_JSON,
    build_mediawiki_page_facts_handler,
    build_mediawiki_search_handler,
    maybe_advance_plan,
    mediawiki_search_fingerprint,
    schedule_mediawiki_search,
)
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
NOW = moment()
MODEL = "openai/gpt-test"


def _main_config(
    *,
    max_query_forms: int = 6,
    max_continuations_per_form: int = 1,
    max_search_hits_per_form: int = 20,
    max_redirect_hops: int = 3,
    max_fact_pages_per_plan: int = 40,
    max_page_ids_per_facts_request: int = 20,
    max_candidates: int = 8,
    search_srlimit: int = 10,
) -> MainConfig:
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
                model=MODEL,
                max_query_forms=max_query_forms,
                max_continuations_per_form=max_continuations_per_form,
                max_search_hits_per_form=max_search_hits_per_form,
                max_redirect_hops=max_redirect_hops,
                max_fact_pages_per_plan=max_fact_pages_per_plan,
                max_page_ids_per_facts_request=max_page_ids_per_facts_request,
                max_candidates=max_candidates,
                search_srlimit=search_srlimit,
            )
        ),
    )


@dataclass
class ScriptedMediaWikiClient:
    search_results: list[MediaWikiSearchPage | ProviderFailure] = field(
        default_factory=list
    )
    facts_results: list[MediaWikiPageFactsBatch | ProviderFailure] = field(
        default_factory=list
    )
    search_calls: list[tuple[str, str | None]] = field(default_factory=list)
    facts_calls: list[tuple[int, ...]] = field(default_factory=list)
    _search_i: int = 0
    _facts_i: int = 0

    def search_pages(
        self, query: str, *, continuation: str | None
    ) -> MediaWikiSearchPage:
        self.search_calls.append((query, continuation))
        if self._search_i >= len(self.search_results):
            raise RuntimeError("no further search results scripted")
        result = self.search_results[self._search_i]
        self._search_i += 1
        if isinstance(result, ProviderFailure):
            raise result
        return result

    def get_page_facts(self, page_ids: Sequence[int]) -> MediaWikiPageFactsBatch:
        key = tuple(int(p) for p in page_ids)
        self.facts_calls.append(key)
        if self._facts_i >= len(self.facts_results):
            raise RuntimeError("no further facts results scripted")
        result = self.facts_results[self._facts_i]
        self._facts_i += 1
        if isinstance(result, ProviderFailure):
            raise result
        return result


def _person(
    connection: sqlite3.Connection, *, run_id: int, name: str = "Alex Smith"
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (?, ?, ?, ?)
        """,
        (NOW, run_id, name, _HASH),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return cursor.lastrowid


def _open_plan_with_forms(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    forms: Sequence[tuple[str, str]],
    material_fingerprint: str = _HASH,
) -> tuple[int, tuple[int, ...]]:
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
            forms=tuple(
                {
                    "ordinal": index,
                    "variant_kind": kind,
                    "query_text": text,
                }
                for index, (kind, text) in enumerate(forms, start=1)
            ),
        )
    return plan_id, form_ids


def _work_item_row(
    connection: sqlite3.Connection,
    *,
    work_id: int,
) -> WorkItem:
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
    provider: str = PROVIDER,
    operation: str = SEARCH_OPERATION,
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
        (run_id, work_id, provider, operation, NOW, fingerprint),
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


def _run_search(
    connection: sqlite3.Connection,
    *,
    client: ScriptedMediaWikiClient,
    config: MainConfig,
    form_id: int,
    material_fingerprint: str,
    run_id: int,
    continuation_in: str | None = None,
) -> WorkItem:
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_id,
            material_fingerprint=material_fingerprint,
            continuation_in=continuation_in,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection, work_id=work_id, run_id=run_id, operation=SEARCH_OPERATION
    )
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.prepare is not None
    assert handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        # persist joins caller txn (engine shape)
        handler.persist(work, outcome)
    _settle_work_succeeded(connection, work_id=work_id)
    return work


def _run_facts(
    connection: sqlite3.Connection,
    *,
    client: ScriptedMediaWikiClient,
    config: MainConfig,
    batch_id: int,
    run_id: int,
) -> WorkItem:
    row = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ?
           AND state IN ('pending', 'running', 'deferred')
         ORDER BY id DESC LIMIT 1
        """,
        (MEDIAWIKI_PAGE_FACTS_TASK_TYPE, batch_id),
    ).fetchone()
    assert row is not None, f"no pending facts work for batch {batch_id}"
    work_id = int(row["id"])
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        operation=PAGE_FACTS_OPERATION,
    )
    handler = build_mediawiki_page_facts_handler(
        connection, client=client, config=config
    )
    assert handler.prepare is not None
    assert handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(work, outcome)
    _settle_work_succeeded(connection, work_id=work_id)
    return work


def _page(
    page_id: int,
    title: str,
    *,
    namespace: int = 0,
    missing: bool = False,
    dab: bool = False,
    redirect_to: int | None = None,
    extract: str = "biography extract",
) -> MediaWikiPageFact:
    return MediaWikiPageFact(
        page_id=page_id,
        requested_title=title,
        canonical_title=title,
        namespace=namespace,
        missing=missing,
        redirect_to_page_id=redirect_to,
        redirect_to_title=None if redirect_to is None else f"T{redirect_to}",
        is_disambiguation=dab,
        canonical_url=f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
        description="desc",
        extract=extract,
        categories=("People",),
    )


def _search_page(
    hits: Sequence[tuple[int | None, str]],
    *,
    continuation: str | None = None,
    more: bool = False,
) -> MediaWikiSearchPage:
    return MediaWikiSearchPage(
        hits=tuple(
            MediaWikiSearchHit(page_id=page_id, title=title) for page_id, title in hits
        ),
        continuation=continuation,
        more_results=more,
        provider_total_hits=None,
    )


def _active_match_count(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ?
           AND state IN ('pending', 'running', 'deferred')
        """,
        (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,),
    ).fetchone()
    assert row is not None
    return int(row["n"])


def _obs_for_person(
    connection: sqlite3.Connection, person_id: int
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT * FROM wikipedia_identity_observation
         WHERE person_id = ?
         ORDER BY id DESC LIMIT 1
        """,
        (person_id,),
    ).fetchone()


# ---------------------------------------------------------------------------
# K4: empty complete safe → no_match, zero match work
# ---------------------------------------------------------------------------


def test_empty_complete_search_writes_no_match_zero_match_work(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"), ("comma_swap", "Smith Alex")),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            _search_page(()),
            _search_page(()),
        ]
    )
    for form_id in form_ids:
        _run_search(
            connection,
            client=client,
            config=config,
            form_id=form_id,
            material_fingerprint=_HASH,
            run_id=run_id,
        )

    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"
    assert _active_match_count(connection) == 0
    assert len(client.facts_calls) == 0  # zero-hit plans skip facts
    assert client._search_i == 2  # noqa: SLF001 — OpenRouter never involved

    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "completed"
    assert obs["semantic_outcome"] == "no_matching_page_found"
    assert obs["attempt_id"] is None
    assert obs["model_inspection_id"] is None
    assert obs["candidate_page_ids_json"] == "[]"
    assert obs["validated_output_json"] == NO_MATCH_VALIDATED_OUTPUT_JSON
    assert obs["prompt_hash"] is None
    person = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    assert person is not None
    assert person["current_wikipedia_identity_observation_id"] == obs["id"]


# ---------------------------------------------------------------------------
# K5: truncated empty → failed, not no_match
# ---------------------------------------------------------------------------


def test_truncated_empty_search_fails_not_no_match(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_continuations_per_form=0)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            _search_page((), continuation="10", more=True),
        ]
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert _active_match_count(connection) == 0
    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "failed"
    assert obs["semantic_outcome"] is None
    assert obs["attempt_id"] is None
    assert obs["failure_category"] == "unsafe_truncation"
    person = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    assert person is not None
    assert person["current_wikipedia_identity_observation_id"] is None


# ---------------------------------------------------------------------------
# Continuation bound
# ---------------------------------------------------------------------------


def test_search_continuation_within_bound(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_continuations_per_form=1)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            _search_page(((100, "A"),), continuation="10", more=True),
            _search_page(((101, "B"),)),
        ],
        facts_results=[
            MediaWikiPageFactsBatch(
                pages=(
                    _page(100, "A"),
                    _page(101, "B"),
                )
            )
        ],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    # Continuation scheduled
    cont = connection.execute(
        """
        SELECT id, fingerprint FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE, form_ids[0]),
    ).fetchone()
    assert cont is not None
    expected_fp = mediawiki_search_fingerprint(
        material_fingerprint=_HASH,
        query_form_id=form_ids[0],
        continuation_in="10",
    )
    assert cont["fingerprint"] == expected_fp

    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
        continuation_in="10",
    )
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "completed"
    assert form.continuations_used == 1
    assert form.hit_count == 2
    assert form.truncated is False

    # Ranks are global within the form (continuation page does not restart at 1).
    ranks = connection.execute(
        """
        SELECT h.rank, h.page_id
          FROM mediawiki_search_hit AS h
          JOIN mediawiki_search_observation AS o
            ON o.id = h.search_observation_id
         WHERE o.query_form_id = ?
         ORDER BY h.rank
        """,
        (form_ids[0],),
    ).fetchall()
    assert [(int(r["rank"]), int(r["page_id"])) for r in ranks] == [
        (1, 100),
        (2, 101),
    ]

    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    assert batches[0].status == "pending"
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"
    assert _active_match_count(connection) == 1
    assert client.search_calls == [("Alex Smith", None), ("Alex Smith", "10")]


def test_page0_hits_retained_when_continuation_permanently_fails(
    connection: sqlite3.Connection,
) -> None:
    """Successful page-0 hits must survive form permanent fail (K22 partial)."""
    config = _main_config(max_continuations_per_form=1)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            _search_page(((50, "Alex Smith"),), continuation="10", more=True),
        ],
        facts_results=[MediaWikiPageFactsBatch(pages=(_page(50, "Alex Smith"),))],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "pending"
    assert form.hit_count == 1

    # Permanent-fail the scheduled continuation.
    cont = connection.execute(
        """
        SELECT id FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
         ORDER BY id DESC LIMIT 1
        """,
        (MEDIAWIKI_SEARCH_TASK_TYPE, form_ids[0]),
    ).fetchone()
    assert cont is not None
    work_id = int(cont["id"])
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    _settle_work_failed_permanent(connection, work_id=work_id)
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=PROVIDER,
                operation=SEARCH_OPERATION,
                detail="continuation died",
            ),
        )

    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "failed"
    # Page-0 hit must still drive facts + match (not empty-fail).
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    assert json.loads(batches[0].page_ids_json) == [50]
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"
    assert plan.partial_retrieval is True
    assert _active_match_count(connection) == 1
    obs = _obs_for_person(connection, person_id)
    assert obs is None  # match not yet run; no empty-fail obs


def test_failed_facts_batch_empty_uses_unsafe_truncation_not_redirect(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((9, "Alex"),))],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    batch_id = batches[0].id
    row = connection.execute(
        """
        SELECT id FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
        """,
        (MEDIAWIKI_PAGE_FACTS_TASK_TYPE, batch_id),
    ).fetchone()
    assert row is not None
    work_id = int(row["id"])
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        operation=PAGE_FACTS_OPERATION,
    )
    _settle_work_failed_permanent(connection, work_id=work_id)
    handler = build_mediawiki_page_facts_handler(
        connection, client=client, config=config
    )
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=PROVIDER,
                operation=PAGE_FACTS_OPERATION,
            ),
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "failed"
    assert obs["failure_category"] == "unsafe_truncation"
    assert obs["failure_category"] != "redirect_budget_exhausted"
    assert _active_match_count(connection) == 0


# ---------------------------------------------------------------------------
# K24 multi-wave redirects
# ---------------------------------------------------------------------------


def test_k24_redirect_wave_schedules_second_batch(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((10, "Redirect Source"),))],
        facts_results=[
            MediaWikiPageFactsBatch(
                pages=(_page(10, "Redirect Source", redirect_to=20),)
            ),
            MediaWikiPageFactsBatch(pages=(_page(20, "Terminal Bio"),)),
        ],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    assert batches[0].wave == 1
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 2
    assert batches[1].wave == 2
    assert batches[1].status == "pending"
    page_ids = json.loads(batches[1].page_ids_json)
    assert page_ids == [20]

    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[1].id,
        run_id=run_id,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"
    assert _active_match_count(connection) == 1
    assert len(client.facts_calls) == 2


def test_k24_two_hop_redirect_chain(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_redirect_hops=3)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((1, "A"),))],
        facts_results=[
            MediaWikiPageFactsBatch(pages=(_page(1, "A", redirect_to=2),)),
            MediaWikiPageFactsBatch(pages=(_page(2, "B", redirect_to=3),)),
            MediaWikiPageFactsBatch(pages=(_page(3, "C Terminal"),)),
        ],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    for _ in range(3):
        pending = connection.execute(
            """
            SELECT id FROM wikipedia_page_facts_batch
             WHERE plan_id = ? AND status = 'pending'
             ORDER BY ordinal LIMIT 1
            """,
            (plan_id,),
        ).fetchone()
        if pending is None:
            break
        _run_facts(
            connection,
            client=client,
            config=config,
            batch_id=int(pending["id"]),
            run_id=run_id,
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"
    assert len(client.facts_calls) == 3


# ---------------------------------------------------------------------------
# Hop/page budget exhaustion → failed not no-match
# ---------------------------------------------------------------------------


def test_redirect_hop_budget_exhaustion_empty_fails(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_redirect_hops=1)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    # Root redirects; hop budget 1 allows fetch of target; target still redirects
    # and hop 2 is over budget → unresolved trail, empty candidates.
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((1, "A"),))],
        facts_results=[
            MediaWikiPageFactsBatch(pages=(_page(1, "A", redirect_to=2),)),
            MediaWikiPageFactsBatch(pages=(_page(2, "B", redirect_to=3),)),
        ],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    for _ in range(2):
        pending = connection.execute(
            """
            SELECT id FROM wikipedia_page_facts_batch
             WHERE plan_id = ? AND status = 'pending' ORDER BY ordinal LIMIT 1
            """,
            (plan_id,),
        ).fetchone()
        if pending is None:
            break
        _run_facts(
            connection,
            client=client,
            config=config,
            batch_id=int(pending["id"]),
            run_id=run_id,
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert _active_match_count(connection) == 0
    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "failed"
    assert obs["failure_category"] in {
        "redirect_budget_exhausted",
        "unsafe_truncation",
    }
    assert obs["attempt_id"] is None


def test_fact_page_budget_exhaustion_empty_fails(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_fact_pages_per_plan=1)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((1, "A"),))],
        facts_results=[
            MediaWikiPageFactsBatch(pages=(_page(1, "A", redirect_to=2),)),
        ],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    # No second wave (budget 1 already used); plan fails empty+unsafe.
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert plan.truncated_unsafe_for_negative is True
    assert _active_match_count(connection) == 0
    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "failed"
    assert obs["attempt_id"] is None


# ---------------------------------------------------------------------------
# K22 partial form fail
# ---------------------------------------------------------------------------


def test_k22_one_form_fail_with_candidates_schedules_match_partial(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex Smith"), ("comma_swap", "Smith Alex")),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((50, "Alex Smith"),))],
        facts_results=[MediaWikiPageFactsBatch(pages=(_page(50, "Alex Smith"),))],
    )
    # Succeed first form
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    # Permanent-fail second form
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_ids[1],
            material_fingerprint=_HASH,
            continuation_in=None,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    _settle_work_failed_permanent(connection, work_id=work_id)
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.persist_failure is not None
    failure = ProviderFailure(
        FailureCategory.CONFIGURATION,
        provider=PROVIDER,
        operation=SEARCH_OPERATION,
        detail="bad",
    )
    with immediate(connection):
        handler.persist_failure(work, failure)

    form2 = list_query_forms_for_plan(connection, plan_id=plan_id)[1]
    assert form2.status == "failed"

    # Facts for successful form should be scheduled after advance
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    assert len(batches) == 1
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"
    assert plan.partial_retrieval is True
    assert _active_match_count(connection) == 1


def test_k22_empty_plus_form_fail_is_failed_not_no_match(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(
            ("exact", "Nobody"),
            ("comma_swap", "Nobody X"),
        ),
    )
    client = ScriptedMediaWikiClient(search_results=[_search_page(())])
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_ids[1],
            material_fingerprint=_HASH,
            continuation_in=None,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    _settle_work_failed_permanent(connection, work_id=work_id)
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.CONFIGURATION,
                provider=PROVIDER,
                operation=SEARCH_OPERATION,
            ),
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "failed"
    assert _active_match_count(connection) == 0
    obs = _obs_for_person(connection, person_id)
    assert obs is not None
    assert obs["disposition"] == "failed"
    assert obs["failure_category"] == "partial_retrieval_empty"
    assert obs["attempt_id"] is None


# ---------------------------------------------------------------------------
# K23 accent phase
# ---------------------------------------------------------------------------


def test_k23_primaries_with_main_ns_non_dab_never_insert_accent(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_query_forms=6)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id, name="José García")
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "José García"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[_search_page(((7, "José García"),))],
        facts_results=[MediaWikiPageFactsBatch(pages=(_page(7, "José García"),))],
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    batches = list_page_facts_batches_for_plan(connection, plan_id=plan_id)
    _run_facts(
        connection,
        client=client,
        config=config,
        batch_id=batches[0].id,
        run_id=run_id,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert all(f.variant_kind != "accent_fallback" for f in forms)
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "ready_for_match"


def test_k23_empty_primaries_insert_accent_once(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config(max_query_forms=4)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id, name="José García")
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "José García"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            _search_page(()),  # primary
            _search_page(()),  # accent
        ]
    )
    _run_search(
        connection,
        client=client,
        config=config,
        form_id=form_ids[0],
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    forms = list_query_forms_for_plan(connection, plan_id=plan_id)
    accent = [f for f in forms if f.variant_kind == "accent_fallback"]
    assert len(accent) == 1
    assert accent[0].query_text == "Jose Garcia"
    assert accent[0].status == "pending"
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "retrieving"

    _run_search(
        connection,
        client=client,
        config=config,
        form_id=accent[0].id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    forms_after = list_query_forms_for_plan(connection, plan_id=plan_id)
    assert sum(1 for f in forms_after if f.variant_kind == "accent_fallback") == 1
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "completed"  # empty complete after accent
    assert _active_match_count(connection) == 0


# ---------------------------------------------------------------------------
# Idempotent double settle + superseded no-op
# ---------------------------------------------------------------------------


def test_double_settle_same_attempt_does_not_duplicate_hits(
    connection: sqlite3.Connection,
) -> None:
    """At-least-once re-persist while form still pending must not re-insert hits."""
    config = _main_config(max_continuations_per_form=1)
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    client = ScriptedMediaWikiClient(
        search_results=[
            # more=True keeps form pending after page-0 (continuation scheduled).
            _search_page(((1, "Alex"), (2, "Alex Jr")), continuation="10", more=True),
        ]
    )
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_ids[0],
            material_fingerprint=_HASH,
            continuation_in=None,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    assert work.priority == 50
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.prepare is not None and handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(work, outcome)
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "pending"
    hits1 = connection.execute(
        "SELECT COUNT(*) AS n FROM mediawiki_search_hit"
    ).fetchone()
    assert hits1 is not None
    count1 = int(hits1["n"])
    assert count1 == 2

    # Second persist same attempt (at-least-once); form still pending.
    with immediate(connection):
        handler.persist(work, outcome)
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "pending"
    assert form.hit_count == 2
    hits2 = connection.execute(
        "SELECT COUNT(*) AS n FROM mediawiki_search_hit"
    ).fetchone()
    assert hits2 is not None
    assert int(hits2["n"]) == count1
    del person_id


def test_superseded_plan_persist_is_noop(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    connection.execute(
        """
        UPDATE wikipedia_identity_plan
           SET status = 'superseded', completed_at = ?
         WHERE id = ?
        """,
        (NOW, plan_id),
    )
    connection.commit()

    client = ScriptedMediaWikiClient(search_results=[_search_page(((9, "Alex"),))])
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_ids[0],
            material_fingerprint=_HASH,
            continuation_in=None,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(connection, work_id=work_id, run_id=run_id)
    handler = build_mediawiki_search_handler(connection, client=client, config=config)
    assert handler.prepare is not None and handler.persist is not None
    # prepare refuses superseded
    with pytest.raises(ValueError, match="superseded"):
        handler.prepare(work)

    # Even if execute ran and persist is invoked, domain writes no-op.
    prepared_call = wikipedia_service._SearchCall(  # noqa: SLF001
        form_id=form_ids[0],
        plan_id=plan_id,
        query_text="Alex",
        continuation_in=None,
        continuations_used_before=0,
        material_fingerprint=_HASH,
    )
    outcome = handler.execute(work, 1, prepared_call)
    with immediate(connection):
        handler.persist(work, outcome)
    form = list_query_forms_for_plan(connection, plan_id=plan_id)[0]
    assert form.status == "pending"
    obs_count = connection.execute(
        "SELECT COUNT(*) AS n FROM mediawiki_search_observation"
    ).fetchone()
    assert obs_count is not None
    assert int(obs_count["n"]) == 0


def test_maybe_advance_waits_while_form_pending(
    connection: sqlite3.Connection,
) -> None:
    config = _main_config()
    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    plan_id, _form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    with immediate(connection) as conn:
        maybe_advance_plan(
            conn,
            plan_id=plan_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )
    plan = load_plan(connection, plan_id=plan_id)
    assert plan is not None
    assert plan.status == "retrieving"
    assert _active_match_count(connection) == 0


def test_handlers_use_http_pool_priority_and_no_budget(
    connection: sqlite3.Connection,
) -> None:
    from notable_person_finder.wikipedia.service import MEDIAWIKI_HTTP_PRIORITY

    config = _main_config()
    client = ScriptedMediaWikiClient()
    search = build_mediawiki_search_handler(connection, client=client, config=config)
    facts = build_mediawiki_page_facts_handler(connection, client=client, config=config)
    assert search.provider == PROVIDER
    assert search.operation == SEARCH_OPERATION
    assert search.reserved_nano_usd == 0
    assert search.pool.name == "HTTP"
    assert facts.provider == PROVIDER
    assert facts.operation == PAGE_FACTS_OPERATION
    assert facts.reserved_nano_usd == 0
    assert facts.task_type == MEDIAWIKI_PAGE_FACTS_TASK_TYPE

    run_id = insert_run(connection)
    person_id = _person(connection, run_id=run_id)
    _plan_id, form_ids = _open_plan_with_forms(
        connection,
        person_id=person_id,
        run_id=run_id,
        forms=(("exact", "Alex"),),
    )
    with immediate(connection) as conn:
        work_id = schedule_mediawiki_search(
            conn,
            form_id=form_ids[0],
            material_fingerprint=_HASH,
            continuation_in=None,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    assert work.priority == MEDIAWIKI_HTTP_PRIORITY
    assert work.priority == 50
