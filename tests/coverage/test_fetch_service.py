"""fetch_article handler: views, K3 HTML ban, K28 snippets, K31 person_article."""

from __future__ import annotations

import json
import sqlite3
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
from notable_person_finder.coverage.repository import (
    list_coverage_article_targets_for_plan,
    list_query_forms_for_plan,
    load_article_view,
    load_coverage_article_target,
    load_person_article_by_pair,
)
from notable_person_finder.coverage.screening import source_policy_from_mapping
from notable_person_finder.coverage.service import (
    ASSESS_ARTICLE_TASK_TYPE,
    FETCH_ARTICLE_TASK_TYPE,
    build_brave_web_search_handler,
    build_fetch_article_handler,
    open_coverage_plan,
    schedule_brave_web_search,
    schedule_fetch_article,
)
from notable_person_finder.people.identity import match_key
from notable_person_finder.people.repository import mechanical_search_name
from notable_person_finder.providers.articles import (
    ARTICLE_PROVIDER,
    OPERATION_FETCH_ARTICLE,
    ArticleAccessKind,
    ExtractionQuality,
)
from notable_person_finder.providers.brave import (
    OPERATION_SEARCH_WEB,
    SearchResult,
)
from notable_person_finder.providers.brave import (
    PROVIDER as BRAVE_PROVIDER,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs.models import WorkItem, WorkState
from tests.coverage.fakes_articles import (
    FakeArticleExtractor,
    FakeArticleFetcher,
    sample_access_denied,
    sample_extracted,
    sample_fetch_success,
)
from tests.coverage.fakes_brave import FakeWebSearchClient, sample_search_page
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
NOW = moment()
MODEL = "openai/gpt-test"
HTML_MARKER = b"<html><body><p>SECRET_HTML_BODY_MUST_NOT_LEAK</p></body></html>"


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
    retrieval_target: int = 5,
    max_eligible_fetches: int = 8,
    max_unclassified_fetches: int = 2,
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
                search_count=10,
                max_offsets_per_form=0,
                max_results_per_form=20,
                retrieval_target=retrieval_target,
                max_eligible_fetches=max_eligible_fetches,
                max_unclassified_fetches=max_unclassified_fetches,
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
    provider: str,
    operation: str,
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
           SET finished_at = ?, outcome = 'failed', failure_category = 'timeout'
         WHERE work_item_id = ? AND finished_at IS NULL
        """,
        (moment(1), work_id),
    )
    connection.commit()


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


def _run_brave(
    connection: sqlite3.Connection,
    *,
    client: FakeWebSearchClient,
    config: MainConfig,
    policy,
    form_id: int,
    material_fingerprint: str,
    run_id: int,
) -> None:
    with immediate(connection) as conn:
        work_id = schedule_brave_web_search(
            conn,
            form_id=form_id,
            material_fingerprint=material_fingerprint,
            offset_in=0,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        provider=BRAVE_PROVIDER,
        operation=OPERATION_SEARCH_WEB,
    )
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


def _pending_fetch_work(
    connection: sqlite3.Connection,
) -> list[tuple[int, int]]:
    rows = connection.execute(
        """
        SELECT id, subject_id FROM work_item
         WHERE task_type = ? AND state = 'pending'
         ORDER BY id
        """,
        (FETCH_ARTICLE_TASK_TYPE,),
    ).fetchall()
    return [(int(r["id"]), int(r["subject_id"])) for r in rows]


def _run_fetch(
    connection: sqlite3.Connection,
    *,
    fetcher: FakeArticleFetcher,
    extractor: FakeArticleExtractor,
    config: MainConfig,
    policy,
    target_id: int,
    material_fingerprint: str,
    run_id: int,
) -> tuple[WorkItem, object]:
    with immediate(connection) as conn:
        work_id = schedule_fetch_article(
            conn,
            target_id=target_id,
            material_fingerprint=material_fingerprint,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
    )
    handler = build_fetch_article_handler(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
    )
    assert handler.prepare is not None
    assert handler.persist is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(work, outcome)
    _settle_work_succeeded(connection, work_id=work_id)
    return work, outcome


def _plan_with_search_hits(
    connection: sqlite3.Connection,
    *,
    urls: tuple[str, ...],
    titles: tuple[str, ...] | None = None,
    snippets: tuple[str, ...] | None = None,
    config: MainConfig | None = None,
    policy=None,
) -> tuple[int, int, int, MainConfig, object]:
    """Open plan, complete one Brave form with the given result URLs."""
    policy = policy if policy is not None else _policy()
    config = config if config is not None else _main_config()
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
    titles = titles or tuple(f"Title {i}" for i in range(len(urls)))
    snippets = snippets or tuple(
        f"Snippet body for article {i}" for i in range(len(urls))
    )
    results = tuple(
        SearchResult(
            rank=index,
            url=url,
            title=titles[index - 1],
            snippet=snippets[index - 1],
            extra_snippets=(),
            language="en",
            provider_result_id=None,
        )
        for index, url in enumerate(urls, start=1)
    )
    client = FakeWebSearchClient(
        pages=[sample_search_page(query=form.query_text, results=results)]
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
    return plan_id, person_id, run_id, config, policy


# ---------------------------------------------------------------------------
# Selection → targets (K34 / K31 / ineligible ban)
# ---------------------------------------------------------------------------


def test_selection_creates_k34_targets_and_person_article(
    connection: sqlite3.Connection,
) -> None:
    plan_id, person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(
            "https://example.com/eligible-one",
            "https://example.com/eligible-two",
        ),
        config=_main_config(retrieval_target=2),
    )
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) >= 1
    reasons = {t.selection_reason for t in targets}
    assert reasons <= {
        "discovery_curated_eligible",
        "discovery_unclassified_fallback",
        "search_curated_eligible",
        "search_unclassified_fallback",
    }
    assert "discovery" not in reasons
    assert all(t.status == "pending" for t in targets)

    for target in targets:
        person_article = load_person_article_by_pair(
            connection,
            person_id=person_id,
            canonical_article_id=target.canonical_article_id,
        )
        assert person_article is not None
        assert person_article.first_plan_id == plan_id

    pending = _pending_fetch_work(connection)
    assert len(pending) == len(targets)
    del run_id, config, policy


def test_curated_ineligible_never_gets_targets(
    connection: sqlite3.Connection,
) -> None:
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(
            "https://twitter.com/status/1",
            "https://example.com/good-piece",
        ),
        config=_main_config(retrieval_target=5),
    )
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) >= 1
    for target in targets:
        assert "twitter.com" not in target.request_url
        assert target.selection_reason != "discovery"
    ineligible_screenings = connection.execute(
        """
        SELECT COUNT(*) AS n FROM source_screening
         WHERE plan_id = ? AND rule_status = 'curated_ineligible'
        """,
        (plan_id,),
    ).fetchone()
    assert ineligible_screenings is not None
    assert int(ineligible_screenings["n"]) >= 1
    del run_id, config, policy


# ---------------------------------------------------------------------------
# Fetch success: full / partial
# ---------------------------------------------------------------------------


def test_fetch_success_full_view_and_one_get(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/full-story"
    plan_id, person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        titles=("Full story title",),
        snippets=("Search snippet for full story",),
        config=_main_config(retrieval_target=1),
    )
    targets = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)
    assert len(targets) == 1
    target = targets[0]

    fetcher = FakeArticleFetcher(
        results=[sample_fetch_success(requested_url=url, html=HTML_MARKER)]
    )
    extractor = FakeArticleExtractor(
        results=[
            sample_extracted(title="Extracted title", quality=ExtractionQuality.FULL)
        ]
    )
    _work, outcome = _run_fetch(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
        target_id=target.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )

    assert len(fetcher.calls) == 1
    assert fetcher.calls[0] == url
    assert len(extractor.calls) == 1
    # HTML must not appear on the outcome surface (K3).
    payload_text = repr(outcome.payload) + (outcome.detail_json or "")
    assert b"SECRET_HTML" not in payload_text.encode("utf-8", errors="replace")
    assert "SECRET_HTML" not in payload_text
    assert getattr(outcome.payload, "html", None) is None

    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "fetched"
    assert target.article_view_id is not None
    view = load_article_view(connection, view_id=target.article_view_id)
    assert view is not None
    assert view.access_kind == "full"
    assert view.title == "Extracted title"
    assert view.extraction_quality == "full"
    blocks = json.loads(view.main_text_blocks_json)
    assert isinstance(blocks, list) and len(blocks) >= 1
    assert all("<html" not in json.dumps(block).lower() for block in blocks)

    # K31 person_article present; assess scheduled for usable view.
    pa = load_person_article_by_pair(
        connection,
        person_id=person_id,
        canonical_article_id=target.canonical_article_id,
    )
    assert pa is not None
    assess = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
        """,
        (ASSESS_ARTICLE_TASK_TYPE, pa.id),
    ).fetchone()
    assert assess is not None
    assert int(assess["n"]) == 1


def test_fetch_success_partial_view(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/partial-story"
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(results=[sample_fetch_success(requested_url=url)])
    extractor = FakeArticleExtractor(
        results=[
            sample_extracted(
                title="Partial",
                quality=ExtractionQuality.PARTIAL,
                blocks=sample_extracted().blocks[:1],
            )
        ]
    )
    _run_fetch(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
        target_id=target.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "fetched"
    view = load_article_view(connection, view_id=target.article_view_id)  # type: ignore[arg-type]
    assert view is not None
    assert view.access_kind == "partial"
    assert view.extraction_quality == "partial"


# ---------------------------------------------------------------------------
# Snippets path (K28): access denied / empty extract
# ---------------------------------------------------------------------------


def test_access_denied_creates_snippets_only_view(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/paywalled"
    plan_id, person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        titles=("Paywalled title",),
        snippets=("Visible search snippet about the person",),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(
        results=[
            sample_access_denied(
                kind=ArticleAccessKind.ACCESS_DENIED,
                requested_url=url,
                status_code=403,
            )
        ]
    )
    extractor = FakeArticleExtractor(results=[])
    _run_fetch(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
        target_id=target.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    assert len(fetcher.calls) == 1
    assert extractor.calls == []

    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "snippets_only"
    assert target.article_view_id is not None
    view = load_article_view(connection, view_id=target.article_view_id)
    assert view is not None
    assert view.access_kind == "snippets"
    assert view.title == "Paywalled title"
    snippets = json.loads(view.snippets_json)
    assert any("Visible search snippet" in s for s in snippets)
    assert (
        view.main_text_blocks_json in ("[]", "null")
        or json.loads(view.main_text_blocks_json) == []
    )

    pa = load_person_article_by_pair(
        connection,
        person_id=person_id,
        canonical_article_id=target.canonical_article_id,
    )
    assert pa is not None
    assess = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (ASSESS_ARTICLE_TASK_TYPE, pa.id),
    ).fetchone()
    assert assess is not None
    assert int(assess["n"]) == 1


def test_empty_extract_creates_snippets_only_view(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/empty-body"
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        titles=("Empty body title",),
        snippets=("Snippet when extract empty",),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(
        results=[sample_fetch_success(requested_url=url, html=HTML_MARKER)]
    )
    extractor = FakeArticleExtractor(
        results=[
            sample_extracted(
                title=None,
                quality=ExtractionQuality.EMPTY,
                blocks=(),
            )
        ]
    )
    _work, outcome = _run_fetch(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
        target_id=target.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    assert len(fetcher.calls) == 1
    assert len(extractor.calls) == 1
    payload_text = repr(outcome.payload) + (outcome.detail_json or "")
    assert "SECRET_HTML" not in payload_text

    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "snippets_only"
    view = load_article_view(connection, view_id=target.article_view_id)  # type: ignore[arg-type]
    assert view is not None
    assert view.access_kind == "snippets"
    snippets = json.loads(view.snippets_json)
    assert any("Snippet when extract empty" in s for s in snippets)


# ---------------------------------------------------------------------------
# HTML ban in detail_json / attempt surface (K3)
# ---------------------------------------------------------------------------


def test_html_never_in_detail_json_or_payload(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/html-ban"
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(
        results=[sample_fetch_success(requested_url=url, html=HTML_MARKER)]
    )
    extractor = FakeArticleExtractor(results=[sample_extracted()])
    handler = build_fetch_article_handler(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
    )
    with immediate(connection) as conn:
        work_id = schedule_fetch_article(
            conn,
            target_id=target.id,
            material_fingerprint=_HASH,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
    )
    assert handler.prepare is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    with immediate(connection):
        assert handler.persist is not None
        handler.persist(work, outcome)

    assert outcome.detail_json is None or "SECRET_HTML" not in outcome.detail_json
    assert outcome.detail_json is None or "<html" not in outcome.detail_json.lower()
    assert (
        not hasattr(outcome.payload, "html")
        or getattr(outcome.payload, "html", None) is None
    )
    # Whole payload must not smuggle raw HTML.
    blob = json.dumps(
        outcome.payload,
        default=lambda o: getattr(o, "__dict__", repr(o)),
    )
    assert "SECRET_HTML" not in blob
    assert "<html" not in blob.lower()

    # Durable view rows hold cleaned text only.
    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None and target.article_view_id is not None
    view = load_article_view(connection, view_id=target.article_view_id)
    assert view is not None
    packed = json.dumps(
        {
            "title": view.title,
            "blocks": view.main_text_blocks_json,
            "snippets": view.snippets_json,
        }
    )
    assert "SECRET_HTML" not in packed
    assert "<html" not in packed.lower()


def test_redirect_final_url_recorded_as_alias(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/start"
    final = "https://example.com/final-dest"
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(
        results=[
            sample_fetch_success(
                requested_url=url, final_url=final, html=b"<html><p>ok</p></html>"
            )
        ]
    )
    extractor = FakeArticleExtractor(results=[sample_extracted()])
    _run_fetch(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
        target_id=target.id,
        material_fingerprint=_HASH,
        run_id=run_id,
    )
    alias = connection.execute(
        """
        SELECT kind FROM article_url_alias
         WHERE url = ? AND canonical_article_id = ?
        """,
        (final, target.canonical_article_id),
    ).fetchone()
    assert alias is not None
    assert alias["kind"] == "redirect_destination"


def test_one_get_per_execute(
    connection: sqlite3.Connection,
) -> None:
    url = "https://example.com/once"
    plan_id, _person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    fetcher = FakeArticleFetcher(results=[sample_fetch_success(requested_url=url)])
    extractor = FakeArticleExtractor(results=[sample_extracted()])
    handler = build_fetch_article_handler(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
    )
    with immediate(connection) as conn:
        work_id = schedule_fetch_article(
            conn,
            target_id=target.id,
            material_fingerprint=_HASH,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
    )
    assert handler.prepare is not None
    prepared = handler.prepare(work)
    handler.execute(work, 1, prepared.payload)
    assert len(fetcher.calls) == 1
    # Second execute would need another queued result; one call per execute only.
    assert fetcher.calls == [url]


def _run_permanent_fetch_failure(
    connection: sqlite3.Connection,
    *,
    target_id: int,
    run_id: int,
    config: MainConfig,
    policy,
) -> WorkItem:
    """Execute raises ProviderFailure; settle permanent; run persist_failure."""
    fetcher = FakeArticleFetcher(
        results=[
            ProviderFailure(
                FailureCategory.TIMEOUT,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                detail="timeout",
            )
        ]
    )
    extractor = FakeArticleExtractor()
    handler = build_fetch_article_handler(
        connection,
        fetcher=fetcher,
        extractor=extractor,
        config=config,
        policy=policy,
    )
    with immediate(connection) as conn:
        work_id = schedule_fetch_article(
            conn,
            target_id=target_id,
            material_fingerprint=_HASH,
            run_id=run_id,
            now=NOW,
        )
    work = _work_item_row(connection, work_id=work_id)
    _insert_running_attempt(
        connection,
        work_id=work_id,
        run_id=run_id,
        provider=ARTICLE_PROVIDER,
        operation=OPERATION_FETCH_ARTICLE,
    )
    assert handler.prepare is not None
    prepared = handler.prepare(work)
    with pytest.raises(ProviderFailure):
        handler.execute(work, 1, prepared.payload)
    _settle_work_failed_permanent(connection, work_id=work_id)
    assert handler.persist_failure is not None
    with immediate(connection):
        handler.persist_failure(
            work,
            ProviderFailure(
                FailureCategory.TIMEOUT,
                provider=ARTICLE_PROVIDER,
                operation=OPERATION_FETCH_ARTICLE,
                detail="timeout",
            ),
        )
    return work


def test_permanent_failure_with_search_text_is_snippets_only(
    connection: sqlite3.Connection,
) -> None:
    """K28: permanent fail + search title/snippets → snippets_only + view + assess."""
    url = "https://example.com/timeout-with-text"
    plan_id, person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        titles=("Timeout title",),
        snippets=("Timeout snippet text",),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    _run_permanent_fetch_failure(
        connection,
        target_id=target.id,
        run_id=run_id,
        config=config,
        policy=policy,
    )

    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "snippets_only"
    assert target.failure_category == "timeout"
    assert target.article_view_id is not None

    view = load_article_view(connection, view_id=target.article_view_id)
    assert view is not None
    assert view.access_kind == "snippets"
    assert view.title == "Timeout title"
    snippets = json.loads(view.snippets_json)
    assert any("Timeout snippet text" in s for s in snippets)
    assert json.loads(view.main_text_blocks_json) == []

    pa = load_person_article_by_pair(
        connection,
        person_id=person_id,
        canonical_article_id=target.canonical_article_id,
    )
    assert pa is not None
    assess = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ? AND state = 'pending'
        """,
        (ASSESS_ARTICLE_TASK_TYPE, pa.id),
    ).fetchone()
    assert assess is not None
    assert int(assess["n"]) == 1


def test_permanent_failure_without_text_is_failed(
    connection: sqlite3.Connection,
) -> None:
    """K28 positive control: permanent fail with no title/snippets → failed."""
    url = "https://example.com/timeout-no-text"
    plan_id, person_id, run_id, config, policy = _plan_with_search_hits(
        connection,
        urls=(url,),
        titles=("",),
        snippets=("",),
        config=_main_config(retrieval_target=1),
    )
    target = list_coverage_article_targets_for_plan(connection, plan_id=plan_id)[0]
    _run_permanent_fetch_failure(
        connection,
        target_id=target.id,
        run_id=run_id,
        config=config,
        policy=policy,
    )

    target = load_coverage_article_target(connection, target_id=target.id)
    assert target is not None
    assert target.status == "failed"
    assert target.failure_category == "timeout"
    assert target.article_view_id is None

    views = connection.execute(
        """
        SELECT COUNT(*) AS n FROM article_view
         WHERE canonical_article_id = ?
        """,
        (target.canonical_article_id,),
    ).fetchone()
    assert views is not None
    assert int(views["n"]) == 0

    pa = load_person_article_by_pair(
        connection,
        person_id=person_id,
        canonical_article_id=target.canonical_article_id,
    )
    assert pa is not None
    assess = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (ASSESS_ARTICLE_TASK_TYPE, pa.id),
    ).fetchone()
    assert assess is not None
    assert int(assess["n"]) == 0


def test_handler_uses_http_pool_and_article_provider(
    connection: sqlite3.Connection,
) -> None:
    from notable_person_finder.runs.scheduler import WorkerPool

    handler = build_fetch_article_handler(
        connection,
        fetcher=FakeArticleFetcher(),
        extractor=FakeArticleExtractor(),
        config=_main_config(),
        policy=_policy(),
    )
    assert handler.task_type == FETCH_ARTICLE_TASK_TYPE
    assert handler.provider == ARTICLE_PROVIDER
    assert handler.operation == OPERATION_FETCH_ARTICLE
    assert handler.pool is WorkerPool.HTTP
    assert handler.reserved_nano_usd == 0
