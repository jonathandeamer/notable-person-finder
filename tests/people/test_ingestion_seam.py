"""Same-run seam: feed persist notifies detection scheduling.

Ingestion never imports people. The CLI (and this seam) inject
`schedule_source_items` as `on_source_items` on `build_fetch_handler`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from notable_person_finder.config.models import (
    BudgetConfig,
    DetectPeopleConfig,
    DomainProfileConfig,
    FeedConfig,
    FeedsConfig,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    TasksConfig,
)
from notable_person_finder.ingestion.service import (
    FETCH_FEED_TASK_TYPE,
    build_fetch_handler,
    seed_feeds,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    load_current_triage_observation,
)
from notable_person_finder.people.service import schedule_source_items
from notable_person_finder.providers.feeds import (
    FeedEntry,
    FeedValidators,
    Modified,
    NotModified,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.engine import TaskOutcome
from notable_person_finder.runs.models import WorkItem, WorkState
from tests.ingestion.helpers import immediate, insert_run, moment

MODEL = "openai/gpt-test"


def _main_config() -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        budget=BudgetConfig(openrouter_usd_per_run=None),
        openrouter=OpenRouterConfig(routing=ProviderRoutingConfig()),
        tasks=TasksConfig(
            detect_people=DetectPeopleConfig(
                model=MODEL,
                max_input_tokens=4415,
                max_completion_tokens=512,
                max_people=3,
            )
        ),
    )


def _profile() -> DomainProfileConfig:
    return DomainProfileConfig(
        schema_version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples={
            "significant_recognition": ("major art prize",),
            "institutional_recognition": ("permanent museum collection",),
        },
    )


def _feeds() -> FeedsConfig:
    return FeedsConfig(
        schema_version=1,
        feeds=(
            FeedConfig(
                key="alpha",
                label="Alpha",
                url="https://alpha.example.com/feed.xml",
                enabled=True,
            ),
        ),
    )


class _UnusedClient:
    def fetch_feed(
        self, feed: FeedConfig, validators: FeedValidators
    ) -> NotModified | Modified:
        raise AssertionError("execute is not used by this seam test")


def _modified(*, entries: tuple[FeedEntry, ...]) -> Modified:
    return Modified(
        requested_url="https://alpha.example.com/feed.xml",
        final_url="https://alpha.example.com/feed.xml",
        redirect_chain=(),
        status_code=200,
        validators=FeedValidators(
            etag='"e1"', last_modified="Wed, 22 Jul 2026 06:00:00 GMT"
        ),
        feed_type="rss20",
        source_title="Alpha",
        source_link="https://alpha.example.com/",
        source_updated_raw=None,
        entries=entries,
        warnings=(),
        response_bytes=512,
    )


def _entry(
    *,
    entry_id: str,
    url: str,
    title: str | None,
    summary: str | None,
) -> FeedEntry:
    return FeedEntry(
        entry_id=entry_id,
        url=url,
        title=title,
        summary=summary,
        content=None,
        author=None,
        published_raw=None,
        updated_raw=None,
    )


def _claim_and_start(connection: sqlite3.Connection, *, run_id: int) -> WorkItem:
    batch = repository.claim_batch(
        connection,
        run_id=run_id,
        now=moment(1),
        task_types=[FETCH_FEED_TASK_TYPE],
        limit=1,
    )
    item = batch[0]
    repository.start_attempt(
        connection,
        run_id=run_id,
        work_item_id=item.id,
        provider="feeds",
        operation="fetch_feed",
        ordinal=1,
        request_fingerprint="f" * 64,
        destination_host="alpha.example.com",
        reserved_nano_usd=0,
        now=moment(1),
    )
    return item


def _persist_with_schedule(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    entries: tuple[FeedEntry, ...],
) -> None:
    feeds = _feeds()
    config = _main_config()
    profile = _profile()
    seed_feeds(connection, feeds=feeds, run_id=run_id, now=moment())
    item = _claim_and_start(connection, run_id=run_id)

    def on_source_items(
        source_item_ids: tuple[int, ...], callback_run_id: int, now: str
    ) -> None:
        schedule_source_items(
            connection,
            source_item_ids=source_item_ids,
            run_id=callback_run_id,
            config=config,
            profile=profile,
            now=now,
        )

    handler = build_fetch_handler(
        connection,
        client=_UnusedClient(),
        feeds=feeds,
        on_source_items=on_source_items,
    )
    assert handler.persist is not None
    outcome = TaskOutcome(
        state=WorkState.SUCCEEDED,
        reason=None,
        payload=_modified(entries=entries),
    )
    with immediate(connection):
        handler.persist(item, outcome)


def test_feed_persist_schedules_detection_for_usable_items_in_same_run(
    connection: sqlite3.Connection,
) -> None:
    """Injected schedule_source_items creates detect_people work on insert."""
    run_id = insert_run(connection)
    _persist_with_schedule(
        connection,
        run_id=run_id,
        entries=(
            _entry(
                entry_id="usable",
                url="https://alpha.example.com/usable",
                title="Élodie N'Diaye wins the Prix Exemple",
                summary="The sculptor was honoured in Paris.",
            ),
        ),
    )

    source_ids = [
        int(row["id"])
        for row in connection.execute("SELECT id FROM source_item ORDER BY id")
    ]
    assert len(source_ids) == 1
    detect_rows = list(
        connection.execute(
            """
            SELECT * FROM work_item
             WHERE task_type = ? AND subject_kind = 'source_item'
               AND subject_id = ? AND state = 'pending'
            """,
            (DETECT_PEOPLE_TASK_TYPE, source_ids[0]),
        )
    )
    assert len(detect_rows) == 1
    assert detect_rows[0]["required"] == 1


def test_feed_persist_writes_insufficient_input_for_empty_text_in_same_run(
    connection: sqlite3.Connection,
) -> None:
    """Empty title and summary become a durable observation, no detect work."""
    run_id = insert_run(connection)
    _persist_with_schedule(
        connection,
        run_id=run_id,
        entries=(
            _entry(
                entry_id="empty",
                url="https://alpha.example.com/empty",
                title=None,
                summary="   ",
            ),
        ),
    )

    source_id = int(connection.execute("SELECT id FROM source_item").fetchone()["id"])
    detect_count = connection.execute(
        """
        SELECT COUNT(*) AS n FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, source_id),
    ).fetchone()["n"]
    assert detect_count == 0
    current = load_current_triage_observation(connection, source_item_id=source_id)
    assert current is not None
    assert current.disposition == "insufficient_input"
    assert current.run_id == run_id
