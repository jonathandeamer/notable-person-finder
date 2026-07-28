from __future__ import annotations

import sqlite3

from notable_person_finder.ingestion.repository import (
    insert_source_item,
    latest_validators,
    record_fetch,
    source_item_counts,
    upsert_article,
    upsert_feed_identity,
)
from tests.ingestion.helpers import insert_run, moment


def _fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    outcome: str = "modified",
    etag: str | None = None,
    last_modified: str | None = None,
) -> int:
    return record_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_at=moment(),
        requested_url="https://example.com/feed",
        resolved_url="https://example.com/feed",
        redirect_chain_json=None,
        http_status=200,
        etag=etag,
        last_modified=last_modified,
        outcome=outcome,
        feed_type="rss",
        parse_outcome="ok" if outcome != "failed" else None,
        parser_warnings_json=None,
        failure_category=None if outcome != "failed" else "network",
        entry_count=1,
        response_bytes=1024,
    )


def _item(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    fetch_id: int,
    run_id: int,
    canonical_article_id: int | None = None,
    source_entry_id: str | None = None,
    original_url: str | None = None,
) -> int | None:
    return insert_source_item(
        connection,
        feed_identity_id=feed_identity_id,
        discovered_by_fetch_id=fetch_id,
        discovered_by_run_id=run_id,
        canonical_article_id=canonical_article_id,
        source_entry_id=source_entry_id,
        original_url=original_url,
        title_raw="Raw Title",
        title_text="Title",
        summary_raw="Raw summary",
        summary_text="Summary",
        author_raw="Author",
        published_raw="2026-07-25",
        published_at=None,
        published_issue="unparseable",
        url_issue=None,
        now=moment(),
    )


# ---------------------------------------------------------------------------
# insert_source_item: dedup on canonical_article_id, across runs and feeds
# ---------------------------------------------------------------------------


def test_insert_once_across_two_runs_yields_one_source_item(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    article_id = upsert_article(
        connection,
        canonical_url="https://example.com/article",
        publisher_key="example.com",
        now=moment(),
    )

    run_one = insert_run(connection)
    fetch_one = _fetch(connection, feed_identity_id=feed_id, run_id=run_one)
    first = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_one,
        run_id=run_one,
        canonical_article_id=article_id,
    )

    run_two = insert_run(connection)
    fetch_two = _fetch(connection, feed_identity_id=feed_id, run_id=run_two)
    second = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_two,
        run_id=run_two,
        canonical_article_id=article_id,
    )

    assert first is not None
    assert second is None
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 1


def test_insert_once_across_two_feeds_returns_none_on_the_second(
    connection: sqlite3.Connection,
) -> None:
    feed_one = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed-a",
        now=moment(),
    )
    feed_two = upsert_feed_identity(
        connection,
        key="feed-b",
        label="Feed B",
        url="https://example.com/feed-b",
        now=moment(),
    )
    article_id = upsert_article(
        connection,
        canonical_url="https://example.com/shared-article",
        publisher_key="example.com",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_one = _fetch(connection, feed_identity_id=feed_one, run_id=run_id)
    fetch_two = _fetch(connection, feed_identity_id=feed_two, run_id=run_id)

    first = _item(
        connection,
        feed_identity_id=feed_one,
        fetch_id=fetch_one,
        run_id=run_id,
        canonical_article_id=article_id,
    )
    second = _item(
        connection,
        feed_identity_id=feed_two,
        fetch_id=fetch_two,
        run_id=run_id,
        canonical_article_id=article_id,
    )

    assert first is not None
    assert second is None
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 1


# ---------------------------------------------------------------------------
# insert_source_item: entry-id dedup, only where there is no article
# ---------------------------------------------------------------------------


def test_urlless_entry_dedups_on_feed_and_entry_id(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_id = _fetch(connection, feed_identity_id=feed_id, run_id=run_id)

    first = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        source_entry_id="entry-1",
    )
    second = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        source_entry_id="entry-1",
    )

    assert first is not None
    assert second is None
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 1


def test_two_urlless_entries_with_different_entry_ids_both_insert(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_id = _fetch(connection, feed_identity_id=feed_id, run_id=run_id)

    first = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        source_entry_id="entry-1",
    )
    second = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        source_entry_id="entry-2",
    )

    assert first is not None
    assert second is not None
    assert first != second
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 2


def test_entry_id_dedup_does_not_apply_once_articles_differ(
    connection: sqlite3.Connection,
) -> None:
    """Entry-id dedup is a fallback only where there is no article to dedup
    on. Two entries sharing an entry id but resolving to distinct articles
    must both insert: the entry-id index never fires once an article is
    present, so it cannot collapse genuinely different articles."""
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_id = _fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    article_one = upsert_article(
        connection,
        canonical_url="https://example.com/one",
        publisher_key="example.com",
        now=moment(),
    )
    article_two = upsert_article(
        connection,
        canonical_url="https://example.com/two",
        publisher_key="example.com",
        now=moment(),
    )

    first = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        canonical_article_id=article_one,
        source_entry_id="shared-entry",
    )
    second = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        canonical_article_id=article_two,
        source_entry_id="shared-entry",
    )

    assert first is not None
    assert second is not None
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 2


def test_item_with_neither_article_nor_entry_id_always_inserts(
    connection: sqlite3.Connection,
) -> None:
    """With no article and no entry id, there is nothing to dedup on: every
    call inserts a fresh row, however many times it is repeated."""
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_id = _fetch(connection, feed_identity_id=feed_id, run_id=run_id)

    first = _item(
        connection, feed_identity_id=feed_id, fetch_id=fetch_id, run_id=run_id
    )
    second = _item(
        connection, feed_identity_id=feed_id, fetch_id=fetch_id, run_id=run_id
    )

    assert first is not None
    assert second is not None
    assert first != second


# ---------------------------------------------------------------------------
# latest_validators
# ---------------------------------------------------------------------------


def test_latest_validators_ignores_failed_fetches(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)

    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        outcome="modified",
        etag="good-etag",
        last_modified="good-last-modified",
    )
    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        outcome="failed",
        etag="should-be-ignored",
        last_modified="should-be-ignored",
    )

    etag, last_modified = latest_validators(connection, feed_identity_id=feed_id)
    assert etag == "good-etag"
    assert last_modified == "good-last-modified"


def test_latest_validators_returns_none_when_only_failed_fetches_exist(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        outcome="failed",
        etag="should-be-ignored",
        last_modified="should-be-ignored",
    )

    etag, last_modified = latest_validators(connection, feed_identity_id=feed_id)
    assert etag is None
    assert last_modified is None


def test_latest_validators_returns_none_with_no_fetch_history(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    etag, last_modified = latest_validators(connection, feed_identity_id=feed_id)
    assert etag is None
    assert last_modified is None


# ---------------------------------------------------------------------------
# source_item_counts
# ---------------------------------------------------------------------------


def test_source_item_counts_reports_totals_and_run_scoped_creation(
    connection: sqlite3.Connection,
) -> None:
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_one = insert_run(connection)
    fetch_one = _fetch(connection, feed_identity_id=feed_id, run_id=run_one)
    article_one = upsert_article(
        connection,
        canonical_url="https://example.com/one",
        publisher_key="example.com",
        now=moment(),
    )
    _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_one,
        run_id=run_one,
        canonical_article_id=article_one,
    )

    run_two = insert_run(connection)
    fetch_two = _fetch(connection, feed_identity_id=feed_id, run_id=run_two)
    article_two = upsert_article(
        connection,
        canonical_url="https://example.com/two",
        publisher_key="example.com",
        now=moment(),
    )
    _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_two,
        run_id=run_two,
        canonical_article_id=article_two,
    )
    # A third item, discovered in run_two, with no article at all.
    _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_two,
        run_id=run_two,
        source_entry_id="entry-only",
    )

    counts = source_item_counts(connection, run_id=run_two)
    assert counts.total == 3
    assert counts.articles_total == 2
    assert counts.created_in_run == 2
