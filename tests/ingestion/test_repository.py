from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.ingestion.repository import (
    feed_identities,
    insert_source_item,
    latest_validators,
    record_alias,
    record_fetch,
    source_item_counts,
    upsert_article,
    upsert_feed_identity,
)
from tests.ingestion.helpers import immediate, insert_run, moment


def _fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    outcome: str = "modified",
    etag: str | None = None,
    last_modified: str | None = None,
) -> int:
    with immediate(connection):
        return record_fetch(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            requested_at=moment(),
            requested_url="https://example.com/feed",
            resolved_url="https://example.com/feed",
            redirect_chain_json=None,
            http_status=200 if outcome != "not_modified" else 304,
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


def _article(connection: sqlite3.Connection, *, canonical_url: str) -> int:
    with immediate(connection):
        return upsert_article(
            connection,
            canonical_url=canonical_url,
            publisher_key="example.com",
            now=moment(),
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
    with immediate(connection):
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
    article_id = _article(connection, canonical_url="https://example.com/article")

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
    article_id = _article(
        connection, canonical_url="https://example.com/shared-article"
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
    article_one = _article(connection, canonical_url="https://example.com/one")
    article_two = _article(connection, canonical_url="https://example.com/two")

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
# feed_identities
# ---------------------------------------------------------------------------


def test_feed_identities_reads_every_stored_identity_in_key_order(
    connection: sqlite3.Connection,
) -> None:
    """Ordered by key, not by insertion.

    Seeding iterates this list to retire work for feeds that left
    configuration, and the handler's `prepare` resolves a work item's subject
    through it. Neither depends on a *particular* order, but both benefit from a
    stable one: identical configuration must retire identical work in an
    identical sequence from one run to the next, or a diagnostic log's ordering
    becomes noise. Insertion order would make that vary with the order feeds
    were first discovered.

    Three feeds, inserted in an order that matches neither ascending nor
    descending id. With only two rows, reverse-id order and key order coincide
    for one of the two possible insertion sequences, so a two-row fixture
    cannot tell "ordered by key" from "ordered by id" at all -- it passes
    against both.
    """
    zulu = upsert_feed_identity(
        connection,
        key="zulu",
        label="Zulu",
        url="https://z.example.com/f",
        now=moment(),
    )
    alpha = upsert_feed_identity(
        connection,
        key="alpha",
        label="Alpha",
        url="https://a.example.com/f",
        now=moment(1),
    )
    mike = upsert_feed_identity(
        connection,
        key="mike",
        label="Mike",
        url="https://m.example.com/f",
        now=moment(2),
    )

    identities = feed_identities(connection)

    assert [identity.key for identity in identities] == ["alpha", "mike", "zulu"]
    # Neither ascending nor descending insertion order, both of which this
    # sequence makes distinct from the assertion above.
    assert [identity.id for identity in identities] == [alpha, mike, zulu]
    assert [zulu, alpha, mike] != [alpha, mike, zulu]
    assert identities[0].current_label == "Alpha"
    assert identities[0].current_url == "https://a.example.com/f"


def test_feed_identities_is_empty_before_anything_is_seeded(
    connection: sqlite3.Connection,
) -> None:
    assert feed_identities(connection) == ()


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

    etag, last_modified = latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    )
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

    etag, last_modified = latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    )
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
    etag, last_modified = latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    )
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
    article_one = _article(connection, canonical_url="https://example.com/one")
    _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_one,
        run_id=run_one,
        canonical_article_id=article_one,
    )

    run_two = insert_run(connection)
    fetch_two = _fetch(connection, feed_identity_id=feed_id, run_id=run_two)
    article_two = _article(connection, canonical_url="https://example.com/two")
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


# ---------------------------------------------------------------------------
# Transaction discipline: the domain writers are transaction-neutral
# ---------------------------------------------------------------------------


def test_record_fetch_refuses_to_run_outside_a_transaction(
    connection: sqlite3.Connection,
) -> None:
    """The guard fires before any SQL, so the row ids here need not exist:
    what is under test is that autocommit mode is refused outright, not that
    the statement would otherwise have succeeded."""
    with pytest.raises(RuntimeError, match="record_fetch requires an active"):
        record_fetch(
            connection,
            feed_identity_id=1,
            run_id=1,
            requested_at=moment(),
            requested_url="https://example.com/feed",
            resolved_url=None,
            redirect_chain_json=None,
            http_status=200,
            etag=None,
            last_modified=None,
            outcome="modified",
            feed_type="rss",
            parse_outcome="ok",
            parser_warnings_json=None,
            failure_category=None,
            entry_count=0,
            response_bytes=0,
        )


def test_upsert_article_refuses_to_run_outside_a_transaction(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(RuntimeError, match="upsert_article requires an active"):
        upsert_article(
            connection,
            canonical_url="https://example.com/a",
            publisher_key="example.com",
            now=moment(),
        )


def test_record_alias_refuses_to_run_outside_a_transaction(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(RuntimeError, match="record_alias requires an active"):
        record_alias(
            connection,
            canonical_article_id=1,
            url="https://example.com/a?utm_source=x",
            kind="feed_original",
            now=moment(),
        )


def test_insert_source_item_refuses_to_run_outside_a_transaction(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(RuntimeError, match="insert_source_item requires an active"):
        insert_source_item(
            connection,
            feed_identity_id=1,
            discovered_by_fetch_id=1,
            discovered_by_run_id=1,
            canonical_article_id=None,
            source_entry_id="entry-1",
            original_url=None,
            title_raw=None,
            title_text=None,
            summary_raw=None,
            summary_text=None,
            author_raw=None,
            published_raw=None,
            published_at=None,
            published_issue=None,
            url_issue=None,
            now=moment(),
        )


def test_all_four_domain_writers_work_inside_one_caller_transaction(
    connection: sqlite3.Connection,
) -> None:
    """The production shape: every domain write for one work item lands in a
    single caller-owned BEGIN IMMEDIATE, as `complete_work` supplies."""
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)

    with immediate(connection):
        fetch_id = record_fetch(
            connection,
            feed_identity_id=feed_id,
            run_id=run_id,
            requested_at=moment(),
            requested_url="https://example.com/feed",
            resolved_url="https://example.com/feed",
            redirect_chain_json=None,
            http_status=200,
            etag="E1",
            last_modified=None,
            outcome="modified",
            feed_type="rss",
            parse_outcome="ok",
            parser_warnings_json=None,
            failure_category=None,
            entry_count=1,
            response_bytes=512,
        )
        article_id = upsert_article(
            connection,
            canonical_url="https://example.com/article",
            publisher_key="example.com",
            now=moment(),
        )
        record_alias(
            connection,
            canonical_article_id=article_id,
            url="https://example.com/article?utm_source=feed",
            kind="feed_original",
            now=moment(),
        )
        item_id = insert_source_item(
            connection,
            feed_identity_id=feed_id,
            discovered_by_fetch_id=fetch_id,
            discovered_by_run_id=run_id,
            canonical_article_id=article_id,
            source_entry_id="entry-1",
            original_url="https://example.com/article?utm_source=feed",
            title_raw="Raw",
            title_text="Title",
            summary_raw=None,
            summary_text=None,
            author_raw=None,
            published_raw=None,
            published_at=None,
            published_issue="missing",
            url_issue=None,
            now=moment(),
        )

    assert item_id is not None
    for table in (
        "feed_fetch",
        "canonical_article",
        "article_url_alias",
        "source_item",
    ):
        (count,) = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        assert count == 1, table


def test_a_domain_write_does_not_commit_the_callers_unrelated_open_write(
    connection: sqlite3.Connection,
) -> None:
    """The bite of the finding: these writers must not commit on their own.

    A caller opens a transaction, makes an unrelated write, then performs a
    domain write. If the domain write committed -- as `connection.commit()`
    on a shared connection would -- the caller's own `rollback()` could no
    longer undo either write, and a settlement would already be durable
    before the handler finished.
    """
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)

    connection.execute("BEGIN IMMEDIATE")
    connection.execute(
        "UPDATE feed_identity SET current_label = 'Caller Edit' WHERE id = ?",
        (feed_id,),
    )
    record_fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        requested_at=moment(),
        requested_url="https://example.com/feed",
        resolved_url=None,
        redirect_chain_json=None,
        http_status=200,
        etag="E1",
        last_modified=None,
        outcome="modified",
        feed_type="rss",
        parse_outcome="ok",
        parser_warnings_json=None,
        failure_category=None,
        entry_count=1,
        response_bytes=512,
    )
    connection.rollback()

    row = connection.execute(
        "SELECT current_label FROM feed_identity WHERE id = ?", (feed_id,)
    ).fetchone()
    assert row["current_label"] == "Feed A"
    (fetches,) = connection.execute("SELECT COUNT(*) FROM feed_fetch").fetchone()
    assert fetches == 0


# ---------------------------------------------------------------------------
# latest_validators: latest wins, and a bare 304 does not erase what is known
# ---------------------------------------------------------------------------


def test_latest_validators_returns_the_newest_of_two_non_failed_fetches(
    connection: sqlite3.Connection,
) -> None:
    """Both fetches qualify, so only ordering decides: the newer must win, or
    a feed would keep offering a validator the publisher has already
    superseded."""
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
        etag="older-etag",
        last_modified="older-last-modified",
    )
    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        etag="newer-etag",
        last_modified="newer-last-modified",
    )

    assert latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    ) == (
        "newer-etag",
        "newer-last-modified",
    )


def test_a_bare_304_does_not_erase_the_validators_it_was_conditional_on(
    connection: sqlite3.Connection,
) -> None:
    """A 304 normally omits Last-Modified and often omits ETag too. If that
    validator-less row were taken as the answer, conditional requests would
    break permanently after the very first 304 and every feed body would be
    re-downloaded on every run."""
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
        etag="E1",
        last_modified="Fri, 24 Jul 2026 06:00:00 GMT",
    )
    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        outcome="not_modified",
        etag=None,
        last_modified=None,
    )

    etag, last_modified = latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    )
    assert etag == "E1"
    assert last_modified == "Fri, 24 Jul 2026 06:00:00 GMT"


def test_a_304_carrying_a_fresh_etag_supersedes_the_older_one(
    connection: sqlite3.Connection,
) -> None:
    """Skipping validator-less rows must not become "ignore 304s": a 304 that
    does carry an ETag is the newest word on this feed and wins."""
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
        etag="E1",
        last_modified=None,
    )
    _fetch(
        connection,
        feed_identity_id=feed_id,
        run_id=run_id,
        outcome="not_modified",
        etag="E2",
        last_modified=None,
    )

    etag, _ = latest_validators(
        connection,
        feed_identity_id=feed_id,
        requested_url="https://example.com/feed",
    )
    assert etag == "E2"


# ---------------------------------------------------------------------------
# insert_source_item: the scope of entry-id dedup
# ---------------------------------------------------------------------------


def test_the_same_entry_id_in_two_different_feeds_both_insert(
    connection: sqlite3.Connection,
) -> None:
    """Entry-id dedup is scoped to `(feed, entry_id)`, never `entry_id` alone.

    Entry ids are publisher-chosen and not globally unique -- 'post-1' or a
    bare integer is commonplace -- so an unscoped check would let one feed's
    entry suppress an unrelated entry in another feed entirely.
    """
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
    run_id = insert_run(connection)
    fetch_one = _fetch(connection, feed_identity_id=feed_one, run_id=run_id)
    fetch_two = _fetch(connection, feed_identity_id=feed_two, run_id=run_id)

    first = _item(
        connection,
        feed_identity_id=feed_one,
        fetch_id=fetch_one,
        run_id=run_id,
        source_entry_id="shared-entry",
    )
    second = _item(
        connection,
        feed_identity_id=feed_two,
        fetch_id=fetch_two,
        run_id=run_id,
        source_entry_id="shared-entry",
    )

    assert first is not None
    assert second is not None
    assert first != second
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 2


def test_an_entry_id_already_used_by_an_article_row_does_not_block_a_urlless_item(
    connection: sqlite3.Connection,
) -> None:
    """The entry index is confined to rows with no article, so an existing
    `(feed, entry-X, article A)` row is outside it. A later URL-less item
    reusing `entry-X` must insert: the article row is deduped on its article,
    and treating it as an entry-id duplicate would silently drop the only
    record of the URL-less entry."""
    feed_id = upsert_feed_identity(
        connection,
        key="feed-a",
        label="Feed A",
        url="https://example.com/feed",
        now=moment(),
    )
    run_id = insert_run(connection)
    fetch_id = _fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    article_id = _article(connection, canonical_url="https://example.com/a")

    with_article = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        canonical_article_id=article_id,
        source_entry_id="entry-X",
    )
    urlless = _item(
        connection,
        feed_identity_id=feed_id,
        fetch_id=fetch_id,
        run_id=run_id,
        source_entry_id="entry-X",
    )

    assert with_article is not None
    assert urlless is not None
    assert with_article != urlless
    (count,) = connection.execute("SELECT COUNT(*) FROM source_item").fetchone()
    assert count == 2
