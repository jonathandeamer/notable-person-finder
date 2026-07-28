from __future__ import annotations

import sqlite3

from notable_person_finder.ingestion.models import SourceItemCounts


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    """The row id of a just-completed INSERT.

    `sqlite3` types `lastrowid` as optional because it is None before any
    INSERT on the cursor. Every call site here has just inserted exactly one
    row, so None means the statement did not do what the caller assumed and
    must fail loudly rather than propagate a bad id.
    """
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT did not produce a row id")
    return row_id


def upsert_feed_identity(
    connection: sqlite3.Connection,
    *,
    key: str,
    label: str,
    url: str,
    now: str,
) -> int:
    """Create the feed identity keyed on `key`, or refresh its live fields.

    `key` is the configured stable identity, never the feed's URL, so a feed
    whose URL moves keeps its history. `first_seen_at` is set only on first
    insert; `current_label`, `current_url`, and `last_seen_at` always move to
    the latest observed values.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            INSERT INTO feed_identity (
                key, current_label, current_url, first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                current_label = excluded.current_label,
                current_url = excluded.current_url,
                last_seen_at = excluded.last_seen_at
            """,
            (key, label, url, now, now),
        )
        row = connection.execute(
            "SELECT id FROM feed_identity WHERE key = ?", (key,)
        ).fetchone()
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return int(row["id"])


def latest_validators(
    connection: sqlite3.Connection, *, feed_identity_id: int
) -> tuple[str | None, str | None]:
    """The ETag and Last-Modified to offer on the next fetch of this feed.

    Derived from the latest fetch whose `outcome` is not 'failed'. Validator
    state is deliberately not stored as mutable columns on `feed_identity`:
    deriving it here means a failed fetch can never overwrite the validators
    a previous, successful fetch offered.
    """
    row = connection.execute(
        """
        SELECT etag, last_modified FROM feed_fetch
         WHERE feed_identity_id = ? AND outcome <> 'failed'
         ORDER BY id DESC
         LIMIT 1
        """,
        (feed_identity_id,),
    ).fetchone()
    if row is None:
        return (None, None)
    return (row["etag"], row["last_modified"])


def record_fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    requested_at: str,
    requested_url: str,
    resolved_url: str | None,
    redirect_chain_json: str | None,
    http_status: int | None,
    etag: str | None,
    last_modified: str | None,
    outcome: str,
    feed_type: str | None,
    parse_outcome: str | None,
    parser_warnings_json: str | None,
    failure_category: str | None,
    entry_count: int | None,
    response_bytes: int | None,
) -> int:
    """Insert one immutable fetch attempt row; fetches are never rewritten."""
    cursor = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, resolved_url,
            redirect_chain_json, http_status, etag, last_modified, outcome,
            feed_type, parse_outcome, parser_warnings_json, failure_category,
            entry_count, response_bytes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            feed_identity_id,
            run_id,
            requested_at,
            requested_url,
            resolved_url,
            redirect_chain_json,
            http_status,
            etag,
            last_modified,
            outcome,
            feed_type,
            parse_outcome,
            parser_warnings_json,
            failure_category,
            entry_count,
            response_bytes,
        ),
    )
    connection.commit()
    return _last_row_id(cursor)


def upsert_article(
    connection: sqlite3.Connection, *, canonical_url: str, publisher_key: str, now: str
) -> int:
    """Create the canonical article, or return the identity of the existing one.

    `first_seen_at` is set only on first insert: `ON CONFLICT DO NOTHING`
    means a rediscovery of the same `canonical_url` never touches it.
    """
    connection.execute(
        """
        INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
        VALUES (?, ?, ?)
        ON CONFLICT(canonical_url) DO NOTHING
        """,
        (canonical_url, publisher_key, now),
    )
    connection.commit()
    row = connection.execute(
        "SELECT id FROM canonical_article WHERE canonical_url = ?", (canonical_url,)
    ).fetchone()
    return int(row["id"])


def record_alias(
    connection: sqlite3.Connection,
    *,
    canonical_article_id: int,
    url: str,
    kind: str,
    now: str,
) -> None:
    """Record `url` as resolving to `canonical_article_id`, once.

    `ON CONFLICT DO NOTHING` on the unique `url` column makes a rediscovery
    of an already-recorded alias a no-op rather than an error.
    """
    connection.execute(
        """
        INSERT INTO article_url_alias (canonical_article_id, url, kind, first_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO NOTHING
        """,
        (canonical_article_id, url, kind, now),
    )
    connection.commit()


def insert_source_item(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    discovered_by_fetch_id: int,
    discovered_by_run_id: int,
    canonical_article_id: int | None,
    source_entry_id: str | None,
    original_url: str | None,
    title_raw: str | None,
    title_text: str | None,
    summary_raw: str | None,
    summary_text: str | None,
    author_raw: str | None,
    published_raw: str | None,
    published_at: str | None,
    published_issue: str | None,
    url_issue: str | None,
    now: str,
) -> int | None:
    """Insert one source item, or report that it already exists.

    Returns `None` exactly when one of the two partial unique indexes on
    `source_item` means this item was already discovered:
    `source_item_article` when `canonical_article_id` is given, or
    `source_item_entry` (scoped to `feed_identity_id`) when it is not but
    `source_entry_id` is. With neither key, there is nothing to dedup on and
    the item is always inserted.

    The duplicate check is an explicit pre-check inside this transaction,
    not a catch of `sqlite3.IntegrityError` after a failed insert.
    `IntegrityError` is also raised for a foreign-key violation or a NOT
    NULL violation on a genuinely malformed call, and catching it broadly
    would misreport either of those as "already exists" -- turning a real
    bug into a silent no-op. `BEGIN IMMEDIATE` takes the write lock before
    the pre-check runs, so it cannot race a concurrent insert of the same
    item: the two are serialized, not merely both individually correct.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        if canonical_article_id is not None:
            duplicate = connection.execute(
                "SELECT 1 FROM source_item WHERE canonical_article_id = ?",
                (canonical_article_id,),
            ).fetchone()
        elif source_entry_id is not None:
            duplicate = connection.execute(
                """
                SELECT 1 FROM source_item
                 WHERE feed_identity_id = ? AND source_entry_id = ?
                   AND canonical_article_id IS NULL
                """,
                (feed_identity_id, source_entry_id),
            ).fetchone()
        else:
            duplicate = None

        if duplicate is not None:
            connection.rollback()
            return None

        cursor = connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                canonical_article_id, source_entry_id, original_url,
                title_raw, title_text, summary_raw, summary_text, author_raw,
                published_raw, published_at, published_issue, url_issue,
                discovered_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feed_identity_id,
                discovered_by_fetch_id,
                discovered_by_run_id,
                canonical_article_id,
                source_entry_id,
                original_url,
                title_raw,
                title_text,
                summary_raw,
                summary_text,
                author_raw,
                published_raw,
                published_at,
                published_issue,
                url_issue,
                now,
            ),
        )
        item_id = _last_row_id(cursor)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return item_id


def source_item_counts(
    connection: sqlite3.Connection, *, run_id: int
) -> SourceItemCounts:
    """Summarize `source_item` rows: whole-table totals plus this run's share."""
    total = int(
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"]
    )
    created_in_run = int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM source_item WHERE discovered_by_run_id = ?",
            (run_id,),
        ).fetchone()["n"]
    )
    articles_total = int(
        connection.execute(
            """
            SELECT COUNT(DISTINCT canonical_article_id) AS n FROM source_item
             WHERE canonical_article_id IS NOT NULL
            """
        ).fetchone()["n"]
    )
    return SourceItemCounts(
        total=total, created_in_run=created_in_run, articles_total=articles_total
    )
