from __future__ import annotations

import sqlite3

from notable_person_finder.ingestion.models import FeedIdentity, SourceItemCounts

# Two deliberate transaction disciplines live in this module.
#
# `record_fetch`, `upsert_article`, `record_alias`, and `insert_source_item`
# are transaction-neutral: they neither begin, commit, nor roll back. They are
# the domain writes a work-item handler performs, and `runs.repository`'s
# `complete_work` runs its `domain_writes` callback *inside* its own open
# BEGIN IMMEDIATE transaction so that a handler's domain rows and the
# settlement that justifies them commit or roll back together. A function that
# opened its own transaction there would raise `sqlite3.OperationalError:
# cannot start a transaction within a transaction`, and one that committed
# there would commit the settling transaction early -- publishing a settled
# work item before its domain rows were complete. Each guards its entry and
# refuses to run outside a transaction rather than silently writing in
# autocommit mode.
#
# `upsert_feed_identity` owns its own transaction: it is called during seeding,
# before any work item is claimed, so there is no caller transaction to join.
# `feed_identities`, `latest_validators`, and `source_item_counts` are reads and
# need neither.


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


def _require_transaction(connection: sqlite3.Connection, name: str) -> None:
    """Refuse to write outside a caller-owned transaction.

    Mirrors the guard `runs.budget`'s `reserve_in_transaction` uses, including
    its message shape: writing in autocommit mode would commit on its own and
    break the all-or-nothing settlement this module's writers depend on.
    """
    if not connection.in_transaction:
        raise RuntimeError(f"{name} requires an active transaction")


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


def feed_identities(connection: sqlite3.Connection) -> tuple[FeedIdentity, ...]:
    """Every feed identity ever seeded, ordered by key.

    A read, so it needs no transaction. Returns the whole table rather than
    taking a filter: both callers want it whole. Seeding compares it against
    configuration to find identities that no longer appear there, which cannot
    be expressed as a query because the configured set lives in a file rather
    than in SQLite; the handler's `prepare` resolves a work item's subject back
    to its feed. A deployment's feed list is measured in tens, so a full read
    once per run and once per fetch is cheaper than the round trips a narrower
    interface would need.
    """
    return tuple(
        FeedIdentity(
            id=int(row["id"]),
            key=row["key"],
            current_label=row["current_label"],
            current_url=row["current_url"],
        )
        for row in connection.execute(
            """
            SELECT id, key, current_label, current_url
              FROM feed_identity
             ORDER BY key
            """
        )
    )


def latest_validators(
    connection: sqlite3.Connection, *, feed_identity_id: int, requested_url: str
) -> tuple[str | None, str | None]:
    """The ETag and Last-Modified to offer for this requested feed URL.

    Derived from the latest fetch whose `outcome` is not 'failed'. Validator
    state is deliberately not stored as mutable columns on `feed_identity`:
    deriving it here means a failed fetch can never overwrite the validators
    a previous, successful fetch offered.

    Validators are representation-specific, whereas a feed identity remains
    stable when its configured URL moves. Restricting history to the exact
    requested URL prevents a validator learned from the old representation
    from eliciting an unusable 304 from the new one.

    Rows carrying *no* validator at all are skipped rather than treated as the
    answer. A 304 response normally omits Last-Modified and often omits ETag
    too, so the latest row is frequently validator-less; returning its empty
    pair would discard the validators an earlier fetch established and make
    every subsequent request unconditional -- re-downloading each feed body
    forever after the first 304. A row with nothing to offer offers nothing,
    so falling back to the newest row that does have a validator is strictly
    better and never worse.
    """
    row = connection.execute(
        """
        SELECT etag, last_modified FROM feed_fetch
         WHERE feed_identity_id = ? AND requested_url = ? AND outcome <> 'failed'
           AND (etag IS NOT NULL OR last_modified IS NOT NULL)
         ORDER BY id DESC
         LIMIT 1
        """,
        (feed_identity_id, requested_url),
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
    """Insert one immutable fetch attempt row; fetches are never rewritten.

    **The caller must already hold an open transaction; this function does not
    begin, commit, or roll back one.** It runs inside the settling transaction
    `runs.repository.complete_work` opens around its `domain_writes` callback,
    so a handler's domain rows and the settlement that justifies them commit
    or roll back together. Raises `RuntimeError` if no transaction is open.
    """
    _require_transaction(connection, "record_fetch")
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
    return _last_row_id(cursor)


def upsert_article(
    connection: sqlite3.Connection, *, canonical_url: str, publisher_key: str, now: str
) -> int:
    """Create the canonical article, or return the identity of the existing one.

    `first_seen_at` is set only on first insert: `ON CONFLICT DO NOTHING`
    means a rediscovery of the same `canonical_url` never touches it.

    **The caller must already hold an open transaction; this function does not
    begin, commit, or roll back one.** It runs inside the settling transaction
    `runs.repository.complete_work` opens around its `domain_writes` callback,
    so a handler's domain rows and the settlement that justifies them commit
    or roll back together. Raises `RuntimeError` if no transaction is open.
    """
    _require_transaction(connection, "upsert_article")
    connection.execute(
        """
        INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
        VALUES (?, ?, ?)
        ON CONFLICT(canonical_url) DO NOTHING
        """,
        (canonical_url, publisher_key, now),
    )
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

    **The caller must already hold an open transaction; this function does not
    begin, commit, or roll back one.** It runs inside the settling transaction
    `runs.repository.complete_work` opens around its `domain_writes` callback,
    so a handler's domain rows and the settlement that justifies them commit
    or roll back together. Raises `RuntimeError` if no transaction is open.
    """
    _require_transaction(connection, "record_alias")
    connection.execute(
        """
        INSERT INTO article_url_alias (canonical_article_id, url, kind, first_seen_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO NOTHING
        """,
        (canonical_article_id, url, kind, now),
    )


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

    **The caller must already hold an open transaction; this function does not
    begin, commit, or roll back one.** It runs inside the settling transaction
    `runs.repository.complete_work` opens around its `domain_writes` callback,
    so a handler's domain rows and the settlement that justifies them commit
    or roll back together. Raises `RuntimeError` if no transaction is open.

    The duplicate check is an explicit pre-check, not a catch of
    `sqlite3.IntegrityError` after a failed insert. `IntegrityError` is also
    raised for a foreign-key violation or a NOT NULL violation on a genuinely
    malformed call, and catching it broadly would misreport either of those as
    "already exists" -- turning a real bug into a silent no-op.

    The pre-check is race-free only if the caller's transaction is IMMEDIATE:
    the write lock must already be held when the check reads, or two
    concurrent inserts of the same item can both see no duplicate and the
    second fails on the unique index instead of returning `None`. Every
    production caller reaches this through `complete_work`, which opens
    `BEGIN IMMEDIATE`. `sqlite3` exposes no way to tell a deferred transaction
    from an immediate one at runtime -- `in_transaction` is true for both --
    so this is a documented contract, not an enforced one, and the guard above
    deliberately does not pretend otherwise.
    """
    _require_transaction(connection, "insert_source_item")

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
    return _last_row_id(cursor)


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
