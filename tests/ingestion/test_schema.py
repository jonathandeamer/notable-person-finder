from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations, load_migrations
from notable_person_finder.providers.failures import FailureCategory
from tests.ingestion.helpers import insert_run, moment

# ---------------------------------------------------------------------------
# Migration application
# ---------------------------------------------------------------------------


def test_migration_0003_applies_to_a_fresh_database(
    connection: sqlite3.Connection,
) -> None:
    versions = {
        row["version"]
        for row in connection.execute("SELECT version FROM schema_migration")
    }
    assert {1, 2, 3} <= versions


def test_migration_0003_applies_on_top_of_an_existing_0002_database(
    tmp_path: Path,
) -> None:
    database = tmp_path / "notable.sqlite3"
    backups = tmp_path / "backups"
    connection = connect_database(database)
    try:
        early_migrations = tuple(
            migration for migration in load_migrations() if migration.version <= 2
        )
        first = apply_migrations(connection, database, backups, early_migrations)
        assert first.applied_versions == (1, 2)

        ingestion_migrations = tuple(
            migration for migration in load_migrations() if migration.version <= 3
        )
        second = apply_migrations(connection, database, backups, ingestion_migrations)
        assert second.applied_versions == (3,)

        versions = {
            row["version"]
            for row in connection.execute("SELECT version FROM schema_migration")
        }
        assert versions == {1, 2, 3}
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Tables and columns
# ---------------------------------------------------------------------------

_EXPECTED_COLUMNS = {
    "feed_identity": {
        "id",
        "key",
        "current_label",
        "current_url",
        "first_seen_at",
        "last_seen_at",
    },
    "feed_fetch": {
        "id",
        "feed_identity_id",
        "run_id",
        "requested_at",
        "requested_url",
        "resolved_url",
        "redirect_chain_json",
        "http_status",
        "etag",
        "last_modified",
        "outcome",
        "feed_type",
        "parse_outcome",
        "parser_warnings_json",
        "failure_category",
        "entry_count",
        "response_bytes",
    },
    "canonical_article": {
        "id",
        "canonical_url",
        "publisher_key",
        "first_seen_at",
    },
    "article_url_alias": {
        "id",
        "canonical_article_id",
        "url",
        "kind",
        "first_seen_at",
    },
    "source_item": {
        "id",
        "feed_identity_id",
        "discovered_by_fetch_id",
        "discovered_by_run_id",
        "canonical_article_id",
        "source_entry_id",
        "original_url",
        "title_raw",
        "title_text",
        "summary_raw",
        "summary_text",
        "author_raw",
        "published_raw",
        "published_at",
        "published_issue",
        "url_issue",
        "discovered_at",
        "current_triage_observation_id",
    },
}


def test_every_ingestion_table_exists(connection: sqlite3.Connection) -> None:
    names = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert set(_EXPECTED_COLUMNS) <= names


@pytest.mark.parametrize("table", sorted(_EXPECTED_COLUMNS))
def test_table_has_expected_columns(connection: sqlite3.Connection, table: str) -> None:
    columns = {row["name"] for row in connection.execute(f"PRAGMA table_info({table})")}
    assert columns == _EXPECTED_COLUMNS[table]


# ---------------------------------------------------------------------------
# Fixture helpers: minimal valid rows to mutate for CHECK/unique/FK tests
# ---------------------------------------------------------------------------


def _feed_identity(connection: sqlite3.Connection, *, key: str = "feed-a") -> int:
    cursor = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        )
        VALUES (?, 'Label', 'https://example.com/feed', ?, ?)
        """,
        (key, moment(), moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _feed_fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    outcome: str = "modified",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        )
        VALUES (?, ?, ?, 'https://example.com/feed', ?)
        """,
        (feed_identity_id, run_id, moment(), outcome),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _canonical_article(
    connection: sqlite3.Connection, *, canonical_url: str = "https://example.com/a"
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
        VALUES (?, 'example.com', ?)
        """,
        (canonical_url, moment()),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


def _source_item(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    discovered_by_fetch_id: int,
    discovered_by_run_id: int,
    canonical_article_id: int | None = None,
    source_entry_id: str | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            canonical_article_id, source_entry_id, discovered_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            feed_identity_id,
            discovered_by_fetch_id,
            discovered_by_run_id,
            canonical_article_id,
            source_entry_id,
            moment(),
        ),
    )
    assert cursor.lastrowid is not None
    return cursor.lastrowid


# ---------------------------------------------------------------------------
# CHECK constraints
# ---------------------------------------------------------------------------


def test_feed_identity_first_seen_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_identity (
                key, current_label, current_url, first_seen_at, last_seen_at
            )
            VALUES ('feed-bad', 'Label', 'https://example.com/feed',
                    '2026-07-25 06:00:00', ?)
            """,
            (moment(),),
        )


def test_feed_identity_last_seen_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_identity (
                key, current_label, current_url, first_seen_at, last_seen_at
            )
            VALUES ('feed-bad2', 'Label', 'https://example.com/feed', ?,
                    '2026-07-25 06:00:00')
            """,
            (moment(),),
        )


def test_feed_fetch_outcome_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'stale')
            """,
            (feed_id, run_id, moment()),
        )


def test_feed_fetch_parse_outcome_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome,
                parse_outcome
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'modified', 'garbled')
            """,
            (feed_id, run_id, moment()),
        )


def test_feed_fetch_failure_category_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome,
                failure_category
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'failed', 'made_up_category')
            """,
            (feed_id, run_id, moment()),
        )


def test_feed_fetch_entry_count_rejects_a_negative_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome,
                entry_count
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'modified', -1)
            """,
            (feed_id, run_id, moment()),
        )


def test_feed_fetch_response_bytes_rejects_a_negative_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome,
                response_bytes
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'modified', -1)
            """,
            (feed_id, run_id, moment()),
        )


def test_canonical_article_first_seen_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES ('https://example.com/bad', 'example.com', '2026-07-25 06:00:00')
            """
        )


def test_article_url_alias_kind_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    article_id = _canonical_article(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            )
            VALUES (?, 'https://example.com/alias', 'search_result', ?)
            """,
            (article_id, moment()),
        )


def test_article_url_alias_first_seen_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    article_id = _canonical_article(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            )
            VALUES (?, 'https://example.com/alias', 'feed_original',
                    '2026-07-25 06:00:00')
            """,
            (article_id,),
        )


def test_source_item_discovered_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                discovered_at
            )
            VALUES (?, ?, ?, '2026-07-25 06:00:00')
            """,
            (feed_id, fetch_id, run_id),
        )


def test_source_item_published_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                published_at, discovered_at
            )
            VALUES (?, ?, ?, '2026-07-25 06:00:00', ?)
            """,
            (feed_id, fetch_id, run_id, moment()),
        )


def test_source_item_published_issue_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                published_issue, discovered_at
            )
            VALUES (?, ?, ?, 'nonsensical', ?)
            """,
            (feed_id, fetch_id, run_id, moment()),
        )


def test_source_item_url_issue_rejects_an_unlisted_value(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                url_issue, discovered_at
            )
            VALUES (?, ?, ?, 'nonsensical', ?)
            """,
            (feed_id, fetch_id, run_id, moment()),
        )


# ---------------------------------------------------------------------------
# Unique indexes
# ---------------------------------------------------------------------------


def test_feed_identity_key_rejects_a_duplicate(connection: sqlite3.Connection) -> None:
    _feed_identity(connection, key="dup-feed")
    with pytest.raises(sqlite3.IntegrityError):
        _feed_identity(connection, key="dup-feed")


def test_canonical_article_canonical_url_rejects_a_duplicate(
    connection: sqlite3.Connection,
) -> None:
    _canonical_article(connection, canonical_url="https://example.com/dup")
    with pytest.raises(sqlite3.IntegrityError):
        _canonical_article(connection, canonical_url="https://example.com/dup")


def test_article_url_alias_url_rejects_a_duplicate(
    connection: sqlite3.Connection,
) -> None:
    article_id = _canonical_article(connection)
    other_article_id = _canonical_article(
        connection, canonical_url="https://example.com/other"
    )
    connection.execute(
        """
        INSERT INTO article_url_alias (canonical_article_id, url, kind, first_seen_at)
        VALUES (?, 'https://example.com/alias-dup', 'feed_original', ?)
        """,
        (article_id, moment()),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            )
            VALUES (?, 'https://example.com/alias-dup', 'feed_original', ?)
            """,
            (other_article_id, moment()),
        )


def test_source_item_article_index_rejects_a_second_claim_on_the_same_article(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    article_id = _canonical_article(connection)
    _source_item(
        connection,
        feed_identity_id=feed_id,
        discovered_by_fetch_id=fetch_id,
        discovered_by_run_id=run_id,
        canonical_article_id=article_id,
    )
    with pytest.raises(sqlite3.IntegrityError):
        _source_item(
            connection,
            feed_identity_id=feed_id,
            discovered_by_fetch_id=fetch_id,
            discovered_by_run_id=run_id,
            canonical_article_id=article_id,
        )


def test_source_item_entry_index_rejects_a_second_urlless_claim_on_the_same_entry(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    _source_item(
        connection,
        feed_identity_id=feed_id,
        discovered_by_fetch_id=fetch_id,
        discovered_by_run_id=run_id,
        source_entry_id="entry-1",
    )
    with pytest.raises(sqlite3.IntegrityError):
        _source_item(
            connection,
            feed_identity_id=feed_id,
            discovered_by_fetch_id=fetch_id,
            discovered_by_run_id=run_id,
            source_entry_id="entry-1",
        )


def test_source_item_entry_index_does_not_apply_once_an_article_is_present(
    connection: sqlite3.Connection,
) -> None:
    """The entry-id index is scoped to `canonical_article_id IS NULL`.

    Two entries that happen to share an entry id but each resolved to their
    own (distinct) article must both be able to insert -- entry-id dedup is a
    fallback only for entries with no article to dedup on, not a competing
    key once one exists.
    """
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    article_one = _canonical_article(connection, canonical_url="https://example.com/1")
    article_two = _canonical_article(connection, canonical_url="https://example.com/2")

    _source_item(
        connection,
        feed_identity_id=feed_id,
        discovered_by_fetch_id=fetch_id,
        discovered_by_run_id=run_id,
        canonical_article_id=article_one,
        source_entry_id="shared-entry",
    )
    # Must not raise: the partial index does not cover rows with an article.
    _source_item(
        connection,
        feed_identity_id=feed_id,
        discovered_by_fetch_id=fetch_id,
        discovered_by_run_id=run_id,
        canonical_article_id=article_two,
        source_entry_id="shared-entry",
    )


# ---------------------------------------------------------------------------
# Foreign keys
# ---------------------------------------------------------------------------


def test_foreign_keys_are_enforced_by_the_connection(
    connection: sqlite3.Connection,
) -> None:
    """A guard on the guard: if `PRAGMA foreign_keys` were ever off, every FK
    test below would pass vacuously by never raising for the right reason."""
    (enabled,) = connection.execute("PRAGMA foreign_keys").fetchone()
    assert enabled == 1


def test_feed_fetch_rejects_an_unknown_feed_identity(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            )
            VALUES (999999, ?, ?, 'https://example.com/feed', 'modified')
            """,
            (run_id, moment()),
        )


def test_feed_fetch_rejects_an_unknown_run(connection: sqlite3.Connection) -> None:
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            )
            VALUES (?, 999999, ?, 'https://example.com/feed', 'modified')
            """,
            (feed_id, moment()),
        )


def test_article_url_alias_rejects_an_unknown_canonical_article(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO article_url_alias (
                canonical_article_id, url, kind, first_seen_at
            )
            VALUES (999999, 'https://example.com/orphan', 'feed_original', ?)
            """,
            (moment(),),
        )


def test_source_item_rejects_an_unknown_feed_identity(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                discovered_at
            )
            VALUES (999999, ?, ?, ?)
            """,
            (fetch_id, run_id, moment()),
        )


def test_source_item_rejects_an_unknown_discovering_run(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                discovered_at
            )
            VALUES (?, ?, 999999, ?)
            """,
            (feed_id, fetch_id, moment()),
        )


def test_source_item_rejects_an_unknown_canonical_article(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    fetch_id = _feed_fetch(connection, feed_identity_id=feed_id, run_id=run_id)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                canonical_article_id, discovered_at
            )
            VALUES (?, ?, ?, 999999, ?)
            """,
            (feed_id, fetch_id, run_id, moment()),
        )


def test_source_item_rejects_an_unknown_discovering_fetch(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO source_item (
                feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
                discovered_at
            )
            VALUES (?, 999999, ?, ?)
            """,
            (feed_id, run_id, moment()),
        )


# ---------------------------------------------------------------------------
# feed_fetch: timestamp shape, the full failure vocabulary, and the coupling
# ---------------------------------------------------------------------------


def test_feed_fetch_requested_at_rejects_a_non_zulu_timestamp(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            )
            VALUES (?, ?, '2026-07-25 06:00:00', 'https://example.com/feed',
                    'modified')
            """,
            (feed_id, run_id),
        )


@pytest.mark.parametrize("category", list(FailureCategory))
def test_feed_fetch_failure_category_accepts_every_failure_category(
    connection: sqlite3.Connection, category: FailureCategory
) -> None:
    """Parametrised over the enum itself, not a copied list: a copied list
    would drift, and a new `FailureCategory` member absent from the migration's
    CHECK would then fail for the first time in production rather than here."""
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome,
            failure_category
        )
        VALUES (?, ?, ?, 'https://example.com/feed', 'failed', ?)
        """,
        (feed_id, run_id, moment(), str(category)),
    )


def test_feed_fetch_failed_outcome_requires_a_failure_category(
    connection: sqlite3.Connection,
) -> None:
    """`0002` couples these on `attempt`; `0003` couples them here for the same
    reason: a failed fetch with no category cannot be explained by the digest.
    """
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome
            )
            VALUES (?, ?, ?, 'https://example.com/feed', 'failed')
            """,
            (feed_id, run_id, moment()),
        )


@pytest.mark.parametrize("outcome", ["modified", "not_modified"])
def test_feed_fetch_a_non_failed_outcome_rejects_a_failure_category(
    connection: sqlite3.Connection, outcome: str
) -> None:
    """The other direction of the same coupling: a successful fetch carrying a
    category would read as a failure to anything grouping on that column."""
    run_id = insert_run(connection)
    feed_id = _feed_identity(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO feed_fetch (
                feed_identity_id, run_id, requested_at, requested_url, outcome,
                failure_category
            )
            VALUES (?, ?, ?, 'https://example.com/feed', ?, 'network')
            """,
            (feed_id, run_id, moment(), outcome),
        )
