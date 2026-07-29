from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta

from notable_person_finder.runs.clock import utc_timestamp

# A fixed base moment for deterministic test timestamps. Every timestamp used
# in these tests is derived from `utc_timestamp`, which always emits
# microseconds -- a hard-coded whole-second literal like "...:00Z" compares
# incorrectly against it under SQLite's TEXT ordering, so no test here should
# ever write one by hand.
_BASE_MOMENT = datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)


def moment(offset_seconds: float = 0.0) -> str:
    """A canonical timestamp string, offset from a fixed base moment."""
    return utc_timestamp(_BASE_MOMENT + timedelta(seconds=offset_seconds))


@contextmanager
def immediate(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Stand in for the caller transaction the domain writers require.

    `record_fetch`, `upsert_article`, `record_alias`, and `insert_source_item`
    are transaction-neutral: in production they run inside the BEGIN IMMEDIATE
    transaction `runs.repository.complete_work` opens around its
    `domain_writes` callback. Tests must supply the same shape, so this mirrors
    that transaction rather than letting the writers run in autocommit mode --
    which they refuse to do.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def unique_fingerprint() -> str:
    """A syntactically valid (64 hex character) configuration fingerprint."""
    return (uuid.uuid4().hex + uuid.uuid4().hex)[:64]


def insert_configuration_snapshot(
    connection: sqlite3.Connection, *, fingerprint: str | None = None
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, '{}', ?)
        """,
        (fingerprint or unique_fingerprint(), moment()),
    )
    assert cursor.lastrowid is not None
    # Commit rather than leaving `sqlite3`'s implicit transaction open: an
    # open transaction here would make the next `BEGIN IMMEDIATE` -- the shape
    # every domain-write test needs -- fail as a nested transaction.
    connection.commit()
    return cursor.lastrowid


def insert_run(
    connection: sqlite3.Connection, *, configuration_snapshot_id: int | None = None
) -> int:
    """Seed one `run` row (and, unless given, its own configuration snapshot).

    `feed_fetch.run_id` and `source_item.discovered_by_run_id` reference
    `run`, so repository and schema tests that exercise those tables need a
    real row here rather than a bare integer.
    """
    snapshot_id = configuration_snapshot_id or insert_configuration_snapshot(connection)
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at
        )
        VALUES ('running', ?, 'Europe/Paris', ?, ?, ?)
        """,
        (snapshot_id, moment(-3600), moment(), moment()),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return cursor.lastrowid
