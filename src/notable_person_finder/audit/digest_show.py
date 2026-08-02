"""Locate and verify a persisted digest for `notable digest show`."""

from __future__ import annotations

import hashlib
import sqlite3

from notable_person_finder.audit.models import DigestLocation


class DigestLookupError(Exception):
    """Carries an operator-facing message; never a bare traceback."""


def _table_present(connection: sqlite3.Connection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        is not None
    )


def _digest_row_for_run(
    connection: sqlite3.Connection, *, run_id: int
) -> sqlite3.Row | None:
    if not _table_present(connection, "digest"):
        return None
    return connection.execute(
        """
        SELECT run_id, file_path, content_hash, timezone, window_start,
               window_end, run_state, created_at
        FROM digest
        WHERE run_id = ?
        ORDER BY id DESC LIMIT 1
        """,
        (run_id,),
    ).fetchone()


def _newest_digest_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    if not _table_present(connection, "digest"):
        return None
    return connection.execute(
        """
        SELECT run_id, file_path, content_hash, timezone, window_start,
               window_end, run_state, created_at
        FROM digest
        ORDER BY run_id DESC, id DESC LIMIT 1
        """
    ).fetchone()


def _run_row(connection: sqlite3.Connection, *, run_id: int) -> sqlite3.Row | None:
    if not _table_present(connection, "run"):
        return None
    return connection.execute(
        "SELECT id, state, digest_path, digest_sha256 FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()


def _newest_run_row(connection: sqlite3.Connection) -> sqlite3.Row | None:
    if not _table_present(connection, "run"):
        return None
    return connection.execute(
        "SELECT id, state, digest_path, digest_sha256 FROM run ORDER BY id DESC LIMIT 1"
    ).fetchone()


def _location_from_digest_row(digest_row: sqlite3.Row) -> DigestLocation:
    return DigestLocation(
        run_id=int(digest_row["run_id"]),
        file_path=digest_row["file_path"],
        content_hash=digest_row["content_hash"],
        timezone=digest_row["timezone"],
        window_start=digest_row["window_start"],
        window_end=digest_row["window_end"],
        run_state=digest_row["run_state"],
        created_at=digest_row["created_at"],
        source="digest_table",
    )


def _location_from_run_columns(run_row: sqlite3.Row) -> DigestLocation:
    resolved_run_id = int(run_row["id"])
    file_path = run_row["digest_path"]
    content_hash = run_row["digest_sha256"]
    if file_path is None or content_hash is None:
        raise DigestLookupError(f"run {resolved_run_id} produced no digest")
    return DigestLocation(
        run_id=resolved_run_id,
        file_path=file_path,
        content_hash=content_hash,
        timezone=None,
        window_start=None,
        window_end=None,
        run_state=run_row["state"],
        created_at=None,
        source="run_columns",
    )


def locate_digest(
    connection: sqlite3.Connection, *, run_id: int | None
) -> DigestLocation:
    if run_id is not None:
        digest_row = _digest_row_for_run(connection, run_id=run_id)
        if digest_row is not None:
            return _location_from_digest_row(digest_row)
        run_row = _run_row(connection, run_id=run_id)
        if run_row is None:
            raise DigestLookupError(f"no run found with id {run_id}")
        return _location_from_run_columns(run_row)

    # No RUN_ID given. K4: "the row with the highest run_id" of the DIGEST
    # table — deliberately the highest run_id *present in the digest table*,
    # not the highest run_id overall. If the newest run was interrupted and
    # wrote no digest, this still surfaces the last run that actually
    # produced one, rather than erroring just because a newer, digest-less
    # run exists. A database with a `digest` table but no rows in it yet (or
    # one migrated before the `digest` table existed) falls back to the
    # newest run's own `digest_path`/`digest_sha256` columns.
    digest_row = _newest_digest_row(connection)
    if digest_row is not None:
        return _location_from_digest_row(digest_row)

    run_row = _newest_run_row(connection)
    if run_row is None:
        raise DigestLookupError("no run has been recorded yet")
    return _location_from_run_columns(run_row)


def read_verified_digest(location: DigestLocation) -> bytes:
    try:
        with open(location.file_path, "rb") as handle:
            data = handle.read()
    except FileNotFoundError as error:
        raise DigestLookupError(
            f"digest file not found: {location.file_path}"
        ) from error
    except OSError as error:
        raise DigestLookupError(
            f"could not read digest file: {location.file_path} ({error})"
        ) from error

    actual_hash = hashlib.sha256(data).hexdigest()
    if actual_hash != location.content_hash:
        raise DigestLookupError(
            f"digest file hash mismatch: {location.file_path} "
            f"(expected {location.content_hash}, got {actual_hash})"
        )

    try:
        data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DigestLookupError(
            f"digest file is not valid UTF-8: {location.file_path}"
        ) from error

    return data
