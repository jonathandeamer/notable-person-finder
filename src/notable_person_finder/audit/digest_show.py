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


def _run_row(
    connection: sqlite3.Connection, *, run_id: int | None
) -> sqlite3.Row | None:
    if not _table_present(connection, "run"):
        return None
    if run_id is None:
        return connection.execute(
            "SELECT id, state, digest_path, digest_sha256 FROM run "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return connection.execute(
        "SELECT id, state, digest_path, digest_sha256 FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()


def locate_digest(
    connection: sqlite3.Connection, *, run_id: int | None
) -> DigestLocation:
    run_row = _run_row(connection, run_id=run_id)
    if run_row is None:
        if run_id is None:
            raise DigestLookupError("no run has been recorded yet")
        raise DigestLookupError(f"no run found with id {run_id}")

    resolved_run_id = int(run_row["id"])

    if _table_present(connection, "digest"):
        digest_row = connection.execute(
            """
            SELECT file_path, content_hash, timezone, window_start, window_end,
                   run_state, created_at
            FROM digest
            WHERE run_id = ?
            ORDER BY id DESC LIMIT 1
            """,
            (resolved_run_id,),
        ).fetchone()
        if digest_row is not None:
            return DigestLocation(
                run_id=resolved_run_id,
                file_path=digest_row["file_path"],
                content_hash=digest_row["content_hash"],
                timezone=digest_row["timezone"],
                window_start=digest_row["window_start"],
                window_end=digest_row["window_end"],
                run_state=digest_row["run_state"],
                created_at=digest_row["created_at"],
                source="digest_table",
            )

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
