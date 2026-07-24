from __future__ import annotations

import sqlite3
from pathlib import Path


def connect_database(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    resolved = path.expanduser().resolve()
    if readonly:
        connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    else:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(resolved)

    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 5000")
    if not readonly:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
    return connection
