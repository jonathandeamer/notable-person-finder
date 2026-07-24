from __future__ import annotations

from pathlib import Path

from notable_person_finder.db.connection import connect_database


def test_writable_connection_enables_required_pragmas(tmp_path: Path) -> None:
    connection = connect_database(tmp_path / "notable.sqlite3")
    try:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 5_000
    finally:
        connection.close()
