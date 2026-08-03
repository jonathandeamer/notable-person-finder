"""SQLite persistence: two state tables the pipeline reads, two logs it never does."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS item (
    url           TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL,
    settled_at    TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS surfaced (
    identity_key     TEXT PRIMARY KEY,
    last_surfaced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at         TEXT NOT NULL,
    ended_at           TEXT NOT NULL,
    n_items_settled    INTEGER NOT NULL,
    n_items_incomplete INTEGER NOT NULL,
    cost_usd           TEXT NOT NULL,
    status             TEXT NOT NULL,
    digest_path        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lead (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL,
    identity_key  TEXT NOT NULL,
    display_name  TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    rank_key_json TEXT NOT NULL,
    detail_json   TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Everything the run log needs, derived in one place.

    Two conditions make a run partial -- items that raised `Incomplete`, and a
    run cut short by the spend cap. A mutable status string set only on the
    budget path reports a run with failed items as `ok`.
    """

    settled: list[str]
    incomplete: list[str]
    capped: bool
    cost_usd: Decimal
    started_at: str = field(default_factory=_now)

    @property
    def status(self) -> str:
        return "partial" if (self.incomplete or self.capped) else "ok"


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    # -- state the pipeline reads -------------------------------------------

    def is_eligible(self, url: str, *, max_attempts: int) -> bool:
        """Eligible means new, or previously incomplete and still retryable."""
        row = self.connection.execute(
            "SELECT settled_at, attempts FROM item WHERE url = ?", (url,)
        ).fetchone()
        if row is None:
            return True
        settled_at, attempts = row
        return settled_at is None and attempts < max_attempts

    def attempts(self, url: str) -> int:
        row = self.connection.execute(
            "SELECT attempts FROM item WHERE url = ?", (url,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def commit(
        self, settled: list[str], incomplete: list[str], surfaced_keys: list[str]
    ) -> None:
        """Commit the state tables in one transaction, after the digest lands."""
        now = _now()
        with self.connection:
            for url in settled:
                self.connection.execute(
                    "INSERT INTO item (url, first_seen_at, settled_at, attempts) "
                    "VALUES (?, ?, ?, 0) "
                    "ON CONFLICT(url) DO UPDATE SET settled_at = excluded.settled_at",
                    (url, now, now),
                )
            for url in incomplete:
                self.connection.execute(
                    "INSERT INTO item (url, first_seen_at, settled_at, attempts) "
                    "VALUES (?, ?, NULL, 1) "
                    "ON CONFLICT(url) DO UPDATE SET attempts = item.attempts + 1",
                    (url, now),
                )
            for key in surfaced_keys:
                self.connection.execute(
                    "INSERT INTO surfaced (identity_key, last_surfaced_at) "
                    "VALUES (?, ?) "
                    "ON CONFLICT(identity_key) DO UPDATE SET "
                    "last_surfaced_at = excluded.last_surfaced_at",
                    (key, now),
                )

    # -- logs the pipeline never reads --------------------------------------

    def log(
        self, summary: RunSummary, leads: list[dict[str, Any]], digest_path: str
    ) -> None:
        """Append-only diagnostics, best effort and outside the state transaction.

        Sharing `commit`'s transaction would let a serialization error roll back
        the item markers *after* the digest file was written, so the next run
        repeats all the work and hits the identical deterministic failure. One
        bad log row would livelock the product.
        """
        try:
            with self.connection:
                run_id = self._insert_run(summary, digest_path)
                for lead in leads:
                    self.connection.execute(
                        "INSERT INTO lead (run_id, identity_key, display_name, "
                        "outcome, rank_key_json, detail_json) VALUES (?,?,?,?,?,?)",
                        (
                            run_id,
                            lead["identity_key"],
                            lead["display_name"],
                            lead["outcome"],
                            json.dumps(lead["rank_key"], sort_keys=True),
                            json.dumps(lead["detail"], sort_keys=True),
                        ),
                    )
        except Exception:
            logger.warning(
                "run log write failed; state is already committed", exc_info=True
            )

    def _insert_run(self, summary: RunSummary, digest_path: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO run (started_at, ended_at, n_items_settled, "
            "n_items_incomplete, cost_usd, status, digest_path) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                summary.started_at,
                _now(),
                len(summary.settled),
                len(summary.incomplete),
                str(summary.cost_usd),
                summary.status,
                digest_path,
            ),
        )
        return int(cursor.lastrowid or 0)
