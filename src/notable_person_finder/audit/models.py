"""Read-only audit models.

`audit/` may import only from `config/`, `db/`, and `obs/` within this
package; it reads every other package's tables with its own SQL rather than
importing them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DigestLocation:
    run_id: int
    file_path: str
    content_hash: str
    timezone: str | None  # None on the pre-0008 run-column fallback
    window_start: str | None
    window_end: str | None
    run_state: str | None
    created_at: str | None
    source: str  # "digest_table" | "run_columns"
