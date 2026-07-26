"""Shared name normalization utilities."""

from __future__ import annotations

import re
import unicodedata


_PARENS_RE = re.compile(r"\s*\([^)]*\)")
_WS_RE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str:
    if not name:
        return ""
    name = name.strip()
    name = _PARENS_RE.sub("", name)
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower()
    name = _WS_RE.sub(" ", name).strip()
    return name


def sort_by_priority_recency(records: list[dict]) -> list[dict]:
    """Sort records by feed_priority (ascending) then published_at_utc (descending, newest first).

    Records with no priority sort last; records with no date sort to the bottom within tier.
    Uses stable sort to preserve relative order within each priority tier's date ordering.
    """
    _INF = float("inf")
    # First stable sort: by published_at_utc descending (newest first)
    result = sorted(records, key=lambda r: r.get("published_at_utc") or "", reverse=True)
    # Second stable sort: by feed_priority ascending (lower number = higher priority)
    result.sort(key=lambda r: r.get("feed_priority") if r.get("feed_priority") is not None else _INF)
    return result
