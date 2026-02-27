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
