"""Mechanical Wikipedia search query forms (name-derived only).

Primary forms (``exact``, ``comma_swap``) open a plan. Accent fallback is a
separate generator for the two-phase K23 path: callers must not pre-insert
accent at plan open. No nickname map, no model-invented aliases, no initials
expansion (K7).
"""

from __future__ import annotations

import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass

from notable_person_finder.people.identity import collapse_whitespace
from notable_person_finder.people.repository import mechanical_search_name

QUERY_PLAN_VERSION = 1

_VARIANT_EXACT = "exact"
_VARIANT_COMMA_SWAP = "comma_swap"
_VARIANT_ACCENT = "accent_fallback"


@dataclass(frozen=True, slots=True)
class QueryFormSpec:
    """One planned search string and its mechanical variant kind."""

    variant_kind: str
    query_text: str


def generate_primary_query_forms(
    names: Sequence[str], *, max_forms: int
) -> tuple[QueryFormSpec, ...]:
    """Build primary (plan-open) query forms from operational sourced names.

    Emits honorific-stripped, whitespace-collapsed exact forms and mechanical
    ``Last, First...`` → ``First... Last`` swaps. Never emits ``accent_fallback``.
    Deduplicates identical query texts; caps at ``max_forms``.
    """
    if max_forms < 1:
        raise ValueError("max_forms must be >= 1")

    out: list[QueryFormSpec] = []
    seen: set[str] = set()

    def _try_add(kind: str, text: str) -> bool:
        if not text or text in seen:
            return False
        if len(out) >= max_forms:
            return False
        seen.add(text)
        out.append(QueryFormSpec(variant_kind=kind, query_text=text))
        return True

    for raw in names:
        if len(out) >= max_forms:
            break
        collapsed = collapse_whitespace(raw)
        if not collapsed:
            continue

        exact = collapse_whitespace(mechanical_search_name(collapsed))
        if exact:
            _try_add(_VARIANT_EXACT, exact)

        if len(out) >= max_forms:
            break

        swapped = _comma_swap(collapsed)
        if swapped is not None:
            # Swap may still carry a leading honorific after reordering.
            swapped_clean = collapse_whitespace(mechanical_search_name(swapped))
            if swapped_clean:
                _try_add(_VARIANT_COMMA_SWAP, swapped_clean)

    return tuple(out)


def generate_accent_fallback_forms(
    primary_query_texts: Sequence[str],
    *,
    existing_query_texts: set[str],
    remaining_budget: int,
) -> tuple[QueryFormSpec, ...]:
    """Unicode accent-stripped forms of primary query texts (K23 secondary).

    Never identity evidence. Skips texts already present (or equal after strip).
    Caps at ``remaining_budget`` (``max_query_forms - count(existing forms)``).
    """
    if remaining_budget < 1:
        return ()

    out: list[QueryFormSpec] = []
    seen: set[str] = set(existing_query_texts)

    for text in primary_query_texts:
        if len(out) >= remaining_budget:
            break
        original = collapse_whitespace(text)
        stripped = collapse_whitespace(strip_accents(original))
        # No diacritics to remove, already planned, or duplicate strip result.
        if not stripped or stripped == original or stripped in seen:
            continue
        seen.add(stripped)
        out.append(QueryFormSpec(variant_kind=_VARIANT_ACCENT, query_text=stripped))

    return tuple(out)


def strip_accents(value: str) -> str:
    """NFD-decompose and drop combining marks; preserve non-accent structure."""
    decomposed = unicodedata.normalize("NFD", value)
    return "".join(char for char in decomposed if unicodedata.category(char) != "Mn")


def _comma_swap(name: str) -> str | None:
    """If ``Last, First...``, return ``First... Last``; otherwise ``None``."""
    if "," not in name:
        return None
    left, right = name.split(",", 1)
    last = left.strip()
    rest = right.strip()
    if not last or not rest:
        return None
    return collapse_whitespace(f"{rest} {last}")
