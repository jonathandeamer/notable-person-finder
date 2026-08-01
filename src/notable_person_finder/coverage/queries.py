"""Mechanical coverage query forms (exact / alias / context / obituary) (K6).

Inputs are plain operational name and fact sequences for pure tests. No
nickname map, no model-invented aliases, no initials expansion. Obituary
variants require an explicit death/obituary fact signal from the caller.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from notable_person_finder.people.identity import collapse_whitespace
from notable_person_finder.people.repository import mechanical_search_name

COVERAGE_QUERY_PLAN_VERSION = 1

_VARIANT_EXACT = "exact"
_VARIANT_EXACT_OBITUARY = "exact_obituary"
_VARIANT_ALIAS = "alias"
_VARIANT_CONTEXT = "context"

_OBITUARY_SUFFIX = "obituary"

# A merged person accumulates one context fact per mention across the whole
# merge closure. Joining every one of them produces a multi-hundred-character
# query Brave will reject -- a wasted paid call. These caps are deliberately
# small: a context form is a last-resort broadening, not an exhaustive one.
MAX_CONTEXT_TERMS = 3
MAX_CONTEXT_TERM_CHARACTERS = 40
MAX_CONTEXT_QUERY_CHARACTERS = 200


@dataclass(frozen=True, slots=True)
class QueryFormSpec:
    """One planned Brave search string and its mechanical variant kind."""

    variant_kind: str
    query_text: str


def generate_exact_forms(
    names: Sequence[str],
    *,
    max_exact_forms: int,
    death_supported: bool = False,
) -> tuple[QueryFormSpec, ...]:
    """Stage-1 exact-name (and optional exact_obituary) forms at plan open.

    Emits one quoted honorific-stripped form per distinct exact search string,
    then, when ``death_supported`` is true, inserts ``exact_obituary`` variants
    for those base strings while budget remains. Never invents death facts.
    """
    if max_exact_forms < 1:
        raise ValueError("max_exact_forms must be >= 1")

    out: list[QueryFormSpec] = []
    seen: set[str] = set()
    base_texts: list[str] = []

    def _try_add(kind: str, text: str) -> bool:
        if not text or text in seen:
            return False
        if len(out) >= max_exact_forms:
            return False
        seen.add(text)
        out.append(QueryFormSpec(variant_kind=kind, query_text=text))
        return True

    for raw in names:
        if len(out) >= max_exact_forms:
            break
        collapsed = collapse_whitespace(raw)
        if not collapsed:
            continue
        exact = collapse_whitespace(mechanical_search_name(collapsed))
        if not exact:
            continue
        quoted = _quote_phrase(exact)
        if _try_add(_VARIANT_EXACT, quoted):
            base_texts.append(quoted)

    if death_supported:
        for base in base_texts:
            if len(out) >= max_exact_forms:
                break
            obit = f"{base} {_OBITUARY_SUFFIX}"
            _try_add(_VARIANT_EXACT_OBITUARY, obit)

    return tuple(out)


def generate_alias_forms(
    alias_names: Sequence[str],
    *,
    existing_query_texts: set[str],
    max_alias_forms: int,
) -> tuple[QueryFormSpec, ...]:
    """Stage-2 alias forms from provenance-backed operational alias names.

    Skips empty names, honorific-only residue, and texts already scheduled.
    Cap at ``max_alias_forms`` (0 yields an empty tuple).
    """
    if max_alias_forms < 0:
        raise ValueError("max_alias_forms must be >= 0")
    if max_alias_forms == 0:
        return ()

    out: list[QueryFormSpec] = []
    seen: set[str] = set(existing_query_texts)

    for raw in alias_names:
        if len(out) >= max_alias_forms:
            break
        collapsed = collapse_whitespace(raw)
        if not collapsed:
            continue
        exact = collapse_whitespace(mechanical_search_name(collapsed))
        if not exact:
            continue
        quoted = _quote_phrase(exact)
        if not quoted or quoted in seen:
            continue
        seen.add(quoted)
        out.append(QueryFormSpec(variant_kind=_VARIANT_ALIAS, query_text=quoted))

    return tuple(out)


def generate_context_form(
    display_name: str,
    context_terms: Sequence[str],
    *,
    existing_query_texts: set[str],
) -> QueryFormSpec | None:
    """At most one stage-3 context form from display name + grounded facts.

    Context terms are caller-supplied profession/place/work strings already
    stripped of empty values. Returns ``None`` when no non-empty terms remain
    after collapse, or when the composed query is already scheduled.
    """
    collapsed_name = collapse_whitespace(display_name)
    if not collapsed_name:
        return None
    exact = collapse_whitespace(mechanical_search_name(collapsed_name))
    if not exact:
        return None

    quoted = _quote_phrase(exact)
    cleaned_terms: list[str] = []
    seen: set[str] = set()
    budget = MAX_CONTEXT_QUERY_CHARACTERS - len(quoted)
    for term in context_terms:
        if len(cleaned_terms) >= MAX_CONTEXT_TERMS:
            break
        collapsed = collapse_whitespace(term)[:MAX_CONTEXT_TERM_CHARACTERS].strip()
        if not collapsed or collapsed in seen:
            continue
        if len(collapsed) + 1 > budget:
            continue
        seen.add(collapsed)
        cleaned_terms.append(collapsed)
        budget -= len(collapsed) + 1
    if not cleaned_terms:
        return None

    composed = f"{quoted} {' '.join(cleaned_terms)}"
    if composed in existing_query_texts:
        return None
    return QueryFormSpec(variant_kind=_VARIANT_CONTEXT, query_text=composed)


def _quote_phrase(text: str) -> str:
    """Wrap a phrase in double quotes for Brave phrase intent."""
    return f'"{text}"'
