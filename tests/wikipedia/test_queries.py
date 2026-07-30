"""Pure query-form generation for Wikipedia identity retrieval (K7/K23).

No HTTP, no SQLite. Forms are name-derived only: honorific strip, comma-swap,
and (separately) accent fallback. Nickname maps and accent-at-primary are
forbidden.
"""

from __future__ import annotations

import pytest

from notable_person_finder.wikipedia.queries import (
    QUERY_PLAN_VERSION,
    QueryFormSpec,
    generate_accent_fallback_forms,
    generate_primary_query_forms,
)


def _texts(forms: tuple[QueryFormSpec, ...]) -> list[str]:
    return [form.query_text for form in forms]


def _kinds(forms: tuple[QueryFormSpec, ...]) -> list[str]:
    return [form.variant_kind for form in forms]


def test_query_plan_version_is_positive() -> None:
    assert QUERY_PLAN_VERSION >= 1


def test_exact_form_strips_leading_honorific_and_collapses_whitespace() -> None:
    forms = generate_primary_query_forms(
        ["  Dr.   Ada   Lovelace  ", "SIR Isaac Newton"],
        max_forms=6,
    )
    assert _texts(forms) == ["Ada Lovelace", "Isaac Newton"]
    assert _kinds(forms) == ["exact", "exact"]


def test_exact_form_preserves_useful_casing() -> None:
    forms = generate_primary_query_forms(["mcDonald Smith"], max_forms=4)
    assert forms[0].query_text == "mcDonald Smith"
    assert forms[0].variant_kind == "exact"


def test_comma_swap_emits_first_last_from_last_comma_first() -> None:
    forms = generate_primary_query_forms(["Lovelace, Ada"], max_forms=6)
    assert (forms[0].variant_kind, forms[0].query_text) == ("exact", "Lovelace, Ada")
    assert (forms[1].variant_kind, forms[1].query_text) == (
        "comma_swap",
        "Ada Lovelace",
    )


def test_comma_swap_table_cases() -> None:
    cases = [
        ("Doe, Jane", "Jane Doe"),
        ("García Márquez, Gabriel", "Gabriel García Márquez"),
        ("Smith, John Q.", "John Q. Smith"),
    ]
    for source, swapped in cases:
        forms = generate_primary_query_forms([source], max_forms=6)
        by_kind = {form.variant_kind: form.query_text for form in forms}
        assert by_kind["exact"] == source
        assert by_kind["comma_swap"] == swapped


def test_no_comma_means_no_comma_swap_variant() -> None:
    forms = generate_primary_query_forms(["Ada Lovelace"], max_forms=6)
    assert _kinds(forms) == ["exact"]


def test_nickname_map_is_not_applied_positive_control() -> None:
    """Map-like common nicknames must not expand (legacy NICKNAME_MAP deleted).

    Positive control: if a nickname map were active, 'Nick White' would yield
    'Nicholas White'. Assert that expansion is absent while the source form
    remains.
    """
    forms = generate_primary_query_forms(
        ["Nick White", "Bill Clinton", "Bob Smith"],
        max_forms=16,
    )
    texts = set(_texts(forms))
    assert "Nick White" in texts
    assert "Bill Clinton" in texts
    assert "Bob Smith" in texts
    assert "Nicholas White" not in texts
    assert "William Clinton" not in texts
    assert "Robert Smith" not in texts
    assert "accent_fallback" not in _kinds(forms)


def test_primary_generator_never_emits_accent_fallback() -> None:
    forms = generate_primary_query_forms(
        ["José García", "François Hollande", "Søren Kierkegaard"],
        max_forms=16,
    )
    assert "accent_fallback" not in _kinds(forms)
    # Accented source strings are preserved on the primary path.
    assert "José García" in _texts(forms)
    assert "François Hollande" in _texts(forms)


def test_accent_fallback_generator_only_when_called() -> None:
    primaries = generate_primary_query_forms(["José García"], max_forms=6)
    assert _texts(primaries) == ["José García"]

    accents = generate_accent_fallback_forms(
        ["José García"],
        existing_query_texts=set(_texts(primaries)),
        remaining_budget=4,
    )
    assert len(accents) == 1
    assert accents[0].variant_kind == "accent_fallback"
    assert accents[0].query_text == "Jose Garcia"


def test_accent_fallback_skips_identical_and_existing_texts() -> None:
    accents = generate_accent_fallback_forms(
        ["Ada Lovelace", "José", "José"],
        existing_query_texts={"Jose"},
        remaining_budget=6,
    )
    # Ada has no accents → strip equals original → skip; José already present.
    assert accents == ()


def test_accent_fallback_respects_remaining_budget() -> None:
    accents = generate_accent_fallback_forms(
        ["José", "François", "Søren Kierkegaard"],
        existing_query_texts=set(),
        remaining_budget=2,
    )
    assert len(accents) == 2
    assert all(form.variant_kind == "accent_fallback" for form in accents)
    assert _texts(accents) == ["Jose", "Francois"]


def test_accent_fallback_skips_when_strip_is_noop() -> None:
    # ø does not NFD-decompose to a combining mark; strip is a no-op.
    accents = generate_accent_fallback_forms(
        ["Søren", "Ada Lovelace"],
        existing_query_texts=set(),
        remaining_budget=6,
    )
    assert accents == ()


def test_primary_deduplicates_identical_query_strings() -> None:
    forms = generate_primary_query_forms(
        ["Ada Lovelace", "  Ada   Lovelace  ", "Dr. Ada Lovelace"],
        max_forms=8,
    )
    # All three normalize to the same exact text once.
    assert _texts(forms) == ["Ada Lovelace"]
    assert _kinds(forms) == ["exact"]


def test_comma_swap_dedupes_when_swap_equals_another_exact() -> None:
    forms = generate_primary_query_forms(
        ["Ada Lovelace", "Lovelace, Ada"],
        max_forms=8,
    )
    texts = _texts(forms)
    assert texts.count("Ada Lovelace") == 1
    assert "Lovelace, Ada" in texts


def test_max_query_forms_cap() -> None:
    names = [f"Person {i}" for i in range(10)]
    forms = generate_primary_query_forms(names, max_forms=3)
    assert len(forms) == 3
    assert _texts(forms) == ["Person 0", "Person 1", "Person 2"]


def test_max_forms_includes_comma_swap_in_budget() -> None:
    # First name consumes exact + comma_swap; third form is next exact only.
    forms = generate_primary_query_forms(
        ["Lovelace, Ada", "Newton, Isaac"],
        max_forms=3,
    )
    assert len(forms) == 3
    assert _kinds(forms) == ["exact", "comma_swap", "exact"]
    assert _texts(forms) == ["Lovelace, Ada", "Ada Lovelace", "Newton, Isaac"]


def test_skips_empty_and_whitespace_only_names() -> None:
    forms = generate_primary_query_forms(["", "   ", "Ada"], max_forms=4)
    assert _texts(forms) == ["Ada"]


def test_max_forms_must_be_at_least_one() -> None:
    with pytest.raises(ValueError, match="max_forms"):
        generate_primary_query_forms(["Ada"], max_forms=0)


def test_accent_remaining_budget_zero_returns_empty() -> None:
    assert (
        generate_accent_fallback_forms(
            ["José"],
            existing_query_texts=set(),
            remaining_budget=0,
        )
        == ()
    )
