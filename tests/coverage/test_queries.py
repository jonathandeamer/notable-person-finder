"""Coverage query form generation (K6): exact, alias, context, obituary."""

from __future__ import annotations

import pytest

from notable_person_finder.coverage.queries import (
    COVERAGE_QUERY_PLAN_VERSION,
    generate_alias_forms,
    generate_context_form,
    generate_exact_forms,
)


def test_query_plan_version_is_one() -> None:
    assert COVERAGE_QUERY_PLAN_VERSION == 1


def test_exact_forms_quoted_honorific_stripped_deduped() -> None:
    forms = generate_exact_forms(
        ("Sir Jane Doe", "Jane Doe", "  Jane   Doe  "),
        max_exact_forms=4,
        death_supported=False,
    )
    assert len(forms) == 1
    assert forms[0].variant_kind == "exact"
    assert forms[0].query_text == '"Jane Doe"'


def test_exact_always_emitted_before_obituary() -> None:
    forms = generate_exact_forms(
        ("Alex Rivera",),
        max_exact_forms=4,
        death_supported=True,
    )
    assert [f.variant_kind for f in forms] == ["exact", "exact_obituary"]
    assert forms[0].query_text == '"Alex Rivera"'
    assert forms[1].query_text == '"Alex Rivera" obituary'


def test_obituary_only_when_death_supported() -> None:
    without = generate_exact_forms(
        ("Alex Rivera",),
        max_exact_forms=4,
        death_supported=False,
    )
    assert all(f.variant_kind != "exact_obituary" for f in without)
    assert len(without) == 1

    with_death = generate_exact_forms(
        ("Alex Rivera",),
        max_exact_forms=4,
        death_supported=True,
    )
    assert any(f.variant_kind == "exact_obituary" for f in with_death)


def test_obituary_respects_max_exact_forms_budget() -> None:
    forms = generate_exact_forms(
        ("A One", "B Two"),
        max_exact_forms=2,
        death_supported=True,
    )
    # Two exact forms fill the budget; no room for obituaries.
    assert len(forms) == 2
    assert all(f.variant_kind == "exact" for f in forms)


def test_no_nickname_map_or_initials_expansion() -> None:
    forms = generate_exact_forms(
        ("Jonathan Smith",),
        max_exact_forms=8,
        death_supported=False,
    )
    texts = {f.query_text for f in forms}
    assert '"Jon Smith"' not in texts
    assert '"J. Smith"' not in texts
    assert '"JS"' not in texts
    assert texts == {'"Jonathan Smith"'}


def test_alias_forms_skip_existing_and_cap() -> None:
    forms = generate_alias_forms(
        ("Johnny Smith", "J. Smith", "Johnny Smith"),
        existing_query_texts={'"Jonathan Smith"'},
        max_alias_forms=2,
    )
    assert all(f.variant_kind == "alias" for f in forms)
    assert forms[0].query_text == '"Johnny Smith"'
    # second distinct alias
    assert len(forms) == 2
    assert forms[1].query_text == '"J. Smith"'

    empty = generate_alias_forms(
        ("Johnny Smith",),
        existing_query_texts={'"Johnny Smith"'},
        max_alias_forms=4,
    )
    assert empty == ()

    zero = generate_alias_forms(
        ("Johnny Smith",),
        existing_query_texts=set(),
        max_alias_forms=0,
    )
    assert zero == ()


def test_context_form_from_display_and_terms() -> None:
    form = generate_context_form(
        "Jane Doe",
        ("painter", "Paris"),
        existing_query_texts=set(),
    )
    assert form is not None
    assert form.variant_kind == "context"
    assert form.query_text == '"Jane Doe" painter Paris'


def test_context_form_none_without_terms_or_when_duplicate() -> None:
    assert generate_context_form("Jane Doe", (), existing_query_texts=set()) is None
    assert (
        generate_context_form("Jane Doe", ("  ",), existing_query_texts=set()) is None
    )
    existing = {'"Jane Doe" painter'}
    assert (
        generate_context_form("Jane Doe", ("painter",), existing_query_texts=existing)
        is None
    )


def test_max_exact_forms_must_be_positive() -> None:
    with pytest.raises(ValueError, match="max_exact_forms"):
        generate_exact_forms(("A",), max_exact_forms=0)
