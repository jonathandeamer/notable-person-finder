from __future__ import annotations

from dataclasses import replace

import pytest

from notable.wiki_contract import (
    Candidate,
    MatchInvalid,
    PageFact,
    build_candidates,
    match_schema,
    validate_match,
)


def _base_page() -> PageFact:
    return PageFact(
        page_id=1,
        title="Ana Poy",
        namespace=0,
        missing=False,
        is_redirect=False,
        is_disambiguation=False,
        description="Sculptor",
        extract="Ana Poy is a sculptor.",
        categories=("Sculptors",),
    )


def _page(**overrides: object) -> PageFact:
    return replace(_base_page(), **overrides)  # type: ignore[arg-type]


def _candidates(**overrides) -> tuple[Candidate, ...]:
    return build_candidates(
        (_page(**overrides),), max_extract_characters=1000, max_categories_per_page=20
    )


# -- schema ----------------------------------------------------------------


def test_schema_root_is_an_object_not_a_union():
    schema = match_schema()
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_selected_page_id_is_nullable():
    schema = match_schema()
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert properties["selected_page_id"]["type"] == ["integer", "null"]


def test_schema_forbids_additional_properties():
    schema = match_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == set(schema["required"])  # type: ignore[arg-type]


# -- build_candidates --------------------------------------------------------


def test_main_namespace_non_dab_page_becomes_a_candidate():
    candidates = _candidates()
    assert len(candidates) == 1
    assert candidates[0].page_id == 1
    assert {fact.field for fact in candidates[0].facts} == {
        "description",
        "extract",
        "category",
    }


def test_non_main_namespace_page_is_excluded():
    assert _candidates(namespace=1) == ()


def test_disambiguation_page_is_excluded():
    assert _candidates(is_disambiguation=True) == ()


def test_redirect_page_is_excluded():
    assert _candidates(is_redirect=True) == ()


def test_missing_page_is_excluded():
    assert _candidates(missing=True) == ()


def test_duplicate_page_ids_are_deduplicated():
    candidates = build_candidates(
        (_page(), _page()), max_extract_characters=1000, max_categories_per_page=20
    )
    assert len(candidates) == 1


def test_extract_is_bounded():
    candidates = build_candidates(
        (_page(extract="x" * 100),),
        max_extract_characters=10,
        max_categories_per_page=20,
    )
    extract_fact = next(f for f in candidates[0].facts if f.field == "extract")
    assert len(extract_fact.text) == 10


def test_categories_are_bounded():
    page = _page(categories=tuple(f"Cat{i}" for i in range(30)))
    candidates = build_candidates(
        (page,), max_extract_characters=1000, max_categories_per_page=5
    )
    assert len([f for f in candidates[0].facts if f.field == "category"]) == 5


def test_fact_ids_are_unique_across_candidates():
    pages = (_page(page_id=1), _page(page_id=2, title="Bo Li"))
    candidates = build_candidates(
        pages, max_extract_characters=1000, max_categories_per_page=20
    )
    ids = [fact.id for candidate in candidates for fact in candidate.facts]
    assert len(ids) == len(set(ids))


# -- validate_match ----------------------------------------------------------


def _good(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "outcome": "matching_page",
        "selected_page_id": 1,
        "supporting_fact_ids": [],
        "conflicting_fact_ids": [],
        "rationale": "Same person.",
    }
    return base | overrides


def test_matching_page_requires_a_valid_selected_page_id():
    candidates = _candidates()
    validate_match(_good(), candidates=candidates, truncated=False)
    with pytest.raises(MatchInvalid, match="selected_page_id"):
        validate_match(
            _good(selected_page_id=None), candidates=candidates, truncated=False
        )
    with pytest.raises(MatchInvalid, match="selected_page_id"):
        validate_match(
            _good(selected_page_id=999), candidates=candidates, truncated=False
        )


def test_non_matching_outcomes_must_leave_selected_page_id_null():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="null"):
        validate_match(
            _good(outcome="uncertain", selected_page_id=1),
            candidates=candidates,
            truncated=False,
        )


def test_truncated_search_forbids_no_matching_page():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="truncated"):
        validate_match(
            _good(outcome="no_matching_page", selected_page_id=None),
            candidates=candidates,
            truncated=True,
        )


def test_uncertain_is_unaffected_by_truncation():
    candidates = _candidates()
    result = validate_match(
        _good(outcome="uncertain", selected_page_id=None),
        candidates=candidates,
        truncated=True,
    )
    assert result.outcome == "uncertain"


def test_unknown_fact_reference_is_rejected():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="unknown fact"):
        validate_match(
            _good(supporting_fact_ids=["f99"]), candidates=candidates, truncated=False
        )


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(MatchInvalid):
        validate_match({"nonsense": True}, candidates=(), truncated=False)
