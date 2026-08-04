import pytest

from notable.coverage_contract import (
    AssessInvalid,
    assessment_schema,
    build_article_passage,
    validate_assessment,
)


def _good(**overrides):
    base = {
        "person_relation": "same_person",
        "coverage_depth": "significant",
        "content_types": ["profile"],
        "subject_relationship": "editorially_independent",
        "signals": [],
        "supporting_passage_ids": ["p1"],
        "rationale": "In-depth profile of the subject.",
    }
    return base | overrides


# -- build_article_passage --------------------------------------------------


def test_passage_is_bounded_and_flagged_when_truncated():
    passage = build_article_passage("x" * 100, max_characters=20)
    assert passage.id == "p1"
    assert len(passage.text) == 20
    assert passage.truncated is True


def test_passage_is_not_flagged_when_it_fits():
    passage = build_article_passage("short", max_characters=100)
    assert passage.truncated is False
    assert passage.text == "short"


# -- schema ------------------------------------------------------------------


def test_schema_root_is_an_object_not_a_union():
    schema = assessment_schema()
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_forbids_additional_properties_everywhere():
    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
                assert set(node["properties"]) == set(node["required"])
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(assessment_schema())


def test_schema_bounds_content_types_to_one_through_three_items():
    schema = assessment_schema()
    content_types = schema["properties"]["content_types"]
    assert content_types["minItems"] == 1
    assert content_types["maxItems"] == 3
    # uniqueItems is deliberately absent: OpenRouter's strict mode rejects it
    # (docs/findings.md). Deduplication is enforced by AssessmentOutput's
    # pydantic validator instead -- see test_duplicate_content_types_are_rejected.
    assert "uniqueItems" not in content_types


def test_schema_restricts_passage_ids_to_the_one_passage_this_contract_supplies():
    schema = assessment_schema()
    assert schema["properties"]["supporting_passage_ids"]["items"]["enum"] == ["p1"]
    signal_items = schema["properties"]["signals"]["items"]
    assert signal_items["properties"]["supporting_passage_ids"]["items"]["enum"] == [
        "p1"
    ]


# -- validate_assessment ------------------------------------------------------


def test_valid_output_parses():
    result = validate_assessment(_good())
    assert result.person_relation == "same_person"
    assert result.content_types == ("profile",)


def test_a_signal_may_be_included():
    result = validate_assessment(
        _good(
            signals=[
                {
                    "kind": "attention",
                    "category": "major retrospective",
                    "claim": "First major US retrospective.",
                    "supporting_passage_ids": ["p1"],
                    "grounding": "source_text",
                }
            ]
        )
    )
    assert result.signals[0].category == "major retrospective"


def test_an_unknown_passage_id_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(supporting_passage_ids=["p9"]))


def test_more_than_three_content_types_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(
            _good(content_types=["profile", "review", "interview", "obituary"])
        )


def test_duplicate_content_types_are_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(content_types=["profile", "profile"]))


def test_an_unknown_content_type_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(content_types=["not_a_real_type"]))


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(AssessInvalid):
        validate_assessment({"nonsense": True})
