import pytest

from notable.config import DetectConfig
from notable.detect_contract import (
    DetectionInvalid,
    build_passages,
    detection_schema,
    validate_detection,
)
from notable.feeds import SourceItem

CONFIG = DetectConfig(model="m", max_title_characters=20, max_summary_characters=30)


def _item(
    title: str | None = "Sculptor Ana Poy wins prize",
    summary: str | None = "Ana Poy showed in Paris.",
):
    return SourceItem(
        url="https://a.test/1",
        title=title,
        summary=summary,
        published_at=None,
        feed_key="a",
        publisher_label="A",
    )


def _mention(**overrides):
    base = {
        "exact_name": "Ana Poy",
        "canonical_name": "Ana Poy",
        "outcome": "research",
        "supporting_passage_ids": ["p1"],
        "identity_facts": [
            {
                "kind": "profession_or_role",
                "value": "Sculptor",
                "supporting_passage_ids": ["p1"],
            }
        ],
        "signals": [],
        "rationale": "Named subject.",
    }
    return base | overrides


def _output(**overrides):
    base = {
        "item_outcome": "research_people",
        "mentions": [_mention()],
        "overflow": False,
        "rationale": "One subject.",
    }
    return base | overrides


PASSAGES = build_passages(
    _item(title="Sculptor Ana Poy", summary="Ana Poy in Paris."), CONFIG
)


# -- passages ------------------------------------------------------------


def test_passages_are_bounded_and_flagged_when_truncated():
    passages = build_passages(_item(title="x" * 100, summary="y" * 100), CONFIG)
    title = next(p for p in passages if p.field == "title")
    assert len(title.text) == 20
    assert title.truncated is True


def test_absent_fields_produce_no_passage():
    assert [p.id for p in build_passages(_item(summary=None), CONFIG)] == ["p1"]


# -- schema --------------------------------------------------------------


def test_schema_root_is_an_object_not_a_union():
    # Strict structured output rejects a root-level anyOf with HTTP 400.
    schema = detection_schema(max_people=8)
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_caps_mentions_at_max_people():
    # The cap must be unrepresentable, not merely rejected after payment.
    schema = detection_schema(max_people=3)
    properties = schema["properties"]
    assert isinstance(properties, dict)
    mentions = properties["mentions"]
    assert isinstance(mentions, dict)
    assert mentions["maxItems"] == 3


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

    walk(detection_schema(max_people=8))


def _signal_schema() -> dict:
    schema = detection_schema(max_people=8)
    properties = schema["properties"]
    assert isinstance(properties, dict)
    mentions = properties["mentions"]
    assert isinstance(mentions, dict)
    items = mentions["items"]
    assert isinstance(items, dict)
    item_props = items["properties"]
    assert isinstance(item_props, dict)
    signals = item_props["signals"]
    assert isinstance(signals, dict)
    signal_items = signals["items"]
    assert isinstance(signal_items, dict)
    return signal_items


def test_signal_kind_and_category_are_paired_in_the_schema():
    # A nested union is accepted by strict mode and is what findings.md says
    # to use here. Independent enums let the model emit a caution category
    # under kind "attention" -- billable, then rejected.
    variants = {
        arm["properties"]["kind"]["enum"][0]: set(arm["properties"]["category"]["enum"])
        for arm in _signal_schema()["anyOf"]
    }
    assert set(variants) == {"attention", "caution"}
    assert "single_event_only" in variants["caution"]
    assert "single_event_only" not in variants["attention"]
    assert "major_achievement" in variants["attention"]
    assert "major_achievement" not in variants["caution"]


def test_schema_does_not_offer_domain_profile_grounding():
    # Phase 1 supplies no profile. Offering the value means paying for it
    # before the validator refuses it.
    for arm in _signal_schema()["anyOf"]:
        assert arm["properties"]["grounding"]["enum"] == ["source_text"]


# -- validation ----------------------------------------------------------


def test_valid_output_parses():
    result = validate_detection(_output(), passages=PASSAGES, max_people=8)
    assert result.mentions[0].exact_name == "Ana Poy"


def test_research_and_uncertain_mentions_are_research_worthy():
    for outcome in ("research", "uncertain"):
        result = validate_detection(
            _output(
                item_outcome="research_people"
                if outcome == "research"
                else "uncertain",
                mentions=[_mention(outcome=outcome)],
            ),
            passages=PASSAGES,
            max_people=8,
        )
        assert result.mentions[0].research_worthy is True


def test_do_not_research_is_not_research_worthy():
    result = validate_detection(
        _output(
            item_outcome="do_not_research",
            mentions=[_mention(outcome="do_not_research")],
        ),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].research_worthy is False


def test_more_mentions_than_the_cap_is_rejected():
    with pytest.raises(DetectionInvalid, match="max_people"):
        validate_detection(
            _output(mentions=[_mention(), _mention()]), passages=PASSAGES, max_people=1
        )


def test_unknown_passage_id_is_rejected():
    with pytest.raises(DetectionInvalid, match="passage"):
        validate_detection(
            _output(mentions=[_mention(supporting_passage_ids=["p9"])]),
            passages=PASSAGES,
            max_people=8,
        )


def test_name_grounding_is_case_sensitive():
    # Deliberate: a name is an identity, and case is part of it.
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(mentions=[_mention(exact_name="ana poy")]),
            passages=PASSAGES,
            max_people=8,
        )


def test_invented_name_is_rejected():
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(mentions=[_mention(exact_name="Someone Else")]),
            passages=PASSAGES,
            max_people=8,
        )


def test_a_name_inside_a_longer_word_is_not_grounded():
    # Plain substring containment accepts "Ana" for "Anastasia Poy" -- a name
    # the supplied text does not contain.
    passages = build_passages(
        _item(title="Anastasia Poyner wins", summary="Anastasia Poyner in Paris."),
        DetectConfig(model="m"),
    )
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(
                mentions=[_mention(exact_name="Ana", supporting_passage_ids=["p1"])]
            ),
            passages=passages,
            max_people=8,
        )


def test_a_name_adjacent_to_punctuation_is_still_grounded():
    # The boundary test must not reject a name that the text quotes or
    # parenthesizes -- a far more common shape than the case above.
    passages = build_passages(
        _item(title='"Ana Poy" wins', summary="The winner (Ana Poy) spoke."),
        DetectConfig(model="m"),
    )
    result = validate_detection(
        _output(mentions=[_mention(identity_facts=[], supporting_passage_ids=["p1"])]),
        passages=passages,
        max_people=8,
    )
    assert result.mentions[0].exact_name == "Ana Poy"


def test_overflow_requires_a_full_mention_list():
    # overflow claims there were more people than the cap allowed, which
    # cannot be true of a list with room left in it.
    with pytest.raises(DetectionInvalid, match="overflow"):
        validate_detection(
            _output(mentions=[_mention()], overflow=True),
            passages=PASSAGES,
            max_people=8,
        )


def test_overflow_is_accepted_when_the_list_is_full():
    result = validate_detection(
        _output(mentions=[_mention()], overflow=True), passages=PASSAGES, max_people=1
    )
    assert result.overflow is True


def test_item_outcome_uncertain_may_carry_no_mentions():
    # The ported prompt permits this explicitly: "either no mentions are
    # returned or at least one mention is `uncertain`". A rule requiring an
    # uncertain mention would reject valid output.
    result = validate_detection(
        _output(item_outcome="uncertain", mentions=[]), passages=PASSAGES, max_people=8
    )
    assert result.mentions == ()


def test_item_outcome_uncertain_may_carry_an_uncertain_mention():
    result = validate_detection(
        _output(item_outcome="uncertain", mentions=[_mention(outcome="uncertain")]),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].research_worthy is True


def test_item_outcome_uncertain_contradicts_a_research_mention():
    # "Item `uncertain` iff no mention is `research`". Without this the model
    # can return an internally inconsistent answer that is cached permanently.
    with pytest.raises(DetectionInvalid, match="uncertain"):
        validate_detection(
            _output(item_outcome="uncertain", mentions=[_mention(outcome="research")]),
            passages=PASSAGES,
            max_people=8,
        )


def test_identity_fact_value_grounding_is_case_insensitive():
    # Feed titles are title-cased and the model quotes them back in sentence
    # case. This rejected valid output in the prior programme.
    result = validate_detection(
        _output(
            mentions=[
                _mention(
                    identity_facts=[
                        {
                            "kind": "profession_or_role",
                            "value": "sculptor",
                            "supporting_passage_ids": ["p1"],
                        }
                    ]
                )
            ]
        ),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].identity_facts[0].value == "sculptor"


def test_identity_fact_may_be_grounded_in_any_supplied_passage():
    # The value was checked only against cited passages, so a value present in
    # the title but cited to the summary was rejected in 3 of 3 replays.
    result = validate_detection(
        _output(
            mentions=[
                _mention(
                    identity_facts=[
                        {
                            "kind": "place",
                            "value": "Paris",
                            "supporting_passage_ids": ["p1"],
                        }
                    ]
                )
            ]
        ),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].identity_facts[0].value == "Paris"


def test_ungrounded_identity_fact_value_is_rejected():
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(
                mentions=[
                    _mention(
                        identity_facts=[
                            {
                                "kind": "place",
                                "value": "Reykjavik",
                                "supporting_passage_ids": ["p1"],
                            }
                        ]
                    )
                ]
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_domain_profile_grounding_is_rejected_since_no_profile_is_supplied():
    # Belt and braces. The schema no longer offers this value, so a conforming
    # model cannot produce it; the validator rule stays because the schema is
    # the provider's promise and this is ours.
    with pytest.raises(DetectionInvalid, match="domain_profile"):
        validate_detection(
            _output(
                mentions=[
                    _mention(
                        signals=[
                            {
                                "kind": "attention",
                                "category": "major_achievement",
                                "claim": "Won a prize.",
                                "supporting_passage_ids": ["p1"],
                                "grounding": "domain_profile",
                            }
                        ]
                    )
                ]
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_research_people_requires_a_research_or_uncertain_mention():
    with pytest.raises(DetectionInvalid, match="item_outcome"):
        validate_detection(
            _output(
                item_outcome="research_people",
                mentions=[_mention(outcome="do_not_research")],
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(DetectionInvalid):
        validate_detection({"nonsense": True}, passages=PASSAGES, max_people=8)
