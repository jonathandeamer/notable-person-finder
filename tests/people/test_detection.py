from __future__ import annotations

import hashlib
import json
import traceback
import types
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from notable_person_finder.config.models import (
    DetectPeopleConfig,
    DomainProfileConfig,
    FeedConfig,
)
from notable_person_finder.people import (
    DETECTION_SCHEMA_VERSION,
    DetectionInput,
    DetectionValidationError,
    DomainProfileEvidence,
    build_detection_input,
    detection_schema,
    render_detection_request,
    validate_detection_output,
)

FIXTURES = Path(__file__).with_name("fixtures")


def _source_item(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "id": 17,
        "feed_identity_id": 4,
        "canonical_article_id": 29,
        "original_url": "https://example.com/people/elodie",
        "title_text": "Élodie N'Diaye wins the Prix Exemple",
        "summary_text": "The sculptor was honoured in Paris.",
        "published_at": "2026-07-28T10:15:00Z",
        "published_issue": None,
        "url_issue": None,
    }
    value.update(changes)
    return value


def _feed() -> FeedConfig:
    return FeedConfig(
        key="arts-news",
        label="Arts News",
        url="https://example.com/feed.xml",
    )


def _profile() -> DomainProfileConfig:
    return DomainProfileConfig(
        schema_version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples={
            "significant_recognition": ("major art prize",),
            "institutional_recognition": ("permanent museum collection",),
        },
    )


def _config(**changes: object) -> DetectPeopleConfig:
    values: dict[str, object] = {
        "max_input_tokens": 4096,
        "max_completion_tokens": 512,
        "max_people": 3,
        "max_title_characters": 200,
        "max_summary_characters": 2000,
    }
    values.update(changes)
    return DetectPeopleConfig.model_validate(values)


def _supplied(*, max_people: int = 3) -> DetectionInput:
    return build_detection_input(
        _source_item(), _feed(), _profile(), _config(max_people=max_people)
    )


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _capture_validation_error(raw: str, supplied: DetectionInput) -> BaseException:
    try:
        validate_detection_output(raw, supplied)
    except DetectionValidationError as error:
        del raw, supplied
        return error
    raise AssertionError("expected detection validation to fail")


def _retained_exception_values(error: BaseException) -> Iterator[object]:
    pending: list[object] = [error]
    seen: set[int] = set()
    while pending:
        value = pending.pop()
        identity = id(value)
        if identity in seen:
            continue
        seen.add(identity)
        yield value
        if isinstance(value, BaseException):
            pending.extend(value.args)
            pending.extend(value.__dict__.values())
            if value.__cause__ is not None:
                pending.append(value.__cause__)
            if value.__context__ is not None:
                pending.append(value.__context__)
            if value.__traceback__ is not None:
                pending.append(value.__traceback__)
        elif isinstance(value, types.TracebackType):
            pending.append(value.tb_frame)
            if value.tb_next is not None:
                pending.append(value.tb_next)
        elif isinstance(value, types.FrameType):
            pending.extend(value.f_locals.values())
        elif isinstance(value, BaseModel):
            pending.extend(value.__dict__.values())
        elif isinstance(value, dict):
            pending.extend(value.keys())
            pending.extend(value.values())
        elif isinstance(value, (list, tuple, set, frozenset)):
            pending.extend(value)


def _assert_error_retains_no_sensitive_values(
    error: BaseException,
    supplied: DetectionInput,
    *sentinels: str,
) -> None:
    retained = tuple(_retained_exception_values(error))
    assert all(value is not supplied for value in retained)
    retained_strings = tuple(value for value in retained if isinstance(value, str))
    for sentinel in sentinels:
        assert all(sentinel not in value for value in retained_strings)


def test_builds_nullable_feed_metadata_and_stable_numbered_passages() -> None:
    value = build_detection_input(
        _source_item(
            title_text=None,
            summary_text="  Banksy opened a supplied exhibition.  ",
            canonical_article_id=None,
            original_url=None,
            published_at=None,
            published_issue="missing",
            url_issue="missing",
        ),
        _feed(),
        _profile(),
        _config(),
    )

    assert value.source_item_id == 17
    assert value.feed_id == 4
    assert value.feed_key == "arts-news"
    assert value.publisher_label == "Arts News"
    assert value.title is None
    assert value.summary == "Banksy opened a supplied exhibition."
    actual_passages = [
        (passage.id, passage.field, passage.text) for passage in value.passages
    ]
    assert actual_passages == [
        ("p2", "summary", "Banksy opened a supplied exhibition.")
    ]
    assert value.canonical_article_id is None
    assert value.original_url is None
    assert value.published_at is None
    assert value.published_issue == "missing"
    assert value.url_issue == "missing"
    assert value.view.kind == "feed_metadata"
    assert value.view.title_available is False
    assert value.view.summary_available is True


def test_character_truncation_is_deterministic_and_marks_each_passage() -> None:
    source = _source_item(
        title_text="ABCDEéé",
        summary_text="12345🙂🙂",
    )
    config = _config(
        max_title_characters=5,
        max_summary_characters=6,
        max_input_tokens=4096,
    )

    first = build_detection_input(source, _feed(), _profile(), config)
    second = build_detection_input(source, _feed(), _profile(), config)

    assert first == second
    assert first.title == "ABCDE"
    assert first.summary == "12345🙂"
    assert [(p.id, p.truncated) for p in first.passages] == [
        ("p1", True),
        ("p2", True),
    ]
    assert first.view.input_truncated is True
    assert first.view.title_truncated is True
    assert first.view.summary_truncated is True


def test_token_envelope_truncation_marks_the_summary_view_and_passage() -> None:
    value = build_detection_input(
        _source_item(summary_text="x" * 5000),
        _feed(),
        _profile(),
        _config(
            max_input_tokens=4096,
            max_completion_tokens=128,
            max_summary_characters=6000,
        ),
    )

    summary_passage = next(passage for passage in value.passages if passage.id == "p2")
    assert len(value.summary or "") < 5000
    assert value.view.input_truncated is True
    assert value.view.summary_truncated is True
    assert summary_passage.truncated is True


def test_canonical_utf8_rendering_respects_worst_case_token_ceiling() -> None:
    value = build_detection_input(
        _source_item(
            title_text="Élodie " + ("🎨" * 400),
            summary_text='Quoted "text" and a newline\n' + ("λ" * 4000),
        ),
        _feed(),
        _profile(),
        _config(
            max_input_tokens=6000,
            max_completion_tokens=128,
            max_title_characters=500,
            max_summary_characters=4000,
        ),
    )
    rendered = render_detection_request(value)

    parsed = json.loads(rendered.user_input_json)
    assert rendered.user_input_json == json.dumps(
        parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    assert "Élodie" in rendered.user_input_json
    assert "\\u00c9" not in rendered.user_input_json
    exact_bytes = sum(
        len(part.encode("utf-8"))
        for part in (
            rendered.system_prompt,
            rendered.user_input_json,
            rendered.canonical_schema_json,
        )
    )
    assert rendered.token_bearing_utf8_bytes == exact_bytes
    assert rendered.chat_framing_token_allowance > 0
    assert rendered.worst_case_input_tokens == (
        exact_bytes + rendered.chat_framing_token_allowance
    )
    assert rendered.worst_case_input_tokens <= value.max_input_tokens
    assert value.view.input_truncated is True
    assert len(value.title or "") > 0
    assert len(value.summary or "") < 4000


def test_dense_ascii_punctuation_cannot_exceed_the_input_token_ceiling() -> None:
    source_summary = "!,.[]{}:;" * 1000
    value = build_detection_input(
        _source_item(title_text="Dense", summary_text=source_summary),
        _feed(),
        _profile(),
        _config(
            max_input_tokens=4096,
            max_completion_tokens=128,
            max_summary_characters=10_000,
        ),
    )
    rendered = render_detection_request(value)

    assert len(value.summary or "") < len(source_summary)
    assert value.view.summary_truncated is True
    assert rendered.token_bearing_utf8_bytes == sum(
        len(part.encode("utf-8"))
        for part in (
            rendered.system_prompt,
            rendered.user_input_json,
            rendered.canonical_schema_json,
        )
    )
    assert rendered.worst_case_input_tokens <= 4096


def test_fixed_prompt_schema_and_framing_must_fit_before_text() -> None:
    with pytest.raises(ValueError, match="fixed prompt, schema, and chat framing"):
        build_detection_input(
            _source_item(title_text=None, summary_text=None),
            _feed(),
            _profile(),
            _config(max_input_tokens=1000, max_completion_tokens=128),
        )


def test_rendering_accepts_the_exact_worst_case_boundary_and_rejects_one_less() -> None:
    roomy = build_detection_input(
        _source_item(title_text="Boundary", summary_text=None),
        _feed(),
        _profile(),
        _config(max_input_tokens=20_000, max_completion_tokens=128),
    )
    first = render_detection_request(roomy)
    boundary_input = roomy.model_copy(
        update={"max_input_tokens": first.worst_case_input_tokens}
    )
    at_boundary = render_detection_request(boundary_input)

    assert at_boundary.worst_case_input_tokens <= boundary_input.max_input_tokens
    too_small = boundary_input.model_copy(
        update={"max_input_tokens": at_boundary.worst_case_input_tokens - 1}
    )
    with pytest.raises(ValueError, match="worst-case input token ceiling"):
        render_detection_request(too_small)


def test_rendering_hashes_reviewed_prompt_and_explicit_schema_version(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.chdir(tmp_path)
    value = _supplied()
    rendered = render_detection_request(value)

    assert rendered.task == "detect_people"
    assert rendered.schema_version == DETECTION_SCHEMA_VERSION == 1
    assert (
        rendered.prompt_hash
        == hashlib.sha256(rendered.system_prompt.encode("utf-8")).hexdigest()
    )
    expected_schema_envelope = json.dumps(
        {
            "schema": detection_schema(),
            "schema_version": DETECTION_SCHEMA_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert (
        rendered.schema_hash
        == hashlib.sha256(expected_schema_envelope.encode("utf-8")).hexdigest()
    )
    assert "Use only the supplied passages" in rendered.system_prompt
    assert "article body" not in rendered.user_input_json
    assert "title_raw" not in rendered.user_input_json
    assert "summary_raw" not in rendered.user_input_json


def test_prompt_pins_item_and_mention_outcome_mapping_within_boundary() -> None:
    rendered = render_detection_request(_supplied())
    prompt = rendered.system_prompt

    assert (
        "Use `research_people` only when at least one returned mention is "
        "`research` or `uncertain`" in prompt
    )
    assert "use `do_not_research` only when none is actionable" in prompt
    assert (
        "use item-level `uncertain` when the item decision remains uncertain" in prompt
    )
    assert rendered.worst_case_input_tokens <= _supplied().max_input_tokens


def test_prompt_pins_precise_source_and_profile_grounding_rules() -> None:
    prompt = render_detection_request(_supplied()).system_prompt

    assert "`source_text`" in prompt
    assert "literal and passage-grounded" in prompt
    assert "names, facts, and signals" in prompt
    assert "`domain_profile`" in prompt
    assert "active supplied category, example, and version" in prompt
    assert "must not invent external facts" in prompt


def test_raw_source_fields_have_positive_controls_and_never_enter_the_request() -> None:
    sentinel = "RAW_SOURCE_FIELD_SENTINEL_6w"
    source = _source_item(
        article_body=sentinel,
        title_raw=sentinel,
        summary_raw=sentinel,
    )
    value = build_detection_input(source, _feed(), _profile(), _config())
    rendered = render_detection_request(value)

    assert source["article_body"] == sentinel
    assert source["title_raw"] == sentinel
    assert source["summary_raw"] == sentinel
    assert sentinel not in value.model_dump_json()
    assert sentinel not in rendered.user_input_json
    for forbidden in ("article_body", "title_raw", "summary_raw"):
        assert forbidden not in type(value).model_fields
        assert forbidden not in rendered.user_input_json


def test_domain_profile_version_and_examples_participate_exactly() -> None:
    future_profile = _profile().model_copy(update={"schema_version": 7})
    value = build_detection_input(_source_item(), _feed(), future_profile, _config())
    rendered = render_detection_request(value)
    payload = json.loads(rendered.user_input_json)

    assert value.domain_profile.version == 7
    assert payload["domain_profile"] == {
        "attention_examples": [
            {
                "category": "institutional_recognition",
                "examples": ["permanent museum collection"],
            },
            {
                "category": "significant_recognition",
                "examples": ["major art prize"],
            },
        ],
        "key": "visual-arts-en",
        "label": "English visual arts",
        "language": "en",
        "version": 7,
    }
    changed_profile = DomainProfileEvidence(
        version=7,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples=(),
    )
    changed = value.model_copy(update={"domain_profile": changed_profile})
    assert render_detection_request(changed).user_input_json != rendered.user_input_json


def test_models_are_strict_and_frozen() -> None:
    value = _supplied()

    with pytest.raises(ValidationError):
        DetectionInput.model_validate(
            {**value.model_dump(), "article_body": "forbidden"}
        )
    with pytest.raises(ValidationError):
        value.__setattr__("title", "changed")
    with pytest.raises(ValidationError):
        DomainProfileEvidence.model_validate(
            {
                **value.domain_profile.model_dump(),
                "publisher_policy": "outside this task",
            }
        )


# Output-domain tests are below the construction/rendering contract so the TDD
# transcript can demonstrate a separate RED for the validator family.


def test_valid_multi_person_fixture_preserves_mononym_and_uncertainty() -> None:
    output = validate_detection_output(
        _fixture("detect_people_valid.json"), _supplied()
    )

    assert output.item_outcome == "research_people"
    assert [mention.exact_name for mention in output.mentions] == [
        "Élodie N'Diaye",
        "sculptor",
    ]
    assert output.mentions[0].outcome == "research"
    assert output.mentions[1].outcome == "uncertain"
    assert output.overflow is False


@pytest.mark.parametrize(
    ("item_outcome", "mentions"),
    [
        ("do_not_research", []),
        ("uncertain", []),
        (
            "uncertain",
            [
                {
                    "exact_name": "Élodie N'Diaye",
                    "outcome": "uncertain",
                    "supporting_passage_ids": ["p1"],
                    "identity_facts": [],
                    "signals": [],
                    "rationale": "The supplied item leaves subject focus uncertain.",
                }
            ],
        ),
    ],
)
def test_valid_zero_and_uncertain_results_succeed_without_repair(
    item_outcome: str, mentions: list[dict[str, object]]
) -> None:
    raw = json.dumps(
        {
            "item_outcome": item_outcome,
            "mentions": mentions,
            "overflow": False,
            "rationale": "A concise supplied-evidence assessment.",
        }
    )

    assert validate_detection_output(raw, _supplied()).item_outcome == item_outcome


def test_unknown_output_fields_are_rejected() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["confidence"] = 0.99

    with pytest.raises(DetectionValidationError, match="output schema"):
        validate_detection_output(json.dumps(payload), _supplied())


def test_schema_errors_retain_no_raw_provider_or_source_content() -> None:
    raw_sentinel = "RAW9z"
    provider_sentinel = "PROV8y"
    source_sentinel = "SRC7x"
    raw = json.dumps(
        {
            "item_outcome": "do_not_research",
            "mentions": [],
            "overflow": False,
            "rationale": "Safe rationale.",
            "unknown_provider_debug": {
                "raw": raw_sentinel,
                "provider": provider_sentinel,
                "source": source_sentinel,
            },
        }
    )

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(raw, _supplied())

    error = caught.value
    retained_exceptions = (error, error.__cause__, error.__context__)
    rendered_traceback = "".join(
        line
        for retained in retained_exceptions
        if retained is not None
        for line in traceback.format_exception(retained)
    )
    for sentinel in (raw_sentinel, provider_sentinel, source_sentinel, raw):
        assert sentinel not in str(error)
        assert sentinel not in rendered_traceback
    assert error.__cause__ is None
    assert error.__context__ is None


def test_schema_failure_traceback_locals_retain_no_provider_or_source_data() -> None:
    provider_sentinel = "SCHEMA_PROVIDER_SENTINEL_5v"
    source_sentinel = "SCHEMA_SOURCE_SENTINEL_4u"
    supplied = build_detection_input(
        _source_item(title_text=source_sentinel),
        _feed(),
        _profile(),
        _config(),
    )
    raw = json.dumps(
        {
            "item_outcome": "do_not_research",
            "mentions": [],
            "overflow": False,
            "rationale": "Safe rationale.",
            "unknown": provider_sentinel,
        }
    )

    error = _capture_validation_error(raw, supplied)

    _assert_error_retains_no_sensitive_values(
        error, supplied, provider_sentinel, source_sentinel, raw
    )


def test_domain_failure_traceback_locals_retain_no_provider_or_source_data() -> None:
    provider_sentinel = "DOMAIN_PROVIDER_SENTINEL_3t"
    source_sentinel = "DOMAIN_SOURCE_SENTINEL_2s"
    supplied = build_detection_input(
        _source_item(title_text=source_sentinel),
        _feed(),
        _profile(),
        _config(),
    )
    raw = json.dumps(
        {
            "item_outcome": "research_people",
            "mentions": [
                {
                    "exact_name": provider_sentinel,
                    "outcome": "research",
                    "supporting_passage_ids": ["p1"],
                    "identity_facts": [],
                    "signals": [],
                    "rationale": "Safe rationale.",
                }
            ],
            "overflow": False,
            "rationale": "Safe rationale.",
        }
    )

    error = _capture_validation_error(raw, supplied)

    _assert_error_retains_no_sensitive_values(
        error, supplied, provider_sentinel, source_sentinel, raw
    )


def test_unseen_passage_ids_are_rejected_with_safe_identifiers() -> None:
    raw = _fixture("detect_people_invalid_references.json")

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(raw, _supplied())

    message = str(caught.value)
    assert "mention[1]" in message
    assert "p99" in message
    assert "Élodie" not in message
    assert raw not in message


def test_names_must_be_exactly_grounded_in_their_referenced_passages() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"][0]["exact_name"] = "Elodie Ndiaye"

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(json.dumps(payload), _supplied())

    assert str(caught.value) == "mention[1]: exact_name is not grounded"
    assert "Elodie Ndiaye" not in str(caught.value)


def test_name_grounding_requires_a_complete_source_written_token() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"][0]["exact_name"] = "lodie"

    with pytest.raises(DetectionValidationError, match="exact_name is not grounded"):
        validate_detection_output(json.dumps(payload), _supplied())


def test_duplicate_local_fact_ids_are_rejected() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    duplicate = dict(payload["mentions"][0]["identity_facts"][0])
    payload["mentions"][0]["identity_facts"].append(duplicate)

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(json.dumps(payload), _supplied())

    assert str(caught.value) == "mention[1]: duplicate identity fact id fact-1"


@pytest.mark.parametrize(
    ("target", "replacement", "safe_id"),
    [
        ("fact", "p404", "fact-1"),
        ("signal", "p405", "signal[1]"),
    ],
)
def test_fact_and_signal_passage_references_must_be_supplied(
    target: str, replacement: str, safe_id: str
) -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    field = "identity_facts" if target == "fact" else "signals"
    payload["mentions"][0][field][0]["supporting_passage_ids"] = [replacement]

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(json.dumps(payload), _supplied())

    message = str(caught.value)
    assert safe_id in message
    assert replacement in message
    assert "Prix Exemple" not in message


def test_identity_fact_values_must_be_literal_supplied_text() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"][0]["identity_facts"][0]["value"] = "world-famous artist"

    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(json.dumps(payload), _supplied())

    assert str(caught.value) == "mention[1] fact-1: value is not grounded"


@pytest.mark.parametrize(
    ("item_outcome", "mention_outcome"),
    [
        ("research_people", "do_not_research"),
        ("do_not_research", "research"),
        ("do_not_research", "uncertain"),
        ("uncertain", "research"),
    ],
)
def test_contradictory_item_and_mention_outcomes_are_rejected(
    item_outcome: str, mention_outcome: str
) -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["item_outcome"] = item_outcome
    payload["mentions"] = [payload["mentions"][0]]
    payload["mentions"][0]["outcome"] = mention_outcome

    with pytest.raises(DetectionValidationError, match="item_outcome"):
        validate_detection_output(json.dumps(payload), _supplied())


def test_overflow_requires_the_returned_list_to_reach_the_cap() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"] = [payload["mentions"][0]]
    payload["overflow"] = True

    with pytest.raises(DetectionValidationError, match="overflow"):
        validate_detection_output(json.dumps(payload), _supplied(max_people=3))


def test_mention_count_cannot_exceed_the_supplied_cap() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"] = [payload["mentions"][0], payload["mentions"][0]]

    with pytest.raises(DetectionValidationError, match="mention cap 1"):
        validate_detection_output(json.dumps(payload), _supplied(max_people=1))


def test_signal_kind_and_category_vocabulary_are_coupled() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"][0]["signals"][0].update(
        {
            "kind": "caution",
            "category": "significant_recognition",
            "grounding": "source_text",
        }
    )

    with pytest.raises(DetectionValidationError, match=r"mention\[1\] signal\[1\]"):
        validate_detection_output(json.dumps(payload), _supplied())


def test_domain_profile_grounding_requires_a_supplied_profile_category() -> None:
    payload = json.loads(_fixture("detect_people_valid.json"))
    payload["mentions"][0]["signals"][0]["category"] = "major_achievement"

    with pytest.raises(DetectionValidationError, match="domain_profile category"):
        validate_detection_output(json.dumps(payload), _supplied())


@pytest.mark.parametrize("raw", ["not json", "{} trailing", "[1, 2, 3]"])
def test_malformed_errors_never_echo_raw_model_output(raw: str) -> None:
    with pytest.raises(DetectionValidationError) as caught:
        validate_detection_output(raw, _supplied())

    assert raw not in str(caught.value)
