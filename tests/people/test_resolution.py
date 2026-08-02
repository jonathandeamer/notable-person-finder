from __future__ import annotations

import hashlib
import json
import traceback
import types
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from notable_person_finder.config.models import ResolvePersonEntityConfig
from notable_person_finder.people import (
    RESOLUTION_ADAPTER_VERSION,
    RESOLUTION_SCHEMA_VERSION,
    AttentionCategory,
    DetectionPassage,
    GroundedSignal,
    IdentityFact,
    IdentityFactKind,
    ResolutionValidationError,
    ResolveCandidate,
    ResolveCandidateFact,
    ResolveCandidateName,
    ResolvePersonEntityInput,
    ResolvePersonEntityOutput,
    SignalGrounding,
    SignalKind,
    build_resolve_input,
    first_pass_task_fingerprint,
    reconsider_task_fingerprint,
    render_resolution_request,
    resolution_prompt_and_schema_hashes,
    resolution_schema,
    validate_resolution_output,
)

FIXTURES = Path(__file__).with_name("fixtures")


def _config(**changes: object) -> ResolvePersonEntityConfig:
    values: dict[str, object] = {
        "max_input_tokens": 8192,
        "max_completion_tokens": 512,
        "max_candidates": 4,
        "max_facts_per_candidate": 8,
        "max_names_per_candidate": 4,
        "max_title_characters": 200,
        "max_summary_characters": 2000,
    }
    values.update(changes)
    return ResolvePersonEntityConfig.model_validate(values)


def _passage(
    *,
    passage_id: str = "p1",
    field: str = "title",
    text: str = "Élodie N'Diaye wins the Prix Exemple",
    truncated: bool = False,
) -> DetectionPassage:
    return DetectionPassage(
        id=passage_id,  # type: ignore[arg-type]
        field=field,  # type: ignore[arg-type]
        text=text,
        truncated=truncated,
    )


def _fact(
    local_id: str = "fact-1",
    *,
    kind: IdentityFactKind = IdentityFactKind.NAME,
    value: str = "Élodie N'Diaye",
    passages: tuple[str, ...] = ("p1",),
) -> IdentityFact:
    return IdentityFact(
        local_id=local_id,
        kind=kind,
        value=value,
        supporting_passage_ids=passages,
    )


def _signal() -> GroundedSignal:
    return GroundedSignal(
        kind=SignalKind.ATTENTION,
        category=AttentionCategory.SIGNIFICANT_RECOGNITION,
        claim="The supplied title reports a prize.",
        supporting_passage_ids=("p1",),
        grounding=SignalGrounding.SOURCE_TEXT,
    )


def _candidate(
    person_id: int = 11,
    *,
    display_name: str = "Élodie N'Diaye",
    extra_facts: tuple[ResolveCandidateFact, ...] = (),
) -> ResolveCandidate:
    facts = (
        ResolveCandidateFact(
            local_id=f"c{person_id}-f1",
            kind=IdentityFactKind.PROFESSION_OR_ROLE,
            value="sculptor",
        ),
        *extra_facts,
    )
    return ResolveCandidate(
        person_id=person_id,
        display_name=display_name,
        names=(
            ResolveCandidateName(
                exact_name=display_name,
                search_name=display_name,
                match_key=display_name.casefold(),
                kind="professional",
            ),
        ),
        identity_facts=facts,
    )


def _supplied(**changes: object) -> ResolvePersonEntityInput:
    base = build_resolve_input(
        person_mention_id=7,
        source_item_id=17,
        exact_name="Élodie N'Diaye",
        search_name="Elodie NDiaye",
        mention_outcome="research",
        passages=(
            _passage(),
            _passage(
                passage_id="p2",
                field="summary",
                text="The sculptor was honoured in Paris.",
            ),
        ),
        identity_facts=(
            _fact(),
            _fact(
                "fact-2",
                kind=IdentityFactKind.PROFESSION_OR_ROLE,
                value="sculptor",
                passages=("p2",),
            ),
        ),
        signals=(_signal(),),
        candidates=(_candidate(),),
        config=_config(),
    )
    if not changes:
        return base
    return base.model_copy(update=changes)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _capture_validation_error(
    raw: str, supplied: ResolvePersonEntityInput
) -> BaseException:
    try:
        validate_resolution_output(raw, supplied)
    except ResolutionValidationError as error:
        del raw, supplied
        return error
    raise AssertionError("expected resolution validation to fail")


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
    supplied: ResolvePersonEntityInput,
    *sentinels: str,
) -> None:
    retained = tuple(_retained_exception_values(error))
    assert all(value is not supplied for value in retained)
    retained_strings = tuple(value for value in retained if isinstance(value, str))
    for sentinel in sentinels:
        assert all(sentinel not in value for value in retained_strings)


def _prompt_lines(prompt: str) -> frozenset[str]:
    return frozenset(line for line in prompt.splitlines() if line)


def test_builds_strict_resolve_input_with_candidates() -> None:
    value = _supplied()

    assert value.task == "resolve_person_entity"
    assert value.person_mention_id == 7
    assert value.source_item_id == 17
    assert value.mention_outcome == "research"
    assert value.max_candidates == 4
    assert value.view.kind == "mention_candidates"
    assert value.view.candidate_count == 1
    assert value.candidates[0].person_id == 11
    assert value.candidates[0].identity_facts[0].local_id == "c11-f1"


def test_build_rejects_empty_candidates_and_over_cap() -> None:
    with pytest.raises(ValueError, match="at least one candidate"):
        build_resolve_input(
            person_mention_id=1,
            source_item_id=1,
            exact_name="A",
            search_name="A",
            mention_outcome="research",
            passages=(_passage(text="A"),),
            identity_facts=(),
            signals=(),
            candidates=(),
            config=_config(),
        )

    with pytest.raises(ValueError, match="max_candidates"):
        build_resolve_input(
            person_mention_id=1,
            source_item_id=1,
            exact_name="A",
            search_name="A",
            mention_outcome="research",
            passages=(_passage(text="A"),),
            identity_facts=(),
            signals=(),
            candidates=(_candidate(1), _candidate(2), _candidate(3)),
            config=_config(max_candidates=2),
        )


def test_valid_three_outcomes_are_accepted() -> None:
    supplied = _supplied()

    same = validate_resolution_output(
        _fixture("resolve_person_entity_valid_same.json"), supplied
    )
    different = validate_resolution_output(
        _fixture("resolve_person_entity_valid_different.json"), supplied
    )
    uncertain = validate_resolution_output(
        _fixture("resolve_person_entity_valid_uncertain.json"), supplied
    )

    assert same.outcome == "same_person"
    assert same.selected_person_id == 11
    assert different.outcome == "different_people"
    assert different.selected_person_id is None
    assert uncertain.outcome == "uncertain"
    assert uncertain.selected_person_id is None


def test_same_person_rejects_unseen_selected_id() -> None:
    with pytest.raises(ResolutionValidationError, match="not a supplied candidate"):
        validate_resolution_output(
            _fixture("resolve_person_entity_invalid_selected.json"), _supplied()
        )


def test_same_person_requires_selected_id() -> None:
    raw = json.dumps(
        {
            "outcome": "same_person",
            "selected_person_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Missing selection.",
        }
    )

    with pytest.raises(
        ResolutionValidationError, match="same_person requires selected_person_id"
    ):
        validate_resolution_output(raw, _supplied())


@pytest.mark.parametrize("outcome", ["different_people", "uncertain"])
def test_non_match_outcomes_require_null_selected_id(outcome: str) -> None:
    raw = json.dumps(
        {
            "outcome": outcome,
            "selected_person_id": 11,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Should not select.",
        }
    )

    with pytest.raises(
        ResolutionValidationError, match="selected_person_id to be null"
    ):
        validate_resolution_output(raw, _supplied())


def test_empty_rationale_is_rejected() -> None:
    raw = json.dumps(
        {
            "outcome": "uncertain",
            "selected_person_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "",
        }
    )

    with pytest.raises(ResolutionValidationError, match="output schema"):
        validate_resolution_output(raw, _supplied())


def test_unknown_output_fields_are_rejected() -> None:
    payload = json.loads(_fixture("resolve_person_entity_valid_same.json"))
    payload["confidence"] = 0.99

    with pytest.raises(ResolutionValidationError, match="output schema"):
        validate_resolution_output(json.dumps(payload), _supplied())


def test_unseen_fact_ids_are_rejected() -> None:
    raw = json.dumps(
        {
            "outcome": "same_person",
            "selected_person_id": 11,
            "supporting_fact_ids": ["unseen-fact"],
            "conflicting_fact_ids": [],
            "rationale": "Uses an unseen fact id.",
        }
    )

    with pytest.raises(
        ResolutionValidationError, match="unseen supporting fact id unseen-fact"
    ):
        validate_resolution_output(raw, _supplied())


def test_models_are_strict_and_frozen() -> None:
    value = _supplied()

    with pytest.raises(ValidationError):
        ResolvePersonEntityInput.model_validate(
            {**value.model_dump(), "wikipedia_page": "forbidden"}
        )
    with pytest.raises(ValidationError):
        value.__setattr__("exact_name", "changed")
    with pytest.raises(ValidationError):
        ResolvePersonEntityOutput.model_validate(
            {
                "outcome": "uncertain",
                "selected_person_id": None,
                "supporting_fact_ids": [],
                "conflicting_fact_ids": [],
                "rationale": "ok",
                "debug": True,
            }
        )


def test_rendering_hashes_reviewed_prompt_and_explicit_schema_version() -> None:
    value = _supplied()
    rendered = render_resolution_request(value)

    assert rendered.task == "resolve_person_entity"
    assert rendered.schema_version == RESOLUTION_SCHEMA_VERSION == 1
    assert (
        rendered.prompt_hash
        == hashlib.sha256(rendered.system_prompt.encode("utf-8")).hexdigest()
    )
    expected_schema_envelope = json.dumps(
        {
            "schema": resolution_schema(),
            "schema_version": RESOLUTION_SCHEMA_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert (
        rendered.schema_hash
        == hashlib.sha256(expected_schema_envelope.encode("utf-8")).hexdigest()
    )
    assert "wikipedia" not in rendered.user_input_json
    assert rendered.worst_case_input_tokens <= value.max_input_tokens


@pytest.mark.parametrize(
    "rule",
    [
        "Decide whether the mention describes one of the code-supplied candidates.",
        "`same_person` only when supplied evidence supports identity with exactly "
        "one candidate; set `selected_person_id` to that candidate's person id.",
        "`different_people` when the mention is a distinct person from every "
        "supplied candidate; leave `selected_person_id` null.",
        "`uncertain` when the evidence is insufficient to decide; leave "
        "`selected_person_id` null.",
        "Name equality alone never establishes identity.",
    ],
)
def test_prompt_pins_each_complete_outcome_rule(rule: str) -> None:
    assert rule in _prompt_lines(render_resolution_request(_supplied()).system_prompt)


def test_first_pass_fingerprint_is_stable_and_excludes_candidates() -> None:
    config = _config()
    facts = (_fact(),)
    signals = (_signal(),)
    first = first_pass_task_fingerprint(
        person_mention_id=7,
        mention_outcome="research",
        exact_name="Élodie N'Diaye",
        search_name="Elodie NDiaye",
        identity_facts=facts,
        signals=signals,
        config=config,
    )
    second = first_pass_task_fingerprint(
        person_mention_id=7,
        mention_outcome="research",
        exact_name="Élodie N'Diaye",
        search_name="Elodie NDiaye",
        identity_facts=facts,
        signals=signals,
        config=config,
    )
    assert first == second
    assert len(first) == 64

    # Positive control: a material field change must move the fingerprint.
    changed_name = first_pass_task_fingerprint(
        person_mention_id=7,
        mention_outcome="research",
        exact_name="Different Name",
        search_name="Elodie NDiaye",
        identity_facts=facts,
        signals=signals,
        config=config,
    )
    assert changed_name != first

    # Positive control: config candidate bound participates.
    changed_bound = first_pass_task_fingerprint(
        person_mention_id=7,
        mention_outcome="research",
        exact_name="Élodie N'Diaye",
        search_name="Elodie NDiaye",
        identity_facts=facts,
        signals=signals,
        config=_config(max_candidates=3),
    )
    assert changed_bound != first

    # K16: live candidate person ids must not participate. Prove by hashing the
    # material payload ourselves and confirming candidate ids are absent, and by
    # showing two builds with different candidates share the same fingerprint.
    with_alt_candidates = first_pass_task_fingerprint(
        person_mention_id=7,
        mention_outcome="research",
        exact_name="Élodie N'Diaye",
        search_name="Elodie NDiaye",
        identity_facts=facts,
        signals=signals,
        config=config,
    )
    assert with_alt_candidates == first

    prompt_hash, schema_hash, schema_version = resolution_prompt_and_schema_hashes()
    material = {
        "task": "resolve_person_entity",
        "adapter_version": RESOLUTION_ADAPTER_VERSION,
        "person_mention_id": 7,
        "mention_outcome": "research",
        "exact_name": "Élodie N'Diaye",
        "search_name": "Elodie NDiaye",
        "identity_facts": [
            {
                "local_id": "fact-1",
                "kind": "name",
                "value": "Élodie N'Diaye",
                "supporting_passage_ids": ["p1"],
            }
        ],
        "signals": [
            {
                "kind": "attention",
                "category": "significant_recognition",
                "claim": "The supplied title reports a prize.",
                "grounding": "source_text",
                "supporting_passage_ids": ["p1"],
            }
        ],
        "model": config.model,
        "parameters": {
            "temperature": config.parameters.temperature,
            "top_p": config.parameters.top_p,
            "reasoning_effort": config.parameters.reasoning_effort,
        },
        "max_input_tokens": config.max_input_tokens,
        "max_completion_tokens": config.max_completion_tokens,
        "max_candidates": config.max_candidates,
        "max_facts_per_candidate": config.max_facts_per_candidate,
        "max_names_per_candidate": config.max_names_per_candidate,
        "max_title_characters": config.max_title_characters,
        "max_summary_characters": config.max_summary_characters,
        "prompt_hash": prompt_hash,
        "schema_hash": schema_hash,
        "schema_version": schema_version,
    }
    expected = hashlib.sha256(
        json.dumps(
            material, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    assert first == expected
    serialized = json.dumps(material)
    assert "person_id" not in serialized
    assert "c11-f1" not in serialized
    assert '"candidates"' not in serialized


def test_reconsider_fingerprint_includes_both_identity_sides_and_relation() -> None:
    config = _config()
    left = "a" * 64
    right = "b" * 64
    first = reconsider_task_fingerprint(
        person_relation_id=3,
        subject_identity_fingerprint=left,
        peer_identity_fingerprint=right,
        config=config,
    )
    second = reconsider_task_fingerprint(
        person_relation_id=3,
        subject_identity_fingerprint=left,
        peer_identity_fingerprint=right,
        config=config,
    )
    assert first == second

    changed_relation = reconsider_task_fingerprint(
        person_relation_id=4,
        subject_identity_fingerprint=left,
        peer_identity_fingerprint=right,
        config=config,
    )
    changed_peer = reconsider_task_fingerprint(
        person_relation_id=3,
        subject_identity_fingerprint=left,
        peer_identity_fingerprint="c" * 64,
        config=config,
    )
    assert changed_relation != first
    assert changed_peer != first


def test_schema_errors_retain_no_raw_provider_or_source_content() -> None:
    raw_sentinel = "RAW9z"
    provider_sentinel = "PROV8y"
    source_sentinel = "SRC7x"
    raw = json.dumps(
        {
            "outcome": "uncertain",
            "selected_person_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Safe rationale.",
            "unknown_provider_debug": {
                "raw": raw_sentinel,
                "provider": provider_sentinel,
                "source": source_sentinel,
            },
        }
    )
    supplied = _supplied()

    with pytest.raises(ResolutionValidationError) as caught:
        validate_resolution_output(raw, supplied)

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


def test_domain_failure_retains_no_sensitive_values() -> None:
    provider_sentinel = "DOMAIN_PROVIDER_SENTINEL_3t"
    source_sentinel = "DOMAIN_SOURCE_SENTINEL_2s"
    supplied = build_resolve_input(
        person_mention_id=7,
        source_item_id=17,
        exact_name=source_sentinel,
        search_name=source_sentinel,
        mention_outcome="research",
        passages=(_passage(text=source_sentinel),),
        identity_facts=(_fact(value=source_sentinel),),
        signals=(),
        candidates=(_candidate(display_name=source_sentinel),),
        config=_config(),
    )
    raw = json.dumps(
        {
            "outcome": "same_person",
            "selected_person_id": 999,
            "supporting_fact_ids": [provider_sentinel],
            "conflicting_fact_ids": [],
            "rationale": "Safe rationale.",
        }
    )

    error = _capture_validation_error(raw, supplied)

    _assert_error_retains_no_sensitive_values(
        error, supplied, provider_sentinel, source_sentinel, raw
    )


def test_resolution_schema_requires_every_property_for_strict_structured_output() -> (
    None
):
    """Strict structured output rejects a `required` that omits any property.

    `selected_person_id` is nullable with a pydantic default, so pydantic left
    it out of `required` and OpenRouter answered HTTP 400 ("'required' is
    required to be supplied and to be an array including every key in
    properties"). Optionality must ride on the nullable type instead.
    """
    schema = resolution_schema()

    def objects(node: object) -> list[dict[str, object]]:
        found: list[dict[str, object]] = []
        if isinstance(node, dict):
            if node.get("type") == "object" and isinstance(
                node.get("properties"), dict
            ):
                found.append(node)
            for child in node.values():
                found.extend(objects(child))
        elif isinstance(node, list):
            for child in node:
                found.extend(objects(child))
        return found

    checked = objects(schema)
    assert checked, "expected at least one object schema to check"
    for node in checked:
        properties = node["properties"]
        required = node.get("required", [])
        assert isinstance(properties, dict)
        assert isinstance(required, list)
        assert set(required) == set(properties)
    root_required = schema["required"]
    assert isinstance(root_required, list)
    assert "selected_person_id" in root_required
