from __future__ import annotations

import hashlib
import json
import traceback
import types
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from notable_person_finder.config.models import MatchWikipediaIdentityConfig
from notable_person_finder.people.models import IdentityFactKind
from notable_person_finder.wikipedia.matching import (
    MATCH_SCHEMA_VERSION,
    WIKIPEDIA_ADAPTER_VERSION,
    MatchValidationError,
    base_material_fingerprint,
    base_material_payload,
    build_match_input,
    map_model_outcome_to_semantic,
    match_prompt_and_schema_hashes,
    match_schema,
    observation_matches_live_material,
    render_match_request,
    validate_match_output,
)
from notable_person_finder.wikipedia.models import (
    MatchFact,
    MatchName,
    MatchWikiCandidate,
    MatchWikipediaIdentityInput,
    MatchWikipediaIdentityOutput,
    WikipediaPersonMaterialView,
)
from notable_person_finder.wikipedia.queries import QUERY_PLAN_VERSION

FIXTURES = Path(__file__).with_name("fixtures")


def _config(**changes: object) -> MatchWikipediaIdentityConfig:
    values: dict[str, object] = {
        "max_input_tokens": 8192,
        "max_completion_tokens": 512,
        "max_candidates": 4,
        "max_names_in_prompt": 4,
        "max_facts_in_prompt": 8,
        "max_extract_characters": 400,
        "max_categories_per_page": 8,
        "max_title_characters": 200,
        "max_summary_characters": 2000,
    }
    values.update(changes)
    return MatchWikipediaIdentityConfig.model_validate(values)


def _name(
    exact: str = "Élodie N'Diaye",
    *,
    kind: str = "professional",
) -> MatchName:
    return MatchName(
        exact_name=exact,
        search_name=exact,
        match_key=exact.casefold(),
        kind=kind,  # type: ignore[arg-type]
    )


def _fact(
    local_id: str = "f1",
    *,
    kind: IdentityFactKind = IdentityFactKind.NAME,
    value: str = "Élodie N'Diaye",
) -> MatchFact:
    return MatchFact(local_id=local_id, kind=kind, value=value)


def _candidate(
    page_id: int = 101,
    *,
    title: str = "Élodie N'Diaye",
    extract: str | None = "French sculptor active in Paris.",
) -> MatchWikiCandidate:
    return MatchWikiCandidate(
        page_id=page_id,
        title=title,
        canonical_url=f"https://en.wikipedia.org/wiki/{title.replace(' ', '_')}",
        namespace=0,
        is_disambiguation=False,
        description="French sculptor",
        extract=extract,
        categories=("French sculptors", "Living people"),
        redirect_trail=(),
    )


def _supplied(**changes: object) -> MatchWikipediaIdentityInput:
    base = build_match_input(
        person_id=7,
        display_name="Élodie N'Diaye",
        sourced_names=(_name(),),
        identity_facts=(
            _fact(),
            _fact(
                "f2",
                kind=IdentityFactKind.PROFESSION_OR_ROLE,
                value="sculptor",
            ),
        ),
        candidates=(_candidate(),),
        config=_config(),
        truncated_unsafe_for_negative=False,
        partial_retrieval=False,
    )
    if not changes:
        return base
    return base.model_copy(update=changes)


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _capture_validation_error(
    raw: str,
    supplied: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool = False,
) -> BaseException:
    try:
        validate_match_output(
            raw,
            supplied,
            truncated_unsafe_for_negative=truncated_unsafe_for_negative,
        )
    except MatchValidationError as error:
        del raw, supplied
        return error
    raise AssertionError("expected match validation to fail")


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
    supplied: MatchWikipediaIdentityInput,
    *sentinels: str,
) -> None:
    retained = tuple(_retained_exception_values(error))
    assert all(value is not supplied for value in retained)
    retained_strings = tuple(value for value in retained if isinstance(value, str))
    for sentinel in sentinels:
        assert all(sentinel not in value for value in retained_strings)


def _prompt_lines(prompt: str) -> frozenset[str]:
    return frozenset(line for line in prompt.splitlines() if line)


def _person(
    person_id: int = 7, identity_fingerprint: str | None = None
) -> WikipediaPersonMaterialView:
    return WikipediaPersonMaterialView(
        person_id=person_id,
        identity_fingerprint=identity_fingerprint or ("a" * 64),
    )


def test_builds_strict_match_input_with_candidates() -> None:
    value = _supplied()

    assert value.task == "match_wikipedia_identity"
    assert value.person_id == 7
    assert value.display_name == "Élodie N'Diaye"
    assert value.max_candidates == 4
    assert value.view.kind == "wikipedia_candidates"
    assert value.view.candidate_count == 1
    assert value.view.truncated_unsafe_for_negative is False
    assert value.candidates[0].page_id == 101
    assert value.identity_facts[0].local_id == "f1"


def test_build_rejects_empty_candidates_and_over_cap() -> None:
    with pytest.raises(ValueError, match="at least one candidate"):
        build_match_input(
            person_id=1,
            display_name="A",
            sourced_names=(_name("A"),),
            identity_facts=(),
            candidates=(),
            config=_config(),
            truncated_unsafe_for_negative=False,
            partial_retrieval=False,
        )

    with pytest.raises(ValueError, match="max_candidates"):
        build_match_input(
            person_id=1,
            display_name="A",
            sourced_names=(_name("A"),),
            identity_facts=(),
            candidates=(_candidate(1), _candidate(2), _candidate(3)),
            config=_config(max_candidates=2),
            truncated_unsafe_for_negative=False,
            partial_retrieval=False,
        )


def test_valid_three_outcomes_are_accepted() -> None:
    supplied = _supplied()

    matching = validate_match_output(
        _fixture("match_wikipedia_identity_valid_matching.json"),
        supplied,
        truncated_unsafe_for_negative=False,
    )
    no_match = validate_match_output(
        _fixture("match_wikipedia_identity_valid_no_match.json"),
        supplied,
        truncated_unsafe_for_negative=False,
    )
    uncertain = validate_match_output(
        _fixture("match_wikipedia_identity_valid_uncertain.json"),
        supplied,
        truncated_unsafe_for_negative=False,
    )

    assert matching.outcome == "matching_page"
    assert matching.selected_page_id == 101
    assert no_match.outcome == "no_matching_page"
    assert no_match.selected_page_id is None
    assert uncertain.outcome == "uncertain"
    assert uncertain.selected_page_id is None


def test_matching_page_rejects_unseen_selected_id() -> None:
    with pytest.raises(MatchValidationError, match="not a supplied candidate"):
        validate_match_output(
            _fixture("match_wikipedia_identity_invalid_selected.json"),
            _supplied(),
            truncated_unsafe_for_negative=False,
        )


def test_matching_page_requires_selected_id() -> None:
    raw = json.dumps(
        {
            "outcome": "matching_page",
            "selected_page_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Missing selection.",
        }
    )

    with pytest.raises(
        MatchValidationError, match="matching_page requires selected_page_id"
    ):
        validate_match_output(raw, _supplied(), truncated_unsafe_for_negative=False)


@pytest.mark.parametrize("outcome", ["no_matching_page", "uncertain"])
def test_non_match_outcomes_require_null_selected_id(outcome: str) -> None:
    raw = json.dumps(
        {
            "outcome": outcome,
            "selected_page_id": 101,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Should not select.",
        }
    )

    with pytest.raises(MatchValidationError, match="selected_page_id to be null"):
        validate_match_output(raw, _supplied(), truncated_unsafe_for_negative=False)


def test_empty_rationale_is_rejected() -> None:
    raw = json.dumps(
        {
            "outcome": "uncertain",
            "selected_page_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "",
        }
    )

    with pytest.raises(MatchValidationError, match="output schema"):
        validate_match_output(raw, _supplied(), truncated_unsafe_for_negative=False)


def test_unknown_output_fields_are_rejected() -> None:
    payload = json.loads(_fixture("match_wikipedia_identity_valid_matching.json"))
    payload["confidence"] = 0.99

    with pytest.raises(MatchValidationError, match="output schema"):
        validate_match_output(
            json.dumps(payload),
            _supplied(),
            truncated_unsafe_for_negative=False,
        )


def test_unseen_fact_ids_are_rejected() -> None:
    raw = json.dumps(
        {
            "outcome": "matching_page",
            "selected_page_id": 101,
            "supporting_fact_ids": ["unseen-fact"],
            "conflicting_fact_ids": [],
            "rationale": "Uses an unseen fact id.",
        }
    )

    with pytest.raises(
        MatchValidationError, match="unseen supporting fact id unseen-fact"
    ):
        validate_match_output(raw, _supplied(), truncated_unsafe_for_negative=False)


def test_truncated_unsafe_rejects_no_matching_page() -> None:
    """K5: truncated_unsafe_for_negative makes model no_matching_page invalid."""
    with pytest.raises(
        MatchValidationError,
        match="no_matching_page is invalid when truncated_unsafe_for_negative",
    ):
        validate_match_output(
            _fixture("match_wikipedia_identity_valid_no_match.json"),
            _supplied(),
            truncated_unsafe_for_negative=True,
        )


def test_truncated_unsafe_still_accepts_matching_and_uncertain() -> None:
    supplied = _supplied()
    matching = validate_match_output(
        _fixture("match_wikipedia_identity_valid_matching.json"),
        supplied,
        truncated_unsafe_for_negative=True,
    )
    uncertain = validate_match_output(
        _fixture("match_wikipedia_identity_valid_uncertain.json"),
        supplied,
        truncated_unsafe_for_negative=True,
    )
    assert matching.outcome == "matching_page"
    assert uncertain.outcome == "uncertain"


@pytest.mark.parametrize(
    ("model_outcome", "semantic"),
    [
        ("matching_page", "matching_page_found"),
        ("no_matching_page", "no_matching_page_found"),
        ("uncertain", "uncertain_identity"),
    ],
)
def test_k6_maps_model_outcomes_to_product_semantic(
    model_outcome: str, semantic: str
) -> None:
    assert map_model_outcome_to_semantic(model_outcome) == semantic


def test_k6_mapping_rejects_unknown_model_outcome() -> None:
    with pytest.raises(ValueError, match="unknown model outcome"):
        map_model_outcome_to_semantic("matching_page_found")


def test_models_are_strict_and_frozen() -> None:
    value = _supplied()

    with pytest.raises(ValidationError):
        MatchWikipediaIdentityInput.model_validate(
            {**value.model_dump(), "outside_knowledge": True}
        )
    with pytest.raises(ValidationError):
        value.__setattr__("display_name", "changed")
    with pytest.raises(ValidationError):
        MatchWikipediaIdentityOutput.model_validate(
            {
                "outcome": "uncertain",
                "selected_page_id": None,
                "supporting_fact_ids": [],
                "conflicting_fact_ids": [],
                "rationale": "ok",
                "debug": True,
            }
        )


def test_rendering_hashes_reviewed_prompt_and_explicit_schema_version() -> None:
    value = _supplied()
    rendered = render_match_request(value)

    assert rendered.task == "match_wikipedia_identity"
    assert rendered.schema_version == MATCH_SCHEMA_VERSION == 1
    assert (
        rendered.prompt_hash
        == hashlib.sha256(rendered.system_prompt.encode("utf-8")).hexdigest()
    )
    expected_schema_envelope = json.dumps(
        {
            "schema": match_schema(),
            "schema_version": MATCH_SCHEMA_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert (
        rendered.schema_hash
        == hashlib.sha256(expected_schema_envelope.encode("utf-8")).hexdigest()
    )
    assert rendered.worst_case_input_tokens <= value.max_input_tokens
    prompt_hash, schema_hash, schema_version = match_prompt_and_schema_hashes()
    assert rendered.prompt_hash == prompt_hash
    assert rendered.schema_hash == schema_hash
    assert schema_version == MATCH_SCHEMA_VERSION


@pytest.mark.parametrize(
    "rule",
    [
        "Decide whether exactly one supplied candidate page describes the same person.",
        "`matching_page` only when supplied evidence supports identity with exactly "
        "one candidate; set `selected_page_id` to that candidate's page id.",
        "`no_matching_page` when no supplied candidate describes the person; leave "
        "`selected_page_id` null.",
        "`uncertain` when the evidence is insufficient to decide; leave "
        "`selected_page_id` null.",
        "Name equality alone never establishes identity.",
    ],
)
def test_prompt_pins_each_complete_outcome_rule(rule: str) -> None:
    assert rule in _prompt_lines(render_match_request(_supplied()).system_prompt)


def test_material_fingerprint_is_stable_and_includes_all_material_bounds() -> None:
    config = _config()
    person = _person()
    first = base_material_fingerprint(person, config, refresh_of_observation_id=None)
    second = base_material_fingerprint(person, config, refresh_of_observation_id=None)
    assert first == second
    assert len(first) == 64

    # Positive control: identity fingerprint participates.
    changed_identity = base_material_fingerprint(
        _person(identity_fingerprint="b" * 64),
        config,
        refresh_of_observation_id=None,
    )
    assert changed_identity != first

    # Positive control: mutate one bound → fingerprint changes.
    changed_bound = base_material_fingerprint(
        person, _config(max_candidates=3), refresh_of_observation_id=None
    )
    assert changed_bound != first

    # refresh_of anchors successive interval refreshes (K17).
    with_refresh = base_material_fingerprint(
        person, config, refresh_of_observation_id=42
    )
    assert with_refresh != first

    prompt_hash, schema_hash, schema_version = match_prompt_and_schema_hashes()
    material = base_material_payload(
        person,
        config,
        refresh_of_observation_id=None,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
    )
    expected = hashlib.sha256(
        json.dumps(
            material, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    assert first == expected

    # Normative field completeness (design §Fingerprints).
    required_keys = {
        "task",
        "adapter_version",
        "person_id",
        "identity_fingerprint",
        "query_plan_version",
        "model",
        "parameters",
        "max_input_tokens",
        "max_completion_tokens",
        "max_candidates",
        "max_query_forms",
        "search_srlimit",
        "max_continuations_per_form",
        "max_search_hits_per_form",
        "max_page_ids_per_facts_request",
        "max_redirect_hops",
        "max_fact_pages_per_plan",
        "max_extract_characters",
        "max_categories_per_page",
        "max_names_in_prompt",
        "max_facts_in_prompt",
        "max_title_characters",
        "max_summary_characters",
        "prompt_hash",
        "schema_hash",
        "schema_version",
        "refresh_of_observation_id",
    }
    assert set(material) == required_keys
    assert material["task"] == "wikipedia_identity"
    assert material["adapter_version"] == WIKIPEDIA_ADAPTER_VERSION
    assert material["query_plan_version"] == QUERY_PLAN_VERSION
    assert material["schema_version"] == MATCH_SCHEMA_VERSION

    # Live candidate page IDs must not participate.
    assert "candidates" not in material
    assert "page_id" not in material
    assert all(
        key not in material for key in ("candidate_page_ids", "selected_page_id")
    )


@pytest.mark.parametrize(
    "bound_name",
    [
        "max_candidates",
        "max_query_forms",
        "search_srlimit",
        "max_continuations_per_form",
        "max_search_hits_per_form",
        "max_page_ids_per_facts_request",
        "max_redirect_hops",
        "max_fact_pages_per_plan",
        "max_extract_characters",
        "max_categories_per_page",
        "max_names_in_prompt",
        "max_facts_in_prompt",
        "max_title_characters",
        "max_summary_characters",
        "max_input_tokens",
        "max_completion_tokens",
    ],
)
def test_each_material_bound_changes_fingerprint(bound_name: str) -> None:
    person = _person()
    base = _config()
    baseline = base_material_fingerprint(person, base, refresh_of_observation_id=None)
    current = getattr(base, bound_name)
    mutated = base_material_fingerprint(
        person,
        _config(**{bound_name: current + 1}),
        refresh_of_observation_id=None,
    )
    assert mutated != baseline


def test_observation_matches_live_material_is_forward_only() -> None:
    person = _person()
    config = _config()
    fingerprint = base_material_fingerprint(
        person, config, refresh_of_observation_id=None
    )

    assert observation_matches_live_material(
        observation_task_fingerprint=fingerprint,
        plan_id=1,
        plan_refresh_of_observation_id=None,
        person=person,
        config=config,
    )
    assert not observation_matches_live_material(
        observation_task_fingerprint=fingerprint,
        plan_id=None,
        plan_refresh_of_observation_id=None,
        person=person,
        config=config,
    )
    assert not observation_matches_live_material(
        observation_task_fingerprint=fingerprint,
        plan_id=1,
        plan_refresh_of_observation_id=None,
        person=_person(identity_fingerprint="c" * 64),
        config=config,
    )
    # Refresh anchor must match the plan's stored refresh_of (not reverse hash).
    refreshed = base_material_fingerprint(person, config, refresh_of_observation_id=9)
    assert observation_matches_live_material(
        observation_task_fingerprint=refreshed,
        plan_id=2,
        plan_refresh_of_observation_id=9,
        person=person,
        config=config,
    )
    assert not observation_matches_live_material(
        observation_task_fingerprint=refreshed,
        plan_id=2,
        plan_refresh_of_observation_id=None,
        person=person,
        config=config,
    )


def test_schema_errors_retain_no_raw_provider_or_source_content() -> None:
    raw_sentinel = "RAW9z"
    provider_sentinel = "PROV8y"
    source_sentinel = "SRC7x"
    raw = json.dumps(
        {
            "outcome": "uncertain",
            "selected_page_id": None,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Safe rationale.",
            "unknown_provider_debug": {
                "raw": raw_sentinel,
                "provider": provider_sentinel,
            },
        }
    )
    supplied = _supplied(
        display_name=source_sentinel,
        sourced_names=(_name(source_sentinel),),
    )
    error = _capture_validation_error(raw, supplied)
    _assert_error_retains_no_sensitive_values(
        error, supplied, raw_sentinel, provider_sentinel, source_sentinel
    )
    # Positive control: traceback formatting still works without content leak.
    assert "MatchValidationError" in "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


def test_match_schema_requires_every_property_for_strict_structured_output() -> None:
    """Strict structured output rejects a `required` that omits any property.

    `selected_page_id` is nullable with a pydantic default, so pydantic left it
    out of `required` and OpenRouter answered HTTP 400 ("'required' is required
    to be supplied and to be an array including every key in properties").
    Optionality must ride on the nullable type instead.
    """
    schema = match_schema()

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
    assert "selected_page_id" in root_required


def test_match_schema_omits_no_matching_page_when_truncated() -> None:
    """Kills offering an outcome the domain validator forbids.

    A model shown `max_candidates` of N candidates cannot safely assert that
    no page exists, and `_domain_validation_error` rejects it. Leaving the
    outcome in the enum meant the model returned the honest answer, the call
    was paid for, and the response was discarded -- 6 of 6 people failed
    permanently this way, with retries unable to recover.
    """
    properties = match_schema(truncated_unsafe_for_negative=True)["properties"]
    assert isinstance(properties, dict)
    outcome = properties["outcome"]
    assert isinstance(outcome, dict)
    outcomes = set(outcome["enum"])
    assert "no_matching_page" not in outcomes
    assert outcomes == {"matching_page", "uncertain"}


def test_match_schema_offers_no_matching_page_when_not_truncated() -> None:
    """Positive control for the test above.

    Without this, that assertion would pass if the outcome were removed
    unconditionally -- which would make a true negative unreportable.
    """
    properties = match_schema(truncated_unsafe_for_negative=False)["properties"]
    assert isinstance(properties, dict)
    outcome = properties["outcome"]
    assert isinstance(outcome, dict)
    outcomes = set(outcome["enum"])
    assert "no_matching_page" in outcomes
    assert outcomes == {"matching_page", "no_matching_page", "uncertain"}


def test_match_schema_root_is_an_object_not_a_union() -> None:
    """Kills expressing the outcome/selected_page_id pairing as a root union.

    That pairing is a real validator rule and a root-level `anyOf` would make
    the invalid combination unrepresentable -- but strict structured output
    requires the root to be `type: "object"` and answers HTTP 400 to a root
    union. Verified live 2026-08-02: root `anyOf` REJECTED, plain object root
    ACCEPTED, nested `anyOf` on a property ACCEPTED. Detection's signal union
    works only because it is nested inside `mentions.items`.

    The pairing therefore stays a domain-validator rule. Do not "express it in
    the schema" at the root; it cannot be sent.
    """
    for flag in (False, True):
        schema = match_schema(truncated_unsafe_for_negative=flag)
        assert schema.get("type") == "object"
        assert "anyOf" not in schema


def test_match_schema_hash_differs_between_truncation_variants() -> None:
    """The two variants are different contracts and must not share a hash."""
    import hashlib
    import json

    def digest(flag: bool) -> str:
        canonical = json.dumps(
            match_schema(truncated_unsafe_for_negative=flag),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    assert digest(True) != digest(False)
