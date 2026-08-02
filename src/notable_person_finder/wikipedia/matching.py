"""match_wikipedia_identity contract: schema, validation, render, fingerprints.

Material fingerprints deliberately exclude live candidate page IDs (bound by
plan material + config bounds). ``observation_matches_live_material`` is
forward-only recompute (K17); there is no reverse-hash API.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from importlib import resources
from typing import cast

from notable_person_finder.config.models import MatchWikipediaIdentityConfig
from notable_person_finder.wikipedia.models import (
    MatchFact,
    MatchName,
    MatchView,
    MatchWikiCandidate,
    MatchWikipediaIdentityInput,
    MatchWikipediaIdentityOutput,
    RenderedMatchRequest,
    WikipediaPersonMaterialView,
)
from notable_person_finder.wikipedia.queries import QUERY_PLAN_VERSION

MATCH_SCHEMA_VERSION = 1
WIKIPEDIA_ADAPTER_VERSION = 1
MATCH_CHAT_FRAMING_TOKEN_ALLOWANCE = 64
MATCH_WIKIPEDIA_IDENTITY_TASK = "match_wikipedia_identity"
WIKIPEDIA_IDENTITY_MATERIAL_TASK = "wikipedia_identity"

_MODEL_TO_SEMANTIC: dict[str, str] = {
    "matching_page": "matching_page_found",
    "no_matching_page": "no_matching_page_found",
    "uncertain": "uncertain_identity",
}


class MatchValidationError(ValueError):
    """A safe rejection of model output that contains no supplied content."""


def build_match_input(
    *,
    person_id: int,
    display_name: str,
    sourced_names: Sequence[MatchName],
    identity_facts: Sequence[MatchFact],
    candidates: Sequence[MatchWikiCandidate],
    config: MatchWikipediaIdentityConfig,
    truncated_unsafe_for_negative: bool,
    partial_retrieval: bool,
) -> MatchWikipediaIdentityInput:
    """Build a strict match input from already-bounded material.

    Callers supply non-empty code-selected candidates (length 1..max_candidates).
    Empty candidate sets take the deterministic ``no_matching_page_found`` path
    and never call this builder.
    """
    candidate_tuple = tuple(candidates)
    if not candidate_tuple:
        raise ValueError("match model path requires at least one candidate")
    if len(candidate_tuple) > config.max_candidates:
        raise ValueError(
            f"candidates exceed configured max_candidates {config.max_candidates}"
        )

    name_tuple = tuple(sourced_names[: config.max_names_in_prompt])
    fact_tuple = tuple(identity_facts[: config.max_facts_in_prompt])
    bounded_candidates = tuple(
        _bound_candidate(candidate, config) for candidate in candidate_tuple
    )

    value = MatchWikipediaIdentityInput(
        task="match_wikipedia_identity",
        person_id=person_id,
        display_name=display_name,
        sourced_names=name_tuple,
        identity_facts=fact_tuple,
        candidates=bounded_candidates,
        max_candidates=config.max_candidates,
        view=MatchView(
            kind="wikipedia_candidates",
            candidate_count=len(bounded_candidates),
            truncated_unsafe_for_negative=truncated_unsafe_for_negative,
            partial_retrieval=partial_retrieval,
        ),
        max_input_tokens=config.max_input_tokens,
    )
    if _fixed_request_tokens() > config.max_input_tokens:
        raise ValueError(
            "max_input_tokens cannot fit the fixed prompt, schema, and chat framing"
        )
    if _worst_case_input_tokens(value) > config.max_input_tokens:
        raise ValueError("max_input_tokens cannot fit bounded match metadata")
    return value


def _require_every_property(value: object) -> object:
    """List every property of every object in ``required``.

    OpenAI-style strict structured output rejects a schema whose ``required``
    omits any key in ``properties``; optionality must be carried by a nullable
    type, not by absence from ``required``. Pydantic leaves a field with a
    default out of ``required``, which is why ``selected_page_id`` produced
    HTTP 400. This only widens ``required`` in the schema sent to the provider
    -- Python-side validation still accepts the field's default.
    """
    if isinstance(value, dict):
        result = {key: _require_every_property(child) for key, child in value.items()}
        properties = result.get("properties")
        if result.get("type") == "object" and isinstance(properties, dict):
            result["required"] = sorted(properties)
        return result
    if isinstance(value, list):
        return [_require_every_property(child) for child in value]
    return value


def _without_outcome(schema: dict[str, object], outcome: str) -> dict[str, object]:
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return schema
    declared = properties.get("outcome")
    if not isinstance(declared, dict):
        return schema
    outcomes = declared.get("enum")
    if not isinstance(outcomes, list) or outcome not in outcomes:
        return schema
    remaining = [name for name in outcomes if name != outcome]
    if not remaining:
        raise ValueError("removing the outcome would leave no permitted outcome")
    result = dict(schema)
    result["properties"] = {**properties, "outcome": {**declared, "enum": remaining}}
    return result


def _pair_outcome_with_selected_page_id(schema: dict[str, object]) -> dict[str, object]:
    """Constrain `selected_page_id` nullability by its sibling `outcome`.

    `_domain_validation_error` requires `matching_page` to carry a
    `selected_page_id` and every other outcome to leave it null, but the schema
    types the field `integer | null` with no dependency on `outcome`. Both
    invalid pairings are therefore structurally valid on the wire: the call is
    made and paid for, and only then rejected as `malformed_response`.

    Splitting the object into a discriminated union on `outcome` makes both
    unrepresentable. The domain validator stays in place as defence in depth.
    """
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        return schema
    outcome = properties.get("outcome")
    if not isinstance(outcome, dict):
        return schema
    outcomes = outcome.get("enum")
    if not isinstance(outcomes, list) or "selected_page_id" not in properties:
        return schema

    def variant(name: object, page_id: dict[str, object]) -> dict[str, object]:
        branch_properties = dict(properties)
        branch_properties["outcome"] = {"enum": [name]}
        branch_properties["selected_page_id"] = page_id
        branch = {key: value for key, value in schema.items() if key != "properties"}
        branch["properties"] = branch_properties
        branch["required"] = sorted(branch_properties)
        return branch

    selected: dict[str, object] = {"minimum": 1, "type": "integer"}
    null: dict[str, object] = {"type": "null"}
    return {
        "anyOf": [
            variant(name, selected if name == "matching_page" else null)
            for name in outcomes
        ]
    }


def match_schema(*, truncated_unsafe_for_negative: bool = False) -> dict[str, object]:
    """The match output schema, optionally without the negative outcome.

    When the candidate list was truncated, `_domain_validation_error` forbids
    `no_matching_page`: a model shown 8 of N candidates cannot safely assert
    that no page exists. Leaving that outcome in the enum anyway meant the
    model returned the honest answer, the call was paid for, and the response
    was discarded -- and retries could not recover, because the input is
    byte-identical. Measured live: 6 of 6 affected people failed permanently
    across 12 attempts, and truncation held for 111 of 118 plans.

    Removing the outcome forces `uncertain`, which is both the semantically
    correct answer for a partial list and coverage-eligible, so the person
    flows onward instead of dying.

    The default is the canonical (untruncated) schema, which is what
    `match_prompt_and_schema_hashes` hashes for seed-time fingerprints.
    """
    schema = MatchWikipediaIdentityOutput.model_json_schema(mode="validation")
    definitions = schema.get("$defs", {})

    def compact(value: object) -> object:
        if isinstance(value, dict):
            if set(value) == {"$ref"}:
                reference = value["$ref"]
                if isinstance(reference, str):
                    name = reference.rsplit("/", maxsplit=1)[-1]
                    target = definitions.get(name)
                    if target is not None:
                        return compact(target)
            result = {
                key: compact(child)
                for key, child in value.items()
                if key not in {"$defs", "title"}
            }
            if "enum" in result and result.get("type") == "string":
                del result["type"]
            branches = result.get("anyOf")
            if isinstance(branches, list) and all(
                isinstance(branch, dict)
                and (
                    set(branch) == {"enum"}
                    or set(branch) == {"type"}
                    or set(branch) <= {"type", "minimum", "maximum"}
                )
                for branch in branches
            ):
                return {
                    "anyOf": [
                        cast(dict[str, object], compact(branch)) for branch in branches
                    ]
                }
            return result
        if isinstance(value, list):
            return [compact(child) for child in value]
        return value

    compacted = cast(dict[str, object], _require_every_property(compact(schema)))
    bounded_text_schema: dict[str, object] = {
        "maxLength": 1000,
        "minLength": 1,
        "type": "string",
    }

    def factor_repeated(value: object) -> object:
        if value == bounded_text_schema:
            return {"$ref": "#/$defs/t"}
        if isinstance(value, dict):
            return {key: factor_repeated(child) for key, child in value.items()}
        if isinstance(value, list):
            return [factor_repeated(child) for child in value]
        return value

    if truncated_unsafe_for_negative:
        compacted = _without_outcome(compacted, "no_matching_page")
    factored = cast(dict[str, object], factor_repeated(compacted))
    factored["$defs"] = {"t": bounded_text_schema}
    return factored


def render_match_request(
    value: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool = False,
) -> RenderedMatchRequest:
    system_prompt, user_json, schema, schema_json = _render_parts(
        value, truncated_unsafe_for_negative=truncated_unsafe_for_negative
    )
    token_bearing_utf8_bytes = sum(
        len(part.encode("utf-8")) for part in (system_prompt, user_json, schema_json)
    )
    worst_case_input_tokens = (
        token_bearing_utf8_bytes + MATCH_CHAT_FRAMING_TOKEN_ALLOWANCE
    )
    if worst_case_input_tokens > value.max_input_tokens:
        raise ValueError("match request exceeds worst-case input token ceiling")
    schema_envelope = _canonical_json(
        {"schema": schema, "schema_version": MATCH_SCHEMA_VERSION}
    )
    return RenderedMatchRequest(
        task="match_wikipedia_identity",
        system_prompt=system_prompt,
        user_input_json=user_json,
        schema=schema,
        canonical_schema_json=schema_json,
        schema_version=MATCH_SCHEMA_VERSION,
        prompt_hash=_sha256(system_prompt),
        schema_hash=_sha256(schema_envelope),
        token_bearing_utf8_bytes=token_bearing_utf8_bytes,
        chat_framing_token_allowance=MATCH_CHAT_FRAMING_TOKEN_ALLOWANCE,
        worst_case_input_tokens=worst_case_input_tokens,
    )


def validate_match_output(
    raw: str,
    supplied: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool,
) -> MatchWikipediaIdentityOutput:
    parsed = _parse_match_output(raw)
    if parsed is None:
        safe_error = MatchValidationError("invalid match output schema")
        del raw, supplied, parsed
        raise safe_error

    failure = _domain_validation_status(
        parsed,
        supplied,
        truncated_unsafe_for_negative=truncated_unsafe_for_negative,
    )
    if failure is not None:
        safe_error = MatchValidationError(failure)
        del raw, supplied, parsed, failure
        raise safe_error

    del raw, supplied
    return parsed


def map_model_outcome_to_semantic(outcome: str) -> str:
    """Map Task 3 short model outcomes to product semantic outcomes (K6)."""
    try:
        return _MODEL_TO_SEMANTIC[outcome]
    except KeyError as error:
        raise ValueError(f"unknown model outcome {outcome!r}") from error


def match_prompt_and_schema_hashes() -> tuple[str, str, int]:
    """Stable hashes shared by match path and material fingerprints."""
    prompt = _system_prompt()
    schema_envelope = _canonical_json(
        {
            "schema": match_schema(),
            "schema_version": MATCH_SCHEMA_VERSION,
        }
    )
    return _sha256(prompt), _sha256(schema_envelope), MATCH_SCHEMA_VERSION


def base_material_fingerprint(
    person: WikipediaPersonMaterialView,
    config: MatchWikipediaIdentityConfig,
    *,
    refresh_of_observation_id: int | None = None,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> str:
    """Plan / observation material fingerprint (forward-only; K17).

    Includes every bound that changes Task 3 input or candidate text. Live
    candidate page IDs are excluded. ``refresh_of_observation_id`` anchors
    interval refreshes.
    """
    return _sha256(
        _canonical_json(
            base_material_payload(
                person,
                config,
                refresh_of_observation_id=refresh_of_observation_id,
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
            )
        )
    )


def base_material_payload(
    person: WikipediaPersonMaterialView,
    config: MatchWikipediaIdentityConfig,
    *,
    refresh_of_observation_id: int | None = None,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> dict[str, object]:
    """Material fields for Wikipedia identity fingerprints."""
    resolved_prompt_hash = prompt_hash
    resolved_schema_hash = schema_hash
    resolved_schema_version = schema_version
    if (
        resolved_prompt_hash is None
        or resolved_schema_hash is None
        or resolved_schema_version is None
    ):
        default_prompt, default_schema, default_version = (
            match_prompt_and_schema_hashes()
        )
        resolved_prompt_hash = resolved_prompt_hash or default_prompt
        resolved_schema_hash = resolved_schema_hash or default_schema
        resolved_schema_version = resolved_schema_version or default_version

    parameters = config.parameters
    return {
        "task": WIKIPEDIA_IDENTITY_MATERIAL_TASK,
        "adapter_version": WIKIPEDIA_ADAPTER_VERSION,
        "person_id": person.person_id,
        "identity_fingerprint": person.identity_fingerprint,
        "query_plan_version": QUERY_PLAN_VERSION,
        "model": config.model,
        "parameters": {
            "temperature": parameters.temperature,
            "top_p": parameters.top_p,
            "reasoning_effort": parameters.reasoning_effort,
        },
        "max_input_tokens": config.max_input_tokens,
        "max_completion_tokens": config.max_completion_tokens,
        "max_candidates": config.max_candidates,
        "max_query_forms": config.max_query_forms,
        "search_srlimit": config.search_srlimit,
        "max_continuations_per_form": config.max_continuations_per_form,
        "max_search_hits_per_form": config.max_search_hits_per_form,
        "max_page_ids_per_facts_request": config.max_page_ids_per_facts_request,
        "max_redirect_hops": config.max_redirect_hops,
        "max_fact_pages_per_plan": config.max_fact_pages_per_plan,
        "max_extract_characters": config.max_extract_characters,
        "max_categories_per_page": config.max_categories_per_page,
        "max_names_in_prompt": config.max_names_in_prompt,
        "max_facts_in_prompt": config.max_facts_in_prompt,
        "max_title_characters": config.max_title_characters,
        "max_summary_characters": config.max_summary_characters,
        "prompt_hash": resolved_prompt_hash,
        "schema_hash": resolved_schema_hash,
        "schema_version": resolved_schema_version,
        "refresh_of_observation_id": refresh_of_observation_id,
    }


def observation_matches_live_material(
    *,
    observation_task_fingerprint: str,
    plan_id: int | None,
    plan_refresh_of_observation_id: int | None,
    person: WikipediaPersonMaterialView,
    config: MatchWikipediaIdentityConfig,
) -> bool:
    """Forward recompute: does this observation match live person+config material?

    Requires a plan (``plan_id`` non-null). Uses the plan's stored
    ``refresh_of_observation_id`` — never reverse-hash surgery (K17).
    """
    if plan_id is None:
        return False
    expected = base_material_fingerprint(
        person,
        config,
        refresh_of_observation_id=plan_refresh_of_observation_id,
    )
    return observation_task_fingerprint == expected


def _bound_candidate(
    candidate: MatchWikiCandidate, config: MatchWikipediaIdentityConfig
) -> MatchWikiCandidate:
    extract = candidate.extract
    if extract is not None and len(extract) > config.max_extract_characters:
        extract = extract[: config.max_extract_characters]
    description = candidate.description
    if description is not None and len(description) > config.max_extract_characters:
        description = description[: config.max_extract_characters]
    categories = candidate.categories[: config.max_categories_per_page]
    title = candidate.title[: config.max_title_characters] or candidate.title[:1]
    if (
        extract == candidate.extract
        and description == candidate.description
        and categories == candidate.categories
        and title == candidate.title
    ):
        return candidate
    return MatchWikiCandidate(
        page_id=candidate.page_id,
        title=title,
        canonical_url=candidate.canonical_url,
        namespace=candidate.namespace,
        is_disambiguation=candidate.is_disambiguation,
        description=description,
        extract=extract,
        categories=categories,
        redirect_trail=candidate.redirect_trail,
    )


def _parse_match_output(raw: str) -> MatchWikipediaIdentityOutput | None:
    try:
        return MatchWikipediaIdentityOutput.model_validate_json(raw, strict=True)
    except Exception:
        return None


def _domain_validation_status(
    output: MatchWikipediaIdentityOutput,
    supplied: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool,
) -> str | None:
    try:
        return _domain_validation_error(
            output,
            supplied,
            truncated_unsafe_for_negative=truncated_unsafe_for_negative,
        )
    except Exception:
        return "invalid match output domain"


def _domain_validation_error(
    output: MatchWikipediaIdentityOutput,
    supplied: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool,
) -> str | None:
    if truncated_unsafe_for_negative and output.outcome == "no_matching_page":
        return "no_matching_page is invalid when truncated_unsafe_for_negative"

    candidate_ids = {candidate.page_id for candidate in supplied.candidates}
    if output.outcome == "matching_page":
        if output.selected_page_id is None:
            return "matching_page requires selected_page_id"
        if output.selected_page_id not in candidate_ids:
            return "selected_page_id is not a supplied candidate"
    elif output.selected_page_id is not None:
        return f"{output.outcome} requires selected_page_id to be null"

    allowed_fact_ids = {fact.local_id for fact in supplied.identity_facts}
    for fact_id in output.supporting_fact_ids:
        if fact_id not in allowed_fact_ids:
            return f"unseen supporting fact id {fact_id}"
    for fact_id in output.conflicting_fact_ids:
        if fact_id not in allowed_fact_ids:
            return f"unseen conflicting fact id {fact_id}"

    if not output.rationale.strip():
        return "rationale must be non-empty"
    return None


def _token_bearing_utf8_bytes(value: MatchWikipediaIdentityInput) -> int:
    prompt, user_json, _, schema_json = _render_parts(value)
    return sum(len(part.encode("utf-8")) for part in (prompt, user_json, schema_json))


def _worst_case_input_tokens(value: MatchWikipediaIdentityInput) -> int:
    return _token_bearing_utf8_bytes(value) + MATCH_CHAT_FRAMING_TOKEN_ALLOWANCE


def _fixed_request_tokens() -> int:
    prompt = _system_prompt()
    schema_json = _canonical_json(match_schema())
    return (
        len(prompt.encode("utf-8"))
        + len(schema_json.encode("utf-8"))
        + MATCH_CHAT_FRAMING_TOKEN_ALLOWANCE
    )


def _render_parts(
    value: MatchWikipediaIdentityInput,
    *,
    truncated_unsafe_for_negative: bool = False,
) -> tuple[str, str, dict[str, object], str]:
    prompt = _system_prompt()
    user_json = _canonical_json(value.model_dump(mode="json"))
    schema = match_schema(truncated_unsafe_for_negative=truncated_unsafe_for_negative)
    schema_json = _canonical_json(schema)
    return prompt, user_json, schema, schema_json


def _system_prompt() -> str:
    return (
        resources.files("notable_person_finder.wikipedia")
        .joinpath("prompts", "match_wikipedia_identity.md")
        .read_text(encoding="utf-8")
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
