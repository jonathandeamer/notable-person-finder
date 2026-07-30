"""Resolve-person-entity contract: schema, validation, render, fingerprints.

First-pass material fingerprints deliberately exclude live candidate person
ids and candidate identity fingerprints (K16). Reconsideration fingerprints
include both sides' identity fingerprints and the relation id.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from importlib import resources
from typing import Any, cast

from notable_person_finder.config.models import ResolvePersonEntityConfig
from notable_person_finder.people.models import (
    DetectionPassage,
    GroundedSignal,
    IdentityFact,
    RenderedResolutionRequest,
    ResolveCandidate,
    ResolvePersonEntityInput,
    ResolvePersonEntityOutput,
    ResolveView,
)

RESOLUTION_SCHEMA_VERSION = 1
RESOLUTION_ADAPTER_VERSION = 1
RECONSIDER_ADAPTER_VERSION = 1
RESOLUTION_CHAT_FRAMING_TOKEN_ALLOWANCE = 64
RESOLVE_PERSON_ENTITY_TASK = "resolve_person_entity"
RECONSIDER_PERSON_ENTITY_TASK = "reconsider_person_entity"


class ResolutionValidationError(ValueError):
    """A safe rejection of model output that contains no supplied content."""


def build_resolve_input(
    *,
    person_mention_id: int,
    source_item_id: int,
    exact_name: str,
    search_name: str,
    mention_outcome: str,
    passages: Sequence[DetectionPassage],
    identity_facts: Sequence[IdentityFact],
    signals: Sequence[GroundedSignal],
    candidates: Sequence[ResolveCandidate],
    config: ResolvePersonEntityConfig,
) -> ResolvePersonEntityInput:
    """Build a strict model-path resolve input from already-bounded material.

    Callers supply name-gated candidates (length 1..max_candidates). Empty
    candidate sets take the deterministic ``created_new`` path and never call
    this builder.
    """
    if mention_outcome not in {"research", "uncertain"}:
        raise ValueError("mention_outcome must be research or uncertain")
    candidate_tuple = tuple(candidates)
    if not candidate_tuple:
        raise ValueError("resolve model path requires at least one candidate")
    if len(candidate_tuple) > config.max_candidates:
        raise ValueError(
            f"candidates exceed configured max_candidates {config.max_candidates}"
        )
    for candidate in candidate_tuple:
        if len(candidate.names) > config.max_names_per_candidate:
            raise ValueError(
                "candidate names exceed configured max_names_per_candidate "
                f"{config.max_names_per_candidate}"
            )
        if len(candidate.identity_facts) > config.max_facts_per_candidate:
            raise ValueError(
                "candidate facts exceed configured max_facts_per_candidate "
                f"{config.max_facts_per_candidate}"
            )

    bounded_passages = tuple(
        _bound_passage(passage, config) for passage in passages if passage.text
    )
    value = ResolvePersonEntityInput(
        task="resolve_person_entity",
        person_mention_id=person_mention_id,
        source_item_id=source_item_id,
        exact_name=exact_name,
        search_name=search_name,
        mention_outcome=cast(Any, mention_outcome),
        passages=bounded_passages,
        identity_facts=tuple(identity_facts),
        signals=tuple(signals),
        candidates=candidate_tuple,
        max_candidates=config.max_candidates,
        view=ResolveView(
            kind="mention_candidates",
            candidate_count=len(candidate_tuple),
            passages_truncated=any(passage.truncated for passage in bounded_passages),
        ),
        max_input_tokens=config.max_input_tokens,
    )
    if _fixed_request_tokens() > config.max_input_tokens:
        raise ValueError(
            "max_input_tokens cannot fit the fixed prompt, schema, and chat framing"
        )
    if _worst_case_input_tokens(value) > config.max_input_tokens:
        raise ValueError("max_input_tokens cannot fit bounded resolve metadata")
    return value


def resolution_schema() -> dict[str, object]:
    schema = ResolvePersonEntityOutput.model_json_schema(mode="validation")
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
                # Keep nullable integer / enum anyOf as compacted branches.
                return {
                    "anyOf": [
                        cast(dict[str, object], compact(branch)) for branch in branches
                    ]
                }
            return result
        if isinstance(value, list):
            return [compact(child) for child in value]
        return value

    compacted = cast(dict[str, object], compact(schema))
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

    factored = cast(dict[str, object], factor_repeated(compacted))
    factored["$defs"] = {"t": bounded_text_schema}
    return factored


def render_resolution_request(
    value: ResolvePersonEntityInput,
) -> RenderedResolutionRequest:
    system_prompt, user_json, schema, schema_json = _render_parts(value)
    token_bearing_utf8_bytes = sum(
        len(part.encode("utf-8")) for part in (system_prompt, user_json, schema_json)
    )
    worst_case_input_tokens = (
        token_bearing_utf8_bytes + RESOLUTION_CHAT_FRAMING_TOKEN_ALLOWANCE
    )
    if worst_case_input_tokens > value.max_input_tokens:
        raise ValueError("resolution request exceeds worst-case input token ceiling")
    schema_envelope = _canonical_json(
        {"schema": schema, "schema_version": RESOLUTION_SCHEMA_VERSION}
    )
    return RenderedResolutionRequest(
        task="resolve_person_entity",
        system_prompt=system_prompt,
        user_input_json=user_json,
        schema=schema,
        canonical_schema_json=schema_json,
        schema_version=RESOLUTION_SCHEMA_VERSION,
        prompt_hash=_sha256(system_prompt),
        schema_hash=_sha256(schema_envelope),
        token_bearing_utf8_bytes=token_bearing_utf8_bytes,
        chat_framing_token_allowance=RESOLUTION_CHAT_FRAMING_TOKEN_ALLOWANCE,
        worst_case_input_tokens=worst_case_input_tokens,
    )


def validate_resolution_output(
    raw: str, supplied: ResolvePersonEntityInput
) -> ResolvePersonEntityOutput:
    parsed = _parse_resolution_output(raw)
    if parsed is None:
        safe_error = ResolutionValidationError("invalid resolution output schema")
        del raw, supplied, parsed
        raise safe_error

    failure = _domain_validation_status(parsed, supplied)
    if failure is not None:
        safe_error = ResolutionValidationError(failure)
        del raw, supplied, parsed, failure
        raise safe_error

    del raw, supplied
    return parsed


def resolution_prompt_and_schema_hashes() -> tuple[str, str, int]:
    """Stable hashes shared by model path, created_new, and fingerprints."""
    prompt = _system_prompt()
    schema_envelope = _canonical_json(
        {
            "schema": resolution_schema(),
            "schema_version": RESOLUTION_SCHEMA_VERSION,
        }
    )
    return _sha256(prompt), _sha256(schema_envelope), RESOLUTION_SCHEMA_VERSION


def first_pass_task_fingerprint(
    *,
    person_mention_id: int,
    mention_outcome: str,
    exact_name: str,
    search_name: str,
    identity_facts: Sequence[IdentityFact],
    signals: Sequence[GroundedSignal],
    config: ResolvePersonEntityConfig,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> str:
    """Material fingerprint for first-pass resolve work and ER rows (K16).

    Deliberately excludes live candidate person ids, candidate identity
    fingerprints, and corpus size.
    """
    resolved_prompt_hash = prompt_hash
    resolved_schema_hash = schema_hash
    resolved_schema_version = schema_version
    if (
        resolved_prompt_hash is None
        or resolved_schema_hash is None
        or resolved_schema_version is None
    ):
        default_prompt, default_schema, default_version = (
            resolution_prompt_and_schema_hashes()
        )
        resolved_prompt_hash = resolved_prompt_hash or default_prompt
        resolved_schema_hash = resolved_schema_hash or default_schema
        resolved_schema_version = resolved_schema_version or default_version

    parameters = config.parameters
    material = {
        "task": RESOLVE_PERSON_ENTITY_TASK,
        "adapter_version": RESOLUTION_ADAPTER_VERSION,
        "person_mention_id": person_mention_id,
        "mention_outcome": mention_outcome,
        "exact_name": exact_name,
        "search_name": search_name,
        "identity_facts": _sorted_identity_facts(identity_facts),
        "signals": _sorted_signals(signals),
        "model": config.model,
        "parameters": {
            "temperature": parameters.temperature,
            "top_p": parameters.top_p,
            "reasoning_effort": parameters.reasoning_effort,
        },
        "max_input_tokens": config.max_input_tokens,
        "max_completion_tokens": config.max_completion_tokens,
        "max_candidates": config.max_candidates,
        "max_facts_per_candidate": config.max_facts_per_candidate,
        "max_names_per_candidate": config.max_names_per_candidate,
        "max_title_characters": config.max_title_characters,
        "max_summary_characters": config.max_summary_characters,
        "prompt_hash": resolved_prompt_hash,
        "schema_hash": resolved_schema_hash,
        "schema_version": resolved_schema_version,
    }
    return _sha256(_canonical_json(material))


def reconsider_task_fingerprint(
    *,
    person_relation_id: int,
    subject_identity_fingerprint: str,
    peer_identity_fingerprint: str,
    config: ResolvePersonEntityConfig,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> str:
    """Fingerprint for edge-scoped reconsideration (both identity sides)."""
    resolved_prompt_hash = prompt_hash
    resolved_schema_hash = schema_hash
    resolved_schema_version = schema_version
    if (
        resolved_prompt_hash is None
        or resolved_schema_hash is None
        or resolved_schema_version is None
    ):
        default_prompt, default_schema, default_version = (
            resolution_prompt_and_schema_hashes()
        )
        resolved_prompt_hash = resolved_prompt_hash or default_prompt
        resolved_schema_hash = resolved_schema_hash or default_schema
        resolved_schema_version = resolved_schema_version or default_version

    parameters = config.parameters
    material = {
        "task": RECONSIDER_PERSON_ENTITY_TASK,
        "adapter_version": RECONSIDER_ADAPTER_VERSION,
        "person_relation_id": person_relation_id,
        "subject_identity_fingerprint": subject_identity_fingerprint,
        "peer_identity_fingerprint": peer_identity_fingerprint,
        "model": config.model,
        "parameters": {
            "temperature": parameters.temperature,
            "top_p": parameters.top_p,
            "reasoning_effort": parameters.reasoning_effort,
        },
        "max_input_tokens": config.max_input_tokens,
        "max_completion_tokens": config.max_completion_tokens,
        "prompt_hash": resolved_prompt_hash,
        "schema_hash": resolved_schema_hash,
        "schema_version": resolved_schema_version,
    }
    return _sha256(_canonical_json(material))


def _bound_passage(
    passage: DetectionPassage, config: ResolvePersonEntityConfig
) -> DetectionPassage:
    limit = (
        config.max_title_characters
        if passage.field == "title"
        else config.max_summary_characters
    )
    text = passage.text[:limit] or passage.text[:1]
    truncated = passage.truncated or text != passage.text
    if text == passage.text and truncated == passage.truncated:
        return passage
    return DetectionPassage(
        id=passage.id,
        field=passage.field,
        text=text,
        truncated=truncated,
    )


def _parse_resolution_output(raw: str) -> ResolvePersonEntityOutput | None:
    try:
        return ResolvePersonEntityOutput.model_validate_json(raw, strict=True)
    except Exception:
        return None


def _domain_validation_status(
    output: ResolvePersonEntityOutput, supplied: ResolvePersonEntityInput
) -> str | None:
    try:
        return _domain_validation_error(output, supplied)
    except Exception:
        return "invalid resolution output domain"


def _domain_validation_error(
    output: ResolvePersonEntityOutput, supplied: ResolvePersonEntityInput
) -> str | None:
    candidate_ids = {candidate.person_id for candidate in supplied.candidates}
    if output.outcome == "same_person":
        if output.selected_person_id is None:
            return "same_person requires selected_person_id"
        if output.selected_person_id not in candidate_ids:
            return "selected_person_id is not a supplied candidate"
    elif output.selected_person_id is not None:
        return f"{output.outcome} requires selected_person_id to be null"

    allowed_fact_ids = {fact.local_id for fact in supplied.identity_facts}
    for candidate in supplied.candidates:
        allowed_fact_ids.update(fact.local_id for fact in candidate.identity_facts)

    for fact_id in output.supporting_fact_ids:
        if fact_id not in allowed_fact_ids:
            return f"unseen supporting fact id {fact_id}"
    for fact_id in output.conflicting_fact_ids:
        if fact_id not in allowed_fact_ids:
            return f"unseen conflicting fact id {fact_id}"

    if not output.rationale.strip():
        return "rationale must be non-empty"
    return None


def _sorted_identity_facts(facts: Sequence[IdentityFact]) -> list[dict[str, object]]:
    rows = [
        {
            "local_id": fact.local_id,
            "kind": fact.kind.value if hasattr(fact.kind, "value") else fact.kind,
            "value": fact.value,
            "supporting_passage_ids": list(fact.supporting_passage_ids),
        }
        for fact in facts
    ]
    rows.sort(key=lambda row: str(row["local_id"]))
    return rows


def _sorted_signals(signals: Sequence[GroundedSignal]) -> list[dict[str, object]]:
    rows = [
        {
            "kind": signal.kind.value if hasattr(signal.kind, "value") else signal.kind,
            "category": (
                signal.category.value
                if hasattr(signal.category, "value")
                else signal.category
            ),
            "claim": signal.claim,
            "grounding": (
                signal.grounding.value
                if hasattr(signal.grounding, "value")
                else signal.grounding
            ),
            "supporting_passage_ids": list(signal.supporting_passage_ids),
        }
        for signal in signals
    ]
    rows.sort(
        key=lambda row: (
            str(row["kind"]),
            str(row["category"]),
            str(row["claim"]),
            str(row["grounding"]),
            json.dumps(row["supporting_passage_ids"], separators=(",", ":")),
        )
    )
    return rows


def _token_bearing_utf8_bytes(value: ResolvePersonEntityInput) -> int:
    prompt, user_json, _, schema_json = _render_parts(value)
    return sum(len(part.encode("utf-8")) for part in (prompt, user_json, schema_json))


def _worst_case_input_tokens(value: ResolvePersonEntityInput) -> int:
    return _token_bearing_utf8_bytes(value) + RESOLUTION_CHAT_FRAMING_TOKEN_ALLOWANCE


def _fixed_request_tokens() -> int:
    prompt = _system_prompt()
    schema_json = _canonical_json(resolution_schema())
    return (
        len(prompt.encode("utf-8"))
        + len(schema_json.encode("utf-8"))
        + RESOLUTION_CHAT_FRAMING_TOKEN_ALLOWANCE
    )


def _render_parts(
    value: ResolvePersonEntityInput,
) -> tuple[str, str, dict[str, object], str]:
    prompt = _system_prompt()
    user_json = _canonical_json(value.model_dump(mode="json"))
    schema = resolution_schema()
    schema_json = _canonical_json(schema)
    return prompt, user_json, schema, schema_json


def _system_prompt() -> str:
    return (
        resources.files("notable_person_finder.people")
        .joinpath("prompts", "resolve_person_entity.md")
        .read_text(encoding="utf-8")
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
