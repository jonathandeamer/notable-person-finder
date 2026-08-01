"""assess_article contract: schema, validation, render, fingerprints (K22/K33).

Material fingerprints deliberately exclude live article and candidate IDs
(bound by person identity + config bounds + policy). Forward recompute only
via ``refresh_of_plan_id`` — no reverse-hash API.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import resources
from typing import Annotated, Literal, cast

from pydantic import BaseModel, ConfigDict, Field

from notable_person_finder.config.models import AssessArticleConfig
from notable_person_finder.coverage.passages import PassageView, SelectedPassage
from notable_person_finder.coverage.queries import COVERAGE_QUERY_PLAN_VERSION
from notable_person_finder.people.models import (
    DomainProfileEvidence,
    IdentityFactKind,
)
from notable_person_finder.providers.article_versions import EXTRACTOR_VERSION

ASSESS_SCHEMA_VERSION = 1
COVERAGE_ADAPTER_VERSION = 1
ASSESS_CHAT_FRAMING_TOKEN_ALLOWANCE = 64
ASSESS_ARTICLE_TASK = "assess_article"
COVERAGE_EVIDENCE_MATERIAL_TASK = "coverage_evidence"

CONTENT_TYPES = frozenset(
    {
        "reporting",
        "profile",
        "review",
        "interview",
        "obituary",
        "listing",
        "announcement",
        "press_release",
        "sponsored",
        "other",
    }
)

ContentType = Literal[
    "reporting",
    "profile",
    "review",
    "interview",
    "obituary",
    "listing",
    "announcement",
    "press_release",
    "sponsored",
    "other",
]

PassageReference = Annotated[str, Field(pattern=r"^p[1-9][0-9]{0,5}$")]


class _StrictBoundaryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class AssessName(_StrictBoundaryModel):
    exact_name: str = Field(min_length=1, max_length=300)
    match_key: str = Field(min_length=1, max_length=300)
    kind: str = Field(min_length=1, max_length=64)


class AssessFact(_StrictBoundaryModel):
    local_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    kind: IdentityFactKind
    value: str = Field(min_length=1, max_length=500)


class AssessPassage(_StrictBoundaryModel):
    id: PassageReference
    text: str = Field(min_length=1)
    source_block_id: str = Field(min_length=1, max_length=64)
    truncated: bool


class AssessScreening(_StrictBoundaryModel):
    rule_id: str = Field(min_length=1, max_length=128)
    rule_status: str = Field(min_length=1, max_length=64)


class AssessView(_StrictBoundaryModel):
    kind: Literal["article_passages"]
    access_kind: Literal["full", "partial", "snippets"]
    name_absent: bool
    truncated_by_blocks: bool
    truncated_by_chars: bool
    extraction_quality: str | None = Field(default=None, max_length=64)
    passage_count: int = Field(ge=0, le=64)


class AssessArticleInput(_StrictBoundaryModel):
    task: Literal["assess_article"]
    person_id: int = Field(ge=1)
    display_name: str = Field(min_length=1, max_length=300)
    sourced_names: tuple[AssessName, ...]
    identity_facts: tuple[AssessFact, ...]
    person_article_id: int = Field(ge=1)
    canonical_article_id: int = Field(ge=1)
    article_view_id: int = Field(ge=1)
    screening: AssessScreening
    title: str | None = Field(default=None, max_length=2000)
    dek: str | None = Field(default=None, max_length=20_000)
    byline: str | None = Field(default=None, max_length=500)
    published_at: str | None = Field(default=None, max_length=64)
    editorial_labels: tuple[str, ...]
    passages: tuple[AssessPassage, ...]
    view: AssessView
    domain_profile: DomainProfileEvidence
    max_input_tokens: int = Field(ge=1)


class AssessSignal(_StrictBoundaryModel):
    kind: Literal["attention", "caution"]
    category: str = Field(min_length=1, max_length=128)
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[PassageReference, ...] = Field(min_length=1)


class AssessArticleOutput(_StrictBoundaryModel):
    person_relation: Literal["same_person", "different_person", "uncertain"]
    person_relation_passage_ids: tuple[PassageReference, ...]
    person_relation_rationale: str = Field(min_length=1, max_length=1000)
    coverage_depth: Literal["significant", "passing", "uncertain"]
    coverage_depth_passage_ids: tuple[PassageReference, ...]
    coverage_depth_rationale: str = Field(min_length=1, max_length=1000)
    content_types: tuple[ContentType, ...] = Field(min_length=1, max_length=3)
    content_types_passage_ids: tuple[PassageReference, ...]
    content_types_rationale: str = Field(min_length=1, max_length=1000)
    subject_relationship: Literal[
        "editorially_independent", "affiliated", "self_published", "uncertain"
    ]
    subject_relationship_passage_ids: tuple[PassageReference, ...]
    subject_relationship_rationale: str = Field(min_length=1, max_length=1000)
    signals: tuple[AssessSignal, ...]


@dataclass(frozen=True, slots=True)
class RenderedAssessRequest:
    task: Literal["assess_article"]
    system_prompt: str
    user_input_json: str
    schema: dict[str, object]
    canonical_schema_json: str
    schema_version: int
    prompt_hash: str
    schema_hash: str
    token_bearing_utf8_bytes: int
    chat_framing_token_allowance: int
    worst_case_input_tokens: int

    @property
    def canonical_input_json(self) -> str:
        return self.user_input_json


@dataclass(frozen=True, slots=True)
class CoveragePersonMaterialView:
    """Person fields needed for coverage material fingerprints (K30)."""

    person_id: int
    identity_fingerprint: str


class AssessValidationError(ValueError):
    """A safe rejection of model output that contains no supplied content."""


class AssessInputTooLarge(ValueError):
    """The bounded assess request does not fit ``max_input_tokens``.

    Distinguished from every other ``ValueError`` a caller may raise so that
    the handler can record a ``local_refuse`` assessment and let the plan
    advance, instead of wedging it on a settlement that bypasses persist.
    """


def assess_fixed_request_tokens() -> int:
    """The prompt + schema + chat-framing floor every assess request pays.

    Public so configuration validation can be checked against the real
    rendered floor rather than a hand-copied number.
    """
    return _fixed_request_tokens()


def build_assess_input(
    *,
    person_id: int,
    display_name: str,
    sourced_names: Sequence[AssessName],
    identity_facts: Sequence[AssessFact],
    person_article_id: int,
    canonical_article_id: int,
    article_view_id: int,
    screening_rule_id: str,
    screening_rule_status: str,
    title: str | None,
    dek: str | None,
    byline: str | None,
    published_at: str | None,
    editorial_labels: Sequence[str],
    passage_view: PassageView,
    access_kind: Literal["full", "partial", "snippets"],
    extraction_quality: str | None,
    domain_profile: DomainProfileEvidence,
    config: AssessArticleConfig,
) -> AssessArticleInput:
    """Build a strict assess input from already-selected passages and metadata."""
    passages = tuple(_to_assess_passage(passage) for passage in passage_view.passages)
    # Bound the two free-text article fields to their configured caps. Passage
    # selection already applies these caps to the body it builds; the raw
    # title and dek reached the request unbounded, so the configured ceiling
    # could not be reasoned about at configuration time.
    title = _prefix(title, config.max_title_characters)
    dek = _prefix(dek, config.max_summary_characters)
    value = AssessArticleInput(
        task="assess_article",
        person_id=person_id,
        display_name=display_name,
        sourced_names=tuple(sourced_names),
        identity_facts=tuple(identity_facts),
        person_article_id=person_article_id,
        canonical_article_id=canonical_article_id,
        article_view_id=article_view_id,
        screening=AssessScreening(
            rule_id=screening_rule_id,
            rule_status=screening_rule_status,
        ),
        title=title,
        dek=dek,
        byline=byline,
        published_at=published_at,
        editorial_labels=tuple(editorial_labels),
        passages=passages,
        view=AssessView(
            kind="article_passages",
            access_kind=access_kind,
            name_absent=passage_view.name_absent,
            truncated_by_blocks=passage_view.truncated_by_blocks,
            truncated_by_chars=passage_view.truncated_by_chars,
            extraction_quality=extraction_quality,
            passage_count=len(passages),
        ),
        domain_profile=domain_profile,
        max_input_tokens=config.max_input_tokens,
    )
    if _fixed_request_tokens() > config.max_input_tokens:
        raise AssessInputTooLarge(
            "max_input_tokens cannot fit the fixed prompt, schema, and chat framing"
        )
    if _worst_case_input_tokens(value) > config.max_input_tokens:
        raise AssessInputTooLarge("max_input_tokens cannot fit bounded assess metadata")
    return value


def assess_schema() -> dict[str, object]:
    schema = AssessArticleOutput.model_json_schema(mode="validation")
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

    compacted = cast(dict[str, object], compact(schema))
    passage_ids_schema: dict[str, object] = {
        "pattern": r"^p[1-9][0-9]{0,5}$",
        "type": "string",
    }
    bounded_text_schema: dict[str, object] = {
        "maxLength": 1000,
        "minLength": 1,
        "type": "string",
    }

    def factor_repeated(value: object) -> object:
        if value == passage_ids_schema:
            return {"$ref": "#/$defs/p"}
        if value == bounded_text_schema:
            return {"$ref": "#/$defs/t"}
        if isinstance(value, dict):
            return {key: factor_repeated(child) for key, child in value.items()}
        if isinstance(value, list):
            return [factor_repeated(child) for child in value]
        return value

    factored = cast(dict[str, object], factor_repeated(compacted))
    factored["$defs"] = {"p": passage_ids_schema, "t": bounded_text_schema}
    return factored


def render_assess_request(value: AssessArticleInput) -> RenderedAssessRequest:
    system_prompt, user_json, schema, schema_json = _render_parts(value)
    token_bearing_utf8_bytes = sum(
        len(part.encode("utf-8")) for part in (system_prompt, user_json, schema_json)
    )
    worst_case_input_tokens = (
        token_bearing_utf8_bytes + ASSESS_CHAT_FRAMING_TOKEN_ALLOWANCE
    )
    if worst_case_input_tokens > value.max_input_tokens:
        raise AssessInputTooLarge(
            "assess request exceeds worst-case input token ceiling"
        )
    schema_envelope = _canonical_json(
        {"schema": schema, "schema_version": ASSESS_SCHEMA_VERSION}
    )
    return RenderedAssessRequest(
        task="assess_article",
        system_prompt=system_prompt,
        user_input_json=user_json,
        schema=schema,
        canonical_schema_json=schema_json,
        schema_version=ASSESS_SCHEMA_VERSION,
        prompt_hash=_sha256(system_prompt),
        schema_hash=_sha256(schema_envelope),
        token_bearing_utf8_bytes=token_bearing_utf8_bytes,
        chat_framing_token_allowance=ASSESS_CHAT_FRAMING_TOKEN_ALLOWANCE,
        worst_case_input_tokens=worst_case_input_tokens,
    )


def validate_assess_output(
    raw: str, supplied: AssessArticleInput
) -> AssessArticleOutput:
    parsed = _parse_assess_output(raw)
    if parsed is None:
        safe_error = AssessValidationError("invalid assess output schema")
        del raw, supplied, parsed
        raise safe_error

    failure = _domain_validation_status(parsed, supplied)
    if failure is not None:
        safe_error = AssessValidationError(failure)
        del raw, supplied, parsed, failure
        raise safe_error

    del raw, supplied
    return parsed


def assess_prompt_and_schema_hashes() -> tuple[str, str, int]:
    """Stable hashes shared by assess path and material fingerprints."""
    prompt = _system_prompt()
    schema_envelope = _canonical_json(
        {
            "schema": assess_schema(),
            "schema_version": ASSESS_SCHEMA_VERSION,
        }
    )
    return _sha256(prompt), _sha256(schema_envelope), ASSESS_SCHEMA_VERSION


def coverage_material_fingerprint(
    person: CoveragePersonMaterialView,
    config: AssessArticleConfig,
    *,
    source_policy_fingerprint: str,
    refresh_of_plan_id: int | None = None,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> str:
    """Plan material fingerprint (forward-only; K30)."""
    return _sha256(
        _canonical_json(
            coverage_material_payload(
                person,
                config,
                source_policy_fingerprint=source_policy_fingerprint,
                refresh_of_plan_id=refresh_of_plan_id,
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
            )
        )
    )


def coverage_material_payload(
    person: CoveragePersonMaterialView,
    config: AssessArticleConfig,
    *,
    source_policy_fingerprint: str,
    refresh_of_plan_id: int | None = None,
    prompt_hash: str | None = None,
    schema_hash: str | None = None,
    schema_version: int | None = None,
) -> dict[str, object]:
    """Material fields for coverage evidence fingerprints (normative list)."""
    resolved_prompt_hash = prompt_hash
    resolved_schema_hash = schema_hash
    resolved_schema_version = schema_version
    if (
        resolved_prompt_hash is None
        or resolved_schema_hash is None
        or resolved_schema_version is None
    ):
        default_prompt, default_schema, default_version = (
            assess_prompt_and_schema_hashes()
        )
        resolved_prompt_hash = resolved_prompt_hash or default_prompt
        resolved_schema_hash = resolved_schema_hash or default_schema
        resolved_schema_version = resolved_schema_version or default_version

    parameters = config.parameters
    return {
        "task": COVERAGE_EVIDENCE_MATERIAL_TASK,
        "adapter_version": COVERAGE_ADAPTER_VERSION,
        "person_id": person.person_id,
        "identity_fingerprint": person.identity_fingerprint,
        "query_plan_version": COVERAGE_QUERY_PLAN_VERSION,
        "source_policy_fingerprint": source_policy_fingerprint,
        "extractor_version": EXTRACTOR_VERSION,
        "model": config.model,
        "parameters": {
            "temperature": parameters.temperature,
            "top_p": parameters.top_p,
            "reasoning_effort": parameters.reasoning_effort,
        },
        "max_input_tokens": config.max_input_tokens,
        "max_completion_tokens": config.max_completion_tokens,
        "retrieval_target": config.retrieval_target,
        "max_exact_forms": config.max_exact_forms,
        "max_alias_forms": config.max_alias_forms,
        "max_context_forms": config.max_context_forms,
        "search_count": config.search_count,
        "max_offsets_per_form": config.max_offsets_per_form,
        "max_results_per_form": config.max_results_per_form,
        "max_eligible_fetches": config.max_eligible_fetches,
        "max_unclassified_fetches": config.max_unclassified_fetches,
        "max_passage_characters": config.max_passage_characters,
        "max_passage_blocks": config.max_passage_blocks,
        "opening_block_count": config.opening_block_count,
        "max_title_characters": config.max_title_characters,
        "max_summary_characters": config.max_summary_characters,
        "reject_altered_query": config.reject_altered_query,
        "assess_ineligible": config.assess_ineligible,
        "prompt_hash": resolved_prompt_hash,
        "schema_hash": resolved_schema_hash,
        "schema_version": resolved_schema_version,
        "refresh_of_plan_id": refresh_of_plan_id,
    }


def assess_task_fingerprint(
    *,
    material_fingerprint: str,
    person_article_id: int,
    article_view_id: int,
) -> str:
    """Work-item and assessment-row task fingerprint (same string; K25)."""
    return _sha256(
        _canonical_json(
            {
                "material": material_fingerprint,
                "person_article_id": person_article_id,
                "article_view_id": article_view_id,
            }
        )
    )


def _to_assess_passage(passage: SelectedPassage) -> AssessPassage:
    return AssessPassage(
        id=passage.id,  # type: ignore[arg-type]
        text=passage.text,
        source_block_id=passage.source_block_id,
        truncated=passage.truncated,
    )


def _parse_assess_output(raw: str) -> AssessArticleOutput | None:
    try:
        return AssessArticleOutput.model_validate_json(raw, strict=True)
    except Exception:
        return None


def _domain_validation_status(
    output: AssessArticleOutput, supplied: AssessArticleInput
) -> str | None:
    try:
        return _domain_validation_error(output, supplied)
    except Exception:
        return "invalid assess output domain"


def _domain_validation_error(
    output: AssessArticleOutput, supplied: AssessArticleInput
) -> str | None:
    allowed = {passage.id for passage in supplied.passages}

    for owner, refs in (
        ("person_relation", output.person_relation_passage_ids),
        ("coverage_depth", output.coverage_depth_passage_ids),
        ("content_types", output.content_types_passage_ids),
        ("subject_relationship", output.subject_relationship_passage_ids),
    ):
        failure = _reference_error(refs, allowed, owner=owner)
        if failure is not None:
            return failure

    if len(output.content_types) != len(set(output.content_types)):
        return "content_types must not contain duplicates"
    for content_type in output.content_types:
        if content_type not in CONTENT_TYPES:
            return f"unknown content_type {content_type}"

    for signal_index, signal in enumerate(output.signals, start=1):
        owner = f"signal[{signal_index}]"
        failure = _reference_error(signal.supporting_passage_ids, allowed, owner=owner)
        if failure is not None:
            return failure
        if not signal.claim.strip():
            return f"{owner}: claim must be non-empty"
        if not signal.category.strip():
            return f"{owner}: category must be non-empty"

    for field_name, rationale in (
        ("person_relation_rationale", output.person_relation_rationale),
        ("coverage_depth_rationale", output.coverage_depth_rationale),
        ("content_types_rationale", output.content_types_rationale),
        ("subject_relationship_rationale", output.subject_relationship_rationale),
    ):
        if not rationale.strip():
            return f"{field_name} must be non-empty"
    return None


def _reference_error(
    references: tuple[str, ...],
    allowed: set[str],
    *,
    owner: str,
) -> str | None:
    for reference in references:
        if reference not in allowed:
            return f"{owner}: unseen passage id {reference}"
    return None


def _prefix(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    return value[:limit]


def _token_bearing_utf8_bytes(value: AssessArticleInput) -> int:
    prompt, user_json, _, schema_json = _render_parts(value)
    return sum(len(part.encode("utf-8")) for part in (prompt, user_json, schema_json))


def _worst_case_input_tokens(value: AssessArticleInput) -> int:
    return _token_bearing_utf8_bytes(value) + ASSESS_CHAT_FRAMING_TOKEN_ALLOWANCE


def _fixed_request_tokens() -> int:
    prompt = _system_prompt()
    schema_json = _canonical_json(assess_schema())
    return (
        len(prompt.encode("utf-8"))
        + len(schema_json.encode("utf-8"))
        + ASSESS_CHAT_FRAMING_TOKEN_ALLOWANCE
    )


def _render_parts(
    value: AssessArticleInput,
) -> tuple[str, str, dict[str, object], str]:
    prompt = _system_prompt()
    user_json = _canonical_json(value.model_dump(mode="json"))
    schema = assess_schema()
    schema_json = _canonical_json(schema)
    return prompt, user_json, schema, schema_json


def _system_prompt() -> str:
    return (
        resources.files("notable_person_finder.coverage")
        .joinpath("prompts", "assess_article.md")
        .read_text(encoding="utf-8")
    )


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
