from __future__ import annotations

import hashlib
import json
import re
from importlib import resources
from typing import Any

from pydantic import ValidationError

from notable_person_finder.config.models import (
    DetectPeopleConfig,
    DomainProfileConfig,
    FeedConfig,
)
from notable_person_finder.people.models import (
    AttentionCategory,
    CautionCategory,
    DetectionInput,
    DetectionOutput,
    DetectionPassage,
    DetectionView,
    DomainProfileEvidence,
    DomainProfileEvidenceExample,
    ItemOutcome,
    MentionOutcome,
    RenderedDetectionRequest,
)

DETECTION_SCHEMA_VERSION = 1


class DetectionValidationError(ValueError):
    """A safe rejection of model output that contains no supplied content."""


def build_detection_input(
    source_item: Any,
    feed: FeedConfig,
    profile: DomainProfileConfig,
    config: DetectPeopleConfig,
) -> DetectionInput:
    title_original = _normal_text(_item_value(source_item, "title_text"), "title")
    summary_original = _normal_text(_item_value(source_item, "summary_text"), "summary")
    title = _prefix(title_original, config.max_title_characters)
    summary = _prefix(summary_original, config.max_summary_characters)
    title_truncated = title != title_original
    summary_truncated = summary != summary_original

    def make_value(
        current_title: str | None,
        current_summary: str | None,
        *,
        byte_truncated: bool,
    ) -> DetectionInput:
        current_title_truncated = title_truncated or current_title != title
        current_summary_truncated = summary_truncated or current_summary != summary
        passages: list[DetectionPassage] = []
        if current_title:
            passages.append(
                DetectionPassage(
                    id="p1",
                    field="title",
                    text=current_title,
                    truncated=current_title_truncated,
                )
            )
        if current_summary:
            passages.append(
                DetectionPassage(
                    id="p2",
                    field="summary",
                    text=current_summary,
                    truncated=current_summary_truncated,
                )
            )
        examples = tuple(
            DomainProfileEvidenceExample(
                category=AttentionCategory(category), examples=tuple(values)
            )
            for category, values in sorted(profile.attention_examples.items())
        )
        return DetectionInput(
            task="detect_people",
            source_item_id=_required_int(source_item, "id"),
            feed_id=_required_int(source_item, "feed_identity_id"),
            feed_key=feed.key,
            publisher_label=feed.label,
            title=current_title,
            summary=current_summary,
            passages=tuple(passages),
            canonical_article_id=_optional_value(source_item, "canonical_article_id"),
            original_url=_optional_value(source_item, "original_url"),
            published_at=_optional_value(source_item, "published_at"),
            published_issue=_optional_value(source_item, "published_issue"),
            url_issue=_optional_value(source_item, "url_issue"),
            view=DetectionView(
                kind="feed_metadata",
                title_available=title_original is not None,
                summary_available=summary_original is not None,
                title_truncated=current_title_truncated,
                summary_truncated=current_summary_truncated,
                input_truncated=(
                    title_truncated or summary_truncated or byte_truncated
                ),
            ),
            domain_profile=DomainProfileEvidence(
                version=profile.schema_version,
                key=profile.key,
                label=profile.label,
                language=profile.language,
                attention_examples=examples,
            ),
            max_people=config.max_people,
            max_input_tokens=config.max_input_tokens,
        )

    value = make_value(title, summary, byte_truncated=False)
    byte_limit = config.max_input_tokens * 4
    if _rendered_size(value) <= byte_limit:
        return value

    byte_truncated = True
    summary = _largest_fitting_prefix(
        summary,
        lambda candidate: (
            _rendered_size(make_value(title, candidate, byte_truncated=byte_truncated))
            <= byte_limit
        ),
    )
    value = make_value(title, summary, byte_truncated=byte_truncated)
    if _rendered_size(value) <= byte_limit:
        return value

    title = _largest_fitting_prefix(
        title,
        lambda candidate: (
            _rendered_size(
                make_value(candidate, summary, byte_truncated=byte_truncated)
            )
            <= byte_limit
        ),
    )
    value = make_value(title, summary, byte_truncated=byte_truncated)
    if _rendered_size(value) > byte_limit:
        raise ValueError("max_input_tokens is too small for the detection contract")
    return value


def detection_schema() -> dict[str, object]:
    return DetectionOutput.model_json_schema(mode="validation")


def render_detection_request(value: DetectionInput) -> RenderedDetectionRequest:
    system_prompt, user_json, schema, schema_json = _render_parts(value)
    input_utf8_bytes = sum(
        len(part.encode("utf-8")) for part in (system_prompt, user_json, schema_json)
    )
    if input_utf8_bytes > value.max_input_tokens * 4:
        raise ValueError("detection input exceeds max_input_tokens")
    schema_envelope = _canonical_json(
        {"schema": schema, "schema_version": DETECTION_SCHEMA_VERSION}
    )
    return RenderedDetectionRequest(
        task="detect_people",
        system_prompt=system_prompt,
        user_input_json=user_json,
        schema=schema,
        canonical_schema_json=schema_json,
        schema_version=DETECTION_SCHEMA_VERSION,
        prompt_hash=_sha256(system_prompt),
        schema_hash=_sha256(schema_envelope),
        input_utf8_bytes=input_utf8_bytes,
    )


def validate_detection_output(raw: str, supplied: DetectionInput) -> DetectionOutput:
    try:
        output = DetectionOutput.model_validate_json(raw, strict=True)
    except (ValidationError, ValueError, TypeError) as error:
        raise DetectionValidationError("invalid detection output schema") from error

    if len(output.mentions) > supplied.max_people:
        raise DetectionValidationError(
            f"mentions exceed supplied mention cap {supplied.max_people}"
        )

    passages = {passage.id: passage.text for passage in supplied.passages}
    profile_categories = {
        example.category for example in supplied.domain_profile.attention_examples
    }
    for mention_index, mention in enumerate(output.mentions, start=1):
        mention_id = f"mention[{mention_index}]"
        _validate_references(
            mention.supporting_passage_ids,
            passages,
            owner=mention_id,
        )
        if not _is_grounded_name(
            mention.exact_name,
            mention.supporting_passage_ids,
            passages,
        ):
            raise DetectionValidationError(f"{mention_id}: exact_name is not grounded")

        seen_fact_ids: set[str] = set()
        for fact in mention.identity_facts:
            if fact.local_id in seen_fact_ids:
                raise DetectionValidationError(
                    f"{mention_id}: duplicate identity fact id {fact.local_id}"
                )
            seen_fact_ids.add(fact.local_id)
            fact_owner = f"{mention_id} {fact.local_id}"
            _validate_references(
                fact.supporting_passage_ids,
                passages,
                owner=fact_owner,
            )
            if not _literal_is_grounded(
                fact.value, fact.supporting_passage_ids, passages
            ):
                raise DetectionValidationError(f"{fact_owner}: value is not grounded")

        for signal_index, signal in enumerate(mention.signals, start=1):
            signal_owner = f"{mention_id} signal[{signal_index}]"
            _validate_references(
                signal.supporting_passage_ids,
                passages,
                owner=signal_owner,
            )
            if signal.kind == "attention" and not isinstance(
                signal.category, AttentionCategory
            ):
                raise DetectionValidationError(
                    f"{signal_owner}: category does not match signal kind"
                )
            if signal.kind == "caution" and not isinstance(
                signal.category, CautionCategory
            ):
                raise DetectionValidationError(
                    f"{signal_owner}: category does not match signal kind"
                )
            if signal.grounding == "domain_profile" and (
                signal.kind != "attention" or signal.category not in profile_categories
            ):
                raise DetectionValidationError(
                    f"{signal_owner}: unseen domain_profile category"
                )

    _validate_outcome_consistency(output)
    if output.overflow and len(output.mentions) != supplied.max_people:
        raise DetectionValidationError(
            "overflow requires the returned mentions to reach the supplied cap"
        )
    return output


def _validate_references(
    references: tuple[str, ...],
    passages: dict[str, str],
    *,
    owner: str,
) -> None:
    for reference in references:
        if reference not in passages:
            raise DetectionValidationError(f"{owner}: unseen passage id {reference}")


def _is_grounded_name(
    name: str,
    references: tuple[str, ...],
    passages: dict[str, str],
) -> bool:
    pattern = re.compile(
        rf"(?<![^\W_]){re.escape(name)}(?![^\W_])",
        flags=re.UNICODE,
    )
    return any(pattern.search(passages[reference]) for reference in references)


def _literal_is_grounded(
    value: str,
    references: tuple[str, ...],
    passages: dict[str, str],
) -> bool:
    return any(value in passages[reference] for reference in references)


def _validate_outcome_consistency(output: DetectionOutput) -> None:
    outcomes = tuple(mention.outcome for mention in output.mentions)
    if output.item_outcome == ItemOutcome.RESEARCH_PEOPLE:
        valid = any(
            outcome in {MentionOutcome.RESEARCH, MentionOutcome.UNCERTAIN}
            for outcome in outcomes
        )
    elif output.item_outcome == ItemOutcome.DO_NOT_RESEARCH:
        valid = all(outcome == MentionOutcome.DO_NOT_RESEARCH for outcome in outcomes)
    else:
        valid = MentionOutcome.RESEARCH not in outcomes and (
            not outcomes or MentionOutcome.UNCERTAIN in outcomes
        )
    if not valid:
        raise DetectionValidationError(
            "item_outcome contradicts returned mention outcomes"
        )


def _normal_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be text or null")
    normalized = value.strip()
    return normalized or None


def _prefix(value: str | None, length: int) -> str | None:
    if value is None:
        return None
    return value[:length] or None


def _item_value(source_item: Any, key: str) -> object:
    try:
        return source_item[key]
    except (KeyError, IndexError, TypeError):
        try:
            return getattr(source_item, key)
        except AttributeError as error:
            raise ValueError(f"source item is missing {key}") from error


def _optional_value(source_item: Any, key: str) -> Any:
    try:
        return _item_value(source_item, key)
    except ValueError:
        return None


def _required_int(source_item: Any, key: str) -> int:
    value = _item_value(source_item, key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"source item {key} must be a positive integer")
    return value


def _largest_fitting_prefix(value: str | None, fits: Any) -> str | None:
    if value is None:
        return None
    low = 0
    high = len(value)
    best = -1
    while low <= high:
        middle = (low + high) // 2
        candidate = value[:middle] or None
        if fits(candidate):
            best = middle
            low = middle + 1
        else:
            high = middle - 1
    if best < 0:
        return None
    return value[:best] or None


def _rendered_size(value: DetectionInput) -> int:
    prompt, user_json, _, schema_json = _render_parts(value)
    return sum(len(part.encode("utf-8")) for part in (prompt, user_json, schema_json))


def _render_parts(
    value: DetectionInput,
) -> tuple[str, str, dict[str, object], str]:
    prompt = (
        resources.files("notable_person_finder.people")
        .joinpath("prompts", "detect_people.md")
        .read_text(encoding="utf-8")
    )
    user_json = _canonical_json(value.model_dump(mode="json"))
    schema = detection_schema()
    schema_json = _canonical_json(schema)
    return prompt, user_json, schema, schema_json


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
