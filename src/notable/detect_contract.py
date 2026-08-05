"""The detect_people contract: models, wire schema, and domain validation.

Pure: no I/O, no network client. Every rule here traces to docs/findings.md.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from notable.config import DetectConfig
from notable.feeds import SourceItem

ITEM_OUTCOMES = ("research_people", "do_not_research", "uncertain")
MENTION_OUTCOMES = ("research", "do_not_research", "uncertain")
IDENTITY_FACT_KINDS = (
    "name",
    "profession_or_role",
    "place",
    "nationality",
    "era_or_date",
    "work",
    "affiliation",
    "other",
)
ATTENTION_CATEGORIES = (
    "significant_recognition",
    "enduring_contribution",
    "significant_work",
    "institutional_recognition",
    "sustained_field_attention",
    "major_achievement",
    "influential_role",
)
CAUTION_CATEGORIES = (
    "single_event_only",
    "inherited_association",
    "routine_role_or_listing",
    "primary_or_promotional",
    "significance_unclear",
)
PASSAGE_IDS = ("p1", "p2")


class DetectionInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Passage(_Strict):
    id: Literal["p1", "p2"]
    field: Literal["title", "summary"]
    text: str = Field(min_length=1)
    truncated: bool


class IdentityFact(_Strict):
    kind: str
    value: str = Field(min_length=1, max_length=500)
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)


class GroundedSignal(_Strict):
    kind: Literal["attention", "caution"]
    category: str
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)
    grounding: Literal["source_text", "domain_profile"]


class DetectedMention(_Strict):
    exact_name: str = Field(min_length=1, max_length=300)
    canonical_name: str = Field(min_length=1, max_length=300)
    outcome: Literal["research", "do_not_research", "uncertain"]
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)
    identity_facts: tuple[IdentityFact, ...]
    signals: tuple[GroundedSignal, ...]
    rationale: str = Field(min_length=1, max_length=1000)

    @property
    def research_worthy(self) -> bool:
        """High recall: uncertainty continues, it does not suppress."""
        return self.outcome in {"research", "uncertain"}


class DetectionOutput(_Strict):
    item_outcome: Literal["research_people", "do_not_research", "uncertain"]
    mentions: tuple[DetectedMention, ...]
    overflow: bool
    rationale: str = Field(min_length=1, max_length=1000)


def build_passages(item: SourceItem, config: DetectConfig) -> tuple[Passage, ...]:
    passages: list[Passage] = []
    for identifier, field, raw, limit in (
        ("p1", "title", item.title, config.max_title_characters),
        ("p2", "summary", item.summary, config.max_summary_characters),
    ):
        if not raw:
            continue
        bounded = raw[:limit]
        passages.append(
            Passage(
                id=identifier,  # type: ignore[arg-type]
                field=field,  # type: ignore[arg-type]
                text=bounded,
                truncated=bounded != raw,
            )
        )
    return tuple(passages)


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def _signal_variant(kind: str, categories: tuple[str, ...]) -> dict[str, Any]:
    """One arm of the signal union: a kind welded to its own category set."""
    return _object(
        {
            "kind": _string(enum=[kind]),
            "category": _string(enum=list(categories)),
            "claim": _string(maxLength=1000),
            "supporting_passage_ids": {
                "type": "array",
                "items": {"type": "string", "enum": list(PASSAGE_IDS)},
                "minItems": 1,
            },
            # Phase 1 supplies no domain profile, so `domain_profile` is not
            # offered. Making it representable means paying for it before the
            # validator refuses it.
            "grounding": _string(enum=["source_text"]),
        }
    )


def detection_schema(*, max_people: int) -> dict[str, object]:
    """The wire schema, capped at `max_people`.

    Config-dependent on purpose: a fixed cap here would drift from
    `max_people` silently, and an uncapped array means the overrun is paid for
    before it is rejected.

    Written by hand rather than derived from pydantic: strict structured
    output supports a narrow subset, and the root must be a plain object --
    a root-level `anyOf` is rejected with HTTP 400 (docs/findings.md).
    """
    passage_ids = {
        "type": "array",
        "items": {"type": "string", "enum": list(PASSAGE_IDS)},
        "minItems": 1,
    }
    return _object(
        {
            "item_outcome": _string(enum=list(ITEM_OUTCOMES)),
            "mentions": {
                "type": "array",
                "maxItems": max_people,
                "items": _object(
                    {
                        "exact_name": _string(maxLength=300),
                        "canonical_name": _string(maxLength=300),
                        "outcome": _string(enum=list(MENTION_OUTCOMES)),
                        "supporting_passage_ids": passage_ids,
                        "identity_facts": {
                            "type": "array",
                            "items": _object(
                                {
                                    "kind": _string(enum=list(IDENTITY_FACT_KINDS)),
                                    "value": _string(maxLength=500),
                                    "supporting_passage_ids": passage_ids,
                                }
                            ),
                        },
                        "signals": {
                            "type": "array",
                            # A union nested on a property, which strict mode
                            # accepts -- only the root may not be one. Paired
                            # this way, a caution category under kind
                            # "attention" is unrepresentable rather than
                            # billable-then-rejected (docs/findings.md).
                            "items": {
                                "anyOf": [
                                    _signal_variant("attention", ATTENTION_CATEGORIES),
                                    _signal_variant("caution", CAUTION_CATEGORIES),
                                ]
                            },
                        },
                        "rationale": _string(maxLength=1000),
                    }
                ),
            },
            "overflow": {"type": "boolean"},
            "rationale": _string(maxLength=1000),
        }
    )


def _contains(haystack: str, needle: str, *, fold_case: bool) -> bool:
    if fold_case:
        return needle.casefold() in haystack.casefold()
    return needle in haystack


def _contains_whole(haystack: str, needle: str) -> bool:
    """Containment that will not accept `Ana` for `Anastasia Poy`.

    Case-sensitive, because a name is an identity. Bounded by "the adjacent
    character is not a word character" rather than by `\\b`: names are not all
    Latin-script and not all space-delimited, and `\\b` means nothing where
    there are no word characters to bound.
    """
    if not needle:
        return False
    start = 0
    while (index := haystack.find(needle, start)) != -1:
        before = haystack[index - 1] if index > 0 else ""
        after_index = index + len(needle)
        after = haystack[after_index] if after_index < len(haystack) else ""
        if not (before.isalnum() or before == "_") and not (
            after.isalnum() or after == "_"
        ):
            return True
        start = index + 1
    return False


def validate_detection(
    raw: dict[str, Any], *, passages: tuple[Passage, ...], max_people: int
) -> DetectionOutput:
    """Parse and apply the rules the wire schema cannot express."""
    try:
        output = DetectionOutput.model_validate(raw)
    except ValidationError as error:
        raise DetectionInvalid(
            f"detection output failed validation: {error}"
        ) from error

    if len(output.mentions) > max_people:
        raise DetectionInvalid(
            f"{len(output.mentions)} mentions exceeds max_people {max_people}"
        )

    known = {passage.id for passage in passages}
    corpus = "\n".join(passage.text for passage in passages)

    if output.overflow and len(output.mentions) < max_people:
        # overflow asserts there were more people than the cap allowed, which
        # cannot be true of a list that is not full. Not expressible in the
        # schema: it relates two root-level properties, and strict mode has no
        # `if`/`then` and rejects a root-level union (docs/findings.md).
        raise DetectionInvalid(
            f"overflow is set but only {len(output.mentions)} of {max_people} "
            "mention slots were used"
        )

    for mention in output.mentions:
        _check_references(mention.supporting_passage_ids, known)
        # A name is an identity: grounding stays case-sensitive, and matches
        # whole words so `Ana` is not accepted for `Anastasia Poy`.
        if not _contains_whole(corpus, mention.exact_name):
            raise DetectionInvalid("mention name is not grounded in supplied text")

        for fact in mention.identity_facts:
            _check_references(fact.supporting_passage_ids, known)
            if fact.kind not in IDENTITY_FACT_KINDS:
                raise DetectionInvalid(f"unknown identity fact kind: {fact.kind}")
            # Case-insensitive, and searching *every* supplied passage rather
            # than only the cited ones. Both widenings fixed real rejections of
            # valid output; neither admits invention, because the value must
            # still appear as a contiguous run differing only in case.
            if not _contains(corpus, fact.value, fold_case=True):
                raise DetectionInvalid(
                    f"identity fact value is not grounded: {fact.value!r}"
                )

        for signal in mention.signals:
            _check_references(signal.supporting_passage_ids, known)
            if signal.grounding == "domain_profile":
                raise DetectionInvalid(
                    "domain_profile grounding requires a supplied profile; none is"
                )
            expected = (
                ATTENTION_CATEGORIES
                if signal.kind == "attention"
                else CAUTION_CATEGORIES
            )
            if signal.category not in expected:
                raise DetectionInvalid(
                    f"category {signal.category} is not valid "
                    f"for a {signal.kind} signal"
                )

    _check_item_outcome(output)
    return output


def _check_references(ids: tuple[str, ...], known: set[str]) -> None:
    unknown = set(ids) - known
    if unknown:
        raise DetectionInvalid(f"unknown passage reference: {sorted(unknown)}")


def _check_item_outcome(output: DetectionOutput) -> None:
    outcomes = {mention.outcome for mention in output.mentions}
    if output.item_outcome == "research_people" and not (
        outcomes & {"research", "uncertain"}
    ):
        raise DetectionInvalid(
            "item_outcome research_people requires a research or uncertain mention"
        )
    if output.item_outcome == "do_not_research" and outcomes - {"do_not_research"}:
        raise DetectionInvalid(
            "item_outcome do_not_research requires every mention to be do_not_research"
        )
    # The ported prompt: "Item `uncertain` iff no mention is `research` and
    # either no mentions are returned or at least one mention is `uncertain`."
    # Only the first clause is enforced. The prompt's three rules overlap --
    # an empty result satisfies both `do_not_research` and `uncertain` -- so
    # enforcing them as full biconditionals would reject valid output.
    if output.item_outcome == "uncertain" and "research" in outcomes:
        raise DetectionInvalid("item_outcome uncertain contradicts a research mention")
