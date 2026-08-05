"""One model call per source item: who is meaningfully in this story?"""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources

from notable.config import Config
from notable.detect_contract import (
    DetectedMention,
    DetectionInvalid,
    DetectionOutput,
    build_passages,
    detection_schema,
    validate_detection,
)
from notable.errors import Incomplete, ProviderFailure
from notable.feeds import SourceItem
from notable.llm import LlmClient

logger = logging.getLogger(__name__)


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("detect_people.md")
        .read_text(encoding="utf-8")
    )


def people_in(
    item: SourceItem, config: Config, llm: LlmClient
) -> tuple[DetectedMention, ...]:
    """Detect mentions in one item.

    Raises `Incomplete` if the call or its validation fails: that ends this
    item, leaves it unsettled for a later run, and must never be converted
    into a semantic "found nobody". `BudgetExceeded` deliberately propagates —
    it ends the whole pass, not just this item.
    """
    passages = build_passages(item, config.detect)
    if not passages:
        return ()

    payload = {
        "task": "detect_people",
        "feed_key": item.feed_key,
        "publisher_label": item.publisher_label,
        "url": item.url,
        "published_at": item.published_at,
        "passages": [passage.model_dump() for passage in passages],
        "max_people": config.detect.max_people,
    }

    # The validator is handed to the client rather than applied to its return
    # value, so that domain rejection happens *before* the response is cached.
    # Applied afterwards, a rejected response is already stored: every later
    # retry replays the same bad answer without re-calling the provider, the
    # item burns its attempt cap without a single new request, and the bad
    # response lands in the fixture later phases are graded against.
    held: list[DetectionOutput] = []

    def _validate(raw: dict) -> None:
        held.append(
            validate_detection(
                raw, passages=passages, max_people=config.detect.max_people
            )
        )

    try:
        for attempt in range(3):
            try:
                llm.structured(
                    task="detect_people",
                    model=config.detect.model,
                    system=_system_prompt(),
                    user_payload=payload,
                    schema=detection_schema(max_people=config.detect.max_people),
                    max_completion_tokens=config.detect.max_completion_tokens,
                    reasoning_effort=config.detect.reasoning_effort,
                    timeout=config.transport.llm_read_timeout_seconds,
                    validate=_validate,
                )
                break
            except DetectionInvalid as e:
                if attempt == 2:
                    raise
                logger.warning("Retrying detect_people due to validation failure: %s", e)
    except ProviderFailure as error:
        logger.warning("detect_people failed for %s: %s", item.url, error)
        raise Incomplete(f"detect_people failed for {item.url}") from error
    except DetectionInvalid as error:
        # Not retried in-run: a retry re-sends identical input, and the prior
        # programme measured twelve such retries with zero recoveries.
        logger.warning("detect_people output rejected for %s: %s", item.url, error)
        raise Incomplete(f"detect_people output rejected for {item.url}") from error

    return held[0].mentions
