"""Opt-in smoke for match_wikipedia_identity OpenRouter path.

Marked ``live`` and deselected by default. Requires ``OPENROUTER_API_KEY``.
Records model/provider/usage/cost/outcome without secrets when run.
"""

from __future__ import annotations

import os

import pytest

from notable_person_finder.config.models import (
    MatchWikipediaIdentityConfig,
    ProviderRoutingConfig,
)
from notable_person_finder.people.models import IdentityFactKind
from notable_person_finder.providers.openrouter import (
    ModelInspectionRequest,
    OpenRouterClient,
    StructuredGenerationRequest,
)
from notable_person_finder.runs.clock import SystemClock
from notable_person_finder.wikipedia.matching import (
    build_match_input,
    match_schema,
    render_match_request,
    validate_match_output,
)
from notable_person_finder.wikipedia.models import (
    MatchFact,
    MatchName,
    MatchWikiCandidate,
)

_API_KEY_ENV = "OPENROUTER_API_KEY"


def _require_key() -> str:
    key = os.environ.get(_API_KEY_ENV)
    if not key:
        pytest.skip(f"{_API_KEY_ENV} is unset; live OpenRouter match smoke deselected")
    return key


def _fixture_input(config: MatchWikipediaIdentityConfig):
    return build_match_input(
        person_id=1,
        display_name="Ada Lovelace",
        sourced_names=(
            MatchName(
                exact_name="Ada Lovelace",
                search_name="Ada Lovelace",
                match_key="ada lovelace",
                kind="professional",  # type: ignore[arg-type]
            ),
        ),
        identity_facts=(
            MatchFact(
                local_id="f1",
                kind=IdentityFactKind.PROFESSION_OR_ROLE,
                value="mathematician",
            ),
        ),
        candidates=(
            MatchWikiCandidate(
                page_id=1012,
                title="Ada Lovelace",
                canonical_url="https://en.wikipedia.org/wiki/Ada_Lovelace",
                namespace=0,
                is_disambiguation=False,
                description="English mathematician",
                extract="Augusta Ada King, Countess of Lovelace.",
                categories=("English mathematicians",),
                redirect_trail=(),
            ),
        ),
        config=config,
        truncated_unsafe_for_negative=False,
        partial_retrieval=False,
    )


@pytest.mark.live
def test_live_inspect_match_model() -> None:
    key = _require_key()
    config = MatchWikipediaIdentityConfig(model="openai/gpt-5.4-mini")
    with OpenRouterClient(
        api_key=key,
        endpoint="https://openrouter.ai/api/v1",
        routing=ProviderRoutingConfig(),
        timeout_seconds=60.0,
        clock=SystemClock(),
    ) as client:
        result = client.inspect_model(ModelInspectionRequest(model_id=config.model))
    assert result.configured_model_id == config.model
    assert result.resolved_model_id
    print(
        "live_match_inspect",
        {
            "configured_model": result.configured_model_id,
            "resolved_model": result.resolved_model_id,
            "supports_strict": result.supports_strict_structured_output,
            "pricing_usable": (
                result.prompt_unit_price_nano_usd is not None
                and result.completion_unit_price_nano_usd is not None
            ),
        },
    )


@pytest.mark.live
def test_live_generate_match_with_fixture_candidates() -> None:
    key = _require_key()
    match_config = MatchWikipediaIdentityConfig(
        model="openai/gpt-5.4-mini",
        max_input_tokens=8192,
        max_completion_tokens=512,
    )
    payload = _fixture_input(match_config)
    rendered = render_match_request(payload)
    schema = match_schema()
    with OpenRouterClient(
        api_key=key,
        endpoint="https://openrouter.ai/api/v1",
        routing=ProviderRoutingConfig(),
        timeout_seconds=90.0,
        clock=SystemClock(),
    ) as client:
        result = client.generate_structured(
            StructuredGenerationRequest(
                model_id=match_config.model,
                system_prompt=rendered.system_prompt,
                user_content=rendered.user_input_json,
                schema_name="match_wikipedia_identity",
                json_schema=schema,
                max_completion_tokens=match_config.max_completion_tokens,
                temperature=match_config.parameters.temperature,
                top_p=match_config.parameters.top_p,
                reasoning_effort=match_config.parameters.reasoning_effort,
            )
        )
    validated = validate_match_output(
        result.raw_text,
        payload,
        truncated_unsafe_for_negative=False,
    )
    print(
        "live_match_generate",
        {
            "configured_model": result.configured_model_id,
            "resolved_model": result.resolved_model_id,
            "serving_provider": result.serving_provider,
            "usage": (
                None
                if result.usage is None
                else {
                    "prompt_tokens": result.usage.prompt_tokens,
                    "completion_tokens": result.usage.completion_tokens,
                    "total_tokens": result.usage.total_tokens,
                }
            ),
            "actual_nano_usd": result.actual_nano_usd,
            "outcome": validated.outcome,
            "selected_page_id": validated.selected_page_id,
        },
    )
    assert validated.outcome in {"matching_page", "no_matching_page", "uncertain"}
