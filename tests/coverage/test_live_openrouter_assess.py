"""Opt-in smoke coverage for the real ``assess_article`` OpenRouter boundary.

Marked ``live`` and deselected by default. Requires ``OPENROUTER_API_KEY``.
Records model/provider/usage/cost/outcome without secrets when run.

Deliberately uses ``AssessArticleConfig()`` -- the shipped default, with no
``max_input_tokens`` override -- against a real multi-paragraph article. A
prior Critical finding was that the shipped default ceiling could not fit the
fixed prompt-and-schema floor plus a real article at all; every offline
assess test had been overriding ``max_input_tokens`` to 16384 and so never
saw it. This smoke is the live counterpart to that Critical: inflating the
ceiling here would recreate exactly the blindness that let it ship.
"""

from __future__ import annotations

import os

import pytest

from notable_person_finder.config.models import (
    AssessArticleConfig,
    ProviderRoutingConfig,
)
from notable_person_finder.coverage.assessment import (
    AssessFact,
    AssessName,
    build_assess_input,
    render_assess_request,
    validate_assess_output,
)
from notable_person_finder.coverage.passages import (
    ArticleViewLike,
    TextBlock,
    select_passages,
)
from notable_person_finder.people.models import (
    AttentionCategory,
    DomainProfileEvidence,
    DomainProfileEvidenceExample,
    IdentityFactKind,
)
from notable_person_finder.providers.openrouter import (
    OpenRouterClient,
    StructuredGenerationRequest,
)
from notable_person_finder.runs.clock import SystemClock

_API_KEY_ENV = "OPENROUTER_API_KEY"

# A real, six-paragraph article -- long enough to be the genuine article the
# shipped default's docstring cites ("a six-paragraph article"), not a
# one-line fixture that would hide an overflow the way the offline tests did.
_ARTICLE_TITLE = "Ada Lovelace: the mathematician behind the first algorithm"
_ARTICLE_DEK = (
    "A profile of the countess who wrote the first published algorithm "
    "intended for a machine."
)
_ARTICLE_PARAGRAPHS = (
    "Augusta Ada King, Countess of Lovelace, born Ada Byron in 1815, was an "
    "English mathematician and writer chiefly known for her work on Charles "
    "Babbage's proposed mechanical general-purpose computer, the Analytical "
    "Engine.",
    "Lovelace was the only legitimate child of the poet Lord Byron and his "
    "wife Anne Isabella Milbanke. Her mother, wary of the volatility she "
    "associated with poetry, arranged for Ada to be tutored intensively in "
    "mathematics and logic from an early age.",
    "In 1833, at the age of seventeen, Ada was introduced to Charles Babbage "
    "and became fascinated by his plans for the Analytical Engine, a "
    "general-purpose mechanical computer far in advance of any machine yet "
    "built.",
    "Between 1842 and 1843, Lovelace translated an article by the Italian "
    "engineer Luigi Menabrea on the Analytical Engine, supplementing it with "
    "an extensive set of her own notes, labelled A through G, that were "
    "roughly three times the length of the original text.",
    "Note G, the longest, is often cited as the first published algorithm "
    "specifically tailored for implementation on a computer, describing a "
    "method for calculating Bernoulli numbers using the Engine. Lovelace "
    "also speculated more broadly than Babbage about the machine's "
    "potential, suggesting it could one day manipulate symbols and compose "
    "music, not merely numbers.",
    "Lovelace died in 1852 at the age of thirty-six. Her notes went largely "
    "unrecognized during her lifetime but were rediscovered in the twentieth "
    "century, and she is now widely regarded as an early visionary of "
    "computing; the Ada programming language was named in her honor.",
)


def _require_key() -> str:
    key = os.environ.get(_API_KEY_ENV)
    if not key:
        pytest.skip(f"{_API_KEY_ENV} is unset; live assess OpenRouter smoke deselected")
    return key


def _domain_profile() -> DomainProfileEvidence:
    return DomainProfileEvidence(
        version=1,
        key="stem-en",
        label="English STEM biography",
        language="en",
        attention_examples=(
            DomainProfileEvidenceExample(
                category=AttentionCategory.ENDURING_CONTRIBUTION,
                examples=("posthumous scholarly recognition",),
            ),
        ),
    )


@pytest.mark.live
def test_live_assess_article_with_shipped_default_config() -> None:
    key = _require_key()
    # The shipped default: no override of max_input_tokens or any other bound.
    config = AssessArticleConfig()

    passage_view = select_passages(
        ("Ada Lovelace",),
        ArticleViewLike(
            title=_ARTICLE_TITLE,
            dek=_ARTICLE_DEK,
            main_text_blocks=tuple(
                TextBlock(id=f"b{index}", text=paragraph)
                for index, paragraph in enumerate(_ARTICLE_PARAGRAPHS, start=1)
            ),
        ),
        config,
    )
    assert passage_view.passages, "fixture article produced no passages to assess"

    payload = build_assess_input(
        person_id=1,
        display_name="Ada Lovelace",
        sourced_names=(
            AssessName(
                exact_name="Ada Lovelace",
                match_key="ada lovelace",
                kind="professional",
            ),
        ),
        identity_facts=(
            AssessFact(
                local_id="f1",
                kind=IdentityFactKind.PROFESSION_OR_ROLE,
                value="mathematician",
            ),
        ),
        person_article_id=1,
        canonical_article_id=1,
        article_view_id=1,
        screening_rule_id="fixture.curated",
        screening_rule_status="curated_eligible",
        title=_ARTICLE_TITLE,
        dek=_ARTICLE_DEK,
        byline="Staff",
        published_at="2026-01-01",
        editorial_labels=("Profile",),
        passage_view=passage_view,
        access_kind="full",
        extraction_quality="full",
        domain_profile=_domain_profile(),
        config=config,
    )
    # render_assess_request itself raises AssessInputTooLarge if the shipped
    # default cannot fit prompt + schema + this article -- the live
    # counterpart to the Critical this smoke exists to catch.
    rendered = render_assess_request(payload)

    with OpenRouterClient(
        api_key=key,
        endpoint="https://openrouter.ai/api/v1",
        routing=ProviderRoutingConfig(),
        timeout_seconds=90.0,
        clock=SystemClock(),
    ) as client:
        result = client.generate_structured(
            StructuredGenerationRequest(
                model_id=config.model,
                system_prompt=rendered.system_prompt,
                user_content=rendered.user_input_json,
                schema_name="assess_article",
                json_schema=rendered.schema,
                max_completion_tokens=config.max_completion_tokens,
                temperature=None,
                top_p=None,
                reasoning_effort=config.parameters.reasoning_effort,
            )
        )

    validated = validate_assess_output(result.raw_text, payload)

    print(
        "live_assess_article",
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
            "person_relation": validated.person_relation,
            "coverage_depth": validated.coverage_depth,
            "outcome": "validated",
        },
    )

    # No assertion on specific content -- only on structure/type, which
    # validate_assess_output already enforces by raising on anything else.
    assert validated.person_relation in {
        "same_person",
        "different_person",
        "uncertain",
    }
    assert validated.coverage_depth in {"significant", "passing", "uncertain"}
    assert len(validated.content_types) >= 1
