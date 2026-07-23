# OpenRouter Model Pricing and Selection Research

**Research date:** 2026-07-23  
**Currency:** USD  
**Status:** Point-in-time research; verify the live catalog before selecting production defaults

## Purpose

Compare the legacy workflow's default model, `openai/gpt-5.2`, with similarly capable or more economical OpenRouter models suitable for bounded person triage, Wikipedia identity matching, and news-coverage verification.

The comparison is a shortlist for Promptfoo evaluation, not a final model decision. Models, routing, reasoning settings, and budgets remain configuration values in the redesigned application.

## Requirements

Candidate models should:

- follow classification instructions reliably;
- support structured JSON output through OpenRouter;
- handle short-to-medium evidence bundles;
- distinguish people with similar names;
- judge whether coverage is substantial, independent, and focused on the subject;
- provide sufficient quality for a high-recall workflow;
- have a plausible role within an optional per-run budget.

Long context, autonomous tool use, coding ability, and multimodal support are not primary selection criteria for this application.

## Live Pricing Snapshot

Prices are OpenRouter list prices per one million tokens as observed on the research date. Provider discounts, caching, reasoning tokens, and routing can change actual cost.

| Model | Input / 1M | Output / 1M | Illustrative call | Relative to GPT-5.2 | Candidate role |
| --- | ---: | ---: | ---: | ---: | --- |
| `openai/gpt-5.2` | $1.75 | $14.00 | $0.0158 | 100% | Quality baseline or escalation |
| `anthropic/claude-sonnet-5` | $2.00 | $10.00 | $0.0150 | 95% | Frontier alternative |
| `google/gemini-2.5-pro` | $1.25 | $10.00 | $0.0113 | 71% | Strong reasoning alternative |
| `anthropic/claude-haiku-4.5` | $1.00 | $5.00 | $0.0075 | 48% | Fast value model |
| `openai/gpt-5.4-mini` | $0.75 | $4.50 | $0.0060 | 38% | Default candidate |
| `openai/gpt-5-mini` | $0.25 | $2.00 | $0.0023 | 14% | Cheap first-pass candidate |
| `qwen/qwen3.7-plus` | $0.32 | $1.28 | $0.0022 | 14% | Budget candidate |
| `deepseek/deepseek-v3.2` | approximately $0.23 | approximately $0.34 | $0.0013 | 8% | Very cheap triage candidate |

The illustrative call contains 5,000 input tokens and 500 billed output tokens:

```text
cost = (5,000 × input_price / 1,000,000)
     + (500 × output_price / 1,000,000)
```

The example does not predict a production call's exact size. In particular, billed reasoning tokens and a second coverage-verification pass can materially increase output cost.

## Illustrative Daily Cost

At 50 calls of the illustrative size:

| Model | Approximate cost for 50 calls |
| --- | ---: |
| `openai/gpt-5.2` | $0.79 |
| `anthropic/claude-sonnet-5` | $0.75 |
| `google/gemini-2.5-pro` | $0.56 |
| `anthropic/claude-haiku-4.5` | $0.38 |
| `openai/gpt-5.4-mini` | $0.30 |
| `openai/gpt-5-mini` | $0.11 |
| `qwen/qwen3.7-plus` | $0.11 |
| `deepseek/deepseek-v3.2` | $0.07 |

The calculation shows that GPT-5.2 may fit within the initial rough budget preference for a small run, but retries, reasoning, larger evidence bundles, and second-pass verification make it a poor universal default without evaluation evidence.

## Initial Promptfoo Shortlist

Limit the first comparison to four models so that results remain understandable:

1. `openai/gpt-5.4-mini`
   - Primary default candidate.
   - Substantially cheaper than GPT-5.2.
   - Intended to balance capability and high-throughput production use.

2. `qwen/qwen3.7-plus`
   - Primary budget candidate.
   - Particularly attractive for initial triage and possibly coverage verification.
   - Must demonstrate reliable identity handling and structured output on the project corpus.

3. `anthropic/claude-haiku-4.5`
   - Alternative value candidate from a different model family.
   - Helps reveal prompts that work only with OpenAI-style model behavior.

4. `openai/gpt-5.2`
   - Legacy quality baseline.
   - Candidate for selective escalation when a cheaper model returns an ambiguous result.

All four advertised response-format or structured-output support in OpenRouter's live model catalog on the research date. Production routing must still request structured-output-capable providers explicitly.

If the first comparison does not achieve adequate Gate 1 recall, a second experiment may add `deepseek/deepseek-v3.2` and `openai/gpt-5-mini`. Testing every plausible model in the initial matrix would add noise and evaluation cost without improving the first design decision.

## Initial Routing Hypothesis

This routing is a hypothesis for evaluation, not a committed default:

| Task | Primary candidates | Escalation candidates |
| --- | --- | --- |
| Initial person triage | Qwen3.7 Plus, GPT-5.4 Mini | GPT-5.2 |
| Wikipedia identity matching | GPT-5.4 Mini, Claude Haiku 4.5 | GPT-5.2 |
| Coverage verification | GPT-5.4 Mini, Qwen3.7 Plus | GPT-5.2, Claude Sonnet 5 |

The working prior is that GPT-5.4 Mini will provide the best simplicity, quality, and cost balance, while Qwen3.7 Plus has the strongest potential for a material cost reduction. Promptfoo results on project-specific cases should accept or reject that prior.

## Selection Rules

- Do not use a model merely because it is cheapest on general benchmarks.
- Optimize Gate 1 for recall; a cheap model that drops valid people is not economical.
- Measure precision, recall, structured-output failures, and cost separately for each task.
- Prefer one adequate default model over unnecessary per-stage diversity.
- Use escalation only when its trigger is explicit and measurable.
- Pin production model identifiers rather than following `latest` aliases.
- Recheck price, availability, structured-output support, and provider data policy before adopting a model.
- Record the resolved model, provider, usage, and cost for every production attempt.

## Sources

- [OpenRouter live model catalog](https://openrouter.ai/api/v1/models)
- [GPT-5.2 pricing](https://openrouter.ai/openai/gpt-5.2/api)
- [Claude Sonnet 5 catalog entry](https://openrouter.ai/anthropic/)
- [Gemini 2.5 Pro pricing](https://openrouter.ai/google/gemini-2.5-pro)
- [Claude Haiku 4.5 pricing](https://openrouter.ai/anthropic/claude-haiku-4.5/providers)
- [GPT-5.4 Mini pricing](https://openrouter.ai/openai/gpt-5.4-mini/api)
- [GPT-5 Mini pricing](https://openrouter.ai/openai/gpt-5-mini/providers)
- [Qwen3.7 Plus pricing](https://openrouter.ai/qwen/qwen3.7-plus/pricing)
- [DeepSeek V3.2 pricing](https://openrouter.ai/deepseek/deepseek-v3.2/pricing)
- [OpenRouter structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
