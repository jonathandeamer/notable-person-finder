# Clean-Slate Rewrite: Architecture Notes

**Status:** Direction agreed; detailed design still in progress  
**Date:** 2026-07-23  
**Branch:** `refactor/rearchitecture`

## Purpose

Reimplement Notable Person Finder as a maintainable personal batch application. The application runs once daily on one machine, discovers people appearing in news coverage, checks whether they already have an English Wikipedia biography, assesses the breadth and quality of independent coverage, and produces a short confidence-ranked digest for human review.

The application will never create or edit Wikipedia articles.

## Agreed Product Constraints

- The application is a personal, once-daily batch tool running on one machine.
- The rewrite starts with no migration of existing events, caches, decisions, indices, or run history.
- The initial [visual arts discovery profile](../product/arts-discovery-profile.md) monitors ten editor-selected publications through RSS.
- MediaWiki remains the source for existing-biography checks.
- Brave Web Search is the initial broad coverage-search provider.
- Each external service is hidden behind an application-owned interface so it can be replaced without changing the workflow.
- OpenRouter is the only LLM gateway.
- The Codex CLI, Claude CLI, and direct OpenAI backends will be removed.
- The workflow optimizes for recall because a human reviews the final digest.
- Digest results are ranked by confidence and evidence strength. Roughly ten candidates is the initial operational preference, not a hard architectural limit.
- Models, model routing, budgets, escalation thresholds, concurrency, and digest size are configuration values.
- A budget such as GBP 1 per run may be supplied and enforced, but the architecture must not require that exact value or any budget to be configured.
- People and their supporting evidence persist across daily runs. New coverage can make an older candidate newly worth surfacing.

## Product Principles

The Wikimania 2026 talk, [Who's Missing? Using AI to Spot Wikipedia's Coverage Gaps](../product/wikimania-2026-lightning-talk.md), provides the product charter for the rewrite.

The system allocates scarce editor attention. It is not a notability oracle, an autonomous researcher, or an article-generation system.

- **AI pays attention; people exercise judgment.** Models assist with monitoring, primary-subject detection, identity matching, and evidence triage. Editors decide whether a person is notable, assess the sources, write the article, and participate in normal community review.
- **The output is a shortlist, not a verdict.** A label such as `likely_notable` means “worth an editor's attention,” not “meets Wikipedia's notability policy.”
- **Every recommendation is inspectable.** A surfaced person includes the
  coverage leads, relevant evidence, and the reason the system considers the
  candidate newly interesting. The application does not infer cross-source
  independence from a numeric result count.
- **Uncertainty remains visible.** Ambiguous identity, incomplete evidence, and model or provider failure cannot silently become a confident rejection.
- **Sources express editorial interest.** Editors choose the feeds and source sets that define the topics, regions, and communities to monitor. Source selection is part of the equity mechanism, not merely ingestion configuration.
- **Evidence accumulates over time.** A person is a durable entity rather than a row passing through a daily pipeline. Coverage from multiple days can combine into a stronger case and cause a candidate to resurface when materially new evidence arrives.
- **Reliability outranks automation.** The workflow prioritizes deliberately
  curated publishers, keeps deterministic control flow, records provenance,
  exposes provisional evidence for human review, and never creates or edits
  Wikimedia content.
- **Success is attention saved.** A small, well-explained set of genuinely useful leads is better than a large volume of classifications.
- **The method remains adaptable.** A technically curious editor can change source sets, policies, prompts, and models through configuration without modifying core workflow code.

The longer-term possibility of community subscriptions and topic-specific notifications informs clean interfaces and durable data, but multi-user accounts, shared hosting, and notification infrastructure remain outside the initial personal batch rewrite.

## Current Repository Assessment

The current implementation contains sound product ideas but expresses them through script-level coupling rather than stable application boundaries.

### What is worth preserving

- Deterministic application code controls the workflow.
- LLMs are restricted to semantic classification decisions.
- Failures generally fall back to conservative outcomes instead of silently producing confident negative decisions.
- External API responses are cached.
- The pipeline is covered by focused unit tests and an offline end-to-end smoke test.
- Prompts are checked into version control.
- Final decisions remain subject to human review.

### What should not be carried into the rewrite

- `run_pipeline.py` is an 824-line subprocess orchestrator rather than an application service.
- Production behavior is spread across executable files in `scripts/`; the largest LLM runners are approximately 600 to 900 lines each.
- The three LLM runners duplicate CLI invocation, retry, parsing, schema, output, and resume logic.
- JSONL files act simultaneously as transport, persistence, checkpoints, audit history, and application state.
- Records are untyped dictionaries whose implicit schemas drift between stages.
- Tests commonly use dynamic file imports because production code is not structured as an installable package.
- There is no checked-in Python dependency manifest or lockfile.
- The documented test command currently discovers 184 tests but reports 11 errors when `jsonschema` is absent; the dependency is used by tests but is not declared.
- Malformed JSONL rows are sometimes skipped, which can turn data corruption into silent data loss.
- Retry and last-write-wins behavior is reconstructed from append-only files instead of represented directly in durable state.
- Configuration is distributed across CLI flags, environment variables, prompt files, Markdown tables, and hard-coded defaults.

## Planned Legacy Behavior Review

Before the detailed rewrite design is finalized, the existing tests and implementation will be reviewed as evidence of legacy behavior. Existing tests are not automatically requirements: copying them wholesale would preserve obsolete interfaces and accidental complexity.

This review is underway. Approved decisions are indexed in the
[legacy behavior inventory](legacy-behavior-inventory.md) and recorded in one
file per capability.

### Review structure

The review is organized by product capability rather than by legacy filename:

1. discovery and feed ingestion;
2. person detection and initial triage;
3. Wikipedia identity matching;
4. coverage discovery and source reliability;
5. notability assessment and ranking;
6. digest generation;
7. failures, retries, budgets, and resumability.

Each capability is reviewed separately. The relevant tests, prompts, documentation, and implementation are inspected together, and the resulting behavior decisions are approved before moving to the next capability.

### Decision categories

Every meaningful legacy behavior receives one disposition:

- **Preserve:** a product, correctness, or safety requirement that should remain observable in the rewrite.
- **Change:** valuable intent whose current implementation or exact semantics should be redesigned.
- **Delete:** an obsolete feature, old-provider behavior, file-format concern, or other implementation artifact.
- **Investigate:** ambiguous behavior that requires a product decision before it can be specified.

Each entry records the behavior in user-visible terms, its evidence in the legacy repository, the decision and rationale, and how the new implementation will verify it. Verification may use pytest, Promptfoo, a manual acceptance check, or no replacement when the behavior is intentionally deleted.

Example structure:

| Behavior | Legacy evidence | Decision | New verification |
| --- | --- | --- | --- |
| Duplicate feed entries are processed once | RSS ingestion tests | Preserve | pytest |
| Model failure cannot become a confident rejection | Gate 3 and Gate 4b tests | Preserve | pytest |
| Distinct domains are treated as independent evidence | Gate 4b tests and prompt | Delete | Output-language and duplicate-URL tests |
| LLM JSON is recovered by searching for braces | LLM runner tests | Delete | Replaced by strict structured output |
| Existing JSONL output prevents a stage from running | Stage collision tests | Delete | Replaced by SQLite idempotency |

### Evidence rules

- Tests demonstrate implemented behavior, not necessarily desired behavior.
- Documentation and prompts are supporting evidence, not automatically authoritative when they disagree with tests or code.
- Implementation details are translated into observable behavior before a decision is made.
- Thresholds, heuristics, whitelists, and ranking rules are treated as product policy and require explicit approval.
- Legacy output snapshots may be used as temporary characterization evidence but do not become permanent golden masters by default.
- Deleted behavior does not receive a compatibility layer or replacement test merely to preserve historical structure.

### Outputs

The activity produces `docs/architecture/legacy-behavior-inventory.md` as an
index plus a logically discrete file containing each capability's traceability
table and approved dispositions. The final design specification includes only
preserved behavior and intentionally changed behavior expressed as acceptance
criteria.

Implementation tests are written from those approved acceptance criteria rather than copied mechanically from the legacy suite. Temporary characterization tests may be used while investigating unclear behavior, but they are not automatically retained in the new test suite.

## Considered Architectures

### 1. Modular monolith with SQLite

A packaged Python application with one CLI, one SQLite database, typed domain models, explicit workflow stages, and thin external-service adapters.

This is the selected direction. It provides reliable resumability, transactions, queryable provenance, and simple deployment without introducing service infrastructure.

### 2. Packaged pipeline retaining JSONL state

This would clean up imports and modules while continuing to pass files between stages. It would preserve easy artifact inspection but retain weak schema enforcement, awkward deduplication, whole-file processing, and fragile checkpoint semantics.

This approach is rejected because it preserves the most consequential accidental complexity in the current implementation.

### 3. LangChain or LangGraph workflow

The stages could be represented as graph nodes with framework-managed checkpointing and tracing.

This approach is rejected for the initial rewrite. The workflow is bounded and primarily linear, and it still needs durable domain storage. A graph framework would add a second state model and couple straightforward application behavior to framework abstractions. LangGraph may be reconsidered if the product later becomes interactive, long-running, dynamically tool-using, or dependent on human intervention during a run.

## Recommended Direction

Build a modular monolith in Python with the following properties:

- One installable package and one CLI entry point, tentatively `npf`.
- Ordinary Python orchestration with explicit stage transitions.
- SQLite as the source of truth for runs, discovered items, people, evidence, decisions, attempts, and cached provider responses.
- Pydantic models at configuration, provider, persistence, and LLM boundaries.
- A small OpenRouter adapter using the official OpenRouter Python SDK.
- Strict JSON Schema responses for every model-assisted decision.
- Deterministic adapters for RSS, MediaWiki, and Brave.
- Dependency injection through constructors and protocols, without a dependency-injection framework.
- Configuration loaded once, validated once, and passed explicitly.
- Idempotent stages that can safely resume after interruption.
- Structured logging and a complete decision audit trail.
- Evaluation-driven prompt and model changes.

This is not an autonomous agent architecture. Models make bounded semantic judgments; application code owns control flow, persistence, network access, retries, budgets, and terminal outcomes.

## LLM Engineering Practices

### One gateway owned by the application

All model calls go through a narrow application interface. Domain services depend on that interface rather than on the OpenRouter SDK. The adapter converts application requests into OpenRouter calls and records provider metadata without leaking transport types into the domain.

### Typed structured outputs

Every LLM task has a named input model and output model. The output model produces the strict JSON Schema sent to OpenRouter. Responses are validated locally before they can affect workflow state. Free-form JSON extraction and brace-finding parsers are not part of the new design.

OpenRouter provider routing must require support for the requested structured-output parameters. Models incapable of satisfying a task's schema are rejected before a run or treated as a configuration error.

### Explicit model routing

Configuration maps logical tasks to model policies rather than scattering model names through code. A policy can identify a primary model, optional fallback or escalation model, request parameters, maximum attempts, and per-task cost allowance.

The default operating strategy is cheap first-pass classification followed by selective escalation of ambiguous or high-value cases. The exact models remain configurable.

### Bounded retries

Retry policy distinguishes:

- transient transport and provider failures;
- rate limits;
- structured-output validation failures;
- model refusals;
- valid low-confidence answers.

Only the first three are mechanically retried. Low confidence is a domain outcome that may trigger a configured escalation; it is not treated as a transport failure.

### Budget enforcement

A run may have an optional budget. Before a call, the workflow estimates whether the configured request can fit within the remaining allowance. After a call, actual usage and cost are recorded. When the allowance is exhausted, remaining optional classifications receive an explicit `not_evaluated_budget` outcome; the workflow still produces a digest and a complete run report.

### Provenance

Each attempt records:

- run and task identifiers;
- model and resolved provider;
- prompt name and immutable prompt version;
- normalized input hash;
- request parameters;
- raw response retained according to configuration;
- parsed result or typed failure;
- token usage, reported cost, and latency;
- attempt number and timestamps.

This makes every surfaced candidate explainable and every model or prompt change measurable.

### Lean prompt evaluation

Promptfoo is a development tool for improving the model-assisted decisions. It is not a production dependency and does not become a general workflow-testing framework.

Each LLM task starts with approximately 20 to 30 hand-labelled cases covering representative positives, negatives, ambiguous identities, same-name collisions, globally known people, insufficient coverage, and previously mishandled inputs. Promptfoo compares prompt variants and configurable OpenRouter models using:

- recall;
- precision;
- structured-output failure rate;
- estimated or reported cost.

The production application and Promptfoo use the same prompt files and JSON Schemas. This prevents the evaluation copy of a prompt from drifting away from the deployed copy.

The initial corpus is deliberately small. A real production mistake becomes a regression case after its expected outcome has been reviewed. The suite grows from evidence rather than from an up-front effort to construct a statistically representative benchmark.

Pytest remains responsible for deterministic behavior: workflow transitions, ranking, persistence, provider adapters, retry classification, budget enforcement, and report generation. Unit tests mock the `LlmClient` boundary. One small offline end-to-end fixture, containing approximately ten representative people, verifies that the complete application is wired correctly without making network calls.

The rewrite receives one lightweight comparison with the legacy workflow. Approximately 20 representative inputs and their external evidence are saved, the legacy outputs are captured once, and the completed rewrite is run against the same evidence. Differences are reviewed manually and summarized. The new application will not contain a legacy compatibility adapter or maintain a permanent dual-workflow benchmark.

The project initially excludes repeated stochastic trials, confidence intervals, ranking metrics such as NDCG, separate development and holdout partitions, and LLM-as-judge scoring. These can be introduced later if the volume or consequences justify them.

## Baseline Engineering Practices

### Repository foundation

- Use the distribution name `notable-person-finder` and import package `notable_person_finder`.
- Put production code under `src/notable_person_finder/` and expose one installed CLI entry point.
- Use Python 3.13 consistently in `.python-version`, `project.requires-python`, and Pyright configuration.
- Manage the project, runtime dependencies, and development dependency group in `pyproject.toml` with `uv`.
- Use `uv_build` as the build backend and commit `uv.lock`.
- Do not reinitialize Git or let generated bootstrap files overwrite the existing repository blindly.
- Keep the existing MIT license.
- Do not include `tests/__init__.py`; pytest does not require tests to be an importable package.
- Do not include `py.typed` unless the package later becomes a supported library for external consumers.

### Code quality

- Use Ruff for formatting, import sorting, linting, and Python-version upgrades. Enable at least the `E`, `F`, `I`, and `UP` rule families with an 88-character line length.
- Use Pyright in standard type-checking mode.
- Require parameter and return annotations on every function signature.
- Avoid `Any`; an unavoidable use requires an inline explanation.
- Catch specific exceptions. A boundary-level broad exception is permitted only when it records the original failure and converts it into a defined application failure without hiding cancellation or shutdown signals.
- Use structured logging in application and provider code, including run and candidate identifiers.
- Direct terminal output is restricted to the CLI presentation and report-rendering boundary.

### Tests and evaluations

- Use pytest for unit, contract, migration, integration, and end-to-end tests.
- Organize focused tests near the source structure where useful, without forcing integration, migration, or end-to-end tests into a one-to-one mirror.
- Write the failing test before implementing domain behavior or a bug fix.
- Prefer unit tests at stable interfaces over deep mocks of internal implementation details.
- Mark tests that make real network requests with `@pytest.mark.integration` and run them only when explicitly selected.
- Keep the default test suite offline and deterministic.
- Use Promptfoo as a development-only prompt and model comparison tool.
- Measure coverage, with emphasis on deterministic domain behavior rather than a repository-wide vanity percentage.

### Local and CI enforcement

- Commit `.githooks/pre-commit` to run Ruff lint and format checks on staged Python files.
- Commit `.githooks/commit-msg` to enforce conventional commit subjects using `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`, `ci`, or `build`.
- Install the hooks locally with `git config core.hooksPath .githooks`.
- CI runs Ruff over the entire repository, Pyright, the offline pytest suite, and database migration checks.
- Document `uv sync`, `uv run pytest`, `uv run ruff check .`, `uv run ruff format .`, and `uv run pyright` as standard commands.

### Runtime operations

- Use schema migrations from the first release even though SQLite is local.
- Keep secrets in environment variables or an OS-level secret mechanism, never in configuration files or the database.
- Commit `.env.example` with `OPENROUTER_API_KEY`, `BRAVE_API_KEY`, and documented optional OpenRouter attribution settings, but no real credentials.
- Make every run acquire a single-machine lock and create a durable run record before doing work.

## Deliberate Non-Goals

- No web application.
- No multi-user support.
- No distributed task queue.
- No Celery, Airflow, or Kubernetes.
- No autonomous browsing or model-selected tools.
- No generic plugin system.
- No abstraction for hypothetical non-OpenRouter LLM gateways.
- No compatibility layer for current JSONL state.
- No automated Wikipedia edits.

## Remaining Design Work

The detailed design still needs agreement on:

- the legacy behavior inventory and disposition decisions;
- package and module boundaries;
- the domain model and SQLite schema;
- exact stage transitions and terminal outcomes;
- confidence ranking and escalation semantics;
- transaction and resumability boundaries;
- configuration file shape and precedence;
- error taxonomy and run status rules;
- report and digest schemas;
- test layers and provider contract fixtures.

After those sections are reviewed, this note will be superseded by a complete design specification and a task-by-task implementation plan.

## References

- [OpenRouter Python SDK](https://openrouter.ai/docs/client-sdks/python/overview)
- [OpenRouter structured outputs](https://openrouter.ai/docs/guides/features/structured-outputs)
- [OpenRouter provider routing](https://openrouter.ai/docs/guides/routing/provider-selection)
- [Promptfoo OpenRouter provider](https://www.promptfoo.dev/docs/providers/openrouter/)
- [Promptfoo assertions and metrics](https://www.promptfoo.dev/docs/configuration/expected-outputs/)
- [Dated OpenRouter model pricing and selection research](../research/openrouter-model-pricing-2026-07-23.md)
- [Wikimania 2026 lightning talk](../product/wikimania-2026-lightning-talk.md)
- [Visual arts discovery profile and evidence policy](../product/arts-discovery-profile.md)
- [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview)
- [Preferred Python repository bootstrap](https://github.com/jonathandeamer/jd-claude-skills/blob/master/plugins/bootstrap-python-repo/skills/bootstrap-python-repo/SKILL.md)
