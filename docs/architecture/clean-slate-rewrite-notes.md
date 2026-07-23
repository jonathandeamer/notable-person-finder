# Clean-Slate Rewrite: Architecture Notes

**Status:** Direction agreed; detailed design still in progress  
**Date:** 2026-07-23  
**Branch:** `design/clean-slate-rearchitecture`

## Purpose

Reimplement Notable Person Finder as a maintainable personal batch application. The application runs once daily on one machine, discovers people appearing in news coverage, checks whether they already have an English Wikipedia biography, assesses the breadth and quality of independent coverage, and produces a short confidence-ranked digest for human review.

The application will never create or edit Wikipedia articles.

## Agreed Product Constraints

- The application is a personal, once-daily batch tool running on one machine.
- The rewrite starts with no migration of existing events, caches, decisions, indices, or run history.
- RSS remains the discovery source.
- MediaWiki remains the source for existing-biography checks.
- Brave News Search remains the coverage-search provider.
- Each external service is hidden behind an application-owned interface so it can be replaced without changing the workflow.
- OpenRouter is the only LLM gateway.
- The Codex CLI, Claude CLI, and direct OpenAI backends will be removed.
- The workflow optimizes for recall because a human reviews the final digest.
- Digest results are ranked by confidence and evidence strength. Roughly ten candidates is the initial operational preference, not a hard architectural limit.
- Models, model routing, budgets, escalation thresholds, concurrency, and digest size are configuration values.
- A budget such as GBP 1 per run may be supplied and enforced, but the architecture must not require that exact value or any budget to be configured.

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

### Evaluation before prompt changes

A checked-in evaluation corpus contains representative positive, negative, ambiguous, same-name, globally known, and malformed-provider cases. Changes to prompts, schemas, thresholds, or default models run against this corpus and report at least recall, precision, abstention rate, cost, and latency by task.

The evaluation corpus tests task behavior; unit tests mock the `LlmClient` boundary and do not assert prose generated by a live model.

## Baseline Engineering Practices

- Manage the project and locked dependencies with `uv` and `pyproject.toml`.
- Use Ruff for formatting and linting.
- Use Pyright for static type checking.
- Use pytest for unit, contract, integration, and end-to-end tests.
- Measure coverage, with emphasis on deterministic domain behavior rather than a repository-wide vanity percentage.
- Keep network tests opt-in; the default suite is offline and deterministic.
- Run formatting, linting, type checking, tests, and database migration checks in CI.
- Use schema migrations from the first release even though SQLite is local.
- Keep secrets in environment variables or an OS-level secret mechanism, never in configuration files or the database.
- Use structured logs with run and candidate identifiers.
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
- [LangGraph overview](https://docs.langchain.com/oss/python/langgraph/overview)
