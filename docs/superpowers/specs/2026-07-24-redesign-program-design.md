# Redesign Programme Design

**Status:** Completed
**Date:** 2026-07-24
**Branch:** `refactor/rearchitecture`

## Purpose

This document defines how the clean-slate redesign of Notable Person Finder will
be designed before implementation begins. The redesign spans product policy,
legacy behavior, durable state, bounded LLM decisions, external providers, and
the daily operator experience. These concerns will be designed in a controlled
sequence rather than compressed into one large session.

The programme covers the personal, once-daily, single-machine visual-arts tool
already scoped in the branch documentation. It does not authorize
implementation.

## Outputs and Authority

The design programme will produce:

1. A legacy behavior inventory index plus one focused inventory file per
   product capability. Each behavior will cite its legacy evidence and receive
   an approved disposition.
2. Five focused design specifications covering the product workflow, domain
   and persistence, LLM behavior, provider contracts, and operator experience.
3. A short overarching system specification that connects the focused designs,
   records cross-cutting invariants, and resolves inconsistencies without
   duplicating their detail.
4. Detailed implementation plans after all design specifications and the
   overarching system specification have been approved.

The sources of authority, in descending order, are:

1. approved product decisions;
2. the new design specifications;
3. legacy tests, prompts, documentation, and implementation as evidence of old
   behavior.

Legacy evidence is not automatically a requirement. Where legacy sources
disagree, the intended behavior will be decided according to product purpose,
safety, and simplicity. An intentionally deleted behavior will receive neither
a compatibility layer nor a replacement test.

## Locked Design Constraints

Decisions already recorded on this branch are constraints for the design
programme rather than questions to reopen routinely:

- The product is a personal batch tool that runs once daily on one machine.
- The rewrite starts without migration of legacy events, caches, indices,
  decisions, or run history.
- The initial discovery scope is the ten-source English-language visual-arts
  profile.
- MediaWiki supplies existing-biography checks and Brave supplies broad
  coverage search, both behind application-owned interfaces.
- OpenRouter is the only LLM gateway. One typed gateway keeps model assignment
  configurable while eliminating subprocess parsing and duplicated schema,
  retry, credential, and provenance behavior. Codex CLI, Claude CLI, and
  direct OpenAI backends will be removed.
- SQLite is the durable source of truth in a Python modular monolith with one
  installed CLI.
- Task model assignments, request parameters, budgets, thresholds,
  concurrency, source profiles, and digest size are configurable rather than
  architectural constants.
- The workflow optimizes for recall and produces an inspectable shortlist for
  human judgment.
- The application never writes or edits Wikipedia content.

A locked constraint may be revisited only when the design work reveals a
specific contradiction, safety problem, or inability to satisfy the product
purpose. Any revision must be explicit and documented.

## Session Method

Each focused design session follows the same sequence:

1. Inspect the relevant legacy tests, prompts, documentation, and
   implementation.
2. Translate implementation evidence into observable product behaviors,
   consolidating tests that cover the same behavior.
3. Resolve product questions one at a time, with a recommendation and explicit
   trade-offs.
4. Present two or three viable approaches for material design choices.
5. Present the proposed design in small sections and obtain approval for each.
6. Write and self-review the approved specification or inventory section.
7. Commit the documents logically and ask for review of the committed version.

No implementation or implementation planning begins during these sessions.
All core design sessions will be completed before implementation starts because
they share foundational concepts such as person identity, evidence, run state,
and failure outcomes.

## Legacy Behavior Review

The legacy review is the first dedicated session. It produces the index
`docs/architecture/legacy-behavior-inventory.md` and one file per capability
under `docs/architecture/legacy-behaviors/`.

The inventory is organized by observable behavior rather than by individual
test. Each row may cite several tests, prompts, documents, or implementation
locations. Every meaningful behavior receives one of these dispositions:

- **Preserve:** retain the observable behavior as a product, correctness, or
  safety requirement.
- **Change:** retain its intent but define different observable semantics.
- **Delete:** remove obsolete functionality or accidental implementation
  behavior.
- **Investigate:** hold the behavior open while a product decision is needed.

`Investigate` is temporary. Every investigated item must become `preserve`,
`change`, or `delete` before the design specification that depends on it is
finalized.

Each preserved or changed behavior will identify its replacement verification
method. The normal choices are pytest for deterministic application behavior,
Promptfoo for model-assisted semantic behavior, and a concise manual acceptance
check where automation would not be proportionate.

The review covers these capabilities:

1. discovery and feed ingestion;
2. person detection and initial triage;
3. Wikipedia identity matching;
4. coverage discovery and source reliability;
5. lead assessment and ranking;
6. digest generation;
7. failures, retries, budgets, and resumability.

## Design Session Sequence

### 1. Legacy behavior inventory

Review all seven legacy capabilities and approve the disposition of each
meaningful behavior. This establishes what the replacement must do without
preserving the old pipeline structure by accident.

**Completed:** [Legacy behavior inventory](../../architecture/legacy-behavior-inventory.md)

### 2. Product workflow and decision policy

Define the candidate lifecycle, stage transitions, human-review outcomes,
evidence thresholds, ranking, resurfacing, and the operational meaning of high
recall.

**Completed:** [Product workflow and decision policy](2026-07-24-product-workflow-design.md)

### 3. Domain model, persistence, and resumability

Define people, identities, source items, evidence, publisher domains,
decisions, attempts, runs, caches, SQLite ownership, transactions, idempotency,
retention, interruption, and continuation.

**Completed:** [Domain model, persistence, and continuation](2026-07-24-domain-persistence-design.md)

### 4. LLM tasks and evaluation

Define the bounded judgments that use models, their typed inputs and outputs,
uncertainty behavior, configurable task model assignment and budgets,
prompt versioning, and lean Promptfoo datasets.

**Completed:** [LLM tasks and evaluation](2026-07-24-llm-evaluation-design.md)

### 5. Provider adapter contracts

Define the RSS, MediaWiki, Brave, and OpenRouter boundaries, including
normalization, caching, rate limits, retries, access constraints, error
translation, and contract fixtures.

**Completed:** [Provider adapter contracts](2026-07-24-provider-adapters-design.md)

### 6. Operator experience and verification

Define configuration and precedence, CLI commands, daily execution and
single-machine locking, logs and run summaries, digest presentation, error
reporting, pytest layers, the lightweight legacy comparison, and operational
acceptance criteria.

**Completed:** [Operator experience and verification](2026-07-24-operator-experience-verification-design.md)

### 7. Overarching system specification

Reconcile the six approved designs into one component map and end-to-end data
flow. Identify authoritative ownership and cross-cutting invariants, and link to
the focused specifications instead of restating them.

**Completed:** [Overarching system specification](2026-07-24-overarching-system-design.md)

Each session depends on the approved outputs above it. If later work exposes a
conflict, the earlier document will be amended explicitly rather than silently
overridden.

## Completion Criteria

The initial design is complete only when:

- every meaningful legacy behavior has a final disposition;
- every preserved or changed behavior maps to an acceptance criterion and a
  verification method;
- the legacy behavior inventory and all five focused specifications are
  approved and internally consistent;
- the overarching specification describes one complete once-daily visual-arts
  workflow;
- configuration covers task models, request parameters, budgets, thresholds, concurrency,
  feeds, and digest size without hard-coding current guesses;
- failure, uncertainty, interruption, and resumption paths are specified as
  fully as the successful path; and
- the design can be divided into independently verifiable implementation
  milestones.

## Scope Boundaries

The initial design excludes:

- multi-user accounts or shared hosting;
- community subscriptions and notification infrastructure;
- autonomous agents, model-selected tools, or open-ended browsing;
- article generation or automated Wikipedia edits;
- generic plugin infrastructure;
- compatibility with legacy JSONL state; and
- speculative features without a current acceptance criterion.

Future possibilities may be noted only where they justify a clean interface in
the initial implementation. They do not add behavior to the first release.

## Replacement Boundary

The prototype remains recoverable in Git and is the operational fallback until
the rewrite passes its approved offline verification, manual live checks, and
one-time legacy comparison. That comparison is the go/no-go review for
replacing the prototype, not a permanent compatibility target. The rewrite
starts with new state, so fallback means running the preserved prototype
rather than downgrading or translating the new SQLite database.

## Integration and Cutover Policy

`refactor/rearchitecture` is the integration branch for the rewrite. Focused
implementation branches start from it and return to it after their own review
and verification; they do not merge independently into `main`. Once a focused
branch has been integrated, its worktree and local branch may be removed.

Legacy removal is a product cutover step, not ordinary branch cleanup. The
prototype runtime, tests, configuration, and entry points remain present until
the replacement passes the approved offline verification, manual live checks,
and one-time legacy comparison. Their removal is then made as an explicit,
reviewed commit on the integration branch. That removal must not delete
operator data or the historical design and behavior documentation.

Only the complete, verified `refactor/rearchitecture` branch merges into
`main`. Merged local worktrees and branches are cleaned up afterward; remote
branch deletion requires explicit operator approval.

## Transition to Implementation

After the legacy behavior inventory, five focused specifications, and
overarching system specification are approved, the work will transition to
detailed implementation planning. The plans will be derived from the approved
acceptance criteria and divided into independently verifiable milestones. No
legacy test will be copied into the new suite solely because it existed in the
old repository.
