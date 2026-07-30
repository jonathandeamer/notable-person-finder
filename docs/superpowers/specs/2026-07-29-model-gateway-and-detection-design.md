# Model Gateway and Person Detection Design

**Status:** Approved for implementation planning
**Date:** 2026-07-29
**Branch:** `feat/model-gateway-detection`

## Purpose and Scope

This document defines milestone 3b1 of the clean-slate rewrite: the shared
OpenRouter gateway and bounded person detection over durable ingestion records.
It turns every untriaged usable source item into one immutable triage
observation and zero or more unresolved, source-grounded person mentions.

The milestone deliberately stops before durable person identity. It creates no
`person` rows, performs no entity resolution, creates no person relations, and
merges no evidence. Milestone 3b2 will resolve the unresolved mentions produced
here into durable people and will own `possible_same_person` and confirmed
merges.

This design refines, without replacing, the approved overarching, product,
domain, LLM, provider, and operator specifications. Those documents remain
authoritative for system-wide policy. Where the foundation's original
"Ingestion and people" milestone grouped the first model gateway with the full
people identity capability, this design records the approved split into two
independently reviewable milestones.

## Goals

Milestone 3b1 must:

- add the application-owned `LlmClient` boundary and one run-scoped OpenRouter
  implementation;
- inspect each needed configured model before its first paid generation in a
  run;
- execute the exact approved `detect_people` task over one source item at a
  time;
- persist immutable triage observations, unresolved mentions, identity facts,
  and grounded signals;
- backfill all existing untriaged source items and schedule newly ingested
  source items idempotently;
- preserve attempt, budget, retry, redaction, and crash-boundary invariants
  from the run engine, and extend the engine only where the pinned interfaces
  below require it (prepare-returned reservation, dual worker pools, and
  schedule-time no-call terminal dispositions); and
- expose truthful triage progress, backlog, failure, and cost information to
  the operator.

It does not add Promptfoo suites, entity resolution, MediaWiki, Brave, article
assessment, lead ranking, digest queuing, article drafting, or Wikipedia edits.

## Component Ownership

### `people`

A new `people/` package owns:

- source-item detection input construction and passage numbering;
- the strict Pydantic input and output models for `detect_people`;
- the reviewed production system prompt and its content hash;
- task schema versioning and canonical request fingerprints;
- domain validation of exact names, bounds, supplied identifiers, evidence
  references, and source-grounded facts and signals;
- triage observations, unresolved mentions, identity facts, and signal SQL;
- backfill and downstream scheduling policy; and
- the task handlers that divide application-thread preparation/persistence
  from worker-thread external execution.

The package depends on an application-owned `LlmClient` protocol. It never
imports OpenRouter SDK or HTTPX types.

### `providers`

`providers/openrouter.py` owns a run-scoped implementation of two operations:

- `inspect_model(model_id)` retrieves current capability and pricing facts;
- `generate_structured(request)` performs one strict structured generation.

The adapter translates SDK values and failures into application-owned typed
DTOs and `ProviderFailure`. It disables SDK retries, applies the configured
LLM timeout and routing requirements, and retains the safe provider metadata
needed for attempt provenance. It does not render task prompts, understand
person detection, validate domain references, decide retries, reserve budgets,
or repair malformed JSON.

### `runs`, `config`, `cli`, and `reporting`

The run engine continues to own claims, persisted attempts, retry decisions,
provider pauses, budget reservation and reconciliation, concurrency, and work
settlement. Configuration owns model assignment, parameters, bounds, routing,
timeouts, concurrency, budget policy, and secret availability. The CLI owns
the run-scoped adapter lifecycle and wiring only. Reporting renders durable
counts and safe failure summaries; it does not infer semantic state from logs
or model text.

Milestone 3b1 requires three small, named engine/CLI extensions rather than
bypassing those owners:

- **Prepare-returned reservation.** Today's `TaskHandler.reserved_nano_usd` is
  a fixed integer. Generation reservation must use live inspection pricing
  times configured token bounds, so `prepare` returns (or carries on its
  prepared value) the integer nano-USD to reserve for that attempt. The engine
  reads that amount at `start_attempt` instead of the frozen handler field.
  Handlers that still use a fixed ceiling may keep the field as a default when
  prepare does not supply an override.
- **Dual worker pools.** Configuration already separates `http_workers` and
  `llm_workers`. The CLI today builds one `BoundedScheduler(http_workers)`.
  3b1 introduces two pools: ordinary HTTP work (feeds, and later MediaWiki or
  Brave) submits to the HTTP pool; OpenRouter inspection and generation submit
  to the LLM pool. Claim batch sizes must not exceed the target pool. The same
  pool-aware claim routing evaluates each handler's application-thread
  readiness predicate before including its task type in `claim_batch`;
  `detect_people` is ready only after this run has persisted its exact model
  inspection. This prevents a claimed item from needing a no-call dependency
  settlement and keeps readiness out of worker threads.
- **Schedule-time no-call terminals.** Empty title and summary never enter the
  generation path as work that must invent a zero-byte attempt. See
  Scheduling and Data Flow.

## Detection Contract

The production task is exactly `detect_people`. Its input contains one source
item and no article body or unrelated person's context:

- source-item and feed identifiers;
- configured publisher label;
- nullable normalized title and summary represented as numbered passages;
- publication-date and URL metadata;
- view and truncation metadata;
- active domain-profile examples and version; and
- the configured maximum returned people.

Its strict output contains:

- item outcome `research_people`, `do_not_research`, or `uncertain`;
- zero to the configured maximum mentions;
- exact source-written public name for each mention;
- mention outcome `research`, `do_not_research`, or `uncertain`;
- supporting passage identifiers;
- source-grounded identity facts;
- source-grounded attention and caution signals;
- concise mention and item rationales; and
- an `overflow` boolean.

Code rejects unknown fields, unseen identifiers, unsupported passage
references, names not grounded in supplied text, inconsistent item/mention
outcomes, excessive mentions, and invalid signal references before domain state
changes. It performs no brace extraction, JSON repair, permissive parsing, or
different-model fallback. Valid semantic uncertainty is successful output and
does not trigger another call.

## Triage and Mention Persistence

Migration 0004 introduces immutable records for:

- OpenRouter model-inspection observations tied to run attempts;
- source-item triage observations;
- unresolved person mentions tied to their originating observation;
- source-grounded identity facts tied to mentions; and
- grounded attention and caution signals tied to mentions.

A triage observation distinguishes its processing disposition from the model's
nullable semantic outcome:

- `completed` has a validated model outcome, including a valid zero-person
  result;
- `insufficient_input` is a deterministic terminal observation for an item
  whose normalized title and summary are both empty; it is written at schedule
  time (backfill and post-ingestion), creates no work item, makes no model
  attempt, reserves no budget, and is not represented as `do_not_research`;
- `failed` records a typed permanent failure when no semantic observation can
  be stored.

Transient exhaustion leaves the work item deferred and does not manufacture a
triage observation. Attempt history remains the authority until later work
either succeeds or fails permanently.

Every stored mention preserves the exact source-written name. A mechanical
search form may remove only text-grounded presentation differences such as an
honorific; it never expands initials, invents a legal name, or uses model
memory. Neither exact nor normalized names are unique across mentions. Mentions
remain unresolved in this milestone and contain no `person_id`.

Repeated successful execution with the same material fingerprint reuses the
existing observation. Historical observations remain immutable when a changed
prompt, schema, model, material source input, or domain-profile version
legitimately schedules a replacement. Current-selection mechanics, if needed
for efficient status queries, update atomically with the replacement without
rewriting history.

## Scheduling and Data Flow

At run start, code walks every source item without a reusable triage
observation (including rows ingested before milestone 3b1 was installed). For
each such item:

- if both normalized title and summary are empty, write a durable
  `insufficient_input` triage observation and **do not** create a
  `detect_people` work item;
- otherwise schedule required `detect_people` work idempotently.

Successful ingestion applies the same branch when committing new source items.
The ordinary bounded queue, configured priorities, and budget determine how
quickly a generation backlog clears; records are never silently omitted to
make a run appear complete. Because empty-input terminals never become work,
they do not need a fake attempt row or a new "settle without attempt" engine
path.

Each generation work item follows the existing engine phases (claim work →
prepare → start_attempt/reserve → execute → finish_attempt →
complete_work/persist):

1. **Prepare (application thread, may read SQLite).** Build bounded source
   context and determine the configured model, prompt, schema, parameters,
   routing policy, and material fingerprint. Read the run-scoped successful
   model-inspection observation for that exact model and routing requirements
   (claim eligibility already requires it; prepare fails permanent if it is
   unexpectedly absent so a generation attempt is never started without
   preflight). When a hard OpenRouter budget is enabled, compute the
   worst-case integer nano-USD reservation from the inspection's usable unit
   prices and the configured input/output token bounds, and return that amount
   on the prepared value. Without a hard budget, reserve `0`.
2. **Start attempt (application thread, brief transaction).** The engine
   inserts the attempt and reserves the prepare-returned nano-USD amount (not
   a frozen handler constant). Budget refusal defers without a provider call,
   as today.
3. **Execute (LLM worker thread).** Perform exactly one SDK generation
   without SQLite, sleeps, client retries, or domain persistence. On the same
   worker, against the prepared context only, validate strict structured
   output and the full domain contract (passage references, grounded names,
   bounds, outcome consistency, signal references). Schema or domain
   validation failure is raised as
   `ProviderFailure(category=MALFORMED_RESPONSE, retryable=True, …)` so the
   existing central `malformed_retries` ceiling can grant at most one fresh
   attempt. The default category table does not treat `MALFORMED_RESPONSE` as
   retryable unless the failure sets `retryable=True`; LLM validation must
   set that flag. Valid semantic `uncertain` is success and does not raise.
4. **Persist and settle (application thread).** Reconcile actual cost against
   the reservation, record raw and validated provenance, and atomically write
   the triage observation and its unresolved mentions. `persist` receives only
   outcomes that already passed domain validation in `execute`; it must not
   re-open a second paid attempt. A successful 3b1 observation creates no
   identity work until milestone 3b2 exists.

The preflight itself is attributable required work with persisted attempts,
scheduled at higher priority than dependent generation for each configured
model needed in the run. A fresh successful inspection may be shared within
one run by every generation using the exact configured model and routing
requirements; claim eligibility for those generations requires that
observation, so waiting dependents simply remain pending rather than minting
no-call attempt rows. Transient inspect exhaustion leaves dependents pending
or deferred with the model; permanent inspect failure (auth, unsupported
strict output, incompatible routing) settles dependents permanently without a
generation attempt through the inspection handler's application-thread
settlement callback. Inspection is not reused as fresh capability evidence in
a later run.

No SQLite transaction spans model work, and no worker thread accesses SQLite.
The documented at-least-once remote-success/local-crash boundary remains in
force.

## Capability Preflight and Budgeting

Before a model's first paid generation in a run, `inspect_model` verifies that
the exact configured model currently supports strict structured output under
the configured provider-routing and privacy requirements. The observation
records current capability metadata and available pricing.

With a hard OpenRouter budget, fresh usable pricing is required so prepare can
calculate a worst-case integer nano-USD reservation from configured
input/output token bounds and the inspection's unit prices. That amount is
returned from prepare and consumed by the engine at `start_attempt`. Without a
hard budget, absent price metadata alone does not block a compatible model
call, and prepare returns a zero reservation. Reservations always occur before
submission and reconcile against the provider's reported actual cost and usage
afterward. Missing actual cost stays explicitly unknown rather than becoming
zero.

The first implementation supports only the cost dimensions required for the
configured model under the approved OpenRouter contract: prompt and completion
unit prices (as exposed by the locked SDK/provider metadata). A model whose
pricing cannot be bounded under an enabled hard cap fails local or preflight
validation rather than bypassing the budget; prepare must not invent prices.

## Concurrency

Configuration already defines independent `concurrency.http_workers` (default
4) and `concurrency.llm_workers` (default 2). Milestone 3b1 must honour both
when feed ingestion and model work share a run.

The CLI constructs two `BoundedScheduler` instances (or an equivalent pair of
worker pools): one sized to `http_workers` for ordinary HTTP providers, and one
sized to `llm_workers` for OpenRouter inspection and generation. Handlers
declare which pool they use (by provider class or an explicit pool key). The
engine claim loop never submits more work to a pool than that pool's
`max_workers`, and never runs OpenRouter generation on the HTTP pool or feed
fetches on the LLM pool.

A single shared pool sized to either figure is not acceptable: sizing to
`http_workers` can oversubscribe long-running LLM generations; sizing to
`llm_workers` underuses HTTP. Per-origin pacing and the existing mutation lock
are unchanged.

## Failure Semantics

Provider and SDK failures translate into the established safe failure
categories. The central retry policy alone starts another visible attempt.
Generation and inspection client retries are disabled.

- transient preflight exhaustion leaves dependent generations unclaimed or
  deferred with that model and starts no generation attempts;
- authentication, invalid configuration, unsupported strict output, or
  incompatible routing fails dependent work permanently without a generation
  attempt for that material input;
- transient generation failures follow central retry and defer when exhausted;
- invalid structured output or invalid domain references are raised from
  **execute** as `ProviderFailure(MALFORMED_RESPONSE, retryable=True)` and
  permit at most one fresh attempt using the same configured model, subject to
  the existing `malformed_retries` ceiling and `max_attempts` outer bound;
- a second invalid result fails permanently and retains both attempts;
- valid `uncertain` output succeeds immediately and does not count as
  malformed;
- validation must not run for the first time in `persist`: a successful
  attempt that only fails domain checks after the call would be unable to
  re-arm another generation through the central coordinator; and
- budget exhaustion defers required generation without making a provider call.

One source item's failure commits independently and cannot roll back another's
triage. A provider failure, missing result, budget deferral, invalid output, or
truncation never becomes `do_not_research` or another confident semantic
negative. Schedule-time `insufficient_input` is not a failure path.

## Configuration and Secrets

Strict file-first configuration adds:

- an OpenRouter endpoint and environment-variable name for the API key;
- timeout, generation-concurrency, and safe routing/privacy requirements;
- one preferred model assignment for `detect_people`;
- generation parameters and configured input/output token bounds;
- maximum mentions and context bounds;
- prompt and schema version participation in material fingerprints; and
- optional hard run budget expressed through the existing decimal-USD to
  integer-nano-USD boundary.

`OPENROUTER_API_KEY` remains environment-only. An adjacent ignored `.env` may
fill it only when the process environment lacks it. Resolved snapshots record
secret availability, never the value. Logs, terminal output, exceptions,
digests, fixtures, attempts, prompts, and test snapshots must not contain the
key or authorization headers.

Local `notable config validate` checks the complete graph and secret presence
without a network request. Current capability and pricing inspection remains
lazy run work immediately before the first generation that needs the model.

## Operator Surface

`notable run` constructs one run-scoped OpenRouter client only when model work
is registered, closes it on every normal and exceptional path after both
scheduler pools drain, registers inspection and detection handlers (LLM pool)
alongside feed handlers (HTTP pool), and applies the schedule-time triage
branch for the untriaged backlog.

The digest operational summary gains:

- source items triaged;
- research, uncertain, and do-not-research outcomes;
- unresolved research or uncertain mentions;
- overflow observations;
- insufficient-input observations;
- model work deferred or permanently failed by safe category; and
- configured, reserved, and actual OpenRouter cost when available.

`notable status` gains durable triage totals and the count of required
untriaged source items. It does not add queue tiers, person identity, ranking,
or shortlist synthesis. The digest shortlist remains the existing placeholder.

Complete detection results may appear in a partial run. Exit status remains
`0` for complete, `2` for partial, and `1` for failed according to durable run
state.

## Verification

Default verification is offline and includes:

- migration tests from every retained rewrite schema version;
- model and configuration validation, fingerprint, snapshot, and redaction
  tests;
- OpenRouter adapter contract tests over fake SDK clients;
- capability, pricing, routing, timeout, exception translation, and
  retry-disable tests;
- strict input/output schema and evidence-reference validation tests;
- persistence constraints, replacement observations, and namesake tests;
- backfill, new-ingestion scheduling, active-work deduplication, and restart
  continuation tests;
- budget reservation (including prepare-returned dynamic amounts),
  unknown-cost, reconciliation, and exhaustion tests;
- dual-pool concurrency tests proving HTTP and LLM limits are enforced
  independently;
- schedule-time `insufficient_input` tests proving no work item and no
  attempt row;
- worker-thread SQLite, transaction-boundary, scheduler-lifecycle, and
  at-least-once seam tests;
- CLI, digest, status, partial-run, interruption, and failure-isolation tests;
  and
- mutation checks proving that preflight cannot be bypassed, invalid
  references cannot persist, SDK retries remain disabled, and secrets cannot
  leak.

An opt-in live smoke performs one capability inspection and one minimal strict
structured generation when `OPENROUTER_API_KEY` is available. It is deselected
from ordinary pytest and never uses operator state. Promptfoo semantic suites
remain milestone 7, but later evaluation must import the exact production
prompt and generated schema introduced here.

The milestone completion gate is:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people
```

Manual recorded evidence additionally includes the opt-in OpenRouter smoke, an
installed fixture-backed CLI run on a fresh temporary data root, a
binary-inclusive scan proving configured synthetic secrets are absent from the
populated root, every named seam mutation, `git diff --check`, and a clean
feature worktree.

## Acceptance Criteria

Milestone 3b1 is complete when:

- every existing and newly ingested untriaged usable source item becomes
  idempotently eligible for detection;
- an empty title and summary produce `insufficient_input` at schedule time
  with no work item, no attempt, and no paid call;
- one item can yield zero, one, or several independently traceable mentions;
- research-worthy mononyms and professional names can be retained without
  invented expansions;
- passing mentions can be excluded while semantic uncertainty remains visible;
- unresolved mentions contain no durable person identity and names are not
  unique across mentions;
- every paid generation follows a fresh compatible preflight and a successful
  prepare-returned reservation when a hard budget is enabled;
- one model request contains exactly one source item and only supplied
  evidence;
- unseen references and invalid structured output fail in execute before
  domain writes, permit at most one central malformed retry, and cannot change
  domain state;
- valid uncertainty never retries, while technical failure never becomes a
  semantic outcome;
- no client retry bypasses persisted central attempt accounting;
- HTTP and LLM concurrency limits are enforced on separate pools;
- no transaction spans model work and no worker touches SQLite;
- one item's failure cannot prevent unrelated triage or a truthful digest;
- secrets remain absent from snapshots, diagnostics, logs, digests, database
  records, and tests; and
- no workflow path creates durable people, performs identity resolution, or
  writes or edits Wikipedia.

## Deferred to Milestone 3b2 and Later

Milestone 3b2 owns durable people, sourced names, bounded existing-person
candidate retrieval, `resolve_person_entity`, entity-resolution observations,
new-person creation, `possible_same_person`, reconsideration, confirmed merges,
and canonical work reconciliation.

MediaWiki identity remains milestone 4; coverage research remains milestone 5;
lead assessment, ranking, digest queue, and audit commands remain milestone 6;
Promptfoo suites, full-system live verification, legacy comparison, and
cutover remain milestone 7.
