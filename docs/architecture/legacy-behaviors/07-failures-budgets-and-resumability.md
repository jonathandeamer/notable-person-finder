# Legacy Behavior Review: Failures, Budgets, and Resumability

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability makes a once-daily local batch predictable when external
services, models, individual records, or the process itself fail. Application
code owns retries, budgets, concurrency, durable transitions, and run status.
Models never decide operational control flow.

Primary legacy evidence:

- orchestration, locks, stage results, retry passes, and summaries in
  `run_pipeline.py`
- retry and append/resume behavior in `scripts/llm_gate1_runner.py`,
  `scripts/llm_gate3_runner.py`, and `scripts/llm_gate4b_runner.py`
- collision and last-write-wins behavior across deterministic scripts
- retry, parse-failure, collision, and failed-manifest assertions in `tests/`
- operational direction already approved in
  `docs/architecture/clean-slate-rewrite-notes.md`

## Run States

Every started run has one current state and an immutable transition history:

- **`complete`:** every planned required item reached a valid semantic or
  policy terminal outcome. Genuine semantic uncertainty is allowed.
- **`partial`:** useful work completed, but required work remains unevaluated
  because of an exhausted provider failure, budget deferral, or unsafe
  truncation.
- **`failed`:** a run-level problem such as invalid configuration, unavailable
  storage, migration failure, lock conflict, or inability to perform or report
  meaningful work prevented a usable run.
- **`interrupted`:** the process ended before assigning another terminal run
  state. The next invocation records this for an abandoned `running` row.

One bad feed, article, or model response is isolated to its work item. It does
not fail the batch when other useful work can continue. A valid empty result is
`complete`, not failed.

Optional synthesis and stronger-model escalation are not required for run
completeness. When budget or policy skips them, the existing semantic outcome
and deterministic digest fallback remain valid. Only deferred required
first-pass work makes the run partial.

The CLI returns `0` for complete, `2` for partial, and `1` for failed. A lock
conflict prevents a new usable run and returns `1`. An inaccessible or
paywalled article does not make a run partial when the approved snippet
fallback supplies a valid terminal article view.

## Typed Failures

Provider and application boundaries convert failures into a compact typed
vocabulary such as:

- configuration or unsupported capability;
- authentication or permission;
- rate limit;
- timeout or transient network failure;
- provider unavailable;
- invalid or schema-nonconforming response;
- access denied, paywall, or robots restriction;
- malformed input or extraction failure;
- budget exhausted; and
- storage or internal application failure.

Each failure records its run, task, entity, provider, retryability, attempt,
provider status or code where safe, sanitized message, and timestamp. It never
becomes a semantic negative or fabricated uncertainty. Logs and persisted
messages redact credentials, authorization headers, and configured sensitive
values.

## Retry Policy

Use one application-owned retry policy across provider adapters:

- Retry transient network errors, timeouts, HTTP 408, HTTP 429, HTTP 5xx, and
  invalid structured model output.
- Respect `Retry-After`; otherwise use bounded exponential backoff with jitter.
- Do not retry authentication or configuration errors, ordinary HTTP 4xx,
  paywalls, robots denial, malformed source content, model refusals, or valid
  semantic uncertainty.
- Invalid structured model output receives at most one fresh attempt before an
  explicitly configured fallback or escalation.
- Adapter timeouts and maximum attempts are configurable with conservative
  defaults.
- Every attempt is immutable and separately recorded; a success never
  overwrites its failed predecessor.

Retry bounds apply per run. Exhausted transient work remains eligible for the
next ordinary invocation. Permanent failures wait for changed input or
configuration rather than being retried in a tight or daily loop. There are no
retry-specific CLI modes.

Use a simple run-scoped provider safeguard instead of a persistent circuit
breaker. Authentication, invalid credentials, or incompatible model
configuration stops that provider immediately and normally fails the run.
After a configurable number of consecutive exhausted transient failures,
pause that provider for the rest of the run and defer dependent work. The next
daily invocation starts with a clean provider state.

## OpenRouter Budget

An OpenRouter budget is optional and expressed only as a decimal USD per-run
cap. There is no default cap, exchange-rate lookup, or multi-currency feature.
Model names, routing, request parameters, task limits, and the budget remain
configuration rather than application constants.

Resolve current model pricing through OpenRouter metadata and stamp the exact
price information and retrieval time into the run. Before a call, reserve a
conservative maximum cost from known input size, configured output bound, and
resolved prices. Reconcile the reservation against provider-reported actual
usage and cost. Concurrent scheduling cannot reserve the same remaining budget
twice. If the application cannot establish a safe upper bound while a hard cap
is enabled, it does not make the call.

Budgeted work order is:

1. required first-pass semantic judgments for pending items, oldest first
   within ordinary workflow order;
2. boundary-critical optional escalations;
3. optional shortlist synthesis; and
4. non-critical retries or refinements.

Required work that cannot fit is recorded as `not_evaluated_budget` and remains
pending for a later run. Budget exhaustion does not silently substitute a
cheaper model, truncate required context, or invent a semantic outcome.
Fallback models run only when explicitly configured. Skipped optional work is
reported separately and does not make the run partial.

Brave search uses the configurable query, result, page, and request bounds
approved in capability 4. Version one does not add a general cross-provider
currency ledger.

## Durable Continuation

There is no special resume mode. Every CLI invocation creates a new run. At
startup, an abandoned `running` record from a crashed process becomes
`interrupted`, then the workflow queries durable state for unfinished work.
The new run naturally continues pending items.

Each item transition commits transactionally. Successfully persisted external
work is reused when the normalized input and relevant provider, policy,
schema, prompt, and model versions match. A crash after the remote provider
accepts a request but before SQLite stores the result can still repeat paid
work unless that provider supports a usable idempotency key; exactly-once
execution across those systems is not promised. Changed inputs or versions
create new observations rather than mutating history.

One real operating-system file lock prevents concurrent local runs and is
released automatically when the process exits or crashes. Do not use a stale
sentinel file requiring manual deletion, distributed leases, or multi-worker
coordination. Configurable within-run concurrency remains bounded by
application-owned semaphores and budget reservation.

Delete `--from-gate`, retry-parse-failure modes, overwrite flags, file
collision errors, and stage-specific resume mechanics. The database and
workflow state determine what remains to do.

There is no whole-run wall-clock limit in version one. Finite queues,
configurable work and request bounds, per-call timeouts, bounded retries,
provider pausing, and an optional cost cap already prevent unbounded agentic
behavior without introducing another deferral path.

## Storage Safety

SQLite transactions and foreign keys protect state transitions. Schema
migrations are explicit and transactional, with a recoverable backup created
immediately before migration. A database, integrity, or schema error fails
loudly. The application never skips malformed durable state, silently creates
a replacement database, or attempts automatic corruption repair.

Version one has no scheduled backup service, replication, distributed
database, or general override subsystem. Process interruption must not swallow
normal cancellation and shutdown signals; already committed work remains
valid, and the next run records the interrupted predecessor.

## Approved Dispositions

| Observable behavior | Decision and rationale | Replacement verification |
| --- | --- | --- |
| A lock-file sentinel prevents concurrent runs and may require manual deletion after a crash. | **Change.** Use an OS-managed lock automatically released with the process. | Concurrent-run integration test |
| Output-file collisions stop stages unless files are manually removed or overwritten. | **Delete.** Transactional durable state and idempotent work identities replace transport-file collisions. | Repeated-run tests |
| `--from-gate` and stage-specific flags resume a run. | **Delete.** Each invocation starts a run and naturally continues durable unfinished work. | Crash-and-next-run test |
| Append-only JSONL plus last-write-wins reconstructs retry state. | **Delete.** Store immutable attempts and explicit current transitions relationally. | Persistence and ordering tests |
| Malformed JSONL rows may be skipped. | **Delete.** Invalid durable state fails loudly rather than becoming silent data loss. | Corruption-path test |
| Whole runner commands are retried regardless of failure type. | **Change.** Retry only typed transient and structured-output failures under one bounded policy. | Retry matrix tests |
| Parse failures trigger a later retry pass over output files. | **Change.** Validate at the model boundary and retry the individual attempt at most once. | Schema-retry tests |
| Provider or parse failure becomes semantic `UNCERTAIN`. | **Delete.** Preserve typed operational failure separately from domain uncertainty. | Failure-transition tests |
| One item failure can stop a stage or be merely printed as a warning. | **Change.** Isolate item failures, persist them, and derive explicit complete, partial, or failed run state. | Mixed-result run tests |
| A numeric Gate 1 event count acts as the budget. | **Delete.** Work bounds and optional OpenRouter USD cost cap are separate configurable controls. | Budget configuration tests |
| Budget exhaustion drops remaining items. | **Delete.** Required work receives `not_evaluated_budget` and remains pending. | Multi-run budget tests |
| Models and backends are embedded in runner flags. | **Delete.** OpenRouter-only logical task policies own configurable model routing; no CLI or direct-provider backend remains. | Configuration validation tests |
| Retrying a crashed run repeats already persisted paid work. | **Delete.** Reuse matching successful attempts through versioned idempotency identity; document the unavoidable post-provider/pre-persistence window. | Crash-boundary cost test |
| Any failed run can leave the prior successful digest looking current. | **Delete.** Reporting exposes the latest attempt and explicit run state. | Failed-report test |
| The application needs an overall elapsed-time kill switch. | **Delete for version one.** Existing finite bounds are sufficient; add one only if observed runs justify it. | Absence from initial configuration schema |
| Database damage can be bypassed by starting fresh automatically. | **Delete.** Fail loudly and preserve the damaged database for manual recovery. | Storage-failure test |

## Verification Boundary

Pytest covers the retryability matrix, `Retry-After`, bounded backoff without
real sleeping, structured-output retry, provider pausing, complete/partial/
failed states, exit codes, item isolation, lock contention, crash continuation,
idempotent paid-call reuse, prospective versions, budget reservation and
reconciliation, required versus optional deferral, transactional migrations,
pre-migration backup, redaction, and storage failure. A small offline
end-to-end fixture interrupts the workflow after a committed provider result
and verifies that the next run continues without repeating that call.
