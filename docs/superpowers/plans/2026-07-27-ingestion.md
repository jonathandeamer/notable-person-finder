# Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Track progress in a separate ledger, not in this file.

**Goal:** Turn configured RSS and Atom feeds into durable, deduplicated source items and canonical articles, with nothing yet interpreting them.

**Architecture:** This is milestone 3a of the seven-milestone programme — the first half of the foundation plan's "Ingestion and people" milestone, split so that the first real provider adapter and the first model gateway do not debut together. A new `ingestion/` package owns URL identity, the ingestion schema, and the `fetch_feed` handler; a new `providers/feeds.py` supplies the `FeedClient` protocol and its feedparser adapter over milestone 2's run-scoped transport. The run engine is reshaped from a one-item-at-a-time loop into a batch-claim / execute / settle state machine over the existing `BoundedScheduler`, keeping SQLite exclusively on the application thread. Several milestone 2 review findings that this milestone's own code depends on are fixed here rather than deferred.

**Tech Stack:** Python 3.13, `feedparser` (new), HTTPX (existing transport), standard-library `sqlite3`, Pydantic at the configuration boundary, pytest, Ruff, Pyright, uv.

## Global Constraints

- Python 3.13 or newer; dependencies installed with `uv sync --frozen`.
- Distribution `notable-person-finder`; import package `notable_person_finder`; executable `notable`.
- The application never edits Wikipedia and never publishes anything automatically.
- Secrets never appear in snapshots, fingerprints, diagnostics, terminal output, digests, the database, or tests.
- Every external network call maps to exactly one persisted attempt attributed to its run and work item.
- Only the central retry policy starts a repeat request.
- No SQLite transaction is held across its own item's network call, and no worker thread touches SQLite. Sibling network calls may remain in flight while the application thread settles another item's short, local transaction.
- Migrations are forward-only, checksummed, transactional, and backed up before changing an existing database.
- Money is integer nano-USD; durations are integer milliseconds; UTC timestamps are ISO-8601 text ending in `Z`; booleans and enumerations carry `CHECK` constraints.
- Every bound is configuration, not a Python constant, and must fail validation before a run is created.
- Default verification is offline. Live tests are opt-in and deselected by default.
- Do not import, run, reorganize, or repair `run_pipeline.py`, `scripts/`, JSONL state, or `tests/test_*.py`. The prototype suite is not a gate for this milestone.
- Keep this plan under ~1,500 lines. Reference code blocks are illustrative; shipped code and its tests govern.
- A test earns its place by discriminating, and the `expect failures` steps below do not demonstrate that — before its module exists a test fails with `ImportError`, which proves only that it runs. For every behaviour a task brief names: mutate the rule in the source, confirm a **specific named** test fails, restore, and record which mutation killed which test. A negative assertion (`X not in output`) additionally needs a positive control proving `X` is reachable at all. If source is written before its tests for any reason, this evidence is required for every named rule rather than a sample.

---

## Design Decisions

These decisions were settled during brainstorming and refine, but do not contradict, the approved designs. Where a decision resolves something the approved specs left open, that is noted.

### D1. The milestone is split

The foundation plan's milestone 3 covers "feed adapter, source items, person detection, entity resolution, and durable people". Person detection (`detect_people`) and entity resolution (`resolve_person_entity`) are both model tasks, so as written milestone 3 would debut the first real HTTP adapter *and* the OpenRouter gateway, prompt versioning, structured output, and budget reservation amounts in one plan.

This plan is **milestone 3a: Ingestion**. Milestone 3b: People carries `LlmClient`, `detect_people`, triage observations, person mentions, people, sourced names, entity resolution, `possible_same_person`, and merges.

A 3a source item therefore carries **no triage observation**. The domain spec's "every new source item receives an immutable triage observation" is satisfied in 3b; it is not violated in 3a, because no run in 3a can produce one.

A 3a run registers only `fetch_feed`, so it creates no *downstream* pending work. It does **not** follow that every 3a run is `complete`: seeded `fetch_feed` items are required, and `derive_run_state` returns `partial` whenever `required_deferred > 0`, or when required work fails permanently alongside some successes. Task 9's outcome mapping makes that routine — an oversized response or an exhausted transient failure defers, while a repeated malformed response at the shipped default attempt budget is foreclosed permanently as described in Task 8. A run in which one feed fails and the rest ingest is a `partial` run, and Task 11's tests must assert that state and its exit code rather than assuming `complete`.

### D2. The engine gains a batch-claim state machine

Milestone 2's review recorded that `RunEngine` and the `BoundedScheduler` it holds cannot compose: `scheduler.py` documents "a worker thread never touches SQLite", while `engine._perform` interleaves attempt persistence with the provider call, and `engine.execute` pulls one eligible item at a time. Calling `claim_and_start_attempt` from a worker raises `sqlite3.ProgrammingError`, which the scheduler swallows into `Completion.error`.

Ingestion alone would not force this (roughly ten feeds), but 3b issues one `detect_people` call per source item at a 300-second timeout, where sequential execution is unusable. Resolving it here — against a deterministic, cheap, fully fakeable handler — is materially safer than resolving it beside the LLM gateway.

The shape is a per-item state machine with SQLite exclusively on the application thread:

1. The application thread commits a batch claim of up to `concurrency.http_workers` eligible items.
2. For each claimed item it checks pause state and runs `prepare`; only an item that will actually be called then gets its attempt row and budget reservation in a separate transaction.
3. It submits **pure** call closures — one attempt each, no database access, no retry loop, no connection captured.
4. It drains completions on the application thread, persists each attempt outcome, and then either settles the item with its handler persistence or re-arms it at its backoff deadline.
5. A re-armed item is resubmitted when due. Other items keep the pool busy meanwhile, so backoff never sleeps the application thread.

The rejected alternative was letting the worker keep its own retry loop and return a list of raw attempt results for the application thread to persist afterwards. It is a much smaller change, but a crash mid-call then leaves no attempt row for a call the provider may already have accepted and charged for. That costs little for feeds and a great deal for OpenRouter in 3b.

### D3. `RetryCoordinator` decomposes into a pure policy

`RetryCoordinator.call` is a blocking loop that owns retry decisions *and* the backoff sleep, so the state machine cannot use it as-is. It decomposes into:

- `RetryPolicy` — pure decision logic plus provider-pause bookkeeping. Given a provider, an attempt history, and a failure, it returns retry-after-a-delay, permanent, or exhausted.
- `RetryCoordinator.call` — retained as a thin sequential wrapper over `RetryPolicy`, so existing callers and tests keep working.

Every adjudicated semantic must survive the decomposition unchanged: the `max_attempts` outer bound, the at-most-one-fresh-attempt ceiling for `malformed_response`, the rule that `malformed_response` never counts toward provider pausing, the `Retry-After` hint capped at `max_backoff_seconds` while the attempt row persists the raw uncapped value, and jitter.

### D4. Ingestion is gated by insert-once, not by the observation window

Milestone 2's `_window_start` is "most recent local midnight". If ingestion filtered entries against it, the first run would ingest almost nothing and a skipped day would permanently lose items.

The unique canonical-URL constraint is the only gate — which the domain spec already calls "the approved insert-once behavior". Every entry a feed currently serves becomes a source item exactly once, ever. The run's observation window stays reporting metadata. Undated and misdated entries ingest normally.

Accepted cost: a first run over ten feeds may yield several hundred source items. Milestone 3b's budget cap and durable work queue meter those out across runs.

**This plan's own decision, not a spec requirement:** an entry is never dropped for a date problem, so undated, unparseable, and implausible dates all ingest with the raw text retained and a typed issue recorded (Task 10). The domain spec only implies date problems are *storable* — it does not say an entry must survive one. The choice follows from high-recall semantics, since a missing date says nothing about whether an entry names a person, but it is an inference and a later design may narrow it.

### D5. Article identity is staged

3a owns `canonicalize_article_url` as the single application-owned URL identity policy, creates `canonical_article` with its unique normalized-URL constraint and provenance aliases, and stores a canonical publisher key derived mechanically from the registrable domain.

Rule matching, rule status, and source-policy fingerprint provenance stay deferred to milestone 5. This lets feed and search discovery converge on one article without milestone 5 retrofitting identity onto existing rows.

### D6. Duplicate-feed discovery is not recorded

Insert-once is implemented as `UNIQUE(canonical_article_id)` on `source_item`, with the article owning URL identity, rather than a second unique URL column. Entries with no usable URL fall back to `UNIQUE(feed_identity_id, source_entry_id)`.

Accepted consequence: if two configured feeds syndicate the same article URL, the second feed's discovery is not recorded anywhere. A `source_item_sighting` child table would capture it. It is deliberately out of scope.

### D7. The live smoke is opt-in with a named manual gate step

Milestone 2's review found that 359 passing offline tests are weak evidence for transport behaviour against a real remote, and recommended a live smoke in the first adapter milestone. Live tests are written behind the existing `live` pytest marker, deselected by default so the standard gate stays offline. The completion gate carries one explicit manual step that runs them and records the output.

### D8. Carried milestone 2 findings fixed here

These are milestone 2 review findings that this milestone's own code depends on, so they are tasks here rather than separate remediation. Riding a feature branch with a full pull request and independent review is *stricter* than CLAUDE.md's remediation path, not looser.

| Finding | Why it must land in 3a |
| --- | --- |
| Engine and scheduler hold mutually exclusive contracts and cannot compose | D2; this milestone performs the reshape |
| `RunReport` budget fields | The `TODO(milestone 3)` in `engine.py`; the digest cannot explain deferral without them |
| The `build_request` guard catches only `UnicodeEncodeError`, so any other exception it raises still escapes `providers/` untranslated | Unreachable in milestone 2, reachable by the first real adapter, which this is |
| A non-settling work state aborts the run without settling the item or writing a digest | The first milestone with a real handler that could return one |
| `actual_nano_usd=0` for zero-cost failed attempts | The transport-wiring decision milestone 2 deferred to the first adapter |
| Two event-name taxonomies (`run.started` vs `run_started`) | Cheap now, entrenched once ingestion adds a third set |

**On attribution.** Milestone 2's review record exists only in the PR #4 body and its progress ledger; no review document was committed and the findings carry no official identifiers. Findings are described here rather than referenced by identifier, because no identifier scheme exists outside this plan. The last two rows above are real code issues observed during planning rather than recorded review findings — the defects are verifiable, the provenance label would not be.

---

## File Structure

**Created**

| Path | Responsibility |
| --- | --- |
| `src/notable_person_finder/ingestion/__init__.py` | Package marker |
| `src/notable_person_finder/ingestion/urls.py` | `canonicalize_article_url`, registrable-domain publisher key. Pure; no I/O, no database |
| `src/notable_person_finder/ingestion/models.py` | Frozen dataclasses and enums for fetch outcomes, entry drafts, and typed entry issues |
| `src/notable_person_finder/ingestion/repository.py` | Feature-owned SQL for feed identities, fetches, articles, aliases, and source items |
| `src/notable_person_finder/ingestion/service.py` | Feed seeding, the `fetch_feed` handler, and result persistence |
| `src/notable_person_finder/providers/feeds.py` | `FeedClient` protocol and the feedparser adapter |
| `src/notable_person_finder/db/migrations/0003_ingestion.sql` | Ingestion schema |
| `tests/ingestion/` | New test area (`__init__.py`, `conftest.py`, `helpers.py`, and the test modules named per task) |
| `tests/ingestion/fixtures/` | Feed payload fixtures |

**Modified**

| Path | Change |
| --- | --- |
| `src/notable_person_finder/runs/retry.py` | Extract `RetryPolicy`; `RetryCoordinator.call` becomes a wrapper |
| `src/notable_person_finder/runs/engine.py` | Batch-claim state machine; seeding hook; `RunReport` budget fields; non-settling state handled |
| `src/notable_person_finder/runs/repository.py` | `claim_batch`; budget columns in `run_counters` or a sibling query |
| `src/notable_person_finder/providers/transport.py` | Widen the `build_request` guard beyond `UnicodeEncodeError` |
| `src/notable_person_finder/reporting/digest.py` | Ingestion summary and budget-aware deferral reasons |
| `src/notable_person_finder/cli/main.py` | Build the feed client, register the handler, pass the seeding hook |
| `src/notable_person_finder/obs/logging.py` (or call sites) | One event-name taxonomy |
| `pyproject.toml` | Pyright includes `tests/run_engine` (Task 0) and `tests/ingestion` (Task 14); `feedparser` dependency; `addopts` deselecting `live` |
| `uv.lock` | Regenerated |
| `CLAUDE.md`, `AGENTS.md` | "What Is Actually Built" and the "Known gaps" list updated in both; repo policy requires them to stay in step |
| `docs/running.md`, `docs/troubleshooting.md` | Ingestion behaviour and failure guidance |

---

## Delivery: Two Pull Requests

Tasks 0–5 are engine and transport surgery with no ingestion dependency, and they touch code every later milestone depends on. The batch-claim state machine (Task 3) is this milestone's real engineering risk. Landing it behind the same review as the ingestion feature work would mean a reviewer judging a foundational concurrency change and a new provider adapter in one sitting — which is how milestone 2's seam defects survived per-task review.

**Pull request 1 — Engine and transport (Tasks 0–5).** Branch from `refactor/rearchitecture`, target `refactor/rearchitecture`. Its gate is:

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest tests/foundation tests/run_engine
```

No new tests directory, no new dependency, no schema change. The test count must not fall; if a reshaped test is deleted rather than adapted, say so explicitly in the pull request.

**Pull request 2 — Ingestion (Tasks 6–14).** Branch from PR 1 once merged. Its gate is the full Milestone Completion Gate below.

Both paths are new work, not remediation, so each needs its own pull request and independent review before merge, and explicit authorization to push or merge.

---

## Task Sequence

Task 0 comes first so the run engine tests are type-checked *before* Task 3 reshapes them. Tasks 1–5 are engine and transport work with no ingestion dependency. Tasks 6–11 build ingestion on top. Tasks 12–14 close the milestone. Task 6 (`canonicalize_article_url`) has no dependency on Tasks 0–5 and may be written early, but it ships in pull request 2: start its branch from pull request 1 once merged, or rebase onto it. Everything else is sequential.

---

### Task 0: Type-check the run engine tests

**Files:**
- Modify: `pyproject.toml`, and whichever `tests/run_engine/*.py` files the checker faults
- Test: `uv run pyright`

**Why this comes first.** Pyright's `include` is `["src", "tests/foundation"]`, so `tests/run_engine` has never been type-checked. Adding it surfaces roughly 61 errors. Fixing them *before* Task 3 reshapes those same tests means the reshape happens against a checked baseline, and a type error introduced by the reshape is visible rather than lost in pre-existing noise.

**Constraint that matters more than the error count:** these are tests, and a type fix must never weaken an assertion. The known clusters are generator-typed fixtures needing `Iterator[T]` return annotations, `fetchone()` results needing a `None` guard before member access, and `sorted()` over `int | None`. Each is a genuine looseness, not a checker complaint to silence.

**Prohibited:** blanket `# type: ignore`, `Any` annotations added to make an error disappear, and `reportOptionalMemberAccess` downgraded in configuration. If a fault cannot be fixed honestly, leave it and say why in the pull request.

**Steps:**

- [ ] Add `tests/run_engine` to Pyright's `include` in `pyproject.toml`.
- [ ] Run `uv run pyright` and record the starting error count.
- [ ] Fix the faults in clusters, running `uv run pytest tests/run_engine` after each cluster to confirm the test count has not changed and no assertion was softened.
- [ ] Run the PR 1 gate; expect zero Pyright errors and an unchanged test count.
- [ ] Commit.

---

### Task 1: Extract `RetryPolicy` from `RetryCoordinator`

**Files:**
- Modify: `src/notable_person_finder/runs/retry.py`
- Test: `tests/run_engine/test_retry.py`

**Interfaces:**
- Consumes: `ProviderFailure`, `FailureCategory`, `RETRYABLE_CATEGORIES`, `ProviderPaused`, `RetryConfig`, `Clock`.
- Produces:
  - `RetryDecision` — a frozen dataclass with `action` (`RETRY`, `PERMANENT`, `EXHAUSTED`) and `delay_seconds: float | None`.
  - `RetryPolicy(config, clock)` with `is_paused(provider)`, `paused_providers()`, `decide(provider, *, failure, attempt_index, malformed_retries) -> RetryDecision`, `record_success(provider)`, `record_exhaustion(provider)`, and `start(provider)` raising `ProviderPaused` when paused.
  - `RetryCoordinator` keeps its current public surface — `call`, `is_paused`, `paused_providers` — implemented over `RetryPolicy`.

**Behaviour that must not change.** Each of these already has tests in `tests/run_engine/test_retry.py`; they must keep passing untouched wherever possible:

- `max_attempts` is the outer bound on total calls.
- `malformed_response` gets at most one fresh retry, and never more than `max_attempts` total.
- `malformed_response` never increments the provider-pause counter, even when it consumes the final attempt.
- A non-retryable failure resets the pause counter and re-raises immediately.
- A `Retry-After` hint is capped at `max_backoff_seconds` for the *sleep*, while `AttemptRecord.retry_after_ms` persists the provider's raw uncapped value.
- Jitter is applied via the policy's injected random source; the coordinator
  uses its injected clock to sleep and to measure attempts.

**Steps:**

- [ ] Write failing tests against `RetryPolicy` directly: a retryable failure below `max_attempts` yields `RETRY` with a capped delay; a non-retryable failure yields `PERMANENT`; the final attempt yields `EXHAUSTED`; a second `malformed_response` yields `PERMANENT` rather than `RETRY` (the behaviour-preservation ruling: foreclosure re-raises the original failure rather than wrapping it as exhausted); `record_exhaustion` pauses at the configured threshold; `malformed_response` exhaustion does not.
- [ ] Run `uv run pytest tests/run_engine/test_retry.py -v`; expect failures naming `RetryPolicy`.
- [ ] Implement `RetryPolicy`, moving `_delay`, `_record_success_or_permanent`, `_record_exhaustion`, and the pause guard into it. Keep the existing comments explaining the malformed-response adjudication — they record decisions, not implementation detail.
- [ ] Reimplement `RetryCoordinator.call` over `RetryPolicy`, preserving its signature exactly, including `starting_ordinal` and `on_attempt`.
- [ ] Run `uv run pytest tests/run_engine -v`; every pre-existing retry test must pass unmodified.
- [ ] **Discrimination check.** Individually break each moved rule (remove the delay cap; let `malformed_response` count toward pausing; allow a second malformed retry) and confirm a *specific, named* test fails for the intended reason each time. Restore afterwards. A rule that survives its mutation is untested — write the test.
- [ ] Commit.

---

### Task 2: Batch claiming in the run repository

**Files:**
- Modify: `src/notable_person_finder/runs/repository.py`
- Test: `tests/run_engine/test_work_items.py`

**Interfaces:**
- Consumes: `WorkItem`, `WorkState`, the `work_item` table from migration `0002`.
- Produces: `claim_batch(connection, *, run_id, now, task_types, limit) -> tuple[WorkItem, ...]` — claims up to `limit` eligible items in one transaction, moving each to `running` and setting `claimed_by_run_id`.

**Constraints:**
- Ordering matches `next_eligible` exactly: `ORDER BY priority ASC, id ASC`. `eligible_at` is **not** a sort key — it is part of `_CLAIMABLE_PREDICATE` (`repository.py:324`), which also requires `state = 'pending'`, or `state = 'deferred'` from a *different* run. Required-before-optional is not expressed by the `required` column at all; it holds only insofar as seeding encodes it in `priority`.
- Because a `deferred` item is not claimable by the run that deferred it, Task 3's re-arm must return an item to `pending` with a future `eligible_at`, never to `deferred`. Deferring inside a run makes the item unclaimable for the remainder of that run.
- The whole batch claims in one transaction, so a crash leaves either all or none of the batch `running`. Milestone 2's startup sweep already returns abandoned `running` items to `pending`.
- `limit` must be at least 1; reject anything lower with `ValueError`.
- `claim_next` stays for any caller that wants a single item, implemented over `claim_batch` if that is clean, duplicated if not.

**Steps:**

- [ ] Write failing tests: claiming three of five eligible items returns exactly three in the documented order and leaves two `pending`; a claimed item is not reclaimable by a second call; `task_types` filtering is respected; items whose `eligible_at` is in the future are excluded; `limit=0` raises.
- [ ] Run the tests; expect failures naming `claim_batch`.
- [ ] Implement `claim_batch`.
- [ ] Run `uv run pytest tests/run_engine -v`.
- [ ] Commit.

---

### Task 3: Reshape the engine into a batch-claim state machine

**Files:**
- Modify: `src/notable_person_finder/runs/engine.py`
- Test: `tests/run_engine/test_engine.py`, `tests/run_engine/test_crash_boundary.py`

**Interfaces:**
- Consumes: `claim_batch` (Task 2), `RetryPolicy` (Task 1), `BoundedScheduler`, `repository.start_attempt`, `repository.finish_attempt`, `repository.complete_work`.
- Produces:
  - `TaskHandler.prepare: Callable[[WorkItem], object] | None` — runs on the **application thread** before submission, and is the only place a handler may read SQLite. Its return value is passed to `execute`. This is how a handler obtains database-derived call inputs, such as a feed's stored ETag and Last-Modified validators (Task 9) or, in 3b, an assembled prompt context.
  - `TaskHandler.execute` becomes `Callable[[WorkItem, int, object], TaskOutcome]` — a **one-shot call** taking the work item, the attempt ordinal, and whatever `prepare` returned (`None` when there is no `prepare`). Its contract is that it performs exactly one external call and never retries, never sleeps, and never touches SQLite. Document this on the dataclass.
  - `TaskOutcome.payload: object | None` — carries the handler's parsed result from the worker thread back to the application thread. It is never persisted directly and never inspected by the engine.
  - `TaskHandler.persist: Callable[[WorkItem, TaskOutcome], None] | None` — runs on the **application thread**, inside the same transaction that settles the work item, so a handler's domain writes and its work-item settlement commit or roll back together. This is where Task 10's ingestion persistence hangs.
  - `TaskHandler.persist_failure: Callable[[WorkItem, ProviderFailure], None] | None` — the matching application-thread persistence seam for engine-adjudicated provider failures that produce no `TaskOutcome`. It runs inside the item's settlement transaction and is mutually exclusive with `persist` when either hook is eligible; settlements without a trusted outcome or provider failure may invoke neither. Task 10 binds failed-fetch history here.
  - `TaskHandler.destination_host: Callable[[WorkItem], str | None] | None` — lets the engine populate `attempt.destination_host`, which milestone 2 always wrote as `None` because it had no real adapter.

  The three handler phases are `prepare` on the application thread, `execute` on a worker, and `persist` on the application thread. The engine's persistence phase spans two transactions: attempt outcome first, then handler domain writes and settlement together.
  - `RunEngine.execute(handlers, *, seed=None)` — `seed` is `Callable[[int], None] | None`, invoked with the run id after run creation and before the claim loop. See Task 9.

**The loop:**

1. Claim a batch of up to `concurrency.http_workers` items.
2. For each claimed item, on the application thread, **check the provider pause first**: if `RetryPolicy.is_paused(handler.provider)`, settle the item as `DEFERRED` with reason `provider paused` and write **no attempt row** — a paused provider means no call was made, and an attempt row must always correspond to exactly one external call. Milestone 2 enforced this inside `RetryCoordinator.call`, which the reshaped engine no longer routes through, so the engine must now own it explicitly. Only for unpaused items: call `handler.prepare` if present, compute the request fingerprint and destination host, and write the attempt row for the item's next ordinal.
3. Submit one closure per item to the scheduler. The closure captures the handler, the work item, and the prepared value — never the connection.
4. Drain completions on the application thread. For each, first commit the attempt outcome and cost reconciliation. Then, in the work-item settlement transaction, call `handler.persist` for an accepted `TaskOutcome`, or `handler.persist_failure` for an engine-adjudicated provider failure with no outcome, and either settle or re-arm the work item. A handler's domain writes and its work-item settlement commit or roll back together. An ordinary `prepare` exception is isolated before any attempt is created; a handler-persistence exception rolls back that settlement and is isolated by a second `failed_permanent` settlement with no domain writes. A refused non-settling outcome invokes neither hook.
5. **Re-arm means `pending` with a future `eligible_at`**, never `deferred`. `_CLAIMABLE_PREDICATE` excludes a `deferred` item from the run that deferred it, so re-arming to `deferred` would silently strand the item for the rest of the run. `deferred` remains the correct *terminal* state for an exhausted item, which should not be retried again until tomorrow.
6. Re-armed items rejoin the next claim batch when due. When only re-armed items remain and none is yet due, wait until the earliest deadline rather than spinning.
7. Stop when no eligible items remain.

**Invariants to test explicitly:**
- No worker thread touches SQLite. Assert structurally: the submitted closure must not close over the connection. A test that passes a connection-detecting sentinel, or that asserts on the thread identity at the point of every repository call, is acceptable; a comment is not.
- No transaction is open across an item's own call: that item's prepare and attempt insert commit before submission, and its outcome is persisted only after the call returns. This does not forbid a sibling call remaining in flight while the application thread settles another item's short SQLite transaction. Keep a guard that pins the per-item boundary now that `execute` runs on a worker.
- A paused provider settles its items as `DEFERRED` with no attempt row written.
- Bounded concurrency is actually observed: with `http_workers=2` and four items, at most two closures are ever in flight. Use a barrier or a counting fake, not timing.
- Backoff does not sleep the application thread: with one item re-armed for a long delay and another eligible now, the second completes before the first's deadline.
- Attempt ordinals stay contiguous per work item across re-arms.
- A crash between the attempt write and the outcome persist leaves the attempt row present and the work item recoverable by the sweep.

**Steps:**

- [ ] Write the failing tests above.
- [ ] Run them; expect failures.
- [ ] Implement the loop, deleting `_perform`'s interleaved retry.
- [ ] Run `uv run pytest tests/foundation tests/run_engine -v`. Existing engine tests will need updating where they assumed strict sequential execution — update them deliberately, and for each one you touch, confirm it still fails against the mutation it was written to catch. Adapting tests while reshaping the code they guard is how a suite gets quietly weakened.
- [ ] Commit.

---

### Task 4: `RunReport` budget fields and the non-settling state fix

**Files:**
- Modify: `src/notable_person_finder/runs/engine.py`, `src/notable_person_finder/runs/repository.py`, `src/notable_person_finder/reporting/digest.py`
- Test: `tests/run_engine/test_engine.py`, `tests/run_engine/test_digest.py`

**Interfaces:**
- Produces: `RunReport` gains `budget_limit_nano_usd: int | None`, `budget_reserved_nano_usd: int`, `budget_actual_nano_usd: int`, and `deferred_reasons: Mapping[str, int]` (reason string to count). The `TODO(milestone 3)` comment is deleted.

**Why:** milestone 2's review reproduced this with the real engine, cap, and digest — three required items at 1 USD against a 1.5 USD cap render "Required work deferred: 2" with the reason living only in SQLite.

**Also in this task — and this is a behaviour change, not a crash fix.** `NonSettlingStateError` is already caught: `cli/main.py:192-212` finishes the run as `interrupted`, logs `run_non_settling_state`, and returns `EXIT_FAILED`. No traceback escapes today.

The actual defect is what that path leaves behind: the offending work item is never settled, the whole run aborts on one bad item, and **no digest is written** even though other work may have succeeded. Move the handling into the engine — settle just that item as `failed_permanent` with a diagnostic reason, let the loop continue, and let the run finish and report normally. The CLI's `NonSettlingStateError` branch then becomes unreachable and should be removed rather than left as dead code.

Write the test against the new behaviour, not the old: a run with one non-settling handler and one healthy handler must produce a digest, settle the bad item `failed_permanent`, and record the healthy item's success.

**Steps:**

- [ ] Write failing tests: a run where budget exhaustion defers required work renders a digest naming the budget as the reason and reporting the cap and reserved amount; a handler returning `WorkState.PENDING` produces a `failed_permanent` item and a finished run rather than a traceback.
- [ ] Run them; expect failures.
- [ ] Add the repository query for per-run budget totals and deferral reason counts, extend `RunReport`, render them, and handle `NonSettlingStateError`.
- [ ] Run `uv run pytest tests/foundation tests/run_engine -v`.
- [ ] Commit.

---

### Task 5: Transport translation boundary and event-name taxonomy

**Files:**
- Modify: `src/notable_person_finder/providers/transport.py`, `src/notable_person_finder/obs/logging.py` and its call sites in `src/notable_person_finder/cli/main.py`
- Test: `tests/run_engine/test_transport.py`, `tests/run_engine/test_logging.py`

**Part A — widen the request-construction guard.** Commit `f9541cf` already moved `build_request` inside a `try` (`transport.py:276-290`), so the original finding is closed and a non-ASCII header value is already translated. Do **not** write that test: it passes today.

The remaining gap is narrower. The `except` clause catches only `UnicodeEncodeError`, so anything else `build_request` raises — `httpx.InvalidURL` is the realistic one — still escapes the `providers/` boundary untranslated. Widen the clause to catch `Exception` and translate to `ProviderFailure(FailureCategory.CONFIGURATION)`, keeping the existing sanitized detail for the encoding case so its message does not regress.

Test with an input that makes `build_request` raise something other than `UnicodeEncodeError`, and confirm it fails before the widening.

**Part B — `actual_nano_usd`.** `finish_attempt` defaults `actual_nano_usd=None`, and reconciliation deliberately retains the reservation in that case, so a failed attempt that cost nothing still consumes cap unless the caller passes `0`. Wire the HTTP path to pass `0` explicitly for attempts that cannot have incurred a charge. Document at the call site why this is deliberate, since 3b's LLM path must *not* do the same for a call the provider may have accepted.

**Part C — event names.** `run.started` and `run_started` coexist in one log file with overlapping fields. Keep dotted names throughout, but give the CLI's overlapping lifecycle emissions their own namespace and schema: `cli.run_started`, `cli.run_finished`, and `cli.run_reporting_failed`. The engine retains its existing `run.*` names. Update `cli/main.py`'s underscored emissions without making two emitters share one event name.

**Steps:**

- [ ] Write failing tests: an input making `build_request` raise something other than `UnicodeEncodeError` produces a `ProviderFailure` with `FailureCategory.CONFIGURATION`; a failed HTTP attempt records `actual_nano_usd = 0` and releases its reservation; a real run's `notable.jsonl` contains only dotted event names. Confirm each fails first — the pre-existing non-ASCII case already passes and is not a valid TDD step.
- [ ] Run them; expect failures.
- [ ] Implement all three.
- [ ] Run `uv run pytest tests/foundation tests/run_engine -v`.
- [ ] Commit.

---

### Task 6: `canonicalize_article_url` and the publisher key

**Files:**
- Create: `src/notable_person_finder/ingestion/__init__.py`, `src/notable_person_finder/ingestion/urls.py`
- Test: `tests/ingestion/__init__.py`, `tests/ingestion/test_urls.py`

**Interfaces:**
- Produces:
  - `canonicalize_article_url(url: str) -> str` — raises `UnusableArticleUrl` for anything that cannot be an article identity.
  - `publisher_key(canonical_url: str) -> str` — the registrable domain, lowercased.
  - `UnusableArticleUrl(ValueError)` carrying a typed `reason`.

**This is the single application-owned URL identity policy** named in the domain spec. Milestone 5's search occurrences and redirect destinations use this exact function; nothing else may normalize a URL.

**Normalization rules — conservative by design.** Over-normalizing merges distinct articles, which is unrecoverable; under-normalizing creates duplicates, which is merely untidy.

- Lowercase scheme and host; strip a default port; strip a trailing dot from the host.
- Reject anything that is not `http` or `https`, anything with embedded credentials, and anything without a host.
- Normalize `https` and `http` to a single form? **No.** Keep the scheme as given — a publisher serving both is rare, and collapsing them is a guess.
- Strip a `www.` prefix from the host for the *publisher key only*, never from the canonical URL.
- Strip the fragment entirely.
- Remove known tracking parameters — the `utm_*` family, `fbclid`, `gclid`, `mc_cid`, `mc_eid`, `igshid`, `ref`, `ref_src`, `s`, `cmp`, `CMP`. Keep every other parameter, and keep the surviving parameters in their original order.
- Do not strip trailing slashes, do not resolve dot segments beyond what `urllib` does, do not lowercase the path, and do not follow or guess at canonical link tags.
- Percent-encoding is normalized only by uppercasing hex digits and decoding unreserved characters.

**Registrable domain.** The dependency budget does not stretch to a public-suffix list. Derive the key as the last two labels of the host, with a hard-coded exception table for common multi-part public suffixes — `co.uk`, `org.uk`, `ac.uk`, `com.au`, `co.jp`, `co.nz`, `com.br` — where the last three labels are used instead. Document that this is deliberately approximate and that milestone 5's source policy, which is what actually depends on precise publisher identity, may need to revisit it.

**Steps:**

- [ ] Write a table-driven failing test covering every rule above, each with a named case, plus the convergence criterion: a feed-shaped URL with `utm_*` parameters and a fragment, and a bare search-shaped URL for the same article, canonicalize to the same string.
- [ ] Write failing tests for `publisher_key`, including each multi-part suffix and a `www.` prefix.
- [ ] Run `uv run pytest tests/ingestion/test_urls.py -v`; expect import failure.
- [ ] Implement `urls.py`.
- [ ] Run the tests.
- [ ] Commit.

---

### Task 7: Migration `0003` and the ingestion repository

**Files:**
- Create: `src/notable_person_finder/db/migrations/0003_ingestion.sql`, `src/notable_person_finder/ingestion/models.py`, `src/notable_person_finder/ingestion/repository.py`
- Test: `tests/ingestion/test_schema.py`, `tests/ingestion/test_repository.py`, `tests/ingestion/conftest.py`, `tests/ingestion/helpers.py`

**Schema.** Follow milestone 2's conventions exactly: `CHECK` constraints on every enumeration, `GLOB '*Z'` on every timestamp, integer nano-USD, integer milliseconds, foreign keys declared.

`feed_identity`
- `id`, `key` UNIQUE (the configured stable key — **never** the URL), `current_label`, `current_url`, `first_seen_at`, `last_seen_at`.

`feed_fetch`
- `id`, `feed_identity_id`, `run_id`, `requested_at`, `requested_url`, `resolved_url`, `redirect_chain_json`, `http_status`, `etag`, `last_modified`, `outcome` CHECK in (`not_modified`, `modified`, `failed`), `feed_type`, `parse_outcome` CHECK in (`ok`, `recovered`, `unusable`), `parser_warnings_json`, `failure_category` CHECK against the `FailureCategory` values, `entry_count`, `response_bytes`.
- Conditional state for the next fetch is **derived** from the latest fetch for
  the exact configured `requested_url` whose `outcome` is not `failed`. Do not
  store mutable validator state on `feed_identity`; a configured URL move must
  not inherit validators learned from the old representation.

`canonical_article`
- `id`, `canonical_url` UNIQUE, `publisher_key`, `first_seen_at`.

`article_url_alias`
- `id`, `canonical_article_id`, `url` UNIQUE, `kind` CHECK in (`feed_original`, `redirect_destination`), `first_seen_at`.
- Milestone 5 adds `search_result` to the CHECK. Note this in the migration comment so the later migration is expected, not a surprise.

`source_item`
- `id`, `feed_identity_id`, `discovered_by_fetch_id`, `discovered_by_run_id`, `canonical_article_id` nullable, `source_entry_id` nullable, `original_url` nullable, `title_raw`, `title_text`, `summary_raw`, `summary_text`, `author_raw`, `published_raw`, `published_at` nullable, `published_issue` nullable CHECK in (`missing`, `unparseable`, `implausible`), `url_issue` nullable CHECK in (`missing`, `not_http`, `unsafe`, `unusable`), `discovered_at`.
- `CREATE UNIQUE INDEX source_item_article ON source_item(canonical_article_id) WHERE canonical_article_id IS NOT NULL`.
- `CREATE UNIQUE INDEX source_item_entry ON source_item(feed_identity_id, source_entry_id) WHERE source_entry_id IS NOT NULL AND canonical_article_id IS NULL`.

The second index is deliberately narrow: entry-id dedup applies only where there is no article to dedup on, so a feed that reuses entry ids across genuinely different URLs is not silently collapsed.

**Repository interfaces** (all take `connection` first, all keyword-only after):
- `upsert_feed_identity(*, key, label, url, now) -> int`
- `latest_validators(*, feed_identity_id, requested_url) -> tuple[str | None, str | None]` — ETag and Last-Modified from the latest non-failed fetch for that exact requested URL
- `record_fetch(*, feed_identity_id, run_id, requested_at, requested_url, resolved_url, redirect_chain_json, http_status, etag, last_modified, outcome, feed_type, parse_outcome, parser_warnings_json, failure_category, entry_count, response_bytes) -> int`
- `upsert_article(*, canonical_url, publisher_key, now) -> int`
- `record_alias(*, canonical_article_id, url, kind, now) -> None`
- `insert_source_item(*, feed_identity_id, discovered_by_fetch_id, discovered_by_run_id, canonical_article_id, source_entry_id, original_url, title_raw, title_text, summary_raw, summary_text, author_raw, published_raw, published_at, published_issue, url_issue, now) -> int | None` — returns `None` when a uniqueness constraint means the item already exists
- `source_item_counts(*, run_id) -> SourceItemCounts`

`SourceItemCounts` is a frozen dataclass in `ingestion/models.py` with `total: int`, `created_in_run: int`, and `articles_total: int`. Its scopes are deliberate: `total` and `articles_total` are unscoped whole-corpus counts across every feed and run, while `created_in_run` is the source-item delta whose `discovered_by_run_id` equals the supplied `run_id`.

**Steps:**

- [ ] Write failing schema tests: migration `0003` applies to a fresh database and to a `0002` database; every table exists with the expected columns; each CHECK rejects an invalid value; each unique index rejects a duplicate; foreign keys are enforced.
- [ ] Write failing repository tests: insert-once across two runs yields one source item; insert-once across two feeds yields one source item and the second call returns `None`; a URL-less entry dedups on `(feed, entry_id)`; two URL-less entries with different entry ids both insert; `latest_validators` ignores failed fetches and fetches made against an earlier configured URL.
- [ ] Run them; expect failures.
- [ ] Write the migration and the repository.
- [ ] Run `uv run pytest tests/foundation tests/ingestion -v`. The foundation migration tests must still pass — `0003` must not break the checksum chain.
- [ ] Commit.

---

### Task 8: `FeedClient` and the feedparser adapter

**Files:**
- Create: `src/notable_person_finder/providers/feeds.py`, `tests/ingestion/fixtures/*`
- Modify: `pyproject.toml`, `uv.lock`
- Test: `tests/ingestion/test_feeds.py`

**Interfaces:**
- Produces:
  - `FeedClient` — a `Protocol` with `fetch_feed(feed: FeedConfig, validators: FeedValidators) -> FeedFetchResult`.
  - `FeedValidators` — frozen: `etag: str | None`, `last_modified: str | None`.
  - `FeedFetchResult` — frozen, a discriminated result: `NotModified` (response metadata only) or `Modified` (requested and resolved URLs, redirect chain, response validators, feed type, source metadata, `entries: tuple[FeedEntry, ...]`, `warnings: tuple[str, ...]`, `response_bytes`).
  - `FeedEntry` — frozen, source-written values only: `entry_id`, `url`, `title`, `summary`, `content`, `author`, `published_raw`, `updated_raw`. The two raw date fields stay distinct; the adapter never falls back from one to the other.
  - `FeedparserClient(transport: HttpTransport)` implementing the protocol.

**Boundary discipline.** The adapter translates external field names and optionality and decides **nothing** about usability. It never validates a URL, never normalizes text, never parses a date, and never assigns a skip reason. Ingestion owns all of that (Task 10).

**Failure translation:**
- HTTP failures already arrive as `ProviderFailure` from the transport. Pass them through.
- HTTP 304 is `NotModified` — a success, not a failure.
- A payload feedparser recognizes as RSS or Atom yields `Modified` with its entries **and** its warnings retained, even when `bozo` is set. Recovered entries are accepted; warnings are never silently erased.
- A payload that is not recognizably RSS or Atom, or has no trustworthy feed structure, raises `ProviderFailure(FailureCategory.MALFORMED_RESPONSE, retryable=True, ...)`.

  **The explicit `retryable=True` is required and is a deliberate policy choice.** `MALFORMED_RESPONSE` is *not* in `RETRYABLE_CATEGORIES` (`failures.py:28-36`), so `ProviderFailure.retryable` defaults to `False` for it. The override buys one fresh attempt because a feed's malformed payload is often a self-healing CDN or origin error page.

  The shared foreclosure rule still governs the second malformed response: `RetryPolicy` answers `PERMANENT` before the attempt budget is exhausted. Consequently, with `max_attempts=1` a first malformed response exhausts and settles `DEFERRED`; with any `max_attempts >= 2`, including the shipped default of 3, a second malformed response settles `FAILED_PERMANENT` after two calls. This is intentional behaviour preservation from Task 1. It does not remove the feed forever: a terminal item does not block the next ordinary run from seeding a fresh `fetch_feed` item for the same feed.
- `detail` on every raised failure must be sanitized: no response bodies, no full URLs with query strings, no credentials.

**Dependency.** Add `feedparser>=6.0,<7` and regenerate `uv.lock` with `uv sync`. Note that feedparser is used **only** to parse supplied bytes — it must never fetch, so pass bytes, never a URL.

**Fixtures** in `tests/ingestion/fixtures/`: a well-formed RSS 2.0 feed, a well-formed Atom feed, an imperfect feed that parses with warnings and recoverable entries, an HTML error page, an empty body, a feed with entries lacking ids, and a feed with entries lacking links.

**Steps:**

- [ ] Add the dependency and regenerate the lock; commit that alone so the dependency change is reviewable in isolation.
- [ ] Write failing tests using a fake `HttpTransport` that returns fixture bytes: each fixture produces the expected result kind, entry count, and warning behaviour; 304 yields `NotModified`; the HTML error page and the empty body raise `MALFORMED_RESPONSE`; conditional headers are sent when validators are present and omitted when they are not; response validators are read back off the response.
- [ ] Run them; expect import failure.
- [ ] Implement `providers/feeds.py`.
- [ ] Run `uv run pytest tests/ingestion -v`.
- [ ] Commit.

---

### Task 9: Seeding and the `fetch_feed` handler

**Files:**
- Create: `src/notable_person_finder/ingestion/service.py`
- Test: `tests/ingestion/test_service.py`

**Interfaces:**
- Produces:
  - `seed_feeds(connection, *, feeds: FeedsConfig, run_id: int, now: str) -> None`.
  - `build_seed_hook(connection, *, feeds: FeedsConfig, clock: Clock) -> Callable[[int], None]` — adapts `seed_feeds` to the engine's `seed` signature, which takes only the run id. The CLI passes this, not `seed_feeds` directly.
  - `build_fetch_handler(connection, *, client: FeedClient, feeds: FeedsConfig) -> TaskHandler` — returns the `fetch_feed` handler, with `prepare` bound to the connection and `execute` bound only to the client.
  - `FETCH_FEED_TASK_TYPE = "fetch_feed"`.

**Seeding, with repository-owned transactions:**
- Each feed identity upsert commits before that feed's scheduling call;
  `schedule_work` then owns its own `BEGIN IMMEDIATE` transaction. There is no
  transaction spanning the pair or spanning multiple feeds, so a later
  scheduling failure cannot roll back an already-refreshed identity.
- Upsert `feed_identity` for every **enabled** feed, refreshing `current_label`, `current_url`, and `last_seen_at`.
- Schedule one **required** `fetch_feed` work item per enabled feed, `subject_kind="feed_identity"`, `subject_id` the feed identity id.
- The fingerprint is SHA-256 over canonical JSON of `{feed key, configured URL, parser version}` — at seed time no fetch has happened, so the configured URL is the only one that exists. The parser version is a module constant bumped when parsing behaviour changes materially, so a parser change legitimately reschedules and a config comment change does not.
- Call `schedule_work` unconditionally and let it decide. It already opens `BEGIN IMMEDIATE`, selects any existing item in `pending`/`running`/`deferred`, and returns that id instead of inserting (`repository.py:246-287`) — the partial unique index backs that pre-check rather than replacing it. So a *succeeded* item from yesterday is terminal and does not block today's, while a *deferred* item is reused rather than duplicated. Do not add a second existence check in the ingestion layer.
- Supersede active work for feeds that have been disabled or removed from configuration, with reason `feed disabled`.

**The handler is split across the two threads**, and this is the one place the division is easy to get wrong:

- `prepare(work_item)` runs on the **application thread**. It resolves the feed identity to its `FeedConfig` and reads validators stored for that feed's exact configured URL via `latest_validators`. This is the handler's only SQLite access. It returns a `FeedCall` frozen dataclass carrying `feed: FeedConfig` and `validators: FeedValidators`; a configured URL move therefore starts without validators learned from the old representation.
- `execute(work_item, ordinal, prepared)` runs on a **worker thread**. It performs exactly one `client.fetch_feed(prepared.feed, prepared.validators)` and returns a `TaskOutcome`. No retry, no sleep, and no SQLite — the closure never sees a connection.

State this in the code, not only here. Task 10's persistence likewise runs on the application thread.

**Outcome mapping:**
- `NotModified` → `WorkState.SUCCEEDED`, reason `not modified`, with the result on `TaskOutcome.payload` so the fetch row is still written.
- `Modified` → `WorkState.SUCCEEDED`, with the parsed result on `TaskOutcome.payload` for Task 10's `persist`.
- `ProviderFailure` propagates; the engine's retry policy decides. Exhausted transient failures settle as `DEFERRED`, eligible tomorrow. Non-retryable failures settle as `FAILED_PERMANENT`.
- `response_too_large` and `unsupported_content` settle as `DEFERRED` and are recorded honestly as *not inspected* — never as an empty feed. Neither is in `RETRYABLE_CATEGORIES`, so both default to `FAILED_PERMANENT`. Because retrying them within the run is pointless — the response will be just as large next time — do **not** override `retryable`. Instead the handler catches these two categories and returns `TaskOutcome(state=WorkState.DEFERRED, ...)` directly, so they are deferred without burning attempts. (`unsupported_content` is not raised anywhere in `src/` today; the mapping is written now so the first adapter that raises it behaves correctly.)

  This direct `DEFERRED` result is stored as a successful engine attempt because the attempt schema permits `failure_category` only when `outcome='failed'`. The adapter's sanitized category and detail therefore live in `attempt.detail_json`, while the ingestion layer writes a `feed_fetch(outcome='failed', failure_category=...)` row. The digest's deferral reason and ingestion failure count retain the operator-visible result, but the attempt-based operational-failure count and failure-category breakdown omit this not-inspected path by construction.

**Steps:**

- [ ] Write failing tests: seeding creates one work item per enabled feed and none for a disabled feed; re-seeding with an already-active item does not duplicate; a succeeded item from a previous run does not block a new one; a deferred item is reused; disabling a feed supersedes its active work; a changed feed URL produces a new fingerprint.
- [ ] Write failing tests for the handler: `NotModified` succeeds; `Modified` succeeds; a transient failure exhausts to `DEFERRED`; with `max_attempts=1`, a first `malformed_response` exhausts to `DEFERRED`, while with `max_attempts>=2` the fresh retry's second malformed response forecloses as `FAILED_PERMANENT`; `prepare` returns the stored validators for a feed with a previous successful fetch and empty validators for a first fetch; `execute` closes over no connection.
- [ ] Run them; expect failures.
- [ ] Implement `service.py`.
- [ ] Run `uv run pytest tests/ingestion -v`.
- [ ] Commit.

---

### Task 10: Persist fetches, articles, and source items

**Files:**
- Modify: `src/notable_person_finder/ingestion/service.py`, `src/notable_person_finder/ingestion/models.py`
- Test: `tests/ingestion/test_persistence.py`

**Interfaces:**
- Consumes: `TaskHandler.persist` and `TaskHandler.persist_failure` (Task 3), `TaskOutcome.payload` (Task 3), the repository (Task 7), `canonicalize_article_url` and `publisher_key` (Task 6).
- Produces:
  - `persist_fetch(connection, *, feed_identity_id, run_id, result, now) -> FetchPersistResult` with `source_items_created: int`, `source_items_existing: int`, `articles_created: int`, `entry_issues: Mapping[str, int]`. `build_fetch_handler` binds this as the handler's `persist`, reading the `FeedFetchResult` off `TaskOutcome.payload`.
  - `persist_failed_fetch(connection, *, feed_identity_id, run_id, requested_url, failure, now) -> FetchPersistResult`, bound as the handler's `persist_failure`, so an engine-adjudicated provider failure writes its `feed_fetch` history inside the same settlement transaction and returns zero ingestion deltas.

**Runs on the application thread**, inside the engine's per-item settlement transaction, so one feed's failure cannot roll back another's items.

**Always** write a `feed_fetch` row for an accepted handler outcome or an adjudicated provider failure — including `not_modified`, not-inspected, and failed settlements. A failed fetch that leaves no trace is indistinguishable from a fetch that never happened. The deliberate exception is the engine's non-settling-state guard: it discards an outcome it refuses to trust, settles the item `failed_permanent`, and leaves only its `attempt` row as call evidence rather than invoking either persistence hook.

**Per entry, in order:**
1. Normalize title and summary to plain text: strip HTML tags, unescape entities, collapse whitespace. Keep the raw value alongside.
2. Resolve the entry URL. If present and usable, canonicalize via Task 6 and derive the publisher key; upsert `canonical_article`; record the original URL as a `feed_original` alias when it differs from the canonical form.
3. Parse the publication date. On success store the UTC value; on failure store the raw text with a typed `published_issue`. **Never drop an entry for a date problem.**
4. Insert the source item. A `None` return means it already exists — count it as existing and continue.

**Entry-level failures never fail the fetch.** An entry whose URL is missing, non-HTTP, unsafe, or unusable still becomes a source item with `canonical_article_id` NULL and a typed `url_issue`. It still has a title and a summary, and may still name a person. This is the high-recall rule at its smallest scale, and it is why the URL-less dedup fallback exists.

**Steps:**

- [ ] Write failing tests: a `modified` fetch with three entries creates three source items and one fetch row; re-running the identical fetch creates a second fetch row and zero new source items; a `not_modified` fetch creates a fetch row and no source items; a failed fetch creates a fetch row with its failure category; an entry with no URL becomes a source item with `url_issue='missing'`; an entry with an unparseable date becomes a source item with the raw text and `published_issue='unparseable'`; HTML in a title is stripped in `title_text` and preserved in `title_raw`; two feeds serving the same URL yield one source item; a tracking-parameter URL and a bare URL converge on one article with two aliases.
- [ ] Run them; expect failures.
- [ ] Implement `persist_fetch`.
- [ ] Run `uv run pytest tests/ingestion -v`.
- [ ] Commit.

---

### Task 11: CLI wiring, digest summary, and `notable status`

**Files:**
- Modify: `src/notable_person_finder/cli/main.py`, `src/notable_person_finder/reporting/digest.py`
- Test: `tests/ingestion/test_run_cli.py`

**Wiring.** `command_run` constructs no transport today (`cli/main.py:152-154` notes it is left to the first adapter milestone), so this task builds that lifecycle from scratch: build one run-scoped `PacingGate` from the configured pacing and per-origin limits, inject it into `build_transport(...)`, and wrap the transport in a `with` block so it closes on every exit path including failure. Enter the transport before the scheduler so exceptional reverse-order exit drains in-flight workers before closing their HTTP client. Build a `FeedparserClient` over the transport, construct the handler via `build_fetch_handler`, and pass `build_seed_hook(...)` as the engine's `seed` hook. Tests must prove configured same-origin concurrency on real request calls and transport closure only after in-flight siblings exit, not merely that the objects were constructed.

**Digest.** The operational summary gains an ingestion section: feeds fetched, feeds not modified, feeds failed, source items created, and articles created. The three feed counters use the final stored settlement per feed identity in the run (the highest `feed_fetch.id`), not raw calls or rows, so duplicate work after a URL change cannot count one feed twice. Milestone 6 still owns the ranked shortlist; this section sits in the operational summary, not in the placeholder shortlist.

**`notable status`.** Add total source items, total articles, and the most recent successful fetch time per feed. Backlog, queue tiers, and oldest pending candidate remain milestone 6's.

**Steps:**

- [ ] Write failing end-to-end tests through `command_run` against a temporary data root and a fake transport serving fixtures: the run exits 0, writes a dated digest and `latest.md`, and the digest reports the ingestion counts; a second identical run reports zero new source items; a run where one feed fails still ingests the others and reports the failure; `notable status` reports the source-item count.
- [ ] Run them; expect failures.
- [ ] Implement the wiring and rendering.
- [ ] Run `uv run pytest tests/foundation tests/run_engine tests/ingestion -v`.
- [ ] Commit.

---

### Task 12: Seam tests

**Files:**
- Test: `tests/ingestion/test_seams.py`

**Why this task exists.** Milestone 2's whole-branch review found that three of its four passing broken variants were CLI-to-module wiring that no task brief owned — per-task review structurally could not catch them. That is the finding about the process, not just the code. This task exists so the seams have an owner.

**Method.** For each seam below, edit the shipped source to break it, run the full gate, and confirm a **specific, named** test fails for the intended reason. Then restore and confirm the gate is green. A seam that survives its mutation is untested — write the test before moving on.

Seams to break:
- `command_run` passes no `seed` hook → no work is created, no feed is ever fetched.
- `command_run` registers no `fetch_feed` handler → the run completes with no ingestion.
- The feed client is built with a transport that ignores the configured response-size limit.
- `persist_fetch` skips the `feed_fetch` row on failure.
- `persist_fetch` silently swallows an entry-level exception instead of recording a typed issue.
- The handler closure captures the connection (worker-thread SQLite access).
- Seeding does not supersede work for a disabled feed.

Record in the commit message which mutations were run and which test caught each one.

**Steps:**

- [ ] Work through each seam, writing the test that catches it where none exists.
- [ ] Run the full offline gate and confirm green after restoring every mutation.
- [ ] Commit.

---

### Task 13: Opt-in live smoke

**Files:**
- Create: `tests/ingestion/test_live_feeds.py`
- Modify: `pyproject.toml`
- Test: itself

**Configuration.** The `live` marker already exists in `pyproject.toml`. Add `addopts = "-ra -m 'not live'"` so live tests are deselected by default and the standard gate stays offline. Confirm that `uv run pytest tests/ingestion` reports them as deselected, not as passed.

**The tests**, each marked `live`, against two or three real feeds from `config/discovery-feeds.example.toml`:
- A real conditional fetch returns `Modified` with at least one entry, or `NotModified`.
- A second fetch using the returned validators yields `NotModified`, proving conditional requests actually work against a real server.
- Redirect handling and the response-size bound behave against a real origin.
- Entries canonicalize to plausible article URLs and publisher keys.

These must skip cleanly, not fail, when the network is unavailable.

**Why it matters.** Milestone 2's review recorded that "359 passing is weaker evidence for transport behaviour against a real remote than the number suggests", naming the uncapped `Retry-After`, the narrow `build_request` guard, and a truncated-gzip case as concrete instances. This is the first milestone that can test that claim.

**Steps:**

- [ ] Add the `addopts` change and confirm deselection.
- [ ] Write the live tests.
- [ ] Run `uv run pytest tests/ingestion -m live -v` against the network and record the output.
- [ ] Confirm `uv run pytest tests/ingestion` deselects them.
- [ ] Commit.

---

### Task 14: Milestone acceptance and documentation

**Files:**
- Modify: `CLAUDE.md`, `AGENTS.md`, `docs/running.md`, `docs/troubleshooting.md`, `pyproject.toml`

**Documentation:**
- `CLAUDE.md` is the canonical repository guide and `AGENTS.md` is its symlink. Update the canonical file once, verify both paths resolve to identical content, move feed ingestion into "delivered", state plainly that source items carry no triage observation until 3b, and remove the gaps this milestone closed (`RunReport` budget fields; the non-settling-state abort). Retain the narrower call-evidence limitation of a non-settling outcome, which the engine's handled settlement does not close.
- `docs/running.md`: what ingestion does, what the digest's ingestion section means, and how to run the live smoke.
- `docs/troubleshooting.md`: a feed stuck in `deferred`, a feed producing `malformed_response`, and what `url_issue` and `published_issue` values mean.
- `pyproject.toml`: add `tests/ingestion` to Pyright's `include`.

**Steps:**

- [ ] Update the documentation.
- [ ] Run the full completion gate below.
- [ ] Confirm `git diff --check` and `git status --short` are clean.
- [ ] Commit.

---

## Milestone Completion Gate

The milestone is complete only when every item below holds.

**Automated, offline:**

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion
```

**Manual, recorded:**

```bash
uv run pytest tests/ingestion -m live -v
```

Run against real configured feeds, with the output recorded as evidence in the progress ledger.

**Also required:**

- `git diff --check` and `git status --short` are clean.
- A real `notable run` on a fresh temporary data root exits 0, writes a dated digest and `latest.md`, and ingests from fixture feeds.
- Grepping the whole populated data root for the configured secret values finds nothing.
- Every seam mutation in Task 12 was executed and caught by a named test.
- Every retry semantic listed in Task 1 survives its own mutation.

---

## Acceptance Criteria

Mapped from the approved designs:

- A feed serving the same article twice yields one source item.
- Two feeds serving the same article yield one source item. Duplicate-feed discovery is knowingly not recorded (D6).
- Feed and search provenance can converge on one canonical article: a tracking-parameter URL and a bare URL produce one article with two aliases.
- An entry with no usable URL still becomes a source item, with a typed issue.
- An entry with an unparseable date still becomes a source item, with its raw text retained.
- A feed identity survives a change to its URL, because identity is the configured key.
- Conditional requests work: a second run against an unchanged feed does no redundant parsing.
- One feed's failure does not roll back another feed's items.
- An interrupted fetch becomes eligible in the next run.
- No transaction is held across its own item's external I/O, and no worker thread touches SQLite; sibling calls may remain in flight during another item's local settlement.
- Parser warnings are retained and never silently erase valid entries.
- A first malformed feed response gets at most one fresh attempt. At `max_attempts=1` it exhausts to `deferred`; at the shipped default a second malformed response is foreclosed `failed_permanent`, and the next ordinary run seeds fresh work for that feed.
- The digest reports ingestion counts, and a deferred run can explain why.
- No workflow path creates, drafts, or edits Wikipedia content.

---

## Deferred to Later Milestones

- `LlmClient` and OpenRouter; `detect_people`; triage observations; person mentions; people; sourced names; entity resolution; `possible_same_person`; merges — milestone 3b.
- `MediaWikiClient` and Wikipedia identity — milestone 4.
- `WebSearchClient`, `ArticleFetcher`, `ArticleExtractor`, search occurrences, article views, person–article assessment, source-policy screening and its provenance, and the `search_result` alias kind — milestone 5.
- Lead assessment, ranking, the digest queue and its shortlist section, model synthesis, `notable digest show`, `notable audit run`, `notable audit person`, and `notable status` backlog and tiers — milestone 6.
- Promptfoo suites, manual live checks, the full-system offline acceptance, the one-time legacy comparison, and the prototype cutover decision — milestone 7.
- `source_item_sighting` for duplicate-feed discovery (D6).
- A real public-suffix list for publisher keys (Task 6).
- `robots.txt` handling, explicitly deferred by the provider adapters design.
