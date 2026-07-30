# Model Gateway and Person Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Track execution evidence in
> `.superpowers/sdd/2026-07-29-model-gateway-and-detection/progress.md`, not in
> this plan.

**Goal:** Add the shared OpenRouter gateway and turn every durable untriaged
source item into an immutable detection observation and zero or more unresolved,
source-grounded person mentions.

**Architecture:** The `people` package owns detection schemas, prompt, context,
validation, persistence, and scheduling behind an application-owned `LlmClient`
protocol. The OpenRouter adapter owns SDK translation only. The run engine gains
prepare-returned reservations and pool-aware claim routing over independent HTTP
and LLM worker pools; current-run model inspection is a readiness gate for paid
generation.

**Tech Stack:** Python 3.13, SQLite, Pydantic 2, official `openrouter` Python SDK
1.x, pytest, Ruff, Pyright, uv.

## Global Constraints

- Python 3.13 or newer; distribution `notable-person-finder`; import package
  `notable_person_finder`; executable `notable`.
- Work only under `src/notable_person_finder` and the focused rewrite tests.
  Do not import, run, reorganize, or repair the retained prototype.
- The application never writes, drafts, or edits Wikipedia content.
- OpenRouter is the only model gateway. Models receive no tools, network access,
  memory lookup, workflow control, publisher policy, ranking authority, or
  notability verdict.
- Every external call maps to exactly one persisted attempt. SDK retries are
  disabled; only the central retry policy starts another visible call.
- No SQLite transaction spans network/model work and no worker thread touches
  SQLite.
- Every request contains one source item and only supplied evidence. Valid
  semantic uncertainty succeeds; operational failure never becomes a semantic
  negative.
- Secrets remain environment-only and never enter TOML values, snapshots,
  fingerprints, SQLite product records, logs, digests, terminal output, or tests.
- Money is integer nano-USD; UTC timestamps are canonical ISO-8601 text ending
  in `Z`; booleans/enums use checked columns.
- Configuration is strict, file-first, and validates every bound before a run is
  created. Default tests are offline; paid/live checks are explicit.
- Migrations are forward-only, checksummed, transactional, and backed up before
  changing an existing database.
- Plans remain authorities for interfaces and gates, not sources of canonical
  implementation fragments. Shipped code and tests govern.

## Authoritative Inputs

- Design: `docs/superpowers/specs/2026-07-29-model-gateway-and-detection-design.md`.
- Cross-cutting specs: the approved overarching, product workflow, domain
  persistence, LLM evaluation, provider adapter, and operator experience specs
  under `docs/superpowers/specs/`.
- Current provider contract: official OpenRouter Python SDK package
  `openrouter>=1.0,<2`; `OpenRouter.models.get(...)` for one model inspection;
  `OpenRouter.chat.send(...)` for strict JSON-schema generation; SDK-level
  `RetryConfig(strategy="none", ...)`; `response_format.type="json_schema"`,
  strict schema, and provider `require_parameters=true`. Reconfirm these against
  the locked version during Task 7 rather than coding against memory.

---

## Task 1: Add and lock the official OpenRouter SDK

**Files:**

- Modify: `pyproject.toml`
- Modify: `uv.lock`

**Interfaces:**

- Produces the import `from openrouter import OpenRouter` at a compatible
  constraint `openrouter>=1.0,<2`.
- No production code imports the dependency in this task.

- [ ] Add `openrouter>=1.0,<2` to runtime dependencies and run `uv sync` to
  regenerate `uv.lock`.
- [ ] Inspect the locked package, recording in the progress ledger the exact
  version and signatures of `OpenRouter`, `models.get`, `chat.send`,
  `RetryConfig`, and the context-manager lifecycle.
- [ ] Run `uv sync --frozen` and `uv run pytest tests/foundation -q`.
- [ ] Commit only dependency metadata with
  `build: add the OpenRouter Python SDK`.

## Task 2: Let preparation override an attempt reservation

**Files:**

- Modify: `src/notable_person_finder/runs/engine.py`
- Modify: `src/notable_person_finder/ingestion/service.py`
- Modify: `tests/run_engine/test_engine.py`
- Modify: `tests/ingestion/test_service.py`

**Interfaces:**

- Produce:

  ```python
  @dataclass(frozen=True, slots=True)
  class TaskPreparation:
      payload: object = None
      reserved_nano_usd: int | None = None
  ```

- Change `TaskHandler.prepare` to
  `Callable[[WorkItem], TaskPreparation] | None`.
- `None` reservation uses `TaskHandler.reserved_nano_usd`; an explicit integer,
  including zero, overrides it for this attempt. `_Submission.prepared` carries
  only `TaskPreparation.payload` to the worker.

- [ ] Write failing engine tests proving a prepare-returned amount reaches both
  the attempt and run reservation, fixed handlers retain their default, zero is
  a real override, a negative override is rejected before an attempt, and budget
  refusal writes neither attempt nor provider call.
- [ ] Run the named tests and confirm failure against the current fixed field:
  `uv run pytest tests/run_engine/test_engine.py -k 'preparation or reservation' -v`.
- [ ] Implement `TaskPreparation`, validate the effective reservation on the
  application thread, and use it in `repository.start_attempt`.
- [ ] Update the feed handler to return
  `TaskPreparation(payload=FeedCall(...))`; keep its fixed zero reservation.
- [ ] Run `uv run pytest tests/run_engine tests/ingestion/test_service.py -q`.
- [ ] Commit with `feat(runs): allow prepared attempt reservations`.

## Task 3: Add pool-aware concurrent scheduling and readiness

**Files:**

- Modify: `src/notable_person_finder/runs/scheduler.py`
- Modify: `src/notable_person_finder/runs/engine.py`
- Modify: `tests/run_engine/test_scheduler.py`
- Modify: `tests/run_engine/test_engine.py`

**Interfaces:**

- Produce `WorkerPool(StrEnum)` with `HTTP = "http"` and `LLM = "llm"`.
- Produce `SchedulerSet`, constructed from exactly one `BoundedScheduler` for
  every registered `WorkerPool`, with:

  ```python
  def max_workers(self, pool: WorkerPool) -> int: ...
  def run(self, submissions, worker) -> Iterator[Completion]: ...
  ```

- Add to `TaskHandler`:
  `pool: WorkerPool = WorkerPool.HTTP` and
  `ready: Callable[[int], bool] | None = None`.
- Change `RunEngine` to accept one `SchedulerSet`. Each loop evaluates readiness
  on the application thread, groups ready task types by pool, claims at most that
  pool's capacity, prepares every claimed item on the application thread, then
  waits over the union of futures so HTTP and LLM calls may overlap.

- [ ] Write failing scheduler tests proving each pool's independent in-flight
  cap, cross-pool overlap, completion on the calling thread, worker exceptions
  retaining tracebacks, and clean shutdown waiting for both pools.
- [ ] Write failing engine tests proving handler pool routing, per-pool batch
  limits, `ready=False` preventing claim/attempt creation, readiness becoming
  true after another settlement in the same run, and pending gated work making
  the run partial rather than disappearing.
- [ ] Run the focused tests and confirm the single-scheduler engine cannot pass.
- [ ] Implement `SchedulerSet` without creating another executor and preserve
  `BoundedScheduler.run` compatibility for focused scheduler tests.
- [ ] Refactor `_drain` to claim per ready pool and resolve the combined
  completion stream on the application thread. A handler belongs to one pool;
  reject missing pool registrations before run creation.
- [ ] Run `uv run pytest tests/run_engine -q` and `uv run pyright`.
- [ ] Commit with `refactor(runs): schedule HTTP and LLM worker pools`.

## Task 4: Add strict model and detection configuration

**Files:**

- Modify: `src/notable_person_finder/config/models.py`
- Modify: `src/notable_person_finder/config/loader.py`
- Modify: `config/notable.example.toml`
- Modify: `tests/foundation/helpers.py`
- Modify: `tests/foundation/test_config_models.py`
- Modify: `tests/foundation/test_config_loader.py`
- Modify: `tests/foundation/test_review_findings.py`

**Interfaces:**

- Produce frozen strict models:
  `OpenRouterConfig`, `ProviderRoutingConfig`, `GenerationParameters`,
  `DetectPeopleConfig`, and `TasksConfig`.
- Pin these fields:
  - endpoint (public HTTPS by default), `allow_fallbacks`, `data_collection`,
    and `zdr`;
  - `tasks.detect_people.model`, `max_input_tokens`,
    `max_completion_tokens`, `temperature`, `top_p`, optional
    `reasoning_effort`, `max_people`, `max_title_characters`, and
    `max_summary_characters`;
  - positive finite token bounds and internally ordered character/mention
    limits. Exact rendered-input enforcement belongs to Task 6 because config
    loading must not import the People prompt or schema.
- The resolved redacted snapshot includes every non-secret value and only the
  OpenRouter key's availability.

- [ ] Write failing model tests for accepted defaults and rejection of unknown
  fields, unsafe endpoints, router aliases/multiple-model syntax, invalid model
  slugs, non-finite generation numbers, incompatible bounds, and out-of-range
  mention/context limits.
- [ ] Write failing loader tests proving config changes alter the fingerprint,
  environment secret values do not, process environment beats `.env`, and
  validation never makes a network request.
- [ ] Implement the models and snapshot wiring. Keep `max_completion_tokens`
  as the SDK parameter; do not use deprecated `max_tokens`.
- [ ] Update the tracked example with copyable conservative values and no key.
- [ ] Run `uv run pytest tests/foundation -q` and `uv run pyright`.
- [ ] Commit with `feat(config): add model detection settings`.

## Task 5: Add migration 0004 for model inspection and unresolved mentions

**Files:**

- Create: `src/notable_person_finder/db/migrations/0004_people_detection.sql`
- Create: `tests/people/__init__.py`
- Create: `tests/people/conftest.py`
- Create: `tests/people/test_schema.py`
- Modify: `tests/foundation/test_migrations.py`
- Modify: `pyproject.toml`

**Interfaces:**

- Add immutable tables `model_inspection`, `triage_observation`,
  `person_mention`, `mention_identity_fact`, and `mention_signal`.
- Add nullable `source_item.current_triage_observation_id` and enforce current
  pointer updates transactionally in repository code.
- `triage_observation.disposition` is `completed`, `insufficient_input`, or
  `failed`; semantic outcome is nullable and checked against disposition;
  `attempt_id` is required only for model-completed/model-failed observations.
- A triage observation persists canonical supplied input JSON, validated output
  JSON when present, prompt hash, schema hash/version, task fingerprint,
  truncation/overflow facts, rationale, and failure category. Raw provider
  output remains on the attributable attempt's bounded detail JSON rather than
  being copied into mention rows.
- A model inspection persists exact configured/resolved model identity,
  routing fingerprint, supported-parameter facts, prompt/completion unit
  prices when usable, compatibility outcome, attempt/run provenance, and
  inspection time.
- Mentions have no `person_id`; exact/search names are non-unique; observation
  ordinal, fact local ID, and signal ordinal are unique only within their owner.
- Add `tests/people` to Pyright includes.

- [ ] Write migration tests from empty state and every retained schema version,
  including backup/checksum behavior already owned by foundation tests.
- [ ] Write schema tests for every enum/coupling constraint, FK rollback,
  immutable-history shape, namesake coexistence, mention/fact/signal ordering,
  and absence of a durable-person table or identifier.
- [ ] Run the tests and confirm migration 0004 is absent.
- [ ] Write the forward-only SQL with explicit indexes for source item, run,
  attempt, model/routing fingerprint, and current status queries.
- [ ] Run `uv run pytest tests/foundation/test_migrations.py tests/people/test_schema.py -q`.
- [ ] Commit with `feat(people): add detection persistence schema`.

## Task 6: Define the detection contract, prompt, and validator

**Files:**

- Create: `src/notable_person_finder/people/__init__.py`
- Create: `src/notable_person_finder/people/models.py`
- Create: `src/notable_person_finder/people/detection.py`
- Create: `src/notable_person_finder/people/prompts/detect_people.md`
- Create: `tests/people/test_detection.py`
- Create: `tests/people/fixtures/detect_people_valid.json`
- Create: `tests/people/fixtures/detect_people_invalid_references.json`

**Interfaces:**

- Produce strict Pydantic inputs `DetectionInput`, `DetectionPassage`, and
  domain-profile evidence models; outputs `DetectionOutput`,
  `DetectedMention`, `IdentityFact`, and `GroundedSignal` with the exact enums
  from the approved LLM design.
- Produce:

  ```python
  build_detection_input(source_item, feed, profile, config) -> DetectionInput
  detection_schema() -> dict[str, object]
  render_detection_request(value: DetectionInput) -> RenderedDetectionRequest
  validate_detection_output(raw: str, supplied: DetectionInput) -> DetectionOutput
  ```

- The rendered request is one reviewed system prompt plus canonical JSON user
  input. Hash prompt content and explicit schema version. No Jinja, tools,
  external knowledge, raw HTML, or article body.

- [ ] Write failing construction tests for nullable fields, numbered title and
  summary passages, deterministic truncation, UTF-8/canonical JSON bounds, and
  exact domain-profile version participation.
- [ ] Write failing output tests for valid zero/multi-person/mononym/uncertain
  results and rejection of unknown fields, unseen passage IDs, ungrounded names,
  duplicate local fact IDs, invalid signal references, contradictory item and
  mention outcomes, overflow inconsistencies, and mention-cap excess.
- [ ] Implement the strict models, prompt loader, canonical renderer, and
  full-domain validator. Validation errors must contain safe local identifiers,
  never source text or raw model output.
- [ ] Run `uv run pytest tests/people/test_detection.py -q` and `uv run pyright`.
- [ ] Commit with `feat(people): define the person detection contract`.

## Task 7: Implement the narrow OpenRouter adapter

**Files:**

- Create: `src/notable_person_finder/providers/openrouter.py`
- Create: `tests/people/test_openrouter.py`
- Create: `tests/people/fakes_openrouter.py`

**Interfaces:**

- Define application-owned frozen DTOs and `LlmClient` protocol:

  ```python
  inspect_model(request: ModelInspectionRequest) -> ModelInspectionResult
  generate_structured(request: StructuredGenerationRequest) -> StructuredGenerationResult
  ```

- `OpenRouterClient` wraps one SDK instance for one run. It uses
  `models.get(author, slug)` and `chat.send`, supplies strict JSON Schema,
  `require_parameters=True`, the configured privacy/routing preferences,
  metadata opt-in, non-streaming mode, and `max_completion_tokens`.
- Construct one SDK `RetryConfig` with strategy `none`; pass it at client and
  per-operation boundaries so a generated SDK default cannot re-enable retry.
- Translate all SDK exceptions into sanitized `ProviderFailure`, preserving
  status and bounded `Retry-After` when available. No response body, messages,
  schema, full endpoint query, or key enters failure detail.
- Parse pricing strings with `Decimal`; return exact per-token nano-USD values
  only when representable. Convert reported float cost via `Decimal(str(cost))`
  and explicit rounding; missing cost is `None`, never zero.

- [ ] Write fake-SDK contract tests for exact model splitting, capability and
  pricing extraction, strict request shape, same-model provider routing,
  retries disabled, timeout, response/refusal/finish reason, usage, request ID,
  configured/resolved model, provider metadata, and context-manager closure.
- [ ] Write table-driven exception tests for network, timeout, 400/401/402/403,
  408/429, 5xx, malformed response, retryability, and sanitized detail.
- [ ] Pin broken variants: remove per-call retry override, enable response
  healing, omit `require_parameters`, allow a model list/fallback model, and
  log an SDK response; each must fail a named test.
- [ ] Implement only the DTO translation. Domain output validation remains in
  `people.detection`.
- [ ] Run `uv run pytest tests/people/test_openrouter.py -q` and `uv run pyright`.
- [ ] Commit with `feat(providers): add the OpenRouter model adapter`.

## Task 8: Implement People repository operations

**Files:**

- Create: `src/notable_person_finder/people/repository.py`
- Create: `tests/people/test_repository.py`

**Interfaces:**

- Produce transaction-neutral operations for:
  - listing untriaged source items and loading one bounded source/feed record;
  - storing and loading a current-run model inspection by exact
    `(run_id, model_id, routing_fingerprint)`;
  - inserting `insufficient_input`, completed, and failed triage observations;
  - atomically updating the source item's current pointer;
  - inserting unresolved mentions, facts, and signals in validated order;
  - triage/status/digest aggregate queries; and
  - settling active `detect_people` work after permanent preflight failure.
- Repository functions never begin/commit when called from a handler
  settlement transaction; top-level scheduling helpers use their own brief
  transaction where required.

- [ ] Write failing real-SQLite tests for every operation, rollback on a child
  insert error, reusable current observation lookup, prospectively changed
  fingerprint, namesakes, failed observation coupling, aggregate counts, and
  permanent preflight propagation without generation attempts.
- [ ] Implement explicit feature-owned SQL and frozen dataclass return values.
- [ ] Run `uv run pytest tests/people/test_repository.py -q`.
- [ ] Commit with `feat(people): persist detection observations`.

## Task 9: Add model inspection work and readiness gating

**Files:**

- Create: `src/notable_person_finder/people/service.py`
- Create: `tests/people/test_inspection_service.py`
- Modify: `src/notable_person_finder/runs/engine.py` only if tests expose a
  missing generic readiness seam from Task 3.

**Interfaces:**

- Constants: `INSPECT_MODEL_TASK_TYPE = "inspect_model"`, provider
  `openrouter`, operation `inspect_model`, required priority `20`, pool `LLM`.
- Produce:

  ```python
  ensure_model_inspection(connection, *, run_id, config, now) -> int
  build_inspection_handler(connection, *, client, config) -> TaskHandler
  inspection_ready(connection, *, run_id, config) -> bool
  ```

- The inspection fingerprint includes run ID, exact model, routing/privacy
  fingerprint, adapter version, and capability contract version. Seeding
  supersedes older active inspection work so every run gets fresh evidence.
- Successful persistence unlocks detection readiness. Transient settlement
  leaves detection active and unclaimable; permanent auth/config/capability
  settlement writes failed triage observations and settles dependent detection
  work without generation attempts.

- [ ] Write failing tests for one inspection per needed model/run, no inspection
  when no usable untriaged item exists, no cross-run freshness reuse, success
  readiness, transient deferral, permanent propagation, pricing required only
  under a hard budget, unsupported strict output, and zero generation attempts
  before readiness.
- [ ] Implement the application-thread prepare/persist callbacks and pure
  worker execute callback. Inspection reserves zero because it is not a paid
  generation under this product contract.
- [ ] Run `uv run pytest tests/people/test_inspection_service.py tests/run_engine -q`.
- [ ] Commit with `feat(people): gate generation on model inspection`.

## Task 10: Schedule, execute, and persist detection

**Files:**

- Modify: `src/notable_person_finder/people/service.py`
- Create: `tests/people/test_detection_service.py`

**Interfaces:**

- Constants: `DETECT_PEOPLE_TASK_TYPE = "detect_people"`, operation
  `generate_structured`, required priority `30`, pool `LLM`.
- Produce:

  ```python
  schedule_source_items(connection, *, source_item_ids, run_id, config, profile, now) -> None
  seed_untriaged(connection, *, run_id, config, profile, now) -> None
  build_detection_handler(connection, *, client, config, profile) -> TaskHandler
  ```

- Scheduling writes `insufficient_input` immediately with no work/attempt. For
  usable items it schedules active detection idempotently and ensures current
  inspection work exists.
- Detection `ready` requires current-run exact inspection. Prepare loads source
  context and pricing, constructs the canonical request, and returns a
  `TaskPreparation` whose reservation is zero without a hard budget or the
  exact ceiling under a hard budget. Use checked/ceiling decimal arithmetic;
  reject missing/negative/overflow pricing before an attempt.
- Execute calls once, validates schema and domain references on the worker, and
  translates validation failure to
  `ProviderFailure(MALFORMED_RESPONSE, retryable=True)` without raw output in
  detail. Persist writes only prevalidated output. Final failure writes one
  failed triage observation; transient re-arm writes none.

- [ ] Write failing scheduling tests for backfill, newly named IDs,
  idempotency, changed material fingerprints, schedule-time empty input, and
  inspection creation.
- [ ] Write failing handler tests for exact context, dynamic reservation,
  no-hard-budget zero, valid zero/multiple/uncertain results, one malformed
  retry, second malformed permanent result, unseen references, budget refusal,
  worker closure containing no connection, atomic persistence, interruption,
  and reuse after successful persistence.
- [ ] Implement the service with module-level worker callbacks and bounded safe
  reasons. Never validate for the first time in `persist`.
- [ ] Run `uv run pytest tests/people/test_detection_service.py tests/run_engine -q`.
- [ ] Commit with `feat(people): detect unresolved person mentions`.

## Task 11: Connect ingestion to same-run detection

**Files:**

- Modify: `src/notable_person_finder/ingestion/models.py`
- Modify: `src/notable_person_finder/ingestion/service.py`
- Modify: `tests/ingestion/test_persistence.py`
- Modify: `tests/ingestion/test_service.py`
- Create: `tests/people/test_ingestion_seam.py`

**Interfaces:**

- Extend `FetchPersistResult` with ordered
  `created_source_item_ids: tuple[int, ...]`.
- Add optional injection to `build_fetch_handler`:

  ```python
  on_source_items: Callable[[tuple[int, ...], int, str], None] | None = None
  ```

- Ingestion invokes it on the application thread inside the successful
  settlement transaction after source-item inserts. Ingestion never imports
  `people`; the CLI injects `schedule_source_items`.

- [ ] Write failing ingestion tests proving only newly inserted IDs are
  returned, duplicates return none, callback ordering is deterministic, a
  callback failure rolls back both source items and feed settlement, and the
  absent callback preserves milestone 3a behavior.
- [ ] Write a seam test proving a feed result creates detection work (or an
  empty-input observation) in the same run.
- [ ] Implement the additive result/callback seam and run
  `uv run pytest tests/ingestion tests/people/test_ingestion_seam.py -q`.
- [ ] Commit with `feat(ingestion): notify downstream source-item work`.

## Task 12: Wire CLI lifecycle, two pools, reporting, and status

**Files:**

- Modify: `src/notable_person_finder/cli/main.py`
- Modify: `src/notable_person_finder/reporting/digest.py`
- Create: `tests/people/test_run_cli.py`
- Modify: `tests/run_engine/test_run_cli.py`
- Modify: `tests/ingestion/test_run_cli.py`

**Interfaces:**

- `command_run` creates the HTTP transport, one run-scoped OpenRouter client,
  and HTTP/LLM `BoundedScheduler`s in a context order that drains both pools
  before either provider client or SQLite closes.
- Register feed, inspection, and detection handlers; compose one seed hook for
  feed seeding plus People backfill; inject the ingestion callback.
- Add a frozen `PeopleRunSummary` consumed by digest rendering. Add source items
  triaged, three semantic outcomes, unresolved actionable mentions, overflow,
  insufficient input, model deferrals/failures, and configured/reserved/actual
  OpenRouter cost. `notable status` adds durable triage and untriaged counts.

- [ ] Write failing installed-interface tests over fake feed/OpenRouter clients:
  fresh run, existing backlog, zero/multiple mentions, empty input, one item
  malformed while siblings succeed, budget partial run/exit 2, second run
  reuse, digest counts, status counts, and redacted diagnostics.
- [ ] Write lifecycle tests proving both pools drain before clients close on
  success, provider failure, reporting failure, and unexpected worker error.
- [ ] Implement wiring and reporting without adding shortlist ranking or person
  identities.
- [ ] Run
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people -q`.
- [ ] Commit with `feat(cli): run feed ingestion and person detection`.

## Task 13: Own cross-component seams with mutation tests

**Files:**

- Create: `tests/people/test_seams.py`

**Method:** For each mutation below, first run a named test on unmodified code,
apply the broken variant, clear affected `__pycache__` directories, run the same
test and record its expected failure, restore, clear caches again, and rerun
green. A mutation that survives is a missing test, not evidence.

- [ ] Remove inspection readiness from detection claim routing.
- [ ] Use the fixed handler reservation instead of prepare-returned pricing.
- [ ] Send OpenRouter work to the HTTP pool or size both pools from one setting.
- [ ] Enable SDK default retries for inspection or generation.
- [ ] Validate unseen references only in `persist`.
- [ ] Convert valid `uncertain` into `MALFORMED_RESPONSE`.
- [ ] Persist empty input as `do_not_research` or create a fake attempt.
- [ ] Omit the ingestion downstream callback.
- [ ] Close either provider client while its pool still has a worker.
- [ ] Include raw model output or a synthetic secret in logs/failure detail.
- [ ] Commit the named tests and recorded mutation mapping with
  `test(people): pin model detection seams`.

## Task 14: Add the opt-in OpenRouter live smoke

**Files:**

- Create: `tests/people/test_live_openrouter.py`

**Interfaces:**

- Tests are marked `live`, require `OPENROUTER_API_KEY`, use the configured
  model/routing contract, and never use operator state.
- One test inspects capability/pricing; one sends the smallest strict schema
  generation and verifies identifiers, usage, request ID, and parseability.
- Missing credentials/network skip cleanly only before a request can be known
  to have reached OpenRouter. Auth, capability, schema, and returned-response
  failures do not skip.

- [ ] Write the live tests and prove ordinary
  `uv run pytest tests/people` deselects them.
- [ ] Run `uv run pytest tests/people -m live -v` with the configured key and
  record model, resolved provider when available, usage, cost, and outcome—but
  never prompt, output, or key—in the progress ledger.
- [ ] Commit with `test(people): add OpenRouter live smoke`.

## Task 15: Documentation, acceptance, and completion evidence

**Files:**

- Modify: `AGENTS.md`
- Modify: `CLAUDE.md`
- Modify: `docs/running.md`
- Modify: `docs/troubleshooting.md`
- Modify: `config/notable.example.toml` if acceptance finds stale copy
- Modify: `.env.example` if present

**Documentation:**

- Update both agent guides identically: gateway and detection are delivered;
  unresolved mentions intentionally have no durable person until 3b2; do not
  imply entity resolution, Wikipedia, coverage, ranking, or synthesis exists.
- Document model configuration, environment secret, hard budget behavior,
  preflight, partial runs, malformed output, unsupported capability, triage
  counters, status, and live smoke.
- Point at `docs/architecture/at-least-once-execution.md` rather than restating
  crash windows.

- [ ] Update documentation and Pyright/test configuration.
- [ ] Run the full automated gate below from the real 3b1 worktree.
- [ ] Run an installed fixture-backed `uv run notable ... run` on a fresh
  temporary root and record exit status, digest files, triage/mention counts,
  attempts, and status output.
- [ ] Scan the complete populated temporary root, logs, captured streams, and
  database text/blob values for two configured synthetic secret values; record
  zero matches without recording the values.
- [ ] Confirm every Task 13 mutation was observed red then restored green.
- [ ] Run `git diff --check` and inspect `git status --short` before committing.
- [ ] Commit with `docs: close model detection milestone`.

---

## Milestone Completion Gate

Automated and offline:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people
```

Manual and recorded:

```bash
uv run pytest tests/people -m live -v
```

Also required:

- the installed fixture-backed CLI acceptance run succeeds from a fresh
  temporary data root and writes a dated digest plus `latest.md`;
- binary-inclusive secret scans find no configured secret;
- all Task 13 mutations fail for their intended reason and restore green;
- the feature worktree passes `git diff --check` and is clean after the final
  commit;
- a whole-branch review finds no unresolved Critical or Important issue;
- fixes receive scoped re-review; and
- publishing/PR creation targets `refactor/rearchitecture`, never `main`, only
  under the user's existing authorization. Merge still requires a separate
  explicit user decision after review approval.

## Acceptance Criteria

- Existing and newly ingested untriaged usable source items become idempotently
  eligible for detection.
- Empty title and summary create `insufficient_input` at schedule time with no
  work item, attempt, reservation, or paid call.
- One item yields zero, one, or several independently traceable unresolved
  mentions; exact names, mononyms, and professional names are retained without
  invention or uniqueness assumptions.
- Every generation follows fresh exact-model inspection and, under a hard cap,
  an application-thread dynamic reservation derived from usable current
  pricing and configured bounds.
- Detection is never claimed before current-run readiness. Permanent preflight
  failure settles dependents without generation; transient failure leaves them
  active for an ordinary later run.
- HTTP and LLM calls use independent bounded pools and may overlap without any
  worker accessing SQLite.
- Strict schema and domain validation occurs in execute; invalid output permits
  at most one central malformed retry and cannot change domain state.
- Valid uncertainty succeeds without retry. Technical failure, truncation, or
  budget deferral never becomes a semantic negative.
- Every external call has one persisted attempt and SDK retries cannot bypass
  central accounting.
- One item's failure does not prevent sibling persistence or a truthful partial
  digest.
- Prompt, schema, input, configured/resolved model, serving provider when
  exposed, usage, cost, and evidence provenance remain recoverable.
- Secrets remain absent from every prohibited surface.
- The milestone creates no durable people, identity relations, Wikipedia work,
  coverage research, ranking, synthesis, drafting, or publication behavior.

## Deferred

- Milestone 3b2: durable people, sourced names, bounded person candidates,
  `resolve_person_entity`, entity-resolution observations, new-person creation,
  `possible_same_person`, reconsideration, confirmed merges, and canonical work
  reconciliation.
- Milestones 4–7 retain their approved Wikipedia, coverage, assessment/digest,
  Promptfoo, full-system verification, legacy comparison, and cutover scope.
