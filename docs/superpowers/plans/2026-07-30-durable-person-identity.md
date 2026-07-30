# Durable Person Identity and Entity Resolution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Track execution evidence in
> `.superpowers/sdd/2026-07-30-durable-person-identity/progress.md`, not in
> this plan.

**Goal:** Turn every eligible unresolved person mention into a durable
application person through bounded, evidence-grounded entity resolution —
including empty-candidate create, model resolution, possible-same-person edges,
reconsideration, confirmed merges, and operator-facing identity counters.

**Architecture:** Extend the existing `people/` package (not a new package).
Schedule-time deterministic create when candidates are empty (no work item, no
attempt — mirrors 3b1 `insufficient_input`). Model-path `resolve_person_entity`
work only when candidates exist. Edge-scoped `reconsider_person_entity` work for
material fingerprint changes on active `possible_same_person` relations.
Confirmed merge uses lower canonical id as survivor, never rewrites historical
FKs, and reconciles via operational projection. Multi-model inspection
parameterizes readiness by a task→model map so a fully-triaged corpus still
inspects the resolve model when resolution-eligible mentions remain.

**Tech Stack:** Python 3.13, SQLite, Pydantic 2, existing OpenRouter `LlmClient`
and dual worker pools, pytest, Ruff, Pyright, uv.

## Global Constraints

- Python 3.13 or newer; distribution `notable-person-finder`; import package
  `notable_person_finder`; executable `notable`.
- Work only under `src/notable_person_finder` and focused rewrite tests. Do not
  import, run, reorganize, or repair the retained prototype.
- The application never writes, drafts, or edits Wikipedia content.
- Every **external** call maps to exactly one persisted attempt. Deterministic
  terminals (empty-candidate create, skipped match_key) use no attempt.
- Only the central retry coordinator starts a repeat request. SDK retries stay
  disabled.
- No SQLite transaction spans network/model work; workers never open SQLite.
- Name equality never establishes identity. Candidate retrieval is name-gated;
  the model never searches.
- Secrets remain environment-only and never enter TOML values, snapshots,
  fingerprints, SQLite product records, logs, digests, terminal output, or tests.
- Money is integer nano-USD; UTC timestamps are canonical ISO-8601 text ending
  in `Z`; booleans/enums use checked columns.
- Migrations are forward-only, checksummed, transactional, and backed up before
  changing an existing database. **Never rewrite applied migration 0005** —
  add 0006+ if a column is missing.
- Plans remain authorities for interfaces and gates, not sources of canonical
  implementation fragments. Shipped code and tests govern.
- Keep this plan under ~1,500 lines. Reference code blocks are illustrative.
- Mutation evidence (Agents.md): for every locked behaviour this plan names,
  mutate the rule, confirm a **specific named** test fails, restore, and record
  which mutation killed which test. Negative assertions need a positive control.

## Authoritative Inputs

- Design (locks K1–K24):  
  `docs/superpowers/specs/2026-07-30-durable-person-identity-design.md`
- Cross-cutting: product workflow, domain persistence, LLM evaluation Task 2,
  model-gateway 3b1 design, at-least-once execution note.
- Code precedents: `people/service.py` (`_write_insufficient_input`,
  `ensure_model_inspection`, detection handler),  
  `people/repository.py` (`mechanical_search_name`,
  `settle_active_detect_people_after_permanent_preflight`),  
  `runs/engine.py` prepare/settle contracts.

## Locked Decisions (do not reopen)

| ID | Lock |
| --- | --- |
| K3 | Empty candidates ⇒ schedule-time create; no work item; no attempt; ER `created_new` |
| K4 | `uncertain` opens active edges to **all** code-supplied candidates |
| K7 | Merge survivor = lower canonical `person.id` |
| K9 | Mononyms eligible; empty `match_key` ⇒ disposition `skipped` |
| K14 | Single migration `0005_people_identity.sql` with full DDL in Task 1 |
| K16 | First-pass fingerprint **excludes** live candidate set |
| K17 | Txn re-check before empty create; peer-edge scan needs ≥1 non-name fact on a side |
| K18 | Auto-merge only on reconsider `same_person` + guardrails; name-only never merges |
| K21 | Edges opened in the same settlement as material attach do **not** schedule reconsider |
| K22 | Completed ER CHECKs branch on subject kind; reconsider never sets `created_person_id` |
| K23 | Permanent-preflight dependents settle out-of-band (generalize 3b1 settler) |
| K24 | Shared `is_resolution_eligible_mention` for seed, inspect, digest, status |

## File Structure

```text
src/notable_person_finder/
  db/migrations/0005_people_identity.sql     # NEW (full DDL)
  config/models.py                           # ResolvePersonEntityConfig
  config/loader.py                           # snapshot/fingerprint fields
  people/
    detection.py                             # unchanged contract
    identity.py                              # NEW: person create, projection, match_key, display name
    candidates.py                            # NEW: retrieval, scoring, peer scan
    resolution.py                            # NEW: I/O, schema, validate, fingerprint, prompt hash
    merge.py                                 # NEW: confirmed merge + work reconciliation
    models.py                                # resolution DTOs
    repository.py                            # ER/relation/person SQL; re-export match_key
    service.py                               # schedule, handlers, multi-model inspect, seed
    prompts/resolve_person_entity.md         # NEW
  reporting/digest.py                        # identity section
  cli/main.py                                # seed composition, handlers, status lines
config/notable.example.toml
tests/people/
  test_schema.py                             # extend 0005
  test_identity.py                           # NEW
  test_resolution_repository.py              # NEW (or extend test_repository)
  test_candidates.py                         # NEW
  test_resolution.py                         # NEW (contract/validate)
  test_inspection_service.py                 # multi-model + K23/K24
  test_resolution_service.py                 # NEW first-pass
  test_reconsideration_service.py            # NEW
  test_merge.py                              # NEW
  test_run_cli.py / test_seams.py            # wire + digest/status
  test_live_openrouter.py                    # opt-in resolve smoke
  fixtures/resolve_person_entity_*.json
```

## Public Interfaces (milestone surface)

```python
# people/identity.py (or repository re-export)
def match_key(value: str) -> str: ...
def collapse_whitespace(value: str) -> str: ...
def canonical_person_id(connection, person_id: int) -> int: ...
def person_id_closure_for_canonical(connection, canonical_id: int) -> tuple[int, ...]: ...
def mentions_for_canonical_person(connection, canonical_id: int) -> ...: ...
def create_person_for_mention(...) -> int: ...
def upsert_sourced_names(...) -> None: ...
def recompute_identity_fingerprint(connection, person_id: int) -> str: ...
def select_display_name(...) -> str: ...

# people/candidates.py
def retrieve_candidates(connection, *, mention_id, config) -> tuple[CandidatePerson, ...]: ...
def open_name_matched_peer_edges(...) -> int: ...

# people/resolution.py
RESOLUTION_SCHEMA_VERSION = 1
RESOLUTION_ADAPTER_VERSION = 1
def build_resolve_input(...) -> ResolvePersonEntityInput: ...
def resolution_schema() -> dict[str, object]: ...
def render_resolution_request(...) -> RenderedResolutionRequest: ...
def validate_resolution_output(raw: str, supplied: ResolvePersonEntityInput) -> ResolvePersonEntityOutput: ...
def first_pass_task_fingerprint(...) -> str: ...
def reconsider_task_fingerprint(...) -> str: ...

# people/merge.py
def confirm_person_merge(connection, *, person_id_a, person_id_b, run_id, observation_id, now) -> int: ...
def reconcile_digest_queue_on_merge(connection, *, survivor_id, loser_id, now) -> None: ...  # no-op

# people/service.py
RESOLVE_PERSON_ENTITY_TASK_TYPE = "resolve_person_entity"
RECONSIDER_PERSON_ENTITY_TASK_TYPE = "reconsider_person_entity"
RESOLVE_PERSON_PRIORITY = 40
SUBJECT_KIND_PERSON_MENTION = "person_mention"
SUBJECT_KIND_PERSON_RELATION = "person_relation"
SUBJECT_KIND_PERSON = "person"

def is_resolution_eligible_mention(...) -> bool: ...
def ensure_resolution_for_mention(...) -> str:  # created_new|scheduled|reused|skipped|ineligible
def schedule_resolution_for_observation(...) -> int: ...  # txn-neutral
def seed_unresolved_mentions(...) -> int: ...
def models_needed_for_run(...) -> tuple[str, ...]: ...
def ensure_model_inspections_for_run(...) -> int: ...
def inspection_ready(connection, *, run_id, model_id, config) -> bool: ...
def settle_active_tasks_after_permanent_preflight(...) -> int: ...
def build_resolution_handler(...) -> TaskHandler: ...
def build_reconsideration_handler(...) -> TaskHandler: ...
```

### Seed composition (CLI `_compose_seed` order)

```text
1. seed_feeds(...)
2. seed_untriaged(...)
3. seed_unresolved_mentions(...)
4. ensure_model_inspections_for_run(...)
```

### Config defaults

```toml
[tasks.resolve_person_entity]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_candidates = 8          # 1–16
max_facts_per_candidate = 12  # 1–32
max_names_per_candidate = 8   # 1–32
max_title_characters = 500
max_summary_characters = 4000

[tasks.resolve_person_entity.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"
```

---

## Task 1: Migration 0005 + identity helpers + person/name repository

**Files:**

- Create: `src/notable_person_finder/db/migrations/0005_people_identity.sql`
- Create: `src/notable_person_finder/people/identity.py`
- Modify: `src/notable_person_finder/people/repository.py` (re-export `match_key`; person/name writers)
- Modify: `tests/people/test_schema.py`
- Create: `tests/people/test_identity.py`
- Modify: `tests/foundation/test_migrations.py` if version chain asserts need updating

**Interfaces:**

- Full DDL from design §Schema outline: `person`, `sourced_name`, `person_relation`,
  `entity_resolution_observation`, `person_mention` columns `person_id` and
  `current_entity_resolution_observation_id`, indexes, ownership triggers.
- **No** `PRAGMA foreign_keys` in 0005; **no** `ALTER TABLE … ADD FOREIGN KEY`.
  Mutual `REFERENCES` between `person_relation` and `entity_resolution_observation`
  are legal at CREATE time (SQLite validates at DML).
- `match_key(value)` = `collapse_whitespace(mechanical_search_name(value)).casefold()`.
- Person create, sourced-name upsert, display-name preference (professional >
  display > alias > other > mononym; then longer exact_name; then latest
  `last_observed_at`; then lower id), `canonical_person_id`,
  `person_id_closure_for_canonical`, `mentions_for_canonical_person`,
  `identity_fingerprint` via operational projection.
- K22 dual-branch CHECK on completed ER rows.

**Steps:**

- [ ] **1.1** Write failing schema tests:
  - migrate empty → 0005; migrate 0004 fixture → 0005;
  - assert tables/columns/indexes exist;
  - assert migration SQL contains **no** `PRAGMA foreign_keys`;
  - `PRAGMA foreign_key_list` shows mutual REFERENCES both ways;
  - insert-order protocol: first-pass ER then relation; reconsider ER with
    `person_relation_id`; orphan `created_by_observation_id` fails;
  - namesakes: two people, same exact_name and match_key, both insert;
  - ownership trigger: mention cannot point `current_entity_resolution_observation_id`
    at a relation-scoped ER (`person_mention_id IS NULL`);
  - **K22 truth table inserts** for every first-pass completed outcome
    (`created_new`, `same_person`, `different_people`, `uncertain`), skipped,
    failed; and every reconsider completed outcome with
    `created_person_id IS NULL`; reject first-pass-style `created_person_id`
    on a relation-scoped row.
- [ ] **1.2** Run and confirm failure:  
  `uv run pytest tests/people/test_schema.py tests/people/test_identity.py -q`
- [ ] **1.3** Write `0005_people_identity.sql` exactly matching the design outline
  (CHECK clauses, unique partial indexes, mention ALTER columns, triggers
  mirroring 0004's current-pointer ownership pattern).
- [ ] **1.4** Implement `identity.py` + repository person/name writers. Keep
  repository functions transaction-neutral (join caller's open transaction).
- [ ] **1.5** Run  
  `uv run pytest tests/foundation/test_migrations.py tests/people/test_schema.py tests/people/test_identity.py -q`  
  and `uv run pyright`.
- [ ] **1.6** Commit: `feat(people): add durable person identity schema (0005)`.

---

## Task 2: Entity-resolution observation and relation writers

**Files:**

- Modify: `src/notable_person_finder/people/repository.py`
- Create or extend: `tests/people/test_resolution_repository.py`

**Interfaces:**

```python
def insert_entity_resolution_observation(...) -> int: ...
def load_er_by_mention_fingerprint(connection, person_mention_id, task_fingerprint) -> ...: ...
def load_er_by_relation_fingerprint(connection, person_relation_id, task_fingerprint) -> ...: ...
def point_mention_current_er(connection, person_mention_id, observation_id, person_id | None) -> None: ...
def upsert_active_possible_same_person(
    connection, *, person_id_a, person_id_b, run_id, created_by_observation_id, now
) -> int: ...  # store person_id_a < person_id_b; unique active edge
def dismiss_relation(...) -> None: ...
def supersede_relation(...) -> None: ...
def list_active_possible_same_person_for(connection, person_id) -> ...: ...
```

Insert protocol respects FK order. Reuse on unique-index conflict loads existing
row and does not create a second person. Failed insert requires non-null
`attempt_id`.

**Steps:**

- [ ] **2.1** Write failing tests for every disposition path (first-pass +
  reconsider), fingerprint reuse/point, active edge uniqueness, dismiss/
  supersede, and CHECK rejection of illegal combinations.
- [ ] **2.2** Implement writers; no handlers in this task.
- [ ] **2.3** Run `uv run pytest tests/people/test_resolution_repository.py tests/people/test_schema.py -q`.
- [ ] **2.4** Commit: `feat(people): persist entity-resolution observations and relations`.

---

## Task 3: Bounded name-gated candidate retrieval

**Files:**

- Create: `src/notable_person_finder/people/candidates.py`
- Create: `tests/people/test_candidates.py`

**Interfaces:**

```python
@dataclass(frozen=True, slots=True)
class CandidatePerson:
    person_id: int
    display_name: str
    score: int
    names: tuple[SourcedNameView, ...]
    facts: tuple[CandidateFactView, ...]  # local ids c{person_id}-f{n}

def retrieve_candidates(
    connection,
    *,
    person_mention_id: int,
    max_candidates: int,
    max_facts_per_candidate: int,
    max_names_per_candidate: int,
) -> tuple[CandidatePerson, ...]: ...
```

**Algorithm locks:**

- Query names: mention exact, search, and identity facts with `kind == 'name'`.
- SQL: canonical people only (`merged_into_person_id IS NULL`) with
  `match_key IN keys OR exact_name IN exact_names`. No fact-only candidates.
- Scoring (rank only, no drop floor): exact name +100; primary match_key +80;
  other match_key +60; non-name fact overlap +10 each cap +40; prior research
  mention +5. Tie-break `person_id ASC`. Top `max_candidates`.
- Facts from `mentions_for_canonical_person` (operational projection).
- Peer scan helper used later by service: name-gated peers excluding self and
  `reject_set`; edge only if K17 non-name eligibility holds.

**Steps:**

- [ ] **3.1** Write failing tests: name gate, scoring order, bounds, merged-away
  exclusion, post-merge projection scoring includes loser mention facts,
  no fact-only admission, stable ties.
- [ ] **3.2** Implement `candidates.py`.
- [ ] **3.3** Run `uv run pytest tests/people/test_candidates.py -q`.
- [ ] **3.4** Commit: `feat(people): add bounded name-gated candidate retrieval`.

---

## Task 4: Resolve contract, prompt, and config

**Files:**

- Create: `src/notable_person_finder/people/resolution.py`
- Create: `src/notable_person_finder/people/prompts/resolve_person_entity.md`
- Modify: `src/notable_person_finder/people/models.py`
- Modify: `src/notable_person_finder/config/models.py`
- Modify: `src/notable_person_finder/config/loader.py` (snapshot includes resolve task fields)
- Modify: `config/notable.example.toml`
- Modify: `tests/foundation/helpers.py`, `tests/foundation/test_config_models.py`,
  `tests/foundation/test_config_loader.py` as needed
- Create: `tests/people/test_resolution.py`
- Create: `tests/people/fixtures/resolve_person_entity_valid_same.json` (and
  different/uncertain/invalid as needed)

**Interfaces:**

```python
class ResolvePersonEntityInput(_StrictBoundaryModel):
    task: Literal["resolve_person_entity"]
    person_mention_id: int
    source_item_id: int
    exact_name: str
    search_name: str
    mention_outcome: Literal["research", "uncertain"]
    passages: tuple[DetectionPassage, ...]
    identity_facts: tuple[IdentityFact, ...]
    signals: tuple[GroundedSignal, ...]
    candidates: tuple[ResolveCandidate, ...]  # length 1..max_candidates
    max_candidates: int
    view: ResolveView

class ResolvePersonEntityOutput(_StrictBoundaryModel):
    outcome: Literal["same_person", "different_people", "uncertain"]
    selected_person_id: int | None
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str

class ResolvePersonEntityConfig(_StrictConfigurationModel):
    # mirrors DetectPeopleConfig + candidate bounds (see design defaults)
    ...

class TasksConfig(...):
    detect_people: DetectPeopleConfig = ...
    resolve_person_entity: ResolvePersonEntityConfig = ...
```

Validation (execute, before domain write):

- `same_person` ⇒ selected id non-null and ∈ supplied candidate ids.
- `different_people` / `uncertain` ⇒ selected null.
- Fact ids ⊆ mention local ∪ candidate fact ids.
- Rationale non-empty, length-bounded; `extra=forbid`; no JSON repair.

First-pass fingerprint fields **exactly** as design §First-pass material
fingerprints (exclude live candidates). Reconsider fingerprint includes both
sides' identity fingerprints + relation id + model/prompt/schema.

**Steps:**

- [ ] **4.1** Write failing config tests (bounds, model slug, snapshot
  fingerprint changes with resolve fields, secrets absent).
- [ ] **4.2** Write failing resolution contract tests (valid three outcomes,
  reject unseen selected id, empty rationale, unknown fields, fingerprint
  stability and candidate exclusion).
- [ ] **4.3** Implement models, `resolution.py`, prompt file, config, example TOML.
- [ ] **4.4** Run  
  `uv run pytest tests/foundation/test_config_models.py tests/foundation/test_config_loader.py tests/people/test_resolution.py -q`  
  and `uv run pyright`.
- [ ] **4.5** Commit: `feat(people): add resolve_person_entity contract and config`.

---

## Task 5: Multi-model inspection gating (K23/K24)

**Files:**

- Modify: `src/notable_person_finder/people/service.py`
- Modify: `src/notable_person_finder/people/repository.py` (generalized settler)
- Modify: `tests/people/test_inspection_service.py`
- Modify: `src/notable_person_finder/cli/main.py` (seed order later finalized in Task 6;
  this task may expose `ensure_model_inspections_for_run` and leave CLI on the
  new API if detection still works)

**Interfaces:**

```python
def is_resolution_eligible_mention(connection, *, person_mention_id, config, profile) -> bool:
    """K24: research/uncertain, non-empty match_key(exact_name), no ER at
    current material fingerprint (any disposition)."""

def models_needed_for_run(connection, run_id, config) -> tuple[str, ...]: ...
def ensure_model_inspections_for_run(connection, *, run_id, config, now) -> int: ...
def inspection_ready(connection, *, run_id, model_id, config) -> bool: ...
def settle_active_tasks_after_permanent_preflight(
    connection, *, run_id, attempt_id, model_inspection_id, model_id,
    failure_category, rationale, task_types, now, ...
) -> int: ...
```

**Behaviour:**

- Replace hard-coded detect-only early-return: inspect every model that has
  dependent work this run (usable untriaged / active detect; **or**
  K24-eligible mentions / active resolve/reconsider).
- One inspect work item per `(run_id, model_id, routing_fingerprint)`.
- Detection `ready` → detect model; resolve/reconsider `ready` → resolve model
  (handlers land in Tasks 6–7; readiness helpers exist now).
- Permanent inspect failure for model M: out-of-band settler fails mapped task
  types; for `resolve_person_entity` writes failed ER with **inspection**
  `attempt_id` (reuse on unique conflict); closes K24 for that fingerprint.
  Dependent `persist_failure` never runs on this path.
- Keep 3b1 detect preflight behaviour; generalize rather than fork.

**Steps:**

- [ ] **5.1** Write failing tests:
  - fully triaged corpus + K24-eligible mentions + distinct resolve model ⇒
    resolve model inspect scheduled;
  - corpus of only skipped + failed ER mentions ⇒ no resolve-model inspect;
  - permanent resolve-model preflight writes failed ER with inspection
    attempt_id and settles active resolve/reconsider work;
  - existing detect-only paths still pass.
- [ ] **5.2** Implement parameterization; keep backward-compatible wrappers if
  needed for current detection tests.
- [ ] **5.3** Run `uv run pytest tests/people/test_inspection_service.py -q`.
- [ ] **5.4** Commit: `feat(people): multi-model inspect readiness for detect and resolve`.

---

## Task 6: First-pass resolution (created_new + model path)

**Files:**

- Modify: `src/notable_person_finder/people/service.py`
- Modify: `src/notable_person_finder/cli/main.py`
- Create: `tests/people/test_resolution_service.py`
- Modify: `tests/people/test_detection_service.py` (atomic persist hook)
- Modify: `tests/people/test_run_cli.py`, `tests/people/test_seams.py` as needed

**Interfaces:**

```python
def ensure_resolution_for_mention(...) -> str: ...
def schedule_resolution_for_observation(...) -> int: ...  # no BEGIN/COMMIT
def seed_unresolved_mentions(...) -> int: ...
def build_resolution_handler(...) -> TaskHandler: ...
```

**`ensure_resolution_for_mention` algorithm** (hashes/fingerprint before any ER
insert) — implement exactly as design:

1. Load mention; outcome ∉ {research, uncertain} or empty exact_name →
   `ineligible` (no row).
2. Compute prompt/schema hashes + first-pass fingerprint.
3. Existing ER for `(mention, fingerprint)` any disposition → point current →
   `reused`.
4. Empty `match_key(exact_name)` → ER `skipped` → `skipped`.
5. Candidate retrieval.
6. Empty: txn re-check; still empty → create person + sourced names + link + ER
   `created_new` (`attempt_id` NULL, fixed validated JSON, peer-edge scan K17)
   → `created_new`; peers appeared → fall through to schedule.
7. Non-empty: supersede stale active resolve work; `schedule_work` priority 40
   → `scheduled`.

**Detection persist (atomic):** inside `_persist_detection_for` after
`insert_completed_observation`, call `schedule_resolution_for_observation`.
Failure rolls back entire settlement including triage. No nested commit.

**Handler (model path only):**

| Phase | Behaviour |
| --- | --- |
| `ready` | `inspection_ready(..., resolve_model)` |
| `prepare` | Load mention + candidates. If ≥1 candidates and still unresolved: build input, reservation. If empty / already linked / reusable ER: call `ensure_resolution_for_mention` (own brief txn if needed), then raise plain `ValueError` (optional log prefix `resolve_prepare_refused:`). Never `ProviderFailure` from prepare for this path. |
| `execute` | One `generate_structured`; validate; malformed → `ProviderFailure(MALFORMED_RESPONSE, retryable=True)`. |
| `persist` | Apply same/different/uncertain; peer-edge scan; **K21**: do not schedule reconsider for edges created in this settlement. |
| `persist_failure` | If ER already exists for fingerprint → no-op; else insert failed ER. |

Fixed handler fields: `provider=openrouter`, `operation=generate_structured`,
`pool=LLM`, priority 40.

**Outcomes:**

| Outcome | Effect |
| --- | --- |
| `same_person` | Link to selected (resolve to canonical if merged-away); upsert names; no new person |
| `different_people` | Create person; no edges to rejected candidates; peer scan outside reject set |
| `uncertain` | Create person; edges to **all** supplied candidates; peer scan may add others |

**do_not_research** mentions never enter resolution. Mononyms with
research/uncertain do.

**Steps:**

- [ ] **6.1** Write failing service tests covering design “Required test themes”
  for first-pass: empty create (zero work/attempts, call count 0), three model
  outcomes, unseen selected id rejected, name equality never skips model,
  empty match_key skipped, txn re-check, peer-edge K17, empty-at-prepare race
  (first-attempt and re-armed persist_failure no-op), fingerprint reuse,
  do_not_research ineligible, mononym eligible, detection persist atomicity
  (forced schedule failure rolls back triage), backfill seed, CLI registration.
- [ ] **6.2** Implement ensure/schedule/seed/handler and CLI wiring
  (`_compose_seed` order; register `resolve_person_entity` handler).
- [ ] **6.3** Intermediate gate:  
  `uv run pytest tests/people -q`  
  `uv run ruff check .` · `uv run pyright`
- [ ] **6.4** Mutation evidence for: empty create no attempt; K21 zero reconsider
  (when reconsider not yet built, assert no `reconsider_person_entity` work);
  detection rollback on schedule failure; peer-edge non-name filter.
- [ ] **6.5** Commit: `feat(people): first-pass resolve_person_entity and created_new`.

---

## Task 7: Reconsideration of possible_same_person edges

**Files:**

- Modify: `src/notable_person_finder/people/service.py`
- Create: `tests/people/test_reconsideration_service.py`

**Interfaces:**

```python
def maybe_schedule_reconsideration_for_person(
    connection, *, person_id, pre_existing_relation_ids, run_id, config, now
) -> int: ...
def build_reconsideration_handler(...) -> TaskHandler: ...
```

**Locks:**

- **K21:** After settlement attaches new material to canonical P, schedule
  reconsider only for active edges involving P that **existed before this
  settlement**. First-pass `uncertain` with N candidates ⇒ N edges and **zero**
  reconsider work items in that settlement. Later non-name fact attach does
  schedule.
- Subject = fingerprint-changed side's latest linked mention in operational
  projection; sole candidate = peer. If both sides change, lower person id is
  subject.
- Outcomes: `different_people` → dismiss; `uncertain` → leave active;
  `same_person` → attempt merge in Task 8 (this task may call into merge or
  leave a guarded hook). Prefer implementing guardrail check here and merge
  call in Task 8 if merge not yet landed — but Task 7 must write ER with
  `created_person_id IS NULL` for all reconsider completed outcomes (K22).
- Reconsider fingerprint includes both identity fingerprints + relation id.

**Steps:**

- [ ] **7.1** Write failing tests: K21 zero reconsider after first-pass uncertain;
  later material change schedules; subject = changed side; dismiss/uncertain;
  missing peer prepare-refuse pattern; relation-scoped failed ER.
- [ ] **7.2** Implement scheduling triggers and handler; register in CLI.
- [ ] **7.3** Run `uv run pytest tests/people/test_reconsideration_service.py tests/people/test_resolution_service.py -q`.
- [ ] **7.4** Commit: `feat(people): reconsideration of possible_same_person edges`.

---

## Task 8: Confirmed merges and canonical work reconciliation

**Files:**

- Create: `src/notable_person_finder/people/merge.py`
- Create: `tests/people/test_merge.py`
- Modify: `src/notable_person_finder/people/service.py` (reconsider persist calls merge)

**Interfaces:**

```python
def confirm_person_merge(
    connection, *, loser_id: int, survivor_id: int, run_id: int,
    observation_id: int | None, now: str,
) -> int:
    """Returns canonical survivor id. Open transaction required. Idempotent
    if loser already merged-away."""

def merge_guardrails_pass(...) -> bool: ...
def reconcile_digest_queue_on_merge(...) -> None:  # no-op until milestone 6
```

**Invariants:**

- Survivor = lower canonical id (K7); resolve redirects first.
- Do not delete loser; do not rewrite historical FKs on mentions or ER rows.
- Insert `person_relation(kind='merge', person_id_a=loser, person_id_b=survivor)`.
- Set `loser.merged_into_person_id = survivor`; flatten anyone pointing at loser.
- Upsert survivor sourced names from loser; recompute display name and
  identity_fingerprint via operational projection.
- Supersede active possible_same_person edges involving loser; re-link peers to
  survivor when peer ≠ survivor.
- Work reconciliation: supersede active work with `subject_kind=person` and
  `subject_id=loser`; supersede active `reconsider_person_entity` for relations
  involving loser; belt-and-braces pending resolve on loser-linked mentions.
- Guardrails (all required): relation still active possible_same_person; both
  canonical; supporting_fact_ids non-empty and valid for reconsider input
  namespace; each side operational projection has ≥1 non-name identity fact.
  Guardrail failure leaves edge active; records observation; does not re-arm
  until fingerprint changes.

**Steps:**

- [ ] **8.1** Write failing tests: merge FK stability; fingerprint includes loser
  facts; candidate scoring uses projection; status counts de-dupe canonical;
  relation-scoped work superseded; idempotent re-merge; guardrails block
  name-only; self-link/cycle rejected; digest-queue hook is no-op.
- [ ] **8.2** Implement `merge.py`; wire reconsider `same_person` path.
- [ ] **8.3** Run `uv run pytest tests/people/test_merge.py tests/people/test_reconsideration_service.py -q`.
- [ ] **8.4** Commit: `feat(people): confirmed merge and canonical reconciliation`.

---

## Task 9: Digest, status, CLI integration, live smoke, milestone gate

**Files:**

- Modify: `src/notable_person_finder/reporting/digest.py`
- Modify: `src/notable_person_finder/cli/main.py`
- Modify: `src/notable_person_finder/people/repository.py` (count helpers)
- Modify: `tests/people/test_run_cli.py`, `tests/people/test_seams.py`
- Modify: `tests/people/test_live_openrouter.py` (opt-in resolve smoke)
- Modify: `Agents.md` only if “What Is Actually Built” needs an accurate
  3b2 paragraph after completion (do not invent unfinished features)

**Digest section:**

```markdown
### Person identity
- People created this run: N
- Mentions resolved this run: N
- Linked same_person: N
- Created via different_people: N
- Created via created_new (no candidates): N
- Uncertain (possible same person): N
- Unresolved eligible mentions remaining: N   # K24 only
- Active possible_same_person relations (corpus): N
- Confirmed merges this run: N
- Resolution model deferred: N
- Resolution model permanently failed: N
```

OpenRouter cost remains the single shared budget line. Shortlist stays placeholder.

**`notable status` corpus counters:**

- canonical people (`merged_into_person_id IS NULL`);
- merged-away people (informational);
- unresolved eligible mentions (K24);
- active possible_same_person;
- mentions linked to people.

Still no digest backlog, queue tiers, or budget breakdown.

**Live smoke (deselected by default):** one inspect + one resolve generation
with fixture candidates when `OPENROUTER_API_KEY` is set.

**Steps:**

- [ ] **9.1** Write failing digest/status tests with positive controls for each
  new line; K24-only eligible counters (skipped/failed not counted as eligible).
- [ ] **9.2** Implement count helpers and rendering.
- [ ] **9.3** Full offline gate:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people
git diff --check
```

- [ ] **9.4** Mutation evidence ledger for all locked rules K3, K4, K7, K16–K18,
  K21–K24 and critical first-pass paths. Record killer test names in
  `.superpowers/sdd/2026-07-30-durable-person-identity/progress.md`.
- [ ] **9.5** Commit: `feat(people): identity counters in digest and status`.
- [ ] **9.6** Update design status to Implemented only if user requests; do not
  mark programme cutover.

---

## Completion Gate

### Offline (milestone close)

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people
```

### Intermediate (after Task 6)

```bash
uv run pytest tests/people -q
```

Must cover empty create, three model outcomes, backfill, detection-persist
atomicity, multi-model inspect when fully triaged.

### Live (opt-in, not default gate)

```bash
OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v
```

## Required Test Themes Checklist

Use this as the Task 9 self-review against the design:

- [ ] Migration 0004→0005; full DDL; no PRAGMA; mutual REFERENCES; insert order; orphan FK
- [ ] K22 CHECK first-pass + reconsider completed inserts; illegal reconsider create rejected
- [ ] Namesakes; no unique name constraint
- [ ] Empty candidates ⇒ person, zero work items, zero attempts, OpenRouter call count 0
- [ ] Empty match_key ⇒ skipped; K24 false thereafter
- [ ] Txn re-check schedules model path when peer appears
- [ ] Peer-edge scan: non-name eligibility; no name-only↔name-only peer-scan edges
- [ ] Empty-at-prepare race + re-armed persist_failure no-op
- [ ] Model path same/different/uncertain; unseen selected id rejected
- [ ] Name equality never skips model
- [ ] do_not_research never resolves; mononyms do
- [ ] Detection persist atomic with resolution schedule
- [ ] Multi-model inspect for resolve when fully triaged + K24 eligible
- [ ] Skipped+failed-only corpus ⇒ no resolve inspect
- [ ] Permanent resolve preflight: failed ER with inspection attempt_id; K24 closed
- [ ] K21: first-pass uncertain ⇒ N edges, zero reconsider work
- [ ] Reconsider subject = fingerprint-changed side
- [ ] Guardrails block name-only merge; allow with non-name facts
- [ ] Merge: FK stability, projection fingerprint/scoring, work supersession, idempotent
- [ ] Ownership triggers on current ER pointer
- [ ] Digest/status K24-only eligible counters; split created_new vs different_people
- [ ] Dual-pool LLM; workers never open SQLite
- [ ] Secrets absent from fingerprints / ER JSON / digests

## Spec Coverage Map

| Design section | Task(s) |
| --- | --- |
| Migration 0005 + schema + triggers + K22 | 1–2 |
| match_key, person, sourced names, projection | 1 |
| Candidate retrieval + scoring | 3 |
| Resolve I/O, prompt, fingerprint, config | 4 |
| Multi-model inspect, K23, K24 | 5 |
| ensure_resolution, created_new, model path, seed, detection hook | 6 |
| Reconsideration + K21 | 7 |
| Confirmed merge + work reconciliation + digest hook | 8 |
| Digest/status/live smoke/gate | 9 |

## Out of Scope (do not implement)

- MediaWiki / Wikipedia matching; Brave research; article fetch
- Lead assessment, ranking, shortlist synthesis, `notable audit *`
- Promptfoo suites, legacy JSONL migration, manual merge UI
- Engine per-attempt provider overrides or prepare-settle hooks
- Editing applied 0005; feature flags; reverse migrations

## Execution Notes

- Preferred branch: `feat/durable-person-identity` from
  `refactor/rearchitecture`, preferably in an isolated worktree.
- Track progress in
  `.superpowers/sdd/2026-07-30-durable-person-identity/progress.md`.
- Do not push, open PRs, or merge without explicit user authorization.
- Intermediate offline people-gate after Task 6; full programme gate after Task 9.
