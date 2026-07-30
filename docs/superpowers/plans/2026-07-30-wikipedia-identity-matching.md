# Wikipedia Identity Matching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Track execution evidence in
> `.superpowers/sdd/2026-07-30-wikipedia-identity-matching/progress.md`, not in
> this plan.

**Goal:** Turn every eligible durable canonical person into an inspectable
Wikipedia identity judgment — mechanical MediaWiki search and page-fact
retrieval, deterministic empty-complete-search `no_matching_page_found`, and
when biography candidates exist one focused OpenRouter comparison — with
current-pointer attachment, refresh, merge reconciliation, and operator-facing
Wikipedia counters.

**Architecture:** New `wikipedia/` capability package (K1) plus
`providers/mediawiki.py`. Three work-item kinds only (K2):
`mediawiki_search` and `mediawiki_page_facts` on the HTTP pool (priority 50,
provider `mediawiki`, no OpenRouter budget), and `match_wikipedia_identity` on
the LLM pool (priority 55). Durable person-scoped query plans drive multi-call
retrieval without multi-call execute. Deterministic empty complete search
mirrors 3b2 empty-candidate create / 3b1 `insufficient_input`. Multi-model
inspection extends 3b2 so the match model is armed for K17-eligible people,
active match work, **and** active Wikipedia plans / MediaWiki HTTP work (K21),
with mid-run `ensure_model_inspections_for_run` when match is scheduled.
People merge calls a thin Wikipedia reconcile hook (K16).

**Tech Stack:** Python 3.13, SQLite, Pydantic 2, shared `HttpTransport` /
`PacingGate` / dual worker pools, existing OpenRouter `LlmClient`, pytest,
Ruff, Pyright, uv.

## Global Constraints

- Python 3.13 or newer; distribution `notable-person-finder`; import package
  `notable_person_finder`; executable `notable`.
- Work only under `src/notable_person_finder` and focused rewrite tests. Do not
  import, run, reorganize, or repair the retained prototype.
- **The application never writes, drafts, or edits Wikipedia content.**
- Every **external** call maps to exactly one persisted attempt. Deterministic
  terminals (empty complete safe search, local assembly failures) use no
  attempt and no match work item.
- Exactly **one external call per `execute`**. Only three handlers (K2). Fixed
  `provider`/`operation` per handler; no per-attempt override.
- Only the central retry coordinator starts a repeat request. HTTPX / SDK
  retries stay disabled.
- No SQLite transaction spans network/model work; workers never open SQLite.
- MediaWiki attempts are HTTP: **no OpenRouter USD budget reservation** (K13).
  Match-model generations reserve under the existing OpenRouter hard cap.
- Names, spelling similarity, edit distance, and category “biography scores”
  never establish identity (K7). They may order candidates only.
- Secrets remain environment-only. MediaWiki public API needs **no** auth
  secret. Secrets never enter TOML values, snapshots, fingerprints, SQLite
  product records, logs, digests, terminal output, or tests.
- Money is integer nano-USD; UTC timestamps are canonical ISO-8601 text ending
  in `Z`; booleans/enums use checked columns.
- Migrations are forward-only, checksummed, transactional, and backed up before
  changing an existing database. **Never rewrite applied migration 0006** —
  add 0007+ if a column is missing (K14).
- Plans remain authorities for interfaces and gates, not sources of canonical
  implementation fragments. Shipped code and tests govern. Full column lists
  and CHECK prose live in design §Observation model and DDL sketch — this plan
  lists critical tables, CHECKs, and uniques only.
- Keep this plan under ~1,500 lines. Reference code blocks are illustrative.
- Mutation evidence (Agents.md): for every locked behaviour this plan names,
  mutate the rule, confirm a **specific named** test fails, restore, and record
  which mutation killed which test. Negative assertions need a positive
  control. Use `cp` backup + `diff` restore (never `git stash`);
  `PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__` between mutation runs.

## Authoritative Inputs

- Design (locks K1–K27, full):  
  `docs/superpowers/specs/2026-07-30-wikipedia-identity-matching-design.md`
- Cross-cutting: product workflow (lifecycle table), domain persistence
  (MediaWiki pages / Wikipedia identity observations), provider adapters
  (`MediaWikiClient`), LLM-evaluation Task 3, durable person identity 3b2,
  model-gateway 3b1 dual pools / prepare-execute-settle, at-least-once note.
- Style precedent:  
  `docs/superpowers/plans/2026-07-30-durable-person-identity.md`
- Code precedents: `people/service.py` (multi-model inspect, empty-create,
  handlers), `people/repository.py` (`settle_active_tasks_after_permanent_preflight`),
  `people/merge.py` (work supersession), `providers/feeds.py` (HTTP adapter
  boundary: never leak bodies into `ProviderFailure.detail`),
  `providers/pacing.py` (`mediawiki` key already present), `runs/engine.py`
  (`TaskHandler` one-call execute).

## Locked Decisions (do not reopen)

| ID | Lock |
| --- | --- |
| K1 | New `wikipedia/` package; do not fold into `people/` |
| K2 | Three work-item kinds only: `mediawiki_search`, `mediawiki_page_facts`, `match_wikipedia_identity` |
| K3 | Terminal identity subject is always canonical `person`; mentions never Wikipedia subjects |
| K4 | Empty complete bounded search ⇒ deterministic `no_matching_page_found`; no match work; no model attempt |
| K5 | Unsafe truncation never yields `no_matching_page_found`; empty+unsafe → failed; non-empty+unsafe → model may match/uncertain only (reject model `no_matching_page`) |
| K6 | Model schema short outcomes (`matching_page` / `no_matching_page` / `uncertain`); persist product outcomes (`*_found` / `uncertain_identity`) |
| K7 | Names / similarity / biography category scores never establish identity; no nickname map |
| K8 | Disambiguation pages and non-main-namespace pages never biography candidates; redirects followed via multi-wave facts |
| K9 | Reuse/skip only same person entity + material fingerprint (+ refresh); never by name across people |
| K10 | Refresh after `refresh_interval` for **every** completed semantic outcome; material change also re-arms |
| K11 | Only `matching_page_found` will suppress coverage (m5); m4 reports identity state only — no coverage-skipped counters |
| K12 | Priority: feed 10 → inspect 20 → detect 30 → resolve/reconsider 40 → MediaWiki HTTP 50 → match 55 |
| K13 | MediaWiki: no OpenRouter USD budget; match model: reserve under hard cap |
| K14 | Single migration `0006_wikipedia_identity.sql` with full DDL in Task 1; never edit 0006 later |
| K15 | Durable person-scoped query plan under material fingerprint; two-phase accent (K23) |
| K16 | Merge never copies loser’s Wikipedia current pointer; supersede loser work; ensure survivor |
| K17 | Shared `is_wikipedia_match_eligible`; successive refreshes re-anchor `refresh_of` on **current** completed observation; forward material match only |
| K18 | Permanent match-model preflight settles active `match_wikipedia_identity` out-of-band (failed obs with inspection `attempt_id`) |
| K19 | At most one active plan per `(person, material_fingerprint)`; supersede on fingerprint change |
| K20 | No second model call for uncertainty in v1 |
| K21 | Match-model inspect when K17 eligible **or** active match work **or** active plan/HTTP work; mid-run ensure when scheduling match |
| K22 | Partial form permanent fail: form-level only; non-empty candidates still → match with `partial_retrieval`; empty + form fail → failed not no-match |
| K23 | Two-phase query plan: primary forms at open; accent only if primaries complete with zero main-ns non-dab hits |
| K24 | Multi-wave page facts for redirect terminals; hop/page budgets; exhaustion → `truncated_unsafe_for_negative` |
| K25 | Current pointer only for `disposition='completed'`; failed never sets/moves pointer |
| K26 | Domain `failure_category` vocabulary separate from `ProviderFailure.category` enum |
| K27 | Operational projection for all person-derived Wikipedia inputs (closure + mentions helpers) |

## File Structure

```text
src/notable_person_finder/
  db/migrations/0006_wikipedia_identity.sql   # NEW (full DDL — K14)
  providers/mediawiki.py                      # NEW: MediaWikiClient
  wikipedia/                                  # NEW package
    __init__.py
    queries.py                                # mechanical query forms
    candidates.py                             # filters, rank, bounds, truncation flags
    matching.py                               # Task 3 I/O, schema, validate, hashes
    models.py                                 # domain DTOs
    repository.py                             # pages, searches, plans, batches, observations
    service.py                                # seed, ensure, handlers, maybe_advance_plan
    merge_hooks.py                            # reconcile_on_merge
    prompts/match_wikipedia_identity.md
  config/models.py                            # MediaWikiConfig, MatchWikipediaIdentityConfig
  config/loader.py                            # snapshot/fingerprint fields
  people/service.py                           # schedule_wikipedia_after_person_ready hooks
  people/merge.py                             # call wikipedia merge hook
  people/repository.py                        # extend permanent-preflight settler (K18)
  reporting/digest.py                         # Wikipedia identity section
  cli/main.py                                 # handlers, seed order, status lines
config/notable.example.toml
tests/wikipedia/                              # NEW package tests
  __init__.py
  conftest.py
  fakes_mediawiki.py
  test_schema.py
  test_repository.py
  test_mediawiki.py
  test_queries.py
  test_candidates.py
  test_matching.py
  test_http_service.py                        # search/facts handlers + maybe_advance_plan
  test_match_service.py                       # match handler, K21, K18
  test_eligibility.py                         # K17 refresh / material
  test_seed_and_hooks.py
  test_merge_hooks.py
  test_run_cli.py
  test_seams.py
  test_digest_status.py
  test_live_mediawiki.py                      # opt-in
  test_live_openrouter_match.py               # opt-in
  fixtures/match_wikipedia_identity_*.json
  fixtures/mediawiki_*.json                   # search / pageprops responses
tests/foundation/                             # config model/loader + migration chain
tests/people/                                 # extend only where people call sites change
```

## Public Interfaces

```python
# providers/mediawiki.py
PROVIDER = "mediawiki"
OPERATION_SEARCH_PAGES = "search_pages"
OPERATION_GET_PAGE_FACTS = "get_page_facts"

class MediaWikiClient(Protocol):
    def search_pages(
        self, query: str, *, continuation: str | None
    ) -> MediaWikiSearchPage: ...
    def get_page_facts(
        self, page_ids: Sequence[int]
    ) -> MediaWikiPageFactsBatch: ...

class HttpxMediaWikiClient:
    def __init__(self, transport: HttpTransport, *, config: MediaWikiConfig, ...): ...

# wikipedia/queries.py
QUERY_PLAN_VERSION = 1
def generate_primary_query_forms(names: Sequence[str], *, max_forms: int) -> tuple[QueryFormSpec, ...]: ...
def generate_accent_fallback_forms(
    primary_query_texts: Sequence[str], *, existing_query_texts: set[str], remaining_budget: int
) -> tuple[QueryFormSpec, ...]: ...

# wikipedia/candidates.py
@dataclass(frozen=True, slots=True)
class BiographyCandidate: ...
@dataclass(frozen=True, slots=True)
class AssemblyResult:
    candidates: tuple[BiographyCandidate, ...]
    uncapped_count: int
    truncated_unsafe_for_negative: bool
    partial_retrieval: bool
    failure_category_if_empty: str | None  # domain category or None if safe empty

def assemble_biography_candidates(...) -> AssemblyResult: ...

# wikipedia/matching.py
MATCH_SCHEMA_VERSION = 1
WIKIPEDIA_ADAPTER_VERSION = 1
def build_match_input(...) -> MatchWikipediaIdentityInput: ...
def match_schema() -> dict[str, object]: ...
def render_match_request(...) -> RenderedMatchRequest: ...
def validate_match_output(
    raw: str, supplied: MatchWikipediaIdentityInput, *, truncated_unsafe_for_negative: bool
) -> MatchWikipediaIdentityOutput: ...
def map_model_outcome_to_semantic(outcome: str) -> str: ...  # K6
def base_material_fingerprint(person_view, config, *, refresh_of_observation_id: int | None) -> str: ...
def observation_matches_live_material(obs, person_view, config, plan_refresh_of) -> bool: ...

# wikipedia/models.py — MatchWikipediaIdentityInput/Output, MatchWikiCandidate, MatchView, ...

# wikipedia/repository.py
def upsert_mediawiki_page(...) -> int: ...
def insert_or_load_search_observation_by_attempt(...) -> int: ...  # UNIQUE(attempt_id)
def insert_search_hits(...) -> None: ...
def open_plan(...) -> int: ...
def insert_query_forms(...) -> None: ...
def insert_page_facts_batch(...) -> int: ...
def mark_batch_completed(..., attempt_id: int) -> None: ...
def insert_wikipedia_identity_observation(...) -> int: ...
def point_person_current_wikipedia_observation(...) -> None:  # completed only (K25)
def load_active_plan_for_fingerprint(...) -> ...: ...
def supersede_plan(...) -> None: ...

# wikipedia/service.py
MEDIAWIKI_SEARCH_TASK_TYPE = "mediawiki_search"
MEDIAWIKI_PAGE_FACTS_TASK_TYPE = "mediawiki_page_facts"
MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE = "match_wikipedia_identity"
SUBJECT_KIND_WIKIPEDIA_QUERY_FORM = "wikipedia_query_form"
SUBJECT_KIND_WIKIPEDIA_PAGE_FACTS_BATCH = "wikipedia_page_facts_batch"
SUBJECT_KIND_PERSON = "person"  # reuse people constant if already exported
MEDIAWIKI_HTTP_PRIORITY = 50
MATCH_WIKIPEDIA_PRIORITY = 55

def is_wikipedia_match_eligible(connection, *, person_id, config, now) -> bool: ...  # K17
def ensure_wikipedia_identity(...) -> str: ...  # scheduled|reused|ineligible|completed_empty|...
def seed_wikipedia_identity(...) -> int: ...
def schedule_wikipedia_after_person_ready(...) -> None: ...  # txn-neutral join
def maybe_advance_plan(connection, *, plan_id, run_id, config, now) -> None: ...
def build_mediawiki_search_handler(...) -> TaskHandler: ...
def build_mediawiki_page_facts_handler(...) -> TaskHandler: ...
def build_match_wikipedia_handler(...) -> TaskHandler: ...
def wikipedia_match_model_needed(connection, run_id, config) -> bool: ...  # K21

# wikipedia/merge_hooks.py
def reconcile_on_merge(connection, *, survivor_id, loser_id, run_id, config, now) -> None: ...
```

### Seed composition (CLI `_compose_seed` order)

```text
1. seed_feeds(...)
2. seed_untriaged(...)
3. seed_unresolved_mentions(...)
4. seed_wikipedia_identity(...)          # NEW
5. ensure_model_inspections_for_run(...) # includes match model when K21
```

### Config defaults

```toml
[mediawiki]
endpoint = "https://en.wikipedia.org/w/api.php"
maxlag_seconds = 5

[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_candidates = 8                    # 1–16
max_query_forms = 6                   # 1–16
search_srlimit = 10                   # 1–50
max_continuations_per_form = 1        # 0–5
max_search_hits_per_form = 20         # 1–100
max_page_ids_per_facts_request = 20   # 1–50
max_redirect_hops = 3                 # 1–5
max_fact_pages_per_plan = 40          # 1–100
max_extract_characters = 1200
max_categories_per_page = 20
max_names_in_prompt = 8
max_facts_in_prompt = 16
refresh_interval_hours = 720          # 30 days
max_title_characters = 500
max_summary_characters = 4000

[tasks.match_wikipedia_identity.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"
```

`PacingConfig.mediawiki_min_interval_ms` already defaults to 900. No new secrets.

### Material fingerprint fields (normative)

`base_material_fingerprint` = `sha256(canonical_json({...}))` with exactly the
fields listed in design §Fingerprints: `task="wikipedia_identity"`,
`adapter_version`, `person_id`, `identity_fingerprint`, `query_plan_version`,
match model + generation parameters + all bound ints that affect Task 3 input
or candidate text, `prompt_hash`, `schema_hash`, `schema_version`,
`refresh_of_observation_id`. **No reverse-hash API** — forward recompute via
plan `refresh_of` only (K17).

### Work-item fingerprints

| Task | Fingerprint inputs |
| --- | --- |
| `mediawiki_search` | plan material fp + `query_form_id` + continuation token / step |
| `mediawiki_page_facts` | plan material fp + batch id (or sorted page_ids + wave) |
| `match_wikipedia_identity` | plan material fp (person subject); live candidate IDs not in fp |

---

## Task 1: Migration 0006 + repository DDL/writers + schema tests

**Files:**

- Create: `src/notable_person_finder/db/migrations/0006_wikipedia_identity.sql`
- Create: `src/notable_person_finder/wikipedia/__init__.py`
- Create: `src/notable_person_finder/wikipedia/repository.py`
- Create: `src/notable_person_finder/wikipedia/models.py` (minimal row DTOs if needed)
- Create: `tests/wikipedia/__init__.py`, `conftest.py`, `test_schema.py`,
  `test_repository.py`
- Modify: `tests/foundation/test_migrations.py` if version chain asserts need updating

**Interfaces / DDL surface (critical — full columns in design §DDL):**

| Table | Critical constraints |
| --- | --- |
| `mediawiki_page` | `UNIQUE (wiki_id, page_id)`; `wiki_id` default enwiki space |
| `wikipedia_identity_plan` | status ∈ retrieving/ready_for_match/completed/failed/superseded; partial UNIQUE active `(person_id, material_fingerprint)` WHERE status ∈ (retrieving, ready_for_match); `truncated_unsafe_for_negative`, `partial_retrieval` 0/1 |
| `wikipedia_query_form` | variant ∈ exact/comma_swap/accent_fallback; status pending/completed/failed; `UNIQUE (plan_id, ordinal)` |
| `wikipedia_page_facts_batch` | subject for facts work; wave ≥ 1; status pending/completed/failed/superseded; `UNIQUE (plan_id, ordinal)`; **UNIQUE attempt_id** (partial WHERE NOT NULL) |
| `mediawiki_search_observation` | attempt_id NOT NULL; **UNIQUE (attempt_id)**; FK (attempt_id, run_id) → attempt |
| `mediawiki_search_hit` | `UNIQUE (search_observation_id, rank)` |
| `wikipedia_identity_observation` | disposition completed/failed; semantic outcomes product vocabulary; UNIQUE `(person_id, task_fingerprint)`; failed CHECK: attempt NOT NULL **or** domain local categories (`unsafe_truncation`, `partial_retrieval_empty`, `redirect_budget_exhausted`); completed truth table per design K25/K6 |
| `person` | `ALTER … ADD current_wikipedia_identity_observation_id` + ownership trigger: pointed obs must match person_id and `disposition='completed'` |

- **No** `PRAGMA foreign_keys` in 0006; **no** reverse migration.
- Repository writers are transaction-neutral (join caller’s open transaction).
- Insert-or-load search observation by `attempt_id`; never double-insert hits
  for one attempt.

**Steps:**

- [ ] **1.1** Write failing schema tests:
  - migrate empty → 0006; migrate 0005 fixture → 0006;
  - assert all tables/columns/indexes exist including `wikipedia_page_facts_batch`
    and search `UNIQUE(attempt_id)`;
  - assert migration SQL contains **no** `PRAGMA foreign_keys`;
  - ownership trigger rejects pointing current at failed obs or wrong person;
  - completed outcome truth-table inserts (deterministic no-match attempt NULL;
    model matching/no-match/uncertain with attempt+inspection);
  - failed CHECK matrix: attempt null only for local domain categories; reject
    failed with null attempt for `permanent_provider`;
  - partial unique active plan: second retrieving plan same fp fails;
  - search observation double-insert same attempt_id loads one row.
- [ ] **1.2** Run and confirm failure:  
  `uv run pytest tests/wikipedia/test_schema.py tests/wikipedia/test_repository.py -q`
- [ ] **1.3** Write `0006_wikipedia_identity.sql` matching design §DDL (CHECK
  clauses, uniques, person ALTER, ownership triggers mirroring 0004/0005
  current-pointer pattern). **Do not leave any Task-later column out of 0006.**
- [ ] **1.4** Implement repository writers for pages, plans, forms, batches,
  search observations/hits, identity observations, pointer updates.
- [ ] **1.5** Run  
  `uv run pytest tests/foundation/test_migrations.py tests/wikipedia/test_schema.py tests/wikipedia/test_repository.py -q`  
  and `uv run pyright`.
- [ ] **1.6** Commit: `feat(wikipedia): add Wikipedia identity schema (0006)`.

**Mutation evidence (named rules):** delete `UNIQUE(attempt_id)` on search obs →
named uniqueness test fails; relax failed attempt CHECK → local-null category
matrix test fails.

---

## Task 2: MediaWiki adapter (`providers/mediawiki.py`)

**Files:**

- Create: `src/notable_person_finder/providers/mediawiki.py`
- Modify: `src/notable_person_finder/config/models.py` (`MediaWikiConfig` on
  `MainConfig`)
- Modify: `src/notable_person_finder/config/loader.py` (snapshot includes
  mediawiki endpoint/maxlag)
- Modify: `config/notable.example.toml`
- Create: `tests/wikipedia/test_mediawiki.py`, fixtures under
  `tests/wikipedia/fixtures/`
- Modify: `tests/foundation/test_config_models.py`,
  `tests/foundation/test_config_loader.py` as needed

**Interfaces:**

```python
@dataclass(frozen=True, slots=True)
class MediaWikiSearchHit:
    page_id: int | None
    title: str
    # snippet/timestamp optional as available

@dataclass(frozen=True, slots=True)
class MediaWikiSearchPage:
    hits: tuple[MediaWikiSearchHit, ...]
    continuation: str | None
    more_results: bool
    provider_total_hits: int | None

@dataclass(frozen=True, slots=True)
class MediaWikiPageFact:
    page_id: int
    requested_title: str | None
    canonical_title: str
    namespace: int
    missing: bool
    redirect_to_page_id: int | None
    redirect_to_title: str | None
    is_disambiguation: bool
    canonical_url: str
    description: str | None
    extract: str | None
    categories: tuple[str, ...]

@dataclass(frozen=True, slots=True)
class MediaWikiPageFactsBatch:
    pages: tuple[MediaWikiPageFact, ...]
```

**Behaviour locks:**

- Only `providers/` may import httpx for MediaWiki. Exactly one Action API
  request per method call.
- Endpoint: configurable HTTPS public URL, no query/fragment (same family as
  OpenRouter endpoint validator). Default
  `https://en.wikipedia.org/w/api.php`. `maxlag` on every request.
- User agent: `transport.resolved_user_agent(version)` — same app UA as feeds.
- No auth secret; provider key `"mediawiki"` for pacing/pause.
- `search_pages`: application supplies `srlimit` from config; adapter returns
  hits + continuation completeness; **never** decides workflow completeness.
- `get_page_facts`: caller-bounded page ID list; missing/redirects first-class;
  description/categories optional context only (not identity gates).
- Failure classification → existing `FailureCategory`; honor Retry-After /
  maxlag 503; `ProviderFailure.detail` = type name / compact codes only — never
  response bodies or full query strings (feeds precedent).
- Adapter does not filter namespaces, dabs, or empty-search outcomes.

**Steps:**

- [ ] **2.1** Write failing adapter tests with fixtures: search hits +
  continuation; redirects; namespaces; dab props; missing pages; maxlag/rate
  limit; malformed JSON; size limit path; detail redaction positive control
  (forced error with body does not appear in detail).
- [ ] **2.2** Write failing config tests: endpoint HTTPS/no query; maxlag
  bounds; snapshot fingerprint changes with mediawiki fields; no secret fields.
- [ ] **2.3** Implement `MediaWikiConfig`, loader snapshot fields, example TOML,
  `HttpxMediaWikiClient`.
- [ ] **2.4** Run  
  `uv run pytest tests/wikipedia/test_mediawiki.py tests/foundation/test_config_models.py tests/foundation/test_config_loader.py -q`  
  and `uv run pyright`.
- [ ] **2.5** Commit: `feat(providers): MediaWikiClient search_pages and get_page_facts`.

---

## Task 3: Query plan generation + candidate assembly (pure logic)

**Files:**

- Create: `src/notable_person_finder/wikipedia/queries.py`
- Create: `src/notable_person_finder/wikipedia/candidates.py`
- Create: `tests/wikipedia/test_queries.py`, `tests/wikipedia/test_candidates.py`

**Interfaces / algorithms:**

**Query forms (name-derived only; K27 names from operational projection
supplied by caller):**

| Variant | Phase | Rule |
| --- | --- | --- |
| `exact` | Primary | Honorific-stripped exact/search forms; whitespace-collapsed; useful casing preserved for API string |
| `comma_swap` | Primary | If `Last, First...` → `First... Last` |
| `accent_fallback` | Secondary | Unicode accent-stripped of primary query texts; never identity evidence |

- Deduplicate identical query strings; cap at `max_query_forms`.
- **Forbidden:** nickname maps; model-invented aliases; initials expansion;
  queries from non-name facts only; pre-inserting accent at plan open.
- Pure functions: no SQLite, no HTTP.

**Candidate assembly (fixture page/search rows):**

1. Union hits by page ID (or title resolved through facts).
2. Walk redirect edges up to `max_redirect_hops`; terminal must be fetched or
   budget exhausted (K24).
3. Drop missing, namespace ≠ 0, `is_disambiguation`.
4. Dedupe by terminal page ID; rank by best (lowest) search rank then lower
   page ID; keep top `max_candidates`.
5. Set `truncated_unsafe_for_negative` per design K5 table (pagination,
   hit-cap, `uncapped_count > max_candidates`, redirect budget exhaustion,
   `partial_retrieval`).
6. **No** biography category score gate.

**Steps:**

- [ ] **3.1** Write failing query tests: honorific/comma_swap table; no nickname
  path (positive control: map-like input not expanded); accent generator only
  when called; max_forms cap; dedupe.
- [ ] **3.2** Write failing assembly tests: dab/namespace drop; redirect chain
  length 1 and 2; hop budget drops trail; dedupe/cap/rank; uncapped >
  max_candidates ⇒ truncated_unsafe; partial_retrieval flag plumbing.
- [ ] **3.3** Implement pure modules.
- [ ] **3.4** Run `uv run pytest tests/wikipedia/test_queries.py tests/wikipedia/test_candidates.py -q`.
- [ ] **3.5** Commit: `feat(wikipedia): query forms and candidate assembly`.

**Mutation evidence:** remove dab filter → dab-exclusion test fails; always
insert accent in primary generator → “primaries only” test fails.

---

## Task 4: Match contract, prompt, config, outcome mapping, validation

**Files:**

- Create: `src/notable_person_finder/wikipedia/matching.py`
- Create: `src/notable_person_finder/wikipedia/prompts/match_wikipedia_identity.md`
- Modify: `src/notable_person_finder/wikipedia/models.py`
- Modify: `src/notable_person_finder/config/models.py` (`MatchWikipediaIdentityConfig`,
  `TasksConfig`)
- Modify: `src/notable_person_finder/config/loader.py`
- Modify: `config/notable.example.toml`
- Create: `tests/wikipedia/test_matching.py` + fixtures
- Extend foundation config tests for new task bounds

**Interfaces:**

```python
class MatchWikipediaIdentityInput(_StrictBoundaryModel):
    task: Literal["match_wikipedia_identity"]
    person_id: int
    display_name: str
    sourced_names: tuple[MatchName, ...]
    identity_facts: tuple[MatchFact, ...]  # local ids f1, f2, ...
    candidates: tuple[MatchWikiCandidate, ...]  # length 1..max_candidates
    max_candidates: int
    view: MatchView  # truncated_unsafe_for_negative, partial_retrieval, ...

class MatchWikipediaIdentityOutput(_StrictBoundaryModel):
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str

class MatchWikipediaIdentityConfig(_StrictConfigurationModel):
    # model slug + all bounds in Config defaults above
    ...

class TasksConfig(...):
    detect_people: DetectPeopleConfig = ...
    resolve_person_entity: ResolvePersonEntityConfig = ...
    match_wikipedia_identity: MatchWikipediaIdentityConfig = ...
```

**Validation (execute, before domain write):**

- `matching_page` ⇒ `selected_page_id` ∈ supplied candidate page IDs.
- `no_matching_page` / `uncertain` ⇒ selected null.
- Fact ids ⊆ supplied local ids; rationale non-empty, length-bounded;
  `extra=forbid`; no JSON repair.
- If `truncated_unsafe_for_negative`: `no_matching_page` is **invalid** (K5).
- K6 mapping unit-tested:

| Model | Stored semantic_outcome |
| --- | --- |
| matching_page | matching_page_found |
| no_matching_page | no_matching_page_found |
| uncertain | uncertain_identity |

**Fingerprint helpers** in this task: `base_material_fingerprint`,
`observation_matches_live_material` (forward only). Prompt/schema hash stable
functions.

**Steps:**

- [ ] **4.1** Write failing config tests (bounds, model slug, snapshot changes
  with match fields, secrets absent, refresh_interval_hours positive).
- [ ] **4.2** Write failing matching tests: three valid outcomes; reject unseen
  page ID; empty rationale; unknown fields; truncated rejects no_matching_page;
  K6 mapping table; fingerprint stability and field completeness (mutate one
  bound → fp changes).
- [ ] **4.3** Implement models, `matching.py`, prompt, config, example TOML.
- [ ] **4.4** Run  
  `uv run pytest tests/wikipedia/test_matching.py tests/foundation/test_config_models.py tests/foundation/test_config_loader.py -q`  
  and `uv run pyright`.
- [ ] **4.5** Commit: `feat(wikipedia): match_wikipedia_identity contract and config`.

---

## Task 5: HTTP handlers — search, page facts, `maybe_advance_plan`

**Files:**

- Create / expand: `src/notable_person_finder/wikipedia/service.py`
- Expand: `src/notable_person_finder/wikipedia/repository.py` as needed
- Create: `tests/wikipedia/test_http_service.py`
- CLI registration of HTTP handlers may wait until Task 7–8 if tests invoke
  handlers directly (preferred for intermediate gate).

**Handler contracts:**

| Task | Pool | provider | operation | Subject | Priority |
| --- | --- | --- | --- | --- | --- |
| `mediawiki_search` | HTTP | `mediawiki` | `search_pages` | `wikipedia_query_form` | 50 |
| `mediawiki_page_facts` | HTTP | `mediawiki` | `get_page_facts` | `wikipedia_page_facts_batch` | 50 |

**`mediawiki_search` phases:**

| Phase | Behaviour |
| --- | --- |
| prepare | Load form + continuation; refuse if form not pending / plan superseded (`ValueError`) |
| execute | Exactly one `client.search_pages(query, continuation=...)`; **no** budget reserve |
| persist | Insert-or-load search obs by `UNIQUE(attempt_id)` + hits; update form counters; schedule next continuation if within bounds; if form complete → `maybe_advance_plan` |
| persist_failure | Permanent → mark **form** `failed` (K22); `maybe_advance_plan`; never write `no_matching_page_found` |

**`mediawiki_page_facts` phases:**

| Phase | Behaviour |
| --- | --- |
| prepare | Load batch; refuse if superseded |
| execute | Exactly one `client.get_page_facts(page_ids)` |
| persist | Upsert `mediawiki_page`; mark batch completed + attempt_id unique; if redirect targets unfetched within budget → new wave batches + schedule; else `maybe_advance_plan` |
| persist_failure | Permanent → batch `failed`; `maybe_advance_plan` |

**`maybe_advance_plan` ordering (load-bearing — implement exactly as design):**

1. Terminal plan statuses → return.
2. Any form `pending` → return.
3. Facts for known hits on completed forms (wave 1 batches) if needed.
4. Any facts batch pending → return.
5. Redirect waves (K24); budget exhaust → `plan.truncated_unsafe_for_negative = 1`.
6. Accent phase (K23) only when all **primary** forms terminal, primary facts
   quiescent, accent not yet decided: skip if any main-ns non-dab reachable;
   else insert accent forms under budget and schedule search; else skip.
7. All forms terminal + facts quiescent → assemble:
   - empty + complete safe → deterministic `no_matching_page_found`; plan
     `completed`; set current pointer (K4/K25); **zero** match work; **zero**
     OpenRouter calls; `attempt_id`/`model_inspection_id` NULL; fixed
     validated empty JSON payload (same family as 3b2 `created_new` fixed
     output); prompt/schema hashes may be null on this path;
   - empty + (truncated_unsafe OR partial_retrieval OR redirect budget) → failed
     obs (attempt NULL allowed); plan `failed`; no pointer move; domain
     `failure_category` distinguishes `unsafe_truncation` /
     `partial_retrieval_empty` / `redirect_budget_exhausted`;
   - non-empty → plan `ready_for_match`; flags set; schedule
     `match_wikipedia_identity`; **always** call
     `ensure_model_inspections_for_run` on the application thread (K21b).
     Until Task 6 extends `models_needed_for_run`, the call is a no-op for the
     match model — Task 6 must add a regression that advance + K21 arming
     schedules match-model inspect in the same run (cold-start test).

**Plan open (used by ensure in Task 7, may land helpers here):** primary forms
only; no accent rows.

**Superseded plan:** in-flight persist no-ops domain writes when plan is
`superseded`.

**Steps:**

- [ ] **5.1** Write failing service tests (fixture MediaWiki client, fake clock
  optional):
  - multi-form search + continuation bound;
  - multi-wave redirect facts (chain 1 and 2);
  - hop/page budget exhaustion empty path → failed not no-match;
  - empty complete search ⇒ no_match obs, zero match work, OpenRouter call
    count 0; zero-hit plans skip facts batches;
  - truncated empty ⇒ failed, attempt_id NULL;
  - K22: one form permanent fail + candidates from other form ⇒ match
    scheduled with partial_retrieval; empty + form fail ⇒ failed;
  - K23: primaries with main-ns non-dab hits never insert accent; empty
    primaries insert accent once under max_query_forms;
  - K24 redirect wave schedules second batch;
  - double settle same attempt_id does not duplicate hits;
  - superseded plan persist no-op.
- [ ] **5.2** Implement handlers + `maybe_advance_plan` + scheduling helpers.
- [ ] **5.3** Intermediate gate:  
  `uv run pytest tests/wikipedia/test_http_service.py tests/wikipedia/test_schema.py tests/wikipedia/test_repository.py tests/wikipedia/test_queries.py tests/wikipedia/test_candidates.py -q`  
  `uv run ruff check .` · `uv run pyright`
- [ ] **5.4** Mutation evidence: K4 (force schedule match on empty safe) →
  zero-match-work test fails; K5 (write no_match on truncated empty) →
  truncated-empty test fails; K22 (fail whole plan on one form fail with
  candidates) → partial_retrieval match test fails; K23 (always accent) →
  accent-skip test fails.
- [ ] **5.5** Commit: `feat(wikipedia): mediawiki_search and page_facts handlers`.

---

## Task 6: Match handler + K21 inspection + K18 preflight settler

**Files:**

- Modify: `src/notable_person_finder/wikipedia/service.py`
- Modify: `src/notable_person_finder/people/service.py`
  (`models_needed_for_run`, `task_types_for_model`,
  `ensure_model_inspections_for_run` consumers)
- Modify: `src/notable_person_finder/people/repository.py`
  (`settle_active_tasks_after_permanent_preflight` supports
  `match_wikipedia_identity`)
- Create: `tests/wikipedia/test_match_service.py`
- Modify: `tests/people/test_inspection_service.py` for multi-model Wikipedia arming

**`match_wikipedia_identity` phases:**

| Phase | Behaviour |
| --- | --- |
| ready | `inspection_ready(..., match_model)` |
| prepare | Load operational projection (K27) + plan candidates; empty race → deterministic path then `ValueError` refuse; existing obs for fingerprint → reuse/point then refuse; build Task 3 input; prepare-returned nano-USD reservation |
| execute | One `generate_structured`; validate; unseen page ID / truncated+`no_matching_page` → `ProviderFailure(MALFORMED_RESPONSE, retryable=True)` (≤1 retry policy via coordinator) |
| persist | Map outcomes (K6); insert observation; set current pointer (K25); plan `completed` |
| persist_failure | Idempotent failed obs for fingerprint; plan `failed` if not terminal; **no-op** if completed exists; do not move current pointer |

Fixed: `provider=openrouter`, `operation=generate_structured`, `pool=LLM`,
priority 55.

**K21 inspection arming:**

```text
wikipedia_match_model_needed =
  has_wikipedia_match_eligible_people   # K17
  OR has_active_match_wikipedia_work
  OR has_active_wikipedia_plan          # retrieving | ready_for_match
  OR has_active_mediawiki_wikipedia_http_work
```

Extend `models_needed_for_run` / `task_types_for_model` so match model maps to
`match_wikipedia_identity` only. HTTP kinds have **no** model ready gate.

**K18:** permanent preflight for match model fails active match work
out-of-band: failed Wikipedia observation with **inspection** `attempt_id`,
`failure_category='permanent_preflight'`, plan `failed`, pointer unchanged.
Reuse on unique fingerprint conflict. Dependent `persist_failure` never runs
on this path.

**Cold-start required test:** seed (or ensure) opens only MediaWiki HTTP → after
facts, match work **and** match-model inspection both present; match becomes
ready same run. `maybe_advance_plan` must call
`ensure_model_inspections_for_run` when scheduling match.

**Steps:**

- [ ] **6.1** Write failing tests:
  - three model outcomes → pointer set; rationale stored;
  - unseen page ID rejected;
  - truncated_unsafe rejects model `no_matching_page` (invalid → retry then
    permanent failed obs);
  - uncertain sets pointer (never “suppress” claims);
  - cold-start inspect (K21);
  - multi-model inspect when only Wikipedia backlog remains;
  - permanent match preflight: failed obs with inspection attempt_id; plan
    failed; prior completed pointer unchanged;
  - empty-at-prepare race + re-armed persist_failure no-op;
  - budget exhaustion defers match only (MediaWiki unaffected) if testable
    with budget fake.
- [ ] **6.2** Implement match handler; wire K21 into people inspect APIs; extend
  preflight settler; ensure mid-run inspect from `maybe_advance_plan`.
- [ ] **6.3** Run  
  `uv run pytest tests/wikipedia/test_match_service.py tests/wikipedia/test_http_service.py tests/people/test_inspection_service.py -q`  
  and `uv run pyright`.
- [ ] **6.4** Mutation evidence: remove active-plan arm from
  `wikipedia_match_model_needed` → cold-start inspect test fails; allow
  preflight settler to set current pointer → K25 preflight test fails.
- [ ] **6.5** Commit: `feat(wikipedia): match handler and multi-model inspect gating`.

---

## Task 7: Seed, ensure hooks, merge reconciliation (K16, K17, K27)

**Files:**

- Modify: `src/notable_person_finder/wikipedia/service.py` (`ensure`, `seed`,
  eligibility)
- Create: `src/notable_person_finder/wikipedia/merge_hooks.py`
- Modify: `src/notable_person_finder/people/merge.py` (call reconcile hook after
  work reconcile / fingerprint update)
- Modify: `src/notable_person_finder/people/service.py` (call
  `schedule_wikipedia_after_person_ready` on every mandatory path)
- Create: `tests/wikipedia/test_eligibility.py`, `test_seed_and_hooks.py`,
  `test_merge_hooks.py`
- Extend people resolution/merge tests only as needed for hooks (thin asserts)

**`is_wikipedia_match_eligible` — implement design algorithm verbatim (K17):**

- Merged-away → false.
- No operational sourced_name with non-empty `match_key` → false.
- Branch 1: current completed still matches live material → false until
  `refresh_interval` elapses; then live_fp with `refresh_of=cur.id`; eligible if
  no terminal obs and no active plan for that fp.
- Branch 2: material changed / no matching current → base_fp work; failed
  terminal for base_fp closes eligibility (no time refresh from failed — K25);
  completed base without pointer follows interval + refresh_of=that id;
  active plan for base_fp → false; else true.
- **Double/triple refresh clock test:** O0 → R1 (`refresh_of=O0`) → R2
  (`refresh_of=R1`) under fixed person+config material.

**`ensure_wikipedia_identity`:** same branches to choose `live_fp` /
`refresh_of`; open plan with primary forms only (K19 supersede stale active
plans for other fps); schedule search work; return
`scheduled|reused|ineligible|…`.

**Mandatory `schedule_wikipedia_after_person_ready` call sites** (txn join):

| People path | When |
| --- | --- |
| Empty-candidate `created_new` | After person create + fingerprint |
| First-pass `same_person` link | After link + names + fingerprint recompute |
| First-pass `different_people` / `uncertain` create | After create + fingerprint |
| Reconsider confirmed merge | Via `reconcile_on_merge` → ensure survivor (K16); not on loser |
| Peer-edge / non-name material attach that changes fingerprint | After recompute when fingerprint string changes |
| Run seed | `seed_wikipedia_identity` for all eligible people |

Do **not** schedule from `do_not_research` mentions that never created a person.
K27: query forms, fingerprints, match input names/facts use
`canonical_person_id`, `person_id_closure_for_canonical`,
`mentions_for_canonical_person` — never merge-blind `person_id = canonical` only.

**`reconcile_on_merge` (K16):**

1. Supersede active match work for loser person subject; search work for forms
   under loser plans; facts work for batches under loser plans.
2. Mark loser active plans `superseded`.
3. **Do not** copy loser `current_wikipedia_identity_observation_id` to survivor.
4. `ensure_wikipedia_identity(survivor)` under combined identity fingerprint
   (txn-neutral join). Historical loser observations remain immutable.

**Steps:**

- [ ] **7.1** Write failing eligibility tests: all K17 branches; double/triple
  refresh re-anchor; failed does not time-refresh; material/config change
  re-arms base_fp; namesake people no shared mapping (K9).
- [ ] **7.2** Write failing seed/hook tests: each mandatory people call site
  schedules Wikipedia when eligible; fingerprint-unchanged ensure is no-op;
  do_not_research without person does not schedule; backfill seed.
- [ ] **7.3** Write failing merge tests: loser work/plans superseded; survivor
  ensure scheduled; pointer not copied; post-merge projection includes loser
  mention facts in match input assembly.
- [ ] **7.4** Implement eligibility, ensure, seed, hooks, merge call site.
- [ ] **7.5** Run  
  `uv run pytest tests/wikipedia/test_eligibility.py tests/wikipedia/test_seed_and_hooks.py tests/wikipedia/test_merge_hooks.py tests/people/test_merge.py tests/people/test_resolution_service.py -q`
- [ ] **7.6** Mutation evidence: copy loser pointer in merge → K16 test fails;
  eligibility uses reverse-hash → double-refresh test fails; miss
  `same_person` hook → that site’s test fails.
- [ ] **7.7** Commit: `feat(wikipedia): seed, person-ready hooks, and merge reconcile`.

---

## Task 8: Digest/status counters + CLI wire + offline gate + live smokes

**Files:**

- Modify: `src/notable_person_finder/reporting/digest.py`
- Modify: `src/notable_person_finder/cli/main.py` (handlers map, `_compose_seed`,
  status lines)
- Expand: counter helpers in `wikipedia/repository.py` or `service.py`
- Create/modify: `tests/wikipedia/test_digest_status.py`, `test_run_cli.py`,
  `test_seams.py`
- Create: `tests/wikipedia/test_live_mediawiki.py`,
  `tests/wikipedia/test_live_openrouter_match.py` (mark `live`, deselected by
  default)
- Update `Agents.md` “What Is Actually Built” only if user requests accuracy
  pass after merge — do not invent unfinished features

**Digest section (exact lines / meanings):**

```markdown
### Wikipedia identity
- Matching page found (this run): N
- No matching page (this run): N
- Uncertain identity (this run): N
- Deterministic no-match (empty complete search): N
- People with current matching page (corpus): N
- People with current no-match (corpus): N
- People with current uncertain identity (corpus): N
- Wikipedia eligible remaining: N
- Canonical people without Wikipedia pointer: N
- MediaWiki work deferred: N
- MediaWiki work permanently failed: N
- Match model deferred: N
- Match model permanently failed: N
```

- **Wikipedia eligible remaining** = count where `is_wikipedia_match_eligible`
  (includes refresh-due people who already have a current completed obs).
- **Canonical people without Wikipedia pointer** =
  `merged_into_person_id IS NULL AND current_wikipedia_identity_observation_id IS NULL`.
- Do **not** add coverage-skipped lines (K11).
- OpenRouter cost remains the single shared budget line. Shortlist stays
  placeholder.

**`notable status` corpus lines** (same definitions as digest when 0006
present): current matching / no-match / uncertain counts; eligible remaining;
without pointer. Still no digest backlog, queue tiers, or budget breakdown on
status.

**CLI:**

- Register three Wikipedia handlers on `notable run`.
- Seed order: feeds → untriaged → unresolved mentions → **wikipedia** →
  inspections.
- Construct `HttpxMediaWikiClient` with run-scoped transport like feeds.

**Live smokes (opt-in, not default gate):**

```bash
uv run pytest tests/wikipedia -m live -v
OPENROUTER_API_KEY=… uv run pytest tests/wikipedia -m live -v
```

MediaWiki smoke: one public search + page facts (no secret). Match smoke:
inspect + one generation with fixture candidates when key present. Record
model/provider/usage/cost/outcome without secrets when run.

**Steps:**

- [ ] **8.1** Write failing digest/status tests with positive controls for each
  new line; assert absence of coverage-skipped wording with a control that
  would fail if the string were added.
- [ ] **8.2** Implement counters, digest, status, full CLI wire.
- [ ] **8.3** Full offline gate:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia
git diff --check
```

- [ ] **8.4** Mutation evidence ledger for K1–K27 critical rules (at minimum
  K2/K4/K5/K6/K9/K16/K17/K18/K21–K25). Record killer test names in
  `.superpowers/sdd/2026-07-30-wikipedia-identity-matching/progress.md`.
- [ ] **8.5** Commit: `feat(wikipedia): digest, status, and CLI integration`.
- [ ] **8.6** Do not mark design Implemented or programme cutover unless the
  user requests.

---

## Completion Gate

### Offline (milestone close)

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia
git diff --check
```

### Intermediate (after Task 5)

```bash
uv run pytest tests/wikipedia/test_http_service.py tests/wikipedia/test_schema.py \
  tests/wikipedia/test_repository.py tests/wikipedia/test_queries.py \
  tests/wikipedia/test_candidates.py -q
```

### Intermediate (after Task 6)

```bash
uv run pytest tests/wikipedia tests/people/test_inspection_service.py -q \
  --ignore=tests/wikipedia/test_live_mediawiki.py \
  --ignore=tests/wikipedia/test_live_openrouter_match.py
```

(or rely on default deselect of `live`)

### Live (opt-in, not default gate)

```bash
uv run pytest tests/wikipedia -m live -v
OPENROUTER_API_KEY=… uv run pytest tests/wikipedia -m live -v
```

## Required Test Themes Checklist

Use this as the Task 8 self-review against the design:

- [ ] Migration 0005→0006; full DDL including `wikipedia_page_facts_batch` and
  search `UNIQUE(attempt_id)`; person pointer ownership; failed CHECK matrix
- [ ] MediaWiki adapter fixtures; no secret; detail redaction; maxlag
- [ ] Query generation: honorific/comma_swap; no nickname map; two-phase accent
- [ ] Candidate assembly: dab/ns drop; multi-wave redirects; hop budget; cap ⇒
  truncated_unsafe
- [ ] Empty complete search ⇒ no_match, zero match work, zero OpenRouter calls
- [ ] Truncated/partial/redirect-empty ⇒ failed not no_match; attempt NULL local
- [ ] K22 partial form fail with candidates ⇒ match + partial_retrieval
- [ ] Model path three outcomes; K6 mapping; unseen page ID rejected; K5 reject
  model no_match when unsafe
- [ ] K17 eligibility + double/triple refresh re-anchor; failed no time-refresh
- [ ] K9 namesakes; no name-scoped reuse
- [ ] Seed backfill; every mandatory person-ready call site
- [ ] Priorities 50/55; dual pools; workers never open SQLite; MediaWiki no USD
- [ ] K21 cold-start inspect + mid-run ensure when scheduling match
- [ ] K18 permanent match preflight; pointer unchanged
- [ ] K16 merge supersession + survivor ensure; no pointer copy
- [ ] Interrupt/idempotency: double settle same attempt_id
- [ ] Digest/status aligned counters; no coverage-skipped lines
- [ ] Mutation evidence per Agents.md for locked rules
- [ ] Application never edits Wikipedia (boundary: no write API in adapter)

## Spec Coverage Map

| Design section | Task(s) |
| --- | --- |
| Migration 0006 + schema + triggers + K14/K25/K26 CHECKs | 1 |
| MediaWiki adapter + MediaWikiConfig | 2 |
| Query plan generation + candidate assembly + K5/K8/K23/K24 pure logic | 3 |
| Match I/O, prompt, fingerprint, config, K6 | 4 |
| HTTP handlers, plan lifecycle, empty no_match, K22/K23/K24 service | 5 |
| Match handler, K21 inspect, K18 preflight | 6 |
| Eligibility K17, seed, person hooks, merge K16/K27 | 7 |
| Digest/status/CLI/live smoke/gate | 8 |

## Non-goals / Out of scope

- Brave Web Search, article fetch/extract, coverage plans (milestone 5).
- Lead assessment, ranking, digest shortlist synthesis, digest queue, audit
  commands (milestone 6).
- Promptfoo suites, full live verification matrix, legacy comparison, cutover
  (milestone 7).
- Automated Wikipedia edits, drafts, or notability verdicts.
- Nickname maps; biography category scores as identity gates; name-only page
  reuse across people.
- Migrating legacy JSONL Wikipedia indexes.
- Engine extensions for multi-call execute, per-attempt provider override, or
  prepare-time network I/O.
- Claiming coverage research was “skipped” as an operational count.
- Editing applied 0006; feature flags; reverse migrations.
- Importing or wrapping prototype modules.

## Execution Notes

- Preferred branch: `feat/wikipedia-identity-matching` from
  `refactor/rearchitecture`, preferably in an isolated worktree.
- Track progress in
  `.superpowers/sdd/2026-07-30-wikipedia-identity-matching/progress.md`.
- PR decomposition may follow design PR plan (PR1=Task1 … PR8=Task8);
  migration ships complete in the first PR; no later edit of 0006.
- Do not push, open PRs, or merge without explicit user authorization.
- Intermediate offline wikipedia gates after Tasks 5 and 6; full programme gate
  after Task 8.
- Point at `docs/architecture/at-least-once-execution.md` for crash windows;
  do not restate them in code comments or digests.

## Self-review (plan author)

- **K1–K27:** each lock appears in Global Constraints, Locked Decisions table,
  and at least one task interface or required test theme.
- **Placeholder scan:** no TBD/TODO/“fill in later” for policy; implementers
  follow design §DDL for full column lists, this plan for critical CHECKs and
  algorithms.
- **Type consistency:** three task type strings, subject kinds, priorities
  50/55, product vs model outcome names, domain failure categories, and seed
  order match the design public interfaces section.
- **Three handlers only; never edit Wikipedia; one external call per execute;
  MediaWiki no OpenRouter budget; UNIQUE(attempt_id) search obs; page facts
  batch subject; progress ledger separate from this plan.**
