# Coverage Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. Track execution evidence in
> `.superpowers/sdd/2026-07-30-coverage-evidence/progress.md`, not in this plan.

**Goal:** Turn every eligible durable canonical person without a matching
English Wikipedia page into inspectable **coverage evidence** — bounded Brave
Web Search plans, deterministic source-policy screening, shared canonical
articles, article fetch + Trafilatura extraction (never raw HTML), and
immutable person–article assessments via OpenRouter Task 4 `assess_article` —
with Wikipedia-gate stop/supersede, merge reconciliation, and truthful
coverage counters.

**Architecture:** New `coverage/` capability package (K1) plus
`providers/brave.py` and `providers/articles.py`. Three work-item kinds only
(K2): `brave_web_search` and `fetch_article` on the HTTP pool (priorities 60 /
65), and `assess_article` on the LLM pool (priority 70). Extract runs
**in-process** after fetch and is not a work item (K3). Durable person-scoped
coverage plans drive multi-call retrieval without multi-call execute. Wikipedia
`matching_page_found` stops eligibility and supersedes mid-flight work (K5).
Multi-model inspection arms the assess model for eligible people, active assess
work, or active coverage plan/HTTP work (K20), with mid-run ensure when assess
is scheduled. People merge calls a thin coverage reconcile hook (K18).

**Tech Stack:** Python 3.13, SQLite, Pydantic 2, shared `HttpTransport` /
`PacingGate` / dual worker pools, existing OpenRouter `LlmClient`, Trafilatura
(new pinned dep), pytest, Ruff, Pyright, uv.

## Global Constraints

- Python 3.13 or newer; distribution `notable-person-finder`; import package
  `notable_person_finder`; executable `notable`.
- Work only under `src/notable_person_finder` and focused rewrite tests. Do not
  import, run, reorganize, or repair the retained prototype.
- **The application never writes, drafts, or edits Wikipedia content.**
- Every **external** call maps to exactly one persisted attempt. Deterministic
  terminals (empty complete plan T1, local refuse) use no paid attempt where
  the design says so.
- Exactly **one external call per `execute`**. Only three handlers (K2). Fixed
  `provider`/`operation` per handler; no per-attempt override.
- Only the central retry coordinator starts a repeat request. HTTPX / SDK
  retries stay disabled.
- No SQLite transaction spans network/model work; workers never open SQLite.
- Brave and article HTTP attempts are **not** OpenRouter USD budget
  reservations (K15). Assess-model generations reserve under the existing
  hard cap.
- **Article identity:** only `ingestion.urls.canonicalize_article_url` (K9).
  Never invent a second URL policy.
- **Raw HTML never persisted** (K3): no column; not in `detail_json`,
  `ProviderFailure.detail`, logs, or model input. Crash after GET re-issues GET
  (at-least-once). Point at `docs/architecture/at-least-once-execution.md`.
- Secrets remain environment-only. `BRAVE_API_KEY` and `OPENROUTER_API_KEY`
  are always required on `notable run` (K32). Secrets never enter TOML values,
  snapshots, fingerprints, SQLite product records, logs, digests, terminal
  output, or tests.
- Money is integer nano-USD; UTC timestamps are canonical ISO-8601 text ending
  in `Z`; booleans/enums use checked columns.
- Migrations are forward-only, checksummed, transactional, and backed up before
  changing an existing database. **Never rewrite applied migration 0007** —
  add 0008+ if a column is missing (K16).
- **No lead outcomes, ranking, digest queue, or shortlist synthesis** in this
  milestone (m6). Empty/complete plans never invent `insufficient_evidence`
  leads (K12).
- Plans remain authorities for interfaces and gates, not sources of canonical
  implementation fragments. Shipped code and tests govern. Full column lists,
  plan terminal table T1–T11, assessment CHECK truth tables, and normative
  algorithms live in the design — this plan lists critical tables, CHECKs,
  interfaces, and sequencing only.
- Keep this plan under ~1,500 lines. Reference code blocks are illustrative.
- Mutation evidence (Agents.md): for every locked behaviour this plan names,
  mutate the rule, confirm a **specific named** test fails, restore, and record
  which mutation killed which test. Negative assertions need a positive
  control. Use `cp` backup + `diff` restore (never `git stash`);
  `PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__` between mutation runs.

## Authoritative Inputs

- Design (locks K1–K34, full, revision 4):  
  `docs/superpowers/specs/2026-07-30-coverage-evidence-design.md`
- Cross-cutting: product workflow (lifecycle stop/continue), domain
  persistence (coverage evidence records), provider adapters (`WebSearchClient`,
  `ArticleFetcher`, `ArticleExtractor`, PassageSelector), LLM-evaluation Task 4
  `assess_article`, Wikipedia m4 K11 stop rule, durable person identity 3b2,
  model-gateway dual pools / prepare-execute-settle, at-least-once note,
  legacy `04-coverage-and-source-reliability.md`.
- Style precedent:  
  `docs/superpowers/plans/2026-07-30-wikipedia-identity-matching.md`
- Code precedents: `wikipedia/service.py` (plan advance, multi-model inspect,
  handlers), `wikipedia/merge_hooks.py`, `people/service.py` (preflight settler
  extension pattern), `providers/mediawiki.py` / `providers/feeds.py` (HTTP
  adapter boundary, compact failure detail), `providers/pacing.py` (`brave`
  key already present), `ingestion/urls.py` (`canonicalize_article_url`,
  `publisher_key`), `ingestion` canonical_article upsert helpers,
  `runs/engine.py` (`TaskHandler` one-call execute).

## Locked Decisions (do not reopen)

| ID | Lock |
| --- | --- |
| K1 | New `coverage/` package; do not fold into `people/` or `wikipedia/` |
| K2 | Three work-item kinds only: `brave_web_search`, `fetch_article`, `assess_article` |
| K3 | Extract in-process after fetch; drop HTML before execute returns; never HTML in detail_json/logs/SQLite; crash re-GET |
| K4 | Coverage plan subject is always canonical `person` |
| K5 | Eligible only when current completed Wikipedia is `no_matching_page_found` or `uncertain_identity`; `matching_page_found` stops + `supersede_coverage_work_for_person`; null/failed Wikipedia do not unlock |
| K6 | Exact-name always; alias if below target; at most one context; obituary with stage-1 when death facts exist |
| K7 | Adapter never invents queries / News / reliability / cross-call rerank |
| K8 | Screen after canonicalize, before fetch selection; ineligible never fetched |
| K9 | Only `canonicalize_article_url`; alias kind `search_result` |
| K10 | Discovery attach at plan open; discovery row only for usable URL + `screening_id NOT NULL`; unusable → `source_screening` only with `source_item_id` |
| K11 | Deterministic selection; `assess_ineligible=true` rejected in m5 |
| K12 | Plan terminals T1–T11; never invent lead outcomes |
| K13 | Unsafe truncation never claims “no coverage found” as product absence |
| K14 | Priority: … → MediaWiki 50 → match 55 → Brave 60 → fetch 65 → assess 70 |
| K15 | Brave/article HTTP: no OpenRouter USD; assess: hard-cap reservation |
| K16 | Single migration `0007_coverage_evidence.sql` full DDL in Task 1; never edit 0007 later |
| K17 | Durable person-scoped plan under material fingerprint; multi-wave advance |
| K18 | Merge: supersede loser; reassign/merge `person_article`; assessment `person_id` stays observation-time; ensure survivor |
| K19 | Shared `is_coverage_research_eligible`; forward material match only |
| K20 | Assess inspect when eligible **or** active assess **or** active plan/HTTP; mid-run ensure when scheduling assess |
| K21 | PassageSelector in `coverage/`; title/dek count toward block and char caps; fill first |
| K22 | Task 4 fields only; no reliability/notability booleans from model |
| K23 | Permanent assess preflight settles active assess work out-of-band |
| K24 | Altered-query: record provenance; optional `reject_altered_query` (default false) |
| K25 | Current person–article pointer only for completed assessments |
| K26 | Domain `failure_category` separate from `ProviderFailure.category` |
| K27 | Operational projection for person-derived inputs (closure + mentions) |
| K28 | Snippets-only assess path when fetch skipped/fails/empty extract |
| K29 | No second model for passage selection or publisher recon in m5 |
| K30 | Refresh after interval for completed/incomplete; failed no time-refresh; multi-refresh O0→R1→R2 |
| K31 | Upsert `person_article` when creating `coverage_article_target` |
| K32 | Always require `BRAVE_API_KEY` on `notable run` (existing `require_secrets=True`) |
| K33 | `source_policy_file` required; Brave endpoint only; all fingerprint bounds under `tasks.assess_article` |
| K34 | `selection_reason` ∈ four-value source×tier enum (no bare `discovery`) |

## File Structure

```text
src/notable_person_finder/
  db/migrations/0007_coverage_evidence.sql     # NEW (full DDL — K16)
  providers/brave.py                           # NEW: WebSearchClient
  providers/articles.py                        # NEW: ArticleFetcher + ArticleExtractor
  coverage/                                    # NEW package
    __init__.py
    queries.py                                 # exact / alias / context forms
    screening.py                               # source-policy TOML load + match
    selection.py                               # fetch/assess selection + reasons
    passages.py                                # PassageSelector
    assessment.py                              # Task 4 I/O, schema, validate, hashes
    models.py                                  # domain DTOs
    repository.py                              # plans, forms, occurrences, views, assessments
    service.py                                 # seed, ensure, handlers, maybe_advance
    merge_hooks.py                             # reconcile_on_merge
    prompts/assess_article.md
  config/models.py                             # BraveConfig; AssessArticleConfig; source_policy_file
  config/loader.py                             # load source policy path; snapshot fields
  people/merge.py                              # call coverage merge hook after wikipedia
  wikipedia/service.py                         # schedule_coverage_after_wikipedia_ready call sites
  people/repository.py                         # extend permanent-preflight settler for assess (K23)
  reporting/digest.py                          # Coverage section
  cli/main.py                                  # handlers, seed order, status, clients
config/
  notable.example.toml                         # [brave], [tasks.assess_article], source_policy_file
  source_policies/visual_arts.example.toml     # NEW example policy
pyproject.toml / uv.lock                       # trafilatura pin (Task 3)
tests/coverage/                                # NEW
  __init__.py
  conftest.py
  fakes_brave.py
  fakes_articles.py
  test_schema.py
  test_repository.py
  test_brave.py
  test_articles.py
  test_screening.py
  test_selection.py
  test_queries.py
  test_eligibility.py                          # K5 gate matrix early
  test_passages.py
  test_assessment.py
  test_http_service.py                         # Brave + plan advance
  test_fetch_service.py                        # fetch + extract persist
  test_assess_service.py                       # assess handler, K20, K23
  test_seed_and_hooks.py
  test_merge_hooks.py
  test_run_cli.py
  test_seams.py
  test_digest_status.py
  test_live_brave.py                           # opt-in
  test_live_article_fetch.py                   # opt-in
  test_live_openrouter_assess.py               # opt-in
  fixtures/
tests/foundation/                              # config + migration chain
tests/wikipedia/ or people/                    # only where call sites change
```

## Public Interfaces

```python
# providers/brave.py
PROVIDER = "brave"
OPERATION_SEARCH_WEB = "search_web"

class WebSearchClient(Protocol):
    def search_web(
        self, query: str, *, count: int, offset: int
    ) -> SearchPage: ...

class HttpxBraveWebSearchClient:
    def __init__(
        self, transport: HttpTransport, *, config: BraveConfig, api_key: str, clock: Clock
    ) -> None: ...

# providers/articles.py
ARTICLE_PROVIDER = "article_http"
OPERATION_FETCH_ARTICLE = "fetch_article"
EXTRACTOR_VERSION = 1

class ArticleFetcher(Protocol):
    def fetch_article(self, url: str) -> ArticleFetchResult: ...  # success bytes or typed access

class ArticleExtractor(Protocol):
    def extract_article(self, html: bytes | str) -> ExtractedArticle: ...

class HttpxArticleFetcher: ...
class TrafilaturaArticleExtractor: ...

# coverage/queries.py
COVERAGE_QUERY_PLAN_VERSION = 1
def generate_exact_forms(...) -> tuple[QueryFormSpec, ...]: ...
def generate_alias_forms(...) -> tuple[QueryFormSpec, ...]: ...
def generate_context_form(...) -> QueryFormSpec | None: ...

# coverage/screening.py
def load_source_policy(path: Path) -> SourcePolicy: ...
def fingerprint_source_policy(policy: SourcePolicy) -> str: ...  # 64 hex
def screen_url(
    policy: SourcePolicy, *, url: str, publisher_key: str | None
) -> ScreeningDecision: ...  # rule_id, rule_status

# coverage/selection.py
def eligible_selected_count(...) -> int: ...
def final_selection(...) -> tuple[SelectedArticle, ...]: ...
# SelectedArticle.selection_reason ∈ K34 four values

# coverage/passages.py
def select_passages(
    person_names: Sequence[str],
    article_view: ArticleViewLike,
    config: AssessArticleConfig,
) -> PassageView: ...

# coverage/assessment.py
ASSESS_SCHEMA_VERSION = 1
COVERAGE_ADAPTER_VERSION = 1
CONTENT_TYPES = frozenset({
    "reporting", "profile", "review", "interview", "obituary",
    "listing", "announcement", "press_release", "sponsored", "other",
})
def coverage_material_fingerprint(
    person_view, config: AssessArticleConfig, *, source_policy_fingerprint: str,
    refresh_of_plan_id: int | None,
) -> str: ...
def assess_task_fingerprint(
    *, material_fingerprint: str, person_article_id: int, article_view_id: int
) -> str: ...  # == assessment.task_fingerprint
def build_assess_input(...) -> AssessArticleInput: ...
def assess_schema() -> dict[str, object]: ...
def render_assess_request(...) -> RenderedAssessRequest: ...
def validate_assess_output(raw: str, supplied: AssessArticleInput) -> AssessArticleOutput: ...

# coverage/service.py
BRAVE_WEB_SEARCH_TASK_TYPE = "brave_web_search"
FETCH_ARTICLE_TASK_TYPE = "fetch_article"
ASSESS_ARTICLE_TASK_TYPE = "assess_article"
SUBJECT_KIND_COVERAGE_QUERY_FORM = "coverage_query_form"
SUBJECT_KIND_COVERAGE_ARTICLE_TARGET = "coverage_article_target"
SUBJECT_KIND_PERSON_ARTICLE = "person_article"
BRAVE_SEARCH_PRIORITY = 60
FETCH_ARTICLE_PRIORITY = 65
ASSESS_ARTICLE_PRIORITY = 70

def is_coverage_research_eligible(connection, *, person_id, config, now) -> bool: ...
def plan_matches_live_material(plan, person_view, config) -> bool: ...
def ensure_coverage_research(...) -> str: ...  # scheduled|reused|ineligible|stopped_matching|...
def seed_coverage_research(...) -> int: ...
def schedule_coverage_after_wikipedia_ready(...) -> None: ...
def supersede_coverage_work_for_person(connection, person_id, *, run_id, now) -> None: ...
def maybe_advance_coverage_plan(connection, *, plan_id, run_id, config, now) -> None: ...
def build_brave_web_search_handler(...) -> TaskHandler: ...
def build_fetch_article_handler(...) -> TaskHandler: ...
def build_assess_article_handler(...) -> TaskHandler: ...
def assess_model_needed(connection, run_id, config) -> bool: ...  # K20

# coverage/merge_hooks.py
def reconcile_on_merge(
    connection, *, survivor_id: int, loser_id: int, run_id: int, config, now
) -> None: ...
```

### Seed composition (CLI `_compose_seed` order)

```text
1. seed_feeds(...)
2. seed_untriaged(...)
3. seed_unresolved_mentions(...)
4. seed_wikipedia_identity(...)
5. seed_coverage_research(...)          # NEW — after Wikipedia
6. ensure_model_inspections_for_run(...)  # includes assess when K20
```

### Config defaults (K33)

```toml
source_policy_file = "source_policies/visual_arts.toml"  # REQUIRED

[brave]
endpoint = "https://api.search.brave.com/res/v1/web/search"

# Plan-level bounds live here because they enter coverage_material_fingerprint
[tasks.assess_article]
model = "openai/gpt-5.4-mini"
max_input_tokens = 4096
max_completion_tokens = 1024
max_title_characters = 500
max_summary_characters = 4000
retrieval_target = 5
max_exact_forms = 4
max_alias_forms = 4
max_context_forms = 1
search_count = 10
max_offsets_per_form = 0          # additional pages after first; 0 = one page
max_results_per_form = 20
max_eligible_fetches = 8
max_unclassified_fetches = 2
max_passage_characters = 6000
max_passage_blocks = 24
opening_block_count = 2
coverage_refresh_interval_hours = 720
reject_altered_query = false
assess_ineligible = false           # true rejected at validation in m5

[tasks.assess_article.parameters]
temperature = 0.0
top_p = 1.0
reasoning_effort = "low"
```

`PacingConfig.brave_min_interval_ms` already defaults to 1100. Article HTTP uses
provider key `"article_http"` with **no** global min-interval (per-origin only).

Hard config failures: missing/invalid `source_policy_file`; `assess_ineligible=true`;
invalid bounds; non-HTTPS Brave endpoint (mirror MediaWiki validator).

### Material fingerprint fields (normative)

`coverage_material_fingerprint` = `sha256(canonical_json({...}))` with exactly
the fields listed in design §Fingerprints: `task="coverage_evidence"`,
`adapter_version`, `person_id`, `identity_fingerprint`, `query_plan_version`,
`source_policy_fingerprint`, `extractor_version`, assess model + parameters +
**all** `AssessArticleConfig` bound fields including `reject_altered_query` and
`assess_ineligible`, `prompt_hash`, `schema_hash`, `schema_version`,
`refresh_of_plan_id`. **No reverse-hash API** — forward recompute via plan
`refresh_of_plan_id` only (K19/K30).

### Work-item fingerprints

| Task | Fingerprint inputs |
| --- | --- |
| `brave_web_search` | plan material fp + `query_form_id` + `offset_in` |
| `fetch_article` | plan material fp + `coverage_article_target_id` |
| `assess_article` | `assess_task_fingerprint(material, person_article_id, article_view_id)` |

### Plan terminal truth table (summary — full T1–T11 in design)

| ID | Outline | Status |
| --- | --- | --- |
| T1 | All required scheduled forms succeeded; not truncated; zero selected | `completed` (empty-claim allowed) |
| T3 | Truncated unsafe; zero completed assessments | `incomplete` |
| T4 | Truncated; ≥1 completed assessment | `completed` + flags |
| T5/T8 | Partial form success with assessments | `completed` + `partial_retrieval` |
| T6/T7 | Total exact-form death; zero discovery salvage | `failed` |
| T10 | No successful search form; ≥1 discovery selected path terminal | `completed` + `partial_retrieval` |
| T11 | Mixed form failure; empty selection; not truncated | `failed` / `partial_retrieval_empty` |
| T* | `matching_page_found` supersede | `superseded` |

Empty-claim (T1) **only** when every *required scheduled* form completed
successfully and NOT truncated. Never invent lead rows.

### Selection reason (K34)

| Condition | `selection_reason` |
| --- | --- |
| Discovery + curated_eligible | `discovery_curated_eligible` |
| Discovery + unclassified fallback | `discovery_unclassified_fallback` |
| Search + curated_eligible | `search_curated_eligible` |
| Search + unclassified fallback | `search_unclassified_fallback` |

Curated-ineligible / unusable: never create `coverage_article_target`.

---

## Task 1: Migration 0007 + repository DDL/writers + schema tests

**Files:**

- Create: `src/notable_person_finder/db/migrations/0007_coverage_evidence.sql`
- Create: `src/notable_person_finder/coverage/__init__.py`
- Create: `src/notable_person_finder/coverage/repository.py`
- Create: `src/notable_person_finder/coverage/models.py` (row DTOs as needed)
- Create: `tests/coverage/__init__.py`, `conftest.py`, `test_schema.py`,
  `test_repository.py`
- Modify: `tests/foundation/test_migrations.py` if version chain asserts need updating

**Interfaces / DDL surface (critical — full columns in design §DDL):**

| Table / change | Critical constraints |
| --- | --- |
| `article_url_alias.kind` | Rebuild CHECK to include `search_result` (was feed_original, redirect_destination); survival test for old aliases |
| `person_coverage_plan` | status ∈ retrieving/selecting/assessing/completed/failed/incomplete/superseded; partial UNIQUE active `(person_id, material_fingerprint)` WHERE status ∈ active set; flags `truncated_unsafe`, `partial_retrieval`; `refresh_of_plan_id`; `eligible_selected_count` |
| `coverage_query_form` | variant ∈ exact/exact_obituary/alias/context; status pending/completed/failed/superseded; `UNIQUE (plan_id, ordinal)`; offsets counters |
| `brave_search_observation` | attempt_id NOT NULL; **UNIQUE (attempt_id)**; submitted query + altered query fields; completeness |
| `search_result_occurrence` | `UNIQUE (search_observation_id, rank)`; nullable `canonical_article_id` / `screening_id` for unusable |
| `source_screening` | rule_status ∈ curated_eligible/curated_ineligible/unclassified/unusable; null `canonical_article_id` only when unusable; optional `source_item_id` / `person_mention_id` for discovery provenance |
| `coverage_discovery_article` | `canonical_article_id NOT NULL`; **`screening_id NOT NULL`**; `UNIQUE (plan_id, canonical_article_id)` |
| `coverage_article_target` | `selection_reason` K34 four values; status pending/fetched/snippets_only/failed/superseded; `UNIQUE (plan_id, canonical_article_id)` |
| `article_view` | access_kind full/partial/snippets; **no HTML column**; attempt_id NULL allowed for snippets (document SQLite NULL UNIQUE); prefer partial unique index `WHERE attempt_id IS NOT NULL` |
| `person_article` | `UNIQUE (person_id, canonical_article_id)`; `current_assessment_id` ownership trigger completed-only |
| `person_article_assessment` | disposition completed/failed; UNIQUE `(person_article_id, task_fingerprint)`; completed/failed CHECK truth table from design; closed content_types validated in app |
| `article_assessment_signal` | kind attention/caution; free non-empty category string |

**Steps:**

- [ ] **1.1** Write failing schema tests: migration applies after 0006; every
  critical CHECK rejects illegal rows; discovery `screening_id` NOT NULL;
  assessment completed/failed matrices; no HTML column; alias rebuild keeps
  existing kinds and accepts `search_result`.
- [ ] **1.2** Implement `0007_coverage_evidence.sql` from design DDL sketch
  (complete, frozen — K16).
- [ ] **1.3** Implement repository writers/readers needed by later tasks
  (open plan, forms, screening, discovery, occurrences, targets, views,
  person_article upsert, assessments, supersede helpers). Prefer small pure
  SQL helpers mirroring `wikipedia/repository.py`.
- [ ] **1.4** Run: `uv run pytest tests/coverage/test_schema.py tests/coverage/test_repository.py tests/foundation -q`
  Expected: PASS.
- [ ] **1.5** Commit: `feat(coverage): evidence schema (0007)`.

---

## Task 2: Brave WebSearchClient adapter

**Files:**

- Create: `src/notable_person_finder/providers/brave.py`
- Create: `tests/coverage/test_brave.py`, `tests/coverage/fakes_brave.py`,
  fixtures under `tests/coverage/fixtures/`
- Modify: `src/notable_person_finder/config/models.py` — `BraveConfig` (endpoint only)
- Modify: example TOML for `[brave]`

**Interfaces:**

- Consumes: `HttpTransport`, `PacingGate` key `"brave"`, `BraveConfig`, API key
- Produces: `SearchPage` with ranks, URLs, titles, snippets, altered query,
  offset/completeness; `ProviderFailure` with compact detail only

**Behavior locks:**

- Exactly one Brave Web Search API request per `search_web` call.
- Fixed product settings: global web, English, moderate SafeSearch; no News.
- Disable silent query correction when API permits; still record provider
  altered query if returned.
- Never invent follow-up queries, reliability, or cross-call rerank (K7).
- Auth via header from env key; never log the key or put it in failure detail.

**Steps:**

- [ ] **2.1** Failing tests: happy page parse; pagination/completeness fields;
  altered query captured; 401/429/5xx map to typed failures; detail has no
  secret and no full response body.
- [ ] **2.2** Implement `HttpxBraveWebSearchClient` following MediaWiki/feeds
  adapter style.
- [ ] **2.3** Run: `uv run pytest tests/coverage/test_brave.py -q` → PASS.
- [ ] **2.4** Commit: `feat(providers): WebSearchClient Brave search_web`.

---

## Task 3: ArticleFetcher + Trafilatura ArticleExtractor

**Files:**

- Create: `src/notable_person_finder/providers/articles.py`
- Create: `tests/coverage/test_articles.py`, `fakes_articles.py`, HTML fixtures
- Modify: `pyproject.toml` / `uv.lock` — pin `trafilatura` (reasonable current
  release; freeze with `uv lock`)
- Modify: transport/safety usage for article origin provider key `article_http`

**Interfaces:**

- Consumes: `HttpTransport` (URL safety, redirects, size, timeout),
  `max_article_response_bytes` already on transport config
- Produces: typed fetch success (requested/final URL, redirect chain, HTML
  **only for current attempt**) or typed access states (not found, auth,
  paywall/deny when detectable, unsupported, too large); extract → title,
  dek, byline, date, labels, ordered blocks, quality full/partial/empty

**Behavior locks (K3):**

- One GET per `fetch_article`.
- Extract is pure/network-free.
- Returned DTOs never carry HTML after extract boundary.
- Failure detail never includes HTML bodies.
- No publisher-specific scrapers.

**Steps:**

- [ ] **3.1** Failing tests: safe URL / redirect deny; too large; unsupported
  content-type; extract fixture HTML → blocks; empty/partial quality; assert
  no `html` attribute on returned extract DTO.
- [ ] **3.2** Add trafilatura dependency; implement fetcher + extractor.
- [ ] **3.3** Run: `uv run pytest tests/coverage/test_articles.py -q` → PASS.
- [ ] **3.4** Commit: `feat(providers): ArticleFetcher and Trafilatura extractor`.

---

## Task 4: Source policy, screening, selection, queries + Wikipedia gate unit tests

**Files:**

- Create: `coverage/screening.py`, `selection.py`, `queries.py`
- Create: `config/source_policies/visual_arts.example.toml`
- Create: `tests/coverage/test_screening.py`, `test_selection.py`,
  `test_queries.py`, `test_eligibility.py`
- Modify: `config/models.py` / `loader.py` — required `source_policy_file` on
  `MainConfig`; load failure when missing/invalid schema_version
- Modify: example TOML

**Interfaces:**

- Consumes: design screening/selection algorithms; `canonicalize_article_url`,
  `publisher_key`; person operational names for query generation (helpers
  may take plain sequences in pure tests)
- Produces: pure functions used by service later; **`is_coverage_research_eligible`
  pure matrix tests with faked Wikipedia rows** (no HTTP)

**Behavior locks:**

- First-match source policy; unclassified default; fingerprint stability.
- Discovery attach rules (K10) unit-tested without full service if possible:
  usable → discovery+screening NOT NULL; unusable → screening only; empty URL
  → neither.
- Selection ordering + K34 reasons; ineligible never selected.
- Queries: exact always; no nickname map; alias/context generation pure;
  obituary only with explicit death facts.
- Eligibility: matching → false; no_match/uncertain → true (other gates
  permitting); null pointer / failed → false (K5).

**Steps:**

- [ ] **4.1** Failing pure tests for policy match, selection matrix, query
  forms, eligibility gate matrix (positive controls for each outcome).
- [ ] **4.2** Implement pure modules + config load for source policy path.
- [ ] **4.3** Run: `uv run pytest tests/coverage/test_screening.py tests/coverage/test_selection.py tests/coverage/test_queries.py tests/coverage/test_eligibility.py -q` → PASS.
- [ ] **4.4** Commit: `feat(coverage): policy, selection, queries, eligibility gate`.

---

## Task 5: PassageSelector + assess_article contract + AssessArticleConfig

**Files:**

- Create: `coverage/passages.py`, `assessment.py`, `prompts/assess_article.md`
- Create: `tests/coverage/test_passages.py`, `test_assessment.py`
- Modify: `config/models.py` — `AssessArticleConfig` with **all** fingerprint
  fields (K33); `TasksConfig.assess_article`; reject `assess_ineligible=true`
- Modify: `notable.example.toml` with comment on plan-level bounds co-location
- Modify: loader snapshot/fingerprint inclusion for new task config fields

**Interfaces:**

- Consumes: Task 4 LLM-evaluation I/O; K21 passage algorithm
- Produces: `coverage_material_fingerprint`, `assess_task_fingerprint`,
  schema/prompt hashes, validate_assess_output

**Behavior locks:**

- Closed `content_types` including `press_release` snake_case.
- Passage IDs must be subset of supplied; bad ids rejected.
- Title/dek count toward max blocks and characters; fill first (K21).
- Fingerprint includes `reject_altered_query` and `assess_ineligible`.
- Model never asked for reliability/notability booleans (K22).

**Steps:**

- [ ] **5.1** Failing tests: schema version; validation; fingerprint field
  list (mutate omit `reject_altered_query` → hash changes); passage budget
  with title/dek counting; name_absent path.
- [ ] **5.2** Implement prompt + Pydantic models + config.
- [ ] **5.3** Run: `uv run pytest tests/coverage/test_passages.py tests/coverage/test_assessment.py tests/foundation -q` → PASS.
- [ ] **5.4** Commit: `feat(coverage): assess_article contract, passages, config`.

---

## Task 6a: Plan lifecycle + brave_web_search handler

**Files:**

- Create/extend: `coverage/service.py` (plan open, discovery attach, form
  stages, `maybe_advance_coverage_plan` terminals T1–T11, Brave handler)
- Create: `tests/coverage/test_http_service.py`
- Modify: CLI registration of Brave handler only (or full three if easier;
  fetch/assess can refuse until wired)

**Interfaces:**

- Consumes: Tasks 1–2, 4–5 repository + pure modules + Brave client
- Produces: durable plans/forms/occurrences/screening; work items for Brave;
  plan terminalization without lead rows

**Behavior locks:**

- Stage 0 discovery attach at plan open only (K10 algorithm).
- Stage 1 exact (+ obituary) at open; exact-name always even if discovery meets target.
- `max_offsets_per_form`: additional pages after first; default 0 ⇒ one page.
- One `search_web` per execute; persist observation UNIQUE(attempt_id).
- Alias/context gated by `eligible_selected_count` including discovery.
- Altered query: store provenance; if `reject_altered_query` fail form.
- Empty complete T1: plan `completed`, zero assess work, zero lead rows.
- Truncated empty T3: `incomplete`, not empty-claim.

**Steps:**

- [ ] **6a.1** Failing tests: plan open discovery matrix; exact always; offset
  0 single page; T1 empty complete; T3 truncated; T10/T11 rows; no second
  canonicalize function.
- [ ] **6a.2** Implement plan service + Brave handler prepare/execute/persist.
- [ ] **6a.3** Run: `uv run pytest tests/coverage/test_http_service.py tests/coverage/test_schema.py tests/coverage/test_repository.py -q` → PASS.
- [ ] **6a.4** Commit: `feat(coverage): plan lifecycle and brave_web_search handler`.

---

## Task 6b: fetch_article handler + article views

**Files:**

- Extend: `coverage/service.py` — `build_fetch_article_handler`, final
  selection → targets, K31 person_article upsert, snippets path (K28)
- Create: `tests/coverage/test_fetch_service.py`

**Interfaces:**

- Consumes: ArticleFetcher + ArticleExtractor; selection reasons K34
- Produces: `article_view` rows; fetch work settlement; schedules assess only
  when view ready (assess handler may land in Task 7)

**Behavior locks:**

- One GET per execute; extract in-process; HTML dropped before return (K3).
- Never put HTML in attempt `detail_json`.
- Screen-before-fetch: curated_ineligible never gets targets (mutation later).
- Snippets-only view when not selected for body / inaccessible / empty extract.
- Upsert `person_article` before/with target (K31).

**Steps:**

- [ ] **6b.1** Failing tests: selection creates K34 reasons; fetch success
  full/partial; snippets path; HTML ban; person_article upsert; ineligible
  has zero targets.
- [ ] **6b.2** Implement fetch handler + selection→target scheduling from
  `maybe_advance_coverage_plan`.
- [ ] **6b.3** Run: `uv run pytest tests/coverage/test_fetch_service.py tests/coverage/test_http_service.py -q` → PASS.
- [ ] **6b.4** Commit: `feat(coverage): fetch_article handler and article views`.

---

## Task 7: Assess handler + multi-model inspection

**Files:**

- Extend: `coverage/service.py` — assess handler; `assess_model_needed` (K20);
  mid-run `ensure_model_inspections_for_run` when scheduling assess
- Modify: `people/repository.py` or shared preflight settler — permanent
  assess-model preflight settles active `assess_article` (K23)
- Modify: multi-model inspection arming in people/service (pattern from match)
- Create: `tests/coverage/test_assess_service.py`
- Modify: people inspection tests if arming surface shared

**Interfaces:**

- Consumes: OpenRouter `generate_structured`; PassageSelector; screening on input
- Produces: immutable assessments + signals; current pointer completed-only (K25)

**Behavior locks:**

- Subject is `person_article`; fingerprint includes `article_view_id`.
- K20 cold-start: active plan/HTTP arms inspect before assess work exists.
- Invalid passage refs → malformed retry then permanent failed assessment.
- Permanent preflight: failed assessment with inspection attempt_id; pointer
  unchanged.

**Steps:**

- [ ] **7.1** Failing tests: happy assess path; bad passage id; K20 cold-start;
  K23 preflight; pointer only on completed.
- [ ] **7.2** Implement assess handler + inspection arming + preflight settle.
- [ ] **7.3** Run: `uv run pytest tests/coverage -q --ignore=tests/coverage/test_live_brave.py --ignore=tests/coverage/test_live_article_fetch.py --ignore=tests/coverage/test_live_openrouter_assess.py` → PASS (or equivalent deselect live).
- [ ] **7.4** Commit: `feat(coverage): assess_article handler and inspect gating`.

---

## Task 8: Seed, Wikipedia supersede hooks, merge

**Files:**

- Extend: `coverage/service.py` — `seed_coverage_research`,
  `schedule_coverage_after_wikipedia_ready`, `ensure_coverage_research`,
  `supersede_coverage_work_for_person`
- Create: `coverage/merge_hooks.py`
- Modify: `wikipedia/service.py` (and empty no-match settle paths) to call
  `schedule_coverage_after_wikipedia_ready` after completed identity
- Modify: `people/merge.py` — coverage reconcile after wikipedia
- Create: `tests/coverage/test_seed_and_hooks.py`, `test_merge_hooks.py`

**Interfaces:**

- Consumes: K5/K19 eligibility; ensure live_fp table; K18 merge algorithm
- Produces: mid-flight cancel on matching; survivor ensure; multi-refresh O0→R1→R2

**Behavior locks:**

- matching → supersede then stop (not eligibility alone).
- no_match / uncertain → ensure.
- null Wikipedia pointer → no coverage open.
- Merge: loser supersede; person_article reassign/conflict; assessment
  `person_id` observation-time immutability; never blind pointer copy.
- Multi-refresh clock test required (K30).

**Steps:**

- [ ] **8.1** Failing tests: gate + supersede mid-flight; seed order effects;
  merge algorithm; multi-refresh.
- [ ] **8.2** Implement hooks and wire call sites.
- [ ] **8.3** Run: `uv run pytest tests/coverage/test_seed_and_hooks.py tests/coverage/test_merge_hooks.py tests/coverage/test_eligibility.py -q` → PASS.
- [ ] **8.4** Commit: `feat(coverage): seed, supersede on matching, merge hooks`.

---

## Task 9: Operator surface + CLI + milestone gate

**Files:**

- Modify: `reporting/digest.py` — Coverage section counters
- Modify: `cli/main.py` — register three handlers; construct Brave + article
  clients; seed order; status lines
- Create: `tests/coverage/test_digest_status.py`, `test_run_cli.py`,
  `test_seams.py`
- Create: live smokes (opt-in) stubs
- Update: `Agents.md` “What Is Actually Built” only if user requests; default
  leave for post-merge docs task

**Digest / status counters (truthful only):**

- coverage plans completed / incomplete / failed (this run and durable as designed)
- assessments completed this run; people with ≥1 assessment
- `coverage_eligible_remaining` (K19)
- `coverage_stopped_matching_wikipedia` — current completed matching only;
  positive control test required
- model deferred/failed for assess; OpenRouter cost remains shared line
- **No** lead tiers, shortlist synthesis, or false “skipped research” beyond
  the stopped-matching counter that m5 enforces

**CLI:**

- Register three coverage handlers on `notable run`.
- Seed: feeds → untriaged → unresolved → wikipedia → **coverage** → inspections.
- Construct `HttpxBraveWebSearchClient` with non-null key (K32) and article
  fetcher/extractor with shared transport.
- Priorities 60/65/70.

**Live smokes (opt-in, not default gate):**

```bash
BRAVE_API_KEY=… uv run pytest tests/coverage -m live -v
OPENROUTER_API_KEY=… BRAVE_API_KEY=… uv run pytest tests/coverage -m live -v
```

**Steps:**

- [ ] **9.1** Write failing digest/status tests with positive controls for each
  new line (especially stopped-matching).
- [ ] **9.2** Implement counters, digest, status, full CLI wire, seams.
- [ ] **9.3** Full offline gate:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage
git diff --check
```

- [ ] **9.4** Mutation evidence ledger for locked rules (minimum:
  K2/K3/K5/K5-supersede/K8/K9/K10/K12/K18/K19/K20/K21/K23/K25/K32/K34).
  Record killer test names in
  `.superpowers/sdd/2026-07-30-coverage-evidence/progress.md`.
- [ ] **9.5** Commit: `feat(coverage): digest, status, and CLI integration`.
- [ ] **9.6** Do not mark design Implemented or programme cutover unless the
  user requests.

---

## Completion Gate

### Offline (milestone close)

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage
git diff --check
```

### Intermediate (after Task 4)

```bash
uv run pytest tests/coverage/test_screening.py tests/coverage/test_selection.py \
  tests/coverage/test_queries.py tests/coverage/test_eligibility.py \
  tests/coverage/test_schema.py tests/coverage/test_repository.py -q
```

### Intermediate (after Task 6b)

```bash
uv run pytest tests/coverage/test_http_service.py tests/coverage/test_fetch_service.py \
  tests/coverage/test_brave.py tests/coverage/test_articles.py -q
```

### Intermediate (after Task 7)

```bash
uv run pytest tests/coverage -q \
  --ignore=tests/coverage/test_live_brave.py \
  --ignore=tests/coverage/test_live_article_fetch.py \
  --ignore=tests/coverage/test_live_openrouter_assess.py
```

### Live (opt-in, not default gate)

```bash
BRAVE_API_KEY=… uv run pytest tests/coverage -m live -v
OPENROUTER_API_KEY=… uv run pytest tests/coverage -m live -v
```

## Required Test Themes Checklist

Use this as the Task 9 self-review against the design:

- [ ] Migration 0006→0007; full DDL; `search_result` alias; no HTML column;
  assessment CHECK truth table; person_article pointer ownership
- [ ] Brave adapter fixtures; secret redaction; pacing key `brave`
- [ ] Article fetch safety; extract no HTML in DTOs; typed access states
- [ ] Source policy first-match; fingerprint; discovery attach K10 matrix
- [ ] Selection K34 reasons; ineligible never fetched
- [ ] Query plan exact always; alias/context gated; no nickname map
- [ ] Plan terminals T1–T11; empty complete no lead rows; truncated honesty
- [ ] Offset semantics default 0 = one page
- [ ] Wikipedia gate + supersede mid-flight; null/failed do not unlock
- [ ] Multi-refresh O0→R1→R2
- [ ] Fetch+extract one external call; assess one call; workers no SQLite
- [ ] PassageSelector title/dek count toward caps
- [ ] Assess validation; K20 cold-start; K23 preflight
- [ ] Merge K18 immutability + survivor ensure
- [ ] Digest/status counters; stopped-matching positive control; no lead tiers
- [ ] Mutation evidence per Agents.md
- [ ] Application never edits Wikipedia; no lead assessment in m5

## Spec Coverage Map

| Design section | Task(s) |
| --- | --- |
| Migration 0007 + schema + triggers + K16/K25/K34 CHECKs | 1 |
| Brave adapter + BraveConfig endpoint | 2 |
| ArticleFetcher + ArticleExtractor + trafilatura | 3 |
| Screening, selection, queries, K5 gate pure tests, source_policy_file | 4 |
| PassageSelector K21, assess contract K22, AssessArticleConfig K33 | 5 |
| Plan lifecycle, Brave handler, T1–T11, discovery K10 | 6a |
| Fetch handler, views, K3/K28/K31 | 6b |
| Assess handler, K20 inspect, K23 preflight | 7 |
| Seed, supersede, ensure, merge K18/K19/K30 | 8 |
| Digest/status/CLI/live smoke/gate | 9 |

## Non-goals / Out of scope

- Lead assessment, ranking, digest shortlist synthesis, digest queue, audit
  commands (milestone 6).
- Promptfoo suites, full live verification matrix, legacy comparison, cutover
  (milestone 7).
- Automated Wikipedia edits, drafts, or notability verdicts.
- Model-chosen URLs, Brave News cascade, open-ended agent loops.
- `assess_ineligible=true` live path (reserved; rejected in m5).
- Engine extensions for multi-call execute, per-attempt provider override, or
  prepare-time network I/O.
- Conditional Brave key / coverage disable flag (K32 keeps always-required).
- Migrating legacy JSONL coverage state.
- Editing applied 0007; feature flags; reverse migrations.
- Importing or wrapping prototype modules.

## PR / Task mapping

Design PR plan maps to tasks as:

| Design PR | Plan task |
| --- | --- |
| PR1 schema 0007 | Task 1 |
| PR2 Brave adapter | Task 2 |
| PR3 article fetch/extract | Task 3 |
| PR4 policy/selection/eligibility | Task 4 |
| PR5 assess contract + config | Task 5 |
| PR6a plan + Brave handler | Task 6a |
| PR6b fetch handler | Task 6b |
| PR7 assess + inspect | Task 7 |
| PR8 seed/supersede/merge | Task 8 |
| PR9 digest/status/gate | Task 9 |

Parallelism (same as design): Tasks 2/3/5 can proceed after or alongside Task 1
once interfaces are agreed; Task 4 needs Task 1 for optional repository types
but pure eligibility can use fakes; 6a needs 1+2+4+5; 6b needs 3+6a; 7 needs
5+6b; 8 needs 6–7; 9 last.

## Execution Notes

- Preferred branch: `feat/coverage-evidence` from `refactor/rearchitecture`
  (worktree already at `.worktrees/feat-coverage-evidence`).
- Track progress in
  `.superpowers/sdd/2026-07-30-coverage-evidence/progress.md`.
- Do not push, open PRs, or merge without explicit user authorization.
- Intermediate offline coverage gates after Tasks 4, 6b, and 7; full programme
  gate after Task 9.
- Point at `docs/architecture/at-least-once-execution.md` for crash windows;
  do not restate them in digests.
- Design document remains Draft until implementation completes and user
  authorizes status flip.

## Self-review (plan author)

- **K1–K34:** each lock appears in Global Constraints, Locked Decisions table,
  and at least one task interface or required test theme.
- **Placeholder scan:** no TBD/TODO for product policy; implementers follow
  design §DDL / T1–T11 / assessment CHECK for full detail, this plan for
  critical CHECKs, interfaces, and sequencing.
- **Type consistency:** three task type strings, subject kinds, priorities
  60/65/70, K34 selection_reason values, fingerprint field list, seed order,
  and provider keys (`brave`, `article_http`, `openrouter`) match the design
  public interfaces section.
- **Three handlers only; extract not a work item; no raw HTML; matching_page
  supersede; always-required Brave key; no lead outcomes; progress ledger
  separate from this plan.**
- **Spec coverage:** discovery K10, selection K34, passage caps K21, terminals
  T1–T11, merge K18, and operator counters each have a task.
