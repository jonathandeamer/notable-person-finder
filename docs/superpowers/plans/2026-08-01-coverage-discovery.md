# Coverage Discovery (Milestone 5a) Implementation Plan

> **SUPERSEDED — NEVER EXECUTED. DO NOT IMPLEMENT THIS PLAN.** Superseded on
> 2026-08-01 by `docs/superpowers/plans/2026-07-30-coverage-evidence.md`
> (Tasks 1–8, all complete), whose design authority is
> `docs/superpowers/specs/2026-07-30-coverage-evidence-design.md`. This plan
> was written without knowledge of the `feat/coverage-evidence` branch, which
> had already shipped the capability. Its own design authority
> (`2026-08-01-coverage-research-design.md`) is likewise superseded. Retained
> as a record only; do not delete.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A run takes every person whose Wikipedia identity is
`no_matching_page_found` or `uncertain_identity`, executes one bounded,
deterministic Brave Web Search plan for that person, screens every result
against a versioned publisher policy, and deterministically selects the
articles that milestone 5b will fetch — reporting all of it in the digest and
`notable status`.

**Architecture:** A new `providers/brave.py` translator performing exactly one
Brave request per call over the existing shared transport, and a new
`coverage/` domain package holding publisher policy, the query plan, screening
and selection, persistence, and one `coverage_search` work-item handler. The
`wikipedia/` package is the structural template throughout: plan → ordered
query units → per-unit HTTP work items → deterministic advancement on the
application thread inside the engine's settlement transaction.

**Tech Stack:** Python 3.13, uv, Pydantic v2 strict models, SQLite with
forward-only checked migrations, httpx behind the existing `HttpTransport`,
pytest.

**Authorities:** `docs/superpowers/specs/2026-08-01-coverage-research-design.md`
is the design authority for this milestone. Where this plan and that spec
disagree, the spec wins and the plan is wrong.

## Global Constraints

- Python 3.13+. Install with `uv sync --frozen`. No new dependency in this
  milestone — Trafilatura belongs to 5b.
- Only `providers/` may import `httpx`. `coverage/` must not.
- Exactly one external call per `execute`, mapping to exactly one persisted
  attempt. Handlers never retry, never sleep, never touch SQLite.
- Workers never open SQLite. `prepare`, `persist`, `persist_failure` run on the
  application thread; `execute` runs on a worker.
- `TaskOutcome.reason` is a low-cardinality token. Never interpolate a URL,
  query text, provider text, or any remote string into it.
- `ProviderFailure.detail` must be a compact code or `type(error).__name__` —
  never a response body, never full query text, never a key.
- The Brave API key never appears in a snapshot, fingerprint, log, digest,
  terminal output, exception message, or test assertion.
- Migrations are forward-only. Add `0007_coverage_discovery.sql`; never edit an
  existing migration file.
- `canonicalize_article_url` and `publisher_key` in
  `src/notable_person_finder/ingestion/urls.py` are the only URL
  canonicalization and publisher-identity functions. Do not write a second one.
- Timestamps are UTC strings ending in `Z`, produced by
  `runs.clock.utc_timestamp`.
- Default test runs are offline. Live tests carry `@pytest.mark.live`.
- Every rule named in a task's **Rules** block needs a recorded mutation kill
  before that task is reported complete: mutate the rule in source, confirm a
  **specific named** test fails, restore from a `cp` backup, verify with
  `diff`. Never `git stash`. Run mutations with `PYTHONDONTWRITEBYTECODE=1` and
  clear `__pycache__` between iterations.
- Gate after every task: `uv run ruff check .`, `uv run ruff format .`,
  `uv run pyright`, and the tests named in that task.

## File Structure

**Create:**

| Path | Responsibility |
| --- | --- |
| `config/source-policy.toml` | The tracked curated publisher policy. |
| `src/notable_person_finder/coverage/__init__.py` | Package exports. |
| `src/notable_person_finder/coverage/policy.py` | Policy loading, fingerprint, matching. |
| `src/notable_person_finder/coverage/models.py` | Frozen coverage domain values shared by every other coverage module. |
| `src/notable_person_finder/coverage/queries.py` | Deterministic query-plan generation. |
| `src/notable_person_finder/coverage/selection.py` | Screening dispositions and article selection. |
| `src/notable_person_finder/coverage/repository.py` | Coverage persistence. |
| `src/notable_person_finder/coverage/service.py` | Fingerprints, scheduling, the `coverage_search` handler, plan advancement. |
| `src/notable_person_finder/coverage/seed_hooks.py` | Run seeding and person-merge reconciliation. |
| `src/notable_person_finder/providers/brave.py` | Brave Web Search adapter. |
| `src/notable_person_finder/db/migrations/0007_coverage_discovery.sql` | Coverage schema. |
| `tests/coverage/` | This milestone's tests (see per-task files). |

**Modify:**

| Path | Change |
| --- | --- |
| `src/notable_person_finder/config/models.py` | `BraveConfig`, `main.source_policy_file`, wire into `MainConfig`. |
| `src/notable_person_finder/config/loader.py` | Load and validate the policy file; add it to `ResolvedConfig` and the snapshot. |
| `src/notable_person_finder/cli/main.py` | Build the Brave client, register the handler, extend `_compose_seed`, pass coverage summary to the digest, extend `notable status`. |
| `src/notable_person_finder/reporting/digest.py` | `CoverageRunSummary` and its section. |
| `src/notable_person_finder/wikipedia/merge_hooks.py` | Reconcile coverage plans on a confirmed merge. |
| `config/notable.example.toml` | Document the new configuration blocks. |
| `CLAUDE.md` | Record milestone 5a as delivered. |

`coverage/service.py` must stay under roughly 700 lines. `wikipedia/service.py`
reached 3,130 and is the anti-pattern to avoid: if the handler file grows past
that, move plan advancement into a `coverage/advancement.py` rather than
letting it accrete.

---

### Task 1: Publisher policy artifact, loader, and matching

**Files:**
- Create: `config/source-policy.toml`
- Create: `src/notable_person_finder/coverage/__init__.py`
- Create: `src/notable_person_finder/coverage/policy.py`
- Modify: `src/notable_person_finder/config/models.py`
- Modify: `src/notable_person_finder/config/loader.py`
- Test: `tests/coverage/__init__.py`, `tests/coverage/test_policy.py`

**Interfaces:**
- Consumes: `StrictModel`, `_source_path`, `_read_toml`, `_validate`, and
  `ResolvedConfig` from the existing config package; `publisher_key` from
  `ingestion.urls`.
- Produces:
  ```python
  class PolicyStatus(StrEnum):
      ELIGIBLE = "eligible"
      INELIGIBLE = "ineligible"

  class Disposition(StrEnum):
      ELIGIBLE = "eligible"
      INELIGIBLE = "ineligible"
      UNCLASSIFIED = "unclassified"

  class SourcePolicyRule(StrictModel):
      rule_id: str
      domain: str
      path_prefix: str | None = None
      status: PolicyStatus
      rationale: str
      decision_basis: str
      source_url: str | None = None
      reviewed_on: date
      rsp_revision: str | None = None

  class SourcePolicyConfig(StrictModel):
      policy_version: str
      rules: tuple[SourcePolicyRule, ...]

  def policy_fingerprint(policy: SourcePolicyConfig) -> str: ...

  @dataclass(frozen=True, slots=True)
  class ScreeningDecision:
      disposition: Disposition
      rule_id: str | None

  def screen(policy: SourcePolicyConfig, canonical_url: str) -> ScreeningDecision: ...
  ```

**Rules:**

1. `screen` matches on the registrable domain from `publisher_key`, so
   `www.example.com` and `example.com` screen identically.
2. A rule with a `path_prefix` matches only when the canonical URL's path
   starts with it; between a bare-domain rule and a path rule on the same
   domain, the longest matching `path_prefix` wins.
3. An unmatched URL is `UNCLASSIFIED` with `rule_id is None`.
4. Duplicate `rule_id` values are a load error.
5. Two rules with the same `(domain, path_prefix)` and different `status` are a
   load error.
6. `policy_fingerprint` is `sha256` over canonical JSON of `policy_version`
   plus rules sorted by `rule_id`, and changes when any rule field changes.
7. `policy_fingerprint` is stable across TOML key reordering and whitespace.

- [ ] **Step 1: Write the failing tests**

`tests/coverage/test_policy.py` — write all of these, each named for the rule
it defends:

```python
def test_screen_matches_registrable_domain_ignoring_www() -> None:
    policy = _policy(_rule("r1", domain="example.com", status="eligible"))
    assert screen(policy, "https://www.example.com/a").disposition is Disposition.ELIGIBLE
    assert screen(policy, "https://example.com/a").rule_id == "r1"


def test_longest_path_prefix_wins_over_bare_domain() -> None:
    policy = _policy(
        _rule("bare", domain="example.com", status="eligible"),
        _rule("pr", domain="example.com", path_prefix="/press", status="ineligible"),
        _rule("deep", domain="example.com", path_prefix="/press/wire", status="eligible"),
    )
    assert screen(policy, "https://example.com/news/x").rule_id == "bare"
    assert screen(policy, "https://example.com/press/x").rule_id == "pr"
    assert screen(policy, "https://example.com/press/wire/x").rule_id == "deep"


def test_unmatched_domain_is_unclassified_with_no_rule_id() -> None:
    decision = screen(_policy(_rule("r1", domain="example.com")), "https://other.test/a")
    assert decision.disposition is Disposition.UNCLASSIFIED
    assert decision.rule_id is None


def test_duplicate_rule_id_is_a_load_error() -> None:
    with pytest.raises(ValidationError):
        SourcePolicyConfig.model_validate(
            {"policy_version": "1", "rules": [_raw("r1", "a.test"), _raw("r1", "b.test")]}
        )


def test_conflicting_status_on_same_target_is_a_load_error() -> None:
    with pytest.raises(ValidationError):
        SourcePolicyConfig.model_validate(
            {
                "policy_version": "1",
                "rules": [
                    _raw("r1", "a.test", status="eligible"),
                    _raw("r2", "a.test", status="ineligible"),
                ],
            }
        )


def test_fingerprint_changes_when_any_rule_field_changes() -> None:
    base = _policy(_rule("r1", domain="a.test", status="eligible"))
    changed = _policy(_rule("r1", domain="a.test", status="ineligible"))
    assert policy_fingerprint(base) != policy_fingerprint(changed)


def test_fingerprint_is_stable_across_rule_order() -> None:
    a = _policy(_rule("r1", domain="a.test"), _rule("r2", domain="b.test"))
    b = _policy(_rule("r2", domain="b.test"), _rule("r1", domain="a.test"))
    assert policy_fingerprint(a) == policy_fingerprint(b)


def test_shipped_policy_file_loads_and_covers_pilot_publishers() -> None:
    policy = _load_shipped_policy()
    for url in _PILOT_ARTICLE_URLS:  # one per pilot publisher, plus Reuters and AP
        assert screen(policy, url).disposition is Disposition.ELIGIBLE
```

`_PILOT_ARTICLE_URLS` must contain one realistic article URL per publisher in
`config/discovery-feeds.example.toml`, plus `reuters.com` and `apnews.com`.
This is also the test that catches a `publisher_key` suffix-table miss: if a
pilot publisher uses a multi-part suffix not in `_TWO_LABEL_SUFFIXES`, this
test fails and the fix is to grow that table in `ingestion/urls.py` with its
own regression test in `tests/ingestion/test_persistence.py`.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/coverage/test_policy.py -v`
Expected: collection error / `ImportError` — which proves only that the file
runs, not that any rule is covered. The mutation step below is the real
evidence.

- [ ] **Step 3: Write `config/source-policy.toml`**

Top-level `policy_version = "2026-08-01.1"`, then an array of tables. Every
rule carries `rule_id`, `domain`, optional `path_prefix`, `status`,
`rationale`, `decision_basis`, optional `source_url`, `reviewed_on`, and
optional `rsp_revision`. Populate:

- every publisher in `config/discovery-feeds.example.toml` as `eligible`;
- `reuters.com` and `apnews.com` as `eligible`;
- an ineligible set covering at minimum `facebook.com`, `x.com`,
  `instagram.com`, `linkedin.com`, `reddit.com`, `medium.com`,
  `substack.com`, `prnewswire.com`, `businesswire.com`, `globenewswire.com`,
  `einpresswire.com`, `wikipedia.org`, `wikidata.org`, `youtube.com`,
  `pinterest.com`, `tumblr.com`.

`decision_basis` is a short free-text justification such as
`"user-generated content"` or `"press-release distribution"`. Do not import or
paraphrase the Perennial Sources table.

- [ ] **Step 4: Implement `coverage/policy.py`**

Pydantic models per the Interfaces block. Enforce rules 4 and 5 in a
`model_validator(mode="after")` on `SourcePolicyConfig`. `screen` derives the
key via `publisher_key(canonical_url)`, collects rules whose `domain` equals
that key, and returns the one with the longest matching `path_prefix`
(treating a bare-domain rule as prefix `""`). `policy_fingerprint` hashes
`json.dumps(..., sort_keys=True, separators=(",", ":"))` of the version plus
rules sorted by `rule_id`, with `date` rendered ISO-8601.

- [ ] **Step 5: Wire the policy into configuration loading**

In `config/models.py` add `source_policy_file: str = "source-policy.toml"` to
`MainConfig`. In `config/loader.py` resolve it with `_source_path` against the
main config's parent, read it with `_read_toml`, validate it with `_validate`
into `SourcePolicyConfig`, add `source_policy: SourcePolicyConfig` to
`ResolvedConfig`, and include the policy version and fingerprint — not the rule
bodies — in the configuration snapshot, following how `feeds_file` is handled
at `config/loader.py:64` and `:231`.

- [ ] **Step 6: Run the tests and the gate**

Run: `uv run pytest tests/coverage/test_policy.py tests/foundation -v`
Expected: PASS.

- [ ] **Step 7: Record mutation evidence for rules 1–7**

For each rule: `cp` the source aside, mutate, run the suite, record which named
test failed, restore, `diff` to confirm restoration. Rule 3 needs a positive
control — confirm some URL does produce a non-`None` `rule_id`, so the
`is None` assertion is not passing vacuously.

- [ ] **Step 8: Commit**

```bash
git add config/source-policy.toml src/notable_person_finder/coverage src/notable_person_finder/config tests/coverage
git commit -m "feat(coverage): versioned publisher policy and screening"
```

---

### Task 2: Brave Web Search adapter

**Files:**
- Create: `src/notable_person_finder/providers/brave.py`
- Modify: `src/notable_person_finder/config/models.py`
- Test: `tests/coverage/test_brave_adapter.py`

**Interfaces:**
- Consumes: `HttpTransport.request`, `HttpResponse`, `ResponseLimit.API`,
  `ProviderFailure`, `FailureCategory`, `parse_retry_after`, `Clock`.
- Produces:
  ```python
  PROVIDER = "brave"
  SEARCH_OPERATION = "search_web"

  @dataclass(frozen=True, slots=True)
  class BraveHit:
      rank: int
      result_id: str | None
      url: str
      title: str
      snippet: str | None
      extra_snippets: tuple[str, ...]
      language: str | None
      page_age: str | None

  @dataclass(frozen=True, slots=True)
  class BraveSearchPage:
      submitted_query: str
      altered_query: str | None
      offset: int
      hits: tuple[BraveHit, ...]
      more_results_available: bool

  class WebSearchClient(Protocol):
      def search_web(self, query: str, *, count: int, offset: int) -> BraveSearchPage: ...

  class HttpxBraveClient:
      def __init__(self, transport: HttpTransport, *, config: BraveConfig,
                   api_key: str, clock: Clock) -> None: ...
  ```
  and in `config/models.py`:
  ```python
  class BraveConfig(_StrictConfigurationModel):
      endpoint: str = "https://api.search.brave.com/res/v1/web/search"
      count: int = Field(default=10, strict=True, ge=1, le=20)
      max_pages_per_query: int = Field(default=1, strict=True, ge=1, le=5)
      max_queries_per_plan: int = Field(default=6, strict=True, ge=1, le=16)
      max_results_per_query: int = Field(default=20, strict=True, ge=1, le=100)
      retrieval_target: int = Field(default=4, strict=True, ge=1, le=20)
      max_unclassified_selected: int = Field(default=3, strict=True, ge=0, le=20)
      max_query_characters: int = Field(default=400, strict=True, ge=1, le=1000)
      refresh_interval_hours: int = Field(default=720, strict=True, ge=1, le=87_600)
  ```
  `BraveConfig.endpoint` reuses the HTTPS/public-URL validator pattern from
  `MediaWikiConfig.public_https_endpoint` at `config/models.py:196`.
  `MainConfig` gains `brave: BraveConfig = BraveConfig()`.

**Rules:**

1. One `search_web` call issues exactly one `transport.request`.
2. Request parameters always include `country=ALL`, `search_lang=en`,
   `safesearch=moderate`, `spellcheck=0`, `result_filter=web`, plus `q`,
   `count`, and `offset`.
3. The API key travels only in the `X-Subscription-Token` header, and never
   appears in `ProviderFailure.detail` or any exception string.
4. `altered_query` is populated from the response's `query.altered` when
   present and is `None` otherwise.
5. Hit `rank` is the 1-based position within this page, offset by
   `offset * count`, so ranks do not restart per page.
6. HTTP 429 maps to `FailureCategory.RATE_LIMIT` and carries `retry_after_ms`
   from the `Retry-After` header when parseable.
7. HTTP 401 and 403 map to `FailureCategory.AUTHENTICATION`.
8. A response body that is not a JSON object, or that lacks `web.results`,
   maps to `FailureCategory.MALFORMED_RESPONSE`; a present-but-empty
   `web.results` is a valid empty page, not a failure.
9. A hit whose `url` is missing or empty is skipped rather than failing the
   page, and the skip does not renumber the surviving ranks.

- [ ] **Step 1: Write the failing tests**

Follow `tests/wikipedia/test_mediawiki_adapter.py` for the fake-transport
harness. Named tests, one per rule:

```python
def test_one_search_issues_exactly_one_request() -> None: ...
def test_fixed_product_parameters_are_always_sent() -> None: ...
def test_api_key_is_only_in_the_subscription_header() -> None: ...
def test_api_key_never_appears_in_failure_detail() -> None: ...
def test_altered_query_is_captured_when_present() -> None: ...
def test_altered_query_is_none_when_absent() -> None: ...
def test_ranks_continue_across_offset_pages() -> None: ...
def test_rate_limited_response_carries_retry_after_ms() -> None: ...
def test_unauthorized_maps_to_authentication_failure() -> None: ...
def test_missing_web_results_is_malformed_response() -> None: ...
def test_empty_web_results_is_a_valid_empty_page() -> None: ...
def test_hit_without_url_is_skipped_without_renumbering() -> None: ...
```

`test_api_key_never_appears_in_failure_detail` needs a positive control:
assert the key string *is* present in the request headers the fake transport
recorded, then assert it is absent from `str(failure)` and `failure.detail`.
Without the first assertion the second passes vacuously.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/coverage/test_brave_adapter.py -v`
Expected: `ImportError`.

- [ ] **Step 3: Implement `providers/brave.py`**

Mirror `providers/mediawiki.py` in structure: a module docstring stating the
adapter is a translator that decides nothing about relevance or reliability, a
`_STATUS_CATEGORIES` mapping, one `search_web` method that builds params,
calls `transport.request("GET", ...)` with `provider=PROVIDER`,
`operation=SEARCH_OPERATION`, `limit=ResponseLimit.API`, parses JSON, and maps
into the frozen values. No follow-up query, no Brave News, no reranking, no
retry, no sleep.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/coverage/test_brave_adapter.py -v`
Expected: PASS.

- [ ] **Step 5: Record mutation evidence for rules 1–9**

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/providers/brave.py src/notable_person_finder/config/models.py tests/coverage/test_brave_adapter.py
git commit -m "feat(providers): Brave Web Search adapter"
```

---

### Task 3: Coverage schema and repository

**Files:**
- Create: `src/notable_person_finder/db/migrations/0007_coverage_discovery.sql`
- Create: `src/notable_person_finder/coverage/models.py`
- Create: `src/notable_person_finder/coverage/repository.py`
- Test: `tests/coverage/test_schema.py`, `tests/coverage/test_repository.py`

**Interfaces:**
- Consumes: `canonical_article` and `person` tables from migrations 0003 and
  0005; `attempt` and `run` from 0002; `Disposition` from `coverage/policy.py`.
- Produces `coverage/models.py`, which is the single definition site for every
  frozen value later coverage modules exchange. Tasks 4, 5, and 6 import from
  here and define no duplicate:
  ```python
  class QueryStage(StrEnum):
      EXACT = "exact"
      ALIAS = "alias"
      CONTEXTUAL = "contextual"

  @dataclass(frozen=True, slots=True)
  class PlannedQuery:
      stage: QueryStage
      query_text: str
      sourced_name_id: int | None

  @dataclass(frozen=True, slots=True)
  class ScreenedResult:
      rank: int
      provider_result_id: str | None
      url: str
      canonical_url: str | None
      title: str | None
      snippet: str | None
      extra_snippets: tuple[str, ...]
      language: str | None
      disposition: Disposition
      policy_rule_id: str | None
      unusable_url_reason: str | None

  @dataclass(frozen=True, slots=True)
  class SelectionCandidate:
      canonical_url: str
      source: Literal["discovery", "search"]
      disposition: Disposition
      stage_ordinal: int
      rank: int
      result_id: int | None
  ```
  plus the read-side records `PlanRecord`, `QueryRecord`, `ResultRecord`, and
  `SelectionRecord`.
- Produces the tables below and these repository functions:
  ```python
  def open_plan(connection, *, person_id: int, run_id: int,
                evidence_fingerprint: str, policy_fingerprint: str,
                retrieval_target: int, now: str) -> int: ...
  def load_active_plan_for_fingerprint(connection, *, person_id: int,
                                       evidence_fingerprint: str) -> PlanRecord | None: ...
  def load_plan(connection, *, plan_id: int) -> PlanRecord: ...
  def latest_completed_plan_for_person(connection, *, person_id: int) -> PlanRecord | None: ...
  def insert_queries(connection, *, plan_id: int, queries: Sequence[PlannedQuery],
                     first_ordinal: int) -> tuple[int, ...]: ...
  def load_query(connection, *, query_id: int) -> QueryRecord: ...
  def list_queries_for_plan(connection, *, plan_id: int) -> tuple[QueryRecord, ...]: ...
  def next_query_ordinal(connection, *, plan_id: int) -> int: ...
  def mark_query_completed(connection, *, query_id: int, result_count: int,
                           pages_fetched: int, altered_query: str | None,
                           truncated: bool) -> None: ...
  def mark_query_failed(connection, *, query_id: int, failure_category: str) -> None: ...
  def insert_results(connection, *, query_id: int,
                     results: Sequence[ScreenedResult]) -> tuple[int, ...]: ...
  def list_results_for_plan(connection, *, plan_id: int) -> tuple[ResultRecord, ...]: ...
  def record_selection(connection, *, plan_id: int, canonical_article_id: int,
                       source: str, disposition: str, selected_rank: int,
                       first_result_id: int | None, now: str) -> None: ...
  def list_selections_for_plan(connection, *, plan_id: int) -> tuple[SelectionRecord, ...]: ...
  def count_selected(connection, *, plan_id: int, disposition: str) -> int: ...
  def mark_plan_status(connection, *, plan_id: int, status: str,
                       incomplete_reason: str | None, now: str) -> None: ...
  def supersede_plans_for_person(connection, *, person_id: int, now: str) -> int: ...
  def repoint_plans_to_person(connection, *, from_person_id: int,
                              to_person_id: int) -> int: ...
  ```

**Schema:** `0007_coverage_discovery.sql` creates exactly these, following the
`CHECK`-heavy, index-explicit style of `0006_wikipedia_identity.sql`:

- `coverage_plan(id, person_id → person, run_id → run, evidence_fingerprint
  TEXT length 64, policy_fingerprint TEXT, retrieval_target INTEGER,
  status IN ('searching','completed','failed','superseded'), incomplete_reason
  TEXT NULL, created_at, completed_at)` with a partial unique index
  `coverage_plan_active ON (person_id, evidence_fingerprint) WHERE status =
  'searching'` and `coverage_plan_by_person ON (person_id, id)`.
- `coverage_query(id, plan_id → coverage_plan, ordinal ≥ 1, stage IN
  ('exact','alias','contextual'), query_text non-empty, sourced_name_id →
  sourced_name NULL, status IN ('pending','completed','failed'), pages_fetched
  ≥ 0, result_count NULL or ≥ 0, altered_query TEXT NULL, truncated IN (0,1),
  failure_category TEXT NULL)` with unique `(plan_id, ordinal)`.
- `coverage_result(id, query_id → coverage_query, rank ≥ 1, provider_result_id
  TEXT NULL, url TEXT non-empty, canonical_url TEXT NULL, canonical_article_id
  → canonical_article NULL, title TEXT NULL, snippet TEXT NULL,
  extra_snippets_json TEXT NOT NULL, language TEXT NULL, retrieved_at,
  disposition IN ('eligible','ineligible','unclassified'), policy_rule_id TEXT
  NULL, policy_fingerprint TEXT NOT NULL, unusable_url_reason TEXT NULL)` with
  unique `(query_id, rank)` and an index on `canonical_article_id`.
- `coverage_selection(id, plan_id → coverage_plan, canonical_article_id →
  canonical_article, source IN ('discovery','search'), disposition IN
  ('eligible','unclassified'), selected_rank ≥ 1, first_result_id →
  coverage_result NULL, selected_at)` with unique `(plan_id,
  canonical_article_id)` and unique `(plan_id, selected_rank)`.

**Rules:**

1. Two `searching` plans for the same `(person_id, evidence_fingerprint)`
   cannot coexist; the second insert raises `sqlite3.IntegrityError`.
2. A plan whose status moves off `searching` frees the fingerprint for a new
   plan.
3. `coverage_result` rows for the same canonical article under different
   queries both persist — the unique key is `(query_id, rank)`, never the
   article.
4. `coverage_selection` is unique per `(plan_id, canonical_article_id)`, so an
   article discovered by three queries is selected once.
5. `coverage_result.canonical_url` and `canonical_article_id` are NULL exactly
   when `unusable_url_reason` is non-NULL.
6. `repoint_plans_to_person` moves every plan from the merged-away person to
   the survivor and returns the count moved.
7. The migration's checksum registers cleanly and `notable db migrate` is
   idempotent on a database already at 0007.

- [ ] **Step 1: Write the failing schema and repository tests**

`tests/coverage/test_schema.py` asserts column sets, the four unique indexes,
and each `CHECK` constraint by attempting an invalid insert. Model it on
`tests/wikipedia/test_schema.py`. `tests/coverage/test_repository.py` covers
rules 1–6 with named tests:

```python
def test_second_active_plan_for_same_fingerprint_is_rejected() -> None: ...
def test_superseding_a_plan_frees_the_fingerprint() -> None: ...
def test_same_article_under_two_queries_keeps_both_result_rows() -> None: ...
def test_article_found_by_three_queries_is_selected_once() -> None: ...
def test_unusable_url_result_has_null_canonical_columns() -> None: ...
def test_repoint_plans_moves_every_plan_to_the_survivor() -> None: ...
```

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/coverage/test_schema.py tests/coverage/test_repository.py -v`
Expected: FAIL — no such table `coverage_plan`.

- [ ] **Step 3: Write the migration**

Header comment stating what the migration adds and that there is no reverse
migration. Do not set the `foreign_keys` pragma inside it.

- [ ] **Step 4: Write `coverage/models.py` and `coverage/repository.py`**

Write `coverage/models.py` exactly as the Interfaces block above specifies —
it is the definition site Tasks 4, 5, and 6 import from. Repository
functions are transaction-neutral: they join a caller-owned open transaction
when one exists, exactly as `runs/repository.py:schedule_work` does at line
252. No function commits on the caller's behalf when the caller already holds a
transaction.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/coverage -v`
Expected: PASS.

- [ ] **Step 6: Verify migration idempotence**

Run `uv run notable db migrate` twice against a temporary data root and
confirm the second run is a no-op and the backup behaviour is unchanged.

- [ ] **Step 7: Record mutation evidence for rules 1–6**

Mutate each unique index and each NULL-consistency guard in turn.

- [ ] **Step 8: Commit**

```bash
git add src/notable_person_finder/db/migrations/0007_coverage_discovery.sql src/notable_person_finder/coverage tests/coverage
git commit -m "feat(coverage): coverage discovery schema and repository"
```

---

### Task 4: Deterministic query plan

**Files:**
- Create: `src/notable_person_finder/coverage/queries.py`
- Test: `tests/coverage/test_queries.py`

**Interfaces:**
- Consumes: `SourcedNameKind`, `IdentityFactKind` from `people/models.py`;
  `select_display_name`, `mentions_for_canonical_person` from
  `people/identity.py`.
  `QueryStage` and `PlannedQuery` come from `coverage/models.py` (Task 3);
  do not redefine them here.
- Produces:
  ```python
  @dataclass(frozen=True, slots=True)
  class PersonSearchMaterial:
      display_name: str
      sourced_names: tuple[SourcedName, ...]
      facts: tuple[IdentityFact, ...]
      obituary_supported: bool

  def plan_exact_queries(material, *, config: BraveConfig) -> tuple[PlannedQuery, ...]: ...
  def plan_alias_queries(material, *, config: BraveConfig,
                         already_planned: Sequence[str]) -> tuple[PlannedQuery, ...]: ...
  def plan_contextual_query(material, *, config: BraveConfig,
                            already_planned: Sequence[str]) -> PlannedQuery | None: ...
  def evidence_fingerprint(material, *, policy_fingerprint: str,
                           config: BraveConfig) -> str: ...
  ```

**Rules:**

1. The exact stage produces exactly one query: the display name wrapped in
   double quotes, unmodified.
2. A multipart name is never shortened, initialized, or reordered.
3. The alias stage produces one quoted query per sourced name that is not the
   display name and has a recorded `sourced_name_id`; a name with no
   provenance produces no query.
4. Alias queries are ordered by `sourced_name_id` ascending, so the plan is
   reproducible.
5. A query textually identical to one already planned is dropped rather than
   repeated.
6. The contextual stage produces at most one query: the quoted display name
   plus at most two fact values drawn in the fixed order profession, then
   place, then era. It returns `None` when no usable fact exists.
7. The obituary modifier is appended only when `obituary_supported` is true,
   which the caller sets only from explicit source material.
8. A planned query longer than `config.max_query_characters` is dropped, not
   truncated — a truncated quoted name is a different search.
9. The total planned queries across all stages never exceeds
   `config.max_queries_per_plan`.
10. `evidence_fingerprint` changes when a sourced name is added, when an
    identity fact is added, when the policy fingerprint changes, or when a
    query-shaping config bound changes; it does not change when a mention is
    merely repeated with no new name or fact.

- [ ] **Step 1: Write the failing tests**

One named test per rule. Rule 10 needs four separate positive cases and one
negative:

```python
def test_fingerprint_changes_when_a_sourced_name_is_added() -> None: ...
def test_fingerprint_changes_when_an_identity_fact_is_added() -> None: ...
def test_fingerprint_changes_when_policy_fingerprint_changes() -> None: ...
def test_fingerprint_changes_when_max_queries_per_plan_changes() -> None: ...
def test_fingerprint_is_unchanged_by_a_repeated_mention() -> None: ...
```

The last one is the rule that actually prevents duplicate paid searches, and
is the one most likely to survive its own deletion — give it explicit mutation
evidence.

- [ ] **Step 2: Run and confirm failure**

Run: `uv run pytest tests/coverage/test_queries.py -v`

- [ ] **Step 3: Implement `coverage/queries.py`**

Pure functions only — no SQLite, no config loading, no clock. The module takes
already-assembled material and returns values.

- [ ] **Step 4: Run the tests**

Expected: PASS.

- [ ] **Step 5: Record mutation evidence for rules 1–10**

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/coverage/queries.py tests/coverage/test_queries.py
git commit -m "feat(coverage): deterministic person query plan"
```

---

### Task 5: Screening and selection

**Files:**
- Create: `src/notable_person_finder/coverage/selection.py`
- Test: `tests/coverage/test_selection.py`

**Interfaces:**
- Consumes: `screen`, `Disposition`, `SourcePolicyConfig` from
  `coverage/policy.py`; `canonicalize_article_url`, `UnusableArticleUrl` from
  `ingestion/urls.py`; `BraveHit` from `providers/brave.py`; `ScreenedResult`
  and `SelectionCandidate` from `coverage/models.py` (Task 3) — do not
  redefine them here.
- Produces:
  ```python
  def screen_hits(hits: Sequence[BraveHit], *, policy: SourcePolicyConfig,
                  now: str) -> tuple[ScreenedResult, ...]: ...

  def select_articles(candidates: Sequence[SelectionCandidate], *,
                      config: BraveConfig) -> tuple[SelectionCandidate, ...]: ...

  def retrieval_target_met(selected: Sequence[SelectionCandidate], *,
                           config: BraveConfig) -> bool: ...
  ```

**Rules:**

1. Every hit is screened and returned; an ineligible hit is retained with its
   disposition, never dropped.
2. A hit whose URL raises `UnusableArticleUrl` is retained with
   `canonical_url=None` and the reason recorded, and is never selected.
3. Selection orders by stage ordinal ascending, then rank ascending. It never
   computes a cross-query global relevance score.
4. The discovery article is always selected, on the same terms as any other
   candidate, and is ordered first.
5. Only `eligible` candidates count toward `retrieval_target`.
6. Unclassified candidates are selected only when the eligible set has not met
   `retrieval_target`, and never more than `max_unclassified_selected`.
7. Ineligible candidates are never selected at any target level.
8. An article appearing under several queries is selected once, keeping its
   best (earliest stage, then lowest rank) position.

- [ ] **Step 1: Write the failing tests**

One named test per rule. Rule 7 needs a positive control: the same fixture with
the publisher reclassified as eligible must produce a selection, proving the
`not in selected` assertion is reachable.

```python
def test_ineligible_result_is_retained_with_its_disposition() -> None: ...
def test_unusable_url_is_retained_and_never_selected() -> None: ...
def test_selection_orders_by_stage_then_rank() -> None: ...
def test_discovery_article_is_always_selected_first() -> None: ...
def test_only_eligible_candidates_count_toward_retrieval_target() -> None: ...
def test_unclassified_selected_only_below_target_and_within_cap() -> None: ...
def test_ineligible_is_never_selected_even_when_target_unmet() -> None: ...
def test_eligible_publisher_control_produces_a_selection() -> None: ...
def test_duplicate_article_keeps_its_best_position() -> None: ...
```

- [ ] **Step 2: Run and confirm failure**

- [ ] **Step 3: Implement `coverage/selection.py`**

Pure functions. No SQLite, no HTTP.

- [ ] **Step 4: Run the tests**

- [ ] **Step 5: Record mutation evidence for rules 1–8**

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/coverage/selection.py tests/coverage/test_selection.py
git commit -m "feat(coverage): deterministic screening and article selection"
```

---

### Task 6: `coverage_search` work item, handler, and plan advancement

**Files:**
- Create: `src/notable_person_finder/coverage/service.py`
- Test: `tests/coverage/test_search_handler.py`,
  `tests/coverage/test_advancement.py`

**Interfaces:**
- Consumes: `TaskHandler`, `TaskOutcome`, `TaskPreparation`, `WorkItem`,
  `WorkState`, `WorkerPool`; `runs_repository.schedule_work`; everything from
  Tasks 1–5.
- Produces:
  ```python
  COVERAGE_SEARCH_TASK_TYPE = "coverage_search"
  SUBJECT_KIND_COVERAGE_QUERY = "coverage_query"

  def schedule_coverage_query(connection, *, query_id: int, run_id: int,
                              now: str) -> int: ...
  def coverage_search_fingerprint(*, plan_id: int, query_id: int) -> str: ...
  def build_coverage_search_handler(connection, *, client: WebSearchClient,
                                    config: MainConfig,
                                    policy: SourcePolicyConfig) -> TaskHandler: ...
  def maybe_advance_plan(connection, *, plan_id: int, run_id: int,
                         config: MainConfig, policy: SourcePolicyConfig,
                         now: str) -> None: ...
  ```

**Rules:**

1. `execute` performs exactly one `search_web` call and never touches SQLite.
2. The handler uses `WorkerPool.HTTP` and `reserved_nano_usd = 0` — Brave is
   not on the OpenRouter budget.
3. `persist` writes results and marks the query completed inside the engine's
   settlement transaction; a rollback leaves no orphan results.
4. `persist_failure` marks the query failed with the failure category, so a
   translated-nowhere provider failure still leaves durable evidence.
5. A `ProviderFailure` with `category=RATE_LIMIT` settles `DEFERRED` with
   reason `brave_rate_limited`; the reason token contains no query text and no
   URL.
6. Advancement runs the alias stage only when the exact stage has completed and
   `retrieval_target_met` is false.
7. Advancement runs the contextual stage only when exact and alias stages have
   completed and `retrieval_target_met` is still false.
8. A plan completes with `incomplete_reason=None` when every query completed
   and no bound was exhausted.
9. A plan whose query count hit `max_queries_per_plan` before the stages were
   exhausted completes with `incomplete_reason='query_bound_exhausted'`.
10. A plan with any failed query completes with
    `incomplete_reason='search_failed'`.
11. A plan with any still-pending or deferred query does not complete at all
    and stays `searching`.
12. Advancement is idempotent: calling it twice for the same plan schedules no
    duplicate query and no duplicate work item.
13. A completely empty exact-stage result set still advances to the alias stage
    rather than completing early — an empty search is not a met target.

**Design note for the implementer:** rules 8–11 are the 5a half of the
completeness bar that 5b's `assess_person_lead` depends on. Getting
`incomplete_reason` wrong here is what would later turn a budget deferral into
a false `insufficient_evidence`. Treat rules 9, 10, and 11 as the highest-value
mutation targets in the milestone.

- [ ] **Step 1: Write the failing handler tests**

`tests/coverage/test_search_handler.py` uses a fake `WebSearchClient` and a
real in-memory SQLite migrated to 0007. Model the harness on
`tests/wikipedia/test_http_handlers.py`. One named test per rule 1–5, plus:

```python
def test_execute_never_opens_sqlite() -> None:
    """Passing a connection-free execute payload proves the worker is isolated."""
```

- [ ] **Step 2: Write the failing advancement tests**

`tests/coverage/test_advancement.py`, one named test per rule 6–13.

- [ ] **Step 3: Run both and confirm failure**

Run: `uv run pytest tests/coverage/test_search_handler.py tests/coverage/test_advancement.py -v`

- [ ] **Step 4: Implement `coverage/service.py`**

`prepare` loads the query row and returns its text and offsets as the payload.
`execute` calls `search_web` once and returns a `TaskOutcome` carrying the
parsed page as `payload`. `persist` screens the hits via `screen_hits`,
canonicalizes and upserts canonical articles through the existing ingestion
repository helper, writes `coverage_result` rows, marks the query completed,
then calls `maybe_advance_plan`. `persist_failure` marks the query failed and
calls `maybe_advance_plan` so a failed query still moves the plan to a terminal
state rather than stranding it.

Keep the file under ~700 lines; if advancement pushes past that, split it into
`coverage/advancement.py` and re-export.

- [ ] **Step 5: Run the tests**

Expected: PASS.

- [ ] **Step 6: Record mutation evidence for rules 1–13**

Rules 9, 10, and 11 each need their own recorded kill. For rule 11, the
mutation is to let a pending query complete the plan; confirm a named test
fails, because that mutation is exactly the false-negative the completeness bar
exists to prevent.

- [ ] **Step 7: Commit**

```bash
git add src/notable_person_finder/coverage/service.py tests/coverage/test_search_handler.py tests/coverage/test_advancement.py
git commit -m "feat(coverage): coverage_search handler and plan advancement"
```

---

### Task 7: Seeding and merge reconciliation

**Files:**
- Create: `src/notable_person_finder/coverage/seed_hooks.py`
- Modify: `src/notable_person_finder/wikipedia/merge_hooks.py`
- Test: `tests/coverage/test_seed_hooks.py`

**Interfaces:**
- Consumes: `load_wikipedia_identity_observation_by_fingerprint` and the
  person Wikipedia pointer from `wikipedia/repository.py`; `open_plan`,
  `insert_queries`, `supersede_plans_for_person`, `repoint_plans_to_person`.
- Produces:
  ```python
  def seed_coverage_discovery(connection, *, run_id: int, config: MainConfig,
                              policy: SourcePolicyConfig, now: str) -> int: ...
  def reconcile_coverage_on_merge(connection, *, survivor_person_id: int,
                                  merged_person_id: int, now: str) -> None: ...
  ```

**Rules:**

1. A person whose current Wikipedia observation is `no_matching_page_found`
   gets a plan.
2. A person whose current Wikipedia observation is `uncertain_identity` gets a
   plan — uncertainty never suppresses coverage research.
3. A person whose current Wikipedia observation is `matching_page_found` gets
   no plan.
4. A person with no current Wikipedia observation gets no plan; coverage never
   runs ahead of the identity check.
5. Several mentions resolving to one person in one run produce exactly one
   plan.
6. A person with an active plan for the same evidence fingerprint gets no
   second plan.
7. A person whose evidence fingerprint has changed since its last completed
   plan gets exactly one new plan.
8. A completed plan older than `refresh_interval_hours` permits one new plan.
9. On a confirmed merge, the merged-away person's plans repoint to the
   survivor and the survivor's active plan is superseded so the next run
   replans against combined evidence.

- [ ] **Step 1: Write the failing tests**

One named test per rule. Rules 3 and 4 need positive controls — the same
fixture with the observation flipped to `no_matching_page_found` must produce a
plan, proving the "no plan" assertions are reachable.

- [ ] **Step 2: Run and confirm failure**

- [ ] **Step 3: Implement `coverage/seed_hooks.py`**

Follow `wikipedia/service.py:seed_wikipedia_identity` for the query shape and
`wikipedia/merge_hooks.py` for the merge seam.

- [ ] **Step 4: Extend `wikipedia/merge_hooks.py`**

Call `reconcile_coverage_on_merge` from the existing confirmed-merge hook, in
the same transaction as the merge.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/coverage tests/wikipedia -v`
Expected: PASS, with no wikipedia regression.

- [ ] **Step 6: Record mutation evidence for rules 1–9**

- [ ] **Step 7: Commit**

```bash
git add src/notable_person_finder/coverage/seed_hooks.py src/notable_person_finder/wikipedia/merge_hooks.py tests/coverage/test_seed_hooks.py
git commit -m "feat(coverage): coverage seeding and merge reconciliation"
```

---

### Task 8: Digest and status reporting

**Files:**
- Modify: `src/notable_person_finder/reporting/digest.py`
- Modify: `src/notable_person_finder/cli/main.py`
- Create: `src/notable_person_finder/coverage/reporting.py`
- Test: `tests/coverage/test_reporting.py`

**Interfaces:**
- Produces:
  ```python
  @dataclass(frozen=True, slots=True)
  class CoverageRunSummary:
      plans_created: int
      plans_completed: int
      plans_incomplete: int
      queries_by_stage: Mapping[str, int]
      results_by_disposition: Mapping[str, int]
      articles_selected: int
      unclassified_selected: int
      incomplete_reasons: Mapping[str, int]

  def coverage_run_summary(connection, *, run_id: int) -> CoverageRunSummary: ...
  ```
  `render_digest` and `write_digest` in `reporting/digest.py` gain
  `coverage: CoverageRunSummary | None = None`, following the existing
  `wikipedia: WikipediaRunSummary | None = None` parameter at
  `reporting/digest.py:138`.

**Rules:**

1. The digest renders a `### Coverage discovery` section when `coverage` is
   supplied and omits it entirely when `coverage is None`.
2. Counts are scoped to the run: a plan created by an earlier run is not
   counted in this run's `plans_created`.
3. `incomplete_reasons` renders one line per distinct reason with its count.
4. `notable status` reports durable coverage counters: active plans, completed
   plans, people awaiting coverage, and articles selected but not yet fetched.
5. No query text, result URL, or article title appears in the digest coverage
   section — it is counters only in 5a, because the shortlist is still a
   placeholder until 5b.

- [ ] **Step 1: Write the failing tests**

One named test per rule. Rule 5 needs a positive control: assert a known query
string *is* present in the database fixture, then assert it is absent from the
rendered Markdown.

- [ ] **Step 2: Run and confirm failure**

- [ ] **Step 3: Implement `coverage/reporting.py` and extend the digest**

- [ ] **Step 4: Extend `notable status`**

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/coverage tests/foundation -v`

- [ ] **Step 6: Record mutation evidence for rules 1–5**

- [ ] **Step 7: Commit**

```bash
git add src/notable_person_finder/coverage/reporting.py src/notable_person_finder/reporting/digest.py src/notable_person_finder/cli/main.py tests/coverage/test_reporting.py
git commit -m "feat(coverage): coverage counters in digest and status"
```

---

### Task 9: CLI wiring, end-to-end seam, documentation, and the completion gate

**Files:**
- Modify: `src/notable_person_finder/cli/main.py`
- Modify: `config/notable.example.toml`
- Modify: `CLAUDE.md`
- Test: `tests/coverage/test_cli_seam.py`, `tests/coverage/test_live_brave.py`

**Rules:**

1. `notable run` with a Brave key configured builds `HttpxBraveClient`,
   registers `COVERAGE_SEARCH_TASK_TYPE`, and seeds coverage discovery in
   `_compose_seed` after `seed_wikipedia_identity`.
2. `notable run` without a Brave key fails configuration validation with a
   clear message, exactly as a missing OpenRouter key does at
   `cli/main.py:346`.
3. The Brave key never appears in the configuration snapshot, the fingerprint,
   the digest, or any log line.
4. One end-to-end run over stub providers takes a person from
   `no_matching_page_found` through a completed coverage plan with selected
   articles, and the digest reports it.
5. `notable config validate` rejects a malformed `source-policy.toml` with a
   path-qualified error.

- [ ] **Step 1: Write the failing CLI seam test**

`tests/coverage/test_cli_seam.py` drives `notable run` end to end against stub
feed, MediaWiki, OpenRouter, and Brave clients on a temporary data root — model
it on the existing wikipedia CLI seam test. Rule 3 needs a positive control:
put a recognizable sentinel key in the environment, assert it reaches the fake
Brave client's headers, then assert it appears in none of the snapshot, digest,
or captured log output.

- [ ] **Step 2: Write the opt-in live smoke**

`tests/coverage/test_live_brave.py`, marked `@pytest.mark.live`, performing one
real exact-name search against Brave and asserting the page parses. It must
skip cleanly when `BRAVE_API_KEY` is absent and must never assert on specific
result content.

- [ ] **Step 3: Run and confirm failure**

Run: `uv run pytest tests/coverage/test_cli_seam.py -v`

- [ ] **Step 4: Wire the CLI**

Add the Brave client to the `with` block at `cli/main.py:352`, scoped inside
the same context so it shuts down before the connection closes. Register the
handler in the `handlers` dict. Add `seed_coverage_discovery` to
`_compose_seed`. Pass `coverage_run_summary(...)` to `write_digest`.

- [ ] **Step 5: Document the configuration**

Add `[brave]` and `source_policy_file` to `config/notable.example.toml` with
commented defaults. `[lead_policy]` belongs to 5b — do not add it here.

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/coverage -v`
Expected: PASS, live tests deselected.

- [ ] **Step 7: Record mutation evidence for rules 1–5**

- [ ] **Step 8: Update `CLAUDE.md`**

Move Brave web search out of "Not built at all". Add coverage discovery to
"What Is Actually Built", and add to the partial list that article fetch,
extraction, assessment, and lead outcomes remain unbuilt. Add
`uv run pytest tests/coverage` to the verification section and to the combined
gate. Add `coverage/` to the Rewrite Structure list.

- [ ] **Step 9: Run the full completion gate**

```bash
uv run ruff check .
uv run ruff format .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage
git diff --check
git status --short
```

All must pass and both git checks must be clean. Note in the completion report
that the Brave live smoke (`uv run pytest tests/coverage -m live -v`) still
requires an operator run with a real key before cutover.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "feat(coverage): wire coverage discovery into the run (milestone 5a)"
```

---

## Out of Scope

Deliberately not in 5a, to prevent scope drift:

- Article fetch, extraction, and passage selection.
- The `assess_article` task and any prompt.
- `assess_person_lead`, lead assessments, and the lead decision table.
- Ranking, the digest queue, `compose_lead_summary`, and digest shortlist
  entries.
- Any change to `ingestion/urls.py` beyond growing `_TWO_LABEL_SUFFIXES` if
  Task 1's pilot-publisher test proves a publisher is mis-split.
