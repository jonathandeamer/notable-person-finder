# Phase 3: Coverage Research Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `policy.py`, `coverage_contract.py`, and `coverage.py` so
the pipeline can, given a research-worthy mention, search for English-language
coverage, screen it against publisher policy, fetch and assess the articles
worth assessing, and produce a tuple of `ArticleAssessment` records.

**Scope boundary — read this first:** this plan does **not** wire
`coverage.research` into `pipeline.py`. Phase 4 ("Ranking and the full
digest" in the master spec's build order) pairs `coverage.research` with
`rank.assess` at the same time, since calling `coverage.research` and
discarding its result with nothing to consume it would be dead code. This
phase proves `coverage.py` correct and validated in isolation — real unit
tests plus a dedicated live smoke that calls `coverage.research` directly,
mirroring how `test_live_detection_smoke` already exercises one piece
directly without a full pipeline run.

**Architecture:** `policy.py` classifies a URL against the ported publisher
policy TOML. `coverage_contract.py` holds the pure `assess_article`
models/schema (no custom validator needed — every rule the prompt states is
schema-expressible). `coverage.py` orchestrates: one Brave search, screening,
a bounded fetch+extract loop (Trafilatura), and one `assess_article` call per
surviving article. `http.py` gains an optional streaming size bound so
article fetch can't be tricked by a missing or lying `Content-Length`.

**Tech Stack:** Python 3.13, pydantic, httpx (streaming), Trafilatura (new
dependency), pytest, the Brave Web Search API.

## Global Constraints

- Source stays under 3,000 lines total: check with
  `find src -name '*.py' | xargs wc -l | tail -1` before and after. Current
  baseline is ~2,389 lines; this phase is expected to add ~450, landing
  around 2,840 — comfortably under budget but worth re-checking, not assuming.
- No new SQLite table; `coverage.py` reads and writes no durable state.
- Conventional Commits are enforced (`git config core.hooksPath .githooks`).
- Checks before each commit that touches source: `uv run ruff check .`,
  `uv run ruff format .`, `uv run pyright`.
- Tests: `uv run pytest tests/mvp` (live tests are deselected by default).
- Continue on the existing feature branch/worktree used for Phase 2, or a
  fresh one forked from `mvp` — either way, do not push, open a PR, merge, or
  target `main` without explicit user authorization, per `CLAUDE.md`.
- Authorities: `docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`
  (master spec), `docs/superpowers/specs/2026-08-04-coverage-research-design.md`
  (this phase's design), `docs/findings.md`.
- Only `curated_eligible` and `unclassified` screening statuses ever reach
  fetch/assess; `curated_ineligible` is dropped at screening, before any
  network call beyond the Brave search itself.
- A single article's fetch/extract/content-type failure drops that article
  and continues; only the Brave search call and `assess_article` failures
  raise `Incomplete` for the whole mention.
- Failed article fetches are never cached, per the shared cache contract
  ("only validated successes are cached") — this was reconsidered during
  design review and deliberately left unchanged; do not add per-provider
  cache-failure exceptions.

---

## Task 1: Config — Brave, coverage, and the `assess_article` task

**Files:**
- Modify: `src/notable/config.py`
- Modify: `config/notable.example.toml`
- Modify: `tests/mvp/conftest.py`
- Modify: `tests/mvp/test_config.py`

**Interfaces:**
- Consumes: `notable.config.ModelTaskConfig` (already exists from Phase 2).
- Produces: `notable.config.BraveConfig` (fields `endpoint`,
  `max_search_results`, `max_articles_per_mention`), `notable.config.
  CoverageConfig` (fields `source_policy_path: Path`,
  `max_article_characters`, `max_article_bytes`), `Config.assess:
  ModelTaskConfig`, `Config.brave: BraveConfig`, `Config.coverage:
  CoverageConfig`, `Config.brave_api_key: str`, all populated by
  `load_config`. Later tasks import `BraveConfig`/`CoverageConfig` from
  `notable.config` (test fixtures only — `coverage.py` reads them off
  `Config`, not by importing the classes directly).

- [ ] **Step 1: Write the failing config tests**

Replace the `BASE` fixture in `tests/mvp/test_config.py`:

```python
BASE = """
schema_version = 1
feeds_file = "feeds.toml"
data_dir = "data"
digest_dir = "digests"

[secrets]
openrouter_api_key = "TEST_OR_KEY"
brave_api_key = "TEST_BRAVE_KEY"

[transport]
contact_url = "https://example.com/contact"

[cache]
dir = "cache"

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[tasks.detect_people]
model = "openai/gpt-5.4-mini"

[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"

[tasks.assess_article]
model = "openai/gpt-5.4-mini"
"""
```

Update every existing test in the file that calls `monkeypatch.setenv` for
`TEST_OR_KEY` to also set `TEST_BRAVE_KEY`:

```python
def test_loads_feeds_and_resolves_paths_relative_to_the_config_file(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    config = load_config(_write(tmp_path, BASE))
    assert [feed.key for feed in config.feeds] == ["a"]
    assert config.data_dir == tmp_path / "data"
    assert config.cache.dir == tmp_path / "cache"
```

Apply the same `monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")` addition
to `test_secret_comes_from_the_environment_not_the_file`,
`test_unknown_key_is_rejected`, `test_duplicate_feed_keys_are_rejected`,
`test_defaults_match_the_spec`, and `test_budget_parses_as_decimal`. Leave
`test_missing_secret_is_a_clear_error` as-is (it deliberately omits
`TEST_OR_KEY` and must still fail on that check first).

Add to `test_defaults_match_the_spec` (this test already receives `tmp_path`
as a fixture argument; `_write` places `notable.toml` directly in `tmp_path`,
so `source_policy_path` — which defaults to `"source_policies/visual_arts.
toml"` resolved against the config file's own directory — resolves to
`tmp_path / "source_policies" / "visual_arts.toml"`):

```python
    assert config.assess.model == "openai/gpt-5.4-mini"
    assert config.brave.endpoint == "https://api.search.brave.com/res/v1/web/search"
    assert config.brave.max_search_results == 10
    assert config.brave.max_articles_per_mention == 5
    assert config.coverage.max_article_characters == 6000
    assert config.coverage.max_article_bytes == 2_000_000
    assert config.coverage.source_policy_path == (
        tmp_path / "source_policies" / "visual_arts.toml"
    )
```

Add a new test for the missing-Brave-key error:

```python
def test_missing_brave_secret_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.delenv("TEST_BRAVE_KEY", raising=False)
    with pytest.raises(ValueError, match="TEST_BRAVE_KEY"):
        load_config(_write(tmp_path, BASE))
```

Add `from pathlib import Path` to the top of the file if not already present
(it already is, per the existing `EXAMPLE = Path(...)` line).

Update `test_shipped_example_config_is_loadable` and
`test_shipped_example_writes_runtime_state_where_gitignore_covers_it` to also
set the Brave env var:

```python
def test_shipped_example_config_is_loadable(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    monkeypatch.setenv("BRAVE_API_KEY", "sk-test")
    config = load_config(EXAMPLE)
    assert len(config.feeds) == 10


def test_shipped_example_writes_runtime_state_where_gitignore_covers_it(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    monkeypatch.setenv("BRAVE_API_KEY", "sk-test")
    config = load_config(EXAMPLE)
    root = EXAMPLE.resolve().parent.parent
    assert config.data_dir == root / "data"
    assert config.digest_dir == root / "digests"
    assert config.cache.dir == root / "cache"

    ignored = Path(".gitignore").read_text("utf-8").splitlines()
    for pattern in ("/data/", "/digests/", "/cache/"):
        assert pattern in ignored, f"{pattern} missing from .gitignore"
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_config.py -v`
Expected: failures — `config.assess`/`config.brave`/`config.coverage`/
`config.brave_api_key` don't exist yet, and the shipped-example tests fail
on the missing `[tasks.assess_article]` section and missing `BRAVE_API_KEY`.

- [ ] **Step 3: Implement the config changes**

In `src/notable/config.py`, add three new classes after `MediaWikiConfig`:

```python
class BraveConfig(_Strict):
    endpoint: str = "https://api.search.brave.com/res/v1/web/search"
    max_search_results: int = Field(default=10, ge=1, le=20)
    max_articles_per_mention: int = Field(default=5, ge=1)


class CoverageConfig(_Strict):
    source_policy_path: Path
    max_article_characters: int = Field(default=6000, ge=1)
    max_article_bytes: int = Field(default=2_000_000, ge=1)


class _CoverageFile(_Strict):
    source_policy_file: str = "source_policies/visual_arts.toml"
    max_article_characters: int = 6000
    max_article_bytes: int = 2_000_000
```

Update `_Secrets`:

```python
class _Secrets(_Strict):
    openrouter_api_key: str = "OPENROUTER_API_KEY"
    brave_api_key: str = "BRAVE_API_KEY"
```

Update `Config` — add four fields after `match`:

```python
class Config(_Strict):
    feeds: tuple[Feed, ...]
    data_dir: Path
    digest_dir: Path
    digest_size: int = Field(default=20, ge=1)
    resurface_after_days: int = Field(default=30, ge=1)
    max_item_attempts: int = Field(default=3, ge=1, le=10)
    transport: TransportConfig
    cache: CacheConfig
    detect: DetectConfig
    match: ModelTaskConfig
    assess: ModelTaskConfig
    mediawiki: MediaWikiConfig
    brave: BraveConfig
    coverage: CoverageConfig
    openrouter: OpenRouterConfig
    budget_usd: Decimal | None
    openrouter_api_key: str = Field(min_length=1, repr=False)
    brave_api_key: str = Field(min_length=1, repr=False)
```

Update `_File`:

```python
class _File(_Strict):
    schema_version: int
    feeds_file: str
    data_dir: str = "data"
    digest_dir: str = "digests"
    digest_size: int = 20
    resurface_after_days: int = 30
    max_item_attempts: int = 3
    secrets: _Secrets = _Secrets()
    transport: TransportConfig
    cache: CacheConfig = CacheConfig()
    mediawiki: MediaWikiConfig = MediaWikiConfig()
    brave: BraveConfig = BraveConfig()
    coverage: _CoverageFile = _CoverageFile()
    openrouter: OpenRouterConfig = OpenRouterConfig()
    budget: _Budget = _Budget()
    tasks: dict[str, dict[str, Any]]
```

In `load_config`, after the existing `detect`/`match` lookups, add:

```python
    detect = _task_config(parsed, "detect_people", DetectConfig, path)
    match = _task_config(parsed, "match_wikipedia_identity", ModelTaskConfig, path)
    assess = _task_config(parsed, "assess_article", ModelTaskConfig, path)
```

After the existing `openrouter_api_key` lookup block, add the Brave key
lookup (same shape, different variable):

```python
    or_variable = parsed.secrets.openrouter_api_key
    or_key = os.environ.get(or_variable, "").strip()
    if not or_key:
        raise ValueError(
            f"missing OpenRouter API key: set the {or_variable} environment "
            f"variable, or add it to {root / '.env'}"
        )

    brave_variable = parsed.secrets.brave_api_key
    brave_key = os.environ.get(brave_variable, "").strip()
    if not brave_key:
        raise ValueError(
            f"missing Brave API key: set the {brave_variable} environment "
            f"variable, or add it to {root / '.env'}"
        )
```

(This renames the existing local variable from `api_key` to `or_key` for
symmetry with `brave_key` — update the one place it's referenced in the
final `Config(...)` call below.)

Update the `Config(...)` call at the end of `load_config`:

```python
    return Config(
        feeds=feeds_file.feeds,
        data_dir=(root / parsed.data_dir).resolve(),
        digest_dir=(root / parsed.digest_dir).resolve(),
        digest_size=parsed.digest_size,
        resurface_after_days=parsed.resurface_after_days,
        max_item_attempts=parsed.max_item_attempts,
        transport=parsed.transport,
        cache=parsed.cache.model_copy(
            update={"dir": (root / parsed.cache.dir).resolve()}
        ),
        detect=detect,
        match=match,
        assess=assess,
        mediawiki=parsed.mediawiki,
        brave=parsed.brave,
        coverage=CoverageConfig(
            source_policy_path=(root / parsed.coverage.source_policy_file).resolve(),
            max_article_characters=parsed.coverage.max_article_characters,
            max_article_bytes=parsed.coverage.max_article_bytes,
        ),
        openrouter=parsed.openrouter,
        budget_usd=None if budget is None else Decimal(budget),
        openrouter_api_key=or_key,
        brave_api_key=brave_key,
    )
```

- [ ] **Step 4: Update `config/notable.example.toml`**

Add `brave_api_key` to the `[secrets]` section:

```toml
[secrets]
# Names of environment variables. Never put a key in this file.
openrouter_api_key = "OPENROUTER_API_KEY"
brave_api_key = "BRAVE_API_KEY"
```

Append after the existing `[tasks.match_wikipedia_identity]` section:

```toml
[tasks.assess_article]
model = "openai/gpt-5.4-mini"
max_completion_tokens = 4096
reasoning_effort = "low"

[brave]
endpoint = "https://api.search.brave.com/res/v1/web/search"
max_search_results = 10
max_articles_per_mention = 5

[coverage]
source_policy_file = "source_policies/visual_arts.toml"
max_article_characters = 6000
max_article_bytes = 2000000
```

- [ ] **Step 5: Update `tests/mvp/conftest.py`'s `make_config` fixture**

```python
from notable.config import (
    BraveConfig,
    CacheConfig,
    Config,
    CoverageConfig,
    DetectConfig,
    Feed,
    MediaWikiConfig,
    ModelTaskConfig,
    OpenRouterConfig,
    TransportConfig,
)
```

```python
@pytest.fixture
def make_config(tmp_path) -> Callable[..., Config]:
    def build(**overrides) -> Config:
        base = Config(
            feeds=(Feed(key="a", label="Feed A", url="https://a.test/rss"),),
            data_dir=tmp_path / "data",
            digest_dir=tmp_path / "digests",
            transport=TRANSPORT,
            cache=CacheConfig(dir=tmp_path / "cache"),
            detect=DetectConfig(model="m"),
            match=ModelTaskConfig(model="m"),
            assess=ModelTaskConfig(model="m"),
            mediawiki=MediaWikiConfig(),
            brave=BraveConfig(),
            coverage=CoverageConfig(
                source_policy_path=Path("config/source_policies/visual_arts.toml")
            ),
            openrouter=OpenRouterConfig(),
            budget_usd=None,
            openrouter_api_key="sk-test",
            brave_api_key="sk-test-brave",
        )
        return base.model_copy(update=overrides) if overrides else base

    return build
```

Add `from pathlib import Path` to the top of `conftest.py` if not already
present.

- [ ] **Step 6: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp -v`
Expected: PASS across every file — every test file that builds a `Config`
goes through `make_config`, so this fixture change must keep them all green.

- [ ] **Step 7: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format . && uv run pyright`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add src/notable/config.py config/notable.example.toml tests/mvp/conftest.py tests/mvp/test_config.py
git commit -m "feat(config): add assess_article, brave, and coverage settings"
```

---

## Task 2: `http.py` — bounded-size streaming and content-type

**Files:**
- Modify: `src/notable/http.py`
- Test: `tests/mvp/test_http.py`

**Interfaces:**
- Consumes: nothing new (this is a foundational change other tasks build on).
- Produces: `Response.content_type: str | None` (new field, default `None`).
  `Transport.request(..., max_bytes: int | None = None)` (new optional
  parameter; `None`, the default, preserves every existing call site's
  behavior exactly — this must not change any existing test's outcome).
  Task 5 (`coverage.py`) is the only caller that passes `max_bytes` and reads
  `response.content_type`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/mvp/test_http.py`:

```python
def test_a_response_over_max_bytes_is_rejected(tmp_path):
    def handler(request):
        return httpx.Response(200, text="x" * 100)

    with pytest.raises(ProviderFailure) as info:
        _transport(tmp_path, handler).request(
            provider="t",
            method="GET",
            url="https://a.test/x",
            ttl_seconds=None,
            max_bytes=10,
        )
    assert info.value.permanent is True


def test_a_response_under_max_bytes_is_returned_normally(tmp_path):
    def handler(request):
        return httpx.Response(200, text="small")

    result = _transport(tmp_path, handler).request(
        provider="t",
        method="GET",
        url="https://a.test/x",
        ttl_seconds=None,
        max_bytes=1000,
    )
    assert result.text == "small"


def test_max_bytes_is_not_applied_when_absent(tmp_path):
    # Every existing caller omits max_bytes; a large response must still
    # succeed for them, since the default must be "no bound."
    def handler(request):
        return httpx.Response(200, text="x" * 10_000)

    result = _transport(tmp_path, handler).request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert len(result.text) == 10_000


def test_content_type_is_returned_and_cached(tmp_path):
    def handler(request):
        return httpx.Response(
            200, text="ok", headers={"content-type": "text/html; charset=utf-8"}
        )

    transport = _transport(tmp_path, handler)
    first = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    second = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert first.content_type == "text/html; charset=utf-8"
    assert second.content_type == "text/html; charset=utf-8"
    assert second.from_cache is True


def test_content_type_defaults_to_none_when_absent(tmp_path):
    result = _transport(tmp_path, lambda r: httpx.Response(200, text="ok")).request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert result.content_type is None
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_http.py -v`
Expected: `TypeError: request() got an unexpected keyword argument 'max_bytes'`
for the size tests, and `AttributeError` (no `content_type` field) for the
content-type tests.

- [ ] **Step 3: Implement the `http.py` changes**

Add `content_type` to `Response`, right after `text`:

```python
@dataclass(frozen=True, slots=True)
class Response:
    status: int
    text: str
    content_type: str | None = None
    # A cache hit cost nothing. `llm.py` relies on this to keep replayed calls
    # out of the run's spend: counting them would report money that was never
    # spent and could trip the budget cap during a free crash replay.
    from_cache: bool = False
    # Stores this response. A no-op unless the call was made with
    # `defer_cache=True` and has not been committed yet. The caller invokes it
    # once the response has passed every downstream check.
    commit: Callable[[], None] = _noop

    def json(self) -> Any:
        return json.loads(self.text)
```

In `Transport.request`, add the `max_bytes` parameter and thread it through.
Replace the method signature:

```python
    def request(
        self,
        *,
        provider: str,
        method: str,
        url: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        ttl_seconds: int | None,
        timeout: float | None = None,
        max_bytes: int | None = None,
        extra_key: dict[str, object] | None = None,
        auth_token: str | None = None,
        auth_header: str = "Authorization",
        bypass_cache: bool = False,
        defer_cache: bool = False,
    ) -> Response:
```

Update the cache-hit branch to restore `content_type`:

```python
        if not bypass_cache:
            cached = self._cache.get(key, ttl_seconds=ttl_seconds)
            if cached is not None:
                self.cache_hits += 1
                return Response(
                    status=int(cached["status"]),
                    text=str(cached["text"]),
                    content_type=cached.get("content_type"),
                    from_cache=True,
                )
```

Update the `_send` call to pass `max_bytes`:

```python
        response = self._send(
            method=method,
            url=url,
            params=params,
            json_body=json_body,
            headers=headers,
            timeout=timeout or self._config.read_timeout_seconds,
            max_bytes=max_bytes,
        )
```

Update `commit`'s cache write to include `content_type`:

```python
        def commit() -> None:
            nonlocal stored
            if stored:
                return
            stored = True
            self._cache.put(
                key,
                {
                    "status": response.status,
                    "text": response.text,
                    "content_type": response.content_type,
                },
            )
```

Replace `_send`'s signature and body:

```python
    def _send(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None,
        headers: dict[str, str],
        timeout: float,
        max_bytes: int | None = None,
    ) -> Response:
        backoff = self._config.initial_backoff_seconds
        last: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._pace(url)
            self._count(attempts=1, retries=1 if attempt > 1 else 0)
            self._client.cookies.clear()
            try:
                request = httpx.Request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                )
                request.extensions["timeout"] = httpx.Timeout(
                    timeout, connect=self._config.connect_timeout_seconds
                ).as_dict()
                # send() transmits this fully-formed request as-is. Using
                # client.request() would merge client-level headers/cookies
                # back into the request and undermine the cache profile.
                # stream=True only when a size bound is set: reading the
                # body incrementally is how the bound is enforced without
                # trusting a Content-Length header that many servers omit
                # or lie about.
                raw = self._client.send(
                    request, follow_redirects=True, stream=max_bytes is not None
                )
            except httpx.HTTPError as error:
                last = ProviderFailure(
                    f"{type(error).__name__}: {error}", permanent=False
                )
            else:
                try:
                    text = self._read_body(raw, max_bytes, url)
                finally:
                    if max_bytes is not None:
                        raw.close()
                if raw.status_code < 400:
                    self._client.cookies.clear()
                    return Response(
                        status=raw.status_code,
                        text=text,
                        content_type=raw.headers.get("content-type"),
                    )
                if raw.status_code < 500 and raw.status_code != 429:
                    raise ProviderFailure(
                        f"HTTP {raw.status_code} from {url}", permanent=True
                    )
                if raw.status_code == 429:
                    self._count(rate_limited=1)
                last = ProviderFailure(
                    f"HTTP {raw.status_code} from {url}", permanent=False
                )
            finally:
                # Do not carry Set-Cookie state into a later request, including
                # after failures and redirects handled by httpx.
                self._client.cookies.clear()

            if attempt < self._config.max_attempts:
                logger.info(
                    "retrying %s after %s (attempt %d/%d)",
                    url,
                    last,
                    attempt,
                    self._config.max_attempts,
                )
                self._sleep(backoff)
                backoff *= 2
        raise last or ProviderFailure(f"no response from {url}", permanent=False)

    @staticmethod
    def _read_body(raw: httpx.Response, max_bytes: int | None, url: str) -> str:
        """Read the body, enforcing `max_bytes` by streaming rather than by
        trusting `Content-Length` -- many servers omit it or lie."""
        if max_bytes is None:
            return raw.text
        total = 0
        chunks: list[bytes] = []
        for chunk in raw.iter_bytes():
            total += len(chunk)
            if total > max_bytes:
                raise ProviderFailure(
                    f"response exceeded {max_bytes} bytes from {url}", permanent=True
                )
            chunks.append(chunk)
        return b"".join(chunks).decode(raw.encoding or "utf-8", errors="replace")
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_http.py -v`
Expected: PASS, all tests green, including every pre-existing test in the
file (nothing about the default `max_bytes=None` path changed).

- [ ] **Step 5: Run the full suite**

Run: `uv run pytest tests/mvp -v`
Expected: PASS everywhere — `wiki.py`, `detect.py`, and `feeds.py` all call
`Transport.request` without `max_bytes` and must be completely unaffected.

- [ ] **Step 6: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format . && uv run pyright`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/notable/http.py tests/mvp/test_http.py
git commit -m "feat(http): add a streamed response-size bound and content-type"
```

---

## Task 3: `policy.py` — publisher policy classification

**Files:**
- Create: `src/notable/policy.py`
- Test: `tests/mvp/test_policy.py`

**Interfaces:**
- Consumes: nothing from other new modules; reads
  `config/source_policies/visual_arts.toml` directly (already committed,
  ported in Phase 0).
- Produces: `classify(url: str, policy_path: Path) -> Literal[
  "curated_eligible", "curated_ineligible", "unclassified"]`,
  `canonical_domain(url: str) -> str`. Task 5 (`coverage.py`) imports both.

- [ ] **Step 1: Write the failing tests**

Create `tests/mvp/test_policy.py`:

```python
from pathlib import Path

from notable.policy import canonical_domain, classify

POLICY = Path("config/source_policies/visual_arts.toml")


def test_a_known_eligible_publisher_is_curated_eligible():
    assert (
        classify("https://www.theartnewspaper.com/2026/x", POLICY)
        == "curated_eligible"
    )


def test_a_known_ineligible_publisher_is_curated_ineligible():
    assert classify("https://twitter.com/someone/status/1", POLICY) == (
        "curated_ineligible"
    )


def test_an_unlisted_publisher_is_unclassified():
    assert classify("https://some-random-blog.example/post", POLICY) == "unclassified"


def test_a_subdomain_matches_its_parent_host_suffix():
    assert classify("https://news.artnet.com/x", POLICY) == "curated_eligible"


def test_www_prefix_does_not_change_the_result():
    assert classify("https://www.artforum.com/news/x", POLICY) == "curated_eligible"
    assert classify("https://artforum.com/news/x", POLICY) == "curated_eligible"


def test_first_match_wins_in_file_order():
    # bbc.co.uk and bbc.com are both curated_eligible in the ported policy;
    # this just pins that a rule fires at all for both, not a specific order.
    assert classify("https://www.bbc.co.uk/news/x", POLICY) == "curated_eligible"
    assert classify("https://www.bbc.com/news/x", POLICY) == "curated_eligible"


def test_canonical_domain_strips_www():
    assert canonical_domain("https://www.artforum.com/x") == "artforum.com"
    assert canonical_domain("https://artforum.com/x") == "artforum.com"


def test_canonical_domain_is_case_insensitive():
    assert canonical_domain("https://WWW.ArtForum.com/x") == "artforum.com"
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_policy.py -v`
Expected: `ModuleNotFoundError: No module named 'notable.policy'`.

- [ ] **Step 3: Implement `src/notable/policy.py`**

```python
"""Publisher policy: TOML rules -> eligible/ineligible/unclassified, plus
same-host canonicalization for Phase 4's two-domain qualifying threshold."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

_ScreeningStatus = Literal["curated_eligible", "curated_ineligible", "unclassified"]


@dataclass(frozen=True, slots=True)
class _Rule:
    host_suffix: str
    status: Literal["curated_eligible", "curated_ineligible"]


@cache
def _rules(path: Path) -> tuple[_Rule, ...]:
    parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    return tuple(
        _Rule(host_suffix=rule["match"]["host_suffix"], status=rule["status"])
        for rule in parsed["rules"]
    )


def canonical_domain(url: str) -> str:
    """The bare host, `www.` stripped -- what "the same publisher" means for
    the two-distinct-domain qualifying threshold."""
    return urlsplit(url).netloc.lower().removeprefix("www.")


def classify(url: str, policy_path: Path) -> _ScreeningStatus:
    """First-match-wins over `host_suffix` in file order. No match ->
    unclassified. `host_suffix` is the only match form the ported policy
    file uses; other forms are not implemented until something needs them.
    """
    host = canonical_domain(url)
    for rule in _rules(policy_path):
        if host == rule.host_suffix or host.endswith("." + rule.host_suffix):
            return rule.status
    return "unclassified"
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_policy.py -v`
Expected: PASS, all tests green.

- [ ] **Step 5: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/notable/policy.py tests/mvp/test_policy.py
git commit -m "feat(policy): add publisher policy classification"
```

---

## Task 4: `coverage_contract.py` — the `assess_article` contract

**Files:**
- Create: `src/notable/coverage_contract.py`
- Test: `tests/mvp/test_coverage_contract.py`

**Interfaces:**
- Consumes: nothing from other new modules.
- Produces: `ArticlePassage` (dataclass: `id`, `text`, `truncated`),
  `build_article_passage(text: str, *, max_characters: int) ->
  ArticlePassage`, `GroundedSignal` (pydantic: `kind`, `category`, `claim`,
  `supporting_passage_ids`, `grounding`), `AssessmentOutput` (pydantic:
  `person_relation`, `coverage_depth`, `content_types`,
  `subject_relationship`, `signals`, `supporting_passage_ids`, `rationale`),
  `ArticleAssessment` (dataclass: `url`, `screening_status`,
  `person_relation`, `coverage_depth`, `content_types`,
  `subject_relationship`, `signals`, `rationale`), `AssessInvalid`
  (exception), `assessment_schema() -> dict[str, object]`,
  `validate_assessment(raw: dict) -> AssessmentOutput`. Task 5
  (`coverage.py`) imports all of these.

- [ ] **Step 1: Write the failing tests**

Create `tests/mvp/test_coverage_contract.py`:

```python
import pytest

from notable.coverage_contract import (
    AssessInvalid,
    assessment_schema,
    build_article_passage,
    validate_assessment,
)


def _good(**overrides):
    base = {
        "person_relation": "same_person",
        "coverage_depth": "significant",
        "content_types": ["profile"],
        "subject_relationship": "editorially_independent",
        "signals": [],
        "supporting_passage_ids": ["p1"],
        "rationale": "In-depth profile of the subject.",
    }
    return base | overrides


# -- build_article_passage --------------------------------------------------


def test_passage_is_bounded_and_flagged_when_truncated():
    passage = build_article_passage("x" * 100, max_characters=20)
    assert passage.id == "p1"
    assert len(passage.text) == 20
    assert passage.truncated is True


def test_passage_is_not_flagged_when_it_fits():
    passage = build_article_passage("short", max_characters=100)
    assert passage.truncated is False
    assert passage.text == "short"


# -- schema ------------------------------------------------------------------


def test_schema_root_is_an_object_not_a_union():
    schema = assessment_schema()
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_forbids_additional_properties_everywhere():
    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
                assert set(node["properties"]) == set(node["required"])
            for child in node.values():
                walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(assessment_schema())


def test_schema_bounds_content_types_to_one_through_three_unique_values():
    schema = assessment_schema()
    content_types = schema["properties"]["content_types"]
    assert content_types["minItems"] == 1
    assert content_types["maxItems"] == 3
    assert content_types["uniqueItems"] is True


def test_schema_restricts_passage_ids_to_the_one_passage_this_contract_supplies():
    schema = assessment_schema()
    assert schema["properties"]["supporting_passage_ids"]["items"]["enum"] == ["p1"]
    signal_items = schema["properties"]["signals"]["items"]
    assert signal_items["properties"]["supporting_passage_ids"]["items"][
        "enum"
    ] == ["p1"]


# -- validate_assessment ------------------------------------------------------


def test_valid_output_parses():
    result = validate_assessment(_good())
    assert result.person_relation == "same_person"
    assert result.content_types == ("profile",)


def test_a_signal_may_be_included():
    result = validate_assessment(
        _good(
            signals=[
                {
                    "kind": "attention",
                    "category": "major retrospective",
                    "claim": "First major US retrospective.",
                    "supporting_passage_ids": ["p1"],
                    "grounding": "source_text",
                }
            ]
        )
    )
    assert result.signals[0].category == "major retrospective"


def test_an_unknown_passage_id_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(supporting_passage_ids=["p9"]))


def test_more_than_three_content_types_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(
            _good(content_types=["profile", "review", "interview", "obituary"])
        )


def test_duplicate_content_types_are_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(content_types=["profile", "profile"]))


def test_an_unknown_content_type_is_rejected():
    with pytest.raises(AssessInvalid):
        validate_assessment(_good(content_types=["not_a_real_type"]))


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(AssessInvalid):
        validate_assessment({"nonsense": True})
```

Note: `test_more_than_three_content_types_is_rejected` and
`test_duplicate_content_types_are_rejected` exercise `pydantic`'s own
`Field(min_length=1, max_length=3)` and tuple-uniqueness handling at the
Python level (the JSON-schema `uniqueItems`/`maxItems` constraints govern
what a *model* can emit; the pydantic model must independently reject the
same shapes if handed directly, since `validate_assessment` is also the
front door for anything a test constructs by hand). Implement both: the
schema constraints in `assessment_schema` **and** matching `Field` bounds on
`AssessmentOutput.content_types` (`min_length=1, max_length=3`) plus an
`AfterValidator`-free plain check is not needed for uniqueness if a `set`
comparison is cheaper — see Step 3 for the exact approach.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_coverage_contract.py -v`
Expected: `ModuleNotFoundError: No module named 'notable.coverage_contract'`.

- [ ] **Step 3: Implement `src/notable/coverage_contract.py`**

```python
"""The assess_article contract: models and wire schema.

Pure: no I/O, no network client, no HTML parsing. Every rule here traces to
docs/superpowers/specs/2026-08-04-coverage-research-design.md.

Unlike detect_people and match_wikipedia_identity, this contract needs no
domain validator beyond parsing: every rule the ported prompt states
(content-type dedup/bound, the fixed single passage id, the closed enums) is
directly schema-expressible, so there is no cross-field rule left for code to
check.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

PERSON_RELATIONS = ("same_person", "different_person", "uncertain")
COVERAGE_DEPTHS = ("significant", "passing", "uncertain")
CONTENT_TYPES = (
    "reporting",
    "profile",
    "review",
    "interview",
    "obituary",
    "listing",
    "announcement",
    "press_release",
    "sponsored",
    "other",
)
SUBJECT_RELATIONSHIPS = (
    "editorially_independent",
    "affiliated",
    "self_published",
    "uncertain",
)
ARTICLE_PASSAGE_ID = "p1"


class AssessInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, slots=True)
class ArticlePassage:
    id: Literal["p1"]
    text: str
    truncated: bool


def build_article_passage(text: str, *, max_characters: int) -> ArticlePassage:
    bounded = text[:max_characters]
    return ArticlePassage(id="p1", text=bounded, truncated=bounded != text)


class GroundedSignal(_Strict):
    kind: Literal["attention", "caution"]
    category: str = Field(min_length=1, max_length=100)
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[Literal["p1"], ...] = Field(min_length=1)
    grounding: Literal["source_text"]


class AssessmentOutput(_Strict):
    person_relation: Literal["same_person", "different_person", "uncertain"]
    coverage_depth: Literal["significant", "passing", "uncertain"]
    content_types: tuple[
        Literal[
            "reporting",
            "profile",
            "review",
            "interview",
            "obituary",
            "listing",
            "announcement",
            "press_release",
            "sponsored",
            "other",
        ],
        ...,
    ] = Field(min_length=1, max_length=3)
    subject_relationship: Literal[
        "editorially_independent", "affiliated", "self_published", "uncertain"
    ]
    signals: tuple[GroundedSignal, ...]
    supporting_passage_ids: tuple[Literal["p1"], ...] = Field(min_length=1)
    rationale: str = Field(min_length=1, max_length=1000)

    @field_validator("content_types")
    @classmethod
    def _no_duplicate_content_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("content_types must not contain duplicates")
        return value


@dataclass(frozen=True, slots=True)
class ArticleAssessment:
    """One (mention, article) judgment: the full assess_article output plus
    the screening status coverage.py already knew -- the model never
    re-derives it."""

    url: str
    screening_status: Literal["curated_eligible", "unclassified"]
    person_relation: Literal["same_person", "different_person", "uncertain"]
    coverage_depth: Literal["significant", "passing", "uncertain"]
    content_types: tuple[str, ...]
    subject_relationship: Literal[
        "editorially_independent", "affiliated", "self_published", "uncertain"
    ]
    signals: tuple[GroundedSignal, ...]
    rationale: str


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def assessment_schema() -> dict[str, object]:
    """The wire schema. Root is a plain object -- a root-level `anyOf` is
    rejected under strict mode (docs/findings.md). Every rule the prompt
    states is expressed here directly: content_types is bounded and
    deduplicated by minItems/maxItems/uniqueItems, and every passage-id field
    is enum-restricted to the one passage this contract ever supplies -- so a
    model cannot cite a passage that doesn't exist."""
    passage_ids = {
        "type": "array",
        "items": _string(enum=[ARTICLE_PASSAGE_ID]),
        "minItems": 1,
    }
    return _object(
        {
            "person_relation": _string(enum=list(PERSON_RELATIONS)),
            "coverage_depth": _string(enum=list(COVERAGE_DEPTHS)),
            "content_types": {
                "type": "array",
                "items": _string(enum=list(CONTENT_TYPES)),
                "minItems": 1,
                "maxItems": 3,
                "uniqueItems": True,
            },
            "subject_relationship": _string(enum=list(SUBJECT_RELATIONSHIPS)),
            "signals": {
                "type": "array",
                "items": _object(
                    {
                        "kind": _string(enum=["attention", "caution"]),
                        "category": _string(maxLength=100),
                        "claim": _string(maxLength=1000),
                        "supporting_passage_ids": passage_ids,
                        "grounding": _string(enum=["source_text"]),
                    }
                ),
            },
            "supporting_passage_ids": passage_ids,
            "rationale": _string(maxLength=1000),
        }
    )


def validate_assessment(raw: dict[str, Any]) -> AssessmentOutput:
    """Parse and validate. Every rule the ported prompt states is
    schema-expressible (see `assessment_schema`), so this is parsing plus
    error-type translation, not cross-field checking."""
    try:
        return AssessmentOutput.model_validate(raw)
    except ValidationError as error:
        raise AssessInvalid(f"assessment output failed validation: {error}") from error
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_coverage_contract.py -v`
Expected: PASS, all tests green.

- [ ] **Step 5: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/notable/coverage_contract.py tests/mvp/test_coverage_contract.py
git commit -m "feat(coverage): add the assess_article contract"
```

---

## Task 5: `coverage.py` — Brave, screening, fetch/extract, and assess

**Files:**
- Modify: `pyproject.toml` (add Trafilatura)
- Create: `src/notable/coverage.py`
- Test: `tests/mvp/test_coverage.py`
- Modify: `tests/mvp/test_live.py` (add a live smoke test)

**Interfaces:**
- Consumes: `notable.config.Config` (`.brave`, `.coverage`, `.assess`,
  `.cache`, `.transport`, `.brave_api_key`), `notable.detect_contract.
  DetectedMention` (`.exact_name`, `.identity_facts`), `notable.policy.
  classify`, everything from Task 4's `notable.coverage_contract`,
  `notable.http.Transport.request(...)` including the new `max_bytes`
  parameter and `Response.content_type`, `notable.llm.LlmClient.
  structured(...)`, `notable.errors.Incomplete`, `notable.errors.
  ProviderFailure`.
- Produces: `research(mention: DetectedMention, config: Config, transport:
  Transport, llm: LlmClient) -> tuple[ArticleAssessment, ...]`. No other task
  in this plan calls it — Phase 4 will, alongside `rank.assess`.

- [ ] **Step 1: Add the Trafilatura dependency**

In `pyproject.toml`, add to `dependencies`:

```toml
dependencies = [
  "feedparser>=6.0,<7",
  "httpx>=0.28,<0.29",
  "pydantic>=2.11,<3",
  "python-dotenv>=1.1,<2",
  "trafilatura>=1.12,<2",
  "tzdata>=2025.2",
]
```

Run: `uv sync`
Expected: Trafilatura and its transitive dependencies install; `uv.lock`
updates. Commit this as part of Step 8, not separately.

- [ ] **Step 2: Write the failing tests**

The prompt file `src/notable/prompts/assess_article.md` already exists
(ported verbatim in an earlier phase) — do not edit it.

Create `tests/mvp/test_coverage.py`:

```python
from typing import Any, cast

import httpx
import pytest

from notable.detect_contract import DetectedMention
from notable.errors import BudgetExceeded, Incomplete, ProviderFailure
from notable.llm import LlmClient
from notable.coverage import research


def _mention(name: str = "Ana Poy") -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        outcome="research",
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Named subject.",
    )


class FakeLlm:
    def __init__(self, results: list[dict[str, Any]] | None = None, error=None):
        self.results = list(results or [])
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        result = self.results.pop(0)
        validate = kwargs.get("validate")
        if validate is not None:
            validate(result)
        return result


def _brave_response(urls: list[str]) -> dict:
    return {"web": {"results": [{"url": u, "title": "T", "description": "D"} for u in urls]}}


def _assessment(**overrides) -> dict:
    base = {
        "person_relation": "same_person",
        "coverage_depth": "significant",
        "content_types": ["profile"],
        "subject_relationship": "editorially_independent",
        "signals": [],
        "supporting_passage_ids": ["p1"],
        "rationale": "In-depth profile.",
    }
    return base | overrides


def _handler_for(brave_urls, article_responses):
    """article_responses maps url -> (status, text, content_type) or None
    (meaning: the URL is never fetched -- used to assert ineligible/dropped
    URLs never reach the transport)."""

    def handler(request):
        if "brave" in request.url.host or "search" in str(request.url):
            return httpx.Response(200, json=_brave_response(brave_urls))
        url = str(request.url)
        if url not in article_responses or article_responses[url] is None:
            raise AssertionError(f"unexpected fetch of {url}")
        status, text, content_type = article_responses[url]
        headers = {"content-type": content_type} if content_type else {}
        return httpx.Response(status, text=text, headers=headers)

    return handler


def test_ineligible_results_are_never_fetched(make_config, make_transport):
    # twitter.com is curated_ineligible in the ported policy.
    handler = _handler_for(["https://twitter.com/x"], {})
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_zero_brave_results_short_circuits_to_empty(make_config, make_transport):
    handler = _handler_for([], {})
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_a_dead_link_does_not_fail_the_mention(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a", "https://www.theartnewspaper.com/b"]
    handler = _handler_for(
        urls,
        {
            urls[0]: (404, "not found", "text/html"),
            urls[1]: (200, "<html>body</html>", "text/html"),
        },
    )
    transport = make_transport(handler)
    llm = FakeLlm(results=[_assessment()])
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert len(result) == 1
    assert result[0].url == urls[1]


def test_brave_failure_raises_incomplete(make_config, make_transport):
    def handler(request):
        return httpx.Response(503, text="down")

    transport = make_transport(handler)
    with pytest.raises(Incomplete):
        research(_mention(), make_config(), transport, cast(LlmClient, FakeLlm()))


def test_an_assess_article_failure_raises_incomplete_for_the_whole_mention(
    make_config, make_transport
):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(urls, {urls[0]: (200, "<html>body</html>", "text/html")})
    transport = make_transport(handler)
    llm = FakeLlm(error=ProviderFailure("boom", permanent=False))
    with pytest.raises(Incomplete):
        research(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_budget_exceeded_propagates(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(urls, {urls[0]: (200, "<html>body</html>", "text/html")})
    transport = make_transport(handler)
    llm = FakeLlm(error=BudgetExceeded("cap"))
    with pytest.raises(BudgetExceeded):
        research(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_screening_status_is_carried_not_rederived(make_config, make_transport):
    # theartnewspaper.com is curated_eligible; an unlisted host is unclassified.
    urls = ["https://www.theartnewspaper.com/a", "https://some-blog.example/b"]
    handler = _handler_for(
        urls,
        {
            urls[0]: (200, "<html>a</html>", "text/html"),
            urls[1]: (200, "<html>b</html>", "text/html"),
        },
    )
    transport = make_transport(handler)
    llm = FakeLlm(results=[_assessment(), _assessment()])
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    statuses = {a.url: a.screening_status for a in result}
    assert statuses[urls[0]] == "curated_eligible"
    assert statuses[urls[1]] == "unclassified"


def test_non_html_content_type_is_dropped(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(
        urls, {urls[0]: (200, "{}", "application/json")}
    )
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_max_articles_per_mention_bounds_fetch_count(make_config, make_transport):
    urls = [f"https://www.theartnewspaper.com/{i}" for i in range(8)]
    responses = {u: (200, "<html>body</html>", "text/html") for u in urls}
    fetched = []

    def handler(request):
        if "search" in str(request.url):
            return httpx.Response(200, json=_brave_response(urls))
        url = str(request.url)
        fetched.append(url)
        status, text, content_type = responses[url]
        return httpx.Response(status, text=text, headers={"content-type": content_type})

    transport = make_transport(handler)
    config = make_config()
    llm = FakeLlm(results=[_assessment() for _ in range(config.brave.max_articles_per_mention)])
    research(_mention(), config, transport, cast(LlmClient, llm))
    assert len(fetched) == config.brave.max_articles_per_mention
```

- [ ] **Step 3: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_coverage.py -v`
Expected: `ModuleNotFoundError: No module named 'notable.coverage'`.

- [ ] **Step 4: Implement `src/notable/coverage.py`**

```python
"""Brave search, publisher-policy screening, article fetch + extract, and
the assess_article call. Not a lead outcome yet -- Phase 4's rank.assess
consumes this module's output."""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources
from typing import Any, Literal

import trafilatura

from notable import policy
from notable.config import Config
from notable.coverage_contract import (
    ArticleAssessment,
    AssessInvalid,
    assessment_schema,
    build_article_passage,
    validate_assessment,
)
from notable.detect_contract import DetectedMention
from notable.errors import Incomplete, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient

logger = logging.getLogger(__name__)

_HTML_CONTENT_TYPES = ("text/html", "application/xhtml+xml")


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("assess_article.md")
        .read_text(encoding="utf-8")
    )


def research(
    mention: DetectedMention, config: Config, transport: Transport, llm: LlmClient
) -> tuple[ArticleAssessment, ...]:
    """Search, screen, fetch, and assess English-language coverage of
    `mention`.

    Raises `Incomplete` if the Brave search fails, an assess_article call
    fails, or its output is domain-rejected -- the whole mention retries on
    a later run. A single article's fetch, extraction, or content-type
    failure just drops that article and continues; a dead search result is
    not evidence of a technical failure in this system. Zero surviving
    articles returns `()` with zero model calls, never `Incomplete` -- an
    empty result honestly means the search ran and found nothing usable.
    `BudgetExceeded` propagates unchanged.
    """
    try:
        hits = _search_brave(mention.exact_name, config, transport)
    except ProviderFailure as error:
        logger.warning("brave search failed for %r: %s", mention.exact_name, error)
        raise Incomplete(f"brave search failed for {mention.exact_name!r}") from error

    survivors: list[tuple[str, Literal["curated_eligible", "unclassified"]]] = []
    for url in hits:
        status = policy.classify(url, config.coverage.source_policy_path)
        if status == "curated_ineligible":
            continue
        survivors.append((url, status))
        if len(survivors) >= config.brave.max_articles_per_mention:
            break

    assessments: list[ArticleAssessment] = []
    for url, status in survivors:
        text = _fetch_and_extract(url, config, transport)
        if text is None:
            continue
        passage = build_article_passage(
            text, max_characters=config.coverage.max_article_characters
        )
        assessments.append(_assess_article(mention, url, status, passage, config, llm))
    return tuple(assessments)


def _parsed_json(response: Any, *, context: str) -> dict[str, Any]:
    try:
        data = response.json()
    except (ValueError, TypeError) as error:
        raise ProviderFailure(
            f"unreadable Brave {context} response: {error}", permanent=False
        ) from error
    if not isinstance(data, dict):
        raise ProviderFailure(
            f"unreadable Brave {context} response: not an object", permanent=False
        )
    return data


def _search_brave(name: str, config: Config, transport: Transport) -> tuple[str, ...]:
    response = transport.request(
        provider="brave",
        method="GET",
        url=config.brave.endpoint,
        params={"q": name, "count": config.brave.max_search_results},
        ttl_seconds=config.cache.discovery_ttl_seconds,
        auth_token=config.brave_api_key,
        auth_header="X-Subscription-Token",
    )
    data = _parsed_json(response, context="search")
    try:
        results = data.get("web", {}).get("results", [])
        return tuple(result["url"] for result in results)
    except (KeyError, TypeError, AttributeError) as error:
        raise ProviderFailure(
            f"malformed Brave search response: {error}", permanent=False
        ) from error


def _fetch_and_extract(url: str, config: Config, transport: Transport) -> str | None:
    try:
        response = transport.request(
            provider="article",
            method="GET",
            url=url,
            ttl_seconds=None,  # Stable class: permanent, per the master spec.
            max_bytes=config.coverage.max_article_bytes,
        )
    except ProviderFailure as error:
        logger.info("article fetch failed for %s: %s", url, error)
        return None

    content_type = (response.content_type or "").split(";")[0].strip().lower()
    if content_type not in _HTML_CONTENT_TYPES:
        logger.info("skipping non-HTML article %s (%r)", url, content_type)
        return None

    extracted = trafilatura.extract(
        response.text,
        favor_precision=True,
        include_comments=False,
        include_tables=False,
    )
    if not extracted:
        logger.info("extraction produced no usable text for %s", url)
        return None
    return extracted


def _assess_article(
    mention: DetectedMention,
    url: str,
    status: Literal["curated_eligible", "unclassified"],
    passage: Any,
    config: Config,
    llm: LlmClient,
) -> ArticleAssessment:
    payload = {
        "task": "assess_article",
        "mention_name": mention.exact_name,
        "identity_facts": [
            {"kind": fact.kind, "value": fact.value} for fact in mention.identity_facts
        ],
        "screening_status": status,
        "article_url": url,
        "passages": [
            {"id": passage.id, "text": passage.text, "truncated": passage.truncated}
        ],
    }

    held: list[Any] = []

    def _validate(raw: dict[str, Any]) -> None:
        held.append(validate_assessment(raw))

    try:
        llm.structured(
            task="assess_article",
            model=config.assess.model,
            system=_system_prompt(),
            user_payload=payload,
            schema=assessment_schema(),
            max_completion_tokens=config.assess.max_completion_tokens,
            reasoning_effort=config.assess.reasoning_effort,
            timeout=config.transport.llm_read_timeout_seconds,
            validate=_validate,
        )
    except ProviderFailure as error:
        logger.warning(
            "assess_article failed for %s (%r): %s", url, mention.exact_name, error
        )
        raise Incomplete(f"assess_article failed for {url}") from error
    except AssessInvalid as error:
        logger.warning(
            "assess_article output rejected for %s (%r): %s",
            url,
            mention.exact_name,
            error,
        )
        raise Incomplete(f"assess_article output rejected for {url}") from error

    output = held[0]
    return ArticleAssessment(
        url=url,
        screening_status=status,
        person_relation=output.person_relation,
        coverage_depth=output.coverage_depth,
        content_types=output.content_types,
        subject_relationship=output.subject_relationship,
        signals=output.signals,
        rationale=output.rationale,
    )
```

- [ ] **Step 5: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_coverage.py -v`
Expected: PASS, all tests green. If `test_max_articles_per_mention_bounds_
fetch_count` or others fail because `_handler_for`'s Brave-vs-article
dispatch (`"brave" in request.url.host or "search" in str(request.url)`)
doesn't match `config.brave.endpoint`'s real host, adjust the dispatch
condition to check the actual configured endpoint host instead — this is
the one place the test double's routing logic depends on a real config
value, so verify it against `make_config().brave.endpoint`.

- [ ] **Step 6: Add the live smoke test**

In `tests/mvp/test_live.py`, add (near the existing
`test_live_detection_smoke`, keeping the same opt-in pattern):

```python
@pytest.mark.live
def test_live_coverage_smoke(tmp_path):
    """Opt-in: uv run pytest tests/mvp -m live -v

    Exercises coverage.research directly against real Brave, a real article
    fetch, and a real assess_article call -- the three external surfaces
    this phase adds that findings.md has no data on yet. A cheap, well-known
    query is used so the run is inexpensive and reproducible.
    """
    from notable.coverage import research
    from notable.detect_contract import DetectedMention

    config = load_config(Path("config/mvp.local.toml"))
    assert config.budget_usd is not None, "never run live without a cap"

    client = httpx.Client(follow_redirects=True)
    transport = Transport(config.transport, Cache(tmp_path / "cache"), client=client)
    llm = LlmClient(
        transport,
        config.openrouter,
        api_key=config.openrouter_api_key,
        budget_usd=config.budget_usd,
    )
    mention = DetectedMention(
        exact_name="David Hockney",
        outcome="research",
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Live smoke.",
    )
    result = research(mention, config, transport, llm)
    # The point is that Brave's real response shape, a real fetched page,
    # Trafilatura against real HTML, and the assess_article schema all
    # survive contact with real providers -- not any particular verdict.
    assert llm.spend() > 0, "the assess_article call must report a real cost"
    assert transport.cache_misses > 0
    if result:
        assert result[0].content_types
```

- [ ] **Step 7: Run the full suite (excluding live) and lint/typecheck**

Run: `uv run pytest tests/mvp -v`
Expected: PASS everywhere.

Run: `uv run ruff check . && uv run ruff format . && uv run pyright`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/notable/coverage.py tests/mvp/test_coverage.py tests/mvp/test_live.py
git commit -m "feat(coverage): add Brave search, fetch/extract, and assess_article"
```

---

## Task 6: Verification pass

**Files:** none created or modified — this task only runs checks and records
their output; if any check fails, fix the regression in the file it points to
and re-run before moving on.

- [ ] **Step 1: Confirm the source budget**

Run: `find src -name '*.py' | xargs wc -l | tail -1`
Expected: under 3000. Record the number. If it is close to the limit, note
it plainly rather than silently proceeding — per `CLAUDE.md`, the first
response to a binding limit is to ask what can be removed, not to raise it.

- [ ] **Step 2: Confirm no new SQLite table**

Run: `grep -n "CREATE TABLE" src/notable/store.py`
Expected: still exactly four tables (`item`, `surfaced`, `run`, `lead`) —
this phase introduces none.

- [ ] **Step 3: Full test suite, lint, and typecheck**

Run:
```bash
uv run pytest tests/mvp -v
uv run ruff check .
uv run ruff format --check .
uv run pyright
```
Expected: all clean.

- [ ] **Step 4: Confirm `pipeline.py` is untouched**

Run: `git diff mvp -- src/notable/pipeline.py` (or the appropriate base
branch/commit for this work)
Expected: no output — this phase deliberately does not wire
`coverage.research` into the pipeline (see the plan header's scope
boundary). If this diff is non-empty, something in the task work strayed
outside scope; investigate before proceeding.

- [ ] **Step 5: Live smoke — report readiness, do not run without authorization**

This phase is not done until the live smoke test in `test_live.py`
(`test_live_coverage_smoke`) has actually run against real Brave and
OpenRouter traffic and been inspected, per the master spec's "a written live
smoke is not evidence until it has actually run." That run costs real money
(Brave + one `assess_article` call) and requires `.env` with live
`OPENROUTER_API_KEY` and `BRAVE_API_KEY` values. Surface this explicitly to
the user as the next manual step — do not run it and do not mark the phase
complete without it having actually executed and been inspected (raw
response, extracted text quality, measured spend).

- [ ] **Step 6: Report**

Summarize for the user: line count, test count, and the outstanding
live-smoke step from Step 5.

---

## Self-Review Notes

- **Spec coverage:** the Brave→screen→fetch→extract→assess data flow (Task
  5), the ineligible-skip-before-fetch rule (Task 5's screening loop), the
  per-article-failure-vs-whole-mention-failure boundary (Task 5's
  try/except placement — fetch failures caught per-article, `assess_article`
  failures propagate), the empty-candidate short-circuit (falls out of Task
  5's loop structure with no special-casing needed, per the design doc),
  `policy.py`'s scope (Task 3), the `assess_article` contract (Task 4),
  config additions (Task 1), the streamed size bound and content-type
  addition to `http.py` (Task 2), the accepted "failed fetches are never
  cached" tradeoff (unchanged, per Global Constraints), and the named
  invariant tests from the design doc's table are all covered by Task 2's,
  Task 3's, Task 4's, and Task 5's test files.
- **Scope boundary honored:** `pipeline.py` is not touched by any task in
  this plan (verified explicitly in Task 6, Step 4) — `coverage.research`
  is proven correct in isolation; Phase 4 wires it in alongside
  `rank.assess`.
- **Type consistency checked:** `ArticleAssessment`, `AssessInvalid`,
  `assessment_schema`, `validate_assessment`, and `build_article_passage`
  are defined once in `coverage_contract.py` (Task 4) and imported, never
  redefined, in `coverage.py` (Task 5). `config.brave`/`config.coverage`/
  `config.assess`/`config.brave_api_key` names are consistent across Task
  1's `config.py` changes, Task 5's `coverage.py`, and every test file.
  `Response.content_type` and `Transport.request`'s `max_bytes` parameter
  (Task 2) are consumed by name in Task 5's `_fetch_and_extract` exactly as
  defined.
- **No placeholders:** every step above contains complete, runnable code —
  no "add validation" or "similar to Task N" shorthand. The one exception —
  Task 5 Step 5's note about adjusting `_handler_for`'s dispatch condition —
  is flagged explicitly as a possible adjustment with a concrete fallback
  ("check the actual configured endpoint host instead"), not left open-ended.
