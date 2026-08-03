# Phase 1: Walking Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `notable run` fetches the ten configured feeds, detects people in each new item with one model call, writes a dated Markdown digest plus `latest.md`, and commits its state — with a response cache that makes a crashed re-run nearly free.

**Architecture:** One sequential in-memory pass, then the digest file, then one state commit, then a best-effort log. Every external call passes through a hash-keyed disk cache with TTL classes. No durable intermediate state: a crash before `store.commit` writes nothing and the re-run replays from cache.

**Tech Stack:** Python 3.13, httpx, feedparser, pydantic v2, SQLite (stdlib), pytest.

## Global Constraints

Copied verbatim from `docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. Every task's requirements implicitly include this section.

- **Source stays under 3,000 lines.** No new table without deleting one — four is the budget. `pipeline.py`'s main loop fits on one screen. No new cross-run state without leaving MVP scope.
- **Secrets never appear** in diagnostics, terminal output, tests, or **cache keys**.
- **`http.py` sends one fixed header set and keeps no cookie jar.** No per-request header override is offered.
- **Only validated successes are cached.** Transport failures, timeouts, 429s, 5xx, and model responses failing schema or domain validation are never stored.
- **A validation failure is not retried within a run.** It raises `Incomplete`; the item retries on a later run, bounded by the attempt cap.
- **`run` and `lead` are append-only logs the pipeline never reads**, written by `store.log` in a separate transaction *after* the state tables commit, with failures warned rather than raised.
- **Commit ordering:** digest temp file → `os.replace` → `os.replace` `latest.md` → commit state → write logs.
- **The application never writes Wikipedia content.**
- Conventional Commits are enforced by `.githooks/commit-msg`.

## Phase 1 scope boundaries

The spec describes the finished loop. Phase 1 builds the skeleton it hangs on, so three things are deliberately absent:

- **No `wiki.match`, no `coverage.research`, no `rank.assess`.** Phase 1's digest lists *detected people*, not ranked leads. Inventing a lead outcome without coverage evidence would corrupt the outcome vocabulary; `promising_lead` means something specific.
- **No `surfaced` writes and no suppression.** Suppression operates on the shortlist, which arrives in Phase 4. Writing `surfaced` now would suppress merely-*detected* people from the real shortlist later — a footgun during development.
- **No `lead` rows.** All four tables are created in Phase 1 (the schema is trivial and `CREATE TABLE IF NOT EXISTS` is idempotent), but `lead` is populated from Phase 4.

**One documented deviation from the spec's sketch.** The spec writes `run(cfg, store)`; module-level provider clients would make that untestable. Phase 1 uses `run(cfg, store, providers)` where `providers` is a two-field frozen dataclass. The loop shape and every invariant are unchanged.

---

## File Structure

| Path | Responsibility | Est. lines |
| --- | --- | --- |
| `src/notable/config.py` | One pydantic model; load TOML + `.env` secrets; resolve paths | 150 |
| `src/notable/cache.py` | `cache_key`, atomic write, TTL classes, corrupt-is-miss | 90 |
| `src/notable/store.py` | SQLite: four tables, item lifecycle, commit and log | 130 |
| `src/notable/http.py` | One httpx client: fixed headers, retry, pacing, cache | 140 |
| `src/notable/llm.py` | OpenRouter structured output, spend counter, `BudgetExceeded` | 140 |
| `src/notable/feeds.py` | feedparser → `SourceItem`; URL canonicalization; `fetch_new` | 120 |
| `src/notable/detect_contract.py` | Detection models, wire schema, domain validation | 180 |
| `src/notable/detect.py` | Prompt render, model call, `people_in` | 90 |
| `src/notable/digest.py` | Markdown render, atomic file write | 110 |
| `src/notable/pipeline.py` | The loop | 90 |
| `src/notable/errors.py` | `Incomplete`, `ProviderFailure`, `BudgetExceeded` | 25 |
| `src/notable/cli.py` | Wire everything (modify) | 80 |

**Why `detect_contract.py` is separate from `detect.py`:** the contract is pure — models in, schema and validation out, no I/O — and it carries every rule from `docs/findings.md`. Keeping it callable without a network client is what lets the validation tests be fast and exhaustive.

---

### Task 1: Errors and configuration

**Files:**
- Create: `src/notable/errors.py`
- Create: `src/notable/config.py`
- Create: `config/notable.example.toml`
- Test: `tests/mvp/test_config.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `notable.errors.Incomplete(Exception)`, `notable.errors.ProviderFailure(Exception)` with `.permanent: bool`, `notable.errors.BudgetExceeded(Exception)`.
  - `notable.config.Config` (frozen pydantic model) with fields `feeds: tuple[Feed, ...]`, `data_dir: Path`, `digest_dir: Path`, `digest_size: int`, `resurface_after_days: int`, `max_item_attempts: int`, `transport: TransportConfig`, `cache: CacheConfig`, `detect: DetectConfig`, `openrouter: OpenRouterConfig`, `budget_usd: Decimal | None`, `openrouter_api_key: str`.
  - `notable.config.Feed` with `key: str`, `label: str`, `url: str`.
  - `notable.config.load_config(path: Path) -> Config`.

- [ ] **Step 1: Write `src/notable/errors.py`**

```python
"""The three exceptions that cross module boundaries."""

from __future__ import annotations


class Incomplete(Exception):
    """Research for one item did not reach a terminal state.

    The item is not settled and retries on a later run, bounded by the attempt
    cap. Never convert this into a semantic outcome: doing so would report a
    provider failure as "we looked and found nothing".
    """


class ProviderFailure(Exception):
    """One external call failed."""

    def __init__(self, message: str, *, permanent: bool) -> None:
        super().__init__(message)
        self.permanent = permanent


class BudgetExceeded(Exception):
    """The run's accumulated model spend passed the configured cap."""
```

- [ ] **Step 2: Write the failing test**

`tests/mvp/test_config.py`:

```python
from decimal import Decimal
from pathlib import Path

import pytest

from notable.config import load_config

EXAMPLE = Path("config/notable.example.toml")


def _write(tmp_path: Path, body: str, *, feeds: str | None = None) -> Path:
    (tmp_path / "feeds.toml").write_text(
        feeds
        or 'schema_version = 1\n[[feeds]]\nkey = "a"\nlabel = "A"\nurl = "https://a.test/f"\n',
        encoding="utf-8",
    )
    path = tmp_path / "notable.toml"
    path.write_text(body, encoding="utf-8")
    return path


BASE = """
schema_version = 1
feeds_file = "feeds.toml"
data_dir = "data"
digest_dir = "digests"

[secrets]
openrouter_api_key = "TEST_OR_KEY"

[transport]
contact_url = "https://example.com/contact"

[cache]
dir = "cache"

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[tasks.detect_people]
model = "openai/gpt-5.4-mini"
"""


def test_loads_feeds_and_resolves_paths_relative_to_the_config_file(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    config = load_config(_write(tmp_path, BASE))
    assert [feed.key for feed in config.feeds] == ["a"]
    assert config.data_dir == tmp_path / "data"
    assert config.cache.dir == tmp_path / "cache"


def test_secret_comes_from_the_environment_not_the_file(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-live")
    config = load_config(_write(tmp_path, BASE))
    assert config.openrouter_api_key == "sk-live"


def test_missing_secret_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_OR_KEY", raising=False)
    with pytest.raises(ValueError, match="TEST_OR_KEY"):
        load_config(_write(tmp_path, BASE))


def test_unknown_key_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    with pytest.raises(ValueError):
        load_config(_write(tmp_path, BASE + '\nunexpected_key = "x"\n'))


def test_duplicate_feed_keys_are_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    feeds = (
        "schema_version = 1\n"
        '[[feeds]]\nkey = "a"\nlabel = "A"\nurl = "https://a.test/f"\n'
        '[[feeds]]\nkey = "a"\nlabel = "B"\nurl = "https://b.test/f"\n'
    )
    with pytest.raises(ValueError, match="duplicate feed key"):
        load_config(_write(tmp_path, BASE, feeds=feeds))


def test_defaults_match_the_spec(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    config = load_config(_write(tmp_path, BASE))
    assert config.cache.feed_ttl_seconds == 43200
    assert config.cache.discovery_ttl_seconds == 86400
    assert config.max_item_attempts == 3
    assert config.resurface_after_days == 30
    assert config.digest_size == 20
    assert config.detect.max_people == 8
    assert config.detect.max_completion_tokens == 4096
    assert config.budget_usd is None


def test_budget_parses_as_decimal(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    body = BASE + '\n[budget]\nopenrouter_usd_per_run = "2.50"\n'
    assert load_config(_write(tmp_path, body)).budget_usd == Decimal("2.50")


def test_shipped_example_config_is_loadable(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    config = load_config(EXAMPLE)
    assert len(config.feeds) == 10
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.config'`.

- [ ] **Step 4: Write `src/notable/config.py`**

```python
"""Strict, file-first configuration. Environment supplies secrets only."""

from __future__ import annotations

import os
import tomllib
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Feed(_Strict):
    key: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=120)
    url: str = Field(min_length=1)


class TransportConfig(_Strict):
    contact_url: str = Field(min_length=1)
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0
    llm_read_timeout_seconds: float = 300.0
    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_backoff_seconds: float = 1.0
    per_host_min_interval_ms: int = Field(default=900, ge=0)


class CacheConfig(_Strict):
    dir: Path = Path("cache")
    feed_ttl_seconds: int = Field(default=43200, ge=1)
    discovery_ttl_seconds: int = Field(default=86400, ge=1)


class DetectConfig(_Strict):
    model: str = Field(min_length=1)
    # Raise with max_people, never independently: at 1024 against max_people 8
    # responses were cut off mid-string and rejected as malformed. See
    # docs/findings.md.
    max_completion_tokens: int = Field(default=4096, ge=256)
    max_people: int = Field(default=8, ge=1, le=32)
    max_title_characters: int = Field(default=500, ge=1)
    max_summary_characters: int = Field(default=4000, ge=1)
    reasoning_effort: str | None = "low"


class OpenRouterConfig(_Strict):
    endpoint: str = "https://openrouter.ai/api/v1"


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
    openrouter: OpenRouterConfig
    budget_usd: Decimal | None
    openrouter_api_key: str = Field(min_length=1, repr=False)


class _Secrets(_Strict):
    openrouter_api_key: str = "OPENROUTER_API_KEY"


class _Budget(_Strict):
    openrouter_usd_per_run: str | None = None


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
    openrouter: OpenRouterConfig = OpenRouterConfig()
    budget: _Budget = _Budget()
    tasks: dict[str, DetectConfig]


class _FeedsFile(_Strict):
    schema_version: int
    feeds: tuple[Feed, ...]


def _read_toml(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"configuration file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{path} is not valid TOML: {error}") from error


def load_config(path: Path) -> Config:
    """Load and validate configuration. Secrets come from the environment.

    An adjacent `.env` fills missing values, but the process environment wins.
    """
    path = path.resolve()
    root = path.parent
    load_dotenv(root / ".env", override=False)

    try:
        parsed = _File.model_validate(_read_toml(path))
    except Exception as error:  # pydantic ValidationError or our ValueError
        raise ValueError(f"invalid configuration in {path}: {error}") from error

    feeds_path = (root / parsed.feeds_file).resolve()
    try:
        feeds_file = _FeedsFile.model_validate(_read_toml(feeds_path))
    except Exception as error:
        raise ValueError(f"invalid feed list in {feeds_path}: {error}") from error

    seen: set[str] = set()
    for feed in feeds_file.feeds:
        if feed.key in seen:
            raise ValueError(f"duplicate feed key: {feed.key}")
        seen.add(feed.key)

    detect = parsed.tasks.get("detect_people")
    if detect is None:
        raise ValueError(f"{path} is missing [tasks.detect_people]")

    variable = parsed.secrets.openrouter_api_key
    api_key = os.environ.get(variable, "").strip()
    if not api_key:
        raise ValueError(
            f"missing OpenRouter API key: set the {variable} environment "
            f"variable, or add it to {root / '.env'}"
        )

    budget = parsed.budget.openrouter_usd_per_run
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
        openrouter=parsed.openrouter,
        budget_usd=None if budget is None else Decimal(budget),
        openrouter_api_key=api_key,
    )
```

- [ ] **Step 5: Write `config/notable.example.toml`**

```toml
schema_version = 1
feeds_file = "feeds.example.toml"

# Runtime paths, resolved relative to this file.
data_dir = "data"
digest_dir = "digests"

# How many people the digest shows. Leads below the cut are logged, not
# queued: there is no backlog. Set this generously.
digest_size = 20
resurface_after_days = 30
# An item that fails this many times is abandoned rather than retried daily.
max_item_attempts = 3

[secrets]
# Names of environment variables. Never put a key in this file.
openrouter_api_key = "OPENROUTER_API_KEY"

[transport]
# Appended to the user agent so operators can contact you. Required by
# MediaWiki's API etiquette policy.
contact_url = "https://example.com/contact"
connect_timeout_seconds = 10.0
read_timeout_seconds = 30.0
llm_read_timeout_seconds = 300.0
max_attempts = 3
initial_backoff_seconds = 1.0
per_host_min_interval_ms = 900

[cache]
dir = "cache"
# Longer than a full run, so a crash-and-restart does not get a different feed
# snapshot; far shorter than a day, so the daily run sees fresh content.
feed_ttl_seconds = 43200
# Brave and MediaWiki must not be permanent: a person who gains a Wikipedia
# page would otherwise be judged forever on a stale snapshot.
discovery_ttl_seconds = 86400

[budget]
# Optional soft cap as a decimal USD string. One-call overshoot is accepted.
# openrouter_usd_per_run = "2.00"

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[tasks.detect_people]
model = "openai/gpt-5.4-mini"
# Raise this with max_people, never independently. See docs/findings.md.
max_completion_tokens = 4096
max_people = 8
max_title_characters = 500
max_summary_characters = 4000
reasoning_effort = "low"
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_config.py -v`
Expected: 8 passed.

- [ ] **Step 7: Commit**

```bash
git add src/notable/errors.py src/notable/config.py config/notable.example.toml tests/mvp/test_config.py
git commit -m "feat(config): add strict file-first configuration and shared errors"
```

---

### Task 2: The response cache

**Files:**
- Create: `src/notable/cache.py`
- Test: `tests/mvp/test_cache.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `notable.cache.cache_key(*, provider: str, method: str, url: str, body: object, transport_profile: str, extra: dict[str, object] | None = None) -> str`
  - `notable.cache.Cache(root: Path, clock: Callable[[], float] = time.time)` with `get(key: str, *, ttl_seconds: int | None) -> dict | None` and `put(key: str, payload: dict) -> None`.

This carries the entire recovery guarantee. Atomic writes are not optional: without them the exact event the cache exists to survive can leave a truncated entry that poisons every subsequent replay.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_cache.py`:

```python
import json

import pytest

from notable.cache import Cache, cache_key

KEY_ARGS = {
    "provider": "openrouter",
    "method": "POST",
    "url": "https://openrouter.ai/api/v1/chat/completions",
    "body": {"model": "m", "messages": []},
    "transport_profile": "v1",
}


def test_key_is_stable_across_dict_ordering():
    a = cache_key(**KEY_ARGS | {"body": {"model": "m", "messages": []}})
    b = cache_key(**KEY_ARGS | {"body": {"messages": [], "model": "m"}})
    assert a == b


@pytest.mark.parametrize(
    "override",
    [
        {"provider": "brave"},
        {"method": "GET"},
        {"url": "https://openrouter.ai/api/v1/other"},
        {"body": {"model": "n", "messages": []}},
        {"transport_profile": "v2"},
        {"extra": {"schema": {"type": "object"}}},
    ],
)
def test_every_key_component_changes_the_key(override):
    assert cache_key(**KEY_ARGS) != cache_key(**KEY_ARGS | override)


def test_key_never_contains_a_secret():
    key = cache_key(**KEY_ARGS | {"extra": {"model": "openai/gpt-5.4-mini"}})
    assert "sk-" not in key
    assert len(key) == 64 and int(key, 16) >= 0  # plain hex digest


def test_roundtrip(tmp_path):
    cache = Cache(tmp_path)
    cache.put("abc123", {"status": 200, "text": "hi"})
    assert cache.get("abc123", ttl_seconds=None) == {"status": 200, "text": "hi"}


def test_missing_key_is_none(tmp_path):
    assert Cache(tmp_path).get("nope", ttl_seconds=None) is None


def test_entry_past_its_ttl_is_a_miss(tmp_path):
    now = [1000.0]
    cache = Cache(tmp_path, clock=lambda: now[0])
    cache.put("k", {"v": 1})
    now[0] = 1000.0 + 59
    assert cache.get("k", ttl_seconds=60) == {"v": 1}
    now[0] = 1000.0 + 61
    assert cache.get("k", ttl_seconds=60) is None


def test_ttl_none_never_expires(tmp_path):
    now = [0.0]
    cache = Cache(tmp_path, clock=lambda: now[0])
    cache.put("k", {"v": 1})
    now[0] = 10_000_000.0
    assert cache.get("k", ttl_seconds=None) == {"v": 1}


def test_truncated_entry_is_a_miss_and_is_removed(tmp_path):
    cache = Cache(tmp_path)
    cache.put("k", {"v": 1})
    path = cache.path_for("k")
    path.write_text(path.read_text("utf-8")[: len(path.read_text("utf-8")) // 2], "utf-8")
    assert cache.get("k", ttl_seconds=None) is None
    assert not path.exists(), "a corrupt entry must be deleted, not left to rot"


def test_entry_missing_required_envelope_fields_is_a_miss(tmp_path):
    cache = Cache(tmp_path)
    cache.put("k", {"v": 1})
    cache.path_for("k").write_text(json.dumps({"garbage": True}), "utf-8")
    assert cache.get("k", ttl_seconds=None) is None


def test_no_partial_file_is_left_when_writing_fails(tmp_path, monkeypatch):
    cache = Cache(tmp_path)
    monkeypatch.setattr("notable.cache.os.replace", _boom)
    with pytest.raises(OSError):
        cache.put("k", {"v": 1})
    assert list(tmp_path.rglob("*.tmp*")) == []


def _boom(*_args, **_kwargs):
    raise OSError("replace failed")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_cache.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.cache'`.

- [ ] **Step 3: Write `src/notable/cache.py`**

```python
"""The hash-keyed response cache that carries the recovery guarantee."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

CACHE_FORMAT_VERSION = 1


def cache_key(
    *,
    provider: str,
    method: str,
    url: str,
    body: object,
    transport_profile: str,
    extra: dict[str, object] | None = None,
) -> str:
    """A SHA-256 over everything that can change the response.

    `extra` carries per-provider discriminators -- for model calls, the model
    id and the structured-output schema. Authorization headers and API keys are
    never included: a rotated key must not invalidate the cache, and no secret
    may reach disk.
    """
    material = {
        "version": CACHE_FORMAT_VERSION,
        "provider": provider,
        "method": method.upper(),
        "url": url,
        "body": body,
        "transport_profile": transport_profile,
        "extra": extra or {},
    }
    canonical = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class Cache:
    """Content-addressed response storage on disk."""

    def __init__(self, root: Path, clock: Callable[[], float] = time.time) -> None:
        self._root = root
        self._clock = clock

    def path_for(self, key: str) -> Path:
        # Shard by the first two hex characters: a year of daily runs would
        # otherwise put tens of thousands of files in one directory.
        return self._root / key[:2] / f"{key}.json"

    def get(self, key: str, *, ttl_seconds: int | None) -> dict[str, Any] | None:
        path = self.path_for(key)
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError:
            return None

        try:
            envelope = json.loads(raw)
            stored_at = float(envelope["stored_at"])
            payload = envelope["payload"]
            if not isinstance(payload, dict):
                raise TypeError("payload is not an object")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # A cache is an optimization; it may never be a source of failure.
            # Delete rather than leave a poisoned entry to be re-read forever.
            path.unlink(missing_ok=True)
            return None

        if ttl_seconds is not None and self._clock() - stored_at > ttl_seconds:
            return None
        return payload

    def put(self, key: str, payload: dict[str, Any]) -> None:
        path = self.path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        envelope = {"stored_at": self._clock(), "payload": payload}
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f"{key}.",
            suffix=".tmp",
            delete=False,
        )
        temporary = Path(handle.name)
        try:
            with handle:
                json.dump(envelope, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_cache.py -v`
Expected: 15 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/cache.py tests/mvp/test_cache.py
git commit -m "feat(cache): add atomic hash-keyed response cache with TTL classes"
```

---

### Task 3: The store

**Files:**
- Create: `src/notable/store.py`
- Test: `tests/mvp/test_store.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `notable.store.RunSummary(settled: list[str], incomplete: list[str], capped: bool, cost_usd: Decimal)` — frozen dataclass with a `status` property returning `"partial"` if `incomplete or capped` else `"ok"`.
  - `notable.store.Store(path: Path)` with `is_eligible(url: str, *, max_attempts: int) -> bool`, `commit(settled: list[str], incomplete: list[str], surfaced_keys: list[str]) -> None`, `log(summary: RunSummary, leads: list[dict], digest_path: str) -> None`, `close() -> None`.

**Phase 1 note:** `commit` is called with `surfaced_keys=[]` and `log` with `leads=[]`. Both parameters exist now so Phase 4 wires them without changing the signature.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_store.py`:

```python
from decimal import Decimal

import pytest

from notable.store import RunSummary, Store


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path / "notable.db")
    yield instance
    instance.close()


def test_status_is_ok_only_when_nothing_failed_and_the_cap_did_not_bind():
    assert RunSummary([], [], False, Decimal("0")).status == "ok"


@pytest.mark.parametrize(
    "summary",
    [
        RunSummary([], ["u"], False, Decimal("0")),
        RunSummary([], [], True, Decimal("0")),
        RunSummary(["a"], ["u"], True, Decimal("0")),
    ],
)
def test_any_incomplete_item_or_the_cap_makes_a_run_partial(summary):
    # A mutable status set only on the budget path reported a run with failed
    # items as ok. Both conditions must reach the log.
    assert summary.status == "partial"


def test_a_new_url_is_eligible(store):
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True


def test_a_settled_url_is_not_eligible(store):
    store.commit(["https://a.test/1"], [], [])
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_an_incomplete_url_stays_eligible_until_the_cap(store):
    url = "https://a.test/1"
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is True
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is True
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is False, "abandoned at the cap"


def test_attempts_accumulate_across_runs(store):
    url = "https://a.test/1"
    for _ in range(2):
        store.commit([], [url], [])
    assert store.attempts(url) == 2


def test_settling_a_previously_incomplete_url_ends_retries(store):
    url = "https://a.test/1"
    store.commit([], [url], [])
    store.commit([url], [], [])
    assert store.is_eligible(url, max_attempts=3) is False


def test_log_writes_a_run_row(store):
    store.log(RunSummary(["a", "b"], ["c"], False, Decimal("1.25")), [], "d.md")
    row = store.connection.execute(
        "SELECT n_items_settled, n_items_incomplete, cost_usd, status FROM run"
    ).fetchone()
    assert row == (2, 1, "1.25", "partial")


def test_log_failure_does_not_prevent_state_from_committing(store, monkeypatch):
    # One bad log row must not livelock the product: the digest is already
    # written, so rolling back item markers would repeat the run forever.
    store.commit(["https://a.test/1"], [], [])
    monkeypatch.setattr(store, "_insert_run", _boom)
    store.log(RunSummary([], [], False, Decimal("0")), [], "d.md")  # must not raise
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_schema_creation_is_idempotent(tmp_path):
    path = tmp_path / "notable.db"
    Store(path).close()
    second = Store(path)
    assert {r[0] for r in second.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )} == {"item", "surfaced", "run", "lead"}
    second.close()


def _boom(*_args, **_kwargs):
    raise RuntimeError("log write failed")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_store.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.store'`.

- [ ] **Step 3: Write `src/notable/store.py`**

```python
"""SQLite persistence: two state tables the pipeline reads, two logs it never does."""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS item (
    url           TEXT PRIMARY KEY,
    first_seen_at TEXT NOT NULL,
    settled_at    TEXT,
    attempts      INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS surfaced (
    identity_key     TEXT PRIMARY KEY,
    last_surfaced_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS run (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at         TEXT NOT NULL,
    ended_at           TEXT NOT NULL,
    n_items_settled    INTEGER NOT NULL,
    n_items_incomplete INTEGER NOT NULL,
    cost_usd           TEXT NOT NULL,
    status             TEXT NOT NULL,
    digest_path        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS lead (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id        INTEGER NOT NULL,
    identity_key  TEXT NOT NULL,
    display_name  TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    rank_key_json TEXT NOT NULL,
    detail_json   TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class RunSummary:
    """Everything the run log needs, derived in one place.

    Two conditions make a run partial -- items that raised `Incomplete`, and a
    run cut short by the spend cap. A mutable status string set only on the
    budget path reports a run with failed items as `ok`.
    """

    settled: list[str]
    incomplete: list[str]
    capped: bool
    cost_usd: Decimal
    started_at: str = field(default_factory=_now)

    @property
    def status(self) -> str:
        return "partial" if (self.incomplete or self.capped) else "ok"


class Store:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.executescript(SCHEMA)
        self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    # -- state the pipeline reads -------------------------------------------

    def is_eligible(self, url: str, *, max_attempts: int) -> bool:
        """Eligible means new, or previously incomplete and still retryable."""
        row = self.connection.execute(
            "SELECT settled_at, attempts FROM item WHERE url = ?", (url,)
        ).fetchone()
        if row is None:
            return True
        settled_at, attempts = row
        return settled_at is None and attempts < max_attempts

    def attempts(self, url: str) -> int:
        row = self.connection.execute(
            "SELECT attempts FROM item WHERE url = ?", (url,)
        ).fetchone()
        return 0 if row is None else int(row[0])

    def commit(
        self, settled: list[str], incomplete: list[str], surfaced_keys: list[str]
    ) -> None:
        """Commit the state tables in one transaction, after the digest lands."""
        now = _now()
        with self.connection:
            for url in settled:
                self.connection.execute(
                    "INSERT INTO item (url, first_seen_at, settled_at, attempts) "
                    "VALUES (?, ?, ?, 0) "
                    "ON CONFLICT(url) DO UPDATE SET settled_at = excluded.settled_at",
                    (url, now, now),
                )
            for url in incomplete:
                self.connection.execute(
                    "INSERT INTO item (url, first_seen_at, settled_at, attempts) "
                    "VALUES (?, ?, NULL, 1) "
                    "ON CONFLICT(url) DO UPDATE SET attempts = item.attempts + 1",
                    (url, now),
                )
            for key in surfaced_keys:
                self.connection.execute(
                    "INSERT INTO surfaced (identity_key, last_surfaced_at) "
                    "VALUES (?, ?) "
                    "ON CONFLICT(identity_key) DO UPDATE SET "
                    "last_surfaced_at = excluded.last_surfaced_at",
                    (key, now),
                )

    # -- logs the pipeline never reads --------------------------------------

    def log(
        self, summary: RunSummary, leads: list[dict[str, Any]], digest_path: str
    ) -> None:
        """Append-only diagnostics, best effort and outside the state transaction.

        Sharing `commit`'s transaction would let a serialization error roll back
        the item markers *after* the digest file was written, so the next run
        repeats all the work and hits the identical deterministic failure. One
        bad log row would livelock the product.
        """
        try:
            with self.connection:
                run_id = self._insert_run(summary, digest_path)
                for lead in leads:
                    self.connection.execute(
                        "INSERT INTO lead (run_id, identity_key, display_name, "
                        "outcome, rank_key_json, detail_json) VALUES (?,?,?,?,?,?)",
                        (
                            run_id,
                            lead["identity_key"],
                            lead["display_name"],
                            lead["outcome"],
                            json.dumps(lead["rank_key"], sort_keys=True),
                            json.dumps(lead["detail"], sort_keys=True),
                        ),
                    )
        except Exception:
            logger.warning("run log write failed; state is already committed", exc_info=True)

    def _insert_run(self, summary: RunSummary, digest_path: str) -> int:
        cursor = self.connection.execute(
            "INSERT INTO run (started_at, ended_at, n_items_settled, "
            "n_items_incomplete, cost_usd, status, digest_path) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                summary.started_at,
                _now(),
                len(summary.settled),
                len(summary.incomplete),
                str(summary.cost_usd),
                summary.status,
                digest_path,
            ),
        )
        return int(cursor.lastrowid or 0)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_store.py -v`
Expected: 13 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/store.py tests/mvp/test_store.py
git commit -m "feat(store): add four-table SQLite store with best-effort logging"
```

---

### Task 4: The transport

**Files:**
- Create: `src/notable/http.py`
- Test: `tests/mvp/test_http.py`

**Interfaces:**
- Consumes: `notable.config.TransportConfig`, `notable.cache.Cache`, `notable.cache.cache_key`, `notable.errors.ProviderFailure`.
- Produces:
  - `notable.http.TRANSPORT_PROFILE: str`
  - `notable.http.Response(status: int, text: str, from_cache: bool)` frozen dataclass with a `json()` method.
  - `notable.http.Transport(config, cache, *, client, sleep=time.sleep)` with `request(*, provider, method, url, params=None, json_body=None, ttl_seconds, timeout=None, extra_key=None, auth_token=None, auth_header="Authorization") -> Response`.

**`from_cache` is load-bearing, not diagnostic.** A cache hit costs nothing, so `llm.py` must not add its recorded cost to the run's spend. Without this flag, replaying a crashed run reports money it never spent and can falsely trip the budget cap on a free replay — which would break the crash-recovery guarantee this whole design rests on.

`client` is an injected `httpx.Client`, which is what makes these tests fast and offline.

**On `TRANSPORT_PROFILE`:** it covers the fixed *response-affecting* headers (`Accept`, `Accept-Language`). `User-Agent` is deliberately excluded even though it is fixed: it carries the operator's `contact_url`, and including it would invalidate the entire cache whenever that changes, for no change in any response. Authorization is excluded because no secret may reach disk.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_http.py`:

```python
import httpx
import pytest

from notable.cache import Cache
from notable.config import TransportConfig
from notable.errors import ProviderFailure
from notable.http import TRANSPORT_PROFILE, Transport

CONFIG = TransportConfig(
    contact_url="https://example.com/c", per_host_min_interval_ms=0, initial_backoff_seconds=0
)


def _transport(tmp_path, handler):
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return Transport(CONFIG, Cache(tmp_path), client=client, sleep=lambda _s: None)


def test_successful_response_is_returned_and_cached(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="hello")

    transport = _transport(tmp_path, handler)
    first = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    second = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert first.text == second.text == "hello"
    assert len(calls) == 1, "the second call must be served from cache"


def test_sends_fixed_headers_and_no_cookies(tmp_path):
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return httpx.Response(200, text="ok")

    _transport(tmp_path, handler).request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert "example.com/c" in seen["user-agent"]
    assert seen["accept-language"].startswith("en")
    assert "cookie" not in seen


def test_server_errors_are_retried_then_fail(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(503, text="down")

    with pytest.raises(ProviderFailure) as info:
        _transport(tmp_path, handler).request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    assert info.value.permanent is False
    assert len(calls) == CONFIG.max_attempts


def test_a_retry_that_succeeds_returns_the_success(tmp_path):
    responses = [httpx.Response(503, text="down"), httpx.Response(200, text="good")]

    def handler(request):
        return responses.pop(0)

    result = _transport(tmp_path, handler).request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert result.text == "good"


def test_client_errors_are_permanent_and_not_retried(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(404, text="nope")

    with pytest.raises(ProviderFailure) as info:
        _transport(tmp_path, handler).request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    assert info.value.permanent is True
    assert len(calls) == 1


def test_failures_are_never_cached(tmp_path):
    # A transport failure followed by a success must return the success.
    responses = [httpx.Response(500, text="x")] * CONFIG.max_attempts + [
        httpx.Response(200, text="recovered")
    ]

    def handler(request):
        return responses.pop(0)

    transport = _transport(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        transport.request(
            provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
        )
    result = transport.request(
        provider="t", method="GET", url="https://a.test/x", ttl_seconds=None
    )
    assert result.text == "recovered"


def test_transport_profile_excludes_the_contact_url(tmp_path):
    # Changing contact_url must not invalidate the whole cache.
    assert "example.com" not in TRANSPORT_PROFILE


def test_a_cache_hit_is_flagged_so_callers_can_skip_charging_for_it(tmp_path):
    transport = _transport(tmp_path, lambda r: httpx.Response(200, text="ok"))
    kwargs = {"provider": "t", "method": "GET", "url": "https://a.test/x", "ttl_seconds": None}
    assert transport.request(**kwargs).from_cache is False
    assert transport.request(**kwargs).from_cache is True


def test_pacing_sleeps_between_calls_to_one_host(tmp_path):
    slept = []
    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, text="ok"))
    )
    config = TransportConfig(contact_url="https://e.test/c", per_host_min_interval_ms=900)
    transport = Transport(config, Cache(tmp_path), client=client, sleep=slept.append)
    for path in ("a", "b"):
        transport.request(
            provider="t", method="GET", url=f"https://a.test/{path}", ttl_seconds=None
        )
    assert slept and slept[-1] > 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_http.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.http'`.

- [ ] **Step 3: Write `src/notable/http.py`**

```python
"""One httpx client: fixed headers, bounded retry, per-host pacing, caching."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from notable.cache import Cache, cache_key
from notable.config import TransportConfig
from notable.errors import ProviderFailure

# Fixed response-affecting headers, folded into every cache key. `User-Agent`
# is excluded on purpose: it carries the operator's contact URL, and including
# it would invalidate the whole cache when that changes without changing any
# response. Authorization is excluded because no secret may reach disk.
_FIXED_HEADERS = {"Accept": "*/*", "Accept-Language": "en"}
TRANSPORT_PROFILE = json.dumps(_FIXED_HEADERS, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    text: str
    # A cache hit cost nothing. `llm.py` relies on this to keep replayed calls
    # out of the run's spend: counting them would report money that was never
    # spent and could trip the budget cap during a free crash replay.
    from_cache: bool = False

    def json(self) -> Any:
        return json.loads(self.text)


class Transport:
    def __init__(
        self,
        config: TransportConfig,
        cache: Cache,
        *,
        client: httpx.Client,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._config = config
        self._cache = cache
        self._client = client
        self._sleep = sleep
        self._last_call: dict[str, float] = {}
        self._user_agent = f"notable/0.1 (+{config.contact_url})"

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
        extra_key: dict[str, object] | None = None,
        auth_token: str | None = None,
        auth_header: str = "Authorization",
    ) -> Response:
        key = cache_key(
            provider=provider,
            method=method,
            url=url,
            body={"params": params or {}, "json": json_body},
            transport_profile=TRANSPORT_PROFILE,
            extra=extra_key,
        )
        cached = self._cache.get(key, ttl_seconds=ttl_seconds)
        if cached is not None:
            return Response(
                status=int(cached["status"]), text=str(cached["text"]), from_cache=True
            )

        headers = dict(_FIXED_HEADERS)
        headers["User-Agent"] = self._user_agent
        if auth_token:
            headers[auth_header] = auth_token

        response = self._send(
            method=method,
            url=url,
            params=params,
            json_body=json_body,
            headers=headers,
            timeout=timeout or self._config.read_timeout_seconds,
        )
        # Only durable successes are cached. A cached failure would make a
        # transient problem permanent and poison the fixture set.
        self._cache.put(key, {"status": response.status, "text": response.text})
        return response

    def _send(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json_body: dict[str, Any] | None,
        headers: dict[str, str],
        timeout: float,
    ) -> Response:
        backoff = self._config.initial_backoff_seconds
        last: Exception | None = None
        for attempt in range(1, self._config.max_attempts + 1):
            self._pace(url)
            try:
                raw = self._client.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                    timeout=httpx.Timeout(
                        timeout, connect=self._config.connect_timeout_seconds
                    ),
                    follow_redirects=True,
                )
            except httpx.HTTPError as error:
                last = ProviderFailure(f"{type(error).__name__}: {error}", permanent=False)
            else:
                if raw.status_code < 400:
                    return Response(status=raw.status_code, text=raw.text)
                if raw.status_code < 500 and raw.status_code != 429:
                    raise ProviderFailure(
                        f"HTTP {raw.status_code} from {url}", permanent=True
                    )
                last = ProviderFailure(f"HTTP {raw.status_code} from {url}", permanent=False)

            if attempt < self._config.max_attempts:
                self._sleep(backoff)
                backoff *= 2
        raise last or ProviderFailure(f"no response from {url}", permanent=False)

    def _pace(self, url: str) -> None:
        interval = self._config.per_host_min_interval_ms / 1000
        if interval <= 0:
            return
        host = urlsplit(url).netloc
        previous = self._last_call.get(host)
        now = time.monotonic()
        if previous is not None:
            wait = interval - (now - previous)
            if wait > 0:
                self._sleep(wait)
        self._last_call[host] = time.monotonic()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_http.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/http.py tests/mvp/test_http.py
git commit -m "feat(http): add caching transport with fixed headers and bounded retry"
```

---

### Task 5: The OpenRouter client

**Files:**
- Create: `src/notable/llm.py`
- Test: `tests/mvp/test_llm.py`

**Interfaces:**
- Consumes: `notable.http.Transport`, `notable.config.OpenRouterConfig`, `notable.errors.BudgetExceeded`, `notable.errors.ProviderFailure`.
- Produces: `notable.llm.LlmClient(transport, config, *, api_key, budget_usd)` with `structured(*, task, model, system, user_payload, schema, max_completion_tokens, reasoning_effort, timeout) -> dict` and `spend() -> Decimal`.

**Model responses are cached permanently** (the "stable" TTL class): a model's answer to a fixed prompt is treated as fixed.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_llm.py`:

```python
import json
from decimal import Decimal

import httpx
import pytest

from notable.cache import Cache
from notable.config import OpenRouterConfig, TransportConfig
from notable.errors import BudgetExceeded, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient

SCHEMA = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
TRANSPORT_CONFIG = TransportConfig(
    contact_url="https://e.test/c", per_host_min_interval_ms=0, initial_backoff_seconds=0
)


def _reply(content: dict, cost: str = "0.01") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [{"message": {"content": json.dumps(content)}}],
            "usage": {"cost": cost},
        },
    )


def _client(tmp_path, handler, budget=None):
    http = httpx.Client(transport=httpx.MockTransport(handler))
    transport = Transport(
        TRANSPORT_CONFIG, Cache(tmp_path), client=http, sleep=lambda _s: None
    )
    return LlmClient(
        transport, OpenRouterConfig(), api_key="sk-secret", budget_usd=budget
    )


def _call(client, n: int = 1):
    """`n` varies the payload so successive calls are distinct cache keys."""
    return client.structured(
        task="detect_people",
        model="m",
        system="s",
        user_payload={"a": n},
        schema=SCHEMA,
        max_completion_tokens=256,
        reasoning_effort="low",
        timeout=5.0,
    )


def test_returns_parsed_content(tmp_path):
    assert _call(_client(tmp_path, lambda r: _reply({"ok": True}))) == {"ok": True}


def test_accumulates_actual_cost(tmp_path):
    client = _client(tmp_path, lambda r: _reply({"ok": True}, cost="0.0125"))
    _call(client)
    assert client.spend() == Decimal("0.0125")


def test_request_carries_strict_schema_and_no_temperature(tmp_path):
    captured = {}

    def handler(request):
        captured.update(json.loads(request.content))
        return _reply({"ok": True})

    _call(_client(tmp_path, handler))
    fmt = captured["response_format"]
    assert fmt["type"] == "json_schema"
    assert fmt["json_schema"]["strict"] is True
    assert fmt["json_schema"]["schema"] == SCHEMA
    # The default models' endpoints declare support for neither, and routing
    # requires an endpoint supporting every supplied parameter (HTTP 404).
    assert "temperature" not in captured
    assert "top_p" not in captured


def test_api_key_is_sent_but_never_reaches_the_cache(tmp_path):
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return _reply({"ok": True})

    _call(_client(tmp_path, handler))
    assert seen["authorization"] == "Bearer sk-secret"
    for path in tmp_path.rglob("*.json"):
        assert "sk-secret" not in path.read_text("utf-8")


def test_budget_is_checked_before_the_next_call(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return _reply({"ok": True}, cost="0.60")

    client = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(client, 1)  # 0.60, under the cap
    _call(client, 2)  # 1.20, over -- but this call was already permitted
    with pytest.raises(BudgetExceeded):
        _call(client, 3)
    assert len(calls) == 2, "no call is made once the cap is known to be passed"
    assert client.spend() == Decimal("1.20"), "soft cap: one-call overshoot is accepted"


def test_a_cache_hit_does_not_add_to_spend(tmp_path):
    # Replaying a crashed run must be free. Charging for cache hits would
    # report money never spent and could trip the cap during a free replay.
    client = _client(tmp_path, lambda r: _reply({"ok": True}, cost="0.50"))
    _call(client, 1)
    _call(client, 1)  # identical payload -> cache hit
    assert client.spend() == Decimal("0.50")


def test_a_fully_replayed_run_never_exceeds_the_budget(tmp_path):
    handler = lambda r: _reply({"ok": True}, cost="0.90")
    first = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(first, 1)
    _call(first, 2)
    # Same cache directory, fresh client: every call replays for free.
    second = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(second, 1)
    _call(second, 2)
    assert second.spend() == Decimal("0")


def test_malformed_json_content_is_a_provider_failure(tmp_path):
    handler = lambda r: httpx.Response(
        200, json={"choices": [{"message": {"content": "{not json"}}], "usage": {}}
    )
    with pytest.raises(ProviderFailure):
        _call(_client(tmp_path, handler))


def test_repeated_identical_call_is_served_from_cache(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return _reply({"ok": True})

    client = _client(tmp_path, handler)
    _call(client)
    _call(client)
    assert len(calls) == 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_llm.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.llm'`.

- [ ] **Step 3: Write `src/notable/llm.py`**

```python
"""OpenRouter structured-output calls and the run's spend counter."""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from typing import Any

from notable.config import OpenRouterConfig
from notable.errors import BudgetExceeded, ProviderFailure
from notable.http import Transport


class LlmClient:
    def __init__(
        self,
        transport: Transport,
        config: OpenRouterConfig,
        *,
        api_key: str,
        budget_usd: Decimal | None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._api_key = api_key
        self._budget = budget_usd
        self._spend = Decimal("0")

    def spend(self) -> Decimal:
        return self._spend

    def structured(
        self,
        *,
        task: str,
        model: str,
        system: str,
        user_payload: dict[str, Any],
        schema: dict[str, Any],
        max_completion_tokens: int,
        reasoning_effort: str | None,
        timeout: float,
    ) -> dict[str, Any]:
        """One focused decision. Raises BudgetExceeded before spending past the cap."""
        if self._budget is not None and self._spend >= self._budget:
            raise BudgetExceeded(
                f"run spend {self._spend} reached the cap {self._budget}"
            )

        body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        user_payload, sort_keys=True, separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": task, "strict": True, "schema": schema},
            },
            "max_tokens": max_completion_tokens,
            "usage": {"include": True},
            "provider": {"allow_fallbacks": True, "data_collection": "deny"},
        }
        if reasoning_effort:
            body["reasoning"] = {"effort": reasoning_effort}

        response = self._transport.request(
            provider="openrouter",
            method="POST",
            url=f"{self._config.endpoint}/chat/completions",
            json_body=body,
            # A model's answer to a fixed prompt is treated as fixed.
            ttl_seconds=None,
            timeout=timeout,
            # The model and schema must discriminate the key: the same prompt
            # under a different schema is a different call.
            extra_key={"model": model, "schema": schema, "task": task},
            auth_token=f"Bearer {self._api_key}",
        )

        try:
            envelope = response.json()
            content = envelope["choices"][0]["message"]["content"]
        except (json.JSONDecodeError, KeyError, IndexError, TypeError) as error:
            raise ProviderFailure(
                f"unreadable OpenRouter envelope for {task}: {error}", permanent=False
            ) from error

        # A replayed call cost nothing. Charging for it would report money that
        # was never spent, and would let a free crash replay trip the cap.
        if not response.from_cache:
            self._record_cost(envelope.get("usage") or {})

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as error:
            raise ProviderFailure(
                f"{task} returned content that is not JSON: {error}", permanent=True
            ) from error
        if not isinstance(parsed, dict):
            raise ProviderFailure(f"{task} returned a non-object", permanent=True)
        return parsed

    def _record_cost(self, usage: dict[str, Any]) -> None:
        raw = usage.get("cost")
        if raw is None:
            return
        try:
            self._spend += Decimal(str(raw))
        except InvalidOperation:
            return
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_llm.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/llm.py tests/mvp/test_llm.py
git commit -m "feat(llm): add OpenRouter structured output with a soft spend cap"
```

---

### Task 6: Feed ingestion

**Files:**
- Create: `src/notable/feeds.py`
- Create: `tests/mvp/conftest.py`
- Test: `tests/mvp/test_feeds.py`

**Why a `conftest.py`:** Tasks 8 and 10 need the same `Config` factory. Importing it from `tests.mvp.test_feeds` would require a `tests/__init__.py`, which would pull the legacy prototype's tests into the same package. A fixture is the mechanism pytest provides for exactly this.

**Interfaces:**
- Consumes: `notable.config.Config`, `notable.http.Transport`, `notable.store.Store`.
- Produces:
  - `notable.feeds.SourceItem` frozen dataclass: `url: str`, `title: str | None`, `summary: str | None`, `published_at: str | None`, `feed_key: str`, `publisher_label: str`.
  - `notable.feeds.canonical_url(raw: str) -> str | None` — returns `None` for anything not `http`/`https`.
  - `notable.feeds.fetch_new(config, transport, store, *, fresh=False) -> Iterator[SourceItem]`.

- [ ] **Step 1: Write the shared fixture**

`tests/mvp/conftest.py`:

```python
"""Shared fixtures. Tasks 6, 8, and 10 all need a Config and a Transport."""

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from notable.cache import Cache
from notable.config import (
    CacheConfig,
    Config,
    DetectConfig,
    Feed,
    OpenRouterConfig,
    TransportConfig,
)
from notable.http import Transport
from notable.store import Store

TRANSPORT = TransportConfig(
    contact_url="https://e.test/c",
    per_host_min_interval_ms=0,
    initial_backoff_seconds=0,
)


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
            openrouter=OpenRouterConfig(),
            budget_usd=None,
            openrouter_api_key="sk-test",
        )
        return base.model_copy(update=overrides) if overrides else base

    return build


@pytest.fixture
def make_transport(tmp_path) -> Callable[[Callable], Transport]:
    def build(handler: Callable) -> Transport:
        client = httpx.Client(transport=httpx.MockTransport(handler))
        return Transport(
            TRANSPORT, Cache(tmp_path / "cache"), client=client, sleep=lambda _s: None
        )

    return build


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path / "db.sqlite")
    yield instance
    instance.close()
```

- [ ] **Step 2: Write the failing test**

`tests/mvp/test_feeds.py`:

```python
import httpx
import pytest

from notable.config import Feed
from notable.feeds import canonical_url, fetch_new

FEED_XML = """<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Painter wins prize</title><description>A summary.</description>
<link>https://a.test/one?utm_source=rss&amp;utm_medium=feed</link>
<pubDate>Tue, 01 Jul 2025 10:00:00 GMT</pubDate></item>
<item><title>Second piece</title><description>More.</description>
<link>https://a.test/two#section</link></item>
</channel></rss>"""


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://a.test/one?utm_source=rss", "https://a.test/one"),
        ("https://a.test/one#frag", "https://a.test/one"),
        ("https://a.test/one?b=2&a=1", "https://a.test/one?a=1&b=2"),
        ("https://A.TEST/one", "https://a.test/one"),
        ("javascript:alert(1)", None),
        ("ftp://a.test/x", None),
        ("", None),
    ],
)
def test_canonical_url(raw, expected):
    assert canonical_url(raw) == expected


def _ok(_request):
    return httpx.Response(200, text=FEED_XML)


def test_parses_items_and_canonicalizes_links(make_config, make_transport, store):
    items = list(fetch_new(make_config(), make_transport(_ok), store))
    assert [item.url for item in items] == ["https://a.test/one", "https://a.test/two"]
    assert items[0].title == "Painter wins prize"
    assert items[0].summary == "A summary."
    assert items[0].publisher_label == "Feed A"


def test_settled_items_are_skipped(make_config, make_transport, store):
    store.commit(["https://a.test/one"], [], [])
    urls = [i.url for i in fetch_new(make_config(), make_transport(_ok), store)]
    assert urls == ["https://a.test/two"]


def test_an_abandoned_item_is_skipped(make_config, make_transport, store):
    for _ in range(3):
        store.commit([], ["https://a.test/one"], [])
    urls = [i.url for i in fetch_new(make_config(), make_transport(_ok), store)]
    assert urls == ["https://a.test/two"], "an item at the attempt cap is abandoned"


def test_a_failing_feed_does_not_stop_the_others(make_config, make_transport, store):
    config = make_config(
        feeds=(
            Feed(key="bad", label="Bad", url="https://bad.test/rss"),
            Feed(key="a", label="Feed A", url="https://a.test/rss"),
        )
    )

    def handler(request):
        if "bad.test" in str(request.url):
            return httpx.Response(500, text="down")
        return httpx.Response(200, text=FEED_XML)

    urls = [i.url for i in fetch_new(config, make_transport(handler), store)]
    assert urls == ["https://a.test/one", "https://a.test/two"]


def test_duplicate_urls_across_feeds_yield_once(make_config, make_transport, store):
    config = make_config(
        feeds=(
            Feed(key="a", label="A", url="https://a.test/rss"),
            Feed(key="b", label="B", url="https://b.test/rss"),
        )
    )
    urls = [i.url for i in fetch_new(config, make_transport(_ok), store)]
    assert urls == ["https://a.test/one", "https://a.test/two"]


def test_fresh_bypasses_the_feed_cache(make_config, make_transport, store):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, text=FEED_XML)

    transport = make_transport(handler)
    config = make_config()
    list(fetch_new(config, transport, store))
    list(fetch_new(config, transport, store))          # cached
    list(fetch_new(config, transport, store, fresh=True))
    assert len(calls) == 2, "only --fresh-feeds refetches within the TTL"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_feeds.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.feeds'`.

- [ ] **Step 4: Write `src/notable/feeds.py`**

```python
"""Feed fetching and normalization."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser

from notable.config import Config
from notable.errors import ProviderFailure
from notable.http import Transport
from notable.store import Store

logger = logging.getLogger(__name__)

_TRACKING_PREFIXES = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid")


@dataclass(frozen=True, slots=True)
class SourceItem:
    url: str
    title: str | None
    summary: str | None
    published_at: str | None
    feed_key: str
    publisher_label: str


def canonical_url(raw: str) -> str | None:
    """Normalize a link, or return None if it is not a usable http(s) URL."""
    if not raw:
        return None
    parts = urlsplit(raw.strip())
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return None
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith(_TRACKING_PREFIXES)
    ]
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path,
            urlencode(sorted(query)),
            "",
        )
    )


def _text(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned or None


def fetch_new(
    config: Config, transport: Transport, store: Store, *, fresh: bool = False
) -> Iterator[SourceItem]:
    """Yield eligible items from every configured feed.

    One feed failing must not stop the others: a single publisher's outage is
    not a reason to produce no digest.
    """
    emitted: set[str] = set()
    for feed in config.feeds:
        try:
            response = transport.request(
                provider="feed",
                method="GET",
                url=feed.url,
                ttl_seconds=None if fresh else config.cache.feed_ttl_seconds,
            )
        except ProviderFailure:
            logger.warning("feed fetch failed: %s", feed.key, exc_info=True)
            continue

        parsed = feedparser.parse(response.text)
        for entry in parsed.entries:
            url = canonical_url(getattr(entry, "link", "") or "")
            if url is None or url in emitted:
                continue
            if not store.is_eligible(url, max_attempts=config.max_item_attempts):
                continue
            emitted.add(url)
            yield SourceItem(
                url=url,
                title=_text(getattr(entry, "title", None)),
                summary=_text(getattr(entry, "summary", None)),
                published_at=_text(getattr(entry, "published", None)),
                feed_key=feed.key,
                publisher_label=feed.label,
            )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_feeds.py -v`
Expected: 13 passed.

- [ ] **Step 6: Commit**

```bash
git add src/notable/feeds.py tests/mvp/conftest.py tests/mvp/test_feeds.py
git commit -m "feat(feeds): add feed fetching, URL canonicalization, and eligibility"
```

---

### Task 7: The detection contract

**Files:**
- Create: `src/notable/detect_contract.py`
- Test: `tests/mvp/test_detect_contract.py`

**Interfaces:**
- Consumes: `notable.feeds.SourceItem`, `notable.config.DetectConfig`.
- Produces:
  - `notable.detect_contract.Passage`, `IdentityFact`, `GroundedSignal`, `DetectedMention`, `DetectionOutput` (pydantic models). `DetectedMention.research_worthy: bool` property — true when `outcome` is `research` or `uncertain`.
  - `build_passages(item, config) -> tuple[Passage, ...]`
  - `detection_schema(*, max_people: int) -> dict[str, object]`
  - `validate_detection(raw: dict, *, passages, max_people) -> DetectionOutput` raising `DetectionInvalid(ValueError)`.

**The wire schema is written by hand, not derived from pydantic.** Pydantic emits `$ref`/`$defs`, `pattern`, and length constraints that strict structured output handles inconsistently. Writing it by hand keeps the wire contract to the subset that is known to work, and pydantic then validates what came back. `docs/findings.md` records that a root-level `anyOf` is rejected outright — the root here is a plain object.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_detect_contract.py`:

```python
import pytest

from notable.config import DetectConfig
from notable.detect_contract import (
    DetectionInvalid,
    build_passages,
    detection_schema,
    validate_detection,
)
from notable.feeds import SourceItem

CONFIG = DetectConfig(model="m", max_title_characters=20, max_summary_characters=30)


def _item(title="Sculptor Ana Poy wins prize", summary="Ana Poy showed in Paris."):
    return SourceItem(
        url="https://a.test/1",
        title=title,
        summary=summary,
        published_at=None,
        feed_key="a",
        publisher_label="A",
    )


def _mention(**overrides):
    base = {
        "exact_name": "Ana Poy",
        "outcome": "research",
        "supporting_passage_ids": ["p1"],
        "identity_facts": [
            {"kind": "profession_or_role", "value": "Sculptor", "supporting_passage_ids": ["p1"]}
        ],
        "signals": [],
        "rationale": "Named subject.",
    }
    return base | overrides


def _output(**overrides):
    base = {
        "item_outcome": "research_people",
        "mentions": [_mention()],
        "overflow": False,
        "rationale": "One subject.",
    }
    return base | overrides


PASSAGES = build_passages(_item(title="Sculptor Ana Poy", summary="Ana Poy in Paris."), CONFIG)


# -- passages ------------------------------------------------------------

def test_passages_are_bounded_and_flagged_when_truncated():
    passages = build_passages(_item(title="x" * 100, summary="y" * 100), CONFIG)
    title = next(p for p in passages if p.field == "title")
    assert len(title.text) == 20
    assert title.truncated is True


def test_absent_fields_produce_no_passage():
    assert [p.id for p in build_passages(_item(summary=None), CONFIG)] == ["p1"]


# -- schema --------------------------------------------------------------

def test_schema_root_is_an_object_not_a_union():
    # Strict structured output rejects a root-level anyOf with HTTP 400.
    schema = detection_schema(max_people=8)
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_caps_mentions_at_max_people():
    # The cap must be unrepresentable, not merely rejected after payment.
    assert detection_schema(max_people=3)["properties"]["mentions"]["maxItems"] == 3


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

    walk(detection_schema(max_people=8))


# -- validation ----------------------------------------------------------

def test_valid_output_parses():
    result = validate_detection(_output(), passages=PASSAGES, max_people=8)
    assert result.mentions[0].exact_name == "Ana Poy"


def test_research_and_uncertain_mentions_are_research_worthy():
    for outcome in ("research", "uncertain"):
        result = validate_detection(
            _output(
                item_outcome="research_people" if outcome == "research" else "uncertain",
                mentions=[_mention(outcome=outcome)],
            ),
            passages=PASSAGES,
            max_people=8,
        )
        assert result.mentions[0].research_worthy is True


def test_do_not_research_is_not_research_worthy():
    result = validate_detection(
        _output(item_outcome="do_not_research", mentions=[_mention(outcome="do_not_research")]),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].research_worthy is False


def test_more_mentions_than_the_cap_is_rejected():
    with pytest.raises(DetectionInvalid, match="max_people"):
        validate_detection(
            _output(mentions=[_mention(), _mention()]), passages=PASSAGES, max_people=1
        )


def test_unknown_passage_id_is_rejected():
    with pytest.raises(DetectionInvalid, match="passage"):
        validate_detection(
            _output(mentions=[_mention(supporting_passage_ids=["p9"])]),
            passages=PASSAGES,
            max_people=8,
        )


def test_name_grounding_is_case_sensitive():
    # Deliberate: a name is an identity, and case is part of it.
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(mentions=[_mention(exact_name="ana poy")]),
            passages=PASSAGES,
            max_people=8,
        )


def test_invented_name_is_rejected():
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(mentions=[_mention(exact_name="Someone Else")]),
            passages=PASSAGES,
            max_people=8,
        )


def test_identity_fact_value_grounding_is_case_insensitive():
    # Feed titles are title-cased and the model quotes them back in sentence
    # case. This rejected valid output in the prior programme.
    result = validate_detection(
        _output(
            mentions=[
                _mention(
                    identity_facts=[
                        {
                            "kind": "profession_or_role",
                            "value": "sculptor",
                            "supporting_passage_ids": ["p1"],
                        }
                    ]
                )
            ]
        ),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].identity_facts[0].value == "sculptor"


def test_identity_fact_may_be_grounded_in_any_supplied_passage():
    # The value was checked only against cited passages, so a value present in
    # the title but cited to the summary was rejected in 3 of 3 replays.
    result = validate_detection(
        _output(
            mentions=[
                _mention(
                    identity_facts=[
                        {
                            "kind": "place",
                            "value": "Paris",
                            "supporting_passage_ids": ["p1"],
                        }
                    ]
                )
            ]
        ),
        passages=PASSAGES,
        max_people=8,
    )
    assert result.mentions[0].identity_facts[0].value == "Paris"


def test_ungrounded_identity_fact_value_is_rejected():
    with pytest.raises(DetectionInvalid, match="not grounded"):
        validate_detection(
            _output(
                mentions=[
                    _mention(
                        identity_facts=[
                            {
                                "kind": "place",
                                "value": "Reykjavik",
                                "supporting_passage_ids": ["p1"],
                            }
                        ]
                    )
                ]
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_domain_profile_grounding_is_rejected_since_no_profile_is_supplied():
    with pytest.raises(DetectionInvalid, match="domain_profile"):
        validate_detection(
            _output(
                mentions=[
                    _mention(
                        signals=[
                            {
                                "kind": "attention",
                                "category": "major_achievement",
                                "claim": "Won a prize.",
                                "supporting_passage_ids": ["p1"],
                                "grounding": "domain_profile",
                            }
                        ]
                    )
                ]
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_research_people_requires_a_research_or_uncertain_mention():
    with pytest.raises(DetectionInvalid, match="item_outcome"):
        validate_detection(
            _output(
                item_outcome="research_people",
                mentions=[_mention(outcome="do_not_research")],
            ),
            passages=PASSAGES,
            max_people=8,
        )


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(DetectionInvalid):
        validate_detection({"nonsense": True}, passages=PASSAGES, max_people=8)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_detect_contract.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.detect_contract'`.

- [ ] **Step 3: Write `src/notable/detect_contract.py`**

```python
"""The detect_people contract: models, wire schema, and domain validation.

Pure: no I/O, no network client. Every rule here traces to docs/findings.md.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from notable.config import DetectConfig
from notable.feeds import SourceItem

ITEM_OUTCOMES = ("research_people", "do_not_research", "uncertain")
MENTION_OUTCOMES = ("research", "do_not_research", "uncertain")
IDENTITY_FACT_KINDS = (
    "name", "profession_or_role", "place", "nationality",
    "era_or_date", "work", "affiliation", "other",
)
ATTENTION_CATEGORIES = (
    "significant_recognition", "enduring_contribution", "significant_work",
    "institutional_recognition", "sustained_field_attention",
    "major_achievement", "influential_role",
)
CAUTION_CATEGORIES = (
    "single_event_only", "inherited_association", "routine_role_or_listing",
    "primary_or_promotional", "significance_unclear",
)
PASSAGE_IDS = ("p1", "p2")


class DetectionInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Passage(_Strict):
    id: Literal["p1", "p2"]
    field: Literal["title", "summary"]
    text: str = Field(min_length=1)
    truncated: bool


class IdentityFact(_Strict):
    kind: str
    value: str = Field(min_length=1, max_length=500)
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)


class GroundedSignal(_Strict):
    kind: Literal["attention", "caution"]
    category: str
    claim: str = Field(min_length=1, max_length=1000)
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)
    grounding: Literal["source_text", "domain_profile"]


class DetectedMention(_Strict):
    exact_name: str = Field(min_length=1, max_length=300)
    outcome: Literal["research", "do_not_research", "uncertain"]
    supporting_passage_ids: tuple[str, ...] = Field(min_length=1)
    identity_facts: tuple[IdentityFact, ...]
    signals: tuple[GroundedSignal, ...]
    rationale: str = Field(min_length=1, max_length=1000)

    @property
    def research_worthy(self) -> bool:
        """High recall: uncertainty continues, it does not suppress."""
        return self.outcome in {"research", "uncertain"}


class DetectionOutput(_Strict):
    item_outcome: Literal["research_people", "do_not_research", "uncertain"]
    mentions: tuple[DetectedMention, ...]
    overflow: bool
    rationale: str = Field(min_length=1, max_length=1000)


def build_passages(item: SourceItem, config: DetectConfig) -> tuple[Passage, ...]:
    passages: list[Passage] = []
    for identifier, field, raw, limit in (
        ("p1", "title", item.title, config.max_title_characters),
        ("p2", "summary", item.summary, config.max_summary_characters),
    ):
        if not raw:
            continue
        bounded = raw[:limit]
        passages.append(
            Passage(
                id=identifier,  # type: ignore[arg-type]
                field=field,  # type: ignore[arg-type]
                text=bounded,
                truncated=bounded != raw,
            )
        )
    return tuple(passages)


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def detection_schema(*, max_people: int) -> dict[str, object]:
    """The wire schema, capped at `max_people`.

    Config-dependent on purpose: a fixed cap here would drift from
    `max_people` silently, and an uncapped array means the overrun is paid for
    before it is rejected.

    Written by hand rather than derived from pydantic: strict structured
    output supports a narrow subset, and the root must be a plain object --
    a root-level `anyOf` is rejected with HTTP 400 (docs/findings.md).
    """
    passage_ids = {
        "type": "array",
        "items": {"type": "string", "enum": list(PASSAGE_IDS)},
        "minItems": 1,
    }
    return _object(
        {
            "item_outcome": _string(enum=list(ITEM_OUTCOMES)),
            "mentions": {
                "type": "array",
                "maxItems": max_people,
                "items": _object(
                    {
                        "exact_name": _string(maxLength=300),
                        "outcome": _string(enum=list(MENTION_OUTCOMES)),
                        "supporting_passage_ids": passage_ids,
                        "identity_facts": {
                            "type": "array",
                            "items": _object(
                                {
                                    "kind": _string(enum=list(IDENTITY_FACT_KINDS)),
                                    "value": _string(maxLength=500),
                                    "supporting_passage_ids": passage_ids,
                                }
                            ),
                        },
                        "signals": {
                            "type": "array",
                            "items": _object(
                                {
                                    "kind": _string(enum=["attention", "caution"]),
                                    "category": _string(
                                        enum=[
                                            *ATTENTION_CATEGORIES,
                                            *CAUTION_CATEGORIES,
                                        ]
                                    ),
                                    "claim": _string(maxLength=1000),
                                    "supporting_passage_ids": passage_ids,
                                    "grounding": _string(
                                        enum=["source_text", "domain_profile"]
                                    ),
                                }
                            ),
                        },
                        "rationale": _string(maxLength=1000),
                    }
                ),
            },
            "overflow": {"type": "boolean"},
            "rationale": _string(maxLength=1000),
        }
    )


def _contains(haystack: str, needle: str, *, fold_case: bool) -> bool:
    if fold_case:
        return needle.casefold() in haystack.casefold()
    return needle in haystack


def validate_detection(
    raw: dict[str, Any], *, passages: tuple[Passage, ...], max_people: int
) -> DetectionOutput:
    """Parse and apply the rules the wire schema cannot express."""
    try:
        output = DetectionOutput.model_validate(raw)
    except ValidationError as error:
        raise DetectionInvalid(f"detection output failed validation: {error}") from error

    if len(output.mentions) > max_people:
        raise DetectionInvalid(
            f"{len(output.mentions)} mentions exceeds max_people {max_people}"
        )

    known = {passage.id for passage in passages}
    corpus = "\n".join(passage.text for passage in passages)

    for mention in output.mentions:
        _check_references(mention.supporting_passage_ids, known)
        # A name is an identity: grounding stays case-sensitive on purpose.
        if not _contains(corpus, mention.exact_name, fold_case=False):
            raise DetectionInvalid("mention name is not grounded in supplied text")

        for fact in mention.identity_facts:
            _check_references(fact.supporting_passage_ids, known)
            if fact.kind not in IDENTITY_FACT_KINDS:
                raise DetectionInvalid(f"unknown identity fact kind: {fact.kind}")
            # Case-insensitive, and searching *every* supplied passage rather
            # than only the cited ones. Both widenings fixed real rejections of
            # valid output; neither admits invention, because the value must
            # still appear as a contiguous run differing only in case.
            if not _contains(corpus, fact.value, fold_case=True):
                raise DetectionInvalid("identity fact value is not grounded")

        for signal in mention.signals:
            _check_references(signal.supporting_passage_ids, known)
            if signal.grounding == "domain_profile":
                raise DetectionInvalid(
                    "domain_profile grounding requires a supplied profile; none is"
                )
            expected = (
                ATTENTION_CATEGORIES if signal.kind == "attention" else CAUTION_CATEGORIES
            )
            if signal.category not in expected:
                raise DetectionInvalid(
                    f"category {signal.category} is not valid for a {signal.kind} signal"
                )

    _check_item_outcome(output)
    return output


def _check_references(ids: tuple[str, ...], known: set[str]) -> None:
    unknown = set(ids) - known
    if unknown:
        raise DetectionInvalid(f"unknown passage reference: {sorted(unknown)}")


def _check_item_outcome(output: DetectionOutput) -> None:
    outcomes = {mention.outcome for mention in output.mentions}
    if output.item_outcome == "research_people" and not (
        outcomes & {"research", "uncertain"}
    ):
        raise DetectionInvalid(
            "item_outcome research_people requires a research or uncertain mention"
        )
    if output.item_outcome == "do_not_research" and outcomes - {"do_not_research"}:
        raise DetectionInvalid(
            "item_outcome do_not_research requires every mention to be do_not_research"
        )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_detect_contract.py -v`
Expected: 21 passed.

- [ ] **Step 5: Verify the grounding rules actually discriminate**

Each of these must turn a passing test red. Restore after each.

```bash
cp src/notable/detect_contract.py /tmp/dc.bak
# 1. Make name grounding case-insensitive -> test_name_grounding_is_case_sensitive fails
# 2. Make fact grounding case-sensitive   -> test_identity_fact_value_grounding_is_case_insensitive fails
# 3. Drop the maxItems line               -> test_schema_caps_mentions_at_max_people fails
PYTHONDONTWRITEBYTECODE=1 uv run pytest tests/mvp/test_detect_contract.py -q
cp /tmp/dc.bak src/notable/detect_contract.py && diff /tmp/dc.bak src/notable/detect_contract.py
```

Report which mutation killed which test. A rule that survives its own removal is untested.

- [ ] **Step 6: Commit**

```bash
git add src/notable/detect_contract.py tests/mvp/test_detect_contract.py
git commit -m "feat(detect): add detection contract with schema and grounding rules"
```

---

### Task 8: The detection service

**Files:**
- Create: `src/notable/detect.py`
- Test: `tests/mvp/test_detect.py`

**Interfaces:**
- Consumes: `notable.detect_contract`, `notable.llm.LlmClient`, `notable.config.Config`, `notable.errors.Incomplete`, `notable.errors.ProviderFailure`.
- Produces: `notable.detect.people_in(item: SourceItem, config: Config, llm: LlmClient) -> tuple[DetectedMention, ...]`.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_detect.py`:

```python
import pytest

from notable.detect import people_in
from notable.errors import BudgetExceeded, Incomplete, ProviderFailure
from notable.feeds import SourceItem

ITEM = SourceItem(
    url="https://a.test/1",
    title="Sculptor Ana Poy wins prize",
    summary="Ana Poy showed in Paris.",
    published_at=None,
    feed_key="a",
    publisher_label="A",
)

GOOD = {
    "item_outcome": "research_people",
    "mentions": [
        {
            "exact_name": "Ana Poy",
            "outcome": "research",
            "supporting_passage_ids": ["p1"],
            "identity_facts": [],
            "signals": [],
            "rationale": "Named subject.",
        }
    ],
    "overflow": False,
    "rationale": "One subject.",
}


class FakeLlm:
    def __init__(self, result=None, error=None):
        self.result, self.error, self.calls = result, error, []

    def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


def test_returns_validated_mentions(make_config):
    mentions = people_in(ITEM, make_config(), FakeLlm(result=GOOD))
    assert [m.exact_name for m in mentions] == ["Ana Poy"]
    assert mentions[0].research_worthy is True


def test_an_item_with_no_usable_text_makes_no_model_call(make_config):
    llm = FakeLlm(result=GOOD)
    empty = SourceItem(ITEM.url, None, None, None, "a", "A")
    assert people_in(empty, make_config(), llm) == ()
    assert llm.calls == [], "an empty item must not be paid for"


def test_the_call_carries_the_ported_prompt_and_capped_schema(make_config):
    llm = FakeLlm(result=GOOD)
    people_in(ITEM, make_config(), llm)
    call = llm.calls[0]
    assert call["task"] == "detect_people"
    assert "Return strict schema" in call["system"]
    assert call["schema"]["properties"]["mentions"]["maxItems"] == 8
    assert call["max_completion_tokens"] == 4096


def test_a_validation_failure_raises_incomplete_and_is_not_retried(make_config):
    llm = FakeLlm(result={"item_outcome": "research_people"})  # missing fields
    with pytest.raises(Incomplete):
        people_in(ITEM, make_config(), llm)
    assert len(llm.calls) == 1, "a validation failure must not be re-paid for in-run"


def test_a_provider_failure_raises_incomplete(make_config):
    llm = FakeLlm(error=ProviderFailure("boom", permanent=False))
    with pytest.raises(Incomplete):
        people_in(ITEM, make_config(), llm)


def test_budget_exceeded_propagates_rather_than_becoming_incomplete(make_config):
    # BudgetExceeded ends the pass; Incomplete only ends the item.
    llm = FakeLlm(error=BudgetExceeded("cap"))
    with pytest.raises(BudgetExceeded):
        people_in(ITEM, make_config(), llm)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_detect.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.detect'`.

- [ ] **Step 3: Write `src/notable/detect.py`**

```python
"""One model call per source item: who is meaningfully in this story?"""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources

from notable.config import Config
from notable.detect_contract import (
    DetectedMention,
    DetectionInvalid,
    build_passages,
    detection_schema,
    validate_detection,
)
from notable.errors import Incomplete, ProviderFailure
from notable.feeds import SourceItem
from notable.llm import LlmClient

logger = logging.getLogger(__name__)


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("detect_people.md")
        .read_text(encoding="utf-8")
    )


def people_in(
    item: SourceItem, config: Config, llm: LlmClient
) -> tuple[DetectedMention, ...]:
    """Detect mentions in one item.

    Raises `Incomplete` if the call or its validation fails: that ends this
    item, leaves it unsettled for a later run, and must never be converted
    into a semantic "found nobody". `BudgetExceeded` deliberately propagates —
    it ends the whole pass, not just this item.
    """
    passages = build_passages(item, config.detect)
    if not passages:
        return ()

    payload = {
        "task": "detect_people",
        "feed_key": item.feed_key,
        "publisher_label": item.publisher_label,
        "url": item.url,
        "published_at": item.published_at,
        "passages": [passage.model_dump() for passage in passages],
        "max_people": config.detect.max_people,
    }

    try:
        raw = llm.structured(
            task="detect_people",
            model=config.detect.model,
            system=_system_prompt(),
            user_payload=payload,
            schema=detection_schema(max_people=config.detect.max_people),
            max_completion_tokens=config.detect.max_completion_tokens,
            reasoning_effort=config.detect.reasoning_effort,
            timeout=config.transport.llm_read_timeout_seconds,
        )
    except ProviderFailure as error:
        logger.warning("detect_people failed for %s: %s", item.url, error)
        raise Incomplete(f"detect_people failed for {item.url}") from error

    try:
        output = validate_detection(
            raw, passages=passages, max_people=config.detect.max_people
        )
    except DetectionInvalid as error:
        # Not retried in-run: a retry re-sends identical input, and the prior
        # programme measured twelve such retries with zero recoveries.
        logger.warning("detect_people output rejected for %s: %s", item.url, error)
        raise Incomplete(f"detect_people output rejected for {item.url}") from error

    return output.mentions
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_detect.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/detect.py tests/mvp/test_detect.py
git commit -m "feat(detect): add the detection service with Incomplete on failure"
```

---

### Task 9: The digest writer

**Files:**
- Create: `src/notable/digest.py`
- Test: `tests/mvp/test_digest.py`

**Interfaces:**
- Consumes: `notable.config.Config`, `notable.detect_contract.DetectedMention`.
- Produces:
  - `notable.digest.DigestEntry(identity_key: str, display_name: str, source_url: str, publisher_label: str, rationale: str)` frozen dataclass.
  - `notable.digest.identity_key(name: str) -> str`
  - `notable.digest.render(entries, *, generated_at, status, cost_usd, n_settled, n_incomplete) -> str`
  - `notable.digest.write(entries, directory: Path, *, generated_at, status, cost_usd, n_settled, n_incomplete) -> Path` — takes the directory, not the whole `Config`, so it stays testable without one.

**Phase 1 note:** entries are *detected people*, not ranked leads. The heading says so. Phase 4 replaces the body with the ranked shortlist; the atomic write and `latest.md` handling stay.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_digest.py`:

```python
from decimal import Decimal

import pytest

from notable.digest import DigestEntry, identity_key, render, write

ENTRY = DigestEntry(
    identity_key="ana poy",
    display_name="Ana Poy",
    source_url="https://a.test/1",
    publisher_label="Feed A",
    rationale="Named subject.",
)

COUNTS = {
    "generated_at": "2026-08-03T09:00:00+00:00",
    "status": "ok",
    "cost_usd": Decimal("0.42"),
    "n_settled": 3,
    "n_incomplete": 0,
}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ana Poy", "ana poy"),
        ("  Ana   Poy  ", "ana poy"),
        ("ANA POY", "ana poy"),
        ("Ana Poy", "ana poy"),      # NFKC folds the non-breaking space
        ("Ａna Poy", "ana poy"),      # NFKC folds fullwidth Latin
    ],
)
def test_identity_key_normalizes(raw, expected):
    assert identity_key(raw) == expected


def test_identity_key_keeps_different_names_distinct():
    assert identity_key("Ana Poy") != identity_key("Ana Poye")


def test_render_lists_entries_with_their_sources():
    text = render([ENTRY], **COUNTS)
    assert "Ana Poy" in text
    assert "https://a.test/1" in text
    assert "Feed A" in text


def test_render_reports_run_status_and_cost():
    text = render([ENTRY], **COUNTS | {"status": "partial", "n_incomplete": 2})
    assert "Run status: **partial**" in text
    assert "$0.42" in text
    assert "Items incomplete: 2" in text


def test_empty_digest_is_still_a_valid_document():
    text = render([], **COUNTS)
    assert text.startswith("# ")
    assert "No people detected" in text


def test_write_creates_the_dated_file_and_latest(tmp_path):
    path = write([ENTRY], tmp_path, **COUNTS)
    assert path.parent == tmp_path
    assert path.name == "2026-08-03.md"
    latest = tmp_path / "latest.md"
    assert latest.read_text("utf-8") == path.read_text("utf-8")


def test_write_leaves_no_temporary_file_behind(tmp_path):
    write([ENTRY], tmp_path, **COUNTS)
    assert [p.name for p in tmp_path.iterdir() if ".tmp" in p.name] == []


def test_write_replaces_an_existing_digest_for_the_same_day(tmp_path):
    write([ENTRY], tmp_path, **COUNTS)
    second = DigestEntry("bo li", "Bo Li", "https://a.test/2", "Feed A", "Named.")
    path = write([second], tmp_path, **COUNTS)
    assert "Bo Li" in path.read_text("utf-8")
    assert "Ana Poy" not in path.read_text("utf-8")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_digest.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.digest'`.

- [ ] **Step 3: Write `src/notable/digest.py`**

```python
"""Markdown digest rendering and atomic file writing."""

from __future__ import annotations

import os
import tempfile
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DigestEntry:
    identity_key: str
    display_name: str
    source_url: str
    publisher_label: str
    rationale: str


def identity_key(name: str) -> str:
    """An opaque scalar. In the MVP a normalized name; later a person id.

    Only `rank` (as a tie-breaker and for collapsing), `digest` (for
    suppression) and `store` (which keys `surfaced` on it) may read this, and
    none of them may parse it.
    """
    folded = unicodedata.normalize("NFKC", name).casefold()
    return " ".join(folded.split())


def render(
    entries: Sequence[DigestEntry],
    *,
    generated_at: str,
    status: str,
    cost_usd: Decimal,
    n_settled: int,
    n_incomplete: int,
) -> str:
    day = generated_at[:10]
    lines = [
        f"# Notable — detected people, {day}",
        "",
        f"- Run status: **{status}**",
        f"- Items settled: {n_settled}",
        f"- Items incomplete: {n_incomplete}",
        f"- Model spend: ${cost_usd}",
        "",
        "> Phase 1 lists people detected in feed items. It does not check",
        "> Wikipedia and does not assess coverage, so nothing here is a lead.",
        "",
        "## Detected",
        "",
    ]
    if not entries:
        lines.append("No people detected in this run.")
    else:
        for entry in entries:
            lines.append(f"### {entry.display_name}")
            lines.append("")
            lines.append(f"- Source: [{entry.publisher_label}]({entry.source_url})")
            lines.append(f"- Why: {entry.rationale}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write(
    entries: Sequence[DigestEntry],
    directory: Path,
    *,
    generated_at: str,
    status: str,
    cost_usd: Decimal,
    n_settled: int,
    n_incomplete: int,
) -> Path:
    """Write the dated digest and `latest.md`, both atomically.

    Ordering is load-bearing: the files land before any state is committed. A
    crash between them repeats a digest next run, which is recoverable. The
    reverse would mark leads surfaced that were never seen.
    """
    directory.mkdir(parents=True, exist_ok=True)
    text = render(
        entries,
        generated_at=generated_at,
        status=status,
        cost_usd=cost_usd,
        n_settled=n_settled,
        n_incomplete=n_incomplete,
    )
    dated = directory / f"{generated_at[:10]}.md"
    _atomic_write(dated, text)
    _atomic_write(directory / "latest.md", text)
    return dated


def _atomic_write(path: Path, text: str) -> None:
    handle = tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent,
        prefix=f"{path.name}.", suffix=".tmp", delete=False,
    )
    temporary = Path(handle.name)
    try:
        with handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/mvp/test_digest.py -v`
Expected: 12 passed.

- [ ] **Step 5: Commit**

```bash
git add src/notable/digest.py tests/mvp/test_digest.py
git commit -m "feat(digest): add Markdown rendering and atomic digest writing"
```

---

### Task 10: The pipeline and CLI

**Files:**
- Create: `src/notable/pipeline.py`
- Modify: `src/notable/cli.py`
- Test: `tests/mvp/test_pipeline.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `notable.pipeline.Providers(transport: Transport, llm: LlmClient)` frozen dataclass.
  - `notable.pipeline.run(config: Config, store: Store, providers: Providers, *, fresh_feeds: bool = False) -> Path`.

- [ ] **Step 1: Write the failing test**

`tests/mvp/test_pipeline.py`:

```python
from decimal import Decimal
from typing import cast

import pytest

from notable.detect_contract import DetectedMention
from notable.errors import BudgetExceeded, Incomplete
from notable.feeds import SourceItem
from notable.http import Transport
from notable.llm import LlmClient
from notable.pipeline import Providers, run


def _item(n: int) -> SourceItem:
    return SourceItem(f"https://a.test/{n}", f"Title {n}", "Summary.", None, "a", "A")


def _mention(name: str, outcome: str = "research") -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        outcome=outcome,
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Named subject.",
    )


class FakeLlm:
    def spend(self) -> Decimal:
        return Decimal("0.10")


@pytest.fixture
def providers() -> Providers:
    # The pipeline never touches the transport directly; feeds and detect are
    # both patched out, so a null transport is honest about what is exercised.
    return Providers(
        transport=cast(Transport, None), llm=cast(LlmClient, FakeLlm())
    )


@pytest.fixture
def install(monkeypatch):
    """Patch the two seams the pipeline calls. monkeypatch undoes both."""

    def apply(items, detect_fn):
        monkeypatch.setattr(
            "notable.pipeline.feeds.fetch_new", lambda *a, **k: iter(items)
        )
        monkeypatch.setattr("notable.pipeline.detect.people_in", detect_fn)

    return apply


def _returning(mapping):
    return lambda item, cfg, llm: mapping[item.url]


def test_writes_a_digest_and_settles_items(make_config, store, providers, install):
    install([_item(1)], _returning({"https://a.test/1": (_mention("Ana Poy"),)}))
    path = run(make_config(), store, providers)
    assert "Ana Poy" in path.read_text("utf-8")
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_non_research_worthy_mentions_are_not_shown(
    make_config, store, providers, install
):
    install(
        [_item(1)],
        _returning(
            {"https://a.test/1": (_mention("Ana Poy", outcome="do_not_research"),)}
        ),
    )
    path = run(make_config(), store, providers)
    assert "Ana Poy" not in path.read_text("utf-8")
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_an_incomplete_item_is_not_settled_and_contributes_nothing(
    make_config, store, providers, install
):
    def detect(item, cfg, llm):
        if item.url.endswith("1"):
            raise Incomplete("nope")
        return (_mention("Bo Li"),)

    install([_item(1), _item(2)], detect)
    path = run(make_config(), store, providers)
    text = path.read_text("utf-8")
    assert "Bo Li" in text
    assert "Run status: **partial**" in text
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True
    assert store.is_eligible("https://a.test/2", max_attempts=3) is False


def test_incomplete_discards_earlier_mentions_of_the_same_item(
    make_config, store, providers, install
):
    # Invariant 3: mentions one and two go with mention three's failure. A
    # generator that yields two mentions and then raises is exactly that case.
    def detect(item, cfg, llm):
        def mentions():
            yield _mention("Ana Poy")
            yield _mention("Bo Li")
            raise Incomplete("failed on the third mention")

        return mentions()

    install([_item(1)], detect)
    path = run(make_config(), store, providers)
    text = path.read_text("utf-8")
    assert "Ana Poy" not in text and "Bo Li" not in text
    assert "No people detected" in text
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True


def test_budget_exceeded_renders_what_finished(
    make_config, store, providers, install
):
    def detect(item, cfg, llm):
        if item.url.endswith("2"):
            raise BudgetExceeded("cap")
        return (_mention("Ana Poy"),)

    install([_item(1), _item(2)], detect)
    path = run(make_config(), store, providers)
    text = path.read_text("utf-8")
    assert "Ana Poy" in text
    assert "Run status: **partial**" in text
    assert store.is_eligible("https://a.test/2", max_attempts=3) is True


def test_a_crash_before_commit_writes_nothing(
    make_config, store, providers, install, monkeypatch
):
    install([_item(1)], _returning({"https://a.test/1": (_mention("Ana Poy"),)}))
    monkeypatch.setattr("notable.pipeline.digest.write", _boom)
    with pytest.raises(RuntimeError):
        run(make_config(), store, providers)
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True
    assert store.connection.execute("SELECT COUNT(*) FROM run").fetchone()[0] == 0
    assert store.connection.execute("SELECT COUNT(*) FROM item").fetchone()[0] == 0


def _boom(*_args, **_kwargs):
    raise RuntimeError("digest write failed")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/mvp/test_pipeline.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notable.pipeline'`.

- [ ] **Step 3: Write `src/notable/pipeline.py`**

```python
"""The whole loop, readable top to bottom."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from notable import detect, digest, feeds
from notable.config import Config
from notable.errors import BudgetExceeded, Incomplete
from notable.http import Transport
from notable.llm import LlmClient
from notable.store import RunSummary, Store


@dataclass(frozen=True, slots=True)
class Providers:
    transport: Transport
    llm: LlmClient


def run(
    config: Config,
    store: Store,
    providers: Providers,
    *,
    fresh_feeds: bool = False,
) -> Path:
    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    entries: list[digest.DigestEntry] = []
    settled: list[str] = []
    incomplete: list[str] = []
    capped = False
    try:
        for item in feeds.fetch_new(config, providers.transport, store, fresh=fresh_feeds):
            item_entries = []
            try:
                for mention in detect.people_in(item, config, providers.llm):
                    if not mention.research_worthy:
                        continue
                    item_entries.append(
                        digest.DigestEntry(
                            identity_key=digest.identity_key(mention.exact_name),
                            display_name=mention.exact_name,
                            source_url=item.url,
                            publisher_label=item.publisher_label,
                            rationale=mention.rationale,
                        )
                    )
            except Incomplete:
                incomplete.append(item.url)   # retried next run, up to a cap
                continue
            entries.extend(item_entries)
            settled.append(item.url)
    except BudgetExceeded:
        capped = True                         # render what finished

    summary = RunSummary(settled, incomplete, capped, providers.llm.spend(), started_at)
    written = digest.write(
        entries[: config.digest_size],
        config.digest_dir,
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        status=summary.status,
        cost_usd=summary.cost_usd,
        n_settled=len(settled),
        n_incomplete=len(incomplete),
    )
    # Nothing durable is written until the digest file exists. Phase 4 passes
    # surfaced keys and lead rows; both parameters exist now so the signatures
    # do not move.
    store.commit(settled, incomplete, [])
    store.log(summary, [], str(written))
    return written
```

- [ ] **Step 4: Wire the CLI**

Replace the `main` function in `src/notable/cli.py`:

```python
def main(argv: list[str] | None = None) -> int:
    import logging
    from pathlib import Path

    import httpx

    from notable.cache import Cache
    from notable.config import load_config
    from notable.http import Transport
    from notable.llm import LlmClient
    from notable.pipeline import Providers, run
    from notable.store import Store

    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.command != "run":
        parser.error(f"unknown command: {args.command}")
        return 2

    try:
        config = load_config(Path(args.config))
    except ValueError as error:
        print(f"configuration error: {error}")
        return 2

    store = Store(config.data_dir / "notable.db")
    try:
        with httpx.Client(follow_redirects=True) as client:
            transport = Transport(config.transport, Cache(config.cache.dir), client=client)
            providers = Providers(
                transport=transport,
                llm=LlmClient(
                    transport,
                    config.openrouter,
                    api_key=config.openrouter_api_key,
                    budget_usd=config.budget_usd,
                ),
            )
            written = run(config, store, providers, fresh_feeds=args.fresh_feeds)
    finally:
        store.close()

    print(written.read_text(encoding="utf-8"))
    return 0
```

Add the `--config` argument inside `build_parser`, on the `run` subparser:

```python
    run.add_argument(
        "--config",
        default="config/notable.toml",
        help="Path to the configuration file.",
    )
```

- [ ] **Step 5: Run the whole suite to verify it passes**

Run: `uv run pytest tests/mvp -v`
Expected: all pass. Then check the guardrails:

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
find src -name '*.py' | xargs wc -l | tail -1
awk '/^def run\(/,/^    return written/' src/notable/pipeline.py | wc -l
```

Expected: no errors; source well under 3,000 lines; the loop under 40 lines.

- [ ] **Step 6: Commit**

```bash
git add src/notable/pipeline.py src/notable/cli.py tests/mvp/test_pipeline.py
git commit -m "feat(pipeline): wire the detection-only walking skeleton"
```

---

### Task 11: The live run and its fixture

**Files:**
- Create: `config/notable.toml` (untracked — local configuration)
- Create: `tests/mvp/fixtures/phase1/` (recorded cache)
- Create: `tests/mvp/test_live.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: everything.
- Produces: a committed fixture cache directory that later phases replay.

**A digest appearing is not the success condition.** Nothing in this phase has met a provider, and `docs/findings.md` records that the prior programme's two most expensive defects were invisible offline and survived two independent static reviews. **A fixture recorded from an unexamined run encodes its bugs as expected behaviour.**

- [ ] **Step 1: Create local configuration**

```bash
cp config/notable.example.toml config/notable.toml
cp config/feeds.example.toml config/feeds.toml
```

Edit `config/notable.toml`: set `feeds_file = "feeds.toml"`, set a real `contact_url`, and **uncomment `openrouter_usd_per_run = "1.00"`** — the first live run must be capped.

Add to `.gitignore`:

```gitignore
/config/notable.toml
/config/feeds.toml
```

- [ ] **Step 2: Verify configuration without spending anything**

```bash
set -a && . ./.env && set +a
uv run python -c "
from pathlib import Path
from notable.config import load_config
c = load_config(Path('config/notable.toml'))
print(f'{len(c.feeds)} feeds, cap {c.budget_usd}, model {c.detect.model}')
print('key loaded:', bool(c.openrouter_api_key))
"
```

Expected: 10 feeds, a cap, the model id, and `key loaded: True`. **The key itself must never print.**

- [ ] **Step 3: Do a first live run**

```bash
set -a && . ./.env && set +a
time uv run notable run 2>&1 | tee /tmp/phase1-run.log
```

- [ ] **Step 4: Examine the run before trusting it**

This is the step that matters. Work through all six:

```bash
# 1. Raw model responses -- read three in full, not just the parsed result.
find data/cache -name '*.json' | head -3 | xargs -I{} sh -c 'python -m json.tool {} | head -40'

# 2. Validation failures and their reasons.
grep -c "output rejected" /tmp/phase1-run.log
grep "output rejected" /tmp/phase1-run.log | head

# 3. Truncation: any response cut off mid-string is a completion-budget bug,
#    not a model bug. Raise max_completion_tokens with max_people.
grep -ci "unterminated\|Expecting" /tmp/phase1-run.log

# 4. Cache replay actually works.
sqlite3 data/notable.db "SELECT status, n_items_settled, n_items_incomplete, cost_usd FROM run"
time uv run notable run          # second run: should be seconds, near-zero cost
sqlite3 data/notable.db "SELECT cost_usd FROM run ORDER BY id DESC LIMIT 1"

# 5. Measured spend against estimate.
find data/cache -name '*.json' | wc -l

# 6. Pacing and 429s.
grep -c "429" /tmp/phase1-run.log
```

Expected: the second run costs about $0.00 and settles 0 new items. A non-zero `429` count means `per_host_min_interval_ms` is too low. Any truncation means fix `max_completion_tokens` and re-run **before** recording the fixture.

- [ ] **Step 5: Record the fixture only once the run is clean**

```bash
mkdir -p tests/mvp/fixtures/phase1
cp -R data/cache/. tests/mvp/fixtures/phase1/
grep -rl "sk-" tests/mvp/fixtures/phase1/ || echo "no secrets in fixture"
du -sh tests/mvp/fixtures/phase1
```

The `grep` must print `no secrets in fixture`. If it lists files, **stop** — the cache key or envelope is leaking credentials and that is a bug in `Task 4`, not something to scrub by hand.

- [ ] **Step 6: Write the offline replay test**

`tests/mvp/test_live.py`:

```python
"""Replay of a recorded live run, plus the opt-in live smoke."""

import shutil
from pathlib import Path

import httpx
import pytest

from notable.cache import Cache
from notable.config import load_config
from notable.http import Transport
from notable.llm import LlmClient
from notable.pipeline import Providers, run
from notable.store import Store

FIXTURE = Path("tests/mvp/fixtures/phase1")


def _refuse(request):  # pragma: no cover - only fires on a cache miss
    raise AssertionError(f"unexpected network call to {request.url}")


@pytest.mark.skipif(not FIXTURE.exists(), reason="fixture not recorded yet")
def test_recorded_run_replays_offline_with_no_network(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    cache_dir = tmp_path / "cache"
    shutil.copytree(FIXTURE, cache_dir)
    loaded = load_config(Path("config/notable.example.toml"))
    config = loaded.model_copy(
        update={
            "digest_dir": tmp_path / "digests",
            "data_dir": tmp_path / "data",
            # model_copy does not recurse: the nested cache config must be
            # replaced explicitly, or the test writes to the real cache dir.
            "cache": loaded.cache.model_copy(update={"dir": cache_dir}),
        }
    )
    store = Store(tmp_path / "db.sqlite")
    client = httpx.Client(transport=httpx.MockTransport(_refuse))
    transport = Transport(config.transport, Cache(cache_dir), client=client, sleep=lambda _s: None)
    providers = Providers(
        transport=transport,
        llm=LlmClient(transport, config.openrouter, api_key="sk-test", budget_usd=None),
    )
    written = run(config, store, providers)
    assert written.exists()
    assert store.connection.execute("SELECT COUNT(*) FROM item").fetchone()[0] > 0
    store.close()


@pytest.mark.live
def test_live_detection_smoke():
    """Opt-in: uv run pytest tests/mvp -m live -v"""
    config = load_config(Path("config/notable.toml"))
    assert config.budget_usd is not None, "never run live without a cap"
```

- [ ] **Step 7: Run the full suite and commit**

```bash
uv run pytest tests/mvp -v
git add tests/mvp/fixtures/phase1 tests/mvp/test_live.py .gitignore
git commit -m "test: record the phase 1 live-run cache as a replay fixture"
```

---

## Phase 1 completion gate

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/mvp -v
find src -name '*.py' | xargs wc -l | tail -1
git status --short
git diff --check
```

All must pass, the working tree must be clean, and source must be under 3,000 lines.

Then confirm the four spec invariants hold in the shipped code, by reading `pipeline.py`:

1. Nothing durable is written before `digest.write` returns.
2. `store.commit` is called once, after the digest file lands.
3. `item_entries` is local to the item, so `Incomplete` discards it.
4. `BudgetExceeded` is caught in `run`, not in `cli.py`.

**Report from the live run:** items fetched, items settled, items incomplete, validation failures with reasons, measured spend, second-run replay cost, and any 429s. That report — not the digest's existence — is what says Phase 1 is done.
