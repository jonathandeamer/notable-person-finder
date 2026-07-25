# Run Engine and Shared Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the durable run engine — run, work-item, and attempt state; the retry coordinator and budget reservation; the bounded shared HTTP transport with URL, DNS, redirect, and response-size safety; structured redacted logging; and terminal-state reporting that writes a real digest for an empty run.

**Architecture:** Milestone 2 adds the operational half of the application: `runs/` owns run lifecycle, durable work claiming, retry, budget, and bounded concurrency; `providers/` owns the run-scoped HTTPX transport, URL/DNS/redirect safety, and the typed failure taxonomy that every future adapter raises; `reporting/` writes the immutable Markdown digest; `obs/` writes redacted JSON Lines logs. No concrete provider adapter and no domain record exists yet — the only task types are synthetic ones defined in tests. Migration `0002` creates `run`, `run_transition`, `work_item`, and `attempt` only.

**Tech Stack:** Python 3.13, uv, hatchling, HTTPX 0.28, Pydantic 2, standard-library `sqlite3`, `concurrent.futures`, `logging.handlers`, `socket`, `ipaddress`, `decimal`, pytest 8.

## Global Constraints

- The distribution name is `notable-person-finder`, the import package is `notable_person_finder`, and the executable is `notable`.
- Do not import, run, reorganize, or repair `run_pipeline.py`, `scripts/`, JSONL state, or `tests/test_*.py`. The prototype suite is not a gate for this milestone.
- This milestone adds **no** concrete provider adapter (`FeedClient`, `MediaWikiClient`, `WebSearchClient`, `ArticleFetcher`, `ArticleExtractor`, `LlmClient`) and **no** domain table. Those belong to milestones 3–6.
- Migration files are forward-only, checksummed, and transactional. Never edit `0001_configuration_snapshots.sql`; its checksum is already recorded in operator databases.
- Money is integer nano-USD (1 USD = 1_000_000_000). Never use float for money.
- Timestamps are UTC ISO-8601 text ending in `Z`. Durations are integer milliseconds.
- Every connection already sets foreign keys, WAL, `synchronous=FULL`, and a busy timeout via `connect_database`; do not open `sqlite3.connect` directly in application code.
- No SQLite transaction may be open across a network call. Worker threads never touch a `sqlite3.Connection`.
- The first external attempt is created in the same transaction that claims its work item and reserves its budget. Each retry continues the persisted work-item ordinal and reserves its own maximum cost before the call.
- Only the central `RetryCoordinator` starts a repeat request. HTTPX automatic retries stay disabled.
- Raw `httpx` types never cross the `providers/` boundary. Domain and CLI code sees `HttpResponse` and `ProviderFailure` only.
- Secrets never appear in logs, snapshots, digests, exception messages, terminal output, or tests.
- The default pytest suite is offline. `tests/run_engine/conftest.py` denies socket creation; no test sleeps in real time — clocks are injected.
- Run `uv run pytest tests/run_engine` for this milestone and `uv run pytest tests/foundation tests/run_engine` before claiming completion. Do not run bare `pytest`.

---

## Implementation Programme

This is milestone 2 of the seven listed in `docs/superpowers/plans/2026-07-24-application-foundation.md`. It consumes the foundation's configuration loader, resolved paths, SQLite connection helper, migration runner, and mutation lock. It produces the run, work, attempt, transport, retry, and budget interfaces that milestones 3–6 build their adapters and workflow on.

Two commands arrive **partially** and the plan says so explicitly rather than pretending they are finished:

- `notable status` reports run state, pending/deferred required work, and operational-failure counts. Digest backlog, oldest pending candidate, and queue tiers need milestone 6's digest queue.
- The digest writer emits the header and the operational summary. The ranked shortlist section and optional model synthesis arrive in milestone 6.

## File Structure

```text
pyproject.toml                              # add httpx dependency
config/notable.example.toml                 # add operational sections
src/notable_person_finder/
  config/
    models.py                               # MODIFY: add operational config models
  db/
    migrations/
      0002_run_engine.sql                   # run, run_transition, work_item, attempt
  obs/
    __init__.py
    logging.py                              # JSON Lines rotating log + secret redaction
  providers/
    __init__.py
    failures.py                             # FailureCategory, ProviderFailure, ProviderPaused
    safety.py                               # scheme/credential/DNS checks, redirect guard
    transport.py                            # run-scoped HTTPX transport, streaming bounds
    pacing.py                               # per-origin concurrency, per-provider interval
  runs/
    lock.py                                 # unchanged foundation module
    clock.py                                # Clock protocol, SystemClock, injectable fake
    models.py                               # RunState, WorkState, AttemptOutcome, records
    repository.py                           # feature-owned SQL for run/work/attempt
    budget.py                               # nano-USD reserve and reconcile
    retry.py                                # retry coordinator and provider pause
    scheduler.py                            # bounded ThreadPoolExecutor submission
    engine.py                               # run lifecycle and terminal-state derivation
  reporting/
    __init__.py
    digest.py                               # render, atomic write, latest.md, content hash
  cli/
    main.py                                 # MODIFY: notable run, notable status, exits
tests/
  run_engine/
    __init__.py
    conftest.py                             # offline guard, temp roots, fixtures
    helpers.py                              # config graph with operational sections
    test_operational_config.py
    test_run_engine_schema.py
    test_failures.py
    test_safety.py
    test_transport.py
    test_pacing.py
    test_retry.py
    test_budget.py
    test_repository.py
    test_scheduler.py
    test_engine.py
    test_logging.py
    test_digest.py
    test_run_cli.py
    test_crash_boundary.py
```

---

## Task 1: Operational Configuration Models

The foundation's `MainConfig` covers timezone, file references, paths, and secret names. Every bound this milestone enforces — timeouts, redirect limit, response sizes, retry policy, concurrency, pacing, budget, digest, and logging — must be configuration rather than a constant, and must fail validation before any run is created.

All new sections carry defaults so the existing foundation configuration graph still validates unchanged.

**Files:**
- Modify: `src/notable_person_finder/config/models.py`
- Modify: `config/notable.example.toml`
- Create: `tests/run_engine/__init__.py`
- Create: `tests/run_engine/helpers.py`
- Create: `tests/run_engine/conftest.py`
- Test: `tests/run_engine/test_operational_config.py`

**Interfaces:**
- Consumes: `StrictModel`, `MainConfig` from `notable_person_finder.config.models`; `load_config`, `ResolvedConfig` from `notable_person_finder.config.loader`.
- Produces: `TransportConfig`, `RetryConfig`, `ConcurrencyConfig`, `PacingConfig`, `BudgetConfig`, `DigestConfig`, `LoggingConfig`, and `usd_to_nano_usd(value: str) -> int`. `MainConfig` gains fields `transport: TransportConfig`, `retry: RetryConfig`, `concurrency: ConcurrencyConfig`, `pacing: PacingConfig`, `budget: BudgetConfig`, `digest: DigestConfig`, `logging: LoggingConfig`.

- [ ] **Step 1: Create the test package and its offline guard**

Create `tests/run_engine/__init__.py` as an empty file.

Create `tests/run_engine/conftest.py`:

```python
from __future__ import annotations

import socket

import pytest


class BlockedNetwork(RuntimeError):
    pass


@pytest.fixture(autouse=True)
def deny_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default suite is offline; an accidental live request must fail loudly."""

    def blocked(*args: object, **kwargs: object) -> None:
        raise BlockedNetwork(
            "the default test suite is offline; inject a fake transport instead"
        )

    monkeypatch.setattr(socket, "socket", blocked)
    monkeypatch.setattr(socket, "create_connection", blocked)
    monkeypatch.setattr(socket, "getaddrinfo", blocked)
```

- [ ] **Step 2: Write the failing configuration tests**

Create `tests/run_engine/helpers.py`:

```python
from pathlib import Path

OPERATIONAL_SECTIONS = """\
[transport]
contact_url = "https://example.com/contact"

[retry]
max_attempts = 3

[concurrency]
http_workers = 4

[budget]
openrouter_usd_per_run = "2.50"
"""


def write_graph(root: Path, *, operational: str = OPERATIONAL_SECTIONS) -> Path:
    config_file = root / "notable.toml"
    config_file.write_text(
        """\
schema_version = 1
timezone = "Europe/Paris"
feeds_file = "feeds.toml"
domain_profile_file = "profiles/art.toml"

[paths]
root = "portable"

[secrets]
openrouter_api_key = "TEST_OPENROUTER"
brave_api_key = "TEST_BRAVE"
"""
        + operational,
        encoding="utf-8",
    )
    (root / "profiles").mkdir()
    (root / "feeds.toml").write_text(
        """\
schema_version = 1
[[feeds]]
key = "art-news"
label = "Art News"
url = "https://example.com/feed.xml"
""",
        encoding="utf-8",
    )
    (root / "profiles" / "art.toml").write_text(
        """\
schema_version = 1
key = "visual-arts-en"
label = "English visual arts"
language = "en"
[attention_examples]
significant_recognition = ["major art prize"]
""",
        encoding="utf-8",
    )
    return config_file


ENVIRONMENT = {"TEST_OPENROUTER": "or-secret-value", "TEST_BRAVE": "brave-secret-value"}
```

Create `tests/run_engine/test_operational_config.py`:

```python
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from notable_person_finder.config.loader import ConfigLoadError, load_config
from notable_person_finder.config.models import (
    BudgetConfig,
    ConcurrencyConfig,
    MainConfig,
    RetryConfig,
    TransportConfig,
    usd_to_nano_usd,
)
from tests.run_engine.helpers import ENVIRONMENT, write_graph


def test_operational_sections_have_conservative_defaults() -> None:
    transport = TransportConfig()
    assert transport.connect_timeout_seconds == 10.0
    assert transport.read_timeout_seconds == 30.0
    assert transport.llm_read_timeout_seconds == 300.0
    assert transport.max_redirects == 5
    assert transport.max_api_response_bytes == 5 * 1024 * 1024
    assert transport.max_article_response_bytes == 10 * 1024 * 1024

    retry = RetryConfig()
    assert retry.max_attempts == 3
    assert retry.provider_pause_after_consecutive_exhaustions == 3

    concurrency = ConcurrencyConfig()
    assert (concurrency.http_workers, concurrency.llm_workers, concurrency.per_origin) == (4, 2, 2)


def test_default_user_agent_identifies_the_application() -> None:
    agent = TransportConfig().resolved_user_agent("0.1.0")
    assert agent.startswith("notable-person-finder/0.1.0 (+https://github.com/")
    assert "bot" not in agent.lower()


def test_contact_url_is_appended_and_override_replaces_completely() -> None:
    appended = TransportConfig(contact_url="https://example.com/contact").resolved_user_agent("0.1.0")
    assert appended.endswith("; https://example.com/contact)")

    replaced = TransportConfig(user_agent_override="custom-agent/9").resolved_user_agent("0.1.0")
    assert replaced == "custom-agent/9"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("connect_timeout_seconds", 0.0),
        ("read_timeout_seconds", -1.0),
        ("max_redirects", 0),
        ("max_api_response_bytes", 0),
    ],
)
def test_transport_bounds_must_be_positive(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        TransportConfig(**{field: value})


def test_unknown_operational_field_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TransportConfig(max_redirect=5)


def test_budget_is_optional_and_parsed_as_exact_nano_usd() -> None:
    assert BudgetConfig().openrouter_nano_usd_per_run() is None
    assert BudgetConfig(openrouter_usd_per_run="2.50").openrouter_nano_usd_per_run() == 2_500_000_000


@pytest.mark.parametrize("value", ["-1.00", "1.0000000001", "abc", "1e3", "NaN", ""])
def test_invalid_budget_syntax_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        BudgetConfig(openrouter_usd_per_run=value)


def test_usd_to_nano_usd_is_exact() -> None:
    assert usd_to_nano_usd("0.000000001") == 1
    assert usd_to_nano_usd("10") == 10_000_000_000


def test_loaded_graph_exposes_operational_settings(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert loaded.main.transport.contact_url == "https://example.com/contact"
    assert loaded.main.budget.openrouter_nano_usd_per_run() == 2_500_000_000
    assert loaded.main.pacing.mediawiki_min_interval_ms == 900


def test_snapshot_records_operational_bounds_but_no_secret_value(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert '"max_redirects":5' in loaded.snapshot_json
    assert "or-secret-value" not in loaded.snapshot_json
    assert "brave-secret-value" not in loaded.snapshot_json


def test_invalid_operational_bound_fails_before_any_run(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path, operational="[concurrency]\nhttp_workers = 0\n")
    with pytest.raises(ConfigLoadError) as raised:
        load_config(config_file, environ=ENVIRONMENT)
    assert any("concurrency.http_workers" in error for error in raised.value.errors)


def test_main_config_still_validates_without_operational_sections(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path, operational="")
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert isinstance(loaded.main, MainConfig)
    assert loaded.main.budget.openrouter_nano_usd_per_run() is None
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_operational_config.py -v`
Expected: FAIL with `ImportError: cannot import name 'BudgetConfig' from 'notable_person_finder.config.models'`

- [ ] **Step 4: Add the operational models**

Append to `src/notable_person_finder/config/models.py`, and add `Decimal`/`InvalidOperation` to the imports at the top (`from decimal import Decimal, InvalidOperation`):

```python
NANO_USD = 1_000_000_000
_USD_PATTERN = re.compile(r"^\d{1,9}(\.\d{1,9})?$")

DEFAULT_USER_AGENT_URL = "https://github.com/jonathandeamer/notable-person-finder"


def usd_to_nano_usd(value: str) -> int:
    """Convert a decimal USD string to exact integer nano-USD."""
    if _USD_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "must be a non-negative decimal USD amount with at most 9 decimal places"
        )
    try:
        amount = Decimal(value)
    except InvalidOperation as error:  # pragma: no cover - guarded by the pattern
        raise ValueError("must be a decimal USD amount") from error
    return int(amount * NANO_USD)


class TransportConfig(StrictModel):
    contact_url: str | None = None
    user_agent_override: str | None = Field(default=None, min_length=1, max_length=200)
    connect_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    read_timeout_seconds: float = Field(default=30.0, gt=0, le=600)
    llm_read_timeout_seconds: float = Field(default=300.0, gt=0, le=1800)
    max_redirects: int = Field(default=5, ge=1, le=10)
    max_api_response_bytes: int = Field(default=5 * 1024 * 1024, gt=0)
    max_article_response_bytes: int = Field(default=10 * 1024 * 1024, gt=0)

    @field_validator("contact_url")
    @classmethod
    def public_contact_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_public_http_url(value)

    def resolved_user_agent(self, version: str) -> str:
        if self.user_agent_override is not None:
            return self.user_agent_override
        contact = f"; {self.contact_url}" if self.contact_url else ""
        return f"notable-person-finder/{version} (+{DEFAULT_USER_AGENT_URL}{contact})"


class RetryConfig(StrictModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_backoff_seconds: float = Field(default=1.0, gt=0, le=60)
    max_backoff_seconds: float = Field(default=30.0, gt=0, le=300)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    jitter_ratio: float = Field(default=0.25, ge=0.0, le=1.0)
    provider_pause_after_consecutive_exhaustions: int = Field(default=3, ge=1, le=50)

    @model_validator(mode="after")
    def backoff_is_ordered(self) -> RetryConfig:
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        return self


class ConcurrencyConfig(StrictModel):
    http_workers: int = Field(default=4, ge=1, le=32)
    llm_workers: int = Field(default=2, ge=1, le=16)
    per_origin: int = Field(default=2, ge=1, le=16)


class PacingConfig(StrictModel):
    mediawiki_min_interval_ms: int = Field(default=900, ge=0, le=60_000)
    brave_min_interval_ms: int = Field(default=1100, ge=0, le=60_000)


class BudgetConfig(StrictModel):
    openrouter_usd_per_run: str | None = None

    @field_validator("openrouter_usd_per_run")
    @classmethod
    def decimal_usd(cls, value: str | None) -> str | None:
        if value is None:
            return None
        usd_to_nano_usd(value)
        return value

    def openrouter_nano_usd_per_run(self) -> int | None:
        if self.openrouter_usd_per_run is None:
            return None
        return usd_to_nano_usd(self.openrouter_usd_per_run)


class DigestConfig(StrictModel):
    write_latest_copy: bool = True


class LoggingConfig(StrictModel):
    max_bytes: int = Field(default=5 * 1024 * 1024, gt=0)
    backup_count: int = Field(default=5, ge=0, le=100)
```

Then extend `MainConfig` with the new fields (add them after `secrets`):

```python
    transport: TransportConfig = TransportConfig()
    retry: RetryConfig = RetryConfig()
    concurrency: ConcurrencyConfig = ConcurrencyConfig()
    pacing: PacingConfig = PacingConfig()
    budget: BudgetConfig = BudgetConfig()
    digest: DigestConfig = DigestConfig()
    logging: LoggingConfig = LoggingConfig()
```

`TransportConfig`, `RetryConfig`, and the rest are defined below `MainConfig` in the file, so move the new classes **above** the `MainConfig` definition rather than appending them at the end.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_operational_config.py -v`
Expected: PASS

- [ ] **Step 6: Confirm the foundation suite still passes**

Run: `uv run pytest tests/foundation -v`
Expected: PASS. The foundation graph omits every new section, so defaults apply. If a foundation test asserts an exact snapshot body, update that assertion — the snapshot legitimately gained operational bounds.

- [ ] **Step 7: Document the sections in the tracked example**

Append to `config/notable.example.toml`:

```toml

[transport]
# Appended to the default user agent so operators can be contacted.
contact_url = "https://example.com/contact"
connect_timeout_seconds = 10.0
read_timeout_seconds = 30.0
llm_read_timeout_seconds = 300.0
max_redirects = 5
max_api_response_bytes = 5242880
max_article_response_bytes = 10485760

[retry]
max_attempts = 3
initial_backoff_seconds = 1.0
max_backoff_seconds = 30.0
backoff_multiplier = 2.0
jitter_ratio = 0.25
provider_pause_after_consecutive_exhaustions = 3

[concurrency]
http_workers = 4
llm_workers = 2
per_origin = 2

[pacing]
mediawiki_min_interval_ms = 900
brave_min_interval_ms = 1100

[budget]
# Optional hard per-run OpenRouter cap as a decimal USD string. Omit for no cap.
# openrouter_usd_per_run = "2.50"

[digest]
write_latest_copy = true

[logging]
max_bytes = 5242880
backup_count = 5
```

- [ ] **Step 8: Commit**

```bash
git add src/notable_person_finder/config/models.py config/notable.example.toml tests/run_engine
git commit -m "feat(config): add transport, retry, concurrency, pacing, budget, digest, and logging settings"
```

---

## Task 2: Operations Schema Migration

Migration `0002` creates the four operations tables and nothing else. The partial unique index is the mechanism that enforces "at most one active work item per task type and fingerprint" — SQLite evaluates the `WHERE` clause per row, so completed and superseded rows never collide.

**Files:**
- Create: `src/notable_person_finder/db/migrations/0002_run_engine.sql`
- Test: `tests/run_engine/test_run_engine_schema.py`

**Interfaces:**
- Consumes: `connect_database`, `apply_migrations` from the foundation.
- Produces: tables `run`, `run_transition`, `work_item`, `attempt` with the column names every later task's SQL depends on.

- [ ] **Step 1: Write the failing schema tests**

Create `tests/run_engine/test_run_engine_schema.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


def snapshot_id(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, '{}', '2026-07-25T00:00:00Z')
        """,
        ("a" * 64,),
    )
    return int(cursor.lastrowid)


def start_run(connection: sqlite3.Connection) -> int:
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at
        )
        VALUES ('running', ?, 'Europe/Paris',
                '2026-07-24T00:00:00Z', '2026-07-25T00:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (snapshot_id(connection),),
    )
    return int(cursor.lastrowid)


def add_work(connection: sqlite3.Connection, *, state: str, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1,
                100, '2026-07-25T06:00:00Z', ?, '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (fingerprint, state),
    )
    return int(cursor.lastrowid)


def test_migration_0002_is_applied(connection: sqlite3.Connection) -> None:
    versions = {
        row["version"] for row in connection.execute("SELECT version FROM schema_migration")
    }
    assert {1, 2} <= versions


def test_operations_tables_exist(connection: sqlite3.Connection) -> None:
    names = {
        row["name"]
        for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }
    assert {"run", "run_transition", "work_item", "attempt"} <= names


def test_run_state_vocabulary_is_constrained(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE run SET state = 'succeeded' WHERE id = ?", (run_id,))


def test_running_run_must_not_have_a_finish_time(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE run SET finished_at = '2026-07-25T07:00:00Z' WHERE id = ?", (run_id,)
        )


def test_terminal_run_requires_a_finish_time(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE run SET state = 'complete' WHERE id = ?", (run_id,))
    connection.execute(
        "UPDATE run SET state = 'complete', finished_at = '2026-07-25T07:00:00Z' WHERE id = ?",
        (run_id,),
    )


def test_one_active_work_item_per_task_and_fingerprint(connection: sqlite3.Connection) -> None:
    add_work(connection, state="pending", fingerprint="b" * 64)
    with pytest.raises(sqlite3.IntegrityError):
        add_work(connection, state="running", fingerprint="b" * 64)


def test_completed_work_does_not_block_a_new_active_item(connection: sqlite3.Connection) -> None:
    first = add_work(connection, state="pending", fingerprint="c" * 64)
    connection.execute("UPDATE work_item SET state = 'superseded' WHERE id = ?", (first,))
    add_work(connection, state="pending", fingerprint="c" * 64)


def test_attempt_ordinal_is_unique_within_a_work_item(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="d" * 64)
    for ordinal in (1, 2):
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, request_fingerprint
            )
            VALUES (?, ?, 'mediawiki', 'search_pages', ?, '2026-07-25T06:00:00Z', ?)
            """,
            (run_id, work_id, ordinal, "e" * 64),
        )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, request_fingerprint
            )
            VALUES (?, ?, 'mediawiki', 'search_pages', 1, '2026-07-25T06:00:00Z', ?)
            """,
            (run_id, work_id, "e" * 64),
        )


def test_failed_attempt_requires_a_failure_category(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="f" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint
        )
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (run_id, work_id, "0" * 64),
    )
    attempt_id = int(cursor.lastrowid)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE attempt SET outcome = 'failed', finished_at = '2026-07-25T06:00:01Z' WHERE id = ?",
            (attempt_id,),
        )
    connection.execute(
        """
        UPDATE attempt
           SET outcome = 'failed',
               failure_category = 'rate_limit',
               finished_at = '2026-07-25T06:00:01Z'
         WHERE id = ?
        """,
        (attempt_id,),
    )


def test_finished_attempt_requires_an_outcome(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    work_id = add_work(connection, state="running", fingerprint="2" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint
        )
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (run_id, work_id, "3" * 64),
    )
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE attempt SET finished_at = '2026-07-25T06:00:01Z' WHERE id = ?",
            (int(cursor.lastrowid),),
        )


def test_money_columns_reject_negative_values(connection: sqlite3.Connection) -> None:
    run_id = start_run(connection)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE run SET budget_reserved_nano_usd = -1 WHERE id = ?", (run_id,)
        )


def test_work_item_requires_an_existing_run_reference(connection: sqlite3.Connection) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_at, updated_at, created_by_run_id
            )
            VALUES ('detect_people', 'source_item', 1, ?, 1, 100,
                    '2026-07-25T06:00:00Z', 'pending',
                    '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z', 9999)
            """,
            ("1" * 64,),
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_run_engine_schema.py -v`
Expected: FAIL with `sqlite3.OperationalError: no such table: run`

- [ ] **Step 3: Write the migration**

Create `src/notable_person_finder/db/migrations/0002_run_engine.sql`:

```sql
CREATE TABLE run (
    id INTEGER PRIMARY KEY,
    state TEXT NOT NULL CHECK (
        state IN ('running', 'complete', 'partial', 'failed', 'interrupted')
    ),
    configuration_snapshot_id INTEGER NOT NULL
        REFERENCES configuration_snapshot(id),
    timezone TEXT NOT NULL,
    window_start TEXT NOT NULL CHECK (window_start GLOB '*Z'),
    window_end TEXT NOT NULL CHECK (window_end GLOB '*Z'),
    started_at TEXT NOT NULL CHECK (started_at GLOB '*Z'),
    finished_at TEXT CHECK (finished_at IS NULL OR finished_at GLOB '*Z'),
    budget_limit_nano_usd INTEGER
        CHECK (budget_limit_nano_usd IS NULL OR budget_limit_nano_usd >= 0),
    budget_reserved_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (budget_reserved_nano_usd >= 0),
    budget_actual_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (budget_actual_nano_usd >= 0),
    digest_path TEXT,
    digest_sha256 TEXT
        CHECK (digest_sha256 IS NULL OR length(digest_sha256) = 64),
    -- A run is running exactly while it has no finish time. This makes the
    -- interrupted-run sweep a pure state query rather than a heuristic.
    CHECK ((state = 'running') = (finished_at IS NULL))
);

CREATE TABLE run_transition (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    state TEXT NOT NULL CHECK (
        state IN ('running', 'complete', 'partial', 'failed', 'interrupted')
    ),
    reason TEXT,
    occurred_at TEXT NOT NULL CHECK (occurred_at GLOB '*Z')
);

CREATE INDEX run_transition_by_run ON run_transition(run_id, id);

CREATE TABLE work_item (
    id INTEGER PRIMARY KEY,
    task_type TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id INTEGER,
    fingerprint TEXT NOT NULL CHECK (length(fingerprint) = 64),
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    priority INTEGER NOT NULL,
    eligible_at TEXT NOT NULL CHECK (eligible_at GLOB '*Z'),
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'running', 'succeeded',
            'deferred', 'failed_permanent', 'superseded'
        )
    ),
    reason TEXT,
    created_by_run_id INTEGER REFERENCES run(id),
    claimed_by_run_id INTEGER REFERENCES run(id),
    completed_by_run_id INTEGER REFERENCES run(id),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    updated_at TEXT NOT NULL CHECK (updated_at GLOB '*Z')
);

-- At most one active work item may exist for a task type and fingerprint.
-- Terminal rows are excluded by the partial predicate, so history never
-- blocks legitimate rescheduling after an input changes.
CREATE UNIQUE INDEX work_item_active_identity
    ON work_item(task_type, fingerprint)
    WHERE state IN ('pending', 'running', 'deferred');

CREATE INDEX work_item_eligible
    ON work_item(state, eligible_at, priority, id);

CREATE TABLE attempt (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    work_item_id INTEGER NOT NULL REFERENCES work_item(id),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    started_at TEXT NOT NULL CHECK (started_at GLOB '*Z'),
    finished_at TEXT CHECK (finished_at IS NULL OR finished_at GLOB '*Z'),
    -- NULL while the external call is in flight; the startup sweep marks
    -- abandoned rows 'interrupted'.
    outcome TEXT CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'failed', 'interrupted')
    ),
    failure_category TEXT,
    provider_status INTEGER,
    retry_after_ms INTEGER CHECK (retry_after_ms IS NULL OR retry_after_ms >= 0),
    request_fingerprint TEXT NOT NULL CHECK (length(request_fingerprint) = 64),
    destination_host TEXT,
    response_bytes INTEGER CHECK (response_bytes IS NULL OR response_bytes >= 0),
    latency_ms INTEGER CHECK (latency_ms IS NULL OR latency_ms >= 0),
    reserved_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (reserved_nano_usd >= 0),
    actual_nano_usd INTEGER
        CHECK (actual_nano_usd IS NULL OR actual_nano_usd >= 0),
    provider_request_id TEXT,
    detail_json TEXT,
    UNIQUE (work_item_id, ordinal),
    CHECK ((outcome IS NULL) = (finished_at IS NULL)),
    CHECK (
        CASE
            WHEN outcome = 'failed' THEN failure_category IS NOT NULL
            ELSE failure_category IS NULL
        END
    )
);

CREATE INDEX attempt_by_run ON attempt(run_id, id);
CREATE INDEX attempt_by_work_item ON attempt(work_item_id, ordinal);
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_run_engine_schema.py -v`
Expected: PASS

- [ ] **Step 5: Confirm the migration runner still validates checksums**

Run: `uv run pytest tests/foundation/test_migrations.py -v`
Expected: PASS. Migration `0001` is unchanged, so its recorded checksum still matches.

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/db/migrations/0002_run_engine.sql tests/run_engine/test_run_engine_schema.py
git commit -m "feat(db): add run, work item, and attempt operations schema"
```

---

## Task 3: Typed Failure Taxonomy

Every adapter written in milestones 3–6 raises `ProviderFailure`. Its category decides the default retry disposition, and nothing else in the application is allowed to inspect an `httpx` exception. Because "invalid structured model output receives at most one fresh attempt", retryability is a field set at raise time with the category supplying the default.

**Files:**
- Create: `src/notable_person_finder/providers/__init__.py`
- Create: `src/notable_person_finder/providers/failures.py`
- Test: `tests/run_engine/test_failures.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `FailureCategory` (str enum), `ProviderFailure`, `ProviderPaused`, `RETRYABLE_CATEGORIES`, and `parse_retry_after(value: str | None, *, now: datetime) -> int | None` returning milliseconds.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_failures.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from notable_person_finder.providers.failures import (
    RETRYABLE_CATEGORIES,
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
    parse_retry_after,
)

NOW = datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)


def test_transient_categories_are_retryable_by_default() -> None:
    for category in (
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    ):
        assert ProviderFailure(category, provider="brave", operation="search_web").retryable


def test_permanent_categories_are_not_retryable_by_default() -> None:
    for category in (
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.ACCESS_DENIED,
        FailureCategory.UNSUPPORTED_CONTENT,
        FailureCategory.RESPONSE_TOO_LARGE,
        FailureCategory.MALFORMED_RESPONSE,
        FailureCategory.BUDGET_EXHAUSTED,
        FailureCategory.STORAGE,
        FailureCategory.INTERNAL,
    ):
        assert not ProviderFailure(category, provider="brave", operation="search_web").retryable


def test_retryable_set_matches_the_default_dispositions() -> None:
    assert RETRYABLE_CATEGORIES == {
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    }


def test_malformed_response_can_be_marked_retryable_once_at_raise_time() -> None:
    failure = ProviderFailure(
        FailureCategory.MALFORMED_RESPONSE,
        provider="openrouter",
        operation="generate_structured",
        retryable=True,
    )
    assert failure.retryable


def test_failure_message_is_safe_and_structured() -> None:
    failure = ProviderFailure(
        FailureCategory.RATE_LIMIT,
        provider="brave",
        operation="search_web",
        status_code=429,
        detail="slow down",
    )
    assert str(failure) == "brave.search_web failed: rate_limit (HTTP 429): slow down"


def test_failure_detail_must_not_carry_a_secret_value() -> None:
    # Adapters pass sanitized text; this asserts the contract is documented in code.
    failure = ProviderFailure(
        FailureCategory.AUTHENTICATION,
        provider="brave",
        operation="search_web",
        status_code=401,
    )
    assert failure.detail is None
    assert "401" in str(failure)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("5", 5000),
        ("0", 0),
        ("-3", None),
        ("not-a-number", None),
        ("Fri, 25 Jul 2026 06:00:30 GMT", 30_000),
        ("Fri, 25 Jul 2026 05:59:30 GMT", 0),
    ],
)
def test_parse_retry_after(header: str | None, expected: int | None) -> None:
    assert parse_retry_after(header, now=NOW) == expected


def test_provider_paused_names_the_provider_and_reason() -> None:
    paused = ProviderPaused("mediawiki", consecutive_exhaustions=3)
    assert "mediawiki" in str(paused)
    assert paused.provider == "mediawiki"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_failures.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.providers'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/providers/__init__.py` as an empty file.

Create `src/notable_person_finder/providers/failures.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum


class FailureCategory(StrEnum):
    """The compact vocabulary every adapter translates its errors into."""

    NETWORK = "network"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    TRANSIENT_SERVER_ERROR = "transient_server_error"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    AUTHENTICATION = "authentication"
    CONFIGURATION = "configuration"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    ACCESS_DENIED = "access_denied"
    UNSUPPORTED_CONTENT = "unsupported_content"
    RESPONSE_TOO_LARGE = "response_too_large"
    MALFORMED_RESPONSE = "malformed_response"
    BUDGET_EXHAUSTED = "budget_exhausted"
    STORAGE = "storage"
    INTERNAL = "internal"


RETRYABLE_CATEGORIES = frozenset(
    {
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    }
)


class ProviderFailure(Exception):
    """Operational breakage at a provider boundary.

    `detail` must already be sanitized by the raising adapter: it never
    contains credentials, authorization headers, response bodies, or full
    query text.
    """

    def __init__(
        self,
        category: FailureCategory,
        *,
        provider: str,
        operation: str,
        retryable: bool | None = None,
        status_code: int | None = None,
        retry_after_ms: int | None = None,
        detail: str | None = None,
    ) -> None:
        self.category = category
        self.provider = provider
        self.operation = operation
        self.retryable = (
            category in RETRYABLE_CATEGORIES if retryable is None else retryable
        )
        self.status_code = status_code
        self.retry_after_ms = retry_after_ms
        self.detail = detail
        status = f" (HTTP {status_code})" if status_code is not None else ""
        suffix = f": {detail}" if detail else ""
        super().__init__(f"{provider}.{operation} failed: {category}{status}{suffix}")


class ProviderPaused(Exception):
    """Raised when a provider is paused for the rest of the run."""

    def __init__(self, provider: str, *, consecutive_exhaustions: int) -> None:
        self.provider = provider
        self.consecutive_exhaustions = consecutive_exhaustions
        super().__init__(
            f"{provider} is paused for this run after "
            f"{consecutive_exhaustions} consecutive exhausted failures"
        )


def parse_retry_after(value: str | None, *, now: datetime) -> int | None:
    """Translate a `Retry-After` header into milliseconds, or None if unusable."""
    if not value:
        return None
    text = value.strip()
    if text.lstrip("+").isdigit():
        return int(text) * 1000
    try:
        target = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    delta_ms = int((target - now).total_seconds() * 1000)
    return max(delta_ms, 0)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_failures.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/providers tests/run_engine/test_failures.py
git commit -m "feat(providers): add typed provider failure taxonomy"
```

---

## Task 4: Injectable Clock

Retry backoff, pacing intervals, and run timestamps must be deterministic in tests. One tiny module supplies both the monotonic reading used for pacing and the wall-clock timestamp written to SQLite, so no test sleeps in real time.

**Files:**
- Create: `src/notable_person_finder/runs/clock.py`
- Test: `tests/run_engine/test_clock.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `Clock` protocol with `monotonic() -> float`, `sleep(seconds: float) -> None`, `now() -> datetime`; `SystemClock`; `FakeClock` (test double exported from the production module so every later task uses one implementation); `utc_timestamp(moment: datetime) -> str`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_clock.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime

from notable_person_finder.runs.clock import FakeClock, SystemClock, utc_timestamp


def test_utc_timestamp_is_normalized_iso_8601_zulu() -> None:
    moment = datetime(2026, 7, 25, 6, 0, 0, 123456, tzinfo=UTC)
    assert utc_timestamp(moment) == "2026-07-25T06:00:00.123456Z"


def test_system_clock_now_is_timezone_aware_utc() -> None:
    assert SystemClock().now().tzinfo == UTC


def test_fake_clock_sleep_advances_without_real_delay() -> None:
    clock = FakeClock(start=datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC))
    assert clock.monotonic() == 0.0
    clock.sleep(2.5)
    assert clock.monotonic() == 2.5
    assert clock.now() == datetime(2026, 7, 25, 6, 0, 2, 500000, tzinfo=UTC)
    assert clock.slept == [2.5]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_clock.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.clock'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/runs/clock.py`:

```python
from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def now(self) -> datetime: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)

    def now(self) -> datetime:
        return datetime.now(UTC)


class FakeClock:
    """Deterministic clock for tests; no test in this suite sleeps for real."""

    def __init__(self, start: datetime | None = None) -> None:
        self._start = start or datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)
        self._elapsed = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self._elapsed

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self._elapsed += seconds

    def advance(self, seconds: float) -> None:
        self._elapsed += seconds

    def now(self) -> datetime:
        return self._start + timedelta(seconds=self._elapsed)


def utc_timestamp(moment: datetime) -> str:
    """Render an aware datetime as the ISO-8601 Zulu text SQLite stores."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_clock.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/clock.py tests/run_engine/test_clock.py
git commit -m "feat(runs): add injectable clock for deterministic pacing and timestamps"
```

---

## Task 5: URL, DNS, and Redirect Safety

The foundation's `validate_public_http_url` is syntactic and deliberately does not resolve hostnames, because configuration validation must stay offline. Request-time safety is different: it resolves the destination and rejects non-public addresses **before the initial request and before following every redirect**. This is the approved proportionate preflight for a personal single-machine tool. Because HTTPX performs its own resolution when connecting, this check reduces exposure but does not pin the connection to the inspected address and must not be described as complete DNS-rebinding protection.

**Files:**
- Create: `src/notable_person_finder/providers/safety.py`
- Test: `tests/run_engine/test_safety.py`

**Interfaces:**
- Consumes: `validate_public_http_url` from `notable_person_finder.config.models`; `FailureCategory`, `ProviderFailure` from `providers.failures`.
- Produces: `HostResolver` protocol with `resolve(host: str) -> tuple[str, ...]`; `SystemHostResolver`; `StaticHostResolver(mapping)` for tests; `UnsafeUrl` exception; `assert_safe_url(url: str, *, resolver: HostResolver) -> str` returning the resolved host.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_safety.py`:

```python
from __future__ import annotations

import pytest

from notable_person_finder.providers.safety import (
    StaticHostResolver,
    UnsafeUrl,
    assert_safe_url,
)

PUBLIC = StaticHostResolver({"example.com": ("93.184.216.34",)})


def test_public_https_url_is_accepted() -> None:
    assert assert_safe_url("https://example.com/feed.xml", resolver=PUBLIC) == "example.com"


def test_public_http_url_is_accepted() -> None:
    assert assert_safe_url("http://example.com/feed.xml", resolver=PUBLIC) == "example.com"


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/x",
        "file:///etc/passwd",
        "gopher://example.com",
        "//example.com/x",
        "https:///nohost",
    ],
)
def test_non_http_schemes_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeUrl):
        assert_safe_url(url, resolver=PUBLIC)


def test_embedded_credentials_are_rejected() -> None:
    with pytest.raises(UnsafeUrl, match="credentials"):
        assert_safe_url("https://user:pass@example.com/x", resolver=PUBLIC)


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",       # loopback
        "10.0.0.5",        # private
        "192.168.1.10",    # private
        "169.254.169.254", # link-local metadata endpoint
        "224.0.0.1",       # multicast
        "0.0.0.0",         # reserved
        "::1",             # IPv6 loopback
        "fd00::1",         # IPv6 unique local
    ],
)
def test_non_public_resolved_addresses_are_rejected(address: str) -> None:
    resolver = StaticHostResolver({"rebind.example": (address,)})
    with pytest.raises(UnsafeUrl, match="not publicly routable"):
        assert_safe_url("https://rebind.example/x", resolver=resolver)


def test_a_single_private_answer_rejects_the_whole_host() -> None:
    # A host that resolves to both a public and a private address must be
    # refused: choosing the public answer is not something HTTPX guarantees.
    resolver = StaticHostResolver({"mixed.example": ("93.184.216.34", "127.0.0.1")})
    with pytest.raises(UnsafeUrl):
        assert_safe_url("https://mixed.example/x", resolver=resolver)


def test_literal_private_address_is_rejected_without_resolution() -> None:
    resolver = StaticHostResolver({})
    with pytest.raises(UnsafeUrl):
        assert_safe_url("https://127.0.0.1/x", resolver=resolver)


def test_unresolvable_host_is_rejected() -> None:
    with pytest.raises(UnsafeUrl, match="could not be resolved"):
        assert_safe_url("https://missing.example/x", resolver=StaticHostResolver({}))


def test_ordinary_public_ports_are_permitted() -> None:
    assert assert_safe_url("https://example.com:8443/x", resolver=PUBLIC) == "example.com"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_safety.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.providers.safety'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/providers/safety.py`:

```python
from __future__ import annotations

import ipaddress
import socket
from collections.abc import Mapping
from typing import Protocol
from urllib.parse import urlsplit


class UnsafeUrl(ValueError):
    """A URL or redirect destination the application refuses to request."""


class HostResolver(Protocol):
    def resolve(self, host: str) -> tuple[str, ...]: ...


class SystemHostResolver:
    def resolve(self, host: str) -> tuple[str, ...]:
        try:
            answers = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
        except OSError:
            return ()
        return tuple(dict.fromkeys(answer[4][0] for answer in answers))


class StaticHostResolver:
    """Deterministic resolver for tests; the default suite never uses DNS."""

    def __init__(self, mapping: Mapping[str, tuple[str, ...]]) -> None:
        self._mapping = dict(mapping)

    def resolve(self, host: str) -> tuple[str, ...]:
        return self._mapping.get(host.rstrip(".").lower(), ())


def _is_public_address(address: str) -> bool:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False
    return parsed.is_global


def assert_safe_url(url: str, *, resolver: HostResolver) -> str:
    """Validate a request or redirect destination, returning its hostname.

    Applied before the initial request and again before every redirect, so a
    server cannot redirect the application onto a loopback, private, or
    link-local address.
    """
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeUrl(f"{url}: only http and https URLs are requested")
    if not parsed.hostname:
        raise UnsafeUrl(f"{url}: URL has no host")
    if parsed.username is not None or parsed.password is not None:
        raise UnsafeUrl(f"{url}: URL must not contain embedded credentials")

    host = parsed.hostname.rstrip(".").lower()
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None

    if literal is not None:
        if not literal.is_global:
            raise UnsafeUrl(f"{url}: address is not publicly routable")
        return host

    addresses = resolver.resolve(host)
    if not addresses:
        raise UnsafeUrl(f"{url}: host could not be resolved")
    for address in addresses:
        if not _is_public_address(address):
            raise UnsafeUrl(f"{url}: host resolves to an address that is not publicly routable")
    return host
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_safety.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/providers/safety.py tests/run_engine/test_safety.py
git commit -m "feat(providers): reject unsafe URLs and redirect destinations"
```

---

## Task 6: Shared HTTP Transport

One run-scoped transport owns connection reuse, the two timeout profiles, the user agent, manual redirect following, streamed response bounds, and translation of HTTPX and URL-safety errors into `ProviderFailure`.

Redirects are followed **manually** (`follow_redirects=False`) because each hop must pass `assert_safe_url` before it is requested. Bodies use `iter_raw()` plus a bounded incremental gzip/deflate decoder. Both encoded input and decoded output are capped while streaming, so decompression never materializes an unbounded decoded chunk before the limit check.

**Files:**
- Modify: `pyproject.toml`
- Create: `src/notable_person_finder/providers/transport.py`
- Test: `tests/run_engine/test_transport.py`

**Interfaces:**
- Consumes: `TransportConfig`; `assert_safe_url`, `HostResolver`, `UnsafeUrl`; `FailureCategory`, `ProviderFailure`, `parse_retry_after`; `Clock`.
- Produces: `ResponseLimit` (str enum: `API`, `ARTICLE`), `HttpResponse` frozen dataclass, `HttpTransport` with `request(...) -> HttpResponse` and `close()`, and `build_transport(config, *, version, resolver, clock) -> HttpTransport`.

- [ ] **Step 1: Add the dependency**

Add to `pyproject.toml` `dependencies`:

```toml
  "httpx>=0.28,<0.29",
```

Run: `uv sync` then `git add pyproject.toml uv.lock`

- [ ] **Step 2: Write the failing tests**

Create `tests/run_engine/test_transport.py`:

```python
from __future__ import annotations

import gzip

import httpx
import pytest

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.safety import StaticHostResolver
from notable_person_finder.providers.transport import (
    HttpTransport,
    ResponseLimit,
    build_transport,
)
from notable_person_finder.runs.clock import FakeClock

RESOLVER = StaticHostResolver(
    {
        "example.com": ("93.184.216.34",),
        "other.example": ("93.184.216.35",),
        "internal.example": ("127.0.0.1",),
    }
)


def transport_for(handler, config: TransportConfig | None = None) -> HttpTransport:
    """Build a transport whose network layer is an in-process handler."""
    return build_transport(
        config or TransportConfig(),
        version="0.1.0",
        resolver=RESOLVER,
        clock=FakeClock(),
        http_transport=httpx.MockTransport(handler),
    )


def test_successful_request_returns_decoded_body_and_final_url() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="hello", headers={"content-type": "text/plain"})

    with transport_for(handler) as transport:
        response = transport.request(
            "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
        )
    assert response.status_code == 200
    assert response.content == b"hello"
    assert response.requested_url == "https://example.com/a"
    assert response.final_url == "https://example.com/a"
    assert response.redirect_chain == ()
    assert response.destination_host == "example.com"


def test_user_agent_is_sent_on_every_request() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers["user-agent"])
        return httpx.Response(200, text="ok")

    with transport_for(handler) as transport:
        transport.request("GET", "https://example.com/a", provider="feeds", operation="fetch_feed")
    assert seen == ["notable-person-finder/0.1.0 (+https://github.com/jonathandeamer/notable-person-finder)"]


def test_adapter_headers_are_merged_without_replacing_the_user_agent() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, text="ok")

    with transport_for(handler) as transport:
        transport.request(
            "GET",
            "https://example.com/a",
            provider="brave",
            operation="search_web",
            headers={
                "X-Subscription-Token": "secret",
                "User-Agent": "untrusted-override",
                "Accept-Encoding": "br",
            },
        )
    assert seen["x-subscription-token"] == "secret"
    assert seen["user-agent"].startswith("notable-person-finder/")
    assert seen["accept-encoding"] == "gzip, deflate"


def test_redirects_are_followed_and_the_chain_is_recorded() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/a":
            return httpx.Response(301, headers={"location": "https://other.example/b"})
        return httpx.Response(200, text="final")

    with transport_for(handler) as transport:
        response = transport.request(
            "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
        )
    assert response.redirect_chain == ("https://example.com/a",)
    assert response.final_url == "https://other.example/b"
    assert response.destination_host == "other.example"
    assert response.content == b"final"


def test_redirect_to_a_private_address_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://internal.example/secrets"})

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is FailureCategory.CONFIGURATION
    assert "internal.example" not in str(raised.value)


def test_redirect_limit_is_enforced() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": "https://example.com/loop"})

    with transport_for(handler, TransportConfig(max_redirects=2)) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is FailureCategory.MALFORMED_RESPONSE
    assert not raised.value.retryable


def test_decoded_body_over_the_limit_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 5000)

    config = TransportConfig(max_api_response_bytes=1000)
    with transport_for(handler, config) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET",
                "https://example.com/a",
                provider="feeds",
                operation="fetch_feed",
                limit=ResponseLimit.API,
            )
    assert raised.value.category is FailureCategory.RESPONSE_TOO_LARGE


def test_compression_cannot_smuggle_a_body_past_the_decoded_limit() -> None:
    payload = gzip.compress(b"x" * 200_000)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=payload, headers={"content-encoding": "gzip"}
        )

    config = TransportConfig(max_api_response_bytes=1000)
    with transport_for(handler, config) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET",
                "https://example.com/a",
                provider="feeds",
                operation="fetch_feed",
                limit=ResponseLimit.API,
            )
    assert raised.value.category is FailureCategory.RESPONSE_TOO_LARGE


def test_article_limit_is_larger_than_the_api_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 4000)

    config = TransportConfig(max_api_response_bytes=1000, max_article_response_bytes=8000)
    with transport_for(handler, config) as transport:
        response = transport.request(
            "GET",
            "https://example.com/a",
            provider="articles",
            operation="fetch_article",
            limit=ResponseLimit.ARTICLE,
        )
    assert response.decoded_bytes == 4000


@pytest.mark.parametrize(
    ("status", "category", "retryable"),
    [
        (401, FailureCategory.AUTHENTICATION, False),
        (403, FailureCategory.ACCESS_DENIED, False),
        (404, FailureCategory.ACCESS_DENIED, False),
        (408, FailureCategory.TIMEOUT, True),
        (429, FailureCategory.RATE_LIMIT, True),
        (500, FailureCategory.TRANSIENT_SERVER_ERROR, True),
        (503, FailureCategory.PROVIDER_UNAVAILABLE, True),
        (400, FailureCategory.CONFIGURATION, False),
    ],
)
def test_http_statuses_map_to_typed_failures(
    status: int, category: FailureCategory, retryable: bool
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="body text")

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="brave", operation="search_web"
            )
    assert raised.value.category is category
    assert raised.value.retryable is retryable
    assert raised.value.status_code == status
    assert "body text" not in str(raised.value)


def test_retry_after_is_captured_from_a_rate_limited_response() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"retry-after": "7"})

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="brave", operation="search_web"
            )
    assert raised.value.retry_after_ms == 7000


@pytest.mark.parametrize(
    ("error", "category"),
    [
        (httpx.ConnectTimeout("slow"), FailureCategory.TIMEOUT),
        (httpx.ReadTimeout("slow"), FailureCategory.TIMEOUT),
        (httpx.ConnectError("refused"), FailureCategory.NETWORK),
        (httpx.RemoteProtocolError("garbage"), FailureCategory.MALFORMED_RESPONSE),
    ],
)
def test_httpx_errors_are_translated(error: Exception, category: FailureCategory) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure) as raised:
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )
    assert raised.value.category is category


def test_no_httpx_exception_escapes_the_boundary() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ProxyError("proxy exploded")

    with transport_for(handler) as transport:
        with pytest.raises(ProviderFailure):
            transport.request(
                "GET", "https://example.com/a", provider="feeds", operation="fetch_feed"
            )


def test_llm_profile_uses_the_longer_read_timeout() -> None:
    config = TransportConfig(read_timeout_seconds=30.0, llm_read_timeout_seconds=300.0)
    with transport_for(lambda request: httpx.Response(200, text="ok"), config) as transport:
        assert transport.timeout_for(profile="llm").read == 300.0
        assert transport.timeout_for(profile="ordinary").read == 30.0
        assert transport.timeout_for(profile="ordinary").connect == 10.0


def test_client_level_automatic_retries_are_disabled() -> None:
    with transport_for(lambda request: httpx.Response(200, text="ok")) as transport:
        assert transport.automatic_retries_enabled() is False
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_transport.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.providers.transport'`

- [ ] **Step 4: Write the implementation**

Create `src/notable_person_finder/providers/transport.py`:

```python
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import TracebackType
from typing import Any, Literal
import zlib

import httpx

from notable_person_finder.config.models import TransportConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    parse_retry_after,
)
from notable_person_finder.providers.safety import HostResolver, UnsafeUrl, assert_safe_url
from notable_person_finder.runs.clock import Clock

_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})

_STATUS_CATEGORIES: tuple[tuple[range, FailureCategory], ...] = (
    (range(500, 600), FailureCategory.TRANSIENT_SERVER_ERROR),
    (range(400, 500), FailureCategory.CONFIGURATION),
)

_EXACT_STATUS_CATEGORIES: Mapping[int, FailureCategory] = {
    401: FailureCategory.AUTHENTICATION,
    402: FailureCategory.ACCESS_DENIED,
    403: FailureCategory.ACCESS_DENIED,
    404: FailureCategory.ACCESS_DENIED,
    408: FailureCategory.TIMEOUT,
    410: FailureCategory.ACCESS_DENIED,
    429: FailureCategory.RATE_LIMIT,
    451: FailureCategory.ACCESS_DENIED,
    502: FailureCategory.PROVIDER_UNAVAILABLE,
    503: FailureCategory.PROVIDER_UNAVAILABLE,
    504: FailureCategory.TIMEOUT,
}

_TRANSPORT_ERROR_CATEGORIES: tuple[tuple[type[Exception], FailureCategory], ...] = (
    (httpx.TimeoutException, FailureCategory.TIMEOUT),
    (httpx.RemoteProtocolError, FailureCategory.MALFORMED_RESPONSE),
    (httpx.DecodingError, FailureCategory.MALFORMED_RESPONSE),
    (httpx.TransportError, FailureCategory.NETWORK),
)


class ResponseLimit(StrEnum):
    API = "api"
    ARTICLE = "article"


@dataclass(frozen=True, slots=True)
class HttpResponse:
    requested_url: str
    final_url: str
    redirect_chain: tuple[str, ...]
    destination_host: str
    status_code: int
    headers: Mapping[str, str]
    content: bytes
    encoded_bytes: int
    decoded_bytes: int


def _decoder(content_encoding: str) -> Any | None:
    encoding = content_encoding.strip().lower()
    if encoding in {"", "identity"}:
        return None
    if encoding == "gzip":
        return zlib.decompressobj(16 + zlib.MAX_WBITS)
    if encoding == "deflate":
        return zlib.decompressobj()
    raise ValueError("unsupported content encoding")


def _bounded_body(response: httpx.Response, *, byte_limit: int) -> tuple[bytes, int]:
    """Read encoded bytes and incrementally cap decoded output."""
    try:
        decoder = _decoder(response.headers.get("content-encoding", ""))
    except ValueError as error:
        raise httpx.DecodingError(str(error), request=response.request) from error

    encoded_bytes = 0
    decoded = bytearray()
    for chunk in response.iter_raw():
        encoded_bytes += len(chunk)
        if encoded_bytes > byte_limit:
            raise OverflowError("encoded")
        if decoder is None:
            piece = chunk
        else:
            piece = decoder.decompress(chunk, byte_limit - len(decoded) + 1)
        decoded.extend(piece)
        if len(decoded) > byte_limit:
            raise OverflowError("decoded")

    if decoder is not None:
        decoded.extend(decoder.flush(byte_limit - len(decoded) + 1))
        if len(decoded) > byte_limit:
            raise OverflowError("decoded")
    return bytes(decoded), encoded_bytes


class HttpTransport:
    """Run-scoped synchronous HTTP boundary shared by every ordinary adapter."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        config: TransportConfig,
        user_agent: str,
        resolver: HostResolver,
        clock: Clock,
    ) -> None:
        self._client = client
        self._config = config
        self._user_agent = user_agent
        self._resolver = resolver
        self._clock = clock

    def __enter__(self) -> HttpTransport:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def automatic_retries_enabled(self) -> bool:
        """Only the central retry coordinator starts a repeat request."""
        return False

    def timeout_for(self, *, profile: Literal["ordinary", "llm"]) -> httpx.Timeout:
        read = (
            self._config.llm_read_timeout_seconds
            if profile == "llm"
            else self._config.read_timeout_seconds
        )
        return httpx.Timeout(
            read, connect=self._config.connect_timeout_seconds, pool=read
        )

    def _byte_limit(self, limit: ResponseLimit) -> int:
        if limit is ResponseLimit.ARTICLE:
            return self._config.max_article_response_bytes
        return self._config.max_api_response_bytes

    def request(
        self,
        method: str,
        url: str,
        *,
        provider: str,
        operation: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str | int] | None = None,
        limit: ResponseLimit = ResponseLimit.API,
        profile: Literal["ordinary", "llm"] = "ordinary",
    ) -> HttpResponse:
        requested_url = url
        current_url = url
        redirect_chain: list[str] = []
        byte_limit = self._byte_limit(limit)

        for _ in range(self._config.max_redirects + 1):
            try:
                host = assert_safe_url(current_url, resolver=self._resolver)
            except UnsafeUrl as error:
                raise ProviderFailure(
                    FailureCategory.CONFIGURATION,
                    provider=provider,
                    operation=operation,
                    detail="unsafe request destination",
                ) from error
            response = self._send(
                method,
                current_url,
                provider=provider,
                operation=operation,
                headers=headers,
                params=params if not redirect_chain else None,
                profile=profile,
                byte_limit=byte_limit,
                host=host,
                requested_url=requested_url,
                redirect_chain=tuple(redirect_chain),
            )
            if response is None:
                continue
            if isinstance(response, str):
                redirect_chain.append(current_url)
                current_url = response
                continue
            return response

        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider=provider,
            operation=operation,
            detail=f"exceeded {self._config.max_redirects} redirects",
        )

    def _send(
        self,
        method: str,
        url: str,
        *,
        provider: str,
        operation: str,
        headers: Mapping[str, str] | None,
        params: Mapping[str, str | int] | None,
        profile: Literal["ordinary", "llm"],
        byte_limit: int,
        host: str,
        requested_url: str,
        redirect_chain: tuple[str, ...],
    ) -> HttpResponse | str | None:
        merged = {
            "user-agent": self._user_agent,
            # Only encodings handled by the bounded incremental decoder.
            "accept-encoding": "gzip, deflate",
        }
        if headers:
            for name, value in headers.items():
                lowered = name.lower()
                if lowered in {"user-agent", "accept-encoding"}:
                    continue
                merged[lowered] = value

        request = self._client.build_request(
            method,
            url,
            headers=merged,
            params=params,
            timeout=self.timeout_for(profile=profile),
        )
        try:
            response = self._client.send(request, stream=True)
        except Exception as error:  # translated below; no httpx type escapes
            raise self._translate(error, provider=provider, operation=operation) from error

        try:
            if response.status_code in _REDIRECT_STATUSES:
                location = response.headers.get("location")
                if not location:
                    raise ProviderFailure(
                        FailureCategory.MALFORMED_RESPONSE,
                        provider=provider,
                        operation=operation,
                        detail=f"HTTP {response.status_code} without a Location header",
                    )
                return str(httpx.URL(url).join(location))

            if response.status_code >= 400:
                raise self._status_failure(response, provider=provider, operation=operation)

            try:
                content, encoded_bytes = _bounded_body(response, byte_limit=byte_limit)
            except OverflowError as error:
                raise ProviderFailure(
                    FailureCategory.RESPONSE_TOO_LARGE,
                    provider=provider,
                    operation=operation,
                    detail=f"{error.args[0]} body exceeded {byte_limit} bytes",
                ) from error

            return HttpResponse(
                requested_url=requested_url,
                final_url=url,
                redirect_chain=redirect_chain,
                destination_host=host,
                status_code=response.status_code,
                headers={
                    name.lower(): value
                    for name, value in response.headers.items()
                    if name.lower() not in {"authorization", "set-cookie", "cookie"}
                },
                content=content,
                encoded_bytes=encoded_bytes,
                decoded_bytes=len(content),
            )
        except ProviderFailure:
            raise
        except Exception as error:
            raise self._translate(error, provider=provider, operation=operation) from error
        finally:
            response.close()

    def _status_failure(
        self, response: httpx.Response, *, provider: str, operation: str
    ) -> ProviderFailure:
        status = response.status_code
        category = _EXACT_STATUS_CATEGORIES.get(status)
        if category is None:
            category = next(
                (mapped for span, mapped in _STATUS_CATEGORIES if status in span),
                FailureCategory.INTERNAL,
            )
        return ProviderFailure(
            category,
            provider=provider,
            operation=operation,
            status_code=status,
            retry_after_ms=parse_retry_after(
                response.headers.get("retry-after"), now=self._clock.now()
            ),
        )

    def _translate(
        self, error: Exception, *, provider: str, operation: str
    ) -> ProviderFailure:
        for error_type, category in _TRANSPORT_ERROR_CATEGORIES:
            if isinstance(error, error_type):
                return ProviderFailure(
                    category,
                    provider=provider,
                    operation=operation,
                    detail=type(error).__name__,
                )
        return ProviderFailure(
            FailureCategory.INTERNAL,
            provider=provider,
            operation=operation,
            detail=type(error).__name__,
        )


def build_transport(
    config: TransportConfig,
    *,
    version: str,
    resolver: HostResolver,
    clock: Clock,
    http_transport: httpx.BaseTransport | None = None,
) -> HttpTransport:
    client = httpx.Client(
        follow_redirects=False,  # each hop is validated by assert_safe_url first
        transport=http_transport,
        timeout=httpx.Timeout(
            config.read_timeout_seconds, connect=config.connect_timeout_seconds
        ),
    )
    return HttpTransport(
        client,
        config=config,
        user_agent=config.resolved_user_agent(version),
        resolver=resolver,
        clock=clock,
    )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_transport.py -v`
Expected: PASS

- [ ] **Step 6: Confirm no domain module imports httpx**

Run:

```bash
! grep -rn "import httpx" src/notable_person_finder --include='*.py' \
  | grep -v "src/notable_person_finder/providers/"
```

Expected: exit status 0 (no matches outside `providers/`).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock src/notable_person_finder/providers/transport.py tests/run_engine/test_transport.py
git commit -m "feat(providers): add bounded run-scoped HTTP transport"
```

---

## Task 7: Per-Origin Concurrency and Provider Pacing

MediaWiki and Brave request *starts* must be spaced by a configured interval, and no more than `per_origin` requests may be in flight against one host. Both gates use the injected clock, so tests assert the sleep amounts rather than waiting.

**Files:**
- Create: `src/notable_person_finder/providers/pacing.py`
- Test: `tests/run_engine/test_pacing.py`

**Interfaces:**
- Consumes: `PacingConfig`, `ConcurrencyConfig`, `Clock`.
- Produces: `PacingGate` with `acquire(provider: str, host: str)` as a context manager, and `build_pacing_gate(pacing, concurrency, *, clock) -> PacingGate`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_pacing.py`:

```python
from __future__ import annotations

import threading

import pytest

from notable_person_finder.config.models import ConcurrencyConfig, PacingConfig
from notable_person_finder.providers.pacing import build_pacing_gate
from notable_person_finder.runs.clock import FakeClock


def test_first_request_to_a_provider_does_not_sleep() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(PacingConfig(), ConcurrencyConfig(), clock=clock)
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == []


def test_second_request_waits_for_the_configured_interval() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900), ConcurrencyConfig(), clock=clock
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == [0.9]


def test_elapsed_time_is_credited_against_the_interval() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900), ConcurrencyConfig(), clock=clock
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    clock.advance(0.5)
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    assert clock.slept == [pytest.approx(0.4)]


def test_providers_are_paced_independently() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(
        PacingConfig(mediawiki_min_interval_ms=900, brave_min_interval_ms=1100),
        ConcurrencyConfig(),
        clock=clock,
    )
    with gate.acquire("mediawiki", "en.wikipedia.org"):
        pass
    with gate.acquire("brave", "api.search.brave.com"):
        pass
    assert clock.slept == []


def test_unpaced_providers_never_sleep() -> None:
    clock = FakeClock()
    gate = build_pacing_gate(PacingConfig(), ConcurrencyConfig(), clock=clock)
    for _ in range(5):
        with gate.acquire("feeds", "example.com"):
            pass
    assert clock.slept == []


def test_per_origin_concurrency_is_capped() -> None:
    gate = build_pacing_gate(
        PacingConfig(), ConcurrencyConfig(per_origin=2), clock=FakeClock()
    )
    in_flight = 0
    peak = 0
    guard = threading.Lock()
    release = threading.Event()

    def worker() -> None:
        nonlocal in_flight, peak
        with gate.acquire("feeds", "example.com"):
            with guard:
                in_flight += 1
                peak = max(peak, in_flight)
            release.wait(timeout=2)
            with guard:
                in_flight -= 1

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    release.set()
    for thread in threads:
        thread.join(timeout=5)
    assert peak <= 2


def test_different_origins_do_not_share_a_slot() -> None:
    gate = build_pacing_gate(
        PacingConfig(), ConcurrencyConfig(per_origin=1), clock=FakeClock()
    )
    with gate.acquire("feeds", "a.example"):
        with gate.acquire("feeds", "b.example"):
            pass
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_pacing.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.providers.pacing'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/providers/pacing.py`:

```python
from __future__ import annotations

import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager

from notable_person_finder.config.models import ConcurrencyConfig, PacingConfig
from notable_person_finder.runs.clock import Clock


class PacingGate:
    """Spaces provider request starts and caps concurrency per origin."""

    def __init__(
        self,
        *,
        intervals_ms: Mapping[str, int],
        per_origin: int,
        clock: Clock,
    ) -> None:
        self._intervals_ms = dict(intervals_ms)
        self._per_origin = per_origin
        self._clock = clock
        self._guard = threading.Lock()
        self._last_start: dict[str, float] = {}
        self._origin_slots: dict[str, threading.BoundedSemaphore] = {}

    def _semaphore(self, host: str) -> threading.BoundedSemaphore:
        with self._guard:
            slot = self._origin_slots.get(host)
            if slot is None:
                slot = threading.BoundedSemaphore(self._per_origin)
                self._origin_slots[host] = slot
            return slot

    def _wait_for_interval(self, provider: str) -> None:
        interval = self._intervals_ms.get(provider, 0) / 1000
        if interval <= 0:
            return
        with self._guard:
            previous = self._last_start.get(provider)
            now = self._clock.monotonic()
            delay = 0.0 if previous is None else interval - (now - previous)
            self._last_start[provider] = now + max(delay, 0.0)
        if delay > 0:
            self._clock.sleep(delay)

    @contextmanager
    def acquire(self, provider: str, host: str) -> Iterator[None]:
        slot = self._semaphore(host)
        slot.acquire()
        try:
            self._wait_for_interval(provider)
            yield
        finally:
            slot.release()


def build_pacing_gate(
    pacing: PacingConfig, concurrency: ConcurrencyConfig, *, clock: Clock
) -> PacingGate:
    return PacingGate(
        intervals_ms={
            "mediawiki": pacing.mediawiki_min_interval_ms,
            "brave": pacing.brave_min_interval_ms,
        },
        per_origin=concurrency.per_origin,
        clock=clock,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_pacing.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/providers/pacing.py tests/run_engine/test_pacing.py
git commit -m "feat(providers): pace provider requests and cap per-origin concurrency"
```

---

## Task 8: Retry Coordinator and Provider Pause

One application-owned policy decides every repeat request. It retries only transient categories, honours `Retry-After` over computed backoff, applies bounded exponential backoff with jitter otherwise, and after a configured number of consecutive exhausted failures pauses that provider for the rest of the run.

Every physical call gets its own ordinal, and the coordinator reports each one to a callback so the caller can persist an immutable attempt row. A success never overwrites its failed predecessor.

**Files:**
- Create: `src/notable_person_finder/runs/retry.py`
- Test: `tests/run_engine/test_retry.py`

**Interfaces:**
- Consumes: `RetryConfig`, `Clock`, `FailureCategory`, `ProviderFailure`, `ProviderPaused`.
- Produces: `AttemptRecord` frozen dataclass (`ordinal`, `outcome`, `failure_category`, `status_code`, `retry_after_ms`, `latency_ms`, `detail`); `RetryExhausted` exception carrying `last_failure` and `attempts`; `RetryCoordinator` with `call(provider, operation, action, *, on_attempt, starting_ordinal=1) -> T`, `is_paused(provider) -> bool`, and `paused_providers() -> frozenset[str]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_retry.py`:

```python
from __future__ import annotations

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
)
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.retry import (
    AttemptRecord,
    RetryCoordinator,
    RetryExhausted,
)

NO_JITTER = RetryConfig(
    max_attempts=3,
    initial_backoff_seconds=1.0,
    max_backoff_seconds=30.0,
    backoff_multiplier=2.0,
    jitter_ratio=0.0,
)


def coordinator(config: RetryConfig = NO_JITTER, clock: FakeClock | None = None) -> RetryCoordinator:
    return RetryCoordinator(config, clock=clock or FakeClock())


def failure(category: FailureCategory, **kwargs: object) -> ProviderFailure:
    return ProviderFailure(category, provider="brave", operation="search_web", **kwargs)


def test_successful_call_records_one_attempt() -> None:
    records: list[AttemptRecord] = []
    result = coordinator().call(
        "brave", "search_web", lambda ordinal: "page", on_attempt=records.append
    )
    assert result == "page"
    assert [(r.ordinal, r.outcome) for r in records] == [(1, "succeeded")]


def test_transient_failure_is_retried_until_success() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        if ordinal < 3:
            raise failure(FailureCategory.TRANSIENT_SERVER_ERROR, status_code=500)
        return "page"

    records: list[AttemptRecord] = []
    clock = FakeClock()
    assert coordinator(clock=clock).call(
        "brave", "search_web", action, on_attempt=records.append
    ) == "page"
    assert calls == [1, 2, 3]
    assert [r.outcome for r in records] == ["failed", "failed", "succeeded"]
    assert clock.slept == [1.0, 2.0]


def test_backoff_is_capped() -> None:
    config = RetryConfig(
        max_attempts=5,
        initial_backoff_seconds=10.0,
        max_backoff_seconds=15.0,
        backoff_multiplier=10.0,
        jitter_ratio=0.0,
    )
    clock = FakeClock()

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT)

    with pytest.raises(RetryExhausted):
        coordinator(config, clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert clock.slept == [10.0, 15.0, 15.0, 15.0]


def test_retry_after_overrides_computed_backoff() -> None:
    clock = FakeClock()

    def action(ordinal: int) -> str:
        if ordinal == 1:
            raise failure(FailureCategory.RATE_LIMIT, status_code=429, retry_after_ms=7000)
        return "page"

    coordinator(clock=clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert clock.slept == [7.0]


@pytest.mark.parametrize(
    "category",
    [
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.ACCESS_DENIED,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.RESPONSE_TOO_LARGE,
    ],
)
def test_permanent_failures_are_not_retried(category: FailureCategory) -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise failure(category)

    clock = FakeClock()
    with pytest.raises(ProviderFailure) as raised:
        coordinator(clock=clock).call("brave", "search_web", action, on_attempt=lambda _: None)
    assert raised.value.category is category
    assert calls == [1]
    assert clock.slept == []


def test_malformed_output_marked_retryable_gets_exactly_one_more_attempt() -> None:
    calls: list[int] = []

    def action(ordinal: int) -> str:
        calls.append(ordinal)
        raise ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="openrouter",
            operation="generate_structured",
            retryable=True,
        )

    with pytest.raises(ProviderFailure):
        coordinator().call(
            "openrouter", "generate_structured", action, on_attempt=lambda _: None
        )
    assert calls == [1, 2]


def test_exhaustion_raises_retry_exhausted_with_the_last_failure() -> None:
    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT, status_code=408)

    with pytest.raises(RetryExhausted) as raised:
        coordinator().call("brave", "search_web", action, on_attempt=lambda _: None)
    assert raised.value.attempts == 3
    assert raised.value.last_failure.category is FailureCategory.TIMEOUT


def test_provider_pauses_after_consecutive_exhaustions() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = coordinator(config)

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.PROVIDER_UNAVAILABLE)

    for _ in range(2):
        with pytest.raises(RetryExhausted):
            coordination.call("brave", "search_web", action, on_attempt=lambda _: None)

    assert coordination.is_paused("brave")
    assert coordination.paused_providers() == frozenset({"brave"})
    with pytest.raises(ProviderPaused):
        coordination.call("brave", "search_web", lambda ordinal: "page", on_attempt=lambda _: None)


def test_a_success_resets_the_consecutive_exhaustion_counter() -> None:
    config = RetryConfig(
        max_attempts=1,
        jitter_ratio=0.0,
        provider_pause_after_consecutive_exhaustions=2,
    )
    coordination = coordinator(config)

    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    coordination.call("brave", "search_web", lambda ordinal: "ok", on_attempt=lambda _: None)
    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    assert not coordination.is_paused("brave")


def test_pausing_one_provider_does_not_pause_another() -> None:
    config = RetryConfig(
        max_attempts=1, jitter_ratio=0.0, provider_pause_after_consecutive_exhaustions=1
    )
    coordination = coordinator(config)
    with pytest.raises(RetryExhausted):
        coordination.call(
            "brave",
            "search_web",
            lambda ordinal: (_ for _ in ()).throw(failure(FailureCategory.TIMEOUT)),
            on_attempt=lambda _: None,
        )
    assert coordination.is_paused("brave")
    assert not coordination.is_paused("mediawiki")
    assert coordination.call(
        "mediawiki", "search_pages", lambda ordinal: "ok", on_attempt=lambda _: None
    ) == "ok"


def test_jitter_stays_within_the_configured_ratio() -> None:
    import random

    config = RetryConfig(
        max_attempts=4,
        initial_backoff_seconds=10.0,
        max_backoff_seconds=100.0,
        backoff_multiplier=1.0,
        jitter_ratio=0.25,
    )
    clock = FakeClock()
    coordination = RetryCoordinator(config, clock=clock, random_source=random.Random(7))

    def action(ordinal: int) -> str:
        raise failure(FailureCategory.TIMEOUT)

    with pytest.raises(RetryExhausted):
        coordination.call("brave", "search_web", action, on_attempt=lambda _: None)
    assert len(clock.slept) == 3
    assert all(7.5 <= delay <= 12.5 for delay in clock.slept)


def test_attempt_records_carry_status_and_latency() -> None:
    clock = FakeClock()
    records: list[AttemptRecord] = []

    def action(ordinal: int) -> str:
        clock.advance(0.25)
        raise failure(FailureCategory.RATE_LIMIT, status_code=429, retry_after_ms=1000)

    with pytest.raises(RetryExhausted):
        coordinator(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock).call(
            "brave", "search_web", action, on_attempt=records.append
        )
    assert records[0].status_code == 429
    assert records[0].retry_after_ms == 1000
    assert records[0].latency_ms == 250
    assert records[0].failure_category is FailureCategory.RATE_LIMIT


def test_persisted_ordinals_can_continue_after_an_interrupted_attempt() -> None:
    records: list[AttemptRecord] = []
    calls: list[int] = []
    result = coordinator().call(
        "brave",
        "search_web",
        lambda ordinal: calls.append(ordinal) or "page",
        on_attempt=records.append,
        starting_ordinal=4,
    )
    assert result == "page"
    assert calls == [4]
    assert records[0].ordinal == 4
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_retry.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.retry'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/runs/retry.py`:

```python
from __future__ import annotations

import random
import threading
from collections.abc import Callable
from dataclasses import dataclass

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
)
from notable_person_finder.runs.clock import Clock


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    ordinal: int
    outcome: str
    latency_ms: int
    failure_category: FailureCategory | None = None
    status_code: int | None = None
    retry_after_ms: int | None = None
    detail: str | None = None


class RetryExhausted(Exception):
    """Every permitted attempt for one work item failed transiently."""

    def __init__(self, last_failure: ProviderFailure, *, attempts: int) -> None:
        self.last_failure = last_failure
        self.attempts = attempts
        super().__init__(
            f"{last_failure.provider}.{last_failure.operation} exhausted "
            f"{attempts} attempts: {last_failure.category}"
        )


class RetryCoordinator:
    """The only component permitted to start a repeat provider request."""

    def __init__(
        self,
        config: RetryConfig,
        *,
        clock: Clock,
        random_source: random.Random | None = None,
    ) -> None:
        self._config = config
        self._clock = clock
        self._random = random_source or random.Random()
        self._guard = threading.Lock()
        self._consecutive_exhaustions: dict[str, int] = {}
        self._paused: set[str] = set()

    def is_paused(self, provider: str) -> bool:
        with self._guard:
            return provider in self._paused

    def paused_providers(self) -> frozenset[str]:
        with self._guard:
            return frozenset(self._paused)

    def call[T](
        self,
        provider: str,
        operation: str,
        action: Callable[[int], T],
        *,
        on_attempt: Callable[[AttemptRecord], None],
        starting_ordinal: int = 1,
    ) -> T:
        if starting_ordinal < 1:
            raise ValueError("starting_ordinal must be at least 1")
        if self.is_paused(provider):
            with self._guard:
                exhaustions = self._consecutive_exhaustions.get(provider, 0)
            raise ProviderPaused(provider, consecutive_exhaustions=exhaustions)

        last_failure: ProviderFailure | None = None
        malformed_retries = 0
        for attempt_index in range(self._config.max_attempts):
            ordinal = starting_ordinal + attempt_index
            started = self._clock.monotonic()
            try:
                result = action(ordinal)
            except ProviderFailure as failure:
                on_attempt(
                    AttemptRecord(
                        ordinal=ordinal,
                        outcome="failed",
                        latency_ms=self._elapsed_ms(started),
                        failure_category=failure.category,
                        status_code=failure.status_code,
                        retry_after_ms=failure.retry_after_ms,
                        detail=failure.detail,
                    )
                )
                if not failure.retryable:
                    self._record_success_or_permanent(provider)
                    raise
                if failure.category is FailureCategory.MALFORMED_RESPONSE:
                    if malformed_retries >= 1:
                        self._record_success_or_permanent(provider)
                        raise
                    malformed_retries += 1
                last_failure = failure
                if attempt_index + 1 < self._config.max_attempts:
                    self._clock.sleep(self._delay(attempt_index + 1, failure))
                continue

            on_attempt(
                AttemptRecord(
                    ordinal=ordinal,
                    outcome="succeeded",
                    latency_ms=self._elapsed_ms(started),
                )
            )
            self._record_success_or_permanent(provider)
            return result

        assert last_failure is not None
        self._record_exhaustion(provider)
        raise RetryExhausted(last_failure, attempts=self._config.max_attempts)

    def _elapsed_ms(self, started: float) -> int:
        return max(int((self._clock.monotonic() - started) * 1000), 0)

    def _delay(self, ordinal: int, failure: ProviderFailure) -> float:
        if failure.retry_after_ms is not None:
            return failure.retry_after_ms / 1000
        base = self._config.initial_backoff_seconds * (
            self._config.backoff_multiplier ** (ordinal - 1)
        )
        capped = min(base, self._config.max_backoff_seconds)
        if self._config.jitter_ratio == 0:
            return capped
        spread = capped * self._config.jitter_ratio
        return max(self._random.uniform(capped - spread, capped + spread), 0.0)

    def _record_success_or_permanent(self, provider: str) -> None:
        # A permanent failure is a real answer from the provider, so it does not
        # count towards the consecutive-exhaustion pause either.
        with self._guard:
            self._consecutive_exhaustions[provider] = 0

    def _record_exhaustion(self, provider: str) -> None:
        with self._guard:
            count = self._consecutive_exhaustions.get(provider, 0) + 1
            self._consecutive_exhaustions[provider] = count
            if count >= self._config.provider_pause_after_consecutive_exhaustions:
                self._paused.add(provider)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_retry.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/retry.py tests/run_engine/test_retry.py
git commit -m "feat(runs): add central retry coordinator with provider pausing"
```

---

## Task 9: Budget Reservation and Reconciliation

The budget is optional and expressed as a decimal USD per-run cap. Reservation and reconciliation expose transaction-scoped primitives. Task 12 uses them inside the brief transactions that claim work and create the attempt, or finish the attempt and store its cost. The public wrappers in this task exist for focused tests and use `BEGIN IMMEDIATE`, so two concurrently scheduled external calls cannot reserve the same remaining allowance.

The run row holds one outstanding-plus-settled `budget_reserved_nano_usd` figure. Reserving adds; reconciling subtracts the reservation and adds the reported actual cost.

**Files:**
- Create: `src/notable_person_finder/runs/budget.py`
- Test: `tests/run_engine/test_budget.py`

**Interfaces:**
- Consumes: `sqlite3.Connection`; `FailureCategory`, `ProviderFailure`.
- Produces: `BudgetExhausted` exception; transaction primitives `reserve_in_transaction(connection, *, run_id, nano_usd) -> None` and `reconcile_in_transaction(connection, *, run_id, reserved_nano_usd, actual_nano_usd) -> None`; focused wrappers `reserve(...)` and `reconcile(...)`; `remaining(connection, *, run_id) -> int | None`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_budget.py`:

```python
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs.budget import (
    BudgetExhausted,
    reconcile,
    remaining,
    reserve,
)
from tests.run_engine.test_run_engine_schema import add_work, snapshot_id


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, tmp_path / "backups")
    connection.close()
    return path


def make_run(connection: sqlite3.Connection, limit_nano_usd: int | None) -> int:
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at, budget_limit_nano_usd
        )
        VALUES ('running', ?, 'Europe/Paris',
                '2026-07-24T00:00:00Z', '2026-07-25T00:00:00Z',
                '2026-07-25T06:00:00Z', ?)
        """,
        (snapshot_id(connection), limit_nano_usd),
    )
    connection.commit()
    return int(cursor.lastrowid)


def test_reservation_reduces_the_remaining_allowance(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    reserve(connection, run_id=run_id, nano_usd=400_000_000)
    assert remaining(connection, run_id=run_id) == 600_000_000
    connection.close()


def test_reservation_beyond_the_cap_is_refused(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    reserve(connection, run_id=run_id, nano_usd=900_000_000)
    with pytest.raises(BudgetExhausted):
        reserve(connection, run_id=run_id, nano_usd=200_000_000)
    assert remaining(connection, run_id=run_id) == 100_000_000
    connection.close()


def test_a_refused_reservation_leaves_no_partial_state(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    with pytest.raises(BudgetExhausted):
        reserve(connection, run_id=run_id, nano_usd=2_000_000_000)
    assert remaining(connection, run_id=run_id) == 1_000_000_000
    assert not connection.in_transaction
    connection.close()


def test_no_cap_means_unlimited_reservations(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, None)
    reserve(connection, run_id=run_id, nano_usd=999_999_999_999)
    assert remaining(connection, run_id=run_id) is None
    connection.close()


def test_reconciliation_replaces_the_reservation_with_the_actual_cost(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="a" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500_000_000)
        """.replace("500_000_000", "500000000"),
        (run_id, work_id, "b" * 64),
    )
    attempt_id = int(cursor.lastrowid)
    reserve(connection, run_id=run_id, nano_usd=500_000_000)

    reconcile(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        reserved_nano_usd=500_000_000,
        actual_nano_usd=120_000_000,
    )

    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 120_000_000
    assert row["budget_actual_nano_usd"] == 120_000_000
    assert (
        connection.execute(
            "SELECT actual_nano_usd FROM attempt WHERE id = ?", (attempt_id,)
        ).fetchone()["actual_nano_usd"]
        == 120_000_000
    )
    connection.close()


def test_reconciling_an_unreported_cost_keeps_the_reservation(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    work_id = add_work(connection, state="running", fingerprint="c" * 64)
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, reserved_nano_usd
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                '2026-07-25T06:00:00Z', ?, 500000000)
        """,
        (run_id, work_id, "d" * 64),
    )
    reserve(connection, run_id=run_id, nano_usd=500_000_000)
    reconcile(
        connection,
        run_id=run_id,
        attempt_id=int(cursor.lastrowid),
        reserved_nano_usd=500_000_000,
        actual_nano_usd=None,
    )
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    assert row["budget_reserved_nano_usd"] == 500_000_000
    assert row["budget_actual_nano_usd"] == 0
    connection.close()


def test_concurrent_reservations_cannot_oversubscribe(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    connection.close()

    granted = 0
    guard = threading.Lock()
    start = threading.Event()

    def worker() -> None:
        nonlocal granted
        own = connect_database(database)
        try:
            start.wait(timeout=5)
            reserve(own, run_id=run_id, nano_usd=300_000_000)
        except BudgetExhausted:
            return
        else:
            with guard:
                granted += 1
        finally:
            own.close()

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    start.set()
    for thread in threads:
        thread.join(timeout=10)

    assert granted == 3
    verify = connect_database(database, readonly=True)
    assert verify.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 900_000_000
    verify.close()


def test_negative_reservation_is_rejected(database: Path) -> None:
    connection = connect_database(database)
    run_id = make_run(connection, 1_000_000_000)
    with pytest.raises(ValueError):
        reserve(connection, run_id=run_id, nano_usd=-1)
    connection.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_budget.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.budget'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/runs/budget.py`:

```python
from __future__ import annotations

import sqlite3


class BudgetExhausted(Exception):
    """The configured hard per-run cap cannot cover this reservation."""

    def __init__(self, *, requested_nano_usd: int, remaining_nano_usd: int) -> None:
        self.requested_nano_usd = requested_nano_usd
        self.remaining_nano_usd = remaining_nano_usd
        super().__init__(
            f"reservation of {requested_nano_usd} nano-USD exceeds the "
            f"{remaining_nano_usd} nano-USD remaining in the run budget"
        )


def remaining(connection: sqlite3.Connection, *, run_id: int) -> int | None:
    row = connection.execute(
        "SELECT budget_limit_nano_usd, budget_reserved_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} does not exist")
    if row["budget_limit_nano_usd"] is None:
        return None
    return row["budget_limit_nano_usd"] - row["budget_reserved_nano_usd"]


def reserve(connection: sqlite3.Connection, *, run_id: int, nano_usd: int) -> None:
    """Reserve a conservative maximum cost, or refuse the call entirely.

    Uses BEGIN IMMEDIATE so that a concurrently scheduled external call cannot
    read the same remaining allowance and spend it twice.
    """
    if nano_usd < 0:
        raise ValueError("a budget reservation must not be negative")

    connection.execute("BEGIN IMMEDIATE")
    try:
        reserve_in_transaction(connection, run_id=run_id, nano_usd=nano_usd)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def reserve_in_transaction(
    connection: sqlite3.Connection, *, run_id: int, nano_usd: int
) -> None:
    """Reserve within the caller's existing write transaction."""
    if nano_usd < 0:
        raise ValueError("a budget reservation must not be negative")
    if not connection.in_transaction:
        raise RuntimeError("reserve_in_transaction requires an active transaction")
    row = connection.execute(
        "SELECT budget_limit_nano_usd, budget_reserved_nano_usd FROM run WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"run {run_id} does not exist")
    limit = row["budget_limit_nano_usd"]
    reserved = row["budget_reserved_nano_usd"]
    if limit is not None and reserved + nano_usd > limit:
        raise BudgetExhausted(
            requested_nano_usd=nano_usd, remaining_nano_usd=limit - reserved
        )
    connection.execute(
        "UPDATE run SET budget_reserved_nano_usd = ? WHERE id = ?",
        (reserved + nano_usd, run_id),
    )


def reconcile(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    reserved_nano_usd: int,
    actual_nano_usd: int | None,
) -> None:
    """Replace a reservation with the provider-reported actual cost.

    When the provider reports no cost the reservation is retained: releasing it
    would let an unmeasured call escape the cap.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        reconcile_in_transaction(
            connection,
            run_id=run_id,
            reserved_nano_usd=reserved_nano_usd,
            actual_nano_usd=actual_nano_usd,
        )
        connection.execute(
            "UPDATE attempt SET actual_nano_usd = ? WHERE id = ?",
            (actual_nano_usd, attempt_id),
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def reconcile_in_transaction(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    reserved_nano_usd: int,
    actual_nano_usd: int | None,
) -> None:
    """Replace a reservation inside the caller's finishing transaction."""
    if not connection.in_transaction:
        raise RuntimeError("reconcile_in_transaction requires an active transaction")
    if actual_nano_usd is None:
        return
    connection.execute(
        """
        UPDATE run
           SET budget_reserved_nano_usd =
                   MAX(budget_reserved_nano_usd - ? + ?, 0),
               budget_actual_nano_usd = budget_actual_nano_usd + ?
         WHERE id = ?
        """,
        (reserved_nano_usd, actual_nano_usd, actual_nano_usd, run_id),
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_budget.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/budget.py tests/run_engine/test_budget.py
git commit -m "feat(runs): add transactional nano-USD budget reservation"
```

---

## Task 10: Domain Enums and Run Lifecycle

`runs/models.py` holds the frozen dataclasses and enums the rest of the milestone passes around. `runs/repository.py` holds the feature-owned SQL. This task covers run creation, transitions, terminal states, and the startup sweep that converts an abandoned `running` run into `interrupted`.

There is no resume mode: every invocation creates a run, and the sweep is what makes a crashed predecessor's work eligible again.

**Files:**
- Create: `src/notable_person_finder/runs/models.py`
- Create: `src/notable_person_finder/runs/repository.py`
- Test: `tests/run_engine/test_repository.py`

**Interfaces:**
- Consumes: `sqlite3.Connection`. Callers pass `now` as a UTC ISO-8601 `Z` string; the `Clock` and `utc_timestamp` are threaded by the engine in Task 12, not by this layer.
- Produces: `RunState`, `WorkState`, `AttemptOutcome` (str enums); `RunRecord`, `WorkItem`, `SweepResult`, `RunCounters` (frozen dataclasses); repository functions `store_snapshot`, `create_run`, `finish_run`, `sweep_interrupted`, `load_run`, `latest_run`. Transition rows are written by the private `_insert_transition` as part of the state change that produces them; there is no public transition writer.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_repository.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.models import RunState, WorkState

WINDOW = ("2026-07-24T06:00:00Z", "2026-07-25T06:00:00Z")


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


def new_run(connection: sqlite3.Connection, *, at: str = "2026-07-25T06:00:00Z") -> int:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=at
    )
    return repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start=WINDOW[0],
        window_end=WINDOW[1],
        budget_limit_nano_usd=None,
        now=at,
    )


def add_pending_work(connection: sqlite3.Connection, *, run_id: int, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 100,
                '2026-07-25T06:00:00Z', 'pending', ?,
                '2026-07-25T06:00:00Z', '2026-07-25T06:00:00Z')
        """,
        (fingerprint, run_id),
    )
    connection.commit()
    return int(cursor.lastrowid)


def test_snapshot_is_stored_once_per_fingerprint(connection: sqlite3.Connection) -> None:
    first = repository.store_snapshot(
        connection, fingerprint="b" * 64, canonical_json="{}", now="2026-07-25T06:00:00Z"
    )
    second = repository.store_snapshot(
        connection, fingerprint="b" * 64, canonical_json="{}", now="2026-07-25T07:00:00Z"
    )
    assert first == second


def test_creating_a_run_records_the_running_transition(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    record = repository.load_run(connection, run_id=run_id)
    assert record.state is RunState.RUNNING
    assert record.finished_at is None
    assert record.human_id == f"run-{run_id}"
    states = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id", (run_id,)
        )
    ]
    assert states == ["running"]


def test_every_invocation_creates_a_new_run_on_the_same_day(connection: sqlite3.Connection) -> None:
    first = new_run(connection)
    second = new_run(connection)
    assert first != second


def test_finishing_a_run_sets_state_time_and_transition(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path="/tmp/2026-07-25-run-1.md",
        digest_sha256="c" * 64,
        now="2026-07-25T06:30:00Z",
    )
    record = repository.load_run(connection, run_id=run_id)
    assert record.state is RunState.COMPLETE
    assert record.finished_at == "2026-07-25T06:30:00Z"
    assert record.digest_path == "/tmp/2026-07-25-run-1.md"
    states = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id", (run_id,)
        )
    ]
    assert states == ["running", "complete"]


def test_sweep_marks_an_abandoned_run_interrupted(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.runs == (abandoned,)
    record = repository.load_run(connection, run_id=abandoned)
    assert record.state is RunState.INTERRUPTED
    assert record.finished_at == "2026-07-25T07:00:00Z"


def test_sweep_returns_abandoned_running_work_to_pending(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    work_id = add_pending_work(connection, run_id=abandoned, fingerprint="d" * 64)
    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (abandoned, work_id),
    )
    connection.commit()

    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.work_items == 1
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.PENDING
    assert row["claimed_by_run_id"] is None


def test_sweep_marks_in_flight_attempts_interrupted(connection: sqlite3.Connection) -> None:
    abandoned = new_run(connection)
    work_id = add_pending_work(connection, run_id=abandoned, fingerprint="e" * 64)
    connection.execute(
        """
        INSERT INTO attempt (run_id, work_item_id, provider, operation, ordinal,
                             started_at, request_fingerprint)
        VALUES (?, ?, 'brave', 'search_web', 1, '2026-07-25T06:00:00Z', ?)
        """,
        (abandoned, work_id, "f" * 64),
    )
    connection.commit()

    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.attempts == 1
    row = connection.execute("SELECT outcome, finished_at FROM attempt").fetchone()
    assert row["outcome"] == "interrupted"
    assert row["finished_at"] == "2026-07-25T07:00:00Z"


def test_sweep_leaves_completed_runs_alone(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path=None,
        digest_sha256=None,
        now="2026-07-25T06:30:00Z",
    )
    result = repository.sweep_interrupted(connection, now="2026-07-25T07:00:00Z")
    assert result.runs == ()
    assert repository.load_run(connection, run_id=run_id).state is RunState.COMPLETE


def test_latest_run_returns_the_most_recent(connection: sqlite3.Connection) -> None:
    new_run(connection)
    second = new_run(connection)
    assert repository.latest_run(connection).id == second


def test_latest_run_is_none_on_an_empty_database(connection: sqlite3.Connection) -> None:
    assert repository.latest_run(connection) is None


def test_a_run_cannot_be_finished_twice(connection: sqlite3.Connection) -> None:
    run_id = new_run(connection)
    repository.finish_run(
        connection,
        run_id=run_id,
        state=RunState.COMPLETE,
        reason=None,
        digest_path=None,
        digest_sha256=None,
        now="2026-07-25T06:30:00Z",
    )
    with pytest.raises(RuntimeError, match="already terminal"):
        repository.finish_run(
            connection,
            run_id=run_id,
            state=RunState.FAILED,
            reason="late reporting failure",
            digest_path=None,
            digest_sha256=None,
            now="2026-07-25T06:31:00Z",
        )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.models'`

- [ ] **Step 3: Write the domain models**

Create `src/notable_person_finder/runs/models.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RunState(StrEnum):
    RUNNING = "running"
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


TERMINAL_RUN_STATES = frozenset(
    {RunState.COMPLETE, RunState.PARTIAL, RunState.FAILED, RunState.INTERRUPTED}
)


class WorkState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEFERRED = "deferred"
    FAILED_PERMANENT = "failed_permanent"
    SUPERSEDED = "superseded"


ACTIVE_WORK_STATES = frozenset({WorkState.PENDING, WorkState.RUNNING, WorkState.DEFERRED})


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: int
    state: RunState
    timezone: str
    window_start: str
    window_end: str
    started_at: str
    finished_at: str | None
    budget_limit_nano_usd: int | None
    budget_reserved_nano_usd: int
    budget_actual_nano_usd: int
    digest_path: str | None
    digest_sha256: str | None

    @property
    def human_id(self) -> str:
        return f"run-{self.id}"


@dataclass(frozen=True, slots=True)
class WorkItem:
    id: int
    task_type: str
    subject_kind: str
    subject_id: int | None
    fingerprint: str
    required: bool
    priority: int
    state: WorkState


@dataclass(frozen=True, slots=True)
class SweepResult:
    runs: tuple[int, ...]
    work_items: int
    attempts: int


@dataclass(frozen=True, slots=True)
class RunCounters:
    required_succeeded: int
    required_pending: int
    required_deferred: int
    required_failed_permanent: int
    optional_succeeded: int
    optional_skipped: int
    operational_failures: int
```

- [ ] **Step 4: Write the run lifecycle repository**

Create `src/notable_person_finder/runs/repository.py`:

```python
from __future__ import annotations

import sqlite3

from notable_person_finder.runs.models import (
    RunRecord,
    RunState,
    SweepResult,
    WorkState,
)


def store_snapshot(
    connection: sqlite3.Connection, *, fingerprint: str, canonical_json: str, now: str
) -> int:
    """Insert the redacted configuration snapshot, reusing an identical one."""
    connection.execute(
        """
        INSERT INTO configuration_snapshot (fingerprint, canonical_json, created_at)
        VALUES (?, ?, ?)
        ON CONFLICT(fingerprint) DO NOTHING
        """,
        (fingerprint, canonical_json, now),
    )
    connection.commit()
    row = connection.execute(
        "SELECT id FROM configuration_snapshot WHERE fingerprint = ?", (fingerprint,)
    ).fetchone()
    return int(row["id"])


def create_run(
    connection: sqlite3.Connection,
    *,
    snapshot_id: int,
    timezone: str,
    window_start: str,
    window_end: str,
    budget_limit_nano_usd: int | None,
    now: str,
) -> int:
    connection.execute("BEGIN IMMEDIATE")
    try:
        cursor = connection.execute(
            """
            INSERT INTO run (
                state, configuration_snapshot_id, timezone,
                window_start, window_end, started_at, budget_limit_nano_usd
            )
            VALUES ('running', ?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, timezone, window_start, window_end, now, budget_limit_nano_usd),
        )
        run_id = int(cursor.lastrowid)
        _insert_transition(connection, run_id=run_id, state=RunState.RUNNING, reason=None, now=now)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return run_id


def _insert_transition(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    state: RunState,
    reason: str | None,
    now: str,
) -> None:
    connection.execute(
        """
        INSERT INTO run_transition (run_id, state, reason, occurred_at)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, str(state), reason, now),
    )


def finish_run(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    state: RunState,
    reason: str | None,
    digest_path: str | None,
    digest_sha256: str | None,
    now: str,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE run
               SET state = ?, finished_at = ?, digest_path = ?, digest_sha256 = ?
             WHERE id = ? AND state = 'running'
            """,
            (str(state), now, digest_path, digest_sha256, run_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"run-{run_id} is already terminal or does not exist")
        _insert_transition(connection, run_id=run_id, state=state, reason=reason, now=now)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def sweep_interrupted(connection: sqlite3.Connection, *, now: str) -> SweepResult:
    """Record abandoned runs, work, and attempts left by a crashed process.

    Runs before any provider call in a new invocation, under the mutation lock,
    so no live run can be swept.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        abandoned = tuple(
            int(row["id"])
            for row in connection.execute("SELECT id FROM run WHERE state = 'running'")
        )
        if not abandoned:
            connection.rollback()
            return SweepResult(runs=(), work_items=0, attempts=0)

        placeholders = ",".join("?" for _ in abandoned)
        attempts = connection.execute(
            f"""
            UPDATE attempt
               SET outcome = 'interrupted', finished_at = ?
             WHERE outcome IS NULL AND run_id IN ({placeholders})
            """,
            (now, *abandoned),
        ).rowcount
        work_items = connection.execute(
            f"""
            UPDATE work_item
               SET state = 'pending', claimed_by_run_id = NULL, updated_at = ?
             WHERE state = 'running' AND claimed_by_run_id IN ({placeholders})
            """,
            (now, *abandoned),
        ).rowcount
        connection.execute(
            f"UPDATE run SET state = 'interrupted', finished_at = ? WHERE id IN ({placeholders})",
            (now, *abandoned),
        )
        for run_id in abandoned:
            _insert_transition(
                connection,
                run_id=run_id,
                state=RunState.INTERRUPTED,
                reason="process ended before a terminal state was recorded",
                now=now,
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return SweepResult(runs=abandoned, work_items=work_items, attempts=attempts)


def _to_record(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        id=int(row["id"]),
        state=RunState(row["state"]),
        timezone=row["timezone"],
        window_start=row["window_start"],
        window_end=row["window_end"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        budget_limit_nano_usd=row["budget_limit_nano_usd"],
        budget_reserved_nano_usd=row["budget_reserved_nano_usd"],
        budget_actual_nano_usd=row["budget_actual_nano_usd"],
        digest_path=row["digest_path"],
        digest_sha256=row["digest_sha256"],
    )


def load_run(connection: sqlite3.Connection, *, run_id: int) -> RunRecord:
    row = connection.execute("SELECT * FROM run WHERE id = ?", (run_id,)).fetchone()
    if row is None:
        raise LookupError(f"run-{run_id} does not exist")
    return _to_record(row)


def latest_run(connection: sqlite3.Connection) -> RunRecord | None:
    row = connection.execute("SELECT * FROM run ORDER BY id DESC LIMIT 1").fetchone()
    return None if row is None else _to_record(row)
```

The `WorkState` import is used by Task 11; leave it in place.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_repository.py -v`
Expected: PASS. Task 10 uses direct SQL fixtures for work rows so this commit remains green; Task 11 adds the public scheduling interface.

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/runs/models.py src/notable_person_finder/runs/repository.py tests/run_engine/test_repository.py
git commit -m "feat(runs): add run lifecycle records and interrupted-run sweep"
```

---

## Task 11: Work Item Scheduling and Claiming

Work is scheduled with a fingerprint that identifies its material inputs. The partial unique index makes duplicate active scheduling impossible, so `schedule_work` returns the existing row's identity instead of raising. A changed input supersedes the obsolete active row and schedules a new one.

Queue inspection can be restricted to task types for which the current engine has handlers, so unknown future work remains pending without being reclaimed in a loop. `claim_next` provides a focused queue primitive for deterministic work and tests. External work uses Task 12's `claim_and_start_attempt`, which performs the claim, budget reservation, and attempt insert in one transaction. Deferred work is eligible only when it was completed by an earlier run.

**Files:**
- Modify: `src/notable_person_finder/runs/repository.py`
- Test: `tests/run_engine/test_work_items.py`

**Interfaces:**
- Consumes: `WorkItem`, `WorkState`, `RunCounters`.
- Produces: `schedule_work(connection, *, task_type, subject_kind, subject_id, fingerprint, required, priority, eligible_at, run_id, now) -> int`; `supersede_work(connection, *, task_type, fingerprint, now, reason) -> int`; `next_eligible(connection, *, run_id, now, task_types=None) -> WorkItem | None`; `claim_next(connection, *, run_id, now, task_types=None) -> WorkItem | None`; `complete_work(connection, *, work_item_id, run_id, state, reason, now, eligible_at=None) -> None`; `run_counters(connection, *, run_id, now) -> RunCounters`; `pending_required(connection, *, now=None) -> int`; `deferred_required(connection) -> int`; and `operational_failures_for_run(connection, *, run_id) -> int`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_work_items.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.models import WorkState

NOW = "2026-07-25T06:00:00Z"
LATER = "2026-07-25T07:00:00Z"


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


@pytest.fixture
def run_id(connection: sqlite3.Connection) -> int:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    return repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        window_end=NOW,
        budget_limit_nano_usd=None,
        now=NOW,
    )


def schedule(
    connection: sqlite3.Connection,
    run_id: int,
    *,
    fingerprint: str = "b" * 64,
    task_type: str = "detect_people",
    required: bool = True,
    priority: int = 100,
    eligible_at: str = NOW,
) -> int:
    return repository.schedule_work(
        connection,
        task_type=task_type,
        subject_kind="source_item",
        subject_id=1,
        fingerprint=fingerprint,
        required=required,
        priority=priority,
        eligible_at=eligible_at,
        run_id=run_id,
        now=NOW,
    )


def test_scheduling_returns_a_new_work_item(connection: sqlite3.Connection, run_id: int) -> None:
    work_id = schedule(connection, run_id)
    assert work_id > 0


def test_rescheduling_identical_work_reuses_the_active_row(
    connection: sqlite3.Connection, run_id: int
) -> None:
    first = schedule(connection, run_id)
    second = schedule(connection, run_id)
    assert first == second
    count = connection.execute("SELECT COUNT(*) AS n FROM work_item").fetchone()["n"]
    assert count == 1


def test_a_changed_fingerprint_supersedes_the_old_active_row(
    connection: sqlite3.Connection, run_id: int
) -> None:
    stale = schedule(connection, run_id, fingerprint="c" * 64)
    superseded = repository.supersede_work(
        connection, task_type="detect_people", fingerprint="c" * 64, now=LATER, reason="input changed"
    )
    assert superseded == 1
    fresh = schedule(connection, run_id, fingerprint="d" * 64)
    assert fresh != stale
    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (stale,)).fetchone()["state"]
        == WorkState.SUPERSEDED
    )


def test_claiming_marks_the_item_running_and_attributes_the_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    claimed = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert claimed is not None
    assert claimed.id == work_id
    assert claimed.state is WorkState.RUNNING
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.RUNNING
    assert row["claimed_by_run_id"] == run_id


def test_claiming_holds_no_transaction_afterwards(
    connection: sqlite3.Connection, run_id: int
) -> None:
    schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    assert not connection.in_transaction


def test_claims_are_ordered_by_priority_then_identity(
    connection: sqlite3.Connection, run_id: int
) -> None:
    low = schedule(connection, run_id, fingerprint="e" * 64, priority=500)
    high = schedule(connection, run_id, fingerprint="f" * 64, priority=10)
    assert repository.claim_next(connection, run_id=run_id, now=NOW).id == high
    assert repository.claim_next(connection, run_id=run_id, now=NOW).id == low


def test_work_is_not_claimed_before_its_eligibility_time(
    connection: sqlite3.Connection, run_id: int
) -> None:
    schedule(connection, run_id, eligible_at="2026-07-26T00:00:00Z")
    assert repository.claim_next(connection, run_id=run_id, now=NOW) is None


def test_deferred_work_is_not_claimed_in_the_same_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="not_evaluated_budget",
        now=NOW,
    )
    assert repository.claim_next(connection, run_id=run_id, now=NOW) is None


def test_deferred_work_becomes_eligible_in_the_next_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    next_run = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=LATER,
    )
    claimed = repository.claim_next(connection, run_id=next_run, now=LATER)
    assert claimed is not None and claimed.id == work_id


def test_claiming_can_be_restricted_to_registered_task_types(
    connection: sqlite3.Connection, run_id: int
) -> None:
    unknown = schedule(connection, run_id, fingerprint="9" * 64, task_type="future_task")
    assert repository.claim_next(
        connection, run_id=run_id, now=NOW, task_types={"detect_people"}
    ) is None
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (unknown,)
    ).fetchone()["state"] == WorkState.PENDING


def test_permanently_failed_work_is_never_reclaimed(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.FAILED_PERMANENT,
        reason="authentication",
        now=NOW,
    )
    next_run = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=LATER,
    )
    assert repository.claim_next(connection, run_id=next_run, now=LATER) is None


def test_succeeded_work_frees_the_active_identity(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )
    # Same identity may be scheduled again only if the caller decides to; the
    # index no longer blocks it, and history is preserved.
    again = schedule(connection, run_id)
    assert again != work_id


def test_counters_separate_required_from_optional_work(
    connection: sqlite3.Connection, run_id: int
) -> None:
    done = schedule(connection, run_id, fingerprint="1" * 64, required=True)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection, work_item_id=done, run_id=run_id, state=WorkState.SUCCEEDED, reason=None, now=NOW
    )
    schedule(connection, run_id, fingerprint="2" * 64, required=True)
    optional = schedule(connection, run_id, fingerprint="3" * 64, required=False)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=optional,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )

    counters = repository.run_counters(connection, run_id=run_id, now=NOW)
    assert counters.required_succeeded == 1
    assert counters.optional_succeeded == 1
    assert repository.pending_required(connection, now=NOW) >= 1
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_work_items.py -v`
Expected: FAIL with `AttributeError: module 'notable_person_finder.runs.repository' has no attribute 'schedule_work'`

- [ ] **Step 3: Add the work-item functions**

Append to `src/notable_person_finder/runs/repository.py`, add `from collections.abc import Collection`, and add `RunCounters` and `WorkItem` to the existing `notable_person_finder.runs.models` import:

```python
def schedule_work(
    connection: sqlite3.Connection,
    *,
    task_type: str,
    subject_kind: str,
    subject_id: int | None,
    fingerprint: str,
    required: bool,
    priority: int,
    eligible_at: str,
    run_id: int | None,
    now: str,
) -> int:
    """Create the work item, or return the identity of the existing active one.

    Duplicate active scheduling is prevented by `work_item_active_identity`, so
    a caller that rediscovers the same input is a no-op rather than an error.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = connection.execute(
            """
            SELECT id FROM work_item
             WHERE task_type = ? AND fingerprint = ?
               AND state IN ('pending', 'running', 'deferred')
            """,
            (task_type, fingerprint),
        ).fetchone()
        if existing is not None:
            work_id = int(existing["id"])
        else:
            cursor = connection.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    task_type,
                    subject_kind,
                    subject_id,
                    fingerprint,
                    1 if required else 0,
                    priority,
                    eligible_at,
                    run_id,
                    now,
                    now,
                ),
            )
            work_id = int(cursor.lastrowid)
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return work_id


def supersede_work(
    connection: sqlite3.Connection,
    *,
    task_type: str,
    fingerprint: str,
    now: str,
    reason: str,
) -> int:
    """Retire active work whose material input has changed."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'superseded', reason = ?, updated_at = ?
             WHERE task_type = ? AND fingerprint = ?
               AND state IN ('pending', 'deferred')
            """,
            (reason, now, task_type, fingerprint),
        ).rowcount
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return changed


def _work_item(row: sqlite3.Row, *, state: WorkState | None = None) -> WorkItem:
    return WorkItem(
        id=int(row["id"]),
        task_type=row["task_type"],
        subject_kind=row["subject_kind"],
        subject_id=row["subject_id"],
        fingerprint=row["fingerprint"],
        required=bool(row["required"]),
        priority=int(row["priority"]),
        state=state or WorkState(row["state"]),
    )


def next_eligible(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Inspect the next eligible item without mutating it."""
    if task_types is not None and not task_types:
        return None
    task_filter = ""
    parameters: list[object] = [run_id, now]
    if task_types is not None:
        ordered = sorted(task_types)
        task_filter = f" AND task_type IN ({','.join('?' for _ in ordered)})"
        parameters.extend(ordered)
    row = connection.execute(
        f"""
        SELECT * FROM work_item
         WHERE (
                   state = 'pending'
                OR (state = 'deferred' AND COALESCE(completed_by_run_id, -1) <> ?)
               )
           AND eligible_at <= ?
           {task_filter}
         ORDER BY priority ASC, id ASC
         LIMIT 1
        """,
        parameters,
    ).fetchone()
    return None if row is None else _work_item(row)


def claim_next(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    now: str,
    task_types: Collection[str] | None = None,
) -> WorkItem | None:
    """Claim deterministic work; external calls use claim_and_start_attempt."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        item = next_eligible(
            connection, run_id=run_id, now=now, task_types=task_types
        )
        if item is None:
            connection.rollback()
            return None
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'running', claimed_by_run_id = ?, updated_at = ?
             WHERE id = ? AND state IN ('pending', 'deferred')
            """,
            (run_id, now, item.id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"work item {item.id} was no longer claimable")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return WorkItem(
        id=item.id,
        task_type=item.task_type,
        subject_kind=item.subject_kind,
        subject_id=item.subject_id,
        fingerprint=item.fingerprint,
        required=item.required,
        priority=item.priority,
        state=WorkState.RUNNING,
    )


def complete_work(
    connection: sqlite3.Connection,
    *,
    work_item_id: int,
    run_id: int,
    state: WorkState,
    reason: str | None,
    now: str,
    eligible_at: str | None = None,
) -> None:
    """Record one item's terminal or deferred outcome; failures are isolated."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            UPDATE work_item
               SET state = ?,
                   reason = ?,
                   completed_by_run_id = ?,
                   claimed_by_run_id = NULL,
                   eligible_at = COALESCE(?, eligible_at),
                   updated_at = ?
             WHERE id = ?
            """,
            (str(state), reason, run_id, eligible_at, now, work_item_id),
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def pending_required(
    connection: sqlite3.Connection, *, now: str | None = None
) -> int:
    eligibility = "" if now is None else " AND eligible_at <= ?"
    parameters = () if now is None else (now,)
    return int(
        connection.execute(
            f"""
            SELECT COUNT(*) AS n FROM work_item
             WHERE required = 1 AND state IN ('pending', 'running'){eligibility}
            """,
            parameters,
        ).fetchone()["n"]
    )


def deferred_required(connection: sqlite3.Connection) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM work_item WHERE required = 1 AND state = 'deferred'"
        ).fetchone()["n"]
    )


def operational_failures_for_run(
    connection: sqlite3.Connection, *, run_id: int
) -> int:
    return int(
        connection.execute(
            "SELECT COUNT(*) AS n FROM attempt WHERE run_id = ? AND outcome = 'failed'",
            (run_id,),
        ).fetchone()["n"]
    )


def run_counters(
    connection: sqlite3.Connection, *, run_id: int, now: str
) -> RunCounters:
    tally = {
        (row["required"], row["state"]): int(row["n"])
        for row in connection.execute(
            """
            SELECT required, state, COUNT(*) AS n
              FROM work_item
             WHERE completed_by_run_id = ?
             GROUP BY required, state
            """,
            (run_id,),
        )
    }
    required_deferred = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM work_item
             WHERE required = 1 AND state = 'deferred' AND eligible_at <= ?
            """,
            (now,),
        ).fetchone()["n"]
    )
    return RunCounters(
        required_succeeded=tally.get((1, "succeeded"), 0),
        required_pending=pending_required(connection, now=now),
        required_deferred=required_deferred,
        required_failed_permanent=tally.get((1, "failed_permanent"), 0),
        optional_succeeded=tally.get((0, "succeeded"), 0),
        optional_skipped=tally.get((0, "superseded"), 0),
        operational_failures=operational_failures_for_run(connection, run_id=run_id),
    )
```

- [ ] **Step 4: Run both repository test files to verify they pass**

Run: `uv run pytest tests/run_engine/test_work_items.py tests/run_engine/test_repository.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/repository.py tests/run_engine/test_work_items.py
git commit -m "feat(runs): add durable work scheduling, claiming, and completion"
```

---

## Task 12: Persisted Attempt Attribution

Every physical external call maps to exactly one immutable attempt row, attributed to its run and work item. Attempts are created before the call and finished after it, so a crash leaves a row with a NULL outcome for the sweep to mark `interrupted`. A success never overwrites its failed predecessor: each retry gets a new ordinal.

**Files:**
- Modify: `src/notable_person_finder/runs/repository.py`
- Test: `tests/run_engine/test_attempts.py`

**Interfaces:**
- Consumes: `AttemptOutcome`, `FailureCategory`, `AttemptRecord`, `reserve_in_transaction`, and `reconcile_in_transaction`.
- Produces: `next_attempt_ordinal(connection, *, work_item_id) -> int`; `claim_and_start_attempt(connection, *, run_id, work_item_id, provider, operation, ordinal, request_fingerprint, destination_host, reserved_nano_usd, now) -> int`; `start_attempt` with the same keyword parameters for an already-claimed retry; `finish_attempt(connection, *, attempt_id, record, response_bytes, provider_request_id, detail_json, now, actual_nano_usd=None) -> None`; and `attempts_for_run(connection, *, run_id) -> tuple[sqlite3.Row, ...]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_attempts.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.failures import FailureCategory
from notable_person_finder.runs import repository
from notable_person_finder.runs.models import WorkState
from notable_person_finder.runs.retry import AttemptRecord

NOW = "2026-07-25T06:00:00Z"
DONE = "2026-07-25T06:00:02Z"


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


@pytest.fixture
def context(connection: sqlite3.Connection) -> tuple[int, int]:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    run_id = repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        window_end=NOW,
        budget_limit_nano_usd=None,
        now=NOW,
    )
    work_id = repository.schedule_work(
        connection,
        task_type="search_coverage",
        subject_kind="person",
        subject_id=7,
        fingerprint="b" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )
    return run_id, work_id


def start(connection: sqlite3.Connection, context: tuple[int, int], ordinal: int) -> int:
    run_id, work_id = context
    function = (
        repository.claim_and_start_attempt
        if ordinal == 1
        else repository.start_attempt
    )
    return function(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="brave",
        operation="search_web",
        ordinal=ordinal,
        request_fingerprint="c" * 64,
        destination_host="api.search.brave.com",
        reserved_nano_usd=0,
        now=NOW,
    )


def test_attempt_is_created_in_flight_with_no_outcome(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] is None
    assert row["finished_at"] is None
    assert row["destination_host"] == "api.search.brave.com"


def test_finishing_a_successful_attempt_records_latency_and_bytes(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(ordinal=1, outcome="succeeded", latency_ms=250),
        response_bytes=4096,
        provider_request_id="req-abc",
        detail_json=None,
        now=DONE,
    )
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] == "succeeded"
    assert row["latency_ms"] == 250
    assert row["response_bytes"] == 4096
    assert row["provider_request_id"] == "req-abc"
    assert row["failure_category"] is None
    assert row["finished_at"] == DONE


def test_finishing_a_failed_attempt_records_the_typed_category(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(
            ordinal=1,
            outcome="failed",
            latency_ms=90,
            failure_category=FailureCategory.RATE_LIMIT,
            status_code=429,
            retry_after_ms=7000,
            detail="rate limited",
        ),
        response_bytes=None,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] == "failed"
    assert row["failure_category"] == "rate_limit"
    assert row["provider_status"] == 429
    assert row["retry_after_ms"] == 7000


def test_a_retry_creates_a_new_ordinal_and_preserves_the_failure(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    first = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=first,
        record=AttemptRecord(
            ordinal=1,
            outcome="failed",
            latency_ms=10,
            failure_category=FailureCategory.TIMEOUT,
        ),
        response_bytes=None,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    second = start(connection, context, 2)
    repository.finish_attempt(
        connection,
        attempt_id=second,
        record=AttemptRecord(ordinal=2, outcome="succeeded", latency_ms=20),
        response_bytes=10,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    outcomes = [
        row["outcome"]
        for row in connection.execute("SELECT outcome FROM attempt ORDER BY ordinal")
    ]
    assert outcomes == ["failed", "succeeded"]


def test_every_attempt_is_attributed_to_its_run_and_work_item(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    run_id, work_id = context
    start(connection, context, 1)
    row = repository.attempts_for_run(connection, run_id=run_id)[0]
    assert row["run_id"] == run_id
    assert row["work_item_id"] == work_id


def test_attempt_detail_must_not_contain_a_raw_body(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    # detail_json is reserved for immutable provider metadata; the transport
    # never passes response content into it.
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(ordinal=1, outcome="succeeded", latency_ms=5),
        response_bytes=12,
        provider_request_id=None,
        detail_json='{"resolved_provider":"anthropic"}',
        now=DONE,
    )
    row = connection.execute("SELECT detail_json FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["detail_json"] == '{"resolved_provider":"anthropic"}'


def test_an_unfinished_attempt_survives_for_the_sweep(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    start(connection, context, 1)
    result = repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert result.attempts == 1
    assert (
        connection.execute("SELECT outcome FROM attempt").fetchone()["outcome"] == "interrupted"
    )


def test_next_ordinal_continues_after_an_interrupted_attempt(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    start(connection, context, 1)
    repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert repository.next_attempt_ordinal(connection, work_item_id=context[1]) == 2


def test_first_attempt_claims_and_reserves_in_one_transaction(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    run_id, work_id = context
    connection.execute(
        "UPDATE run SET budget_limit_nano_usd = 1000 WHERE id = ?", (run_id,)
    )
    connection.commit()
    repository.claim_and_start_attempt(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="openrouter",
        operation="generate_structured",
        ordinal=1,
        request_fingerprint="d" * 64,
        destination_host=None,
        reserved_nano_usd=400,
        now=NOW,
    )
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == "running"
    assert connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 400


def test_refused_first_reservation_leaves_work_pending_and_no_attempt(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    from notable_person_finder.runs.budget import BudgetExhausted

    run_id, work_id = context
    connection.execute(
        "UPDATE run SET budget_limit_nano_usd = 100 WHERE id = ?", (run_id,)
    )
    connection.commit()
    with pytest.raises(BudgetExhausted):
        repository.claim_and_start_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_id,
            provider="openrouter",
            operation="generate_structured",
            ordinal=1,
            request_fingerprint="e" * 64,
            destination_host=None,
            reserved_nano_usd=400,
            now=NOW,
        )
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == "pending"
    assert connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()["n"] == 0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_attempts.py -v`
Expected: FAIL with `AttributeError: module 'notable_person_finder.runs.repository' has no attribute 'claim_and_start_attempt'`

- [ ] **Step 3: Add the attempt functions**

Append to `src/notable_person_finder/runs/repository.py`, adding `from notable_person_finder.runs.retry import AttemptRecord` and `from notable_person_finder.runs.budget import reconcile_in_transaction, reserve_in_transaction` to the imports:

```python
def next_attempt_ordinal(
    connection: sqlite3.Connection, *, work_item_id: int
) -> int:
    row = connection.execute(
        "SELECT COALESCE(MAX(ordinal), 0) + 1 AS ordinal FROM attempt WHERE work_item_id = ?",
        (work_item_id,),
    ).fetchone()
    return int(row["ordinal"])


def _insert_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal,
            started_at, request_fingerprint, destination_host, reserved_nano_usd
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id, work_item_id, provider, operation, ordinal, now,
            request_fingerprint, destination_host, reserved_nano_usd,
        ),
    )
    return int(cursor.lastrowid)


def claim_and_start_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    """Atomically claim work, reserve cost, and persist the first attempt."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        changed = connection.execute(
            """
            UPDATE work_item
               SET state = 'running', claimed_by_run_id = ?, updated_at = ?
             WHERE id = ?
               AND (
                       state = 'pending'
                    OR (state = 'deferred' AND COALESCE(completed_by_run_id, -1) <> ?)
               )
            """,
            (run_id, now, work_item_id, run_id),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"work item {work_item_id} is not claimable by run-{run_id}")
        reserve_in_transaction(
            connection,
            run_id=run_id,
            nano_usd=reserved_nano_usd,
        )
        attempt_id = _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            provider=provider,
            operation=operation,
            ordinal=ordinal,
            request_fingerprint=request_fingerprint,
            destination_host=destination_host,
            reserved_nano_usd=reserved_nano_usd,
            now=now,
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return attempt_id


def start_attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int,
    provider: str,
    operation: str,
    ordinal: int,
    request_fingerprint: str,
    destination_host: str | None,
    reserved_nano_usd: int,
    now: str,
) -> int:
    """Atomically reserve and persist a retry for already-running work."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        reserve_in_transaction(
            connection,
            run_id=run_id,
            nano_usd=reserved_nano_usd,
        )
        attempt_id = _insert_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_item_id,
            provider=provider,
            operation=operation,
            ordinal=ordinal,
            request_fingerprint=request_fingerprint,
            destination_host=destination_host,
            reserved_nano_usd=reserved_nano_usd,
            now=now,
        )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return attempt_id


def finish_attempt(
    connection: sqlite3.Connection,
    *,
    attempt_id: int,
    record: AttemptRecord,
    response_bytes: int | None,
    provider_request_id: str | None,
    detail_json: str | None,
    now: str,
    actual_nano_usd: int | None = None,
) -> None:
    """Close one immutable attempt. Retries insert new rows; nothing is rewritten."""
    connection.execute("BEGIN IMMEDIATE")
    try:
        attempt = connection.execute(
            "SELECT run_id, reserved_nano_usd FROM attempt WHERE id = ? AND outcome IS NULL",
            (attempt_id,),
        ).fetchone()
        if attempt is None:
            raise RuntimeError(f"attempt {attempt_id} is already finished or does not exist")
        reconcile_in_transaction(
            connection,
            run_id=int(attempt["run_id"]),
            reserved_nano_usd=int(attempt["reserved_nano_usd"]),
            actual_nano_usd=actual_nano_usd,
        )
        changed = connection.execute(
            """
            UPDATE attempt
               SET outcome = ?, failure_category = ?, provider_status = ?,
                   retry_after_ms = ?, latency_ms = ?, response_bytes = ?,
                   provider_request_id = ?, detail_json = ?, actual_nano_usd = ?,
                   finished_at = ?
             WHERE id = ? AND outcome IS NULL
            """,
            (
                record.outcome,
                None if record.failure_category is None else str(record.failure_category),
                record.status_code,
                record.retry_after_ms,
                record.latency_ms,
                response_bytes,
                provider_request_id,
                detail_json,
                actual_nano_usd,
                now,
                attempt_id,
            ),
        ).rowcount
        if changed != 1:
            raise RuntimeError(f"attempt {attempt_id} could not be finished")
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()


def attempts_for_run(
    connection: sqlite3.Connection, *, run_id: int
) -> tuple[sqlite3.Row, ...]:
    return tuple(
        connection.execute(
            "SELECT * FROM attempt WHERE run_id = ? ORDER BY id", (run_id,)
        )
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_attempts.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/repository.py tests/run_engine/test_attempts.py
git commit -m "feat(runs): persist immutable attempt attribution"
```

---

## Task 13: Bounded Scheduler

Independent external calls run in a bounded `ThreadPoolExecutor`. The scheduler owns submission and the in-flight cap; adapters do not. Work is claimed on the application thread before submission and results are persisted on the application thread after completion, so no worker ever holds a `sqlite3.Connection`.

**Files:**
- Create: `src/notable_person_finder/runs/scheduler.py`
- Test: `tests/run_engine/test_scheduler.py`

**Interfaces:**
- Consumes: nothing beyond the standard library.
- Produces: `Completion[I, R]` frozen dataclass (`item`, `result`, `error`); `BoundedScheduler(max_workers)` with `run(items: Iterable[I], worker: Callable[[I], R]) -> Iterator[Completion[I, R]]` and context-manager support.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_scheduler.py`:

```python
from __future__ import annotations

import threading

import pytest

from notable_person_finder.runs.scheduler import BoundedScheduler, Completion


def test_all_items_are_processed_and_results_returned() -> None:
    with BoundedScheduler(max_workers=3) as scheduler:
        completions = list(scheduler.run(range(5), lambda value: value * 2))
    assert sorted(c.result for c in completions) == [0, 2, 4, 6, 8]
    assert all(c.error is None for c in completions)


def test_completion_carries_the_originating_item() -> None:
    with BoundedScheduler(max_workers=2) as scheduler:
        completions = list(scheduler.run(["a", "b"], str.upper))
    assert {(c.item, c.result) for c in completions} == {("a", "A"), ("b", "B")}


def test_in_flight_work_never_exceeds_the_worker_limit() -> None:
    in_flight = 0
    peak = 0
    guard = threading.Lock()
    release = threading.Event()

    def worker(value: int) -> int:
        nonlocal in_flight, peak
        with guard:
            in_flight += 1
            peak = max(peak, in_flight)
        release.wait(timeout=2)
        with guard:
            in_flight -= 1
        return value

    with BoundedScheduler(max_workers=2) as scheduler:
        results = scheduler.run(range(10), worker)
        release.set()
        list(results)
    assert peak <= 2


def test_a_worker_failure_is_returned_not_raised() -> None:
    def worker(value: int) -> int:
        if value == 2:
            raise ValueError("bad item")
        return value

    with BoundedScheduler(max_workers=2) as scheduler:
        completions = list(scheduler.run(range(4), worker))

    failures = [c for c in completions if c.error is not None]
    assert len(failures) == 1
    assert isinstance(failures[0].error, ValueError)
    assert sorted(c.result for c in completions if c.error is None) == [0, 1, 3]


def test_one_failure_does_not_cancel_unrelated_work() -> None:
    def worker(value: int) -> int:
        if value % 2 == 0:
            raise RuntimeError("even values fail")
        return value

    with BoundedScheduler(max_workers=4) as scheduler:
        completions = list(scheduler.run(range(10), worker))
    assert len(completions) == 10
    assert sorted(c.result for c in completions if c.error is None) == [1, 3, 5, 7, 9]


def test_results_are_yielded_on_the_calling_thread() -> None:
    caller = threading.get_ident()
    observed: list[int] = []

    with BoundedScheduler(max_workers=3) as scheduler:
        for completion in scheduler.run(range(6), lambda value: value):
            observed.append(threading.get_ident())
    assert set(observed) == {caller}


def test_workers_run_off_the_calling_thread() -> None:
    caller = threading.get_ident()
    worker_threads: set[int] = set()

    def worker(value: int) -> int:
        worker_threads.add(threading.get_ident())
        return value

    with BoundedScheduler(max_workers=2) as scheduler:
        list(scheduler.run(range(6), worker))
    assert caller not in worker_threads


def test_an_empty_item_stream_completes_immediately() -> None:
    with BoundedScheduler(max_workers=2) as scheduler:
        assert list(scheduler.run([], lambda value: value)) == []


def test_single_worker_serializes_work() -> None:
    order: list[int] = []
    with BoundedScheduler(max_workers=1) as scheduler:
        list(scheduler.run(range(4), lambda value: order.append(value) or value))
    assert order == [0, 1, 2, 3]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_scheduler.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.scheduler'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/runs/scheduler.py`:

```python
from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from types import TracebackType


@dataclass(frozen=True, slots=True)
class Completion[I, R]:
    item: I
    result: R | None
    error: BaseException | None


class BoundedScheduler:
    """Runs independent external calls with a hard in-flight cap.

    Results are yielded on the calling thread. Callers claim work and persist
    results there too, so a worker thread never touches SQLite.
    """

    def __init__(self, max_workers: int) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        self._max_workers = max_workers
        self._executor = ThreadPoolExecutor(max_workers=max_workers)

    def __enter__(self) -> BoundedScheduler:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._executor.shutdown(wait=True)

    def run[I, R](
        self, items: Iterable[I], worker: Callable[[I], R]
    ) -> Iterator[Completion[I, R]]:
        pending: dict[Future[R], I] = {}
        stream = iter(items)
        exhausted = False

        while True:
            while not exhausted and len(pending) < self._max_workers:
                try:
                    item = next(stream)
                except StopIteration:
                    exhausted = True
                    break
                pending[self._executor.submit(worker, item)] = item

            if not pending:
                return

            done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
            for future in done:
                item = pending.pop(future)
                error = future.exception()
                yield Completion(
                    item=item,
                    result=None if error is not None else future.result(),
                    error=error,
                )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_scheduler.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/scheduler.py tests/run_engine/test_scheduler.py
git commit -m "feat(runs): add bounded scheduler for independent external calls"
```

---

## Task 14: Structured Redacted Logging

Routine logs carry run, work, and attempt IDs, provider operation, destination host, duration, retry ordinal, byte counts, and typed outcome — and never carry credentials, headers, bodies, prompts, complete queries, or full article URLs. Redaction is a filter on the handler, so no code path can accidentally bypass it.

**Files:**
- Create: `src/notable_person_finder/obs/__init__.py`
- Create: `src/notable_person_finder/obs/logging.py`
- Test: `tests/run_engine/test_logging.py`

**Interfaces:**
- Consumes: `LoggingConfig`, `Credentials`.
- Produces: `EVENT_LOGGER_NAME`; `configure_logging(log_file, config, *, secrets) -> logging.Logger`; `log_event(logger, event, **fields) -> None`; `redact(text, secrets) -> str`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_logging.py`:

```python
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from notable_person_finder.config.models import LoggingConfig
from notable_person_finder.obs.logging import configure_logging, log_event, redact


@pytest.fixture
def logger(tmp_path: Path) -> logging.Logger:
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=2048, backup_count=2),
        secrets=("or-secret-value", "brave-secret-value"),
    )
    yield created
    for handler in list(created.handlers):
        handler.close()
        created.removeHandler(handler)


def read_events(tmp_path: Path) -> list[dict[str, object]]:
    text = (tmp_path / "logs" / "notable.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line]


def test_events_are_json_lines_with_the_required_envelope(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_attempt_finished", run_id=1, attempt_id=9, outcome="succeeded")
    event = read_events(tmp_path)[0]
    assert event["event"] == "provider_attempt_finished"
    assert event["severity"] == "INFO"
    assert event["timestamp"].endswith("Z")
    assert event["run_id"] == 1
    assert event["outcome"] == "succeeded"


def test_log_directory_is_created(tmp_path: Path, logger: logging.Logger) -> None:
    log_event(logger, "run_started", run_id=1)
    assert (tmp_path / "logs" / "notable.jsonl").exists()


def test_provider_fields_are_recorded(tmp_path: Path, logger: logging.Logger) -> None:
    log_event(
        logger,
        "provider_attempt_finished",
        run_id=1,
        work_item_id=4,
        attempt_id=9,
        provider="brave",
        operation="search_web",
        destination_host="api.search.brave.com",
        duration_ms=310,
        retry_ordinal=2,
        response_bytes=2048,
        outcome="failed",
        failure_category="rate_limit",
    )
    event = read_events(tmp_path)[0]
    assert event["provider"] == "brave"
    assert event["destination_host"] == "api.search.brave.com"
    assert event["retry_ordinal"] == 2
    assert event["failure_category"] == "rate_limit"


def test_a_secret_value_is_redacted_from_any_field(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_attempt_failed", detail="key=or-secret-value rejected")
    event = read_events(tmp_path)[0]
    assert "or-secret-value" not in json.dumps(event)
    assert "[redacted]" in event["detail"]


def test_a_secret_value_is_redacted_from_the_message_of_an_exception(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "internal_error", detail="Bearer brave-secret-value")
    assert "brave-secret-value" not in (tmp_path / "logs" / "notable.jsonl").read_text()


def test_authorization_like_fields_are_dropped_entirely(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_request", authorization="Bearer abc", api_key="xyz")
    event = read_events(tmp_path)[0]
    assert "authorization" not in event
    assert "api_key" not in event


def test_rotation_keeps_the_configured_number_of_files(
    tmp_path: Path, logger: logging.Logger
) -> None:
    for index in range(400):
        log_event(logger, "run_progress", run_id=1, index=index, note="x" * 50)
    files = sorted(p.name for p in (tmp_path / "logs").iterdir())
    assert "notable.jsonl" in files
    assert len(files) <= 3  # the live file plus backup_count=2


def test_redact_replaces_every_occurrence() -> None:
    assert redact("a secret and secret again", ("secret",)) == "a [redacted] and [redacted] again"


def test_redact_ignores_empty_secrets() -> None:
    assert redact("unchanged", ("", None)) == "unchanged"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_logging.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.obs'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/obs/__init__.py` as an empty file.

Create `src/notable_person_finder/obs/logging.py`:

```python
from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from notable_person_finder.config.models import LoggingConfig

EVENT_LOGGER_NAME = "notable"
REDACTION = "[redacted]"

# Field names that must never be logged even when a caller passes them.
_FORBIDDEN_FIELDS = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "cookie",
        "prompt",
        "response_body",
        "body",
        "query",
        "url",
    }
)

_ENVELOPE_FIELDS = frozenset({"event", "severity", "timestamp"})


def redact(text: str, secrets: Sequence[str | None]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTION)
    return text


class _RedactingJsonFormatter(logging.Formatter):
    def __init__(self, secrets: Sequence[str | None]) -> None:
        super().__init__()
        self._secrets = tuple(secret for secret in secrets if secret)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "severity": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
        }
        for name, value in getattr(record, "fields", {}).items():
            if name.lower() in _FORBIDDEN_FIELDS or name in _ENVELOPE_FIELDS:
                continue
            payload[name] = value

        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return redact(rendered, self._secrets)


def configure_logging(
    log_file: Path, config: LoggingConfig, *, secrets: Sequence[str | None]
) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(EVENT_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=config.max_bytes,
        backupCount=config.backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(_RedactingJsonFormatter(secrets))
    logger.addHandler(handler)
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    severity: int = logging.INFO,
    **fields: Any,
) -> None:
    logger.log(severity, event, extra={"event": event, "fields": fields})
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_logging.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/obs tests/run_engine/test_logging.py
git commit -m "feat(obs): add rotating JSON Lines logging with secret redaction"
```

---

## Task 15: Run Engine and Terminal-State Derivation

The engine assembles the pieces: sweep, create run, claim eligible work, execute each handler through the retry coordinator, persist attempts and outcomes on the application thread, then derive the terminal state.

Terminal-state derivation is a pure function tested independently of SQLite. A valid empty result is `complete`; pending or deferred required work is `partial`; a permanent required failure is `partial` when other meaningful results exist and `failed` when it prevents meaningful work; and a reporting failure is always `failed`. The engine does not store a terminal state until its injected reporter has durably written the artifact.

**Files:**
- Create: `src/notable_person_finder/runs/engine.py`
- Test: `tests/run_engine/test_engine.py`

**Interfaces:**
- Consumes: everything from Tasks 4 and 8–14.
- Produces: `TaskHandler`, `TaskOutcome`, `ReportArtifact`, and `RunReport` frozen dataclasses; `derive_run_state(...) -> RunState`; `RunEngine(reporter=...)` with `execute(handlers) -> RunReport`. The reporter returns the persisted path, hash, and exact Markdown; the engine then records the one terminal transition.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_engine.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
    derive_run_state,
)
from notable_person_finder.runs.models import RunState, WorkState
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

NOW = "2026-07-25T06:00:00Z"


# --- pure terminal-state rules -------------------------------------------------

def test_no_outstanding_required_work_is_complete() -> None:
    assert derive_run_state(
        required_pending=0, required_deferred=0, required_failed_permanent=0,
        meaningful_results=False, reporting_failed=False
    ) is RunState.COMPLETE


def test_an_empty_run_is_complete_not_failed() -> None:
    assert derive_run_state(
        required_pending=0, required_deferred=0, required_failed_permanent=0,
        meaningful_results=False, reporting_failed=False
    ) is RunState.COMPLETE


def test_deferred_required_work_makes_the_run_partial() -> None:
    assert derive_run_state(
        required_pending=0, required_deferred=2, required_failed_permanent=0,
        meaningful_results=True, reporting_failed=False
    ) is RunState.PARTIAL


def test_unevaluated_required_work_makes_the_run_partial() -> None:
    assert derive_run_state(
        required_pending=3, required_deferred=0, required_failed_permanent=0,
        meaningful_results=True, reporting_failed=False
    ) is RunState.PARTIAL


def test_a_reporting_failure_makes_the_run_failed() -> None:
    assert derive_run_state(
        required_pending=0, required_deferred=0, required_failed_permanent=0,
        meaningful_results=True, reporting_failed=True
    ) is RunState.FAILED
    assert derive_run_state(
        required_pending=1, required_deferred=1, required_failed_permanent=0,
        meaningful_results=True, reporting_failed=True
    ) is RunState.FAILED


def test_required_permanent_failure_is_never_complete() -> None:
    assert derive_run_state(
        required_pending=0, required_deferred=0, required_failed_permanent=1,
        meaningful_results=True, reporting_failed=False
    ) is RunState.PARTIAL
    assert derive_run_state(
        required_pending=0, required_deferred=0, required_failed_permanent=1,
        meaningful_results=False, reporting_failed=False
    ) is RunState.FAILED


# --- engine behaviour ----------------------------------------------------------

@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, tmp_path / "backups")
    connection.close()
    return path


def memory_reporter(report) -> ReportArtifact:
    return ReportArtifact(path=None, sha256=None, markdown="")


def build_engine(
    connection: sqlite3.Connection,
    clock: FakeClock,
    *,
    budget_limit_nano_usd: int | None = None,
) -> RunEngine:
    return RunEngine(
        connection,
        retry=RetryCoordinator(
            RetryConfig(max_attempts=2, jitter_ratio=0.0), clock=clock
        ),
        scheduler=BoundedScheduler(max_workers=2),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=budget_limit_nano_usd,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=memory_reporter,
    )


def succeeding_handler(calls: list[int]) -> TaskHandler:
    def execute(work_item, ordinal: int) -> TaskOutcome:
        calls.append(work_item.id)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    return TaskHandler(
        task_type="probe",
        provider="probe_provider",
        operation="probe_call",
        execute=execute,
    )


def schedule_probe(connection: sqlite3.Connection, fingerprint: str, *, required: bool = True) -> int:
    return repository.schedule_work(
        connection,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint=fingerprint,
        required=required,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )


def test_a_run_with_no_work_is_complete(database: Path) -> None:
    connection = connect_database(database)
    report = build_engine(connection, FakeClock()).execute({})
    assert report.state is RunState.COMPLETE
    assert report.counters.required_succeeded == 0
    connection.close()


def test_eligible_work_is_executed_and_marked_succeeded(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "b" * 64)
    calls: list[int] = []
    handler = succeeding_handler(calls)

    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert calls == [work_id]
    assert report.state is RunState.COMPLETE
    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (work_id,)).fetchone()[
            "state"
        ]
        == WorkState.SUCCEEDED
    )
    connection.close()


def test_run_counters_do_not_include_historical_work_or_attempts(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "7" * 64)
    handler = succeeding_handler([])
    first = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    second = build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    assert first.counters.required_succeeded == 1
    assert second.counters.required_succeeded == 0
    assert second.counters.operational_failures == 0
    connection.close()


def test_each_call_persists_one_attributed_attempt(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "c" * 64)
    handler = succeeding_handler([])
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    attempts = repository.attempts_for_run(connection, run_id=report.run_id)
    assert len(attempts) == 1
    assert attempts[0]["provider"] == "probe_provider"
    assert attempts[0]["outcome"] == "succeeded"
    assert attempts[0]["ordinal"] == 1
    connection.close()


def test_budget_is_reserved_before_the_call_and_actual_cost_is_reconciled(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "8" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        row = connection.execute(
            "SELECT budget_reserved_nano_usd FROM run ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row["budget_reserved_nano_usd"] == 400
        return TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None, actual_nano_usd=120
        )

    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=execute,
        reserved_nano_usd=400,
    )
    report = build_engine(
        connection, FakeClock(), budget_limit_nano_usd=1000
    ).execute({handler.task_type: handler})
    row = connection.execute(
        "SELECT budget_reserved_nano_usd, budget_actual_nano_usd FROM run WHERE id = ?",
        (report.run_id,),
    ).fetchone()
    assert (row["budget_reserved_nano_usd"], row["budget_actual_nano_usd"]) == (120, 120)
    assert connection.execute(
        "SELECT actual_nano_usd FROM attempt WHERE run_id = ?", (report.run_id,)
    ).fetchone()["actual_nano_usd"] == 120
    connection.close()


def test_refused_budget_reservation_makes_no_external_call(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "9" * 64)
    calls: list[int] = []
    handler = TaskHandler(
        task_type="probe",
        provider="openrouter",
        operation="generate_structured",
        execute=lambda work, ordinal: calls.append(ordinal) or TaskOutcome(
            state=WorkState.SUCCEEDED, reason=None
        ),
        reserved_nano_usd=200,
    )
    report = build_engine(
        connection, FakeClock(), budget_limit_nano_usd=100
    ).execute({handler.task_type: handler})
    assert calls == []
    assert report.state is RunState.PARTIAL
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == WorkState.DEFERRED
    assert repository.attempts_for_run(connection, run_id=report.run_id) == ()
    connection.close()


def test_a_transient_failure_is_retried_and_both_attempts_persist(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "d" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        if ordinal == 1:
            raise ProviderFailure(
                FailureCategory.TRANSIENT_SERVER_ERROR,
                provider="probe_provider",
                operation="probe_call",
                status_code=503,
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    outcomes = [row["outcome"] for row in repository.attempts_for_run(connection, run_id=report.run_id)]
    assert outcomes == ["failed", "succeeded"]
    assert report.state is RunState.COMPLETE
    connection.close()


def test_exhausted_transient_work_is_deferred_and_the_run_is_partial(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "e" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.TIMEOUT, provider="probe_provider", operation="probe_call"
        )

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert report.state is RunState.PARTIAL
    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (work_id,)).fetchone()[
            "state"
        ]
        == WorkState.DEFERRED
    )
    connection.close()


def test_a_permanent_failure_without_useful_results_fails_the_run(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule_probe(connection, "f" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.AUTHENTICATION,
            provider="probe_provider",
            operation="probe_call",
            status_code=401,
        )

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    report = build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (work_id,)).fetchone()[
            "state"
        ]
        == WorkState.FAILED_PERMANENT
    )
    assert report.state is RunState.FAILED
    connection.close()


def test_one_item_failure_does_not_stop_unrelated_work(database: Path) -> None:
    connection = connect_database(database)
    good = schedule_probe(connection, "1" * 64)
    bad = schedule_probe(connection, "2" * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        if work_item.id == bad:
            raise ProviderFailure(
                FailureCategory.ACCESS_DENIED,
                provider="probe_provider",
                operation="probe_call",
                status_code=403,
            )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})

    states = {
        row["id"]: row["state"]
        for row in connection.execute("SELECT id, state FROM work_item")
    }
    assert states[good] == WorkState.SUCCEEDED
    assert states[bad] == WorkState.FAILED_PERMANENT
    connection.close()


def test_work_for_a_paused_provider_is_deferred(database: Path) -> None:
    connection = connect_database(database)
    for index in range(3):
        schedule_probe(connection, str(index) * 64)

    def execute(work_item, ordinal: int) -> TaskOutcome:
        raise ProviderFailure(
            FailureCategory.PROVIDER_UNAVAILABLE,
            provider="probe_provider",
            operation="probe_call",
            status_code=503,
        )

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryCoordinator(
            RetryConfig(
                max_attempts=1,
                jitter_ratio=0.0,
                provider_pause_after_consecutive_exhaustions=1,
            ),
            clock=clock,
        ),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
    )
    report = engine.execute({handler.task_type: handler})

    assert report.paused_providers == frozenset({"probe_provider"})
    deferred = connection.execute(
        "SELECT COUNT(*) AS n FROM work_item WHERE state = 'deferred'"
    ).fetchone()["n"]
    assert deferred == 3
    # Only one provider request was made; the other two were deferred unmade.
    assert len(repository.attempts_for_run(connection, run_id=report.run_id)) == 1
    connection.close()


def test_the_engine_sweeps_an_abandoned_predecessor_first(database: Path) -> None:
    connection = connect_database(database)
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    abandoned = repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-23T06:00:00Z",
        window_end="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        now="2026-07-24T06:00:00Z",
    )
    report = build_engine(connection, FakeClock()).execute({})

    assert report.interrupted_runs == (abandoned,)
    assert repository.load_run(connection, run_id=abandoned).state is RunState.INTERRUPTED
    assert report.run_id != abandoned
    connection.close()


def test_no_transaction_is_open_while_a_handler_runs(database: Path) -> None:
    connection = connect_database(database)
    schedule_probe(connection, "3" * 64)
    observed: list[bool] = []

    def execute(work_item, ordinal: int) -> TaskOutcome:
        observed.append(connection.in_transaction)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    build_engine(connection, FakeClock()).execute({handler.task_type: handler})
    assert observed == [False]
    connection.close()


def test_work_without_a_registered_handler_stays_pending(database: Path) -> None:
    connection = connect_database(database)
    work_id = repository.schedule_work(
        connection,
        task_type="unknown_task",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="4" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )
    report = build_engine(connection, FakeClock()).execute({})
    assert report.state is RunState.PARTIAL
    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (work_id,)).fetchone()[
            "state"
        ]
        == WorkState.PENDING
    )
    connection.close()


def test_reporting_failure_is_the_only_terminal_transition(database: Path) -> None:
    connection = connect_database(database)

    def fail_reporting(report) -> ReportArtifact:
        raise OSError("digest root unavailable")

    clock = FakeClock()
    engine = RunEngine(
        connection,
        retry=RetryCoordinator(RetryConfig(max_attempts=1), clock=clock),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=fail_reporting,
    )
    with pytest.raises(OSError, match="digest root"):
        engine.execute({})
    latest = repository.latest_run(connection)
    assert latest is not None and latest.state is RunState.FAILED
    transitions = [
        row["state"]
        for row in connection.execute(
            "SELECT state FROM run_transition WHERE run_id = ? ORDER BY id", (latest.id,)
        )
    ]
    assert transitions == ["running", "failed"]
    connection.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_engine.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.runs.engine'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/runs/engine.py`:

```python
from __future__ import annotations

import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from notable_person_finder.providers.failures import ProviderFailure, ProviderPaused
from notable_person_finder.runs import repository
from notable_person_finder.runs.budget import BudgetExhausted
from notable_person_finder.runs.clock import Clock, utc_timestamp
from notable_person_finder.runs.models import (
    RunCounters,
    RunState,
    WorkItem,
    WorkState,
)
from notable_person_finder.runs.retry import (
    AttemptRecord,
    RetryCoordinator,
    RetryExhausted,
)
from notable_person_finder.runs.scheduler import BoundedScheduler


@dataclass(frozen=True, slots=True)
class TaskOutcome:
    state: WorkState
    reason: str | None
    response_bytes: int | None = None
    provider_request_id: str | None = None
    detail_json: str | None = None
    actual_nano_usd: int | None = None


@dataclass(frozen=True, slots=True)
class TaskHandler:
    task_type: str
    provider: str
    operation: str
    execute: Callable[[WorkItem, int], TaskOutcome]
    request_fingerprint: Callable[[WorkItem], str] | None = None
    reserved_nano_usd: int = 0


@dataclass(frozen=True, slots=True)
class ReportArtifact:
    path: str | None
    sha256: str | None
    markdown: str


@dataclass(frozen=True, slots=True)
class RunReport:
    run_id: int
    state: RunState
    started_at: str
    finished_at: str
    timezone: str
    window_start: str
    window_end: str
    counters: RunCounters
    paused_providers: frozenset[str]
    interrupted_runs: tuple[int, ...] = ()
    failure_categories: Mapping[str, int] = field(default_factory=dict)

    @property
    def human_id(self) -> str:
        return f"run-{self.run_id}"


def derive_run_state(
    *,
    required_pending: int,
    required_deferred: int,
    required_failed_permanent: int,
    meaningful_results: bool,
    reporting_failed: bool,
) -> RunState:
    """Derive state from this run's durable outcomes and reporting result."""
    if reporting_failed:
        return RunState.FAILED
    if required_failed_permanent > 0:
        return RunState.PARTIAL if meaningful_results else RunState.FAILED
    if required_pending > 0 or required_deferred > 0:
        return RunState.PARTIAL
    return RunState.COMPLETE


class RunEngine:
    """Owns the run lifecycle: sweep, claim, execute, persist, and report."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        *,
        retry: RetryCoordinator,
        scheduler: BoundedScheduler,
        clock: Clock,
        timezone: str,
        window_start: str,
        budget_limit_nano_usd: int | None,
        snapshot_fingerprint: str,
        snapshot_json: str,
        reporter: Callable[[RunReport], ReportArtifact],
    ) -> None:
        self._connection = connection
        self._retry = retry
        self._scheduler = scheduler
        self._clock = clock
        self._timezone = timezone
        self._window_start = window_start
        self._budget_limit_nano_usd = budget_limit_nano_usd
        self._snapshot_fingerprint = snapshot_fingerprint
        self._snapshot_json = snapshot_json
        self._reporter = reporter

    def _now(self) -> str:
        return utc_timestamp(self._clock.now())

    def execute(self, handlers: Mapping[str, TaskHandler]) -> RunReport:
        started_at = self._now()
        sweep = repository.sweep_interrupted(self._connection, now=started_at)

        snapshot_id = repository.store_snapshot(
            self._connection,
            fingerprint=self._snapshot_fingerprint,
            canonical_json=self._snapshot_json,
            now=started_at,
        )
        run_id = repository.create_run(
            self._connection,
            snapshot_id=snapshot_id,
            timezone=self._timezone,
            window_start=self._window_start,
            window_end=started_at,
            budget_limit_nano_usd=self._budget_limit_nano_usd,
            now=started_at,
        )

        failure_categories: dict[str, int] = {}
        while True:
            item = repository.next_eligible(
                self._connection,
                run_id=run_id,
                now=self._now(),
                task_types=handlers.keys(),
            )
            if item is None:
                break
            handler = handlers[item.task_type]
            self._perform(run_id, item, handler, failure_categories)

        finished_at = self._now()
        counters = repository.run_counters(
            self._connection, run_id=run_id, now=finished_at
        )
        meaningful_results = (
            counters.required_succeeded > 0 or counters.optional_succeeded > 0
        )
        state = derive_run_state(
            required_pending=counters.required_pending,
            required_deferred=counters.required_deferred,
            required_failed_permanent=counters.required_failed_permanent,
            meaningful_results=meaningful_results,
            reporting_failed=False,
        )
        record = repository.load_run(self._connection, run_id=run_id)
        report = RunReport(
            run_id=run_id,
            state=state,
            started_at=record.started_at,
            finished_at=finished_at,
            timezone=record.timezone,
            window_start=record.window_start,
            window_end=record.window_end,
            counters=counters,
            paused_providers=self._retry.paused_providers(),
            interrupted_runs=sweep.runs,
            failure_categories=failure_categories,
        )
        try:
            artifact = self._reporter(report)
        except Exception as error:
            repository.finish_run(
                self._connection,
                run_id=run_id,
                state=RunState.FAILED,
                reason=f"reporting failed: {type(error).__name__}",
                digest_path=None,
                digest_sha256=None,
                now=self._now(),
            )
            raise
        repository.finish_run(
            self._connection,
            run_id=run_id,
            state=state,
            reason=None,
            digest_path=artifact.path,
            digest_sha256=artifact.sha256,
            now=finished_at,
        )
        return report

    def _perform(
        self,
        run_id: int,
        work_item: WorkItem,
        handler: TaskHandler,
        failure_categories: dict[str, int],
    ) -> None:
        fingerprint = (
            handler.request_fingerprint(work_item)
            if handler.request_fingerprint is not None
            else work_item.fingerprint
        )
        attempt_ids: dict[int, int] = {}
        starting_ordinal = repository.next_attempt_ordinal(
            self._connection, work_item_id=work_item.id
        )

        last_outcome: TaskOutcome | None = None

        def action(ordinal: int) -> TaskOutcome:
            nonlocal last_outcome
            # Persist the attempt before the call so a crash leaves evidence.
            start = (
                repository.claim_and_start_attempt
                if ordinal == starting_ordinal
                else repository.start_attempt
            )
            attempt_ids[ordinal] = start(
                self._connection,
                run_id=run_id,
                work_item_id=work_item.id,
                provider=handler.provider,
                operation=handler.operation,
                ordinal=ordinal,
                request_fingerprint=fingerprint,
                destination_host=None,
                reserved_nano_usd=handler.reserved_nano_usd,
                now=self._now(),
            )
            last_outcome = handler.execute(work_item, ordinal)
            return last_outcome

        def on_attempt(record: AttemptRecord) -> None:
            # A failed attempt has no outcome to persist; only a success does.
            outcome = last_outcome if record.outcome == "succeeded" else None
            if record.failure_category is not None:
                key = str(record.failure_category)
                failure_categories[key] = failure_categories.get(key, 0) + 1
            repository.finish_attempt(
                self._connection,
                attempt_id=attempt_ids[record.ordinal],
                record=record,
                response_bytes=None if outcome is None else outcome.response_bytes,
                provider_request_id=None if outcome is None else outcome.provider_request_id,
                detail_json=None if outcome is None else outcome.detail_json,
                now=self._now(),
                actual_nano_usd=None if outcome is None else outcome.actual_nano_usd,
            )

        try:
            result = self._retry.call(
                handler.provider,
                handler.operation,
                action,
                on_attempt=on_attempt,
                starting_ordinal=starting_ordinal,
            )
        except ProviderPaused as paused:
            repository.complete_work(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                state=WorkState.DEFERRED,
                reason=f"{paused.provider} paused for this run",
                now=self._now(),
            )
            return
        except RetryExhausted as exhausted:
            repository.complete_work(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                state=WorkState.DEFERRED,
                reason=f"exhausted transient failure: {exhausted.last_failure.category}",
                now=self._now(),
            )
            return
        except BudgetExhausted:
            repository.complete_work(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                state=WorkState.DEFERRED,
                reason="not_evaluated_budget",
                now=self._now(),
            )
            return
        except ProviderFailure as failure:
            repository.complete_work(
                self._connection,
                work_item_id=work_item.id,
                run_id=run_id,
                state=WorkState.FAILED_PERMANENT,
                reason=str(failure.category),
                now=self._now(),
            )
            return

        repository.complete_work(
            self._connection,
            work_item_id=work_item.id,
            run_id=run_id,
            state=result.state,
            reason=result.reason,
            now=self._now(),
        )
```

The `BoundedScheduler` is held by the engine for milestones 3–6, which submit independent per-subject calls through it. Milestone 2's synthetic handlers execute sequentially; the scheduler's own bounded-concurrency contract is proven by Task 13.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_engine.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/runs/engine.py tests/run_engine/test_engine.py
git commit -m "feat(runs): add run engine and terminal-state derivation"
```

---

## Task 16: Digest Writer

Every complete or partial run atomically writes one immutable dated Markdown digest, records its path and SHA-256 content hash, and replaces the enabled-by-default `latest.md` copy. A failed run still writes a digest that prominently says so whenever durable state permits.

This milestone renders the header and the operational summary. Milestone 6 inserts the ranked shortlist between them.

**Files:**
- Create: `src/notable_person_finder/reporting/__init__.py`
- Create: `src/notable_person_finder/reporting/digest.py`
- Test: `tests/run_engine/test_digest.py`

**Interfaces:**
- Consumes: `RunReport`, `RunState`, `DigestConfig`.
- Produces: `DigestRecord` frozen dataclass (`path`, `sha256`, `markdown`); `render_digest(report, *, local_date) -> str`; `write_digest(digests_dir, report, *, local_date, config) -> DigestRecord`; `DigestWriteError`.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_digest.py`:

```python
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from notable_person_finder.config.models import DigestConfig
from notable_person_finder.reporting.digest import (
    DigestWriteError,
    render_digest,
    write_digest,
)
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState

COUNTERS = RunCounters(
    required_succeeded=4,
    required_pending=0,
    required_deferred=0,
    required_failed_permanent=0,
    optional_succeeded=1,
    optional_skipped=0,
    operational_failures=0,
)


def report(state: RunState = RunState.COMPLETE, **overrides: object) -> RunReport:
    defaults: dict[str, object] = {
        "run_id": 42,
        "state": state,
        "started_at": "2026-07-25T06:00:00Z",
        "finished_at": "2026-07-25T06:04:00Z",
        "timezone": "Europe/Paris",
        "window_start": "2026-07-24T06:00:00Z",
        "window_end": "2026-07-25T06:00:00Z",
        "counters": COUNTERS,
        "paused_providers": frozenset(),
        "interrupted_runs": (),
        "failure_categories": {},
    }
    defaults.update(overrides)
    return RunReport(**defaults)  # type: ignore[arg-type]


def test_header_states_run_identity_window_and_timezone() -> None:
    markdown = render_digest(report(), local_date="2026-07-25")
    assert markdown.startswith("# Notable Person Finder — 2026-07-25\n")
    assert "run-42" in markdown
    assert "Europe/Paris" in markdown
    assert "2026-07-24T06:00:00Z" in markdown


def test_a_complete_empty_run_is_unambiguously_successful() -> None:
    empty = RunCounters(0, 0, 0, 0, 0, 0, 0)
    markdown = render_digest(report(counters=empty), local_date="2026-07-25")
    assert "**State:** complete" in markdown
    assert "No candidates met the shortlist criteria in this window." in markdown
    assert "fail" not in markdown.lower()


def test_a_partial_run_warns_prominently_and_names_the_audit_command() -> None:
    counters = RunCounters(2, 1, 3, 0, 0, 0, 5)
    markdown = render_digest(report(RunState.PARTIAL, counters=counters), local_date="2026-07-25")
    warning_line = markdown.splitlines()[2]
    assert "PARTIAL" in warning_line
    assert "notable audit run 42" in markdown


def test_a_failed_run_says_so_prominently() -> None:
    markdown = render_digest(report(RunState.FAILED), local_date="2026-07-25")
    assert "FAILED" in markdown.splitlines()[2]


def test_paused_providers_appear_in_the_summary() -> None:
    markdown = render_digest(
        report(RunState.PARTIAL, paused_providers=frozenset({"brave"})), local_date="2026-07-25"
    )
    assert "brave" in markdown


def test_failure_categories_are_summarized_by_safe_category() -> None:
    markdown = render_digest(
        report(failure_categories={"rate_limit": 3, "timeout": 1}), local_date="2026-07-25"
    )
    assert "rate_limit" in markdown
    assert "timeout" in markdown


def test_an_interrupted_predecessor_is_reported() -> None:
    markdown = render_digest(report(interrupted_runs=(41,)), local_date="2026-07-25")
    assert "run-41" in markdown


def test_digest_ends_with_exactly_one_trailing_newline() -> None:
    markdown = render_digest(report(), local_date="2026-07-25")
    assert markdown.endswith("\n")
    assert not markdown.endswith("\n\n")


def test_digest_is_written_with_the_dated_run_filename(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert record.path == tmp_path / "2026-07-25-run-42.md"
    assert record.path.read_text(encoding="utf-8") == record.markdown


def test_content_hash_matches_the_written_bytes(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert record.sha256 == hashlib.sha256(record.path.read_bytes()).hexdigest()


def test_latest_copy_is_written_by_default(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    latest = tmp_path / "latest.md"
    assert latest.is_file()
    assert not latest.is_symlink()
    assert latest.read_text(encoding="utf-8") == record.markdown


def test_latest_copy_can_be_disabled(tmp_path: Path) -> None:
    write_digest(
        tmp_path, report(), local_date="2026-07-25", config=DigestConfig(write_latest_copy=False)
    )
    assert not (tmp_path / "latest.md").exists()


def test_latest_always_represents_the_newest_attempt(tmp_path: Path) -> None:
    write_digest(tmp_path, report(RunState.COMPLETE), local_date="2026-07-25", config=DigestConfig())
    write_digest(
        tmp_path,
        report(RunState.FAILED, run_id=43),
        local_date="2026-07-25",
        config=DigestConfig(),
    )
    assert "FAILED" in (tmp_path / "latest.md").read_text(encoding="utf-8")


def test_dated_digests_are_immutable_across_runs(tmp_path: Path) -> None:
    first = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    write_digest(
        tmp_path, report(run_id=43), local_date="2026-07-25", config=DigestConfig()
    )
    assert first.path.read_text(encoding="utf-8") == first.markdown
    assert (tmp_path / "2026-07-25-run-43.md").is_file()


def test_an_unwritable_digest_root_raises_a_typed_error(tmp_path: Path) -> None:
    blocked = tmp_path / "digests"
    blocked.write_text("not a directory", encoding="utf-8")
    with pytest.raises(DigestWriteError):
        write_digest(blocked, report(), local_date="2026-07-25", config=DigestConfig())


def test_no_partial_file_remains_after_a_failed_write(tmp_path: Path, monkeypatch) -> None:
    import os

    def failing_replace(src: object, dst: object) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", failing_replace)
    with pytest.raises(DigestWriteError):
        write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert list(tmp_path.iterdir()) == []
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_digest.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.reporting'`

- [ ] **Step 3: Write the implementation**

Create `src/notable_person_finder/reporting/__init__.py` as an empty file.

Create `src/notable_person_finder/reporting/digest.py`:

```python
from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from notable_person_finder.config.models import DigestConfig
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunState

_PROMINENT_STATES = {
    RunState.PARTIAL: "PARTIAL RUN — required work remains unevaluated.",
    RunState.FAILED: "FAILED RUN — this digest may be incomplete.",
    RunState.INTERRUPTED: "INTERRUPTED RUN — the process ended before finishing.",
}


class DigestWriteError(Exception):
    """The digest could not be persisted atomically."""


@dataclass(frozen=True, slots=True)
class DigestRecord:
    path: Path
    sha256: str
    markdown: str


def render_digest(report: RunReport, *, local_date: str) -> str:
    lines = [f"# Notable Person Finder — {local_date}", ""]

    warning = _PROMINENT_STATES.get(report.state)
    if warning is not None:
        lines.append(f"> **{warning}** See `notable audit run {report.run_id}`.")
    else:
        lines.append(f"> Run {report.human_id} completed normally.")
    lines.append("")

    lines += [
        f"**State:** {report.state}",
        f"**Run:** {report.human_id}",
        f"**Timezone:** {report.timezone}",
        f"**Observation window:** {report.window_start} to {report.window_end}",
        f"**Started:** {report.started_at}",
        f"**Finished:** {report.finished_at}",
        "",
        "## Shortlist",
        "",
    ]

    # Milestone 6 replaces this section with ranked entries and synthesis.
    lines += ["No candidates met the shortlist criteria in this window.", ""]

    counters = report.counters
    lines += [
        "## Operational summary",
        "",
        f"- Required work succeeded: {counters.required_succeeded}",
        f"- Required work still pending: {counters.required_pending}",
        f"- Required work deferred: {counters.required_deferred}",
        f"- Required work permanently failed: {counters.required_failed_permanent}",
        f"- Optional work succeeded: {counters.optional_succeeded}",
        f"- Operational failures: {counters.operational_failures}",
    ]

    if report.failure_categories:
        rendered = ", ".join(
            f"{category} ({count})"
            for category, count in sorted(report.failure_categories.items())
        )
        lines.append(f"- Failures by category: {rendered}")
    if report.paused_providers:
        lines.append(f"- Providers paused this run: {', '.join(sorted(report.paused_providers))}")
    if report.interrupted_runs:
        rendered = ", ".join(f"run-{run_id}" for run_id in report.interrupted_runs)
        lines.append(f"- Interrupted predecessor runs recorded: {rendered}")

    return "\n".join(lines) + "\n"


def _atomic_write(target: Path, markdown: str) -> None:
    handle, temporary_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(markdown)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException as error:
        temporary.unlink(missing_ok=True)
        raise DigestWriteError(f"could not write {target}: {error}") from error


def write_digest(
    digests_dir: Path,
    report: RunReport,
    *,
    local_date: str,
    config: DigestConfig,
) -> DigestRecord:
    """Atomically persist the immutable dated digest and the latest copy."""
    markdown = render_digest(report, local_date=local_date)
    try:
        digests_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise DigestWriteError(f"could not create {digests_dir}: {error}") from error

    target = digests_dir / f"{local_date}-{report.human_id}.md"
    _atomic_write(target, markdown)
    if config.write_latest_copy:
        _atomic_write(digests_dir / "latest.md", markdown)

    return DigestRecord(
        path=target,
        sha256=hashlib.sha256(markdown.encode("utf-8")).hexdigest(),
        markdown=markdown,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_digest.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/reporting tests/run_engine/test_digest.py
git commit -m "feat(reporting): write immutable dated digests with a latest copy"
```

---

## Task 17: `notable run`, `notable status`, and Exit Semantics

`notable run` validates configuration, acquires the mutation lock, migrates if needed, creates a run, executes eligible work, writes the digest to standard output and to disk, and records a terminal state. There is no `--force`, resume, stage, or retry flag.

`notable status` performs no work and acquires no lock. It arrives partially: digest backlog, oldest pending candidate, and queue tiers need milestone 6's digest queue and are deliberately absent.

Exit statuses: `0` complete, `1` failed (including validation, migration, storage, lock, and reporting), `2` partial, `64` usage, `130` `SIGINT`.

**Files:**
- Modify: `src/notable_person_finder/cli/main.py`
- Test: `tests/run_engine/test_run_cli.py`

**Interfaces:**
- Consumes: everything above.
- Produces: `command_run(config_file, *, verbose) -> int`, `command_status(config_file) -> int`, and `EXIT_*` constants.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_run_cli.py`:

```python
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from tests.run_engine.helpers import ENVIRONMENT, write_graph


def run_notable(
    config_file: Path, *arguments: str, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, **ENVIRONMENT, **(environment or {})}
    return subprocess.run(
        [sys.executable, "-m", "notable_person_finder", "--config", str(config_file), *arguments],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    return write_graph(tmp_path)


def test_run_on_a_fresh_database_is_complete(config_file: Path) -> None:
    completed = run_notable(config_file, "run")
    assert completed.returncode == 0, completed.stderr
    assert "# Notable Person Finder" in completed.stdout
    assert "**State:** complete" in completed.stdout


def test_run_stdout_is_byte_for_byte_the_persisted_digest(
    config_file: Path, tmp_path: Path
) -> None:
    completed = run_notable(config_file, "run")
    digests = tmp_path / "portable" / "data" / "digests"
    written = next(path for path in digests.iterdir() if path.name != "latest.md")
    assert completed.stdout == written.read_text(encoding="utf-8")


def test_run_creates_the_database_and_applies_migrations(
    config_file: Path, tmp_path: Path
) -> None:
    run_notable(config_file, "run")
    assert (tmp_path / "portable" / "data" / "notable.sqlite3").is_file()


def test_a_second_same_day_run_is_allowed_and_creates_another_run(
    config_file: Path, tmp_path: Path
) -> None:
    assert run_notable(config_file, "run").returncode == 0
    assert run_notable(config_file, "run").returncode == 0
    digests = tmp_path / "portable" / "data" / "digests"
    dated = [path for path in digests.iterdir() if path.name != "latest.md"]
    assert len(dated) == 2


def test_invalid_configuration_fails_with_status_one_and_no_traceback(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path, operational="[concurrency]\nhttp_workers = 0\n")
    completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    assert "Traceback" not in completed.stderr
    assert "http_workers" in completed.stderr


def test_a_missing_secret_names_the_variable_but_not_its_value(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    completed = subprocess.run(
        [sys.executable, "-m", "notable_person_finder", "--config", str(config_file), "run"],
        capture_output=True,
        text=True,
        env={
            key: value
            for key, value in os.environ.items()
            if key not in {"TEST_OPENROUTER", "TEST_BRAVE"}
        },
        timeout=120,
    )
    assert completed.returncode == 1
    assert "TEST_OPENROUTER" in completed.stderr
    assert "or-secret-value" not in completed.stderr


def test_no_secret_value_appears_in_output_or_digest(
    config_file: Path, tmp_path: Path
) -> None:
    completed = run_notable(config_file, "run")
    combined = completed.stdout + completed.stderr
    assert "or-secret-value" not in combined
    assert "brave-secret-value" not in combined
    for path in (tmp_path / "portable" / "data" / "digests").iterdir():
        assert "or-secret-value" not in path.read_text(encoding="utf-8")


def test_an_overlapping_run_fails_immediately_without_creating_a_run(
    config_file: Path, tmp_path: Path
) -> None:
    import portalocker

    lock_file = tmp_path / "portable" / "data" / "notable.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_file, "a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        completed = run_notable(config_file, "run")
    assert completed.returncode == 1
    assert "lock" in completed.stderr.lower()


def test_usage_error_returns_sixty_four(config_file: Path) -> None:
    completed = run_notable(config_file, "nonsense")
    assert completed.returncode == 64


def test_sigint_during_a_run_returns_one_hundred_thirty(
    config_file: Path, tmp_path: Path
) -> None:
    # Raise KeyboardInterrupt from inside the run, exactly as SIGINT would, and
    # assert the documented status rather than racing a real signal.
    script = textwrap.dedent(
        f"""
        import sys
        from notable_person_finder.cli import main as cli
        from notable_person_finder.runs.engine import RunEngine

        def interrupted(self, handlers):
            raise KeyboardInterrupt

        RunEngine.execute = interrupted
        sys.exit(cli.main(["--config", {str(config_file)!r}, "run"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 130
    assert "Traceback" not in completed.stderr

# Recording an interrupted run in durable state is covered directly by
# tests/run_engine/test_crash_boundary.py; this case owns the exit status only.


def test_reporting_failure_exits_one_and_persists_failed_state(
    config_file: Path, tmp_path: Path
) -> None:
    script = textwrap.dedent(
        f"""
        import sys
        from notable_person_finder.cli import main as cli
        from notable_person_finder.reporting.digest import DigestWriteError

        def fail(*args, **kwargs):
            raise DigestWriteError("simulated digest failure")

        cli.write_digest = fail
        sys.exit(cli.main(["--config", {str(config_file)!r}, "run"]))
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, **ENVIRONMENT},
        timeout=120,
    )
    assert completed.returncode == 1
    database = tmp_path / "portable" / "data" / "notable.sqlite3"
    connection = sqlite3.connect(database)
    assert connection.execute("SELECT state FROM run").fetchone()[0] == "failed"
    assert [row[0] for row in connection.execute(
        "SELECT state FROM run_transition ORDER BY id"
    )] == ["running", "failed"]
    connection.close()


def test_status_reports_the_latest_run_without_locking(
    config_file: Path, tmp_path: Path
) -> None:
    run_notable(config_file, "run")
    completed = run_notable(config_file, "status")
    assert completed.returncode == 0
    assert "run-1" in completed.stdout
    assert "complete" in completed.stdout


def test_status_can_inspect_committed_state_while_the_lock_is_held(
    config_file: Path, tmp_path: Path
) -> None:
    import portalocker

    run_notable(config_file, "run")
    lock_file = tmp_path / "portable" / "data" / "notable.lock"
    with open(lock_file, "a+", encoding="utf-8") as handle:
        portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        completed = run_notable(config_file, "status")
    assert completed.returncode == 0


def test_status_before_any_run_reports_that_none_exists(config_file: Path) -> None:
    completed = run_notable(config_file, "status")
    assert completed.returncode == 0
    assert "no run" in completed.stdout.lower()


def test_run_writes_a_latest_copy(config_file: Path, tmp_path: Path) -> None:
    run_notable(config_file, "run")
    latest = tmp_path / "portable" / "data" / "digests" / "latest.md"
    assert latest.is_file()


def test_run_writes_structured_logs(config_file: Path, tmp_path: Path) -> None:
    run_notable(config_file, "run")
    log_file = tmp_path / "portable" / "logs" / "notable.jsonl"
    assert log_file.is_file()
    assert "run_started" in log_file.read_text(encoding="utf-8")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_run_cli.py -v`
Expected: FAIL — `notable run` is not a recognized command, so every case returns 64.

- [ ] **Step 3: Extend the CLI**

Replace `src/notable_person_finder/cli/main.py` with:

```python
from __future__ import annotations

import argparse
import signal
import sqlite3
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import NoReturn
from zoneinfo import ZoneInfo

from notable_person_finder import __version__
from notable_person_finder.config.loader import ConfigLoadError, ResolvedConfig, load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import MigrationError, apply_migrations
from notable_person_finder.obs.logging import configure_logging, log_event
from notable_person_finder.providers.safety import SystemHostResolver
from notable_person_finder.providers.transport import build_transport
from notable_person_finder.reporting.digest import (
    DigestRecord,
    DigestWriteError,
    write_digest,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import SystemClock, utc_timestamp
from notable_person_finder.runs.engine import ReportArtifact, RunEngine
from notable_person_finder.runs.lock import LockUnavailable, MutationLock
from notable_person_finder.runs.models import RunState
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_PARTIAL = 2
EXIT_USAGE = 64
EXIT_INTERRUPTED = 130

_EXIT_BY_STATE = {
    RunState.COMPLETE: EXIT_OK,
    RunState.PARTIAL: EXIT_PARTIAL,
    RunState.FAILED: EXIT_FAILED,
    RunState.INTERRUPTED: EXIT_INTERRUPTED,
}


class UsageError(Exception):
    pass


class NotableArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise UsageError(message)


def build_parser() -> argparse.ArgumentParser:
    parser = NotableArgumentParser(prog="notable")
    parser.add_argument("--config", type=Path, help="path to notable.toml")
    parser.add_argument("--verbose", action="store_true")
    commands = parser.add_subparsers(dest="command", required=True)

    config = commands.add_parser("config")
    config.add_subparsers(dest="config_command", required=True).add_parser("validate")
    commands.add_parser("paths")
    commands.add_parser("run")
    commands.add_parser("status")
    db = commands.add_parser("db")
    db.add_subparsers(dest="db_command", required=True).add_parser("migrate")
    return parser


def command_config_validate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=True)
    print("configuration valid")
    print(f"fingerprint: {loaded.fingerprint}")
    print(f"main: {loaded.paths.config_file}")
    return EXIT_OK


def command_paths(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    for name in (
        "config_file",
        "data_root",
        "database",
        "backups",
        "digests",
        "log_file",
        "cache_root",
        "lock_file",
    ):
        print(f"{name}: {getattr(loaded.paths, name)}")
    return EXIT_OK


def command_db_migrate(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    with MutationLock(loaded.paths.lock_file):
        connection = connect_database(loaded.paths.database)
        try:
            result = apply_migrations(
                connection, loaded.paths.database, loaded.paths.backups
            )
        finally:
            connection.close()
    versions = ", ".join(str(version) for version in result.applied_versions) or "none"
    print(f"applied migrations: {versions}")
    if result.backup_path is not None:
        print(f"backup: {result.backup_path}")
    return EXIT_OK


def _observation_window(loaded: ResolvedConfig, now: datetime) -> tuple[str, str]:
    local = now.astimezone(ZoneInfo(loaded.main.timezone))
    start = local.replace(hour=0, minute=0, second=0, microsecond=0)
    return utc_timestamp(start), utc_timestamp(now)


def command_run(config_file: Path | None, *, verbose: bool) -> int:
    loaded = load_config(config_file, require_secrets=True)
    clock = SystemClock()
    logger = configure_logging(
        loaded.paths.log_file,
        loaded.main.logging,
        secrets=(
            loaded.credentials.openrouter_api_key,
            loaded.credentials.brave_api_key,
        ),
    )

    with MutationLock(loaded.paths.lock_file):
        connection = connect_database(loaded.paths.database)
        try:
            apply_migrations(connection, loaded.paths.database, loaded.paths.backups)

            now = clock.now()
            window_start, _ = _observation_window(loaded, now)
            transport = build_transport(
                loaded.main.transport,
                version=__version__,
                resolver=SystemHostResolver(),
                clock=clock,
            )
            # The pacing gate is built by the first adapter milestone that has
            # a provider to pace; nothing in this milestone makes requests.
            local_date = (
                clock.now().astimezone(ZoneInfo(loaded.main.timezone)).date().isoformat()
            )
            written: DigestRecord | None = None

            def report_run(report) -> ReportArtifact:
                nonlocal written
                try:
                    written = write_digest(
                        loaded.paths.digests,
                        report,
                        local_date=local_date,
                        config=loaded.main.digest,
                    )
                except DigestWriteError:
                    log_event(logger, "run_reporting_failed", run_id=report.run_id)
                    raise
                return ReportArtifact(
                    path=str(written.path),
                    sha256=written.sha256,
                    markdown=written.markdown,
                )

            engine = RunEngine(
                connection,
                retry=RetryCoordinator(loaded.main.retry, clock=clock),
                scheduler=BoundedScheduler(loaded.main.concurrency.http_workers),
                clock=clock,
                timezone=loaded.main.timezone,
                window_start=window_start,
                budget_limit_nano_usd=loaded.main.budget.openrouter_nano_usd_per_run(),
                snapshot_fingerprint=loaded.fingerprint,
                snapshot_json=loaded.snapshot_json,
                reporter=report_run,
            )
            log_event(logger, "run_started", fingerprint=loaded.fingerprint)
            try:
                # No provider adapters exist in this milestone, so no task
                # handlers are registered. Milestones 3-6 supply them.
                report = engine.execute({})
            finally:
                transport.close()

            assert written is not None

            log_event(
                logger,
                "run_finished",
                run_id=report.run_id,
                state=str(report.state),
                required_succeeded=report.counters.required_succeeded,
                required_deferred=report.counters.required_deferred,
                operational_failures=report.counters.operational_failures,
            )
            sys.stdout.write(written.markdown)
            if verbose:
                print(f"digest: {written.path}", file=sys.stderr)
            return _EXIT_BY_STATE[report.state]
        finally:
            connection.close()


def command_status(config_file: Path | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    if not loaded.paths.database.exists():
        print("no run has been recorded yet")
        return EXIT_OK

    connection = connect_database(loaded.paths.database, readonly=True)
    try:
        record = repository.latest_run(connection)
        if record is None:
            print("no run has been recorded yet")
            return EXIT_OK
        failures = repository.operational_failures_for_run(
            connection, run_id=record.id
        )
        print(f"latest run: {record.human_id} ({record.state})")
        print(f"started: {record.started_at}")
        print(f"finished: {record.finished_at or '-'}")
        print(f"digest: {record.digest_path or '-'}")
        print(f"required work pending: {repository.pending_required(connection)}")
        print(f"required work deferred: {repository.deferred_required(connection)}")
        print(f"operational failures: {failures}")
        # Digest backlog, queue tiers, and the oldest pending candidate arrive
        # with the digest queue in the lead-assessment milestone.
        return EXIT_OK
    finally:
        connection.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        arguments = parser.parse_args(argv)
        if arguments.command == "config" and arguments.config_command == "validate":
            return command_config_validate(arguments.config)
        if arguments.command == "paths":
            return command_paths(arguments.config)
        if arguments.command == "run":
            return command_run(arguments.config, verbose=arguments.verbose)
        if arguments.command == "status":
            return command_status(arguments.config)
        if arguments.command == "db" and arguments.db_command == "migrate":
            return command_db_migrate(arguments.config)
        raise UsageError("command is not implemented")
    except UsageError as error:
        parser.print_usage(sys.stderr)
        print(f"{parser.prog}: error: {error}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    except (
        ConfigLoadError,
        MigrationError,
        LockUnavailable,
        DigestWriteError,
        OSError,
        sqlite3.Error,
    ) as error:
        print(error, file=sys.stderr)
        return EXIT_FAILED


def entrypoint() -> NoReturn:
    signal.signal(signal.SIGINT, signal.default_int_handler)
    raise SystemExit(main())
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_run_cli.py -v`
Expected: PASS

- [ ] **Step 5: Confirm the foundation CLI tests still pass**

Run: `uv run pytest tests/foundation/test_foundation_cli.py tests/foundation/test_package_cli.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/cli/main.py tests/run_engine/test_run_cli.py
git commit -m "feat(cli): add notable run and notable status with documented exit statuses"
```

---

## Task 18: At-Least-Once Crash Boundary

Both the persistence design and capability 7 require the unavoidable window — a provider accepted a request but the process died before SQLite stored the response — to be **documented and tested**, not merely described. This task proves two things: that a crash inside that window leaves an in-flight attempt which the next run records as `interrupted` and retries; and that a result which *was* persisted is never repeated.

**Files:**
- Create: `tests/run_engine/test_crash_boundary.py`
- Create: `docs/architecture/at-least-once-execution.md`

**Interfaces:**
- Consumes: `RunEngine`, `repository`, `TaskHandler`, `TaskOutcome`.
- Produces: no production interface; this task adds the regression proof and the operator-facing note.

- [ ] **Step 1: Write the failing tests**

Create `tests/run_engine/test_crash_boundary.py`:

```python
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.config.models import RetryConfig
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    TaskHandler,
    TaskOutcome,
)
from notable_person_finder.runs.models import RunState, WorkState
from notable_person_finder.runs.retry import RetryCoordinator
from notable_person_finder.runs.scheduler import BoundedScheduler

NOW = "2026-07-25T06:00:00Z"


class SimulatedCrash(BaseException):
    """Stands in for process death: not catchable as an ordinary Exception."""


@pytest.fixture
def database(tmp_path: Path) -> Path:
    path = tmp_path / "notable.sqlite3"
    connection = connect_database(path)
    apply_migrations(connection, path, tmp_path / "backups")
    connection.close()
    return path


def engine_for(connection: sqlite3.Connection) -> RunEngine:
    clock = FakeClock()
    return RunEngine(
        connection,
        retry=RetryCoordinator(RetryConfig(max_attempts=1, jitter_ratio=0.0), clock=clock),
        scheduler=BoundedScheduler(max_workers=1),
        clock=clock,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        budget_limit_nano_usd=None,
        snapshot_fingerprint="a" * 64,
        snapshot_json="{}",
        reporter=lambda report: ReportArtifact(path=None, sha256=None, markdown=""),
    )


def schedule(connection: sqlite3.Connection) -> int:
    return repository.schedule_work(
        connection,
        task_type="probe",
        subject_kind="synthetic",
        subject_id=None,
        fingerprint="b" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=None,
        now=NOW,
    )


def test_a_crash_after_the_provider_accepted_leaves_an_in_flight_attempt(
    database: Path,
) -> None:
    connection = connect_database(database)
    work_id = schedule(connection)
    provider_calls: list[int] = []

    def execute(work_item, ordinal: int) -> TaskOutcome:
        provider_calls.append(ordinal)
        # The provider has accepted and charged for the request at this point.
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    connection.close()

    # The process died: the attempt row exists with no outcome, and the run is
    # still 'running' because no terminal state was ever written.
    inspect = connect_database(database, readonly=True)
    attempt = inspect.execute("SELECT outcome, finished_at FROM attempt").fetchone()
    assert attempt["outcome"] is None
    assert attempt["finished_at"] is None
    assert inspect.execute("SELECT state FROM run").fetchone()["state"] == "running"
    inspect.close()
    assert provider_calls == [1]
    assert work_id > 0


def test_the_next_run_records_the_interruption_and_repeats_the_call(
    database: Path,
) -> None:
    connection = connect_database(database)
    schedule(connection)

    def crashing(work_item, ordinal: int) -> TaskOutcome:
        raise SimulatedCrash

    crash_handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=crashing
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({crash_handler.task_type: crash_handler})
    connection.close()

    # A fresh process starts: no resume mode, just another ordinary run.
    connection = connect_database(database)
    second_calls: list[int] = []

    def succeeding(work_item, ordinal: int) -> TaskOutcome:
        second_calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=succeeding
    )
    report = engine_for(connection).execute({handler.task_type: handler})

    # This is the documented at-least-once window: the request is made again.
    assert second_calls == [2]
    assert report.state is RunState.COMPLETE
    assert report.interrupted_runs != ()

    outcomes = [
        row["outcome"] for row in connection.execute("SELECT outcome FROM attempt ORDER BY id")
    ]
    assert outcomes == ["interrupted", "succeeded"]
    connection.close()


def test_a_persisted_result_is_never_repeated(database: Path) -> None:
    connection = connect_database(database)
    schedule(connection)
    calls: list[int] = []

    def execute(work_item, ordinal: int) -> TaskOutcome:
        calls.append(ordinal)
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None)

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=execute
    )
    engine_for(connection).execute({handler.task_type: handler})
    engine_for(connection).execute({handler.task_type: handler})

    assert calls == [1]  # the second run found no eligible work
    connection.close()


def test_an_interrupted_run_is_not_reported_as_complete(database: Path) -> None:
    connection = connect_database(database)
    schedule(connection)

    def crashing(work_item, ordinal: int) -> TaskOutcome:
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=crashing
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})
    connection.close()

    connection = connect_database(database)
    engine_for(connection).execute({})
    states = [row["state"] for row in connection.execute("SELECT state FROM run ORDER BY id")]
    assert states[0] == RunState.INTERRUPTED
    connection.close()


def test_abandoned_work_returns_to_pending_not_to_deferred(database: Path) -> None:
    connection = connect_database(database)
    work_id = schedule(connection)

    def crashing(work_item, ordinal: int) -> TaskOutcome:
        raise SimulatedCrash

    handler = TaskHandler(
        task_type="probe", provider="probe_provider", operation="probe_call", execute=crashing
    )
    with pytest.raises(SimulatedCrash):
        engine_for(connection).execute({handler.task_type: handler})

    repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert (
        connection.execute("SELECT state FROM work_item WHERE id = ?", (work_id,)).fetchone()[
            "state"
        ]
        == WorkState.PENDING
    )
    connection.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/run_engine/test_crash_boundary.py -v`

**This task is a characterisation test, not red-green TDD.** Tasks 10–15 already
built the behaviour these tests describe; this task proves it and writes it
down. **Passing on the first run is the expected and correct result** — it
confirms the claim in `docs/architecture/at-least-once-execution.md`. Record
that in the commit message and move to Step 4.

If a test *fails*, you have found a real defect in Task 12 or Task 15 — most
likely a transaction still open across `handler.execute`, or an attempt row
that was never committed before the call. Fix the production code in
`repository.claim_and_start_attempt`, `repository.start_attempt`, or
`RunEngine._perform`. Never weaken or delete an assertion to make this file
pass.

- [ ] **Step 3: Confirm the attempt is committed before the external call**

Both `repository.claim_and_start_attempt` and `repository.start_attempt` commit
their own transaction before `handler.execute` runs. Verify by inspection that
neither holds an open transaction across the call, and that
`RunEngine._perform` does not wrap them in an outer transaction.

- [ ] **Step 4: Write the operator-facing note**

Create `docs/architecture/at-least-once-execution.md`:

```markdown
# At-Least-Once External Execution

**Status:** Current
**Applies to:** the rewrite run engine (`src/notable_person_finder/runs/`)

## The window

External work runs in three steps:

1. a brief transaction claims the work item, creates the attempt, and reserves
   budget;
2. the external call runs with no database lock held; and
3. a brief transaction stores the provider evidence and the typed domain
   observation, then completes or defers the work item.

If the process dies between steps 2 and 3, a provider may already have accepted
— and charged for — a request whose response was never stored. The next
ordinary `notable run` marks the abandoned attempt `interrupted`, returns the
work item to `pending`, and performs the request again.

## What the application does and does not promise

The application guarantees **reuse after successful persistence**: once a
result is committed, no later run repeats that call while its fingerprint is
unchanged. It does **not** promise exactly-once execution across a remote API
and a local database. That would require a usable provider idempotency key,
which version one does not assume.

## Consequences for the operator

- A crash during a paid OpenRouter generation can cost that generation twice.
- No manual cleanup is needed: recovery is another ordinary `notable run`.
- `notable audit run RUN_ID` shows interrupted attempts explicitly, so a
  duplicated charge is always traceable.

## Verification

`tests/run_engine/test_crash_boundary.py` covers the window directly: a
simulated crash inside step 2, the interrupted-attempt record, the repeated
call in the next run, and the guarantee that a persisted result is never
repeated.
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/run_engine/test_crash_boundary.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add tests/run_engine/test_crash_boundary.py docs/architecture/at-least-once-execution.md
git commit -m "test(runs): prove and document the at-least-once execution window"
```

---

## Task 19: Milestone Acceptance and Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`
- Test: the full milestone gate

**Interfaces:**
- Consumes: everything above.
- Produces: no new code interface.

- [ ] **Step 1: Verify no domain module imports a transport type**

Run:

```bash
! grep -rn "import httpx\|from httpx" src/notable_person_finder --include='*.py' \
  | grep -v "src/notable_person_finder/providers/"
```

Expected: exit status 0.

- [ ] **Step 2: Verify no secret can reach a log, snapshot, or digest**

Run: `uv run pytest tests/run_engine -k "secret or redact" -v`
Expected: PASS.

- [ ] **Step 3: Run the full milestone gate**

Run:

```bash
uv sync --frozen
uv run pytest tests/foundation tests/run_engine
uv run notable --help
```

Expected: all tests PASS; `notable --help` lists `run`, `status`, `config`, `paths`, and `db`.

- [ ] **Step 4: Update the repository agent guide**

In `CLAUDE.md`, under **Environment and Verification**, replace the line

```text
- For the completed application foundation, use
  `uv run pytest tests/foundation`.
```

with

```text
- For the completed application foundation, use
  `uv run pytest tests/foundation`.
- For the completed run engine and shared transport, use
  `uv run pytest tests/run_engine`.
```

Add to the **Foundation Invariants** section:

```text
- Every external network call maps to exactly one persisted attempt attributed
  to its run and work item. Only the central retry coordinator starts a repeat
  request, and no transaction is held across a network call.
- External execution is at-least-once. See
  `docs/architecture/at-least-once-execution.md`.
```

- [ ] **Step 5: Document the commands in the README**

Add a section to `README.md`:

```markdown
### Rewrite commands

| Command | Purpose |
| --- | --- |
| `notable run` | Validate configuration, migrate, continue eligible work, and write the daily digest. |
| `notable status` | Show the latest run state and outstanding work. Performs no work and takes no lock. |
| `notable config validate` | Validate the configuration graph offline. |
| `notable paths` | Print every resolved storage location. |
| `notable db migrate` | Apply pending checked migrations explicitly. |

Exit statuses: `0` complete, `1` failed, `2` partial, `64` usage, `130` interrupted.
```

- [ ] **Step 6: Confirm the working tree is clean**

Run:

```bash
git diff --check
git status --short
```

Expected: no output from either command after the final commit.

- [ ] **Step 7: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: document run engine commands and verification gate"
```

---

## Milestone Completion Gate

The milestone is complete only when every item below holds:

- `uv sync --frozen` succeeds and `uv.lock` is committed with the `httpx` addition.
- `uv run pytest tests/foundation tests/run_engine` passes with no network access and no real sleeping.
- Migration `0002` applies cleanly to a database that already has `0001`, and `0001`'s recorded checksum is unchanged.
- `notable run` on a fresh data root exits `0`, writes an immutable dated digest plus `latest.md`, and emits byte-identical Markdown on standard output.
- A run with outstanding required work exits `2`; validation, migration, lock, storage, and reporting failures exit `1`; a bad command exits `64`.
- A required permanent failure cannot produce a `complete` run: it is `partial` when other meaningful results exist and `failed` when it prevents meaningful work.
- The run remains `running` until reporting succeeds. A reporting failure records exactly one `failed` terminal transition, while a successful report records its digest path and hash with the terminal transition.
- An overlapping mutating invocation fails immediately without creating a run row.
- `notable status` inspects committed state while the mutation lock is held.
- Every external call maps to exactly one attempt row attributed to its run and work item. Retries and post-crash execution continue the persisted ordinal rather than overwriting predecessors or restarting at one.
- The first external attempt atomically claims work, reserves budget, and creates its row. Budget reservations cannot oversubscribe the configured cap under concurrency, and reported actual cost is reconciled when the attempt finishes.
- Deferred work cannot run twice in one invocation but becomes eligible in the next ordinary run; unknown task types remain pending without making the engine loop.
- Digest and status run summaries use only the selected run's attempts and completions; durable pending and deferred backlog counts are queried separately.
- URL, DNS-preflight, redirect, timeout, encoded/decoded response-size, concurrency, and pacing bounds are enforced and tested. Documentation does not claim that separate preflight DNS resolution pins HTTPX's connection address.
- No secret value appears in logs, snapshots, digests, terminal output, or test fixtures.
- No module outside `src/notable_person_finder/providers/` imports `httpx`.
- `docs/architecture/at-least-once-execution.md` exists and its claims are covered by `tests/run_engine/test_crash_boundary.py`.
- `git diff --check` and `git status --short` are clean.

## Deferred to Later Milestones

Named here so a reviewer does not read their absence as an omission:

- Every concrete provider adapter (`FeedClient`, `MediaWikiClient`, `WebSearchClient`, `ArticleFetcher`, `ArticleExtractor`, `LlmClient`) — milestones 3–6.
- OpenRouter capability and pricing preflight; the budget coordinator here is provider-agnostic — milestone 6's LLM work supplies the reservation amounts.
- Every domain table: source items, people, Wikipedia observations, articles, assessments, digest queue — milestones 3–6.
- The digest shortlist section, ranking, and optional model synthesis — milestone 6.
- `notable digest show`, `notable audit run`, and `notable audit person` — milestone 6.
- `notable status` backlog, queue tiers, and oldest pending candidate — milestone 6.
- Promptfoo suites, live smoke tests, and the one-time legacy comparison — milestone 7.
- Connection-level DNS-answer pinning or a custom network transport. Version one performs the approved public-address preflight before each request and redirect, but does not claim multi-tenant-grade DNS-rebinding protection.
