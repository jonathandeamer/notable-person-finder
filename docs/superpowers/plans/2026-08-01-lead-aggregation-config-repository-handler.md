# Milestone 6a-ii: Lead Aggregation — Config, Repository, and Work-Item Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `[tasks.aggregate_lead]` config surface, the SQL
repository layer for all seven `leads/` tables, and the `aggregate_person_lead`
work-item handler (K4, K5, K6) that reads milestone 5's
`person_article_assessment`/`article_assessment_signal` rows, calls
6a-i's `aggregate_lead`/`decide_queue_transition`, and persists a
`lead_assessment` plus `digest_queue`/`queue_transition` rows.

**Architecture:** `leads/repository.py` holds all SQL, operating inside the
caller's transaction (never opening or committing its own, matching every
other domain's repository module). `leads/service.py` builds a
`prepare`/`execute`/`persist` `TaskHandler` in the same shape as
`wikipedia/service.py`'s `build_match_wikipedia_handler`, but with
`provider='local'` and no external call — `execute` is pure computation over
already-persisted rows. This plan does not yet wire the handler into
`notable run`'s settlement points or CLI registration; that is 6a-iii.

**Tech Stack:** Python 3.13, SQLite (stdlib `sqlite3`), the existing run
engine's `TaskHandler`/`TaskPreparation`/`TaskOutcome`/`WorkState` contract,
pytest, ruff, pyright, `uv`.

**Plan sequence:** This is plan 2 of 4. Depends on 6a-i
(`docs/superpowers/plans/2026-08-01-lead-aggregation-schema-and-pure-logic.md`)
being merged — imports `notable_person_finder.leads.aggregation`,
`notable_person_finder.leads.ranking`, and `notable_person_finder.leads.queue`
directly, and assumes migration `0008_lead_aggregation.sql` is applied.
Next: 6a-iii (settlement hooks + merge reconciliation + CLI registration),
then 6a-iv (digest rendering + `notable status` + completion gate).

## Global Constraints

- `aggregate_person_lead`'s `execute` makes **no external call** — no
  attempt row, `provider='local'` convention (matches 3b2's
  empty-candidate-create pattern).
- The handler receives `SourcePolicy` as an explicit builder parameter from
  the caller's already-loaded config (`loaded.source_policy` in
  `cli/main.py`, wired in 6a-iii) — it never calls `load_source_policy`
  itself.
- No secrets, no model, no Promptfoo suite in this milestone (6a).
- Full design authority:
  `docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
  (K1–K13).
- Local completion gate for this plan alone:
  `uv run pytest tests/leads tests/foundation`, `uv run ruff check .`,
  `uv run ruff format .`, `uv run pyright`.

---

## File Structure (this plan's scope)

```
src/notable_person_finder/leads/
    repository.py          # SQL for all seven new tables
    service.py             # aggregate_person_lead handler (K4, K5, K6)

src/notable_person_finder/config/models.py      # modify: AggregateLeadConfig, TasksConfig field
config/*.example.toml                             # modify: [tasks.aggregate_lead] example block

tests/leads/
    test_repository.py
    test_service.py
tests/foundation/test_aggregate_lead_config.py
```

---

## Task 5: `AggregateLeadConfig` (config surface)

**Files:**
- Modify: `src/notable_person_finder/config/models.py`
- Test: add to `tests/foundation/test_config_models.py` (find the existing
  file that tests `TasksConfig`/`AssessArticleConfig`; if no single file
  covers all task configs, add `tests/foundation/test_aggregate_lead_config.py`
  following whichever pattern the existing `AssessArticleConfig` tests use —
  check `tests/foundation/` for the exact filename before creating a new one)

**Interfaces:**
- Produces: `AggregateLeadConfig` with fields `promising_domain_threshold:
  int`, `digest_limit: int`, `starvation_days: int`,
  `reminder_interval_days: int`; `TasksConfig.aggregate_lead` field.

- [ ] **Step 1: Locate the existing task-config test file**

Run: `grep -rl "AssessArticleConfig\|TasksConfig" tests/foundation/`

Use whichever file that returns as the home for the new test; if none
directly tests `TasksConfig` field composition, create
`tests/foundation/test_aggregate_lead_config.py`.

- [ ] **Step 2: Write the failing test**

```python
# tests/foundation/test_aggregate_lead_config.py
from notable_person_finder.config.models import AggregateLeadConfig, TasksConfig


def test_aggregate_lead_config_defaults():
    config = AggregateLeadConfig()
    assert config.promising_domain_threshold == 2
    assert config.digest_limit == 10
    assert config.starvation_days == 14
    assert config.reminder_interval_days == 0


def test_tasks_config_composes_aggregate_lead():
    tasks = TasksConfig()
    assert isinstance(tasks.aggregate_lead, AggregateLeadConfig)


def test_aggregate_lead_config_rejects_zero_domain_threshold():
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        AggregateLeadConfig(promising_domain_threshold=0)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/foundation/test_aggregate_lead_config.py -v`
Expected: FAIL — `ImportError: cannot import name 'AggregateLeadConfig'`

- [ ] **Step 4: Implement**

In `src/notable_person_finder/config/models.py`, add near `AssessArticleConfig`
(this task config has no generation parameters, so it derives from the same
`_StrictConfigurationModel` base but has no `model`/`parameters` fields —
follow `DigestConfig`'s plainer shape instead, adjusted to
`_StrictConfigurationModel` for consistency with the other `[tasks.*]`
entries):

```python
class AggregateLeadConfig(_StrictConfigurationModel):
    promising_domain_threshold: int = Field(default=2, strict=True, ge=1, le=10)
    digest_limit: int = Field(default=10, strict=True, ge=1, le=1000)
    starvation_days: int = Field(default=14, strict=True, ge=1, le=365)
    reminder_interval_days: int = Field(default=0, strict=True, ge=0, le=365)
```

Add the field to `TasksConfig` (after `assess_article`, matching pipeline
order):

```python
class TasksConfig(_StrictConfigurationModel):
    detect_people: DetectPeopleConfig = DetectPeopleConfig()
    resolve_person_entity: ResolvePersonEntityConfig = ResolvePersonEntityConfig()
    match_wikipedia_identity: MatchWikipediaIdentityConfig = (
        MatchWikipediaIdentityConfig()
    )
    assess_article: AssessArticleConfig = AssessArticleConfig()
    aggregate_lead: AggregateLeadConfig = AggregateLeadConfig()
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/foundation/test_aggregate_lead_config.py -v`
Expected: PASS

- [ ] **Step 6: Add the `[tasks.aggregate_lead]` example block**

Check `config/*.example.toml` for where `[tasks.assess_article]` is
documented and add directly below it:

```toml
[tasks.aggregate_lead]
promising_domain_threshold = 2
digest_limit = 10
starvation_days = 14
reminder_interval_days = 0
```

- [ ] **Step 7: Commit**

```bash
git add src/notable_person_finder/config/models.py tests/foundation/test_aggregate_lead_config.py config/*.example.toml
git commit -m "feat(config): add [tasks.aggregate_lead] section"
```

---

## Task 6: `leads/repository.py` — SQL for all seven tables

**Files:**
- Create: `src/notable_person_finder/leads/repository.py`
- Test: `tests/leads/test_repository.py`

**Interfaces:**
- Consumes: an open `sqlite3.Connection` inside the caller's transaction
  (never opens or commits its own — matches every other domain's
  `repository.py`).
- Produces:
  - `insert_lead_assessment(connection, *, person_id, run_id, outcome:
    LeadOutcome, lead_policy_fingerprint, ordering_factors_json, decided_at)
    -> int` (returns new row id, also inserts
    `lead_assessment_qualifying_article` and `lead_assessment_signal` rows,
    and sets `person.current_lead_assessment_id`).
  - `fetch_prior_queue_state(connection, *, person_id) -> PriorLeadState |
    None`.
  - `upsert_digest_queue(connection, *, person_id, status, tier,
    eligibility_reason, lead_assessment_id, first_pending_at,
    last_material_change_at) -> None`.
  - `remove_digest_queue(connection, *, person_id, removed_reason,
    last_material_change_at) -> None`.
  - `insert_queue_transition(connection, *, person_id, run_id,
    lead_assessment_id, tier, from_status, to_status, reason, occurred_at)
    -> int`.
  - `fetch_pending_queue_entries(connection) -> list[QueueEntry]` (joins
    `digest_queue` + `lead_assessment` for ranking input).
  - `insert_digest(connection, *, run_id, file_path, timezone, window_start,
    window_end, run_state, content_hash, created_at) -> int`.
  - `insert_digest_entry(connection, *, digest_id, person_id,
    lead_assessment_id, queue_transition_id, ordinal) -> None`.

- [ ] **Step 1: Write failing repository tests**

```python
# tests/leads/test_repository.py
import sqlite3

from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.aggregation import ArticleEvidence, LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.repository import (
    fetch_pending_queue_entries,
    fetch_prior_queue_state,
    insert_lead_assessment,
    insert_queue_transition,
    remove_digest_queue,
    upsert_digest_queue,
)


def _seeded_connection(tmp_path):
    connection = sqlite3.connect(tmp_path / "test.db")
    apply_migrations(connection)
    connection.execute(
        "INSERT INTO run (id, started_at, state) VALUES (1, '2026-08-01T00:00:00Z', 'running')"
    )
    connection.execute(
        "INSERT INTO person (id, created_at) VALUES (7, '2026-08-01T00:00:00Z')"
    )
    connection.commit()
    return connection


def test_insert_lead_assessment_sets_current_pointer(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="promising_lead",
        qualifying_domain_count=2,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    pointer = connection.execute(
        "SELECT current_lead_assessment_id FROM person WHERE id = 7"
    ).fetchone()[0]
    assert pointer == lead_id


def test_upsert_then_fetch_prior_queue_state(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=7, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=7, status="pending", tier="possible_lead",
        eligibility_reason="new", lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    prior = fetch_prior_queue_state(connection, person_id=7)
    assert prior == PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-08-01T00:00:00Z",
    )


def test_remove_digest_queue_sets_removed_status(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=7, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=7, status="pending", tier="possible_lead",
        eligibility_reason="new", lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    remove_digest_queue(
        connection, person_id=7, removed_reason="matching_page_found",
        last_material_change_at="2026-08-02T00:00:00Z",
    )
    connection.commit()
    row = connection.execute(
        "SELECT status, removed_reason FROM digest_queue WHERE person_id = 7"
    ).fetchone()
    assert row == ("removed", "matching_page_found")


def test_insert_queue_transition_records_ledger_row(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=7, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    transition_id = insert_queue_transition(
        connection, person_id=7, run_id=1, lead_assessment_id=lead_id,
        tier="possible_lead", from_status=None, to_status="pending",
        reason="new", occurred_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    row = connection.execute(
        "SELECT person_id, to_status, reason FROM queue_transition WHERE id = ?",
        (transition_id,),
    ).fetchone()
    assert row == (7, "pending", "new")


def test_fetch_pending_queue_entries_excludes_removed(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=7, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=7, status="pending", tier="possible_lead",
        eligibility_reason="new", lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    entries = fetch_pending_queue_entries(connection)
    assert len(entries) == 1
    assert entries[0].person_id == 7
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_repository.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `repository.py`**

```python
# src/notable_person_finder/leads/repository.py
"""SQL for the seven leads/digest-queue tables. Every function operates
inside the caller's transaction; none opens or commits its own."""

from __future__ import annotations

import json
import sqlite3

from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.ranking import QueueEntry


def insert_lead_assessment(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    outcome: LeadOutcome,
    lead_policy_fingerprint: str,
    ordering_factors_json: str,
    decided_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO lead_assessment (
            person_id, run_id, outcome, qualifying_domain_count,
            incompleteness_reason, ordering_factors_json,
            lead_policy_fingerprint, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            outcome.outcome,
            outcome.qualifying_domain_count,
            outcome.incompleteness_reason,
            ordering_factors_json,
            lead_policy_fingerprint,
            decided_at,
        ),
    )
    lead_id = cursor.lastrowid
    assert lead_id is not None
    for article in outcome.qualifying_articles:
        connection.execute(
            """
            INSERT INTO lead_assessment_qualifying_article (
                lead_assessment_id, person_article_assessment_id, canonical_domain
            ) VALUES (?, ?, ?)
            """,
            (lead_id, article.person_article_assessment_id, article.canonical_domain),
        )
    for signal in outcome.contributing_signals:
        connection.execute(
            """
            INSERT INTO lead_assessment_signal (
                lead_assessment_id, article_assessment_signal_id
            ) VALUES (?, ?)
            """,
            (lead_id, signal.article_assessment_signal_id),
        )
    connection.execute(
        "UPDATE person SET current_lead_assessment_id = ? WHERE id = ?",
        (lead_id, person_id),
    )
    return lead_id


def fetch_prior_queue_state(
    connection: sqlite3.Connection, *, person_id: int
) -> PriorLeadState | None:
    row = connection.execute(
        """
        SELECT dq.status, dq.tier, la.qualifying_domain_count, dq.first_pending_at
        FROM digest_queue dq
        JOIN lead_assessment la ON la.id = dq.lead_assessment_id
        WHERE dq.person_id = ?
        """,
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    status, tier, qualifying_domain_count, first_pending_at = row
    return PriorLeadState(
        status=status,
        tier=tier,
        qualifying_domain_count=qualifying_domain_count,
        first_pending_at=first_pending_at,
    )


def upsert_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    status: str,
    tier: str,
    eligibility_reason: str,
    lead_assessment_id: int,
    first_pending_at: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_queue (
            person_id, status, tier, eligibility_reason, lead_assessment_id,
            first_pending_at, last_material_change_at, removed_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT (person_id) DO UPDATE SET
            status = excluded.status,
            tier = excluded.tier,
            eligibility_reason = excluded.eligibility_reason,
            lead_assessment_id = excluded.lead_assessment_id,
            last_material_change_at = excluded.last_material_change_at,
            removed_reason = NULL
        """,
        (
            person_id,
            status,
            tier,
            eligibility_reason,
            lead_assessment_id,
            first_pending_at,
            last_material_change_at,
        ),
    )


def remove_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    removed_reason: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        UPDATE digest_queue
        SET status = 'removed', removed_reason = ?, last_material_change_at = ?
        WHERE person_id = ?
        """,
        (removed_reason, last_material_change_at, person_id),
    )


def insert_queue_transition(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    lead_assessment_id: int,
    tier: str,
    from_status: str | None,
    to_status: str,
    reason: str,
    occurred_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO queue_transition (
            person_id, run_id, lead_assessment_id, tier, from_status,
            to_status, reason, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            lead_assessment_id,
            tier,
            from_status,
            to_status,
            reason,
            occurred_at,
        ),
    )
    transition_id = cursor.lastrowid
    assert transition_id is not None
    return transition_id


def fetch_pending_queue_entries(
    connection: sqlite3.Connection,
) -> list[QueueEntry]:
    rows = connection.execute(
        """
        SELECT person_id, eligibility_reason, first_pending_at
        FROM digest_queue
        WHERE status = 'pending'
        """
    ).fetchall()
    return [
        QueueEntry(person_id=r[0], eligibility_reason=r[1], first_pending_at=r[2])
        for r in rows
    ]


def insert_digest(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    timezone: str,
    window_start: str,
    window_end: str,
    run_state: str,
    content_hash: str,
    created_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO digest (
            run_id, file_path, timezone, window_start, window_end,
            run_state, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            file_path,
            timezone,
            window_start,
            window_end,
            run_state,
            content_hash,
            created_at,
        ),
    )
    digest_id = cursor.lastrowid
    assert digest_id is not None
    return digest_id


def insert_digest_entry(
    connection: sqlite3.Connection,
    *,
    digest_id: int,
    person_id: int,
    lead_assessment_id: int,
    queue_transition_id: int,
    ordinal: int,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_entry (
            digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal),
    )
```

Note: `ON CONFLICT (person_id) DO UPDATE` requires SQLite's upsert syntax
(3.24+, available in stdlib on Python 3.13) — confirm the project's existing
repositories use the same idiom (`grep -r "ON CONFLICT" src/`) and match
column-list style if a convention differs (e.g. `ON CONFLICT(person_id)` vs.
`ON CONFLICT (person_id)`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_repository.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/leads/repository.py tests/leads/test_repository.py
git commit -m "feat(leads): repository SQL for lead/queue/digest tables"
```

---

## Task 7: `aggregate_person_lead` handler and evidence loading (K5, K6)

**Files:**
- Modify: `src/notable_person_finder/leads/service.py` (new file)
- Test: `tests/leads/test_service.py`

**Interfaces:**
- Consumes: `TaskHandler`/`TaskPreparation`/`TaskOutcome`/`WorkState` from
  the run engine (same imports as `wikipedia/service.py`'s
  `build_match_wikipedia_handler`); `aggregate_lead` (Task 2);
  `decide_queue_transition` (Task 4); `rank_key`/`QueueEntry` (Task 3);
  `leads.repository` functions (Task 6); `SourcePolicy` (from
  `coverage/screening.py`); `AggregateLeadConfig` (Task 5).
- Produces:
  - `AGGREGATE_PERSON_LEAD_TASK_TYPE = "aggregate_person_lead"`
  - `AGGREGATE_PERSON_LEAD_PRIORITY = 75`
  - `build_aggregate_person_lead_handler(connection, *, config: MainConfig,
    policy: SourcePolicy) -> TaskHandler`
  - `_schedule_lead_aggregation_after_settled(connection, *, person_id: int,
    run_id: int, config: MainConfig, now: str) -> None` — the K5-pattern
    same-run hook, consumed by Task 8.
  - `_compute_material_fingerprint(...) -> str` (K6).

- [ ] **Step 1: Write the failing handler tests**

```python
# tests/leads/test_service.py
"""Unit tests for the aggregate_person_lead handler's prepare/execute/persist
lifecycle, following the shape of tests/wikipedia/test_match_service.py."""
import sqlite3

import pytest

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.service import (
    AGGREGATE_PERSON_LEAD_TASK_TYPE,
    build_aggregate_person_lead_handler,
)


@pytest.fixture
def connection(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db")
    apply_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture
def empty_policy():
    return SourcePolicy(
        schema_version=1, key="test", label="Test", rules=(), fingerprint="a" * 64
    )


def test_handler_task_type_matches_constant(connection, empty_policy):
    handler = build_aggregate_person_lead_handler(
        connection, config=MainConfig(), policy=empty_policy
    )
    assert handler.task_type == AGGREGATE_PERSON_LEAD_TASK_TYPE


def test_handler_provider_is_local(connection, empty_policy):
    handler = build_aggregate_person_lead_handler(
        connection, config=MainConfig(), policy=empty_policy
    )
    assert handler.provider == "local"


def test_execute_produces_no_attempt_row(connection, empty_policy):
    """K5: execute performs pure computation, no external call, no attempt
    row is ever the site of a network request for this work kind."""
    handler = build_aggregate_person_lead_handler(
        connection, config=MainConfig(), policy=empty_policy
    )
    assert handler.reserved_nano_usd == 0
```

Extend this file with a full prepare→execute→persist round trip once
`leads/service.py`'s exact `TaskHandler` construction is written (mirror
`tests/wikipedia/test_match_service.py`'s fixture setup for a `person`, a
completed coverage plan, and `person_article_assessment` rows — read that
file for the exact fixture-building helpers before duplicating them here;
reuse its helper functions via import if they are already factored into a
shared `tests/coverage/conftest.py` or `tests/wikipedia/conftest.py`
fixture, rather than re-deriving fixture SQL from scratch).

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_service.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `leads/service.py`**

```python
# src/notable_person_finder/leads/service.py
"""aggregate_person_lead work-item handler and same-run scheduling hook.

K5: execute is pure computation over already-persisted rows -- no external
call, no attempt row, provider='local' (3b2's empty-candidate-create
convention). K4: the handler receives SourcePolicy from the caller's already-
loaded config; only the settlement-hook function below re-loads it, because
it runs inside another domain's transaction where the loaded object is not
in scope (matching wikipedia/service.py's
_schedule_coverage_after_wikipedia_settled).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.leads.aggregation import (
    ArticleEvidence,
    LeadOutcome,
    SignalEvidence,
    aggregate_lead,
)
from notable_person_finder.leads.queue import decide_queue_transition
from notable_person_finder.leads.ranking import QueueEntry
from notable_person_finder.leads.repository import (
    fetch_prior_queue_state,
    insert_lead_assessment,
    insert_queue_transition,
    remove_digest_queue,
    upsert_digest_queue,
)
from notable_person_finder.runs.engine import (  # match actual module path used by wikipedia/service.py
    TaskHandler,
    TaskOutcome,
    TaskPreparation,
    WorkState,
)

AGGREGATE_PERSON_LEAD_TASK_TYPE = "aggregate_person_lead"
AGGREGATE_PERSON_LEAD_PRIORITY = 75
LOCAL_PROVIDER = "local"


def _canonical_domain_map(policy: SourcePolicy) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for rule in policy.rules:
        own_domain = getattr(rule, "host_exact", None) or getattr(
            rule, "host_suffix", None
        )
        canonical = getattr(rule, "canonical_domain", None) or own_domain
        if own_domain and canonical:
            mapping[own_domain] = canonical
    return mapping


def _load_article_evidence(
    connection: sqlite3.Connection, *, person_id: int, canonical_domains: dict[str, str]
) -> list[ArticleEvidence]:
    rows = connection.execute(
        """
        SELECT id, screening_rule_status, person_relation, coverage_depth,
               content_types_json, subject_relationship, source_policy_fingerprint
        FROM person_article_assessment
        WHERE person_id = ? AND disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    evidence: list[ArticleEvidence] = []
    for (
        assessment_id,
        screening_status,
        person_relation,
        coverage_depth,
        content_types_json,
        subject_relationship,
        _fingerprint,
    ) in rows:
        content_types = json.loads(content_types_json) if content_types_json else []
        domain_row = connection.execute(
            """
            SELECT ca.canonical_url
            FROM person_article_assessment paa
            JOIN canonical_article ca ON ca.id = paa.canonical_article_id
            WHERE paa.id = ?
            """,
            (assessment_id,),
        ).fetchone()
        domain = domain_row[0] if domain_row else ""
        evidence.append(
            ArticleEvidence(
                person_article_assessment_id=assessment_id,
                domain=domain,
                canonical_domain=canonical_domains.get(domain, domain),
                same_person=(person_relation == "same_person"),
                coverage_depth=coverage_depth,
                content_qualifying=bool(content_types),
                editorially_independent=(
                    subject_relationship == "editorially_independent"
                ),
                screening_status=screening_status,
            )
        )
    return evidence


def _load_signal_evidence(
    connection: sqlite3.Connection, *, person_id: int
) -> list[SignalEvidence]:
    rows = connection.execute(
        """
        SELECT s.id, s.signal_kind, s.category
        FROM article_assessment_signal s
        JOIN person_article_assessment paa ON paa.id = s.assessment_id
        WHERE paa.person_id = ? AND paa.disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    return [
        SignalEvidence(
            article_assessment_signal_id=r[0],
            signal_kind=r[1],
            category=r[2],
            transferable=True,
        )
        for r in rows
    ]


def _compute_material_fingerprint(
    *,
    article_assessment_ids: list[int],
    signal_ids: list[int],
    wikipedia_outcome: str | None,
    policy: SourcePolicy,
    config: MainConfig,
) -> str:
    payload = {
        "article_assessment_ids": sorted(article_assessment_ids),
        "signal_ids": sorted(signal_ids),
        "wikipedia_outcome": wikipedia_outcome,
        "source_policy_fingerprint": policy.fingerprint,
        "promising_domain_threshold": config.tasks.aggregate_lead.promising_domain_threshold,
        "starvation_days": config.tasks.aggregate_lead.starvation_days,
        "reminder_interval_days": config.tasks.aggregate_lead.reminder_interval_days,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class _AggregatePayload:
    person_id: int
    wikipedia_outcome: str | None


def build_aggregate_person_lead_handler(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> TaskHandler:
    canonical_domains = _canonical_domain_map(policy)

    def prepare(work_item) -> TaskPreparation:
        person_id = work_item.subject_id
        row = connection.execute(
            "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
            (person_id,),
        ).fetchone()
        wikipedia_outcome = None
        if row and row[0]:
            outcome_row = connection.execute(
                "SELECT outcome FROM wikipedia_identity_observation WHERE id = ?",
                (row[0],),
            ).fetchone()
            wikipedia_outcome = outcome_row[0] if outcome_row else None
        return TaskPreparation(
            payload=_AggregatePayload(
                person_id=person_id, wikipedia_outcome=wikipedia_outcome
            ),
            reserved_nano_usd=0,
        )

    def execute(payload: _AggregatePayload) -> TaskOutcome:
        articles = _load_article_evidence(
            connection, person_id=payload.person_id, canonical_domains=canonical_domains
        )
        signals = _load_signal_evidence(connection, person_id=payload.person_id)
        outcome = aggregate_lead(
            articles=articles,
            signals=signals,
            wikipedia_outcome=payload.wikipedia_outcome,
            promising_domain_threshold=config.tasks.aggregate_lead.promising_domain_threshold,
        )
        return TaskOutcome(state=WorkState.SUCCEEDED, payload=outcome, error=None)

    def persist(work_item, outcome: LeadOutcome, *, run_id: int, now: str) -> None:
        person_id = work_item.subject_id
        ordering_factors = json.dumps(
            {
                "qualifying_domain_count": outcome.qualifying_domain_count,
                "wikipedia_outcome": outcome.wikipedia_outcome,
            },
            sort_keys=True,
        )
        lead_id = insert_lead_assessment(
            connection,
            person_id=person_id,
            run_id=run_id,
            outcome=outcome,
            lead_policy_fingerprint=policy.fingerprint,
            ordering_factors_json=ordering_factors,
            decided_at=now,
        )
        prior = fetch_prior_queue_state(connection, person_id=person_id)
        matching_page_found = outcome.wikipedia_outcome == "matching_page_found"
        decision = decide_queue_transition(
            prior=prior, outcome=outcome, matching_page_found=matching_page_found, now=now
        )
        tier = outcome.outcome if outcome.outcome in ("promising_lead", "possible_lead") else (
            prior.tier if prior else "possible_lead"
        )
        if decision.should_upsert:
            upsert_digest_queue(
                connection,
                person_id=person_id,
                status=decision.to_status or "pending",
                tier=tier,
                eligibility_reason=decision.eligibility_reason or "new",
                lead_assessment_id=lead_id,
                first_pending_at=decision.first_pending_at or now,
                last_material_change_at=now,
            )
            insert_queue_transition(
                connection,
                person_id=person_id,
                run_id=run_id,
                lead_assessment_id=lead_id,
                tier=tier,
                from_status=prior.status if prior else None,
                to_status=decision.to_status or "pending",
                reason=decision.eligibility_reason or "new",
                occurred_at=now,
            )
        elif decision.should_remove:
            remove_digest_queue(
                connection,
                person_id=person_id,
                removed_reason=decision.removed_reason or "matching_page_found",
                last_material_change_at=now,
            )
            insert_queue_transition(
                connection,
                person_id=person_id,
                run_id=run_id,
                lead_assessment_id=lead_id,
                tier=prior.tier if prior else tier,
                from_status=prior.status if prior else None,
                to_status="removed",
                reason=decision.removed_reason or "matching_page_found",
                occurred_at=now,
            )

    return TaskHandler(
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        provider=LOCAL_PROVIDER,
        operation="aggregate",
        execute=execute,
        prepare=prepare,
        persist=persist,
        persist_failure=lambda *args, **kwargs: None,
        destination_host=None,
        reserved_nano_usd=0,
        pool=None,
        ready=lambda *args, **kwargs: True,
    )


def _schedule_lead_aggregation_after_settled(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    now: str,
) -> None:
    """Best-effort same-run scheduling hook (K5 pattern). Missing source
    policy is a no-op, matching
    wikipedia/service.py's _schedule_coverage_after_wikipedia_settled."""
    from notable_person_finder.coverage.screening import (
        SourcePolicyError,
        load_source_policy,
    )
    from notable_person_finder.runs.scheduling import enqueue_work_item

    try:
        policy = load_source_policy(config.source_policy_file)
    except (SourcePolicyError, OSError):
        return
    del policy  # fingerprint/canonical-domain use happens inside the handler
    enqueue_work_item(
        connection,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        run_id=run_id,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        now=now,
    )
```

The exact names of run-engine primitives (`TaskHandler`, `TaskPreparation`,
`TaskOutcome`, `WorkState`, work-item enqueue helper, `work_item.subject_id`)
must be confirmed against `wikipedia/service.py`'s actual imports before
writing this file — copy the import line block from
`wikipedia/service.py`'s top-of-file imports verbatim and adjust only the
symbol names actually used, rather than guessing a module path. Likewise
confirm the real work-item-enqueue function name and signature (used by
`seed_coverage_research` / `schedule_coverage_after_wikipedia_ready` in
`coverage/service.py`) and match it exactly instead of the illustrative
`enqueue_work_item` above.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_service.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/leads/service.py tests/leads/test_service.py
git commit -m "feat(leads): aggregate_person_lead handler and scheduling hook (K4, K5, K6)"
```
