# Milestone 6a-iii: Lead Aggregation — Settlement Hooks, Merge Reconciliation, and CLI Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire 6a-ii's `aggregate_person_lead` handler into the live system:
same-run scheduling from coverage-plan completion and Wikipedia
`matching_page_found`, a top-of-run seed sweep for missed hooks, confirmed-
merge reconciliation replacing `people/merge.py`'s
`reconcile_digest_queue_on_merge` no-op, and `notable run` CLI registration.

**Architecture:** Three same-run scheduling call sites following the
existing K5 pattern (`wikipedia/service.py`'s
`_schedule_coverage_after_wikipedia_settled`) call a new
`_schedule_lead_aggregation_after_settled` hook. `leads/merge_hooks.py`
mirrors `coverage/merge_hooks.py`'s naming and dedup-then-reaggregate shape.
After this plan, a live `notable run` produces real `lead_assessment` and
`digest_queue` rows end-to-end (though the digest still shows the placeholder
shortlist until 6a-iv).

**Tech Stack:** Python 3.13, SQLite (stdlib `sqlite3`), pytest, ruff,
pyright, `uv`.

**Plan sequence:** This is plan 3 of 4. Depends on 6a-ii
(`docs/superpowers/plans/2026-08-01-lead-aggregation-config-repository-handler.md`)
being merged — imports `build_aggregate_person_lead_handler`,
`AGGREGATE_PERSON_LEAD_TASK_TYPE`, `AGGREGATE_PERSON_LEAD_PRIORITY` from
`leads/service.py` and the repository functions from `leads/repository.py`.
Next: 6a-iv (digest rendering + `notable status` + completion gate).

## Global Constraints

- `_schedule_lead_aggregation_after_settled` is best-effort: a missing
  source policy is a no-op (matches the existing K4/K5 exception where a
  settlement hook re-loads the policy because it runs inside another
  domain's transaction, not the handler-builder's already-loaded object).
- Survivor in a merge is always `min(resolved_loser, resolved_survivor)`
  (the lower canonical `person.id`), per the existing K7 convention in
  `people/merge.py` — this plan's merge hook must not introduce a different
  survivor rule.
- `lead_assessment` rows are immutable history and are never rewritten onto
  a merge survivor's `person_id`; only `person.current_lead_assessment_id`
  is repointed.
- Full design authority:
  `docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
  (K1–K13), especially the Work Item and Triggering and Merge Reconciliation
  sections.
- Local completion gate for this plan alone:
  `uv run pytest tests/leads tests/coverage tests/wikipedia tests/people tests/foundation`,
  `uv run ruff check .`, `uv run ruff format .`, `uv run pyright`.

---

## File Structure (this plan's scope)

```
src/notable_person_finder/leads/
    service.py             # modify: add _schedule_lead_aggregation_after_settled, seed_lead_aggregation
    merge_hooks.py          # reconcile_on_merge (replaces reconcile_digest_queue_on_merge)

src/notable_person_finder/coverage/service.py   # modify: call scheduling hook at plan-completion
src/notable_person_finder/wikipedia/service.py  # modify: call scheduling hook on matching_page_found
src/notable_person_finder/people/merge.py       # modify: call leads merge hook, delete no-op
src/notable_person_finder/cli/main.py           # modify: register handler, wire seed sweep

tests/leads/
    test_seed_and_hooks.py
    test_merge_hooks.py
    test_run_cli.py
```

---

## Task 8: Wire settlement hooks (coverage plan completion, Wikipedia match)

**Files:**
- Modify: `src/notable_person_finder/coverage/service.py` (near
  `advance_coverage_plan_after_assess` / `_persist_assess_for`, line ~4179 /
  ~3988)
- Modify: `src/notable_person_finder/wikipedia/service.py`
  `_persist_match_for` (lines 2615-2621 and 2683-2689)
- Test: `tests/leads/test_seed_and_hooks.py`

**Interfaces:**
- Consumes: `_schedule_lead_aggregation_after_settled` (Task 7).
- Produces: two additional call sites; no new public interface.

- [x] **Step 1: Write the failing seam test**

```python
# tests/leads/test_seed_and_hooks.py
"""Cross-module scheduling-hook seams. Mirrors tests/coverage/test_seams.py
and tests/wikipedia/test_seed_and_hooks.py in structure."""
import sqlite3

import pytest

from notable_person_finder.config.models import MainConfig
from notable_person_finder.db.migrate import apply_migrations


@pytest.fixture
def connection(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db")
    apply_migrations(conn)
    yield conn
    conn.close()


def test_coverage_plan_completion_schedules_aggregate_person_lead(connection):
    """When a coverage plan reaches 'completed', an aggregate_person_lead
    work item must be enqueued in the same run (K5 pattern)."""
    # Build the minimal fixture: a person, a run, a coverage plan reaching
    # 'completed' via the real coverage/service.py entry point used by
    # tests/coverage/test_seams.py — import and call that same fixture
    # helper rather than re-deriving the coverage-plan-completion sequence
    # here. Once wired, assert:
    #     row = connection.execute(
    #         "SELECT COUNT(*) FROM work_item WHERE task_type = 'aggregate_person_lead'"
    #     ).fetchone()
    #     assert row[0] == 1
    pytest.skip(
        "fill in using the same coverage-plan-completion fixture helper as "
        "tests/coverage/test_seams.py before this task is done"
    )


def test_wikipedia_matching_page_found_schedules_aggregate_person_lead(connection):
    """A matching_page_found Wikipedia observation must also schedule one
    aggregation pass so pre-match evidence still gets a closing verdict."""
    pytest.skip(
        "fill in using the same wikipedia-match fixture helper as "
        "tests/wikipedia/test_seed_and_hooks.py before this task is done"
    )
```

Before implementation, read `tests/coverage/test_seams.py` and
`tests/wikipedia/test_seed_and_hooks.py` in full and replace both
`pytest.skip(...)` bodies with real fixture setup reusing those files'
existing helper functions (import them directly if they are module-level
functions, e.g. `from tests.coverage.test_seams import
_complete_a_coverage_plan` — adjust the actual helper name found by reading
the file) so this task has no placeholder steps remaining before Step 2.

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_seed_and_hooks.py -v`
Expected: FAIL (either explicit assertion failure once fixtures are filled
in, or the `pytest.skip` calls removed and a `sqlite3.OperationalError` /
count-mismatch before the hook is wired).

- [x] **Step 3: Wire the coverage plan-completion call site**

In `coverage/service.py`, locate the point(s) where a plan transitions to
`status='completed'` or a terminal incomplete state (per K13 of the
coverage-evidence design — required work `failed_permanent` or truncated
search) inside `advance_coverage_plan_after_assess` /
`_persist_assess_for`. Immediately after `mark_plan_status(...)` calls that
set a terminal status, add:

```python
    from notable_person_finder.leads.service import (
        _schedule_lead_aggregation_after_settled,
    )

    _schedule_lead_aggregation_after_settled(
        connection, person_id=person_id, run_id=run_id, config=config, now=now
    )
```

Use the exact `person_id`/`run_id`/`config`/`now` variable names already in
scope at each call site (confirm from the actual function signature — do not
introduce new parameters if the enclosing function already has these bound).

- [x] **Step 4: Wire the Wikipedia matching_page_found call site**

In `wikipedia/service.py`'s `_persist_match_for`, at both existing call
sites of `_schedule_coverage_after_wikipedia_settled` (lines ~2615-2621 and
~2683-2689), when `outcome == "matching_page_found"`, add a call to
`_schedule_lead_aggregation_after_settled` right after the existing coverage
scheduling call:

```python
        from notable_person_finder.leads.service import (
            _schedule_lead_aggregation_after_settled,
        )

        _schedule_lead_aggregation_after_settled(
            connection, person_id=person_id, run_id=run_id, config=config, now=now
        )
```

Per the design spec: this both closes a person's evidence with a final
`lead_assessment` and lets the queue-lifecycle `matching_page_found` removal
rule (Task 4) fire even for people who already had a `digest_queue` row.

- [x] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_seed_and_hooks.py -v`
Expected: PASS

- [x] **Step 6: Run the full coverage and wikipedia suites to guard against regressions**

Run: `uv run pytest tests/coverage tests/wikipedia -v`
Expected: PASS (no existing test broken by the new import/call)

- [x] **Step 7: Commit**

```bash
git add src/notable_person_finder/coverage/service.py src/notable_person_finder/wikipedia/service.py tests/leads/test_seed_and_hooks.py
git commit -m "feat(leads): schedule aggregate_person_lead from coverage/wikipedia settlement"
```

---

## Task 9: Merge reconciliation (`leads/merge_hooks.py`)

**Files:**
- Create: `src/notable_person_finder/leads/merge_hooks.py`
- Modify: `src/notable_person_finder/people/merge.py` (replace
  `reconcile_digest_queue_on_merge` no-op at lines 34-42; wire a new
  `_reconcile_lead_aggregation_on_merge` call after `_reconcile_coverage_on_merge`
  in the `if config is not None:` block, ~lines 263-274)
- Test: `tests/leads/test_merge_hooks.py`

**Interfaces:**
- Consumes: `decide_queue_transition`/rank_key (Tasks 3-4),
  `leads.repository` (Task 6), `build_aggregate_person_lead_handler`'s
  `execute`/`persist` logic (Task 7) re-invoked directly, not through the
  scheduler.
- Produces: `reconcile_on_merge(connection, *, survivor_id, loser_id,
  run_id, config, policy, now) -> None`.

- [x] **Step 1: Write the failing tests**

```python
# tests/leads/test_merge_hooks.py
import sqlite3

import pytest

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.merge_hooks import reconcile_on_merge
from notable_person_finder.leads.repository import (
    insert_lead_assessment,
    insert_queue_transition,
    upsert_digest_queue,
)
from notable_person_finder.leads.aggregation import LeadOutcome


@pytest.fixture
def connection(tmp_path):
    conn = sqlite3.connect(tmp_path / "test.db")
    apply_migrations(conn)
    conn.execute(
        "INSERT INTO run (id, started_at, state) VALUES (1, '2026-08-01T00:00:00Z', 'running')"
    )
    conn.execute(
        "INSERT INTO person (id, created_at) VALUES (1, '2026-08-01T00:00:00Z')"
    )
    conn.execute(
        "INSERT INTO person (id, created_at) VALUES (2, '2026-08-01T00:00:00Z')"
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def empty_policy():
    return SourcePolicy(
        schema_version=1, key="test", label="Test", rules=(), fingerprint="a" * 64
    )


def test_loser_only_queue_row_moves_to_survivor(connection, empty_policy):
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=2, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=2, status="pending", tier="possible_lead",
        eligibility_reason="new", lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection, survivor_id=1, loser_id=2, run_id=1,
        config=MainConfig(), policy=empty_policy, now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    row = connection.execute(
        "SELECT person_id, status FROM digest_queue WHERE person_id = 1"
    ).fetchone()
    assert row is not None
    assert row[1] in ("pending", "removed")  # depends on fresh aggregation outcome
    loser_row = connection.execute(
        "SELECT * FROM digest_queue WHERE person_id = 2"
    ).fetchone()
    assert loser_row is None


def test_lead_assessment_history_is_not_rewritten_onto_survivor(connection, empty_policy):
    outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection, person_id=2, run_id=1, outcome=outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection, survivor_id=1, loser_id=2, run_id=1,
        config=MainConfig(), policy=empty_policy, now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    original = connection.execute(
        "SELECT person_id FROM lead_assessment WHERE id = ?", (lead_id,)
    ).fetchone()
    assert original[0] == 2  # untouched, still attributed to the loser


def test_both_sides_have_queue_rows_higher_rank_key_wins(connection, empty_policy):
    survivor_outcome = LeadOutcome(
        outcome="promising_lead", qualifying_domain_count=2,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    survivor_lead_id = insert_lead_assessment(
        connection, person_id=1, run_id=1, outcome=survivor_outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=1, status="pending", tier="promising_lead",
        eligibility_reason="new", lead_assessment_id=survivor_lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    loser_outcome = LeadOutcome(
        outcome="possible_lead", qualifying_domain_count=1,
        qualifying_articles=(), contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    loser_lead_id = insert_lead_assessment(
        connection, person_id=2, run_id=1, outcome=loser_outcome,
        lead_policy_fingerprint="a" * 64, ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection, person_id=2, status="pending", tier="possible_lead",
        eligibility_reason="new", lead_assessment_id=loser_lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection, survivor_id=1, loser_id=2, run_id=1,
        config=MainConfig(), policy=empty_policy, now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    remaining = connection.execute("SELECT person_id FROM digest_queue").fetchall()
    assert remaining == [(1,)]
```

- [x] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_merge_hooks.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [x] **Step 3: Implement `leads/merge_hooks.py`**

```python
# src/notable_person_finder/leads/merge_hooks.py
"""Confirmed-merge reconciliation for the digest queue.

Runs in two steps, matching the design spec's Merge Reconciliation section:
(1) queue dedup -- exactly one digest_queue row survives, keyed by rank_key
    when both sides had one; (2) fresh aggregation over the survivor's
    combined evidence, which both produces the new lead_assessment and
    re-evaluates the deduplicated row via the ordinary queue-lifecycle rules.
lead_assessment rows are immutable history and are never rewritten onto the
survivor's person_id (matches K12 of the durable-person-identity design).
"""

from __future__ import annotations

import sqlite3

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.ranking import QueueEntry, rank_key
from notable_person_finder.leads.repository import fetch_prior_queue_state
from notable_person_finder.leads.aggregation import LeadOutcome


def _remove_loser_queue_row(connection: sqlite3.Connection, *, person_id: int) -> None:
    connection.execute("DELETE FROM digest_queue WHERE person_id = ?", (person_id,))


def _move_queue_row_to_survivor(
    connection: sqlite3.Connection, *, survivor_id: int, loser_id: int, run_id: int, now: str
) -> None:
    row = connection.execute(
        "SELECT lead_assessment_id, tier FROM digest_queue WHERE person_id = ?",
        (loser_id,),
    ).fetchone()
    if row is None:
        return
    lead_assessment_id, tier = row
    connection.execute(
        "UPDATE digest_queue SET person_id = ? WHERE person_id = ?",
        (survivor_id, loser_id),
    )
    connection.execute(
        """
        INSERT INTO queue_transition (
            person_id, run_id, lead_assessment_id, tier, from_status,
            to_status, reason, occurred_at
        ) VALUES (?, ?, ?, ?, 'pending', 'pending', 'merged', ?)
        """,
        (survivor_id, run_id, lead_assessment_id, tier, now),
    )


def _dedup_queue_rows(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    now: str,
) -> None:
    survivor_prior = fetch_prior_queue_state(connection, person_id=survivor_id)
    loser_prior = fetch_prior_queue_state(connection, person_id=loser_id)

    if survivor_prior is None and loser_prior is not None:
        _move_queue_row_to_survivor(
            connection, survivor_id=survivor_id, loser_id=loser_id, run_id=run_id, now=now
        )
        return
    if survivor_prior is None or loser_prior is None:
        return

    survivor_outcome = LeadOutcome(
        outcome=survivor_prior.tier, qualifying_domain_count=survivor_prior.qualifying_domain_count,
        qualifying_articles=(), contributing_signals=(), wikipedia_outcome=None,
    )
    loser_outcome = LeadOutcome(
        outcome=loser_prior.tier, qualifying_domain_count=loser_prior.qualifying_domain_count,
        qualifying_articles=(), contributing_signals=(), wikipedia_outcome=None,
    )
    survivor_key = rank_key(
        QueueEntry(person_id=survivor_id, eligibility_reason="new", first_pending_at=survivor_prior.first_pending_at),
        survivor_outcome, positive_signal_count=0, best_evidence_visibility="full",
        freshest_qualifying_article_at=None, starvation_cutoff="0000-01-01T00:00:00Z",
    )
    loser_key = rank_key(
        QueueEntry(person_id=loser_id, eligibility_reason="new", first_pending_at=loser_prior.first_pending_at),
        loser_outcome, positive_signal_count=0, best_evidence_visibility="full",
        freshest_qualifying_article_at=None, starvation_cutoff="0000-01-01T00:00:00Z",
    )
    if loser_key < survivor_key:
        connection.execute("DELETE FROM digest_queue WHERE person_id = ?", (survivor_id,))
        _move_queue_row_to_survivor(
            connection, survivor_id=survivor_id, loser_id=loser_id, run_id=run_id, now=now
        )
    else:
        _remove_loser_queue_row(connection, person_id=loser_id)


def reconcile_on_merge(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    from notable_person_finder.leads.service import build_aggregate_person_lead_handler

    _dedup_queue_rows(
        connection, survivor_id=survivor_id, loser_id=loser_id, run_id=run_id, now=now
    )

    handler = build_aggregate_person_lead_handler(connection, config=config, policy=policy)

    class _WorkItem:
        subject_id = survivor_id

    preparation = handler.prepare(_WorkItem())
    outcome = handler.execute(preparation.payload)
    handler.persist(_WorkItem(), outcome.payload, run_id=run_id, now=now)
```

The inline `_WorkItem` shim exists only because `handler.prepare`/`execute`/
`persist` expect a work-item-shaped object carrying `subject_id`; if the run
engine's real `WorkItem` type is easy to construct directly (check its
constructor in `runs/engine.py` or wherever `TaskHandler` is defined), prefer
constructing a real instance over this shim.

- [x] **Step 4: Wire `people/merge.py`**

Replace the no-op function (lines 34-42) — delete it entirely, since nothing
else references `reconcile_digest_queue_on_merge` once this hook exists
under its new name. In the `if config is not None:` block (~lines 263-274),
after the existing `reconcile_wikipedia_on_merge` and
`_reconcile_coverage_on_merge` calls, add:

```python
    from notable_person_finder.leads.merge_hooks import (
        reconcile_on_merge as reconcile_lead_aggregation_on_merge,
    )
    from notable_person_finder.coverage.screening import (
        SourcePolicyError,
        load_source_policy,
    )

    lead_policy = coverage_policy if isinstance(coverage_policy, SourcePolicy) else None
    if lead_policy is None:
        try:
            lead_policy = load_source_policy(config.source_policy_file)
        except (SourcePolicyError, OSError):
            lead_policy = None
    if lead_policy is not None:
        reconcile_lead_aggregation_on_merge(
            connection,
            survivor_id=actual_survivor,
            loser_id=actual_loser,
            run_id=run_id,
            config=config,
            policy=lead_policy,
            now=now,
        )
```

Use the exact variable names already bound in `confirm_person_merge`
(`actual_survivor`, `actual_loser`, `run_id`, `now`, `coverage_policy` if
that variable exists at this point — confirm by reading the surrounding
function body before inserting).

- [x] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_merge_hooks.py tests/people -v`
Expected: PASS

- [x] **Step 6: Commit**

```bash
git add src/notable_person_finder/leads/merge_hooks.py src/notable_person_finder/people/merge.py tests/leads/test_merge_hooks.py
git commit -m "feat(leads): merge reconciliation replaces digest_queue no-op"
```

---

## Task 10: Seed sweep for missed settlement hooks

**Files:**
- Modify: `src/notable_person_finder/leads/service.py` (add `seed_lead_aggregation`)
- Modify: `src/notable_person_finder/cli/main.py` `_compose_seed()` (append
  after `seed_coverage_research`)
- Test: `tests/leads/test_seed_and_hooks.py` (extend)

**Interfaces:**
- Produces: `seed_lead_aggregation(connection, *, run_id: int, config:
  MainConfig, now: str) -> None` — same signature shape as
  `seed_coverage_research`.

- [x] **Step 1: Write the failing test**

```python
# append to tests/leads/test_seed_and_hooks.py
def test_seed_lead_aggregation_catches_person_with_completed_evidence_but_no_lead(connection):
    """Same at-least-once posture as every other milestone: a crash between
    settlement and hook-firing must be swept at the top of the next run."""
    from notable_person_finder.leads.service import seed_lead_aggregation

    connection.execute(
        "INSERT INTO run (id, started_at, state) VALUES (1, '2026-08-01T00:00:00Z', 'running')"
    )
    connection.execute(
        "INSERT INTO person (id, created_at) VALUES (1, '2026-08-01T00:00:00Z')"
    )
    # a completed person_article_assessment with no current_lead_assessment_id
    # simulates a person whose coverage plan settled but whose scheduling
    # hook was missed by a crash window
    connection.commit()

    from notable_person_finder.config.models import MainConfig

    seed_lead_aggregation(connection, run_id=1, config=MainConfig(), now="2026-08-02T00:00:00Z")
    connection.commit()

    row = connection.execute(
        "SELECT COUNT(*) FROM work_item WHERE task_type = 'aggregate_person_lead'"
    ).fetchone()
    assert row[0] == 1
```

Populate a real `person_article_assessment` completed row (not just a bare
`person`) using the same fixture helper referenced in Task 8's seam test,
since the seed sweep's eligibility condition must find a person with
completed evidence and no `current_lead_assessment_id`, not merely any
person row.

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/leads/test_seed_and_hooks.py -k seed_lead_aggregation -v`
Expected: FAIL — `ImportError`.

- [x] **Step 3: Implement `seed_lead_aggregation`**

Append to `leads/service.py`:

```python
def seed_lead_aggregation(
    connection: sqlite3.Connection, *, run_id: int, config: MainConfig, now: str
) -> None:
    """Top-of-run sweep for any person whose settlement hook was missed by a
    crash window (same at-least-once posture as every other milestone)."""
    from notable_person_finder.runs.scheduling import enqueue_work_item

    rows = connection.execute(
        """
        SELECT DISTINCT p.id
        FROM person p
        JOIN person_article_assessment paa ON paa.person_id = p.id
        WHERE paa.disposition = 'completed'
          AND p.current_lead_assessment_id IS NULL
        """
    ).fetchall()
    for (person_id,) in rows:
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

Confirm `enqueue_work_item`'s real name/signature the same way as Task 7 —
copy from `coverage/service.py`'s `seed_coverage_research` verbatim.

- [x] **Step 4: Wire into `cli/main.py`'s `_compose_seed()`**

After the existing `seed_coverage_research(...)` call, add:

```python
    from notable_person_finder.leads.service import seed_lead_aggregation

    seed_lead_aggregation(connection, run_id=run_id, config=config, now=now)
```

- [x] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_seed_and_hooks.py -v`
Expected: PASS

- [x] **Step 6: Commit**

```bash
git add src/notable_person_finder/leads/service.py src/notable_person_finder/cli/main.py tests/leads/test_seed_and_hooks.py
git commit -m "feat(leads): seed sweep for missed lead-aggregation hooks"
```

---

## Task 11: Register the handler in `cli/main.py`

**Files:**
- Modify: `src/notable_person_finder/cli/main.py` (imports ~line 105-114,
  `handlers` dict ~line 444-506)
- Test: `tests/leads/test_run_cli.py`

**Interfaces:**
- Consumes: `build_aggregate_person_lead_handler`, `AGGREGATE_PERSON_LEAD_TASK_TYPE`
  (Task 7).

- [x] **Step 1: Write the failing CLI registration test**

```python
# tests/leads/test_run_cli.py
"""End-to-end notable run CLI tests, mirroring tests/coverage/test_run_cli.py."""
import subprocess
import sys


def test_notable_run_registers_aggregate_person_lead_handler(tmp_path, monkeypatch):
    """The handler must be registered so notable run does not error with an
    unregistered work-kind if any aggregate_person_lead item is seeded."""
    # Follow the exact fixture/invocation pattern of
    # tests/coverage/test_run_cli.py's handler-registration test (temp config
    # dir, temp data root, `notable config validate` + `notable db migrate` +
    # `notable run` sequence, or the in-process CliRunner-equivalent that
    # file already uses) rather than re-deriving CLI invocation from scratch.
    import pytest

    pytest.skip(
        "port the exact CLI-invocation fixture from tests/coverage/test_run_cli.py "
        "before this task is done; read that file first"
    )
```

Replace the `pytest.skip` with the ported fixture before proceeding — this
plan intentionally defers to that existing file's exact mechanics (temp
paths, env vars for `BRAVE_API_KEY`/`OPENROUTER_API_KEY` if the fixture
needs them stubbed) rather than re-specifying them, since they were already
verified working in milestone 5.

- [x] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/leads/test_run_cli.py -v`
Expected: FAIL until the skip is replaced; then FAIL with a registration
error before Step 3.

- [x] **Step 3: Register the handler**

In `cli/main.py`, add the import alongside the existing wikipedia/coverage
imports (~line 105-114):

```python
from notable_person_finder.leads.service import (
    AGGREGATE_PERSON_LEAD_TASK_TYPE,
    build_aggregate_person_lead_handler,
)
```

In the `handlers = {...}` dict (~line 444-506), add:

```python
    AGGREGATE_PERSON_LEAD_TASK_TYPE: build_aggregate_person_lead_handler(
        connection,
        config=loaded.main,
        policy=loaded.source_policy,
    ),
```

- [x] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_run_cli.py -v`
Expected: PASS

- [x] **Step 5: Commit**

```bash
git add src/notable_person_finder/cli/main.py tests/leads/test_run_cli.py
git commit -m "feat(cli): register aggregate_person_lead handler"
```
