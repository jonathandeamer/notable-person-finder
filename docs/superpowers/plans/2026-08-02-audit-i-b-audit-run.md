# Milestone 6b-i (b): `notable audit run` — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Scope:** Tasks 3–4 of milestone 6b-i. Delivers the eight `audit run` sections and the `--attempt` drill-down.

**Prerequisite:** part (a) complete — `audit/models.py`, `audit/registry.py`, `audit/digest_show.py`, `_parse_run_id_argument`, and `tests/audit/helpers.py` all exist and their tests pass.

**Next:** `2026-08-02-audit-i-c-audit-person-and-gate.md`.

**Goal:** Add three read-only commands — `notable digest show`,
`notable audit run`, and `notable audit person` — that make the evidence
persisted by milestones 1–6a inspectable from the command line.

**Architecture:** A new `audit/` capability package owns read-only SQL
(`repository.py`), frozen view models (`models.py`), pure text rendering
(`render.py`), a `task_type`-keyed attempt-to-result registry
(`registry.py`), and digest lookup with SHA-256 verification
(`digest_show.py`). `cli/main.py` gains only parser wiring and three thin
`command_*` functions. No writes, no migrations, no model calls, no new
dependency, no mutation lock.

**Tech Stack:** Python 3.13+, stdlib `sqlite3`, `argparse`, `hashlib`;
pytest; ruff; pyright. No new third-party package.

**Authority:** `docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md`
(locked decisions K1–K14). Where this plan and the spec disagree, the spec
wins — except for the two plan-level decisions recorded under Global
Constraints below, which the spec does not cover.

## Global Constraints

- **Read-only.** No task may write to the database, run a migration, or
  acquire the mutation lock (K2, K3). Every connection is opened with
  `connect_database(path, readonly=True)`.
- **Package layering (K1).** `audit/` may import `config/`, `db/`, and
  `obs/` only. It must not import `reporting/`, `leads/`, `coverage/`,
  `wikipedia/`, `people/`, `ingestion/`, `runs/`, or `providers/`. It reads
  those packages' tables directly with its own SQL.
- **No query or formatting logic in `cli/main.py`.** Command functions open
  a connection, call `audit/`, print, and return an exit status.
- **Exit statuses (K13):** `EXIT_OK = 0`, `EXIT_FAILED = 1`,
  `EXIT_USAGE = 64`. Already defined at `cli/main.py:137-141`. Missing run
  or person → `EXIT_FAILED`. Malformed argument → `EXIT_USAGE`. These
  commands never return `EXIT_PARTIAL`.
- **Diagnostics to stderr, data to stdout.** Never mix.
- **Reference code in this plan is illustrative** except in steps that give
  a complete test body — those are the discriminating assertions and should
  be written as shown. Per `CLAUDE.md`, the shipped code and its tests
  govern; do not treat sketch implementations as canonical.
- **Type checking:** `tests/audit` is added to `[tool.pyright] include` in
  Task 1, not at the end. A new test package outside the type-checked set is
  how commit `f2e9e76` shipped an invisible type violation.
- **Mutation evidence is required** before this milestone is reported
  complete (Task 7). Restore mutated source from a `cp` backup verified with
  `diff`, never `git stash`. Run with `PYTHONDONTWRITEBYTECODE=1` and clear
  `__pycache__` between iterations.

### Plan-level decisions not covered by the spec

- **D1 — `RUN_ID` accepts `12` and `run-12`.** `notable status` prints the
  run as `run-12` (`runs/models.py:56`), so an operator copy-pasting from
  `status` will type that form. Both parse to the integer id. Anything else
  is `EXIT_USAGE`. Applies to `digest show` and `audit run`.
- **D2 — `PERSON_ID` accepts only a bare integer.** No `person-N` form is
  printed anywhere today, so there is nothing to copy-paste and no reason to
  invent a prefix.

---

## File Structure

**Created:**

| File | Responsibility |
| --- | --- |
| `src/notable_person_finder/audit/__init__.py` | Empty package marker |
| `src/notable_person_finder/audit/models.py` | Frozen view models. No SQL, no I/O |
| `src/notable_person_finder/audit/registry.py` | `task_type` → result-table registry (K9, K10) |
| `src/notable_person_finder/audit/repository.py` | All read-only SQL; returns view models |
| `src/notable_person_finder/audit/render.py` | Pure view model → `str`. No connection parameter |
| `src/notable_person_finder/audit/digest_show.py` | Digest row lookup, file read, hash verification |
| `tests/audit/__init__.py` | Test package marker (matches `tests/leads/`) |
| `tests/audit/helpers.py` | Shared fixture builders for the audit tests |
| `tests/audit/test_digest_show.py` | Task 1 |
| `tests/audit/test_registry.py` | Task 2 |
| `tests/audit/test_audit_run.py` | Tasks 3, 4 |
| `tests/audit/test_audit_person.py` | Task 5 |
| `tests/audit/test_render.py` | Task 6 |
| `tests/audit/test_seams.py` | Task 6 |

**Modified:**

| File | Change |
| --- | --- |
| `src/notable_person_finder/cli/main.py` | Parser wiring (near `:166-172`) and three thin `command_*` functions |
| `pyproject.toml` | Add `tests/audit` to `[tool.pyright] include` |
| `CLAUDE.md` | Task 7: record the milestone complete |

---

## Task 3: `notable audit run RUN_ID` — the eight sections

**Files:**
- Create: `src/notable_person_finder/audit/repository.py`,
  `audit/render.py`
- Modify: `src/notable_person_finder/audit/models.py`,
  `src/notable_person_finder/cli/main.py`
- Create: `tests/audit/test_audit_run.py`

**Interfaces:**

- Consumes: `_parse_run_id_argument` (Task 1).
- Produces:
  ```python
  # audit/models.py additions
  @dataclass(frozen=True, slots=True)
  class RunAudit:
      run: RunHeader
      configuration: ConfigurationProvenance | None
      transitions: tuple[Transition, ...]
      work_outcomes: WorkOutcomes
      attempts: tuple[AttemptLine, ...]
      failures: tuple[FailureGroup, ...]
      budget: BudgetSummary
      reporting: ReportingResult | None
      unavailable_sections: tuple[str, ...]   # K3 schema guards

  # audit/repository.py
  def load_run_audit(
      connection: sqlite3.Connection, *, run_id: int
  ) -> RunAudit | None:        # None when the run does not exist

  # audit/render.py
  def render_run_audit(audit: RunAudit) -> str: ...
  ```
  `WorkOutcomes` carries `counts: tuple[tuple[str, str, int], ...]`
  (task type, state, count) plus `failed_permanent` and `deferred` tuples of
  `WorkItemLine(id, task_type, subject_kind, subject_id, fingerprint,
  state, reason)`. `BudgetSummary` carries `limit_nano_usd: int | None`,
  `reserved_nano_usd: int`, `actual_nano_usd: int`, and
  `attempt_actual_sum_nano_usd: int`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/audit/test_audit_run.py
from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_run, migrated_database


def test_prints_every_section_heading(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    for heading in (
        "run",
        "configuration",
        "transitions",
        "work outcomes",
        "attempts",
        "failures",
        "budget",
        "reporting",
    ):
        assert heading in captured.out.lower()


def test_deferred_work_items_show_their_reason(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, reason, created_by_run_id,
                claimed_by_run_id, created_at, updated_at
            ) VALUES (
                'assess_article', 'person_article', 7, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'deferred', 'budget_exhausted', ?, ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("a" * 64, run_id, run_id),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    # The reason is the whole point of this section: status cannot explain
    # why work deferred, and this is where that answer lives.
    assert "budget_exhausted" in out
    assert "assess_article" in out


def test_budget_divergence_between_run_total_and_attempt_sum_is_shown(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute(
            "UPDATE run SET budget_actual_nano_usd = 5000 WHERE id = ?", (run_id,)
        )
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'detect_people', 'source_item', 1, ?, 1, 10,
                '2026-08-02T00:00:00Z', 'succeeded', ?,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:00Z'
            )
            """,
            ("b" * 64, run_id),
        )
        work_item_id = connection.execute(
            "SELECT id FROM work_item WHERE fingerprint = ?", ("b" * 64,)
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal,
                started_at, finished_at, outcome, request_fingerprint,
                reserved_nano_usd, actual_nano_usd
            ) VALUES (
                ?, ?, 'openrouter', 'generate_structured', 1,
                '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z', 'succeeded',
                ?, 0, 3000
            )
            """,
            (run_id, work_item_id, "c" * 64),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    out = capsys.readouterr().out
    # 5000 recorded on the run, 3000 summed from attempts. Both must appear:
    # hiding the divergence would defeat the cross-check.
    assert "5000" in out.replace(",", "") or "0.000005" in out
    assert "3000" in out.replace(",", "") or "0.000003" in out


def test_unknown_run_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id + 999), attempt_id_argument=None
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_malformed_run_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument="bogus", attempt_id_argument=None
    )

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/audit/test_audit_run.py -v`
Expected: FAIL — no `command_audit_run`.

- [ ] **Step 3: Implement the repository queries**

One function per section, composed by `load_run_audit`. Each guards on table
presence and appends to `unavailable_sections` rather than raising (K3).
Return `None` when the `run` row does not exist.

The work-outcomes query is
`SELECT task_type, state, COUNT(*) FROM work_item WHERE claimed_by_run_id = ?
OR completed_by_run_id = ? OR created_by_run_id = ? GROUP BY task_type, state`.
Use a single `run_id` parameter bound three times; a work item touched by the
run in any of those roles belongs in that run's audit.

The budget cross-check sums
`SELECT COALESCE(SUM(actual_nano_usd), 0) FROM attempt WHERE run_id = ?`.

- [ ] **Step 4: Implement `audit/render.py`**

`render_run_audit` returns one string. Sections in the K7 order, each with a
lower-case heading matching the words asserted in Step 1. Render nano-USD as
USD to six decimal places **and** print the raw integer, so both forms in the
Step 1 budget assertion are satisfiable and no precision is lost.

Any section named in `unavailable_sections` renders as
`section unavailable: schema predates migration NNNN`.

- [ ] **Step 5: Add the CLI wiring**

```python
audit = commands.add_parser("audit")
audit_commands = audit.add_subparsers(dest="audit_command", required=True)
audit_run = audit_commands.add_parser("run")
audit_run.add_argument("run_id")
audit_run.add_argument("--attempt", dest="attempt_id", default=None)
```

`command_audit_run(config_file, *, run_id_argument, attempt_id_argument)`
parses the run id via `_parse_run_id_argument`, opens a read-only
connection, calls `load_run_audit`, and prints `render_run_audit`. The
`attempt_id_argument` parameter is accepted now and ignored until Task 4, so
that Task 4 changes no signature.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/audit/test_audit_run.py -v`
Expected: PASS (5 tests).

- [ ] **Step 7: Commit**

```bash
git add src/notable_person_finder/audit tests/audit/test_audit_run.py \
  src/notable_person_finder/cli/main.py
git commit -m "feat(audit): notable audit run section rendering"
```

---

## Task 4: `--attempt ATTEMPT_ID`

**Files:**
- Modify: `src/notable_person_finder/audit/repository.py`,
  `audit/models.py`, `audit/render.py`,
  `src/notable_person_finder/cli/main.py`
- Modify: `tests/audit/test_audit_run.py`

**Interfaces:**

- Consumes: `binding_for` (Task 2), `load_run_audit` (Task 3).
- Produces:
  ```python
  # audit/models.py additions
  @dataclass(frozen=True, slots=True)
  class AttemptAudit:
      attempt: AttemptLine
      work_item: WorkItemLine
      retry_history: tuple[AttemptLine, ...]
      result_rows: tuple[Mapping[str, object], ...]
      binding: ResultBinding | None
      caveat: str | None          # set for fetch_feed
      no_result_row: bool

  # audit/repository.py
  class AttemptScopeError(Exception): ...

  def load_attempt_audit(
      connection: sqlite3.Connection, *, run_id: int, attempt_id: int
  ) -> AttemptAudit: ...
  ```
  `load_attempt_audit` raises `AttemptScopeError` when the attempt does not
  exist or belongs to a different run (K8).

- [ ] **Step 1: Write the failing tests**

Append to `tests/audit/test_audit_run.py`. The helper below seeds one
succeeded `detect_people` attempt and its `triage_observation`.

```python
def _seed_attempt(
    connection, *, run_id: int, task_type: str = "detect_people"
) -> int:
    connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at,
            updated_at
        ) VALUES (?, 'source_item', 1, ?, 1, 10, '2026-08-02T00:00:00Z',
                  'succeeded', ?, '2026-08-02T00:00:00Z',
                  '2026-08-02T00:00:00Z')
        """,
        (task_type, "d" * 64, run_id),
    )
    work_item_id = connection.execute(
        "SELECT id FROM work_item WHERE fingerprint = ?", ("d" * 64,)
    ).fetchone()[0]
    connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint, reserved_nano_usd,
            actual_nano_usd
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1,
                  '2026-08-02T00:00:00Z', '2026-08-02T00:00:01Z',
                  'succeeded', ?, 0, 100)
        """,
        (run_id, work_item_id, "e" * 64),
    )
    attempt_id = connection.execute(
        "SELECT id FROM attempt WHERE request_fingerprint = ?", ("e" * 64,)
    ).fetchone()[0]
    return attempt_id


def test_attempt_from_another_run_is_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_a = insert_run(connection)
        run_b = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_a)
        connection.commit()
    finally:
        connection.close()

    # The attempt exists, but not in run_b. Answering with run_a's data
    # would be a correctness bug dressed up as convenience.
    status = cli_main.command_audit_run(
        config_file,
        run_id_argument=str(run_b),
        attempt_id_argument=str(attempt_id),
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""
    assert str(attempt_id) in captured.err


def test_attempt_without_a_result_row_says_so(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_id)
        connection.commit()   # no triage_observation inserted
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    assert status == cli_main.EXIT_OK
    assert "no persisted result row" in out


def test_fetch_feed_attempt_carries_the_unattributable_caveat(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        attempt_id = _seed_attempt(connection, run_id=run_id, task_type="fetch_feed")
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=str(attempt_id)
    )

    out = capsys.readouterr().out
    # feed_fetch records no attempt_id, so per-attempt attribution is
    # impossible and must be disclosed rather than guessed by position.
    assert "cannot be attributed to a single attempt" in out


def test_malformed_attempt_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument="nope"
    )

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/audit/test_audit_run.py -k attempt -v`
Expected: FAIL.

- [ ] **Step 3: Implement `load_attempt_audit`**

Select the attempt by id. If absent, or if its `run_id` differs from the
requested run, raise `AttemptScopeError` (K8). Load the owning `work_item`
and all sibling attempts by `work_item_id` ordered by `ordinal`.

Resolve the result rows through `binding_for(work_item.task_type)`:

- binding is `None` → treat as no result row.
- `binding.external is False` → an attempt exists for a task type that
  should make no external call. Report the inconsistency; do not render a
  result (K10).
- `task_type == "fetch_feed"` → select all `feed_fetch` rows for
  `(feed_identity_id = work_item.subject_id, run_id)` ordered by
  `requested_at`, and set `caveat` to the K9 text.
- otherwise → select from `binding.result_table` on `binding.join_columns`.

Guard every result-table read on table presence (K3).

- [ ] **Step 4: Implement the rendering and CLI branch**

`render_attempt_audit(audit: AttemptAudit) -> str`. When `no_result_row`,
emit exactly `no persisted result row; the attempt is the only durable
evidence of this call`. When `caveat` is set, emit it verbatim above the
rows. In `command_audit_run`, parse `attempt_id_argument` with `int()`,
returning `EXIT_USAGE` on `ValueError`; on `AttemptScopeError` print to
stderr and return `EXIT_FAILED`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/audit/test_audit_run.py -v`
Expected: PASS (9 tests).

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/audit tests/audit/test_audit_run.py \
  src/notable_person_finder/cli/main.py
git commit -m "feat(audit): --attempt drill-down with run-scoped rejection"
```

---

