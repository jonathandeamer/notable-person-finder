# Milestone 6b-i (a): `notable digest show` and the Registry — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Scope:** Tasks 1–2 of milestone 6b-i. Creates the `audit/` package, delivers `notable digest show` end to end, and builds the `task_type`-keyed attempt-to-result registry.

**Prerequisite:** none beyond milestone 6a on `refactor/rearchitecture`.

**Next:** `2026-08-02-audit-i-b-audit-run.md`.

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

## Task 1: Package skeleton and `notable digest show`

Delivers a working command end to end, so the package layout is validated by
something real before four more tasks depend on it.

**Files:**
- Create: `src/notable_person_finder/audit/__init__.py`,
  `audit/models.py`, `audit/digest_show.py`
- Create: `tests/audit/__init__.py`, `tests/audit/helpers.py`,
  `tests/audit/test_digest_show.py`
- Modify: `src/notable_person_finder/cli/main.py`, `pyproject.toml`

**Interfaces:**

- Produces:
  ```python
  # audit/models.py
  @dataclass(frozen=True, slots=True)
  class DigestLocation:
      run_id: int
      file_path: str
      content_hash: str
      timezone: str | None      # None on the pre-0008 run-column fallback
      window_start: str | None
      window_end: str | None
      run_state: str | None
      created_at: str | None
      source: str               # "digest_table" | "run_columns"

  # audit/digest_show.py
  class DigestLookupError(Exception):
      """Carries an operator-facing message; never a bare traceback."""

  def locate_digest(
      connection: sqlite3.Connection, *, run_id: int | None
  ) -> DigestLocation: ...

  def read_verified_digest(location: DigestLocation) -> bytes: ...
  ```
  `locate_digest` raises `DigestLookupError` when the run does not exist or
  produced no digest. `read_verified_digest` raises `DigestLookupError` when
  the file is missing, unreadable, or fails hash verification.

- [ ] **Step 1: Create the package markers and the pyright include**

```bash
mkdir -p src/notable_person_finder/audit tests/audit
touch src/notable_person_finder/audit/__init__.py tests/audit/__init__.py
```

In `pyproject.toml`, add `"tests/audit",` to `[tool.pyright] include` after
the existing `"tests/leads",` entry.

- [ ] **Step 2: Write `tests/audit/helpers.py`**

The audit tests need a migrated database with a run in it. Reuse the
existing conventions rather than inventing new ones — `tests/leads/
test_digest_status.py:18-36` shows the imports these helpers mirror.

```python
"""Shared fixture builders for the audit tests."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from notable_person_finder.config.loader import load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from tests.ingestion.helpers import insert_configuration_snapshot, moment
from tests.people.test_run_cli import _single_feed, write_people_graph


def migrated_database(tmp_path: Path) -> tuple[Path, sqlite3.Connection]:
    """Return (config_file, open read-write connection) on a migrated db."""
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)
    connection = connect_database(database)
    apply_migrations(connection, database, loaded.paths.backups)
    return config_file, connection


def insert_run(
    connection: sqlite3.Connection,
    *,
    state: str = "complete",
    started_at: str | None = None,
    finished_at: str | None = None,
) -> int:
    snapshot_id = insert_configuration_snapshot(connection)
    started = started_at or moment()
    finished = finished_at if finished_at is not None else moment()
    if state == "running":
        finished = None
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone, window_start,
            window_end, started_at, finished_at, budget_limit_nano_usd,
            budget_reserved_nano_usd, budget_actual_nano_usd
        ) VALUES (?, ?, 'UTC', ?, ?, ?, ?, NULL, 0, 0)
        """,
        (state, started, started, started, finished),
    )
    run_id = cursor.lastrowid
    assert run_id is not None
    return run_id


def insert_digest_row(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    content_hash: str,
    run_state: str = "complete",
) -> int:
    now = moment()
    cursor = connection.execute(
        """
        INSERT INTO digest (
            run_id, file_path, timezone, window_start, window_end,
            run_state, content_hash, created_at
        ) VALUES (?, ?, 'UTC', ?, ?, ?, ?, ?)
        """,
        (run_id, file_path, now, now, run_state, content_hash, now),
    )
    digest_id = cursor.lastrowid
    assert digest_id is not None
    return digest_id
```

- [ ] **Step 3: Write the failing tests**

These are the discriminating assertions for K4 and K6. Write them exactly.

```python
# tests/audit/test_digest_show.py
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_digest_row, insert_run, migrated_database

BODY = "# Digest\n\nreal persisted content\n"


def _seed(tmp_path: Path, *, body: str = BODY) -> tuple[Path, Path, int]:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        digest_file = tmp_path / "digest.md"
        digest_file.write_text(body, encoding="utf-8")
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
        insert_digest_row(
            connection,
            run_id=run_id,
            file_path=str(digest_file),
            content_hash=digest,
        )
        connection.commit()
    finally:
        connection.close()
    return config_file, digest_file, run_id


def test_prints_verified_digest_verbatim_and_nothing_else(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    # Verbatim: stdout must hash back to the recorded content_hash. Any
    # banner, header, or trailing summary breaks this equality, which is
    # what makes it a real assertion of K6 rather than a substring check.
    assert captured.out == BODY


def test_hash_mismatch_writes_nothing_to_stdout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, digest_file, run_id = _seed(tmp_path)
    digest_file.write_text("tampered\n", encoding="utf-8")

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    # The point of the check: a piped invocation must forward nothing.
    assert captured.out == ""
    assert "tampered" not in captured.err
    assert "hash" in captured.err.lower()


def test_missing_file_reports_and_prints_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, digest_file, run_id = _seed(tmp_path)
    digest_file.unlink()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""
    assert str(digest_file) in captured.err


def test_defaults_to_the_highest_run_id(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        older_body = "# older\n"
        newer_body = "# newer\n"
        for body in (older_body, newer_body):
            run_id = insert_run(connection)
            path = tmp_path / f"digest-{run_id}.md"
            path.write_text(body, encoding="utf-8")
            insert_digest_row(
                connection,
                run_id=run_id,
                file_path=str(path),
                content_hash=hashlib.sha256(body.encode("utf-8")).hexdigest(),
            )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=None)

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    assert captured.out == newer_body


def test_accepts_the_human_id_form(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(
        config_file, run_id_argument=f"run-{run_id}"
    )

    assert status == cli_main.EXIT_OK
    assert capsys.readouterr().out == BODY


def test_malformed_run_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, _ = _seed(tmp_path)

    status = cli_main.command_digest_show(config_file, run_id_argument="not-a-run")

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_USAGE
    assert captured.out == ""


def test_unknown_run_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, _, run_id = _seed(tmp_path)

    status = cli_main.command_digest_show(
        config_file, run_id_argument=str(run_id + 999)
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_falls_back_to_run_columns_when_no_digest_row(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        path = tmp_path / "legacy.md"
        path.write_text(BODY, encoding="utf-8")
        connection.execute(
            "UPDATE run SET digest_path = ?, digest_sha256 = ? WHERE id = ?",
            (str(path), hashlib.sha256(BODY.encode("utf-8")).hexdigest(), run_id),
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_digest_show(config_file, run_id_argument=str(run_id))

    assert status == cli_main.EXIT_OK
    assert capsys.readouterr().out == BODY
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `uv run pytest tests/audit/test_digest_show.py -v`
Expected: FAIL — `AttributeError: module ... has no attribute
'command_digest_show'`. Note this is the trivial failure; it proves the test
runs, not that it discriminates. Task 7 supplies that evidence.

- [ ] **Step 5: Implement `audit/models.py` and `audit/digest_show.py`**

`locate_digest` selects from `digest` by `run_id`, or with the highest
`run_id` when the argument is `None`. When the run exists but has no
`digest` row, fall back to `run.digest_path` / `run.digest_sha256`. Guard
both lookups on table presence (K3) using the
`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?` idiom already
used at `cli/main.py:661-667`.

`read_verified_digest` reads the file as bytes, compares
`hashlib.sha256(data).hexdigest()` to `location.content_hash`, and returns
the bytes only on a match. Decode as UTF-8 and raise `DigestLookupError` on
`UnicodeDecodeError`, `FileNotFoundError`, and `OSError`, each with a
message naming the path.

- [ ] **Step 6: Add the CLI wiring**

In `build_parser` near `cli/main.py:166-172`:

```python
digest = commands.add_parser("digest")
digest_show = digest.add_subparsers(dest="digest_command", required=True).add_parser(
    "show"
)
digest_show.add_argument("run_id", nargs="?", default=None)
```

Add a module-level helper for D1, used by this command and by Task 3:

```python
def _parse_run_id_argument(value: str) -> int:
    """Accept `12` or `run-12`. Raises ValueError otherwise."""
    text = value.removeprefix("run-")
    return int(text)
```

Then the thin command:

```python
def command_digest_show(config_file: Path | None, *, run_id_argument: str | None) -> int:
    loaded = load_config(config_file, require_secrets=False)
    if run_id_argument is not None:
        try:
            run_id = _parse_run_id_argument(run_id_argument)
        except ValueError:
            print(f"invalid run id: {run_id_argument}", file=sys.stderr)
            return EXIT_USAGE
    else:
        run_id = None
    if not loaded.paths.database.exists():
        print("no run has been recorded yet", file=sys.stderr)
        return EXIT_FAILED
    connection = connect_database(loaded.paths.database, readonly=True)
    try:
        location = locate_digest(connection, run_id=run_id)
        body = read_verified_digest(location)
    except DigestLookupError as error:
        print(str(error), file=sys.stderr)
        return EXIT_FAILED
    finally:
        connection.close()
    sys.stdout.write(body.decode("utf-8"))
    return EXIT_OK
```

Wire it into the command dispatch alongside the existing commands.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/audit/test_digest_show.py -v`
Expected: PASS (8 tests).

- [ ] **Step 8: Commit**

```bash
git add src/notable_person_finder/audit tests/audit pyproject.toml \
  src/notable_person_finder/cli/main.py
git commit -m "feat(audit): notable digest show with hash verification"
```

---

## Task 2: The `task_type` registry

Pure data plus a completeness test. No CLI, no SQL execution.

**Files:**
- Create: `src/notable_person_finder/audit/registry.py`
- Create: `tests/audit/test_registry.py`

**Interfaces:**

- Produces:
  ```python
  # audit/registry.py
  @dataclass(frozen=True, slots=True)
  class ResultBinding:
      task_type: str
      result_table: str
      join_columns: tuple[str, ...]   # columns on the result table
      external: bool
      provenance_columns: tuple[str, ...] = ()

  REGISTRY: Mapping[str, ResultBinding]

  def binding_for(task_type: str) -> ResultBinding | None: ...
  ```

- [ ] **Step 1: Write the failing test**

The third test is the one that matters: it is the regression guard for the
`(provider, operation)` ambiguity that this decision exists to prevent.

```python
# tests/audit/test_registry.py
from __future__ import annotations

from notable_person_finder.audit.registry import REGISTRY, binding_for


def test_every_registered_handler_task_type_has_a_binding() -> None:
    # The twelve task types the run engine registers today. Kept as a
    # literal so that adding a handler without an audit binding fails here
    # loudly rather than silently producing a blank --attempt view.
    expected = {
        "fetch_feed",
        "inspect_model",
        "detect_people",
        "resolve_person_entity",
        "reconsider_person_entity",
        "mediawiki_search",
        "mediawiki_page_facts",
        "match_wikipedia_identity",
        "brave_web_search",
        "fetch_article",
        "assess_article",
        "aggregate_person_lead",
    }
    assert set(REGISTRY) == expected


def test_the_local_handler_is_present_and_marked_non_external() -> None:
    binding = binding_for("aggregate_person_lead")
    assert binding is not None
    assert binding.external is False
    assert binding.result_table == "lead_assessment"


def test_openrouter_sharing_task_types_resolve_to_distinct_tables() -> None:
    # detect_people, resolve_person_entity, reconsider_person_entity,
    # match_wikipedia_identity and assess_article all run through
    # providers/openrouter.py's single GENERATE_OPERATION constant. Keying
    # the registry on (provider, operation) would collapse these five to one
    # entry; keying on task_type must not.
    tables = {
        task_type: binding_for(task_type).result_table  # type: ignore[union-attr]
        for task_type in (
            "detect_people",
            "resolve_person_entity",
            "reconsider_person_entity",
            "match_wikipedia_identity",
            "assess_article",
        )
    }
    assert tables == {
        "detect_people": "triage_observation",
        "resolve_person_entity": "entity_resolution_observation",
        "reconsider_person_entity": "entity_resolution_observation",
        "match_wikipedia_identity": "wikipedia_identity_observation",
        "assess_article": "person_article_assessment",
    }


def test_all_but_the_local_handler_are_external() -> None:
    external = {name for name, b in REGISTRY.items() if b.external}
    assert len(external) == 11
    assert "aggregate_person_lead" not in external


def test_unknown_task_type_returns_none() -> None:
    assert binding_for("no_such_task") is None
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/audit/test_registry.py -v`
Expected: FAIL — `ModuleNotFoundError: notable_person_finder.audit.registry`.

- [ ] **Step 3: Implement `audit/registry.py`**

Twelve `ResultBinding` entries exactly as tabulated in K9 of the spec.
`fetch_feed` binds `feed_fetch` with join columns
`("feed_identity_id", "run_id")` and is resolved from
`work_item.subject_id`, not from the attempt. `aggregate_person_lead` binds
`lead_assessment` with `("person_id", "run_id")` and `external=False`.
Everything else joins `("attempt_id", "run_id")` except `model_inspection`
and `wikipedia_page_facts_batch`, which join on `attempt_id` alone.

Do **not** import from `people/`, `wikipedia/`, `coverage/`, `leads/`,
`ingestion/`, or `runs/` to obtain the task-type strings — that would breach
K1. Declare them as literals in this module.

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/audit/test_registry.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/audit/registry.py tests/audit/test_registry.py
git commit -m "feat(audit): task_type-keyed attempt-to-result registry"
```

---

