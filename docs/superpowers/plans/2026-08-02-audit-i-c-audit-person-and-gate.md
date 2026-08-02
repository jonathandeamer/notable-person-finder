# Milestone 6b-i (c): `notable audit person`, Seams, and Completion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Scope:** Tasks 5–7 of milestone 6b-i. Delivers `notable audit person`, the cross-cutting layering/lock/redaction seams, the mutation-evidence pass, the full completion gate, and the `CLAUDE.md` update.

**Prerequisite:** parts (a) and (b) complete.

**This part closes the milestone.**

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

## Task 5: `notable audit person PERSON_ID`

**Files:**
- Modify: `src/notable_person_finder/audit/repository.py`,
  `audit/models.py`, `audit/render.py`,
  `src/notable_person_finder/cli/main.py`
- Create: `tests/audit/test_audit_person.py`

**Interfaces:**

- Produces:
  ```python
  # audit/models.py additions
  @dataclass(frozen=True, slots=True)
  class PersonAudit:
      identity: PersonIdentity          # includes merged_into_person_id
      merged_into: PersonIdentity | None
      merged_by_run_id: int | None
      sourced_names: tuple[SourcedNameLine, ...]
      relations: tuple[RelationLine, ...]
      mentions: tuple[MentionLine, ...]
      resolutions: tuple[ResolutionLine, ...]
      wikipedia: WikipediaEvidence
      coverage: CoverageEvidence
      assessments: tuple[AssessmentLine, ...]
      leads: tuple[LeadLine, ...]
      queue: QueueEvidence
      digest_entries: tuple[DigestEntryLine, ...]
      unavailable_sections: tuple[str, ...]

  # audit/repository.py
  def load_person_audit(
      connection: sqlite3.Connection, *, person_id: int
  ) -> PersonAudit | None: ...

  # audit/render.py
  def render_person_audit(audit: PersonAudit) -> str: ...
  ```

- [ ] **Step 1: Write the failing tests**

The mention test is the one that discriminates K11. It needs a person whose
current association differs from a historical observation — without that
divergence, re-deriving mentions from entity-resolution history would still
pass and the rule would be untested.

```python
# tests/audit/test_audit_person.py
from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_run, migrated_database


def _person(connection, *, run_id: int, name: str, fingerprint: str) -> int:
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES ('2026-08-02T00:00:00Z', ?, ?, ?)
        """,
        (run_id, name, fingerprint),
    )
    person_id = cursor.lastrowid
    assert person_id is not None
    return person_id


def test_prints_every_section_heading(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Ada Example", fingerprint="1" * 64
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument=str(person_id))

    out = capsys.readouterr().out.lower()
    assert status == cli_main.EXIT_OK
    for heading in (
        "identity",
        "sourced names",
        "relations",
        "mentions",
        "entity resolution",
        "wikipedia",
        "coverage",
        "assessments",
        "lead history",
        "queue history",
        "digest history",
    ):
        assert heading in out


def test_empty_sections_are_marked_not_omitted(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_id = _person(
            connection, run_id=run_id, name="Empty Person", fingerprint="2" * 64
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_person(config_file, person_id_argument=str(person_id))

    out = capsys.readouterr().out
    # A person with no coverage must still show the heading with an explicit
    # marker; a silently omitted section reads as "not queried".
    assert "none" in out.lower()


def test_mentions_use_the_current_person_id_not_resolution_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """K11: person_mention.person_id is the durable association.

    Seeds a mention whose *current* person_id is B while an entity-resolution
    observation still records selected_person_id = A. Deriving mentions from
    the observation would attribute the mention to A and miss it under B.
    """
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        person_a = _person(
            connection, run_id=run_id, name="Person A", fingerprint="3" * 64
        )
        person_b = _person(
            connection, run_id=run_id, name="Person B", fingerprint="4" * 64
        )
        connection.execute(
            """
            INSERT INTO source_item (
                canonical_article_id, feed_identity_id, run_id, external_id,
                title, published_at, fetched_at, summary, link
            )
            SELECT 1, 1, ?, 'ext-1', 'Title', '2026-08-02T00:00:00Z',
                   '2026-08-02T00:00:00Z', 'Summary', 'https://example.test/a'
            """,
            (run_id,),
        )
        source_item_id = connection.execute(
            "SELECT id FROM source_item ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO triage_observation (
                source_item_id, run_id, attempt_id, disposition, outcome,
                model, prompt_hash, schema_hash, schema_version,
                task_fingerprint, rationale, decided_at
            ) VALUES (?, ?, NULL, 'skipped', 'research', 'test-model',
                      ?, ?, 1, ?, 'seeded', '2026-08-02T00:00:00Z')
            """,
            (source_item_id, run_id, "a" * 64, "b" * 64, "c" * 64),
        )
        triage_id = connection.execute(
            "SELECT id FROM triage_observation ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name,
                outcome, supporting_passage_ids_json, rationale, person_id
            ) VALUES (?, 1, 'Reassigned Name', 'reassigned name', 'research',
                      '[]', 'seeded', ?)
            """,
            (triage_id, person_b),
        )
        mention_id = connection.execute(
            "SELECT id FROM person_mention ORDER BY id DESC LIMIT 1"
        ).fetchone()[0]
        connection.execute(
            """
            INSERT INTO entity_resolution_observation (
                person_mention_id, run_id, disposition, semantic_outcome,
                selected_person_id, candidate_person_ids_json,
                canonical_supplied_input_json, prompt_hash, schema_hash,
                schema_version, task_fingerprint, rationale, decided_at
            ) VALUES (?, ?, 'completed', 'same_person', ?, '[]', '{}',
                      ?, ?, 1, ?, 'historical', '2026-08-02T00:00:00Z')
            """,
            (mention_id, run_id, person_a, "d" * 64, "e" * 64, "f" * 64),
        )
        connection.commit()
    finally:
        connection.close()

    cli_main.command_audit_person(config_file, person_id_argument=str(person_b))
    out_b = capsys.readouterr().out

    assert "Reassigned Name" in out_b


def test_merged_away_person_shows_a_banner_and_its_own_history(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        survivor = _person(
            connection, run_id=run_id, name="Survivor", fingerprint="5" * 64
        )
        merged = _person(
            connection, run_id=run_id, name="Merged Away", fingerprint="6" * 64
        )
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, merged),
        )
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument=str(merged))

    out = capsys.readouterr().out
    assert status == cli_main.EXIT_OK
    # Must NOT redirect: the merged person's own name and the survivor's id
    # both appear, because a merge is what the operator came to understand.
    assert "Merged Away" in out
    assert str(survivor) in out


def test_unknown_person_reports_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument="4242")

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_FAILED
    assert captured.out == ""


def test_malformed_person_id_is_a_usage_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file, connection = migrated_database(tmp_path)
    try:
        insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_person(config_file, person_id_argument="person-1")

    assert status == cli_main.EXIT_USAGE
    assert capsys.readouterr().out == ""
```

If the `source_item` insert in the mention test fails on a foreign key or a
missing column, read `0003_ingestion.sql:105` and adjust the column list to
match the shipped schema — the surrounding assertions are what matter, not
this particular seed shape.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/audit/test_audit_person.py -v`
Expected: FAIL — no `command_audit_person`.

- [ ] **Step 3: Implement `load_person_audit`**

Eleven section queries in the K11 order. The ones with a shape worth stating
explicitly:

- **Mentions:** `SELECT ... FROM person_mention WHERE person_id = ?`, joined
  to `triage_observation` for the source item and left-joined to
  `entity_resolution_observation` on
  `person_mention.current_entity_resolution_observation_id` for the current
  `semantic_outcome`. Do **not** derive this set from
  `entity_resolution_observation.selected_person_id`.
- **Relations:** `WHERE person_id_a = ? OR person_id_b = ?`; render the
  *other* person's id in each row.
- **Merged-away:** `person.merged_into_person_id`. When non-null, load the
  survivor's identity row. Derive `merged_by_run_id` from the `merge`
  `person_relation` row joining the two, if present.
- **Leads:** order by `id`; mark the row whose id equals
  `person.current_lead_assessment_id`.
- **Wikipedia:** mark the observation whose id equals
  `person.current_wikipedia_identity_observation_id`.

Guard every section on table presence (K3): a database migrated only through
0006 has no `person_coverage_plan` and no `lead_assessment`.

- [ ] **Step 4: Implement rendering and CLI wiring**

`render_person_audit` emits all eleven headings unconditionally. An empty
section renders its heading followed by `none`. When `merged_into` is set,
emit a banner above section 1 naming the survivor id and the merging run,
and stating that the survivor's history is at
`notable audit person <survivor_id>`.

```python
audit_person = audit_commands.add_parser("person")
audit_person.add_argument("person_id")
```

`command_audit_person(config_file, *, person_id_argument)` parses with a
bare `int()` per D2, returning `EXIT_USAGE` on `ValueError`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/audit/test_audit_person.py -v`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/audit tests/audit/test_audit_person.py \
  src/notable_person_finder/cli/main.py
git commit -m "feat(audit): notable audit person with merged-away banner"
```

---

## Task 6: Seams, layering, redaction, and pure rendering

**Files:**
- Create: `tests/audit/test_seams.py`, `tests/audit/test_render.py`

**Interfaces:** consumes everything from Tasks 1–5. Produces no new source
interface; this task adds the cross-cutting guards.

- [ ] **Step 1: Write the layering and lock tests**

```python
# tests/audit/test_seams.py
from __future__ import annotations

import ast
import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from tests.audit.helpers import insert_run, migrated_database

AUDIT_PACKAGE = Path("src/notable_person_finder/audit")
FORBIDDEN = {
    "reporting",
    "leads",
    "coverage",
    "wikipedia",
    "people",
    "ingestion",
    "runs",
    "providers",
}


def _imported_sibling_packages(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            parts = node.module.split(".")
            if parts[0] == "notable_person_finder" and len(parts) > 1:
                found.add(parts[1])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if parts[0] == "notable_person_finder" and len(parts) > 1:
                    found.add(parts[1])
    return found


def test_audit_package_imports_no_forbidden_sibling() -> None:
    detected = {
        package
        for source in AUDIT_PACKAGE.glob("*.py")
        for package in _imported_sibling_packages(source)
    }
    # Positive control. `audit/` genuinely imports from `db` and `config`, so
    # a detector that silently returned an empty set -- a broken AST walk, a
    # wrong glob, a renamed package -- fails HERE rather than making the
    # forbidden-import assertion below pass for the wrong reason.
    assert "db" in detected
    assert "config" in detected

    for source in AUDIT_PACKAGE.glob("*.py"):
        assert not (_imported_sibling_packages(source) & FORBIDDEN), source


def test_audit_does_not_import_the_digest_renderer() -> None:
    # K5: digest show reads bytes; it must never re-render.
    for source in AUDIT_PACKAGE.glob("*.py"):
        text = source.read_text(encoding="utf-8")
        assert "reporting.digest" not in text, source


def test_commands_succeed_while_another_handle_holds_the_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # K2: read-only commands take no mutation lock.
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")

        status = cli_main.command_audit_run(
            config_file, run_id_argument=str(run_id), attempt_id_argument=None
        )
        assert status == cli_main.EXIT_OK
        connection.rollback()
    finally:
        connection.close()
    capsys.readouterr()


def test_audit_does_not_migrate_an_out_of_date_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # K3: the schema must be unchanged after the command runs.
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute("DROP TABLE digest_entry")
        connection.execute("DROP TABLE digest_queue")
        connection.commit()
        before = connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'"
        ).fetchone()[0]
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )
    assert status == cli_main.EXIT_OK

    database = load_config(config_file, require_secrets=False).paths.database
    check = sqlite3.connect(str(database))
    try:
        after = check.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'"
        ).fetchone()[0]
    finally:
        check.close()
    assert after == before
    capsys.readouterr()
```

This test needs `from notable_person_finder.config.loader import load_config`
at the top of the module alongside the existing imports. `load_config` is the
authoritative path resolver; never hard-code the database location.

- [ ] **Step 2: Write the redaction positive control**

Append to `tests/audit/test_seams.py`:

```python
def test_secrets_never_appear_in_audit_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """K14. Positive control included: an assertion that a sentinel is
    absent proves nothing unless the sentinel was reachable in the first
    place."""
    brave_sentinel = "brave-sentinel-6b1i-do-not-leak"
    openrouter_sentinel = "openrouter-sentinel-6b1i-do-not-leak"
    monkeypatch.setenv("BRAVE_API_KEY", brave_sentinel)
    monkeypatch.setenv("OPENROUTER_API_KEY", openrouter_sentinel)

    # Positive control: the sentinels ARE readable in this process, so a
    # later "not in output" assertion is meaningful rather than vacuous.
    import os

    assert os.environ["BRAVE_API_KEY"] == brave_sentinel
    assert os.environ["OPENROUTER_API_KEY"] == openrouter_sentinel

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
    assert brave_sentinel not in captured.out
    assert brave_sentinel not in captured.err
    assert openrouter_sentinel not in captured.out
    assert openrouter_sentinel not in captured.err
    # The configuration section really did render; without this the test
    # could pass on empty output.
    assert "configuration" in captured.out.lower()
```

- [ ] **Step 3: Write the pure-rendering tests**

```python
# tests/audit/test_render.py
from __future__ import annotations

from notable_person_finder.audit import render


def test_render_functions_take_no_connection() -> None:
    # render.py must be callable with view models alone. A connection
    # parameter would mean query logic leaked into the renderer.
    import inspect

    for name in ("render_run_audit", "render_attempt_audit", "render_person_audit"):
        signature = inspect.signature(getattr(render, name))
        assert "connection" not in signature.parameters, name
```

Add one constructed-view-model test per renderer, building the frozen models
directly from `audit/models.py` with distinct field values (as
`tests/leads/test_digest_status.py` does with nine distinct counters) and
asserting each value's exact rendered line. Distinct values are what catch a
swapped-field defect; equal values let it survive.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/audit -v`
Expected: PASS. Fix any layering violation the AST test finds by moving the
offending query into `audit/repository.py` — do not relax the test.

- [ ] **Step 5: Run the static gates**

```bash
uv run ruff check .
uv run ruff format .
uv run pyright
```

- [ ] **Step 6: Commit**

```bash
git add tests/audit
git commit -m "test(audit): layering, lock, migration, redaction, render seams"
```

---

## Task 7: Mutation evidence, full gate, and documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: any test that a surviving mutation proves inadequate

- [ ] **Step 1: Back up the source tree**

```bash
cp -R src/notable_person_finder/audit /tmp/audit-backup
export PYTHONDONTWRITEBYTECODE=1
```

- [ ] **Step 2: Run each mutation, recording which named test dies**

For each row: apply the mutation, clear caches, run the suite, record the
failing test name, restore, and verify the restore with `diff`.

```bash
find . -name __pycache__ -type d -prune -exec rm -rf {} +
uv run pytest tests/audit -q
diff -r src/notable_person_finder/audit /tmp/audit-backup   # must be silent
```

| # | Mutation | Must kill |
| --- | --- | --- |
| 1 | `read_verified_digest` returns bytes without comparing the hash | `test_hash_mismatch_writes_nothing_to_stdout` |
| 2 | On mismatch, write the body to stdout before returning failure | `test_hash_mismatch_writes_nothing_to_stdout` |
| 3 | `locate_digest` orders by lowest `run_id` | `test_defaults_to_the_highest_run_id` |
| 4 | Import `reporting.digest` in `digest_show.py` and render from it | `test_audit_does_not_import_the_digest_renderer` |
| 5 | Re-key `REGISTRY` on `(provider, operation)` | `test_openrouter_sharing_task_types_resolve_to_distinct_tables` |
| 6 | Delete the `aggregate_person_lead` entry | `test_every_registered_handler_task_type_has_a_binding` |
| 7 | Add a thirteenth binding for a fictitious task type | `test_every_registered_handler_task_type_has_a_binding` |
| 8 | `load_attempt_audit` drops the `run_id` equality check | `test_attempt_from_another_run_is_rejected` |
| 9 | Emit an empty section instead of the no-result-row sentence | `test_attempt_without_a_result_row_says_so` |
| 10 | Drop the `fetch_feed` caveat string | `test_fetch_feed_attempt_carries_the_unattributable_caveat` |
| 11 | Derive mentions from `entity_resolution_observation.selected_person_id` | `test_mentions_use_the_current_person_id_not_resolution_history` |
| 12 | Redirect a merged-away person to the survivor | `test_merged_away_person_shows_a_banner_and_its_own_history` |
| 13 | Remove the `sqlite_master` table guard from one section | a named `test_audit_*` test |
| 14 | Return `EXIT_OK` for an unknown run | `test_unknown_run_reports_failure` |
| 15 | Return `EXIT_FAILED` instead of `EXIT_USAGE` on a bad id | `test_malformed_run_id_is_a_usage_error` |
| 16 | Print `canonical_json` bypassing `obs.logging.redact` | `test_secrets_never_appear_in_audit_output` |

**A mutation that survives means the rule is untested.** Strengthen the test
until it dies, then re-run. Record the surviving-then-killed pair in the
completion report — that pair is the evidence, not the final green run.

Mutation 16 will survive if the configuration snapshot genuinely never
contains a secret. That is the expected outcome and it is not a failure of
the test: record it explicitly as "survives because the invariant holds
upstream", and keep the test as the regression guard for the day a snapshot
starts carrying one.

- [ ] **Step 3: Restore and confirm the tree is clean**

```bash
rm -rf src/notable_person_finder/audit
cp -R /tmp/audit-backup src/notable_person_finder/audit
find . -name __pycache__ -type d -prune -exec rm -rf {} +
git status --short
git diff --check
```

- [ ] **Step 4: Run the full completion gate**

```bash
uv run pytest tests/foundation tests/run_engine tests/ingestion \
  tests/people tests/wikipedia tests/coverage tests/leads tests/audit
uv run ruff check .
uv run ruff format .
uv run pyright
```

Expected: all pass. The pre-existing baseline is 1921 passed, 14 deselected.

- [ ] **Step 5: Update `CLAUDE.md`**

Move `notable digest show`, `notable audit run`, and `notable audit person`
out of "Not built at all" into "Delivered and usable". Add `audit/` to the
Rewrite Structure package list and `tests/audit/` to the test list. Update
the milestone count to nine and extend the combined gate command with
`tests/audit`. Record under known gaps that `--attempt` shows the validated
persisted result and not raw provider bodies, because none are persisted,
per the spec's recorded amendment.

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md tests/audit
git commit -m "docs: record milestone 6b-i complete"
```

- [ ] **Step 7: Report completion**

Report: the gate output, the mutation table with the specific test that died
for each row, any mutation that survived and what was strengthened, and
confirmation that `git status --short` and `git diff --check` are clean.

Do not push, open a pull request, or merge without explicit user
authorization.

---

## Self-Review

**Spec coverage.** K1 → Tasks 1, 6. K2 → Task 6. K3 → Tasks 1, 3, 5, 6.
K4 → Task 1. K5 → Tasks 1, 6. K6 → Task 1. K7 → Task 3. K8 → Task 4.
K9 → Tasks 2, 4. K10 → Task 2. K11 → Task 5. K12 → Task 5. K13 → Tasks 1,
3, 4, 5. K14 → Task 6. The spec's amendment and non-goals are recorded in
Task 7 Step 5. No K-numbered decision is unimplemented.

**Type consistency.** `command_digest_show(config_file, *,
run_id_argument)`, `command_audit_run(config_file, *, run_id_argument,
attempt_id_argument)`, and `command_audit_person(config_file, *,
person_id_argument)` are used with those exact keyword names in every test
body. `_parse_run_id_argument` is defined in Task 1 and reused in Task 3.
`binding_for` is defined in Task 2 and used in Task 4. `ResultBinding`
fields (`task_type`, `result_table`, `join_columns`, `external`,
`provenance_columns`) are consistent between Tasks 2 and 4.

**Known soft spots, flagged rather than hidden.** Two test seeds in Tasks 5
and 6 hard-code a schema shape (`source_item` columns, the database path)
that the implementer should verify against the shipped migration and config
resolver; both steps say so inline and name the authoritative source. These
are seed mechanics, not assertions — the surrounding assertions are what the
task delivers.
