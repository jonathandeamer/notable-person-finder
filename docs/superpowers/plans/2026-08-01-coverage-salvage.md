# Coverage Evidence Salvage (Milestone 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the interrupted `feat/coverage-evidence` branch to a green,
reviewed, mergeable state implementing workflow steps J→L (Brave search,
article fetch and extraction, `assess_article`) as specified by its own
authority, `docs/superpowers/specs/2026-07-30-coverage-evidence-design.md`.

**Design authority:** the 2026-07-30 coverage evidence design and its locked
decisions K1–K34. The branch's plan (Tasks 1–8) is recorded complete and
reviewed in `.superpowers/sdd/2026-07-30-coverage-evidence/progress.md`. What
remained when the session was interrupted was cross-suite integration fallout,
not milestone work.

**Status of the work:** This plan does **not** write the coverage milestone from
scratch. Roughly 8,200 lines of coverage source and 20+ test files already
exist on `feat/coverage-evidence`. Commit `aa30d87` preserved an interrupted
working tree that had taken the branch from 55 failing cross-suite tests down
to 2. This plan finishes that repair, removes the debug scaffolding, reconciles
one design decision, and gets the branch independently reviewed.

**Supersedes:** `docs/superpowers/plans/2026-08-01-coverage-discovery.md`. That
plan was written before the existing branch was discovered and assumes a
clean slate. Do not execute it. It is retained only as a record.

**Out of scope — deferred to milestone 5c:** step M, lead aggregation. The four
lead outcomes (`promising_lead`, `possible_lead`, `insufficient_evidence`,
`assessment_incomplete`), the lead assessment record, the person pointer to it,
and the `assess_person_lead` work item do not exist on the branch and are not
added here. Do not start them.

## Global Constraints

- Work happens on branch `feat/coverage-evidence` in the existing worktree at
  `.worktrees/feat-coverage-evidence`. Do not create a new branch or worktree.
- Baseline is commit `aa30d87`. Two tests fail there; everything else in
  `tests/foundation tests/run_engine tests/ingestion tests/people
  tests/wikipedia tests/coverage` passes (1,804 passed / 2 failed).
- Python 3.13+, `uv sync --frozen`. Do not add a dependency.
- Only `providers/` may import `httpx` or Trafilatura.
- Exactly one external call per `execute`, one persisted attempt per call.
  Workers never open SQLite.
- `TaskOutcome.reason` is a low-cardinality token — never a URL, query text, or
  provider string.
- No secret (`BRAVE_API_KEY`, `OPENROUTER_API_KEY`) may appear in a snapshot,
  fingerprint, log, digest, terminal output, or test assertion.
- Migrations are forward-only. `0007_coverage_evidence.sql` already exists and
  is already committed — do not edit it. Any schema change needs `0008`.
- Restore mutated source from a `cp` backup and verify with `diff`. Never
  `git stash` — the stash stack is shared across the six worktrees in this
  repository.
- Run mutations with `PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__`
  between iterations.
- Gate after every task: `uv run ruff check .`, `uv run ruff format .`,
  `uv run pyright`, plus the tests named in that task.

---

### Task 1: Remove debug scaffolding and restore a clean baseline

**Files:**
- Delete: `scratch.py`, `test_out.txt`
- Modify: `tests/people/test_run_cli.py`

**Context:** Commit `aa30d87` deliberately preserved diagnostic scaffolding left
by the interrupted session. `tests/people/test_run_cli.py` contains inline
`import sqlite3` blocks that dump `work_item` rows and then `assert False`,
inside `if cli_main.command_run(...) != cli_main.EXIT_OK:` guards, in at least
two tests. Those blocks query `state = "permanently_failed"`, which is not a
valid value — the enum in `runs/models.py:25` is `failed_permanent` — so the
dumps printed nothing and the previous session stalled.

**Rules:**

1. `scratch.py` and `test_out.txt` are gone from the working tree and the index.
2. No test in `tests/` contains an inline `import sqlite3` debug dump or a bare
   `assert False` used as a diagnostic.
3. Each affected test asserts on the run's exit code with a real assertion —
   `assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK` —
   rather than an `if`-guarded `assert False`.
4. No test asserts on the string `"permanently_failed"`; the valid value is
   `"failed_permanent"`.
5. The two failing tests still fail after this task, for their real reasons —
   this task removes scaffolding, it does not fix behaviour. Do not change
   production code here.

- [ ] **Step 1: Find every affected site**

```bash
grep -rn "permanently_failed\|assert False\|import sqlite3;" tests/
```

- [ ] **Step 2: Remove the scaffolding**

Replace each `if ... != EXIT_OK: <dump>; assert False` block with a direct
equality assertion. Delete the two stray files.

- [ ] **Step 3: Confirm the expected state**

Run: `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage -q`
Expected: exactly 2 failures, both in `tests/people/test_run_cli.py`
(`test_existing_backlog_is_seeded_without_new_feed_items` and
`test_second_run_reuses_completed_triage`). Report the exact counts.

- [ ] **Step 4: Gate and commit**

```bash
uv run ruff check . && uv run ruff format . && uv run pyright
git add -A
git commit -m "test(coverage): remove interrupted-session debug scaffolding"
```

---

### Task 2: Fix the `inspect_model` IntegrityError on a second run

**Files:**
- Modify: `src/notable_person_finder/people/service.py` (and/or
  `people/repository.py` — locate the real owner)
- Test: `tests/people/test_inspection_service.py`

**Context:** This is a real production defect, not a test-harness gap. On the
second `notable run` against an existing database, a required `inspect_model`
work item settles `failed_permanent` with reason `persist raised
IntegrityError`. Observed row:

```
{'task_type': 'inspect_model', 'subject_kind': 'model', 'subject_id': None,
 'state': 'failed_permanent', 'reason': 'persist raised IntegrityError',
 'created_by_run_id': 2}
```

The branch added a fourth configured task model (`assess_article`) to
`TasksConfig`. `ensure_model_inspections_for_run` re-arms inspection per run;
the persist path evidently violates a unique constraint when an inspection for
that model already exists from an earlier run.

**Investigate before fixing.** Determine whether this reproduces on
`refactor/rearchitecture` with four configured models, or is specific to how
the branch registers `assess_article`. Say which in your report — it changes
whether this is a coverage bug or a pre-existing latent one.

**Rules:**

1. A second run against an existing database settles every `inspect_model`
   item without an `IntegrityError`.
2. An inspection already recorded for a model in a prior run is reused or
   refreshed per the existing K21b re-arm rules, not inserted twice.
3. The fix does not weaken the uniqueness guarantee that caught this — do not
   solve it by dropping the constraint or by blanket `INSERT OR IGNORE`
   without establishing that the ignored row is genuinely identical.
4. A named regression test drives a two-run sequence with four configured task
   models and fails before the fix.

- [ ] **Step 1: Write the failing regression test**

In `tests/people/test_inspection_service.py`, a named test such as
`test_second_run_does_not_reinsert_an_existing_model_inspection`, exercising
two runs against one database with `assess_article` configured.

- [ ] **Step 2: Run it and confirm it fails with IntegrityError**

Record the exact error and the constraint named.

- [ ] **Step 3: Determine and report the root cause**

State the offending table, constraint, and code path in your report before
changing anything.

- [ ] **Step 4: Fix it**

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/people -q`
Expected: the regression test passes; only the Brave-stub failures remain.

- [ ] **Step 6: Mutation evidence for rules 1–4**

Revert the fix, confirm the named regression test fails, restore from the `cp`
backup, verify with `diff`.

- [ ] **Step 7: Gate and commit**

```bash
git commit -m "fix(people): reuse existing model inspection across runs"
```

---

### Task 3: Give cross-suite CLI tests a Brave stub

**Files:**
- Modify: `tests/people/test_run_cli.py`
- Possibly modify: `tests/ingestion/test_run_cli.py`,
  `tests/wikipedia/test_run_cli.py`, and the shared helpers those suites use
- Reference: `tests/coverage/fakes_brave.py` (the existing fake)

**Context:** `tests/people/test_run_cli.py` drives a full `notable run`. Now that
coverage discovery seeds `brave_web_search` work, those runs reach Brave with
no stub and settle `failed_permanent` with reason `configuration`, which makes
the run exit non-OK. The `tests/coverage` suite already has a working fake at
`tests/coverage/fakes_brave.py` — reuse it rather than writing a second one.

**Rules:**

1. `tests/people`, `tests/ingestion`, and `tests/wikipedia` CLI tests that drive
   a full run supply a Brave stub and complete with `EXIT_OK`.
2. The stub is the existing `tests/coverage/fakes_brave.py`, imported or
   promoted to a shared location — not a duplicate implementation.
3. No test reaches the real Brave endpoint. A test lacking the stub must fail
   loudly rather than silently making a network call.
4. Tests in those suites that deliberately assert on coverage *not* running
   keep asserting that, and are not papered over by the stub.
5. The full offline gate passes with zero failures.

- [ ] **Step 1: Inventory which run-driving tests now need the stub**

- [ ] **Step 2: Promote or import the existing fake**

If more than one suite needs it, move it to a shared test-support location and
update `tests/coverage` to import from there. Do not copy it.

- [ ] **Step 3: Wire it into the affected tests**

- [ ] **Step 4: Run the full gate**

```bash
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage -q
```
Expected: zero failures. Report the exact passed count.

- [ ] **Step 5: Positive control for rule 3**

Temporarily remove the stub from one test and confirm it fails rather than
attempting a live call. Restore and confirm with `diff`.

- [ ] **Step 6: Gate and commit**

```bash
git commit -m "test: stub Brave in cross-suite run CLI tests"
```

---

### Task 4: Make the source policy a tracked artifact

**Files:**
- Rename: `config/source_policies/visual_arts.example.toml` →
  `config/source_policies/visual_arts.toml`
- Modify: `config/notable.example.toml`, and any loader default or test that
  names the example path

**Context:** The branch ships the curated publisher policy as an operator-copied
`.example.toml`. The approved design
(`docs/superpowers/specs/2026-08-01-coverage-research-design.md`, "Publisher
Policy") requires it to be a tracked repository artifact reviewed in pull
requests, because the curated set is product content rather than an operator
preference. Everything else about the branch's implementation — `schema_version`,
`key`, ordered `[[rules]]`, `host_suffix` matching, first-match-wins,
`curated_eligible` / `curated_ineligible` / unclassified, the content
fingerprint in `coverage/screening.py` — already satisfies the design and must
not be changed.

**Rules:**

1. `config/source_policies/visual_arts.toml` is tracked in git.
2. No `.example` copy of it remains.
3. `config/notable.example.toml` points `source_policy_file` at the tracked
   path.
4. The policy content fingerprint is unchanged by the rename — it is computed
   over parsed content, not the filename. Assert this with a named test.
5. Loading a missing or malformed policy file still produces a path-qualified
   configuration error.

- [ ] **Step 1: Write the failing fingerprint-stability test**

A named test asserting the fingerprint of the tracked file equals the known
value from before the rename.

- [ ] **Step 2: Rename with `git mv` and update references**

```bash
git mv config/source_policies/visual_arts.example.toml config/source_policies/visual_arts.toml
grep -rn "visual_arts.example" . --exclude-dir=.git
```

- [ ] **Step 3: Run the gate**

Run: `uv run pytest tests/foundation tests/coverage -q`

- [ ] **Step 4: Mutation evidence for rules 4 and 5**

- [ ] **Step 5: Gate and commit**

```bash
git commit -m "feat(coverage): ship the source policy as a tracked artifact"
```

---

### Task 5: Reconcile the design document and CLAUDE.md with what shipped

**Files:**
- Modify: `docs/superpowers/specs/2026-08-01-coverage-research-design.md`
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/plans/2026-08-01-coverage-discovery.md` (mark
  superseded)

**Context:** The authoritative design for this milestone is
`docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` (2,005 lines,
locked decisions K1–K34) with plan
`docs/superpowers/plans/2026-07-30-coverage-evidence.md` (Tasks 1–8, all
recorded complete and reviewed in the branch's own ledger at
`.superpowers/sdd/2026-07-30-coverage-evidence/progress.md`).

`docs/superpowers/specs/2026-08-01-coverage-research-design.md` and
`docs/superpowers/plans/2026-08-01-coverage-discovery.md` were written on
2026-08-01 without knowledge of that branch. They are redundant, and in places
contradict locked decisions — they describe `eligible`/`ineligible` with
longest-path-prefix matching where K8/K33 shipped `curated_eligible` /
`curated_ineligible` with `host_suffix` first-match-wins, and they propose an
`assess_person_lead` work kind where K2 locks three kinds and K12 forbids
inventing lead outcomes in this milestone.

**Retire the 2026-08-01 documents rather than amending the 2026-07-30 ones to
match them.** The earlier design is the reviewed authority and the shipped code
implements it.

**Rules:**

1. `2026-08-01-coverage-research-design.md` carries a prominent header marking
   it superseded by `2026-07-30-coverage-evidence-design.md`, never
   implemented, and retained only as a record. Do not delete it.
2. Any genuinely new decision it contains that is *not* covered by K1–K34 — the
   tracked-artifact source policy from Task 4 — is folded into the 2026-07-30
   design as an explicit amendment note with today's date.
3. Nothing in the 2026-07-30 design is rewritten to match the 2026-08-01 one.
4. `CLAUDE.md` moves Brave web search, article fetch, and article extraction
   out of "Not built at all"; records coverage research through per-article
   assessment as delivered; and states plainly that lead aggregation, ranking,
   and the digest queue remain unbuilt.
6. `CLAUDE.md` adds `tests/coverage` to the verification section and to the
   combined gate command, and adds `coverage/` to the Rewrite Structure list.
7. The digest's "Coverage evidence" section is described accurately, including
   that the shortlist remains a placeholder.
8. `2026-08-01-coverage-discovery.md` carries a header line marking it
   superseded and never executed.
9. The branch's own ledger at
   `.superpowers/sdd/2026-07-30-coverage-evidence/progress.md` is left in
   place — it is the record of Tasks 1-8.

- [ ] **Step 1: Read the shipped code, not the spec, for each claim**

- [ ] **Step 2: Amend the spec**

- [ ] **Step 3: Amend CLAUDE.md**

- [ ] **Step 4: Mark both superseded 2026-08-01 documents**

- [ ] **Step 5: Run the full completion gate**

```bash
uv run ruff check .
uv run ruff format .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage
git diff --check
git status --short
```

All must pass; both git checks clean. Report the exact test count. Note in the
report that three live smokes (`tests/coverage -m live`) still require an
operator run with real keys before cutover.

- [ ] **Step 6: Commit**

```bash
git commit -m "docs: reconcile coverage design and agent guide with shipped code"
```

---

## After This Plan

The branch then needs an independent whole-branch review — it has never been
reviewed, and it carries ~22,800 lines. That review is the final step of the
subagent-driven-development loop, not a task here. Merge to
`refactor/rearchitecture` requires explicit user authorization.
