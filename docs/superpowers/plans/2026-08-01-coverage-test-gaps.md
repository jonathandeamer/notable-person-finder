# Coverage Test Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the six vacuous test files in `tests/coverage/` with tests that
actually discriminate, closing the milestone 5 cutover blocker.

**Why this exists:** the final whole-branch review of `feat/coverage-evidence`
found six files that assert nothing. Four are `live`-marked; two are not, so they
run green in every offline gate and look like passing coverage. As a result the
coverage CLI registration, the cross-component seams, the rendered digest
section, and the `notable status` lines have **zero** regression protection
today. Two Criticals already escaped into this milestone; these are the tests
that would have caught the class.

**Architecture:** no production code changes. Each task replaces one or two test
files, modelled on the reviewed equivalents in `tests/wikipedia/`, which are the
house style for exactly these four concerns.

**Tech Stack:** Python 3.13, uv, pytest, SQLite, existing fakes at
`tests/coverage/fakes_brave.py` and `tests/coverage/fakes_articles.py`.

**Branch:** `feat/coverage-evidence`, in the existing worktree at
`.worktrees/feat-coverage-evidence`. Baseline commit `f98be9a`, gate green at
1827 passed / 0 failed / 15 deselected.

## Global Constraints

- **No production code changes.** This plan writes tests. If a test cannot be
  written without changing `src/`, that means a real defect — stop and report it
  rather than changing production code or weakening the test.
- Expect to find defects. These surfaces have never been tested. A new test that
  fails against current behaviour is a finding, not a broken test: report it,
  and do not paper over it with a weakened assertion.
- Do not edit any migration.
- Do not add a dependency.
- Every test must discriminate. For each behaviour a task names, mutate the
  production rule, confirm a **specific named** test fails, restore from a `cp`
  backup, verify with `diff`. A test that survives deletion of the rule it is
  named for is not done.
- Every negative assertion needs a positive control. `assert X not in output`
  proves nothing unless some input makes `X` appear.
- Never use `git stash` — the stash stack is shared across six worktrees.
- Run mutations with `PYTHONDONTWRITEBYTECODE=1`, clearing `__pycache__` between
  iterations.
- No secret may appear in any assertion, fixture, log, or snapshot.
- No offline test may make a network call. Live tests carry
  `@pytest.mark.live` and must skip cleanly when their key is absent.
- Gate after every task: `uv run ruff check .`, `uv run ruff format .`,
  `uv run pyright`, and
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage -q`.

## The Six Files

| File | Current state | Task |
| --- | --- | --- |
| `tests/coverage/test_run_cli.py` | 2-line `pass`, **not** live-marked | 1 |
| `tests/coverage/test_seams.py` | 7-line `pass`, wrongly live-marked | 2 |
| `tests/coverage/test_digest_status.py` | 22 lines, zero-counts on an empty DB only | 3 |
| `tests/coverage/test_live_brave.py` | 7-line `pass` | 4 |
| `tests/coverage/test_live_article_fetch.py` | 7-line `pass` | 4 |
| `tests/coverage/test_live_openrouter_assess.py` | 7-line `pass` | 4 |

---

### Task 1: `test_run_cli.py` — CLI registration and run integration

**Files:**
- Rewrite: `tests/coverage/test_run_cli.py`
- Template: `tests/wikipedia/test_run_cli.py` (272 lines)

**Context:** the file is currently `def test_run_cli_coverage_registered(): pass`
with no `live` marker, so it has been reporting green in every gate. The
wikipedia equivalent is the model: it drives `cli_main.command_run` end to end
against stubbed providers on a temporary data root and asserts on registration,
seeding, and durable outcomes.

Reuse the existing stub patterns. `tests/people/test_run_cli.py` shows how the
`StaticHostResolver` must carry `api.search.brave.com` — a missing entry makes
the transport reject the request as `CONFIGURATION` before the mock is reached,
which is the exact trap that wedged the earlier session.

**Rules:**

1. `command_run` registers all three coverage work kinds on the engine handler
   map: `brave_web_search`, `fetch_article`, `assess_article` (K2).
2. Seeding runs `seed_coverage_research` in the correct order relative to the
   Wikipedia seed and the model-inspection ensure.
3. A run with a person whose current Wikipedia observation is
   `no_matching_page_found` opens exactly one coverage plan.
4. A run with a person whose current observation is `matching_page_found` opens
   no plan and supersedes existing coverage work (K5).
5. A second run over unchanged material opens no second plan and issues no
   second paid Brave call.
6. `command_run` returns `EXIT_OK` and writes a digest containing the
   `### Coverage evidence` section.
7. No secret reaches the digest, the snapshot, or captured output — with a
   positive control proving the sentinel key is reachable in the request path.

- [ ] **Step 1: Read the template and the existing stubs**

Read `tests/wikipedia/test_run_cli.py` in full, and the stub construction in
`tests/people/test_run_cli.py`. Do not invent a new harness.

- [ ] **Step 2: Write the failing tests**

One named test per rule.

- [ ] **Step 3: Run them and record what fails and why**

Run: `uv run pytest tests/coverage/test_run_cli.py -v`
Some may fail against real defects rather than missing test code. Record which,
and report them — do not adjust the assertion to match current behaviour without
saying so explicitly.

- [ ] **Step 4: Resolve to green or to a reported finding**

- [ ] **Step 5: Mutation evidence for rules 1–7**

Rule 4's "no plan" and rule 5's "no second call" both need positive controls.

- [ ] **Step 6: Gate and commit**

```bash
git commit -m "test(coverage): real CLI registration and run integration tests"
```

---

### Task 2: `test_seams.py` — cross-component seams, offline

**Files:**
- Rewrite: `tests/coverage/test_seams.py`
- Template: `tests/wikipedia/test_seams.py` (143 lines)

**Context:** the file is a 7-line `pass` carrying `pytestmark =
pytest.mark.live`. That marker is wrong twice over: a seam test needs no network,
and the marker is why `pytest tests/coverage -m live` collects four tests instead
of three. **Remove the `live` marker.** These must run in the default offline
gate.

**Rules:**

1. The Wikipedia settle seam schedules coverage research when an identity
   observation completes as `no_matching_page_found` or `uncertain_identity`.
2. The same seam schedules nothing for `matching_page_found`.
3. The person-merge seam reconciles coverage state: the survivor's stale plan is
   superseded before a new one opens, and no `person_article` that pending assess
   work names is deleted (the I3/I4 fixes from the final review — these are
   regression tests for defects already found, so they must fail if those fixes
   are reverted).
4. `_compose_seed` includes coverage seeding, and the seam runs after the
   Wikipedia seed.
5. Coverage never imports from `people/` or `wikipedia/` in a direction that
   would invert the package layering (K1). Assert the seam is injected, not
   reached by a reverse import.

- [ ] **Step 1: Remove the `live` marker and read the template**

- [ ] **Step 2: Write the failing tests, one named test per rule**

- [ ] **Step 3: Run and record**

Run: `uv run pytest tests/coverage/test_seams.py -v`
Then confirm the collection count changed:
`uv run pytest tests/coverage -m live --collect-only -q` must now collect
**three**, not four.

- [ ] **Step 4: Resolve to green or to a reported finding**

- [ ] **Step 5: Mutation evidence for rules 1–5**

Rule 3 is a regression test for a fix that already landed — verify it fails when
that fix is reverted. Rule 2 needs a positive control.

- [ ] **Step 6: Gate and commit**

```bash
git commit -m "test(coverage): real cross-component seam tests, offline"
```

---

### Task 3: `test_digest_status.py` — rendered output against a populated database

**Files:**
- Rewrite: `tests/coverage/test_digest_status.py`
- Template: `tests/wikipedia/test_digest_status.py` (594 lines)

**Context:** the current file asserts nine zero-counts against a freshly migrated
empty database and never renders anything. The final review confirmed it passes
with the entire coverage summary deleted. The digest section is nine lines
(`reporting/digest.py:346-362`) and `notable status` prints three coverage lines
(`cli/main.py:932-947`); none of that text is covered.

**Rules:**

1. Against a **populated** database, every one of the nine digest counters
   renders its correct value. Build fixtures with real plans, targets, and
   assessments — not zeroes.
2. The `### Coverage evidence` heading and all nine labels render with their
   exact wording.
3. `notable status` prints its three coverage lines with correct values.
4. `people_with_completed_assessment` excludes merged-away people (the I7 fix —
   a regression test that must fail if that fix is reverted).
5. Plan counters credit the **settling** run, not the opening run (the I5 fix —
   same requirement; build a plan that opens in run N and terminalizes in N+1,
   and assert it appears in run N+1's digest).
6. The digest omits the coverage section entirely when `coverage is None`, with a
   positive control proving the section appears when it is supplied.
7. No query text, result URL, or article title appears in the digest coverage
   section — with a positive control proving such a string exists in the fixture
   database.

- [ ] **Step 1: Read the template, especially its fixture builders**

`tests/wikipedia/test_digest_status.py` at 594 lines is mostly fixture
construction. Follow it; do not hand-roll a thinner harness.

- [ ] **Step 2: Write the failing tests, one named test per rule**

- [ ] **Step 3: Run and record**

- [ ] **Step 4: Resolve to green or to a reported finding**

Rules 4 and 5 are regression tests for landed fixes. If either passes without
the fix, the fix is not doing what the review believed — report that.

- [ ] **Step 5: Mutation evidence for rules 1–7**

Deleting the coverage summary from the digest must now fail a named test. That
is the specific hole this task closes.

- [ ] **Step 6: Gate and commit**

```bash
git commit -m "test(coverage): digest and status tests against a populated database"
```

---

### Task 4: The three live smokes

**Files:**
- Rewrite: `tests/coverage/test_live_brave.py`
- Rewrite: `tests/coverage/test_live_article_fetch.py`
- Rewrite: `tests/coverage/test_live_openrouter_assess.py`
- Template: `tests/wikipedia/test_live_mediawiki.py` (189 lines) and
  `tests/wikipedia/test_live_openrouter_match.py`

**Context:** all three are 7-line `pass` bodies. A green `-m live` run currently
proves nothing. These are the only tests that can catch a provider contract
break — including finding I9, where Brave's `extra_snippets` parameter is
rejected on free and base tiers and no offline test can see it.

**You cannot verify these against a live provider.** There is no API key in this
environment. Write them so that they are correct by construction and skip
cleanly, and say plainly in your report that they remain unexecuted.

**Rules:**

1. Each test carries `@pytest.mark.live` and skips cleanly when its key is
   absent — `BRAVE_API_KEY` for Brave, `OPENROUTER_API_KEY` for assess. The
   article fetch smoke needs no key.
2. The Brave smoke performs one real exact-name search and asserts the response
   parses into an ordered `SearchPage` with at least one result carrying a URL,
   a title, and a rank. It must not assert on specific result content, which
   would make it flaky.
3. The Brave smoke exercises the `extra_snippets` path explicitly enough that a
   tier rejection surfaces as a clear failure rather than a silent skip
   (finding I9).
4. The article fetch smoke fetches one stable public article, asserts extraction
   produces ordered blocks and a title, and asserts no raw HTML survives into the
   returned value.
5. The assess smoke performs one real `assess_article` generation against the
   **shipped default** `AssessArticleConfig` — not an inflated
   `max_input_tokens` — and asserts the response validates against the schema.
   This is the live counterpart to Critical 1.
6. Each smoke records model, provider, usage, cost, and outcome for the operator,
   and no secret appears in that output.
7. No live test runs in the default offline gate.

- [ ] **Step 1: Read both wikipedia live templates**

- [ ] **Step 2: Write the three smokes**

- [ ] **Step 3: Confirm they skip cleanly with no key present**

Run: `uv run pytest tests/coverage -m live -v`
Expected: three tests, all skipped, no network attempted, no error.

- [ ] **Step 4: Confirm they stay deselected in the offline gate**

Run: `uv run pytest tests/coverage -q` and confirm the deselected count.

- [ ] **Step 5: Static verification in place of execution**

You cannot run these for real. Instead, verify by inspection against the
adapter source that every field each smoke asserts on actually exists with that
name and type, and say so field by field in your report. A live test that fails
on an `AttributeError` the first time an operator runs it is worse than no test.

- [ ] **Step 6: Gate and commit**

```bash
git commit -m "test(coverage): real live smokes for Brave, article fetch, and assess"
```

---

### Task 5: Update the agent guide

**Files:**
- Modify: `CLAUDE.md`

**Context:** CLAUDE.md currently states bluntly that six coverage test files are
stubs and that the corresponding surfaces have no regression protection. Once
Tasks 1–4 land, that text is wrong in the opposite direction.

**Rules:**

1. The stub warnings are removed or rewritten to match reality.
2. `pytest tests/coverage -m live` is described as collecting **three** tests,
   all of them real, and still requiring an operator run with real keys before
   cutover.
3. The known-gaps section states accurately what remains unverified: the three
   live smokes have been written but never executed against a real provider.
4. Every claim added is verified against the test files as they now stand, not
   against this plan's intent.
5. No claim of coverage is made for a surface that is still untested.

- [ ] **Step 1: Re-read the six files as they now stand**

- [ ] **Step 2: Rewrite the affected CLAUDE.md passages**

- [ ] **Step 3: Run the full completion gate**

```bash
uv run ruff check .
uv run ruff format .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage
uv run pytest tests/coverage -m live --collect-only -q
git diff --check
git status --short
```

- [ ] **Step 4: Commit**

```bash
git commit -m "docs: record real coverage test protection in the agent guide"
```

---

## Out of Scope

- Any production code change. A defect found is reported, not fixed here.
- The deferred minors already recorded in
  `.superpowers/sdd/2026-08-01-coverage-salvage/progress.md`.
- Lead aggregation (milestone 5c).
- Pushing, opening a pull request, or merging.
