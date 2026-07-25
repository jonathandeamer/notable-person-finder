# Rewrite Agent Guidance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make repository agent instructions rewrite-first, establish `CLAUDE.md` as the canonical guide, and expose the same guide through an `AGENTS.md` symlink.

**Architecture:** Replace the legacy-oriented `CLAUDE.md` with a concise routing and safety guide for the clean-slate package and redesign programme. Keep one source of instruction truth by pointing `AGENTS.md` to `CLAUDE.md`, and make reviewed pull requests into `refactor/rearchitecture` the default rewrite integration path.

**Tech Stack:** Markdown, Git symbolic links, shell validation.

## Global Constraints

- `CLAUDE.md` is the canonical instruction file.
- `AGENTS.md` is a relative symbolic link whose target is exactly `CLAUDE.md`.
- Rewrite pull requests target `refactor/rearchitecture` by default.
- A separate agent independently reviews each rewrite pull request before merge.
- Pushing, opening a pull request, merging, and targeting `main` require explicit user authorization.
- `main` receives the rewrite only through the later explicit cutover decision.
- Prototype code and tests are historical evidence, remain untouched unless explicitly requested, and are not rewrite verification gates.
- Active guidance must not present `run_pipeline.py`, legacy scripts, JSONL state, unittest, or the prototype gate sequence as rewrite conventions.

---

## File Structure

```text
CLAUDE.md  # canonical, rewrite-first repository instructions
AGENTS.md  # relative symlink to CLAUDE.md
```

## Task 1: Replace and Alias Repository Agent Guidance

**Files:**
- Modify: `CLAUDE.md`
- Create: `AGENTS.md` as a symbolic link to `CLAUDE.md`

**Interfaces:**
- Produces: identical repository instructions when an agent reads either `CLAUDE.md` or `AGENTS.md`.
- Produces: a reviewed-PR integration policy for rewrite feature branches.

- [ ] **Step 1: Replace `CLAUDE.md` with rewrite-first guidance**

Use this content:

```markdown
# Repository Agent Guide

## Current Development Direction

This repository is being rebuilt as a clean-slate, installable Python
application. Current development belongs in the `src/notable_person_finder`
package and follows the redesign programme. Do not infer rewrite architecture
or conventions from the retained prototype.

The architectural authorities are:

- `docs/superpowers/specs/2026-07-24-redesign-program-design.md` for programme
  scope, sequencing, replacement boundaries, and cutover policy.
- The applicable file in `docs/superpowers/plans/` for the active milestone's
  exact interfaces, constraints, tests, and completion gate.
- `docs/superpowers/specs/2026-07-25-rewrite-agent-guidance-design.md` for the
  repository's agent publishing and review workflow.

Read the applicable design and plan before implementing. If a progress ledger
exists for an active plan, use it as the recovery authority and do not repeat
tasks already recorded complete.

## Rewrite Structure

- `src/notable_person_finder/` — installable application package.
- `tests/foundation/` — application-foundation tests. Later milestones add
  their own rewrite test areas as specified by their plans.
- `config/*.example.toml` — tracked, copyable configuration examples; local
  configuration variants remain untracked.
- `docs/superpowers/specs/` — approved architecture and policy.
- `docs/superpowers/plans/` — executable milestone plans and completion gates.
- `pyproject.toml` and `uv.lock` — package metadata and frozen dependency graph.

Follow the file structure and interfaces in the active milestone plan. Do not
import or wrap prototype modules to shortcut rewrite work.

## Environment and Verification

- Python version: 3.13 or newer.
- Install and synchronize dependencies with `uv sync --frozen`.
- Run the focused rewrite tests and completion commands named by the active
  milestone plan.
- For the completed application foundation, use
  `uv run pytest tests/foundation`.
- Exercise the installed interface with `uv run notable ...`.
- Keep default rewrite verification offline and isolate configuration and
  storage with temporary paths.
- Do not use bare pytest or the prototype suite as evidence that a rewrite
  milestone passes.

Before reporting completion, run the active plan's full completion gate and
confirm `git diff --check` and `git status --short` are clean as applicable.

## Foundation Invariants

- Distribution: `notable-person-finder`; import package:
  `notable_person_finder`; executable: `notable`.
- Configuration is strict and file-first. Environment variables supply secrets
  only, and secrets must never appear in snapshots, fingerprints, diagnostics,
  terminal output, or tests.
- SQLite migrations are forward-only, checksummed, transactional, and backed
  up before changing an existing database.
- Mutating commands use the nonblocking, OS-managed lock scoped to one data
  root. File contents are diagnostic and never determine lock ownership.
- Preserve these reviewed contracts unless a later approved design explicitly
  replaces them.

## Branch, Pull Request, and Review Workflow

`refactor/rearchitecture` is the rewrite integration branch. Implement changes
on a focused feature branch, preferably in an isolated worktree.

For rewrite work:

1. Implement and verify the active milestone on the feature branch.
2. Obtain explicit user authorization before publishing external changes.
3. Push the feature branch and open a pull request targeting
   `refactor/rearchitecture`.
4. Have a separate agent independently review the pull request.
5. Fix all Critical and Important findings and obtain scoped re-review of the
   fixes.
6. Merge only after review approval and explicit user authorization.

Do not directly merge a feature branch into `refactor/rearchitecture` as the
normal completion path. Do not push, open or modify pull requests, merge, or
target `main` without explicit user authorization. Only the complete, verified
rewrite integration branch may merge into `main`, and only as a later explicit
cutover decision.

## Legacy Prototype

The root-level pipeline, `run_pipeline.py`, `scripts/`, JSONL state, and their
tests are the preserved legacy prototype. They are historical evidence and the
operational fallback until product cutover. Do not import, reorganize, remove,
run, or repair them unless the user explicitly requests legacy work. Prototype
tests are not a rewrite gate.
```

- [ ] **Step 2: Create the relative symlink**

Run:

```bash
ln -s CLAUDE.md AGENTS.md
```

Expected: `AGENTS.md` is a symlink, not a duplicate regular file.

- [ ] **Step 3: Validate the instruction alias and content**

Run:

```bash
test -L AGENTS.md
test "$(readlink AGENTS.md)" = "CLAUDE.md"
cmp -s AGENTS.md CLAUDE.md
git diff --check
git status --short
```

Expected:

- all validation commands exit `0`;
- `git status --short` lists only the intended `CLAUDE.md`, `AGENTS.md`, and
  plan-related changes;
- reading either instruction filename yields identical content.

- [ ] **Step 4: Review the guide against the approved design**

Confirm all of these explicitly:

- the active guide is rewrite-first;
- the architecture and command references point to the rewrite;
- pull requests target `refactor/rearchitecture` by default;
- independent agent review and explicit merge authorization are required;
- `main` remains behind the explicit cutover boundary;
- the only legacy material is the short historical-prototype section;
- no active instruction assumes the prototype's gate structure, scripts,
  JSONL state, unittest workflow, or dependencies apply to the rewrite.

- [ ] **Step 5: Commit the repository guidance**

```bash
git add CLAUDE.md AGENTS.md
git commit -m "docs: align agent guidance with rewrite"
```
