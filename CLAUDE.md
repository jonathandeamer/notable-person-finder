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

- The application never edits Wikipedia automatically. Every Wikipedia edit or
  publication action requires explicit human review and approval.
- Distribution: `notable-person-finder`; import package:
  `notable_person_finder`; executable: `notable`.
- Configuration is strict and file-first. Environment variables supply secrets
  only, and secrets must never appear in snapshots, fingerprints, diagnostics,
  terminal output, or tests. An adjacent `.env` may fill missing secret values,
  but the process environment takes precedence.
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
