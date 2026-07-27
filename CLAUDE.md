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

## What Is Actually Built

Two milestones are complete: the application foundation, and the run engine and
shared transport. A cold-starting agent should assume nothing beyond this list.

Delivered and usable:

- `notable config validate`, `notable paths`, `notable db migrate`.
- `notable run` — validates configuration, migrates, takes the mutation lock,
  sweeps interrupted predecessor runs, executes eligible work through the
  scheduler, writes a dated digest plus `latest.md`, and prints the same
  Markdown on standard output.
- The shared HTTP transport with URL, DNS-preflight, redirect, timeout,
  response-size, concurrency, and pacing bounds; the retry coordinator; the
  per-run budget reservation; and redacting structured logging.

Delivered only in part — do not describe these as finished:

- `notable status` reports the latest run, its digest, required pending and
  deferred counts, and operational failures. It has no digest backlog, no
  oldest pending candidate, and no queue tiers; those need the digest queue
  from the lead-assessment milestone.
- The digest emits its header, banner, and operational summary. Its shortlist
  section is a placeholder: there is no ranking and no model synthesis yet.

Not built at all, so do not document, import, or assume any of it:

- Any concrete provider adapter (feeds, MediaWiki, web search, article fetch
  and extraction, LLM). `providers/` is transport plumbing only.
- Any domain table — source items, people, Wikipedia observations, articles,
  assessments, digest queue.
- `notable digest show`, `notable audit run`, `notable audit person`.

Known gaps carried forward, recorded so a later change does not mistake them
for regressions:

- `RunReport` carries no budget fields, so neither the digest nor `status` can
  currently explain *why* work was deferred.
- A handler that returns a non-settling work state escapes as an uncaught
  `ValueError` with a traceback rather than a handled run failure.
- Two crash windows can repeat a paid provider call; see
  `docs/architecture/at-least-once-execution.md`.

## Rewrite Structure

- `src/notable_person_finder/` — installable application package.
  - `cli/` — argument parsing and the `notable` commands.
  - `config/` — strict configuration models, loading, and path resolution.
  - `db/` — SQLite connections and forward-only checked migrations.
  - `runs/` — run engine, clock, repository, work-item scheduling, retry
    coordination, budget reservation, and the mutation lock.
  - `providers/` — the shared HTTP transport, request safety checks, pacing,
    and provider failure classification. This is the only package that may
    import `httpx`. It contains no concrete provider adapter yet.
  - `obs/` — redacting structured logging.
  - `reporting/` — the daily digest writer.
- `tests/foundation/` — application-foundation tests.
- `tests/run_engine/` — run engine and shared transport tests. Later milestones
  add their own rewrite test areas as specified by their plans.
- `docs/architecture/at-least-once-execution.md` — the operator-facing note on
  the crash windows in which a paid provider call can be repeated. Point at it
  rather than restating it.
- `config/*.example.toml` — tracked, copyable configuration examples; local
  configuration variants remain untracked.
- `docs/superpowers/specs/` — approved architecture and policy.
- `docs/superpowers/plans/` — executable milestone plans and completion gates.
- `pyproject.toml` and `uv.lock` — package metadata and frozen dependency graph.

Follow the file structure and interfaces in the active milestone plan. Do not
import or wrap prototype modules to shortcut rewrite work.

## Plans and Specifications

Plans and specs are authorities, not source-code repositories. Keep them
reviewable and executable.

- **Keep milestone plans under ~1,500 lines.** If a plan grows larger, the
  milestone is too big: split it, or move reference material into an appendix
  or separate design note.
- **Reference code blocks are illustrative, not canonical.** Do not paste
  large implementation fragments into a plan. The shipped code and its tests
  govern; the plan describes interfaces, invariants, the completion gate, and
  sequencing.
- **A plan specifies *what* and *why*, not *how* line-by-line.** Include:
  public interfaces, constraints, failure modes, test strategy, and the exact
  verification command that closes the milestone.
- **Track progress in a ledger, not in the plan file.** Use a separate
  progress ledger or task list for checkbox tracking; do not leave the plan
  itself full of unchecked boxes once implementation is complete.
- **Specs are for architecture and policy; plans are for executable
  milestones.** A spec may be long-lived and revised; a plan should be small
  enough to read in one sitting before starting work.

## Environment and Verification

- Python version: 3.13 or newer.
- Install and synchronize dependencies with `uv sync --frozen`.
- Configure repository git hooks: `git config core.hooksPath .githooks`.
- Code quality and static analysis:
  - `uv run ruff check .`
  - `uv run ruff format .`
  - `uv run pyright`
- Run the focused rewrite tests and completion commands named by the active
  milestone plan.
- For the completed application foundation, use
  `uv run pytest tests/foundation`.
- For the completed run engine and shared transport, use
  `uv run pytest tests/run_engine`.
- Both completed milestones together gate with
  `uv run pytest tests/foundation tests/run_engine`. Run it from a real
  checkout: it needs the tracked `config/` directory.
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
- Every external network call maps to exactly one persisted attempt attributed
  to its run and work item. Only the central retry coordinator starts a repeat
  request, and no transaction is held across a network call.
- External execution is at-least-once. See
  `docs/architecture/at-least-once-execution.md`.
- Preserve these reviewed contracts unless a later approved design explicitly
  replaces them.

## Branch, Pull Request, and Review Workflow

`refactor/rearchitecture` is the rewrite integration branch. Implement changes
on a focused feature branch, preferably in an isolated worktree.

### New rewrite work

New milestone, feature, or design work follows the full path:

1. Implement and verify the active milestone on the feature branch.
2. Obtain explicit user authorization before publishing external changes.
3. Push the feature branch and open a pull request targeting
   `refactor/rearchitecture`.
4. Have a separate agent independently review the pull request.
5. Fix all Critical and Important findings and obtain scoped re-review of the
   fixes.
6. Merge only after review approval and explicit user authorization.

Do not directly merge a feature branch into `refactor/rearchitecture` as the
normal completion path.

### Remediation of review findings

Remediation is not new work. When a review produces findings against work that
is already on the integration branch, the fixes do not need their own pull
request or their own independent review — the review that produced them already
supplied the independent judgement. Commit them on a focused branch and merge
into `refactor/rearchitecture` with explicit user authorization.

Treat work as remediation only when all of the following hold:

- the findings come from a completed review of work already integrated;
- every change traces to a specific finding, and no change adds feature
  surface, a new interface, or a new dependency;
- each behavioural fix carries a regression test that fails before the fix and
  passes after it;
- the active milestone's full verification gate passes.

The classification is not the implementing agent's to stretch. If the work grows
past the findings, it is no longer remediation: stop, open a pull request, and
follow the full path. When in doubt, use the full path.

### Always

Do not push, open or modify pull requests, merge, or target `main` without
explicit user authorization, on either path. Only the complete, verified rewrite
integration branch may merge into `main`, and only as a later explicit cutover
decision.

## Legacy Prototype

The root-level pipeline, `run_pipeline.py`, `scripts/`, JSONL state, and their
tests are the preserved legacy prototype. They are historical evidence and the
operational fallback until product cutover. Do not import, reorganize, remove,
run, or repair them unless the user explicitly requests legacy work. Prototype
tests are not a rewrite gate.
