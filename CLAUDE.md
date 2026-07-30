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

Six milestones are complete: the application foundation, the run engine and
shared transport, feed ingestion, the OpenRouter model gateway with person
detection (3b1), durable person identity with first-pass resolution,
reconsideration, and confirmed merges (3b2), and Wikipedia identity matching
with MediaWiki retrieval and semantic match (milestone 4). A cold-starting
agent should assume nothing beyond this list.

Delivered and usable:

- `notable config validate`, `notable paths`, `notable db migrate`.
- `notable run` — validates configuration, migrates, takes the mutation lock,
  sweeps interrupted predecessor runs, executes eligible work through the
  scheduler, ingests enabled RSS and Atom feeds, inspects configured detection
  and resolve models, detects people in untriaged source items, resolves
  eligible mentions into durable people (empty-candidate create, model path,
  `possible_same_person`, reconsideration, confirmed merges), writes a dated
  digest plus `latest.md`, and prints the same Markdown on standard output.
- The shared HTTP transport with URL, DNS-preflight, redirect, timeout,
  response-size, concurrency, and pacing bounds; the retry coordinator; the
  per-run budget reservation; and redacting structured logging.
- The feedparser-backed feed adapter, conditional feed fetching, durable feed
  identity and fetch history, canonical article and URL-alias identity, and
  insert-once source items.
- The OpenRouter provider adapter (SDK-backed inspect and structured
  generation with SDK retries disabled so only the central coordinator may
  repeat a call), exact-model preflight inspection, dynamic per-generation
  budget reservation under an optional hard USD cap, person-detection work
  items, and `resolve_person_entity` / `reconsider_person_entity` work items.
- Triage observations and person mentions on source items. Mentions retain
  exact names, mononyms, and professional names. Eligible research/uncertain
  mentions resolve to durable people, sourced names, entity-resolution
  observations, optional `possible_same_person` edges, and confirmed merges
  with lower-id survivors and canonical work reconciliation.
- Separate bounded HTTP and LLM worker pools that may overlap; workers never
  access SQLite.
- MediaWiki adapter (`search_pages`, `get_page_facts`) with shared transport,
  pacing, maxlag, and no auth secret; durable pages, query plans, search
  observations, page-fact batches, and Wikipedia identity observations with
  outcomes `matching_page_found`, `no_matching_page_found`, and
  `uncertain_identity`; person current Wikipedia pointer; work kinds
  `mediawiki_search`, `mediawiki_page_facts`, and `match_wikipedia_identity`;
  deterministic empty complete search no-match; refresh, merge reconcile, and
  Wikipedia counters on digest and `notable status`.

Delivered only in part — do not describe these as finished:

- `notable status` reports the latest run, its digest, required pending and
  deferred counts, operational failures, corpus source-item and article totals,
  each feed's latest successful fetch, durable triage counters (triaged,
  untriaged, research, uncertain, do not research, insufficient input, failed
  triage, and unresolved research or uncertain mentions), and durable identity
  counters (canonical people, merged-away people, unresolved eligible
  mentions under K24, active `possible_same_person`, and mentions linked to
  people). It has no digest backlog, no oldest pending candidate, no queue
  tiers, and still no budget or deferral-reason breakdown; those need later
  milestones.
- The digest emits its header, banner, operational summary, per-run budget
  line, deferral-reason breakdown, ingestion summary, person-detection summary
  (triage outcomes, unresolved mention counts, model deferred/failed,
  OpenRouter cost), and person-identity summary (people created, mentions
  resolved, outcome split including created_new vs different_people, K24
  eligible remaining, active possible_same_person, confirmed merges, resolve
  model deferred/failed). Its shortlist section is a placeholder: there is no
  ranking and no model synthesis yet.

Not built at all, so do not document, import, or assume any of it:

- Brave web search, article fetch and extraction adapters.
- Coverage research, assessments, ranking, synthesis, drafting, or the digest
  queue.
- `notable digest show`, `notable audit run`, `notable audit person`.

Known gaps carried forward, recorded so a later change does not mistake them
for regressions:

- `notable status` prints bare pending and deferred counts, with no budget
  figures and no deferral-reason breakdown, so it still cannot explain *why*
  work was deferred. The digest now can; `status` has not caught up.
- If a handler returns a non-settling state after making an external call, the
  engine settles that item `failed_permanent` and finishes the run, but
  deliberately discards the untrusted payload. The attempt row is then the
  only durable call evidence; a handler-owned domain row such as `feed_fetch`
  is not written.
- Two crash windows can repeat a paid provider call; see
  `docs/architecture/at-least-once-execution.md`. Do not restate those windows
  elsewhere.
- OpenRouter live smoke (`uv run pytest tests/people -m live -v`) requires a
  real `OPENROUTER_API_KEY`. Offline gates deselect it. An operator must still
  run the live smoke and record model/provider/usage/cost/outcome (without
  secrets) before milestone cutover when a key is available.

## Rewrite Structure

- `src/notable_person_finder/` — installable application package.
  - `cli/` — argument parsing and the `notable` commands.
  - `config/` — strict configuration models, loading, and path resolution.
  - `db/` — SQLite connections and forward-only checked migrations.
  - `runs/` — run engine, clock, repository, work-item scheduling, retry
    coordination, budget reservation, and the mutation lock.
  - `providers/` — the shared HTTP transport, request safety checks, pacing,
    provider failure classification, the feedparser-backed feed adapter, the MediaWiki client, and the
    OpenRouter client. This is the only package that may import `httpx` or
    the OpenRouter SDK.
  - `ingestion/` — feed seeding and handling, URL identity, domain models, and
    transaction-neutral persistence helpers for ingestion settlements.
  - `people/` — detection and identity: triage, first-pass resolution,
    reconsideration, confirmed merges, candidate retrieval, prompts, and
    domain validation.
  - `wikipedia/` — MediaWiki query plans, candidate assembly, identity
    observations, match handler, seed/merge hooks.
  - `obs/` — redacting structured logging.
  - `reporting/` — the daily digest writer.
- `tests/foundation/` — application-foundation tests.
- `tests/run_engine/` — run engine and shared transport tests.
- `tests/ingestion/` — feed adapter, domain persistence, CLI integration, seam,
  and opt-in live-smoke tests.
- `tests/people/` — OpenRouter adapter, detection service, repository, CLI
  integration, cross-component seams, and opt-in OpenRouter live-smoke tests.
- `tests/wikipedia/` — MediaWiki adapter, schema, queries/candidates, match
  contract, HTTP and match handlers, seed/merge hooks, digest/status, CLI
  seams, and opt-in MediaWiki/OpenRouter live-smoke tests.
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
- For completed feed ingestion, use `uv run pytest tests/ingestion`.
- For completed model gateway, detection, and durable person identity, use
  `uv run pytest tests/people`.
- For completed Wikipedia identity matching, use
  `uv run pytest tests/wikipedia`.
- The six completed milestones together gate with
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia`.
  Run it from a real checkout: it needs the tracked `config/` directory.
  Default pytest `addopts` deselect `live`. Opt-in live smokes:
  - feeds: `uv run pytest tests/ingestion -m live -v`
  - OpenRouter (detect + resolve):
    `OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v`
  - MediaWiki + match OpenRouter:
    `uv run pytest tests/wikipedia -m live -v` (OpenRouter key for match smoke)
- Exercise the installed interface with `uv run notable ...`.
- Keep default rewrite verification offline and isolate configuration and
  storage with temporary paths.
- Do not use bare pytest or the prototype suite as evidence that a rewrite
  milestone passes.

Before reporting completion, run the active plan's full completion gate and
confirm `git diff --check` and `git status --short` are clean as applicable.

## Test Evidence

Tests-first ordering is not evidence that a test discriminates. A test written
before its module fails with `ImportError`; that proves the test runs, not that
it detects the rule it is named for. Milestone 3a lost four rules to exactly
this gap — each test went red for the trivial reason, green once the code
arrived, and stayed green when the rule it was named for was deleted.

Before reporting a task complete:

- For each behaviour the plan names, mutate that rule in the source, confirm a
  **specific named** test fails, then restore. Report which mutation killed
  which test. A rule that survives its own removal is untested.
- A negative assertion needs a positive control. `assert X not in output`
  proves nothing unless some input makes `X` appear; otherwise it passes
  because `X` was never reachable, not because the code excluded it.
- If source was written before its tests — after an interruption, or because a
  task was recovered — every named rule needs this evidence, not a sample.
  That ordering is how the four escapes above were introduced.

Restore mutated source from a `cp` backup and verify with `diff`, never with
`git stash`: the stash stack is shared across worktrees and other sessions.

Run mutations with `PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__` between
iterations. Rewriting one file repeatedly inside the same second produces cache
entries that CPython's mtime-and-size invalidation accepts, so an iteration can
silently test the *previous* mutation. This has already produced two false
`SURVIVED` verdicts in this repository — the direction that matters, because a
survivor reported as killed is a rule you believe is covered and is not.

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
  request. No transaction is held across its own item's network call: sibling
  calls may still be in flight while the application thread settles another
  item's short, local SQLite transaction.
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
