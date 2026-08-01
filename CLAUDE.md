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

Seven milestones are complete: the application foundation, the run engine and
shared transport, feed ingestion, the OpenRouter model gateway with person
detection (3b1), durable person identity with first-pass resolution,
reconsideration, and confirmed merges (3b2), Wikipedia identity matching
with MediaWiki retrieval and semantic match (milestone 4), and coverage
evidence — bounded Brave Web Search, article fetch and extraction, and
per-article assessment (milestone 5). A cold-starting agent should assume
nothing beyond this list.

Milestone 5's authorities are
`docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` (locked
decisions K1–K34, plus a 2026-08-01 amendment) and
`docs/superpowers/plans/2026-07-30-coverage-evidence.md`. The 2026-08-01
`coverage-research-design` spec and `coverage-discovery` plan are superseded
and were never implemented; they contradict the locked decisions and must not
be built from.

Delivered and usable:

- `notable config validate`, `notable paths`, `notable db migrate`.
- `notable run` — validates configuration, migrates, takes the mutation lock,
  sweeps interrupted predecessor runs, executes eligible work through the
  scheduler, ingests enabled RSS and Atom feeds, inspects configured detection
  and resolve models, detects people in untriaged source items, resolves
  eligible mentions into durable people (empty-candidate create, model path,
  `possible_same_person`, reconsideration, confirmed merges), matches
  Wikipedia identity, opens and advances bounded coverage plans (Brave search,
  article fetch and extraction, per-article assessment), writes a dated
  digest plus `latest.md`, and prints the same Markdown on standard output.
  `notable run` now requires `BRAVE_API_KEY` as well as `OPENROUTER_API_KEY`;
  it refuses to start without either.
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
- Coverage evidence (milestone 5). Three work kinds and no more —
  `brave_web_search`, `fetch_article`, and `assess_article` (K2) — each making
  exactly one external call per execute. The Brave Web Search adapter
  (`providers/brave.py`, endpoint-only config, `BRAVE_API_KEY` from the
  environment, paced by `brave_min_interval_ms`); the article fetcher and the
  Trafilatura extractor (`providers/articles.py`,
  `providers/article_versions.py`) which persist cleaned article views and
  never raw HTML. The `coverage/` package: the Wikipedia eligibility gate
  (only `no_matching_page_found` and `uncertain_identity` people are
  researched, so a current `matching_page_found` stops research), bounded
  per-person coverage plans with deterministic query forms in four variants
  (`exact`, `exact_obituary`, `alias`, `context`), deterministic publisher
  screening against the versioned source
  policy (`curated_eligible` / `curated_ineligible`, with absence of a
  matching rule meaning `unclassified`; first matching rule wins over
  `host_exact`, `host_suffix`, and `path_prefix`), deterministic article
  selection with a bounded unclassified fallback, person-specific passage
  selection, and immutable person–article assessments with their signals.
  Merge reconciliation for coverage work, coverage material fingerprints and
  refresh, and a "Coverage evidence" digest section plus `notable status`
  coverage lines.

Delivered only in part — do not describe these as finished:

- `notable status` reports the latest run, its digest, required pending and
  deferred counts, operational failures, corpus source-item and article totals,
  each feed's latest successful fetch, durable triage counters (triaged,
  untriaged, research, uncertain, do not research, insufficient input, failed
  triage, and unresolved research or uncertain mentions), and durable identity
  counters (canonical people, merged-away people, unresolved eligible
  mentions under K24, active `possible_same_person`, and mentions linked to
  people), and — when the coverage schema is present — three coverage lines
  (people with a completed assessment, coverage eligible remaining, and people
  stopped because they match Wikipedia). It has no digest backlog, no oldest
  pending candidate, no queue tiers, and still no budget or deferral-reason
  breakdown; those need later milestones.
- The digest emits its header, banner, operational summary, per-run budget
  line, deferral-reason breakdown, ingestion summary, person-detection summary
  (triage outcomes, unresolved mention counts, model deferred/failed,
  OpenRouter cost), and person-identity summary (people created, mentions
  resolved, outcome split including created_new vs different_people, K24
  eligible remaining, active possible_same_person, confirmed merges, resolve
  model deferred/failed), and a "Coverage evidence" section (plans completed,
  incomplete, and permanently failed this run; assessments completed this run;
  people with a completed assessment; coverage eligible remaining; people
  stopped because they match Wikipedia; assess model deferred and permanently
  failed) emitted only when the coverage schema is present. Its shortlist
  section is still a placeholder — "No candidates met the shortlist criteria
  in this window." is printed unconditionally, because there is no ranking and
  no model synthesis yet.

Not built at all, so do not document, import, or assume any of it:

- Lead aggregation. Milestone 5 stops at the per-article assessment. There is
  no lead assessment record, no person pointer to one, and no
  `assess_person_lead` work item; the strings `promising_lead`,
  `possible_lead`, `insufficient_evidence`, and `assessment_incomplete` appear
  nowhere in `src/`, and K12 forbids inventing them. A person still has no
  product verdict.
- Ranking, the digest queue, the digest shortlist, synthesis, and drafting.
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
- **Six test files in `tests/coverage` are stubs that assert nothing.** They
  were left behind by the interrupted session and were never filled in. They
  pass, they are counted in the gate, and they protect nothing:
  - `test_live_brave.py`, `test_live_article_fetch.py`,
    `test_live_openrouter_assess.py` — each a `live`-marked `pass`, unlike the
    substantive MediaWiki and OpenRouter match smokes in `tests/wikipedia`.
  - `test_seams.py` — a `live`-marked `pass`. It is also the reason
    `uv run pytest tests/coverage -m live` collects **four** tests, not three;
    the fourth is a no-op cross-component seam, so no coverage seam is
    exercised live or offline.
  - `test_run_cli.py` — two lines, `def test_run_cli_coverage_registered():
    pass`, with no imports. It is *not* `live`-marked, so it runs green in
    every offline gate while verifying nothing about handler registration.
  - `test_digest_status.py` — 22 lines whose own comment reads "Testing that
    the queries don't crash". It calls `coverage_corpus_counts` and
    `coverage_run_counts` against a freshly migrated, empty database and
    asserts every counter is `0`. It never populates a plan, never renders the
    "Coverage evidence" digest section, and never runs a `notable status`
    line. Delete the whole coverage summary and it still passes.

  So there is **no regression protection at all** for coverage CLI handler
  registration, for cross-component seams, for the digest section's rendered
  text, or for the status lines — the four things a reader would most expect
  the file names to cover. Everything else in `tests/coverage` (schema, Brave
  and article adapters, screening, selection, passages, queries, eligibility,
  the assessment contract, the fetch/assess/HTTP services, repository, merge
  hooks, seed hooks) is substantive. Before milestone 5 cutover these six need
  real bodies, and the three live smokes then need an operator run with
  `BRAVE_API_KEY` and `OPENROUTER_API_KEY`, recording
  model/provider/usage/cost/outcome without secrets. Do not read a green
  `tests/coverage` run — offline or `-m live` — as evidence about any of them.

## Rewrite Structure

- `src/notable_person_finder/` — installable application package.
  - `cli/` — argument parsing and the `notable` commands.
  - `config/` — strict configuration models, loading, and path resolution.
  - `db/` — SQLite connections and forward-only checked migrations.
  - `runs/` — run engine, clock, repository, work-item scheduling, retry
    coordination, budget reservation, and the mutation lock.
  - `providers/` — the shared HTTP transport, request safety checks, pacing,
    provider failure classification, the feedparser-backed feed adapter, the
    MediaWiki client, the Brave Web Search client, the article fetcher and
    Trafilatura extractor, and the OpenRouter client. This is the only package
    that may import `httpx`, `trafilatura`, or the OpenRouter SDK.
  - `ingestion/` — feed seeding and handling, URL identity, domain models, and
    transaction-neutral persistence helpers for ingestion settlements.
  - `people/` — detection and identity: triage, first-pass resolution,
    reconsideration, confirmed merges, candidate retrieval, prompts, and
    domain validation.
  - `wikipedia/` — MediaWiki query plans, candidate assembly, identity
    observations, match handler, seed/merge hooks.
  - `coverage/` — coverage research: the Wikipedia eligibility gate, coverage
    plans and query forms, publisher screening against the source policy,
    article selection, passage selection, the `assess_article` contract and
    prompt, repository SQL, the three work-item handlers, and merge hooks.
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
- `tests/coverage/` — substantive coverage for the Brave and article adapters,
  the coverage schema, eligibility, queries, screening, selection, passages,
  the assessment contract, the HTTP/fetch/assess services, repository, merge
  hooks, and seed hooks. Six files in this directory are stubs that assert
  nothing — `test_run_cli.py`, `test_seams.py`, `test_digest_status.py`, and
  the three live smokes — so despite their names there is no test protecting
  coverage CLI registration, cross-component seams, the rendered digest
  section, or the status lines. See the known gaps above before trusting a
  green run.
- `docs/architecture/at-least-once-execution.md` — the operator-facing note on
  the crash windows in which a paid provider call can be repeated. Point at it
  rather than restating it.
- `config/*.example.toml` — tracked, copyable configuration examples; local
  configuration variants remain untracked.
- `config/source_policies/visual_arts.toml` — the curated publisher policy. It
  is a tracked product artifact, not an example to copy: changes belong in a
  reviewed diff, and every screening decision records its fingerprint.
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
- For completed coverage evidence, use `uv run pytest tests/coverage`.
- The seven completed milestones together gate with
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage`.
  Run it from a real checkout: it needs the tracked `config/` directory,
  including `config/source_policies/`.
  Default pytest `addopts` deselect `live`. Opt-in live smokes:
  - feeds: `uv run pytest tests/ingestion -m live -v`
  - OpenRouter (detect + resolve):
    `OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v`
  - MediaWiki + match OpenRouter:
    `uv run pytest tests/wikipedia -m live -v` (OpenRouter key for match smoke)
  - Brave + article fetch + assess OpenRouter:
    `BRAVE_API_KEY=… OPENROUTER_API_KEY=… uv run pytest tests/coverage -m live -v`
    — this collects four tests (the three smokes plus `test_seams.py`) and all
    four are `pass` stubs, so it needs no key and proves nothing. See the known
    gaps above.
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
