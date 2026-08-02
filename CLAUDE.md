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

Nine milestones are complete: the application foundation, the run engine and
shared transport, feed ingestion, the OpenRouter model gateway with person
detection (3b1), durable person identity with first-pass resolution,
reconsideration, and confirmed merges (3b2), Wikipedia identity matching
with MediaWiki retrieval and semantic match (milestone 4), coverage
evidence — bounded Brave Web Search, article fetch and extraction, and
per-article assessment (milestone 5), lead aggregation with the digest
queue, deterministic ranking, and the real digest shortlist (milestone 6a),
and read-only audit and inspection commands — `notable digest show`,
`notable audit run`, and `notable audit person` (milestone 6b-i). A
cold-starting agent should assume nothing beyond this list.

Milestone 5's authorities are
`docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` (locked
decisions K1–K34, plus a 2026-08-01 amendment) and
`docs/superpowers/plans/2026-07-30-coverage-evidence.md`. The 2026-08-01
`coverage-research-design` spec and `coverage-discovery` plan are superseded
and were never implemented; they contradict the locked decisions and must not
be built from.

Milestone 6a's authorities are
`docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
(locked decisions K1–K13) and the four sequential sub-plans
`docs/superpowers/plans/2026-08-01-lead-aggregation-*.md`, all of whose steps
are recorded complete.

Milestone 6b-i's authorities are
`docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md` (locked
decisions K1–K14, plus the recorded `--attempt` "raw response" amendment) and
the three sequential sub-plans
`docs/superpowers/plans/2026-08-02-audit-i-*.md`, all of whose steps are
recorded complete.

Delivered and usable — the whole `notable` surface:

- `notable config validate`, `notable paths`, `notable db migrate`.
- `notable run` — validates configuration, migrates, takes the mutation lock,
  sweeps interrupted predecessor runs, then executes eligible work through the
  scheduler: feed ingestion, person detection, person resolution and confirmed
  merges, Wikipedia identity matching, bounded coverage research, and lead
  aggregation. It writes a dated digest plus `latest.md` and prints the same
  Markdown on standard output. It requires both `OPENROUTER_API_KEY` and
  `BRAVE_API_KEY` and refuses to start without either.
- `notable status` — the latest run and its digest, required pending and
  deferred counts, operational failures, corpus totals, each feed's latest
  successful fetch, and durable triage, identity, coverage, and digest-backlog
  counters. Coverage and lead lines appear only when their schema is present.
- `notable digest show [run_id]`, `notable audit run <run_id> [--attempt
  <attempt_id>]`, and `notable audit person <person_id>` — read-only
  inspection. All three open the database `readonly=True` and take no mutation
  lock, and every audit section degrades to an explicit "section unavailable"
  marker rather than raising when the database predates a table (K3).

What each package does is under Rewrite Structure below, and the locked
decisions behind it are in each milestone's spec. Do not restate either here.

Delivered only in part — do not describe these as finished:

- `notable status` prints bare pending and deferred counts, with no budget
  figures and no deferral-reason breakdown, so it cannot explain *why* work was
  deferred. The digest can; `status` has not caught up.
- The digest shortlist is rendered entirely from deterministic aggregation
  output, so no entry carries a `why_review` narrative.

Not built at all, so do not document, import, or assume any of it:

- The `compose_lead_summary` optional model synthesis and its Promptfoo suite,
  and source reconnaissance for unclassified publishers. These remain milestone
  6b-ii scope and were not touched by 6b-i.
- Drafting. The application never generates or publishes Wikipedia content.

### Known gaps

Live, diagnosed gaps in shipped behaviour are recorded in full in
`docs/architecture/known-gaps.md`, so that a later change does not mistake one
for a regression. Read it before working in the areas it covers, and point at
it rather than restating its reasoning:

- Budget reservation carries a deliberate ~4x margin (UTF-8 bytes charged as
  tokens). Not a defect — do not "fix" it without reading the entry.
- Model-output validation: all five diagnosed causes are fixed (2026-08-02),
  including the one that made `no_matching_page_found` unreachable and so
  disabled coverage research entirely. The entry also records the one rule
  that **cannot** be expressed in a schema — strict structured output rejects
  a root-level `anyOf` — so do not retry it.
- `uncertain_identity` now absorbs most true negatives, because truncation
  fires on 111 of 118 plans. Not a defect; the lever is `max_candidates`.
- `notable audit person` does not yet render the complete K11 forensic record.
- K1's `canonical_domain` cross-host alias override is not wired in.
- A non-settling handler discards its payload after an external call.
- Two crash windows can repeat a paid provider call; see
  `docs/architecture/at-least-once-execution.md`.

Two recorded decisions that look like gaps and are not:

- `notable audit run --attempt` shows the persisted validated result and its
  provenance, never a raw request or response body, because no raw payload is
  persisted anywhere in the current schema. That is a recorded amendment in
  `docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md`
  ("Amendments to Approved Specifications"), not a shortcut.
- Migration `0008_lead_aggregation.sql` was revised in place under its own
  plan's authorization. A database that ran the earlier 0008 must be deleted
  and re-migrated; see `docs/troubleshooting.md`.

## Next Steps

As of 2026-08-02 the pipeline runs end to end and produces a real shortlist.
A full uncapped ten-feed run over 246 source items cost **$1.08** and yielded
65 Wikipedia matches, 27 completed coverage plans, 74 article assessments, 7
`promising_lead` and 19 `possible_lead` outcomes, and 5 digest entries. Both
earlier blockers — budget over-reservation and the unreachable
`no_matching_page_found` outcome — are fixed.

In rough priority order:

1. **Raise `max_candidates` (currently 8).** The highest-value open item.
   Truncation fires on 111 of 118 plans, which forces most true negatives to
   report as `uncertain_identity` and blurs the shortlist's Wikipedia signal.
   Needs its own design: it trades retrieval cost and prompt size against
   answer quality, and it moves match fingerprints.
2. **Milestone 6b-ii — `compose_lead_summary`.** The remaining unbuilt product
   surface. Shortlist entries render from deterministic aggregation with no
   `why_review` narrative. Source reconnaissance for unclassified publishers
   belongs here too.
3. **`notable status` budget figures and deferral-reason breakdown.** The
   digest has both; `status` still cannot explain *why* work deferred.
4. **`notable audit person` K11 gaps.** Several required forensic fields are
   loaded but not rendered, or discarded while loading.
5. **Residual permanent failures.** The verification run ended `partial` on 3
   (one each of `detect_people`, `mediawiki_search`, `resolve_person_entity`),
   down from 7. `notable audit run <id> --attempt <id>` now shows the
   validation reason directly, so these no longer need a live replay to
   diagnose — start there.

Cutover to `main` remains a separate, explicit decision. The legacy prototype
is still the operational fallback until then.

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
  - `leads/` — lead aggregation and the digest queue: deterministic
    aggregation of per-article assessments into a lead outcome, ranking,
    queue lifecycle and transitions, repository SQL for the lead, queue, and
    digest tables, the `aggregate_person_lead` handler and its scheduling and
    sweep hooks, and merge reconciliation.
  - `audit/` — read-only audit and inspection: the `notable digest show`
    hash-verified digest re-read, the `notable audit run` / `notable audit
    person` repository queries with per-section `_table_present` degradation
    (K3), the `task_type`-keyed result-binding registry (`registry.py` — not
    `(provider, operation)`, which is ambiguous because five task types
    share the single OpenRouter `generate_structured` operation), and
    Markdown rendering (`render.py`). Opens the database `readonly=True` and
    takes no mutation lock.
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
  hooks, seed hooks, CLI registration and integration (`test_run_cli.py`),
  cross-component seams (`test_seams.py`), and digest/status rendering
  (`test_digest_status.py`), plus three opt-in live smokes.
- `tests/leads/` — lead aggregation and digest queue: aggregation and ranking
  logic, queue lifecycle, repository SQL, the handler service and its
  scheduling and fingerprint-reuse behaviour (`test_service.py`), merge hooks,
  CLI registration and same-run firing (`test_run_cli.py`), and digest and
  `notable status` rendering (`test_digest_status.py`). No live smokes: the
  milestone makes no external call.
- `tests/audit/` — digest-hash verification and lookup, the `task_type`-keyed
  result-binding registry, Markdown rendering for run, attempt,
  and person audits, repository queries including per-section schema-
  degradation (K3), CLI integration for all three commands, and cross-
  component seams (`test_seams.py`, including the layering check that
  `audit/` never imports `reporting/digest.py`). No live smokes: the
  milestone makes no external call.
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
- For completed lead aggregation and the digest queue, use
  `uv run pytest tests/leads`.
- For completed audit and inspection commands, use `uv run pytest tests/audit`.
- The nine completed milestones together gate with
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads tests/audit`.
  Run it from a real checkout: it needs the tracked `config/` directory,
  including `config/source_policies/`.
  Default pytest `addopts` deselect `live`. As of 2026-08-02 all 14 live
  smokes have been executed against real providers and pass, and all ten
  configured feeds have been fetched for real, so reported model cost is real
  and the budget reservation path has met live pricing. A written live smoke
  is not evidence until it has actually run: the two most recent provider
  defects were both invisible offline and to two independent static reviews.
  Opt-in live smokes:
  - feeds: `uv run pytest tests/ingestion -m live -v`
  - OpenRouter (detect + resolve):
    `OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v`
  - MediaWiki + match OpenRouter:
    `uv run pytest tests/wikipedia -m live -v` (OpenRouter key for match smoke)
  - Brave + article fetch + assess OpenRouter:
    `BRAVE_API_KEY=… OPENROUTER_API_KEY=… uv run pytest tests/coverage -m live -v`
    — this collects three tests. An adjacent `.env` supplies both keys, so
    `set -a && . ./.env && set +a` before the command is enough; never pass a
    key inline in a way that lands in shell history or output.
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
- A refused `resolve_person_entity` or `reconsider_person_entity` prepare
  settles `failed_permanent` **by design** — do not "fix" it.
  `docs/superpowers/specs/2026-07-30-durable-person-identity-design.md` (line
  759, restated at 1368) requires prepare to call
  `ensure_resolution_for_mention` first and *then* raise a plain `ValueError`
  prefixed `resolve_prepare_refused:`, so the engine settles the claimed item
  with no new attempt: that permanent state is accounting noise, not a domain
  failure, because the mention's durable state was already written.
  `tests/people/test_resolution_service.py::
  test_empty_at_prepare_ensure_then_value_error` and
  `tests/people/test_reconsideration_service.py::
  test_missing_peer_prepare_refuse` pin it deliberately. The cost is real but
  cosmetic: the run reports operational failures and goes `partial` for work
  that in fact completed. Making the engine settle a refusal as `succeeded`
  would change an approved design and needs a design revision, not a
  remediation branch.
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
