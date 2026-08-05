# Repository Agent Guide

## Current Development Direction

This repository is being rebuilt as `notable`: a small, maintainable
application implementing the core "find promising leads" loop. Current
development belongs in `src/notable/`.

The single architectural authority is
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. Read it before
implementing. `docs/findings.md` records provider and model facts that cost
real money to discover — read it before touching schema construction or prompt
rendering.

Two prior versions exist and **neither is a base for this one**:

- The **legacy prototype** at the repository root (`run_pipeline.py`,
  `name_utils.py`, `scripts/`, `ingest/`, `prompts/`, `state/`, `output/`,
  `tests/test_*.py`) is the operational fallback until cutover. Do not import,
  reorganize, remove, run, or repair it. Its tests are not a gate.
  Its untracked local configuration — `config/notable.toml`,
  `config/discovery-feeds.toml`, `config/discovery_profiles/`,
  `config/feeds*.md` — belongs to it too. `config/notable.toml` uses a
  different schema and the MVP's strict loader rejects it by design; that is
  not a bug to fix. MVP configuration is `config/*.example.toml`, copied to
  `config/mvp.local.toml`.
  **These files are untracked, so `git status` is never empty.** "Clean"
  means nothing you created is left uncommitted.
- The **`refactor/rearchitecture` branch** is a complete, working rewrite that
  succumbed to the second-system effect: 38,608 lines of source, 71,334 of
  tests, 45 SQLite tables. It is frozen reference. Consult it for product
  reasoning; do not port its code.

**Important Note**: Now that the original plan for the MVP is feature complete, the focus should be on ensuring better outputs without adding scope or unnecessary complexity. Additionally, determine the right time to focus on prompt engineering and evals instead of the code and core loop logic.

## Hard Guardrails

The prior failure mode was gradual accretion in which every individual step
looked justified. These limits are deliberately uncomfortable. When one binds,
the first response is to ask what can be removed — not to raise the limit.

1. **Source line count is a canary, not the product.** Check with
   `find src -name '*.py' | xargs wc -l | tail -1`.
   - **Soft target: ~3,000 lines.** Crossing it is a smell: say what grew and
     why in the PR. Prefer deleting scope or machinery over shaving blanks,
     densifying one-liners, or gutting unrelated modules to hit an exact
     number.
   - **Hard alert: ~3,500 lines, or any new table / cross-run state / second
     recovery path.** That is where second-system failure starts again; stop
     and cut concepts, not comments.
   - Planned MVP surface (including finishing a named phase) may land slightly
     over 3,000 without ceremony. New *machinery* may not.
2. **No new table without deleting one.** Four is the budget: `item`,
   `surfaced`, `run`, `lead`.
3. **`pipeline.py`'s main loop fits on one screen.**
4. **No new cross-run state** without an explicit decision to leave MVP scope.

Two further rules from the spec that are easy to erode:

- **Consume less than the prompt emits.** Ported prompts return more than the
  rules use. Store all of it in `lead.detail_json`; consume only what an
  outcome or ranking rule needs. Every consumed field is a concept the reader
  holds and a branch the tests cover.
- **`run` and `lead` are logs the pipeline never reads.** They are written
  outside the state transaction, best-effort. If code starts reading `lead`, or
  it moves inside the state transaction, the durable-workflow system is growing
  back and that needs an explicit decision.

## Foundation Invariants

- **The application never edits Wikipedia.** Every publication action requires
  explicit human review. This is not a milestone boundary.
- Distribution and import package: `notable`; executable: `notable`.
- Configuration is file-first; environment variables supply secrets only.
  Secrets must never appear in diagnostics, terminal output, tests, or cache
  keys.
- **Recovery is replay, not reconstruction.** Nothing durable is written until
  the digest file exists. A crash before `store.commit` leaves the database
  untouched and the re-run replays from cache.

## Environment and Verification

- Python 3.13 or newer. `uv sync` to install.
- `git config core.hooksPath .githooks` (Conventional Commits are enforced).
- Checks: `uv run ruff check .`, `uv run ruff format .`, `uv run pyright`.
- Tests: `uv run pytest tests/mvp`. Default `addopts` deselect `live`.
- MVP tests live in `tests/mvp/`, never `tests/test_*.py` — ruff excludes the
  latter by path segment at any depth to keep the prototype unlinted.
- Live smokes are opt-in: `uv run pytest tests/mvp -m live -v`. An adjacent
  `.env` supplies keys; `set -a && . ./.env && set +a` before running. Never
  pass a key inline where it lands in shell history.

**A written live smoke is not evidence until it has actually run.** The prior
programme's two most expensive defects were invisible offline and survived two
independent static reviews.

## Testing

Target roughly a 1:1 test-to-source ratio. The spec's "Named invariant tests"
table lists the rules that are cheap to break and expensive to notice — each
fails as *missing output* rather than as a crash. Every one must fail if its
rule is removed.

The prior programme's mutation-evidence protocol is **dropped**. It was a
rational response to a 71,000-line suite whose coverage claims could not be
trusted; at this size it is friction without a corresponding risk.

## Branch and Review Workflow

`mvp` is the integration branch for this rebuild. Implement on a focused
feature branch, open a pull request against `mvp`, and have a separate agent
review it.

Do not push, open or modify pull requests, merge, or target `main` without
explicit user authorization. `main` remains the operational fallback.

## Current Focus (as of Phase 4 MVP Feature Complete)
Now the original plan for the MVP is feature complete, the focus should be on ensuring better outputs without adding scope or unnecessary complexity, and determining the right time to focus on prompt engineering and evals instead of the code and core loop logic.
