# Rewrite Agent Guidance Design

**Date:** 2026-07-25

## Goal

Replace the repository's legacy-oriented agent instructions with concise,
rewrite-first guidance that accurately describes the clean-slate architecture,
verification boundary, and pull-request workflow.

## Instruction Files

`CLAUDE.md` remains the canonical tracked instruction file. A repository-root
`AGENTS.md` relative symlink points to `CLAUDE.md`, so agents that discover
either conventional filename receive identical instructions without duplicated
content.

## Active Development Context

The guidance treats the installable `src/notable_person_finder` package and the
approved redesign programme as current. It directs agents to the redesign spec
and milestone plans for detailed architecture rather than restating volatile
implementation details.

Active commands use `uv`, focused rewrite tests, and the installed `notable`
CLI. The guide must not present legacy scripts, JSONL state, unittest commands,
or prototype gate structure as current rewrite conventions.

## Branch and Review Workflow

Rewrite implementation happens on feature branches. By default, pull requests
target `refactor/rearchitecture`; merging to `main` is reserved for an explicit
later cutover decision.

New milestone, feature, and design work publishes and integrates through this
sequence:

1. Verify the feature branch with the milestone's approved rewrite checks.
2. Ask for explicit user authorization before pushing.
3. Push the feature branch and open a pull request targeting
   `refactor/rearchitecture`.
4. Have a separate agent independently review the pull request.
5. Address all blocking review findings and obtain re-review when fixes are
   required.
6. Merge only with explicit user authorization after review approval.

Remediation of review findings is a distinct, lighter path. Fixes for findings
raised against already-integrated work carry no fresh review requirement,
because the review that produced the findings is itself the independent
judgement; requiring a second review of the remedy would be circular. Such
fixes merge into `refactor/rearchitecture` from a focused branch with explicit
user authorization and without a pull request.

The lighter path is conditional, not discretionary. It applies only to changes
that trace to specific findings against integrated work, add no feature
surface, interface, or dependency, carry regression tests that fail before the
fix and pass after, and leave the milestone's verification gate passing. An
implementing agent must not widen the classification to cover adjacent
improvements it happens to want; work that outgrows the findings returns to the
full path.

Agents must not push, merge, or target `main` merely because implementation and
tests are complete, on either path.

## Legacy Prototype

The guide retains only a short legacy section. It states that the root-level
prototype, its scripts, JSONL state, and tests are historical evidence. Agents
must not import, reorganize, remove, run, or repair them unless explicitly
requested. Prototype tests are not a rewrite gate.

## Maintenance

The guide stays concise and principle-oriented. Detailed milestone interfaces,
commands, and completion gates belong in `docs/superpowers/plans/`; programme
architecture and cutover policy belong in `docs/superpowers/specs/`. This keeps
repository instructions stable as later rewrite milestones replace foundation
details.
