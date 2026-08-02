# Milestone 6b-i: Audit and Inspection Commands

| Field | Value |
| --- | --- |
| **Status** | Draft (revision 1) |
| **Date** | 2026-08-02 |
| **Author** | (design agent) |
| **Branch** | `feat/audit-commands` (proposed) |
| **Programme** | Notable Person Finder clean-slate rewrite |
| **Depends on** | Milestones 1–6a complete on `refactor/rearchitecture` (HEAD `1733cc6`) |

## Overview

Seven milestones have persisted a large, carefully fingerprinted evidence
trail: runs, transitions, work items, attempts, triage observations, entity
resolution, Wikipedia identity, coverage plans, article assessments, lead
assessments, queue transitions, and digests. Almost none of it is reachable
from the command line. `notable status` prints a dozen aggregate counters;
`notable run` prints the current digest. Everything else requires opening
SQLite by hand.

Milestone 6b-i makes that evidence inspectable. It adds the three read-only
commands the operator-experience design has specified since 2026-07-24:

```text
notable digest show [RUN_ID]
notable audit run RUN_ID [--attempt ATTEMPT_ID]
notable audit person PERSON_ID
```

This milestone owns:

- the `audit/` capability package (view models, read-only repository SQL,
  pure text rendering, digest lookup and verification);
- an explicit attempt-to-result registry, keyed on `task_type`, covering
  every registered handler — the eleven that make an external call and the
  one local handler, which is marked as making none;
- the `tests/audit/` package and its addition to the type-checked set.

It performs **no** writes, **no** migrations, **no** model calls, and adds
**no** dependency. It takes no mutation lock.

It does not implement `compose_lead_summary` synthesis, source
reconnaissance for unclassified publishers, or the Promptfoo suite. Those
remain milestone 6b-ii.

## Background and Motivation

### Current state (end of milestone 6a)

Milestone 6a landed lead aggregation, the digest queue, deterministic
ranking, and the real digest shortlist. The full offline gate passes: 1921
tests, 14 deselected (`live`), exit code 0.

The command surface is `run`, `status`, `paths`, `config validate`, and
`db migrate`. `cli/main.py` is 1,286 lines and already holds argument
parsing, every command body, and the `status` summary assembly.

### Pain points this milestone removes

- A digest written by an earlier run can only be retrieved by knowing the
  configured digest directory and the file naming convention.
- When a run ends `partial`, the operator can see *that* work deferred but
  not *why*: `notable status` prints bare pending and deferred counts with
  no reason breakdown. The digest carries a breakdown only for the run that
  just happened, and only while that digest is the latest.
- A person's verdict is visible in the digest, but the evidence chain that
  produced it — which mentions, which resolution, which Wikipedia outcome,
  which articles, which signals — is not reachable at all.
- A failed or expensive attempt cannot be traced to the result it produced.

### Authorities this design refines

This document refines, without replacing:

- `docs/superpowers/specs/2026-07-24-operator-experience-verification-design.md`
  — the command surface, exit statuses, stream discipline, and locking
  policy. Section "Amendments to Approved Specifications" below records the
  one clause this milestone changes.
- `docs/superpowers/specs/2026-07-24-domain-persistence-design.md` — the
  persisted evidence this milestone reads.
- `docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
  — the `digest` and `digest_entry` tables `digest show` reads, and the
  milestone that deferred these commands to 6b.

## Decisions

Decisions are numbered `K1`–`K14` and are binding on the implementation
plan. A change to a K-numbered decision is a change to this document, not an
implementation choice.

### K1 — The package is `audit/`, and `cli/main.py` stays thin

`audit/` follows the established capability-package convention set by
`coverage/` and `leads/`:

| Module | Responsibility |
| --- | --- |
| `audit/models.py` | Frozen view models, one per section. No SQL, no I/O. |
| `audit/repository.py` | All read-only SQL. Returns view models. Never mutates. |
| `audit/render.py` | Pure view model → text. Takes no database handle. |
| `audit/digest_show.py` | Digest row lookup, file read, SHA-256 verification. |

`cli/main.py` gains parser wiring and three thin `command_*` functions that
open a connection, call the repository, and print the rendered text. No
query or formatting logic is added to `cli/main.py`.

The `render.py` / `repository.py` split is load-bearing for testing, not
cosmetic: rendering assertions run against constructed view models with no
database fixture, and repository assertions run against fixtures without
asserting on formatting. A change to wording cannot break a query test, and
a change to a join cannot break a wording test.

`audit/` may import `config/`, `db/`, and `obs/`. It must not import
`reporting/`, `leads/`, `coverage/`, `wikipedia/`, `people/`, `ingestion/`,
`runs/`, or `providers/`. It reads those packages' tables directly; it does
not reuse their write-path code. This is asserted by a layering test in the
same manner as milestone 5's K1 package-layering check.

### K2 — The three commands are read-only and take no mutation lock

Consistent with the operator-experience design: read-only `status`, `paths`,
`digest`, and `audit` commands do not acquire the mutation lock, and SQLite
WAL with short read transactions lets them inspect committed state while a
batch is running.

A test asserts each command succeeds while the mutation lock is held by
another handle.

### K3 — Audit never migrates

The commands open the database without running migrations. Audit is what an
operator reaches for when a run went wrong; silently upgrading the schema
under inspection would destroy the evidence being sought.

Against a database older than the current migration set, each section guards
on table presence — the same pattern `notable status` already uses for the
leads schema — and prints

```text
section unavailable: schema predates migration NNNN
```

rather than raising on a missing table. A command whose *entire* subject is
missing (for example `digest show` against a database with no `digest`
table and no `run.digest_path`) reports that and exits `1`.

### K4 — `digest show` reads the file and verifies it against `content_hash`

The `digest` table stores `file_path`, `timezone`, `window_start`,
`window_end`, `run_state`, `content_hash`, and `created_at` — but not the
Markdown body. The body is on disk.

`digest show` therefore reads the file and verifies it. Storing a second
copy of the body in the database was rejected: it duplicates content, grows
the database without bound, and would be a write-path change to a digest
writer that shipped one milestone ago. Reading the file without verifying
was rejected because it would waste the `content_hash` 6a already records
and would print a hand-edited digest as though it were authentic.

Resolution: with `RUN_ID`, the `digest` row for that run; without, the row
with the highest `run_id`. `record_digest_with_entries` inserts exactly one
row per run that writes a digest, including runs whose shortlist is empty,
so the table is a complete digest history.

Fallback: if the run exists but has no `digest` row (a run predating
migration 0008), fall back to `run.digest_path` and `run.digest_sha256`,
which `runs/engine.py` sets at run finish. If neither source exists, that
run produced no digest.

| Condition | Behaviour | Status |
| --- | --- | --- |
| Hash matches | Bytes written verbatim to stdout | `0` |
| File missing | stderr names the path and run; stdout empty | `1` |
| Hash mismatch | stderr names path, expected and actual hash; **stdout empty** | `1` |
| Unreadable or not UTF-8 | stderr carries the OS error; stdout empty | `1` |
| Run has no digest | stderr says so | `1` |
| Run does not exist | stderr says so | `1` |

Printing nothing to stdout on mismatch is the entire point of the check. A
piped `notable digest show | mail` must not forward a document that failed
verification.

### K5 — `digest show` never re-renders

The command does not import `reporting/digest.py` and does not touch
`leads/`. It reads bytes and verifies them. This satisfies the
operator-experience design's requirement that the command "never reruns
selection or synthesis," and it is enforced by the K1 layering test rather
than by convention.

### K6 — `digest show` emits the digest and nothing else

No header, no banner, no "verified" line, no trailing summary. Stdout is the
exact persisted document, so that

```text
notable digest show 12 | shasum -a 256
```

reproduces the recorded `content_hash`. Diagnostics go to stderr.

### K7 — `audit run` sections and their order

1. **Run header** — id, state, timezone, window start and end, started and
   finished timestamps, duration.
2. **Configuration provenance** — `configuration_snapshot.fingerprint`,
   `created_at`, and `canonical_json` pretty-printed.
3. **Transitions** — every `run_transition` in id order: state, reason,
   `occurred_at`.
4. **Work outcomes** — counts by `task_type` × `state`, then the individual
   `failed_permanent` and `deferred` rows with their `reason`. This is the
   per-run deferral-reason breakdown `notable status` still lacks.
5. **Attempts** — one line each: id, provider, operation, ordinal, outcome,
   `failure_category`, `provider_status`, `latency_ms`, `response_bytes`,
   `destination_host`, reserved and actual cost.
6. **Failures** — attempts with outcome `failed` or `interrupted`, grouped
   by `failure_category`.
7. **Budget** — `budget_limit_nano_usd`, reserved, and actual rendered as
   USD, plus the summed `attempt.actual_nano_usd` as a cross-check. A
   divergence between the run's rolled-up total and the attempt sum is
   displayed, not hidden.
8. **Reporting result** — the `digest` row: path, content hash, `run_state`
   at write time, and entry count, with a pointer to
   `notable digest show RUN_ID`.

### K8 — `--attempt` is restricted to the named run

An attempt id belonging to a different run is an error with status `1`, not
a silent redirect. The operator asked about a specific run; answering with
another run's data would be a correctness bug disguised as convenience.

`--attempt` prints the full attempt row, its retry history (all sibling
attempts sharing `work_item_id`, in `ordinal` order), the owning
`work_item` (task type, subject kind and id, fingerprint, state, reason),
and the persisted validated result located through the K9 registry.

### K9 — The attempt-to-result registry is keyed on `task_type`

**The registry key is `work_item.task_type`, not `(provider, operation)`.**

`(provider, operation)` is ambiguous and cannot be used. Five distinct task
types share the single pair `(openrouter, generate_structured)` —
`detect_people`, `resolve_person_entity`, `reconsider_person_entity`,
`match_wikipedia_identity`, and `assess_article` — because
`GENERATE_OPERATION` is one constant in `providers/openrouter.py` reused by
every structured-generation handler. Keying on that pair would let
`--attempt` select the wrong result surface.

`task_type` is unambiguous across all twelve registered handlers.
`inspect_model` is its own task type, so it does not collide with the
generation tasks that share its provider. An attempt reaches its task type
through `attempt.work_item_id → work_item.task_type`, which `--attempt`
already loads to print the owning work item.

The registry maps each `task_type` to its result table, join columns,
provenance columns, and whether the task makes an external call:

| `task_type` | Result table | Join | External |
| --- | --- | --- | --- |
| `fetch_feed` | `feed_fetch` | `(run_id, work_item.subject_id → feed_identity_id)` | yes |
| `inspect_model` | `model_inspection` | `attempt_id` | yes |
| `detect_people` | `triage_observation` | `(attempt_id, run_id)` | yes |
| `resolve_person_entity` | `entity_resolution_observation` | `(attempt_id, run_id)` | yes |
| `reconsider_person_entity` | `entity_resolution_observation` | `(attempt_id, run_id)` | yes |
| `mediawiki_search` | `mediawiki_search_observation` | `(attempt_id, run_id)` | yes |
| `mediawiki_page_facts` | `wikipedia_page_facts_batch` | `attempt_id` | yes |
| `match_wikipedia_identity` | `wikipedia_identity_observation` | `(attempt_id, run_id)` | yes |
| `brave_web_search` | `brave_search_observation` | `(attempt_id, run_id)` | yes |
| `fetch_article` | `article_view` | `(attempt_id, run_id)` | yes |
| `assess_article` | `person_article_assessment` | `(attempt_id, run_id)` | yes |
| `aggregate_person_lead` | `lead_assessment` | `(person_id, run_id)` | **no** |

`resolve_person_entity` and `reconsider_person_entity` legitimately share
`entity_resolution_observation`; the registry is many-to-one, not a
bijection. That is fine — the key is unique, the target need not be.

For model tasks the rendering includes version provenance: model,
`prompt_hash`, `schema_version`, token usage, and cost.

#### `fetch_feed` cannot be resolved to a single attempt

`feed_fetch` records `feed_identity_id`, `run_id`, and `requested_at`, but
**no `attempt_id`**, and `attempt` records no URL — only
`request_fingerprint` and `destination_host`. There is therefore no column
pair that identifies which attempt produced which `feed_fetch` row when a
work item retried inside one run.

`--attempt` for a `fetch_feed` attempt lists **every** `feed_fetch` row for
that `(feed_identity_id, run_id)` in `requested_at` order, under an explicit
caveat:

```text
feed_fetch records no attempt_id; all fetch rows for this feed and run are
shown, and cannot be attributed to a single attempt
```

Showing all candidate rows with a stated limitation is correct. Guessing by
position would be a heuristic presented as evidence, which is precisely what
an audit command must not do.

#### The no-result-row case

Where no result row exists — a failed attempt, or the documented case in
which the engine settles a non-settling handler outcome `failed_permanent`
and discards the untrusted payload — the output says so explicitly:

```text
no persisted result row; the attempt is the only durable evidence of this call
```

That is a real, documented state. Naming it is more useful than printing an
empty section.

### K10 — Registry completeness is a tested invariant

A test enumerates every handler registered with the run engine and asserts
that each one's `task_type` has a registry entry. A future milestone that
adds a work kind without an audit mapping fails that test rather than
silently producing a blank `--attempt` view.

**The registry covers all twelve registered handlers, including local ones.**
`aggregate_person_lead` is registered with `LOCAL_PROVIDER` and operation
`aggregate` and makes no external call, so it creates no `attempt` row and
can never be the subject of `--attempt`. It is still in the registry, marked
`external: no`, for two reasons: the completeness test can then enumerate the
handler set without a hand-maintained exclusion list, and `audit run`'s work
outcomes section (K7 section 4) can name the result table for a local task
type just as it does for an external one.

A registry entry marked `external: no` asserts that no attempt row exists
for that task type. If `--attempt` is somehow given an attempt whose work
item has a non-external task type, that is a data inconsistency and is
reported as one, not rendered as a result.

This is the milestone's most important structural test: it is the only
mechanism preventing audit coverage from decaying as the system grows.

### K11 — `audit person` prints every section in full, with no flags

Audit of a single person is a deliberate forensic command, run because
something looks wrong. A truncated default view is precisely the case in
which the cause would be missed. Terminal scrollback and `| less` already
solve volume, and no flags means no combinatorial output surface to test.

Sections are printed in lifecycle order — the order in which evidence was
acquired — so that reading top to bottom retraces how the system reached
its verdict:

1. **Identity** — id, display name, `identity_fingerprint`, created at and
   by which run, canonical or merged status.
2. **Sourced names** — every `sourced_name` with exact name, search name,
   match key, and source.
3. **Relations** — `person_relation` rows on either side: kind, the other
   person, status, opening and closing runs and observations.
4. **Mentions** — `person_mention` rows joined directly on
   `person_mention.person_id`, each with source item, exact name, mention
   outcome, rationale, and the `semantic_outcome` of the resolution named by
   `person_mention.current_entity_resolution_observation_id`.

   Both columns are added to `person_mention` by `ALTER TABLE` in migration
   `0005_people_identity.sql`, not by the `CREATE TABLE` in `0004`, and
   `person_mention_by_person` indexes the first. The direct join is the
   correct primary path: `person_id` is the durable association and the
   pointer is, per 0005's own comment, "the only mutable entity-resolution
   state on a mention." Reaching mentions instead through
   `entity_resolution_observation.selected_person_id` and `created_person_id`
   would reconstruct that association from history and would misreport any
   mention whose current association differs from an individual historical
   observation.

   Historical resolutions are still shown — that is section 5's job. Section
   4 reports the current association; section 5 reports how it was reached,
   including superseded observations.
5. **Entity resolution** — each observation: disposition, semantic outcome,
   candidates considered, selected or created person, `prompt_hash`,
   `schema_version`, `task_fingerprint`, rationale.
6. **Wikipedia identity** — plans, search observations, page-fact batches,
   and identity observations with outcome; the row matching
   `person.current_wikipedia_identity_observation_id` marked current.
7. **Coverage evidence** — `person_coverage_plan` and its query forms,
   Brave observations, screening decisions with rule and source-policy
   fingerprint, and selected article targets.
8. **Article assessments** — each `person_article_assessment` with
   same-person, coverage-depth, content-type, and subject-relationship
   judgments, its `article_assessment_signal` rows by `signal_kind`, and
   cited passage IDs.
9. **Lead history** — every `lead_assessment` in id order: outcome,
   `qualifying_domain_count`, `incompleteness_reason`, ordering factors,
   `lead_policy_fingerprint`, `material_fingerprint`. The row matching
   `person.current_lead_assessment_id` is marked current.
10. **Queue history** — the `digest_queue` row (status, tier,
    `eligibility_reason`, `first_pending_at`, `last_material_change_at`,
    `removed_reason`) and every `queue_transition` in id order.
11. **Digest history** — every `digest_entry` for this person: digest, run,
    ordinal, and the lead assessment that surfaced them.

A person with no rows in a section gets an explicit empty marker for that
section, not a silently omitted heading.

### K12 — A merged-away person is audited, not redirected

Auditing a merged-away id prints a prominent banner naming the survivor and
the merging run, then this person's own recorded history.

A merge is exactly the situation an operator runs an audit to understand.
Redirecting to the survivor would hide the evidence being sought. The
survivor's audit is one command away and is named in the banner.

### K13 — Exit statuses follow the approved table; usage errors are `64`

Per the operator-experience design: read-only commands return `1` when a
requested run or person does not exist. A non-integer or malformed argument
is a usage error, status `64`. Success is `0`. These commands never return
`2`, which is reserved for a `partial` run.

### K14 — Redaction is proven by a positive control

Audit deliberately shows detailed persisted evidence, which makes it the
highest-risk surface in the application for secret leakage. Three defences
apply:

- all output passes through the existing `obs/` redaction helpers;
- the configuration snapshot is printed from `canonical_json`, which the
  foundation invariant already guarantees contains no secrets;
- that guarantee becomes a tested claim rather than an assumption.

The test constructs a configuration with `BRAVE_API_KEY` and
`OPENROUTER_API_KEY` set to distinctive sentinel values, runs `audit run`,
and asserts neither sentinel appears in stdout or stderr. It is a
**positive control**: it also asserts the sentinels are readable from the
environment in the same test, so `assert sentinel not in output` cannot
pass merely because the value was never reachable. That is the exact
failure mode `CLAUDE.md`'s Test Evidence section warns about, and a
redaction test is the worst possible place to repeat it.

## Amendments to Approved Specifications

### The `--attempt` "raw response" clause

The operator-experience design states that `notable audit run --attempt`
shows "the exact persisted provider or model request evidence, raw
response, validated result, retry history, usage, and version provenance."

Raw request and response bodies are not persisted anywhere in the current
schema. `attempt` records `request_fingerprint`, outcome,
`failure_category`, `provider_status`, `latency_ms`, `response_bytes`,
`destination_host`, cost, `provider_request_id`, and a `detail_json` column
used by exactly one call site. Version provenance exists, but on the domain
observation tables, not on `attempt`.

**This design amends that clause.** `--attempt` shows the request
fingerprint, outcome, retry history, usage, cost, version provenance, and
the persisted **validated** result. It does not show raw bodies.

Adding raw payload persistence was considered and rejected for this
milestone. It would require a new migration, redaction, size caps, and a
retention policy; it would turn a read-only milestone into a write-path
change touching every provider adapter; it would create a secrets-at-rest
surface the foundation invariants currently forbid; and it would leave
audits of every already-completed run empty, which is the case that exists
today. If replay-grade evidence is later required, it is its own milestone
with its own design.

This amendment is a recorded decision, not an oversight.

## Non-Goals

Recorded explicitly so that a later agent does not mistake any of these for
a regression introduced by this milestone:

- `compose_lead_summary` synthesis and its Promptfoo suite (6b-ii).
- Source reconnaissance for unclassified publishers (6b-ii).
- Raw provider payload persistence.
- `notable status` budget figures and deferral-reason breakdown. Milestone
  6a delivered `status`'s backlog, tier split, and oldest-pending lines, so
  the queue-reporting gap is closed; what remains is that `status` still
  prints bare pending and deferred counts and cannot explain *why* work was
  deferred. `audit run` answers that question per-run (K7 section 4), but
  it does not change `status`, which stays as 6a left it.
- JSON output, a `--format` flag, pagination, or any flag beyond
  `--attempt`. A future integration receives an intentional export or
  adapter rather than a scrape of terminal formatting.
- Any change to `reporting/digest.py` or to milestone 6a's write path.

## Testing Strategy

Tests land in `tests/audit/` and the directory is added to
`[tool.pyright] include` in the same commit that creates it. Leaving a new
test package out of the type-checked set is how commit `f2e9e76` shipped a
type violation the project's own gate could not see. The lead-aggregation
design's own K12 (a different decision from this document's K12) closed
that gap for `tests/leads`; this milestone does not reopen it.

| File | Covers |
| --- | --- |
| `test_digest_show.py` | Resolution with and without `RUN_ID`, latest default, hash match, hash mismatch with empty stdout, missing file, missing run, run with no digest, pre-0008 fallback |
| `test_audit_run.py` | Each K7 section, `--attempt`, cross-run attempt rejection (K8), the no-result-row wording (K9), budget divergence display |
| `test_audit_person.py` | Each K11 section, empty-section markers, merged-away banner (K12), unknown id |
| `test_registry.py` | K10 completeness against the engine's twelve registered handlers; that the key is `task_type`; that the five task types sharing `(openrouter, generate_structured)` resolve to their own distinct result tables; that `aggregate_person_lead` is present and marked `external: no` |
| `test_render.py` | Pure rendering on constructed view models, no database |
| `test_seams.py` | CLI registration, exit statuses (K13), no mutation lock taken (K2), the K1 layering assertion, the K14 redaction positive control |

### Mutation evidence

Per `CLAUDE.md`, tests-first ordering is not evidence that a test
discriminates. Before this milestone is reported complete, each named rule
below is mutated in source, the specific named test that fails is recorded,
and the source is restored from a `cp` backup verified with `diff` — never
`git stash`, whose stack is shared across worktrees. Mutations run with
`PYTHONDONTWRITEBYTECODE=1` and `__pycache__` cleared between iterations.

Rules requiring mutation evidence:

- K4 hash verification, and specifically the empty-stdout-on-mismatch rule.
- K5 no re-rendering (layering).
- K8 cross-run attempt rejection.
- K9 the no-result-row branch.
- K9 the registry key. Re-key the registry on `(provider, operation)` and
  confirm a named test fails on the five colliding OpenRouter task types. A
  registry that still resolves correctly under that mutation is not testing
  the disambiguation this decision exists for.
- K9 the `fetch_feed` all-rows-with-caveat behaviour.
- K10 registry completeness, mutated by deleting one entry **and**
  separately by adding a thirteenth handler with no entry.
- K11 the mention join. Re-derive mentions from
  `entity_resolution_observation.selected_person_id` instead of
  `person_mention.person_id` and confirm a named test fails on a person
  whose current association differs from a historical observation. This
  needs a fixture that actually exercises the divergence — a person whose
  mention was later re-associated — or the mutation will survive.
- K12 the merged-away banner.
- K3 the schema-presence guard.
- K13 each distinct exit status.
- K14 redaction, including its positive control.

## Completion Gate

```text
uv run pytest tests/foundation tests/run_engine tests/ingestion \
  tests/people tests/wikipedia tests/coverage tests/leads tests/audit
uv run ruff check .
uv run ruff format .
uv run pyright
```

All must pass, `git diff --check` and `git status --short` must be clean,
and the mutation evidence above must be recorded.

## Open Questions

None. Every decision required to write the implementation plan is fixed by
`K1`–`K14` and the recorded amendment.
