# Milestone 6a: Lead Aggregation and Digest Queue

| Field | Value |
| --- | --- |
| **Status** | Draft (revision 1) |
| **Date** | 2026-08-01 |
| **Author** | (design agent) |
| **Branch** | `feat/lead-aggregation` (proposed) |
| **Programme** | Notable Person Finder clean-slate rewrite |
| **Depends on** | Milestones 1–5 complete on `refactor/rearchitecture` (HEAD `3b1af22`) |

## Overview

Milestone 5 leaves every eligible person with immutable, typed
person–article coverage assessments, but nothing yet converts that evidence
into a verdict. Milestone 6a turns accumulated evidence into a transparent,
deterministic **lead**: an outcome (`promising_lead` / `possible_lead` /
`insufficient_evidence` / `assessment_incomplete`), a durable digest queue
that tracks what should surface next, deterministic ranking, and a real
digest shortlist section — replacing today's hardcoded placeholder.

This milestone owns:

- the `leads/` capability package (aggregation, ranking, queue lifecycle,
  repository SQL, merge/seed hooks);
- the `aggregate_person_lead` work-item kind;
- five new tables: `lead_assessment`, `digest_queue`, `queue_transition`,
  `digest`, `digest_entry`;
- the digest's real shortlist section and queue-flow run-summary block;
- `notable status`'s backlog, oldest-pending-candidate, and queue-tier catch
  up (a gap carried since milestone 3).

It reuses the run engine's prepare/execute/settle handler contract, the
same-run scheduling-hook pattern established (and, this session, repaired)
by milestone 4's K5 Wikipedia-to-coverage handoff, and milestone 5's
versioned, fingerprinted source policy.

It does **not** run any model call, optional synthesis, `notable digest
show`, `notable audit run`, or `notable audit person`. There is no
candidate-level notability model call in this milestone — every outcome is
deterministic code aggregating already-persisted, model-produced per-article
judgments. Synthesis and the audit commands are milestone 6b.

## Background & Motivation

### Current state (end of milestone 5)

- Pipeline: feeds → detect → resolve people → Wikipedia identity → coverage
  evidence (Brave search, fetch, `assess_article`).
- `person_article_assessment` holds immutable same-person/coverage-depth/
  content-type/subject-relationship judgments plus `article_assessment_signal`
  rows (attention or caution, via `signal_kind`), each citing supplied
  passage IDs and a screening-rule/source-policy fingerprint.
- `person.current_wikipedia_identity_observation_id` distinguishes
  `matching_page_found` (stops research) from `no_matching_page_found` /
  `uncertain_identity` (coverage-eligible).
- `people/merge.py::reconcile_digest_queue_on_merge` is a named no-op
  (K13 of the durable-person-identity design): "no-op until milestone 6
  creates `digest_queue`."
- `reporting/digest.py` renders a hardcoded shortlist placeholder ("No
  candidates met the shortlist criteria in this window.") unconditionally.
- `notable status` has no backlog, oldest-pending, or queue-tier reporting.
- Migrations run through `0007_coverage_evidence.sql`; next is `0008_...`.

### Pain points this milestone removes

- A person with strong, multi-domain coverage has no recorded verdict and
  never surfaces to the operator.
- There is no durable notion of "this person is due for review" separate
  from the immutable evidence itself — nothing tracks queue state,
  resurfacing, or backlog pressure.
- The digest cannot show anything beyond operational counts; its entire
  reason for existing (a ranked, inspectable shortlist) is unimplemented.

### Authorities this design refines

This document refines, without replacing:

- `docs/superpowers/specs/2026-07-24-product-workflow-design.md` — the
  approved candidate lifecycle, lead-policy table, high-recall semantics,
  and scheduling/fairness rules.
- `docs/architecture/legacy-behaviors/05-assessment-and-ranking.md` — the
  approved deterministic outcome table, canonical-publisher-domain rule,
  attention/caution signal vocabulary, and the exact ranking tie-breaker
  order.
- `docs/architecture/legacy-behaviors/06-digest-generation.md` — the
  approved candidate-selection/resurfacing rules, candidate-presentation
  fields, run-summary contents, and queue-flow metrics.
- `docs/superpowers/specs/2026-07-24-domain-persistence-design.md` — the
  sketch of lead-assessment and digest-queue records this design turns into
  concrete DDL.
- `docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` — the
  precedent for a same-run scheduling hook after a settling event (K5), and
  the source-policy fingerprinting this milestone extends.

Any product rule already locked by those documents is cited, not
re-litigated. The K-numbered decisions below cover only what those documents
leave to this milestone's engineering judgment.

## Schema

New migration `0008_lead_aggregation.sql`.

```sql
CREATE TABLE lead_assessment (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'promising_lead', 'possible_lead',
        'insufficient_evidence', 'assessment_incomplete'
    )),
    qualifying_domain_count INTEGER NOT NULL DEFAULT 0,
    incompleteness_reason TEXT,                 -- NULL unless assessment_incomplete
    ordering_factors_json TEXT NOT NULL,         -- ranking snapshot, see Ranking
    lead_policy_fingerprint TEXT NOT NULL,       -- thresholds + source-policy fingerprint
    decided_at TEXT NOT NULL
);
CREATE INDEX ix_lead_assessment_person ON lead_assessment(person_id, id);

CREATE TABLE lead_assessment_qualifying_article (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    person_article_assessment_id INTEGER NOT NULL
        REFERENCES person_article_assessment(id),
    canonical_domain TEXT NOT NULL,
    PRIMARY KEY (lead_assessment_id, person_article_assessment_id)
);

CREATE TABLE lead_assessment_signal (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    article_assessment_signal_id INTEGER NOT NULL
        REFERENCES article_assessment_signal(id),
    PRIMARY KEY (lead_assessment_id, article_assessment_signal_id)
);

ALTER TABLE person ADD COLUMN current_lead_assessment_id INTEGER
    REFERENCES lead_assessment(id);

CREATE TABLE digest_queue (
    person_id INTEGER PRIMARY KEY REFERENCES person(id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'emitted', 'removed')),
    tier TEXT NOT NULL CHECK (tier IN ('promising_lead', 'possible_lead')),
    eligibility_reason TEXT NOT NULL CHECK (eligibility_reason IN (
        'new', 'promoted', 'strengthened', 'reminder'
    )),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    first_pending_at TEXT NOT NULL,
    last_material_change_at TEXT NOT NULL,
    removed_reason TEXT                          -- e.g. 'matching_page_found'
);

CREATE TABLE queue_transition (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    from_status TEXT,                            -- NULL on first arrival
    to_status TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX ix_queue_transition_run ON queue_transition(run_id, id);

CREATE TABLE digest (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    file_path TEXT NOT NULL,
    timezone TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    run_state TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE digest_entry (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NOT NULL REFERENCES digest(id),
    person_id INTEGER NOT NULL REFERENCES person(id),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    queue_transition_id INTEGER NOT NULL REFERENCES queue_transition(id),
    ordinal INTEGER NOT NULL                     -- rendered position, 1-based
);
CREATE INDEX ix_digest_entry_digest ON digest_entry(digest_id, ordinal);
```

**K1 — Canonical publisher domain aliasing lives in the source policy, not a
new table.** `config/source_policies/*.toml` rules gain an optional
`canonical_domain` key (defaults to the rule's own `host_exact` /
`host_suffix` value when absent). Two rules sharing a `canonical_domain`
count as one publisher for the two-domain threshold. This keeps the
already-versioned, fingerprinted source policy as the single configured
surface, per milestone 5's "tracked product artifact, not per-machine
preference" precedent, rather than introducing a second alias table that
could drift out of fingerprint coverage.

**K2 — `lead_assessment` records the qualifying set, not just a count.**
`lead_assessment_qualifying_article` exists so a `promising_lead` is
auditable (which two articles, which domains) without re-deriving it from
mutable `digest_queue` state. `lead_assessment_signal` does the same for
attention/caution signals that strengthened a `possible_lead` or will feed
6b's synthesis citations.

**K3 — `digest_queue` has no history of its own; `queue_transition` is the
ledger.** `digest_queue` is a mutable projection (one row per person,
upserted), matching the domain-persistence design's "mutable operational
projections are allowed because immutable domain observations preserve what
matters." All arrival/emission/backlog/drain-rate metrics read
`queue_transition`, never `digest_queue` history, because there is none to
read.

## Work Item and Triggering

New work kind: **`aggregate_person_lead`**, subject_kind `person`, priority
**75** (`FETCH_FEED=10 → INSPECT_MODEL=20 → DETECT_PEOPLE=30 →
RESOLVE/RECONSIDER=40 → MEDIAWIKI_HTTP=50 → MATCH_WIKIPEDIA=55 →
BRAVE_SEARCH=60 → FETCH_ARTICLE=65 → ASSESS_ARTICLE=70`, per the shipped
`*_PRIORITY` constants — `aggregate_person_lead` is the last required-work
tier, after every input it aggregates over has had a chance to settle).

Per this session's confirmed choice, aggregation is scheduled **synchronously
in the same run**, mirroring the K5 same-run pattern — and its recently
fixed defect is exactly the shape of bug this design must not repeat.
Three settlement points call a shared
`_schedule_lead_aggregation_after_settled(connection, person_id=, run_id=,
config=, now=)` hook:

1. `coverage/service.py`'s plan-completion path, when a coverage plan
   reaches `completed` (all required `assess_article` work settled) or a
   terminal incomplete state (K13 of the coverage-evidence design: required
   work `failed_permanent` or truncated search).
2. `wikipedia/service.py`'s `_schedule_coverage_after_wikipedia_settled`,
   when the outcome is `matching_page_found` — this both supersedes open
   coverage work (existing K5 behavior) **and** now also schedules one
   aggregation pass so a person who already had qualifying evidence before
   the match is confirmed still receives a closing `lead_assessment`, and
   `digest_queue` gets a `removed` transition with reason
   `matching_page_found` rather than being silently abandoned mid-queue.
3. `people/merge.py`'s confirmed-merge path, replacing the
   `reconcile_digest_queue_on_merge` no-op (see Merge Reconciliation below).

**K4 — the hook resolves `config.source_policy_file` through the caller's
already-loaded `MainConfig`, never re-reads it.** This session's fix made
`load_config` resolve that path onto `MainConfig` itself; this design must
not reintroduce a second raw `load_source_policy(config.source_policy_file)`
call site with its own silent-failure mode. The lead-aggregation handler
receives the source policy as an explicit parameter from the same caller
that already loaded it for coverage screening, not by loading it again.

**K5 — the aggregation handler makes no external call.** It is a
`prepare`/`execute`/`settle` handler like every other work kind for
scheduling uniformity, but `execute` performs pure computation over
already-persisted rows (no attempt row, no provider, `provider='local'`
convention consistent with 3b2's empty-candidate create). This keeps the
"exactly one external call per execute" invariant vacuously true rather than
inventing an exception to it.

**K6 — fingerprint scope.** The work item's material fingerprint is the
canonical JSON of: the set of `person_article_assessment` IDs currently
attached to the person, the set of attention/caution signal IDs, the current
Wikipedia identity outcome, the lead-policy config (thresholds), and the
source-policy fingerprint. An unchanged fingerprint means no repeat
aggregation — satisfying the product workflow's "policy changes apply
prospectively" rule and preventing redundant work when a settlement hook
fires again for unrelated reasons (e.g. a second coverage plan for the same
person after a reminder-driven refresh, once 6b exists).

## Aggregation Algorithm

Deterministic, pure function `aggregate_lead(person_article_assessments,
signals, wikipedia_outcome, policy) -> LeadOutcome`, unit-testable without a
database.

Applies the approved decision table from capability 5 directly:

| Outcome | Condition |
| --- | --- |
| `promising_lead` | Qualifying articles (same-person, significant-or-greater depth, curated-eligible canonical domain, editorially independent, qualifying content type) exist from at least `promising_domain_threshold` (default 2) distinct canonical domains. |
| `possible_lead` | Threshold not met, but at least one useful reason exists: one qualifying domain; a plausibly-qualifying unresolved article; significant coverage from an unclassified publisher; a substantial non-qualifying supporting source; or a grounded transferable attention signal. |
| `insufficient_evidence` | Assessment completed (coverage plan terminal, non-incomplete) and every result is clearly non-qualifying with nothing useful. |
| `assessment_incomplete` | Required work failed, was truncated, or was not performed, and no positive outcome is supportable from evidence already collected. |

**K7 — a positive outcome is never downgraded by an unrelated later
failure.** Per the spec's "later query, retrieval, or optional assessment
failures remain visible caveats but do not erase the positive lead," the
algorithm evaluates qualifying evidence first; only when nothing qualifies
does it check for incompleteness to distinguish `insufficient_evidence` from
`assessment_incomplete`.

**K8 — unresolved Wikipedia identity is a caveat, never itself a
`possible_lead` reason.** The algorithm reads `wikipedia_outcome` only to
record it as provenance/caveat on the `lead_assessment`; it does not appear
in the outcome branches above, matching capability 5's explicit rule.

**K9 — qualifying content types and editorial-independence checks reuse
milestone 5's `person_article_assessment` fields directly** (`content_types_json`,
`subject_relationship`, `coverage_depth`) — this milestone adds no new
article-level classification, only the person-level aggregation over
classifications milestone 5 already produces.

## Ranking

Pure function `rank_key(queue_entry, lead_assessment) -> tuple`, implementing
capability 5's exact lexicographic order:

```python
def rank_key(entry: QueueEntry, lead: LeadAssessment) -> tuple[object, ...]:
    return (
        _OUTCOME_RANK[lead.outcome],                      # promising before possible
        _ELIGIBILITY_RANK[entry.eligibility_reason],       # new/promoted/strengthened before reminder
        _WIKIPEDIA_RANK[lead.wikipedia_outcome],           # no_matching_page_found before uncertain_identity
        -lead.qualifying_domain_count,                     # more domains, tie-break only
        -lead.positive_signal_count,
        _EVIDENCE_VISIBILITY_RANK[lead.best_evidence_visibility],  # full > partial > snippet
        -_freshness_days(lead.freshest_qualifying_article_at),
        entry.person_id,                                   # final deterministic tie-break
    )
```

`ordering_factors_json` on `lead_assessment` stores the concrete inputs to
this tuple (not the tuple itself) so a later ranking-formula change can
re-rank without re-running aggregation, and so each digest entry can display
which factors applied (capability 5's "each candidate records the ordering
factors that applied").

## Digest Queue Lifecycle

On each aggregation:

1. Compute the new outcome and, if `promising_lead` or `possible_lead`,
   determine whether this is `new` (no current `digest_queue` row),
   `promoted` (possible → promising), `strengthened` (new qualifying domain,
   `uncertain_identity` → `no_matching_page_found`, or a new positive-signal
   category — the exact list from capability 6), or ineligible for
   resurfacing (repeated evidence from an already-counted domain).
2. If eligible: upsert `digest_queue` (`status='pending'`, tier, reason,
   `last_material_change_at = now`; `first_pending_at` only set if the row
   is new), insert a `queue_transition`.
3. If the outcome is `insufficient_evidence` or `assessment_incomplete` and
   a `digest_queue` row exists: leave it as-is (a dormant person with a
   still-pending prior positive lead is not retracted by a later neutral
   result — only new evidence changes an active queue entry).
4. If the settling event is `matching_page_found` and a `digest_queue` row
   exists: set `status='removed'`, `removed_reason='matching_page_found'`,
   insert a `queue_transition`.

**K10 — reminders are represented but disabled by default.**
`reminder_interval_days = 0` means the reminder resurfacing path never
fires; the schema and queue-transition machinery for it exist now (so 6b or
a later config change is additive, not a migration) rather than being
retrofitted.

**K11 — the starvation guard is a ranking-time adjustment, not a queue-state
change.** A `pending` entry older than `starvation_days` in its tier is
sorted ahead of newer same-tier entries at render time (an extra leading
sort key: `entry.first_pending_at < starvation_cutoff`); it does not change
`digest_queue.status` or `eligibility_reason`. This keeps "why is this
person showing today" answerable from `queue_transition` alone.

## Digest Rendering and `notable status`

**Shortlist section.** Replace the hardcoded placeholder in
`reporting/digest.py`. Rank all `status='pending'` entries (with the
starvation adjustment), take the configured `digest_limit` (default 10),
and for each: insert a `digest_entry`, transition `pending → emitted` in
`queue_transition`, and render per capability 6's presentation contract —
outcome, why-shown reason, Wikipedia state, qualifying sources (publisher,
title, date, URL, content type, depth, evidence visibility), attention/
caution signals, and unresolved issues. Milestone 6b's synthesis is not
present; render the deterministic fallback text capability 6 requires
("deterministic code builds a useful fallback from the outcome, article
titles, attention signals, and caveats") so the section is genuinely useful
standalone, not a placeholder for a later milestone.

**Run summary additions:** `promising_lead` / `possible_lead` /
`insufficient_evidence` / `assessment_incomplete` counts this run;
candidates emitted / omitted-by-limit / still-queued; and the queue-flow
block — candidates newly queued, emitted, removed
(`matching_page_found`), ending backlog by tier, rolling 7-day/30-day
arrival and emission rates, net queue growth, oldest-pending age, estimated
clear-time or explicit "not clearing" / "insufficient history" text (never a
fabricated estimate, per capability 6).

**`notable status`:** add digest backlog by tier, oldest pending candidate
age, and saturation days — closing the gap CLAUDE.md has carried since
milestone 3b1 ("no digest backlog, no oldest pending candidate, no queue
tiers").

## Merge Reconciliation

Replace `people/merge.py`'s `reconcile_digest_queue_on_merge` no-op:

- If only the loser has a `digest_queue` row: move it to the survivor
  (update `person_id`), inserting a `queue_transition` noting the merge.
- If both have rows: keep the higher-priority one by `rank_key`
  (deterministic; no "more evidence" heuristic, matching K7 of the
  durable-person-identity design's survivor rule), transition the other to
  `removed` with reason `merged_away`.
- `lead_assessment` rows are immutable history and are **not** rewritten
  onto the survivor's `person_id` — same non-rewrite-historical-FKs
  invariant as K12 of the durable-person-identity design.
  `person.current_lead_assessment_id` on the survivor is recomputed by one
  fresh aggregation pass over the merged evidence (reusing the same handler
  the settlement hooks call), not by picking one side's stale pointer.

## Configuration Surface

New `[tasks.aggregate_lead]` section in `MainConfig`, following the existing
per-task config pattern (`config/models.py`):

```toml
[tasks.aggregate_lead]
promising_domain_threshold = 2
digest_limit = 10
starvation_days = 14
reminder_interval_days = 0   # 0 disables time-based reminders
```

No secrets, no model — this task makes no provider call.

## Files

- `src/notable_person_finder/leads/`
  - `aggregation.py` — `aggregate_lead`, the decision-table implementation
    (K7, K8, K9).
  - `ranking.py` — `rank_key` and the starvation adjustment (K11).
  - `queue.py` — resurfacing-eligibility rules and `digest_queue` /
    `queue_transition` transitions (K3, K10).
  - `service.py` — the `aggregate_person_lead` `prepare`/`execute`/`settle`
    handler (K5, K6) and `_schedule_lead_aggregation_after_settled`.
  - `repository.py` — SQL for all five new tables.
  - `merge_hooks.py` — replaces `reconcile_digest_queue_on_merge`.
  - `seed.py` — top-of-run sweep for any person whose settlement hook was
    missed by a crash window (same at-least-once posture as every other
    milestone; see `docs/architecture/at-least-once-execution.md`).
- `src/notable_person_finder/reporting/digest.py` — real shortlist section,
  queue-flow run-summary block.
- `src/notable_person_finder/cli/main.py` — registers `aggregate_person_lead`,
  wires `notable status`'s new lines.
- `src/notable_person_finder/coverage/service.py`,
  `src/notable_person_finder/wikipedia/service.py`,
  `src/notable_person_finder/people/merge.py` — call the new settlement
  hook (K4).
- `db/migrations/0008_lead_aggregation.sql`.
- `tests/leads/` — new test package (see Testing Strategy).

## Testing Strategy

Offline, deterministic; no Promptfoo (no model call in 6a):

- **Decision table:** every branch of `aggregate_lead` — 2-domain
  promising, 1-domain possible, unclassified-significant possible,
  completed-nothing-qualifies insufficient, missing-required-work
  incomplete, and the K7 positive-outcome-survives-later-failure case.
- **Canonical domain aliasing:** two source-policy rules sharing
  `canonical_domain` count as one; two without it count as two.
- **Ranking:** every tie-breaker in `rank_key` in isolation, including the
  starvation adjustment under a controlled clock.
- **Resurfacing:** each triggering condition (promotion, new domain,
  identity improvement, new signal category) and the non-triggering
  repeated-domain case, as a positive/negative control pair.
- **Queue persistence:** pending across runs, digest-limit omission
  retains eligibility, starvation reordering, Wikipedia-match removal.
- **Same-run firing:** an end-to-end CLI test proving aggregation fires in
  the same run coverage/Wikipedia settles — the direct regression test for
  the class of bug this session just fixed in K5, applied to this
  milestone's own hook.
- **Merge reconciliation:** survivor keeps higher-`rank_key` queue row,
  loser's transitions to `removed`, `lead_assessment` history untouched on
  both sides, fresh aggregation recomputes the survivor's current pointer.
- **Digest/status rendering:** distinct per-counter fixture values (the
  milestone-5 `test_digest_status.py` pattern that catches swapped-field
  defects), queue-flow metrics under a controlled clock including the
  "insufficient history" and "not clearing" fallback text paths.
- **Fingerprint reuse:** an unchanged evidence/policy fingerprint does not
  re-aggregate; a changed one does.

**Completion gate:**
`uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads`,
plus `uv run ruff check .`, `uv run ruff format .`, and `uv run pyright`.

**K12 — extend `pyproject.toml`'s `[tool.pyright] include` list with
`tests/leads` (and, opportunistically, the pre-existing `tests/wikipedia`
and `tests/coverage` gap this session's review surfaced).** Leaving new test
directories out of the type-checked set is exactly how `f2e9e76` shipped a
real type violation the project's own gate could not see; this milestone
should not repeat that gap in its own new test package, and closing the
pre-existing one is a one-line, in-scope fix while touching this file.

## Deferred to Milestone 6b

- The `compose_lead_summary` optional model synthesis (already fully
  specced by the LLM-evaluation design's Task 5) and its Promptfoo suite.
- Source reconnaissance for unclassified publishers attached to a possible
  lead.
- `notable digest show`, `notable audit run`, `notable audit person`.
- Time-based reminders becoming operator-configurable in practice (the
  mechanism ships now per K10; turning it on by default, if ever, is a
  product decision outside this milestone).
