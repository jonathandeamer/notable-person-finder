# Phase 4: Ranking and Full Digest

**Status:** Approved
**Date:** 2026-08-04
**Scope:** implementation and automated tests only
**Authority:** subordinate to
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. Phase 3's
coverage contract and implementation are inputs to this phase:
`docs/superpowers/specs/2026-08-04-coverage-research-design.md`.

## Goal

Complete the MVP loop by consuming Phase 3 article assessments, assigning a
deterministic lead outcome, selecting a ranked shortlist, and rendering the
full human-readable digest. This phase does not include a live provider run or
promotion of a new cache fixture.

## Data flow and module boundaries

The existing sequential pipeline is preserved:

```text
feed item
  → detect.people_in
  → wiki.match
  → coverage.research
  → rank.assess
  → per-item lead buffer
  → rank.shortlist
  → digest.write
  → store.commit
  → store.log
```

Responsibilities remain narrow:

- `rank.py` owns lead construction, qualifying-article predicates, outcome
  assignment, rank keys, duplicate collapse, suppression filtering, and the
  shortlist cut.
- `digest.py` owns Markdown formatting and atomic file writes. It receives
  already-ranked records and makes no policy decisions.
- `pipeline.py` owns sequencing and error boundaries. It discards all buffered
  leads for an item when that item raises `Incomplete`, catches
  `BudgetExceeded` at the run boundary, and passes settled leads to selection.
- `store.py` receives surfaced identity keys for state and serialized full
  leads for append-only logs. The application never branches on log rows.
- `coverage.py`, `wiki.py`, and their contracts remain behaviorally unchanged;
  only typing adjustments needed to consume their existing outputs are in
  scope.

`rank.assess` is pure: it receives one mention, its Wikipedia verdict, article
assessments, and ranking configuration, and returns one complete lead record.
`rank.shortlist` performs selection using a read-only suppression query and
returns both the exact rendered records and the identity keys to mark
surfaced.

## Lead assessment and outcome rules

`rank.assess` produces one immutable `Lead` for every research-worthy mention
that reaches a terminal coverage result.

An article qualifies only when all of these hold:

- `person_relation == "same_person"`;
- `coverage_depth == "significant"`;
- `screening_status == "curated_eligible"`;
- `subject_relationship == "editorially_independent"`;
- none of its content types is `listing`, `announcement`, `press_release`, or
  `sponsored`.

Outcome assignment is deterministic:

- `promising_lead`: qualifying articles span at least
  `promising_domain_threshold` distinct `policy.canonical_domain(...)`
  values;
- `possible_lead`: below that threshold, but at least one of these holds:
  - there is a qualifying article;
  - a same-person article has significant depth from an unclassified
    publisher;
  - a same-person article has significant or passing depth and content that
    is qualifying, but its publisher status or relationship prevents full
    qualification;
- `insufficient_evidence`: none of the above holds.

An empty article tuple is a valid terminal result and therefore produces
`insufficient_evidence`. Technical failures have already become `Incomplete`
and never reach ranking.

The immutable `Lead` retains the identity key, display name, source item URL
and publisher label, Wikipedia verdict, outcome, qualifying-domain count and
values, all article assessments, a deterministic explanation, and the
computed rank tuple. Full assessment detail is retained for `detail_json`,
including fields that current outcome and ranking rules do not consume.

The rank tuple is lexicographic and ascending:

```python
(
    outcome_rank,              # promising before possible
    wikipedia_rank,            # no page before uncertain
    -qualifying_domain_count,  # more qualifying domains first
    identity_key,              # stable final tie-breaker
)
```

`insufficient_evidence` has no rank entry because it is filtered before
ranking.

## Shortlist selection, digest, and persistence

Selection occurs in this fixed order:

1. Remove `insufficient_evidence` leads.
2. Suppress identities surfaced within `resurface_after_days`.
3. Collapse remaining equal `identity_key` values to the highest-ranked
   representative.
4. Attach a namesake note to a representative when other leads share its key,
   including their source URLs.
5. Sort representatives by the rank tuple.
6. Cut to `digest_size`.

Only records surviving the cut are marked surfaced. This prevents duplicate
mentions from consuming the digest cut and prevents suppressed entries from
displacing eligible people.

The digest remains a dated Markdown artifact plus `latest.md`, written by
atomic replacement. It retains run status, settled and incomplete item
counts, model spend, and one section per surfaced lead containing the outcome,
source item and coverage links, qualifying-domain evidence, concise rationale,
and any shared-name note. Model-provided text is whitespace-normalized so it
cannot inject headings or additional Markdown structure.

Persistence uses the existing four-table model:

- `store.commit(settled, incomplete, surfaced_keys)` updates item lifecycle
  and only identities actually rendered;
- `store.log(...)` records every terminal lead, including leads filtered from
  the digest, with outcome, rank tuple, and complete detail JSON;
- log failure is warned and cannot roll back state committed after the digest
  lands.

Durable ordering remains:

```text
write dated digest + latest.md
→ commit item/surfaced state
→ best-effort append run/lead logs
```

## Error handling and automated tests

Existing failure semantics are preserved:

- `Incomplete` from coverage or Wikipedia abandons the current item's entire
  in-memory lead buffer and leaves the item retryable under the existing cap;
- `BudgetExceeded` stops the outer pass, renders completed leads, and marks the
  run partial;
- empty coverage is terminal `insufficient_evidence`, not a technical failure;
- no retry, concurrency, queue, or durable intermediate state is added.

Automated tests cover:

- the qualifying-article predicate and all three outcomes;
- canonical-domain deduplication and configurable promising threshold;
- rank ordering, Wikipedia uncertainty, and identity tie-breaking;
- insufficient-evidence filtering;
- suppression inside and outside the resurface window;
- same-name collapse and its rendered note;
- collapse-before-cut behavior;
- digest rendering and Markdown flattening;
- only rendered identities passed to `store.commit`;
- full leads logged even when not shortlisted;
- incomplete-item buffering and budget-capped partial output;
- an end-to-end fixture run from Phase 3 results through the final digest;
- regression coverage ensuring Phase 1–3 provider and cache behavior remains
  unchanged.

The automated completion gate is:

```text
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/mvp
```

Live provider execution and promotion of a new cache fixture are explicitly
outside this Phase 4 scope.

## Out of scope

This phase does not add durable person identity, cross-mention evidence
accumulation, a backlog or digest queue, new model tasks, provider changes,
live-run validation, cache-fixture promotion, concurrency, or automatic
Wikipedia edits.
