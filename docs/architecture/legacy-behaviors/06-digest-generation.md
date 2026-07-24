# Legacy Behavior Review: Daily Digest Generation

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability turns ranked lead assessments and run health into a concise
daily artifact for one human editor. SQLite remains the structured source of
truth. The digest is a view of current work, not a transport format, state
store, notification protocol, or notability verdict.

Primary legacy evidence:

- `scripts/det_openclaw_daily_digest.py`
- `scripts/daily_notability_digest_report.py`
- final-summary behavior in `run_pipeline.py`
- digest assertions in `tests/test_smoke_pipeline.py`
- `docs/running.md`, `docs/data-flow.md`, and `README.md` descriptions of the
  prototype output

## Output Contract

Delete the OpenClaw-specific JSON contract. Each run that reaches reporting
produces:

- an immutable human-readable Markdown digest at a path such as
  `output/digests/YYYY-MM-DD-{run_id}.md`;
- the same concise content on standard output; and
- an optional `output/digests/latest.md` convenience copy representing the
  latest attempt.

The exact path is configurable. There is no JSONL join, separate digest
subprocess, 24-hour reconstruction from files, or second report-generation
script. A future notification adapter can consume an application service or
explicit export without defining the core digest schema.

All stored timestamps use UTC. Human-facing dates use one configured IANA
timezone, defaulting to the machine's local timezone. The digest header states
the timezone and exact observation window.

## Candidate Selection

The digest shortlist contains only `promising_lead` and `possible_lead`
candidates that are:

- newly eligible and not previously shown;
- materially strengthened since their last appearance; or
- due under an explicitly configured reminder policy.

Time-based reminders are disabled by default. Material improvement means at
least one of:

- promotion from `possible_lead` to `promising_lead`;
- a new qualifying canonical publisher domain;
- improvement from `uncertain_identity` to `no_matching_page_found`;
- a new category of grounded positive attention signal; or
- previously partial evidence becoming qualifying after fuller retrieval.

Another article from an already counted canonical publisher domain does not
automatically cause resurfacing. A later matching English Wikipedia page stops
future surfacing, as defined in capability 5.

Apply the deterministic ranking from capability 5, then the configurable
digest-size limit. Roughly ten candidates is the initial operating preference,
not a fixed product rule.

Candidates omitted by the limit remain `pending_for_digest` until actually
shown. They do not lose eligibility at the end of the run. A configurable
starvation guard moves a candidate ahead of newer candidates in the same lead
tier after it has remained pending for a configured number of days.

## Candidate Presentation

Each candidate entry contains:

- display name and lead outcome;
- why the person appears today: new, promoted, strengthened, or reminder;
- English Wikipedia state and any identity uncertainty;
- the grounded “why review?” synthesis or deterministic fallback;
- positive attention signals and caution signals;
- every qualifying source, including publisher, title, publication date, URL,
  content type, coverage depth, and whether assessment used full text, partial
  text, or snippets;
- a configurable number of clearly labelled provisional or supporting
  sources; and
- unresolved issues and suggested human checks.

Source entries remain traceable to stored evidence identifiers. The renderer,
not the model, constructs Markdown structure and validated links. It does not
emit raw HTML or allow model text to create untrusted links.

Raw prompts and responses, model and provider identifiers, token usage, cost,
retry history, and full decision provenance stay available through a separate
audit command. They do not overwhelm the ordinary digest.

The optional bounded synthesis from capability 5 cannot control inclusion,
ordering, or source selection. If it fails or is not run, deterministic code
builds a useful fallback from the outcome, article titles, attention signals,
and caveats. The candidate remains in the digest and synthesis is marked
unavailable.

## Run Summary

An empty shortlist can be a successful result. Every digest includes a compact
run summary with:

- run identifier, state, start, finish, duration, and observation window;
- number of feed items and people considered;
- matching Wikipedia pages found;
- `promising_lead`, `possible_lead`, `insufficient_evidence`, and
  `assessment_incomplete` counts;
- operational failure and budget-deferred counts;
- candidates emitted, omitted by the limit, and still queued; and
- configured budget plus actual provider cost when available.

Detailed non-shortlist records remain queryable in SQLite rather than being
printed. The failure and budget capability will finalize which health fields
and exit statuses are required.

Generate a digest for complete and partial runs, and whenever possible for a
failed run. Mark the run prominently as `complete`, `partial`, or `failed`.
`latest.md` represents the latest attempt rather than silently retaining stale
successful output. Completed candidate results may appear in a partial digest,
with skipped work and failures made explicit. A failed run returns a non-zero
CLI exit status; the exit behavior for `partial` is deferred to capability 7.

## Queue-Flow Metrics

Persist daily queue events so simple SQL aggregation can show whether editor
attention is keeping up with candidate creation. The digest run summary
reports:

- candidates newly queued;
- candidates emitted;
- candidates removed because a matching Wikipedia page was found;
- ending backlog split by lead tier;
- rolling 7-day and 30-day arrival and emission rates;
- net queue growth;
- age of the oldest pending candidate;
- estimated days to clear a static backlog at the recent emission rate;
- net-clearance status after accounting for the recent arrival rate; and
- number of days the configured digest limit was saturated.

When history is insufficient or the recent emission rate is zero, report that
state instead of fabricating an estimate. When arrivals equal or exceed
emissions, report that the queue is not clearing rather than presenting a
misleading net-clearance date. Show a concise backlog warning when the rolling
arrival rate exceeds the emission rate or the oldest pending item crosses a
configurable age. This requires no external monitoring stack.

## Approved Dispositions

| Observable behavior | Decision and rationale | Replacement verification |
| --- | --- | --- |
| A structured JSON digest is built specifically for OpenClaw. | **Delete.** The initial product is a personal local tool; integration-specific transport must not define its domain. | Absence from application contract |
| A second script refreshes the JSON digest through a subprocess and prints it. | **Delete.** One application service and CLI render Markdown and stdout directly from durable state. | CLI acceptance tests |
| The digest reconstructs a rolling 24-hour view by joining JSON and JSONL files. | **Change.** Query explicit run, entity, assessment, evidence, and queue records in SQLite. | Persistence integration tests |
| Only `LIKELY_NOTABLE` and `POSSIBLY_NOTABLE` rows are shown. | **Change.** Show eligible `promising_lead` and `possible_lead` queue entries under explicit resurfacing rules. | Selection matrix tests |
| People are grouped by case-folded display name. | **Delete.** Use stable person entities so namesakes do not merge. | Namesake digest tests |
| People are sorted alphabetically. | **Delete.** Use the approved deterministic priority order and expose its reasons. | Ranking acceptance tests |
| RSS and Brave URLs are printed as undifferentiated lists. | **Change.** Render typed qualifying and provisional source cards with access and assessment context. | Markdown snapshot tests |
| A fixed observation window determines whether a person can appear. | **Change.** Current run changes and durable pending state determine eligibility; the header still states the run window. | Multi-run queue tests |
| A candidate omitted by output limits disappears after the window closes. | **Delete.** Keep it pending until surfaced, with a simple starvation guard. | Backlog and clock-controlled tests |
| Every new article can repeat an existing candidate. | **Change.** Resurface only on defined material improvement or configured reminder. | Resurfacing decision-table tests |
| The report shows run count and latest run identifier. | **Preserve and expand.** Include concise workflow health, outcome, failure, budget, and queue counts. | Complete, empty, partial, and failed run snapshots |
| A prior successful digest can remain visible when the latest attempt fails. | **Delete.** `latest.md` identifies the latest attempt and its state. | Failure-path filesystem tests |
| Model-generated text is required to produce a useful candidate entry. | **Delete.** Deterministic fallback rendering is always available. | Model-failure snapshot test |
| Queue pressure is invisible. | **Change.** Derive arrival, emission, backlog, age, saturation, and drain estimates from stored queue events. | Clock-controlled metric tests |

## Verification Boundary

Pytest covers selection, resurfacing, pending persistence, starvation,
Wikipedia suppression, configurable limits, queue-flow calculations, UTC and
local-time rendering, complete and partial run summaries, optional synthesis
failure, validated links, deterministic Markdown, stdout parity, and immutable
dated output. Snapshot tests use stable clocks and identifiers. No model call
is required to test digest generation.
