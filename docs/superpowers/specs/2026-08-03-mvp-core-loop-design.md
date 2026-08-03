# MVP Core Loop Design

**Status:** Approved
**Date:** 2026-08-03
**Branch:** `mvp`
**Supersedes for implementation purposes:** the entire
`refactor/rearchitecture` programme (frozen as reference, not deleted)

## Purpose

Rebuild the "find promising leads" loop as a small, maintainable application
that one person can hold in their head. This document is the single
architectural authority for the MVP. There are no companion specs.

## Background: why a second rebuild

Two prior versions exist and neither is the base for this one.

`main` is the original prototype: ~7,000 lines, a `run_pipeline.py` driver
shelling out to fifteen gate scripts over JSONL state. Broken and non-robust in
the ways recorded in its own docs, but comprehensible end to end.

`refactor/rearchitecture` is the clean-slate rewrite: **38,608 lines of source,
71,334 lines of tests, 34,463 lines of documentation, and 45 SQLite tables.**
It works — a live ten-feed run over 246 items cost $1.08 and produced a real
shortlist — but it succumbed to the second-system effect. Its *product* design
is sound and much of it is carried forward here. Its *machinery* is not.

Four drivers produced the bulk, and they compounded:

1. **Full event-sourced durability.** Every provider observation, query form,
   plan, transition, alias, and signal is separately persisted.
2. **A hand-rolled durable workflow engine.** `runs/` implements a work-item
   queue with a four-phase handler contract, provider-split thread pools, a
   central retry coordinator, nano-USD budget reservation, a mutation lock, and
   interrupted-run sweeps — Temporal, in SQLite, for a once-daily personal
   batch job.
3. **Fingerprints as the change-detection primitive.** `material_fingerprint`,
   `request_fingerprint`, `routing_fingerprint`, `lead_policy_fingerprint`, and
   source-policy document hashes each existed to answer "should we redo this
   work?", and each propagated into schema, tests, merge hooks, and sweeps.
4. **Multi-wave state machines per domain.** `wikipedia/service.py` alone runs a
   wave-1 facts pass, a redirect wave, an accent phase, continuation
   resolution, plan superseding, and stalled-plan sweeps.

Together these imposed roughly **five integration points per feature** — a
table, a fingerprint, a scheduling hook, a merge/supersede hook, a sweep, and
audit rendering. That is why `coverage/service.py` reached 4,708 lines, and why
the programme's top open item ("raise `max_candidates` from 8") was blocked:
the value moves match fingerprints, which move tests, which move plan
reconciliation. A tuning knob had become a schema migration.

The MVP targets **under 3,000 lines of source**.

## The central decision

**A hash-keyed response cache replaces the durability machinery.**

Every external call — Brave, MediaWiki, article fetch, and OpenRouter — passes
through one caching wrapper keyed by a hash of the canonical request. A run
that crashes is simply re-run, and every prior call is a cache hit, so the
retry costs approximately nothing.

This single substitution removes the need for work items, retry coordination,
budget reservation, mutation locks, interrupted-run sweeps, superseding, and
per-stage checkpoints. Roughly 60 lines replace roughly 2,800.

It has a second-order benefit that the design leans on deliberately: **a real
run's cache directory is a test fixture set, for free.** Provider-facing tests
replay recorded responses; no hand-written HTTP mocks.

## Product boundary

Unchanged from the prior programme, and still correct.

The product is a personal, non-interactive, once-daily batch tool. It monitors
editor-configured RSS or Atom feeds, finds meaningful individual subjects,
checks English Wikipedia, researches English-language coverage, and produces a
small inspectable Markdown shortlist.

**It never writes Wikipedia content, drafts an article, or decides that a
person satisfies Wikipedia policy.** Every publication action requires explicit
human review. This is a hard invariant, not a milestone boundary.

The visual-arts source profile is pilot configuration, not application logic.
No publisher-specific branches in Python.

## State model: stateless pass with a seen-set

Each run processes new feed items end to end and renders a digest. Durable
state is limited to what stops repeated work and repeated surfacing.

There is **no durable person entity**, no LLM entity resolution, no
reconsideration, no confirmed merges, no `possible_same_person` relation, and
no cross-run evidence accumulation. Within a run, mentions group by normalized
name. Across runs, deduplication is on source item URL.

The consequence is accepted deliberately: a person mentioned in two articles a
week apart is researched twice. At roughly $1 per run this is cheaper than the
~8,000 lines of identity machinery it replaces.

**Durable person identity is the intended first post-MVP feature.** It is
deferred, not rejected. The prior programme's
`2026-07-30-durable-person-identity-design.md` on `refactor/rearchitecture`
remains the best available thinking on it and should be revisited — critically,
and at a fraction of its implemented weight — when that work begins. Nothing in
this design may foreclose it: `rank.py` and `digest.py` must treat a lead's
identity as an opaque key so that key can later become a durable person id.

## Architecture

```
src/notable/
  cli.py        ~80   `notable run`, `notable feeds check`
  config.py    ~150   one pydantic model, TOML + env secrets
  cache.py      ~60   sha256(canonical request) -> response on disk
  http.py      ~120   one httpx client: timeouts, UA, 5xx retry, pacing
  llm.py       ~150   OpenRouter structured output + spend counter
  feeds.py     ~120   feedparser -> SourceItem
  detect.py    ~180   prompt + schema -> mentions
  wiki.py      ~250   search + facts, one wave -> match verdict
  coverage.py  ~350   brave -> screen -> fetch -> extract -> assess
  policy.py     ~90   publisher policy TOML -> eligible/ineligible/unclassified
  rank.py      ~120   deterministic lead outcome + ordering
  digest.py    ~120   Markdown render
  store.py     ~120   SQLite: four tables
  pipeline.py  ~150   the whole loop, readable top to bottom
  prompts/           ported verbatim from the prior rewrite
```

Each module has one purpose, a small public surface, and no knowledge of the
engine that calls it. `pipeline.py` is the only module that knows the order of
operations.

### The readability gate

`pipeline.py`'s main loop must fit on one screen:

```python
def run(cfg) -> Digest:
    items = [i for i in feeds.fetch_all(cfg) if not store.seen(i.url)]
    leads = []
    for item in items:
        for mention in detect.people_in(item, cfg):
            if not mention.research_worthy:
                continue
            if wiki.match(mention, cfg).has_page:
                continue
            articles = coverage.research(mention, cfg)
            leads.append(rank.assess(mention, articles, cfg))
    store.mark_seen(items)
    return digest.render(rank.order(leads)[: cfg.digest_size])
```

The *shape* is a design constraint, not an illustration: if the loop stops
fitting on one screen, the abstraction is wrong and the fix belongs in the
modules, not in the loop. The sketch elides only the run-record bookkeeping —
opening the `run` row, writing each `lead` row, and closing the run — which
`pipeline.py` owns via a context manager so it stays out of the reader's way.

Execution is **sequential**. No thread pools, no per-origin concurrency
manager. A 246-item run takes roughly 40 minutes instead of 10; for a
once-daily batch that is an acceptable trade for the ability to read a stack
trace. Politeness pacing between calls to one host remains, as a `sleep`.

## Data model: four tables

| Table | Columns | Purpose |
| --- | --- | --- |
| `seen_item` | `url`, `first_seen_at` | do not re-process a feed item |
| `surfaced` | `name_key`, `last_surfaced_at` | do not surface the same person daily |
| `run` | `id`, `started_at`, `ended_at`, `n_items`, `cost_usd`, `status` | run log |
| `lead` | `run_id`, `name`, `outcome`, `score`, `detail_json` | one lead, full detail as JSON |

`lead.detail_json` carries the complete evidence for a lead: mentions,
Wikipedia verdict, screened domains, per-article assessments, and the reasoning
behind the outcome. It is the audit story. `sqlite3 notable.db` plus `jq`
answers the questions the prior `audit/` package answered in ~2,600 lines of
source, and adding a field to a lead costs no migration.

Schema is created idempotently at startup with `CREATE TABLE IF NOT EXISTS`.
There is no migration framework; at four tables, deleting the database file and
re-running is a valid recovery path, because nothing in it is irreplaceable.

## Model use

Deterministic code owns all mechanics: provider calls, parsing, normalization,
configured rules, query construction, selection, bounds, caching, persistence,
aggregation, ranking, and rendering.

Models make only irreducibly semantic judgments, **one focused decision per
call**, with no cross-candidate batching, no agent loop, and no model-selected
tool:

| Task | Unit | Output |
| --- | --- | --- |
| `detect_people` | one source item | meaningful individual subjects |
| `match_wikipedia_identity` | one mention + bounded candidates | match verdict |
| `assess_article` | one person-article pair | identity, depth, content type |

`resolve_person_entity` is **not** in the MVP; it belongs to durable identity.

The four prompts port verbatim from `refactor/rearchitecture`. They are tuned
against real content and expensive to rediscover.

## Cost control

A running counter, not a reservation system.

Actual cost is read from OpenRouter's `usage` field after each call and
accumulated. When the total exceeds the configured cap, the pipeline raises
`BudgetExceeded`, which `cli.py` catches at the top level and renders a partial
digest from the leads completed so far. The run is recorded `partial`.

This deletes by construction the prior system's documented 4–9x
over-reservation defect: there is nothing to over-reserve.

## Lead policy

The prior programme's decision table is real product logic and carries over
unchanged:

- **`promising_lead`** — qualifying coverage from at least two distinct
  canonical eligible publisher domains.
- **`possible_lead`** — at least one useful coverage or attention reason, below
  the promising threshold.
- **`insufficient_evidence`** — a completed assessment with no qualifying or
  unresolved useful evidence.

`assessment_incomplete` is dropped: in a stateless pass there is no pending
work to represent. An item whose research could not complete (budget, provider
failure) is simply absent from this run's digest and will be retried on the
next run, at cache-warm cost.

The application does not infer intellectual independence or syndication, and
does not describe these outcomes as Wikipedia notability decisions.

### High recall

- Every usable feed item receives bounded person detection.
- **An uncertain Wikipedia match never suppresses a candidate.** Both
  `no_matching_page` and `uncertain` continue to coverage research.
- Technical failure and budget exhaustion never become semantic rejection.
- An unclassified publisher can remain a visible provisional lead.

## Publisher policy

`config/source_policies/visual_arts.toml` ports as curated data, minus the
`review_date`, `provenance_url`, and fingerprinting bookkeeping. First matching
rule in file order wins; no match means unclassified.

`policy.py` exposes one function: given a URL, return
`eligible | ineligible | unclassified`. Same-host collapsing to a canonical
domain for the two-domain threshold happens here.

## Testing

The prior suite's 71,334 lines and the mutation-evidence protocol that governed
it were themselves complexity drivers, and are in scope for this rebuild.

- **Target roughly a 1:1 test-to-source ratio.**
- Real unit tests for the deterministic core: policy screening, ranking, digest
  rendering, cache keying, URL canonicalization, extraction.
- Provider-facing tests replay recorded cache fixtures captured from real runs.
- One end-to-end test drives the whole pipeline from a fixture cache directory
  to a rendered digest.
- Live smoke tests are opt-in and marked, one per provider.

**The mutation-evidence protocol is dropped.** It was a rational response to a
71,000-line suite whose coverage claims could not be trusted. At this size it
is friction without a corresponding risk.

## Findings carried forward

`docs/findings.md` records the hard-won provider and model facts from the prior
programme — findings that cost real money to discover and would otherwise be
re-broken. It is a one-page constraints list, not a design authority. Read it
before touching schema construction or prompt rendering.

## Build order

Branch `mvp` off `main`. `main` remains the operational fallback.
`refactor/rearchitecture` is frozen as reference and is not a base.

| Phase | Deliverable |
| --- | --- |
| 0 | Port assets, replace `CLAUDE.md`, write `docs/findings.md`, skeleton package |
| 1 | **Walking skeleton** — config, cache, http, llm, feeds, detect, digest, store: a real digest from live feeds, detection only |
| 2 | Wikipedia match |
| 3 | Coverage — Brave, policy screening, fetch, extract, assess |
| 4 | Ranking and the full digest |

Each phase ends with a live run against real providers, and that run's cache
directory becomes the next phase's fixture set. **Phase 1 is the phase to get
right**; everything else hangs off the skeleton.

## Guardrails

The prior failure mode was gradual accretion in which every individual step
looked justified. Crude limits work better than judgment here. These belong in
`CLAUDE.md` as hard rules:

1. **Source stays under 3,000 lines.** Past that, something gets deleted or the
   feature does not land.
2. **No new table without deleting one.** Four is the budget.
3. **`pipeline.py`'s main loop fits on one screen.**
4. **No new cross-run state** without an explicit decision to leave MVP scope.

A limit that is never uncomfortable is not doing any work. When one of these
binds, the correct first response is to ask what can be removed.

## Explicitly out of scope

Deferred, in rough priority order:

1. **Durable person identity** — cross-run people, evidence accumulation,
   dormancy and reactivation. The intended first post-MVP feature.
2. `compose_lead_summary` — model-written `why_review` narrative on shortlist
   entries. Until then the digest renders from deterministic aggregation.
3. Source reconnaissance for unclassified publishers.
4. Concurrency.
5. `notable status` and the `audit` command family. `sqlite3` serves.
6. Promptfoo evaluation suites.

Permanently out of scope for this product: interactive review, acknowledgement,
dismissal or snoozing; multi-user assignment; multilingual research;
cross-publisher syndication inference; and automated article drafting or
editing.

## Acceptance

The MVP is complete when:

- `notable run` fetches configured feeds, detects people, checks English
  Wikipedia, researches coverage, and writes a dated Markdown digest plus
  `latest.md`;
- a crashed run re-runs to completion at near-zero provider cost;
- the spend cap produces a partial digest rather than a failure;
- source is under 3,000 lines and `pipeline.py`'s main loop fits on one screen;
- a live run over the ten configured feeds produces a shortlist comparable to
  the prior rewrite's verified run (5 digest entries from 246 items at $1.08),
  at comparable or lower cost; and
- no code path creates, drafts, or edits Wikipedia content.
