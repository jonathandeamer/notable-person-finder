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

### What is cacheable

**Feed fetches are not permanently cacheable.** A feed URL's whole purpose is
to return different content tomorrow; caching it by request hash would replay
the first response forever, and combined with the seen-set that yields an empty
digest every day thereafter. Feed requests therefore take a short TTL
(`cache.feed_ttl_seconds`, default 900 — long enough to make a same-session
re-run free, short enough that a daily run always sees fresh content), and
`notable run --fresh-feeds` bypasses it. Recorded feed responses remain
perfectly good *fixtures*; the TTL governs production only.

**Only durable successes are cached.** A response is written to the cache only
when it is a provider success *and*, for model calls, passes schema and domain
validation. Transport failures, timeouts, 429s, 5xx, and model responses that
fail validation are never stored — a cached failure would make a transient
problem permanent and would poison the fixture set. Retrying a validation
failure re-sends byte-identical input and so cannot succeed; such a call is
recorded as a failure for that item and the item does not settle.

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
no cross-run evidence accumulation. Across runs, deduplication is on source
item URL.

### Mentions are never merged across items

Each mention is researched independently and produces its own lead. Two source
items naming the same person produce two leads.

This is the explicit choice, and the alternative is rejected: grouping mentions
by normalized name would let **two different people sharing a name have their
publisher domains unioned into a false `promising_lead`** — precisely the
failure the prior programme's LLM entity resolution existed to prevent.
Deciding that two mentions are one person *is* durable identity, and durable
identity is deferred. Nothing may quietly reintroduce it under the guise of
deduplication.

**Evidence is never unioned across mentions.** The qualifying-domain count for
a lead counts only domains found while researching that one mention.

The cost of duplication is small, because the cache absorbs it: two mentions of
the same name issue byte-identical MediaWiki and Brave requests, so the second
is a cache hit. Only the per-article `assess_article` calls differ, and those
are genuinely different articles.

Duplicates are handled at **render** time only: `digest.py` shows one entry per
`identity_key`, keeping the highest-ranked lead and discarding the rest. It
does not combine them. Suppression of a duplicate can never promote an outcome.

The remaining consequence is accepted deliberately: a person mentioned in two
articles a week apart is researched twice, and evidence spread across those two
articles never adds up to a promising lead. At roughly $1 per run this is
cheaper than the ~8,000 lines of identity machinery it replaces, and it is the
single clearest thing durable identity buys back.

### The identity key

`identity_key` is an **opaque scalar** on every lead. In the MVP it is
constructed upstream as a normalized-name key; after durable identity lands it
becomes a person id. Its construction is owned solely by the code that builds a
lead.

Downstream modules are constrained so that substitution is free:

- **`rank.py` may use `identity_key` only as the final stable tie-breaker.** It
  must never parse it, compare it for similarity, or derive an ordering signal
  from it.
- **`digest.py` may use it only for duplicate suppression.**
- **`store.py`** keys the `surfaced` table on it.

No other module reads it.

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
    leads = []
    for item in feeds.fetch_new(cfg, store):
        try:
            for mention in detect.people_in(item, cfg):
                if not mention.research_worthy:
                    continue
                if wiki.match(mention, cfg).has_page:
                    continue
                articles = coverage.research(mention, cfg)
                leads.append(rank.assess(mention, articles, cfg))
        except Incomplete:
            continue                   # item stays unseen; retried next run
        store.mark_seen(item)          # per item, only once fully settled
    return digest.render(rank.order(leads), store)
```

The *shape* is a design constraint, not an illustration: if the loop stops
fitting on one screen, the abstraction is wrong and the fix belongs in the
modules, not in the loop. The sketch elides only the run-record bookkeeping —
opening the `run` row, writing each `lead` row, and closing the run — which
`pipeline.py` owns via a context manager so it stays out of the reader's way.

Three invariants are load-bearing and must not be refactored away:

1. **`store.mark_seen` takes one item and is committed per item**, never once
   over a batch. A batch marker would either lose every item's work on one
   failure or, worse, silently mark unfinished items complete forever.
2. **It runs only after every research-worthy mention in that item reaches a
   semantic terminal.** `Incomplete` — raised for a provider failure, a
   validation failure, or an exhausted retry — skips the marker, so the item is
   reprocessed next run at cache-warm cost.
3. **`BudgetExceeded` is not caught here.** It propagates to `cli.py`, which
   renders a partial digest from the leads already collected. Every item not
   yet reached is simply still unseen, which is the correct state.

Execution is **sequential**. No thread pools, no per-origin concurrency
manager. A 246-item run takes roughly 40 minutes instead of 10; for a
once-daily batch that is an acceptable trade for the ability to read a stack
trace. Politeness pacing between calls to one host remains, as a `sleep`.

## Data model: four tables

| Table | Columns | Purpose |
| --- | --- | --- |
| `seen_item` | `url`, `first_seen_at` | do not re-process a feed item |
| `surfaced` | `identity_key`, `last_surfaced_at` | do not surface the same person daily |
| `run` | `id`, `started_at`, `ended_at`, `n_items`, `cost_usd`, `status` | run log |
| `lead` | `run_id`, `identity_key`, `display_name`, `outcome`, `rank_key_json`, `detail_json` | one lead, full detail as JSON |

`lead` carries no scalar score. Ordering is a lexicographic tuple (below);
`rank_key_json` stores the computed tuple so a digest can be reproduced and a
ranking decision explained without re-running the pipeline. `display_name` is
for rendering only and is never an identity.

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

The three corresponding prompts port verbatim from `refactor/rearchitecture`.
They are tuned against real content and expensive to rediscover.
`resolve_person_entity.md` is not ported; it returns with durable identity.

## Cost control

A running counter, not a reservation system.

Actual cost is read from OpenRouter's `usage` field after each call and
accumulated. When the total exceeds the configured cap, the pipeline raises
`BudgetExceeded`, which `cli.py` catches at the top level and renders a partial
digest from the leads completed so far. The run is recorded `partial`.

**This is a soft cap, and one-call overshoot is accepted.** Cost is knowable
only after a call returns, so a run can exceed the configured amount by at most
the cost of one maximum-sized call. Set the cap with that headroom in mind. The
alternative — pre-call reservation — is what produced the prior system's
documented 4–9x over-reservation defect, and buying a hard bound at that price
is a bad trade for a personal tool.

This deletes by construction the prior system's documented 4–9x
over-reservation defect: there is nothing to over-reserve.

## Lead policy

The prior programme's decision table is real product logic and carries over.
It is reproduced here in full, because this document is the only authority and
implementation must not have to consult the superseded specifications.

### Qualifying article

An assessed article **qualifies** when all five hold:

- `same_person` — the article is about this person;
- `coverage_depth == "significant"` — not passing or uncertain;
- `screening_status == "curated_eligible"` — per `policy.py`;
- `editorially_independent`; and
- `content_qualifying` — the content type is not a listing, announcement,
  press release, or sponsored placement.

### Outcomes

- **`promising_lead`** — qualifying articles from at least
  `promising_domain_threshold` (default 2) distinct **canonical** eligible
  domains.
- **`possible_lead`** — below that threshold, and any one of:
  - at least one qualifying article;
  - a `same_person` article with `significant` depth from an **unclassified**
    publisher;
  - a `same_person` article with `significant` or `passing` depth that is
    `content_qualifying` but not fully qualifying; or
  - at least one **transferable attention signal**.
- **`insufficient_evidence`** — a *terminal* assessment with none of the above.

### Incompleteness is operational, not semantic

`assessment_incomplete` is dropped **as a lead outcome**, but the condition it
represented is not, and must not collapse into `insufficient_evidence`. That
conflation would turn a provider failure into a product judgement of "we looked
and found nothing" — a silent false negative, and the exact hazard the prior
code guarded with its `assessment_terminal` flag.

Instead, completeness is an **operational status carried outside the decision
table**:

- Research that does not reach a terminal state raises `Incomplete`.
- **No lead outcome is computed for incomplete research** — no row is written,
  nothing is ranked, nothing is surfaced.
- The source item is not marked seen, so the next run retries it at cache-warm
  cost.
- The run is recorded `partial`, with a count of incomplete items.

`rank.assess` must therefore refuse to return `insufficient_evidence` unless
every research-worthy mention on the item settled. Empty coverage results are
`insufficient_evidence` only when the search genuinely ran and returned
nothing.

### Ranking

Ordering is a lexicographic tuple, never a scalar score, so that every
comparison is explicable. Ascending; the prior programme's queue-lifecycle
terms (starvation guard, eligibility reason) are dropped with the queue:

```python
(
    OUTCOME_RANK[outcome],              # promising 0, possible 1
    WIKIPEDIA_RANK[wikipedia_outcome],  # no_matching_page 0, uncertain 1, else 2
    -qualifying_domain_count,
    -positive_signal_count,             # transferable attention signals
    EVIDENCE_VISIBILITY_RANK[best],     # full 0, partial 1, snippet 2
    -freshest_qualifying_article_at,    # ISO-8601 UTC, sorts chronologically
    identity_key,                       # stable tie-breaker only
)
```

`insufficient_evidence` leads are never ranked or rendered; they are stored for
inspection.

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

Four tests pin invariants that are cheap to break and expensive to notice.
Each must fail if its rule is removed:

- **Namesake safety.** Two mentions of different people sharing one name, each
  with one qualifying domain, must produce two `possible_lead`s — never one
  `promising_lead`. This is the guard on the deferred-identity decision.
- **Feed freshness.** A second run past the feed TTL must see new feed items.
  A permanently cached feed silently yields an empty digest forever.
- **Incomplete never settles.** An item whose research raises `Incomplete`
  must not be marked seen, must write no lead row, and must not appear as
  `insufficient_evidence`.
- **Failures are not cached.** A transport failure followed by a success must
  return the success, not a replayed failure.

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
