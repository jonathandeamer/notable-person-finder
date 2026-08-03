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

### The cache contract

The cache carries the entire recovery guarantee, so its contract is specified
rather than left to the implementation.

**Key composition.** The key is a SHA-256 over a canonical JSON encoding of:
provider name, HTTP method, full URL including query, the request body, and —
for model calls — the model id and the structured-output schema. Two calls that
could return different results must never collide. **Authorization headers,
API keys, and cookies are excluded from the key**, so a rotated key does not
invalidate the cache and no secret is written to disk.

**Atomic writes.** Entries are written to a temporary file in the same
directory and `os.replace`d into place. Without this, the precise event the
cache exists to survive — a crash mid-run — can leave a truncated entry that
poisons every subsequent replay.

**Corrupt entries are misses.** A entry that fails to parse is deleted and
treated as absent, never raised. A cache is an optimization; it may not be a
source of failure.

### Time-to-live classes

Three classes, because "cache everything forever" is wrong for anything that
describes the live web.

| Class | Calls | TTL | Why |
| --- | --- | --- | --- |
| Feed | RSS/Atom fetches | `feed_ttl_seconds`, default 43200 (12h) | Their whole purpose is to change |
| Discovery | Brave search, MediaWiki search and facts | `discovery_ttl_seconds`, default 86400 (24h) | Coverage and Wikipedia status change over time |
| Stable | Article fetches, model calls | permanent | Content at a URL, and a model's answer to a fixed prompt, are treated as fixed |

**Discovery calls must not be permanent.** A person researched once would
otherwise replay the same Brave results and the same Wikipedia verdict
indefinitely — so a person who *gained* a Wikipedia page, or gained coverage,
would be judged forever on a stale snapshot. A 24-hour TTL still makes
same-day crash recovery free, which is all recovery needs.

The feed TTL is 12 hours rather than one: a run takes roughly 40 minutes, so a
one-hour TTL can expire during a crash-and-restart and hand the re-run a
different feed snapshot, silently dropping items that had scrolled off. Twelve
hours comfortably covers a restart while still refreshing daily.
`notable run --fresh-feeds` bypasses the feed class. Recorded responses remain
perfectly good *fixtures*; TTLs govern production only.

**Only durable successes are cached.** A response is written to the cache only
when it is a provider success *and*, for model calls, passes schema and domain
validation. Transport failures, timeouts, 429s, 5xx, and model responses that
fail validation are never stored — a cached failure would make a transient
problem permanent and would poison the fixture set.

A validation failure is **not retried within a run**. The justification is
cost, not determinism: a retry re-sends identical input, and while sampling
means it *might* land differently, the prior programme measured twelve such
retries across six people with zero recoveries. Paying repeatedly for a call
that has already demonstrated it produces invalid output is a bad trade. The
call is recorded as a failure, `Incomplete` is raised, and the item is retried
on a **later run** — bounded by the attempt cap, so a permanently poisonous
item is abandoned rather than re-paid for daily.

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

**The mirror hazard, accepted explicitly.** Because `identity_key` is a
normalized name, two genuinely different people sharing a name collide on it.
Refusing to merge protects the *outcome* — their domains are never unioned into
a false `promising_lead` — but render-time suppression then hides one of them,
and `surfaced` suppresses both for the resurface window. The design trades a
false positive for a false negative. That is the right direction for a tool
whose output a human reads, but it is a real recall loss, not a non-issue.

It is not allowed to be *silent*. When suppression discards leads, the rendered
entry carries a one-line note — "2 further leads share this name" — with their
source URLs. The reader can then see that something was collapsed and go look.
Salting the key to make collisions impossible is rejected: it would defeat
cross-run suppression entirely, and distinguishing two same-named people is
durable identity, which is deferred.

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
def run(cfg, store) -> Digest:
    leads, settled, stuck = [], [], []
    try:
        for item in feeds.fetch_new(cfg, store):
            item_leads = []
            try:
                for mention in detect.people_in(item, cfg):
                    if not mention.research_worthy:
                        continue
                    verdict = wiki.match(mention, cfg)
                    if verdict.has_page:
                        continue
                    articles = coverage.research(mention, cfg)
                    item_leads.append(rank.assess(mention, verdict, articles, cfg))
            except Incomplete:
                stuck.append(item)               # retried next run, up to a cap
                continue
            leads.extend(item_leads)
            settled.append(item)
    except BudgetExceeded:
        pass                                     # render what finished
    shortlist = rank.order(leads)[: cfg.digest_size]
    written = digest.write(shortlist, cfg)       # temp file, atomic rename
    store.commit(settled, stuck, leads, written) # only after the file lands
    return written
```

The *shape* is a design constraint, not an illustration: if the loop stops
fitting on one screen, the abstraction is wrong and the fix belongs in the
modules, not in the loop.

Four invariants are load-bearing and must not be refactored away:

1. **Nothing durable is written until the digest file exists.** The run is one
   in-memory pass followed by one commit. There is no per-item settlement, no
   durable intermediate state, and no reconstruction of results across runs.
2. **Recovery is replay, not reconstruction.** A crash at any point before
   `store.commit` leaves the database untouched, so the re-run reprocesses
   every item — and every provider call it already made is a cache hit, so the
   replay is nearly free. This is the design's strongest idea, followed all the
   way through: because replay is cheap, durability of intermediate results
   buys nothing and costs a great deal.
3. **Leads are buffered per item.** If mention three of five raises
   `Incomplete`, the leads from mentions one and two are discarded with it.
   Extending `leads` inside the mention loop would leak an unsettled item's
   leads into the digest.
4. **`BudgetExceeded` is caught around the item loop, not at the CLI.** The
   partial digest renders from the leads already in memory. Catching it further
   out would lose them, since they are local to this function.

The Wikipedia verdict is threaded into `rank.assess` rather than discarded:
ranking needs it, and the lead log records it.

Two nested `try` blocks is the honest amount of nesting here: `Incomplete` is
per item and recoverable, `BudgetExceeded` ends the pass. They are not the same
failure and should not share a handler.

### Commit ordering

`digest.write` and `store.commit` cannot share a transaction — one writes files
and the other writes SQLite. The ordering is therefore load-bearing:

1. write the dated digest to a temporary file and `os.replace` it into place;
2. `os.replace` `latest.md`;
3. commit `surfaced`, the item markers, and the logs.

A crash between 2 and 3 repeats a digest on the next run. A crash in the other
ordering would mark leads surfaced that the user never saw, hiding them for the
whole resurface window. Repeating is recoverable; hiding is not.

Execution is **sequential**. No thread pools, no per-origin concurrency
manager. A 246-item run takes roughly 40 minutes instead of 10; for a
once-daily batch that is an acceptable trade for the ability to read a stack
trace. Politeness pacing between calls to one host remains, as a `sleep`.

## Data model: four tables

| Table | Columns | Purpose |
| --- | --- | --- |
| `item` | `url`, `first_seen_at`, `settled_at`, `attempts` | feed-item lifecycle |
| `surfaced` | `identity_key`, `last_surfaced_at` | do not surface the same person repeatedly |
| `run` | `id`, `started_at`, `ended_at`, `n_items_settled`, `n_items_incomplete`, `cost_usd`, `status` | run log |
| `lead` | `run_id`, `identity_key`, `display_name`, `outcome`, `rank_key_json`, `detail_json` | one lead, full detail as JSON |

`lead` carries no scalar score. Ordering is a lexicographic tuple (below);
`rank_key_json` stores the computed tuple so a digest can be reproduced and a
ranking decision explained without re-running the pipeline. `display_name` is
for rendering only and is never an identity. A lead is per **mention**, so
`detail_json` carries one mention, its Wikipedia verdict, its screened domains,
its per-article assessments, and the reasoning behind the outcome.

`detail_json` is the audit story. `sqlite3 notable.db` plus `jq` answers the
questions the prior `audit/` package answered in ~2,600 lines of source, and
adding a field to a lead costs no migration.

### Two tables are state; two are logs

This distinction is what keeps the persistence layer from becoming a second
recovery mechanism, and it must not blur.

**`item` and `surfaced` are state.** The pipeline reads them, and they change
what it does: `item` decides what to fetch, `surfaced` decides what to show.

**`run` and `lead` are append-only logs.** They are written by `store.commit`
after the digest file lands, and **the pipeline never reads them.** Nothing
branches on their contents; no recovery path consults them; no digest is
rendered from them. They exist so a human with `sqlite3` can ask what happened
and why — which is the entire audit story, at the cost of two `INSERT`s and
zero invariants.

A row the application never reads is a log, not state. If a future change makes
the pipeline read `lead`, that is the durable-workflow system growing back, and
it needs an explicit decision rather than a patch.

### The digest renders from this run only

`rank.order(leads)` ranks the leads produced by the current pass, takes the top
`cfg.digest_size`, and **discards the tail.**

There is no backlog, no pending queue, and no rendering from history. An
earlier draft had the digest select every unsurfaced lead ever stored,
regardless of run — which was a persistent digest queue with none of its
semantics: leads below the cutoff pending forever, old leads becoming eligible
again after the resurface window and recurring indefinitely, and `possible_lead`
entries starving behind a steady supply of `promising_lead` ones. The prior
programme built a real queue with a real starvation guard to handle exactly
this, and it was expensive. Backlog delivery is a product feature, not a
side effect of a render filter; if it is wanted, it gets its own design.

The cost is accepted and stated: **a lead that ranks below the cutoff on a busy
day is not shown, and its item is marked seen, so it will not be found again.**
It remains in the `lead` log. Set `digest_size` generously — this is a personal
daily digest, and 20 entries costs nothing to skim.

### Suppression window

`surfaced` records each `identity_key` the digest actually rendered, committed
in step 3 of the ordering above. A lead is suppressed while `last_surfaced_at`
is within `cfg.resurface_after_days` (default 30).

Its job is narrow: two *different* items about the same person, days apart,
each produce a lead, and without suppression the person appears in two digests.
It is not a queue and never resurrects a lead — a suppressed lead is dropped,
not deferred.

### Item lifecycle and the attempt cap

`item` replaces a bare seen-set because an item has three states, not two:
unseen, settled (`settled_at` set), and **stuck** (`attempts` at the cap,
`settled_at` null).

Without the third, a permanently poisonous item — a model response that fails
domain validation every time, or a discovery article behind a hard 404 —
never settles and is therefore retried on **every subsequent daily run,
forever**, re-paying for the failing model call each time, since validation
failures are deliberately not cached. One bad item becomes a permanent daily
tax and a permanent silent recall loss.

`store.commit` increments `attempts` for every item in the `stuck` list.
`feeds.fetch_new` skips items that are settled *or* at
`cfg.max_item_attempts` (default 3). A stuck item
computes no lead outcome and is never surfaced; it is simply abandoned, and
`sqlite3` finds it: `SELECT url FROM item WHERE settled_at IS NULL AND
attempts >= 3`.

This is two extra columns on one table, not a fifth table. The four-table
budget holds.

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
    publisher; or
  - a `same_person` article with `significant` or `passing` depth that is
    `content_qualifying` but not fully qualifying.
- **`insufficient_evidence`** — a *terminal* assessment with none of the above.

### Consume less than the prompt emits

The ported `assess_article` prompt and schema are richer than these rules: they
also return attention and caution signals, content-type detail, and evidence
visibility. **All of it is stored in `lead.detail_json`. None of it is consumed
by the MVP's outcome or ranking rules.**

This is the standing discipline for the rebuild, and it is what keeps a tuned
prompt from dragging its whole downstream apparatus back in. Ported prompts are
free; *code that consumes their output* is not, and every consumed field is a
concept the reader must hold and a branch the tests must cover.

The prior programme had a fourth `possible_lead` reason — "at least one
transferable attention signal" — dropped here. The recall it adds over the
three rules above is narrow: an article that is *not* `content_qualifying` (a
listing or announcement) but carries an award or major-show signal. That is
weak evidence, and reasons two and three already catch the substantive cases.

Because the signals are still in `detail_json`, this is reversible from real
run data rather than from argument: query the stored leads, count how many
`insufficient_evidence` results carried a transferable signal, and re-add the
rule if the number justifies it.

### Incompleteness is operational, not semantic

`assessment_incomplete` is dropped **as a lead outcome**, but the condition it
represented is not, and must not collapse into `insufficient_evidence`. That
conflation would turn a provider failure into a product judgement of "we looked
and found nothing" — a silent false negative, and the exact hazard the prior
code guarded with its `assessment_terminal` flag.

Instead, completeness is an **operational status carried outside the decision
table**:

- Research that does not reach a terminal state raises `Incomplete`.
- **`coverage.research` raises rather than returning partial results.** This is
  what makes the rule implementable: `rank.assess` is never called on
  incomplete research, so it never has to detect it. Empty coverage results
  therefore mean the search genuinely ran and returned nothing, which is
  honestly `insufficient_evidence`.
- **No lead outcome is computed for incomplete research** — and, by invariant 3
  of the loop, no lead already computed for *earlier mentions of the same item*
  is kept either. The whole item is discarded and retried.
- The item is not settled, so the next run retries it at cache-warm cost, up to
  `cfg.max_item_attempts`.
- The run is logged `partial`, with `n_items_incomplete` counting the items that
  raised. `store.commit` writes this from the `stuck` list.

### Ranking

Ordering is a lexicographic tuple, never a scalar score, so that every
comparison is explicable. Ascending:

```python
(
    OUTCOME_RANK[outcome],              # promising 0, possible 1
    WIKIPEDIA_RANK[wikipedia_outcome],  # no_matching_page 0, uncertain 1
    -qualifying_domain_count,
    identity_key,                       # stable tie-breaker only
)
```

`WIKIPEDIA_RANK` has exactly two values, not three: a `matching_page` mention
hits `continue` in the loop and never becomes a lead, so no third branch is
reachable. If a third value ever becomes necessary, the loop's skip has changed
and both must move together.

**`rank.order` filters `insufficient_evidence` before sorting.** Those leads
carry no `OUTCOME_RANK` entry, so ranking one is a `KeyError` — deliberately,
as the loud failure is better than an arbitrary ordering. They are stored for
inspection and never rendered.

Four terms, not the prior programme's nine. Two of its terms (starvation guard,
eligibility reason) die with the digest queue. Three more —
`positive_signal_count`, evidence visibility, and article freshness — are
dropped under the rule above: each demanded that a consumed field be threaded
from the assessment through to ranking, and all three only order the *tail* of
a shortlist that runs to a handful of entries. They order nothing a reader of
the digest would notice.

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

Seven tests pin invariants that are cheap to break and expensive to notice.
Each must fail if its rule is removed:

- **Namesake safety.** Two mentions of different people sharing one name, each
  with one qualifying domain, must produce two `possible_lead`s — never one
  `promising_lead`. The guard on the deferred-identity decision. Its companion
  asserts the collapse is *visible*: the rendered entry must carry the
  "further leads share this name" note.
- **Feed freshness.** A second run past the feed TTL must see new feed items.
  A permanently cached feed silently yields an empty digest forever.
- **Incomplete discards the whole item.** An item where mention three raises
  `Incomplete` must leave no lead rows at all — including for mentions one and
  two — must not be settled, and must not appear as `insufficient_evidence`.
- **A crash writes nothing.** A run interrupted before `store.commit` must
  leave `item`, `surfaced`, `run`, and `lead` untouched, and the re-run must
  produce the same digest making zero new provider calls. This pins both
  halves of the recovery model: nothing durable before the digest, and replay
  covered entirely by the cache.
- **Cache entries survive a truncated write.** A half-written entry must be
  treated as a miss and refetched, not raised and not replayed.
- **Discovery calls expire.** A Brave or MediaWiki call repeated past
  `discovery_ttl_seconds` must re-request rather than replay, or a person's
  Wikipedia status freezes at whatever it was the first time.
- **Poison items are abandoned.** An item that raises `Incomplete` every time
  must stop being retried at `max_item_attempts`, and must make no further
  provider calls thereafter.
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
- **the fixed-corpus regression passes.** One recorded cache directory from a
  real ten-feed run is committed as a fixture, together with an expected-results
  file naming, for each person the corpus should surface, the required outcome
  class (`promising_lead` or `possible_lead`) and the people that must *not* be
  surfaced. This is the pass/fail gate, and it runs offline in seconds. "A
  shortlist comparable to the prior run" is not a criterion — it cannot be
  evaluated, and a rebuild needs a test that can fail;
- the live ten-feed run remains as a qualitative smoke check, compared against
  the prior rewrite's verified figures (5 digest entries from 246 items at
  $1.08) for cost and order of magnitude, not for pass/fail; and
- no code path creates, drafts, or edits Wikipedia content.
