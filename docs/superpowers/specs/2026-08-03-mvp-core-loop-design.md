# MVP Core Loop Design

**Status:** Approved
**Date:** 2026-08-03
**Branch:** `mvp`
**Supersedes for implementation purposes:** the entire
`refactor/rearchitecture` programme (frozen as reference, not deleted)

Rebuild the "find promising leads" loop as a small, maintainable application
that one person can hold in their head. This is the single architectural
authority for the MVP; there are no companion specs. Appendix A records the
alternatives that were considered and rejected, so the body can state rules
rather than argue for them.

## Background

`main` is the original prototype: ~7,000 lines, a `run_pipeline.py` driver
shelling out to fifteen gate scripts over JSONL state. Broken, but
comprehensible end to end.

`refactor/rearchitecture` is the clean-slate rewrite: **38,608 lines of source,
71,334 of tests, 34,463 of documentation, and 45 SQLite tables.** It works — a
live ten-feed run over 246 items cost $1.08 and produced a real shortlist — but
it succumbed to the second-system effect. Its *product* design is sound and
carries forward. Its *machinery* does not: full event-sourced durability, a
hand-rolled workflow engine, fingerprints as the change-detection primitive,
and multi-wave state machines per domain together imposed roughly five
integration points per feature. That is why `coverage/service.py` reached 4,708
lines, and why the top open item ("raise `max_candidates` from 8") was blocked —
the value moves match fingerprints, which move tests, which move plan
reconciliation. A tuning knob had become a schema migration.

The MVP targets **under 3,000 lines of source**.

## The central decision

**A hash-keyed response cache replaces the durability machinery.** Every
external call — feeds, Brave, MediaWiki, article fetch, OpenRouter — passes
through one caching wrapper keyed by a hash of the canonical request. A crashed
run is simply re-run, and every prior call is a cache hit.

This removes the need for work items, retry coordination, budget reservation,
mutation locks, interrupted-run sweeps, superseding, and per-stage checkpoints:
roughly 60 lines replace roughly 2,800. It also means **a real run's cache
directory is a test fixture set, for free.**

### The cache contract

The cache carries the entire recovery guarantee, so its contract is specified
rather than left to the implementation.

**Key composition.** SHA-256 over a canonical JSON encoding of: provider name,
HTTP method, full URL including query, request body, and — for model calls —
model id and structured-output schema. Two calls that could return different
results must never collide.

**Secrets are excluded from the key**, so a rotated key does not invalidate the
cache and nothing sensitive reaches disk. Excluding *all* headers would break
the collision guarantee, since `Accept`, `Accept-Language`, and cookies can
change a response. The guarantee is restored by constraining the transport
instead: **`http.py` sends one fixed header set and keeps no cookie jar**, and
those fixed values are folded into the key as a single `transport_profile`
string. No per-request header override is offered.

**Atomic writes.** Entries are written to a temporary file in the same
directory and `os.replace`d into place. Without this, the exact event the cache
exists to survive — a crash mid-run — can leave a truncated entry that poisons
every subsequent replay.

**Corrupt entries are misses.** An entry that fails to parse is deleted and
treated as absent, never raised. A cache is an optimization; it may not be a
source of failure.

**Only validated successes are cached.** Transport failures, timeouts, 429s,
5xx, and model responses failing schema or domain validation are never stored.
A cached failure would make a transient problem permanent and would poison the
fixture set.

A validation failure is **not retried within a run** — on cost grounds, not
determinism (see A1). It raises `Incomplete`; the item retries on a later run.

### Time-to-live classes

| Class | Calls | TTL |
| --- | --- | --- |
| Feed | RSS/Atom fetches | `feed_ttl_seconds`, default 43200 (12h) |
| Discovery | Brave search, MediaWiki search and facts | `discovery_ttl_seconds`, default 86400 (24h) |
| Stable | Article fetches, model calls | permanent |

**Discovery calls must not be permanent.** A person researched once would
otherwise replay the same Brave results and Wikipedia verdict indefinitely, so
someone who *gained* a page or gained coverage would be judged forever on a
stale snapshot. A 24-hour TTL still makes same-day crash recovery free, which is
all recovery needs.

The feed TTL exceeds a run's ~40 minutes so a crash-and-restart does not receive
a different feed snapshot and silently drop items that scrolled off.
`notable run --fresh-feeds` bypasses the feed class. TTLs govern production
only; recorded responses remain valid fixtures.

## Product boundary

A personal, non-interactive, once-daily batch tool. It monitors
editor-configured RSS/Atom feeds, finds meaningful individual subjects, checks
English Wikipedia, researches English-language coverage, and produces a small
inspectable Markdown shortlist.

**It never writes Wikipedia content, drafts an article, or decides that a
person satisfies Wikipedia policy.** Every publication action requires explicit
human review. This is a hard invariant, not a milestone boundary.

The visual-arts profile is pilot configuration, not application logic. No
publisher-specific branches in Python.

## State model

Each run processes new feed items end to end and renders a digest. Durable
state is limited to what stops repeated work and repeated surfacing.

There is **no durable person entity**, no LLM entity resolution, no
reconsideration, no confirmed merges, and no cross-run evidence accumulation.
Across runs, deduplication is on source item URL.

### Mentions are never merged

Each mention is researched independently and produces its own lead. Two items
naming the same person produce two leads, and **evidence is never unioned
across them** — a lead's qualifying-domain count reflects only that one
mention's research.

Merging by normalized name would let two different people sharing a name have
their domains unioned into a false `promising_lead`. Deciding two mentions are
one person *is* durable identity, which is deferred; nothing may reintroduce it
as deduplication.

Duplication is cheap because the cache absorbs it: two mentions of one name
issue identical MediaWiki and Brave requests. Only `assess_article` differs, and
those are genuinely different articles.

Duplicates are collapsed during **selection** (step 3 below), never combined.

**The mirror hazard, accepted explicitly.** Because `identity_key` is a
normalized name, two genuinely different people sharing a name collide on it.
Refusing to merge protects the *outcome*, but collapsing then hides one of them,
and `surfaced` suppresses both for the resurface window. The design trades a
false positive for a false negative — the right direction for human-read output,
but a real recall loss.

It may not be *silent*. The rendered entry carries a one-line note — "2 further
leads share this name" — with the discarded leads' source URLs.

### The identity key

`identity_key` is an **opaque scalar** on every lead: a normalized-name key in
the MVP, a person id once durable identity lands. Its construction is owned
solely by the code that builds a lead. To keep that substitution free:

- **`rank.py` may use it only as the final tie-breaker and for collapsing.** It
  must never parse it or derive an ordering signal from it.
- **`store.py`** keys `surfaced` on it.
- No other module reads it.

**Durable person identity is the intended first post-MVP feature**, deferred
rather than rejected. `2026-07-30-durable-person-identity-design.md` on
`refactor/rearchitecture` remains the best thinking on it and should be
revisited critically, at a fraction of its implemented weight.

## Architecture

```
src/notable/
  cli.py        ~80   `notable run`, `notable feeds check`
  config.py    ~150   one pydantic model, TOML + env secrets
  cache.py      ~60   sha256(canonical request) -> response on disk
  http.py      ~120   one httpx client: fixed headers, timeouts, 5xx retry, pacing
  llm.py       ~150   OpenRouter structured output + spend counter
  feeds.py     ~120   feedparser -> SourceItem
  detect.py    ~180   prompt + schema -> mentions
  wiki.py      ~250   search + facts, one wave -> match verdict
  coverage.py  ~350   brave -> screen -> fetch -> extract -> assess
  policy.py     ~90   publisher policy TOML -> eligible/ineligible/unclassified
  rank.py      ~120   lead outcome, selection, ordering
  digest.py    ~120   Markdown render
  store.py     ~120   SQLite: four tables
  pipeline.py  ~150   the whole loop, readable top to bottom
  prompts/           ported verbatim
```

Each module has one purpose and no knowledge of the engine calling it.
`pipeline.py` alone knows the order of operations.

### The readability gate

```python
def run(cfg, store) -> Digest:
    leads, settled, stuck, status = [], [], [], "ok"
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
        status = "partial"                       # render what finished
    shortlist = rank.shortlist(leads, store, cfg)   # selection order below
    written = digest.write(shortlist, cfg)          # temp file, atomic rename
    store.commit(settled, stuck, shortlist)         # state, after the file lands
    store.log(status, leads, written, llm.spend())  # best effort, separate txn
    return written
```

The *shape* is a design constraint: if this stops fitting on one screen, the
abstraction is wrong and the fix belongs in the modules, not the loop.

Four invariants are load-bearing and must not be refactored away:

1. **Nothing durable is written until the digest file exists.** One in-memory
   pass, then one commit. No per-item settlement, no durable intermediate
   state, no reconstruction across runs.
2. **Recovery is replay, not reconstruction.** A crash before `store.commit`
   leaves the database untouched, so the re-run reprocesses everything against a
   warm cache. Because replay is cheap, durable intermediate results buy nothing
   and cost a great deal.
3. **Leads are buffered per item.** If mention three of five raises
   `Incomplete`, mentions one and two are discarded with it. Extending `leads`
   inside the mention loop would leak an unsettled item's leads into the digest.
4. **`BudgetExceeded` is caught here, not at the CLI.** The partial digest
   renders from leads local to this function and unreachable from `cli.py`. It
   sets `status`, which reaches the run log; it is not swallowed.

Two nested `try` blocks is the honest amount: `Incomplete` is per item and
recoverable, `BudgetExceeded` ends the pass. The Wikipedia verdict is threaded
into `rank.assess` rather than discarded — ranking needs it, and the lead log
records it.

Execution is **sequential**: no thread pools, no concurrency manager. A 246-item
run takes ~40 minutes rather than ~10, traded for readable stack traces.
Politeness pacing between calls to one host remains, as a `sleep`.

### Selection order

`rank.shortlist` is one function with a fixed order, because each step changes
what the next sees and the wrong order loses people silently:

1. **filter** out `insufficient_evidence`;
2. **suppress** identities in `surfaced` within `cfg.resurface_after_days`;
3. **collapse** same-name duplicates to the highest-ranked representative,
   retaining the discarded leads' source URLs for the shared-name note;
4. **rank** representatives by the tuple below;
5. **cut** to `cfg.digest_size`.

Slicing before collapsing is the trap: twenty top-ranked mentions of one name
would fill the shortlist and render as a single entry while everyone else fell
off the end. Collapsing before ranking is what makes the cut mean "top N
*people*". Suppression must also precede the cut, or a digest can consist
entirely of entries then removed for having been shown last week.

### Commit ordering

`digest.write` and `store.commit` cannot share a transaction — one writes files,
the other SQLite. The ordering is load-bearing:

1. write the dated digest to a temp file and `os.replace` it into place;
2. `os.replace` `latest.md`;
3. commit the state tables (`item`, `surfaced`);
4. write the logs, separately and best-effort.

A crash between 2 and 3 repeats a digest next run. The reverse ordering would
mark leads surfaced that the user never saw, hiding them for the whole
resurface window. Repeating is recoverable; hiding is not.

## Data model

| Table | Columns | Purpose |
| --- | --- | --- |
| `item` | `url`, `first_seen_at`, `settled_at`, `attempts` | feed-item lifecycle |
| `surfaced` | `identity_key`, `last_surfaced_at` | do not re-surface a person |
| `run` | `id`, `started_at`, `ended_at`, `n_items_settled`, `n_items_incomplete`, `cost_usd`, `status` | run log |
| `lead` | `run_id`, `identity_key`, `display_name`, `outcome`, `rank_key_json`, `detail_json` | one lead, full detail as JSON |

A lead is per **mention**. `detail_json` carries that mention, its Wikipedia
verdict, screened domains, per-article assessments, and the reasoning behind the
outcome. `rank_key_json` stores the computed ordering tuple so a ranking
decision is explicable without re-running. `display_name` is for rendering only
and is never an identity.

Schema is created idempotently with `CREATE TABLE IF NOT EXISTS`. There is no
migration framework: at four tables, deleting the database and re-running is a
valid recovery path, because nothing in it is irreplaceable.

### Two tables are state; two are logs

This distinction keeps persistence from becoming a second recovery mechanism,
and it must not blur.

**`item` and `surfaced` are state.** The pipeline reads them and they change
what it does: `item` decides what to fetch, `surfaced` what to show.

**`run` and `lead` are append-only logs the pipeline never reads.** Nothing
branches on them, no recovery path consults them, no digest renders from them.
They are written by `store.log` **in a separate transaction after the state
tables are committed, and a failure to write them is warned about, not raised.**

That separation is not fussiness. Sharing `store.commit`'s transaction would let
a `detail_json` serialization error roll back the item markers *after the digest
file was written* — so the next run repeats all the work, rewrites the digest,
and hits the identical deterministic failure. One bad log row would livelock the
product.

`lead` earns its place despite being unread for a concrete reason: **tuning
recall is this product's main development activity**, and it is done by querying
stored leads. Re-adding the dropped signal rule is specified as exactly such a
query. Without the log, that question costs another paid run.

A row the application never reads is a log, not state. If a change makes the
pipeline read `lead`, or moves it inside the state transaction, the
durable-workflow system is growing back and needs an explicit decision.

### The digest renders from this run only

`rank.shortlist` sees only the current pass's leads and **discards the tail**
below `cfg.digest_size`. There is no backlog, no pending queue, no rendering
from history (see A2).

The cost is accepted and stated: **a lead ranking below the cutoff on a busy day
is not shown, and its item is marked settled, so it will not be found again.** It
remains in the `lead` log. Set `digest_size` generously — 20 entries costs
nothing to skim.

### Suppression window

`surfaced` records each `identity_key` the digest actually rendered. A lead is
suppressed while `last_surfaced_at` is within `cfg.resurface_after_days`
(default 30).

Its job is narrow: two *different* items about one person, days apart, would
otherwise put them in two digests. It is not a queue and never resurrects a
lead — a suppressed lead is dropped, not deferred.

### Item lifecycle and the attempt cap

An item has three states: unseen, settled (`settled_at` set), and **stuck**
(`attempts` at the cap, `settled_at` null).

Without the third, a permanently poisonous item — a response that fails domain
validation every time, or an article behind a hard 404 — never settles and is
retried on **every subsequent daily run, forever**, re-paying for the failing
call each time. One bad item becomes a permanent daily tax and a permanent
silent recall loss.

`store.commit` increments `attempts` for each item in `stuck`.
`feeds.fetch_new` skips items that are settled *or* at `cfg.max_item_attempts`
(default 3). A stuck item computes no outcome and is never surfaced; it is
abandoned, and `sqlite3` finds it:
`SELECT url FROM item WHERE settled_at IS NULL AND attempts >= 3`.

Two extra columns on one table, not a fifth table. The budget holds.

## Model use

Deterministic code owns all mechanics: provider calls, parsing, normalization,
configured rules, query construction, selection, bounds, caching, persistence,
aggregation, ranking, rendering.

Models make only irreducibly semantic judgments, **one focused decision per
call**, with no cross-candidate batching, agent loop, or model-selected tool:

| Task | Unit | Output |
| --- | --- | --- |
| `detect_people` | one source item | meaningful individual subjects |
| `match_wikipedia_identity` | one mention + bounded candidates | match verdict |
| `assess_article` | one person-article pair | identity, depth, content type |

The three corresponding prompts port verbatim; they are tuned against real
content and expensive to rediscover. `resolve_person_entity` is **not** in the
MVP and its prompt is not ported — it returns with durable identity.

## Cost control

A running counter, not a reservation system. Actual cost is read from
OpenRouter's `usage` field and accumulated by `llm.py`. Past the cap, the next
call raises `BudgetExceeded`; **`pipeline.run` is its only handler**, setting
status `partial` and rendering from leads already in memory. `llm.spend()`
supplies the total to the run log.

**This is a soft cap, and one-call overshoot is accepted.** Cost is knowable
only after a call returns. Set the cap with that headroom in mind; pre-call
reservation is what produced the prior system's 4–9x over-reservation defect.

## Lead policy

Carried over from the prior programme and reproduced in full, because this
document is the only authority.

### Qualifying article

An assessed article **qualifies** when all five hold:

- `same_person`;
- `coverage_depth == "significant"`;
- `screening_status == "curated_eligible"` per `policy.py`;
- `editorially_independent`;
- `content_qualifying` — not a listing, announcement, press release, or
  sponsored placement.

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

The application does not infer intellectual independence or syndication, and
does not describe these outcomes as Wikipedia notability decisions.

### Consume less than the prompt emits

The ported `assess_article` prompt returns more than these rules use: attention
and caution signals, content-type detail, evidence visibility. **All of it is
stored in `detail_json`. None is consumed by outcome or ranking rules.**

This is the standing discipline for the rebuild, and it is what stops a tuned
prompt dragging its whole downstream apparatus back in. Ported prompts are free;
*code that consumes their output* is not — every consumed field is a concept the
reader holds and a branch the tests cover.

Because the unused fields are still logged, re-adding a rule is a query against
real run data rather than an argument (see A3).

### Incompleteness is operational, not semantic

`assessment_incomplete` is dropped **as a lead outcome**, but the condition is
not, and must not collapse into `insufficient_evidence` — that would turn a
provider failure into "we looked and found nothing", a silent false negative.

- Research that does not reach a terminal state raises `Incomplete`.
- **`coverage.research` raises rather than returning partial results.** This is
  what makes the rule implementable: `rank.assess` never sees incomplete
  research and never has to detect it. Empty coverage therefore means the search
  genuinely ran and returned nothing, which is honestly `insufficient_evidence`.
- No lead outcome is computed, and by invariant 3 no lead from earlier mentions
  of the same item is kept. The whole item is discarded and retried.
- The run is logged `partial`, `n_items_incomplete` counting items that raised.

### Ranking

A lexicographic tuple, never a scalar score, so every comparison is explicable.
Ascending:

```python
(
    OUTCOME_RANK[outcome],              # promising 0, possible 1
    WIKIPEDIA_RANK[wikipedia_outcome],  # no_matching_page 0, uncertain 1
    -qualifying_domain_count,
    identity_key,                       # stable tie-breaker only
)
```

`WIKIPEDIA_RANK` has exactly two values: a `matching_page` mention hits
`continue` in the loop and never becomes a lead. If a third becomes necessary,
the loop's skip has changed and both must move together.

`insufficient_evidence` carries no `OUTCOME_RANK` entry, so ranking one raises
`KeyError` — deliberately, since a loud failure beats an arbitrary ordering.
Step 1 of selection filters them out; they are logged, never rendered.

Four terms, not the prior programme's nine (see A4).

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

The prior suite's 71,334 lines and its mutation-evidence protocol were
themselves complexity drivers, and are in scope for this rebuild.

- **Target roughly a 1:1 test-to-source ratio.**
- Real unit tests for the deterministic core: policy screening, ranking, digest
  rendering, cache keying, URL canonicalization, extraction.
- Provider-facing tests replay recorded cache fixtures from real runs.
- One end-to-end test drives the pipeline from a fixture cache to a digest.
- Live smoke tests are opt-in and marked, one per provider.

**The mutation-evidence protocol is dropped.** It was a rational response to a
71,000-line suite whose coverage claims could not be trusted. At this size it is
friction without a corresponding risk.

### Named invariant tests

These pin rules that are cheap to break and expensive to notice — every one
fails as missing output rather than as a crash. Each must fail if its rule is
removed.

| Test | Pins |
| --- | --- |
| **Namesake safety** | Two different people sharing a name, each with one qualifying domain, give two `possible_lead`s — never one `promising_lead`. A companion asserts the collapse is visible in the rendered note. |
| **Collapsing precedes the cut** | A run whose top `digest_size` leads all share one name still renders other people below them, not a one-entry digest. |
| **Suppression is applied** | An identity surfaced inside the resurface window does not reappear; one outside it does. |
| **A crash writes nothing** | A run interrupted before `store.commit` leaves all four tables untouched, and the re-run produces the same digest with zero new provider calls. |
| **A log failure cannot block state** | With `store.log` forced to raise, state is still committed and the digest still exists, so the next run progresses. |
| **Incomplete discards the whole item** | An item where mention three raises leaves no leads at all, is not settled, and never appears as `insufficient_evidence`. |
| **Poison items are abandoned** | An item raising every time stops being retried at `max_item_attempts` and makes no further provider calls. |
| **Feed freshness** | A second run past the feed TTL sees new items. |
| **Discovery calls expire** | A Brave or MediaWiki call past `discovery_ttl_seconds` re-requests rather than replays. |
| **Failures are not cached** | A transport failure followed by a success returns the success. |
| **Cache survives a truncated write** | A half-written entry is a miss and is refetched, not raised and not replayed. |

## Findings carried forward

`docs/findings.md` records the hard-won provider and model facts from the prior
programme. It is a constraints list, not a design authority. Read it before
touching schema construction or prompt rendering.

## Build order

Branch `mvp` off `main`. `main` remains the operational fallback;
`refactor/rearchitecture` is frozen reference and is not a base.

| Phase | Deliverable |
| --- | --- |
| 0 | Port assets, replace `CLAUDE.md`, write `docs/findings.md`, skeleton package |
| 1 | **Walking skeleton** — config, cache, http, llm, feeds, detect, digest, store: a real digest from live feeds, detection only |
| 2 | Wikipedia match |
| 3 | Coverage — Brave, policy screening, fetch, extract, assess |
| 4 | Ranking and the full digest |

Each phase ends with a live run, and that run's cache directory becomes the next
phase's fixture set. **Phase 1 is the phase to get right**; everything else
hangs off the skeleton.

## Guardrails

The prior failure mode was gradual accretion in which every individual step
looked justified. Crude limits work better than judgment. These belong in
`CLAUDE.md` as hard rules:

1. **Source stays under 3,000 lines.** Past that, something gets deleted or the
   feature does not land.
2. **No new table without deleting one.** Four is the budget.
3. **`pipeline.py`'s main loop fits on one screen.**
4. **No new cross-run state** without an explicit decision to leave MVP scope.

A limit that is never uncomfortable is not doing any work. When one binds, the
first response is to ask what can be removed.

## Out of scope

Deferred, in rough priority order:

1. **Durable person identity** — the intended first post-MVP feature.
2. `compose_lead_summary` — model-written `why_review` narrative.
3. Source reconnaissance for unclassified publishers.
4. Concurrency.
5. `notable status` and the `audit` command family. `sqlite3` serves.
6. Promptfoo evaluation suites.

Permanently out of scope: interactive review, acknowledgement, dismissal or
snoozing; multi-user assignment; multilingual research; cross-publisher
syndication inference; automated article drafting or editing.

## Acceptance

- `notable run` fetches feeds, detects people, checks English Wikipedia,
  researches coverage, and writes a dated digest plus `latest.md`;
- a crashed run re-runs to completion at near-zero provider cost;
- the spend cap produces a partial digest rather than a failure;
- source is under 3,000 lines and the main loop fits on one screen;
- **the fixed-corpus regression passes.** One recorded cache directory from a
  real ten-feed run is committed as a fixture, with an expected-results file
  naming each person the corpus should surface, their required outcome class,
  and the people that must *not* be surfaced. This is the pass/fail gate and it
  runs offline in seconds;
- the live ten-feed run remains a qualitative smoke check against the prior
  rewrite's figures (5 entries from 246 items at $1.08) for cost and order of
  magnitude, not pass/fail; and
- no code path creates, drafts, or edits Wikipedia content.

---

## Appendix A: rejected alternatives

Recorded once, so the body states rules instead of arguing for them. Each was
considered and rejected on the reasoning given.

**A1. Retrying a validation failure within the run.** Rejected on cost. A retry
re-sends identical input; sampling means it *might* land differently, but the
prior programme measured twelve such retries across six people with zero
recoveries. Paying repeatedly for a call that has demonstrated it produces
invalid output is a bad trade. (The stronger claim — that identical input
*cannot* succeed — is false for sampled decoding and is not the justification.)

**A2. Rendering the digest from all unsurfaced leads across runs.** Drafted, and
rejected: it is a persistent digest queue with none of a queue's semantics.
Leads below the cutoff pend forever; old leads become eligible again after the
resurface window and recur indefinitely; `possible_lead` entries starve behind a
steady supply of `promising_lead` ones. The prior programme built a real queue
with a real starvation guard for exactly this, at real expense. Backlog delivery
is a product feature, not a side effect of a render filter — if wanted, it gets
its own design.

**A3. The fourth `possible_lead` reason.** The prior programme also admitted
"at least one transferable attention signal". Dropped: the recall it adds over
the three retained reasons is narrow — an article that is *not*
`content_qualifying` (a listing or announcement) but carries an award or
major-show signal, which is weak evidence. Reasons two and three already catch
the substantive cases. Reversible by querying `insufficient_evidence` leads
whose `detail_json` carries a transferable signal.

**A4. The prior nine-term rank tuple.** Two terms (starvation guard,
eligibility reason) die with the digest queue. Three more —
`positive_signal_count`, evidence visibility, article freshness — each demanded
that another consumed field be threaded from assessment through to ranking, and
all three only order the *tail* of a shortlist of a handful of entries. They
order nothing a reader would notice.

**A5. Salting `identity_key` to avoid namesake collisions.** Rejected: it would
defeat cross-run suppression entirely, and telling two same-named people apart
*is* durable identity, which is deferred. The collision is instead made visible
in the rendered entry.

**A6. Deleting `run` and `lead` outright.** Considered, since they are unread by
the pipeline. Retained because tuning recall is done by querying stored leads,
and the alternative is another paid run per question. Made safe by writing them
outside the state transaction, best-effort.
