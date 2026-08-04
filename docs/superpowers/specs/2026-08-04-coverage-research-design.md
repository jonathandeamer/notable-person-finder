# Phase 3: Coverage Research

**Status:** Approved
**Date:** 2026-08-04
**Branch:** `feat/wikipedia-identity-match` (or its own follow-on branch)
**Authority:** subordinate to
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. That document is
the single architectural authority for the MVP; this note fills in the detail
it deliberately left to the phase that implements it (`coverage.py ~350`,
"brave -> screen -> fetch -> extract -> assess"; `policy.py ~90`, "publisher
policy TOML -> eligible/ineligible/unclassified").

## Goal

Given one mention whose Wikipedia verdict is `no_matching_page` or
`uncertain` (a `matching_page` verdict already short-circuited the pipeline
loop before reaching here), search for English-language coverage, screen it
against publisher policy, fetch and assess the articles worth assessing, and
return the evidence Phase 4's `rank.assess` will consume:

```python
articles = coverage.research(mention, config, transport, llm)
item_entries.append(rank.assess(mention, verdict, articles, config))
```

Phase 3 does not compute a lead outcome. `rank.assess` and the lead-outcome
rules (qualifying article, `promising_lead`/`possible_lead`/
`insufficient_evidence`) are Phase 4. This phase's job ends at producing a
tuple of per-article assessments — full prompt output stored, nothing
consumed yet, per the master spec's "consume less than the prompt emits"
discipline.

## Data flow: one mention

At most one Brave call, up to `max_articles_per_mention` fetch+extract
attempts, and at most that many `assess_article` model calls:

1. **Search.** One Brave Web Search request, query = the mention's exact
   name, bounded to `max_search_results` results.
2. **Screen.** Every result URL goes through `policy.py` →
   `curated_eligible | curated_ineligible | unclassified`.
   **`curated_ineligible` results are dropped here — never fetched, never
   assessed.** Ineligible sources in the ported policy (`config/source_
   policies/visual_arts.toml`) are exclusively social media, UGC platforms,
   and PR-wire distributors — content that cannot satisfy
   `editorially_independent` under any circumstance, so paying for a fetch
   and a model call on it is pure waste, not caution.
3. **Bound.** Cap the screened (`curated_eligible` + `unclassified`) set at
   `max_articles_per_mention` before any fetch happens — an independent cost
   control from screening.
4. **Fetch + extract, per article.** One HTTP GET via the existing
   `Transport` (`provider="article"`), followed by Trafilatura extraction
   (pure, network-free, runs in-process after a successful fetch).
   **A single article's failure — 404, paywall, non-HTML content-type, a
   response over the size limit, or Trafilatura extracting nothing usable —
   drops just that article from the evidence set and moves on to the next.**
   It is not a `ProviderFailure` and does not raise `Incomplete`: a dead
   search-result link is not evidence of a technical failure in *this*
   system, and Brave's index rots constantly. Failed fetches are never
   cached (never a validated success).
5. **Assess, per surviving article.** One `assess_article` model call per
   (mention, article) pair — the same one-focused-decision-per-call
   discipline as `detect_people` and `match_wikipedia_identity`.
6. **Return.** A tuple of `ArticleAssessment` results: the full
   `assess_article` output (identity, depth, content types, subject
   relationship, attention/caution signals, rationale) plus the
   already-known screening status attached by `coverage.py` itself — **the
   model never re-derives screening status; the prompt is explicit that
   "screening state is an input, not a verdict to invent."**

### Empty-candidate short-circuit

If the surviving article set is empty after step 3 or 4 — zero Brave
results, every result was `curated_ineligible`, or every fetch/extract
attempt failed — `coverage.research` returns `()` immediately, making
**zero** `assess_article` calls. This is a deterministic short-circuit, not
`Incomplete`: an empty coverage result is not a technical failure, it is
exactly what "the search genuinely ran and returned nothing usable" means,
and Phase 4's `rank.assess` evaluates zero articles as `insufficient_evidence`
by construction (an outcome computed over an empty evidence set, not a
special case `coverage.py` has to name). This mirrors Phase 2's
empty-candidate short-circuit for the same reason: a call that cannot
possibly say anything useful is not made.

### What still raises `Incomplete`, whole-mention

- The Brave search call itself failing (`ProviderFailure`) — nothing to
  screen without it.
- Any `assess_article` model call failing (`ProviderFailure`) or its output
  being domain-rejected (`AssessInvalid`) — same as `detect.py`/`wiki.py`.

`BudgetExceeded` is never caught here; it propagates to end the whole pass,
exactly as in every other module.

## `policy.py` (~90 lines)

Loads `config/source_policies/visual_arts.toml` once, module-level cached
like `detect.py`'s prompt loading. First-match-wins in file order over
`host_suffix` — the only match form the ported file uses; `host_exact` and
`path_prefix` are not implemented, since nothing needs them yet and adding
unused matching modes would be paying for generality before it is asked for.
No match ⇒ `unclassified`.

Two functions:

- `classify(url: str) -> Literal["curated_eligible", "curated_ineligible", "unclassified"]`
- `canonical_domain(url: str) -> str` — same-host collapsing for Phase 4's
  two-distinct-domain `promising_lead` threshold. Lives here because it is a
  policy-shaped concern (what counts as "the same publisher"), not a ranking
  one.

## `coverage_contract.py`

Pure models, wire schema, domain validation — mirrors the existing
`wiki_contract.py`/`detect_contract.py` split.

- `ArticlePassage`: one article's extracted text, bounded by
  `coverage.max_article_characters` and truncation-flagged exactly like
  `build_passages`, with a local id for grounding (an article is one
  passage, unlike detection's title+summary split, since Trafilatura already
  returns one clean body).
- Wire schema for `assess_article`, porting the prompt's fields verbatim:
  - `person_relation`: `same_person | different_person | uncertain`
  - `coverage_depth`: `significant | passing | uncertain`
  - `content_types`: 1–3 of the closed set (`reporting`, `profile`,
    `review`, `interview`, `obituary`, `listing`, `announcement`,
    `press_release`, `sponsored`, `other`). `minItems`/`maxItems`/
    `uniqueItems` are schema-expressible directly, unlike detection's
    cross-field rules — express them there per `docs/findings.md`'s
    "express every validator rule the schema can carry," not as a
    post-hoc validator check
  - `subject_relationship`: `editorially_independent | affiliated |
    self_published | uncertain`
  - attention/caution signals — the same nested-union schema shape
    `detect_contract.py` already established for exactly this pairing
  - rationale, cited passage ids
- `validate_assessment(raw, *, passages, ...) -> AssessmentOutput`: passage-id
  grounding (the same `_check_references` pattern) and signal category/kind
  pairing — the schema-unexpressible rules, same discipline as the other two
  contracts.
- `ArticleAssessment`: the validated model output plus `screening_status`
  (attached by `coverage.py`, not the model) and the source URL — this is
  the full record Phase 4 will read from.

## Config additions

```toml
[brave]
endpoint = "https://api.search.brave.com/res/v1/web/search"
max_search_results = 10
max_articles_per_mention = 5
# extra_snippets is a paid-plan parameter -- omit it (docs/findings.md).

[tasks.assess_article]
model = "openai/gpt-5.4-mini"
max_completion_tokens = 4096
reasoning_effort = "low"

[coverage]
max_article_characters = 6000
```

`[tasks.assess_article]` reuses Phase 2's `ModelTaskConfig` base — no new
task-config type needed. `max_search_results` (10) and
`max_articles_per_mention` (5) are both deliberately smaller than the frozen
system's defaults (20 and effectively unbounded): this is a small pilot
corpus, not a production crawl, and both are plain config ints, tunable
without a code change if real runs show them too tight.

## Caching

| Call | Provider key | TTL class |
| --- | --- | --- |
| Brave search | `brave` | Discovery (`discovery_ttl_seconds`, 24h) |
| Article fetch | `article` | Stable (permanent) |
| `assess_article` | `openrouter` | Stable (permanent, `defer_cache` until validated) |

Brave is Discovery for the same reason MediaWiki search is: a person who
later gains real coverage must not be judged forever on a stale snapshot.
Article fetch and the model call are Stable per the master spec's TTL table
— an already-published article's content and an already-validated model
judgment about it don't need re-checking. **This means fetched article HTML
accumulates on local disk indefinitely** — an explicit, accepted tradeoff
matching the master spec's existing TTL contract, not a new decision this
phase introduces. No redistribution risk (cache never leaves local disk); if
disk growth ever becomes a real problem, that is a separate future decision
about cache pruning, not a reason to weaken this phase's caching contract.

## `http.py` addition

Article fetch needs one new fixed transport rule, additive to the existing
contract: **bound response size and require an HTML-ish content-type before
attempting extraction.**

**The size bound must not trust `Content-Length`.** Many servers omit it or
lie, and the frozen `refactor/rearchitecture` system's own transport layer
already had to solve exactly this (`providers/transport.py`'s
`ResponseLimit`). `Transport.request` gains an optional `max_bytes:
int | None = None` parameter (default `None`, meaning every existing
provider is unaffected); when set, `_send` reads the response as a stream
and aborts once the running byte count exceeds the limit, rather than
downloading the full body first and checking its length afterward. An
aborted-for-size stream is treated as a normal fetch failure — for article
fetch specifically, that means "drop this article," not a transport-level
crash. `coverage.py` passes `max_bytes` and reads the response's
`Content-Type` header before invoking Trafilatura; a non-HTML content-type
is the same "drop this article" outcome, checked before extraction is
attempted rather than left to Trafilatura to fail on.

**Failed article fetches are not cached** — a 404, paywall response, or a
size/content-type rejection is never stored, per the shared cache contract's
"only validated successes are cached" rule (`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`).
This was raised explicitly during review as a real cost: a dead URL that
Brave keeps returning gets refetched on every retry of the mention that
discovered it. The exposure is bounded and accepted rather than fixed: a
dead article never makes its *mention* `Incomplete` (see the empty-candidate
short-circuit above and the per-article-failure rule below), so an item that
settles is never revisited regardless of the dead URL; the same URL can only
be refetched if that item retries for an unrelated whole-mention failure,
capped at `max_item_attempts` (3). HTTP fetches cost no money — only model
calls do — so the waste is bounded network time, not spend. A provider-scoped
exception to the shared cache-failure contract (caching 4xx article
responses) was considered and rejected: it would fragment one caching rule
into a per-provider question and contradicts the master spec's explicit
"never cache failures" invariant for a saving that is not measured against
any real run yet.

## Testing

Named invariant tests for this phase:

| Test | Pins |
| --- | --- |
| **Ineligible is never fetched** | A Brave result on a `curated_ineligible` host produces zero HTTP fetch calls. |
| **A dead link doesn't fail the mention** | One article 404s, another succeeds — the mention still returns the successful article's assessment, not `Incomplete`. |
| **Brave failure fails the mention** | A `ProviderFailure` from Brave search raises `Incomplete`, no fetch/assess calls made. |
| **Screening status is carried, not re-derived** | The returned assessment's screening status matches `policy.classify`'s output exactly, regardless of what the model says. |
| **`content_types` is deduplicated and bounded** | 1–3 values, no duplicates; a 4th or a duplicate is unrepresentable in the schema, not merely rejected after payment. |
| **Article cache is permanent** | A second run past `discovery_ttl_seconds` still replays a cached article fetch. |
| **Brave cache expires** | A Brave call past `discovery_ttl_seconds` re-requests rather than replays. |
| **`max_articles_per_mention` bounds fetch count** | More screened results than the cap still fetches only the cap's worth. |
| **Oversized or non-HTML responses are dropped, not crashed on** | A response failing the size/content-type check is treated as a failed fetch for that one article, not raised. |
| **A stream over `max_bytes` aborts before full download** | Enforcement does not depend on `Content-Length`: a response streaming past the limit is caught mid-transfer, not after buffering the whole body. |
| **Zero surviving articles short-circuits to an empty tuple** | Zero Brave results, all-ineligible screening, or every fetch/extract failing all return `()` with zero `assess_article` calls, never `Incomplete`. |
| **Failed article fetches are never cached** | A 404 followed by a retry of the same mention re-requests the same URL rather than replaying a cached failure. |

Provider-facing tests replay recorded cache fixtures, per the master spec.
Trafilatura extraction tests use small fixture HTML strings, not live
fetches. This phase ends with a live run over real feeds — Brave and article
fetch are new external surfaces findings.md has no data on yet — inspected
per the master spec's pre-promotion checklist before that cache becomes the
Phase 4 fixture set.

## Out of scope for this phase

Ranking, the lead-outcome decision table, the digest shortlist, and
`rank.assess` are Phase 4. Multi-query Brave variants, publisher-specific
scrapers, paywall bypass, and robots.txt handling are permanently out of
scope per the master spec's guardrails and Appendix A precedent (the same
minimalism that rejected multi-form Wikipedia queries in Phase 2 applies
here).
