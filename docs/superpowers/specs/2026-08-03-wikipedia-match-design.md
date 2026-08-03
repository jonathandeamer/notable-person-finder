# Phase 2: Wikipedia Identity Match

**Status:** Approved
**Date:** 2026-08-03
**Branch:** `mvp`
**Authority:** subordinate to
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. That document is
the single architectural authority for the MVP; this note only fills in the
detail it deliberately left to the phase that implements it (`wiki.py ~250`,
"search + facts, one wave -> match verdict").

## Goal

Given one detected, research-worthy mention, decide whether they already have
a current English Wikipedia biography. Feeds `pipeline.py`'s loop:

```python
verdict = wiki.match(mention, config, transport, llm)
if verdict.has_page:
    continue
```

`matching_page` stops the mention here — no coverage research, no lead. Both
`no_matching_page` and `uncertain` continue to coverage research (Phase 3);
neither may suppress a candidate, per the master spec's high-recall rule.

This phase reuses `refactor/rearchitecture`'s product reasoning (query
construction, namespace/disambiguation filtering, the "no `no_matching_page`
on truncation" safety rule) but not its machinery. There is no durable plan,
no multi-hop redirect chase, no accent-fallback query variant, no refresh
interval, no per-person state of any kind. A crash replays from cache, exactly
like every other call in this codebase.

## Data flow: what "one wave" means

One `wiki.match` call performs at most two MediaWiki HTTP calls and at most
one model call:

1. **Search.** `search_pages(query=mention.exact_name, limit=max_candidates)`
   — one `list=search` request via the MediaWiki Action API.
2. **Facts.** `get_page_facts(page_ids=<search hits>)` — one batched request
   for namespace, disambiguation flag, redirect target, description, extract,
   and categories for every hit.
3. **Filter.** Keep only main-namespace (`ns == 0`), non-disambiguation
   pages. Anything left that is itself a redirect is collected as a set of
   redirect targets.
4. **One bounded redirect-resolution pass.** If that set is non-empty, one
   more `get_page_facts` batch resolves those targets to their terminal
   pages. Those terminal pages pass through the **same** namespace/
   disambiguation filter as step 3 before they may replace the redirect
   entries as candidates — a redirect can terminate outside the main
   namespace or on a disambiguation page, and that is exactly as
   disqualifying as a search hit landing there directly. This is a single
   extra wave, not a loop: a target that is itself a redirect (a second hop)
   is dropped rather than chased. This is a rare case and the cache means it
   costs nothing to pick up correctly on a later run if it ever matters for a
   real mention.
5. **Assemble.** The final candidate list is whatever survives steps 3–4,
   each with a title, description, extract, and categories.

At most three HTTP calls total (search, facts, redirect-resolution facts),
never more, and never a loop bound by anything other than "did step 3 produce
any redirects."

### Truncation

MediaWiki's search response reports whether more results existed beyond what
was returned (`continue`/`sroffset` present). When that flag is set, the wave
is `truncated`.

**A `truncated` wave with at least one candidate may never produce
`no_matching_page`.** The candidate universe the model saw was incomplete, so
"no page exists" would be a false negative manufactured by a config knob
(`max_candidates`) rather than by evidence. This is enforced in the domain
validator, not the prompt: if the model returns `no_matching_page` on a
truncated wave, that is `MatchInvalid` and the mention becomes `Incomplete` —
not silently coerced to `uncertain`, because coercing a judgment the model
wasn't actually asked to make is worse than retrying on a later run against a
possibly-different (or reconfigured) candidate set. `matching_page` and
`uncertain` are unaffected by truncation. (Zero candidates and truncated is
handled before any model call exists to validate — see the short-circuit
above.)

### Empty-candidate short-circuit

Zero candidates after step 5, **and not truncated**, yields `no_matching_page`
deterministically, with no model call. This mirrors `detect.py`'s handling of
an item with no usable passages: a call that cannot possibly say anything
useful is not made.

**Zero candidates *and* truncated is a second, distinct deterministic
short-circuit: `uncertain`, also with no model call.** This is not the same
case as above and must not resolve to `no_matching_page`. It is also not left
to the model: with zero candidates, `matching_page` is unreachable
(`selected_page_id` would have to name a candidate that doesn't exist), and
`no_matching_page` is forbidden by the truncation rule below — so the model's
only valid answer is already `uncertain` before the prompt is built, and
sending the call would only spend money to confirm what the validator already
knows. Nor is it `Incomplete`: the search response is cached for
`discovery_ttl_seconds`, so a later run would replay the identical truncated
result and hit the identical dead end, burning the item's attempt cap until
it is abandoned — silently dropping a mention that never got the chance to
reach coverage research. `uncertain` is the honest label ("evidence
insufficient to decide") and, under the master spec's high-recall rule,
continues to coverage research exactly as a non-empty `uncertain` would.

## Contract: `wiki_contract.py`

Pure models, wire schema, and domain validation — no I/O — mirroring the
existing split between `detect.py` (orchestration) and `detect_contract.py`
(contract).

- `Candidate`: `page_id`, `title`, `description`, `extract` (bounded by
  `mediawiki.max_extract_characters`, truncation flagged the same way
  `build_passages` flags it), `categories` (bounded by
  `mediawiki.max_categories_per_page`). Each candidate is given a local id
  (`c1`, `c2`, ...) the way passages get `p1`/`p2`, so the model's cited
  supporting/conflicting facts are checkable against a known id, the same
  grounding pattern as `_check_references`.
- Wire schema for `match_wikipedia_identity`: plain object root (a root-level
  `anyOf` is rejected under strict mode per `docs/findings.md`). The
  `matching_page` / `selected_page_id` pairing — "set together, or both
  null" — cannot be expressed in-schema for the same reason detection's
  `overflow`/`item_outcome` pairing can't: it relates two root-level
  properties, and strict mode has no `if`/`then`. It is a validator rule.
- `validate_match(raw, *, candidates, truncated) -> MatchOutput`:
  - `selected_page_id` is set if and only if `outcome == "matching_page"`,
    and when set must be one of the supplied candidate ids;
  - every cited fact id (supporting or conflicting) must be a known
    candidate-fact id;
  - `outcome == "no_matching_page"` and `truncated` together is
    `MatchInvalid`;
  - name equality alone is never sufficient — this is a prompt instruction,
    not a machine-checkable rule, and is not re-validated in code.
- `MatchVerdict`: `outcome`, `selected_page_id`, `rationale`, plus
  `has_page` as a property (`outcome == "matching_page"`) so `pipeline.py`
  never compares strings.

## Config

`DetectConfig`'s model-call fields (`model`, `max_completion_tokens`,
`reasoning_effort`) are factored into a shared base so `match_wikipedia_identity`
can reuse them without inheriting detection-only fields (`max_people`,
`max_title_characters`, `max_summary_characters`). `tasks` stops being
`dict[str, DetectConfig]` and each task is validated against its own model;
`load_config` extracts `match_wikipedia_identity` the same way it already
extracts `detect_people`, and fails the same way if the section is missing.

New `[mediawiki]` section:

```toml
[mediawiki]
endpoint = "https://en.wikipedia.org/w/api.php"
max_candidates = 15
max_extract_characters = 1200
max_categories_per_page = 20
```

`max_candidates` defaults to 15, not the frozen system's 8: `docs/findings.md`
records that 8 pushed 111 of 118 plans into truncation, and the fingerprint
coupling that blocked raising it there does not exist here. It remains a
plain config int, tunable without a code change.

## Error handling

Unchanged shape from `detect.py`: `ProviderFailure` from either MediaWiki call
or the model call, and `MatchInvalid` from the domain validator, both become
`Incomplete` — the mention's item is retried on a later run, never converted
into a semantic outcome. Nothing here introduces a new exception type;
`Incomplete`, `ProviderFailure`, and `BudgetExceeded` (propagated, not caught
here) are the same three from `errors.py`.

## Caching

Both MediaWiki calls go through the existing `Transport`, `provider="mediawiki"`,
`ttl_seconds=config.cache.discovery_ttl_seconds` — the Discovery class from the
master spec (default 24h), not permanent. A person who gains a page after
being checked once must not be judged forever on a stale miss. The model call
reuses `llm.structured` exactly as `detect.py` does: `ttl_seconds=None` with
`defer_cache`, committed only after `validate_match` passes, keyed additionally
on model id and schema.

No new durable state. `wiki.match` reads and writes nothing in `store.py`;
the master spec's four-table budget is untouched.

## Testing

Named invariant tests for this phase, in the same spirit as the master spec's
table — each pins a rule that is cheap to break and expensive to notice:

| Test | Pins |
| --- | --- |
| **Namespace/dab filtering** | A disambiguation page and a non-main-namespace hit are never candidates. |
| **Truncation blocks the negative** | A truncated search followed by model output `no_matching_page` is rejected (`Incomplete`), never silently coerced to `uncertain`. |
| **Empty search short-circuits to `no_matching_page`** | Zero candidates, not truncated, yields `no_matching_page` with zero model calls. |
| **Empty-and-truncated short-circuits to `uncertain`** | Zero candidates *with* truncation yields `uncertain` with zero model calls — never `no_matching_page`, never `Incomplete`. |
| **One redirect hop resolves** | A search hit that is a redirect is replaced by its namespace/dab-filtered terminal page as a candidate. |
| **A second hop is dropped, not chased** | A redirect-to-a-redirect does not produce a third HTTP call and does not appear as a candidate. |
| **A redirect terminating off-namespace or on a dab page is discarded** | The redirect-resolution pass applies the same namespace/disambiguation filter as the initial search hits; a terminal page that fails it does not become a candidate. |
| **`selected_page_id` pairing** | `matching_page` without a `selected_page_id`, or any other outcome with one set, is `MatchInvalid`. |
| **Fact-id grounding** | A cited supporting or conflicting fact id not among the supplied candidates' ids is `MatchInvalid`. |
| **Discovery TTL governs MediaWiki** | A MediaWiki call past `discovery_ttl_seconds` re-requests rather than replays; within it, replays. |
| **`matching_page` short-circuits the pipeline loop** | `pipeline.run` never calls coverage research for a mention whose verdict is `matching_page`; both `no_matching_page` and `uncertain` do. |

Provider-facing tests replay recorded cache fixtures, per the master spec.
This phase ends with a live run over the Phase 1 fixture corpus, inspected
per the master spec's pre-promotion checklist (raw responses, validation
failures, cache hit/miss counts on replay, spend, pacing) before that run's
cache becomes the Phase 3 fixture set.

## Out of scope for this phase

Everything the master spec already defers stays deferred: durable Wikipedia
state, refresh intervals, multi-model inspection, merge reconciliation
(there is no durable person to merge), accent-fallback query variants, and any
notion of a "plan." If a mention needs re-checking, the next run does it
again — bounded by the discovery TTL, not by a schedule.
