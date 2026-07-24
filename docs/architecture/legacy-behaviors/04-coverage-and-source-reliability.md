# Legacy Behavior Review: Coverage Discovery and Source Reliability

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability finds English-language coverage about a person who does not
have a confirmed matching English Wikipedia page. It retrieves a bounded set
of results, applies inspectable publisher rules, prepares relevant article
content, and returns a structured evidence set. It does not decide whether the
person is notable.

Primary legacy evidence:

- `scripts/det_brave_coverage.py`
- `scripts/det_gate4_reliable_filter.py`
- `scripts/llm_gate4b_runner.py`
- `prompts/gate4b_about.md`
- `prompts/gate4b_source.md`
- `tests/test_brave_coverage.py`
- `tests/test_gate4_reliable_filter.py`
- `tests/test_gate4b_runner.py`

## Search Boundary

Use Brave Web Search directly. Do not cascade through Brave News first: most
people will not have enough plausible News results, so the cascade would add
requests and branching without a useful product distinction. The provider
interface may support other search products later if evidence justifies them.

The initial search plan is deterministic and finite:

1. search the exact, quoted source-written name;
2. search known aliases only when they have recorded provenance; and
3. add at most one contextual query, derived from supplied profession,
   location, or career facts, when earlier results do not meet a configurable
   retrieval target.

An obituary modifier is allowed only when the source material explicitly
supports it. Do not shorten multipart names automatically, accept silent
provider spelling changes, let a model invent aliases, or let an agent decide
which search to run next. Query count, results, pages, requests, elapsed time,
and cost are configurable bounds. An exhausted bound is recorded as an
incomplete search, not silently treated as absence of coverage.

Search is global, restricted to English-language results, with moderate
SafeSearch for the initial version. Multilingual research is deferred.

## Publisher Screening

Publisher handling has three deterministic states:

- **curated eligible:** worth retrieving and assessing, but not automatically
  valid notability evidence;
- **curated ineligible:** an obvious poor fit such as social media, user-
  generated content, press-release distribution, or promotional material; and
- **unclassified:** neither accepted nor rejected by the maintained rules.

Rules may address a domain, subdomain, simple path, or publisher alias. Every
curated decision records its rationale, decision basis, source URL, review
date, and the exact Wikipedia Reliable Sources/Perennial Sources revision when
that page informed the decision. The policy is manually reviewed and versioned
independently; it is never silently synchronized at runtime. Its eventual
representation as configuration, seed data, or database records is deferred to
the domain and persistence design.

The application will not import the full Perennial Sources table or implement
its contextual caveats as a policy engine. That page is non-exhaustive,
context-sensitive, and mutable. It informs a small local set of clear
decisions; an absent or complicated publisher remains unclassified. All ten
pilot publishers are initially curated eligible for investigation. Reuters
and AP are also suitable eligible evidence publishers when search discovers
their work, although neither supplies a public arts feed for the pilot.

A model must not declare an unclassified publisher “Wikipedia reliable.” A
capped set of unclassified results may be retrieved as fallback leads, but
they remain visibly provisional for human review. Repeatedly useful publishers
can be researched and curated manually.

## Result Selection and Retrieval

Selection is deterministic:

1. include the original discovery article on the same terms as other evidence;
2. select curated-eligible results by query stage and original Brave rank;
3. if that set does not meet a configurable retrieval target, select a
   configurable number of unclassified results by the same ordering; and
4. retain curated-ineligible results and their disposition as metadata without
   fetching their article bodies.

Do not ask a model which URLs to fetch or synthesize a global rank after
combining queries. Preserve the originating query, original rank, provider
identifiers, title, snippets, extra snippets, URL, retrieval time, and cache
state for every result. Deduplicate only conservatively canonicalized identical
URLs; retain every query occurrence and its rank.

After screening, attempt ordinary retrieval of selected articles with one
generic extractor. Do not add publisher-specific scrapers or circumvent
authentication, paywalls, robots controls, or anti-bot measures. Fall back to
the Brave title and snippets when access or extraction is incomplete, and
record whether the available view was full text, partial text, or snippets.

Content preparation is deterministic. Extract title, dek, byline, date,
editorial labels, and main body; remove navigation, footers, cookie notices,
comments, related or trending links, boilerplate, and unrelated captions.
Supply the model with the lead, paragraphs containing the name or supported
aliases, and limited surrounding context under an explicit token bound. Never
send raw HTML. A doubtful extraction is marked partial and may fall back to
snippets rather than being presented as complete text.

## Semantic Article Assessment

The model assesses only irreducibly semantic properties of the supplied
article view:

- whether it concerns the same, a different, or an uncertain person;
- whether coverage is significant, passing, or uncertain;
- whether the content is reporting, a profile, review, interview, obituary,
  listing, announcement, press release, sponsored material, or other; and
- whether it is independent of, affiliated with, self-published by, or
  uncertain with respect to the subject.

Each judgment includes a grounded rationale and full attempt provenance. Code
owns source rules, query construction, selection, retrieval state, limits,
schema validation, and workflow transitions. The model does not infer what it
was not shown.

The capability performs no cross-source independence clustering. That behavior
was primarily useful for overlapping prototype feeds and is disproportionate
for this human-reviewed pilot. Only identical canonical URLs are deduplicated.
The output may say that multiple qualifying coverage leads were found; it must
not claim that a numeric count proves multiple independent reliable sources.

## Output Contract

Return all normalized results with dispositions and reasons, article views,
semantic assessments, unresolved items, and search-completeness metadata.
Retain rejected and inaccessible results for audit rather than physically
dropping them.

This capability does not emit `likely_notable`, `possibly_notable`, or
`not_enough_evidence`. The notability-assessment capability applies
configurable recommendation policy to this evidence set. Operational failures
remain distinct from semantic uncertainty.

Search caches use a finite configurable lifetime. Refresh after the interval
or when materially new discovery evidence warrants it, merge newly found
evidence without erasing prior observations, and retain query, time, and cache
provenance.

## Approved Dispositions

| Observable behavior | Decision and rationale | Replacement verification |
| --- | --- | --- |
| Brave News is searched before other research. | **Change.** Use Brave Web Search directly; a News-first cascade is unlikely to help the expected sparse cases. | Search-adapter contract tests |
| Exact-name, shortened-name, and obituary queries are generated. | **Change.** Use exact names, provenance-backed aliases, one grounded context query, and evidence-backed obituary modifiers only. | Table-driven query-plan tests |
| The provider may silently correct a person's name. | **Delete.** Disable correction where supported or reject and record an altered query unless an explicit retry permits it. | Provider-response tests |
| Results from several queries are merged, reranked, and stripped of query origin. | **Delete.** Retain query occurrence and provider rank; deterministic selection replaces synthetic ranking. | Persistence and ordering tests |
| A hardcoded domain list drops all other publishers. | **Change.** Use small, maintained eligible and ineligible sets plus a capped unclassified fallback; retain every disposition. | Source-rule tests |
| Wikipedia's Perennial Sources page functions as a pre-approved allowlist. | **Delete.** Use it only as dated provenance for deliberate local decisions. | Policy provenance tests |
| An LLM decides whether an unknown publisher is reliable. | **Delete.** Unknown publishers remain provisional until human curation. | Prompt and workflow tests |
| The discovery article is excluded from corroborating evidence. | **Delete.** Assess it on the same terms as every other article. | Evidence-set tests |
| Models judge subject focus and source relationship from snippets or retrieved text. | **Preserve and clarify.** Split typed semantic judgments and record exactly what the model saw. | Promptfoo and schema tests |
| Distinct domains and inferred syndication clusters count as independent sources. | **Delete.** Do not automate cross-source independence or make that claim from a count. | Output-language and duplicate-URL tests |
| A fixed source count directly emits notability labels. | **Delete from this capability.** Return evidence; apply recommendation policy in capability 5. | Workflow-boundary tests |
| Search caches can live forever. | **Change.** Use configurable finite refresh and retain old observations when merging new results. | Clock-controlled cache tests |
| Article pages are publisher-specifically scraped. | **Delete.** Use one generic, access-respecting extractor with explicit partial/snippet fallback. | Extractor fixture tests |

## Verification Boundary

Pytest covers query construction, spelling changes, search bounds, language and
SafeSearch parameters, source-rule precedence, result ordering, URL
deduplication, caching, article-view states, content cleaning, token bounds,
schema validation, and complete versus incomplete searches. Promptfoo covers
same-name collisions, significant versus passing coverage, interviews and
reviews, press releases and sponsored content, affiliation, sparse snippets,
partial extraction, and obvious duplicate coverage that must not trigger an
overconfident recommendation later.
