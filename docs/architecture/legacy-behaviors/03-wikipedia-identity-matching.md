# Legacy Behavior Review: Wikipedia Identity Matching

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability determines whether an English Wikipedia article describes the
same person discovered in a source item. MediaWiki supplies current candidate
pages and stable identifiers. Deterministic code owns retrieval and mechanical
processing; a bounded model is used only for semantic identity comparison.

Primary legacy evidence:

- `scripts/det_mw_candidates.py`
- `scripts/det_gate2_has_page.py`
- `scripts/llm_gate3_runner.py`
- `scripts/det_gate2_index_update.py`
- `scripts/det_gate3_index_update.py`
- `prompts/gate3.md`
- `tests/test_mw_candidates.py`
- `tests/test_gate2_has_page.py`
- `tests/test_gate2_index_update.py`
- `tests/test_gate3_runner.py`
- Gate 2 and Gate 3 assertions in `tests/test_smoke_pipeline.py`

## Deterministic and Model Boundary

Use deterministic code whenever an answer follows from explicit provider data.
Use a model only for semantic equivalence that code cannot establish safely.

Deterministic code:

- normalizes whitespace, punctuation, honorifics, and mechanically derived
  query forms;
- queries MediaWiki, follows redirects, filters namespaces, identifies
  disambiguation pages, and deduplicates by page ID;
- retrieves titles, descriptions, lead extracts, categories, URLs, and stable
  identifiers;
- validates that any model-selected page came from the supplied candidate set;
- reuses a previously established mapping for the same person entity;
- applies search bounds, caching, retries, and workflow transitions; and
- records a no-match observation after a successful, complete bounded search
  finds no plausible candidates.

The model:

- compares accumulated source context with plausible Wikipedia candidates;
- evaluates whether supplied facts such as profession, nationality, era,
  location, career, and associated work identify the same person; and
- returns a grounded match, non-match, or genuine ambiguity.

The model does not search Wikipedia, invent aliases, select tools or control
flow, interpret provider failures, or promote a name-only match.

## Outcome Semantics

### `matching_page_found`

The system has sufficient supplied evidence that a particular English
Wikipedia page describes the same person. The outcome requires:

- a stable MediaWiki page ID from the retrieved candidate set;
- either a previously established mapping to the same person entity or a
  validated semantic comparison;
- positive biographical alignment rather than name equality alone; and
- the decision basis, supporting facts, decision time, and complete model
  provenance where a model was used.

Coverage research stops for that candidate because the product is looking for
missing biographies. The outcome is named `matching_page_found` rather than
`confirmed_existing_page` because the latter could imply human verification.

### `no_matching_page_found`

This is a time-stamped observation about a completed search, not a permanent
claim that Wikipedia lacks an article. It requires:

- successful MediaWiki requests using every configured, text-grounded query;
- a candidate set that was not truncated in a way that invalidates a negative
  conclusion;
- no remaining plausible biography candidate after deterministic processing
  and any required semantic comparison; and
- no provider, parsing, or validation failure.

The person proceeds to coverage research. The observation retains its query
forms, result page IDs, time, and decision basis. It can be refreshed when
materially new evidence arrives, before resurfacing after a configured
interval, or after an earlier inconclusive search.

### `uncertain_identity`

A plausible candidate page remains, but the supplied evidence cannot safely
establish or reject identity. When configured and affordable, the workflow may
escalate once to a stronger model. If uncertainty remains:

1. coverage research continues with the identity warning attached;
2. newly collected coverage may support one later identity reconsideration;
3. the person is never classified as already covered merely to resolve the
   ambiguity; and
4. otherwise strong results may be surfaced as “Wikipedia identity unresolved”
   below equivalent candidates with a completed no-match observation.

Provider errors, unsafe truncation, invalid structured output, and inadequate
source identity are not negative page findings. They are operational failures
or uncertainty, as appropriate.

## Approved Dispositions

| Observable behavior | Legacy evidence | Decision and rationale | Replacement verification |
| --- | --- | --- | --- |
| Current English Wikipedia data, rather than general model knowledge, supplies candidate pages. | MediaWiki search and page-detail requests | **Preserve.** Existing-biography checks require current, inspectable evidence. | Adapter and workflow tests |
| Deterministic code retrieves candidates and a model compares ambiguous identities. | Gate 2 and Gate 3 division | **Preserve and clarify.** Apply the boundary above; models perform only irreducible semantic comparison. | Unit tests at the adapter and LLM-client boundaries |
| Outcomes are `HAS_PAGE`, `MISSING`, or `UNCERTAIN`. | Gate 3 schema | **Change.** Use the explicitly scoped outcomes `matching_page_found`, `no_matching_page_found`, and `uncertain_identity`; keep operational failure separate. | Schema and workflow-transition tests |
| No MediaWiki candidates short-circuit to `MISSING` without a model call. | Gate 3 runner | **Preserve and clarify.** A successful, complete bounded search with no plausible candidates produces a time-stamped no-match observation. | Empty-result workflow test |
| Exact title, redirect equality, edit distance, and category score annotate likely matches. | Gate 2 helpers and tests | **Delete as identity evidence.** Names and spelling similarity generate or order candidates but cannot establish that two mentions identify the same person. | Negative namesake tests |
| English nickname pairs generate searches in both directions. | `NICKNAME_MAP`; nickname tests | **Delete.** The list is culturally narrow and can introduce unrelated candidates. MediaWiki redirects and results provide aliases with provenance. | Absence from query-generation tests |
| Honorific removal, comma-name swapping, and accent stripping generate query variants. | `query_variants` tests | **Change.** Keep mechanically text-grounded variants. Use accent stripping only as a fallback query, never identity evidence. | Table-driven query-generation tests |
| English category and occupation keywords score whether a result is a biography. | `biography_score`; Gate 2 tests | **Delete.** The taxonomy is incomplete and unsuitable as a recall-sensitive gate. Categories may be supplied as semantic context. | Candidate-selection tests without category gating |
| The biography score controls which results the model sees. | `select_candidates`; Gate 3 tests | **Delete.** Supply a bounded set of objectively plausible top results rather than filtering through the custom score. | Candidate-set contract tests |
| Non-article namespaces and disambiguation pages are detected from MediaWiki data. | Page properties and candidate construction | **Preserve.** These are deterministic provider facts. A disambiguation page is not itself a biography. | Namespace and disambiguation fixtures |
| Search and page results are capped. | `srlimit`, `search_max_results`, and candidate selection | **Preserve and clarify.** Bounds are configurable and truncation is explicit. A negative result from an unsafe truncated set cannot become a no-match observation. | Boundary and truncation tests |
| A matching page must repeat the discovery article's specific work or event. | Gate 3 prompt warns against this inference | **Delete.** Wikipedia lead extracts are selective. Require positive alignment of available identity facts, not repetition of every source detail. | Promptfoo cases with aligned identity and missing event detail |
| The model may use outside knowledge to fill gaps. | Gate 3 prompt prohibits it | **Preserve the prohibition.** Compare only accumulated source context and supplied MediaWiki data. | Promptfoo grounding cases |
| Genuine doubt favors `UNCERTAIN` over an unsupported `HAS_PAGE`. | Gate 3 prompt and tests | **Preserve and extend.** Escalate selectively, continue coverage, and surface unresolved identity rather than suppress the person. | Promptfoo ambiguity cases and workflow tests |
| Model or parsing failures fall back to semantic `UNCERTAIN` and continue. | Gate 3 runner fallback tests | **Change.** Technical failures receive typed operational outcomes. Whether independent coverage work continues is decided by workflow and failure policy, not by fabricating a semantic judgment. | Failure tests in capability 7 |
| Known pages are cached by normalized subject name and regex-derived aliases. | Gate 2 and Gate 3 index update scripts | **Delete.** A bare normalized name is unsafe for namesakes. | Namesake persistence tests |
| A confirmed mapping stores only a title in some paths and omits stable page identity. | Gate 3 index update | **Change.** Store an entity-scoped relation to MediaWiki page ID, canonical title, URL, decision basis, and provenance. | Persistence and referential-integrity tests |
| A later item with the same normalized name automatically reuses the mapping. | Gate 0 known-page skip | **Delete.** Reuse requires association with the same person entity. Names and aliases are candidate-generation aids only. | Same-name/different-person tests |
| Cached Wikipedia existence is permanent unless files are manually replaced. | Known-page index behavior | **Change.** Allow refresh after a configurable interval because pages can be created, deleted, redirected, or retargeted. | Clock-controlled refresh tests |
| Model output is structured and includes a matched title, confidence, and grounded evidence. | Gate 3 schema tests | **Preserve the typed, grounded contract.** Return stable candidate identity and rationale; redesign exact confidence fields later. Strict validation replaces brace-search parsing. | JSON Schema and provenance tests |
| Identity state is passed through JSONL files and updated by separate index scripts. | Gate 2 and Gate 3 artifacts | **Delete as implementation behavior.** SQLite owns candidates, attempts, observations, and entity-to-page mappings transactionally. | Persistence and resumability tests |

## Verification Boundary

Pytest covers deterministic query generation, redirects, namespaces,
disambiguation, result deduplication, empty results, truncation, stable page
IDs, caching, entity-scoped reuse, refresh, and provider failures. Promptfoo
covers exact-name collisions, professional names, sparse extracts, aligned and
conflicting professions or eras, missing event details, and genuinely ambiguous
identities.
