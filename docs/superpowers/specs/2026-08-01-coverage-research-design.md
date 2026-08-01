# Coverage Research Design

Status: approved
Date: 2026-08-01
Supersedes: nothing
Depends on:

- [Product workflow and decision policy](2026-07-24-product-workflow-design.md)
- [Domain model, persistence, and continuation](2026-07-24-domain-persistence-design.md)
- [LLM tasks and evaluation](2026-07-24-llm-evaluation-design.md)
- [Provider adapter contracts](2026-07-24-provider-adapters-design.md)
- [Wikipedia identity matching](2026-07-30-wikipedia-identity-matching-design.md)

## Purpose

This design specifies steps J through M of the product workflow: bounded Brave
Web Search, retrieval of selected article views, per-person-article semantic
assessment, and deterministic aggregation into a lead outcome.

It is the first design to give a person a *product verdict*. Everything before
it establishes who a person is; this establishes whether English-language
coverage of that person is worth a human editor's attention, and says so in one
of four words: `promising_lead`, `possible_lead`, `insufficient_evidence`, or
`assessment_incomplete`.

It does not cover ranking, the digest queue, `compose_lead_summary`, or digest
presentation beyond replacing the current shortlist placeholder with an
unranked grouped list. Those belong to the following milestone.

## Entry and Exit

A person enters coverage research when its current Wikipedia identity
observation is `no_matching_page_found` or `uncertain_identity`. A person with
`matching_page_found` never enters, and a person that later acquires one stops
future coverage work without any historical record being rewritten.

A person exits with a current lead assessment. That assessment is immutable and
may be superseded by a later one when material new evidence arrives; the person
carries a pointer to the current row.

## Milestone Split

The arc is delivered as two milestones, each a vertical slice with observable
output.

**Milestone 5a — coverage discovery.** Publisher policy, the Brave adapter,
person search plans, first-class queries, ranked result occurrences, shared
canonical-article identity, deterministic screening, and deterministic
selection. A run searches, screens, and selects; the digest and `notable
status` report what happened. No article body is fetched and no assessment is
made.

**Milestone 5b — article assessment and lead outcomes.** Article fetch,
extraction, person-specific passage selection, article views, the
`assess_article` task, attention and caution signals, and the lead decision
table. A run produces lead outcomes and the digest shows them.

Each milestone gets its own plan under `docs/superpowers/plans/` and its own
completion gate.

## Component Boundaries

New provider adapters, in the only package permitted to import `httpx` or a
provider SDK:

- `providers/brave.py` — the Brave Web Search boundary.
- `providers/article_fetch.py` — HTTPX article retrieval over the existing
  shared transport, safety checks, and pacing.
- `providers/article_extract.py` — Trafilatura extraction. Pure and
  network-free; the one module in `providers/` that makes no call.

A new `coverage/` package holds the domain, mirroring the structure that
`wikipedia/` established:

- `policy.py` — publisher policy loading, fingerprinting, and matching.
- `queries.py` — the deterministic person query plan.
- `selection.py` — screening dispositions and article selection.
- `views.py` — article view assembly and access-kind determination.
- `passages.py` — the deterministic `PassageSelector`.
- `assessment.py` — the `assess_article` task contract and validation.
- `leads.py` — completeness and the lead decision table.
- `repository.py` — coverage persistence.
- `service.py` — work-item scheduling, fingerprints, and handlers.
- `prompts/` — the reviewed Markdown system prompt for `assess_article`.

Trafilatura is the only new dependency. Brave and the article fetcher use the
transport that already exists.

## Work Items

Four new work kinds:

| Kind | External call | Owner |
| --- | --- | --- |
| `coverage_search` | one Brave Web Search request | 5a |
| `fetch_article` | one HTTP GET | 5b |
| `assess_article` | one OpenRouter generation | 5b |
| `assess_person_lead` | none | 5b |

The first three each map to exactly one persisted attempt, preserving the
foundation invariant. `assess_person_lead` is deliberately the first work kind
with no external call and therefore no attempt. The engine must tolerate a
zero-attempt settlement explicitly rather than incidentally, and the milestone
plan names that as a rule requiring its own test.

`assess_person_lead` exists as a work item rather than as an inline side effect
so that ordering, fairness, deferral, failure recording, and idempotent
re-execution come from the scheduler, and so that the "one lead calculation per
person per run" coalescing rule is enforced in one place. It also gives a
person with zero fetchable articles a natural trigger.

## Publisher Policy

Publisher policy is a tracked repository artifact, `config/source-policy.toml`,
not an operator-copied example. The curated set is product content that should
be reviewed in pull requests, not a per-machine preference.

Each rule carries a stable `rule_id`, a match target, a status, a rationale, a
decision basis, a source URL, a review date, and, where the Wikipedia Reliable
Sources / Perennial Sources page informed the decision, the exact revision
consulted. Match targets are a domain, a subdomain, a domain with a simple path
prefix, or a publisher alias. Status is `eligible` or `ineligible`. There is no
`unclassified` status: unclassified is the absence of a matching rule and never
has a row.

The file declares a `policy_version` string. The policy fingerprint is that
version combined with a content hash over the canonicalized parsed table, so an
unreviewed local edit is visible in provenance rather than silent.

Loading is strict. Duplicate rule IDs, duplicate match targets, and overlapping
targets with conflicting status are configuration errors rather than precedence
puzzles. Where a domain rule and a path-prefix rule on the same domain both
match, the longer path match wins. The file path is overridable in
`notable.toml`.

The initial set contains the ten pilot publishers as eligible, Reuters and AP
as eligible, and a small ineligible set covering social platforms, user-
generated content, press-release distribution, and overtly promotional
material. The application does not import the Perennial Sources table and does
not implement its contextual caveats. A complicated or absent publisher stays
unclassified.

Screening stamps the matched rule ID, status, and policy fingerprint into
provenance at decision time. Historical evidence is never re-joined against
today's policy file and never retroactively reinterpreted.

## Search

### Query plan

The plan is deterministic, finite, and person-level. When several mentions
resolve to one person in a run, they produce exactly one plan.

1. The exact, quoted, source-written name.
2. Each alias with recorded provenance.
3. At most one contextual query derived from sourced profession, place, or
   career facts, and only when the earlier stages have not met the configured
   retrieval target.

An obituary modifier is permitted only when source material explicitly supports
it. Multipart names are never automatically shortened. No model invents an
alias, chooses a follow-up query, or decides which stage runs next.

A plan records its triggering evidence fingerprint — sourced names, identity
facts, the policy fingerprint, and the configured bounds — together with a plan
version. A materially new source-grounded alias or identity fact changes the
fingerprint and permits one further plan. A merely repeated mention does not.
Plans also refresh after a configurable interval.

Query count, result count, page count, request count, elapsed time, and cost
are configurable bounds. An exhausted bound records the search as incomplete.
Incomplete is never treated as absence of coverage.

Every research-worthy person without a confirmed matching Wikipedia page
receives the stage-one exact-name search, even when discovery articles already
look strong. Finding source material is itself a product outcome.

### Adapter contract

`search_web(query, count, offset)` performs exactly one Brave Web Search
request and returns one `SearchPage`. The adapter fixes the approved product
settings — global, English-language results, moderate SafeSearch — and disables
silent query correction where the API permits it.

The response retains the submitted query, any provider-reported altered query,
the offset, and for each hit the provider identifier, original rank, title,
URL, snippets, extra snippets, language, and pagination and completeness facts.

The adapter never chooses a follow-up query, never switches to Brave News,
never assesses source reliability, and never reranks results across calls. A
provider-altered query is recorded and its results retained, but the altered
form never seeds a new stage.

### Results and article identity

Every result occurrence is first-class and retains its query identity, original
rank, provider identifiers, URL, title, snippets, extra snippets, retrieval
time, language and search parameters, and screening disposition. Repeated
occurrences may reference one article, but query and rank are never collapsed.

One application-owned `canonicalize_article_url` policy defines article
identity for feed items, search occurrences, and observed redirect
destinations alike. This is a change to existing ingestion, which currently
owns its own URL identity: milestone 5a moves ingestion onto the shared
function so that feed and search discovery converge on one canonical article
without a second normalizer. Original and redirected URLs remain as provenance
aliases.

A canonical article has one conservatively normalized unique URL and a
canonical publisher key derived under the active source policy. Only identical
canonical URLs deduplicate. The application performs no cross-source
independence clustering and never claims that a count of results proves
multiple independent reliable sources.

### Selection

Selection is deterministic and involves no model:

1. include the original discovery article on the same terms as any other
   evidence;
2. select eligible results ordered by query stage, then original Brave rank;
3. if that set does not meet the configured retrieval target, select a
   configurable number of unclassified results under the same ordering; and
4. retain ineligible results and their disposition as metadata, and never fetch
   their bodies.

## Retrieval

`fetch_article(url)` applies URL safety, DNS preflight, redirect, timeout,
content-type, and body-size rules through the existing transport. A successful
result contains the requested and final URLs, the redirect chain, response
facts, byte count, and HTML bytes held only for the current attempt.

Expected inaccessible outcomes are typed settled values, not exceptions and not
retries: `not_found`, `auth_required`, `paywalled`, `access_denied`,
`unsupported_content`, and `too_large`. Operational transport or server
breakage remains a typed failure and goes to the central retry coordinator.

The application does not circumvent authentication, paywalls, or anti-bot
measures, adds no publisher-specific scraper, and does not request or interpret
`robots.txt` in version one.

## Extraction and Passage Selection

`extract_article(html)` is pure and network-free. It uses Trafilatura plus
deterministic cleanup to return title, dek, byline, publication date, editorial
labels, ordered and numbered main-text blocks, and extraction-quality or
partial-view warnings. It removes navigation, footers, consent text, comments,
related and trending links, repeated boilerplate, and unrelated captions as far
as a generic extractor safely can. There is no publisher-specific cleanup rule.

Raw HTML is never persisted and never sent to a model. It is released after the
attempt.

The extractor is person-agnostic. A deterministic `PassageSelector` then builds
the person-specific view: title and dek, the opening blocks, every block
containing a sourced name or alias, and adjacent blocks for local context, all
capped by configurable task bounds. Block IDs and truncation metadata let model
output cite only supplied evidence.

If the name does not occur in the extracted text, the selector supplies a
bounded opening plus available feed and search snippets and records the
limitation. It never invokes a model to choose passages.

An article view records access kind — `full`, `partial`, or `snippets` — fetch
metadata, title, dek, byline, date, editorial labels, cleaned main text, the
extraction result, and the exact bounded context supplied to the model. A
doubtful extraction is marked partial and may fall back to snippets rather than
being presented as complete text.

## Article Assessment

One `assess_article` generation per person-article pair. Each call carries
exactly one person and one article view, and no unrelated person's evidence.

The input contains the person and its bounded sourced facts; the article ID and
its deterministic screening state; title, dek, byline, date, and editorial
labels; the numbered passages selected by code; access and truncation metadata;
and active domain-profile examples. It contains no raw HTML, page chrome, or
related links.

The output is:

- `person_relation`: `same_person`, `different_person`, or `uncertain`;
- `coverage_depth`: `significant`, `passing`, or `uncertain`;
- one to three `content_types` from reporting, profile, review, interview,
  obituary, listing, announcement, press release, sponsored, or other;
- `subject_relationship`: `editorially_independent`, `affiliated`,
  `self_published`, or `uncertain`;
- grounded attention and caution signals;
- supporting passage IDs for every semantic field; and
- a concise rationale per field.

Code validates bounds and verifies that every cited passage ID was actually
supplied, rejecting the response otherwise. The model does not determine
publisher reliability and emits no accepted-source, reliable-publisher, or
notability boolean.

Assessments are immutable. A person-article relation lets one article be
assessed independently for several people and points at its current assessment
while retaining prior observations. Attention and caution signals are typed
child records carrying category, exact claim, supporting passage IDs, article,
extraction attempt, and active domain-profile version.

## Completeness

A person's coverage assessment is complete when all of the following hold:

- the query plan reached its configured stopping point — stage one ran, and
  stages two and three either ran or were correctly ruled out by the retrieval
  target;
- no configured search bound was exhausted mid-plan;
- every selected article settled into either an assessment or a typed
  inaccessible state; and
- no required coverage work item for that person is pending, deferred, or
  failed.

Any exhausted bound, budget deferral, provider failure, or unsettled item makes
the assessment incomplete. This keeps the outcome a statement about work
actually performed, and preserves the high-recall rule that technical failure
and budget deferral never become semantic rejection.

## Lead Decision Table

Code applies the table; no model participates.

| Outcome | Condition |
| --- | --- |
| `promising_lead` | qualifying coverage from at least N distinct canonical eligible publisher domains, N configurable, initially two |
| `possible_lead` | at least one useful coverage item or grounded attention signal, below the promising threshold |
| `insufficient_evidence` | a complete assessment with no qualifying or unresolved useful evidence |
| `assessment_incomplete` | required work remains, per the completeness rule above |

Qualifying coverage means an assessment with `person_relation: same_person`,
`coverage_depth: significant`, and `subject_relationship:
editorially_independent`, on an article whose canonical publisher is eligible
under the policy in force at screening time.

A *useful coverage item* is an assessment with `person_relation: same_person`
that falls short of qualifying — because its depth is `passing` or `uncertain`,
its subject relationship is not `editorially_independent`, or its publisher is
unclassified — but still gives a human reviewer something to look at.

Unclassified publishers can support `possible_lead` and are marked provisional
for human review. They can never contribute to the promising threshold.
Domain-neutral attention and caution signals can strengthen or order a possible
lead but cannot substitute for the multi-domain promising threshold.

A lead assessment is immutable and records the outcome, the thresholds applied,
qualifying assessment and canonical publisher references, attention and caution
signals, incompleteness reasons, ordering factors, decision time, and both
configuration and policy provenance. The person points at the current one.

The application does not infer intellectual independence or syndication and
does not describe any of this as a Wikipedia notability decision.

Evidence accumulates across runs. New evidence can reactivate a dormant person
and improve a current assessment. Prompt, model, policy, and profile changes
apply prospectively.

## Configuration

New strict configuration, following the shapes already established:

- `BraveConfig` — endpoint, per-request result count, page and request bounds,
  retrieval target, unclassified-selection cap, plan refresh interval.
- `ArticleFetchConfig` — timeout, maximum body bytes, accepted content types,
  redirect hops, concurrency and pacing participation.
- `SourcePolicyConfig` — the policy file path.
- `LeadPolicyConfig` — the distinct-eligible-domain threshold, initially two.
- `tasks.assess_article` — model, token bounds, generation parameters, passage
  and context caps, following `MatchWikipediaIdentityConfig`.

The Brave API key joins the existing secret-environment mechanism. It is
file-first, supplied only by environment or an adjacent `.env`, and never
appears in snapshots, fingerprints, diagnostics, terminal output, or tests.

## Reporting

Milestone 5a adds a coverage-discovery block to the digest and to `notable
status`: plans created, queries by stage, result occurrences by disposition,
articles selected, searches incomplete with reasons, and Brave cost.

Milestone 5b adds a coverage-assessment block — articles fetched, inaccessible
outcomes by type, assessments made, outcome split, signals recorded, model
deferred and failed counts, and OpenRouter cost — and replaces the digest's
shortlist placeholder with a real unranked list grouped by lead outcome.
`possible_same_person` pairs appear separately with a possible-duplicate
warning; unclassified evidence is marked provisional. Ranking, the digest
queue, and synthesis remain out of scope.

## Testing

Offline by default, in `tests/coverage/`.

Recorded contract fixtures cover the Brave adapter and a representative spread
of fetch outcomes including a paywall, a not-found, an oversized body, and an
unsupported content type. Extraction fixtures are saved HTML with expected
block output.

Every rule this design names gets a recorded mutation kill in its milestone
plan. Particular attention goes to rules that fail silently:

- the N-distinct-eligible-domain promising threshold;
- the rule that unclassified publishers cannot reach `promising_lead`;
- passage-ID validation against supplied evidence;
- the assertion that raw HTML is never persisted or sent to a model, which
  needs a positive control proving the assertion can fail;
- the completeness bar, including that a budget deferral yields
  `assessment_incomplete` rather than `insufficient_evidence`;
- policy-fingerprint provenance, including that a policy edit does not
  reinterpret an existing screening decision; and
- zero-attempt settlement of `assess_person_lead`.

Opt-in live smokes cover Brave and `assess_article`, deselected by default.

## Acceptance Criteria

This design is satisfied when:

- a person with `no_matching_page_found` or `uncertain_identity` receives
  exactly one bounded search plan per run, whatever the mention count;
- an exhausted bound produces a recorded incomplete search and never an implied
  absence of coverage;
- feed and search discovery converge on one canonical article through one
  shared canonicalization function;
- screening provenance survives a later policy edit unchanged;
- an ineligible result is retained with its disposition and never fetched;
- a paywalled or missing article settles as a typed state without a retry and
  without becoming an operational failure;
- no raw HTML is persisted or transmitted to a model;
- one article can hold different assessments for different people;
- an assessment citing an unsupplied passage ID is rejected;
- unclassified evidence can produce `possible_lead` and never `promising_lead`;
- a budget deferral produces `assessment_incomplete`; and
- a later confirmed Wikipedia page stops future coverage work without
  rewriting any historical lead assessment.
