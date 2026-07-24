# Legacy Behavior Review: Lead Assessment and Ranking

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability converts accumulated, typed coverage evidence into a
transparent priority for human review. It does not decide whether a person is
notable under Wikipedia policy. It replaces the prototype's overconfident
notability labels with deterministic lead-assessment outcomes and inspectable
ordering reasons.

Primary legacy evidence:

- `scripts/llm_gate4b_runner.py`
- `prompts/gate4b.md`
- `prompts/gate4b_unlisted.md`
- `tests/test_gate4b_runner.py`
- Gate 4b assertions in `tests/test_smoke_pipeline.py`
- assessment fields consumed by `scripts/det_openclaw_daily_digest.py`

Policy evidence reviewed for this decision:

- [Wikipedia:Notability (people), revision 1365615841, 23 July 2026](https://en.wikipedia.org/w/index.php?oldid=1365615841&title=Wikipedia%3ANotability_%28people%29),
  captured on 2026-07-24.

The Wikipedia guideline informs useful evidence patterns and cautions. It is
not converted into an automated policy engine. The application allocates
editor attention; the editor applies current Wikipedia policy.

## Deterministic Assessment

There is no candidate-level “is this person notable?” model call. Models have
already performed bounded semantic judgments on individual source items and
articles. Code aggregates those typed results using configurable policy.

An article qualifies toward the strongest lead outcome when it:

- concerns the same person;
- provides significant rather than passing coverage;
- comes from a curated-eligible canonical publisher domain;
- is editorially independent of the subject rather than self-published,
  affiliated, sponsored, or promotional; and
- is reporting, a profile, a substantial review, an obituary, or a
  substantial editorially commissioned interview.

An interview in a curated-eligible, editorially independent publication can
qualify because the publication's decision to conduct substantial coverage is
useful attention evidence. The subject's statements remain primary-source
claims and are labelled accordingly. Blog, self-published, affiliated,
promotional, and unclassified interviews do not qualify for the strongest
outcome.

Listings, routine announcements, press releases, sponsored pieces, passing
mentions, different-person results, and unresolved classifications do not
qualify. They remain inspectable and may support a provisional lead where the
rules below allow it.

Canonical publisher aliases are deterministic policy. For example, configured
BBC domains count as one publisher. The application does not infer
cross-publisher syndication or claim intellectual independence, but the
strongest lead outcome requires qualifying coverage from at least two distinct
canonical publisher domains. The default of two is configurable.

## Outcomes

### `promising_lead`

Qualifying articles exist from at least the configured number of distinct
canonical publisher domains. The initial default is two. This means that the
evidence deserves stronger editor attention, not that Wikipedia notability has
been established.

A positive result does not require every planned operation to succeed. When
enough qualifying evidence already exists, later query, retrieval, or optional
assessment failures remain visible caveats but do not erase the positive lead.

### `possible_lead`

The strongest threshold is not met, but at least one useful reason for human
review exists. Examples include:

- qualifying coverage from one canonical publisher domain;
- a plausibly qualifying unresolved article;
- significant coverage from an unclassified publisher;
- a substantial non-qualifying supporting source; or
- a grounded domain-transferable attention signal.

Unresolved Wikipedia identity is not itself evidence for this outcome. It is a
caveat and ranking factor. A person with strong coverage may still be
`promising_lead` while the Wikipedia identity remains unresolved; a person
with no useful coverage does not become `possible_lead` merely because the
MediaWiki comparison was difficult.

### `insufficient_evidence`

The bounded assessment completed and every result was clearly non-qualifying,
with no unresolved or otherwise useful evidence. This is a time-stamped
assessment of the collected evidence, not a claim that the person is not
notable.

### `assessment_incomplete`

Required work failed, was truncated, or was not performed, and no positive
outcome is supportable from the evidence already collected. Missing work can
never be converted into a confident negative result.

Operational failure remains typed separately from semantic uncertainty. The
failure and budget design will define exact retry and terminal mechanics.

## Transferable Attention Signals

The stable, domain-neutral positive vocabulary is:

- `significant_recognition`: an important award, honour, or repeated
  nomination;
- `enduring_contribution`: a contribution described as influential,
  innovative, or historically lasting;
- `significant_work`: creation of, or a major role in, an important work or
  body of work;
- `institutional_recognition`: inclusion in permanent collections, national
  reference works, major appointments, or analogous institutions;
- `sustained_field_attention`: substantial criticism, citation, profiles,
  retrospectives, or professional attention over time;
- `major_achievement`: success at a field-defining competition, production,
  election, or analogous domain event; and
- `influential_role`: a role whose reported scope is significant rather than
  a routine job title.

The caution vocabulary is:

- `single_event_only`;
- `inherited_association`;
- `routine_role_or_listing`;
- `primary_or_promotional`; and
- `significance_unclear`.

The existing triage and article-assessment model tasks may extract these
signals from supplied text. Every signal includes its exact claim, supporting
passage, article identifier, extraction attempt, and active profile version.
The model may describe something as major or significant only when the
supplied source does so or a separately configured policy identifies it. It
must not use outside knowledge to infer prestige.

An editor-selected domain profile maps concrete examples onto the stable
vocabulary without adding application branches. The visual-arts profile may
map art prizes to `significant_recognition`, permanent museum collections to
`institutional_recognition`, and retrospective critical coverage to
`sustained_field_attention`. Future profiles can supply examples for sport,
academia, politics, music, or other fields without changing the core schema.

Attention signals can make or strengthen a `possible_lead` and act as a late
ordering factor. They cannot replace the multi-domain coverage requirement for
`promising_lead`.

## Limited Model Synthesis

After code assigns `promising_lead` or `possible_lead`, a bounded model may
prepare human-facing synthesis for shortlisted candidates. It returns:

- a concise explanation of why the person may be worth reviewing;
- the strongest supplied evidence, citing stored article identifiers;
- recorded identity, access, source, and evidence caveats; and
- useful questions for human follow-up.

The synthesis cannot change the outcome or ordering, assign a probability of
notability, omit required caveats, search for new facts, or introduce outside
knowledge. Code validates that every cited identifier belongs to the person.

For an unclassified publisher attached to a possible lead, the same bounded
task may report source-reconnaissance signals visible in already retrieved
material: apparent publication type, bylines, masthead or corrections
information, original-reporting indicators, sponsored labels, promotional
wording, affiliation, unclear ownership, and exactly what was inspected. It
must return unknown when evidence is inadequate and always mark the publisher
as requiring human source review. It cannot promote or demote publisher
policy. Version one performs no separate crawl of publisher About or editorial
policy pages.

Boundary-critical uncertain article judgments remain visibly uncertain in
version one. They do not trigger another production model.

## Ranking

Use deterministic lexicographic ordering rather than an opaque weighted score
or model confidence:

1. `promising_lead` before `possible_lead`;
2. newly promoted or materially strengthened candidates before configured
   reminders;
3. `no_matching_page_found` before `uncertain_identity`;
4. more qualifying canonical publisher domains, used only as a tie-breaker;
5. grounded positive attention signals, with caution signals remaining
   visible;
6. better evidence visibility: full article before partial text before
   snippets;
7. fresher triggering coverage; and
8. stable person identifier as the final deterministic tie-breaker.

Each candidate records the ordering factors that applied. The digest-size
limit is configurable; roughly ten is an operating preference, not an
architectural constraint.

Numeric model confidence does not drive assessment or ranking. Categorical
semantic outcomes, grounded rationales, and explicit uncertainty replace
false precision. Provider probabilities, if ever available, may be retained as
raw provenance only.

## Accumulation and Prospective Policy

Assessment operates on a durable person entity rather than only today's feed
item. Qualifying coverage and attention signals accumulate across runs. New
evidence or a new judgment triggers deterministic reassessment, and a
candidate resurfaces only after material improvement or a configured reminder
interval. Prior outcomes remain immutable history.

Publisher policy, model, prompt, and profile changes apply prospectively only
in version one. Each new assessment copies the applicable policy and version
into its provenance. Existing article assessments and candidate outcomes are
not automatically replayed, reclassified, migrated, or downgraded. There is no
background publisher monitoring or retroactive reclassification subsystem.

The representation of the single versioned source-policy configuration
surface is deferred to the configuration and persistence design. A plainly
incorrect historical record may be corrected manually outside the ordinary
workflow; version one does not require a general override interface.

If a later MediaWiki refresh finds a matching English Wikipedia page, future
coverage research and lead surfacing stop for that person. Historical lead
assessments remain unchanged.

## Approved Dispositions

| Observable behavior | Decision and rationale | Replacement verification |
| --- | --- | --- |
| An LLM evaluates each result, then code counts accepted domains. | **Preserve the boundary and clarify.** Models make per-article semantic judgments; configurable code aggregates them. | Workflow and Promptfoo tests |
| Two accepted domains emit `LIKELY_NOTABLE`. | **Change.** Two distinct canonical eligible domains initially emit `promising_lead`, an attention-routing label rather than a verdict. | Decision-table tests |
| One accepted domain emits `UNCERTAIN`; zero emits `NOT_NOTABLE`. | **Change.** Use explicit `possible_lead`, `insufficient_evidence`, and `assessment_incomplete` semantics. | Decision-table and failure tests |
| Too few prefiltered results emit `SKIPPED`. | **Delete.** Sparse positive, completed negative, and incomplete work are meaningfully different outcomes. | Sparse-evidence tests |
| An unlisted-source model pass can emit `POSSIBLY_NOTABLE`. | **Delete.** Models cannot determine publisher reliability. Unclassified significant coverage can support `possible_lead` only. | Source-policy and Promptfoo tests |
| Distinct raw domains are treated as independent sources. | **Change.** Require distinct canonical publisher domains for the strongest lead while making no claim about intellectual independence or syndication. | Alias and output-language tests |
| The original discovery article is excluded from the count. | **Delete.** The coverage capability assesses it on the same terms as other articles. | Assessment fixture tests |
| Substantial interviews never count because they contain primary material. | **Change.** A substantial interview from an eligible, editorially independent publication may qualify, with primary claims clearly labelled. | Interview matrix tests |
| Awards, roles, obituaries, and similar facts are direct notability verdicts. | **Change.** Represent grounded, transferable attention and caution signals; they strengthen review priority but do not prove notability. | Schema, profile, and Promptfoo tests |
| Model confidence numbers drive outcomes. | **Delete.** Use categorical judgments, explicit caveats, and deterministic ordering. | Schema and ranking tests |
| Assessment considers only one event in the current run. | **Change.** Accumulate evidence on a durable person and reassess on material new evidence. | Multi-run persistence tests |
| People are grouped by case-folded display name. | **Delete.** Aggregate by stable person entity; names are not identity keys. | Namesake tests |
| Policy or prompt changes implicitly reinterpret old results. | **Delete.** Version-one changes are prospective and stamped into new observations. | Version-provenance tests |
| A later confirmed Wikipedia page leaves the lead active. | **Delete.** Stop future research and surfacing while preserving history. | Refresh lifecycle tests |

## Verification Boundary

Pytest covers the outcome decision table, canonical publisher aliases,
qualifying content types, substantial interviews, positive results despite
later failures, incomplete negatives, entity-based accumulation, prospective
policy versions, resurfacing, MediaWiki suppression, and every ranking
tie-breaker. Promptfoo covers article semantics, grounded positive and caution
signals across several domain profiles, synthesis citations, caveat retention,
source reconnaissance, outside-knowledge prohibition, and appropriately
non-verdict language.
