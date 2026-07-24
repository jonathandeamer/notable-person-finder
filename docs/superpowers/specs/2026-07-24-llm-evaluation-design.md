# LLM Tasks and Evaluation

**Status:** Approved
**Date:** 2026-07-24
**Branch:** `refactor/rearchitecture`

## Purpose

This specification defines the semantic judgments delegated to models, their
typed evidence boundaries, production model assignment, prompt ownership, and
lean Promptfoo evaluation. It implements the deterministic/model boundary in
the approved [product workflow](2026-07-24-product-workflow-design.md) and
stores attempts according to the
[domain design](2026-07-24-domain-persistence-design.md).

The application is not an autonomous agent. Models receive no tools, make no
network requests, control no workflow transitions, and issue no Wikipedia
verdict. Each request answers one narrow question about supplied evidence.

## Invariants

- OpenRouter is the only model gateway.
- Every logical task has one configured preferred model in version one;
  several tasks may use the same model.
- Models and request parameters are configuration, never Python constants.
- Production models must support the task's strict JSON Schema through
  OpenRouter.
- Each request contains exactly one task unit and no unrelated person's
  context.
- Code constructs context, bounds candidates and passages, validates output,
  and applies policy afterward.
- Models use only supplied evidence, preserve attribution and uncertainty, and
  ignore instructions contained inside feed or article text.
- Outputs contain categorical judgments, evidence references, and concise
  rationales, with no numeric confidence or hidden chain-of-thought.
- Valid semantic uncertainty is a terminal result for that attempt, not an
  operational failure or a reason to call another model.

Version one has no different-model fallback, cheap-first routing, or stronger-
model escalation. An exhausted operational failure leaves required work
incomplete. Promptfoo may compare candidate models, but production changes
model only through explicit configuration. Complex routing is deferred until
observed quality, cost, or availability data justifies it.

## Shared Typed Components

Strict Pydantic boundary models generate the exact JSON Schemas. The following
conceptual components are shared without collapsing different tasks into one
generic prompt.

### Evidence boundary

Every input states:

- identifiers for the supplied item, person, article, passages, and candidates;
- view kind: feed metadata, snippets, partial text, or full cleaned text;
- which fields and passage IDs are present;
- whether text was shortened to the configured token bound;
- whether a candidate or result list was truncated; and
- relevant domain-profile and schema versions.

Code creates numbered passages and performs selection and truncation before
the call. Prompts state that missing information is unknown, not negative
evidence.

### Identity facts

An identity fact contains an input-local ID, kind, literal supplied value, and
supporting passage IDs. Kinds cover name, profession or role, place,
nationality, era or date, work, affiliation, and `other`. Models compare these
facts but cannot add an unsourced alias or biographical fact.

### Attention and caution signals

The positive and caution vocabularies are defined by capability 5. A returned
signal contains category, concise claim, supporting passage IDs, and whether
significance was explicit in supplied text or an active configured profile.
The model cannot infer prestige from training knowledge. Deterministic ranking
uses categories rather than model confidence.

## Task 1: `detect_people`

### Question and input

Which meaningful individual subjects in this one discovery item should enter
research?

The input contains source item and feed IDs, configured publisher label,
nullable title, normalized summary, numbered title/summary fields, publication
date and URL metadata, active domain-profile examples, and the configured
maximum returned people. It never contains an article body and need not elicit
people named only in passing.

### Output

- item outcome: `research_people`, `do_not_research`, or `uncertain`;
- zero to the configured maximum meaningful mentions;
- for each mention:
  - exact source-written public name;
  - `research`, `do_not_research`, or `uncertain`;
  - supporting passage IDs;
  - source-grounded identity facts;
  - attention and caution signals; and
  - concise rationale;
- `overflow` boolean; and
- concise item-level rationale.

Code verifies names and evidence references against supplied material. An
actionable uncertain mention continues under workflow policy. Overflow remains
reviewable and does not trigger an unbounded loop.

Person detection and triage stay in one call because they require the same
subject-focus understanding. A separate named-entity pass would add calls and
irrelevant passing names without a product benefit.

## Task 2: `resolve_person_entity`

### Question and input

Does this new mention describe one of these bounded existing application
people? The input contains one mention with its names, passages, and facts plus
code-selected candidates with application person IDs, sourced names, and
bounded facts. With no plausible candidates, code creates a new person without
a model call.

### Output

- `same_person`, `different_people`, or `uncertain`;
- selected existing person ID only for `same_person`;
- supporting supplied fact IDs;
- conflicting supplied fact IDs; and
- concise grounded rationale.

The selected ID must be supplied. Name equality alone cannot establish a
match. `uncertain` keeps entities and evidence separate; material new facts may
create a later focused task using the configured model.

## Task 3: `match_wikipedia_identity`

### Question and input

Does one supplied current English Wikipedia candidate page describe this
application person? The input contains one person's sourced names and facts
plus code-selected MediaWiki candidates with page IDs, titles, redirects,
descriptions, lead passages, categories, namespace, and disambiguation data.

Code owns search, empty-result handling, namespaces, redirects,
disambiguation, bounds, and truncation.

### Output

- `matching_page`, `no_matching_page`, or `uncertain`;
- selected MediaWiki page ID only for `matching_page`;
- supporting supplied fact/passage IDs;
- conflicting supplied fact/passage IDs; and
- concise grounded rationale.

The page ID must be supplied. The model cannot search, choose an unseen page,
or require a lead to repeat every source event. A complete empty candidate set
is handled deterministically without a call.

Entity resolution and Wikipedia matching share typed components and test
utilities but retain separate prompts and schemas. Unsafe application entity
merges contaminate evidence; unsafe Wikipedia matches suppress research. Their
consequences and candidate evidence are not interchangeable.

## Task 4: `assess_article`

### Question and input

What does this one supplied article view establish about this one person? The
input contains the person and bounded facts; article ID and deterministic
publisher screening state; title, dek, byline, date, and editorial labels;
numbered passages selected by code; access/truncation metadata; and active
domain-profile examples.

The model receives no raw HTML, page chrome, related links, or unrelated
person's evidence. It does not determine publisher reliability.

### Output

- `person_relation`: `same_person`, `different_person`, or `uncertain`;
- `coverage_depth`: `significant`, `passing`, or `uncertain`;
- one to three `content_types` from reporting, profile, review, interview,
  obituary, listing, announcement, press release, sponsored, or other;
- `subject_relationship`: `editorially_independent`, `affiliated`,
  `self_published`, or `uncertain`;
- grounded attention and caution signals;
- supporting passage IDs for every semantic field; and
- concise rationale for each field.

A small content-type set represents combinations such as interview-profile
without an exploding taxonomy. Code validates bounds and references, then
applies qualification policy. The model never emits an overall accepted-source,
reliable-publisher, or notability boolean.

## Task 5: `compose_lead_summary`

### Question and timing

How can the already selected lead and supplied evidence be compressed for one
human reviewer without changing the decision?

Run this optional task only after deterministic ranking selects a candidate
for the current digest. Do not synthesize every eligible or queued person. A
candidate omitted by the digest limit remains pending and is synthesized only
when selected later.

### Input and output

The input contains the person, exact lead outcome, surfacing reason, selected
qualifying and provisional article assessments, signals, caveats, and observed
metadata for unclassified publishers already present in retrieved material.

The output contains:

- `why_review`, at most two concise sentences;
- strongest supplied article-assessment IDs;
- attention and caution signal IDs;
- unresolved caveats;
- up to three suggested human checks; and
- for each unclassified publisher, apparent publication type or `unknown`,
  observed positive/concern signals, inspected article/passage IDs, and
  `needs_human_source_review: true`.

Every identifier must be supplied. Inadequate publisher material produces
`unknown`; the model cannot alter policy, rank, outcome, or source selection.
It performs no separate About-page crawl. Deterministic fallback is always
available. Source reconnaissance never feeds back into classification.

## Prompt Ownership and Versioning

Store one reviewed Markdown system prompt per logical task. Python Pydantic
models are the schema source of truth, and code serializes task input as
canonical JSON. Avoid prompts embedded in Python, Jinja-heavy templates,
model-specific production prompt forks, and duplicated free-form schemas.

An attempt's prompt version comprises the prompt content hash and explicit
schema version. The input fingerprint also includes the configured task model
and material parameters. Changes apply prospectively and do not schedule
historical replay.

Promptfoo imports the same production prompt and generated schema.
Experimental variants remain evaluation fixtures until an explicit reviewed
change promotes one.

## Production Model Assignment and Validation

Configuration maps each task to exactly one preferred OpenRouter model and its
request parameters and token bound. The provider configuration supplies the
single OpenRouter timeout profile and central retry policy shared by tasks.
Startup validation checks strict structured-output support and permitted
provider routing.

OpenRouter may route the configured model among compatible serving providers
under recorded capability and privacy requirements. It may not silently choose
a different model. Attempts record configured/resolved model, resolved
provider, parameters, request ID, usage, latency, and cost.

Transient and invalid-output retries use the same model. Invalid structured
output gets at most one fresh attempt. Unknown fields, invalid enums, unseen
IDs, missing references, or constraint violations fail strict local
validation. There is no brace extraction, JSON repair, permissive parsing,
free-form fallback, or alternative-model retry.

## Promptfoo Dataset

Build the initial dataset from a coverage matrix of approved behavior,
important boundaries, and known failure modes rather than treating a case
count as a quota. Aim for 10–12 cases each for `detect_people`,
`resolve_person_entity`, `match_wikipedia_identity`, and `assess_article`,
plus 5–8 cases for `compose_lead_summary`: roughly 45–55 cases total. Source
reconnaissance boundary cases belong within `assess_article` rather than
forming a sixth task suite.

Fixtures are deliberately selected boundaries, local and stable, and contain
exactly production context. They cover multiple people, passing mentions,
professional names, namesakes, sparse or conflicting identity context, missing
Wikipedia event details, reviews, interviews, obituaries, promotion, partial
views, unclassified sources, signals, and synthesis caveats. Each confirmed
production error adds the smallest representative regression case.

This initial suite is a prompt-regression and model-comparison aid, not a
statistically representative estimate of recall or accuracy. Stop adding
initial cases when the coverage matrix has representative examples for every
material class; do not pad the suite to meet the approximate total. Thereafter
grow it when real runs reveal a distinct failure mode.

Version one has no stochastic repetitions, confidence intervals, holdout
partition, NDCG, or LLM-as-judge.

## Assertions and Reports

Use deterministic task reports rather than one aggregate score:

- 100% valid structured output is required.
- Person detection reports required-person recall and false research
  inclusions.
- Entity resolution reports unsafe merges separately from missed merges.
- Wikipedia identity reports unsafe matches separately from unresolved or
  missed matches.
- Article assessment reports identity, depth, content type, relationship, and
  signal extraction independently.
- Synthesis asserts only supplied references, caveats, maximum length, unknown
  handling, and absence of verdict language; usefulness gets a brief human
  diff review.
- Tokens, cost, and latency are reported per task and candidate model.

All designated critical false-suppression, unsafe-merge, and unsafe-page-match
cases must pass. Easier examples cannot average away a critical failure. The
small set informs human selection rather than pretending to estimate stable
population performance.

## Evaluation Workflow

Check in Promptfoo configuration, datasets, deterministic assertions, prompt
adapters, and production prompt references. Ignore generated artifacts by
default. Commit a short dated summary only when it explains a production
prompt change or recommended model decision.

Evals are explicit and paid, not part of default pytest, pre-commit, or CI.
Before changing a production prompt, schema, or recommended model, run that
task's suite, inspect critical failures and output diffs, compare cost, and
record the decision. Pytest owns context construction, schema generation,
identifier validation, transitions, and deterministic fallback.

The one-time lightweight legacy comparison remains separate. It manually
reviews approximately 20 frozen inputs and evidence packets after the rewrite;
it does not become a compatibility suite or permanent dual-workflow benchmark.

## Acceptance Criteria

- Production has exactly the five named task contracts.
- Every request contains one task unit and evidence-boundary metadata.
- Unseen IDs and invalid references are rejected before domain state changes.
- Valid uncertainty never triggers retry or another model.
- Changing a task model requires configuration only.
- No task can search, call tools, rank candidates, change publisher policy, or
  emit a notability verdict.
- Digest generation remains useful with synthesis disabled.
- Promptfoo uses the same prompt and schema artifacts as production.
- Critical unsafe cases cannot be hidden by an aggregate score.
- Model, prompt, schema, input, provider, cost, and evidence provenance are
  recoverable for every semantic observation.

## Deferred

- multiple production models per task;
- semantic escalation or different-model operational fallback;
- automatic cost/quality routing;
- cross-candidate batching;
- autonomous browsing or model-selected tools;
- repair loops and conversational self-critique;
- LLM-as-judge, stochastic trials, holdouts, and confidence intervals; and
- percentage quality targets unsupported by the initial evaluation set.
