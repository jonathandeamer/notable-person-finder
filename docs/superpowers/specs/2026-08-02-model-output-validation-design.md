# Model Output Validation: Stop Paying for Answers the Schema Permits and the Validator Forbids

| Field | Value |
| --- | --- |
| **Status** | Proposed |
| **Date** | 2026-08-02 |
| **Author** | (design agent) |
| **Branch** | `fix/model-output-validation` (proposed) |
| **Programme** | Notable Person Finder clean-slate rewrite |
| **Depends on** | Milestones 1–6b-i complete on `refactor/rearchitecture` (HEAD `ee01d17`) |
| **Revises** | `docs/superpowers/specs/2026-07-30-wikipedia-identity-matching-design.md` (the match output schema) and `docs/superpowers/specs/2026-07-29-model-gateway-and-detection-design.md` (the detection grounding rule) |

## Overview

A real ten-feed run produced 23 `malformed_response` attempts across three task
types. Replaying the failing items against the live model isolated the causes.
None is model unreliability. Each is the same structural defect the repository
has already recorded twice:

> **The schema systematically under-constrains rules the domain validator
> enforces, and every rule left unexpressed is paid for before it is
> rejected.**

One of the two confirmed causes is not a nuisance. It makes
`no_matching_page_found` **structurally unreachable**, which silently disables
the entire coverage-and-leads half of the product.

## Evidence

From the run database and log:

| Task | Malformed attempts | Distinct items | Permanently failed |
| --- | --- | --- | --- |
| `match_wikipedia_identity` | 12 | 6 | **6 of 6** |
| `detect_people` | 8 | 7 | 1 |
| `resolve_person_entity` | 3 | 3 | 0 |

Neither the `attempt` row nor the JSON log records *why* any of these was
rejected — `detail_json` is empty by design because the untrusted payload is
discarded, and the log records only `failure_category`. Diagnosing this
required replaying every failing item against the live model. See A0.

### A — `match_wikipedia_identity`: `no_matching_page` is offered and then refused

**Confirmed by live replay: 6 of 6, one rule, deterministic.**

```
no_matching_page is invalid when truncated_unsafe_for_negative
```

Every replayed item returned `outcome=no_matching_page` with exactly 8
candidates supplied — the `max_candidates` default.

The chain:

1. `candidates.py:138` sets
   `truncated_unsafe = search_incomplete or capped or redirect_budget_exhausted
   or partial_retrieval`, where `capped = uncapped_count > max_candidates`
   (default **8**).
2. That is true for **111 of 118 plans** in the run. A Wikipedia search
   returning more than 8 pages is the normal case, not an edge case.
3. `matching.py:431` then correctly rules that a model shown 8 of N candidates
   cannot safely assert that no page exists.
4. But `match_schema()`'s `outcome` enum offers `matching_page`,
   `no_matching_page`, and `uncertain` **unconditionally**. The model picks the
   honest answer, the call is paid for, and the response is discarded.
5. Retries cannot recover: the input is byte-identical, so the model returns
   the same answer. 12 attempts, 12 rejections, 0 recoveries.

**Why this is severe rather than annoying.** The run reported "No matching page
(this run): 0. Uncertain identity: 0." That is not a fact about the corpus. Of
27 people who received a verdict, 21 matched and 6 failed; `no_matching_page`
was unreachable. Coverage research is gated on `no_matching_page_found` or
`uncertain_identity`, so no coverage plan can open, no article can be assessed,
and every lead settles `insufficient_evidence`. The run produced **zero**
digest entries, and this is why.

### B — `detect_people`: grounding is checked only against *cited* passages

**Confirmed by live replay: 5 of 7 failing items; the one permanent failure
reproduces 3 of 3.**

```
mention[1] f2: value is not grounded
```

Item 66's passages:

```
[p1] "Artists' monumental hot dog sculpture, now with patriotic toppings,
      returns to Times Square"
[p2] "Jen Catron and Paul Outlaw's 65ft-long "Hot Dog in the City" is back on
      display in New York ..."
```

The model returned `profession_or_role = "Artists"` citing `p2`. The word
**is** present in the supplied text — in `p1`. `_literal_is_grounded` searches
only the passages the model cited, so a correct extraction with a misattributed
citation is rejected.

This is deterministic for item 66 because the role word appears in the title
while the person names appear in the summary, so the model naturally cites the
passage containing the person. It is intermittent elsewhere, which is why 6 of
7 items recovered on retry — at a paid call each.

This is a **citation** error, not a grounding failure, and grounding is the
rule that matters: it exists to make invention impossible.

### C — `detect_people`: mention cap overrun (previously recorded as cause 4)

Observed once in replay:

```
mentions exceed supplied mention cap 3
```

`detection_schema()` sets no `maxItems` on `mentions`, so the model may return
more than `max_people`. Already recorded in
`docs/architecture/known-gaps.md`; carried here because it is the same defect
class and should be fixed in the same pass.

### D — `resolve_person_entity`: not yet diagnosed

3 attempts, 3 items, **all recovered on retry**, so no item was lost. The
validator's rules are structurally identical to match's
(`resolution.py:470-488`: `same_person` requires `selected_person_id`, it must
be a supplied candidate, other outcomes require null, fact ids must be
supplied, rationale non-empty), and the same outcome/nullability dependency is
unexpressed in its schema. **This is a hypothesis, not a confirmed cause.** It
is in scope only for the A2 schema change, which fixes it if the hypothesis
holds and is harmless if it does not.

## Decisions

### A0 — Persist the domain-validation reason

The rejection message is already constructed to be payload-free: every
`_domain_validation_error` return is a fixed string plus identifiers the
application itself supplied (`mention[1] f2`, a fact id, an outcome name). None
of it is untrusted model text.

Record that string on the attempt's `detail_json` and in the structured log.
This does not weaken the rule that the untrusted payload is discarded — the
payload stays discarded.

Without this, every future validation defect costs a live replay to diagnose,
which is what this design cost. It is listed first because it is the change
that makes the others cheap to find.

### A1 — The match outcome enum depends on `truncated_unsafe_for_negative`

When the plan is truncated, `match_schema()` omits `no_matching_page` from the
`outcome` enum. The model must then answer `matching_page` or `uncertain`, and
`uncertain` is the semantically correct answer for "I cannot rule it out from a
partial candidate list."

`uncertain_identity` **is** coverage-eligible, so affected people flow onward
into coverage research instead of dying at `failed_permanent`.

The domain validator keeps its rule as defence in depth.

### A2 — Outcome and selected-id nullability become a discriminated union

Both `match_schema()` and `resolution_schema()` currently type their selected
id as `integer | null` with no dependency on `outcome`, so
`outcome=no_matching_page` with a non-null `selected_page_id` is structurally
valid and then rejected.

Both become a two-branch discriminated union on `outcome`, exactly as
`_pair_signal_kind_with_category` already does for detection signals — a
pattern already verified live to be accepted by strict structured output.

### A3 — Grounding is checked against every supplied passage, not only cited ones

`_literal_is_grounded` searches all passages in `supplied.passages` rather than
only `references`.

This **cannot admit invention**: the value must still appear verbatim in text
the application itself supplied. There are only two passages (title and
summary), so the widening is small and bounded.

Citation accuracy is *not* abandoned. `_reference_error` continues to reject a
citation naming an unknown passage id, and signals keep their existing
reference checks. What is dropped is the requirement that the *value* appear in
the *specific* passage cited, which is a presentation detail the model gets
wrong without inventing anything.

Precedent: this is the same shape as the accepted casefold fix — the text must
still appear verbatim, so the anti-hallucination property is preserved.

### A4 — `detection_schema()` caps `mentions` at `max_people`

Add `maxItems: max_people`. This makes the detection schema depend on
configuration, so `detection_schema()` gains a parameter and the schema hash
becomes config-dependent.

### A5 — Schema hash provenance follows the schema

A1 and A4 make schemas input- and config-dependent, so a single module-level
constant hash is no longer truthful.

- **Match (A1)** varies over exactly **two** variants. `match_prompt_and_schema_hashes()`
  takes `truncated_unsafe_for_negative` and returns that variant's hash.
  `truncated_unsafe_for_negative` is already persisted on
  `wikipedia_identity_plan` and already material, so nothing new becomes
  material.
- **Detection (A4)** varies with `max_people`, which is already a fingerprinted
  config bound.

**This moves existing fingerprints.** Every fingerprint derived from a match or
detection schema hash changes, so settled work re-seeds and re-executes on the
next run. That is the cost of the change and must be stated in the plan, not
discovered during it.

### A6 — `max_candidates` is not raised in this change

With 111 of 118 plans truncating, the default of 8 is binding almost always,
which plausibly weakens match quality on its own. Raising it is a separate
question with its own retrieval-cost and prompt-size trade-offs, and A1 makes
truncation non-fatal regardless. Out of scope here; worth its own look.

## Non-Goals

- Changing the retrieval or candidate-assembly logic.
- Persisting raw provider request or response bodies. A0 records the
  application's own validation message only; the payload stays discarded.
- Any change to the budget reservation contract, settled in
  `2026-08-02-budget-reservation-sizing-design.md`.

## Testing Strategy

Per `CLAUDE.md`, tests-first ordering is not evidence. Every rule needs a named
test killed by mutating that rule.

1. `test_match_schema_omits_no_matching_page_when_truncated` — and a **positive
   control** asserting the untruncated schema still offers it. Without the
   control the assertion passes when the enum is empty.
2. `test_match_schema_pairs_outcome_with_selected_page_id` /
   `test_resolution_schema_pairs_outcome_with_selected_person_id` — the invalid
   pairing must be structurally unrepresentable.
3. `test_identity_fact_value_grounded_in_uncited_supplied_passage` — item 66's
   real fixture: value in `p1`, citation `p2`, must validate. Plus
   `test_identity_fact_value_absent_from_every_passage_is_rejected` as the
   control that A3 did not disable grounding.
4. `test_detection_schema_caps_mentions_at_max_people` — asserting `maxItems`
   tracks the configured value, not a constant.
5. `test_match_schema_hash_differs_between_truncation_variants` — A5's
   provenance rule.
6. `test_validation_detail_is_recorded_on_the_attempt` — plus a control proving
   no model-supplied text reaches `detail_json`.

Live smokes must be re-run: this changes what is sent to the provider, and the
last two provider defects were both invisible offline.

## Completion Gate

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people \
  tests/wikipedia tests/coverage tests/leads tests/audit
set -a && . ./.env && set +a && uv run pytest tests -m live -v
```

Plus a real ten-feed run, uncapped, demonstrating:

- `no_matching_page_found` **or** `uncertain_identity` greater than zero — the
  outcome that is currently unreachable;
- coverage plans opened greater than zero;
- `malformed_response` attempts materially below 23.

The change is only demonstrated if the pipeline reaches coverage research,
because that is what cause A currently prevents.

## Open Questions

1. **Should `uncertain` also be removed when truncation is total?** If
   retrieval returned nothing usable, `uncertain` is the only permitted answer
   and the call is a foregone conclusion. `failure_category_if_empty` already
   handles the empty-candidate case deterministically without a model call, so
   this may already be covered; confirm during implementation.
2. **Does A4's config-dependent schema hash interact with the coverage
   material fingerprint?** Detection hashes feed triage fingerprints; the
   re-seed blast radius should be measured before implementing, not after.
3. **Is cause D real?** A2 is cheap and harmless either way, but A0 shipped
   first would answer it from the next run's data rather than by inference.
