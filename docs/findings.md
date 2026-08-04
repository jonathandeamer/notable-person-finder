# Hard-Won Findings

Provider and model facts discovered the expensive way during the
`refactor/rearchitecture` programme (July–August 2026). They cost real money and
live debugging to find, and several were invisible to offline tests and to two
independent static reviews.

This is a constraints list, not a design authority. The design authority is
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`.

## OpenRouter strict structured output

**A root-level `anyOf` is rejected with HTTP 400.** The root of a strict schema
must be `type: "object"`. Probed live on 2026-08-02: root `anyOf` REJECTED,
plain object root ACCEPTED, `anyOf` nested on a property ACCEPTED. Strict mode
also supports neither `if`/`then` nor `dependentSchemas`.

The practical consequence: a dependency between two *root-level* properties
cannot be expressed in the schema and must be a domain-validator rule. The
canonical case is "`matching_page` must carry `selected_page_id`, and every
other outcome must leave it null" — there is nowhere to nest it. **Do not retry
this as a discriminated union.** A union nested on a property (detection's
signal category/kind pairing) does work, and should be used there.

**Otherwise, express every validator rule the schema can carry.** The governing
pattern behind five separate defects: the schema systematically under-
constrained rules the domain validator enforced, and every rule left
unexpressed is *paid for* before it is rejected. Item caps belong in `maxItems`,
not in a post-hoc length check.

**`uniqueItems` is rejected with HTTP 400.** Probed live on 2026-08-04 via the
`assess_article` schema's `content_types` array: `"'uniqueItems' is not
permitted"` (Azure-routed `openai/gpt-5.4-mini`, `invalid_json_schema`). This
is the one exception to "express every validator rule the schema can carry" --
`minItems`/`maxItems` are fine, but array-uniqueness has no schema-level
expression under strict mode. Enforce de-duplication only in the pydantic
domain validator (a `set`-length comparison is enough; see
`coverage_contract.AssessmentOutput`).

## Completion budget

**`max_completion_tokens` must scale with the item cap it serves.** Shipped at
1024 against `max_people` 8, responses were cut off mid-string and rejected as
`malformed_response` — intermittently, because output length varies per call,
which is what made it hard to see. 4096 was the working value at `max_people`
8. Measured at `max_people` 3: 693–962 completion tokens across repeated calls
on one real item.

Raise these two together. Never independently.

## Grounding checks

Two separate causes made valid model output look ungrounded:

1. **Case.** Feed titles are title-cased and the model quotes them back in
   sentence case. Literal-value grounding must compare **case-insensitively**.
2. **Citation scope.** A value was checked only against the passages the model
   *cited*. `"Artists"` — present in the title passage but cited to the summary
   passage — was rejected in 3 of 3 replays. Grounding must search **every
   supplied passage**, not just cited ones.

Neither widening admits invention. The value must still appear in
application-supplied text as a **contiguous run of characters differing only in
case** — no reordering, no gaps, no paraphrase — and an unknown passage id must
still be rejected. Name grounding stays **case-sensitive** on purpose.

## Candidate truncation skews Wikipedia verdicts

With `max_candidates` at 8, truncation fired on **111 of 118 plans**. Because
the prior design forbade the "no matching page" outcome whenever the candidate
list was truncated, a model that would honestly say "no page exists" had to
answer `uncertain` instead — measured 25 `uncertain` versus 4
`no_matching_page`, and 4 of 5 shortlist entries were `uncertain`.

Nothing was lost (both outcomes continue to coverage research), but the
distinction between "definitely absent" and "cannot tell" largely collapsed.

**`max_candidates` is the lever, and in this codebase it is a plain config
int.** Raising it trades retrieval cost and prompt size against answer quality.
Try it early; it was the prior programme's top open item and was blocked there
only by fingerprint coupling that no longer exists.

## Prompt size floors

Measured worst cases, if input ceilings are ever reintroduced:

- `detect_people` floor: **13,141 bytes**.
- `match_wikipedia_identity` worst case: near **16 kB**.

Do not set an input ceiling below these. A too-small ceiling truncates silently
on detection and hard-fails on match.

Note the prior system's budget *reservation* charged UTF-8 bytes as tokens,
giving a deliberate ~4x margin that measured 4.6–9.5x live. The MVP reads
actual cost from OpenRouter's `usage` field instead, so this does not apply —
recorded only so the byte/token conflation is not reintroduced by accident.

## Brave Web Search

**`extra_snippets` is a paid-plan parameter.** Brave rejects it on the free and
base tiers, and that rejection is *permanent* — retrying wastes the run. Leave
it off unless the subscription includes it.

## MediaWiki

Use the public Action API with a `maxlag` parameter (5s worked) and pace
requests — roughly 900 ms minimum interval was polite and unproblematic. Brave
tolerated ~1100 ms.

Send a contactable User-Agent. It is a condition of the API etiquette policy,
not a nicety.

## Reference economics

A full uncapped ten-feed run over **246 source items** on 2026-08-02, after the
validation fixes:

- **$1.08 total**
- 65 Wikipedia matches
- 27 completed coverage plans, 74 article assessments
- 7 `promising_lead`, 19 `possible_lead`
- **5 digest entries**

This is the MVP's comparison baseline. A rebuild that produces a materially
worse shortlist at similar cost has lost something real.

## Process finding

**A written live smoke test is not evidence until it has actually run.** The
two most costly provider defects in the prior programme — the completion-budget
truncation and the unreachable negative outcome that disabled coverage research
entirely — were both invisible offline and survived two independent static
reviews. Only a real run against real providers found them.
