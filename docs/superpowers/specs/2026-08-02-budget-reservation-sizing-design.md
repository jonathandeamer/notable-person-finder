# Budget Reservation Sizing: Reserve Against the Rendered Request

| Field | Value |
| --- | --- |
| **Status** | Proposed |
| **Date** | 2026-08-02 |
| **Author** | (design agent) |
| **Branch** | `fix/reserve-against-rendered-request` (proposed) |
| **Programme** | Notable Person Finder clean-slate rewrite |
| **Depends on** | Milestones 1–6b-i complete on `refactor/rearchitecture` (HEAD `6703e2c`) |
| **Revises** | The per-generation budget reservation contract established in `docs/superpowers/specs/2026-07-29-model-gateway-and-detection-design.md` |

## Overview

Every OpenRouter generation reserves budget before the call and reconciles to
actual cost after it. The reservation is currently computed from the
*configured ceiling* rather than the request that is about to be sent:

```python
reserve = max_input_tokens * prompt_price + max_completion_tokens * completion_price
```

`max_input_tokens` is a per-task configuration bound, not a property of the
request. The result is that a nominal USD cap throttles at a small fraction of
its value, deferring work that the cap could comfortably afford.

This revision changes the input side of that formula to use the rendered
request's own measured size, which every render already computes and carries.
The completion side is unchanged. No new dependency is introduced and the
reservation stays deterministic and offline.

This is a change to a reviewed contract, so it is a design revision rather
than remediation, and follows the full pull-request path.

## Background and Motivation

### Measured evidence

From the first ten-feed `notable run` (2026-08-02), recorded in
`docs/architecture/known-gaps.md`:

| Figure | Value |
| --- | --- |
| Reserved across the run | $0.9679 |
| Actually spent | $0.0539 |
| Over-reservation | ~31x |
| Average real `detect_people` call | ~$0.0017 |
| Items deferred `not_evaluated_budget` | 219 of 236 |
| Share of the cap genuinely consumed | ~5% |

Processing that run's 236 source items currently needs a nominal cap near
$12.70 to spend about $0.40.

### The two compounding errors

1. **A byte ceiling charged as a token count.** `max_input_tokens` bounds the
   worst-case **UTF-8 byte** length of the rendered input. English prose runs
   roughly 4 bytes per token, so charging bytes as tokens over-reserves by
   about 4x.
2. **The ceiling charged instead of the request.** A `detect_people` request
   that renders to ~3.4 kB is charged as though it were 65,536. On the shipped
   configuration that is a further ~8x.

Error 2 is the one that blocks work, and it is the one this revision fixes.
Error 1 is retained deliberately; see K2.

### Why raising `max_input_tokens` did not cause this

`max_input_tokens` moved from 4096 to 65536 on 2026-08-02, multiplying the
per-call reserve by exactly 7 ($0.00768 to $0.05376). That made the flaw
visible at operational scale; it did not introduce it. The ratio between
reserve and real spend was already wrong at 4096.

The input budgets must not be lowered to compensate: 13,141 bytes is the
measured `detect_people` floor and `match_wikipedia_identity`'s worst case is
near 16 kB with a hard `ValueError` below it.

## Decisions

### K1 — Reserve the input side against the rendered request

The reservation uses the rendered request's `worst_case_input_tokens` in place
of the task's configured `max_input_tokens`.

`worst_case_input_tokens` is already computed by every render as
`token_bearing_utf8_bytes` plus that task's chat-framing allowance, and is
already carried on all four rendered-request models:

| Task | Rendered model field |
| --- | --- |
| `detect_people` | `people/models.py:160` |
| `resolve_person_entity` | `people/models.py:235` |
| `reconsider_person_entity` | `people/models.py:235` (shared) |
| `match_wikipedia_identity` | `wikipedia/models.py:186` |
| `assess_article` | `coverage/assessment.py:161` |

**Verified: the rendered value is in scope at all five reservation sites**, in
every case bound before the reservation runs:

| Site | `rendered` bound | Reservation |
| --- | --- | --- |
| `people/service.py` `detect_people` | 1681 | 1716 |
| `people/service.py` `resolve_person_entity` | 2848 | 2883 |
| `people/service.py` `reconsider_person_entity` | 3647 | 3683 |
| `wikipedia/service.py` `match_wikipedia_identity` | 2455 | 2493 |
| `coverage/service.py` `assess_article` | in the `try` above 3855 | 3900 |

No call site needs reordering. This is an argument substitution at five call
sites plus the helper's signature.

### K2 — The UTF-8-bytes-as-tokens conversion is retained as the safety margin

Error 1 above is **not** fixed. Reserving measured bytes as though they were
tokens leaves roughly a 4x margin over real token cost, and that is the
intended post-change behaviour.

Rationale:

- It requires no tokenizer, so reservation stays deterministic, offline, and
  free of a model-specific dependency. A tokenizer would also have to match
  whichever endpoint OpenRouter routes to, which is not knowable at reserve
  time.
- Over-reserving against a request whose size is *known* is a sound buffer.
  Under-reserving is the direction that lets a run exceed its cap.
- Reconciliation at `finish_attempt` already replaces the reserve with actual
  cost, so the margin costs in-run headroom only, never reported accuracy.

Expected outcome: ~31x over-reservation becomes ~4x. A $1.00 cap processes the
full 236-item run instead of deferring 219 items.

### K3 — The completion side is unchanged

`max_completion_tokens * completion_price` stays. Output length is not
knowable before the call — it is the quantity cause 1 of the `detect_people`
gap was about — so the configured ceiling is the only sound bound. Any attempt
to predict completion length is out of scope.

### K4 — The helper moves to `runs/budget.py` and is renamed

`_worst_case_reservation_nano_usd` currently lives in `people/service.py` and
is imported as a private symbol by `wikipedia/service.py:37` and
`coverage/service.py:123`. Budget arithmetic belongs with the rest of budget
handling.

It moves to `runs/budget.py` as a public `reservation_nano_usd`, with the
input parameter renamed from `max_input_tokens` to `input_tokens` so the call
sites cannot silently keep passing a ceiling:

```python
def reservation_nano_usd(
    *,
    prompt_unit_price_nano_usd: int,
    completion_unit_price_nano_usd: int,
    input_tokens: int,
    max_completion_tokens: int,
) -> int:
```

The `_checked_product` overflow guard and `OVERFLOW_PRICING_DETAIL` move with
it unchanged. The rename is the point: a mechanical substitution that left the
parameter named `max_input_tokens` would read as correct at every call site
while being wrong.

### K5 — The change can only lower a reservation, never raise one

Every render already raises when `worst_case_input_tokens > max_input_tokens`
(`people/detection.py:144`, `people/resolution.py:106`,
`wikipedia/matching.py:97`, `coverage/assessment.py:263`). Therefore
`worst_case_input_tokens <= max_input_tokens` holds at every reservation site
by construction.

This is a load-bearing safety property: no run that completes under a given
cap today can begin failing on budget after the change. It must be asserted,
not merely argued — see the testing strategy.

### K6 — No change to reconciliation, persistence, or reporting

`reserve`, `reserve_in_transaction`, and the reconcile path in
`runs/budget.py` are untouched. The `attempt` row's reserved and actual cost
columns keep their meaning; `notable audit run --attempt` and the digest's
per-run budget line keep rendering the same fields. Only the reserved figure's
magnitude changes.

### K7 — No configuration change

No new key, no default moves. `openrouter_usd_per_run` keeps its meaning and
becomes usable at a realistic value. `max_input_tokens` keeps its existing
role as the hard render ceiling that produces `input_too_large` refusals; it
simply stops doubling as the billing estimate.

The operator guidance in `docs/architecture/known-gaps.md` ("set
`openrouter_usd_per_run` well above expected real spend") is withdrawn as part
of this change, and that gap entry is replaced with a note recording the
retained ~4x margin from K2.

## Non-Goals

- Token counting of any kind. No tokenizer, no `tiktoken`, no per-model
  encoding table.
- Predicting completion length (K3).
- Changing `max_input_tokens` defaults, the render ceilings, or the
  `input_too_large` refusal path.
- Fixing the `detect_people` mention-cap overrun (cause 4). Unrelated, and
  still gated on the schema-hash question.
- Adding budget figures to `notable status`. Still a later milestone, though
  this change makes those figures worth printing.

## Testing Strategy

Per `CLAUDE.md`, tests-first ordering is not evidence. Every rule below needs a
named test killed by mutating that rule.

### New tests

1. `tests/run_engine/test_budget.py::test_reservation_uses_supplied_input_tokens`
   — a reservation computed from a small `input_tokens` is strictly less than
   one computed from a large ceiling at identical prices. Kills a helper that
   ignores its input argument.
2. `tests/people/test_detection_service.py::test_reservation_tracks_the_rendered_request_not_the_configured_ceiling`
   — prepare two source items whose rendered sizes differ materially under one
   `max_input_tokens`, and assert the reserved figures differ in the same
   direction. This is the test that fails today, and the one that fails again
   if any call site reverts to passing the ceiling.
3. `tests/people/test_detection_service.py::test_reservation_never_exceeds_the_configured_ceiling_reservation`
   — the K5 safety property, asserted rather than argued.
4. One call-site test per remaining task, asserting the reserved figure equals
   `reservation_nano_usd` computed from that render's own
   `worst_case_input_tokens`: `tests/people/test_resolution_service.py`,
   `tests/people/test_reconsideration_service.py`,
   `tests/wikipedia/test_match_service.py`,
   `tests/coverage/test_assess_service.py`. Without these, four of five sites
   could keep passing the ceiling with the suite green.

### Existing tests that must move

Only two pin the formula directly, so the blast radius is small:

- `tests/people/test_detection_service.py:784` —
  `expected_reservation = prompt_price * 4415 + completion_price * 512`
- `tests/people/test_seams.py:167` —
  `expected = prompt_price * max_input + completion_price * max_completion`

Both must be recomputed from the render's `worst_case_input_tokens`, **not**
updated to a new hardcoded constant. A hardcoded number would pass while the
call site passed any fixed value.

### Mutation evidence required

| Mutation | Must kill |
| --- | --- |
| Helper ignores `input_tokens`, uses a constant | test 1 |
| One call site reverts to the configured ceiling | test 2, and that site's test from 4 |
| Helper drops the completion term | existing reservation tests |
| Render's ceiling check removed at any of the four sites | test 3 |

### What cannot be mutation-tested

The ~4x retained margin (K2) is a judgement, not a rule, and has no test. It
is recorded in `docs/architecture/known-gaps.md` instead.

## Completion Gate

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people \
  tests/wikipedia tests/coverage tests/leads tests/audit
```

Plus, because this changes real spending behaviour and the last two provider
defects were invisible offline:

```bash
set -a && . ./.env && set +a
uv run pytest tests/people -m live -v
uv run pytest tests/coverage -m live -v
```

And one real `notable run` over the full ten-feed configuration at
`openrouter_usd_per_run = "1.00"`, recording reserved versus actual spend and
the `not_evaluated_budget` deferral count. The change is only demonstrated if
that run processes materially more than the 17 of 236 items the same cap
processed before it. Record the figures, no secrets.

## Open Questions

1. **Should the retained margin be explicit rather than incidental?** K2 keeps
   ~4x as a side effect of the byte/token confusion. An explicit
   `RESERVATION_SAFETY_MULTIPLIER` over a real token estimate would be
   honest about the intent, at the cost of the tokenizer K2 rejects.
   Recommend deferring, but it should be a conscious deferral.
2. **Does the assess path's `AssessInputTooLarge` refusal interact with K5?**
   It raises before the reservation, so it should not, but the interaction
   with `_record_local_refuse_assessment` deserves a read during
   implementation.

### Resolved before approval

- **`worst_case_input_tokens` counts the JSON schema, in all four render
  modules.** Each `_token_bearing_utf8_bytes` has the identical shape —
  `prompt`, `user_json`, and `schema_json`, summed as UTF-8 — at
  `people/detection.py:532`, `people/resolution.py:537`,
  `wikipedia/matching.py:456`, and `coverage/assessment.py:591`. The schema is
  a genuine part of the billed prompt, so counting it is correct, and the
  consistency means no task under-reserves relative to the others. This also
  explains why the discriminated union's 319 schema bytes moved the detection
  floor from 3,065 to 3,384: schema growth is charged, and under K1 it will be
  charged accurately rather than absorbed into a fixed ceiling.
