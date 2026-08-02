# Known Gaps Carried Forward

**Status:** Current
**Applies to:** the rewrite (`src/notable_person_finder/`)

These are live, diagnosed gaps in shipped rewrite behaviour. They are recorded
so a later change does not mistake one for a regression, and so a fix starts
from the diagnosis rather than repeating it. `CLAUDE.md` carries a one-line
pointer to each; the reasoning lives here.

Resolved findings are not kept here. A fix is held by its named regression test
and, where the reasoning is needed to avoid re-breaking it, by a comment at the
code. Look in git history for how something came to be the way it is.

## Budget reservation charges the configured ceiling, not the real request

A USD cap therefore throttles at a small fraction of its nominal value.

The reserve for one generation is `max_input_tokens * prompt_price +
max_completion_tokens * completion_price`. That is wrong twice over:
`max_input_tokens` is a worst-case **UTF-8 byte** ceiling being charged as a
**token** count (~4x over), and the worst case is charged rather than the
actual request (~8x over).

Measured on the first ten-feed run: reserved $0.9679 against $0.0539 actually
spent, roughly 31x over-reservation on a ~$0.0017 average `detect_people` call,
which deferred 219 items `not_evaluated_budget` while only 5% of the cap was
really used. Processing that run's 236 source items needs a nominal cap near
$12.70 for about $0.40 of real spend.

Raising `max_input_tokens` to 65536 multiplied the per-call reserve by exactly
7, from $0.00768 to $0.05376 — it made this pre-existing flaw visible, it did
not create it.

Do not "fix" this by lowering the input budgets: 13,141 bytes is the measured
`detect_people` floor and `match_wikipedia_identity`'s worst case is near 16k
with a hard `ValueError`. The real fix is to reserve against the rendered
request's actual size, which is already computed as `token_bearing_utf8_bytes`
on each rendered request, and it needs its own design.

Until then, set `openrouter_usd_per_run` well above expected real spend; actual
cost stays bounded by the per-task character caps regardless of the cap value.

## `detect_people` fails model-output validation on real feed content

Diagnosed: four independent causes, three fixed.

The first ten-feed run produced 17 `malformed_response` failures, 6 of them
exhausting retries — 6 of 31 items triaged. Replaying the six failing items
against the live model isolated the causes:

1. **Response truncation (fixed).** `max_completion_tokens` shipped at 1024
   while `max_people` shipped at 8. One item measured 693, 713, 851, 960 and
   962 completion tokens across repeated calls at `max_people = 3`; the longer
   ones were cut off mid-string and the truncated JSON was rejected. Output
   length varies per call, which is why this was intermittent and why retries
   often recovered. `detect_people.max_completion_tokens` is now 4096, pinned
   by `tests/people/test_detection.py::
   test_shipped_example_completion_budget_scales_with_max_people` at the
   measured ~300 tokens per permitted mention. Raise it with `max_people`.
2. **Signal category/kind mismatch (fixed).** `detection_schema()` flattened
   `AttentionCategory` and `CautionCategory` into a single 12-value `category`
   enum with no dependency on the sibling `kind`, so `kind: "attention"` with a
   caution category was structurally valid under strict structured output: the
   call was paid for, then `people/detection.py` rejected the whole response.
   This was the dominant cause in replay, 3 of 6 items.
   `_pair_signal_kind_with_category` now emits a two-branch discriminated
   union, verified live to be accepted by strict mode and to make the invalid
   pairing unrepresentable. The domain validator stays as defence in depth. The
   union costs 319 schema bytes, which raised the fixed request floor from
   3,065 to 3,384 and is why several test fixtures moved from
   `max_input_tokens=4096` to `4415` — the same free space as before, so every
   truncation test keeps its intent.
3. **Ungrounded identity-fact values (fixed).** Not model invention, as first
   assumed. Feed titles are title-cased and the model quotes them back in
   sentence case: observed live as `'starring Michael B. Jordan as a Notorious
   Art Thief'` against a passage reading `'Starring ...'`, the same text
   differing in one letter's case. `_literal_is_grounded` now compares
   case-insensitively, which cannot admit invention because the text must still
   appear verbatim. `_is_grounded_name` stays case-sensitive on purpose:
   `exact_name` is persisted as the person's name, so a lowercased proper noun
   there is a worse artifact, not a presentation difference.
4. **Mention cap overrun (found, not fixed).** With 1-3 fixed, one item still
   fails with "mentions exceed supplied mention cap 3": the schema sets no
   `maxItems` on `mentions`, so the model may return more than `max_people` and
   the response is discarded after payment. Fixing it means making
   `detection_schema()` depend on `max_people`, which makes the schema hash
   config-dependent and moves every fingerprint derived from it — real surface,
   not a contained fix.

The pattern across all four is the finding worth carrying: **the detection
schema systematically under-constrains rules the domain validator enforces, and
every rule left unexpressed is paid for before it is rejected.** Prefer
expressing a validator rule in the schema wherever strict mode can carry it.

## `notable audit person` does not render the complete K11 forensic record

Although the repository loads some of these fields, the command currently omits
each sourced name's `search_name` and `match_key`; each mention's persisted
`rationale`; each assessment's `content_types_json` judgment; and queue-history
`last_material_change_at` and `removed_reason`. Lead-history rows additionally
discard ordering factors, `lead_policy_fingerprint`, and `material_fingerprint`
while loading, so the renderer cannot show them.

This leaves identity lookup evidence, mention outcomes, assessment content-type
judgments, lead provenance/ranking, and queue lifecycle evidence incomplete.
These are all required by K11 in
`docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md` and need a
follow-up audit remediation.

## K1's `canonical_domain` cross-host alias override is not wired in

Same-host collapsing already works.

`docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
locks K1: "`config/source_policies/*.toml` rules gain an optional
`canonical_domain` key." `PolicyRule` in `coverage/screening.py` has no such
field — its only fields are `id`, `status`, `match`, `rationale`,
`review_date`, `provenance_url` — so a policy author cannot yet explicitly
alias two *different* hosts (e.g. `www.example.com` and `example.org`) to one
canonical domain for the promising-lead threshold.

`leads/service.py`'s `_canonical_domain_map` reads each rule's own
`host_exact`/`host_suffix` from `rule.match`, so two rules matching the *same*
host value (or one rule matching many articles from that host) already collapse
to one domain today; only the explicit alias-across-different-hosts override
remains unreachable. `tests/leads/test_service.py::
test_canonical_domain_map_reads_host_exact_and_host_suffix_from_real_policy`
builds a real `SourcePolicy` via `source_policy_from_mapping` and proves the
same-host collapsing path.

Adding the `canonical_domain` field is deliberately deferred, not done inline:
every `PolicyRule` change alters `fingerprint_source_policy_document`'s hash
(full pydantic `model_dump`), which would move
`config/source_policies/visual_arts.toml`'s tracked fingerprint and every
fingerprint-pinned test, coverage plan fingerprint, and merge reconciliation
path that depends on it — real schema/feature surface, not a
remediation-scoped fix.

## A non-settling handler discards its payload after an external call

If a handler returns a non-settling state after making an external call, the
engine settles that item `failed_permanent` and finishes the run, but
deliberately discards the untrusted payload. The attempt row is then the only
durable call evidence; a handler-owned domain row such as `feed_fetch` is not
written.

## Two crash windows can repeat a paid provider call

See `docs/architecture/at-least-once-execution.md`. Do not restate those
windows elsewhere.
