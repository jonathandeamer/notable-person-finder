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

## Budget reservation carries a deliberate ~4x margin

**Not a defect. Do not "fix" it without replacing the reasoning below.**

The reserve for one generation is `input_tokens * prompt_price +
max_completion_tokens * completion_price`, where `input_tokens` is *this*
request's measured size (`worst_case_input_tokens`), not the task's configured
`max_input_tokens` ceiling.

That measured size is a UTF-8 **byte** count charged as a **token** count.
English prose runs roughly 4 bytes per token, so the reservation sits about 4x
above real token cost. That margin is intentional:

- it needs no tokenizer, so reservation stays deterministic and offline, and
  does not have to guess which endpoint OpenRouter will route to;
- over-reserving against a request whose size is known is the safe direction
  for a spend cap; and
- reconciliation at `finish_attempt` replaces the reserve with actual cost, so
  the margin costs in-run headroom only, never reported accuracy.

The completion side stays at the configured ceiling because output length is
not knowable before the call.

Measured on a real ten-feed run at a $1.00 cap (2026-08-02), the residual is
4.6x on `match_wikipedia_identity`, 5.1x on `resolve_person_entity`, and 9.5x
on `detect_people` — wider than the ~4x a prose byte/token ratio predicts,
because JSON tokenizes denser and because the fixed worst-case **completion**
term dominates whenever the input is small. Any further reduction has to come
from the completion side, which is the harder problem.

**History.** Until 2026-08-02 the input side charged `max_input_tokens`
instead, compounding the byte/token margin with a worst-case-versus-actual
error. That same $1.00 cap triaged 17 of 236 items and deferred 219
`not_evaluated_budget`, reserving $0.9679 against $0.0539 spent. After the fix
it triaged 246 of 246 and spent $0.6541 — roughly 14x the work per dollar of
cap. See
`docs/superpowers/specs/2026-08-02-budget-reservation-sizing-design.md`.

Do not compensate by lowering the input budgets: 13,141 bytes is the measured
`detect_people` floor and `match_wikipedia_identity`'s worst case is near 16 kB
with a hard `ValueError`. `max_input_tokens` is the render ceiling that
produces `input_too_large` refusals; it is no longer the billing estimate.

## Model-output validation: what is fixed, and the one rule that cannot be

All causes diagnosed by live replay of the failing items from the first
ten-feed run and fixed on 2026-08-02. See
`docs/superpowers/specs/2026-08-02-model-output-validation-design.md`.

Result on a re-run of the same 246-item corpus: `malformed_response` attempts
fell 23 -> 7, match permanent failures 6 -> 0, and the pipeline reached
coverage research and produced a real shortlist for the first time.

Fixed:

1. **Response truncation.** `max_completion_tokens` shipped at 1024 against
   `max_people` 8; now 4096, pinned by `tests/people/test_detection.py::
   test_shipped_example_completion_budget_scales_with_max_people`. Raise it
   with `max_people`.
2. **Signal category/kind mismatch.** `_pair_signal_kind_with_category` emits a
   two-branch discriminated union so the invalid pairing is unrepresentable.
3. **Ungrounded identity-fact values — two separate causes.** Case: feed titles
   are title-cased and the model quotes them in sentence case, so
   `_literal_is_grounded` compares case-insensitively. Citation: the value was
   checked only against the passages the model *cited*, so `"Artists"` —
   present in the title passage but cited to the summary passage — was
   rejected (3 of 3 replays). It now searches every supplied passage. Neither
   widening can admit invention: the text must still appear verbatim in
   application-supplied text, and `_reference_error` still rejects an unknown
   passage id. `_is_grounded_name` stays case-sensitive on purpose.
4. **Mention cap overrun.** `detection_schema()` now takes `max_people` and
   emits `maxItems`. This makes the schema config-dependent, which is why the
   detection fixed floor moved 3384 -> 3397 and fixtures moved 4415 -> 4428.
5. **`no_matching_page` offered when the validator forbids it.** The severe
   one: `capped = uncapped_count > max_candidates` (default 8) held for 111 of
   118 plans, so the negative outcome was forbidden almost always while the
   schema still offered it. Six people failed permanently, 12 attempts, zero
   recoveries — retries re-send byte-identical input. `match_schema()` now
   omits the outcome when truncated, forcing `uncertain`, which is
   coverage-eligible. **This is why the earlier run produced zero digest
   entries: `no_matching_page_found` was unreachable and coverage research is
   gated on it.**

**Cannot be expressed, do not retry: the outcome/selected-id pairing.**
`matching_page` must carry `selected_page_id` and other outcomes must leave it
null (same for resolution's `same_person`). A root-level discriminated union
would make the invalid pairing unrepresentable — and **strict structured
output rejects it with HTTP 400, because the root must be `type: "object"`**.
Probed live 2026-08-02: root `anyOf` REJECTED, plain object root ACCEPTED,
nested `anyOf` on a property ACCEPTED. Detection's signal union works only
because it is *nested*; this dependency is between two root properties and has
nowhere to nest, and strict mode supports neither `if`/`then` nor
`dependentSchemas`. The pairing stays a domain-validator rule.
`test_match_schema_root_is_an_object_not_a_union` and its resolution twin pin
the root shape.

The governing pattern, now bounded: **the schema systematically
under-constrains rules the domain validator enforces, and every rule left
unexpressed is paid for before it is rejected — so express a validator rule in
the schema wherever strict mode can carry it, but strict mode cannot carry a
dependency between two root-level properties.**

## `uncertain_identity` now absorbs most true negatives

Not a defect; a consequence of the fix above, recorded so the skew is not
mistaken for a model problem.

Because truncation fires on 111 of 118 plans, a model that would honestly say
"no page exists" must now answer `uncertain`. Measured on the verification run:
`uncertain_identity` 25 versus `no_matching_page_found` 4, and 4 of the 5
shortlist entries are `uncertain_identity`.

Nothing is lost — both outcomes are coverage-eligible — but the distinction
between "definitely absent" and "cannot tell" is largely collapsed. The lever
is `max_candidates`, which at 8 is binding almost always. Raising it would let
more plans answer definitively, at a retrieval-cost and prompt-size tradeoff
that deserves its own look rather than a quiet bump.

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
