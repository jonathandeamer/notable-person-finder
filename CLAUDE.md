# Repository Agent Guide

## Current Development Direction

This repository is being rebuilt as a clean-slate, installable Python
application. Current development belongs in the `src/notable_person_finder`
package and follows the redesign programme. Do not infer rewrite architecture
or conventions from the retained prototype.

The architectural authorities are:

- `docs/superpowers/specs/2026-07-24-redesign-program-design.md` for programme
  scope, sequencing, replacement boundaries, and cutover policy.
- The applicable file in `docs/superpowers/plans/` for the active milestone's
  exact interfaces, constraints, tests, and completion gate.
- `docs/superpowers/specs/2026-07-25-rewrite-agent-guidance-design.md` for the
  repository's agent publishing and review workflow.

Read the applicable design and plan before implementing. If a progress ledger
exists for an active plan, use it as the recovery authority and do not repeat
tasks already recorded complete.

## What Is Actually Built

Nine milestones are complete: the application foundation, the run engine and
shared transport, feed ingestion, the OpenRouter model gateway with person
detection (3b1), durable person identity with first-pass resolution,
reconsideration, and confirmed merges (3b2), Wikipedia identity matching
with MediaWiki retrieval and semantic match (milestone 4), coverage
evidence — bounded Brave Web Search, article fetch and extraction, and
per-article assessment (milestone 5), lead aggregation with the digest
queue, deterministic ranking, and the real digest shortlist (milestone 6a),
and read-only audit and inspection commands — `notable digest show`,
`notable audit run`, and `notable audit person` (milestone 6b-i). A
cold-starting agent should assume nothing beyond this list.

Milestone 5's authorities are
`docs/superpowers/specs/2026-07-30-coverage-evidence-design.md` (locked
decisions K1–K34, plus a 2026-08-01 amendment) and
`docs/superpowers/plans/2026-07-30-coverage-evidence.md`. The 2026-08-01
`coverage-research-design` spec and `coverage-discovery` plan are superseded
and were never implemented; they contradict the locked decisions and must not
be built from.

Milestone 6a's authorities are
`docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
(locked decisions K1–K13) and the four sequential sub-plans
`docs/superpowers/plans/2026-08-01-lead-aggregation-*.md`, all of whose steps
are recorded complete.

Milestone 6b-i's authorities are
`docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md` (locked
decisions K1–K14, plus the recorded `--attempt` "raw response" amendment) and
the three sequential sub-plans
`docs/superpowers/plans/2026-08-02-audit-i-*.md`, all of whose steps are
recorded complete.

Delivered and usable:

- `notable config validate`, `notable paths`, `notable db migrate`.
- `notable run` — validates configuration, migrates, takes the mutation lock,
  sweeps interrupted predecessor runs, executes eligible work through the
  scheduler, ingests enabled RSS and Atom feeds, inspects configured detection
  and resolve models, detects people in untriaged source items, resolves
  eligible mentions into durable people (empty-candidate create, model path,
  `possible_same_person`, reconsideration, confirmed merges), matches
  Wikipedia identity, opens and advances bounded coverage plans (Brave search,
  article fetch and extraction, per-article assessment), writes a dated
  digest plus `latest.md`, and prints the same Markdown on standard output.
  `notable run` now requires `BRAVE_API_KEY` as well as `OPENROUTER_API_KEY`;
  it refuses to start without either.
- The shared HTTP transport with URL, DNS-preflight, redirect, timeout,
  response-size, concurrency, and pacing bounds; the retry coordinator; the
  per-run budget reservation; and redacting structured logging.
- The feedparser-backed feed adapter, conditional feed fetching, durable feed
  identity and fetch history, canonical article and URL-alias identity, and
  insert-once source items.
- The OpenRouter provider adapter (SDK-backed inspect and structured
  generation with SDK retries disabled so only the central coordinator may
  repeat a call), exact-model preflight inspection, dynamic per-generation
  budget reservation under an optional hard USD cap, person-detection work
  items, and `resolve_person_entity` / `reconsider_person_entity` work items.
- Triage observations and person mentions on source items. Mentions retain
  exact names, mononyms, and professional names. Eligible research/uncertain
  mentions resolve to durable people, sourced names, entity-resolution
  observations, optional `possible_same_person` edges, and confirmed merges
  with lower-id survivors and canonical work reconciliation.
- Separate bounded HTTP and LLM worker pools that may overlap; workers never
  access SQLite.
- MediaWiki adapter (`search_pages`, `get_page_facts`) with shared transport,
  pacing, maxlag, and no auth secret; durable pages, query plans, search
  observations, page-fact batches, and Wikipedia identity observations with
  outcomes `matching_page_found`, `no_matching_page_found`, and
  `uncertain_identity`; person current Wikipedia pointer; work kinds
  `mediawiki_search`, `mediawiki_page_facts`, and `match_wikipedia_identity`;
  deterministic empty complete search no-match; refresh, merge reconcile, and
  Wikipedia counters on digest and `notable status`.
- Coverage evidence (milestone 5). Three work kinds and no more —
  `brave_web_search`, `fetch_article`, and `assess_article` (K2) — each making
  exactly one external call per execute. The Brave Web Search adapter
  (`providers/brave.py`, endpoint-only config, `BRAVE_API_KEY` from the
  environment, paced by `brave_min_interval_ms`); the article fetcher and the
  Trafilatura extractor (`providers/articles.py`,
  `providers/article_versions.py`) which persist cleaned article views and
  never raw HTML. The `coverage/` package: the Wikipedia eligibility gate
  (only `no_matching_page_found` and `uncertain_identity` people are
  researched, so a current `matching_page_found` stops research), bounded
  per-person coverage plans with deterministic query forms in four variants
  (`exact`, `exact_obituary`, `alias`, `context`), deterministic publisher
  screening against the versioned source
  policy (`curated_eligible` / `curated_ineligible`, with absence of a
  matching rule meaning `unclassified`; first matching rule wins over
  `host_exact`, `host_suffix`, and `path_prefix`), deterministic article
  selection with a bounded unclassified fallback, person-specific passage
  selection, and immutable person–article assessments with their signals.
  Merge reconciliation for coverage work, coverage material fingerprints and
  refresh, and a "Coverage evidence" digest section plus `notable status`
  coverage lines.
- Lead aggregation and the digest queue (milestone 6a). The `leads/` package
  (`aggregation.py`, `ranking.py`, `queue.py`, `service.py`, `repository.py`,
  `merge_hooks.py`); the single `aggregate_person_lead` work kind, scheduled
  from coverage and Wikipedia settlement and swept for missed hooks; migration
  `0008_lead_aggregation.sql` with `lead_assessment`,
  `lead_assessment_qualifying_article`, `lead_assessment_signal`,
  `digest_queue`, `queue_transition`, `digest`, and `digest_entry`, plus
  `person.current_lead_assessment_id`. Outcomes are `promising_lead`,
  `possible_lead`, `insufficient_evidence`, and `assessment_incomplete`,
  produced by deterministic code aggregating already-persisted per-article
  judgments — there is no candidate-level notability model call. Queue
  lifecycle (`pending` / `emitted` / `removed`, tiers `promising_lead` and
  `possible_lead`, eligibility reasons `new` / `promoted` / `strengthened` /
  `reminder`), material fingerprints that stop a person re-aggregating on
  every run, merge reconciliation replacing the former named no-op, the real
  digest shortlist and queue-flow sections, and the `digest` / `digest_entry`
  history written on every run that writes a digest, including runs with an
  empty shortlist.
- Read-only audit and inspection commands (milestone 6b-i), in the new
  `audit/` package. `notable digest show [run_id]` re-reads a previously
  written digest body from disk, verifies it against its recorded hash, and
  prints it unchanged — defaulting to the highest `run_id` when none is
  given, and refusing to print anything on a hash mismatch. `notable audit
  run <run_id> [--attempt <attempt_id>]` renders the full lifecycle-ordered
  evidence for one run — configuration provenance, state transitions, work
  outcomes, attempts, failures, budget, and reporting — or, with `--attempt`,
  the single named attempt's persisted validated result, retry history,
  usage, and version provenance located through the `audit/registry.py`
  binding for its `task_type` (not `(provider, operation)`, which is
  ambiguous: five task types share the single OpenRouter
  `generate_structured` operation). `notable audit person
  <person_id>` renders one person's full lifecycle-ordered evidence chain —
  identity, sourced names, relations, mentions, entity resolution,
  Wikipedia, coverage, assessments, lead history, queue history, and digest
  history — printing a banner naming the survivor and merging run followed
  by the merged person's own recorded history, with a pointer to
  `notable audit person <survivor_id>`, for a merged-away person.
  Every section degrades to an explicit "section unavailable" marker rather
  than raising when the database predates the table it needs (K3). All three
  commands are read-only: they open the database with `readonly=True` and
  take no mutation lock.

Delivered only in part — do not describe these as finished:

- `notable status` reports the latest run, its digest, required pending and
  deferred counts, operational failures, corpus source-item and article totals,
  each feed's latest successful fetch, durable triage counters (triaged,
  untriaged, research, uncertain, do not research, insufficient input, failed
  triage, and unresolved research or uncertain mentions), and durable identity
  counters (canonical people, merged-away people, unresolved eligible
  mentions under K24, active `possible_same_person`, and mentions linked to
  people), and — when the coverage schema is present — three coverage lines
  (people with a completed assessment, coverage eligible remaining, and people
  stopped because they match Wikipedia), and — when the leads schema is
  present — a `digest backlog: N promising_lead, M possible_lead` line and an
  `oldest pending candidate` line. Milestone 6a closed the backlog,
  oldest-pending, and queue-tier gaps. What `status` still lacks is budget
  figures and a deferral-reason breakdown, so it cannot explain *why* work was
  deferred; that needs a later milestone.
- The digest emits its header, banner, operational summary, per-run budget
  line, deferral-reason breakdown, ingestion summary, person-detection summary
  (triage outcomes, unresolved mention counts, model deferred/failed,
  OpenRouter cost), and person-identity summary (people created, mentions
  resolved, outcome split including created_new vs different_people, K24
  eligible remaining, active possible_same_person, confirmed merges, resolve
  model deferred/failed), and a "Coverage evidence" section (plans completed,
  incomplete, and permanently failed this run; assessments completed this run;
  people with a completed assessment; coverage eligible remaining; people
  stopped because they match Wikipedia; assess model deferred and permanently
  failed) emitted only when the coverage schema is present. Milestone 6a
  replaced the hardcoded shortlist placeholder with a real ranked shortlist
  and a queue-flow block, both emitted when the leads schema is present. What
  the shortlist still lacks is the optional model synthesis: each entry is
  rendered from deterministic aggregation output, with no `why_review`
  narrative.

Not built at all, so do not document, import, or assume any of it:

- The `compose_lead_summary` optional model synthesis and its Promptfoo
  suite, and source reconnaissance for unclassified publishers. A shortlist
  entry has no model-written summary. These remain milestone 6b-ii scope and
  were not touched by 6b-i.
- Drafting. The application never generates or publishes Wikipedia content.

Known gaps carried forward, recorded so a later change does not mistake them
for regressions:

- **Budget reservation charges the configured ceiling, not the real request,
  so a USD cap throttles at a small fraction of its nominal value.** The
  reserve for one generation is `max_input_tokens * prompt_price +
  max_completion_tokens * completion_price`. That is wrong twice over:
  `max_input_tokens` is a worst-case **UTF-8 byte** ceiling being charged as a
  **token** count (~4x over), and the worst case is charged rather than the
  actual request (~8x over). Measured on the first ten-feed run: reserved
  $0.9679 against $0.0539 actually spent, roughly 31x over-reservation on a
  ~$0.0017 average `detect_people` call, which deferred 219 items
  `not_evaluated_budget` while only 5% of the cap was really used. Processing
  that run's 236 source items needs a nominal cap near $12.70 for about $0.40
  of real spend. Raising `max_input_tokens` to 65536 (see the entry below)
  multiplied the per-call reserve by exactly 7, from $0.00768 to $0.05376 — it
  made this pre-existing flaw visible, it did not create it. Do not "fix" this
  by lowering the input budgets: 13,141 bytes is the measured `detect_people`
  floor and `match_wikipedia_identity`'s worst case is near 16k with a hard
  `ValueError`. The real fix is to reserve against the rendered request's
  actual size, which is already computed as `token_bearing_utf8_bytes` on each
  rendered request, and it needs its own design. Until then, set
  `openrouter_usd_per_run` well above expected real spend; actual cost stays
  bounded by the per-task character caps regardless of the cap value.
- **`detect_people` fails model-output validation on a material share of real
  feed content. Diagnosed: three independent causes, one fixed.** The first
  ten-feed run produced 17 `malformed_response` failures, 6 of them exhausting
  retries — 6 of 31 items triaged. Replaying the six failing items against the
  live model isolated the causes:
  1. **Response truncation (fixed).** `max_completion_tokens` shipped at 1024
     while `max_people` shipped at 8. One item measured 693, 713, 851, 960 and
     962 completion tokens across repeated calls at `max_people = 3`; the
     longer ones were cut off mid-string and the truncated JSON was rejected.
     Output length varies per call, which is why this was intermittent and why
     retries often recovered. `detect_people.max_completion_tokens` is now
     4096, pinned by `tests/people/test_detection.py::
     test_shipped_example_completion_budget_scales_with_max_people` at the
     measured ~300 tokens per permitted mention. Raise it with `max_people`.
  2. **Signal category/kind mismatch (not fixed — needs a design decision).**
     `detection_schema()` flattens `AttentionCategory` and `CautionCategory`
     into a single 12-value `category` enum with no dependency on the sibling
     `kind` field, so `kind: "attention"` with a caution category is
     structurally valid under strict structured output. The call is paid for,
     then `people/detection.py:336-343` rejects the whole response with
     "category does not match signal kind". This was the dominant failure in
     replay (3 of 6 items). It cannot be fixed in the schema builder alone:
     `GroundedSignal` (`people/models.py:126`) carries
     `category: AttentionCategory | CautionCategory` alongside a separate
     `kind`, so expressing the constraint on the wire needs a discriminated
     union — a change to the domain model and the output contract.
  3. **Ungrounded identity-fact values (not fixable in code).** The model
     occasionally returns a fact value that is not a literal substring of its
     cited passage, and `_literal_is_grounded` correctly rejects it. Seen once
     in replay. This is model quality at `reasoning_effort = "low"`, and it
     belongs to the unvalidated-model-choice gap: the Promptfoo comparison the
     pricing research called for was never run.
- **NOT A DEFECT — `resolve_person_entity` settling `failed_permanent` at a
  refused prepare is specified behaviour.** An earlier revision of this file
  recorded it as a bug; that was wrong, and the correction is kept here so it
  is not "fixed" again. `docs/superpowers/specs/2026-07-30-durable-person-
  identity-design.md` line 759 (restated at line 1368) requires prepare to call
  `ensure_resolution_for_mention` first and *then* raise a plain `ValueError`
  with the log-only prefix `resolve_prepare_refused:`, "so the engine settles
  this **claimed** work item `failed_permanent` with no new attempt. That
  permanent state is **accounting noise**, not a domain failure: the mention's
  durable state was already fixed in step (1)."
  `tests/people/test_resolution_service.py::
  test_empty_at_prepare_ensure_then_value_error` and
  `tests/people/test_reconsideration_service.py::test_missing_peer_prepare_
  refuse` pin it deliberately. The observable cost is real but cosmetic: the
  run reports operational failures and goes `partial` for work that in fact
  completed, and any config change that moves a task fingerprint re-seeds
  already-settled mentions and so re-triggers it. Making the engine settle a
  refusal as `succeeded` (for example via a `PrepareRefused` exception the
  engine recognises) would be a **change to an approved design**, not a fix,
  and needs a design revision before implementation — not a remediation
  branch.
- **`notable audit person` does not yet render the complete K11 forensic
  record.** Although the repository loads some of these fields, the command
  currently omits each sourced name's `search_name` and `match_key`; each
  mention's persisted `rationale`; each assessment's `content_types_json`
  judgment; and queue-history `last_material_change_at` and `removed_reason`.
  Lead-history rows additionally discard ordering factors,
  `lead_policy_fingerprint`, and `material_fingerprint` while loading, so the
  renderer cannot show them. This leaves identity lookup evidence, mention
  outcomes, assessment content-type judgments, lead provenance/ranking, and
  queue lifecycle evidence incomplete. These are all required by K11 in
  `docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md` and need
  a follow-up audit remediation.
- `notable status` prints bare pending and deferred counts, with no budget
  figures and no deferral-reason breakdown, so it still cannot explain *why*
  work was deferred. The digest now can; `status` has not caught up.
- If a handler returns a non-settling state after making an external call, the
  engine settles that item `failed_permanent` and finishes the run, but
  deliberately discards the untrusted payload. The attempt row is then the
  only durable call evidence; a handler-owned domain row such as `feed_fetch`
  is not written.
- Two crash windows can repeat a paid provider call; see
  `docs/architecture/at-least-once-execution.md`. Do not restate those windows
  elsewhere.
- **Closed 2026-08-02: every live smoke has now been executed against real
  providers and passes** — all 14 across `tests/ingestion` (4), `tests/people`
  (3), `tests/wikipedia` (4), and `tests/coverage` (3). Recorded provenance,
  no secrets: configured and resolved model `openai/gpt-5.4-mini` for every
  OpenRouter call, `supports_strict` true, pricing usable. `match_wikipedia_
  identity` served by Azure, 452 prompt + 73 completion tokens, 667,500
  nano-USD, outcome `matching_page`. `assess_article` served by Azure, 1,029
  prompt + 275 completion tokens, 2,009,250 nano-USD, outcome `validated`
  (`same_person` / `significant`). Reported cost is therefore real and the
  budget reservation path has now met live pricing. Note `tests/people`'s
  live smokes assert but print no provenance record, unlike the other three
  suites; recording there is still by hand.
- **Closed 2026-08-02: all ten configured feeds have been fetched for real.**
  The live ingestion smokes only ever exercise two (`artnet-news` and, for the
  redirect assertion alone, `hyperallergic`), so the feedparser adapter,
  canonical URL identity, and publisher-key derivation had never met the other
  eight publishers' RSS dialects. A `notable run` over the full
  `config/discovery-feeds.example.toml` set fetched 9 and took one 304 Not
  Modified, with **0 feeds failed**, producing 236 source items and 236
  articles. Ingestion is the one layer now exercised against every configured
  publisher. Note this says nothing about the feeds' *content* passing
  detection — see the `detect_people` malformed-output gap above, which only
  appeared once these nine publishers were in play.
- **The six previously-stub `tests/coverage` files now have real bodies**
  (`test_run_cli.py`, `test_seams.py`, `test_digest_status.py`, and the three
  live smokes), so the CLI-registration, cross-component-seam, digest, and
  status gaps once recorded here are closed. `test_run_cli.py` asserts
  handler registration of all three coverage work kinds, seed ordering,
  eligibility/stop-supersede behaviour (K5), no-double-work on unchanged
  material, the digest section's presence, and Brave secret redaction.
  `test_seams.py` is no longer `live`-marked — its Wikipedia-to-coverage
  handoff, merge-reconcile, seed-composition, and package-layering (K1)
  checks need no network, so they now run offline, which is why
  `uv run pytest tests/coverage -m live` collects **three** tests, not four.
  `test_digest_status.py` builds populated fixtures with nine distinct
  counter values (1 through 9) and asserts each one's exact rendered line in
  both the digest section and `notable status` output, catching a
  swapped-field defect that the old all-zero version could not.
- **Fixed 2026-08-02: `tests/coverage`'s Brave and OpenRouter assess smokes
  have now been run against real providers and pass.** Both were previously
  written but never executed. Running them exposed two real defects that
  static review by two independent agents had missed, both now fixed on
  `fix/omit-unsupported-sampling-parameters` — see the two entries below.
  This is the concrete case for why a written live smoke is not evidence
  until it has actually run.
- **Fixed 2026-08-02: sending `temperature`/`top_p` made every structured
  generation fail with HTTP 404.** `[openrouter.routing]` sets
  `require_parameters: True`, which tells OpenRouter to exclude any endpoint
  that does not declare every supplied parameter. No endpoint serving
  `openai/gpt-5.4-mini` declares `temperature` or `top_p` — it is a reasoning
  model, controlled by `reasoning_effort` — so supplying them left no
  eligible endpoint and the router answered 404 for all four model tasks.
  `GenerationParameters.temperature` and `.top_p` are now `float | None`
  defaulting to `None`, and `providers/openrouter.py` omits each key when it
  is `None`, exactly as `reasoning_effort` already did. Note this ground was
  contested: `f2e9e76` introduced the omission for the right reason but
  attributed it to null serialization and made the smoke pass `None` into
  then-`float` fields; `7045697` correctly caught that type violation but
  fixed it by making both parameters unconditional, which reintroduced the
  404. Widening the types is what makes the omission legitimate. Do not
  restore either parameter unconditionally; set one only for a model whose
  endpoints advertise it.
- **Fixed 2026-08-02: `resolve_person_entity` and `match_wikipedia_identity`
  sent a schema strict structured output rejects, failing with HTTP 400.**
  Strict mode requires `required` to list every key in `properties`;
  optionality must be carried by a nullable type. Pydantic omits a field
  that has a default, so `selected_person_id` and `selected_page_id` were
  absent from `required` and the provider rejected the schema outright
  ("'required' is required to be supplied and to be an array including every
  key in properties"). Both builders now pass their compacted schema through
  a module-private `_require_every_property` before returning. `detection`
  and `assessment` were already compliant and are unchanged, so no schema
  hash moved for them. The transform is idempotent for a compliant schema.
  This defect was invisible offline because no fake ever enforced the
  provider's strict-schema contract.
- **Fixed:** `config/loader.py`'s `load_config` previously resolved
  `source_policy_file` to an absolute path only into a local variable and the
  configuration snapshot dict, never into the `MainConfig` object itself, so
  `wikipedia/service.py`'s `_schedule_coverage_after_wikipedia_settled` (and
  `people/merge.py`'s `_reconcile_coverage_on_merge`) re-resolved the
  unresolved relative path against the process CWD and silently no-op'd
  outside the config directory. `load_config` now returns `MainConfig` with
  `source_policy_file` already absolute
  (`tests/foundation/test_review_findings.py::
  test_source_policy_file_is_resolved_absolute_on_the_main_config`).
- **K1's `canonical_domain` cross-host alias override is not wired in; same-
  host collapsing already works.** `docs/superpowers/specs/2026-08-01-lead-
  aggregation-and-digest-queue-design.md` locks K1: "`config/source_policies/
  *.toml` rules gain an optional `canonical_domain` key." `PolicyRule` in
  `coverage/screening.py` has no such field — its only fields are `id`,
  `status`, `match`, `rationale`, `review_date`, `provenance_url` — so a
  policy author cannot yet explicitly alias two *different* hosts (e.g.
  `www.example.com` and `example.org`) to one canonical domain for the
  promising-lead threshold. `leads/service.py`'s `_canonical_domain_map` now
  correctly reads each rule's own `host_exact`/`host_suffix` from
  `rule.match` (previously it read `getattr(rule, "host_exact", None)` off
  `rule` itself, which is always `None` since that attribute lives on the
  nested `PolicyMatch`, so the whole map was always empty for every real
  policy — fixed as part of this fix wave). This means two rules that match
  the *same* `host_exact`/`host_suffix` value (or one rule matching many
  articles from that host) already collapse to one domain today; only the
  explicit alias-across-different-hosts override via a `canonical_domain`
  key remains unreachable. `tests/leads/test_service.py::
  test_canonical_domain_map_reads_host_exact_and_host_suffix_from_real_policy`
  builds a real `SourcePolicy` via `source_policy_from_mapping` and proves
  the same-host collapsing path. Adding the `canonical_domain` field is
  deliberately deferred, not done inline: every `PolicyRule` change alters
  `fingerprint_source_policy_document`'s hash (full pydantic `model_dump`),
  which would move `config/source_policies/visual_arts.toml`'s tracked
  fingerprint and every fingerprint-pinned test, coverage plan fingerprint,
  and merge reconciliation path that depends on it — real schema/feature
  surface, not a remediation-scoped fix.
- **Migration `0008_lead_aggregation.sql` was revised in place** (adding
  `material_fingerprint` to `lead_assessment`) rather than superseded by a
  new migration file, per this milestone's own plan authorization — no
  operator had deployed a database against the prior version of 0008 before
  this revision. Any pre-existing local/dev database that already ran the
  old migration 0008 must be deleted and re-migrated from scratch
  (`notable db migrate` will otherwise fail on a checksum mismatch); this is
  a deliberate pre-cutover schema revision, not a violation of the forward-
  only-migrations invariant.
- **`notable audit run --attempt` shows the persisted validated result plus
  provenance, not raw provider request/response bodies, because no raw
  payload is persisted anywhere in the current schema.** This is a
  deliberate recorded amendment to the operator-experience design in
  `docs/superpowers/specs/2026-08-02-audit-and-inspection-design.md`
  ("Amendments to Approved Specifications" → "The `--attempt` "raw response"
  clause"), not an oversight or a milestone 6b-i shortcut. `--attempt`
  renders the attempt's outcome, failure category, provider status, latency,
  byte counts, reserved and actual cost, the handler-owned result row located
  through the `task_type` registry binding (not `(provider, operation)`,
  which is ambiguous: five task types share the single OpenRouter
  `generate_structured` operation), and retry history in
  ordinal order — never a raw request or response body. Adding raw payload
  persistence was considered and rejected; it remains out of scope until a
  later, explicitly approved design revisits it.

## Rewrite Structure

- `src/notable_person_finder/` — installable application package.
  - `cli/` — argument parsing and the `notable` commands.
  - `config/` — strict configuration models, loading, and path resolution.
  - `db/` — SQLite connections and forward-only checked migrations.
  - `runs/` — run engine, clock, repository, work-item scheduling, retry
    coordination, budget reservation, and the mutation lock.
  - `providers/` — the shared HTTP transport, request safety checks, pacing,
    provider failure classification, the feedparser-backed feed adapter, the
    MediaWiki client, the Brave Web Search client, the article fetcher and
    Trafilatura extractor, and the OpenRouter client. This is the only package
    that may import `httpx`, `trafilatura`, or the OpenRouter SDK.
  - `ingestion/` — feed seeding and handling, URL identity, domain models, and
    transaction-neutral persistence helpers for ingestion settlements.
  - `people/` — detection and identity: triage, first-pass resolution,
    reconsideration, confirmed merges, candidate retrieval, prompts, and
    domain validation.
  - `wikipedia/` — MediaWiki query plans, candidate assembly, identity
    observations, match handler, seed/merge hooks.
  - `coverage/` — coverage research: the Wikipedia eligibility gate, coverage
    plans and query forms, publisher screening against the source policy,
    article selection, passage selection, the `assess_article` contract and
    prompt, repository SQL, the three work-item handlers, and merge hooks.
  - `leads/` — lead aggregation and the digest queue: deterministic
    aggregation of per-article assessments into a lead outcome, ranking,
    queue lifecycle and transitions, repository SQL for the lead, queue, and
    digest tables, the `aggregate_person_lead` handler and its scheduling and
    sweep hooks, and merge reconciliation.
  - `audit/` — read-only audit and inspection: the `notable digest show`
    hash-verified digest re-read, the `notable audit run` / `notable audit
    person` repository queries with per-section `_table_present` degradation
    (K3), the `task_type`-keyed result-binding registry (`registry.py` — not
    `(provider, operation)`, which is ambiguous because five task types
    share the single OpenRouter `generate_structured` operation), and
    Markdown rendering (`render.py`). Opens the database `readonly=True` and
    takes no mutation lock.
  - `obs/` — redacting structured logging.
  - `reporting/` — the daily digest writer.
- `tests/foundation/` — application-foundation tests.
- `tests/run_engine/` — run engine and shared transport tests.
- `tests/ingestion/` — feed adapter, domain persistence, CLI integration, seam,
  and opt-in live-smoke tests.
- `tests/people/` — OpenRouter adapter, detection service, repository, CLI
  integration, cross-component seams, and opt-in OpenRouter live-smoke tests.
- `tests/wikipedia/` — MediaWiki adapter, schema, queries/candidates, match
  contract, HTTP and match handlers, seed/merge hooks, digest/status, CLI
  seams, and opt-in MediaWiki/OpenRouter live-smoke tests.
- `tests/coverage/` — substantive coverage for the Brave and article adapters,
  the coverage schema, eligibility, queries, screening, selection, passages,
  the assessment contract, the HTTP/fetch/assess services, repository, merge
  hooks, seed hooks, CLI registration and integration (`test_run_cli.py`),
  cross-component seams (`test_seams.py`), and digest/status rendering
  (`test_digest_status.py`), plus three opt-in live smokes, all three of
  which have now been executed against real providers and pass.
- `tests/leads/` — lead aggregation and digest queue: aggregation and ranking
  logic, queue lifecycle, repository SQL, the handler service and its
  scheduling and fingerprint-reuse behaviour (`test_service.py`), merge hooks,
  CLI registration and same-run firing (`test_run_cli.py`), and digest and
  `notable status` rendering (`test_digest_status.py`). No live smokes: the
  milestone makes no external call.
- `tests/audit/` — digest-hash verification and lookup, the `task_type`-keyed
  result-binding registry, Markdown rendering for run, attempt,
  and person audits, repository queries including per-section schema-
  degradation (K3), CLI integration for all three commands, and cross-
  component seams (`test_seams.py`, including the layering check that
  `audit/` never imports `reporting/digest.py`). No live smokes: the
  milestone makes no external call.
- `docs/architecture/at-least-once-execution.md` — the operator-facing note on
  the crash windows in which a paid provider call can be repeated. Point at it
  rather than restating it.
- `config/*.example.toml` — tracked, copyable configuration examples; local
  configuration variants remain untracked.
- `config/source_policies/visual_arts.toml` — the curated publisher policy. It
  is a tracked product artifact, not an example to copy: changes belong in a
  reviewed diff, and every screening decision records its fingerprint.
- `docs/superpowers/specs/` — approved architecture and policy.
- `docs/superpowers/plans/` — executable milestone plans and completion gates.
- `pyproject.toml` and `uv.lock` — package metadata and frozen dependency graph.

Follow the file structure and interfaces in the active milestone plan. Do not
import or wrap prototype modules to shortcut rewrite work.

## Plans and Specifications

Plans and specs are authorities, not source-code repositories. Keep them
reviewable and executable.

- **Keep milestone plans under ~1,500 lines.** If a plan grows larger, the
  milestone is too big: split it, or move reference material into an appendix
  or separate design note.
- **Reference code blocks are illustrative, not canonical.** Do not paste
  large implementation fragments into a plan. The shipped code and its tests
  govern; the plan describes interfaces, invariants, the completion gate, and
  sequencing.
- **A plan specifies *what* and *why*, not *how* line-by-line.** Include:
  public interfaces, constraints, failure modes, test strategy, and the exact
  verification command that closes the milestone.
- **Track progress in a ledger, not in the plan file.** Use a separate
  progress ledger or task list for checkbox tracking; do not leave the plan
  itself full of unchecked boxes once implementation is complete.
- **Specs are for architecture and policy; plans are for executable
  milestones.** A spec may be long-lived and revised; a plan should be small
  enough to read in one sitting before starting work.

## Environment and Verification

- Python version: 3.13 or newer.
- Install and synchronize dependencies with `uv sync --frozen`.
- Configure repository git hooks: `git config core.hooksPath .githooks`.
- Code quality and static analysis:
  - `uv run ruff check .`
  - `uv run ruff format .`
  - `uv run pyright`
- Run the focused rewrite tests and completion commands named by the active
  milestone plan.
- For the completed application foundation, use
  `uv run pytest tests/foundation`.
- For the completed run engine and shared transport, use
  `uv run pytest tests/run_engine`.
- For completed feed ingestion, use `uv run pytest tests/ingestion`.
- For completed model gateway, detection, and durable person identity, use
  `uv run pytest tests/people`.
- For completed Wikipedia identity matching, use
  `uv run pytest tests/wikipedia`.
- For completed coverage evidence, use `uv run pytest tests/coverage`.
- For completed lead aggregation and the digest queue, use
  `uv run pytest tests/leads`.
- For completed audit and inspection commands, use `uv run pytest tests/audit`.
- The nine completed milestones together gate with
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads tests/audit`.
  Run it from a real checkout: it needs the tracked `config/` directory,
  including `config/source_policies/`.
  Default pytest `addopts` deselect `live`. Opt-in live smokes:
  - feeds: `uv run pytest tests/ingestion -m live -v`
  - OpenRouter (detect + resolve):
    `OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v`
  - MediaWiki + match OpenRouter:
    `uv run pytest tests/wikipedia -m live -v` (OpenRouter key for match smoke)
  - Brave + article fetch + assess OpenRouter:
    `BRAVE_API_KEY=… OPENROUTER_API_KEY=… uv run pytest tests/coverage -m live -v`
    — this collects three real tests, all of which now pass against real
    providers. An adjacent `.env` supplies both keys, so `set -a && . ./.env
    && set +a` before the command is enough; never pass a key inline in a way
    that lands in shell history or output.
- Exercise the installed interface with `uv run notable ...`.
- Keep default rewrite verification offline and isolate configuration and
  storage with temporary paths.
- Do not use bare pytest or the prototype suite as evidence that a rewrite
  milestone passes.

Before reporting completion, run the active plan's full completion gate and
confirm `git diff --check` and `git status --short` are clean as applicable.

## Test Evidence

Tests-first ordering is not evidence that a test discriminates. A test written
before its module fails with `ImportError`; that proves the test runs, not that
it detects the rule it is named for. Milestone 3a lost four rules to exactly
this gap — each test went red for the trivial reason, green once the code
arrived, and stayed green when the rule it was named for was deleted.

Before reporting a task complete:

- For each behaviour the plan names, mutate that rule in the source, confirm a
  **specific named** test fails, then restore. Report which mutation killed
  which test. A rule that survives its own removal is untested.
- A negative assertion needs a positive control. `assert X not in output`
  proves nothing unless some input makes `X` appear; otherwise it passes
  because `X` was never reachable, not because the code excluded it.
- If source was written before its tests — after an interruption, or because a
  task was recovered — every named rule needs this evidence, not a sample.
  That ordering is how the four escapes above were introduced.

Restore mutated source from a `cp` backup and verify with `diff`, never with
`git stash`: the stash stack is shared across worktrees and other sessions.

Run mutations with `PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__` between
iterations. Rewriting one file repeatedly inside the same second produces cache
entries that CPython's mtime-and-size invalidation accepts, so an iteration can
silently test the *previous* mutation. This has already produced two false
`SURVIVED` verdicts in this repository — the direction that matters, because a
survivor reported as killed is a rule you believe is covered and is not.

## Foundation Invariants

- The application never edits Wikipedia automatically. Every Wikipedia edit or
  publication action requires explicit human review and approval.
- Distribution: `notable-person-finder`; import package:
  `notable_person_finder`; executable: `notable`.
- Configuration is strict and file-first. Environment variables supply secrets
  only, and secrets must never appear in snapshots, fingerprints, diagnostics,
  terminal output, or tests. An adjacent `.env` may fill missing secret values,
  but the process environment takes precedence.
- SQLite migrations are forward-only, checksummed, transactional, and backed
  up before changing an existing database.
- Mutating commands use the nonblocking, OS-managed lock scoped to one data
  root. File contents are diagnostic and never determine lock ownership.
- Every external network call maps to exactly one persisted attempt attributed
  to its run and work item. Only the central retry coordinator starts a repeat
  request. No transaction is held across its own item's network call: sibling
  calls may still be in flight while the application thread settles another
  item's short, local SQLite transaction.
- External execution is at-least-once. See
  `docs/architecture/at-least-once-execution.md`.
- Preserve these reviewed contracts unless a later approved design explicitly
  replaces them.

## Branch, Pull Request, and Review Workflow

`refactor/rearchitecture` is the rewrite integration branch. Implement changes
on a focused feature branch, preferably in an isolated worktree.

### New rewrite work

New milestone, feature, or design work follows the full path:

1. Implement and verify the active milestone on the feature branch.
2. Obtain explicit user authorization before publishing external changes.
3. Push the feature branch and open a pull request targeting
   `refactor/rearchitecture`.
4. Have a separate agent independently review the pull request.
5. Fix all Critical and Important findings and obtain scoped re-review of the
   fixes.
6. Merge only after review approval and explicit user authorization.

Do not directly merge a feature branch into `refactor/rearchitecture` as the
normal completion path.

### Remediation of review findings

Remediation is not new work. When a review produces findings against work that
is already on the integration branch, the fixes do not need their own pull
request or their own independent review — the review that produced them already
supplied the independent judgement. Commit them on a focused branch and merge
into `refactor/rearchitecture` with explicit user authorization.

Treat work as remediation only when all of the following hold:

- the findings come from a completed review of work already integrated;
- every change traces to a specific finding, and no change adds feature
  surface, a new interface, or a new dependency;
- each behavioural fix carries a regression test that fails before the fix and
  passes after it;
- the active milestone's full verification gate passes.

The classification is not the implementing agent's to stretch. If the work grows
past the findings, it is no longer remediation: stop, open a pull request, and
follow the full path. When in doubt, use the full path.

### Always

Do not push, open or modify pull requests, merge, or target `main` without
explicit user authorization, on either path. Only the complete, verified rewrite
integration branch may merge into `main`, and only as a later explicit cutover
decision.

## Legacy Prototype

The root-level pipeline, `run_pipeline.py`, `scripts/`, JSONL state, and their
tests are the preserved legacy prototype. They are historical evidence and the
operational fallback until product cutover. Do not import, reorganize, remove,
run, or repair them unless the user explicitly requests legacy work. Prototype
tests are not a rewrite gate.
