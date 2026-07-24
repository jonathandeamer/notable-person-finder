# Legacy Behavior Review: Person Detection and Initial Triage

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability identifies people who receive meaningful individual focus in a
feed item and decides whether current feed metadata justifies further research.
It does not decide Wikipedia notability and does not use outside knowledge.

Primary legacy evidence:

- `scripts/det_gate0_prefilter.py`
- `scripts/llm_gate1_runner.py`
- `scripts/det_gate1_index_update.py`
- `prompts/gate1.md`
- `tests/test_prefilter_gate0.py`
- `tests/test_gate1_trial_retries.py`
- Gate 1 fixtures and assertions in `tests/test_smoke_pipeline.py`

## Prototype Context

Much of Gate 0 and several Gate 1 exclusions were introduced while the
prototype monitored broad BBC local-news feeds and a Guardian feed containing
reader-written obituaries. Those sources produced large volumes of named but
privately notable people, letters, tributes, and structural roundup content.
They are not part of the visual-arts pilot. Their noise mitigations are not
general product requirements.

## Approved Dispositions

| Observable behavior | Legacy evidence | Decision and rationale | Replacement verification |
| --- | --- | --- | --- |
| English capitalization and name regexes decide whether an article reaches semantic triage. | `FULL_NAME_PATTERN`, `INITIAL_SURNAME_PATTERN`, and Gate 0 tests | **Delete.** Every technically usable item reaches bounded, high-recall semantic triage. The regexes are brittle for diverse names and stylized identities. | Workflow pytest assertion; semantic coverage in Promptfoo |
| Hard-coded phrases reject letters, “in pictures,” trivia, and family tributes. | Structural and personal-obituary Gate 0 tests | **Delete.** These were source-specific noise controls. Relevant examples may be retained as prompt-evaluation negatives, not Python policy. | No production rule; selected Promptfoo cases |
| A normalized name in the known-pages JSON skips triage before identity is checked. | `test_known_pages_skip`; `wiki_known_pages.json` | **Delete.** A bare normalized name is not a safe identity key. Confirmed Wikipedia identities are handled by the later identity capability. | Persistence constraint and identity-routing tests defined later |
| Triage uses only title, summary, source, and publication date. | `map_gate_input`; Gate 1 prompt constraints | **Preserve and clarify.** Use normalized feed metadata plus provenance. The model must not rely on remembered facts about the person. | Promptfoo grounding cases and request-contract tests |
| One article produces at most one candidate person. | Singular Gate 1 schema | **Change.** Return zero or more people who receive meaningful individual focus, including joint profiles and artist duos. Merely listed, quoted, or mentioned people do not qualify. A configurable safety cap must expose overflow rather than silently truncate it. | Promptfoo multi-person cases; schema and overflow tests |
| A person must have a two-token full name. | Gate 1 single-token hard failure | **Change.** Accept a mononym, pseudonym, or professional name when the text clearly presents it as the person's public identity. Ambiguous surname-only references remain unresolved. | Promptfoo cases for professional mononyms and ambiguous surnames |
| The model returns both the written and expanded full name. | Gate 1 schema | **Change.** Preserve the exact source form and derive a search form only from supplied text, such as by removing an honorific. Never invent a legal name or expand initials from model memory. | Structured-output and grounding tests |
| The article must be about one primary individual. | `primary_focus` and Gate 1 prompt | **Change.** Require meaningful individual focus, not necessarily a sole primary subject. Multiple people can qualify when each receives substantive career or biographical context. | Promptfoo joint-profile, list, quotation, and group-show cases |
| A plausible durable career, body of work, public role, or achievement merits later research. | Strong/weak pass criteria | **Preserve.** This is the high-recall purpose of triage, not a notability conclusion. | Promptfoo representative positive cases |
| Passing mentions, spokesperson quotations, and purely private single-event roles do not merit research. | Gate 1 failure criteria and examples | **Preserve and clarify.** Apply this based on supplied evidence, not categorical assumptions about crime, locality, or profession. Sparse evidence becomes semantic uncertainty. | Promptfoo negative and sparse-context cases |
| Crime, controversy, illness, accident, or death framing can automatically disqualify a person. | Gate 1 single-event exclusions | **Delete.** Continue when the supplied item also indicates a durable public footprint. | Paired Promptfoo cases with and without career context |
| A publisher-written editorial obituary about an identified person always proceeds. | Editorial-obituary auto-pass | **Preserve.** It is inherently person-focused and normally supplies career context, but it does not itself prove notability. | Promptfoo obituary cases |
| Reader letters and family tributes receive special treatment. | Gate 0 personal-obituary and letters rules | **Delete.** The relevant Guardian feed is not in the pilot and no general special case is required. | No replacement test |
| Awards, national political roles, and selected achievements force a strong pass. | Gate 1 hard auto-pass rules | **Change.** Record award, office, profile, obituary, and similar facts as reasons to research. They never assert notability before coverage is evaluated. | Promptfoo signal-extraction cases and schema tests |
| Triage returns `STRONG_PASS`, `WEAK_PASS`, `FAIL`, or `SKIP_GLOBALLY_KNOWN`. | Gate 1 schema and smoke tests | **Change.** Use `research`, `do_not_research`, or semantic `uncertain`, with subject focus, signals, and rationale represented separately. Exact names belong to the LLM design. | Schema tests, workflow transition tests, and Promptfoo cases |
| Semantic uncertainty is folded into weak pass, fail, or model confidence. | Gate 1 prompt and categorical confidence | **Change.** Escalate genuine ambiguity once when configured and affordable. If a usable identity remains and focus is plausible, continue research with uncertainty visible. Otherwise retain a capped needs-review item. Budget-deferred escalation remains pending. | Workflow pytest tests and Promptfoo ambiguity cases |
| Provider failure, invalid output, and budget exhaustion are semantic uncertainty. | Runner error paths | **Change.** These are typed operational outcomes with separate retry and reporting behavior. | Failure and budget tests in capability 7 |
| Model training knowledge can assert that a famous person already has a Wikipedia page. | `SKIP_GLOBALLY_KNOWN`; Gate 1 index update | **Delete.** Every candidate uses current MediaWiki evidence. Do not cache model guesses as page identities. | Workflow test ensuring famous fixtures reach MediaWiki |
| A fixed confidence label and signal taxonomy are part of the product contract. | Gate 1 schema enums | **Change.** Preserve visible uncertainty, grounded rationale, and useful signals, but redesign the exact typed fields and escalation semantics in the LLM specification. | Schema and Promptfoo contract tests after design approval |
| Model output is structured and includes a short grounded rationale. | Gate 1 JSON schema and tests | **Preserve.** Strict local validation and complete attempt provenance replace permissive parsing. | Schema-validation and provenance tests |
| Gate 1 production behavior includes random samples, keyword trial filters, batch event-ID recovery, and append-only last-write-wins output. | Gate 1 runner flags and retry tests | **Delete as production behavior.** Prompt experimentation belongs in Promptfoo; production uses durable work state, typed attempts, and explicit retries. | Promptfoo configuration tests and persistence tests defined later |

## Provisional Uncertainty Flow

The exact thresholds and model routing remain part of later workflow and LLM
design, but the observable intent is approved:

1. Genuine semantic ambiguity may receive one configured stronger-model
   escalation when budget permits.
2. A result with a usable person identity and plausible individual focus
   continues to MediaWiki and coverage research with its uncertainty visible.
3. A result without a stable person identity is retained in a small, capped
   needs-review section rather than entering the ranked candidate shortlist.
4. An escalation prevented by budget remains explicitly pending for a later
   run.
5. Technical failures are never represented as semantic judgments.

## Verification Boundary

Pytest verifies deterministic routing, typed schema validation, persistence,
overflow, and operational separation. Promptfoo evaluates the semantic
behavior using representative positive, negative, multiple-subject, mononym,
passing-mention, sparse-context, obituary, and ambiguity cases. Prototype
examples survive only when they represent the pilot or a general regression
risk.
