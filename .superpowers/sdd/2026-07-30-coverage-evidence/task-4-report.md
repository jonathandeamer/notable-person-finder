# Task 4 Report: Source policy, screening, selection, queries + Wikipedia gate

**Status:** Complete  
**Commit:** `ebd786f` — `feat(coverage): policy, selection, queries, eligibility gate`  
**Branch:** `feat/coverage-evidence`  
**Date:** 2026-07-30

## Delivered

| Item | Path |
| --- | --- |
| Screening | `src/notable_person_finder/coverage/screening.py` |
| Selection | `src/notable_person_finder/coverage/selection.py` |
| Queries | `src/notable_person_finder/coverage/queries.py` |
| Eligibility (pure) | `src/notable_person_finder/coverage/eligibility.py` |
| Example policy | `config/source_policies/visual_arts.example.toml` |
| Config | `MainConfig.source_policy_file` required; loader loads/validates policy |
| Example TOML | `config/notable.example.toml` sets `source_policy_file` |
| Tests | `tests/coverage/test_{screening,selection,queries,eligibility}.py` |
| Loader hard-fail | `tests/foundation/test_config_loader.py::test_missing_or_invalid_source_policy_fails_config_load` |

No HTTP handlers, service seed, or assess work (Tasks 5–9).

## Interfaces

```python
# screening.py
def load_source_policy(path: Path) -> SourcePolicy: ...
def fingerprint_source_policy(policy: SourcePolicy) -> str: ...  # 64 hex
def screen_url(policy, *, url, publisher_key_value=None) -> ScreeningDecision: ...
def attach_discovery_url(policy, *, url) -> DiscoveryAttachDecision: ...  # K10 pure

# selection.py
def eligible_selected_count(candidates, *, max_eligible_fetches) -> int: ...
def final_selection(...) -> tuple[SelectedArticle, ...]: ...
# SelectedArticle.selection_reason ∈ K34 four values

# queries.py
COVERAGE_QUERY_PLAN_VERSION = 1
def generate_exact_forms(names, *, max_exact_forms, death_supported=False) -> tuple[QueryFormSpec, ...]: ...
def generate_alias_forms(alias_names, *, existing_query_texts, max_alias_forms) -> tuple[QueryFormSpec, ...]: ...
def generate_context_form(display_name, context_terms, *, existing_query_texts) -> QueryFormSpec | None: ...

# eligibility.py
def wikipedia_allows_coverage_research(wikipedia: WikipediaIdentityView) -> bool: ...
def is_coverage_research_eligible(view: CoverageEligibilityView) -> bool: ...  # pure matrix
```

URL identity reuses `ingestion.urls.canonicalize_article_url` and `publisher_key` only.

## Behavior locks

| Lock | Implementation |
| --- | --- |
| K8 first-match | Rules in file order; first `_rule_matches` wins |
| Unclassified default | `default.unclassified` when no rule matches |
| Fingerprint stability | SHA-256 of canonical JSON of validated policy document |
| K10 usable | `DISCOVERY_AND_SCREENING` with non-null screening + canonical URL |
| K10 unusable | `SCREENING_ONLY` / `rule_status=unusable` / `url_unusable` |
| K10 empty URL | `NEITHER` — no screening, no discovery |
| K11/K34 | Four-value `selection_reason`; curated-ineligible never selected |
| K6 exact always | Quoted honorific-stripped names; no nickname/initials map |
| K6 obituary | Only when `death_supported=True`; counts against `max_exact_forms` |
| K5 gate | matching → false; no_match/uncertain → true; null/failed → false |
| K33 policy required | Missing path or unsupported `schema_version` → `ConfigLoadError` |

## Verification

```text
uv run pytest tests/coverage/test_screening.py tests/coverage/test_selection.py \
  tests/coverage/test_queries.py tests/coverage/test_eligibility.py -q
# 44 passed (+ loader hard-fail test separately)

uv run ruff check src/notable_person_finder/coverage src/notable_person_finder/config
uv run pyright src/notable_person_finder/coverage src/notable_person_finder/config
# clean
```

Broader smoke: foundation/coverage/people/wikipedia suites green after
`source_policy_file` fixture updates (helpers write minimal policy TOML).

## Mutation evidence

Restore: `cp` backup + `diff` verify. `PYTHONDONTWRITEBYTECODE=1` + clear
`__pycache__` between iterations.

| Mutation | Killed by |
| --- | --- |
| Expand `_ELIGIBLE_OUTCOMES` with `matching_page_found` **and** remove explicit matching stop | `test_matching_page_found_stops_even_when_other_gates_pass` |
| `_reason` emit bare `"discovery"` | `test_k34_four_reasons_matrix` |
| Last-match instead of first-match in `screen_url` | `test_first_match_wins_over_later_broader_rule` |
| Empty discovery URL → screening_only | `test_discovery_empty_url_is_neither` |
| Always emit obituary (`if True` for death) | `test_obituary_only_when_death_supported` |
| Include `curated_ineligible` in eligible partition | `test_curated_ineligible_never_selected` |

Note: removing only the explicit matching stop while the allowlist stays correct
**survives** (allowlist already excludes matching). The named K5 test still
fails when matching is wrongly allowlisted without the stop.

## Self-review

- Pure modules only; discovery attach is decision-level without DB inserts.
- Full DB `is_coverage_research_eligible` with material fingerprints deferred
  to seed/service (Task 8); pure view matrix locks K5 now.
- `AssessArticleConfig` remains Task 5.
