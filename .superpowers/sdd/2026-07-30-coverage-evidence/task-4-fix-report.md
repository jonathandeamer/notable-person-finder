# Task 4 Fix Report — Important test discrimination

**Date:** 2026-07-30  
**Branch:** `feat/coverage-evidence`  
**Review:** `task-4-review.md` (2 Important, 0 Critical)

## Fixes

| Finding | Change |
| --- | --- |
| Important 1: fingerprint content-sensitivity untested | Added `test_fingerprint_changes_when_policy_rules_change` — same key/label, rule status change and append each produce distinct 64-hex fingerprints |
| Important 2: `max_unclassified_fetches` cap untested | Added `test_max_unclassified_fetches_caps_surplus_fallback` — 1 eligible + 4 unclassified, cap 2 → exactly 3 selected (1+2), ordered, K34 reasons |
| Minor 1: dead no-op list | Removed `_ = [c for c in candidates if … _INELIGIBLE]`; comment that partitions omit ineligible |

## Mutation evidence

| Mutation | Killed by |
| --- | --- |
| `fingerprint_source_policy_document` → always `"0"*64` | `test_fingerprint_changes_when_policy_rules_change` |
| Hash only `key`/`label` (ignore rules) | `test_fingerprint_changes_when_policy_rules_change` |
| Remove unclassified fallback slot cap (select all) | `test_max_unclassified_fetches_caps_surplus_fallback` |

Restore: `cp` backup + `diff`; `PYTHONDONTWRITEBYTECODE=1` + clear `__pycache__`.

## Verification

```text
uv run pytest tests/coverage/test_screening.py tests/coverage/test_selection.py \
  tests/coverage/test_queries.py tests/coverage/test_eligibility.py -q
# 46 passed
```
