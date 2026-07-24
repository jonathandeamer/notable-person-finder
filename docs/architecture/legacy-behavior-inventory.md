# Legacy Behavior Inventory

**Status:** In progress; three of seven capabilities approved
**Date:** 2026-07-24
**Branch:** `refactor/rearchitecture`

## Purpose

This inventory records which observable behaviors from the prototype should be
preserved, changed, or deleted in the clean-slate rewrite. Legacy tests,
prompts, documentation, and implementation are evidence of old behavior, not
requirements by themselves.

The review process and authority rules are defined in the
[redesign programme design](../superpowers/specs/2026-07-24-redesign-program-design.md).

## Decision Legend

- **Preserve:** retain the observable behavior as a product, correctness, or
  safety requirement.
- **Change:** retain its intent but define different observable semantics.
- **Delete:** remove obsolete behavior or accidental implementation
  complexity.
Each preserved or changed behavior identifies its expected replacement
verification. Exact schemas, thresholds, and module boundaries remain matters
for the focused design sessions.

## Capability Files

| Capability | Status | File |
| --- | --- | --- |
| Discovery and feed ingestion | Approved | [01-discovery-and-ingestion.md](legacy-behaviors/01-discovery-and-ingestion.md) |
| Person detection and initial triage | Approved | [02-person-detection-and-triage.md](legacy-behaviors/02-person-detection-and-triage.md) |
| Wikipedia identity matching | Approved | [03-wikipedia-identity-matching.md](legacy-behaviors/03-wikipedia-identity-matching.md) |
| Coverage discovery and source reliability | Not started | `04-coverage-and-source-reliability.md` |
| Notability assessment and ranking | Not started | `05-assessment-and-ranking.md` |
| Digest generation | Not started | `06-digest-generation.md` |
| Failures, retries, budgets, and resumability | Not started | `07-failures-budgets-and-resumability.md` |

## Cross-Capability Rules

- Product decisions already approved on this branch take precedence over
  prototype behavior.
- Prototype file formats, subprocess boundaries, and command-line compatibility
  are not observable product requirements.
- A deleted behavior receives no compatibility layer or replacement test.
- Behavior created for a source set that is outside the visual-arts pilot is
  not generalized without a current product reason.
- The arts sources are pilot configuration. The application must accept other
  standards-based feed configurations without code changes or publisher-aware
  branches.
