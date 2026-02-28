# Overview

This repo executes a deterministic, staged pipeline that flags people who may merit a Wikipedia biography while keeping LLM usage scoped to semantic “gates”.

High-level flow:
1. RSS ingest (`ingest/rss_ingest.py`)
2. Gate 0 prefilter (deterministic heuristics)
3. Gate 1 triage (LLM + strict JSON schema)
4. Gate 1 index update — writes `SKIP_GLOBALLY_KNOWN` decisions to `wiki_known_pages.json` (deterministic)
5. MediaWiki candidate search (deterministic)
6. Gate 2 has-page filter (deterministic)
7. Gate 3 page-match (LLM)
8. Gate 3 index update — writes `HAS_PAGE` decisions + alias keys to `wiki_known_pages.json` (deterministic)
9. Brave coverage search + Gate 4 reliable-filter (deterministic)
10. Gate 4b coverage verifier (LLM counting distinct reliable Brave domains)
11. Digest/report generation (`scripts/det_openclaw_daily_digest.py`, `scripts/daily_notability_digest_report.py`)

Outputs (state/ + openclaw digest) are records of candidate decisions and reliable-source coverage; automatic Wikipedia edits are never performed—final actions require human review.
