# Overview

This repo executes a deterministic, staged pipeline that flags people who may merit a Wikipedia biography while keeping LLM usage scoped to semantic “gates”.

High-level flow:
1. RSS ingest (`ingest/rss_ingest.py`)
2. Gate 0 prefilter (deterministic heuristics)
3. Gate 1 triage (LLM + strict JSON schema)
4. MediaWiki candidate search (deterministic)
5. Gate 2 has-page filter (deterministic)
6. Gate 3 page-match (LLM)
7. Brave coverage search + Gate 4 reliable-filter (deterministic)
8. Gate 4b coverage verifier (LLM counting distinct reliable Brave domains)
9. Digest/report generation (`scripts/det_openclaw_daily_digest.py`, `scripts/daily_notability_digest_report.py`)

Outputs (state/ + openclaw digest) are records of candidate decisions and reliable-source coverage; automatic Wikipedia edits are never performed—final actions require human review.
