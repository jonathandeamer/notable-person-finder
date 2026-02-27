# Overview

This repo implements a deterministic pipeline for identifying people who may merit a Wikipedia biography, with bounded AI assistance at specific gates.

High-level flow:
1. RSS ingest (deterministic)
2. Gate 0 prefilter (deterministic name heuristic)
3. Gate 1 (LLM triage with strict JSON)
4. MediaWiki candidate search (deterministic)
5. Gate 2 (deterministic has-page filter)
6. Gate 3 (LLM page match decision) — not implemented yet
7. Coverage search + Gate 4 (LLM notability signal) — not implemented yet

This project never makes automatic edits to Wikipedia. Human review is required for any public action.
