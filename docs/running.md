# Running the Pipeline (Current Stages)

All commands execute from the project root (`/home/admin/notable-person-finder` by default).

## 1) RSS Ingest

```bash
python3 -m ingest.rss_ingest \
  --feeds config/feeds.md \
  --state-dir state
```

## 2) Gate 0 Prefilter (deterministic)

```bash
python3 scripts/det_gate0_prefilter.py \
  --events state/events.jsonl \
  --pass-output state/prefilter_pass.jsonl \
  --skip-output state/prefilter_skip.jsonl \
  --known-pages state/wiki_known_pages.json
```

## 3) Gate 1 (LLM triage)

```bash
python3 scripts/llm_gate1_runner.py \
  --backend codex-cli \
  --codex-cwd . \
  --events state/prefilter_pass.jsonl \
  --prompt prompts/gate1.md \
  --output state/gate1_llm_results.jsonl \
  --sample-size 60
```

## 4) Gate 1 index update (deterministic)

```bash
python3 scripts/det_gate1_index_update.py \
  --input state/gate1_llm_results.jsonl \
  --known-pages state/wiki_known_pages.json
```

## 5) MediaWiki candidate search

```bash
python3 scripts/det_mw_candidates.py \
  --input state/gate1_llm_results.jsonl \
  --output state/wiki_candidates.jsonl \
  --overwrite \
  --search-max-results 10 \
  --progress-every 10 \
  --log-file state/mw_candidates.log
```

## 6) Gate 2 has-page filter

```bash
python3 scripts/det_gate2_has_page.py \
  --input state/wiki_candidates.jsonl \
  --pass-output state/wiki_candidates_pass.jsonl \
  --skip-output state/wiki_candidates_skip.jsonl \
  --known-pages state/wiki_known_pages.json \
  --overwrite
```

## 7) Gate 3 (LLM page match) + index update

```bash
python3 scripts/llm_gate3_runner.py \
  --backend codex-cli \
  --codex-cwd . \
  --input state/wiki_candidates_pass.jsonl \
  --prompt prompts/gate3.md \
  --output state/gate3_llm_results.jsonl

python3 scripts/det_gate3_index_update.py \
  --input state/gate3_llm_results.jsonl \
  --known-pages state/wiki_known_pages.json
```

## 8) Brave coverage + Gate 4 reliable filter

```bash
python3 scripts/det_brave_coverage.py \
  --input state/gate3_llm_results.jsonl \
  --overwrite \
  --cache-dir state/brave_cache \
  --api-key "$BRAVE_API_KEY"

python3 scripts/det_gate4_reliable_filter.py \
  --input state/brave_coverage.jsonl \
  --output state/gate4_reliable_coverage.jsonl \
  --overwrite
```

## 9) Gate 4b (LLM coverage verifier counting distinct domains)

```bash
python3 scripts/llm_gate4b_runner.py \
  --backend codex-cli \
  --codex-cwd . \
  --prompt prompts/gate4b.md \
  --unlisted-prompt prompts/gate4b_unlisted.md \
  --brave-input state/gate4_reliable_coverage.jsonl \
  --output state/gate4b_llm_results.jsonl \
  --fresh-output
```

## 10) Digest + report for OpenClaw

```bash
python3 scripts/det_openclaw_daily_digest.py \
  --window-hours 24 \
  --output output/openclaw/daily_notability_digest.json

python3 scripts/daily_notability_digest_report.py
```

### Notes
- Use `run_pipeline.py` (default `--state-dir state --output output`) to chain all stages end-to-end; it writes `state/runs/*.json` and `output/runs/*_summary.json`.
- Gate 4b now requires two distinct reliable Brave domains (first or second pass) to mark someone `LIKELY_NOTABLE`; otherwise it falls back to `POSSIBLY_NOTABLE` or `NOT_NOTABLE`.
- The digest report script refreshes `output/openclaw/daily_notability_digest.json` before summarizing.
