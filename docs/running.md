# Running the Pipeline (Current Stages)

This page documents the commands for the stages implemented so far.

## 1) RSS Ingest

```bash
python3 -m ingest.rss_ingest \
  --feeds /Users/jonathan/new-wikipedia-article-checker/config/feeds.md \
  --state-dir /Users/jonathan/new-wikipedia-article-checker/state
```

## 2) Gate 0 Prefilter (Deterministic)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/det_gate0_prefilter.py \
  --events /Users/jonathan/new-wikipedia-article-checker/state/events.jsonl \
  --pass-output /Users/jonathan/new-wikipedia-article-checker/state/prefilter_pass.jsonl \
  --skip-output /Users/jonathan/new-wikipedia-article-checker/state/prefilter_skip.jsonl
```

## 3) Gate 1 (LLM)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/llm_gate1_runner.py \
  --backend codex-cli \
  --codex-cwd /Users/jonathan/new-wikipedia-article-checker \
  --events /Users/jonathan/new-wikipedia-article-checker/state/prefilter_pass.jsonl \
  --prompt /Users/jonathan/new-wikipedia-article-checker/prompts/gate1.md \
  --output /Users/jonathan/new-wikipedia-article-checker/state/gate1_llm_results.jsonl \
  --sample-size 60
```

## 4) MediaWiki Candidate Search (Deterministic)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/det_mw_candidates.py \
  --input /Users/jonathan/new-wikipedia-article-checker/state/gate1_llm_results.jsonl \
  --output /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates.jsonl \
  --overwrite \
  --search-max-results 10 \
  --progress-every 5 \
  --log-file /Users/jonathan/new-wikipedia-article-checker/state/mw_candidates.log
```

Notes:
- Output is written incrementally (one JSON line per record), so you can `tail -f` during a run.
- Reduce `--throttle-ms` to speed up requests, but be mindful of rate limits.
- `biography_score` is a heuristic (>= 3 means likely biography); `biography_prioritized` is true when the score is >= 3.

## 5) Gate 2 (Deterministic Has-Page Filter)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/det_gate2_has_page.py \
  --input /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates.jsonl \
  --pass-output /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates_pass.jsonl \
  --skip-output /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates_skip.jsonl \
  --known-pages /Users/jonathan/new-wikipedia-article-checker/state/wiki_known_pages.json \
  --overwrite
```

## 6) Gate 3 (LLM Page Match)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/llm_gate3_runner.py \
  --input /Users/jonathan/new-wikipedia-article-checker/state/wiki_candidates_pass.jsonl \
  --prompt /Users/jonathan/new-wikipedia-article-checker/prompts/gate3.md \
  --output /Users/jonathan/new-wikipedia-article-checker/state/gate3_llm_results.jsonl \
  --model claude-sonnet-4-6
```

## 6a) Gate 3 Index Update (after Gate 3)

```bash
python3 /Users/jonathan/new-wikipedia-article-checker/scripts/det_gate3_index_update.py \
  --input /Users/jonathan/new-wikipedia-article-checker/state/gate3_llm_results.jsonl \
  --known-pages /Users/jonathan/new-wikipedia-article-checker/state/wiki_known_pages.json
```

## 7) Coverage Search + Gate 4 (LLM Notability Signal)

Not implemented yet.
