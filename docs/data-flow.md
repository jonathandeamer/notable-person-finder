# Data Flow

```mermaid
flowchart TD
    A[RSS feeds] --> B["ingest/rss_ingest.py<br/>state/events.jsonl"]
    J["state/wiki_known_pages.json"] --> C
    B --> C["scripts/det_gate0_prefilter.py<br/>state/prefilter_pass.jsonl"]
    B --> D["scripts/det_gate0_prefilter.py<br/>state/prefilter_skip.jsonl"]
    C --> E["scripts/llm_gate1_runner.py<br/>state/gate1_llm_results.jsonl"]
    E --> L["scripts/det_gate1_index_update.py<br/>(SKIP_GLOBALLY_KNOWN → wiki_known_pages.json)"]
    L --> J
    L --> F["scripts/det_mw_candidates.py<br/>state/wiki_candidates.jsonl"]
    F --> G["scripts/det_gate2_has_page.py<br/>state/wiki_candidates_pass.jsonl"]
    F --> K["scripts/det_gate2_has_page.py<br/>state/wiki_candidates_skip.jsonl"]
    G --> M["scripts/llm_gate3_runner.py<br/>state/gate3_llm_results.jsonl"]
    M --> N["scripts/det_gate3_index_update.py<br/>state/wiki_known_pages.json"]
    N --> J
    G --> J
    K --> J
    M --> O["scripts/det_brave_coverage.py<br/>state/brave_coverage.jsonl"]
    O --> P["scripts/det_gate4_reliable_filter.py<br/>state/gate4_reliable_coverage.jsonl"]
    P --> Q["scripts/llm_gate4b_runner.py<br/>state/gate4b_llm_results.jsonl"]
    Q --> R["output/openclaw/daily_notability_digest.json"]
    R --> S["scripts/daily_notability_digest_report.py"]
```

## Notes

- Deterministic stages: ingest, Gate 0 prefilter, Gate 1 index update, MediaWiki candidates, Gate 2 has-page, Gate 3 index update, Gate 4 reliable filtering.
- LLM stages: Gate 1 triage, Gate 3 page-match, Gate 4b coverage verifier (two-pass: first pass counts distinct domains from a curated Wikipedia-reliable source list → `LIKELY_NOTABLE`; second pass asks the LLM to judge source reliability from the full Brave result set → `POSSIBLY_NOTABLE`).
- Summary artifacts:
  - `state/gate4b_llm_results.jsonl` (per-event domain counts)
  - `output/openclaw/daily_notability_digest.json` + `scripts/daily_notability_digest_report.py`
