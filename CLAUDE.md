# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Summary

Wikipedia Notability Finder: A deterministic Python pipeline that identifies people who may merit Wikipedia biographies. The pipeline uses bounded AI assistance at specific semantic gates, while maintaining strict control flow via deterministic Python. **Critical: Never auto-edits Wikipedia—all final actions require human review.**

## Development Setup

### Python Environment
- **Version**: Python 3.13 (configured in `.venv/pyvenv.cfg`)
- **Virtual Environment**: Located at `.venv/` (already created)
- **Dependencies**: Minimal; uses mostly standard library + `requests`

### Running Tests
Use Python's built-in `unittest` module (no pytest required):

```bash
# Run all tests
python3 -m unittest discover tests/

# Run a single test file
python3 -m unittest tests.test_name_utils -v

# Run a single test case
python3 -m unittest tests.test_name_utils.TestNameUtils.test_normalize_basic -v
```

## Common Commands

### Run the Full Pipeline
```bash
python3 run_pipeline.py [options]
```

Key options:
- `--dry-run` — Preview stages without executing
- `--from-gate STAGE_NAME` — Resume from a specific stage (useful for iterative development)
- `--gate1-budget N` — Limit Gate 1 LLM calls to N records (default 50)
- `--model-gate1 MODEL` — Override Gate 1 model (default: `gpt-5.2`)
- `--model-gate3 MODEL` — Override Gate 3 model (default: `gpt-5.2`)
- `--model-gate4b MODEL` — Override Gate 4b model (default: `gpt-5.2`)
- `--backend-gate1 BACKEND` — LLM backend for Gate 1 (default: `codex-cli`)
- `--backend-gate3 BACKEND` — LLM backend for Gate 3 (default: `codex-cli`)
- `--backend-gate4b BACKEND` — LLM backend for Gate 4b (default: `codex-cli`)

Example:
```bash
python3 run_pipeline.py --dry-run --from-gate gate1 --gate1-budget 50
```

### Run Individual Pipeline Stages
Each stage is a standalone Python script in `scripts/`:

```bash
# Gate 0: Deterministic prefilter
python3 scripts/det_gate0_prefilter.py --events state/events.jsonl \
  --pass-output state/prefilter_pass.jsonl

# Gate 1: LLM triage
python3 scripts/llm_gate1_runner.py --events state/prefilter_pass.jsonl \
  --prompt prompts/gate1.md --output state/gate1_llm_results.jsonl

# Gate 2: Has-page filter
python3 scripts/det_gate2_has_page.py --input state/wiki_candidates.jsonl \
  --pass-output state/wiki_candidates_pass.jsonl \
  --known-pages state/wiki_known_pages.json
```

See `docs/running.md` for complete stage commands.

## Architecture & Design

### Pipeline Stages (11 total)
1. **RSS Ingest** — Fetches feeds from `config/feeds.md`, appends to `state/events.jsonl`
2. **Gate 0** — Deterministic name-heuristic filter; uses `state/wiki_known_pages.json` to skip already-known names
3. **Gate 1** — LLM triage (name-strict, high-recall); outputs structured JSON with decisions: `STRONG_PASS`/`WEAK_PASS`/`FAIL`
4. **MediaWiki Candidates** — Deterministic Wikipedia API search; fetches article metadata
5. **Gate 2** — Deterministic has-page filter using biography score + Levenshtein distance; updates known pages index
6. **Gate 3** — LLM page-match decision; compares candidate to parsed Wikipedia content
7. **Gate 3 Index Update** — Writes `HAS_PAGE` decisions back to `state/wiki_known_pages.json`
8. **Brave Coverage** — Deterministic Brave Search News API queries; caches results
9. **Gate 4 Reliable Filter** — Keeps only Wikipedia-approved news source domains
10. **Gate 4b LLM Coverage Verifier** — Two-pass LLM coverage verifier: first pass counts distinct domains from a curated Wikipedia-reliable source list (`LIKELY_NOTABLE` if ≥2); second pass asks the LLM to judge source reliability itself from the full Brave result set (`POSSIBLY_NOTABLE` if combined domains ≥2)
11. **Report + Digest** — Generates summary output plus `output/openclaw/daily_notability_digest.json` for external agents (see `scripts/daily_notability_digest_report.py`)

### Key Design Principles
- **Deterministic control flow**: All state transitions managed by deterministic Python; LLM only at semantic gates
- **Strict schemas**: All LLM outputs validated against JSON schemas; fail-safe to `UNCERTAIN`
- **Immutable state**: All artifacts stored as structured JSON/JSONL under `state/`
- **Prompt versioning**: Any prompt change requires version bump (gate1, gate3, gate4b_unlisted exist)
- **No auto-edits**: Pipeline never modifies Wikipedia; human review required

### Directory Structure
- **`scripts/`** — Pipeline stage executors; naming: `{llm_,det_}gate{N}_{name}.py`
- **`state/`** — All runtime state: JSONL records, indices, caches, manifests
- **`prompts/`** — LLM prompts (gate1.md, gate3.md, gate4b.md); versioned
- **`config/`** — feeds.md (RSS sources)
- **`ingest/`** — RSS ingestion logic (rss_ingest.py)
- **`output/`** — Final reports and candidate lists
- **`tests/`** — Unit tests for all major stages
- **`docs/`** — Architecture docs (overview, data-flow, running, troubleshooting)

### Critical Files
- **`run_pipeline.py`** — Orchestrator; chains all 11 stages; respects `--from-gate`, `--dry-run`
- **`name_utils.py`** — Shared utility; `normalize_name()` strips parens, applies NFKD normalization
- **`state/events.jsonl`** — Raw RSS events (starting point for pipeline)
- **`state/wiki_known_pages.json`** — Index of names with confirmed Wikipedia pages (used by Gate 0, Gate 2, Gate 3)
- **`state/gate1_llm_results.jsonl`** — Gate 1 decisions + structured fields (name_completeness, confidence, etc.)
- **`state/gate3_llm_results.jsonl`** — Gate 3 page-match results; decisions: `HAS_PAGE`/`MISSING`/`UNCERTAIN`
- **`state/gate4_reliable_coverage.jsonl`** — Gate 4 Brave results filtered to Wikipedia reliable domains
- **`state/gate4b_llm_results.jsonl`** — Gate 4b coverage verdicts plus deduped domain lists/counts
- **`output/openclaw/daily_notability_digest.json`** — Daily digest of likely/possibly notable people used by OpenClaw
- **`scripts/det_openclaw_daily_digest.py`** / **`scripts/daily_notability_digest_report.py`** — Build the digest, then print a concise summary
- **`state/runs/YYYYMMDDTHHMMSS.json`** — Run manifest (metadata about each pipeline execution)

### Gate-Specific Details

**Gate 1 JSON Schema:**
```json
{
  "person_detected": bool,
  "subject_name_as_written": string,
  "subject_name_full": string,
  "name_completeness": "FULL_NAME" | "SINGLE_TOKEN" | "UNKNOWN",
  "primary_focus": string,
  "gate1_decision": "STRONG_PASS" | "WEAK_PASS" | "FAIL",
  "reasoning_summary": [string, ...],
  "signal_type": string,
  "confidence": "high" | "medium" | "low"
}
```

**Gate 2 Logic:**
- `biography_score >= 3` → biography candidate (births/deaths/living people categories = +3 each)
- Levenshtein distance ≤ 2 for similar-name check
- BIO_SCORE_THRESHOLD = 3

**Gate 3 Decisions:**
- `HAS_PAGE` — High confidence that subject has a Wikipedia page; written to known pages index
- `MISSING` — Subject likely doesn't have a page; passed to Brave coverage search
- `UNCERTAIN` — Can't determine; fallback to coverage search

**Gate 4b Decisions:**
- `LIKELY_NOTABLE` — ≥2 distinct domains from the curated Wikipedia-reliable source list confirm the subject is the primary focus (first pass)
- `POSSIBLY_NOTABLE` — Didn't reach the curated-source threshold, but the LLM judged ≥2 sources in the broader (unfiltered) Brave results to be editorially reliable and about the subject (second pass)
- `UNCERTAIN` — The LLM could not parse or verify results
- `NOT_NOTABLE` — No reliable coverage found
- `SKIPPED`/`SKIPPED_HAS_PAGE` — Filtered early (below minimum result threshold, or already has Wikipedia page)
- `--min-reliable-results` default: 2 (minimum Brave results sent to the model per stage)

## Common Development Tasks

### Debugging a Gate
Check stage inputs/outputs:
```bash
# Inspect gate input
tail state/gate1_llm_results.jsonl | head -5

# Count records at each stage
wc -l state/events.jsonl state/prefilter_pass.jsonl state/gate1_llm_results.jsonl

# Check for LLM errors
grep "llm_error" state/gate1_llm_results.jsonl | head -5
```

### Modifying a Prompt
1. Edit the corresponding prompt file (e.g., `prompts/gate1.md`)
2. Consider version suffix if major changes (e.g., `gate4b_unlisted.md`)
3. Test against sample data: `python3 scripts/llm_gate1_runner.py --sample-size 5 [...]`
4. Update run manifest to note prompt version

### Adding a New Gate
1. Create stage script: `scripts/{det_,llm_}gateX_runner.py`
2. Add stage name to `STAGE_ORDER` in `run_pipeline.py`
3. Implement `build_arg_parser()` and `main()` following existing patterns
4. Write tests in `tests/test_gateX.py`
5. Update `docs/data-flow.md` with mermaid diagram
6. Document in `docs/running.md`

### Handling API Keys
- **OpenAI**: Set `OPENAI_API_KEY` env var
- **Brave**: Set `BRAVE_API_KEY` env var or store in `~/.brave`
- **Codex CLI**: Uses local `codex` CLI (respects `~/.codex/` config)
- **Claude CLI**: Uses local `claude` CLI (respects `~/.clauderc`)
- Pipeline checks for Brave API key only if brave stage will run (respects `--from-gate`)

### Troubleshooting
Refer to `docs/troubleshooting.md` for:
- Codex model errors (non-Codex accounts, unsupported models)
- Schema validation failures
- MediaWiki rate limits / stuck runs
- Gate 2 filter sensitivity (too aggressive/conservative)

## Testing Guidelines

- **Test location**: `tests/test_{stage_name}.py`
- **Test framework**: `unittest` (standard library)
- **Pattern**: Load test fixtures from `state/fixtures/` or create inline
- **Coverage**: Deterministic gates should have >95% coverage; LLM gates test schema/error handling

Example test:
```python
import unittest
from name_utils import normalize_name

class TestNameUtils(unittest.TestCase):
    def test_normalize_basic(self) -> None:
        self.assertEqual(normalize_name("Donald Trump"), "donald trump")
```

## Environment Variables & Configuration

| Variable | Purpose | Default |
|----------|---------|---------|
| `OPENAI_API_KEY` | OpenAI API authentication | (unset) |
| `BRAVE_API_KEY` | Brave Search API key | `~/.brave` |
| `CLAUDE_MODEL` | Claude model for gates (deprecated) | (use --model-* flags) |
| `PROJECT_ROOT` | Pipeline working directory | Directory of run_pipeline.py |

## Notes for Future Development

- **Codebase is Python 3.13+**: Use f-strings, type hints, `from __future__ import annotations`
- **No external frameworks**: Avoid adding framework dependencies; prefer stdlib or minimal deps
- **State is immutable JSONL**: Don't modify records in-place; create new stages/files instead
- **Dry-run safety**: Always support `--dry-run` flag for new stages
- **Error messages**: Include context (filename, record count, API limits) to aid debugging
