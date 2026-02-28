# Wikipedia Notability Finder

A deterministic pipeline that identifies people who may merit a Wikipedia biography. It scans RSS news feeds, checks whether each person already has a Wikipedia article, and evaluates the breadth of their press coverage using only Wikipedia-approved sources.

**It does not create or edit Wikipedia articles.** All final decisions require human review.

---

## How It Works

The pipeline runs in four phases. Each phase is designed to reduce noise—so that by the end, only genuinely newsworthy people without existing Wikipedia coverage remain.

### Phase A — Discover People

The pipeline begins by reading RSS news feeds (configured in `config/feeds.md`) and extracting named individuals from each article. It immediately discards obvious noise: entries without a recognizable full name, mentions of people already confirmed to have Wikipedia pages, and events that clearly aren't about a person's notability (e.g., sports scores, product announcements).

What passes through is a short list of people who appear in news and might plausibly lack a Wikipedia article. An AI model reviews each one and assigns a confidence level—high, medium, or low—before passing candidates forward.

### Phase B — Deduplicate Against Wikipedia

Before investing in coverage research, the pipeline checks Wikipedia directly. It searches the MediaWiki API for articles matching each candidate's name and uses heuristics (category tags, biography scores) to detect existing articles.

For ambiguous matches—where the name is common or the article title differs slightly—an AI model reads the Wikipedia page and decides whether it describes the same person from the news story. Anyone already covered by Wikipedia is removed from the candidate list.

### Phase C — Collect Coverage

For each person who doesn't yet have a Wikipedia article, the pipeline queries the Brave News Search API to find press coverage. Only results from domains on Wikipedia's list of reliable sources are kept—tabloids, content farms, and low-quality outlets are filtered out automatically.

This produces a set of credible, verifiable sources for each remaining candidate.

### Phase D — Surface Candidates for Human Review

An AI model reviews the coverage and judges whether each result is genuinely about the subject—ruling out passing mentions and people with the same name. The strength of the verdict depends on where the coverage comes from.

**Likely notable** means at least two sources from a curated list of known reliable outlets (major wire services, national broadcasters, established newspapers) independently cover the person. The list is fixed and the threshold is objective.

**Possibly notable** is the fallback for people who don't clear that bar. Here the AI itself judges whether the remaining sources look editorially credible—a softer signal that warrants attention but not yet a confident recommendation.

The results are written to a daily digest (`output/openclaw/daily_notability_digest.json`) and a human-readable report. No edits to Wikipedia happen automatically.

---

## Output

Each pipeline run produces:

- **`output/openclaw/daily_notability_digest.json`** — Structured digest consumed by the OpenClaw agent platform; contains all candidates with their notability verdict and source count
- **`output/runs/*_summary.json`** — Per-run summary with stage-by-stage record counts

A typical candidate entry looks like:

```json
{
  "subject_name": "Jane Doe",
  "gate4b_decision": "LIKELY_NOTABLE",
  "distinct_reliable_domains": ["apnews.com", "reuters.com"],
  "signal_type": "award",
  "source_headline": "Jane Doe wins regional science prize"
}
```

Possible verdicts: `LIKELY_NOTABLE` (≥2 curated reliable sources), `POSSIBLY_NOTABLE` (covered, but reliability judged by AI rather than a fixed list), `NOT_NOTABLE`, `HAS_PAGE` (already on Wikipedia), `SKIPPED`.

---

## Setup

**Requirements:** Python 3.13, a Brave Search API key, and either an OpenAI API key or a locally installed `codex` CLI.

```bash
# Clone and activate the virtual environment
git clone <repo-url>
cd notable-person-finder
python3 -m venv .venv
source .venv/bin/activate
pip install requests   # only non-stdlib dependency

# Set API keys
export BRAVE_API_KEY="your-brave-key"
export OPENAI_API_KEY="your-openai-key"   # or configure codex CLI in ~/.codex/
```

Edit `config/feeds.md` to add or remove RSS feeds before running.

---

## Usage

### Run the full pipeline

```bash
python3 run_pipeline.py
```

### Useful flags

| Flag | Purpose |
|------|---------|
| `--dry-run` | Preview all stages without executing them |
| `--from-gate STAGE` | Resume from a specific stage (e.g., `gate1`, `brave`, `gate4b`) |
| `--gate1-budget N` | Limit AI triage to N records per run (default: 50) |
| `--model-gate1 MODEL` | Override the AI model used at triage (default: `gpt-5.2`) |
| `--backend-gate1 BACKEND` | Switch LLM backend: `openai` or `codex-cli` |

Example — preview a run starting from the coverage search stage:

```bash
python3 run_pipeline.py --dry-run --from-gate brave
```

See [`docs/running.md`](docs/running.md) for per-stage commands if you need to run individual steps manually.

---

## Development

### Run tests

```bash
python3 -m unittest discover tests/
```

### Project documentation

| Document | Contents |
|----------|---------|
| [`docs/overview.md`](docs/overview.md) | Pipeline architecture and stage list |
| [`docs/running.md`](docs/running.md) | Per-stage CLI commands |
| [`docs/troubleshooting.md`](docs/troubleshooting.md) | Common errors and fixes |
| [`CLAUDE.md`](CLAUDE.md) | Developer guide for AI-assisted contributions |

### Design principles

- **Deterministic control flow**: Python code drives all state transitions; AI is used only at named semantic gates
- **Strict schemas**: All AI outputs are validated against JSON schemas and fail-safe to a conservative default
- **Immutable state**: Every stage reads from and writes to separate JSONL files under `state/`; no in-place modification
- **No auto-edits**: The pipeline never touches Wikipedia; it only surfaces candidates for a human editor to evaluate
