#!/usr/bin/env python3
"""
run_pipeline.py — End-to-end orchestrator for the Wikipedia notability finder pipeline.

Chains all 11 pipeline stages for daily/on-demand runs:
  rss_ingest → gate0 → gate1 → mw_candidates → gate2 → gate3 → gate3_index
  → brave → gate4_filter → gate4b → report

Usage examples:
  python3 run_pipeline.py --dry-run
  python3 run_pipeline.py --from-gate gate1 --gate1-budget 50
  python3 run_pipeline.py
"""

import argparse
import json
import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Project root: directory containing this script
PROJECT_ROOT = Path(__file__).parent

# ──────────────────────────────────────────────────────────────────────────────
# Stage ordering (used for --from-gate validation and short-circuit logic)
# ──────────────────────────────────────────────────────────────────────────────

STAGE_ORDER = [
    "rss_ingest",
    "gate0",
    "gate1",
    "mw_candidates",
    "gate2",
    "gate3",
    "gate3_index",
    "brave",
    "gate4_filter",
    "gate4b",
    "report",
]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def count_jsonl_rows(path: Path) -> int:
    """Count non-empty lines in a JSONL file (returns 0 if file missing)."""
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def load_jsonl(path: Path) -> list:
    """Load all valid JSON records from a JSONL file (skips malformed lines)."""
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def resolve_brave_api_key() -> Optional[str]:
    """Return Brave API key from BRAVE_API_KEY env var or ~/.brave file."""
    key = os.environ.get("BRAVE_API_KEY", "").strip()
    if key:
        return key
    brave_file = Path.home() / ".brave"
    if brave_file.exists():
        val = brave_file.read_text().strip()
        if val:
            return val
    return None


def fatal(msg: str) -> None:
    print(f"\nFATAL: {msg}", file=sys.stderr)
    sys.exit(1)


def warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr)


def warn_llm_errors(path: Path, stage_name: str, skip_rows: int = 0) -> None:
    """Log a warning if any *new* records (after skip_rows) have llm_error != null."""
    records = load_jsonl(path)[skip_rows:]
    error_records = [r for r in records if r.get("llm_error") is not None]
    if error_records:
        warn(f"{stage_name}: {len(error_records)} new record(s) have llm_error != null")
        distinct = sorted({(r.get("llm_error") or "")[:300] for r in error_records})
        for msg in distinct:
            warn(f"  sample: {msg}")


# ──────────────────────────────────────────────────────────────────────────────
# Stage result
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class StageResult:
    name: str
    cmd: list
    duration_s: float = 0.0
    exit_code: int = 0
    notes: str = ""
    skipped: bool = False

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "duration_s": round(self.duration_s, 2),
            "exit_code": self.exit_code,
            "notes": self.notes,
        }
        if self.skipped:
            d["skipped"] = True
        return d


def make_skipped(name: str, reason: str) -> StageResult:
    return StageResult(name=name, cmd=[], notes=f"skipped ({reason})", skipped=True)


# ──────────────────────────────────────────────────────────────────────────────
# Stage runner
# ──────────────────────────────────────────────────────────────────────────────

def run_stage_cmd(name: str, cmd: list, dry_run: bool) -> StageResult:
    """
    Run a subprocess stage, streaming output to the terminal.
    Returns a StageResult with timing and exit code.
    """
    cmd_str = " ".join(str(c) for c in cmd)

    if dry_run:
        print(f"  [DRY-RUN] {cmd_str}")
        return StageResult(name=name, cmd=cmd, notes="[dry-run]")

    print(f"\n{'─' * 64}")
    print(f"  STAGE : {name}")
    print(f"  CMD   : {cmd_str}")
    print(f"{'─' * 64}")
    sys.stdout.flush()

    t0 = datetime.now(timezone.utc)
    proc = subprocess.run(cmd, cwd=str(PROJECT_ROOT))
    duration_s = (datetime.now(timezone.utc) - t0).total_seconds()

    return StageResult(name=name, cmd=cmd, duration_s=duration_s, exit_code=proc.returncode)


# ──────────────────────────────────────────────────────────────────────────────
# Run manifest
# ──────────────────────────────────────────────────────────────────────────────

def write_manifest(
    state_dir: Path,
    run_ts: str,
    started_at: str,
    stage_results: list,
    new_gate1_events: int = 0,
    likely_notable_subjects: Optional[list] = None,
    possibly_notable_subjects: Optional[list] = None,
) -> Path:
    manifest = {
        "run_id": run_ts,
        "started_at": started_at,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "stages": [r.to_dict() for r in stage_results],
        "new_gate1_events": new_gate1_events,
        "likely_notable_subjects": likely_notable_subjects or [],
        "possibly_notable_subjects": possibly_notable_subjects or [],
    }
    runs_dir = state_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = runs_dir / f"{run_ts}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"\nWrote manifest: {manifest_path}")
    return manifest_path


# ──────────────────────────────────────────────────────────────────────────────
# Output report generation
# ──────────────────────────────────────────────────────────────────────────────

def generate_report(
    state_dir: Path, output_dir: Path, run_ts: str
) -> tuple:
    """
    Read gate4b_llm_results.jsonl and write:
      output/runs/{run_ts}_summary.json    — machine-readable
      output/runs/{run_ts}_candidates.txt  — human-readable

    Returns (likely_notable_names, possibly_notable_names).
    """
    records = load_jsonl(state_dir / "gate4b_llm_results.jsonl")

    def _dedup_by_name(recs: list) -> list:
        """One record per subject_name, keeping the highest confirmed_count."""
        best: dict[str, dict] = {}
        for rec in recs:
            name = rec.get("subject_name") or ""
            if name not in best or rec.get("confirmed_count", 0) > best[name].get("confirmed_count", 0):
                best[name] = rec
        return list(best.values())

    likely_notable = _dedup_by_name(
        [r for r in records if r.get("gate4b_status") == "LIKELY_NOTABLE"]
    )
    possibly_notable = _dedup_by_name(
        [r for r in records if r.get("gate4b_status") == "POSSIBLY_NOTABLE"]
    )

    runs_dir = output_dir / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    # ── Machine-readable summary JSON ─────────────────────────────────────────
    def _entry(rec: dict, use_second_pass: bool = False) -> dict:
        e = {
            "subject_name": rec.get("subject_name"),
            "gate4b_status": rec.get("gate4b_status"),
            "confirmed_count": rec.get("confirmed_count", 0),
            "source_article": (rec.get("source_context") or {}).get("entry_title"),
            "source_urls": [
                r["url"] for r in rec.get("results_sent", []) if r.get("url")
            ],
        }
        if use_second_pass:
            e["second_pass_confirmed_count"] = rec.get("second_pass_confirmed_count", 0)
        return e

    summary = {
        "run_id": run_ts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "likely_notable_count": len(likely_notable),
        "possibly_notable_count": len(possibly_notable),
        "likely_notable": [_entry(r) for r in likely_notable],
        "possibly_notable": [_entry(r, use_second_pass=True) for r in possibly_notable],
    }

    summary_path = runs_dir / f"{run_ts}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\nWrote summary : {summary_path}")

    # ── Human-readable candidates.txt ────────────────────────────────────────
    lines: list[str] = [
        f"Wikipedia Notability Finder — Run {run_ts}",
        f"Generated : {summary['generated_at']}",
        "",
        f"LIKELY NOTABLE ({len(likely_notable)} subject(s))",
        "=" * 60,
    ]

    def _append_subject(lines: list, rec: dict, use_second_pass: bool = False) -> None:
        if use_second_pass:
            confirmed = rec.get("second_pass_confirmed_count", 0)
            results_key = "second_pass_results_sent"
        else:
            confirmed = rec.get("confirmed_count", 0)
            results_key = "results_sent"
        lines.append(f"\n  • {rec.get('subject_name')}  (confirmed: {confirmed})")
        title = (rec.get("source_context") or {}).get("entry_title", "N/A")
        lines.append(f"    Source article : {title}")
        for r in rec.get(results_key, []):
            if r.get("url"):
                lines.append(f"    - {r['url']}")

    for rec in likely_notable:
        _append_subject(lines, rec)

    lines += [
        "",
        f"POSSIBLY NOTABLE ({len(possibly_notable)} subject(s))",
        "=" * 60,
    ]
    for rec in possibly_notable:
        _append_subject(lines, rec, use_second_pass=True)

    candidates_path = runs_dir / f"{run_ts}_candidates.txt"
    candidates_path.write_text("\n".join(lines) + "\n")
    print(f"Wrote candidates: {candidates_path}")

    return (
        [r.get("subject_name") for r in likely_notable],
        [r.get("subject_name") for r in possibly_notable],
    )


# ──────────────────────────────────────────────────────────────────────────────
# Parse-failure retry helper
# ──────────────────────────────────────────────────────────────────────────────

def _run_parse_failure_retry(
    stage_name: str,
    base_cmd: list,
    output_path: Path,
    dry_run: bool,
    pre_rows: int = 0,
) -> Optional[StageResult]:
    """
    If the records written by the current run contain parse failures, fire one
    retry pass with --retry-parse-failures appended to base_cmd.

    pre_rows: number of rows that existed before this run started (so we only
    examine NEW records when deciding whether to fire the retry).

    Retry failure is non-fatal: logged as a warning, pipeline continues.
    """
    all_records = load_jsonl(output_path)
    new_records = all_records[pre_rows:]
    failures = [r for r in new_records if not r.get("json_parse_ok") or r.get("llm_error")]
    if not failures:
        return None

    failed_ids = {r.get("event_id") for r in failures if isinstance(r.get("event_id"), str)}

    retry_name = f"{stage_name}_retry"
    retry_cmd = base_cmd + ["--retry-parse-failures"]
    print(f"\n  {len(failures)} parse failure(s) in {stage_name} — running retry pass...")

    result = run_stage_cmd(retry_name, retry_cmd, dry_run)

    if result.exit_code == 0:
        # Use last-wins per event_id to check which originally-failed IDs are now resolved.
        last_by_id: dict = {}
        for r in load_jsonl(output_path):
            eid = r.get("event_id")
            if isinstance(eid, str):
                last_by_id[eid] = r
        remaining = sum(
            1 for eid in failed_ids
            if not last_by_id.get(eid, {}).get("json_parse_ok")
            or last_by_id.get(eid, {}).get("llm_error")
        )
        resolved = len(failures) - remaining
        result.notes = f"resolved {resolved}/{len(failures)} failure(s); {remaining} still failing"
    else:
        warn(f"{retry_name} exited {result.exit_code} — continuing")
        result.notes = f"retry of {len(failures)} failure(s) — exit {result.exit_code}"

    return result


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="End-to-end orchestrator for the Wikipedia notability finder pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """\
            Stage names (for --from-gate):
              rss_ingest, gate0, gate1, mw_candidates, gate2, gate3,
              gate3_index, brave, gate4_filter, gate4b, report
            """
        ),
    )
    p.add_argument(
        "--state-dir",
        type=Path,
        default=Path("state"),
        metavar="PATH",
        help="State directory (default: state/)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        metavar="PATH",
        help="Output directory for reports (default: output/)",
    )
    p.add_argument(
        "--gate1-budget",
        type=int,
        default=50,
        metavar="N",
        help="Max new events for Gate 1 per run (default: 50)",
    )
    p.add_argument(
        "--from-gate",
        metavar="GATE_NAME",
        help="Skip all stages before GATE_NAME (see stage names below)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them",
    )
    p.add_argument(
        "--model-gate1",
        default="claude-haiku-4-5-20251001",
        metavar="MODEL",
        help="LLM model for Gate 1 (default: claude-haiku-4-5-20251001)",
    )
    p.add_argument(
        "--model-gate3",
        default="claude-haiku-4-5-20251001",
        metavar="MODEL",
        help="LLM model for Gate 3 (default: claude-haiku-4-5-20251001)",
    )
    p.add_argument(
        "--model-gate4b",
        default="claude-haiku-4-5-20251001",
        metavar="MODEL",
        help="LLM model for Gate 4b (default: claude-haiku-4-5-20251001)",
    )
    return p.parse_args(argv)


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main(argv=None) -> None:
    args = parse_args(argv)

    state_dir: Path = args.state_dir
    output_dir: Path = args.output_dir
    python = sys.executable

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    started_at = datetime.now(timezone.utc).isoformat()

    # ── Validate --from-gate ──────────────────────────────────────────────────
    if args.from_gate and args.from_gate not in STAGE_ORDER:
        fatal(
            f"Unknown gate name: {args.from_gate!r}\n"
            f"Valid names: {', '.join(STAGE_ORDER)}"
        )
    start_idx = STAGE_ORDER.index(args.from_gate) if args.from_gate else 0

    # ── Pre-flight: validate env (only if brave stage will run) ───────────────
    brave_stage_idx = STAGE_ORDER.index("brave")
    if not args.dry_run and start_idx <= brave_stage_idx:
        if not resolve_brave_api_key():
            fatal(
                "BRAVE_API_KEY is not set and ~/.brave does not exist.\n"
                "Stage 'brave' (step 8/11) requires a Brave Search API key.\n"
                "Set the environment variable or write your key to ~/.brave."
            )

    # ── Create required directories ───────────────────────────────────────────
    if not args.dry_run:
        for d in [
            state_dir / "runs",
            state_dir / "mw_cache",
            state_dir / "brave_cache",
            output_dir / "runs",
        ]:
            d.mkdir(parents=True, exist_ok=True)

    # ── Build ordered stage list ──────────────────────────────────────────────
    # Each entry: (stage_name, cmd_list)
    stages_def: list[tuple[str, list]] = [
        (
            "rss_ingest",
            [
                python, "ingest/rss_ingest.py",
                "--feeds", "config/feeds.md",
                "--state-dir", str(state_dir),
            ],
        ),
        (
            "gate0",
            [
                python, "scripts/det_gate0_prefilter.py",
                "--overwrite",
                "--known-pages", str(state_dir / "wiki_known_pages.json"),
                "--feeds", str(PROJECT_ROOT / "config" / "feeds.md"),
            ],
        ),
        (
            "gate1",
            [
                python, "scripts/llm_gate1_runner.py",
                "--events", str(state_dir / "prefilter_pass.jsonl"),
                "--prompt", "prompts/gate1.md",
                "--output", str(state_dir / "gate1_llm_results.jsonl"),
                "--backend", "claude-cli",
                "--model", args.model_gate1,
                "--sample-size", str(args.gate1_budget),
            ],
        ),
        (
            "mw_candidates",
            [
                python, "scripts/det_mw_candidates.py",
                "--input", str(state_dir / "gate1_llm_results.jsonl"),
                "--output", str(state_dir / "wiki_candidates.jsonl"),
                "--overwrite",
                "--cache-dir", str(state_dir / "mw_cache"),
            ],
        ),
        (
            "gate2",
            [
                python, "scripts/det_gate2_has_page.py",
                "--input", str(state_dir / "wiki_candidates.jsonl"),
                "--overwrite",
            ],
        ),
        (
            "gate3",
            [
                python, "scripts/llm_gate3_runner.py",
                "--input", str(state_dir / "wiki_candidates_pass.jsonl"),
                "--prompt", "prompts/gate3.md",
                "--output", str(state_dir / "gate3_llm_results.jsonl"),
                "--model", args.model_gate3,
            ],
        ),
        (
            "gate3_index",
            [
                python, "scripts/det_gate3_index_update.py",
                "--input", str(state_dir / "gate3_llm_results.jsonl"),
                "--known-pages", str(state_dir / "wiki_known_pages.json"),
            ],
        ),
        (
            "brave",
            [
                python, "scripts/det_brave_coverage.py",
                "--overwrite",
                "--throttle-ms", "1100",
            ],
        ),
        (
            "gate4_filter",
            [
                python, "scripts/det_gate4_reliable_filter.py",
                "--overwrite",
            ],
        ),
        (
            "gate4b",
            [
                python, "scripts/llm_gate4b_runner.py",
                "--prompt", "prompts/gate4b.md",
                "--model", args.model_gate4b,
                "--brave-input", str(state_dir / "brave_coverage.jsonl"),
                "--unlisted-prompt", "prompts/gate4b_unlisted.md",
            ],
        ),
    ]

    # ── Print run header ──────────────────────────────────────────────────────
    print(f"\n{'━' * 64}")
    print(f"  Wikipedia Notability Finder — Pipeline Run {run_ts}")
    if args.dry_run:
        print("  MODE    : DRY-RUN (no commands will be executed)")
    if args.from_gate:
        print(f"  From    : {args.from_gate}")
    print(f"  Budget  : {args.gate1_budget} Gate 1 events/run")
    print(f"  State   : {state_dir}")
    print(f"{'━' * 64}\n")

    # ── Execute stages ────────────────────────────────────────────────────────
    stage_results: list[StageResult] = []
    new_gate1_events = 0
    short_circuited = False

    for stage_name, cmd in stages_def:
        stage_idx = STAGE_ORDER.index(stage_name)

        # Skip stages before --from-gate
        if stage_idx < start_idx:
            stage_results.append(make_skipped(stage_name, "--from-gate"))
            continue

        # Short-circuit: skip all downstream stages if gate1 produced nothing new
        if short_circuited:
            stage_results.append(make_skipped(stage_name, "no new gate1 events"))
            continue

        # Snapshot row counts for LLM stages (to detect new rows and scope error warnings)
        _llm_files = {
            "gate1": state_dir / "gate1_llm_results.jsonl",
            "gate3": state_dir / "gate3_llm_results.jsonl",
            "gate4b": state_dir / "gate4b_llm_results.jsonl",
        }
        pre_rows = count_jsonl_rows(_llm_files[stage_name]) if stage_name in _llm_files else 0

        # Run the stage
        result = run_stage_cmd(stage_name, cmd, args.dry_run)
        stage_results.append(result)

        # Fail fast on non-zero exit
        if result.exit_code != 0:
            print(
                f"\nStage '{stage_name}' exited with code {result.exit_code}.",
                file=sys.stderr,
            )
            if not args.dry_run:
                write_manifest(
                    state_dir, run_ts, started_at, stage_results,
                    new_gate1_events=new_gate1_events,
                )
            sys.exit(result.exit_code)

        # Post-stage: compute notes and check short-circuit
        if stage_name == "rss_ingest":
            total = count_jsonl_rows(state_dir / "events.jsonl")
            result.notes = f"{total} total events"

        elif stage_name == "gate0":
            n_pass = count_jsonl_rows(state_dir / "prefilter_pass.jsonl")
            n_skip = count_jsonl_rows(state_dir / "prefilter_skip.jsonl")
            result.notes = f"{n_pass} pass / {n_skip} skip"

        elif stage_name == "gate1":
            post_rows = count_jsonl_rows(state_dir / "gate1_llm_results.jsonl")
            new_gate1_events = post_rows - pre_rows
            result.notes = f"{new_gate1_events} new events (total: {post_rows})"
            if new_gate1_events == 0 and not args.dry_run:
                print(
                    "\nNo new Gate 1 events — skipping downstream stages.",
                    file=sys.stderr,
                )
                short_circuited = True
            retry = _run_parse_failure_retry(
                "gate1", cmd, state_dir / "gate1_llm_results.jsonl", args.dry_run, pre_rows
            )
            if retry:
                stage_results.append(retry)

        elif stage_name == "mw_candidates":
            n = count_jsonl_rows(state_dir / "wiki_candidates.jsonl")
            result.notes = f"{n} candidates"

        elif stage_name == "gate2":
            n = count_jsonl_rows(state_dir / "wiki_candidates_pass.jsonl")
            result.notes = f"{n} candidates passed"

        elif stage_name == "gate3":
            post_rows = count_jsonl_rows(state_dir / "gate3_llm_results.jsonl")
            result.notes = f"{post_rows - pre_rows} new / {post_rows} total gate3 results"
            warn_llm_errors(state_dir / "gate3_llm_results.jsonl", "gate3", skip_rows=pre_rows)
            retry = _run_parse_failure_retry(
                "gate3", cmd, state_dir / "gate3_llm_results.jsonl", args.dry_run, pre_rows
            )
            if retry:
                stage_results.append(retry)

        elif stage_name == "gate3_index":
            result.notes = "wiki_known_pages.json updated"

        elif stage_name == "brave":
            n = count_jsonl_rows(state_dir / "brave_coverage.jsonl")
            result.notes = f"{n} subjects covered"

        elif stage_name == "gate4_filter":
            n = count_jsonl_rows(state_dir / "gate4_reliable_coverage.jsonl")
            result.notes = f"{n} subjects with reliable coverage"

        elif stage_name == "gate4b":
            post_rows = count_jsonl_rows(state_dir / "gate4b_llm_results.jsonl")
            result.notes = f"{post_rows - pre_rows} new / {post_rows} total gate4b results"
            warn_llm_errors(state_dir / "gate4b_llm_results.jsonl", "gate4b", skip_rows=pre_rows)
            retry = _run_parse_failure_retry(
                "gate4b", cmd, state_dir / "gate4b_llm_results.jsonl", args.dry_run, pre_rows
            )
            if retry:
                stage_results.append(retry)

    # ── Report stage (built-in Python, not a subprocess) ─────────────────────
    report_result = StageResult(name="report", cmd=[])
    t0 = datetime.now(timezone.utc)

    if STAGE_ORDER.index("report") < start_idx:
        report_result = make_skipped("report", "--from-gate")
    elif short_circuited:
        report_result = make_skipped("report", "no new gate1 events")
    elif args.dry_run:
        print(
            f"\n  [DRY-RUN] generate_report("
            f"state_dir={state_dir}, output_dir={output_dir}, run_ts={run_ts!r})"
        )
        report_result.notes = "[dry-run]"
    else:
        try:
            likely_notable_names, possibly_notable_names = generate_report(state_dir, output_dir, run_ts)
            report_result.notes = (
                f"{len(likely_notable_names)} likely notable, {len(possibly_notable_names)} possibly notable"
            )
        except Exception as exc:
            warn(f"Report generation failed: {exc}")
            report_result.exit_code = 1
            report_result.notes = f"error: {exc}"
            likely_notable_names, possibly_notable_names = [], []

    report_result.duration_s = (datetime.now(timezone.utc) - t0).total_seconds()
    stage_results.append(report_result)

    # ── Collect final subject lists for manifest ──────────────────────────────
    likely_notable_names: list[str] = []
    possibly_notable_names: list[str] = []
    if not args.dry_run and not short_circuited:
        records = load_jsonl(state_dir / "gate4b_llm_results.jsonl")
        likely_notable_names = [
            r.get("subject_name") for r in records if r.get("gate4b_status") == "LIKELY_NOTABLE"
        ]
        possibly_notable_names = [
            r.get("subject_name") for r in records if r.get("gate4b_status") == "POSSIBLY_NOTABLE"
        ]

    # ── Write run manifest ────────────────────────────────────────────────────
    if not args.dry_run:
        manifest_path = write_manifest(
            state_dir, run_ts, started_at, stage_results,
            new_gate1_events=new_gate1_events,
            likely_notable_subjects=likely_notable_names,
            possibly_notable_subjects=possibly_notable_names,
        )
    else:
        manifest_path = state_dir / "runs" / f"{run_ts}.json"

    # ── Final summary ─────────────────────────────────────────────────────────
    total_duration = sum(r.duration_s for r in stage_results)
    print(f"\n{'━' * 64}")
    print(f"  Run complete  : {run_ts}")
    print(f"  Duration      : {total_duration:.1f}s")
    print(f"  New Gate 1    : {new_gate1_events} events")
    if not short_circuited:
        print(f"  Likely Notable  : {len(likely_notable_names)}")
        if likely_notable_names:
            print(f"  Names           : {', '.join(likely_notable_names)}")
        print(f"  Possibly Notable: {len(possibly_notable_names)}")
    else:
        print("  (downstream stages skipped — no new events)")
    print(f"  Manifest      : {manifest_path}")
    print(f"{'━' * 64}\n")


if __name__ == "__main__":
    main()
