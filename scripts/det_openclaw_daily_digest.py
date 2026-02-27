#!/usr/bin/env python3
"""Build a deterministic 24-hour digest for OpenClaw morning updates.

This script aggregates notable-person outcomes across one or more pipeline runs in
an observation window (default: last 24h), then emits a structured JSON report
containing:
- runs included in the window
- likely notable people
- possibly notable people
- per-person event provenance (original RSS URL + reliable Brave URLs)

Primary join keys:
- run_id (summary/manifests)
- event_id (gate4b, reliable coverage, events)
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from glob import glob
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parent.parent


def parse_iso8601(value: str | None) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict):
                    out.append(obj)
    except OSError:
        return out
    return out


def dedup_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def pick_run_for_trial(
    trial_at: datetime | None,
    run_windows: dict[str, tuple[datetime | None, datetime | None]],
) -> str | None:
    if trial_at is None:
        return None

    # Prefer strict containment inside a run window.
    candidates: list[tuple[datetime, str]] = []
    for run_id, (started, finished) in run_windows.items():
        if started is None or finished is None:
            continue
        if started <= trial_at <= finished:
            candidates.append((finished, run_id))

    if candidates:
        candidates.sort()
        return candidates[-1][1]

    # Fallback: nearest run by finished_at only if very close (clock drift edge case).
    nearest: tuple[float, str] | None = None
    for run_id, (_, finished) in run_windows.items():
        if finished is None:
            continue
        delta = abs((trial_at - finished).total_seconds())
        if nearest is None or delta < nearest[0]:
            nearest = (delta, run_id)
    if nearest and nearest[0] <= 15 * 60:
        return nearest[1]
    return None


def collect_people(
    records: list[dict[str, Any]],
    target_status: str,
    run_ids_in_window: set[str],
    run_windows: dict[str, tuple[datetime | None, datetime | None]],
    events_by_id: dict[str, dict[str, Any]],
    reliable_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}

    for rec in records:
        if rec.get("gate4b_status") != target_status:
            continue

        event_id = rec.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            continue

        trial_at = parse_iso8601(rec.get("trial_at_utc"))
        run_id = pick_run_for_trial(trial_at, run_windows)
        if run_id is None or run_id not in run_ids_in_window:
            continue

        subject_name = (rec.get("subject_name") or "").strip()
        if not subject_name:
            continue
        subject_key = subject_name.casefold()

        event_row = events_by_id.get(event_id, {})
        rss_url = event_row.get("entry_url_canonical")
        if not isinstance(rss_url, str):
            rss_url = None

        coverage_row = reliable_by_id.get(event_id, {})
        brave_results = coverage_row.get("brave_results")
        reliable_urls: list[str] = []
        if isinstance(brave_results, list):
            for item in brave_results:
                if not isinstance(item, dict):
                    continue
                url = item.get("url")
                if isinstance(url, str) and url:
                    reliable_urls.append(url)

        if not reliable_urls:
            results_sent = rec.get("results_sent")
            if isinstance(results_sent, list):
                for item in results_sent:
                    if not isinstance(item, dict):
                        continue
                    url = item.get("url")
                    if isinstance(url, str) and url:
                        reliable_urls.append(url)

        if rss_url:
            reliable_urls = [u for u in reliable_urls if u != rss_url]
        reliable_urls = dedup_preserve_order(reliable_urls)

        source_context = rec.get("source_context") if isinstance(rec.get("source_context"), dict) else {}
        event_obj = {
            "event_id": event_id,
            "run_id": run_id,
            "trial_at_utc": iso_utc(trial_at) if trial_at else None,
            "rss_entry_url": rss_url,
            "rss_published_at_utc": event_row.get("published_at_utc"),
            "source_article_title": source_context.get("entry_title"),
            "reliable_brave_urls": reliable_urls,
        }

        if subject_key not in grouped:
            grouped[subject_key] = {
                "subject_name": subject_name,
                "gate4b_status": target_status,
                "run_ids": [],
                "events": [],
                "all_reliable_brave_urls": [],
            }

        group = grouped[subject_key]
        if run_id not in group["run_ids"]:
            group["run_ids"].append(run_id)

        # Keep latest event row when gate4b has multiple entries for same event.
        existing_idx = None
        for idx, existing in enumerate(group["events"]):
            if isinstance(existing, dict) and existing.get("event_id") == event_id:
                existing_idx = idx
                break
        if existing_idx is None:
            group["events"].append(event_obj)
        else:
            old_trial = group["events"][existing_idx].get("trial_at_utc") or ""
            new_trial = event_obj.get("trial_at_utc") or ""
            if new_trial >= old_trial:
                group["events"][existing_idx] = event_obj

        group["all_reliable_brave_urls"] = dedup_preserve_order(
            group["all_reliable_brave_urls"] + reliable_urls
        )

    people = list(grouped.values())
    for person in people:
        person["run_ids"].sort()
        person["events"].sort(key=lambda e: (e.get("trial_at_utc") or "", e.get("event_id") or ""))
        person["event_count"] = len(person["events"])
        person["reliable_brave_url_count"] = len(person["all_reliable_brave_urls"])

    people.sort(key=lambda p: p.get("subject_name", "").casefold())
    return people


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a structured OpenClaw digest from the last N hours of run output."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="Project root used to resolve relative paths/globs (default: repo root)",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Observation window in hours (default: 24)",
    )
    parser.add_argument(
        "--now-utc",
        type=str,
        default=None,
        help="Override current UTC time (ISO-8601) for deterministic backfills/tests",
    )
    parser.add_argument(
        "--summary-glob",
        type=str,
        default="output/runs/*_summary.json",
        help="Glob for run summary JSON files",
    )
    parser.add_argument(
        "--manifest-glob",
        type=str,
        default="state/runs/*.json",
        help="Glob for run manifest JSON files",
    )
    parser.add_argument(
        "--gate4b",
        type=Path,
        default=Path("state/gate4b_llm_results.jsonl"),
        help="Path to gate4b_llm_results.jsonl",
    )
    parser.add_argument(
        "--reliable-coverage",
        type=Path,
        default=Path("state/gate4_reliable_coverage.jsonl"),
        help="Path to gate4_reliable_coverage.jsonl",
    )
    parser.add_argument(
        "--events",
        type=Path,
        default=Path("state/events.jsonl"),
        help="Path to events.jsonl",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/openclaw/daily_notability_digest.json"),
        help="Output JSON path",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Also print the JSON payload to stdout",
    )
    return parser


def resolve_path(path: Path, project_root: Path) -> Path:
    return path if path.is_absolute() else (project_root / path)


def resolve_glob(pattern: str, project_root: Path) -> str:
    if Path(pattern).is_absolute():
        return pattern
    return str(project_root / pattern)


def main() -> int:
    args = build_arg_parser().parse_args()
    project_root = args.project_root.resolve()

    now_dt = parse_iso8601(args.now_utc) if args.now_utc else datetime.now(timezone.utc)
    if now_dt is None:
        print("error: invalid --now-utc value")
        return 1

    window_start = now_dt - timedelta(hours=args.window_hours)
    summary_glob = resolve_glob(args.summary_glob, project_root)
    manifest_glob = resolve_glob(args.manifest_glob, project_root)
    gate4b_path = resolve_path(args.gate4b, project_root)
    reliable_coverage_path = resolve_path(args.reliable_coverage, project_root)
    events_path = resolve_path(args.events, project_root)
    output_path = resolve_path(args.output, project_root)

    # Load summaries in window.
    run_summaries: dict[str, dict[str, Any]] = {}
    for raw_path in sorted(glob(summary_glob)):
        path = Path(raw_path)
        data = load_json(path)
        if not data:
            continue

        run_id = data.get("run_id")
        generated_at = parse_iso8601(data.get("generated_at"))
        if not isinstance(run_id, str) or generated_at is None:
            continue
        if not (window_start <= generated_at <= now_dt):
            continue

        run_summaries[run_id] = {
            "run_id": run_id,
            "generated_at": iso_utc(generated_at),
            "likely_notable_count": data.get("likely_notable_count", 0),
            "possibly_notable_count": data.get("possibly_notable_count", 0),
            "summary_path": str(path.resolve()),
        }

    # Load manifests.
    run_windows: dict[str, tuple[datetime | None, datetime | None]] = {}
    run_details: dict[str, dict[str, Any]] = {}
    for raw_path in sorted(glob(manifest_glob)):
        path = Path(raw_path)
        data = load_json(path)
        if not data:
            continue
        run_id = data.get("run_id")
        if not isinstance(run_id, str):
            continue

        started = parse_iso8601(data.get("started_at"))
        finished = parse_iso8601(data.get("finished_at"))
        run_windows[run_id] = (started, finished)
        run_details[run_id] = {
            "started_at": iso_utc(started) if started else None,
            "finished_at": iso_utc(finished) if finished else None,
            "manifest_path": str(path.resolve()),
        }

    run_ids_in_window = set(run_summaries.keys())

    # Build event and coverage indexes by event_id.
    events_by_id: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(events_path):
        event_id = row.get("event_id")
        if isinstance(event_id, str) and event_id:
            events_by_id[event_id] = row

    reliable_by_id: dict[str, dict[str, Any]] = {}
    for row in load_jsonl(reliable_coverage_path):
        event_id = row.get("event_id")
        if isinstance(event_id, str) and event_id:
            reliable_by_id[event_id] = row

    gate4b_rows = load_jsonl(gate4b_path)

    likely_people = collect_people(
        records=gate4b_rows,
        target_status="LIKELY_NOTABLE",
        run_ids_in_window=run_ids_in_window,
        run_windows=run_windows,
        events_by_id=events_by_id,
        reliable_by_id=reliable_by_id,
    )
    possibly_people = collect_people(
        records=gate4b_rows,
        target_status="POSSIBLY_NOTABLE",
        run_ids_in_window=run_ids_in_window,
        run_windows=run_windows,
        events_by_id=events_by_id,
        reliable_by_id=reliable_by_id,
    )

    runs_output: list[dict[str, Any]] = []
    for run_id in sorted(run_ids_in_window):
        row = dict(run_summaries[run_id])
        row.update(run_details.get(run_id, {}))
        runs_output.append(row)

    payload = {
        "generated_at_utc": iso_utc(now_dt),
        "window": {
            "hours": args.window_hours,
            "start_utc": iso_utc(window_start),
            "end_utc": iso_utc(now_dt),
        },
        "data_locations": {
            "project_root": str(project_root),
            "summary_glob": summary_glob,
            "manifest_glob": manifest_glob,
            "gate4b": str(gate4b_path),
            "reliable_coverage": str(reliable_coverage_path),
            "events": str(events_path),
        },
        "runs_in_window": runs_output,
        "likely_notable_people": likely_people,
        "possibly_notable_people": possibly_people,
        "counts": {
            "runs_in_window": len(runs_output),
            "likely_people": len(likely_people),
            "possibly_people": len(possibly_people),
            "likely_events": sum(p.get("event_count", 0) for p in likely_people),
            "possibly_events": sum(p.get("event_count", 0) for p in possibly_people),
        },
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote OpenClaw digest: {output_path}")
    if args.stdout:
        print(json.dumps(payload, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
