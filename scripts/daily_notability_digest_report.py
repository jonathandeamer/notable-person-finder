#!/usr/bin/env python3
"""Refresh the daily notability digest and print a concise summary."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Iterable

SCRIPT_PATH = Path(__file__).resolve()
DEFAULT_PROJECT_ROOT = SCRIPT_PATH.parent.parent


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh daily_notability_digest.json and print a concise summary."
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=DEFAULT_PROJECT_ROOT,
        help="Project root used to resolve relative paths (default: repo root)",
    )
    parser.add_argument(
        "--digest-output",
        type=Path,
        default=Path("output/openclaw/daily_notability_digest.json"),
        help="Path to digest JSON (relative to --project-root by default)",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=24,
        help="Window passed to det_openclaw_daily_digest.py (default: 24)",
    )
    parser.add_argument(
        "--now-utc",
        type=str,
        default=None,
        help="Optional --now-utc override passed through to digest generator",
    )
    parser.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Print summary from existing digest file without regenerating it",
    )
    return parser


def resolve_path(path: Path, project_root: Path) -> Path:
    return path if path.is_absolute() else (project_root / path)


def refresh_digest(
    project_root: Path,
    digest_output: Path,
    window_hours: int,
    now_utc: str | None,
) -> None:
    cmd = [
        sys.executable,
        "scripts/det_openclaw_daily_digest.py",
        "--window-hours",
        str(window_hours),
        "--output",
        str(digest_output),
    ]
    if now_utc:
        cmd.extend(["--now-utc", now_utc])
    subprocess.run(cmd, check=True, cwd=project_root)


def load_digest(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def normalize_urls(value: Iterable[str] | str | None) -> set[str]:
    if value is None:
        return set()
    if isinstance(value, str):
        return {value}
    return set(value)


def gather_person_urls(events: list[dict], summary: dict) -> tuple[list[str], list[str]]:
    rss_urls: set[str] = set()
    brave_urls: set[str] = set()
    for event in events:
        rss_urls.update(normalize_urls(event.get("rss_entry_url")))
        brave_urls.update(normalize_urls(event.get("reliable_brave_urls")))
    brave_urls.update(normalize_urls(summary.get("all_reliable_brave_urls")))
    return sorted(rss_urls), sorted(brave_urls)


def select_latest_run(runs: list[dict]) -> dict | None:
    if not runs:
        return None
    def sort_key(run: dict) -> str:
        return run.get("generated_at") or run.get("finished_at") or ""
    return max(runs, key=sort_key)


def format_section(title: str, people: list[dict]) -> list[str]:
    lines = [title]
    if not people:
        lines.append("- None")
        return lines
    for person in people:
        name = person.get("subject_name", "(unknown)")
        rss_urls, brave_urls = gather_person_urls(person.get("events", []), person)
        lines.append(f"- {name}")
        lines.append(
            f"  • RSS entries: {', '.join(rss_urls) if rss_urls else 'none reported'}"
        )
        if brave_urls:
            lines.append(f"  • Brave URLs: {', '.join(brave_urls)}")
        else:
            lines.append("  • Brave URLs: none reported")
    return lines


def main() -> int:
    args = build_arg_parser().parse_args()
    project_root = args.project_root.resolve()
    digest_output = resolve_path(args.digest_output, project_root)

    if not args.skip_refresh:
        try:
            refresh_digest(
                project_root=project_root,
                digest_output=digest_output,
                window_hours=args.window_hours,
                now_utc=args.now_utc,
            )
        except subprocess.CalledProcessError as exc:
            print(f"Failed to refresh digest: {exc}", file=sys.stderr)
            return 1

    if not digest_output.exists():
        print(f"Digest file not found: {digest_output}", file=sys.stderr)
        return 1

    data = load_digest(digest_output)
    counts = data.get("counts", {})
    runs = data.get("runs_in_window", [])
    latest_run = select_latest_run(runs)
    latest_run_id = latest_run.get("run_id") if latest_run else "(none)"

    print("Notable Person Finder Summary (Last 24h)")
    print(
        f"Pipeline health: {counts.get('runs_in_window', 0)} runs · latest run {latest_run_id}"
    )
    print()
    for line in format_section("Likely notable", data.get("likely_notable_people", [])):
        print(line)
    print()
    for line in format_section("Possibly notable", data.get("possibly_notable_people", [])):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
