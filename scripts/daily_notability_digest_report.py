#!/usr/bin/env python3
"""Refresh the daily notability digest and print a concise summary."""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

WORKDIR = Path("/home/admin/notable-person-finder")
OUTPUT_FILE = WORKDIR / "output/openclaw/daily_notability_digest.json"
REFRESH_CMD = [
    "python3",
    "scripts/det_openclaw_daily_digest.py",
    "--window-hours",
    "24",
    "--output",
    str(OUTPUT_FILE),
]


def refresh_digest() -> None:
    subprocess.run(REFRESH_CMD, check=True, cwd=WORKDIR)


def load_digest() -> dict:
    with OUTPUT_FILE.open() as fh:
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
    try:
        refresh_digest()
    except subprocess.CalledProcessError as exc:
        print(f"Failed to refresh digest: {exc}", file=sys.stderr)
        return 1
    data = load_digest()
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
