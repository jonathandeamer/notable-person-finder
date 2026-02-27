#!/usr/bin/env python3
"""Update known Wikipedia pages index from Gate 2 output."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from name_utils import normalize_name


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update known Wikipedia pages index from Gate 2 output"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Gate 2 output JSONL",
    )
    parser.add_argument(
        "--known-pages",
        type=Path,
        default=Path(
            "/Users/jonathan/new-wikipedia-article-checker/state/wiki_known_pages.json"
        ),
        help="Known pages index JSON",
    )
    parser.add_argument(
        "--gate2-run-id",
        type=str,
        default=None,
        help="Optional run id to store on updated entries",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing index instead of merging",
    )
    return parser


def load_index(path: Path, overwrite: bool) -> dict:
    if overwrite or not path.exists():
        return {"version": 1, "updated_at_utc": utc_now_iso(), "entries": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": 1, "updated_at_utc": utc_now_iso(), "entries": {}}
    if not isinstance(payload, dict):
        return {"version": 1, "updated_at_utc": utc_now_iso(), "entries": {}}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        payload["entries"] = {}
    payload.setdefault("version", 1)
    payload.setdefault("updated_at_utc", utc_now_iso())
    return payload


def write_index(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                print(f"warning: invalid JSON at line {line_no}; skipping", file=sys.stderr)
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def extract_gate2_status(row: dict) -> str | None:
    parsed = row.get("parsed_output") if isinstance(row.get("parsed_output"), dict) else {}
    for key in ("gate2_status", "gate2_decision", "status"):
        val = row.get(key)
        if isinstance(val, str):
            return val
    for key in ("gate2_status", "gate2_decision", "status"):
        val = parsed.get(key)
        if isinstance(val, str):
            return val
    return None


def extract_match_info(row: dict) -> dict:
    parsed = row.get("parsed_output") if isinstance(row.get("parsed_output"), dict) else {}
    for key in ("matched_page", "match", "matched"):
        val = row.get(key)
        if isinstance(val, dict):
            return val
    for key in ("matched_page", "match", "matched"):
        val = parsed.get(key)
        if isinstance(val, dict):
            return val
    return {}


def extract_title(row: dict, match_info: dict) -> str | None:
    for key in ("title", "matched_title", "page_title"):
        val = row.get(key)
        if isinstance(val, str):
            return val
    for key in ("title", "matched_title", "page_title"):
        val = match_info.get(key)
        if isinstance(val, str):
            return val
    return None


def extract_pageid(match_info: dict) -> int | None:
    pageid = match_info.get("pageid")
    if isinstance(pageid, int):
        return pageid
    if isinstance(pageid, str) and pageid.isdigit():
        return int(pageid)
    return None


def extract_fullurl(match_info: dict) -> str | None:
    fullurl = match_info.get("fullurl")
    if isinstance(fullurl, str):
        return fullurl
    return None


def extract_subject_name(row: dict) -> str | None:
    for key in ("subject_name_full", "subject_name_as_written", "subject_name"):
        val = row.get(key)
        if isinstance(val, str):
            return val
    parsed = row.get("parsed_output") if isinstance(row.get("parsed_output"), dict) else {}
    for key in ("subject_name_full", "subject_name_as_written", "subject_name"):
        val = parsed.get(key)
        if isinstance(val, str):
            return val
    return None


def run_update(input_path: Path, known_pages_path: Path, gate2_run_id: str | None, overwrite: bool) -> int:
    rows = read_jsonl(input_path)
    index = load_index(known_pages_path, overwrite)
    entries = index.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        index["entries"] = entries

    counts = Counter()
    now = utc_now_iso()

    for row in rows:
        status = extract_gate2_status(row)
        if status is None:
            counts["missing_status"] += 1
            continue
        if status not in {"HAS_PAGE", "MATCH"}:
            counts[f"status_{status}"] += 1
            continue

        subject = extract_subject_name(row)
        match_info = extract_match_info(row)
        title = extract_title(row, match_info)
        pageid = extract_pageid(match_info)
        fullurl = extract_fullurl(match_info)

        name_for_key = subject or title
        normalized = normalize_name(name_for_key)
        if not normalized:
            counts["missing_name"] += 1
            continue

        existing = entries.get(normalized)
        added_at = existing.get("added_at_utc") if isinstance(existing, dict) else now
        entry = {
            "normalized_name": normalized,
            "pageid": pageid,
            "title": title,
            "fullurl": fullurl,
            "added_at_utc": added_at,
            "source_event_id": row.get("event_id"),
            "source_gate2_run_id": gate2_run_id,
        }
        entries[normalized] = entry
        counts["updated"] += 1

    index["updated_at_utc"] = now
    write_index(known_pages_path, index)

    print(f"records_read: {len(rows)}")
    print("update_counts:")
    for key in sorted(counts):
        print(f"- {key}: {counts[key]}")
    print(f"output: {known_pages_path}")
    return 0


def main() -> int:
    args = build_arg_parser().parse_args()
    return run_update(
        input_path=args.input,
        known_pages_path=args.known_pages,
        gate2_run_id=args.gate2_run_id,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    raise SystemExit(main())
