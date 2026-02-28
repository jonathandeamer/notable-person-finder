#!/usr/bin/env python3
"""Update known Wikipedia pages index from Gate 1 SKIP_GLOBALLY_KNOWN decisions.

Reads gate1_llm_results.jsonl and updates wiki_known_pages.json for any
record where gate1_decision == "SKIP_GLOBALLY_KNOWN".  This prevents
repeated Gate 1 LLM calls for globally famous people on future runs.

Example:
  python3 scripts/det_gate1_index_update.py \
    --input state/gate1_llm_results.jsonl \
    --known-pages state/wiki_known_pages.json
"""

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
        description="Update known Wikipedia pages index from Gate 1 SKIP_GLOBALLY_KNOWN decisions"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/gate1_llm_results.jsonl"),
        help="Gate 1 output JSONL (default: state/gate1_llm_results.jsonl)",
    )
    parser.add_argument(
        "--known-pages",
        type=Path,
        default=Path("state/wiki_known_pages.json"),
        help="Known pages index JSON (default: state/wiki_known_pages.json)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Start from empty index instead of merging",
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
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
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


def extract_subject_name(row: dict) -> str | None:
    """Extract subject name from a Gate 1 result record.

    Mirrors the pattern in det_mw_candidates.py: check top-level fields
    first, then fall back to parsed_output dict.
    """
    subject = row.get("subject_name_full") or row.get("subject_name_as_written")
    if not subject:
        parsed = row.get("parsed_output")
        if isinstance(parsed, dict):
            subject = parsed.get("subject_name_full") or parsed.get("subject_name_as_written")
    return subject or None


def run_update(
    input_path: Path,
    known_pages_path: Path,
    overwrite: bool,
) -> int:
    if not input_path.exists():
        print(f"error: input file not found: {input_path}", file=sys.stderr)
        return 1

    rows = read_jsonl(input_path)
    index = load_index(known_pages_path, overwrite)
    entries = index.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        index["entries"] = entries

    counts: Counter = Counter()
    now = utc_now_iso()

    for row in rows:
        gate1_decision = row.get("gate1_decision")
        if gate1_decision != "SKIP_GLOBALLY_KNOWN":
            counts[f"decision_{gate1_decision or 'missing'}"] += 1
            continue

        subject_name = extract_subject_name(row)
        if not subject_name:
            counts["missing_name"] += 1
            continue

        normalized = normalize_name(subject_name)
        if not normalized:
            counts["missing_name"] += 1
            continue

        existing = entries.get(normalized)
        added_at = existing.get("added_at_utc") if isinstance(existing, dict) else now
        entry = {
            "normalized_name": normalized,
            "pageid": None,
            "title": subject_name,
            "fullurl": None,
            "added_at_utc": added_at,
            "source_event_id": row.get("event_id"),
            "source_gate3_run_id": None,
            "source_stage": "gate1_skip_globally_known",
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
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    raise SystemExit(main())
