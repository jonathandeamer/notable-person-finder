#!/usr/bin/env python3
"""Update known Wikipedia pages index from Gate 3 LLM output.

Reads gate3_llm_results.jsonl and updates wiki_known_pages.json for any
record where gate3_status == "HAS_PAGE".

Example:
  python3 scripts/det_gate3_index_update.py \
    --input state/gate3_llm_results.jsonl \
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
sys.path.append(str(Path(__file__).resolve().parent))

from name_utils import normalize_name
from det_gate0_prefilter import extract_candidate_name


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Update known Wikipedia pages index from Gate 3 output"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Gate 3 output JSONL (gate3_llm_results.jsonl)",
    )
    parser.add_argument(
        "--known-pages",
        type=Path,
        default=Path("state/wiki_known_pages.json"),
        help="Known pages index JSON",
    )
    parser.add_argument(
        "--gate3-run-id",
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


def extract_matched_title(row: dict) -> str | None:
    """Extract matched_title from parsed_output or top-level fields."""
    parsed = row.get("parsed_output") if isinstance(row.get("parsed_output"), dict) else {}
    title = parsed.get("matched_title")
    if isinstance(title, str):
        return title
    # Fallback to top-level
    title = row.get("matched_title")
    if isinstance(title, str):
        return title
    return None


def run_update(
    input_path: Path,
    known_pages_path: Path,
    gate3_run_id: str | None,
    overwrite: bool,
) -> int:
    rows = read_jsonl(input_path)
    index = load_index(known_pages_path, overwrite)
    entries = index.get("entries")
    if not isinstance(entries, dict):
        entries = {}
        index["entries"] = entries

    counts: Counter = Counter()
    now = utc_now_iso()

    for row in rows:
        gate3_status = row.get("gate3_status")
        if gate3_status is None:
            counts["missing_status"] += 1
            continue
        if gate3_status != "HAS_PAGE":
            counts[f"status_{gate3_status}"] += 1
            continue

        subject_name = row.get("subject_name")
        matched_title = extract_matched_title(row)

        name_for_key = subject_name or matched_title
        normalized = normalize_name(name_for_key)
        if not normalized:
            counts["missing_name"] += 1
            continue

        existing = entries.get(normalized)
        added_at = existing.get("added_at_utc") if isinstance(existing, dict) else now
        entry = {
            "normalized_name": normalized,
            "pageid": None,
            "title": matched_title,
            "fullurl": None,
            "added_at_utc": added_at,
            "source_event_id": row.get("event_id"),
            "source_gate3_run_id": gate3_run_id,
        }
        entries[normalized] = entry
        counts["updated"] += 1

        # Write a Gate-0-style alias so future articles whose title/summary yields a
        # different regex name (e.g. "Nick" vs LLM full name "Nicholas Smith") will
        # still hit the index at Gate 0, avoiding redundant Gate 1+3 LLM calls.
        src_ctx = row.get("source_context") or {}
        event_text = f"{src_ctx.get('entry_title') or ''} {src_ctx.get('summary') or ''}".strip()
        alias_name = extract_candidate_name(event_text)
        if alias_name:
            alias_key = normalize_name(alias_name)
            if alias_key and alias_key != normalized:
                existing_alias = entries.get(alias_key)
                alias_added_at = existing_alias.get("added_at_utc") if isinstance(existing_alias, dict) else now
                entries[alias_key] = {**entry, "normalized_name": alias_key, "added_at_utc": alias_added_at}
                counts["alias_written"] += 1

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
        gate3_run_id=args.gate3_run_id,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    raise SystemExit(main())
