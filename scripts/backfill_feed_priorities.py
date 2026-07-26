#!/usr/bin/env python3
"""Backfill feed_priority field to existing events in state/events.jsonl."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from scripts.det_gate0_prefilter import parse_feed_priorities


def backfill_feed_priorities(
    events_path: Path,
    feeds_path: Path,
    output_path: Path | None = None,
    overwrite: bool = False,
) -> int:
    """Add feed_priority field to events based on feeds.md."""
    if not events_path.exists():
        print(f"Error: {events_path} does not exist", file=sys.stderr)
        return 1
    
    if output_path is None:
        output_path = events_path
    
    if output_path.exists() and not overwrite:
        print(f"Error: {output_path} already exists; use --overwrite to replace", file=sys.stderr)
        return 1
    
    # Parse feed priorities
    feed_priorities = parse_feed_priorities(feeds_path)
    print(f"Loaded {len(feed_priorities)} prioritized feed(s) from {feeds_path}")
    
    # Read events and add feed_priority
    events = []
    updated_count = 0
    skipped_count = 0
    
    with events_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                print(f"Warning: invalid JSON at line {line_no}; skipping", file=sys.stderr)
                continue
            
            if not isinstance(event, dict):
                print(f"Warning: non-object JSON at line {line_no}; skipping", file=sys.stderr)
                continue
            
            # Check if already has feed_priority
            if "feed_priority" in event:
                skipped_count += 1
            else:
                # Assign feed_priority
                feed_url = event.get("source_feed_url_original") or ""
                event["feed_priority"] = feed_priorities.get(feed_url)
                updated_count += 1
            
            events.append(event)
    
    # Write updated events
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    
    print(f"\nBackfill summary:")
    print(f"  Input file       : {events_path}")
    print(f"  Output file      : {output_path}")
    print(f"  Total events     : {len(events)}")
    print(f"  Updated          : {updated_count}")
    print(f"  Already had field: {skipped_count}")
    print(f"  Feeds with priority: {len(feed_priorities)}")
    
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill feed_priority field to events in state/events.jsonl"
    )
    parser.add_argument(
        "--events",
        type=Path,
        default=Path("state/events.jsonl"),
        help="Input events JSONL file to backfill",
    )
    parser.add_argument(
        "--feeds",
        type=Path,
        default=Path("config/feeds.md"),
        help="Path to feeds.md for priority definitions",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file (defaults to overwriting input)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    return backfill_feed_priorities(
        events_path=args.events,
        feeds_path=args.feeds,
        output_path=args.output,
        overwrite=args.overwrite,
    )


if __name__ == "__main__":
    raise SystemExit(main())
