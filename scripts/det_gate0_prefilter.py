#!/usr/bin/env python3
"""Deterministic Gate-0 prefilter for events before LLM Gate 1."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from name_utils import normalize_name

FULL_NAME_PATTERN = re.compile(r"\b[A-Z][a-z]{1,29}\s+[A-Z][a-z]{1,39}\b")
INITIAL_SURNAME_PATTERN = re.compile(
    r"\b(?:[A-Z]\.\s*[A-Z][a-z]{1,39}|[A-Z](?:\s+[A-Z])?\s+[A-Z][a-z]{1,39})\b"
)
OBIT_CUE_PATTERN = re.compile(
    r"\b(obituary|obit|died|dies|dead|aged\s+\d{2,3}|tributes?|remembered)\b",
    re.IGNORECASE,
)
OBIT_GUARDRAIL_TOKEN_PATTERN = re.compile(r"\b[A-Z][a-z]{2,39}\b")
_LETTERS_RE = re.compile(r"^Letters?:\s", re.ASCII)
_PERSONAL_OBIT_RE = re.compile(
    r"(?:^|<p>)\s*(?:My|Our)\s+(?:late\s+)?"
    r"(?:husband|wife|father|mother|son|daughter|sister|brother|"
    r"twin\s+(?:brother|sister)|partner|friend|colleague|former\s+colleague|"
    r"uncle|aunt|grandfather|grandmother|nephew|niece|cousin)\b",
    re.IGNORECASE,
)

PREFILTER_PASS = "PREFILTER_PASS_TO_LLM"
PREFILTER_SKIP = "PREFILTER_SKIP_NO_NAME"
PREFILTER_SKIP_KNOWN = "PREFILTER_SKIP_HAS_WIKI_PAGE"
PREFILTER_SKIP_LETTERS = "LETTERS_HEADER_SKIP"
PREFILTER_SKIP_READER_OBIT = "PERSONAL_TRIBUTE_OBIT_SKIP"


def parse_feed_priorities(path: Path) -> dict[str, int]:
    """Return {url: priority} for feeds that have a priority number in feeds.md."""
    priorities: dict[str, int] = {}
    if not path.exists():
        return priorities
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or not line.startswith("- "):
            continue
        rest = line[2:].strip()
        parts = rest.rsplit(None, 1)
        if len(parts) == 2 and parts[1].isdigit():
            priorities[parts[0]] = int(parts[1])
    return priorities


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run deterministic Gate-0 prefilter")
    parser.add_argument(
        "--events",
        type=Path,
        default=Path("/Users/jonathan/new-wikipedia-article-checker/state/events.jsonl"),
        help="Input events JSONL",
    )
    parser.add_argument(
        "--pass-output",
        type=Path,
        default=Path("/Users/jonathan/new-wikipedia-article-checker/state/prefilter_pass.jsonl"),
        help="Output JSONL for prefilter pass events",
    )
    parser.add_argument(
        "--skip-output",
        type=Path,
        default=Path("/Users/jonathan/new-wikipedia-article-checker/state/prefilter_skip.jsonl"),
        help="Output JSONL for prefilter skip events",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they already exist",
    )
    parser.add_argument(
        "--known-pages",
        type=Path,
        default=Path(
            "/Users/jonathan/new-wikipedia-article-checker/state/wiki_known_pages.json"
        ),
        help="Known Wikipedia page index used to skip already-covered names",
    )
    parser.add_argument(
        "--feeds",
        type=Path,
        default=None,
        help="Path to feeds.md for feed priority assignment (optional)",
    )
    return parser


def event_text(event: dict) -> str:
    title = event.get("entry_title") or ""
    summary = event.get("summary") or ""
    return f"{title} {summary}".strip()


def extract_candidate_name(text: str) -> str | None:
    match = FULL_NAME_PATTERN.search(text)
    if match:
        return match.group(0)
    match = INITIAL_SURNAME_PATTERN.search(text)
    if match:
        return match.group(0)
    return None

def classify_event(event: dict) -> dict:
    entry_title = event.get("entry_title") or ""
    summary = event.get("summary") or ""
    if _LETTERS_RE.match(entry_title):
        return {
            "prefilter_decision": PREFILTER_SKIP_LETTERS,
            "prefilter_reason_codes": ["LETTERS_HEADER_SKIP"],
            "prefilter_signals": {
                "full_name_match": False,
                "initial_surname_match": False,
                "obit_cue_match": False,
                "obit_guardrail_token_match": False,
                "known_wiki_page_match": False,
            },
        }

    if _PERSONAL_OBIT_RE.search(summary):
        return {
            "prefilter_decision": PREFILTER_SKIP_READER_OBIT,
            "prefilter_reason_codes": ["PERSONAL_TRIBUTE_OBIT_SKIP"],
            "prefilter_signals": {
                "full_name_match": False,
                "initial_surname_match": False,
                "obit_cue_match": False,
                "obit_guardrail_token_match": False,
                "known_wiki_page_match": False,
            },
        }

    text = event_text(event)

    full_name_match = bool(FULL_NAME_PATTERN.search(text))
    initial_surname_match = bool(INITIAL_SURNAME_PATTERN.search(text))
    obit_cue_match = bool(OBIT_CUE_PATTERN.search(text))
    obit_guardrail_token_match = bool(OBIT_GUARDRAIL_TOKEN_PATTERN.search(text))

    reason_codes: list[str] = []
    if full_name_match:
        decision = PREFILTER_PASS
        reason_codes.append("NAME_FULL_MATCH")
    elif initial_surname_match:
        decision = PREFILTER_PASS
        reason_codes.append("NAME_INITIAL_SURNAME_MATCH")
    elif obit_cue_match and obit_guardrail_token_match:
        decision = PREFILTER_PASS
        reason_codes.append("OBIT_CUE_WITH_CAPITALIZED_TOKEN")
    elif obit_cue_match and not obit_guardrail_token_match:
        decision = PREFILTER_SKIP
        reason_codes.append("NO_NAME_SIGNAL_OBIT_CUE_WITHOUT_GUARDRAIL")
    else:
        decision = PREFILTER_SKIP
        reason_codes.append("NO_NAME_SIGNAL_DEFAULT_SKIP")

    return {
        "prefilter_decision": decision,
        "prefilter_reason_codes": reason_codes,
        "prefilter_signals": {
            "full_name_match": full_name_match,
            "initial_surname_match": initial_surname_match,
            "obit_cue_match": obit_cue_match,
            "obit_guardrail_token_match": obit_guardrail_token_match,
            "known_wiki_page_match": False,
        },
    }


def load_events(path: Path) -> tuple[list[dict], int]:
    events: list[dict] = []
    invalid_lines = 0
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except json.JSONDecodeError:
                invalid_lines += 1
                print(f"warning: invalid JSON at line {line_no}; skipping", file=sys.stderr)
                continue
            if not isinstance(event, dict):
                invalid_lines += 1
                print(
                    f"warning: non-object JSON at line {line_no}; skipping",
                    file=sys.stderr,
                )
                continue
            events.append(event)
    return events, invalid_lines


def _check_output_paths(pass_output: Path, skip_output: Path, overwrite: bool) -> None:
    if overwrite:
        return
    collisions = [p for p in (pass_output, skip_output) if p.exists()]
    if collisions:
        joined = ", ".join(str(p) for p in collisions)
        raise FileExistsError(
            f"output path(s) already exist: {joined}; re-run with --overwrite"
        )


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def load_known_pages(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        print(f"warning: invalid known-pages JSON at {path}; ignoring", file=sys.stderr)
        return {}
    entries = payload.get("entries")
    if not isinstance(entries, dict):
        return {}
    return entries

def run_prefilter(
    events_path: Path,
    pass_output: Path,
    skip_output: Path,
    overwrite: bool = False,
    known_pages_path: Path | None = None,
    feeds_path: Path | None = None,
) -> int:
    _check_output_paths(pass_output, skip_output, overwrite)
    events, invalid_lines = load_events(events_path)
    known_entries = load_known_pages(known_pages_path) if known_pages_path else {}

    # Resolve feeds path
    if feeds_path is None:
        feeds_path = Path("config/feeds.md")
    feed_priorities = parse_feed_priorities(feeds_path)

    pass_rows: list[dict] = []
    skip_rows: list[dict] = []
    reason_counts: Counter[str] = Counter()

    for event in events:
        prefilter = classify_event(event)
        candidate_name = extract_candidate_name(event_text(event))
        normalized = normalize_name(candidate_name) if candidate_name else ""
        if normalized and normalized in known_entries:
            prefilter["prefilter_decision"] = PREFILTER_SKIP_KNOWN
            prefilter["prefilter_reason_codes"].append("KNOWN_WIKI_PAGE")
            prefilter["prefilter_signals"]["known_wiki_page_match"] = True
        out_row = dict(event)
        out_row.update(prefilter)
        feed_url = event.get("source_feed_url_original") or ""
        out_row["feed_priority"] = feed_priorities.get(feed_url)

        for code in prefilter["prefilter_reason_codes"]:
            reason_counts[code] += 1

        if prefilter["prefilter_decision"] == PREFILTER_PASS:
            pass_rows.append(out_row)
        else:
            skip_rows.append(out_row)

    _write_jsonl(pass_output, pass_rows)
    _write_jsonl(skip_output, skip_rows)

    total = len(events)
    pass_count = len(pass_rows)
    skip_count = len(skip_rows)

    print("Gate-0 prefilter summary")
    print(f"input_events: {total}")
    print(f"invalid_lines_skipped: {invalid_lines}")
    print(f"prefilter_pass_count: {pass_count}")
    print(f"prefilter_skip_count: {skip_count}")
    if total > 0:
        print(f"prefilter_pass_rate: {pass_count / total:.1%}")
        print(f"prefilter_skip_rate: {skip_count / total:.1%}")
    print("reason_code_counts:")
    for code in sorted(reason_counts):
        print(f"- {code}: {reason_counts[code]}")
    print(f"pass_output: {pass_output}")
    print(f"skip_output: {skip_output}")
    return 0


def main() -> int:
    args = build_arg_parser().parse_args()
    return run_prefilter(
        events_path=args.events,
        pass_output=args.pass_output,
        skip_output=args.skip_output,
        overwrite=args.overwrite,
        known_pages_path=args.known_pages,
        feeds_path=args.feeds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
