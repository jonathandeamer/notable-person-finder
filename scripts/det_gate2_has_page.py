#!/usr/bin/env python3
"""Deterministic Gate 2: annotate candidates with match metadata; all records proceed to Gate 3."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from name_utils import normalize_name, sort_by_priority_recency


BIO_SCORE_THRESHOLD = 3
SIMILARITY_DISTANCE = 2


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic Gate 2: has-page filter")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/wiki_candidates.jsonl"),
        help="Input JSONL (MediaWiki candidates)",
    )
    parser.add_argument(
        "--pass-output",
        type=Path,
        default=Path("state/wiki_candidates_pass.jsonl"),
        help="Output JSONL for records that should proceed to LLM Gate 3",
    )
    parser.add_argument(
        "--skip-output",
        type=Path,
        default=Path("state/wiki_candidates_skip.jsonl"),
        help="Output JSONL (always empty; kept for backwards-compatible CLI compatibility)",
    )
    parser.add_argument(
        "--known-pages",
        type=Path,
        default=Path("state/wiki_known_pages.json"),
        help="No-op; kept for backwards-compatible CLI compatibility",
    )
    parser.add_argument(
        "--gate2-run-id",
        type=str,
        default=None,
        help="No-op; kept for backwards-compatible CLI compatibility",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output files if they already exist",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file for decisions",
    )
    return parser


def _log(msg: str, log_file: Path | None) -> None:
    ts = utc_now_iso()
    line = f"{ts} {msg}"
    print(line)
    if log_file is None:
        return
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


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


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")



def _check_output_paths(pass_output: Path, skip_output: Path, overwrite: bool) -> None:
    if overwrite:
        return
    collisions = [p for p in (pass_output, skip_output) if p.exists()]
    if collisions:
        joined = ", ".join(str(p) for p in collisions)
        raise FileExistsError(
            f"output path(s) already exist: {joined}; re-run with --overwrite"
        )


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            ins = cur[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            cur.append(min(ins, delete, sub))
        prev = cur
    return prev[-1]


def last_name(name: str) -> str:
    parts = [p for p in name.split(" ") if p]
    return parts[-1] if parts else ""


def is_biography_candidate(candidate: dict) -> bool:
    return bool(candidate.get("biography_prioritized"))


def normalized_candidate_title(candidate: dict) -> str:
    return normalize_name(candidate.get("title"))

def normalized_redirect_title(candidate: dict) -> str:
    return normalize_name(candidate.get("redirected_from"))


def match_type_for_candidate(candidate: dict, subject_norm: str) -> str | None:
    if normalized_candidate_title(candidate) == subject_norm:
        return "title"
    if normalized_redirect_title(candidate) == subject_norm:
        return "redirect"
    return None


def has_similar_biography(
    subject_norm: str, candidates: list[dict], skip_candidate: dict
) -> bool:
    subject_last = last_name(subject_norm)
    for cand in candidates:
        if cand is skip_candidate:
            continue
        if not is_biography_candidate(cand):
            continue
        title_norm = normalized_candidate_title(cand)
        if not title_norm:
            continue
        if subject_last and last_name(title_norm) != subject_last:
            continue
        if levenshtein(subject_norm, title_norm) <= SIMILARITY_DISTANCE:
            return True
    return False


def pick_best_match(candidates: list[dict]) -> dict | None:
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda c: (
            c.get("biography_score", 0),
            -(c.get("pageid") or 0),
        ),
        reverse=True,
    )[0]


def run_gate2(
    input_path: Path,
    pass_output: Path,
    skip_output: Path,
    known_pages_path: Path,
    gate2_run_id: str | None,
    overwrite: bool,
    log_file: Path | None,
) -> int:
    _check_output_paths(pass_output, skip_output, overwrite)
    rows = read_jsonl(input_path)

    # Sort by feed priority tier, then by recency (newest first)
    rows = sort_by_priority_recency(rows)

    pass_rows: list[dict] = []
    skip_rows: list[dict] = []
    counts = Counter()

    for row in rows:
        subject = row.get("subject_name")
        subject_norm = normalize_name(subject)
        results = row.get("mw_search", {}).get("results", [])

        row_out = dict(row)

        if not subject_norm or not results:
            row_out["det_gate2_signal"] = "NO_RESULTS"
            row_out["det_gate2_best_match_title"] = None
            row_out["det_gate2_best_match_pageid"] = None
            row_out["det_gate2_best_match_fullurl"] = None
            row_out["det_gate2_best_match_type"] = None
            row_out["det_gate2_best_match_score"] = None
            row_out["det_gate2_has_similar_bio"] = False
            row_out["det_gate2_bio_candidates_count"] = 0
            counts["pass_no_results"] += 1
            pass_rows.append(row_out)
            continue

        bio_candidates = [c for c in results if is_biography_candidate(c)]
        if not bio_candidates:
            row_out["det_gate2_signal"] = "NO_BIO_CANDIDATES"
            row_out["det_gate2_best_match_title"] = None
            row_out["det_gate2_best_match_pageid"] = None
            row_out["det_gate2_best_match_fullurl"] = None
            row_out["det_gate2_best_match_type"] = None
            row_out["det_gate2_best_match_score"] = None
            row_out["det_gate2_has_similar_bio"] = False
            row_out["det_gate2_bio_candidates_count"] = 0
            counts["pass_no_bio_candidates"] += 1
            pass_rows.append(row_out)
            continue

        exact_matches = []
        match_type_by_id: dict[int | None, str] = {}
        for c in bio_candidates:
            mt = match_type_for_candidate(c, subject_norm)
            if mt:
                exact_matches.append(c)
                match_type_by_id[c.get("pageid")] = mt

        if not exact_matches:
            row_out["det_gate2_signal"] = "NO_EXACT_MATCH"
            row_out["det_gate2_best_match_title"] = None
            row_out["det_gate2_best_match_pageid"] = None
            row_out["det_gate2_best_match_fullurl"] = None
            row_out["det_gate2_best_match_type"] = None
            row_out["det_gate2_best_match_score"] = None
            row_out["det_gate2_has_similar_bio"] = False
            row_out["det_gate2_bio_candidates_count"] = len(bio_candidates)
            counts["pass_no_exact_match"] += 1
            pass_rows.append(row_out)
            continue

        match = pick_best_match(exact_matches)
        match_type = match_type_by_id.get(match.get("pageid"), "title") if match else None
        has_similar = has_similar_biography(subject_norm, bio_candidates, match) if match else False

        if has_similar:
            signal = "EXACT_MATCH_AMBIGUOUS"
            counts["pass_exact_match_ambiguous"] += 1
        else:
            signal = "EXACT_MATCH"
            counts["pass_exact_match"] += 1
            _log(
                f"exact_match subject={subject} title={match.get('title') if match else None} pageid={match.get('pageid') if match else None}",
                log_file,
            )

        row_out["det_gate2_signal"] = signal
        row_out["det_gate2_best_match_title"] = match.get("title") if match else None
        row_out["det_gate2_best_match_pageid"] = match.get("pageid") if match else None
        row_out["det_gate2_best_match_fullurl"] = match.get("fullurl") if match else None
        row_out["det_gate2_best_match_type"] = match_type
        row_out["det_gate2_best_match_score"] = match.get("biography_score") if match else None
        row_out["det_gate2_has_similar_bio"] = has_similar
        row_out["det_gate2_bio_candidates_count"] = len(bio_candidates)
        pass_rows.append(row_out)

    write_jsonl(pass_output, pass_rows)
    write_jsonl(skip_output, skip_rows)

    print(f"records_read: {len(rows)}")
    print(f"pass_records: {len(pass_rows)}")
    print(f"skip_records: {len(skip_rows)}")
    print("decision_counts:")
    for key in sorted(counts):
        print(f"- {key}: {counts[key]}")
    print(f"pass_output: {pass_output}")
    print(f"skip_output: {skip_output}")
    return 0


def main() -> int:
    args = build_arg_parser().parse_args()
    return run_gate2(
        input_path=args.input,
        pass_output=args.pass_output,
        skip_output=args.skip_output,
        known_pages_path=args.known_pages,
        gate2_run_id=args.gate2_run_id,
        overwrite=args.overwrite,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
