#!/usr/bin/env python3
"""Gate 3 LLM page-match runner.

Reads state/wiki_candidates_pass.jsonl and decides, for each candidate set,
whether any Wikipedia page is the same person as the subject.

Example:
  python3 scripts/llm_gate3_runner.py \
    --input state/wiki_candidates_pass.jsonl \
    --prompt prompts/gate3.md \
    --output state/gate3_llm_results.jsonl \
    --model claude-sonnet-4-6
"""

from __future__ import annotations

import argparse
import json
import random
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from name_utils import sort_by_priority_recency


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_json_parse(text: str) -> tuple[bool, object | None, str | None]:
    raw = (text or "").strip()
    if not raw:
        return False, None, "empty_output"

    try:
        return True, json.loads(raw), None
    except json.JSONDecodeError:
        pass

    start = raw.find("{")
    end = raw.rfind("}")
    if start != -1 and end != -1 and end > start:
        candidate = raw[start : end + 1]
        try:
            return True, json.loads(candidate), None
        except json.JSONDecodeError as exc:
            return False, None, f"json_decode_error: {exc}"

    return False, None, "no_json_object_found"


def gate3_output_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["status", "matched_title", "confidence", "evidence"],
        "properties": {
            "status": {"type": "string", "enum": ["HAS_PAGE", "MISSING", "UNCERTAIN"]},
            "matched_title": {"type": ["string", "null"]},
            "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            "evidence": {
                "type": "array",
                "items": {"type": "string"},
                "minItems": 1,
                "maxItems": 3,
            },
        },
    }


def select_candidates(results: list[dict]) -> list[dict]:
    """Select candidates to send to the LLM.

    If there are 2 or more biography-positive candidates (biography_score > 0),
    return all biography-positive candidates.
    Otherwise return all bio-positive plus the top non-bio candidate as fallback.
    """
    bio_positive = [c for c in results if (c.get("biography_score") or 0) > 0]
    if len(bio_positive) >= 2:
        return bio_positive
    # Keep one fallback non-bio candidate so the model can still detect an
    # obvious page match when scoring underestimates biography relevance.
    non_bio = [c for c in results if (c.get("biography_score") or 0) <= 0]
    return bio_positive + non_bio[:1]


def extract_prose_snippet(raw_extract: str | None, min_chars: int = 700) -> str:
    """Extract a meaningful prose snippet from a raw Wikipedia extract.

    1. Strip residual template syntax: {{...}}
    2. Normalise whitespace
    3. Split on double newlines to find paragraphs
    4. Return max(first_paragraph, text[:700]) by character length
    """
    if not raw_extract:
        return ""

    text = re.sub(r"\{\{[^}]*\}\}", "", raw_extract)
    text = re.sub(r"[ \t]+", " ", text).strip()

    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    first_para = paragraphs[0] if paragraphs else text

    if len(first_para) >= min_chars:
        return first_para
    return text[:min_chars]


def format_gate3_prompt(
    prompt_body: str,
    subject: str,
    source_context: dict,
    candidates: list[dict],
) -> str:
    """Append subject + source context + numbered candidates to the prompt body."""
    lines = [prompt_body.rstrip()]
    lines.append("")
    lines.append(f"Subject: {subject}")
    lines.append("")
    lines.append("Source article:")
    lines.append(f"  Title: {source_context.get('entry_title') or '(none)'}")
    lines.append(f"  Summary: {source_context.get('summary') or '(none)'}")
    lines.append(f"  Source: {source_context.get('source') or '(none)'}")
    lines.append(f"  Date: {source_context.get('publication_date') or '(none)'}")
    lines.append("")
    lines.append("Wikipedia candidates:")

    for i, cand in enumerate(candidates, start=1):
        title = cand.get("title") or "(untitled)"
        description = cand.get("description") or "(none)"
        raw_extract = cand.get("extract") or ""
        snippet = extract_prose_snippet(raw_extract)
        lines.append(f"{i}. {title}")
        lines.append(f"   Description: {description}")
        lines.append(f"   Extract: {snippet}")

    return "\n".join(lines)


def call_claude_cli(prompt_text: str, model: str, cwd: Path) -> tuple[str, dict]:
    """Call the claude CLI with prompt via stdin."""
    import os
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    cmd = ["claude", "--model", model, "-p", "-"]
    proc = subprocess.run(
        cmd,
        input=prompt_text,
        capture_output=True,
        text=True,
        check=False,
        cwd=str(cwd),
        env=env,
    )
    meta = {
        "backend": "claude-cli",
        "returncode": proc.returncode,
        "stderr_excerpt": (proc.stderr or "")[:1000],
    }
    if proc.returncode != 0:
        raise RuntimeError(
            f"claude cli failed (code {proc.returncode}): {proc.stderr.strip()}"
        )
    return proc.stdout.strip(), meta


def call_claude_cli_with_retries(
    prompt_text: str,
    model: str,
    cwd: Path,
    max_attempts: int,
) -> tuple[str, dict]:
    """Call claude CLI with exponential backoff retries."""
    attempt = 0
    delay = 0.5
    last_error: Exception | None = None
    while attempt < max_attempts:
        attempt += 1
        try:
            return call_claude_cli(prompt_text=prompt_text, model=model, cwd=cwd)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(delay + random.random() * 0.2)
            delay = min(delay * 2, 5.0)
    raise RuntimeError(f"claude cli failed after {max_attempts} attempts: {last_error}")


def call_codex_cli(
    prompt_text: str,
    model: str,
    codex_cwd: Path,
    output_schema: dict,
) -> tuple[str, dict]:
    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        schema_path = tmp_dir / "gate3_schema.json"
        out_path = tmp_dir / "last_message.txt"
        schema_path.write_text(json.dumps(output_schema), encoding="utf-8")

        cmd = [
            "codex",
            "exec",
            "--skip-git-repo-check",
            "--cd",
            str(codex_cwd),
            "--model",
            model,
            "--output-schema",
            str(schema_path),
            "--output-last-message",
            str(out_path),
            "-",
        ]

        proc = subprocess.run(
            cmd,
            input=prompt_text,
            text=True,
            capture_output=True,
            check=False,
        )

        response_meta = {
            "backend": "codex-cli",
            "returncode": proc.returncode,
            "stderr_excerpt": (proc.stderr or "")[:1000],
            "stdout_excerpt": (proc.stdout or "")[:1000],
        }

        if proc.returncode != 0:
            raise RuntimeError(
                f"codex exec failed (code {proc.returncode}): {(proc.stderr or proc.stdout).strip()}"
            )

        if not out_path.exists():
            return "", response_meta

        return out_path.read_text(encoding="utf-8"), response_meta


def call_codex_cli_with_retries(
    prompt_text: str,
    model: str,
    codex_cwd: Path,
    output_schema: dict,
    max_attempts: int,
) -> tuple[str, dict]:
    attempt = 0
    delay = 0.5
    last_error: Exception | None = None
    while attempt < max_attempts:
        attempt += 1
        try:
            return call_codex_cli(
                prompt_text=prompt_text,
                model=model,
                codex_cwd=codex_cwd,
                output_schema=output_schema,
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt >= max_attempts:
                break
            time.sleep(delay + random.random() * 0.2)
            delay = min(delay * 2, 5.0)
    raise RuntimeError(f"codex exec failed after {max_attempts} attempts: {last_error}")


def load_input(input_path: Path) -> list[dict]:
    rows: list[dict] = []
    with input_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                print(
                    f"warning: invalid JSON at line {line_no}; skipping",
                    file=sys.stderr,
                )
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def build_source_context(record: dict) -> dict:
    """Extract source context fields from an input record."""
    ctx = record.get("source_context")
    if isinstance(ctx, dict):
        return ctx
    # Fall back to top-level fields (Gate 1 style)
    return {
        "entry_title": record.get("entry_title"),
        "summary": record.get("summary"),
        "source": record.get("source_feed_title") or record.get("source"),
        "publication_date": record.get("published_at_utc") or record.get("publication_date"),
    }


def _read_existing_output(path: Path) -> list[dict]:
    """Read all valid JSON records from an existing output file."""
    records: list[dict] = []
    if not path.exists():
        return records
    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gate 3 LLM page-match runner")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/wiki_candidates_pass.jsonl"),
        help="Path to wiki_candidates_pass.jsonl (default: state/wiki_candidates_pass.jsonl)",
    )
    parser.add_argument(
        "--prompt",
        type=Path,
        required=True,
        help="Path to prompts/gate3.md",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path to gate3_llm_results.jsonl",
    )
    parser.add_argument(
        "--model",
        default="gpt-5.2",
        help="Model name (default: gpt-5.2)",
    )
    parser.add_argument(
        "--backend",
        choices=["claude-cli", "codex-cli"],
        default="codex-cli",
        help="LLM backend to use (default: codex-cli)",
    )
    parser.add_argument(
        "--cwd",
        type=Path,
        default=Path.cwd(),
        help="Working directory for claude cli (default: current directory)",
    )
    parser.add_argument(
        "--codex-cwd",
        type=Path,
        default=Path.cwd(),
        help="Working directory for codex exec (default: current directory)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="Number of records to process (default: all)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for sampling (default: 42)",
    )
    parser.add_argument(
        "--max-output-chars",
        type=int,
        default=5000,
        help="Truncate stored raw output to this size (default: 5000)",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Delay between requests (default: 0.0)",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=3,
        help="Max retry attempts for LLM CLI failures (default: 3)",
    )
    parser.add_argument(
        "--fresh-output",
        action="store_true",
        help="Overwrite output file instead of appending",
    )
    parser.add_argument(
        "--retry-parse-failures",
        action="store_true",
        help="Re-run only records whose existing output has json_parse_ok=False or llm_error set",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    random.seed(args.seed)

    prompt_body = args.prompt.read_text(encoding="utf-8")
    records = load_input(args.input)

    if not records:
        print("No records found in input.", file=sys.stderr)
        return 1

    if args.retry_parse_failures:
        # Use last-wins per event_id so already-resolved records aren't retried again.
        last_by_id: dict[str, dict] = {}
        for r in _read_existing_output(args.output):
            eid = r.get("event_id")
            if isinstance(eid, str):
                last_by_id[eid] = r
        failed_ids = {
            eid for eid, r in last_by_id.items()
            if not r.get("json_parse_ok") or r.get("llm_error")
        }
        if not failed_ids:
            print("No parse failures to retry.")
            return 0
        records = [r for r in records if r.get("event_id") in failed_ids]
        sample_size = len(records)
        sampled = records
    elif args.sample_size is not None:
        # Sort by priority + recency, then take top N
        records = sort_by_priority_recency(records)
        sample_size = min(args.sample_size, len(records))
        sampled = records[:sample_size]
    else:
        # Process all records, sorted by priority + recency
        records = sort_by_priority_recency(records)
        sample_size = len(records)
        sampled = records

    args.output.parent.mkdir(parents=True, exist_ok=True)
    output_mode = "w" if args.fresh_output else "a"

    valid_json_count = 0
    with args.output.open(output_mode, encoding="utf-8") as out_f:
        for idx, record in enumerate(sampled, start=1):
            event_id = record.get("event_id")
            subject_name = record.get("subject_name") or ""
            source_context = build_source_context(record)

            mw_search = record.get("mw_search") or {}
            all_results = mw_search.get("results") or []
            candidates = select_candidates(all_results)

            # Short-circuit: no MW candidates → deterministically MISSING
            if not candidates:
                result_record = {
                    "trial_at_utc": utc_now_iso(),
                    "model": args.model,
                    "backend": "deterministic",
                    "event_id": event_id,
                    "subject_name": subject_name,
                    "source_context": source_context,
                    "candidates_sent": [],
                    "duration_ms": 0,
                    "llm_error": None,
                    "json_parse_ok": True,
                    "json_parse_error": None,
                    "gate3_status": "MISSING",
                    "parsed_output": {"status": "MISSING", "matched_title": None, "confidence": 1.0, "evidence": ["No MediaWiki search results returned for subject."]},
                    "raw_output": "",
                }
                out_f.write(json.dumps(result_record, ensure_ascii=False, sort_keys=True) + "\n")
                out_f.flush()
                valid_json_count += 1
                print(f"[{idx}/{sample_size}] MISSING (no candidates) - {subject_name}")
                continue

            started = time.time()
            error: str | None = None
            raw_output = ""
            parsed_output = None
            parse_ok = False
            parse_error: str | None = None
            call_meta: dict = {}

            try:
                trial_prompt = format_gate3_prompt(
                    prompt_body=prompt_body,
                    subject=subject_name,
                    source_context=source_context,
                    candidates=candidates,
                )
                if args.backend == "codex-cli":
                    raw_output, call_meta = call_codex_cli_with_retries(
                        prompt_text=trial_prompt,
                        model=args.model,
                        codex_cwd=args.codex_cwd,
                        output_schema=gate3_output_schema(),
                        max_attempts=args.max_attempts,
                    )
                else:
                    raw_output, call_meta = call_claude_cli_with_retries(
                        prompt_text=trial_prompt,
                        model=args.model,
                        cwd=args.cwd,
                        max_attempts=args.max_attempts,
                    )
                parse_ok, parsed_output, parse_error = safe_json_parse(raw_output)
                if parse_ok:
                    valid_json_count += 1
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

            duration_ms = int((time.time() - started) * 1000)

            gate3_status: str | None = None
            if parse_ok and isinstance(parsed_output, dict):
                gate3_status = parsed_output.get("status")

            result_record = {
                "trial_at_utc": utc_now_iso(),
                "model": args.model,
                "backend": call_meta.get("backend", "claude-cli"),
                "event_id": event_id,
                "subject_name": subject_name,
                "source_context": source_context,
                "candidates_sent": candidates,
                "duration_ms": duration_ms,
                "llm_error": error,
                "cli_returncode": call_meta.get("returncode"),
                "stderr_excerpt": call_meta.get("stderr_excerpt"),
                "json_parse_ok": parse_ok,
                "json_parse_error": parse_error,
                "gate3_status": gate3_status,
                "parsed_output": parsed_output,
                "raw_output": (raw_output or "")[: args.max_output_chars],
            }
            out_f.write(json.dumps(result_record, ensure_ascii=False, sort_keys=True) + "\n")
            out_f.flush()

            status_label = gate3_status or ("ERROR" if error else "INVALID_JSON")
            print(f"[{idx}/{sample_size}] {status_label} - {subject_name}")
            if error:
                print(f"  ↳ {error}", file=sys.stderr)

            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)

    print(f"Completed {sample_size} records; valid JSON responses: {valid_json_count}")
    print(f"Output written to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
