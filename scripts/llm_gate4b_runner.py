#!/usr/bin/env python3
"""Gate 4b LLM coverage-verifier runner.

Reads state/gate4_reliable_coverage.jsonl and, for each subject with enough
reliable Brave results, asks the LLM to judge whether each article is genuinely
about the subject as its primary focus.

Example:
  python3 scripts/llm_gate4b_runner.py \
    --prompt prompts/gate4b.md \
    --fresh-output
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


def gate4b_output_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["rank", "about_subject", "confidence", "reasoning"],
                    "properties": {
                        "rank": {"type": "integer"},
                        "about_subject": {"type": "boolean"},
                        "confidence": {
                            "type": "number",
                            "minimum": 0.0,
                            "maximum": 1.0,
                        },
                        "reasoning": {"type": "string"},
                    },
                },
            },
        },
    }


def gate4b_unlisted_output_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["rank", "about_subject", "is_reliable_source", "confidence", "reasoning"],
                    "properties": {
                        "rank": {"type": "integer"},
                        "about_subject": {"type": "boolean"},
                        "is_reliable_source": {"type": "boolean"},
                        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                        "reasoning": {"type": "string"},
                    },
                },
            },
        },
    }


def _is_original_source(result: dict, source_context: dict) -> bool:
    """Return True if this result appears to be the original RSS article.

    Checks:
    1. First 60 characters of titles match (case-insensitive).
    2. A significant token from the source name appears in the result's domain.
    """
    entry_title = (source_context.get("entry_title") or "").lower()
    result_title = (result.get("title") or "").lower()
    if entry_title and result_title:
        if entry_title[:60] == result_title[:60]:
            return True

    source_name = (source_context.get("source") or "").strip()
    domain = (result.get("source_domain") or "").lower()
    if source_name and domain:
        tokens = [
            t.lower()
            for t in re.split(r"[^a-zA-Z]+", source_name)
            if len(t) >= 3
        ]
        if tokens and tokens[0] in domain:
            return True

    return False


def format_gate4b_prompt(
    prompt_body: str,
    subject: str,
    source_context: dict,
    results: list[dict],
) -> str:
    """Append subject + source context + numbered results to the prompt body."""
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
    lines.append("Reliable news results to evaluate:")

    for result in results:
        rank = result.get("rank", "?")
        title = result.get("title") or "(untitled)"
        description = result.get("description") or "(none)"
        url = result.get("url") or "(none)"
        domain = result.get("source_domain") or "(unknown)"
        lines.append(f"{rank}. {title}")
        lines.append(f"   Description: {description}")
        lines.append(f"   Domain: {domain}")
        lines.append(f"   URL: {url}")

    lines.append("")
    lines.append(
        "Output the JSON object now. Do not ask for clarification. "
        "Do not include any text outside the JSON."
    )

    return "\n".join(lines)


def format_gate4b_unlisted_prompt(
    prompt_body: str,
    subject: str,
    source_context: dict,
    results: list[dict],
) -> str:
    """Append subject + source context + numbered results to the unlisted prompt body."""
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
    lines.append("All news results to evaluate:")

    for result in results:
        rank = result.get("rank", "?")
        title = result.get("title") or "(untitled)"
        description = result.get("description") or "(none)"
        url = result.get("url") or "(none)"
        domain = result.get("source_domain") or "(unknown)"
        lines.append(f"{rank}. {title}")
        lines.append(f"   Description: {description}")
        lines.append(f"   Domain: {domain}")
        lines.append(f"   URL: {url}")

    lines.append("")
    lines.append(
        "Output the JSON object now. Do not ask for clarification. "
        "Do not include any text outside the JSON."
    )

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
        schema_path = tmp_dir / "gate4b_schema.json"
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


def build_source_context(record: dict) -> dict:
    """Extract source context fields from an input record."""
    ctx = record.get("source_context")
    if isinstance(ctx, dict):
        return ctx
    return {
        "entry_title": record.get("entry_title"),
        "summary": record.get("summary"),
        "source": record.get("source_feed_title") or record.get("source"),
        "publication_date": record.get("published_at_utc") or record.get("publication_date"),
    }


def run(args: argparse.Namespace) -> int:
    prompt_body = args.prompt.read_text(encoding="utf-8")
    records = load_input(args.input)

    # ── Load second-pass resources ────────────────────────────────────────────
    unlisted_prompt_body: str | None = None
    brave_index: dict[str, dict] = {}
    if args.brave_input is not None:
        if args.unlisted_prompt is None:
            print("error: --unlisted-prompt required when --brave-input is set", file=sys.stderr)
            return 1
        unlisted_prompt_body = args.unlisted_prompt.read_text(encoding="utf-8")
        for row in load_input(args.brave_input):
            eid = row.get("event_id")
            if isinstance(eid, str):
                brave_index[eid] = row

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
        sampled = [r for r in records if r.get("event_id") in failed_ids]
    else:
        sampled = records

    # Sort by feed priority tier, then by recency (newest first)
    sampled = sort_by_priority_recency(sampled)

    sample_size = len(sampled)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    output_mode = "w" if args.fresh_output else "a"

    processed = 0
    with args.output.open(output_mode, encoding="utf-8") as out_f:
        for idx, record in enumerate(sampled, start=1):
            processed += 1
            event_id = record.get("event_id")
            subject_name = record.get("subject_name") or ""
            source_context = build_source_context(record)
            gate3_status = record.get("gate3_status")
            all_results = record.get("brave_results") or []

            # Pre-filter: exclude results from the original RSS source
            included_results = []
            excluded_count = 0
            for r in all_results:
                if _is_original_source(r, source_context):
                    excluded_count += 1
                else:
                    included_results.append(r)

            # Short-circuit: not enough reliable results after exclusion
            if len(included_results) < args.min_reliable_results:
                result_record = {
                    "trial_at_utc": utc_now_iso(),
                    "model": args.model,
                    "backend": "deterministic",
                    "event_id": event_id,
                    "subject_name": subject_name,
                    "source_context": source_context,
                    "gate3_status": gate3_status,
                    "results_sent": [],
                    "results_excluded_count": excluded_count,
                    "duration_ms": 0,
                    "llm_error": None,
                    "json_parse_ok": True,
                    "json_parse_error": None,
                    "gate4b_status": "SKIPPED",
                    "confirmed_count": 0,
                    "parsed_output": None,
                    "raw_output": "",
                    "second_pass_results_sent": [],
                    "second_pass_confirmed_count": 0,
                    "second_pass_llm_error": None,
                    "second_pass_json_parse_ok": None,
                    "second_pass_raw_output": "",
                    "second_pass_parsed_output": None,
                }
                out_f.write(
                    json.dumps(result_record, ensure_ascii=False, sort_keys=True) + "\n"
                )
                out_f.flush()
                print(
                    f"[{idx}/{sample_size}] SKIPPED "
                    f"(< {args.min_reliable_results} reliable results after exclusion) "
                    f"- {subject_name}"
                )
                continue

            # Call LLM
            started = time.time()
            error: str | None = None
            raw_output = ""
            parsed_output = None
            parse_ok = False
            parse_error: str | None = None
            call_meta: dict = {}

            try:
                trial_prompt = format_gate4b_prompt(
                    prompt_body=prompt_body,
                    subject=subject_name,
                    source_context=source_context,
                    results=included_results,
                )
                if args.backend == "codex-cli":
                    raw_output, call_meta = call_codex_cli_with_retries(
                        prompt_text=trial_prompt,
                        model=args.model,
                        codex_cwd=args.codex_cwd,
                        output_schema=gate4b_output_schema(),
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
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

            duration_ms = int((time.time() - started) * 1000)

            # Deterministic post-processing
            gate4b_status: str | None = None
            confirmed_count = 0
            if parse_ok and isinstance(parsed_output, dict):
                results_list = parsed_output.get("results") or []
                confirmed_count = sum(
                    1 for r in results_list if r.get("about_subject") is True
                )
                if confirmed_count >= 2:
                    gate4b_status = "LIKELY_NOTABLE"
                elif confirmed_count == 1:
                    gate4b_status = "UNCERTAIN"
                else:
                    gate4b_status = "NOT_NOTABLE"

            # ── Second pass: POSSIBLY_NOTABLE check ──────────────────────────
            second_pass_results_sent: list[dict] = []
            second_pass_confirmed_count: int = 0
            second_pass_llm_error: str | None = None
            second_pass_json_parse_ok: bool | None = None  # None = not run
            second_pass_raw_output: str = ""
            second_pass_parsed_output: object | None = None

            if args.brave_input is not None and gate4b_status != "LIKELY_NOTABLE":
                brave_record = brave_index.get(event_id) if event_id else None
                all_brave = (brave_record.get("brave_results") or []) if brave_record else []
                candidates = [r for r in all_brave if not _is_original_source(r, source_context)]
                if candidates:
                    second_pass_results_sent = candidates
                    try:
                        sp_prompt = format_gate4b_unlisted_prompt(
                            unlisted_prompt_body, subject_name, source_context, candidates
                        )
                        if args.backend == "codex-cli":
                            sp_raw, _ = call_codex_cli_with_retries(
                                prompt_text=sp_prompt,
                                model=args.model,
                                codex_cwd=args.codex_cwd,
                                output_schema=gate4b_unlisted_output_schema(),
                                max_attempts=args.max_attempts,
                            )
                        else:
                            sp_raw, _ = call_claude_cli_with_retries(
                                prompt_text=sp_prompt,
                                model=args.model,
                                cwd=args.cwd,
                                max_attempts=args.max_attempts,
                            )
                        second_pass_raw_output = sp_raw
                        sp_ok, sp_parsed, _ = safe_json_parse(sp_raw)
                        second_pass_json_parse_ok = sp_ok
                        second_pass_parsed_output = sp_parsed
                        if sp_ok and isinstance(sp_parsed, dict):
                            second_pass_confirmed_count = sum(
                                1 for r in (sp_parsed.get("results") or [])
                                if r.get("about_subject") is True and r.get("is_reliable_source") is True
                            )
                            if second_pass_confirmed_count >= 2:
                                gate4b_status = "POSSIBLY_NOTABLE"
                    except Exception as exc:  # noqa: BLE001
                        second_pass_llm_error = str(exc)
                        second_pass_json_parse_ok = False

            result_record = {
                "trial_at_utc": utc_now_iso(),
                "model": args.model,
                "backend": call_meta.get("backend", "claude-cli"),
                "event_id": event_id,
                "subject_name": subject_name,
                "source_context": source_context,
                "gate3_status": gate3_status,
                "results_sent": included_results,
                "results_excluded_count": excluded_count,
                "duration_ms": duration_ms,
                "llm_error": error,
                "json_parse_ok": parse_ok,
                "json_parse_error": parse_error,
                "gate4b_status": gate4b_status,
                "confirmed_count": confirmed_count,
                "parsed_output": parsed_output,
                "raw_output": (raw_output or "")[: args.max_output_chars],
                "second_pass_results_sent": second_pass_results_sent,
                "second_pass_confirmed_count": second_pass_confirmed_count,
                "second_pass_llm_error": second_pass_llm_error,
                "second_pass_json_parse_ok": second_pass_json_parse_ok,
                "second_pass_raw_output": (second_pass_raw_output or "")[: args.max_output_chars],
                "second_pass_parsed_output": second_pass_parsed_output,
            }
            out_f.write(
                json.dumps(result_record, ensure_ascii=False, sort_keys=True) + "\n"
            )
            out_f.flush()

            status_label = gate4b_status or ("ERROR" if error else "INVALID_JSON")
            print(f"[{idx}/{sample_size}] {status_label} - {subject_name}")
            if error:
                print(f"  ↳ {error}", file=sys.stderr)

            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)

    print(f"Completed {processed} records.")
    print(f"Output written to: {args.output}")
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gate 4b LLM coverage-verifier runner")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/gate4_reliable_coverage.jsonl"),
        help="Path to gate4_reliable_coverage.jsonl (default: state/gate4_reliable_coverage.jsonl)",
    )
    parser.add_argument(
        "--prompt",
        type=Path,
        required=True,
        help="Path to prompts/gate4b.md",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("state/gate4b_llm_results.jsonl"),
        help="Path to gate4b_llm_results.jsonl (default: state/gate4b_llm_results.jsonl)",
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
        "--min-reliable-results",
        type=int,
        default=2,
        help="Minimum reliable results needed to qualify for LLM (default: 2)",
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
        help="Re-run only subjects whose existing output record has json_parse_ok=False or llm_error set",
    )
    parser.add_argument(
        "--brave-input",
        type=Path,
        default=None,
        help="Path to brave_coverage.jsonl (full unfiltered results). Enables second pass.",
    )
    parser.add_argument(
        "--unlisted-prompt",
        type=Path,
        default=None,
        help="Path to prompts/gate4b_unlisted.md. Required when --brave-input is set.",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
