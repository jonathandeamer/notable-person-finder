#!/usr/bin/env python3
"""Quick trial runner for Gate 1 prompt against events.jsonl.

Example:
  OPENAI_API_KEY=... python3 scripts/llm_gate1_runner.py \
    --events state/prefilter_pass.jsonl \
    --prompt prompts/gate1.md \
    --output state/gate1_llm_results.jsonl \
    --sample-size 20 \
    --model gpt-5.1-codex-mini
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


CODEX_BATCH_SIZE = 10


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Gate1 prompt trials on events.jsonl")
    parser.add_argument(
        "--events",
        type=Path,
        default=None,
        help="Path to events.jsonl. If omitted, auto-uses state/prefilter_pass.jsonl when present, else state/events.jsonl",
    )
    parser.add_argument(
        "--prompt", type=Path, required=True, help="Path to Gate 1 prompt file"
    )
    parser.add_argument("--output", type=Path, required=True, help="Path to output jsonl")
    parser.add_argument("--model", default="gpt-5.2", help="Model name")
    parser.add_argument(
        "--backend",
        choices=["openai-api", "codex-cli", "claude-cli"],
        default="codex-cli",
        help="LLM backend to use",
    )
    parser.add_argument(
        "--codex-cwd",
        type=Path,
        default=Path.cwd(),
        help="Working directory for codex exec when using codex-cli backend",
    )
    parser.add_argument(
        "--claude-cwd",
        type=Path,
        default=Path.cwd(),
        help="Working directory for claude CLI when using claude-cli backend",
    )
    parser.add_argument("--sample-size", type=int, default=20, help="Number of events to test")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument(
        "--sort-by",
        choices=["recency", "random", "priority"],
        default="priority",
        help="How to prioritise events from the unprocessed pool: "
             "'priority' (by feed_priority then recency), 'recency' (most-recently published first), or 'random' (default: priority)",
    )
    parser.add_argument(
        "--keyword-regex",
        default=None,
        help="Optional regex to filter events by title+summary before sampling",
    )
    parser.add_argument(
        "--max-output-chars",
        type=int,
        default=5000,
        help="Truncate stored raw output text to this size",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=None,
        help="Sampling temperature for model call (omit for model default)",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.0,
        help="Delay between requests to reduce burstiness",
    )
    parser.add_argument(
        "--codex-max-attempts",
        type=int,
        default=3,
        help="Retry attempts for codex exec failures",
    )
    parser.add_argument(
        "--claude-max-attempts",
        type=int,
        default=3,
        help="Retry attempts for claude CLI failures",
    )
    parser.add_argument(
        "--retry-missing-event-ids",
        action="store_true",
        help="Retry missing event_ids from batch output as single-item calls",
    )
    parser.add_argument(
        "--fresh-output",
        action="store_true",
        help="Write to a clean output file for this run (no append)",
    )
    parser.add_argument(
        "--retry-parse-failures",
        action="store_true",
        help="Re-run only events whose existing output record has json_parse_ok=False or llm_error set",
    )
    return parser


def load_events(events_path: Path) -> list[dict]:
    events: list[dict] = []
    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            events.append(json.loads(line))
    return events


def resolve_events_path(
    explicit_events: Path | None,
    prefilter_pass_path: Path = Path("state/prefilter_pass.jsonl"),
    raw_events_path: Path = Path("state/events.jsonl"),
) -> Path:
    if explicit_events is not None:
        return explicit_events
    if prefilter_pass_path.exists():
        return prefilter_pass_path
    return raw_events_path


def map_gate_input(event: dict) -> dict:
    return {
        "title": event.get("entry_title"),
        "summary": event.get("summary"),
        "source": event.get("source_feed_title") or event.get("source_feed_url_resolved"),
        "publication_date": event.get("published_at_utc"),
    }


def _extract_response_text(body: dict) -> str:
    text = body.get("output_text")
    if isinstance(text, str) and text.strip():
        return text

    outputs = body.get("output")
    if not isinstance(outputs, list):
        return ""

    chunks: list[str] = []
    for item in outputs:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            part_type = (part.get("type") or "").lower()
            # Responses API may return text under output_text/text fields by model/version.
            if part_type in {"output_text", "text"}:
                value = part.get("text") or part.get("output_text")
                if isinstance(value, str) and value:
                    chunks.append(value)
    return "\n".join(chunks).strip()


def call_openai(prompt_text: str, model: str, temperature: float | None) -> tuple[str, dict]:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    payload = {"model": model, "input": prompt_text}
    if temperature is not None:
        payload["temperature"] = temperature
    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=90) as response:
        body = json.loads(response.read().decode("utf-8"))
    return _extract_response_text(body), body


def gate1_output_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "person_detected",
            "subject_name_as_written",
            "subject_name_full",
            "name_completeness",
            "primary_focus",
            "gate1_decision",
            "reasoning_summary",
            "signal_type",
            "confidence",
        ],
        "properties": {
            "person_detected": {"type": "boolean"},
            "subject_name_as_written": {"type": ["string", "null"]},
            "subject_name_full": {"type": ["string", "null"]},
            "name_completeness": {
                "type": "string",
                "enum": ["FULL_NAME", "SINGLE_TOKEN", "UNKNOWN"],
            },
            "primary_focus": {"type": "boolean"},
            "gate1_decision": {
                "type": "string",
                "enum": ["STRONG_PASS", "WEAK_PASS", "FAIL", "SKIP_GLOBALLY_KNOWN"],
            },
            "reasoning_summary": {
                "type": "array",
                "minItems": 1,
                "maxItems": 2,
                "items": {"type": "string"},
            },
            "signal_type": {
                "type": "string",
                "enum": [
                    "EDITORIAL_OBIT",
                    "CAREER_PROFILE",
                    "PUBLIC_ROLE",
                    "MID_TIER_PROFESSIONAL",
                    "SINGLE_EVENT",
                    "COLLECTIVE",
                    "GLOBALLY_FAMOUS",
                    "OTHER",
                ],
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
            },
        },
    }


def gate1_item_with_event_id_schema() -> dict:
    base = gate1_output_schema()
    props = dict(base["properties"])
    props["event_id"] = {"type": "string"}
    required = ["event_id"] + [x for x in base["required"]]
    return {
        "type": "object",
        "additionalProperties": False,
        "required": required,
        "properties": props,
    }


def gate1_batch_output_schema() -> dict:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["items"],
        "properties": {
            "items": {
                "type": "array",
                "minItems": 1,
                "items": gate1_item_with_event_id_schema(),
            }
        },
    }


def call_codex_cli(
    prompt_text: str,
    model: str,
    codex_cwd: Path,
    output_schema: dict,
) -> tuple[str, dict]:
    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        schema_path = tmp_dir / "gate1_schema.json"
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


def call_claude_cli(prompt_text: str, model: str, cwd: Path) -> tuple[str, dict]:
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    cmd = ["claude", "--model", model, "-p", "-"]
    proc = subprocess.run(
        cmd, input=prompt_text, capture_output=True, text=True,
        check=False, cwd=str(cwd), env=env,
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


def chunked(items: list[dict], size: int) -> list[list[dict]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


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


def main() -> int:
    args = build_arg_parser().parse_args()
    random.seed(args.seed)

    prompt_body = args.prompt.read_text(encoding="utf-8")
    events_path = resolve_events_path(args.events)
    print(f"Using events input: {events_path}")
    events = load_events(events_path)

    if args.keyword_regex is not None:
        rx = re.compile(args.keyword_regex, re.IGNORECASE)
        filtered = [
            e for e in events
            if rx.search(f"{e.get('entry_title', '')} {e.get('summary', '')}")
        ]
        if not filtered:
            print("No events matched keyword filter.", file=sys.stderr)
            return 1
    else:
        filtered = events

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.fresh_output:
        if not args.output.stem.endswith("_latest"):
            args.output = args.output.with_name(
                f"{args.output.stem}_latest{args.output.suffix}"
            )
        print(f"Using fresh output: {args.output}")

    # Build already-processed set BEFORE sampling so we select from the correct pool.
    already_processed: set[str] = set()
    if not args.fresh_output:
        for _rec in _read_existing_output(args.output):
            if _rec.get("llm_error") is None and _rec.get("json_parse_ok"):
                _eid = _rec.get("event_id")
                if isinstance(_eid, str):
                    already_processed.add(_eid)

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
        sampled = [e for e in events if e.get("event_id") in failed_ids]
    else:
        unprocessed = [e for e in filtered if e.get("event_id") not in already_processed]
        if already_processed:
            print(
                f"Skipping {len(already_processed)} already-processed event_id(s); "
                f"{len(unprocessed)} unprocessed remaining."
            )
        if args.sort_by == "priority":
            # Two stable sorts: first by recency descending, then by priority ascending.
            # Within each priority tier, recency order is preserved.
            unprocessed.sort(
                key=lambda e: e.get("published_at_utc") or "",
                reverse=True,
            )
            _PRIORITY_MAX = float("inf")
            unprocessed.sort(
                key=lambda e: e.get("feed_priority") if e.get("feed_priority") is not None else _PRIORITY_MAX,
            )
            sampled = unprocessed[: args.sample_size]
        elif args.sort_by == "recency":
            unprocessed.sort(
                key=lambda e: e.get("published_at_utc") or "",
                reverse=True,
            )
            sampled = unprocessed[: args.sample_size]
        else:
            sampled = random.sample(unprocessed, min(args.sample_size, len(unprocessed)))

    sample_size = len(sampled)

    valid_json_count = 0
    output_mode = "w" if args.fresh_output else "a"
    with args.output.open(output_mode, encoding="utf-8") as out_f:
        if args.backend == "openai-api":
            for idx, event in enumerate(sampled, start=1):
                gate_input = map_gate_input(event)
                trial_prompt = (
                    f"{prompt_body}\n\nInput:\n{json.dumps(gate_input, ensure_ascii=False)}"
                )

                started = time.time()
                error = None
                raw_output = ""
                parsed_output = None
                parse_ok = False
                parse_error = None

                response_id = None
                response_status = None
                response_debug_excerpt = None
                try:
                    raw_output, raw_body = call_openai(
                        prompt_text=trial_prompt,
                        model=args.model,
                        temperature=args.temperature,
                    )
                    response_id = raw_body.get("id")
                    response_status = raw_body.get("status")
                    if not raw_output:
                        response_debug_excerpt = json.dumps(raw_body, ensure_ascii=False)[:1000]
                    parse_ok, parsed_output, parse_error = safe_json_parse(raw_output)
                    if parse_ok:
                        valid_json_count += 1
                except urllib.error.HTTPError as exc:
                    error = f"http_error_{exc.code}"
                    raw_output = exc.read().decode("utf-8", errors="replace")
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)

                duration_ms = int((time.time() - started) * 1000)
                decision = (
                    parsed_output.get("gate1_decision")
                    if parse_ok and isinstance(parsed_output, dict)
                    else None
                )

                trial_record = {
                    "trial_at_utc": utc_now_iso(),
                    "model": args.model,
                    "backend": args.backend,
                    "event_id": event.get("event_id"),
                    "entry_url_canonical": event.get("entry_url_canonical"),
                    "entry_title": event.get("entry_title"),
                    "gate_input": gate_input,
                    "request_prompt_excerpt": prompt_body[:500],
                    "duration_ms": duration_ms,
                    "llm_error": error,
                    "response_id": response_id,
                    "response_status": response_status,
                    "response_debug_excerpt": response_debug_excerpt,
                    "json_parse_ok": parse_ok,
                    "json_parse_error": parse_error,
                    "gate1_decision": decision,
                    "parsed_output": parsed_output,
                    "raw_output": (raw_output or "")[: args.max_output_chars],
                }
                out_f.write(json.dumps(trial_record, ensure_ascii=False, sort_keys=True) + "\n")
                out_f.flush()

                status = decision or ("ERROR" if error else "INVALID_JSON")
                print(f"[{idx}/{sample_size}] {status} - {event.get('entry_title')}")
                if error:
                    print(f"  ↳ {error}", file=sys.stderr)

                if args.delay_seconds > 0:
                    time.sleep(args.delay_seconds)
        elif args.backend == "claude-cli":
            for idx, event in enumerate(sampled, start=1):
                gate_input = map_gate_input(event)
                trial_prompt = f"{prompt_body}\n\nInput:\n{json.dumps(gate_input, ensure_ascii=False)}"

                started = time.time()
                error = None
                raw_output = ""
                parsed_output = None
                parse_ok = False
                parse_error = None
                call_meta: dict = {}

                try:
                    raw_output, call_meta = call_claude_cli_with_retries(
                        prompt_text=trial_prompt,
                        model=args.model,
                        cwd=args.claude_cwd,
                        max_attempts=args.claude_max_attempts,
                    )
                    parse_ok, parsed_output, parse_error = safe_json_parse(raw_output)
                    if parse_ok:
                        valid_json_count += 1
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)

                duration_ms = int((time.time() - started) * 1000)
                decision = (
                    parsed_output.get("gate1_decision")
                    if parse_ok and isinstance(parsed_output, dict)
                    else None
                )
                trial_record = {
                    "trial_at_utc": utc_now_iso(),
                    "model": args.model,
                    "backend": args.backend,
                    "event_id": event.get("event_id"),
                    "entry_url_canonical": event.get("entry_url_canonical"),
                    "entry_title": event.get("entry_title"),
                    "gate_input": gate_input,
                    "request_prompt_excerpt": prompt_body[:500],
                    "duration_ms": duration_ms,
                    "llm_error": error,
                    "response_id": None,
                    "response_status": f"claude_rc_{call_meta.get('returncode')}" if call_meta else None,
                    "response_debug_excerpt": call_meta.get("stderr_excerpt") if not raw_output else None,
                    "json_parse_ok": parse_ok,
                    "json_parse_error": parse_error,
                    "gate1_decision": decision,
                    "parsed_output": parsed_output,
                    "raw_output": (raw_output or "")[: args.max_output_chars],
                }
                out_f.write(json.dumps(trial_record, ensure_ascii=False, sort_keys=True) + "\n")
                out_f.flush()

                status = decision or ("ERROR" if error else "INVALID_JSON")
                print(f"[{idx}/{sample_size}] {status} - {event.get('entry_title')}")
                if error:
                    print(f"  ↳ {error}", file=sys.stderr)

                if args.delay_seconds > 0:
                    time.sleep(args.delay_seconds)
        else:
            processed = 0
            for batch_idx, batch_events in enumerate(chunked(sampled, CODEX_BATCH_SIZE), start=1):
                batch_inputs = []
                for event in batch_events:
                    batch_inputs.append(
                        {
                            "event_id": event.get("event_id"),
                            **map_gate_input(event),
                        }
                    )

                trial_prompt = (
                    f"{prompt_body}\n\n"
                    "Now process multiple inputs at once.\n"
                    "Return ONLY a JSON object with key 'items'.\n"
                    "'items' must be an array with one object per input.\n"
                    "Each object must include: event_id and all required Gate 1 fields.\n"
                    "Preserve event_id exactly from input.\n"
                    "Use the same order as the input array.\n\n"
                    f"Input array:\n{json.dumps(batch_inputs, ensure_ascii=False)}"
                )

                started = time.time()
                error = None
                raw_output = ""
                parsed_output = None
                parse_ok = False
                parse_error = None
                response_id = None
                response_status = None
                response_debug_excerpt = None
                try:
                    raw_output, raw_body = call_codex_cli_with_retries(
                        prompt_text=trial_prompt,
                        model=args.model,
                        codex_cwd=args.codex_cwd,
                        output_schema=gate1_batch_output_schema(),
                        max_attempts=args.codex_max_attempts,
                    )
                    response_status = f"codex_rc_{raw_body.get('returncode')}"
                    if not raw_output:
                        response_debug_excerpt = json.dumps(raw_body, ensure_ascii=False)[:1000]
                    parse_ok, parsed_output, parse_error = safe_json_parse(raw_output)
                except Exception as exc:  # noqa: BLE001
                    error = str(exc)

                duration_ms = int((time.time() - started) * 1000)

                batch_result_by_event_id: dict[str, dict] = {}
                parsed_items = None
                if parse_ok and isinstance(parsed_output, dict):
                    maybe_items = parsed_output.get("items")
                    if isinstance(maybe_items, list):
                        parsed_items = maybe_items
                elif parse_ok and isinstance(parsed_output, list):
                    # Backward-compat in case model still returns top-level array.
                    parsed_items = parsed_output

                if parse_ok and isinstance(parsed_items, list):
                    for item in parsed_items:
                        if isinstance(item, dict) and isinstance(item.get("event_id"), str):
                            batch_result_by_event_id[item["event_id"]] = item
                elif parse_ok:
                    parse_ok = False
                    parse_error = "expected_json_object_with_items_array_for_batch"

                missing_events = []
                for i, event in enumerate(batch_events, start=1):
                    processed += 1
                    gate_input = map_gate_input(event)
                    item_output = batch_result_by_event_id.get(event.get("event_id"))
                    item_parse_ok = bool(item_output)
                    if item_parse_ok:
                        valid_json_count += 1
                    item_parse_error = None if item_parse_ok else (
                        parse_error or "event_id_missing_in_batch_output"
                    )
                    decision = item_output.get("gate1_decision") if item_parse_ok else None

                    trial_record = {
                        "trial_at_utc": utc_now_iso(),
                        "model": args.model,
                        "backend": args.backend,
                        "batch_index": batch_idx,
                        "batch_size": len(batch_events),
                        "event_id": event.get("event_id"),
                        "entry_url_canonical": event.get("entry_url_canonical"),
                        "entry_title": event.get("entry_title"),
                        "gate_input": gate_input,
                        "request_prompt_excerpt": prompt_body[:500],
                        "duration_ms": duration_ms,
                        "llm_error": error,
                        "response_id": response_id,
                        "response_status": response_status,
                        "response_debug_excerpt": response_debug_excerpt,
                        "json_parse_ok": item_parse_ok,
                        "json_parse_error": item_parse_error,
                        "gate1_decision": decision,
                        "parsed_output": item_output,
                        "raw_output": (raw_output or "")[: args.max_output_chars],
                    }
                    out_f.write(json.dumps(trial_record, ensure_ascii=False, sort_keys=True) + "\n")
                    out_f.flush()

                    status = decision or ("ERROR" if error else "INVALID_JSON")
                    print(f"[{processed}/{sample_size}] {status} - {event.get('entry_title')}")
                    if error:
                        print(f"  ↳ {error}", file=sys.stderr)
                    if not item_parse_ok and args.retry_missing_event_ids:
                        missing_events.append(event)

                if missing_events and args.retry_missing_event_ids:
                    for event in missing_events:
                        single_prompt = (
                            f"{prompt_body}\n\nInput:\n"
                            f"{json.dumps(map_gate_input(event), ensure_ascii=False)}"
                        )
                        single_started = time.time()
                        single_error = None
                        single_raw = ""
                        single_parsed = None
                        single_parse_ok = False
                        single_parse_error = None
                        single_response_debug = None
                        try:
                            single_raw, single_body = call_codex_cli_with_retries(
                                prompt_text=single_prompt,
                                model=args.model,
                                codex_cwd=args.codex_cwd,
                                output_schema=gate1_output_schema(),
                                max_attempts=args.codex_max_attempts,
                            )
                            if not single_raw:
                                single_response_debug = json.dumps(
                                    single_body, ensure_ascii=False
                                )[:1000]
                            single_parse_ok, single_parsed, single_parse_error = safe_json_parse(
                                single_raw
                            )
                            if single_parse_ok:
                                valid_json_count += 1
                        except Exception as exc:  # noqa: BLE001
                            single_error = str(exc)

                        single_duration = int((time.time() - single_started) * 1000)
                        single_decision = (
                            single_parsed.get("gate1_decision")
                            if single_parse_ok and isinstance(single_parsed, dict)
                            else None
                        )
                        retry_record = {
                            "trial_at_utc": utc_now_iso(),
                            "model": args.model,
                            "backend": args.backend,
                            "event_id": event.get("event_id"),
                            "entry_url_canonical": event.get("entry_url_canonical"),
                            "entry_title": event.get("entry_title"),
                            "gate_input": map_gate_input(event),
                            "request_prompt_excerpt": prompt_body[:500],
                            "duration_ms": single_duration,
                            "llm_error": single_error,
                            "response_debug_excerpt": single_response_debug,
                            "json_parse_ok": single_parse_ok,
                            "json_parse_error": single_parse_error,
                            "gate1_decision": single_decision,
                            "parsed_output": single_parsed,
                            "raw_output": (single_raw or "")[: args.max_output_chars],
                            "retry_of_batch": batch_idx,
                        }
                        out_f.write(
                            json.dumps(retry_record, ensure_ascii=False, sort_keys=True) + "\n"
                        )
                        out_f.flush()
                        status = single_decision or (
                            "ERROR" if single_error else "INVALID_JSON"
                        )
                        print(f"[retry] {status} - {event.get('entry_title')}")
                        if single_error:
                            print(f"  ↳ {single_error}", file=sys.stderr)

                if args.delay_seconds > 0:
                    time.sleep(args.delay_seconds)

    print(f"Completed {sample_size} trials; valid JSON responses: {valid_json_count}")
    print(f"Output appended to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
