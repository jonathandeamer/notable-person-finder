#!/usr/bin/env python3
"""Deterministic Brave News coverage search for Gate 4."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest


BRAVE_NEWS_API = "https://api.search.brave.com/res/v1/news/search"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Brave News coverage search for Gate 4"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("state/gate3_llm_results.jsonl"),
        help="Input JSONL (Gate 3 output)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("state/brave_coverage.jsonl"),
        help="Output JSONL with Brave News results",
    )
    parser.add_argument(
        "--statuses",
        nargs="+",
        default=["MISSING", "UNCERTAIN"],
        help="gate3_status values to process (default: MISSING UNCERTAIN)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="Results per page per query (max 20)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=2,
        help="Number of result pages to fetch per query (default: 2, giving up to 40 results)",
    )
    parser.add_argument(
        "--throttle-ms",
        type=int,
        default=1100,
        help="Delay between API calls in ms (default: 1100)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Retry count for 429/5xx",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("state/brave_cache"),
        help="Cache directory for Brave responses",
    )
    parser.add_argument(
        "--cache-ttl-days",
        type=int,
        default=None,
        help="Expire cached responses older than this many days (omit for no expiry)",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="Brave Search API key (falls back to BRAVE_API_KEY env var)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=5,
        help="Print progress every N processed records",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file for request/response diagnostics",
    )
    return parser


def _cache_key(query: str, count: int, offset: int = 0) -> str:
    raw = f"brave_news:{query}:{count}:{offset}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _cache_get(cache_dir: Path, key: str, ttl_days: int | None) -> dict | None:
    path = cache_dir / f"{key}.json"
    if not path.exists():
        return None
    if ttl_days is not None:
        age_days = (time.time() - path.stat().st_mtime) / 86400.0
        if age_days > ttl_days:
            return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _cache_set(cache_dir: Path, key: str, payload: dict) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    (cache_dir / f"{key}.json").write_text(
        json.dumps(payload, ensure_ascii=False), encoding="utf-8"
    )


def _sleep(ms: int) -> None:
    time.sleep(ms / 1000.0)


def _log(msg: str, log_file: Path | None) -> None:
    ts = utc_now_iso()
    line = f"{ts} {msg}"
    print(line)
    if log_file is None:
        return
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def _fetch_json(
    url: str,
    headers: dict[str, str],
    max_retries: int,
    throttle_ms: int,
    log_file: Path | None,
) -> dict:
    attempt = 0
    delay = 0.8
    while True:
        attempt += 1
        _sleep(throttle_ms)
        req = urlrequest.Request(url, headers=headers, method="GET")
        try:
            _log(f"request attempt={attempt} url={url}", log_file)
            with urlrequest.urlopen(req, timeout=20) as resp:
                body = resp.read().decode("utf-8")
                parsed = json.loads(body)
                if not isinstance(parsed, dict):
                    raise ValueError(
                        f"invalid_json_type:{type(parsed).__name__} body_excerpt={body[:200]!r}"
                    )
                _log(
                    f"response attempt={attempt} url={url} bytes={len(body)}",
                    log_file,
                )
                return parsed
        except urlerror.HTTPError as exc:
            _log(
                f"http_error attempt={attempt} url={url} code={exc.code}",
                log_file,
            )
            if exc.code in {429, 502, 503, 504} and attempt <= max_retries:
                time.sleep(delay + random.random() * 0.2)
                delay = min(delay * 2, 12.0)
                continue
            raise
        except Exception as exc:
            _log(
                f"request_error attempt={attempt} url={url} error={type(exc).__name__}:{exc}",
                log_file,
            )
            if attempt <= max_retries:
                time.sleep(delay + random.random() * 0.2)
                delay = min(delay * 2, 12.0)
                continue
            raise


def brave_news_search(
    query: str,
    count: int,
    api_key: str,
    cache_dir: Path,
    ttl_days: int | None,
    max_retries: int,
    throttle_ms: int,
    log_file: Path | None,
    pages: int = 1,
) -> list[dict]:
    """Return list of normalised result dicts for a single query, across `pages` pages."""
    headers = {
        "Accept": "application/json",
        "X-Subscription-Token": api_key,
    }
    all_items: list[dict] = []
    seen_urls: set[str] = set()
    for offset in range(pages):
        key = _cache_key(query, count, offset)
        cached = _cache_get(cache_dir, key, ttl_days)
        if cached is not None:
            items = cached.get("results", [])
        else:
            params = urlparse.urlencode({"q": query, "count": count, "offset": offset})
            url = f"{BRAVE_NEWS_API}?{params}"
            data = _fetch_json(url, headers, max_retries, throttle_ms, log_file)
            _cache_set(cache_dir, key, data)
            items = data.get("results", [])
        for item in items:
            u = item.get("url", "")
            if u and u in seen_urls:
                continue
            if u:
                seen_urls.add(u)
            all_items.append(item)
    return [build_result(rank, item) for rank, item in enumerate(all_items, start=1)]


def build_result(rank: int, raw: dict) -> dict:
    """Normalise one Brave News result item into the output schema."""
    url = raw.get("url") or ""
    # Derive source_domain from URL
    try:
        parts = urlparse.urlsplit(url)
        source_domain = parts.netloc.lstrip("www.") if parts.netloc else ""
    except Exception:
        source_domain = ""
    return {
        "rank": rank,
        "title": raw.get("title") or "",
        "url": url,
        "description": raw.get("description") or None,
        "age": raw.get("age") or None,
        "page_age": raw.get("page_age") or None,
        "source_domain": source_domain,
    }


def build_queries(subject_name: str, entry_title: str | None) -> list[str]:
    """Return 1–2 query strings for this subject."""
    primary = f'"{subject_name}"'
    queries = [primary]
    if entry_title and "obituary" in entry_title.lower():
        secondary = f'"{subject_name}" obituary'
        queries.append(secondary)
    return queries


def _read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                print(f"warning: invalid JSON at line {line_no}; skipping")
                continue
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def run(
    input_path: Path,
    output_path: Path,
    statuses: list[str],
    count: int,
    pages: int,
    throttle_ms: int,
    max_retries: int,
    cache_dir: Path,
    cache_ttl_days: int | None,
    api_key: str,
    overwrite: bool,
    progress_every: int,
    log_file: Path | None,
) -> int:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output exists: {output_path} (use --overwrite)")

    rows = _read_jsonl(input_path)
    skip_counts: Counter = Counter()
    error_counts: Counter = Counter()
    work_rows: list[dict] = []

    # Deduplicate by event_id keeping the last occurrence (so retry-appended
    # records supersede earlier failed/null-status records for the same event).
    deduped: dict[str, dict] = {}
    no_id_rows: list[dict] = []
    for row in rows:
        event_id = row.get("event_id")
        if isinstance(event_id, str):
            if event_id in deduped:
                skip_counts["duplicate_event_id"] += 1
            deduped[event_id] = row
        else:
            no_id_rows.append(row)

    for row in list(deduped.values()) + no_id_rows:
        gate3_status = row.get("gate3_status")
        if gate3_status not in statuses:
            skip_counts[f"status_{gate3_status or 'missing'}"] += 1
            continue

        subject = row.get("subject_name")
        if not subject:
            skip_counts["missing_subject"] += 1
            continue

        work_rows.append(row)

    total_work = len(work_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as out_f:
        for idx, row in enumerate(work_rows, start=1):
            event_id = row.get("event_id")
            subject_name = row.get("subject_name", "")
            gate3_status = row.get("gate3_status")
            src_ctx = row.get("source_context") or {}
            entry_title = src_ctx.get("entry_title")

            queries = build_queries(subject_name, entry_title)

            record: dict[str, Any] = {
                "event_id": event_id,
                "subject_name": subject_name,
                "source_context": {
                    "entry_title": src_ctx.get("entry_title"),
                    "summary": src_ctx.get("summary"),
                    "source": src_ctx.get("source"),
                    "publication_date": src_ctx.get("publication_date"),
                },
                "gate3_status": gate3_status,
                "brave_queries": queries,
                "brave_results": [],
                "brave_result_count": 0,
                "errors": [],
                "fetched_at_utc": utc_now_iso(),
            }

            seen_urls: set[str] = set()
            all_results: list[dict] = []

            for query in queries:
                try:
                    results = brave_news_search(
                        query=query,
                        count=count,
                        api_key=api_key,
                        cache_dir=cache_dir,
                        ttl_days=cache_ttl_days,
                        max_retries=max_retries,
                        throttle_ms=throttle_ms,
                        log_file=log_file,
                        pages=pages,
                    )
                    for result in results:
                        url = result.get("url", "")
                        if url and url in seen_urls:
                            continue
                        if url:
                            seen_urls.add(url)
                        all_results.append(result)
                except Exception as exc:
                    record["errors"].append(f"search_error:{query}:{exc}")
                    error_counts["search_error"] += 1

            # Re-rank after deduplication
            for new_rank, result in enumerate(all_results, start=1):
                result["rank"] = new_rank

            record["brave_results"] = all_results
            record["brave_result_count"] = len(all_results)

            out_f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            out_f.flush()

            if progress_every and idx % progress_every == 0:
                print(f"progress: {idx}/{total_work}")

    print(f"records_written: {total_work}")
    print("skip_counts:")
    for key in sorted(skip_counts):
        print(f"  {key}: {skip_counts[key]}")
    print("error_counts:")
    for key in sorted(error_counts):
        print(f"  {key}: {error_counts[key]}")
    print(f"output: {output_path}")
    return 0


def _read_api_key(cli_value: str | None) -> str:
    if cli_value:
        return cli_value
    env = os.environ.get("BRAVE_API_KEY", "")
    if env:
        return env
    key_file = Path.home() / ".brave"
    if key_file.exists():
        return key_file.read_text(encoding="utf-8").strip()
    return ""


def main() -> int:
    args = build_arg_parser().parse_args()
    api_key = _read_api_key(args.api_key)
    if not api_key:
        print("error: no API key found (tried --api-key, BRAVE_API_KEY, ~/.brave)")
        return 1
    return run(
        input_path=args.input,
        output_path=args.output,
        statuses=args.statuses,
        count=args.count,
        pages=args.pages,
        throttle_ms=args.throttle_ms,
        max_retries=args.max_retries,
        cache_dir=args.cache_dir,
        cache_ttl_days=args.cache_ttl_days,
        api_key=api_key,
        overwrite=args.overwrite,
        progress_every=args.progress_every,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
