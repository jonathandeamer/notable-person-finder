#!/usr/bin/env python3
"""Deterministic MediaWiki candidate retrieval for Gate 2."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest


DEFAULT_USER_AGENT = (
    "WikiNotabilityFinder/0.1 "
    "(https://en.wikipedia.org/wiki/User:Jonathan_Deamer; bot) python-urllib/3.x"
)

MW_API = "https://en.wikipedia.org/w/api.php"

NICKNAME_MAP: dict[str, str] = {
    "al": "Alfred",
    "alex": "Alexander",
    "andy": "Andrew",
    "bart": "Bartholomew",
    "ben": "Benjamin",
    "benny": "Benjamin",
    "bert": "Albert",
    "bill": "William",
    "billy": "William",
    "bob": "Robert",
    "bobby": "Robert",
    "chuck": "Charles",
    "charlie": "Charles",
    "chris": "Christopher",
    "dan": "Daniel",
    "danny": "Daniel",
    "dave": "David",
    "dick": "Richard",
    "dot": "Dorothy",
    "dottie": "Dorothy",
    "drew": "Andrew",
    "ed": "Edward",
    "eddie": "Edward",
    "fran": "Frances",
    "frank": "Francis",
    "fred": "Frederick",
    "freddie": "Frederick",
    "gene": "Eugene",
    "geoff": "Geoffrey",
    "hank": "Henry",
    "harry": "Harold",
    "jack": "John",
    "jeff": "Jeffrey",
    "jerry": "Gerald",
    "jim": "James",
    "jimmy": "James",
    "joe": "Joseph",
    "joey": "Joseph",
    "jon": "Jonathan",
    "josh": "Joshua",
    "kate": "Katherine",
    "kathy": "Katherine",
    "ken": "Kenneth",
    "larry": "Lawrence",
    "liz": "Elizabeth",
    "maggie": "Margaret",
    "matt": "Matthew",
    "meg": "Margaret",
    "mike": "Michael",
    "mick": "Michael",
    "mickey": "Michael",
    "nat": "Nathaniel",
    "ned": "Edward",
    "nick": "Nicholas",
    "nicky": "Nicholas",
    "ollie": "Oliver",
    "pat": "Patrick",
    "peg": "Margaret",
    "peggy": "Margaret",
    "pete": "Peter",
    "phil": "Philip",
    "ralph": "Raphael",
    "randy": "Randolph",
    "ray": "Raymond",
    "reg": "Reginald",
    "reggie": "Reginald",
    "rich": "Richard",
    "rick": "Richard",
    "ricky": "Richard",
    "rob": "Robert",
    "robbie": "Robert",
    "rod": "Roderick",
    "ron": "Ronald",
    "ronnie": "Ronald",
    "sam": "Samuel",
    "sandy": "Alexander",
    "stan": "Stanley",
    "steve": "Stephen",
    "stu": "Stuart",
    "sue": "Susan",
    "susie": "Susan",
    "ted": "Edward",
    "terry": "Terence",
    "theo": "Theodore",
    "tim": "Timothy",
    "timmy": "Timothy",
    "tom": "Thomas",
    "tommy": "Thomas",
    "tony": "Anthony",
    "vince": "Vincent",
    "wally": "Walter",
    "walt": "Walter",
    "will": "William",
    "willie": "William",
}

# Inverted map: formal name (lowercase) → list of nickname forms
FORMAL_TO_NICKNAMES: dict[str, list[str]] = {}
for _nick, _formal in NICKNAME_MAP.items():
    FORMAL_TO_NICKNAMES.setdefault(_formal.lower(), []).append(_nick.capitalize())


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch MediaWiki candidates for Gate 2")
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input JSONL (Gate 1 output or prefilter pass + Gate 1 data)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("state/wiki_candidates.jsonl"),
        help="Output JSONL for MediaWiki candidates",
    )
    parser.add_argument("--srlimit", type=int, default=10, help="Search result limit")
    parser.add_argument(
        "--search-max-results",
        type=int,
        default=10,
        help="Hard cap on number of search results to process per subject",
    )
    parser.add_argument("--throttle-ms", type=int, default=900, help="Delay between MW calls")
    parser.add_argument("--max-retries", type=int, default=4, help="Retry count for 429/5xx")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("state/mw_cache"),
        help="Cache directory for MW responses",
    )
    parser.add_argument(
        "--cache-ttl-days",
        type=int,
        default=None,
        help="Expire cached responses older than this many days (omit for no expiry)",
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
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-Agent header to use for MW requests",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Optional log file for request/response diagnostics",
    )
    return parser


def _hash_key(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def normalize_query(name: str) -> str:
    name = (name or "").strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"\s*\([^)]*\)", "", name).strip()
    return name


def query_variants(name: str) -> list[str]:
    variants = []
    base = normalize_query(name)
    if base:
        variants.append(base)
    # remove honorifics
    honorifics = {"sir", "dame", "dr", "mr", "mrs", "ms", "rev", "prof"}
    parts = base.split()
    if parts and parts[0].lower().strip(".") in honorifics:
        v = " ".join(parts[1:])
        if v and v not in variants:
            variants.append(v)
    # swap "Last, First"
    if "," in base:
        bits = [b.strip() for b in base.split(",", 1)]
        swapped = f"{bits[1]} {bits[0]}".strip()
        if swapped and swapped not in variants:
            variants.append(swapped)
    # ASCII fallback
    ascii_only = base.encode("ascii", "ignore").decode("ascii")
    if ascii_only and ascii_only not in variants:
        variants.append(ascii_only)
    # nickname expansion: Nick → Nicholas, Bill → William, etc.
    for expanded in expand_nickname_variants(base):
        if expanded and expanded not in variants:
            variants.append(expanded)
    return variants


def expand_nickname_variants(name: str) -> list[str]:
    """Return alternate-name variants by expanding nicknames in both directions.

    Nickname → formal:  "Nick White"     → ["Nicholas White"]
    Formal → nicknames: "Nicholas White" → ["Nick White", "Nicky White"]
    Returns [] if no match in either direction.
    """
    parts = name.strip().split()
    if not parts:
        return []
    first = parts[0].lower().rstrip(".")
    rest = parts[1:]
    variants: list[str] = []

    # nickname → formal name
    formal = NICKNAME_MAP.get(first)
    if formal:
        variants.append(" ".join([formal] + rest))

    # formal name → nicknames (inverse)
    for nick in FORMAL_TO_NICKNAMES.get(first, []):
        expanded = " ".join([nick] + rest)
        if expanded not in variants:
            variants.append(expanded)

    return variants


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
    user_agent: str,
    throttle_ms: int,
    max_retries: int,
    log_file: Path | None,
) -> dict:
    attempt = 0
    delay = 0.8
    while True:
        attempt += 1
        _sleep(throttle_ms)
        req = urlrequest.Request(
            url,
            headers={
                "User-Agent": user_agent,
                "Accept": "application/json",
            },
            method="GET",
        )
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


def _mw_url(params: dict[str, str]) -> str:
    u = urlparse.urlsplit(MW_API)
    qs = urlparse.urlencode(params)
    return urlparse.urlunsplit((u.scheme, u.netloc, u.path, qs, ""))


def _cache_get(cache_dir: Path, key: str, cache_ttl_days: int | None) -> dict | None:
    path = cache_dir / f"{key}.json"
    if not path.exists():
        return None
    if cache_ttl_days is not None:
        age_days = (time.time() - path.stat().st_mtime) / 86400.0
        if age_days > cache_ttl_days:
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


def mw_search(
    query: str,
    srlimit: int,
    user_agent: str,
    throttle_ms: int,
    max_retries: int,
    cache_dir: Path,
    cache_ttl_days: int | None,
    log_file: Path | None,
    search_max_results: int,
) -> dict:
    params = {
        "action": "query",
        "format": "json",
        "list": "search",
        "srsearch": query,
        "srlimit": str(srlimit),
    }
    key = _hash_key("search:" + query + f":{srlimit}")
    cached = _cache_get(cache_dir, key, cache_ttl_days)
    if cached is not None:
        return cached
    data = _fetch_json(_mw_url(params), user_agent, throttle_ms, max_retries, log_file)
    if not isinstance(data, dict):
        raise ValueError("invalid_search_response")
    all_items = list(data.get("query", {}).get("search", []))
    if search_max_results > 0:
        # Hard cap: do not paginate beyond the first response.
        all_items = all_items[:search_max_results]
        cont = {}
    else:
        cont = data.get("continue") or {}
    while cont.get("sroffset"):
        params["sroffset"] = str(cont["sroffset"])
        more = _fetch_json(_mw_url(params), user_agent, throttle_ms, max_retries, log_file)
        all_items.extend(more.get("query", {}).get("search", []))
        cont = more.get("continue") or {}
    # rebuild response with merged results
    data = {
        "query": {"search": all_items},
        "continue": data.get("continue"),
    }
    _cache_set(cache_dir, key, data)
    return data


def mw_page_details(
    title: str,
    user_agent: str,
    throttle_ms: int,
    max_retries: int,
    cache_dir: Path,
    cache_ttl_days: int | None,
    log_file: Path | None,
) -> dict:
    params = {
        "action": "query",
        "format": "json",
        "redirects": "1",
        "prop": "info|pageprops|description|extracts|categories",
        "inprop": "url",
        "exintro": "1",
        "explaintext": "1",
        "cllimit": "max",
        "titles": title,
    }
    key = _hash_key("page:" + title)
    cached = _cache_get(cache_dir, key, cache_ttl_days)
    if cached is not None:
        return cached
    data = _fetch_json(_mw_url(params), user_agent, throttle_ms, max_retries, log_file)
    # Handle category continuation
    pages = data.get("query", {}).get("pages", {})
    cont = data.get("continue", {})
    while cont.get("clcontinue") and pages:
        params["clcontinue"] = cont["clcontinue"]
        more = _fetch_json(_mw_url(params), user_agent, throttle_ms, max_retries, log_file)
        more_pages = more.get("query", {}).get("pages", {})
        for page_id, page_obj in more_pages.items():
            if page_id in pages and "categories" in page_obj:
                pages[page_id].setdefault("categories", [])
                pages[page_id]["categories"].extend(page_obj.get("categories", []))
        cont = more.get("continue", {})
    data["query"]["pages"] = pages
    _cache_set(cache_dir, key, data)
    return data


def _page_from(json_obj: dict) -> dict | None:
    pages = json_obj.get("query", {}).get("pages")
    if not pages:
        return None
    return pages[next(iter(pages))]


def _normalize_category(cat: str) -> str:
    if not cat:
        return ""
    cat = cat.strip()
    if cat.lower().startswith("category:"):
        cat = cat.split(":", 1)[1]
    return cat.lower().strip()


def biography_score(categories: list[str]) -> int:
    score = 0
    norm = [_normalize_category(c) for c in categories]
    norm = [c for c in norm if c]

    # Strong positives
    if any(c.endswith(" births") for c in norm):
        score += 3
    if any(c.endswith(" deaths") for c in norm):
        score += 3
    if any(c == "living people" for c in norm):
        score += 3

    # People/occupation positives
    if any(" people" in c for c in norm):
        score += 2

    occupation_keywords = (
        "actors",
        "actresses",
        "singers",
        "musicians",
        "composers",
        "writers",
        "poets",
        "journalists",
        "politicians",
        "scientists",
        "engineers",
        "artists",
        "painters",
        "designers",
        "directors",
        "producers",
        "athletes",
        "sportspeople",
        "footballers",
        "players",
        "coaches",
        "lawyers",
        "judges",
        "professors",
        "historians",
        "photographers",
        "dancers",
    )
    if any(any(c.endswith(k) for k in occupation_keywords) for c in norm):
        score += 1

    # Strong negatives
    negative_tokens = (
        "lists",
        "events",
        "awards",
        "templates",
        "disambiguation",
        "articles",
        "redirects",
        "organizations",
        "companies",
        "municipalities",
        "geographic",
        "schools",
        "universities",
        "clubs",
    )
    if any(any(tok in c for tok in negative_tokens) for c in norm):
        score -= 3

    return score


def build_candidate(
    rank: int,
    search_item: dict,
    page: dict | None,
    redirected_from: str | None,
) -> dict:
    categories = []
    if page:
        categories = [c.get("title") for c in page.get("categories", []) if c.get("title")]
    bio_score = biography_score(categories)
    return {
        "rank": rank,
        "title": page.get("title") if page else search_item.get("title"),
        "pageid": page.get("pageid") if page else None,
        "fullurl": page.get("fullurl") if page else None,
        "snippet": search_item.get("snippet"),
        "is_disambig": bool(page and page.get("pageprops", {}).get("disambiguation") is not None),
        "redirected_from": redirected_from,
        "description": page.get("description") if page else None,
        "extract": page.get("extract") if page else None,
        "categories": categories,
        "biography_score": bio_score,
        "biography_prioritized": bio_score >= 3,
    }


def run(
    input_path: Path,
    output_path: Path,
    srlimit: int,
    throttle_ms: int,
    max_retries: int,
    cache_dir: Path,
    cache_ttl_days: int | None,
    overwrite: bool,
    user_agent: str,
    progress_every: int,
    log_file: Path | None,
    search_max_results: int,
) -> int:
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"output exists: {output_path} (use --overwrite)")

    rows = _read_jsonl(input_path)
    seen_event_ids: set[str] = set()
    error_counts = Counter()
    gate1_skip_counts = Counter()
    work_rows: list[dict] = []

    for row in rows:
        event_id = row.get("event_id")

        # Register event_id for ALL records before the decision filter so that a
        # FAIL record blocks any later WEAK_PASS for the same event (first record wins).
        if isinstance(event_id, str):
            if event_id in seen_event_ids:
                gate1_skip_counts["duplicate_event_id"] += 1
                continue
            seen_event_ids.add(event_id)

        subject = row.get("subject_name_full") or row.get("subject_name_as_written")
        if not subject:
            parsed = row.get("parsed_output")
            if isinstance(parsed, dict):
                subject = parsed.get("subject_name_full") or parsed.get("subject_name_as_written")
        if not subject:
            gate1_skip_counts["missing_subject"] += 1
            continue

        decision = row.get("gate1_decision")
        if not decision:
            parsed = row.get("parsed_output")
            if isinstance(parsed, dict):
                decision = parsed.get("gate1_decision")
        if decision not in {"STRONG_PASS", "WEAK_PASS"}:
            gate1_skip_counts[f"gate1_{decision or 'missing'}"] += 1
            continue

        work_rows.append(row)

    total_work = len(work_rows)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as out_f:
        for idx, row in enumerate(work_rows, start=1):
            event_id = row.get("event_id")
            subject = row.get("subject_name_full") or row.get("subject_name_as_written")
            if not subject:
                parsed = row.get("parsed_output")
                if isinstance(parsed, dict):
                    subject = parsed.get("subject_name_full") or parsed.get(
                        "subject_name_as_written"
                    )
            variants = query_variants(subject or "")

            _gi = row.get("gate_input") or {}
            record = {
                "event_id": event_id,
                "subject_name": subject,
                "source_context": {
                    "entry_title": row.get("entry_title"),
                    "summary": row.get("summary") or _gi.get("summary"),
                    "source": row.get("source") or _gi.get("source"),
                    "publication_date": row.get("publication_date") or _gi.get("publication_date"),
                },
                "query": {
                    "original": subject,
                    "normalized": normalize_query(subject or ""),
                    "variants": variants,
                    "variants_searched": [],
                },
                "mw_search": {
                    "srlimit": srlimit,
                    "results": [],
                    "truncated": False,
                    "continue": None,
                },
                "errors": [],
                "fetched_at_utc": utc_now_iso(),
                "feed_priority": row.get("feed_priority"),
                "published_at_utc": _gi.get("publication_date"),
            }

            try:
                seen_pageids: set[int] = set()
                search_items: list[dict] = []
                for variant in variants:
                    search_json = mw_search(
                        variant,
                        srlimit,
                        user_agent,
                        throttle_ms,
                        max_retries,
                        cache_dir,
                        cache_ttl_days,
                        log_file,
                        search_max_results,
                    )
                    if not isinstance(search_json, dict):
                        raise ValueError("invalid_search_response")
                    for result in search_json.get("query", {}).get("search", []):
                        pid = result.get("pageid")
                        if pid and pid not in seen_pageids:
                            seen_pageids.add(pid)
                            search_items.append(result)
                        elif not pid:
                            search_items.append(result)
                    record["query"]["variants_searched"].append(variant)
            except Exception as exc:  # noqa: BLE001
                record["errors"].append(f"search_error:{exc}")
                error_counts["search_error"] += 1
                out_f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                out_f.flush()
                if progress_every and idx % progress_every == 0:
                    print(f"progress: {idx}/{total_work}")
                continue

            for rank, item in enumerate(search_items, start=1):
                title = item.get("title")
                if not title:
                    continue
                redirected_from = None
                try:
                    page_json = mw_page_details(
                        title,
                        user_agent,
                        throttle_ms,
                        max_retries,
                        cache_dir,
                        cache_ttl_days,
                        log_file,
                    )
                    redirect_list = page_json.get("query", {}).get("redirects", [])
                    if redirect_list:
                        redirected_from = redirect_list[0].get("from")
                    page = _page_from(page_json)
                except Exception as exc:  # noqa: BLE001
                    record["errors"].append(f"page_error:{title}:{exc}")
                    error_counts["page_error"] += 1
                    page = None

                record["mw_search"]["results"].append(
                    build_candidate(rank, item, page, redirected_from)
                )

            out_f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
            out_f.flush()
            if progress_every and idx % progress_every == 0:
                print(f"progress: {idx}/{total_work}")

    print(f"records_written: {total_work}")
    print("gate1_skip_counts:")
    for key in sorted(gate1_skip_counts):
        print(f"- {key}: {gate1_skip_counts[key]}")
    print("error_counts:")
    for key in sorted(error_counts):
        print(f"- {key}: {error_counts[key]}")
    print(f"output: {output_path}")
    return 0


def main() -> int:
    args = build_arg_parser().parse_args()
    return run(
        input_path=args.input,
        output_path=args.output,
        srlimit=args.srlimit,
        throttle_ms=args.throttle_ms,
        max_retries=args.max_retries,
        cache_dir=args.cache_dir,
        cache_ttl_days=args.cache_ttl_days,
        overwrite=args.overwrite,
        user_agent=args.user_agent,
        progress_every=args.progress_every,
        log_file=args.log_file,
        search_max_results=args.search_max_results,
    )


if __name__ == "__main__":
    raise SystemExit(main())
