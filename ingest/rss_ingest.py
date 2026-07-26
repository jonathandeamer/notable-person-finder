"""Deterministic RSS/Atom ingest script.

Usage:
    python -m ingest.rss_ingest --feeds /path/config/feeds.md --state-dir /path/state
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Callable
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest
import xml.etree.ElementTree as ET


TRACKING_PARAM_EXACT = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "src",
}
TRACKING_PARAM_PREFIXES = ("utm_",)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_json_dumps(value: object) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def canonical_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def event_id_for(
    entry_url_canonical: str,
    entry_guid: str | None,
    source_feed_url_original: str,
    published_at_utc: str | None,
) -> str:
    bucket = (published_at_utc or "unknown-date")[:10]
    payload = (
        f"{entry_url_canonical}|{entry_guid or ''}|"
        f"{source_feed_url_original}|{bucket}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_feeds_markdown(path: Path) -> list[str]:
    urls: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("- "):
            maybe_url = line[2:].strip()
            # Strip optional trailing priority number: "https://example.com 2.5" -> "https://example.com"
            parts = maybe_url.rsplit(None, 1)
            if len(parts) == 2:
                try:
                    float(parts[1])
                    maybe_url = parts[0]
                except ValueError:
                    pass
            if maybe_url.startswith("http://") or maybe_url.startswith("https://"):
                urls.append(maybe_url)
    return urls


def _candidate_feed_urls(original_url: str) -> list[str]:
    if original_url.startswith("http://"):
        return ["https://" + original_url[len("http://") :], original_url]
    return [original_url]


def normalize_url(raw_url: str) -> str | None:
    raw_url = (raw_url or "").strip()
    if not raw_url:
        return None
    if raw_url.startswith("//"):
        raw_url = "https:" + raw_url
    parts = urlparse.urlsplit(raw_url)
    if not parts.scheme and parts.netloc:
        parts = urlparse.urlsplit("https://" + raw_url)
    if not parts.netloc:
        return None

    scheme = (parts.scheme or "https").lower()
    hostname = (parts.hostname or "").lower()
    if not hostname:
        return None
    port = parts.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None
    netloc = hostname if port is None else f"{hostname}:{port}"

    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if not path.startswith("/"):
        path = "/" + path
    if path != "/" and path.endswith("/"):
        path = path[:-1]

    kept_pairs: list[tuple[str, str]] = []
    for key, value in urlparse.parse_qsl(parts.query, keep_blank_values=True):
        k = key.lower().strip()
        if not k:
            continue
        if k in TRACKING_PARAM_EXACT:
            continue
        if any(k.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        kept_pairs.append((k, value))
    kept_pairs.sort(key=lambda kv: (kv[0], kv[1]))
    query = urlparse.urlencode(kept_pairs, doseq=True)

    return urlparse.urlunsplit((scheme, netloc, path, query, ""))


def parse_datetime_to_rfc3339(date_text: str | None) -> tuple[str | None, bool]:
    date_text = (date_text or "").strip()
    if not date_text:
        return None, True

    try:
        dt = parsedate_to_datetime(date_text)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return to_rfc3339(dt), False
    except (TypeError, ValueError):
        pass

    iso_candidate = date_text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return to_rfc3339(dt), False
    except ValueError:
        return None, True


def _strip_tag_ns(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _first_text(node: ET.Element, names: tuple[str, ...]) -> str | None:
    for child in node:
        local = _strip_tag_ns(child.tag).lower()
        if local in names:
            text = (child.text or "").strip()
            if text:
                return text
    return None


def _extract_link(entry: ET.Element) -> str | None:
    for child in entry:
        local = _strip_tag_ns(child.tag).lower()
        if local != "link":
            continue
        href = child.attrib.get("href")
        if href:
            rel = (child.attrib.get("rel") or "alternate").strip().lower()
            if rel in {"alternate", ""}:
                return href.strip()
    text_link = _first_text(entry, ("link",))
    if text_link:
        return text_link
    return None


def _extract_author(entry: ET.Element) -> str | None:
    direct = _first_text(entry, ("author", "dc:creator", "creator"))
    if direct:
        return direct
    for child in entry:
        if _strip_tag_ns(child.tag).lower() != "author":
            continue
        name = _first_text(child, ("name",))
        if name:
            return name
    return None


@dataclass
class ParsedEntry:
    title: str
    url_raw: str
    guid: str | None
    summary: str | None
    author: str | None
    published_raw: str | None


def parse_feed_bytes(content: bytes) -> tuple[str | None, list[ParsedEntry]]:
    root = ET.fromstring(content)
    root_name = _strip_tag_ns(root.tag).lower()

    feed_title: str | None = None
    entries: list[ParsedEntry] = []

    if root_name == "rss":
        channel = None
        for child in root:
            if _strip_tag_ns(child.tag).lower() == "channel":
                channel = child
                break
        if channel is None:
            return None, []
        feed_title = _first_text(channel, ("title",))
        for item in channel:
            if _strip_tag_ns(item.tag).lower() != "item":
                continue
            title = _first_text(item, ("title",)) or "(untitled)"
            link = _extract_link(item)
            if not link:
                continue
            entries.append(
                ParsedEntry(
                    title=title,
                    url_raw=link,
                    guid=_first_text(item, ("guid", "id")),
                    summary=_first_text(item, ("description", "summary", "content")),
                    author=_extract_author(item),
                    published_raw=_first_text(
                        item, ("pubdate", "published", "updated", "dc:date")
                    ),
                )
            )
        return feed_title, entries

    if root_name == "feed":
        feed_title = _first_text(root, ("title",))
        for entry in root:
            if _strip_tag_ns(entry.tag).lower() != "entry":
                continue
            title = _first_text(entry, ("title",)) or "(untitled)"
            link = _extract_link(entry)
            if not link:
                continue
            entries.append(
                ParsedEntry(
                    title=title,
                    url_raw=link,
                    guid=_first_text(entry, ("id", "guid")),
                    summary=_first_text(entry, ("summary", "content", "description")),
                    author=_extract_author(entry),
                    published_raw=_first_text(entry, ("published", "updated", "dc:date")),
                )
            )
        return feed_title, entries

    return None, []


@dataclass
class FeedFetchResult:
    source_feed_url_original: str
    source_feed_url_resolved: str | None
    fetched_at_utc: str
    http_status: int | None
    content: bytes | None
    etag: str | None
    last_modified: str | None
    not_modified: bool
    error: str | None


def _is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, TimeoutError | socket.timeout):
        return True
    if isinstance(exc, urlerror.URLError):
        return True
    if isinstance(exc, urlerror.HTTPError):
        return exc.code >= 500 or exc.code == 429
    return False


def fetch_feed(
    source_feed_url_original: str,
    prior_state: dict[str, str],
    timeout_seconds: float,
    retries: int,
    user_agent: str,
) -> FeedFetchResult:
    etag = prior_state.get("etag")
    last_modified = prior_state.get("last_modified")

    attempts = 0
    last_exc: Exception | None = None
    for candidate_url in _candidate_feed_urls(source_feed_url_original):
        attempts = 0
        while attempts <= retries:
            attempts += 1
            headers = {"User-Agent": user_agent}
            if etag:
                headers["If-None-Match"] = etag
            if last_modified:
                headers["If-Modified-Since"] = last_modified

            req = urlrequest.Request(candidate_url, headers=headers, method="GET")
            fetched_at_utc = to_rfc3339(utc_now())
            try:
                with urlrequest.urlopen(req, timeout=timeout_seconds) as response:
                    content = response.read()
                    return FeedFetchResult(
                        source_feed_url_original=source_feed_url_original,
                        source_feed_url_resolved=response.geturl(),
                        fetched_at_utc=fetched_at_utc,
                        http_status=response.status,
                        content=content,
                        etag=response.headers.get("ETag"),
                        last_modified=response.headers.get("Last-Modified"),
                        not_modified=False,
                        error=None,
                    )
            except urlerror.HTTPError as exc:
                if exc.code == 304:
                    return FeedFetchResult(
                        source_feed_url_original=source_feed_url_original,
                        source_feed_url_resolved=candidate_url,
                        fetched_at_utc=fetched_at_utc,
                        http_status=304,
                        content=None,
                        etag=etag,
                        last_modified=last_modified,
                        not_modified=True,
                        error=None,
                    )
                last_exc = exc
                if _is_transient_error(exc) and attempts <= retries:
                    time.sleep((2 ** (attempts - 1)) + random.random() * 0.2)
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if _is_transient_error(exc) and attempts <= retries:
                    time.sleep((2 ** (attempts - 1)) + random.random() * 0.2)
                    continue
                break

        if last_exc and _is_transient_error(last_exc):
            continue
        break

    return FeedFetchResult(
        source_feed_url_original=source_feed_url_original,
        source_feed_url_resolved=None,
        fetched_at_utc=to_rfc3339(utc_now()),
        http_status=None,
        content=None,
        etag=etag,
        last_modified=last_modified,
        not_modified=False,
        error=str(last_exc) if last_exc else "Unknown fetch error",
    )


def load_json_map(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        print(f"warning: invalid JSON in {path}; using empty state.")
        return {}


def atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(stable_json_dumps(payload) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def run_ingest(
    feeds_path: Path,
    state_dir: Path,
    concurrency: int = 8,
    timeout_seconds: float = 15.0,
    retries: int = 2,
    user_agent: str = "wikipedia-notability-finder/1.0 (+rss-ingest)",
    fetcher: Callable[..., FeedFetchResult] = fetch_feed,
) -> int:
    state_dir.mkdir(parents=True, exist_ok=True)
    events_path = state_dir / "events.jsonl"
    seen_path = state_dir / "feed_seen.json"
    fetch_state_path = state_dir / "feed_fetch_state.json"

    feed_urls = parse_feeds_markdown(feeds_path)
    seen_map = load_json_map(seen_path)
    fetch_state = load_json_map(fetch_state_path)

    stats = {
        "feeds_total": len(feed_urls),
        "feeds_succeeded": 0,
        "feeds_failed": 0,
        "feeds_not_modified": 0,
        "entries_parsed": 0,
        "entries_invalid": 0,
        "new_events_appended": 0,
        "duplicates_skipped": 0,
        "date_parse_errors": 0,
    }

    fetch_results: list[FeedFetchResult] = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = {
            executor.submit(
                fetcher,
                feed_url,
                fetch_state.get(feed_url, {}),
                timeout_seconds,
                retries,
                user_agent,
            ): feed_url
            for feed_url in feed_urls
        }
        for future in as_completed(futures):
            fetch_results.append(future.result())

    fetch_results.sort(key=lambda r: r.source_feed_url_original)

    pending_lines: list[str] = []
    seen_keys_this_run: set[str] = set()

    for result in fetch_results:
        fetch_state[result.source_feed_url_original] = {
            "etag": result.etag,
            "last_modified": result.last_modified,
            "resolved_url": result.source_feed_url_resolved,
            "last_status": result.http_status,
            "last_fetch_at_utc": result.fetched_at_utc,
            "last_error": result.error,
        }

        if result.error:
            stats["feeds_failed"] += 1
            continue
        if result.not_modified:
            stats["feeds_not_modified"] += 1
            stats["feeds_succeeded"] += 1
            continue
        if not result.content:
            stats["feeds_failed"] += 1
            continue

        try:
            source_feed_title, parsed_entries = parse_feed_bytes(result.content)
        except ET.ParseError:
            stats["feeds_failed"] += 1
            continue

        stats["feeds_succeeded"] += 1
        parsed_entries.sort(key=lambda e: (e.url_raw, e.title))
        stats["entries_parsed"] += len(parsed_entries)
        ingested_at = to_rfc3339(utc_now())

        for entry in parsed_entries:
            entry_url_canonical = normalize_url(entry.url_raw)
            if not entry_url_canonical:
                stats["entries_invalid"] += 1
                continue

            key = canonical_key(entry_url_canonical)
            if key in seen_map or key in seen_keys_this_run:
                stats["duplicates_skipped"] += 1
                continue

            published_at_utc, date_parse_error = parse_datetime_to_rfc3339(
                entry.published_raw
            )
            if date_parse_error:
                stats["date_parse_errors"] += 1

            event_id = event_id_for(
                entry_url_canonical=entry_url_canonical,
                entry_guid=entry.guid,
                source_feed_url_original=result.source_feed_url_original,
                published_at_utc=published_at_utc,
            )
            event = {
                "event_id": event_id,
                "event_type": "feed_entry_discovered",
                "ingested_at_utc": ingested_at,
                "source_feed_url_original": result.source_feed_url_original,
                "source_feed_url_resolved": result.source_feed_url_resolved,
                "source_feed_title": source_feed_title,
                "entry_guid": entry.guid,
                "entry_title": entry.title,
                "entry_url_raw": entry.url_raw,
                "entry_url_canonical": entry_url_canonical,
                "published_at_utc": published_at_utc,
                "fetched_at_utc": result.fetched_at_utc,
                "summary": entry.summary,
                "author": entry.author,
                "date_parse_error": date_parse_error,
            }
            pending_lines.append(stable_json_dumps(event))

            seen_map[key] = {
                "entry_url_canonical": entry_url_canonical,
                "event_id": event_id,
                "ingested_at_utc": ingested_at,
                "source_feed_url_original": result.source_feed_url_original,
            }
            seen_keys_this_run.add(key)
            stats["new_events_appended"] += 1

    if pending_lines:
        events_path.parent.mkdir(parents=True, exist_ok=True)
        with events_path.open("a", encoding="utf-8") as fh:
            for line in pending_lines:
                fh.write(line + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    atomic_write_json(seen_path, seen_map)
    atomic_write_json(fetch_state_path, fetch_state)

    print("RSS ingest summary")
    for key in (
        "feeds_total",
        "feeds_succeeded",
        "feeds_failed",
        "feeds_not_modified",
        "entries_parsed",
        "entries_invalid",
        "new_events_appended",
        "duplicates_skipped",
        "date_parse_errors",
    ):
        print(f"{key}: {stats[key]}")

    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RSS ingest for notability pipeline")
    parser.add_argument("--feeds", required=True, type=Path, help="Path to config/feeds.md")
    parser.add_argument("--state-dir", required=True, type=Path, help="State directory")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument(
        "--user-agent",
        default="wikipedia-notability-finder/1.0 (+rss-ingest)",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    return run_ingest(
        feeds_path=args.feeds,
        state_dir=args.state_dir,
        concurrency=args.concurrency,
        timeout_seconds=args.timeout_seconds,
        retries=args.retries,
        user_agent=args.user_agent,
    )


if __name__ == "__main__":
    raise SystemExit(main())
