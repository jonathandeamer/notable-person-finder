# Legacy Behavior Review: Discovery and Feed Ingestion

**Status:** Approved
**Date:** 2026-07-24

## Scope

This capability discovers article metadata from editor-configured web feeds and
stores each new article once. It does not decide whether an article concerns a
person and does not retrieve linked article bodies.

Primary legacy evidence:

- `ingest/rss_ingest.py`
- `tests/test_rss_ingest.py`
- `scripts/backfill_feed_priorities.py`
- feed-priority tests in `tests/test_prefilter_gate0.py`
- `config/feeds.md`
- `docs/running.md`

## Approved Dispositions

| Observable behavior | Legacy evidence | Decision and rationale | Replacement verification |
| --- | --- | --- | --- |
| Sources are supplied through configuration rather than code. | Markdown feed parser and `config/feeds.md` | **Preserve.** The ten arts sources are a pilot profile, not application logic. Any standards-based RSS or Atom feed can be configured without publisher-specific code. | Pytest configuration tests; explicit network integration checks for the pilot profile |
| RSS 2.0 and Atom entries are parsed. | `test_parse_feed_bytes_rss_and_atom` | **Preserve.** Supporting both common feed standards is low-cost when using a maintained parser. This is not a promise to repair every malformed publisher dialect. | Offline RSS and Atom fixtures |
| Discovery uses title, link, summary, author, publication date, GUID, and source metadata when present. | `ParsedEntry` and event construction in `rss_ingest.py` | **Preserve.** These fields supply discovery context and provenance. | Parser and persistence tests with missing and present optional fields |
| Feed-supplied text is sent downstream largely as received. | Event construction and Gate 1 mapping | **Change.** Retain original title and summary for provenance, but derive normalized plain text for deterministic processing and model input. | Unit tests for markup removal and preservation of original values |
| Entries without a link are dropped; missing titles become the string `"(untitled)"`. | `parse_feed_bytes` | **Change.** Require a valid article URL and usable text in either title or summary. Store a missing title as null. Skip entries with neither title nor summary and record the reason. | Offline fixtures for each incomplete-entry case |
| Each canonical URL is ingested once across runs. | `test_run_ingest_idempotent_and_cross_feed_dedupe`; `feed_seen.json` | **Change.** Enforce one source item per conservatively normalized URL in SQLite. Ignore later appearances. Do not build last-seen, revision, or reevaluation-on-change behavior without evidence that it is needed. | Repeated-run and uniqueness tests |
| The same literal article URL from two feeds is stored once. | Cross-feed portion of `test_run_ingest_idempotent_and_cross_feed_dedupe` | **Preserve as a uniqueness consequence, not a subsystem.** The pilot uses separate publishers and does not expect cross-feed duplicates, so it needs no discovery-observation model. | Canonical URL uniqueness test |
| URLs are normalized by scheme, host, port, path, query, and fragment; tracking parameters are removed. | `test_normalize_url_tracking_removed_and_sorted`; `normalize_url` | **Change.** Normalize scheme, host, default ports, fragments, duplicate slashes, and query ordering. Remove only known tracking parameters such as `utm_*`, `fbclid`, and `gclid`; do not strip ambiguous parameters such as `ref` or `src`. | Table-driven normalization tests |
| Missing or invalid publication dates do not discard an entry. | `test_parse_datetime`; `date_parse_error` | **Preserve.** Retain the raw value, store normalized UTC when parsing succeeds, and record an explicit problem otherwise. Ingestion time may order work but must not be presented as publication time. | Date parsing and fallback-order tests |
| Feeds use ETag and Last-Modified conditional requests. | `fetch_feed`; `feed_fetch_state.json` | **Preserve.** Avoid downloading unchanged feeds when the server supports standard HTTP validators. | Adapter tests for conditional headers and HTTP 304 |
| Fetching follows redirects and applies timeouts, bounded retries, a user agent, and configurable concurrency. | `fetch_feed`; `run_ingest` | **Preserve.** These are polite and resilient network-client behaviors. Exact defaults belong in the provider design. | Adapter tests using a fake transport and retry classification |
| An HTTP-configured URL is tried over HTTPS before its configured scheme. | `_candidate_feed_urls` | **Delete.** Request the configured URL and follow ordinary server redirects rather than silently rewriting configuration. | Adapter request test |
| One failed or malformed feed does not stop successful feeds from being ingested. | Per-result error handling in `run_ingest` | **Change.** Preserve isolation, but record failures durably and ensure the run cannot appear fully successful. The overall run status is deferred to workflow design. | Mixed-success integration test and durable run-report assertion |
| Fetch, parse, duplicate, and date-error counts are printed. | `run_ingest` summary | **Change.** Preserve useful counts in the durable run report; terminal presentation belongs to operator design. | Run-summary tests |
| Numeric feed priority controls which entries receive limited model capacity first. | `config/feeds.md`; priority tests; Gate 1 sorting | **Delete.** All enabled pilot sources have equal discovery priority. Durable pending work handles a budget-limited run without discarding lower-priority publishers. | Absence from configuration schema; queue-order tests defined later |
| Feed definitions and priorities are parsed from Markdown and can be backfilled into JSONL events. | `parse_feeds_markdown`; `backfill_feed_priorities.py` | **Delete.** Replace with validated, versioned configuration and SQLite. No legacy-state migration is required. | Configuration validation tests |
| Deterministic hashes are exposed as event IDs and JSONL plus sidecar maps hold ingest state. | `event_id_for`; `events.jsonl`; `feed_seen.json`; `feed_fetch_state.json` | **Delete as implementation behavior.** Stable database identities, constraints, and audit records replace file-derived identifiers. | Persistence and idempotency tests in the domain design |

## Approved Boundaries

- Retain parsed provenance fields rather than complete raw feed documents.
- Discovery does not retrieve linked article bodies. Whether later evidence
  research may retrieve legally accessible text is a separate design decision.

## Verification Boundary

Default tests remain offline and deterministic. RSS, Atom, normalization,
incomplete entries, idempotence, conditional responses, malformed XML, retry
classification, and partial failure are pytest responsibilities. Live checks of
the configured pilot feeds are explicitly invoked integration tests and do not
run in the default suite.
