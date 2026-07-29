# Troubleshooting the Rewrite

Start by locating the active files and checking the latest run:

```bash
uv run notable --config /path/to/config/notable.toml paths
uv run notable --config /path/to/config/notable.toml status
```

The dated digest is the best operator summary. The SQLite database and
`notable.jsonl` retain lower-level evidence; inspect them read-only and do not
repair state by hand.

## Configuration is invalid

Run `notable config validate`. Configuration is strict: unknown keys, invalid
bounds, duplicate feed keys, non-public feed URLs, missing referenced TOML
files, and missing named secret variables all fail before a run is created.

The values under `[secrets]` must be uppercase environment-variable names. Put
the actual values in the process environment or in an adjacent `.env`; a
nonblank process value takes precedence.

## Another mutating command holds the lock

Only one mutating command may use a data root at a time. Wait for the active
`notable run` or `notable db migrate` process to finish. The lock file's text is
diagnostic only; deleting or editing it does not determine lock ownership.

## A feed remains `deferred`

Read the latest digest's `Required work deferred` reason breakdown. Common feed
reasons include an exhausted transient provider failure, a paused provider,
`not inspected: response too large`, and `not inspected: unsupported content`.
`notable status` currently prints only the aggregate deferred count, not this
breakdown.

A terminal deferred item is eligible for a later ordinary run, but not again in
the run that deferred it. Check that the configured feed is still enabled and
that a later run has actually occurred. Raising response limits or changing
retry bounds is an operator policy change: validate why the existing safety
bound was hit before changing it.

## A feed reports `malformed_response`

The adapter uses `malformed_response` when bytes are not recognizable RSS or
Atom, have no trustworthy feed structure, or defeat feedparser. HTML error
pages, empty bodies, JSON Feed, and a wrongly configured non-feed URL can all
produce it. The stored detail is sanitized and never includes the response
body.

Malformed responses get at most one fresh attempt. The exact settlement is
easy to misread:

- with `retry.max_attempts = 1`, the first malformed response exhausts and the
  work is `deferred`;
- with any value of 2 or more, including the shipped default of 3, a second
  malformed response is foreclosed as `failed_permanent` after two calls.

That permanent result ends only that work item. It does not blacklist the feed:
the next ordinary run seeds fresh work for the same enabled feed. Check the
configured URL in `feed_identity.current_url`, then compare the corresponding
`feed_fetch` and `attempt` rows to distinguish a persistent non-feed endpoint
from a temporary publisher error page.

## Evidence for failed and not-inspected fetches

For an ordinary provider failure, the final failed attempt records
`attempt.outcome='failed'`, its `failure_category`, and sanitized `detail_json`.
The handler also writes one `feed_fetch.outcome='failed'` row for the final
settlement, so feed history records that the run tried and failed.

`response_too_large` and `unsupported_content` are the special not-inspected
path. The handler returns a direct deferred outcome rather than retrying a
response it deliberately refused to parse. Because the attempt schema permits
a failure category only on a failed attempt, its attempt row has
`outcome='succeeded'` and a null `failure_category`; the sanitized category and
detail are in `attempt.detail_json`. The corresponding `feed_fetch` row is
`outcome='failed'` with the real category. Consequently:

- the digest's deferral reason and `Feeds failed` count include it; but
- attempt-based `Operational failures` and `Failures by category` omit it.

If a buggy handler returns a non-settling state after a real call, the engine
settles the work item `failed_permanent` and discards the untrusted payload.
That narrower known gap leaves the `attempt` row as the only durable call
evidence and writes no handler-owned `feed_fetch` row.

## Understanding `url_issue`

An entry is still stored when its link cannot become an article identity:

- `missing` — no link, or an empty link;
- `not_http` — a scheme other than HTTP or HTTPS;
- `unsafe` — embedded username or password credentials; and
- `unusable` — another unusable shape, normally a missing host, or an isolated
  entry-level normalization fault.

`original_url` retains the publisher's value when one existed.
`canonical_article_id` remains null. These are recorded source items, not
silently dropped entries.

## Understanding `published_issue`

The raw publisher value remains in `published_raw` when date parsing cannot
produce a usable UTC timestamp:

- `missing` — absent, empty, or whitespace-only publication text;
- `unparseable` — neither an RFC 822 nor ISO-8601 date; and
- `implausible` — before 1900 or more than one day after the settlement time.

When an issue is present, `published_at` is null. The entry is still ingested.
The feed adapter also keeps Atom's raw `updated` value distinct from
`published`; it never silently substitutes one date claim for the other.

## Status shows no successful fetch for a feed

A dash under `latest successful fetch` means that feed identity has no stored
`modified` or `not_modified` fetch. Failed and not-inspected rows are retained
as evidence but intentionally do not become conditional-request validators or
a successful-fetch timestamp.
