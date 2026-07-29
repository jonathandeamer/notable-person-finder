# Running the Rewrite

The supported interface is the installed `notable` command. Run commands from
a real checkout with dependencies synchronized:

```bash
uv sync --frozen
```

## Configure the application

Copy the tracked examples into an operator-owned configuration directory and
remove or disable feeds you do not want:

```bash
cp config/notable.example.toml /path/to/config/notable.toml
cp config/discovery-feeds.example.toml /path/to/config/discovery-feeds.toml
mkdir -p /path/to/config/discovery_profiles
cp config/discovery_profiles/art.example.toml \
  /path/to/config/discovery_profiles/art.toml
```

Paths in `notable.toml` are resolved relative to that file. Set `[paths].root`
for a portable data, log, and cache tree; omit it to use the platform's normal
application directories.

The `[secrets]` values are environment-variable names, not credentials. Export
the named variables before `config validate` or `run`. An adjacent `.env` may
fill missing values, but the process environment wins. Secret values are never
stored in configuration snapshots, the database, digests, or logs.

Use `--config` before the subcommand when the file is not at the platform
default location:

```bash
uv run notable --config /path/to/config/notable.toml config validate
uv run notable --config /path/to/config/notable.toml paths
```

`paths` prints the resolved database, digest, log, backup, cache, and lock
locations. Migrations normally run automatically, but can be applied alone:

```bash
uv run notable --config /path/to/config/notable.toml db migrate
```

## Run feed ingestion

```bash
uv run notable --config /path/to/config/notable.toml run
```

For every enabled feed, a run:

- preserves identity by configured feed `key`, even if its label or URL moves;
- sends stored ETag and Last-Modified validators when available;
- parses response bytes as RSS or Atom without letting feedparser fetch URLs;
- retains parser warnings alongside recovered entries;
- canonicalizes usable article URLs, strips only the approved tracking parts,
  records observed URL aliases, and deduplicates source items insert-once;
- stores entries with missing or unusable URLs and bad dates with typed issues;
  and
- isolates each feed's settlement, so one failed feed does not roll back a
  sibling feed's items.

The ingestion milestone does not detect or resolve people. Its source items
carry no triage observation; that begins in milestone 3b. The application does
not draft, edit, or publish Wikipedia content.

The command writes an immutable dated Markdown digest and, when enabled,
refreshes `latest.md`; it prints the same Markdown to standard output. Exit 0
means `complete`, 2 means `partial`, 1 means `failed`, and 130 means
`interrupted`.

Use the status command for the latest stored run and corpus totals:

```bash
uv run notable --config /path/to/config/notable.toml status
```

## Reading the ingestion digest fields

The `### Ingestion` section is a per-run summary:

- `Feeds fetched` counts feeds whose final stored settlement for this run was
  a modified feed.
- `Feeds not modified` counts final 304 settlements.
- `Feeds failed` counts final failed or not-inspected settlements.
- `Source items created` is the number first inserted by this run, after
  insert-once deduplication across feeds and earlier runs.
- `Articles created` is the number of canonical article identities first
  associated with source items created by this run.

The feed counts are deliberately final-per-feed, not raw HTTP calls or
`feed_fetch` rows. A feed can have duplicate work in one run after a URL change;
only its highest-id settlement controls these three digest counters. Attempt
history and all fetch rows remain in SQLite for diagnosis.

`Required work deferred` and its indented reason rows describe outstanding
required work. Budget fields report the configured cap and the run's reserved
and spent amounts. The shortlist remains a placeholder until lead assessment.

## Verification and the live smoke

The default completion suite is offline. Pytest's configured `addopts` excludes
the `live` marker automatically:

```bash
uv run pytest tests/foundation tests/run_engine tests/ingestion
```

Run the real-network feed smoke explicitly:

```bash
uv run pytest tests/ingestion -m live -v
```

The live command uses selected feeds from
`config/discovery-feeds.example.toml` to exercise conditional requests,
redirects, the response-size bound, and URL identity. It skips only when the
environment cannot establish the required connection; TLS, protocol,
read-timeout, parsing, and provider-contract failures remain failures.
