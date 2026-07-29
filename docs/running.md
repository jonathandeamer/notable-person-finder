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

`config validate` and `run` require every named secret environment variable to
be present and nonblank, including both `[secrets].openrouter_api_key` (the
example uses `OPENROUTER_API_KEY`) and `[secrets].brave_api_key` (example
`BRAVE_API_KEY`). Detection uses the OpenRouter key; Brave is reserved for
later search work but is still a required secret under the current loader.

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

## Model configuration

Detection is controlled by the example's OpenRouter and task blocks:

- `[openrouter]` — endpoint and routing (fallbacks may change the *serving*
  provider, never the configured model id).
- `[tasks.detect_people]` — model id, input and completion token ceilings,
  `max_people`, title/summary character limits, and generation parameters
  (`temperature`, `top_p`, `reasoning_effort`).
- `[budget].openrouter_usd_per_run` — optional hard per-run OpenRouter cap as a
  decimal USD string. Omit the key for no cap. Under a hard cap, each
  generation reserves on the application thread from usable current pricing
  and the configured token ceilings; when the remaining budget cannot cover
  that reservation, the work is deferred for a later ordinary run rather than
  making a partial paid call.
- `[concurrency].http_workers` and `llm_workers` — independent bounded pools.
  Feed HTTP and OpenRouter LLM work may overlap; workers never touch SQLite.
- `[transport].llm_read_timeout_seconds` — long read timeout for model calls
  (distinct from ordinary HTTP read timeout).

Every generation is preceded by a fresh exact-model inspection for the
configured model. Permanent preflight failure (for example authentication or
unsupported structured-output capability) settles dependents without a paid
generation. Transient preflight failure leaves dependents active for a later
run. Detection is never claimed before current-run readiness.

## Run feed ingestion and person detection

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

For untriaged usable source items (new in this run or still untriaged from
earlier runs), the same command:

- schedules detection after ingestion (and backfills the untriaged corpus on
  later runs);
- records `insufficient_input` at schedule time when both title and summary
  are empty or whitespace-only, with no work item, attempt, reservation, or
  paid call;
- runs model inspection then structured generation under the hard budget;
- writes one triage observation per source item and zero or more unresolved
  person mentions that remain independently traceable; and
- isolates sibling failures: one item's permanent failure or deferral does not
  prevent other items from persisting, and the digest remains truthful about
  the partial outcome.

Unresolved mentions intentionally have **no durable person**. Entity
resolution, Wikipedia work, coverage research, ranking, and synthesis are not
part of this milestone.

The command writes an immutable dated Markdown digest and, when enabled,
refreshes `latest.md`; it prints the same Markdown to standard output. Exit 0
means `complete`, 2 means `partial`, 1 means `failed`, and 130 means
`interrupted`. A hard budget that defers remaining model work is a normal
`partial` outcome.

Use the status command for the latest stored run and corpus totals:

```bash
uv run notable --config /path/to/config/notable.toml status
```

Status includes durable triage counters (triaged, untriaged, research,
uncertain, do not research, insufficient input, failed triage, and unresolved
research or uncertain mentions). It still does not explain deferral reasons or
budget figures; read the latest digest for those.

External execution remains at-least-once. See
`docs/architecture/at-least-once-execution.md` for the crash windows in which a
paid provider call can be repeated.

## Reading the digest fields

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

The `### Person detection` section is a per-run triage summary:

- `Source items triaged` — observations settled for this run.
- `Research` / `Uncertain` / `Do not research` — item-level outcomes.
- `Unresolved research or uncertain mentions` — actionable mentions that still
  have no durable person (expected until 3b2).
- `Overflow observations` — items that hit the configured people ceiling.
- `Insufficient input` — empty title and summary settled without a model call.
- `Model work deferred` / `Model work permanently failed` — inspect and detect
  work settled by this run.
- `OpenRouter cost` — configured cap (or none), reserved, and spent amounts.

`Required work deferred` and its indented reason rows describe outstanding
required work across the whole queue. Budget fields report the configured cap
and the run's reserved and spent amounts. The shortlist remains a placeholder
until lead assessment.

## Verification and live smokes

The default completion suite is offline. Pytest's configured `addopts` excludes
the `live` marker automatically:

```bash
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people
```

Run real-network smokes explicitly:

```bash
# Feeds (public HTTP only)
uv run pytest tests/ingestion -m live -v

# OpenRouter (requires a real key in the process environment)
OPENROUTER_API_KEY=… uv run pytest tests/people -m live -v
```

The OpenRouter live smoke uses the application defaults for model and routing
and never records the API key. Offline environments skip when the key is
absent; an operator must still run it before cutover when a key is available
and record model, serving provider when exposed, usage, cost, and outcome
without secrets.
