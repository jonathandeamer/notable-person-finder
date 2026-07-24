# Operator Experience and Verification

**Status:** Approved
**Date:** 2026-07-24
**Branch:** `refactor/rearchitecture`

## Purpose

This specification defines how one editor configures, runs, observes,
diagnoses, and verifies the once-daily Notable Person Finder batch. It turns
the workflow, persistence, model, and provider designs into a small operational
surface without adding a daemon, scheduler, machine-output API, or second
source of truth.

The product remains a human-oriented local tool. SQLite is the structured
source of truth, the Markdown digest is the daily review artifact, and the
installed `notable` command is the only supported application entry point.

## Configuration Files and Precedence

Use one required main TOML file, `notable.toml`. By default it lives in the
OS-standard per-user configuration directory resolved with `platformdirs`.
The global `--config PATH` option selects a different main file. The
application never searches the current working directory or merges several
candidate main files.

The main file explicitly references auxiliary TOML files for configured feeds
and domain profiles. Relative references resolve from the directory containing
the main file. Auxiliary files own distinct typed sections; they do not
override arbitrary values from the main file. This makes the resolved
configuration a directed, unambiguous file graph rather than a general include
or inheritance system.

Precedence is deliberately narrow:

1. application defaults supply safe operational defaults;
2. the selected TOML graph supplies all non-secret product and provider
   settings; and
3. process environment variables supply secret values.

An ignored `.env` beside the selected main file may fill secret variables that
are absent from the process environment. It never replaces an existing
process value. TOML may configure the environment-variable names to read but
never contains secret values. No environment variable overrides a model,
budget, threshold, feed, concurrency limit, digest policy, endpoint, or other
non-secret setting.

The CLI has no per-invocation workflow-setting overrides. `--config` selects
the configuration graph; it does not participate in a layered merge.
`--verbose` affects safe terminal detail only.

## Validation and Snapshots

Pydantic boundary models reject unknown fields and validate the complete
configuration graph before workflow work is claimed. Validation covers:

- required files, unique stable keys, references, and compatible sections;
- safe HTTP or HTTPS endpoint rules and absolute feed URLs;
- IANA timezone names;
- positive, finite, and internally compatible bounds, timeouts, retry counts,
  pacing intervals, concurrency limits, and digest limits;
- decimal USD budget syntax and values;
- task-to-model assignments, parameters, token bounds, and structured-output
  compatibility requirements;
- required user-agent and contact details; and
- the presence, but never the value, of required secrets.

Report independent validation failures together when practical. Each error
identifies its file, dotted field path, rejected safe value when useful, and a
specific correction. Never begin a provider request after local validation
fails.

`notable config validate` performs local parsing, schema, cross-field, path,
URL, and secret-presence validation without making a network request or paid
call. `notable run` performs the provider and configured-model preflight
defined by the provider design after local validation and before paid LLM
generation.

Each created run references the canonical, fully resolved, redacted
configuration snapshot and its SHA-256 fingerprint. The snapshot records
secret availability but not values. Logs and terminal diagnostics apply the
same redaction policy.

## CLI Surface

The initial command surface is:

```text
notable run
notable config validate
notable status
notable paths
notable digest show [RUN_ID]
notable audit run RUN_ID [--attempt ATTEMPT_ID]
notable audit person PERSON_ID
notable db migrate
```

`notable run` validates configuration, acquires the mutation lock, migrates if
needed, creates a run, continues eligible durable work, renders the digest, and
records a terminal state. Every successful startup creates a new run,
including a manual invocation on the same local date. Matching persisted work
is reused through the approved fingerprints. There is no `--force` or special
retry path.

`notable status` shows the latest run state and time, latest digest path,
pending and deferred required work, operational-failure count, digest backlog,
and oldest pending candidate. It performs no work. `notable paths` prints the
resolved configuration, database, backup, digest, log, and cache locations.

`notable digest show` writes the exact persisted Markdown for the requested
run to standard output, defaulting to the latest attempt. It never reruns
selection or synthesis. `notable audit run` shows configuration provenance,
transitions, work outcomes, attempts, failures, budget and cost, and reporting
result for one run. Its optional `--attempt` drill-down is restricted to an
attempt belonging to that run and shows the exact persisted provider or model
request evidence, raw response, validated result, retry history, usage, and
version provenance. `notable audit person` shows sourced names, possible or
confirmed relations, mentions, Wikipedia observations, coverage evidence,
article assessments, lead history, and digest history for one person. Audit
views use stable IDs and apply credential and authorization redaction even
when intentionally showing detailed persisted evidence.

`notable db migrate` acquires the same mutation lock as a run, creates the
approved pre-migration backup, and applies pending checked migrations. Normal
runs already do this automatically.

The CLI has no general JSON output, export contract, stage command, resume
command, retry command, overwrite flag, model flag, budget flag, or threshold
flag. A future integration must receive an intentional export or adapter
rather than depend on terminal formatting or query private tables directly.

## Standard Streams and Exit Behavior

Commands use standard streams consistently:

- successful data or document output goes to standard output;
- concise progress, warnings, and actionable failures go to standard error;
- `notable run` and `notable digest show` emit the persisted Markdown digest
  on standard output; and
- structured detail goes to application logs rather than flooding either
  stream.

Stable process statuses are:

| Status | Meaning |
| --- | --- |
| `0` | The command succeeded; for `run`, the run is `complete`. |
| `1` | The command or run failed, including validation, migration, storage, preflight, reporting, and lock failures. |
| `2` | The run is `partial` but produced a usable digest. |
| `64` | CLI syntax or argument usage is invalid. |
| `130` | The process was interrupted with `SIGINT`. |

Normal platform signal semantics are preserved for other termination signals;
for example, `SIGTERM` conventionally produces `143`. The application records
an already-created run as interrupted when shutdown permits, then does not
misreport it as complete or partial. Read-only commands return `1` when a
requested run or person does not exist.

## Daily Execution and Locking

Once daily is an operating cadence, not an application prohibition. The
application documents minimal `launchd`, `systemd` timer, and cron examples,
including how to select a configuration file and inspect the resulting exit
status and log path. It does not install, edit, inspect, or remove scheduler
configuration, run as a daemon, or send notifications.

A nonblocking operating-system advisory file lock beside the resolved database
protects every mutating `notable run` and `notable db migrate` invocation for
that data root. The OS lock is authoritative and is automatically released
when the process exits or crashes. The lock file may contain safe diagnostic
metadata such as PID and start time, but that metadata is never used as a
stale-lock sentinel.

Acquire the lock before migration or run creation. Contention fails
immediately with status `1`, identifies the competing PID when safely
available, suggests checking the process and resolved data path, and creates no
run row. Read-only `status`, `paths`, `digest`, and `audit` commands do not
acquire the mutation lock. SQLite WAL and short read transactions allow them
to inspect committed state during a batch.

There is no scheduler retry loop or application-level same-day guard. After an
operator corrects a configuration or credential problem, another ordinary
`notable run` is the recovery action.

## Logs and Run Summaries

Write rotating JSON Lines application logs under the OS-standard log directory.
Rotation size and retained-file count are configuration values with
conservative defaults. Every event has a UTC timestamp, severity, stable event
name, and safe structured fields. When applicable those fields include run,
work, person, and attempt IDs; provider operation and destination host;
duration; retry ordinal; byte or token counts; and typed outcome.

Routine logs never contain authorization headers, credentials, known secret
values, raw prompts or model responses, article bodies, complete search
queries, complete provider payloads, or full evidence records. Full product
URLs, queries, model context, outputs, and provider provenance remain in their
approved SQLite records. `--verbose` increases safe console progress for the
current invocation but does not change persisted workflow behavior or weaken
redaction.

Every usable run has a compact summary containing:

- run identifier and state, start and finish, duration, timezone, and exact
  observation window;
- feed items and people considered and matching Wikipedia pages found;
- promising, possible, insufficient, and incomplete assessment counts;
- required work succeeded, deferred by budget, exhausted transiently, and
  permanently failed;
- provider pauses and operational failures by safe category;
- configured OpenRouter budget, reserved amount, and actual provider cost when
  available;
- candidates newly queued, emitted, omitted by the digest limit, removed after
  a Wikipedia match, and still queued by tier; and
- rolling queue arrival, emission, saturation, age, and clearance indicators
  defined by the digest design.

The terminal shows only the detail needed to understand the run. Full attempt
and decision provenance belongs in the audit commands.

## Digest Presentation and Safety

Each complete or partial run atomically writes one immutable Markdown digest
under the configured digest root using its local date and run identifier, for
example `2026-07-24-run-42.md`. Whenever durable state permits, a failed run
also writes a digest that prominently says it failed. Reporting persists the
file identity and SHA-256 content hash.

An enabled-by-default `latest.md` convenience copy is atomically replaced with
the latest attempt, regardless of state. It is a regular copy rather than a
required symbolic link so behavior is portable. It can never retain an older
successful digest while a newer attempt is partial or failed. Standard output
is byte-for-byte the persisted digest, including one normalized trailing
newline.

The Markdown order is:

1. a header with run state, identifier, observation window, timezone, and any
   prominent partial, failure, budget, or backlog warning;
2. the ranked shortlist entries defined by the digest design; and
3. the compact operational and queue-flow summary.

An empty shortlist is explicitly successful when the run is otherwise
complete. Completed candidate results remain visible in a partial run, while
unfinished work and its consequences are stated. Model synthesis remains
optional; its absence produces the approved deterministic candidate fallback.
If the application cannot atomically persist a trustworthy required digest or
record its reporting result, the run is failed rather than claimed partial or
complete.

## Actionable Error Reporting

Every expected operator-facing error answers four questions:

1. What failed?
2. What consequence did it have for this command or run?
3. What concrete action should the operator take next?
4. Which safe run, work, field, environment-variable name, or path identifies
   the problem?

Configuration errors identify file and dotted field. Missing credentials name
the required environment variable without showing its value. Provider errors
state the provider, operation, safe category, retry disposition, and whether
work remains pending. Migration errors state that no provider work began and
name the backup path. Partial-run messages point to
`notable audit run RUN_ID`. Lock errors identify the competing invocation when
possible. Storage errors never suggest discarding or silently replacing the
database.

Expected problems do not print Python tracebacks. Unexpected internal failures
receive a correlation identifier; standard error reports that identifier and
the log location, while a sanitized traceback is written to the structured
log. Exception formatting passes through the same secret redaction as all
other observability paths.

## Pytest Layers

The default pytest suite is completely offline and deterministic. It contains:

- focused unit tests for configuration, policy, scheduling, rendering,
  redaction, error mapping, and other pure behavior;
- integration tests against temporary real SQLite databases for constraints,
  migrations, transactions, work continuation, audit queries, and reporting;
- provider contract tests using local fakes and minimal checked-in boundary
  fixtures;
- CLI subprocess tests for streams, statuses, signals, path resolution,
  locking, and migration behavior;
- stable-clock Markdown snapshots for complete, empty, partial, failed,
  synthesis-fallback, and backlog-warning digests; and
- a small offline end-to-end workflow that crosses every major application
  boundary and proves crash continuation without repeating a persisted call.

Tests never use the operator's configuration, database, cache, logs, digests,
or credentials. Temporary configuration and storage roots are mandatory.
Network access in the default suite is denied or fails the test so an
accidental live request cannot pass unnoticed. Provider fixtures contain only
the minimum contract surface needed by the case and are scrubbed of secrets.

Prompt construction, generated schema, identifier checks, workflow
transitions, fallback rendering, and deterministic policy remain pytest
responsibilities. Promptfoo owns semantic prompt and model evaluation and is
not invoked by pytest, pre-commit, or ordinary CI.

## Manual Live Tests

Tests marked `live` are explicitly invoked with `pytest -m live` and are never
part of ordinary CI. Narrow markers or test selection may run one provider at
a time. The layer exercises:

- one configured feed fetch and parse;
- a bounded MediaWiki search and page retrieval;
- one minimal Brave search;
- one small permitted article retrieval and extraction; and
- one tiny OpenRouter request with strict structured output.

Every live test uses isolated temporary application state, conservative
bounds, an identifiable user agent, and the same production adapter contract.
A missing credential skips only the dependent test with a clear reason. If a
credential is supplied but invalid, the test fails. Provider denial, schema
drift, or an incompatible configured model also fails rather than being
reported as an environmental skip.

Live tests are smoke checks for integration drift, not reliability benchmarks,
load tests, production-health monitors, or substitutes for Promptfoo. The
scheduled production batch is never treated as a test run.

## Lightweight Legacy Comparison

Before prototype removal, freeze approximately twenty representative legacy
discovery inputs, their available evidence packets, and their recorded legacy
shortlist outcomes. Choose cases across meaningful-person detection,
namesakes, existing Wikipedia biographies, strong and weak coverage,
inaccessible articles, and empty or failed paths. Scrub or replace any
sensitive and unstable material needed to make review safe and repeatable.

After the rewrite is operationally complete, run equivalent local inputs
through it and manually classify each material outcome difference as:

- expected improvement;
- acceptable approved policy change;
- regression; or
- inconclusive because the evidence or model result cannot support a fair
  comparison.

This is a qualitative release gate, not a percentage-equivalence test. Every
lost high-value person, unsafe entity merge, and unsafe Wikipedia suppression
must be explained and resolved or explicitly accepted before release. New
candidates are reviewed for obvious relevance regressions. A short dated
report records inputs, configuration and prompt versions, differences,
dispositions, and reviewer conclusion.

The comparison runs once. Its frozen inputs may seed focused pytest or
Promptfoo regression cases when they reveal a genuine requirement, but the
dual-workflow harness and aggregate comparison are then retired. The rewrite
does not carry a permanent compatibility suite or matching-score target.

## Operational Acceptance Criteria

The operator and verification design is satisfied when:

- the same selected files, defaults, and secret-presence state resolve to the
  same canonical redacted configuration fingerprint;
- invalid local configuration reports all practical independent errors and
  makes no provider request;
- environment secrets outrank adjacent `.env` values and no non-secret
  environment override exists;
- a second same-day invocation is allowed, while an overlapping mutating
  invocation fails immediately without creating a run;
- a killed process cannot leave a stale lock requiring manual deletion;
- read-only status and audit commands can inspect committed state during a
  run;
- complete, partial, failed, interrupted, usage, and lock outcomes return the
  documented process status and agree with durable state where a run exists;
- a complete empty run emits an unambiguously successful digest;
- partial and failed digests expose completed results without hiding skipped
  or failed work;
- `latest.md` always represents the latest attempt and immutable dated digests
  never change;
- digest stdout is identical to the persisted artifact;
- missing optional synthesis cannot remove a selected candidate;
- routine and verbose output, logs, snapshots, errors, and digests reveal no
  configured secret or authorization data;
- every shortlist claim and operational failure is traceable through the
  defined audit commands;
- every expected failure category gives a concrete safe recovery action;
- all core workflow, persistence, CLI, locking, digest, and adapter-contract
  verification runs offline under default pytest;
- live tests are opt-in, isolated from operator state, bounded, and fail on
  supplied invalid credentials;
- the one-time legacy comparison resolves all high-value losses and unsafe
  identity or Wikipedia differences without becoming a compatibility target;
  and
- a documented external scheduler can run `notable run` unattended and leave
  enough local evidence for diagnosis the next day.

## Deferred

- scheduler installation or management;
- a daemon, service API, web dashboard, or notification delivery;
- general JSON output or a stable database-query API;
- per-run workflow-setting overrides;
- automatic live tests or production probes in CI;
- centralized log shipping, metrics infrastructure, or alerting;
- automatic database repair, scheduled backups, or retention jobs; and
- a permanent dual-run legacy compatibility harness.
