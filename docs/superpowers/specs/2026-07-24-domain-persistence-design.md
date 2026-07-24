# Domain Model, Persistence, and Continuation

**Status:** Approved
**Date:** 2026-07-24
**Branch:** `refactor/rearchitecture`

## Purpose

This specification defines the application-owned domain records, SQLite
ownership, transaction boundaries, idempotency, migrations, and local storage
layout for the approved
[product workflow](2026-07-24-product-workflow-design.md). It deliberately
avoids legacy JSONL state, a workflow framework, an ORM, and event-sourcing
ceremony.

Provider request and response shapes remain for the provider-contract design.
Exact model input and output schemas remain for the LLM design. The conceptual
records and invariants here constrain both.

## Architecture

Use a Python modular monolith with one SQLite database and one installed CLI,
`notable`. SQLite is the source of truth for runs, discovered material, people,
evidence, observations, attempts, pending work, and digest history.

The database contains two complementary kinds of state:

- first-class domain records describe what was observed and decided; and
- a small operational `work_item` queue describes what bounded task should run
  next.

The work queue never becomes the domain model. A completed job cannot stand in
for a missing identity observation or article assessment, and deleting an
operational queue row cannot delete domain history.

Use standard-library `sqlite3`, explicit feature-owned SQL queries, and
checked-in numbered SQL migrations. Do not introduce SQLAlchemy, Alembic, an
ORM, a generic repository hierarchy, a unit-of-work framework, or an async
database client.

## Package Boundaries

Organize production code by product capability:

```text
src/notable_person_finder/
  cli/
  config/
  db/
  runs/
  ingestion/
  people/
  wikipedia/
  coverage/
  assessment/
  digest/
  providers/
```

Each feature owns its domain types, SQL queries, and application service.
`db` owns connections, transaction helpers, and migrations, but no product
decisions. `providers` implements narrow protocols for feeds and HTTP,
MediaWiki, Brave, article extraction, and OpenRouter without exposing transport
objects to the domain.

Use frozen dataclasses and enums for internal domain values. Use Pydantic at
configuration, provider, and LLM boundaries, where untrusted or external data
must be validated. Avoid a dependency-injection container, repository base
classes, and duplicated ORM/domain models. Dependencies are passed explicitly
through constructors or functions.

The distribution name is `notable-person-finder`, the import package is
`notable_person_finder`, and the executable is `notable`. Initial commands
include `notable run`, `notable paths`, and `notable db migrate`; audit commands
will live under `notable audit ...`.

## Aggregate Map

The initial schema has five understandable areas. The names below are
conceptual and may receive mechanical pluralization or junction-table changes
in DDL, but their ownership and relationships are requirements.

| Area | First-class records |
| --- | --- |
| Operations | configuration snapshots, runs, work items, provider/model attempts |
| Ingestion | feed identities, feed fetches, source items, triage observations, person mentions |
| People and Wikipedia | people, sourced names, person relations, entity-resolution observations, MediaWiki pages/searches/candidates, Wikipedia identity observations |
| Coverage evidence | search plans, queries, ranked result occurrences, canonical articles, article views, person–article assessments, attention signals |
| Assessment and output | lead assessments, digest queue and transitions, digests, digest entries |

There is no generic evidence blob, entity-attribute-value decision table, or
universal event log. Fields used for identity, constraints, state transitions,
selection, ranking, and reporting are relational columns. Canonical JSON text
is limited to immutable external payload detail that is not queried as a
domain fact.

## Operations Records

### Configuration snapshots and runs

Each run references one immutable, canonical, redacted configuration snapshot
and SHA-256 fingerprint. The snapshot contains resolved feeds, source policy,
domain profile, thresholds, model policies, bounds, timezone, and output
settings. It records whether required secrets were available but never their
values. Model attempts additionally record exact prompt and schema hashes.

A run records integer identity, state and state-transition times, local
timezone, observation window, configuration snapshot, counters, reserved and
actual cost, and reporting result. Run states follow capability 7. Run identity
is formatted for humans as `run-{id}` rather than requiring UUID machinery.

### Work items

A work item records:

- task type and application-owned subject reference;
- canonical input fingerprint;
- required or optional class;
- deterministic priority and eligibility time;
- current state and defer or failure reason;
- creating, claiming, and completing run identities; and
- timestamps.

The small state vocabulary is `pending`, `running`, `succeeded`, `deferred`,
`failed_permanent`, and `superseded`. A unique constraint prevents duplicate
active work for one task and fingerprint. A changed material input supersedes
obsolete pending work and creates a new fingerprint. Optional work skipped by
policy need not create a row.

At startup, attempts abandoned by an interrupted process are marked
interrupted and their work returns to pending. Exhausted transient work becomes
deferred for the next ordinary run. Permanent failure waits for changed input
or configuration rather than being retried every day.

### Attempts

Each provider or model call attempt belongs to a run and work item and records
provider, operation, ordinal, start and finish, outcome, typed failure,
sanitized provider status, request fingerprint, retry metadata, request and
response provenance, token or request usage, resolved pricing, budget
reservation, actual cost, and latency.

Parsed domain results live in their typed tables. Attempt records retain the
immutable provider detail required to reproduce or diagnose how those results
were obtained; they do not become current domain state by themselves.

## Ingestion Records

A feed identity uses a stable configured key rather than treating its current
URL as application identity. Feed fetches store request time, URL, redirects,
HTTP outcome, ETag, Last-Modified, parse outcome, and error. Conditional state
is derived from the latest valid fetch.

A source item stores original feed values, derived normalized plain text, raw
publication-date text and parsed value or issue, canonical URL, feed identity,
and discovery time. A unique canonical-URL constraint implements the approved
insert-once behavior.

Every new source item receives an immutable triage observation, including a
valid zero-person result or typed failure. Research-worthy people become
person-mention records containing the exact source-written name, article
context, semantic outcome, and originating triage observation. One source item
may own several mentions; `do_not_research` remains mention/item-scoped.

When a source item has a usable canonical URL, it references the same canonical
article concept used by search discovery. Feed provenance remains on the
source item rather than being flattened into the article.

## People and Identity Records

### People and sourced names

Application-owned people use SQLite integer primary keys, formatted as
`person-{id}` when exposed to a human. A person is a durable evidence aggregate,
not a normalized string.

Every supported written form is stored separately with:

- exact source-written text;
- mechanically normalized search form;
- kind, such as display, professional, or supported alias;
- originating mention or article; and
- first and last observation times.

A deterministic preference rule selects the current display name from sourced
forms. It may improve when fuller supported text arrives, but old forms remain.
Neither exact nor normalized name has a uniqueness constraint across people.

### Person relations and merges

Entity-resolution observations record the mention, bounded candidate people,
supplied facts, semantic result, rationale, model attempt where applicable,
and selected person.

`possible_same_person` is a symmetric relation that never redirects or combines
evidence. A confirmed duplicate creates a directed merge relation to one
canonical survivor. It does not delete the duplicate or rewrite historical
foreign keys. Future queries resolve the canonical person, and new evidence
attaches there. Merge creation flattens prior redirects and rejects self-links
and cycles so operational lookup never walks an unbounded chain.

The merge transaction also restores canonical operational invariants. It
supersedes active work for the merged-away person and schedules replacement
work against the survivor when the combined input fingerprint changes. If both
people have digest-queue entries, retain one survivor entry with the stronger
tier, earliest pending time, latest material-change time, and combined
eligibility reasons; record the other entry's removal as a merge transition.
Immutable observations and their original current pointers remain historical,
but the survivor never adopts an incompatible pointer merely because it is
newer. Equivalent observations may be selected deterministically; conflicting
or newly combined evidence schedules a fresh canonical observation or lead
assessment. These changes commit atomically with the merge relation.

### Wikipedia identity

MediaWiki pages are provider entities keyed by stable page ID, with canonical
title, URL, namespace, redirect and disambiguation metadata, and observed
content fields. Search observations record query forms, bounds, truncation,
time, and ranked page occurrences.

Each person receives immutable Wikipedia identity observations with the
approved `matching_page_found`, `no_matching_page_found`, or
`uncertain_identity` semantics and full decision basis. A matching observation
references a stable MediaWiki page row. The person points to its current
Wikipedia observation for efficient workflow queries while all prior
observations remain.

## Coverage Evidence Records

### Queries, results, and articles

A person search plan records the triggering evidence fingerprint and bounded
plan version. Each exact, alias, or context query is first-class. Every Brave
result occurrence retains query identity, original rank, provider identifiers,
URL, title, snippets and extra snippets, retrieval time, language/search
parameters, and source-screening disposition.

Repeated occurrences can reference one article, but their query and rank are
never collapsed. One application-owned `canonicalize_article_url` policy
defines article identity for feed items, search occurrences, and observed
redirect destinations alike. A canonical article has one conservatively
normalized unique URL under that policy and a canonical publisher key derived
under the active source policy. Original and redirected URLs remain as
provenance aliases, allowing feed and search discovery to converge without
using separate normalization functions.

Publisher policy is authoritative versioned TOML. Each new screening or
assessment copies the matched rule identifier, rule status, source-policy
fingerprint, and decision time into provenance. SQLite does not dynamically
join historical evidence against today's TOML or retroactively reinterpret it.

### Article views and assessments

An article view records access kind (`full`, `partial`, or `snippets`), fetch
metadata, title, dek, byline, date, editorial labels, cleaned main text, and
extraction result. Store feed metadata, all search snippets, cleaned main-text
extraction, and the exact bounded context supplied to each model. Never store
raw HTML, page chrome, cookie text, binary assets, or paywall-bypassing output.

A person–article relation allows one article to be assessed independently for
several people. Its immutable assessments store same-person judgment, coverage
depth, content type, subject relationship, grounded rationale, exact article
view, policy provenance, and model attempt. The relation points to its current
assessment while preserving prior observations.

Attention and caution signals are typed child records of supplied evidence and
include category, exact claim, supporting passage, article, extraction
attempt, and active domain-profile version.

Retain article text, snippets, structured outputs, and audit records
indefinitely in version one. There is no cleanup scheduler. The pilot volume is
too small to justify retention tiers, archival tables, or compaction.

## Lead and Digest Records

Lead assessments are immutable person-level results with outcome, applied
thresholds, qualifying assessment and canonical publisher references,
attention and caution signals, incompleteness, ordering factors, decision time,
and configuration provenance. A person points to its current lead assessment.

The digest queue is a mutable operational projection with at most one current
row per canonical person. It records pending, emitted, or removed state;
eligibility reason and tier; first-pending and latest-material-change times;
and starvation priority. Immutable queue-transition rows provide arrival,
emission, removal, backlog, and drain-rate history.

A digest records run, file identity, rendering timezone and window, run state,
counts, and immutable content hash. Each digest entry references the exact
person, lead assessment, queue transition, synthesis or fallback, and evidence
rendered. Lead assessments remain authoritative; the queue only answers what
should appear next.

## Current State Without Event Sourcing

Wikipedia identity observations, article assessments, and lead assessments are
immutable. The relevant aggregate keeps an explicit foreign-key pointer to the
current observation. Inserting a replacement and updating the pointer happen
in one transaction.

This is an audit trail, not event sourcing. There is no generic event replay,
event bus, projection daemon, or requirement to reconstruct all current state
from a universal log. Mutable operational projections such as the work and
digest queues are allowed because immutable domain observations and rendered
entries preserve the decisions that matter.

## Idempotency

Each task declares the material fields and versions that affect its result.
Canonical JSON plus those fields is hashed with SHA-256. Relevant inputs may
include schema, prompt, model policy, source policy, query-plan, extractor, or
domain-profile versions. Irrelevant configuration changes do not invalidate
successful work.

The work fingerprint controls duplicate scheduling. Typed domain uniqueness
constraints separately protect canonical feed URLs, article URLs, provider
page IDs, query occurrences, active queue entries, and other invariants. A
successful result is reusable only when its full relevant fingerprint matches.

Policy and prompt changes remain prospective: changing configuration does not
scan and reschedule historical successful work. If new evidence or an explicit
new task legitimately requires reassessment, the current versions participate
in that new fingerprint and produce a new observation.

## Transaction Boundaries

Never hold a SQLite transaction across network or model work. External work
uses:

1. a brief transaction that claims the work item, creates the attempt, and
   reserves budget;
2. the external call with no database lock held; and
3. a brief transaction that stores provider evidence and the typed domain
   observation, reconciles cost, updates current pointers where appropriate,
   and completes or defers the work item.

This is at-least-once external execution. A process can crash after a provider
accepted a request but before SQLite stored the response. Unless a provider
contract offers a usable idempotency key, a later run may repeat that request
and charge. The application guarantees reuse after successful persistence, not
impossible exactly-once semantics across a remote API and local database.

Each source item, person, article, or attempt failure commits independently.
One failure cannot roll back unrelated work. Budget reservation and
reconciliation use integer nano-USD and occur transactionally so concurrent
external scheduling cannot spend the same remaining allowance twice.

## SQLite Configuration

Every connection enables foreign keys. Use WAL journal mode so audit reads do
not block the batch writer, `synchronous=FULL` because write volume is small,
and a short busy timeout. Application code serializes SQLite writes through one
managed connection or transaction boundary even when bounded external calls
run concurrently. No connection pool is required.

Store:

- UTC timestamps as normalized ISO-8601 text ending in `Z`;
- durations as integer milliseconds;
- tokens and request counts as integers;
- money as integer nano-USD, never floating point;
- booleans and enumerations under `CHECK` constraints; and
- canonical JSON text only at the external-detail boundary.

## Migrations and Recovery

Use forward-only files such as `migrations/0001_initial.sql`. Record applied
version, checksum, and time in SQLite. Fail if an applied migration's checksum
changes or the schema version is unexpected.

After acquiring the OS lock, `notable run` checks for pending migrations. It
uses SQLite's backup API to create a recoverable copy, then applies each
migration transactionally before any external call. `notable db migrate`
provides the same operation explicitly but is not a required deployment step.
A migration failure stops the run and reports the backup location.

There are no downgrade migrations. Recovery means restoring the backup,
fixing the forward migration, and retrying. Version one starts with a new
database and performs no import or migration of legacy JSONL state.

## Local Storage Layout

Use `platformdirs` to resolve OS-standard per-user data, log, and cache
locations rather than tying an installed CLI to the repository or current
directory. Typical data roots are:

- macOS: `~/Library/Application Support/notable-person-finder/`;
- Linux: `$XDG_DATA_HOME/notable-person-finder/`, normally
  `~/.local/share/notable-person-finder/`; and
- Windows: the user's local application-data directory.

The data root contains the SQLite database, migration backups, and digest
history. Logs use the corresponding platform log location. A single
configuration override supports portable or server-style paths; tests always
use temporary directories. `notable paths` prints every resolved database,
backup, digest, log, and cache location.

Secrets remain in environment variables or an OS secret mechanism. They are
never stored in TOML, SQLite, snapshots, logs, attempts, or digests.

## Caching

Persist typed provider observations rather than one opaque cache table:

- feed fetches own conditional-request state;
- MediaWiki searches own candidates and expiry;
- Brave queries own ranked result occurrences and expiry;
- article fetches own access state and cleaned views; and
- OpenRouter reuse is based on successful versioned attempt fingerprints.

Adapters may share cache-expiry helpers, but expiry semantics and persisted
fields remain provider-specific and queryable. Refresh creates new
observations; it does not overwrite historical provider results.

## Acceptance Criteria

The persistence design is satisfied when:

- two namesakes can coexist without violating a name uniqueness constraint;
- one confirmed recurring person can accumulate mentions and articles across
  runs;
- a possible same-person relation cannot combine evidence;
- a confirmed merge redirects future work without deleting historical rows;
- feed and search provenance converge on one canonical article without losing
  occurrence rank;
- one article can have different assessments for different people;
- current observations are fast to query and every predecessor remains
  inspectable;
- source-policy changes do not reinterpret old article assessments;
- a crash after a persisted result does not repeat it, while the unavoidable
  pre-persistence remote-call window is documented and tested;
- no transaction is held during external I/O;
- an interrupted work item becomes eligible in the next run;
- budget reservations cannot oversubscribe the configured allowance;
- migrations are checked, backed up, and applied before provider calls;
- database errors fail rather than silently discarding state; and
- the installed command resolves portable OS-standard storage paths.

Pytest covers constraints, migrations from every retained schema version,
transaction rollback, current pointers, merge cycles, namesakes, canonical URL
idempotency, provider observation refresh, work scheduling, interruption,
budget arithmetic, configuration redaction, and storage paths. Integration
tests use temporary real SQLite databases rather than mocking repository SQL.

## Deferred

- ORM and database-framework adoption;
- distributed IDs, workers, leases, or queues;
- multi-user or remote database access;
- historical-policy replay and bulk reassessment;
- automatic database repair or scheduled backup service;
- retention, compaction, or archive jobs;
- import of prototype JSONL data; and
- an interactive data-administration interface.
