# Provider Adapters Design

**Status:** Approved design
**Date:** 2026-07-24

## Purpose

Define the external-service boundaries for the clean-slate redesign: configured
RSS and Atom feeds, English Wikipedia through MediaWiki, Brave Web Search,
ordinary article retrieval and extraction, and OpenRouter. This design makes
remote work bounded, observable, and independently testable without turning
the application into a provider framework.

It implements the workflow, domain, persistence, and LLM decisions in the
earlier design documents. It does not choose prompts, models, search queries,
publisher policy, lead outcomes, database tables, or workflow transitions.

## Principles

- Domain services never receive raw HTTP, parser, or SDK types.
- Each adapter has one provider-specific purpose; there is no generic
  `Provider` superclass.
- Deterministic application code owns workflow and policy. Adapters translate
  requests and external facts.
- One network-adapter call maps to one persisted logical provider attempt.
  Application-controlled pagination is not hidden inside an adapter. Redirect
  exchanges and server-side gateway activity remain visible within their
  owning attempt. The network-free extractor is not an external request.
- Expected provider alternatives are data. Broken execution is a typed
  failure.
- External calls are synchronous and bounded. The design does not introduce
  an async stack or async database access.
- Libraries parse or extract only the content supplied to them. They do not
  silently take ownership of fetching.

## Architecture

Domain services depend on six narrow typed protocols:

1. `FeedClient`;
2. `MediaWikiClient`;
3. `WebSearchClient`;
4. `ArticleFetcher`;
5. `ArticleExtractor`; and
6. `LlmClient`.

A run-scoped synchronous HTTP transport owns ordinary connection reuse,
timeouts, redirects, request headers, response bounds, URL safety, and HTTP
failure translation. Feed, MediaWiki, Brave, and article-fetch adapters use
that transport. The OpenRouter adapter owns the official SDK's client for the
same run, applies the approved LLM timeout, and disables SDK-level application
retries.

Independent remote calls run in a bounded `ThreadPoolExecutor`. The scheduler,
not an adapter, owns submission and concurrency limits. The application thread
claims work before submission and persists results after completion. Worker
threads do not write SQLite, and no database transaction remains open across a
network call.

## Dependencies

Use a small, purpose-specific dependency set:

- [HTTPX](https://www.python-httpx.org/advanced/clients/) for ordinary
  synchronous HTTP transport, connection pooling, timeouts, redirects, and
  streaming responses;
- [feedparser](https://feedparser.readthedocs.io/) for parsing supplied RSS and
  Atom bytes and exposing recoverable parser warnings; the HTTP transport owns
  ETag and Last-Modified validators;
- [Trafilatura](https://trafilatura.readthedocs.io/en/stable/) for extracting
  main text and metadata from supplied HTML, never for fetching;
- the official [OpenRouter Python SDK](https://openrouter.ai/docs/client-sdks/python/overview)
  for model metadata and structured generation; and
- Pydantic for validating untrusted request and response boundaries.

Dependencies use compatible constraints in `pyproject.toml` and exact
resolution in `uv.lock`. Upgrades are explicit reviewed changes. Version one
does not add LangChain, an agent framework, an alternate LLM SDK, an HTTP
cassette framework, a browser, or a second transport stack.

## Shared HTTP Transport

### Lifetime and identity

Create one ordinary transport at run start and close it reliably at run end.
Provider adapters share its policy and connection pool. The standard
application user agent is:

```text
notable-person-finder/<version> (+https://github.com/jonathandeamer/notable-person-finder)
```

Configuration may append a contact URL or email, or replace the complete user
agent. The application does not label itself a Wikimedia bot: it neither edits
Wikipedia nor operates as one. API-specific authentication and content headers
are added by the relevant adapter.

### Timeouts

Use two configurable profiles:

- ordinary HTTP: 10-second connection timeout and 30-second response-inactivity
  timeout;
- OpenRouter: 10-second connection timeout and 300-second response-inactivity
  timeout.

The longer LLM timeout accommodates provider variance and bounded reasoning
without prematurely abandoning a request which may still complete and incur a
charge. Task-specific output and reasoning bounds remain part of LLM
configuration. Version one has no per-feed or per-publisher timeout overrides.

### URL and redirect safety

Accept only `http` and `https` URLs without embedded credentials. Before the
initial request and every redirect, resolve and reject localhost, loopback,
private, link-local, multicast, reserved, or otherwise non-public destination
addresses. Follow at most five redirects and record the requested URL, redirect
chain, and final URL.

This is proportionate protection for a personal single-machine application,
not a claim of public multi-tenant crawler hardening. Ordinary public ports are
permitted; the application does not maintain publisher-specific URL
exceptions.

### Response limits

Stream response bodies and stop when they exceed the applicable configurable
limit:

- 5 MiB for feeds and JSON APIs;
- 10 MiB for article HTML.

Do not download images, PDFs, video, or other non-text article assets. Record
`unsupported_content` or `response_too_large` without pretending that the body
was inspected. Existing feed excerpts or search snippets may still support a
partial evidence view.

### Access boundaries and robots

Version one does **not** request, cache, or interpret `robots.txt`. Proactive
robots handling is explicitly deferred until evidence justifies the feature.
The application nevertheless does not bypass authentication, paywalls,
explicit access denials, or anti-bot controls, and does not impersonate or
rotate browser identities. Direct access limitations are normal retrieval
outcomes rather than invitations to work around them.

## Provider Contracts

All request and response DTOs are immutable typed values. External field names
and optionality are translated at the adapter boundary. Application services
perform domain normalization and decisions.

### FeedClient

`fetch_feed(feed, validators)` performs one conditional feed request. The
request contains the configured feed key and URL plus any prior ETag and
Last-Modified value. It returns either:

- `not_modified`, with response and fetch metadata; or
- `modified`, with requested and resolved URLs, response validators, fetch
  time, recognizable feed type, source metadata, parsed entries, and parser
  warnings.

Entry DTOs retain source-written IDs, URLs, titles, summaries/content, authors,
and date fields without deciding whether an entry is usable. Ingestion owns URL
validation, text normalization, date parsing, and skip reasons.

Feedparser may recover useful entries from imperfect feeds. Accept those
entries when the payload is recognizably RSS or Atom and retain the warnings.
An unrecognizable payload or one without trustworthy feed structure is a typed
`malformed_response` failure. A warning is never silently erased, but it does
not automatically discard valid entries.

### MediaWikiClient

MediaWiki has two operations:

- `search_pages(query, continuation)` returns one API page of ordered search
  hits, completeness facts, and an opaque continuation value;
- `get_page_facts(page_ids)` retrieves a provider-bounded batch of exact pages
  in one request.

Page facts include stable page ID, requested and canonical title, redirect
mapping, namespace, missing-page status, disambiguation status, canonical URL,
and a bounded extract required for identity comparison. Application code owns
the name and alias query plan, namespace filtering, continuation bounds,
candidate assembly, and the decision that retrieval is complete enough to
permit `no_matching_page_found`.

Larger page-ID sets are partitioned by the application rather than hidden
inside the adapter. Requests use the configured user agent and the MediaWiki
`maxlag` parameter.

### WebSearchClient

`search_web(query, count, offset)` performs exactly one Brave Web Search API
request and returns one `SearchPage`. The adapter fixes the approved global,
English-language, moderate-SafeSearch product settings and disables silent
query correction when the API permits it.

The response retains the submitted query, any provider-reported altered query,
offset, provider identifiers, original rank, title, URL, snippets and extra
snippets, language, and pagination/completeness facts. Application code owns
the finite exact-name, sourced-alias, and grounded-context query plan, request
and page bounds, altered-query policy, canonical-URL deduplication, publisher
classification, and article selection.

The adapter never chooses a follow-up query, switches to Brave News, assesses
source reliability, or reranks results from different calls.

### ArticleFetcher

`fetch_article(url)` applies URL safety, ordinary access policy, redirect,
timeout, content-type, and body-size rules. A successful result contains the
requested and final URLs, redirect chain, response facts, byte count, and HTML
bytes held only for the current attempt.

Expected inaccessible states are typed values, including not found,
authentication required, paywalled or explicit access denial when detectable,
unsupported content, and response too large. Operational transport or server
breakage remains a typed failure. The fetcher does not extract text or infer
article significance.

### ArticleExtractor

`extract_article(html)` is a pure, network-free boundary. It uses Trafilatura
plus deterministic cleanup to return title, dek, byline, publication date,
editorial labels, ordered main-text blocks, and extraction-quality or partial-
view warnings. It removes navigation, footer, consent text, comments, related
or trending links, repeated boilerplate, and unrelated captions as far as the
generic extractor can do so safely.

Raw HTML is never persisted or sent to a model. It is released after the
attempt. Version one has no publisher-specific scraper or cleanup rule.

The extractor remains person-agnostic. A deterministic application
`PassageSelector` constructs a person-specific view containing title and dek,
opening blocks, every block containing a sourced name or alias, and adjacent
blocks for local context. Configurable task bounds cap that view. Block IDs and
truncation metadata let model outputs cite only supplied evidence. If the name
does not occur, the selector supplies a bounded opening plus available feed or
search snippets and records the limitation; it does not invoke another model
to select passages.

### LlmClient

The OpenRouter boundary has two operations:

- `inspect_model(model_id)` retrieves current capability and pricing metadata;
- `generate_structured(request)` submits one rendered system prompt, canonical
  task input, JSON Schema, configured model and parameters, and provider-routing
  requirements.

Every run inspects each distinct configured model once before paid generation,
deduplicating models shared by tasks. Preflight verifies strict structured-
output support and records current pricing/capability metadata. If fresh
preflight cannot complete, the run fails before paid LLM calls rather than
using stale capability or price data.

A generation response retains raw model output, configured and resolved model,
resolved serving provider when supplied, parameters, request ID, structured-
output metadata, usage, latency, and reported cost. The adapter does not know
the meanings of `detect_people`, `resolve_person_entity`,
`match_wikipedia_identity`, `assess_article`, or `compose_lead_summary`; choose
models; render prompts; validate domain references; retry malformed output; or
repair JSON. Pydantic and application constraints make those decisions after
the provider response.

OpenRouter may route the same configured model among compatible serving
providers. It may not silently select a different model. Its invisible
gateway-level routing or retry before one returned response remains one
application attempt and is recorded using the metadata OpenRouter exposes.

## Outcomes and Failures

Use a mixed contract:

- expected alternatives and limitations are discriminated return values;
- operational breakage is a typed exception containing a safe category,
  retryability input, provider status or code, and `Retry-After` when present;
- raw HTTPX, feedparser, Trafilatura, MediaWiki, Brave, or OpenRouter exceptions
  never cross the adapter boundary.

Typed operational categories cover DNS/connection/TLS failure, timeout, rate
limit, transient server error, authentication or authorization failure,
invalid request or configuration, malformed response, and provider
unavailability. The application retry policy maps those facts to the already
approved transient/permanent behavior. Semantic uncertainty is never a
provider failure.

Disable automatic request retries in HTTP clients and through the OpenRouter
SDK's documented per-request
[`retries`](https://openrouter.ai/docs/client-sdks/python/api-reference/chat)
configuration. Only the central application retry coordinator starts another
visible request. This keeps attempt ordinals, logs, cost reservations, and
persisted retry state truthful.

## Concurrency and Pacing

Initial configurable defaults are:

- four concurrent ordinary HTTP requests;
- two concurrent OpenRouter generations; and
- two concurrent requests to any single origin.

Independent feeds, people, and articles may proceed concurrently. Query or
pagination steps whose inputs depend on an earlier result remain sequential
for that work item.

Preserve the legacy tool's conservative API pacing as initial defaults:

- MediaWiki request starts are at least 900 ms apart;
- Brave request starts are at least 1,100 ms apart.

Feeds and publisher article pages use the per-origin concurrency cap without
an artificial interval. All adapters expose `Retry-After` to the central retry
coordinator. Limits are provider-level or global; version one has no
publisher-specific transport tuning.

## Credentials and Configuration

Read secrets only from environment variables:

- `OPENROUTER_API_KEY`;
- `BRAVE_API_KEY`.

An ignored local `.env` may supply them for convenience, and `.env.example`
documents names only. TOML may configure which environment-variable name to
read but never contains the value. Feed, MediaWiki, and article retrieval are
unauthenticated in version one.

Validated configuration contains provider endpoints, timeout profiles,
response limits, redirect and concurrency bounds, pacing, user-agent/contact,
model IDs and parameters, and secret environment-variable names. Invalid
bounds, missing required credentials, unsafe endpoint URLs, unsupported model
capabilities, or incompatible structured-output settings fail before workflow
work is claimed.

Config snapshots, provider attempts, logs, exceptions, and digest output redact
authorization headers, query credentials, and known secret values.

## Persistence and Observability

Structured provider-attempt logs contain run and work IDs, provider operation,
destination host, safe status or typed outcome, duration, retry ordinal, byte
count, and a safe provider request ID. Routine logs do not contain response
bodies, prompts, model responses, request headers, secrets, complete search
queries, or full article URLs. An explicit debug mode may expose safe URLs and
queries but never credentials.

Product provenance still persists full source URLs, search queries, normalized
provider observations, exact supplied LLM context, and raw/validated model
output in their already designated SQLite records. Do not duplicate these
large or potentially sensitive values in logs.

Each external network call maps to one persisted provider attempt. Local
article extraction is instead part of the owning work item's deterministic
processing record. Redirects remain one logical attempt with their chain;
server-directed OpenRouter gateway activity remains one attempt with available
resolved metadata. Successful provider observations are immutable, and the
existing typed cache and work-fingerprint rules decide reuse.

## Testing

Use three layers.

### Unit and contract tests

Fake HTTP transports and fake OpenRouter clients verify request construction,
headers, conditional requests, Pydantic conversion, safe exception mapping,
retry metadata, pagination, continuation, altered-query handling, redirects,
URL safety, response bounds, redaction, concurrency, and pacing without using
the network or sleeping in real time.

### Parser and extractor fixtures

Commit small sanitized fixtures covering valid and imperfect RSS and Atom,
MediaWiki redirects/namespaces/disambiguation/missing pages, Brave search
pages, representative publisher HTML and chrome removal, partial and
inaccessible article views, and valid/invalid OpenRouter structured responses.
Fixtures contain the provider boundary needed by the test, not complete saved
websites or opaque SDK objects.

### Live smoke tests

Explicitly invoked live tests exercise each provider and are skipped by
default. Credentialed tests require the corresponding environment variable.
The OpenRouter smoke test requests one tiny structured response and remains
separate from paid Promptfoo evaluation. The configurable pilot feeds are
checked by opt-in integration tests rather than ordinary CI.

Version one does not add recorded HTTP cassettes. They duplicate typed fixtures,
age with external responses, and create another place where credentials can be
captured.

## Acceptance Criteria

- Domain code imports no HTTPX, feedparser, Trafilatura, Brave, MediaWiki, or
  OpenRouter response types.
- Each network-adapter call maps to one attributable persisted provider
  attempt; application code owns pagination and continuation bounds. Redirect
  chains remain within that attempt, and local extraction is not a provider
  attempt.
- All core workflow and adapter-contract tests run offline.
- Recoverable feed warnings remain visible without discarding usable entries.
- Provider failure or incomplete retrieval cannot become a semantic negative.
- Article extraction makes no network request and raw HTML is not persisted or
  sent to a model.
- The passage selector is deterministic, bounded, person-specific, and cites
  supplied block IDs.
- No automatic client retry can bypass persisted retry and budget accounting.
- URL, timeout, redirect, content, response-size, concurrency, and pacing bounds
  are enforced.
- Secrets and raw bodies do not appear in logs or configuration snapshots.
- Fresh OpenRouter capability and pricing preflight occurs before paid calls.
- Version one neither fetches `robots.txt` nor circumvents direct access
  controls.
