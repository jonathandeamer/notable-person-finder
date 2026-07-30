"""Seeding feed work, and the `fetch_feed` handler that performs it.

This module is where configuration meets the run engine. It has two jobs:

* `seed_feeds` turns the configured feed list into work the engine can claim,
  once per run, before the first claim happens.
* `build_fetch_handler` supplies the three-phase handler that fetches one feed.

**The thread split is the thing to get right here, and it is easy to get
wrong.** The run engine runs a handler's phases on two different threads:

* `prepare` runs on the **application thread**. It is the only phase permitted
  to read SQLite, and in this handler it is the only phase that touches it at
  all -- reading the feed identity a work item points at, and the validators a
  previous fetch stored.
* `execute` runs on a **scheduler worker thread**. It performs exactly one
  external call and must never retry, never sleep, and never touch SQLite. The
  engine's connection belongs to the application thread and is not opened with
  `check_same_thread=False`, so using it from a worker is a fault, not a race
  to be tolerated. `execute` is built by `_execute_for`, a module-level
  factory, so that no connection is *in scope* where it is defined: a nested
  definition inside `build_fetch_handler` would sit in a scope where one is in
  reach, and only a comment would stand between a later edit and a
  cross-thread write. To be precise about how much that buys, since the test
  pinning it can only see what the closure actually captured: CPython creates a
  cell only for a name a nested function *references*, so a nested `execute`
  that never mentioned the connection would look identical from the outside.
  The factory is what stops the next edit from mentioning it, not a proof about
  the code as it stands today.
* `persist` (added by the next task) runs on the application thread inside the
  transaction that settles the work item.
"""

from __future__ import annotations

import hashlib
import html
import json
import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

from notable_person_finder.config.models import FeedConfig, FeedsConfig
from notable_person_finder.ingestion.models import (
    FetchPersistResult,
    PublishedIssue,
    UrlIssue,
)
from notable_person_finder.ingestion.repository import (
    feed_identities,
    insert_source_item,
    latest_validators,
    record_alias,
    record_fetch,
    upsert_article,
    upsert_feed_identity,
)
from notable_person_finder.ingestion.urls import (
    UnusableArticleUrl,
    UnusableUrlReason,
    canonicalize_article_url,
    publisher_key,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.feeds import (
    PROVIDER,
    FeedClient,
    FeedEntry,
    FeedValidators,
    Modified,
    NotModified,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import Clock, utc_timestamp
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome, TaskPreparation
from notable_person_finder.runs.models import WorkItem, WorkState

FETCH_FEED_TASK_TYPE = "fetch_feed"
FETCH_FEED_OPERATION = "fetch_feed"
SUBJECT_KIND = "feed_identity"

# Lower sorts earlier: `claim_batch` orders by `priority ASC, id ASC`.
# Required-before-optional is not expressed by the `required` column at all --
# it holds only insofar as seeding encodes it here. A feed fetch is the root of
# every downstream task, so it takes the front of the queue, with room left
# below for anything that must ever pre-empt it.
FETCH_FEED_PRIORITY = 10

# Bumped when parsing behaviour changes materially. It is part of the work
# item's fingerprint, so raising it reschedules every feed even where nothing
# in configuration moved -- which is the point: improved parsing should get a
# chance to re-read today's feeds. Editing a comment or a label in the
# configuration file must not have that effect, which is why the fingerprint is
# built from named fields rather than from the feed's serialized form.
PARSER_VERSION = 1

# One stable, low-cardinality literal. It reaches the `work_item.reason`
# column, so it must not interpolate a key, a label, or a URL. "Disabled" and
# "deleted from configuration" deliberately share it: to this module they are
# the same event -- a feed that is no longer configured to be fetched -- and
# splitting them would mint reason keys for a distinction nothing downstream
# acts on.
SUPERSEDED_REASON = "feed disabled"

# Two categories the engine's retry policy would settle `failed_permanent`,
# because neither is in `RETRYABLE_CATEGORIES`. Both are wrong there, and each
# is wrong for its own reason:
#
# * `response_too_large` says nothing permanent about the publisher; it is a
#   response this application declined to read. Tomorrow's feed is a different
#   document and may well be within bounds.
# * `unsupported_content` is the same shape of judgement about a content type.
#
# Retrying either *within* the run is pointless, though -- the response will be
# just as large and just as unsupported on the next call -- so the override is
# not `retryable=True` at the raise site (which would burn the run's whole
# attempt budget to learn nothing) but a direct `DEFERRED` outcome here, which
# spends no further attempt and leaves the item claimable by a later run.
#
# The reasons say "not inspected" and never "empty": a response that was
# refused before parsing is not a feed with no entries, and a digest that
# conflated the two would report a publisher as having gone quiet when in fact
# this application never looked. `NotInspected` carries the same distinction
# into the stored fetch row.
NOT_INSPECTED_REASONS: dict[FailureCategory, str] = {
    FailureCategory.RESPONSE_TOO_LARGE: "not inspected: response too large",
    FailureCategory.UNSUPPORTED_CONTENT: "not inspected: unsupported content",
}


@dataclass(frozen=True, slots=True)
class FeedCall:
    """What `prepare` read on the application thread for `execute` to use.

    Carries no connection and no repository handle on purpose: this is the
    whole of what crosses to the worker thread, so what a worker can reach is
    bounded by this type rather than by discipline.
    """

    feed: FeedConfig
    validators: FeedValidators


@dataclass(frozen=True, slots=True)
class NotInspected:
    """A response that was refused before it was read.

    Deliberately *not* a variant of `FeedFetchResult`: the adapter returns that
    union only for responses it actually parsed, and widening it would let a
    refusal reach code written to assume a document was read. This is the
    handler's own payload type, so the persistence step can record a fetch that
    happened and produced no entries *because nothing was inspected*, rather
    than as a successful read of an empty feed.
    """

    requested_url: str
    category: FailureCategory
    # Frequently `None`, and that is not a bug to route around: the transport
    # raises `response_too_large` from its streaming read
    # (`transport.py:333-338`) with no status code, because the refusal is about
    # the body rather than the response line. A caller must treat this as
    # "unknown", never as "no HTTP exchange happened".
    status_code: int | None
    # The raising adapter's sanitized detail -- for `response_too_large`, the
    # byte cap that was exceeded. Carried because it is the only quantitative
    # fact about a response nobody read, and because `status_code` is usually
    # absent. Safe to store: `ProviderFailure` requires `detail` to be free of
    # bodies, credentials, and query text. Must never be folded into a
    # `TaskOutcome.reason`, which is a `GROUP BY` key.
    detail: str | None


# --- entry normalization and persistence --------------------------------------


def _plain_text(raw: str | None) -> str | None:
    """Turn raw HTML-ish text into plain text for storage and downstream use.

    Strips HTML tags, unescapes entities, and collapses runs of whitespace.
    `None` and the empty string both become `None` so the database stores a
    clean NULL rather than an empty column that downstream code has to treat
    as missing.
    """
    if raw is None:
        return None
    text = re.sub(r"<[^>]+>", "", raw)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def _url_issue_for(reason: UnusableUrlReason) -> UrlIssue:
    """Map a URL usability reason to the stored `url_issue` enum value."""
    if reason is UnusableUrlReason.UNSUPPORTED_SCHEME:
        return UrlIssue.NOT_HTTP
    if reason is UnusableUrlReason.EMBEDDED_CREDENTIALS:
        return UrlIssue.UNSAFE
    return UrlIssue.UNUSABLE


def _parse_date(raw: str) -> datetime | None:
    """Parse an RFC 822 or ISO-8601 date string into a UTC datetime, or None."""
    try:
        parsed = parsedate_to_datetime(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (TypeError, ValueError, IndexError):
        pass
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def _parse_published(
    raw: str | None,
    reference: datetime,
) -> tuple[str | None, PublishedIssue | None]:
    """Turn a feed entry's raw date into a stored UTC value or a typed issue.

    `reference` is the run's settlement timestamp; dates outside a reasonable
    window around it are flagged `implausible` rather than accepted. The raw
    text is always preserved by the caller, so nothing is lost on failure.
    """
    if raw is None or raw.strip() == "":
        return None, PublishedIssue.MISSING
    parsed = _parse_date(raw)
    if parsed is None:
        return None, PublishedIssue.UNPARSEABLE
    if parsed.year < 1900 or parsed > reference + timedelta(days=1):
        return None, PublishedIssue.IMPLAUSIBLE
    return utc_timestamp(parsed), None


def _resolve_article(
    connection: sqlite3.Connection,
    original_url: str | None,
    now: str,
    articles_created: list[int],
) -> tuple[int | None, str | None, UrlIssue | None]:
    """Canonicalize an entry URL, upsert the article, and record any alias.

    Returns `(article_id, original_url, url_issue)`. When the URL is unusable
    the article is `None` and the original URL is still returned so the source
    item can keep what the feed published. `articles_created` is a mutable
    counter because the caller tracks several counts at once.
    """
    if original_url is None or original_url == "":
        return None, None, UrlIssue.MISSING
    try:
        canonical_url = canonicalize_article_url(original_url)
    except UnusableArticleUrl as error:
        return None, original_url, _url_issue_for(error.reason)

    key = publisher_key(canonical_url)
    existing = connection.execute(
        "SELECT id FROM canonical_article WHERE canonical_url = ?", (canonical_url,)
    ).fetchone()
    if existing is None:
        upsert_article(
            connection,
            canonical_url=canonical_url,
            publisher_key=key,
            now=now,
        )
        articles_created.append(1)
    article_id = int(
        connection.execute(
            "SELECT id FROM canonical_article WHERE canonical_url = ?", (canonical_url,)
        ).fetchone()["id"]
    )
    # Record the observed URL as an alias even when it already is the canonical
    # form: a bare feed link is a genuine sighting, and the acceptance criterion
    # for convergence requires the bare URL to appear alongside a tracked one.
    record_alias(
        connection,
        canonical_article_id=article_id,
        url=original_url,
        kind="feed_original",
        now=now,
    )
    return article_id, original_url, None


def _record_modified(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    result: Modified,
    now: str,
) -> int:
    """Write one `modified` fetch row and return its id."""
    return record_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_at=now,
        requested_url=result.requested_url,
        resolved_url=result.final_url,
        redirect_chain_json=(
            json.dumps(list(result.redirect_chain)) if result.redirect_chain else None
        ),
        http_status=result.status_code,
        etag=result.validators.etag,
        last_modified=result.validators.last_modified,
        outcome="modified",
        feed_type=result.feed_type,
        parse_outcome="recovered" if result.warnings else "ok",
        parser_warnings_json=(
            json.dumps(list(result.warnings)) if result.warnings else None
        ),
        failure_category=None,
        entry_count=len(result.entries),
        response_bytes=result.response_bytes,
    )


def _record_not_modified(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    result: NotModified,
    now: str,
) -> None:
    """Write one `not_modified` fetch row."""
    record_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_at=now,
        requested_url=result.requested_url,
        resolved_url=result.final_url,
        redirect_chain_json=(
            json.dumps(list(result.redirect_chain)) if result.redirect_chain else None
        ),
        http_status=result.status_code,
        etag=result.validators.etag,
        last_modified=result.validators.last_modified,
        outcome="not_modified",
        feed_type=None,
        parse_outcome=None,
        parser_warnings_json=None,
        failure_category=None,
        entry_count=0,
        response_bytes=0,
    )


def _record_not_inspected(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    result: NotInspected,
    now: str,
) -> None:
    """Write one `failed` fetch row for a response that was refused before parsing."""
    record_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_at=now,
        requested_url=result.requested_url,
        resolved_url=None,
        redirect_chain_json=None,
        http_status=result.status_code,
        etag=None,
        last_modified=None,
        outcome="failed",
        feed_type=None,
        parse_outcome=None,
        parser_warnings_json=None,
        failure_category=str(result.category),
        entry_count=None,
        response_bytes=None,
    )


def _record_failed_fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    requested_url: str,
    failure: ProviderFailure,
    now: str,
) -> None:
    """Write one `failed` fetch row for a provider failure the engine adjudicated."""
    record_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_at=now,
        requested_url=requested_url,
        resolved_url=None,
        redirect_chain_json=None,
        http_status=failure.status_code,
        etag=None,
        last_modified=None,
        outcome="failed",
        feed_type=None,
        parse_outcome=None,
        parser_warnings_json=None,
        failure_category=str(failure.category),
        entry_count=None,
        response_bytes=None,
    )


def _persist_entries(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    fetch_id: int,
    run_id: int,
    entries: tuple[FeedEntry, ...],
    now: str,
) -> FetchPersistResult:
    """Persist every entry from a modified feed, counting creates and issues."""
    reference = datetime.fromisoformat(now.replace("Z", "+00:00"))
    source_items_created = 0
    source_items_existing = 0
    articles_created: list[int] = []
    entry_issues: dict[str, int] = {}
    created_source_item_ids: list[int] = []

    for entry in entries:
        try:
            article_id, original_url, url_issue = _resolve_article(
                connection, entry.url, now, articles_created
            )
        except Exception:
            # A per-entry fault must not roll back the whole fetch. Treat it
            # as an unusable URL and keep the raw text, which may still name
            # a person.
            article_id = None
            original_url = entry.url
            url_issue = UrlIssue.UNUSABLE

        published_at, published_issue = _parse_published(entry.published_raw, reference)

        item_id = insert_source_item(
            connection,
            feed_identity_id=feed_identity_id,
            discovered_by_fetch_id=fetch_id,
            discovered_by_run_id=run_id,
            canonical_article_id=article_id,
            source_entry_id=entry.entry_id,
            original_url=original_url,
            title_raw=entry.title,
            title_text=_plain_text(entry.title),
            summary_raw=entry.summary,
            summary_text=_plain_text(entry.summary),
            author_raw=entry.author,
            published_raw=entry.published_raw,
            published_at=published_at,
            published_issue=None if published_issue is None else str(published_issue),
            url_issue=None if url_issue is None else str(url_issue),
            now=now,
        )
        if item_id is None:
            source_items_existing += 1
        else:
            source_items_created += 1
            created_source_item_ids.append(item_id)

        if url_issue is not None:
            key = f"url:{url_issue}"
            entry_issues[key] = entry_issues.get(key, 0) + 1
        if published_issue is not None:
            key = f"published:{published_issue}"
            entry_issues[key] = entry_issues.get(key, 0) + 1

    return FetchPersistResult(
        source_items_created=source_items_created,
        source_items_existing=source_items_existing,
        articles_created=sum(articles_created),
        entry_issues=entry_issues,
        created_source_item_ids=tuple(created_source_item_ids),
    )


def persist_fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    result: Modified | NotModified | NotInspected,
    now: str,
) -> FetchPersistResult:
    """Persist one feed fetch and its entries inside the settling transaction.

    Always writes a `feed_fetch` row: successes, 304s, and failures all leave
    durable evidence. For a `Modified` result, every entry is normalized and
    inserted; duplicates are counted as existing rather than errors.
    """
    if isinstance(result, Modified):
        fetch_id = _record_modified(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            result=result,
            now=now,
        )
        return _persist_entries(
            connection,
            feed_identity_id=feed_identity_id,
            fetch_id=fetch_id,
            run_id=run_id,
            entries=result.entries,
            now=now,
        )
    if isinstance(result, NotModified):
        _record_not_modified(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            result=result,
            now=now,
        )
        return FetchPersistResult(0, 0, 0, {})
    if isinstance(result, NotInspected):
        _record_not_inspected(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            result=result,
            now=now,
        )
        return FetchPersistResult(0, 0, 0, {})
    raise TypeError(f"unexpected fetch result type: {type(result).__name__}")


def persist_failed_fetch(
    connection: sqlite3.Connection,
    *,
    feed_identity_id: int,
    run_id: int,
    requested_url: str,
    failure: ProviderFailure,
    now: str,
) -> FetchPersistResult:
    """Persist a fetch that failed before returning a parsed result.

    This is the `persist_failure` seam for the engine-settled failures that
    never produce a `TaskOutcome` -- exhausted transient failures and
    non-retryable permanent failures. The fetch row records that the attempt
    happened and why it failed.
    """
    _record_failed_fetch(
        connection,
        feed_identity_id=feed_identity_id,
        run_id=run_id,
        requested_url=requested_url,
        failure=failure,
        now=now,
    )
    return FetchPersistResult(0, 0, 0, {})


def _fingerprint(*, feed_key: str, url: str) -> str:
    """SHA-256 over canonical JSON of the fields that identify this request.

    Exactly three things make one scheduled fetch a different fetch from
    another: which feed it is, where that feed lives, and how this application
    parses feeds. Nothing else from `FeedConfig` belongs here -- a label change
    is cosmetic, and `enabled` is a scheduling decision rather than part of a
    request's identity.

    At seed time no fetch has happened, so the configured URL is the only URL
    that exists. A feed later discovered to redirect keeps this fingerprint;
    the resolved URL is recorded on the fetch row, not folded back into the
    identity of the work.
    """
    canonical = json.dumps(
        {"feed_key": feed_key, "parser_version": PARSER_VERSION, "url": url},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def seed_feeds(
    connection: sqlite3.Connection,
    *,
    feeds: FeedsConfig,
    run_id: int,
    now: str,
) -> None:
    """Register today's feed fetches, and retire work for feeds that left.

    Each feed's identity upsert and its scheduling commit separately, so one
    feed cannot take another's scheduling down with it -- nothing here is
    wrapped in a transaction spanning feeds.

    `schedule_work` is called unconditionally and left to decide. It already
    opens `BEGIN IMMEDIATE`, looks for an item in `pending`, `running`, or
    `deferred` for this task type and fingerprint, and returns that item's id
    instead of inserting. So yesterday's *succeeded* item is terminal and does
    not block today's, while a *deferred* item is reused rather than
    duplicated. A second existence check here would be a duplicate of that
    logic under a different lock, and the two would eventually disagree.
    """
    enabled = [feed for feed in feeds.feeds if feed.enabled]
    for feed in enabled:
        feed_identity_id = upsert_feed_identity(
            connection,
            key=feed.key,
            label=feed.label,
            url=feed.url,
            now=now,
        )
        repository.schedule_work(
            connection,
            task_type=FETCH_FEED_TASK_TYPE,
            subject_kind=SUBJECT_KIND,
            subject_id=feed_identity_id,
            fingerprint=_fingerprint(feed_key=feed.key, url=feed.url),
            required=True,
            priority=FETCH_FEED_PRIORITY,
            eligible_at=now,
            run_id=run_id,
            now=now,
        )

    _retire_unconfigured_feeds(
        connection,
        enabled_keys={feed.key for feed in enabled},
        run_id=run_id,
        now=now,
    )


def _retire_unconfigured_feeds(
    connection: sqlite3.Connection,
    *,
    enabled_keys: set[str],
    run_id: int,
    now: str,
) -> None:
    """Supersede outstanding work for feeds configuration no longer asks for.

    Driven from the stored identities rather than from configuration, which is
    what makes a *deleted* feed reachable at all: a feed absent from the file
    has no configured entry to schedule from, but its identity row still names
    the subject whose outstanding work must be retired.

    Retirement is by subject, not by the current-URL fingerprint. A URL move
    leaves an older active fingerprint for the same feed identity; superseding
    only the latest fingerprint would leave that orphan to fail later in
    prepare. `supersede_work_for_subject` touches only `pending` and
    `deferred`, so a feed's completed history is never rewritten by switching
    the feed off.
    """
    for identity in feed_identities(connection):
        if identity.key in enabled_keys:
            continue
        repository.supersede_work_for_subject(
            connection,
            task_type=FETCH_FEED_TASK_TYPE,
            subject_kind=SUBJECT_KIND,
            subject_id=identity.id,
            run_id=run_id,
            now=now,
            reason=SUPERSEDED_REASON,
        )


def build_seed_hook(
    connection: sqlite3.Connection, *, feeds: FeedsConfig, clock: Clock
) -> Callable[[int], None]:
    """Adapt `seed_feeds` to the engine's `seed` hook, which takes only a run id.

    The engine calls its hook after creating the run and before the first
    claim, and it passes the run id and nothing else -- it has no business
    knowing that seeding needs a feed list or a clock. Binding them here is
    what keeps that hook's signature free of ingestion's concerns.
    """

    def seed(run_id: int) -> None:
        seed_feeds(
            connection,
            feeds=feeds,
            run_id=run_id,
            now=utc_timestamp(clock.now()),
        )

    return seed


def _execute_for(
    client: FeedClient,
) -> Callable[[WorkItem, int, object], TaskOutcome]:
    """Build the worker-thread phase, in a scope that holds no connection.

    Module-level, and not a nested function inside `build_fetch_handler`, for
    the reason given in the module docstring: the returned closure's only free
    variable is `client`, so "a worker never touches SQLite" is a property of
    the code's shape rather than a rule someone has to remember.
    """

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        if not isinstance(prepared, FeedCall):
            # Unreachable while `prepare` is the only producer, and translated
            # rather than raised bare because the engine's execute phase does
            # not isolate a non-`ProviderFailure`: one would propagate out of
            # the run and discard every sibling feed's results. `INTERNAL` is
            # not retryable, so this settles the one item permanently, which is
            # the right answer for a wiring fault.
            raise ProviderFailure(
                FailureCategory.INTERNAL,
                provider=PROVIDER,
                operation=FETCH_FEED_OPERATION,
                detail=f"prepared value was {type(prepared).__name__}",
            )

        try:
            result = client.fetch_feed(prepared.feed, prepared.validators)
        except ProviderFailure as failure:
            reason = NOT_INSPECTED_REASONS.get(failure.category)
            if reason is None:
                # Everything else is the retry policy's call, not this
                # handler's. Deciding here would put retry adjudication in two
                # places, which is what the central coordinator exists to
                # prevent.
                raise
            return TaskOutcome(
                state=WorkState.DEFERRED,
                reason=reason,
                # The one place this failure survives on the attempt row. The
                # engine records any non-None outcome as an attempt that
                # `succeeded` with a NULL `failure_category`
                # (`engine.py:736-748`), and migration `0002`'s CHECK on
                # `attempt` forbids a `failure_category` unless `outcome =
                # 'failed'`, so the category cannot be stored in its own column
                # on this path however the handler behaves. Without this the
                # only trace of a 40 MB feed anywhere in the run would be a
                # deferral reason: `operational_failures_for_run` counts
                # `outcome = 'failed'`, so the digest's operational-failure
                # count and its failure-category breakdown both omit it.
                # `failure.detail` is sanitized by the raising adapter per
                # `ProviderFailure`'s contract -- for `response_too_large` it is
                # the byte cap, which is the fact worth keeping.
                detail_json=json.dumps(
                    {"detail": failure.detail, "not_inspected": str(failure.category)},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                payload=NotInspected(
                    # The configured URL, not anything the response said: this
                    # value is stored and rendered, and the response was
                    # refused precisely because it was not to be trusted or
                    # read.
                    requested_url=prepared.feed.url,
                    category=failure.category,
                    status_code=failure.status_code,
                    detail=failure.detail,
                ),
            )

        if isinstance(result, NotModified):
            # A success: the feed is unchanged. The result still travels on the
            # payload, because the fetch row is written for a 304 too -- a
            # conditional hit that left no trace would be indistinguishable
            # from a fetch that never ran.
            return TaskOutcome(
                state=WorkState.SUCCEEDED, reason="not modified", payload=result
            )

        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=result,
            response_bytes=result.response_bytes,
        )

    return execute


def _attempt_context(
    connection: sqlite3.Connection, work_item_id: int
) -> tuple[int, str]:
    """The run id and started-at timestamp of the latest attempt for an item.

    `persist` and `persist_failure` need the run id and the instant the fetch
    began, neither of which is in the `TaskHandler` signatures. The attempt row
    written just before the call was committed by `finish_attempt`, so it is
    the durable source for both values.
    """
    row = connection.execute(
        "SELECT run_id, started_at FROM attempt "
        "WHERE work_item_id = ? ORDER BY id DESC LIMIT 1",
        (work_item_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"no attempt row for work item {work_item_id}; cannot persist fetch"
        )
    return int(row["run_id"]), row["started_at"]


def _persist_for(
    connection: sqlite3.Connection,
    resolve: Callable[[WorkItem], tuple[int, FeedConfig]],
    on_source_items: Callable[[tuple[int, ...], int, str], None] | None,
) -> Callable[[WorkItem, TaskOutcome], None]:
    """Build the application-thread `persist` callback for `build_fetch_handler`.

    When `on_source_items` is set, it runs on this same application thread
    *inside* the settling transaction, after source-item inserts, so a
    callback failure rolls back both the feed settlement and the new items.
    Ingestion never imports people; the CLI injects scheduling here.
    """

    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        feed_identity_id, _feed = resolve(work_item)
        run_id, requested_at = _attempt_context(connection, work_item.id)
        result = outcome.payload
        if not isinstance(result, Modified | NotModified | NotInspected):
            raise RuntimeError(
                f"unexpected fetch payload type: {type(result).__name__}"
            )
        counts = persist_fetch(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            result=result,
            now=requested_at,
        )
        if on_source_items is not None and counts.created_source_item_ids:
            on_source_items(counts.created_source_item_ids, run_id, requested_at)

    return persist


def _persist_failure_for(
    connection: sqlite3.Connection,
    resolve: Callable[[WorkItem], tuple[int, FeedConfig]],
) -> Callable[[WorkItem, ProviderFailure], None]:
    """Build the application-thread `persist_failure` callback."""

    def persist_failure(work_item: WorkItem, failure: ProviderFailure) -> None:
        feed_identity_id, feed = resolve(work_item)
        run_id, requested_at = _attempt_context(connection, work_item.id)
        persist_failed_fetch(
            connection,
            feed_identity_id=feed_identity_id,
            run_id=run_id,
            requested_url=feed.url,
            failure=failure,
            now=requested_at,
        )

    return persist_failure


def build_fetch_handler(
    connection: sqlite3.Connection,
    *,
    client: FeedClient,
    feeds: FeedsConfig,
    on_source_items: Callable[[tuple[int, ...], int, str], None] | None = None,
) -> TaskHandler:
    """The `fetch_feed` handler: one feed fetch, split across the two threads.

    `on_source_items`, when provided, is invoked from `persist` on the
    application thread inside the settlement transaction after newly inserted
    source items, as ``(created_source_item_ids, run_id, now)``. Absent, the
    handler matches milestone 3a behaviour.
    """
    configured = {feed.key: feed for feed in feeds.feeds}

    def resolve(work_item: WorkItem) -> tuple[int, FeedConfig]:
        """Which feed this work item is for. Application thread; raises if none.

        Seeding supersedes items whose feed has left configuration before the
        claim loop reaches them, so every failure below is unreachable in an
        ordinary run. Each still refuses rather than repairs: substituting some
        other configured feed would attribute one publisher's entries to
        another, which is unrecoverable once stored.
        """
        feed_identity_id = work_item.subject_id
        if feed_identity_id is None:
            raise LookupError(
                f"{FETCH_FEED_TASK_TYPE} work item {work_item.id} names no feed"
            )
        # Read per call rather than cached at construction: the identities this
        # has to resolve are created by seeding, which runs after the handler
        # is built.
        identity = next(
            (
                candidate
                for candidate in feed_identities(connection)
                if candidate.id == feed_identity_id
            ),
            None,
        )
        if identity is None:
            raise LookupError(f"feed identity {feed_identity_id} is not stored")
        feed = configured.get(identity.key)
        if feed is None or not feed.enabled:
            # `not feed.enabled` is not redundant with `feed is None`: a feed
            # can be present in the file with `enabled = false`, and fetching
            # one an operator has switched off would ignore the only instruction
            # they gave about it.
            raise LookupError(f"feed {identity.key!r} is not enabled in configuration")
        return (feed_identity_id, feed)

    def destination_host(work_item: WorkItem) -> str | None:
        """The host this attempt will contact, for the attempt row's diagnostics.

        Deliberately swallows a resolution failure and returns `None`. The engine
        calls this *outside* the try/except that isolates `prepare`
        (`engine.py:667-671`), so an exception here would propagate out of the
        run, leave it `running`, and write no digest -- discarding every sibling
        feed's results to fail at labelling one attempt row. A diagnostic column
        must never be able to do that.

        Nothing is lost by staying quiet: the item's real problem has *already*
        surfaced, in isolation, because `prepare` runs first and settles the item
        `failed_permanent` for the same reason (`engine.py:635-660`), so on the
        engine's path this branch is unreachable. It exists so that a future
        reordering cannot turn a diagnostic into a run-aborting fault.
        """
        try:
            _, feed = resolve(work_item)
        except LookupError:
            return None
        return urlsplit(feed.url).hostname

    def prepare(work_item: WorkItem) -> TaskPreparation:
        """Application thread. The handler's only access to SQLite."""
        feed_identity_id, feed = resolve(work_item)
        etag, last_modified = latest_validators(
            connection,
            feed_identity_id=feed_identity_id,
            requested_url=feed.url,
        )
        return TaskPreparation(
            payload=FeedCall(
                feed=feed,
                validators=FeedValidators(etag=etag, last_modified=last_modified),
            )
        )

    return TaskHandler(
        task_type=FETCH_FEED_TASK_TYPE,
        provider=PROVIDER,
        operation=FETCH_FEED_OPERATION,
        execute=_execute_for(client),
        prepare=prepare,
        persist=_persist_for(connection, resolve, on_source_items),
        persist_failure=_persist_failure_for(connection, resolve),
        destination_host=destination_host,
        # A feed fetch costs no money, so it reserves nothing against the run
        # budget. Reserving zero is not the same as not reserving: the attempt
        # row is still written and still counted.
        reserved_nano_usd=0,
    )
