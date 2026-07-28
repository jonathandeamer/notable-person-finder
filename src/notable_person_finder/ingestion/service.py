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
import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

from notable_person_finder.config.models import FeedConfig, FeedsConfig
from notable_person_finder.ingestion.repository import (
    feed_identities,
    latest_validators,
    upsert_feed_identity,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.feeds import (
    PROVIDER,
    FeedClient,
    FeedValidators,
    NotModified,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import Clock, utc_timestamp
from notable_person_finder.runs.engine import TaskHandler, TaskOutcome
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
    has no configured URL to fingerprint from, and its identity row holds the
    URL it was last seeded with -- the very URL its outstanding item was
    fingerprinted from, because the loop above refreshes an identity only while
    its feed is still enabled.

    `supersede_work` touches only `pending` and `deferred`, so a feed's
    completed history is never rewritten by switching the feed off.
    """
    for identity in feed_identities(connection):
        if identity.key in enabled_keys:
            continue
        repository.supersede_work(
            connection,
            task_type=FETCH_FEED_TASK_TYPE,
            fingerprint=_fingerprint(feed_key=identity.key, url=identity.current_url),
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


def build_fetch_handler(
    connection: sqlite3.Connection,
    *,
    client: FeedClient,
    feeds: FeedsConfig,
) -> TaskHandler:
    """The `fetch_feed` handler: one feed fetch, split across the two threads."""
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

    def prepare(work_item: WorkItem) -> object:
        """Application thread. The handler's only access to SQLite."""
        feed_identity_id, feed = resolve(work_item)
        etag, last_modified = latest_validators(
            connection, feed_identity_id=feed_identity_id
        )
        return FeedCall(
            feed=feed,
            validators=FeedValidators(etag=etag, last_modified=last_modified),
        )

    return TaskHandler(
        task_type=FETCH_FEED_TASK_TYPE,
        provider=PROVIDER,
        operation=FETCH_FEED_OPERATION,
        execute=_execute_for(client),
        prepare=prepare,
        destination_host=destination_host,
        # A feed fetch costs no money, so it reserves nothing against the run
        # budget. Reserving zero is not the same as not reserving: the attempt
        # row is still written and still counted.
        reserved_nano_usd=0,
    )
