"""Tests for feed seeding and the `fetch_feed` handler.

Every test here is offline. The feed client is always a fake, because what is
under test is the *handler* -- which thread reads SQLite, what a result maps
onto, and what seeding schedules -- not the adapter, which `test_feeds.py`
covers against real parsing.

Two depths are used deliberately:

* Most handler assertions call `handler.prepare` and `handler.execute`
  directly, so the mapping from one feed result to one `TaskOutcome` is
  observed without an engine in the way.
* The retry dispositions drive a **real** `RunEngine`. Whether a failure
  settles `deferred` or `failed_permanent` is decided by `RetryPolicy` and the
  engine's action mapping, not by anything in `service.py`, so asserting it
  against a stub would only restate this module's own hopes.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import sqlite3
from collections.abc import Sequence

import pytest

from notable_person_finder.config.models import (
    FeedConfig,
    FeedsConfig,
    RetryConfig,
)
from notable_person_finder.ingestion import service
from notable_person_finder.ingestion.repository import record_fetch
from notable_person_finder.ingestion.service import (
    FETCH_FEED_TASK_TYPE,
    FeedCall,
    NotInspected,
    build_fetch_handler,
    build_seed_hook,
    seed_feeds,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.feeds import (
    FeedEntry,
    FeedValidators,
    Modified,
    NotModified,
)
from notable_person_finder.runs import repository
from notable_person_finder.runs.clock import FakeClock, utc_timestamp
from notable_person_finder.runs.engine import (
    ReportArtifact,
    RunEngine,
    RunReport,
    TaskHandler,
)
from notable_person_finder.runs.models import RunState, WorkItem, WorkState
from notable_person_finder.runs.retry import RetryPolicy
from notable_person_finder.runs.scheduler import BoundedScheduler
from tests.ingestion.helpers import immediate, insert_run, moment

# --- fixtures and fakes --------------------------------------------------------


def feeds_config(*specs: tuple[str, str, bool]) -> FeedsConfig:
    """A `FeedsConfig` from `(key, url, enabled)` triples."""
    return FeedsConfig(
        schema_version=1,
        feeds=tuple(
            FeedConfig(key=key, label=f"{key} label", url=url, enabled=enabled)
            for key, url, enabled in specs
        ),
    )


ALPHA = ("alpha", "https://alpha.example.com/feed.xml", True)
BETA = ("beta", "https://beta.example.com/feed.xml", True)


class FakeFeedClient:
    """Returns a canned result, or raises a canned failure, and records the call."""

    def __init__(
        self,
        *,
        result: NotModified | Modified | None = None,
        error: BaseException | None = None,
    ) -> None:
        self._result = result
        self._error = error
        self.calls: list[tuple[FeedConfig, FeedValidators]] = []

    def fetch_feed(
        self, feed: FeedConfig, validators: FeedValidators
    ) -> NotModified | Modified:
        self.calls.append((feed, validators))
        if self._error is not None:
            raise self._error
        assert self._result is not None
        return self._result


def modified_result(
    *, url: str = ALPHA[1], entries: int = 1, response_bytes: int = 512
) -> Modified:
    return Modified(
        requested_url=url,
        final_url=url,
        redirect_chain=(),
        status_code=200,
        validators=FeedValidators(
            etag='"e1"', last_modified="Wed, 22 Jul 2026 06:00:00 GMT"
        ),
        feed_type="rss20",
        source_title="Alpha",
        source_link="https://alpha.example.com/",
        source_updated_raw=None,
        entries=tuple(
            FeedEntry(
                entry_id=f"entry-{index}",
                url=f"https://alpha.example.com/{index}",
                title=f"Title {index}",
                summary=None,
                content=None,
                author=None,
                published_raw=None,
                updated_raw=None,
            )
            for index in range(entries)
        ),
        warnings=(),
        response_bytes=response_bytes,
    )


def not_modified_result(*, url: str = ALPHA[1]) -> NotModified:
    return NotModified(
        requested_url=url,
        final_url=url,
        redirect_chain=(),
        status_code=304,
        validators=FeedValidators(etag='"e1"', last_modified=None),
    )


def work_items(
    connection: sqlite3.Connection, *, task_type: str = FETCH_FEED_TASK_TYPE
) -> list[sqlite3.Row]:
    return list(
        connection.execute(
            "SELECT * FROM work_item WHERE task_type = ? ORDER BY id",
            (task_type,),
        )
    )


def identity_keys(connection: sqlite3.Connection) -> list[str]:
    return [
        row["key"]
        for row in connection.execute("SELECT key FROM feed_identity ORDER BY key")
    ]


def claimed_item(
    connection: sqlite3.Connection, *, run_id: int, index: int = 0
) -> WorkItem:
    """The seeded work items, claimed so `prepare` sees what the engine sees."""
    batch = repository.claim_batch(
        connection,
        run_id=run_id,
        now=moment(1),
        task_types=[FETCH_FEED_TASK_TYPE],
        limit=10,
    )
    return batch[index]


# --- seeding -------------------------------------------------------------------


def test_seeding_schedules_one_required_item_per_enabled_feed(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)

    seed_feeds(
        connection,
        feeds=feeds_config(
            ALPHA, BETA, ("gamma", "https://gamma.example.com/f.xml", False)
        ),
        run_id=run_id,
        now=moment(),
    )

    rows = work_items(connection)
    assert len(rows) == 2
    for row in rows:
        assert row["subject_kind"] == "feed_identity"
        assert row["required"] == 1
        assert row["state"] == "pending"
        assert row["created_by_run_id"] == run_id
        assert row["subject_id"] is not None
        # Literal, not `FETCH_FEED_PRIORITY`: `claim_batch` orders by
        # `priority ASC`, and required-before-optional exists nowhere except in
        # the values seeding writes here. A later task type must be placed
        # relative to a known number, so the number is the contract.
        assert row["priority"] == 10


def test_seeding_touches_no_identity_for_a_disabled_feed(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)

    seed_feeds(
        connection,
        feeds=feeds_config(ALPHA, ("gamma", "https://gamma.example.com/f.xml", False)),
        run_id=run_id,
        now=moment(),
    )

    # The positive control for the negative half: `alpha` proves an identity
    # row *is* written by this call, so `gamma`'s absence is an exclusion
    # rather than seeding having written nothing at all.
    assert identity_keys(connection) == ["alpha"]


def test_seeding_refreshes_an_existing_identity_rather_than_forking_it(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())

    moved = ("alpha", "https://alpha.example.com/feed-v2.xml", True)
    seed_feeds(connection, feeds=feeds_config(moved), run_id=run_id, now=moment(60))

    rows = list(connection.execute("SELECT * FROM feed_identity"))
    assert len(rows) == 1
    assert rows[0]["current_url"] == moved[1]
    assert rows[0]["first_seen_at"] == moment()
    assert rows[0]["last_seen_at"] == moment(60)


def test_reseeding_does_not_duplicate_an_already_active_item(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    first = [row["id"] for row in work_items(connection)]

    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment(60))

    assert [row["id"] for row in work_items(connection)] == first


def test_a_succeeded_item_from_a_previous_run_does_not_block_a_new_one(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=first_run, now=moment())
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    connection.commit()

    second_run = insert_run(connection)
    seed_feeds(
        connection, feeds=feeds_config(ALPHA), run_id=second_run, now=moment(86400)
    )

    rows = work_items(connection)
    assert [row["state"] for row in rows] == ["succeeded", "pending"]
    # Yesterday's terminal item and today's fresh one share a fingerprint: the
    # identity of the *request* has not changed, only its scheduling.
    assert rows[0]["fingerprint"] == rows[1]["fingerprint"]


def test_a_deferred_item_is_reused_rather_than_duplicated(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=first_run, now=moment())
    connection.execute("UPDATE work_item SET state = 'deferred'")
    connection.commit()
    existing = [row["id"] for row in work_items(connection)]

    second_run = insert_run(connection)
    seed_feeds(
        connection, feeds=feeds_config(ALPHA), run_id=second_run, now=moment(86400)
    )

    rows = work_items(connection)
    assert [row["id"] for row in rows] == existing
    assert rows[0]["state"] == "deferred"


def test_disabling_a_feed_supersedes_its_active_work(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA, BETA), run_id=run_id, now=moment())

    disabled = ("alpha", ALPHA[1], False)
    seed_feeds(
        connection, feeds=feeds_config(disabled, BETA), run_id=run_id, now=moment(60)
    )

    rows = work_items(connection)
    states = {row["subject_id"]: (row["state"], row["reason"]) for row in rows}
    alpha_identity = connection.execute(
        "SELECT id FROM feed_identity WHERE key = 'alpha'"
    ).fetchone()["id"]
    beta_identity = connection.execute(
        "SELECT id FROM feed_identity WHERE key = 'beta'"
    ).fetchone()["id"]
    assert states[alpha_identity] == ("superseded", "feed disabled")
    assert states[beta_identity][0] == "pending"


def test_removing_a_feed_from_configuration_supersedes_its_active_work(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA, BETA), run_id=run_id, now=moment())

    # `alpha` is gone from the file entirely, so its fingerprint cannot be
    # recomputed from configuration -- only from its stored identity.
    seed_feeds(connection, feeds=feeds_config(BETA), run_id=run_id, now=moment(60))

    rows = work_items(connection)
    superseded = [row for row in rows if row["state"] == "superseded"]
    assert len(superseded) == 1
    assert superseded[0]["reason"] == "feed disabled"


def test_superseding_leaves_a_terminal_item_alone(
    connection: sqlite3.Connection,
) -> None:
    """A feed's history is not rewritten when the feed is switched off.

    `supersede_work` only touches `pending` and `deferred`, and this pins that
    seeding relies on that rather than on a state filter of its own.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    connection.commit()

    seed_feeds(
        connection,
        feeds=feeds_config(("alpha", ALPHA[1], False)),
        run_id=run_id,
        now=moment(60),
    )

    assert [row["state"] for row in work_items(connection)] == ["succeeded"]


# --- the fingerprint -----------------------------------------------------------


def test_the_fingerprint_is_sha256_over_canonical_json(
    connection: sqlite3.Connection,
) -> None:
    """Pinned as a literal digest, deliberately.

    Recomputing it here with the same `json.dumps` call the source uses would
    make this a change-detector against itself: any edit to the serialization --
    a separator, a field name, an added key -- would move both sides together
    and the test would still pass. A literal makes the stored fingerprint a
    stable value, which is what it has to be, since it is the identity under
    which a work item is deduplicated across runs. If this assertion fails, ask
    whether existing rows were just orphaned before changing the constant.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())

    assert service.PARSER_VERSION == 1, "the literal below is version-specific"
    assert (
        work_items(connection)[0]["fingerprint"]
        == "c86875c5422895e60099525a82f7e677d607808b00847af4ee4934a6448ced52"
    )


def test_the_canonical_json_is_compact_and_key_sorted(
    connection: sqlite3.Connection,
) -> None:
    """The serialization the digest above commits to, stated independently.

    Kept apart from the literal so a serialization change is legible as such
    rather than as an opaque hash mismatch. `sort_keys=True` happens to be inert
    for the current field names, which are already alphabetical -- so this
    asserts the *contract* rather than pretending the flag is load-bearing
    today. Add a field out of alphabetical order and it becomes so.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())

    canonical = json.dumps(
        {"feed_key": "alpha", "parser_version": 1, "url": ALPHA[1]},
        sort_keys=True,
        separators=(",", ":"),
    )
    # No whitespace, so two writers cannot produce different bytes for one feed.
    assert ", " not in canonical
    assert '": ' not in canonical
    assert (
        work_items(connection)[0]["fingerprint"]
        == hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    )


def test_a_changed_feed_url_produces_a_new_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    original = work_items(connection)[0]["fingerprint"]

    seed_feeds(
        connection,
        feeds=feeds_config(("alpha", "https://alpha.example.com/feed-v2.xml", True)),
        run_id=run_id,
        now=moment(60),
    )

    fingerprints = [row["fingerprint"] for row in work_items(connection)]
    assert len(fingerprints) == 2
    assert fingerprints[0] == original
    assert fingerprints[1] != original


def test_a_changed_parser_version_produces_a_new_fingerprint(
    connection: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A parser change legitimately reschedules; a comment change does not.

    Without the parser version in the fingerprint, a feed already fetched
    today could not be re-parsed today by improved parsing code.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    original = work_items(connection)[0]["fingerprint"]
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    connection.commit()

    monkeypatch.setattr(service, "PARSER_VERSION", service.PARSER_VERSION + 1)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment(60))

    fingerprints = [row["fingerprint"] for row in work_items(connection)]
    assert len(fingerprints) == 2
    assert fingerprints[1] != original


def test_the_label_is_not_part_of_the_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    """Relabelling a feed is cosmetic and must not reschedule a fetch."""
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    original = work_items(connection)[0]["fingerprint"]

    relabelled = FeedsConfig(
        schema_version=1,
        feeds=(FeedConfig(key="alpha", label="Renamed", url=ALPHA[1], enabled=True),),
    )
    seed_feeds(connection, feeds=relabelled, run_id=run_id, now=moment(60))

    rows = work_items(connection)
    assert len(rows) == 1
    assert rows[0]["fingerprint"] == original


# --- the seed hook -------------------------------------------------------------


def test_the_seed_hook_takes_only_a_run_id_and_reads_its_clock(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    clock = FakeClock()
    hook = build_seed_hook(connection, feeds=feeds_config(ALPHA), clock=clock)

    clock.advance(120)
    hook(run_id)

    rows = work_items(connection)
    assert len(rows) == 1
    assert rows[0]["created_at"] == utc_timestamp(clock.now())


# --- prepare -------------------------------------------------------------------


def test_prepare_returns_empty_validators_for_a_first_fetch(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert handler.prepare is not None

    call = handler.prepare(claimed_item(connection, run_id=run_id))

    assert isinstance(call, FeedCall)
    assert call.feed.key == "alpha"
    assert call.validators == FeedValidators(etag=None, last_modified=None)


def test_prepare_returns_the_stored_validators_after_a_successful_fetch(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    identity_id = connection.execute("SELECT id FROM feed_identity").fetchone()["id"]
    with immediate(connection):
        record_fetch(
            connection,
            feed_identity_id=identity_id,
            run_id=run_id,
            requested_at=moment(),
            requested_url=ALPHA[1],
            resolved_url=ALPHA[1],
            redirect_chain_json=None,
            http_status=200,
            etag='"stored-etag"',
            last_modified="Wed, 22 Jul 2026 06:00:00 GMT",
            outcome="modified",
            feed_type="rss20",
            parse_outcome="ok",
            parser_warnings_json=None,
            failure_category=None,
            entry_count=3,
            response_bytes=1024,
        )
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert handler.prepare is not None

    call = handler.prepare(claimed_item(connection, run_id=run_id))

    assert isinstance(call, FeedCall)
    assert call.validators == FeedValidators(
        etag='"stored-etag"', last_modified="Wed, 22 Jul 2026 06:00:00 GMT"
    )


def test_prepare_does_not_offer_old_validators_after_the_configured_url_moves(
    connection: sqlite3.Connection,
) -> None:
    """A validator describes one requested representation, not a feed key.

    The stable feed identity survives configuration changes, but validators
    learned from the old URL cannot make a 304 from the new URL meaningful.
    This drives the real seed -> repository history -> prepare path so a URL
    filter added only at a test-facing helper would not satisfy it.
    """
    first_run = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=first_run, now=moment())
    identity_id = connection.execute("SELECT id FROM feed_identity").fetchone()["id"]
    with immediate(connection):
        record_fetch(
            connection,
            feed_identity_id=identity_id,
            run_id=first_run,
            requested_at=moment(),
            requested_url=ALPHA[1],
            resolved_url=ALPHA[1],
            redirect_chain_json=None,
            http_status=200,
            etag='"old-url-etag"',
            last_modified="Wed, 22 Jul 2026 06:00:00 GMT",
            outcome="modified",
            feed_type="rss20",
            parse_outcome="ok",
            parser_warnings_json=None,
            failure_category=None,
            entry_count=3,
            response_bytes=1024,
        )
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    connection.commit()

    moved = ("alpha", "https://alpha.example.com/feed-v2.xml", True)
    second_run = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(moved), run_id=second_run, now=moment())
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result(url=moved[1])),
        feeds=feeds_config(moved),
    )
    assert handler.prepare is not None

    call = handler.prepare(claimed_item(connection, run_id=second_run))

    assert isinstance(call, FeedCall)
    assert call.feed.url == moved[1]
    assert call.validators == FeedValidators(etag=None, last_modified=None)


def test_prepare_refuses_an_item_whose_feed_left_configuration(
    connection: sqlite3.Connection,
) -> None:
    """An unroutable item fails loudly rather than fetching the wrong feed.

    Seeding supersedes such items before the claim loop reaches them, so this
    is unreachable in an ordinary run. It must still not fall back to some
    other feed: the engine settles a raising `prepare` as `failed_permanent`
    for that item alone, which is the right disposition for work whose subject
    no longer exists.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    item = claimed_item(connection, run_id=run_id)
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(BETA),
    )
    assert handler.prepare is not None

    with pytest.raises(LookupError):
        handler.prepare(item)


def test_prepare_refuses_an_item_that_names_no_feed(
    connection: sqlite3.Connection,
) -> None:
    """A subject-less item must not be routed to a guessed feed.

    `WorkItem.subject_id` is nullable for the whole engine, so the type permits
    this even though seeding always sets it. Defaulting to the first configured
    feed would attribute one publisher's entries to another, which is
    unrecoverable once stored -- so the guard has to refuse rather than repair.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    item = claimed_item(connection, run_id=run_id)
    subjectless = dataclasses.replace(item, subject_id=None)
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert handler.prepare is not None
    # Positive control: the same item with its subject intact resolves, so the
    # refusal below is caused by the missing subject and not by the fixture.
    assert isinstance(handler.prepare(item), FeedCall)

    with pytest.raises(LookupError):
        handler.prepare(subjectless)


def test_prepare_refuses_a_feed_that_is_present_but_disabled(
    connection: sqlite3.Connection,
) -> None:
    """`enabled = false` is an instruction, not a comment.

    Distinct from the feed-absent case above: this feed *is* in the file, so a
    guard that only checked for absence would fetch a feed the operator
    explicitly switched off. Reachable in a way the absent case is not -- an
    item seeded while the feed was enabled, then claimed after it was disabled
    in the same run's configuration.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    item = claimed_item(connection, run_id=run_id)
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(("alpha", ALPHA[1], False)),
    )
    assert handler.prepare is not None

    with pytest.raises(LookupError):
        handler.prepare(item)


def test_prepare_refuses_an_item_whose_identity_is_not_stored(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    item = claimed_item(connection, run_id=run_id)
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert handler.prepare is not None

    with pytest.raises(LookupError):
        handler.prepare(dataclasses.replace(item, subject_id=9999))


# --- execute: the outcome mapping ----------------------------------------------


def handler_for(
    client: FakeFeedClient, connection: sqlite3.Connection
) -> tuple[TaskHandler, WorkItem]:
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    handler = build_fetch_handler(connection, client=client, feeds=feeds_config(ALPHA))
    return handler, claimed_item(connection, run_id=run_id)


def test_a_modified_result_succeeds_and_carries_the_result_as_payload(
    connection: sqlite3.Connection,
) -> None:
    result = modified_result(entries=3, response_bytes=2048)
    client = FakeFeedClient(result=result)
    handler, item = handler_for(client, connection)

    outcome = handler.execute(
        item,
        1,
        FeedCall(
            feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
        ),
    )

    assert outcome.state is WorkState.SUCCEEDED
    assert outcome.reason is None
    assert outcome.payload is result
    assert outcome.response_bytes == 2048


def test_a_not_modified_result_succeeds_with_a_stable_reason(
    connection: sqlite3.Connection,
) -> None:
    result = not_modified_result()
    client = FakeFeedClient(result=result)
    handler, item = handler_for(client, connection)

    outcome = handler.execute(
        item,
        1,
        FeedCall(
            feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
        ),
    )

    assert outcome.state is WorkState.SUCCEEDED
    assert outcome.reason == "not modified"
    # The fetch row is still written, so the result has to reach `persist`.
    assert outcome.payload is result


def test_execute_passes_the_prepared_feed_and_validators_to_the_client(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(result=modified_result())
    handler, item = handler_for(client, connection)
    validators = FeedValidators(etag='"e0"', last_modified=None)

    handler.execute(
        item, 1, FeedCall(feed=feeds_config(ALPHA).feeds[0], validators=validators)
    )

    assert len(client.calls) == 1
    called_feed, called_validators = client.calls[0]
    assert called_feed.key == "alpha"
    assert called_validators is validators


def test_execute_performs_exactly_one_call_per_invocation(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(result=modified_result())
    handler, item = handler_for(client, connection)
    call = FeedCall(
        feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
    )

    handler.execute(item, 1, call)

    assert len(client.calls) == 1


@pytest.mark.parametrize(
    ("category", "reason"),
    [
        (FailureCategory.RESPONSE_TOO_LARGE, "not inspected: response too large"),
        (FailureCategory.UNSUPPORTED_CONTENT, "not inspected: unsupported content"),
    ],
)
def test_an_uninspectable_response_defers_without_raising(
    connection: sqlite3.Connection, category: FailureCategory, reason: str
) -> None:
    """Neither category is retryable and neither is worth a repeat call.

    Left to propagate, both would settle `failed_permanent` -- a response that
    is merely too big is not a permanent answer about the publisher. Returning
    `DEFERRED` directly also spends no further attempt, because the response
    will be exactly as large next time.
    """
    failure = ProviderFailure(
        category,
        provider="feeds",
        operation="fetch_feed",
        status_code=200,
        detail="body exceeded 5242880 bytes",
    )
    client = FakeFeedClient(error=failure)
    handler, item = handler_for(client, connection)

    outcome = handler.execute(
        item,
        1,
        FeedCall(
            feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
        ),
    )

    assert outcome.state is WorkState.DEFERRED
    assert outcome.reason == reason
    assert isinstance(outcome.payload, NotInspected)
    assert outcome.payload.category is category
    assert outcome.payload.requested_url == ALPHA[1]
    # The sanitized detail travels to `persist`, which is the only quantitative
    # fact available about a response nobody read -- and the only one available
    # at all in production, where the transport raises `response_too_large` from
    # its streaming read with no status code.
    assert outcome.payload.detail == "body exceeded 5242880 bytes"
    # It must reach the payload and never the reason, which is a `GROUP BY` key.
    assert outcome.reason is not None
    assert "5242880" not in outcome.reason


def test_an_uninspectable_response_records_no_entry_count(
    connection: sqlite3.Connection,
) -> None:
    """The honest record is "not inspected", never "a feed with no entries".

    The positive control is `modified_result(entries=0)`: an empty feed really
    was read and really did have nothing in it, so `Modified` is the payload
    type that can say so. `NotInspected` must not be confusable with it.
    """
    call = FeedCall(
        feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
    )
    empty = FakeFeedClient(result=modified_result(entries=0))
    handler, item = handler_for(empty, connection)
    read_but_empty = handler.execute(item, 1, call)
    assert isinstance(read_but_empty.payload, Modified)
    assert read_but_empty.payload.entries == ()

    refused = build_fetch_handler(
        connection,
        client=FakeFeedClient(
            error=ProviderFailure(
                FailureCategory.RESPONSE_TOO_LARGE,
                provider="feeds",
                operation="fetch_feed",
            )
        ),
        feeds=feeds_config(ALPHA),
    )
    outcome = refused.execute(item, 1, call)

    assert not isinstance(outcome.payload, Modified)


@pytest.mark.parametrize(
    "category",
    [
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.MALFORMED_RESPONSE,
        FailureCategory.ACCESS_DENIED,
        FailureCategory.INTERNAL,
    ],
)
def test_every_other_failure_propagates_for_the_engine_to_adjudicate(
    connection: sqlite3.Connection, category: FailureCategory
) -> None:
    """Retry policy lives in the engine; the handler never pre-empts it."""
    failure = ProviderFailure(category, provider="feeds", operation="fetch_feed")
    client = FakeFeedClient(error=failure)
    handler, item = handler_for(client, connection)

    with pytest.raises(ProviderFailure) as raised:
        handler.execute(
            item,
            1,
            FeedCall(
                feed=feeds_config(ALPHA).feeds[0], validators=FeedValidators(None, None)
            ),
        )
    assert raised.value is failure


def test_execute_translates_a_wrong_prepared_value_into_a_provider_failure(
    connection: sqlite3.Connection,
) -> None:
    """A wiring fault must not escape a worker thread untranslated.

    Unreachable while `prepare` is the only producer of `prepared`. It is
    translated rather than raised bare because the engine's execute phase
    deliberately does not isolate a non-`ProviderFailure` (`engine.py` re-raises
    it out of `unwrap`), so a `TypeError` here would propagate out of the run
    and discard every sibling feed's results. `INTERNAL` is not retryable, so
    this settles the one item permanently instead.
    """
    client = FakeFeedClient(result=modified_result())
    handler, item = handler_for(client, connection)

    with pytest.raises(ProviderFailure) as raised:
        handler.execute(item, 1, object())

    assert raised.value.category is FailureCategory.INTERNAL
    assert raised.value.retryable is False
    # No call was attempted with an unusable input.
    assert client.calls == []


# --- the attempt row ------------------------------------------------------------


def test_a_not_inspected_deferral_keeps_its_evidence_on_the_attempt_row(
    connection: sqlite3.Connection,
) -> None:
    """The one place a refused response survives in the durable attempt log.

    A `DEFERRED` outcome is still an outcome, so the engine records the attempt
    as `succeeded` with a NULL `failure_category` -- and migration `0002`'s CHECK
    on `attempt` forbids a `failure_category` unless `outcome = 'failed'`, so
    the category cannot go in its own column on this path at all. That means
    `operational_failures_for_run` (which counts `outcome = 'failed'`) reports
    zero, and the digest's operational-failure count and failure-category
    breakdown both omit a feed nobody could read. `detail_json` is what stops
    the evidence disappearing entirely.

    Asserted, rather than left as a comment, because the reporting consequence
    is the kind of thing a later task would otherwise rediscover as a bug.
    """
    client = FakeFeedClient(
        error=ProviderFailure(
            FailureCategory.RESPONSE_TOO_LARGE,
            provider="feeds",
            operation="fetch_feed",
            detail="feeds.fetch_feed body exceeded 5242880 bytes",
        )
    )

    report = run_engine_with(connection, client, max_attempts=3)

    attempts = repository.attempts_for_run(connection, run_id=report.run_id)
    assert len(attempts) == 1
    row = connection.execute(
        "SELECT outcome, failure_category, detail_json FROM attempt"
    ).fetchone()
    # The shape the engine and the schema force, pinned so it is a known
    # limitation rather than a surprise.
    assert row["outcome"] == "succeeded"
    assert row["failure_category"] is None
    # ...and the evidence that survives it.
    stored = json.loads(row["detail_json"])
    assert stored["not_inspected"] == "response_too_large"
    assert stored["detail"] == "feeds.fetch_feed body exceeded 5242880 bytes"
    # The failure category reaches no operational counter, which is exactly why
    # the assertions above matter.
    assert report.failure_categories == {}
    assert report.counters.operational_failures == 0


def test_the_attempt_row_records_the_feed_host(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(result=modified_result())

    run_engine_with(connection, client, max_attempts=3)

    row = connection.execute("SELECT destination_host FROM attempt").fetchone()
    assert row["destination_host"] == "alpha.example.com"


def test_destination_host_is_none_rather_than_raising_for_an_unroutable_item(
    connection: sqlite3.Connection,
) -> None:
    """A diagnostic column must not be able to abort a run.

    The engine calls `destination_host` *outside* the try/except that isolates
    `prepare`, so raising here would propagate out of `RunEngine.execute`, leave
    the run `running`, and write no digest -- losing every sibling feed's
    results in order to fail at labelling one attempt row.
    """
    run_id = insert_run(connection)
    seed_feeds(connection, feeds=feeds_config(ALPHA), run_id=run_id, now=moment())
    item = claimed_item(connection, run_id=run_id)
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(BETA),
    )
    assert handler.destination_host is not None
    # Positive control: with the feed configured, a host really is produced, so
    # the None below is the refusal path and not an always-None function.
    routable = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert routable.destination_host is not None
    assert routable.destination_host(item) == "alpha.example.com"

    assert handler.destination_host(item) is None


# --- the thread split ----------------------------------------------------------


def closure_values(function: object) -> list[object]:
    cells = getattr(function, "__closure__", None)
    if cells is None:
        return []
    values: list[object] = []
    for cell in cells:
        try:
            values.append(cell.cell_contents)
        except ValueError:  # pragma: no cover - an unfilled cell holds nothing
            continue
    return values


def test_execute_closes_over_no_database_connection(
    connection: sqlite3.Connection,
) -> None:
    """`execute` runs on a worker thread, where the connection may not be used.

    The assertion is structural rather than behavioural: a worker cannot reach
    SQLite if the callable it is handed has no path to a connection. `prepare`
    is the positive control -- it *does* hold one, which proves this test can
    tell the two apart rather than passing because neither closure was
    inspected.
    """
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )
    assert handler.prepare is not None

    assert any(value is connection for value in closure_values(handler.prepare))
    assert not any(
        isinstance(value, sqlite3.Connection)
        for value in closure_values(handler.execute)
    )


def test_the_handler_declares_the_feed_provider_and_task_type(
    connection: sqlite3.Connection,
) -> None:
    handler = build_fetch_handler(
        connection,
        client=FakeFeedClient(result=modified_result()),
        feeds=feeds_config(ALPHA),
    )

    # Deliberately literal rather than compared against the module's own
    # constants, which would only assert the code equals itself. `task_type` is
    # stored in `work_item.task_type`, so renaming it orphans every row an
    # earlier run wrote: the constant has to be a tripwire, not a variable.
    assert handler.task_type == "fetch_feed"
    assert FETCH_FEED_TASK_TYPE == "fetch_feed"
    assert handler.provider == "feeds"
    assert handler.operation == "fetch_feed"
    # A feed fetch is free, so it reserves nothing against the run budget.
    assert handler.reserved_nano_usd == 0


# --- retry dispositions, through a real engine ---------------------------------


def memory_reporter(report: RunReport) -> ReportArtifact:
    return ReportArtifact(path=None, sha256=None, markdown="")


def run_engine_with(
    connection: sqlite3.Connection,
    client: FakeFeedClient,
    *,
    max_attempts: int,
    feeds: FeedsConfig | None = None,
) -> RunReport:
    configured = feeds or feeds_config(ALPHA)
    clock = FakeClock()
    handler = build_fetch_handler(connection, client=client, feeds=configured)
    with BoundedScheduler(max_workers=2) as scheduler:
        engine = RunEngine(
            connection,
            retry=RetryPolicy(
                RetryConfig(max_attempts=max_attempts, jitter_ratio=0.0), clock=clock
            ),
            scheduler=scheduler,
            clock=clock,
            timezone="Europe/Paris",
            window_start=moment(-86400),
            budget_limit_nano_usd=None,
            snapshot_fingerprint="a" * 64,
            snapshot_json="{}",
            reporter=memory_reporter,
        )
        return engine.execute(
            {FETCH_FEED_TASK_TYPE: handler},
            seed=build_seed_hook(connection, feeds=configured, clock=clock),
        )


def settled(connection: sqlite3.Connection) -> Sequence[sqlite3.Row]:
    return work_items(connection)


def test_a_seeded_run_fetches_every_enabled_feed_and_succeeds(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(result=modified_result())

    report = run_engine_with(
        connection, client, max_attempts=3, feeds=feeds_config(ALPHA, BETA)
    )

    assert len(client.calls) == 2
    assert {row["state"] for row in settled(connection)} == {"succeeded"}
    assert report.counters.required_succeeded == 2
    assert report.state is RunState.COMPLETE


def test_an_exhausted_transient_failure_defers_the_feed(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(
        error=ProviderFailure(
            FailureCategory.NETWORK, provider="feeds", operation="fetch_feed"
        )
    )

    run_engine_with(connection, client, max_attempts=2)

    rows = settled(connection)
    assert [row["state"] for row in rows] == ["deferred"]
    assert rows[0]["reason"] == "exhausted transient failure: network"
    # Deferred, not dead: tomorrow's run can claim it again.
    assert len(client.calls) == 2


def test_a_malformed_payload_defers_rather_than_dropping_the_publisher(
    connection: sqlite3.Connection,
) -> None:
    """`retryable=True` on `MALFORMED_RESPONSE` is what makes this a deferral.

    Without the adapter's override the category is not in
    `RETRYABLE_CATEGORIES`, the policy answers `PERMANENT`, and one CDN error
    page would retire the feed's work item as `failed_permanent`.
    """
    client = FakeFeedClient(
        error=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="feeds",
            operation="fetch_feed",
            retryable=True,
        )
    )

    run_engine_with(connection, client, max_attempts=1)

    rows = settled(connection)
    assert [row["state"] for row in rows] == ["deferred"]
    assert rows[0]["state"] != WorkState.FAILED_PERMANENT.value
    assert rows[0]["reason"] == "exhausted transient failure: malformed_response"


def test_a_repeated_malformed_payload_is_foreclosed_at_two_or_more_attempts(
    connection: sqlite3.Connection,
) -> None:
    """The malformed ceiling outranks the adapter's retryable override.

    `RetryPolicy.decide` forecloses a *second* `malformed_response` with
    `PERMANENT` regardless of remaining attempt budget, so at any
    `max_attempts >= 2` a feed serving an error page twice in one run settles
    `failed_permanent` rather than `deferred` -- the opposite of the
    single-attempt case above, and not what the adapter's `retryable=True`
    override reads as promising.

    Pinned rather than corrected: the foreclosure rule was adjudicated for the
    LLM path and changing it is a design decision beyond this handler. The
    practical exposure is bounded, because seeding schedules a fresh work item
    the next day -- a `failed_permanent` item is terminal and does not block
    one. What differs is only how the run reports the feed, so this test
    exists to keep that difference visible instead of latent.
    """
    client = FakeFeedClient(
        error=ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider="feeds",
            operation="fetch_feed",
            retryable=True,
        )
    )

    run_engine_with(connection, client, max_attempts=3)

    rows = settled(connection)
    assert [row["state"] for row in rows] == ["failed_permanent"]
    assert rows[0]["reason"] == "malformed_response"
    # Foreclosed after two calls, not after all three the budget allowed.
    assert len(client.calls) == 2


def test_an_uninspectable_response_defers_without_burning_attempts(
    connection: sqlite3.Connection,
) -> None:
    client = FakeFeedClient(
        error=ProviderFailure(
            FailureCategory.RESPONSE_TOO_LARGE,
            provider="feeds",
            operation="fetch_feed",
            status_code=200,
        )
    )

    report = run_engine_with(connection, client, max_attempts=3)

    rows = settled(connection)
    assert [row["state"] for row in rows] == ["deferred"]
    assert rows[0]["reason"] == "not inspected: response too large"
    # One call, though the budget allowed three: retrying cannot shrink a
    # response.
    assert len(client.calls) == 1
    assert len(repository.attempts_for_run(connection, run_id=report.run_id)) == 1
