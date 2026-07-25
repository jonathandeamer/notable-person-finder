from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.providers.failures import FailureCategory
from notable_person_finder.runs import repository
from notable_person_finder.runs.retry import AttemptRecord

NOW = "2026-07-25T06:00:00Z"
DONE = "2026-07-25T06:00:02Z"


@pytest.fixture
def connection(tmp_path: Path) -> sqlite3.Connection:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


@pytest.fixture
def context(connection: sqlite3.Connection) -> tuple[int, int]:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    run_id = repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        window_end=NOW,
        budget_limit_nano_usd=None,
        now=NOW,
    )
    work_id = repository.schedule_work(
        connection,
        task_type="search_coverage",
        subject_kind="person",
        subject_id=7,
        fingerprint="b" * 64,
        required=True,
        priority=100,
        eligible_at=NOW,
        run_id=run_id,
        now=NOW,
    )
    return run_id, work_id


def start(connection: sqlite3.Connection, context: tuple[int, int], ordinal: int) -> int:
    run_id, work_id = context
    function = (
        repository.claim_and_start_attempt
        if ordinal == 1
        else repository.start_attempt
    )
    return function(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="brave",
        operation="search_web",
        ordinal=ordinal,
        request_fingerprint="c" * 64,
        destination_host="api.search.brave.com",
        reserved_nano_usd=0,
        now=NOW,
    )


def test_attempt_is_created_in_flight_with_no_outcome(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] is None
    assert row["finished_at"] is None
    assert row["destination_host"] == "api.search.brave.com"


def test_finishing_a_successful_attempt_records_latency_and_bytes(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(ordinal=1, outcome="succeeded", latency_ms=250),
        response_bytes=4096,
        provider_request_id="req-abc",
        detail_json=None,
        now=DONE,
    )
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] == "succeeded"
    assert row["latency_ms"] == 250
    assert row["response_bytes"] == 4096
    assert row["provider_request_id"] == "req-abc"
    assert row["failure_category"] is None
    assert row["finished_at"] == DONE


def test_finishing_a_failed_attempt_records_the_typed_category(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(
            ordinal=1,
            outcome="failed",
            latency_ms=90,
            failure_category=FailureCategory.RATE_LIMIT,
            status_code=429,
            retry_after_ms=7000,
            detail="rate limited",
        ),
        response_bytes=None,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    row = connection.execute("SELECT * FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["outcome"] == "failed"
    assert row["failure_category"] == "rate_limit"
    assert row["provider_status"] == 429
    assert row["retry_after_ms"] == 7000


def test_a_retry_creates_a_new_ordinal_and_preserves_the_failure(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    first = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=first,
        record=AttemptRecord(
            ordinal=1,
            outcome="failed",
            latency_ms=10,
            failure_category=FailureCategory.TIMEOUT,
        ),
        response_bytes=None,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    second = start(connection, context, 2)
    repository.finish_attempt(
        connection,
        attempt_id=second,
        record=AttemptRecord(ordinal=2, outcome="succeeded", latency_ms=20),
        response_bytes=10,
        provider_request_id=None,
        detail_json=None,
        now=DONE,
    )
    outcomes = [
        row["outcome"]
        for row in connection.execute("SELECT outcome FROM attempt ORDER BY ordinal")
    ]
    assert outcomes == ["failed", "succeeded"]


def test_every_attempt_is_attributed_to_its_run_and_work_item(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    run_id, work_id = context
    start(connection, context, 1)
    row = repository.attempts_for_run(connection, run_id=run_id)[0]
    assert row["run_id"] == run_id
    assert row["work_item_id"] == work_id


def test_detail_json_round_trips_through_finish_attempt(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    # detail_json is reserved for immutable provider metadata; the transport
    # never passes response content into it. This test confirms the column
    # persists whatever it is given verbatim -- it does not itself enforce
    # that callers keep raw bodies out.
    attempt_id = start(connection, context, 1)
    repository.finish_attempt(
        connection,
        attempt_id=attempt_id,
        record=AttemptRecord(ordinal=1, outcome="succeeded", latency_ms=5),
        response_bytes=12,
        provider_request_id=None,
        detail_json='{"resolved_provider":"anthropic"}',
        now=DONE,
    )
    row = connection.execute("SELECT detail_json FROM attempt WHERE id = ?", (attempt_id,)).fetchone()
    assert row["detail_json"] == '{"resolved_provider":"anthropic"}'


def test_an_unfinished_attempt_survives_for_the_sweep(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    start(connection, context, 1)
    result = repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert result.attempts == 1
    assert (
        connection.execute("SELECT outcome FROM attempt").fetchone()["outcome"] == "interrupted"
    )


def test_next_ordinal_continues_after_an_interrupted_attempt(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    start(connection, context, 1)
    repository.sweep_interrupted(connection, now="2026-07-25T08:00:00Z")
    assert repository.next_attempt_ordinal(connection, work_item_id=context[1]) == 2


def test_first_attempt_updates_work_item_and_budget_columns(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    # Happy-path column check only. This does NOT prove atomicity -- three
    # separate autocommitted statements would pass it too. The atomicity
    # proof is `test_refused_first_reservation_leaves_work_pending_and_no_attempt`
    # below, which forces a mid-transaction failure and checks nothing stuck.
    run_id, work_id = context
    connection.execute(
        "UPDATE run SET budget_limit_nano_usd = 1000 WHERE id = ?", (run_id,)
    )
    connection.commit()
    repository.claim_and_start_attempt(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="openrouter",
        operation="generate_structured",
        ordinal=1,
        request_fingerprint="d" * 64,
        destination_host=None,
        reserved_nano_usd=400,
        now=NOW,
    )
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == "running"
    assert connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 400


def test_refused_first_reservation_leaves_work_pending_and_no_attempt(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    from notable_person_finder.runs.budget import BudgetExhausted

    run_id, work_id = context
    connection.execute(
        "UPDATE run SET budget_limit_nano_usd = 100 WHERE id = ?", (run_id,)
    )
    connection.commit()
    with pytest.raises(BudgetExhausted):
        repository.claim_and_start_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_id,
            provider="openrouter",
            operation="generate_structured",
            ordinal=1,
            request_fingerprint="e" * 64,
            destination_host=None,
            reserved_nano_usd=400,
            now=NOW,
        )
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == "pending"
    assert connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()["n"] == 0
    assert connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 0


def test_reservation_is_rolled_back_when_the_attempt_insert_fails(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    """Proves the reserve-then-insert half of the atomicity guarantee.

    Forces `_insert_attempt` to fail *after* `reserve_in_transaction` has
    already run (a duplicate ordinal violates `UNIQUE(work_item_id, ordinal)`)
    and asserts the reservation from that failed call did not stick.
    """
    run_id, work_id = context
    connection.execute(
        "UPDATE run SET budget_limit_nano_usd = 1000 WHERE id = ?", (run_id,)
    )
    connection.commit()
    repository.claim_and_start_attempt(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="brave",
        operation="search_web",
        ordinal=1,
        request_fingerprint="f" * 64,
        destination_host=None,
        reserved_nano_usd=100,
        now=NOW,
    )
    reserved_before = connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"]
    assert reserved_before == 100

    with pytest.raises(sqlite3.IntegrityError):
        repository.start_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_id,
            provider="brave",
            operation="search_web",
            ordinal=1,  # duplicate ordinal -> UNIQUE(work_item_id, ordinal) violation
            request_fingerprint="g" * 64,
            destination_host=None,
            reserved_nano_usd=200,
            now=NOW,
        )

    reserved_after = connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (run_id,)
    ).fetchone()["budget_reserved_nano_usd"]
    assert reserved_after == reserved_before


def test_claim_and_start_attempt_refuses_a_work_item_still_in_backoff(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    """`claim_and_start_attempt` must honor eligible_at like `claim_next` does.

    Parks the item in `deferred` with a future `eligible_at` (a retry
    backoff window) and confirms the shared claim predicate refuses it at a
    `now` that precedes that window, rather than authorizing a paid external
    call early.
    """
    run_id, work_id = context
    connection.execute(
        "UPDATE work_item SET state = 'deferred', eligible_at = ? WHERE id = ?",
        ("2026-07-25T09:00:00Z", work_id),
    )
    connection.commit()
    with pytest.raises(RuntimeError):
        repository.claim_and_start_attempt(
            connection,
            run_id=run_id,
            work_item_id=work_id,
            provider="brave",
            operation="search_web",
            ordinal=1,
            request_fingerprint="h" * 64,
            destination_host=None,
            reserved_nano_usd=0,
            now=NOW,  # 06:00, before the 09:00 backoff window ends
        )
    assert connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()["state"] == "deferred"
    assert connection.execute("SELECT COUNT(*) AS n FROM attempt").fetchone()["n"] == 0


def test_start_attempt_refuses_a_work_item_owned_by_another_run(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    """A mis-wired caller must not bill and reserve against another run's item."""
    run_id, work_id = context
    other_run_id = repository.create_run(
        connection,
        snapshot_id=repository.store_snapshot(
            connection, fingerprint="z" * 64, canonical_json="{}", now=NOW
        ),
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        window_end=NOW,
        budget_limit_nano_usd=None,
        now=NOW,
    )
    repository.claim_and_start_attempt(
        connection,
        run_id=run_id,
        work_item_id=work_id,
        provider="brave",
        operation="search_web",
        ordinal=1,
        request_fingerprint="i" * 64,
        destination_host=None,
        reserved_nano_usd=0,
        now=NOW,
    )
    with pytest.raises(RuntimeError):
        repository.start_attempt(
            connection,
            run_id=other_run_id,
            work_item_id=work_id,
            provider="brave",
            operation="search_web",
            ordinal=2,
            request_fingerprint="j" * 64,
            destination_host=None,
            reserved_nano_usd=0,
            now=NOW,
        )
    assert connection.execute(
        "SELECT budget_reserved_nano_usd FROM run WHERE id = ?", (other_run_id,)
    ).fetchone()["budget_reserved_nano_usd"] == 0
    assert connection.execute(
        "SELECT COUNT(*) AS n FROM attempt WHERE ordinal = 2"
    ).fetchone()["n"] == 0


def test_finish_attempt_refuses_a_mismatched_ordinal(
    connection: sqlite3.Connection, context: tuple[int, int]
) -> None:
    """Pairing the wrong AttemptRecord with an attempt_id must not silently write."""
    attempt_id = start(connection, context, 1)
    with pytest.raises(RuntimeError):
        repository.finish_attempt(
            connection,
            attempt_id=attempt_id,
            record=AttemptRecord(ordinal=2, outcome="succeeded", latency_ms=5),
            response_bytes=None,
            provider_request_id=None,
            detail_json=None,
            now=DONE,
        )
    row = connection.execute(
        "SELECT outcome, finished_at FROM attempt WHERE id = ?", (attempt_id,)
    ).fetchone()
    assert row["outcome"] is None
    assert row["finished_at"] is None
