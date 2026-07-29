from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Sequence
from pathlib import Path

import pytest

from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.runs import repository
from notable_person_finder.runs.models import WorkState

NOW = "2026-07-25T06:00:00Z"
LATER = "2026-07-25T07:00:00Z"


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    apply_migrations(connection, database, tmp_path / "backups")
    yield connection
    connection.close()


@pytest.fixture
def run_id(connection: sqlite3.Connection) -> int:
    snapshot = repository.store_snapshot(
        connection, fingerprint="a" * 64, canonical_json="{}", now=NOW
    )
    return repository.create_run(
        connection,
        snapshot_id=snapshot,
        timezone="Europe/Paris",
        window_start="2026-07-24T06:00:00Z",
        window_end=NOW,
        budget_limit_nano_usd=None,
        now=NOW,
    )


def schedule(
    connection: sqlite3.Connection,
    run_id: int,
    *,
    fingerprint: str = "b" * 64,
    task_type: str = "detect_people",
    subject_kind: str = "source_item",
    subject_id: int = 1,
    required: bool = True,
    priority: int = 100,
    eligible_at: str = NOW,
) -> int:
    return repository.schedule_work(
        connection,
        task_type=task_type,
        subject_kind=subject_kind,
        subject_id=subject_id,
        fingerprint=fingerprint,
        required=required,
        priority=priority,
        eligible_at=eligible_at,
        run_id=run_id,
        now=NOW,
    )


def test_scheduling_returns_a_new_work_item(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id, priority=42, required=True)
    row = connection.execute(
        "SELECT state, priority, required FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.PENDING
    assert row["priority"] == 42
    assert row["required"] == 1


def test_rescheduling_identical_work_reuses_the_active_row(
    connection: sqlite3.Connection, run_id: int
) -> None:
    first = schedule(connection, run_id)
    second = schedule(connection, run_id)
    assert first == second
    count = connection.execute("SELECT COUNT(*) AS n FROM work_item").fetchone()["n"]
    assert count == 1


def test_a_changed_fingerprint_supersedes_the_old_active_row(
    connection: sqlite3.Connection, run_id: int
) -> None:
    stale = schedule(connection, run_id, fingerprint="c" * 64)
    superseded = repository.supersede_work(
        connection,
        task_type="detect_people",
        fingerprint="c" * 64,
        run_id=run_id,
        now=LATER,
        reason="input changed",
    )
    assert superseded == 1
    fresh = schedule(connection, run_id, fingerprint="d" * 64)
    assert fresh != stale
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (stale,)
        ).fetchone()["state"]
        == WorkState.SUPERSEDED
    )


def test_claiming_marks_the_item_running_and_attributes_the_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    claimed = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert claimed is not None
    assert claimed.id == work_id
    assert claimed.state is WorkState.RUNNING
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.RUNNING
    assert row["claimed_by_run_id"] == run_id


def test_claiming_holds_no_transaction_afterwards(
    connection: sqlite3.Connection, run_id: int
) -> None:
    schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    assert not connection.in_transaction


def test_claims_are_ordered_by_priority_then_identity(
    connection: sqlite3.Connection, run_id: int
) -> None:
    low = schedule(connection, run_id, fingerprint="e" * 64, priority=500)
    high = schedule(connection, run_id, fingerprint="f" * 64, priority=10)
    first_claim = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert first_claim is not None
    assert first_claim.id == high
    second_claim = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert second_claim is not None
    assert second_claim.id == low


def test_work_is_not_claimed_before_its_eligibility_time(
    connection: sqlite3.Connection, run_id: int
) -> None:
    schedule(connection, run_id, eligible_at="2026-07-26T00:00:00Z")
    assert repository.claim_next(connection, run_id=run_id, now=NOW) is None


def test_deferred_work_is_not_claimed_in_the_same_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="not_evaluated_budget",
        now=NOW,
    )
    assert repository.claim_next(connection, run_id=run_id, now=NOW) is None


def test_deferred_work_becomes_eligible_in_the_next_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    next_run = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=LATER,
    )
    claimed = repository.claim_next(connection, run_id=next_run, now=LATER)
    assert claimed is not None and claimed.id == work_id


def test_claiming_can_be_restricted_to_registered_task_types(
    connection: sqlite3.Connection, run_id: int
) -> None:
    unknown = schedule(
        connection, run_id, fingerprint="9" * 64, task_type="future_task"
    )
    assert (
        repository.claim_next(
            connection, run_id=run_id, now=NOW, task_types={"detect_people"}
        )
        is None
    )
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (unknown,)
        ).fetchone()["state"]
        == WorkState.PENDING
    )


def test_permanently_failed_work_is_never_reclaimed(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.FAILED_PERMANENT,
        reason="authentication",
        now=NOW,
    )
    next_run = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=LATER,
    )
    assert repository.claim_next(connection, run_id=next_run, now=LATER) is None


def test_succeeded_work_frees_the_active_identity(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )
    # Same identity may be scheduled again only if the caller decides to; the
    # index no longer blocks it, and history is preserved.
    again = schedule(connection, run_id)
    assert again != work_id


def test_counters_separate_required_from_optional_work(
    connection: sqlite3.Connection, run_id: int
) -> None:
    done = schedule(connection, run_id, fingerprint="1" * 64, required=True)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=done,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )
    schedule(connection, run_id, fingerprint="2" * 64, required=True)
    optional = schedule(connection, run_id, fingerprint="3" * 64, required=False)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=optional,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )

    counters = repository.run_counters(connection, run_id=run_id, now=NOW)
    assert counters.required_succeeded == 1
    assert counters.optional_succeeded == 1
    assert repository.pending_required(connection, now=NOW) == 1


def test_completing_a_non_running_item_raises(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.SUCCEEDED,
        reason=None,
        now=NOW,
    )
    with pytest.raises(RuntimeError):
        repository.complete_work(
            connection,
            work_item_id=work_id,
            run_id=run_id,
            state=WorkState.SUCCEEDED,
            reason=None,
            now=LATER,
        )
    row = connection.execute(
        "SELECT state, updated_at FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.SUCCEEDED
    assert row["updated_at"] == NOW


def test_next_eligible_peeks_without_mutating(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    peeked = repository.next_eligible(connection, run_id=run_id, now=NOW)
    assert peeked is not None
    assert peeked.id == work_id
    assert peeked.state is WorkState.PENDING
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.PENDING
    assert row["claimed_by_run_id"] is None


def test_running_work_is_not_claimed_again(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    first = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert first is not None and first.id == work_id
    second = repository.claim_next(connection, run_id=run_id, now=NOW)
    assert second is None
    row = connection.execute(
        "SELECT state, claimed_by_run_id FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.RUNNING
    assert row["claimed_by_run_id"] == run_id


def test_supersede_work_does_not_touch_a_running_item(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id, fingerprint="6" * 64)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    changed = repository.supersede_work(
        connection,
        task_type="detect_people",
        fingerprint="6" * 64,
        run_id=run_id,
        now=LATER,
        reason="input changed",
    )
    assert changed == 0
    row = connection.execute(
        "SELECT state FROM work_item WHERE id = ?", (work_id,)
    ).fetchone()
    assert row["state"] == WorkState.RUNNING


def test_supersede_work_for_subject_retires_every_active_fingerprint(
    connection: sqlite3.Connection, run_id: int
) -> None:
    first = schedule(
        connection, run_id, fingerprint="a1" * 32, subject_id=7, required=True
    )
    second = schedule(
        connection, run_id, fingerprint="a2" * 32, subject_id=7, required=True
    )
    other = schedule(
        connection, run_id, fingerprint="b1" * 32, subject_id=8, required=True
    )

    changed = repository.supersede_work_for_subject(
        connection,
        task_type="detect_people",
        subject_kind="source_item",
        subject_id=7,
        run_id=run_id,
        now=LATER,
        reason="subject retired",
    )

    assert changed == 2
    states = {
        row["id"]: (row["state"], row["reason"])
        for row in connection.execute("SELECT id, state, reason FROM work_item")
    }
    assert states[first] == (WorkState.SUPERSEDED, "subject retired")
    assert states[second] == (WorkState.SUPERSEDED, "subject retired")
    assert states[other][0] == WorkState.PENDING


def test_deferred_required_counts_only_required_deferred_items(
    connection: sqlite3.Connection, run_id: int
) -> None:
    required_item = schedule(connection, run_id, fingerprint="4" * 64, required=True)
    optional_item = schedule(connection, run_id, fingerprint="5" * 64, required=False)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=required_item,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    repository.complete_work(
        connection,
        work_item_id=optional_item,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    assert repository.deferred_required(connection) == 1


def test_operational_failures_for_run_counts_only_failed_attempts(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    connection.execute(
        """
        INSERT INTO attempt (run_id, work_item_id, provider, operation, ordinal,
                             started_at, finished_at, outcome, failure_category,
                             request_fingerprint)
        VALUES (?, ?, 'brave', 'search_web', 1, ?, ?, 'failed', 'provider_error', ?)
        """,
        (run_id, work_id, NOW, NOW, "f" * 64),
    )
    connection.execute(
        """
        INSERT INTO attempt (run_id, work_item_id, provider, operation, ordinal,
                             started_at, finished_at, outcome, request_fingerprint)
        VALUES (?, ?, 'brave', 'search_web', 2, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_id, NOW, NOW, "e" * 64),
    )
    connection.commit()
    assert repository.operational_failures_for_run(connection, run_id=run_id) == 1


def test_counters_cover_deferred_failed_permanent_skipped_and_operational_failures(
    connection: sqlite3.Connection, run_id: int
) -> None:
    deferred_item = schedule(connection, run_id, fingerprint="7" * 64, required=True)
    failed_item = schedule(connection, run_id, fingerprint="8" * 64, required=True)
    # Superseded below by fingerprint, so the returned id is deliberately unused.
    schedule(connection, run_id, fingerprint="a1" * 32, required=False)

    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.claim_next(connection, run_id=run_id, now=NOW)

    repository.complete_work(
        connection,
        work_item_id=deferred_item,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    repository.complete_work(
        connection,
        work_item_id=failed_item,
        run_id=run_id,
        state=WorkState.FAILED_PERMANENT,
        reason="authentication",
        now=NOW,
    )
    repository.supersede_work(
        connection,
        task_type="detect_people",
        fingerprint="a1" * 32,
        run_id=run_id,
        now=NOW,
        reason="input changed",
    )
    connection.execute(
        """
        INSERT INTO attempt (run_id, work_item_id, provider, operation, ordinal,
                             started_at, finished_at, outcome, failure_category,
                             request_fingerprint)
        VALUES (?, ?, 'brave', 'search_web', 1, ?, ?, 'failed', 'provider_error', ?)
        """,
        (run_id, failed_item, NOW, NOW, "d" * 64),
    )
    connection.commit()

    counters = repository.run_counters(connection, run_id=run_id, now=LATER)
    assert counters.required_deferred == 1
    assert counters.required_failed_permanent == 1
    assert counters.optional_skipped == 1
    assert counters.operational_failures == 1


def test_deferring_unclaimed_work_keeps_it_out_of_the_same_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)

    repository.defer_unclaimed(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        reason="not_evaluated_budget",
        now=NOW,
    )

    row = connection.execute(
        "SELECT state, reason, claimed_by_run_id, completed_by_run_id "
        "FROM work_item WHERE id = ?",
        (work_id,),
    ).fetchone()
    assert row["state"] == WorkState.DEFERRED
    assert row["reason"] == "not_evaluated_budget"
    assert row["claimed_by_run_id"] is None
    assert row["completed_by_run_id"] == run_id
    assert repository.next_eligible(connection, run_id=run_id, now=LATER) is None


def test_deferring_unclaimed_work_refuses_a_claimed_item(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)

    with pytest.raises(RuntimeError, match="unclaimed work"):
        repository.defer_unclaimed(
            connection,
            work_item_id=work_id,
            run_id=run_id,
            reason="not_evaluated_budget",
            now=LATER,
        )
    assert (
        connection.execute(
            "SELECT state FROM work_item WHERE id = ?", (work_id,)
        ).fetchone()["state"]
        == WorkState.RUNNING
    )
    assert not connection.in_transaction


def test_claim_batch_claims_a_subset_in_priority_then_identity_order(
    connection: sqlite3.Connection, run_id: int
) -> None:
    ids = [
        schedule(connection, run_id, fingerprint=str(n) * 64, priority=priority)
        for n, priority in enumerate([50, 10, 30, 10, 20], start=1)
    ]
    # priority 10 appears twice (ids[1], ids[3]); ties break by id ascending.
    expected_order = [ids[1], ids[3], ids[4]]

    claimed = repository.claim_batch(connection, run_id=run_id, now=NOW, limit=3)

    assert [item.id for item in claimed] == expected_order
    assert all(item.state is WorkState.RUNNING for item in claimed)
    rows = {
        row["id"]: row["state"]
        for row in connection.execute("SELECT id, state FROM work_item")
    }
    remaining_pending = {ids[0], ids[2]}
    for work_id in remaining_pending:
        assert rows[work_id] == WorkState.PENDING
    for work_id in expected_order:
        assert rows[work_id] == WorkState.RUNNING


def test_claim_batch_does_not_reclaim_already_claimed_items(
    connection: sqlite3.Connection, run_id: int
) -> None:
    first = schedule(connection, run_id, fingerprint="1" * 64, priority=10)
    second = schedule(connection, run_id, fingerprint="2" * 64, priority=20)

    first_batch = repository.claim_batch(connection, run_id=run_id, now=NOW, limit=1)
    assert [item.id for item in first_batch] == [first]

    second_batch = repository.claim_batch(connection, run_id=run_id, now=NOW, limit=5)
    assert [item.id for item in second_batch] == [second]


def test_claim_batch_respects_task_type_filter(
    connection: sqlite3.Connection, run_id: int
) -> None:
    wanted = schedule(
        connection, run_id, fingerprint="1" * 64, task_type="detect_people"
    )
    schedule(connection, run_id, fingerprint="2" * 64, task_type="future_task")

    claimed = repository.claim_batch(
        connection, run_id=run_id, now=NOW, task_types={"detect_people"}, limit=5
    )

    assert [item.id for item in claimed] == [wanted]


def test_claim_batch_excludes_items_not_yet_eligible(
    connection: sqlite3.Connection, run_id: int
) -> None:
    ready = schedule(connection, run_id, fingerprint="1" * 64, eligible_at=NOW)
    schedule(
        connection,
        run_id,
        fingerprint="2" * 64,
        eligible_at="2026-07-26T00:00:00Z",
    )

    claimed = repository.claim_batch(connection, run_id=run_id, now=NOW, limit=5)

    assert [item.id for item in claimed] == [ready]


def test_claim_batch_rejects_a_limit_below_one(
    connection: sqlite3.Connection, run_id: int
) -> None:
    schedule(connection, run_id)
    with pytest.raises(ValueError):
        repository.claim_batch(connection, run_id=run_id, now=NOW, limit=0)
    with pytest.raises(ValueError):
        repository.claim_batch(connection, run_id=run_id, now=NOW, limit=-1)
    # Rejection must not leave a stray transaction or touch any row.
    assert not connection.in_transaction
    row = connection.execute("SELECT state FROM work_item").fetchone()
    assert row["state"] == WorkState.PENDING


def test_claim_batch_includes_work_deferred_by_a_different_run(
    connection: sqlite3.Connection, run_id: int
) -> None:
    work_id = schedule(connection, run_id)
    repository.claim_next(connection, run_id=run_id, now=NOW)
    repository.complete_work(
        connection,
        work_item_id=work_id,
        run_id=run_id,
        state=WorkState.DEFERRED,
        reason="exhausted transient failure",
        now=NOW,
    )
    next_run = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=LATER,
    )

    claimed = repository.claim_batch(connection, run_id=next_run, now=LATER, limit=5)

    assert [item.id for item in claimed] == [work_id]


class _RacingConnection:
    """Forwards to a real connection, but intercepts `execute` to inject a
    same-connection, same-transaction "steal" of a row right before the
    batch claim's UPDATE runs.

    `sqlite3.Connection.execute` cannot be monkeypatched directly (it is a
    read-only attribute on the C type), so this thin proxy stands in for the
    connection object instead.
    """

    def __init__(self, real: sqlite3.Connection, stolen_id: int, thief_run_id: int):
        self._real = real
        self._stolen_id = stolen_id
        self._thief_run_id = thief_run_id

    def execute(self, sql: str, parameters: Sequence[object] = ()) -> sqlite3.Cursor:
        if sql.lstrip().startswith("UPDATE work_item") and "'running'" in sql:
            self._real.execute(
                "UPDATE work_item SET state = 'running', claimed_by_run_id = ? "
                "WHERE id = ?",
                (self._thief_run_id, self._stolen_id),
            )
        return self._real.execute(sql, parameters)

    def __getattr__(self, name: str) -> object:
        return getattr(self._real, name)


def test_claim_batch_is_all_or_nothing_when_the_update_detects_a_race(
    connection: sqlite3.Connection, run_id: int
) -> None:
    """A crash (or a lost race) partway through the claim must not leave a
    partially-claimed batch: either every selected item ends up `running`, or
    none does.

    This is simulated by stealing one of the two selected items — via the
    same connection, inside the still-open transaction — right before the
    batch UPDATE runs. That makes the UPDATE's row count come up short, which
    must raise and roll back everything, including the "steal" itself.
    """
    first = schedule(connection, run_id, fingerprint="1" * 64, priority=10)
    second = schedule(connection, run_id, fingerprint="2" * 64, priority=20)
    thief_run_id = repository.create_run(
        connection,
        snapshot_id=1,
        timezone="Europe/Paris",
        window_start=NOW,
        window_end=LATER,
        budget_limit_nano_usd=None,
        now=NOW,
    )
    racing = _RacingConnection(connection, stolen_id=first, thief_run_id=thief_run_id)

    with pytest.raises(RuntimeError):
        repository.claim_batch(racing, run_id=run_id, now=NOW, limit=2)  # type: ignore[arg-type]

    rows = {
        row["id"]: (row["state"], row["claimed_by_run_id"])
        for row in connection.execute(
            "SELECT id, state, claimed_by_run_id FROM work_item"
        )
    }
    assert rows[first] == (WorkState.PENDING, None)
    assert rows[second] == (WorkState.PENDING, None)
    assert not connection.in_transaction
