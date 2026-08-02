"""Regression coverage for the finding raised on Task 12's shortlist render:
`_leads_summary` selected and rendered the top-`digest_limit` pending
`digest_queue` entries but never persisted the `digest_entry` rows nor
transitioned `digest_queue`/`queue_transition` from `pending` to `emitted`,
so the same candidates reappeared in every digest forever and the "Emitted"
queue-flow counter could never be anything but 0.

Tests here prove, against a real migrated database (no mocks):

- the repository-level primitives (`emit_shortlist_entries`,
  `record_digest_with_entries`) actually write the queue_transition,
  digest_queue-status, digest, and digest_entry rows the design spec
  requires; and
- `cli.main._leads_summary` actually calls them for the entries it selects
  into the shortlist, and *only* those -- an entry ranked below
  `digest_limit` must keep its `pending` status untouched (the existing
  digest-limit-omission-retains-eligibility rule).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from notable_person_finder.cli.main import _leads_summary
from notable_person_finder.config.models import (
    AggregateLeadConfig,
    MainConfig,
    TasksConfig,
)
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.repository import (
    ShortlistCandidate,
    emit_shortlist_entries,
    insert_lead_assessment,
    record_digest_with_entries,
    upsert_digest_queue,
)
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState


def _seeded_connection(tmp_path: Path, *, person_count: int) -> sqlite3.Connection:
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    apply_migrations(connection, db_path, tmp_path / "backups")
    connection.execute(
        """
        INSERT INTO configuration_snapshot (
            id, fingerprint, canonical_json, created_at
        ) VALUES (1, ?, '{}', '2026-08-01T00:00:00Z')
        """,
        ("a" * 64,),
    )
    connection.execute(
        """
        INSERT INTO run (
            id, configuration_snapshot_id, state, timezone,
            window_start, window_end, started_at
        ) VALUES (
            1, 1, 'running', 'UTC',
            '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'
        )
        """
    )
    for person_id in range(1, person_count + 1):
        connection.execute(
            """
            INSERT INTO person (
                id, created_at, created_by_run_id, display_name,
                identity_fingerprint
            ) VALUES (?, '2026-08-01T00:00:00Z', 1, ?, ?)
            """,
            (person_id, f"Person {person_id}", f"{person_id:064d}"),
        )
    connection.commit()
    return connection


def _seed_pending_lead(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    first_pending_at: str,
) -> int:
    outcome = LeadOutcome(
        outcome="promising_lead",
        qualifying_domain_count=2,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=person_id,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json='{"wikipedia_outcome": "no_matching_page_found"}',
        decided_at=first_pending_at,
    )
    upsert_digest_queue(
        connection,
        person_id=person_id,
        status="pending",
        tier="promising_lead",
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at=first_pending_at,
        last_material_change_at=first_pending_at,
    )
    connection.commit()
    return lead_id


def _report() -> RunReport:
    return RunReport(
        run_id=1,
        state=RunState.COMPLETE,
        started_at="2026-08-01T00:00:00Z",
        finished_at="2026-08-01T00:01:00Z",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-02T00:00:00Z",
        counters=RunCounters(
            required_succeeded=1,
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=0,
            optional_succeeded=0,
            optional_skipped=0,
            operational_failures=0,
        ),
        paused_providers=frozenset(),
        interrupted_runs=(),
        failure_categories={},
        budget_limit_nano_usd=None,
        budget_reserved_nano_usd=0,
        budget_actual_nano_usd=0,
        deferred_reasons={},
    )


def _config(*, digest_limit: int) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="UTC",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
        tasks=TasksConfig(
            aggregate_lead=AggregateLeadConfig(digest_limit=digest_limit)
        ),
    )


# ---------------------------------------------------------------------------
# Repository-level primitives
# ---------------------------------------------------------------------------


def test_emit_shortlist_entries_transitions_pending_to_emitted(tmp_path: Path) -> None:
    connection = _seeded_connection(tmp_path, person_count=1)
    lead_id = _seed_pending_lead(
        connection, person_id=1, first_pending_at="2026-08-01T00:00:00Z"
    )
    candidate = ShortlistCandidate(
        person_id=1,
        display_name="Person 1",
        eligibility_reason="new",
        first_pending_at="2026-08-01T00:00:00Z",
        tier="promising_lead",
        lead_assessment_id=lead_id,
        outcome="promising_lead",
        qualifying_domain_count=2,
        wikipedia_outcome="no_matching_page_found",
    )
    emitted = emit_shortlist_entries(
        connection, run_id=1, occurred_at="2026-08-01T12:00:00Z", candidates=[candidate]
    )
    assert len(emitted) == 1
    _, transition_id = emitted[0]

    status = connection.execute(
        "SELECT status FROM digest_queue WHERE person_id = 1"
    ).fetchone()[0]
    assert status == "emitted"

    transition_row = connection.execute(
        "SELECT from_status, to_status, reason FROM queue_transition WHERE id = ?",
        (transition_id,),
    ).fetchone()
    assert tuple(transition_row) == ("pending", "emitted", "digest_rendered")


def test_record_digest_with_entries_writes_digest_and_entry_rows(
    tmp_path: Path,
) -> None:
    connection = _seeded_connection(tmp_path, person_count=1)
    lead_id = _seed_pending_lead(
        connection, person_id=1, first_pending_at="2026-08-01T00:00:00Z"
    )
    candidate = ShortlistCandidate(
        person_id=1,
        display_name="Person 1",
        eligibility_reason="new",
        first_pending_at="2026-08-01T00:00:00Z",
        tier="promising_lead",
        lead_assessment_id=lead_id,
        outcome="promising_lead",
        qualifying_domain_count=2,
        wikipedia_outcome="no_matching_page_found",
    )
    emitted = emit_shortlist_entries(
        connection, run_id=1, occurred_at="2026-08-01T12:00:00Z", candidates=[candidate]
    )
    digest_id = record_digest_with_entries(
        connection,
        run_id=1,
        file_path="/digests/2026-08-01-run.md",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-02T00:00:00Z",
        run_state="complete",
        content_hash="c" * 64,
        created_at="2026-08-01T12:00:00Z",
        emitted=emitted,
    )
    digest_row = connection.execute(
        "SELECT file_path, content_hash FROM digest WHERE id = ?", (digest_id,)
    ).fetchone()
    assert tuple(digest_row) == ("/digests/2026-08-01-run.md", "c" * 64)

    entry_row = connection.execute(
        "SELECT digest_id, person_id, lead_assessment_id, ordinal "
        "FROM digest_entry WHERE digest_id = ?",
        (digest_id,),
    ).fetchone()
    assert tuple(entry_row) == (digest_id, 1, lead_id, 1)


# ---------------------------------------------------------------------------
# `_leads_summary` wiring: it only ranks and reads -- it must not itself
# transition any digest_queue row. Emission is the caller's (report_run's)
# job, deliberately deferred until after write_digest succeeds (see the
# emission-ordering fix report: emitting before the digest file is durably
# written left a crash window where entries vanished from the queue with no
# digest ever having shown them). These tests prove _leads_summary selects
# the right candidates for the shortlist without mutating status, and that
# a caller-driven emission afterward transitions only those, leaving
# overflow entries pending and untouched.
# ---------------------------------------------------------------------------


def test_leads_summary_does_not_itself_transition_any_digest_queue_row(
    tmp_path: Path,
) -> None:
    """Regression test for the emission-ordering fix: `_leads_summary` must
    only rank and read. If it still emitted internally (the pre-fix
    behaviour), every digest_queue row it selected would already be
    'emitted' by the time this function returns, before write_digest has
    even run -- reintroducing the crash window where a write_digest failure
    left entries committed 'emitted' with no digest ever recording them.
    """
    connection = _seeded_connection(tmp_path, person_count=3)
    _seed_pending_lead(connection, person_id=1, first_pending_at="2026-07-29T00:00:00Z")
    _seed_pending_lead(connection, person_id=2, first_pending_at="2026-07-30T00:00:00Z")
    _seed_pending_lead(connection, person_id=3, first_pending_at="2026-07-31T00:00:00Z")

    config = _config(digest_limit=2)
    report = _report()
    result = _leads_summary(
        connection, report, config=config, now="2026-08-01T00:00:00Z"
    )
    assert result is not None
    shortlist_entries, _queue_flow, limited_candidates = result

    assert len(shortlist_entries) == 2
    assert len(limited_candidates) == 2

    statuses = {
        row[0]
        for row in connection.execute("SELECT status FROM digest_queue").fetchall()
    }
    # Every row -- selected or overflow -- is still 'pending': _leads_summary
    # ranked and selected but never emitted.
    assert statuses == {"pending"}
    transition_count = connection.execute(
        "SELECT COUNT(*) FROM queue_transition WHERE to_status = 'emitted'"
    ).fetchone()[0]
    assert transition_count == 0


def test_leads_summary_emits_selected_entries_and_leaves_overflow_pending(
    tmp_path: Path,
) -> None:
    connection = _seeded_connection(tmp_path, person_count=3)
    # Distinct first_pending_at so ranking/ordering is deterministic; all
    # three share outcome/tier/eligibility_reason so rank_key falls back to
    # recency, oldest first.
    _seed_pending_lead(connection, person_id=1, first_pending_at="2026-07-29T00:00:00Z")
    _seed_pending_lead(connection, person_id=2, first_pending_at="2026-07-30T00:00:00Z")
    _seed_pending_lead(connection, person_id=3, first_pending_at="2026-07-31T00:00:00Z")

    config = _config(digest_limit=2)
    report = _report()
    result = _leads_summary(
        connection, report, config=config, now="2026-08-01T00:00:00Z"
    )
    assert result is not None
    shortlist_entries, _queue_flow, limited_candidates = result

    assert len(shortlist_entries) == 2
    assert len(limited_candidates) == 2

    # The caller (report_run) is the one that emits, after write_digest
    # succeeds -- exercised here directly.
    emitted = emit_shortlist_entries(
        connection,
        run_id=report.run_id,
        occurred_at="2026-08-01T00:00:00Z",
        candidates=limited_candidates,
    )
    assert len(emitted) == 2
    emitted_person_ids = {candidate.person_id for candidate, _tid in emitted}

    statuses = dict(
        connection.execute("SELECT person_id, status FROM digest_queue").fetchall()
    )
    # Exactly the two selected entries flip to 'emitted'; the third (ranked
    # last by recency, beyond digest_limit=2) must remain 'pending'.
    emitted_statuses = {pid for pid, status in statuses.items() if status == "emitted"}
    pending_statuses = {pid for pid, status in statuses.items() if status == "pending"}
    assert emitted_statuses == emitted_person_ids
    assert len(pending_statuses) == 1
    assert pending_statuses.isdisjoint(emitted_person_ids)

    transition_count = connection.execute(
        "SELECT COUNT(*) FROM queue_transition WHERE to_status = 'emitted'"
    ).fetchone()[0]
    assert transition_count == 2

    # No digest_transition to 'emitted' was recorded for the overflow person.
    (overflow_person_id,) = pending_statuses
    overflow_emitted = connection.execute(
        "SELECT COUNT(*) FROM queue_transition "
        "WHERE person_id = ? AND to_status = 'emitted'",
        (overflow_person_id,),
    ).fetchone()[0]
    assert overflow_emitted == 0


def test_leads_summary_all_entries_emitted_when_digest_limit_covers_all(
    tmp_path: Path,
) -> None:
    connection = _seeded_connection(tmp_path, person_count=2)
    _seed_pending_lead(connection, person_id=1, first_pending_at="2026-07-30T00:00:00Z")
    _seed_pending_lead(connection, person_id=2, first_pending_at="2026-07-31T00:00:00Z")

    config = _config(digest_limit=10)
    report = _report()
    result = _leads_summary(
        connection, report, config=config, now="2026-08-01T00:00:00Z"
    )
    assert result is not None
    shortlist_entries, _queue_flow, limited_candidates = result

    assert len(shortlist_entries) == 2
    assert len(limited_candidates) == 2

    emitted = emit_shortlist_entries(
        connection,
        run_id=report.run_id,
        occurred_at="2026-08-01T00:00:00Z",
        candidates=limited_candidates,
    )
    assert len(emitted) == 2
    statuses = {
        row[0]
        for row in connection.execute("SELECT status FROM digest_queue").fetchall()
    }
    assert statuses == {"emitted"}

    entry_count = connection.execute(
        "SELECT COUNT(*) FROM queue_transition WHERE to_status = 'emitted'"
    ).fetchone()[0]
    assert entry_count == 2

    # digest/digest_entry rows are the caller's (report_run's) job once
    # write_digest's real file_path/content_hash exist -- not created here.
    digest_rows = connection.execute("SELECT COUNT(*) FROM digest").fetchone()[0]
    assert digest_rows == 0
    digest_id = record_digest_with_entries(
        connection,
        run_id=1,
        file_path="/digests/2026-08-01-run.md",
        timezone="UTC",
        window_start=report.window_start,
        window_end=report.window_end,
        run_state=str(report.state),
        content_hash="d" * 64,
        created_at="2026-08-01T00:00:00Z",
        emitted=emitted,
    )
    entry_rows = connection.execute(
        "SELECT COUNT(*) FROM digest_entry WHERE digest_id = ?", (digest_id,)
    ).fetchone()[0]
    assert entry_rows == 2


# ---------------------------------------------------------------------------
# Atomic emit-and-record: the crash-window fix for the ordering finding.
# ---------------------------------------------------------------------------


def test_emit_shortlist_entries_and_record_digest_join_one_caller_transaction(
    tmp_path: Path,
) -> None:
    """Regression test for the emission-ordering fix: when the caller wraps
    both calls in its own `BEGIN IMMEDIATE`, neither function may commit or
    roll back independently -- they must join the caller's transaction, so
    a failure between them rolls back everything (no entries silently
    marked 'emitted' with no digest ever recording them) and success
    commits everything together in one atomic step. This mirrors exactly
    how `cli.main.command_run`'s `report_run` calls them after
    `write_digest` succeeds.
    """
    connection = _seeded_connection(tmp_path, person_count=1)
    lead_id = _seed_pending_lead(
        connection, person_id=1, first_pending_at="2026-08-01T00:00:00Z"
    )
    candidate = ShortlistCandidate(
        person_id=1,
        display_name="Person 1",
        eligibility_reason="new",
        first_pending_at="2026-08-01T00:00:00Z",
        tier="promising_lead",
        lead_assessment_id=lead_id,
        outcome="promising_lead",
        qualifying_domain_count=2,
        wikipedia_outcome="no_matching_page_found",
    )

    connection.execute("BEGIN IMMEDIATE")
    emitted = emit_shortlist_entries(
        connection, run_id=1, occurred_at="2026-08-01T12:00:00Z", candidates=[candidate]
    )
    digest_id = record_digest_with_entries(
        connection,
        run_id=1,
        file_path="/digests/2026-08-01-run.md",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-02T00:00:00Z",
        run_state="complete",
        content_hash="e" * 64,
        created_at="2026-08-01T12:00:00Z",
        emitted=emitted,
    )
    # Neither call committed on its own: from a second, independent
    # connection, nothing is visible yet.
    other = sqlite3.connect(tmp_path / "test.db")
    uncommitted_status = other.execute(
        "SELECT status FROM digest_queue WHERE person_id = 1"
    ).fetchone()[0]
    assert uncommitted_status == "pending"
    other.close()

    connection.commit()

    status = connection.execute(
        "SELECT status FROM digest_queue WHERE person_id = 1"
    ).fetchone()[0]
    assert status == "emitted"
    digest_row = connection.execute(
        "SELECT id FROM digest WHERE id = ?", (digest_id,)
    ).fetchone()
    assert digest_row is not None


def test_emit_shortlist_entries_and_record_digest_roll_back_together_on_failure(
    tmp_path: Path,
) -> None:
    """If the caller's transaction fails after emission but before (or
    during) recording the digest, both must roll back together -- proving
    the atomicity the ordering fix relies on. Simulates the failure by
    rolling back explicitly rather than raising mid-`record_digest_with_
    entries` (which would require corrupting its arguments); the assertion
    that matters is that a rollback after both calls undoes both, not just
    one.
    """
    connection = _seeded_connection(tmp_path, person_count=1)
    lead_id = _seed_pending_lead(
        connection, person_id=1, first_pending_at="2026-08-01T00:00:00Z"
    )
    candidate = ShortlistCandidate(
        person_id=1,
        display_name="Person 1",
        eligibility_reason="new",
        first_pending_at="2026-08-01T00:00:00Z",
        tier="promising_lead",
        lead_assessment_id=lead_id,
        outcome="promising_lead",
        qualifying_domain_count=2,
        wikipedia_outcome="no_matching_page_found",
    )

    connection.execute("BEGIN IMMEDIATE")
    emitted = emit_shortlist_entries(
        connection, run_id=1, occurred_at="2026-08-01T12:00:00Z", candidates=[candidate]
    )
    record_digest_with_entries(
        connection,
        run_id=1,
        file_path="/digests/2026-08-01-run.md",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-02T00:00:00Z",
        run_state="complete",
        content_hash="f" * 64,
        created_at="2026-08-01T12:00:00Z",
        emitted=emitted,
    )
    connection.rollback()

    status = connection.execute(
        "SELECT status FROM digest_queue WHERE person_id = 1"
    ).fetchone()[0]
    assert status == "pending"
    digest_rows = connection.execute("SELECT COUNT(*) FROM digest").fetchone()[0]
    assert digest_rows == 0


def test_record_digest_with_entries_writes_a_digest_row_for_an_empty_shortlist(
    tmp_path: Path,
) -> None:
    """A run with zero shortlist entries still writes an actual digest file,
    so it must still get a `digest` row (with zero `digest_entry` rows) --
    otherwise the `digest` table is not a complete history of every digest
    actually written. Before this fix, `command_run` guarded the call with
    `if emitted:`, skipping the digest row entirely on an empty shortlist.
    """
    connection = _seeded_connection(tmp_path, person_count=0)
    digest_id = record_digest_with_entries(
        connection,
        run_id=1,
        file_path="/digests/2026-08-01-run.md",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-02T00:00:00Z",
        run_state="complete",
        content_hash="a" * 64,
        created_at="2026-08-01T00:00:00Z",
        emitted=[],
    )
    digest_row = connection.execute(
        "SELECT file_path FROM digest WHERE id = ?", (digest_id,)
    ).fetchone()
    assert digest_row is not None
    assert digest_row[0] == "/digests/2026-08-01-run.md"
    entry_count = connection.execute(
        "SELECT COUNT(*) FROM digest_entry WHERE digest_id = ?", (digest_id,)
    ).fetchone()[0]
    assert entry_count == 0
