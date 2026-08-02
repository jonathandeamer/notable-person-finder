import sqlite3

from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.repository import (
    fetch_pending_queue_entries,
    fetch_prior_queue_state,
    fetch_queue_flow_counts,
    insert_digest,
    insert_digest_entry,
    insert_lead_assessment,
    insert_queue_transition,
    remove_digest_queue,
    upsert_digest_queue,
)


def _seeded_connection(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
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
    connection.execute(
        """
        INSERT INTO person (
            id, created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (7, '2026-08-01T00:00:00Z', 1, 'Person 7', ?)
        """,
        ("b" * 64,),
    )
    connection.commit()
    return connection


def test_insert_lead_assessment_sets_current_pointer(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="promising_lead",
        qualifying_domain_count=2,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    pointer = connection.execute(
        "SELECT current_lead_assessment_id FROM person WHERE id = 7"
    ).fetchone()[0]
    assert pointer == lead_id


def test_upsert_then_fetch_prior_queue_state(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=7,
        status="pending",
        tier="possible_lead",
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    prior = fetch_prior_queue_state(connection, person_id=7)
    assert prior == PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-08-01T00:00:00Z",
    )


def test_remove_digest_queue_sets_removed_status(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=7,
        status="pending",
        tier="possible_lead",
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    remove_digest_queue(
        connection,
        person_id=7,
        removed_reason="matching_page_found",
        last_material_change_at="2026-08-02T00:00:00Z",
    )
    connection.commit()
    row = connection.execute(
        "SELECT status, removed_reason FROM digest_queue WHERE person_id = 7"
    ).fetchone()
    assert row == ("removed", "matching_page_found")


def test_insert_queue_transition_records_ledger_row(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    transition_id = insert_queue_transition(
        connection,
        person_id=7,
        run_id=1,
        lead_assessment_id=lead_id,
        tier="possible_lead",
        from_status=None,
        to_status="pending",
        reason="new",
        occurred_at="2026-08-01T00:00:00Z",
    )
    connection.commit()
    row = connection.execute(
        "SELECT person_id, to_status, reason FROM queue_transition WHERE id = ?",
        (transition_id,),
    ).fetchone()
    assert row == (7, "pending", "new")


def test_fetch_pending_queue_entries_excludes_removed(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=7,
        status="pending",
        tier="possible_lead",
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    remove_digest_queue(
        connection,
        person_id=7,
        removed_reason="matching_page_found",
        last_material_change_at="2026-08-02T00:00:00Z",
    )
    connection.commit()
    entries = fetch_pending_queue_entries(connection)
    assert len(entries) == 0


def test_insert_digest_and_entry(tmp_path):
    connection = _seeded_connection(tmp_path)
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    transition_id = insert_queue_transition(
        connection,
        person_id=7,
        run_id=1,
        lead_assessment_id=lead_id,
        tier="possible_lead",
        from_status=None,
        to_status="pending",
        reason="new",
        occurred_at="2026-08-01T00:00:00Z",
    )
    digest_id = insert_digest(
        connection,
        run_id=1,
        file_path="digests/digest.md",
        timezone="UTC",
        window_start="2026-08-01T00:00:00Z",
        window_end="2026-08-01T00:00:00Z",
        run_state="completed",
        content_hash="c" * 64,
        created_at="2026-08-01T00:00:00Z",
    )
    insert_digest_entry(
        connection,
        digest_id=digest_id,
        person_id=7,
        lead_assessment_id=lead_id,
        queue_transition_id=transition_id,
        ordinal=1,
    )
    connection.commit()

    d_row = connection.execute(
        "SELECT file_path, content_hash FROM digest WHERE id = ?", (digest_id,)
    ).fetchone()
    assert d_row == ("digests/digest.md", "c" * 64)

    de_row = connection.execute(
        """
        SELECT digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal
        FROM digest_entry WHERE digest_id = ?
        """,
        (digest_id,),
    ).fetchone()
    assert de_row == (digest_id, 7, lead_id, transition_id, 1)


def _seeded_row_connection(tmp_path):
    """Same fixture as `_seeded_connection`, but with `sqlite3.Row` enabled --
    needed for `fetch_queue_flow_counts`, which reads columns by name."""
    connection = _seeded_connection(tmp_path)
    connection.row_factory = sqlite3.Row
    return connection


def _seed_queue_transition(
    connection, *, occurred_at: str, reason: str, to_status: str
) -> None:
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=7,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at=occurred_at,
    )
    insert_queue_transition(
        connection,
        person_id=7,
        run_id=1,
        lead_assessment_id=lead_id,
        tier="possible_lead",
        from_status=None,
        to_status=to_status,
        reason=reason,
        occurred_at=occurred_at,
    )
    connection.commit()


def test_fetch_queue_flow_counts_reports_no_rate_with_less_than_seven_days_history(
    tmp_path,
):
    """A two-day-old install with real queue_transition rows must not
    fabricate a 7-day rate by dividing a partial-window count by a hard-coded
    7.0 -- the global constraint against fabricating a rate from less than a
    full window of real history. Before the fix, the gate was "does any
    queue_transition row exist at all," which this same fixture would have
    satisfied and produced a bogus rate.
    """
    connection = _seeded_row_connection(tmp_path)
    _seed_queue_transition(
        connection,
        occurred_at="2026-07-31T00:00:00Z",
        reason="new",
        to_status="pending",
    )
    _seed_queue_transition(
        connection,
        occurred_at="2026-08-01T00:00:00Z",
        reason="new",
        to_status="pending",
    )

    counts = fetch_queue_flow_counts(connection, run_id=1, now="2026-08-02T00:00:00Z")

    assert counts.arrival_rate_7d is None
    assert counts.emission_rate_7d is None


def test_fetch_queue_flow_counts_reports_a_rate_once_seven_days_of_history_exists(
    tmp_path,
):
    """Once the earliest queue_transition row is at least seven days old,
    the 7-day rates are real averages over that full window."""
    connection = _seeded_row_connection(tmp_path)
    _seed_queue_transition(
        connection,
        occurred_at="2026-07-26T00:00:00Z",
        reason="new",
        to_status="pending",
    )
    _seed_queue_transition(
        connection,
        occurred_at="2026-08-01T00:00:00Z",
        reason="new",
        to_status="pending",
    )

    counts = fetch_queue_flow_counts(connection, run_id=1, now="2026-08-02T00:00:00Z")

    assert counts.arrival_rate_7d == 2 / 7.0
