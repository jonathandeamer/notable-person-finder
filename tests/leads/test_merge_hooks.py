import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.merge_hooks import reconcile_on_merge
from notable_person_finder.leads.repository import (
    insert_lead_assessment,
    upsert_digest_queue,
)


@pytest.fixture
def connection(tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    apply_migrations(conn, db_path, tmp_path / "backups")
    conn.execute(
        """
        INSERT INTO configuration_snapshot (
            id, fingerprint, canonical_json, created_at
        ) VALUES (1, ?, '{}', '2026-08-01T00:00:00Z')
        """,
        ("a" * 64,),
    )
    conn.execute(
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
    conn.execute(
        """
        INSERT INTO person (
            id, created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (1, '2026-08-01T00:00:00Z', 1, 'Person 1', ?)
        """,
        ("a" * 64,),
    )
    conn.execute(
        """
        INSERT INTO person (
            id, created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (2, '2026-08-01T00:00:00Z', 1, 'Person 2', ?)
        """,
        ("b" * 64,),
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def empty_policy():
    return SourcePolicy(
        schema_version=1, key="test", label="Test", rules=(), fingerprint="a" * 64
    )


def _main_config() -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=Path("source_policies/visual_arts.toml"),
    )


def test_loser_only_queue_row_moves_to_survivor(connection, empty_policy):
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=2,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=2,
        status="pending",
        tier="possible_lead",
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection,
        survivor_id=1,
        loser_id=2,
        run_id=1,
        config=_main_config(),
        policy=empty_policy,
        now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    row = connection.execute(
        "SELECT person_id, status FROM digest_queue WHERE person_id = 1"
    ).fetchone()
    assert row is not None
    assert row[1] in ("pending", "removed")  # depends on fresh aggregation outcome

    loser_row = connection.execute(
        "SELECT * FROM digest_queue WHERE person_id = 2"
    ).fetchone()
    assert loser_row is None

    transition = connection.execute(
        "SELECT person_id, reason FROM queue_transition WHERE person_id = 1"
    ).fetchone()
    assert transition is not None
    assert transition[1] == "merged"


def test_lead_assessment_history_is_not_rewritten_onto_survivor(
    connection, empty_policy
):
    outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=2,
        run_id=1,
        outcome=outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection,
        survivor_id=1,
        loser_id=2,
        run_id=1,
        config=_main_config(),
        policy=empty_policy,
        now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    original = connection.execute(
        "SELECT person_id FROM lead_assessment WHERE id = ?", (lead_id,)
    ).fetchone()
    assert original[0] == 2  # untouched, still attributed to the loser


def test_both_sides_have_queue_rows_higher_rank_key_wins(connection, empty_policy):
    survivor_outcome = LeadOutcome(
        outcome="promising_lead",
        qualifying_domain_count=2,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    survivor_lead_id = insert_lead_assessment(
        connection,
        person_id=1,
        run_id=1,
        outcome=survivor_outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=1,
        status="pending",
        tier="promising_lead",
        eligibility_reason="new",
        lead_assessment_id=survivor_lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    loser_outcome = LeadOutcome(
        outcome="possible_lead",
        qualifying_domain_count=1,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )
    loser_lead_id = insert_lead_assessment(
        connection,
        person_id=2,
        run_id=1,
        outcome=loser_outcome,
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at="2026-08-01T00:00:00Z",
    )
    upsert_digest_queue(
        connection,
        person_id=2,
        status="pending",
        tier="possible_lead",
        eligibility_reason="new",
        lead_assessment_id=loser_lead_id,
        first_pending_at="2026-08-01T00:00:00Z",
        last_material_change_at="2026-08-01T00:00:00Z",
    )
    connection.commit()

    reconcile_on_merge(
        connection,
        survivor_id=1,
        loser_id=2,
        run_id=1,
        config=_main_config(),
        policy=empty_policy,
        now="2026-08-02T00:00:00Z",
    )
    connection.commit()

    remaining = [
        r["person_id"]
        for r in connection.execute("SELECT person_id FROM digest_queue").fetchall()
    ]
    assert remaining == [1]
