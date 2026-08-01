"""SQL for the seven leads/digest-queue tables. Every function operates
inside the caller's transaction; none opens or commits its own."""

from __future__ import annotations

import sqlite3

from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.ranking import QueueEntry


def insert_lead_assessment(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    outcome: LeadOutcome,
    lead_policy_fingerprint: str,
    ordering_factors_json: str,
    decided_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO lead_assessment (
            person_id, run_id, outcome, qualifying_domain_count,
            incompleteness_reason, ordering_factors_json,
            lead_policy_fingerprint, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            outcome.outcome,
            outcome.qualifying_domain_count,
            outcome.incompleteness_reason,
            ordering_factors_json,
            lead_policy_fingerprint,
            decided_at,
        ),
    )
    lead_id = cursor.lastrowid
    assert lead_id is not None
    for article in outcome.qualifying_articles:
        connection.execute(
            """
            INSERT INTO lead_assessment_qualifying_article (
                lead_assessment_id, person_article_assessment_id, canonical_domain
            ) VALUES (?, ?, ?)
            """,
            (lead_id, article.person_article_assessment_id, article.canonical_domain),
        )
    for signal in outcome.contributing_signals:
        connection.execute(
            """
            INSERT INTO lead_assessment_signal (
                lead_assessment_id, article_assessment_signal_id
            ) VALUES (?, ?)
            """,
            (lead_id, signal.article_assessment_signal_id),
        )
    connection.execute(
        "UPDATE person SET current_lead_assessment_id = ? WHERE id = ?",
        (lead_id, person_id),
    )
    return lead_id


def fetch_prior_queue_state(
    connection: sqlite3.Connection, *, person_id: int
) -> PriorLeadState | None:
    row = connection.execute(
        """
        SELECT dq.status, dq.tier, la.qualifying_domain_count, dq.first_pending_at
        FROM digest_queue dq
        JOIN lead_assessment la ON la.id = dq.lead_assessment_id
        WHERE dq.person_id = ?
        """,
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    status, tier, qualifying_domain_count, first_pending_at = row
    return PriorLeadState(
        status=status,
        tier=tier,
        qualifying_domain_count=qualifying_domain_count,
        first_pending_at=first_pending_at,
    )


def upsert_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    status: str,
    tier: str,
    eligibility_reason: str,
    lead_assessment_id: int,
    first_pending_at: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_queue (
            person_id, status, tier, eligibility_reason, lead_assessment_id,
            first_pending_at, last_material_change_at, removed_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT (person_id) DO UPDATE SET
            status = excluded.status,
            tier = excluded.tier,
            eligibility_reason = excluded.eligibility_reason,
            lead_assessment_id = excluded.lead_assessment_id,
            last_material_change_at = excluded.last_material_change_at,
            removed_reason = NULL
        """,
        (
            person_id,
            status,
            tier,
            eligibility_reason,
            lead_assessment_id,
            first_pending_at,
            last_material_change_at,
        ),
    )


def remove_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    removed_reason: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        UPDATE digest_queue
        SET status = 'removed', removed_reason = ?, last_material_change_at = ?
        WHERE person_id = ?
        """,
        (removed_reason, last_material_change_at, person_id),
    )


def insert_queue_transition(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    lead_assessment_id: int,
    tier: str,
    from_status: str | None,
    to_status: str,
    reason: str,
    occurred_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO queue_transition (
            person_id, run_id, lead_assessment_id, tier, from_status,
            to_status, reason, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            lead_assessment_id,
            tier,
            from_status,
            to_status,
            reason,
            occurred_at,
        ),
    )
    transition_id = cursor.lastrowid
    assert transition_id is not None
    return transition_id


def fetch_pending_queue_entries(
    connection: sqlite3.Connection,
) -> list[QueueEntry]:
    rows = connection.execute(
        """
        SELECT person_id, eligibility_reason, first_pending_at
        FROM digest_queue
        WHERE status = 'pending'
        """
    ).fetchall()
    return [
        QueueEntry(person_id=r[0], eligibility_reason=r[1], first_pending_at=r[2])
        for r in rows
    ]


def insert_digest(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    timezone: str,
    window_start: str,
    window_end: str,
    run_state: str,
    content_hash: str,
    created_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO digest (
            run_id, file_path, timezone, window_start, window_end,
            run_state, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            file_path,
            timezone,
            window_start,
            window_end,
            run_state,
            content_hash,
            created_at,
        ),
    )
    digest_id = cursor.lastrowid
    assert digest_id is not None
    return digest_id


def insert_digest_entry(
    connection: sqlite3.Connection,
    *,
    digest_id: int,
    person_id: int,
    lead_assessment_id: int,
    queue_transition_id: int,
    ordinal: int,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_entry (
            digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal),
    )
