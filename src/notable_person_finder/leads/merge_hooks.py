"""Confirmed-merge reconciliation for the digest queue.

Runs in two steps, matching the design spec's Merge Reconciliation section:
(1) queue dedup -- exactly one digest_queue row survives, keyed by rank_key
    when both sides had one; (2) fresh aggregation over the survivor's
    combined evidence, which both produces the new lead_assessment and
    re-evaluates the deduplicated row via the ordinary queue-lifecycle rules.
lead_assessment rows are immutable history and are never rewritten onto the
survivor's person_id (matches K12 of the durable-person-identity design).
"""

from __future__ import annotations

import sqlite3

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.ranking import QueueEntry, rank_key
from notable_person_finder.leads.repository import fetch_prior_queue_state


def _remove_loser_queue_row(connection: sqlite3.Connection, *, person_id: int) -> None:
    connection.execute("DELETE FROM digest_queue WHERE person_id = ?", (person_id,))


def _move_queue_row_to_survivor(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    now: str,
) -> None:
    row = connection.execute(
        "SELECT lead_assessment_id, tier FROM digest_queue WHERE person_id = ?",
        (loser_id,),
    ).fetchone()
    if row is None:
        return
    lead_assessment_id, tier = row
    connection.execute(
        "UPDATE digest_queue SET person_id = ? WHERE person_id = ?",
        (survivor_id, loser_id),
    )
    connection.execute(
        """
        INSERT INTO queue_transition (
            person_id, run_id, lead_assessment_id, tier, from_status,
            to_status, reason, occurred_at
        ) VALUES (?, ?, ?, ?, 'pending', 'pending', 'merged', ?)
        """,
        (survivor_id, run_id, lead_assessment_id, tier, now),
    )


def _dedup_queue_rows(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    now: str,
) -> None:
    survivor_prior = fetch_prior_queue_state(connection, person_id=survivor_id)
    loser_prior = fetch_prior_queue_state(connection, person_id=loser_id)

    if survivor_prior is None and loser_prior is not None:
        _move_queue_row_to_survivor(
            connection,
            survivor_id=survivor_id,
            loser_id=loser_id,
            run_id=run_id,
            now=now,
        )
        return
    if survivor_prior is None or loser_prior is None:
        return

    survivor_outcome = LeadOutcome(
        outcome=survivor_prior.tier,
        qualifying_domain_count=survivor_prior.qualifying_domain_count,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome=None,
    )
    loser_outcome = LeadOutcome(
        outcome=loser_prior.tier,
        qualifying_domain_count=loser_prior.qualifying_domain_count,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome=None,
    )
    survivor_key = rank_key(
        QueueEntry(
            person_id=survivor_id,
            eligibility_reason="new",
            first_pending_at=survivor_prior.first_pending_at,
        ),
        survivor_outcome,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at=None,
        starvation_cutoff="0000-01-01T00:00:00Z",
    )
    loser_key = rank_key(
        QueueEntry(
            person_id=loser_id,
            eligibility_reason="new",
            first_pending_at=loser_prior.first_pending_at,
        ),
        loser_outcome,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at=None,
        starvation_cutoff="0000-01-01T00:00:00Z",
    )
    if loser_key < survivor_key:
        connection.execute(
            "DELETE FROM digest_queue WHERE person_id = ?", (survivor_id,)
        )
        _move_queue_row_to_survivor(
            connection,
            survivor_id=survivor_id,
            loser_id=loser_id,
            run_id=run_id,
            now=now,
        )
    else:
        _remove_loser_queue_row(connection, person_id=loser_id)


def reconcile_on_merge(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    from notable_person_finder.leads.service import reaggregate_person_lead

    _dedup_queue_rows(
        connection,
        survivor_id=survivor_id,
        loser_id=loser_id,
        run_id=run_id,
        now=now,
    )

    reaggregate_person_lead(
        connection,
        person_id=survivor_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=now,
    )
