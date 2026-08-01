"""Digest-queue resurfacing eligibility and transition decisions (K3, K10).

digest_queue is a mutable one-row-per-person projection; queue_transition is
the append-only ledger. This module decides *what* should happen; callers
(leads/service.py, leads/merge_hooks.py) perform the actual SQL writes inside
their own transaction.
"""

from __future__ import annotations

from dataclasses import dataclass

from notable_person_finder.leads.aggregation import LeadOutcome

_POSITIVE_OUTCOMES = {"promising_lead", "possible_lead"}


@dataclass(frozen=True, slots=True)
class PriorLeadState:
    status: str  # 'pending' | 'emitted' | 'removed'
    tier: str  # 'promising_lead' | 'possible_lead'
    qualifying_domain_count: int
    first_pending_at: str


@dataclass(frozen=True, slots=True)
class QueueDecision:
    should_upsert: bool
    should_remove: bool
    eligibility_reason: str | None
    to_status: str | None
    first_pending_at: str | None
    removed_reason: str | None = None


def decide_queue_transition(
    *,
    prior: PriorLeadState | None,
    outcome: LeadOutcome,
    matching_page_found: bool,
    now: str,
) -> QueueDecision:
    if matching_page_found:
        if prior is not None and prior.status != "removed":
            return QueueDecision(
                should_upsert=False,
                should_remove=True,
                eligibility_reason=None,
                to_status="removed",
                first_pending_at=None,
                removed_reason="matching_page_found",
            )
        return QueueDecision(
            should_upsert=False,
            should_remove=False,
            eligibility_reason=None,
            to_status=None,
            first_pending_at=None,
        )

    if outcome.outcome not in _POSITIVE_OUTCOMES:
        # A neutral/negative outcome never retracts an existing positive
        # entry — only new positive evidence changes an active queue row.
        return QueueDecision(
            should_upsert=False,
            should_remove=False,
            eligibility_reason=None,
            to_status=None,
            first_pending_at=None,
        )

    if prior is None:
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="new",
            to_status="pending",
            first_pending_at=now,
        )

    if prior.tier == "possible_lead" and outcome.outcome == "promising_lead":
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="promoted",
            to_status="pending",
            first_pending_at=prior.first_pending_at,
        )

    if outcome.qualifying_domain_count > prior.qualifying_domain_count:
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="strengthened",
            to_status="pending",
            first_pending_at=prior.first_pending_at,
        )

    # Same tier, no new qualifying domain: repeated evidence from an
    # already-counted domain is not independently resurfacing-eligible.
    return QueueDecision(
        should_upsert=False,
        should_remove=False,
        eligibility_reason=None,
        to_status=None,
        first_pending_at=None,
    )
