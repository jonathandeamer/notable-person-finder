from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState, decide_queue_transition


def _outcome(outcome, domains=1):
    return LeadOutcome(
        outcome=outcome,
        qualifying_domain_count=domains,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )


def test_new_positive_outcome_with_no_prior_row_is_new():
    decision = decide_queue_transition(
        prior=None,
        outcome=_outcome("possible_lead"),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_upsert
    assert decision.eligibility_reason == "new"
    assert decision.to_status == "pending"


def test_possible_to_promising_is_promoted():
    prior = PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("promising_lead", domains=2),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_upsert
    assert decision.eligibility_reason == "promoted"


def test_repeated_evidence_from_already_counted_domain_is_not_eligible():
    prior = PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("possible_lead", domains=1),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert not decision.should_upsert


def test_neutral_outcome_leaves_existing_row_untouched():
    prior = PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("insufficient_evidence", domains=0),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert not decision.should_upsert
    assert not decision.should_remove


def test_matching_page_found_removes_existing_row():
    prior = PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("possible_lead", domains=1),
        matching_page_found=True,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_remove
    assert decision.removed_reason == "matching_page_found"


def test_first_pending_at_preserved_across_updates():
    prior = PriorLeadState(
        status="pending",
        tier="possible_lead",
        qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("promising_lead", domains=2),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.first_pending_at == "2026-07-01T00:00:00Z"
