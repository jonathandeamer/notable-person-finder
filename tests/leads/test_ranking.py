from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.ranking import QueueEntry, rank_key


def _lead(
    outcome="promising_lead",
    domains=2,
    signals=0,
    wikipedia="no_matching_page_found",
    freshest="2026-07-01T00:00:00Z",
    visibility="full",
):
    return (
        LeadOutcome(
            outcome=outcome,
            qualifying_domain_count=domains,
            qualifying_articles=(),
            contributing_signals=(),
            wikipedia_outcome=wikipedia,
        ),
        signals,
        freshest,
        visibility,
    )


def _entry(person_id=1, eligibility="new", first_pending_at="2026-08-01T00:00:00Z"):
    return QueueEntry(
        person_id=person_id,
        eligibility_reason=eligibility,
        first_pending_at=first_pending_at,
    )


def test_promising_ranks_before_possible():
    lead_a, *_ = _lead(outcome="promising_lead")
    lead_b, *_ = _lead(outcome="possible_lead")
    key_a = rank_key(
        _entry(person_id=1),
        lead_a,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    key_b = rank_key(
        _entry(person_id=2),
        lead_b,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    assert key_a < key_b


def test_starved_entry_ranks_ahead_of_fresher_same_tier_entry():
    # person_id deliberately reversed relative to staleness (starved=99 >
    # fresh=1): person_id is rank_key's final tie-breaker, so if the
    # starvation key were dropped from the tuple, the tie-breaker alone
    # would sort the fresh entry first (1 < 99) -- disagreeing with the
    # assertion below. Only the (correct) starvation logic makes the
    # starved entry sort ahead here; with matching low-to-high person_ids
    # this test would pass for the wrong reason with or without it.
    lead, *_ = _lead(outcome="possible_lead")
    starved = _entry(person_id=99, first_pending_at="2025-01-01T00:00:00Z")
    fresh = _entry(person_id=1, first_pending_at="2026-08-01T00:00:00Z")
    starved_key = rank_key(
        starved,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-06-01T00:00:00Z",
    )
    fresh_key = rank_key(
        fresh,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-06-01T00:00:00Z",
    )
    assert starved_key < fresh_key


def test_starved_possible_still_sorts_behind_fresh_promising():
    starved_possible, *_ = _lead(outcome="possible_lead")
    fresh_promising, *_ = _lead(outcome="promising_lead")
    starved_entry = _entry(person_id=1, first_pending_at="2025-01-01T00:00:00Z")
    fresh_entry = _entry(person_id=2, first_pending_at="2026-08-01T00:00:00Z")
    starved_key = rank_key(
        starved_entry,
        starved_possible,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-06-01T00:00:00Z",
    )
    promising_key = rank_key(
        fresh_entry,
        fresh_promising,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-06-01T00:00:00Z",
    )
    assert promising_key < starved_key


def test_eligibility_reason_tie_break_new_before_reminder():
    lead, *_ = _lead()
    new_entry = _entry(person_id=1, eligibility="new")
    reminder_entry = _entry(person_id=2, eligibility="reminder")
    new_key = rank_key(
        new_entry,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    reminder_key = rank_key(
        reminder_entry,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    assert new_key < reminder_key


def test_person_id_is_final_deterministic_tiebreak():
    lead, *_ = _lead()
    entry_low = _entry(person_id=1)
    entry_high = _entry(person_id=2)
    key_low = rank_key(
        entry_low,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    key_high = rank_key(
        entry_high,
        lead,
        positive_signal_count=0,
        best_evidence_visibility="full",
        freshest_qualifying_article_at="2026-08-01T00:00:00Z",
        starvation_cutoff="2026-01-01T00:00:00Z",
    )
    assert key_low < key_high
