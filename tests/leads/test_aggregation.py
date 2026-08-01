from notable_person_finder.leads.aggregation import (
    ArticleEvidence,
    SignalEvidence,
    aggregate_lead,
)


def _article(
    *,
    domain="example.com",
    canonical_domain=None,
    same_person=True,
    depth="significant",
    content_qualifying=True,
    editorially_independent=True,
    screening_status="curated_eligible",
) -> ArticleEvidence:
    return ArticleEvidence(
        person_article_assessment_id=1,
        domain=domain,
        canonical_domain=canonical_domain or domain,
        same_person=same_person,
        coverage_depth=depth,
        content_qualifying=content_qualifying,
        editorially_independent=editorially_independent,
        screening_status=screening_status,
    )


def test_two_distinct_domains_promising():
    articles = [
        _article(domain="a.example"),
        _article(domain="b.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "promising_lead"
    assert outcome.qualifying_domain_count == 2


def test_one_domain_possible():
    articles = [_article(domain="a.example")]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"
    assert outcome.qualifying_domain_count == 1


def test_unclassified_significant_coverage_is_possible():
    articles = [
        _article(
            domain="unclassified.example",
            depth="significant",
            screening_status="unclassified",
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"


def test_completed_nothing_qualifies_is_insufficient_evidence():
    articles = [
        _article(
            domain="a.example",
            same_person=False,
            content_qualifying=False,
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "insufficient_evidence"


def test_missing_required_work_is_assessment_incomplete():
    outcome = aggregate_lead(
        articles=[],
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
        assessment_terminal=False,
    )
    assert outcome.outcome == "assessment_incomplete"
    assert outcome.incompleteness_reason is not None


def test_k7_positive_outcome_survives_later_incomplete_flag():
    """A qualifying article already exists; a later incomplete flag must not
    downgrade the outcome (K7)."""
    articles = [
        _article(domain="a.example"),
        _article(domain="b.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=False,
    )
    assert outcome.outcome == "promising_lead"


def test_k8_wikipedia_outcome_is_recorded_but_not_a_possible_lead_reason():
    articles = [
        _article(
            domain="a.example",
            same_person=False,
            content_qualifying=False,
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "insufficient_evidence"
    assert outcome.wikipedia_outcome == "uncertain_identity"


def test_canonical_domain_aliasing_counts_as_one_publisher():
    articles = [
        _article(domain="a.example", canonical_domain="shared.example"),
        _article(domain="b.example", canonical_domain="shared.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"
    assert outcome.qualifying_domain_count == 1


def test_grounded_transferable_signal_is_possible_lead():
    outcome = aggregate_lead(
        articles=[],
        signals=[
            SignalEvidence(
                article_assessment_signal_id=1,
                signal_kind="attention",
                category="award",
                transferable=True,
            )
        ],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "possible_lead"
