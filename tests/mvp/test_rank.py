from notable.rank import Lead, assess


def test_assess_empty_articles_returns_insufficient_evidence():
    """An empty article tuple is a valid terminal result and produces insufficient_evidence."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    wiki = MagicMock()
    
    # We might need a real config, or we can just mock the fields we need
    # But let's try just passing None if it doesn't complain yet.
    config = None 
    
    lead = assess(mention, wiki, articles=(), config=config)
    
    assert lead.outcome == "insufficient_evidence"

def test_assess_retains_lead_fields():
    """Lead retains required context fields from inputs."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    
    wiki = MagicMock()
    item = MagicMock()
    item.url = "https://example.com/item"
    item.publisher_label = "Example Times"
    
    lead = assess(mention, wiki, articles=(), config=None, item=item)
    
    assert lead.display_name == "John Doe"
    assert lead.source_url == "https://example.com/item"
    assert lead.publisher_label == "Example Times"
    assert lead.wikipedia_verdict == wiki
    assert lead.article_assessments == ()

def test_assess_single_qualifying_article_is_possible_lead():
    """A single qualifying article produces possible_lead when below threshold."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    
    article = MagicMock()
    article.person_relation = "same_person"
    article.coverage_depth = "significant"
    article.screening_status = "curated_eligible"
    article.subject_relationship = "editorially_independent"
    article.content_types = ("news_report",) # Not a disqualifying type
    article.url = "https://example.com/article"
    
    config = MagicMock()
    config.promising_domain_threshold = 2
    
    item = MagicMock()
    item.url = "http://example.com"
    item.publisher_label = "Example"
    
    lead = assess(mention, MagicMock(), articles=(article,), config=config, item=item)
    
    assert lead.outcome == "possible_lead"

def test_assess_meets_promising_domain_threshold():
    """Returns promising_lead when qualifying articles span enough distinct domains."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    
    # Create two qualifying articles
    a1 = MagicMock()
    a1.person_relation = "same_person"
    a1.coverage_depth = "significant"
    a1.screening_status = "curated_eligible"
    a1.subject_relationship = "editorially_independent"
    a1.content_types = ()
    a1.url = "https://example.com/article1"
    
    a2 = MagicMock()
    a2.person_relation = "same_person"
    a2.coverage_depth = "significant"
    a2.screening_status = "curated_eligible"
    a2.subject_relationship = "editorially_independent"
    a2.content_types = ()
    a2.url = "https://other.com/article2"
    
    config = MagicMock()
    config.promising_domain_threshold = 2
    
    item = MagicMock()
    item.url = "http://example.com"
    item.publisher_label = "Example"
    
    lead = assess(mention, MagicMock(), articles=(a1, a2), config=config, item=item)
    
    assert lead.outcome == "promising_lead"

def test_assess_unclassified_significant_is_possible_lead():
    """A same-person significant article from an unclassified publisher is a possible_lead."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    
    article = MagicMock()
    article.person_relation = "same_person"
    article.coverage_depth = "significant"
    article.screening_status = "unclassified" # Fails fully qualifying
    article.subject_relationship = "editorially_independent"
    article.content_types = ()
    article.url = "https://example.com/article"
    
    config = MagicMock()
    config.promising_domain_threshold = 2
    
    item = MagicMock()
    item.url = "http://example.com"
    item.publisher_label = "Example"
    
    lead = assess(mention, MagicMock(), articles=(article,), config=config, item=item)
    
    assert lead.outcome == "possible_lead"

def test_assess_rank_tuple():
    """Rank tuple is (outcome_rank, wikipedia_rank, -domain_count, identity_key)."""
    from unittest.mock import MagicMock
    mention = MagicMock()
    mention.exact_name = "John Doe"
    mention.rationale = "why"
    
    wiki = MagicMock()
    wiki.outcome = "no_matching_page"
    
    a1 = MagicMock()
    a1.person_relation = "same_person"
    a1.coverage_depth = "significant"
    a1.screening_status = "curated_eligible"
    a1.subject_relationship = "editorially_independent"
    a1.content_types = ()
    a1.url = "https://example.com/article1"
    
    config = MagicMock()
    config.promising_domain_threshold = 2
    
    item = MagicMock()
    item.url = "http://example.com"
    item.publisher_label = "Example"
    
    lead = assess(mention, wiki, articles=(a1,), config=config, item=item)
    
    assert lead.outcome == "possible_lead"
    # outcome_rank (promising=0, possible=1), wiki_rank (no_page=0, uncertain=1), domain_count=1 -> -1, identity_key
    expected_tuple = (1, 0, -1, "john doe")
    assert lead.rank_tuple == expected_tuple

def test_shortlist_removes_insufficient_and_sorts():
    """Removes insufficient_evidence and sorts by rank_tuple."""
    from notable.rank import shortlist
    
    l1 = Lead(
        identity_key="jane doe", display_name="Jane", source_url="u1", publisher_label="p",
        wikipedia_verdict=None, outcome="promising_lead", article_assessments=(),
        rank_tuple=(0, 0, -2, "jane doe")
    )
    l2 = Lead(
        identity_key="john doe", display_name="John", source_url="u2", publisher_label="p",
        wikipedia_verdict=None, outcome="insufficient_evidence", article_assessments=(),
        rank_tuple=None
    )
    l3 = Lead(
        identity_key="bob smith", display_name="Bob", source_url="u3", publisher_label="p",
        wikipedia_verdict=None, outcome="possible_lead", article_assessments=(),
        rank_tuple=(1, 0, -1, "bob smith")
    )
    
    from unittest.mock import MagicMock
    config = MagicMock()
    config.digest_size = 10
    config.resurface_after_days = 30
    
    store = MagicMock()
    store.is_suppressed.return_value = False
    
    leads, keys = shortlist([l3, l2, l1], store, config)
    
    assert [l.identity_key for l in leads] == ["jane doe", "bob smith"]
    assert keys == ["jane doe", "bob smith"]
