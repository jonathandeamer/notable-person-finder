"""Deterministic ranking and shortlist selection."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from notable.rank import Lead, assess, identity_key, lead_to_log_dict, shortlist


def _mention(name: str = "John Doe") -> MagicMock:
    mention = MagicMock()
    mention.exact_name = name
    mention.canonical_name = name
    mention.rationale = "why"
    return mention


def _item(url: str = "https://feed.example/item") -> MagicMock:
    item = MagicMock()
    item.url = url
    item.publisher_label = "Feed"
    return item


def _config(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {
        "promising_domain_threshold": 2,
        "digest_size": 10,
        "resurface_after_days": 30,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _wiki(outcome: str = "no_matching_page") -> MagicMock:
    verdict = MagicMock()
    verdict.outcome = outcome
    return verdict


def _article(
    url: str,
    *,
    person_relation: str = "same_person",
    coverage_depth: str = "significant",
    screening_status: str = "curated_eligible",
    subject_relationship: str = "editorially_independent",
    content_types: tuple[str, ...] = ("reporting",),
) -> MagicMock:
    article = MagicMock()
    article.url = url
    article.person_relation = person_relation
    article.coverage_depth = coverage_depth
    article.screening_status = screening_status
    article.subject_relationship = subject_relationship
    article.content_types = content_types
    return article


def _lead(
    key: str,
    *,
    outcome: str = "possible_lead",
    rank_tuple: tuple | None = (1, 0, -1, "x"),
    source_url: str = "https://a.test/1",
    domains: tuple[str, ...] = (),
) -> Lead:
    return Lead(
        identity_key=key,
        display_name=key.title(),
        source_url=source_url,
        publisher_label="p",
        wikipedia_verdict=None,
        outcome=outcome,
        article_assessments=(),
        rank_tuple=rank_tuple if outcome != "insufficient_evidence" else None,
        qualifying_domains=domains,
    )


def test_identity_key_normalizes():
    assert identity_key("  Ana   Poy  ") == "ana poy"
    assert identity_key("Ana\u00a0Poy") == "ana poy"
    assert identity_key("Ana Poy") != identity_key("Ana Poye")


def test_assess_empty_articles_returns_insufficient_evidence():
    lead = assess(_mention(), _wiki(), (), _config(), _item())
    assert lead.outcome == "insufficient_evidence"
    assert lead.rank_tuple is None


def test_assess_retains_lead_fields():
    lead = assess(_mention(), _wiki(), (), _config(), _item("https://example.com/item"))
    assert lead.display_name == "John Doe"
    assert lead.source_url == "https://example.com/item"
    assert lead.publisher_label == "Feed"


def test_assess_single_qualifying_article_is_possible_lead():
    article = _article("https://example.com/a")
    lead = assess(_mention(), _wiki(), (article,), _config(), _item())
    assert lead.outcome == "possible_lead"
    assert lead.qualifying_domains == ("example.com",)


def test_assess_meets_promising_domain_threshold():
    articles = (
        _article("https://example.com/a1"),
        _article("https://other.com/a2"),
    )
    lead = assess(_mention(), _wiki(), articles, _config(), _item())
    assert lead.outcome == "promising_lead"


def test_assess_uses_config_threshold_without_hasattr_soft_fail():
    """Real Config-shaped objects must promote; no hasattr gate."""
    articles = (
        _article("https://example.com/a1"),
        _article("https://other.com/a2"),
    )
    lead = assess(
        _mention(), _wiki(), articles, _config(promising_domain_threshold=2), _item()
    )
    assert lead.outcome == "promising_lead"
    one_domain = assess(
        _mention(),
        _wiki(),
        (_article("https://example.com/a1"),),
        _config(promising_domain_threshold=1),
        _item(),
    )
    assert one_domain.outcome == "promising_lead"


def test_assess_unclassified_significant_is_possible_lead():
    article = _article("https://example.com/a", screening_status="unclassified")
    lead = assess(_mention(), _wiki(), (article,), _config(), _item())
    assert lead.outcome == "possible_lead"


def test_assess_eligible_passing_content_qualifying_is_possible_lead():
    """Reason 3: fully eligible but passing depth is possible, not insufficient."""
    article = _article("https://example.com/a", coverage_depth="passing")
    lead = assess(_mention(), _wiki(), (article,), _config(), _item())
    assert lead.outcome == "possible_lead"
    assert lead.qualifying_domains == ()


def test_assess_rank_tuple():
    article = _article("https://example.com/a1")
    lead = assess(_mention(), _wiki("no_matching_page"), (article,), _config(), _item())
    assert lead.rank_tuple == (1, 0, -1, "john doe")


def test_shortlist_removes_insufficient_and_sorts():
    store = MagicMock()
    store.is_suppressed.return_value = False
    leads, keys = shortlist(
        [
            _lead(
                "bob smith", outcome="possible_lead", rank_tuple=(1, 0, -1, "bob smith")
            ),
            _lead("john doe", outcome="insufficient_evidence", rank_tuple=None),
            _lead(
                "jane doe",
                outcome="promising_lead",
                rank_tuple=(0, 0, -2, "jane doe"),
            ),
        ],
        store,
        _config(),
    )
    assert [lead.identity_key for lead in leads] == ["jane doe", "bob smith"]
    assert keys == ["jane doe", "bob smith"]


def test_shortlist_suppresses_recently_surfaced():
    store = MagicMock()
    store.is_suppressed.side_effect = lambda key, max_days: key == "ana poy"
    leads, keys = shortlist(
        [
            _lead(
                "ana poy", outcome="promising_lead", rank_tuple=(0, 0, -2, "ana poy")
            ),
            _lead("bo li", outcome="possible_lead", rank_tuple=(1, 0, -1, "bo li")),
        ],
        store,
        _config(),
    )
    assert [lead.identity_key for lead in leads] == ["bo li"]
    assert keys == ["bo li"]


def test_shortlist_collapse_before_cut_preserves_other_people():
    """Top-N same-name mentions must not fill the digest alone."""
    store = MagicMock()
    store.is_suppressed.return_value = False
    same_name = [
        _lead(
            "ana poy",
            outcome="promising_lead",
            rank_tuple=(0, 0, -3, "ana poy"),
            source_url=f"https://a.test/{n}",
        )
        for n in range(5)
    ]
    other = _lead(
        "bo li",
        outcome="possible_lead",
        rank_tuple=(1, 0, -1, "bo li"),
        source_url="https://b.test/1",
    )
    leads, keys = shortlist(same_name + [other], store, _config(digest_size=2))
    assert [lead.identity_key for lead in leads] == ["ana poy", "bo li"]
    assert keys == ["ana poy", "bo li"]
    assert leads[0].namesake_urls == tuple(f"https://a.test/{n}" for n in range(1, 5))


def test_namesake_safety_does_not_merge_outcomes():
    """Two mentions sharing a key keep separate evidence; never union domains."""
    a = assess(
        _mention("Ana Poy"),
        _wiki(),
        (_article("https://example.com/a"),),
        _config(),
        _item("https://feed.example/1"),
    )
    b = assess(
        _mention("Ana Poy"),
        _wiki(),
        (_article("https://other.com/b"),),
        _config(),
        _item("https://feed.example/2"),
    )
    assert a.outcome == "possible_lead"
    assert b.outcome == "possible_lead"
    assert a.qualifying_domains == ("example.com",)
    assert b.qualifying_domains == ("other.com",)

    store = MagicMock()
    store.is_suppressed.return_value = False
    leads, _ = shortlist([a, b], store, _config())
    assert len(leads) == 1
    assert leads[0].outcome == "possible_lead"
    assert leads[0].qualifying_domains == ("example.com",)
    assert leads[0].namesake_urls == ("https://feed.example/2",)


def test_shortlist_does_not_mutate_input_leads():
    store = MagicMock()
    store.is_suppressed.return_value = False
    original = _lead(
        "ana poy",
        outcome="possible_lead",
        rank_tuple=(1, 0, -1, "ana poy"),
        source_url="https://a.test/1",
    )
    other = _lead(
        "ana poy",
        outcome="possible_lead",
        rank_tuple=(1, 1, -1, "ana poy"),
        source_url="https://a.test/2",
    )
    shortlist([original, other], store, _config())
    assert original.namesake_urls == ()


def test_lead_to_log_dict_serializes_rank_key_and_detail():
    lead = assess(
        _mention(),
        _wiki(),
        (_article("https://example.com/a"),),
        _config(),
        _item(),
    )
    row = lead_to_log_dict(lead)
    assert row["identity_key"] == "john doe"
    assert row["outcome"] == "possible_lead"
    assert row["rank_key"] == [1, 0, -1, "john doe"]
    assert row["detail"]["qualifying_domains"] == ["example.com"]
    ie = lead_to_log_dict(assess(_mention(), _wiki(), (), _config(), _item()))
    assert ie["rank_key"] == []


def test_assess_wikipedia_page_found_in_coverage_is_insufficient_evidence():
    article = _article(
        "https://en.wikipedia.org/wiki/Agnes_Martin", person_relation="same_person"
    )
    lead = assess(_mention(), _wiki(), (article,), _config(), _item())
    assert lead.outcome == "insufficient_evidence"
    assert "Wikipedia page" in lead.explanation
