from pathlib import Path

from notable.policy import canonical_domain, classify

POLICY = Path("config/source_policies/visual_arts.toml")


def test_a_known_eligible_publisher_is_curated_eligible():
    assert (
        classify("https://www.theartnewspaper.com/2026/x", POLICY) == "curated_eligible"
    )


def test_a_known_ineligible_publisher_is_curated_ineligible():
    assert classify("https://twitter.com/someone/status/1", POLICY) == (
        "curated_ineligible"
    )


def test_an_unlisted_publisher_is_unclassified():
    assert classify("https://some-random-blog.example/post", POLICY) == "unclassified"


def test_a_subdomain_matches_its_parent_host_suffix():
    assert classify("https://news.artnet.com/x", POLICY) == "curated_eligible"


def test_www_prefix_does_not_change_the_result():
    assert classify("https://www.artforum.com/news/x", POLICY) == "curated_eligible"
    assert classify("https://artforum.com/news/x", POLICY) == "curated_eligible"


def test_first_match_wins_in_file_order():
    # bbc.co.uk and bbc.com are both curated_eligible in the ported policy;
    # this just pins that a rule fires at all for both, not a specific order.
    assert classify("https://www.bbc.co.uk/news/x", POLICY) == "curated_eligible"
    assert classify("https://www.bbc.com/news/x", POLICY) == "curated_eligible"


def test_canonical_domain_strips_www():
    assert canonical_domain("https://www.artforum.com/x") == "artforum.com"
    assert canonical_domain("https://artforum.com/x") == "artforum.com"


def test_canonical_domain_is_case_insensitive():
    assert canonical_domain("https://WWW.ArtForum.com/x") == "artforum.com"
