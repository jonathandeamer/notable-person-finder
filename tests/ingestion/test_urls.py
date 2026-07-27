from __future__ import annotations

import pytest

from notable_person_finder.ingestion.urls import (
    UnusableArticleUrl,
    canonicalize_article_url,
    publisher_key,
)


@pytest.mark.parametrize(
    ("name", "url", "expected"),
    [
        (
            "lowercase_scheme",
            "HTTPS://example.com/path",
            "https://example.com/path",
        ),
        (
            "lowercase_host",
            "https://Example.COM/path",
            "https://example.com/path",
        ),
        (
            "strip_default_https_port",
            "https://example.com:443/path",
            "https://example.com/path",
        ),
        (
            "strip_default_http_port",
            "http://example.com:80/path",
            "http://example.com/path",
        ),
        (
            "keep_non_default_port",
            "https://example.com:8443/path",
            "https://example.com:8443/path",
        ),
        (
            "strip_trailing_dot_from_host",
            "https://example.com./path",
            "https://example.com/path",
        ),
        (
            "keep_http_as_given",
            "http://example.com/path",
            "http://example.com/path",
        ),
        (
            "keep_https_as_given",
            "https://example.com/path",
            "https://example.com/path",
        ),
        (
            "strip_fragment",
            "https://example.com/path#section-2",
            "https://example.com/path",
        ),
        (
            "strip_utm_family",
            "https://example.com/a?utm_source=x&utm_medium=y&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "keep_uppercase_utm_is_still_stripped_by_prefix",
            # The utm_ prefix match is case-sensitive; an upper-cased prefix
            # like UTM_source is NOT covered by the "utm_" rule and survives.
            "https://example.com/a?UTM_source=x&keep=1",
            "https://example.com/a?UTM_source=x&keep=1",
        ),
        (
            "strip_fbclid",
            "https://example.com/a?fbclid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_gclid",
            "https://example.com/a?gclid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_mc_cid",
            "https://example.com/a?mc_cid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_mc_eid",
            "https://example.com/a?mc_eid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_igshid",
            "https://example.com/a?igshid=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_ref",
            "https://example.com/a?ref=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_ref_src",
            "https://example.com/a?ref_src=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_lowercase_s",
            "https://example.com/a?s=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_lowercase_cmp",
            "https://example.com/a?cmp=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "strip_uppercase_cmp",
            "https://example.com/a?CMP=abc&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "mixed_case_cmp_is_not_stripped",
            # Deliberate reading: matching is exact and case-sensitive, so
            # "Cmp" is neither "cmp" nor "CMP" and is not on the strip list.
            "https://example.com/a?Cmp=abc&keep=1",
            "https://example.com/a?Cmp=abc&keep=1",
        ),
        (
            "surviving_parameters_keep_original_order",
            "https://example.com/a?z=1&utm_source=x&a=2",
            "https://example.com/a?z=1&a=2",
        ),
        (
            "valueless_tracking_param_is_stripped",
            "https://example.com/a?ref&keep=1",
            "https://example.com/a?keep=1",
        ),
        (
            "valueless_non_tracking_param_is_preserved",
            "https://example.com/a?keep&other=1",
            "https://example.com/a?keep&other=1",
        ),
        (
            "stripping_everything_leaves_no_query_marker",
            "https://example.com/a?utm_source=x&ref=y",
            "https://example.com/a",
        ),
        (
            "no_query_string_is_untouched",
            "https://example.com/a",
            "https://example.com/a",
        ),
        (
            "trailing_slash_is_preserved",
            "https://example.com/a/",
            "https://example.com/a/",
        ),
        (
            "no_trailing_slash_is_not_added",
            "https://example.com",
            "https://example.com",
        ),
        (
            "path_case_is_preserved",
            "https://example.com/PathCase",
            "https://example.com/PathCase",
        ),
        (
            "dot_segments_are_left_to_urllib",
            "https://example.com/a/../b",
            "https://example.com/a/../b",
        ),
        (
            "percent_hex_digits_are_uppercased",
            "https://example.com/a%2fb",
            "https://example.com/a%2Fb",
        ),
        (
            "percent_encoded_unreserved_characters_are_decoded",
            "https://example.com/%7Euser",
            "https://example.com/~user",
        ),
        (
            "percent_encoded_reserved_characters_stay_encoded",
            "https://example.com/a%2Fb%3Fc",
            "https://example.com/a%2Fb%3Fc",
        ),
    ],
)
def test_canonicalize_article_url_rules(name: str, url: str, expected: str) -> None:
    assert canonicalize_article_url(url) == expected, name


def test_convergence_of_feed_and_search_shaped_urls_for_the_same_article() -> None:
    feed_shaped = (
        "https://example.com/2026/07/27/some-article"
        "?utm_source=feed&utm_medium=rss#comments"
    )
    search_shaped = "https://example.com/2026/07/27/some-article?s=search-term"
    assert canonicalize_article_url(feed_shaped) == canonicalize_article_url(
        search_shaped
    )
    assert (
        canonicalize_article_url(feed_shaped)
        == "https://example.com/2026/07/27/some-article"
    )


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/x",
        "file:///etc/passwd",
        "//example.com/x",
        "example.com/x",
    ],
)
def test_non_http_schemes_are_rejected(url: str) -> None:
    with pytest.raises(UnusableArticleUrl):
        canonicalize_article_url(url)


def test_missing_host_is_rejected() -> None:
    with pytest.raises(UnusableArticleUrl):
        canonicalize_article_url("https:///no-host")


def test_embedded_credentials_are_rejected() -> None:
    with pytest.raises(UnusableArticleUrl):
        canonicalize_article_url("https://user:pass@example.com/x")


def test_unusable_article_url_carries_a_reason() -> None:
    try:
        canonicalize_article_url("ftp://example.com/x")
    except UnusableArticleUrl as exc:
        assert exc.reason
    else:
        pytest.fail("expected UnusableArticleUrl")


@pytest.mark.parametrize(
    ("name", "canonical_url", "expected"),
    [
        ("simple_domain", "https://example.com/path", "example.com"),
        ("subdomain_is_dropped", "https://news.example.com/path", "example.com"),
        ("www_prefix_is_stripped", "https://www.example.com/path", "example.com"),
        (
            "www_prefix_stripped_before_subdomain_check",
            "https://www.news.example.com/path",
            "example.com",
        ),
        ("co_uk_exception", "https://www.bbc.co.uk/news", "bbc.co.uk"),
        ("org_uk_exception", "https://www.charity.org.uk/x", "charity.org.uk"),
        ("ac_uk_exception", "https://www.university.ac.uk/x", "university.ac.uk"),
        ("com_au_exception", "https://www.example.com.au/x", "example.com.au"),
        ("co_jp_exception", "https://www.example.co.jp/x", "example.co.jp"),
        ("co_nz_exception", "https://www.example.co.nz/x", "example.co.nz"),
        ("com_br_exception", "https://www.example.com.br/x", "example.com.br"),
    ],
)
def test_publisher_key_registrable_domain(
    name: str, canonical_url: str, expected: str
) -> None:
    assert publisher_key(canonical_url) == expected, name
