"""Source-policy load, first-match screening, fingerprint, discovery attach (K8/K10)."""

from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.coverage.screening import (
    DEFAULT_UNCLASSIFIED_RULE_ID,
    URL_UNUSABLE_RULE_ID,
    DiscoveryAttachKind,
    SourcePolicyError,
    attach_discovery_url,
    fingerprint_source_policy,
    load_source_policy,
    screen_url,
    source_policy_from_mapping,
)

_EXAMPLE_POLICY = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "source_policies"
    / "visual_arts.toml"
)

# Known-good fingerprint computed over the tracked policy's parsed content
# before config/source_policies/visual_arts.example.toml was renamed (via
# `git mv`) to config/source_policies/visual_arts.toml. The fingerprint is
# computed over parsed content, not the filename, so the rename must not
# change it (task 4, rule 4).
_FINGERPRINT_BEFORE_RENAME = (
    "fea3ae16c79b49e0800bfc28df360acab0fd00693bcd276be3a9e8a5a2b5b590"
)


def _minimal_policy_mapping(
    *,
    rules: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "key": "test-sources",
        "label": "Test policy",
        "rules": rules
        if rules is not None
        else [
            {
                "id": "eligible.example",
                "status": "curated_eligible",
                "match": {"host_suffix": "example.com"},
                "rationale": "test eligible",
                "review_date": "2026-07-24",
            },
            {
                "id": "ineligible.social",
                "status": "curated_ineligible",
                "match": {"host_suffix": "twitter.com"},
                "rationale": "social",
                "review_date": "2026-07-24",
            },
        ],
    }


def test_load_example_policy_has_stable_fingerprint() -> None:
    policy = load_source_policy(_EXAMPLE_POLICY)
    assert policy.schema_version == 1
    assert policy.key == "visual-arts-en-sources"
    assert len(policy.fingerprint) == 64
    assert policy.fingerprint == fingerprint_source_policy(policy)
    again = load_source_policy(_EXAMPLE_POLICY)
    assert again.fingerprint == policy.fingerprint


def test_tracked_policy_fingerprint_unchanged_by_rename() -> None:
    """The rename of the shipped policy file must not change its fingerprint.

    The fingerprint is computed over parsed content (schema_version, key,
    label, ordered rules), never the filename or path, so renaming
    visual_arts.example.toml to visual_arts.toml (task 4) must reproduce the
    exact fingerprint recorded before the rename.
    """
    policy = load_source_policy(_EXAMPLE_POLICY)
    assert policy.fingerprint == _FINGERPRINT_BEFORE_RENAME


def test_fingerprint_stable_across_mapping_reload() -> None:
    data = _minimal_policy_mapping()
    first = source_policy_from_mapping(data)
    second = source_policy_from_mapping(dict(data))
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


def test_fingerprint_changes_when_policy_rules_change() -> None:
    """Content-sensitive: rule body/order enter the fingerprint (not key alone)."""
    base = source_policy_from_mapping(_minimal_policy_mapping())
    # Same key/label, different rule status → must rehash.
    status_changed = source_policy_from_mapping(
        _minimal_policy_mapping(
            rules=[
                {
                    "id": "eligible.example",
                    "status": "curated_ineligible",
                    "match": {"host_suffix": "example.com"},
                    "rationale": "test eligible",
                    "review_date": "2026-07-24",
                },
                {
                    "id": "ineligible.social",
                    "status": "curated_ineligible",
                    "match": {"host_suffix": "twitter.com"},
                    "rationale": "social",
                    "review_date": "2026-07-24",
                },
            ]
        )
    )
    # Append an extra rule (same key/label).
    appended = source_policy_from_mapping(
        _minimal_policy_mapping(
            rules=[
                {
                    "id": "eligible.example",
                    "status": "curated_eligible",
                    "match": {"host_suffix": "example.com"},
                    "rationale": "test eligible",
                    "review_date": "2026-07-24",
                },
                {
                    "id": "ineligible.social",
                    "status": "curated_ineligible",
                    "match": {"host_suffix": "twitter.com"},
                    "rationale": "social",
                    "review_date": "2026-07-24",
                },
                {
                    "id": "eligible.extra",
                    "status": "curated_eligible",
                    "match": {"host_suffix": "extra.example"},
                    "rationale": "extra",
                    "review_date": "2026-07-24",
                },
            ]
        )
    )
    assert len(base.fingerprint) == 64
    assert all(c in "0123456789abcdef" for c in base.fingerprint)
    assert status_changed.fingerprint != base.fingerprint
    assert appended.fingerprint != base.fingerprint
    assert status_changed.fingerprint != appended.fingerprint
    assert len(status_changed.fingerprint) == 64
    assert len(appended.fingerprint) == 64


def test_first_match_wins_over_later_broader_rule() -> None:
    policy = source_policy_from_mapping(
        _minimal_policy_mapping(
            rules=[
                {
                    "id": "eligible.sub.example",
                    "status": "curated_eligible",
                    "match": {"host_exact": "news.example.com"},
                    "rationale": "specific host first",
                    "review_date": "2026-07-24",
                },
                {
                    "id": "ineligible.example",
                    "status": "curated_ineligible",
                    "match": {"host_suffix": "example.com"},
                    "rationale": "broader later",
                    "review_date": "2026-07-24",
                },
            ]
        )
    )
    decision = screen_url(policy, url="https://news.example.com/story")
    assert decision.rule_id == "eligible.sub.example"
    assert decision.rule_status == "curated_eligible"
    assert decision.canonical_url is not None
    assert decision.publisher_key == "example.com"


def test_no_match_is_unclassified_default() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    decision = screen_url(policy, url="https://unknown-publisher.test/a")
    assert decision.rule_id == DEFAULT_UNCLASSIFIED_RULE_ID
    assert decision.rule_status == "unclassified"
    assert decision.publisher_key == "unknown-publisher.test"


def test_host_suffix_does_not_match_suffix_lookalike() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    decision = screen_url(policy, url="https://notexample.com/story")
    assert decision.rule_status == "unclassified"
    decision_real = screen_url(policy, url="https://www.example.com/story")
    assert decision_real.rule_status == "curated_eligible"
    assert decision_real.rule_id == "eligible.example"


def test_publisher_key_exact_match() -> None:
    policy = source_policy_from_mapping(
        _minimal_policy_mapping(
            rules=[
                {
                    "id": "eligible.by-key",
                    "status": "curated_eligible",
                    "match": {"publisher_key_exact": "example.co.uk"},
                    "rationale": "key match",
                    "review_date": "2026-07-24",
                }
            ]
        )
    )
    decision = screen_url(policy, url="https://www.example.co.uk/article")
    assert decision.rule_status == "curated_eligible"
    assert decision.rule_id == "eligible.by-key"
    assert decision.publisher_key == "example.co.uk"


def test_path_prefix_match() -> None:
    policy = source_policy_from_mapping(
        _minimal_policy_mapping(
            rules=[
                {
                    "id": "eligible.path",
                    "status": "curated_eligible",
                    "match": {
                        "host_suffix": "atlasobscura.com",
                        "path_prefix": "/articles",
                    },
                    "rationale": "articles only",
                    "review_date": "2026-07-24",
                }
            ]
        )
    )
    ok = screen_url(policy, url="https://www.atlasobscura.com/articles/x")
    assert ok.rule_status == "curated_eligible"
    miss = screen_url(policy, url="https://www.atlasobscura.com/places/x")
    assert miss.rule_status == "unclassified"


def test_unusable_url_screens_as_unusable() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    decision = screen_url(policy, url="ftp://files.example.com/a")
    assert decision.rule_status == "unusable"
    assert decision.rule_id == URL_UNUSABLE_RULE_ID
    assert decision.canonical_url is None
    assert decision.publisher_key is None


def test_discovery_empty_url_is_neither() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    for url in (None, "", "   "):
        decision = attach_discovery_url(policy, url=url)
        assert decision.kind == DiscoveryAttachKind.NEITHER
        assert decision.screening is None
        assert decision.canonical_url is None


def test_discovery_unusable_url_is_screening_only() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    decision = attach_discovery_url(policy, url="javascript:alert(1)")
    assert decision.kind == DiscoveryAttachKind.SCREENING_ONLY
    assert decision.screening is not None
    assert decision.screening.rule_status == "unusable"
    assert decision.screening.rule_id == URL_UNUSABLE_RULE_ID
    assert decision.canonical_url is None


def test_discovery_usable_url_is_discovery_and_screening() -> None:
    policy = source_policy_from_mapping(_minimal_policy_mapping())
    decision = attach_discovery_url(
        policy, url="https://www.example.com/story?utm_source=x"
    )
    assert decision.kind == DiscoveryAttachKind.DISCOVERY_AND_SCREENING
    assert decision.screening is not None
    assert decision.screening.rule_status == "curated_eligible"
    assert decision.screening.canonical_url is not None
    assert decision.canonical_url == decision.screening.canonical_url
    # screening_id NOT NULL path: usable discovery always carries a decision
    assert decision.screening.rule_id == "eligible.example"


def test_load_missing_file_raises() -> None:
    with pytest.raises(SourcePolicyError, match="does not exist"):
        load_source_policy(Path("/nonexistent/source_policy.toml"))


def test_unsupported_schema_version_raises() -> None:
    with pytest.raises(SourcePolicyError):
        source_policy_from_mapping(
            {
                "schema_version": 99,
                "key": "x",
                "label": "y",
                "rules": [],
            }
        )


def test_example_policy_marks_social_ineligible_and_pilot_eligible() -> None:
    policy = load_source_policy(_EXAMPLE_POLICY)
    social = screen_url(policy, url="https://twitter.com/someone/status/1")
    assert social.rule_status == "curated_ineligible"
    nyt = screen_url(policy, url="https://www.nytimes.com/2024/01/01/arts/x.html")
    assert nyt.rule_status == "curated_eligible"
