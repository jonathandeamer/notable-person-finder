import tomllib
from importlib import resources
from pathlib import Path

import pytest

PROMPTS = ("detect_people.md", "match_wikipedia_identity.md", "assess_article.md")


@pytest.mark.parametrize("name", PROMPTS)
def test_prompt_is_packaged_and_non_empty(name):
    text = resources.files("notable.prompts").joinpath(name).read_text("utf-8")
    assert text.strip(), f"{name} is empty"


def test_resolve_person_entity_prompt_is_not_ported():
    # Durable person identity is deferred; its prompt returns with it.
    assert not resources.files("notable.prompts").joinpath(
        "resolve_person_entity.md"
    ).is_file()


def test_feed_list_parses_and_has_ten_feeds():
    data = tomllib.loads(Path("config/feeds.example.toml").read_text("utf-8"))
    feeds = data["feeds"]
    assert len(feeds) == 10
    assert {"key", "label", "url"} <= set(feeds[0])
    keys = [feed["key"] for feed in feeds]
    assert len(set(keys)) == len(keys), "feed keys must be unique"


def test_source_policy_parses_and_carries_rules():
    data = tomllib.loads(
        Path("config/source_policies/visual_arts.toml").read_text("utf-8")
    )
    assert data["key"] == "visual-arts-en-sources"
    statuses = {rule["status"] for rule in data["rules"]}
    assert "curated_eligible" in statuses


def test_source_policy_carries_no_policy_bookkeeping():
    # The spec ports this as curated data minus the prior programme's
    # policy-versioning fields. Left in, they read as a live contract.
    data = tomllib.loads(
        Path("config/source_policies/visual_arts.toml").read_text("utf-8")
    )
    for rule in data["rules"]:
        assert "review_date" not in rule
        assert "provenance_url" not in rule


def test_every_source_policy_rule_still_has_a_status():
    # Guards the strip: a rule table emptied by grep would parse and pass the
    # test above while silently dropping a publisher classification.
    data = tomllib.loads(
        Path("config/source_policies/visual_arts.toml").read_text("utf-8")
    )
    assert data["rules"], "the policy has no rules"
    for rule in data["rules"]:
        assert rule.get("status"), f"rule without a status: {rule}"
