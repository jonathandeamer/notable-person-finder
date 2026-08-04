from decimal import Decimal
from pathlib import Path

import pytest

from notable.config import load_config

EXAMPLE = Path("config/notable.example.toml")


def _write(tmp_path: Path, body: str, *, feeds: str | None = None) -> Path:
    (tmp_path / "feeds.toml").write_text(
        feeds
        or 'schema_version = 1\n[[feeds]]\nkey = "a"\nlabel = "A"\nurl = "https://a.test/f"\n',
        encoding="utf-8",
    )
    path = tmp_path / "notable.toml"
    path.write_text(body, encoding="utf-8")
    return path


BASE = """
schema_version = 1
feeds_file = "feeds.toml"
data_dir = "data"
digest_dir = "digests"

[secrets]
openrouter_api_key = "TEST_OR_KEY"
brave_api_key = "TEST_BRAVE_KEY"

[transport]
contact_url = "https://example.com/contact"

[cache]
dir = "cache"

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[tasks.detect_people]
model = "openai/gpt-5.4-mini"

[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"

[tasks.assess_article]
model = "openai/gpt-5.4-mini"
"""


def test_loads_feeds_and_resolves_paths_relative_to_the_config_file(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    config = load_config(_write(tmp_path, BASE))
    assert [feed.key for feed in config.feeds] == ["a"]
    assert config.data_dir == tmp_path / "data"
    assert config.cache.dir == tmp_path / "cache"


def test_secret_comes_from_the_environment_not_the_file(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-live")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    config = load_config(_write(tmp_path, BASE))
    assert config.openrouter_api_key == "sk-live"


def test_missing_secret_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_OR_KEY", raising=False)
    with pytest.raises(ValueError, match="TEST_OR_KEY"):
        load_config(_write(tmp_path, BASE))


def test_missing_brave_secret_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.delenv("TEST_BRAVE_KEY", raising=False)
    with pytest.raises(ValueError, match="TEST_BRAVE_KEY"):
        load_config(_write(tmp_path, BASE))


def test_unknown_key_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    with pytest.raises(ValueError):
        load_config(_write(tmp_path, BASE + '\nunexpected_key = "x"\n'))


def test_duplicate_feed_keys_are_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    feeds = (
        "schema_version = 1\n"
        '[[feeds]]\nkey = "a"\nlabel = "A"\nurl = "https://a.test/f"\n'
        '[[feeds]]\nkey = "a"\nlabel = "B"\nurl = "https://b.test/f"\n'
    )
    with pytest.raises(ValueError, match="duplicate feed key"):
        load_config(_write(tmp_path, BASE, feeds=feeds))


def test_defaults_match_the_spec(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    config = load_config(_write(tmp_path, BASE))
    assert config.cache.feed_ttl_seconds == 43200
    assert config.cache.discovery_ttl_seconds == 86400
    assert config.max_item_attempts == 3
    assert config.resurface_after_days == 30
    assert config.digest_size == 20
    assert config.promising_domain_threshold == 2
    assert config.detect.max_people == 8
    assert config.detect.max_completion_tokens == 4096
    assert config.budget_usd is None
    assert config.match.model == "openai/gpt-5.4-mini"
    assert config.match.max_completion_tokens == 4096
    assert config.match.reasoning_effort == "low"
    assert config.assess.model == "openai/gpt-5.4-mini"
    assert config.brave.endpoint == "https://api.search.brave.com/res/v1/web/search"
    assert config.brave.max_search_results == 10
    assert config.brave.max_articles_per_mention == 5
    assert config.coverage.max_article_characters == 6000
    assert config.coverage.max_article_bytes == 2_000_000
    assert config.coverage.source_policy_path == (
        tmp_path / "source_policies" / "visual_arts.toml"
    )
    assert config.mediawiki.endpoint == "https://en.wikipedia.org/w/api.php"
    assert config.mediawiki.max_candidates == 15
    assert config.mediawiki.max_extract_characters == 1200
    assert config.mediawiki.max_categories_per_page == 20
    assert config.mediawiki.maxlag_seconds == 5


def test_budget_parses_as_decimal(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    monkeypatch.setenv("TEST_BRAVE_KEY", "brave-test")
    body = BASE + '\n[budget]\nopenrouter_usd_per_run = "2.50"\n'
    assert load_config(_write(tmp_path, body)).budget_usd == Decimal("2.50")


def test_shipped_example_config_is_loadable(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    monkeypatch.setenv("BRAVE_API_KEY", "sk-test")
    config = load_config(EXAMPLE)
    assert len(config.feeds) == 10


def test_shipped_example_writes_runtime_state_where_gitignore_covers_it(monkeypatch):
    # Paths resolve relative to the config file, which lives in config/. The
    # example therefore uses `../`, and .gitignore matches `/data/`,
    # `/digests/`, `/cache/` at the repository root. If either side changes
    # alone, a live run's database and digests become committable.
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    monkeypatch.setenv("BRAVE_API_KEY", "sk-test")
    config = load_config(EXAMPLE)
    root = EXAMPLE.resolve().parent.parent
    assert config.data_dir == root / "data"
    assert config.digest_dir == root / "digests"
    assert config.cache.dir == root / "cache"

    ignored = Path(".gitignore").read_text("utf-8").splitlines()
    for pattern in ("/data/", "/digests/", "/cache/"):
        assert pattern in ignored, f"{pattern} missing from .gitignore"


def test_missing_match_task_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    body = BASE.replace(
        '[tasks.match_wikipedia_identity]\nmodel = "openai/gpt-5.4-mini"\n', ""
    )
    with pytest.raises(ValueError, match="tasks.match_wikipedia_identity"):
        load_config(_write(tmp_path, body))
