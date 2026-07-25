
import pytest
from pydantic import ValidationError

from notable_person_finder.config.models import (
    DomainProfileConfig,
    FeedsConfig,
    MainConfig,
)


def test_main_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        MainConfig.model_validate(
            {
                "schema_version": 1,
                "timezone": "Europe/Paris",
                "feeds_file": "feeds.toml",
                "domain_profile_file": "profile.toml",
                "unexpected": True,
            }
        )


def test_feeds_require_unique_keys_and_public_http_urls() -> None:
    with pytest.raises(ValidationError, match="duplicate feed key"):
        FeedsConfig.model_validate(
            {
                "schema_version": 1,
                "feeds": [
                    {"key": "art", "label": "Art", "url": "https://example.com/a"},
                    {"key": "art", "label": "Other", "url": "https://example.org/b"},
                ],
            }
        )

    with pytest.raises(ValidationError, match="embedded credentials"):
        FeedsConfig.model_validate(
            {
                "schema_version": 1,
                "feeds": [
                    {
                        "key": "bad",
                        "label": "Bad",
                        "url": "https://u:p@example.com/feed",
                    }
                ],
            }
        )


def test_domain_profile_accepts_only_known_attention_signals() -> None:
    profile = DomainProfileConfig.model_validate(
        {
            "schema_version": 1,
            "key": "visual-arts-en",
            "label": "English visual arts",
            "language": "en",
            "attention_examples": {
                "significant_recognition": ["major art prize"],
                "institutional_recognition": ["permanent museum collection"],
            },
        }
    )

    assert profile.key == "visual-arts-en"
    assert profile.attention_examples["significant_recognition"] == ("major art prize",)
