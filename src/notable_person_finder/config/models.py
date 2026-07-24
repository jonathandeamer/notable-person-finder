from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

StableKey = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{1,63}$")]
AttentionSignal = Literal[
    "significant_recognition",
    "enduring_contribution",
    "significant_work",
    "institutional_recognition",
    "sustained_field_attention",
    "major_achievement",
    "influential_role",
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def validate_public_http_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("must be an absolute HTTP or HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL must not contain embedded credentials")
    return value


class PathsConfig(StrictModel):
    root: Path | None = None


class SecretEnvConfig(StrictModel):
    openrouter_api_key: str = "OPENROUTER_API_KEY"
    brave_api_key: str = "BRAVE_API_KEY"


class MainConfig(StrictModel):
    schema_version: Literal[1]
    timezone: str
    feeds_file: Path
    domain_profile_file: Path
    paths: PathsConfig = PathsConfig()
    secrets: SecretEnvConfig = SecretEnvConfig()

    @field_validator("timezone")
    @classmethod
    def timezone_exists(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError("must be an installed IANA timezone") from error
        return value


class FeedConfig(StrictModel):
    key: StableKey
    label: str = Field(min_length=1, max_length=120)
    url: str
    enabled: bool = True

    @field_validator("url")
    @classmethod
    def public_url(cls, value: str) -> str:
        return validate_public_http_url(value)


class FeedsConfig(StrictModel):
    schema_version: Literal[1]
    feeds: tuple[FeedConfig, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_keys(self) -> FeedsConfig:
        keys = [feed.key for feed in self.feeds]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate feed key")
        return self


class DomainProfileConfig(StrictModel):
    schema_version: Literal[1]
    key: StableKey
    label: str = Field(min_length=1, max_length=120)
    language: Literal["en"]
    attention_examples: dict[AttentionSignal, tuple[str, ...]] = Field(
        default_factory=dict
    )
