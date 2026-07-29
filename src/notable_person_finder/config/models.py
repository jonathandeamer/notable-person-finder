from __future__ import annotations

import ipaddress
import re
from decimal import Decimal, InvalidOperation
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


_NON_PUBLIC_SUFFIXES = (".localhost", ".local", ".internal", ".home.arpa")


def _is_public_host(hostname: str) -> bool:
    host = hostname.rstrip(".").lower()
    if not host:
        return False
    if host == "localhost" or host.endswith(_NON_PUBLIC_SUFFIXES):
        return False
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        # A registered name; resolution is deliberately not attempted so that
        # validation stays offline and deterministic.
        return True
    return address.is_global


def validate_public_http_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("must be an absolute HTTP or HTTPS URL")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("URL must not contain embedded credentials")
    if not _is_public_host(parsed.hostname):
        raise ValueError("host must be publicly routable")
    return value


class PathsConfig(StrictModel):
    root: Path | None = None


class SecretEnvConfig(StrictModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    openrouter_api_key: str = "OPENROUTER_API_KEY"
    brave_api_key: str = "BRAVE_API_KEY"

    @field_validator("*")
    @classmethod
    def environment_variable_identifier(cls, value: str) -> str:
        if re.fullmatch(r"[A-Z_][A-Z0-9_]*", value) is None:
            raise ValueError("must be an uppercase environment-variable identifier")
        return value


NANO_USD = 1_000_000_000
_USD_PATTERN = re.compile(r"^\d{1,9}(\.\d{1,9})?$")

DEFAULT_USER_AGENT_URL = "https://github.com/jonathandeamer/notable-person-finder"


def usd_to_nano_usd(value: str) -> int:
    """Convert a decimal USD string to exact integer nano-USD."""
    if _USD_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "must be a non-negative decimal USD amount with at most 9 decimal places"
        )
    try:
        amount = Decimal(value)
    except InvalidOperation as error:  # pragma: no cover - guarded by the pattern
        raise ValueError("must be a decimal USD amount") from error
    return int(amount * NANO_USD)


class TransportConfig(StrictModel):
    contact_url: str | None = None
    user_agent_override: str | None = Field(default=None, min_length=1, max_length=200)
    connect_timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    read_timeout_seconds: float = Field(default=30.0, gt=0, le=600)
    llm_read_timeout_seconds: float = Field(default=300.0, gt=0, le=1800)
    max_redirects: int = Field(default=5, ge=1, le=10)
    max_api_response_bytes: int = Field(default=5 * 1024 * 1024, gt=0)
    max_article_response_bytes: int = Field(default=10 * 1024 * 1024, gt=0)

    @field_validator("contact_url")
    @classmethod
    def public_contact_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_public_http_url(value)

    def resolved_user_agent(self, version: str) -> str:
        if self.user_agent_override is not None:
            return self.user_agent_override
        contact = f"; {self.contact_url}" if self.contact_url else ""
        return f"notable-person-finder/{version} (+{DEFAULT_USER_AGENT_URL}{contact})"


class RetryConfig(StrictModel):
    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_backoff_seconds: float = Field(default=1.0, gt=0, le=60)
    max_backoff_seconds: float = Field(default=30.0, gt=0, le=300)
    backoff_multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    jitter_ratio: float = Field(default=0.25, ge=0.0, le=1.0)
    provider_pause_after_consecutive_exhaustions: int = Field(default=3, ge=1, le=50)

    @model_validator(mode="after")
    def backoff_is_ordered(self) -> RetryConfig:
        if self.max_backoff_seconds < self.initial_backoff_seconds:
            raise ValueError(
                "max_backoff_seconds must be at least initial_backoff_seconds"
            )
        return self


class ConcurrencyConfig(StrictModel):
    http_workers: int = Field(default=4, ge=1, le=32)
    llm_workers: int = Field(default=2, ge=1, le=16)
    per_origin: int = Field(default=2, ge=1, le=16)


class PacingConfig(StrictModel):
    mediawiki_min_interval_ms: int = Field(default=900, ge=0, le=60_000)
    brave_min_interval_ms: int = Field(default=1100, ge=0, le=60_000)


class BudgetConfig(StrictModel):
    openrouter_usd_per_run: str | None = None

    @field_validator("openrouter_usd_per_run")
    @classmethod
    def decimal_usd(cls, value: str | None) -> str | None:
        if value is None:
            return None
        usd_to_nano_usd(value)
        return value

    def openrouter_nano_usd_per_run(self) -> int | None:
        if self.openrouter_usd_per_run is None:
            return None
        return usd_to_nano_usd(self.openrouter_usd_per_run)


class ProviderRoutingConfig(StrictModel):
    allow_fallbacks: bool = True
    data_collection: Literal["allow", "deny"] = "deny"
    zdr: bool = True


class OpenRouterConfig(StrictModel):
    endpoint: str = "https://openrouter.ai/api/v1"
    routing: ProviderRoutingConfig = ProviderRoutingConfig()

    @field_validator("endpoint")
    @classmethod
    def public_https_endpoint(cls, value: str) -> str:
        if any(character.isspace() for character in value):
            raise ValueError("endpoint must not contain whitespace")
        validate_public_http_url(value)
        parsed = urlsplit(value)
        if parsed.scheme != "https":
            raise ValueError("must use HTTPS")
        if parsed.query or parsed.fragment:
            raise ValueError("endpoint must not contain a query or fragment")
        return value


ReasoningEffort = Literal["none", "minimal", "low", "medium", "high", "xhigh"]


class GenerationParameters(StrictModel):
    temperature: float = Field(default=0.0, ge=0.0, le=2.0, allow_inf_nan=False)
    top_p: float = Field(default=1.0, ge=0.0, le=1.0, allow_inf_nan=False)
    reasoning_effort: ReasoningEffort | None = None


_MODEL_SLUG_PATTERN = re.compile(
    r"^[a-z0-9][a-z0-9._-]{0,63}/[a-z0-9][a-z0-9._-]{0,127}$"
)


class DetectPeopleConfig(StrictModel):
    model: str = "openai/gpt-5.4-mini"
    max_input_tokens: int = Field(default=4096, strict=True, ge=1, le=1_000_000)
    max_completion_tokens: int = Field(default=1024, strict=True, ge=1, le=100_000)
    parameters: GenerationParameters = GenerationParameters()
    max_people: int = Field(default=8, strict=True, ge=1, le=32)
    max_title_characters: int = Field(default=500, strict=True, ge=1, le=2000)
    max_summary_characters: int = Field(default=4000, strict=True, ge=1, le=20_000)

    @field_validator("model")
    @classmethod
    def exact_model_slug(cls, value: str) -> str:
        if _MODEL_SLUG_PATTERN.fullmatch(value) is None:
            raise ValueError("model must be one exact lowercase author/slug identifier")
        if value.startswith("openrouter/"):
            raise ValueError("model must not use an OpenRouter routing alias")
        return value

    @model_validator(mode="after")
    def bounds_are_compatible(self) -> DetectPeopleConfig:
        if self.max_completion_tokens >= self.max_input_tokens:
            raise ValueError("max_completion_tokens must be less than max_input_tokens")
        if self.max_summary_characters < self.max_title_characters:
            raise ValueError(
                "max_summary_characters must be at least max_title_characters"
            )
        return self


class TasksConfig(StrictModel):
    detect_people: DetectPeopleConfig = DetectPeopleConfig()


class DigestConfig(StrictModel):
    write_latest_copy: bool = True


class LoggingConfig(StrictModel):
    max_bytes: int = Field(default=5 * 1024 * 1024, gt=0)
    backup_count: int = Field(default=5, ge=0, le=100)


class MainConfig(StrictModel):
    schema_version: Literal[1]
    timezone: str
    feeds_file: Path
    domain_profile_file: Path
    paths: PathsConfig = PathsConfig()
    secrets: SecretEnvConfig = SecretEnvConfig()
    transport: TransportConfig = TransportConfig()
    retry: RetryConfig = RetryConfig()
    concurrency: ConcurrencyConfig = ConcurrencyConfig()
    pacing: PacingConfig = PacingConfig()
    budget: BudgetConfig = BudgetConfig()
    openrouter: OpenRouterConfig = OpenRouterConfig()
    tasks: TasksConfig = TasksConfig()
    digest: DigestConfig = DigestConfig()
    logging: LoggingConfig = LoggingConfig()

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
