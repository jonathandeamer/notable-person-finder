"""Strict, file-first configuration. Environment supplies secrets only."""

from __future__ import annotations

import os
import tomllib
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Feed(_Strict):
    key: str = Field(min_length=1, max_length=64)
    label: str = Field(min_length=1, max_length=120)
    url: str = Field(min_length=1)


class TransportConfig(_Strict):
    contact_url: str = Field(min_length=1)
    connect_timeout_seconds: float = 10.0
    read_timeout_seconds: float = 30.0
    llm_read_timeout_seconds: float = 300.0
    max_attempts: int = Field(default=3, ge=1, le=10)
    initial_backoff_seconds: float = 1.0
    per_host_min_interval_ms: int = Field(default=900, ge=0)


class CacheConfig(_Strict):
    dir: Path = Path("cache")
    feed_ttl_seconds: int = Field(default=43200, ge=1)
    discovery_ttl_seconds: int = Field(default=86400, ge=1)


class ModelTaskConfig(_Strict):
    """Shared fields for a config-driven model call: one task, one model."""

    model: str = Field(min_length=1)
    # detect_people: raise this with max_people, never independently. At
    # 1024 against max_people 8, responses were cut off mid-string and
    # rejected as malformed. See docs/findings.md.
    max_completion_tokens: int = Field(default=4096, ge=256)
    reasoning_effort: str | None = "low"


class DetectConfig(ModelTaskConfig):
    max_people: int = Field(default=8, ge=1, le=32)
    max_title_characters: int = Field(default=500, ge=1)
    max_summary_characters: int = Field(default=4000, ge=1)


class MediaWikiConfig(_Strict):
    endpoint: str = "https://en.wikipedia.org/w/api.php"
    # Up from the frozen `refactor/rearchitecture` system's 8: findings.md
    # records that value pushed 111 of 118 plans into forced-uncertain
    # truncation, and the fingerprint coupling that blocked raising it there
    # does not exist here.
    max_candidates: int = Field(default=15, ge=1, le=50)
    max_extract_characters: int = Field(default=1200, ge=1)
    max_categories_per_page: int = Field(default=20, ge=0)
    maxlag_seconds: int = Field(default=5, ge=1)


class BraveConfig(_Strict):
    endpoint: str = "https://api.search.brave.com/res/v1/web/search"
    max_search_results: int = Field(default=10, ge=1, le=20)
    max_articles_per_mention: int = Field(default=5, ge=1)


class CoverageConfig(_Strict):
    source_policy_path: Path
    max_article_characters: int = Field(default=6000, ge=1)
    max_article_bytes: int = Field(default=2_000_000, ge=1)


class _CoverageFile(_Strict):
    source_policy_file: str = "source_policies/visual_arts.toml"
    max_article_characters: int = 6000
    max_article_bytes: int = 2_000_000


class OpenRouterConfig(_Strict):
    endpoint: str = "https://openrouter.ai/api/v1"


class Config(_Strict):
    feeds: tuple[Feed, ...]
    data_dir: Path
    digest_dir: Path
    digest_size: int = Field(default=20, ge=1)
    resurface_after_days: int = Field(default=30, ge=1)
    promising_domain_threshold: int = Field(default=2, ge=1)
    max_item_attempts: int = Field(default=3, ge=1, le=10)
    transport: TransportConfig
    cache: CacheConfig
    detect: DetectConfig
    match: ModelTaskConfig
    assess: ModelTaskConfig
    mediawiki: MediaWikiConfig
    brave: BraveConfig
    coverage: CoverageConfig
    openrouter: OpenRouterConfig
    budget_usd: Decimal | None
    openrouter_api_key: str = Field(min_length=1, repr=False)
    brave_api_key: str = Field(min_length=1, repr=False)


class _Secrets(_Strict):
    openrouter_api_key: str = "OPENROUTER_API_KEY"
    brave_api_key: str = "BRAVE_API_KEY"


class _Budget(_Strict):
    openrouter_usd_per_run: str | None = None


class _File(_Strict):
    schema_version: int
    feeds_file: str
    data_dir: str = "data"
    digest_dir: str = "digests"
    digest_size: int = 20
    resurface_after_days: int = 30
    promising_domain_threshold: int = 2
    max_item_attempts: int = 3
    secrets: _Secrets = _Secrets()
    transport: TransportConfig
    cache: CacheConfig = CacheConfig()
    mediawiki: MediaWikiConfig = MediaWikiConfig()
    brave: BraveConfig = BraveConfig()
    coverage: _CoverageFile = _CoverageFile()
    openrouter: OpenRouterConfig = OpenRouterConfig()
    budget: _Budget = _Budget()
    tasks: dict[str, dict[str, Any]]


class _FeedsFile(_Strict):
    schema_version: int
    feeds: tuple[Feed, ...]


def _read_toml(path: Path) -> dict[str, object]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"configuration file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ValueError(f"{path} is not valid TOML: {error}") from error


def _task_config(parsed: _File, name: str, model_type: type[Any], path: Path) -> Any:
    raw = parsed.tasks.get(name)
    if raw is None:
        raise ValueError(f"{path} is missing [tasks.{name}]")
    try:
        return model_type.model_validate(raw)
    except ValidationError as error:
        raise ValueError(f"invalid [tasks.{name}] in {path}: {error}") from error


def load_config(path: Path) -> Config:
    """Load and validate configuration. Secrets come from the environment.

    An adjacent `.env` fills missing values, but the process environment wins.
    """
    path = path.resolve()
    root = path.parent
    load_dotenv(root / ".env", override=False)

    try:
        parsed = _File.model_validate(_read_toml(path))
    except Exception as error:  # pydantic ValidationError or our ValueError
        raise ValueError(f"invalid configuration in {path}: {error}") from error

    feeds_path = (root / parsed.feeds_file).resolve()
    try:
        feeds_file = _FeedsFile.model_validate(_read_toml(feeds_path))
    except Exception as error:
        raise ValueError(f"invalid feed list in {feeds_path}: {error}") from error

    seen: set[str] = set()
    for feed in feeds_file.feeds:
        if feed.key in seen:
            raise ValueError(f"duplicate feed key: {feed.key}")
        seen.add(feed.key)

    detect = _task_config(parsed, "detect_people", DetectConfig, path)
    match = _task_config(parsed, "match_wikipedia_identity", ModelTaskConfig, path)
    assess = _task_config(parsed, "assess_article", ModelTaskConfig, path)

    or_variable = parsed.secrets.openrouter_api_key
    or_key = os.environ.get(or_variable, "").strip()
    if not or_key:
        raise ValueError(
            f"missing OpenRouter API key: set the {or_variable} environment "
            f"variable, or add it to {root / '.env'}"
        )

    brave_variable = parsed.secrets.brave_api_key
    brave_key = os.environ.get(brave_variable, "").strip()
    if not brave_key:
        raise ValueError(
            f"missing Brave API key: set the {brave_variable} environment "
            f"variable, or add it to {root / '.env'}"
        )

    budget = parsed.budget.openrouter_usd_per_run
    return Config(
        feeds=feeds_file.feeds,
        data_dir=(root / parsed.data_dir).resolve(),
        digest_dir=(root / parsed.digest_dir).resolve(),
        digest_size=parsed.digest_size,
        resurface_after_days=parsed.resurface_after_days,
        promising_domain_threshold=parsed.promising_domain_threshold,
        max_item_attempts=parsed.max_item_attempts,
        transport=parsed.transport,
        cache=parsed.cache.model_copy(
            update={"dir": (root / parsed.cache.dir).resolve()}
        ),
        detect=detect,
        match=match,
        assess=assess,
        mediawiki=parsed.mediawiki,
        brave=parsed.brave,
        coverage=CoverageConfig(
            source_policy_path=(root / parsed.coverage.source_policy_file).resolve(),
            max_article_characters=parsed.coverage.max_article_characters,
            max_article_bytes=parsed.coverage.max_article_bytes,
        ),
        openrouter=parsed.openrouter,
        budget_usd=None if budget is None else Decimal(budget),
        openrouter_api_key=or_key,
        brave_api_key=brave_key,
    )
