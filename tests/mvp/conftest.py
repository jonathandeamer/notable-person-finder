"""Shared fixtures. Tasks 6, 8, and 10 all need a Config and a Transport."""

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from notable.cache import Cache
from notable.config import (
    CacheConfig,
    Config,
    DetectConfig,
    Feed,
    OpenRouterConfig,
    TransportConfig,
)
from notable.http import Transport
from notable.store import Store

TRANSPORT = TransportConfig(
    contact_url="https://e.test/c",
    per_host_min_interval_ms=0,
    initial_backoff_seconds=0,
)


@pytest.fixture
def make_config(tmp_path) -> Callable[..., Config]:
    def build(**overrides) -> Config:
        base = Config(
            feeds=(Feed(key="a", label="Feed A", url="https://a.test/rss"),),
            data_dir=tmp_path / "data",
            digest_dir=tmp_path / "digests",
            transport=TRANSPORT,
            cache=CacheConfig(dir=tmp_path / "cache"),
            detect=DetectConfig(model="m"),
            openrouter=OpenRouterConfig(),
            budget_usd=None,
            openrouter_api_key="sk-test",
        )
        return base.model_copy(update=overrides) if overrides else base

    return build


@pytest.fixture
def make_transport(tmp_path) -> Callable[[Callable], Transport]:
    def build(handler: Callable) -> Transport:
        client = httpx.Client(transport=httpx.MockTransport(handler))
        return Transport(
            TRANSPORT, Cache(tmp_path / "cache"), client=client, sleep=lambda _s: None
        )

    return build


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path / "db.sqlite")
    yield instance
    instance.close()
