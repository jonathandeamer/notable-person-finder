"""Replay of a recorded live run, plus the opt-in live smoke."""

import re
import shutil
import time
import tomllib
from pathlib import Path

import httpx
import pytest

from notable.cache import Cache
from notable.config import load_config
from notable.detect_contract import detection_schema
from notable.http import Transport
from notable.llm import LlmClient
from notable.pipeline import Providers, run
from notable.store import Store

FIXTURE = Path("tests/mvp/fixtures/phase1")
EXPECTED = Path("tests/mvp/fixtures/phase1_expected.toml")

# The fixture is a required acceptance artifact, not an optional test input.
# Once this file is committed, a missing directory must fail the suite loudly.


def _refuse(request):  # pragma: no cover - only fires on a cache miss
    raise AssertionError(f"unexpected network call to {request.url}")


def _replay(tmp_path, *, clock=time.time):
    cache_dir = tmp_path / "cache"
    shutil.copytree(FIXTURE, cache_dir)
    loaded = load_config(Path("config/notable.example.toml"))
    config = loaded.model_copy(
        update={
            "digest_dir": tmp_path / "digests",
            "data_dir": tmp_path / "data",
            # model_copy does not recurse: the nested cache config must be
            # replaced explicitly, or the test writes to the real cache dir.
            "cache": loaded.cache.model_copy(update={"dir": cache_dir}),
        }
    )
    store = Store(tmp_path / "db.sqlite")
    client = httpx.Client(transport=httpx.MockTransport(_refuse))
    # ignore_ttl: recorded responses stay valid fixtures indefinitely. Without
    # it the feed entries age past feed_ttl_seconds twelve hours after
    # recording, miss, and hit _refuse -- so this test would pass for half a
    # day and then fail permanently, on a change that had nothing to do with it.
    transport = Transport(
        config.transport,
        Cache(cache_dir, clock, ignore_ttl=True),
        client=client,
        sleep=lambda _s: None,
    )
    providers = Providers(
        transport=transport,
        llm=LlmClient(transport, config.openrouter, api_key="sk-test", budget_usd=None),
    )
    return config, store, providers


def test_recorded_run_replays_offline_with_no_network(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    config, store, providers = _replay(tmp_path)
    result = run(config, store, providers)
    assert result.digest_path.exists()
    assert store.connection.execute("SELECT COUNT(*) FROM item").fetchone()[0] > 0
    assert providers.llm.calls == 0, "a replay must make no provider call"
    assert providers.llm.spend() == 0
    store.close()


def _detected_names(digest_text: str) -> list[str]:
    """The people the digest actually lists, from its entry headings.

    Substring searches over the whole document do not work as a gate: a name
    that appears only inside another entry's rationale would satisfy
    `must_detect`, and an unexpected person passes unnoticed unless someone
    thought to name them in `must_not_detect`.
    """
    return re.findall(r"^### (.+)$", digest_text, re.MULTILINE)


def test_the_fixture_corpus_surfaces_exactly_the_people_it_should(
    tmp_path, monkeypatch
):
    """The pass/fail gate the spec's acceptance criteria name.

    Exact-set comparison, not containment: the gate has to fail on a person
    who appears as much as on one who disappears. Without that, Phase 2 and 3
    regressions show up only as a digest nobody is comparing.
    """
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    expected = tomllib.loads(EXPECTED.read_text("utf-8"))
    config, store, providers = _replay(tmp_path)
    result = run(config, store, providers)
    store.close()

    detected = _detected_names(result.digest_path.read_text("utf-8"))
    assert sorted(detected) == sorted(expected["must_detect"])
    # Redundant against the equality above, but it names the regression: a
    # failure here says *which* person came back.
    for name in expected["must_not_detect"]:
        assert name not in detected, f"{name} must not be surfaced by this corpus"


def test_the_fixture_replays_long_after_its_ttls_expire(tmp_path, monkeypatch):
    """The fixture must not rot.

    Its entries keep their original `stored_at` while the clock moves on, so
    without `ignore_ttl` the feed entries expire twelve hours after recording,
    miss, and hit `_refuse` -- the replay would pass for half a day and then
    fail permanently, on a change that had nothing to do with it.

    An injected clock rather than `faketime`: this must run on every machine
    and in CI, not only where an external tool happens to be installed.
    """
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    a_month_on = time.time() + 30 * 86400
    config, store, providers = _replay(tmp_path, clock=lambda: a_month_on)
    result = run(config, store, providers)
    store.close()
    assert result.digest_path.exists()
    assert providers.llm.calls == 0, "every call must still be served from the fixture"


def test_the_fixture_corpus_settles_the_items_it_should(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
    expected = tomllib.loads(EXPECTED.read_text("utf-8"))
    config, store, providers = _replay(tmp_path)
    result = run(config, store, providers)
    store.close()

    assert len(result.summary.settled) == expected["n_items_settled"]
    assert len(result.summary.incomplete) == expected["n_items_incomplete"]
    assert (
        len(_detected_names(result.digest_path.read_text("utf-8")))
        == (expected["n_detected"])
    )


@pytest.mark.live
def test_live_detection_smoke(tmp_path):
    """Opt-in: uv run pytest tests/mvp -m live -v

    This makes one real, cheap model call. A "live smoke" that only loads
    configuration proves nothing about the provider, and findings.md is
    explicit that a written live test is not evidence until it has run.
    """
    config = load_config(Path("config/mvp.local.toml"))
    assert config.budget_usd is not None, "never run live without a cap"

    client = httpx.Client()
    # A throwaway cache, not `config.cache.dir`. Against the production cache
    # the second invocation would be served from disk, making no call at all
    # -- so the assertions below would pass without touching the provider on
    # the first run and fail on `spend() > 0` on every run after it.
    transport = Transport(config.transport, Cache(tmp_path / "cache"), client=client)
    llm = LlmClient(
        transport,
        config.openrouter,
        api_key=config.openrouter_api_key,
        budget_usd=config.budget_usd,
    )
    result = llm.structured(
        task="detect_people",
        model=config.detect.model,
        system="Return a detection with no mentions.",
        user_payload={"passages": []},
        schema=detection_schema(max_people=3),
        max_completion_tokens=config.detect.max_completion_tokens,
        reasoning_effort=config.detect.reasoning_effort,
        timeout=config.transport.llm_read_timeout_seconds,
    )
    # The point is that the strict schema, the nested signal union, the wire
    # field names and usage.cost all survive contact with the real router.
    assert "item_outcome" in result
    assert llm.truncations == 0
    assert llm.spend() > 0, "usage.cost must be reported, or the cap is blind"
