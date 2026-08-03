import json
from typing import Any

import pytest

from notable.cache import Cache, cache_key

KEY_ARGS: dict[str, Any] = {
    "provider": "openrouter",
    "method": "POST",
    "url": "https://openrouter.ai/api/v1/chat/completions",
    "body": {"model": "m", "messages": []},
    "transport_profile": "v1",
}


def test_key_is_stable_across_dict_ordering():
    a_args: dict[str, Any] = KEY_ARGS | {"body": {"model": "m", "messages": []}}
    b_args: dict[str, Any] = KEY_ARGS | {"body": {"messages": [], "model": "m"}}
    assert cache_key(**a_args) == cache_key(**b_args)


@pytest.mark.parametrize(
    "override",
    [
        {"provider": "brave"},
        {"method": "GET"},
        {"url": "https://openrouter.ai/api/v1/other"},
        {"body": {"model": "n", "messages": []}},
        {"transport_profile": "v2"},
        {"extra": {"schema": {"type": "object"}}},
    ],
)
def test_every_key_component_changes_the_key(override):
    overridden: dict[str, Any] = KEY_ARGS | override
    assert cache_key(**KEY_ARGS) != cache_key(**overridden)


def test_key_never_contains_a_secret():
    args: dict[str, Any] = KEY_ARGS | {"extra": {"model": "openai/gpt-5.4-mini"}}
    key = cache_key(**args)
    assert "sk-" not in key
    assert len(key) == 64 and int(key, 16) >= 0  # plain hex digest


def test_roundtrip(tmp_path):
    cache = Cache(tmp_path)
    cache.put("abc123", {"status": 200, "text": "hi"})
    assert cache.get("abc123", ttl_seconds=None) == {"status": 200, "text": "hi"}


def test_missing_key_is_none(tmp_path):
    assert Cache(tmp_path).get("nope", ttl_seconds=None) is None


def test_entry_past_its_ttl_is_a_miss(tmp_path):
    now = [1000.0]
    cache = Cache(tmp_path, clock=lambda: now[0])
    cache.put("k", {"v": 1})
    now[0] = 1000.0 + 59
    assert cache.get("k", ttl_seconds=60) == {"v": 1}
    now[0] = 1000.0 + 61
    assert cache.get("k", ttl_seconds=60) is None


def test_ttl_none_never_expires(tmp_path):
    now = [0.0]
    cache = Cache(tmp_path, clock=lambda: now[0])
    cache.put("k", {"v": 1})
    now[0] = 10_000_000.0
    assert cache.get("k", ttl_seconds=None) == {"v": 1}


def test_replay_mode_ignores_a_ttl_that_has_passed(tmp_path):
    # A committed fixture must not rot. Its entries keep their original
    # stored_at while the clock moves on; without this the phase 1 replay test
    # passes for twelve hours and then fails forever.
    now = [1000.0]
    Cache(tmp_path, clock=lambda: now[0]).put("k", {"v": 1})
    now[0] = 1000.0 + 999_999
    assert Cache(tmp_path, clock=lambda: now[0]).get("k", ttl_seconds=60) is None
    replay = Cache(tmp_path, clock=lambda: now[0], ignore_ttl=True)
    assert replay.get("k", ttl_seconds=60) == {"v": 1}


def test_replay_mode_still_rejects_a_corrupt_entry(tmp_path):
    # Ignoring the TTL must not weaken anything else: a fixture entry that
    # fails to parse is still a miss, which the replay transport turns into a
    # loud "unexpected network call" rather than silent bad data.
    cache = Cache(tmp_path, ignore_ttl=True)
    cache.put("k", {"v": 1})
    cache.path_for("k").write_text("{truncated", "utf-8")
    assert cache.get("k", ttl_seconds=60) is None


def test_truncated_entry_is_a_miss_and_is_removed(tmp_path):
    cache = Cache(tmp_path)
    cache.put("k", {"v": 1})
    path = cache.path_for("k")
    path.write_text(
        path.read_text("utf-8")[: len(path.read_text("utf-8")) // 2], "utf-8"
    )
    assert cache.get("k", ttl_seconds=None) is None
    assert not path.exists(), "a corrupt entry must be deleted, not left to rot"


def test_entry_missing_required_envelope_fields_is_a_miss(tmp_path):
    cache = Cache(tmp_path)
    cache.put("k", {"v": 1})
    cache.path_for("k").write_text(json.dumps({"garbage": True}), "utf-8")
    assert cache.get("k", ttl_seconds=None) is None


def test_corrupt_entry_is_a_miss_even_if_cleanup_fails(tmp_path, monkeypatch):
    cache = Cache(tmp_path)
    cache.put("k", {"v": 1})
    cache.path_for("k").write_text("{truncated", "utf-8")
    monkeypatch.setattr(type(cache.path_for("k")), "unlink", _unlink_boom)
    assert cache.get("k", ttl_seconds=None) is None


def test_no_partial_file_is_left_when_writing_fails(tmp_path, monkeypatch):
    cache = Cache(tmp_path)
    monkeypatch.setattr("notable.cache.os.replace", _boom)
    with pytest.raises(OSError):
        cache.put("k", {"v": 1})
    assert list(tmp_path.rglob("*.tmp*")) == []


def _boom(*_args, **_kwargs):
    raise OSError("replace failed")


def _unlink_boom(*_args, **_kwargs):
    raise OSError("cleanup failed")
