from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from notable_person_finder.config.loader import ConfigLoadError, load_config
from tests.foundation.helpers import write_graph


def test_process_environment_overrides_adjacent_dotenv(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / ".env").write_text(
        "TEST_OPENROUTER=dotenv-openrouter\nTEST_BRAVE=dotenv-brave\n",
        encoding="utf-8",
    )

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "process-openrouter"},
    )

    assert loaded.credentials.openrouter_api_key == "process-openrouter"
    assert loaded.credentials.brave_api_key == "dotenv-brave"
    assert "process-openrouter" not in loaded.snapshot_json
    assert "dotenv-brave" not in loaded.snapshot_json
    snapshot = json.loads(loaded.snapshot_json)
    assert snapshot["secret_availability"] == {
        "TEST_BRAVE": True,
        "TEST_OPENROUTER": True,
    }
    assert len(loaded.fingerprint) == 64
    assert loaded.paths.data_root == (tmp_path / "portable" / "data").resolve()


def test_model_settings_are_complete_in_the_redacted_snapshot(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "snapshot-secret", "TEST_BRAVE": "brave"},
    )

    snapshot = json.loads(loaded.snapshot_json)
    assert snapshot["main"]["openrouter"] == {
        "endpoint": "https://openrouter.ai/api/v1",
        "routing": {
            "allow_fallbacks": True,
            "data_collection": "deny",
            "zdr": True,
        },
    }
    assert snapshot["main"]["tasks"]["detect_people"] == {
        "model": "openai/gpt-5.4-mini",
        "max_input_tokens": 4096,
        "max_completion_tokens": 1024,
        "parameters": {
            "temperature": 0.0,
            "top_p": 1.0,
            "reasoning_effort": "low",
        },
        "max_people": 8,
        "max_title_characters": 500,
        "max_summary_characters": 4000,
    }
    assert snapshot["secret_availability"]["TEST_OPENROUTER"] is True
    assert "snapshot-secret" not in loaded.snapshot_json


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            'endpoint = "https://openrouter.ai/api/v1"',
            'endpoint = "https://router.example/v1"',
        ),
        ("allow_fallbacks = true", "allow_fallbacks = false"),
        ('data_collection = "deny"', 'data_collection = "allow"'),
        ("zdr = true", "zdr = false"),
        ('model = "openai/gpt-5.4-mini"', 'model = "qwen/qwen3.7-plus"'),
        ("max_input_tokens = 4096", "max_input_tokens = 8192"),
        ("max_completion_tokens = 1024", "max_completion_tokens = 2048"),
        ("temperature = 0.0", "temperature = 0.2"),
        ("top_p = 1.0", "top_p = 0.9"),
        ('reasoning_effort = "low"', 'reasoning_effort = "medium"'),
        ("max_people = 8", "max_people = 12"),
        ("max_title_characters = 500", "max_title_characters = 600"),
        ("max_summary_characters = 4000", "max_summary_characters = 5000"),
    ],
)
def test_each_model_setting_changes_the_configuration_fingerprint(
    tmp_path: Path, old: str, new: str
) -> None:
    first_file = write_graph(tmp_path)
    environment = {"TEST_OPENROUTER": "first-key", "TEST_BRAVE": "brave-key"}
    first = load_config(first_file, environ=environment)
    first_file.write_text(
        first_file.read_text(encoding="utf-8").replace(old, new),
        encoding="utf-8",
    )

    second = load_config(first_file, environ=environment)

    assert first.fingerprint != second.fingerprint


def test_secret_values_do_not_change_the_configuration_fingerprint(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)

    first = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "first-key", "TEST_BRAVE": "first-brave"},
    )
    second = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "second-key", "TEST_BRAVE": "second-brave"},
    )

    assert first.snapshot_json == second.snapshot_json
    assert first.fingerprint == second.fingerprint


def test_configuration_validation_never_resolves_or_contacts_the_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config_file = write_graph(tmp_path)

    def unexpected_network(*args: object, **kwargs: object) -> object:
        raise AssertionError("configuration validation attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", unexpected_network)

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "offline-key", "TEST_BRAVE": "brave"},
    )

    assert loaded.main.openrouter.endpoint == "https://openrouter.ai/api/v1"


@pytest.mark.parametrize("port", ["abc", "70000"])
def test_malformed_openrouter_port_is_rejected_without_network_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, port: str
) -> None:
    config_file = write_graph(tmp_path)
    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace(
            "https://openrouter.ai/api/v1",
            f"https://openrouter.ai:{port}/api/v1",
        ),
        encoding="utf-8",
    )

    def unexpected_network(*args: object, **kwargs: object) -> object:
        raise AssertionError("configuration validation attempted network access")

    monkeypatch.setattr(socket, "getaddrinfo", unexpected_network)

    with pytest.raises(ConfigLoadError) as captured:
        load_config(
            config_file,
            environ={"TEST_OPENROUTER": "offline-key", "TEST_BRAVE": "brave"},
        )

    diagnostic = str(captured.value)
    assert "openrouter.endpoint" in diagnostic
    assert "valid port" in diagnostic
    assert "network access" not in diagnostic


def test_missing_files_and_secrets_are_actionable(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / "feeds.toml").unlink()

    with pytest.raises(ConfigLoadError) as captured:
        load_config(config_file, environ={})

    assert "feeds.toml" in "\n".join(captured.value.errors)
    assert "TEST_OPENROUTER" in "\n".join(captured.value.errors)
    assert "TEST_BRAVE" in "\n".join(captured.value.errors)


def test_literal_secret_selector_is_rejected_without_disclosure(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    literal_secret = "sk-or-v1.pasted-secret-value"
    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace(
            'openrouter_api_key = "TEST_OPENROUTER"',
            f'openrouter_api_key = "{literal_secret}"',
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigLoadError) as captured:
        load_config(config_file, environ={}, require_secrets=False)

    diagnostic = str(captured.value)
    assert "secrets.openrouter_api_key" in diagnostic
    assert "environment-variable identifier" in diagnostic
    assert literal_secret not in diagnostic
