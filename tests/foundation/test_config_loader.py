from __future__ import annotations

import json
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


def test_missing_files_and_secrets_are_actionable(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / "feeds.toml").unlink()

    with pytest.raises(ConfigLoadError) as captured:
        load_config(config_file, environ={})

    assert "feeds.toml" in "\n".join(captured.value.errors)
    assert "TEST_OPENROUTER" in "\n".join(captured.value.errors)
    assert "TEST_BRAVE" in "\n".join(captured.value.errors)
