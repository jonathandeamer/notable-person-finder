"""Regression tests for the foundation code-review findings."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.config.loader import ConfigLoadError, load_config
from notable_person_finder.config.models import validate_public_http_url
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import Migration, MigrationError, apply_migrations
from tests.foundation.helpers import write_graph


def _checksum(sql: str) -> str:
    import hashlib

    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


def test_blank_process_variable_does_not_shadow_dotenv_secret(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / ".env").write_text(
        "TEST_OPENROUTER=dotenv-openrouter\nTEST_BRAVE=dotenv-brave\n",
        encoding="utf-8",
    )

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "", "TEST_BRAVE": "   "},
        require_secrets=True,
    )

    assert loaded.credentials.openrouter_api_key == "dotenv-openrouter"
    assert loaded.credentials.brave_api_key == "dotenv-brave"


def test_process_variable_still_overrides_dotenv(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / ".env").write_text(
        "TEST_OPENROUTER=dotenv-openrouter\nTEST_BRAVE=dotenv-brave\n",
        encoding="utf-8",
    )

    loaded = load_config(
        config_file,
        environ={
            "TEST_OPENROUTER": "process-openrouter",
            "TEST_BRAVE": "process-brave",
        },
        require_secrets=True,
    )

    assert loaded.credentials.openrouter_api_key == "process-openrouter"
    assert loaded.credentials.brave_api_key == "process-brave"


def test_secret_values_are_stripped(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "  padded-key\n", "TEST_BRAVE": "\tbrave-key  "},
        require_secrets=True,
    )

    assert loaded.credentials.openrouter_api_key == "padded-key"
    assert loaded.credentials.brave_api_key == "brave-key"


def test_openrouter_secret_redaction_has_a_non_secret_snapshot_control(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)
    configured_secret = "openrouter-sensitive-value"

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": configured_secret, "TEST_BRAVE": "brave-key"},
        require_secrets=True,
    )

    assert "openai/gpt-5.4-mini" in loaded.snapshot_json
    assert '"TEST_OPENROUTER":true' in loaded.snapshot_json
    assert configured_secret not in loaded.snapshot_json


def test_non_utf8_configuration_reports_actionable_error(tmp_path: Path) -> None:
    config_file = tmp_path / "notable.toml"
    config_file.write_bytes(b'schema_version = 1\ntimezone = "Europe/Paris\xe9"\n')

    with pytest.raises(ConfigLoadError) as caught:
        load_config(config_file, environ={}, require_secrets=False)

    assert any("not valid UTF-8" in error for error in caught.value.errors)


def test_source_policy_file_is_resolved_absolute_on_the_main_config(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)

    loaded = load_config(
        config_file,
        environ={"TEST_OPENROUTER": "openrouter-key", "TEST_BRAVE": "brave-key"},
        require_secrets=True,
    )

    expected = (tmp_path / "source_policies" / "visual_arts.toml").resolve()
    assert loaded.main.source_policy_file == expected
    assert loaded.main.source_policy_file.is_absolute()


def test_configuration_path_through_a_file_reports_actionable_error(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)
    (tmp_path / "notes.txt").write_text("not a directory", encoding="utf-8")
    config_file.write_text(
        config_file.read_text(encoding="utf-8").replace(
            'feeds_file = "feeds.toml"', 'feeds_file = "notes.txt/feeds.toml"'
        ),
        encoding="utf-8",
    )

    with pytest.raises(ConfigLoadError) as caught:
        load_config(config_file, environ={}, require_secrets=False)

    assert any("not a directory" in error for error in caught.value.errors)


@pytest.mark.parametrize("directory", ["we#ird", "quer?y", "per%cent"])
def test_readonly_connection_honours_uri_special_characters(
    tmp_path: Path, directory: str
) -> None:
    database = tmp_path / directory / "notable.sqlite3"
    writable = connect_database(database)
    writable.execute("CREATE TABLE canary (id INTEGER PRIMARY KEY)")
    writable.commit()
    writable.close()

    readonly = connect_database(database, readonly=True)
    try:
        names = {
            row[0]
            for row in readonly.execute("SELECT name FROM sqlite_master").fetchall()
        }
        assert "canary" in names
        with pytest.raises(sqlite3.OperationalError):
            readonly.execute("CREATE TABLE blocked (id INTEGER PRIMARY KEY)")
    finally:
        readonly.close()

    assert not (tmp_path / directory.split("#")[0].split("?")[0]).is_file()


def test_failed_migration_reports_versions_already_applied(tmp_path: Path) -> None:
    database = tmp_path / "notable.sqlite3"
    connection = connect_database(database)
    good = "CREATE TABLE first (id INTEGER PRIMARY KEY);"
    second = "CREATE TABLE second (id INTEGER PRIMARY KEY);"
    broken = "CREATE TABLE first (id INTEGER PRIMARY KEY);"
    migrations = (
        Migration(version=1, name="first", sql=good, checksum=_checksum(good)),
        Migration(version=2, name="second", sql=second, checksum=_checksum(second)),
        Migration(version=3, name="broken", sql=broken, checksum=_checksum(broken)),
    )

    try:
        with pytest.raises(MigrationError) as caught:
            apply_migrations(connection, database, tmp_path / "backups", migrations)
    finally:
        connection.close()

    message = str(caught.value)
    assert "migration 3 failed" in message
    assert "already applied: 1, 2" in message
    assert caught.value.applied_versions == (1, 2)


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost/feed",
        "http://LOCALHOST/feed",
        "http://feeds.localhost/feed",
        "http://127.0.0.1/feed",
        "http://[::1]/feed",
        "http://192.168.1.5/rss",
        "http://10.0.0.1/rss",
        "http://172.16.4.2/rss",
        "http://169.254.169.254/latest/meta-data/",
        "http://0.0.0.0/feed",
        "http://printer.local/feed",
    ],
)
def test_public_url_validator_rejects_non_public_hosts(url: str) -> None:
    with pytest.raises(ValueError, match="publicly routable"):
        validate_public_http_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com/feed.xml",
        "https://www.theartnewspaper.com/rss.xml",
        "http://93.184.216.34/feed",
        "https://sub.domain.example.co.uk/rss?x=1",
    ],
)
def test_public_url_validator_accepts_public_hosts(url: str) -> None:
    assert validate_public_http_url(url) == url
