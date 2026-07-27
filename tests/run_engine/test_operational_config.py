from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from notable_person_finder.config.loader import ConfigLoadError, load_config
from notable_person_finder.config.models import (
    BudgetConfig,
    ConcurrencyConfig,
    MainConfig,
    RetryConfig,
    TransportConfig,
    usd_to_nano_usd,
)
from tests.run_engine.helpers import ENVIRONMENT, write_graph


def test_operational_sections_have_conservative_defaults() -> None:
    transport = TransportConfig()
    assert transport.connect_timeout_seconds == 10.0
    assert transport.read_timeout_seconds == 30.0
    assert transport.llm_read_timeout_seconds == 300.0
    assert transport.max_redirects == 5
    assert transport.max_api_response_bytes == 5 * 1024 * 1024
    assert transport.max_article_response_bytes == 10 * 1024 * 1024

    retry = RetryConfig()
    assert retry.max_attempts == 3
    assert retry.provider_pause_after_consecutive_exhaustions == 3

    concurrency = ConcurrencyConfig()
    assert (
        concurrency.http_workers,
        concurrency.llm_workers,
        concurrency.per_origin,
    ) == (4, 2, 2)


def test_default_user_agent_identifies_the_application() -> None:
    agent = TransportConfig().resolved_user_agent("0.1.0")
    assert agent.startswith("notable-person-finder/0.1.0 (+https://github.com/")
    assert "bot" not in agent.lower()


def test_contact_url_is_appended_and_override_replaces_completely() -> None:
    appended = TransportConfig(
        contact_url="https://example.com/contact"
    ).resolved_user_agent("0.1.0")
    assert appended.endswith("; https://example.com/contact)")

    replaced = TransportConfig(
        user_agent_override="custom-agent/9"
    ).resolved_user_agent("0.1.0")
    assert replaced == "custom-agent/9"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("connect_timeout_seconds", 0.0),
        ("read_timeout_seconds", -1.0),
        ("max_redirects", 0),
        ("max_api_response_bytes", 0),
    ],
)
def test_transport_bounds_must_be_positive(field: str, value: float) -> None:
    # The field under test varies per parametrize case and its value never
    # matches the corresponding attribute's declared type (e.g. a float
    # standing in for `max_redirects: int`), which is the point: pydantic,
    # not the constructor's static signature, is what must reject it.
    # `model_validate` is pydantic's own entry point for validating a
    # dynamically-shaped mapping, so it is used here instead of splatting
    # into the strongly-typed `__init__`.
    with pytest.raises(ValidationError):
        TransportConfig.model_validate({field: value})


def test_unknown_operational_field_is_rejected() -> None:
    with pytest.raises(ValidationError):
        TransportConfig.model_validate({"max_redirect": 5})


def test_budget_is_optional_and_parsed_as_exact_nano_usd() -> None:
    assert BudgetConfig().openrouter_nano_usd_per_run() is None
    assert (
        BudgetConfig(openrouter_usd_per_run="2.50").openrouter_nano_usd_per_run()
        == 2_500_000_000
    )


@pytest.mark.parametrize("value", ["-1.00", "1.0000000001", "abc", "1e3", "NaN", ""])
def test_invalid_budget_syntax_is_rejected(value: str) -> None:
    with pytest.raises(ValidationError):
        BudgetConfig(openrouter_usd_per_run=value)


def test_usd_to_nano_usd_is_exact() -> None:
    assert usd_to_nano_usd("0.000000001") == 1
    assert usd_to_nano_usd("10") == 10_000_000_000


def test_loaded_graph_exposes_operational_settings(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path)
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert loaded.main.transport.contact_url == "https://example.com/contact"
    assert loaded.main.budget.openrouter_nano_usd_per_run() == 2_500_000_000
    assert loaded.main.pacing.mediawiki_min_interval_ms == 900


def test_snapshot_records_operational_bounds_but_no_secret_value(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path)
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert '"max_redirects":5' in loaded.snapshot_json
    assert "or-secret-value" not in loaded.snapshot_json
    assert "brave-secret-value" not in loaded.snapshot_json


def test_invalid_operational_bound_fails_before_any_run(tmp_path: Path) -> None:
    config_file = write_graph(tmp_path, operational="[concurrency]\nhttp_workers = 0\n")
    with pytest.raises(ConfigLoadError) as raised:
        load_config(config_file, environ=ENVIRONMENT)
    assert any("concurrency.http_workers" in error for error in raised.value.errors)


def test_main_config_still_validates_without_operational_sections(
    tmp_path: Path,
) -> None:
    config_file = write_graph(tmp_path, operational="")
    loaded = load_config(config_file, environ=ENVIRONMENT)
    assert isinstance(loaded.main, MainConfig)
    assert loaded.main.budget.openrouter_nano_usd_per_run() is None
