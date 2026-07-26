from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from notable_person_finder.config.models import LoggingConfig
from notable_person_finder.obs.logging import configure_logging, log_event, redact


@pytest.fixture
def logger(tmp_path: Path) -> logging.Logger:
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=2048, backup_count=2),
        secrets=("or-secret-value", "brave-secret-value"),
    )
    yield created
    for handler in list(created.handlers):
        handler.close()
        created.removeHandler(handler)


def read_events(tmp_path: Path) -> list[dict[str, object]]:
    text = (tmp_path / "logs" / "notable.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line]


def test_events_are_json_lines_with_the_required_envelope(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_attempt_finished", run_id=1, attempt_id=9, outcome="succeeded")
    event = read_events(tmp_path)[0]
    assert event["event"] == "provider_attempt_finished"
    assert event["severity"] == "INFO"
    assert event["timestamp"].endswith("Z")
    assert event["run_id"] == 1
    assert event["outcome"] == "succeeded"


def test_log_directory_is_created(tmp_path: Path, logger: logging.Logger) -> None:
    log_event(logger, "run_started", run_id=1)
    assert (tmp_path / "logs" / "notable.jsonl").exists()


def test_provider_fields_are_recorded(tmp_path: Path, logger: logging.Logger) -> None:
    log_event(
        logger,
        "provider_attempt_finished",
        run_id=1,
        work_item_id=4,
        attempt_id=9,
        provider="brave",
        operation="search_web",
        destination_host="api.search.brave.com",
        duration_ms=310,
        retry_ordinal=2,
        response_bytes=2048,
        outcome="failed",
        failure_category="rate_limit",
    )
    event = read_events(tmp_path)[0]
    assert event["provider"] == "brave"
    assert event["destination_host"] == "api.search.brave.com"
    assert event["retry_ordinal"] == 2
    assert event["failure_category"] == "rate_limit"


def test_a_secret_value_is_redacted_from_any_field(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_attempt_failed", detail="key=or-secret-value rejected")
    event = read_events(tmp_path)[0]
    assert "or-secret-value" not in json.dumps(event)
    assert "[redacted]" in event["detail"]


def test_a_secret_value_is_redacted_from_the_message_of_an_exception(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "internal_error", detail="Bearer brave-secret-value")
    assert "brave-secret-value" not in (tmp_path / "logs" / "notable.jsonl").read_text()


def test_authorization_like_fields_are_dropped_entirely(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_request", authorization="Bearer abc", api_key="xyz")
    event = read_events(tmp_path)[0]
    assert "authorization" not in event
    assert "api_key" not in event


def test_rotation_keeps_the_configured_number_of_files(
    tmp_path: Path, logger: logging.Logger
) -> None:
    for index in range(400):
        log_event(logger, "run_progress", run_id=1, index=index, note="x" * 50)
    files = sorted(p.name for p in (tmp_path / "logs").iterdir())
    assert "notable.jsonl" in files
    assert len(files) <= 3  # the live file plus backup_count=2


def test_redact_replaces_every_occurrence() -> None:
    assert redact("a secret and secret again", ("secret",)) == "a [redacted] and [redacted] again"


def test_redact_ignores_empty_secrets() -> None:
    assert redact("unchanged", ("", None)) == "unchanged"
