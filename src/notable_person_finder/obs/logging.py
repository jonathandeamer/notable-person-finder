from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from notable_person_finder.config.models import LoggingConfig

EVENT_LOGGER_NAME = "notable"
REDACTION = "[redacted]"

# Field names that must never be logged even when a caller passes them.
_FORBIDDEN_FIELDS = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "cookie",
        "prompt",
        "response_body",
        "body",
        "query",
        "url",
    }
)

_ENVELOPE_FIELDS = frozenset({"event", "severity", "timestamp"})


def redact(text: str, secrets: Sequence[str | None]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTION)
    return text


class _RedactingJsonFormatter(logging.Formatter):
    def __init__(self, secrets: Sequence[str | None]) -> None:
        super().__init__()
        self._secrets = tuple(secret for secret in secrets if secret)

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "severity": record.levelname,
            "event": getattr(record, "event", record.getMessage()),
        }
        for name, value in getattr(record, "fields", {}).items():
            if name.lower() in _FORBIDDEN_FIELDS or name in _ENVELOPE_FIELDS:
                continue
            payload[name] = value

        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return redact(rendered, self._secrets)


def configure_logging(
    log_file: Path, config: LoggingConfig, *, secrets: Sequence[str | None]
) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(EVENT_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=config.max_bytes,
        backupCount=config.backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(_RedactingJsonFormatter(secrets))
    logger.addHandler(handler)
    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    severity: int = logging.INFO,
    **fields: Any,
) -> None:
    logger.log(severity, event, extra={"event": event, "fields": fields})
