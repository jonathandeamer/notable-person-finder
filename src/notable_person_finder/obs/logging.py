from __future__ import annotations

import json
import logging
import traceback
from collections.abc import Sequence
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from notable_person_finder.config.models import LoggingConfig

EVENT_LOGGER_NAME = "notable"
REDACTION = "[redacted]"

# Substrings of field names that must never be logged even when a caller
# passes them. Matched case-insensitively against the whole field name, so
# `headers`, `article_url`, `search_query`, and `request_body` are all
# caught alongside the exact literal names.
_FORBIDDEN_FIELD_MARKERS = frozenset(
    {
        "authorization",
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "cookie",
        "prompt",
        "body",
        "query",
        "url",
        "header",
    }
)

_ENVELOPE_FIELDS = frozenset({"event", "severity", "timestamp"})


def redact(text: str, secrets: Sequence[str | None]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTION)
    return text


def _is_forbidden_field(name: str) -> bool:
    # Top-level field names only: a nested value such as
    # context={"authorization": "..."} is not inspected recursively. Only
    # the top-level keys passed to log_event are checked against the marker
    # list; values (including nested dicts) still go through secret
    # redaction, just not this name-based drop.
    lowered = name.lower()
    return any(marker in lowered for marker in _FORBIDDEN_FIELD_MARKERS)


_MAX_REDACT_DEPTH = 20


def _redact_deep(
    value: Any,
    secrets: Sequence[str],
    *,
    _seen: frozenset[int] | None = None,
    _depth: int = 0,
) -> Any:
    """Redact secrets from a value before it is ever serialized.

    Redacting before `json.dumps` (rather than only on the rendered JSON
    text) matters: `json.dumps` escapes non-ASCII characters, quotes, and
    backslashes by default, and a secret containing any of those characters
    would no longer match a plain `str.replace` on the rendered output.
    Redacting each leaf value first, while it is still the caller's raw
    string, sidesteps that entirely. Dict keys go through the same
    `redact()` call as values, for the same reason.

    This function must never raise and must never loop forever: it runs
    inside a logging Filter, outside the `handleError` protection that
    normally contains exceptions raised while emitting a record. A
    self-referential structure or an object whose `__str__` raises must
    degrade to a placeholder, not abort the caller that logged it.
    """
    if _depth > _MAX_REDACT_DEPTH:
        return "[max-depth-exceeded]"

    if isinstance(value, str):
        return redact(value, secrets)

    if isinstance(value, (dict, list, tuple)):
        marker = id(value)
        if _seen is not None and marker in _seen:
            return "[circular-reference]"
        seen = (_seen or frozenset()) | {marker}
        if isinstance(value, dict):
            return {
                redact(str(key), secrets): _redact_deep(
                    item, secrets, _seen=seen, _depth=_depth + 1
                )
                for key, item in value.items()
            }
        return [_redact_deep(item, secrets, _seen=seen, _depth=_depth + 1) for item in value]

    if value is None or isinstance(value, (int, float, bool)):
        return value

    # Anything else (exceptions, dataclasses, arbitrary objects) is what
    # `json.dumps(..., default=str)` would eventually stringify. Redact that
    # string form now, before serialization can escape it. `str(value)` is
    # caller-controlled code and may itself raise; that must not propagate.
    try:
        text = str(value)
    except Exception:
        return "[unrepresentable-value]"
    return redact(text, secrets)


class _RedactionFilter(logging.Filter):
    """Rewrites a LogRecord in place so redaction cannot be bypassed.

    Attached to a *logger* (not a handler), this runs exactly once per
    record — in `Logger.handle`, before `callHandlers` fans the record out —
    so every handler attached to that logger, present or added later, only
    ever sees an already-redacted record. A `Formatter` cannot provide this
    guarantee: it only touches the output of the one handler it is attached
    to.
    """

    def __init__(self, secrets: Sequence[str | None]) -> None:
        super().__init__()
        self._secrets = tuple(secret for secret in secrets if secret)

    def filter(self, record: logging.LogRecord) -> bool:
        # This runs in Logger.handle, outside the handleError protection
        # that wraps Handler.emit. A record this filter cannot fully
        # process (a pathological self-referential field, an object whose
        # __str__ raises, anything unforeseen) must never abort the caller
        # that logged it — logging must never be able to abort a run. Scrub
        # whatever can be scrubbed and always let the record through.
        try:
            # Forbidden-field dropping is a name-based safety check, not a
            # secret-value redaction — it applies even when no secrets are
            # configured, so it must not sit behind the `self._secrets`
            # guard below.
            fields = getattr(record, "fields", None)
            if fields is not None:
                record.fields = {  # type: ignore[attr-defined]
                    name: _redact_deep(value, self._secrets)
                    for name, value in fields.items()
                    if not _is_forbidden_field(name)
                }

            if not self._secrets:
                return True

            record.msg = _redact_deep(record.msg, self._secrets)

            if record.args:
                # Only str entries are redacted in place. A non-str arg
                # (e.g. a Decimal paired with a %d specifier) must reach
                # the eventual `record.getMessage()` % formatting
                # untouched — stringifying it here would silently break
                # %-style formatting for numeric specifiers. Its
                # stringified form is still covered by the post-render
                # redact() pass in the formatter.
                if isinstance(record.args, dict):
                    record.args = {
                        key: redact(value, self._secrets) if isinstance(value, str) else value
                        for key, value in record.args.items()
                    }
                else:
                    record.args = tuple(
                        redact(arg, self._secrets) if isinstance(arg, str) else arg
                        for arg in record.args
                    )

            if hasattr(record, "event"):
                record.event = _redact_deep(record.event, self._secrets)  # type: ignore[attr-defined]

            if record.exc_info:
                traceback_text = "".join(traceback.format_exception(*record.exc_info))
                record.exception = redact(traceback_text, self._secrets)  # type: ignore[attr-defined]
        except Exception:
            pass

        return True


class _RedactingJsonFormatter(logging.Formatter):
    """Renders the JSON envelope.

    By the time a record reaches this formatter, `_RedactionFilter` has
    already scrubbed it. This formatter also redacts the fully-rendered JSON
    text as a second, independent pass — cheap insurance in case any value
    reaches serialization through a path the filter did not anticipate.
    """

    def __init__(self, secrets: Sequence[str | None]) -> None:
        super().__init__()
        self._secrets = tuple(secret for secret in secrets if secret)

    def format(self, record: logging.LogRecord) -> str:
        event_value = getattr(record, "event", None)
        if event_value is None:
            event_value = record.getMessage()

        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "severity": record.levelname,
            "event": event_value,
        }
        for name, value in getattr(record, "fields", {}).items():
            if name in _ENVELOPE_FIELDS:
                continue
            payload[name] = value

        exception_text = getattr(record, "exception", None)
        if exception_text:
            payload["exception"] = exception_text

        rendered = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return redact(rendered, self._secrets)


def _harden_root_logger(secrets: Sequence[str | None]) -> None:
    """Ensure records that propagate to the root logger cannot leak secrets.

    Third-party libraries (e.g. an HTTP client) log through their own
    loggers, which by default propagate to root. If root has no handler,
    Python's `logging.lastResort` prints WARNING+ records to stderr
    completely unredacted. `Logger.filters` on an ancestor logger are *not*
    consulted during that propagation walk — only a handler's own filters
    are — so redaction here has to live on the handler(s) root actually
    uses, not on root's logger-level filter list.

    A global hook does exist that would run for every record on every
    logger before any handler is consulted (`logging.setLogRecordFactory`,
    invoked from `Logger.makeRecord`). We deliberately did not take that
    on: it is process-global mutable state that must chain politely with
    any other code that also sets a record factory, and it still runs
    before `extra` is merged into the record, so this filter would still be
    needed to redact `event`/`fields`. Handler-level attachment, covering
    what `configure_logging` controls plus a one-time root hardening pass,
    is the boundary this module takes responsibility for.
    """
    root = logging.getLogger()
    redaction_filter = _RedactionFilter(secrets)

    for handler in root.handlers:
        for existing in [f for f in handler.filters if isinstance(f, _RedactionFilter)]:
            handler.removeFilter(existing)
        handler.addFilter(redaction_filter)

    if not root.handlers:
        safety_net = logging.StreamHandler()
        safety_net.addFilter(redaction_filter)
        safety_net._notable_root_safety_net = True  # type: ignore[attr-defined]
        root.addHandler(safety_net)


def configure_logging(
    log_file: Path, config: LoggingConfig, *, secrets: Sequence[str | None]
) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(EVENT_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    for existing_filter in list(logger.filters):
        logger.removeFilter(existing_filter)
    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    redaction_filter = _RedactionFilter(secrets)

    # Redaction is attached to the LOGGER, not the handler: it runs once in
    # `Logger.handle` before the record is fanned out, so it covers this
    # handler and any handler added to this logger afterwards, for any
    # record that originates directly on this logger.
    logger.addFilter(redaction_filter)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=config.max_bytes,
        backupCount=config.backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(_RedactingJsonFormatter(secrets))
    # A record originating on a CHILD logger (e.g. "notable.provider") walks
    # up to this handler via `Logger.callHandlers`, which only consults
    # *handler*-level filters during that walk, never the ancestor logger's
    # `Logger.filters`. The same filter is attached here too so redaction
    # still applies to that path.
    handler.addFilter(redaction_filter)
    logger.addHandler(handler)

    _harden_root_logger(secrets)

    return logger


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    severity: int = logging.INFO,
    **fields: Any,
) -> None:
    logger.log(severity, event, extra={"event": event, "fields": fields})
