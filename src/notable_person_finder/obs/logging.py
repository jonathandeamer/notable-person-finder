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

    This function must never loop forever, and degrades known hazards to a
    placeholder rather than raising: a self-referential structure, an
    over-deep subtree, a value whose `__str__` raises. It is NOT
    raise-proof, though — `redact(str(key), ...)` on a mapping key is
    unguarded, and any input this walk did not anticipate can propagate
    out. `_RedactionFilter.filter` is the backstop for that: it catches and
    fails closed, dropping the record rather than emitting it unscrubbed.
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
        return [
            _redact_deep(item, secrets, _seen=seen, _depth=_depth + 1) for item in value
        ]

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


def _redact_arg(arg: Any, secrets: Sequence[str]) -> Any:
    """Redact one `%`-style argument without breaking `%`-formatting.

    A non-`str` arg must normally reach `record.getMessage()` untouched, or a
    numeric specifier (`%d` against a `Decimal`) raises. But leaving it
    untouched unconditionally means its secret is never scrubbed on the
    record itself, and a handler with a formatter we do not own — every
    pre-existing root handler, and the root safety net — renders it straight
    out. So: render it, and only substitute the redacted text when the
    rendered form actually contained a secret. Secret-free args (the
    overwhelming majority) keep their type and their specifier; a secret
    bearing arg loses its type, which at worst breaks `%d` and costs the
    line, never the secret.

    BOTH `str(arg)` and `repr(arg)` are tested, because the caller chooses
    which one renders: `%s` uses `str`, `%r` uses `repr`. An object with a
    clean `__str__` and a secret-bearing `__repr__` leaks unless `repr` is
    inspected too. `str` is tried first so that the common case (the secret
    visible in both) substitutes the `str` form and `%s` output keeps its
    usual shape.

    Neither call is assumed safe: both are caller-controlled code and either
    may raise. A raise in one does not stop the other being tried; if both
    fail the arg is returned unchanged, and `getMessage()` raises later
    inside `Handler.emit` (see the leak inventory's row 3 for what that path
    does and does not guarantee).
    """
    if isinstance(arg, str):
        return redact(arg, secrets)
    for render in (str, repr):
        try:
            text = render(arg)
        except Exception:
            continue
        redacted = redact(text, secrets)
        if redacted != text:
            return redacted
    return arg


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
        # that logged it — logging must never be able to abort a run.
        #
        # It must also never be emitted. If the scrub aborts partway, this
        # filter cannot vouch for the record: the field rebuild below is
        # all-or-nothing, so a raise leaves `record.fields` holding the
        # caller's ORIGINAL values, forbidden field names and all. Falling
        # through with `return True` would hand exactly that to the
        # formatter. So we fail CLOSED: swallow the exception (the caller
        # keeps running) and drop the line (the secret never lands on
        # disk). Losing one log line is the pre-existing behaviour for any
        # record that cannot be serialized; emitting it raw is the single
        # failure this module exists to prevent.
        try:
            # Forbidden-field dropping is a name-based safety check, not a
            # secret-value redaction — it applies even when no secrets are
            # configured, so it must not sit behind the `self._secrets`
            # guard below.
            fields = getattr(record, "fields", None)
            if fields is not None:
                # The NAME is redacted on the same footing as a dict key
                # inside a value (see `_redact_deep`'s dict branch): a field
                # name is serialized as a JSON object key, so an unredacted
                # secret used as a name is escaped by `json.dumps` and
                # recovered verbatim by `json.loads`. The forbidden-name
                # check deliberately runs against the CALLER's original name,
                # before redaction, because it is a name-based safety check
                # rather than a value scrub.
                record.fields = {  # type: ignore[attr-defined]
                    redact(str(name), self._secrets): _redact_deep(value, self._secrets)
                    for name, value in fields.items()
                    if not _is_forbidden_field(name)
                }

            if not self._secrets:
                return True

            record.msg = _redact_deep(record.msg, self._secrets)

            if record.args:
                if isinstance(record.args, dict):
                    record.args = {
                        key: _redact_arg(value, self._secrets)
                        for key, value in record.args.items()
                    }
                else:
                    record.args = tuple(
                        _redact_arg(arg, self._secrets) for arg in record.args
                    )

            if hasattr(record, "event"):
                record.event = _redact_deep(record.event, self._secrets)  # type: ignore[attr-defined]

            if record.exc_info:
                traceback_text = "".join(traceback.format_exception(*record.exc_info))
                record.exception = redact(traceback_text, self._secrets)  # type: ignore[attr-defined]
        except Exception:
            return False

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
            # `record.args` entries that are not already `str` keep their
            # original type when secret-free, so %-formatting keeps working
            # for numeric specifiers (`%d` with a Decimal). Their string
            # form is produced HERE, when getMessage() renders `msg % args`.
            # Redact that rendered text now, while it is still a raw Python
            # string — after json.dumps it may be escaped (non-ASCII, quote,
            # backslash) and the post-render redact() pass below could no
            # longer match it. This is the last redaction point before
            # serialization for this axis.
            event_value = redact(record.getMessage(), self._secrets)
        elif isinstance(event_value, str):
            # Already redacted by the filter; idempotent, and the only
            # guarantee if this formatter is ever reached by another route.
            event_value = redact(event_value, self._secrets)

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

        rendered = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=str
        )
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

    `logging.setLogRecordFactory()` is a supported global hook that would
    genuinely cover this gap: `Logger.makeRecord` calls it for every record
    on every logger, so it reaches any future handler on any logger, which
    handler-level attachment cannot. We decline it on a deliberate
    trade-off, not because it would not work — it is process-global mutable
    state, a single slot this library would have to claim from the
    application and chain politely with anyone else who sets a factory.
    (It also runs before `extra` is merged onto the record, so the filter
    would still be needed for `event`/`fields` regardless.) Handler-level
    attachment, covering what `configure_logging` controls plus a one-time
    root hardening pass, is the boundary this module takes responsibility
    for; installing a process-global factory is the application's call.
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
