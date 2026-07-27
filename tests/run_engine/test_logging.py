from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from notable_person_finder.config.models import LoggingConfig
from notable_person_finder.obs.logging import (
    EVENT_LOGGER_NAME,
    configure_logging,
    log_event,
    redact,
)


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


def iter_strings(value: object) -> Iterator[str]:
    """Yield every string reachable in a parsed JSON structure, keys included."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from iter_strings(key)
            yield from iter_strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_strings(item)


def assert_secret_absent(events: object, secret: str) -> None:
    """Assert a secret is absent from the PARSED structure, not the file text.

    A raw-text search is not evidence of absence: `json.dumps` defaults to
    `ensure_ascii=True` and escapes quotes and backslashes, so a secret that
    is fully recoverable via `json.loads` can be invisible to a text grep of
    the same file. Every absence assertion in this module goes through here
    (or checks a parsed value directly) for that reason.
    """
    for text in iter_strings(events):
        assert secret not in text, f"secret recoverable from parsed log: {text!r}"


def test_events_are_json_lines_with_the_required_envelope(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(
        logger, "provider_attempt_finished", run_id=1, attempt_id=9, outcome="succeeded"
    )
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
    # Checked against the parsed structure, not `json.dumps(event)`: re-dumping
    # re-escapes, which would hide the very leak this asserts against.
    assert_secret_absent(event, "or-secret-value")
    assert "[redacted]" in event["detail"]


def test_secrets_with_non_ascii_or_quote_characters_are_redacted(
    tmp_path: Path,
) -> None:
    # json.dumps escapes non-ASCII characters and quotes by default. If
    # redaction only ran on the rendered JSON text, the escaped form of
    # these secrets would no longer match a plain str.replace and they would
    # be written to disk fully recoverable via json.loads.
    non_ascii_secret = "sk-café-1234"
    quote_secret = 'ab"cd'
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=2048, backup_count=2),
        secrets=(non_ascii_secret, quote_secret),
    )
    try:
        log_event(
            created,
            "provider_attempt_failed",
            detail=f"key={non_ascii_secret} and quoted={quote_secret} rejected",
            # A secret can also appear as a mapping KEY, not just a value.
            # The key gets stringified during serialization the same way a
            # value does, and is just as exposed to the escaping issue.
            context={non_ascii_secret: "v"},
        )
    finally:
        for handler in list(created.handlers):
            handler.close()
            created.removeHandler(handler)

    raw_text = (tmp_path / "logs" / "notable.jsonl").read_text(encoding="utf-8")
    assert non_ascii_secret not in raw_text
    assert quote_secret not in raw_text

    # json.dumps escapes non-ASCII characters and quotes; a secret absent
    # from the raw file text can still be fully recoverable once the line
    # is parsed back with json.loads. Check the parsed value directly,
    # not a re-dumped (re-escaped) version of it.
    # Walks every parsed string, KEYS included, rather than a re-dumped
    # (re-escaped) copy — re-dumping would hide the same leak all over again.
    event = json.loads(raw_text.splitlines()[0])
    assert_secret_absent(event, non_ascii_secret)
    assert_secret_absent(event, quote_secret)


def test_an_exception_traceback_is_captured_and_redacted(
    tmp_path: Path, logger: logging.Logger
) -> None:
    try:
        raise ValueError("Bearer brave-secret-value")
    except ValueError:
        logger.exception("internal_error")

    raw_text = (tmp_path / "logs" / "notable.jsonl").read_text(encoding="utf-8")
    assert "brave-secret-value" not in raw_text

    event = json.loads(raw_text.splitlines()[0])
    # The traceback is a serialized string; the raw-text check above is not
    # sufficient on its own, so assert absence on the parsed value too.
    assert_secret_absent(event, "brave-secret-value")
    assert "Traceback" in event["exception"]
    assert "ValueError" in event["exception"]


def test_a_circular_reference_field_does_not_abort_the_run(
    tmp_path: Path, logger: logging.Logger
) -> None:
    # The redaction filter runs in Logger.handle, outside the handleError
    # protection that wraps Handler.emit. A self-referential field must be
    # scrubbed to a placeholder, not sent into infinite recursion that
    # crashes whatever code was logging about it.
    circular: dict[str, object] = {"self": None}
    circular["self"] = circular

    log_event(logger, "provider_attempt_failed", context=circular)
    # If the filter propagated an exception, this second call would never
    # run and the assertion below would see only one event.
    log_event(logger, "run_progress", run_id=1)

    events = read_events(tmp_path)
    assert [event["event"] for event in events] == [
        "provider_attempt_failed",
        "run_progress",
    ]


class _RaisesOnStr:
    def __str__(self) -> str:
        raise RuntimeError("boom")


def test_a_field_whose_str_raises_does_not_abort_the_run(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_attempt_failed", detail=_RaisesOnStr())
    log_event(logger, "run_progress", run_id=1)

    events = read_events(tmp_path)
    assert [event["event"] for event in events] == [
        "provider_attempt_failed",
        "run_progress",
    ]


def test_percent_style_args_with_non_string_values_still_format(
    tmp_path: Path, logger: logging.Logger
) -> None:
    # A secret-free non-str %-arg (e.g. routed through the root safety net
    # from a third-party logger) must reach %-formatting with its type
    # intact. Stringifying it during redaction would break a numeric
    # specifier like %d.
    logger.info("took %d ms", Decimal("5"))

    raw_text = (tmp_path / "logs" / "notable.jsonl").read_text(encoding="utf-8")
    events = [json.loads(line) for line in raw_text.splitlines() if line]
    assert events[-1]["event"] == "took 5 ms"


def test_non_string_percent_args_containing_secrets_are_redacted(
    tmp_path: Path,
) -> None:
    # `record.args` entries that are not already `str` are deliberately left
    # untouched by the filter so %-formatting for numeric specifiers keeps
    # working. Their secret only materialises when `record.getMessage()`
    # renders them, which happens in the formatter. That rendered message
    # must be redacted BEFORE `json.dumps` sees it: afterwards the secret is
    # escaped (non-ASCII, quotes, backslashes) and a plain `str.replace` on
    # the rendered text can no longer match it — the secret then survives in
    # the file, invisible to a raw-text grep but recoverable via json.loads.
    non_ascii_secret = "sk-café-1234"

    class _Response:
        def __repr__(self) -> str:
            return f"<Response token={non_ascii_secret}>"

    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=8192, backup_count=2),
        secrets=(non_ascii_secret,),
    )
    try:
        created.info("upstream returned %s after %d ms", _Response(), 12)
    finally:
        for handler in list(created.handlers):
            handler.close()
            created.removeHandler(handler)

    events = read_events(tmp_path)
    assert_secret_absent(events, non_ascii_secret)
    # The line must still be written and still be readable.
    assert "[redacted]" in events[-1]["event"]
    assert events[-1]["event"].endswith("after 12 ms")


def test_top_level_field_names_containing_secrets_are_redacted(tmp_path: Path) -> None:
    # A secret can arrive as a FIELD NAME, not just as a value or as a key
    # inside a field's value. `log_event(**fields)` accepts any string key
    # CPython will pass through `**kwargs` — including non-identifier and
    # non-ASCII shapes — and the name is serialized as a JSON object key, so
    # it is exposed to exactly the escaping hazard that hides an unredacted
    # secret from a raw-text grep while `json.loads` hands it straight back.
    identifier_secret = "sk_café_1234"
    awkward_secret = 'sk-café"\\-1234'
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=8192, backup_count=2),
        secrets=(identifier_secret, awkward_secret),
    )
    try:
        log_event(
            created,
            "provider_attempt_failed",
            **{identifier_secret: "v", awkward_secret: "w"},
        )
    finally:
        for handler in list(created.handlers):
            handler.close()
            created.removeHandler(handler)

    events = read_events(tmp_path)
    assert len(events) == 1
    assert_secret_absent(events, identifier_secret)
    assert_secret_absent(events, awkward_secret)


def test_args_whose_repr_carries_a_secret_are_redacted_on_the_root_path(
    tmp_path: Path,
) -> None:
    # `%r` renders `repr(arg)`, not `str(arg)`. An arg with a clean `__str__`
    # and a secret-bearing `__repr__` must still be scrubbed on the RECORD,
    # because handlers we do not own (every pre-existing root handler, and
    # the root safety net) render it with their own formatter.
    secret = 'sk-café"\\-1234'

    class _Sneaky:
        def __str__(self) -> str:
            return "<Response>"

        def __repr__(self) -> str:
            return f"<Response token={secret}>"

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    stream = io.StringIO()
    pre_existing = logging.StreamHandler(stream)
    root.addHandler(pre_existing)
    root.setLevel(logging.WARNING)
    notable_logger = logging.getLogger(EVENT_LOGGER_NAME)

    try:
        configure_logging(
            tmp_path / "logs" / "notable.jsonl",
            LoggingConfig(max_bytes=2048, backup_count=2),
            secrets=(secret,),
        )
        third_party = logging.getLogger("thirdparty.notable_test_task14_repr")
        third_party.propagate = True
        third_party.warning("resp=%r", _Sneaky())
    finally:
        pre_existing.close()
        root.removeHandler(pre_existing)
        root.handlers[:] = original_handlers
        root.setLevel(original_level)
        for handler in list(notable_logger.handlers):
            handler.close()
            notable_logger.removeHandler(handler)

    # Plain-text handler output: raw text is the final rendered form here.
    rendered = stream.getvalue()
    assert secret not in rendered
    assert "[redacted]" in rendered


class _KeyWhoseStrRaises(str):
    """A `str` subclass whose `str()` raises.

    `json.dumps` accepts it verbatim as a mapping key (it is a `str`
    instance), but `redact(str(key), ...)` inside the redaction walk raises
    — so it is exactly the input that makes the filter's scrub abort partway
    while the record itself remains perfectly serializable.
    """

    def __str__(self) -> str:
        raise RuntimeError("boom")


def test_a_record_the_filter_cannot_fully_scrub_is_dropped(tmp_path: Path) -> None:
    # If the redaction walk raises partway, the filter cannot vouch for the
    # record. It must fail CLOSED and drop the line, never fall through and
    # hand the original, unscrubbed record to the formatter.
    non_ascii_secret = "sk-café-1234"
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=8192, backup_count=2),
        secrets=(non_ascii_secret,),
    )
    try:
        log_event(
            created,
            "provider_attempt_failed",
            context={_KeyWhoseStrRaises("kk"): "v"},
            authorization="Bearer abc",
            detail=f"key={non_ascii_secret} rejected",
        )
        # The run must continue: dropping one line is not aborting the caller.
        log_event(created, "run_progress", run_id=1)
    finally:
        for handler in list(created.handlers):
            handler.close()
            created.removeHandler(handler)

    events = read_events(tmp_path)
    assert [event["event"] for event in events] == ["run_progress"]
    assert_secret_absent(events, non_ascii_secret)
    assert_secret_absent(events, "Bearer abc")
    assert all("authorization" not in event for event in events)


def test_forbidden_fields_are_dropped_even_with_no_secrets_configured(
    tmp_path: Path,
) -> None:
    created = configure_logging(
        tmp_path / "logs" / "notable.jsonl",
        LoggingConfig(max_bytes=2048, backup_count=2),
        secrets=(),
    )
    try:
        log_event(created, "provider_request", authorization="Bearer abc")
    finally:
        for handler in list(created.handlers):
            handler.close()
            created.removeHandler(handler)

    event = read_events(tmp_path)[0]
    assert "authorization" not in event
    assert_secret_absent(event, "Bearer abc")


def test_authorization_like_fields_are_dropped_entirely(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(logger, "provider_request", authorization="Bearer abc", api_key="xyz")
    event = read_events(tmp_path)[0]
    assert "authorization" not in event
    assert "api_key" not in event
    assert_secret_absent(event, "Bearer abc")
    assert_secret_absent(event, "xyz")


def test_url_and_header_like_fields_are_dropped_entirely(
    tmp_path: Path, logger: logging.Logger
) -> None:
    log_event(
        logger,
        "provider_request",
        article_url="https://en.wikipedia.org/wiki/Some_Notable_Person",
        headers={"Authorization": "Bearer or-secret-value"},
    )
    event = read_events(tmp_path)[0]
    assert "article_url" not in event
    assert "headers" not in event
    assert_secret_absent(event, "Some_Notable_Person")
    assert_secret_absent(event, "or-secret-value")


def test_a_second_handler_on_the_logger_also_receives_redacted_records(
    tmp_path: Path, logger: logging.Logger
) -> None:
    # Proves redaction lives on the LOGGER, not on the one handler wired up
    # by configure_logging: a handler added afterwards, with its own
    # formatter that never calls redact() itself, still only ever sees an
    # already-redacted record. This must fail against a design where
    # redaction is implemented solely inside a Formatter.
    class _FieldsFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            return json.dumps(getattr(record, "fields", {}))

    stream = io.StringIO()
    second_handler = logging.StreamHandler(stream)
    second_handler.setFormatter(_FieldsFormatter())
    logger.addHandler(second_handler)
    try:
        log_event(
            logger, "provider_attempt_failed", detail="key=or-secret-value rejected"
        )
    finally:
        second_handler.close()
        logger.removeHandler(second_handler)

    # That handler's output is itself JSON, so parse it before asserting
    # absence rather than grepping the escaped text.
    assert_secret_absent(json.loads(stream.getvalue()), "or-secret-value")


def test_direct_logger_calls_and_child_loggers_stay_redacted(
    tmp_path: Path, logger: logging.Logger
) -> None:
    logger.info("plain message %s", "or-secret-value")
    child = logging.getLogger(f"{logger.name}.provider")
    child.info("child message %s", "brave-secret-value")

    events = read_events(tmp_path)
    assert len(events) == 2
    assert_secret_absent(events, "or-secret-value")
    assert_secret_absent(events, "brave-secret-value")


def test_third_party_loggers_reaching_root_are_redacted(tmp_path: Path) -> None:
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    stream = io.StringIO()
    pre_existing = logging.StreamHandler(stream)
    root.addHandler(pre_existing)
    root.setLevel(logging.WARNING)
    notable_logger = logging.getLogger(EVENT_LOGGER_NAME)

    try:
        configure_logging(
            tmp_path / "logs" / "notable.jsonl",
            LoggingConfig(max_bytes=2048, backup_count=2),
            secrets=("brave-secret-value",),
        )
        third_party = logging.getLogger("thirdparty.notable_test_task14")
        third_party.propagate = True
        third_party.warning("token=%s", "brave-secret-value")
    finally:
        pre_existing.close()
        root.removeHandler(pre_existing)
        root.handlers[:] = original_handlers
        root.setLevel(original_level)
        for handler in list(notable_logger.handlers):
            handler.close()
            notable_logger.removeHandler(handler)

    # This handler renders with the stdlib default formatter, so its stream
    # holds the final plain-text form with no serialization/escaping layer
    # between it and the reader. Here — unlike the JSON paths above — the
    # raw text IS the parsed form, so a text search is a valid absence check.
    assert "brave-secret-value" not in stream.getvalue()


def test_non_string_percent_args_are_redacted_on_the_root_path_too(
    tmp_path: Path,
) -> None:
    # Handlers we do not own (every pre-existing root handler, and the root
    # safety net) render with their own formatter, so the rendered-message
    # redaction inside _RedactingJsonFormatter cannot protect them. A non-str
    # arg carrying a secret must therefore be scrubbed on the RECORD itself.
    secret = "sk-café-1234"

    class _Response:
        def __repr__(self) -> str:
            return f"<Response token={secret}>"

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    stream = io.StringIO()
    pre_existing = logging.StreamHandler(stream)
    root.addHandler(pre_existing)
    root.setLevel(logging.WARNING)
    notable_logger = logging.getLogger(EVENT_LOGGER_NAME)

    try:
        configure_logging(
            tmp_path / "logs" / "notable.jsonl",
            LoggingConfig(max_bytes=2048, backup_count=2),
            secrets=(secret,),
        )
        third_party = logging.getLogger("thirdparty.notable_test_task14_args")
        third_party.propagate = True
        third_party.warning("upstream returned %s", _Response())
    finally:
        pre_existing.close()
        root.removeHandler(pre_existing)
        root.handlers[:] = original_handlers
        root.setLevel(original_level)
        for handler in list(notable_logger.handlers):
            handler.close()
            notable_logger.removeHandler(handler)

    # Plain-text handler output: raw text is the final rendered form here.
    rendered = stream.getvalue()
    assert secret not in rendered
    assert "[redacted]" in rendered


def test_rotation_keeps_the_configured_number_of_files(
    tmp_path: Path, logger: logging.Logger
) -> None:
    for index in range(400):
        log_event(logger, "run_progress", run_id=1, index=index, note="x" * 50)
    files = sorted(p.name for p in (tmp_path / "logs").iterdir())
    assert "notable.jsonl" in files
    assert "notable.jsonl.1" in files
    assert len(files) == 3  # the live file plus backup_count=2, no more


def test_redact_replaces_every_occurrence() -> None:
    assert (
        redact("a secret and secret again", ("secret",))
        == "a [redacted] and [redacted] again"
    )


def test_redact_ignores_empty_secrets() -> None:
    assert redact("unchanged", ("", None)) == "unchanged"
