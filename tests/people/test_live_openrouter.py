"""Opt-in smoke coverage for the real OpenRouter provider boundary.

The live smoke uses the production ``OpenRouterClient`` against the public
OpenRouter endpoint with the application's configured model and routing
contract. Tests are marked ``live`` so the standard suite makes no network
call; invoke them with ``pytest -m live``. Missing credentials and
pre-request network unavailability skip; auth, capability, schema, and
returned-response failures raise.

Never uses operator state directories, databases, or digests. Credentials come
only from the process environment variable named by the secret contract.
"""

from __future__ import annotations

import errno
import json
import os
import socket
import ssl
from typing import Any

import httpx
import pytest
from openrouter.errors.no_response_error import NoResponseError
from openrouter.errors.openroutererror import OpenRouterError

from notable_person_finder.config.models import (
    DetectPeopleConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    ModelInspectionRequest,
    OpenRouterClient,
    StructuredGenerationRequest,
)
from notable_person_finder.runs.clock import SystemClock

# Smallest strict JSON Schema the generation contract will accept.
_MINIMAL_STRICT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "ok": {"type": "boolean"},
    },
    "required": ["ok"],
    "additionalProperties": False,
}
_MINIMAL_SCHEMA_NAME = "live_smoke_ok"
_API_KEY_ENV = "OPENROUTER_API_KEY"
_UNREACHABLE_NETWORK_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH})


def _failure_caused_by(
    category: FailureCategory,
    cause: Exception,
    *,
    status_code: int | None = None,
    detail: str | None = None,
) -> ProviderFailure:
    failure = ProviderFailure(
        category,
        provider="openrouter",
        operation="inspect_model",
        status_code=status_code,
        detail=detail,
    )
    failure.__cause__ = cause
    return failure


def _connect_error_caused_by(cause: Exception) -> httpx.ConnectError:
    connect_error = httpx.ConnectError("connect failed")
    connect_error.__cause__ = cause
    return connect_error


def _http_error(status: int, message: str = "provider error") -> OpenRouterError:
    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(status, request=request, content=b"{}")
    return OpenRouterError(message, response, body="{}")


@pytest.mark.parametrize(
    ("category", "cause"),
    [
        (FailureCategory.NETWORK, socket.gaierror(socket.EAI_NONAME, "no name")),
        (
            FailureCategory.NETWORK,
            _connect_error_caused_by(OSError(errno.ENETUNREACH, "no route")),
        ),
        (FailureCategory.NETWORK, ConnectionRefusedError("refused")),
        (FailureCategory.TIMEOUT, httpx.ConnectTimeout("connect timed out")),
        (FailureCategory.NETWORK, NoResponseError("no response")),
    ],
)
def test_environment_unavailability_has_a_skip_reason(
    category: FailureCategory, cause: Exception
) -> None:
    assert _skip_reason(_failure_caused_by(category, cause)) is not None


@pytest.mark.parametrize(
    ("category", "cause", "status_code"),
    [
        (FailureCategory.AUTHENTICATION, _http_error(401), 401),
        (FailureCategory.ACCESS_DENIED, _http_error(403), 403),
        (FailureCategory.CONFIGURATION, _http_error(400), 400),
        (FailureCategory.UNSUPPORTED_CAPABILITY, RuntimeError("capability"), None),
        (FailureCategory.MALFORMED_RESPONSE, ValueError("bad body"), None),
        (FailureCategory.RATE_LIMIT, _http_error(429), 429),
        (FailureCategory.BUDGET_EXHAUSTED, _http_error(402), 402),
        (
            FailureCategory.NETWORK,
            _connect_error_caused_by(ssl.SSLError("TLS handshake failed")),
            None,
        ),
        (FailureCategory.TIMEOUT, httpx.ReadTimeout("read timed out"), None),
        (FailureCategory.TRANSIENT_SERVER_ERROR, _http_error(500), 500),
    ],
)
def test_deterministic_provider_failures_have_no_skip_reason(
    category: FailureCategory, cause: Exception, status_code: int | None
) -> None:
    assert (
        _skip_reason(_failure_caused_by(category, cause, status_code=status_code))
        is None
    )


def _cause_chain(error: BaseException) -> tuple[BaseException, ...]:
    """Return the explicit/contextual causes without looping on malformed chains."""
    causes: list[BaseException] = []
    current = error.__cause__ or error.__context__
    while current is not None and id(current) not in {id(item) for item in causes}:
        causes.append(current)
        current = current.__cause__ or current.__context__
    return tuple(causes)


def _skip_reason(error: ProviderFailure) -> str | None:
    """Name only environment failures that make a live assertion impossible.

    Once a request can be known to have reached OpenRouter, auth, capability,
    schema, rate-limit, billing, and response-shape failures must surface as
    hard failures so the smoke cannot silently pass on a broken contract.
    """
    causes = _cause_chain(error)
    if error.category is FailureCategory.TIMEOUT and any(
        isinstance(cause, httpx.ConnectTimeout) for cause in causes
    ):
        return "live OpenRouter connection timed out"
    if error.category is not FailureCategory.NETWORK:
        return None
    if any(isinstance(cause, socket.gaierror) for cause in causes):
        return "live DNS unavailable"
    if any(isinstance(cause, ConnectionRefusedError) for cause in causes):
        return "live OpenRouter connection refused"
    if any(
        isinstance(cause, OSError) and cause.errno in _UNREACHABLE_NETWORK_ERRNOS
        for cause in causes
    ):
        return "live network unreachable"
    # NoResponseError / bare ConnectError without a nested connect cause is
    # still pre-response environment unavailability for this smoke.
    if any(isinstance(cause, NoResponseError) for cause in causes):
        return "live OpenRouter returned no response"
    if any(isinstance(cause, httpx.ConnectError) for cause in causes) and not any(
        isinstance(cause, ssl.SSLError) for cause in causes
    ):
        return "live OpenRouter connection failed"
    if error.detail in {"NoResponseError", "ConnectError", "ConnectTimeout"}:
        return f"live OpenRouter transport unavailable ({error.detail})"
    return None


def _require_api_key() -> str:
    """Return the process-environment key, or skip before any request."""
    raw = os.environ.get(_API_KEY_ENV)
    if raw is None or not raw.strip():
        pytest.skip(f"{_API_KEY_ENV} is unset; live OpenRouter smoke deselected")
    return raw.strip()


def _configured_client(api_key: str) -> OpenRouterClient:
    """Build the production client from application defaults, not operator state."""
    openrouter = OpenRouterConfig()
    return OpenRouterClient(
        api_key=api_key,
        endpoint=openrouter.endpoint,
        routing=openrouter.routing,
        timeout_seconds=300.0,
        clock=SystemClock(),
    )


def _call_or_skip(operation: Any) -> Any:
    """Run one OpenRouter call; skip only pre-request environment failures."""
    try:
        return operation()
    except ProviderFailure as error:
        if reason := _skip_reason(error):
            pytest.skip(reason)
        raise


@pytest.mark.live
def test_configured_model_inspection_reports_capability_and_pricing() -> None:
    """Capability must include strict structured output for the configured model."""
    api_key = _require_api_key()
    detect = DetectPeopleConfig()

    with _configured_client(api_key) as client:
        result = _call_or_skip(
            lambda: client.inspect_model(ModelInspectionRequest(model_id=detect.model))
        )

    assert result.configured_model_id == detect.model
    assert isinstance(result.resolved_model_id, str) and result.resolved_model_id
    assert result.supports_strict_structured_output is True
    assert "response_format" in result.supported_parameters
    assert "structured_outputs" in result.supported_parameters
    assert result.latency_ms >= 0
    if result.prompt_unit_price_nano_usd is not None:
        assert result.prompt_unit_price_nano_usd >= 0
    if result.completion_unit_price_nano_usd is not None:
        assert result.completion_unit_price_nano_usd >= 0


@pytest.mark.live
def test_smallest_strict_schema_generation_is_parseable() -> None:
    """A minimal strict generation must return identifiers, usage, and JSON."""
    api_key = _require_api_key()
    detect = DetectPeopleConfig()
    routing = ProviderRoutingConfig()
    assert routing.data_collection == "deny"
    assert routing.zdr is True

    request = StructuredGenerationRequest(
        model_id=detect.model,
        system_prompt="Return only the JSON object required by the schema.",
        user_content="Set ok to true.",
        json_schema=_MINIMAL_STRICT_SCHEMA,
        schema_name=_MINIMAL_SCHEMA_NAME,
        max_completion_tokens=32,
        temperature=0.0,
        top_p=1.0,
        reasoning_effort=None,
    )

    with _configured_client(api_key) as client:
        result = _call_or_skip(lambda: client.generate_structured(request))

    assert result.configured_model_id == detect.model
    assert isinstance(result.resolved_model_id, str) and result.resolved_model_id
    assert isinstance(result.provider_request_id, str) and result.provider_request_id
    assert result.usage is not None
    assert result.usage.prompt_tokens >= 0
    assert result.usage.completion_tokens >= 0
    assert result.usage.total_tokens >= 0
    assert result.latency_ms >= 0
    if result.actual_nano_usd is not None:
        assert result.actual_nano_usd >= 0
    if result.serving_provider is not None:
        assert isinstance(result.serving_provider, str) and result.serving_provider
    # Parseability only — never assert on free-text content of the model body.
    payload = json.loads(result.raw_text)
    assert isinstance(payload, dict)
    assert set(payload) == {"ok"}
    assert isinstance(payload["ok"], bool)
