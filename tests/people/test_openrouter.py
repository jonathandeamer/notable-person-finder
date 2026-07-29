"""Contract tests for the narrow OpenRouter SDK translation adapter.

All tests are offline. A recording fake SDK stands in for the official client
so request construction, pricing conversion, and exception sanitization are
asserted without a network.
"""

from __future__ import annotations

import logging
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import httpx
import pytest
from openrouter.errors.no_response_error import NoResponseError
from openrouter.errors.openroutererror import OpenRouterError
from openrouter.errors.responsevalidationerror import ResponseValidationError
from openrouter.utils.retries import BackoffStrategy, RetryConfig

from notable_person_finder.config.models import (
    NANO_USD,
    ProviderRoutingConfig,
)
from notable_person_finder.providers.failures import FailureCategory, ProviderFailure
from notable_person_finder.providers.openrouter import (
    LlmClient,
    ModelInspectionRequest,
    OpenRouterClient,
    StructuredGenerationRequest,
)
from notable_person_finder.runs.clock import FakeClock
from tests.people.fakes_openrouter import (
    FakeChatResult,
    FakeChoice,
    FakeOpenRouterSdk,
    FakeUsage,
    default_chat_result,
    default_model_response,
)

MODEL_ID = "openai/gpt-test"
SCHEMA = {
    "type": "object",
    "properties": {"item_outcome": {"type": "string"}},
    "required": ["item_outcome"],
    "additionalProperties": False,
}


def _client(
    sdk: FakeOpenRouterSdk,
    *,
    routing: ProviderRoutingConfig | None = None,
    timeout_seconds: float = 300.0,
    clock: FakeClock | None = None,
) -> OpenRouterClient:
    return OpenRouterClient(
        api_key="test-key-not-a-secret-for-production",
        endpoint="https://openrouter.ai/api/v1",
        routing=routing or ProviderRoutingConfig(),
        timeout_seconds=timeout_seconds,
        sdk=sdk,
        clock=clock or FakeClock(),
    )


def _generation_request(**overrides: Any) -> StructuredGenerationRequest:
    values: dict[str, Any] = {
        "model_id": MODEL_ID,
        "system_prompt": "You are a careful classifier.",
        "user_content": '{"source_item_id": 1}',
        "json_schema": SCHEMA,
        "schema_name": "detect_people",
        "max_completion_tokens": 1024,
        "temperature": 0.0,
        "top_p": 1.0,
        "reasoning_effort": None,
    }
    values.update(overrides)
    return StructuredGenerationRequest(**values)


def _http_error(
    status: int,
    *,
    body: str = '{"error":{"message":"upstream detail with sk-secret"}}',
    headers: dict[str, str] | None = None,
    message: str = "provider said something sensitive",
) -> OpenRouterError:
    request = httpx.Request("POST", "https://openrouter.ai/api/v1/chat/completions")
    response = httpx.Response(
        status,
        headers=headers or {},
        request=request,
        content=body.encode(),
    )
    return OpenRouterError(message, response, body=body)


# --- Protocol and lifecycle ----------------------------------------------------


def test_openrouter_client_satisfies_llm_client_protocol() -> None:
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(),
        chat_result=default_chat_result(),
    )
    client: LlmClient = _client(sdk)
    assert callable(client.inspect_model)
    assert callable(client.generate_structured)


def test_context_manager_closes_the_run_scoped_sdk() -> None:
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(),
        chat_result=default_chat_result(),
    )
    client = _client(sdk)
    with client as entered:
        assert entered is client
        assert sdk.entered
        assert not sdk.exited
    assert sdk.exited


# --- Inspection ----------------------------------------------------------------


def test_inspect_model_splits_author_and_slug_exactly() -> None:
    sdk = FakeOpenRouterSdk(model_response=default_model_response(model_id=MODEL_ID))
    client = _client(sdk)
    client.inspect_model(ModelInspectionRequest(model_id="acme-corp/model.v2-beta"))
    call = sdk.models_get_calls[0]
    assert call["author"] == "acme-corp"
    assert call["slug"] == "model.v2-beta"


def test_inspect_model_extracts_capabilities_and_exact_pricing() -> None:
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(
            model_id=MODEL_ID,
            prompt_price="0.00000015",
            completion_price="0.0000006",
        )
    )
    clock = FakeClock()
    client = _client(sdk, clock=clock)
    original_get = sdk.models.get

    def _advance_during_get(**kwargs: Any) -> Any:
        clock.advance(0.125)
        return original_get(**kwargs)

    sdk.models.get = _advance_during_get  # type: ignore[method-assign]
    result = client.inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    assert result.configured_model_id == MODEL_ID
    assert result.resolved_model_id == MODEL_ID
    assert "structured_outputs" in result.supported_parameters
    assert "response_format" in result.supported_parameters
    assert result.supports_strict_structured_output is True
    assert result.prompt_unit_price_nano_usd == 150
    assert result.completion_unit_price_nano_usd == 600
    assert result.latency_ms == 125


def test_inspect_model_marks_missing_strict_support() -> None:
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(
            supported_parameters=["temperature", "max_tokens"]
        )
    )
    result = _client(sdk).inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    assert result.supports_strict_structured_output is False


def test_inspect_model_returns_none_prices_when_not_exactly_representable() -> None:
    # Eleven decimal places of USD cannot be an integer nano-USD amount.
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(
            prompt_price="0.00000000015",
            completion_price="0.0000006",
        )
    )
    result = _client(sdk).inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    assert result.prompt_unit_price_nano_usd is None
    assert result.completion_unit_price_nano_usd == 600


def test_inspect_model_returns_none_for_unparseable_price_strings() -> None:
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(
            prompt_price="not-a-number",
            completion_price="",
        )
    )
    result = _client(sdk).inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    assert result.prompt_unit_price_nano_usd is None
    assert result.completion_unit_price_nano_usd is None


def test_inspect_model_passes_disabled_retries_and_timeout() -> None:
    sdk = FakeOpenRouterSdk(model_response=default_model_response())
    _client(sdk, timeout_seconds=12.5).inspect_model(
        ModelInspectionRequest(model_id=MODEL_ID)
    )
    call = sdk.models_get_calls[0]
    retries = call["retries"]
    assert isinstance(retries, RetryConfig)
    assert retries.strategy == "none"
    assert call["timeout_ms"] == 12_500


# --- Generation request shape --------------------------------------------------


def test_generate_structured_uses_strict_json_schema_and_routing() -> None:
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    routing = ProviderRoutingConfig(
        allow_fallbacks=False, data_collection="deny", zdr=True
    )
    _client(sdk, routing=routing).generate_structured(_generation_request())
    call = sdk.chat_send_calls[0]
    assert call["model"] == MODEL_ID
    assert call.get("models") in (None, ...)
    assert "models" not in call or call["models"] is None
    assert call["stream"] is False
    assert call["max_completion_tokens"] == 1024
    assert "max_tokens" not in call or call["max_tokens"] is None
    assert call["temperature"] == 0.0
    assert call["top_p"] == 1.0
    assert call["x_open_router_metadata"] == "enabled"

    response_format = call["response_format"]
    assert response_format["type"] == "json_schema"
    schema_block = response_format["json_schema"]
    assert schema_block["name"] == "detect_people"
    assert schema_block["strict"] is True
    assert schema_block["schema"] == SCHEMA

    provider = call["provider"]
    assert provider["require_parameters"] is True
    assert provider["allow_fallbacks"] is False
    assert provider["data_collection"] == "deny"
    assert provider["zdr"] is True

    messages = list(call["messages"])
    assert messages[0]["role"] == "system"
    assert messages[0]["content"] == "You are a careful classifier."
    assert messages[1]["role"] == "user"
    assert messages[1]["content"] == '{"source_item_id": 1}'


def test_generate_structured_pins_same_configured_model() -> None:
    sdk = FakeOpenRouterSdk(
        chat_result=default_chat_result(model="openai/gpt-test-served")
    )
    result = _client(sdk).generate_structured(
        _generation_request(model_id="anthropic/claude-test")
    )
    assert sdk.chat_send_calls[0]["model"] == "anthropic/claude-test"
    assert result.configured_model_id == "anthropic/claude-test"
    assert result.resolved_model_id == "openai/gpt-test-served"


def test_generate_structured_captures_usage_cost_provider_and_finish() -> None:
    sdk = FakeOpenRouterSdk(
        chat_result=default_chat_result(
            content='{"item_outcome":"research_people"}',
            cost=0.000012345,
            finish_reason="stop",
            refusal=None,
            serving_provider="Together",
            request_id="gen-abc-123",
        )
    )
    clock = FakeClock()
    client = _client(sdk, clock=clock)

    original_send = sdk.chat.send

    def _timed_send(**kwargs: Any) -> Any:
        clock.advance(0.25)
        return original_send(**kwargs)

    sdk.chat.send = _timed_send  # type: ignore[method-assign]
    result = client.generate_structured(_generation_request())
    assert result.raw_text == '{"item_outcome":"research_people"}'
    assert result.finish_reason == "stop"
    assert result.refusal is None
    assert result.provider_request_id == "gen-abc-123"
    assert result.serving_provider == "Together"
    assert result.usage is not None
    assert result.usage.prompt_tokens == 10
    assert result.usage.completion_tokens == 5
    assert result.usage.total_tokens == 15
    expected = int(
        (Decimal("0.000012345") * NANO_USD).to_integral_value(rounding=ROUND_HALF_UP)
    )
    assert result.actual_nano_usd == expected
    assert result.latency_ms == 250


def test_generate_structured_missing_cost_is_none_not_zero() -> None:
    result_payload = default_chat_result()
    result_payload.usage = FakeUsage(cost=None)
    sdk = FakeOpenRouterSdk(chat_result=result_payload)
    result = _client(sdk).generate_structured(_generation_request())
    assert result.actual_nano_usd is None


def test_generate_structured_preserves_refusal_and_finish_reason() -> None:
    sdk = FakeOpenRouterSdk(
        chat_result=default_chat_result(
            content="",
            refusal="I cannot help with that.",
            finish_reason="content_filter",
        )
    )
    result = _client(sdk).generate_structured(_generation_request())
    assert result.raw_text == ""
    assert result.refusal == "I cannot help with that."
    assert result.finish_reason == "content_filter"


def test_generate_structured_passes_reasoning_effort_when_configured() -> None:
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    _client(sdk).generate_structured(_generation_request(reasoning_effort="low"))
    assert sdk.chat_send_calls[0]["reasoning_effort"] == "low"


def test_generate_structured_omits_reasoning_effort_when_absent() -> None:
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    _client(sdk).generate_structured(_generation_request(reasoning_effort=None))
    call = sdk.chat_send_calls[0]
    assert call.get("reasoning_effort") is None


# --- Broken-variant pins -------------------------------------------------------


def test_per_call_retry_override_is_always_strategy_none() -> None:
    """Kills dropping the per-operation retries= override (SDK default re-enables)."""
    sdk = FakeOpenRouterSdk(
        model_response=default_model_response(),
        chat_result=default_chat_result(),
    )
    client = _client(sdk)
    client.inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    client.generate_structured(_generation_request())
    for call in (*sdk.models_get_calls, *sdk.chat_send_calls):
        assert "retries" in call
        retries = call["retries"]
        assert isinstance(retries, RetryConfig)
        assert retries.strategy == "none"
        assert retries.retry_connection_errors is False
        assert isinstance(retries.backoff, BackoffStrategy)


def test_response_healing_plugin_is_not_enabled() -> None:
    """Kills enabling OpenRouter response-healing plugin."""
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    _client(sdk).generate_structured(_generation_request())
    call = sdk.chat_send_calls[0]
    plugins = call.get("plugins")
    assert plugins in (None, (), [])
    if plugins:
        ids = {
            getattr(plugin, "id", None)
            if not isinstance(plugin, dict)
            else plugin.get("id")
            for plugin in plugins
        }
        assert "response-healing" not in ids


def test_require_parameters_is_always_true() -> None:
    """Kills omitting provider.require_parameters=True."""
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    _client(sdk).generate_structured(_generation_request())
    provider = sdk.chat_send_calls[0]["provider"]
    assert provider["require_parameters"] is True


def test_generation_does_not_pass_model_list_or_fallback_model() -> None:
    """Kills allowing models=[] fallback list selection."""
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result())
    _client(sdk).generate_structured(_generation_request())
    call = sdk.chat_send_calls[0]
    assert call["model"] == MODEL_ID
    assert call.get("models") is None


def test_sdk_response_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    """Kills logging raw SDK response bodies (may carry prompts/schema output)."""
    secretish = '{"secret_payload":"must-not-appear-in-logs"}'
    sdk = FakeOpenRouterSdk(chat_result=default_chat_result(content=secretish))
    with caplog.at_level(logging.DEBUG):
        result = _client(sdk).generate_structured(_generation_request())
    assert result.raw_text == secretish
    assert secretish not in caplog.text
    assert "must-not-appear-in-logs" not in caplog.text


# --- Exception translation -----------------------------------------------------


@pytest.mark.parametrize(
    ("error", "category", "status_code", "retryable"),
    [
        (NoResponseError("connection reset"), FailureCategory.NETWORK, None, True),
        (
            _http_error(408, message="request timeout"),
            FailureCategory.TIMEOUT,
            408,
            True,
        ),
        (
            _http_error(400, message="bad request"),
            FailureCategory.CONFIGURATION,
            400,
            False,
        ),
        (
            _http_error(401, message="unauthorized"),
            FailureCategory.AUTHENTICATION,
            401,
            False,
        ),
        (
            _http_error(402, message="payment required"),
            FailureCategory.BUDGET_EXHAUSTED,
            402,
            False,
        ),
        (
            _http_error(403, message="forbidden"),
            FailureCategory.ACCESS_DENIED,
            403,
            False,
        ),
        (
            _http_error(429, headers={"retry-after": "3"}, message="slow down"),
            FailureCategory.RATE_LIMIT,
            429,
            True,
        ),
        (
            _http_error(500, message="internal"),
            FailureCategory.TRANSIENT_SERVER_ERROR,
            500,
            True,
        ),
        (
            _http_error(502, message="bad gateway"),
            FailureCategory.PROVIDER_UNAVAILABLE,
            502,
            True,
        ),
        (
            _http_error(503, message="unavailable"),
            FailureCategory.PROVIDER_UNAVAILABLE,
            503,
            True,
        ),
    ],
)
def test_sdk_exceptions_translate_to_sanitized_provider_failure(
    error: BaseException,
    category: FailureCategory,
    status_code: int | None,
    retryable: bool,
) -> None:
    sdk = FakeOpenRouterSdk(chat_send_error=error)
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    failure = raised.value
    assert failure.category is category
    assert failure.provider == "openrouter"
    assert failure.operation == "generate_structured"
    assert failure.status_code == status_code
    assert failure.retryable is retryable
    # Detail is the type name only — never the body, message, or secret.
    assert failure.detail == type(error).__name__
    assert "sk-secret" not in str(failure)
    assert "upstream detail" not in str(failure)
    assert "provider said something sensitive" not in str(failure)
    if status_code == 429:
        assert failure.retry_after_ms == 3000


def test_http_504_is_timeout() -> None:
    sdk = FakeOpenRouterSdk(chat_send_error=_http_error(504, message="gateway timeout"))
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    assert raised.value.category is FailureCategory.TIMEOUT
    assert raised.value.status_code == 504


def test_httpx_timeout_is_timeout() -> None:
    sdk = FakeOpenRouterSdk(
        chat_send_error=httpx.ReadTimeout("read timed out", request=None)
    )
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    assert raised.value.category is FailureCategory.TIMEOUT
    assert raised.value.detail == "ReadTimeout"
    assert raised.value.retryable is True


def test_httpx_connect_error_is_network() -> None:
    sdk = FakeOpenRouterSdk(
        chat_send_error=httpx.ConnectError("connect failed", request=None)
    )
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    assert raised.value.category is FailureCategory.NETWORK
    assert raised.value.detail == "ConnectError"


def test_response_validation_error_is_malformed() -> None:
    request = httpx.Request("GET", "https://openrouter.ai/api/v1/model/a/b")
    response = httpx.Response(200, request=request, content=b"{}")
    error = ResponseValidationError(
        "type mismatch",
        response,
        ValueError("field required: choices with body fragment"),
        body='{"choices": "bad"}',
    )
    sdk = FakeOpenRouterSdk(models_get_error=error)
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).inspect_model(ModelInspectionRequest(model_id=MODEL_ID))
    failure = raised.value
    assert failure.category is FailureCategory.MALFORMED_RESPONSE
    assert failure.operation == "inspect_model"
    assert failure.detail == "ResponseValidationError"
    assert "field required" not in str(failure)
    assert "choices" not in (failure.detail or "")


def test_empty_choices_is_malformed_response() -> None:
    payload = FakeChatResult(choices=[], usage=FakeUsage(cost=None))
    sdk = FakeOpenRouterSdk(chat_result=payload)
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    failure = raised.value
    assert failure.category is FailureCategory.MALFORMED_RESPONSE
    assert failure.retryable is False
    assert failure.detail == "empty_choices"
    assert "{" not in (failure.detail or "")


def test_non_string_message_content_is_malformed() -> None:
    payload = default_chat_result()
    bad_message = type("M", (), {"content": {"a": 1}, "refusal": None})()
    payload.choices = [FakeChoice(message=bad_message)]  # type: ignore[list-item]
    sdk = FakeOpenRouterSdk(chat_result=payload)
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    assert raised.value.category is FailureCategory.MALFORMED_RESPONSE
    assert raised.value.detail == "non_string_content"


def test_unexpected_exception_becomes_internal() -> None:
    sdk = FakeOpenRouterSdk(chat_send_error=RuntimeError("kaboom with key=sk-xyz"))
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    failure = raised.value
    assert failure.category is FailureCategory.INTERNAL
    assert failure.detail == "RuntimeError"
    assert "kaboom" not in str(failure)
    assert "sk-xyz" not in str(failure)


def test_provider_failure_is_not_re_wrapped() -> None:
    original = ProviderFailure(
        FailureCategory.RATE_LIMIT,
        provider="openrouter",
        operation="generate_structured",
        status_code=429,
        detail="TooManyRequestsResponseError",
    )
    sdk = FakeOpenRouterSdk(chat_send_error=original)
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).generate_structured(_generation_request())
    assert raised.value is original


def test_inspect_model_rejects_model_id_without_slash() -> None:
    sdk = FakeOpenRouterSdk(model_response=default_model_response())
    with pytest.raises(ProviderFailure) as raised:
        _client(sdk).inspect_model(ModelInspectionRequest(model_id="not-a-slug"))
    assert raised.value.category is FailureCategory.CONFIGURATION
    assert raised.value.detail == "invalid_model_id"
    assert sdk.models_get_calls == []
