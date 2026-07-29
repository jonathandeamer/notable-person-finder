"""Narrow OpenRouter SDK translation adapter.

This module is a *translator*, not a judge. It converts application-owned
inspection and structured-generation requests into the official OpenRouter
Python SDK's ``models.get`` / ``chat.send`` calls, and converts SDK values and
exceptions into frozen application DTOs and sanitized ``ProviderFailure``.

It deliberately does not:

* render task prompts or know ``detect_people`` (or any other task) semantics;
* validate domain references or structured domain output;
* decide retries, reserve budgets, or repair malformed JSON;
* log response bodies, messages, schemas, full endpoint queries, or API keys.

Only the central application retry coordinator starts another visible request.
SDK-level retries are constructed once with ``strategy="none"`` and passed at
both client construction and every per-operation call so a generated default
cannot re-enable them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from types import TracebackType
from typing import Any, Protocol

import httpx
from openrouter import OpenRouter
from openrouter.errors.no_response_error import NoResponseError
from openrouter.errors.openroutererror import OpenRouterError
from openrouter.errors.responsevalidationerror import ResponseValidationError
from openrouter.utils.retries import BackoffStrategy, RetryConfig

from notable_person_finder.config.models import NANO_USD, ProviderRoutingConfig
from notable_person_finder.providers.failures import (
    FailureCategory,
    ProviderFailure,
    parse_retry_after,
)
from notable_person_finder.runs.clock import Clock, SystemClock

PROVIDER = "openrouter"
INSPECT_OPERATION = "inspect_model"
GENERATE_OPERATION = "generate_structured"

# Parameters that together indicate strict structured-output support under the
# OpenRouter capability surface used by this adapter.
_STRICT_STRUCTURED_PARAMETERS = frozenset({"response_format", "structured_outputs"})

_EXACT_STATUS_CATEGORIES: Mapping[int, FailureCategory] = {
    400: FailureCategory.CONFIGURATION,
    401: FailureCategory.AUTHENTICATION,
    402: FailureCategory.BUDGET_EXHAUSTED,
    403: FailureCategory.ACCESS_DENIED,
    404: FailureCategory.CONFIGURATION,
    408: FailureCategory.TIMEOUT,
    413: FailureCategory.RESPONSE_TOO_LARGE,
    422: FailureCategory.CONFIGURATION,
    429: FailureCategory.RATE_LIMIT,
    500: FailureCategory.TRANSIENT_SERVER_ERROR,
    502: FailureCategory.PROVIDER_UNAVAILABLE,
    503: FailureCategory.PROVIDER_UNAVAILABLE,
    504: FailureCategory.TIMEOUT,
    529: FailureCategory.PROVIDER_UNAVAILABLE,
}


def _disabled_retry_config() -> RetryConfig:
    """SDK retries off. ``strategy="none"`` still requires backoff fields."""
    return RetryConfig(
        strategy="none",
        backoff=BackoffStrategy(
            initial_interval=0,
            max_interval=0,
            exponent=1.0,
            max_elapsed_time=0,
        ),
        retry_connection_errors=False,
    )


@dataclass(frozen=True, slots=True)
class ModelInspectionRequest:
    model_id: str


@dataclass(frozen=True, slots=True)
class ModelInspectionResult:
    configured_model_id: str
    resolved_model_id: str
    supported_parameters: tuple[str, ...]
    supports_strict_structured_output: bool
    prompt_unit_price_nano_usd: int | None
    completion_unit_price_nano_usd: int | None
    latency_ms: int


@dataclass(frozen=True, slots=True)
class StructuredGenerationRequest:
    model_id: str
    system_prompt: str
    user_content: str
    json_schema: Mapping[str, Any]
    schema_name: str
    max_completion_tokens: int
    temperature: float
    top_p: float
    reasoning_effort: str | None = None


@dataclass(frozen=True, slots=True)
class TokenUsage:
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


@dataclass(frozen=True, slots=True)
class StructuredGenerationResult:
    raw_text: str
    configured_model_id: str
    resolved_model_id: str
    serving_provider: str | None
    finish_reason: str | None
    refusal: str | None
    usage: TokenUsage | None
    latency_ms: int
    provider_request_id: str | None
    actual_nano_usd: int | None


class LlmClient(Protocol):
    """The seam handlers depend on so OpenRouter can be faked in tests."""

    def inspect_model(
        self, request: ModelInspectionRequest
    ) -> ModelInspectionResult: ...

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult: ...


def _split_model_id(model_id: str, *, operation: str) -> tuple[str, str]:
    if "/" not in model_id or model_id.startswith("/") or model_id.endswith("/"):
        raise ProviderFailure(
            FailureCategory.CONFIGURATION,
            provider=PROVIDER,
            operation=operation,
            detail="invalid_model_id",
        )
    author, slug = model_id.split("/", 1)
    if not author or not slug or "/" in slug:
        # Config validation already forbids multi-segment slugs; reject here so
        # a hand-built request cannot widen the pin to a different model path.
        raise ProviderFailure(
            FailureCategory.CONFIGURATION,
            provider=PROVIDER,
            operation=operation,
            detail="invalid_model_id",
        )
    return author, slug


def _price_string_to_nano_usd(value: object) -> int | None:
    """Exact per-token nano-USD, or None when not representable without loss."""
    if not isinstance(value, str) or not value:
        return None
    try:
        amount = Decimal(value)
    except InvalidOperation:
        return None
    if amount < 0:
        return None
    nano = amount * Decimal(NANO_USD)
    if nano != nano.to_integral_value():
        return None
    return int(nano)


def _reported_cost_to_nano_usd(cost: object) -> int | None:
    """Convert a reported float cost via Decimal(str) and explicit rounding.

    Missing cost stays ``None`` (never invented as zero). A genuine reported
    zero remains zero.
    """
    if cost is None:
        return None
    try:
        amount = Decimal(str(cost))
    except (InvalidOperation, ValueError):
        return None
    if amount < 0:
        return None
    nano = (amount * Decimal(NANO_USD)).to_integral_value(rounding=ROUND_HALF_UP)
    return int(nano)


def _supported_parameters(raw: object) -> tuple[str, ...]:
    if not isinstance(raw, list | tuple):
        return ()
    return tuple(item for item in raw if isinstance(item, str))


def _supports_strict_structured_output(parameters: tuple[str, ...]) -> bool:
    present = set(parameters)
    return _STRICT_STRUCTURED_PARAMETERS.issubset(present)


def _provider_preferences(routing: ProviderRoutingConfig) -> dict[str, Any]:
    return {
        "allow_fallbacks": routing.allow_fallbacks,
        "data_collection": routing.data_collection,
        "zdr": routing.zdr,
        "require_parameters": True,
    }


def _serving_provider(result: object) -> str | None:
    meta = getattr(result, "openrouter_metadata", None)
    if meta is None:
        return None
    endpoints = getattr(meta, "endpoints", None)
    available = getattr(endpoints, "available", None) if endpoints is not None else None
    if isinstance(available, list | tuple):
        for endpoint in available:
            if getattr(endpoint, "selected", False):
                provider = getattr(endpoint, "provider", None)
                if isinstance(provider, str) and provider:
                    return provider
    attempts = getattr(meta, "attempts", None)
    if isinstance(attempts, list | tuple) and attempts:
        provider = getattr(attempts[-1], "provider", None)
        if isinstance(provider, str) and provider:
            return provider
    return None


def _message_text(message: object) -> str:
    content = getattr(message, "content", None)
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    raise ProviderFailure(
        FailureCategory.MALFORMED_RESPONSE,
        provider=PROVIDER,
        operation=GENERATE_OPERATION,
        detail="non_string_content",
    )


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _translate_failure(error: BaseException, *, operation: str) -> ProviderFailure:
    if isinstance(error, ProviderFailure):
        return error

    if isinstance(error, NoResponseError):
        return ProviderFailure(
            FailureCategory.NETWORK,
            provider=PROVIDER,
            operation=operation,
            detail=type(error).__name__,
        )

    if isinstance(error, httpx.TimeoutException):
        return ProviderFailure(
            FailureCategory.TIMEOUT,
            provider=PROVIDER,
            operation=operation,
            detail=type(error).__name__,
        )

    if isinstance(error, httpx.TransportError):
        return ProviderFailure(
            FailureCategory.NETWORK,
            provider=PROVIDER,
            operation=operation,
            detail=type(error).__name__,
        )

    if isinstance(error, ResponseValidationError):
        return ProviderFailure(
            FailureCategory.MALFORMED_RESPONSE,
            provider=PROVIDER,
            operation=operation,
            status_code=getattr(error, "status_code", None),
            detail=type(error).__name__,
        )

    if isinstance(error, OpenRouterError):
        status = error.status_code
        category = _EXACT_STATUS_CATEGORIES.get(status)
        if category is None:
            if 500 <= status <= 599:
                category = FailureCategory.TRANSIENT_SERVER_ERROR
            elif 400 <= status <= 499:
                category = FailureCategory.CONFIGURATION
            else:
                category = FailureCategory.PROVIDER_UNAVAILABLE
        retry_after_ms = None
        headers = getattr(error, "headers", None)
        if headers is not None:
            retry_after_ms = parse_retry_after(
                headers.get("retry-after"),
                now=datetime.now(UTC),
            )
        return ProviderFailure(
            category,
            provider=PROVIDER,
            operation=operation,
            status_code=status,
            retry_after_ms=retry_after_ms,
            detail=type(error).__name__,
        )

    return ProviderFailure(
        FailureCategory.INTERNAL,
        provider=PROVIDER,
        operation=operation,
        detail=type(error).__name__,
    )


class OpenRouterClient:
    """One run-scoped OpenRouter SDK wrapper implementing ``LlmClient``."""

    def __init__(
        self,
        *,
        api_key: str,
        endpoint: str = "https://openrouter.ai/api/v1",
        routing: ProviderRoutingConfig | None = None,
        timeout_seconds: float = 300.0,
        http_referer: str | None = None,
        app_title: str | None = None,
        sdk: Any | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._routing = routing if routing is not None else ProviderRoutingConfig()
        self._timeout_ms = int(timeout_seconds * 1000)
        self._retry = _disabled_retry_config()
        self._clock = clock if clock is not None else SystemClock()
        self._owns_sdk = sdk is None
        if sdk is None:
            self._sdk = OpenRouter(
                api_key=api_key,
                server_url=endpoint,
                http_referer=http_referer,
                x_open_router_title=app_title,
                retry_config=self._retry,
                timeout_ms=self._timeout_ms,
                debug_logger=None,
            )
        else:
            self._sdk = sdk

    def __enter__(self) -> OpenRouterClient:
        enter = getattr(self._sdk, "__enter__", None)
        if enter is not None:
            enter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        exit_fn = getattr(self._sdk, "__exit__", None)
        if exit_fn is not None:
            exit_fn(exc_type, exc, tb)
            return None
        if self._owns_sdk:
            close = getattr(self._sdk, "close", None)
            if close is not None:
                close()
        return None

    def inspect_model(self, request: ModelInspectionRequest) -> ModelInspectionResult:
        operation = INSPECT_OPERATION
        author, slug = _split_model_id(request.model_id, operation=operation)
        started = self._clock.monotonic()
        try:
            response = self._sdk.models.get(
                author=author,
                slug=slug,
                retries=self._retry,
                timeout_ms=self._timeout_ms,
            )
            data = getattr(response, "data", None)
            if data is None:
                raise ProviderFailure(
                    FailureCategory.MALFORMED_RESPONSE,
                    provider=PROVIDER,
                    operation=operation,
                    detail="missing_model_data",
                )
            parameters = _supported_parameters(
                getattr(data, "supported_parameters", ())
            )
            pricing = getattr(data, "pricing", None)
            prompt_price = (
                _price_string_to_nano_usd(getattr(pricing, "prompt", None))
                if pricing is not None
                else None
            )
            completion_price = (
                _price_string_to_nano_usd(getattr(pricing, "completion", None))
                if pricing is not None
                else None
            )
            resolved = getattr(data, "id", None) or getattr(
                data, "canonical_slug", None
            )
            if not isinstance(resolved, str) or not resolved:
                resolved = request.model_id
            latency_ms = int((self._clock.monotonic() - started) * 1000)
            return ModelInspectionResult(
                configured_model_id=request.model_id,
                resolved_model_id=resolved,
                supported_parameters=parameters,
                supports_strict_structured_output=_supports_strict_structured_output(
                    parameters
                ),
                prompt_unit_price_nano_usd=prompt_price,
                completion_unit_price_nano_usd=completion_price,
                latency_ms=latency_ms,
            )
        except ProviderFailure:
            raise
        except Exception as error:
            raise _translate_failure(error, operation=operation) from error

    def generate_structured(
        self, request: StructuredGenerationRequest
    ) -> StructuredGenerationResult:
        operation = GENERATE_OPERATION
        # Exact configured model only — never a models=[] fallback list.
        _split_model_id(request.model_id, operation=operation)
        started = self._clock.monotonic()
        kwargs: dict[str, Any] = {
            "messages": [
                {"role": "system", "content": request.system_prompt},
                {"role": "user", "content": request.user_content},
            ],
            "model": request.model_id,
            "models": None,
            "max_completion_tokens": request.max_completion_tokens,
            "temperature": request.temperature,
            "top_p": request.top_p,
            "stream": False,
            "plugins": None,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": request.schema_name,
                    "schema": dict(request.json_schema),
                    "strict": True,
                },
            },
            "provider": _provider_preferences(self._routing),
            "x_open_router_metadata": "enabled",
            "retries": self._retry,
            "timeout_ms": self._timeout_ms,
            "reasoning_effort": request.reasoning_effort,
        }
        try:
            response = self._sdk.chat.send(**kwargs)
            choices = getattr(response, "choices", None)
            if not isinstance(choices, list | tuple) or not choices:
                raise ProviderFailure(
                    FailureCategory.MALFORMED_RESPONSE,
                    provider=PROVIDER,
                    operation=operation,
                    detail="empty_choices",
                )
            choice = choices[0]
            message = getattr(choice, "message", None)
            if message is None:
                raise ProviderFailure(
                    FailureCategory.MALFORMED_RESPONSE,
                    provider=PROVIDER,
                    operation=operation,
                    detail="missing_message",
                )
            raw_text = _message_text(message)
            refusal = getattr(message, "refusal", None)
            if refusal is not None and not isinstance(refusal, str):
                refusal = None
            finish_reason = _optional_str(getattr(choice, "finish_reason", None))
            usage_raw = getattr(response, "usage", None)
            usage: TokenUsage | None = None
            actual_nano_usd: int | None = None
            if usage_raw is not None:
                usage = TokenUsage(
                    prompt_tokens=int(getattr(usage_raw, "prompt_tokens", 0) or 0),
                    completion_tokens=int(
                        getattr(usage_raw, "completion_tokens", 0) or 0
                    ),
                    total_tokens=int(getattr(usage_raw, "total_tokens", 0) or 0),
                )
                actual_nano_usd = _reported_cost_to_nano_usd(
                    getattr(usage_raw, "cost", None)
                )
            resolved = getattr(response, "model", None)
            if not isinstance(resolved, str) or not resolved:
                resolved = request.model_id
            request_id = getattr(response, "id", None)
            if not isinstance(request_id, str):
                request_id = None
            latency_ms = int((self._clock.monotonic() - started) * 1000)
            return StructuredGenerationResult(
                raw_text=raw_text,
                configured_model_id=request.model_id,
                resolved_model_id=resolved,
                serving_provider=_serving_provider(response),
                finish_reason=finish_reason,
                refusal=refusal,
                usage=usage,
                latency_ms=latency_ms,
                provider_request_id=request_id,
                actual_nano_usd=actual_nano_usd,
            )
        except ProviderFailure:
            raise
        except Exception as error:
            raise _translate_failure(error, operation=operation) from error
