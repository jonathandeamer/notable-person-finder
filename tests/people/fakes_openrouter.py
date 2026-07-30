"""Fake OpenRouter SDK surfaces for offline adapter contract tests.

The real SDK is not imported here. Attribute shapes mirror only what
``providers.openrouter`` reads, so the adapter can be exercised without a
network and without coupling tests to generated Pydantic models.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FakePricing:
    prompt: str
    completion: str


@dataclass
class FakeModelData:
    id: str
    canonical_slug: str
    supported_parameters: list[str]
    pricing: FakePricing


@dataclass
class FakeModelResponse:
    data: FakeModelData


@dataclass
class FakeUsage:
    prompt_tokens: int = 10
    completion_tokens: int = 5
    total_tokens: int = 15
    cost: float | None = 0.000012


@dataclass
class FakeAssistantMessage:
    role: str = "assistant"
    content: str | None = '{"ok": true}'
    refusal: str | None = None


@dataclass
class FakeChoice:
    finish_reason: str | None = "stop"
    index: int = 0
    message: FakeAssistantMessage = field(default_factory=FakeAssistantMessage)


@dataclass
class FakeEndpointInfo:
    model: str = "openai/gpt-test"
    provider: str = "OpenAI"
    selected: bool = True


@dataclass
class FakeEndpointsMetadata:
    available: list[FakeEndpointInfo] = field(
        default_factory=lambda: [FakeEndpointInfo()]
    )
    total: int = 1


@dataclass
class FakeOpenRouterMetadata:
    attempt: int = 1
    endpoints: FakeEndpointsMetadata = field(default_factory=FakeEndpointsMetadata)
    is_byok: bool = False
    region: str | None = None
    requested: str = "openai/gpt-test"
    strategy: str = "direct"
    summary: str = "ok"
    attempts: list[Any] | None = None


@dataclass
class FakeChatResult:
    choices: list[FakeChoice] = field(default_factory=lambda: [FakeChoice()])
    created: int = 0
    id: str = "gen-test-request-id"
    model: str = "openai/gpt-test"
    object: str = "chat.completion"
    system_fingerprint: str | None = None
    openrouter_metadata: FakeOpenRouterMetadata | None = field(
        default_factory=FakeOpenRouterMetadata
    )
    service_tier: str | None = None
    usage: FakeUsage | None = field(default_factory=FakeUsage)


class FakeModelsApi:
    def __init__(self, owner: FakeOpenRouterSdk) -> None:
        self._owner = owner

    def get(self, **kwargs: Any) -> FakeModelResponse:
        self._owner.models_get_calls.append(dict(kwargs))
        if self._owner.models_get_error is not None:
            raise self._owner.models_get_error
        if self._owner.model_response is None:
            raise RuntimeError("FakeOpenRouterSdk.model_response is not configured")
        return self._owner.model_response


class FakeChatApi:
    def __init__(self, owner: FakeOpenRouterSdk) -> None:
        self._owner = owner

    def send(self, **kwargs: Any) -> FakeChatResult:
        self._owner.chat_send_calls.append(dict(kwargs))
        if self._owner.chat_send_error is not None:
            raise self._owner.chat_send_error
        if self._owner.chat_result is None:
            raise RuntimeError("FakeOpenRouterSdk.chat_result is not configured")
        return self._owner.chat_result


class FakeOpenRouterSdk:
    """Stand-in for ``openrouter.OpenRouter`` used by ``OpenRouterClient``."""

    def __init__(
        self,
        *,
        model_response: FakeModelResponse | None = None,
        chat_result: FakeChatResult | None = None,
        models_get_error: BaseException | None = None,
        chat_send_error: BaseException | None = None,
    ) -> None:
        self.model_response = model_response
        self.chat_result = chat_result
        self.models_get_error = models_get_error
        self.chat_send_error = chat_send_error
        self.models_get_calls: list[dict[str, Any]] = []
        self.chat_send_calls: list[dict[str, Any]] = []
        self.entered = False
        self.exited = False
        self.exit_args: tuple[Any, ...] | None = None
        self.models = FakeModelsApi(self)
        self.chat = FakeChatApi(self)

    def __enter__(self) -> FakeOpenRouterSdk:
        self.entered = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: Any,
    ) -> None:
        self.exited = True
        self.exit_args = (exc_type, exc, tb)
        return None


def default_model_response(
    *,
    model_id: str = "openai/gpt-test",
    supported_parameters: list[str] | None = None,
    prompt_price: str = "0.00000015",
    completion_price: str = "0.0000006",
) -> FakeModelResponse:
    return FakeModelResponse(
        data=FakeModelData(
            id=model_id,
            canonical_slug=model_id,
            supported_parameters=supported_parameters
            or [
                "temperature",
                "top_p",
                "max_completion_tokens",
                "response_format",
                "structured_outputs",
            ],
            pricing=FakePricing(prompt=prompt_price, completion=completion_price),
        )
    )


def default_chat_result(
    *,
    content: str = '{"item_outcome": "uncertain"}',
    model: str = "openai/gpt-test",
    request_id: str = "gen-test-request-id",
    cost: float | None = 0.000012,
    finish_reason: str | None = "stop",
    refusal: str | None = None,
    serving_provider: str = "OpenAI",
) -> FakeChatResult:
    return FakeChatResult(
        choices=[
            FakeChoice(
                finish_reason=finish_reason,
                message=FakeAssistantMessage(content=content, refusal=refusal),
            )
        ],
        id=request_id,
        model=model,
        openrouter_metadata=FakeOpenRouterMetadata(
            requested=model,
            endpoints=FakeEndpointsMetadata(
                available=[
                    FakeEndpointInfo(
                        model=model, provider=serving_provider, selected=True
                    )
                ]
            ),
        ),
        usage=FakeUsage(cost=cost),
    )
