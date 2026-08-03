import json
from decimal import Decimal

import httpx
import pytest

from notable.cache import Cache
from notable.config import OpenRouterConfig, TransportConfig
from notable.errors import BudgetExceeded, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient

SCHEMA = {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
TRANSPORT_CONFIG = TransportConfig(
    contact_url="https://e.test/c", per_host_min_interval_ms=0, initial_backoff_seconds=0
)


def _reply(content: dict, cost: str = "0.01", finish: str = "stop") -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "choices": [
                {"message": {"content": json.dumps(content)}, "finish_reason": finish}
            ],
            "usage": {"cost": cost},
        },
    )


def _client(tmp_path, handler, budget=None):
    http = httpx.Client(transport=httpx.MockTransport(handler))
    transport = Transport(
        TRANSPORT_CONFIG, Cache(tmp_path), client=http, sleep=lambda _s: None
    )
    return LlmClient(
        transport, OpenRouterConfig(), api_key="sk-secret", budget_usd=budget
    )


def _call(client, n: int = 1):
    """`n` varies the payload so successive calls are distinct cache keys."""
    return client.structured(
        task="detect_people",
        model="m",
        system="s",
        user_payload={"a": n},
        schema=SCHEMA,
        max_completion_tokens=256,
        reasoning_effort="low",
        timeout=5.0,
    )


def test_returns_parsed_content(tmp_path):
    assert _call(_client(tmp_path, lambda r: _reply({"ok": True}))) == {"ok": True}


def test_accumulates_actual_cost(tmp_path):
    client = _client(tmp_path, lambda r: _reply({"ok": True}, cost="0.0125"))
    _call(client)
    assert client.spend() == Decimal("0.0125")


def test_request_carries_strict_schema_and_no_temperature(tmp_path):
    captured = {}

    def handler(request):
        captured.update(json.loads(request.content))
        return _reply({"ok": True})

    _call(_client(tmp_path, handler))
    fmt = captured["response_format"]
    assert fmt["type"] == "json_schema"
    assert fmt["json_schema"]["strict"] is True
    assert fmt["json_schema"]["schema"] == SCHEMA
    # Reasoning models' endpoints declare neither, and require_parameters
    # excludes every endpoint missing a supplied parameter (HTTP 404).
    assert "temperature" not in captured
    assert "top_p" not in captured


def test_request_uses_the_wire_fields_the_frozen_implementation_proved(tmp_path):
    # Both assertions are load-bearing and neither is stylistic. See the task
    # preamble: refactor/rearchitecture:providers/openrouter.py lines 458, 218.
    captured = {}

    def handler(request):
        captured.update(json.loads(request.content))
        return _reply({"ok": True})

    _call(_client(tmp_path, handler))
    assert captured["max_completion_tokens"] == 256
    assert "max_tokens" not in captured
    # Without this, the router may pick an endpoint that ignores the strict
    # schema -- and the omission of temperature/top_p above stops meaning
    # anything.
    assert captured["provider"]["require_parameters"] is True


def test_a_truncated_response_is_named_as_truncation(tmp_path):
    # findings.md's most expensive defect. Shipped at 1024 against max_people
    # 8, responses were cut off mid-string and rejected as "malformed" --
    # which sent the diagnosis after the model instead of the token budget.
    handler = lambda r: _reply({"ok": True}, finish="length")
    with pytest.raises(ProviderFailure, match="truncat"):
        _call(_client(tmp_path, handler))


def test_a_missing_finish_reason_is_a_failure(tmp_path):
    # Absence of completion evidence is not evidence of completion, and the
    # cost of guessing wrong is a permanently cached partial response.
    handler = lambda r: httpx.Response(
        200,
        json={
            "choices": [{"message": {"content": '{"ok": true}'}}],
            "usage": {"cost": "0.01"},
        },
    )
    with pytest.raises(ProviderFailure, match="finish_reason"):
        _call(_client(tmp_path, handler))


def test_cost_is_recorded_when_the_response_envelope_is_unreadable(tmp_path):
    # OpenRouter can bill a request even when the response is missing choices.
    # Spend must be counted before choice/content validation or the cap is
    # silently undercounted.
    handler = lambda r: httpx.Response(
        200, json={"usage": {"cost": "0.30"}, "choices": []}
    )
    client = _client(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        _call(client)
    assert client.spend() == Decimal("0.30")


def test_a_provider_error_finish_reason_is_a_failure(tmp_path):
    # OpenRouter reports upstream errors as finish_reason "error" with partial
    # content. That content can parse and pass the domain rules, and would
    # then be cached permanently as a success.
    handler = lambda r: _reply({"ok": True}, finish="error")
    with pytest.raises(ProviderFailure, match="finish_reason"):
        _call(_client(tmp_path, handler))


def test_an_error_completion_is_not_cached(tmp_path):
    replies = [_reply({"ok": True}, finish="error"), _reply({"ok": True})]
    calls = []

    def handler(request):
        calls.append(request)
        return replies.pop(0)

    client = _client(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        _call(client, 1)
    assert _call(client, 1) == {"ok": True}
    assert len(calls) == 2


@pytest.mark.parametrize("cost", ["-0.50", "NaN", "Infinity"])
def test_an_implausible_cost_is_refused(tmp_path, cost):
    # A negative cost refunds the run; a NaN makes every later
    # `spend >= budget` comparison false, so the cap stops binding silently.
    with pytest.raises(ProviderFailure, match="implausible"):
        _call(_client(tmp_path, lambda r: _reply({"ok": True}, cost=cost),
                      budget=Decimal("1.00")))


def test_a_truncated_response_is_counted_for_the_run_report(tmp_path):
    client = _client(tmp_path, lambda r: _reply({"ok": True}, finish="length"))
    with pytest.raises(ProviderFailure):
        _call(client)
    assert client.truncations == 1


def test_missing_cost_raises_when_a_cap_is_configured(tmp_path):
    # Treating absent cost as zero disables the cap for the rest of the run --
    # silently, and precisely when the cap is what is protecting the spend.
    handler = lambda r: httpx.Response(
        200,
        json={
            "choices": [
                {"message": {"content": "{}"}, "finish_reason": "stop"}
            ],
            "usage": {},
        },
    )
    with pytest.raises(ProviderFailure, match="cost"):
        _call(_client(tmp_path, handler, budget=Decimal("1.00")))


def test_missing_cost_only_warns_when_there_is_no_cap(tmp_path, caplog):
    handler = lambda r: httpx.Response(
        200,
        json={
            "choices": [
                {"message": {"content": '{"ok": true}'}, "finish_reason": "stop"}
            ],
            "usage": {},
        },
    )
    assert _call(_client(tmp_path, handler)) == {"ok": True}
    assert "cost" in caplog.text


def test_cost_is_recorded_even_when_the_content_is_unusable(tmp_path):
    # The call was billed whether or not its output parsed. Recording cost
    # only on the success path undercounts a run made of failures.
    handler = lambda r: httpx.Response(
        200,
        json={
            "choices": [
                {"message": {"content": "{not json"}, "finish_reason": "stop"}
            ],
            "usage": {"cost": "0.30"},
        },
    )
    client = _client(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        _call(client)
    assert client.spend() == Decimal("0.30")


def test_an_invalid_response_is_not_cached_and_the_retry_calls_again(tmp_path):
    # The whole point of the deferred cache write. Without it a malformed 200
    # is a permanent cache hit: re-rejected on every retry, never re-requested,
    # and then recorded into the fixture later phases are graded against.
    replies = [
        httpx.Response(
            200,
            json={
                "choices": [
                    {"message": {"content": "{not json"}, "finish_reason": "stop"}
                ],
                "usage": {"cost": "0.01"},
            },
        ),
        _reply({"ok": True}),
    ]
    calls = []

    def handler(request):
        calls.append(request)
        return replies.pop(0)

    client = _client(tmp_path, handler)
    with pytest.raises(ProviderFailure):
        _call(client, 1)
    assert _call(client, 1) == {"ok": True}, "identical payload must be re-requested"
    assert len(calls) == 2


def test_a_response_rejected_by_the_domain_validator_is_not_cached(tmp_path):
    # Domain validation lives downstream in detect_contract.py, so `structured`
    # takes the validator rather than assuming JSON-parseable means valid.
    calls = []

    def handler(request):
        calls.append(request)
        return _reply({"ok": True})

    def reject(_parsed):
        raise ValueError("domain says no")

    client = _client(tmp_path, handler)
    with pytest.raises(ValueError):
        client.structured(
            task="detect_people", model="m", system="s", user_payload={"a": 1},
            schema=SCHEMA, max_completion_tokens=256, reasoning_effort="low",
            timeout=5.0, validate=reject,
        )
    with pytest.raises(ValueError):
        client.structured(
            task="detect_people", model="m", system="s", user_payload={"a": 1},
            schema=SCHEMA, max_completion_tokens=256, reasoning_effort="low",
            timeout=5.0, validate=reject,
        )
    assert len(calls) == 2, "a domain rejection must not become a permanent cache hit"


def test_api_key_is_sent_but_never_reaches_the_cache(tmp_path):
    seen = {}

    def handler(request):
        seen.update(request.headers)
        return _reply({"ok": True})

    _call(_client(tmp_path, handler))
    assert seen["authorization"] == "Bearer sk-secret"
    for path in tmp_path.rglob("*.json"):
        assert "sk-secret" not in path.read_text("utf-8")


def test_budget_is_checked_before_the_next_call(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return _reply({"ok": True}, cost="0.60")

    client = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(client, 1)  # 0.60, under the cap
    _call(client, 2)  # 1.20, over -- but this call was already permitted
    with pytest.raises(BudgetExceeded):
        _call(client, 3)
    assert len(calls) == 2, "no call is made once the cap is known to be passed"
    assert client.spend() == Decimal("1.20"), "soft cap: one-call overshoot is accepted"


def test_a_cache_hit_does_not_add_to_spend(tmp_path):
    # Replaying a crashed run must be free. Charging for cache hits would
    # report money never spent and could trip the cap during a free replay.
    client = _client(tmp_path, lambda r: _reply({"ok": True}, cost="0.50"))
    _call(client, 1)
    _call(client, 1)  # identical payload -> cache hit
    assert client.spend() == Decimal("0.50")


def test_a_fully_replayed_run_never_exceeds_the_budget(tmp_path):
    handler = lambda r: _reply({"ok": True}, cost="0.90")
    first = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(first, 1)
    _call(first, 2)
    # Same cache directory, fresh client: every call replays for free.
    second = _client(tmp_path, handler, budget=Decimal("1.00"))
    _call(second, 1)
    _call(second, 2)
    assert second.spend() == Decimal("0")


def test_malformed_json_content_is_a_provider_failure(tmp_path):
    handler = lambda r: httpx.Response(
        200, json={"choices": [{"message": {"content": "{not json"}}], "usage": {}}
    )
    with pytest.raises(ProviderFailure):
        _call(_client(tmp_path, handler))


def test_repeated_identical_call_is_served_from_cache(tmp_path):
    calls = []

    def handler(request):
        calls.append(request)
        return _reply({"ok": True})

    client = _client(tmp_path, handler)
    _call(client)
    _call(client)
    assert len(calls) == 1
