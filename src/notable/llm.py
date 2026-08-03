"""OpenRouter structured-output calls and the run's spend counter."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Any

from notable.config import OpenRouterConfig
from notable.errors import BudgetExceeded, ProviderFailure
from notable.http import Transport

logger = logging.getLogger(__name__)


class LlmClient:
    def __init__(
        self,
        transport: Transport,
        config: OpenRouterConfig,
        *,
        api_key: str,
        budget_usd: Decimal | None,
    ) -> None:
        self._transport = transport
        self._config = config
        self._api_key = api_key
        self._budget = budget_usd
        self._spend = Decimal("0")
        # Counters for the run report. A live gate that cannot say how many
        # calls it made or how many were truncated is not measuring anything.
        self.calls = 0
        self.truncations = 0

    def spend(self) -> Decimal:
        return self._spend

    def structured(
        self,
        *,
        task: str,
        model: str,
        system: str,
        user_payload: dict[str, Any],
        schema: dict[str, Any],
        max_completion_tokens: int,
        reasoning_effort: str | None,
        timeout: float,
        validate: Callable[[dict[str, Any]], Any] | None = None,
    ) -> dict[str, Any]:
        """One focused decision. Raises BudgetExceeded before spending past the cap.

        `validate` is the caller's domain validator. It runs before the
        response is committed to cache, so that a response the domain rejects
        is never stored and the next run genuinely re-requests it. Whatever it
        raises propagates unchanged.
        """
        if self._budget is not None and self._spend >= self._budget:
            raise BudgetExceeded(
                f"run spend {self._spend} reached the cap {self._budget}"
            )

        body: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": json.dumps(
                        user_payload,
                        sort_keys=True,
                        separators=(",", ":"),
                        ensure_ascii=False,
                    ),
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": task, "strict": True, "schema": schema},
            },
            # Not `max_tokens`. The frozen implementation sends this field, and
            # under require_parameters below the field name is what routing
            # filters on. See docs/findings.md on the completion budget.
            "max_completion_tokens": max_completion_tokens,
            "usage": {"include": True},
            "provider": {
                "allow_fallbacks": True,
                "data_collection": "deny",
                # Excludes any endpoint not declaring support for every
                # parameter sent. This is what makes the strict schema binding
                # rather than advisory -- and it is the reason temperature and
                # top_p are omitted rather than sent as null.
                "require_parameters": True,
            },
        }
        if reasoning_effort:
            body["reasoning"] = {"effort": reasoning_effort}

        response = self._transport.request(
            provider="openrouter",
            method="POST",
            url=f"{self._config.endpoint}/chat/completions",
            json_body=body,
            # A model's answer to a fixed prompt is treated as fixed -- once
            # it has been validated. `defer_cache` holds the write until then.
            ttl_seconds=None,
            timeout=timeout,
            # The model and schema must discriminate the key: the same prompt
            # under a different schema is a different call.
            extra_key={"model": model, "schema": schema, "task": task},
            auth_token=f"Bearer {self._api_key}",
            defer_cache=True,
        )
        if not response.from_cache:
            self.calls += 1

        # Recorded before any further check: the call was billed whether or not
        # its content turns out to be usable. A replayed call cost nothing, so
        # charging for it would report money never spent and could trip the cap
        # during a free crash replay.
        try:
            envelope = response.json()
        except (json.JSONDecodeError, TypeError) as error:
            raise ProviderFailure(
                f"unreadable OpenRouter envelope for {task}: {error}", permanent=False
            ) from error
        if not response.from_cache:
            if not isinstance(envelope, dict):
                raise ProviderFailure(
                    f"unreadable OpenRouter envelope for {task}: not an object",
                    permanent=False,
                )
            self._record_cost(envelope.get("usage") or {}, task=task)

        try:
            choice = envelope["choices"][0]
            content = choice["message"]["content"]
        except (KeyError, IndexError, TypeError) as error:
            raise ProviderFailure(
                f"unreadable OpenRouter envelope for {task}: {error}", permanent=False
            ) from error

        # Truncation is named directly rather than left to surface as a JSON
        # parse error. This is findings.md's costliest defect, and it is one
        # comparison.
        finish_reason = choice.get("finish_reason")
        if finish_reason == "length":
            self.truncations += 1
            raise ProviderFailure(
                f"{task} response was truncated at max_completion_tokens "
                f"({max_completion_tokens}); raise it together with max_people",
                permanent=False,
            )
        # Only a clean stop is evidence of completion, and only evidence of
        # completion may be cached permanently. OpenRouter reports upstream
        # provider errors as `finish_reason: "error"` with partial content
        # attached; that content can parse and can satisfy the domain rules.
        #
        # A missing reason fails too. An earlier draft allowed `None` on the
        # theory that some endpoint might omit it -- but that is a guess, and
        # the cost of being wrong is a permanently cached partial response.
        # If a real provider does omit it, the first live run says so loudly
        # and the exception gets made against evidence.
        if finish_reason != "stop":
            raise ProviderFailure(
                f"{task} did not complete: finish_reason={finish_reason!r}",
                permanent=False,
            )

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError as error:
            raise ProviderFailure(
                f"{task} returned content that is not JSON: {error}", permanent=True
            ) from error
        if not isinstance(parsed, dict):
            raise ProviderFailure(f"{task} returned a non-object", permanent=True)

        # The domain validator is the last gate. Only once it passes does the
        # response become a cacheable success.
        if validate is not None:
            validate(parsed)
        response.commit()
        return parsed

    def _record_cost(self, usage: dict[str, Any], *, task: str) -> None:
        """Missing cost is never invented as zero.

        Silently treating an absent or unparseable cost as zero disables the
        spend cap for the rest of the run. When a cap is configured that is a
        failure; with no cap there is nothing to protect, so it is a warning.
        """
        raw = usage.get("cost")
        error: str | None = None
        if raw is None:
            error = f"{task} response reported no usage.cost"
        else:
            try:
                amount = Decimal(str(raw))
            except (InvalidOperation, ValueError):
                error = f"{task} reported an unparseable usage.cost: {raw!r}"
            else:
                # `Decimal("NaN")` and `Decimal("-1")` both parse. A NaN in the
                # accumulator makes every later `spend >= budget` comparison
                # false, so the cap silently stops binding; a negative value
                # refunds the run. Neither is a cost.
                if not amount.is_finite() or amount < 0:
                    error = f"{task} reported an implausible usage.cost: {raw!r}"
                else:
                    self._spend += amount
        if error is None:
            return
        if self._budget is not None:
            raise ProviderFailure(
                f"{error}; the spend cap cannot be enforced", permanent=False
            )
        logger.warning("%s; run spend is an undercount", error)
