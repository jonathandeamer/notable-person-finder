from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from enum import StrEnum


class FailureCategory(StrEnum):
    """The compact vocabulary every adapter translates its errors into."""

    NETWORK = "network"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    TRANSIENT_SERVER_ERROR = "transient_server_error"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    AUTHENTICATION = "authentication"
    CONFIGURATION = "configuration"
    UNSUPPORTED_CAPABILITY = "unsupported_capability"
    ACCESS_DENIED = "access_denied"
    UNSUPPORTED_CONTENT = "unsupported_content"
    RESPONSE_TOO_LARGE = "response_too_large"
    MALFORMED_RESPONSE = "malformed_response"
    BUDGET_EXHAUSTED = "budget_exhausted"
    STORAGE = "storage"
    INTERNAL = "internal"


RETRYABLE_CATEGORIES = frozenset(
    {
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    }
)


class ProviderFailure(Exception):
    """Operational breakage at a provider boundary.

    `detail` must already be sanitized by the raising adapter: it never
    contains credentials, authorization headers, response bodies, or full
    query text.
    """

    def __init__(
        self,
        category: FailureCategory,
        *,
        provider: str,
        operation: str,
        retryable: bool | None = None,
        status_code: int | None = None,
        retry_after_ms: int | None = None,
        detail: str | None = None,
    ) -> None:
        self.category = category
        self.provider = provider
        self.operation = operation
        self.retryable = (
            category in RETRYABLE_CATEGORIES if retryable is None else retryable
        )
        self.status_code = status_code
        self.retry_after_ms = retry_after_ms
        self.detail = detail
        status = f" (HTTP {status_code})" if status_code is not None else ""
        suffix = f": {detail}" if detail else ""
        super().__init__(f"{provider}.{operation} failed: {category}{status}{suffix}")


class ProviderPaused(Exception):
    """Raised when a provider is paused for the rest of the run."""

    def __init__(self, provider: str, *, consecutive_exhaustions: int) -> None:
        self.provider = provider
        self.consecutive_exhaustions = consecutive_exhaustions
        super().__init__(
            f"{provider} is paused for this run after "
            f"{consecutive_exhaustions} consecutive exhausted failures"
        )


def parse_retry_after(value: str | None, *, now: datetime) -> int | None:
    """Translate a `Retry-After` header into milliseconds, or None if unusable."""
    if not value:
        return None
    text = value.strip()
    if text.lstrip("+").isdigit():
        return int(text) * 1000
    try:
        target = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if target.tzinfo is None:
        target = target.replace(tzinfo=UTC)
    delta_ms = int((target - now).total_seconds() * 1000)
    return max(delta_ms, 0)
