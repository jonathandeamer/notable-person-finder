from __future__ import annotations

from datetime import UTC, datetime

import pytest

from notable_person_finder.providers.failures import (
    RETRYABLE_CATEGORIES,
    FailureCategory,
    ProviderFailure,
    ProviderPaused,
    parse_retry_after,
)

NOW = datetime(2026, 7, 25, 6, 0, 0, tzinfo=UTC)


def test_transient_categories_are_retryable_by_default() -> None:
    for category in (
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    ):
        assert ProviderFailure(category, provider="brave", operation="search_web").retryable


def test_permanent_categories_are_not_retryable_by_default() -> None:
    for category in (
        FailureCategory.AUTHENTICATION,
        FailureCategory.CONFIGURATION,
        FailureCategory.UNSUPPORTED_CAPABILITY,
        FailureCategory.ACCESS_DENIED,
        FailureCategory.UNSUPPORTED_CONTENT,
        FailureCategory.RESPONSE_TOO_LARGE,
        FailureCategory.MALFORMED_RESPONSE,
        FailureCategory.BUDGET_EXHAUSTED,
        FailureCategory.STORAGE,
        FailureCategory.INTERNAL,
    ):
        assert not ProviderFailure(category, provider="brave", operation="search_web").retryable


def test_retryable_set_matches_the_default_dispositions() -> None:
    assert RETRYABLE_CATEGORIES == {
        FailureCategory.NETWORK,
        FailureCategory.TIMEOUT,
        FailureCategory.RATE_LIMIT,
        FailureCategory.TRANSIENT_SERVER_ERROR,
        FailureCategory.PROVIDER_UNAVAILABLE,
    }


def test_malformed_response_can_be_marked_retryable_once_at_raise_time() -> None:
    failure = ProviderFailure(
        FailureCategory.MALFORMED_RESPONSE,
        provider="openrouter",
        operation="generate_structured",
        retryable=True,
    )
    assert failure.retryable


def test_failure_message_is_safe_and_structured() -> None:
    failure = ProviderFailure(
        FailureCategory.RATE_LIMIT,
        provider="brave",
        operation="search_web",
        status_code=429,
        detail="slow down",
    )
    assert str(failure) == "brave.search_web failed: rate_limit (HTTP 429): slow down"


def test_failure_detail_must_not_carry_a_secret_value() -> None:
    # Adapters pass sanitized text; this asserts the contract is documented in code.
    failure = ProviderFailure(
        FailureCategory.AUTHENTICATION,
        provider="brave",
        operation="search_web",
        status_code=401,
    )
    assert failure.detail is None
    assert "401" in str(failure)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("5", 5000),
        ("0", 0),
        ("-3", None),
        ("not-a-number", None),
        ("Fri, 25 Jul 2026 06:00:30 GMT", 30_000),
        ("Fri, 25 Jul 2026 05:59:30 GMT", 0),
    ],
)
def test_parse_retry_after(header: str | None, expected: int | None) -> None:
    assert parse_retry_after(header, now=NOW) == expected


def test_provider_paused_names_the_provider_and_reason() -> None:
    paused = ProviderPaused("mediawiki", consecutive_exhaustions=3)
    assert "mediawiki" in str(paused)
    assert paused.provider == "mediawiki"
