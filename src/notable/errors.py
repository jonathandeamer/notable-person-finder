"""The three exceptions that cross module boundaries."""

from __future__ import annotations


class Incomplete(Exception):
    """Research for one item did not reach a terminal state.

    The item is not settled and retries on a later run, bounded by the attempt
    cap. Never convert this into a semantic outcome: doing so would report a
    provider failure as "we looked and found nothing".
    """


class ProviderFailure(Exception):
    """One external call failed."""

    def __init__(self, message: str, *, permanent: bool) -> None:
        super().__init__(message)
        self.permanent = permanent


class BudgetExceeded(Exception):
    """The run's accumulated model spend passed the configured cap."""
