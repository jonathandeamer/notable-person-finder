"""Digest shortlist and queue-flow rendering, mirroring the milestone-5
test_digest_status.py fixture-with-distinct-values pattern so swapped-field
defects are caught.

Uses a local minimal RunReport helper rather than importing
tests.coverage.test_digest_status: tests/leads must not import across test
packages, and that module's actual helper (`_report`) takes no override
kwargs -- it is copied here verbatim rather than referenced.
"""

from __future__ import annotations

from notable_person_finder.reporting.digest import (
    QueueFlowSummary,
    ShortlistEntry,
    render_digest,
)
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState


def _report(run_id: int = 1) -> RunReport:
    return RunReport(
        run_id=run_id,
        state=RunState.COMPLETE,
        started_at="2026-07-30T00:00:00Z",
        finished_at="2026-07-30T00:01:00Z",
        timezone="UTC",
        window_start="2026-07-30T00:00:00Z",
        window_end="2026-07-31T00:00:00Z",
        counters=RunCounters(
            required_succeeded=1,
            required_pending=0,
            required_deferred=0,
            required_failed_permanent=0,
            optional_succeeded=0,
            optional_skipped=0,
            operational_failures=0,
        ),
        paused_providers=frozenset(),
        interrupted_runs=(),
        failure_categories={},
        budget_limit_nano_usd=None,
        budget_reserved_nano_usd=0,
        budget_actual_nano_usd=0,
        deferred_reasons={},
    )


def _build_report(**overrides: object) -> RunReport:
    return _report(**overrides)  # type: ignore[arg-type]


def test_shortlist_renders_ranked_entries_with_distinct_fields() -> None:
    report = _build_report()
    entries = [
        ShortlistEntry(
            person_id=1,
            display_name="Person One",
            outcome="promising_lead",
            eligibility_reason="new",
            wikipedia_outcome="no_matching_page_found",
            qualifying_domain_count=2,
            qualifying_sources=(
                (
                    "example.com",
                    "Title One",
                    "2026-07-01",
                    "article",
                    "significant",
                    "full",
                ),
            ),
            attention_signals=("Award nomination",),
            caution_signals=(),
            unresolved_issues=(),
        )
    ]
    output = render_digest(report, local_date="2026-08-01", shortlist_entries=entries)
    assert "Person One" in output
    assert "promising_lead" in output
    assert "example.com" in output
    assert "Award nomination" in output


def test_shortlist_placeholder_text_absent_when_entries_present() -> None:
    report = _build_report()
    entries = [
        ShortlistEntry(
            person_id=1,
            display_name="X",
            outcome="possible_lead",
            eligibility_reason="new",
            wikipedia_outcome="uncertain_identity",
            qualifying_domain_count=1,
            qualifying_sources=(),
            attention_signals=(),
            caution_signals=(),
            unresolved_issues=(),
        )
    ]
    output = render_digest(report, local_date="2026-08-01", shortlist_entries=entries)
    assert "No candidates met the shortlist criteria" not in output


def test_shortlist_placeholder_text_present_when_no_entries() -> None:
    report = _build_report()
    output = render_digest(report, local_date="2026-08-01", shortlist_entries=[])
    assert "No candidates met the shortlist criteria in this window." in output


def test_queue_flow_block_renders_distinct_counters() -> None:
    report = _build_report()
    flow = QueueFlowSummary(
        newly_queued=1,
        emitted=2,
        removed_matching_wikipedia=3,
        ending_backlog_promising=4,
        ending_backlog_possible=5,
        arrival_rate_7d=6.0,
        emission_rate_7d=7.0,
        net_queue_growth=8,
        oldest_pending_days=9,
        estimated_clear_days=None,
    )
    output = render_digest(report, local_date="2026-08-01", queue_flow=flow)
    for expected in ("1", "2", "3", "4", "5", "6.0", "7.0", "8", "9"):
        assert expected in output
    assert "insufficient history" in output.lower() or "not clearing" in output.lower()


def test_queue_flow_block_absent_by_default() -> None:
    report = _build_report()
    output = render_digest(report, local_date="2026-08-01")
    assert "### Digest queue" not in output


def test_queue_flow_insufficient_history_when_rates_none() -> None:
    report = _build_report()
    flow = QueueFlowSummary(
        newly_queued=0,
        emitted=0,
        removed_matching_wikipedia=0,
        ending_backlog_promising=0,
        ending_backlog_possible=0,
        arrival_rate_7d=None,
        emission_rate_7d=None,
        net_queue_growth=0,
        oldest_pending_days=None,
        estimated_clear_days=None,
    )
    output = render_digest(report, local_date="2026-08-01", queue_flow=flow)
    assert "insufficient history" in output
    assert "not clearing" in output
    assert "no pending entries" in output


def test_queue_flow_arrival_and_emission_lines_are_independent() -> None:
    """Only `arrival_rate_7d` is None; `emission_rate_7d` is a real number.

    A single shared "insufficient history" substring check cannot tell the
    two lines apart -- a broken `arrival` fallback that leaked `None` into
    its own line would still pass if the assertion only checked that the
    string appeared *somewhere* in the output, because the still-correct
    emission line contributes the same substring. Asserting each line's
    exact text closes that gap.
    """
    report = _build_report()
    flow = QueueFlowSummary(
        newly_queued=0,
        emitted=0,
        removed_matching_wikipedia=0,
        ending_backlog_promising=0,
        ending_backlog_possible=0,
        arrival_rate_7d=None,
        emission_rate_7d=3.5,
        net_queue_growth=0,
        oldest_pending_days=None,
        estimated_clear_days=None,
    )
    output = render_digest(report, local_date="2026-08-01", queue_flow=flow)
    lines = output.splitlines()
    assert "- 7-day arrival rate: insufficient history" in lines
    assert "- 7-day emission rate: 3.5/day" in lines
