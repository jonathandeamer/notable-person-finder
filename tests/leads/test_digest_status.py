"""Digest shortlist and queue-flow rendering, mirroring the milestone-5
test_digest_status.py fixture-with-distinct-values pattern so swapped-field
defects are caught.

Uses a local minimal RunReport helper rather than importing
tests.coverage.test_digest_status: tests/leads must not import across test
packages, and that module's actual helper (`_report`) takes no override
kwargs -- it is copied here verbatim rather than referenced.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from notable_person_finder.db.connection import connect_database
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.repository import (
    insert_lead_assessment,
    upsert_digest_queue,
)
from notable_person_finder.people.identity import insert_person, upsert_sourced_name
from notable_person_finder.reporting.digest import (
    QueueFlowSummary,
    ShortlistEntry,
    render_digest,
)
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState
from tests.ingestion.helpers import immediate, insert_configuration_snapshot, moment
from tests.people.test_run_cli import _single_feed, write_people_graph

NOW = moment()


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


# ---------------------------------------------------------------------------
# `notable status` prints digest-queue backlog-by-tier and oldest-pending
# lines when the leads schema is present (Task 13).
# ---------------------------------------------------------------------------


def _insert_run(connection: sqlite3.Connection, *, started_at: str) -> int:
    """Seed a `run` row at an explicit `started_at`; mirrors
    tests/coverage/test_digest_status.py's helper of the same name."""
    snapshot_id = insert_configuration_snapshot(connection)
    cursor = connection.execute(
        """
        INSERT INTO run (
            state, configuration_snapshot_id, timezone,
            window_start, window_end, started_at
        )
        VALUES ('running', ?, 'Europe/Paris', ?, ?, ?)
        """,
        (snapshot_id, started_at, started_at, started_at),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return int(cursor.lastrowid)


def _person(
    connection: sqlite3.Connection, *, run_id: int, name: str, fingerprint: str
) -> int:
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=fingerprint,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name=name,
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
    return person_id


def _queue_lead(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    tier: str,
    first_pending_at: str,
) -> None:
    lead_id = insert_lead_assessment(
        connection,
        person_id=person_id,
        run_id=run_id,
        outcome=LeadOutcome(
            outcome=tier,
            qualifying_domain_count=1 if tier == "promising_lead" else 0,
            qualifying_articles=(),
            contributing_signals=(),
            wikipedia_outcome="no_matching_page_found",
        ),
        lead_policy_fingerprint="a" * 64,
        ordering_factors_json="{}",
        decided_at=first_pending_at,
    )
    upsert_digest_queue(
        connection,
        person_id=person_id,
        status="pending",
        tier=tier,
        eligibility_reason="new",
        lead_assessment_id=lead_id,
        first_pending_at=first_pending_at,
        last_material_change_at=first_pending_at,
    )
    connection.commit()


def test_notable_status_prints_backlog_by_tier(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_file = write_people_graph(tmp_path, feeds=_single_feed())
    loaded = load_config(config_file, require_secrets=False)
    database = loaded.paths.database
    database.parent.mkdir(parents=True, exist_ok=True)

    connection = connect_database(database)
    try:
        apply_migrations(connection, database, loaded.paths.backups)
        run_id = _insert_run(connection, started_at=moment())

        # Distinct counts (2 promising vs. 1 possible) so a swapped-field
        # defect that transposed the two tier counts would be caught -- equal
        # counts would let that mutation survive undetected.
        promising_person_a = _person(
            connection, run_id=run_id, name="Backlog Promising A", fingerprint="1" * 64
        )
        _queue_lead(
            connection,
            person_id=promising_person_a,
            run_id=run_id,
            tier="promising_lead",
            first_pending_at="2026-07-28T00:00:00Z",
        )

        promising_person_b = _person(
            connection, run_id=run_id, name="Backlog Promising B", fingerprint="3" * 64
        )
        _queue_lead(
            connection,
            person_id=promising_person_b,
            run_id=run_id,
            tier="promising_lead",
            first_pending_at="2026-07-29T00:00:00Z",
        )

        possible_person = _person(
            connection, run_id=run_id, name="Backlog Possible", fingerprint="2" * 64
        )
        _queue_lead(
            connection,
            person_id=possible_person,
            run_id=run_id,
            tier="possible_lead",
            first_pending_at="2026-07-30T00:00:00Z",
        )
        connection.commit()
    finally:
        connection.close()

    assert cli_main.command_status(config_file) == cli_main.EXIT_OK
    output = capsys.readouterr().out
    assert "digest backlog: 2 promising_lead, 1 possible_lead" in output
    assert "oldest pending candidate: 2026-07-28T00:00:00Z" in output
