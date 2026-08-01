"""Installed-interface CLI test for aggregate_person_lead registration.

Mirrors the reviewed house style in ``tests/coverage/test_run_cli.py``'s
handler-registration test (itself modeled on the Wikipedia equivalent):
a structural assert on the handlers mapping actually passed to
``RunEngine.execute`` by ``command_run``, not a side-constructed builder.
Kills omitting ``AGGREGATE_PERSON_LEAD_TASK_TYPE`` from the CLI map.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.db.connection import connect_database
from notable_person_finder.leads.service import AGGREGATE_PERSON_LEAD_TASK_TYPE
from notable_person_finder.runs import repository as runs_repository
from tests.coverage.test_run_cli import _sole_person_id, _wire
from tests.people.test_run_cli import RESEARCH_FEED, _single_feed, write_people_graph


def test_command_run_registers_aggregate_person_lead_handler_on_engine_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from notable_person_finder.runs.engine import RunEngine, RunReport, TaskHandler

    captured: dict[str, object] = {}
    original_execute = RunEngine.execute

    def spy_execute(
        self: RunEngine,
        handlers: Mapping[str, TaskHandler],
        *,
        seed: Callable[[int], None] | None = None,
    ) -> RunReport:
        assert isinstance(handlers, dict)
        captured["keys"] = frozenset(handlers.keys())
        return original_execute(self, handlers, seed=seed)

    monkeypatch.setattr(RunEngine, "execute", spy_execute)
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    keys = captured["keys"]
    assert isinstance(keys, frozenset)
    assert AGGREGATE_PERSON_LEAD_TASK_TYPE in keys


def test_aggregation_fires_in_same_run_coverage_settles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct regression test for the class of bug fixed this session in K5
    (see CLAUDE.md's 'Test Evidence' section), applied to this milestone's
    own scheduling hook. A single ``notable run`` invocation that completes
    a coverage plan (feed -> detect -> resolve -> Wikipedia -> coverage ->
    assess) must produce a ``lead_assessment`` row for that same ``run_id``,
    not merely on the next run -- proving
    ``_schedule_lead_aggregation_after_settled`` fires from the coverage-plan
    completion hook within the very run that completed coverage.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    person_id = _sole_person_id(config)

    database = config.parent / "portable" / "data" / "notable.sqlite3"
    connection = connect_database(database, readonly=True)
    try:
        run_record = runs_repository.latest_run(connection)
        assert run_record is not None
        coverage_completion_run_id = run_record.id

        lead_row = connection.execute(
            "SELECT run_id FROM lead_assessment WHERE person_id = ?",
            (person_id,),
        ).fetchone()
        assert lead_row is not None, (
            "expected a lead_assessment row for the person whose coverage "
            "plan completed this run"
        )
        assert lead_row["run_id"] == coverage_completion_run_id
    finally:
        connection.close()


def test_second_run_with_unchanged_evidence_still_reports_exit_ok(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Direct regression test for the required=True steady-state bug found
    in Task 16's verification pass: ``aggregate_person_lead``'s K6 prepare()
    refuses (raises) when a person's material fingerprint is unchanged since
    their last aggregation, and the run engine settles any raising prepare()
    as ``failed_permanent``. When the work item was registered
    ``required=True``, that made every steady-state second run (one where at
    least one person's evidence has not changed) report ``EXIT_PARTIAL``
    forever, because ``seed_lead_aggregation`` unconditionally reschedules
    the item every run and the refusal is expected, not a failure.
    ``aggregate_person_lead`` must be scheduled ``required=False`` so this
    expected local refusal does not gate ``RunState``/exit code. The first
    run aggregates fresh evidence for the person; the second run's
    fingerprint is unchanged (no new coverage evidence arrived), so its
    prepare() refuses -- but the run as a whole must still report
    ``EXIT_OK``.
    """
    config = write_people_graph(tmp_path, feeds=_single_feed())
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    person_id = _sole_person_id(config)

    database = config.parent / "portable" / "data" / "notable.sqlite3"
    connection = connect_database(database, readonly=True)
    try:
        first_lead_row = connection.execute(
            "SELECT id FROM lead_assessment WHERE person_id = ?",
            (person_id,),
        ).fetchone()
        assert first_lead_row is not None
    finally:
        connection.close()

    # Second run: no new feed content, no new coverage evidence -- the
    # person's material fingerprint is unchanged, so aggregate_person_lead's
    # prepare() will refuse (K6). The run overall must still be EXIT_OK.
    _wire(monkeypatch, rss=RESEARCH_FEED.encode())
    assert cli_main.command_run(config, verbose=False) == cli_main.EXIT_OK

    connection = connect_database(database, readonly=True)
    try:
        lead_count = connection.execute(
            "SELECT COUNT(*) FROM lead_assessment WHERE person_id = ?",
            (person_id,),
        ).fetchone()[0]
        assert lead_count == 1, (
            "expected no additional lead_assessment row from the refused "
            "second-run aggregation attempt"
        )
    finally:
        connection.close()
