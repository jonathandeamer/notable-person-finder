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
