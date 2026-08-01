"""Installed-interface CLI test for aggregate_person_lead registration.

Mirrors the reviewed house style in ``tests/coverage/test_run_cli.py``'s
handler-registration test (itself modeled on the Wikipedia equivalent):
a structural assert on the handlers mapping actually passed to
``RunEngine.execute`` by ``command_run``, not a side-constructed builder.
Kills omitting ``AGGREGATE_PERSON_LEAD_TASK_TYPE`` from the CLI map.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.leads.service import AGGREGATE_PERSON_LEAD_TASK_TYPE
from tests.coverage.test_run_cli import _wire
from tests.people.test_run_cli import RESEARCH_FEED, _single_feed, write_people_graph


def test_command_run_registers_aggregate_person_lead_handler_on_engine_map(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from notable_person_finder.runs.engine import RunEngine

    captured: dict[str, object] = {}
    original_execute = RunEngine.execute

    def spy_execute(self: RunEngine, handlers: object, seed: object = None) -> object:
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
