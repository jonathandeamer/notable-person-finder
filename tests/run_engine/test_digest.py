from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from notable_person_finder.config.models import DigestConfig
from notable_person_finder.reporting.digest import (
    DigestWriteError,
    render_digest,
    write_digest,
)
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunCounters, RunState

COUNTERS = RunCounters(
    required_succeeded=4,
    required_pending=0,
    required_deferred=0,
    required_failed_permanent=0,
    optional_succeeded=1,
    optional_skipped=0,
    operational_failures=0,
)


def report(state: RunState = RunState.COMPLETE, **overrides: object) -> RunReport:
    defaults: dict[str, object] = {
        "run_id": 42,
        "state": state,
        "started_at": "2026-07-25T06:00:00Z",
        "finished_at": "2026-07-25T06:04:00Z",
        "timezone": "Europe/Paris",
        "window_start": "2026-07-24T06:00:00Z",
        "window_end": "2026-07-25T06:00:00Z",
        "counters": COUNTERS,
        "paused_providers": frozenset(),
        "interrupted_runs": (),
        "failure_categories": {},
    }
    defaults.update(overrides)
    return RunReport(**defaults)  # type: ignore[arg-type]


def test_header_states_run_identity_window_and_timezone() -> None:
    markdown = render_digest(report(), local_date="2026-07-25")
    assert markdown.startswith("# Notable Person Finder — 2026-07-25\n")
    assert "run-42" in markdown
    assert "Europe/Paris" in markdown
    assert "2026-07-24T06:00:00Z" in markdown


def test_a_complete_empty_run_is_unambiguously_successful() -> None:
    empty = RunCounters(0, 0, 0, 0, 0, 0, 0)
    markdown = render_digest(report(counters=empty), local_date="2026-07-25")
    assert "**State:** complete" in markdown
    assert "No candidates met the shortlist criteria in this window." in markdown
    assert "fail" not in markdown.lower()


def test_a_partial_run_warns_prominently_and_names_the_audit_command() -> None:
    counters = RunCounters(2, 1, 3, 0, 0, 0, 5)
    markdown = render_digest(report(RunState.PARTIAL, counters=counters), local_date="2026-07-25")
    warning_line = markdown.splitlines()[2]
    assert "PARTIAL" in warning_line
    assert "notable audit run 42" in markdown


def test_a_failed_run_says_so_prominently() -> None:
    markdown = render_digest(report(RunState.FAILED), local_date="2026-07-25")
    assert "FAILED" in markdown.splitlines()[2]


def test_paused_providers_appear_in_the_summary() -> None:
    markdown = render_digest(
        report(RunState.PARTIAL, paused_providers=frozenset({"brave"})), local_date="2026-07-25"
    )
    assert "brave" in markdown


def test_failure_categories_are_summarized_by_safe_category() -> None:
    markdown = render_digest(
        report(failure_categories={"rate_limit": 3, "timeout": 1}), local_date="2026-07-25"
    )
    assert "rate_limit" in markdown
    assert "timeout" in markdown


def test_an_interrupted_predecessor_is_reported() -> None:
    markdown = render_digest(report(interrupted_runs=(41,)), local_date="2026-07-25")
    assert "run-41" in markdown


def test_digest_ends_with_exactly_one_trailing_newline() -> None:
    markdown = render_digest(report(), local_date="2026-07-25")
    assert markdown.endswith("\n")
    assert not markdown.endswith("\n\n")


def test_digest_is_written_with_the_dated_run_filename(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert record.path == tmp_path / "2026-07-25-run-42.md"
    assert record.path.read_text(encoding="utf-8") == record.markdown


def test_content_hash_matches_the_written_bytes(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert record.sha256 == hashlib.sha256(record.path.read_bytes()).hexdigest()


def test_latest_copy_is_written_by_default(tmp_path: Path) -> None:
    record = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    latest = tmp_path / "latest.md"
    assert latest.is_file()
    assert not latest.is_symlink()
    assert latest.read_text(encoding="utf-8") == record.markdown


def test_latest_copy_can_be_disabled(tmp_path: Path) -> None:
    write_digest(
        tmp_path, report(), local_date="2026-07-25", config=DigestConfig(write_latest_copy=False)
    )
    assert not (tmp_path / "latest.md").exists()


def test_latest_always_represents_the_newest_attempt(tmp_path: Path) -> None:
    write_digest(tmp_path, report(RunState.COMPLETE), local_date="2026-07-25", config=DigestConfig())
    write_digest(
        tmp_path,
        report(RunState.FAILED, run_id=43),
        local_date="2026-07-25",
        config=DigestConfig(),
    )
    assert "FAILED" in (tmp_path / "latest.md").read_text(encoding="utf-8")


def test_dated_digests_are_immutable_across_runs(tmp_path: Path) -> None:
    first = write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    write_digest(
        tmp_path, report(run_id=43), local_date="2026-07-25", config=DigestConfig()
    )
    assert first.path.read_text(encoding="utf-8") == first.markdown
    assert (tmp_path / "2026-07-25-run-43.md").is_file()


def test_an_unwritable_digest_root_raises_a_typed_error(tmp_path: Path) -> None:
    blocked = tmp_path / "digests"
    blocked.write_text("not a directory", encoding="utf-8")
    with pytest.raises(DigestWriteError):
        write_digest(blocked, report(), local_date="2026-07-25", config=DigestConfig())


def test_no_partial_file_remains_after_a_failed_write(tmp_path: Path, monkeypatch) -> None:
    import os

    def failing_replace(src: object, dst: object) -> None:
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", failing_replace)
    with pytest.raises(DigestWriteError):
        write_digest(tmp_path, report(), local_date="2026-07-25", config=DigestConfig())
    assert list(tmp_path.iterdir()) == []
