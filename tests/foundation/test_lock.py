from __future__ import annotations

from pathlib import Path

import pytest

from notable_person_finder.runs.lock import LockUnavailable, MutationLock


def test_second_lock_fails_without_treating_metadata_as_authority(
    tmp_path: Path,
) -> None:
    path = tmp_path / "notable.lock"
    with MutationLock(path) as first:
        assert first.owner.pid > 0
        with pytest.raises(LockUnavailable) as captured, MutationLock(path):
            raise AssertionError("contended lock was acquired")

    assert captured.value.path == path
    with MutationLock(path):
        pass


def test_stale_text_without_an_os_lock_does_not_block(tmp_path: Path) -> None:
    path = tmp_path / "notable.lock"
    path.write_text('{"pid":999999,"started_at":"2000-01-01T00:00:00Z"}\n')

    with MutationLock(path) as acquired:
        assert acquired.owner.pid > 0
