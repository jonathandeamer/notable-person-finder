from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import portalocker


@dataclass(frozen=True, slots=True)
class LockOwner:
    pid: int
    started_at: str


class LockUnavailable(Exception):
    def __init__(self, path: Path, owner: LockOwner | None):
        self.path = path
        self.owner = owner
        detail = f"; held by PID {owner.pid}" if owner is not None else ""
        super().__init__(f"another mutation is using {path}{detail}")


class MutationLock:
    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.owner = LockOwner(pid=os.getpid(), started_at=_utc_now())
        self._lock: portalocker.Lock | None = None
        self._handle: IO[str] | None = None

    def __enter__(self) -> MutationLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = portalocker.Lock(
            self.path,
            mode="a+",
            timeout=0,
            flags=(
                portalocker.LockFlags.EXCLUSIVE | portalocker.LockFlags.NON_BLOCKING
            ),
        )
        try:
            self._handle = self._lock.acquire()
        except portalocker.exceptions.LockException as error:
            raise LockUnavailable(self.path, _read_owner(self.path)) from error

        self._handle.seek(0)
        self._handle.truncate()
        json.dump(
            {"pid": self.owner.pid, "started_at": self.owner.started_at},
            self._handle,
            sort_keys=True,
            separators=(",", ":"),
        )
        self._handle.write("\n")
        self._handle.flush()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        if self._lock is not None:
            self._lock.release()
        self._handle = None
        self._lock = None


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_owner(path: Path) -> LockOwner | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        pid = value["pid"]
        started_at = value["started_at"]
        if not isinstance(pid, int) or not isinstance(started_at, str):
            return None
        return LockOwner(pid=pid, started_at=started_at)
    except (OSError, json.JSONDecodeError, KeyError, TypeError):
        return None
