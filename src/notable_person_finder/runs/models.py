from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RunState(StrEnum):
    RUNNING = "running"
    COMPLETE = "complete"
    PARTIAL = "partial"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


TERMINAL_RUN_STATES = frozenset(
    {RunState.COMPLETE, RunState.PARTIAL, RunState.FAILED, RunState.INTERRUPTED}
)


class WorkState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    DEFERRED = "deferred"
    FAILED_PERMANENT = "failed_permanent"
    SUPERSEDED = "superseded"


ACTIVE_WORK_STATES = frozenset(
    {WorkState.PENDING, WorkState.RUNNING, WorkState.DEFERRED}
)


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    INTERRUPTED = "interrupted"


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: int
    state: RunState
    timezone: str
    window_start: str
    window_end: str
    started_at: str
    finished_at: str | None
    budget_limit_nano_usd: int | None
    budget_reserved_nano_usd: int
    budget_actual_nano_usd: int
    digest_path: str | None
    digest_sha256: str | None

    @property
    def human_id(self) -> str:
        return f"run-{self.id}"


@dataclass(frozen=True, slots=True)
class WorkItem:
    id: int
    task_type: str
    subject_kind: str
    subject_id: int | None
    fingerprint: str
    required: bool
    priority: int
    state: WorkState


@dataclass(frozen=True, slots=True)
class SweepResult:
    runs: tuple[int, ...]
    work_items: int
    attempts: int


@dataclass(frozen=True, slots=True)
class RunCounters:
    required_succeeded: int
    required_pending: int
    required_deferred: int
    required_failed_permanent: int
    optional_succeeded: int
    optional_skipped: int
    operational_failures: int
