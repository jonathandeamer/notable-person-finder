"""Read-only audit models.

`audit/` may import only from `config/`, `db/`, and `obs/` within this
package; it reads every other package's tables with its own SQL rather than
importing them.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DigestLocation:
    run_id: int
    file_path: str
    content_hash: str
    timezone: str | None  # None on the pre-0008 run-column fallback
    window_start: str | None
    window_end: str | None
    run_state: str | None
    created_at: str | None
    source: str  # "digest_table" | "run_columns"


@dataclass(frozen=True, slots=True)
class RunHeader:
    id: int
    state: str
    started_at: str
    finished_at: str | None
    timezone: str
    window_start: str
    window_end: str


@dataclass(frozen=True, slots=True)
class ConfigurationProvenance:
    snapshot_id: int
    fingerprint: str
    created_at: str


@dataclass(frozen=True, slots=True)
class Transition:
    state: str
    reason: str | None
    occurred_at: str


@dataclass(frozen=True, slots=True)
class WorkItemLine:
    id: int
    task_type: str
    subject_kind: str
    subject_id: int | None
    fingerprint: str
    state: str
    reason: str | None


@dataclass(frozen=True, slots=True)
class WorkOutcomes:
    counts: tuple[tuple[str, str, int], ...]
    failed_permanent: tuple[WorkItemLine, ...]
    deferred: tuple[WorkItemLine, ...]


@dataclass(frozen=True, slots=True)
class AttemptLine:
    id: int
    work_item_id: int
    provider: str
    operation: str
    outcome: str | None
    failure_category: str | None
    reserved_nano_usd: int
    actual_nano_usd: int | None


@dataclass(frozen=True, slots=True)
class FailureGroup:
    failure_category: str
    count: int
    example_provider: str
    example_operation: str


@dataclass(frozen=True, slots=True)
class BudgetSummary:
    limit_nano_usd: int | None
    reserved_nano_usd: int
    actual_nano_usd: int
    attempt_actual_sum_nano_usd: int


@dataclass(frozen=True, slots=True)
class ReportingResult:
    digest_path: str | None
    digest_sha256: str | None


@dataclass(frozen=True, slots=True)
class RunAudit:
    run: RunHeader
    configuration: ConfigurationProvenance | None
    transitions: tuple[Transition, ...]
    work_outcomes: WorkOutcomes
    attempts: tuple[AttemptLine, ...]
    failures: tuple[FailureGroup, ...]
    budget: BudgetSummary
    reporting: ReportingResult | None
    unavailable_sections: tuple[str, ...]  # K3 schema guards
