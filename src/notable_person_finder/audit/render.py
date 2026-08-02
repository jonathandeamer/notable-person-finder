"""Pure text rendering for `notable audit run`.

No SQL, no connection parameter, no I/O: every function here maps a view
model to `str`. All queries live in `audit/repository.py`.
"""

from __future__ import annotations

import json
from datetime import datetime

from notable_person_finder.audit.models import (
    AttemptAudit,
    AttemptLine,
    RunAudit,
    RunHeader,
)

# Section key -> the migration that introduced the table it depends on, used
# only to word the "section unavailable" fallback line (K3).
_MIGRATION_BY_SECTION = {
    "configuration": "0001",
    "transitions": "0002",
    "work outcomes": "0002",
    "attempts": "0002",
    "failures": "0002",
    "budget": "0002",
}


def _format_nano_usd(value: int | None) -> str:
    if value is None:
        return "not set"
    dollars = value / 1_000_000_000
    return f"${dollars:.6f} ({value} nano-USD)"


def _unavailable_line(section: str) -> str:
    migration = _MIGRATION_BY_SECTION.get(section, "unknown")
    return f"  section unavailable: schema predates migration {migration}"


def _format_duration(run: RunHeader) -> str:
    if run.finished_at is None:
        return "not finished"
    try:
        started = datetime.fromisoformat(run.started_at)
        finished = datetime.fromisoformat(run.finished_at)
    except ValueError:
        return "unknown"
    return f"{(finished - started).total_seconds():.3f}s"


def _format_pretty_json(canonical_json: str) -> str:
    try:
        parsed = json.loads(canonical_json)
    except json.JSONDecodeError:
        return canonical_json
    return json.dumps(parsed, indent=2, sort_keys=True)


def render_run_audit(audit: RunAudit) -> str:
    lines: list[str] = []
    unavailable = set(audit.unavailable_sections)

    lines.append("Run")
    lines.append(f"  id: {audit.run.id}")
    lines.append(f"  state: {audit.run.state}")
    lines.append(f"  timezone: {audit.run.timezone}")
    lines.append(f"  window: {audit.run.window_start} .. {audit.run.window_end}")
    lines.append(f"  started_at: {audit.run.started_at}")
    lines.append(f"  finished_at: {audit.run.finished_at or 'not finished'}")
    lines.append(f"  duration: {_format_duration(audit.run)}")
    lines.append("")

    lines.append("Configuration")
    if "configuration" in unavailable:
        lines.append(_unavailable_line("configuration"))
    elif audit.configuration is None:
        lines.append("  none recorded")
    else:
        configuration = audit.configuration
        lines.append(f"  snapshot_id: {configuration.snapshot_id}")
        lines.append(f"  fingerprint: {configuration.fingerprint}")
        lines.append(f"  created_at: {configuration.created_at}")
        lines.append("  canonical_json:")
        for json_line in _format_pretty_json(configuration.canonical_json).splitlines():
            lines.append(f"    {json_line}")
    lines.append("")

    lines.append("Transitions")
    if "transitions" in unavailable:
        lines.append(_unavailable_line("transitions"))
    elif not audit.transitions:
        lines.append("  none recorded")
    else:
        for transition in audit.transitions:
            reason = transition.reason or "no reason recorded"
            lines.append(f"  {transition.occurred_at}: {transition.state} ({reason})")
    lines.append("")

    lines.append("Work outcomes")
    if "work outcomes" in unavailable:
        lines.append(_unavailable_line("work outcomes"))
    else:
        outcomes = audit.work_outcomes
        if not outcomes.counts:
            lines.append("  none recorded")
        else:
            for task_type, state, count in outcomes.counts:
                lines.append(f"  {task_type} / {state}: {count}")
        if outcomes.failed_permanent:
            lines.append("  failed_permanent:")
            for item in outcomes.failed_permanent:
                reason = item.reason or "no reason recorded"
                lines.append(
                    f"    #{item.id} {item.task_type} "
                    f"{item.subject_kind}:{item.subject_id} -> {reason}"
                )
        if outcomes.deferred:
            lines.append("  deferred:")
            for item in outcomes.deferred:
                reason = item.reason or "no reason recorded"
                lines.append(
                    f"    #{item.id} {item.task_type} "
                    f"{item.subject_kind}:{item.subject_id} -> {reason}"
                )
    lines.append("")

    lines.append("Attempts")
    if "attempts" in unavailable:
        lines.append(_unavailable_line("attempts"))
    elif not audit.attempts:
        lines.append("  none recorded")
    else:
        for attempt in audit.attempts:
            outcome = attempt.outcome or "in flight"
            failure = (
                f" failure_category={attempt.failure_category}"
                if attempt.failure_category
                else ""
            )
            provider_status = (
                "n/a"
                if attempt.provider_status is None
                else str(attempt.provider_status)
            )
            latency_ms = (
                "n/a" if attempt.latency_ms is None else str(attempt.latency_ms)
            )
            response_bytes = (
                "n/a" if attempt.response_bytes is None else str(attempt.response_bytes)
            )
            destination_host = attempt.destination_host or "n/a"
            lines.append(
                f"  #{attempt.id} work_item={attempt.work_item_id} "
                f"ordinal={attempt.ordinal} {attempt.provider}/{attempt.operation}: "
                f"{outcome}{failure}"
            )
            lines.append(
                f"    provider_status={provider_status} latency_ms={latency_ms} "
                f"response_bytes={response_bytes} destination_host={destination_host}"
            )
            lines.append(
                f"    reserved={_format_nano_usd(attempt.reserved_nano_usd)} "
                f"actual={_format_nano_usd(attempt.actual_nano_usd)}"
            )
    lines.append("")

    lines.append("Failures")
    if "failures" in unavailable:
        lines.append(_unavailable_line("failures"))
    elif not audit.failures:
        lines.append("  none recorded")
    else:
        for failure in audit.failures:
            outcomes_text = ", ".join(failure.outcomes)
            lines.append(
                f"  {failure.failure_category}: {failure.count} "
                f"(outcomes={outcomes_text}, "
                f"e.g. {failure.example_provider}/{failure.example_operation})"
            )
    lines.append("")

    lines.append("Budget")
    if "budget" in unavailable:
        lines.append(_unavailable_line("budget"))
    else:
        budget = audit.budget
        lines.append(f"  limit: {_format_nano_usd(budget.limit_nano_usd)}")
        lines.append(f"  reserved: {_format_nano_usd(budget.reserved_nano_usd)}")
        lines.append(
            f"  actual (run total): {_format_nano_usd(budget.actual_nano_usd)}"
        )
        lines.append(
            "  actual (summed from attempts): "
            f"{_format_nano_usd(budget.attempt_actual_sum_nano_usd)}"
        )
        if budget.actual_nano_usd != budget.attempt_actual_sum_nano_usd:
            lines.append(
                "  divergence: run total and attempt sum disagree "
                "(cross-check failed, both values shown above)"
            )
    lines.append("")

    lines.append("Reporting")
    if audit.reporting is None:
        lines.append("  no digest recorded for this run")
    else:
        reporting = audit.reporting
        lines.append(f"  digest_path: {reporting.digest_path}")
        lines.append(f"  digest_sha256: {reporting.digest_sha256}")
        lines.append(f"  run_state: {reporting.run_state}")
        entry_count = (
            "unknown" if reporting.entry_count is None else str(reporting.entry_count)
        )
        lines.append(f"  entry_count: {entry_count}")
        lines.append(f"  see: notable digest show {audit.run.id}")

    return "\n".join(lines) + "\n"


_NO_RESULT_ROW_TEXT = (
    "no persisted result row; the attempt is the only durable evidence of this call"
)


def _format_attempt_line(prefix: str, attempt: AttemptLine) -> list[str]:
    lines: list[str] = []
    outcome = attempt.outcome or "in flight"
    failure = (
        f" failure_category={attempt.failure_category}"
        if attempt.failure_category
        else ""
    )
    provider_status = (
        "n/a" if attempt.provider_status is None else str(attempt.provider_status)
    )
    latency_ms = "n/a" if attempt.latency_ms is None else str(attempt.latency_ms)
    response_bytes = (
        "n/a" if attempt.response_bytes is None else str(attempt.response_bytes)
    )
    destination_host = attempt.destination_host or "n/a"
    lines.append(
        f"{prefix}#{attempt.id} work_item={attempt.work_item_id} "
        f"ordinal={attempt.ordinal} "
        f"{attempt.provider}/{attempt.operation}: {outcome}{failure}"
    )
    lines.append(
        f"{prefix}  provider_status={provider_status} latency_ms={latency_ms} "
        f"response_bytes={response_bytes} destination_host={destination_host}"
    )
    lines.append(
        f"{prefix}  reserved={_format_nano_usd(attempt.reserved_nano_usd)} "
        f"actual={_format_nano_usd(attempt.actual_nano_usd)}"
    )
    return lines


def render_attempt_audit(audit: AttemptAudit) -> str:
    lines: list[str] = []

    lines.append("Attempt")
    lines.extend(_format_attempt_line("  ", audit.attempt))
    lines.append("")

    lines.append("Work item")
    work_item = audit.work_item
    reason = work_item.reason or "no reason recorded"
    lines.append(f"  id: {work_item.id}")
    lines.append(f"  task_type: {work_item.task_type}")
    lines.append(f"  subject: {work_item.subject_kind}:{work_item.subject_id}")
    lines.append(f"  fingerprint: {work_item.fingerprint}")
    lines.append(f"  state: {work_item.state}")
    lines.append(f"  reason: {reason}")
    lines.append("")

    lines.append("Retry history")
    if not audit.retry_history:
        lines.append("  none recorded")
    else:
        for retry in audit.retry_history:
            lines.extend(_format_attempt_line("  ", retry))
    lines.append("")

    lines.append("Result")
    if audit.binding is not None and not audit.binding.external:
        # K10: a task type registered as making no external call has an
        # attempt anyway -- a data inconsistency, not a missing result.
        lines.append(
            f"  data inconsistency: task_type {work_item.task_type!r} is "
            "registered as making no external call (external=False), but "
            "an attempt exists for it; no result is rendered"
        )
    else:
        if audit.caveat is not None:
            lines.append(f"  caveat: {audit.caveat}")
        if audit.no_result_row:
            lines.append(f"  {_NO_RESULT_ROW_TEXT}")
        else:
            for row in audit.result_rows:
                for key in sorted(row.keys()):
                    lines.append(f"    {key}: {row[key]}")
                lines.append("  ---")

    return "\n".join(lines) + "\n"
