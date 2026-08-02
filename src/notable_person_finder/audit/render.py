"""Pure text rendering for `notable audit run`.

No SQL, no connection parameter, no I/O: every function here maps a view
model to `str`. All queries live in `audit/repository.py`.
"""

from __future__ import annotations

from notable_person_finder.audit.models import RunAudit

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
                f", failure={attempt.failure_category}"
                if attempt.failure_category
                else ""
            )
            lines.append(
                f"  #{attempt.id} work_item={attempt.work_item_id} "
                f"{attempt.provider}/{attempt.operation}: {outcome}{failure}"
            )
    lines.append("")

    lines.append("Failures")
    if "failures" in unavailable:
        lines.append(_unavailable_line("failures"))
    elif not audit.failures:
        lines.append("  none recorded")
    else:
        for failure in audit.failures:
            lines.append(
                f"  {failure.failure_category}: {failure.count} "
                f"(e.g. {failure.example_provider}/{failure.example_operation})"
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
        lines.append(f"  digest_path: {audit.reporting.digest_path}")
        lines.append(f"  digest_sha256: {audit.reporting.digest_sha256}")

    return "\n".join(lines) + "\n"
