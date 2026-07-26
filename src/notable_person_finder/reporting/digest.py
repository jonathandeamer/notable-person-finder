from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from notable_person_finder.config.models import DigestConfig
from notable_person_finder.runs.engine import RunReport
from notable_person_finder.runs.models import RunState

_PROMINENT_STATES = {
    RunState.PARTIAL: "PARTIAL RUN — required work remains unevaluated.",
    RunState.FAILED: "FAILED RUN — this digest may be incomplete.",
    RunState.INTERRUPTED: "INTERRUPTED RUN — the process ended before finishing.",
}


class DigestWriteError(Exception):
    """The digest could not be persisted atomically."""


@dataclass(frozen=True, slots=True)
class DigestRecord:
    path: Path
    sha256: str
    markdown: str


def render_digest(report: RunReport, *, local_date: str) -> str:
    lines = [f"# Notable Person Finder — {local_date}", ""]

    warning = _PROMINENT_STATES.get(report.state)
    if warning is not None:
        lines.append(f"> **{warning}** See `notable audit run {report.run_id}`.")
    else:
        lines.append(f"> Run {report.human_id} completed normally.")
    lines.append("")

    lines += [
        f"**State:** {report.state}",
        f"**Run:** {report.human_id}",
        f"**Timezone:** {report.timezone}",
        f"**Observation window:** {report.window_start} to {report.window_end}",
        f"**Started:** {report.started_at}",
        f"**Finished:** {report.finished_at}",
        "",
        "## Shortlist",
        "",
    ]

    # Milestone 6 replaces this section with ranked entries and synthesis.
    lines += ["No candidates met the shortlist criteria in this window.", ""]

    counters = report.counters
    lines += [
        "## Operational summary",
        "",
        f"- Required work succeeded: {counters.required_succeeded}",
        f"- Required work still pending: {counters.required_pending}",
        f"- Required work deferred: {counters.required_deferred}",
        f"- Optional work succeeded: {counters.optional_succeeded}",
    ]

    if counters.required_failed_permanent:
        lines.append(
            f"- Required work permanently failed: {counters.required_failed_permanent}"
        )
    if counters.operational_failures:
        lines.append(f"- Operational failures: {counters.operational_failures}")
    if report.failure_categories:
        rendered = ", ".join(
            f"{category} ({count})"
            for category, count in sorted(report.failure_categories.items())
        )
        lines.append(f"- Failures by category: {rendered}")
    if report.paused_providers:
        lines.append(f"- Providers paused this run: {', '.join(sorted(report.paused_providers))}")
    if report.interrupted_runs:
        rendered = ", ".join(f"run-{run_id}" for run_id in report.interrupted_runs)
        lines.append(f"- Interrupted predecessor runs recorded: {rendered}")

    return "\n".join(lines) + "\n"


def _atomic_write(target: Path, markdown: str) -> None:
    handle, temporary_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(markdown)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    except BaseException as error:
        temporary.unlink(missing_ok=True)
        raise DigestWriteError(f"could not write {target}: {error}") from error


def write_digest(
    digests_dir: Path,
    report: RunReport,
    *,
    local_date: str,
    config: DigestConfig,
) -> DigestRecord:
    """Atomically persist the immutable dated digest and the latest copy."""
    markdown = render_digest(report, local_date=local_date)
    try:
        digests_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise DigestWriteError(f"could not create {digests_dir}: {error}") from error

    target = digests_dir / f"{local_date}-{report.human_id}.md"
    _atomic_write(target, markdown)
    if config.write_latest_copy:
        _atomic_write(digests_dir / "latest.md", markdown)

    return DigestRecord(
        path=target,
        sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        markdown=markdown,
    )
