from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from notable_person_finder.config.models import DigestConfig
from notable_person_finder.obs.logging import EVENT_LOGGER_NAME, log_event
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


def _fsync_directory(directory: Path) -> None:
    """Fsync the directory entry itself.

    os.replace/os.link are atomic renames, not durability guarantees: fsync
    on the file's own contents says nothing about whether the directory
    entry pointing at it survives a crash. Without this, a power loss
    between write_digest returning and the next filesystem checkpoint can
    lose the rename entirely, even though the caller already recorded the
    path and hash as durable.
    """
    handle = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(handle)
    finally:
        os.close(handle)


def _atomic_write(target: Path, markdown: str, *, exclusive: bool) -> None:
    """Write `markdown` to `target` durably.

    `exclusive=True` is for the dated digest, which is documented immutable:
    the temp file is linked into place with `os.link`, which raises
    `FileExistsError` if `target` already exists rather than silently
    replacing it. `exclusive=False` is for `latest.md`, which is a
    convenience copy meant to be replaced every run.
    """
    handle, temporary_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(markdown)
            stream.flush()
            os.fsync(stream.fileno())

        # Read back what is actually on disk before it ever reaches the
        # final path. A hash computed from a corrupt temp file would
        # otherwise self-consistently describe the corruption and still
        # report success.
        written = temporary.read_bytes()
        if written != markdown.encode("utf-8"):
            raise DigestWriteError(f"content mismatch after writing {target}")

        if exclusive:
            try:
                os.link(temporary, target)
            except FileExistsError as error:
                raise DigestWriteError(
                    f"{target} already exists and is immutable"
                ) from error
            temporary.unlink(missing_ok=True)
        else:
            os.replace(temporary, target)
        _fsync_directory(target.parent)
    except Exception as error:
        temporary.unlink(missing_ok=True)
        if isinstance(error, DigestWriteError):
            raise
        raise DigestWriteError(f"could not write {target}: {error}") from error
    except BaseException:
        # KeyboardInterrupt/SystemExit/GeneratorExit are not write failures;
        # clean up the temp file but let the interrupt keep its identity
        # rather than reporting it as a DigestWriteError.
        temporary.unlink(missing_ok=True)
        raise


def write_digest(
    digests_dir: Path,
    report: RunReport,
    *,
    local_date: str,
    config: DigestConfig,
) -> DigestRecord:
    """Atomically persist the immutable dated digest and the latest copy.

    The dated digest is the authoritative artifact: a failure writing it is
    fatal, and it refuses to overwrite an existing one for the same run.
    `latest.md` is a convenience copy; a failure writing it is logged and
    swallowed rather than discarding the record of an already-durable dated
    digest, since a run whose real digest exists must not be reported as
    having no digest at all.
    """
    markdown = render_digest(report, local_date=local_date)
    try:
        digests_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise DigestWriteError(f"could not create {digests_dir}: {error}") from error

    target = digests_dir / f"{local_date}-{report.human_id}.md"
    _atomic_write(target, markdown, exclusive=True)

    if config.write_latest_copy:
        try:
            _atomic_write(digests_dir / "latest.md", markdown, exclusive=False)
        except DigestWriteError as error:
            log_event(
                logging.getLogger(EVENT_LOGGER_NAME),
                "digest.latest_copy_failed",
                severity=logging.WARNING,
                run_id=report.run_id,
                error_type=type(error.__cause__ or error).__name__,
            )

    return DigestRecord(
        path=target,
        sha256=hashlib.sha256(target.read_bytes()).hexdigest(),
        markdown=markdown,
    )
