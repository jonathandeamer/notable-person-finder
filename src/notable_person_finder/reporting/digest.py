from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from collections.abc import Mapping
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

# `RunEngine._settle` bounds each reason's length, but not how many distinct
# reasons a handler's classification can produce -- a legitimate handler may
# have many legitimate values along its own dimension. This caps only how
# many of those get their own rendered line, so the digest itself cannot grow
# unbounded the way the reason strings already cannot. The operator must never
# lose the total, so the rows folded past this cap are accounted for by one
# final summary row rather than silently dropped.
_MAX_RENDERED_DEFERRAL_REASONS = 12


class DigestWriteError(Exception):
    """The digest could not be persisted atomically."""


def _format_nano_usd(nano_usd: int) -> str:
    """Render nano-USD as a two-decimal dollar figure using only integers.

    Money is integer nano-USD throughout the run engine; this formats it for
    a human reader without ever converting through a float.
    """
    sign = "-" if nano_usd < 0 else ""
    dollars, remainder_nano = divmod(abs(nano_usd), 1_000_000_000)
    cents = (remainder_nano * 100) // 1_000_000_000
    return f"{sign}${dollars}.{cents:02d}"


@dataclass(frozen=True, slots=True)
class DigestRecord:
    path: Path
    sha256: str
    markdown: str


@dataclass(frozen=True, slots=True)
class IngestionSummary:
    """Counts rendered in the digest's ingestion section."""

    feeds_fetched: int
    feeds_not_modified: int
    feeds_failed: int
    source_items_created: int
    articles_created: int


@dataclass(frozen=True, slots=True)
class PeopleRunSummary:
    """Counts rendered in the digest's person-detection section.

    Cost fields are integer nano-USD, matching the run engine's budget
    ledger. OpenRouter cost is the single shared budget line; identity does
    not repeat it.
    """

    source_items_triaged: int
    research_people: int
    do_not_research: int
    uncertain: int
    research_or_uncertain_mentions: int
    overflow: int
    insufficient_input: int
    model_deferred: int
    model_failed: int
    model_failed_by_category: Mapping[str, int]
    budget_limit_nano_usd: int | None
    budget_reserved_nano_usd: int
    budget_actual_nano_usd: int


@dataclass(frozen=True, slots=True)
class IdentityRunSummary:
    """Counts rendered in the digest's person-identity section."""

    people_created: int
    mentions_resolved: int
    linked_same_person: int
    created_via_different_people: int
    created_via_created_new: int
    uncertain: int
    unresolved_eligible_mentions: int
    active_possible_same_person: int
    confirmed_merges: int
    resolution_model_deferred: int
    resolution_model_failed: int


def render_digest(
    report: RunReport,
    *,
    local_date: str,
    ingestion: IngestionSummary | None = None,
    people: PeopleRunSummary | None = None,
    identity: IdentityRunSummary | None = None,
) -> str:
    lines = [f"# Notable Person Finder — {local_date}", ""]

    warning = _PROMINENT_STATES.get(report.state)
    if warning is not None:
        lines.append(f"> **{warning}** Run `notable status` for details.")
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
    ]
    # Highest count first, then reason ascending as a stable tie-break: the
    # rows most worth an operator's attention lead, and a cap that then
    # truncates alphabetically-early-but-low-count reasons would be the wrong
    # ones to keep.
    ordered_reasons = sorted(
        report.deferred_reasons.items(), key=lambda item: (-item[1], item[0])
    )
    # Folding costs the operator a row of detail; showing it costs nothing
    # extra when there is only one row to fold. So a lone reason past the cap
    # is rendered on its own line instead, and folding only ever kicks in for
    # two or more -- otherwise the remainder row's noun would need to be
    # singular AND the row it replaces would already have fit within one more
    # line.
    if len(ordered_reasons) <= _MAX_RENDERED_DEFERRAL_REASONS + 1:
        shown_reasons = ordered_reasons
        folded_reasons: list[tuple[str, int]] = []
    else:
        shown_reasons = ordered_reasons[:_MAX_RENDERED_DEFERRAL_REASONS]
        folded_reasons = ordered_reasons[_MAX_RENDERED_DEFERRAL_REASONS:]
    for reason, count in shown_reasons:
        lines.append(f"  - {reason}: {count}")
    if folded_reasons:
        folded_count = sum(count for _, count in folded_reasons)
        noun = "reason" if len(folded_reasons) == 1 else "reasons"
        lines.append(f"  - ({len(folded_reasons)} more {noun} folded): {folded_count}")
    if (
        report.budget_limit_nano_usd is not None
        or report.budget_reserved_nano_usd
        or report.budget_actual_nano_usd
    ):
        cap = (
            _format_nano_usd(report.budget_limit_nano_usd)
            if report.budget_limit_nano_usd is not None
            else "none"
        )
        lines.append(
            f"- Budget: cap {cap}, "
            f"reserved {_format_nano_usd(report.budget_reserved_nano_usd)}, "
            f"spent {_format_nano_usd(report.budget_actual_nano_usd)}"
        )
    lines.append(f"- Optional work succeeded: {counters.optional_succeeded}")

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
        lines.append(
            f"- Providers paused this run: {', '.join(sorted(report.paused_providers))}"
        )
    if report.interrupted_runs:
        rendered = ", ".join(f"run-{run_id}" for run_id in report.interrupted_runs)
        lines.append(f"- Interrupted predecessor runs recorded: {rendered}")

    if ingestion is not None:
        lines += [
            "",
            "### Ingestion",
            "",
            f"- Feeds fetched: {ingestion.feeds_fetched}",
            f"- Feeds not modified: {ingestion.feeds_not_modified}",
            f"- Feeds failed: {ingestion.feeds_failed}",
            f"- Source items created: {ingestion.source_items_created}",
            f"- Articles created: {ingestion.articles_created}",
        ]

    if people is not None:
        lines += [
            "",
            "### Person detection",
            "",
            f"- Source items triaged: {people.source_items_triaged}",
            f"- Research: {people.research_people}",
            f"- Uncertain: {people.uncertain}",
            f"- Do not research: {people.do_not_research}",
            f"- Unresolved research or uncertain mentions: "
            f"{people.research_or_uncertain_mentions}",
            f"- Overflow observations: {people.overflow}",
            f"- Insufficient input: {people.insufficient_input}",
            f"- Model work deferred: {people.model_deferred}",
            f"- Model work permanently failed: {people.model_failed}",
        ]
        if people.model_failed_by_category:
            rendered = ", ".join(
                f"{category} ({count})"
                for category, count in sorted(people.model_failed_by_category.items())
            )
            lines.append(f"- Model failures by category: {rendered}")
        if (
            people.budget_limit_nano_usd is not None
            or people.budget_reserved_nano_usd
            or people.budget_actual_nano_usd
        ):
            cap = (
                _format_nano_usd(people.budget_limit_nano_usd)
                if people.budget_limit_nano_usd is not None
                else "none"
            )
            lines.append(
                f"- OpenRouter cost: cap {cap}, "
                f"reserved {_format_nano_usd(people.budget_reserved_nano_usd)}, "
                f"spent {_format_nano_usd(people.budget_actual_nano_usd)}"
            )

    if identity is not None:
        lines += [
            "",
            "### Person identity",
            "",
            f"- People created this run: {identity.people_created}",
            f"- Mentions resolved this run: {identity.mentions_resolved}",
            f"- Linked same_person: {identity.linked_same_person}",
            f"- Created via different_people: {identity.created_via_different_people}",
            f"- Created via created_new (no candidates): "
            f"{identity.created_via_created_new}",
            f"- Uncertain (possible same person): {identity.uncertain}",
            f"- Unresolved eligible mentions remaining: "
            f"{identity.unresolved_eligible_mentions}",
            f"- Active possible_same_person relations (corpus): "
            f"{identity.active_possible_same_person}",
            f"- Confirmed merges this run: {identity.confirmed_merges}",
            f"- Resolution model deferred: {identity.resolution_model_deferred}",
            f"- Resolution model permanently failed: "
            f"{identity.resolution_model_failed}",
        ]

    return "\n".join(lines) + "\n"


def _fsync_directory(directory: Path) -> None:
    """Fsync the directory entry itself.

    os.replace is an atomic rename, not a durability guarantee: fsync
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

    `exclusive=True` is for the dated digest, which is documented immutable.
    The final path is first CLAIMED with `os.open(O_CREAT | O_EXCL |
    O_WRONLY)`, which raises `FileExistsError` if `target` already exists
    rather than silently replacing it; the fully written temp file is then
    `os.replace`d over our own zero-byte claim, so the content still appears
    at the final path in one atomic step. `exclusive=False` is for
    `latest.md`, which is a convenience copy meant to be replaced every run.

    Hard links are deliberately NOT used for the claim. `os.link` raises
    `OSError(EOPNOTSUPP)` on exFAT, on many SMB/CIFS mounts and on some
    container bind mounts, which would make every run on such a data root
    fail permanently with a message naming neither hard links nor the fix.
    `O_CREAT | O_EXCL` gives the same atomic create-or-refuse semantics with
    no filesystem feature beyond ordinary file creation.
    """
    handle, temporary_name = tempfile.mkstemp(dir=target.parent, suffix=".tmp")
    temporary = Path(temporary_name)
    # True only while `target` exists solely as OUR empty claim. Cleared the
    # moment the replace lands, so a later failure (an fsync of the directory)
    # leaves the complete digest in place exactly as the previous `os.link`
    # implementation did.
    #
    # An exception or an interrupt between the claim and the replace is
    # unwound by the handlers below, which remove the empty claim. A HARD KILL
    # in that window is not: SIGKILL, the OOM killer or power loss can leave a
    # zero-byte file at the dated path. `os.link` could not, because it was a
    # single atomic step. That regression is accepted deliberately -- POSIX has
    # no portable no-clobber rename (`RENAME_NOREPLACE` is Linux-only), the
    # filename is scoped to one run so a stranded claim blocks only the dead
    # run's own path and nothing ever rewrites it, and it is far narrower than
    # the total exFAT/SMB failure `os.link` caused.
    claimed = False
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
                claim = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            except FileExistsError as error:
                raise DigestWriteError(
                    f"{target} already exists and is immutable"
                ) from error
            os.close(claim)
            claimed = True
        os.replace(temporary, target)
        claimed = False
        _fsync_directory(target.parent)
    except Exception as error:
        temporary.unlink(missing_ok=True)
        if claimed:
            target.unlink(missing_ok=True)
        if isinstance(error, DigestWriteError):
            raise
        raise DigestWriteError(f"could not write {target}: {error}") from error
    except BaseException:
        # KeyboardInterrupt/SystemExit/GeneratorExit are not write failures;
        # clean up the temp file but let the interrupt keep its identity
        # rather than reporting it as a DigestWriteError.
        temporary.unlink(missing_ok=True)
        if claimed:
            target.unlink(missing_ok=True)
        raise


def write_digest(
    digests_dir: Path,
    report: RunReport,
    *,
    local_date: str,
    config: DigestConfig,
    ingestion: IngestionSummary | None = None,
    people: PeopleRunSummary | None = None,
    identity: IdentityRunSummary | None = None,
) -> DigestRecord:
    """Atomically persist the immutable dated digest and the latest copy.

    The dated digest is the authoritative artifact: a failure writing it is
    fatal, and it refuses to overwrite an existing one for the same run.
    `latest.md` is a convenience copy; a failure writing it is logged and
    swallowed rather than discarding the record of an already-durable dated
    digest, since a run whose real digest exists must not be reported as
    having no digest at all.
    """
    markdown = render_digest(
        report,
        local_date=local_date,
        ingestion=ingestion,
        people=people,
        identity=identity,
    )
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
