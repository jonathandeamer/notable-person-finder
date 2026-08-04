"""Markdown digest rendering and atomic file writing."""

from __future__ import annotations

import contextlib
import os
import tempfile
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DigestEntry:
    identity_key: str
    display_name: str
    source_url: str
    publisher_label: str
    rationale: str


def identity_key(name: str) -> str:
    """An opaque scalar. In the MVP a normalized name; later a person id.

    Only `rank` (as a tie-breaker and for collapsing), `digest` (for
    suppression) and `store` (which keys `surfaced` on it) may read this, and
    none of them may parse it.
    """
    folded = unicodedata.normalize("NFKC", name).casefold()
    return " ".join(folded.split())


def _flat(value: str) -> str:
    """Collapse whitespace so one field cannot become two lines.

    Rationale text comes from the model and reaches the artifact. One
    containing a line that begins `### ` would otherwise render a heading for
    a person nobody detected.
    """
    return " ".join(value.split())


def render(
    entries: Sequence[DigestEntry],
    *,
    generated_at: str,
    status: str,
    cost_usd: Decimal,
    n_settled: int,
    n_incomplete: int,
) -> str:
    day = generated_at[:10]
    lines = [
        f"# Notable — detected people, {day}",
        "",
        f"- Run status: **{status}**",
        f"- Items settled: {n_settled}",
        f"- Items incomplete: {n_incomplete}",
        f"- Model spend: ${cost_usd}",
        "",
        "> Phase 2 lists people detected in feed items who do not already",
        "> have a matching Wikipedia page. It does not yet assess coverage,",
        "> so nothing here is a lead.",
        "",
        "## Detected",
        "",
    ]
    if not entries:
        lines.append("No people detected in this run.")
    else:
        for entry in entries:
            lines.append(f"### {_flat(entry.display_name)}")
            lines.append("")
            lines.append(
                f"- Source: [{_flat(entry.publisher_label)}]({_flat(entry.source_url)})"
            )
            lines.append(f"- Why: {_flat(entry.rationale)}")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write(
    entries: Sequence[DigestEntry],
    directory: Path,
    *,
    generated_at: str,
    status: str,
    cost_usd: Decimal,
    n_settled: int,
    n_incomplete: int,
) -> Path:
    """Write the dated digest and `latest.md`, both atomically.

    Ordering is load-bearing: the files land before any state is committed. A
    crash between them repeats a digest next run, which is recoverable. The
    reverse would mark leads surfaced that were never seen.

    The digest renders from this run only -- no history, no backlog. A second
    run the same day replaces the file, and the spec accepts that cost.
    """
    directory.mkdir(parents=True, exist_ok=True)
    dated = directory / f"{generated_at[:10]}.md"
    text = render(
        entries,
        generated_at=generated_at,
        status=status,
        cost_usd=cost_usd,
        n_settled=n_settled,
        n_incomplete=n_incomplete,
    )
    _atomic_write(dated, text)
    _atomic_write(directory / "latest.md", text)
    return dated


def _atomic_write(path: Path, text: str) -> None:
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f"{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        if temporary is not None:
            with contextlib.suppress(OSError):
                temporary.unlink(missing_ok=True)
        raise
