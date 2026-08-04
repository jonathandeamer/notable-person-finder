"""Markdown digest rendering and atomic file writing."""

from __future__ import annotations

import contextlib
import os
import tempfile
import unicodedata
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

from notable.rank import Lead


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
    entries: Sequence[Lead],
    *,
    generated_at: str,
    status: str,
    cost_usd: Decimal,
    n_settled: int,
    n_incomplete: int,
) -> str:
    day = generated_at[:10]
    lines = [
        f"# Notable — {day}",
        "",
        f"- Run status: **{status}**",
        f"- Items settled: {n_settled}",
        f"- Items incomplete: {n_incomplete}",
        f"- Model spend: ${cost_usd}",
        "",
        "## Shortlist",
        "",
    ]
    if not entries:
        lines.append("No leads found in this run.")
    else:
        for lead in entries:
            lines.append(f"### {_flat(lead.display_name)}")
            lines.append("")
            lines.append(f"- Outcome: `{lead.outcome}`")
            wiki_outcome = getattr(lead.wikipedia_verdict, "outcome", "unknown")
            lines.append(f"- Wikipedia: `{wiki_outcome}`")
            lines.append(f"- Source: [{_flat(lead.publisher_label)}]({_flat(lead.source_url)})")
            
            if lead.qualifying_domains:
                lines.append(f"- Qualifying domains: {len(lead.qualifying_domains)} ({', '.join(lead.qualifying_domains)})")
            else:
                lines.append("- Qualifying domains: 0")
                
            if lead.article_assessments:
                lines.append("- Coverage:")
                for i, a in enumerate(lead.article_assessments, start=1):
                    pub = getattr(a, "publisher", "Article")
                    lines.append(f"  - [{_flat(pub)}]({_flat(getattr(a, 'url', ''))})")
                    
            lines.append(f"- Why: {_flat(lead.rationale)}")
            
            if lead.namesake_urls:
                lines.append(f"- Namesakes: {len(lead.namesake_urls)} ({', '.join(lead.namesake_urls)})")
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def write(
    entries: Sequence[Lead],
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
