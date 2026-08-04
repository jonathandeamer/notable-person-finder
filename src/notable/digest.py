"""Markdown digest rendering and atomic file writing."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import Sequence
from decimal import Decimal
from pathlib import Path

from notable.policy import canonical_domain
from notable.rank import Lead


def _flat(value: str) -> str:
    """Collapse whitespace so model text cannot inject headings or structure."""
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
    lines = [
        f"# Notable — {generated_at[:10]}",
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
            lines += [
                f"### {_flat(lead.display_name)}",
                "",
                f"- Outcome: `{lead.outcome}`",
                (
                    "- Wikipedia: "
                    f"`{getattr(lead.wikipedia_verdict, 'outcome', 'unknown')}`"
                ),
                (
                    f"- Source: [{_flat(lead.publisher_label)}]"
                    f"({_flat(lead.source_url)})"
                ),
            ]
            if lead.qualifying_domains:
                joined = ", ".join(lead.qualifying_domains)
                lines.append(
                    f"- Qualifying domains: {len(lead.qualifying_domains)} ({joined})"
                )
            else:
                lines.append("- Qualifying domains: 0")
            if lead.article_assessments:
                lines.append("- Coverage:")
                for article in lead.article_assessments:
                    url = getattr(article, "url", "")
                    label = canonical_domain(url) or url or "article"
                    lines.append(f"  - [{_flat(label)}]({_flat(url)})")
            lines.append(f"- Why: {_flat(lead.rationale)}")
            if lead.namesake_urls:
                urls = ", ".join(lead.namesake_urls)
                n = len(lead.namesake_urls)
                lines.append(f"- {n} further leads share this name ({urls})")
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
    """Atomic dated digest + latest.md. Must land before store.commit."""
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
