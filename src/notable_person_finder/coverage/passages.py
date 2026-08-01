"""Deterministic person-specific passage selection (K21).

Title/dek synthetic blocks count toward both ``max_passage_blocks`` and
``max_passage_characters`` and are filled first. Name matching reuses
``people.identity.match_key`` normalization — no second Unicode policy.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from notable_person_finder.config.models import AssessArticleConfig
from notable_person_finder.people.identity import match_key


@dataclass(frozen=True, slots=True)
class TextBlock:
    """Candidate text unit with stable source id (``t0``/``d0``/``bN``/``sN``)."""

    id: str
    text: str


@dataclass(frozen=True, slots=True)
class ArticleViewLike:
    """Minimal article view surface for passage selection."""

    title: str | None
    dek: str | None
    main_text_blocks: Sequence[TextBlock]
    snippets: Sequence[str] = ()


@dataclass(frozen=True, slots=True)
class SelectedPassage:
    """One model-facing passage after renumbering to ``p1..pN``."""

    id: str
    text: str
    source_block_id: str
    truncated: bool


@dataclass(frozen=True, slots=True)
class PassageView:
    """Bounded person-specific passages plus selection metadata."""

    passages: tuple[SelectedPassage, ...]
    name_absent: bool
    truncated_by_blocks: bool
    truncated_by_chars: bool


def select_passages(
    person_names: Sequence[str],
    article_view: ArticleViewLike,
    config: AssessArticleConfig,
) -> PassageView:
    """Build ordered, budget-capped passages for ``assess_article`` (K21)."""
    name_forms = _name_forms(person_names)
    candidates: list[TextBlock] = []

    title = _optional_text(article_view.title)
    if title is not None:
        candidates.append(
            TextBlock(id="t0", text=_prefix(title, config.max_title_characters))
        )

    dek = _optional_text(article_view.dek)
    if dek is not None:
        candidates.append(
            TextBlock(id="d0", text=_prefix(dek, config.max_summary_characters))
        )

    main_blocks = tuple(
        TextBlock(id=block.id, text=block.text)
        for block in article_view.main_text_blocks
        if _optional_text(block.text) is not None
    )

    opening_count = min(config.opening_block_count, len(main_blocks))
    for block in main_blocks[:opening_count]:
        _append_unique(candidates, block)

    name_hit_indices: list[int] = []
    for index, block in enumerate(main_blocks):
        if _contains_name(block.text, name_forms):
            name_hit_indices.append(index)

    name_absent = len(name_hit_indices) == 0
    if name_absent:
        for snip_index, snippet in enumerate(article_view.snippets, start=1):
            text = _optional_text(snippet)
            if text is None:
                continue
            _append_unique(
                candidates,
                TextBlock(id=f"s{snip_index}", text=text),
            )
    else:
        adjacent_indices: set[int] = set()
        for index in name_hit_indices:
            adjacent_indices.add(index)
            if index > 0:
                adjacent_indices.add(index - 1)
            if index + 1 < len(main_blocks):
                adjacent_indices.add(index + 1)
        for index in sorted(adjacent_indices):
            _append_unique(candidates, main_blocks[index])

    selected: list[SelectedPassage] = []
    chars = 0
    truncated_by_chars = False
    for candidate in candidates:
        if len(selected) >= config.max_passage_blocks:
            break
        if chars >= config.max_passage_characters:
            break
        remaining = config.max_passage_characters - chars
        take = candidate.text
        was_truncated = False
        if len(take) > remaining:
            take = take[:remaining]
            was_truncated = True
            truncated_by_chars = True
        if not take:
            continue
        selected.append(
            SelectedPassage(
                id=f"p{len(selected) + 1}",
                text=take,
                source_block_id=candidate.id,
                truncated=was_truncated,
            )
        )
        chars += len(take)

    truncated_by_blocks = len(candidates) > len(selected) or (
        len(selected) >= config.max_passage_blocks and len(candidates) > len(selected)
    )
    # Prefer explicit block-cap signal when fill stopped for block count.
    if len(selected) >= config.max_passage_blocks and len(candidates) > len(selected):
        truncated_by_blocks = True
    elif len(candidates) > len(selected) and chars >= config.max_passage_characters:
        # Remaining candidates skipped due to character budget only.
        truncated_by_blocks = False
        truncated_by_chars = True
    elif len(candidates) > len(selected):
        truncated_by_blocks = True

    return PassageView(
        passages=tuple(selected),
        name_absent=name_absent,
        truncated_by_blocks=truncated_by_blocks,
        truncated_by_chars=truncated_by_chars,
    )


def _name_forms(person_names: Sequence[str]) -> frozenset[str]:
    forms: set[str] = set()
    for name in person_names:
        stripped = name.strip()
        if not stripped:
            continue
        forms.add(stripped.casefold())
        key = match_key(stripped)
        if key:
            forms.add(key)
    return frozenset(forms)


def _contains_name(text: str, forms: frozenset[str]) -> bool:
    if not forms:
        return False
    folded = text.casefold()
    return any(form in folded for form in forms)


def _optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _prefix(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[:limit]


def _append_unique(candidates: list[TextBlock], block: TextBlock) -> None:
    if any(existing.id == block.id for existing in candidates):
        return
    candidates.append(block)
