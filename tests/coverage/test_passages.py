"""PassageSelector (K21): title/dek count toward caps; name_absent path."""

from __future__ import annotations

from notable_person_finder.config.models import AssessArticleConfig
from notable_person_finder.coverage.passages import (
    ArticleViewLike,
    TextBlock,
    select_passages,
)


def _config(**changes: object) -> AssessArticleConfig:
    values: dict[str, object] = {
        "max_passage_blocks": 24,
        "max_passage_characters": 6000,
        "opening_block_count": 2,
        "max_title_characters": 500,
        "max_summary_characters": 4000,
    }
    values.update(changes)
    return AssessArticleConfig.model_validate(values)


def test_title_and_dek_fill_first_and_count_toward_block_cap() -> None:
    """K21: t0/d0 are ordinary blocks — not exempt from max_passage_blocks.

    If title/dek were free relative to the cap, openings plus name-hit body
    would still fit under a body-only budget of 4. Counting them forces the
    selector to stop after four total passages and drop the name-hit body.
    """
    view = ArticleViewLike(
        title="Élodie N'Diaye opens retrospective",
        dek="A major survey of the sculptor's career.",
        main_text_blocks=(
            TextBlock(id="b1", text="Opening paragraph about the show."),
            TextBlock(id="b2", text="Second opening block continues."),
            TextBlock(id="b3", text="Élodie N'Diaye is mentioned mid-article."),
            TextBlock(id="b4", text="Adjacent after name hit."),
            TextBlock(id="b5", text="Still more body beyond the cap."),
        ),
    )
    result = select_passages(
        ("Élodie N'Diaye",),
        view,
        _config(max_passage_blocks=4, opening_block_count=2),
    )

    assert [p.source_block_id for p in result.passages] == ["t0", "d0", "b1", "b2"]
    assert [p.id for p in result.passages] == ["p1", "p2", "p3", "p4"]
    assert len(result.passages) == 4
    # Positive control vs "title/dek free" mutant: name-hit body must not appear.
    assert "b3" not in {p.source_block_id for p in result.passages}
    assert result.truncated_by_blocks is True
    assert result.name_absent is False


def test_title_and_dek_count_toward_character_cap() -> None:
    """K21: long title/dek consume max_passage_characters before body."""
    view = ArticleViewLike(
        title="T" * 300,
        dek="D" * 300,
        main_text_blocks=(
            TextBlock(id="b1", text="Body block that should be truncated or dropped."),
        ),
    )
    result = select_passages(
        ("Nobody",),
        view,
        _config(
            max_passage_characters=500,
            max_passage_blocks=10,
            opening_block_count=1,
            max_title_characters=400,
            max_summary_characters=400,
        ),
    )

    assert result.passages[0].source_block_id == "t0"
    assert result.passages[0].text == "T" * 300
    assert result.passages[1].source_block_id == "d0"
    total_chars = sum(len(p.text) for p in result.passages)
    assert total_chars <= 500
    assert result.truncated_by_chars is True
    # Body cannot fully fit after title+dek consumed the budget.
    body = [p for p in result.passages if p.source_block_id == "b1"]
    assert not body or body[0].truncated


def test_name_hit_includes_adjacent_blocks() -> None:
    view = ArticleViewLike(
        title=None,
        dek=None,
        main_text_blocks=(
            TextBlock(id="b1", text="Intro without the subject."),
            TextBlock(id="b2", text="Middle without the subject."),
            TextBlock(id="b3", text="Here is Alice Smith in context."),
            TextBlock(id="b4", text="Follow-up after the hit."),
            TextBlock(id="b5", text="Far away unused block."),
        ),
    )
    result = select_passages(
        ("Alice Smith",),
        view,
        _config(opening_block_count=0, max_passage_blocks=10),
    )

    source_ids = {p.source_block_id for p in result.passages}
    assert source_ids == {"b2", "b3", "b4"}
    assert result.name_absent is False


def test_name_absent_uses_snippets_and_flags() -> None:
    view = ArticleViewLike(
        title="Unrelated headline",
        dek=None,
        main_text_blocks=(
            TextBlock(id="b1", text="No subject name appears in the body at all."),
            TextBlock(id="b2", text="Second opening block still name-free."),
        ),
        snippets=("Search snippet mentioning Alice Smith briefly.",),
    )
    result = select_passages(
        ("Alice Smith",),
        view,
        _config(opening_block_count=2),
    )

    assert result.name_absent is True
    source_ids = [p.source_block_id for p in result.passages]
    assert "t0" in source_ids
    assert "b1" in source_ids
    assert "b2" in source_ids
    assert "s1" in source_ids
    # Name is only in the snippet, not main_text — name_absent stays true.
    assert any("Alice Smith" in p.text for p in result.passages)


def test_match_key_honorific_normalization_for_name_hit() -> None:
    """Name forms use match_key (honorific strip + casefold) for containment."""
    view = ArticleViewLike(
        title=None,
        dek=None,
        main_text_blocks=(
            TextBlock(id="b1", text="Critics praise élodie n'diaye this season."),
        ),
        snippets=(),
    )
    result = select_passages(
        ("Dr. Élodie N'Diaye",),
        view,
        _config(opening_block_count=0),
    )
    assert result.name_absent is False
    assert result.passages[0].source_block_id == "b1"


def test_per_field_title_truncation_before_global_budget() -> None:
    view = ArticleViewLike(
        title="X" * 1000,
        dek=None,
        main_text_blocks=(),
    )
    result = select_passages(
        (),
        view,
        _config(max_title_characters=50, max_passage_characters=5000),
    )
    assert len(result.passages) == 1
    assert result.passages[0].text == "X" * 50
    assert result.passages[0].source_block_id == "t0"
