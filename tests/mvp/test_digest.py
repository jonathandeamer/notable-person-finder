from decimal import Decimal

import pytest

from notable.digest import DigestEntry, identity_key, render, write

ENTRY = DigestEntry(
    identity_key="ana poy",
    display_name="Ana Poy",
    source_url="https://a.test/1",
    publisher_label="Feed A",
    rationale="Named subject.",
)

COUNTS = {
    "generated_at": "2026-08-03T09:00:00+00:00",
    "status": "ok",
    "cost_usd": Decimal("0.42"),
    "n_settled": 3,
    "n_incomplete": 0,
}


def _entry(name: str, n: int = 1) -> DigestEntry:
    return DigestEntry(
        identity_key=identity_key(name),
        display_name=name,
        source_url=f"https://a.test/{n}",
        publisher_label="Feed A",
        rationale="Named subject.",
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Ana Poy", "ana poy"),
        ("  Ana   Poy  ", "ana poy"),
        ("ANA POY", "ana poy"),
        ("Ana\u00a0Poy", "ana poy"),   # NFKC folds the non-breaking space
        ("\uff21na Poy", "ana poy"),   # NFKC folds fullwidth Latin
    ],
)
def test_identity_key_normalizes(raw, expected):
    assert identity_key(raw) == expected


def test_identity_key_keeps_different_names_distinct():
    assert identity_key("Ana Poy") != identity_key("Ana Poye")


def test_render_lists_entries_with_their_sources():
    text = render([ENTRY], **COUNTS)
    assert "Ana Poy" in text
    assert "https://a.test/1" in text
    assert "Feed A" in text


def test_render_reports_run_status_and_cost():
    text = render([ENTRY], **COUNTS | {"status": "partial", "n_incomplete": 2})
    assert "Run status: **partial**" in text
    assert "$0.42" in text
    assert "Items incomplete: 2" in text


def test_empty_digest_is_still_a_valid_document():
    text = render([], **COUNTS)
    assert text.startswith("# ")
    assert "No people detected" in text


def test_a_multiline_rationale_cannot_forge_a_heading():
    # Model-supplied text reaches the artifact. Nothing parses it back, but a
    # rationale containing a line beginning "### " would still render a
    # heading for a person nobody detected.
    hostile = DigestEntry(
        "x", "Ana Poy", "https://a.test/1", "Feed A",
        "Won a prize.\n\n### Fake Person\n\n- Source: [x](https://evil.test/)",
    )
    headings = [line for line in render([hostile], **COUNTS).splitlines()
                if line.startswith("### ")]
    assert headings == ["### Ana Poy"]


def test_write_creates_the_dated_file_and_latest(tmp_path):
    path = write([ENTRY], tmp_path, **COUNTS)
    assert path.parent == tmp_path
    assert path.name == "2026-08-03.md"
    latest = tmp_path / "latest.md"
    assert latest.read_text("utf-8") == path.read_text("utf-8")


def test_write_leaves_no_temporary_file_behind(tmp_path):
    write([ENTRY], tmp_path, **COUNTS)
    assert [p.name for p in tmp_path.iterdir() if ".tmp" in p.name] == []


def test_write_replaces_an_existing_digest_for_the_same_day(tmp_path):
    # The digest renders from this run only. The spec states the cost of that
    # directly, and the `lead` log is where a dropped entry survives.
    write([ENTRY], tmp_path, **COUNTS)
    path = write([_entry("Bo Li", 2)], tmp_path, **COUNTS)
    text = path.read_text("utf-8")
    assert "Bo Li" in text
    assert "Ana Poy" not in text
    assert (tmp_path / "latest.md").read_text("utf-8") == text


def test_an_empty_run_writes_an_empty_digest(tmp_path):
    path = write([], tmp_path, **COUNTS)
    assert "No people detected" in path.read_text("utf-8")
    assert (tmp_path / "latest.md").exists()
