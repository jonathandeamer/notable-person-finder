"""Cross-cutting guards for milestone 6b-i: layering, lock, migration, redaction."""

from __future__ import annotations

import ast
import re
import sqlite3
from pathlib import Path

import pytest

from notable_person_finder.cli import main as cli_main
from notable_person_finder.config.loader import load_config
from tests.audit.helpers import insert_run, migrated_database

AUDIT_PACKAGE = Path("src/notable_person_finder/audit")
FORBIDDEN = {
    "reporting",
    "leads",
    "coverage",
    "wikipedia",
    "people",
    "ingestion",
    "runs",
    "providers",
}

# K5: a qualified reference of this shape would mean the CLI re-rendered a
# digest instead of reading persisted bytes. Word-bounded so it does not
# false-positive on unrelated attribute access such as a local variable
# named `reporting` with a `.digest_path` or `.digest_sha256` field (which
# `render.py` legitimately has -- `ReportingResult.digest_path`).
_REPORTING_DIGEST_REFERENCE = re.compile(r"reporting\.digest\b")

# The other idiomatic way to reach the digest renderer: importing the name
# directly out of the `reporting` package rather than qualifying it. Handles
# both the bare form and a parenthesised multi-name import list.
_REPORTING_DIGEST_IMPORT_STATEMENT = re.compile(
    r"from\s+notable_person_finder\.reporting\s+import\s+(?:\([^)]*)?\bdigest\b"
)


def _imported_sibling_packages(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            parts = node.module.split(".")
            if parts[0] == "notable_person_finder" and len(parts) > 1:
                found.add(parts[1])
        elif isinstance(node, ast.Import):
            for alias in node.names:
                parts = alias.name.split(".")
                if parts[0] == "notable_person_finder" and len(parts) > 1:
                    found.add(parts[1])
    return found


def test_import_detector_finds_sibling_packages_in_a_synthetic_file(
    tmp_path: Path,
) -> None:
    # Positive control for `_imported_sibling_packages` itself, independent
    # of what `audit/` currently happens to import. `audit/` today imports
    # no sibling `notable_person_finder` package at all -- only stdlib and
    # its own submodules -- so a control that demanded `db` or `config`
    # show up by scanning the real package would fail for a reason that has
    # nothing to do with the detector. Writing a synthetic file with known
    # imports proves the AST walk, the `ImportFrom`/`Import` branches, and
    # the dotted-path splitting all work, so the "nothing forbidden found"
    # result below is a real negative rather than a detector that silently
    # returns an empty set.
    synthetic = tmp_path / "synthetic_imports.py"
    synthetic.write_text(
        "from notable_person_finder.db.connection import connect_database\n"
        "import notable_person_finder.config.loader\n"
        "from notable_person_finder.reporting import digest\n",
        encoding="utf-8",
    )
    detected = _imported_sibling_packages(synthetic)
    assert detected == {"db", "config", "reporting"}


# The forbidden-import loops below iterate whatever the glob returns. If
# AUDIT_PACKAGE were misspelled, pointed at the wrong directory, or the glob
# pattern were wrong, that loop would iterate zero files and both guard
# tests would pass vacuously -- the exact failure mode the synthetic-file
# positive control (above) closed one level down. This is the matching
# control one level up: known filenames that must be present.
_EXPECTED_AUDIT_SOURCE_FILENAMES = {
    "models.py",
    "registry.py",
    "repository.py",
    "render.py",
    "digest_show.py",
}


def _audit_source_files() -> list[Path]:
    return sorted(AUDIT_PACKAGE.glob("*.py"))


def test_audit_package_glob_discovers_the_real_source_files() -> None:
    # Positive control for the file-discovery step itself, independent of
    # what any individual file does or doesn't import. Without this, a
    # broken AUDIT_PACKAGE path or glob pattern would make every test below
    # that loops over `AUDIT_PACKAGE.glob("*.py")` pass by iterating nothing.
    sources = _audit_source_files()
    assert sources, f"no .py files found under {AUDIT_PACKAGE}"
    discovered = {source.name for source in sources}
    assert discovered >= _EXPECTED_AUDIT_SOURCE_FILENAMES


def test_audit_package_imports_no_forbidden_sibling() -> None:
    # `audit/` is documented (see its `models.py` module docstring) as
    # permitted to import `config/`, `db/`, and `obs/` -- it just does not
    # currently need to, reading every other package's tables with its own
    # SQL instead of importing them. This test is the negative check; the
    # detector itself is proven by the synthetic-file positive control
    # above, and the file-discovery step by the glob control above.
    sources = _audit_source_files()
    assert sources, f"no .py files found under {AUDIT_PACKAGE}"
    for source in sources:
        assert not (_imported_sibling_packages(source) & FORBIDDEN), source


def test_audit_does_not_import_the_digest_renderer() -> None:
    # K5: digest show reads bytes; it must never re-render. Covers both the
    # dotted-attribute form (`reporting.digest`) and the
    # `from notable_person_finder.reporting import digest` form. A bare
    # `import notable_person_finder.reporting` (no `digest` mentioned in the
    # text at all) is not this guard's job -- the AST layering scan above
    # already forbids importing `reporting` in any form whatsoever.
    sources = _audit_source_files()
    assert sources, f"no .py files found under {AUDIT_PACKAGE}"
    for source in sources:
        text = source.read_text(encoding="utf-8")
        assert not _REPORTING_DIGEST_REFERENCE.search(text), source
        assert not _REPORTING_DIGEST_IMPORT_STATEMENT.search(text), source


def test_commands_succeed_while_another_handle_holds_the_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # K2: read-only commands take no mutation lock.
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
        connection.execute("BEGIN IMMEDIATE")

        status = cli_main.command_audit_run(
            config_file, run_id_argument=str(run_id), attempt_id_argument=None
        )
        assert status == cli_main.EXIT_OK
        connection.rollback()
    finally:
        connection.close()
    capsys.readouterr()


def test_audit_does_not_migrate_an_out_of_date_database(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # K3: the schema must be unchanged after the command runs.
    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.execute("DROP TABLE digest_entry")
        connection.execute("DROP TABLE digest_queue")
        connection.commit()
        before = connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'"
        ).fetchone()[0]
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )
    assert status == cli_main.EXIT_OK

    database = load_config(config_file, require_secrets=False).paths.database
    check = sqlite3.connect(str(database))
    try:
        after = check.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'"
        ).fetchone()[0]
    finally:
        check.close()
    assert after == before
    capsys.readouterr()


def test_secrets_never_appear_in_audit_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """K14. Positive control included: an assertion that a sentinel is
    absent proves nothing unless the sentinel was reachable in the first
    place."""
    brave_sentinel = "brave-sentinel-6b1i-do-not-leak"
    openrouter_sentinel = "openrouter-sentinel-6b1i-do-not-leak"
    monkeypatch.setenv("BRAVE_API_KEY", brave_sentinel)
    monkeypatch.setenv("OPENROUTER_API_KEY", openrouter_sentinel)

    # Positive control: the sentinels ARE readable in this process, so a
    # later "not in output" assertion is meaningful rather than vacuous.
    import os

    assert os.environ["BRAVE_API_KEY"] == brave_sentinel
    assert os.environ["OPENROUTER_API_KEY"] == openrouter_sentinel

    config_file, connection = migrated_database(tmp_path)
    try:
        run_id = insert_run(connection)
        connection.commit()
    finally:
        connection.close()

    status = cli_main.command_audit_run(
        config_file, run_id_argument=str(run_id), attempt_id_argument=None
    )

    captured = capsys.readouterr()
    assert status == cli_main.EXIT_OK
    assert brave_sentinel not in captured.out
    assert brave_sentinel not in captured.err
    assert openrouter_sentinel not in captured.out
    assert openrouter_sentinel not in captured.err
    # The configuration section really did render; without this the test
    # could pass on empty output.
    assert "configuration" in captured.out.lower()
