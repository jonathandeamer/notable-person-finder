from __future__ import annotations

import subprocess
import sys

from notable_person_finder import __version__


def test_package_has_version() -> None:
    assert __version__ == "0.1.0"


def test_module_help_lists_foundation_commands() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "notable_person_finder", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "config" in completed.stdout
    assert "paths" in completed.stdout
    assert "db" in completed.stdout
    assert completed.stderr == ""
