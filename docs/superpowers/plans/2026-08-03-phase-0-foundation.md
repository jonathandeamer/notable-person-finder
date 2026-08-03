# Phase 0: Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up an installable `notable` package on the `mvp` branch with its tooling, ported product assets, and agent guidance — so Phase 1 can write code and nothing else.

**Architecture:** A fresh `src/notable/` package alongside the retained legacy prototype at the repository root. No code is ported from either prior version; only *assets* that encode product knowledge — three tuned prompts, the curated publisher policy, the feed list. Tooling mirrors the prior rewrite's proven configuration, minus what the MVP deleted.

**Tech Stack:** Python 3.13+, uv, pydantic v2, httpx, feedparser, python-dotenv, pytest, ruff, pyright.

## Global Constraints

Copied verbatim from `docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. Every task's requirements implicitly include this section.

- **Python 3.13 or newer.** Dependencies installed with `uv sync`.
- **Source stays under 3,000 lines.** Past that, something gets deleted or the feature does not land.
- **No new table without deleting one.** Four is the budget.
- **`pipeline.py`'s main loop fits on one screen.**
- **No new cross-run state** without an explicit decision to leave MVP scope.
- **The application never writes Wikipedia content, drafts an article, or decides that a person satisfies Wikipedia policy.** Hard invariant, not a milestone boundary.
- **Secrets never appear** in snapshots, fingerprints, diagnostics, terminal output, tests, or cache keys.
- **Commit messages follow Conventional Commits** (`.githooks/commit-msg` enforces `^(build|chore|ci|docs|feat|fix|perf|refactor|revert|style|test)(\(scope\))?: description`).
- **The legacy prototype at the repository root** (`run_pipeline.py`, `name_utils.py`, `scripts/`, `ingest/`, `prompts/`, `state/`, `output/`, `tests/test_*.py`) is the operational fallback. Do not import, reorganize, remove, run, or repair it.

---

## File Structure

| Path | Responsibility |
| --- | --- |
| `pyproject.toml` | Package metadata, dependencies, ruff/pyright/pytest configuration |
| `src/notable/__init__.py` | Package marker and version |
| `src/notable/cli.py` | Argument parsing and command dispatch (stub in Phase 0) |
| `src/notable/prompts/detect_people.md` | Ported detection prompt |
| `src/notable/prompts/match_wikipedia_identity.md` | Ported match prompt (used in Phase 2) |
| `src/notable/prompts/assess_article.md` | Ported assessment prompt (used in Phase 3) |
| `config/notable.example.toml` | Copyable main configuration example |
| `config/feeds.example.toml` | Copyable feed list |
| `config/source_policies/visual_arts.toml` | Curated publisher policy (used in Phase 3) |
| `tests/mvp/` | All MVP tests. Separate from prototype `tests/test_*.py` |
| `CLAUDE.md` | Agent guidance for the MVP, replacing the prototype's |

**Why `tests/mvp/` and not `tests/`:** `pyproject.toml`'s ruff `extend-exclude` lists `tests/test_*` to keep the prototype's tests unlinted, and those patterns match by path segment at any depth. A new test placed at `tests/test_cache.py` would be silently excluded from linting. `tests/mvp/` is unambiguous.

---

### Task 1: Package skeleton and tooling

**Files:**
- Create: `pyproject.toml`
- Create: `src/notable/__init__.py`
- Create: `src/notable/cli.py`
- Create: `tests/mvp/__init__.py`
- Create: `tests/mvp/test_cli.py`
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces: `notable.cli.main(argv: list[str] | None = None) -> int` — the console-script entry point, returning a process exit code. `notable.__version__: str`.

- [ ] **Step 1: Create the package files**

`src/notable/__init__.py`:

```python
"""notable — a personal batch tool for finding Wikipedia biography leads."""

__version__ = "0.1.0"
```

`src/notable/cli.py`:

```python
"""Argument parsing and command dispatch."""

from __future__ import annotations

import argparse

from notable import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="notable",
        description="Find inspectable Wikipedia biography leads from RSS feeds.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Fetch feeds and write a digest.")
    run.add_argument(
        "--fresh-feeds",
        action="store_true",
        help="Bypass the feed cache and refetch every feed.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "run":
        raise SystemExit("notable run is not implemented until Phase 1")
    parser.error(f"unknown command: {args.command}")
    return 2
```

`tests/mvp/__init__.py` — empty file.

- [ ] **Step 2: Write the failing test**

`tests/mvp/test_cli.py`:

```python
import pytest

from notable import __version__
from notable.cli import build_parser, main


def test_version_flag_reports_the_package_version(capsys):
    with pytest.raises(SystemExit) as exit_info:
        main(["--version"])
    assert exit_info.value.code == 0
    assert capsys.readouterr().out.strip() == __version__


def test_run_accepts_fresh_feeds_flag():
    args = build_parser().parse_args(["run", "--fresh-feeds"])
    assert args.command == "run"
    assert args.fresh_feeds is True


def test_no_command_is_an_error():
    with pytest.raises(SystemExit) as exit_info:
        build_parser().parse_args([])
    assert exit_info.value.code == 2
```

- [ ] **Step 3: Create `pyproject.toml`**

```toml
[build-system]
requires = ["hatchling>=1.27,<2"]
build-backend = "hatchling.build"

[project]
name = "notable"
version = "0.1.0"
description = "A personal batch tool for finding inspectable Wikipedia biography leads"
readme = "README.md"
requires-python = ">=3.13"
license = { file = "LICENSE" }
dependencies = [
  "feedparser>=6.0,<7",
  "httpx>=0.28,<0.29",
  "pydantic>=2.11,<3",
  "python-dotenv>=1.1,<2",
  "tzdata>=2025.2",
]

[project.scripts]
notable = "notable.cli:main"

[dependency-groups]
dev = [
    "pyright>=1.1.390",
    "pytest>=8.4,<9",
    "ruff>=0.9.0",
]

[tool.hatch.build.targets.wheel]
packages = ["src/notable"]

[tool.pytest.ini_options]
testpaths = ["tests/mvp"]
addopts = "-ra -m 'not live'"
markers = ["live: explicitly invoked tests that contact external providers"]

[tool.ruff]
line-length = 88
target-version = "py313"
# The legacy prototype is historical evidence and the operational fallback
# until product cutover; it is never linted or formatted. These patterns match
# by path segment at any depth, which is why MVP tests live in `tests/mvp/`
# rather than `tests/test_*.py`. Verify with `uv run ruff check . --show-files`.
extend-exclude = [
    "run_pipeline.py",
    "name_utils.py",
    "scripts",
    "ingest",
    "prompts",
    "output",
    "state",
    "tests/test_*",
    "tests/conftest.py",
    # `ruff format` rewrites Python code blocks inside Markdown. Specs and plans
    # are authorities: their code blocks are illustrative and must not be
    # silently reformatted into looking canonical.
    "docs",
]

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "F",   # Pyflakes
    "I",   # isort (import sorting)
    "UP",  # pyupgrade (modern Python syntax)
    "B",   # flake8-bugbear
    "SIM", # flake8-simplify
]

[tool.ruff.lint.isort]
known-first-party = ["notable"]

[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "standard"
include = ["src", "tests/mvp"]
exclude = ["**/__pycache__", ".venv", ".worktrees"]
```

- [ ] **Step 4: Append to `.gitignore`**

Append these lines:

```gitignore
# MVP runtime data
/data/
/digests/
```

- [ ] **Step 5: Install and run the tests to verify they pass**

```bash
uv sync
git config core.hooksPath .githooks
uv run pytest tests/mvp -v
```

Expected: 3 passed. If `uv sync` reports a resolution failure, that is a real blocker — report it rather than loosening a version bound.

- [ ] **Step 6: Verify the console script and the linters**

```bash
uv run notable --version
uv run ruff check .
uv run ruff format --check .
uv run pyright
```

Expected: the version prints; ruff and pyright report no errors. Confirm the prototype is excluded:

```bash
uv run ruff check . --show-files | grep -c "^scripts/"
```

Expected: `0`.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock src/notable tests/mvp .gitignore
git commit -m "build: scaffold the notable package and its tooling"
```

---

### Task 2: Port product assets

**Files:**
- Create: `src/notable/prompts/detect_people.md`
- Create: `src/notable/prompts/match_wikipedia_identity.md`
- Create: `src/notable/prompts/assess_article.md`
- Create: `config/feeds.example.toml`
- Create: `config/source_policies/visual_arts.toml`
- Create: `tests/mvp/test_assets.py`
- Modify: `pyproject.toml`

**Interfaces:**
- Consumes: `notable.__version__` from Task 1.
- Produces: prompt files readable via `importlib.resources.files("notable.prompts")`. Phase 1 Task 7 reads `detect_people.md`.

These are copied byte-for-byte from `refactor/rearchitecture`. They are tuned against real content and expensive to rediscover; **do not edit them**, including whitespace.

- [ ] **Step 1: Copy the assets from the frozen branch**

```bash
mkdir -p src/notable/prompts config/source_policies
git show refactor/rearchitecture:src/notable_person_finder/people/prompts/detect_people.md > src/notable/prompts/detect_people.md
git show refactor/rearchitecture:src/notable_person_finder/wikipedia/prompts/match_wikipedia_identity.md > src/notable/prompts/match_wikipedia_identity.md
git show refactor/rearchitecture:src/notable_person_finder/coverage/prompts/assess_article.md > src/notable/prompts/assess_article.md
git show refactor/rearchitecture:config/discovery-feeds.example.toml > config/feeds.example.toml
git show refactor/rearchitecture:config/source_policies/visual_arts.toml > config/source_policies/visual_arts.toml
```

`resolve_person_entity.md` is deliberately **not** ported. It belongs to durable person identity, which is deferred.

- [ ] **Step 2: Make the prompts package data**

Add to `pyproject.toml` after the `[tool.hatch.build.targets.wheel]` block:

```toml
[tool.hatch.build.targets.wheel.force-include]
"src/notable/prompts" = "notable/prompts"
```

Create `src/notable/prompts/__init__.py` so `importlib.resources` can address it as a package:

```python
"""Ported model prompts. Tuned against real content — do not edit."""
```

- [ ] **Step 3: Write the failing test**

`tests/mvp/test_assets.py`:

```python
import tomllib
from importlib import resources
from pathlib import Path

import pytest

PROMPTS = ("detect_people.md", "match_wikipedia_identity.md", "assess_article.md")


@pytest.mark.parametrize("name", PROMPTS)
def test_prompt_is_packaged_and_non_empty(name):
    text = resources.files("notable.prompts").joinpath(name).read_text("utf-8")
    assert text.strip(), f"{name} is empty"


def test_resolve_person_entity_prompt_is_not_ported():
    # Durable person identity is deferred; its prompt returns with it.
    assert not resources.files("notable.prompts").joinpath(
        "resolve_person_entity.md"
    ).is_file()


def test_feed_list_parses_and_has_ten_feeds():
    data = tomllib.loads(Path("config/feeds.example.toml").read_text("utf-8"))
    feeds = data["feeds"]
    assert len(feeds) == 10
    assert {"key", "label", "url"} <= set(feeds[0])
    keys = [feed["key"] for feed in feeds]
    assert len(set(keys)) == len(keys), "feed keys must be unique"


def test_source_policy_parses_and_carries_rules():
    data = tomllib.loads(
        Path("config/source_policies/visual_arts.toml").read_text("utf-8")
    )
    assert data["key"] == "visual-arts-en-sources"
    statuses = {rule["status"] for rule in data["rules"]}
    assert "curated_eligible" in statuses
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
uv sync && uv run pytest tests/mvp -v
```

Expected: 8 passed. If the prompt tests fail with `ModuleNotFoundError: notable.prompts`, Step 2's `__init__.py` is missing.

- [ ] **Step 5: Confirm the prompts are byte-identical to their source**

```bash
for p in detect_people match_wikipedia_identity assess_article; do
  case $p in
    detect_people) src=people ;;
    match_wikipedia_identity) src=wikipedia ;;
    assess_article) src=coverage ;;
  esac
  git show "refactor/rearchitecture:src/notable_person_finder/$src/prompts/$p.md" \
    | diff -q - "src/notable/prompts/$p.md" && echo "$p OK"
done
```

Expected: three `OK` lines and no diff output.

- [ ] **Step 6: Commit**

```bash
git add src/notable/prompts config/feeds.example.toml config/source_policies pyproject.toml tests/mvp/test_assets.py
git commit -m "feat: port tuned prompts, feed list, and publisher policy"
```

---

### Task 3: Replace CLAUDE.md with MVP guidance

**Files:**
- Modify: `CLAUDE.md` (full replacement)

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by code. This is the file a cold-starting agent reads first, so it must not describe the prototype as current.

The existing `CLAUDE.md` documents the twelve-gate prototype. Leaving it would point every future agent at the wrong system.

- [ ] **Step 1: Replace the file entirely**

```markdown
# Repository Agent Guide

## Current Development Direction

This repository is being rebuilt as `notable`: a small, maintainable
application implementing the core "find promising leads" loop. Current
development belongs in `src/notable/`.

The single architectural authority is
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`. Read it before
implementing. `docs/findings.md` records provider and model facts that cost
real money to discover — read it before touching schema construction or prompt
rendering.

Two prior versions exist and **neither is a base for this one**:

- The **legacy prototype** at the repository root (`run_pipeline.py`,
  `name_utils.py`, `scripts/`, `ingest/`, `prompts/`, `state/`, `output/`,
  `tests/test_*.py`) is the operational fallback until cutover. Do not import,
  reorganize, remove, run, or repair it. Its tests are not a gate.
- The **`refactor/rearchitecture` branch** is a complete, working rewrite that
  succumbed to the second-system effect: 38,608 lines of source, 71,334 of
  tests, 45 SQLite tables. It is frozen reference. Consult it for product
  reasoning; do not port its code.

## Hard Guardrails

The prior failure mode was gradual accretion in which every individual step
looked justified. These limits are deliberately uncomfortable. When one binds,
the first response is to ask what can be removed — not to raise the limit.

1. **Source stays under 3,000 lines.** Check with
   `find src -name '*.py' | xargs wc -l | tail -1`.
2. **No new table without deleting one.** Four is the budget: `item`,
   `surfaced`, `run`, `lead`.
3. **`pipeline.py`'s main loop fits on one screen.**
4. **No new cross-run state** without an explicit decision to leave MVP scope.

Two further rules from the spec that are easy to erode:

- **Consume less than the prompt emits.** Ported prompts return more than the
  rules use. Store all of it in `lead.detail_json`; consume only what an
  outcome or ranking rule needs. Every consumed field is a concept the reader
  holds and a branch the tests cover.
- **`run` and `lead` are logs the pipeline never reads.** They are written
  outside the state transaction, best-effort. If code starts reading `lead`, or
  it moves inside the state transaction, the durable-workflow system is growing
  back and that needs an explicit decision.

## Foundation Invariants

- **The application never edits Wikipedia.** Every publication action requires
  explicit human review. This is not a milestone boundary.
- Distribution and import package: `notable`; executable: `notable`.
- Configuration is file-first; environment variables supply secrets only.
  Secrets must never appear in diagnostics, terminal output, tests, or cache
  keys.
- **Recovery is replay, not reconstruction.** Nothing durable is written until
  the digest file exists. A crash before `store.commit` leaves the database
  untouched and the re-run replays from cache.

## Environment and Verification

- Python 3.13 or newer. `uv sync` to install.
- `git config core.hooksPath .githooks` (Conventional Commits are enforced).
- Checks: `uv run ruff check .`, `uv run ruff format .`, `uv run pyright`.
- Tests: `uv run pytest tests/mvp`. Default `addopts` deselect `live`.
- MVP tests live in `tests/mvp/`, never `tests/test_*.py` — ruff excludes the
  latter by path segment at any depth to keep the prototype unlinted.
- Live smokes are opt-in: `uv run pytest tests/mvp -m live -v`. An adjacent
  `.env` supplies keys; `set -a && . ./.env && set +a` before running. Never
  pass a key inline where it lands in shell history.

**A written live smoke is not evidence until it has actually run.** The prior
programme's two most expensive defects were invisible offline and survived two
independent static reviews.

## Testing

Target roughly a 1:1 test-to-source ratio. The spec's "Named invariant tests"
table lists the rules that are cheap to break and expensive to notice — each
fails as *missing output* rather than as a crash. Every one must fail if its
rule is removed.

The prior programme's mutation-evidence protocol is **dropped**. It was a
rational response to a 71,000-line suite whose coverage claims could not be
trusted; at this size it is friction without a corresponding risk.

## Branch and Review Workflow

`mvp` is the integration branch for this rebuild. Implement on a focused
feature branch, open a pull request against `mvp`, and have a separate agent
review it.

Do not push, open or modify pull requests, merge, or target `main` without
explicit user authorization. `main` remains the operational fallback.
```

- [ ] **Step 2: Verify no stale prototype guidance survives**

```bash
grep -nE "gate[0-9]|Gate [0-9]|codex-cli|run_pipeline\.py --|notable_person_finder" CLAUDE.md
```

Expected: no output except the single `run_pipeline.py` mention inside the legacy-prototype paragraph. Any `Gate N` pipeline description means the replacement was partial.

- [ ] **Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: replace prototype agent guide with MVP guidance"
```

---

## Phase 0 completion gate

Run all of it. Every command must pass before Phase 1 starts.

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest tests/mvp -v
uv run notable --version
git status --short
git diff --check
```

Expected: 8 tests pass, no lint or type errors, the version prints, and the
working tree is clean.

Source line count (should be roughly 60 — the skeleton only):

```bash
find src -name '*.py' | xargs wc -l | tail -1
```
