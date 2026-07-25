# Code Quality Tooling Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent Branch:** `refactor/rearchitecture`  
**Feature Branch:** `feat/code-quality-tooling`  

---

## 1. Overview & Goals

This specification defines the code quality and developer tooling setup for `notable-person-finder`.
The objective is to enforce consistent code formatting, import sorting, linting, static type safety, and commit conventions across the project using lightweight, modern Python 3.13 tooling managed by `uv`.

Specifically, this design introduces:
1. **Ruff**: For fast linting, import sorting (`isort`), code formatting, and modern Python syntax upgrades.
2. **Pyright**: For static type checking of the `src/` and `tests/` directories.
3. **Git Hooks (`.githooks/`)**: Local pre-commit and commit-msg scripts for automated verification before commits land.
4. **Documentation**: Clear verification guidelines added to `CLAUDE.md`.

---

## 2. Dependencies & Configuration (`pyproject.toml`)

### 2.1 Dependency Additions
Under `[dependency-groups] dev` in `pyproject.toml`:
```toml
[dependency-groups]
dev = [
    "pyright>=1.1.390",
    "pytest>=8.4,<9",
    "ruff>=0.9.0",
]
```

### 2.2 Ruff Configuration (`[tool.ruff]`)
```toml
[tool.ruff]
line-length = 88
target-version = "py313"

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "F",   # Pyflakes
    "I",   # isort (import sorting)
    "UP",  # pyupgrade (modern Python syntax)
    "B",   # flake8-bugbear
    "SIM", # flake8-simplify
]
ignore = []

[tool.ruff.lint.isort]
known-first-party = ["notable_person_finder"]
```

### 2.3 Pyright Configuration (`[tool.pyright]`)
```toml
[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "standard"
include = ["src", "tests"]
exclude = ["**/__pycache__", ".venv", ".worktrees"]
```

---

## 3. Git Hooks (`.githooks/`)

### 3.1 Pre-Commit Hook (`.githooks/pre-commit`)
An executable shell script that runs before git commits:
```bash
#!/bin/sh
set -e

echo "Running Ruff check..."
uv run ruff check .

echo "Running Ruff format check..."
uv run ruff format --check .

echo "Running Pyright type check..."
uv run pyright
```

### 3.2 Commit Message Hook (`.githooks/commit-msg`)
An executable shell script validating conventional commit prefixes:
```bash
#!/bin/sh
set -e

COMMIT_MSG_FILE="$1"
COMMIT_MSG=$(cat "$COMMIT_MSG_FILE")

# Allowed prefixes: feat, fix, docs, style, refactor, test, chore, perf, ci, build
PATTERN="^(feat|fix|docs|style|refactor|test|chore|perf|ci|build)(\([a-z0-9_-]+\))?!?: .+$"

if ! echo "$COMMIT_MSG" | grep -Eq "$PATTERN"; then
    echo "ERROR: Invalid commit message format."
    echo "Commit message must follow conventional commit subject guidelines, e.g.:"
    echo "  feat: add ruff and pyright configuration"
    echo "  fix: resolve type error in config loader"
    exit 1
fi
```

---

## 4. Environment & Verification Workflow

### 4.1 CLI Verification Commands
Developers and agents should run the following commands as part of verification:
- `uv sync`: Synchronize virtual environment with new dev dependencies.
- `uv run ruff check .`: Run linter and check for lint issues.
- `uv run ruff format .`: Format Python files.
- `uv run pyright`: Perform static type checking.
- `git config core.hooksPath .githooks`: Activate project git hooks locally.

### 4.2 Updating `CLAUDE.md`
`CLAUDE.md` will be updated under **Environment and Verification** to document:
- Running `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pyright` as part of standard verification.
- Enabling git hooks locally with `git config core.hooksPath .githooks`.

---

## 5. Implementation Boundaries & Verification Plan

1. Modify `pyproject.toml` to add dependencies and configuration blocks.
2. Update `uv.lock` by running `uv lock`.
3. Create/update `.githooks/pre-commit` and `.githooks/commit-msg` (ensure `chmod +x`).
4. Update `CLAUDE.md`.
5. Run `uv run ruff check .`, `uv run ruff format .`, and `uv run pyright` across the codebase to ensure existing foundation code passes without errors. Fix any initial formatting/type findings if needed.
6. Verify foundation tests pass clean via `uv run pytest tests/foundation`.
