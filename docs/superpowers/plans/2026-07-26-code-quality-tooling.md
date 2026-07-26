# Code Quality Tooling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Configure Ruff for formatting and linting, Pyright for static type checking, and Git hooks for commit-time enforcement in Python 3.13.

**Architecture:** Add `ruff` and `pyright` to `pyproject.toml` dev dependencies and tool configurations. Add shell scripts in `.githooks/` for pre-commit (linting/types) and commit-msg (conventional commit subjects). Update `CLAUDE.md` with developer instructions.

**Tech Stack:** Python 3.13, `uv`, `ruff`, `pyright`, Git hooks.

---

### Task 1: Add Ruff and Pyright Configuration to `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Edit `pyproject.toml` to add dev dependencies and tool settings**

Update `pyproject.toml` to include `ruff` and `pyright` in `[dependency-groups] dev`, as well as `[tool.ruff]` and `[tool.pyright]` configuration blocks.

```toml
[dependency-groups]
dev = [
    "pyright>=1.1.390",
    "pytest>=8.4,<9",
    "ruff>=0.9.0",
]

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

[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "standard"
include = ["src", "tests"]
exclude = ["**/__pycache__", ".venv", ".worktrees"]
```

- [ ] **Step 2: Sync dependencies and lockfile**

Run:
```bash
uv lock
uv sync
```
Expected output: Dependencies `ruff` and `pyright` resolved and installed in `.venv`.

- [ ] **Step 3: Commit `pyproject.toml` and `uv.lock`**

Run:
```bash
git add pyproject.toml uv.lock
git commit -m "feat: add ruff and pyright to pyproject.toml"
```

---

### Task 2: Create Executable Git Hooks (`.githooks/`)

**Files:**
- Modify: `.githooks/pre-commit`
- Create/Modify: `.githooks/commit-msg`

- [ ] **Step 1: Write `.githooks/pre-commit`**

Create or replace `.githooks/pre-commit` with:

```bash
#!/bin/sh
set -e

echo "Running Ruff check..."
uv run ruff check .

echo "Running Ruff format check..."
uv run ruff format --check .

echo "Running Pyright type check..."
uv run ruff format --check . || true
uv run pyright
```

Ensure file permissions are executable:
```bash
chmod +x .githooks/pre-commit
```

- [ ] **Step 2: Write `.githooks/commit-msg`**

Create or replace `.githooks/commit-msg` with:

```bash
#!/bin/sh
set -e

COMMIT_MSG_FILE="$1"
COMMIT_MSG=$(cat "$COMMIT_MSG_FILE")

PATTERN="^(feat|fix|docs|style|refactor|test|chore|perf|ci|build)(\([a-z0-9_-]+\))?!?: .+$"

if ! echo "$COMMIT_MSG" | grep -Eq "$PATTERN"; then
    echo "ERROR: Invalid commit message format."
    echo "Commit message must follow conventional commit subject guidelines, e.g.:"
    echo "  feat: add ruff and pyright configuration"
    echo "  fix: resolve type error in config loader"
    exit 1
fi
```

Ensure file permissions are executable:
```bash
chmod +x .githooks/commit-msg
```

- [ ] **Step 3: Test hooks locally**

Run:
```bash
git config core.hooksPath .githooks
.githooks/pre-commit
```
Expected output: Scripts run clean and exit code 0.

- [ ] **Step 4: Commit Git hooks**

Run:
```bash
git add .githooks/pre-commit .githooks/commit-msg
git commit -m "feat: add pre-commit and commit-msg git hooks"
```

---

### Task 3: Run Initial Formatting, Linting, and Type Checking Clean-Up

**Files:**
- Modify: Codebase files under `src/` or `tests/` if needed to pass linting/typing.

- [ ] **Step 1: Run Ruff format and check**

Run:
```bash
uv run ruff format .
uv run ruff check --fix .
```
Expected output: Formatting applied, lint fixes applied.

- [ ] **Step 2: Run Pyright type check**

Run:
```bash
uv run pyright
```
Expected output: 0 errors reported.

- [ ] **Step 3: Run pytest foundation tests**

Run:
```bash
uv run pytest tests/foundation
```
Expected output: All 50 foundation tests pass.

- [ ] **Step 4: Commit any clean-up changes**

Run:
```bash
git status --short
```
If any files were formatted/fixed, stage and commit them:
```bash
git add -u
git commit -m "style: apply ruff format and fix lint issues"
```
*(Skip commit if no files changed).*

---

### Task 4: Update Documentation (`CLAUDE.md`)

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update `CLAUDE.md` under Environment and Verification**

Add documentation for `uv run ruff check .`, `uv run ruff format .`, `uv run pyright`, and `git config core.hooksPath .githooks`.

In `CLAUDE.md`, locate lines under **Environment and Verification** and ensure the commands list includes:
- `uv run ruff check .`
- `uv run ruff format .`
- `uv run pyright`
- `git config core.hooksPath .githooks`

- [ ] **Step 2: Run full verification gate**

Run:
```bash
.githooks/pre-commit
uv run pytest tests/foundation
git diff --check
```
Expected output: All verification checks pass cleanly.

- [ ] **Step 3: Commit `CLAUDE.md`**

Run:
```bash
git add CLAUDE.md
git commit -m "docs: document ruff, pyright, and git hook verification commands in CLAUDE.md"
```
