# Rewrite Agent Guidance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make repository agent instructions rewrite-first, establish `CLAUDE.md` as the canonical guide, and expose the same guide through an `AGENTS.md` symlink.

**Architecture:** Replace the legacy-oriented `CLAUDE.md` with a concise routing and safety guide for the clean-slate package and redesign programme. Keep one source of instruction truth by pointing `AGENTS.md` to `CLAUDE.md`, and make reviewed pull requests into `refactor/rearchitecture` the default rewrite integration path.

**Tech Stack:** Markdown, Git symbolic links, shell validation.

## Global Constraints

- `CLAUDE.md` is the canonical instruction file.
- `AGENTS.md` is a relative symbolic link whose target is exactly `CLAUDE.md`.
- Rewrite pull requests target `refactor/rearchitecture` by default.
- A separate agent independently reviews each rewrite pull request before merge.
- Pushing, opening a pull request, merging, and targeting `main` require explicit user authorization.
- `main` receives the rewrite only through the later explicit cutover decision.
- The application never edits Wikipedia automatically; every Wikipedia edit or publication action requires explicit human review and approval.
- An adjacent `.env` may fill missing secret values, but the process environment takes precedence.
- Prototype code and tests are historical evidence, remain untouched unless explicitly requested, and are not rewrite verification gates.
- Active guidance must not present `run_pipeline.py`, legacy scripts, JSONL state, unittest, or the prototype gate sequence as rewrite conventions.

---

## File Structure

```text
CLAUDE.md  # canonical, rewrite-first repository instructions
AGENTS.md  # relative symlink to CLAUDE.md
```

## Task 1: Replace and Alias Repository Agent Guidance

**Files:**
- Modify: `CLAUDE.md`
- Create: `AGENTS.md` as a symbolic link to `CLAUDE.md`

**Interfaces:**
- Produces: identical repository instructions when an agent reads either `CLAUDE.md` or `AGENTS.md`.
- Produces: a reviewed-PR integration policy for rewrite feature branches.

- [ ] **Step 1: Replace `CLAUDE.md` with rewrite-first guidance**

Replace the legacy guide with concise sections covering:

- the clean-slate package and the authoritative redesign documents;
- rewrite directories, `uv` commands, focused milestone verification, and the
  installed `notable` CLI;
- permanent foundation invariants, including strict secret handling, adjacent
  `.env` fallback below process-environment precedence, checked migrations,
  OS-managed mutation locking, and the prohibition on automatic Wikipedia
  edits or publication without explicit human review and approval;
- feature pull requests into `refactor/rearchitecture`, independent agent
  review, blocking-finding remediation, and explicit authorization before
  publishing or merging;
- a short legacy-prototype section that preserves the prototype as historical
  evidence and fallback without treating its architecture or tests as rewrite
  conventions.

The approved result is the canonical `CLAUDE.md`; do not retain a verbatim copy
of its body in this executed plan because that copy would become a stale second
source of instructions.

- [ ] **Step 2: Create the relative symlink**

Run:

```bash
ln -s CLAUDE.md AGENTS.md
```

Expected: `AGENTS.md` is a symlink, not a duplicate regular file.

- [ ] **Step 3: Validate the instruction alias and content**

Run:

```bash
test -L AGENTS.md
test "$(readlink AGENTS.md)" = "CLAUDE.md"
cmp -s AGENTS.md CLAUDE.md
git diff --check
git status --short
```

Expected:

- all validation commands exit `0`;
- `git status --short` lists only the intended `CLAUDE.md`, `AGENTS.md`, and
  plan-related changes;
- reading either instruction filename yields identical content.

- [ ] **Step 4: Review the guide against the approved design**

Confirm all of these explicitly:

- the active guide is rewrite-first;
- the architecture and command references point to the rewrite;
- automatic Wikipedia editing/publication is prohibited without explicit human
  review and approval;
- adjacent `.env` secrets fill only missing values and never override the
  process environment;
- pull requests target `refactor/rearchitecture` by default;
- independent agent review and explicit merge authorization are required;
- `main` remains behind the explicit cutover boundary;
- the only legacy material is the short historical-prototype section;
- no active instruction assumes the prototype's gate structure, scripts,
  JSONL state, unittest workflow, or dependencies apply to the rewrite.

- [ ] **Step 5: Commit the repository guidance**

```bash
git add CLAUDE.md AGENTS.md docs/superpowers/plans/2026-07-25-rewrite-agent-guidance.md
git commit -m "docs: address agent guidance review"
```
