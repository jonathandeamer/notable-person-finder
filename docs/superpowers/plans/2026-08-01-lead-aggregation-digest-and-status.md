# Milestone 6a-iv: Lead Aggregation — Digest Rendering, `notable status`, and Completion Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the digest's hardcoded shortlist placeholder with a real
ranked shortlist and queue-flow run-summary block, add `notable status`'s
digest-backlog/oldest-pending-candidate lines (closing the gap tracked since
milestone 3b1), extend `pyproject.toml`'s pyright include list (K12), add
same-run-firing and fingerprint-reuse regression tests, and run milestone
6a's full completion gate.

**Architecture:** Follows the existing `CoverageSummary`/`_coverage_summary`/
`_coverage_schema_present` pattern in `reporting/digest.py` and
`cli/main.py`: new `ShortlistEntry`/`QueueFlowSummary` dataclasses, a
guarded render block, and a `_leads_summary`/`_leads_schema_present` pair
that queries `digest_queue`, `lead_assessment`, and `queue_transition`
(ranked via 6a-i's `rank_key`). This is the last of milestone 6a's four
plans; after it, `uv run pytest tests/foundation tests/run_engine
tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads`
plus ruff/pyright is the full milestone 6a completion gate.

**Tech Stack:** Python 3.13, SQLite (stdlib `sqlite3`), pytest, ruff,
pyright, `uv`.

**Plan sequence:** This is plan 4 of 4. Depends on 6a-i, 6a-ii, and 6a-iii
all being merged — a live `notable run` must already produce real
`lead_assessment`/`digest_queue`/`queue_transition` rows (6a-iii) before
this plan's rendering and status lines have anything real to show.

## Global Constraints

- Never fabricate a queue-flow rate or clear-time estimate — `None` must
  render the literal `"insufficient history"` / `"not clearing"` fallback
  text, per the design spec's explicit prohibition.
- No `compose_lead_summary` model synthesis, no `notable digest show`, no
  `notable audit *` in this plan — deferred to milestone 6b. The shortlist's
  deterministic fallback rendering (built from outcome, article titles,
  attention signals, and caveats) must still be genuinely useful standalone.
- Full design authority:
  `docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
  (K1–K13), especially Digest Rendering and `notable status`.
- Milestone 6a's full completion gate (this plan's final task):
  `uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads`,
  `uv run ruff check .`, `uv run ruff format .`, `uv run pyright`. Per
  CLAUDE.md's "Test Evidence" section, completion also requires mutating
  each K-numbered rule's source, confirming the named test fails, and
  restoring — not just a green run.

---

## File Structure (this plan's scope)

```
src/notable_person_finder/reporting/digest.py   # modify: real shortlist, queue-flow run-summary block
src/notable_person_finder/cli/main.py           # modify: _leads_summary, _leads_schema_present, notable status lines
pyproject.toml                                    # modify: pyright include list (K12)

tests/leads/
    test_digest_status.py
    test_run_cli.py            # extend: same-run firing regression
    test_service.py            # extend: fingerprint-reuse regression
```

---

## Task 12: Digest shortlist section and queue-flow run summary

**Files:**
- Modify: `src/notable_person_finder/reporting/digest.py` (replace hardcoded
  placeholder at lines 172-178; add `### Digest queue` run-summary block)
- Modify: `src/notable_person_finder/cli/main.py` (add `_leads_summary`,
  `_leads_schema_present`, following `_coverage_summary`/
  `_coverage_schema_present` at lines 747-772; wire into the `write_digest`/
  `render_digest` call sites)
- Test: `tests/leads/test_digest_status.py`

**Interfaces:**
- Produces: `ShortlistEntry`, `QueueFlowSummary` dataclasses in
  `digest.py`; `render_digest(..., shortlist_entries: list[ShortlistEntry]
  | None = None, queue_flow: QueueFlowSummary | None = None)`.

- [ ] **Step 1: Write the failing digest test with distinct per-counter values**

Follow the milestone-5 `test_digest_status.py` pattern explicitly named in
the design spec: build a fixture with nine-or-more distinct counter values
(1 through N) so a swapped-field defect is caught, and assert each one's
exact rendered line.

```python
# tests/leads/test_digest_status.py
"""Digest shortlist and queue-flow rendering, mirroring the milestone-5
test_digest_status.py fixture-with-distinct-values pattern so swapped-field
defects are caught."""
from notable_person_finder.reporting.digest import (
    QueueFlowSummary,
    ShortlistEntry,
    render_digest,
)


def _build_report(**overrides):
    # Reuse the same RunReport-construction helper tests/coverage's
    # test_digest_status.py uses (import it if factored into a shared
    # conftest, otherwise duplicate its exact minimal-field construction).
    from tests.coverage.test_digest_status import _minimal_run_report  # adjust import to actual helper location

    return _minimal_run_report(**overrides)


def test_shortlist_renders_ranked_entries_with_distinct_fields():
    report = _build_report()
    entries = [
        ShortlistEntry(
            person_id=1,
            display_name="Person One",
            outcome="promising_lead",
            eligibility_reason="new",
            wikipedia_outcome="no_matching_page_found",
            qualifying_domain_count=2,
            qualifying_sources=(
                ("example.com", "Title One", "2026-07-01", "article", "significant", "full"),
            ),
            attention_signals=("Award nomination",),
            caution_signals=(),
            unresolved_issues=(),
        )
    ]
    output = render_digest(report, shortlist_entries=entries)
    assert "Person One" in output
    assert "promising_lead" in output
    assert "example.com" in output
    assert "Award nomination" in output


def test_shortlist_placeholder_text_absent_when_entries_present():
    report = _build_report()
    entries = [
        ShortlistEntry(
            person_id=1, display_name="X", outcome="possible_lead",
            eligibility_reason="new", wikipedia_outcome="uncertain_identity",
            qualifying_domain_count=1, qualifying_sources=(), attention_signals=(),
            caution_signals=(), unresolved_issues=(),
        )
    ]
    output = render_digest(report, shortlist_entries=entries)
    assert "No candidates met the shortlist criteria" not in output


def test_shortlist_placeholder_text_present_when_no_entries():
    report = _build_report()
    output = render_digest(report, shortlist_entries=[])
    assert "No candidates met the shortlist criteria in this window." in output


def test_queue_flow_block_renders_distinct_counters():
    report = _build_report()
    flow = QueueFlowSummary(
        newly_queued=1, emitted=2, removed_matching_wikipedia=3,
        ending_backlog_promising=4, ending_backlog_possible=5,
        arrival_rate_7d=6.0, emission_rate_7d=7.0,
        net_queue_growth=8, oldest_pending_days=9,
        estimated_clear_days=None,
    )
    output = render_digest(report, queue_flow=flow)
    for expected in ("1", "2", "3", "4", "5", "6.0", "7.0", "8", "9"):
        assert expected in output
    assert "insufficient history" in output.lower() or "not clearing" in output.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_digest_status.py -v`
Expected: FAIL — `ImportError: cannot import name 'ShortlistEntry'`.

- [ ] **Step 3: Implement digest.py changes**

Add dataclasses near `CoverageSummary` (after line ~144):

```python
@dataclass(frozen=True, slots=True)
class ShortlistEntry:
    person_id: int
    display_name: str
    outcome: str
    eligibility_reason: str
    wikipedia_outcome: str | None
    qualifying_domain_count: int
    qualifying_sources: tuple[tuple[str, str, str, str, str, str], ...]
    attention_signals: tuple[str, ...]
    caution_signals: tuple[str, ...]
    unresolved_issues: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class QueueFlowSummary:
    newly_queued: int
    emitted: int
    removed_matching_wikipedia: int
    ending_backlog_promising: int
    ending_backlog_possible: int
    arrival_rate_7d: float | None
    emission_rate_7d: float | None
    net_queue_growth: int
    oldest_pending_days: int | None
    estimated_clear_days: int | None
```

Add both as `Optional[...] = None` kwargs to `render_digest(...)`'s
signature (matching the existing `coverage: CoverageSummary | None = None`
kwarg style at line ~155) and to `write_digest(...)`'s mirrored signature
(~462-473).

Replace lines 172-178 (the hardcoded placeholder):

```python
        "**Finished:** {report.finished_at}",
        "",
        "## Shortlist",
        "",
    ]

    if shortlist_entries:
        for entry in shortlist_entries:
            lines.append(f"### {entry.display_name} — {entry.outcome}")
            lines.append(f"- Why shown: {entry.eligibility_reason}")
            lines.append(
                f"- Wikipedia: {entry.wikipedia_outcome or 'not researched'}"
            )
            lines.append(f"- Qualifying domains: {entry.qualifying_domain_count}")
            for domain, title, date, content_type, depth, visibility in entry.qualifying_sources:
                lines.append(
                    f"  - {domain} — \"{title}\" ({date}, {content_type}, "
                    f"{depth}, {visibility})"
                )
            for signal in entry.attention_signals:
                lines.append(f"- Attention: {signal}")
            for signal in entry.caution_signals:
                lines.append(f"- Caution: {signal}")
            for issue in entry.unresolved_issues:
                lines.append(f"- Unresolved: {issue}")
            lines.append("")
    else:
        lines += ["No candidates met the shortlist criteria in this window.", ""]
```

Add the queue-flow run-summary block near the existing `### Coverage
evidence` section (after line ~362):

```python
    if queue_flow is not None:
        lines.append("### Digest queue")
        lines.append(f"- Newly queued: {queue_flow.newly_queued}")
        lines.append(f"- Emitted: {queue_flow.emitted}")
        lines.append(
            f"- Removed (matched Wikipedia): {queue_flow.removed_matching_wikipedia}"
        )
        lines.append(
            "- Ending backlog: "
            f"{queue_flow.ending_backlog_promising} promising, "
            f"{queue_flow.ending_backlog_possible} possible"
        )
        arrival = (
            f"{queue_flow.arrival_rate_7d:.1f}/day"
            if queue_flow.arrival_rate_7d is not None
            else "insufficient history"
        )
        emission = (
            f"{queue_flow.emission_rate_7d:.1f}/day"
            if queue_flow.emission_rate_7d is not None
            else "insufficient history"
        )
        lines.append(f"- 7-day arrival rate: {arrival}")
        lines.append(f"- 7-day emission rate: {emission}")
        lines.append(f"- Net queue growth: {queue_flow.net_queue_growth}")
        oldest = (
            f"{queue_flow.oldest_pending_days} days"
            if queue_flow.oldest_pending_days is not None
            else "no pending entries"
        )
        lines.append(f"- Oldest pending: {oldest}")
        clear = (
            f"{queue_flow.estimated_clear_days} days"
            if queue_flow.estimated_clear_days is not None
            else "not clearing"
        )
        lines.append(f"- Estimated clear time: {clear}")
        lines.append("")
```

Never fabricate a rate or clear-time estimate — `None` must render the
literal `"insufficient history"` / `"not clearing"` fallback text, per the
design spec's explicit prohibition.

- [ ] **Step 4: Add `_leads_summary` / `_leads_schema_present` in `cli/main.py`**

Mirror `_coverage_summary`/`_coverage_schema_present` exactly (lines
747-772):

```python
def _leads_schema_present(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'digest_queue'"
    ).fetchone()
    return row is not None


def _leads_summary(
    connection: sqlite3.Connection,
    report: RunReport,
    *,
    config: MainConfig,
    now: str,
) -> tuple[list[ShortlistEntry], QueueFlowSummary] | None:
    if not _leads_schema_present(connection):
        return None
    from notable_person_finder.leads.ranking import rank_key
    from notable_person_finder.leads.repository import fetch_pending_queue_entries

    # Build ShortlistEntry list ranked by rank_key, limited to
    # config.tasks.aggregate_lead.digest_limit, and the QueueFlowSummary from
    # queue_transition rows for this run and the trailing 7/30-day windows.
    # This function's query bodies are intentionally left for the
    # implementer to fill in against the real digest_queue/queue_transition/
    # lead_assessment schema (Task 1/6) rather than guessed here — follow
    # the exact join style _coverage_summary uses for its corpus/run counts.
    ...
```

Do not leave the `...` body in the merged code — this stub exists only to
mark where the implementer must write real SQL joining `digest_queue`,
`lead_assessment`, `lead_assessment_qualifying_article`, and
`article_assessment_signal` (for `qualifying_sources`/`attention_signals`/
`caution_signals`) plus `queue_transition` (for the flow counters), using
`rank_key` from Task 3 to order entries before slicing to `digest_limit`.
The step is not complete until this function returns real data and Step 5's
tests pass without mocking the query result.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_digest_status.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/notable_person_finder/reporting/digest.py src/notable_person_finder/cli/main.py tests/leads/test_digest_status.py
git commit -m "feat(digest): real shortlist and queue-flow sections"
```

---

## Task 13: `notable status` backlog/tier/oldest-pending lines

**Files:**
- Modify: `src/notable_person_finder/cli/main.py`'s `command_status`
  (replace the placeholder comment at lines 948-949)
- Test: `tests/leads/test_digest_status.py` (extend)

**Interfaces:**
- Consumes: `_leads_schema_present` (Task 12).

- [ ] **Step 1: Write the failing test**

```python
# append to tests/leads/test_digest_status.py
def test_notable_status_prints_backlog_by_tier(tmp_path, capsys):
    # Reuse the same temp-config/temp-db fixture as an existing
    # tests/coverage/test_digest_status.py `notable status` test — port its
    # setup exactly, then seed one promising_lead and one possible_lead
    # digest_queue row via notable_person_finder.leads.repository helpers,
    # invoke command_status, and assert on stdout via capsys:
    #   assert "backlog: 1 promising_lead" in captured.out
    #   assert "oldest pending" in captured.out
    import pytest

    pytest.skip(
        "port the exact command_status test fixture from "
        "tests/coverage/test_digest_status.py before this task is done"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/leads/test_digest_status.py -k backlog -v`
Expected: FAIL (skip, then real failure once fixture ported).

- [ ] **Step 3: Replace the placeholder comment in `command_status`**

```python
        if _leads_schema_present(connection):
            backlog = connection.execute(
                """
                SELECT tier, COUNT(*)
                FROM digest_queue
                WHERE status = 'pending'
                GROUP BY tier
                """
            ).fetchall()
            backlog_by_tier = {tier: count for tier, count in backlog}
            promising = backlog_by_tier.get("promising_lead", 0)
            possible = backlog_by_tier.get("possible_lead", 0)
            print(f"digest backlog: {promising} promising_lead, {possible} possible_lead")
            oldest = connection.execute(
                "SELECT MIN(first_pending_at) FROM digest_queue WHERE status = 'pending'"
            ).fetchone()[0]
            if oldest is not None:
                print(f"oldest pending candidate: {oldest}")
            else:
                print("oldest pending candidate: none")
        return EXIT_OK
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_digest_status.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/cli/main.py tests/leads/test_digest_status.py
git commit -m "feat(status): digest backlog and oldest-pending-candidate lines"
```

---

## Task 14: `pyproject.toml` pyright include list (K12)

**Files:**
- Modify: `pyproject.toml` (lines 81-91)

- [ ] **Step 1: Edit the include list**

```toml
[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "standard"
include = [
    "src",
    "tests/foundation",
    "tests/run_engine",
    "tests/ingestion",
    "tests/people",
    "tests/wikipedia",
    "tests/coverage",
    "tests/leads",
]
exclude = ["**/__pycache__", ".venv", ".worktrees"]
```

- [ ] **Step 2: Run pyright to confirm no new failures were hidden by the prior gap**

Run: `uv run pyright`
Expected: passes, or reveals real type errors in `tests/wikipedia`/
`tests/coverage` that were previously unchecked — fix any that surface
before proceeding (per K12's rationale: this is exactly the gap that let
`f2e9e76`'s type violation ship unseen).

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "fix(config): extend pyright include to tests/wikipedia, tests/coverage, tests/leads (K12)"
```

---

## Task 15: End-to-end same-run firing test and fingerprint reuse test

**Files:**
- Test: `tests/leads/test_run_cli.py` (extend), `tests/leads/test_service.py`
  (extend)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the same-run firing test**

```python
# append to tests/leads/test_run_cli.py
def test_aggregation_fires_in_same_run_coverage_settles():
    """Direct regression test for the class of bug fixed this session in K5
    (see CLAUDE.md's 'Test Evidence' section), applied to this milestone's
    own scheduling hook. A single `notable run` invocation that completes a
    coverage plan must produce a lead_assessment row in that same run_id,
    not merely on the next run."""
    # Port the exact `notable run` CLI-invocation fixture from
    # tests/coverage/test_run_cli.py's own same-run K5 regression test
    # (search that file for its Wikipedia-to-coverage same-run assertion and
    # mirror its structure), then assert:
    #   lead_row = connection.execute(
    #       "SELECT run_id FROM lead_assessment WHERE person_id = ?", (person_id,)
    #   ).fetchone()
    #   assert lead_row[0] == coverage_completion_run_id
    import pytest

    pytest.skip("port fixture from tests/coverage/test_run_cli.py before this task is done")
```

- [ ] **Step 2: Write the fingerprint-reuse test**

```python
# append to tests/leads/test_service.py
def test_unchanged_fingerprint_does_not_reaggregate(connection, empty_policy):
    from notable_person_finder.leads.service import _compute_material_fingerprint
    from notable_person_finder.config.models import MainConfig

    fp1 = _compute_material_fingerprint(
        article_assessment_ids=[1, 2], signal_ids=[], wikipedia_outcome="no_matching_page_found",
        policy=empty_policy, config=MainConfig(),
    )
    fp2 = _compute_material_fingerprint(
        article_assessment_ids=[2, 1], signal_ids=[], wikipedia_outcome="no_matching_page_found",
        policy=empty_policy, config=MainConfig(),
    )
    assert fp1 == fp2  # order-independent (sorted before hashing)


def test_changed_evidence_changes_fingerprint(connection, empty_policy):
    from notable_person_finder.leads.service import _compute_material_fingerprint
    from notable_person_finder.config.models import MainConfig

    fp1 = _compute_material_fingerprint(
        article_assessment_ids=[1], signal_ids=[], wikipedia_outcome="no_matching_page_found",
        policy=empty_policy, config=MainConfig(),
    )
    fp2 = _compute_material_fingerprint(
        article_assessment_ids=[1, 2], signal_ids=[], wikipedia_outcome="no_matching_page_found",
        policy=empty_policy, config=MainConfig(),
    )
    assert fp1 != fp2
```

Wire `_compute_material_fingerprint` into the handler's `prepare` (Task 7):
compare against a stored fingerprint column if the design calls for
persisting one on `digest_queue` or a dedicated tracking row — re-check
whether the design spec's K6 implies a persisted fingerprint column that
Task 1's migration is missing. If so, add a `material_fingerprint TEXT`
column to `digest_queue` in Task 1's migration (revise Task 1 before
finishing this task) and have `prepare` raise the same
`ValueError(f"{...PREPARE_REFUSED_PREFIX}...")` local-refusal pattern used
by `wikipedia/service.py`'s `prepare` when the fingerprint is unchanged.

- [ ] **Step 3: Run tests to verify they pass**

Run: `uv run pytest tests/leads -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add tests/leads/
git commit -m "test(leads): same-run firing and fingerprint-reuse regression coverage"
```

---

## Task 16: Full completion gate

**Files:** none (verification only)

- [ ] **Step 1: Run the full offline gate**

```bash
uv run pytest tests/foundation tests/run_engine tests/ingestion tests/people tests/wikipedia tests/coverage tests/leads
uv run ruff check .
uv run ruff format .
uv run pyright
```

Expected: all green.

- [ ] **Step 2: Confirm clean working tree**

```bash
git diff --check
git status --short
```

Expected: no whitespace errors, only expected new/modified files listed (no
stray artifacts).

- [ ] **Step 3: For each K-numbered decision (K1-K13) and each named rule
  in the design spec, mutate the corresponding source line, confirm a
  specific named test fails, then restore**

Per CLAUDE.md's "Test Evidence" section, this is required before reporting
completion — tests-first ordering alone is not evidence a test
discriminates. At minimum, mutate and verify:

- K1 (canonical domain aliasing): change `_canonical_domain_map` to ignore
  the `canonical_domain` key → `test_canonical_domain_aliasing_counts_as_one_publisher`
  must fail.
- K7 (positive outcome not downgraded): swap the order of the
  `promising`/`possible` check and the `assessment_terminal` check in
  `aggregate_lead` → `test_k7_positive_outcome_survives_later_incomplete_flag`
  must fail.
- K8 (Wikipedia outcome not a possible-lead reason): add `wikipedia_outcome
  == "uncertain_identity"` as a `_has_useful_possible_reason` branch →
  `test_k8_wikipedia_outcome_is_recorded_but_not_a_possible_lead_reason` must
  fail.
- K11 (starvation is ranking-time only): remove the starvation key from
  `rank_key`'s tuple → `test_starved_entry_ranks_ahead_of_fresher_same_tier_entry`
  must fail.
- Queue lifecycle "repeated evidence is not eligible" rule: remove the
  final `else` branch's early return in `decide_queue_transition` (make it
  always upsert) → `test_repeated_evidence_from_already_counted_domain_is_not_eligible`
  must fail.
- Merge K12 (history not rewritten): change `_move_queue_row_to_survivor`
  to also `UPDATE lead_assessment SET person_id = ?` → the
  `test_lead_assessment_history_is_not_rewritten_onto_survivor` assertion
  must fail (add this mutation check even though the current
  `merge_hooks.py` never performs this update, precisely so the negative
  assertion has a positive control per CLAUDE.md's "Test Evidence" rule).

Restore each mutation with `cp` backup + `diff`, never `git stash`. Run with
`PYTHONDONTWRITEBYTECODE=1` and clear `__pycache__` between mutations. Report
which mutation killed which test.

- [ ] **Step 4: Report completion**

Summarize: files created/modified, completion-gate output, and the mutation
kill-list from Step 3.

---

## Self-Review Notes

- **Spec coverage:** Tasks 1-16 cover the design spec's Schema (Task 1),
  K1/K9 (Task 2), Ranking/K11 (Task 3), Digest Queue Lifecycle/K3/K10 (Task
  4), Configuration Surface (Task 5), repository layer implied by every
  section (Task 6), Work Item and Triggering/K4/K5/K6 (Task 7), the three
  settlement call sites (Task 8), Merge Reconciliation (Task 9), the seed
  sweep implied by "same at-least-once posture as every other milestone"
  (Task 10), CLI registration (Task 11), Digest Rendering (Task 12),
  `notable status` (Task 13), K12 (Task 14), same-run firing and fingerprint
  reuse regression tests explicitly named in Testing Strategy (Task 15), and
  the full completion gate plus CLAUDE.md's mutation-testing requirement
  (Task 16).
- **Known open items an implementer must resolve, flagged inline rather
  than guessed:** the exact run-engine primitive names/signatures
  (`TaskHandler`, work-item enqueue function) in Task 7/10 must be copied
  from `wikipedia/service.py`/`coverage/service.py` verbatim, not guessed;
  whether `digest_queue` needs a persisted `material_fingerprint` column
  (Task 15) requires re-checking the design spec's K6 against Task 1's
  migration before Task 15 is considered done; `_leads_summary`'s SQL body
  in Task 12 is intentionally left for the implementer to write against the
  real schema rather than guessed here. Each of these is called out at its
  exact location above — none is silently assumed away.
- **Type consistency:** `ArticleEvidence`, `SignalEvidence`, `LeadOutcome`
  (Task 2) are consumed unchanged by `queue.py` (Task 4), `ranking.py` (Task
  3 uses only `LeadOutcome`'s `outcome`/`qualifying_domain_count`/
  `wikipedia_outcome` fields), `repository.py` (Task 6), and `service.py`
  (Task 7) — field names were kept identical across every task that touches
  them.
