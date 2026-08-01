# Milestone 6a-i: Lead Aggregation — Schema and Pure Decision Logic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lay the schema and pure, database-free decision logic milestone 6a
needs: the `0008_lead_aggregation.sql` migration (seven new tables plus
`person.current_lead_assessment_id`), the deterministic `aggregate_lead`
outcome decision table, `rank_key` ranking with the starvation guard, and the
`digest_queue` resurfacing-eligibility decision.

**Architecture:** Three small, independently unit-testable modules under
`src/notable_person_finder/leads/` (`aggregation.py`, `ranking.py`,
`queue.py`), each pure and database-free, plus the schema migration and its
test. This plan is the prerequisite for 6a-ii (config surface + repository +
work-item handler), which wires these pure functions to persisted rows.

**Tech Stack:** Python 3.13, SQLite (stdlib `sqlite3`) for the migration
only, pytest, ruff, pyright, `uv`.

**Plan sequence:** This is plan 1 of 4 for milestone 6a. Full sequence:
6a-i (this plan: schema + pure functions) → 6a-ii (config + repository +
work-item handler) → 6a-iii (settlement hooks + merge reconciliation + CLI
registration) → 6a-iv (digest rendering + `notable status` + completion
gate). Each later plan depends on the previous one's tasks being merged.

## Global Constraints

- Migration file must be `src/notable_person_finder/db/migrations/0008_lead_aggregation.sql`,
  matching `^(?P<version>[0-9]{4})_(?P<name>[A-Za-z0-9][A-Za-z0-9_-]*)\.sql$`,
  contiguous after `0007`. Once merged, its SQL text is checksummed and must
  never change.
- No secrets, no model, no Promptfoo suite in this milestone (6a). No
  `compose_lead_summary`, no `notable digest show`, no `notable audit *` —
  those are milestone 6b.
- Full design authority:
  `docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md`
  (K1–K13). This plan does not restate rationale already covered there by a
  K-numbered decision; each task cites the K it implements.
- Local completion gate for this plan alone:
  `uv run pytest tests/leads/test_schema.py tests/leads/test_aggregation.py tests/leads/test_ranking.py tests/leads/test_queue.py tests/foundation`,
  `uv run ruff check .`, `uv run ruff format .`, `uv run pyright` (the
  pyright include-list update is 6a-iv's Task 14 — run pyright informally
  here, it may not yet type-check `tests/leads`).

---

## File Structure (this plan's scope)

```
src/notable_person_finder/leads/
    __init__.py
    aggregation.py       # aggregate_lead() decision table (K7, K8, K9)
    ranking.py            # rank_key(), starvation adjustment (K11)
    queue.py               # resurfacing eligibility + digest_queue/queue_transition decision (K3, K10)

db/migrations/0008_lead_aggregation.sql

tests/leads/
    __init__.py
    test_schema.py
    test_aggregation.py
    test_ranking.py
    test_queue.py
```

---

## Task 1: Migration `0008_lead_aggregation.sql`

**Files:**
- Create: `src/notable_person_finder/db/migrations/0008_lead_aggregation.sql`
- Test: `tests/leads/test_schema.py`

**Interfaces:**
- Produces: tables `lead_assessment`, `lead_assessment_qualifying_article`,
  `lead_assessment_signal`, `digest_queue`, `queue_transition`, `digest`,
  `digest_entry`; column `person.current_lead_assessment_id`.

- [ ] **Step 1: Write the failing schema test**

```python
# tests/leads/test_schema.py
import sqlite3

from notable_person_finder.db.migrate import apply_migrations


def test_0008_creates_all_lead_tables(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection)
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    names = {row[0] for row in rows}
    for expected in (
        "lead_assessment",
        "lead_assessment_qualifying_article",
        "lead_assessment_signal",
        "digest_queue",
        "queue_transition",
        "digest",
        "digest_entry",
    ):
        assert expected in names


def test_person_gains_current_lead_assessment_id_column(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection)
    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(person)")
    }
    assert "current_lead_assessment_id" in columns


def test_lead_assessment_outcome_check_rejects_invalid_value(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection)
    connection.execute(
        "INSERT INTO run (id, started_at, state) VALUES (1, '2026-08-01T00:00:00Z', 'running')"
    )
    connection.execute(
        "INSERT INTO person (id, created_at) VALUES (1, '2026-08-01T00:00:00Z')"
    )
    try:
        connection.execute(
            """
            INSERT INTO lead_assessment (
                person_id, run_id, outcome, qualifying_domain_count,
                ordering_factors_json, lead_policy_fingerprint, decided_at
            ) VALUES (1, 1, 'not_a_real_outcome', 0, '{}', 'x' || hex(randomblob(28)), '2026-08-01T00:00:00Z')
            """
        )
        raised = False
    except sqlite3.IntegrityError:
        raised = True
    assert raised
```

Adjust the `run`/`person` INSERT column lists to whatever the real minimal
required columns are — check `db/migrations/0001_*.sql` through `0007_*.sql`
for the actual `NOT NULL` columns on `run` and `person` before running; the
snippet above is illustrative of intent, the columns must match reality
exactly or the INSERT itself will fail with an unrelated `IntegrityError`
that masks the check under test. Confirm with:
`sqlite3 :memory: ".read src/notable_person_finder/db/migrations/0001_*.sql" ".schema run"`
run through each migration in order, or simply read the migrations directly.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_schema.py -v`
Expected: FAIL — `0008_lead_aggregation.sql` does not exist, or table not
found.

- [ ] **Step 3: Write the migration**

Follow the exact style of `0007_coverage_evidence.sql`: header comment block,
`id INTEGER PRIMARY KEY`, `REFERENCES` FKs, `TEXT NOT NULL CHECK (col GLOB
'*Z')` for timestamps, `CHECK (length(col) = 64)` for fingerprints.

```sql
-- 0008_lead_aggregation.sql
--
-- Milestone 6a: deterministic per-person lead verdicts aggregated from
-- milestone 5's immutable person_article_assessment/article_assessment_signal
-- rows, plus a durable digest queue and its transition ledger. See
-- docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md
-- for K1-K13 rationale. lead_assessment rows are immutable domain history;
-- digest_queue is a mutable operational projection with no history of its
-- own (K3) -- queue_transition is the ledger.

CREATE TABLE lead_assessment (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'promising_lead', 'possible_lead',
        'insufficient_evidence', 'assessment_incomplete'
    )),
    qualifying_domain_count INTEGER NOT NULL DEFAULT 0
        CHECK (qualifying_domain_count >= 0),
    incompleteness_reason TEXT,
    ordering_factors_json TEXT NOT NULL,
    lead_policy_fingerprint TEXT NOT NULL
        CHECK (length(lead_policy_fingerprint) = 64),
    decided_at TEXT NOT NULL CHECK (decided_at GLOB '*Z'),
    CHECK (
        (outcome = 'assessment_incomplete' AND incompleteness_reason IS NOT NULL)
        OR (outcome != 'assessment_incomplete' AND incompleteness_reason IS NULL)
    )
);
CREATE INDEX ix_lead_assessment_person ON lead_assessment(person_id, id);

CREATE TABLE lead_assessment_qualifying_article (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    person_article_assessment_id INTEGER NOT NULL
        REFERENCES person_article_assessment(id),
    canonical_domain TEXT NOT NULL CHECK (length(canonical_domain) > 0),
    PRIMARY KEY (lead_assessment_id, person_article_assessment_id)
);

CREATE TABLE lead_assessment_signal (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    article_assessment_signal_id INTEGER NOT NULL
        REFERENCES article_assessment_signal(id),
    PRIMARY KEY (lead_assessment_id, article_assessment_signal_id)
);

ALTER TABLE person ADD COLUMN current_lead_assessment_id INTEGER
    REFERENCES lead_assessment(id);

CREATE TABLE digest_queue (
    person_id INTEGER PRIMARY KEY REFERENCES person(id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'emitted', 'removed')),
    tier TEXT NOT NULL CHECK (tier IN ('promising_lead', 'possible_lead')),
    eligibility_reason TEXT NOT NULL CHECK (eligibility_reason IN (
        'new', 'promoted', 'strengthened', 'reminder'
    )),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    first_pending_at TEXT NOT NULL CHECK (first_pending_at GLOB '*Z'),
    last_material_change_at TEXT NOT NULL CHECK (last_material_change_at GLOB '*Z'),
    removed_reason TEXT
);

CREATE TABLE queue_transition (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    tier TEXT NOT NULL CHECK (tier IN ('promising_lead', 'possible_lead')),
    from_status TEXT CHECK (from_status IN ('pending', 'emitted', 'removed')),
    to_status TEXT NOT NULL CHECK (to_status IN ('pending', 'emitted', 'removed')),
    reason TEXT NOT NULL CHECK (length(reason) > 0),
    occurred_at TEXT NOT NULL CHECK (occurred_at GLOB '*Z')
);
CREATE INDEX ix_queue_transition_run ON queue_transition(run_id, id);
CREATE INDEX ix_queue_transition_person ON queue_transition(person_id, id);

CREATE TABLE digest (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    file_path TEXT NOT NULL CHECK (length(file_path) > 0),
    timezone TEXT NOT NULL CHECK (length(timezone) > 0),
    window_start TEXT NOT NULL CHECK (window_start GLOB '*Z'),
    window_end TEXT NOT NULL CHECK (window_end GLOB '*Z'),
    run_state TEXT NOT NULL CHECK (length(run_state) > 0),
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z')
);

CREATE TABLE digest_entry (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NOT NULL REFERENCES digest(id),
    person_id INTEGER NOT NULL REFERENCES person(id),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    queue_transition_id INTEGER NOT NULL REFERENCES queue_transition(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1)
);
CREATE INDEX ix_digest_entry_digest ON digest_entry(digest_id, ordinal);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_schema.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/db/migrations/0008_lead_aggregation.sql tests/leads/
git commit -m "feat(leads): add 0008 lead-aggregation migration"
```

---

## Task 2: `aggregate_lead` decision table (K7, K8, K9)

**Files:**
- Create: `src/notable_person_finder/leads/__init__.py` (empty)
- Create: `src/notable_person_finder/leads/aggregation.py`
- Test: `tests/leads/test_aggregation.py`

**Interfaces:**
- Consumes: `person_article_assessment` rows (`coverage_depth`,
  `content_types_json`, `subject_relationship`, `person_relation`,
  `screening_rule_status`, `canonical_article_id`), `article_assessment_signal`
  rows (`signal_kind`, `category`), a Wikipedia identity outcome string, and
  the source policy's `canonical_domain` aliasing.
- Produces: `LeadOutcome`, `QualifyingArticle`, `aggregate_lead(...)` — used
  by Task 5's handler and Task 6's ranking.

- [ ] **Step 1: Write the failing tests**

```python
# tests/leads/test_aggregation.py
from notable_person_finder.leads.aggregation import (
    ArticleEvidence,
    LeadOutcome,
    SignalEvidence,
    aggregate_lead,
)


def _article(
    *,
    domain="example.com",
    canonical_domain=None,
    same_person=True,
    depth="significant",
    content_qualifying=True,
    editorially_independent=True,
    screening_status="curated_eligible",
) -> ArticleEvidence:
    return ArticleEvidence(
        person_article_assessment_id=1,
        domain=domain,
        canonical_domain=canonical_domain or domain,
        same_person=same_person,
        coverage_depth=depth,
        content_qualifying=content_qualifying,
        editorially_independent=editorially_independent,
        screening_status=screening_status,
    )


def test_two_distinct_domains_promising():
    articles = [
        _article(domain="a.example"),
        _article(domain="b.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "promising_lead"
    assert outcome.qualifying_domain_count == 2


def test_one_domain_possible():
    articles = [_article(domain="a.example")]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"
    assert outcome.qualifying_domain_count == 1


def test_unclassified_significant_coverage_is_possible():
    articles = [
        _article(
            domain="unclassified.example",
            depth="significant",
            screening_status="unclassified",
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"


def test_completed_nothing_qualifies_is_insufficient_evidence():
    articles = [
        _article(
            domain="a.example",
            same_person=False,
            content_qualifying=False,
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "insufficient_evidence"


def test_missing_required_work_is_assessment_incomplete():
    outcome = aggregate_lead(
        articles=[],
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
        assessment_terminal=False,
    )
    assert outcome.outcome == "assessment_incomplete"
    assert outcome.incompleteness_reason is not None


def test_k7_positive_outcome_survives_later_incomplete_flag():
    """A qualifying article already exists; a later incomplete flag must not
    downgrade the outcome (K7)."""
    articles = [
        _article(domain="a.example"),
        _article(domain="b.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=False,
    )
    assert outcome.outcome == "promising_lead"


def test_k8_wikipedia_outcome_is_recorded_but_not_a_possible_lead_reason():
    articles = [
        _article(
            domain="a.example",
            same_person=False,
            content_qualifying=False,
        )
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="uncertain_identity",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "insufficient_evidence"
    assert outcome.wikipedia_outcome == "uncertain_identity"


def test_canonical_domain_aliasing_counts_as_one_publisher():
    articles = [
        _article(domain="a.example", canonical_domain="shared.example"),
        _article(domain="b.example", canonical_domain="shared.example"),
    ]
    outcome = aggregate_lead(
        articles=articles,
        signals=[],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
    )
    assert outcome.outcome == "possible_lead"
    assert outcome.qualifying_domain_count == 1


def test_grounded_transferable_signal_is_possible_lead():
    outcome = aggregate_lead(
        articles=[],
        signals=[
            SignalEvidence(
                article_assessment_signal_id=1,
                signal_kind="attention",
                category="award",
                transferable=True,
            )
        ],
        wikipedia_outcome="no_matching_page_found",
        promising_domain_threshold=2,
        assessment_terminal=True,
    )
    assert outcome.outcome == "possible_lead"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_aggregation.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'notable_person_finder.leads.aggregation'`

- [ ] **Step 3: Implement `aggregation.py`**

```python
# src/notable_person_finder/leads/aggregation.py
"""Deterministic per-person lead outcome aggregation (K7, K8, K9).

No model call: every branch reads fields milestone 5's
``person_article_assessment``/``article_assessment_signal`` already produce.
"""

from __future__ import annotations

from dataclasses import dataclass, field

LeadOutcomeName = str  # one of the four CHECK-constrained outcome strings


@dataclass(frozen=True, slots=True)
class ArticleEvidence:
    person_article_assessment_id: int
    domain: str
    canonical_domain: str
    same_person: bool
    coverage_depth: str | None  # 'significant' | 'passing' | 'uncertain' | None
    content_qualifying: bool
    editorially_independent: bool
    screening_status: str  # 'curated_eligible' | 'curated_ineligible' | 'unclassified'


@dataclass(frozen=True, slots=True)
class SignalEvidence:
    article_assessment_signal_id: int
    signal_kind: str  # 'attention' | 'caution'
    category: str
    transferable: bool = False


@dataclass(frozen=True, slots=True)
class LeadOutcome:
    outcome: LeadOutcomeName
    qualifying_domain_count: int
    qualifying_articles: tuple[ArticleEvidence, ...]
    contributing_signals: tuple[SignalEvidence, ...]
    wikipedia_outcome: str | None
    incompleteness_reason: str | None = None


def _is_qualifying(article: ArticleEvidence) -> bool:
    return (
        article.same_person
        and article.coverage_depth == "significant"
        and article.screening_status == "curated_eligible"
        and article.editorially_independent
        and article.content_qualifying
    )


def _qualifying_domains(articles: list[ArticleEvidence]) -> set[str]:
    return {a.canonical_domain for a in articles if _is_qualifying(a)}


def _has_useful_possible_reason(
    articles: list[ArticleEvidence], signals: list[SignalEvidence]
) -> bool:
    for article in articles:
        if article.same_person and article.coverage_depth == "significant" and (
            article.screening_status == "unclassified"
        ):
            return True
        if (
            article.same_person
            and article.coverage_depth in ("significant", "passing")
            and article.content_qualifying
            and not _is_qualifying(article)
        ):
            return True
    for signal in signals:
        if signal.signal_kind == "attention" and signal.transferable:
            return True
    return False


def aggregate_lead(
    *,
    articles: list[ArticleEvidence],
    signals: list[SignalEvidence],
    wikipedia_outcome: str | None,
    promising_domain_threshold: int,
    assessment_terminal: bool = True,
) -> LeadOutcome:
    qualifying_domains = _qualifying_domains(articles)
    if len(qualifying_domains) >= promising_domain_threshold:
        qualifying = tuple(a for a in articles if _is_qualifying(a))
        return LeadOutcome(
            outcome="promising_lead",
            qualifying_domain_count=len(qualifying_domains),
            qualifying_articles=qualifying,
            contributing_signals=(),
            wikipedia_outcome=wikipedia_outcome,
        )
    if len(qualifying_domains) >= 1 or _has_useful_possible_reason(
        articles, signals
    ):
        qualifying = tuple(a for a in articles if _is_qualifying(a))
        contributing = tuple(
            s for s in signals if s.signal_kind == "attention" and s.transferable
        )
        return LeadOutcome(
            outcome="possible_lead",
            qualifying_domain_count=len(qualifying_domains),
            qualifying_articles=qualifying,
            contributing_signals=contributing,
            wikipedia_outcome=wikipedia_outcome,
        )
    if assessment_terminal:
        return LeadOutcome(
            outcome="insufficient_evidence",
            qualifying_domain_count=0,
            qualifying_articles=(),
            contributing_signals=(),
            wikipedia_outcome=wikipedia_outcome,
        )
    return LeadOutcome(
        outcome="assessment_incomplete",
        qualifying_domain_count=0,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome=wikipedia_outcome,
        incompleteness_reason="required_coverage_work_not_settled",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_aggregation.py -v`
Expected: PASS (all 9 tests)

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/leads/ tests/leads/test_aggregation.py
git commit -m "feat(leads): deterministic aggregate_lead decision table"
```

---

## Task 3: `rank_key` ranking and starvation adjustment (K11)

**Files:**
- Create: `src/notable_person_finder/leads/ranking.py`
- Test: `tests/leads/test_ranking.py`

**Interfaces:**
- Consumes: `LeadOutcome` (Task 2), a `QueueEntry` dataclass (defined here,
  reused by Task 4).
- Produces: `QueueEntry`, `rank_key(entry, lead, starvation_cutoff) -> tuple`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/leads/test_ranking.py
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.ranking import QueueEntry, rank_key


def _lead(outcome="promising_lead", domains=2, signals=0, wikipedia="no_matching_page_found", freshest="2026-07-01T00:00:00Z", visibility="full"):
    return LeadOutcome(
        outcome=outcome,
        qualifying_domain_count=domains,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome=wikipedia,
    ), signals, freshest, visibility


def _entry(person_id=1, eligibility="new", first_pending="2026-08-01T00:00:00Z"):
    return QueueEntry(
        person_id=person_id,
        eligibility_reason=eligibility,
        first_pending_at=first_pending,
    )


def test_promising_ranks_before_possible():
    lead_a, *_ = _lead(outcome="promising_lead")
    lead_b, *_ = _lead(outcome="possible_lead")
    key_a = rank_key(_entry(person_id=1), lead_a, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    key_b = rank_key(_entry(person_id=2), lead_b, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    assert key_a < key_b


def test_starved_entry_ranks_ahead_of_fresher_same_tier_entry():
    lead, *_ = _lead(outcome="possible_lead")
    starved = _entry(person_id=1, first_pending_at="2025-01-01T00:00:00Z")
    fresh = _entry(person_id=2, first_pending_at="2026-08-01T00:00:00Z")
    starved_key = rank_key(starved, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-06-01T00:00:00Z")
    fresh_key = rank_key(fresh, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-06-01T00:00:00Z")
    assert starved_key < fresh_key


def test_starved_possible_still_sorts_behind_fresh_promising():
    starved_possible, *_ = _lead(outcome="possible_lead")
    fresh_promising, *_ = _lead(outcome="promising_lead")
    starved_entry = _entry(person_id=1, first_pending_at="2025-01-01T00:00:00Z")
    fresh_entry = _entry(person_id=2, first_pending_at="2026-08-01T00:00:00Z")
    starved_key = rank_key(starved_entry, starved_possible, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-06-01T00:00:00Z")
    promising_key = rank_key(fresh_entry, fresh_promising, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-06-01T00:00:00Z")
    assert promising_key < starved_key


def test_eligibility_reason_tie_break_new_before_reminder():
    lead, *_ = _lead()
    new_entry = _entry(person_id=1, eligibility="new")
    reminder_entry = _entry(person_id=2, eligibility="reminder")
    new_key = rank_key(new_entry, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    reminder_key = rank_key(reminder_entry, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    assert new_key < reminder_key


def test_person_id_is_final_deterministic_tiebreak():
    lead, *_ = _lead()
    entry_low = _entry(person_id=1)
    entry_high = _entry(person_id=2)
    key_low = rank_key(entry_low, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    key_high = rank_key(entry_high, lead, positive_signal_count=0, best_evidence_visibility="full", freshest_qualifying_article_at="2026-08-01T00:00:00Z", starvation_cutoff="2026-01-01T00:00:00Z")
    assert key_low < key_high
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_ranking.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `ranking.py`**

```python
# src/notable_person_finder/leads/ranking.py
"""Pure ranking of digest-queue entries (capability 5's rank_key, capability
6's starvation guard). See design spec Ranking section for K11 rationale."""

from __future__ import annotations

from dataclasses import dataclass

from notable_person_finder.leads.aggregation import LeadOutcome

_OUTCOME_RANK = {"promising_lead": 0, "possible_lead": 1}
_ELIGIBILITY_RANK = {"new": 0, "promoted": 0, "strengthened": 0, "reminder": 1}
_WIKIPEDIA_RANK = {"no_matching_page_found": 0, "uncertain_identity": 1}
_EVIDENCE_VISIBILITY_RANK = {"full": 0, "partial": 1, "snippet": 2}


@dataclass(frozen=True, slots=True)
class QueueEntry:
    person_id: int
    eligibility_reason: str  # 'new' | 'promoted' | 'strengthened' | 'reminder'
    first_pending_at: str


def _freshness_days(freshest_qualifying_article_at: str | None) -> int:
    if freshest_qualifying_article_at is None:
        return 0
    # Lexicographic ISO-8601 UTC timestamps sort chronologically; days-since
    # is not needed for ordering, only a monotone proxy is. Callers passing
    # the raw timestamp already yield correct ordering when negated below,
    # so this returns an integer derived from the timestamp for typing only.
    return int(freshest_qualifying_article_at.replace("-", "").replace(":", "").replace("T", "").rstrip("Z")[:14] or 0)


def rank_key(
    entry: QueueEntry,
    lead: LeadOutcome,
    *,
    positive_signal_count: int,
    best_evidence_visibility: str,
    freshest_qualifying_article_at: str | None,
    starvation_cutoff: str,
) -> tuple[object, ...]:
    is_starved = entry.first_pending_at < starvation_cutoff
    return (
        _OUTCOME_RANK[lead.outcome],
        0 if is_starved else 1,
        _ELIGIBILITY_RANK[entry.eligibility_reason],
        _WIKIPEDIA_RANK.get(lead.wikipedia_outcome or "", 2),
        -lead.qualifying_domain_count,
        -positive_signal_count,
        _EVIDENCE_VISIBILITY_RANK.get(best_evidence_visibility, 3),
        -_freshness_days(freshest_qualifying_article_at),
        entry.person_id,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_ranking.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/leads/ranking.py tests/leads/test_ranking.py
git commit -m "feat(leads): rank_key with starvation guard (K11)"
```

---

## Task 4: Digest queue lifecycle (`queue.py`) (K3, K10)

**Files:**
- Create: `src/notable_person_finder/leads/queue.py`
- Test: `tests/leads/test_queue.py`

**Interfaces:**
- Consumes: `LeadOutcome` (Task 2), a `PriorLeadState` snapshot (current
  `digest_queue` row fields, if any), `AggregateLeadConfig` (Task 7).
- Produces: `QueueDecision` dataclass and `decide_queue_transition(prior,
  outcome, *, reminder_interval_days, now) -> QueueDecision`, consumed by
  Task 5's handler for the actual SQL write.

- [ ] **Step 1: Write the failing tests**

```python
# tests/leads/test_queue.py
from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState, decide_queue_transition


def _outcome(outcome, domains=1):
    return LeadOutcome(
        outcome=outcome,
        qualifying_domain_count=domains,
        qualifying_articles=(),
        contributing_signals=(),
        wikipedia_outcome="no_matching_page_found",
    )


def test_new_positive_outcome_with_no_prior_row_is_new():
    decision = decide_queue_transition(
        prior=None,
        outcome=_outcome("possible_lead"),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_upsert
    assert decision.eligibility_reason == "new"
    assert decision.to_status == "pending"


def test_possible_to_promising_is_promoted():
    prior = PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("promising_lead", domains=2),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_upsert
    assert decision.eligibility_reason == "promoted"


def test_repeated_evidence_from_already_counted_domain_is_not_eligible():
    prior = PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("possible_lead", domains=1),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert not decision.should_upsert


def test_neutral_outcome_leaves_existing_row_untouched():
    prior = PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("insufficient_evidence", domains=0),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert not decision.should_upsert
    assert not decision.should_remove


def test_matching_page_found_removes_existing_row():
    prior = PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("possible_lead", domains=1),
        matching_page_found=True,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.should_remove
    assert decision.removed_reason == "matching_page_found"


def test_first_pending_at_preserved_across_updates():
    prior = PriorLeadState(
        status="pending", tier="possible_lead", qualifying_domain_count=1,
        first_pending_at="2026-07-01T00:00:00Z",
    )
    decision = decide_queue_transition(
        prior=prior,
        outcome=_outcome("promising_lead", domains=2),
        matching_page_found=False,
        now="2026-08-01T00:00:00Z",
    )
    assert decision.first_pending_at == "2026-07-01T00:00:00Z"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/leads/test_queue.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Implement `queue.py`**

```python
# src/notable_person_finder/leads/queue.py
"""Digest-queue resurfacing eligibility and transition decisions (K3, K10).

digest_queue is a mutable one-row-per-person projection; queue_transition is
the append-only ledger. This module decides *what* should happen; callers
(leads/service.py, leads/merge_hooks.py) perform the actual SQL writes inside
their own transaction.
"""

from __future__ import annotations

from dataclasses import dataclass

from notable_person_finder.leads.aggregation import LeadOutcome

_POSITIVE_OUTCOMES = {"promising_lead", "possible_lead"}


@dataclass(frozen=True, slots=True)
class PriorLeadState:
    status: str  # 'pending' | 'emitted' | 'removed'
    tier: str  # 'promising_lead' | 'possible_lead'
    qualifying_domain_count: int
    first_pending_at: str


@dataclass(frozen=True, slots=True)
class QueueDecision:
    should_upsert: bool
    should_remove: bool
    eligibility_reason: str | None
    to_status: str | None
    first_pending_at: str | None
    removed_reason: str | None = None


def decide_queue_transition(
    *,
    prior: PriorLeadState | None,
    outcome: LeadOutcome,
    matching_page_found: bool,
    now: str,
) -> QueueDecision:
    if matching_page_found:
        if prior is not None and prior.status != "removed":
            return QueueDecision(
                should_upsert=False,
                should_remove=True,
                eligibility_reason=None,
                to_status="removed",
                first_pending_at=None,
                removed_reason="matching_page_found",
            )
        return QueueDecision(
            should_upsert=False,
            should_remove=False,
            eligibility_reason=None,
            to_status=None,
            first_pending_at=None,
        )

    if outcome.outcome not in _POSITIVE_OUTCOMES:
        # A neutral/negative outcome never retracts an existing positive
        # entry — only new positive evidence changes an active queue row.
        return QueueDecision(
            should_upsert=False,
            should_remove=False,
            eligibility_reason=None,
            to_status=None,
            first_pending_at=None,
        )

    if prior is None:
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="new",
            to_status="pending",
            first_pending_at=now,
        )

    if prior.tier == "possible_lead" and outcome.outcome == "promising_lead":
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="promoted",
            to_status="pending",
            first_pending_at=prior.first_pending_at,
        )

    if outcome.qualifying_domain_count > prior.qualifying_domain_count:
        return QueueDecision(
            should_upsert=True,
            should_remove=False,
            eligibility_reason="strengthened",
            to_status="pending",
            first_pending_at=prior.first_pending_at,
        )

    # Same tier, no new qualifying domain: repeated evidence from an
    # already-counted domain is not independently resurfacing-eligible.
    return QueueDecision(
        should_upsert=False,
        should_remove=False,
        eligibility_reason=None,
        to_status=None,
        first_pending_at=None,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/leads/test_queue.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/notable_person_finder/leads/queue.py tests/leads/test_queue.py
git commit -m "feat(leads): digest_queue resurfacing decision (K3, K10)"
```
