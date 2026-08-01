"""SQL for the seven leads/digest-queue tables. Every function operates
inside the caller's transaction; none opens or commits its own."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from notable_person_finder.leads.aggregation import LeadOutcome
from notable_person_finder.leads.queue import PriorLeadState
from notable_person_finder.leads.ranking import QueueEntry


def insert_lead_assessment(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    outcome: LeadOutcome,
    lead_policy_fingerprint: str,
    ordering_factors_json: str,
    decided_at: str,
    material_fingerprint: str | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO lead_assessment (
            person_id, run_id, outcome, qualifying_domain_count,
            incompleteness_reason, ordering_factors_json,
            lead_policy_fingerprint, material_fingerprint, decided_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            outcome.outcome,
            outcome.qualifying_domain_count,
            outcome.incompleteness_reason,
            ordering_factors_json,
            lead_policy_fingerprint,
            material_fingerprint,
            decided_at,
        ),
    )
    lead_id = cursor.lastrowid
    assert lead_id is not None
    for article in outcome.qualifying_articles:
        connection.execute(
            """
            INSERT INTO lead_assessment_qualifying_article (
                lead_assessment_id, person_article_assessment_id, canonical_domain
            ) VALUES (?, ?, ?)
            """,
            (lead_id, article.person_article_assessment_id, article.canonical_domain),
        )
    for signal in outcome.contributing_signals:
        connection.execute(
            """
            INSERT INTO lead_assessment_signal (
                lead_assessment_id, article_assessment_signal_id
            ) VALUES (?, ?)
            """,
            (lead_id, signal.article_assessment_signal_id),
        )
    connection.execute(
        "UPDATE person SET current_lead_assessment_id = ? WHERE id = ?",
        (lead_id, person_id),
    )
    return lead_id


def fetch_current_lead_assessment_material_fingerprint(
    connection: sqlite3.Connection, *, person_id: int
) -> str | None:
    """The `material_fingerprint` stored on a person's current (most recent)
    lead_assessment row, or None if the person has never been aggregated (or
    a pre-existing row predates this column). Used by
    `aggregate_person_lead`'s `prepare` for the K6 unchanged-evidence
    local-refusal check -- a None result never matches a computed
    fingerprint, so it is never mistaken for "already aggregated."
    """
    row = connection.execute(
        """
        SELECT la.material_fingerprint
        FROM person p
        JOIN lead_assessment la ON la.id = p.current_lead_assessment_id
        WHERE p.id = ?
        """,
        (person_id,),
    ).fetchone()
    return row[0] if row is not None else None


def fetch_prior_queue_state(
    connection: sqlite3.Connection, *, person_id: int
) -> PriorLeadState | None:
    row = connection.execute(
        """
        SELECT dq.status, dq.tier, la.qualifying_domain_count, dq.first_pending_at
        FROM digest_queue dq
        JOIN lead_assessment la ON la.id = dq.lead_assessment_id
        WHERE dq.person_id = ?
        """,
        (person_id,),
    ).fetchone()
    if row is None:
        return None
    status, tier, qualifying_domain_count, first_pending_at = row
    return PriorLeadState(
        status=status,
        tier=tier,
        qualifying_domain_count=qualifying_domain_count,
        first_pending_at=first_pending_at,
    )


def upsert_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    status: str,
    tier: str,
    eligibility_reason: str,
    lead_assessment_id: int,
    first_pending_at: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_queue (
            person_id, status, tier, eligibility_reason, lead_assessment_id,
            first_pending_at, last_material_change_at, removed_reason
        ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
        ON CONFLICT (person_id) DO UPDATE SET
            status = excluded.status,
            tier = excluded.tier,
            eligibility_reason = excluded.eligibility_reason,
            lead_assessment_id = excluded.lead_assessment_id,
            last_material_change_at = excluded.last_material_change_at,
            removed_reason = NULL
        """,
        (
            person_id,
            status,
            tier,
            eligibility_reason,
            lead_assessment_id,
            first_pending_at,
            last_material_change_at,
        ),
    )


def remove_digest_queue(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    removed_reason: str,
    last_material_change_at: str,
) -> None:
    connection.execute(
        """
        UPDATE digest_queue
        SET status = 'removed', removed_reason = ?, last_material_change_at = ?
        WHERE person_id = ?
        """,
        (removed_reason, last_material_change_at, person_id),
    )


def insert_queue_transition(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    lead_assessment_id: int,
    tier: str,
    from_status: str | None,
    to_status: str,
    reason: str,
    occurred_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO queue_transition (
            person_id, run_id, lead_assessment_id, tier, from_status,
            to_status, reason, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            run_id,
            lead_assessment_id,
            tier,
            from_status,
            to_status,
            reason,
            occurred_at,
        ),
    )
    transition_id = cursor.lastrowid
    assert transition_id is not None
    return transition_id


def fetch_pending_queue_entries(
    connection: sqlite3.Connection,
) -> list[QueueEntry]:
    rows = connection.execute(
        """
        SELECT person_id, eligibility_reason, first_pending_at
        FROM digest_queue
        WHERE status = 'pending'
        """
    ).fetchall()
    return [
        QueueEntry(person_id=r[0], eligibility_reason=r[1], first_pending_at=r[2])
        for r in rows
    ]


def insert_digest(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    timezone: str,
    window_start: str,
    window_end: str,
    run_state: str,
    content_hash: str,
    created_at: str,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO digest (
            run_id, file_path, timezone, window_start, window_end,
            run_state, content_hash, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            file_path,
            timezone,
            window_start,
            window_end,
            run_state,
            content_hash,
            created_at,
        ),
    )
    digest_id = cursor.lastrowid
    assert digest_id is not None
    return digest_id


def insert_digest_entry(
    connection: sqlite3.Connection,
    *,
    digest_id: int,
    person_id: int,
    lead_assessment_id: int,
    queue_transition_id: int,
    ordinal: int,
) -> None:
    connection.execute(
        """
        INSERT INTO digest_entry (
            digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (digest_id, person_id, lead_assessment_id, queue_transition_id, ordinal),
    )


# ---------------------------------------------------------------------------
# Digest rendering support (Task 12): shortlist candidates and queue-flow
# counters. These are read-only reporting queries, not settlement writes, but
# live here to keep every piece of leads SQL in one module.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ShortlistCandidate:
    """One pending digest_queue row joined with its current lead_assessment,
    enough to rank via `ranking.rank_key` and render a `ShortlistEntry`."""

    person_id: int
    display_name: str
    eligibility_reason: str
    first_pending_at: str
    tier: str
    lead_assessment_id: int
    outcome: str
    qualifying_domain_count: int
    wikipedia_outcome: str | None


def fetch_pending_shortlist_candidates(
    connection: sqlite3.Connection,
) -> list[ShortlistCandidate]:
    rows = connection.execute(
        """
        SELECT dq.person_id, p.display_name, dq.eligibility_reason,
               dq.first_pending_at, dq.tier, dq.lead_assessment_id,
               la.outcome, la.qualifying_domain_count, la.ordering_factors_json
        FROM digest_queue dq
        JOIN person p ON p.id = dq.person_id
        JOIN lead_assessment la ON la.id = dq.lead_assessment_id
        WHERE dq.status = 'pending'
        """
    ).fetchall()
    candidates: list[ShortlistCandidate] = []
    for (
        person_id,
        display_name,
        eligibility_reason,
        first_pending_at,
        tier,
        lead_assessment_id,
        outcome,
        qualifying_domain_count,
        ordering_factors_json,
    ) in rows:
        factors = json.loads(ordering_factors_json) if ordering_factors_json else {}
        candidates.append(
            ShortlistCandidate(
                person_id=person_id,
                display_name=display_name,
                eligibility_reason=eligibility_reason,
                first_pending_at=first_pending_at,
                tier=tier,
                lead_assessment_id=lead_assessment_id,
                outcome=outcome,
                qualifying_domain_count=qualifying_domain_count,
                wikipedia_outcome=factors.get("wikipedia_outcome"),
            )
        )
    return candidates


def fetch_qualifying_sources_for_lead(
    connection: sqlite3.Connection, *, lead_assessment_id: int
) -> list[tuple[str, str, str, str, str, str]]:
    """(domain, title, date, content_type, depth, visibility) per qualifying
    article, joined through the assessment to its article view."""
    rows = connection.execute(
        """
        SELECT laqa.canonical_domain, av.title, av.published_at,
               paa.content_types_json, paa.coverage_depth, av.access_kind
        FROM lead_assessment_qualifying_article laqa
        JOIN person_article_assessment paa
            ON paa.id = laqa.person_article_assessment_id
        JOIN article_view av ON av.id = paa.article_view_id
        WHERE laqa.lead_assessment_id = ?
        ORDER BY laqa.canonical_domain
        """,
        (lead_assessment_id,),
    ).fetchall()
    sources: list[tuple[str, str, str, str, str, str]] = []
    for (
        domain,
        title,
        published_at,
        content_types_json,
        coverage_depth,
        access_kind,
    ) in rows:
        content_types = json.loads(content_types_json) if content_types_json else []
        content_type = content_types[0] if content_types else "unknown"
        sources.append(
            (
                domain,
                title or "",
                published_at or "-",
                content_type,
                coverage_depth or "-",
                access_kind or "-",
            )
        )
    return sources


def fetch_signal_claims_for_person(
    connection: sqlite3.Connection, *, person_id: int, signal_kind: str
) -> list[str]:
    """Claim text for every completed-assessment signal of `signal_kind`
    ('attention' or 'caution') belonging to this person, newest ordinal
    first within each assessment -- matches leads/service.py's
    `_load_signal_evidence` join style."""
    rows = connection.execute(
        """
        SELECT s.claim
        FROM article_assessment_signal s
        JOIN person_article_assessment paa ON paa.id = s.assessment_id
        WHERE paa.person_id = ? AND paa.disposition = 'completed'
          AND s.signal_kind = ?
        ORDER BY paa.id, s.ordinal
        """,
        (person_id, signal_kind),
    ).fetchall()
    return [row[0] for row in rows]


def count_attention_signals_for_lead(
    connection: sqlite3.Connection, *, lead_assessment_id: int
) -> int:
    """Count of contributing (attention, transferable) signals recorded on
    this lead_assessment -- `ranking.rank_key`'s `positive_signal_count`."""
    row = connection.execute(
        """
        SELECT COUNT(*) AS n
        FROM lead_assessment_signal las
        JOIN article_assessment_signal s ON s.id = las.article_assessment_signal_id
        WHERE las.lead_assessment_id = ? AND s.signal_kind = 'attention'
        """,
        (lead_assessment_id,),
    ).fetchone()
    return int(row["n"])


@dataclass(frozen=True, slots=True)
class QueueFlowCounts:
    """Raw counts and rate inputs for `reporting.digest.QueueFlowSummary`."""

    newly_queued: int
    emitted: int
    removed_matching_wikipedia: int
    ending_backlog_promising: int
    ending_backlog_possible: int
    arrival_rate_7d: float | None
    emission_rate_7d: float | None
    oldest_pending_days: int | None


def fetch_queue_flow_counts(
    connection: sqlite3.Connection, *, run_id: int, now: str
) -> QueueFlowCounts:
    newly_queued = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM queue_transition
            WHERE run_id = ? AND reason = 'new'
            """,
            (run_id,),
        ).fetchone()["n"]
    )
    emitted = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM queue_transition
            WHERE run_id = ? AND to_status = 'emitted'
            """,
            (run_id,),
        ).fetchone()["n"]
    )
    removed_matching_wikipedia = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM queue_transition
            WHERE run_id = ? AND to_status = 'removed'
              AND reason = 'matching_page_found'
            """,
            (run_id,),
        ).fetchone()["n"]
    )
    backlog_rows = connection.execute(
        """
        SELECT tier, COUNT(*) AS n FROM digest_queue
        WHERE status = 'pending'
        GROUP BY tier
        """
    ).fetchall()
    backlog_by_tier = {row["tier"]: int(row["n"]) for row in backlog_rows}
    ending_backlog_promising = backlog_by_tier.get("promising_lead", 0)
    ending_backlog_possible = backlog_by_tier.get("possible_lead", 0)

    has_any_history = (
        connection.execute("SELECT 1 FROM queue_transition LIMIT 1").fetchone()
        is not None
    )
    arrival_rate_7d: float | None = None
    emission_rate_7d: float | None = None
    if has_any_history:
        cutoff = shift_utc_days(now, -7)
        arrivals_7d = int(
            connection.execute(
                """
                SELECT COUNT(*) AS n FROM queue_transition
                WHERE reason = 'new' AND occurred_at >= ?
                """,
                (cutoff,),
            ).fetchone()["n"]
        )
        emissions_7d = int(
            connection.execute(
                """
                SELECT COUNT(*) AS n FROM queue_transition
                WHERE to_status = 'emitted' AND occurred_at >= ?
                """,
                (cutoff,),
            ).fetchone()["n"]
        )
        arrival_rate_7d = arrivals_7d / 7.0
        emission_rate_7d = emissions_7d / 7.0

    oldest_row = connection.execute(
        """
        SELECT MIN(first_pending_at) AS oldest FROM digest_queue
        WHERE status = 'pending'
        """
    ).fetchone()
    oldest_pending_days: int | None = None
    if oldest_row is not None and oldest_row["oldest"] is not None:
        oldest_pending_days = _days_between(oldest_row["oldest"], now)

    return QueueFlowCounts(
        newly_queued=newly_queued,
        emitted=emitted,
        removed_matching_wikipedia=removed_matching_wikipedia,
        ending_backlog_promising=ending_backlog_promising,
        ending_backlog_possible=ending_backlog_possible,
        arrival_rate_7d=arrival_rate_7d,
        emission_rate_7d=emission_rate_7d,
        oldest_pending_days=oldest_pending_days,
    )


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def shift_utc_days(value: str, days: int) -> str:
    shifted = _parse_utc(value) + timedelta(days=days)
    return shifted.strftime("%Y-%m-%dT%H:%M:%SZ")


def mark_digest_queue_emitted(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    last_material_change_at: str,
) -> None:
    """Transition one digest_queue row's status to 'emitted'. Mirrors
    `remove_digest_queue`'s style for the 'removed' transition; called only
    for a person actually selected into this run's rendered shortlist."""
    connection.execute(
        """
        UPDATE digest_queue
        SET status = 'emitted', last_material_change_at = ?
        WHERE person_id = ?
        """,
        (last_material_change_at, person_id),
    )


def emit_shortlist_entries(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    occurred_at: str,
    candidates: Sequence[ShortlistCandidate],
) -> list[tuple[ShortlistCandidate, int]]:
    """Transition every rendered shortlist candidate `pending -> emitted`:
    one `queue_transition` row and a `digest_queue` status update each, in a
    single transaction covering the whole shortlist. Entries beyond
    `digest_limit` are never passed in here and so stay `pending` untouched.

    Runs in its own top-level transaction (`BEGIN IMMEDIATE` / commit) rather
    than the caller's, because it is invoked from the CLI's digest-reporting
    step, outside the run engine's per-work-item transaction scope -- the
    same reason `runs.repository.create_run`/`finish_run` manage their own
    transaction boundaries.

    Returns the candidates paired with their new `queue_transition` id, in
    the same order, so the caller can later attach `digest_entry` rows once
    the digest's real `file_path`/`content_hash` are known.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        emitted: list[tuple[ShortlistCandidate, int]] = []
        for candidate in candidates:
            transition_id = insert_queue_transition(
                connection,
                person_id=candidate.person_id,
                run_id=run_id,
                lead_assessment_id=candidate.lead_assessment_id,
                tier=candidate.tier,
                from_status="pending",
                to_status="emitted",
                reason="digest_rendered",
                occurred_at=occurred_at,
            )
            mark_digest_queue_emitted(
                connection,
                person_id=candidate.person_id,
                last_material_change_at=occurred_at,
            )
            emitted.append((candidate, transition_id))
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return emitted


def record_digest_with_entries(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    file_path: str,
    timezone: str,
    window_start: str,
    window_end: str,
    run_state: str,
    content_hash: str,
    created_at: str,
    emitted: Sequence[tuple[ShortlistCandidate, int]],
) -> int:
    """Insert the `digest` row for a written digest file and one
    `digest_entry` per already-emitted shortlist candidate, in a single
    transaction. Called after `reporting.digest.write_digest` returns, since
    only then are the real `file_path`/`content_hash` known -- see
    `emit_shortlist_entries` for why this manages its own transaction rather
    than the caller's.
    """
    connection.execute("BEGIN IMMEDIATE")
    try:
        digest_id = insert_digest(
            connection,
            run_id=run_id,
            file_path=file_path,
            timezone=timezone,
            window_start=window_start,
            window_end=window_end,
            run_state=run_state,
            content_hash=content_hash,
            created_at=created_at,
        )
        for ordinal, (candidate, transition_id) in enumerate(emitted, start=1):
            insert_digest_entry(
                connection,
                digest_id=digest_id,
                person_id=candidate.person_id,
                lead_assessment_id=candidate.lead_assessment_id,
                queue_transition_id=transition_id,
                ordinal=ordinal,
            )
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    return digest_id


def _days_between(earlier: str, later: str) -> int:
    delta = _parse_utc(later) - _parse_utc(earlier)
    return max(delta.days, 0)
