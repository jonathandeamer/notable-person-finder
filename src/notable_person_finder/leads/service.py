"""aggregate_person_lead work-item handler and same-run scheduling hook.

K5: execute is pure computation over already-persisted rows -- no external
call, no attempt row, provider='local' (3b2's empty-candidate-create
convention). K4: the handler receives SourcePolicy from the caller's already-
loaded config; only the settlement-hook function below re-loads it, because
it runs inside another domain's transaction where the loaded object is not
in scope (matching wikipedia/service.py's
_schedule_coverage_after_wikipedia_settled).
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import urlsplit

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import (
    SourcePolicy,
    SourcePolicyError,
    load_source_policy,
)
from notable_person_finder.leads.aggregation import (
    ArticleEvidence,
    LeadOutcome,
    SignalEvidence,
    aggregate_lead,
)
from notable_person_finder.leads.queue import decide_queue_transition
from notable_person_finder.leads.repository import (
    fetch_current_lead_assessment_material_fingerprint,
    fetch_prior_queue_state,
    insert_lead_assessment,
    insert_queue_transition,
    remove_digest_queue,
    upsert_digest_queue,
)
from notable_person_finder.runs import repository as runs_repository
from notable_person_finder.runs.clock import utc_timestamp
from notable_person_finder.runs.engine import (
    TaskHandler,
    TaskOutcome,
    TaskPreparation,
    WorkState,
)
from notable_person_finder.runs.models import WorkItem
from notable_person_finder.runs.scheduler import WorkerPool

AGGREGATE_PERSON_LEAD_TASK_TYPE = "aggregate_person_lead"
AGGREGATE_PERSON_LEAD_PRIORITY = 75
LOCAL_PROVIDER = "local"

# K6 local-refusal prefix, matching wikipedia/service.py's
# MATCH_PREPARE_REFUSED_PREFIX: build_aggregate_person_lead_handler's
# `prepare` raises a ValueError with this prefix when the person's evidence
# fingerprint is unchanged since their last completed aggregation, so the
# run engine settles the item failed_permanent instead of re-aggregating.
AGGREGATE_PREPARE_REFUSED_PREFIX = "aggregate_prepare_refused:"


@dataclass(frozen=True, slots=True)
class _AggregatePayload:
    person_id: int
    articles: list[ArticleEvidence]
    signals: list[SignalEvidence]
    wikipedia_outcome: str | None
    assessment_terminal: bool
    material_fingerprint: str


@dataclass(frozen=True, slots=True)
class _ExecuteResult:
    lead_outcome: LeadOutcome
    material_fingerprint: str


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    if "://" not in url:
        url = "https://" + url
    return urlsplit(url).netloc


def _canonical_domain_map(policy: SourcePolicy) -> dict[str, str]:
    """Map each rule's own `host_exact`/`host_suffix` match to a canonical
    domain.

    `host_exact`/`host_suffix` live on `PolicyRule.match` (a `PolicyMatch`),
    not on `PolicyRule` itself, so they are read via `rule.match.host_exact`
    / `rule.match.host_suffix` -- real, statically-known attribute paths, not
    duck-typed ones.

    `PolicyRule` has no `canonical_domain` field (a separately-deferred K1
    schema gap; do not add it here -- see docs/architecture/known-gaps.md).
    So this
    only collapses two rules that already match the SAME `host_exact`/
    `host_suffix` value to that value; it cannot yet alias two genuinely
    different hosts to one canonical domain.
    """
    mapping: dict[str, str] = {}
    for rule in policy.rules:
        own_domain = rule.match.host_exact or rule.match.host_suffix
        canonical = getattr(rule, "canonical_domain", None) or own_domain
        if own_domain and canonical:
            mapping[own_domain] = canonical
    return mapping


def _load_article_evidence(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    canonical_domains: dict[str, str],
) -> list[ArticleEvidence]:
    rows = connection.execute(
        """
        SELECT id, screening_rule_status, person_relation, coverage_depth,
               content_types_json, subject_relationship, source_policy_fingerprint
        FROM person_article_assessment
        WHERE person_id = ? AND disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    evidence: list[ArticleEvidence] = []
    for (
        assessment_id,
        screening_status,
        person_relation,
        coverage_depth,
        content_types_json,
        subject_relationship,
        _fingerprint,
    ) in rows:
        content_types = json.loads(content_types_json) if content_types_json else []
        domain_row = connection.execute(
            """
            SELECT ca.canonical_url
            FROM person_article_assessment paa
            JOIN canonical_article ca ON ca.id = paa.canonical_article_id
            WHERE paa.id = ?
            """,
            (assessment_id,),
        ).fetchone()
        url = domain_row[0] if domain_row else ""
        domain = _extract_domain(url)
        evidence.append(
            ArticleEvidence(
                person_article_assessment_id=assessment_id,
                domain=domain,
                canonical_domain=canonical_domains.get(domain, domain),
                same_person=(person_relation == "same_person"),
                coverage_depth=coverage_depth,
                content_qualifying=bool(content_types),
                editorially_independent=(
                    subject_relationship == "editorially_independent"
                ),
                screening_status=screening_status,
            )
        )
    return evidence


def _load_signal_evidence(
    connection: sqlite3.Connection, *, person_id: int
) -> list[SignalEvidence]:
    rows = connection.execute(
        """
        SELECT s.id, s.signal_kind, s.category
        FROM article_assessment_signal s
        JOIN person_article_assessment paa ON paa.id = s.assessment_id
        WHERE paa.person_id = ? AND paa.disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    return [
        SignalEvidence(
            article_assessment_signal_id=r[0],
            signal_kind=r[1],
            category=r[2],
            transferable=True,
        )
        for r in rows
    ]


def _compute_material_fingerprint(
    *,
    article_assessment_ids: list[int],
    signal_ids: list[int],
    wikipedia_outcome: str | None,
    assessment_terminal: bool = True,
    policy: SourcePolicy,
    config: MainConfig,
) -> str:
    lead_cfg = config.tasks.aggregate_lead
    payload = {
        "article_assessment_ids": sorted(article_assessment_ids),
        "signal_ids": sorted(signal_ids),
        "wikipedia_outcome": wikipedia_outcome,
        "assessment_terminal": assessment_terminal,
        "source_policy_fingerprint": policy.fingerprint,
        "promising_domain_threshold": lead_cfg.promising_domain_threshold,
        "starvation_days": lead_cfg.starvation_days,
        "reminder_interval_days": lead_cfg.reminder_interval_days,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _assessment_terminal_for_person(
    connection: sqlite3.Connection, *, person_id: int
) -> bool:
    row = connection.execute(
        """
        SELECT status
          FROM person_coverage_plan
         WHERE person_id = ?
           AND status IN ('completed', 'incomplete', 'failed')
         ORDER BY completed_at DESC, id DESC
         LIMIT 1
        """,
        (person_id,),
    ).fetchone()
    if row is None:
        return True
    return row["status"] == "completed"


def schedule_aggregate_person_lead(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    material_fingerprint: str,
    run_id: int,
    now: str,
) -> int:
    return runs_repository.schedule_work(
        connection,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        fingerprint=material_fingerprint,
        # required=True: the K6 unchanged-fingerprint case is now filtered
        # out before scheduling (see _schedule_lead_aggregation_after_settled
        # and seed_lead_aggregation, which compare the freshly computed
        # fingerprint against fetch_current_lead_assessment_material_
        # fingerprint and skip calling this function entirely when they
        # match), so a work item only ever reaches here when the person's
        # evidence actually changed. A failed_permanent settlement from here
        # on is therefore a genuine, unexpected aggregation bug, not routine
        # steady state -- required=False previously made exactly that kind
        # of failure invisible to RunCounters (it landed in neither
        # optional_succeeded nor any digest-visible failure count) and let
        # one work_item row accumulate per person per run forever, even when
        # nothing had changed. prepare()'s own refusal check stays as a
        # defensive backstop for a race between the skip-check here and a
        # concurrent settlement, but should rarely if ever fire now.
        required=True,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        eligible_at=now,
        run_id=run_id,
        now=now,
    )


def _claimed_run_id(connection: sqlite3.Connection, work_item_id: int) -> int:
    row = connection.execute(
        "SELECT claimed_by_run_id FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is None or row["claimed_by_run_id"] is None:
        attempt = connection.execute(
            """
            SELECT run_id FROM attempt
             WHERE work_item_id = ?
             ORDER BY id DESC LIMIT 1
            """,
            (work_item_id,),
        ).fetchone()
        if attempt is not None:
            return int(attempt["run_id"])
        raise ValueError(f"work item {work_item_id} is not claimed by a run")
    return int(row["claimed_by_run_id"])


def _observed_at_for_prepare(connection: sqlite3.Connection, work_item_id: int) -> str:
    row = connection.execute(
        "SELECT updated_at FROM work_item WHERE id = ?",
        (work_item_id,),
    ).fetchone()
    if row is not None and row["updated_at"]:
        return str(row["updated_at"])
    return utc_timestamp(datetime.now(tz=UTC))


def build_aggregate_person_lead_handler(
    connection: sqlite3.Connection,
    *,
    config: MainConfig,
    policy: SourcePolicy,
) -> TaskHandler:
    canonical_domains = _canonical_domain_map(policy)

    def prepare(work_item: WorkItem) -> TaskPreparation:
        person_id = work_item.subject_id
        if person_id is None:
            raise ValueError(
                "work_item.subject_id must be provided for aggregate_person_lead"
            )
        row = connection.execute(
            "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
            (person_id,),
        ).fetchone()
        wikipedia_outcome = None
        if row and row["current_wikipedia_identity_observation_id"] is not None:
            outcome_row = connection.execute(
                """
                SELECT semantic_outcome FROM wikipedia_identity_observation
                 WHERE id = ?
                """,
                (row["current_wikipedia_identity_observation_id"],),
            ).fetchone()
            if outcome_row:
                wikipedia_outcome = outcome_row["semantic_outcome"]
        articles = _load_article_evidence(
            connection,
            person_id=person_id,
            canonical_domains=canonical_domains,
        )
        signals = _load_signal_evidence(connection, person_id=person_id)
        assessment_terminal = _assessment_terminal_for_person(
            connection, person_id=person_id
        )

        material_fingerprint = _compute_material_fingerprint(
            article_assessment_ids=[a.person_article_assessment_id for a in articles],
            signal_ids=[s.article_assessment_signal_id for s in signals],
            wikipedia_outcome=wikipedia_outcome,
            assessment_terminal=assessment_terminal,
            policy=policy,
            config=config,
        )
        stored_fingerprint = fetch_current_lead_assessment_material_fingerprint(
            connection, person_id=person_id
        )
        if (
            stored_fingerprint is not None
            and stored_fingerprint == material_fingerprint
        ):
            raise ValueError(
                f"{AGGREGATE_PREPARE_REFUSED_PREFIX}unchanged_fingerprint "
                f"person {person_id}"
            )

        return TaskPreparation(
            payload=_AggregatePayload(
                person_id=person_id,
                articles=articles,
                signals=signals,
                wikipedia_outcome=wikipedia_outcome,
                assessment_terminal=assessment_terminal,
                material_fingerprint=material_fingerprint,
            ),
            reserved_nano_usd=0,
        )

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        assert isinstance(prepared, _AggregatePayload)
        outcome = aggregate_lead(
            articles=prepared.articles,
            signals=prepared.signals,
            wikipedia_outcome=prepared.wikipedia_outcome,
            promising_domain_threshold=config.tasks.aggregate_lead.promising_domain_threshold,
            assessment_terminal=prepared.assessment_terminal,
        )
        return TaskOutcome(
            state=WorkState.SUCCEEDED,
            reason=None,
            payload=_ExecuteResult(
                lead_outcome=outcome,
                material_fingerprint=prepared.material_fingerprint,
            ),
        )

    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        person_id = work_item.subject_id
        if person_id is None:
            raise ValueError(
                "work_item.subject_id must be provided for aggregate_person_lead"
            )
        result = outcome.payload
        assert isinstance(result, _ExecuteResult)
        lead_outcome = result.lead_outcome

        run_id = _claimed_run_id(connection, work_item.id)
        now = _observed_at_for_prepare(connection, work_item.id)

        ordering_factors = json.dumps(
            {
                "qualifying_domain_count": lead_outcome.qualifying_domain_count,
                "wikipedia_outcome": lead_outcome.wikipedia_outcome,
            },
            sort_keys=True,
        )
        lead_id = insert_lead_assessment(
            connection,
            person_id=person_id,
            run_id=run_id,
            outcome=lead_outcome,
            lead_policy_fingerprint=policy.fingerprint,
            ordering_factors_json=ordering_factors,
            decided_at=now,
            material_fingerprint=result.material_fingerprint,
        )
        prior = fetch_prior_queue_state(connection, person_id=person_id)
        matching_page_found = lead_outcome.wikipedia_outcome == "matching_page_found"
        decision = decide_queue_transition(
            prior=prior,
            outcome=lead_outcome,
            matching_page_found=matching_page_found,
            now=now,
        )
        tier = (
            lead_outcome.outcome
            if lead_outcome.outcome in ("promising_lead", "possible_lead")
            else (prior.tier if prior else "possible_lead")
        )
        if decision.should_upsert:
            upsert_digest_queue(
                connection,
                person_id=person_id,
                status=decision.to_status or "pending",
                tier=tier,
                eligibility_reason=decision.eligibility_reason or "new",
                lead_assessment_id=lead_id,
                first_pending_at=decision.first_pending_at or now,
                last_material_change_at=now,
            )
            insert_queue_transition(
                connection,
                person_id=person_id,
                run_id=run_id,
                lead_assessment_id=lead_id,
                tier=tier,
                from_status=prior.status if prior else None,
                to_status=decision.to_status or "pending",
                reason=decision.eligibility_reason or "new",
                occurred_at=now,
            )
        elif decision.should_remove:
            remove_digest_queue(
                connection,
                person_id=person_id,
                removed_reason=decision.removed_reason or "matching_page_found",
                last_material_change_at=now,
            )
            insert_queue_transition(
                connection,
                person_id=person_id,
                run_id=run_id,
                lead_assessment_id=lead_id,
                tier=prior.tier if prior else tier,
                from_status=prior.status if prior else None,
                to_status="removed",
                reason=decision.removed_reason or "matching_page_found",
                occurred_at=now,
            )

    return TaskHandler(
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        provider=LOCAL_PROVIDER,
        operation="aggregate",
        execute=execute,
        prepare=prepare,
        persist=persist,
        persist_failure=None,
        destination_host=None,
        reserved_nano_usd=0,
        pool=WorkerPool.HTTP,
        ready=lambda _run_id: True,
    )


def reaggregate_person_lead(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    canonical_domains = _canonical_domain_map(policy)
    row = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    wikipedia_outcome = None
    if row and row["current_wikipedia_identity_observation_id"] is not None:
        outcome_row = connection.execute(
            """
            SELECT semantic_outcome FROM wikipedia_identity_observation
             WHERE id = ?
            """,
            (row["current_wikipedia_identity_observation_id"],),
        ).fetchone()
        if outcome_row:
            wikipedia_outcome = outcome_row["semantic_outcome"]
    articles = _load_article_evidence(
        connection,
        person_id=person_id,
        canonical_domains=canonical_domains,
    )
    signals = _load_signal_evidence(connection, person_id=person_id)
    assessment_terminal = _assessment_terminal_for_person(
        connection, person_id=person_id
    )

    lead_outcome = aggregate_lead(
        articles=articles,
        signals=signals,
        wikipedia_outcome=wikipedia_outcome,
        promising_domain_threshold=config.tasks.aggregate_lead.promising_domain_threshold,
        assessment_terminal=assessment_terminal,
    )

    material_fingerprint = _compute_material_fingerprint(
        article_assessment_ids=[a.person_article_assessment_id for a in articles],
        signal_ids=[s.article_assessment_signal_id for s in signals],
        wikipedia_outcome=wikipedia_outcome,
        assessment_terminal=assessment_terminal,
        policy=policy,
        config=config,
    )

    ordering_factors = json.dumps(
        {
            "qualifying_domain_count": lead_outcome.qualifying_domain_count,
            "wikipedia_outcome": lead_outcome.wikipedia_outcome,
        },
        sort_keys=True,
    )
    lead_id = insert_lead_assessment(
        connection,
        person_id=person_id,
        run_id=run_id,
        outcome=lead_outcome,
        lead_policy_fingerprint=policy.fingerprint,
        ordering_factors_json=ordering_factors,
        decided_at=now,
        material_fingerprint=material_fingerprint,
    )
    prior = fetch_prior_queue_state(connection, person_id=person_id)
    matching_page_found = lead_outcome.wikipedia_outcome == "matching_page_found"
    decision = decide_queue_transition(
        prior=prior,
        outcome=lead_outcome,
        matching_page_found=matching_page_found,
        now=now,
    )
    tier = (
        lead_outcome.outcome
        if lead_outcome.outcome in ("promising_lead", "possible_lead")
        else (prior.tier if prior else "possible_lead")
    )
    if decision.should_upsert:
        upsert_digest_queue(
            connection,
            person_id=person_id,
            status=decision.to_status or "pending",
            tier=tier,
            eligibility_reason=decision.eligibility_reason or "new",
            lead_assessment_id=lead_id,
            first_pending_at=decision.first_pending_at or now,
            last_material_change_at=now,
        )
        insert_queue_transition(
            connection,
            person_id=person_id,
            run_id=run_id,
            lead_assessment_id=lead_id,
            tier=tier,
            from_status=prior.status if prior else None,
            to_status=decision.to_status or "pending",
            reason=decision.eligibility_reason or "new",
            occurred_at=now,
        )
    elif decision.should_remove:
        remove_digest_queue(
            connection,
            person_id=person_id,
            removed_reason=decision.removed_reason or "matching_page_found",
            last_material_change_at=now,
        )
        insert_queue_transition(
            connection,
            person_id=person_id,
            run_id=run_id,
            lead_assessment_id=lead_id,
            tier=prior.tier if prior else tier,
            from_status=prior.status if prior else None,
            to_status="removed",
            reason=decision.removed_reason or "matching_page_found",
            occurred_at=now,
        )


def _schedule_lead_aggregation_after_settled(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    run_id: int,
    config: MainConfig,
    now: str,
) -> None:
    try:
        policy = load_source_policy(config.source_policy_file)
    except (SourcePolicyError, OSError):
        return

    articles = connection.execute(
        """
        SELECT id FROM person_article_assessment
        WHERE person_id = ? AND disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    article_assessment_ids = [r[0] for r in articles]

    signals = connection.execute(
        """
        SELECT s.id
        FROM article_assessment_signal s
        JOIN person_article_assessment paa ON paa.id = s.assessment_id
        WHERE paa.person_id = ? AND paa.disposition = 'completed'
        """,
        (person_id,),
    ).fetchall()
    signal_ids = [r[0] for r in signals]
    assessment_terminal = _assessment_terminal_for_person(
        connection, person_id=person_id
    )

    row = connection.execute(
        "SELECT current_wikipedia_identity_observation_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    wikipedia_outcome = None
    if row and row["current_wikipedia_identity_observation_id"] is not None:
        outcome_row = connection.execute(
            "SELECT semantic_outcome FROM wikipedia_identity_observation WHERE id = ?",
            (row["current_wikipedia_identity_observation_id"],),
        ).fetchone()
        wikipedia_outcome = outcome_row["semantic_outcome"] if outcome_row else None

    fingerprint = _compute_material_fingerprint(
        article_assessment_ids=article_assessment_ids,
        signal_ids=signal_ids,
        wikipedia_outcome=wikipedia_outcome,
        assessment_terminal=assessment_terminal,
        policy=policy,
        config=config,
    )

    # K6 steady state: skip scheduling entirely when nothing about this
    # person's evidence has changed since their last completed aggregation,
    # rather than scheduling a work item that prepare()'s own refusal check
    # would only reject anyway. This is what makes required=True safe on
    # schedule_aggregate_person_lead (a failed_permanent settlement is then
    # a genuine bug, not routine no-op churn) and what stops one work_item
    # row from accumulating per person on every run forever, even when
    # nothing changed -- seed_lead_aggregation sweeps every person with
    # completed coverage evidence unconditionally on every run, so without
    # this check the table grows without bound in steady state.
    stored_fingerprint = fetch_current_lead_assessment_material_fingerprint(
        connection, person_id=person_id
    )
    if stored_fingerprint is not None and stored_fingerprint == fingerprint:
        return

    schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint=fingerprint,
        run_id=run_id,
        now=now,
    )


def seed_lead_aggregation(
    connection: sqlite3.Connection, *, run_id: int, config: MainConfig, now: str
) -> None:
    """Top-of-run sweep for any person whose settlement hook was missed by a
    crash window (same at-least-once posture as every other milestone; see
    docs/architecture/at-least-once-execution.md).

    Reuses `_schedule_lead_aggregation_after_settled` for every person with
    terminal coverage or completed coverage evidence, so scheduling stays
    fingerprint-deduplicated against any work item a hook already enqueued
    this run or a prior one -- matching `seed_coverage_research`'s
    iterate-all-then-let-dedup-handle-it shape rather than trying to detect
    "missed" people directly.
    """
    rows = connection.execute(
        """
        SELECT DISTINCT person_id
          FROM person_article_assessment
         WHERE disposition = 'completed'
        UNION
        SELECT DISTINCT person_id
          FROM person_coverage_plan
         WHERE status IN ('completed', 'incomplete', 'failed')
        """
    ).fetchall()
    for row in rows:
        _schedule_lead_aggregation_after_settled(
            connection,
            person_id=row["person_id"],
            run_id=run_id,
            config=config,
            now=now,
        )
