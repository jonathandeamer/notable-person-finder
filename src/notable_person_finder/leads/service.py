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


@dataclass(frozen=True, slots=True)
class _AggregatePayload:
    person_id: int
    wikipedia_outcome: str | None


def _extract_domain(url: str) -> str:
    if not url:
        return ""
    if "://" not in url:
        url = "https://" + url
    return urlsplit(url).netloc


def _canonical_domain_map(policy: SourcePolicy) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for rule in policy.rules:
        own_domain = getattr(rule, "host_exact", None) or getattr(
            rule, "host_suffix", None
        )
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
    policy: SourcePolicy,
    config: MainConfig,
) -> str:
    lead_cfg = config.tasks.aggregate_lead
    payload = {
        "article_assessment_ids": sorted(article_assessment_ids),
        "signal_ids": sorted(signal_ids),
        "wikipedia_outcome": wikipedia_outcome,
        "source_policy_fingerprint": policy.fingerprint,
        "promising_domain_threshold": lead_cfg.promising_domain_threshold,
        "starvation_days": lead_cfg.starvation_days,
        "reminder_interval_days": lead_cfg.reminder_interval_days,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
        return TaskPreparation(
            payload=_AggregatePayload(
                person_id=person_id, wikipedia_outcome=wikipedia_outcome
            ),
            reserved_nano_usd=0,
        )

    def execute(work_item: WorkItem, ordinal: int, prepared: object) -> TaskOutcome:
        assert isinstance(prepared, _AggregatePayload)
        articles = _load_article_evidence(
            connection,
            person_id=prepared.person_id,
            canonical_domains=canonical_domains,
        )
        signals = _load_signal_evidence(connection, person_id=prepared.person_id)
        outcome = aggregate_lead(
            articles=articles,
            signals=signals,
            wikipedia_outcome=prepared.wikipedia_outcome,
            promising_domain_threshold=config.tasks.aggregate_lead.promising_domain_threshold,
        )
        return TaskOutcome(state=WorkState.SUCCEEDED, reason=None, payload=outcome)

    def persist(work_item: WorkItem, outcome: TaskOutcome) -> None:
        person_id = work_item.subject_id
        if person_id is None:
            raise ValueError(
                "work_item.subject_id must be provided for aggregate_person_lead"
            )
        lead_outcome = outcome.payload
        assert isinstance(lead_outcome, LeadOutcome)

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
        policy=policy,
        config=config,
    )
    schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint=fingerprint,
        run_id=run_id,
        now=now,
    )
