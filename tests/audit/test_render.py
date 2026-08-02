"""Render purity and constructed-view-model tests for `audit/render.py`.

No database access anywhere in this module: every model here is built by
hand from `audit/models.py` with distinct field values, the same technique
`tests/leads/test_digest_status.py` uses with nine distinct counters --
distinct values are what catch a swapped-field defect; equal or default
values let it survive.
"""

from __future__ import annotations

import inspect

from notable_person_finder.audit import render
from notable_person_finder.audit.models import (
    ArticleTargetLine,
    AssessmentLine,
    AssessmentSignalLine,
    AttemptAudit,
    AttemptLine,
    BraveSearchObservationLine,
    BudgetSummary,
    ConfigurationProvenance,
    CoverageEvidence,
    CoveragePlanLine,
    CoverageQueryFormLine,
    DigestEntryLine,
    FailureGroup,
    LeadLine,
    MentionLine,
    PersonAudit,
    PersonIdentity,
    QueueEvidence,
    QueueRowLine,
    QueueTransitionLine,
    RelationLine,
    ReportingResult,
    ResolutionLine,
    RunAudit,
    RunHeader,
    ScreeningLine,
    SourcedNameLine,
    Transition,
    WikipediaEvidence,
    WikipediaIdentityObservationLine,
    WikipediaPageFactsBatchLine,
    WikipediaPlanLine,
    WikipediaQueryFormLine,
    WikipediaSearchObservationLine,
    WorkItemLine,
    WorkOutcomes,
)


def test_render_functions_take_no_connection() -> None:
    # render.py must be callable with view models alone. A connection
    # parameter would mean query logic leaked into the renderer.
    for name in (
        "render_run_audit",
        "render_attempt_audit",
        "render_person_audit",
    ):
        signature = inspect.signature(getattr(render, name))
        assert "connection" not in signature.parameters, name


def test_render_run_audit_renders_distinct_field_values() -> None:
    audit = RunAudit(
        run=RunHeader(
            id=101,
            state="complete",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            timezone="Europe/London",
            window_start="2026-01-01",
            window_end="2026-01-02",
        ),
        configuration=ConfigurationProvenance(
            snapshot_id=202,
            fingerprint="config-fp-303",
            created_at="2026-01-01T00:00:01+00:00",
            canonical_json="{}",
        ),
        transitions=(
            Transition(
                state="running",
                reason="scheduled-start",
                occurred_at="2026-01-01T00:00:02+00:00",
            ),
        ),
        work_outcomes=WorkOutcomes(
            counts=(("fetch_feed", "complete", 7),),
            failed_permanent=(
                WorkItemLine(
                    id=404,
                    task_type="fetch_feed",
                    subject_kind="feed",
                    subject_id=505,
                    fingerprint="wi-fp-606",
                    state="failed_permanent",
                    reason="dns-failure",
                ),
            ),
            deferred=(
                WorkItemLine(
                    id=707,
                    task_type="brave_web_search",
                    subject_kind="person",
                    subject_id=808,
                    fingerprint="wi-fp-909",
                    state="deferred",
                    reason="budget-exhausted",
                ),
            ),
        ),
        attempts=(
            AttemptLine(
                id=1001,
                work_item_id=404,
                provider="brave",
                operation="search",
                ordinal=1,
                outcome="failed_transient",
                failure_category="timeout",
                provider_status=503,
                latency_ms=1234,
                response_bytes=5678,
                destination_host="api.search.brave.com",
                reserved_nano_usd=9_000,
                actual_nano_usd=8_000,
            ),
        ),
        failures=(
            FailureGroup(
                failure_category="timeout",
                count=3,
                example_provider="brave",
                example_operation="search",
                outcomes=("failed_transient", "failed_permanent"),
            ),
        ),
        budget=BudgetSummary(
            limit_nano_usd=50_000,
            reserved_nano_usd=9_000,
            actual_nano_usd=8_000,
            attempt_actual_sum_nano_usd=6_000,
        ),
        reporting=ReportingResult(
            digest_path="/data/digests/2026-01-01.md",
            digest_sha256="digest-sha-abc123",
            run_state="complete",
            entry_count=12,
        ),
        unavailable_sections=(),
    )

    text = render.render_run_audit(audit)

    assert "  id: 101" in text
    assert "  state: complete" in text
    assert "  timezone: Europe/London" in text
    assert "  window: 2026-01-01 .. 2026-01-02" in text
    assert "  started_at: 2026-01-01T00:00:00+00:00" in text
    assert "  finished_at: 2026-01-01T00:05:00+00:00" in text
    assert "  snapshot_id: 202" in text
    assert "  fingerprint: config-fp-303" in text
    assert "  created_at: 2026-01-01T00:00:01+00:00" in text
    assert "  2026-01-01T00:00:02+00:00: running (scheduled-start)" in text
    assert "  fetch_feed / complete: 7" in text
    assert "    #404 fetch_feed feed:505 -> dns-failure" in text
    assert "    #707 brave_web_search person:808 -> budget-exhausted" in text
    assert (
        "  #1001 work_item=404 ordinal=1 brave/search: "
        "failed_transient failure_category=timeout" in text
    )
    assert (
        "    provider_status=503 latency_ms=1234 response_bytes=5678 "
        "destination_host=api.search.brave.com" in text
    )
    assert (
        "  timeout: 3 (outcomes=failed_transient, failed_permanent, "
        "e.g. brave/search)" in text
    )
    assert "  limit: $0.000050 (50000 nano-USD)" in text
    assert "  reserved: $0.000009 (9000 nano-USD)" in text
    assert "  actual (run total): $0.000008 (8000 nano-USD)" in text
    assert "  actual (summed from attempts): $0.000006 (6000 nano-USD)" in text
    # actual_nano_usd (8000) and attempt_actual_sum_nano_usd (7500) are
    # deliberately distinct: equal values would leave the divergence branch
    # at render.py:201-205 unexercised, and the "summed from attempts" line
    # indistinguishable from a swapped field. See the matching no-divergence
    # test below for the other side of this conditional.
    assert (
        "  divergence: run total and attempt sum disagree "
        "(cross-check failed, both values shown above)" in text
    )
    assert "  digest_path: /data/digests/2026-01-01.md" in text
    assert "  digest_sha256: digest-sha-abc123" in text
    assert "  entry_count: 12" in text
    assert "  see: notable digest show 101" in text


def test_render_run_audit_budget_agreement_shows_no_divergence_warning() -> None:
    # The other side of the render.py:201-205 conditional: when the run
    # total and the attempt sum agree, the divergence warning line must be
    # absent. Paired with the distinct-values assertion above, this pins
    # the branch in both directions.
    budget = BudgetSummary(
        limit_nano_usd=50_000,
        reserved_nano_usd=9_000,
        actual_nano_usd=8_000,
        attempt_actual_sum_nano_usd=8_000,
    )
    audit = RunAudit(
        run=RunHeader(
            id=901,
            state="complete",
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            timezone="UTC",
            window_start="2026-01-01",
            window_end="2026-01-02",
        ),
        configuration=None,
        transitions=(),
        work_outcomes=WorkOutcomes(counts=(), failed_permanent=(), deferred=()),
        attempts=(),
        failures=(),
        budget=budget,
        reporting=None,
        unavailable_sections=(),
    )

    text = render.render_run_audit(audit)

    assert "  actual (run total): $0.000008 (8000 nano-USD)" in text
    assert "  actual (summed from attempts): $0.000008 (8000 nano-USD)" in text
    assert "divergence" not in text


def test_render_attempt_audit_renders_distinct_field_values() -> None:
    attempt = AttemptLine(
        id=2001,
        work_item_id=2002,
        provider="openrouter",
        operation="generate",
        ordinal=2,
        outcome="succeeded",
        failure_category=None,
        provider_status=200,
        latency_ms=999,
        response_bytes=4321,
        destination_host="openrouter.ai",
        reserved_nano_usd=11_000,
        actual_nano_usd=10_500,
    )
    retry = AttemptLine(
        id=2003,
        work_item_id=2002,
        provider="openrouter",
        operation="generate",
        ordinal=1,
        outcome="failed_transient",
        failure_category="rate_limited",
        provider_status=429,
        latency_ms=111,
        response_bytes=222,
        destination_host="openrouter.ai",
        reserved_nano_usd=11_000,
        actual_nano_usd=None,
    )
    work_item = WorkItemLine(
        id=2002,
        task_type="assess_article",
        subject_kind="article",
        subject_id=2004,
        fingerprint="wi-fp-2005",
        state="complete",
        reason="scheduled-normally",
    )
    audit = AttemptAudit(
        attempt=attempt,
        work_item=work_item,
        retry_history=(retry,),
        result_rows=({"disposition": "promising_lead", "score": 7},),
        binding=None,
        caveat="fetch_feed persists only the derived feed-fetch summary",
        no_result_row=False,
    )

    text = render.render_attempt_audit(audit)

    assert "  #2001 work_item=2002 ordinal=2 openrouter/generate: succeeded" in text
    assert (
        "    provider_status=200 latency_ms=999 response_bytes=4321 "
        "destination_host=openrouter.ai" in text
    )
    assert "  id: 2002" in text
    assert "  task_type: assess_article" in text
    assert "  subject: article:2004" in text
    assert "  fingerprint: wi-fp-2005" in text
    assert "  state: complete" in text
    assert "  reason: scheduled-normally" in text
    assert (
        "  #2003 work_item=2002 ordinal=1 openrouter/generate: "
        "failed_transient failure_category=rate_limited" in text
    )
    assert "  caveat: fetch_feed persists only the derived feed-fetch summary" in text
    assert "    disposition: promising_lead" in text
    assert "    score: 7" in text


def test_render_person_audit_renders_distinct_field_values() -> None:
    identity = PersonIdentity(
        id=3001,
        display_name="Ada Lovelace",
        identity_fingerprint="identity-fp-3002",
        created_at="2026-01-02T00:00:00+00:00",
        created_by_run_id=3003,
        merged_into_person_id=None,
        current_wikipedia_identity_observation_id=3004,
        current_lead_assessment_id=3005,
    )
    audit = PersonAudit(
        identity=identity,
        merged_into=None,
        merged_by_run_id=None,
        sourced_names=(
            SourcedNameLine(
                id=3101,
                exact_name="Ada Lovelace",
                search_name="ada lovelace",
                match_key="ada-lovelace",
                kind="exact",
                origin_kind="mention",
                origin_mention_id=3102,
                first_observed_at="2026-01-02T00:00:01+00:00",
                last_observed_at="2026-01-02T00:00:02+00:00",
            ),
        ),
        relations=(
            RelationLine(
                id=3201,
                other_person_id=3202,
                kind="possible_same_person",
                status="active",
                created_at="2026-01-02T00:00:03+00:00",
                created_by_run_id=3203,
                created_by_observation_id=3204,
                closed_at=None,
                closed_by_observation_id=None,
            ),
        ),
        mentions=(
            MentionLine(
                id=3301,
                triage_observation_id=3302,
                source_item_id=3303,
                ordinal=1,
                exact_name="A. Lovelace",
                search_name="a lovelace",
                outcome="research",
                rationale="mentioned in an article about early computing",
                current_entity_resolution_observation_id=3304,
                current_semantic_outcome="created_new",
            ),
        ),
        resolutions=(
            ResolutionLine(
                id=3401,
                person_mention_id=3301,
                person_relation_id=None,
                disposition="created_new",
                semantic_outcome="possible_same_person",
                candidate_person_ids_json="[]",
                selected_person_id=None,
                created_person_id=3001,
                prompt_hash="prompt-hash-3402",
                schema_version=1,
                task_fingerprint="task-fp-3403",
                rationale="no existing candidates matched",
            ),
        ),
        wikipedia=WikipediaEvidence(
            plans=(
                WikipediaPlanLine(
                    id=3501,
                    status="complete",
                    created_at="2026-01-02T00:00:04+00:00",
                    completed_at="2026-01-02T00:00:05+00:00",
                    failure_category=None,
                ),
            ),
            query_forms=(
                WikipediaQueryFormLine(
                    id=3502,
                    plan_id=3501,
                    ordinal=1,
                    variant_kind="exact",
                    query_text="Ada Lovelace",
                    status="complete",
                    hit_count=3,
                ),
            ),
            search_observations=(
                WikipediaSearchObservationLine(
                    id=3503,
                    query_form_id=3502,
                    query_text="Ada Lovelace",
                    hit_count=3,
                    truncated=False,
                    response_complete=True,
                ),
            ),
            page_facts_batches=(
                WikipediaPageFactsBatchLine(
                    id=3504,
                    plan_id=3501,
                    ordinal=1,
                    status="complete",
                    wave=1,
                ),
            ),
            identity_observations=(
                WikipediaIdentityObservationLine(
                    id=3505,
                    disposition="matching_page_found",
                    semantic_outcome="uncertain_identity",
                    matched_mediawiki_page_id=3506,
                    task_fingerprint="wiki-task-fp-3507",
                    rationale="page facts uniquely match the person",
                    is_current=True,
                ),
            ),
        ),
        coverage=CoverageEvidence(
            plans=(
                CoveragePlanLine(
                    id=3601,
                    status="complete",
                    created_at="2026-01-02T00:00:06+00:00",
                    completed_at="2026-01-02T00:00:07+00:00",
                    failure_category=None,
                ),
            ),
            query_forms=(
                CoverageQueryFormLine(
                    id=3602,
                    plan_id=3601,
                    ordinal=1,
                    stage=1,
                    variant_kind="exact_obituary",
                    query_text="Ada Lovelace obituary",
                    status="complete",
                    result_count=2,
                ),
            ),
            brave_observations=(
                BraveSearchObservationLine(
                    id=3603,
                    query_form_id=3602,
                    query_text="Ada Lovelace obituary",
                    result_count=2,
                    truncated=False,
                ),
            ),
            screenings=(
                ScreeningLine(
                    id=3604,
                    plan_id=3601,
                    url="https://example.org/ada-lovelace",
                    publisher_key="example.org",
                    rule_id="rule-42",
                    rule_status="curated_eligible",
                    source_policy_fingerprint="policy-fp-3605",
                ),
            ),
            article_targets=(
                ArticleTargetLine(
                    id=3606,
                    plan_id=3601,
                    canonical_article_id=3607,
                    request_url="https://example.org/ada-lovelace",
                    selection_reason="highest-ranked-eligible",
                    status="fetched",
                ),
            ),
        ),
        assessments=(
            AssessmentLine(
                id=3701,
                canonical_article_id=3607,
                disposition="assessed",
                person_relation="subject",
                coverage_depth="substantial",
                subject_relationship="primary_subject",
                content_types_json='["profile"]',
                rationale="the article profiles the person directly",
                failure_category=None,
                signals=(
                    AssessmentSignalLine(
                        id=3702,
                        signal_kind="notability",
                        category="recognition",
                        claim="received a named award",
                        supporting_passage_ids_json="[1, 2]",
                        ordinal=1,
                    ),
                ),
            ),
        ),
        leads=(
            LeadLine(
                id=3801,
                outcome="promising_lead",
                qualifying_domain_count=2,
                incompleteness_reason=None,
                decided_at="2026-01-02T00:00:08+00:00",
                is_current=True,
            ),
        ),
        queue=QueueEvidence(
            row=QueueRowLine(
                status="pending",
                tier="standard",
                eligibility_reason="promising_lead",
                lead_assessment_id=3801,
                first_pending_at="2026-01-02T00:00:09+00:00",
                last_material_change_at="2026-01-02T00:00:10+00:00",
                removed_reason=None,
            ),
            transitions=(
                QueueTransitionLine(
                    id=3901,
                    run_id=3902,
                    from_status=None,
                    to_status="pending",
                    tier="standard",
                    reason="new promising lead",
                    occurred_at="2026-01-02T00:00:11+00:00",
                ),
            ),
        ),
        digest_entries=(
            DigestEntryLine(
                digest_id=4001,
                run_id=4002,
                ordinal=1,
                lead_assessment_id=3801,
                file_path="/data/digests/2026-01-02.md",
            ),
        ),
        unavailable_sections=(),
    )

    text = render.render_person_audit(audit)

    assert "  id: 3001" in text
    assert "  display_name: Ada Lovelace" in text
    assert "  identity_fingerprint: identity-fp-3002" in text
    assert "  created_by_run_id: 3003" in text
    assert "  status: canonical" in text
    assert (
        "  #3101 'Ada Lovelace' (exact, origin=mention) "
        "first=2026-01-02T00:00:01+00:00 last=2026-01-02T00:00:02+00:00" in text
    )
    assert (
        "  #3201 possible_same_person with person 3202: active "
        "created_by_run=3203" in text
    )
    assert (
        "  #3301 'A. Lovelace' outcome=research "
        "triage_observation=3302 source_item=3303" in text
    )
    assert "    current_semantic_outcome: created_new" in text
    assert (
        "  #3401 mention=3301 disposition=created_new "
        "semantic_outcome=possible_same_person" in text
    )
    assert "    selected_person=None created_person=3001 candidates=[]" in text
    assert "  plan #3501 status=complete" in text
    assert (
        "  query_form #3502 plan=3501 ordinal=1 variant=exact "
        "status=complete hit_count=3" in text
    )
    assert (
        "  identity_observation #3505 [CURRENT] disposition=matching_page_found "
        "semantic_outcome=uncertain_identity matched_page=3506" in text
    )
    assert "  plan #3601 status=complete created_at=2026-01-02T00:00:06+00:00" in text
    assert (
        "  query_form #3602 plan=3601 stage=1 variant=exact_obituary "
        "status=complete result_count=2" in text
    )
    assert (
        "  screening #3604 url=https://example.org/ada-lovelace "
        "rule=rule-42 status=curated_eligible policy_fingerprint=policy-fp-3605" in text
    )
    assert (
        "  article_target #3606 article=3607 reason=highest-ranked-eligible "
        "status=fetched url=https://example.org/ada-lovelace" in text
    )
    assert (
        "  #3701 article=3607 disposition=assessed person_relation=subject "
        "coverage_depth=substantial subject_relationship=primary_subject" in text
    )
    assert (
        "    signal #3702 notability/recognition: received a named award "
        "(passages=[1, 2])" in text
    )
    assert (
        "  #3801 [CURRENT] outcome=promising_lead qualifying_domain_count=2 "
        "decided_at=2026-01-02T00:00:08+00:00 incompleteness_reason=n/a" in text
    )
    assert (
        "  current row: status=pending tier=standard "
        "eligibility_reason=promising_lead lead_assessment=3801 "
        "first_pending_at=2026-01-02T00:00:09+00:00" in text
    )
    assert (
        "  transition #3901 run=3902 n/a -> pending tier=standard "
        "reason=new promising lead occurred_at=2026-01-02T00:00:11+00:00" in text
    )
    assert (
        "  digest=4001 run=4002 ordinal=1 lead_assessment=3801 "
        "file=/data/digests/2026-01-02.md" in text
    )
