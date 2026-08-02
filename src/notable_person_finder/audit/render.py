"""Pure text rendering for `notable audit run`.

No SQL, no connection parameter, no I/O: every function here maps a view
model to `str`. All queries live in `audit/repository.py`.
"""

from __future__ import annotations

import json
from datetime import datetime

from notable_person_finder.audit.models import (
    AttemptAudit,
    AttemptLine,
    PersonAudit,
    PersonIdentity,
    RunAudit,
    RunHeader,
)

# Section key -> the migration that introduced the table it depends on, used
# only to word the "section unavailable" fallback line (K3).
_MIGRATION_BY_SECTION = {
    "configuration": "0001",
    "transitions": "0002",
    "work outcomes": "0002",
    "attempts": "0002",
    "failures": "0002",
    "budget": "0002",
}


def _format_nano_usd(value: int | None) -> str:
    if value is None:
        return "not set"
    dollars = value / 1_000_000_000
    return f"${dollars:.6f} ({value} nano-USD)"


def _unavailable_line(section: str) -> str:
    migration = _MIGRATION_BY_SECTION.get(section, "unknown")
    return f"  section unavailable: schema predates migration {migration}"


def _format_duration(run: RunHeader) -> str:
    if run.finished_at is None:
        return "not finished"
    try:
        started = datetime.fromisoformat(run.started_at)
        finished = datetime.fromisoformat(run.finished_at)
    except ValueError:
        return "unknown"
    return f"{(finished - started).total_seconds():.3f}s"


def _format_pretty_json(canonical_json: str) -> str:
    try:
        parsed = json.loads(canonical_json)
    except json.JSONDecodeError:
        return canonical_json
    return json.dumps(parsed, indent=2, sort_keys=True)


def render_run_audit(audit: RunAudit) -> str:
    lines: list[str] = []
    unavailable = set(audit.unavailable_sections)

    lines.append("Run")
    lines.append(f"  id: {audit.run.id}")
    lines.append(f"  state: {audit.run.state}")
    lines.append(f"  timezone: {audit.run.timezone}")
    lines.append(f"  window: {audit.run.window_start} .. {audit.run.window_end}")
    lines.append(f"  started_at: {audit.run.started_at}")
    lines.append(f"  finished_at: {audit.run.finished_at or 'not finished'}")
    lines.append(f"  duration: {_format_duration(audit.run)}")
    lines.append("")

    lines.append("Configuration")
    if "configuration" in unavailable:
        lines.append(_unavailable_line("configuration"))
    elif audit.configuration is None:
        lines.append("  none recorded")
    else:
        configuration = audit.configuration
        lines.append(f"  snapshot_id: {configuration.snapshot_id}")
        lines.append(f"  fingerprint: {configuration.fingerprint}")
        lines.append(f"  created_at: {configuration.created_at}")
        lines.append("  canonical_json:")
        for json_line in _format_pretty_json(configuration.canonical_json).splitlines():
            lines.append(f"    {json_line}")
    lines.append("")

    lines.append("Transitions")
    if "transitions" in unavailable:
        lines.append(_unavailable_line("transitions"))
    elif not audit.transitions:
        lines.append("  none recorded")
    else:
        for transition in audit.transitions:
            reason = transition.reason or "no reason recorded"
            lines.append(f"  {transition.occurred_at}: {transition.state} ({reason})")
    lines.append("")

    lines.append("Work outcomes")
    if "work outcomes" in unavailable:
        lines.append(_unavailable_line("work outcomes"))
    else:
        outcomes = audit.work_outcomes
        if not outcomes.counts:
            lines.append("  none recorded")
        else:
            for task_type, state, count in outcomes.counts:
                lines.append(f"  {task_type} / {state}: {count}")
        if outcomes.failed_permanent:
            lines.append("  failed_permanent:")
            for item in outcomes.failed_permanent:
                reason = item.reason or "no reason recorded"
                lines.append(
                    f"    #{item.id} {item.task_type} "
                    f"{item.subject_kind}:{item.subject_id} -> {reason}"
                )
        if outcomes.deferred:
            lines.append("  deferred:")
            for item in outcomes.deferred:
                reason = item.reason or "no reason recorded"
                lines.append(
                    f"    #{item.id} {item.task_type} "
                    f"{item.subject_kind}:{item.subject_id} -> {reason}"
                )
    lines.append("")

    lines.append("Attempts")
    if "attempts" in unavailable:
        lines.append(_unavailable_line("attempts"))
    elif not audit.attempts:
        lines.append("  none recorded")
    else:
        for attempt in audit.attempts:
            outcome = attempt.outcome or "in flight"
            failure = (
                f" failure_category={attempt.failure_category}"
                if attempt.failure_category
                else ""
            )
            provider_status = (
                "n/a"
                if attempt.provider_status is None
                else str(attempt.provider_status)
            )
            latency_ms = (
                "n/a" if attempt.latency_ms is None else str(attempt.latency_ms)
            )
            response_bytes = (
                "n/a" if attempt.response_bytes is None else str(attempt.response_bytes)
            )
            destination_host = attempt.destination_host or "n/a"
            lines.append(
                f"  #{attempt.id} work_item={attempt.work_item_id} "
                f"ordinal={attempt.ordinal} {attempt.provider}/{attempt.operation}: "
                f"{outcome}{failure}"
            )
            lines.append(
                f"    provider_status={provider_status} latency_ms={latency_ms} "
                f"response_bytes={response_bytes} destination_host={destination_host}"
            )
            lines.append(
                f"    reserved={_format_nano_usd(attempt.reserved_nano_usd)} "
                f"actual={_format_nano_usd(attempt.actual_nano_usd)}"
            )
    lines.append("")

    lines.append("Failures")
    if "failures" in unavailable:
        lines.append(_unavailable_line("failures"))
    elif not audit.failures:
        lines.append("  none recorded")
    else:
        for failure in audit.failures:
            outcomes_text = ", ".join(failure.outcomes)
            lines.append(
                f"  {failure.failure_category}: {failure.count} "
                f"(outcomes={outcomes_text}, "
                f"e.g. {failure.example_provider}/{failure.example_operation})"
            )
    lines.append("")

    lines.append("Budget")
    if "budget" in unavailable:
        lines.append(_unavailable_line("budget"))
    else:
        budget = audit.budget
        lines.append(f"  limit: {_format_nano_usd(budget.limit_nano_usd)}")
        lines.append(f"  reserved: {_format_nano_usd(budget.reserved_nano_usd)}")
        lines.append(
            f"  actual (run total): {_format_nano_usd(budget.actual_nano_usd)}"
        )
        lines.append(
            "  actual (summed from attempts): "
            f"{_format_nano_usd(budget.attempt_actual_sum_nano_usd)}"
        )
        if budget.actual_nano_usd != budget.attempt_actual_sum_nano_usd:
            lines.append(
                "  divergence: run total and attempt sum disagree "
                "(cross-check failed, both values shown above)"
            )
    lines.append("")

    lines.append("Reporting")
    if audit.reporting is None:
        lines.append("  no digest recorded for this run")
    else:
        reporting = audit.reporting
        lines.append(f"  digest_path: {reporting.digest_path}")
        lines.append(f"  digest_sha256: {reporting.digest_sha256}")
        lines.append(f"  run_state: {reporting.run_state}")
        entry_count = (
            "unknown" if reporting.entry_count is None else str(reporting.entry_count)
        )
        lines.append(f"  entry_count: {entry_count}")
        lines.append(f"  see: notable digest show {audit.run.id}")

    return "\n".join(lines) + "\n"


_NO_RESULT_ROW_TEXT = (
    "no persisted result row; the attempt is the only durable evidence of this call"
)


def _format_attempt_line(prefix: str, attempt: AttemptLine) -> list[str]:
    lines: list[str] = []
    outcome = attempt.outcome or "in flight"
    failure = (
        f" failure_category={attempt.failure_category}"
        if attempt.failure_category
        else ""
    )
    provider_status = (
        "n/a" if attempt.provider_status is None else str(attempt.provider_status)
    )
    latency_ms = "n/a" if attempt.latency_ms is None else str(attempt.latency_ms)
    response_bytes = (
        "n/a" if attempt.response_bytes is None else str(attempt.response_bytes)
    )
    destination_host = attempt.destination_host or "n/a"
    lines.append(
        f"{prefix}#{attempt.id} work_item={attempt.work_item_id} "
        f"ordinal={attempt.ordinal} "
        f"{attempt.provider}/{attempt.operation}: {outcome}{failure}"
    )
    lines.append(
        f"{prefix}  provider_status={provider_status} latency_ms={latency_ms} "
        f"response_bytes={response_bytes} destination_host={destination_host}"
    )
    lines.append(
        f"{prefix}  reserved={_format_nano_usd(attempt.reserved_nano_usd)} "
        f"actual={_format_nano_usd(attempt.actual_nano_usd)}"
    )
    return lines


def render_attempt_audit(audit: AttemptAudit) -> str:
    lines: list[str] = []

    lines.append("Attempt")
    lines.extend(_format_attempt_line("  ", audit.attempt))
    lines.append("")

    lines.append("Work item")
    work_item = audit.work_item
    reason = work_item.reason or "no reason recorded"
    lines.append(f"  id: {work_item.id}")
    lines.append(f"  task_type: {work_item.task_type}")
    lines.append(f"  subject: {work_item.subject_kind}:{work_item.subject_id}")
    lines.append(f"  fingerprint: {work_item.fingerprint}")
    lines.append(f"  state: {work_item.state}")
    lines.append(f"  reason: {reason}")
    lines.append("")

    lines.append("Retry history")
    if not audit.retry_history:
        lines.append("  none recorded")
    else:
        for retry in audit.retry_history:
            lines.extend(_format_attempt_line("  ", retry))
    lines.append("")

    lines.append("Result")
    if audit.binding is not None and not audit.binding.external:
        # K10: a task type registered as making no external call has an
        # attempt anyway -- a data inconsistency, not a missing result.
        lines.append(
            f"  data inconsistency: task_type {work_item.task_type!r} is "
            "registered as making no external call (external=False), but "
            "an attempt exists for it; no result is rendered"
        )
    else:
        if audit.caveat is not None:
            lines.append(f"  caveat: {audit.caveat}")
        if audit.no_result_row:
            lines.append(f"  {_NO_RESULT_ROW_TEXT}")
        else:
            for row in audit.result_rows:
                for key in sorted(row.keys()):
                    lines.append(f"    {key}: {row[key]}")
                lines.append("  ---")

    return "\n".join(lines) + "\n"


# --- notable audit person -------------------------------------------------

_PERSON_MIGRATION_BY_SECTION = {
    "sourced names": "0005",
    "relations": "0005",
    "mentions": "0005",
    "entity resolution": "0005",
    "wikipedia": "0006",
    "coverage": "0007",
    "assessments": "0007",
    "lead history": "0008",
    "queue history": "0008",
    "digest history": "0008",
}


def _person_unavailable_line(section: str) -> str:
    migration = _PERSON_MIGRATION_BY_SECTION.get(section, "unknown")
    return f"  section unavailable: schema predates migration {migration}"


def _format_identity(identity: PersonIdentity) -> list[str]:
    lines: list[str] = []
    lines.append(f"  id: {identity.id}")
    lines.append(f"  display_name: {identity.display_name}")
    lines.append(f"  identity_fingerprint: {identity.identity_fingerprint}")
    lines.append(f"  created_at: {identity.created_at}")
    lines.append(f"  created_by_run_id: {identity.created_by_run_id}")
    status = (
        f"merged into person {identity.merged_into_person_id}"
        if identity.merged_into_person_id is not None
        else "canonical"
    )
    lines.append(f"  status: {status}")
    return lines


def render_person_audit(audit: PersonAudit) -> str:
    lines: list[str] = []
    unavailable = set(audit.unavailable_sections)

    if audit.merged_into is not None:
        lines.append(
            "*** MERGED AWAY: this person's identity was merged into "
            f"person {audit.merged_into.id} "
            f"({audit.merged_into.display_name}) "
            f"by run {audit.merged_by_run_id or 'unknown'}. "
            f"The survivor's own history is at "
            f"notable audit person {audit.merged_into.id}. "
            "This is the merged person's own recorded history. ***"
        )
        lines.append("")

    lines.append("Identity")
    lines.extend(_format_identity(audit.identity))
    lines.append("")

    lines.append("Sourced names")
    if "sourced names" in unavailable:
        lines.append(_person_unavailable_line("sourced names"))
    elif not audit.sourced_names:
        lines.append("  none")
    else:
        for name in audit.sourced_names:
            lines.append(
                f"  #{name.id} {name.exact_name!r} ({name.kind}, "
                f"origin={name.origin_kind}) "
                f"first={name.first_observed_at} last={name.last_observed_at}"
            )
    lines.append("")

    lines.append("Relations")
    if "relations" in unavailable:
        lines.append(_person_unavailable_line("relations"))
    elif not audit.relations:
        lines.append("  none")
    else:
        for relation in audit.relations:
            closed = (
                f" closed_at={relation.closed_at}"
                if relation.closed_at is not None
                else ""
            )
            lines.append(
                f"  #{relation.id} {relation.kind} with person "
                f"{relation.other_person_id}: {relation.status}"
                f" created_by_run={relation.created_by_run_id}{closed}"
            )
    lines.append("")

    lines.append("Mentions")
    if "mentions" in unavailable:
        lines.append(_person_unavailable_line("mentions"))
    elif not audit.mentions:
        lines.append("  none")
    else:
        for mention in audit.mentions:
            current_outcome = mention.current_semantic_outcome or "none recorded"
            lines.append(
                f"  #{mention.id} {mention.exact_name!r} outcome={mention.outcome} "
                f"triage_observation={mention.triage_observation_id} "
                f"source_item={mention.source_item_id}"
            )
            lines.append(f"    current_semantic_outcome: {current_outcome}")
    lines.append("")

    lines.append("Entity resolution")
    if "entity resolution" in unavailable:
        lines.append(_person_unavailable_line("entity resolution"))
    elif not audit.resolutions:
        lines.append("  none")
    else:
        for resolution in audit.resolutions:
            subject = (
                f"mention={resolution.person_mention_id}"
                if resolution.person_mention_id is not None
                else f"relation={resolution.person_relation_id}"
            )
            lines.append(
                f"  #{resolution.id} {subject} disposition={resolution.disposition} "
                f"semantic_outcome={resolution.semantic_outcome or 'n/a'}"
            )
            lines.append(
                f"    selected_person={resolution.selected_person_id} "
                f"created_person={resolution.created_person_id} "
                f"candidates={resolution.candidate_person_ids_json}"
            )
            lines.append(
                f"    prompt_hash={resolution.prompt_hash or 'n/a'} "
                f"schema_version={resolution.schema_version or 'n/a'} "
                f"task_fingerprint={resolution.task_fingerprint}"
            )
            lines.append(f"    rationale: {resolution.rationale}")
    lines.append("")

    lines.append("Wikipedia")
    if "wikipedia" in unavailable:
        lines.append(_person_unavailable_line("wikipedia"))
    elif not (
        audit.wikipedia.plans
        or audit.wikipedia.query_forms
        or audit.wikipedia.search_observations
        or audit.wikipedia.page_facts_batches
        or audit.wikipedia.identity_observations
    ):
        lines.append("  none")
    else:
        for plan in audit.wikipedia.plans:
            lines.append(
                f"  plan #{plan.id} status={plan.status} "
                f"created_at={plan.created_at} completed_at={plan.completed_at}"
            )
        for form in audit.wikipedia.query_forms:
            lines.append(
                f"  query_form #{form.id} plan={form.plan_id} "
                f"ordinal={form.ordinal} variant={form.variant_kind} "
                f"status={form.status} hit_count={form.hit_count}"
            )
        for observation in audit.wikipedia.search_observations:
            lines.append(
                f"  search_observation #{observation.id} "
                f"query_form={observation.query_form_id} "
                f"hit_count={observation.hit_count} "
                f"truncated={observation.truncated}"
            )
        for batch in audit.wikipedia.page_facts_batches:
            lines.append(
                f"  page_facts_batch #{batch.id} plan={batch.plan_id} "
                f"status={batch.status} wave={batch.wave}"
            )
        for obs in audit.wikipedia.identity_observations:
            marker = " [CURRENT]" if obs.is_current else ""
            lines.append(
                f"  identity_observation #{obs.id}{marker} "
                f"disposition={obs.disposition} "
                f"semantic_outcome={obs.semantic_outcome or 'n/a'} "
                f"matched_page={obs.matched_mediawiki_page_id}"
            )
            lines.append(f"    rationale: {obs.rationale}")
    lines.append("")

    lines.append("Coverage")
    if "coverage" in unavailable:
        lines.append(_person_unavailable_line("coverage"))
    elif not (
        audit.coverage.plans
        or audit.coverage.query_forms
        or audit.coverage.brave_observations
        or audit.coverage.screenings
        or audit.coverage.article_targets
    ):
        lines.append("  none")
    else:
        for plan in audit.coverage.plans:
            lines.append(
                f"  plan #{plan.id} status={plan.status} created_at={plan.created_at}"
            )
        for form in audit.coverage.query_forms:
            lines.append(
                f"  query_form #{form.id} plan={form.plan_id} "
                f"stage={form.stage} variant={form.variant_kind} "
                f"status={form.status} result_count={form.result_count}"
            )
        for observation in audit.coverage.brave_observations:
            lines.append(
                f"  brave_observation #{observation.id} "
                f"query_form={observation.query_form_id} "
                f"result_count={observation.result_count} "
                f"truncated={observation.truncated}"
            )
        for screening in audit.coverage.screenings:
            lines.append(
                f"  screening #{screening.id} url={screening.url} "
                f"rule={screening.rule_id} status={screening.rule_status} "
                f"policy_fingerprint={screening.source_policy_fingerprint}"
            )
        for target in audit.coverage.article_targets:
            lines.append(
                f"  article_target #{target.id} "
                f"article={target.canonical_article_id} "
                f"reason={target.selection_reason} status={target.status} "
                f"url={target.request_url}"
            )
    lines.append("")

    lines.append("Assessments")
    if "assessments" in unavailable:
        lines.append(_person_unavailable_line("assessments"))
    elif not audit.assessments:
        lines.append("  none")
    else:
        for assessment in audit.assessments:
            lines.append(
                f"  #{assessment.id} article={assessment.canonical_article_id} "
                f"disposition={assessment.disposition} "
                f"person_relation={assessment.person_relation or 'n/a'} "
                f"coverage_depth={assessment.coverage_depth or 'n/a'} "
                f"subject_relationship={assessment.subject_relationship or 'n/a'}"
            )
            lines.append(f"    rationale: {assessment.rationale}")
            if not assessment.signals:
                lines.append("    signals: none")
            else:
                for signal in assessment.signals:
                    lines.append(
                        f"    signal #{signal.id} {signal.signal_kind}/"
                        f"{signal.category}: {signal.claim} "
                        f"(passages={signal.supporting_passage_ids_json})"
                    )
    lines.append("")

    lines.append("Lead history")
    if "lead history" in unavailable:
        lines.append(_person_unavailable_line("lead history"))
    elif not audit.leads:
        lines.append("  none")
    else:
        for lead in audit.leads:
            marker = " [CURRENT]" if lead.is_current else ""
            lines.append(
                f"  #{lead.id}{marker} outcome={lead.outcome} "
                f"qualifying_domain_count={lead.qualifying_domain_count} "
                f"decided_at={lead.decided_at} "
                f"incompleteness_reason={lead.incompleteness_reason or 'n/a'}"
            )
    lines.append("")

    lines.append("Queue history")
    if "queue history" in unavailable:
        lines.append(_person_unavailable_line("queue history"))
    elif audit.queue.row is None and not audit.queue.transitions:
        lines.append("  none")
    else:
        if audit.queue.row is None:
            lines.append("  current row: none")
        else:
            row = audit.queue.row
            lines.append(
                f"  current row: status={row.status} tier={row.tier} "
                f"eligibility_reason={row.eligibility_reason} "
                f"lead_assessment={row.lead_assessment_id} "
                f"first_pending_at={row.first_pending_at}"
            )
        for transition in audit.queue.transitions:
            lines.append(
                f"  transition #{transition.id} run={transition.run_id} "
                f"{transition.from_status or 'n/a'} -> {transition.to_status} "
                f"tier={transition.tier} reason={transition.reason} "
                f"occurred_at={transition.occurred_at}"
            )
    lines.append("")

    lines.append("Digest history")
    if "digest history" in unavailable:
        lines.append(_person_unavailable_line("digest history"))
    elif not audit.digest_entries:
        lines.append("  none")
    else:
        for entry in audit.digest_entries:
            lines.append(
                f"  digest={entry.digest_id} run={entry.run_id} "
                f"ordinal={entry.ordinal} "
                f"lead_assessment={entry.lead_assessment_id} "
                f"file={entry.file_path}"
            )

    return "\n".join(lines) + "\n"
