"""Transaction-neutral persistence for person-detection and identity observations.

Writers neither begin, commit, nor roll back: they join the caller's open
transaction so a handler's domain rows and the settlement that justifies them
commit or roll back together. Top-level scheduling helpers that own their own
brief ``BEGIN IMMEDIATE`` are called out explicitly.

Person identity writers and ``match_key`` live in ``people.identity`` and are
re-exported here so callers have a single repository import path. Mentions may
carry an optional durable ``person_id`` after entity resolution.

Entity-resolution observation and ``person_relation`` writers live here. They
respect the FK insert protocol (first-pass ER before edges; relation before
reconsider ER) and the dual-branch disposition CHECK (K22).
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from notable_person_finder.people.models import DetectionOutput

if TYPE_CHECKING:
    from notable_person_finder.people.identity import (
        collapse_whitespace as collapse_whitespace,
    )
    from notable_person_finder.people.identity import match_key as match_key

DETECT_PEOPLE_TASK_TYPE = "detect_people"
RESOLVE_PERSON_ENTITY_TASK_TYPE = "resolve_person_entity"
RECONSIDER_PERSON_ENTITY_TASK_TYPE = "reconsider_person_entity"
MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE = "match_wikipedia_identity"
_SUBJECT_KIND_SOURCE_ITEM = "source_item"
_SUBJECT_KIND_PERSON_MENTION = "person_mention"
_SUBJECT_KIND_PERSON_RELATION = "person_relation"
_SUBJECT_KIND_PERSON = "person"
_PERMANENT_PREFLIGHT_FAILURE_CATEGORY = "permanent_preflight"

# Leading honorifics stripped only for the mechanical search form. Exact names
# remain source-written. The list is deliberately small and text-grounded; it
# never expands initials or invents a legal name.
_LEADING_HONORIFICS = frozenset(
    {
        "sir",
        "dame",
        "dr",
        "mr",
        "mrs",
        "ms",
        "miss",
        "rev",
        "prof",
        "lord",
        "lady",
    }
)
_HONORIFIC_PREFIX = re.compile(
    r"^(?P<honorific>"
    + "|".join(sorted(_LEADING_HONORIFICS, key=len, reverse=True))
    + r")\.?\s+",
    flags=re.IGNORECASE,
)


def _last_row_id(cursor: sqlite3.Cursor) -> int:
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT did not produce a row id")
    return row_id


def _require_transaction(connection: sqlite3.Connection, name: str) -> None:
    if not connection.in_transaction:
        raise RuntimeError(f"{name} requires an active transaction")


def _as_bool(value: object) -> bool:
    return bool(value)


def mechanical_search_name(exact_name: str) -> str:
    """Derive a mechanical search form from a source-written exact name.

    Removes at most one leading honorific (with optional trailing period).
    Returns the original text when stripping would leave an empty string.
    """
    stripped = exact_name.strip()
    if not stripped:
        return exact_name
    candidate = _HONORIFIC_PREFIX.sub("", stripped, count=1).strip()
    return candidate or stripped


def collapse_whitespace(value: str) -> str:
    """Re-export: collapse runs of whitespace (owned by ``people.identity``)."""
    from notable_person_finder.people.identity import (
        collapse_whitespace as _collapse_whitespace,
    )

    return _collapse_whitespace(value)


def match_key(value: str) -> str:
    """Re-export: retrieval key (owned by ``people.identity``)."""
    from notable_person_finder.people.identity import match_key as _match_key

    return _match_key(value)


@dataclass(frozen=True, slots=True)
class SourceItemRecord:
    """Bounded source item plus its feed identity for detection prepare."""

    id: int
    feed_identity_id: int
    feed_key: str
    feed_label: str
    title_text: str | None
    summary_text: str | None
    canonical_article_id: int | None
    original_url: str | None
    published_at: str | None
    published_issue: str | None
    url_issue: str | None
    current_triage_observation_id: int | None


@dataclass(frozen=True, slots=True)
class ModelInspectionRecord:
    id: int
    run_id: int
    attempt_id: int
    configured_model_id: str
    resolved_model_id: str
    routing_fingerprint: str
    supported_parameters_json: str
    supports_strict_structured_output: bool
    pricing_usable: bool
    prompt_unit_price_nano_usd: int | None
    completion_unit_price_nano_usd: int | None
    compatibility: str
    inspected_at: str


@dataclass(frozen=True, slots=True)
class MentionIdentityFactRecord:
    id: int
    person_mention_id: int
    local_id: str
    kind: str
    value: str
    supporting_passage_ids_json: str


@dataclass(frozen=True, slots=True)
class MentionSignalRecord:
    id: int
    person_mention_id: int
    ordinal: int
    kind: str
    category: str
    claim: str
    supporting_passage_ids_json: str
    grounding: str


@dataclass(frozen=True, slots=True)
class PersonMentionRecord:
    id: int
    triage_observation_id: int
    ordinal: int
    exact_name: str
    search_name: str
    outcome: str
    supporting_passage_ids_json: str
    rationale: str
    person_id: int | None
    current_entity_resolution_observation_id: int | None
    identity_facts: tuple[MentionIdentityFactRecord, ...]
    signals: tuple[MentionSignalRecord, ...]


@dataclass(frozen=True, slots=True)
class TriageObservationRecord:
    id: int
    source_item_id: int
    run_id: int
    attempt_id: int | None
    model_inspection_id: int | None
    disposition: str
    semantic_outcome: str | None
    canonical_supplied_input_json: str
    validated_output_json: str | None
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    input_truncated: bool
    overflow: bool | None
    rationale: str
    failure_category: str | None
    observed_at: str


@dataclass(frozen=True, slots=True)
class TriageCorpusCounts:
    """Whole-corpus durable triage totals for ``notable status``."""

    source_items_total: int
    untriaged: int
    triaged: int
    completed: int
    research_people: int
    do_not_research: int
    uncertain: int
    insufficient_input: int
    failed: int
    overflow: int
    research_or_uncertain_mentions: int


@dataclass(frozen=True, slots=True)
class TriageRunCounts:
    """This-run observation totals for the daily digest."""

    observations: int
    completed: int
    research_people: int
    do_not_research: int
    uncertain: int
    insufficient_input: int
    failed: int
    overflow: int
    research_or_uncertain_mentions: int
    failed_by_category: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class EntityResolutionObservationRecord:
    """Immutable entity-resolution observation (first-pass or reconsider)."""

    id: int
    person_mention_id: int | None
    person_relation_id: int | None
    run_id: int
    attempt_id: int | None
    model_inspection_id: int | None
    disposition: str
    semantic_outcome: str | None
    selected_person_id: int | None
    created_person_id: int | None
    candidate_person_ids_json: str
    canonical_supplied_input_json: str
    validated_output_json: str | None
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    supporting_fact_ids_json: str | None
    conflicting_fact_ids_json: str | None
    rationale: str
    failure_category: str | None
    observed_at: str


@dataclass(frozen=True, slots=True)
class PersonRelationRecord:
    """Directed-or-ordered edge between two people (possible same / merge)."""

    id: int
    kind: str
    person_id_a: int
    person_id_b: int
    status: str
    created_at: str
    created_by_run_id: int
    created_by_observation_id: int | None
    closed_at: str | None
    closed_by_observation_id: int | None


def list_untriaged_source_item_ids(connection: sqlite3.Connection) -> tuple[int, ...]:
    """Source items with no current triage observation, ordered by id."""
    rows = connection.execute(
        """
        SELECT id FROM source_item
         WHERE current_triage_observation_id IS NULL
         ORDER BY id
        """
    )
    return tuple(int(row["id"]) for row in rows)


def load_source_item_record(
    connection: sqlite3.Connection, *, source_item_id: int
) -> SourceItemRecord | None:
    """Load one source item joined to its feed identity for detection prepare."""
    row = connection.execute(
        """
        SELECT
            s.id,
            s.feed_identity_id,
            f.key AS feed_key,
            f.current_label AS feed_label,
            s.title_text,
            s.summary_text,
            s.canonical_article_id,
            s.original_url,
            s.published_at,
            s.published_issue,
            s.url_issue,
            s.current_triage_observation_id
          FROM source_item AS s
          JOIN feed_identity AS f ON f.id = s.feed_identity_id
         WHERE s.id = ?
        """,
        (source_item_id,),
    ).fetchone()
    if row is None:
        return None
    return SourceItemRecord(
        id=int(row["id"]),
        feed_identity_id=int(row["feed_identity_id"]),
        feed_key=row["feed_key"],
        feed_label=row["feed_label"],
        title_text=row["title_text"],
        summary_text=row["summary_text"],
        canonical_article_id=(
            None
            if row["canonical_article_id"] is None
            else int(row["canonical_article_id"])
        ),
        original_url=row["original_url"],
        published_at=row["published_at"],
        published_issue=row["published_issue"],
        url_issue=row["url_issue"],
        current_triage_observation_id=(
            None
            if row["current_triage_observation_id"] is None
            else int(row["current_triage_observation_id"])
        ),
    )


def insert_model_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    configured_model_id: str,
    resolved_model_id: str,
    routing_fingerprint: str,
    supported_parameters_json: str,
    supports_strict_structured_output: bool,
    pricing_usable: bool,
    prompt_unit_price_nano_usd: int | None,
    completion_unit_price_nano_usd: int | None,
    compatibility: str,
    inspected_at: str,
) -> int:
    """Insert one run-scoped model inspection observation.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_model_inspection")
    cursor = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            attempt_id,
            configured_model_id,
            resolved_model_id,
            routing_fingerprint,
            supported_parameters_json,
            1 if supports_strict_structured_output else 0,
            1 if pricing_usable else 0,
            prompt_unit_price_nano_usd,
            completion_unit_price_nano_usd,
            compatibility,
            inspected_at,
        ),
    )
    return _last_row_id(cursor)


def load_model_inspection(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    configured_model_id: str,
    routing_fingerprint: str,
) -> ModelInspectionRecord | None:
    """Load the current-run inspection for an exact model and routing policy."""
    row = connection.execute(
        """
        SELECT
            id, run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
          FROM model_inspection
         WHERE run_id = ?
           AND configured_model_id = ?
           AND routing_fingerprint = ?
        """,
        (run_id, configured_model_id, routing_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _model_inspection(row)


def load_triage_observation_by_fingerprint(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    task_fingerprint: str,
) -> TriageObservationRecord | None:
    """Reusable material observation for this source item and fingerprint."""
    row = connection.execute(
        """
        SELECT *
          FROM triage_observation
         WHERE source_item_id = ? AND task_fingerprint = ?
        """,
        (source_item_id, task_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _triage_observation(row)


def load_current_triage_observation(
    connection: sqlite3.Connection, *, source_item_id: int
) -> TriageObservationRecord | None:
    """The observation currently selected for a source item, if any."""
    row = connection.execute(
        """
        SELECT t.*
          FROM source_item AS s
          JOIN triage_observation AS t
            ON t.id = s.current_triage_observation_id
         WHERE s.id = ?
        """,
        (source_item_id,),
    ).fetchone()
    if row is None:
        return None
    return _triage_observation(row)


def insert_insufficient_input_observation(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    run_id: int,
    canonical_supplied_input_json: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    task_fingerprint: str,
    observed_at: str,
    rationale: str,
) -> int:
    """Insert a deterministic no-attempt terminal and point the source item at it.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_insufficient_input_observation")
    cursor = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale,
            failure_category, observed_at
        ) VALUES (?, ?, NULL, NULL, 'insufficient_input', NULL, ?, NULL, ?, ?, ?,
                  ?, 0, NULL, ?, NULL, ?)
        """,
        (
            source_item_id,
            run_id,
            canonical_supplied_input_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            rationale,
            observed_at,
        ),
    )
    observation_id = _last_row_id(cursor)
    _set_current_triage_observation(
        connection, source_item_id=source_item_id, observation_id=observation_id
    )
    return observation_id


def insert_completed_observation(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int,
    output: DetectionOutput,
    canonical_supplied_input_json: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    task_fingerprint: str,
    input_truncated: bool,
    observed_at: str,
) -> int:
    """Insert a completed observation, its mentions, and update the pointer.

    Mentions, identity facts, and signals are written in validated order
    (mention ordinal, then facts, then signals). There is no durable
    ``person_id``; namesakes may share an exact name under distinct ordinals.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_completed_observation")
    validated_output_json = output.model_dump_json()
    cursor = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale,
            failure_category, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            model_inspection_id,
            str(output.item_outcome),
            canonical_supplied_input_json,
            validated_output_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            1 if input_truncated else 0,
            1 if output.overflow else 0,
            output.rationale,
            observed_at,
        ),
    )
    observation_id = _last_row_id(cursor)
    _insert_mentions(connection, triage_observation_id=observation_id, output=output)
    _set_current_triage_observation(
        connection, source_item_id=source_item_id, observation_id=observation_id
    )
    return observation_id


def insert_failed_observation(
    connection: sqlite3.Connection,
    *,
    source_item_id: int,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    failure_category: str,
    canonical_supplied_input_json: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    task_fingerprint: str,
    input_truncated: bool,
    observed_at: str,
    rationale: str,
) -> int:
    """Insert a typed permanent-failure observation and update the pointer.

    ``model_inspection_id`` may be null when preflight never produced a usable
    inspection. Mentions are never written for failed rows.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_failed_observation")
    cursor = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale,
            failure_category, observed_at
        ) VALUES (?, ?, ?, ?, 'failed', NULL, ?, NULL, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            model_inspection_id,
            canonical_supplied_input_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            1 if input_truncated else 0,
            rationale,
            failure_category,
            observed_at,
        ),
    )
    observation_id = _last_row_id(cursor)
    _set_current_triage_observation(
        connection, source_item_id=source_item_id, observation_id=observation_id
    )
    return observation_id


def load_person_mentions(
    connection: sqlite3.Connection, *, triage_observation_id: int
) -> tuple[PersonMentionRecord, ...]:
    """Load unresolved mentions with nested facts and signals, ordinal order."""
    mention_rows = connection.execute(
        """
        SELECT *
          FROM person_mention
         WHERE triage_observation_id = ?
         ORDER BY ordinal
        """,
        (triage_observation_id,),
    ).fetchall()
    if not mention_rows:
        return ()

    mention_ids = [int(row["id"]) for row in mention_rows]
    placeholders = ",".join("?" for _ in mention_ids)

    facts_by_mention: dict[int, list[MentionIdentityFactRecord]] = {
        mention_id: [] for mention_id in mention_ids
    }
    for row in connection.execute(
        f"""
        SELECT *
          FROM mention_identity_fact
         WHERE person_mention_id IN ({placeholders})
         ORDER BY person_mention_id, local_id
        """,
        mention_ids,
    ):
        fact = MentionIdentityFactRecord(
            id=int(row["id"]),
            person_mention_id=int(row["person_mention_id"]),
            local_id=row["local_id"],
            kind=row["kind"],
            value=row["value"],
            supporting_passage_ids_json=row["supporting_passage_ids_json"],
        )
        facts_by_mention[fact.person_mention_id].append(fact)

    signals_by_mention: dict[int, list[MentionSignalRecord]] = {
        mention_id: [] for mention_id in mention_ids
    }
    for row in connection.execute(
        f"""
        SELECT *
          FROM mention_signal
         WHERE person_mention_id IN ({placeholders})
         ORDER BY person_mention_id, ordinal
        """,
        mention_ids,
    ):
        signal = MentionSignalRecord(
            id=int(row["id"]),
            person_mention_id=int(row["person_mention_id"]),
            ordinal=int(row["ordinal"]),
            kind=row["kind"],
            category=row["category"],
            claim=row["claim"],
            supporting_passage_ids_json=row["supporting_passage_ids_json"],
            grounding=row["grounding"],
        )
        signals_by_mention[signal.person_mention_id].append(signal)

    return tuple(
        PersonMentionRecord(
            id=int(row["id"]),
            triage_observation_id=int(row["triage_observation_id"]),
            ordinal=int(row["ordinal"]),
            exact_name=row["exact_name"],
            search_name=row["search_name"],
            outcome=row["outcome"],
            supporting_passage_ids_json=row["supporting_passage_ids_json"],
            rationale=row["rationale"],
            person_id=(None if row["person_id"] is None else int(row["person_id"])),
            current_entity_resolution_observation_id=(
                None
                if row["current_entity_resolution_observation_id"] is None
                else int(row["current_entity_resolution_observation_id"])
            ),
            identity_facts=tuple(facts_by_mention[int(row["id"])]),
            signals=tuple(signals_by_mention[int(row["id"])]),
        )
        for row in mention_rows
    )


def triage_corpus_counts(connection: sqlite3.Connection) -> TriageCorpusCounts:
    """Durable corpus triage totals based on each source item's current pointer."""
    source_items_total = int(
        connection.execute("SELECT COUNT(*) AS n FROM source_item").fetchone()["n"]
    )
    untriaged = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM source_item
             WHERE current_triage_observation_id IS NULL
            """
        ).fetchone()["n"]
    )
    disposition_rows = connection.execute(
        """
        SELECT t.disposition, t.semantic_outcome, t.overflow, COUNT(*) AS n
          FROM source_item AS s
          JOIN triage_observation AS t
            ON t.id = s.current_triage_observation_id
         GROUP BY t.disposition, t.semantic_outcome, t.overflow
        """
    ).fetchall()

    completed = 0
    research_people = 0
    do_not_research = 0
    uncertain = 0
    insufficient_input = 0
    failed = 0
    overflow = 0
    for row in disposition_rows:
        count = int(row["n"])
        disposition = row["disposition"]
        if disposition == "completed":
            completed += count
            if row["semantic_outcome"] == "research_people":
                research_people += count
            elif row["semantic_outcome"] == "do_not_research":
                do_not_research += count
            elif row["semantic_outcome"] == "uncertain":
                uncertain += count
            if row["overflow"]:
                overflow += count
        elif disposition == "insufficient_input":
            insufficient_input += count
        elif disposition == "failed":
            failed += count

    research_or_uncertain_mentions = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM source_item AS s
              JOIN triage_observation AS t
                ON t.id = s.current_triage_observation_id
              JOIN person_mention AS m
                ON m.triage_observation_id = t.id
             WHERE t.disposition = 'completed'
               AND m.outcome IN ('research', 'uncertain')
            """
        ).fetchone()["n"]
    )

    return TriageCorpusCounts(
        source_items_total=source_items_total,
        untriaged=untriaged,
        triaged=source_items_total - untriaged,
        completed=completed,
        research_people=research_people,
        do_not_research=do_not_research,
        uncertain=uncertain,
        insufficient_input=insufficient_input,
        failed=failed,
        overflow=overflow,
        research_or_uncertain_mentions=research_or_uncertain_mentions,
    )


def triage_run_counts(
    connection: sqlite3.Connection, *, run_id: int
) -> TriageRunCounts:
    """Observation totals written during one run (digest section)."""
    disposition_rows = connection.execute(
        """
        SELECT disposition, semantic_outcome, overflow, COUNT(*) AS n
          FROM triage_observation
         WHERE run_id = ?
         GROUP BY disposition, semantic_outcome, overflow
        """,
        (run_id,),
    ).fetchall()
    observations = 0
    completed = 0
    research_people = 0
    do_not_research = 0
    uncertain = 0
    insufficient_input = 0
    failed = 0
    overflow = 0
    for row in disposition_rows:
        count = int(row["n"])
        observations += count
        disposition = row["disposition"]
        if disposition == "completed":
            completed += count
            if row["semantic_outcome"] == "research_people":
                research_people += count
            elif row["semantic_outcome"] == "do_not_research":
                do_not_research += count
            elif row["semantic_outcome"] == "uncertain":
                uncertain += count
            if row["overflow"]:
                overflow += count
        elif disposition == "insufficient_input":
            insufficient_input += count
        elif disposition == "failed":
            failed += count

    research_or_uncertain_mentions = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM triage_observation AS t
              JOIN person_mention AS m
                ON m.triage_observation_id = t.id
             WHERE t.run_id = ?
               AND t.disposition = 'completed'
               AND m.outcome IN ('research', 'uncertain')
            """,
            (run_id,),
        ).fetchone()["n"]
    )

    failed_by_category = {
        row["failure_category"]: int(row["n"])
        for row in connection.execute(
            """
            SELECT failure_category, COUNT(*) AS n
              FROM triage_observation
             WHERE run_id = ?
               AND disposition = 'failed'
             GROUP BY failure_category
             ORDER BY failure_category
            """,
            (run_id,),
        )
    }
    return TriageRunCounts(
        observations=observations,
        completed=completed,
        research_people=research_people,
        do_not_research=do_not_research,
        uncertain=uncertain,
        insufficient_input=insufficient_input,
        failed=failed,
        overflow=overflow,
        research_or_uncertain_mentions=research_or_uncertain_mentions,
        failed_by_category=failed_by_category,
    )


@dataclass(frozen=True, slots=True)
class IdentityCorpusCounts:
    """Whole-corpus durable person-identity totals for ``notable status``."""

    canonical_people: int
    merged_away_people: int
    active_possible_same_person: int
    mentions_linked_to_people: int


@dataclass(frozen=True, slots=True)
class IdentityRunCounts:
    """This-run identity totals for the daily digest's Person identity section.

    Outcome breakdowns count first-pass ER rows only (``person_mention_id``
    set). Corpus snapshot fields (active edges) are included so the digest can
    render remaining-work figures without a second query. Unresolved eligible
    mention counts are K24 and live in the service layer (fingerprint-aware).
    """

    people_created: int
    mentions_resolved: int
    linked_same_person: int
    created_via_different_people: int
    created_via_created_new: int
    uncertain: int
    active_possible_same_person: int
    confirmed_merges: int
    resolution_model_deferred: int
    resolution_model_failed: int


def identity_corpus_counts(connection: sqlite3.Connection) -> IdentityCorpusCounts:
    """Durable people / relation / link totals based on current rows."""
    canonical_people = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person
             WHERE merged_into_person_id IS NULL
            """
        ).fetchone()["n"]
    )
    merged_away_people = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person
             WHERE merged_into_person_id IS NOT NULL
            """
        ).fetchone()["n"]
    )
    active_possible_same_person = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'possible_same_person'
               AND status = 'active'
            """
        ).fetchone()["n"]
    )
    mentions_linked_to_people = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_mention
             WHERE person_id IS NOT NULL
            """
        ).fetchone()["n"]
    )
    return IdentityCorpusCounts(
        canonical_people=canonical_people,
        merged_away_people=merged_away_people,
        active_possible_same_person=active_possible_same_person,
        mentions_linked_to_people=mentions_linked_to_people,
    )


def identity_run_counts(
    connection: sqlite3.Connection, *, run_id: int
) -> IdentityRunCounts:
    """Identity totals written or settled during one run (digest section)."""
    people_created = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person
             WHERE created_by_run_id = ?
            """,
            (run_id,),
        ).fetchone()["n"]
    )

    outcome_rows = connection.execute(
        """
        SELECT semantic_outcome, COUNT(*) AS n
          FROM entity_resolution_observation
         WHERE run_id = ?
           AND person_mention_id IS NOT NULL
           AND disposition = 'completed'
           AND semantic_outcome IS NOT NULL
         GROUP BY semantic_outcome
        """,
        (run_id,),
    ).fetchall()
    by_outcome = {str(row["semantic_outcome"]): int(row["n"]) for row in outcome_rows}
    linked_same_person = by_outcome.get("same_person", 0)
    created_via_different_people = by_outcome.get("different_people", 0)
    created_via_created_new = by_outcome.get("created_new", 0)
    uncertain = by_outcome.get("uncertain", 0)
    # First-pass completed outcomes that assign a durable person_id.
    mentions_resolved = (
        linked_same_person
        + created_via_different_people
        + created_via_created_new
        + uncertain
    )

    active_possible_same_person = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'possible_same_person'
               AND status = 'active'
            """
        ).fetchone()["n"]
    )
    confirmed_merges = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'merge'
               AND created_by_run_id = ?
            """,
            (run_id,),
        ).fetchone()["n"]
    )

    # ``complete_work`` stamps completed_by_run_id for deferred and permanent
    # failure, matching detection model work attribution.
    resolution_model_deferred = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM work_item
             WHERE task_type IN (?, ?)
               AND state = 'deferred'
               AND completed_by_run_id = ?
            """,
            (
                RESOLVE_PERSON_ENTITY_TASK_TYPE,
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                run_id,
            ),
        ).fetchone()["n"]
    )
    resolution_model_failed = int(
        connection.execute(
            """
            SELECT COUNT(*) AS n
              FROM work_item
             WHERE task_type IN (?, ?)
               AND state = 'failed_permanent'
               AND completed_by_run_id = ?
            """,
            (
                RESOLVE_PERSON_ENTITY_TASK_TYPE,
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                run_id,
            ),
        ).fetchone()["n"]
    )

    return IdentityRunCounts(
        people_created=people_created,
        mentions_resolved=mentions_resolved,
        linked_same_person=linked_same_person,
        created_via_different_people=created_via_different_people,
        created_via_created_new=created_via_created_new,
        uncertain=uncertain,
        active_possible_same_person=active_possible_same_person,
        confirmed_merges=confirmed_merges,
        resolution_model_deferred=resolution_model_deferred,
        resolution_model_failed=resolution_model_failed,
    )


def settle_active_tasks_after_permanent_preflight(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    model_id: str,
    failure_category: str,
    rationale: str,
    task_types: tuple[str, ...],
    now: str,
    detect_prompt_hash: str | None = None,
    detect_schema_hash: str | None = None,
    detect_schema_version: int | None = None,
    resolve_prompt_hash: str | None = None,
    resolve_schema_hash: str | None = None,
    resolve_schema_version: int | None = None,
) -> int:
    """Fail active work for ``task_types`` after permanent preflight loss (K23).

    When the caller already holds a transaction (the inspection handler's
    ``persist`` / ``persist_failure`` path inside ``complete_work``), joins
    that transaction so domain rows and settlement commit together. Otherwise
    owns a brief ``BEGIN IMMEDIATE``.

    For each active work item of a mapped task type:

    * ``detect_people`` (subject source item): ensure a failed triage
      observation for that material fingerprint (reuse on unique conflict).
    * ``resolve_person_entity`` (subject person mention): ensure a failed ER
      with the **inspection** ``attempt_id`` (reuse on unique conflict); point
      the mention's current ER when needed; leave ``person_id`` NULL.
    * ``reconsider_person_entity`` (subject person relation): ensure a failed
      relation-scoped ER (reuse on conflict); do not change edge status.
    * ``match_wikipedia_identity`` (subject person): ensure a failed Wikipedia
      observation with the **inspection** ``attempt_id`` and domain
      ``failure_category='permanent_preflight'`` (reuse on fingerprint
      conflict); mark the active plan ``failed``; **do not** move the current
      Wikipedia pointer (K25 / K18).

    Does not insert ``generate_structured`` attempts. Dependent handlers'
    ``persist_failure`` never runs on this path.
    """
    del model_id  # reserved for diagnostics / future task→model filtering
    owns_transaction = not connection.in_transaction
    if owns_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        settled = 0
        for task_type in task_types:
            if task_type == DETECT_PEOPLE_TASK_TYPE:
                if (
                    detect_prompt_hash is None
                    or detect_schema_hash is None
                    or detect_schema_version is None
                ):
                    raise ValueError(
                        "detect prompt/schema hashes are required to settle "
                        "detect_people after permanent preflight"
                    )
                settled += _settle_active_detect_people(
                    connection,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    model_inspection_id=model_inspection_id,
                    failure_category=failure_category,
                    rationale=rationale,
                    prompt_hash=detect_prompt_hash,
                    schema_hash=detect_schema_hash,
                    schema_version=detect_schema_version,
                    now=now,
                )
            elif task_type == RESOLVE_PERSON_ENTITY_TASK_TYPE:
                if (
                    resolve_prompt_hash is None
                    or resolve_schema_hash is None
                    or resolve_schema_version is None
                ):
                    raise ValueError(
                        "resolve prompt/schema hashes are required to settle "
                        "resolve_person_entity after permanent preflight"
                    )
                settled += _settle_active_resolve_person_entity(
                    connection,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    model_inspection_id=model_inspection_id,
                    failure_category=failure_category,
                    rationale=rationale,
                    prompt_hash=resolve_prompt_hash,
                    schema_hash=resolve_schema_hash,
                    schema_version=resolve_schema_version,
                    now=now,
                )
            elif task_type == RECONSIDER_PERSON_ENTITY_TASK_TYPE:
                if (
                    resolve_prompt_hash is None
                    or resolve_schema_hash is None
                    or resolve_schema_version is None
                ):
                    raise ValueError(
                        "resolve prompt/schema hashes are required to settle "
                        "reconsider_person_entity after permanent preflight"
                    )
                settled += _settle_active_reconsider_person_entity(
                    connection,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    model_inspection_id=model_inspection_id,
                    failure_category=failure_category,
                    rationale=rationale,
                    prompt_hash=resolve_prompt_hash,
                    schema_hash=resolve_schema_hash,
                    schema_version=resolve_schema_version,
                    now=now,
                )
            elif task_type == MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE:
                # Match hashes are optional: preflight failure rows store null
                # prompt/schema when the match path never rendered a request.
                settled += _settle_active_match_wikipedia_identity(
                    connection,
                    run_id=run_id,
                    attempt_id=attempt_id,
                    model_inspection_id=model_inspection_id,
                    rationale=rationale,
                    prompt_hash=None,
                    schema_hash=None,
                    schema_version=None,
                    now=now,
                )
            else:
                raise ValueError(
                    f"unsupported task_type for permanent-preflight settlement: "
                    f"{task_type}"
                )
    except BaseException:
        if owns_transaction:
            connection.rollback()
        raise
    else:
        if owns_transaction:
            connection.commit()
    return settled


def settle_active_detect_people_after_permanent_preflight(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    failure_category: str,
    rationale: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    now: str,
) -> int:
    """Fail pending/deferred ``detect_people`` work after permanent preflight loss.

    Backward-compatible wrapper around
    :func:`settle_active_tasks_after_permanent_preflight`.
    """
    return settle_active_tasks_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        model_inspection_id=None,
        model_id="",
        failure_category=failure_category,
        rationale=rationale,
        task_types=(DETECT_PEOPLE_TASK_TYPE,),
        now=now,
        detect_prompt_hash=prompt_hash,
        detect_schema_hash=schema_hash,
        detect_schema_version=schema_version,
    )


def _settle_work_item_failed_permanent(
    connection: sqlite3.Connection,
    *,
    work_item_id: int,
    run_id: int,
    rationale: str,
    now: str,
    task_label: str,
) -> None:
    changed = connection.execute(
        """
        UPDATE work_item
           SET state = 'failed_permanent',
               reason = ?,
               completed_by_run_id = ?,
               claimed_by_run_id = NULL,
               updated_at = ?
         WHERE id = ?
           AND state IN ('pending', 'deferred')
        """,
        (rationale, run_id, now, work_item_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"work item {work_item_id} is not active {task_label} work "
            f"that run-{run_id} may settle"
        )


def _settle_active_detect_people(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    failure_category: str,
    rationale: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    now: str,
) -> int:
    rows = connection.execute(
        """
        SELECT id, subject_id, fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id IS NOT NULL
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (DETECT_PEOPLE_TASK_TYPE, _SUBJECT_KIND_SOURCE_ITEM),
    ).fetchall()
    settled = 0
    for row in rows:
        work_item_id = int(row["id"])
        source_item_id = int(row["subject_id"])
        task_fingerprint = row["fingerprint"]
        existing = load_triage_observation_by_fingerprint(
            connection,
            source_item_id=source_item_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is None:
            insert_failed_observation(
                connection,
                source_item_id=source_item_id,
                run_id=run_id,
                attempt_id=attempt_id,
                model_inspection_id=model_inspection_id,
                failure_category=failure_category,
                canonical_supplied_input_json="{}",
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
                task_fingerprint=task_fingerprint,
                input_truncated=False,
                observed_at=now,
                rationale=rationale,
            )
        else:
            current = connection.execute(
                """
                SELECT current_triage_observation_id
                  FROM source_item
                 WHERE id = ?
                """,
                (source_item_id,),
            ).fetchone()
            if (
                current is None
                or current["current_triage_observation_id"] != existing.id
            ):
                _set_current_triage_observation(
                    connection,
                    source_item_id=source_item_id,
                    observation_id=existing.id,
                )
        _settle_work_item_failed_permanent(
            connection,
            work_item_id=work_item_id,
            run_id=run_id,
            rationale=rationale,
            now=now,
            task_label=DETECT_PEOPLE_TASK_TYPE,
        )
        settled += 1
    return settled


def _settle_active_resolve_person_entity(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    failure_category: str,
    rationale: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    now: str,
) -> int:
    rows = connection.execute(
        """
        SELECT id, subject_id, fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id IS NOT NULL
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (RESOLVE_PERSON_ENTITY_TASK_TYPE, _SUBJECT_KIND_PERSON_MENTION),
    ).fetchall()
    settled = 0
    for row in rows:
        work_item_id = int(row["id"])
        person_mention_id = int(row["subject_id"])
        task_fingerprint = row["fingerprint"]
        existing = load_er_by_mention_fingerprint(
            connection,
            person_mention_id=person_mention_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is None:
            observation_id = insert_entity_resolution_observation(
                connection,
                person_mention_id=person_mention_id,
                person_relation_id=None,
                run_id=run_id,
                attempt_id=attempt_id,
                model_inspection_id=model_inspection_id,
                disposition="failed",
                semantic_outcome=None,
                selected_person_id=None,
                created_person_id=None,
                candidate_person_ids_json="[]",
                canonical_supplied_input_json="{}",
                validated_output_json=None,
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
                task_fingerprint=task_fingerprint,
                rationale=rationale,
                failure_category=failure_category,
                observed_at=now,
            )
        else:
            observation_id = existing.id
        current = connection.execute(
            """
            SELECT current_entity_resolution_observation_id, person_id
              FROM person_mention
             WHERE id = ?
            """,
            (person_mention_id,),
        ).fetchone()
        if current is None:
            raise RuntimeError(
                f"person_mention {person_mention_id} is missing during "
                "permanent-preflight settle"
            )
        if current["current_entity_resolution_observation_id"] != observation_id:
            # Keep person_id as-is (NULL for unresolved preflight failures).
            point_mention_current_er(
                connection,
                person_mention_id=person_mention_id,
                observation_id=observation_id,
                person_id=(
                    None if current["person_id"] is None else int(current["person_id"])
                ),
            )
        _settle_work_item_failed_permanent(
            connection,
            work_item_id=work_item_id,
            run_id=run_id,
            rationale=rationale,
            now=now,
            task_label=RESOLVE_PERSON_ENTITY_TASK_TYPE,
        )
        settled += 1
    return settled


def _settle_active_reconsider_person_entity(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    failure_category: str,
    rationale: str,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    now: str,
) -> int:
    rows = connection.execute(
        """
        SELECT id, subject_id, fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id IS NOT NULL
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (RECONSIDER_PERSON_ENTITY_TASK_TYPE, _SUBJECT_KIND_PERSON_RELATION),
    ).fetchall()
    settled = 0
    for row in rows:
        work_item_id = int(row["id"])
        person_relation_id = int(row["subject_id"])
        task_fingerprint = row["fingerprint"]
        existing = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=person_relation_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is None:
            insert_entity_resolution_observation(
                connection,
                person_mention_id=None,
                person_relation_id=person_relation_id,
                run_id=run_id,
                attempt_id=attempt_id,
                model_inspection_id=model_inspection_id,
                disposition="failed",
                semantic_outcome=None,
                selected_person_id=None,
                created_person_id=None,
                candidate_person_ids_json="[]",
                canonical_supplied_input_json="{}",
                validated_output_json=None,
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
                task_fingerprint=task_fingerprint,
                rationale=rationale,
                failure_category=failure_category,
                observed_at=now,
            )
        # Do not change person_relation status (design: leave edge active).
        _settle_work_item_failed_permanent(
            connection,
            work_item_id=work_item_id,
            run_id=run_id,
            rationale=rationale,
            now=now,
            task_label=RECONSIDER_PERSON_ENTITY_TASK_TYPE,
        )
        settled += 1
    return settled


def _settle_active_match_wikipedia_identity(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    model_inspection_id: int | None,
    rationale: str,
    prompt_hash: str | None,
    schema_hash: str | None,
    schema_version: int | None,
    now: str,
) -> int:
    """Fail active match work after permanent match-model preflight (K18).

    Writes failed Wikipedia observations with the inspection attempt id and
    domain ``permanent_preflight``. Does **not** move
    ``person.current_wikipedia_identity_observation_id`` (K25).
    """
    # Local imports avoid a people→wikipedia package cycle at module load.
    from notable_person_finder.wikipedia.repository import (
        insert_wikipedia_identity_observation,
        load_active_plan_for_fingerprint,
        load_wikipedia_identity_observation_by_fingerprint,
        mark_plan_status,
    )

    rows = connection.execute(
        """
        SELECT id, subject_id, fingerprint
          FROM work_item
         WHERE task_type = ?
           AND subject_kind = ?
           AND subject_id IS NOT NULL
           AND state IN ('pending', 'deferred')
         ORDER BY id
        """,
        (MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE, _SUBJECT_KIND_PERSON),
    ).fetchall()
    settled = 0
    for row in rows:
        work_item_id = int(row["id"])
        person_id = int(row["subject_id"])
        task_fingerprint = str(row["fingerprint"])
        plan = load_active_plan_for_fingerprint(
            connection,
            person_id=person_id,
            material_fingerprint=task_fingerprint,
        )
        plan_id = None if plan is None else plan.id
        existing = load_wikipedia_identity_observation_by_fingerprint(
            connection,
            person_id=person_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is None:
            insert_wikipedia_identity_observation(
                connection,
                person_id=person_id,
                plan_id=plan_id,
                run_id=run_id,
                attempt_id=attempt_id,
                model_inspection_id=model_inspection_id,
                disposition="failed",
                semantic_outcome=None,
                matched_mediawiki_page_id=None,
                candidate_page_ids_json="[]",
                canonical_supplied_input_json="{}",
                validated_output_json=None,
                prompt_hash=prompt_hash,
                schema_hash=schema_hash,
                schema_version=schema_version,
                task_fingerprint=task_fingerprint,
                rationale=rationale,
                failure_category=_PERMANENT_PREFLIGHT_FAILURE_CATEGORY,
                observed_at=now,
            )
        # K25: never point current at a failed observation.
        if plan is not None and plan.status not in {
            "superseded",
            "completed",
            "failed",
        }:
            mark_plan_status(
                connection,
                plan_id=plan.id,
                status="failed",
                completed_at=now,
                failure_category=_PERMANENT_PREFLIGHT_FAILURE_CATEGORY,
            )
        _settle_work_item_failed_permanent(
            connection,
            work_item_id=work_item_id,
            run_id=run_id,
            rationale=rationale,
            now=now,
            task_label=MATCH_WIKIPEDIA_IDENTITY_TASK_TYPE,
        )
        settled += 1
    return settled


def insert_entity_resolution_observation(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int | None,
    person_relation_id: int | None,
    run_id: int,
    attempt_id: int | None,
    model_inspection_id: int | None,
    disposition: str,
    semantic_outcome: str | None,
    selected_person_id: int | None,
    created_person_id: int | None,
    candidate_person_ids_json: str,
    canonical_supplied_input_json: str,
    validated_output_json: str | None,
    prompt_hash: str,
    schema_hash: str,
    schema_version: int,
    task_fingerprint: str,
    supporting_fact_ids_json: str | None = None,
    conflicting_fact_ids_json: str | None = None,
    rationale: str,
    failure_category: str | None = None,
    observed_at: str,
) -> int:
    """Insert one entity-resolution observation, or reuse the material row.

    Reuse is keyed by the partial unique indexes on
    ``(person_mention_id, task_fingerprint)`` or
    ``(person_relation_id, task_fingerprint)``. On conflict the existing row id
    is returned and no second observation is written. Callers that create a
    person must load first so a reused observation does not orphan a new person.

    Failed rows require a non-null ``attempt_id`` (schema CHECK). Disposition
    combinations must satisfy the dual-branch K22 CHECK.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_entity_resolution_observation")
    if person_mention_id is not None:
        existing = load_er_by_mention_fingerprint(
            connection,
            person_mention_id=person_mention_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            return existing.id
    if person_relation_id is not None:
        existing = load_er_by_relation_fingerprint(
            connection,
            person_relation_id=person_relation_id,
            task_fingerprint=task_fingerprint,
        )
        if existing is not None:
            return existing.id

    cursor = connection.execute(
        """
        INSERT INTO entity_resolution_observation (
            person_mention_id, person_relation_id, run_id, attempt_id,
            model_inspection_id, disposition, semantic_outcome,
            selected_person_id, created_person_id, candidate_person_ids_json,
            canonical_supplied_input_json, validated_output_json,
            prompt_hash, schema_hash, schema_version, task_fingerprint,
            supporting_fact_ids_json, conflicting_fact_ids_json,
            rationale, failure_category, observed_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            person_mention_id,
            person_relation_id,
            run_id,
            attempt_id,
            model_inspection_id,
            disposition,
            semantic_outcome,
            selected_person_id,
            created_person_id,
            candidate_person_ids_json,
            canonical_supplied_input_json,
            validated_output_json,
            prompt_hash,
            schema_hash,
            schema_version,
            task_fingerprint,
            supporting_fact_ids_json,
            conflicting_fact_ids_json,
            rationale,
            failure_category,
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def load_er_by_mention_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    task_fingerprint: str,
) -> EntityResolutionObservationRecord | None:
    """Load the ER observation for a mention and material fingerprint, if any."""
    row = connection.execute(
        """
        SELECT *
          FROM entity_resolution_observation
         WHERE person_mention_id = ? AND task_fingerprint = ?
        """,
        (person_mention_id, task_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _entity_resolution_observation(row)


def load_er_by_relation_fingerprint(
    connection: sqlite3.Connection,
    *,
    person_relation_id: int,
    task_fingerprint: str,
) -> EntityResolutionObservationRecord | None:
    """Load the ER observation for a relation and material fingerprint, if any."""
    row = connection.execute(
        """
        SELECT *
          FROM entity_resolution_observation
         WHERE person_relation_id = ? AND task_fingerprint = ?
        """,
        (person_relation_id, task_fingerprint),
    ).fetchone()
    if row is None:
        return None
    return _entity_resolution_observation(row)


def point_mention_current_er(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    observation_id: int,
    person_id: int | None,
) -> None:
    """Point a mention at its current ER observation and optional person link.

    Ownership triggers require the observation to be mention-scoped for this
    mention (``person_mention_id = mention.id``). Relation-scoped rows cannot
    become the current pointer.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "point_mention_current_er")
    changed = connection.execute(
        """
        UPDATE person_mention
           SET current_entity_resolution_observation_id = ?,
               person_id = ?
         WHERE id = ?
        """,
        (observation_id, person_id, person_mention_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person_mention {person_mention_id} is missing; "
            "cannot set current entity resolution"
        )


def upsert_active_possible_same_person(
    connection: sqlite3.Connection,
    *,
    person_id_a: int,
    person_id_b: int,
    run_id: int,
    created_by_observation_id: int,
    now: str,
) -> int:
    """Insert or reuse an active ``possible_same_person`` edge.

    Endpoints are stored with ``person_id_a < person_id_b``. When an active edge
    already exists for the unordered pair, its id is returned without inserting.

    **The caller must already hold an open transaction.** First-pass ER that
    created the edge must already exist so ``created_by_observation_id`` FK
    checks pass.
    """
    _require_transaction(connection, "upsert_active_possible_same_person")
    if person_id_a == person_id_b:
        raise ValueError("possible_same_person endpoints must differ")
    low, high = sorted((person_id_a, person_id_b))
    existing = connection.execute(
        """
        SELECT id
          FROM person_relation
         WHERE kind = 'possible_same_person'
           AND status = 'active'
           AND person_id_a = ?
           AND person_id_b = ?
         ORDER BY id
         LIMIT 1
        """,
        (low, high),
    ).fetchone()
    if existing is not None:
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO person_relation (
            kind, person_id_a, person_id_b, status, created_at,
            created_by_run_id, created_by_observation_id
        ) VALUES (
            'possible_same_person', ?, ?, 'active', ?, ?, ?
        )
        """,
        (low, high, now, run_id, created_by_observation_id),
    )
    return _last_row_id(cursor)


def dismiss_relation(
    connection: sqlite3.Connection,
    *,
    relation_id: int,
    closed_by_observation_id: int,
    closed_at: str,
) -> None:
    """Mark a ``possible_same_person`` edge dismissed (reconsider different_people).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "dismiss_relation")
    changed = connection.execute(
        """
        UPDATE person_relation
           SET status = 'dismissed',
               closed_at = ?,
               closed_by_observation_id = ?
         WHERE id = ?
        """,
        (closed_at, closed_by_observation_id, relation_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(f"person_relation {relation_id} is missing; cannot dismiss")


def supersede_relation(
    connection: sqlite3.Connection,
    *,
    relation_id: int,
    closed_by_observation_id: int,
    closed_at: str,
) -> None:
    """Mark a ``possible_same_person`` edge superseded by a confirmed merge.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "supersede_relation")
    changed = connection.execute(
        """
        UPDATE person_relation
           SET status = 'superseded_by_merge',
               closed_at = ?,
               closed_by_observation_id = ?
         WHERE id = ?
        """,
        (closed_at, closed_by_observation_id, relation_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person_relation {relation_id} is missing; cannot supersede"
        )


def list_active_possible_same_person_for(
    connection: sqlite3.Connection, person_id: int
) -> tuple[PersonRelationRecord, ...]:
    """Active ``possible_same_person`` edges that include ``person_id``."""
    rows = connection.execute(
        """
        SELECT *
          FROM person_relation
         WHERE kind = 'possible_same_person'
           AND status = 'active'
           AND (person_id_a = ? OR person_id_b = ?)
         ORDER BY id
        """,
        (person_id, person_id),
    ).fetchall()
    return tuple(_person_relation(row) for row in rows)


def _entity_resolution_observation(
    row: sqlite3.Row,
) -> EntityResolutionObservationRecord:
    return EntityResolutionObservationRecord(
        id=int(row["id"]),
        person_mention_id=(
            None if row["person_mention_id"] is None else int(row["person_mention_id"])
        ),
        person_relation_id=(
            None
            if row["person_relation_id"] is None
            else int(row["person_relation_id"])
        ),
        run_id=int(row["run_id"]),
        attempt_id=None if row["attempt_id"] is None else int(row["attempt_id"]),
        model_inspection_id=(
            None
            if row["model_inspection_id"] is None
            else int(row["model_inspection_id"])
        ),
        disposition=row["disposition"],
        semantic_outcome=row["semantic_outcome"],
        selected_person_id=(
            None
            if row["selected_person_id"] is None
            else int(row["selected_person_id"])
        ),
        created_person_id=(
            None if row["created_person_id"] is None else int(row["created_person_id"])
        ),
        candidate_person_ids_json=row["candidate_person_ids_json"],
        canonical_supplied_input_json=row["canonical_supplied_input_json"],
        validated_output_json=row["validated_output_json"],
        prompt_hash=row["prompt_hash"],
        schema_hash=row["schema_hash"],
        schema_version=int(row["schema_version"]),
        task_fingerprint=row["task_fingerprint"],
        supporting_fact_ids_json=row["supporting_fact_ids_json"],
        conflicting_fact_ids_json=row["conflicting_fact_ids_json"],
        rationale=row["rationale"],
        failure_category=row["failure_category"],
        observed_at=row["observed_at"],
    )


def _person_relation(row: sqlite3.Row) -> PersonRelationRecord:
    return PersonRelationRecord(
        id=int(row["id"]),
        kind=row["kind"],
        person_id_a=int(row["person_id_a"]),
        person_id_b=int(row["person_id_b"]),
        status=row["status"],
        created_at=row["created_at"],
        created_by_run_id=int(row["created_by_run_id"]),
        created_by_observation_id=(
            None
            if row["created_by_observation_id"] is None
            else int(row["created_by_observation_id"])
        ),
        closed_at=row["closed_at"],
        closed_by_observation_id=(
            None
            if row["closed_by_observation_id"] is None
            else int(row["closed_by_observation_id"])
        ),
    )


def _set_current_triage_observation(
    connection: sqlite3.Connection, *, source_item_id: int, observation_id: int
) -> None:
    changed = connection.execute(
        """
        UPDATE source_item
           SET current_triage_observation_id = ?
         WHERE id = ?
        """,
        (observation_id, source_item_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"source item {source_item_id} is missing; cannot set current triage"
        )


def _insert_mentions(
    connection: sqlite3.Connection,
    *,
    triage_observation_id: int,
    output: DetectionOutput,
) -> None:
    for ordinal, mention in enumerate(output.mentions, start=1):
        passage_json = json.dumps(list(mention.supporting_passage_ids))
        cursor = connection.execute(
            """
            INSERT INTO person_mention (
                triage_observation_id, ordinal, exact_name, search_name,
                outcome, supporting_passage_ids_json, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                triage_observation_id,
                ordinal,
                mention.exact_name,
                mechanical_search_name(mention.exact_name),
                str(mention.outcome),
                passage_json,
                mention.rationale,
            ),
        )
        mention_id = _last_row_id(cursor)
        for fact in mention.identity_facts:
            connection.execute(
                """
                INSERT INTO mention_identity_fact (
                    person_mention_id, local_id, kind, value,
                    supporting_passage_ids_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    mention_id,
                    fact.local_id,
                    str(fact.kind),
                    fact.value,
                    json.dumps(list(fact.supporting_passage_ids)),
                ),
            )
        for signal_ordinal, signal in enumerate(mention.signals, start=1):
            connection.execute(
                """
                INSERT INTO mention_signal (
                    person_mention_id, ordinal, kind, category, claim,
                    supporting_passage_ids_json, grounding
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mention_id,
                    signal_ordinal,
                    str(signal.kind),
                    str(signal.category),
                    signal.claim,
                    json.dumps(list(signal.supporting_passage_ids)),
                    str(signal.grounding),
                ),
            )


def _model_inspection(row: sqlite3.Row) -> ModelInspectionRecord:
    return ModelInspectionRecord(
        id=int(row["id"]),
        run_id=int(row["run_id"]),
        attempt_id=int(row["attempt_id"]),
        configured_model_id=row["configured_model_id"],
        resolved_model_id=row["resolved_model_id"],
        routing_fingerprint=row["routing_fingerprint"],
        supported_parameters_json=row["supported_parameters_json"],
        supports_strict_structured_output=_as_bool(
            row["supports_strict_structured_output"]
        ),
        pricing_usable=_as_bool(row["pricing_usable"]),
        prompt_unit_price_nano_usd=(
            None
            if row["prompt_unit_price_nano_usd"] is None
            else int(row["prompt_unit_price_nano_usd"])
        ),
        completion_unit_price_nano_usd=(
            None
            if row["completion_unit_price_nano_usd"] is None
            else int(row["completion_unit_price_nano_usd"])
        ),
        compatibility=row["compatibility"],
        inspected_at=row["inspected_at"],
    )


def _triage_observation(row: sqlite3.Row) -> TriageObservationRecord:
    overflow_raw = row["overflow"]
    return TriageObservationRecord(
        id=int(row["id"]),
        source_item_id=int(row["source_item_id"]),
        run_id=int(row["run_id"]),
        attempt_id=None if row["attempt_id"] is None else int(row["attempt_id"]),
        model_inspection_id=(
            None
            if row["model_inspection_id"] is None
            else int(row["model_inspection_id"])
        ),
        disposition=row["disposition"],
        semantic_outcome=row["semantic_outcome"],
        canonical_supplied_input_json=row["canonical_supplied_input_json"],
        validated_output_json=row["validated_output_json"],
        prompt_hash=row["prompt_hash"],
        schema_hash=row["schema_hash"],
        schema_version=int(row["schema_version"]),
        task_fingerprint=row["task_fingerprint"],
        input_truncated=_as_bool(row["input_truncated"]),
        overflow=None if overflow_raw is None else _as_bool(overflow_raw),
        rationale=row["rationale"],
        failure_category=row["failure_category"],
        observed_at=row["observed_at"],
    )
