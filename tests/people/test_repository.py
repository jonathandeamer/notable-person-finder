"""Real-SQLite tests for people detection repository operations."""

from __future__ import annotations

import json
import sqlite3

import pytest

from notable_person_finder.people.identity import insert_person
from notable_person_finder.people.models import (
    AttentionCategory,
    CautionCategory,
    DetectedMention,
    DetectionOutput,
    GroundedSignal,
    IdentityFact,
    IdentityFactKind,
    ItemOutcome,
    MentionOutcome,
    SignalGrounding,
    SignalKind,
)
from notable_person_finder.people.repository import (
    DETECT_PEOPLE_TASK_TYPE,
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    identity_corpus_counts,
    identity_run_counts,
    insert_completed_observation,
    insert_entity_resolution_observation,
    insert_failed_observation,
    insert_insufficient_input_observation,
    insert_model_inspection,
    list_untriaged_source_item_ids,
    load_current_triage_observation,
    load_model_inspection,
    load_person_mentions,
    load_source_item_record,
    load_triage_observation_by_fingerprint,
    mechanical_search_name,
    point_mention_current_er,
    settle_active_detect_people_after_permanent_preflight,
    triage_corpus_counts,
    triage_run_counts,
    upsert_active_possible_same_person,
)
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_THIRD_HASH = "c" * 64
_PROMPT_HASH = "d" * 64
_SCHEMA_HASH = "e" * 64


def _seed_feed_and_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    source_entry_id: str = "entry-a",
    title_text: str | None = "Alex Smith wins award",
    summary_text: str | None = "A prize ceremony.",
    key: str = "feed-a",
) -> tuple[int, int]:
    """Return (feed_identity_id, source_item_id)."""
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (key, moment(), moment()),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, moment()),
    ).lastrowid
    assert fetch is not None
    item = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, original_url,
            published_at, published_issue, url_issue, discovered_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'https://example.com/a', ?, NULL, NULL, ?)
        """,
        (
            feed,
            fetch,
            run_id,
            source_entry_id,
            title_text,
            summary_text,
            moment(),
            moment(),
        ),
    ).lastrowid
    assert item is not None
    connection.commit()
    return feed, item


def _work_item(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    subject_id: int,
    fingerprint: str = _HASH,
    state: str = "pending",
    task_type: str = DETECT_PEOPLE_TASK_TYPE,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES (?, 'source_item', ?, ?, 1, 30, ?, ?, ?, ?, ?)
        """,
        (
            task_type,
            subject_id,
            fingerprint,
            moment(),
            state,
            run_id,
            moment(),
            moment(),
        ),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return cursor.lastrowid


def _attempt(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    work_item_id: int | None = None,
    operation: str = "generate_structured",
) -> int:
    if work_item_id is None:
        work_item_id = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            )
            VALUES ('inspect_model', 'model', NULL, ?, 1, 20, ?, 'running', ?, ?, ?)
            """,
            (_THIRD_HASH, moment(), run_id, moment(), moment()),
        ).lastrowid
        assert work_item_id is not None
    cursor = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', ?, 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work_item_id, operation, moment(), moment(1), _HASH),
    )
    assert cursor.lastrowid is not None
    connection.commit()
    return cursor.lastrowid


def _sample_output(*, names: tuple[str, ...] = ("Alex Smith",)) -> DetectionOutput:
    mentions: list[DetectedMention] = []
    for name in names:
        mentions.append(
            DetectedMention(
                exact_name=name,
                outcome=MentionOutcome.RESEARCH,
                supporting_passage_ids=("p1",),
                identity_facts=(
                    IdentityFact(
                        local_id="fact-name",
                        kind=IdentityFactKind.NAME,
                        value=name,
                        supporting_passage_ids=("p1",),
                    ),
                ),
                signals=(
                    GroundedSignal(
                        kind=SignalKind.ATTENTION,
                        category=AttentionCategory.SIGNIFICANT_RECOGNITION,
                        claim="Prize mentioned in the title.",
                        supporting_passage_ids=("p1",),
                        grounding=SignalGrounding.SOURCE_TEXT,
                    ),
                    GroundedSignal(
                        kind=SignalKind.CAUTION,
                        category=CautionCategory.SINGLE_EVENT_ONLY,
                        claim="Only one ceremony is described.",
                        supporting_passage_ids=("p1",),
                        grounding=SignalGrounding.SOURCE_TEXT,
                    ),
                ),
                rationale=f"{name} is the subject of the item.",
            )
        )
    return DetectionOutput(
        item_outcome=ItemOutcome.RESEARCH_PEOPLE,
        mentions=tuple(mentions),
        overflow=False,
        rationale="Grounded research subjects.",
    )


# ---------------------------------------------------------------------------
# Untriaged listing and bounded source/feed load
# ---------------------------------------------------------------------------


def test_list_untriaged_source_item_ids_excludes_current_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, first = _seed_feed_and_item(connection, run_id=run_id, source_entry_id="e1")
    _, second = _seed_feed_and_item(
        connection, run_id=run_id, source_entry_id="e2", key="feed-b"
    )
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test-2026",
            routing_fingerprint=_HASH,
            supported_parameters_json='["response_format","structured_outputs"]',
            supports_strict_structured_output=True,
            pricing_usable=True,
            prompt_unit_price_nano_usd=100,
            completion_unit_price_nano_usd=200,
            compatibility="compatible",
            inspected_at=moment(),
        )
        insert_completed_observation(
            connection,
            source_item_id=first,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(),
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
        )

    assert list_untriaged_source_item_ids(connection) == (second,)


def test_load_source_item_record_returns_bounded_source_and_feed_fields(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    feed_id, item_id = _seed_feed_and_item(connection, run_id=run_id)
    record = load_source_item_record(connection, source_item_id=item_id)
    assert record is not None
    assert record.id == item_id
    assert record.feed_identity_id == feed_id
    assert record.feed_key == "feed-a"
    assert record.feed_label == "Feed A"
    assert record.title_text == "Alex Smith wins award"
    assert record.summary_text == "A prize ceremony."
    assert record.original_url == "https://example.com/a"
    assert record.canonical_article_id is None
    assert load_source_item_record(connection, source_item_id=999_999) is None


# ---------------------------------------------------------------------------
# Model inspection store/load by exact (run_id, model_id, routing)
# ---------------------------------------------------------------------------


def test_model_inspection_round_trip_by_exact_key(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test-2026",
            routing_fingerprint=_HASH,
            supported_parameters_json='["response_format"]',
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )

    loaded = load_model_inspection(
        connection,
        run_id=run_id,
        configured_model_id="openai/gpt-test",
        routing_fingerprint=_HASH,
    )
    assert loaded is not None
    assert loaded.id == inspection_id
    assert loaded.resolved_model_id == "openai/gpt-test-2026"
    assert loaded.supports_strict_structured_output is True
    assert loaded.pricing_usable is False
    assert loaded.compatibility == "compatible"

    assert (
        load_model_inspection(
            connection,
            run_id=run_id,
            configured_model_id="openai/gpt-test",
            routing_fingerprint=_OTHER_HASH,
        )
        is None
    )


def test_model_inspection_is_not_reused_across_runs(
    connection: sqlite3.Connection,
) -> None:
    first_run = insert_run(connection)
    attempt_id = _attempt(connection, run_id=first_run)
    with immediate(connection):
        insert_model_inspection(
            connection,
            run_id=first_run,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test-2026",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )
    second_run = insert_run(connection)
    assert (
        load_model_inspection(
            connection,
            run_id=second_run,
            configured_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
        )
        is None
    )


def test_writers_require_an_active_transaction(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    attempt_id = _attempt(connection, run_id=run_id)
    with pytest.raises(RuntimeError, match="requires an active transaction"):
        insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )


# ---------------------------------------------------------------------------
# Observations: insufficient, completed, failed + pointer + mentions
# ---------------------------------------------------------------------------


def test_insert_insufficient_input_sets_current_pointer_without_attempt(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(
        connection, run_id=run_id, title_text=None, summary_text=None
    )
    with immediate(connection):
        observation_id = insert_insufficient_input_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            observed_at=moment(),
            rationale="Both title and summary are empty.",
        )

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.id == observation_id
    assert current.disposition == "insufficient_input"
    assert current.attempt_id is None
    assert current.model_inspection_id is None
    assert current.semantic_outcome is None
    assert current.failure_category is None
    assert current.validated_output_json is None
    assert list_untriaged_source_item_ids(connection) == ()


def test_insert_completed_observation_persists_mentions_facts_signals_and_namesakes(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    # Include a leading honorific so the insert path must derive search_name
    # mechanically rather than copying exact_name.
    output = _sample_output(names=("Dr. Jane Doe", "Dr. Jane Doe"))
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json='["response_format","structured_outputs"]',
            supports_strict_structured_output=True,
            pricing_usable=True,
            prompt_unit_price_nano_usd=1,
            completion_unit_price_nano_usd=2,
            compatibility="compatible",
            inspected_at=moment(),
        )
        observation_id = insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=output,
            canonical_supplied_input_json='{"task":"detect_people"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=True,
            observed_at=moment(),
        )

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.id == observation_id
    assert current.disposition == "completed"
    assert current.semantic_outcome == "research_people"
    assert current.input_truncated is True
    assert current.overflow is False
    assert current.validated_output_json is not None
    parsed = json.loads(current.validated_output_json)
    assert parsed["item_outcome"] == "research_people"

    mentions = load_person_mentions(connection, triage_observation_id=observation_id)
    assert len(mentions) == 2
    assert mentions[0].exact_name == "Dr. Jane Doe"
    assert mentions[1].exact_name == "Dr. Jane Doe"
    assert mentions[0].ordinal == 1
    assert mentions[1].ordinal == 2
    assert mentions[0].search_name == "Jane Doe"
    assert mentions[1].search_name == "Jane Doe"
    assert len(mentions[0].identity_facts) == 1
    assert mentions[0].identity_facts[0].kind == "name"
    assert len(mentions[0].signals) == 2
    assert mentions[0].signals[0].kind == "attention"
    assert mentions[0].signals[1].kind == "caution"


def test_child_insert_error_rolls_back_observation_and_pointer(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )

    # Force a child CHECK failure via empty exact_name by patching through a
    # completed insert that violates uniqueness of fact local_id on one mention.
    broken = DetectionOutput(
        item_outcome=ItemOutcome.RESEARCH_PEOPLE,
        mentions=(
            DetectedMention(
                exact_name="Alex Smith",
                outcome=MentionOutcome.RESEARCH,
                supporting_passage_ids=("p1",),
                identity_facts=(
                    IdentityFact(
                        local_id="dup",
                        kind=IdentityFactKind.NAME,
                        value="Alex Smith",
                        supporting_passage_ids=("p1",),
                    ),
                    IdentityFact(
                        local_id="dup",
                        kind=IdentityFactKind.PLACE,
                        value="Paris",
                        supporting_passage_ids=("p1",),
                    ),
                ),
                signals=(),
                rationale="dup facts",
            ),
        ),
        overflow=False,
        rationale="broken graph",
    )
    with pytest.raises(sqlite3.IntegrityError), immediate(connection):
        insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=broken,
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
        )

    assert load_current_triage_observation(connection, source_item_id=item_id) is None
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM triage_observation").fetchone()[
            "n"
        ]
        == 0
    )
    assert (
        connection.execute("SELECT COUNT(*) AS n FROM person_mention").fetchone()["n"]
        == 0
    )
    assert list_untriaged_source_item_ids(connection) == (item_id,)


def test_reusable_observation_lookup_by_material_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )
        observation_id = insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(),
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
        )

    found = load_triage_observation_by_fingerprint(
        connection, source_item_id=item_id, task_fingerprint=_HASH
    )
    assert found is not None
    assert found.id == observation_id
    assert (
        load_triage_observation_by_fingerprint(
            connection, source_item_id=item_id, task_fingerprint=_OTHER_HASH
        )
        is None
    )


def test_prospectively_changed_fingerprint_replaces_current_pointer(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )
        first_id = insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(),
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
        )
        second_id = insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(names=("Jordan Lee",)),
            canonical_supplied_input_json='{"profile":2}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_OTHER_HASH,
            input_truncated=False,
            observed_at=moment(2),
        )

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.id == second_id
    assert current.task_fingerprint == _OTHER_HASH
    history = connection.execute(
        "SELECT id FROM triage_observation WHERE source_item_id = ? ORDER BY id",
        (item_id,),
    ).fetchall()
    assert [row["id"] for row in history] == [first_id, second_id]


def test_failed_observation_coupling_and_optional_inspection(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    attempt_id = _attempt(connection, run_id=run_id)
    with immediate(connection):
        observation_id = insert_failed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=None,
            failure_category="authentication",
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
            rationale="Provider rejected credentials.",
        )

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.id == observation_id
    assert current.disposition == "failed"
    assert current.semantic_outcome is None
    assert current.validated_output_json is None
    assert current.overflow is None
    assert current.failure_category == "authentication"
    assert current.attempt_id == attempt_id
    assert current.model_inspection_id is None
    assert load_person_mentions(connection, triage_observation_id=observation_id) == ()


def test_mechanical_search_name_strips_leading_honorific() -> None:
    assert mechanical_search_name("Dr. Jane Doe") == "Jane Doe"
    assert mechanical_search_name("Sir Alex Smith") == "Alex Smith"
    assert mechanical_search_name("Alex Smith") == "Alex Smith"
    assert mechanical_search_name("Dr.") == "Dr."


# ---------------------------------------------------------------------------
# Aggregates for status and digest
# ---------------------------------------------------------------------------


def test_triage_aggregate_counts(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, usable = _seed_feed_and_item(connection, run_id=run_id, source_entry_id="u1")
    _, empty = _seed_feed_and_item(
        connection,
        run_id=run_id,
        source_entry_id="u2",
        title_text=None,
        summary_text=None,
        key="feed-b",
    )
    _, untriaged = _seed_feed_and_item(
        connection, run_id=run_id, source_entry_id="u3", key="feed-c"
    )
    _, failed_item = _seed_feed_and_item(
        connection, run_id=run_id, source_entry_id="u4", key="feed-d"
    )
    attempt_id = _attempt(connection, run_id=run_id)

    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=_HASH,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )
        insert_completed_observation(
            connection,
            source_item_id=usable,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(names=("Alex Smith", "Jordan Lee")),
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
        )
        insert_insufficient_input_observation(
            connection,
            source_item_id=empty,
            run_id=run_id,
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_OTHER_HASH,
            observed_at=moment(),
            rationale="empty",
        )
        insert_failed_observation(
            connection,
            source_item_id=failed_item,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=None,
            failure_category="unsupported_capability",
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_THIRD_HASH,
            input_truncated=False,
            observed_at=moment(),
            rationale="no strict output",
        )

    corpus = triage_corpus_counts(connection)
    assert corpus.source_items_total == 4
    assert corpus.untriaged == 1
    assert corpus.triaged == 3
    assert corpus.completed == 1
    assert corpus.research_people == 1
    assert corpus.do_not_research == 0
    assert corpus.uncertain == 0
    assert corpus.insufficient_input == 1
    assert corpus.failed == 1
    assert corpus.overflow == 0
    assert corpus.research_or_uncertain_mentions == 2

    run_counts = triage_run_counts(connection, run_id=run_id)
    assert run_counts.observations == 3
    assert run_counts.completed == 1
    assert run_counts.research_people == 1
    assert run_counts.do_not_research == 0
    assert run_counts.uncertain == 0
    assert run_counts.insufficient_input == 1
    assert run_counts.failed == 1
    assert run_counts.failed_by_category == {"unsupported_capability": 1}
    assert run_counts.overflow == 0
    assert run_counts.research_or_uncertain_mentions == 2
    assert untriaged in list_untriaged_source_item_ids(connection)


# ---------------------------------------------------------------------------
# Identity aggregates for status and digest
# ---------------------------------------------------------------------------


def _identity_mention_graph(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    exact_name: str = "Alex Smith",
    source_entry_id: str = "id-a",
    key: str = "feed-identity",
    material_fingerprint: str = _HASH,
) -> int:
    """Seed one research mention; return person_mention id."""
    _, item_id = _seed_feed_and_item(
        connection,
        run_id=run_id,
        source_entry_id=source_entry_id,
        title_text=f"{exact_name} wins",
        key=key,
    )
    work_id = _work_item(
        connection,
        run_id=run_id,
        subject_id=item_id,
        fingerprint=material_fingerprint,
        state="running",
    )
    attempt_id = _attempt(connection, run_id=run_id, work_item_id=work_id)
    with immediate(connection):
        inspection_id = insert_model_inspection(
            connection,
            run_id=run_id,
            attempt_id=attempt_id,
            configured_model_id="openai/gpt-test",
            resolved_model_id="openai/gpt-test",
            routing_fingerprint=material_fingerprint,
            supported_parameters_json="[]",
            supports_strict_structured_output=True,
            pricing_usable=False,
            prompt_unit_price_nano_usd=None,
            completion_unit_price_nano_usd=None,
            compatibility="compatible",
            inspected_at=moment(),
        )
        insert_completed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            output=_sample_output(names=(exact_name,)),
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=material_fingerprint,
            input_truncated=False,
            observed_at=moment(),
        )
    mention_id = int(
        connection.execute(
            """
            SELECT m.id FROM person_mention AS m
              JOIN triage_observation AS t ON t.id = m.triage_observation_id
             WHERE t.source_item_id = ?
            """,
            (item_id,),
        ).fetchone()["id"]
    )
    return mention_id


def test_identity_aggregate_counts_split_outcomes_and_corpus(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    mention_a = _identity_mention_graph(
        connection,
        run_id=run_id,
        exact_name="Alex Smith",
        source_entry_id="a",
        material_fingerprint="a" * 64,
    )
    mention_b = _identity_mention_graph(
        connection,
        run_id=run_id,
        exact_name="Jordan Lee",
        source_entry_id="b",
        key="feed-b",
        material_fingerprint="b" * 64,
    )
    mention_c = _identity_mention_graph(
        connection,
        run_id=run_id,
        exact_name="Casey Ng",
        source_entry_id="c",
        key="feed-c",
        material_fingerprint="c" * 64,
    )
    # Model-path ERs need attempt + inspection rows (CHECK).
    resolve_work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES (
            ?, 'person_mention', ?, ?, 1, 40, ?, 'running', ?, ?, ?
        )
        """,
        (
            RESOLVE_PERSON_ENTITY_TASK_TYPE,
            mention_b,
            "d" * 64,
            moment(),
            run_id,
            moment(),
            moment(),
        ),
    ).lastrowid
    assert resolve_work is not None
    resolve_attempt = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        ) VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, resolve_work, moment(), moment(1), "d" * 64),
    ).lastrowid
    assert resolve_attempt is not None
    resolve_inspection = connection.execute(
        """
        INSERT INTO model_inspection (
            run_id, attempt_id, configured_model_id, resolved_model_id,
            routing_fingerprint, supported_parameters_json,
            supports_strict_structured_output, pricing_usable,
            prompt_unit_price_nano_usd, completion_unit_price_nano_usd,
            compatibility, inspected_at
        ) VALUES (?, ?, 'openai/gpt-test', 'openai/gpt-test', ?,
                  '["response_format"]', 1, 1, 100, 200, 'compatible', ?)
        """,
        (run_id, resolve_attempt, "d" * 64, moment()),
    ).lastrowid
    assert resolve_inspection is not None
    connection.commit()

    with immediate(connection):
        person_a = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        person_b = insert_person(
            connection,
            run_id=run_id,
            display_name="Jordan Lee",
            identity_fingerprint=_OTHER_HASH,
            created_at=moment(),
        )
        person_c = insert_person(
            connection,
            run_id=run_id,
            display_name="Casey Ng",
            identity_fingerprint=_THIRD_HASH,
            created_at=moment(),
        )
        er_created = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_a,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"created_new"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint="e" * 64,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            rationale="empty candidates",
            failure_category=None,
            observed_at=moment(),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_a,
            observation_id=er_created,
            person_id=person_a,
        )
        er_same = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_b,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=resolve_attempt,
            model_inspection_id=resolve_inspection,
            disposition="completed",
            semantic_outcome="same_person",
            selected_person_id=person_a,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_a}]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"same_person"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint="f" * 64,
            supporting_fact_ids_json="[]",
            conflicting_fact_ids_json="[]",
            rationale="same",
            failure_category=None,
            observed_at=moment(),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_b,
            observation_id=er_same,
            person_id=person_a,
        )
        er_diff = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_c,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=resolve_attempt,
            model_inspection_id=resolve_inspection,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=person_c,
            candidate_person_ids_json=f"[{person_a}]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"different_people"}',
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint="0" * 64,
            supporting_fact_ids_json="[]",
            conflicting_fact_ids_json="[]",
            rationale="different",
            failure_category=None,
            observed_at=moment(),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_c,
            observation_id=er_diff,
            person_id=person_c,
        )
        # Uncertain path opens an active edge; person_b is the created peer.
        edge_id = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er_created,
            now=moment(),
        )
        assert edge_id > 0
        # Confirmed merge relation (person_b into person_a conceptually).
        connection.execute(
            """
            INSERT INTO person_relation (
                kind, person_id_a, person_id_b, status, created_at,
                created_by_run_id, created_by_observation_id
            ) VALUES (
                'merge', ?, ?, 'active', ?, ?, ?
            )
            """,
            (person_a, person_b, moment(), run_id, er_same),
        )
        connection.execute(
            """
            UPDATE person SET merged_into_person_id = ? WHERE id = ?
            """,
            (person_a, person_b),
        )
        # Deferred + permanently failed resolve work attributed to this run.
        for state, fingerprint in (
            ("deferred", "1" * 64),
            ("failed_permanent", "2" * 64),
        ):
            connection.execute(
                """
                INSERT INTO work_item (
                    task_type, subject_kind, subject_id, fingerprint, required,
                    priority, eligible_at, state, created_by_run_id, created_at,
                    updated_at, completed_by_run_id
                ) VALUES (
                    ?, 'person_mention', ?, ?, 1, 40, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    RESOLVE_PERSON_ENTITY_TASK_TYPE,
                    mention_a,
                    fingerprint,
                    moment(),
                    state,
                    run_id,
                    moment(),
                    moment(),
                    run_id,
                ),
            )
        # Reconsider deferred must also count.
        connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at, completed_by_run_id
            ) VALUES (
                ?, 'person_relation', ?, ?, 1, 40, ?, 'deferred', ?, ?, ?, ?
            )
            """,
            (
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                edge_id,
                "3" * 64,
                moment(),
                run_id,
                moment(),
                moment(),
                run_id,
            ),
        )

    run_counts = identity_run_counts(connection, run_id=run_id)
    assert run_counts.people_created == 3
    assert run_counts.linked_same_person == 1
    assert run_counts.created_via_different_people == 1
    assert run_counts.created_via_created_new == 1
    assert run_counts.uncertain == 0
    assert run_counts.mentions_resolved == 3
    assert run_counts.active_possible_same_person == 1
    assert run_counts.confirmed_merges == 1
    assert run_counts.resolution_model_deferred == 2
    assert run_counts.resolution_model_failed == 1

    corpus = identity_corpus_counts(connection)
    assert corpus.canonical_people == 2  # person_b merged away
    assert corpus.merged_away_people == 1
    assert corpus.active_possible_same_person == 1
    assert corpus.mentions_linked_to_people == 3


# ---------------------------------------------------------------------------
# Permanent preflight: settle active detect_people without generation attempts
# ---------------------------------------------------------------------------


def test_permanent_preflight_settles_active_detect_people_without_generation_attempts(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    _, first = _seed_feed_and_item(connection, run_id=run_id, source_entry_id="p1")
    _, second = _seed_feed_and_item(
        connection, run_id=run_id, source_entry_id="p2", key="feed-b"
    )
    _, other_task_subject = _seed_feed_and_item(
        connection, run_id=run_id, source_entry_id="p3", key="feed-c"
    )
    _work_item(
        connection,
        run_id=run_id,
        subject_id=first,
        fingerprint=_HASH,
        state="pending",
    )
    _work_item(
        connection,
        run_id=run_id,
        subject_id=second,
        fingerprint=_OTHER_HASH,
        state="deferred",
    )
    _work_item(
        connection,
        run_id=run_id,
        subject_id=other_task_subject,
        fingerprint=_THIRD_HASH,
        state="pending",
        task_type="fetch_feed",
    )
    inspection_attempt = _attempt(connection, run_id=run_id, operation="inspect_model")

    settled = settle_active_detect_people_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=inspection_attempt,
        failure_category="authentication",
        rationale="OpenRouter authentication failed during model inspection.",
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
        now=moment(5),
    )
    assert settled == 2

    for item_id, fingerprint in ((first, _HASH), (second, _OTHER_HASH)):
        current = load_current_triage_observation(connection, source_item_id=item_id)
        assert current is not None
        assert current.disposition == "failed"
        assert current.failure_category == "authentication"
        assert current.attempt_id == inspection_attempt
        assert current.task_fingerprint == fingerprint
        assert current.model_inspection_id is None

    states = {
        row["subject_id"]: row["state"]
        for row in connection.execute(
            """
            SELECT subject_id, state FROM work_item
             WHERE task_type = ?
             ORDER BY subject_id
            """,
            (DETECT_PEOPLE_TASK_TYPE,),
        )
    }
    assert states == {first: "failed_permanent", second: "failed_permanent"}
    other_state = connection.execute(
        "SELECT state FROM work_item WHERE task_type = 'fetch_feed'"
    ).fetchone()["state"]
    assert other_state == "pending"

    generation_attempts = connection.execute(
        """
        SELECT COUNT(*) AS n FROM attempt
         WHERE operation = 'generate_structured'
        """
    ).fetchone()["n"]
    assert generation_attempts == 0

    assert list_untriaged_source_item_ids(connection) == (other_task_subject,)


def test_permanent_preflight_reuses_existing_observation_for_material_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    """A prior failed observation for the same material key must not abort settle.

    Unique (source_item_id, task_fingerprint) means a blind INSERT would raise
    IntegrityError and roll back the whole batch. Reuse the existing row, keep
    a single observation, and still settle the pending work item.
    """
    run_id = insert_run(connection)
    _, item_id = _seed_feed_and_item(connection, run_id=run_id)
    prior_attempt = _attempt(connection, run_id=run_id, operation="inspect_model")
    with immediate(connection):
        existing_id = insert_failed_observation(
            connection,
            source_item_id=item_id,
            run_id=run_id,
            attempt_id=prior_attempt,
            model_inspection_id=None,
            failure_category="authentication",
            canonical_supplied_input_json="{}",
            prompt_hash=_PROMPT_HASH,
            schema_hash=_SCHEMA_HASH,
            schema_version=1,
            task_fingerprint=_HASH,
            input_truncated=False,
            observed_at=moment(),
            rationale="Prior permanent preflight failure.",
        )
    # Clear the pointer so settle must re-point if it reuses the row.
    connection.execute(
        "UPDATE source_item SET current_triage_observation_id = NULL WHERE id = ?",
        (item_id,),
    )
    connection.commit()
    _work_item(
        connection,
        run_id=run_id,
        subject_id=item_id,
        fingerprint=_HASH,
        state="pending",
    )

    settled = settle_active_detect_people_after_permanent_preflight(
        connection,
        run_id=run_id,
        attempt_id=prior_attempt,
        failure_category="authentication",
        rationale="OpenRouter authentication failed during model inspection.",
        prompt_hash=_PROMPT_HASH,
        schema_hash=_SCHEMA_HASH,
        schema_version=1,
        now=moment(5),
    )
    assert settled == 1

    observation_count = connection.execute(
        """
        SELECT COUNT(*) AS n FROM triage_observation
         WHERE source_item_id = ? AND task_fingerprint = ?
        """,
        (item_id, _HASH),
    ).fetchone()["n"]
    assert observation_count == 1

    current = load_current_triage_observation(connection, source_item_id=item_id)
    assert current is not None
    assert current.id == existing_id
    assert current.attempt_id == prior_attempt

    state = connection.execute(
        """
        SELECT state FROM work_item
         WHERE task_type = ? AND subject_id = ?
        """,
        (DETECT_PEOPLE_TASK_TYPE, item_id),
    ).fetchone()["state"]
    assert state == "failed_permanent"
