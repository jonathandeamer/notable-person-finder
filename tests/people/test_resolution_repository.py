"""Transaction-neutral ER observation and person_relation writers."""

from __future__ import annotations

import sqlite3
from typing import TypedDict

import pytest

from notable_person_finder.people.identity import insert_person
from notable_person_finder.people.repository import (
    dismiss_relation,
    insert_entity_resolution_observation,
    list_active_possible_same_person_for,
    load_er_by_mention_fingerprint,
    load_er_by_relation_fingerprint,
    point_mention_current_er,
    supersede_relation,
    upsert_active_possible_same_person,
)
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_THIRD_HASH = "c" * 64
_PROMPT_HASH = "e" * 64
_SCHEMA_HASH = "f" * 64


class _ErCommonKwargs(TypedDict):
    run_id: int
    prompt_hash: str
    schema_hash: str
    schema_version: int
    task_fingerprint: str
    rationale: str
    observed_at: str
    canonical_supplied_input_json: str


def _seed_mention_graph(
    connection: sqlite3.Connection,
    *,
    exact_name: str = "Alex Smith",
) -> tuple[int, int, int, int]:
    """Return (run_id, attempt_id, inspection_id, mention_id)."""
    run_id = insert_run(connection)
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (_HASH, moment(), run_id, moment(), moment()),
    ).lastrowid
    assert work is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work, moment(), moment(1), _HASH),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES ('feed-a', 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (moment(), moment()),
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
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, 'entry-a', 'Alex Smith wins award', ?)
        """,
        (feed, fetch, run_id, moment()),
    ).lastrowid
    assert source_item_id is not None
    inspection_id = connection.execute(
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
        (run_id, attempt_id, _HASH, moment()),
    ).lastrowid
    assert inspection_id is not None
    observation_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded result', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT_HASH,
            _SCHEMA_HASH,
            _HASH,
            moment(),
        ),
    ).lastrowid
    assert observation_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, 'research', '[]', 'reason')
        """,
        (observation_id, exact_name, exact_name),
    ).lastrowid
    assert mention_id is not None
    connection.commit()
    return run_id, attempt_id, inspection_id, mention_id


def _person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    display_name: str = "Alex Smith",
    fingerprint: str = _HASH,
) -> int:
    with immediate(connection):
        return insert_person(
            connection,
            run_id=run_id,
            display_name=display_name,
            identity_fingerprint=fingerprint,
            created_at=moment(),
        )


def _second_attempt_and_inspection(
    connection: sqlite3.Connection, *, run_id: int
) -> tuple[int, int]:
    connection.execute("UPDATE work_item SET state = 'succeeded'")
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES (
            'reconsider_person_entity', 'person_relation', 1, ?, 1, 40, ?,
            'running', ?, ?, ?
        )
        """,
        (_OTHER_HASH, moment(), run_id, moment(), moment()),
    ).lastrowid
    assert work is not None
    attempt_id = connection.execute(
        """
        INSERT INTO attempt (
            run_id, work_item_id, provider, operation, ordinal, started_at,
            finished_at, outcome, request_fingerprint
        )
        VALUES (?, ?, 'openrouter', 'generate_structured', 1, ?, ?, 'succeeded', ?)
        """,
        (run_id, work, moment(), moment(1), _OTHER_HASH),
    ).lastrowid
    assert attempt_id is not None
    inspection_id = connection.execute(
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
        (run_id, attempt_id, _OTHER_HASH, moment()),
    ).lastrowid
    assert inspection_id is not None
    connection.commit()
    return attempt_id, inspection_id


def _er_common(
    *,
    run_id: int,
    task_fingerprint: str = _HASH,
) -> _ErCommonKwargs:
    return {
        "run_id": run_id,
        "prompt_hash": _PROMPT_HASH,
        "schema_hash": _SCHEMA_HASH,
        "schema_version": 1,
        "task_fingerprint": task_fingerprint,
        "rationale": "reason",
        "observed_at": moment(),
        "canonical_supplied_input_json": "{}",
    }


# ---------------------------------------------------------------------------
# First-pass disposition paths
# ---------------------------------------------------------------------------


def test_insert_first_pass_created_new(connection: sqlite3.Connection) -> None:
    run_id, _attempt_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_id,
            observation_id=er_id,
            person_id=person_id,
        )

    loaded = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=_HASH
    )
    assert loaded is not None
    assert loaded.id == er_id
    assert loaded.disposition == "completed"
    assert loaded.semantic_outcome == "created_new"
    assert loaded.attempt_id is None
    assert loaded.created_person_id == person_id
    assert loaded.selected_person_id is None
    mention = connection.execute(
        """
        SELECT person_id, current_entity_resolution_observation_id
          FROM person_mention WHERE id = ?
        """,
        (mention_id,),
    ).fetchone()
    assert mention["person_id"] == person_id
    assert mention["current_entity_resolution_observation_id"] == er_id


@pytest.mark.parametrize(
    ("semantic_outcome", "selected", "created"),
    [
        ("same_person", True, False),
        ("different_people", False, True),
        ("uncertain", False, True),
    ],
)
def test_insert_first_pass_model_completed(
    connection: sqlite3.Connection,
    semantic_outcome: str,
    selected: bool,
    created: bool,
) -> None:
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    created_person = _person(connection, run_id=run_id)
    selected_person = _person(
        connection,
        run_id=run_id,
        display_name="Peer",
        fingerprint=_OTHER_HASH,
    )
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome=semantic_outcome,
            selected_person_id=selected_person if selected else None,
            created_person_id=created_person if created else None,
            candidate_person_ids_json=f"[{selected_person}]",
            validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )

    loaded = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=_HASH
    )
    assert loaded is not None
    assert loaded.id == er_id
    assert loaded.semantic_outcome == semantic_outcome
    assert loaded.attempt_id == attempt_id
    assert loaded.model_inspection_id == inspection_id
    if selected:
        assert loaded.selected_person_id == selected_person
        assert loaded.created_person_id is None
    else:
        assert loaded.selected_person_id is None
        assert loaded.created_person_id == created_person


def test_insert_first_pass_skipped(connection: sqlite3.Connection) -> None:
    run_id, _attempt_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="skipped",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            validated_output_json=None,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_id,
            observation_id=er_id,
            person_id=None,
        )

    loaded = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=_HASH
    )
    assert loaded is not None
    assert loaded.disposition == "skipped"
    assert loaded.semantic_outcome is None
    assert loaded.attempt_id is None
    mention = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert mention["person_id"] is None


def test_insert_first_pass_failed_requires_attempt(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=None,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            validated_output_json=None,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category="malformed_response",
            **_er_common(run_id=run_id),
        )
    loaded = load_er_by_mention_fingerprint(
        connection, person_mention_id=mention_id, task_fingerprint=_HASH
    )
    assert loaded is not None
    assert loaded.id == er_id
    assert loaded.disposition == "failed"
    assert loaded.failure_category == "malformed_response"
    assert loaded.attempt_id == attempt_id

    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            validated_output_json=None,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category="malformed_response",
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )


# ---------------------------------------------------------------------------
# Reconsider disposition paths
# ---------------------------------------------------------------------------


def _open_uncertain_edge(
    connection: sqlite3.Connection,
) -> tuple[int, int, int, int, int, int]:
    """Return (run_id, attempt_id, inspection_id, mention_id, person_a, relation_id)."""
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    person_a = _person(connection, run_id=run_id)
    person_b = _person(
        connection, run_id=run_id, display_name="Peer", fingerprint=_OTHER_HASH
    )
    with immediate(connection):
        first_pass = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json=f"[{person_b}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        relation_id = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=first_pass,
            now=moment(),
        )
    return run_id, attempt_id, inspection_id, mention_id, person_a, relation_id


@pytest.mark.parametrize(
    "semantic_outcome",
    ["same_person", "different_people", "uncertain"],
)
def test_insert_reconsider_completed_never_creates_person(
    connection: sqlite3.Connection,
    semantic_outcome: str,
) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    person_b = connection.execute(
        "SELECT person_id_b FROM person_relation WHERE id = ?", (relation_id,)
    ).fetchone()["person_id_b"]
    # peer may be a or b depending on sort; get the other endpoint
    endpoints = connection.execute(
        "SELECT person_id_a, person_id_b FROM person_relation WHERE id = ?",
        (relation_id,),
    ).fetchone()
    peer = (
        endpoints["person_id_b"]
        if endpoints["person_id_a"] == person_a
        else endpoints["person_id_a"]
    )
    selected = person_a if semantic_outcome == "same_person" else None
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome=semantic_outcome,
            selected_person_id=selected,
            created_person_id=None,
            candidate_person_ids_json=f"[{peer}]",
            validated_output_json=f'{{"outcome":"{semantic_outcome}"}}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )

    loaded = load_er_by_relation_fingerprint(
        connection, person_relation_id=relation_id, task_fingerprint=_OTHER_HASH
    )
    assert loaded is not None
    assert loaded.id == er_id
    assert loaded.person_mention_id is None
    assert loaded.person_relation_id == relation_id
    assert loaded.created_person_id is None
    assert loaded.semantic_outcome == semantic_outcome
    # person_b only used for readability in setup; silence unused if same as peer
    assert person_b in (endpoints["person_id_a"], endpoints["person_id_b"])


def test_insert_reconsider_failed(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, _m, _person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="failed",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[1]",
            validated_output_json=None,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category="provider_error",
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
    loaded = load_er_by_relation_fingerprint(
        connection, person_relation_id=relation_id, task_fingerprint=_OTHER_HASH
    )
    assert loaded is not None
    assert loaded.id == er_id
    assert loaded.disposition == "failed"


# ---------------------------------------------------------------------------
# CHECK rejection of illegal combinations
# ---------------------------------------------------------------------------


def test_check_rejects_created_person_on_reconsider_completed(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json=f"[{person_a}]",
            validated_output_json='{"outcome":"different_people"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )


def test_check_rejects_skipped_on_relation_scoped_row(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, _m, _person_a, relation_id = _open_uncertain_edge(connection)
    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="skipped",
            semantic_outcome=None,
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[]",
            validated_output_json=None,
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )


def test_check_rejects_created_new_on_relation_scoped_row(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )


def test_check_rejects_both_subjects_null(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, _m = _seed_mention_graph(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )


def test_check_rejects_first_pass_same_person_without_attempt(
    connection: sqlite3.Connection,
) -> None:
    run_id, _attempt_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    selected = _person(connection, run_id=run_id)
    with immediate(connection), pytest.raises(sqlite3.IntegrityError):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="same_person",
            selected_person_id=selected,
            created_person_id=None,
            candidate_person_ids_json=f"[{selected}]",
            validated_output_json='{"outcome":"same_person"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )


# ---------------------------------------------------------------------------
# Fingerprint reuse / point
# ---------------------------------------------------------------------------


def test_insert_reuses_existing_row_on_mention_fingerprint_conflict(
    connection: sqlite3.Connection,
) -> None:
    run_id, _attempt_id, _inspection_id, mention_id = _seed_mention_graph(connection)
    person_id = _person(connection, run_id=run_id)
    with immediate(connection):
        first = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        # Second insert with same fingerprint must reuse; does not create another row.
        second = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        point_mention_current_er(
            connection,
            person_mention_id=mention_id,
            observation_id=second,
            person_id=person_id,
        )

    assert second == first
    count = connection.execute(
        "SELECT COUNT(*) AS n FROM entity_resolution_observation"
    ).fetchone()["n"]
    assert count == 1
    people = connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()["n"]
    assert people == 1


def test_point_mention_current_er_rejects_relation_scoped_observation(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, mention_id, _person_a, relation_id = _open_uncertain_edge(
        connection
    )
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    with immediate(connection):
        relation_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json="[1]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        with pytest.raises(sqlite3.IntegrityError):
            point_mention_current_er(
                connection,
                person_mention_id=mention_id,
                observation_id=relation_er,
                person_id=None,
            )


def test_writers_require_active_transaction(connection: sqlite3.Connection) -> None:
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    person_id = _person(connection, run_id=run_id)
    with pytest.raises(RuntimeError, match="requires an active transaction"):
        insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            validated_output_json='{"outcome":"created_new"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
    with pytest.raises(RuntimeError, match="requires an active transaction"):
        point_mention_current_er(
            connection,
            person_mention_id=mention_id,
            observation_id=1,
            person_id=person_id,
        )
    with pytest.raises(RuntimeError, match="requires an active transaction"):
        upsert_active_possible_same_person(
            connection,
            person_id_a=1,
            person_id_b=2,
            run_id=run_id,
            created_by_observation_id=1,
            now=moment(),
        )
    # silence unused seed values used only for structure
    assert attempt_id and inspection_id


# ---------------------------------------------------------------------------
# Active edge uniqueness, dismiss, supersede, list
# ---------------------------------------------------------------------------


def test_upsert_active_possible_same_person_orders_ids_and_is_unique(
    connection: sqlite3.Connection,
) -> None:
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    person_low = _person(connection, run_id=run_id, fingerprint=_HASH)
    person_high = _person(
        connection, run_id=run_id, display_name="Peer", fingerprint=_OTHER_HASH
    )
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=person_low,
            candidate_person_ids_json=f"[{person_high}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        # Pass high, low first — writer must store a < b.
        first = upsert_active_possible_same_person(
            connection,
            person_id_a=person_high,
            person_id_b=person_low,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=moment(),
        )
        second = upsert_active_possible_same_person(
            connection,
            person_id_a=person_low,
            person_id_b=person_high,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=moment(),
        )

    assert first == second
    row = connection.execute(
        """
        SELECT person_id_a, person_id_b, status, kind
          FROM person_relation
         WHERE id = ?
        """,
        (first,),
    ).fetchone()
    assert row["person_id_a"] == min(person_low, person_high)
    assert row["person_id_b"] == max(person_low, person_high)
    assert row["status"] == "active"
    assert row["kind"] == "possible_same_person"
    count = connection.execute("SELECT COUNT(*) AS n FROM person_relation").fetchone()[
        "n"
    ]
    assert count == 1


def test_dismiss_relation(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    closed_at = moment(10)
    with immediate(connection):
        dismiss_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_a}]",
            validated_output_json='{"outcome":"different_people"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        dismiss_relation(
            connection,
            relation_id=relation_id,
            closed_by_observation_id=dismiss_er,
            closed_at=closed_at,
        )

    row = connection.execute(
        """
        SELECT status, closed_at, closed_by_observation_id
          FROM person_relation WHERE id = ?
        """,
        (relation_id,),
    ).fetchone()
    assert row["status"] == "dismissed"
    assert row["closed_at"] == closed_at
    assert row["closed_by_observation_id"] == dismiss_er


def test_supersede_relation(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    closed_at = moment(20)
    with immediate(connection):
        close_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="same_person",
            selected_person_id=person_a,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_a}]",
            validated_output_json='{"outcome":"same_person"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        supersede_relation(
            connection,
            relation_id=relation_id,
            closed_by_observation_id=close_er,
            closed_at=closed_at,
        )

    row = connection.execute(
        """
        SELECT status, closed_at, closed_by_observation_id
          FROM person_relation WHERE id = ?
        """,
        (relation_id,),
    ).fetchone()
    assert row["status"] == "superseded_by_merge"
    assert row["closed_at"] == closed_at
    assert row["closed_by_observation_id"] == close_er


def test_dismissed_pair_may_get_new_active_edge(
    connection: sqlite3.Connection,
) -> None:
    """Unique index is partial on active rows; dismissed edges free the pair."""
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    person_a = _person(connection, run_id=run_id, fingerprint=_HASH)
    person_b = _person(
        connection, run_id=run_id, display_name="Peer", fingerprint=_OTHER_HASH
    )
    with immediate(connection):
        er1 = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json=f"[{person_b}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        edge1 = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er1,
            now=moment(),
        )
    att2, insp2 = _second_attempt_and_inspection(connection, run_id=run_id)
    with immediate(connection):
        dismiss_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=edge1,
            attempt_id=att2,
            model_inspection_id=insp2,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_b}]",
            validated_output_json='{"outcome":"different_people"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        dismiss_relation(
            connection,
            relation_id=edge1,
            closed_by_observation_id=dismiss_er,
            closed_at=moment(5),
        )
        edge2 = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er1,
            now=moment(6),
        )
    assert edge2 != edge1
    assert (
        connection.execute(
            """
            SELECT COUNT(*) AS n FROM person_relation
             WHERE kind = 'possible_same_person' AND status = 'active'
            """
        ).fetchone()["n"]
        == 1
    )


def test_list_active_possible_same_person_for(connection: sqlite3.Connection) -> None:
    run_id, attempt_id, inspection_id, mention_id = _seed_mention_graph(connection)
    person_a = _person(connection, run_id=run_id, fingerprint=_HASH)
    person_b = _person(
        connection, run_id=run_id, display_name="B", fingerprint=_OTHER_HASH
    )
    person_c = _person(
        connection, run_id=run_id, display_name="C", fingerprint=_THIRD_HASH
    )
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=person_a,
            candidate_person_ids_json=f"[{person_b},{person_c}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id),
        )
        edge_ab = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=moment(),
        )
        edge_ac = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_c,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=moment(1),
        )
    att2, insp2 = _second_attempt_and_inspection(connection, run_id=run_id)
    with immediate(connection):
        dismiss_er = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=edge_ab,
            attempt_id=att2,
            model_inspection_id=insp2,
            disposition="completed",
            semantic_outcome="different_people",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_b}]",
            validated_output_json='{"outcome":"different_people"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        dismiss_relation(
            connection,
            relation_id=edge_ab,
            closed_by_observation_id=dismiss_er,
            closed_at=moment(5),
        )

    active_for_a = list_active_possible_same_person_for(connection, person_a)
    assert [r.id for r in active_for_a] == [edge_ac]
    assert active_for_a[0].status == "active"
    assert person_a in (
        active_for_a[0].person_id_a,
        active_for_a[0].person_id_b,
    )

    active_for_b = list_active_possible_same_person_for(connection, person_b)
    assert active_for_b == ()

    active_for_c = list_active_possible_same_person_for(connection, person_c)
    assert [r.id for r in active_for_c] == [edge_ac]


def test_insert_relation_scoped_reuses_on_fingerprint_conflict(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, _m, person_a, relation_id = _open_uncertain_edge(connection)
    attempt_id, inspection_id = _second_attempt_and_inspection(
        connection, run_id=run_id
    )
    with immediate(connection):
        first = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_a}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
        second = insert_entity_resolution_observation(
            connection,
            person_mention_id=None,
            person_relation_id=relation_id,
            attempt_id=attempt_id,
            model_inspection_id=inspection_id,
            disposition="completed",
            semantic_outcome="uncertain",
            selected_person_id=None,
            created_person_id=None,
            candidate_person_ids_json=f"[{person_a}]",
            validated_output_json='{"outcome":"uncertain"}',
            supporting_fact_ids_json=None,
            conflicting_fact_ids_json=None,
            failure_category=None,
            **_er_common(run_id=run_id, task_fingerprint=_OTHER_HASH),
        )
    assert second == first
    count = connection.execute(
        """
        SELECT COUNT(*) AS n FROM entity_resolution_observation
         WHERE person_relation_id = ?
        """,
        (relation_id,),
    ).fetchone()["n"]
    assert count == 1
