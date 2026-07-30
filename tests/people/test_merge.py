"""Confirmed merges and canonical work reconciliation (K7, K12, K13)."""

from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.people.candidates import retrieve_candidates
from notable_person_finder.people.identity import (
    canonical_person_id,
    compute_identity_fingerprint,
    insert_person,
    mentions_for_canonical_person,
    person_id_closure_for_canonical,
    recompute_identity_fingerprint,
    upsert_sourced_name,
)
from notable_person_finder.people.merge import (
    MERGED_AWAY_REASON,
    confirm_person_merge,
    reconcile_digest_queue_on_merge,
)
from notable_person_finder.people.repository import (
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    insert_entity_resolution_observation,
    mechanical_search_name,
    upsert_active_possible_same_person,
)
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER = "b" * 64
_THIRD = "c" * 64
_PROMPT = "p" * 64
_SCHEMA = "s" * 64
NOW = moment()


def _seed_graph(connection: sqlite3.Connection) -> tuple[int, int, int]:
    """Return (run_id, attempt_id, inspection_id) with a minimal valid graph."""
    run_id = insert_run(connection)
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (_HASH, NOW, run_id, NOW, NOW),
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
        (run_id, work, NOW, moment(1), _HASH),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES ('feed-a', 'Feed A', 'https://example.com/feed', ?, ?)
        """,
        (NOW, NOW),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, 'https://example.com/feed', 'modified')
        """,
        (feed, run_id, NOW),
    ).lastrowid
    assert fetch is not None
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
        (run_id, attempt_id, _HASH, NOW),
    ).lastrowid
    assert inspection_id is not None
    connection.commit()
    return run_id, attempt_id, inspection_id


def _add_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    attempt_id: int,
    inspection_id: int,
    exact_name: str,
    non_name_facts: tuple[tuple[str, str], ...] = (),
    entry_id: str = "entry-x",
) -> int:
    feed = connection.execute(
        "SELECT id FROM feed_identity ORDER BY id LIMIT 1"
    ).fetchone()
    assert feed is not None
    fetch = connection.execute(
        "SELECT id FROM feed_fetch ORDER BY id LIMIT 1"
    ).fetchone()
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, summary_text, discovered_at
        ) VALUES (?, ?, ?, ?, ?, 'summary', ?)
        """,
        (feed["id"], fetch["id"], run_id, entry_id, exact_name, NOW),
    ).lastrowid
    assert source_item_id is not None
    observation_id = connection.execute(
        """
        INSERT INTO triage_observation (
            source_item_id, run_id, attempt_id, model_inspection_id,
            disposition, semantic_outcome, canonical_supplied_input_json,
            validated_output_json, prompt_hash, schema_hash, schema_version,
            task_fingerprint, input_truncated, overflow, rationale, observed_at
        ) VALUES (?, ?, ?, ?, 'completed', 'research_people', '{}', '{}',
                  ?, ?, 1, ?, 0, 0, 'Grounded', ?)
        """,
        (
            source_item_id,
            run_id,
            attempt_id,
            inspection_id,
            _PROMPT,
            _SCHEMA,
            (entry_id + _HASH)[:64],
            NOW,
        ),
    ).lastrowid
    assert observation_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale
        ) VALUES (?, 1, ?, ?, 'research', '["p1"]', 'reason')
        """,
        (observation_id, exact_name, mechanical_search_name(exact_name)),
    ).lastrowid
    assert mention_id is not None
    for index, (kind, value) in enumerate(non_name_facts, start=1):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, ?, ?, ?, '["p1"]')
            """,
            (mention_id, f"fact-{index}", kind, value),
        )
    connection.commit()
    return int(mention_id)


def _link_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    mention_id: int,
    person_id: int,
) -> int:
    fingerprint = f"{mention_id:064x}"
    with immediate(connection):
        er_id = insert_entity_resolution_observation(
            connection,
            person_mention_id=mention_id,
            person_relation_id=None,
            run_id=run_id,
            attempt_id=None,
            model_inspection_id=None,
            disposition="completed",
            semantic_outcome="created_new",
            selected_person_id=None,
            created_person_id=person_id,
            candidate_person_ids_json="[]",
            canonical_supplied_input_json="{}",
            validated_output_json='{"outcome":"created_new"}',
            prompt_hash=_PROMPT,
            schema_hash=_SCHEMA,
            schema_version=1,
            task_fingerprint=fingerprint,
            rationale="test link",
            failure_category=None,
            observed_at=NOW,
        )
        connection.execute(
            """
            UPDATE person_mention
               SET person_id = ?,
                   current_entity_resolution_observation_id = ?
             WHERE id = ?
            """,
            (person_id, er_id, mention_id),
        )
        recompute_identity_fingerprint(connection, person_id)
    return er_id


def _two_people(
    connection: sqlite3.Connection,
    *,
    name: str = "Alex Smith",
    a_facts: tuple[tuple[str, str], ...] = (("profession_or_role", "sculptor"),),
    b_facts: tuple[tuple[str, str], ...] = (("profession_or_role", "painter"),),
    b_name: str | None = None,
) -> tuple[int, int, int, int, int, int]:
    """Return run_id, attempt_id, inspection_id, person_a, person_b, er_id."""
    run_id, attempt_id, inspection_id = _seed_graph(connection)
    mention_a = _add_mention(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
        exact_name=name,
        non_name_facts=a_facts,
        entry_id="entry-a",
    )
    mention_b = _add_mention(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
        exact_name=b_name or name,
        non_name_facts=b_facts,
        entry_id="entry-b",
    )
    with immediate(connection):
        person_a = insert_person(
            connection,
            run_id=run_id,
            display_name=name,
            identity_fingerprint=_HASH,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_a,
            exact_name=name,
            kind="display",
            origin_kind="person_mention",
            origin_mention_id=mention_a,
            observed_at=NOW,
        )
        person_b = insert_person(
            connection,
            run_id=run_id,
            display_name=b_name or name,
            identity_fingerprint=_OTHER,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=person_b,
            exact_name=b_name or name,
            kind="display",
            origin_kind="person_mention",
            origin_mention_id=mention_b,
            observed_at=NOW,
        )
    er_a = _link_mention(
        connection, run_id=run_id, mention_id=mention_a, person_id=person_a
    )
    _link_mention(connection, run_id=run_id, mention_id=mention_b, person_id=person_b)
    return run_id, attempt_id, inspection_id, person_a, person_b, er_a


def test_merge_does_not_rewrite_historical_mention_or_er_fks(
    connection: sqlite3.Connection,
) -> None:
    """K12: mention.person_id and ER selected/created FKs stay on the loser."""
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    assert person_a < person_b
    loser_mention = connection.execute(
        "SELECT id, person_id FROM person_mention WHERE person_id = ?",
        (person_b,),
    ).fetchone()
    assert loser_mention is not None
    mention_id = int(loser_mention["id"])
    er_before = connection.execute(
        """
        SELECT id, created_person_id, selected_person_id, person_mention_id
          FROM entity_resolution_observation
         WHERE person_mention_id = ?
        """,
        (mention_id,),
    ).fetchone()
    assert er_before is not None
    assert er_before["created_person_id"] == person_b

    with immediate(connection):
        survivor = confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )

    assert survivor == person_a
    mention_after = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?",
        (mention_id,),
    ).fetchone()
    assert mention_after is not None
    assert mention_after["person_id"] == person_b  # historical FK unchanged
    er_after = connection.execute(
        """
        SELECT created_person_id, selected_person_id
          FROM entity_resolution_observation WHERE id = ?
        """,
        (er_before["id"],),
    ).fetchone()
    assert er_after is not None
    assert er_after["created_person_id"] == person_b
    # Loser still exists; redirect only.
    person = connection.execute(
        "SELECT merged_into_person_id FROM person WHERE id = ?",
        (person_b,),
    ).fetchone()
    assert person is not None
    assert person["merged_into_person_id"] == person_a
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM person WHERE id = ?", (person_b,)
        ).fetchone()["n"]
        == 1
    )


def test_fingerprint_includes_loser_mention_facts_after_merge(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, person_a, person_b, er_id = _two_people(
        connection,
        a_facts=(("profession_or_role", "sculptor"),),
        b_facts=(("place", "Paris"),),
        b_name="Unique Loser Name",
    )
    before = compute_identity_fingerprint(connection, person_a)
    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    after = compute_identity_fingerprint(connection, person_a)
    stored = connection.execute(
        "SELECT identity_fingerprint FROM person WHERE id = ?",
        (person_a,),
    ).fetchone()["identity_fingerprint"]
    assert after != before
    assert stored == after
    # Closure projection includes both people and the loser mention.
    assert person_id_closure_for_canonical(connection, person_a) == tuple(
        sorted((person_a, person_b))
    )
    mention_person_ids = {
        m.person_id for m in mentions_for_canonical_person(connection, person_a)
    }
    assert person_a in mention_person_ids
    assert person_b in mention_person_ids
    # Distinct loser name must appear on survivor via upsert.
    name_keys = {
        row["match_key"]
        for row in connection.execute(
            "SELECT match_key FROM sourced_name WHERE person_id = ?",
            (person_a,),
        )
    }
    assert "unique loser name" in name_keys


def test_candidate_scoring_uses_post_merge_projection(
    connection: sqlite3.Connection,
) -> None:
    """After merge, retrieve_candidates scores survivor with loser facts (K12)."""
    run_id, attempt_id, inspection_id, person_a, person_b, er_id = _two_people(
        connection,
        a_facts=(),
        b_facts=(("profession_or_role", "composer"),),
    )
    query_mention = _add_mention(
        connection,
        run_id=run_id,
        attempt_id=attempt_id,
        inspection_id=inspection_id,
        exact_name="Alex Smith",
        non_name_facts=(("profession_or_role", "composer"),),
        entry_id="entry-query",
    )
    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    candidates = retrieve_candidates(
        connection,
        person_mention_id=query_mention,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert len(candidates) == 1
    assert candidates[0].person_id == person_a
    assert "composer" in {f.value for f in candidates[0].facts}
    # Loser excluded from candidate list (merged-away).
    assert all(c.person_id != person_b for c in candidates)


def test_status_counts_de_dupe_canonical_people(
    connection: sqlite3.Connection,
) -> None:
    """Corpus people counts use merged_into IS NULL only."""
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    before = connection.execute(
        "SELECT COUNT(*) AS n FROM person WHERE merged_into_person_id IS NULL"
    ).fetchone()["n"]
    assert before == 2
    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    after = connection.execute(
        "SELECT COUNT(*) AS n FROM person WHERE merged_into_person_id IS NULL"
    ).fetchone()["n"]
    assert after == 1
    total = connection.execute("SELECT COUNT(*) AS n FROM person").fetchone()["n"]
    assert total == 2  # loser row retained


def test_relation_and_person_work_superseded_on_merge(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    with immediate(connection):
        relation_id = upsert_active_possible_same_person(
            connection,
            person_id_a=person_a,
            person_id_b=person_b,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=NOW,
        )
    # Third peer edge on the loser.
    with immediate(connection):
        peer = insert_person(
            connection,
            run_id=run_id,
            display_name="Peer Person",
            identity_fingerprint=_THIRD,
            created_at=NOW,
        )
        upsert_sourced_name(
            connection,
            person_id=peer,
            exact_name="Peer Person",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
        peer_relation = upsert_active_possible_same_person(
            connection,
            person_id_a=person_b,
            person_id_b=peer,
            run_id=run_id,
            created_by_observation_id=er_id,
            now=NOW,
        )
    # Person-subject + relation-scoped + pending resolve on loser mention.
    loser_mention = connection.execute(
        "SELECT id FROM person_mention WHERE person_id = ?",
        (person_b,),
    ).fetchone()
    assert loser_mention is not None
    connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        ) VALUES
          ('research_person', 'person', ?, ?, 1, 50, ?, 'pending', ?, ?, ?),
          (?, 'person_relation', ?, ?, 1, 40, ?, 'pending', ?, ?, ?),
          (?, 'person_relation', ?, ?, 1, 40, ?, 'deferred', ?, ?, ?),
          (?, 'person_mention', ?, ?, 1, 35, ?, 'pending', ?, ?, ?)
        """,
        (
            person_b,
            "1" * 64,
            NOW,
            run_id,
            NOW,
            NOW,
            RECONSIDER_PERSON_ENTITY_TASK_TYPE,
            relation_id,
            "2" * 64,
            NOW,
            run_id,
            NOW,
            NOW,
            RECONSIDER_PERSON_ENTITY_TASK_TYPE,
            peer_relation,
            "3" * 64,
            NOW,
            run_id,
            NOW,
            NOW,
            RESOLVE_PERSON_ENTITY_TASK_TYPE,
            loser_mention["id"],
            "4" * 64,
            NOW,
            run_id,
            NOW,
            NOW,
        ),
    )
    connection.commit()

    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=moment(5),
        )

    # Original A↔B edge superseded.
    ab = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?",
        (relation_id,),
    ).fetchone()
    assert ab is not None
    assert ab["status"] == "superseded_by_merge"
    # Loser↔peer superseded; peer re-linked to survivor.
    bp = connection.execute(
        "SELECT status FROM person_relation WHERE id = ?",
        (peer_relation,),
    ).fetchone()
    assert bp is not None
    assert bp["status"] == "superseded_by_merge"
    relinked = connection.execute(
        """
        SELECT id, status, person_id_a, person_id_b
          FROM person_relation
         WHERE kind = 'possible_same_person'
           AND status = 'active'
           AND person_id_a = ? AND person_id_b = ?
        """,
        (person_a, peer),
    ).fetchone()
    assert relinked is not None
    # Merge relation recorded loser→survivor.
    merge_row = connection.execute(
        """
        SELECT person_id_a, person_id_b, status
          FROM person_relation
         WHERE kind = 'merge'
        """
    ).fetchone()
    assert merge_row is not None
    assert merge_row["person_id_a"] == person_b
    assert merge_row["person_id_b"] == person_a
    assert merge_row["status"] == "active"

    for fingerprint in ("1" * 64, "2" * 64, "3" * 64, "4" * 64):
        work = connection.execute(
            "SELECT state, reason FROM work_item WHERE fingerprint = ?",
            (fingerprint,),
        ).fetchone()
        assert work is not None
        assert work["state"] == "superseded"
        assert work["reason"] == MERGED_AWAY_REASON


def test_idempotent_re_merge_returns_survivor(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    with immediate(connection):
        first = confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    with immediate(connection):
        second = confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=moment(2),
        )
    assert first == person_a
    assert second == person_a
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM person_relation WHERE kind = 'merge'"
        ).fetchone()["n"]
        == 1
    )
    # Wrong call order still resolves to lower canonical.
    with immediate(connection):
        third = confirm_person_merge(
            connection,
            loser_id=person_a,
            survivor_id=person_b,
            run_id=run_id,
            observation_id=er_id,
            now=moment(3),
        )
    assert third == person_a


def test_survivor_is_lower_canonical_id_regardless_of_call_order(
    connection: sqlite3.Connection,
) -> None:
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    assert person_a < person_b
    with immediate(connection):
        survivor = confirm_person_merge(
            connection,
            loser_id=person_a,  # caller swapped labels
            survivor_id=person_b,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    assert survivor == person_a
    assert canonical_person_id(connection, person_b) == person_a
    assert (
        connection.execute(
            "SELECT merged_into_person_id FROM person WHERE id = ?",
            (person_a,),
        ).fetchone()["merged_into_person_id"]
        is None
    )


def test_self_link_is_noop_idempotent(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, person_a, _person_b, er_id = _two_people(connection)
    with immediate(connection):
        result = confirm_person_merge(
            connection,
            loser_id=person_a,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    assert result == person_a
    assert (
        connection.execute(
            "SELECT COUNT(*) AS n FROM person_relation WHERE kind = 'merge'"
        ).fetchone()["n"]
        == 0
    )
    assert (
        connection.execute(
            "SELECT merged_into_person_id FROM person WHERE id = ?",
            (person_a,),
        ).fetchone()["merged_into_person_id"]
        is None
    )


def test_flatten_redirects_when_prior_loser_points_at_new_loser(
    connection: sqlite3.Connection,
) -> None:
    """Anyone pointing at the loser is flattened to the survivor (one hop)."""
    run_id, _a, _i, person_a, person_b, er_id = _two_people(connection)
    with immediate(connection):
        nested = insert_person(
            connection,
            run_id=run_id,
            display_name="Nested Loser",
            identity_fingerprint=_THIRD,
            created_at=NOW,
            merged_into_person_id=person_b,
        )
        upsert_sourced_name(
            connection,
            person_id=nested,
            exact_name="Nested Loser",
            kind="alias",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=NOW,
        )
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
    assert (
        connection.execute(
            "SELECT merged_into_person_id FROM person WHERE id = ?",
            (nested,),
        ).fetchone()["merged_into_person_id"]
        == person_a
    )
    assert canonical_person_id(connection, nested) == person_a


def test_digest_queue_hook_is_noop(connection: sqlite3.Connection) -> None:
    """K13: named hook does not create tables or rows."""
    run_id, _a, _i, person_a, person_b, _er = _two_people(connection)
    tables_before = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    with immediate(connection):
        reconcile_digest_queue_on_merge(
            connection,
            survivor_id=person_a,
            loser_id=person_b,
            now=NOW,
        )
    tables_after = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
    }
    assert tables_after == tables_before
    assert "digest_queue" not in tables_after
    # Calling it during a real merge must not fail either.
    with immediate(connection):
        confirm_person_merge(
            connection,
            loser_id=person_b,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=None,
            now=NOW,
        )


def test_missing_person_raises(connection: sqlite3.Connection) -> None:
    run_id, _a, _i, person_a, _b, er_id = _two_people(connection)
    with immediate(connection), pytest.raises(LookupError):
        confirm_person_merge(
            connection,
            loser_id=999_999,
            survivor_id=person_a,
            run_id=run_id,
            observation_id=er_id,
            now=NOW,
        )
