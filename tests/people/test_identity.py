"""Tests for durable person identity helpers and person/name writers."""

from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.people.identity import (
    canonical_person_id,
    collapse_whitespace,
    compute_identity_fingerprint,
    create_person_for_mention,
    insert_person,
    match_key,
    mentions_for_canonical_person,
    person_id_closure_for_canonical,
    recompute_identity_fingerprint,
    select_display_name,
    upsert_sourced_name,
)
from notable_person_finder.people.repository import (
    collapse_whitespace as repo_collapse_whitespace,
)
from notable_person_finder.people.repository import match_key as repo_match_key
from notable_person_finder.people.repository import mechanical_search_name
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64


def _seed_mention(
    connection: sqlite3.Connection,
    *,
    exact_name: str = "Alex Smith",
    fact_values: tuple[tuple[str, str], ...] = (),
) -> tuple[int, int]:
    """Return (run_id, mention_id) with a completed triage observation."""
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
            _HASH,
            _OTHER_HASH,
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
        (observation_id, exact_name, mechanical_search_name(exact_name)),
    ).lastrowid
    assert mention_id is not None
    for local_id, (kind, value) in enumerate(fact_values, start=1):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, ?, ?, ?, '[]')
            """,
            (mention_id, f"fact-{local_id}", kind, value),
        )
    connection.commit()
    return run_id, mention_id


def test_match_key_strips_honorific_collapses_whitespace_and_casefolds() -> None:
    assert match_key("  Dr.   Ada   Lovelace  ") == "ada lovelace"
    assert match_key("SIR Isaac Newton") == "isaac newton"
    assert match_key("Madonna") == "madonna"
    assert collapse_whitespace("  a   b\tc  ") == "a b c"
    assert repo_match_key("Dr. Ada") == match_key("Dr. Ada")
    assert repo_collapse_whitespace("a  b") == collapse_whitespace("a  b")


def test_match_key_empty_after_whitespace_only() -> None:
    # mechanical_search_name returns original when strip leaves empty-ish input
    assert match_key("   ") == ""
    assert match_key("") == ""


def test_create_person_for_mention_sets_names_display_and_fingerprint(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("name", "A. Smith"), ("profession_or_role", "composer")),
    )
    with immediate(connection):
        person_id = create_person_for_mention(
            connection, run_id=run_id, mention_id=mention_id, now=moment()
        )

    person = connection.execute(
        """
        SELECT display_name, identity_fingerprint, merged_into_person_id
          FROM person WHERE id = ?
        """,
        (person_id,),
    ).fetchone()
    assert person["display_name"] == "Alex Smith"
    assert person["merged_into_person_id"] is None
    assert len(person["identity_fingerprint"]) == 64
    assert person["identity_fingerprint"] != "0" * 64

    names = {
        (row["exact_name"], row["kind"], row["match_key"])
        for row in connection.execute(
            "SELECT exact_name, kind, match_key FROM sourced_name WHERE person_id = ?",
            (person_id,),
        )
    }
    assert ("Alex Smith", "display", "alex smith") in names
    assert ("A. Smith", "alias", "a. smith") in names

    linked = connection.execute(
        "SELECT person_id FROM person_mention WHERE id = ?", (mention_id,)
    ).fetchone()
    assert linked["person_id"] == person_id

    expected = compute_identity_fingerprint(connection, person_id)
    assert person["identity_fingerprint"] == expected


def test_create_person_for_mention_uses_mononym_for_single_token(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(connection, exact_name="Madonna")
    with immediate(connection):
        person_id = create_person_for_mention(
            connection, run_id=run_id, mention_id=mention_id, now=moment()
        )
    kind = connection.execute(
        "SELECT kind FROM sourced_name WHERE person_id = ?", (person_id,)
    ).fetchone()["kind"]
    assert kind == "mononym"
    assert (
        connection.execute(
            "SELECT display_name FROM person WHERE id = ?", (person_id,)
        ).fetchone()["display_name"]
        == "Madonna"
    )


def test_namesake_people_coexist_with_same_exact_name(
    connection: sqlite3.Connection,
) -> None:
    run_id_a, mention_a = _seed_mention(connection, exact_name="Alex Smith")
    # second mention needs a separate graph; reuse helpers via another seed path
    run_id_b = insert_run(connection)
    with immediate(connection):
        person_a = create_person_for_mention(
            connection, run_id=run_id_a, mention_id=mention_a, now=moment()
        )
        person_b = insert_person(
            connection,
            run_id=run_id_b,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER_HASH,
            created_at=moment(),
        )
        upsert_sourced_name(
            connection,
            person_id=person_b,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
    rows = connection.execute(
        """
        SELECT person_id FROM sourced_name
         WHERE exact_name = 'Alex Smith' AND match_key = 'alex smith'
         ORDER BY person_id
        """
    ).fetchall()
    assert [int(r["person_id"]) for r in rows] == sorted([person_a, person_b])


def test_display_name_preference_order(connection: sqlite3.Connection) -> None:
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="temp",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Al",
            kind="mononym",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(10),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Alias",
            kind="alias",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(20),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Display",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(5),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Dr Alex Professional",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(1),
        )
        assert select_display_name(connection, person_id) == "Dr Alex Professional"

        # Within same kind: more tokens win.
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Professional Longer Name",
            kind="professional",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(0),
        )
        assert (
            select_display_name(connection, person_id)
            == "Alex Professional Longer Name"
        )


def test_display_name_tie_break_latest_then_lower_id(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="temp",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        first = upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Same Length A",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(1),
        )
        second = upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Same Length B",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(2),
        )
        assert first < second
        assert select_display_name(connection, person_id) == "Same Length B"

        # Equal last_observed_at → lower id.
        connection.execute(
            "UPDATE sourced_name SET last_observed_at = ? WHERE id IN (?, ?)",
            (moment(5), first, second),
        )
        assert select_display_name(connection, person_id) == "Same Length A"


def test_upsert_sourced_name_refreshes_last_observed_at(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        name_id = upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(1),
        )
        again = upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(9),
        )
        assert again == name_id
        row = connection.execute(
            "SELECT last_observed_at, first_observed_at FROM sourced_name WHERE id = ?",
            (name_id,),
        ).fetchone()
        assert row["first_observed_at"] == moment(1)
        assert row["last_observed_at"] == moment(9)


def test_canonical_projection_follows_merge_and_includes_loser_mentions(
    connection: sqlite3.Connection,
) -> None:
    """Fingerprint must use operational closure (survivor + merged-away).

    Discriminates the closure rule: if names/facts queries only use the
    canonical person id (not the loser), before/after hashes and the golden
    payload digests below stay equal or miss loser material.
    """
    import hashlib
    import json

    def _golden(names: list[str], facts: list[tuple[str, str]]) -> str:
        payload = {
            "facts": sorted(
                [{"kind": kind, "match_key": key} for kind, key in facts],
                key=lambda item: (item["kind"], item["match_key"]),
            ),
            "names": sorted(names),
        }
        encoded = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    run_id_a, mention_a = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("profession_or_role", "composer"),),
    )
    with immediate(connection):
        survivor = create_person_for_mention(
            connection, run_id=run_id_a, mention_id=mention_a, now=moment()
        )

    # Survivor-only projection: name + profession fact; no loser material.
    survivor_only = compute_identity_fingerprint(connection, survivor)
    assert survivor_only == _golden(
        ["alex smith"],
        [("profession_or_role", "composer")],
    )

    with immediate(connection):
        loser = insert_person(
            connection,
            run_id=run_id_a,
            display_name="Unique Loser Name",
            identity_fingerprint=_OTHER_HASH,
            created_at=moment(),
            merged_into_person_id=None,
        )
        upsert_sourced_name(
            connection,
            person_id=loser,
            exact_name="Unique Loser Name",
            kind="alias",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
        _run2, mention_b = _seed_second_mention(
            connection, run_id=run_id_a, exact_name="Unique Loser Name"
        )
        connection.execute(
            "UPDATE person_mention SET person_id = ? WHERE id = ?",
            (loser, mention_b),
        )
        # Redirect without yet attaching the loser's non-name fact.
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, loser),
        )

    assert canonical_person_id(connection, loser) == survivor
    assert canonical_person_id(connection, survivor) == survivor
    assert person_id_closure_for_canonical(connection, survivor) == tuple(
        sorted((survivor, loser))
    )
    mention_ids = {m.id for m in mentions_for_canonical_person(connection, survivor)}
    assert mention_a in mention_ids
    assert mention_b in mention_ids

    # Closure names query must pull the loser's sourced match_key.
    after_name_merge = compute_identity_fingerprint(connection, survivor)
    assert after_name_merge != survivor_only
    assert after_name_merge == _golden(
        ["alex smith", "unique loser name"],
        [("profession_or_role", "composer")],
    )

    with immediate(connection):
        connection.execute(
            """
            INSERT INTO mention_identity_fact (
                person_mention_id, local_id, kind, value,
                supporting_passage_ids_json
            ) VALUES (?, 'place-1', 'place', 'Paris', '[]')
            """,
            (mention_b,),
        )
        fingerprint = recompute_identity_fingerprint(connection, survivor)

    # Closure facts query must pull the non-name fact still stored on the loser.
    assert fingerprint != after_name_merge
    assert fingerprint != survivor_only
    assert fingerprint == _golden(
        ["alex smith", "unique loser name"],
        [
            ("place", "paris"),
            ("profession_or_role", "composer"),
        ],
    )
    assert fingerprint == compute_identity_fingerprint(connection, survivor)
    stored = connection.execute(
        "SELECT identity_fingerprint FROM person WHERE id = ?",
        (survivor,),
    ).fetchone()["identity_fingerprint"]
    assert stored == fingerprint


def _seed_second_mention(
    connection: sqlite3.Connection, *, run_id: int, exact_name: str
) -> tuple[int, int]:
    """Add another source item + mention under an existing run (open txn ok)."""
    provenance = connection.execute(
        """
        SELECT feed_identity_id, discovered_by_fetch_id
          FROM source_item
         ORDER BY id
         LIMIT 1
        """
    ).fetchone()
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'Second article', ?)
        """,
        (
            provenance["feed_identity_id"],
            provenance["discovered_by_fetch_id"],
            run_id,
            f"entry-{exact_name}",
            moment(50),
        ),
    ).lastrowid
    assert source_item_id is not None
    attempt = connection.execute(
        "SELECT id FROM attempt WHERE run_id = ? ORDER BY id LIMIT 1",
        (run_id,),
    ).fetchone()
    inspection = connection.execute(
        "SELECT id FROM model_inspection WHERE run_id = ? ORDER BY id LIMIT 1",
        (run_id,),
    ).fetchone()
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
            attempt["id"],
            inspection["id"],
            _HASH,
            _OTHER_HASH,
            _OTHER_HASH,
            moment(50),
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
        (observation_id, exact_name, mechanical_search_name(exact_name)),
    ).lastrowid
    assert mention_id is not None
    return run_id, mention_id


def test_person_writers_require_active_transaction(
    connection: sqlite3.Connection,
) -> None:
    run_id = insert_run(connection)
    with pytest.raises(RuntimeError, match="active transaction"):
        insert_person(
            connection,
            run_id=run_id,
            display_name="Alex",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
    run_id, mention_id = _seed_mention(connection)
    with pytest.raises(RuntimeError, match="active transaction"):
        create_person_for_mention(
            connection, run_id=run_id, mention_id=mention_id, now=moment()
        )
