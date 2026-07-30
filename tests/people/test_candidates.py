"""Bounded name-gated candidate retrieval and K17 peer-scan helpers."""

from __future__ import annotations

import sqlite3

import pytest

from notable_person_finder.people.candidates import (
    peer_edge_eligible,
    retrieve_candidates,
    scan_name_matched_peers,
)
from notable_person_finder.people.identity import (
    create_person_for_mention,
    insert_person,
    upsert_sourced_name,
)
from notable_person_finder.people.repository import mechanical_search_name
from tests.ingestion.helpers import immediate, insert_run, moment

_HASH = "a" * 64
_OTHER_HASH = "b" * 64
_THIRD_HASH = "c" * 64


_seed_counter = 0


def _seed_mention(
    connection: sqlite3.Connection,
    *,
    exact_name: str = "Alex Smith",
    outcome: str = "research",
    fact_values: tuple[tuple[str, str], ...] = (),
    person_id: int | None = None,
) -> tuple[int, int]:
    """Return (run_id, mention_id) with a completed triage observation."""
    global _seed_counter
    _seed_counter += 1
    suffix = f"{_seed_counter}"
    # Distinct work fingerprints avoid UNIQUE collisions across seeds.
    work_fp = (f"{_seed_counter:064d}")[-64:]
    task_fp = (f"{_seed_counter + 1000:064d}")[-64:]

    run_id = insert_run(connection)
    work = connection.execute(
        """
        INSERT INTO work_item (
            task_type, subject_kind, subject_id, fingerprint, required,
            priority, eligible_at, state, created_by_run_id, created_at, updated_at
        )
        VALUES ('detect_people', 'source_item', 1, ?, 1, 30, ?, 'running', ?, ?, ?)
        """,
        (work_fp, moment(), run_id, moment(), moment()),
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
        (run_id, work, moment(), moment(1), work_fp),
    ).lastrowid
    assert attempt_id is not None
    feed = connection.execute(
        """
        INSERT INTO feed_identity (
            key, current_label, current_url, first_seen_at, last_seen_at
        ) VALUES (?, 'Feed A', ?, ?, ?)
        """,
        (f"feed-{suffix}", f"https://example.com/feed-{suffix}", moment(), moment()),
    ).lastrowid
    assert feed is not None
    fetch = connection.execute(
        """
        INSERT INTO feed_fetch (
            feed_identity_id, run_id, requested_at, requested_url, outcome
        ) VALUES (?, ?, ?, ?, 'modified')
        """,
        (feed, run_id, moment(), f"https://example.com/feed-{suffix}"),
    ).lastrowid
    assert fetch is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'Alex Smith wins award', ?)
        """,
        (feed, fetch, run_id, f"entry-{suffix}", moment()),
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
        (run_id, attempt_id, work_fp, moment()),
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
            task_fp,
            moment(),
        ),
    ).lastrowid
    assert observation_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale, person_id
        ) VALUES (?, 1, ?, ?, ?, '[]', 'reason', ?)
        """,
        (
            observation_id,
            exact_name,
            mechanical_search_name(exact_name),
            outcome,
            person_id,
        ),
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


def _add_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    exact_name: str,
    outcome: str = "research",
    fact_values: tuple[tuple[str, str], ...] = (),
    person_id: int | None = None,
    entry_suffix: str = "b",
) -> int:
    """Add another source item + mention under an existing run (open txn ok)."""
    global _seed_counter
    _seed_counter += 1
    task_fp = (f"{_seed_counter + 5000:064d}")[-64:]
    provenance = connection.execute(
        """
        SELECT feed_identity_id, discovered_by_fetch_id
          FROM source_item
         ORDER BY id
         LIMIT 1
        """
    ).fetchone()
    assert provenance is not None
    source_item_id = connection.execute(
        """
        INSERT INTO source_item (
            feed_identity_id, discovered_by_fetch_id, discovered_by_run_id,
            source_entry_id, title_text, discovered_at
        ) VALUES (?, ?, ?, ?, 'Another article', ?)
        """,
        (
            provenance["feed_identity_id"],
            provenance["discovered_by_fetch_id"],
            run_id,
            f"entry-{entry_suffix}-{_seed_counter}",
            moment(),
        ),
    ).lastrowid
    assert source_item_id is not None
    attempt = connection.execute(
        """
        SELECT id FROM attempt WHERE run_id = ? ORDER BY id LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    assert attempt is not None
    inspection = connection.execute(
        """
        SELECT id FROM model_inspection WHERE run_id = ? ORDER BY id LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    assert inspection is not None
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
            int(attempt["id"]),
            int(inspection["id"]),
            _HASH,
            _OTHER_HASH,
            task_fp,
            moment(),
        ),
    ).lastrowid
    assert observation_id is not None
    mention_id = connection.execute(
        """
        INSERT INTO person_mention (
            triage_observation_id, ordinal, exact_name, search_name,
            outcome, supporting_passage_ids_json, rationale, person_id
        ) VALUES (?, 1, ?, ?, ?, '[]', 'reason', ?)
        """,
        (
            observation_id,
            exact_name,
            mechanical_search_name(exact_name),
            outcome,
            person_id,
        ),
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
    return int(mention_id)


def _insert_named_person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    exact_name: str,
    kind: str = "display",
    fingerprint: str = _HASH,
    aliases: tuple[str, ...] = (),
) -> int:
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name=exact_name,
            identity_fingerprint=fingerprint,
            created_at=moment(),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name=exact_name,
            kind=kind,
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
        for alias in aliases:
            upsert_sourced_name(
                connection,
                person_id=person_id,
                exact_name=alias,
                kind="alias",
                origin_kind="manual",
                origin_mention_id=None,
                observed_at=moment(),
            )
    return person_id


def test_name_gate_admits_exact_and_match_key_hits(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(connection, exact_name="Alex Smith")
    exact_hit = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    # Different casing / honorific still shares match_key with the mention.
    key_hit = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="Dr. Alex Smith",
        fingerprint=_OTHER_HASH,
    )
    # Unrelated name is outside the gate.
    _insert_named_person(
        connection, run_id=run_id, exact_name="Jamie Lee", fingerprint=_THIRD_HASH
    )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    ids = {c.person_id for c in candidates}
    assert exact_hit in ids
    assert key_hit in ids
    assert len(candidates) == 2


def test_name_fact_on_mention_opens_gate(connection: sqlite3.Connection) -> None:
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="A. Smith",
        fact_values=(("name", "Alexandra Smith"),),
    )
    person_id = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="Alexandra Smith",
        fingerprint=_HASH,
    )
    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert [c.person_id for c in candidates] == [person_id]


def test_no_fact_only_admission(connection: sqlite3.Connection) -> None:
    """Non-name fact overlap alone never admits a candidate (K10)."""
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("profession_or_role", "composer"), ("place", "Paris")),
    )
    # Person shares non-name facts but has a completely different name.
    with immediate(connection):
        person_id = insert_person(
            connection,
            run_id=run_id,
            display_name="Jamie Lee",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name="Jamie Lee",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
        linked = _add_mention(
            connection,
            run_id=run_id,
            exact_name="Jamie Lee",
            fact_values=(
                ("profession_or_role", "composer"),
                ("place", "Paris"),
            ),
            person_id=person_id,
            entry_suffix="fact-only",
        )
        assert linked

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert candidates == ()


def test_scoring_order_exact_over_match_key_over_alias(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(connection, exact_name="Alex Smith")
    # Alias-only hit on a name-fact key would score lower; use primary alias path.
    alias_only = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="A. Smith",
        fingerprint=_HASH,
        aliases=("Alexander Smith",),
    )
    # Primary match_key (case/honorific) without exact string equality.
    match_key_hit = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="ALEX SMITH",
        fingerprint=_OTHER_HASH,
    )
    # Exact exact_name equality is the strongest name signal.
    exact_hit = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="Alex Smith",
        fingerprint=_THIRD_HASH,
    )

    # Give alias_only a query-name hit via match_key of an alternate form.
    # "Alexander Smith" is not in the query set yet — attach a name fact to query.
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value,
            supporting_passage_ids_json
        ) VALUES (?, 'name-alt', 'name', 'Alexander Smith', '[]')
        """,
        (mention_id,),
    )
    connection.commit()

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    scores = {c.person_id: c.score for c in candidates}
    assert scores[exact_hit] > scores[match_key_hit] > scores[alias_only]
    assert [c.person_id for c in candidates] == [
        exact_hit,
        match_key_hit,
        alias_only,
    ]


def test_non_name_fact_overlap_and_prior_research_boost(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("profession_or_role", "composer"), ("place", "Paris")),
    )
    plain = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    boosted = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_OTHER_HASH
    )
    with immediate(connection):
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            outcome="research",
            fact_values=(
                ("profession_or_role", "composer"),
                ("place", "Paris"),
            ),
            person_id=boosted,
            entry_suffix="boost",
        )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    by_id = {c.person_id: c for c in candidates}
    # plain: exact +100, primary match_key +80 = 180
    # boosted: same + fact overlap (+10*2 capped) + prior research +5
    assert by_id[boosted].score > by_id[plain].score
    assert by_id[boosted].score - by_id[plain].score == 25  # 20 fact + 5 research
    assert candidates[0].person_id == boosted


def test_fact_overlap_capped_at_forty(connection: sqlite3.Connection) -> None:
    facts = tuple(
        (kind, f"value-{i}")
        for i, kind in enumerate(
            (
                "profession_or_role",
                "place",
                "nationality",
                "era_or_date",
                "work",
                "affiliation",
            ),
            start=1,
        )
    )
    run_id, mention_id = _seed_mention(
        connection, exact_name="Alex Smith", fact_values=facts
    )
    person_id = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    with immediate(connection):
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            outcome="uncertain",  # not research — no +5
            fact_values=facts,
            person_id=person_id,
            entry_suffix="cap",
        )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert len(candidates) == 1
    # 100 exact + 80 primary + min(6*10, 40) fact = 220; no research boost
    assert candidates[0].score == 220


def test_no_score_floor_keeps_weak_name_hit(
    connection: sqlite3.Connection,
) -> None:
    """Name-gated hits stay eligible even when score is only the weak alias band."""
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="A. Smith",
        fact_values=(("name", "Alexander Smith"),),
    )
    person_id = _insert_named_person(
        connection,
        run_id=run_id,
        exact_name="Alexander Smith",
        fingerprint=_HASH,
    )
    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert len(candidates) == 1
    assert candidates[0].person_id == person_id
    # Alias / other query match_key only: +60, no exact, no primary
    assert candidates[0].score == 60


def test_max_candidates_bound_and_stable_tie_break(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(connection, exact_name="Alex Smith")
    ids: list[int] = []
    for fingerprint in (_HASH, _OTHER_HASH, _THIRD_HASH, "d" * 64):
        ids.append(
            _insert_named_person(
                connection,
                run_id=run_id,
                exact_name="Alex Smith",
                fingerprint=fingerprint,
            )
        )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=2,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert len(candidates) == 2
    # Same score for all exact hits → person_id ASC
    assert [c.person_id for c in candidates] == sorted(ids)[:2]
    assert candidates[0].score == candidates[1].score


def test_merged_away_person_excluded(connection: sqlite3.Connection) -> None:
    run_id, mention_id = _seed_mention(connection, exact_name="Alex Smith")
    survivor = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    loser = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_OTHER_HASH
    )
    with immediate(connection):
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, loser),
        )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=mention_id,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert [c.person_id for c in candidates] == [survivor]


def test_post_merge_projection_scoring_includes_loser_mention_facts(
    connection: sqlite3.Connection,
) -> None:
    """After merge, survivor scores using facts still FK'd to the loser (K12)."""
    run_id, query_mention = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("profession_or_role", "composer"),),
    )
    with immediate(connection):
        survivor = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_HASH,
            created_at=moment(),
        )
        upsert_sourced_name(
            connection,
            person_id=survivor,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
        loser = insert_person(
            connection,
            run_id=run_id,
            display_name="Alex Smith",
            identity_fingerprint=_OTHER_HASH,
            created_at=moment(),
            merged_into_person_id=None,
        )
        # Merge upserts loser's names onto survivor for name gate continuity.
        upsert_sourced_name(
            connection,
            person_id=loser,
            exact_name="Alex Smith",
            kind="display",
            origin_kind="manual",
            origin_mention_id=None,
            observed_at=moment(),
        )
        loser_mention = _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            outcome="research",
            fact_values=(("profession_or_role", "composer"),),
            person_id=loser,
            entry_suffix="loser",
        )
        assert loser_mention
        connection.execute(
            "UPDATE person SET merged_into_person_id = ? WHERE id = ?",
            (survivor, loser),
        )

    candidates = retrieve_candidates(
        connection,
        person_mention_id=query_mention,
        max_candidates=8,
        max_facts_per_candidate=12,
        max_names_per_candidate=8,
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.person_id == survivor
    # exact +100, primary +80, fact +10, research +5
    assert candidate.score == 195
    fact_values = {f.value for f in candidate.facts}
    assert "composer" in fact_values
    assert all(f.local_id.startswith(f"c{survivor}-f") for f in candidate.facts)


def test_candidate_payload_bounds_names_and_facts(
    connection: sqlite3.Connection,
) -> None:
    run_id, mention_id = _seed_mention(
        connection,
        exact_name="Alex Smith",
        fact_values=(("profession_or_role", "composer"),),
    )
    with immediate(connection):
        person_id = create_person_for_mention(
            connection, run_id=run_id, mention_id=mention_id, now=moment()
        )
        for i in range(5):
            upsert_sourced_name(
                connection,
                person_id=person_id,
                exact_name=f"Alias {i}",
                kind="alias",
                origin_kind="manual",
                origin_mention_id=None,
                observed_at=moment(i),
            )
        # Extra non-name facts on a second linked mention.
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            fact_values=tuple(("place", f"City{i}") for i in range(6)),
            person_id=person_id,
            entry_suffix="facts",
        )

    # Query a fresh unresolved mention (not the one already linked).
    _run2, query_mention = _seed_mention(connection, exact_name="Alex Smith")
    candidates = retrieve_candidates(
        connection,
        person_mention_id=query_mention,
        max_candidates=8,
        max_facts_per_candidate=3,
        max_names_per_candidate=2,
    )
    assert len(candidates) == 1
    assert len(candidates[0].names) == 2
    assert len(candidates[0].facts) == 3
    assert candidates[0].display_name
    for index, fact in enumerate(candidates[0].facts, start=1):
        assert fact.local_id == f"c{candidates[0].person_id}-f{index}"


def test_scan_name_matched_peers_excludes_self_and_reject_set(
    connection: sqlite3.Connection,
) -> None:
    run_id, _bootstrap = _seed_mention(connection, exact_name="Bootstrap")
    self_id = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    peer_a = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_OTHER_HASH
    )
    peer_b = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_THIRD_HASH
    )
    # Non-name facts on peers make K17 eligibility pass against name-only self.
    with immediate(connection):
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            fact_values=(("profession_or_role", "composer"),),
            person_id=peer_a,
            entry_suffix="peer-a",
        )
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            fact_values=(("place", "London"),),
            person_id=peer_b,
            entry_suffix="peer-b",
        )

    peers = scan_name_matched_peers(
        connection,
        person_id=self_id,
        reject_set=frozenset({peer_b}),
        max_candidates=8,
        creating_mention_id=None,
    )
    assert self_id not in peers
    assert peer_b not in peers
    assert peers == (peer_a,)


def test_peer_edge_requires_non_name_on_at_least_one_side(
    connection: sqlite3.Connection,
) -> None:
    run_id, creating = _seed_mention(connection, exact_name="Alex Smith")
    name_only_a = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_HASH
    )
    name_only_b = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_OTHER_HASH
    )
    with_fact = _insert_named_person(
        connection, run_id=run_id, exact_name="Alex Smith", fingerprint=_THIRD_HASH
    )
    with immediate(connection):
        _add_mention(
            connection,
            run_id=run_id,
            exact_name="Alex Smith",
            fact_values=(("profession_or_role", "composer"),),
            person_id=with_fact,
            entry_suffix="with-fact",
        )

    assert (
        peer_edge_eligible(
            connection,
            person_id=name_only_a,
            peer_id=name_only_b,
            creating_mention_id=None,
        )
        is False
    )
    assert (
        peer_edge_eligible(
            connection,
            person_id=name_only_a,
            peer_id=with_fact,
            creating_mention_id=None,
        )
        is True
    )

    # Creating mention supplies the non-name fact even when both people are name-only.
    connection.execute(
        """
        INSERT INTO mention_identity_fact (
            person_mention_id, local_id, kind, value,
            supporting_passage_ids_json
        ) VALUES (?, 'role', 'profession_or_role', 'poet', '[]')
        """,
        (creating,),
    )
    connection.commit()
    assert (
        peer_edge_eligible(
            connection,
            person_id=name_only_a,
            peer_id=name_only_b,
            creating_mention_id=creating,
        )
        is True
    )

    peers = scan_name_matched_peers(
        connection,
        person_id=name_only_a,
        reject_set=frozenset(),
        max_candidates=8,
        creating_mention_id=None,
    )
    # Without creating mention, only with_fact is eligible.
    assert peers == (with_fact,)


def test_retrieve_candidates_unknown_mention_raises(
    connection: sqlite3.Connection,
) -> None:
    with pytest.raises(LookupError, match="person_mention"):
        retrieve_candidates(
            connection,
            person_mention_id=999_999,
            max_candidates=8,
            max_facts_per_candidate=12,
            max_names_per_candidate=8,
        )
