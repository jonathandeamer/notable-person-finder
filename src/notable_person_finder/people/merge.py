"""Confirmed person merges and canonical work reconciliation.

Single application-thread transaction; no network. Survivor is always the lower
canonical ``person.id`` (K7). Historical mention and ER foreign keys are never
rewritten (K12). Digest-queue reconciliation is a named no-op until milestone 6
(K13).
"""

from __future__ import annotations

import sqlite3

from notable_person_finder.config.models import MainConfig
from notable_person_finder.people.identity import (
    canonical_person_id,
    recompute_identity_fingerprint,
    select_display_name,
    upsert_sourced_name,
)
from notable_person_finder.people.repository import (
    RECONSIDER_PERSON_ENTITY_TASK_TYPE,
    RESOLVE_PERSON_ENTITY_TASK_TYPE,
    _require_transaction,
    list_active_possible_same_person_for,
    upsert_active_possible_same_person,
)

MERGED_AWAY_REASON = "merged_away"
_SUBJECT_KIND_PERSON = "person"
_SUBJECT_KIND_PERSON_MENTION = "person_mention"
_SUBJECT_KIND_PERSON_RELATION = "person_relation"


def reconcile_digest_queue_on_merge(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    now: str,
) -> None:
    """No-op until milestone 6 creates ``digest_queue`` (K13)."""
    del connection, survivor_id, loser_id, now


def confirm_person_merge(
    connection: sqlite3.Connection,
    *,
    loser_id: int,
    survivor_id: int,
    run_id: int,
    observation_id: int | None,
    now: str,
    config: MainConfig | None = None,
) -> int:
    """Confirm a directed merge. Returns the canonical survivor id.

    **The caller must already hold an open transaction.**

    Resolves redirects first, then forces survivor = lower canonical id (K7).
    Idempotent when the loser is already merged-away into the same survivor (or
    both arguments already resolve to one person). Rejects self-links that would
    set ``merged_into_person_id = id`` and cycles that cannot be flattened to a
    single lower-id survivor.

    When ``config`` is provided, Wikipedia merge reconciliation (K16) runs after
    work-item reconcile. Production callers always pass config.
    """
    _require_transaction(connection, "confirm_person_merge")

    loser_row = _person_row(connection, loser_id)
    survivor_row = _person_row(connection, survivor_id)

    # Resolve existing redirects (one hop after flatten).
    resolved_loser = (
        int(loser_row["merged_into_person_id"])
        if loser_row["merged_into_person_id"] is not None
        else int(loser_row["id"])
    )
    resolved_survivor = (
        int(survivor_row["merged_into_person_id"])
        if survivor_row["merged_into_person_id"] is not None
        else int(survivor_row["id"])
    )

    # Idempotent: both already the same canonical person.
    if resolved_loser == resolved_survivor:
        return resolved_survivor

    # K7: survivor is the lower of the two *canonical* ids.
    actual_survivor = min(resolved_loser, resolved_survivor)
    actual_loser = max(resolved_loser, resolved_survivor)

    # Both endpoints must be canonical after resolve; reject if either still
    # points elsewhere (cycle / unflattened chain).
    for person_id in (actual_survivor, actual_loser):
        row = _person_row(connection, person_id)
        if row["merged_into_person_id"] is not None:
            raise ValueError(
                f"person {person_id} is not canonical after redirect resolve; "
                "refusing merge that would create a cycle"
            )
    if actual_survivor == actual_loser:
        raise ValueError("merge endpoints must differ after redirect resolve")

    # Already recorded merge relation for this directed pair → still ensure
    # redirect + projection, then return (idempotent re-merge).
    existing_merge = connection.execute(
        """
        SELECT id
          FROM person_relation
         WHERE kind = 'merge'
           AND person_id_a = ?
           AND person_id_b = ?
         ORDER BY id
         LIMIT 1
        """,
        (actual_loser, actual_survivor),
    ).fetchone()
    if existing_merge is None:
        connection.execute(
            """
            INSERT INTO person_relation (
                kind, person_id_a, person_id_b, status, created_at,
                created_by_run_id, created_by_observation_id
            ) VALUES (
                'merge', ?, ?, 'active', ?, ?, ?
            )
            """,
            (
                actual_loser,
                actual_survivor,
                now,
                run_id,
                observation_id,
            ),
        )

    # Point loser at survivor; reject self-link via CHECK + explicit guard.
    if actual_loser == actual_survivor:
        raise ValueError("refusing self-link merge")
    changed = connection.execute(
        """
        UPDATE person
           SET merged_into_person_id = ?
         WHERE id = ?
        """,
        (actual_survivor, actual_loser),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person {actual_loser} is missing; cannot set merge redirect"
        )

    # Flatten anyone pointing at loser so operational lookup is one hop.
    connection.execute(
        """
        UPDATE person
           SET merged_into_person_id = ?
         WHERE merged_into_person_id = ?
        """,
        (actual_survivor, actual_loser),
    )

    _upsert_loser_names_onto_survivor(
        connection,
        loser_id=actual_loser,
        survivor_id=actual_survivor,
        now=now,
    )

    display_name = select_display_name(connection, actual_survivor)
    fingerprint = recompute_identity_fingerprint(connection, actual_survivor)
    connection.execute(
        """
        UPDATE person
           SET display_name = ?, identity_fingerprint = ?
         WHERE id = ?
        """,
        (display_name, fingerprint, actual_survivor),
    )

    _reconcile_possible_same_person_edges(
        connection,
        loser_id=actual_loser,
        survivor_id=actual_survivor,
        run_id=run_id,
        observation_id=observation_id,
        now=now,
    )
    _reconcile_work_items(
        connection,
        loser_id=actual_loser,
        survivor_id=actual_survivor,
        run_id=run_id,
        now=now,
    )
    reconcile_digest_queue_on_merge(
        connection,
        survivor_id=actual_survivor,
        loser_id=actual_loser,
        now=now,
    )
    if config is not None:
        # Lazy import keeps people.merge free of wikipedia import cycles at
        # module load; production reconsider path always supplies config (K16).
        from notable_person_finder.wikipedia.merge_hooks import reconcile_on_merge

        reconcile_on_merge(
            connection,
            survivor_id=actual_survivor,
            loser_id=actual_loser,
            run_id=run_id,
            config=config,
            now=now,
        )
    return actual_survivor


def _person_row(connection: sqlite3.Connection, person_id: int) -> sqlite3.Row:
    row = connection.execute(
        "SELECT id, merged_into_person_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"person {person_id} does not exist")
    return row


def _upsert_loser_names_onto_survivor(
    connection: sqlite3.Connection,
    *,
    loser_id: int,
    survivor_id: int,
    now: str,
) -> None:
    """Copy loser sourced names onto the survivor (origin_kind=merge)."""
    rows = connection.execute(
        """
        SELECT exact_name, kind, origin_mention_id
          FROM sourced_name
         WHERE person_id = ?
         ORDER BY id
        """,
        (loser_id,),
    ).fetchall()
    for row in rows:
        upsert_sourced_name(
            connection,
            person_id=survivor_id,
            exact_name=str(row["exact_name"]),
            kind=str(row["kind"]),
            origin_kind="merge",
            origin_mention_id=(
                None
                if row["origin_mention_id"] is None
                else int(row["origin_mention_id"])
            ),
            observed_at=now,
        )


def _reconcile_possible_same_person_edges(
    connection: sqlite3.Connection,
    *,
    loser_id: int,
    survivor_id: int,
    run_id: int,
    observation_id: int | None,
    now: str,
) -> None:
    """Supersede loser's active edges; re-link peers to the survivor."""
    # Snapshot before mutations: list is evaluated eagerly.
    edges = list(list_active_possible_same_person_for(connection, loser_id))
    for edge in edges:
        peer_id = edge.person_id_b if edge.person_id_a == loser_id else edge.person_id_a
        _supersede_relation_row(
            connection,
            relation_id=edge.id,
            closed_by_observation_id=observation_id,
            closed_at=now,
        )
        peer_canonical = canonical_person_id(connection, peer_id)
        if peer_canonical == survivor_id:
            continue
        if observation_id is not None:
            upsert_active_possible_same_person(
                connection,
                person_id_a=survivor_id,
                person_id_b=peer_canonical,
                run_id=run_id,
                created_by_observation_id=observation_id,
                now=now,
            )
        else:
            _insert_active_possible_same_person_optional_observation(
                connection,
                person_id_a=survivor_id,
                person_id_b=peer_canonical,
                run_id=run_id,
                observation_id=None,
                now=now,
            )


def _supersede_relation_row(
    connection: sqlite3.Connection,
    *,
    relation_id: int,
    closed_by_observation_id: int | None,
    closed_at: str,
) -> None:
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


def _insert_active_possible_same_person_optional_observation(
    connection: sqlite3.Connection,
    *,
    person_id_a: int,
    person_id_b: int,
    run_id: int,
    observation_id: int | None,
    now: str,
) -> int:
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
        (low, high, now, run_id, observation_id),
    )
    row_id = cursor.lastrowid
    if row_id is None:
        raise RuntimeError("INSERT person_relation did not produce a row id")
    return int(row_id)


def _reconcile_work_items(
    connection: sqlite3.Connection,
    *,
    loser_id: int,
    survivor_id: int,
    run_id: int,
    now: str,
) -> None:
    """Supersede active work that still names the loser (person / relation / mention).

    Joins the caller's open transaction — does not open nested BEGIN.
    """
    del survivor_id  # replacement scheduling is future work when needed

    # 1. Person-subject work on the loser.
    connection.execute(
        """
        UPDATE work_item
           SET state = 'superseded', reason = ?, completed_by_run_id = ?,
               updated_at = ?
         WHERE subject_kind = ?
           AND subject_id = ?
           AND state IN ('pending', 'deferred')
        """,
        (MERGED_AWAY_REASON, run_id, now, _SUBJECT_KIND_PERSON, loser_id),
    )

    # 2. Relation-scoped reconsider work for any relation that involves loser.
    relation_ids = [
        int(row["id"])
        for row in connection.execute(
            """
            SELECT id
              FROM person_relation
             WHERE person_id_a = ? OR person_id_b = ?
            """,
            (loser_id, loser_id),
        )
    ]
    if relation_ids:
        placeholders = ",".join("?" for _ in relation_ids)
        connection.execute(
            f"""
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id IN ({placeholders})
               AND state IN ('pending', 'deferred')
            """,
            (
                MERGED_AWAY_REASON,
                run_id,
                now,
                RECONSIDER_PERSON_ENTITY_TASK_TYPE,
                _SUBJECT_KIND_PERSON_RELATION,
                *relation_ids,
            ),
        )

    # 3. Belt-and-braces: pending resolve on mentions still FK'd to the loser.
    mention_ids = [
        int(row["id"])
        for row in connection.execute(
            "SELECT id FROM person_mention WHERE person_id = ?",
            (loser_id,),
        )
    ]
    if mention_ids:
        placeholders = ",".join("?" for _ in mention_ids)
        connection.execute(
            f"""
            UPDATE work_item
               SET state = 'superseded', reason = ?, completed_by_run_id = ?,
                   updated_at = ?
             WHERE task_type = ?
               AND subject_kind = ?
               AND subject_id IN ({placeholders})
               AND state IN ('pending', 'deferred')
            """,
            (
                MERGED_AWAY_REASON,
                run_id,
                now,
                RESOLVE_PERSON_ENTITY_TASK_TYPE,
                _SUBJECT_KIND_PERSON_MENTION,
                *mention_ids,
            ),
        )
