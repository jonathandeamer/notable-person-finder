"""Durable person identity helpers: names, projection, and display selection.

Person and sourced-name writers are transaction-neutral: they join the caller's
open transaction so domain rows and settlement commit or roll back together.

``match_key`` is owned here (single definition) and re-exported from
``people.repository`` next to ``mechanical_search_name``.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Sequence

from notable_person_finder.people.repository import (
    PersonMentionRecord,
    _last_row_id,
    _require_transaction,
    mechanical_search_name,
)

_KIND_PREFERENCE: dict[str, int] = {
    "professional": 0,
    "display": 1,
    "alias": 2,
    "other": 3,
    "mononym": 4,
}

_PROVISIONAL_FINGERPRINT = "0" * 64


def collapse_whitespace(value: str) -> str:
    """Collapse runs of whitespace to a single space and strip ends."""
    return " ".join(value.split())


def match_key(value: str) -> str:
    """Retrieval key: honorific-strip → collapse whitespace → casefold."""
    return collapse_whitespace(mechanical_search_name(value)).casefold()


def canonical_person_id(connection: sqlite3.Connection, person_id: int) -> int:
    """Follow one merge hop: return ``merged_into_person_id`` when set.

    After merge flattening, losers point directly at the canonical survivor.
    """
    row = connection.execute(
        "SELECT id, merged_into_person_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"person {person_id} does not exist")
    merged_into = row["merged_into_person_id"]
    if merged_into is None:
        return int(row["id"])
    return int(merged_into)


def person_id_closure_for_canonical(
    connection: sqlite3.Connection, canonical_id: int
) -> tuple[int, ...]:
    """Canonical id plus every person with ``merged_into_person_id = canonical_id``."""
    rows = connection.execute(
        """
        SELECT id
          FROM person
         WHERE id = ? OR merged_into_person_id = ?
         ORDER BY id
        """,
        (canonical_id, canonical_id),
    ).fetchall()
    return tuple(int(row["id"]) for row in rows)


def mentions_for_canonical_person(
    connection: sqlite3.Connection, canonical_id: int
) -> tuple[PersonMentionRecord, ...]:
    """Mentions whose stored ``person_id`` is in the operational closure of P."""
    closure = person_id_closure_for_canonical(connection, canonical_id)
    if not closure:
        return ()
    placeholders = ",".join("?" for _ in closure)
    mention_rows = connection.execute(
        f"""
        SELECT *
          FROM person_mention
         WHERE person_id IN ({placeholders})
         ORDER BY id
        """,
        closure,
    ).fetchall()
    if not mention_rows:
        return ()
    return _person_mention_records(connection, mention_rows)


def select_display_name(connection: sqlite3.Connection, person_id: int) -> str:
    """Pick display name by kind preference, token length, recency, then lower id."""
    rows = connection.execute(
        """
        SELECT id, exact_name, kind, last_observed_at
          FROM sourced_name
         WHERE person_id = ?
        """,
        (person_id,),
    ).fetchall()
    if not rows:
        raise LookupError(f"person {person_id} has no sourced names")
    best = max(
        rows,
        key=lambda row: (
            -_KIND_PREFERENCE.get(row["kind"], 99),
            _token_count(row["exact_name"]),
            row["last_observed_at"],
            -int(row["id"]),
        ),
    )
    return str(best["exact_name"])


def recompute_identity_fingerprint(
    connection: sqlite3.Connection, person_id: int
) -> str:
    """Recompute and persist identity fingerprint for the canonical projection.

    Uses operational projection (K12): names on the closure and non-name facts
    from mentions linked to any id in the closure.
    """
    _require_transaction(connection, "recompute_identity_fingerprint")
    canonical_id = canonical_person_id(connection, person_id)
    fingerprint = compute_identity_fingerprint(connection, canonical_id)
    changed = connection.execute(
        """
        UPDATE person
           SET identity_fingerprint = ?
         WHERE id = ?
        """,
        (fingerprint, canonical_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person {canonical_id} is missing; cannot update identity fingerprint"
        )
    return fingerprint


def compute_identity_fingerprint(
    connection: sqlite3.Connection, canonical_id: int
) -> str:
    """Compute fingerprint without writing (for provisional create paths)."""
    closure = person_id_closure_for_canonical(connection, canonical_id)
    if not closure:
        raise LookupError(f"canonical person {canonical_id} has empty closure")
    placeholders = ",".join("?" for _ in closure)

    name_keys = {
        str(row["match_key"])
        for row in connection.execute(
            f"""
            SELECT match_key
              FROM sourced_name
             WHERE person_id IN ({placeholders})
            """,
            closure,
        )
        if row["match_key"]
    }

    fact_pairs: set[tuple[str, str]] = set()
    for row in connection.execute(
        f"""
        SELECT f.kind, f.value
          FROM person_mention AS m
          JOIN mention_identity_fact AS f ON f.person_mention_id = m.id
         WHERE m.person_id IN ({placeholders})
        """,
        closure,
    ):
        kind = str(row["kind"])
        key = match_key(str(row["value"]))
        if not key:
            continue
        if kind == "name" and key in name_keys:
            continue
        fact_pairs.add((kind, key))

    payload = {
        "names": sorted(name_keys),
        "facts": sorted(
            [{"kind": kind, "match_key": key} for kind, key in fact_pairs],
            key=lambda item: (item["kind"], item["match_key"]),
        ),
    }
    return _sha256(_canonical_json(payload))


def insert_person(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    display_name: str,
    identity_fingerprint: str,
    created_at: str,
    merged_into_person_id: int | None = None,
) -> int:
    """Insert one durable person row.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "insert_person")
    cursor = connection.execute(
        """
        INSERT INTO person (
            created_at, created_by_run_id, display_name, identity_fingerprint,
            merged_into_person_id
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            created_at,
            run_id,
            display_name,
            identity_fingerprint,
            merged_into_person_id,
        ),
    )
    return _last_row_id(cursor)


def upsert_sourced_name(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    exact_name: str,
    kind: str,
    origin_kind: str,
    origin_mention_id: int | None,
    observed_at: str,
) -> int:
    """Insert a sourced name or refresh ``last_observed_at`` for the same exact name.

    Upsert identity is ``(person_id, exact_name)``. Namesakes across people are
    unrestricted. Empty ``match_key`` after normalize is rejected.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "upsert_sourced_name")
    stripped = exact_name.strip()
    if not stripped:
        raise ValueError("exact_name must be non-empty")
    search = mechanical_search_name(stripped)
    key = match_key(stripped)
    if not key:
        raise ValueError("match_key must be non-empty after normalize")

    existing = connection.execute(
        """
        SELECT id FROM sourced_name
         WHERE person_id = ? AND exact_name = ?
         ORDER BY id
         LIMIT 1
        """,
        (person_id, stripped),
    ).fetchone()
    if existing is not None:
        name_id = int(existing["id"])
        connection.execute(
            """
            UPDATE sourced_name
               SET last_observed_at = ?,
                   search_name = ?,
                   match_key = ?,
                   kind = ?
             WHERE id = ?
            """,
            (observed_at, search, key, kind, name_id),
        )
        return name_id

    cursor = connection.execute(
        """
        INSERT INTO sourced_name (
            person_id, exact_name, search_name, match_key, kind,
            origin_kind, origin_mention_id, first_observed_at, last_observed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            person_id,
            stripped,
            search,
            key,
            kind,
            origin_kind,
            origin_mention_id,
            observed_at,
            observed_at,
        ),
    )
    return _last_row_id(cursor)


def upsert_sourced_names_for_mention(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    mention_id: int,
    exact_name: str,
    identity_fact_names: Sequence[str] = (),
    observed_at: str,
) -> None:
    """Upsert primary exact name and distinct name-kind fact values for a mention.

    Primary kind: single token → mononym; else display. Distinct extra name
    strings become aliases.

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "upsert_sourced_names_for_mention")
    primary_kind = _primary_name_kind(exact_name)
    upsert_sourced_name(
        connection,
        person_id=person_id,
        exact_name=exact_name,
        kind=primary_kind,
        origin_kind="person_mention",
        origin_mention_id=mention_id,
        observed_at=observed_at,
    )
    primary_key = match_key(exact_name)
    seen_keys = {primary_key} if primary_key else set()
    for value in identity_fact_names:
        key = match_key(value)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        upsert_sourced_name(
            connection,
            person_id=person_id,
            exact_name=value,
            kind="alias",
            origin_kind="person_mention",
            origin_mention_id=mention_id,
            observed_at=observed_at,
        )


def create_person_for_mention(
    connection: sqlite3.Connection,
    *,
    run_id: int,
    mention_id: int,
    now: str,
) -> int:
    """Create a durable person from a mention, upsert names, set display + fingerprint.

    Points ``person_mention.person_id`` at the new person. Does not write an
    entity-resolution observation (callers own that settlement).

    **The caller must already hold an open transaction.**
    """
    _require_transaction(connection, "create_person_for_mention")
    mention = connection.execute(
        """
        SELECT id, exact_name, person_id
          FROM person_mention
         WHERE id = ?
        """,
        (mention_id,),
    ).fetchone()
    if mention is None:
        raise LookupError(f"person_mention {mention_id} does not exist")
    if mention["person_id"] is not None:
        raise ValueError(
            f"person_mention {mention_id} is already linked to person "
            f"{int(mention['person_id'])}"
        )

    exact_name = str(mention["exact_name"])
    fact_names = tuple(
        str(row["value"])
        for row in connection.execute(
            """
            SELECT value
              FROM mention_identity_fact
             WHERE person_mention_id = ? AND kind = 'name'
             ORDER BY local_id
            """,
            (mention_id,),
        )
    )

    person_id = insert_person(
        connection,
        run_id=run_id,
        display_name=exact_name.strip() or exact_name,
        identity_fingerprint=_PROVISIONAL_FINGERPRINT,
        created_at=now,
    )
    upsert_sourced_names_for_mention(
        connection,
        person_id=person_id,
        mention_id=mention_id,
        exact_name=exact_name,
        identity_fact_names=fact_names,
        observed_at=now,
    )
    changed = connection.execute(
        """
        UPDATE person_mention
           SET person_id = ?
         WHERE id = ?
        """,
        (person_id, mention_id),
    ).rowcount
    if changed != 1:
        raise RuntimeError(
            f"person_mention {mention_id} is missing; cannot link person"
        )

    display_name = select_display_name(connection, person_id)
    fingerprint = recompute_identity_fingerprint(connection, person_id)
    connection.execute(
        """
        UPDATE person
           SET display_name = ?, identity_fingerprint = ?
         WHERE id = ?
        """,
        (display_name, fingerprint, person_id),
    )
    return person_id


def _primary_name_kind(exact_name: str) -> str:
    tokens = exact_name.split()
    if len(tokens) <= 1:
        return "mononym"
    return "display"


def _token_count(exact_name: str) -> int:
    return len(exact_name.split())


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _person_mention_records(
    connection: sqlite3.Connection, mention_rows: Sequence[sqlite3.Row]
) -> tuple[PersonMentionRecord, ...]:
    from notable_person_finder.people.repository import (
        MentionIdentityFactRecord,
        MentionSignalRecord,
    )

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
