"""Bounded name-gated candidate retrieval and K17 peer-scan helpers.

Candidate admission is name-gated only (K10): exact name or match_key hit on
canonical people. Scoring ranks within that set; there is no score floor that
drops a name hit. Candidate facts use the operational projection (K12) so
post-merge loser mention facts still score and appear.

Peer scan lists name-matched canonical peers excluding self and a reject set,
and keeps a peer only when K17 non-name eligibility holds on at least one side.
Edge persistence is left to later service wiring.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence, Set
from dataclasses import dataclass

from notable_person_finder.people.identity import (
    match_key,
    mentions_for_canonical_person,
)


@dataclass(frozen=True, slots=True)
class SourcedNameView:
    """Bounded sourced-name slice for a candidate payload."""

    exact_name: str
    search_name: str
    match_key: str
    kind: str


@dataclass(frozen=True, slots=True)
class CandidateFactView:
    """Operational-projection identity fact with candidate-local id ``c{p}-f{n}``."""

    local_id: str
    kind: str
    value: str


@dataclass(frozen=True, slots=True)
class CandidatePerson:
    person_id: int
    display_name: str
    score: int
    names: tuple[SourcedNameView, ...]
    facts: tuple[CandidateFactView, ...]


def retrieve_candidates(
    connection: sqlite3.Connection,
    *,
    person_mention_id: int,
    max_candidates: int,
    max_facts_per_candidate: int,
    max_names_per_candidate: int,
) -> tuple[CandidatePerson, ...]:
    """Return top name-gated candidates for a mention, score-ordered.

    Application-thread reads only. ``max_candidates`` bounds the returned set;
    scoring never drops a name-gated hit below admission.
    """
    if max_candidates < 1:
        raise ValueError("max_candidates must be >= 1")
    if max_facts_per_candidate < 1:
        raise ValueError("max_facts_per_candidate must be >= 1")
    if max_names_per_candidate < 1:
        raise ValueError("max_names_per_candidate must be >= 1")

    mention = _load_mention_query(connection, person_mention_id)
    exact_names, keys = _query_name_sets(mention)
    if not exact_names and not keys:
        return ()

    person_ids = _name_gated_canonical_person_ids(
        connection, exact_names=exact_names, keys=keys
    )
    if not person_ids:
        return ()

    primary_key = match_key(mention.search_name) or match_key(mention.exact_name)
    query_non_name_keys = _non_name_fact_keys(mention.identity_facts)

    scored: list[tuple[int, int]] = []
    for person_id in person_ids:
        score = _score_person(
            connection,
            person_id=person_id,
            mention_exact_name=mention.exact_name,
            primary_match_key=primary_key,
            query_keys=keys,
            query_non_name_keys=query_non_name_keys,
        )
        scored.append((score, person_id))

    scored.sort(key=lambda item: (-item[0], item[1]))
    retained = scored[:max_candidates]

    return tuple(
        _build_candidate(
            connection,
            person_id=person_id,
            score=score,
            max_facts=max_facts_per_candidate,
            max_names=max_names_per_candidate,
        )
        for score, person_id in retained
    )


def scan_name_matched_peers(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    reject_set: Set[int],
    max_candidates: int,
    creating_mention_id: int | None = None,
) -> tuple[int, ...]:
    """Name-gated canonical peers eligible for K17 peer edges.

    Excludes ``person_id`` and every id in ``reject_set``. A peer is retained
    only when ``peer_edge_eligible`` is true. Ordered by descending name score
    then ``person_id ASC``, capped at ``max_candidates``.
    """
    if max_candidates < 1:
        raise ValueError("max_candidates must be >= 1")

    canonical_id = person_id
    row = connection.execute(
        "SELECT id, merged_into_person_id, display_name FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"person {person_id} does not exist")
    if row["merged_into_person_id"] is not None:
        canonical_id = int(row["merged_into_person_id"])

    name_rows = connection.execute(
        """
        SELECT exact_name, match_key
          FROM sourced_name
         WHERE person_id = ?
        """,
        (canonical_id,),
    ).fetchall()
    exact_names = {str(r["exact_name"]) for r in name_rows if r["exact_name"]}
    keys = {str(r["match_key"]) for r in name_rows if r["match_key"]}
    if not exact_names and not keys:
        return ()

    peer_ids = _name_gated_canonical_person_ids(
        connection, exact_names=exact_names, keys=keys
    )
    excluded = set(reject_set)
    excluded.add(canonical_id)
    excluded.add(person_id)

    primary_key = next(iter(sorted(keys)), "")
    scored: list[tuple[int, int]] = []
    for peer_id in peer_ids:
        if peer_id in excluded:
            continue
        if not peer_edge_eligible(
            connection,
            person_id=canonical_id,
            peer_id=peer_id,
            creating_mention_id=creating_mention_id,
        ):
            continue
        score = _score_person_names_only(
            connection,
            person_id=peer_id,
            mention_exact_name=str(row["display_name"]),
            primary_match_key=primary_key,
            query_keys=keys,
        )
        scored.append((score, peer_id))

    scored.sort(key=lambda item: (-item[0], item[1]))
    return tuple(peer_id for _score, peer_id in scored[:max_candidates])


def peer_edge_eligible(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    peer_id: int,
    creating_mention_id: int | None = None,
) -> bool:
    """K17: edge only if at least one side has a non-name identity fact.

    Checks the creating mention (when provided), then each side's operational
    projection. Pure name-only ↔ name-only pairs are not eligible.
    """
    if creating_mention_id is not None and _mention_has_non_name_fact(
        connection, creating_mention_id
    ):
        return True
    if _canonical_has_non_name_fact(connection, person_id):
        return True
    return _canonical_has_non_name_fact(connection, peer_id)


@dataclass(frozen=True, slots=True)
class _MentionQuery:
    id: int
    exact_name: str
    search_name: str
    identity_facts: tuple[tuple[str, str], ...]  # (kind, value)


def _load_mention_query(
    connection: sqlite3.Connection, person_mention_id: int
) -> _MentionQuery:
    row = connection.execute(
        """
        SELECT id, exact_name, search_name
          FROM person_mention
         WHERE id = ?
        """,
        (person_mention_id,),
    ).fetchone()
    if row is None:
        raise LookupError(f"person_mention {person_mention_id} does not exist")
    facts = tuple(
        (str(fact["kind"]), str(fact["value"]))
        for fact in connection.execute(
            """
            SELECT kind, value
              FROM mention_identity_fact
             WHERE person_mention_id = ?
             ORDER BY local_id
            """,
            (person_mention_id,),
        )
    )
    return _MentionQuery(
        id=int(row["id"]),
        exact_name=str(row["exact_name"]),
        search_name=str(row["search_name"]),
        identity_facts=facts,
    )


def _query_name_sets(mention: _MentionQuery) -> tuple[set[str], set[str]]:
    names: set[str] = set()
    if mention.exact_name:
        names.add(mention.exact_name)
    if mention.search_name:
        names.add(mention.search_name)
    for kind, value in mention.identity_facts:
        if kind == "name" and value:
            names.add(value)
    keys = {match_key(name) for name in names if match_key(name)}
    exact_names = {name for name in names if name}
    return exact_names, keys


def _name_gated_canonical_person_ids(
    connection: sqlite3.Connection,
    *,
    exact_names: Set[str],
    keys: Set[str],
) -> tuple[int, ...]:
    if not exact_names and not keys:
        return ()

    clauses: list[str] = []
    params: list[object] = []
    if keys:
        placeholders = ",".join("?" for _ in keys)
        clauses.append(f"sn.match_key IN ({placeholders})")
        params.extend(sorted(keys))
    if exact_names:
        placeholders = ",".join("?" for _ in exact_names)
        clauses.append(f"sn.exact_name IN ({placeholders})")
        params.extend(sorted(exact_names))

    where_names = " OR ".join(clauses)
    rows = connection.execute(
        f"""
        SELECT DISTINCT p.id AS person_id
          FROM person AS p
          JOIN sourced_name AS sn ON sn.person_id = p.id
         WHERE p.merged_into_person_id IS NULL
           AND ({where_names})
         ORDER BY p.id
        """,
        params,
    ).fetchall()
    return tuple(int(row["person_id"]) for row in rows)


def _score_person(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    mention_exact_name: str,
    primary_match_key: str,
    query_keys: Set[str],
    query_non_name_keys: Set[str],
) -> int:
    score = _score_person_names_only(
        connection,
        person_id=person_id,
        mention_exact_name=mention_exact_name,
        primary_match_key=primary_match_key,
        query_keys=query_keys,
    )

    projection_keys = _operational_non_name_fact_keys(connection, person_id)
    overlap = len(query_non_name_keys & projection_keys)
    if overlap:
        score += min(overlap * 10, 40)

    if _has_prior_research_mention(connection, person_id):
        score += 5
    return score


def _score_person_names_only(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    mention_exact_name: str,
    primary_match_key: str,
    query_keys: Set[str],
) -> int:
    name_rows = connection.execute(
        """
        SELECT exact_name, match_key
          FROM sourced_name
         WHERE person_id = ?
        """,
        (person_id,),
    ).fetchall()
    person_exact = {str(row["exact_name"]) for row in name_rows}
    person_keys = {str(row["match_key"]) for row in name_rows if row["match_key"]}

    score = 0
    if mention_exact_name in person_exact:
        score += 100
    if primary_match_key and primary_match_key in person_keys:
        score += 80
    other_keys = set(query_keys)
    if primary_match_key:
        other_keys.discard(primary_match_key)
    if person_keys & other_keys:
        score += 60
    return score


def _non_name_fact_keys(facts: Sequence[tuple[str, str]]) -> set[str]:
    keys: set[str] = set()
    for kind, value in facts:
        if kind == "name":
            continue
        key = match_key(value)
        if key:
            keys.add(key)
    return keys


def _operational_non_name_fact_keys(
    connection: sqlite3.Connection, canonical_id: int
) -> set[str]:
    keys: set[str] = set()
    for mention in mentions_for_canonical_person(connection, canonical_id):
        for fact in mention.identity_facts:
            if fact.kind == "name":
                continue
            key = match_key(fact.value)
            if key:
                keys.add(key)
    return keys


def _has_prior_research_mention(
    connection: sqlite3.Connection, canonical_id: int
) -> bool:
    for mention in mentions_for_canonical_person(connection, canonical_id):
        if mention.outcome == "research":
            return True
    return False


def _canonical_has_non_name_fact(
    connection: sqlite3.Connection, person_id: int
) -> bool:
    row = connection.execute(
        "SELECT merged_into_person_id FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if row is None:
        return False
    canonical_id = (
        int(row["merged_into_person_id"])
        if row["merged_into_person_id"] is not None
        else person_id
    )
    return bool(_operational_non_name_fact_keys(connection, canonical_id))


def _mention_has_non_name_fact(
    connection: sqlite3.Connection, person_mention_id: int
) -> bool:
    row = connection.execute(
        """
        SELECT 1
          FROM mention_identity_fact
         WHERE person_mention_id = ?
           AND kind != 'name'
         LIMIT 1
        """,
        (person_mention_id,),
    ).fetchone()
    return row is not None


def _build_candidate(
    connection: sqlite3.Connection,
    *,
    person_id: int,
    score: int,
    max_facts: int,
    max_names: int,
) -> CandidatePerson:
    person = connection.execute(
        "SELECT id, display_name FROM person WHERE id = ?",
        (person_id,),
    ).fetchone()
    if person is None:
        raise LookupError(f"person {person_id} does not exist")

    name_rows = connection.execute(
        """
        SELECT exact_name, search_name, match_key, kind
          FROM sourced_name
         WHERE person_id = ?
         ORDER BY id
         LIMIT ?
        """,
        (person_id, max_names),
    ).fetchall()
    names = tuple(
        SourcedNameView(
            exact_name=str(row["exact_name"]),
            search_name=str(row["search_name"]),
            match_key=str(row["match_key"]),
            kind=str(row["kind"]),
        )
        for row in name_rows
    )

    facts: list[CandidateFactView] = []
    for mention in mentions_for_canonical_person(connection, person_id):
        for fact in mention.identity_facts:
            if len(facts) >= max_facts:
                break
            facts.append(
                CandidateFactView(
                    local_id=f"c{person_id}-f{len(facts) + 1}",
                    kind=fact.kind,
                    value=fact.value,
                )
            )
        if len(facts) >= max_facts:
            break

    return CandidatePerson(
        person_id=person_id,
        display_name=str(person["display_name"]),
        score=score,
        names=names,
        facts=tuple(facts),
    )
