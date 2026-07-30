"""Coverage reconcile hooks for confirmed person merges (K18)."""

from __future__ import annotations

import sqlite3

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.coverage.service import (
    ensure_coverage_research,
    supersede_coverage_work_for_person,
)


def reconcile_on_merge(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    config: MainConfig,
    policy: SourcePolicy,
    now: str,
) -> None:
    """Supersede loser coverage work; reassign person_article; ensure survivor.

    Never blind-copies loser coverage current pointers onto the survivor.
    Assessment ``person_id`` columns keep observation-time person ids (audit).

    **The caller must already hold an open transaction** (merge settlement).
    """
    supersede_coverage_work_for_person(
        connection,
        person_id=loser_id,
        run_id=run_id,
        now=now,
    )
    _reassign_person_articles(
        connection,
        survivor_id=survivor_id,
        loser_id=loser_id,
    )
    ensure_coverage_research(
        connection,
        person_id=survivor_id,
        run_id=run_id,
        config=config,
        policy=policy,
        now=now,
    )


def _reassign_person_articles(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
) -> None:
    rows = connection.execute(
        """
        SELECT id, person_id, canonical_article_id, current_assessment_id
          FROM person_article
         WHERE person_id = ?
         ORDER BY id
        """,
        (loser_id,),
    ).fetchall()
    for row in rows:
        loser_pa_id = int(row["id"])
        article_id = int(row["canonical_article_id"])
        survivor_row = connection.execute(
            """
            SELECT id, current_assessment_id
              FROM person_article
             WHERE person_id = ? AND canonical_article_id = ?
            """,
            (survivor_id, article_id),
        ).fetchone()
        if survivor_row is None:
            connection.execute(
                """
                UPDATE person_article
                   SET person_id = ?
                 WHERE id = ?
                """,
                (survivor_id, loser_pa_id),
            )
            continue

        survivor_pa_id = int(survivor_row["id"])
        # Conflict: keep lower person_article.id; retire the other.
        if survivor_pa_id < loser_pa_id:
            keeper_id, retiree_id = survivor_pa_id, loser_pa_id
            keeper_current = (
                None
                if survivor_row["current_assessment_id"] is None
                else int(survivor_row["current_assessment_id"])
            )
            retiree_current = (
                None
                if row["current_assessment_id"] is None
                else int(row["current_assessment_id"])
            )
            reassign_keeper_person = False
        else:
            keeper_id, retiree_id = loser_pa_id, survivor_pa_id
            keeper_current = (
                None
                if row["current_assessment_id"] is None
                else int(row["current_assessment_id"])
            )
            retiree_current = (
                None
                if survivor_row["current_assessment_id"] is None
                else int(survivor_row["current_assessment_id"])
            )
            reassign_keeper_person = True

        _merge_person_article_conflict(
            connection,
            keeper_id=keeper_id,
            retiree_id=retiree_id,
            keeper_current=keeper_current,
            retiree_current=retiree_current,
            survivor_id=survivor_id,
            reassign_keeper_person=reassign_keeper_person,
        )


def _merge_person_article_conflict(
    connection: sqlite3.Connection,
    *,
    keeper_id: int,
    retiree_id: int,
    keeper_current: int | None,
    retiree_current: int | None,
    survivor_id: int,
    reassign_keeper_person: bool,
) -> None:
    """Resolve two person_article rows for the same (person, article).

    Pointer precedence on keeper:
      a) keeper.current completed → keep
      b) else retiree.current completed → adopt retiree.current id
      c) else null
    Assessments keep observation-time ``person_id``; only ``person_article_id``
    is re-pointed for FK integrity before deleting the retiree.
    """
    next_current = keeper_current
    if not _assessment_is_completed(connection, assessment_id=keeper_current):
        if _assessment_is_completed(connection, assessment_id=retiree_current):
            next_current = retiree_current
        else:
            next_current = None

    # Clear pointers that would block assessment re-home / delete.
    connection.execute(
        """
        UPDATE person_article
           SET current_assessment_id = NULL
         WHERE id IN (?, ?)
        """,
        (keeper_id, retiree_id),
    )

    # Re-home assessments that pointed at the retiree relation id.
    connection.execute(
        """
        UPDATE person_article_assessment
           SET person_article_id = ?
         WHERE person_article_id = ?
        """,
        (keeper_id, retiree_id),
    )

    # Delete retiree before reassigning keeper person_id so UNIQUE
    # (person_id, canonical_article_id) never collides.
    connection.execute("DELETE FROM person_article WHERE id = ?", (retiree_id,))

    if reassign_keeper_person:
        connection.execute(
            """
            UPDATE person_article
               SET person_id = ?
             WHERE id = ?
            """,
            (survivor_id, keeper_id),
        )

    if next_current is not None and _assessment_is_completed(
        connection, assessment_id=next_current
    ):
        connection.execute(
            """
            UPDATE person_article_assessment
               SET person_article_id = ?
             WHERE id = ?
            """,
            (keeper_id, next_current),
        )
        connection.execute(
            """
            UPDATE person_article
               SET current_assessment_id = ?
             WHERE id = ?
            """,
            (next_current, keeper_id),
        )


def _assessment_is_completed(
    connection: sqlite3.Connection, *, assessment_id: int | None
) -> bool:
    if assessment_id is None:
        return False
    row = connection.execute(
        """
        SELECT disposition
          FROM person_article_assessment
         WHERE id = ?
        """,
        (assessment_id,),
    ).fetchone()
    return row is not None and row["disposition"] == "completed"
