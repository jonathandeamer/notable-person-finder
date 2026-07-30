"""Wikipedia reconcile hooks for confirmed person merges (K16)."""

from __future__ import annotations

import sqlite3

from notable_person_finder.config.models import MainConfig
from notable_person_finder.wikipedia.service import (
    ensure_wikipedia_identity,
    supersede_wikipedia_work_for_person,
)


def reconcile_on_merge(
    connection: sqlite3.Connection,
    *,
    survivor_id: int,
    loser_id: int,
    run_id: int,
    config: MainConfig,
    now: str,
) -> None:
    """Supersede loser Wikipedia work/plans and ensure the survivor (K16).

    Never copies the loser's ``current_wikipedia_identity_observation_id`` onto
    the survivor. Historical loser observations remain immutable.

    **The caller must already hold an open transaction** (merge settlement).
    """
    supersede_wikipedia_work_for_person(
        connection,
        person_id=loser_id,
        run_id=run_id,
        now=now,
    )
    # K16: deliberately do not copy loser current_wikipedia_identity_observation_id.
    ensure_wikipedia_identity(
        connection,
        person_id=survivor_id,
        run_id=run_id,
        config=config,
        now=now,
    )
