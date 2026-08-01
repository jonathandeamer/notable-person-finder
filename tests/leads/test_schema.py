import sqlite3

from notable_person_finder.db.migrate import apply_migrations


def test_0008_creates_all_lead_tables(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection, db_path, tmp_path / "backups")
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    names = {row[0] for row in rows}
    for expected in (
        "lead_assessment",
        "lead_assessment_qualifying_article",
        "lead_assessment_signal",
        "digest_queue",
        "queue_transition",
        "digest",
        "digest_entry",
    ):
        assert expected in names


def test_person_gains_current_lead_assessment_id_column(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection, db_path, tmp_path / "backups")
    columns = {row[1] for row in connection.execute("PRAGMA table_info(person)")}
    assert "current_lead_assessment_id" in columns


def test_lead_assessment_outcome_check_rejects_invalid_value(tmp_path):
    db_path = tmp_path / "test.db"
    connection = sqlite3.connect(db_path)
    apply_migrations(connection, db_path, tmp_path / "backups")
    fp = "0" * 64
    connection.execute(
        """
        INSERT INTO configuration_snapshot (
            id, fingerprint, canonical_json, created_at
        ) VALUES (1, ?, '{}', '2026-08-01T00:00:00Z')
        """,
        (fp,),
    )
    connection.execute(
        """
        INSERT INTO run (
            id, configuration_snapshot_id, state, timezone,
            window_start, window_end, started_at
        ) VALUES (
            1, 1, 'running', 'UTC',
            '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z'
        )
        """
    )
    connection.execute(
        """
        INSERT INTO person (
            id, created_at, created_by_run_id, display_name, identity_fingerprint
        ) VALUES (1, '2026-08-01T00:00:00Z', 1, 'Test Person', ?)
        """,
        (fp,),
    )
    try:
        connection.execute(
            """
            INSERT INTO lead_assessment (
                person_id, run_id, outcome, qualifying_domain_count,
                ordering_factors_json, lead_policy_fingerprint, decided_at
            ) VALUES (
                1, 1, 'not_a_real_outcome', 0, '{}', ?, '2026-08-01T00:00:00Z'
            )
            """,
            (fp,),
        )
        raised = False
    except sqlite3.IntegrityError:
        raised = True
    assert raised
