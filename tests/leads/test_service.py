"""Unit tests for aggregate_person_lead handler and scheduling hook."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest

from notable_person_finder.config.models import MainConfig
from notable_person_finder.coverage.screening import SourcePolicy
from notable_person_finder.db.migrate import apply_migrations
from notable_person_finder.leads.service import (
    AGGREGATE_PERSON_LEAD_PRIORITY,
    AGGREGATE_PERSON_LEAD_TASK_TYPE,
    LOCAL_PROVIDER,
    _compute_material_fingerprint,
    _schedule_lead_aggregation_after_settled,
    build_aggregate_person_lead_handler,
    schedule_aggregate_person_lead,
)
from notable_person_finder.runs.models import WorkItem, WorkState
from tests.ingestion.helpers import immediate, insert_run, moment

NOW = moment()


def _main_config(*, policy_file: Path | None = None) -> MainConfig:
    return MainConfig(
        schema_version=1,
        timezone="Europe/Paris",
        feeds_file=Path("feeds.toml"),
        domain_profile_file=Path("profiles/art.toml"),
        source_policy_file=policy_file or Path("source_policies/visual_arts.toml"),
    )


@pytest.fixture
def connection(tmp_path: Path) -> Iterator[sqlite3.Connection]:
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    apply_migrations(conn, db_path, tmp_path / "backups")
    yield conn
    conn.close()


@pytest.fixture
def empty_policy() -> SourcePolicy:
    return SourcePolicy(
        schema_version=1,
        key="test_policy",
        label="Test Policy",
        rules=(),
        fingerprint="a" * 64,
    )


def test_handler_task_type_matches_constant(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )
    assert handler.task_type == AGGREGATE_PERSON_LEAD_TASK_TYPE


def test_handler_provider_is_local(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )
    assert handler.provider == LOCAL_PROVIDER


def test_execute_produces_no_attempt_row(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )
    assert handler.reserved_nano_usd == 0


def _seed_person_and_articles(
    connection: sqlite3.Connection, run_id: int
) -> tuple[int, int]:
    with immediate(connection):
        person_id = connection.execute(
            """
            INSERT INTO person (
                created_by_run_id, display_name, identity_fingerprint, created_at
            ) VALUES (?, 'Test Person', ?, ?)
            """,
            (run_id, "b" * 64, NOW),
        ).lastrowid
        assert person_id is not None

        article_id = connection.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES ('https://artnews.com/gallery/1', 'artnews.com', ?)
            """,
            (NOW,),
        ).lastrowid
        assert article_id is not None

        pa_id = connection.execute(
            """
            INSERT INTO person_article (person_id, canonical_article_id)
            VALUES (?, ?)
            """,
            (person_id, article_id),
        ).lastrowid
        assert pa_id is not None

        work_id = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'assess_article', 'person_article', ?, ?, 1, 70, ?, 'succeeded',
                ?, ?, ?
            )
            """,
            (pa_id, "w" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert work_id is not None

        attempt_id = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (?, ?, 'openrouter', 'generate', 1, ?, ?, 'succeeded', ?)
            """,
            (run_id, work_id, NOW, NOW, "r" * 64),
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
            ) VALUES (
                ?, ?, 'model', 'model-resolved', ?, '[]', 1, 1, 100, 100,
                'compatible', ?
            )
            """,
            (run_id, attempt_id, "rf" * 32, NOW),
        ).lastrowid
        assert inspection_id is not None

        view_id = connection.execute(
            """
            INSERT INTO article_view (
                canonical_article_id, run_id, attempt_id, access_kind,
                editorial_labels_json, main_text_blocks_json, snippets_json,
                extractor_version, observed_at
            ) VALUES (?, ?, ?, 'full', '[]', '[]', '[]', 1, ?)
            """,
            (article_id, run_id, attempt_id, NOW),
        ).lastrowid
        assert view_id is not None

        paa_id = connection.execute(
            """
            INSERT INTO person_article_assessment (
                person_article_id, person_id, canonical_article_id,
                article_view_id, run_id, attempt_id, model_inspection_id,
                disposition, person_relation, coverage_depth, content_types_json,
                subject_relationship, screening_rule_id, screening_rule_status,
                source_policy_fingerprint, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, rationale, observed_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, 'completed',
                'same_person', 'significant', '["exhibition_review"]',
                'editorially_independent', 'rule_1', 'curated_eligible', ?,
                '{}', '{}', ?, ?,
                1, ?, 'rationale', ?
            )
            """,
            (
                pa_id,
                person_id,
                article_id,
                view_id,
                run_id,
                attempt_id,
                inspection_id,
                "a" * 64,
                "p" * 64,
                "s" * 64,
                "tf" * 32,
                NOW,
            ),
        ).lastrowid
        assert paa_id is not None

        connection.execute(
            """
            INSERT INTO article_assessment_signal (
                assessment_id, signal_kind, category, claim,
                supporting_passage_ids_json, ordinal
            ) VALUES (?, 'attention', 'career_milestone', 'claim', '[]', 1)
            """,
            (paa_id,),
        )
    return person_id, paa_id


def test_prepare_execute_persist_round_trip(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    work_id = schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint="c" * 64,
        run_id=run_id,
        now=NOW,
    )

    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (run_id, work_id),
    )
    connection.commit()

    work = WorkItem(
        id=work_id,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        fingerprint="c" * 64,
        required=True,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        state=WorkState.RUNNING,
    )

    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )

    assert handler.prepare is not None
    prepared = handler.prepare(work)

    outcome = handler.execute(work, 1, prepared.payload)
    assert outcome.state == WorkState.SUCCEEDED

    assert handler.persist is not None
    with immediate(connection):
        handler.persist(work, outcome)

    # Verify attempt table remains empty for local execution
    attempts_count = connection.execute(
        "SELECT COUNT(*) as n FROM attempt WHERE work_item_id = ?", (work_id,)
    ).fetchone()["n"]
    assert attempts_count == 0

    # Verify lead_assessment row created and pointer updated on person
    person_row = connection.execute(
        "SELECT current_lead_assessment_id FROM person WHERE id = ?", (person_id,)
    ).fetchone()
    assert person_row is not None
    assert person_row["current_lead_assessment_id"] is not None
    lead_id = person_row["current_lead_assessment_id"]

    lead_row = connection.execute(
        "SELECT * FROM lead_assessment WHERE id = ?", (lead_id,)
    ).fetchone()
    assert lead_row is not None
    assert lead_row["person_id"] == person_id
    assert lead_row["outcome"] in ("promising_lead", "possible_lead")

    # Verify digest_queue row created
    queue_row = connection.execute(
        "SELECT * FROM digest_queue WHERE person_id = ?", (person_id,)
    ).fetchone()
    assert queue_row is not None
    assert queue_row["status"] == "pending"
    assert queue_row["lead_assessment_id"] == lead_id

    # Verify queue_transition row created
    transition_row = connection.execute(
        "SELECT * FROM queue_transition WHERE person_id = ?", (person_id,)
    ).fetchone()
    assert transition_row is not None
    assert transition_row["to_status"] == "pending"
    assert transition_row["lead_assessment_id"] == lead_id


def test_prepare_execute_persist_removed_when_wikipedia_matched(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    # Insert matched wikipedia observation and update person pointer
    with immediate(connection):
        page_id = connection.execute(
            """
            INSERT INTO mediawiki_page (
                page_id, canonical_title, canonical_url, namespace,
                is_disambiguation, is_missing, categories_json, last_observed_at
            ) VALUES (
                101, 'Alex Smith', 'https://en.wikipedia.org/wiki/Alex_Smith',
                0, 0, 0, '[]', ?
            )
            """,
            (NOW,),
        ).lastrowid
        assert page_id is not None

        obs_id = connection.execute(
            """
            INSERT INTO wikipedia_identity_observation (
                person_id, run_id, attempt_id, model_inspection_id,
                matched_mediawiki_page_id, disposition, semantic_outcome,
                candidate_page_ids_json, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash,
                schema_version, task_fingerprint, rationale, observed_at
            ) VALUES (
                ?, ?, ?, ?, ?,
                'completed', 'matching_page_found', '[101]', '{}',
                '{}', ?, ?, 1, ?,
                'found match', ?
            )
            """,
            (
                person_id,
                run_id,
                1,
                1,
                page_id,
                "p" * 64,
                "s" * 64,
                "d" * 64,
                NOW,
            ),
        ).lastrowid
        assert obs_id is not None
        connection.execute(
            """
            UPDATE person
               SET current_wikipedia_identity_observation_id = ?
             WHERE id = ?
            """,
            (obs_id, person_id),
        )

        # Seed initial digest_queue row as pending
        connection.execute(
            """
            INSERT INTO lead_assessment (
                person_id, run_id, outcome, qualifying_domain_count,
                ordering_factors_json, lead_policy_fingerprint, decided_at
            ) VALUES (?, ?, 'possible_lead', 1, '{}', ?, ?)
            """,
            (person_id, run_id, "a" * 64, NOW),
        )
        prior_lead_id = connection.execute("SELECT last_insert_rowid()").fetchone()[0]
        connection.execute(
            """
            INSERT INTO digest_queue (
                person_id, status, tier, eligibility_reason, lead_assessment_id,
                first_pending_at, last_material_change_at
            ) VALUES (?, 'pending', 'possible_lead', 'new', ?, ?, ?)
            """,
            (person_id, prior_lead_id, NOW, NOW),
        )

    work_id = schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint="e" * 64,
        run_id=run_id,
        now=NOW,
    )
    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (run_id, work_id),
    )
    connection.commit()

    work = WorkItem(
        id=work_id,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        fingerprint="e" * 64,
        required=True,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        state=WorkState.RUNNING,
    )

    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )

    assert handler.prepare is not None
    prepared = handler.prepare(work)
    outcome = handler.execute(work, 1, prepared.payload)
    assert handler.persist is not None
    with immediate(connection):
        handler.persist(work, outcome)

    # Check digest_queue was removed
    queue_row = connection.execute(
        "SELECT * FROM digest_queue WHERE person_id = ?", (person_id,)
    ).fetchone()
    assert queue_row is not None
    assert queue_row["status"] == "removed"
    assert queue_row["removed_reason"] == "matching_page_found"

    # Check transition logged
    transition_row = connection.execute(
        "SELECT * FROM queue_transition WHERE person_id = ? ORDER BY id DESC LIMIT 1",
        (person_id,),
    ).fetchone()
    assert transition_row is not None
    assert transition_row["from_status"] == "pending"
    assert transition_row["to_status"] == "removed"
    assert transition_row["reason"] == "matching_page_found"


def test_schedule_lead_aggregation_after_settled(
    connection: sqlite3.Connection, tmp_path: Path
) -> None:
    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    policy_file = tmp_path / "policy.toml"
    policy_file.write_text(
        """
schema_version = 1
key = "test_policy"
label = "Test Policy"
        """,
        encoding="utf-8",
    )

    config = _main_config(policy_file=policy_file)

    with immediate(connection):
        _schedule_lead_aggregation_after_settled(
            connection,
            person_id=person_id,
            run_id=run_id,
            config=config,
            now=NOW,
        )

    row = connection.execute(
        "SELECT * FROM work_item WHERE task_type = ? AND subject_id = ?",
        (AGGREGATE_PERSON_LEAD_TASK_TYPE, person_id),
    ).fetchone()
    assert row is not None
    assert row["state"] == "pending"
    assert row["priority"] == AGGREGATE_PERSON_LEAD_PRIORITY


def test_schedule_lead_aggregation_after_settled_invalid_policy_noop(
    connection: sqlite3.Connection, tmp_path: Path
) -> None:
    run_id = insert_run(connection)
    config = _main_config(policy_file=tmp_path / "nonexistent.toml")

    with immediate(connection):
        _schedule_lead_aggregation_after_settled(
            connection,
            person_id=1,
            run_id=run_id,
            config=config,
            now=NOW,
        )

    count = connection.execute(
        "SELECT COUNT(*) as n FROM work_item WHERE task_type = ?",
        (AGGREGATE_PERSON_LEAD_TASK_TYPE,),
    ).fetchone()["n"]
    assert count == 0


def test_compute_material_fingerprint(empty_policy: SourcePolicy) -> None:
    config = _main_config()
    fp1 = _compute_material_fingerprint(
        article_assessment_ids=[1, 2],
        signal_ids=[10],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    fp2 = _compute_material_fingerprint(
        article_assessment_ids=[2, 1],
        signal_ids=[10],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    assert fp1 == fp2
    assert len(fp1) == 64


def test_unchanged_fingerprint_does_not_reaggregate(
    empty_policy: SourcePolicy,
) -> None:
    """`_compute_material_fingerprint` is order-independent (sorted before
    hashing), so two schedule/prepare passes over the same underlying
    evidence -- regardless of the order rows come back from SQLite -- must
    agree on "unchanged." (The end-to-end refusal itself is covered by
    `test_prepare_refuses_when_fingerprint_matches_prior_aggregation` below,
    which drives the real handler and asserts no second `lead_assessment`
    row is written.)"""
    config = _main_config()
    fp1 = _compute_material_fingerprint(
        article_assessment_ids=[1, 2],
        signal_ids=[],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    fp2 = _compute_material_fingerprint(
        article_assessment_ids=[2, 1],
        signal_ids=[],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    assert fp1 == fp2


def test_changed_evidence_changes_fingerprint(empty_policy: SourcePolicy) -> None:
    config = _main_config()
    fp1 = _compute_material_fingerprint(
        article_assessment_ids=[1],
        signal_ids=[],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    fp2 = _compute_material_fingerprint(
        article_assessment_ids=[1, 2],
        signal_ids=[],
        wikipedia_outcome="no_matching_page_found",
        policy=empty_policy,
        config=config,
    )
    assert fp1 != fp2


def _add_second_article_assessment(
    connection: sqlite3.Connection, *, person_id: int, run_id: int
) -> int:
    """Add a second, distinct completed person_article_assessment (and a
    qualifying signal) for an *existing* person, changing that person's
    evidence set -- and therefore their `_compute_material_fingerprint`
    input -- without creating a new person. Mirrors
    `_seed_person_and_articles`'s row shapes but targets `person_id` rather
    than minting a fresh one."""
    with immediate(connection):
        article_id = connection.execute(
            """
            INSERT INTO canonical_article (canonical_url, publisher_key, first_seen_at)
            VALUES ('https://artnews.com/gallery/2', 'artnews.com', ?)
            """,
            (NOW,),
        ).lastrowid
        assert article_id is not None

        pa_id = connection.execute(
            """
            INSERT INTO person_article (person_id, canonical_article_id)
            VALUES (?, ?)
            """,
            (person_id, article_id),
        ).lastrowid
        assert pa_id is not None

        work_id = connection.execute(
            """
            INSERT INTO work_item (
                task_type, subject_kind, subject_id, fingerprint, required,
                priority, eligible_at, state, created_by_run_id, created_at,
                updated_at
            ) VALUES (
                'assess_article', 'person_article', ?, ?, 1, 70, ?, 'succeeded',
                ?, ?, ?
            )
            """,
            (pa_id, "x" * 64, NOW, run_id, NOW, NOW),
        ).lastrowid
        assert work_id is not None

        attempt_id = connection.execute(
            """
            INSERT INTO attempt (
                run_id, work_item_id, provider, operation, ordinal, started_at,
                finished_at, outcome, request_fingerprint
            ) VALUES (?, ?, 'openrouter', 'generate', 1, ?, ?, 'succeeded', ?)
            """,
            (run_id, work_id, NOW, NOW, "y" * 64),
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
            ) VALUES (
                ?, ?, 'model', 'model-resolved', ?, '[]', 1, 1, 100, 100,
                'compatible', ?
            )
            """,
            (run_id, attempt_id, "z" * 64, NOW),
        ).lastrowid
        assert inspection_id is not None

        view_id = connection.execute(
            """
            INSERT INTO article_view (
                canonical_article_id, run_id, attempt_id, access_kind,
                editorial_labels_json, main_text_blocks_json, snippets_json,
                extractor_version, observed_at
            ) VALUES (?, ?, ?, 'full', '[]', '[]', '[]', 1, ?)
            """,
            (article_id, run_id, attempt_id, NOW),
        ).lastrowid
        assert view_id is not None

        paa_id = connection.execute(
            """
            INSERT INTO person_article_assessment (
                person_article_id, person_id, canonical_article_id,
                article_view_id, run_id, attempt_id, model_inspection_id,
                disposition, person_relation, coverage_depth, content_types_json,
                subject_relationship, screening_rule_id, screening_rule_status,
                source_policy_fingerprint, canonical_supplied_input_json,
                validated_output_json, prompt_hash, schema_hash, schema_version,
                task_fingerprint, rationale, observed_at
            ) VALUES (
                ?, ?, ?, ?,
                ?, ?, ?, 'completed',
                'same_person', 'significant', '["exhibition_review"]',
                'editorially_independent', 'rule_1', 'curated_eligible', ?,
                '{}', '{}', ?, ?,
                1, ?, 'rationale', ?
            )
            """,
            (
                pa_id,
                person_id,
                article_id,
                view_id,
                run_id,
                attempt_id,
                inspection_id,
                "a" * 64,
                "p" * 64,
                "s" * 64,
                "tg" * 32,
                NOW,
            ),
        ).lastrowid
        assert paa_id is not None

        connection.execute(
            """
            INSERT INTO article_assessment_signal (
                assessment_id, signal_kind, category, claim,
                supporting_passage_ids_json, ordinal
            ) VALUES (?, 'attention', 'career_milestone', 'claim', '[]', 1)
            """,
            (paa_id,),
        )
    return paa_id


def test_prepare_refuses_when_fingerprint_matches_prior_aggregation(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    """Integration-level regression for the K6 gap: once a person's evidence
    has been aggregated once, a *second* aggregate_person_lead work item
    scheduled against the *same* evidence (schedule_work only dedups
    pending/running/deferred work, not succeeded -- exactly how
    seed_lead_aggregation's every-run sweep would otherwise re-run this
    person forever) must be refused by `prepare` before a second
    `lead_assessment` row is written."""
    from notable_person_finder.leads.service import AGGREGATE_PREPARE_REFUSED_PREFIX

    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )

    def _run_once(fingerprint: str) -> WorkItem:
        work_id = schedule_aggregate_person_lead(
            connection,
            person_id=person_id,
            material_fingerprint=fingerprint,
            run_id=run_id,
            now=NOW,
        )
        connection.execute(
            "UPDATE work_item SET state = 'running', claimed_by_run_id = ? "
            "WHERE id = ?",
            (run_id, work_id),
        )
        connection.commit()
        return WorkItem(
            id=work_id,
            task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
            subject_kind="person",
            subject_id=person_id,
            fingerprint=fingerprint,
            required=True,
            priority=AGGREGATE_PERSON_LEAD_PRIORITY,
            state=WorkState.RUNNING,
        )

    assert handler.prepare is not None
    assert handler.persist is not None

    first_work = _run_once("c" * 64)
    prepared = handler.prepare(first_work)
    outcome = handler.execute(first_work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(first_work, outcome)

    lead_count_after_first = connection.execute(
        "SELECT COUNT(*) AS n FROM lead_assessment WHERE person_id = ?",
        (person_id,),
    ).fetchone()["n"]
    assert lead_count_after_first == 1

    # Settle the first work item to 'succeeded', mirroring exactly what
    # `runs.repository.complete_work` does in production once a handler's
    # `persist` commits: state -> succeeded, completed_by_run_id set,
    # claimed_by_run_id cleared. Without this, the first row would still be
    # `running`, and `schedule_work`'s dedup (pending/running/deferred,
    # `runs/repository.py:254-263`) would itself return the *same* work_id
    # for the "second" schedule call below -- proving nothing about the
    # succeeded-item gap this test exists to cover.
    connection.execute(
        """
        UPDATE work_item
           SET state = 'succeeded',
               reason = NULL,
               completed_by_run_id = ?,
               claimed_by_run_id = NULL,
               updated_at = ?
         WHERE id = ?
        """,
        (run_id, NOW, first_work.id),
    )
    connection.commit()

    # A second work item scheduled against the same, unchanged evidence.
    # schedule_work's dedup only covers pending/running/deferred -- not
    # succeeded -- so this must produce a genuinely new work_id rather than
    # returning the first item's id. That is the exact production gap: a
    # succeeded item's fingerprint is never deduped by schedule_work, so a
    # fresh work item is created and `prepare`'s own refusal check is the
    # only thing standing between it and a duplicate aggregation.
    second_work = _run_once("c" * 64)
    assert second_work.id != first_work.id

    with pytest.raises(ValueError, match=AGGREGATE_PREPARE_REFUSED_PREFIX):
        handler.prepare(second_work)

    lead_count_after_refusal = connection.execute(
        "SELECT COUNT(*) AS n FROM lead_assessment WHERE person_id = ?",
        (person_id,),
    ).fetchone()["n"]
    assert lead_count_after_refusal == 1


def test_prepare_proceeds_when_evidence_changed_since_prior_aggregation(
    connection: sqlite3.Connection, empty_policy: SourcePolicy
) -> None:
    """The converse of the refusal test: once the person gains a new
    qualifying article after their prior aggregation, `prepare` must not
    refuse, and settling the new work item must produce a fresh
    `lead_assessment` row."""
    run_id = insert_run(connection)
    person_id, _paa_id = _seed_person_and_articles(connection, run_id=run_id)

    config = _main_config()
    handler = build_aggregate_person_lead_handler(
        connection, config=config, policy=empty_policy
    )
    assert handler.prepare is not None
    assert handler.persist is not None

    work_id = schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint="c" * 64,
        run_id=run_id,
        now=NOW,
    )
    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (run_id, work_id),
    )
    connection.commit()
    first_work = WorkItem(
        id=work_id,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        fingerprint="c" * 64,
        required=True,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        state=WorkState.RUNNING,
    )
    prepared = handler.prepare(first_work)
    outcome = handler.execute(first_work, 1, prepared.payload)
    with immediate(connection):
        handler.persist(first_work, outcome)

    lead_count_after_first = connection.execute(
        "SELECT COUNT(*) AS n FROM lead_assessment WHERE person_id = ?",
        (person_id,),
    ).fetchone()["n"]
    assert lead_count_after_first == 1

    # Evidence changes: a new qualifying article assessment for this person.
    _add_second_article_assessment(connection, person_id=person_id, run_id=run_id)

    work_id_2 = schedule_aggregate_person_lead(
        connection,
        person_id=person_id,
        material_fingerprint="d" * 64,
        run_id=run_id,
        now=NOW,
    )
    connection.execute(
        "UPDATE work_item SET state = 'running', claimed_by_run_id = ? WHERE id = ?",
        (run_id, work_id_2),
    )
    connection.commit()
    second_work = WorkItem(
        id=work_id_2,
        task_type=AGGREGATE_PERSON_LEAD_TASK_TYPE,
        subject_kind="person",
        subject_id=person_id,
        fingerprint="d" * 64,
        required=True,
        priority=AGGREGATE_PERSON_LEAD_PRIORITY,
        state=WorkState.RUNNING,
    )

    # Must not raise.
    prepared_2 = handler.prepare(second_work)
    outcome_2 = handler.execute(second_work, 1, prepared_2.payload)
    with immediate(connection):
        handler.persist(second_work, outcome_2)

    lead_count_after_second = connection.execute(
        "SELECT COUNT(*) AS n FROM lead_assessment WHERE person_id = ?",
        (person_id,),
    ).fetchone()["n"]
    assert lead_count_after_second == 2
