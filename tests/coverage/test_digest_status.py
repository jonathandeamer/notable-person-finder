import sqlite3

from notable_person_finder.coverage.repository import (
    coverage_corpus_counts,
    coverage_run_counts,
)


def test_coverage_summary_counts_integration(connection: sqlite3.Connection):
    # Testing that the queries don't crash
    corpus = coverage_corpus_counts(connection)
    assert corpus.assessments_completed == 0
    assert corpus.people_with_completed_assessment == 0
    assert corpus.stopped_matching_wikipedia == 0
    
    run = coverage_run_counts(connection, run_id=1)
    assert run.plans_completed == 0
    assert run.plans_incomplete == 0
    assert run.plans_failed == 0
    assert run.assessments_completed_this_run == 0
    assert run.model_deferred == 0
    assert run.model_failed == 0
