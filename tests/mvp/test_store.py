from decimal import Decimal

import pytest

from notable.store import RunSummary, Store


@pytest.fixture
def store(tmp_path):
    instance = Store(tmp_path / "notable.db")
    yield instance
    instance.close()


def test_status_is_ok_only_when_nothing_failed_and_the_cap_did_not_bind():
    assert RunSummary([], [], False, Decimal("0")).status == "ok"


@pytest.mark.parametrize(
    "summary",
    [
        RunSummary([], ["u"], False, Decimal("0")),
        RunSummary([], [], True, Decimal("0")),
        RunSummary(["a"], ["u"], True, Decimal("0")),
    ],
)
def test_any_incomplete_item_or_the_cap_makes_a_run_partial(summary):
    # A mutable status set only on the budget path reported a run with failed
    # items as ok. Both conditions must reach the log.
    assert summary.status == "partial"


def test_a_new_url_is_eligible(store):
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True


def test_a_settled_url_is_not_eligible(store):
    store.commit(["https://a.test/1"], [], [])
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_an_incomplete_url_stays_eligible_until_the_cap(store):
    url = "https://a.test/1"
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is True
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is True
    store.commit([], [url], [])
    assert store.is_eligible(url, max_attempts=3) is False, "abandoned at the cap"


def test_attempts_accumulate_across_runs(store):
    url = "https://a.test/1"
    for _ in range(2):
        store.commit([], [url], [])
    assert store.attempts(url) == 2


def test_settling_a_previously_incomplete_url_ends_retries(store):
    url = "https://a.test/1"
    store.commit([], [url], [])
    store.commit([url], [], [])
    assert store.is_eligible(url, max_attempts=3) is False


def test_log_writes_a_run_row(store):
    store.log(RunSummary(["a", "b"], ["c"], False, Decimal("1.25")), [], "d.md")
    row = store.connection.execute(
        "SELECT n_items_settled, n_items_incomplete, cost_usd, status FROM run"
    ).fetchone()
    assert row == (2, 1, "1.25", "partial")


def test_log_failure_does_not_prevent_state_from_committing(store, monkeypatch):
    # One bad log row must not livelock the product: the digest is already
    # written, so rolling back item markers would repeat the run forever.
    store.commit(["https://a.test/1"], [], [])
    monkeypatch.setattr(store, "_insert_run", _boom)
    store.log(RunSummary([], [], False, Decimal("0")), [], "d.md")  # must not raise
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_schema_creation_is_idempotent(tmp_path):
    path = tmp_path / "notable.db"
    Store(path).close()
    second = Store(path)
    assert {
        r[0]
        for r in second.connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    } == {"item", "surfaced", "run", "lead", "research_cache"}
    second.close()


def _boom(*_args, **_kwargs):
    raise RuntimeError("log write failed")


def test_research_cache_round_trip(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    # Verify misses return None
    assert store.cached_research("fake_id") is None

    # Commit research
    research_dict = {"fake_id": {"outcome": "no_matching_page", "foo": "bar"}}
    store.commit([], [], [], researched=research_dict)

    # Verify hits return the dictionary
    cached = store.cached_research("fake_id")
    assert cached is not None
    assert cached["outcome"] == "no_matching_page"
    assert cached["foo"] == "bar"
