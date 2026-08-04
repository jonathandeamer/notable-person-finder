from decimal import Decimal
from typing import Literal, cast

import pytest

from notable import digest
from notable.detect_contract import DetectedMention
from notable.errors import BudgetExceeded, Incomplete
from notable.feeds import SourceItem
from notable.http import Transport
from notable.llm import LlmClient
from notable.pipeline import Providers, run
from notable.rank import Lead
from notable.wiki_contract import MatchVerdict


def _item(n: int) -> SourceItem:
    return SourceItem(f"https://a.test/{n}", f"Title {n}", "Summary.", None, "a", "A")


def _mention(
    name: str,
    outcome: Literal["research", "do_not_research", "uncertain"] = "research",
) -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        outcome=outcome,
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Named subject.",
    )


class FakeLlm:
    def spend(self) -> Decimal:
        return Decimal("0.10")


@pytest.fixture
def providers() -> Providers:
    # The pipeline never touches the transport directly; feeds and detect are
    # both patched out, so a null transport is honest about what is exercised.
    return Providers(transport=cast(Transport, None), llm=cast(LlmClient, FakeLlm()))


def _returning(mapping):
    return lambda item, cfg, llm: mapping[item.url]


_NO_PAGE = MatchVerdict(
    outcome="no_matching_page", selected_page_id=None, rationale="t"
)


@pytest.fixture
def install(monkeypatch):
    """Patch the three seams the pipeline calls. monkeypatch undoes all three."""

    def apply(items, detect_fn, match_fn=None):
        monkeypatch.setattr(
            "notable.pipeline.feeds.fetch_new", lambda *a, **k: iter(items)
        )
        monkeypatch.setattr("notable.pipeline.detect.people_in", detect_fn)
        monkeypatch.setattr(
            "notable.pipeline.wiki.match",
            match_fn or (lambda mention, cfg, transport, llm: _NO_PAGE),
        )
        monkeypatch.setattr(
            "notable.pipeline.coverage.research",
            lambda *a, **k: ()
        )
        monkeypatch.setattr(
            "notable.pipeline.rank.assess",
            lambda mention, verdict, articles, config, item: Lead(
                identity_key=digest.identity_key(mention.exact_name),
                display_name=mention.exact_name,
                source_url=getattr(item, "url", ""),
                publisher_label=getattr(item, "publisher_label", ""),
                wikipedia_verdict=verdict,
                outcome="promising_lead",
                article_assessments=(),
                rank_tuple=(0, 0, 0, digest.identity_key(mention.exact_name)),
                namesake_urls=(),
                rationale=mention.rationale,
                qualifying_domains=(),
            )
        )

    return apply


def test_writes_a_digest_and_settles_items(make_config, store, providers, install):
    install([_item(1)], _returning({"https://a.test/1": (_mention("Ana Poy"),)}))
    path = run(make_config(), store, providers).digest_path
    assert "Ana Poy" in path.read_text("utf-8")
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_the_summary_describes_this_run_even_if_logging_fails(
    make_config, store, providers, install, monkeypatch
):
    # store.log swallows its own failures by design. If the run report were
    # read back from the `run` table, a failed log write would leave it
    # describing the *previous* run, silently.
    install([_item(1)], _returning({"https://a.test/1": (_mention("Ana Poy"),)}))
    monkeypatch.setattr(store, "_insert_run", _boom)
    result = run(make_config(), store, providers)
    assert result.summary.settled == ["https://a.test/1"]
    assert result.summary.cost_usd == Decimal("0.10")
    assert store.connection.execute("SELECT COUNT(*) FROM run").fetchone()[0] == 0


def test_non_research_worthy_mentions_are_not_shown(
    make_config, store, providers, install
):
    install(
        [_item(1)],
        _returning(
            {"https://a.test/1": (_mention("Ana Poy", outcome="do_not_research"),)}
        ),
    )
    path = run(make_config(), store, providers).digest_path
    assert "Ana Poy" not in path.read_text("utf-8")
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_an_incomplete_item_is_not_settled_and_contributes_nothing(
    make_config, store, providers, install
):
    def detect(item, cfg, llm):
        if item.url.endswith("1"):
            raise Incomplete("nope")
        return (_mention("Bo Li"),)

    install([_item(1), _item(2)], detect)
    path = run(make_config(), store, providers).digest_path
    text = path.read_text("utf-8")
    assert "Bo Li" in text
    assert "Run status: **partial**" in text
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True
    assert store.is_eligible("https://a.test/2", max_attempts=3) is False


def test_incomplete_discards_earlier_mentions_of_the_same_item(
    make_config, store, providers, install
):
    # Invariant 3: mentions one and two go with mention three's failure. A
    # generator that yields two mentions and then raises is exactly that case.
    def detect(item, cfg, llm):
        def mentions():
            yield _mention("Ana Poy")
            yield _mention("Bo Li")
            raise Incomplete("failed on the third mention")

        return mentions()

    install([_item(1)], detect)
    path = run(make_config(), store, providers).digest_path
    text = path.read_text("utf-8")
    assert "Ana Poy" not in text and "Bo Li" not in text
    assert "No leads found" in text
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True


def test_budget_exceeded_renders_what_finished(make_config, store, providers, install):
    def detect(item, cfg, llm):
        if item.url.endswith("2"):
            raise BudgetExceeded("cap")
        return (_mention("Ana Poy"),)

    install([_item(1), _item(2)], detect)
    path = run(make_config(), store, providers).digest_path
    text = path.read_text("utf-8")
    assert "Ana Poy" in text
    assert "Run status: **partial**" in text
    assert store.is_eligible("https://a.test/2", max_attempts=3) is True


def test_a_crash_before_commit_writes_nothing(
    make_config, store, providers, install, monkeypatch
):
    install([_item(1)], _returning({"https://a.test/1": (_mention("Ana Poy"),)}))
    monkeypatch.setattr("notable.pipeline.digest.write", _boom)
    with pytest.raises(RuntimeError):
        run(make_config(), store, providers)
    assert store.is_eligible("https://a.test/1", max_attempts=3) is True
    assert store.connection.execute("SELECT COUNT(*) FROM run").fetchone()[0] == 0
    assert store.connection.execute("SELECT COUNT(*) FROM item").fetchone()[0] == 0


def test_a_mid_run_crash_replays_completed_calls_without_duplicate_work(
    make_config, store, providers, install
):
    # This is the orchestration-level recovery invariant: item 1 completed
    # before the crash is a cache hit on replay; item 2, which never completed,
    # performs the only new call on the second run.
    cached = {}
    provider_calls = []
    crash = [True]

    def detect(item, cfg, llm):
        if item.url in cached:
            return cached[item.url]
        if item.url.endswith("/2") and crash[0]:
            raise RuntimeError("crash after the first completed item")
        # Recorded only once the call has actually happened and would settle
        # the item — a crash below this line must not count as a provider
        # call, or a replayed retry looks like a duplicate call it never made.
        provider_calls.append(item.url)
        result = (_mention("Ana Poy" if item.url.endswith("/1") else "Bo Li"),)
        cached[item.url] = result
        return result

    install([_item(1), _item(2)], detect)
    with pytest.raises(RuntimeError):
        run(make_config(), store, providers)
    crash[0] = False
    run(make_config(), store, providers)
    assert provider_calls == ["https://a.test/1", "https://a.test/2"]


def test_a_matching_wikipedia_page_produces_no_digest_entry(
    make_config, store, providers, install
):
    verdict = MatchVerdict(outcome="matching_page", selected_page_id=1, rationale="r")
    install(
        [_item(1)],
        _returning({"https://a.test/1": (_mention("Ana Poy"),)}),
        match_fn=lambda mention, cfg, transport, llm: verdict,
    )
    path = run(make_config(), store, providers).digest_path
    assert "Ana Poy" not in path.read_text("utf-8")
    assert store.is_eligible("https://a.test/1", max_attempts=3) is False


def test_uncertain_and_no_matching_page_still_produce_entries(
    make_config, store, providers, install
):
    for outcome in ("uncertain", "no_matching_page"):
        store.connection.execute("DELETE FROM surfaced")
        store.connection.execute("DELETE FROM item")
        verdict = MatchVerdict(outcome=outcome, selected_page_id=None, rationale="r")
        install(
            [_item(1)],
            _returning({"https://a.test/1": (_mention("Ana Poy"),)}),
            match_fn=lambda mention, cfg, transport, llm, v=verdict: v,
        )
        path = run(make_config(), store, providers).digest_path
        assert "Ana Poy" in path.read_text("utf-8")


def _boom(*_args, **_kwargs):
    raise RuntimeError("digest write failed")
