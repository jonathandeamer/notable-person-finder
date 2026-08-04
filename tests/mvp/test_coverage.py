from typing import Any, cast

import httpx
import pytest

from notable.coverage import research
from notable.detect_contract import DetectedMention
from notable.errors import BudgetExceeded, Incomplete, ProviderFailure
from notable.llm import LlmClient


def _mention(name: str = "Ana Poy") -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        outcome="research",
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Named subject.",
    )


class FakeLlm:
    def __init__(self, results: list[dict[str, Any]] | None = None, error=None):
        self.results = list(results or [])
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        result = self.results.pop(0)
        validate = kwargs.get("validate")
        if validate is not None:
            validate(result)
        return result


def _brave_response(urls: list[str]) -> dict:
    return {
        "web": {"results": [{"url": u, "title": "T", "description": "D"} for u in urls]}
    }


def _assessment(**overrides) -> dict:
    base = {
        "person_relation": "same_person",
        "coverage_depth": "significant",
        "content_types": ["profile"],
        "subject_relationship": "editorially_independent",
        "signals": [],
        "supporting_passage_ids": ["p1"],
        "rationale": "In-depth profile.",
    }
    return base | overrides


def _handler_for(brave_urls, article_responses):
    """article_responses maps url -> (status, text, content_type) or None
    (meaning: the URL is never fetched -- used to assert ineligible/dropped
    URLs never reach the transport)."""

    def handler(request):
        if "brave" in request.url.host or "search" in str(request.url):
            return httpx.Response(200, json=_brave_response(brave_urls))
        url = str(request.url)
        if url not in article_responses or article_responses[url] is None:
            raise AssertionError(f"unexpected fetch of {url}")
        status, text, content_type = article_responses[url]
        headers = {"content-type": content_type} if content_type else {}
        return httpx.Response(status, text=text, headers=headers)

    return handler


def test_ineligible_results_are_never_fetched(make_config, make_transport):
    # twitter.com is curated_ineligible in the ported policy.
    handler = _handler_for(["https://twitter.com/x"], {})
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_zero_brave_results_short_circuits_to_empty(make_config, make_transport):
    handler = _handler_for([], {})
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_a_dead_link_does_not_fail_the_mention(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a", "https://www.theartnewspaper.com/b"]
    handler = _handler_for(
        urls,
        {
            urls[0]: (404, "not found", "text/html"),
            urls[1]: (200, "<html>body</html>", "text/html"),
        },
    )
    transport = make_transport(handler)
    llm = FakeLlm(results=[_assessment()])
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert len(result) == 1
    assert result[0].url == urls[1]


def test_brave_failure_raises_incomplete(make_config, make_transport):
    def handler(request):
        return httpx.Response(503, text="down")

    transport = make_transport(handler)
    with pytest.raises(Incomplete):
        research(_mention(), make_config(), transport, cast(LlmClient, FakeLlm()))


def test_an_assess_article_failure_raises_incomplete_for_the_whole_mention(
    make_config, make_transport
):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(urls, {urls[0]: (200, "<html>body</html>", "text/html")})
    transport = make_transport(handler)
    llm = FakeLlm(error=ProviderFailure("boom", permanent=False))
    with pytest.raises(Incomplete):
        research(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_budget_exceeded_propagates(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(urls, {urls[0]: (200, "<html>body</html>", "text/html")})
    transport = make_transport(handler)
    llm = FakeLlm(error=BudgetExceeded("cap"))
    with pytest.raises(BudgetExceeded):
        research(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_screening_status_is_carried_not_rederived(make_config, make_transport):
    # theartnewspaper.com is curated_eligible; an unlisted host is unclassified.
    urls = ["https://www.theartnewspaper.com/a", "https://some-blog.example/b"]
    handler = _handler_for(
        urls,
        {
            urls[0]: (200, "<html>a</html>", "text/html"),
            urls[1]: (200, "<html>b</html>", "text/html"),
        },
    )
    transport = make_transport(handler)
    llm = FakeLlm(results=[_assessment(), _assessment()])
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    statuses = {a.url: a.screening_status for a in result}
    assert statuses[urls[0]] == "curated_eligible"
    assert statuses[urls[1]] == "unclassified"


def test_non_html_content_type_is_dropped(make_config, make_transport):
    urls = ["https://www.theartnewspaper.com/a"]
    handler = _handler_for(urls, {urls[0]: (200, "{}", "application/json")})
    transport = make_transport(handler)
    llm = FakeLlm()
    result = research(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert result == ()
    assert llm.calls == []


def test_max_articles_per_mention_bounds_fetch_count(make_config, make_transport):
    urls = [f"https://www.theartnewspaper.com/{i}" for i in range(8)]
    responses = {u: (200, "<html>body</html>", "text/html") for u in urls}
    fetched = []

    def handler(request):
        if "search" in str(request.url):
            return httpx.Response(200, json=_brave_response(urls))
        url = str(request.url)
        fetched.append(url)
        status, text, content_type = responses[url]
        return httpx.Response(status, text=text, headers={"content-type": content_type})

    transport = make_transport(handler)
    config = make_config()
    llm = FakeLlm(
        results=[_assessment() for _ in range(config.brave.max_articles_per_mention)]
    )
    research(_mention(), config, transport, cast(LlmClient, llm))
    assert len(fetched) == config.brave.max_articles_per_mention
