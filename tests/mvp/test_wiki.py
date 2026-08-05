from typing import Any, cast

import httpx
import pytest

from notable.detect_contract import DetectedMention
from notable.errors import BudgetExceeded, Incomplete, ProviderFailure
from notable.llm import LlmClient
from notable.wiki import match


def _mention(name: str = "Ana Poy") -> DetectedMention:
    return DetectedMention(
        exact_name=name,
        canonical_name=name,
        outcome="research",
        supporting_passage_ids=("p1",),
        identity_facts=(),
        signals=(),
        rationale="Named subject.",
    )


class FakeLlm:
    def __init__(self, result: dict[str, Any] | None = None, error=None):
        self.result, self.error, self.calls = result, error, []

    def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        validate = kwargs.get("validate")
        if validate is not None:
            validate(self.result)
        return self.result


def _search_response(hits: list[int], *, truncated: bool = False) -> dict:
    body: dict = {
        "query": {
            "search": [{"pageid": pid, "ns": 0, "title": f"P{pid}"} for pid in hits]
        }
    }
    if truncated:
        body["continue"] = {"sroffset": len(hits)}
    return body


def _facts_response(pages: list[dict]) -> dict:
    return {"query": {"pages": pages}}


def _page_json(page_id: int, **overrides: Any) -> dict:
    base = {
        "pageid": page_id,
        "ns": 0,
        "title": f"P{page_id}",
        "description": "A sculptor",
        "extract": "P is a sculptor.",
        "categories": [{"title": "Category:Sculptors"}],
    }
    return base | overrides


def test_empty_search_short_circuits_to_no_matching_page(make_config, make_transport):
    def handler(request):
        return httpx.Response(200, json=_search_response([]))

    transport = make_transport(handler)
    llm = FakeLlm()
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.outcome == "no_matching_page"
    assert llm.calls == []


def test_empty_and_truncated_search_short_circuits_to_uncertain(
    make_config, make_transport
):
    def handler(request):
        return httpx.Response(200, json=_search_response([], truncated=True))

    transport = make_transport(handler)
    llm = FakeLlm()
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.outcome == "uncertain"
    assert llm.calls == []


def test_a_non_redirect_biography_hit_reaches_the_model(make_config, make_transport):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(200, json=_facts_response([_page_json(1)]))

    transport = make_transport(handler)
    llm = FakeLlm(
        result={
            "outcome": "matching_page",
            "selected_page_id": 1,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Same person.",
        }
    )
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.has_page is True
    assert verdict.selected_page_id == 1
    assert len(llm.calls) == 1


def test_a_redirect_is_resolved_in_one_extra_wave(make_config, make_transport):
    calls = []

    def handler(request):
        calls.append(request)
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        if request.url.params.get("redirects") == "1":
            return httpx.Response(
                200, json=_facts_response([_page_json(2, title="Target")])
            )
        return httpx.Response(
            200,
            json=_facts_response(
                [
                    _page_json(
                        1,
                        redirect=True,
                        extract=None,
                        description=None,
                        categories=[],
                    )
                ]
            ),
        )

    transport = make_transport(handler)
    llm = FakeLlm(
        result={
            "outcome": "matching_page",
            "selected_page_id": 2,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Same person, via redirect.",
        }
    )
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.selected_page_id == 2
    assert len(calls) == 3  # search, facts, redirect-resolution facts


def test_a_double_redirect_is_dropped_not_chased(make_config, make_transport):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        if request.url.params.get("redirects") == "1":
            return httpx.Response(
                200,
                json=_facts_response(
                    [
                        _page_json(
                            2,
                            redirect=True,
                            extract=None,
                            description=None,
                            categories=[],
                        )
                    ]
                ),
            )
        return httpx.Response(
            200,
            json=_facts_response(
                [
                    _page_json(
                        1,
                        redirect=True,
                        extract=None,
                        description=None,
                        categories=[],
                    )
                ]
            ),
        )

    transport = make_transport(handler)
    llm = FakeLlm()
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.outcome == "no_matching_page"
    assert llm.calls == []


def test_a_disambiguation_hit_is_never_a_candidate(make_config, make_transport):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(
            200,
            json=_facts_response([_page_json(1, pageprops={"disambiguation": ""})]),
        )

    transport = make_transport(handler)
    llm = FakeLlm()
    verdict = match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert verdict.outcome == "no_matching_page"
    assert llm.calls == []


def test_a_mediawiki_failure_raises_incomplete(make_config, make_transport):
    def handler(request):
        return httpx.Response(503, text="down")

    transport = make_transport(handler)
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, FakeLlm()))


def test_a_model_provider_failure_raises_incomplete(make_config, make_transport):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(200, json=_facts_response([_page_json(1)]))

    transport = make_transport(handler)
    llm = FakeLlm(error=ProviderFailure("boom", permanent=False))
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_budget_exceeded_propagates(make_config, make_transport):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(200, json=_facts_response([_page_json(1)]))

    transport = make_transport(handler)
    llm = FakeLlm(error=BudgetExceeded("cap"))
    with pytest.raises(BudgetExceeded):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))


def test_a_mediawiki_error_envelope_raises_incomplete_not_no_matching_page(
    make_config, make_transport
):
    def handler(request):
        return httpx.Response(
            200, json={"error": {"code": "badvalue", "info": "bad srsearch"}}
        )

    transport = make_transport(handler)
    llm = FakeLlm()
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert llm.calls == []


def test_a_mediawiki_facts_error_envelope_raises_incomplete(
    make_config, make_transport
):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(
            200, json={"error": {"code": "readapidenied", "info": "denied"}}
        )

    transport = make_transport(handler)
    llm = FakeLlm()
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert llm.calls == []


def test_unparseable_search_response_raises_incomplete_not_unhandled(
    make_config, make_transport
):
    def handler(request):
        return httpx.Response(200, text="<html>not json</html>")

    transport = make_transport(handler)
    llm = FakeLlm()
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert llm.calls == []


def test_a_facts_page_missing_pageid_raises_incomplete_not_unhandled(
    make_config, make_transport
):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        page = _page_json(1)
        del page["pageid"]
        return httpx.Response(200, json=_facts_response([page]))

    transport = make_transport(handler)
    llm = FakeLlm()
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert llm.calls == []


def test_facts_request_scales_cllimit_by_batch_size_not_per_page_config(
    make_config, make_transport
):
    captured = []

    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1, 2, 3]))
        captured.append(request)
        return httpx.Response(
            200,
            json=_facts_response([_page_json(1), _page_json(2), _page_json(3)]),
        )

    transport = make_transport(handler)
    llm = FakeLlm(
        result={
            "outcome": "matching_page",
            "selected_page_id": 1,
            "supporting_fact_ids": [],
            "conflicting_fact_ids": [],
            "rationale": "Same person.",
        }
    )
    config = make_config()
    match(_mention(), config, transport, cast(LlmClient, llm))
    assert len(captured) == 1
    expected = min(config.mediawiki.max_categories_per_page * 3, 500)
    assert captured[0].url.params.get("cllimit") == str(expected)
    assert captured[0].url.params.get("exlimit") == "max"


def test_a_validation_failure_raises_incomplete_and_is_not_retried(
    make_config, make_transport
):
    def handler(request):
        if request.url.params.get("list") == "search":
            return httpx.Response(200, json=_search_response([1]))
        return httpx.Response(200, json=_facts_response([_page_json(1)]))

    transport = make_transport(handler)
    llm = FakeLlm(result={"outcome": "matching_page"})  # missing required fields
    with pytest.raises(Incomplete):
        match(_mention(), make_config(), transport, cast(LlmClient, llm))
    assert len(llm.calls) == 1, "a validation failure must not be re-paid for in-run"
