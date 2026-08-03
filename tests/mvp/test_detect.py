import pytest

from notable.detect import people_in
from notable.errors import BudgetExceeded, Incomplete, ProviderFailure
from notable.feeds import SourceItem

ITEM = SourceItem(
    url="https://a.test/1",
    title="Sculptor Ana Poy wins prize",
    summary="Ana Poy showed in Paris.",
    published_at=None,
    feed_key="a",
    publisher_label="A",
)

GOOD = {
    "item_outcome": "research_people",
    "mentions": [
        {
            "exact_name": "Ana Poy",
            "outcome": "research",
            "supporting_passage_ids": ["p1"],
            "identity_facts": [],
            "signals": [],
            "rationale": "Named subject.",
        }
    ],
    "overflow": False,
    "rationale": "One subject.",
}


class FakeLlm:
    """Stands in for LlmClient, including its validation contract.

    The real client calls `validate` before committing the response to cache,
    so a fake that ignores it would let `people_in` pass while the production
    path caches unvalidated output.
    """

    def __init__(self, result=None, error=None):
        self.result, self.error, self.calls = result, error, []

    def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        validate = kwargs.get("validate")
        if validate is not None:
            validate(self.result)
        return self.result


def test_the_validator_is_passed_to_the_client_not_applied_after(make_config):
    # Validating the return value instead would cache responses the domain
    # rejects, so every retry replays the same bad answer for free until the
    # item hits its attempt cap.
    llm = FakeLlm(result=GOOD)
    people_in(ITEM, make_config(), llm)
    assert callable(llm.calls[0]["validate"])


def test_returns_validated_mentions(make_config):
    mentions = people_in(ITEM, make_config(), FakeLlm(result=GOOD))
    assert [m.exact_name for m in mentions] == ["Ana Poy"]
    assert mentions[0].research_worthy is True


def test_an_item_with_no_usable_text_makes_no_model_call(make_config):
    llm = FakeLlm(result=GOOD)
    empty = SourceItem(ITEM.url, None, None, None, "a", "A")
    assert people_in(empty, make_config(), llm) == ()
    assert llm.calls == [], "an empty item must not be paid for"


def test_the_call_carries_the_ported_prompt_and_capped_schema(make_config):
    llm = FakeLlm(result=GOOD)
    people_in(ITEM, make_config(), llm)
    call = llm.calls[0]
    assert call["task"] == "detect_people"
    assert "Return strict schema" in call["system"]
    assert call["schema"]["properties"]["mentions"]["maxItems"] == 8
    assert call["max_completion_tokens"] == 4096


def test_a_validation_failure_raises_incomplete_and_is_not_retried(make_config):
    llm = FakeLlm(result={"item_outcome": "research_people"})  # missing fields
    with pytest.raises(Incomplete):
        people_in(ITEM, make_config(), llm)
    assert len(llm.calls) == 1, "a validation failure must not be re-paid for in-run"


def test_a_provider_failure_raises_incomplete(make_config):
    llm = FakeLlm(error=ProviderFailure("boom", permanent=False))
    with pytest.raises(Incomplete):
        people_in(ITEM, make_config(), llm)


def test_budget_exceeded_propagates_rather_than_becoming_incomplete(make_config):
    # BudgetExceeded ends the pass; Incomplete only ends the item.
    llm = FakeLlm(error=BudgetExceeded("cap"))
    with pytest.raises(BudgetExceeded):
        people_in(ITEM, make_config(), llm)
