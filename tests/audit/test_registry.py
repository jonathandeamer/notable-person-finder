from __future__ import annotations

from notable_person_finder.audit.registry import REGISTRY, binding_for


def test_every_registered_handler_task_type_has_a_binding() -> None:
    # The twelve task types the run engine registers today. Kept as a
    # literal so that adding a handler without an audit binding fails here
    # loudly rather than silently producing a blank --attempt view.
    expected = {
        "fetch_feed",
        "inspect_model",
        "detect_people",
        "resolve_person_entity",
        "reconsider_person_entity",
        "mediawiki_search",
        "mediawiki_page_facts",
        "match_wikipedia_identity",
        "brave_web_search",
        "fetch_article",
        "assess_article",
        "aggregate_person_lead",
    }
    assert set(REGISTRY) == expected


def test_the_local_handler_is_present_and_marked_non_external() -> None:
    binding = binding_for("aggregate_person_lead")
    assert binding is not None
    assert binding.external is False
    assert binding.result_table == "lead_assessment"


def test_openrouter_sharing_task_types_resolve_to_distinct_tables() -> None:
    # detect_people, resolve_person_entity, reconsider_person_entity,
    # match_wikipedia_identity and assess_article all run through
    # providers/openrouter.py's single GENERATE_OPERATION constant. Keying
    # the registry on (provider, operation) would collapse these five to one
    # entry; keying on task_type must not.
    tables = {
        task_type: binding_for(task_type).result_table  # type: ignore[union-attr]
        for task_type in (
            "detect_people",
            "resolve_person_entity",
            "reconsider_person_entity",
            "match_wikipedia_identity",
            "assess_article",
        )
    }
    assert tables == {
        "detect_people": "triage_observation",
        "resolve_person_entity": "entity_resolution_observation",
        "reconsider_person_entity": "entity_resolution_observation",
        "match_wikipedia_identity": "wikipedia_identity_observation",
        "assess_article": "person_article_assessment",
    }


def test_all_but_the_local_handler_are_external() -> None:
    external = {name for name, b in REGISTRY.items() if b.external}
    assert len(external) == 11
    assert "aggregate_person_lead" not in external


def test_unknown_task_type_returns_none() -> None:
    assert binding_for("no_such_task") is None
