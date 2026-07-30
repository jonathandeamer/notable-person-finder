"""assess_article contract: schema, validation, fingerprints (K22/K33)."""

from __future__ import annotations

import hashlib
import json

import pytest
from pydantic import ValidationError

from notable_person_finder.config.models import AssessArticleConfig
from notable_person_finder.coverage.assessment import (
    ASSESS_SCHEMA_VERSION,
    CONTENT_TYPES,
    COVERAGE_ADAPTER_VERSION,
    AssessArticleInput,
    AssessArticleOutput,
    AssessFact,
    AssessName,
    AssessValidationError,
    CoveragePersonMaterialView,
    assess_prompt_and_schema_hashes,
    assess_schema,
    assess_task_fingerprint,
    build_assess_input,
    coverage_material_fingerprint,
    coverage_material_payload,
    render_assess_request,
    validate_assess_output,
)
from notable_person_finder.coverage.passages import (
    ArticleViewLike,
    PassageView,
    TextBlock,
    select_passages,
)
from notable_person_finder.coverage.queries import COVERAGE_QUERY_PLAN_VERSION
from notable_person_finder.people.models import (
    AttentionCategory,
    DomainProfileEvidence,
    DomainProfileEvidenceExample,
    IdentityFactKind,
)
from notable_person_finder.providers.article_versions import EXTRACTOR_VERSION


def _config(**changes: object) -> AssessArticleConfig:
    values: dict[str, object] = {
        "max_input_tokens": 16_384,
        "max_completion_tokens": 1024,
        "max_title_characters": 500,
        "max_summary_characters": 4000,
        "max_passage_characters": 6000,
        "max_passage_blocks": 24,
        "opening_block_count": 2,
    }
    values.update(changes)
    return AssessArticleConfig.model_validate(values)


def _profile() -> DomainProfileEvidence:
    return DomainProfileEvidence(
        version=1,
        key="visual-arts-en",
        label="English visual arts",
        language="en",
        attention_examples=(
            DomainProfileEvidenceExample(
                category=AttentionCategory.SIGNIFICANT_RECOGNITION,
                examples=("major art prize",),
            ),
        ),
    )


def _passage_view() -> PassageView:
    selected = select_passages(
        ("Élodie N'Diaye",),
        ArticleViewLike(
            title="Élodie N'Diaye retrospective",
            dek="A survey of sculpture.",
            main_text_blocks=(
                TextBlock(
                    id="b1",
                    text="Élodie N'Diaye presents new bronzes in Paris.",
                ),
            ),
        ),
        _config(),
    )
    return selected


def _supplied(**changes: object) -> AssessArticleInput:
    base = build_assess_input(
        person_id=7,
        display_name="Élodie N'Diaye",
        sourced_names=(
            AssessName(
                exact_name="Élodie N'Diaye",
                match_key="élodie n'diaye",
                kind="professional",
            ),
        ),
        identity_facts=(
            AssessFact(
                local_id="f1",
                kind=IdentityFactKind.PROFESSION_OR_ROLE,
                value="sculptor",
            ),
        ),
        person_article_id=11,
        canonical_article_id=22,
        article_view_id=33,
        screening_rule_id="default.unclassified",
        screening_rule_status="unclassified",
        title="Élodie N'Diaye retrospective",
        dek="A survey of sculpture.",
        byline="Staff",
        published_at="2026-01-01",
        editorial_labels=("Exhibition",),
        passage_view=_passage_view(),
        access_kind="full",
        extraction_quality="full",
        domain_profile=_profile(),
        config=_config(),
    )
    if not changes:
        return base
    return base.model_copy(update=changes)


def _valid_output_payload(
    *,
    passage_ids: tuple[str, ...] | None = None,
    content_types: tuple[str, ...] = ("profile", "review"),
) -> dict[str, object]:
    ids = list(passage_ids) if passage_ids is not None else ["p1"]
    return {
        "person_relation": "same_person",
        "person_relation_passage_ids": ids,
        "person_relation_rationale": "Name and role match the supplied person.",
        "coverage_depth": "significant",
        "coverage_depth_passage_ids": ids,
        "coverage_depth_rationale": "Extended treatment of the career.",
        "content_types": list(content_types),
        "content_types_passage_ids": ids,
        "content_types_rationale": "Exhibition profile with critical review notes.",
        "subject_relationship": "editorially_independent",
        "subject_relationship_passage_ids": ids,
        "subject_relationship_rationale": "Third-party critical coverage.",
        "signals": [
            {
                "kind": "attention",
                "category": "significant_recognition",
                "claim": "Major retrospective survey.",
                "supporting_passage_ids": ids,
            }
        ],
    }


def _person(
    person_id: int = 7, identity_fingerprint: str | None = None
) -> CoveragePersonMaterialView:
    return CoveragePersonMaterialView(
        person_id=person_id,
        identity_fingerprint=identity_fingerprint or ("a" * 64),
    )


def test_schema_version_is_one() -> None:
    assert ASSESS_SCHEMA_VERSION == 1
    rendered = render_assess_request(_supplied())
    assert rendered.schema_version == 1


def test_assessment_contract_does_not_import_trafilatura() -> None:
    """Design: coverage assess contract must not pull Trafilatura into its graph."""
    import inspect

    import notable_person_finder.coverage.assessment as assessment_mod

    source = inspect.getsource(assessment_mod)
    assert "providers.articles" not in source
    assert "trafilatura" not in source
    assert "article_versions" in source
    assert assessment_mod.EXTRACTOR_VERSION == 1


def test_extractor_version_matches_articles_reexport() -> None:
    from notable_person_finder.providers import articles as articles_mod
    from notable_person_finder.providers.article_versions import (
        EXTRACTOR_VERSION as version_constant,
    )

    assert version_constant == EXTRACTOR_VERSION == articles_mod.EXTRACTOR_VERSION == 1


def test_content_types_closed_set_includes_press_release_snake_case() -> None:
    assert "press_release" in CONTENT_TYPES
    assert "press release" not in CONTENT_TYPES
    payload = _valid_output_payload(content_types=("press_release",))
    parsed = validate_assess_output(json.dumps(payload), _supplied())
    assert parsed.content_types == ("press_release",)


def test_valid_output_is_accepted() -> None:
    supplied = _supplied()
    output = validate_assess_output(json.dumps(_valid_output_payload()), supplied)
    assert output.person_relation == "same_person"
    assert output.coverage_depth == "significant"
    assert output.content_types == ("profile", "review")


def test_unseen_passage_ids_are_rejected() -> None:
    payload = _valid_output_payload(passage_ids=("p99",))
    with pytest.raises(AssessValidationError, match="unseen passage id p99"):
        validate_assess_output(json.dumps(payload), _supplied())


def test_duplicate_content_types_are_rejected() -> None:
    payload = _valid_output_payload(content_types=("profile", "profile"))
    with pytest.raises(AssessValidationError, match="duplicates"):
        validate_assess_output(json.dumps(payload), _supplied())


def test_content_types_empty_list_is_rejected() -> None:
    """Design: content_types array length 1–3; empty fails schema."""
    payload = _valid_output_payload(content_types=())
    with pytest.raises(AssessValidationError, match="output schema"):
        validate_assess_output(json.dumps(payload), _supplied())


def test_content_types_four_items_are_rejected() -> None:
    """Design: content_types array length 1–3; four fails schema (maxLength)."""
    payload = _valid_output_payload(
        content_types=("reporting", "profile", "review", "interview")
    )
    with pytest.raises(AssessValidationError, match="output schema"):
        validate_assess_output(json.dumps(payload), _supplied())


def test_unknown_content_type_rejected_by_schema() -> None:
    payload = _valid_output_payload(content_types=("blog_post",))  # type: ignore[arg-type]
    with pytest.raises(AssessValidationError, match="output schema"):
        validate_assess_output(json.dumps(payload), _supplied())


def test_reliability_and_notability_fields_are_rejected() -> None:
    """K22: model never asked for reliability/notability booleans."""
    for forbidden in (
        "accepted_source",
        "reliable_publisher",
        "is_notable",
        "notability",
        "publisher_reliability",
    ):
        payload = _valid_output_payload()
        payload[forbidden] = True
        with pytest.raises(AssessValidationError, match="output schema"):
            validate_assess_output(json.dumps(payload), _supplied())


def test_schema_and_prompt_omit_reliability_notability_fields() -> None:
    """K22: schema has no reliability/notability fields; prompt forbids them."""
    schema_text = json.dumps(assess_schema())
    prompt = render_assess_request(_supplied()).system_prompt
    for token in (
        "accepted_source",
        "reliable_publisher",
        "is_notable",
        "publisher_reliability",
    ):
        assert token not in schema_text
        assert token not in prompt
    assert "notability" not in schema_text
    # Prompt may mention "notability booleans" only as a prohibition.
    assert "notability booleans" in prompt
    properties = assess_schema().get("properties", {})
    assert isinstance(properties, dict)
    for key in properties:
        assert "reliab" not in key
        assert "notable" not in key
        assert "accepted" not in key


def test_prompt_pins_task_rules() -> None:
    prompt = render_assess_request(_supplied()).system_prompt
    assert "press_release" in prompt
    assert "person_relation" in prompt
    assert "Do not emit publisher reliability" in prompt


def test_rendering_hashes_reviewed_prompt_and_schema_version() -> None:
    value = _supplied()
    rendered = render_assess_request(value)
    assert rendered.task == "assess_article"
    assert rendered.schema_version == ASSESS_SCHEMA_VERSION
    assert (
        rendered.prompt_hash
        == hashlib.sha256(rendered.system_prompt.encode("utf-8")).hexdigest()
    )
    expected_schema_envelope = json.dumps(
        {
            "schema": assess_schema(),
            "schema_version": ASSESS_SCHEMA_VERSION,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    assert (
        rendered.schema_hash
        == hashlib.sha256(expected_schema_envelope.encode("utf-8")).hexdigest()
    )
    prompt_hash, schema_hash, schema_version = assess_prompt_and_schema_hashes()
    assert rendered.prompt_hash == prompt_hash
    assert rendered.schema_hash == schema_hash
    assert schema_version == ASSESS_SCHEMA_VERSION


def test_models_are_strict_and_frozen() -> None:
    value = _supplied()
    with pytest.raises(ValidationError):
        AssessArticleInput.model_validate(
            {**value.model_dump(), "outside_knowledge": True}
        )
    with pytest.raises(ValidationError):
        value.__setattr__("display_name", "changed")
    with pytest.raises(ValidationError):
        AssessArticleOutput.model_validate(
            {**_valid_output_payload(), "is_notable": True}
        )


def test_material_fingerprint_includes_reject_altered_query_and_assess_ineligible() -> (
    None
):
    config = _config()
    person = _person()
    policy_fp = "b" * 64
    first = coverage_material_fingerprint(
        person, config, source_policy_fingerprint=policy_fp
    )
    second = coverage_material_fingerprint(
        person, config, source_policy_fingerprint=policy_fp
    )
    assert first == second
    assert len(first) == 64

    payload = coverage_material_payload(
        person, config, source_policy_fingerprint=policy_fp
    )
    assert "reject_altered_query" in payload
    assert "assess_ineligible" in payload
    assert payload["reject_altered_query"] is False
    assert payload["assess_ineligible"] is False

    # Mutate omit reject_altered_query → hash changes.
    without_reject = dict(payload)
    del without_reject["reject_altered_query"]
    omitted = hashlib.sha256(
        json.dumps(
            without_reject, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    assert omitted != first

    # Positive control: flipping reject_altered_query changes fingerprint.
    flipped = coverage_material_fingerprint(
        person,
        _config(reject_altered_query=True),
        source_policy_fingerprint=policy_fp,
    )
    assert flipped != first


def test_material_fingerprint_field_list_is_normative() -> None:
    config = _config()
    person = _person()
    policy_fp = "c" * 64
    prompt_hash, schema_hash, schema_version = assess_prompt_and_schema_hashes()
    material = coverage_material_payload(
        person,
        config,
        source_policy_fingerprint=policy_fp,
        refresh_of_plan_id=None,
        prompt_hash=prompt_hash,
        schema_hash=schema_hash,
        schema_version=schema_version,
    )
    required_keys = {
        "task",
        "adapter_version",
        "person_id",
        "identity_fingerprint",
        "query_plan_version",
        "source_policy_fingerprint",
        "extractor_version",
        "model",
        "parameters",
        "max_input_tokens",
        "max_completion_tokens",
        "retrieval_target",
        "max_exact_forms",
        "max_alias_forms",
        "max_context_forms",
        "search_count",
        "max_offsets_per_form",
        "max_results_per_form",
        "max_eligible_fetches",
        "max_unclassified_fetches",
        "max_passage_characters",
        "max_passage_blocks",
        "opening_block_count",
        "max_title_characters",
        "max_summary_characters",
        "reject_altered_query",
        "assess_ineligible",
        "prompt_hash",
        "schema_hash",
        "schema_version",
        "refresh_of_plan_id",
    }
    assert set(material) == required_keys
    assert material["task"] == "coverage_evidence"
    assert material["adapter_version"] == COVERAGE_ADAPTER_VERSION
    assert material["query_plan_version"] == COVERAGE_QUERY_PLAN_VERSION
    assert material["extractor_version"] == EXTRACTOR_VERSION
    assert material["schema_version"] == ASSESS_SCHEMA_VERSION

    expected = hashlib.sha256(
        json.dumps(
            material, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    assert (
        coverage_material_fingerprint(
            person, config, source_policy_fingerprint=policy_fp
        )
        == expected
    )


@pytest.mark.parametrize(
    "bound_name",
    [
        "retrieval_target",
        "max_exact_forms",
        "max_alias_forms",
        "max_context_forms",
        "search_count",
        "max_offsets_per_form",
        "max_results_per_form",
        "max_eligible_fetches",
        "max_unclassified_fetches",
        "max_passage_characters",
        "max_passage_blocks",
        "opening_block_count",
        "max_title_characters",
        "max_summary_characters",
        "max_input_tokens",
        "max_completion_tokens",
    ],
)
def test_each_material_bound_changes_fingerprint(bound_name: str) -> None:
    person = _person()
    base = _config()
    policy_fp = "d" * 64
    baseline = coverage_material_fingerprint(
        person, base, source_policy_fingerprint=policy_fp
    )
    current = getattr(base, bound_name)
    step = 1 if isinstance(current, int) else current
    if bound_name == "max_passage_characters":
        mutated_value = current + 100
    elif bound_name == "max_input_tokens":
        mutated_value = current + 1000
    else:
        mutated_value = current + step
    mutated = coverage_material_fingerprint(
        person,
        _config(**{bound_name: mutated_value}),
        source_policy_fingerprint=policy_fp,
    )
    assert mutated != baseline


def test_refresh_of_plan_id_anchors_fingerprint() -> None:
    person = _person()
    config = _config()
    policy_fp = "e" * 64
    base = coverage_material_fingerprint(
        person, config, source_policy_fingerprint=policy_fp, refresh_of_plan_id=None
    )
    refreshed = coverage_material_fingerprint(
        person, config, source_policy_fingerprint=policy_fp, refresh_of_plan_id=9
    )
    assert refreshed != base


def test_assess_task_fingerprint_includes_view_and_relation() -> None:
    material = "f" * 64
    first = assess_task_fingerprint(
        material_fingerprint=material, person_article_id=1, article_view_id=2
    )
    second = assess_task_fingerprint(
        material_fingerprint=material, person_article_id=1, article_view_id=2
    )
    assert first == second
    assert len(first) == 64
    other_view = assess_task_fingerprint(
        material_fingerprint=material, person_article_id=1, article_view_id=3
    )
    assert other_view != first
    other_pa = assess_task_fingerprint(
        material_fingerprint=material, person_article_id=9, article_view_id=2
    )
    assert other_pa != first


def test_assess_ineligible_true_rejected_by_config() -> None:
    with pytest.raises(ValidationError, match="assess_ineligible"):
        AssessArticleConfig.model_validate({"assess_ineligible": True})


def test_assess_article_config_defaults() -> None:
    config = AssessArticleConfig()
    assert config.model == "openai/gpt-5.4-mini"
    assert config.retrieval_target == 5
    assert config.max_offsets_per_form == 0
    assert config.reject_altered_query is False
    assert config.assess_ineligible is False
    assert config.coverage_refresh_interval_hours == 720
    assert config.max_passage_blocks == 24
    assert config.max_passage_characters == 6000


def test_build_assess_input_carries_passage_view_metadata() -> None:
    value = _supplied()
    assert value.task == "assess_article"
    assert value.view.kind == "article_passages"
    assert value.view.passage_count == len(value.passages)
    assert value.passages[0].id == "p1"
    assert value.screening.rule_id == "default.unclassified"
