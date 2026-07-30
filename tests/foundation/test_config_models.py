import pytest
from pydantic import BaseModel, ValidationError

from notable_person_finder.config.models import (
    DetectPeopleConfig,
    DomainProfileConfig,
    FeedsConfig,
    GenerationParameters,
    MainConfig,
    OpenRouterConfig,
    ProviderRoutingConfig,
    ResolvePersonEntityConfig,
    TasksConfig,
)


def _minimal_main() -> dict[str, object]:
    return {
        "schema_version": 1,
        "timezone": "Europe/Paris",
        "feeds_file": "feeds.toml",
        "domain_profile_file": "profile.toml",
    }


def test_model_configuration_accepts_conservative_defaults() -> None:
    config = MainConfig.model_validate(_minimal_main())

    assert config.openrouter == OpenRouterConfig.model_validate({})
    assert config.openrouter.endpoint == "https://openrouter.ai/api/v1"
    assert config.openrouter.routing.allow_fallbacks is True
    assert config.openrouter.routing.data_collection == "deny"
    assert config.openrouter.routing.zdr is True
    assert config.tasks.detect_people.model == "openai/gpt-5.4-mini"
    assert config.tasks.detect_people.max_input_tokens == 4096
    assert config.tasks.detect_people.max_completion_tokens == 1024
    assert config.tasks.detect_people.parameters == GenerationParameters(
        temperature=0.0,
        top_p=1.0,
        reasoning_effort=None,
    )
    assert config.tasks.detect_people.max_people == 8
    assert config.tasks.detect_people.max_title_characters == 500
    assert config.tasks.detect_people.max_summary_characters == 4000
    assert config.tasks.resolve_person_entity.model == "openai/gpt-5.4-mini"
    assert config.tasks.resolve_person_entity.max_input_tokens == 4096
    assert config.tasks.resolve_person_entity.max_completion_tokens == 1024
    assert config.tasks.resolve_person_entity.parameters == GenerationParameters(
        temperature=0.0,
        top_p=1.0,
        reasoning_effort=None,
    )
    assert config.tasks.resolve_person_entity.max_candidates == 8
    assert config.tasks.resolve_person_entity.max_facts_per_candidate == 12
    assert config.tasks.resolve_person_entity.max_names_per_candidate == 8
    assert config.tasks.resolve_person_entity.max_title_characters == 500
    assert config.tasks.resolve_person_entity.max_summary_characters == 4000
    assert GenerationParameters(reasoning_effort=None).reasoning_effort is None


@pytest.mark.parametrize(
    ("value", "field_name"),
    [
        (OpenRouterConfig(), "endpoint"),
        (ProviderRoutingConfig(), "allow_fallbacks"),
        (GenerationParameters(), "temperature"),
        (DetectPeopleConfig(), "model"),
        (ResolvePersonEntityConfig(), "model"),
        (TasksConfig(), "detect_people"),
        (TasksConfig(), "resolve_person_entity"),
    ],
)
def test_model_configuration_models_are_frozen(value: object, field_name: str) -> None:
    with pytest.raises(ValidationError, match="frozen_instance"):
        setattr(value, field_name, None)


@pytest.mark.parametrize(
    ("section", "extra"),
    [
        ("openrouter", {"response_healing": True}),
        ("routing", {"require_parameters": False}),
        ("tasks", {"compose_lead_summary": {}}),
        ("detect_people", {"max_tokens": 1024}),
        ("resolve_person_entity", {"max_tokens": 1024}),
        ("parameters", {"seed": 7}),
        ("resolve_parameters", {"seed": 7}),
    ],
)
def test_model_configuration_rejects_unknown_fields(
    section: str, extra: dict[str, object]
) -> None:
    value = _minimal_main()
    value["openrouter"] = {}
    value["tasks"] = {"detect_people": {}, "resolve_person_entity": {}}
    if section == "openrouter":
        value["openrouter"] = extra
    elif section == "routing":
        value["openrouter"] = {"routing": extra}
    elif section == "tasks":
        value["tasks"] = extra
    elif section == "detect_people":
        value["tasks"] = {"detect_people": extra}
    elif section == "resolve_person_entity":
        value["tasks"] = {"resolve_person_entity": extra}
    elif section == "resolve_parameters":
        value["tasks"] = {"resolve_person_entity": {"parameters": extra}}
    else:
        value["tasks"] = {"detect_people": {"parameters": extra}}

    with pytest.raises(ValidationError, match="extra_forbidden"):
        MainConfig.model_validate(value)


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://openrouter.ai/api/v1",
        "https://localhost/api/v1",
        "https://127.0.0.1/api/v1",
        "https://user:password@openrouter.ai/api/v1",
        "https://openrouter.ai/api/v1?key=secret",
        "https://openrouter.ai/api/v1#fragment",
        " https://openrouter.ai/api/v1",
        "https://openrouter.ai/api/v1\n",
        "https://openrouter.ai:abc/api/v1",
        "https://openrouter.ai:70000/api/v1",
    ],
)
def test_openrouter_endpoint_requires_a_clean_public_https_url(endpoint: str) -> None:
    with pytest.raises(ValidationError):
        OpenRouterConfig(endpoint=endpoint)


@pytest.mark.parametrize(
    ("model_type", "value"),
    [
        (ProviderRoutingConfig, {"allow_fallbacks": 1}),
        (ProviderRoutingConfig, {"zdr": "false"}),
        (OpenRouterConfig, {"endpoint": b"https://openrouter.ai/api/v1"}),
        (GenerationParameters, {"temperature": True}),
        (GenerationParameters, {"top_p": "1.0"}),
        (DetectPeopleConfig, {"model": b"openai/gpt-5.4-mini"}),
        (ResolvePersonEntityConfig, {"model": b"openai/gpt-5.4-mini"}),
        (
            TasksConfig,
            {"detect_people": {"parameters": {"temperature": True}}},
        ),
        (
            TasksConfig,
            {"resolve_person_entity": {"parameters": {"temperature": True}}},
        ),
    ],
)
def test_model_configuration_rejects_type_coercion(
    model_type: type[BaseModel], value: dict[str, object]
) -> None:
    with pytest.raises(ValidationError):
        model_type.model_validate(value)


@pytest.mark.parametrize(
    "model",
    [
        "openrouter/auto",
        "openai/gpt-5.4-mini,anthropic/claude-haiku-4.5",
        "openai/gpt-5.4-mini:free",
        "gpt-5.4-mini",
        "openai/",
        "/gpt-5.4-mini",
        "OpenAI/gpt-5.4-mini",
        "openai/gpt 5.4 mini",
    ],
)
def test_detect_people_rejects_router_aliases_and_invalid_model_slugs(
    model: str,
) -> None:
    with pytest.raises(ValidationError, match="model"):
        DetectPeopleConfig(model=model)


@pytest.mark.parametrize(
    "model",
    [
        "openrouter/auto",
        "openai/gpt-5.4-mini,anthropic/claude-haiku-4.5",
        "openai/gpt-5.4-mini:free",
        "gpt-5.4-mini",
        "OpenAI/gpt-5.4-mini",
    ],
)
def test_resolve_person_entity_rejects_router_aliases_and_invalid_model_slugs(
    model: str,
) -> None:
    with pytest.raises(ValidationError, match="model"):
        ResolvePersonEntityConfig(model=model)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("temperature", float("nan")),
        ("temperature", float("inf")),
        ("temperature", float("-inf")),
        ("top_p", float("nan")),
        ("top_p", float("inf")),
        ("top_p", float("-inf")),
    ],
)
def test_generation_parameters_reject_non_finite_numbers(
    field: str, value: float
) -> None:
    with pytest.raises(ValidationError):
        GenerationParameters.model_validate({field: value})


@pytest.mark.parametrize(
    "values",
    [
        {"max_input_tokens": 0},
        {"max_completion_tokens": 0},
        {"max_input_tokens": 1024, "max_completion_tokens": 1024},
        {"max_input_tokens": 1024, "max_completion_tokens": 2048},
        {"max_title_characters": 1000, "max_summary_characters": 999},
    ],
)
def test_detect_people_rejects_non_positive_or_incompatible_bounds(
    values: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        DetectPeopleConfig.model_validate(values)


@pytest.mark.parametrize(
    "values",
    [
        {"max_people": 0},
        {"max_people": 33},
        {"max_title_characters": 0},
        {"max_title_characters": 2001},
        {"max_summary_characters": 0},
        {"max_summary_characters": 20_001},
    ],
)
def test_detect_people_rejects_out_of_range_mention_and_context_limits(
    values: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        DetectPeopleConfig.model_validate(values)


@pytest.mark.parametrize(
    "values",
    [
        {"max_input_tokens": 0},
        {"max_completion_tokens": 0},
        {"max_input_tokens": 1024, "max_completion_tokens": 1024},
        {"max_input_tokens": 1024, "max_completion_tokens": 2048},
        {"max_title_characters": 1000, "max_summary_characters": 999},
    ],
)
def test_resolve_person_entity_rejects_non_positive_or_incompatible_bounds(
    values: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        ResolvePersonEntityConfig.model_validate(values)


@pytest.mark.parametrize(
    "values",
    [
        {"max_candidates": 0},
        {"max_candidates": 17},
        {"max_facts_per_candidate": 0},
        {"max_facts_per_candidate": 33},
        {"max_names_per_candidate": 0},
        {"max_names_per_candidate": 33},
        {"max_title_characters": 0},
        {"max_title_characters": 2001},
        {"max_summary_characters": 0},
        {"max_summary_characters": 20_001},
    ],
)
def test_resolve_person_entity_rejects_out_of_range_candidate_and_context_limits(
    values: dict[str, int],
) -> None:
    with pytest.raises(ValidationError):
        ResolvePersonEntityConfig.model_validate(values)


def test_main_config_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError, match="extra_forbidden"):
        MainConfig.model_validate(
            {
                "schema_version": 1,
                "timezone": "Europe/Paris",
                "feeds_file": "feeds.toml",
                "domain_profile_file": "profile.toml",
                "unexpected": True,
            }
        )


def test_feeds_require_unique_keys_and_public_http_urls() -> None:
    with pytest.raises(ValidationError, match="duplicate feed key"):
        FeedsConfig.model_validate(
            {
                "schema_version": 1,
                "feeds": [
                    {"key": "art", "label": "Art", "url": "https://example.com/a"},
                    {"key": "art", "label": "Other", "url": "https://example.org/b"},
                ],
            }
        )

    with pytest.raises(ValidationError, match="embedded credentials"):
        FeedsConfig.model_validate(
            {
                "schema_version": 1,
                "feeds": [
                    {
                        "key": "bad",
                        "label": "Bad",
                        "url": "https://u:p@example.com/feed",
                    }
                ],
            }
        )


def test_domain_profile_accepts_only_known_attention_signals() -> None:
    profile = DomainProfileConfig.model_validate(
        {
            "schema_version": 1,
            "key": "visual-arts-en",
            "label": "English visual arts",
            "language": "en",
            "attention_examples": {
                "significant_recognition": ["major art prize"],
                "institutional_recognition": ["permanent museum collection"],
            },
        }
    )

    assert profile.key == "visual-arts-en"
    assert profile.attention_examples["significant_recognition"] == ("major art prize",)
