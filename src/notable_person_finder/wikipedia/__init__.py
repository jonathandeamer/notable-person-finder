"""Wikipedia identity matching: plans, MediaWiki observations, and match state."""

from notable_person_finder.wikipedia.matching import (
    MATCH_SCHEMA_VERSION,
    WIKIPEDIA_ADAPTER_VERSION,
    MatchValidationError,
    base_material_fingerprint,
    build_match_input,
    map_model_outcome_to_semantic,
    match_prompt_and_schema_hashes,
    match_schema,
    observation_matches_live_material,
    render_match_request,
    validate_match_output,
)
from notable_person_finder.wikipedia.models import (
    MatchFact,
    MatchName,
    MatchView,
    MatchWikiCandidate,
    MatchWikipediaIdentityInput,
    MatchWikipediaIdentityOutput,
    RenderedMatchRequest,
    WikipediaPersonMaterialView,
)

__all__ = [
    "MATCH_SCHEMA_VERSION",
    "WIKIPEDIA_ADAPTER_VERSION",
    "MatchFact",
    "MatchName",
    "MatchValidationError",
    "MatchView",
    "MatchWikiCandidate",
    "MatchWikipediaIdentityInput",
    "MatchWikipediaIdentityOutput",
    "RenderedMatchRequest",
    "WikipediaPersonMaterialView",
    "base_material_fingerprint",
    "build_match_input",
    "map_model_outcome_to_semantic",
    "match_prompt_and_schema_hashes",
    "match_schema",
    "observation_matches_live_material",
    "render_match_request",
    "validate_match_output",
]
