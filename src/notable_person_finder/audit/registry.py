"""The attempt-to-result registry, keyed on `task_type`.

`audit/` may import only from `config/`, `db/`, and `obs/` within this
package. The twelve task-type strings below are declared as literals rather
than imported from `people/`, `wikipedia/`, `coverage/`, `leads/`,
`ingestion/`, or `runs/` — importing them would breach that layering rule.

The registry key is `work_item.task_type`, not `(provider, operation)`.
`(provider, operation)` is ambiguous: five distinct task types —
`detect_people`, `resolve_person_entity`, `reconsider_person_entity`,
`match_wikipedia_identity`, and `assess_article` — all share the single pair
`(openrouter, generate_structured)`, because `GENERATE_OPERATION` is one
constant in `providers/openrouter.py` reused by every structured-generation
handler. Keying on that pair would collapse those five task types into one
entry and let `--attempt` select the wrong result table. `task_type` is
unique across all twelve registered handlers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ResultBinding:
    task_type: str
    result_table: str
    join_columns: tuple[str, ...]  # columns on the result table
    external: bool
    provenance_columns: tuple[str, ...] = ()


REGISTRY: Mapping[str, ResultBinding] = {
    "fetch_feed": ResultBinding(
        task_type="fetch_feed",
        result_table="feed_fetch",
        join_columns=("feed_identity_id", "run_id"),
        external=True,
    ),
    "inspect_model": ResultBinding(
        task_type="inspect_model",
        result_table="model_inspection",
        join_columns=("attempt_id",),
        external=True,
    ),
    "detect_people": ResultBinding(
        task_type="detect_people",
        result_table="triage_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "resolve_person_entity": ResultBinding(
        task_type="resolve_person_entity",
        result_table="entity_resolution_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "reconsider_person_entity": ResultBinding(
        task_type="reconsider_person_entity",
        result_table="entity_resolution_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "mediawiki_search": ResultBinding(
        task_type="mediawiki_search",
        result_table="mediawiki_search_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "mediawiki_page_facts": ResultBinding(
        task_type="mediawiki_page_facts",
        result_table="wikipedia_page_facts_batch",
        join_columns=("attempt_id",),
        external=True,
    ),
    "match_wikipedia_identity": ResultBinding(
        task_type="match_wikipedia_identity",
        result_table="wikipedia_identity_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "brave_web_search": ResultBinding(
        task_type="brave_web_search",
        result_table="brave_search_observation",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "fetch_article": ResultBinding(
        task_type="fetch_article",
        result_table="article_view",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "assess_article": ResultBinding(
        task_type="assess_article",
        result_table="person_article_assessment",
        join_columns=("attempt_id", "run_id"),
        external=True,
    ),
    "aggregate_person_lead": ResultBinding(
        task_type="aggregate_person_lead",
        result_table="lead_assessment",
        join_columns=("person_id", "run_id"),
        external=False,
    ),
}


def binding_for(task_type: str) -> ResultBinding | None:
    return REGISTRY.get(task_type)
