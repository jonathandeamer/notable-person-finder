# Phase 2: Wikipedia Identity Match Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `wiki.py` + `wiki_contract.py` so `pipeline.py` can decide,
for each research-worthy detected mention, whether they already have a
current English Wikipedia biography — and skip coverage research when they
do.

**Architecture:** `wiki.py` performs one bounded MediaWiki retrieval wave
(search → facts → one redirect-resolution pass) over the shared `Transport`
cache, then either short-circuits deterministically or asks
`match_wikipedia_identity` via the shared `LlmClient`. `wiki_contract.py`
holds the pure candidate-assembly rules, wire schema, and domain validation —
no I/O — mirroring the existing `detect.py` / `detect_contract.py` split.
`pipeline.py` gains one call and one `continue`.

**Tech Stack:** Python 3.13, pydantic (strict models), httpx (via the
existing `Transport`), pytest, the MediaWiki Action API, OpenRouter structured
output via the existing `LlmClient`.

## Global Constraints

- Source stays under 3,000 lines total: check with
  `find src -name '*.py' | xargs wc -l | tail -1` before and after.
- No new SQLite table; `wiki.py` reads and writes no durable state.
- `pipeline.py`'s main loop must still fit on one screen.
- Conventional Commits are enforced (`git config core.hooksPath .githooks`).
- Checks before each commit that touches source: `uv run ruff check .`,
  `uv run ruff format .`, `uv run pyright`.
- Tests: `uv run pytest tests/mvp` (live tests are deselected by default).
- Implement on a feature branch off `mvp` (e.g. `feat/wikipedia-identity-match`);
  do not push, open a PR, or target `main` without explicit user
  authorization, per `CLAUDE.md`.
- The authorities for every rule below are
  `docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`,
  `docs/superpowers/specs/2026-08-03-wikipedia-match-design.md`, and
  `docs/findings.md`.

---

## Task 1: Config — task-specific model config and a `[mediawiki]` section

**Files:**
- Modify: `src/notable/config.py`
- Modify: `config/notable.example.toml`
- Modify: `tests/mvp/conftest.py`
- Modify: `tests/mvp/test_config.py`

**Interfaces:**
- Consumes: nothing new.
- Produces: `notable.config.ModelTaskConfig` (fields `model`,
  `max_completion_tokens`, `reasoning_effort`), `notable.config.DetectConfig`
  now subclasses it and adds `max_people`, `max_title_characters`,
  `max_summary_characters`; `notable.config.MediaWikiConfig` (fields
  `endpoint`, `max_candidates`, `max_extract_characters`,
  `max_categories_per_page`, `maxlag_seconds`); `Config.match:
  ModelTaskConfig` and `Config.mediawiki: MediaWikiConfig`, both populated by
  `load_config`. Later tasks import `ModelTaskConfig` and `MediaWikiConfig`
  from `notable.config`.

- [ ] **Step 1: Write the failing config tests**

Replace the `BASE` fixture in `tests/mvp/test_config.py` and add new
assertions and a new test:

```python
BASE = """
schema_version = 1
feeds_file = "feeds.toml"
data_dir = "data"
digest_dir = "digests"

[secrets]
openrouter_api_key = "TEST_OR_KEY"

[transport]
contact_url = "https://example.com/contact"

[cache]
dir = "cache"

[openrouter]
endpoint = "https://openrouter.ai/api/v1"

[tasks.detect_people]
model = "openai/gpt-5.4-mini"

[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"
"""
```

Add to `test_defaults_match_the_spec`:

```python
    assert config.match.model == "openai/gpt-5.4-mini"
    assert config.match.max_completion_tokens == 4096
    assert config.match.reasoning_effort == "low"
    assert config.mediawiki.endpoint == "https://en.wikipedia.org/w/api.php"
    assert config.mediawiki.max_candidates == 15
    assert config.mediawiki.max_extract_characters == 1200
    assert config.mediawiki.max_categories_per_page == 20
    assert config.mediawiki.maxlag_seconds == 5
```

Add a new test:

```python
def test_missing_match_task_is_a_clear_error(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_OR_KEY", "sk-test")
    body = BASE.replace(
        '[tasks.match_wikipedia_identity]\nmodel = "openai/gpt-5.4-mini"\n', ""
    )
    with pytest.raises(ValueError, match="tasks.match_wikipedia_identity"):
        load_config(_write(tmp_path, body))
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_config.py -v`
Expected: failures — `config.match` / `config.mediawiki` don't exist yet, and
`test_missing_match_task_is_a_clear_error` currently passes for the wrong
reason (the whole file is invalid TOML) or errors before the `.match` change
lands. Confirm the new assertions fail with `AttributeError` on `config.match`.

- [ ] **Step 3: Implement the config changes**

In `src/notable/config.py`, add `from typing import Any` to the imports and
`ValidationError` to the pydantic import line:

```python
from pydantic import BaseModel, ConfigDict, Field, ValidationError
```

Replace the `DetectConfig` class with:

```python
class ModelTaskConfig(_Strict):
    """Shared fields for a config-driven model call: one task, one model."""

    model: str = Field(min_length=1)
    # detect_people: raise this with max_people, never independently. At
    # 1024 against max_people 8, responses were cut off mid-string and
    # rejected as malformed. See docs/findings.md.
    max_completion_tokens: int = Field(default=4096, ge=256)
    reasoning_effort: str | None = "low"


class DetectConfig(ModelTaskConfig):
    max_people: int = Field(default=8, ge=1, le=32)
    max_title_characters: int = Field(default=500, ge=1)
    max_summary_characters: int = Field(default=4000, ge=1)
```

Add a new class after `CacheConfig`:

```python
class MediaWikiConfig(_Strict):
    endpoint: str = "https://en.wikipedia.org/w/api.php"
    # Up from the frozen `refactor/rearchitecture` system's 8: findings.md
    # records that value pushed 111 of 118 plans into forced-uncertain
    # truncation, and the fingerprint coupling that blocked raising it there
    # does not exist here.
    max_candidates: int = Field(default=15, ge=1, le=50)
    max_extract_characters: int = Field(default=1200, ge=1)
    max_categories_per_page: int = Field(default=20, ge=0)
    maxlag_seconds: int = Field(default=5, ge=1)
```

Update `Config` — add two fields after `detect`:

```python
class Config(_Strict):
    feeds: tuple[Feed, ...]
    data_dir: Path
    digest_dir: Path
    digest_size: int = Field(default=20, ge=1)
    resurface_after_days: int = Field(default=30, ge=1)
    max_item_attempts: int = Field(default=3, ge=1, le=10)
    transport: TransportConfig
    cache: CacheConfig
    detect: DetectConfig
    match: ModelTaskConfig
    mediawiki: MediaWikiConfig
    openrouter: OpenRouterConfig
    budget_usd: Decimal | None
    openrouter_api_key: str = Field(min_length=1, repr=False)
```

Update `_File` — `tasks` stops being typed per-detect, and a `mediawiki`
section is added:

```python
class _File(_Strict):
    schema_version: int
    feeds_file: str
    data_dir: str = "data"
    digest_dir: str = "digests"
    digest_size: int = 20
    resurface_after_days: int = 30
    max_item_attempts: int = 3
    secrets: _Secrets = _Secrets()
    transport: TransportConfig
    cache: CacheConfig = CacheConfig()
    mediawiki: MediaWikiConfig = MediaWikiConfig()
    openrouter: OpenRouterConfig = OpenRouterConfig()
    budget: _Budget = _Budget()
    tasks: dict[str, dict[str, Any]]
```

Add a helper above `load_config`:

```python
def _task_config(parsed: _File, name: str, model_type: type[Any], path: Path) -> Any:
    raw = parsed.tasks.get(name)
    if raw is None:
        raise ValueError(f"{path} is missing [tasks.{name}]")
    try:
        return model_type.model_validate(raw)
    except ValidationError as error:
        raise ValueError(f"invalid [tasks.{name}] in {path}: {error}") from error
```

In `load_config`, replace:

```python
    detect = parsed.tasks.get("detect_people")
    if detect is None:
        raise ValueError(f"{path} is missing [tasks.detect_people]")
```

with:

```python
    detect = _task_config(parsed, "detect_people", DetectConfig, path)
    match = _task_config(parsed, "match_wikipedia_identity", ModelTaskConfig, path)
```

And in the `Config(...)` call at the end of `load_config`, add two lines:

```python
    return Config(
        feeds=feeds_file.feeds,
        data_dir=(root / parsed.data_dir).resolve(),
        digest_dir=(root / parsed.digest_dir).resolve(),
        digest_size=parsed.digest_size,
        resurface_after_days=parsed.resurface_after_days,
        max_item_attempts=parsed.max_item_attempts,
        transport=parsed.transport,
        cache=parsed.cache.model_copy(
            update={"dir": (root / parsed.cache.dir).resolve()}
        ),
        detect=detect,
        match=match,
        mediawiki=parsed.mediawiki,
        openrouter=parsed.openrouter,
        budget_usd=None if budget is None else Decimal(budget),
        openrouter_api_key=api_key,
    )
```

- [ ] **Step 4: Update `config/notable.example.toml`**

Append after the existing `[tasks.detect_people]` section:

```toml
[tasks.match_wikipedia_identity]
model = "openai/gpt-5.4-mini"
max_completion_tokens = 4096
reasoning_effort = "low"

[mediawiki]
endpoint = "https://en.wikipedia.org/w/api.php"
max_candidates = 15
max_extract_characters = 1200
max_categories_per_page = 20
maxlag_seconds = 5
```

- [ ] **Step 5: Update `tests/mvp/conftest.py`'s `make_config` fixture**

```python
from notable.config import (
    CacheConfig,
    Config,
    DetectConfig,
    Feed,
    MediaWikiConfig,
    ModelTaskConfig,
    OpenRouterConfig,
    TransportConfig,
)
```

```python
@pytest.fixture
def make_config(tmp_path) -> Callable[..., Config]:
    def build(**overrides) -> Config:
        base = Config(
            feeds=(Feed(key="a", label="Feed A", url="https://a.test/rss"),),
            data_dir=tmp_path / "data",
            digest_dir=tmp_path / "digests",
            transport=TRANSPORT,
            cache=CacheConfig(dir=tmp_path / "cache"),
            detect=DetectConfig(model="m"),
            match=ModelTaskConfig(model="m"),
            mediawiki=MediaWikiConfig(),
            openrouter=OpenRouterConfig(),
            budget_usd=None,
            openrouter_api_key="sk-test",
        )
        return base.model_copy(update=overrides) if overrides else base

    return build
```

- [ ] **Step 6: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_config.py -v`
Expected: PASS. Also run `uv run pytest tests/mvp -v` to confirm nothing else
broke (`test_pipeline.py`, `test_detect.py`, etc. all construct `Config`
through `make_config`, which now requires the two new fixture imports to
resolve correctly).

- [ ] **Step 7: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add src/notable/config.py config/notable.example.toml tests/mvp/conftest.py tests/mvp/test_config.py
git commit -m "feat(config): add match_wikipedia_identity task and mediawiki settings"
```

---

## Task 2: `wiki_contract.py` — models, wire schema, domain validation

**Files:**
- Create: `src/notable/wiki_contract.py`
- Test: `tests/mvp/test_wiki_contract.py`

**Interfaces:**
- Consumes: nothing from other new modules.
- Produces: `PageFact` (dataclass: `page_id`, `title`, `namespace`,
  `missing`, `is_redirect`, `is_disambiguation`, `description`, `extract`,
  `categories`), `is_biography_candidate(page: PageFact) -> bool`,
  `CandidateFact` (pydantic: `id`, `page_id`, `field`, `text`), `Candidate`
  (pydantic: `page_id`, `title`, `facts`), `build_candidates(pages:
  tuple[PageFact, ...], *, max_extract_characters: int,
  max_categories_per_page: int) -> tuple[Candidate, ...]`, `MatchOutput`
  (pydantic: `outcome`, `selected_page_id`, `supporting_fact_ids`,
  `conflicting_fact_ids`, `rationale`), `MatchVerdict` (dataclass: `outcome`,
  `selected_page_id`, `rationale`, property `has_page`), `MatchInvalid`
  (exception), `match_schema() -> dict[str, object]`, `validate_match(raw:
  dict, *, candidates: tuple[Candidate, ...], truncated: bool) ->
  MatchOutput`. Task 3 (`wiki.py`) imports all of these except `MatchOutput`
  is used internally by `wiki.py` too, to type the `held` list. Task 4
  (`pipeline.py`) imports `MatchVerdict` only (via `wiki.py`'s return type).

- [ ] **Step 1: Write the failing tests**

Create `tests/mvp/test_wiki_contract.py`:

```python
import pytest

from notable.wiki_contract import (
    Candidate,
    MatchInvalid,
    PageFact,
    build_candidates,
    match_schema,
    validate_match,
)


def _page(**overrides) -> PageFact:
    base = dict(
        page_id=1,
        title="Ana Poy",
        namespace=0,
        missing=False,
        is_redirect=False,
        is_disambiguation=False,
        description="Sculptor",
        extract="Ana Poy is a sculptor.",
        categories=("Sculptors",),
    )
    return PageFact(**(base | overrides))


def _candidates(**overrides) -> tuple[Candidate, ...]:
    return build_candidates(
        (_page(**overrides),), max_extract_characters=1000, max_categories_per_page=20
    )


# -- schema ----------------------------------------------------------------


def test_schema_root_is_an_object_not_a_union():
    schema = match_schema()
    assert schema["type"] == "object"
    assert "anyOf" not in schema


def test_schema_selected_page_id_is_nullable():
    schema = match_schema()
    properties = schema["properties"]
    assert isinstance(properties, dict)
    assert properties["selected_page_id"]["type"] == ["integer", "null"]


def test_schema_forbids_additional_properties():
    schema = match_schema()
    assert schema["additionalProperties"] is False
    assert set(schema["properties"]) == set(schema["required"])


# -- build_candidates --------------------------------------------------------


def test_main_namespace_non_dab_page_becomes_a_candidate():
    candidates = _candidates()
    assert len(candidates) == 1
    assert candidates[0].page_id == 1
    assert {fact.field for fact in candidates[0].facts} == {
        "description",
        "extract",
        "category",
    }


def test_non_main_namespace_page_is_excluded():
    assert _candidates(namespace=1) == ()


def test_disambiguation_page_is_excluded():
    assert _candidates(is_disambiguation=True) == ()


def test_redirect_page_is_excluded():
    assert _candidates(is_redirect=True) == ()


def test_missing_page_is_excluded():
    assert _candidates(missing=True) == ()


def test_duplicate_page_ids_are_deduplicated():
    candidates = build_candidates(
        (_page(), _page()), max_extract_characters=1000, max_categories_per_page=20
    )
    assert len(candidates) == 1


def test_extract_is_bounded():
    candidates = build_candidates(
        (_page(extract="x" * 100),),
        max_extract_characters=10,
        max_categories_per_page=20,
    )
    extract_fact = next(f for f in candidates[0].facts if f.field == "extract")
    assert len(extract_fact.text) == 10


def test_categories_are_bounded():
    page = _page(categories=tuple(f"Cat{i}" for i in range(30)))
    candidates = build_candidates(
        (page,), max_extract_characters=1000, max_categories_per_page=5
    )
    assert len([f for f in candidates[0].facts if f.field == "category"]) == 5


def test_fact_ids_are_unique_across_candidates():
    pages = (_page(page_id=1), _page(page_id=2, title="Bo Li"))
    candidates = build_candidates(
        pages, max_extract_characters=1000, max_categories_per_page=20
    )
    ids = [fact.id for candidate in candidates for fact in candidate.facts]
    assert len(ids) == len(set(ids))


# -- validate_match ----------------------------------------------------------


def _good(**overrides):
    base = {
        "outcome": "matching_page",
        "selected_page_id": 1,
        "supporting_fact_ids": [],
        "conflicting_fact_ids": [],
        "rationale": "Same person.",
    }
    return base | overrides


def test_matching_page_requires_a_valid_selected_page_id():
    candidates = _candidates()
    validate_match(_good(), candidates=candidates, truncated=False)
    with pytest.raises(MatchInvalid, match="selected_page_id"):
        validate_match(
            _good(selected_page_id=None), candidates=candidates, truncated=False
        )
    with pytest.raises(MatchInvalid, match="selected_page_id"):
        validate_match(
            _good(selected_page_id=999), candidates=candidates, truncated=False
        )


def test_non_matching_outcomes_must_leave_selected_page_id_null():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="null"):
        validate_match(
            _good(outcome="uncertain", selected_page_id=1),
            candidates=candidates,
            truncated=False,
        )


def test_truncated_search_forbids_no_matching_page():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="truncated"):
        validate_match(
            _good(outcome="no_matching_page", selected_page_id=None),
            candidates=candidates,
            truncated=True,
        )


def test_uncertain_is_unaffected_by_truncation():
    candidates = _candidates()
    result = validate_match(
        _good(outcome="uncertain", selected_page_id=None),
        candidates=candidates,
        truncated=True,
    )
    assert result.outcome == "uncertain"


def test_unknown_fact_reference_is_rejected():
    candidates = _candidates()
    with pytest.raises(MatchInvalid, match="unknown fact"):
        validate_match(
            _good(supporting_fact_ids=["f99"]), candidates=candidates, truncated=False
        )


def test_structurally_invalid_output_is_rejected_not_raised_raw():
    with pytest.raises(MatchInvalid):
        validate_match({"nonsense": True}, candidates=(), truncated=False)
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_wiki_contract.py -v`
Expected: `ModuleNotFoundError: No module named 'notable.wiki_contract'`.

- [ ] **Step 3: Implement `src/notable/wiki_contract.py`**

```python
"""The match_wikipedia_identity contract: models, wire schema, and domain
validation, plus the pure MediaWiki-facts domain rules (namespace and
disambiguation filtering) that decide what counts as a candidate.

Pure: no I/O, no network client. Every rule here traces to
docs/superpowers/specs/2026-08-03-wikipedia-match-design.md and
docs/findings.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

MATCH_OUTCOMES = ("matching_page", "no_matching_page", "uncertain")
FACT_FIELDS = ("description", "extract", "category")

_MAIN_NAMESPACE = 0


class MatchInvalid(ValueError):
    """Model output that the domain rejects. Carries no supplied content."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True, slots=True)
class PageFact:
    """One MediaWiki page as retrieved: not yet judged a candidate."""

    page_id: int
    title: str
    namespace: int
    missing: bool
    is_redirect: bool
    is_disambiguation: bool
    description: str | None
    extract: str | None
    categories: tuple[str, ...]


def is_biography_candidate(page: PageFact) -> bool:
    """Main namespace, not missing, not a disambiguation page, not a
    redirect. A redirect that survived one resolution pass and is still a
    redirect (or resolved onto a disambiguation / non-main-namespace page)
    is dropped rather than chased further -- see the design doc's "one
    bounded redirect-resolution pass"."""
    return (
        not page.missing
        and page.namespace == _MAIN_NAMESPACE
        and not page.is_disambiguation
        and not page.is_redirect
    )


class CandidateFact(_Strict):
    id: str
    page_id: int
    field: Literal["description", "extract", "category"]
    text: str = Field(min_length=1)


class Candidate(_Strict):
    page_id: int
    title: str = Field(min_length=1, max_length=300)
    facts: tuple[CandidateFact, ...]


def build_candidates(
    pages: tuple[PageFact, ...],
    *,
    max_extract_characters: int,
    max_categories_per_page: int,
) -> tuple[Candidate, ...]:
    """Assemble the final candidate list from every retrieved page.

    Deduplicates by page id -- a redirect's terminal page can be reached
    twice if two different search hits redirected to the same article.
    """
    candidates: list[Candidate] = []
    seen: set[int] = set()
    fact_counter = 0
    for page in pages:
        if not is_biography_candidate(page) or page.page_id in seen:
            continue
        seen.add(page.page_id)
        facts: list[CandidateFact] = []
        if page.description:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="description",
                    text=page.description,
                )
            )
        if page.extract:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="extract",
                    text=page.extract[:max_extract_characters],
                )
            )
        for category in page.categories[:max_categories_per_page]:
            fact_counter += 1
            facts.append(
                CandidateFact(
                    id=f"f{fact_counter}",
                    page_id=page.page_id,
                    field="category",
                    text=category,
                )
            )
        candidates.append(
            Candidate(page_id=page.page_id, title=page.title, facts=tuple(facts))
        )
    return tuple(candidates)


class MatchOutput(_Strict):
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None
    supporting_fact_ids: tuple[str, ...]
    conflicting_fact_ids: tuple[str, ...]
    rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True, slots=True)
class MatchVerdict:
    outcome: Literal["matching_page", "no_matching_page", "uncertain"]
    selected_page_id: int | None
    rationale: str

    @property
    def has_page(self) -> bool:
        return self.outcome == "matching_page"


def _string(**extra: Any) -> dict[str, Any]:
    return {"type": "string", **extra}


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": properties,
        "required": list(properties),
        "additionalProperties": False,
    }


def match_schema() -> dict[str, object]:
    """The wire schema. Root is a plain object -- a root-level `anyOf` is
    rejected under strict mode (docs/findings.md)."""
    return _object(
        {
            "outcome": _string(enum=list(MATCH_OUTCOMES)),
            "selected_page_id": {"type": ["integer", "null"]},
            "supporting_fact_ids": {"type": "array", "items": _string()},
            "conflicting_fact_ids": {"type": "array", "items": _string()},
            "rationale": _string(maxLength=1000),
        }
    )


def validate_match(
    raw: dict[str, Any], *, candidates: tuple[Candidate, ...], truncated: bool
) -> MatchOutput:
    """Parse and apply the rules the wire schema cannot express."""
    try:
        output = MatchOutput.model_validate(raw)
    except ValidationError as error:
        raise MatchInvalid(f"match output failed validation: {error}") from error

    page_ids = {candidate.page_id for candidate in candidates}
    fact_ids = {fact.id for candidate in candidates for fact in candidate.facts}

    # Not expressible in the schema: it relates two root-level properties,
    # and strict mode has no if/then and rejects a root-level union
    # (docs/findings.md) -- the same reason detection's overflow/item_outcome
    # pairing is a validator rule rather than a schema one.
    if output.outcome == "matching_page":
        if output.selected_page_id is None or output.selected_page_id not in page_ids:
            raise MatchInvalid(
                "matching_page requires selected_page_id naming a supplied candidate"
            )
    elif output.selected_page_id is not None:
        raise MatchInvalid(f"{output.outcome} must leave selected_page_id null")

    # A truncated search saw an incomplete candidate universe: "no page
    # exists" would be a false negative manufactured by max_candidates, not
    # by evidence. See the design doc's truncation rule.
    if output.outcome == "no_matching_page" and truncated:
        raise MatchInvalid(
            "no_matching_page is not valid when the search was truncated"
        )

    unknown = (
        set(output.supporting_fact_ids) | set(output.conflicting_fact_ids)
    ) - fact_ids
    if unknown:
        raise MatchInvalid(f"unknown fact reference: {sorted(unknown)}")

    return output
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_wiki_contract.py -v`
Expected: PASS, all tests green.

- [ ] **Step 5: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/notable/wiki_contract.py tests/mvp/test_wiki_contract.py
git commit -m "feat(wiki): add the match_wikipedia_identity contract"
```

---

## Task 3: `wiki.py` — MediaWiki retrieval and the match call

**Files:**
- Create: `src/notable/wiki.py`
- Test: `tests/mvp/test_wiki.py`

**Interfaces:**
- Consumes: `notable.config.Config` (`.mediawiki`, `.match`, `.cache`,
  `.transport`), `notable.detect_contract.DetectedMention`
  (`.exact_name`, `.identity_facts`), `notable.http.Transport.request(...)`,
  `notable.llm.LlmClient.structured(...)`, `notable.errors.Incomplete`,
  `notable.errors.ProviderFailure`, everything from Task 2's
  `notable.wiki_contract`.
- Produces: `match(mention: DetectedMention, config: Config, transport:
  Transport, llm: LlmClient) -> MatchVerdict`. Task 4 (`pipeline.py`) calls
  this exactly as `wiki.match(mention, config, providers.transport,
  providers.llm)`.

- [ ] **Step 1: Add the ported prompt reference and write the failing tests**

The prompt file `src/notable/prompts/match_wikipedia_identity.md` already
exists (ported verbatim in Phase 0) — do not edit it.

Create `tests/mvp/test_wiki.py`:

```python
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
            "search": [
                {"pageid": pid, "ns": 0, "title": f"P{pid}"} for pid in hits
            ]
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
                        1, redirect=True, extract=None, description=None,
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
                            2, redirect=True, extract=None, description=None,
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
                        1, redirect=True, extract=None, description=None,
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
            json=_facts_response(
                [_page_json(1, pageprops={"disambiguation": ""})]
            ),
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
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_wiki.py -v`
Expected: `ModuleNotFoundError: No module named 'notable.wiki'`.

- [ ] **Step 3: Implement `src/notable/wiki.py`**

```python
"""One MediaWiki wave plus at most one model call per mention: does an
English Wikipedia biography already describe this person?"""

from __future__ import annotations

import logging
from functools import cache
from importlib import resources
from typing import Any

from notable.config import Config
from notable.detect_contract import DetectedMention
from notable.errors import Incomplete, ProviderFailure
from notable.http import Transport
from notable.llm import LlmClient
from notable.wiki_contract import (
    Candidate,
    MatchInvalid,
    MatchOutput,
    MatchVerdict,
    PageFact,
    build_candidates,
    match_schema,
    validate_match,
)

logger = logging.getLogger(__name__)


@cache
def _system_prompt() -> str:
    return (
        resources.files("notable.prompts")
        .joinpath("match_wikipedia_identity.md")
        .read_text(encoding="utf-8")
    )


def match(
    mention: DetectedMention, config: Config, transport: Transport, llm: LlmClient
) -> MatchVerdict:
    """Decide whether `mention` already has a current English Wikipedia page.

    Raises `Incomplete` if a MediaWiki call, the model call, or its
    validation fails: the mention's item retries on a later run. Never
    convert a technical failure into a semantic verdict. `BudgetExceeded`
    propagates unchanged -- it ends the whole pass, not just this mention.
    """
    try:
        hit_ids, truncated = _search(mention.exact_name, config, transport)
        pages = _facts(config, transport, hit_ids)
        redirect_ids = tuple(
            page.page_id for page in pages if page.is_redirect and not page.missing
        )
        if redirect_ids:
            resolved = _facts(config, transport, redirect_ids, follow_redirects=True)
            pages = tuple(page for page in pages if not page.is_redirect) + resolved
    except ProviderFailure as error:
        logger.warning(
            "wikipedia retrieval failed for %r: %s", mention.exact_name, error
        )
        raise Incomplete(
            f"wikipedia retrieval failed for {mention.exact_name!r}"
        ) from error

    candidates = build_candidates(
        pages,
        max_extract_characters=config.mediawiki.max_extract_characters,
        max_categories_per_page=config.mediawiki.max_categories_per_page,
    )

    if not candidates:
        # Deterministic short-circuits: no model call is made in either
        # case. See the design doc's "Empty-candidate short-circuit"
        # section -- zero-and-truncated is `uncertain`, never `Incomplete`,
        # because the search is cached and a later run would hit the
        # identical dead end and abandon the item without ever reaching
        # coverage research.
        if truncated:
            return MatchVerdict(
                outcome="uncertain",
                selected_page_id=None,
                rationale="search was truncated and left no safe candidates",
            )
        return MatchVerdict(
            outcome="no_matching_page",
            selected_page_id=None,
            rationale="search returned no biography candidates",
        )

    return _ask_model(mention, candidates, truncated, config, llm)


def _search(
    name: str, config: Config, transport: Transport
) -> tuple[tuple[int, ...], bool]:
    response = transport.request(
        provider="mediawiki",
        method="GET",
        url=config.mediawiki.endpoint,
        params={
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "search",
            "srsearch": name,
            "srlimit": config.mediawiki.max_candidates,
            "maxlag": config.mediawiki.maxlag_seconds,
        },
        ttl_seconds=config.cache.discovery_ttl_seconds,
    )
    data = response.json()
    hits = tuple(hit["pageid"] for hit in data.get("query", {}).get("search", []))
    return hits, "continue" in data


def _facts(
    config: Config,
    transport: Transport,
    page_ids: tuple[int, ...],
    *,
    follow_redirects: bool = False,
) -> tuple[PageFact, ...]:
    if not page_ids:
        return ()
    params: dict[str, Any] = {
        "action": "query",
        "format": "json",
        "formatversion": 2,
        "pageids": "|".join(str(page_id) for page_id in page_ids),
        "prop": "info|description|extracts|categories|pageprops",
        "explaintext": 1,
        "exchars": config.mediawiki.max_extract_characters,
        "cllimit": config.mediawiki.max_categories_per_page,
        "ppprop": "disambiguation",
        "maxlag": config.mediawiki.maxlag_seconds,
    }
    if follow_redirects:
        params["redirects"] = 1
    response = transport.request(
        provider="mediawiki",
        method="GET",
        url=config.mediawiki.endpoint,
        params=params,
        ttl_seconds=config.cache.discovery_ttl_seconds,
    )
    data = response.json()
    return tuple(_parse_page(page) for page in data.get("query", {}).get("pages", []))


def _parse_page(page: dict[str, Any]) -> PageFact:
    categories = tuple(
        category["title"].removeprefix("Category:")
        for category in page.get("categories", [])
    )
    return PageFact(
        page_id=page["pageid"],
        title=page.get("title", ""),
        namespace=page.get("ns", -1),
        missing=bool(page.get("missing", False)),
        is_redirect=bool(page.get("redirect", False)),
        is_disambiguation="disambiguation" in page.get("pageprops", {}),
        description=page.get("description"),
        extract=page.get("extract") or None,
        categories=categories,
    )


def _ask_model(
    mention: DetectedMention,
    candidates: tuple[Candidate, ...],
    truncated: bool,
    config: Config,
    llm: LlmClient,
) -> MatchVerdict:
    payload = {
        "task": "match_wikipedia_identity",
        "mention_name": mention.exact_name,
        "identity_facts": [
            {"kind": fact.kind, "value": fact.value} for fact in mention.identity_facts
        ],
        "candidates": [
            {
                "page_id": candidate.page_id,
                "title": candidate.title,
                "facts": [
                    {"id": fact.id, "field": fact.field, "text": fact.text}
                    for fact in candidate.facts
                ],
            }
            for candidate in candidates
        ],
    }

    held: list[MatchOutput] = []

    def _validate(raw: dict[str, Any]) -> None:
        held.append(validate_match(raw, candidates=candidates, truncated=truncated))

    try:
        llm.structured(
            task="match_wikipedia_identity",
            model=config.match.model,
            system=_system_prompt(),
            user_payload=payload,
            schema=match_schema(),
            max_completion_tokens=config.match.max_completion_tokens,
            reasoning_effort=config.match.reasoning_effort,
            timeout=config.transport.llm_read_timeout_seconds,
            validate=_validate,
        )
    except ProviderFailure as error:
        logger.warning(
            "match_wikipedia_identity failed for %r: %s", mention.exact_name, error
        )
        raise Incomplete(
            f"match_wikipedia_identity failed for {mention.exact_name!r}"
        ) from error
    except MatchInvalid as error:
        logger.warning(
            "match_wikipedia_identity output rejected for %r: %s",
            mention.exact_name,
            error,
        )
        raise Incomplete(
            f"match_wikipedia_identity output rejected for {mention.exact_name!r}"
        ) from error

    output = held[0]
    return MatchVerdict(
        outcome=output.outcome,
        selected_page_id=output.selected_page_id,
        rationale=output.rationale,
    )
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_wiki.py -v`
Expected: PASS, all tests green.

- [ ] **Step 5: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/notable/wiki.py tests/mvp/test_wiki.py
git commit -m "feat(wiki): add MediaWiki retrieval and the Wikipedia match call"
```

---

## Task 4: Wire `wiki.match` into `pipeline.py`

**Files:**
- Modify: `src/notable/pipeline.py`
- Modify: `tests/mvp/test_pipeline.py`

**Interfaces:**
- Consumes: `notable.wiki.match` (Task 3), `notable.wiki_contract.MatchVerdict`
  (Task 2).
- Produces: `pipeline.run` now skips coverage-bound mentions that already
  have a Wikipedia page. No new public names.

- [ ] **Step 1: Write the failing pipeline tests**

In `tests/mvp/test_pipeline.py`, add the import:

```python
from notable.wiki_contract import MatchVerdict
```

Replace the `install` fixture:

```python
_NO_PAGE = MatchVerdict(outcome="no_matching_page", selected_page_id=None, rationale="t")


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

    return apply
```

Add two new tests at the end of the file, before `_boom`:

```python
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
        verdict = MatchVerdict(outcome=outcome, selected_page_id=None, rationale="r")
        install(
            [_item(1)],
            _returning({"https://a.test/1": (_mention("Ana Poy"),)}),
            match_fn=lambda mention, cfg, transport, llm, v=verdict: v,
        )
        path = run(make_config(), store, providers).digest_path
        assert "Ana Poy" in path.read_text("utf-8")
```

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `uv run pytest tests/mvp/test_pipeline.py -v`
Expected: `AttributeError: <module 'notable.pipeline'> does not have the
attribute 'wiki'` from inside the `install` fixture's `monkeypatch.setattr`
call, since `pipeline.py` does not import `wiki` yet.

- [ ] **Step 3: Wire `wiki.match` into `pipeline.py`**

Change the import line:

```python
from notable import detect, digest, feeds, wiki
```

Change the mention loop inside `run`:

```python
            item_entries = []
            try:
                for mention in detect.people_in(item, config, providers.llm):
                    if not mention.research_worthy:
                        continue
                    verdict = wiki.match(
                        mention, config, providers.transport, providers.llm
                    )
                    if verdict.has_page:
                        continue
                    item_entries.append(
                        digest.DigestEntry(
                            identity_key=digest.identity_key(mention.exact_name),
                            display_name=mention.exact_name,
                            source_url=item.url,
                            publisher_label=item.publisher_label,
                            rationale=mention.rationale,
                        )
                    )
            except Incomplete:
                incomplete.append(item.url)  # retried next run, up to a cap
                continue
```

Update the comment above `digest.write` (it still describes the current
state accurately, just extend it):

```python
    # Phase 2 filters out mentions with a matching Wikipedia page but still
    # has no ranking or shortlist: the digest retains every remaining
    # research-worthy mention so the fixture exercises the full corpus.
    # Phase 4 applies digest_size after ranking and duplicate collapse.
```

- [ ] **Step 4: Run the tests and confirm they pass**

Run: `uv run pytest tests/mvp/test_pipeline.py -v`
Expected: PASS, all tests green, including the two new ones.

- [ ] **Step 5: Run the full test suite**

Run: `uv run pytest tests/mvp -v`
Expected: PASS across every file.

- [ ] **Step 6: Lint and typecheck**

Run: `uv run ruff check . && uv run ruff format --check . && uv run pyright`
Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add src/notable/pipeline.py tests/mvp/test_pipeline.py
git commit -m "feat(pipeline): skip mentions with a matching Wikipedia page"
```

---

## Task 5: Verification pass

**Files:** none created or modified — this task only runs checks and records
their output; if any check fails, fix the regression in the file it points to
and re-run before moving on.

- [ ] **Step 1: Confirm the source budget**

Run: `find src -name '*.py' | xargs wc -l | tail -1`
Expected: under 3000. Record the number.

- [ ] **Step 2: Confirm `pipeline.py`'s main loop still fits on one screen**

Run: `wc -l src/notable/pipeline.py` and open the file.
Expected: the `run` function body is still readable without scrolling on a
normal terminal (roughly 60 lines). If it has grown past that, the fix
belongs in `wiki.py`, not in loosening this constraint — re-read
`docs/superpowers/specs/2026-08-03-mvp-core-loop-design.md`'s "readability
gate" section before changing anything.

- [ ] **Step 3: Full test suite, lint, and typecheck**

Run:
```bash
uv run pytest tests/mvp -v
uv run ruff check .
uv run ruff format --check .
uv run pyright
```
Expected: all clean.

- [ ] **Step 4: Confirm no new SQLite table**

Run: `grep -n "CREATE TABLE" src/notable/store.py`
Expected: still exactly four tables (`item`, `surfaced`, `run`, `lead`) —
`wiki.py` introduces none.

- [ ] **Step 5: Record what a live run still needs**

This phase is not done until a live run against the Phase 1 fixture corpus
has been inspected per the master spec's pre-promotion checklist (raw model
responses, validation failures, cache hit/miss counts on a second run,
measured spend, MediaWiki pacing and any 429s) and that run's cache is
promoted to the Phase 3 fixture set. That run costs real money and requires
`.env` with a live `OPENROUTER_API_KEY`, so it is **not** part of this
automated plan — surface this explicitly to the user as the next manual step
rather than marking the phase complete without it. Do not commit anything in
this step.

- [ ] **Step 6: Report**

Summarize for the user: line count, test count, and the outstanding live-run
step from Step 5.

---

## Self-Review Notes

- **Spec coverage:** data flow (Task 3), truncation rule (Task 2 validator +
  Task 3 short-circuit), empty-candidate short-circuits including the
  reviewed zero-and-truncated → `uncertain` fix (Task 3), redirect resolution
  with same-filter re-application (Task 2's `is_biography_candidate` applied
  uniformly to both waves, Task 3's merge logic), contract/schema/config
  (Tasks 1–2), caching via existing `Transport`/discovery TTL (Task 3, no new
  cache code needed), error handling mapping to `Incomplete`/`BudgetExceeded`
  (Task 3), pipeline integration (Task 4), named invariant tests from the
  design doc's table (Task 2 and Task 3 test files cover all twelve rows).
- **Type consistency checked:** `MatchVerdict` is defined once in
  `wiki_contract.py` (Task 2) and imported, never redefined, in `wiki.py`
  (Task 3) and `pipeline.py`/`test_pipeline.py` (Task 4).
  `config.mediawiki`/`config.match` names are consistent across Task 1's
  `config.py` changes, Task 3's `wiki.py`, and every test file.
  `build_candidates`'s keyword arguments
  (`max_extract_characters`, `max_categories_per_page`) match between its
  definition (Task 2) and every call site (Task 2 tests, Task 3's `wiki.py`).
- **No placeholders:** every step above contains complete, runnable code —
  no "add validation" or "similar to Task N" shorthand.
