-- Run-scoped OpenRouter capability observations and immutable person-detection
-- history. This milestone deliberately stores unresolved source mentions, not
-- application person identities.

-- Composite provenance keys prevent an observation from naming an attempt
-- belonging to another run. `attempt.id` remains the ordinary primary key;
-- this redundant unique index exists solely as the parent key for the
-- composite foreign keys below.
CREATE UNIQUE INDEX attempt_id_run_id ON attempt(id, run_id);

CREATE TABLE model_inspection (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER NOT NULL,
    configured_model_id TEXT NOT NULL CHECK (length(configured_model_id) > 0),
    resolved_model_id TEXT NOT NULL CHECK (length(resolved_model_id) > 0),
    routing_fingerprint TEXT NOT NULL CHECK (length(routing_fingerprint) = 64),
    supported_parameters_json TEXT NOT NULL,
    supports_strict_structured_output INTEGER NOT NULL
        CHECK (supports_strict_structured_output IN (0, 1)),
    pricing_usable INTEGER NOT NULL CHECK (pricing_usable IN (0, 1)),
    prompt_unit_price_nano_usd INTEGER
        CHECK (
            prompt_unit_price_nano_usd IS NULL
            OR prompt_unit_price_nano_usd >= 0
        ),
    completion_unit_price_nano_usd INTEGER
        CHECK (
            completion_unit_price_nano_usd IS NULL
            OR completion_unit_price_nano_usd >= 0
        ),
    compatibility TEXT NOT NULL
        CHECK (compatibility IN ('compatible', 'incompatible')),
    inspected_at TEXT NOT NULL CHECK (inspected_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id),
    CHECK (
        compatibility != 'compatible'
        OR supports_strict_structured_output = 1
    ),
    CHECK (
        CASE pricing_usable
            WHEN 1 THEN
                prompt_unit_price_nano_usd IS NOT NULL
                AND completion_unit_price_nano_usd IS NOT NULL
            ELSE
                prompt_unit_price_nano_usd IS NULL
                AND completion_unit_price_nano_usd IS NULL
        END
    )
);

-- One successful inspection for an exact configured model and routing policy
-- is reusable within its run, but never across runs.
CREATE UNIQUE INDEX model_inspection_by_run_model_routing
    ON model_inspection(run_id, configured_model_id, routing_fingerprint);
CREATE INDEX model_inspection_by_attempt ON model_inspection(attempt_id);
CREATE UNIQUE INDEX model_inspection_id_run_id ON model_inspection(id, run_id);

CREATE TABLE triage_observation (
    id INTEGER PRIMARY KEY,
    source_item_id INTEGER NOT NULL REFERENCES source_item(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER,
    model_inspection_id INTEGER,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('completed', 'insufficient_input', 'failed')
    ),
    semantic_outcome TEXT CHECK (
        semantic_outcome IS NULL
        OR semantic_outcome IN ('research_people', 'do_not_research', 'uncertain')
    ),
    canonical_supplied_input_json TEXT NOT NULL,
    validated_output_json TEXT,
    prompt_hash TEXT NOT NULL CHECK (length(prompt_hash) = 64),
    schema_hash TEXT NOT NULL CHECK (length(schema_hash) = 64),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    task_fingerprint TEXT NOT NULL CHECK (length(task_fingerprint) = 64),
    input_truncated INTEGER NOT NULL CHECK (input_truncated IN (0, 1)),
    overflow INTEGER CHECK (overflow IS NULL OR overflow IN (0, 1)),
    rationale TEXT NOT NULL,
    failure_category TEXT CHECK (
        failure_category IS NULL OR failure_category IN (
            'network', 'timeout', 'rate_limit', 'transient_server_error',
            'provider_unavailable', 'authentication', 'configuration',
            'unsupported_capability', 'access_denied', 'unsupported_content',
            'response_too_large', 'malformed_response', 'budget_exhausted',
            'storage', 'internal'
        )
    ),
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id),
    FOREIGN KEY (model_inspection_id, run_id)
        REFERENCES model_inspection(id, run_id),
    -- Completed rows describe validated semantic output. The deterministic
    -- insufficient-input terminal is the sole no-attempt disposition. Failed
    -- model work names its attributable attempt and a typed operational cause.
    CHECK (
        CASE disposition
            WHEN 'completed' THEN
                semantic_outcome IS NOT NULL
                AND attempt_id IS NOT NULL
                AND model_inspection_id IS NOT NULL
                AND validated_output_json IS NOT NULL
                AND overflow IS NOT NULL
                AND failure_category IS NULL
            WHEN 'insufficient_input' THEN
                semantic_outcome IS NULL
                AND attempt_id IS NULL
                AND model_inspection_id IS NULL
                AND validated_output_json IS NULL
                AND overflow IS NULL
                AND failure_category IS NULL
            WHEN 'failed' THEN
                semantic_outcome IS NULL
                AND attempt_id IS NOT NULL
                AND validated_output_json IS NULL
                AND overflow IS NULL
                AND failure_category IS NOT NULL
        END
    )
);

-- A material task result is insert-once. A changed prompt, schema, model,
-- source input, or profile produces a different fingerprint and another row.
CREATE UNIQUE INDEX triage_observation_material_identity
    ON triage_observation(source_item_id, task_fingerprint);
CREATE INDEX triage_observation_by_source_item
    ON triage_observation(source_item_id, id);
CREATE INDEX triage_observation_by_run ON triage_observation(run_id, id);
CREATE INDEX triage_observation_by_attempt ON triage_observation(attempt_id);
CREATE INDEX triage_observation_by_status
    ON triage_observation(disposition, semantic_outcome, id);

-- The pointer is intentionally the only mutable triage state. Repository code
-- replaces it in the same transaction that inserts a new immutable history
-- row and its children.
ALTER TABLE source_item
    ADD COLUMN current_triage_observation_id INTEGER
        REFERENCES triage_observation(id);

CREATE INDEX source_item_by_current_triage
    ON source_item(current_triage_observation_id, id);

-- `ALTER TABLE ... ADD COLUMN` cannot add the composite foreign key needed to
-- express that the pointed-to observation belongs to this same source item.
-- These guards enforce that ownership while retaining the ordinary foreign key
-- above for existence and delete protection.
CREATE TRIGGER source_item_current_triage_owner_on_insert
BEFORE INSERT ON source_item
WHEN NEW.current_triage_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM triage_observation
        WHERE id = NEW.current_triage_observation_id
          AND source_item_id = NEW.id
    )
BEGIN
    SELECT RAISE(ABORT, 'current triage observation belongs to another source item');
END;

CREATE TRIGGER source_item_current_triage_owner_on_update
BEFORE UPDATE OF id, current_triage_observation_id ON source_item
WHEN NEW.current_triage_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM triage_observation
        WHERE id = NEW.current_triage_observation_id
          AND source_item_id = NEW.id
    )
BEGIN
    SELECT RAISE(ABORT, 'current triage observation belongs to another source item');
END;

CREATE TRIGGER triage_observation_preserves_current_owner
BEFORE UPDATE OF id, source_item_id ON triage_observation
WHEN EXISTS (
    SELECT 1
    FROM source_item
    WHERE current_triage_observation_id = OLD.id
      AND id != NEW.source_item_id
)
BEGIN
    SELECT RAISE(ABORT, 'current triage observation ownership is immutable');
END;

CREATE TABLE person_mention (
    id INTEGER PRIMARY KEY,
    triage_observation_id INTEGER NOT NULL REFERENCES triage_observation(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    exact_name TEXT NOT NULL CHECK (length(exact_name) > 0),
    search_name TEXT NOT NULL CHECK (length(search_name) > 0),
    outcome TEXT NOT NULL
        CHECK (outcome IN ('research', 'do_not_research', 'uncertain')),
    supporting_passage_ids_json TEXT NOT NULL,
    rationale TEXT NOT NULL,
    UNIQUE (triage_observation_id, ordinal)
);

CREATE INDEX person_mention_by_observation
    ON person_mention(triage_observation_id, ordinal);

CREATE TABLE mention_identity_fact (
    id INTEGER PRIMARY KEY,
    person_mention_id INTEGER NOT NULL REFERENCES person_mention(id),
    local_id TEXT NOT NULL CHECK (length(local_id) > 0),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'name', 'profession_or_role', 'place', 'nationality',
            'era_or_date', 'work', 'affiliation', 'other'
        )
    ),
    value TEXT NOT NULL CHECK (length(value) > 0),
    supporting_passage_ids_json TEXT NOT NULL,
    UNIQUE (person_mention_id, local_id)
);

CREATE INDEX mention_identity_fact_by_mention
    ON mention_identity_fact(person_mention_id, local_id);

CREATE TABLE mention_signal (
    id INTEGER PRIMARY KEY,
    person_mention_id INTEGER NOT NULL REFERENCES person_mention(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    kind TEXT NOT NULL CHECK (kind IN ('attention', 'caution')),
    category TEXT NOT NULL CHECK (length(category) > 0),
    claim TEXT NOT NULL CHECK (length(claim) > 0),
    supporting_passage_ids_json TEXT NOT NULL,
    grounding TEXT NOT NULL CHECK (
        grounding IN ('source_text', 'domain_profile')
    ),
    UNIQUE (person_mention_id, ordinal)
);

CREATE INDEX mention_signal_by_mention
    ON mention_signal(person_mention_id, ordinal);
