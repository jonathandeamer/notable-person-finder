-- Durable person identity, sourced names, possible-same-person relations, and
-- immutable entity-resolution observations. Mentions gain optional person_id
-- and a current ER pointer; unresolved mentions remain legal.

-- Table creation order deliberately creates person_relation before
-- entity_resolution_observation so mutual REFERENCES are legal at CREATE time
-- (SQLite validates FK targets at DML). Do not set foreign_keys pragma here
-- (it is a no-op inside the migration transaction). No ALTER TABLE … ADD
-- FOREIGN KEY.

CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    created_by_run_id INTEGER NOT NULL REFERENCES run(id),
    display_name TEXT NOT NULL CHECK (length(display_name) > 0),
    identity_fingerprint TEXT NOT NULL CHECK (length(identity_fingerprint) = 64),
    merged_into_person_id INTEGER REFERENCES person(id),
    CHECK (merged_into_person_id IS NULL OR merged_into_person_id != id)
);

CREATE INDEX person_by_canonical ON person(id) WHERE merged_into_person_id IS NULL;
CREATE INDEX person_by_merged_into
    ON person(merged_into_person_id) WHERE merged_into_person_id IS NOT NULL;

CREATE TABLE sourced_name (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    exact_name TEXT NOT NULL CHECK (length(exact_name) > 0),
    search_name TEXT NOT NULL CHECK (length(search_name) > 0),
    match_key TEXT NOT NULL CHECK (length(match_key) > 0),
    kind TEXT NOT NULL CHECK (
        kind IN ('display', 'professional', 'mononym', 'alias', 'other')
    ),
    origin_kind TEXT NOT NULL CHECK (
        origin_kind IN ('person_mention', 'merge', 'manual')
    ),
    origin_mention_id INTEGER REFERENCES person_mention(id),
    first_observed_at TEXT NOT NULL CHECK (first_observed_at GLOB '*Z'),
    last_observed_at TEXT NOT NULL CHECK (last_observed_at GLOB '*Z')
);
-- No uniqueness on exact_name or match_key across people.
CREATE INDEX sourced_name_by_match_key ON sourced_name(match_key, person_id);
CREATE INDEX sourced_name_by_person ON sourced_name(person_id, id);
CREATE INDEX sourced_name_by_exact ON sourced_name(exact_name, person_id);

CREATE TABLE person_relation (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('possible_same_person', 'merge')),
    person_id_a INTEGER NOT NULL REFERENCES person(id),
    person_id_b INTEGER NOT NULL REFERENCES person(id),
    status TEXT NOT NULL CHECK (
        status IN ('active', 'dismissed', 'superseded_by_merge')
    ),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    created_by_run_id INTEGER NOT NULL REFERENCES run(id),
    -- Mutual REFERENCES with entity_resolution_observation are legal at CREATE
    -- time (SQLite validates FK targets at DML, not DDL; see Table creation
    -- order). Insert protocol below keeps runtime FK checks satisfied.
    created_by_observation_id INTEGER
        REFERENCES entity_resolution_observation(id),
    closed_at TEXT CHECK (closed_at IS NULL OR closed_at GLOB '*Z'),
    closed_by_observation_id INTEGER
        REFERENCES entity_resolution_observation(id),
    CHECK (person_id_a != person_id_b),
    CHECK (kind != 'possible_same_person' OR person_id_a < person_id_b),
    CHECK (kind != 'merge' OR status = 'active')
);

CREATE UNIQUE INDEX person_relation_active_possible
    ON person_relation(person_id_a, person_id_b)
    WHERE kind = 'possible_same_person' AND status = 'active';

CREATE TABLE entity_resolution_observation (
    id INTEGER PRIMARY KEY,
    person_mention_id INTEGER REFERENCES person_mention(id),
    person_relation_id INTEGER REFERENCES person_relation(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER,
    model_inspection_id INTEGER,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('completed', 'failed', 'skipped')
    ),
    semantic_outcome TEXT CHECK (
        semantic_outcome IS NULL OR semantic_outcome IN (
            'same_person', 'different_people', 'uncertain', 'created_new'
        )
    ),
    selected_person_id INTEGER REFERENCES person(id),
    created_person_id INTEGER REFERENCES person(id),
    candidate_person_ids_json TEXT NOT NULL,
    canonical_supplied_input_json TEXT NOT NULL,
    validated_output_json TEXT,
    prompt_hash TEXT NOT NULL CHECK (length(prompt_hash) = 64),
    schema_hash TEXT NOT NULL CHECK (length(schema_hash) = 64),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    task_fingerprint TEXT NOT NULL CHECK (length(task_fingerprint) = 64),
    supporting_fact_ids_json TEXT,
    conflicting_fact_ids_json TEXT,
    rationale TEXT NOT NULL,
    failure_category TEXT,
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id),
    FOREIGN KEY (model_inspection_id, run_id)
        REFERENCES model_inspection(id, run_id),
    -- Exactly one subject. skipped/first-pass use mention; reconsider uses relation.
    CHECK (
        (person_mention_id IS NOT NULL AND person_relation_id IS NULL)
        OR (person_mention_id IS NULL AND person_relation_id IS NOT NULL)
    ),
    -- K22: completed semantics branch on subject kind. First-pass (mention)
    -- creates people for different_people/uncertain/created_new. Reconsider
    -- (relation) never creates a person — different_people dismisses the edge,
    -- uncertain leaves it active, same_person may merge.
    CHECK (
        CASE disposition
            WHEN 'completed' THEN
                semantic_outcome IS NOT NULL
                AND failure_category IS NULL
                AND validated_output_json IS NOT NULL
                AND (
                    CASE
                        WHEN person_mention_id IS NOT NULL THEN
                            -- First-pass / schedule-time create
                            CASE semantic_outcome
                                WHEN 'created_new' THEN
                                    attempt_id IS NULL
                                    AND model_inspection_id IS NULL
                                    AND selected_person_id IS NULL
                                    AND created_person_id IS NOT NULL
                                    AND candidate_person_ids_json = '[]'
                                WHEN 'same_person' THEN
                                    attempt_id IS NOT NULL
                                    AND model_inspection_id IS NOT NULL
                                    AND selected_person_id IS NOT NULL
                                    AND created_person_id IS NULL
                                    AND candidate_person_ids_json != '[]'
                                WHEN 'different_people' THEN
                                    attempt_id IS NOT NULL
                                    AND model_inspection_id IS NOT NULL
                                    AND selected_person_id IS NULL
                                    AND created_person_id IS NOT NULL
                                    AND candidate_person_ids_json != '[]'
                                WHEN 'uncertain' THEN
                                    attempt_id IS NOT NULL
                                    AND model_inspection_id IS NOT NULL
                                    AND selected_person_id IS NULL
                                    AND created_person_id IS NOT NULL
                                    AND candidate_person_ids_json != '[]'
                            END
                        WHEN person_relation_id IS NOT NULL THEN
                            -- Reconsider: no person creation; model path only
                            semantic_outcome IN (
                                'same_person', 'different_people', 'uncertain'
                            )
                            AND attempt_id IS NOT NULL
                            AND model_inspection_id IS NOT NULL
                            AND created_person_id IS NULL
                            AND candidate_person_ids_json != '[]'
                            AND (
                                CASE semantic_outcome
                                    WHEN 'same_person' THEN
                                        selected_person_id IS NOT NULL
                                    ELSE
                                        selected_person_id IS NULL
                                END
                            )
                    END
                )
            WHEN 'skipped' THEN
                -- Unusable match_key / inadequate identity at resolution time.
                -- Mirrors triage insufficient_input: durable, no attempt, no person.
                semantic_outcome IS NULL
                AND attempt_id IS NULL
                AND model_inspection_id IS NULL
                AND selected_person_id IS NULL
                AND created_person_id IS NULL
                AND validated_output_json IS NULL
                AND failure_category IS NULL
                AND candidate_person_ids_json = '[]'
                AND person_mention_id IS NOT NULL
                AND person_relation_id IS NULL
            WHEN 'failed' THEN
                -- attempt_id is the generate attempt on model permanent fail,
                -- or the inspection attempt_id when written by the out-of-band
                -- permanent-preflight settler (K23 / 3b1 pattern).
                semantic_outcome IS NULL
                AND attempt_id IS NOT NULL
                AND failure_category IS NOT NULL
                AND validated_output_json IS NULL
                AND selected_person_id IS NULL
                AND created_person_id IS NULL
        END
    )
);

CREATE UNIQUE INDEX entity_resolution_mention_material
    ON entity_resolution_observation(person_mention_id, task_fingerprint)
    WHERE person_mention_id IS NOT NULL;

CREATE UNIQUE INDEX entity_resolution_relation_material
    ON entity_resolution_observation(person_relation_id, task_fingerprint)
    WHERE person_relation_id IS NOT NULL;

ALTER TABLE person_mention
    ADD COLUMN person_id INTEGER REFERENCES person(id);
ALTER TABLE person_mention
    ADD COLUMN current_entity_resolution_observation_id INTEGER
        REFERENCES entity_resolution_observation(id);

CREATE INDEX person_mention_by_person
    ON person_mention(person_id, id) WHERE person_id IS NOT NULL;

-- The pointer is the only mutable entity-resolution state on a mention.
-- Ownership must match first-pass observations (person_mention_id = mention.id).
-- Relation-scoped reconsider rows (person_mention_id IS NULL) may never be current.
CREATE TRIGGER person_mention_current_er_owner_on_insert
BEFORE INSERT ON person_mention
WHEN NEW.current_entity_resolution_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM entity_resolution_observation
        WHERE id = NEW.current_entity_resolution_observation_id
          AND person_mention_id = NEW.id
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current entity resolution observation belongs to another person mention'
    );
END;

CREATE TRIGGER person_mention_current_er_owner_on_update
BEFORE UPDATE ON person_mention
WHEN NEW.current_entity_resolution_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM entity_resolution_observation
        WHERE id = NEW.current_entity_resolution_observation_id
          AND person_mention_id = NEW.id
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current entity resolution observation belongs to another person mention'
    );
END;

CREATE TRIGGER entity_resolution_preserves_current_owner_on_insert
BEFORE INSERT ON entity_resolution_observation
WHEN EXISTS (
    SELECT 1
    FROM person_mention
    WHERE current_entity_resolution_observation_id = NEW.id
      AND id != NEW.person_mention_id
)
BEGIN
    SELECT RAISE(ABORT, 'current entity resolution ownership is immutable');
END;

CREATE TRIGGER entity_resolution_preserves_current_owner
BEFORE UPDATE ON entity_resolution_observation
WHEN EXISTS (
    SELECT 1
    FROM person_mention
    WHERE current_entity_resolution_observation_id = OLD.id
      AND (NEW.id != OLD.id OR id != NEW.person_mention_id)
)
BEGIN
    SELECT RAISE(ABORT, 'current entity resolution ownership is immutable');
END;
