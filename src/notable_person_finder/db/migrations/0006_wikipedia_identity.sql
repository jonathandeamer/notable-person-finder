-- Wikipedia identity: MediaWiki pages/searches, retrieval plans, page-facts
-- batches, and immutable Wikipedia identity observations with a completed-only
-- current pointer on person.
--
-- Mutual REFERENCES between wikipedia_identity_plan and
-- wikipedia_identity_observation are legal at CREATE time (SQLite validates FK
-- targets at DML). Do not set foreign_keys pragma here (it is a no-op inside
-- the migration transaction). Do not alter tables to attach FKs later. No
-- reverse migration.

CREATE TABLE mediawiki_page (
    id INTEGER PRIMARY KEY,
    wiki_id TEXT NOT NULL DEFAULT 'enwiki' CHECK (length(wiki_id) > 0),
    page_id INTEGER NOT NULL,
    canonical_title TEXT NOT NULL CHECK (length(canonical_title) > 0),
    canonical_url TEXT NOT NULL CHECK (length(canonical_url) > 0),
    namespace INTEGER NOT NULL,
    is_disambiguation INTEGER NOT NULL CHECK (is_disambiguation IN (0, 1)),
    is_missing INTEGER NOT NULL CHECK (is_missing IN (0, 1)),
    redirect_to_page_id INTEGER,
    description TEXT,
    extract TEXT,
    categories_json TEXT NOT NULL,
    last_observed_at TEXT NOT NULL CHECK (last_observed_at GLOB '*Z'),
    last_attempt_id INTEGER REFERENCES attempt(id)
);

CREATE UNIQUE INDEX mediawiki_page_by_wiki_page
    ON mediawiki_page(wiki_id, page_id);

CREATE TABLE wikipedia_identity_plan (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    material_fingerprint TEXT NOT NULL CHECK (length(material_fingerprint) = 64),
    status TEXT NOT NULL CHECK (
        status IN (
            'retrieving',
            'ready_for_match',
            'completed',
            'failed',
            'superseded'
        )
    ),
    refresh_of_observation_id INTEGER
        REFERENCES wikipedia_identity_observation(id),
    truncated_unsafe_for_negative INTEGER NOT NULL
        CHECK (truncated_unsafe_for_negative IN (0, 1)),
    partial_retrieval INTEGER NOT NULL CHECK (partial_retrieval IN (0, 1)),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    completed_at TEXT CHECK (completed_at IS NULL OR completed_at GLOB '*Z'),
    failure_category TEXT
);

-- At most one active plan per person and material fingerprint (K19).
CREATE UNIQUE INDEX wikipedia_identity_plan_active
    ON wikipedia_identity_plan(person_id, material_fingerprint)
    WHERE status IN ('retrieving', 'ready_for_match');

CREATE INDEX wikipedia_identity_plan_by_person
    ON wikipedia_identity_plan(person_id, id);

CREATE TABLE wikipedia_query_form (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL REFERENCES wikipedia_identity_plan(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    variant_kind TEXT NOT NULL CHECK (
        variant_kind IN ('exact', 'comma_swap', 'accent_fallback')
    ),
    query_text TEXT NOT NULL CHECK (length(query_text) > 0),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'completed', 'failed')
    ),
    continuations_used INTEGER NOT NULL CHECK (continuations_used >= 0),
    hit_count INTEGER CHECK (hit_count IS NULL OR hit_count >= 0),
    truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
    failure_category TEXT
);

CREATE UNIQUE INDEX wikipedia_query_form_by_plan_ordinal
    ON wikipedia_query_form(plan_id, ordinal);

CREATE INDEX wikipedia_query_form_by_plan
    ON wikipedia_query_form(plan_id, id);

CREATE TABLE wikipedia_page_facts_batch (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL REFERENCES wikipedia_identity_plan(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    page_ids_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'completed', 'failed', 'superseded')
    ),
    wave INTEGER NOT NULL CHECK (wave >= 1),
    attempt_id INTEGER,
    failure_category TEXT,
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    completed_at TEXT CHECK (completed_at IS NULL OR completed_at GLOB '*Z')
);

CREATE UNIQUE INDEX wikipedia_page_facts_batch_by_plan_ordinal
    ON wikipedia_page_facts_batch(plan_id, ordinal);

-- One batch per settled attempt when attempt_id is set; multiple NULL pending.
CREATE UNIQUE INDEX wikipedia_page_facts_batch_by_attempt
    ON wikipedia_page_facts_batch(attempt_id)
    WHERE attempt_id IS NOT NULL;

CREATE INDEX wikipedia_page_facts_batch_by_plan
    ON wikipedia_page_facts_batch(plan_id, id);

CREATE TABLE mediawiki_search_observation (
    id INTEGER PRIMARY KEY,
    query_form_id INTEGER NOT NULL REFERENCES wikipedia_query_form(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER NOT NULL,
    query_text TEXT NOT NULL CHECK (length(query_text) > 0),
    continuation_in TEXT,
    continuation_out TEXT,
    srlimit INTEGER NOT NULL CHECK (srlimit >= 1),
    hit_count INTEGER NOT NULL CHECK (hit_count >= 0),
    truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
    response_complete INTEGER NOT NULL CHECK (response_complete IN (0, 1)),
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id)
);

-- Idempotent persist: one observation per external search attempt.
CREATE UNIQUE INDEX mediawiki_search_observation_by_attempt
    ON mediawiki_search_observation(attempt_id);

CREATE INDEX mediawiki_search_observation_by_query_form
    ON mediawiki_search_observation(query_form_id, id);

CREATE TABLE mediawiki_search_hit (
    id INTEGER PRIMARY KEY,
    search_observation_id INTEGER NOT NULL
        REFERENCES mediawiki_search_observation(id),
    rank INTEGER NOT NULL CHECK (rank >= 1),
    page_id INTEGER,
    title TEXT NOT NULL CHECK (length(title) > 0)
);

CREATE UNIQUE INDEX mediawiki_search_hit_by_observation_rank
    ON mediawiki_search_hit(search_observation_id, rank);

CREATE TABLE wikipedia_identity_observation (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    plan_id INTEGER REFERENCES wikipedia_identity_plan(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER,
    model_inspection_id INTEGER,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('completed', 'failed')
    ),
    semantic_outcome TEXT CHECK (
        semantic_outcome IS NULL
        OR semantic_outcome IN (
            'matching_page_found',
            'no_matching_page_found',
            'uncertain_identity'
        )
    ),
    matched_mediawiki_page_id INTEGER REFERENCES mediawiki_page(id),
    candidate_page_ids_json TEXT NOT NULL,
    canonical_supplied_input_json TEXT NOT NULL,
    validated_output_json TEXT,
    prompt_hash TEXT CHECK (
        prompt_hash IS NULL OR length(prompt_hash) = 64
    ),
    schema_hash TEXT CHECK (
        schema_hash IS NULL OR length(schema_hash) = 64
    ),
    schema_version INTEGER CHECK (
        schema_version IS NULL OR schema_version >= 1
    ),
    task_fingerprint TEXT NOT NULL CHECK (length(task_fingerprint) = 64),
    supporting_fact_ids_json TEXT,
    conflicting_fact_ids_json TEXT,
    rationale TEXT NOT NULL,
    failure_category TEXT,
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id),
    FOREIGN KEY (model_inspection_id, run_id)
        REFERENCES model_inspection(id, run_id),
    -- Outcome truth table (design K4/K6/K25/K26).
    CHECK (
        CASE disposition
            WHEN 'completed' THEN
                semantic_outcome IS NOT NULL
                AND failure_category IS NULL
                AND validated_output_json IS NOT NULL
                AND (
                    CASE semantic_outcome
                        WHEN 'no_matching_page_found' THEN
                            (
                                -- Deterministic empty complete search (K4)
                                attempt_id IS NULL
                                AND model_inspection_id IS NULL
                                AND matched_mediawiki_page_id IS NULL
                                AND candidate_page_ids_json = '[]'
                                AND prompt_hash IS NULL
                                AND schema_hash IS NULL
                                AND schema_version IS NULL
                            )
                            OR (
                                -- Model no_matching_page when NOT truncated_unsafe
                                attempt_id IS NOT NULL
                                AND model_inspection_id IS NOT NULL
                                AND matched_mediawiki_page_id IS NULL
                                AND candidate_page_ids_json != '[]'
                                AND prompt_hash IS NOT NULL
                                AND schema_hash IS NOT NULL
                                AND schema_version IS NOT NULL
                            )
                        WHEN 'matching_page_found' THEN
                            attempt_id IS NOT NULL
                            AND model_inspection_id IS NOT NULL
                            AND matched_mediawiki_page_id IS NOT NULL
                            AND candidate_page_ids_json != '[]'
                            AND prompt_hash IS NOT NULL
                            AND schema_hash IS NOT NULL
                            AND schema_version IS NOT NULL
                        WHEN 'uncertain_identity' THEN
                            attempt_id IS NOT NULL
                            AND model_inspection_id IS NOT NULL
                            AND matched_mediawiki_page_id IS NULL
                            AND candidate_page_ids_json != '[]'
                            AND prompt_hash IS NOT NULL
                            AND schema_hash IS NOT NULL
                            AND schema_version IS NOT NULL
                    END
                )
            WHEN 'failed' THEN
                semantic_outcome IS NULL
                AND failure_category IS NOT NULL
                AND validated_output_json IS NULL
                AND matched_mediawiki_page_id IS NULL
                AND (
                    attempt_id IS NOT NULL
                    OR failure_category IN (
                        'unsafe_truncation',
                        'partial_retrieval_empty',
                        'redirect_budget_exhausted'
                    )
                )
        END
    )
);

CREATE UNIQUE INDEX wikipedia_identity_observation_by_person_fingerprint
    ON wikipedia_identity_observation(person_id, task_fingerprint);

CREATE INDEX wikipedia_identity_observation_by_person
    ON wikipedia_identity_observation(person_id, id);

CREATE INDEX wikipedia_identity_observation_by_plan
    ON wikipedia_identity_observation(plan_id, id)
    WHERE plan_id IS NOT NULL;

ALTER TABLE person
    ADD COLUMN current_wikipedia_identity_observation_id INTEGER
        REFERENCES wikipedia_identity_observation(id);

-- Current pointer only for disposition='completed' and matching person (K25).
CREATE TRIGGER person_current_wikipedia_obs_owner_on_insert
BEFORE INSERT ON person
WHEN NEW.current_wikipedia_identity_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM wikipedia_identity_observation
        WHERE id = NEW.current_wikipedia_identity_observation_id
          AND person_id = NEW.id
          AND disposition = 'completed'
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current wikipedia identity observation must be completed for this person'
    );
END;

CREATE TRIGGER person_current_wikipedia_obs_owner_on_update
BEFORE UPDATE ON person
WHEN NEW.current_wikipedia_identity_observation_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM wikipedia_identity_observation
        WHERE id = NEW.current_wikipedia_identity_observation_id
          AND person_id = NEW.id
          AND disposition = 'completed'
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current wikipedia identity observation must be completed for this person'
    );
END;

CREATE TRIGGER wikipedia_identity_preserves_current_owner_on_insert
BEFORE INSERT ON wikipedia_identity_observation
WHEN EXISTS (
    SELECT 1
    FROM person
    WHERE current_wikipedia_identity_observation_id = NEW.id
      AND (
          id != NEW.person_id
          OR NEW.disposition != 'completed'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'current wikipedia identity ownership is immutable');
END;

CREATE TRIGGER wikipedia_identity_preserves_current_owner
BEFORE UPDATE ON wikipedia_identity_observation
WHEN EXISTS (
    SELECT 1
    FROM person
    WHERE current_wikipedia_identity_observation_id = OLD.id
      AND (
          NEW.id != OLD.id
          OR id != NEW.person_id
          OR NEW.disposition != 'completed'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'current wikipedia identity ownership is immutable');
END;
