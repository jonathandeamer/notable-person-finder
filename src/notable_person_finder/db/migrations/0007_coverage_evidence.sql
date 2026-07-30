-- Coverage evidence: Brave search plans/forms/observations, source screening,
-- discovery attach, article targets/views, person–article assessments, and
-- article_url_alias kind extension for search_result.
--
-- Mutual REFERENCES between person_article and person_article_assessment are
-- legal at CREATE time (SQLite validates FK targets at DML). Do not set
-- foreign_keys pragma here (it is a no-op inside the migration transaction).
-- Do not alter tables to attach FKs later. No reverse migration.
--
-- article_view.attempt_id uniqueness uses a partial unique index WHERE
-- attempt_id IS NOT NULL. SQLite treats each NULL as distinct under a plain
-- UNIQUE(attempt_id), so multiple snippets-only views with attempt_id IS NULL
-- remain legal either way; the partial index documents the non-null intent.

-- Rebuild article_url_alias so kind may include search_result (m5 / K9).
-- Existing feed_original and redirect_destination rows are preserved.
CREATE TABLE article_url_alias_new (
    id INTEGER PRIMARY KEY,
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    url TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (
        kind IN ('feed_original', 'redirect_destination', 'search_result')
    ),
    first_seen_at TEXT NOT NULL CHECK (first_seen_at GLOB '*Z')
);

INSERT INTO article_url_alias_new (
    id, canonical_article_id, url, kind, first_seen_at
)
SELECT id, canonical_article_id, url, kind, first_seen_at
  FROM article_url_alias;

DROP TABLE article_url_alias;

ALTER TABLE article_url_alias_new RENAME TO article_url_alias;

CREATE INDEX article_url_alias_by_article
    ON article_url_alias(canonical_article_id);

CREATE TABLE person_coverage_plan (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    material_fingerprint TEXT NOT NULL CHECK (length(material_fingerprint) = 64),
    status TEXT NOT NULL CHECK (
        status IN (
            'retrieving',
            'selecting',
            'assessing',
            'completed',
            'failed',
            'incomplete',
            'superseded'
        )
    ),
    refresh_of_plan_id INTEGER REFERENCES person_coverage_plan(id),
    source_policy_fingerprint TEXT NOT NULL
        CHECK (length(source_policy_fingerprint) = 64),
    truncated_unsafe INTEGER NOT NULL CHECK (truncated_unsafe IN (0, 1)),
    partial_retrieval INTEGER NOT NULL CHECK (partial_retrieval IN (0, 1)),
    retrieval_target INTEGER NOT NULL CHECK (retrieval_target >= 0),
    eligible_selected_count INTEGER NOT NULL DEFAULT 0
        CHECK (eligible_selected_count >= 0),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    completed_at TEXT CHECK (completed_at IS NULL OR completed_at GLOB '*Z'),
    failure_category TEXT
);

-- At most one active plan per person and material fingerprint (K17).
CREATE UNIQUE INDEX person_coverage_plan_active
    ON person_coverage_plan(person_id, material_fingerprint)
    WHERE status IN ('retrieving', 'selecting', 'assessing');

CREATE INDEX person_coverage_plan_by_person
    ON person_coverage_plan(person_id, id);

CREATE TABLE coverage_query_form (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL REFERENCES person_coverage_plan(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    stage INTEGER NOT NULL CHECK (stage >= 1),
    variant_kind TEXT NOT NULL CHECK (
        variant_kind IN ('exact', 'exact_obituary', 'alias', 'context')
    ),
    query_text TEXT NOT NULL CHECK (length(query_text) > 0),
    status TEXT NOT NULL CHECK (
        status IN ('pending', 'completed', 'failed')
    ),
    offsets_used INTEGER NOT NULL CHECK (offsets_used >= 0),
    result_count INTEGER CHECK (result_count IS NULL OR result_count >= 0),
    truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
    failure_category TEXT
);

CREATE UNIQUE INDEX coverage_query_form_by_plan_ordinal
    ON coverage_query_form(plan_id, ordinal);

CREATE INDEX coverage_query_form_by_plan
    ON coverage_query_form(plan_id, id);

CREATE TABLE brave_search_observation (
    id INTEGER PRIMARY KEY,
    query_form_id INTEGER NOT NULL REFERENCES coverage_query_form(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER NOT NULL,
    query_text TEXT NOT NULL CHECK (length(query_text) > 0),
    altered_query TEXT,
    offset_in INTEGER NOT NULL CHECK (offset_in >= 0),
    count_requested INTEGER NOT NULL CHECK (count_requested >= 1),
    result_count INTEGER NOT NULL CHECK (result_count >= 0),
    truncated INTEGER NOT NULL CHECK (truncated IN (0, 1)),
    response_complete INTEGER NOT NULL CHECK (response_complete IN (0, 1)),
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id)
);

-- Idempotent persist: one observation per external Brave search attempt.
CREATE UNIQUE INDEX brave_search_observation_by_attempt
    ON brave_search_observation(attempt_id);

CREATE INDEX brave_search_observation_by_query_form
    ON brave_search_observation(query_form_id, id);

CREATE TABLE source_screening (
    id INTEGER PRIMARY KEY,
    canonical_article_id INTEGER REFERENCES canonical_article(id),
    url TEXT NOT NULL,
    publisher_key TEXT,
    rule_id TEXT NOT NULL CHECK (length(rule_id) > 0),
    rule_status TEXT NOT NULL CHECK (
        rule_status IN (
            'curated_eligible',
            'curated_ineligible',
            'unclassified',
            'unusable'
        )
    ),
    source_policy_fingerprint TEXT NOT NULL
        CHECK (length(source_policy_fingerprint) = 64),
    decided_at TEXT NOT NULL CHECK (decided_at GLOB '*Z'),
    plan_id INTEGER REFERENCES person_coverage_plan(id),
    source_item_id INTEGER,
    person_mention_id INTEGER,
    -- Null canonical_article_id only when unusable (K10 unusable discovery /
    -- unusable search URL). Usable statuses require a canonical article.
    CHECK (
        (rule_status = 'unusable' AND canonical_article_id IS NULL)
        OR (rule_status != 'unusable' AND canonical_article_id IS NOT NULL)
    )
);

CREATE INDEX source_screening_by_plan
    ON source_screening(plan_id, id)
    WHERE plan_id IS NOT NULL;

CREATE INDEX source_screening_by_article
    ON source_screening(canonical_article_id, id)
    WHERE canonical_article_id IS NOT NULL;

CREATE TABLE brave_search_result_occurrence (
    id INTEGER PRIMARY KEY,
    search_observation_id INTEGER NOT NULL
        REFERENCES brave_search_observation(id),
    rank INTEGER NOT NULL CHECK (rank >= 1),
    url TEXT NOT NULL CHECK (length(url) > 0),
    title TEXT,
    snippet TEXT,
    extra_snippet TEXT,
    language TEXT,
    provider_result_id TEXT,
    canonical_article_id INTEGER REFERENCES canonical_article(id),
    screening_id INTEGER REFERENCES source_screening(id)
);

CREATE UNIQUE INDEX brave_search_result_occurrence_by_observation_rank
    ON brave_search_result_occurrence(search_observation_id, rank);

CREATE INDEX brave_search_result_occurrence_by_observation
    ON brave_search_result_occurrence(search_observation_id, id);

-- Usable discovery only: screening_id NOT NULL (K10). Unusable discovery
-- URLs live only as source_screening (unusable) with source_item_id set.
CREATE TABLE coverage_discovery_article (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL REFERENCES person_coverage_plan(id),
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    source_item_id INTEGER NOT NULL,
    person_mention_id INTEGER,
    screening_id INTEGER NOT NULL REFERENCES source_screening(id)
);

CREATE UNIQUE INDEX coverage_discovery_article_by_plan_article
    ON coverage_discovery_article(plan_id, canonical_article_id);

CREATE INDEX coverage_discovery_article_by_plan
    ON coverage_discovery_article(plan_id, id);

-- Cleaned extract / snippets only. No HTML / raw body column (K3).
-- Created before coverage_article_target so article_view_id can reference it.
CREATE TABLE article_view (
    id INTEGER PRIMARY KEY,
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER,
    access_kind TEXT NOT NULL CHECK (
        access_kind IN ('full', 'partial', 'snippets')
    ),
    requested_url TEXT,
    final_url TEXT,
    title TEXT,
    dek TEXT,
    byline TEXT,
    published_at TEXT,
    editorial_labels_json TEXT NOT NULL,
    main_text_blocks_json TEXT NOT NULL,
    snippets_json TEXT NOT NULL,
    extraction_quality TEXT,
    extractor_version INTEGER NOT NULL CHECK (extractor_version >= 1),
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id)
);

-- Non-null attempt_ids are unique; multiple NULL attempt_id rows are allowed
-- (snippets-only path). See migration header comment.
CREATE UNIQUE INDEX article_view_by_attempt
    ON article_view(attempt_id)
    WHERE attempt_id IS NOT NULL;

CREATE INDEX article_view_by_article
    ON article_view(canonical_article_id, id);

CREATE TABLE coverage_article_target (
    id INTEGER PRIMARY KEY,
    plan_id INTEGER NOT NULL REFERENCES person_coverage_plan(id),
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    request_url TEXT NOT NULL CHECK (length(request_url) > 0),
    -- K34: source × selection tier (never bare 'discovery').
    selection_reason TEXT NOT NULL CHECK (
        selection_reason IN (
            'discovery_curated_eligible',
            'discovery_unclassified_fallback',
            'search_curated_eligible',
            'search_unclassified_fallback'
        )
    ),
    status TEXT NOT NULL CHECK (
        status IN (
            'pending',
            'fetched',
            'snippets_only',
            'failed',
            'superseded'
        )
    ),
    article_view_id INTEGER REFERENCES article_view(id),
    attempt_id INTEGER,
    failure_category TEXT
);

CREATE UNIQUE INDEX coverage_article_target_by_plan_article
    ON coverage_article_target(plan_id, canonical_article_id);

CREATE INDEX coverage_article_target_by_plan
    ON coverage_article_target(plan_id, id);

CREATE INDEX coverage_article_target_by_view
    ON coverage_article_target(article_view_id)
    WHERE article_view_id IS NOT NULL;

-- person_article is created without current_assessment_id so the pointer can
-- REFERENCES person_article_assessment after that table exists (m3/m4 pattern).
CREATE TABLE person_article (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    first_plan_id INTEGER REFERENCES person_coverage_plan(id)
);

CREATE UNIQUE INDEX person_article_by_person_article
    ON person_article(person_id, canonical_article_id);

CREATE INDEX person_article_by_person
    ON person_article(person_id, id);

CREATE TABLE person_article_assessment (
    id INTEGER PRIMARY KEY,
    person_article_id INTEGER NOT NULL REFERENCES person_article(id),
    person_id INTEGER NOT NULL REFERENCES person(id),
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    plan_id INTEGER REFERENCES person_coverage_plan(id),
    article_view_id INTEGER NOT NULL REFERENCES article_view(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    attempt_id INTEGER,
    model_inspection_id INTEGER,
    disposition TEXT NOT NULL CHECK (
        disposition IN ('completed', 'failed')
    ),
    person_relation TEXT CHECK (
        person_relation IS NULL
        OR person_relation IN (
            'same_person', 'different_person', 'uncertain'
        )
    ),
    coverage_depth TEXT CHECK (
        coverage_depth IS NULL
        OR coverage_depth IN (
            'significant', 'passing', 'uncertain'
        )
    ),
    content_types_json TEXT,
    subject_relationship TEXT CHECK (
        subject_relationship IS NULL
        OR subject_relationship IN (
            'editorially_independent',
            'affiliated',
            'self_published',
            'uncertain'
        )
    ),
    screening_rule_id TEXT NOT NULL CHECK (length(screening_rule_id) > 0),
    screening_rule_status TEXT NOT NULL CHECK (length(screening_rule_status) > 0),
    source_policy_fingerprint TEXT NOT NULL
        CHECK (length(source_policy_fingerprint) = 64),
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
    rationale TEXT NOT NULL CHECK (length(rationale) > 0),
    failure_category TEXT,
    observed_at TEXT NOT NULL CHECK (observed_at GLOB '*Z'),
    FOREIGN KEY (attempt_id, run_id) REFERENCES attempt(id, run_id),
    FOREIGN KEY (model_inspection_id, run_id)
        REFERENCES model_inspection(id, run_id),
    -- Assessment CHECK truth table (design K25 / completed vs failed paths).
    CHECK (
        CASE disposition
            WHEN 'completed' THEN
                failure_category IS NULL
                AND person_relation IS NOT NULL
                AND coverage_depth IS NOT NULL
                AND subject_relationship IS NOT NULL
                AND content_types_json IS NOT NULL
                AND validated_output_json IS NOT NULL
                AND attempt_id IS NOT NULL
                AND model_inspection_id IS NOT NULL
                AND prompt_hash IS NOT NULL
                AND schema_hash IS NOT NULL
                AND schema_version IS NOT NULL
            WHEN 'failed' THEN
                person_relation IS NULL
                AND coverage_depth IS NULL
                AND subject_relationship IS NULL
                AND content_types_json IS NULL
                AND validated_output_json IS NULL
                AND failure_category IS NOT NULL
                AND (
                    attempt_id IS NOT NULL
                    OR failure_category IN ('superseded', 'local_refuse')
                )
        END
    )
);

CREATE UNIQUE INDEX person_article_assessment_by_relation_fingerprint
    ON person_article_assessment(person_article_id, task_fingerprint);

CREATE INDEX person_article_assessment_by_person
    ON person_article_assessment(person_id, id);

CREATE INDEX person_article_assessment_by_plan
    ON person_article_assessment(plan_id, id)
    WHERE plan_id IS NOT NULL;

-- Current pointer FK (design + m3/m4): attach after assessment table exists.
ALTER TABLE person_article
    ADD COLUMN current_assessment_id INTEGER
        REFERENCES person_article_assessment(id);

CREATE TABLE article_assessment_signal (
    id INTEGER PRIMARY KEY,
    assessment_id INTEGER NOT NULL REFERENCES person_article_assessment(id),
    signal_kind TEXT NOT NULL CHECK (
        signal_kind IN ('attention', 'caution')
    ),
    category TEXT NOT NULL CHECK (length(category) > 0),
    claim TEXT NOT NULL CHECK (length(claim) > 0),
    supporting_passage_ids_json TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1)
);

CREATE UNIQUE INDEX article_assessment_signal_by_assessment_kind_ordinal
    ON article_assessment_signal(assessment_id, signal_kind, ordinal);

CREATE INDEX article_assessment_signal_by_assessment
    ON article_assessment_signal(assessment_id, id);

-- Current pointer only for disposition='completed' and matching person_article
-- relation (K25).
CREATE TRIGGER person_article_current_assessment_owner_on_insert
BEFORE INSERT ON person_article
WHEN NEW.current_assessment_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM person_article_assessment
        WHERE id = NEW.current_assessment_id
          AND person_article_id = NEW.id
          AND disposition = 'completed'
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current assessment must be completed for this person_article'
    );
END;

CREATE TRIGGER person_article_current_assessment_owner_on_update
BEFORE UPDATE ON person_article
WHEN NEW.current_assessment_id IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM person_article_assessment
        WHERE id = NEW.current_assessment_id
          AND person_article_id = NEW.id
          AND disposition = 'completed'
    )
BEGIN
    SELECT RAISE(
        ABORT,
        'current assessment must be completed for this person_article'
    );
END;

CREATE TRIGGER person_article_assessment_preserves_current_owner_on_insert
BEFORE INSERT ON person_article_assessment
WHEN EXISTS (
    SELECT 1
    FROM person_article
    WHERE current_assessment_id = NEW.id
      AND (
          id != NEW.person_article_id
          OR NEW.disposition != 'completed'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'current assessment ownership is immutable');
END;

CREATE TRIGGER person_article_assessment_preserves_current_owner
BEFORE UPDATE ON person_article_assessment
WHEN EXISTS (
    SELECT 1
    FROM person_article
    WHERE current_assessment_id = OLD.id
      AND (
          NEW.id != OLD.id
          OR id != NEW.person_article_id
          OR NEW.disposition != 'completed'
      )
)
BEGIN
    SELECT RAISE(ABORT, 'current assessment ownership is immutable');
END;
