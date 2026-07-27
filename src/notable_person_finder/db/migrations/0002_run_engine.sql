CREATE TABLE run (
    id INTEGER PRIMARY KEY,
    state TEXT NOT NULL CHECK (
        state IN ('running', 'complete', 'partial', 'failed', 'interrupted')
    ),
    configuration_snapshot_id INTEGER NOT NULL
        REFERENCES configuration_snapshot(id),
    timezone TEXT NOT NULL,
    window_start TEXT NOT NULL CHECK (window_start GLOB '*Z'),
    window_end TEXT NOT NULL CHECK (window_end GLOB '*Z'),
    started_at TEXT NOT NULL CHECK (started_at GLOB '*Z'),
    finished_at TEXT CHECK (finished_at IS NULL OR finished_at GLOB '*Z'),
    budget_limit_nano_usd INTEGER
        CHECK (budget_limit_nano_usd IS NULL OR budget_limit_nano_usd >= 0),
    budget_reserved_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (budget_reserved_nano_usd >= 0),
    budget_actual_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (budget_actual_nano_usd >= 0),
    digest_path TEXT,
    digest_sha256 TEXT
        CHECK (digest_sha256 IS NULL OR length(digest_sha256) = 64),
    -- A run is running exactly while it has no finish time. This makes the
    -- interrupted-run sweep a pure state query rather than a heuristic.
    CHECK ((state = 'running') = (finished_at IS NULL))
);

CREATE TABLE run_transition (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    state TEXT NOT NULL CHECK (
        state IN ('running', 'complete', 'partial', 'failed', 'interrupted')
    ),
    reason TEXT,
    occurred_at TEXT NOT NULL CHECK (occurred_at GLOB '*Z')
);

CREATE INDEX run_transition_by_run ON run_transition(run_id, id);

CREATE TABLE work_item (
    id INTEGER PRIMARY KEY,
    task_type TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id INTEGER,
    fingerprint TEXT NOT NULL CHECK (length(fingerprint) = 64),
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    priority INTEGER NOT NULL,
    eligible_at TEXT NOT NULL CHECK (eligible_at GLOB '*Z'),
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'running', 'succeeded',
            'deferred', 'failed_permanent', 'superseded'
        )
    ),
    reason TEXT,
    created_by_run_id INTEGER REFERENCES run(id),
    claimed_by_run_id INTEGER REFERENCES run(id),
    completed_by_run_id INTEGER REFERENCES run(id),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z'),
    updated_at TEXT NOT NULL CHECK (updated_at GLOB '*Z')
);

-- At most one active work item may exist for a task type and fingerprint.
-- Terminal rows are excluded by the partial predicate, so history never
-- blocks legitimate rescheduling after an input changes.
CREATE UNIQUE INDEX work_item_active_identity
    ON work_item(task_type, fingerprint)
    WHERE state IN ('pending', 'running', 'deferred');

CREATE INDEX work_item_eligible
    ON work_item(state, eligible_at, priority, id);

CREATE TABLE attempt (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    work_item_id INTEGER NOT NULL REFERENCES work_item(id),
    provider TEXT NOT NULL,
    operation TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    started_at TEXT NOT NULL CHECK (started_at GLOB '*Z'),
    finished_at TEXT CHECK (finished_at IS NULL OR finished_at GLOB '*Z'),
    -- NULL while the external call is in flight; the startup sweep marks
    -- abandoned rows 'interrupted'.
    outcome TEXT CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'failed', 'interrupted')
    ),
    failure_category TEXT,
    provider_status INTEGER,
    retry_after_ms INTEGER CHECK (retry_after_ms IS NULL OR retry_after_ms >= 0),
    request_fingerprint TEXT NOT NULL CHECK (length(request_fingerprint) = 64),
    destination_host TEXT,
    response_bytes INTEGER CHECK (response_bytes IS NULL OR response_bytes >= 0),
    latency_ms INTEGER CHECK (latency_ms IS NULL OR latency_ms >= 0),
    reserved_nano_usd INTEGER NOT NULL DEFAULT 0
        CHECK (reserved_nano_usd >= 0),
    actual_nano_usd INTEGER
        CHECK (actual_nano_usd IS NULL OR actual_nano_usd >= 0),
    provider_request_id TEXT,
    detail_json TEXT,
    UNIQUE (work_item_id, ordinal),
    CHECK ((outcome IS NULL) = (finished_at IS NULL)),
    CHECK (
        CASE
            WHEN outcome = 'failed' THEN failure_category IS NOT NULL
            ELSE failure_category IS NULL
        END
    )
);

CREATE INDEX attempt_by_run ON attempt(run_id, id);
CREATE INDEX attempt_by_work_item ON attempt(work_item_id, ordinal);
