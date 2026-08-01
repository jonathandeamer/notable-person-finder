-- 0008_lead_aggregation.sql
--
-- Milestone 6a: deterministic per-person lead verdicts aggregated from
-- milestone 5's immutable person_article_assessment/article_assessment_signal
-- rows, plus a durable digest queue and its transition ledger. See
-- docs/superpowers/specs/2026-08-01-lead-aggregation-and-digest-queue-design.md
-- for K1-K13 rationale. lead_assessment rows are immutable domain history;
-- digest_queue is a mutable operational projection with no history of its
-- own (K3) -- queue_transition is the ledger.

CREATE TABLE lead_assessment (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    outcome TEXT NOT NULL CHECK (outcome IN (
        'promising_lead', 'possible_lead',
        'insufficient_evidence', 'assessment_incomplete'
    )),
    qualifying_domain_count INTEGER NOT NULL DEFAULT 0
        CHECK (qualifying_domain_count >= 0),
    incompleteness_reason TEXT,
    ordering_factors_json TEXT NOT NULL,
    lead_policy_fingerprint TEXT NOT NULL
        CHECK (length(lead_policy_fingerprint) = 64),
    decided_at TEXT NOT NULL CHECK (decided_at GLOB '*Z'),
    CHECK (
        (outcome = 'assessment_incomplete' AND incompleteness_reason IS NOT NULL)
        OR (outcome != 'assessment_incomplete' AND incompleteness_reason IS NULL)
    )
);
CREATE INDEX ix_lead_assessment_person ON lead_assessment(person_id, id);

CREATE TABLE lead_assessment_qualifying_article (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    person_article_assessment_id INTEGER NOT NULL
        REFERENCES person_article_assessment(id),
    canonical_domain TEXT NOT NULL CHECK (length(canonical_domain) > 0),
    PRIMARY KEY (lead_assessment_id, person_article_assessment_id)
);

CREATE TABLE lead_assessment_signal (
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    article_assessment_signal_id INTEGER NOT NULL
        REFERENCES article_assessment_signal(id),
    PRIMARY KEY (lead_assessment_id, article_assessment_signal_id)
);

ALTER TABLE person ADD COLUMN current_lead_assessment_id INTEGER
    REFERENCES lead_assessment(id);

CREATE TABLE digest_queue (
    person_id INTEGER PRIMARY KEY REFERENCES person(id),
    status TEXT NOT NULL CHECK (status IN ('pending', 'emitted', 'removed')),
    tier TEXT NOT NULL CHECK (tier IN ('promising_lead', 'possible_lead')),
    eligibility_reason TEXT NOT NULL CHECK (eligibility_reason IN (
        'new', 'promoted', 'strengthened', 'reminder'
    )),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    first_pending_at TEXT NOT NULL CHECK (first_pending_at GLOB '*Z'),
    last_material_change_at TEXT NOT NULL CHECK (last_material_change_at GLOB '*Z'),
    removed_reason TEXT
);

CREATE TABLE queue_transition (
    id INTEGER PRIMARY KEY,
    person_id INTEGER NOT NULL REFERENCES person(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    tier TEXT NOT NULL CHECK (tier IN ('promising_lead', 'possible_lead')),
    from_status TEXT CHECK (from_status IN ('pending', 'emitted', 'removed')),
    to_status TEXT NOT NULL CHECK (to_status IN ('pending', 'emitted', 'removed')),
    reason TEXT NOT NULL CHECK (length(reason) > 0),
    occurred_at TEXT NOT NULL CHECK (occurred_at GLOB '*Z')
);
CREATE INDEX ix_queue_transition_run ON queue_transition(run_id, id);
CREATE INDEX ix_queue_transition_person ON queue_transition(person_id, id);

CREATE TABLE digest (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(id),
    file_path TEXT NOT NULL CHECK (length(file_path) > 0),
    timezone TEXT NOT NULL CHECK (length(timezone) > 0),
    window_start TEXT NOT NULL CHECK (window_start GLOB '*Z'),
    window_end TEXT NOT NULL CHECK (window_end GLOB '*Z'),
    run_state TEXT NOT NULL CHECK (length(run_state) > 0),
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 64),
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z')
);

CREATE TABLE digest_entry (
    id INTEGER PRIMARY KEY,
    digest_id INTEGER NOT NULL REFERENCES digest(id),
    person_id INTEGER NOT NULL REFERENCES person(id),
    lead_assessment_id INTEGER NOT NULL REFERENCES lead_assessment(id),
    queue_transition_id INTEGER NOT NULL REFERENCES queue_transition(id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1)
);
CREATE INDEX ix_digest_entry_digest ON digest_entry(digest_id, ordinal);
