CREATE TABLE configuration_snapshot (
    id INTEGER PRIMARY KEY,
    fingerprint TEXT NOT NULL UNIQUE CHECK (length(fingerprint) = 64),
    canonical_json TEXT NOT NULL,
    created_at TEXT NOT NULL CHECK (created_at GLOB '*Z')
);
