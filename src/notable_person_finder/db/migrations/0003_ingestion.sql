-- Feed identity, fetch history, canonical articles, and source items --
-- the ingestion milestone's domain tables. `feed_identity.key` is the
-- configured stable key from configuration, never the feed's URL: a feed's
-- URL can move without changing which feed it is, and re-keying on URL
-- would silently fork history for a feed operators redirect.

CREATE TABLE feed_identity (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    current_label TEXT NOT NULL,
    current_url TEXT NOT NULL,
    first_seen_at TEXT NOT NULL CHECK (first_seen_at GLOB '*Z'),
    last_seen_at TEXT NOT NULL CHECK (last_seen_at GLOB '*Z')
);

-- One row per settled feed work item: a successful/not-modified result, a
-- not-inspected result, or the final failure after retry adjudication. The
-- `attempt` table remains the one-row-per-external-call record. Conditional-
-- request state (the ETag and Last-Modified to send next time) is deliberately
-- not stored as mutable columns on `feed_identity`: it is derived by the
-- repository from the latest fetch whose `outcome` is not 'failed' *and*
-- which actually carries a validator, so neither a failed fetch nor a bare
-- 304 (which usually omits Last-Modified and often ETag) can poison the
-- validators an earlier successful fetch already established.
CREATE TABLE feed_fetch (
    id INTEGER PRIMARY KEY,
    feed_identity_id INTEGER NOT NULL REFERENCES feed_identity(id),
    run_id INTEGER NOT NULL REFERENCES run(id),
    requested_at TEXT NOT NULL CHECK (requested_at GLOB '*Z'),
    requested_url TEXT NOT NULL,
    resolved_url TEXT,
    redirect_chain_json TEXT,
    http_status INTEGER,
    etag TEXT,
    last_modified TEXT,
    outcome TEXT NOT NULL CHECK (outcome IN ('not_modified', 'modified', 'failed')),
    feed_type TEXT,
    parse_outcome TEXT CHECK (parse_outcome IN ('ok', 'recovered', 'unusable')),
    parser_warnings_json TEXT,
    failure_category TEXT CHECK (
        failure_category IS NULL OR failure_category IN (
            'network', 'timeout', 'rate_limit', 'transient_server_error',
            'provider_unavailable', 'authentication', 'configuration',
            'unsupported_capability', 'access_denied', 'unsupported_content',
            'response_too_large', 'malformed_response', 'budget_exhausted',
            'storage', 'internal'
        )
    ),
    entry_count INTEGER CHECK (entry_count IS NULL OR entry_count >= 0),
    response_bytes INTEGER CHECK (response_bytes IS NULL OR response_bytes >= 0),
    -- The same coupling `0002` enforces on `attempt`: a failure must always
    -- say why, and a non-failure must never claim a reason. Without this a
    -- 'failed' row can carry no category (so the digest cannot explain it)
    -- and a 'modified' row can carry one (so a successful fetch reads as a
    -- failure to anything grouping on `failure_category`).
    CHECK (
        CASE
            WHEN outcome = 'failed' THEN failure_category IS NOT NULL
            ELSE failure_category IS NULL
        END
    )
);

CREATE INDEX feed_fetch_by_identity ON feed_fetch(feed_identity_id, id);
CREATE INDEX feed_fetch_by_run ON feed_fetch(run_id, id);

-- The one application-owned article identity: `canonical_url` is always the
-- output of `ingestion.urls.canonicalize_article_url`, never a raw feed or
-- redirect URL. `publisher_key` is denormalized here (rather than derived on
-- every read) because it is cheap to compute once at insert time and is read
-- far more often than an article is inserted.
CREATE TABLE canonical_article (
    id INTEGER PRIMARY KEY,
    canonical_url TEXT NOT NULL UNIQUE,
    publisher_key TEXT NOT NULL,
    first_seen_at TEXT NOT NULL CHECK (first_seen_at GLOB '*Z')
);

CREATE INDEX canonical_article_by_publisher ON canonical_article(publisher_key);

-- Every URL ever observed to resolve to a canonical article, so a later
-- sighting of a URL already seen (as a feed link, or as a redirect's final
-- destination) is recognized as the same article rather than re-inserted.
-- 'search_result' is not yet a valid `kind`: milestone 5 adds it to this
-- CHECK when search occurrences start recording aliases, so that addition is
-- expected here, not a surprise.
CREATE TABLE article_url_alias (
    id INTEGER PRIMARY KEY,
    canonical_article_id INTEGER NOT NULL REFERENCES canonical_article(id),
    url TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL CHECK (kind IN ('feed_original', 'redirect_destination')),
    first_seen_at TEXT NOT NULL CHECK (first_seen_at GLOB '*Z')
);

CREATE INDEX article_url_alias_by_article ON article_url_alias(canonical_article_id);

-- One row per feed entry ever discovered, whether or not it resolved to a
-- usable article URL. `canonical_article_id`, `source_entry_id`, and
-- `original_url` are all nullable because a feed entry can arrive with an
-- unusable or missing URL and still be worth recording (see `url_issue`).
CREATE TABLE source_item (
    id INTEGER PRIMARY KEY,
    feed_identity_id INTEGER NOT NULL REFERENCES feed_identity(id),
    discovered_by_fetch_id INTEGER NOT NULL REFERENCES feed_fetch(id),
    discovered_by_run_id INTEGER NOT NULL REFERENCES run(id),
    canonical_article_id INTEGER REFERENCES canonical_article(id),
    source_entry_id TEXT,
    original_url TEXT,
    title_raw TEXT,
    title_text TEXT,
    summary_raw TEXT,
    summary_text TEXT,
    author_raw TEXT,
    published_raw TEXT,
    published_at TEXT CHECK (published_at IS NULL OR published_at GLOB '*Z'),
    published_issue TEXT CHECK (
        published_issue IS NULL
        OR published_issue IN ('missing', 'unparseable', 'implausible')
    ),
    url_issue TEXT CHECK (
        url_issue IS NULL
        OR url_issue IN ('missing', 'not_http', 'unsafe', 'unusable')
    ),
    discovered_at TEXT NOT NULL CHECK (discovered_at GLOB '*Z')
);

CREATE INDEX source_item_by_feed ON source_item(feed_identity_id, id);
CREATE INDEX source_item_by_run ON source_item(discovered_by_run_id);

-- An article, once identified, is the durable dedup key: at most one source
-- item may claim a given canonical article, across every feed and every run.
CREATE UNIQUE INDEX source_item_article
    ON source_item(canonical_article_id)
    WHERE canonical_article_id IS NOT NULL;

-- Where there is no article to dedup on, fall back to the feed's own entry
-- id, scoped to that feed. This index is deliberately narrow -- it only
-- applies while `canonical_article_id IS NULL` -- so entry-id dedup never
-- competes with the article-based index above, and a feed that reuses entry
-- ids across genuinely different URLs is not silently collapsed once those
-- URLs each resolve to their own article.
CREATE UNIQUE INDEX source_item_entry
    ON source_item(feed_identity_id, source_entry_id)
    WHERE source_entry_id IS NOT NULL AND canonical_article_id IS NULL;
