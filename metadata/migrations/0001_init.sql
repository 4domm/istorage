CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    manifest JSONB NOT NULL,
    PRIMARY KEY (bucket, object_key)
);

CREATE TABLE IF NOT EXISTS chunk_refcounts (
    node_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    ref_count BIGINT NOT NULL CHECK (ref_count >= 0),
    PRIMARY KEY (node_id, chunk_id)
);

CREATE TABLE IF NOT EXISTS gc_queue (
    seq BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    claimed_by TEXT,
    claimed_until TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_gc_queue_seq ON gc_queue(seq);
CREATE INDEX IF NOT EXISTS idx_gc_queue_claimed_until ON gc_queue(claimed_until);

CREATE TABLE IF NOT EXISTS chunker_nodes (
    node_id TEXT PRIMARY KEY,
    base_url TEXT NOT NULL,
    healthy BOOLEAN NOT NULL DEFAULT TRUE,
    last_healthcheck_at TIMESTAMPTZ,
    last_error TEXT
);
