CREATE TABLE IF NOT EXISTS objects (
    bucket TEXT NOT NULL,
    object_key TEXT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    manifest JSONB NOT NULL,
    PRIMARY KEY (bucket, object_key)
);

CREATE TABLE IF NOT EXISTS chunk_refcounts (
    chunk_id TEXT PRIMARY KEY,
    ref_count BIGINT NOT NULL CHECK (ref_count >= 0)
);

CREATE TABLE IF NOT EXISTS gc_queue (
    seq BIGSERIAL PRIMARY KEY,
    chunk_id TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gc_queue_seq ON gc_queue(seq);
