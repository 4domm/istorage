CREATE TABLE IF NOT EXISTS chunker_nodes (
    node_id TEXT PRIMARY KEY,
    base_url TEXT NOT NULL,
    healthy BOOLEAN NOT NULL DEFAULT TRUE,
    last_healthcheck_at TIMESTAMPTZ,
    last_error TEXT
);

ALTER TABLE chunk_refcounts
    ADD COLUMN IF NOT EXISTS node_id TEXT;

UPDATE chunk_refcounts
SET node_id = 'default'
WHERE node_id IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'chunk_refcounts'
          AND constraint_type = 'PRIMARY KEY'
          AND constraint_name = 'chunk_refcounts_pkey'
    ) THEN
        ALTER TABLE chunk_refcounts DROP CONSTRAINT chunk_refcounts_pkey;
    END IF;
END $$;

ALTER TABLE chunk_refcounts
    ALTER COLUMN node_id SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'chunk_refcounts'
          AND constraint_type = 'PRIMARY KEY'
          AND constraint_name = 'chunk_refcounts_pkey'
    ) THEN
        ALTER TABLE chunk_refcounts ADD PRIMARY KEY (node_id, chunk_id);
    END IF;
END $$;

ALTER TABLE gc_queue
    ADD COLUMN IF NOT EXISTS node_id TEXT;

UPDATE gc_queue
SET node_id = 'default'
WHERE node_id IS NULL;

ALTER TABLE gc_queue
    ALTER COLUMN node_id SET NOT NULL;
