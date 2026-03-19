use anyhow::{anyhow, Result};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;

use metadata::{ChunkerNode, GcTask, ObjectMeta, PutObjectRequest};

type ChunkKey = (String, String);

fn manifest_counts(manifest: &[metadata::ChunkRef]) -> HashMap<ChunkKey, u64> {
    let mut counts = HashMap::new();
    for chunk in manifest {
        *counts
            .entry((chunk.node_id.clone(), chunk.chunk_id.clone()))
            .or_insert(0) += 1;
    }
    counts
}

fn parse_manifest(value: serde_json::Value) -> Result<Vec<metadata::ChunkRef>> {
    serde_json::from_value(value).map_err(|e| anyhow!("invalid manifest json: {e}"))
}

fn u64_to_i64_size(size: u64) -> Result<i64> {
    i64::try_from(size).map_err(|_| anyhow!("size is too large for storage"))
}

fn i64_to_u64_size(size: i64) -> Result<u64> {
    u64::try_from(size).map_err(|_| anyhow!("negative size in db"))
}

async fn enqueue_gc_tasks(
    tx: &mut Transaction<'_, Postgres>,
    orphan_chunks: &[ChunkKey],
) -> Result<()> {
    for (node_id, chunk_id) in orphan_chunks {
        sqlx::query("INSERT INTO gc_queue (node_id, chunk_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(chunk_id)
            .execute(&mut **tx)
            .await?;
    }
    Ok(())
}

async fn apply_refcount_deltas(
    tx: &mut Transaction<'_, Postgres>,
    deltas: HashMap<ChunkKey, i64>,
) -> Result<Vec<ChunkKey>> {
    let mut orphan_chunks = Vec::new();

    for ((node_id, chunk_id), delta) in deltas {
        if delta == 0 {
            continue;
        }

        if delta > 0 {
            sqlx::query(
                "INSERT INTO chunk_refcounts (node_id, chunk_id, ref_count)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (node_id, chunk_id)
                 DO UPDATE SET ref_count = chunk_refcounts.ref_count + EXCLUDED.ref_count",
            )
            .bind(&node_id)
            .bind(&chunk_id)
            .bind(delta)
            .execute(&mut **tx)
            .await?;
            continue;
        }

        let current: i64 = sqlx::query_scalar(
            "SELECT ref_count
             FROM chunk_refcounts
             WHERE node_id = $1 AND chunk_id = $2
             FOR UPDATE",
        )
        .bind(&node_id)
        .bind(&chunk_id)
        .fetch_optional(&mut **tx)
        .await?
        .unwrap_or(0);

        let sub = -delta;
        let next = current.saturating_sub(sub);

        if next == 0 {
            sqlx::query("DELETE FROM chunk_refcounts WHERE node_id = $1 AND chunk_id = $2")
                .bind(&node_id)
                .bind(&chunk_id)
                .execute(&mut **tx)
                .await?;
            if current > 0 {
                orphan_chunks.push((node_id, chunk_id));
            }
        } else {
            sqlx::query(
                "UPDATE chunk_refcounts
                 SET ref_count = $3
                 WHERE node_id = $1 AND chunk_id = $2",
            )
            .bind(&node_id)
            .bind(&chunk_id)
            .bind(next)
            .execute(&mut **tx)
            .await?;
        }
    }

    Ok(orphan_chunks)
}

#[derive(Clone)]
pub struct MetadataRepo {
    pool: PgPool,
}

impl MetadataRepo {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn put_object(&self, bucket: &str, key: &str, body: PutObjectRequest) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let old_manifest_json: Option<serde_json::Value> = sqlx::query_scalar(
            "SELECT manifest FROM objects WHERE bucket = $1 AND object_key = $2 FOR UPDATE",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        let old_counts = if let Some(v) = old_manifest_json {
            let old_manifest = parse_manifest(v)?;
            manifest_counts(&old_manifest)
        } else {
            HashMap::new()
        };

        let new_counts = manifest_counts(&body.manifest);
        let manifest_json = serde_json::to_value(&body.manifest)?;

        sqlx::query(
            "INSERT INTO objects (bucket, object_key, size, etag, manifest)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (bucket, object_key)
             DO UPDATE SET size = EXCLUDED.size, etag = EXCLUDED.etag, manifest = EXCLUDED.manifest",
        )
        .bind(bucket)
        .bind(key)
        .bind(u64_to_i64_size(body.size)?)
        .bind(&body.etag)
        .bind(manifest_json)
        .execute(&mut *tx)
        .await?;

        let mut deltas: HashMap<ChunkKey, i64> = HashMap::new();
        for (chunk_key, count) in old_counts {
            let c = i64::try_from(count).map_err(|_| anyhow!("old count overflow"))?;
            *deltas.entry(chunk_key).or_insert(0) -= c;
        }
        for (chunk_key, count) in new_counts {
            let c = i64::try_from(count).map_err(|_| anyhow!("new count overflow"))?;
            *deltas.entry(chunk_key).or_insert(0) += c;
        }

        let orphan_chunks = apply_refcount_deltas(&mut tx, deltas).await?;
        enqueue_gc_tasks(&mut tx, &orphan_chunks).await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let row: Option<(i64, String, serde_json::Value)> = sqlx::query_as(
            "SELECT size, etag, manifest FROM objects WHERE bucket = $1 AND object_key = $2",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;

        let Some((size, etag, manifest_json)) = row else {
            return Ok(None);
        };

        let manifest = parse_manifest(manifest_json)?;
        let size = i64_to_u64_size(size)?;
        Ok(Some(ObjectMeta {
            size,
            etag,
            manifest,
        }))
    }

    pub async fn list_bucket_keys(
        &self,
        bucket: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>)> {
        let db_limit =
            i64::try_from(limit.saturating_add(1)).map_err(|_| anyhow!("list limit overflow"))?;
        let mut rows: Vec<String> = sqlx::query_scalar(
            "SELECT object_key
             FROM objects
             WHERE bucket = $1
               AND ($2::text IS NULL OR object_key > $2)
             ORDER BY object_key ASC
             LIMIT $3",
        )
        .bind(bucket)
        .bind(cursor)
        .bind(db_limit)
        .fetch_all(&self.pool)
        .await?;

        let next_cursor = if rows.len() > limit {
            rows.truncate(limit);
            rows.last().cloned()
        } else {
            None
        };
        Ok((rows, next_cursor))
    }

    pub async fn gc_next_batch(
        &self,
        limit: usize,
        owner: &str,
        lease_seconds: u64,
    ) -> Result<Vec<GcTask>> {
        let limit = i64::try_from(limit).map_err(|_| anyhow!("gc batch limit overflow"))?;
        let lease_seconds =
            i64::try_from(lease_seconds).map_err(|_| anyhow!("gc lease overflow"))?;
        let rows: Vec<(i64, String, String, String)> = sqlx::query_as(
            "WITH candidates AS (
                SELECT seq
                FROM gc_queue
                WHERE claimed_until IS NULL
                   OR claimed_until < NOW()
                   OR claimed_by = $2
                ORDER BY seq ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
             ),
             claimed AS (
                UPDATE gc_queue q
                SET claimed_by = $2,
                    claimed_until = NOW() + ($3::text || ' seconds')::interval
                FROM candidates c
                WHERE q.seq = c.seq
                RETURNING q.seq, q.chunk_id, q.node_id
             )
             SELECT c.seq, c.chunk_id, c.node_id, n.base_url
             FROM claimed c
             JOIN chunker_nodes n ON n.node_id = c.node_id
             ORDER BY c.seq ASC",
        )
        .bind(limit)
        .bind(owner)
        .bind(lease_seconds)
        .fetch_all(&self.pool)
        .await?;
        let mut tasks = Vec::with_capacity(rows.len());
        for (seq, chunk_id, node_id, base_url) in rows {
            tasks.push(GcTask {
                seq: u64::try_from(seq).map_err(|_| anyhow!("negative gc seq"))?,
                chunk_id,
                node_id,
                base_url,
            });
        }
        Ok(tasks)
    }

    pub async fn gc_ack_batch(&self, owner: &str, seqs: &[u64]) -> Result<u64> {
        if seqs.is_empty() {
            return Ok(0);
        }
        let seqs: Vec<i64> = seqs
            .iter()
            .map(|seq| i64::try_from(*seq).map_err(|_| anyhow!("gc seq overflow")))
            .collect::<Result<Vec<_>>>()?;

        let deleted = sqlx::query("DELETE FROM gc_queue WHERE seq = ANY($1) AND claimed_by = $2")
            .bind(&seqs)
            .bind(owner)
            .execute(&self.pool)
            .await?
            .rows_affected();
        Ok(deleted)
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        let mut tx = self.pool.begin().await?;

        let old_manifest_json: Option<serde_json::Value> = sqlx::query_scalar(
            "DELETE FROM objects WHERE bucket = $1 AND object_key = $2 RETURNING manifest",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(old_manifest_json) = old_manifest_json else {
            return Ok(false);
        };

        let old_manifest = parse_manifest(old_manifest_json)?;
        let old_counts = manifest_counts(&old_manifest);

        let mut deltas: HashMap<ChunkKey, i64> = HashMap::new();
        for (chunk_key, count) in old_counts {
            let c = i64::try_from(count).map_err(|_| anyhow!("old count overflow"))?;
            *deltas.entry(chunk_key).or_insert(0) -= c;
        }

        let orphan_chunks = apply_refcount_deltas(&mut tx, deltas).await?;
        enqueue_gc_tasks(&mut tx, &orphan_chunks).await?;

        tx.commit().await?;
        Ok(true)
    }

    pub async fn chunk_in_use(&self, node_id: &str, chunk_id: &str) -> Result<bool> {
        let count: Option<i64> = sqlx::query_scalar(
            "SELECT ref_count
             FROM chunk_refcounts
             WHERE node_id = $1 AND chunk_id = $2",
        )
        .bind(node_id)
        .bind(chunk_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(count.unwrap_or(0) > 0)
    }

    pub async fn sync_chunker_nodes(&self, nodes: &[ChunkerNode]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for node in nodes {
            sqlx::query(
                "INSERT INTO chunker_nodes (node_id, base_url, healthy)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (node_id)
                 DO UPDATE SET base_url = EXCLUDED.base_url",
            )
            .bind(&node.node_id)
            .bind(&node.base_url)
            .bind(node.healthy)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn list_chunker_nodes(&self) -> Result<Vec<ChunkerNode>> {
        let rows: Vec<(String, String, bool)> = sqlx::query_as(
            "SELECT node_id, base_url, healthy
             FROM chunker_nodes
             ORDER BY node_id ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|(node_id, base_url, healthy)| ChunkerNode {
                node_id,
                base_url,
                healthy,
            })
            .collect())
    }

    pub async fn update_chunker_health(
        &self,
        node_id: &str,
        healthy: bool,
        last_error: Option<&str>,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE chunker_nodes
             SET healthy = $2,
                 last_healthcheck_at = NOW(),
                 last_error = $3
             WHERE node_id = $1",
        )
        .bind(node_id)
        .bind(healthy)
        .bind(last_error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
