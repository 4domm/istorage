use anyhow::{anyhow, Result};
use sqlx::{PgPool, Postgres, Transaction};
use std::collections::HashMap;

use metadata::{ChunkerNode, GcAckResult, GcTask, MutationResult, ObjectMeta, PutObjectRequest};

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

    pub async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: PutObjectRequest,
    ) -> Result<MutationResult> {
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
        Ok(MutationResult {
            orphan_chunks: orphan_chunks
                .into_iter()
                .map(|(_, chunk_id)| chunk_id)
                .collect(),
        })
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

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<MutationResult>> {
        let mut tx = self.pool.begin().await?;

        let old_manifest_json: Option<serde_json::Value> = sqlx::query_scalar(
            "DELETE FROM objects WHERE bucket = $1 AND object_key = $2 RETURNING manifest",
        )
        .bind(bucket)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await?;

        let Some(old_manifest_json) = old_manifest_json else {
            return Ok(None);
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
        Ok(Some(MutationResult {
            orphan_chunks: orphan_chunks
                .into_iter()
                .map(|(_, chunk_id)| chunk_id)
                .collect(),
        }))
    }

    pub async fn gc_next(&self) -> Result<Option<GcTask>> {
        let row: Option<(i64, String, String, String)> = sqlx::query_as(
            "SELECT q.seq, q.chunk_id, q.node_id, n.base_url
             FROM gc_queue q
             JOIN chunker_nodes n ON n.node_id = q.node_id
             ORDER BY q.seq ASC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(match row {
            Some((seq, chunk_id, node_id, base_url)) => Some(GcTask {
                seq: u64::try_from(seq).map_err(|_| anyhow!("negative gc seq"))?,
                chunk_id,
                node_id,
                base_url,
            }),
            None => None,
        })
    }

    pub async fn gc_ack(&self, seq: u64) -> Result<GcAckResult> {
        let seq = i64::try_from(seq).map_err(|_| anyhow!("gc seq overflow"))?;
        let deleted = sqlx::query("DELETE FROM gc_queue WHERE seq = $1")
            .bind(seq)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0;
        Ok(GcAckResult { acked: deleted })
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
