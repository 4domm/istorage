use async_stream::stream;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::put,
    Json, Router,
};
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use metadata::{ChunkRef, ChunkerNode, PutObjectRequest};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
    time::SystemTime,
};
use tokio::task::JoinSet;

use crate::chunk_client::ChunkClient;
use crate::config::Config;
use crate::error::ApiError;
use crate::metadata_client::MetadataClient;

const GC_DELETE_ATTEMPTS: u8 = 4;
const GC_EMPTY_POLL_DELAY: Duration = Duration::from_millis(300);
const GC_ERROR_POLL_DELAY: Duration = Duration::from_secs(1);
const GC_CLAIM_LEASE_SECONDS: u64 = 30;
const PUT_UPLOAD_CONCURRENCY: usize = 16;

#[derive(Debug, Deserialize)]
struct ListBucketQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

pub struct AppState {
    pub config: Config,
    pub metadata: MetadataClient,
    pub chunk: ChunkClient,
}

struct UploadResult {
    node_id: String,
    chunk_id: String,
    created: bool,
}

fn spawn_chunk_upload(
    uploads: &mut JoinSet<Result<UploadResult, ApiError>>,
    chunk: ChunkClient,
    base_url: String,
    node_id: String,
    chunk_id: String,
    data: Bytes,
) {
    uploads.spawn(async move {
        let created = chunk.put_chunk(&base_url, &chunk_id, data).await?;
        Ok(UploadResult {
            node_id,
            chunk_id,
            created,
        })
    });
}

async fn collect_one_upload_result(
    uploads: &mut JoinSet<Result<UploadResult, ApiError>>,
    uploaded_chunks: &mut Vec<(String, String)>,
) -> Result<(), ApiError> {
    let joined = uploads.join_next().await.ok_or_else(|| {
        ApiError::Internal(anyhow::anyhow!("chunk upload queue is unexpectedly empty"))
    })?;
    let result = joined
        .map_err(|e| ApiError::Internal(anyhow::anyhow!("chunk upload task failed: {}", e)))??;
    if result.created {
        uploaded_chunks.push((result.node_id, result.chunk_id));
    }
    Ok(())
}

async fn drain_upload_results(
    uploads: &mut JoinSet<Result<UploadResult, ApiError>>,
    uploaded_chunks: &mut Vec<(String, String)>,
) -> Result<(), ApiError> {
    let mut first_err: Option<ApiError> = None;
    while let Some(joined) = uploads.join_next().await {
        match joined {
            Ok(Ok(result)) => {
                if result.created {
                    uploaded_chunks.push((result.node_id, result.chunk_id));
                }
            }
            Ok(Err(err)) => {
                if first_err.is_none() {
                    first_err = Some(err);
                }
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => {
                if first_err.is_none() {
                    first_err = Some(ApiError::Internal(anyhow::anyhow!(
                        "chunk upload task failed: {}",
                        err
                    )));
                }
            }
        }
    }
    if let Some(err) = first_err {
        return Err(err);
    }
    Ok(())
}

fn gc_worker_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("api-{}-{}", std::process::id(), nanos)
}

fn score_node(chunk_id: &str, node_id: &str) -> u64 {
    let digest = Sha256::digest(format!("{chunk_id}:{node_id}").as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("sha256 prefix"))
}

fn select_chunker_node<'a>(chunk_id: &str, nodes: &'a [ChunkerNode]) -> Option<&'a ChunkerNode> {
    nodes
        .iter()
        .max_by_key(|node| score_node(chunk_id, &node.node_id))
}

fn build_chunker_map(nodes: Vec<ChunkerNode>) -> HashMap<String, String> {
    nodes
        .into_iter()
        .map(|node| (node.node_id, node.base_url))
        .collect()
}

async fn fetch_window_batches(
    chunk: &ChunkClient,
    node_map: &HashMap<String, String>,
    window: &[ChunkRef],
) -> Result<HashMap<String, HashMap<String, Bytes>>, std::io::Error> {
    let mut ids_by_node: HashMap<String, HashSet<String>> = HashMap::new();
    for chunk_ref in window {
        ids_by_node
            .entry(chunk_ref.node_id.clone())
            .or_default()
            .insert(chunk_ref.chunk_id.clone());
    }

    let mut join_set = JoinSet::new();
    for (node_id, chunk_ids) in ids_by_node {
        let Some(base_url) = node_map.get(&node_id) else {
            return Err(std::io::Error::other(format!(
                "missing chunker node {} for batch get",
                node_id
            )));
        };
        let chunk_ids: Vec<String> = chunk_ids.into_iter().collect();
        let chunk_client = chunk.clone();
        let base_url = base_url.clone();
        join_set.spawn(async move {
            let chunks = chunk_client
                .batch_get_chunks(&base_url, &chunk_ids)
                .await
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok::<(String, HashMap<String, Bytes>), std::io::Error>((node_id, chunks))
        });
    }

    let mut batches: HashMap<String, HashMap<String, Bytes>> = HashMap::new();
    while let Some(joined) = join_set.join_next().await {
        let (node_id, chunks) = joined
            .map_err(|e| std::io::Error::other(format!("batch get join failed: {}", e)))??;
        batches.insert(node_id, chunks);
    }

    Ok(batches)
}

pub async fn create_router(config: Config) -> Result<Router, ApiError> {
    let state = AppState {
        metadata: MetadataClient::new(config.metadata_service_url.clone()),
        chunk: ChunkClient::new(),
        config: config.clone(),
    };
    spawn_chunk_gc_worker(
        state.metadata.clone(),
        state.chunk.clone(),
        state.config.gc_batch_size,
    );
    let state = Arc::new(state);

    let app = Router::new()
        .route("/:bucket", axum::routing::get(list_bucket))
        .route(
            "/:bucket/*key",
            put(put_object).get(get_object).delete(delete_object),
        )
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    Ok(app)
}

fn spawn_chunk_gc_worker(metadata: MetadataClient, chunk: ChunkClient, gc_batch_size: usize) {
    tokio::spawn(async move {
        let gc_batch_size = gc_batch_size.max(1);
        let worker_id = gc_worker_id();
        loop {
            let tasks = match metadata
                .gc_next_batch(gc_batch_size, &worker_id, GC_CLAIM_LEASE_SECONDS)
                .await
            {
                Ok(tasks) => tasks,
                Err(err) => {
                    tracing::warn!("gc poll failed: {}", err);
                    tokio::time::sleep(GC_ERROR_POLL_DELAY).await;
                    continue;
                }
            };
            if tasks.is_empty() {
                tokio::time::sleep(GC_EMPTY_POLL_DELAY).await;
                continue;
            }

            let mut by_node: HashMap<String, (String, Vec<(u64, String)>)> = HashMap::new();
            for task in tasks {
                let entry = by_node
                    .entry(task.node_id)
                    .or_insert_with(|| (task.base_url, Vec::new()));
                entry.1.push((task.seq, task.chunk_id));
            }

            let mut acked_seqs: Vec<u64> = Vec::new();
            for (node_id, (base_url, entries)) in by_node {
                let mut dedup = HashSet::new();
                let chunk_ids: Vec<String> = entries
                    .iter()
                    .map(|(_, chunk_id)| chunk_id.clone())
                    .filter(|chunk_id| dedup.insert(chunk_id.clone()))
                    .collect();

                let mut attempt = 0u8;
                let mut deleted = false;
                while attempt < GC_DELETE_ATTEMPTS {
                    attempt += 1;
                    match chunk.batch_delete_chunks(&base_url, &chunk_ids).await {
                        Ok(()) => {
                            deleted = true;
                            break;
                        }
                        Err(err) if attempt < GC_DELETE_ATTEMPTS => {
                            tracing::warn!(
                                "gc batch delete attempt {} failed for node {} ({} chunks): {}",
                                attempt,
                                node_id,
                                chunk_ids.len(),
                                err
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        Err(err) => {
                            tracing::error!(
                                "gc batch delete permanently failed for node {} ({} chunks) after {} attempts: {}",
                                node_id,
                                chunk_ids.len(),
                                attempt,
                                err
                            );
                        }
                    }
                }

                if deleted {
                    acked_seqs.extend(entries.into_iter().map(|(seq, _)| seq));
                }
            }

            if acked_seqs.is_empty() {
                tokio::time::sleep(GC_ERROR_POLL_DELAY).await;
                continue;
            }

            match metadata.gc_ack_batch(&worker_id, &acked_seqs).await {
                Ok(acked) => {
                    let expected = acked_seqs.len() as u64;
                    if acked != expected {
                        tracing::warn!(
                            "gc batch ack mismatch: expected {}, acked {}",
                            expected,
                            acked
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!("gc batch ack failed: {}", err);
                }
            }
        }
    });
}

async fn rollback_new_chunks_on_metadata_error(
    state: &AppState,
    uploaded_chunks: Vec<(String, String)>,
) {
    let nodes = match state.metadata.list_chunker_nodes().await {
        Ok(nodes) => build_chunker_map(nodes),
        Err(err) => {
            tracing::warn!("failed to list chunker nodes for rollback: {}", err);
            return;
        }
    };

    for (node_id, chunk_id) in uploaded_chunks {
        let in_use = match state.metadata.chunk_in_use(&node_id, &chunk_id).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    "failed to check chunk usage for rollback {} on node {}: {}",
                    chunk_id,
                    node_id,
                    err
                );
                continue;
            }
        };
        if in_use {
            continue;
        }
        let Some(base_url) = nodes.get(&node_id) else {
            tracing::warn!("missing chunker node {} during rollback", node_id);
            continue;
        };
        if let Err(err) = state.chunk.delete_chunk(base_url, &chunk_id).await {
            tracing::warn!(
                "failed to rollback newly created chunk {} on node {}: {}",
                chunk_id,
                node_id,
                err
            );
        }
    }
}

fn validate_bucket(bucket: &str) -> Result<(), ApiError> {
    if bucket.is_empty() || bucket.contains('/') || bucket.starts_with('.') {
        return Err(ApiError::Validation("invalid bucket name".into()));
    }
    Ok(())
}

async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
    mut body: axum::body::Body,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    if key.is_empty() {
        return Err(ApiError::Validation("key must be non-empty".into()));
    }

    let chunk_size = state.config.chunk_size;
    if chunk_size == 0 {
        return Err(ApiError::Validation("chunk_size must be > 0".into()));
    }

    let all_nodes = state.metadata.list_chunker_nodes().await?;
    let healthy_nodes: Vec<ChunkerNode> =
        all_nodes.into_iter().filter(|node| node.healthy).collect();
    if healthy_nodes.is_empty() {
        return Err(ApiError::Internal(anyhow::anyhow!(
            "no healthy chunker nodes available"
        )));
    }

    let mut manifest = Vec::new();
    let mut uploaded_chunks = Vec::new();
    let mut uploads: JoinSet<Result<UploadResult, ApiError>> = JoinSet::new();
    let mut offset = 0u64;
    let mut hasher = Sha256::new();
    let mut pending = BytesMut::with_capacity(chunk_size.saturating_mul(2));

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| ApiError::Internal(anyhow::anyhow!("body: {}", e)))?;
        if let Some(data) = frame.data_ref() {
            hasher.update(data.as_ref());
            pending.extend_from_slice(data.as_ref());

            while pending.len() >= chunk_size {
                let chunk_data = pending.split_to(chunk_size).freeze();
                let chunk_hash = hex::encode(Sha256::digest(&chunk_data));
                let node = select_chunker_node(&chunk_hash, &healthy_nodes).ok_or_else(|| {
                    ApiError::Internal(anyhow::anyhow!("no healthy chunker nodes available"))
                })?;
                let created = state.chunk.clone();
                spawn_chunk_upload(
                    &mut uploads,
                    created,
                    node.base_url.clone(),
                    node.node_id.clone(),
                    chunk_hash.clone(),
                    chunk_data.clone(),
                );
                if uploads.len() >= PUT_UPLOAD_CONCURRENCY {
                    if let Err(err) =
                        collect_one_upload_result(&mut uploads, &mut uploaded_chunks).await
                    {
                        uploads.abort_all();
                        let _ = drain_upload_results(&mut uploads, &mut uploaded_chunks).await;
                        rollback_new_chunks_on_metadata_error(&state, uploaded_chunks).await;
                        return Err(err);
                    }
                }
                manifest.push(ChunkRef {
                    chunk_id: chunk_hash,
                    node_id: node.node_id.clone(),
                    offset,
                    size: chunk_data.len() as u64,
                });
                offset += chunk_data.len() as u64;
            }
        }
    }

    if !pending.is_empty() {
        let chunk_data: Bytes = pending.freeze();
        let chunk_hash = hex::encode(Sha256::digest(&chunk_data));
        let node = select_chunker_node(&chunk_hash, &healthy_nodes).ok_or_else(|| {
            ApiError::Internal(anyhow::anyhow!("no healthy chunker nodes available"))
        })?;
        let created = state.chunk.clone();
        spawn_chunk_upload(
            &mut uploads,
            created,
            node.base_url.clone(),
            node.node_id.clone(),
            chunk_hash.clone(),
            chunk_data.clone(),
        );
        if uploads.len() >= PUT_UPLOAD_CONCURRENCY {
            if let Err(err) = collect_one_upload_result(&mut uploads, &mut uploaded_chunks).await {
                uploads.abort_all();
                let _ = drain_upload_results(&mut uploads, &mut uploaded_chunks).await;
                rollback_new_chunks_on_metadata_error(&state, uploaded_chunks).await;
                return Err(err);
            }
        }
        manifest.push(ChunkRef {
            chunk_id: chunk_hash,
            node_id: node.node_id.clone(),
            offset,
            size: chunk_data.len() as u64,
        });
        offset += chunk_data.len() as u64;
    }

    while !uploads.is_empty() {
        if let Err(err) = collect_one_upload_result(&mut uploads, &mut uploaded_chunks).await {
            uploads.abort_all();
            let _ = drain_upload_results(&mut uploads, &mut uploaded_chunks).await;
            rollback_new_chunks_on_metadata_error(&state, uploaded_chunks).await;
            return Err(err);
        }
    }

    let total_size = offset;
    let etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    let req = PutObjectRequest {
        size: total_size,
        etag: etag.clone(),
        manifest,
    };
    if let Err(err) = state.metadata.put_object(&bucket, &key, req).await {
        rollback_new_chunks_on_metadata_error(&state, uploaded_chunks).await;
        return Err(ApiError::Internal(err));
    }

    let mut headers = HeaderMap::new();
    headers.insert("ETag", etag.parse().unwrap());
    Ok((StatusCode::OK, headers, Body::empty()))
}

async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    let meta = state
        .metadata
        .get_object(&bucket, &key)
        .await?
        .ok_or_else(|| ApiError::NotFound {
            bucket: bucket.clone(),
            key: key.clone(),
        })?;
    let chunk = state.chunk.clone();
    let manifest = meta.manifest;
    let node_map = build_chunker_map(state.metadata.list_chunker_nodes().await?);
    let batch_window = state.config.get_batch_window.max(1);
    let stream = stream! {
        for window in manifest.chunks(batch_window) {
            let batches = match fetch_window_batches(&chunk, &node_map, window).await {
                Ok(batches) => batches,
                Err(err) => {
                    yield Err(err);
                    break;
                }
            };

            for chunk_ref in window {
                let Some(chunks) = batches.get(&chunk_ref.node_id) else {
                    yield Err(std::io::Error::other(format!(
                        "missing batch data for node {}",
                        chunk_ref.node_id
                    )));
                    break;
                };
                let Some(bytes) = chunks.get(&chunk_ref.chunk_id) else {
                    yield Err(std::io::Error::other(format!(
                        "missing chunk {} in batch response",
                        chunk_ref.chunk_id
                    )));
                    break;
                };
                yield Ok::<Bytes, std::io::Error>(bytes.clone());
            }
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
    headers.insert("ETag", meta.etag.parse().unwrap());
    Ok((StatusCode::OK, headers, Body::from_stream(stream)))
}

async fn list_bucket(
    State(state): State<Arc<AppState>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListBucketQuery>,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    let response = state
        .metadata
        .list_bucket_keys(&bucket, query.limit, query.cursor.as_deref())
        .await?;
    Ok((StatusCode::OK, Json(response)))
}

async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    if !state.metadata.delete_object(&bucket, &key).await? {
        return Ok(StatusCode::NOT_FOUND);
    }
    Ok(StatusCode::NO_CONTENT)
}
