use async_stream::stream;
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::put,
    Router,
};
use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use metadata::{ChunkRef, ChunkerNode, PutObjectRequest};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::chunk_client::ChunkClient;
use crate::config::Config;
use crate::error::ApiError;
use crate::metadata_client::MetadataClient;

pub struct AppState {
    pub config: Config,
    pub metadata: MetadataClient,
    pub chunk: ChunkClient,
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

pub async fn create_router(config: Config) -> Result<Router, ApiError> {
    let state = AppState {
        metadata: MetadataClient::new(config.metadata_service_url.clone()),
        chunk: ChunkClient::new(),
        config: config.clone(),
    };
    spawn_chunk_gc_worker(state.metadata.clone(), state.chunk.clone());
    let state = Arc::new(state);

    let app = Router::new()
        .route(
            "/:bucket/*key",
            put(put_object).get(get_object).delete(delete_object),
        )
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    Ok(app)
}

fn spawn_chunk_gc_worker(metadata: MetadataClient, chunk: ChunkClient) {
    tokio::spawn(async move {
        loop {
            let maybe_task = match metadata.gc_next().await {
                Ok(task) => task,
                Err(err) => {
                    tracing::warn!("gc poll failed: {}", err);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };
            let Some(task) = maybe_task else {
                tokio::time::sleep(Duration::from_millis(300)).await;
                continue;
            };
            let mut attempt = 0u8;
            loop {
                attempt += 1;
                match chunk.delete_chunk(&task.base_url, &task.chunk_id).await {
                    Ok(()) => {
                        match metadata.gc_ack(task.seq).await {
                            Ok(true) => {}
                            Ok(false) => tracing::warn!("gc ack mismatch for seq {}", task.seq),
                            Err(err) => {
                                tracing::warn!("gc ack failed for seq {}: {}", task.seq, err)
                            }
                        }
                        break;
                    }
                    Err(err) if attempt < 4 => {
                        tracing::warn!(
                            "gc delete attempt {} failed for chunk {} on node {}: {}",
                            attempt,
                            task.chunk_id,
                            task.node_id,
                            err
                        );
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(err) => {
                        tracing::error!(
                            "gc delete permanently failed for chunk {} on node {} after {} attempts: {}",
                            task.chunk_id,
                            task.node_id,
                            attempt,
                            err
                        );
                        tokio::time::sleep(Duration::from_secs(2)).await;
                        break;
                    }
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
                let created = state
                    .chunk
                    .put_chunk(&node.base_url, &chunk_hash, chunk_data.clone())
                    .await?;
                if created {
                    uploaded_chunks.push((node.node_id.clone(), chunk_hash.clone()));
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
        let created = state
            .chunk
            .put_chunk(&node.base_url, &chunk_hash, chunk_data.clone())
            .await?;
        if created {
            uploaded_chunks.push((node.node_id.clone(), chunk_hash.clone()));
        }
        manifest.push(ChunkRef {
            chunk_id: chunk_hash,
            node_id: node.node_id.clone(),
            offset,
            size: chunk_data.len() as u64,
        });
        offset += chunk_data.len() as u64;
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
    let stream = stream! {
        for chunk_ref in manifest {
            let Some(base_url) = node_map.get(&chunk_ref.node_id) else {
                yield Err(std::io::Error::other(format!(
                    "missing chunker node {} for chunk {}",
                    chunk_ref.node_id,
                    chunk_ref.chunk_id
                )));
                break;
            };
            match chunk.get_chunk(base_url, &chunk_ref.chunk_id).await {
                Ok(bytes) => yield Ok::<Bytes, std::io::Error>(bytes),
                Err(e) => {
                    yield Err(std::io::Error::other(e.to_string()));
                    break;
                }
            }
        }
    };

    let mut headers = HeaderMap::new();
    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
    headers.insert("ETag", meta.etag.parse().unwrap());
    Ok((StatusCode::OK, headers, Body::from_stream(stream)))
}

async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    let Some(_) = state.metadata.delete_object(&bucket, &key).await? else {
        return Ok(StatusCode::NOT_FOUND);
    };
    Ok(StatusCode::NO_CONTENT)
}
