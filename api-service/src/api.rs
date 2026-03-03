use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{delete, get, put},
    Router,
};
use axum::body::to_bytes;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use api_service::chunk_client::ChunkClient;
use api_service::config::Config;
use api_service::error::ApiError;
use api_service::metadata_client::MetadataClient;
use metadata::{ChunkRef, PutObjectRequest};

pub struct AppState {
    pub config: Config,
    pub metadata: MetadataClient,
    pub chunk: ChunkClient,
}

pub async fn create_router(config: Config) -> Result<Router, ApiError> {
    let state = AppState {
        metadata: MetadataClient::new(config.metadata_service_url.clone()),
        chunk: ChunkClient::new(config.chunk_service_url.clone()),
        config: config.clone(),
    };
    let state = Arc::new(state);

    let app = Router::new()
        .route("/:bucket/*key", put(put_object).get(get_object).delete(delete_object))
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    Ok(app)
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
    body: axum::body::Body,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    if key.is_empty() {
        return Err(ApiError::Validation("key must be non-empty".into()));
    }

    let chunk_size = state.config.chunk_size;
    let data = to_bytes(body, usize::MAX).await.map_err(|e| ApiError::Internal(anyhow::anyhow!("body: {}", e)))?;
    let mut manifest = Vec::new();
    let mut offset = 0u64;
    let mut hasher = Sha256::new();

    for chunk_data in data.chunks(chunk_size) {
        let chunk_data = Bytes::copy_from_slice(chunk_data);
        let chunk_hash = hex::encode(Sha256::digest(&chunk_data));
        hasher.update(&chunk_data);

        state.chunk.put_chunk(&chunk_hash, chunk_data.clone()).await?;

        manifest.push(ChunkRef {
            chunk_id: chunk_hash,
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
    state.metadata.put_object(&bucket, &key, req).await?;

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

    let mut body = Vec::with_capacity(meta.size as usize);
    for chunk_ref in &meta.manifest {
        let bytes = state.chunk.get_chunk(&chunk_ref.chunk_id).await?;
        body.extend_from_slice(&bytes);
    }

    let mut headers = HeaderMap::new();
    headers.insert("Content-Length", meta.size.to_string().parse().unwrap());
    headers.insert("ETag", meta.etag.parse().unwrap());
    Ok((StatusCode::OK, headers, body))
}

async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    validate_bucket(&bucket)?;
    let existed = state.metadata.delete_object(&bucket, &key).await?;
    let status = if existed {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    };
    Ok(status)
}
