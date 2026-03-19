use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use std::sync::Arc;

type Db = Arc<rocksdb::DB>;

#[derive(Clone)]
pub struct AppState {
    db: Db,
    max_body_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct BatchGetRequest {
    chunk_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct BatchDeleteRequest {
    chunk_ids: Vec<String>,
}

fn validate_chunk_id(chunk_id: &str) -> Result<(), (StatusCode, String)> {
    if chunk_id.is_empty() || chunk_id.contains('/') {
        return Err((StatusCode::BAD_REQUEST, "invalid chunk_id".into()));
    }
    Ok(())
}

async fn put_chunk(
    State(state): State<AppState>,
    Path(chunk_id): Path<String>,
    body: axum::body::Body,
) -> Result<StatusCode, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let data = axum::body::to_bytes(body, state.max_body_bytes)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let db = state.db.clone();
    let result = tokio::task::spawn_blocking(move || {
        let existed = db.get(chunk_id.as_bytes())?.is_some();
        if !existed {
            db.put(chunk_id.as_bytes(), data.as_ref())?;
        }
        Ok::<bool, rocksdb::Error>(existed)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let existed = result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    })
}

async fn get_chunk(
    State(state): State<AppState>,
    Path(chunk_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let db = state.db.clone();
    let value = tokio::task::spawn_blocking(move || db.get(chunk_id.as_bytes()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "chunk not found".to_string()))?;
    Ok((StatusCode::OK, value))
}

async fn batch_get_chunks(
    State(state): State<AppState>,
    Json(body): Json<BatchGetRequest>,
) -> Result<(StatusCode, Bytes), (StatusCode, String)> {
    if body.chunk_ids.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "chunk_ids must be non-empty".into(),
        ));
    }
    for chunk_id in &body.chunk_ids {
        validate_chunk_id(chunk_id)?;
    }

    let db = state.db.clone();
    let chunk_ids = body.chunk_ids;
    let framed = tokio::task::spawn_blocking(move || {
        // Binary response contract (all-or-nothing):
        // u32 count
        // repeated count times:
        //   u32 chunk_id_len
        //   [chunk_id bytes]
        //   u64 payload_len
        //   [payload bytes]
        let mut response = BytesMut::new();
        response.put_u32(chunk_ids.len() as u32);

        for chunk_id in chunk_ids {
            let bytes = match db.get(chunk_id.as_bytes()).map_err(|e| e.to_string())? {
                Some(bytes) => bytes,
                None => return Err("batch chunk not found".to_string()),
            };
            response.put_u32(chunk_id.len() as u32);
            response.extend_from_slice(chunk_id.as_bytes());
            response.put_u64(bytes.len() as u64);
            response.extend_from_slice(&bytes);
        }

        Ok::<Bytes, String>(response.freeze())
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| {
        if e == "batch chunk not found" {
            (StatusCode::NOT_FOUND, e)
        } else {
            (StatusCode::INTERNAL_SERVER_ERROR, e)
        }
    })?;

    Ok((StatusCode::OK, framed))
}

async fn delete_chunk(
    State(state): State<AppState>,
    Path(chunk_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let db = state.db.clone();
    let result = tokio::task::spawn_blocking(move || db.delete(chunk_id.as_bytes()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn batch_delete_chunks(
    State(state): State<AppState>,
    Json(body): Json<BatchDeleteRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    if body.chunk_ids.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "chunk_ids must be non-empty".into(),
        ));
    }
    for chunk_id in &body.chunk_ids {
        validate_chunk_id(chunk_id)?;
    }

    let db = state.db.clone();
    let chunk_ids = body.chunk_ids;
    tokio::task::spawn_blocking(move || {
        let mut batch = rocksdb::WriteBatch::default();
        for chunk_id in &chunk_ids {
            batch.delete(chunk_id.as_bytes());
        }
        db.write(batch)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> StatusCode {
    StatusCode::OK
}

pub fn create_router(db: Arc<rocksdb::DB>, max_body_bytes: usize) -> Router {
    let state = AppState { db, max_body_bytes };
    Router::new()
        .route("/health", get(health))
        .route("/chunks/batch-get", post(batch_get_chunks))
        .route("/chunks/batch-delete", post(batch_delete_chunks))
        .route(
            "/chunks/:chunk_id",
            put(put_chunk).get(get_chunk).delete(delete_chunk),
        )
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http())
}
