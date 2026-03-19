use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use serde::Deserialize;

use metadata::{
    ChunkInUseResult, ChunkerNode, GcAckBatchRequest, GcAckBatchResult, GcTask, ListBucketResponse,
    ObjectMeta, PutObjectRequest,
};

use crate::repo::MetadataRepo;

const DEFAULT_LIST_LIMIT: usize = 100;
const MAX_LIST_LIMIT: usize = 1000;
const DEFAULT_GC_BATCH_LIMIT: usize = 128;
const MAX_GC_BATCH_LIMIT: usize = 1000;

#[derive(Debug, Deserialize)]
struct ListBucketQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcNextBatchQuery {
    limit: Option<usize>,
    owner: Option<String>,
    lease_seconds: Option<u64>,
}

fn normalized_limit(
    limit: Option<usize>,
    default_limit: usize,
    max_limit: usize,
) -> Result<usize, (StatusCode, String)> {
    let limit = limit.unwrap_or(default_limit);
    if limit == 0 {
        return Err((StatusCode::BAD_REQUEST, "limit must be > 0".into()));
    }
    Ok(limit.min(max_limit))
}

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

async fn put_object(
    State(repo): State<MetadataRepo>,
    Path((bucket, key)): Path<(String, String)>,
    Json(body): Json<PutObjectRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    if bucket.is_empty() || key.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bucket and key required".into()));
    }
    repo.put_object(&bucket, &key, body)
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::OK)
}

async fn get_object(
    State(repo): State<MetadataRepo>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Json<ObjectMeta>, (StatusCode, String)> {
    let meta = repo
        .get_object(&bucket, &key)
        .await
        .map_err(internal_error)?;
    match meta {
        Some(meta) => Ok(Json(meta)),
        None => Err((StatusCode::NOT_FOUND, "object not found".into())),
    }
}

async fn list_bucket(
    State(repo): State<MetadataRepo>,
    Path(bucket): Path<String>,
    Query(query): Query<ListBucketQuery>,
) -> Result<Json<ListBucketResponse>, (StatusCode, String)> {
    if bucket.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bucket required".into()));
    }
    let limit = normalized_limit(query.limit, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT)?;
    let (keys, next_cursor) = repo
        .list_bucket_keys(&bucket, limit, query.cursor.as_deref())
        .await
        .map_err(internal_error)?;
    Ok(Json(ListBucketResponse { keys, next_cursor }))
}

async fn delete_object(
    State(repo): State<MetadataRepo>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let deleted = repo
        .delete_object(&bucket, &key)
        .await
        .map_err(internal_error)?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((StatusCode::NOT_FOUND, "object not found".into()))
    }
}

async fn gc_next_batch(
    State(repo): State<MetadataRepo>,
    Query(query): Query<GcNextBatchQuery>,
) -> Result<Json<Vec<GcTask>>, (StatusCode, String)> {
    let limit = normalized_limit(query.limit, DEFAULT_GC_BATCH_LIMIT, MAX_GC_BATCH_LIMIT)?;
    let owner = query
        .owner
        .as_deref()
        .filter(|owner| !owner.is_empty())
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "owner is required".into()))?;
    let lease_seconds = query.lease_seconds.unwrap_or(30).max(1);
    let tasks = repo
        .gc_next_batch(limit, owner, lease_seconds)
        .await
        .map_err(internal_error)?;
    Ok(Json(tasks))
}

async fn gc_ack_batch(
    State(repo): State<MetadataRepo>,
    Json(body): Json<GcAckBatchRequest>,
) -> Result<Json<GcAckBatchResult>, (StatusCode, String)> {
    if body.owner.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "owner is required".into()));
    }
    let acked = repo
        .gc_ack_batch(&body.owner, &body.seqs)
        .await
        .map_err(internal_error)?;
    Ok(Json(GcAckBatchResult { acked }))
}

async fn chunk_in_use(
    State(repo): State<MetadataRepo>,
    Path((node_id, chunk_id)): Path<(String, String)>,
) -> Result<Json<ChunkInUseResult>, (StatusCode, String)> {
    let in_use = repo
        .chunk_in_use(&node_id, &chunk_id)
        .await
        .map_err(internal_error)?;
    Ok(Json(ChunkInUseResult { in_use }))
}

async fn list_chunker_nodes(
    State(repo): State<MetadataRepo>,
) -> Result<Json<Vec<ChunkerNode>>, (StatusCode, String)> {
    let nodes = repo.list_chunker_nodes().await.map_err(internal_error)?;
    Ok(Json(nodes))
}

pub fn build_router(repo: MetadataRepo) -> Router {
    let traced_routes = Router::new()
        .route(
            "/objects/:bucket/*key",
            put(put_object).get(get_object).delete(delete_object),
        )
        .route("/buckets/:bucket/objects", get(list_bucket))
        .route("/chunks/:node_id/:chunk_id/in-use", get(chunk_in_use))
        .route("/chunkers", get(list_chunker_nodes))
        .with_state(repo.clone())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let gc_routes = Router::new()
        .route("/gc/next-batch", get(gc_next_batch))
        .route("/gc/ack-batch", put(gc_ack_batch))
        .with_state(repo);

    traced_routes.merge(gc_routes)
}
