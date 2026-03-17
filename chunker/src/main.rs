use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, put},
    Router,
};
use std::sync::Arc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

type Db = Arc<rocksdb::DB>;

fn validate_chunk_id(chunk_id: &str) -> Result<(), (StatusCode, String)> {
    if chunk_id.is_empty() || chunk_id.contains('/') {
        return Err((StatusCode::BAD_REQUEST, "invalid chunk_id".into()));
    }
    Ok(())
}

async fn put_chunk(
    State(db): State<Db>,
    Path(chunk_id): Path<String>,
    body: axum::body::Body,
) -> Result<StatusCode, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let data = axum::body::to_bytes(body, 128 * 1024 * 1024)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let db = db.clone();
    let result = tokio::task::spawn_blocking(move || {
        let existed = db.get(chunk_id.as_bytes())?.is_some();
        db.put(chunk_id.as_bytes(), data.as_ref())?;
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
    State(db): State<Db>,
    Path(chunk_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let db = db.clone();
    let value = tokio::task::spawn_blocking(move || db.get(chunk_id.as_bytes()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "chunk not found".to_string()))?;
    Ok((StatusCode::OK, value))
}

async fn delete_chunk(
    State(db): State<Db>,
    Path(chunk_id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    validate_chunk_id(&chunk_id)?;
    let db = db.clone();
    let result = tokio::task::spawn_blocking(move || db.delete(chunk_id.as_bytes()))
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    result.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

async fn health() -> StatusCode {
    StatusCode::OK
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = std::env::var("CHUNKER_LOG_FILTER")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "chunker=info,tower_http=debug".into());

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(log_filter))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port: u16 = std::env::var("CHUNK_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3002);
    let db_path = std::env::var("CHUNK_DB_PATH").unwrap_or_else(|_| "./data/chunks".into());

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    let db = rocksdb::DB::open(&opts, &db_path)?;
    let db = Arc::new(db);

    let app = Router::new()
        .route("/health", get(health))
        .route(
            "/chunks/:chunk_id",
            put(put_chunk).get(get_chunk).delete(delete_chunk),
        )
        .with_state(db)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Chunk storage listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
