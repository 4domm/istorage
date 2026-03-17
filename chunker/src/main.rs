use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, put},
    Json, Router,
};
use bytes::{BufMut, Bytes, BytesMut};
use serde::Deserialize;
use std::{path::Path as FsPath, sync::Arc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

type Db = Arc<rocksdb::DB>;

#[derive(Clone)]
struct AppState {
    db: Db,
    max_body_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct Config {
    #[serde(default = "default_chunk_size")]
    chunk_size: usize,
}

#[derive(Debug, Deserialize)]
struct BatchGetRequest {
    chunk_ids: Vec<String>,
}

fn default_chunk_size() -> usize {
    256 * 1024
}

impl Default for Config {
    fn default() -> Self {
        Self {
            chunk_size: default_chunk_size(),
        }
    }
}

impl Config {
    fn load() -> anyhow::Result<Self> {
        let path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.yaml".into());
        Self::load_from_path(path.as_ref())
    }

    fn load_from_path(path: &FsPath) -> anyhow::Result<Self> {
        let yaml = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Self::default()),
            Err(e) => return Err(anyhow::anyhow!("read config {}: {}", path.display(), e)),
        };
        let config: Config =
            serde_yaml::from_str(&yaml).map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        Ok(config)
    }
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
    let chunks = tokio::task::spawn_blocking(move || {
        let mut results = Vec::with_capacity(chunk_ids.len());
        for chunk_id in chunk_ids {
            let bytes = db
                .get(chunk_id.as_bytes())
                .map_err(|e| e.to_string())?
                .ok_or_else(|| "chunk not found".to_string())?;
            results.push((chunk_id, bytes));
        }
        Ok::<Vec<(String, Vec<u8>)>, String>(results)
    })
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| {
        let status = if e.contains("chunk not found") {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };
        (status, e)
    })?;

    let mut response = BytesMut::new();
    for (chunk_id, bytes) in chunks {
        response.put_u32(chunk_id.len() as u32);
        response.extend_from_slice(chunk_id.as_bytes());
        response.put_u64(bytes.len() as u64);
        response.extend_from_slice(&bytes);
    }

    Ok((StatusCode::OK, response.freeze()))
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
    let config = Config::load()?;
    let max_body_bytes = std::env::var("CHUNK_MAX_BODY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(config.chunk_size);

    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    let db = rocksdb::DB::open(&opts, &db_path)?;
    let state = AppState {
        db: Arc::new(db),
        max_body_bytes,
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/chunks/batch-get", post(batch_get_chunks))
        .route(
            "/chunks/:chunk_id",
            put(put_chunk).get(get_chunk).delete(delete_chunk),
        )
        .with_state(state)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Chunk storage listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
