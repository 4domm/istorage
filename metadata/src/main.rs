mod repo;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use reqwest::Client;
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use metadata::{
    ChunkInUseResult, ChunkerNode, GcAckRequest, GcAckResult, GcTask, ObjectMeta, PutObjectRequest,
};
use repo::MetadataRepo;

fn internal_error(err: anyhow::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

fn parse_chunker_nodes(value: &str) -> anyhow::Result<Vec<ChunkerNode>> {
    let mut nodes = Vec::new();
    for entry in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (node_id, base_url) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("invalid CHUNKER_NODES entry: {}", entry))?;
        nodes.push(ChunkerNode {
            node_id: node_id.trim().to_string(),
            base_url: base_url.trim().trim_end_matches('/').to_string(),
            healthy: true,
        });
    }
    if nodes.is_empty() {
        return Err(anyhow::anyhow!(
            "CHUNKER_NODES must contain at least one node"
        ));
    }
    Ok(nodes)
}

fn spawn_chunker_health_manager(repo: MetadataRepo, interval: Duration) {
    tokio::spawn(async move {
        let client = Client::new();
        loop {
            match repo.list_chunker_nodes().await {
                Ok(nodes) => {
                    for node in nodes {
                        let health_url = format!("{}/health", node.base_url.trim_end_matches('/'));
                        let result = client.get(&health_url).send().await;
                        match result {
                            Ok(response) if response.status().is_success() => {
                                if let Err(err) =
                                    repo.update_chunker_health(&node.node_id, true, None).await
                                {
                                    tracing::warn!(
                                        "failed to update healthy status for {}: {}",
                                        node.node_id,
                                        err
                                    );
                                }
                            }
                            Ok(response) => {
                                let msg = format!("health check returned {}", response.status());
                                if let Err(err) = repo
                                    .update_chunker_health(&node.node_id, false, Some(&msg))
                                    .await
                                {
                                    tracing::warn!(
                                        "failed to update unhealthy status for {}: {}",
                                        node.node_id,
                                        err
                                    );
                                }
                            }
                            Err(err) => {
                                let msg = err.to_string();
                                if let Err(update_err) = repo
                                    .update_chunker_health(&node.node_id, false, Some(&msg))
                                    .await
                                {
                                    tracing::warn!(
                                        "failed to update unhealthy status for {}: {}",
                                        node.node_id,
                                        update_err
                                    );
                                }
                            }
                        }
                    }
                }
                Err(err) => tracing::warn!("chunker health manager list failed: {}", err),
            }
            tokio::time::sleep(interval).await;
        }
    });
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

async fn gc_next(
    State(repo): State<MetadataRepo>,
) -> Result<Json<Option<GcTask>>, (StatusCode, String)> {
    let task = repo.gc_next().await.map_err(internal_error)?;
    Ok(Json(task))
}

async fn gc_ack(
    State(repo): State<MetadataRepo>,
    Json(body): Json<GcAckRequest>,
) -> Result<Json<GcAckResult>, (StatusCode, String)> {
    let result = repo.gc_ack(body.seq).await.map_err(internal_error)?;
    Ok(Json(result))
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = std::env::var("METADATA_LOG_FILTER")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "metadata=info,tower_http=debug".into());

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(log_filter))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port: u16 = std::env::var("METADATA_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001);

    let database_url = std::env::var("METADATA_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgres://sstorage:sstorage@127.0.0.1:5432/sstorage".into());
    let chunker_nodes = parse_chunker_nodes(
        &std::env::var("CHUNKER_NODES").unwrap_or_else(|_| "chunker=http://127.0.0.1:3002".into()),
    )?;
    let healthcheck_interval = Duration::from_millis(
        std::env::var("CHUNKER_HEALTHCHECK_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000),
    );

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    let repo = MetadataRepo::new(pool);
    repo.sync_chunker_nodes(&chunker_nodes).await?;
    spawn_chunker_health_manager(repo.clone(), healthcheck_interval);

    let traced_routes = Router::new()
        .route(
            "/objects/:bucket/*key",
            put(put_object).get(get_object).delete(delete_object),
        )
        .route("/chunks/:node_id/:chunk_id/in-use", get(chunk_in_use))
        .route("/chunkers", get(list_chunker_nodes))
        .with_state(repo.clone())
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let gc_routes = Router::new()
        .route("/gc/next", get(gc_next))
        .route("/gc/ack", put(gc_ack))
        .with_state(repo);

    let app = traced_routes.merge(gc_routes);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Metadata service listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
