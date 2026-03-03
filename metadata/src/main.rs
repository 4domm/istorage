use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, put},
    Json, Router,
};
use foundationdb::Database;
use futures::prelude::*;
use std::sync::Arc;

use metadata::{ObjectMeta, PutObjectRequest};

fn object_key(bucket: &str, key: &str) -> Vec<u8> {
    format!("obj:{}\0{}", bucket, key).into_bytes()
}

fn fdb_error_to_status(e: foundationdb::FdbError) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
}

async fn put_object(
    State(db): State<Arc<Database>>,
    Path((bucket, key)): Path<(String, String)>,
    Json(body): Json<PutObjectRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    if bucket.is_empty() || key.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "bucket and key required".into()));
    }
    let key_bytes = object_key(&bucket, &key);
    let value = serde_json::to_vec(&ObjectMeta {
        size: body.size,
        etag: body.etag.clone(),
        manifest: body.manifest.clone(),
    })
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let key_bytes2 = key_bytes.clone();
    let value2 = value.clone();
    db.run(|trx, _| async move {
        trx.set(&key_bytes2, &value2);
        Ok(())
    })
    .await
    .map_err(fdb_error_to_status)?;
    Ok(StatusCode::OK)
}

async fn get_object(
    State(db): State<Arc<Database>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Json<ObjectMeta>, (StatusCode, String)> {
    let key_bytes = object_key(&bucket, &key);
    let value: Option<Vec<u8>> = db
        .run(|trx, _| async move { trx.get(&key_bytes, false).await })
        .await
        .map_err(fdb_error_to_status)?
        .map(|s| s.as_ref().to_vec());
    let value = value.ok_or_else(|| (StatusCode::NOT_FOUND, "object not found".to_string()))?;
    let meta: ObjectMeta =
        serde_json::from_slice(&value).map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(meta))
}

async fn delete_object(
    State(db): State<Arc<Database>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<StatusCode, (StatusCode, String)> {
    let key_bytes = object_key(&bucket, &key);
    let existed: bool = db
        .run(|trx, _| async move {
            let v = trx.get(&key_bytes, false).await?;
            let existed = v.is_some();
            if existed {
                trx.clear(&key_bytes);
            }
            Ok(existed)
        })
        .await
        .map_err(fdb_error_to_status)?;
    if !existed {
        return Err((StatusCode::NOT_FOUND, "object not found".into()));
    }
    Ok(StatusCode::NO_CONTENT)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _network = unsafe { foundationdb::boot() };

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "metadata=info,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let port: u16 = std::env::var("METADATA_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001);

    let db = foundationdb::Database::default()?;
    let db = Arc::new(db);

    let app = Router::new()
        .route("/objects/:bucket/*key", put(put_object).get(get_object).delete(delete_object))
        .with_state(db)
        .layer(tower_http::trace::TraceLayer::new_for_http());

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("Metadata service listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
