mod http;
mod settings;

use std::sync::Arc;

use http::create_router;
use settings::Settings;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = std::env::var("CHUNKER_LOG_FILTER")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "chunker=info,tower_http=debug".into());

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(log_filter))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let settings = Settings::load()?;
    let mut opts = rocksdb::Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(rocksdb::DB::open(&opts, &settings.db_path)?);

    let app = create_router(db, settings.max_body_bytes);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], settings.port));
    tracing::info!("Chunk storage listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
