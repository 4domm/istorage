use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use api_service::api::create_router;
use api_service::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load()?;
    let log_filter = std::env::var("API_SERVICE_LOG_FILTER")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "api_service=debug,tower_http=debug".into());
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(log_filter))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let addr = config.socket_addr()?;
    let app = create_router(config).await?;
    tracing::info!("API server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
