use anyhow::Result;
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use api_service::api::create_router;
use api_service::config::Config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "api_service=debug,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Config::load()?;
    let app = create_router(config).await?;
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    tracing::info!("API server listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
