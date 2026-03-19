mod health_manager;
mod http;
mod repo;
mod settings;

use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use health_manager::spawn_chunker_health_manager;
use http::build_router;
use repo::MetadataRepo;
use settings::Settings;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_filter = std::env::var("METADATA_LOG_FILTER")
        .or_else(|_| std::env::var("RUST_LOG"))
        .unwrap_or_else(|_| "metadata=info,tower_http=debug".into());

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(log_filter))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let settings = Settings::load()?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&settings.database_url)
        .await?;

    sqlx::migrate!("./migrations").run(&pool).await?;
    let repo = MetadataRepo::new(pool);
    repo.sync_chunker_nodes(&settings.chunker_nodes).await?;
    spawn_chunker_health_manager(repo.clone(), settings.healthcheck_interval);

    let app = build_router(repo);
    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], settings.port));
    tracing::info!("Metadata service listening on http://{}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
