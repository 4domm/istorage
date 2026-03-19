use std::time::Duration;

use metadata::ChunkerNode;

pub struct Settings {
    pub port: u16,
    pub database_url: String,
    pub chunker_nodes: Vec<ChunkerNode>,
    pub healthcheck_interval: Duration,
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

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        let port: u16 = std::env::var("METADATA_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3001);

        let database_url = std::env::var("METADATA_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .unwrap_or_else(|_| "postgres://sstorage:sstorage@127.0.0.1:5432/sstorage".into());

        let chunker_nodes = parse_chunker_nodes(
            &std::env::var("CHUNKER_NODES")
                .unwrap_or_else(|_| "chunker=http://127.0.0.1:3002".into()),
        )?;

        let healthcheck_interval = Duration::from_millis(
            std::env::var("CHUNKER_HEALTHCHECK_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000),
        );

        Ok(Self {
            port,
            database_url,
            chunker_nodes,
            healthcheck_interval,
        })
    }
}
