use serde::Deserialize;
use std::path::Path;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default = "default_metadata_url")]
    pub metadata_service_url: String,
    #[serde(default = "default_chunk_url")]
    pub chunk_service_url: String,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
}

fn default_port() -> u16 {
    3000
}
fn default_metadata_url() -> String {
    "http://127.0.0.1:3001".into()
}
fn default_chunk_url() -> String {
    "http://127.0.0.1:3002".into()
}
fn default_chunk_size() -> usize {
    256 * 1024
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: default_port(),
            metadata_service_url: default_metadata_url(),
            chunk_service_url: default_chunk_url(),
            chunk_size: default_chunk_size(),
        }
    }
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.yaml".into());
        Self::load_from_path(path.as_ref())
    }

    pub fn load_from_path(path: &Path) -> anyhow::Result<Self> {
        let yaml = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("config file not found, using defaults: {}", path.display());
                return Ok(Self::default());
            }
            Err(e) => return Err(anyhow::anyhow!("read config {}: {}", path.display(), e)),
        };
        let config: Config = serde_yaml::from_str(&yaml).map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        Ok(config)
    }
}
