use serde::Deserialize;
use std::{net::SocketAddr, path::Path};

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    #[serde(default = "default_api_service_addr")]
    pub api_service_addr: String,
    #[serde(default = "default_metadata_url")]
    pub metadata_service_url: String,
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,
    #[serde(default = "default_get_batch_window")]
    pub get_batch_window: usize,
    #[serde(default = "default_gc_batch_size")]
    pub gc_batch_size: usize,
}

fn default_api_service_addr() -> String {
    "0.0.0.0:3000".into()
}
fn default_metadata_url() -> String {
    "http://127.0.0.1:3001".into()
}
fn default_chunk_size() -> usize {
    256 * 1024
}
fn default_get_batch_window() -> usize {
    16
}
fn default_gc_batch_size() -> usize {
    128
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_service_addr: default_api_service_addr(),
            metadata_service_url: default_metadata_url(),
            chunk_size: default_chunk_size(),
            get_batch_window: default_get_batch_window(),
            gc_batch_size: default_gc_batch_size(),
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
                let mut config = Self::default();
                config.apply_env_overrides()?;
                return Ok(config);
            }
            Err(e) => return Err(anyhow::anyhow!("read config {}: {}", path.display(), e)),
        };
        let mut config: Config =
            serde_yaml::from_str(&yaml).map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        config.apply_env_overrides()?;
        Ok(config)
    }

    fn apply_env_overrides(&mut self) -> anyhow::Result<()> {
        if let Ok(raw) = std::env::var("API_GET_BATCH_WINDOW") {
            self.get_batch_window = raw
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid API_GET_BATCH_WINDOW {}: {}", raw, e))?;
        }
        if let Ok(raw) = std::env::var("API_GC_BATCH_SIZE") {
            self.gc_batch_size = raw
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid API_GC_BATCH_SIZE {}: {}", raw, e))?;
        }
        Ok(())
    }

    pub fn socket_addr(&self) -> anyhow::Result<SocketAddr> {
        self.api_service_addr.parse().map_err(|e| {
            anyhow::anyhow!("invalid api_service_addr {}: {}", self.api_service_addr, e)
        })
    }
}
