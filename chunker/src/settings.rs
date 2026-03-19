use serde::Deserialize;
use std::path::Path as FsPath;

pub struct Settings {
    pub port: u16,
    pub db_path: String,
    pub max_body_bytes: usize,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    #[serde(default = "default_chunk_size")]
    chunk_size: usize,
}

fn default_chunk_size() -> usize {
    256 * 1024
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            chunk_size: default_chunk_size(),
        }
    }
}

impl FileConfig {
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
        let config: FileConfig =
            serde_yaml::from_str(&yaml).map_err(|e| anyhow::anyhow!("parse config: {}", e))?;
        Ok(config)
    }
}

impl Settings {
    pub fn load() -> anyhow::Result<Self> {
        let port: u16 = std::env::var("CHUNK_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3002);
        let db_path = std::env::var("CHUNK_DB_PATH").unwrap_or_else(|_| "./data/chunks".into());

        let config = FileConfig::load()?;
        let max_body_bytes = std::env::var("CHUNK_MAX_BODY_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(config.chunk_size);

        Ok(Self {
            port,
            db_path,
            max_body_bytes,
        })
    }
}
