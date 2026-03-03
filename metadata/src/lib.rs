use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkRef {
    pub chunk_id: String,
    pub offset: u64,
    pub size: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub size: u64,
    pub etag: String,
    pub manifest: Vec<ChunkRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PutObjectRequest {
    pub size: u64,
    pub etag: String,
    pub manifest: Vec<ChunkRef>,
}
