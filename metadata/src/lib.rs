use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkRef {
    pub chunk_id: String,
    pub node_id: String,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ListBucketResponse {
    pub keys: Vec<String>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GcTask {
    pub seq: u64,
    pub chunk_id: String,
    pub node_id: String,
    pub base_url: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GcAckBatchRequest {
    pub owner: String,
    pub seqs: Vec<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GcAckBatchResult {
    pub acked: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkInUseResult {
    pub in_use: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkerNode {
    pub node_id: String,
    pub base_url: String,
    pub healthy: bool,
}
