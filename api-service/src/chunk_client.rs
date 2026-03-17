use crate::error::ChunkServiceError;
use bytes::{Buf, Bytes};
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Clone)]
pub struct ChunkClient {
    client: Client,
}

#[derive(Serialize)]
struct BatchGetRequest<'a> {
    chunk_ids: &'a [String],
}

impl ChunkClient {
    pub fn new() -> Self {
        Self {
            client: Client::new(),
        }
    }

    fn chunk_url(&self, base_url: &str, chunk_id: &str) -> String {
        format!("{}/chunks/{}", base_url.trim_end_matches('/'), chunk_id)
    }

    fn batch_get_url(&self, base_url: &str) -> String {
        format!("{}/chunks/batch-get", base_url.trim_end_matches('/'))
    }

    pub async fn put_chunk(
        &self,
        base_url: &str,
        chunk_id: &str,
        data: Bytes,
    ) -> Result<bool, ChunkServiceError> {
        let url = self.chunk_url(base_url, chunk_id);
        let res = self
            .client
            .put(&url)
            .body(data)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest::Error::from)?;
        Ok(res.status() == reqwest::StatusCode::CREATED)
    }

    pub async fn batch_get_chunks(
        &self,
        base_url: &str,
        chunk_ids: &[String],
    ) -> Result<HashMap<String, Bytes>, ChunkServiceError> {
        let url = self.batch_get_url(base_url);
        let req = BatchGetRequest { chunk_ids };
        let res = self.client.post(&url).json(&req).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ChunkServiceError::NotFound("batch chunk not found".into()));
        }
        let res = res.error_for_status().map_err(ChunkServiceError::Request)?;
        let body = res.bytes().await.map_err(ChunkServiceError::Request)?;
        Self::decode_batch_response(body)
    }

    fn decode_batch_response(body: Bytes) -> Result<HashMap<String, Bytes>, ChunkServiceError> {
        let mut buf = body;
        let mut chunks = HashMap::new();

        while buf.has_remaining() {
            if buf.remaining() < 4 {
                return Err(ChunkServiceError::InvalidResponse(
                    "invalid batch chunk framing: missing id length".into(),
                ));
            }
            let id_len = buf.get_u32() as usize;
            if buf.remaining() < id_len + 8 {
                return Err(ChunkServiceError::InvalidResponse(
                    "invalid batch chunk framing: truncated id or size".into(),
                ));
            }
            let chunk_id = String::from_utf8(buf.copy_to_bytes(id_len).to_vec())
                .map_err(|e| ChunkServiceError::InvalidResponse(e.to_string()))?;
            let data_len = buf.get_u64() as usize;
            if buf.remaining() < data_len {
                return Err(ChunkServiceError::InvalidResponse(
                    "invalid batch chunk framing: truncated payload".into(),
                ));
            }
            let chunk_bytes = buf.copy_to_bytes(data_len);
            chunks.insert(chunk_id, chunk_bytes);
        }

        Ok(chunks)
    }

    pub async fn delete_chunk(
        &self,
        base_url: &str,
        chunk_id: &str,
    ) -> Result<(), ChunkServiceError> {
        let url = self.chunk_url(base_url, chunk_id);
        let res = self.client.delete(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        res.error_for_status().map_err(ChunkServiceError::Request)?;
        Ok(())
    }
}
