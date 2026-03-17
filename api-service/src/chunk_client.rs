use crate::error::ChunkServiceError;
use bytes::Bytes;
use reqwest::Client;

#[derive(Clone)]
pub struct ChunkClient {
    client: Client,
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

    pub async fn get_chunk(
        &self,
        base_url: &str,
        chunk_id: &str,
    ) -> Result<Bytes, ChunkServiceError> {
        let url = self.chunk_url(base_url, chunk_id);
        let res = self.client.get(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ChunkServiceError::NotFound(chunk_id.to_string()));
        }
        let res = res.error_for_status().map_err(ChunkServiceError::Request)?;
        let bytes = res.bytes().await.map_err(ChunkServiceError::Request)?;
        Ok(bytes)
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
