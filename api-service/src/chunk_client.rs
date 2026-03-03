use api_service::error::ChunkServiceError;
use bytes::Bytes;
use reqwest::Client;

#[derive(Clone)]
pub struct ChunkClient {
    client: Client,
    base_url: String,
}

impl ChunkClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn chunk_url(&self, chunk_id: &str) -> String {
        format!("{}/chunks/{}", self.base_url, chunk_id)
    }

    pub async fn put_chunk(&self, chunk_id: &str, data: Bytes) -> Result<(), ChunkServiceError> {
        let url = self.chunk_url(chunk_id);
        self.client
            .put(&url)
            .body(data)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest::Error::from)?;
        Ok(())
    }

    pub async fn get_chunk(&self, chunk_id: &str) -> Result<Bytes, ChunkServiceError> {
        let url = self.chunk_url(chunk_id);
        let res = self.client.get(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ChunkServiceError::NotFound(chunk_id.to_string()));
        }
        res.error_for_status().map_err(ChunkServiceError::Request)?;
        let bytes = res.bytes().await.map_err(ChunkServiceError::Request)?;
        Ok(bytes)
    }

    pub async fn delete_chunk(&self, chunk_id: &str) -> Result<(), ChunkServiceError> {
        let url = self.chunk_url(chunk_id);
        let res = self.client.delete(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        res.error_for_status().map_err(ChunkServiceError::Request)?;
        Ok(())
    }
}
