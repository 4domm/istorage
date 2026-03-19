use anyhow::Result;
use metadata::{
    ChunkInUseResult, ChunkerNode, GcAckRequest, GcAckResult, GcTask, ListBucketResponse,
    ObjectMeta, PutObjectRequest,
};
use reqwest::Client;

#[derive(Clone)]
pub struct MetadataClient {
    client: Client,
    base_url: String,
}

impl MetadataClient {
    pub fn new(base_url: String) -> Self {
        Self {
            client: Client::new(),
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn path(&self, bucket: &str, key: &str) -> String {
        let encoded_bucket = urlencoding::encode(bucket).into_owned();
        let encoded_key = urlencoding::encode(key).into_owned();
        format!(
            "{}/objects/{}/{}",
            self.base_url, encoded_bucket, encoded_key
        )
    }

    pub async fn put_object(&self, bucket: &str, key: &str, req: PutObjectRequest) -> Result<()> {
        let url = self.path(bucket, key);
        let res = self.client.put(&url).json(&req).send().await?;
        res.error_for_status()?;
        Ok(())
    }

    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>> {
        let url = self.path(bucket, key);
        let res = self.client.get(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let res = res.error_for_status()?;
        let meta = res.json().await?;
        Ok(Some(meta))
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<bool> {
        let url = self.path(bucket, key);
        let res = self.client.delete(&url).send().await?;
        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        res.error_for_status()?;
        Ok(true)
    }

    pub async fn gc_next(&self) -> Result<Option<GcTask>> {
        let url = format!("{}/gc/next", self.base_url);
        let res = self.client.get(&url).send().await?;
        let res = res.error_for_status()?;
        let task = res.json().await?;
        Ok(task)
    }

    pub async fn gc_ack(&self, seq: u64) -> Result<bool> {
        let url = format!("{}/gc/ack", self.base_url);
        let body = GcAckRequest { seq };
        let res = self.client.put(&url).json(&body).send().await?;
        let res = res.error_for_status()?;
        let result: GcAckResult = res.json().await?;
        Ok(result.acked)
    }

    pub async fn chunk_in_use(&self, node_id: &str, chunk_id: &str) -> Result<bool> {
        let encoded_node_id = urlencoding::encode(node_id).into_owned();
        let url = format!(
            "{}/chunks/{}/{}/in-use",
            self.base_url, encoded_node_id, chunk_id
        );
        let res = self.client.get(&url).send().await?;
        let res = res.error_for_status()?;
        let result: ChunkInUseResult = res.json().await?;
        Ok(result.in_use)
    }

    pub async fn list_chunker_nodes(&self) -> Result<Vec<ChunkerNode>> {
        let url = format!("{}/chunkers", self.base_url);
        let res = self.client.get(&url).send().await?;
        let res = res.error_for_status()?;
        let nodes = res.json().await?;
        Ok(nodes)
    }

    pub async fn list_bucket_keys(&self, bucket: &str) -> Result<Vec<String>> {
        let encoded_bucket = urlencoding::encode(bucket).into_owned();
        let url = format!("{}/buckets/{}/objects", self.base_url, encoded_bucket);
        let res = self.client.get(&url).send().await?;
        let res = res.error_for_status()?;
        let body: ListBucketResponse = res.json().await?;
        Ok(body.keys)
    }
}
