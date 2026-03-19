use anyhow::Result;
use metadata::{
    ChunkInUseResult, ChunkerNode, GcAckBatchRequest, GcAckBatchResult, GcTask, ListBucketResponse,
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

    pub async fn gc_next_batch(
        &self,
        limit: usize,
        owner: &str,
        lease_seconds: u64,
    ) -> Result<Vec<GcTask>> {
        let url = format!("{}/gc/next-batch", self.base_url);
        let res = self
            .client
            .get(&url)
            .query(&[
                ("limit", limit.to_string()),
                ("owner", owner.to_string()),
                ("lease_seconds", lease_seconds.to_string()),
            ])
            .send()
            .await?;
        let res = res.error_for_status()?;
        let tasks = res.json().await?;
        Ok(tasks)
    }

    pub async fn gc_ack_batch(&self, owner: &str, seqs: &[u64]) -> Result<u64> {
        let url = format!("{}/gc/ack-batch", self.base_url);
        let body = GcAckBatchRequest {
            owner: owner.to_string(),
            seqs: seqs.to_vec(),
        };
        let res = self.client.put(&url).json(&body).send().await?;
        let res = res.error_for_status()?;
        let result: GcAckBatchResult = res.json().await?;
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

    pub async fn list_bucket_keys(
        &self,
        bucket: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<ListBucketResponse> {
        let encoded_bucket = urlencoding::encode(bucket).into_owned();
        let url = format!("{}/buckets/{}/objects", self.base_url, encoded_bucket);
        let mut req = self.client.get(&url);
        if let Some(limit) = limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(cursor) = cursor {
            req = req.query(&[("cursor", cursor)]);
        }
        let res = req.send().await?;
        let res = res.error_for_status()?;
        let body = res.json().await?;
        Ok(body)
    }
}
