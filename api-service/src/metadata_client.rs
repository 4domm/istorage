use anyhow::Result;
use metadata::{ObjectMeta, PutObjectRequest};
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
        let encoded_key = urlencoding::encode(key).into_owned();
        format!("{}/objects/{}/{}", self.base_url, bucket, encoded_key)
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
        res.error_for_status()?;
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
}
