use reqwest::StatusCode;
use uuid::Uuid;

/// S3-compatible object storage client using path-style URL uploads.
#[derive(Debug, Clone)]
pub struct ObjectStoreClient {
    endpoint: String,
    region: String,
    bucket: String,
    prefix: String,
    client: reqwest::Client,
}

impl ObjectStoreClient {
    /// Creates storage client from config.
    pub fn new(
        endpoint: &str,
        region: String,
        bucket: String,
        prefix: &str,
    ) -> anyhow::Result<Self> {
        let endpoint = endpoint.trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(anyhow::anyhow!("S3 endpoint must not be empty"));
        }
        if bucket.trim().is_empty() {
            return Err(anyhow::anyhow!("S3 bucket must not be empty"));
        }

        Ok(Self {
            endpoint,
            region,
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
            client: reqwest::Client::new(),
        })
    }

    /// Uploads bytes to object storage and returns object URL.
    pub async fn upload_result(
        &self,
        snapshot_id: Uuid,
        job_id: Uuid,
        suffix: &str,
        extension: &str,
        content_type: &str,
        bytes: Vec<u8>,
    ) -> anyhow::Result<String> {
        let key = self.object_key(snapshot_id, job_id, suffix, extension);
        let url = format!("{}/{}/{}", self.endpoint, self.bucket, key);

        let response = self
            .client
            .put(&url)
            .header("content-type", content_type)
            .header("x-amz-acl", "public-read")
            .header("x-amz-region", &self.region)
            .body(bytes)
            .send()
            .await?;

        if response.status().is_success() {
            return Ok(url);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let body_preview = body.chars().take(240).collect::<String>();
        let auth_hint = if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
            " (authorization likely required for PUT)"
        } else {
            ""
        };

        Err(anyhow::anyhow!(
            "object upload failed status={status}{auth_hint} body={body_preview}"
        ))
    }

    fn object_key(&self, snapshot_id: Uuid, job_id: Uuid, suffix: &str, extension: &str) -> String {
        if self.prefix.is_empty() {
            return format!("snapshots/{snapshot_id}/jobs/{job_id}/{suffix}.{extension}");
        }

        format!(
            "{}/snapshots/{snapshot_id}/jobs/{job_id}/{suffix}.{extension}",
            self.prefix
        )
    }
}
