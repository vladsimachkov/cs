// This crate maps gRPC failures onto S3 responses, so its handlers and backend return
// `tonic::Status` everywhere; the type is too large to shrink from our side.
#![allow(clippy::result_large_err)]

pub mod handler;
pub mod protocol;
pub mod router;
pub(crate) mod s3;

use anyhow::Result;
use coldstore_common::config::GatewayConfig;
use coldstore_proto::common;
use coldstore_proto::scheduler::scheduler_service_client::SchedulerServiceClient;
use coldstore_proto::scheduler::{
    CreateBucketRequest, DeleteBucketRequest, DeleteObjectRequest, HeadBucketRequest,
    HeadObjectRequest, HeadObjectResponse, ListBucketsResponse, ListObjectsRequest,
    ListObjectsResponse, PutObjectMeta, PutObjectRequest, PutObjectResponse, RestoreObjectRequest,
    RestoreObjectResponse,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tonic::transport::Channel;
use tracing::info;

pub struct DownloadedObject {
    pub head: HeadObjectResponse,
    pub body: Vec<u8>,
}

#[tonic::async_trait]
pub trait GatewayBackend: Send + Sync + 'static {
    async fn list_buckets(&self) -> std::result::Result<ListBucketsResponse, tonic::Status>;
    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status>;
    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status>;
    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status>;
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
    ) -> std::result::Result<ListObjectsResponse, tonic::Status>;
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, tonic::Status>;
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<DownloadedObject, tonic::Status>;
    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(), tonic::Status>;
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<HeadObjectResponse, tonic::Status>;
    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: u32,
        tier: common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, tonic::Status>;
}

pub struct GrpcGatewayBackend {
    scheduler_addrs: Vec<String>,
    next_addr: Arc<AtomicUsize>,
}

impl GrpcGatewayBackend {
    pub fn new(scheduler_addrs: Vec<String>) -> Self {
        Self {
            scheduler_addrs,
            next_addr: Arc::new(AtomicUsize::new(0)),
        }
    }

    async fn connect(&self) -> std::result::Result<SchedulerServiceClient<Channel>, tonic::Status> {
        if self.scheduler_addrs.is_empty() {
            return Err(tonic::Status::unavailable(
                "no scheduler addresses configured",
            ));
        }

        let mut last_error: Option<tonic::Status> = None;
        let base = self.next_addr.fetch_add(1, Ordering::Relaxed);
        for offset in 0..self.scheduler_addrs.len() {
            let index = (base + offset) % self.scheduler_addrs.len();
            let addr = normalize_scheduler_addr(&self.scheduler_addrs[index]);
            match SchedulerServiceClient::connect(addr).await {
                Ok(client) => return Ok(client),
                Err(err) => {
                    last_error = Some(tonic::Status::unavailable(err.to_string()));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            tonic::Status::unavailable("all scheduler addresses are unavailable")
        }))
    }
}

fn normalize_scheduler_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

#[tonic::async_trait]
impl GatewayBackend for GrpcGatewayBackend {
    async fn list_buckets(&self) -> std::result::Result<ListBucketsResponse, tonic::Status> {
        let mut client = self.connect().await?;
        client.list_buckets(()).await.map(|r| r.into_inner())
    }

    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        let mut client = self.connect().await?;
        client
            .create_bucket(CreateBucketRequest {
                bucket: bucket.to_string(),
            })
            .await
            .map(|_| ())
    }

    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        let mut client = self.connect().await?;
        client
            .delete_bucket(DeleteBucketRequest {
                bucket: bucket.to_string(),
            })
            .await
            .map(|_| ())
    }

    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        let mut client = self.connect().await?;
        client
            .head_bucket(HeadBucketRequest {
                bucket: bucket.to_string(),
            })
            .await
            .map(|_| ())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
    ) -> std::result::Result<ListObjectsResponse, tonic::Status> {
        let mut client = self.connect().await?;
        client
            .list_objects(ListObjectsRequest {
                bucket: bucket.to_string(),
                prefix: prefix.map(str::to_string),
                marker: marker.map(str::to_string),
                delimiter: delimiter.map(str::to_string),
                max_keys,
            })
            .await
            .map(|r| r.into_inner())
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, tonic::Status> {
        let mut client = self.connect().await?;
        let stream = tokio_stream::iter(vec![
            PutObjectRequest {
                payload: Some(
                    coldstore_proto::scheduler::put_object_request::Payload::Meta(PutObjectMeta {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        content_length: body.len() as u64,
                        content_type,
                        checksum_sha256: None,
                    }),
                ),
            },
            PutObjectRequest {
                payload: Some(coldstore_proto::scheduler::put_object_request::Payload::Data(body)),
            },
        ]);
        client.put_object(stream).await.map(|r| r.into_inner())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<DownloadedObject, tonic::Status> {
        let mut client = self.connect().await?;
        let mut stream = client
            .get_object(coldstore_proto::scheduler::GetObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            })
            .await?
            .into_inner();

        let first = stream
            .message()
            .await?
            .ok_or_else(|| tonic::Status::internal("missing object metadata chunk"))?;
        let head = match first.payload {
            Some(coldstore_proto::scheduler::get_object_response::Payload::Meta(meta)) => {
                HeadObjectResponse {
                    content_length: meta.content_length,
                    content_type: meta.content_type,
                    etag: meta.etag,
                    storage_class: meta.storage_class,
                    restore_info: meta.restore_info,
                    last_modified: meta.last_modified,
                }
            }
            _ => {
                return Err(tonic::Status::internal(
                    "first object chunk was not metadata",
                ))
            }
        };

        let mut body = Vec::new();
        while let Some(chunk) = stream.message().await? {
            if let Some(coldstore_proto::scheduler::get_object_response::Payload::Data(bytes)) =
                chunk.payload
            {
                body.extend_from_slice(&bytes);
            }
        }

        Ok(DownloadedObject { head, body })
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(), tonic::Status> {
        let mut client = self.connect().await?;
        client
            .delete_object(DeleteObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            })
            .await
            .map(|_| ())
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<HeadObjectResponse, tonic::Status> {
        let mut client = self.connect().await?;
        client
            .head_object(HeadObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
            })
            .await
            .map(|r| r.into_inner())
    }

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: u32,
        tier: common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, tonic::Status> {
        let mut client = self.connect().await?;
        client
            .restore_object(RestoreObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: None,
                days,
                tier: tier as i32,
            })
            .await
            .map(|r| r.into_inner())
    }
}

pub struct GatewayState {
    pub backend: Arc<dyn GatewayBackend>,
}

pub async fn run(config: GatewayConfig) -> Result<()> {
    if config.scheduler_addrs.is_empty() {
        anyhow::bail!("gateway requires at least one scheduler address");
    }

    let scheduler_addrs = config
        .scheduler_addrs
        .into_iter()
        .map(|addr| normalize_scheduler_addr(&addr))
        .collect::<Vec<_>>();

    info!(
        scheduler_count = scheduler_addrs.len(),
        first_scheduler_addr = &scheduler_addrs[0],
        "Gateway routing to scheduler fleet"
    );

    let state = Arc::new(GatewayState {
        backend: Arc::new(GrpcGatewayBackend::new(scheduler_addrs)),
    });

    let app = router::router(state);

    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    info!("S3 Gateway 启动在 {}", config.listen);
    axum::serve(listener, app).await?;

    Ok(())
}
