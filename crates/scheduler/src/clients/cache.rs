use crate::util::sha256_hex;
use coldstore_proto::cache::cache_service_client::CacheServiceClient;
use coldstore_proto::cache::get_staging_response::Payload as GetStagingPayload;
use coldstore_proto::cache::put_restored_request::Payload as PutRestoredPayload;
use coldstore_proto::cache::{
    DeleteStagingRequest, GetStagingRequest, ListStagingKeysRequest, PutRestoredMeta,
    PutRestoredRequest, StagingKeyEntry, StagingObjectMeta,
};
use coldstore_proto::common;
use prost_types::Timestamp;
use tonic::{Request, Status};

#[tonic::async_trait]
pub trait Phase1ArchiveCache: Send + Sync {
    async fn list_staging_keys(
        &self,
        limit: u32,
    ) -> std::result::Result<Vec<StagingKeyEntry>, Status>;

    async fn get_staging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> std::result::Result<Phase1StagedObject, Status>;

    async fn delete_staging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> std::result::Result<(), Status>;
}

#[derive(Clone)]
pub struct CacheArchiveClient {
    client: CacheServiceClient<tonic::transport::Channel>,
}

impl CacheArchiveClient {
    pub fn new(client: CacheServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl Phase1ArchiveCache for CacheArchiveClient {
    async fn list_staging_keys(
        &self,
        limit: u32,
    ) -> std::result::Result<Vec<StagingKeyEntry>, Status> {
        let mut client = self.client.clone();
        Ok(client
            .list_staging_keys(Request::new(ListStagingKeysRequest { limit, after: None }))
            .await?
            .into_inner()
            .entries)
    }

    async fn get_staging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> std::result::Result<Phase1StagedObject, Status> {
        let mut client = self.client.clone();
        let mut stream = client
            .get_staging(Request::new(GetStagingRequest {
                bucket: bucket.into(),
                key: key.into(),
                version_id: version_id.map(str::to_owned),
            }))
            .await?
            .into_inner();

        let mut meta = None;
        let mut data = Vec::new();
        while let Some(message) = stream.message().await? {
            match message.payload {
                Some(GetStagingPayload::Meta(next_meta)) => {
                    if meta.replace(next_meta).is_some() {
                        return Err(Status::invalid_argument(
                            "cache get_staging returned duplicate metadata",
                        ));
                    }
                }
                Some(GetStagingPayload::Data(chunk)) => data.extend_from_slice(&chunk),
                None => return Err(Status::internal("cache get_staging returned empty chunk")),
            }
        }

        let meta = meta
            .ok_or_else(|| Status::internal("cache get_staging stream ended without metadata"))?;
        Ok(Phase1StagedObject { meta, data })
    }

    async fn delete_staging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> std::result::Result<(), Status> {
        let mut client = self.client.clone();
        client
            .delete_staging(Request::new(DeleteStagingRequest {
                bucket: bucket.into(),
                key: key.into(),
                version_id: version_id.map(str::to_owned),
            }))
            .await?;
        Ok(())
    }
}

#[tonic::async_trait]
pub trait Phase1RestoreCache: Send + Sync {
    async fn put_restored(
        &self,
        object: &common::ObjectMetadata,
        data: Vec<u8>,
        expire_at: Timestamp,
    ) -> std::result::Result<(), Status>;
}

#[derive(Clone)]
pub struct CacheRestoreClient {
    client: CacheServiceClient<tonic::transport::Channel>,
}

impl CacheRestoreClient {
    pub fn new(client: CacheServiceClient<tonic::transport::Channel>) -> Self {
        Self { client }
    }
}

#[tonic::async_trait]
impl Phase1RestoreCache for CacheRestoreClient {
    async fn put_restored(
        &self,
        object: &common::ObjectMetadata,
        data: Vec<u8>,
        expire_at: Timestamp,
    ) -> std::result::Result<(), Status> {
        let mut client = self.client.clone();
        client
            .put_restored(Request::new(tokio_stream::iter(vec![
                PutRestoredRequest {
                    payload: Some(PutRestoredPayload::Meta(PutRestoredMeta {
                        bucket: object.bucket.clone(),
                        key: object.key.clone(),
                        version_id: object.version_id.clone(),
                        size: data.len() as u64,
                        checksum: Some(sha256_hex(&data)),
                        content_type: object.content_type.clone(),
                        etag: object.etag.clone(),
                        expire_at: Some(expire_at),
                    })),
                },
                PutRestoredRequest {
                    payload: Some(PutRestoredPayload::Data(data)),
                },
            ])))
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Phase1StagedObject {
    pub meta: StagingObjectMeta,
    pub data: Vec<u8>,
}
