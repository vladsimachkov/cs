pub use crate::archive::archive_staging_once;
use crate::archive::run_archive_agent;
pub use crate::recall::recall_pending_once;
use crate::recall::run_recall_agent;

// AR-00 moved these out of this module; the re-exports keep the pre-split public paths
// (`coldstore_scheduler::service::Phase1ArchiveCache`, `...::ListObjectsPage`, ...) working, so the
// split is move-only for consumers as well as for behavior.
pub use crate::clients::cache::{
    CacheArchiveClient, CacheRestoreClient, Phase1ArchiveCache, Phase1RestoreCache,
    Phase1StagedObject,
};
pub use crate::clients::tape::{
    TapeArchiveClient, TapeArchiveWrite, TapeArchiveWriter, TapeRecallClient, TapeRecallReader,
};
pub use crate::responses::ListObjectsPage;
use crate::responses::*;
#[cfg(test)]
use crate::util::sha256_hex;
use crate::SchedulerState;
use coldstore_proto::cache::cache_service_client::CacheServiceClient;
use coldstore_proto::common;
use coldstore_proto::scheduler::scheduler_service_server::SchedulerService;
use coldstore_proto::scheduler::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::info;

#[tonic::async_trait]
pub trait Phase1SchedulerBackend: Send + Sync + 'static {
    async fn list_buckets(&self) -> std::result::Result<Vec<common::BucketInfo>, Status>;
    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), Status>;
    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), Status>;
    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), Status>;
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<common::ObjectMetadata, Status>;
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(common::ObjectMetadata, Vec<u8>), Status>;
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, Status>;
    async fn delete_object(&self, bucket: &str, key: &str) -> std::result::Result<(), Status>;
    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: u32,
        tier: common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, Status>;
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
    ) -> std::result::Result<ListObjectsPage, Status>;
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct ArchiveBatchResult {
    pub archived_objects: u32,
    pub bytes_written: u64,
    pub bundle_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct RecallBatchResult {
    pub restored_objects: u32,
    pub bytes_read: u64,
    pub task_ids: Vec<String>,
}

pub struct MetadataBackedSchedulerBackend {
    pub(crate) metadata: coldstore_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
    pub(crate) cache: Option<CacheServiceClient<tonic::transport::Channel>>,
}

impl MetadataBackedSchedulerBackend {
    pub fn new(
        metadata: coldstore_proto::metadata::metadata_service_client::MetadataServiceClient<
            tonic::transport::Channel,
        >,
    ) -> Self {
        Self {
            metadata,
            cache: None,
        }
    }

    pub fn new_with_cache(
        metadata: coldstore_proto::metadata::metadata_service_client::MetadataServiceClient<
            tonic::transport::Channel,
        >,
        cache: Option<CacheServiceClient<tonic::transport::Channel>>,
    ) -> Self {
        Self { metadata, cache }
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn cache_client(
        &self,
    ) -> std::result::Result<CacheServiceClient<tonic::transport::Channel>, Status> {
        self.cache
            .clone()
            .ok_or_else(|| Status::failed_precondition("scheduler cache client is not configured"))
    }
}

pub struct SchedulerServiceImpl {
    _state: Arc<SchedulerState>,
    backend: Arc<dyn Phase1SchedulerBackend>,
}

impl SchedulerServiceImpl {
    pub fn new(state: Arc<SchedulerState>) -> Self {
        let backend = Arc::new(MetadataBackedSchedulerBackend::new_with_cache(
            state.metadata.clone(),
            state.cache.clone(),
        ));
        Self {
            _state: state,
            backend,
        }
    }

    pub fn new_with_backend(
        state: Arc<SchedulerState>,
        backend: Arc<dyn Phase1SchedulerBackend>,
    ) -> Self {
        Self {
            _state: state,
            backend,
        }
    }
}

#[cfg(test)]
fn phase1_unimplemented(op: &str) -> Status {
    Status::unimplemented(format!(
        "{op} is not implemented in phase-1 safe mode; use unit-tested metadata/cache services only"
    ))
}

pub fn spawn_background_loops(state: Arc<SchedulerState>) {
    if state.config.archive.enabled {
        let workers = state.config.archive.max_workers.max(1);
        for worker in 0..workers {
            tokio::spawn(run_archive_agent(state.clone(), worker as u64));
        }
    } else {
        info!("scheduler archive background loop disabled");
    }

    if state.config.recall.enabled {
        let workers = state.config.recall.max_workers.max(1);
        for worker in 0..workers {
            tokio::spawn(run_recall_agent(state.clone(), worker as u64));
        }
    } else {
        info!("scheduler recall background loop disabled");
    }
}

#[tonic::async_trait]
impl SchedulerService for SchedulerServiceImpl {
    async fn put_object(
        &self,
        request: Request<Streaming<PutObjectRequest>>,
    ) -> std::result::Result<Response<PutObjectResponse>, Status> {
        let mut stream = request.into_inner();
        let mut meta: Option<PutObjectMeta> = None;
        let mut body = Vec::new();
        while let Some(chunk) = stream.message().await? {
            match chunk.payload {
                Some(put_object_request::Payload::Meta(m)) => meta = Some(m),
                Some(put_object_request::Payload::Data(bytes)) => body.extend_from_slice(&bytes),
                None => return Err(Status::invalid_argument("empty put_object chunk")),
            }
        }
        let meta = meta.ok_or_else(|| Status::invalid_argument("missing put_object metadata"))?;
        if meta.content_length != body.len() as u64 {
            return Err(Status::invalid_argument(
                "content_length does not match body size",
            ));
        }
        let response = self
            .backend
            .put_object(&meta.bucket, &meta.key, body, meta.content_type)
            .await?;
        Ok(Response::new(response))
    }

    type GetObjectStream = ReceiverStream<Result<GetObjectResponse, Status>>;

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> std::result::Result<Response<Self::GetObjectStream>, Status> {
        let request = request.into_inner();
        let (object, data) = self
            .backend
            .get_object(&request.bucket, &request.key)
            .await?;
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(GetObjectResponse {
                    payload: Some(get_object_response::Payload::Meta(build_get_object_meta(
                        &object,
                    ))),
                }))
                .await;
            let _ = tx
                .send(Ok(GetObjectResponse {
                    payload: Some(get_object_response::Payload::Data(data)),
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> std::result::Result<Response<HeadObjectResponse>, Status> {
        let request = request.into_inner();
        let object = self
            .backend
            .head_object(&request.bucket, &request.key)
            .await?;
        Ok(Response::new(build_head_object_response(&object)))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        let request = request.into_inner();
        self.backend
            .delete_object(&request.bucket, &request.key)
            .await?;
        Ok(Response::new(()))
    }

    async fn restore_object(
        &self,
        request: Request<RestoreObjectRequest>,
    ) -> std::result::Result<Response<RestoreObjectResponse>, Status> {
        let request = request.into_inner();
        let tier = common::RestoreTier::try_from(request.tier)
            .map_err(|_| Status::invalid_argument("invalid restore tier"))?;
        let response = self
            .backend
            .restore_object(&request.bucket, &request.key, request.days, tier)
            .await?;
        Ok(Response::new(response))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> std::result::Result<Response<ListObjectsResponse>, Status> {
        let request = request.into_inner();
        let page = self
            .backend
            .list_objects(
                &request.bucket,
                request.prefix.as_deref(),
                request.marker.as_deref(),
                request.delimiter.as_deref(),
                request.max_keys,
            )
            .await?;
        Ok(Response::new(ListObjectsResponse {
            bucket: request.bucket,
            prefix: request.prefix,
            marker: request.marker,
            next_marker: page.next_marker,
            max_keys: request.max_keys,
            is_truncated: page.is_truncated,
            contents: page
                .objects
                .into_iter()
                .map(|object| build_object_entry(&object))
                .collect(),
            common_prefixes: page
                .common_prefixes
                .into_iter()
                .map(|prefix| CommonPrefix { prefix })
                .collect(),
        }))
    }

    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        self.backend
            .create_bucket(&request.into_inner().bucket)
            .await?;
        Ok(Response::new(()))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        self.backend
            .delete_bucket(&request.into_inner().bucket)
            .await?;
        Ok(Response::new(()))
    }

    async fn head_bucket(
        &self,
        request: Request<HeadBucketRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        self.backend
            .head_bucket(&request.into_inner().bucket)
            .await?;
        Ok(Response::new(()))
    }

    async fn list_buckets(
        &self,
        _request: Request<()>,
    ) -> std::result::Result<Response<ListBucketsResponse>, Status> {
        let buckets = self.backend.list_buckets().await?;
        Ok(Response::new(ListBucketsResponse {
            buckets: buckets.iter().map(build_bucket_entry).collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leases::phase1_archive_item_key;
    use crate::test_util::*;
    use coldstore_common::config::SchedulerConfig;
    use coldstore_proto::cache::put_staging_request::Payload as PutStagingPayload;
    use coldstore_proto::cache::{GetStagingRequest, ListStagingKeysRequest};
    use coldstore_proto::cache::{PutStagingMeta, PutStagingRequest};
    use tokio_stream::StreamExt;

    #[test]
    fn helper_head_object_response_contains_restore_info() {
        let (object, _) = InMemoryBackend::with_fixture()
            .objects
            .read()
            .unwrap()
            .get("docs/readme.txt")
            .unwrap()
            .clone();
        let response = build_head_object_response(&object);
        assert_eq!(response.content_length, 42);
        assert_eq!(response.etag, "etag-1");
        assert_eq!(
            response.restore_info.as_deref(),
            Some("ongoing-request=\"false\", expiry-date=\"Thu, 01 Jan 1970 00:02:03 GMT\"")
        );
    }

    #[tokio::test]
    async fn get_object_stream_uses_backend() {
        let mut stream = service()
            .get_object(Request::new(GetObjectRequest {
                bucket: "docs".into(),
                key: "readme.txt".into(),
                version_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        let first = stream.next().await.unwrap().unwrap();
        match first.payload {
            Some(get_object_response::Payload::Meta(meta)) => assert_eq!(meta.etag, "etag-1"),
            _ => panic!("expected meta"),
        }
        let second = stream.next().await.unwrap().unwrap();
        match second.payload {
            Some(get_object_response::Payload::Data(bytes)) => assert_eq!(bytes, b"hello world"),
            _ => panic!("expected data"),
        }
    }

    #[tokio::test]
    async fn delete_object_uses_backend() {
        let svc = service();
        svc.delete_object(Request::new(DeleteObjectRequest {
            bucket: "docs".into(),
            key: "readme.txt".into(),
            version_id: None,
        }))
        .await
        .unwrap();
        assert!(svc
            .head_object(Request::new(HeadObjectRequest {
                bucket: "docs".into(),
                key: "readme.txt".into(),
                version_id: None
            }))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn restore_object_uses_backend() {
        let response = service()
            .restore_object(Request::new(RestoreObjectRequest {
                bucket: "docs".into(),
                key: "readme.txt".into(),
                version_id: None,
                days: 2,
                tier: common::RestoreTier::Standard as i32,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.status_code, 200);
    }

    #[tokio::test]
    async fn list_buckets_uses_backend() {
        let response = service()
            .list_buckets(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.buckets.len(), 1);
        assert_eq!(response.buckets[0].name, "docs");
    }

    #[tokio::test]
    async fn create_bucket_uses_backend() {
        let svc = service();
        svc.create_bucket(Request::new(CreateBucketRequest {
            bucket: "new-bucket".into(),
        }))
        .await
        .unwrap();
        let response = svc
            .list_buckets(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        assert!(response.buckets.iter().any(|b| b.name == "new-bucket"));
    }

    #[tokio::test]
    async fn delete_bucket_uses_backend() {
        let svc = service();
        svc.delete_bucket(Request::new(DeleteBucketRequest {
            bucket: "docs".into(),
        }))
        .await
        .unwrap();
        let response = svc
            .list_buckets(Request::new(()))
            .await
            .unwrap()
            .into_inner();
        assert!(!response.buckets.iter().any(|b| b.name == "docs"));
    }

    #[tokio::test]
    async fn head_bucket_uses_backend() {
        assert!(service()
            .head_bucket(Request::new(HeadBucketRequest {
                bucket: "docs".into()
            }))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn head_object_uses_backend() {
        let response = service()
            .head_object(Request::new(HeadObjectRequest {
                bucket: "docs".into(),
                key: "readme.txt".into(),
                version_id: None,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.content_length, 42);
        assert_eq!(response.etag, "etag-1");
    }

    #[tokio::test]
    async fn list_objects_uses_backend() {
        let response = service()
            .list_objects(Request::new(ListObjectsRequest {
                bucket: "docs".into(),
                prefix: Some("read".into()),
                marker: None,
                delimiter: None,
                max_keys: 100,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.contents.len(), 1);
        assert_eq!(response.contents[0].key, "readme.txt");
        assert_eq!(response.contents[0].storage_class, "COLD");
    }

    #[tokio::test]
    async fn archive_staging_batch_writes_tape_and_updates_metadata() {
        let (_svc, state, shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let mut metadata = state.metadata.clone();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "guide.txt",
            b"abcdef",
            common::StorageClass::ColdPending,
            None,
        )
        .await;

        let cache = TestArchiveCache::with_object("docs", "guide.txt", b"abcdef".to_vec());
        let tape = DirectTapeWriter::loaded().await;

        let archived = backend
            .archive_staging_batch(&state, &cache, &tape, 10)
            .await
            .expect("archive staging batch");
        assert_eq!(archived.archived_objects, 1);
        assert_eq!(archived.bytes_written, 6);
        assert_eq!(archived.bundle_ids, vec!["phase1-bundle:docs/guide.txt"]);

        let object = backend
            .head_object("docs", "guide.txt")
            .await
            .expect("head archived object");
        assert_eq!(object.storage_class, common::StorageClass::Cold as i32);
        assert_eq!(
            object.archive_id.as_deref(),
            Some("phase1-bundle:docs/guide.txt")
        );
        assert_eq!(object.tape_id.as_deref(), Some("TAPE-PHASE1"));
        assert_eq!(object.tape_set, vec!["TAPE-PHASE1"]);
        assert_eq!(object.tape_block_offset, Some(0));

        let mut metadata = state.metadata.clone();
        let bundle = metadata
            .get_archive_bundle(Request::new(
                coldstore_proto::metadata::GetArchiveBundleRequest {
                    id: "phase1-bundle:docs/guide.txt".into(),
                },
            ))
            .await
            .expect("archive bundle stored")
            .into_inner();
        assert_eq!(bundle.tape_id, "TAPE-PHASE1");
        assert_eq!(bundle.filemark_start, 0);
        assert_eq!(bundle.filemark_end, 1);
        assert_eq!(bundle.total_size, 6);
        assert_eq!(bundle.entries.len(), 1);
        assert_eq!(bundle.entries[0].bucket, "docs");
        assert_eq!(bundle.entries[0].key, "guide.txt");
        assert_eq!(bundle.entries[0].tape_block_offset, 0);

        assert_eq!(cache.deleted_keys(), vec!["docs/guide.txt"]);
        assert_eq!(tape.read_filemark(0, 6).await, b"abcdef");

        shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn archive_staging_batch_releases_lock_on_head_object_failure() {
        let (_svc, state, shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        let cache = TestArchiveCache::with_object("docs", "orphan.txt", b"orphan".to_vec());
        let tape = DirectTapeWriter::loaded().await;
        let item_key = phase1_archive_item_key("docs", "orphan.txt", None);

        let result = backend
            .archive_staging_batch(&state, &cache, &tape, 10)
            .await;
        assert!(result.is_err());

        let active = state.active_archive_keys.lock().await;
        assert!(!active.contains(&item_key));

        shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn archive_staging_batch_consumes_real_cache_service_staging() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let mut metadata = state.metadata.clone();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "from-cache.txt",
            b"cache-body",
            common::StorageClass::ColdPending,
            None,
        )
        .await;

        let (mut raw_cache_client, cache_shutdown_tx) = cache_backed_service().await;
        let body = b"cache-body".to_vec();
        raw_cache_client
            .put_staging(Request::new(tokio_stream::iter(vec![
                PutStagingRequest {
                    payload: Some(PutStagingPayload::Meta(PutStagingMeta {
                        bucket: "docs".into(),
                        key: "from-cache.txt".into(),
                        version_id: None,
                        size: body.len() as u64,
                        checksum: Some(sha256_hex(&body)),
                        content_type: Some("text/plain".into()),
                        etag: Some("etag-from-cache".into()),
                    })),
                },
                PutStagingRequest {
                    payload: Some(PutStagingPayload::Data(body.clone())),
                },
            ])))
            .await
            .expect("put staging through real cache service");

        let cache = CacheArchiveClient::new(raw_cache_client.clone());
        let tape = DirectTapeWriter::loaded().await;

        let archived = backend
            .archive_staging_batch(&state, &cache, &tape, 10)
            .await
            .expect("archive real cache staging batch");
        assert_eq!(archived.archived_objects, 1);
        assert_eq!(archived.bytes_written, body.len() as u64);
        assert_eq!(
            archived.bundle_ids,
            vec!["phase1-bundle:docs/from-cache.txt"]
        );

        let object = backend
            .head_object("docs", "from-cache.txt")
            .await
            .expect("head archived object");
        assert_eq!(object.storage_class, common::StorageClass::Cold as i32);
        assert_eq!(
            object.archive_id.as_deref(),
            Some("phase1-bundle:docs/from-cache.txt")
        );

        let listed = raw_cache_client
            .list_staging_keys(Request::new(ListStagingKeysRequest {
                limit: 10,
                after: None,
            }))
            .await
            .expect("list staging after archive")
            .into_inner();
        assert!(listed.entries.is_empty());

        let missing = raw_cache_client
            .get_staging(Request::new(GetStagingRequest {
                bucket: "docs".into(),
                key: "from-cache.txt".into(),
                version_id: None,
            }))
            .await
            .expect_err("staging should be deleted after archive");
        assert_eq!(missing.code(), tonic::Code::NotFound);
        assert_eq!(tape.read_filemark(0, body.len() as u64).await, body);

        metadata_shutdown_tx.send(()).ok();
        cache_shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn archive_staging_batch_uses_cache_and_tape_grpc_clients() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let body = b"grpc-tape-body".to_vec();
        let mut metadata = state.metadata.clone();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "grpc-tape.txt",
            &body,
            common::StorageClass::ColdPending,
            None,
        )
        .await;

        let (mut raw_cache_client, cache_shutdown_tx) = cache_backed_service().await;
        raw_cache_client
            .put_staging(Request::new(tokio_stream::iter(vec![
                PutStagingRequest {
                    payload: Some(PutStagingPayload::Meta(PutStagingMeta {
                        bucket: "docs".into(),
                        key: "grpc-tape.txt".into(),
                        version_id: None,
                        size: body.len() as u64,
                        checksum: Some(sha256_hex(&body)),
                        content_type: Some("text/plain".into()),
                        etag: Some("etag-grpc-tape".into()),
                    })),
                },
                PutStagingRequest {
                    payload: Some(PutStagingPayload::Data(body.clone())),
                },
            ])))
            .await
            .expect("put staging through real cache service");

        let (mut raw_tape_client, tape_shutdown_tx) = tape_backed_service().await;
        let cache = CacheArchiveClient::new(raw_cache_client.clone());
        let tape = TapeArchiveClient::new(
            raw_tape_client.clone(),
            "drive-0",
            "TAPE-GRPC",
            vec!["TAPE-GRPC".into()],
            262_144,
        );

        let archived = backend
            .archive_staging_batch(&state, &cache, &tape, 10)
            .await
            .expect("archive through cache and tape grpc clients");
        assert_eq!(archived.archived_objects, 1);
        assert_eq!(archived.bytes_written, body.len() as u64);
        assert_eq!(
            archived.bundle_ids,
            vec!["phase1-bundle:docs/grpc-tape.txt"]
        );

        let object = backend
            .head_object("docs", "grpc-tape.txt")
            .await
            .expect("head archived object");
        assert_eq!(object.storage_class, common::StorageClass::Cold as i32);
        assert_eq!(object.tape_id.as_deref(), Some("TAPE-GRPC"));
        assert_eq!(object.tape_set, vec!["TAPE-GRPC"]);
        assert_eq!(object.tape_block_offset, Some(0));

        let listed = raw_cache_client
            .list_staging_keys(Request::new(ListStagingKeysRequest {
                limit: 10,
                after: None,
            }))
            .await
            .expect("list staging after archive")
            .into_inner();
        assert!(listed.entries.is_empty());
        assert_eq!(
            read_tape_client_filemark(&mut raw_tape_client, 0, body.len() as u64).await,
            body
        );

        metadata_shutdown_tx.send(()).ok();
        cache_shutdown_tx.send(()).ok();
        tape_shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn scheduler_background_once_archives_and_restores_via_grpc_clients() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let (mut raw_cache_client, cache_shutdown_tx) = cache_backed_service().await;
        let (raw_tape_client, tape_shutdown_tx) = tape_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new_with_cache(
            state.metadata.clone(),
            Some(raw_cache_client.clone()),
        );

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let body = b"background-loop-body".to_vec();
        backend
            .put_object("docs", "loop.txt", body.clone(), Some("text/plain".into()))
            .await
            .expect("put object into staging");

        let mut config = SchedulerConfig::default();
        config.archive.batch_size = 10;
        config.archive.drive_id = "drive-0".into();
        config.archive.tape_id = "TAPE-GRPC".into();
        config.archive.tape_set = vec!["TAPE-GRPC".into()];
        config.recall.max_concurrent_restores = 2;
        config.recall.drive_id = "drive-0".into();
        let loop_state = test_scheduler_state(
            state.metadata.clone(),
            Some(raw_cache_client.clone()),
            Some(raw_tape_client.clone()),
            config,
        );

        let archived = archive_staging_once(loop_state.clone())
            .await
            .expect("archive once");
        assert_eq!(archived.archived_objects, 1);
        assert_eq!(archived.bytes_written, body.len() as u64);

        let archived_object = backend
            .head_object("docs", "loop.txt")
            .await
            .expect("head archived object");
        assert_eq!(
            archived_object.storage_class,
            common::StorageClass::Cold as i32
        );
        assert_eq!(archived_object.tape_id.as_deref(), Some("TAPE-GRPC"));

        let restore = backend
            .restore_object("docs", "loop.txt", 2, common::RestoreTier::Standard)
            .await
            .expect("queue restore");
        assert_eq!(restore.status_code, 202);

        let recalled = recall_pending_once(
            loop_state.clone(),
            loop_state.config.recall.max_concurrent_restores,
        )
        .await
        .expect("recall once");
        assert_eq!(recalled.restored_objects, 1);
        assert_eq!(recalled.bytes_read, body.len() as u64);

        let (object, restored) = backend
            .get_object("docs", "loop.txt")
            .await
            .expect("get restored object from cache");
        assert_eq!(
            object.restore_status,
            Some(common::RestoreStatus::RestoreCompleted as i32)
        );
        assert_eq!(restored, body);

        let listed = raw_cache_client
            .list_staging_keys(Request::new(ListStagingKeysRequest {
                limit: 10,
                after: None,
            }))
            .await
            .expect("list staging after archive")
            .into_inner();
        assert!(listed.entries.is_empty());

        metadata_shutdown_tx.send(()).ok();
        cache_shutdown_tx.send(()).ok();
        tape_shutdown_tx.send(()).ok();
    }

    #[test]
    fn phase1_unimplemented_message_is_stable() {
        let status = phase1_unimplemented("scheduler.list_buckets");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert!(status.message().contains("phase-1 safe mode"));
    }
}
