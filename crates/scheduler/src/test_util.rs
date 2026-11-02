//! Shared test fixtures for the scheduler crate.
//!
//! AR-00 moved these out of `service.rs`'s test module so every module's tests can
//! reach them without duplication. `#[cfg(test)]`, so nothing here ships.
use crate::clients::cache::{Phase1ArchiveCache, Phase1StagedObject};
use crate::clients::tape::{TapeArchiveWrite, TapeArchiveWriter};
use crate::responses::{build_list_objects_page, ListObjectsPage};
use crate::service::{Phase1SchedulerBackend, SchedulerServiceImpl};
use crate::util::{days_from_now, sha256_hex};
use crate::SchedulerState;
use coldstore_cache::service::CacheServiceImpl;
use coldstore_common::config::{
    CacheBackendConfig, CacheConfig, MetadataConfig, SchedulerConfig, TapeConfig,
};
use coldstore_metadata::service::MetadataServiceImpl;
use coldstore_proto::cache::cache_service_client::CacheServiceClient;
use coldstore_proto::cache::cache_service_server::CacheServiceServer;
use coldstore_proto::cache::get_staging_response::Payload as GetStagingPayload;
use coldstore_proto::cache::put_restored_request::Payload as PutRestoredPayload;
use coldstore_proto::cache::{
    GetStagingRequest, PutRestoredMeta, PutRestoredRequest, StagingKeyEntry, StagingObjectMeta,
};
use coldstore_proto::common;
use coldstore_proto::metadata::metadata_service_server::MetadataServiceServer;
use coldstore_proto::scheduler::{PutObjectResponse, RestoreObjectResponse};
use coldstore_proto::tape::read_bundle_request::Location as TapeReadLocation;
use coldstore_proto::tape::read_bundle_response::Payload as TapeReadPayload;
use coldstore_proto::tape::tape_service_client::TapeServiceClient;
use coldstore_proto::tape::tape_service_server::{
    TapeService as TapeWorkerService, TapeServiceServer,
};
use coldstore_proto::tape::write_bundle_request::Payload as TapeWritePayload;
use coldstore_proto::tape::{LoadTapeRequest, ReadBundleRequest as TapeReadBundleRequest};
use coldstore_proto::tape::{WriteBundleMeta, WriteBundleRequest};
use coldstore_tape::service::{SimulatorTapeBackend, TapeServiceImpl as TapeWorkerServiceImpl};
use prost_types::Timestamp;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::{oneshot, Mutex, Semaphore};
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Server};
use tonic::{Request, Status};
#[derive(Default)]
pub(crate) struct InMemoryBackend {
    buckets: RwLock<Vec<common::BucketInfo>>,
    pub(crate) objects: RwLock<HashMap<String, (common::ObjectMetadata, Vec<u8>)>>,
}

impl InMemoryBackend {
    pub(crate) fn with_fixture() -> Self {
        let bucket = common::BucketInfo {
            name: "docs".into(),
            created_at: Some(Timestamp {
                seconds: 5,
                nanos: 0,
            }),
            owner: None,
            versioning_enabled: false,
            object_count: 1,
            total_size: 42,
        };
        let object = common::ObjectMetadata {
            bucket: "docs".into(),
            key: "readme.txt".into(),
            version_id: None,
            size: 42,
            checksum: "sum".into(),
            content_type: Some("text/plain".into()),
            etag: Some("etag-1".into()),
            storage_class: common::StorageClass::Cold as i32,
            archive_id: Some("archive-1".into()),
            tape_id: Some("tape-1".into()),
            tape_set: vec!["tape-1".into()],
            tape_block_offset: Some(1),
            restore_status: Some(common::RestoreStatus::RestoreCompleted as i32),
            restore_expire_at: Some(Timestamp {
                seconds: 123,
                nanos: 0,
            }),
            created_at: Some(Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            updated_at: Some(Timestamp {
                seconds: 2,
                nanos: 0,
            }),
        };
        let mut objects = HashMap::new();
        objects.insert("docs/readme.txt".into(), (object, b"hello world".to_vec()));
        Self {
            buckets: RwLock::new(vec![bucket]),
            objects: RwLock::new(objects),
        }
    }
}

#[tonic::async_trait]
impl Phase1SchedulerBackend for InMemoryBackend {
    async fn list_buckets(&self) -> std::result::Result<Vec<common::BucketInfo>, Status> {
        Ok(self.buckets.read().unwrap().clone())
    }
    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        let mut buckets = self.buckets.write().unwrap();
        if buckets.iter().any(|b| b.name == bucket) {
            return Err(Status::already_exists("bucket exists"));
        }
        buckets.push(common::BucketInfo {
            name: bucket.into(),
            created_at: None,
            owner: None,
            versioning_enabled: false,
            object_count: 0,
            total_size: 0,
        });
        Ok(())
    }
    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        let mut buckets = self.buckets.write().unwrap();
        let before = buckets.len();
        buckets.retain(|b| b.name != bucket);
        if buckets.len() == before {
            Err(Status::not_found("bucket missing"))
        } else {
            Ok(())
        }
    }
    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        if self
            .buckets
            .read()
            .unwrap()
            .iter()
            .any(|b| b.name == bucket)
        {
            Ok(())
        } else {
            Err(Status::not_found("bucket missing"))
        }
    }
    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<common::ObjectMetadata, Status> {
        self.objects
            .read()
            .unwrap()
            .get(&format!("{bucket}/{key}"))
            .map(|(o, _)| o.clone())
            .ok_or_else(|| Status::not_found("object missing"))
    }
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(common::ObjectMetadata, Vec<u8>), Status> {
        self.objects
            .read()
            .unwrap()
            .get(&format!("{bucket}/{key}"))
            .cloned()
            .ok_or_else(|| Status::not_found("object missing"))
    }
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, Status> {
        let object = common::ObjectMetadata {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            size: body.len() as u64,
            checksum: "sum".into(),
            content_type,
            etag: Some("etag-put".into()),
            storage_class: common::StorageClass::ColdPending as i32,
            archive_id: None,
            tape_id: None,
            tape_set: vec![],
            tape_block_offset: None,
            restore_status: None,
            restore_expire_at: None,
            created_at: Some(Timestamp {
                seconds: 10,
                nanos: 0,
            }),
            updated_at: Some(Timestamp {
                seconds: 10,
                nanos: 0,
            }),
        };
        self.objects
            .write()
            .unwrap()
            .insert(format!("{bucket}/{key}"), (object, body));
        Ok(PutObjectResponse {
            etag: "etag-put".into(),
            version_id: "v1".into(),
        })
    }
    async fn delete_object(&self, bucket: &str, key: &str) -> std::result::Result<(), Status> {
        if self
            .objects
            .write()
            .unwrap()
            .remove(&format!("{bucket}/{key}"))
            .is_some()
        {
            Ok(())
        } else {
            Err(Status::not_found("object missing"))
        }
    }
    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        _days: u32,
        _tier: common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, Status> {
        let mut objects = self.objects.write().unwrap();
        let (object, _) = objects
            .get_mut(&format!("{bucket}/{key}"))
            .ok_or_else(|| Status::not_found("object missing"))?;
        let status_code =
            if object.restore_status == Some(common::RestoreStatus::RestoreCompleted as i32) {
                200
            } else {
                202
            };
        object.restore_status = Some(common::RestoreStatus::RestoreCompleted as i32);
        object.restore_expire_at = Some(Timestamp {
            seconds: 999,
            nanos: 0,
        });
        Ok(RestoreObjectResponse { status_code })
    }
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
    ) -> std::result::Result<ListObjectsPage, Status> {
        let objects: Vec<_> = self
            .objects
            .read()
            .unwrap()
            .values()
            .map(|(o, _)| o.clone())
            .collect();
        Ok(build_list_objects_page(
            bucket, objects, prefix, marker, delimiter, max_keys,
        ))
    }
}

pub(crate) fn service() -> SchedulerServiceImpl {
    let state = test_scheduler_state(
        coldstore_proto::metadata::metadata_service_client::MetadataServiceClient::new(
            tonic::transport::Channel::from_static("http://127.0.0.1:1").connect_lazy(),
        ),
        None,
        None,
        SchedulerConfig::default(),
    );
    SchedulerServiceImpl::new_with_backend(state, Arc::new(InMemoryBackend::with_fixture()))
}

pub(crate) fn test_scheduler_state(
    metadata: coldstore_proto::metadata::metadata_service_client::MetadataServiceClient<
        tonic::transport::Channel,
    >,
    cache: Option<
        coldstore_proto::cache::cache_service_client::CacheServiceClient<tonic::transport::Channel>,
    >,
    tape: Option<
        coldstore_proto::tape::tape_service_client::TapeServiceClient<tonic::transport::Channel>,
    >,
    config: SchedulerConfig,
) -> Arc<SchedulerState> {
    Arc::new(SchedulerState {
        metadata,
        cache,
        tape,
        config: config.clone(),
        active_archive_keys: Arc::new(Mutex::new(HashSet::new())),
        active_recall_tasks: Arc::new(Mutex::new(HashSet::new())),
        tape_locks: Arc::new(Mutex::new(HashMap::new())),
        archive_slots: Arc::new(Semaphore::new(config.archive.max_workers.max(1))),
        recall_slots: Arc::new(Semaphore::new(config.recall.max_workers.max(1))),
        recall_task_slots: Arc::new(Semaphore::new(config.recall.max_concurrent_restores.max(1))),
    })
}

pub(crate) async fn metadata_backed_service() -> (
    SchedulerServiceImpl,
    Arc<SchedulerState>,
    oneshot::Sender<()>,
) {
    let metadata = MetadataServiceImpl::new(&MetadataConfig::default())
        .await
        .expect("metadata service init");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(MetadataServiceServer::new(metadata))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("metadata server should run");
    });

    let mut metadata_client = None;
    for _ in 0..20 {
        match coldstore_proto::metadata::metadata_service_client::MetadataServiceClient::connect(
            format!("http://{addr}"),
        )
        .await
        {
            Ok(client) => {
                metadata_client = Some(client);
                break;
            }
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }
    let metadata = metadata_client.expect("connect metadata client");
    let state = test_scheduler_state(metadata, None, None, SchedulerConfig::default());
    (SchedulerServiceImpl::new(state.clone()), state, shutdown_tx)
}

fn test_cache_config() -> CacheConfig {
    // A uuid, not a timestamp: two tests running in parallel can read the clock at the same
    // instant, and then both services share one meta/ directory.
    let unique = uuid::Uuid::new_v4();
    CacheConfig {
        backend: CacheBackendConfig::Hdd {
            path: format!("/tmp/coldstore-scheduler-cache-test-{unique}"),
            max_size_gb: 1,
        },
        ..CacheConfig::default()
    }
}

pub(crate) async fn cache_backed_service() -> (CacheServiceClient<Channel>, oneshot::Sender<()>) {
    let cache = CacheServiceImpl::new(&test_cache_config())
        .await
        .expect("cache service init");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(CacheServiceServer::new(cache))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("cache server should run");
    });

    let mut cache_client = None;
    for _ in 0..20 {
        match CacheServiceClient::connect(format!("http://{addr}")).await {
            Ok(client) => {
                cache_client = Some(client);
                break;
            }
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }
    (cache_client.expect("connect cache client"), shutdown_tx)
}

pub(crate) async fn seed_object_metadata(
    metadata: &mut coldstore_proto::metadata::metadata_service_client::MetadataServiceClient<
        Channel,
    >,
    bucket: &str,
    key: &str,
    body: &[u8],
    storage_class: common::StorageClass,
    restore_status: Option<common::RestoreStatus>,
) {
    let now = Timestamp {
        seconds: 10,
        nanos: 0,
    };
    metadata
        .put_object(Request::new(common::ObjectMetadata {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            size: body.len() as u64,
            checksum: sha256_hex(body),
            content_type: Some("text/plain".into()),
            etag: Some(sha256_hex(body)),
            storage_class: storage_class as i32,
            archive_id: None,
            tape_id: None,
            tape_set: vec![],
            tape_block_offset: None,
            restore_status: restore_status.map(|status| status as i32),
            restore_expire_at: restore_status.map(|_| days_from_now(1)),
            created_at: Some(now),
            updated_at: Some(now),
        }))
        .await
        .expect("seed object metadata");
}

pub(crate) async fn read_staging_object(
    cache: &mut CacheServiceClient<Channel>,
    bucket: &str,
    key: &str,
) -> (StagingObjectMeta, Vec<u8>) {
    let mut stream = cache
        .get_staging(Request::new(GetStagingRequest {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
        }))
        .await
        .expect("get staging")
        .into_inner();
    let mut meta = None;
    let mut body = Vec::new();
    while let Some(message) = stream.next().await {
        match message.expect("staging message").payload.expect("payload") {
            GetStagingPayload::Meta(next_meta) => {
                assert!(meta.replace(next_meta).is_none(), "duplicate staging meta");
            }
            GetStagingPayload::Data(bytes) => body.extend_from_slice(&bytes),
        }
    }
    (meta.expect("staging meta"), body)
}

pub(crate) async fn put_restored_cache_object(
    cache: &mut CacheServiceClient<Channel>,
    bucket: &str,
    key: &str,
    body: Vec<u8>,
) {
    cache
        .put_restored(Request::new(tokio_stream::iter(vec![
            PutRestoredRequest {
                payload: Some(PutRestoredPayload::Meta(PutRestoredMeta {
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id: None,
                    size: body.len() as u64,
                    checksum: Some(sha256_hex(&body)),
                    content_type: Some("text/plain".into()),
                    etag: Some(sha256_hex(&body)),
                    expire_at: Some(days_from_now(1)),
                })),
            },
            PutRestoredRequest {
                payload: Some(PutRestoredPayload::Data(body)),
            },
        ])))
        .await
        .expect("put restored cache object");
}

pub(crate) async fn tape_backed_service() -> (TapeServiceClient<Channel>, oneshot::Sender<()>) {
    let backend = SimulatorTapeBackend::new(2, 1);
    backend.insert_tape("slot-1", "TAPE-GRPC").unwrap();
    let tape = TapeWorkerServiceImpl::new_with_backend(TapeConfig::default(), backend);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
    let addr = listener.local_addr().expect("listener addr");
    drop(listener);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(TapeServiceServer::new(tape))
            .serve_with_shutdown(addr, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("tape server should run");
    });

    let mut tape_client = None;
    for _ in 0..20 {
        match TapeServiceClient::connect(format!("http://{addr}")).await {
            Ok(client) => {
                tape_client = Some(client);
                break;
            }
            Err(_) => sleep(Duration::from_millis(25)).await,
        }
    }
    let mut tape_client = tape_client.expect("connect tape client");
    tape_client
        .load_tape(Request::new(LoadTapeRequest {
            tape_id: "TAPE-GRPC".into(),
            drive_id: "drive-0".into(),
            slot_id: Some("slot-1".into()),
        }))
        .await
        .expect("load tape into in-process simulator");
    (tape_client, shutdown_tx)
}

pub(crate) async fn read_tape_client_filemark(
    client: &mut TapeServiceClient<Channel>,
    filemark: u32,
    length: u64,
) -> Vec<u8> {
    let mut stream = client
        .read_bundle(Request::new(TapeReadBundleRequest {
            drive_id: "drive-0".into(),
            location: Some(TapeReadLocation::Filemark(filemark)),
            length,
        }))
        .await
        .expect("read bundle through tape client")
        .into_inner();
    while let Some(message) = stream.next().await {
        match message.unwrap().payload.unwrap() {
            TapeReadPayload::Meta(_) => {}
            TapeReadPayload::Data(data) => return data,
        }
    }
    Vec::new()
}

pub(crate) struct TestArchiveCache {
    staged: RwLock<HashMap<String, Phase1StagedObject>>,
    deleted: RwLock<Vec<String>>,
}

impl TestArchiveCache {
    pub(crate) fn with_object(bucket: &str, key: &str, data: Vec<u8>) -> Self {
        let meta = StagingObjectMeta {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            size: data.len() as u64,
            checksum: Some(sha256_hex(&data)),
            content_type: Some("text/plain".into()),
            etag: Some("etag-staged".into()),
            staged_at: Some(Timestamp {
                seconds: 30,
                nanos: 0,
            }),
        };
        let mut staged = HashMap::new();
        staged.insert(format!("{bucket}/{key}"), Phase1StagedObject { meta, data });
        Self {
            staged: RwLock::new(staged),
            deleted: RwLock::new(Vec::new()),
        }
    }

    pub(crate) fn deleted_keys(&self) -> Vec<String> {
        self.deleted.read().unwrap().clone()
    }
}

#[tonic::async_trait]
impl Phase1ArchiveCache for TestArchiveCache {
    async fn list_staging_keys(
        &self,
        limit: u32,
    ) -> std::result::Result<Vec<StagingKeyEntry>, Status> {
        let mut entries: Vec<_> = self
            .staged
            .read()
            .unwrap()
            .values()
            .map(|object| StagingKeyEntry {
                bucket: object.meta.bucket.clone(),
                key: object.meta.key.clone(),
                version_id: object.meta.version_id.clone(),
                size: object.meta.size,
                staged_at: object.meta.staged_at,
            })
            .collect();
        entries.sort_by(|a, b| (&a.bucket, &a.key).cmp(&(&b.bucket, &b.key)));
        entries.truncate(limit as usize);
        Ok(entries)
    }

    async fn get_staging(
        &self,
        bucket: &str,
        key: &str,
        _version_id: Option<&str>,
    ) -> std::result::Result<Phase1StagedObject, Status> {
        self.staged
            .read()
            .unwrap()
            .get(&format!("{bucket}/{key}"))
            .cloned()
            .ok_or_else(|| Status::not_found("staging object missing"))
    }

    async fn delete_staging(
        &self,
        bucket: &str,
        key: &str,
        _version_id: Option<&str>,
    ) -> std::result::Result<(), Status> {
        self.staged
            .write()
            .unwrap()
            .remove(&format!("{bucket}/{key}"));
        self.deleted
            .write()
            .unwrap()
            .push(format!("{bucket}/{key}"));
        Ok(())
    }
}

pub(crate) struct DirectTapeWriter {
    service: TapeWorkerServiceImpl,
}

impl DirectTapeWriter {
    pub(crate) async fn loaded() -> Self {
        let backend = SimulatorTapeBackend::new(2, 1);
        backend.insert_tape("slot-1", "TAPE-PHASE1").unwrap();
        let service = TapeWorkerServiceImpl::new_with_backend(TapeConfig::default(), backend);
        service
            .load_tape(Request::new(LoadTapeRequest {
                tape_id: "TAPE-PHASE1".into(),
                drive_id: "drive-0".into(),
                slot_id: Some("slot-1".into()),
            }))
            .await
            .unwrap();
        Self { service }
    }

    pub(crate) async fn read_filemark(&self, filemark: u32, length: u64) -> Vec<u8> {
        let mut stream = self
            .service
            .read_bundle(Request::new(TapeReadBundleRequest {
                drive_id: "drive-0".into(),
                location: Some(TapeReadLocation::Filemark(filemark)),
                length,
            }))
            .await
            .unwrap()
            .into_inner();
        while let Some(message) = stream.next().await {
            match message.unwrap().payload.unwrap() {
                TapeReadPayload::Meta(_) => {}
                TapeReadPayload::Data(data) => return data,
            }
        }
        Vec::new()
    }
}

#[tonic::async_trait]
impl TapeArchiveWriter for DirectTapeWriter {
    async fn write_bundle(
        &self,
        bundle_id: &str,
        object_count: u32,
        data: Vec<u8>,
    ) -> std::result::Result<TapeArchiveWrite, Status> {
        let response = self
            .service
            .write_bundle_from_messages(tokio_stream::iter(vec![
                Ok(WriteBundleRequest {
                    payload: Some(TapeWritePayload::Meta(WriteBundleMeta {
                        drive_id: "drive-0".into(),
                        bundle_id: bundle_id.into(),
                        total_size: data.len() as u64,
                        object_count,
                        block_size: 262_144,
                    })),
                }),
                Ok(WriteBundleRequest {
                    payload: Some(TapeWritePayload::Data(data)),
                }),
            ]))
            .await?;
        Ok(TapeArchiveWrite {
            tape_id: "TAPE-PHASE1".into(),
            tape_set: vec!["TAPE-PHASE1".into()],
            filemark_start: response.filemark_start,
            filemark_end: response.filemark_end,
            bytes_written: response.bytes_written,
        })
    }
}
