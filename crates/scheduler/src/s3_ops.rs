//! The S3-facing operations: bucket and object CRUD, restore requests, and the
//! restored-cache read that `GetObject` is served from.
//!
//! AR-00 moved these out of `service.rs` with no behavior change. `restore_object` is the only
//! entry point that may create a recall task, and it refuses anything that is not `COLD`. The
//! `x-amz-restore` value the gateway reads back is built in `responses::build_restore_info`.

use crate::archive::StagingPut;
use crate::recall::phase1_recall_task_id;
use crate::responses::*;
use crate::service::{MetadataBackedSchedulerBackend, Phase1SchedulerBackend};
use crate::util::*;
use coldstore_proto::cache::get_response::Payload as CacheGetPayload;
use coldstore_proto::cache::{DeleteRequest, DeleteStagingRequest, GetRequest as CacheGetRequest};
use coldstore_proto::common;
use coldstore_proto::scheduler::*;
use tonic::{Request, Status};

impl MetadataBackedSchedulerBackend {
    async fn read_restored_object(
        &self,
        object: &common::ObjectMetadata,
    ) -> std::result::Result<Vec<u8>, Status> {
        if object.storage_class != common::StorageClass::Cold as i32 {
            return Err(Status::failed_precondition(format!(
                "object {}/{} is not archived yet and cannot be read from restored cache",
                object.bucket, object.key
            )));
        }

        let restore_status = object
            .restore_status
            .and_then(|status| common::RestoreStatus::try_from(status).ok());
        if restore_status != Some(common::RestoreStatus::RestoreCompleted) {
            return Err(Status::failed_precondition(format!(
                "object {}/{} must complete restore before GET",
                object.bucket, object.key
            )));
        }

        let mut client = self.cache_client()?;
        let mut stream = client
            .get(Request::new(CacheGetRequest {
                bucket: object.bucket.clone(),
                key: object.key.clone(),
                version_id: object.version_id.clone(),
            }))
            .await
            .map_err(|status| {
                if status.code() == tonic::Code::NotFound {
                    Status::failed_precondition(format!(
                        "restored object {}/{} is not present in cache",
                        object.bucket, object.key
                    ))
                } else {
                    status
                }
            })?
            .into_inner();

        let mut expected_size = None;
        let mut data = Vec::new();
        while let Some(message) = stream.message().await? {
            match message.payload {
                Some(CacheGetPayload::Meta(meta)) => {
                    if expected_size.replace(meta.size).is_some() {
                        return Err(Status::invalid_argument(
                            "cache get returned duplicate metadata",
                        ));
                    }
                }
                Some(CacheGetPayload::Data(chunk)) => data.extend_from_slice(&chunk),
                None => return Err(Status::internal("cache get returned empty chunk")),
            }
        }

        let expected_size = expected_size
            .ok_or_else(|| Status::internal("cache get stream ended without metadata"))?;
        if expected_size != data.len() as u64 {
            return Err(Status::data_loss(format!(
                "cache returned {} bytes for {}/{}, expected {}",
                data.len(),
                object.bucket,
                object.key,
                expected_size
            )));
        }
        if object.size != data.len() as u64 {
            return Err(Status::data_loss(format!(
                "metadata size {} for {}/{} does not match restored cache bytes {}",
                object.size,
                object.bucket,
                object.key,
                data.len()
            )));
        }
        Ok(data)
    }

    async fn delete_cached_object_best_effort(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) {
        if let Some(cache) = &self.cache {
            let mut client = cache.clone();
            let version_id = version_id.map(str::to_string);
            let _ = client
                .delete(Request::new(DeleteRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id: version_id.clone(),
                }))
                .await;
            let _ = client
                .delete_staging(Request::new(DeleteStagingRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id,
                }))
                .await;
        }
    }
}

#[tonic::async_trait]
impl Phase1SchedulerBackend for MetadataBackedSchedulerBackend {
    async fn list_buckets(&self) -> std::result::Result<Vec<common::BucketInfo>, Status> {
        let mut client = self.metadata.clone();
        Ok(client
            .list_buckets(Request::new(()))
            .await?
            .into_inner()
            .buckets)
    }

    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client
            .create_bucket(Request::new(common::BucketInfo {
                name: bucket.into(),
                created_at: Some(now_timestamp()),
                owner: None,
                versioning_enabled: false,
                object_count: 0,
                total_size: 0,
            }))
            .await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client
            .delete_bucket(Request::new(
                coldstore_proto::metadata::DeleteBucketRequest {
                    name: bucket.into(),
                },
            ))
            .await?;
        Ok(())
    }

    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client
            .get_bucket(Request::new(coldstore_proto::metadata::GetBucketRequest {
                name: bucket.into(),
            }))
            .await?;
        Ok(())
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<common::ObjectMetadata, Status> {
        let mut client = self.metadata.clone();
        Ok(client
            .head_object(Request::new(coldstore_proto::metadata::HeadObjectRequest {
                bucket: bucket.into(),
                key: key.into(),
            }))
            .await?
            .into_inner())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(common::ObjectMetadata, Vec<u8>), Status> {
        let object = self.head_object(bucket, key).await?;
        let data = self.read_restored_object(&object).await?;
        Ok((object, data))
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Vec<u8>,
        content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, Status> {
        let checksum = sha256_hex(&body);
        let size = body.len() as u64;
        let version_id: Option<String> = None;
        let _staging_id = self
            .put_staging_object(StagingPut {
                bucket: bucket.into(),
                key: key.into(),
                version_id: version_id.clone(),
                data: body,
                checksum: checksum.clone(),
                content_type: content_type.clone(),
                etag: checksum.clone(),
            })
            .await?;
        let now = now_timestamp();
        let object = common::ObjectMetadata {
            bucket: bucket.into(),
            key: key.into(),
            version_id: version_id.clone(),
            size,
            checksum: checksum.clone(),
            content_type,
            etag: Some(checksum.clone()),
            storage_class: common::StorageClass::ColdPending as i32,
            archive_id: None,
            tape_id: None,
            tape_set: vec![],
            tape_block_offset: None,
            restore_status: None,
            restore_expire_at: None,
            created_at: Some(now),
            updated_at: Some(now),
        };
        let mut client = self.metadata.clone();
        if let Err(status) = client.put_object(Request::new(object)).await {
            self.delete_staging_best_effort(bucket, key, version_id.as_deref())
                .await;
            return Err(status);
        }
        Ok(PutObjectResponse {
            etag: checksum,
            version_id: String::new(),
        })
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client
            .delete_object(Request::new(
                coldstore_proto::metadata::DeleteObjectRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                },
            ))
            .await?;
        self.delete_cached_object_best_effort(bucket, key, None)
            .await;
        Ok(())
    }

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        days: u32,
        tier: common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, Status> {
        let mut client = self.metadata.clone();
        let object = client
            .get_object(Request::new(coldstore_proto::metadata::GetObjectRequest {
                bucket: bucket.into(),
                key: key.into(),
            }))
            .await?
            .into_inner();

        if object.storage_class != common::StorageClass::Cold as i32 {
            return Err(Status::failed_precondition(
                "restore_object requires an archived COLD object in phase-1 metadata-backed mode",
            ));
        }

        if tier == common::RestoreTier::Expedited {
            return Err(Status::unavailable(
                "glacier expedited retrieval is not available in this environment",
            ));
        }

        let restore_status = object
            .restore_status
            .and_then(|status| common::RestoreStatus::try_from(status).ok());

        match restore_status {
            Some(common::RestoreStatus::RestoreCompleted) => {
                Ok(RestoreObjectResponse { status_code: 200 })
            }
            Some(
                common::RestoreStatus::RestorePending
                | common::RestoreStatus::RestoreWaitingForMedia
                | common::RestoreStatus::RestoreInProgress,
            ) => Ok(RestoreObjectResponse { status_code: 409 }),
            Some(common::RestoreStatus::RestoreExpired | common::RestoreStatus::RestoreFailed) => {
                Err(Status::failed_precondition(
                    "restore_object cannot reopen expired or failed restores in phase-1 metadata-backed mode",
                ))
            }
            Some(common::RestoreStatus::Unspecified) | None => {
                let archive_id = object.archive_id.clone().ok_or_else(|| {
                    Status::failed_precondition("restore_object requires archive_id metadata")
                })?;
                let tape_id = object.tape_id.clone().ok_or_else(|| {
                    Status::failed_precondition("restore_object requires tape_id metadata")
                })?;
                let tape_block_offset = object.tape_block_offset.ok_or_else(|| {
                    Status::failed_precondition("restore_object requires tape_block_offset metadata")
                })?;
                let expire_at = days_from_now(days.max(1));
                client
                    .update_restore_status(Request::new(
                        coldstore_proto::metadata::UpdateRestoreStatusRequest {
                            bucket: bucket.into(),
                            key: key.into(),
                            status: common::RestoreStatus::RestorePending as i32,
                            expire_at: Some(expire_at),
                        },
                    ))
                    .await?;
                let task = common::RecallTask {
                    id: phase1_recall_task_id(bucket, key, object.version_id.as_deref()),
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id: object.version_id.clone(),
                    archive_id,
                    tape_id,
                    tape_set: object.tape_set.clone(),
                    tape_block_offset,
                    object_size: object.size,
                    checksum: object.checksum.clone(),
                    tier: tier as i32,
                    days: days.max(1),
                    expire_at: Some(expire_at),
                    status: common::RestoreStatus::RestorePending as i32,
                    drive_id: None,
                    retry_count: 0,
                    created_at: Some(now_timestamp()),
                    started_at: None,
                    completed_at: None,
                    error: None,
                };
                if let Err(status) = client.put_recall_task(Request::new(task)).await {
                    let _ = self
                        .update_object_restore_status(
                            bucket,
                            key,
                            common::RestoreStatus::RestoreFailed,
                            None,
                        )
                        .await;
                    return Err(status);
                }
                Ok(RestoreObjectResponse { status_code: 202 })
            }
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        marker: Option<&str>,
        delimiter: Option<&str>,
        max_keys: u32,
    ) -> std::result::Result<ListObjectsPage, Status> {
        let mut client = self.metadata.clone();
        let objects = client
            .list_objects(Request::new(
                coldstore_proto::metadata::ListObjectsRequest {
                    bucket: bucket.into(),
                    prefix: prefix.map(str::to_owned),
                    marker: marker.map(str::to_owned),
                    max_keys,
                },
            ))
            .await?
            .into_inner()
            .objects;
        Ok(build_list_objects_page(
            bucket, objects, prefix, marker, delimiter, max_keys,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    use coldstore_proto::scheduler::scheduler_service_server::SchedulerService;
    use prost_types::Timestamp;
    #[test]
    fn build_list_objects_page_respects_max_keys_and_delimiter() {
        let objects = vec![
            common::ObjectMetadata {
                bucket: "docs".into(),
                key: "apple.txt".into(),
                version_id: None,
                size: 1,
                checksum: "".into(),
                content_type: None,
                etag: None,
                storage_class: common::StorageClass::Cold as i32,
                archive_id: None,
                tape_id: None,
                tape_set: vec![],
                tape_block_offset: None,
                restore_status: None,
                restore_expire_at: None,
                created_at: None,
                updated_at: None,
            },
            common::ObjectMetadata {
                bucket: "docs".into(),
                key: "docs/file1".into(),
                version_id: None,
                size: 1,
                checksum: "".into(),
                content_type: None,
                etag: None,
                storage_class: common::StorageClass::Cold as i32,
                archive_id: None,
                tape_id: None,
                tape_set: vec![],
                tape_block_offset: None,
                restore_status: None,
                restore_expire_at: None,
                created_at: None,
                updated_at: None,
            },
            common::ObjectMetadata {
                bucket: "docs".into(),
                key: "docs/file2".into(),
                version_id: None,
                size: 1,
                checksum: "".into(),
                content_type: None,
                etag: None,
                storage_class: common::StorageClass::Cold as i32,
                archive_id: None,
                tape_id: None,
                tape_set: vec![],
                tape_block_offset: None,
                restore_status: None,
                restore_expire_at: None,
                created_at: None,
                updated_at: None,
            },
            common::ObjectMetadata {
                bucket: "docs".into(),
                key: "video.txt".into(),
                version_id: None,
                size: 1,
                checksum: "".into(),
                content_type: None,
                etag: None,
                storage_class: common::StorageClass::Cold as i32,
                archive_id: None,
                tape_id: None,
                tape_set: vec![],
                tape_block_offset: None,
                restore_status: None,
                restore_expire_at: None,
                created_at: None,
                updated_at: None,
            },
            common::ObjectMetadata {
                bucket: "docs".into(),
                key: "zebra.txt".into(),
                version_id: None,
                size: 1,
                checksum: "".into(),
                content_type: None,
                etag: None,
                storage_class: common::StorageClass::Cold as i32,
                archive_id: None,
                tape_id: None,
                tape_set: vec![],
                tape_block_offset: None,
                restore_status: None,
                restore_expire_at: None,
                created_at: None,
                updated_at: None,
            },
        ];

        let page = build_list_objects_page("docs", objects, None, None, Some("/"), 3);

        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "apple.txt");
        assert_eq!(page.objects[1].key, "video.txt");
        assert_eq!(page.common_prefixes, vec!["docs/"]);
        assert!(page.is_truncated);
        assert_eq!(page.next_marker.as_deref(), Some("video.txt"));
    }

    #[test]
    fn build_list_objects_page_with_max_keys_zero_is_unbounded() {
        let objects = vec![common::ObjectMetadata {
            bucket: "docs".into(),
            key: "alpha.txt".into(),
            version_id: None,
            size: 1,
            checksum: "".into(),
            content_type: None,
            etag: None,
            storage_class: common::StorageClass::Cold as i32,
            archive_id: None,
            tape_id: None,
            tape_set: vec![],
            tape_block_offset: None,
            restore_status: None,
            restore_expire_at: None,
            created_at: None,
            updated_at: None,
        }];

        let page = build_list_objects_page("docs", objects, None, None, Some("/"), 0);

        assert!(!page.is_truncated);
        assert!(page.next_marker.is_none());
        assert_eq!(page.objects.len(), 1);
        assert_eq!(page.objects[0].key, "alpha.txt");
    }

    #[tokio::test]
    async fn default_service_uses_metadata_for_bucket_ops() {
        let (svc, _state, shutdown_tx) = metadata_backed_service().await;

        svc.create_bucket(Request::new(CreateBucketRequest {
            bucket: "phase1-bucket".into(),
        }))
        .await
        .expect("create bucket through metadata-backed scheduler");

        let buckets = svc
            .list_buckets(Request::new(()))
            .await
            .expect("list buckets through metadata-backed scheduler")
            .into_inner();
        assert!(buckets
            .buckets
            .iter()
            .any(|bucket| bucket.name == "phase1-bucket"));

        shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn default_service_uses_metadata_for_object_metadata_ops() {
        let (svc, state, shutdown_tx) = metadata_backed_service().await;

        svc.create_bucket(Request::new(CreateBucketRequest {
            bucket: "docs".into(),
        }))
        .await
        .expect("create bucket");

        let mut metadata = state.metadata.clone();
        metadata
            .put_object(Request::new(common::ObjectMetadata {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
                size: 5,
                checksum: "seed-checksum".into(),
                content_type: Some("text/plain".into()),
                etag: Some("seed-etag".into()),
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
            }))
            .await
            .expect("seed object in metadata");

        let head = svc
            .head_object(Request::new(HeadObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
            }))
            .await
            .expect("head object")
            .into_inner();
        assert_eq!(head.content_length, 5);
        assert_eq!(head.storage_class, common::StorageClass::ColdPending as i32);

        let list = svc
            .list_objects(Request::new(ListObjectsRequest {
                bucket: "docs".into(),
                prefix: Some("gui".into()),
                marker: None,
                delimiter: None,
                max_keys: 10,
            }))
            .await
            .expect("list objects")
            .into_inner();
        assert_eq!(list.contents.len(), 1);
        assert_eq!(list.contents[0].key, "guide.txt");

        let mut metadata = state.metadata.clone();
        metadata
            .update_archive_location(Request::new(
                coldstore_proto::metadata::UpdateArchiveLocationRequest {
                    bucket: "docs".into(),
                    key: "guide.txt".into(),
                    archive_id: "archive-guide".into(),
                    tape_id: "TAPE-PHASE1".into(),
                    tape_set: vec!["TAPE-PHASE1".into()],
                    tape_block_offset: 0,
                },
            ))
            .await
            .expect("set archive location");
        metadata
            .update_storage_class(Request::new(
                coldstore_proto::metadata::UpdateStorageClassRequest {
                    bucket: "docs".into(),
                    key: "guide.txt".into(),
                    storage_class: common::StorageClass::Cold as i32,
                },
            ))
            .await
            .expect("mark object as cold");

        let restore = svc
            .restore_object(Request::new(RestoreObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
                days: 3,
                tier: common::RestoreTier::Standard as i32,
            }))
            .await
            .expect("restore object")
            .into_inner();
        assert_eq!(restore.status_code, 202);

        let restore_conflict = svc
            .restore_object(Request::new(RestoreObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
                days: 3,
                tier: common::RestoreTier::Standard as i32,
            }))
            .await
            .expect("restore object again")
            .into_inner();
        assert_eq!(restore_conflict.status_code, 409);

        let expedited_err = svc
            .restore_object(Request::new(RestoreObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
                days: 3,
                tier: common::RestoreTier::Expedited as i32,
            }))
            .await
            .expect_err("expedited restore should be unavailable");
        assert_eq!(expedited_err.code(), tonic::Code::Unavailable);
        assert!(expedited_err
            .message()
            .to_lowercase()
            .contains("expedited retrieval"));

        let restored = svc
            .head_object(Request::new(HeadObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
            }))
            .await
            .expect("head restored object")
            .into_inner();
        assert_eq!(
            restored.restore_info.as_deref(),
            Some("ongoing-request=\"true\"")
        );

        svc.delete_object(Request::new(DeleteObjectRequest {
            bucket: "docs".into(),
            key: "guide.txt".into(),
            version_id: None,
        }))
        .await
        .expect("delete object through metadata-backed scheduler");

        assert!(svc
            .head_object(Request::new(HeadObjectRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: None,
            }))
            .await
            .is_err());

        shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn default_metadata_backend_rejects_put_when_cache_is_missing() {
        let (_svc, state, shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");

        let err = backend
            .put_object(
                "docs",
                "guide.txt",
                b"hello".to_vec(),
                Some("text/plain".into()),
            )
            .await
            .expect_err("put object requires cache staging");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("cache client"));

        shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn default_metadata_backend_puts_object_into_cache_staging_and_metadata() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let (mut raw_cache_client, cache_shutdown_tx) = cache_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new_with_cache(
            state.metadata.clone(),
            Some(raw_cache_client.clone()),
        );

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");

        let put = backend
            .put_object(
                "docs",
                "guide.txt",
                b"hello".to_vec(),
                Some("text/plain".into()),
            )
            .await
            .expect("put object through cache-backed metadata backend");
        assert!(!put.etag.is_empty());

        let listed = backend
            .list_objects("docs", Some("gui"), None, None, 10)
            .await
            .expect("list objects through metadata backend");
        assert_eq!(listed.objects.len(), 1);
        assert_eq!(listed.objects[0].etag.as_deref(), Some(put.etag.as_str()));
        assert_eq!(
            listed.objects[0].storage_class,
            common::StorageClass::ColdPending as i32
        );

        let (staging_meta, staging_body) =
            read_staging_object(&mut raw_cache_client, "docs", "guide.txt").await;
        assert_eq!(staging_meta.size, 5);
        assert_eq!(staging_meta.checksum.as_deref(), Some(put.etag.as_str()));
        assert_eq!(staging_body, b"hello");

        metadata_shutdown_tx.send(()).ok();
        cache_shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn default_metadata_backend_gets_object_from_restored_cache() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let (mut raw_cache_client, cache_shutdown_tx) = cache_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new_with_cache(
            state.metadata.clone(),
            Some(raw_cache_client.clone()),
        );

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let body = b"restored-body".to_vec();
        let mut metadata = state.metadata.clone();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "guide.txt",
            &body,
            common::StorageClass::Cold,
            Some(common::RestoreStatus::RestoreCompleted),
        )
        .await;
        put_restored_cache_object(&mut raw_cache_client, "docs", "guide.txt", body.clone()).await;

        let (object, data) = backend
            .get_object("docs", "guide.txt")
            .await
            .expect("get restored object");
        assert_eq!(
            object.restore_status,
            Some(common::RestoreStatus::RestoreCompleted as i32)
        );
        assert_eq!(data, body);

        metadata_shutdown_tx.send(()).ok();
        cache_shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn restore_object_creates_pending_recall_task() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let body = b"archive-me".to_vec();
        let mut metadata = state.metadata.clone();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "restore.txt",
            &body,
            common::StorageClass::Cold,
            None,
        )
        .await;
        metadata
            .update_archive_location(Request::new(
                coldstore_proto::metadata::UpdateArchiveLocationRequest {
                    bucket: "docs".into(),
                    key: "restore.txt".into(),
                    archive_id: "archive-restore".into(),
                    tape_id: "TAPE-RESTORE".into(),
                    tape_set: vec!["TAPE-RESTORE".into()],
                    tape_block_offset: 7,
                },
            ))
            .await
            .expect("set archive location");

        let response = backend
            .restore_object("docs", "restore.txt", 5, common::RestoreTier::Standard)
            .await
            .expect("queue restore");
        assert_eq!(response.status_code, 202);

        let object = backend
            .head_object("docs", "restore.txt")
            .await
            .expect("head restored-pending object");
        assert_eq!(
            object.restore_status,
            Some(common::RestoreStatus::RestorePending as i32)
        );

        let tasks = metadata
            .list_pending_recall_tasks(Request::new(()))
            .await
            .expect("list pending recall tasks")
            .into_inner()
            .tasks;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].bucket, "docs");
        assert_eq!(tasks[0].key, "restore.txt");
        assert_eq!(tasks[0].archive_id, "archive-restore");
        assert_eq!(tasks[0].tape_id, "TAPE-RESTORE");
        assert_eq!(tasks[0].tape_block_offset, 7);
        assert_eq!(tasks[0].object_size, body.len() as u64);
        assert_eq!(tasks[0].checksum, sha256_hex(&body));
        assert_eq!(
            tasks[0].status,
            common::RestoreStatus::RestorePending as i32
        );

        metadata_shutdown_tx.send(()).ok();
    }
}
