//! The archive pipeline: staging hand-off, the archive task lifecycle, and the
//! background archive loop.
//!
//! AR-00 moved these out of `service.rs` with no behavior change. The ordering rule AGENTS.md §5
//! states — staging is deleted only after the metadata commit — lives in
//! `archive_staging_batch`, and `validate_staged_object_matches_metadata` is the §2.3 integrity
//! check that must not be weakened.

use crate::clients::cache::{CacheArchiveClient, Phase1ArchiveCache, Phase1StagedObject};
use crate::clients::tape::{TapeArchiveClient, TapeArchiveWriter};
use crate::leases::*;
use crate::service::{ArchiveBatchResult, MetadataBackedSchedulerBackend, Phase1SchedulerBackend};
use crate::util::*;
use crate::SchedulerState;
use coldstore_proto::cache::put_staging_request::Payload as PutStagingPayload;
use coldstore_proto::cache::{
    DeleteStagingRequest, PutStagingMeta, PutStagingRequest, PutStagingTrailer,
};
use coldstore_proto::common;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tonic::{Request, Status};
use tracing::{debug, info, warn};

pub(crate) struct StagingPut {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) data: Vec<u8>,
    pub(crate) checksum: String,
    pub(crate) content_type: Option<String>,
    pub(crate) etag: String,
}

type ArchiveProcessResult = std::result::Result<
    (Option<u64>, Option<common::ArchiveTask>),
    (Status, Option<common::ArchiveTask>),
>;

impl MetadataBackedSchedulerBackend {
    pub(crate) async fn put_staging_object(
        &self,
        staging: StagingPut,
    ) -> std::result::Result<String, Status> {
        let mut client = self.cache_client()?;
        let size = staging.data.len() as u64;
        let checksum = staging.checksum.clone();
        let mut chunks = vec![
            PutStagingRequest {
                payload: Some(PutStagingPayload::Meta(PutStagingMeta {
                    bucket: staging.bucket,
                    key: staging.key,
                    version_id: staging.version_id,
                    size,
                    checksum: Some(staging.checksum),
                    content_type: staging.content_type,
                    etag: Some(staging.etag),
                })),
            },
            PutStagingRequest {
                payload: Some(PutStagingPayload::Data(staging.data)),
            },
        ];
        // The body is already hashed here to make the ETag, so declaring that digest
        // costs nothing and moves a corrupt staging write from archive time, when the
        // archiver reads the bundle back, to the PUT that caused it.
        if !checksum.is_empty() {
            chunks.push(PutStagingRequest {
                payload: Some(PutStagingPayload::Trailer(PutStagingTrailer {
                    sha256: checksum,
                })),
            });
        }
        Ok(client
            .put_staging(Request::new(tokio_stream::iter(chunks)))
            .await?
            .into_inner()
            .staging_id)
    }

    pub(crate) async fn delete_staging_best_effort(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) {
        if let Some(cache) = &self.cache {
            let mut client = cache.clone();
            let _ = client
                .delete_staging(Request::new(DeleteStagingRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    version_id: version_id.map(str::to_owned),
                }))
                .await;
        }
    }

    pub async fn archive_staging_batch<C, T>(
        &self,
        state: &SchedulerState,
        cache: &C,
        tape: &T,
        limit: u32,
    ) -> std::result::Result<ArchiveBatchResult, Status>
    where
        C: Phase1ArchiveCache + ?Sized,
        T: TapeArchiveWriter + ?Sized,
    {
        let entries = cache.list_staging_keys(limit).await?;
        let mut result = ArchiveBatchResult::default();

        for entry in entries {
            let item_key =
                phase1_archive_item_key(&entry.bucket, &entry.key, entry.version_id.as_deref());
            if !acquire_archive_item(state, &item_key).await {
                continue;
            }

            let process_result: ArchiveProcessResult = async {
                let object = self
                    .head_object(&entry.bucket, &entry.key)
                    .await
                    .map_err(|status| (status, None))?;
                let bundle_id =
                    phase1_bundle_id(&entry.bucket, &entry.key, entry.version_id.as_deref());
                let mut task = self.phase1_archive_task(
                    &entry.bucket,
                    &entry.key,
                    entry.version_id.as_deref(),
                    &bundle_id,
                    &state.config.archive.tape_id,
                    object.size,
                );

                if object.storage_class != common::StorageClass::ColdPending as i32 {
                    self.delete_staging_best_effort(
                        &entry.bucket,
                        &entry.key,
                        entry.version_id.as_deref(),
                    )
                    .await;
                    return Ok((None, None));
                }

                if !self
                    .claim_archive_task(&mut task)
                    .await
                    .map_err(|status| (status, Some(task.clone())))?
                {
                    return Ok((None, None));
                }

                let staged = cache
                    .get_staging(&entry.bucket, &entry.key, entry.version_id.as_deref())
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;
                validate_staged_object_matches_metadata(&object, &staged)
                    .map_err(|status| (*status, Some(task.clone())))?;

                let checksum = sha256_hex(&staged.data);
                let write = tape
                    .write_bundle(&bundle_id, 1, staged.data.clone())
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;
                if write.bytes_written != staged.data.len() as u64 {
                    return Err((
                        Status::internal(format!(
                            "tape writer reported {} bytes for {bundle_id}, expected {}",
                            write.bytes_written,
                            staged.data.len()
                        )),
                        Some(task),
                    ));
                }

                let current_object = self
                    .head_object(&entry.bucket, &entry.key)
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;
                if current_object.storage_class != common::StorageClass::ColdPending as i32 {
                    return Err((
                        Status::failed_precondition(format!(
                            "object {}/{} changed storage class during archive",
                            entry.bucket, entry.key
                        )),
                        Some(task),
                    ));
                }
                validate_staged_object_matches_metadata(&current_object, &staged)
                    .map_err(|status| (*status, Some(task.clone())))?;

                let now = now_timestamp();
                let bundle = common::ArchiveBundle {
                    id: bundle_id.clone(),
                    tape_id: write.tape_id.clone(),
                    tape_set: write.tape_set.clone(),
                    entries: vec![common::BundleEntry {
                        bucket: entry.bucket.clone(),
                        key: entry.key.clone(),
                        version_id: entry.version_id.clone(),
                        size: staged.data.len() as u64,
                        offset_in_bundle: 0,
                        tape_block_offset: write.filemark_start as u64,
                        checksum: checksum.clone(),
                    }],
                    total_size: write.bytes_written,
                    filemark_start: write.filemark_start,
                    filemark_end: write.filemark_end,
                    checksum: Some(checksum),
                    status: common::ArchiveBundleStatus::BundleCompleted as i32,
                    created_at: Some(now),
                    completed_at: Some(now),
                };

                let mut client = self.metadata.clone();
                client
                    .complete_archive_object(Request::new(
                        coldstore_proto::metadata::CompleteArchiveObjectRequest {
                            bucket: entry.bucket.clone(),
                            key: entry.key.clone(),
                            version_id: current_object.version_id.clone(),
                            expected_size: Some(current_object.size),
                            expected_checksum: Some(current_object.checksum.clone()),
                            expected_storage_class: Some(current_object.storage_class),
                            expected_updated_at: current_object.updated_at,
                            bundle: Some(bundle),
                            tape_block_offset: write.filemark_start as u64,
                            storage_class: common::StorageClass::Cold as i32,
                        },
                    ))
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;
                cache
                    .delete_staging(&entry.bucket, &entry.key, entry.version_id.as_deref())
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;

                self.finalize_archive_task_success(&mut task, write.bytes_written)
                    .await
                    .map_err(|status| (status, Some(task.clone())))?;

                result.archived_objects += 1;
                result.bytes_written += write.bytes_written;
                result.bundle_ids.push(bundle_id);

                Ok((Some(write.bytes_written), Some(task)))
            }
            .await;

            match process_result {
                Ok((_bytes_written, _task)) => {
                    release_archive_item(state, &item_key).await;
                }
                Err((status, maybe_task)) => {
                    if let Some(task) = maybe_task {
                        let _ = self
                            .mark_archive_task_failed(task, status.message().to_string())
                            .await;
                    }
                    release_archive_item(state, &item_key).await;
                    return Err(status);
                }
            }
        }

        Ok(result)
    }

    async fn update_archive_task(
        &self,
        task: common::ArchiveTask,
    ) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client.update_archive_task(Request::new(task)).await?;
        Ok(())
    }

    async fn get_archive_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<common::ArchiveTask, Status> {
        let mut client = self.metadata.clone();
        let task = client
            .get_archive_task(Request::new(
                coldstore_proto::metadata::GetArchiveTaskRequest { id: task_id.into() },
            ))
            .await?
            .into_inner();
        Ok(task)
    }

    fn phase1_archive_task(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        bundle_id: &str,
        tape_id: &str,
        object_size: u64,
    ) -> common::ArchiveTask {
        let _ = (bucket, key, version_id);
        common::ArchiveTask {
            id: bundle_id.to_string(),
            bundle_id: bundle_id.to_string(),
            tape_id: tape_id.to_string(),
            drive_id: None,
            object_count: 1,
            total_size: object_size,
            bytes_written: 0,
            status: common::ArchiveTaskStatus::ArchiveTaskPending as i32,
            retry_count: 0,
            created_at: Some(now_timestamp()),
            started_at: None,
            completed_at: None,
            error: None,
        }
    }

    async fn claim_archive_task(
        &self,
        task: &mut common::ArchiveTask,
    ) -> std::result::Result<bool, Status> {
        let mut client = self.metadata.clone();
        if let Err(status) = client.put_archive_task(Request::new(task.clone())).await {
            if status.code() != tonic::Code::AlreadyExists {
                return Err(status);
            }
        }

        let mut current = self.get_archive_task(&task.id).await?;
        let current_status = common::ArchiveTaskStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid archive task status"))?;

        match current_status {
            common::ArchiveTaskStatus::ArchiveTaskCompleted => {
                *task = current;
                Ok(false)
            }
            common::ArchiveTaskStatus::ArchiveTaskFailed => Ok(false),
            common::ArchiveTaskStatus::ArchiveTaskInProgress => {
                *task = current;
                Ok(false)
            }
            common::ArchiveTaskStatus::ArchiveTaskPending => {
                current.status = common::ArchiveTaskStatus::ArchiveTaskInProgress as i32;
                current.error = None;
                current.started_at = Some(now_timestamp());
                if let Err(status) = client
                    .update_archive_task(Request::new(current.clone()))
                    .await
                {
                    if status.code() == tonic::Code::FailedPrecondition {
                        return Ok(false);
                    }
                    return Err(status);
                }
                *task = current;
                Ok(true)
            }
            _ => Err(Status::failed_precondition(
                "invalid archive task state for claim",
            )),
        }
    }

    async fn finalize_archive_task_success(
        &self,
        task: &mut common::ArchiveTask,
        bytes_written: u64,
    ) -> std::result::Result<(), Status> {
        let mut current = self.get_archive_task(&task.id).await?;
        let current_status = common::ArchiveTaskStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid archive task status"))?;

        if current_status == common::ArchiveTaskStatus::ArchiveTaskCompleted {
            return Ok(());
        }

        current.status = common::ArchiveTaskStatus::ArchiveTaskCompleted as i32;
        current.bytes_written = bytes_written;
        current.error = None;
        current.completed_at = Some(now_timestamp());

        if let Err(status) = self.update_archive_task(current.clone()).await {
            if status.code() == tonic::Code::FailedPrecondition {
                return Ok(());
            }
            return Err(status);
        }
        *task = current;
        Ok(())
    }

    async fn mark_archive_task_failed(
        &self,
        task: common::ArchiveTask,
        error: String,
    ) -> std::result::Result<(), Status> {
        let mut current = self.get_archive_task(&task.id).await?;
        let current_status = common::ArchiveTaskStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid archive task status"))?;

        if matches!(
            current_status,
            common::ArchiveTaskStatus::ArchiveTaskCompleted
                | common::ArchiveTaskStatus::ArchiveTaskFailed
        ) {
            return Ok(());
        }

        current.status = common::ArchiveTaskStatus::ArchiveTaskFailed as i32;
        current.error = Some(error);
        current.completed_at = Some(now_timestamp());
        current.retry_count = current.retry_count.saturating_add(1);

        self.update_archive_task(current).await
    }
}

fn phase1_bundle_id(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    match version_id.filter(|version| !version.is_empty()) {
        Some(version) => format!("phase1-bundle:{bucket}/{key}#{version}"),
        None => format!("phase1-bundle:{bucket}/{key}"),
    }
}

fn validate_staged_object_matches_metadata(
    object: &common::ObjectMetadata,
    staged: &Phase1StagedObject,
) -> std::result::Result<(), Box<Status>> {
    if staged.meta.bucket != object.bucket
        || staged.meta.key != object.key
        || staged.meta.version_id != object.version_id
    {
        return Err(Box::new(Status::data_loss(format!(
            "staging identity mismatch: cache returned {}/{}#{:?} for metadata object {}/{}#{:?}",
            staged.meta.bucket,
            staged.meta.key,
            staged.meta.version_id,
            object.bucket,
            object.key,
            object.version_id
        ))));
    }

    if staged.meta.size != staged.data.len() as u64 {
        return Err(Box::new(Status::data_loss(format!(
            "staging size mismatch for {}/{}: meta={}, bytes={}",
            object.bucket,
            object.key,
            staged.meta.size,
            staged.data.len()
        ))));
    }

    if object.size != staged.data.len() as u64 {
        return Err(Box::new(Status::data_loss(format!(
            "metadata size {} for {}/{} does not match staging bytes {}",
            object.size,
            object.bucket,
            object.key,
            staged.data.len()
        ))));
    }

    let computed_checksum = sha256_hex(&staged.data);
    if let Some(staging_checksum) = staged
        .meta
        .checksum
        .as_deref()
        .filter(|checksum| !checksum.trim().is_empty())
    {
        if !checksum_matches(staging_checksum, &computed_checksum) {
            return Err(Box::new(Status::data_loss(format!(
                "staging checksum mismatch for {}/{}",
                object.bucket, object.key
            ))));
        }
    }

    let metadata_checksum = object.checksum.trim();
    if !metadata_checksum.is_empty() && !checksum_matches(metadata_checksum, &computed_checksum) {
        return Err(Box::new(Status::data_loss(format!(
            "metadata checksum mismatch for {}/{}",
            object.bucket, object.key
        ))));
    }

    Ok(())
}

/// The shared rule lives in `coldstore-common` so the cache's staging-trailer check and
/// this archive-time check cannot drift apart (AGENTS.md 6.5).
fn checksum_matches(expected: &str, computed_hex: &str) -> bool {
    coldstore_common::checksum::sha256_matches(expected, computed_hex)
}

pub(crate) async fn run_archive_agent(state: Arc<SchedulerState>, agent_id: u64) {
    let every = Duration::from_secs(state.config.archive.scan_interval_secs.max(1));
    let mut ticker = interval(every);
    info!(
        agent_id,
        "scheduler archive agent enabled: interval={}s batch_size={}",
        every.as_secs(),
        state.config.archive.batch_size
    );

    loop {
        ticker.tick().await;
        let _permit = state
            .archive_slots
            .clone()
            .acquire_owned()
            .await
            .expect("archive concurrency slot should be available");
        match archive_staging_once(state.clone()).await {
            Ok(result) if result.archived_objects > 0 => {
                info!(
                    archived_objects = result.archived_objects,
                    bytes_written = result.bytes_written,
                    "scheduler archive loop archived staging objects"
                );
            }
            Ok(_) => debug!("scheduler archive loop found no staging objects"),
            Err(status) => warn!(
                code = ?status.code(),
                message = status.message(),
                "scheduler archive loop failed"
            ),
        }
    }
}

pub async fn archive_staging_once(
    state: Arc<SchedulerState>,
) -> std::result::Result<ArchiveBatchResult, Status> {
    let cache = state
        .cache
        .clone()
        .ok_or_else(|| Status::failed_precondition("scheduler cache client is not configured"))?;
    let tape = state
        .tape
        .clone()
        .ok_or_else(|| Status::failed_precondition("scheduler tape client is not configured"))?;
    let backend =
        MetadataBackedSchedulerBackend::new_with_cache(state.metadata.clone(), Some(cache.clone()));
    let cache = CacheArchiveClient::new(cache);
    let tape_set = if state.config.archive.tape_set.is_empty() {
        vec![state.config.archive.tape_id.clone()]
    } else {
        state.config.archive.tape_set.clone()
    };

    let tape_id = tape_set
        .first()
        .cloned()
        .unwrap_or_else(|| state.config.archive.tape_id.clone());
    let _tape_lease = acquire_tape_lease(&state, &tape_id).await?;

    let tape = TapeArchiveClient::new(
        tape,
        state.config.archive.drive_id.clone(),
        state.config.archive.tape_id.clone(),
        tape_set,
        state.config.archive.block_size,
    );
    backend
        .archive_staging_batch(
            &state,
            &cache,
            &tape,
            state.config.archive.batch_size.min(u32::MAX as usize) as u32,
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clients::cache::Phase1StagedObject;
    use coldstore_proto::cache::StagingObjectMeta;
    use tonic::Code;
    #[test]
    fn staged_object_validation_rejects_metadata_checksum_mismatch() {
        let data = b"staged bytes".to_vec();
        let staged_checksum = sha256_hex(&data);
        let object = common::ObjectMetadata {
            bucket: "docs".into(),
            key: "a.bin".into(),
            version_id: None,
            size: data.len() as u64,
            checksum: sha256_hex(b"different bytes"),
            ..Default::default()
        };
        let staged = Phase1StagedObject {
            meta: StagingObjectMeta {
                bucket: "docs".into(),
                key: "a.bin".into(),
                version_id: None,
                size: data.len() as u64,
                checksum: Some(staged_checksum),
                ..Default::default()
            },
            data,
        };

        let status = validate_staged_object_matches_metadata(&object, &staged)
            .expect_err("checksum mismatch must fail");
        assert_eq!(status.code(), Code::DataLoss);
    }

    #[test]
    fn staged_object_validation_accepts_prefixed_sha256_checksum() {
        let data = b"staged bytes".to_vec();
        let checksum = sha256_hex(&data);
        let object = common::ObjectMetadata {
            bucket: "docs".into(),
            key: "a.bin".into(),
            version_id: Some("v1".into()),
            size: data.len() as u64,
            checksum: format!("sha256:{checksum}"),
            ..Default::default()
        };
        let staged = Phase1StagedObject {
            meta: StagingObjectMeta {
                bucket: "docs".into(),
                key: "a.bin".into(),
                version_id: Some("v1".into()),
                size: data.len() as u64,
                checksum: Some(checksum),
                ..Default::default()
            },
            data,
        };

        validate_staged_object_matches_metadata(&object, &staged)
            .expect("matching checksum should pass");
    }
}
