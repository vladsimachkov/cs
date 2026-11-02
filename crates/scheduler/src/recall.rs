//! The recall pipeline: the recall task lifecycle, the tape-to-cache restore step,
//! and the background recall loop.
//!
//! AR-00 moved these out of `service.rs` with no behavior change. Retriable failures move a task to
//! `WaitingForMedia` and everything else to `Failed`, which is the rule AGENTS.md §5 states; the
//! size and SHA-256 checks in `process_recall_task` are §2.3 integrity checks.

use crate::clients::cache::{CacheRestoreClient, Phase1RestoreCache};
use crate::clients::tape::{TapeRecallClient, TapeRecallReader};
use crate::leases::*;
use crate::responses::is_pending_recall_task;
use crate::service::{MetadataBackedSchedulerBackend, Phase1SchedulerBackend, RecallBatchResult};
use crate::util::*;
use crate::SchedulerState;
use coldstore_proto::common;
use prost_types::Timestamp;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tonic::{Request, Status};
use tracing::{debug, error, info};

impl MetadataBackedSchedulerBackend {
    async fn update_recall_task(
        &self,
        task: common::RecallTask,
    ) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client.update_recall_task(Request::new(task)).await?;
        Ok(())
    }

    async fn get_recall_task(
        &self,
        task_id: &str,
    ) -> std::result::Result<common::RecallTask, Status> {
        let mut client = self.metadata.clone();
        let task = client
            .get_recall_task(Request::new(
                coldstore_proto::metadata::GetRecallTaskRequest { id: task_id.into() },
            ))
            .await?
            .into_inner();
        Ok(task)
    }

    async fn claim_recall_task(
        &self,
        task: &mut common::RecallTask,
    ) -> std::result::Result<bool, Status> {
        let mut client = self.metadata.clone();
        if let Err(status) = client.put_recall_task(Request::new(task.clone())).await {
            if status.code() != tonic::Code::AlreadyExists {
                return Err(status);
            }
        }

        let mut current = self.get_recall_task(&task.id).await?;
        let current_status = common::RestoreStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid restore task status"))?;

        match current_status {
            common::RestoreStatus::RestoreCompleted | common::RestoreStatus::RestoreExpired => {
                *task = current;
                Ok(false)
            }
            common::RestoreStatus::RestoreInProgress => {
                *task = current;
                Ok(false)
            }
            common::RestoreStatus::RestorePending
            | common::RestoreStatus::RestoreWaitingForMedia => {
                current.status = common::RestoreStatus::RestoreInProgress as i32;
                current.error = None;
                current.started_at = Some(now_timestamp());
                if let Err(status) = client
                    .update_recall_task(Request::new(current.clone()))
                    .await
                {
                    if status.code() == tonic::Code::FailedPrecondition {
                        return Ok(false);
                    }
                    return Err(status);
                }
                self.update_object_restore_status(
                    &current.bucket,
                    &current.key,
                    common::RestoreStatus::RestoreInProgress,
                    current.expire_at,
                )
                .await?;
                *task = current;
                Ok(true)
            }
            common::RestoreStatus::RestoreFailed => {
                *task = current;
                Ok(false)
            }
            common::RestoreStatus::Unspecified => Ok(false),
        }
    }

    pub(crate) async fn update_object_restore_status(
        &self,
        bucket: &str,
        key: &str,
        status: common::RestoreStatus,
        expire_at: Option<Timestamp>,
    ) -> std::result::Result<(), Status> {
        let mut client = self.metadata.clone();
        client
            .update_restore_status(Request::new(
                coldstore_proto::metadata::UpdateRestoreStatusRequest {
                    bucket: bucket.into(),
                    key: key.into(),
                    status: status as i32,
                    expire_at,
                },
            ))
            .await?;
        Ok(())
    }

    async fn mark_recall_failed(
        &self,
        task: common::RecallTask,
        error: String,
    ) -> std::result::Result<(), Status> {
        let mut current = self.get_recall_task(&task.id).await?;
        let status = common::RestoreStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid restore task status"))?;

        if matches!(
            status,
            common::RestoreStatus::RestoreCompleted | common::RestoreStatus::RestoreFailed
        ) {
            return Ok(());
        }

        current.status = common::RestoreStatus::RestoreFailed as i32;
        current.retry_count = current.retry_count.saturating_add(1);
        current.completed_at = Some(now_timestamp());
        current.error = Some(error);
        if let Err(err) = self.update_recall_task(current.clone()).await {
            if err.code() != tonic::Code::FailedPrecondition {
                return Err(err);
            }
        }

        self.update_object_restore_status(
            &current.bucket,
            &current.key,
            common::RestoreStatus::RestoreFailed,
            None,
        )
        .await?;
        Ok(())
    }

    async fn retry_recall_task_later(
        &self,
        task: &mut common::RecallTask,
        error: String,
    ) -> std::result::Result<(), Status> {
        let mut current = self.get_recall_task(&task.id).await?;
        let current_status = common::RestoreStatus::try_from(current.status)
            .map_err(|_| Status::internal("invalid recall task status"))?;

        if current_status != common::RestoreStatus::RestoreInProgress {
            return Ok(());
        }

        current.status = common::RestoreStatus::RestoreWaitingForMedia as i32;
        current.error = Some(error);
        current.started_at = None;
        current.retry_count = current.retry_count.saturating_add(1);
        self.update_recall_task(current.clone()).await?;
        self.update_object_restore_status(
            &current.bucket,
            &current.key,
            common::RestoreStatus::RestoreWaitingForMedia,
            current.expire_at,
        )
        .await?;
        task.status = current.status;
        task.started_at = None;
        Ok(())
    }

    pub async fn recall_pending_batch<C, T>(
        &self,
        state: &SchedulerState,
        cache: &C,
        tape: &T,
        limit: usize,
    ) -> std::result::Result<RecallBatchResult, Status>
    where
        C: Phase1RestoreCache + ?Sized,
        T: TapeRecallReader + ?Sized,
    {
        let mut client = self.metadata.clone();
        let mut tasks = client
            .list_pending_recall_tasks(Request::new(()))
            .await?
            .into_inner()
            .tasks
            .into_iter()
            .filter(is_pending_recall_task)
            .collect::<Vec<_>>();
        tasks.sort_by(|a, b| {
            let tier = restore_tier_rank(a.tier).cmp(&restore_tier_rank(b.tier));
            tier.then_with(|| a.tape_id.cmp(&b.tape_id))
                .then_with(|| a.tape_block_offset.cmp(&b.tape_block_offset))
                .then_with(|| {
                    a.created_at
                        .as_ref()
                        .map(|v| v.seconds)
                        .cmp(&b.created_at.as_ref().map(|v| v.seconds))
                })
        });
        let mut result = RecallBatchResult::default();

        for task in tasks.into_iter().take(limit) {
            let _task_slot = match state.recall_task_slots.clone().acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    release_recall_task(state, &phase1_recall_task_key(&task)).await;
                    return Err(Status::internal("recall task concurrency slots closed"));
                }
            };
            let task_key = phase1_recall_task_key(&task);
            if !acquire_recall_task(state, &task_key).await {
                continue;
            }

            let _tape_lease = match acquire_tape_lease(state, &task.tape_id).await {
                Ok(lease) => lease,
                Err(status) => {
                    release_recall_task(state, &task_key).await;
                    return Err(status);
                }
            };

            let mut claimed_task = task;
            let claim_result = match self.claim_recall_task(&mut claimed_task).await {
                Ok(claimed) => claimed,
                Err(status) => {
                    release_recall_task(state, &task_key).await;
                    return Err(status);
                }
            };

            if !claim_result {
                release_recall_task(state, &task_key).await;
                continue;
            }

            let restore_result = self
                .process_recall_task(cache, tape, claimed_task.clone())
                .await;
            match restore_result {
                Ok(bytes_read) => {
                    result.restored_objects += 1;
                    result.bytes_read += bytes_read;
                    result.task_ids.push(claimed_task.id);
                }
                Err(status) => {
                    let finalize_result = if is_retriable_recall_processing_error(&status) {
                        self.retry_recall_task_later(
                            &mut claimed_task,
                            status.message().to_string(),
                        )
                        .await
                    } else {
                        self.mark_recall_failed(claimed_task.clone(), status.message().to_string())
                            .await
                    };
                    if let Err(mark_status) = finalize_result {
                        release_recall_task(state, &task_key).await;
                        return Err(mark_status);
                    }
                }
            }
            release_recall_task(state, &task_key).await;
        }

        Ok(result)
    }

    async fn process_recall_task<C, T>(
        &self,
        cache: &C,
        tape: &T,
        mut task: common::RecallTask,
    ) -> std::result::Result<u64, Status>
    where
        C: Phase1RestoreCache + ?Sized,
        T: TapeRecallReader + ?Sized,
    {
        let object = self.head_object(&task.bucket, &task.key).await?;
        if object.storage_class != common::StorageClass::Cold as i32 {
            return Err(Status::failed_precondition(format!(
                "recall task {} requires a COLD object",
                task.id
            )));
        }
        if object.archive_id.as_deref() != Some(task.archive_id.as_str()) {
            return Err(Status::failed_precondition(format!(
                "recall task {} archive_id does not match current object metadata",
                task.id
            )));
        }

        let filemark = u32::try_from(task.tape_block_offset)
            .map_err(|_| Status::invalid_argument("recall tape offset does not fit filemark"))?;
        let data = tape
            .read_bundle(&task.tape_id, filemark, task.object_size)
            .await?;
        if data.len() as u64 != task.object_size {
            return Err(Status::data_loss(format!(
                "recall task {} read {} bytes, expected {}",
                task.id,
                data.len(),
                task.object_size
            )));
        }
        let checksum = sha256_hex(&data);
        if checksum != task.checksum {
            return Err(Status::data_loss(format!(
                "recall task {} checksum mismatch",
                task.id
            )));
        }

        let expire_at = task.expire_at.unwrap_or_else(|| days_from_now(1));
        cache.put_restored(&object, data, expire_at).await?;

        task.status = common::RestoreStatus::RestoreCompleted as i32;
        task.completed_at = Some(now_timestamp());
        task.error = None;
        self.update_recall_task(task.clone()).await?;
        self.update_object_restore_status(
            &task.bucket,
            &task.key,
            common::RestoreStatus::RestoreCompleted,
            Some(expire_at),
        )
        .await?;

        Ok(task.object_size)
    }
}

pub(crate) fn phase1_recall_task_id(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    match version_id.filter(|version| !version.is_empty()) {
        Some(version) => format!("phase1-recall:{bucket}/{key}#{version}"),
        None => format!("phase1-recall:{bucket}/{key}"),
    }
}

pub(crate) async fn run_recall_agent(state: Arc<SchedulerState>, agent_id: u64) {
    let every = Duration::from_secs(state.config.recall.scan_interval_secs.max(1));
    let max_batch = state.config.recall.max_concurrent_restores.max(1);
    let mut ticker = interval(every);
    info!(
        agent_id,
        "scheduler recall background loop enabled: interval={}s max_concurrent_restores={}",
        every.as_secs(),
        state.config.recall.max_concurrent_restores
    );

    loop {
        ticker.tick().await;
        let _permit = state
            .recall_slots
            .clone()
            .acquire_owned()
            .await
            .expect("recall concurrency slot should be available");
        match recall_pending_once(state.clone(), max_batch).await {
            Ok(result) if result.restored_objects > 0 => {
                info!(
                    restored_objects = result.restored_objects,
                    bytes_read = result.bytes_read,
                    "scheduler recall loop restored objects"
                );
            }
            Ok(_) => debug!("scheduler recall loop found no pending recall tasks"),
            Err(status) => error!(
                code = ?status.code(),
                message = status.message(),
                "scheduler recall loop failed"
            ),
        }
    }
}

pub async fn recall_pending_once(
    state: Arc<SchedulerState>,
    max_tasks: usize,
) -> std::result::Result<RecallBatchResult, Status> {
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
    let cache = CacheRestoreClient::new(cache);
    let tape = TapeRecallClient::new(tape, state.config.recall.drive_id.clone());
    backend
        .recall_pending_batch(
            &state,
            &cache,
            &tape,
            max_tasks.min(state.config.recall.max_concurrent_restores),
        )
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::*;
    #[tokio::test]
    async fn claim_recall_task_transitions_waiting_for_media_to_in_progress() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let mut metadata = state.metadata.clone();
        let backend = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");
        let body = b"restore-media".to_vec();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "media.txt",
            &body,
            common::StorageClass::Cold,
            None,
        )
        .await;
        metadata
            .update_archive_location(Request::new(
                coldstore_proto::metadata::UpdateArchiveLocationRequest {
                    bucket: "docs".into(),
                    key: "media.txt".into(),
                    archive_id: "archive-waiting".into(),
                    tape_id: "TAPE-WAIT".into(),
                    tape_set: vec!["TAPE-WAIT".into()],
                    tape_block_offset: 11,
                },
            ))
            .await
            .expect("set archive location");

        let task = common::RecallTask {
            id: "media-recall-task".into(),
            bucket: "docs".into(),
            key: "media.txt".into(),
            version_id: None,
            archive_id: "archive-waiting".into(),
            tape_id: "TAPE-WAIT".into(),
            tape_set: vec!["TAPE-WAIT".into()],
            tape_block_offset: 11,
            object_size: body.len() as u64,
            checksum: sha256_hex(&body),
            tier: common::RestoreTier::Standard as i32,
            days: 1,
            expire_at: Some(days_from_now(1)),
            status: common::RestoreStatus::RestoreWaitingForMedia as i32,
            drive_id: None,
            retry_count: 0,
            created_at: Some(Timestamp {
                seconds: 10,
                nanos: 0,
            }),
            started_at: None,
            completed_at: None,
            error: None,
        };
        metadata
            .put_recall_task(Request::new(task.clone()))
            .await
            .expect("seed waiting recall task");

        let mut claimed_task = task;
        assert!(backend
            .claim_recall_task(&mut claimed_task)
            .await
            .expect("claim recall task"));
        assert_eq!(
            claimed_task.status,
            common::RestoreStatus::RestoreInProgress as i32
        );
        assert!(claimed_task.started_at.is_some());

        let current_task = metadata
            .get_recall_task(Request::new(
                coldstore_proto::metadata::GetRecallTaskRequest {
                    id: "media-recall-task".into(),
                },
            ))
            .await
            .expect("lookup updated recall task")
            .into_inner();
        assert_eq!(
            current_task.status,
            common::RestoreStatus::RestoreInProgress as i32
        );

        let object = backend
            .head_object("docs", "media.txt")
            .await
            .expect("head restored object");
        assert_eq!(
            object.restore_status,
            Some(common::RestoreStatus::RestoreInProgress as i32)
        );

        let pending = metadata
            .list_pending_recall_tasks(Request::new(()))
            .await
            .expect("list pending recall tasks")
            .into_inner()
            .tasks;
        assert!(pending.is_empty());

        let mut second_claim = claimed_task;
        assert!(!backend
            .claim_recall_task(&mut second_claim)
            .await
            .expect("re-claim should be rejected"));

        metadata_shutdown_tx.send(()).ok();
    }

    #[tokio::test]
    async fn concurrent_claim_recall_task_has_single_winner() {
        let (_svc, state, metadata_shutdown_tx) = metadata_backed_service().await;
        let mut metadata = state.metadata.clone();

        let backend1 = MetadataBackedSchedulerBackend::new(state.metadata.clone());
        let backend2 = MetadataBackedSchedulerBackend::new(state.metadata.clone());

        backend1
            .create_bucket("docs")
            .await
            .expect("create bucket through metadata backend");

        let body = b"parallel-restore".to_vec();
        seed_object_metadata(
            &mut metadata,
            "docs",
            "parallel.txt",
            &body,
            common::StorageClass::Cold,
            None,
        )
        .await;
        metadata
            .update_archive_location(Request::new(
                coldstore_proto::metadata::UpdateArchiveLocationRequest {
                    bucket: "docs".into(),
                    key: "parallel.txt".into(),
                    archive_id: "archive-parallel".into(),
                    tape_id: "TAPE-PARALLEL".into(),
                    tape_set: vec!["TAPE-PARALLEL".into()],
                    tape_block_offset: 9,
                },
            ))
            .await
            .expect("set archive location");

        let task = common::RecallTask {
            id: "parallel-recall-task".into(),
            bucket: "docs".into(),
            key: "parallel.txt".into(),
            version_id: None,
            archive_id: "archive-parallel".into(),
            tape_id: "TAPE-PARALLEL".into(),
            tape_set: vec!["TAPE-PARALLEL".into()],
            tape_block_offset: 9,
            object_size: body.len() as u64,
            checksum: sha256_hex(&body),
            tier: common::RestoreTier::Standard as i32,
            days: 1,
            expire_at: Some(days_from_now(1)),
            status: common::RestoreStatus::RestorePending as i32,
            drive_id: None,
            retry_count: 0,
            created_at: Some(Timestamp {
                seconds: 10,
                nanos: 0,
            }),
            started_at: None,
            completed_at: None,
            error: None,
        };
        metadata
            .put_recall_task(Request::new(task.clone()))
            .await
            .expect("seed pending recall task");

        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
        let mut first = task.clone();
        let mut second = task.clone();

        let barrier_a = barrier.clone();
        let barrier_b = barrier.clone();

        let winner_a = tokio::spawn(async move {
            barrier_a.wait().await;
            let started_at = {
                let success = backend1.claim_recall_task(&mut first).await?;
                (success, first.started_at)
            };
            Ok::<(bool, Option<Timestamp>), tonic::Status>(started_at)
        });

        let winner_b = tokio::spawn(async move {
            barrier_b.wait().await;
            let started_at = {
                let success = backend2.claim_recall_task(&mut second).await?;
                (success, second.started_at)
            };
            Ok::<(bool, Option<Timestamp>), tonic::Status>(started_at)
        });

        let (result_a, result_b) = tokio::join!(winner_a, winner_b);
        let result_a = result_a
            .expect("agent a spawn joined")
            .expect("agent a claim");
        let result_b = result_b
            .expect("agent b spawn joined")
            .expect("agent b claim");

        let winner_count = (result_a.0 as u8) + (result_b.0 as u8);
        assert_eq!(
            winner_count, 1,
            "only one agent can claim the same recall task"
        );

        let started_count = [result_a.1.is_some(), result_b.1.is_some()]
            .iter()
            .filter(|started| **started)
            .count();
        assert_eq!(started_count, 1, "only one claim should set started_at");

        let current_task = metadata
            .get_recall_task(Request::new(
                coldstore_proto::metadata::GetRecallTaskRequest {
                    id: "parallel-recall-task".into(),
                },
            ))
            .await
            .expect("lookup current recall task")
            .into_inner();
        assert_eq!(
            current_task.status,
            common::RestoreStatus::RestoreInProgress as i32
        );

        metadata_shutdown_tx.send(()).ok();
    }
}
