//! Item/task locks, the per-tape lease, the recall tier ordering key, and the `capacity_reject:`
//! hint parsing.
//!
//! AR-00 moved these out of `service.rs` with no behavior change. The lock and lease helpers are
//! three of the four lock families AGENTS.md §6.4 names (the concurrency slots stay in `service.rs`);
//! the parsing helpers are one half of the §6.5 string contract.

use crate::clients::tape::TapeLease;
use crate::SchedulerState;
use coldstore_proto::common;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{Code, Status};

pub(crate) fn phase1_archive_item_key(bucket: &str, key: &str, version_id: Option<&str>) -> String {
    match version_id.filter(|version| !version.is_empty()) {
        Some(version) => format!("{bucket}/{key}#{version}"),
        None => format!("{bucket}/{key}"),
    }
}

pub(crate) fn phase1_recall_task_key(task: &common::RecallTask) -> String {
    task.id.clone()
}

/// AGENTS.md §6.5 makes this one half of a string contract: the emitting side is
/// `capacity_reject_error` in `crates/cache/src/service.rs`, and the gateway half is
/// `cache_reject_status_hint` in `crates/gateway/src/s3/errors.rs`. A new reason code must be
/// considered in all three — the list below is the retriable subset, so some codes are handled only
/// by the gateway half. Private on purpose: the contract lives in the literals, not in Rust
/// visibility.
const CAPACITY_REJECT_PREFIX: &str = "capacity_reject:";

fn capacity_reject_reason(message: &str) -> Option<&str> {
    let message = message.trim();
    if !message.starts_with(CAPACITY_REJECT_PREFIX) {
        return None;
    }
    let parts: Vec<&str> = message[CAPACITY_REJECT_PREFIX.len()..].split(':').collect();
    parts.first().cloned().filter(|reason| !reason.is_empty())
}

pub(crate) fn is_retriable_recall_processing_error(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable => true,
        Code::ResourceExhausted => {
            matches!(
                capacity_reject_reason(status.message()),
                Some("staging_budget_exceeded")
                    | Some("restored_budget_exceeded")
                    | Some("global_capacity_exceeded")
                    | Some("low_watermark_exceeded")
                    | Some("no_eviction_candidate")
            )
        }
        _ => false,
    }
}

pub(crate) async fn acquire_archive_item(state: &SchedulerState, key: &str) -> bool {
    let mut lock = state.active_archive_keys.lock().await;
    if lock.contains(key) {
        return false;
    }
    lock.insert(key.to_string());
    true
}

pub(crate) async fn release_archive_item(state: &SchedulerState, key: &str) {
    let mut lock = state.active_archive_keys.lock().await;
    lock.remove(key);
}

pub(crate) async fn acquire_recall_task(state: &SchedulerState, key: &str) -> bool {
    let mut lock = state.active_recall_tasks.lock().await;
    if lock.contains(key) {
        return false;
    }
    lock.insert(key.to_string());
    true
}

pub(crate) async fn release_recall_task(state: &SchedulerState, key: &str) {
    let mut lock = state.active_recall_tasks.lock().await;
    lock.remove(key);
}

pub(crate) async fn acquire_tape_lease(
    state: &SchedulerState,
    tape_id: &str,
) -> std::result::Result<TapeLease, Status> {
    let lock = state.tape_locks.clone();
    let tape_lock = {
        let mut map = lock.lock().await;
        map.entry(tape_id.to_string())
            .or_insert_with(|| Arc::new(Semaphore::new(1)))
            .clone()
    };

    let permit = tape_lock
        .acquire_owned()
        .await
        .map_err(|_| Status::internal("tape lease manager closed"))?;
    Ok(TapeLease {
        _permit: permit,
        _tape_id: tape_id.to_string(),
    })
}

pub(crate) fn restore_tier_rank(tier: i32) -> u8 {
    match common::RestoreTier::try_from(tier) {
        Ok(common::RestoreTier::Expedited) => 0,
        Ok(common::RestoreTier::Standard) => 1,
        Ok(common::RestoreTier::Bulk) => 2,
        _ => 3,
    }
}
