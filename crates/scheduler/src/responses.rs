//! Builders that turn stored metadata into gRPC response messages, the list-page DTO, and one
//! small predicate over recall-task status.
//!
//! AR-00 moved these out of `service.rs` with no behavior change.

use coldstore_proto::common;
use coldstore_proto::scheduler::{BucketEntry, GetObjectMeta, HeadObjectResponse, ObjectEntry};
use prost_types::Timestamp;

fn restore_expiry_http_date(expire_at: &Timestamp) -> String {
    use chrono::TimeZone;
    chrono::Utc
        .timestamp_opt(expire_at.seconds, expire_at.nanos as u32)
        .single()
        .map(|value| value.format("%a, %d %b %Y %H:%M:%S GMT").to_string())
        .unwrap_or_else(|| expire_at.seconds.to_string())
}

fn build_restore_info(
    restore_status: Option<i32>,
    restore_expire_at: Option<&Timestamp>,
) -> Option<String> {
    match restore_status.and_then(|status| common::RestoreStatus::try_from(status).ok()) {
        Some(common::RestoreStatus::RestoreInProgress)
        | Some(common::RestoreStatus::RestorePending)
        | Some(common::RestoreStatus::RestoreWaitingForMedia) => {
            Some("ongoing-request=\"true\"".into())
        }
        Some(common::RestoreStatus::RestoreCompleted) => {
            if let Some(expire_at) = restore_expire_at {
                Some(format!(
                    "ongoing-request=\"false\", expiry-date=\"{}\"",
                    restore_expiry_http_date(expire_at)
                ))
            } else {
                Some("ongoing-request=\"false\"".into())
            }
        }
        _ => None,
    }
}

pub(crate) fn build_head_object_response(object: &common::ObjectMetadata) -> HeadObjectResponse {
    HeadObjectResponse {
        content_length: object.size,
        content_type: object.content_type.clone(),
        etag: object.etag.clone().unwrap_or_default(),
        storage_class: object.storage_class,
        restore_info: build_restore_info(object.restore_status, object.restore_expire_at.as_ref()),
        last_modified: object.updated_at,
    }
}

pub(crate) fn build_get_object_meta(object: &common::ObjectMetadata) -> GetObjectMeta {
    GetObjectMeta {
        content_length: object.size,
        content_type: object.content_type.clone(),
        etag: object.etag.clone().unwrap_or_default(),
        storage_class: object.storage_class,
        restore_info: build_restore_info(object.restore_status, object.restore_expire_at.as_ref()),
        last_modified: object.updated_at,
    }
}

pub(crate) fn build_object_entry(object: &common::ObjectMetadata) -> ObjectEntry {
    ObjectEntry {
        key: object.key.clone(),
        last_modified: object.updated_at,
        etag: object.etag.clone().unwrap_or_default(),
        size: object.size,
        storage_class: storage_class_label(object.storage_class).into(),
    }
}

pub(crate) fn build_list_objects_page(
    bucket: &str,
    objects: Vec<common::ObjectMetadata>,
    prefix: Option<&str>,
    marker: Option<&str>,
    delimiter: Option<&str>,
    max_keys: u32,
) -> ListObjectsPage {
    let prefix = prefix.unwrap_or_default();
    let marker = marker.unwrap_or_default();
    let delimiter = delimiter.unwrap_or_default();
    let limit = if max_keys == 0 {
        usize::MAX
    } else {
        max_keys as usize
    };
    let enforce_limit = limit != usize::MAX;

    let mut filtered: Vec<_> = objects
        .into_iter()
        .filter(|object| object.bucket == bucket)
        .filter(|object| object.key.starts_with(prefix) && object.key.as_str() > marker)
        .collect();
    filtered.sort_by(|a, b| {
        a.key
            .cmp(&b.key)
            .then_with(|| a.version_id.cmp(&b.version_id))
    });

    let mut emitted = 0usize;
    let mut next_marker = None;
    let mut is_truncated = false;
    let mut last_emitted_key = None;

    let mut contents = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut prefix_set = std::collections::HashSet::new();
    for object in filtered {
        if emitted >= limit {
            is_truncated = true;
            break;
        }

        if !delimiter.is_empty() {
            if let Some(suffix) = object.key.strip_prefix(prefix) {
                if let Some(pos) = suffix.find(delimiter) {
                    let candidate_prefix = format!("{prefix}{}", &suffix[..pos + delimiter.len()]);
                    if prefix_set.insert(candidate_prefix.clone()) {
                        common_prefixes.push(candidate_prefix.clone());
                        emitted += 1;
                        last_emitted_key = Some(candidate_prefix);
                    }
                    continue;
                }
            }
        }

        contents.push(object);
        emitted += 1;
        if let Some(entry) = contents.last() {
            last_emitted_key = Some(entry.key.clone());
        }
    }

    if is_truncated && enforce_limit {
        next_marker = last_emitted_key;
        is_truncated = true;
    }

    ListObjectsPage {
        objects: contents,
        common_prefixes,
        next_marker,
        is_truncated,
    }
}

pub(crate) fn build_bucket_entry(bucket: &common::BucketInfo) -> BucketEntry {
    BucketEntry {
        name: bucket.name.clone(),
        creation_date: bucket.created_at,
    }
}

fn storage_class_label(storage_class: i32) -> &'static str {
    match common::StorageClass::try_from(storage_class).ok() {
        Some(common::StorageClass::ColdPending) => "COLD_PENDING",
        Some(common::StorageClass::Cold) => "COLD",
        _ => "UNKNOWN",
    }
}

pub(crate) fn is_pending_recall_task(task: &common::RecallTask) -> bool {
    matches!(
        common::RestoreStatus::try_from(task.status),
        Ok(common::RestoreStatus::RestorePending)
            | Ok(common::RestoreStatus::RestoreWaitingForMedia)
    )
}

#[derive(Debug, Clone, Default)]
pub struct ListObjectsPage {
    pub(crate) objects: Vec<common::ObjectMetadata>,
    pub(crate) common_prefixes: Vec<String>,
    pub(crate) next_marker: Option<String>,
    pub(crate) is_truncated: bool,
}
