//! Test doubles shared by the `s3` operation modules.
//!
//! GW-00 moved these out of the single `mod tests` block in `handler.rs`: one mock backend and one
//! `state()` helper, now shared by the per-module test blocks instead of duplicated in each.

use crate::{DownloadedObject, GatewayBackend, GatewayState};

pub(crate) use axum::body::to_bytes;
pub(crate) use axum::body::Body;
pub(crate) use axum::http::{Request, StatusCode};
use coldstore_proto::scheduler::{
    BucketEntry, HeadObjectResponse, ListBucketsResponse, ListObjectsResponse, ObjectEntry,
    PutObjectResponse, RestoreObjectResponse,
};
use std::sync::Arc;

pub(crate) use tower::util::ServiceExt;

pub(crate) struct MockGatewayBackend;

#[tonic::async_trait]
impl GatewayBackend for MockGatewayBackend {
    async fn list_buckets(&self) -> std::result::Result<ListBucketsResponse, tonic::Status> {
        Ok(ListBucketsResponse {
            buckets: vec![BucketEntry {
                name: "docs".into(),
                creation_date: None,
            }],
        })
    }

    async fn create_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        if bucket == "existing" {
            Err(tonic::Status::already_exists("bucket already exists"))
        } else {
            Ok(())
        }
    }

    async fn delete_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        if bucket == "docs" {
            Ok(())
        } else {
            Err(tonic::Status::not_found("bucket missing"))
        }
    }

    async fn head_bucket(&self, bucket: &str) -> std::result::Result<(), tonic::Status> {
        if bucket == "docs" {
            Ok(())
        } else {
            Err(tonic::Status::not_found("bucket missing"))
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        _prefix: Option<&str>,
        _marker: Option<&str>,
        _delimiter: Option<&str>,
        _max_keys: u32,
    ) -> std::result::Result<ListObjectsResponse, tonic::Status> {
        if bucket != "docs" {
            return Err(tonic::Status::not_found("bucket missing"));
        }
        Ok(ListObjectsResponse {
            bucket: "docs".into(),
            prefix: None,
            marker: None,
            next_marker: None,
            max_keys: 1000,
            is_truncated: false,
            contents: vec![ObjectEntry {
                key: "readme.txt".into(),
                last_modified: None,
                etag: "etag-1".into(),
                size: 42,
                storage_class: "COLD".into(),
            }],
            common_prefixes: vec![],
        })
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _body: Vec<u8>,
        _content_type: Option<String>,
    ) -> std::result::Result<PutObjectResponse, tonic::Status> {
        Ok(PutObjectResponse {
            etag: "etag-put".into(),
            version_id: "v1".into(),
        })
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<DownloadedObject, tonic::Status> {
        Ok(DownloadedObject {
            head: self.head_object(bucket, key).await?,
            body: if key == "empty.txt" {
                Vec::new()
            } else {
                b"hello world".to_vec()
            },
        })
    }

    async fn delete_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<(), tonic::Status> {
        if bucket == "docs" && key == "readme.txt" {
            Ok(())
        } else {
            Err(tonic::Status::not_found("object missing"))
        }
    }

    async fn head_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<HeadObjectResponse, tonic::Status> {
        if bucket == "docs" && key == "readme.txt" {
            Ok(HeadObjectResponse {
                content_length: 42,
                content_type: Some("text/plain".into()),
                etag: "etag-1".into(),
                storage_class: 2,
                restore_info: Some("ongoing-request=\"false\", expiry-ts=\"123\"".into()),
                last_modified: None,
            })
        } else if bucket == "docs" && key == "empty.txt" {
            Ok(HeadObjectResponse {
                content_length: 0,
                content_type: Some("text/plain".into()),
                etag: "etag-empty".into(),
                storage_class: 2,
                restore_info: None,
                last_modified: None,
            })
        } else {
            Err(tonic::Status::not_found("object missing"))
        }
    }

    async fn restore_object(
        &self,
        bucket: &str,
        key: &str,
        _days: u32,
        _tier: coldstore_proto::common::RestoreTier,
    ) -> std::result::Result<RestoreObjectResponse, tonic::Status> {
        if bucket == "docs" && key == "readme.txt" {
            Ok(RestoreObjectResponse { status_code: 202 })
        } else if bucket == "docs" && key == "pending.txt" {
            Ok(RestoreObjectResponse { status_code: 409 })
        } else {
            Err(tonic::Status::not_found("object missing"))
        }
    }
}

pub(crate) fn state() -> Arc<GatewayState> {
    Arc::new(GatewayState {
        backend: Arc::new(MockGatewayBackend),
    })
}
