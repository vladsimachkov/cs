use crate::protocol::S3ErrorCode;
use crate::GatewayState;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use std::collections::HashMap;
use std::sync::Arc;

use crate::s3::errors::{bad_request_response_with_code, grpc_status_to_s3_response, xml_response};
use crate::s3::xml::escape_xml;

const DEFAULT_LIST_MAX_KEYS: u32 = 1000;
const MAX_LIST_MAX_KEYS: u32 = 1000;

pub(crate) async fn list_objects(
    State(state): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let prefix = query.get("prefix").map(String::as_str);
    let marker = query.get("marker").map(String::as_str);
    let delimiter = query.get("delimiter").map(String::as_str);
    let max_keys = match parse_list_max_keys(query.get("max-keys").map(String::as_str)) {
        Ok(max_keys) => max_keys,
        Err(response) => return *response,
    };

    match state
        .backend
        .list_objects(&bucket, prefix, marker, delimiter, max_keys)
        .await
    {
        Ok(response) => list_objects_xml_response(&response),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}")),
    }
}

fn parse_list_max_keys(value: Option<&str>) -> std::result::Result<u32, Box<Response>> {
    let raw = value.unwrap_or("");
    let max_keys = if raw.is_empty() {
        DEFAULT_LIST_MAX_KEYS
    } else {
        raw.parse::<u32>().map_err(|_| {
            Box::new(bad_request_response_with_code(
                S3ErrorCode::InvalidArgument,
                "max-keys must be an integer between 1 and 1000",
                "/",
            ))
        })?
    };

    if !(1..=MAX_LIST_MAX_KEYS).contains(&max_keys) {
        return Err(Box::new(bad_request_response_with_code(
            S3ErrorCode::InvalidArgument,
            "max-keys must be an integer between 1 and 1000",
            "/",
        )));
    }

    Ok(max_keys)
}

pub(crate) fn list_buckets_xml_response(
    response: &coldstore_proto::scheduler::ListBucketsResponse,
) -> Response {
    let buckets_xml = response
        .buckets
        .iter()
        .map(|bucket| format!("<Bucket><Name>{}</Name></Bucket>", escape_xml(&bucket.name)))
        .collect::<Vec<_>>()
        .join("");
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Buckets>{}</Buckets></ListAllMyBucketsResult>",
        buckets_xml
    );
    xml_response(StatusCode::OK, body)
}

fn list_objects_xml_response(
    response: &coldstore_proto::scheduler::ListObjectsResponse,
) -> Response {
    let mut contents = String::new();
    for entry in &response.contents {
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><ETag>{}</ETag><Size>{}</Size><StorageClass>{}</StorageClass></Contents>",
            escape_xml(&entry.key),
            escape_xml(&entry.etag),
            entry.size,
            escape_xml(&entry.storage_class)
        ));
    }

    let mut common_prefixes = String::new();
    for common_prefix in &response.common_prefixes {
        common_prefixes.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            escape_xml(&common_prefix.prefix)
        ));
    }

    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>{}</Name><Prefix>{}</Prefix><Marker>{}</Marker><NextMarker>{}</NextMarker><MaxKeys>{}</MaxKeys><IsTruncated>{}</IsTruncated>{}{}</ListBucketResult>",
        escape_xml(&response.bucket),
        escape_xml(&response.prefix.clone().unwrap_or_default()),
        escape_xml(&response.marker.clone().unwrap_or_default()),
        escape_xml(&response.next_marker.clone().unwrap_or_default()),
        response.max_keys,
        response.is_truncated,
        contents,
        common_prefixes,
    );
    xml_response(StatusCode::OK, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::test_router;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn list_objects_route_returns_xml_from_backend() {
        let response = test_router(state())
            .oneshot(Request::builder().uri("/docs").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("<ListBucketResult>"));
        assert!(text.contains("<Key>readme.txt</Key>"));
    }
    #[tokio::test]
    async fn list_objects_route_rejects_invalid_max_keys() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs?max-keys=abc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>InvalidArgument</Code>"));
    }
    #[tokio::test]
    async fn list_objects_route_rejects_invalid_max_keys_range() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs?max-keys=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
}
