use crate::protocol::{S3ErrorCode, S3ErrorResponse};
use axum::body::Body;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::Response;

pub(crate) fn bad_request_response(operation: &str, resource: &str) -> Response {
    bad_request_response_with_code(
        S3ErrorCode::NotImplemented,
        &format!("unsupported POST action for {operation}"),
        resource,
    )
}

pub(crate) fn bad_request_response_with_code(
    code: S3ErrorCode,
    message: &str,
    resource: &str,
) -> Response {
    let body = S3ErrorResponse {
        code,
        message,
        resource,
    }
    .to_xml();
    s3_xml_response(StatusCode::BAD_REQUEST, body)
}

pub(crate) fn grpc_status_to_s3_response(status: tonic::Status, resource: &str) -> Response {
    let status_message = status.message();
    let (code, http_status, retry_after) = match status.code() {
        tonic::Code::AlreadyExists => (
            S3ErrorCode::RestoreAlreadyInProgress,
            StatusCode::CONFLICT,
            None,
        ),
        tonic::Code::NotFound => {
            let code = if resource.matches('/').count() > 1 {
                S3ErrorCode::NoSuchKey
            } else {
                S3ErrorCode::NoSuchBucket
            };
            (code, StatusCode::NOT_FOUND, None)
        }
        tonic::Code::InvalidArgument => {
            (S3ErrorCode::InvalidArgument, StatusCode::BAD_REQUEST, None)
        }
        tonic::Code::FailedPrecondition => {
            (S3ErrorCode::InvalidObjectState, StatusCode::FORBIDDEN, None)
        }
        tonic::Code::Unavailable
            if status_message
                .to_lowercase()
                .contains("glacier expedited retrieval is not available") =>
        {
            (
                S3ErrorCode::GlacierExpeditedRetrievalNotAvailable,
                StatusCode::SERVICE_UNAVAILABLE,
                Some(1),
            )
        }
        tonic::Code::Unavailable => (
            S3ErrorCode::ServiceUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
            Some(1),
        ),
        tonic::Code::ResourceExhausted => match cache_reject_status_hint(status_message) {
            Some(CacheRejectStatus::Backpressure) => (
                S3ErrorCode::SlowDown,
                StatusCode::SERVICE_UNAVAILABLE,
                Some(1),
            ),
            Some(CacheRejectStatus::RateLimited) => (
                S3ErrorCode::SlowDown,
                StatusCode::SERVICE_UNAVAILABLE,
                Some(1),
            ),
            Some(CacheRejectStatus::InvalidRequest) => {
                (S3ErrorCode::InvalidArgument, StatusCode::BAD_REQUEST, None)
            }
            None => (
                S3ErrorCode::SlowDown,
                StatusCode::SERVICE_UNAVAILABLE,
                Some(1),
            ),
        },
        tonic::Code::Unimplemented => (
            S3ErrorCode::NotImplemented,
            StatusCode::NOT_IMPLEMENTED,
            None,
        ),
        _ => (S3ErrorCode::NotImplemented, StatusCode::BAD_GATEWAY, None),
    };
    let mut response = {
        let body = S3ErrorResponse {
            code,
            message: status.message(),
            resource,
        }
        .to_xml();
        s3_xml_response(http_status, body)
    };

    if let Some(seconds) = retry_after {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&seconds.to_string())
                .unwrap_or_else(|_| HeaderValue::from_static("1")),
        );
    }

    response
}

enum CacheRejectStatus {
    Backpressure,
    RateLimited,
    InvalidRequest,
}

/// Classify a `capacity_reject:<reason>:<detail>` hint. AGENTS.md §6.5 makes this one half of a
/// string contract: the emitting side is `capacity_reject_error` in `crates/cache/src/service.rs`,
/// so a new reason code must be added to both sides. The function is module-private on purpose — the
/// contract lives in the literals below, not in Rust visibility.
fn cache_reject_status_hint(message: &str) -> Option<CacheRejectStatus> {
    let message = message.trim();
    if !message.starts_with("capacity_reject:") {
        return None;
    }
    let reason = message["capacity_reject:".len()..]
        .split(':')
        .next()
        .unwrap_or_default();
    match reason {
        "incoming_larger_than_capacity" => Some(CacheRejectStatus::InvalidRequest),
        "client_rate_limited" | "concurrency_limit_exceeded" | "request_rate_exceeded" => {
            Some(CacheRejectStatus::RateLimited)
        }
        "staging_budget_exceeded"
        | "restored_budget_exceeded"
        | "global_capacity_exceeded"
        | "low_watermark_exceeded"
        | "no_eviction_candidate"
        | "zero_capacity" => Some(CacheRejectStatus::Backpressure),
        _ => None,
    }
}

pub(crate) fn s3_xml_response(status: StatusCode, body: String) -> Response {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

pub(crate) fn xml_response(status: StatusCode, body: String) -> Response {
    s3_xml_response(status, body)
}

pub(crate) fn empty_response(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn failed_precondition_maps_to_invalid_object_state() {
        let response = grpc_status_to_s3_response(
            tonic::Status::failed_precondition("object must be restored"),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>InvalidObjectState</Code>"));
    }
    #[tokio::test]
    async fn resource_exhausted_maps_to_s3_slow_down_with_retry_after() {
        let response = grpc_status_to_s3_response(
            tonic::Status::resource_exhausted("cache quota exceeded"),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>SlowDown</Code>"));
    }
    #[tokio::test]
    async fn resource_exhausted_maps_staging_budget_pressure_to_slow_down() {
        let response = grpc_status_to_s3_response(
            tonic::Status::resource_exhausted(
                "capacity_reject:staging_budget_exceeded: not enough staging cache budget",
            ),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>SlowDown</Code>"));
    }
    #[tokio::test]
    async fn unavailable_maps_to_glacier_expedited_retrieval_not_available() {
        let response = grpc_status_to_s3_response(
            tonic::Status::unavailable(
                "glacier expedited retrieval is not available in this environment",
            ),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>GlacierExpeditedRetrievalNotAvailable</Code>"));
    }
    #[tokio::test]
    async fn unavailable_maps_to_service_unavailable_with_retry_after() {
        let response = grpc_status_to_s3_response(
            tonic::Status::unavailable("scheduler down"),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>ServiceUnavailable</Code>"));
    }
    #[tokio::test]
    async fn rate_limited_resource_exhausted_maps_to_s3_slow_down_with_retry_after() {
        let response = grpc_status_to_s3_response(
            tonic::Status::resource_exhausted(
                "capacity_reject:concurrency_limit_exceeded: client throttled",
            ),
            "/docs/readme.txt",
        );
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Code>SlowDown</Code>"));
    }
    #[tokio::test]
    async fn s3_error_response_escapes_backend_message_and_resource() {
        let response = grpc_status_to_s3_response(
            tonic::Status::invalid_argument("bad <tag> & \"quote\""),
            "/docs/a&b<raw>.txt",
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("bad &lt;tag&gt; &amp; &quot;quote&quot;"));
        assert!(body.contains("/docs/a&amp;b&lt;raw&gt;.txt"));
    }
}
