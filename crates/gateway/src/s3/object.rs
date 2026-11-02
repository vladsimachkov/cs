use crate::protocol::S3ErrorCode;
use crate::GatewayState;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::Response;
use std::sync::Arc;

use crate::s3::errors::{
    bad_request_response_with_code, empty_response, grpc_status_to_s3_response,
};
use crate::s3::headers::apply_object_headers;
use crate::s3::preconditions::check_read_preconditions;
use crate::s3::range::parse_range_header;

pub(crate) async fn get_object(
    State(state): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let resource = format!("/{bucket}/{key}");

    if let Some(response) =
        match check_read_preconditions(&state, &bucket, &key, &headers, &resource).await {
            Ok(response) => response,
            Err(status) => return grpc_status_to_s3_response(status, &resource),
        }
    {
        return response;
    }

    let object = match state.backend.get_object(&bucket, &key).await {
        Ok(object) => object,
        Err(status) => return grpc_status_to_s3_response(status, &resource),
    };

    let object_size = object.body.len();
    let mut response_body = object.body;
    let mut status_code = StatusCode::OK;

    if let Some(range_value) = headers.get(header::RANGE) {
        let (start, end) = match range_value.to_str() {
            Ok(raw_range) => match parse_range_header(raw_range, object_size) {
                Ok(Some(range)) => range,
                Ok(None) => {
                    return bad_request_response_with_code(
                        S3ErrorCode::InvalidArgument,
                        "invalid Range header",
                        &resource,
                    )
                }
                Err(response) => return *response,
            },
            Err(_) => {
                return bad_request_response_with_code(
                    S3ErrorCode::InvalidArgument,
                    "invalid Range header",
                    &resource,
                )
            }
        };

        let range_response_length = end.saturating_sub(start).saturating_add(1);
        response_body = response_body[start..=end].to_vec();
        status_code = StatusCode::PARTIAL_CONTENT;

        let mut response = Response::new(Body::from(response_body));
        *response.status_mut() = status_code;
        apply_object_headers(response.headers_mut(), &object.head);
        response.headers_mut().insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&range_response_length.to_string()).unwrap(),
        );
        response
            .headers_mut()
            .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {start}-{end}/{object_size}"))
                .unwrap_or_else(|_| HeaderValue::from_static("bytes */*")),
        );
        return response;
    }

    let mut response = Response::new(Body::from(response_body));
    *response.status_mut() = status_code;
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    apply_object_headers(response.headers_mut(), &object.head);
    response
}

fn put_object_success_response(
    put_response: coldstore_proto::scheduler::PutObjectResponse,
) -> Response {
    let mut response = empty_response(StatusCode::OK);
    response.headers_mut().insert(
        axum::http::header::ETAG,
        HeaderValue::from_str(&put_response.etag).unwrap(),
    );
    response
}

fn head_object_success_response(head: coldstore_proto::scheduler::HeadObjectResponse) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    apply_object_headers(response.headers_mut(), &head);
    response
}

pub(crate) async fn put_object(
    State(state): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let content_type = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    match state
        .backend
        .put_object(&bucket, &key, body.to_vec(), content_type)
        .await
    {
        Ok(response) => put_object_success_response(response),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}/{key}")),
    }
}

pub(crate) async fn delete_object(
    State(state): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
) -> Response {
    match state.backend.delete_object(&bucket, &key).await {
        Ok(()) => empty_response(StatusCode::NO_CONTENT),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}/{key}")),
    }
}

pub(crate) async fn head_object(
    State(state): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let resource = format!("/{bucket}/{key}");

    if let Some(response) =
        match check_read_preconditions(&state, &bucket, &key, &headers, &resource).await {
            Ok(response) => response,
            Err(status) => return grpc_status_to_s3_response(status, &resource),
        }
    {
        return response;
    }

    match state.backend.head_object(&bucket, &key).await {
        Ok(head) => head_object_success_response(head),
        Err(status) => grpc_status_to_s3_response(status, &resource),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::test_router;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn put_object_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/docs/readme.txt")
                    .header("content-type", "text/plain")
                    .body(Body::from("hello world"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["etag"], "etag-put");
    }
    #[tokio::test]
    async fn get_object_route_returns_body_from_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/readme.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"hello world");
    }
    #[tokio::test]
    async fn get_object_route_supports_single_range() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/readme.txt")
                    .header("range", "bytes=1-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"ell");
    }
    #[tokio::test]
    async fn get_object_route_range_on_large_offsets() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/readme.txt")
                    .header("range", "bytes=-3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), b"rld");
    }
    #[tokio::test]
    async fn get_object_route_range_on_empty_object_is_not_satisfiable() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/empty.txt")
                    .header("range", "bytes=0-0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .unwrap(),
            "bytes */0"
        );
    }
    #[tokio::test]
    async fn head_object_route_sets_restore_header() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/docs/readme.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["etag"], "etag-1");
        assert_eq!(
            response.headers()["x-amz-restore"],
            "ongoing-request=\"false\", expiry-date=\"123\""
        );
    }
    #[tokio::test]
    async fn delete_object_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/docs/readme.txt")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    #[tokio::test]
    async fn get_with_if_none_match_returns_not_modified() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/readme.txt")
                    .header("if-none-match", "etag-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }
    #[tokio::test]
    async fn get_with_if_match_returns_not_precondition_failed() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/docs/readme.txt")
                    .header("if-match", "etag-other")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
    }
}
