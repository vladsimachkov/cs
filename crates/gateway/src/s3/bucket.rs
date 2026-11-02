use crate::GatewayState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Response;
use std::sync::Arc;

use crate::s3::errors::{empty_response, grpc_status_to_s3_response};
use crate::s3::list::list_buckets_xml_response;

pub(crate) async fn list_buckets(State(state): State<Arc<GatewayState>>) -> Response {
    match state.backend.list_buckets().await {
        Ok(response) => list_buckets_xml_response(&response),
        Err(status) => grpc_status_to_s3_response(status, "/"),
    }
}

pub(crate) async fn create_bucket(
    State(state): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
) -> Response {
    match state.backend.create_bucket(&bucket).await {
        Ok(()) => empty_response(StatusCode::OK),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}")),
    }
}

pub(crate) async fn delete_bucket(
    State(state): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
) -> Response {
    match state.backend.delete_bucket(&bucket).await {
        Ok(()) => empty_response(StatusCode::NO_CONTENT),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}")),
    }
}

pub(crate) async fn head_bucket(
    State(state): State<Arc<GatewayState>>,
    Path(bucket): Path<String>,
) -> Response {
    match state.backend.head_bucket(&bucket).await {
        Ok(()) => empty_response(StatusCode::OK),
        Err(status) => grpc_status_to_s3_response(status, &format!("/{bucket}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::test_router;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn list_buckets_route_returns_xml_from_backend() {
        let response = test_router(state())
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8(body.to_vec())
            .unwrap()
            .contains("<Name>docs</Name>"));
    }
    #[tokio::test]
    async fn create_bucket_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/new-bucket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    #[tokio::test]
    async fn delete_bucket_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/docs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
    #[tokio::test]
    async fn head_bucket_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/docs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
