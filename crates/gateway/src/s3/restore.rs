use crate::protocol::{is_restore_request, parse_restore_request, S3ErrorCode};
use crate::GatewayState;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use std::collections::HashMap;
use std::sync::Arc;

use crate::s3::errors::{
    bad_request_response, bad_request_response_with_code, empty_response,
    grpc_status_to_s3_response,
};

pub(crate) async fn post_object(
    State(state): State<Arc<GatewayState>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let raw_query = if query.is_empty() {
        None
    } else {
        Some(
            query
                .iter()
                .map(|(k, v)| {
                    if v.is_empty() {
                        k.clone()
                    } else {
                        format!("{k}={v}")
                    }
                })
                .collect::<Vec<_>>()
                .join("&"),
        )
    };
    let resource = format!("/{bucket}/{key}");
    if !is_restore_request(raw_query.as_deref()) {
        return bad_request_response("UnsupportedPostAction", &resource);
    }

    let restore_request = match parse_restore_request(&query, body.as_ref()) {
        Ok(request) => request,
        Err(_) => {
            return bad_request_response_with_code(
                S3ErrorCode::InvalidArgument,
                "invalid restore request payload",
                &resource,
            );
        }
    };

    match state
        .backend
        .restore_object(&bucket, &key, restore_request.days, restore_request.tier)
        .await
    {
        Ok(response) => {
            let status = match response.status_code {
                200 => StatusCode::OK,
                202 => StatusCode::ACCEPTED,
                409 => StatusCode::CONFLICT,
                _ => StatusCode::ACCEPTED,
            };
            empty_response(status)
        }
        Err(status) => grpc_status_to_s3_response(status, &resource),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::test_router;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn restore_post_route_uses_backend() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/docs/readme.txt?restore=true")
                    .body(Body::from(
                        "<RestoreRequest><Days>2</Days><GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters></RestoreRequest>",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }
    #[tokio::test]
    async fn restore_post_route_rejects_invalid_body() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/docs/readme.txt?restore=true")
                    .body(Body::from(
                        "<RestoreRequest><Days>0</Days><GlacierJobParameters><Tier>Bulk</Tier></GlacierJobParameters></RestoreRequest>",
                    ))
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
    async fn restore_post_route_returns_conflict_for_in_progress_restore() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/docs/pending.txt?restore=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CONFLICT);
    }
}
