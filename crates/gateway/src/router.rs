use crate::s3::bucket::{create_bucket, delete_bucket, head_bucket, list_buckets};
use crate::s3::list::list_objects;
use crate::s3::object::{delete_object, get_object, head_object, put_object};
use crate::s3::restore::post_object;
use crate::GatewayState;
use axum::routing::get;
use axum::Router;
use std::sync::Arc;

pub fn router(state: Arc<GatewayState>) -> Router {
    build_router().with_state(state)
}

fn build_router() -> Router<Arc<GatewayState>> {
    Router::new()
        .route("/health", get(health))
        .route("/", get(list_buckets))
        .route(
            "/:bucket",
            get(list_objects)
                .put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket),
        )
        .route(
            "/:bucket/*key",
            get(get_object)
                .put(put_object)
                .delete(delete_object)
                .head(head_object)
                .post(post_object),
        )
}

#[cfg(test)]
pub(crate) fn test_router(state: Arc<GatewayState>) -> Router {
    build_router().with_state(state)
}

async fn health() -> &'static str {
    "OK"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::testing::*;

    #[tokio::test]
    async fn health_route_returns_ok() {
        let response = test_router(state())
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
