use crate::protocol::{S3ErrorCode, S3ErrorResponse};
use crate::GatewayState;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use chrono::DateTime;
use std::sync::Arc;

use crate::s3::errors::{empty_response, s3_xml_response};

pub(crate) async fn check_read_preconditions(
    state: &Arc<GatewayState>,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
    resource: &str,
) -> std::result::Result<Option<Response>, tonic::Status> {
    let if_match = headers
        .get(header::IF_MATCH)
        .and_then(|value| value.to_str().ok());
    let if_none_match = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok());
    let if_modified_since = match headers
        .get(header::IF_MODIFIED_SINCE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => parse_http_datetime(value)
            .map_err(|_| tonic::Status::invalid_argument("invalid If-Modified-Since header"))?,
        None => None,
    };

    let if_unmodified_since = match headers
        .get(header::IF_UNMODIFIED_SINCE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) => parse_http_datetime(value)
            .map_err(|_| tonic::Status::invalid_argument("invalid If-Unmodified-Since header"))?,
        None => None,
    };

    if if_match.is_none()
        && if_none_match.is_none()
        && if_modified_since.is_none()
        && if_unmodified_since.is_none()
    {
        return Ok(None);
    }

    let head = state.backend.head_object(bucket, key).await?;
    let object_etag = head.etag.clone();
    let object_modified_at = head
        .last_modified
        .as_ref()
        .map(timestamp_from_prost_timestamp);

    if let Some(value) = if_match {
        if !etag_header_matches(value, &object_etag) {
            return Ok(Some(precondition_failed_response("If-Match", resource)));
        }
    }

    if let Some(unmodified_since) = if_unmodified_since {
        let Some(object_modified_at) = object_modified_at else {
            return Err(tonic::Status::failed_precondition(
                "If-Unmodified-Since requires Last-Modified",
            ));
        };

        if object_modified_at > unmodified_since {
            return Ok(Some(precondition_failed_response(
                "If-Unmodified-Since failed",
                resource,
            )));
        }
    }

    if let Some(value) = if_none_match {
        if etag_header_matches(value, &object_etag) {
            return Ok(Some(not_modified_response(resource, &object_etag)));
        }
    }

    if let Some(modified_since) = if_modified_since {
        let Some(object_modified_at) = object_modified_at else {
            return Err(tonic::Status::failed_precondition(
                "If-Modified-Since requires Last-Modified",
            ));
        };

        if object_modified_at <= modified_since {
            return Ok(Some(not_modified_response(resource, &object_etag)));
        }
    }

    Ok(None)
}

fn parse_http_datetime(value: &str) -> std::result::Result<Option<i64>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }

    const RFC_1123_HTTP_DATE: &str = "%a, %d %b %Y %H:%M:%S GMT";
    const RFC_850_HTTP_DATE: &str = "%A, %d-%b-%y %H:%M:%S GMT";
    const ANSI_C_DATE: &str = "%a %b %e %H:%M:%S %Y";

    for format in [RFC_1123_HTTP_DATE, RFC_850_HTTP_DATE, ANSI_C_DATE] {
        if let Ok(time) = DateTime::parse_from_str(value, format) {
            return Ok(Some(time.timestamp()));
        }
    }

    DateTime::parse_from_rfc2822(value)
        .map(|time| Some(time.timestamp()))
        .map_err(|_| "unable to parse HTTP date".into())
}

fn timestamp_from_prost_timestamp(timestamp: &prost_types::Timestamp) -> i64 {
    timestamp.seconds
}

fn etag_header_matches(header_value: &str, etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .map(|token| token.trim_matches('"'))
        .any(|token| token == "*" || token == etag || token == format!("W/\"{etag}\""))
}

fn not_modified_response(resource: &str, etag: &str) -> Response {
    let mut response = empty_response(StatusCode::NOT_MODIFIED);
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(etag).unwrap_or_else(|_| HeaderValue::from_static("\"\"")),
    );
    response.headers_mut().insert(
        HeaderName::from_static("x-amz-request-id"),
        HeaderValue::from_str(resource).unwrap_or_else(|_| HeaderValue::from_static("-")),
    );
    response
}

fn precondition_failed_response(message: &str, resource: &str) -> Response {
    let body = S3ErrorResponse {
        code: S3ErrorCode::PreconditionFailed,
        message,
        resource,
    }
    .to_xml();
    s3_xml_response(StatusCode::PRECONDITION_FAILED, body)
}
