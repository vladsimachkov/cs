use crate::protocol::format_restore_header;
use axum::http::{header, HeaderName, HeaderValue};
use chrono::{DateTime, Utc};

pub(crate) fn apply_object_headers(
    headers: &mut axum::http::HeaderMap,
    head: &coldstore_proto::scheduler::HeadObjectResponse,
) {
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_str(&head.content_length.to_string())
            .unwrap_or_else(|_| HeaderValue::from_static("0")),
    );
    if let Some(content_type) = &head.content_type {
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_str(content_type).unwrap(),
        );
    }
    headers.insert(
        axum::http::header::ETAG,
        HeaderValue::from_str(&head.etag).unwrap(),
    );
    if let Some(restore_info) = &head.restore_info {
        headers.insert(
            HeaderName::from_static("x-amz-restore"),
            HeaderValue::from_str(&normalize_restore_info(restore_info)).unwrap(),
        );
    }

    if let Some(last_modified) = &head.last_modified {
        if let Some(last_modified) =
            DateTime::from_timestamp(last_modified.seconds, 0).map(|time| time.with_timezone(&Utc))
        {
            headers.insert(
                header::LAST_MODIFIED,
                HeaderValue::from_str(&format_http_date(last_modified))
                    .unwrap_or_else(|_| HeaderValue::from_static("")),
            );
        }
    }
}

fn format_http_date(time: DateTime<Utc>) -> String {
    time.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

fn normalize_restore_info(restore_info: &str) -> String {
    let restore_info = restore_info.trim();
    if let Some(expiry) = restore_info.strip_prefix("ongoing-request=\"false\", expiry-date=\"") {
        format_restore_header(false, Some(expiry.trim_end_matches('"')))
    } else if let Some(expiry) =
        restore_info.strip_prefix("ongoing-request=\"false\", expiry-ts=\"")
    {
        format_restore_header(false, Some(expiry.trim_end_matches('"')))
    } else if restore_info.starts_with("ongoing-request=\"false\"") {
        format_restore_header(false, None)
    } else {
        format_restore_header(true, None)
    }
}
