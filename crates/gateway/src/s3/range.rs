use crate::protocol::S3ErrorCode;
use axum::http::{header, HeaderValue, StatusCode};
use axum::response::Response;

use crate::s3::errors::{bad_request_response_with_code, empty_response};

pub(crate) fn parse_range_header(
    raw_range: &str,
    body_length: usize,
) -> std::result::Result<Option<(usize, usize)>, Box<Response>> {
    parse_byte_range(raw_range, body_length).map_err(|err| match err {
        ByteRangeParseError::Malformed => Box::new(bad_request_response_with_code(
            S3ErrorCode::InvalidArgument,
            "invalid range syntax",
            "Range",
        )),
        ByteRangeParseError::Unsatisfiable => {
            Box::new(range_not_satisfiable_response(body_length as u64))
        }
    })
}

fn parse_byte_range(
    raw_range: &str,
    object_size: usize,
) -> std::result::Result<Option<(usize, usize)>, ByteRangeParseError> {
    let raw = raw_range.trim();
    let spec = raw
        .split_once('=')
        .and_then(|(unit, spec)| {
            if unit.eq_ignore_ascii_case("bytes") {
                Some(spec)
            } else {
                None
            }
        })
        .ok_or(ByteRangeParseError::Malformed)?;
    if spec.is_empty() {
        return Err(ByteRangeParseError::Malformed);
    }

    if object_size == 0 {
        return Err(ByteRangeParseError::Unsatisfiable);
    }
    if spec.contains(',') {
        return Err(ByteRangeParseError::Malformed);
    }

    let (start_raw, end_raw) = spec.split_once('-').ok_or(ByteRangeParseError::Malformed)?;

    if start_raw.is_empty() {
        let suffix_len = end_raw
            .parse::<usize>()
            .map_err(|_| ByteRangeParseError::Malformed)?;
        if suffix_len == 0 {
            return Err(ByteRangeParseError::Malformed);
        }

        let start = object_size.saturating_sub(suffix_len);
        return Ok(Some((start, object_size.saturating_sub(1))));
    }

    let start = start_raw
        .parse::<usize>()
        .map_err(|_| ByteRangeParseError::Malformed)?;

    if start >= object_size {
        return Err(ByteRangeParseError::Unsatisfiable);
    }

    let mut end = if end_raw.is_empty() {
        object_size.saturating_sub(1)
    } else {
        end_raw
            .parse::<usize>()
            .map_err(|_| ByteRangeParseError::Malformed)?
    };

    if end < start {
        return Err(ByteRangeParseError::Unsatisfiable);
    }
    if end >= object_size {
        end = object_size.saturating_sub(1);
    }

    Ok(Some((start, end)))
}

enum ByteRangeParseError {
    Malformed,
    Unsatisfiable,
}

fn range_not_satisfiable_response(body_size: u64) -> Response {
    let mut response = empty_response(StatusCode::RANGE_NOT_SATISFIABLE);
    response.headers_mut().insert(
        header::CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes */{body_size}")).unwrap(),
    );
    response
}
