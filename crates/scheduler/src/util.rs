//! Leaf helpers with no dependency on the rest of the crate: SHA-256 hex and timestamp arithmetic.
//!
//! AR-00 moved these out of `service.rs` with no behavior change.

use prost_types::Timestamp;
use sha2::{Digest, Sha256};

pub(crate) fn sha256_hex(body: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn now_timestamp() -> Timestamp {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

pub(crate) fn days_from_now(days: u32) -> Timestamp {
    let now = chrono::Utc::now() + chrono::Duration::days(days as i64);
    Timestamp {
        seconds: now.timestamp(),
        nanos: now.timestamp_subsec_nanos() as i32,
    }
}
