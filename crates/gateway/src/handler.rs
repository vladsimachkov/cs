//! Compatibility alias for the pre-GW-00 public path. The handlers moved: the route table is
//! `crate::router`, the operation handlers are `crate::s3`. GW-00 was move-only and kept
//! `handler::router` alive per plan step 2 ("keep public items re-exported"). Nothing in-tree uses
//! it any more — `run()` calls `router::router` directly — and no consumer outside this workspace
//! exists today, so GW-01 should delete this file.

pub use crate::router::router;
