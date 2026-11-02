//! gRPC client wrappers the archive and recall paths depend on.
//!
//! AR-00 moved these out of `service.rs` with no behavior change: `cache` holds the staging and
//! restored cache traits with their gRPC clients, `tape` holds the archive-writer and recall-reader
//! traits, their clients, and the drive acquire/load helpers they share.

pub(crate) mod cache;
pub(crate) mod tape;
