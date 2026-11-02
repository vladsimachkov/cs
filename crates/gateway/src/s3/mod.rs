//! S3 operation handlers, grouped by operation family.
//!
//! GW-00 split these out of `handler.rs` with no behavior change. The route table lives in
//! `crate::router`; response and error construction is in `errors`; the shared test double is
//! `testing` (compiled only under `cfg(test)`). Importers name the concrete module
//! (`use crate::s3::errors::*`), there is no re-export facade.

pub(crate) mod bucket;
pub(crate) mod errors;
pub(crate) mod headers;
pub(crate) mod list;
pub(crate) mod object;
pub(crate) mod preconditions;
pub(crate) mod range;
pub(crate) mod restore;
pub(crate) mod xml;

#[cfg(test)]
pub(crate) mod testing;
