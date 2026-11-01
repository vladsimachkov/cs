// The cache service and its backend trait return `tonic::Status`, which is large enough
// to trip `clippy::result_large_err`; it cannot be shrunk from our side.
#![allow(clippy::result_large_err)]

pub mod backend;
pub mod hdd;
pub mod service;
pub mod spdk;

use anyhow::Result;
use coldstore_common::config::CacheConfig;
use tonic::transport::Server;
use tracing::info;

pub async fn run(config: CacheConfig) -> Result<()> {
    let addr = config.listen.parse()?;

    let cache_service = service::CacheServiceImpl::new(&config).await?;

    info!("Cache Worker 启动在 {}", config.listen);

    Server::builder()
        .add_service(
            coldstore_proto::cache::cache_service_server::CacheServiceServer::new(cache_service),
        )
        .serve(addr)
        .await?;

    Ok(())
}
