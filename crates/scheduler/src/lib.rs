// The scheduler is a gRPC service and a gRPC client, so its functions return
// `tonic::Status`, which is large enough to trip `clippy::result_large_err`.
#![allow(clippy::result_large_err)]

pub(crate) mod clients;
pub(crate) mod leases;
pub(crate) mod responses;
pub mod service;
pub(crate) mod util;

use anyhow::{anyhow, Result};
use coldstore_common::config::SchedulerConfig;
use coldstore_proto::cache::cache_service_client::CacheServiceClient;
use coldstore_proto::metadata::metadata_service_client::MetadataServiceClient;
use coldstore_proto::tape::tape_service_client::TapeServiceClient;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Duration};
use tonic::transport::{Channel, Server};
use tracing::{info, warn};

pub struct SchedulerState {
    pub metadata: MetadataServiceClient<Channel>,
    pub cache: Option<CacheServiceClient<Channel>>,
    pub tape: Option<TapeServiceClient<Channel>>,
    pub config: SchedulerConfig,
    pub active_archive_keys: Arc<Mutex<HashSet<String>>>,
    pub active_recall_tasks: Arc<Mutex<HashSet<String>>>,
    pub tape_locks: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
    pub archive_slots: Arc<Semaphore>,
    pub recall_slots: Arc<Semaphore>,
    pub recall_task_slots: Arc<Semaphore>,
}

pub async fn run(config: SchedulerConfig) -> Result<()> {
    let addr = config.listen.parse()?;

    let metadata_addr = format!("http://{}", config.metadata_addrs[0]);
    let metadata = connect_metadata_with_retry(metadata_addr).await?;
    let cache_addr = config
        .cache_addrs
        .first()
        .ok_or_else(|| anyhow!("scheduler requires at least one cache address"))?;
    let cache = connect_cache_with_retry(format!("http://{cache_addr}")).await?;
    let tape = if config.archive.enabled || config.recall.enabled {
        let tape_addr = config
            .tape_addrs
            .first()
            .ok_or_else(|| anyhow!("scheduler requires at least one tape address"))?;
        Some(connect_tape_with_retry(format!("http://{tape_addr}")).await?)
    } else {
        info!("Scheduler tape client disabled because archive and recall loops are disabled");
        None
    };

    let state = std::sync::Arc::new(SchedulerState {
        metadata,
        cache: Some(cache),
        tape,
        config: config.clone(),
        active_archive_keys: Arc::new(Mutex::new(HashSet::new())),
        active_recall_tasks: Arc::new(Mutex::new(HashSet::new())),
        tape_locks: Arc::new(Mutex::new(HashMap::new())),
        archive_slots: Arc::new(Semaphore::new(config.archive.max_workers.max(1))),
        recall_slots: Arc::new(Semaphore::new(config.recall.max_workers.max(1))),
        recall_task_slots: Arc::new(Semaphore::new(config.recall.max_concurrent_restores.max(1))),
    });

    service::spawn_background_loops(state.clone());
    let scheduler_service = service::SchedulerServiceImpl::new(state);

    info!("Scheduler Worker 启动在 {}", config.listen);

    Server::builder()
        .add_service(
            coldstore_proto::scheduler::scheduler_service_server::SchedulerServiceServer::new(
                scheduler_service,
            ),
        )
        .serve(addr)
        .await?;

    Ok(())
}

async fn connect_metadata_with_retry(addr: String) -> Result<MetadataServiceClient<Channel>> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match MetadataServiceClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(attempt, addr, error = %err, "metadata connection not ready");
                last_error = Some(err);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("metadata connection retry loop did not run")))
}

async fn connect_cache_with_retry(addr: String) -> Result<CacheServiceClient<Channel>> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match CacheServiceClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(attempt, addr, error = %err, "cache connection not ready");
                last_error = Some(err);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("cache connection retry loop did not run")))
}

async fn connect_tape_with_retry(addr: String) -> Result<TapeServiceClient<Channel>> {
    let mut last_error = None;
    for attempt in 1..=30 {
        match TapeServiceClient::connect(addr.clone()).await {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(attempt, addr, error = %err, "tape connection not ready");
                last_error = Some(err);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }
    Err(last_error
        .map(anyhow::Error::from)
        .unwrap_or_else(|| anyhow!("tape connection retry loop did not run")))
}
