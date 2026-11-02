use crate::backend::{CacheBackend, CacheCategory, CacheXattrs};
use crate::hdd::HddBackend;
use crate::spdk::SpdkBackend;
use anyhow::Result;
use coldstore_common::config::{CacheBackendConfig, CacheConfig};
use coldstore_proto::cache::cache_service_server::CacheService;
use coldstore_proto::cache::*;
use prost_types::Timestamp;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

const STREAM_CHUNK_SIZE: usize = 64 * 1024;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct CacheKey {
    bucket: String,
    key: String,
    version_id: Option<String>,
}

impl CacheKey {
    fn new(bucket: String, key: String, version_id: Option<String>) -> Self {
        Self {
            bucket,
            key,
            version_id,
        }
    }

    fn as_cursor(&self) -> String {
        format!(
            "{}\u{0}{}\u{0}{}",
            self.bucket,
            self.key,
            self.version_id.clone().unwrap_or_default()
        )
    }
}

#[derive(Debug, Clone)]
struct StoredEntry {
    storage_id: u64,
    xattrs: CacheXattrs,
}

#[derive(Default)]
struct CacheIndex {
    staging: HashMap<CacheKey, StoredEntry>,
    restored: HashMap<CacheKey, StoredEntry>,
    used_capacity: u64,
    hit_count: u64,
    miss_count: u64,
    evict_count: u64,
    evict_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
enum CapacityRejectReason {
    ZeroCapacity,
    IncomingLargerThanCapacity,
    NoEvictionCandidate,
    StagingBudgetExceeded,
    RestoredBudgetExceeded,
    GlobalCapacityExceeded,
    LowWatermarkExceeded,
}

impl CapacityRejectReason {
    fn code(self) -> &'static str {
        match self {
            Self::ZeroCapacity => "zero_capacity",
            Self::IncomingLargerThanCapacity => "incoming_larger_than_capacity",
            Self::NoEvictionCandidate => "no_eviction_candidate",
            Self::StagingBudgetExceeded => "staging_budget_exceeded",
            Self::RestoredBudgetExceeded => "restored_budget_exceeded",
            Self::GlobalCapacityExceeded => "global_capacity_exceeded",
            Self::LowWatermarkExceeded => "low_watermark_exceeded",
        }
    }
}

fn capacity_reject_error(reason: CapacityRejectReason, detail: impl Into<String>) -> anyhow::Error {
    anyhow::anyhow!("capacity_reject:{}: {}", reason.code(), detail.into())
}

impl CacheIndex {
    fn recompute_used_capacity(&mut self) {
        self.used_capacity = self
            .staging
            .values()
            .chain(self.restored.values())
            .map(|entry| entry.xattrs.size)
            .sum();
    }

    fn staging_used(&self) -> u64 {
        self.staging.values().map(|entry| entry.xattrs.size).sum()
    }

    fn restored_used(&self) -> u64 {
        self.restored.values().map(|entry| entry.xattrs.size).sum()
    }
}

fn rebuilt_entry_should_replace(current: &StoredEntry, candidate: &StoredEntry) -> bool {
    (candidate.xattrs.cached_at, candidate.storage_id)
        > (current.xattrs.cached_at, current.storage_id)
}

#[derive(Clone, Copy)]
enum EvictionPolicy {
    Lru,
    Lfu,
}

impl EvictionPolicy {
    fn from_config(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "lfu" => Self::Lfu,
            _ => Self::Lru,
        }
    }
}

#[derive(Clone)]
pub struct CacheServiceImpl {
    backend: Arc<dyn CacheBackend>,
    max_size_bytes: u64,
    eviction_policy: EvictionPolicy,
    eviction_batch_size: usize,
    eviction_low_watermark: f64,
    staging_capacity_bytes: u64,
    restored_capacity_bytes: u64,
    index: Arc<RwLock<CacheIndex>>,
    write_lock: Arc<Mutex<()>>,
}

#[derive(Clone, Copy)]
struct CapacityPressure {
    staging_after: u64,
    restored_after: u64,
    used_after: u64,
    staging_budget: u64,
    restored_budget: u64,
    low_watermark_target: Option<u64>,
    total_capacity: u64,
}

impl CacheServiceImpl {
    pub async fn new(config: &CacheConfig) -> Result<Self> {
        let backend: Arc<dyn CacheBackend> = match &config.backend {
            CacheBackendConfig::Hdd { path, max_size_gb } => {
                Arc::new(HddBackend::new(path.clone(), *max_size_gb).await?)
            }
            CacheBackendConfig::Spdk {
                config_file,
                bdev_name,
                max_size_gb,
                ..
            } => Arc::new(
                SpdkBackend::new(config_file.clone(), bdev_name.clone(), *max_size_gb).await?,
            ),
        };
        let max_size_bytes = backend_capacity_bytes(&config.backend);
        let eviction_policy = EvictionPolicy::from_config(&config.eviction_policy);
        let (staging_capacity_bytes, restored_capacity_bytes) = split_cache_capacity(
            max_size_bytes,
            config.staging_capacity_ratio,
            config.restored_capacity_ratio,
        );

        let svc = Self {
            backend,
            max_size_bytes,
            eviction_policy,
            eviction_batch_size: config.eviction_batch_size.max(1),
            eviction_low_watermark: normalize_watermark(config.eviction_low_watermark),
            staging_capacity_bytes,
            restored_capacity_bytes,
            index: Arc::new(RwLock::new(CacheIndex::default())),
            write_lock: Arc::new(Mutex::new(())),
        };
        svc.rebuild_index().await?;
        Ok(svc)
    }

    async fn rebuild_index(&self) -> Result<()> {
        let mut index = CacheIndex::default();
        let mut stale_storage_ids = Vec::new();
        let mut duplicate_loser_count = 0u64;
        let mut duplicate_loser_bytes = 0u64;
        for (storage_id, xattrs) in self.backend.list_all().await? {
            let key = CacheKey::new(
                xattrs.bucket.clone(),
                xattrs.key.clone(),
                xattrs.version_id.clone(),
            );
            let entry = StoredEntry { storage_id, xattrs };
            let replaced = match entry.xattrs.category {
                CacheCategory::Staging => match index.staging.get(&key) {
                    Some(current) if !rebuilt_entry_should_replace(current, &entry) => {
                        duplicate_loser_count = duplicate_loser_count.saturating_add(1);
                        duplicate_loser_bytes =
                            duplicate_loser_bytes.saturating_add(entry.xattrs.size);
                        stale_storage_ids.push(entry.storage_id);
                        None
                    }
                    _ => index.staging.insert(key, entry),
                },
                CacheCategory::Restored => match index.restored.get(&key) {
                    Some(current) if !rebuilt_entry_should_replace(current, &entry) => {
                        duplicate_loser_count = duplicate_loser_count.saturating_add(1);
                        duplicate_loser_bytes =
                            duplicate_loser_bytes.saturating_add(entry.xattrs.size);
                        stale_storage_ids.push(entry.storage_id);
                        None
                    }
                    _ => index.restored.insert(key, entry),
                },
            };
            if let Some(replaced) = replaced {
                duplicate_loser_count = duplicate_loser_count.saturating_add(1);
                duplicate_loser_bytes = duplicate_loser_bytes.saturating_add(replaced.xattrs.size);
                stale_storage_ids.push(replaced.storage_id);
            }
        }
        index.recompute_used_capacity();
        *self.index.write().await = index;

        if duplicate_loser_count > 0 {
            warn!(
                duplicate_loser_count,
                duplicate_loser_bytes,
                "cache index rebuild found duplicate cache objects; stale storage objects will be deleted best-effort"
            );
        }
        for storage_id in stale_storage_ids {
            if let Err(err) = self.backend.delete(storage_id).await {
                warn!(
                    storage_id,
                    error = %err,
                    "cache index rebuild failed to delete duplicate stale storage object"
                );
            }
        }

        Ok(())
    }

    async fn find_entry(&self, key: &CacheKey, category: CacheCategory) -> Option<StoredEntry> {
        let index = self.index.read().await;
        match category {
            CacheCategory::Staging => index.staging.get(key).cloned(),
            CacheCategory::Restored => index.restored.get(key).cloned(),
        }
    }

    async fn insert_entry(&self, key: CacheKey, entry: StoredEntry) -> Option<StoredEntry> {
        let mut index = self.index.write().await;
        let previous = match entry.xattrs.category {
            CacheCategory::Staging => index.staging.insert(key, entry),
            CacheCategory::Restored => index.restored.insert(key, entry),
        };
        index.recompute_used_capacity();
        previous
    }

    async fn update_hit_state(&self, hit: bool) {
        let mut index = self.index.write().await;
        if hit {
            index.hit_count += 1;
        } else {
            index.miss_count += 1;
        }
    }

    async fn touch_entry(&self, key: &CacheKey, category: CacheCategory) {
        let now = now_unix();
        let mut index = self.index.write().await;
        match category {
            CacheCategory::Staging => {
                if let Some(entry) = index.staging.get_mut(key) {
                    entry.xattrs.last_access_at = now;
                    entry.xattrs.access_count += 1;
                }
            }
            CacheCategory::Restored => {
                if let Some(entry) = index.restored.get_mut(key) {
                    entry.xattrs.last_access_at = now;
                    entry.xattrs.access_count += 1;
                }
            }
        }
    }

    async fn delete_entry(&self, key: &CacheKey, category: CacheCategory) -> Result<bool> {
        let storage_id = match category {
            CacheCategory::Staging => self
                .index
                .read()
                .await
                .staging
                .get(key)
                .map(|entry| entry.storage_id),
            CacheCategory::Restored => self
                .index
                .read()
                .await
                .restored
                .get(key)
                .map(|entry| entry.storage_id),
        };

        let Some(storage_id) = storage_id else {
            return Ok(false);
        };

        self.backend.delete(storage_id).await?;

        let mut index = self.index.write().await;
        let removed = match category {
            CacheCategory::Staging => index
                .staging
                .get(key)
                .is_some_and(|entry| entry.storage_id == storage_id),
            CacheCategory::Restored => index
                .restored
                .get(key)
                .is_some_and(|entry| entry.storage_id == storage_id),
        };
        if removed {
            match category {
                CacheCategory::Staging => {
                    index.staging.remove(key);
                }
                CacheCategory::Restored => {
                    index.restored.remove(key);
                }
            }
            index.recompute_used_capacity();
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn put_bytes(&self, key: CacheKey, data: Vec<u8>, xattrs: CacheXattrs) -> Result<u64> {
        let guard = self.write_lock.lock().await;
        self.ensure_object_size_within_total_capacity(xattrs.size)
            .await?;
        let existing = self.find_entry(&key, xattrs.category).await;
        let capacity_delta = xattrs
            .size
            .saturating_sub(existing.as_ref().map_or(0, |entry| entry.xattrs.size));
        let reclaim_skip = if xattrs.category == CacheCategory::Restored {
            Some(&key)
        } else {
            None
        };
        self.reclaim_expired_restored_except(reclaim_skip).await?;
        self.evict_if_needed(capacity_delta, xattrs.category)
            .await?;
        let storage_id = self.backend.write(&key.as_cursor(), &data, &xattrs).await?;
        let previous = self
            .insert_entry(key, StoredEntry { storage_id, xattrs })
            .await;
        drop(guard);

        if let Some(previous_storage_id) = previous.map(|entry| entry.storage_id) {
            if previous_storage_id != storage_id {
                if let Err(err) = self.backend.delete(previous_storage_id).await {
                    warn!(
                        previous_storage_id,
                        new_storage_id = storage_id,
                        error = %err,
                        "cache overwrite failed to delete replaced storage object"
                    );
                }
            }
        }

        Ok(storage_id)
    }

    async fn ensure_object_size_within_total_capacity(&self, object_size: u64) -> Result<()> {
        let total_capacity = self.effective_total_capacity().await?;
        if total_capacity == 0 {
            return Err(capacity_reject_error(
                CapacityRejectReason::ZeroCapacity,
                "cache capacity is zero",
            ));
        }
        if object_size > total_capacity {
            return Err(capacity_reject_error(
                CapacityRejectReason::IncomingLargerThanCapacity,
                "incoming object exceeds cache capacity",
            ));
        }
        Ok(())
    }

    async fn read_restored(&self, key: &CacheKey) -> Result<StoredEntry, Status> {
        let Some(entry) = self.find_entry(key, CacheCategory::Restored).await else {
            self.update_hit_state(false).await;
            return Err(Status::not_found("restored object not found in cache"));
        };

        if is_expired(entry.xattrs.expire_at) {
            let _ = self.delete_entry(key, CacheCategory::Restored).await;
            self.update_hit_state(false).await;
            return Err(Status::not_found("restored object has expired"));
        }

        self.touch_entry(key, CacheCategory::Restored).await;
        self.update_hit_state(true).await;
        Ok(entry)
    }

    #[cfg(test)]
    async fn reclaim_expired_restored(&self) -> Result<()> {
        self.reclaim_expired_restored_except(None).await
    }

    async fn reclaim_expired_restored_except(&self, skip: Option<&CacheKey>) -> Result<()> {
        let now = now_unix();
        let expired: Vec<(CacheKey, StoredEntry)> = {
            let index = self.index.read().await;
            index
                .restored
                .iter()
                .filter(|(key, _)| skip != Some(*key))
                .filter(|(_, entry)| is_expired_at(entry.xattrs.expire_at, now))
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect()
        };

        for (key, _entry) in expired {
            let _ = self.delete_entry(&key, CacheCategory::Restored).await;
        }

        Ok(())
    }

    async fn effective_total_capacity(&self) -> Result<u64> {
        let used_capacity = {
            let index = self.index.read().await;
            index.used_capacity
        };
        let available_bytes = self.backend.available_bytes().await?;
        let observed_total = used_capacity.saturating_add(available_bytes);
        Ok(observed_total.min(self.max_size_bytes))
    }

    fn split_capacity_by_static_ratio(&self, total_capacity: u64) -> (u64, u64) {
        let configured_budget_total = self
            .staging_capacity_bytes
            .saturating_add(self.restored_capacity_bytes);
        if configured_budget_total == 0 {
            return (0, total_capacity);
        }

        let staging_budget =
            total_capacity.saturating_mul(self.staging_capacity_bytes) / configured_budget_total;
        let restored_budget = total_capacity.saturating_sub(staging_budget);
        (staging_budget, restored_budget)
    }

    async fn evict_if_needed(&self, incoming_size: u64, category: CacheCategory) -> Result<()> {
        if incoming_size == 0 {
            return Ok(());
        }

        let total_capacity = self.effective_total_capacity().await?;
        if total_capacity == 0 {
            return Err(capacity_reject_error(
                CapacityRejectReason::ZeroCapacity,
                "cache capacity is zero",
            ));
        }
        if incoming_size > total_capacity {
            return Err(capacity_reject_error(
                CapacityRejectReason::IncomingLargerThanCapacity,
                "incoming object exceeds cache capacity",
            ));
        }

        let (staging_budget, restored_budget) = self.split_capacity_by_static_ratio(total_capacity);
        let low_watermark_target = self.effective_low_watermark_used(total_capacity);

        for _ in 0..self.evict_batch_size() {
            let (staging_used, restored_used, used_capacity) = {
                let index = self.index.read().await;
                (
                    index.staging_used(),
                    index.restored_used(),
                    index.used_capacity,
                )
            };

            let staging_after =
                staging_used.saturating_add(if category == CacheCategory::Staging {
                    incoming_size
                } else {
                    0
                });
            let restored_after =
                restored_used.saturating_add(if category == CacheCategory::Restored {
                    incoming_size
                } else {
                    0
                });
            let used_after_incoming = used_capacity.saturating_add(incoming_size);

            let pressure = CapacityPressure {
                staging_after,
                restored_after,
                used_after: used_after_incoming,
                staging_budget,
                restored_budget,
                low_watermark_target,
                total_capacity,
            };

            let needs_eviction = self.should_evict(pressure);

            if !needs_eviction {
                return Ok(());
            }

            let victim_category = {
                let index = self.index.read().await;
                self.choose_victim_category(category, pressure, &index)
            };
            let Some(victim_category) = victim_category else {
                break;
            };

            let victim = {
                let index = self.index.read().await;
                self.select_eviction_candidate(&index, victim_category)
            }
            .ok_or_else(|| {
                capacity_reject_error(
                    CapacityRejectReason::NoEvictionCandidate,
                    "no restored cache victim is available for eviction",
                )
            })?;

            {
                let mut index = self.index.write().await;
                let exists = match victim_category {
                    CacheCategory::Staging => index
                        .staging
                        .get(&victim.0)
                        .is_some_and(|entry| entry.storage_id == victim.1.storage_id),
                    CacheCategory::Restored => index
                        .restored
                        .get(&victim.0)
                        .is_some_and(|entry| entry.storage_id == victim.1.storage_id),
                };
                if !exists {
                    continue;
                }

                match victim_category {
                    CacheCategory::Staging => {
                        index.staging.remove(&victim.0);
                    }
                    CacheCategory::Restored => {
                        index.restored.remove(&victim.0);
                    }
                }
                index.evict_count += 1;
                index.evict_bytes += victim.1.xattrs.size;
                index.recompute_used_capacity();
            }

            match self.backend.delete(victim.1.storage_id).await {
                Ok(()) => {}
                Err(err) => {
                    let mut index = self.index.write().await;
                    match victim_category {
                        CacheCategory::Staging => {
                            index.staging.insert(
                                victim.0,
                                StoredEntry {
                                    storage_id: victim.1.storage_id,
                                    xattrs: victim.1.xattrs,
                                },
                            );
                        }
                        CacheCategory::Restored => {
                            index.restored.insert(
                                victim.0,
                                StoredEntry {
                                    storage_id: victim.1.storage_id,
                                    xattrs: victim.1.xattrs,
                                },
                            );
                        }
                    }
                    index.recompute_used_capacity();
                    return Err(err);
                }
            }
        }

        let index = self.index.read().await;
        let staging_after =
            index
                .staging_used()
                .saturating_add(if category == CacheCategory::Staging {
                    incoming_size
                } else {
                    0
                });
        let restored_after =
            index
                .restored_used()
                .saturating_add(if category == CacheCategory::Restored {
                    incoming_size
                } else {
                    0
                });
        let used_after = index.used_capacity.saturating_add(incoming_size);

        let pressure = CapacityPressure {
            staging_after,
            restored_after,
            used_after,
            staging_budget,
            restored_budget,
            low_watermark_target,
            total_capacity,
        };
        let needs_eviction = self.should_evict(pressure);
        if !needs_eviction {
            return Ok(());
        }
        if staging_after > staging_budget {
            Err(capacity_reject_error(
                CapacityRejectReason::StagingBudgetExceeded,
                "not enough staging cache budget for write path; cannot fit after eviction",
            ))
        } else if restored_after > restored_budget {
            Err(capacity_reject_error(
                CapacityRejectReason::RestoredBudgetExceeded,
                "not enough restored cache budget for write path; cannot fit after eviction",
            ))
        } else if used_after > total_capacity {
            Err(capacity_reject_error(
                CapacityRejectReason::GlobalCapacityExceeded,
                "not enough global cache capacity",
            ))
        } else if low_watermark_target.is_some_and(|target| used_after > target) {
            Err(capacity_reject_error(
                CapacityRejectReason::LowWatermarkExceeded,
                "not enough cache capacity to satisfy low-watermark",
            ))
        } else {
            Ok(())
        }
    }

    fn should_evict(&self, pressure: CapacityPressure) -> bool {
        let staging_pressure = pressure.staging_after > pressure.staging_budget;
        let restored_pressure = pressure.restored_after > pressure.restored_budget;
        let global_watermark_pressure = match pressure.low_watermark_target {
            Some(target) => pressure.used_after > target,
            None => false,
        };
        staging_pressure
            || restored_pressure
            || global_watermark_pressure
            || pressure.used_after > pressure.total_capacity
    }

    fn evict_batch_size(&self) -> usize {
        self.eviction_batch_size.max(1)
    }

    fn map_capacity_error(err: anyhow::Error) -> Status {
        let message = err.to_string();
        let message_lc = message.to_lowercase();
        if message_lc.contains("capacity")
            || message_lc.contains("not enough")
            || message_lc.contains("not enough staging")
            || message_lc.contains("not enough restored")
            || message_lc.contains("exceeds")
            || message_lc.contains("no available")
            || message_lc.contains("insufficient")
        {
            Status::resource_exhausted(message)
        } else {
            internal_status(err)
        }
    }

    fn effective_low_watermark_used(&self, total_bytes: u64) -> Option<u64> {
        let watermark = self.eviction_low_watermark.clamp(0.0, 1.0);
        if !watermark.is_finite() || watermark <= 0.0 {
            None
        } else {
            Some((total_bytes as f64 * watermark) as u64)
        }
    }

    fn choose_victim_category(
        &self,
        incoming_category: CacheCategory,
        pressure: CapacityPressure,
        index: &CacheIndex,
    ) -> Option<CacheCategory> {
        let incoming_used_after = pressure.used_after;
        let staging_after = pressure.staging_after;
        let restored_after = pressure.restored_after;
        let staging_budget = pressure.staging_budget;
        let restored_budget = pressure.restored_budget;
        let total_capacity = pressure.total_capacity;
        let low_watermark_target = pressure.low_watermark_target;
        let has_restored = !index.restored.is_empty();

        let staging_pressure = staging_after > staging_budget;
        let restored_pressure = restored_after > restored_budget;
        let global_watermark_pressure = match low_watermark_target {
            Some(target) => incoming_used_after > target,
            None => false,
        };

        if staging_pressure {
            return None;
        }

        if has_restored
            && (restored_pressure
                || global_watermark_pressure
                || incoming_used_after > total_capacity)
        {
            return Some(CacheCategory::Restored);
        }

        let _ = incoming_category;
        None
    }

    fn select_eviction_candidate(
        &self,
        index: &CacheIndex,
        category: CacheCategory,
    ) -> Option<(CacheKey, StoredEntry)> {
        let candidate_map = match category {
            CacheCategory::Staging => return None,
            CacheCategory::Restored => &index.restored,
        };

        match self.eviction_policy {
            EvictionPolicy::Lfu => candidate_map
                .iter()
                .min_by_key(|(key, entry)| {
                    (
                        entry.xattrs.access_count,
                        entry.xattrs.last_access_at,
                        entry.xattrs.cached_at,
                        entry.xattrs.size,
                        key.as_cursor(),
                    )
                })
                .map(|(key, entry)| (key.clone(), entry.clone())),
            EvictionPolicy::Lru => candidate_map
                .iter()
                .min_by_key(|(_, entry)| {
                    (
                        entry.xattrs.last_access_at,
                        entry.xattrs.cached_at,
                        entry.xattrs.size,
                        entry.xattrs.access_count,
                        entry.xattrs.key.as_str(),
                    )
                })
                .map(|(key, entry)| (key.clone(), entry.clone())),
        }
    }

    async fn build_get_stream(
        &self,
        entry: StoredEntry,
    ) -> Result<Response<ReceiverStream<Result<GetResponse, Status>>>, Status> {
        let data = self
            .backend
            .read(entry.storage_id)
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let meta = GetResponse {
                payload: Some(get_response::Payload::Meta(CachedObjectMeta {
                    size: entry.xattrs.size,
                    expire_at: Some(timestamp_from_unix(entry.xattrs.expire_at)),
                    content_type: entry.xattrs.content_type.clone(),
                    etag: entry.xattrs.etag.clone(),
                    checksum: entry.xattrs.checksum.clone(),
                })),
            };
            let _ = tx.send(Ok(meta)).await;
            for chunk in data.chunks(STREAM_CHUNK_SIZE) {
                let _ = tx
                    .send(Ok(GetResponse {
                        payload: Some(get_response::Payload::Data(chunk.to_vec())),
                    }))
                    .await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn build_staging_stream(
        &self,
        entry: StoredEntry,
    ) -> Result<Response<ReceiverStream<Result<GetStagingResponse, Status>>>, Status> {
        let data = self
            .backend
            .read(entry.storage_id)
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(async move {
            let meta = GetStagingResponse {
                payload: Some(get_staging_response::Payload::Meta(StagingObjectMeta {
                    bucket: entry.xattrs.bucket.clone(),
                    key: entry.xattrs.key.clone(),
                    version_id: entry.xattrs.version_id.clone(),
                    size: entry.xattrs.size,
                    checksum: entry.xattrs.checksum.clone(),
                    content_type: entry.xattrs.content_type.clone(),
                    etag: entry.xattrs.etag.clone(),
                    staged_at: Some(timestamp_from_unix(entry.xattrs.cached_at)),
                })),
            };
            let _ = tx.send(Ok(meta)).await;
            for chunk in data.chunks(STREAM_CHUNK_SIZE) {
                let _ = tx
                    .send(Ok(GetStagingResponse {
                        payload: Some(get_staging_response::Payload::Data(chunk.to_vec())),
                    }))
                    .await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl CacheService for CacheServiceImpl {
    async fn put_staging(
        &self,
        req: Request<Streaming<PutStagingRequest>>,
    ) -> std::result::Result<Response<PutStagingResponse>, Status> {
        let mut stream = req.into_inner();
        let mut meta: Option<PutStagingMeta> = None;
        let mut data = Vec::new();

        while let Some(chunk) = stream.message().await? {
            match chunk.payload {
                Some(put_staging_request::Payload::Meta(m)) => meta = Some(m),
                Some(put_staging_request::Payload::Data(bytes)) => data.extend_from_slice(&bytes),
                None => return Err(Status::invalid_argument("empty put_staging chunk")),
            }
        }

        let meta = meta.ok_or_else(|| Status::invalid_argument("missing staging metadata"))?;
        if meta.size != data.len() as u64 {
            return Err(Status::invalid_argument(
                "staging object size does not match payload",
            ));
        }

        let key = CacheKey::new(
            meta.bucket.clone(),
            meta.key.clone(),
            meta.version_id.clone(),
        );
        let xattrs = CacheXattrs {
            bucket: meta.bucket,
            key: meta.key,
            version_id: meta.version_id,
            size: meta.size,
            expire_at: 0,
            cached_at: now_unix(),
            last_access_at: now_unix(),
            access_count: 0,
            checksum: meta.checksum,
            content_type: meta.content_type,
            etag: meta.etag,
            category: CacheCategory::Staging,
        };
        let storage_id = self
            .put_bytes(key, data, xattrs)
            .await
            .map_err(CacheServiceImpl::map_capacity_error)?;

        Ok(Response::new(PutStagingResponse {
            staging_id: storage_id.to_string(),
        }))
    }

    async fn put_restored(
        &self,
        req: Request<Streaming<PutRestoredRequest>>,
    ) -> std::result::Result<Response<()>, Status> {
        let mut stream = req.into_inner();
        let mut meta: Option<PutRestoredMeta> = None;
        let mut data = Vec::new();

        while let Some(chunk) = stream.message().await? {
            match chunk.payload {
                Some(put_restored_request::Payload::Meta(m)) => meta = Some(m),
                Some(put_restored_request::Payload::Data(bytes)) => data.extend_from_slice(&bytes),
                None => return Err(Status::invalid_argument("empty put_restored chunk")),
            }
        }

        let meta = meta.ok_or_else(|| Status::invalid_argument("missing restored metadata"))?;
        if meta.size != data.len() as u64 {
            return Err(Status::invalid_argument(
                "restored object size does not match payload",
            ));
        }
        let expire_at = meta
            .expire_at
            .ok_or_else(|| Status::invalid_argument("restored object missing expire_at"))?;

        let key = CacheKey::new(
            meta.bucket.clone(),
            meta.key.clone(),
            meta.version_id.clone(),
        );
        let xattrs = CacheXattrs {
            bucket: meta.bucket,
            key: meta.key,
            version_id: meta.version_id,
            size: meta.size,
            expire_at: expire_at.seconds,
            cached_at: now_unix(),
            last_access_at: now_unix(),
            access_count: 0,
            checksum: meta.checksum,
            content_type: meta.content_type,
            etag: meta.etag,
            category: CacheCategory::Restored,
        };
        self.put_bytes(key, data, xattrs)
            .await
            .map_err(CacheServiceImpl::map_capacity_error)?;

        Ok(Response::new(()))
    }

    async fn delete(
        &self,
        req: Request<DeleteRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        let req = req.into_inner();
        let key = CacheKey::new(req.bucket, req.key, req.version_id);
        self.delete_entry(&key, CacheCategory::Restored)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    type GetStream = ReceiverStream<Result<GetResponse, Status>>;

    async fn get(
        &self,
        req: Request<GetRequest>,
    ) -> std::result::Result<Response<Self::GetStream>, Status> {
        let req = req.into_inner();
        let key = CacheKey::new(req.bucket, req.key, req.version_id);
        let entry = self.read_restored(&key).await?;
        self.build_get_stream(entry).await
    }

    async fn contains(
        &self,
        req: Request<ContainsRequest>,
    ) -> std::result::Result<Response<ContainsResponse>, Status> {
        let req = req.into_inner();
        let key = CacheKey::new(req.bucket, req.key, req.version_id);
        let exists = self.find_entry(&key, CacheCategory::Restored).await;
        let response = if let Some(entry) = exists {
            if is_expired(entry.xattrs.expire_at) {
                let _ = self.delete_entry(&key, CacheCategory::Restored).await;
                self.update_hit_state(false).await;
                ContainsResponse {
                    exists: false,
                    expire_at: None,
                }
            } else {
                self.update_hit_state(true).await;
                ContainsResponse {
                    exists: true,
                    expire_at: Some(timestamp_from_unix(entry.xattrs.expire_at)),
                }
            }
        } else {
            self.update_hit_state(false).await;
            ContainsResponse {
                exists: false,
                expire_at: None,
            }
        };
        Ok(Response::new(response))
    }

    type GetStagingStream = ReceiverStream<Result<GetStagingResponse, Status>>;

    async fn get_staging(
        &self,
        req: Request<GetStagingRequest>,
    ) -> std::result::Result<Response<Self::GetStagingStream>, Status> {
        let req = req.into_inner();
        let key = CacheKey::new(req.bucket, req.key, req.version_id);
        let Some(entry) = self.find_entry(&key, CacheCategory::Staging).await else {
            return Err(Status::not_found("staging object not found"));
        };
        self.build_staging_stream(entry).await
    }

    async fn list_staging_keys(
        &self,
        req: Request<ListStagingKeysRequest>,
    ) -> std::result::Result<Response<ListStagingKeysResponse>, Status> {
        let req = req.into_inner();
        let after = req.after.unwrap_or_default();
        let limit = if req.limit == 0 {
            usize::MAX
        } else {
            req.limit as usize
        };

        let index = self.index.read().await;
        let mut entries: Vec<_> = index
            .staging
            .iter()
            .filter(|(key, _)| key.as_cursor() > after)
            .map(|(key, entry)| (key.clone(), entry.clone()))
            .collect();
        entries.sort_by_key(|(key, _)| key.as_cursor());

        let has_more = entries.len() > limit;
        let response_entries = entries
            .into_iter()
            .take(limit)
            .map(|(key, entry)| StagingKeyEntry {
                bucket: key.bucket,
                key: key.key,
                version_id: key.version_id,
                size: entry.xattrs.size,
                staged_at: Some(timestamp_from_unix(entry.xattrs.cached_at)),
            })
            .collect();

        Ok(Response::new(ListStagingKeysResponse {
            entries: response_entries,
            has_more,
        }))
    }

    async fn delete_staging(
        &self,
        req: Request<DeleteStagingRequest>,
    ) -> std::result::Result<Response<()>, Status> {
        let req = req.into_inner();
        let key = CacheKey::new(req.bucket, req.key, req.version_id);
        self.delete_entry(&key, CacheCategory::Staging)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(()))
    }

    async fn stats(&self, _req: Request<()>) -> std::result::Result<Response<CacheStats>, Status> {
        let (
            staging_bytes,
            restored_bytes,
            object_count,
            staging_count,
            restored_count,
            hit_count,
            miss_count,
            evict_count,
            evict_bytes,
        ) = {
            let index = self.index.read().await;
            (
                index
                    .staging
                    .values()
                    .map(|entry| entry.xattrs.size)
                    .sum::<u64>(),
                index
                    .restored
                    .values()
                    .map(|entry| entry.xattrs.size)
                    .sum::<u64>(),
                (index.staging.len() + index.restored.len()) as u64,
                index.staging.len() as u64,
                index.restored.len() as u64,
                index.hit_count,
                index.miss_count,
                index.evict_count,
                index.evict_bytes,
            )
        };

        let used_capacity = staging_bytes + restored_bytes;
        let available_capacity = self
            .backend
            .available_bytes()
            .await
            .map_err(internal_status)?;
        let total_capacity = used_capacity
            .saturating_add(available_capacity)
            .min(self.max_size_bytes);

        Ok(Response::new(CacheStats {
            total_capacity,
            used_capacity,
            object_count,
            staging_count,
            staging_bytes,
            restored_count,
            restored_bytes,
            hit_count,
            miss_count,
            evict_count,
            evict_bytes,
        }))
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

fn timestamp_from_unix(seconds: i64) -> Timestamp {
    Timestamp { seconds, nanos: 0 }
}

fn is_expired(expire_at: i64) -> bool {
    expire_at > 0 && expire_at <= now_unix()
}

fn is_expired_at(expire_at: i64, now: i64) -> bool {
    expire_at > 0 && expire_at <= now
}

fn backend_capacity_bytes(config: &CacheBackendConfig) -> u64 {
    match config {
        CacheBackendConfig::Hdd { max_size_gb, .. } => {
            max_size_gb.saturating_mul(1024 * 1024 * 1024)
        }
        CacheBackendConfig::Spdk { max_size_gb, .. } => {
            max_size_gb.saturating_mul(1024 * 1024 * 1024)
        }
    }
}

fn normalize_watermark(watermark: f64) -> f64 {
    if watermark.is_nan() || !watermark.is_finite() {
        return 0.8;
    }
    watermark.clamp(0.0, 1.0)
}

fn normalize_cache_ratio(ratio: f64) -> f64 {
    if ratio.is_nan() || !ratio.is_finite() {
        return 0.0;
    }
    ratio.clamp(0.0, 1.0)
}

fn split_cache_capacity(total_bytes: u64, staging_ratio: f64, restored_ratio: f64) -> (u64, u64) {
    let staging = normalize_cache_ratio(staging_ratio);
    let restored = normalize_cache_ratio(restored_ratio);
    let normalized_total = staging + restored;

    if normalized_total == 0.0 {
        return (0, total_bytes);
    }

    let staging_bytes = ((total_bytes as f64) * (staging / normalized_total)).floor() as u64;
    let restored_bytes = total_bytes.saturating_sub(staging_bytes);
    (staging_bytes, restored_bytes)
}

fn internal_status(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use coldstore_proto::cache::ContainsRequest;
    use tokio_stream::StreamExt;
    use tonic::Code;

    /// Two tests must never share a cache directory. Naming the path from
    /// `SystemTime::now().as_nanos()` did not guarantee that: two threads asking at the
    /// same instant got the same path, shared one `meta/`, and then a sibling's `delete()`
    /// removed the `meta/<id>.json` the other one was about to read.
    #[test]
    fn test_config_never_reuses_a_temp_dir() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..256 {
            let path = match test_config().backend {
                CacheBackendConfig::Hdd { path, .. } => path,
                CacheBackendConfig::Spdk { .. } => panic!("test_config() must use the hdd backend"),
            };
            assert!(
                seen.insert(path.clone()),
                "test_config() handed out the same temp dir twice: {path}"
            );
            // Pin the mechanism, not just the outcome: a clock reading can collide between
            // two threads that read it at the same instant, so the name must be a uuid.
            let name = path.rsplit('/').next().unwrap();
            let suffix = name
                .strip_prefix("coldstore-cache-test-")
                .expect("unexpected cache test dir prefix");
            uuid::Uuid::parse_str(suffix)
                .expect("the cache test temp dir must be named by a uuid, not a clock reading");
        }
    }

    fn test_config() -> CacheConfig {
        // A uuid, not a timestamp: two tests running in parallel can read the clock at the
        // same instant, and then both services share one meta/ directory.
        let unique = uuid::Uuid::new_v4();
        CacheConfig {
            backend: CacheBackendConfig::Hdd {
                path: format!("/tmp/coldstore-cache-test-{unique}"),
                max_size_gb: 1,
            },
            ..CacheConfig::default()
        }
    }

    #[tokio::test]
    async fn empty_cache_reports_miss() {
        let svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        let response = svc
            .contains(Request::new(ContainsRequest {
                bucket: "docs".into(),
                key: "readme.txt".into(),
                version_id: None,
            }))
            .await
            .expect("contains should return response")
            .into_inner();

        assert!(!response.exists);
    }

    #[tokio::test]
    async fn restored_object_round_trip_streams_data() {
        let svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        let expires_at = now_unix() + 3600;
        let key = CacheKey::new("docs".into(), "guide.txt".into(), Some("v1".into()));
        let xattrs = CacheXattrs {
            bucket: "docs".into(),
            key: "guide.txt".into(),
            version_id: Some("v1".into()),
            size: 11,
            expire_at: expires_at,
            cached_at: now_unix(),
            last_access_at: now_unix(),
            access_count: 0,
            checksum: Some("sum".into()),
            content_type: Some("text/plain".into()),
            etag: Some("etag-1".into()),
            category: CacheCategory::Restored,
        };
        svc.put_bytes(key, b"hello world".to_vec(), xattrs)
            .await
            .expect("put restored should succeed");

        let contains = svc
            .contains(Request::new(ContainsRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: Some("v1".into()),
            }))
            .await
            .expect("contains should succeed")
            .into_inner();
        assert!(contains.exists);

        let mut stream = svc
            .get(Request::new(GetRequest {
                bucket: "docs".into(),
                key: "guide.txt".into(),
                version_id: Some("v1".into()),
            }))
            .await
            .expect("get should succeed")
            .into_inner();

        let first = stream.next().await.expect("meta chunk").expect("meta ok");
        match first.payload {
            Some(get_response::Payload::Meta(meta)) => {
                assert_eq!(meta.size, 11);
                assert_eq!(meta.etag.as_deref(), Some("etag-1"));
            }
            other => panic!("unexpected first payload: {other:?}"),
        }

        let second = stream.next().await.expect("data chunk").expect("data ok");
        match second.payload {
            Some(get_response::Payload::Data(bytes)) => assert_eq!(bytes, b"hello world"),
            other => panic!("unexpected second payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn staging_keys_are_listed() {
        let svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        let key = CacheKey::new("docs".into(), "draft.txt".into(), None);
        let xattrs = CacheXattrs {
            bucket: "docs".into(),
            key: "draft.txt".into(),
            version_id: None,
            size: 5,
            expire_at: 0,
            cached_at: now_unix(),
            last_access_at: now_unix(),
            access_count: 0,
            checksum: None,
            content_type: None,
            etag: Some("etag-2".into()),
            category: CacheCategory::Staging,
        };
        svc.put_bytes(key, b"draft".to_vec(), xattrs)
            .await
            .expect("put staging should succeed");

        let listed = svc
            .list_staging_keys(Request::new(ListStagingKeysRequest {
                limit: 10,
                after: None,
            }))
            .await
            .expect("list staging should succeed")
            .into_inner();

        assert_eq!(listed.entries.len(), 1);
        assert_eq!(listed.entries[0].bucket, "docs");
        assert_eq!(listed.entries[0].key, "draft.txt");
    }

    #[tokio::test]
    async fn expired_restored_is_cleaned() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 100;
        let now = now_unix();
        let stale_key = CacheKey::new("docs".into(), "expired.txt".into(), None);
        svc.put_bytes(
            stale_key.clone(),
            b"x".to_vec(),
            CacheXattrs {
                bucket: "docs".into(),
                key: "expired.txt".into(),
                version_id: None,
                size: 1,
                expire_at: now - 1,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("put stale restored");

        svc.reclaim_expired_restored()
            .await
            .expect("reclaim should run");
        let response = svc
            .contains(Request::new(ContainsRequest {
                bucket: "docs".into(),
                key: "expired.txt".into(),
                version_id: None,
            }))
            .await
            .expect("contains after reclaim")
            .into_inner();
        assert!(!response.exists);

        let stats = svc
            .stats(Request::new(()))
            .await
            .expect("stats")
            .into_inner();
        assert_eq!(stats.used_capacity, 0);
        assert!(svc
            .find_entry(&stale_key, CacheCategory::Restored)
            .await
            .is_none());
    }

    #[tokio::test]
    async fn restored_eviction_prefers_lru() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 50;
        svc.eviction_low_watermark = 0.9;
        svc.eviction_policy = EvictionPolicy::Lru;
        svc.eviction_batch_size = 4;
        svc.staging_capacity_bytes = 0;
        svc.restored_capacity_bytes = 50;
        let now = now_unix();

        svc.put_bytes(
            CacheKey::new("docs".into(), "a".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "a".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now - 30,
                last_access_at: now - 30,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore a");
        svc.put_bytes(
            CacheKey::new("docs".into(), "b".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "b".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now - 20,
                last_access_at: now - 20,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore b");
        svc.put_bytes(
            CacheKey::new("docs".into(), "c".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "c".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore c");

        assert!(svc
            .find_entry(
                &CacheKey::new("docs".into(), "a".into(), None),
                CacheCategory::Restored
            )
            .await
            .is_none());
        assert!(svc
            .find_entry(
                &CacheKey::new("docs".into(), "b".into(), None),
                CacheCategory::Restored
            )
            .await
            .is_some());
        assert!(svc
            .find_entry(
                &CacheKey::new("docs".into(), "c".into(), None),
                CacheCategory::Restored
            )
            .await
            .is_some());
        let stats = svc
            .stats(Request::new(()))
            .await
            .expect("stats")
            .into_inner();
        assert_eq!(stats.restored_count, 2);
        assert_eq!(stats.used_capacity, 40);
    }

    #[tokio::test]
    async fn restored_eviction_prefers_lfu() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 50;
        svc.eviction_low_watermark = 0.5;
        svc.eviction_policy = EvictionPolicy::Lfu;
        svc.eviction_batch_size = 4;
        svc.staging_capacity_bytes = 0;
        svc.restored_capacity_bytes = 50;
        let now = now_unix();

        svc.put_bytes(
            CacheKey::new("docs".into(), "a".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "a".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now - 30,
                last_access_at: now - 30,
                access_count: 1,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore a");
        svc.put_bytes(
            CacheKey::new("docs".into(), "b".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "b".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now - 20,
                last_access_at: now - 20,
                access_count: 5,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore b");
        svc.put_bytes(
            CacheKey::new("docs".into(), "c".into(), None),
            vec![0u8; 20],
            CacheXattrs {
                bucket: "docs".into(),
                key: "c".into(),
                version_id: None,
                size: 20,
                expire_at: now + 3600,
                cached_at: now,
                last_access_at: now,
                access_count: 3,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("restore c");

        assert!(svc
            .find_entry(
                &CacheKey::new("docs".into(), "a".into(), None),
                CacheCategory::Restored
            )
            .await
            .is_none());
        assert!(svc
            .find_entry(
                &CacheKey::new("docs".into(), "c".into(), None),
                CacheCategory::Restored
            )
            .await
            .is_some());
    }

    #[tokio::test]
    async fn staging_oversubscription_returns_resource_exhausted() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 4;
        let now = now_unix();

        let result = svc
            .put_bytes(
                CacheKey::new("docs".into(), "overflow.txt".into(), None),
                vec![0u8; 5],
                CacheXattrs {
                    bucket: "docs".into(),
                    key: "overflow.txt".into(),
                    version_id: None,
                    size: 5,
                    expire_at: 0,
                    cached_at: now,
                    last_access_at: now,
                    access_count: 0,
                    checksum: None,
                    content_type: None,
                    etag: None,
                    category: CacheCategory::Staging,
                },
            )
            .await
            .expect_err("staging oversubscription should fail");
        let mapped = CacheServiceImpl::map_capacity_error(result);
        assert_eq!(mapped.code(), Code::ResourceExhausted);
        assert!(mapped
            .message()
            .contains("capacity_reject:incoming_larger_than_capacity"));
    }

    #[tokio::test]
    async fn staging_budget_pressure_does_not_evict_existing_objects() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 100;
        svc.eviction_low_watermark = 1.0;
        svc.eviction_batch_size = 4;
        svc.staging_capacity_bytes = 50;
        svc.restored_capacity_bytes = 50;
        let now = now_unix();

        let restored_key = CacheKey::new("docs".into(), "restored.bin".into(), None);
        svc.put_bytes(
            restored_key.clone(),
            vec![0u8; 30],
            CacheXattrs {
                bucket: "docs".into(),
                key: "restored.bin".into(),
                version_id: None,
                size: 30,
                expire_at: now + 3600,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Restored,
            },
        )
        .await
        .expect("put restored");

        let staging_key = CacheKey::new("docs".into(), "staging.bin".into(), None);
        svc.put_bytes(
            staging_key.clone(),
            vec![0u8; 40],
            CacheXattrs {
                bucket: "docs".into(),
                key: "staging.bin".into(),
                version_id: None,
                size: 40,
                expire_at: 0,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Staging,
            },
        )
        .await
        .expect("put staging");

        let result = svc
            .put_bytes(
                CacheKey::new("docs".into(), "overflow-staging.bin".into(), None),
                vec![0u8; 20],
                CacheXattrs {
                    bucket: "docs".into(),
                    key: "overflow-staging.bin".into(),
                    version_id: None,
                    size: 20,
                    expire_at: 0,
                    cached_at: now,
                    last_access_at: now,
                    access_count: 0,
                    checksum: None,
                    content_type: None,
                    etag: None,
                    category: CacheCategory::Staging,
                },
            )
            .await
            .expect_err("staging budget pressure should reject write");

        let mapped = CacheServiceImpl::map_capacity_error(result);
        assert_eq!(mapped.code(), Code::ResourceExhausted);
        assert!(mapped
            .message()
            .contains("capacity_reject:staging_budget_exceeded"));
        assert!(svc
            .find_entry(&staging_key, CacheCategory::Staging)
            .await
            .is_some());
        assert!(svc
            .find_entry(&restored_key, CacheCategory::Restored)
            .await
            .is_some());
        let stats = svc
            .stats(Request::new(()))
            .await
            .expect("stats")
            .into_inner();
        assert_eq!(stats.evict_count, 0);
    }

    #[tokio::test]
    async fn failed_staging_overwrite_preserves_existing_object() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 100;
        svc.eviction_low_watermark = 1.0;
        svc.staging_capacity_bytes = 80;
        svc.restored_capacity_bytes = 20;
        let now = now_unix();
        let key = CacheKey::new("docs".into(), "staging.bin".into(), None);

        svc.put_bytes(
            key.clone(),
            vec![1u8; 80],
            CacheXattrs {
                bucket: "docs".into(),
                key: "staging.bin".into(),
                version_id: None,
                size: 80,
                expire_at: 0,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: Some("old".into()),
                content_type: None,
                etag: None,
                category: CacheCategory::Staging,
            },
        )
        .await
        .expect("initial staging write");

        let result = svc
            .put_bytes(
                key.clone(),
                vec![2u8; 90],
                CacheXattrs {
                    bucket: "docs".into(),
                    key: "staging.bin".into(),
                    version_id: None,
                    size: 90,
                    expire_at: 0,
                    cached_at: now + 1,
                    last_access_at: now + 1,
                    access_count: 0,
                    checksum: Some("new".into()),
                    content_type: None,
                    etag: None,
                    category: CacheCategory::Staging,
                },
            )
            .await
            .expect_err("overwrite should be rejected by staging budget");

        let mapped = CacheServiceImpl::map_capacity_error(result);
        assert_eq!(mapped.code(), Code::ResourceExhausted);
        assert!(mapped
            .message()
            .contains("capacity_reject:staging_budget_exceeded"));

        let entry = svc
            .find_entry(&key, CacheCategory::Staging)
            .await
            .expect("old staging entry must remain");
        assert_eq!(entry.xattrs.size, 80);
        assert_eq!(entry.xattrs.checksum.as_deref(), Some("old"));
    }

    #[tokio::test]
    async fn restored_write_pressure_does_not_evict_staging_objects() {
        let mut svc = CacheServiceImpl::new(&test_config())
            .await
            .expect("service init");
        svc.max_size_bytes = 100;
        svc.eviction_low_watermark = 1.0;
        svc.eviction_batch_size = 4;
        svc.staging_capacity_bytes = 80;
        svc.restored_capacity_bytes = 20;
        let now = now_unix();

        let staging_key = CacheKey::new("docs".into(), "protected-staging.bin".into(), None);
        svc.put_bytes(
            staging_key.clone(),
            vec![0u8; 80],
            CacheXattrs {
                bucket: "docs".into(),
                key: "protected-staging.bin".into(),
                version_id: None,
                size: 80,
                expire_at: 0,
                cached_at: now,
                last_access_at: now,
                access_count: 0,
                checksum: None,
                content_type: None,
                etag: None,
                category: CacheCategory::Staging,
            },
        )
        .await
        .expect("put staging");

        let result = svc
            .put_bytes(
                CacheKey::new("docs".into(), "oversized-restored.bin".into(), None),
                vec![0u8; 30],
                CacheXattrs {
                    bucket: "docs".into(),
                    key: "oversized-restored.bin".into(),
                    version_id: None,
                    size: 30,
                    expire_at: now + 3600,
                    cached_at: now,
                    last_access_at: now,
                    access_count: 0,
                    checksum: None,
                    content_type: None,
                    etag: None,
                    category: CacheCategory::Restored,
                },
            )
            .await
            .expect_err("restored pressure should not evict staging");

        let mapped = CacheServiceImpl::map_capacity_error(result);
        assert_eq!(mapped.code(), Code::ResourceExhausted);
        assert!(mapped
            .message()
            .contains("capacity_reject:restored_budget_exceeded"));
        assert!(svc
            .find_entry(&staging_key, CacheCategory::Staging)
            .await
            .is_some());
        let stats = svc
            .stats(Request::new(()))
            .await
            .expect("stats")
            .into_inner();
        assert_eq!(stats.evict_count, 0);
    }
}
