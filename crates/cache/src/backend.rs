use anyhow::Result;
use std::path::PathBuf;

/// An in-progress streaming write.
///
/// The bytes live at `temp_path`, outside the published object namespace, and become
/// visible to readers only when [`CacheBackend::commit_stream`] renames them into
/// place. A stream that is never committed leaves at most the temp file behind, which
/// [`CacheBackend::abort_stream`] removes.
pub struct StreamWrite {
    /// The storage id the object will get if the stream is committed.
    pub storage_id: u64,
    /// Where the bytes are accumulating. Not readable as an object until commit.
    pub temp_path: PathBuf,
}

/// 缓存后端抽象 trait
///
/// 当前实现: HddBackend (机械硬盘)
/// 目标实现: SpdkBlobBackend (SPDK Blobstore on NVMe)
#[tonic::async_trait]
pub trait CacheBackend: Send + Sync + 'static {
    /// Write a whole object in one call and return its internal storage ID.
    ///
    /// Buffers the full object; prefer the streaming methods for anything that
    /// arrives in pieces, since they hold at most one chunk in memory.
    async fn write(&self, cache_key: &str, data: &[u8], xattrs: &CacheXattrs) -> Result<u64>;

    /// Start a streaming write: allocate a storage id and a temp file for it.
    ///
    /// The caller must end the stream with exactly one of `commit_stream` or
    /// `abort_stream`; an abandoned stream leaks a temp file.
    async fn begin_stream(&self, category: CacheCategory) -> Result<StreamWrite>;

    /// Append one chunk to an in-progress stream. Memory cost is this chunk only.
    async fn append_stream(&self, write: &StreamWrite, data: &[u8]) -> Result<()>;

    /// Publish an in-progress stream under its storage id with its xattrs.
    async fn commit_stream(&self, write: &StreamWrite, xattrs: &CacheXattrs) -> Result<u64>;

    /// Discard an in-progress stream. Idempotent: a missing temp file is not an error.
    async fn abort_stream(&self, write: &StreamWrite) -> Result<()>;

    /// 读取对象数据
    async fn read(&self, storage_id: u64) -> Result<Vec<u8>>;

    /// 删除对象
    async fn delete(&self, storage_id: u64) -> Result<()>;

    /// 读取 xattrs
    async fn read_xattrs(&self, storage_id: u64) -> Result<CacheXattrs>;

    /// 列出所有存储对象（启动时重建索引用）
    async fn list_all(&self) -> Result<Vec<(u64, CacheXattrs)>>;

    /// 可用容量
    async fn available_bytes(&self) -> Result<u64>;
}

/// 缓存对象扩展属性
#[derive(Debug, Clone)]
pub struct CacheXattrs {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
    pub size: u64,
    pub expire_at: i64,
    pub cached_at: i64,
    pub last_access_at: i64,
    pub access_count: u64,
    pub checksum: Option<String>,
    pub content_type: Option<String>,
    pub etag: Option<String>,
    /// staging = 暂存数据 (PutObject), restored = 解冻数据
    pub category: CacheCategory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheCategory {
    Staging,
    Restored,
}
