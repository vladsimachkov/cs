use crate::backend::{CacheBackend, CacheCategory, CacheXattrs, StreamWrite};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::debug;

pub struct HddBackend {
    base_path: PathBuf,
    max_size_bytes: u64,
    next_id: AtomicU64,
}

impl HddBackend {
    pub async fn new(base_path: String, max_size_gb: u64) -> Result<Self> {
        let base = PathBuf::from(&base_path);
        fs::create_dir_all(base.join("staging")).await?;
        fs::create_dir_all(base.join("restored")).await?;
        fs::create_dir_all(base.join("meta")).await?;
        let next_id = discover_next_id(&base).await?;
        Ok(Self {
            base_path: base,
            max_size_bytes: max_size_gb * 1024 * 1024 * 1024,
            next_id: AtomicU64::new(next_id),
        })
    }

    fn data_path(&self, id: u64, cat: CacheCategory) -> PathBuf {
        let dir = match cat {
            CacheCategory::Staging => "staging",
            CacheCategory::Restored => "restored",
        };
        self.base_path.join(dir).join(format!("{id}.dat"))
    }

    fn meta_path(&self, id: u64) -> PathBuf {
        self.base_path.join("meta").join(format!("{id}.json"))
    }

    /// The temp path a streaming write for `id` accumulates into. Shares the object's
    /// directory so the commit is a same-filesystem rename, never a copy.
    fn temp_path(&self, id: u64, cat: CacheCategory) -> PathBuf {
        self.data_path(id, cat).with_extension("part")
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct XattrsJson {
    bucket: String,
    key: String,
    version_id: Option<String>,
    size: u64,
    expire_at: i64,
    cached_at: i64,
    last_access_at: Option<i64>,
    access_count: Option<u64>,
    checksum: Option<String>,
    content_type: Option<String>,
    etag: Option<String>,
    category: String,
}

fn to_json(x: &CacheXattrs) -> XattrsJson {
    XattrsJson {
        bucket: x.bucket.clone(),
        key: x.key.clone(),
        version_id: x.version_id.clone(),
        size: x.size,
        expire_at: x.expire_at,
        cached_at: x.cached_at,
        last_access_at: Some(x.last_access_at),
        access_count: Some(x.access_count),
        checksum: x.checksum.clone(),
        content_type: x.content_type.clone(),
        etag: x.etag.clone(),
        category: match x.category {
            CacheCategory::Staging => "staging".into(),
            CacheCategory::Restored => "restored".into(),
        },
    }
}

fn from_json(j: &XattrsJson) -> CacheXattrs {
    CacheXattrs {
        bucket: j.bucket.clone(),
        key: j.key.clone(),
        version_id: j.version_id.clone(),
        size: j.size,
        expire_at: j.expire_at,
        cached_at: j.cached_at,
        last_access_at: j.last_access_at.unwrap_or(j.cached_at),
        access_count: j.access_count.unwrap_or(0),
        checksum: j.checksum.clone(),
        content_type: j.content_type.clone(),
        etag: j.etag.clone(),
        category: if j.category == "staging" {
            CacheCategory::Staging
        } else {
            CacheCategory::Restored
        },
    }
}

#[tonic::async_trait]
impl CacheBackend for HddBackend {
    async fn write(&self, _key: &str, data: &[u8], xattrs: &CacheXattrs) -> Result<u64> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        fs::write(self.data_path(id, xattrs.category), data).await?;
        fs::write(self.meta_path(id), serde_json::to_vec(&to_json(xattrs))?).await?;
        debug!(id, size = data.len(), "HDD write ok");
        Ok(id)
    }

    async fn begin_stream(&self, category: CacheCategory) -> Result<StreamWrite> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let temp_path = self.temp_path(id, category);
        // Create it now so a later append failure means "the object exists and is short",
        // not "the object never existed".
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .await?;
        debug!(id, ?temp_path, "HDD stream begin");
        Ok(StreamWrite {
            storage_id: id,
            temp_path,
        })
    }

    async fn append_stream(&self, write: &StreamWrite, data: &[u8]) -> Result<()> {
        // O_APPEND per chunk: one open per chunk, which at 1 MiB chunks is noise next to
        // the write itself, and it means no file handle is held across await points in the
        // caller's loop.
        let mut file = OpenOptions::new()
            .append(true)
            .open(&write.temp_path)
            .await?;
        file.write_all(data).await?;
        file.flush().await?;
        Ok(())
    }

    async fn commit_stream(&self, write: &StreamWrite, xattrs: &CacheXattrs) -> Result<u64> {
        let id = write.storage_id;
        // Data first, then xattrs. A crash in between leaks one orphan `.dat` (readers
        // enumerate `meta/`, so it is invisible and merely wastes space); the reverse
        // order would publish a `meta/` entry whose data file is missing, which reads
        // as a corrupt object.
        fs::rename(&write.temp_path, self.data_path(id, xattrs.category)).await?;
        fs::write(self.meta_path(id), serde_json::to_vec(&to_json(xattrs))?).await?;
        debug!(id, size = xattrs.size, "HDD stream commit");
        Ok(id)
    }

    async fn abort_stream(&self, write: &StreamWrite) -> Result<()> {
        match fs::remove_file(&write.temp_path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn read(&self, id: u64) -> Result<Vec<u8>> {
        let x = self.read_xattrs(id).await?;
        Ok(fs::read(self.data_path(id, x.category)).await?)
    }

    async fn delete(&self, id: u64) -> Result<()> {
        let x = self.read_xattrs(id).await?;
        let _ = fs::remove_file(self.data_path(id, x.category)).await;
        let _ = fs::remove_file(self.meta_path(id)).await;
        Ok(())
    }

    async fn read_xattrs(&self, id: u64) -> Result<CacheXattrs> {
        let raw = fs::read(self.meta_path(id)).await?;
        let j: XattrsJson = serde_json::from_slice(&raw)?;
        Ok(from_json(&j))
    }

    async fn list_all(&self) -> Result<Vec<(u64, CacheXattrs)>> {
        let mut out = Vec::new();
        let mut rd = fs::read_dir(self.base_path.join("meta")).await?;
        while let Some(e) = rd.next_entry().await? {
            let n = e.file_name();
            let n = n.to_string_lossy();
            if let Some(s) = n.strip_suffix(".json") {
                if let Ok(id) = s.parse::<u64>() {
                    if let Ok(x) = self.read_xattrs(id).await {
                        out.push((id, x));
                    }
                }
            }
        }
        Ok(out)
    }

    async fn available_bytes(&self) -> Result<u64> {
        let used_bytes: u64 = self
            .list_all()
            .await?
            .into_iter()
            .map(|(_, x)| x.size)
            .sum();
        Ok(self.max_size_bytes.saturating_sub(used_bytes))
    }
}

async fn discover_next_id(base: &std::path::Path) -> Result<u64> {
    let mut max_id = 0_u64;
    let mut rd = fs::read_dir(base.join("meta")).await?;
    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(raw) = name.strip_suffix(".json") {
            if let Ok(id) = raw.parse::<u64>() {
                max_id = max_id.max(id);
            }
        }
    }
    Ok(max_id + 1)
}
