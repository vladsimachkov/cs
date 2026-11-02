use crate::backend::{CacheBackend, CacheCategory, CacheXattrs, StreamWrite};
use anyhow::Result;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::hdd::HddBackend;

pub struct SpdkBackend {
    _config_file: String,
    _bdev_name: String,
    inner: Arc<HddBackend>,
}

impl SpdkBackend {
    pub async fn new(config_file: String, bdev_name: String, max_size_gb: u64) -> Result<Self> {
        let root = Path::new(&config_file)
            .join("spdk")
            .join(sanitize_bdev_name(&bdev_name));
        let inner =
            Arc::new(HddBackend::new(root.to_string_lossy().to_string(), max_size_gb).await?);
        Ok(Self {
            _config_file: config_file,
            _bdev_name: bdev_name,
            inner,
        })
    }
}

#[tonic::async_trait]
impl CacheBackend for SpdkBackend {
    async fn write(&self, cache_key: &str, data: &[u8], xattrs: &CacheXattrs) -> Result<u64> {
        self.inner.write(cache_key, data, xattrs).await
    }

    async fn begin_stream(&self, category: CacheCategory) -> Result<StreamWrite> {
        self.inner.begin_stream(category).await
    }

    async fn append_stream(&self, write: &StreamWrite, data: &[u8]) -> Result<()> {
        self.inner.append_stream(write, data).await
    }

    async fn commit_stream(&self, write: &StreamWrite, xattrs: &CacheXattrs) -> Result<u64> {
        self.inner.commit_stream(write, xattrs).await
    }

    async fn abort_stream(&self, write: &StreamWrite) -> Result<()> {
        self.inner.abort_stream(write).await
    }

    async fn read(&self, storage_id: u64) -> Result<Vec<u8>> {
        self.inner.read(storage_id).await
    }

    async fn delete(&self, storage_id: u64) -> Result<()> {
        self.inner.delete(storage_id).await
    }

    async fn read_xattrs(&self, storage_id: u64) -> Result<CacheXattrs> {
        self.inner.read_xattrs(storage_id).await
    }

    async fn list_all(&self) -> Result<Vec<(u64, CacheXattrs)>> {
        self.inner.list_all().await
    }

    async fn available_bytes(&self) -> Result<u64> {
        self.inner.available_bytes().await
    }
}

fn sanitize_bdev_name(name: &str) -> PathBuf {
    let mut value = PathBuf::new();
    let safe = name
        .chars()
        .map(|ch| match ch {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => ch,
            _ => '_',
        })
        .collect::<String>();
    value.push(safe);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_bdev_replaces_special_chars() {
        assert_eq!(
            sanitize_bdev_name("nvme0n1p1@ctrl"),
            PathBuf::from("nvme0n1p1_ctrl")
        );
    }
}
