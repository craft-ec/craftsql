//! CraftOBJ-backed PageStore — bridges CraftSQL to the CraftOBJ P2P network.
//!
//! Local-first: disk cache under a configurable directory. Pages are immutable
//! CIDs cached forever once fetched. Root pointer is fetched/published via DHT.
//!
//! Network operations are abstracted behind [`NetworkBackend`] so the real
//! CraftOBJ client can be wired in later, while tests use a mock.

use craftsql_core::{Cid, Page, PageStore, PageStoreError, Result};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{atomic::{AtomicU64, Ordering}, Mutex};

// ---------------------------------------------------------------------------
// NetworkBackend trait
// ---------------------------------------------------------------------------

/// Abstraction over CraftOBJ network operations.
/// Implement this to wire in the real P2P client.
pub trait NetworkBackend: Send + Sync {
    /// Publish page data to the network, returns its CID.
    fn publish_page(&self, data: &[u8]) -> Result<Cid>;

    /// Fetch page data by CID from the network.
    fn fetch_page(&self, cid: &Cid) -> Result<Vec<u8>>;

    /// Get the current root pointer CID from the DHT.
    fn get_root(&self) -> Result<Option<Cid>>;

    /// Set the root pointer CID in the DHT.
    fn set_root(&self, cid: Cid) -> Result<()>;

    /// Get a named root pointer from the DHT.
    fn get_named_root(&self, name: &str) -> Result<Option<Cid>>;

    /// Set a named root pointer in the DHT.
    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()>;

    /// Remove a named root pointer from the DHT.
    fn remove_named_root(&self, name: &str) -> Result<bool>;

    /// List all named root pointers from the DHT.
    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>>;
}

// ---------------------------------------------------------------------------
// CraftObjPageStore
// ---------------------------------------------------------------------------

/// Stats for observability.
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }
}

/// CraftOBJ-backed PageStore with local disk cache.
pub struct CraftObjPageStore<N: NetworkBackend> {
    cache_dir: PathBuf,
    network: N,
    pub stats: CacheStats,
}

impl<N: NetworkBackend> CraftObjPageStore<N> {
    /// Create a new store. `cache_dir` is the local disk cache directory.
    pub fn new(cache_dir: &Path, network: N) -> Result<Self> {
        fs::create_dir_all(cache_dir.join("pages"))?;
        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            network,
            stats: CacheStats::new(),
        })
    }

    fn page_path(&self, cid: &Cid) -> PathBuf {
        self.cache_dir.join("pages").join(hex::encode(cid.0))
    }

    fn root_path(&self) -> PathBuf {
        self.cache_dir.join("root")
    }

    fn refs_dir(&self) -> PathBuf {
        self.cache_dir.join("refs")
    }

    fn ref_path(&self, name: &str) -> PathBuf {
        let safe: String = name.chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
            .collect();
        self.refs_dir().join(safe)
    }

    fn read_cid_file(path: &Path) -> Result<Option<Cid>> {
        if !path.exists() {
            return Ok(None);
        }
        let hex_str = fs::read_to_string(path)?;
        let bytes = hex::decode(hex_str.trim())
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        if bytes.len() != 32 {
            return Err(PageStoreError::Storage("invalid CID length".into()));
        }
        let mut cid = [0u8; 32];
        cid.copy_from_slice(&bytes);
        Ok(Some(Cid(cid)))
    }

    /// Check if a page is cached locally.
    pub fn is_cached(&self, cid: &Cid) -> bool {
        self.page_path(cid).exists()
    }

    /// Access the network backend.
    pub fn network(&self) -> &N {
        &self.network
    }
}

impl<N: NetworkBackend> PageStore for CraftObjPageStore<N> {
    fn get(&self, cid: &Cid) -> Result<Page> {
        let path = self.page_path(cid);

        // Local cache hit
        if let Ok(data) = fs::read(&path) {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Page { data });
        }

        // Cache miss → fetch from network
        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        let data = self.network.fetch_page(cid)?;

        // Verify CID
        let actual = Cid::from_bytes(&data);
        if actual != *cid {
            return Err(PageStoreError::Storage(format!(
                "CID mismatch: expected {}, got {}", cid, actual
            )));
        }

        // Cache locally (immutable, cache forever)
        fs::write(&path, &data)?;

        Ok(Page { data })
    }

    fn put(&self, page: &Page) -> Result<Cid> {
        let cid = Cid::from_bytes(&page.data);
        let path = self.page_path(&cid);

        // Write to local cache
        if !path.exists() {
            fs::write(&path, &page.data)?;
        }

        // Publish to network
        let net_cid = self.network.publish_page(&page.data)?;
        debug_assert_eq!(cid, net_cid);

        Ok(cid)
    }

    fn current_root(&self) -> Result<Option<Cid>> {
        // Try network first for freshness
        match self.network.get_root() {
            Ok(Some(cid)) => {
                // Cache locally
                let _ = fs::write(self.root_path(), hex::encode(cid.0));
                Ok(Some(cid))
            }
            Ok(None) => {
                // Fall back to local cache
                Self::read_cid_file(&self.root_path())
            }
            Err(_) => {
                // Network error, fall back to local
                Self::read_cid_file(&self.root_path())
            }
        }
    }

    fn update_root(&self, new_root: Cid) -> Result<()> {
        // Write local first
        fs::write(self.root_path(), hex::encode(new_root.0))?;
        // Publish to DHT
        self.network.set_root(new_root)?;
        Ok(())
    }

    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()> {
        fs::create_dir_all(self.refs_dir())?;
        fs::write(self.ref_path(name), hex::encode(cid.0))?;
        self.network.set_named_root(name, cid)?;
        Ok(())
    }

    fn get_named_root(&self, name: &str) -> Result<Option<Cid>> {
        // Try network, fall back to local
        match self.network.get_named_root(name) {
            Ok(Some(cid)) => {
                let _ = fs::create_dir_all(self.refs_dir());
                let _ = fs::write(self.ref_path(name), hex::encode(cid.0));
                Ok(Some(cid))
            }
            Ok(None) => Self::read_cid_file(&self.ref_path(name)),
            Err(_) => Self::read_cid_file(&self.ref_path(name)),
        }
    }

    fn remove_named_root(&self, name: &str) -> Result<bool> {
        let local_removed = self.ref_path(name).exists() && fs::remove_file(self.ref_path(name)).is_ok();
        let net_removed = self.network.remove_named_root(name).unwrap_or(false);
        Ok(local_removed || net_removed)
    }

    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>> {
        // Prefer network list, fall back to local
        match self.network.list_named_roots() {
            Ok(roots) => Ok(roots),
            Err(_) => {
                let mut result = Vec::new();
                if let Ok(entries) = fs::read_dir(self.refs_dir()) {
                    for entry in entries.flatten() {
                        if let Some(name) = entry.file_name().to_str() {
                            if let Ok(Some(cid)) = Self::read_cid_file(&entry.path()) {
                                result.push((name.to_string(), cid));
                            }
                        }
                    }
                }
                result.sort_by(|a, b| a.0.cmp(&b.0));
                Ok(result)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Mock NetworkBackend for tests
// ---------------------------------------------------------------------------

/// In-memory mock backend for testing.
pub struct MockNetworkBackend {
    pages: Mutex<HashMap<Cid, Vec<u8>>>,
    root: Mutex<Option<Cid>>,
    named_roots: Mutex<HashMap<String, Cid>>,
    pub fetch_count: AtomicU64,
    pub publish_count: AtomicU64,
}

impl MockNetworkBackend {
    pub fn new() -> Self {
        Self {
            pages: Mutex::new(HashMap::new()),
            root: Mutex::new(None),
            named_roots: Mutex::new(HashMap::new()),
            fetch_count: AtomicU64::new(0),
            publish_count: AtomicU64::new(0),
        }
    }
}

impl Default for MockNetworkBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl NetworkBackend for MockNetworkBackend {
    fn publish_page(&self, data: &[u8]) -> Result<Cid> {
        let cid = Cid::from_bytes(data);
        self.pages.lock().unwrap().insert(cid, data.to_vec());
        self.publish_count.fetch_add(1, Ordering::Relaxed);
        Ok(cid)
    }

    fn fetch_page(&self, cid: &Cid) -> Result<Vec<u8>> {
        self.fetch_count.fetch_add(1, Ordering::Relaxed);
        self.pages.lock().unwrap()
            .get(cid)
            .cloned()
            .ok_or(PageStoreError::NotFound(*cid))
    }

    fn get_root(&self) -> Result<Option<Cid>> {
        Ok(*self.root.lock().unwrap())
    }

    fn set_root(&self, cid: Cid) -> Result<()> {
        *self.root.lock().unwrap() = Some(cid);
        Ok(())
    }

    fn get_named_root(&self, name: &str) -> Result<Option<Cid>> {
        Ok(self.named_roots.lock().unwrap().get(name).copied())
    }

    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()> {
        self.named_roots.lock().unwrap().insert(name.to_string(), cid);
        Ok(())
    }

    fn remove_named_root(&self, name: &str) -> Result<bool> {
        Ok(self.named_roots.lock().unwrap().remove(name).is_some())
    }

    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>> {
        let roots = self.named_roots.lock().unwrap();
        let mut result: Vec<_> = roots.iter().map(|(k, &v)| (k.clone(), v)).collect();
        result.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_store(dir: &Path) -> CraftObjPageStore<MockNetworkBackend> {
        CraftObjPageStore::new(dir, MockNetworkBackend::new()).unwrap()
    }

    #[test]
    fn test_read_write_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let page = Page { data: b"hello craftsql".to_vec() };
        let cid = store.put(&page).unwrap();
        let got = store.get(&cid).unwrap();
        assert_eq!(got.data, page.data);
    }

    #[test]
    fn test_cache_hit_no_network_call() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let page = Page { data: b"cache me".to_vec() };
        let cid = store.put(&page).unwrap();

        // First read — should be cache hit (put already cached)
        let _ = store.get(&cid).unwrap();
        assert_eq!(store.stats.hits.load(Ordering::Relaxed), 1);
        assert_eq!(store.stats.misses.load(Ordering::Relaxed), 0);

        // Second read — still cache hit
        let _ = store.get(&cid).unwrap();
        assert_eq!(store.stats.hits.load(Ordering::Relaxed), 2);

        // Network fetch should not have been called
        assert_eq!(store.network.fetch_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_cache_miss_fetches_from_network() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Inject data directly into the mock network (bypass cache)
        let data = b"network only data";
        let cid = Cid::from_bytes(data);
        store.network.pages.lock().unwrap().insert(cid, data.to_vec());

        // First read — cache miss, fetches from network
        let got = store.get(&cid).unwrap();
        assert_eq!(got.data, data);
        assert_eq!(store.stats.misses.load(Ordering::Relaxed), 1);
        assert_eq!(store.network.fetch_count.load(Ordering::Relaxed), 1);

        // Second read — cache hit, no network call
        let _ = store.get(&cid).unwrap();
        assert_eq!(store.stats.hits.load(Ordering::Relaxed), 1);
        assert_eq!(store.network.fetch_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_root_get_set() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        assert_eq!(store.current_root().unwrap(), None);

        let cid = Cid::from_bytes(b"root page");
        store.update_root(cid).unwrap();

        assert_eq!(store.current_root().unwrap(), Some(cid));
    }

    #[test]
    fn test_named_roots() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let cid1 = Cid::from_bytes(b"snap1");
        let cid2 = Cid::from_bytes(b"snap2");

        store.set_named_root("v1", cid1).unwrap();
        store.set_named_root("v2", cid2).unwrap();

        assert_eq!(store.get_named_root("v1").unwrap(), Some(cid1));
        assert_eq!(store.get_named_root("v2").unwrap(), Some(cid2));

        let roots = store.list_named_roots().unwrap();
        assert_eq!(roots.len(), 2);

        assert!(store.remove_named_root("v1").unwrap());
        assert_eq!(store.get_named_root("v1").unwrap(), None);
    }

    #[test]
    fn test_concurrent_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Arc::new(make_store(tmp.path()));

        // Put a page
        let page = Page { data: b"concurrent data".to_vec() };
        let cid = store.put(&page).unwrap();

        // Spawn multiple readers
        let mut handles = Vec::new();
        for _ in 0..8 {
            let s = Arc::clone(&store);
            let c = cid;
            handles.push(std::thread::spawn(move || {
                let got = s.get(&c).unwrap();
                assert_eq!(got.data, b"concurrent data");
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // All should be cache hits (page was cached by put)
        assert_eq!(store.stats.hits.load(Ordering::Relaxed), 8);
        assert_eq!(store.network.fetch_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_put_publishes_to_network() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let page = Page { data: b"publish me".to_vec() };
        let cid = store.put(&page).unwrap();

        assert_eq!(store.network.publish_count.load(Ordering::Relaxed), 1);
        assert!(store.network.pages.lock().unwrap().contains_key(&cid));
    }

    #[test]
    fn test_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let cid = Cid::from_bytes(b"nonexistent");
        assert!(matches!(store.get(&cid), Err(PageStoreError::NotFound(_))));
    }
}
