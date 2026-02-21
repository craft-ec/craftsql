//! CraftOBJ-backed PageStore — bridges CraftSQL to the CraftOBJ P2P network.
//!
//! Local-first: disk cache under a configurable directory. Pages are immutable
//! CIDs cached forever once fetched. Root pointer is fetched/published via DHT.
//!
//! ## Page Bundling
//!
//! SQLite pages are 4KB but CraftOBJ deals in 100KB pieces / 10MB segments.
//! Publishing each page individually would be massive overhead. Instead:
//!
//! - `put()` writes to **local cache only** (no network publish)
//! - `update_root()` **bundles ALL pages** into a single blob, publishes as one
//!   CraftOBJ content, and stores the CraftOBJ CID as the root pointer
//! - `get()` on cache miss fetches the **entire bundle** from the root CID,
//!   unpacks all pages into local cache, then serves from cache
//!
//! Bundle format:
//! ```text
//! [magic: 4 bytes "CSQL"]
//! [version: u16 LE]
//! [page_size: u32 LE]
//! [page_count: u32 LE]
//! [page_table: bincode-serialized PageTable]
//! [page_table_len: u32 LE]  (length of serialized page table)
//! [page data: page_count × page_size bytes]
//! ```
//!
//! Network operations are abstracted behind [`NetworkBackend`] so the real
//! CraftOBJ client can be wired in later, while tests use a mock.

use craftsql_core::{Cid, Page, PageStore, PageStoreError, PageTable, Result};
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

/// Bundle magic bytes.
const BUNDLE_MAGIC: &[u8; 4] = b"CSQL";
/// Bundle format version.
const BUNDLE_VERSION: u16 = 1;

/// CraftOBJ-backed PageStore with local disk cache.
///
/// Pages are cached locally and only published as a bundle on `update_root()`.
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

    /// Bundle all pages referenced by the given page table into a single blob.
    ///
    /// All pages must be in the local cache. Returns the bundle bytes.
    fn bundle_pages(&self, page_table: &PageTable, page_size: u32) -> Result<Vec<u8>> {
        let page_count = page_table.len() as u32;
        let pt_bytes = page_table.to_bytes();
        let pt_len = pt_bytes.len() as u32;

        // Header: magic(4) + version(2) + page_size(4) + page_count(4) + pt_bytes + pt_len(4)
        let header_size = 4 + 2 + 4 + 4 + pt_bytes.len() + 4;
        let total_size = header_size + (page_count as usize) * (page_size as usize);
        let mut buf = Vec::with_capacity(total_size);

        buf.extend_from_slice(BUNDLE_MAGIC);
        buf.extend_from_slice(&BUNDLE_VERSION.to_le_bytes());
        buf.extend_from_slice(&page_size.to_le_bytes());
        buf.extend_from_slice(&page_count.to_le_bytes());
        buf.extend_from_slice(&pt_bytes);
        buf.extend_from_slice(&pt_len.to_le_bytes());

        // Append page data in order
        for i in 0..page_count as usize {
            if let Some(cid) = page_table.get(i) {
                let path = self.page_path(cid);
                let data = fs::read(&path).map_err(|e| {
                    PageStoreError::Storage(format!("read cached page {}: {}", cid, e))
                })?;
                buf.extend_from_slice(&data);
                // Pad to page_size if shorter
                if data.len() < page_size as usize {
                    buf.resize(buf.len() + page_size as usize - data.len(), 0);
                }
            } else {
                // Empty page slot — write zeros
                buf.extend(std::iter::repeat(0u8).take(page_size as usize));
            }
        }

        Ok(buf)
    }

    /// Unbundle a blob into individual pages, caching them locally.
    /// Returns the PageTable from the bundle.
    fn unbundle_pages(&self, data: &[u8]) -> Result<PageTable> {
        if data.len() < 18 {
            return Err(PageStoreError::Storage("bundle too small".into()));
        }
        if &data[0..4] != BUNDLE_MAGIC {
            return Err(PageStoreError::Storage("invalid bundle magic".into()));
        }
        let version = u16::from_le_bytes([data[4], data[5]]);
        if version != BUNDLE_VERSION {
            return Err(PageStoreError::Storage(format!("unsupported bundle version {}", version)));
        }
        let page_size = u32::from_le_bytes([data[6], data[7], data[8], data[9]]) as usize;
        let page_count = u32::from_le_bytes([data[10], data[11], data[12], data[13]]) as usize;

        // Read page table: it's right after the fixed header (offset 14), and its length
        // is stored as u32 LE right after the page table bytes.
        // We need to figure out where the page table ends. The pt_len is stored at
        // offset 14 + pt_len. We can find it by computing: total = header + pages
        // header = 14 + pt_len + 4, so pt_len = data.len() - 14 - 4 - page_count * page_size
        let pages_total = page_count * page_size;
        if data.len() < 18 + pages_total {
            return Err(PageStoreError::Storage("bundle data too short".into()));
        }
        let pt_len_offset = data.len() - pages_total - 4;
        let pt_len = u32::from_le_bytes([
            data[pt_len_offset], data[pt_len_offset + 1],
            data[pt_len_offset + 2], data[pt_len_offset + 3],
        ]) as usize;

        let pt_start = 14;
        let pt_end = pt_start + pt_len;
        if pt_end > pt_len_offset {
            return Err(PageStoreError::Storage("invalid page table length".into()));
        }

        let page_table = PageTable::from_bytes(&data[pt_start..pt_end])
            .map_err(|e| PageStoreError::Storage(format!("parse page table: {}", e)))?;

        // Extract and cache each page
        let pages_start = pt_len_offset + 4;
        for i in 0..page_count {
            let offset = pages_start + i * page_size;
            let end = offset + page_size;
            if end > data.len() {
                return Err(PageStoreError::Storage("bundle truncated".into()));
            }
            let page_data = &data[offset..end];
            let cid = Cid::from_bytes(page_data);
            let path = self.page_path(&cid);
            if !path.exists() {
                fs::write(&path, page_data)?;
            }
        }

        // Also cache the page table itself as a page (for VFS compatibility)
        let pt_data = page_table.to_bytes();
        let pt_cid = Cid::from_bytes(&pt_data);
        let pt_path = self.page_path(&pt_cid);
        if !pt_path.exists() {
            fs::write(&pt_path, &pt_data)?;
        }

        Ok(page_table)
    }

    /// Fetch the bundle from the network using the root CID, unbundle into cache.
    /// The root CID points to the bundle content in CraftOBJ.
    fn fetch_and_unbundle(&self, bundle_cid: &Cid) -> Result<PageTable> {
        let data = self.network.fetch_page(bundle_cid)?;
        self.unbundle_pages(&data)
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

        // Cache miss → try fetching the whole bundle from root CID.
        // Individual pages aren't published to CraftOBJ; only bundles are.
        self.stats.misses.fetch_add(1, Ordering::Relaxed);

        // Try to get the root (bundle CID) and unbundle everything
        if let Ok(Some(root_cid)) = self.current_root() {
            // Check if we already have the bundle cached as a page
            if !self.page_path(&root_cid).exists() {
                // Fetch bundle from network and unpack all pages into cache
                let _ = self.fetch_and_unbundle(&root_cid);
            }
        }

        // Retry from cache after unbundling
        let path = self.page_path(cid);
        if let Ok(data) = fs::read(&path) {
            return Ok(Page { data });
        }

        // Last resort: try direct network fetch (for backwards compat / non-bundled pages)
        match self.network.fetch_page(cid) {
            Ok(data) => {
                let actual = Cid::from_bytes(&data);
                if actual != *cid {
                    return Err(PageStoreError::Storage(format!(
                        "CID mismatch: expected {}, got {}", cid, actual
                    )));
                }
                fs::write(&path, &data)?;
                Ok(Page { data })
            }
            Err(e) => Err(e),
        }
    }

    fn put(&self, page: &Page) -> Result<Cid> {
        let cid = Cid::from_bytes(&page.data);
        let path = self.page_path(&cid);

        // Write to local cache only — no network publish.
        // Pages are bundled and published as a single CraftOBJ content on update_root().
        if !path.exists() {
            fs::write(&path, &page.data)?;
        }

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
        // The new_root CID points to the page table (from VFS sync).
        // We need to:
        // 1. Load the page table from local cache
        // 2. Bundle all pages into a single blob
        // 3. Publish the bundle as one CraftOBJ content
        // 4. Store the bundle CID as the root

        // Read the page table from local cache
        let pt_path = self.page_path(&new_root);
        let pt_data = fs::read(&pt_path).map_err(|e| {
            PageStoreError::Storage(format!("read page table for bundling: {}", e))
        })?;
        let page_table = PageTable::from_bytes(&pt_data)
            .map_err(|e| PageStoreError::Storage(format!("parse page table: {}", e)))?;

        // Detect page size from first page
        let page_size = if page_table.len() > 0 {
            if let Some(cid) = page_table.get(0) {
                let p = self.page_path(cid);
                fs::read(&p).map(|d| d.len() as u32).unwrap_or(4096)
            } else {
                4096
            }
        } else {
            4096
        };

        // Bundle all pages
        let bundle = self.bundle_pages(&page_table, page_size)?;

        // Publish bundle as single CraftOBJ content
        let bundle_cid = self.network.publish_page(&bundle)?;

        // Store bundle CID as root (both local and network)
        fs::write(self.root_path(), hex::encode(bundle_cid.0))?;
        self.network.set_root(bundle_cid)?;

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
    fn test_cache_miss_fetches_bundle_from_network() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Put a page locally, update root to create a bundle on network
        let page = Page { data: vec![0xAB; 4096] };
        let cid = store.put(&page).unwrap();
        // Manually create page table and trigger bundling via update_root
        let mut pt = PageTable::new();
        pt.set(0, cid);
        let pt_data = pt.to_bytes();
        let pt_cid = Cid::from_bytes(&pt_data);
        let pt_page = Page { data: pt_data };
        store.put(&pt_page).unwrap();
        store.update_root(pt_cid).unwrap();

        // Now create a new store pointing at the same network but empty cache
        let tmp2 = tempfile::tempdir().unwrap();
        let store2 = CraftObjPageStore {
            cache_dir: tmp2.path().to_path_buf(),
            network: MockNetworkBackend::new(),
            stats: CacheStats::new(),
        };
        fs::create_dir_all(tmp2.path().join("pages")).unwrap();

        // Copy network state from store to store2
        let net_pages = store.network.pages.lock().unwrap().clone();
        *store2.network.pages.lock().unwrap() = net_pages;
        let root = store.network.root.lock().unwrap().clone();
        *store2.network.root.lock().unwrap() = root;

        // First read — cache miss, should fetch bundle and unpack
        let got = store2.get(&cid).unwrap();
        assert_eq!(got.data, page.data);
        assert_eq!(store2.stats.misses.load(Ordering::Relaxed), 1);

        // Second read — cache hit
        let _ = store2.get(&cid).unwrap();
        assert_eq!(store2.stats.hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_root_get_set() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        assert_eq!(store.current_root().unwrap(), None);

        // Create a valid page table and pages so update_root can bundle
        let page = Page { data: vec![0u8; 4096] };
        let page_cid = store.put(&page).unwrap();
        let mut pt = PageTable::new();
        pt.set(0, page_cid);
        let pt_data = pt.to_bytes();
        let pt_cid = Cid::from_bytes(&pt_data);
        store.put(&Page { data: pt_data }).unwrap();

        store.update_root(pt_cid).unwrap();

        // Root should now be the bundle CID (not the page table CID)
        let root = store.current_root().unwrap();
        assert!(root.is_some());
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

        // Put a page (locally cached)
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
    }

    #[test]
    fn test_put_does_not_publish_to_network() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let page = Page { data: b"publish me".to_vec() };
        let _cid = store.put(&page).unwrap();

        // put() only caches locally — no network publish
        assert_eq!(store.network.publish_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_update_root_bundles_and_publishes() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        // Create some pages
        let page1 = Page { data: vec![1u8; 4096] };
        let page2 = Page { data: vec![2u8; 4096] };
        let cid1 = store.put(&page1).unwrap();
        let cid2 = store.put(&page2).unwrap();

        // Build page table and store it
        let mut pt = PageTable::new();
        pt.set(0, cid1);
        pt.set(1, cid2);
        let pt_data = pt.to_bytes();
        let pt_cid = Cid::from_bytes(&pt_data);
        let pt_page = Page { data: pt_data };
        store.put(&pt_page).unwrap();

        // update_root should bundle and publish
        store.update_root(pt_cid).unwrap();

        // Exactly one network publish (the bundle)
        assert_eq!(store.network.publish_count.load(Ordering::Relaxed), 1);

        // Root should be set on network
        assert!(store.network.root.lock().unwrap().is_some());
    }

    #[test]
    fn test_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_store(tmp.path());

        let cid = Cid::from_bytes(b"nonexistent");
        assert!(matches!(store.get(&cid), Err(PageStoreError::NotFound(_))));
    }
}
