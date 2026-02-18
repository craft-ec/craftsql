//! Caching PageStore — bridges local disk cache with remote backends
//! Provides TTL-based root refresh, prefetching, and cache statistics.

use craftsql_core::{Cid, Page, PageStore, PageStoreError, PageTable, Result};
use craftsql_store_local::LocalPageStore;
use std::path::Path;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Mutex};
use std::time::{Duration, Instant};

/// Configuration for caching behavior
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// TTL for root pointer (None = no auto-refresh, manual only)
    pub root_ttl: Option<Duration>,
    /// Whether to prefetch all pages on open
    pub prefetch_on_open: bool,
    /// Max pages to prefetch (0 = all)
    pub max_prefetch_pages: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            root_ttl: Some(Duration::from_secs(300)), // 5 minutes
            prefetch_on_open: false,
            max_prefetch_pages: 0, // no limit
        }
    }
}

/// Root pointer cache with timestamp
#[derive(Debug, Default)]
struct RootCache {
    root: Option<Cid>,
    fetched_at: Option<Instant>,
}

/// Cache statistics
#[derive(Debug)]
pub struct CacheStats {
    pub cached_pages: usize,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            cached_pages: 0,
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Get current hit rate (0.0 to 1.0)
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Caching PageStore that wraps a remote backend with local disk cache
pub struct CachingPageStore<R: PageStore> {
    /// Local disk cache (persistent across restarts)
    local: LocalPageStore,
    /// Remote backend (CraftOBJ, S3, etc.)
    remote: R,
    /// Root pointer cache
    root_cache: Mutex<RootCache>,
    /// Configuration
    config: CacheConfig,
    /// Cache statistics
    stats: CacheStats,
}

impl<R: PageStore> CachingPageStore<R> {
    /// Create new caching store
    pub fn new(cache_dir: &Path, remote: R, config: CacheConfig) -> Result<Self> {
        let local = LocalPageStore::new(cache_dir)?;
        let stats = CacheStats::new();
        
        let store = Self {
            local,
            remote,
            root_cache: Mutex::new(RootCache::default()),
            config,
            stats,
        };

        // Prefetch on open if configured
        if store.config.prefetch_on_open {
            let _ = store.prefetch(); // Ignore errors during initialization
        }

        Ok(store)
    }

    /// Force refresh root pointer from remote
    pub fn refresh_root(&self) -> Result<Option<Cid>> {
        let remote_root = self.remote.current_root()?;
        
        // Update cache
        let mut cache = self.root_cache.lock().unwrap();
        cache.root = remote_root;
        cache.fetched_at = Some(Instant::now());
        drop(cache);

        // Update local cache
        if let Some(cid) = remote_root {
            self.local.update_root(cid)?;
        }

        Ok(remote_root)
    }

    /// Prefetch pages: load page table from remote, bulk fetch all pages into local cache
    pub fn prefetch(&self) -> Result<usize> {
        // Get current root from remote
        let root_cid = match self.remote.current_root()? {
            Some(cid) => cid,
            None => return Ok(0), // No root to prefetch
        };

        // Fetch page table page
        let pt_page = self.remote.get(&root_cid)?;
        let page_table = PageTable::from_bytes(&pt_page.data)
            .map_err(|e| PageStoreError::Storage(format!("Failed to parse page table: {}", e)))?;

        // Store page table in local cache
        self.local.put(&pt_page)?;

        // Collect all CIDs to prefetch
        let mut cids_to_fetch = Vec::new();
        for i in 0..page_table.len() {
            if let Some(cid) = page_table.get(i) {
                // Check if not already in local cache
                if !self.is_cached(cid) {
                    cids_to_fetch.push(*cid);
                    
                    // Respect max_prefetch_pages limit
                    if self.config.max_prefetch_pages > 0 
                        && cids_to_fetch.len() >= self.config.max_prefetch_pages {
                        break;
                    }
                }
            }
        }

        // Prefetch the CIDs
        self.prefetch_cids(&cids_to_fetch)
    }

    /// Prefetch specific pages by CID
    pub fn prefetch_cids(&self, cids: &[Cid]) -> Result<usize> {
        let mut fetched = 0;
        for &cid in cids {
            if !self.is_cached(&cid) {
                match self.remote.get(&cid) {
                    Ok(page) => {
                        self.local.put(&page)?;
                        fetched += 1;
                    }
                    Err(_) => {
                        // Skip pages that can't be fetched from remote
                        continue;
                    }
                }
            }
        }
        Ok(fetched)
    }

    /// Check if a page is locally cached
    pub fn is_cached(&self, cid: &Cid) -> bool {
        self.local.get(cid).is_ok()
    }

    /// Cache stats
    pub fn stats(&self) -> &CacheStats {
        &self.stats
    }
}

impl<R: PageStore> PageStore for CachingPageStore<R> {
    fn get(&self, cid: &Cid) -> Result<Page> {
        // 1. Check local cache
        match self.local.get(cid) {
            Ok(page) => {
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                return Ok(page);
            }
            Err(PageStoreError::NotFound(_)) => {
                // Cache miss, continue to remote
                self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => return Err(e), // Other errors bubble up
        }

        // 2. Miss → fetch from remote
        let page = self.remote.get(cid)?;

        // 3. Store in local cache
        let _ = self.local.put(&page); // Ignore local cache errors

        // 4. Return
        Ok(page)
    }

    fn put(&self, page: &Page) -> Result<Cid> {
        // Write to local cache first
        let cid = self.local.put(page)?;
        
        // Write to remote (write-through)
        self.remote.put(page)?;
        
        Ok(cid)
    }

    fn current_root(&self) -> Result<Option<Cid>> {
        let now = Instant::now();
        
        // Check root cache TTL
        {
            let cache = self.root_cache.lock().unwrap();
            if let (Some(root), Some(fetched_at)) = (cache.root, cache.fetched_at) {
                if let Some(ttl) = self.config.root_ttl {
                    if now.duration_since(fetched_at) < ttl {
                        // Fresh enough → return cached
                        return Ok(Some(root));
                    }
                } else {
                    // No TTL configured, always use cached if available
                    return Ok(Some(root));
                }
            }
        }

        // Stale or missing → fetch from remote, update cache
        self.refresh_root()
    }

    fn update_root(&self, new_root: Cid) -> Result<()> {
        // Update both local and remote
        self.local.update_root(new_root)?;
        self.remote.update_root(new_root)?;

        // Update root cache
        let mut cache = self.root_cache.lock().unwrap();
        cache.root = Some(new_root);
        cache.fetched_at = Some(Instant::now());

        Ok(())
    }

    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()> {
        // Write to both local and remote
        self.local.set_named_root(name, cid)?;
        self.remote.set_named_root(name, cid)?;
        Ok(())
    }

    fn get_named_root(&self, name: &str) -> Result<Option<Cid>> {
        // Try local first, fall back to remote
        match self.local.get_named_root(name) {
            Ok(Some(cid)) => Ok(Some(cid)),
            Ok(None) => self.remote.get_named_root(name),
            Err(_) => self.remote.get_named_root(name),
        }
    }

    fn remove_named_root(&self, name: &str) -> Result<bool> {
        // Remove from both local and remote
        let local_removed = self.local.remove_named_root(name)?;
        let remote_removed = self.remote.remove_named_root(name)?;
        Ok(local_removed || remote_removed)
    }

    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>> {
        // Get from both local and remote, merge and deduplicate
        let local_roots = self.local.list_named_roots().unwrap_or_default();
        let remote_roots = self.remote.list_named_roots().unwrap_or_default();
        
        let mut all_roots = std::collections::BTreeMap::new();
        
        // Add local roots
        for (name, cid) in local_roots {
            all_roots.insert(name, cid);
        }
        
        // Add remote roots (may overwrite local if different)
        for (name, cid) in remote_roots {
            all_roots.insert(name, cid);
        }
        
        Ok(all_roots.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};
    use tempfile::TempDir;

    /// Mock in-memory PageStore for testing
    #[derive(Debug, Clone)]
    struct MockPageStore {
        pages: Arc<StdMutex<HashMap<Cid, Page>>>,
        root: Arc<StdMutex<Option<Cid>>>,
        named_roots: Arc<StdMutex<HashMap<String, Cid>>>,
    }

    impl MockPageStore {
        fn new() -> Self {
            Self {
                pages: Arc::new(StdMutex::new(HashMap::new())),
                root: Arc::new(StdMutex::new(None)),
                named_roots: Arc::new(StdMutex::new(HashMap::new())),
            }
        }

        fn populate_with_pages(&self, page_count: usize) -> Vec<Cid> {
            let mut cids = Vec::new();
            for i in 0..page_count {
                let data = format!("page {} data", i);
                let page = Page { data: data.into_bytes() };
                let cid = self.put(&page).unwrap();
                cids.push(cid);
            }
            cids
        }
    }

    impl PageStore for MockPageStore {
        fn get(&self, cid: &Cid) -> Result<Page> {
            let pages = self.pages.lock().unwrap();
            pages.get(cid).cloned().ok_or(PageStoreError::NotFound(*cid))
        }

        fn put(&self, page: &Page) -> Result<Cid> {
            let cid = Cid::from_bytes(&page.data);
            let mut pages = self.pages.lock().unwrap();
            pages.insert(cid, page.clone());
            Ok(cid)
        }

        fn current_root(&self) -> Result<Option<Cid>> {
            Ok(*self.root.lock().unwrap())
        }

        fn update_root(&self, new_root: Cid) -> Result<()> {
            *self.root.lock().unwrap() = Some(new_root);
            Ok(())
        }

        fn set_named_root(&self, name: &str, cid: Cid) -> Result<()> {
            self.named_roots.lock().unwrap().insert(name.to_string(), cid);
            Ok(())
        }

        fn get_named_root(&self, name: &str) -> Result<Option<Cid>> {
            Ok(self.named_roots.lock().unwrap().get(name).copied())
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

    fn create_test_store() -> (TempDir, CachingPageStore<MockPageStore>) {
        let temp_dir = TempDir::new().unwrap();
        let mock_remote = MockPageStore::new();
        let config = CacheConfig::default();
        let store = CachingPageStore::new(temp_dir.path(), mock_remote, config).unwrap();
        (temp_dir, store)
    }

    #[test]
    fn test_cache_hit() {
        let (_temp_dir, store) = create_test_store();

        // Put via remote, then get via caching store
        let page = Page { data: b"hello world".to_vec() };
        let cid = store.remote.put(&page).unwrap();

        // First call should fetch from remote and cache locally
        let retrieved1 = store.get(&cid).unwrap();
        assert_eq!(retrieved1.data, page.data);
        assert_eq!(store.stats.cache_misses.load(Ordering::Relaxed), 1);

        // Second call should be a cache hit
        let retrieved2 = store.get(&cid).unwrap();
        assert_eq!(retrieved2.data, page.data);
        assert_eq!(store.stats.cache_hits.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_cache_miss_fallback() {
        let (_temp_dir, store) = create_test_store();

        // Page only in remote
        let page = Page { data: b"remote only".to_vec() };
        let cid = store.remote.put(&page).unwrap();

        // Should fetch from remote and cache locally
        let retrieved = store.get(&cid).unwrap();
        assert_eq!(retrieved.data, page.data);
        
        // Should now be cached locally
        assert!(store.is_cached(&cid));
    }

    #[test]
    fn test_root_ttl_expired() {
        let (_temp_dir, mut store) = create_test_store();
        
        // Set very short TTL
        store.config.root_ttl = Some(Duration::from_millis(1));

        let cid = Cid::from_bytes(b"test root");
        store.remote.update_root(cid).unwrap();

        // First call should fetch from remote
        let root1 = store.current_root().unwrap();
        assert_eq!(root1, Some(cid));

        // Wait for TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        // Should refresh from remote again
        let root2 = store.current_root().unwrap();
        assert_eq!(root2, Some(cid));
    }

    #[test]
    fn test_root_ttl_not_expired() {
        let (_temp_dir, mut store) = create_test_store();
        
        // Set long TTL
        store.config.root_ttl = Some(Duration::from_secs(60));

        let cid = Cid::from_bytes(b"test root");
        store.remote.update_root(cid).unwrap();

        // First call
        let root1 = store.current_root().unwrap();
        assert_eq!(root1, Some(cid));

        // Change remote root
        let new_cid = Cid::from_bytes(b"new test root");
        store.remote.update_root(new_cid).unwrap();

        // Should still return cached root (TTL not expired)
        let root2 = store.current_root().unwrap();
        assert_eq!(root2, Some(cid)); // Still old root
    }

    #[test]
    fn test_force_refresh() {
        let (_temp_dir, store) = create_test_store();

        let cid = Cid::from_bytes(b"test root");
        store.remote.update_root(cid).unwrap();

        // Cache the root
        let _ = store.current_root().unwrap();

        // Change remote root
        let new_cid = Cid::from_bytes(b"new test root");
        store.remote.update_root(new_cid).unwrap();

        // Force refresh should get new root
        let refreshed = store.refresh_root().unwrap();
        assert_eq!(refreshed, Some(new_cid));
    }

    #[test]
    fn test_prefetch() {
        let (_temp_dir, store) = create_test_store();

        // Create a page table with some pages
        let page_cids = store.remote.populate_with_pages(3);
        
        let mut page_table = PageTable::new();
        for (i, &cid) in page_cids.iter().enumerate() {
            page_table.set(i, cid);
        }
        
        // Store page table as root
        let pt_page = Page { data: page_table.to_bytes() };
        let pt_cid = store.remote.put(&pt_page).unwrap();
        store.remote.update_root(pt_cid).unwrap();

        // Prefetch should load all pages into local cache
        let fetched = store.prefetch().unwrap();
        assert_eq!(fetched, 3); // Should fetch 3 data pages (page table already fetched)

        // All pages should now be cached
        for &cid in &page_cids {
            assert!(store.is_cached(&cid));
        }
    }

    #[test]
    fn test_write_through() {
        let (_temp_dir, store) = create_test_store();

        let page = Page { data: b"write through test".to_vec() };
        let cid = store.put(&page).unwrap();

        // Should be in both local and remote
        assert!(store.local.get(&cid).is_ok());
        assert!(store.remote.get(&cid).is_ok());
    }

    #[test]
    fn test_update_root_write_through() {
        let (_temp_dir, store) = create_test_store();

        let cid = Cid::from_bytes(b"test root");
        store.update_root(cid).unwrap();

        // Should be in both local and remote
        assert_eq!(store.local.current_root().unwrap(), Some(cid));
        assert_eq!(store.remote.current_root().unwrap(), Some(cid));
    }

    #[test]
    fn test_cache_stats() {
        let (_temp_dir, store) = create_test_store();

        let page = Page { data: b"stats test".to_vec() };
        let cid = store.remote.put(&page).unwrap();

        // Initial stats
        assert_eq!(store.stats.cache_hits.load(Ordering::Relaxed), 0);
        assert_eq!(store.stats.cache_misses.load(Ordering::Relaxed), 0);
        assert_eq!(store.stats.hit_rate(), 0.0);

        // Miss
        let _ = store.get(&cid).unwrap();
        assert_eq!(store.stats.cache_misses.load(Ordering::Relaxed), 1);
        assert_eq!(store.stats.hit_rate(), 0.0);

        // Hit
        let _ = store.get(&cid).unwrap();
        assert_eq!(store.stats.cache_hits.load(Ordering::Relaxed), 1);
        assert_eq!(store.stats.hit_rate(), 0.5);
    }

    #[test]
    fn test_persistent_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        let page = Page { data: b"persistent test".to_vec() };
        let cid;

        // Create first store and put a page
        {
            let mock_remote = MockPageStore::new();
            let config = CacheConfig::default();
            let store = CachingPageStore::new(&cache_path, mock_remote, config).unwrap();
            cid = store.put(&page).unwrap();
        }

        // Create new store with same cache_dir
        {
            let mock_remote = MockPageStore::new();
            let config = CacheConfig::default();
            let store = CachingPageStore::new(&cache_path, mock_remote, config).unwrap();
            
            // Page should still be there from disk cache
            assert!(store.is_cached(&cid));
            let retrieved = store.local.get(&cid).unwrap(); // Direct local access
            assert_eq!(retrieved.data, page.data);
        }
    }

    #[test]
    fn test_named_roots() {
        let (_temp_dir, store) = create_test_store();

        let cid1 = Cid::from_bytes(b"snapshot 1");
        let cid2 = Cid::from_bytes(b"snapshot 2");

        // Set named roots
        store.set_named_root("snap1", cid1).unwrap();
        store.set_named_root("snap2", cid2).unwrap();

        // Should be in both local and remote
        assert_eq!(store.get_named_root("snap1").unwrap(), Some(cid1));
        assert_eq!(store.remote.get_named_root("snap1").unwrap(), Some(cid1));
        assert_eq!(store.local.get_named_root("snap1").unwrap(), Some(cid1));

        // List should return both
        let roots = store.list_named_roots().unwrap();
        assert_eq!(roots.len(), 2);
        assert!(roots.contains(&("snap1".to_string(), cid1)));
        assert!(roots.contains(&("snap2".to_string(), cid2)));

        // Remove one
        assert!(store.remove_named_root("snap1").unwrap());
        assert_eq!(store.list_named_roots().unwrap().len(), 1);
    }

    #[test]
    fn test_max_prefetch_pages() {
        let (_temp_dir, mut store) = create_test_store();
        
        // Limit prefetch to 2 pages
        store.config.max_prefetch_pages = 2;

        // Create more pages than the limit
        let page_cids = store.remote.populate_with_pages(5);
        
        let mut page_table = PageTable::new();
        for (i, &cid) in page_cids.iter().enumerate() {
            page_table.set(i, cid);
        }
        
        let pt_page = Page { data: page_table.to_bytes() };
        let pt_cid = store.remote.put(&pt_page).unwrap();
        store.remote.update_root(pt_cid).unwrap();

        // Should only fetch max_prefetch_pages
        let fetched = store.prefetch().unwrap();
        assert_eq!(fetched, 2);
    }
}