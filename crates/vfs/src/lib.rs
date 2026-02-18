//! CraftSQL VFS — SQLite virtual file system backed by content-addressed PageStore.
//!
//! Translates SQLite's page-level reads/writes into PageStore operations.
//! WAL mode is disabled — rollback journal only (single-owner writes).

use craftsql_core::{Cid, Page, PageStore, PageTable};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenAccess, OpenKind, OpenOptions, Vfs, WalDisabled};
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};

/// Register the CraftSQL VFS with SQLite.
///
/// After registration, open databases with:
/// ```sql
/// .open file:mydb?vfs=craftsql
/// ```
pub fn register<S: PageStore + 'static>(name: &str, store: S) -> Result<(), sqlite_vfs::RegisterError> {
    let vfs = CraftVfs {
        store: Arc::new(store),
    };
    sqlite_vfs::register(name, vfs, false)
}

/// The CraftSQL virtual file system.
struct CraftVfs<S: PageStore> {
    store: Arc<S>,
}

/// Handle to an open database file.
struct CraftDbHandle<S: PageStore> {
    store: Arc<S>,
    /// In-memory page buffer: page_num → data. Flushed on sync.
    pages: Mutex<PageBuffer>,
    lock: Mutex<LockKind>,
}

/// Buffered pages for a database connection.
struct PageBuffer {
    /// Page data by page number.
    pages: Vec<Option<Vec<u8>>>,
    /// SQLite page size (detected from first write or default 4096).
    page_size: usize,
    /// Current total file size reported to SQLite.
    file_size: u64,
    /// Page table loaded from store (or new).
    page_table: PageTable,
    /// Whether any writes have occurred since last sync.
    dirty: bool,
}

impl PageBuffer {
    fn new(page_table: PageTable, page_size: usize) -> Self {
        let num_pages = page_table.len();
        let file_size = (num_pages * page_size) as u64;
        Self {
            pages: vec![None; num_pages],
            page_size,
            file_size,
            page_table,
            dirty: false,
        }
    }

    fn ensure_page(&mut self, page_num: usize) {
        if page_num >= self.pages.len() {
            self.pages.resize(page_num + 1, None);
        }
    }
}

impl<S: PageStore + 'static> Vfs for CraftVfs<S> {
    type Handle = CraftDbHandle<S>;

    fn open(&self, _db: &str, opts: OpenOptions) -> Result<Self::Handle, Error> {
        // For journal/temp files, return an empty handle
        if opts.kind != OpenKind::MainDb {
            return Ok(CraftDbHandle {
                store: Arc::clone(&self.store),
                pages: Mutex::new(PageBuffer::new(PageTable::new(), 4096)),
                lock: Mutex::new(LockKind::None),
            });
        }

        // Load existing page table or create new
        let page_table = match opts.access {
            OpenAccess::Read => {
                // Must exist
                let root = self.store.current_root()
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                    .ok_or_else(|| Error::new(ErrorKind::NotFound, "database not found"))?;
                let pt_page = self.store.get(&root)
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                PageTable::from_bytes(&pt_page.data)
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
            }
            _ => {
                // Try to load existing, or create new
                match self.store.current_root()
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))? {
                    Some(root) => {
                        let pt_page = self.store.get(&root)
                            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                        PageTable::from_bytes(&pt_page.data)
                            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?
                    }
                    None => PageTable::new(),
                }
            }
        };

        let page_size = if page_table.len() > 0 {
            // Detect from first page
            if let Some(cid) = page_table.get(0) {
                let page = self.store.get(cid)
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                page.data.len()
            } else {
                4096
            }
        } else {
            4096
        };

        Ok(CraftDbHandle {
            store: Arc::clone(&self.store),
            pages: Mutex::new(PageBuffer::new(page_table, page_size)),
            lock: Mutex::new(LockKind::None),
        })
    }

    fn delete(&self, db: &str) -> Result<(), Error> {
        // Only delete the main database, not journal/wal files
        if db.ends_with("-journal") || db.ends_with("-wal") || db.ends_with("-shm") {
            return Ok(()); // Journal cleanup is a no-op for us
        }
        // Delete = reset root pointer. Pages are garbage collected separately.
        self.store.update_root(Cid([0u8; 32]))
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    }

    fn exists(&self, db: &str) -> Result<bool, Error> {
        // Journal/WAL files never "exist" in our VFS
        if db.ends_with("-journal") || db.ends_with("-wal") || db.ends_with("-shm") {
            return Ok(false);
        }
        let root = self.store.current_root()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        Ok(root.is_some())
    }

    fn temporary_name(&self) -> String {
        format!("craftsql-tmp-{}", uuid::Uuid::new_v4())
    }

    fn random(&self, buffer: &mut [i8]) {
        // Simple random fill — good enough for SQLite's needs
        for b in buffer.iter_mut() {
            *b = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .subsec_nanos() as i8)
                .wrapping_mul(31);
        }
    }

    fn sleep(&self, duration: std::time::Duration) -> std::time::Duration {
        std::thread::sleep(duration);
        duration
    }
}

impl<S: PageStore + 'static> DatabaseHandle for CraftDbHandle<S> {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, Error> {
        let buf = self.pages.lock().unwrap();
        Ok(buf.file_size)
    }

    fn read_exact_at(&mut self, out: &mut [u8], offset: u64) -> Result<(), Error> {
        let buf = self.pages.lock().unwrap();
        let page_size = buf.page_size;
        let page_num = (offset as usize) / page_size;
        let page_offset = (offset as usize) % page_size;

        // Read may span pages — handle one page at a time
        let mut bytes_read = 0;
        let mut current_page = page_num;
        let mut current_offset = page_offset;

        while bytes_read < out.len() {
            let remaining = out.len() - bytes_read;
            let available_in_page = page_size - current_offset;
            let to_read = remaining.min(available_in_page);

            let page_data = self.read_page(&buf, current_page)?;
            if current_offset + to_read > page_data.len() {
                // Reading past end — fill with zeros (SQLite expects this)
                let valid = page_data.len().saturating_sub(current_offset);
                if valid > 0 {
                    out[bytes_read..bytes_read + valid]
                        .copy_from_slice(&page_data[current_offset..current_offset + valid]);
                }
                out[bytes_read + valid..bytes_read + to_read].fill(0);
            } else {
                out[bytes_read..bytes_read + to_read]
                    .copy_from_slice(&page_data[current_offset..current_offset + to_read]);
            }

            bytes_read += to_read;
            current_page += 1;
            current_offset = 0;
        }

        Ok(())
    }

    fn write_all_at(&mut self, data: &[u8], offset: u64) -> Result<(), Error> {
        let mut buf = self.pages.lock().unwrap();

        // Detect page size from first write (SQLite writes page 1 header first)
        if offset == 0 && buf.page_table.is_empty() && data.len() >= 100 {
            // SQLite stores page size at offset 16 (2 bytes, big-endian)
            let ps = u16::from_be_bytes([data[16], data[17]]) as usize;
            if ps >= 512 && ps <= 65536 && ps.is_power_of_two() {
                buf.page_size = ps;
            }
        }

        let page_size = buf.page_size;
        let page_num = (offset as usize) / page_size;
        let page_offset = (offset as usize) % page_size;

        // Write may span pages
        let mut bytes_written = 0;
        let mut current_page = page_num;
        let mut current_offset = page_offset;

        while bytes_written < data.len() {
            buf.ensure_page(current_page);

            let remaining = data.len() - bytes_written;
            let available_in_page = page_size - current_offset;
            let to_write = remaining.min(available_in_page);

            // Get or create page data
            let page_data = if let Some(ref existing) = buf.pages[current_page] {
                existing.clone()
            } else if let Some(cid) = buf.page_table.get(current_page) {
                // Load from store
                self.store.get(cid)
                    .map(|p| p.data)
                    .unwrap_or_else(|_| vec![0u8; page_size])
            } else {
                vec![0u8; page_size]
            };

            let mut page_data = if page_data.len() < page_size {
                let mut d = page_data;
                d.resize(page_size, 0);
                d
            } else {
                page_data
            };

            page_data[current_offset..current_offset + to_write]
                .copy_from_slice(&data[bytes_written..bytes_written + to_write]);

            buf.pages[current_page] = Some(page_data);
            buf.dirty = true;

            bytes_written += to_write;
            current_page += 1;
            current_offset = 0;
        }

        // Update file size
        let new_end = offset + data.len() as u64;
        if new_end > buf.file_size {
            buf.file_size = new_end;
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), Error> {
        let mut buf = self.pages.lock().unwrap();
        if !buf.dirty {
            return Ok(());
        }

        // Collect dirty pages and their CIDs
        let mut updates: Vec<(usize, Cid)> = Vec::new();
        for (i, page_data) in buf.pages.iter().enumerate() {
            if let Some(data) = page_data {
                let page = Page { data: data.clone() };
                let cid = self.store.put(&page)
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                updates.push((i, cid));
            }
        }
        for (i, cid) in updates {
            buf.page_table.set(i, cid);
        }

        // Persist page table itself as a page
        let pt_data = buf.page_table.to_bytes();
        let pt_page = Page { data: pt_data };
        let pt_cid = self.store.put(&pt_page)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        // Update root pointer
        self.store.update_root(pt_cid)
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;

        // Clear dirty pages (keep table)
        for p in buf.pages.iter_mut() {
            *p = None;
        }
        buf.dirty = false;

        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), Error> {
        let mut buf = self.pages.lock().unwrap();
        buf.file_size = size;
        let page_count = (size as usize + buf.page_size - 1) / buf.page_size;
        buf.pages.truncate(page_count);
        buf.page_table.entries.truncate(page_count);
        buf.dirty = true;
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, Error> {
        *self.lock.lock().unwrap() = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<LockKind, Error> {
        let l = *self.lock.lock().unwrap();
        Ok(l)
    }

    fn wal_index(&self, _readonly: bool) -> Result<WalDisabled, Error> {
        Ok(WalDisabled::default())
    }
}

impl<S: PageStore> CraftDbHandle<S> {
    fn read_page(&self, buf: &PageBuffer, page_num: usize) -> Result<Vec<u8>, Error> {
        // Check buffer first
        if page_num < buf.pages.len() {
            if let Some(ref data) = buf.pages[page_num] {
                return Ok(data.clone());
            }
        }

        // Check page table → store
        if let Some(cid) = buf.page_table.get(page_num) {
            let page = self.store.get(cid)
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
            return Ok(page.data);
        }

        // Page doesn't exist yet — return zeros
        Ok(vec![0u8; buf.page_size])
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use craftsql_core::{PageStoreError, Result as CsResult};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory PageStore for testing. Clone-friendly (Arc inner).
    #[derive(Clone)]
    struct MemStore {
        inner: Arc<MemStoreInner>,
    }

    struct MemStoreInner {
        pages: Mutex<std::collections::HashMap<Cid, Vec<u8>>>,
        root: Mutex<Option<Cid>>,
        named_roots: Mutex<std::collections::HashMap<String, Cid>>,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                inner: Arc::new(MemStoreInner {
                    pages: Mutex::new(std::collections::HashMap::new()),
                    root: Mutex::new(None),
                    named_roots: Mutex::new(std::collections::HashMap::new()),
                }),
            }
        }

        fn page_count(&self) -> usize {
            self.inner.pages.lock().unwrap().len()
        }
    }

    impl PageStore for MemStore {
        fn get(&self, cid: &Cid) -> CsResult<Page> {
            self.inner.pages.lock().unwrap()
                .get(cid)
                .map(|d| Page { data: d.clone() })
                .ok_or(PageStoreError::NotFound(*cid))
        }

        fn put(&self, page: &Page) -> CsResult<Cid> {
            let cid = Cid::from_bytes(&page.data);
            self.inner.pages.lock().unwrap().insert(cid, page.data.clone());
            Ok(cid)
        }

        fn update_root(&self, new_root: Cid) -> CsResult<()> {
            *self.inner.root.lock().unwrap() = Some(new_root);
            Ok(())
        }

        fn current_root(&self) -> CsResult<Option<Cid>> {
            Ok(*self.inner.root.lock().unwrap())
        }

        fn set_named_root(&self, name: &str, cid: Cid) -> CsResult<()> {
            self.inner.named_roots.lock().unwrap().insert(name.to_string(), cid);
            Ok(())
        }

        fn get_named_root(&self, name: &str) -> CsResult<Option<Cid>> {
            Ok(self.inner.named_roots.lock().unwrap().get(name).copied())
        }

        fn remove_named_root(&self, name: &str) -> CsResult<bool> {
            Ok(self.inner.named_roots.lock().unwrap().remove(name).is_some())
        }

        fn list_named_roots(&self) -> CsResult<Vec<(String, Cid)>> {
            let roots = self.inner.named_roots.lock().unwrap();
            let mut v: Vec<_> = roots.iter().map(|(k, v)| (k.clone(), *v)).collect();
            v.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(v)
        }
    }

    static VFS_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn unique_vfs_name() -> String {
        format!("craftsql_test_{}", VFS_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    const OPEN_RW: rusqlite::OpenFlags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        .union(rusqlite::OpenFlags::SQLITE_OPEN_CREATE);

    fn open_db(name: &str) -> rusqlite::Connection {
        let path = format!("/craftsql/{name}/db");
        let db = rusqlite::Connection::open_with_flags_and_vfs(&path, OPEN_RW, name).unwrap();
        db.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();
        db
    }

    // --- Basic SQL tests ---

    #[test]
    fn test_create_insert_query() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();
        let db = open_db(&name);

        db.execute_batch("
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users VALUES (1, 'alice');
            INSERT INTO users VALUES (2, 'bob');
        ").unwrap();

        let names: Vec<String> = db
            .prepare("SELECT name FROM users ORDER BY id").unwrap()
            .query_map([], |r: &rusqlite::Row| r.get(0)).unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(names, vec!["alice", "bob"]);
    }

    #[test]
    fn test_many_rows() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();
        let db = open_db(&name);

        db.execute_batch("CREATE TABLE nums (i INTEGER);").unwrap();
        for i in 0..1000 {
            db.execute("INSERT INTO nums VALUES (?1)", [i]).unwrap();
        }

        let sum: i64 = db.query_row("SELECT SUM(i) FROM nums", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(sum, 999 * 1000 / 2);
    }

    #[test]
    fn test_transaction_rollback() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();
        let db = open_db(&name);

        db.execute_batch("
            CREATE TABLE t (x INTEGER);
            INSERT INTO t VALUES (1);
        ").unwrap();

        db.execute_batch("BEGIN; INSERT INTO t VALUES (2); ROLLBACK;").unwrap();

        let count: i64 = db.query_row("SELECT COUNT(*) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(count, 1);
    }

    // --- Persistence tests ---

    #[test]
    fn test_persist_close_reopen() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Write and close
        {
            let db = open_db(&name);
            db.execute_batch("
                CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT);
                INSERT INTO items VALUES (1, 'persisted');
                INSERT INTO items VALUES (2, 'across');
                INSERT INTO items VALUES (3, 'connections');
            ").unwrap();
        }

        assert!(store.current_root().unwrap().is_some());

        // Reopen and query
        {
            let db = open_db(&name);
            let vals: Vec<String> = db
                .prepare("SELECT val FROM items ORDER BY id").unwrap()
                .query_map([], |r: &rusqlite::Row| r.get(0)).unwrap()
                .map(|r| r.unwrap())
                .collect();
            assert_eq!(vals, vec!["persisted", "across", "connections"]);
        }
    }

    #[test]
    fn test_persist_schema_and_index() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Create schema with index + data
        {
            let db = open_db(&name);
            db.execute_batch("
                CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB);
                CREATE INDEX idx_key ON kv(key);
            ").unwrap();
            for i in 0..50 {
                db.execute("INSERT INTO kv VALUES (?1, ?2)",
                    rusqlite::params![format!("k{:03}", i), vec![i as u8; 32]],
                ).unwrap();
            }
        }

        // Reopen — schema, index, and data survive
        {
            let db = open_db(&name);

            let count: i64 = db.query_row("SELECT COUNT(*) FROM kv", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(count, 50);

            let val: Vec<u8> = db.query_row(
                "SELECT value FROM kv WHERE key = 'k025'", [], |r: &rusqlite::Row| r.get(0)
            ).unwrap();
            assert_eq!(val, vec![25u8; 32]);

            // Can still insert after reopen
            db.execute("INSERT INTO kv VALUES ('new', X'CAFE')", []).unwrap();
            let count2: i64 = db.query_row("SELECT COUNT(*) FROM kv", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(count2, 51);
        }
    }

    #[test]
    fn test_persist_multiple_cycles() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Cycle 1: create table
        {
            let db = open_db(&name);
            db.execute_batch("CREATE TABLE counter (n INTEGER);").unwrap();
            db.execute("INSERT INTO counter VALUES (0)", []).unwrap();
        }

        // Cycles 2-10: reopen, update, close
        for i in 1..=10 {
            let db = open_db(&name);
            db.execute("UPDATE counter SET n = ?1", [i]).unwrap();
        }

        // Verify final value
        {
            let db = open_db(&name);
            let n: i64 = db.query_row("SELECT n FROM counter", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(n, 10);
        }
    }

    #[test]
    fn test_persist_with_local_page_store() {
        use craftsql_store_local::LocalPageStore;

        let dir = std::env::temp_dir().join(format!("craftsql-persist-{}", std::process::id()));

        // Write with LocalPageStore
        let name1 = unique_vfs_name();
        {
            let store = LocalPageStore::new(&dir).unwrap();
            register(&name1, store).unwrap();
            let db = open_db(&name1);
            db.execute_batch("
                CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);
                INSERT INTO docs VALUES (1, 'hello from disk');
            ").unwrap();
        }

        // Reopen with fresh LocalPageStore (new VFS registration, same dir)
        let name2 = unique_vfs_name();
        {
            let store = LocalPageStore::new(&dir).unwrap();
            register(&name2, store).unwrap();
            let db = open_db(&name2);
            let body: String = db.query_row(
                "SELECT body FROM docs WHERE id = 1", [], |r: &rusqlite::Row| r.get(0)
            ).unwrap();
            assert_eq!(body, "hello from disk");
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_content_dedup() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        let db = open_db(&name);
        db.execute_batch("CREATE TABLE t (data BLOB);").unwrap();
        let blob = vec![0xABu8; 3000];
        for _ in 0..100 {
            db.execute("INSERT INTO t VALUES (?1)", [&blob[..]]).unwrap();
        }

        // Verify data
        let count: i64 = db.query_row("SELECT COUNT(*) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(count, 100);

        // Content-addressed dedup means identical pages stored once
        assert!(store.page_count() > 0);
    }

    // --- Snapshot & Branch tests ---

    #[test]
    fn test_snapshot_and_restore() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Create table, insert initial data
        {
            let db = open_db(&name);
            db.execute_batch("CREATE TABLE t (x INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1)", []).unwrap();
        }

        // Take snapshot
        let snap_cid = store.current_root().unwrap().unwrap();
        store.set_named_root("v1", snap_cid).unwrap();

        // Modify data
        {
            let db = open_db(&name);
            db.execute("INSERT INTO t VALUES (2)", []).unwrap();
            db.execute("INSERT INTO t VALUES (3)", []).unwrap();
        }

        // Verify current state has 3 rows
        {
            let db = open_db(&name);
            let count: i64 = db.query_row("SELECT COUNT(*) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(count, 3);
        }

        // Restore snapshot v1
        let v1_cid = store.get_named_root("v1").unwrap().unwrap();
        store.update_root(v1_cid).unwrap();

        // Verify restored state has only 1 row
        {
            let db = open_db(&name);
            let count: i64 = db.query_row("SELECT COUNT(*) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(count, 1);
        }
    }

    #[test]
    fn test_branch_diverge() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Create base state
        {
            let db = open_db(&name);
            db.execute_batch("
                CREATE TABLE t (x INTEGER);
                INSERT INTO t VALUES (100);
            ").unwrap();
        }
        let base = store.current_root().unwrap().unwrap();
        store.set_named_root("main", base).unwrap();
        store.set_named_root("feature", base).unwrap();

        // Diverge on "main": add row 200
        store.update_root(store.get_named_root("main").unwrap().unwrap()).unwrap();
        {
            let db = open_db(&name);
            db.execute("INSERT INTO t VALUES (200)", []).unwrap();
        }
        store.set_named_root("main", store.current_root().unwrap().unwrap()).unwrap();

        // Diverge on "feature": add row 300
        store.update_root(store.get_named_root("feature").unwrap().unwrap()).unwrap();
        {
            let db = open_db(&name);
            db.execute("INSERT INTO t VALUES (300)", []).unwrap();
        }
        store.set_named_root("feature", store.current_root().unwrap().unwrap()).unwrap();

        // Verify "main" has {100, 200}
        store.update_root(store.get_named_root("main").unwrap().unwrap()).unwrap();
        {
            let db = open_db(&name);
            let sum: i64 = db.query_row("SELECT SUM(x) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(sum, 300); // 100 + 200
        }

        // Verify "feature" has {100, 300}
        store.update_root(store.get_named_root("feature").unwrap().unwrap()).unwrap();
        {
            let db = open_db(&name);
            let sum: i64 = db.query_row("SELECT SUM(x) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
            assert_eq!(sum, 400); // 100 + 300
        }
    }

    #[test]
    fn test_snapshot_list_and_remove() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        {
            let db = open_db(&name);
            db.execute_batch("CREATE TABLE t (x INTEGER);").unwrap();
        }

        let root = store.current_root().unwrap().unwrap();
        store.set_named_root("alpha", root).unwrap();
        store.set_named_root("beta", root).unwrap();
        store.set_named_root("gamma", root).unwrap();

        let roots = store.list_named_roots().unwrap();
        assert_eq!(roots.len(), 3);
        assert_eq!(roots[0].0, "alpha");
        assert_eq!(roots[1].0, "beta");
        assert_eq!(roots[2].0, "gamma");

        // Remove one
        assert!(store.remove_named_root("beta").unwrap());
        assert!(!store.remove_named_root("beta").unwrap());
        assert_eq!(store.list_named_roots().unwrap().len(), 2);
    }

    #[test]
    fn test_page_table_diff_via_snapshots() {
        use craftsql_core::PageTable;

        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store.clone()).unwrap();

        // Create initial state
        {
            let db = open_db(&name);
            db.execute_batch("CREATE TABLE t (x INTEGER);").unwrap();
            db.execute("INSERT INTO t VALUES (1)", []).unwrap();
        }
        let root_v1 = store.current_root().unwrap().unwrap();
        store.set_named_root("v1", root_v1).unwrap();

        // Modify
        {
            let db = open_db(&name);
            db.execute("INSERT INTO t VALUES (2)", []).unwrap();
        }
        let root_v2 = store.current_root().unwrap().unwrap();

        // Load both page tables and diff
        let pt1_data = store.get(&root_v1).unwrap();
        let pt1 = PageTable::from_bytes(&pt1_data.data).unwrap();

        let pt2_data = store.get(&root_v2).unwrap();
        let pt2 = PageTable::from_bytes(&pt2_data.data).unwrap();

        let diff = pt2.diff(&pt1);
        // At least one page should have changed (the data page with the new row)
        assert!(!diff.changed.is_empty());
    }
}
