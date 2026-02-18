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
        // Only handle main database files
        if opts.kind != OpenKind::MainDb {
            return Err(Error::new(ErrorKind::Other, "only main database supported"));
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

    fn delete(&self, _db: &str) -> Result<(), Error> {
        // Delete = reset root pointer. Pages are garbage collected separately.
        self.store.update_root(Cid([0u8; 32]))
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))
    }

    fn exists(&self, _db: &str) -> Result<bool, Error> {
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
        // Single-owner: locking always succeeds
        *self.lock.lock().unwrap() = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<LockKind, Error> {
        Ok(*self.lock.lock().unwrap())
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

    /// In-memory PageStore for testing.
    struct MemStore {
        pages: Mutex<std::collections::HashMap<Cid, Vec<u8>>>,
        root: Mutex<Option<Cid>>,
    }

    impl MemStore {
        fn new() -> Self {
            Self {
                pages: Mutex::new(std::collections::HashMap::new()),
                root: Mutex::new(None),
            }
        }

        fn page_count(&self) -> usize {
            self.pages.lock().unwrap().len()
        }
    }

    impl PageStore for MemStore {
        fn get(&self, cid: &Cid) -> CsResult<Page> {
            let pages = self.pages.lock().unwrap();
            pages.get(cid)
                .map(|d| Page { data: d.clone() })
                .ok_or(PageStoreError::NotFound(*cid))
        }

        fn put(&self, page: &Page) -> CsResult<Cid> {
            let cid = Cid::from_bytes(&page.data);
            self.pages.lock().unwrap().insert(cid, page.data.clone());
            Ok(cid)
        }

        fn update_root(&self, new_root: Cid) -> CsResult<()> {
            *self.root.lock().unwrap() = Some(new_root);
            Ok(())
        }

        fn current_root(&self) -> CsResult<Option<Cid>> {
            Ok(*self.root.lock().unwrap())
        }
    }

    static VFS_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn unique_vfs_name() -> String {
        format!("craftsql_test_{}", VFS_COUNTER.fetch_add(1, Ordering::SeqCst))
    }

    #[test]
    fn test_sqlite_create_table_insert_query() {
        let name = unique_vfs_name();
        let store = MemStore::new();
        register(&name, store).unwrap();

        let db = rusqlite::Connection::open_with_flags_and_vfs(":memory:", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE, &name).unwrap();
        db.execute_batch("
            PRAGMA journal_mode=DELETE;
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
            INSERT INTO users VALUES (1, 'alice');
            INSERT INTO users VALUES (2, 'bob');
        ").unwrap();

        let names: Vec<String> = db
            .prepare("SELECT name FROM users ORDER BY id").unwrap()
            .query_map([], |row| row.get(0)).unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(names, vec!["alice", "bob"]);
    }

    #[test]
    fn test_sqlite_multiple_tables() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();

        let db = rusqlite::Connection::open_with_flags_and_vfs(":memory:", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE, &name).unwrap();
        db.execute_batch("
            PRAGMA journal_mode=DELETE;
            CREATE TABLE t1 (x INTEGER);
            CREATE TABLE t2 (y TEXT);
            INSERT INTO t1 VALUES (42);
            INSERT INTO t2 VALUES ('hello');
        ").unwrap();

        let x: i64 = db.query_row("SELECT x FROM t1", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        let y: String = db.query_row("SELECT y FROM t2", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(x, 42);
        assert_eq!(y, "hello");
    }

    #[test]
    fn test_sqlite_many_rows() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();

        let db = rusqlite::Connection::open_with_flags_and_vfs(":memory:", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE, &name).unwrap();
        db.execute_batch("
            PRAGMA journal_mode=DELETE;
            CREATE TABLE nums (i INTEGER);
        ").unwrap();

        for i in 0..1000 {
            db.execute("INSERT INTO nums VALUES (?1)", [i]).unwrap();
        }

        let count: i64 = db.query_row("SELECT COUNT(*) FROM nums", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(count, 1000);

        let sum: i64 = db.query_row("SELECT SUM(i) FROM nums", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(sum, 999 * 1000 / 2);
    }

    #[test]
    fn test_sqlite_index_and_query() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();

        let db = rusqlite::Connection::open_with_flags_and_vfs(":memory:", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE, &name).unwrap();
        db.execute_batch("
            PRAGMA journal_mode=DELETE;
            CREATE TABLE kv (key TEXT PRIMARY KEY, value BLOB);
            CREATE INDEX idx_key ON kv(key);
        ").unwrap();

        for i in 0..100 {
            db.execute("INSERT INTO kv VALUES (?1, ?2)",
                rusqlite::params![format!("key_{:04}", i), vec![i as u8; 64]],
            ).unwrap();
        }

        let val: Vec<u8> = db.query_row(
            "SELECT value FROM kv WHERE key = 'key_0042'", [], |r: &rusqlite::Row| r.get(0)
        ).unwrap();
        assert_eq!(val.len(), 64);
        assert!(val.iter().all(|&b| b == 42));
    }

    #[test]
    fn test_sqlite_transaction_rollback() {
        let name = unique_vfs_name();
        register(&name, MemStore::new()).unwrap();

        let db = rusqlite::Connection::open_with_flags_and_vfs(":memory:", rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_CREATE, &name).unwrap();
        db.execute_batch("
            PRAGMA journal_mode=DELETE;
            CREATE TABLE t (x INTEGER);
            INSERT INTO t VALUES (1);
        ").unwrap();

        // Start transaction, insert, rollback
        db.execute_batch("BEGIN; INSERT INTO t VALUES (2); ROLLBACK;").unwrap();

        let count: i64 = db.query_row("SELECT COUNT(*) FROM t", [], |r: &rusqlite::Row| r.get(0)).unwrap();
        assert_eq!(count, 1); // Only the first row
    }
}
