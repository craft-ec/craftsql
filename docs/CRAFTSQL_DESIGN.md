# CraftSQL Design

## Overview

CraftSQL is a SQLite VFS implementation that maps page I/O to content-addressed storage. SQLite handles all database logic (B-trees, indexing, query planning, transactions). CraftSQL only handles how pages are stored and retrieved.

## Core Concept

```
SQLite page read  → VFS xRead  → PageStore::get(cid)  → fetch from backend
SQLite page write → VFS xWrite → PageStore::put(page)  → store to backend, get new CID
SQLite sync       → VFS xSync  → PageStore::update_root() → update root pointer
```

Every write produces a new CID (content-addressed, immutable). The page table maps page numbers to CIDs. The page table itself is stored as a CID. The root pointer (DHT signed record or local file) points to the current page table CID.

## PageStore Trait

```rust
/// Content identifier — hash of page content
pub struct Cid([u8; 32]);

/// A database page (typically 4096 bytes)
pub struct Page {
    pub data: Vec<u8>,
}

/// Swappable storage backend for CraftSQL
pub trait PageStore: Send + Sync {
    /// Fetch a page by its content identifier
    fn get(&self, cid: &Cid) -> Result<Page>;
    
    /// Store a page, returns its content identifier
    fn put(&self, page: &Page) -> Result<Cid>;
    
    /// Update the root pointer to a new page table CID
    fn update_root(&self, new_root: Cid) -> Result<()>;
    
    /// Get the current root pointer
    fn current_root(&self) -> Result<Option<Cid>>;
}
```

### Backends

| Backend | Use Case | Implementation |
|---------|----------|----------------|
| Local disk | Development, testing, offline | Pages as files in a directory, root in a metadata file |
| CraftOBJ | Production distributed | Pages as CIDs on P2P network, root as DHT signed record |
| S3/R2 | Cloud hybrid | Pages as S3 objects keyed by CID, root in a known key |
| IPFS | Alternative P2P | Pages as IPFS blocks, root as IPNS record |

## Page Table

Maps SQLite page numbers to CIDs:

```
Page Table (itself a CID):
{
    page_count: 25000,
    entries: [
        (0, CID_abc),    // page 0 → CID_abc
        (1, CID_def),    // page 1 → CID_def
        ...
        (24999, CID_xyz) // page 24999 → CID_xyz
    ]
}
```

Size: 25,000 pages × 36 bytes (4-byte page number + 32-byte CID) = ~900 KB. Fits in a few CraftOBJ pieces.

On write:
1. SQLite writes page N with new data
2. VFS stores new page → gets new CID
3. Update page table: entry N → new CID
4. Store updated page table → new page table CID
5. Update root pointer → new page table CID

On commit (xSync):
- Root pointer update is the atomic commit point
- If crash before root update: old state preserved (COW)
- If crash after root update: new state committed

## SQLite VFS Interface

Key methods to implement:

```c
// Required VFS methods
int xOpen(sqlite3_vfs*, const char *zName, sqlite3_file*, int flags, int *pOutFlags);
int xDelete(sqlite3_vfs*, const char *zName, int syncDir);
int xAccess(sqlite3_vfs*, const char *zName, int flags, int *pResOut);

// Required file methods  
int xRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
int xWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);
int xTruncate(sqlite3_file*, sqlite3_int64 size);
int xSync(sqlite3_file*, int flags);
int xFileSize(sqlite3_file*, sqlite3_int64 *pSize);
```

**xRead(offset, amount):**
1. Calculate page number: `page_num = offset / page_size`
2. Look up CID in page table: `cid = page_table[page_num]`
3. Fetch page: `page = page_store.get(cid)`
4. Copy requested bytes to output buffer

**xWrite(offset, data):**
1. Calculate page number: `page_num = offset / page_size`
2. Store new page: `new_cid = page_store.put(data)`
3. Update page table entry: `page_table[page_num] = new_cid`
4. (Don't update root yet — wait for xSync)

**xSync():**
1. Store updated page table: `pt_cid = page_store.put(page_table)`
2. Update root: `page_store.update_root(pt_cid)`
3. This is the commit point

## Snapshots

A snapshot = saving the current root CID.

```rust
fn snapshot(db: &CraftSqlDb) -> Cid {
    db.current_root()  // That's it. One CID.
}

fn restore(db: &mut CraftSqlDb, snapshot: Cid) {
    db.update_root(snapshot)  // Instant restore.
}
```

Old page CIDs still exist in the storage backend. No data copied. Unlimited snapshots at zero storage cost (pages are shared via content-addressing).

## Sharing

A database is identified by its root CID (or a mutable pointer like a DHT signed record).

- **Read-only sharing**: give someone the root CID. They can open it as a read-only SQLite database.
- **Live sharing**: give someone the DHT key for your root pointer. They always see latest.
- **Fork**: take a root CID, start writing. Your writes create new CIDs. Original unchanged.

## Performance

| Operation | Local PageStore | CraftOBJ PageStore |
|-----------|----------------|-------------------|
| Read (cached) | <1ms | <1ms |
| Read (uncached) | <1ms | ~300-500ms (network) |
| Write | <1ms | <1ms (buffered) |
| Commit (xSync) | <5ms | ~300-500ms (root update) |

CraftOBJ latency is dominated by network round trips. Local caching is essential — after first read, pages are cached locally. Writes are buffered locally until commit.

## Implementation Plan

### Phase 1: Local PageStore + VFS
- Implement `LocalPageStore` (pages as files, root in metadata)
- Implement SQLite VFS in Rust (using `rusqlite` custom VFS or C FFI)
- Tests: basic CRUD, transactions, snapshots

### Phase 2: CraftOBJ PageStore
- Implement `CraftObjPageStore` (pages as CIDs on network)
- Wire to CraftOBJ daemon via IPC
- Tests: distributed reads, network latency handling

### Phase 3: CraftVFS
- Inode + dirent schema on CraftSQL
- FUSE mount
- Path resolution

## Dependencies

- `rusqlite` or direct SQLite C FFI for VFS implementation
- `sha2` for CID generation
- `serde` for page table serialization
- CraftOBJ client library (Phase 2)
