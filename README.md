# CraftSQL

A distributed relational database engine built as a SQLite VFS backed by content-addressed storage.

## Architecture

CraftSQL is NOT a new database engine. It is a **SQLite VFS (Virtual File System) implementation** that maps page I/O to content-addressed storage operations.

```
SQLite (unmodified)     ← SQL, B-trees, query planner, ACID transactions
        ↓
CraftSQL VFS (~500 LOC) ← page number → CID mapping
        ↓
PageStore trait          ← swappable storage backend
```

### PageStore

```rust
trait PageStore {
    fn get(&self, cid: &Cid) -> Result<Page>;
    fn put(&self, page: &Page) -> Result<Cid>;
    fn update_root(&self, new_root: Cid) -> Result<()>;
}
```

**Backends:**
- **CraftOBJ** — distributed P2P storage (RLNC erasure, self-healing)
- **Local disk** — SQLite-style, for development/testing/offline use
- **S3/R2** — CID-based versioning on traditional infrastructure
- **IPFS** — alternative CID network

### Features (inherited from SQLite + CID addressing)

- Full SQL with query planner, indexes, ACID transactions
- **Snapshots**: bookmark a page table CID — instant, free, unlimited
- **Versioning**: every mutation creates new CIDs, old versions persist
- **Sharing**: anyone can read a public database by its root CID
- **Single-owner writes**: owner signs mutations, no consensus needed
- **Every SQLite tool and binding works unchanged**

## Part of Craftec

```
CraftNET  — Network transport (DHT, PEX, P2P)
CraftOBJ  — Distributed object storage (RLNC erasure, CIDs)
CraftSQL  — Database engine (this repo)
CraftVFS  — Distributed filesystem (built on CraftSQL)
```

See [VISION.md](../docs/VISION.md) for the full architecture.

## Status

Early development. Design phase.

## License

TBD
