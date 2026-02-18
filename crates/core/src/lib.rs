//! CraftSQL Core — PageStore trait and CID types

use sha2::{Digest, Sha256};
use serde::{Serialize, Deserialize};

/// Content identifier — SHA-256 hash of page content
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Cid(pub [u8; 32]);

impl Cid {
    /// Compute CID from raw bytes
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = Sha256::digest(data);
        let mut out = [0u8; 32];
        out.copy_from_slice(&hash);
        Self(out)
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

impl std::fmt::Display for Cid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", &self.to_hex()[..16])
    }
}

/// A database page
#[derive(Debug, Clone)]
pub struct Page {
    pub data: Vec<u8>,
}

/// Result type for PageStore operations
pub type Result<T> = std::result::Result<T, PageStoreError>;

/// PageStore errors
#[derive(Debug, thiserror::Error)]
pub enum PageStoreError {
    #[error("page not found: {0}")]
    NotFound(Cid),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// Swappable storage backend for CraftSQL
pub trait PageStore: Send + Sync {
    /// Fetch a page by its content identifier
    fn get(&self, cid: &Cid) -> Result<Page>;

    /// Store a page, returns its content identifier
    fn put(&self, page: &Page) -> Result<Cid>;

    /// Update the default root pointer to a new page table CID
    fn update_root(&self, new_root: Cid) -> Result<()>;

    /// Get the current default root pointer
    fn current_root(&self) -> Result<Option<Cid>>;

    /// Save a named root pointer (snapshot/branch)
    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()>;

    /// Get a named root pointer
    fn get_named_root(&self, name: &str) -> Result<Option<Cid>>;

    /// Remove a named root pointer
    fn remove_named_root(&self, name: &str) -> Result<bool>;

    /// List all named root pointers
    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>>;
}

/// Diff between two PageTables — which pages changed
#[derive(Debug, Clone)]
pub struct PageTableDiff {
    /// Pages added or modified: (page_num, old_cid, new_cid)
    pub changed: Vec<(usize, Option<Cid>, Option<Cid>)>,
}

impl PageTable {
    /// Compute diff from `old` to `self` (new).
    /// Returns entries where CIDs differ.
    pub fn diff(&self, old: &PageTable) -> PageTableDiff {
        let max_len = self.entries.len().max(old.entries.len());
        let mut changed = Vec::new();

        for i in 0..max_len {
            let old_cid = old.get(i).copied();
            let new_cid = self.get(i).copied();
            if old_cid != new_cid {
                changed.push((i, old_cid, new_cid));
            }
        }

        PageTableDiff { changed }
    }
}

/// Page table — maps page numbers to CIDs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageTable {
    pub entries: Vec<Option<Cid>>,
}

impl PageTable {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Get CID for a page number
    pub fn get(&self, page_num: usize) -> Option<&Cid> {
        self.entries.get(page_num).and_then(|e| e.as_ref())
    }

    /// Set CID for a page number, growing the table if needed
    pub fn set(&mut self, page_num: usize, cid: Cid) {
        if page_num >= self.entries.len() {
            self.entries.resize(page_num + 1, None);
        }
        self.entries[page_num] = Some(cid);
    }

    /// Number of pages
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("page table serialization")
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> std::result::Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}

impl Default for PageTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_from_bytes() {
        let cid1 = Cid::from_bytes(b"hello");
        let cid2 = Cid::from_bytes(b"hello");
        let cid3 = Cid::from_bytes(b"world");
        assert_eq!(cid1, cid2);
        assert_ne!(cid1, cid3);
    }

    #[test]
    fn test_page_table() {
        let mut pt = PageTable::new();
        assert!(pt.is_empty());

        let cid = Cid::from_bytes(b"page data");
        pt.set(0, cid);
        pt.set(5, cid);

        assert_eq!(pt.len(), 6);
        assert_eq!(pt.get(0), Some(&cid));
        assert_eq!(pt.get(5), Some(&cid));
        assert_eq!(pt.get(1), None);
        assert_eq!(pt.get(100), None);
    }

    #[test]
    fn test_page_table_diff() {
        let mut old = PageTable::new();
        old.set(0, Cid::from_bytes(b"page A"));
        old.set(1, Cid::from_bytes(b"page B"));
        old.set(2, Cid::from_bytes(b"page C"));

        let mut new = PageTable::new();
        new.set(0, Cid::from_bytes(b"page A")); // unchanged
        new.set(1, Cid::from_bytes(b"page B modified")); // changed
        new.set(2, Cid::from_bytes(b"page C")); // unchanged
        new.set(3, Cid::from_bytes(b"page D")); // added

        let diff = new.diff(&old);
        assert_eq!(diff.changed.len(), 2); // page 1 changed, page 3 added
        assert_eq!(diff.changed[0].0, 1); // page 1
        assert_eq!(diff.changed[1].0, 3); // page 3
        assert!(diff.changed[1].1.is_none()); // was absent
        assert!(diff.changed[1].2.is_some()); // now present
    }

    #[test]
    fn test_page_table_diff_empty() {
        let a = PageTable::new();
        let b = PageTable::new();
        assert_eq!(a.diff(&b).changed.len(), 0);
    }

    #[test]
    fn test_page_table_serde() {
        let mut pt = PageTable::new();
        pt.set(0, Cid::from_bytes(b"page 0"));
        pt.set(3, Cid::from_bytes(b"page 3"));

        let bytes = pt.to_bytes();
        let pt2 = PageTable::from_bytes(&bytes).unwrap();

        assert_eq!(pt2.len(), pt.len());
        assert_eq!(pt2.get(0), pt.get(0));
        assert_eq!(pt2.get(3), pt.get(3));
    }
}
