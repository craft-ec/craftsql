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

    /// Update the root pointer to a new page table CID
    fn update_root(&self, new_root: Cid) -> Result<()>;

    /// Get the current root pointer
    fn current_root(&self) -> Result<Option<Cid>>;
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
