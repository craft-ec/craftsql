//! Local disk PageStore — pages as files, root in metadata file.
//! For development, testing, and offline single-machine use.

use craftsql_core::{Cid, Page, PageStore, PageStoreError, Result};
use std::fs;
use std::path::{Path, PathBuf};

pub struct LocalPageStore {
    dir: PathBuf,
}

impl LocalPageStore {
    pub fn new(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir.join("pages"))?;
        Ok(Self { dir: dir.to_path_buf() })
    }

    fn page_path(&self, cid: &Cid) -> PathBuf {
        self.dir.join("pages").join(hex::encode(cid.0))
    }

    fn root_path(&self) -> PathBuf {
        self.dir.join("root")
    }

    fn refs_dir(&self) -> PathBuf {
        self.dir.join("refs")
    }

    fn ref_path(&self, name: &str) -> PathBuf {
        // Sanitize name to avoid path traversal
        let safe_name: String = name.chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
            .collect();
        self.refs_dir().join(safe_name)
    }

    fn read_cid_file(path: &Path) -> Result<Option<Cid>> {
        if !path.exists() {
            return Ok(None);
        }
        let hex_str = fs::read_to_string(path)?;
        let bytes = hex::decode(hex_str.trim())
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        let mut cid = [0u8; 32];
        cid.copy_from_slice(&bytes);
        Ok(Some(Cid(cid)))
    }
}

impl PageStore for LocalPageStore {
    fn get(&self, cid: &Cid) -> Result<Page> {
        let path = self.page_path(cid);
        let data = fs::read(&path).map_err(|_| PageStoreError::NotFound(*cid))?;
        Ok(Page { data })
    }

    fn put(&self, page: &Page) -> Result<Cid> {
        let cid = Cid::from_bytes(&page.data);
        let path = self.page_path(&cid);
        if !path.exists() {
            fs::write(&path, &page.data)?;
        }
        Ok(cid)
    }

    fn update_root(&self, new_root: Cid) -> Result<()> {
        fs::write(self.root_path(), hex::encode(new_root.0))?;
        Ok(())
    }

    fn current_root(&self) -> Result<Option<Cid>> {
        Self::read_cid_file(&self.root_path())
    }

    fn set_named_root(&self, name: &str, cid: Cid) -> Result<()> {
        fs::create_dir_all(self.refs_dir())?;
        fs::write(self.ref_path(name), hex::encode(cid.0))?;
        Ok(())
    }

    fn get_named_root(&self, name: &str) -> Result<Option<Cid>> {
        Self::read_cid_file(&self.ref_path(name))
    }

    fn remove_named_root(&self, name: &str) -> Result<bool> {
        let path = self.ref_path(name);
        if path.exists() {
            fs::remove_file(&path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>> {
        let refs_dir = self.refs_dir();
        if !refs_dir.exists() {
            return Ok(Vec::new());
        }
        let mut roots = Vec::new();
        for entry in fs::read_dir(&refs_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(cid) = Self::read_cid_file(&entry.path())? {
                roots.push((name, cid));
            }
        }
        roots.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(roots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use craftsql_core::PageTable;

    fn temp_dir() -> PathBuf {
        std::env::temp_dir().join(format!("craftsql-test-{}", std::process::id()))
    }

    #[test]
    fn test_put_get() {
        let dir = temp_dir().join("put_get");
        let store = LocalPageStore::new(&dir).unwrap();

        let page = Page { data: b"hello world".to_vec() };
        let cid = store.put(&page).unwrap();

        let retrieved = store.get(&cid).unwrap();
        assert_eq!(retrieved.data, page.data);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_get_not_found() {
        let dir = temp_dir().join("not_found");
        let store = LocalPageStore::new(&dir).unwrap();

        let cid = Cid::from_bytes(b"nonexistent");
        assert!(store.get(&cid).is_err());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_dedup() {
        let dir = temp_dir().join("dedup");
        let store = LocalPageStore::new(&dir).unwrap();

        let page = Page { data: b"same content".to_vec() };
        let cid1 = store.put(&page).unwrap();
        let cid2 = store.put(&page).unwrap();
        assert_eq!(cid1, cid2);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_root_pointer() {
        let dir = temp_dir().join("root");
        let store = LocalPageStore::new(&dir).unwrap();

        assert_eq!(store.current_root().unwrap(), None);

        let cid = Cid::from_bytes(b"root page table");
        store.update_root(cid).unwrap();

        assert_eq!(store.current_root().unwrap(), Some(cid));

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_named_roots() {
        let dir = temp_dir().join("named_roots");
        let store = LocalPageStore::new(&dir).unwrap();

        // No roots initially
        assert!(store.list_named_roots().unwrap().is_empty());
        assert_eq!(store.get_named_root("snap1").unwrap(), None);

        // Set some named roots
        let cid1 = Cid::from_bytes(b"snapshot 1");
        let cid2 = Cid::from_bytes(b"snapshot 2");
        store.set_named_root("snap1", cid1).unwrap();
        store.set_named_root("snap2", cid2).unwrap();

        assert_eq!(store.get_named_root("snap1").unwrap(), Some(cid1));
        assert_eq!(store.get_named_root("snap2").unwrap(), Some(cid2));

        let roots = store.list_named_roots().unwrap();
        assert_eq!(roots.len(), 2);
        assert_eq!(roots[0], ("snap1".to_string(), cid1));
        assert_eq!(roots[1], ("snap2".to_string(), cid2));

        // Remove one
        assert!(store.remove_named_root("snap1").unwrap());
        assert!(!store.remove_named_root("snap1").unwrap()); // already gone
        assert_eq!(store.list_named_roots().unwrap().len(), 1);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_page_table_roundtrip() {
        let dir = temp_dir().join("page_table");
        let store = LocalPageStore::new(&dir).unwrap();

        let mut pt = PageTable::new();
        pt.set(0, Cid::from_bytes(b"page 0 data"));
        pt.set(1, Cid::from_bytes(b"page 1 data"));

        // Store page table as a page
        let pt_page = Page { data: pt.to_bytes() };
        let pt_cid = store.put(&pt_page).unwrap();
        store.update_root(pt_cid).unwrap();

        // Retrieve and verify
        let root = store.current_root().unwrap().unwrap();
        let pt_data = store.get(&root).unwrap();
        let pt2 = PageTable::from_bytes(&pt_data.data).unwrap();

        assert_eq!(pt2.get(0), pt.get(0));
        assert_eq!(pt2.get(1), pt.get(1));

        fs::remove_dir_all(&dir).ok();
    }
}
