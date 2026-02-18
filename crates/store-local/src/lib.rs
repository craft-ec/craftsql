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
        let path = self.root_path();
        if !path.exists() {
            return Ok(None);
        }
        let hex_str = fs::read_to_string(&path)?;
        let bytes = hex::decode(hex_str.trim())
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        let mut cid = [0u8; 32];
        cid.copy_from_slice(&bytes);
        Ok(Some(Cid(cid)))
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
