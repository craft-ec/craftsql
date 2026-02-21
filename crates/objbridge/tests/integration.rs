//! Integration test for the CraftOBJ bridge.
//!
//! Uses a mock daemon (Unix socket server) to test the full pipeline:
//! SQLite → CraftVFS → CraftObjPageStore<DaemonBackend> → mock daemon

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::sync::{Arc, Mutex};

use craftsql_core::Cid;

/// Mock CraftOBJ daemon that handles publish/fetch over a Unix socket.
struct MockDaemon {
    socket_path: String,
    /// Stored content: CID hex → file data
    store: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl MockDaemon {
    fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
            store: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Start listening in a background thread. Returns a handle to stop it.
    fn start(&self) -> std::thread::JoinHandle<()> {
        let path = self.socket_path.clone();
        let store = self.store.clone();

        // Remove stale socket
        let _ = std::fs::remove_file(&path);

        let listener = UnixListener::bind(&path).expect("bind mock daemon socket");
        // Set non-blocking so we can check for shutdown, but we'll use accept timeout
        listener.set_nonblocking(false).ok();

        std::thread::spawn(move || {
            // Accept connections until the socket file is removed
            for stream in listener.incoming() {
                let stream = match stream {
                    Ok(s) => s,
                    Err(_) => break,
                };

                let store = store.clone();
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let mut writer = stream;
                    let mut line = String::new();

                    if reader.read_line(&mut line).is_err() {
                        return;
                    }

                    let request: serde_json::Value = match serde_json::from_str(line.trim()) {
                        Ok(v) => v,
                        Err(_) => return,
                    };

                    let method = request["method"].as_str().unwrap_or("");
                    let params = request.get("params");
                    let id = request["id"].as_u64().unwrap_or(0);

                    let result = match method {
                        "publish" => {
                            let path = params
                                .and_then(|p| p.get("path"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            match std::fs::read(path) {
                                Ok(data) => {
                                    let cid = Cid::from_bytes(&data);
                                    let cid_hex = hex::encode(cid.0);
                                    store.lock().unwrap().insert(cid_hex.clone(), data.clone());
                                    Ok(serde_json::json!({
                                        "cid": cid_hex,
                                        "size": data.len(),
                                        "segments": 1,
                                    }))
                                }
                                Err(e) => Err(format!("read file: {}", e)),
                            }
                        }
                        "fetch" => {
                            let cid_hex = params
                                .and_then(|p| p.get("cid"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let output = params
                                .and_then(|p| p.get("output"))
                                .and_then(|v| v.as_str())
                                .unwrap_or("/tmp/mock-fetch-out");

                            match store.lock().unwrap().get(cid_hex) {
                                Some(data) => {
                                    std::fs::write(output, data).ok();
                                    Ok(serde_json::json!({ "path": output }))
                                }
                                None => Err(format!("content not found: {}", cid_hex)),
                            }
                        }
                        "kv.put" => {
                            let key = params.and_then(|p| p.get("key")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let value = params.and_then(|p| p.get("value")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                            store.lock().unwrap().insert(format!("__kv__{}", key), value.into_bytes());
                            Ok(serde_json::json!({"ok": true}))
                        }
                        "kv.get" => {
                            let key = params.and_then(|p| p.get("key")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let s = store.lock().unwrap();
                            match s.get(&format!("__kv__{}", key)) {
                                Some(data) => {
                                    let val = String::from_utf8_lossy(data).to_string();
                                    Ok(serde_json::json!({"key": key, "value": val}))
                                }
                                None => Ok(serde_json::json!({"key": key, "value": null})),
                            }
                        }
                        "kv.delete" => {
                            let key = params.and_then(|p| p.get("key")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let existed = store.lock().unwrap().remove(&format!("__kv__{}", key)).is_some();
                            Ok(serde_json::json!({"deleted": existed}))
                        }
                        "kv.list" => {
                            let prefix = params.and_then(|p| p.get("prefix")).and_then(|v| v.as_str()).unwrap_or("").to_string();
                            let s = store.lock().unwrap();
                            let keys: Vec<String> = s.keys()
                                .filter(|k| k.starts_with("__kv__"))
                                .map(|k| k.strip_prefix("__kv__").unwrap().to_string())
                                .filter(|k| k.starts_with(&prefix))
                                .collect();
                            Ok(serde_json::json!({"keys": keys}))
                        }
                        _ => Err(format!("unknown method: {}", method)),
                    };

                    let response = match result {
                        Ok(val) => serde_json::json!({
                            "jsonrpc": "2.0",
                            "result": val,
                            "id": id,
                        }),
                        Err(msg) => serde_json::json!({
                            "jsonrpc": "2.0",
                            "error": { "code": -32000, "message": msg },
                            "id": id,
                        }),
                    };

                    let resp_str = serde_json::to_string(&response).unwrap();
                    let _ = writer.write_all(format!("{}\n", resp_str).as_bytes());
                });
            }
        })
    }
}

impl Drop for MockDaemon {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

#[test]
fn test_publish_and_fetch_via_mock_daemon() {
    use craftsql_objbridge::DaemonBackend;
    use craftsql_objstore::NetworkBackend;

    let socket_path = format!("/tmp/craftsql-test-{}.sock", std::process::id());
    let daemon = MockDaemon::new(&socket_path);
    let _handle = daemon.start();

    // Give the listener a moment to bind
    std::thread::sleep(std::time::Duration::from_millis(50));

    let backend = DaemonBackend::new(&socket_path);

    // Publish a page
    let data = b"hello craftsql bridge";
    let cid = backend.publish_page(data).unwrap();
    assert_eq!(cid, Cid::from_bytes(data));

    // Fetch it back
    let fetched = backend.fetch_page(&cid).unwrap();
    assert_eq!(fetched, data);
}

#[test]
fn test_page_store_with_mock_daemon() {
    use craftsql_core::{Page, PageStore, PageTable};
    use craftsql_objbridge::DaemonBackend;
    use craftsql_objstore::CraftObjPageStore;

    let socket_path = format!("/tmp/craftsql-ps-test-{}.sock", std::process::id());
    let daemon = MockDaemon::new(&socket_path);
    let _handle = daemon.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let backend = DaemonBackend::new(&socket_path);
    let tmp = tempfile::tempdir().unwrap();
    let store = CraftObjPageStore::new(tmp.path(), backend).unwrap();

    // Put a page (locally cached only)
    let page = Page { data: vec![0xAB; 4096] };
    let cid = store.put(&page).unwrap();

    // Get it back (should be cache hit)
    let got = store.get(&cid).unwrap();
    assert_eq!(got.data, page.data);

    // Root management — build a proper page table for update_root
    assert_eq!(store.current_root().unwrap(), None);
    let mut pt = PageTable::new();
    pt.set(0, cid);
    let pt_data = pt.to_bytes();
    let pt_cid = Cid::from_bytes(&pt_data);
    store.put(&Page { data: pt_data }).unwrap();
    store.update_root(pt_cid).unwrap();

    // Root is now the bundle CID (not page table CID)
    let root = store.current_root().unwrap();
    assert!(root.is_some());
}

#[test]
fn test_full_vfs_sql_with_mock_daemon() {
    use craftsql_objbridge::DaemonBackend;
    use craftsql_objstore::CraftObjPageStore;

    let socket_path = format!("/tmp/craftsql-vfs-test-{}.sock", std::process::id());
    let daemon = MockDaemon::new(&socket_path);
    let _handle = daemon.start();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let backend = DaemonBackend::new(&socket_path);
    let tmp = tempfile::tempdir().unwrap();
    let store = CraftObjPageStore::new(tmp.path(), backend).unwrap();

    // Register VFS
    let vfs_name = format!("craftsql_bridge_test_{}", std::process::id());
    craftsql_vfs::register(&vfs_name, store).unwrap();

    // Open SQLite DB through the VFS
    let db_path = format!("/craftsql/{}/db", vfs_name);
    let flags = rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
        | rusqlite::OpenFlags::SQLITE_OPEN_CREATE;
    let db = rusqlite::Connection::open_with_flags_and_vfs(&db_path, flags, &vfs_name).unwrap();
    db.execute_batch("PRAGMA journal_mode=DELETE;").unwrap();

    // Create table and insert data
    db.execute_batch("
        CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL);
        INSERT INTO items VALUES (1, 'alpha', 1.5);
        INSERT INTO items VALUES (2, 'beta', 2.7);
        INSERT INTO items VALUES (3, 'gamma', 3.14);
    ").unwrap();

    // Query and verify
    let names: Vec<String> = db
        .prepare("SELECT name FROM items ORDER BY id").unwrap()
        .query_map([], |r: &rusqlite::Row| r.get(0)).unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(names, vec!["alpha", "beta", "gamma"]);

    let sum: f64 = db.query_row(
        "SELECT SUM(value) FROM items", [], |r: &rusqlite::Row| r.get(0)
    ).unwrap();
    assert!((sum - 7.34).abs() < 0.001);
}
