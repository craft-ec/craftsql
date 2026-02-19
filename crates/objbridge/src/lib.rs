//! CraftOBJ Network Bridge — connects CraftSQL to the CraftOBJ daemon via JSON-RPC IPC.
//!
//! Implements [`NetworkBackend`] for `CraftObjPageStore` by communicating with
//! the CraftOBJ daemon over a Unix socket using JSON-RPC 2.0.
//!
//! # Architecture
//!
//! ```text
//! SQLite ←→ CraftVFS ←→ CraftObjPageStore<DaemonBackend> ←→ craftobj daemon
//!                                                              (Unix socket)
//! ```
//!
//! Pages are published as raw content via the daemon's `publish` RPC.
//! Root pointers are managed locally (craftsql-specific, not stored in CraftOBJ DHT).

use craftsql_core::{Cid, PageStoreError, Result};
use craftsql_objstore::NetworkBackend;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// JSON-RPC 2.0 request.
#[derive(Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<serde_json::Value>,
    id: u64,
}

/// JSON-RPC 2.0 response.
#[derive(Deserialize)]
struct RpcResponse {
    result: Option<serde_json::Value>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

/// NetworkBackend that talks to the CraftOBJ daemon over Unix socket IPC.
///
/// Each RPC call opens a fresh connection (the daemon uses one-shot connections).
/// Page data is transferred via temp files (daemon's publish/fetch API is file-based).
pub struct DaemonBackend {
    socket_path: String,
    next_id: AtomicU64,
    timeout: Duration,
}

impl DaemonBackend {
    /// Create a new backend connecting to the given Unix socket path.
    pub fn new(socket_path: &str) -> Self {
        Self {
            socket_path: socket_path.to_string(),
            next_id: AtomicU64::new(1),
            timeout: Duration::from_secs(30),
        }
    }

    /// Create with default socket path (`/tmp/craftobj.sock`).
    pub fn default_socket() -> Self {
        Self::new("/tmp/craftobj.sock")
    }

    /// Set the connection timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Send a JSON-RPC request and return the result.
    fn rpc_call(&self, method: &str, params: Option<serde_json::Value>) -> Result<serde_json::Value> {
        let stream = UnixStream::connect(&self.socket_path)
            .map_err(|e| PageStoreError::Storage(format!("daemon not running at {}: {}", self.socket_path, e)))?;
        stream.set_read_timeout(Some(self.timeout))
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        stream.set_write_timeout(Some(self.timeout))
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = RpcRequest {
            jsonrpc: "2.0",
            method,
            params,
            id,
        };

        let json = serde_json::to_string(&request)
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;

        let mut writer = stream.try_clone()
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        writer.write_all(format!("{}\n", json).as_bytes())
            .map_err(|e| PageStoreError::Storage(format!("write to daemon: {}", e)))?;

        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line)
            .map_err(|e| PageStoreError::Storage(format!("read from daemon: {}", e)))?;

        let response: RpcResponse = serde_json::from_str(line.trim())
            .map_err(|e| PageStoreError::Storage(format!("parse daemon response: {}", e)))?;

        if let Some(err) = response.error {
            return Err(PageStoreError::Storage(format!(
                "daemon error {}: {}", err.code, err.message
            )));
        }

        response.result.ok_or_else(|| PageStoreError::Storage("empty daemon response".into()))
    }
}

impl NetworkBackend for DaemonBackend {
    fn publish_page(&self, data: &[u8]) -> Result<Cid> {
        // Write data to temp file, call daemon's publish RPC
        let mut tmp = tempfile::NamedTempFile::new()
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        tmp.write_all(data)
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;
        tmp.flush()
            .map_err(|e| PageStoreError::Storage(e.to_string()))?;

        let result = self.rpc_call("publish", Some(serde_json::json!({
            "path": tmp.path().to_string_lossy(),
        })))?;

        let _cid_hex = result.get("cid")
            .and_then(|v| v.as_str())
            .ok_or_else(|| PageStoreError::Storage("missing cid in publish response".into()))?;

        // The CraftOBJ CID is SHA-256 of the (possibly encrypted) content.
        // For unencrypted pages, this matches our Cid::from_bytes(data).
        // Return our local CID (content-addressed by page data).
        Ok(Cid::from_bytes(data))
    }

    fn fetch_page(&self, cid: &Cid) -> Result<Vec<u8>> {
        let cid_hex = hex::encode(cid.0);
        let output_path = std::env::temp_dir().join(format!("craftsql-fetch-{}", &cid_hex[..16]));

        let result = self.rpc_call("fetch", Some(serde_json::json!({
            "cid": cid_hex,
            "output": output_path.to_string_lossy(),
        })))?;

        let path = result.get("path")
            .and_then(|v| v.as_str())
            .map(|s| std::path::PathBuf::from(s))
            .unwrap_or(output_path);

        let data = std::fs::read(&path)
            .map_err(|e| PageStoreError::Storage(format!("read fetched page: {}", e)))?;

        // Clean up temp file
        let _ = std::fs::remove_file(&path);

        // Verify CID
        let actual = Cid::from_bytes(&data);
        if actual != *cid {
            return Err(PageStoreError::Storage(format!(
                "CID mismatch after fetch: expected {}, got {}", cid, actual
            )));
        }

        Ok(data)
    }

    fn get_root(&self) -> Result<Option<Cid>> {
        // Root pointers are craftsql-specific — not stored in CraftOBJ.
        // CraftObjPageStore manages roots locally via disk cache.
        // Return None to let the local cache be authoritative.
        Ok(None)
    }

    fn set_root(&self, _cid: Cid) -> Result<()> {
        // Root management is local-only for now.
        Ok(())
    }

    fn get_named_root(&self, _name: &str) -> Result<Option<Cid>> {
        Ok(None)
    }

    fn set_named_root(&self, _name: &str, _cid: Cid) -> Result<()> {
        Ok(())
    }

    fn remove_named_root(&self, _name: &str) -> Result<bool> {
        Ok(false)
    }

    fn list_named_roots(&self) -> Result<Vec<(String, Cid)>> {
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_request_serialization() {
        let req = RpcRequest {
            jsonrpc: "2.0",
            method: "publish",
            params: Some(serde_json::json!({"path": "/tmp/test"})),
            id: 1,
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
        assert!(json.contains("\"method\":\"publish\""));
    }

    #[test]
    fn test_daemon_backend_creation() {
        let backend = DaemonBackend::new("/tmp/test.sock");
        assert_eq!(backend.socket_path, "/tmp/test.sock");
    }

    #[test]
    fn test_default_socket() {
        let backend = DaemonBackend::default_socket();
        assert_eq!(backend.socket_path, "/tmp/craftobj.sock");
    }

    #[test]
    fn test_daemon_not_running() {
        let backend = DaemonBackend::new("/tmp/nonexistent-craftsql-test.sock");
        let result = backend.publish_page(b"test data");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("daemon not running"));
    }

    #[test]
    fn test_root_operations_are_noop() {
        let backend = DaemonBackend::new("/tmp/nonexistent.sock");
        // These should succeed without connecting to daemon
        assert_eq!(backend.get_root().unwrap(), None);
        backend.set_root(Cid([0; 32])).unwrap();
        assert_eq!(backend.get_named_root("test").unwrap(), None);
        backend.set_named_root("test", Cid([0; 32])).unwrap();
        assert!(!backend.remove_named_root("test").unwrap());
        assert!(backend.list_named_roots().unwrap().is_empty());
    }
}
