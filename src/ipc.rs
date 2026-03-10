//! IPC transport layer.
//!
//! On Unix we use a Unix Domain Socket (`~/.marrow/daemon.sock`).
//! On Windows we fall back to localhost TCP (`127.0.0.1:DAEMON_PORT`).

use anyhow::{Context as _, Result};
use std::path::{Path, PathBuf};

pub const DAEMON_PORT: u16 = 17_983; // TCP fallback (Windows / firewall test)

/// Canonical socket path used by both the daemon server and IPC clients.
pub fn default_sock_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".marrow")
        .join("daemon.sock")
}

// ── Client ────────────────────────────────────────────────────────────────────

/// Thin HTTP client that speaks to the daemon over UDS (Unix) or TCP (Windows).
pub struct IpcClient {
    inner: reqwest::Client,
    base_url: String,
}

impl IpcClient {
    /// Create a client that connects via the given Unix socket path.
    #[cfg(unix)]
    pub fn new_unix(sock: &Path) -> Self {
        use reqwest::ClientBuilder;

        let client = ClientBuilder::new()
            .unix_socket(sock)
            .build()
            .expect("reqwest unix socket client");
        Self {
            inner: client,
            base_url: "http://localhost".to_string(), // host is ignored for UDS
        }
    }

    /// Create a client that connects via TCP (Windows or fallback).
    pub fn new_tcp(port: u16) -> Self {
        Self {
            inner: reqwest::Client::new(),
            base_url: format!("http://127.0.0.1:{port}"),
        }
    }

    /// Returns `true` if the daemon responds to `GET /api/health`.
    pub async fn health_check(&self) -> Result<bool> {
        let url = format!("{}/api/health", self.base_url);
        match self.inner.get(&url).send().await {
            Ok(resp) => Ok(resp.status().is_success()),
            Err(e) if is_connection_refused(&e) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Forward raw MCP bytes to `POST /rpc/mcp` and return the response body.
    pub async fn forward_mcp(&self, body: Vec<u8>) -> Result<Vec<u8>> {
        let url = format!("{}/rpc/mcp", self.base_url);
        let resp = self
            .inner
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .context("forwarding MCP request to daemon")?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Register a new repository path for background watching.
    pub async fn register_watch(&self, path: &Path) -> Result<()> {
        let url = format!("{}/api/watch", self.base_url);
        self.inner
            .post(&url)
            .json(&serde_json::json!({ "path": path.to_string_lossy() }))
            .send()
            .await
            .context("registering watch path with daemon")?;
        Ok(())
    }

    /// Send a shutdown signal to the daemon via `POST /api/shutdown`.
    pub async fn shutdown(&self) -> Result<()> {
        let url = format!("{}/api/shutdown", self.base_url);
        self.inner.post(&url).send().await.ok(); // fire-and-forget; daemon is exiting
        Ok(())
    }
}

fn is_connection_refused(e: &reqwest::Error) -> bool {
    use std::error::Error as StdError;
    if let Some(src) = e.source() {
        let msg = src.to_string();
        msg.contains("connection refused") || msg.contains("No such file")
    } else {
        false
    }
}

/// Build the platform-appropriate `IpcClient` pointing at the default socket/port.
pub fn default_client() -> IpcClient {
    #[cfg(unix)]
    {
        IpcClient::new_unix(&default_sock_path())
    }
    #[cfg(not(unix))]
    {
        IpcClient::new_tcp(DAEMON_PORT)
    }
}

// ── Auto-spawn ────────────────────────────────────────────────────────────────

/// Ensure the daemon is running. If the health check fails, spawn the daemon
/// process in the background and retry up to 10 times with 50ms delays.
pub async fn ensure_daemon_running() -> Result<()> {
    let client = default_client();

    if client.health_check().await.unwrap_or(false) {
        return Ok(());
    }

    // Spawn daemon — fully detached so it outlives this process.
    let exe = std::env::current_exe().context("resolving current exe path")?;
    std::process::Command::new(exe)
        .arg("daemon")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .context("spawning daemon process")?;

    // Retry loop
    for i in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if client.health_check().await.unwrap_or(false) {
            return Ok(());
        }
        if i == 9 {
            anyhow::bail!("daemon did not start within 500ms");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sock_path_ends_with_daemon_sock() {
        let p = default_sock_path();
        assert!(p.ends_with("daemon.sock"), "unexpected path: {}", p.display());
    }
}
