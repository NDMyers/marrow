//! IPC transport layer.
//!
//! On Unix we use a Unix Domain Socket (`~/.marrow/daemon.sock`).
//! On Windows we fall back to localhost TCP (`127.0.0.1:DAEMON_PORT`).

#![allow(dead_code)]

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
            .context("forwarding MCP request to daemon")?
            .error_for_status()
            .context("daemon returned error status")?;
        Ok(resp.bytes().await?.to_vec())
    }

    /// Register a new repository path for background watching.
    /// M-10 FIX: Check HTTP response status and return errors to callers.
    pub async fn register_watch(&self, path: &Path) -> Result<()> {
        let url = format!("{}/api/watch", self.base_url);
        let resp = self
            .inner
            .post(&url)
            .json(&serde_json::json!({ "path": path.to_string_lossy() }))
            .send()
            .await
            .context("registering watch path with daemon")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("watch registration failed (HTTP {status}): {body}");
        }
        Ok(())
    }

    pub async fn start_activity(
        &self,
        kind: crate::activity::ActivityKind,
        workspace_id: Option<String>,
        detail: String,
    ) -> Result<Option<String>> {
        let url = format!("{}/api/activity/start", self.base_url);
        let resp = self
            .inner
            .post(&url)
            .json(&serde_json::json!({
                "kind": kind,
                "workspace_id": workspace_id,
                "detail": detail,
            }))
            .send()
            .await
            .context("recording activity with daemon")?;
        if !resp.status().is_success() {
            return Ok(None);
        }
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        Ok(body
            .get("id")
            .and_then(|value| value.as_str())
            .map(str::to_string))
    }

    pub async fn finish_activity(
        &self,
        id: &str,
        state: crate::activity::ActivityState,
        detail: String,
    ) -> Result<()> {
        let url = format!("{}/api/activity/finish", self.base_url);
        let _ = self
            .inner
            .post(&url)
            .json(&serde_json::json!({
                "id": id,
                "state": state,
                "detail": detail,
            }))
            .send()
            .await
            .context("finishing activity with daemon")?;
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
    #[cfg(unix)]
    {
        tokio::process::Command::new(&exe)
            .arg("daemon")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .process_group(0) // detach into its own process group; prevents SIGHUP on parent exit
            .spawn()
            .context("spawning daemon process")?;
    }
    #[cfg(not(unix))]
    {
        tokio::process::Command::new(&exe)
            .arg("daemon")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .context("spawning daemon process")?;
    }

    // Retry loop — poll up to 10 times with 50 ms gaps (500 ms total).
    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if client.health_check().await.unwrap_or(false) {
            return Ok(());
        }
    }
    anyhow::bail!("daemon did not start within 500ms");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_sock_path_ends_with_daemon_sock() {
        let p = default_sock_path();
        assert!(
            p.ends_with("daemon.sock"),
            "unexpected path: {}",
            p.display()
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn health_roundtrip_over_uds() {
        let sock = std::env::temp_dir().join("marrow_ipc_test.sock");
        let _ = std::fs::remove_file(&sock);

        let sock_path = sock.clone();
        let listener = tokio::net::UnixListener::bind(&sock_path).unwrap();
        tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let io = hyper_util::rt::TokioIo::new(stream);
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(
                        io,
                        hyper::service::service_fn(|_req| async {
                            Ok::<_, std::convert::Infallible>(hyper::Response::new(
                                http_body_util::Empty::<bytes::Bytes>::new(),
                            ))
                        }),
                    )
                    .await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let status = IpcClient::new_unix(&sock).health_check().await.unwrap();
        assert!(status, "health check should return true over UDS");
        std::fs::remove_file(&sock).ok();
    }
}
