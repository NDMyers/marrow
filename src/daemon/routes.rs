//! Axum route handlers for the daemon HTTP server.

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::Deserialize;
use std::sync::Arc;
use crate::daemon::pool::{RepoPool, spawn_eviction_loop};
use tokio::sync::{broadcast, mpsc};
use std::time::Duration;

// ── State ─────────────────────────────────────────────────────────────────────

/// Shared state threaded through all Axum route handlers.
#[derive(Clone)]
#[allow(dead_code)]
pub struct DaemonState {
    pub pool:       Arc<RepoPool>,
    /// Sender used to register new repo paths with the background watcher.
    pub watcher_tx: mpsc::Sender<std::path::PathBuf>,
    /// Dashboard broadcast channel for file-change events.
    pub dash_tx:    broadcast::Sender<crate::dashboard::DashboardEvent>,
}

impl DaemonState {
    pub fn new(
        watcher_tx: mpsc::Sender<std::path::PathBuf>,
        dash_tx: broadcast::Sender<crate::dashboard::DashboardEvent>,
    ) -> Self {
        let pool = Arc::new(RepoPool::new());
        // Evict connections idle 60+ minutes, check every 5 minutes.
        spawn_eviction_loop(
            Arc::clone(&pool),
            Duration::from_secs(60 * 60),
            Duration::from_secs(5 * 60),
        );
        Self { pool, watcher_tx, dash_tx }
    }

    /// Test constructor — channels are throwaway (receivers dropped immediately).
    /// All `watcher_tx.send()` calls are handled with `let _ = ...` so dropped
    /// receivers do not panic.
    #[cfg(test)]
    pub fn new_test() -> Self {
        let (watcher_tx, _rx) = mpsc::channel(1);
        let (dash_tx, _)      = broadcast::channel(4);
        Self {
            pool: Arc::new(RepoPool::new()),
            watcher_tx,
            dash_tx,
        }
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn build_router(state: DaemonState) -> Router {
    Router::new()
        .route("/api/health", get(handle_health))
        .route("/rpc/mcp",    post(handle_mcp))
        .route("/api/watch",  post(handle_watch))
        .with_state(state)
}

/// Address to bind when using TCP (Windows / fallback).
#[allow(dead_code)]
pub fn bind_address() -> std::net::SocketAddr {
    format!("127.0.0.1:{}", crate::ipc::DAEMON_PORT)
        .parse()
        .expect("valid socket address")
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({ "status": "ok" })))
}

#[derive(Deserialize)]
struct WatchRequest {
    path: String,
}

async fn handle_watch(
    State(_state): State<DaemonState>,
    Json(req): Json<WatchRequest>,
) -> impl IntoResponse {
    let path = std::path::PathBuf::from(&req.path);
    if !path.exists() {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({ "error": "path does not exist" })));
    }
    // Phase 1 stub: validate path only. Watcher registration is wired in Task 6.
    (StatusCode::OK, Json(serde_json::json!({ "watching": req.path })))
}

/// Forward raw MCP JSON-RPC from `marrow mcp` to the appropriate tool handler.
///
/// Parses the JSON body to extract an optional `workspace` field from either
/// `params.arguments.workspace` or `params.workspace`. If present and the path
/// exists, the pool is touched (opening a connection if needed) and the path is
/// forwarded to the background watcher.
///
/// Phase 2: pool/watcher registration. Full tool dispatch into the pool (replacing
/// ContextEngine) is deferred to a follow-on refactor.
async fn handle_mcp(
    State(state): State<DaemonState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Extract optional workspace path from the JSON payload.
    let workspace: Option<std::path::PathBuf> = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.pointer("/params/arguments/workspace")
                .or_else(|| v.pointer("/params/workspace"))
                .and_then(|w| w.as_str())
                .map(|s| std::path::PathBuf::from(s))
        });

    if let Some(path) = workspace {
        if path.exists() {
            // Touch the pool entry (opens DB if not already open).
            let _ = state.pool.get_or_open(&path).await;
            // Register with the background file watcher (best-effort).
            let _ = state.watcher_tx.send(path).await;
        }
    }

    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn health_returns_200() {
        let app = build_router(DaemonState::new_test());
        let response = app
            .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mcp_no_workspace_echoes_body() {
        let app = build_router(DaemonState::new_test());
        let payload = br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.as_ref()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], payload);
    }

    #[tokio::test]
    async fn mcp_routes_to_pool_connection() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = DaemonState::new_test();

        let payload = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "run_pipeline",
                "arguments": {
                    "intent": "analyze_repo",
                    "workspace": dir.path().to_string_lossy()
                }
            }
        });

        let app = build_router(state.clone());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/mcp")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // Verify pool now has an entry for this path
        let map = state.pool.inner.read().await;
        assert!(!map.is_empty(), "pool should have opened a connection");
    }
}
