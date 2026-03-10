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
// NOTE: Only RwLock is imported from tokio — do NOT import tokio::sync::Mutex here.
// All connection-level mutexes use std::sync::Mutex (see pool.rs, Task 4).
use tokio::sync::RwLock;

// ── State ─────────────────────────────────────────────────────────────────────

/// Shared state threaded through all Axum route handlers.
///
/// Phase 1 scaffold — replaced entirely in Task 4 with pool + channel fields.
#[derive(Clone)]
pub struct DaemonState {
    _placeholder: Arc<RwLock<()>>,
}

impl DaemonState {
    pub fn new() -> anyhow::Result<Self> {
        Ok(Self { _placeholder: Arc::new(RwLock::new(())) })
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::new().expect("DaemonState::new_test")
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
/// Phase 1: naively echoes the request back (stub).
async fn handle_mcp(
    State(_state): State<DaemonState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // Phase 1 stub: echo request back as-is so the proxy wiring can be tested.
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
    async fn mcp_echo_stub_returns_body() {
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
}
