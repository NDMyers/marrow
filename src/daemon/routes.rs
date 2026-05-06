//! Axum route handlers for the daemon HTTP server.

use crate::daemon::pool::{spawn_eviction_loop, RepoPool};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

// ── State ─────────────────────────────────────────────────────────────────────

/// Shared state threaded through all Axum route handlers.
#[derive(Clone)]
pub struct DaemonState {
    pub pool: Arc<RepoPool>,
    /// Sender used to register new repo paths with the background watcher.
    pub watcher_tx: mpsc::Sender<std::path::PathBuf>,
    /// Dashboard broadcast channel for file-change events.
    #[allow(dead_code)]
    pub dash_tx: broadcast::Sender<crate::dashboard::DashboardEvent>,
    /// One-shot sender for graceful shutdown. Consumed on first `/api/shutdown` call.
    pub shutdown_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    /// Approved workspace roots for watch registration. Paths outside these are rejected.
    pub approved_roots: Arc<std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>>,
}

impl DaemonState {
    pub fn new(
        watcher_tx: mpsc::Sender<std::path::PathBuf>,
        dash_tx: broadcast::Sender<crate::dashboard::DashboardEvent>,
        shutdown_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<()>>>>,
    ) -> Self {
        let pool = Arc::new(RepoPool::new());
        // Evict connections idle 60+ minutes, check every 5 minutes.
        spawn_eviction_loop(
            Arc::clone(&pool),
            Duration::from_secs(60 * 60),
            Duration::from_secs(5 * 60),
        );
        Self {
            pool,
            watcher_tx,
            dash_tx,
            shutdown_tx,
            approved_roots: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Test constructor — channels are throwaway (receivers dropped immediately).
    /// All `watcher_tx.send()` calls are handled with `let _ = ...` so dropped
    /// receivers do not panic.
    #[cfg(test)]
    pub fn new_test() -> Self {
        let (watcher_tx, _rx) = mpsc::channel(1);
        let (dash_tx, _) = broadcast::channel(4);
        let (shutdown_tx, _rx2) = oneshot::channel();
        Self {
            pool: Arc::new(RepoPool::new()),
            watcher_tx,
            dash_tx,
            shutdown_tx: Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
            approved_roots: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        }
    }

    /// Register a workspace root as approved for watch registration.
    /// Called when `ingest_repo` runs successfully.
    #[allow(dead_code)]
    pub fn add_approved_root(&self, path: std::path::PathBuf) {
        if let Ok(mut roots) = self.approved_roots.lock() {
            // Canonicalize to handle symlinks consistently.
            let canonical = path.canonicalize().unwrap_or(path);
            roots.insert(canonical);
        }
    }

    /// Check if a path is within an approved workspace root.
    /// Canonicalizes the path to handle symlinks resolving outside workspace.
    /// Falls back to checking the database for indexed repositories if no
    /// in-memory roots are registered.
    pub fn is_path_approved(&self, path: &std::path::Path) -> bool {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => return false, // Path doesn't exist or can't be resolved
        };

        // Check in-memory approved roots first
        if let Ok(roots) = self.approved_roots.lock() {
            for root in roots.iter() {
                if canonical.starts_with(root) {
                    return true;
                }
            }
        }

        false
    }

    /// Check if a path has been indexed (exists in the repositories table).
    /// This provides implicit approval - if a repo was indexed, watching is allowed.
    pub async fn is_path_indexed(&self, path: &std::path::Path) -> bool {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => return false,
        };

        // Get the database connection for this path
        let conn = match self.pool.get_or_open(&canonical).await {
            Ok(c) => c,
            Err(_) => return false,
        };

        // Check if any repository root matches or contains this path
        let conn_guard = match conn.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };

        let result: Result<Vec<String>, _> = conn_guard
            .prepare("SELECT root_path FROM repositories")
            .and_then(|mut stmt| {
                stmt.query_map([], |row| row.get::<_, String>(0))
                    .map(|rows| rows.filter_map(|r| r.ok()).collect())
            });

        match result {
            Ok(roots) => {
                for root in roots {
                    let root_path = std::path::Path::new(&root);
                    let root_canonical = root_path
                        .canonicalize()
                        .unwrap_or_else(|_| root_path.to_path_buf());
                    if canonical.starts_with(&root_canonical) {
                        return true;
                    }
                }
                false
            }
            Err(_) => false,
        }
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn build_router(state: DaemonState) -> Router {
    Router::new()
        .route("/api/health", get(handle_health))
        .route("/api/watch", post(handle_watch))
        .route("/api/shutdown", post(handle_shutdown))
        .with_state(state)
}

/// Build the combined daemon + dashboard router for the TCP dashboard listener.
/// This merges the dashboard routes (UI, SSE, stats, graph, compare, emit)
/// with the daemon's management routes (health, watch, shutdown).
pub fn build_dashboard_router(
    daemon_state: DaemonState,
    dashboard_state: crate::dashboard::AppState,
) -> Router {
    let daemon_routes = Router::new()
        .route("/api/health", get(handle_health))
        .route("/api/watch", post(handle_watch))
        .route("/api/shutdown", post(handle_shutdown))
        .with_state(daemon_state);

    let dashboard_router = crate::dashboard::build_dashboard_router(dashboard_state);

    // Dashboard routes take priority (they include /api/emit, /api/graph, etc.)
    // then daemon management routes are merged underneath.
    dashboard_router.merge(daemon_routes)
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
    State(state): State<DaemonState>,
    Json(req): Json<WatchRequest>,
) -> impl IntoResponse {
    let path = std::path::PathBuf::from(&req.path);
    if !path.exists() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": "path does not exist" })),
        );
    }

    // Security: validate path is within an approved workspace root.
    // Canonicalize first to catch symlinks resolving outside workspace.
    // Check both in-memory approved roots and database-indexed repos.
    let is_approved = state.is_path_approved(&path) || state.is_path_indexed(&path).await;
    if !is_approved {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "path is not within an approved workspace root",
                "hint": "run ingest_repo on the workspace first"
            })),
        );
    }

    // M-10 FIX: Propagate DB/pool errors through HTTP response.
    if let Err(e) = state.pool.get_or_open(&path).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("DB pool error: {e}") })),
        );
    }
    // M-10 FIX: Propagate watcher channel send errors.
    if let Err(e) = state.watcher_tx.send(path).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Watcher channel error: {e}") })),
        );
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "watching": req.path })),
    )
}

/// Signal the daemon to shut down gracefully.
///
/// Fires the oneshot sender stored in `DaemonState::shutdown_tx`. The sender is
/// consumed on first call so subsequent calls are no-ops.
async fn handle_shutdown(State(state): State<DaemonState>) -> impl IntoResponse {
    if let Ok(mut guard) = state.shutdown_tx.lock() {
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "shutting down" })),
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
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// M-9: /rpc/mcp route is removed — callers should get 404.
    #[tokio::test]
    async fn mcp_endpoint_returns_404() {
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
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "/rpc/mcp should no longer exist"
        );
    }
}
