//! Axum route handlers for the daemon HTTP server.

use crate::daemon::pool::{spawn_eviction_loop, RepoPool};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot};

// ── State ─────────────────────────────────────────────────────────────────────

/// Shared state threaded through all Axum route handlers.
#[derive(Clone)]
pub struct DaemonState {
    pub pool: Arc<RepoPool>,
    pub registry: Option<Arc<std::sync::Mutex<crate::registry::Registry>>>,
    pub activity: crate::activity::ActivityTracker,
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
        let registry = crate::registry::Registry::open_default()
            .ok()
            .map(|registry| Arc::new(std::sync::Mutex::new(registry)));
        Self {
            pool,
            registry,
            activity: crate::activity::ActivityTracker::default(),
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
            registry: crate::registry::Registry::open(":memory:")
                .ok()
                .map(|registry| Arc::new(std::sync::Mutex::new(registry))),
            activity: crate::activity::ActivityTracker::default(),
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

    /// Check if a path was already registered through a trusted path such as ingest_repo.
    pub async fn is_path_indexed(&self, path: &std::path::Path) -> bool {
        let canonical = match path.canonicalize() {
            Ok(p) => p,
            Err(_) => return false,
        };
        let Some(registry) = &self.registry else {
            return false;
        };
        let Ok(registry) = registry.lock() else {
            return false;
        };
        let Ok(workspaces) = registry.list_workspaces() else {
            return false;
        };
        for workspace in workspaces {
            let Ok(root) = workspace.workspace_root.canonicalize() else {
                continue;
            };
            if canonical.starts_with(root) {
                return true;
            }
        }
        false
    }
}

// ── Router ────────────────────────────────────────────────────────────────────

pub fn build_router(state: DaemonState) -> Router {
    Router::new()
        .route("/api/health", get(handle_health))
        .route("/api/workspaces", get(handle_workspaces))
        .route("/api/dbs", get(handle_dbs))
        .route("/api/global-stats", get(handle_global_stats))
        .route("/api/activity", get(handle_activity))
        .route("/api/activity/start", post(handle_activity_start))
        .route("/api/activity/finish", post(handle_activity_finish))
        .route("/api/workspace-graph", get(handle_workspace_graph))
        .route("/api/query-failures", get(handle_query_failures))
        .route("/api/cleanup/unregister", post(handle_cleanup_unregister))
        .route("/api/cleanup/clear-index", post(handle_cleanup_clear_index))
        .route("/api/cleanup/delete-db", post(handle_cleanup_delete_db))
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
        .route("/api/workspaces", get(handle_workspaces))
        .route("/api/dbs", get(handle_dbs))
        .route("/api/global-stats", get(handle_global_stats))
        .route("/api/activity", get(handle_activity))
        .route("/api/activity/start", post(handle_activity_start))
        .route("/api/activity/finish", post(handle_activity_finish))
        .route("/api/workspace-graph", get(handle_workspace_graph))
        .route("/api/query-failures", get(handle_query_failures))
        .route("/api/cleanup/unregister", post(handle_cleanup_unregister))
        .route("/api/cleanup/clear-index", post(handle_cleanup_clear_index))
        .route("/api/cleanup/delete-db", post(handle_cleanup_delete_db))
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

fn registry_unavailable() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "registry unavailable" })),
    )
}

async fn handle_workspaces(State(state): State<DaemonState>) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let result = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| registry.list_workspaces().map_err(|e| e.to_string()));
    match result {
        Ok(workspaces) => (StatusCode::OK, Json(json!({ "workspaces": workspaces }))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error })),
        ),
    }
}

async fn handle_dbs(State(state): State<DaemonState>) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let result = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| registry.db_inventory().map_err(|e| e.to_string()));
    match result {
        Ok(dbs) => (StatusCode::OK, Json(json!({ "dbs": dbs }))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error })),
        ),
    }
}

async fn handle_global_stats(State(state): State<DaemonState>) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let result = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| registry.global_lifetime_stats().map_err(|e| e.to_string()));
    match result {
        Ok(stats) => (StatusCode::OK, Json(json!({ "lifetime": stats }))),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error })),
        ),
    }
}

async fn handle_activity(State(state): State<DaemonState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!({ "activity": state.activity.list() })),
    )
}

#[derive(Deserialize)]
struct ActivityStartRequest {
    kind: crate::activity::ActivityKind,
    workspace_id: Option<String>,
    detail: String,
}

async fn handle_activity_start(
    State(state): State<DaemonState>,
    Json(req): Json<ActivityStartRequest>,
) -> impl IntoResponse {
    let id = state.activity.start(req.kind, req.workspace_id, req.detail);
    (StatusCode::OK, Json(json!({ "id": id })))
}

#[derive(Deserialize)]
struct ActivityFinishRequest {
    id: String,
    state: crate::activity::ActivityState,
    detail: String,
}

async fn handle_activity_finish(
    State(state): State<DaemonState>,
    Json(req): Json<ActivityFinishRequest>,
) -> impl IntoResponse {
    state.activity.finish(&req.id, req.state, req.detail);
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

#[derive(Deserialize)]
struct WorkspaceGraphQuery {
    workspace_id: String,
    repo_id: Option<String>,
}

async fn handle_workspace_graph(
    State(state): State<DaemonState>,
    Query(params): Query<WorkspaceGraphQuery>,
) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let result = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| {
            registry
                .graph_snapshot(
                    &params.workspace_id,
                    params.repo_id.as_deref(),
                    crate::registry::default_graph_limit(),
                )
                .map_err(|e| e.to_string())
        });
    match result {
        Ok(graph) => (StatusCode::OK, Json(json!(graph))),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))),
    }
}

#[derive(Deserialize)]
struct QueryFailuresQuery {
    workspace_id: String,
}

/// Per-workspace query-failure telemetry: lifetime counters by category plus
/// the recent-failure ring buffer (agents route around hard failures
/// silently, so this endpoint is how a human finds out they happened).
async fn handle_query_failures(
    State(state): State<DaemonState>,
    Query(params): Query<QueryFailuresQuery>,
) -> impl IntoResponse {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let entry = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| {
            registry
                .find_workspace(&params.workspace_id)
                .map_err(|e| e.to_string())
        });
    let result = entry.and_then(|entry| {
        let Some(entry) = entry else {
            return Err(format!("unknown workspace '{}'", params.workspace_id));
        };
        let conn = rusqlite::Connection::open_with_flags(
            &entry.graph_db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(|e| e.to_string())?;
        let recent = crate::db::recent_query_failures(&conn, 20).map_err(|e| e.to_string())?;
        Ok(json!({
            "workspace_id": entry.workspace_id,
            "tool_calls_total": crate::db::read_stat(&conn, "tool_calls_total"),
            "query_failures_total": crate::db::read_stat(&conn, "query_failures_total"),
            "categories": {
                "symbol_not_found": crate::db::read_stat(&conn, "query_failures_symbol_not_found"),
                "repo_not_found": crate::db::read_stat(&conn, "query_failures_repo_not_found"),
                "invalid_params": crate::db::read_stat(&conn, "query_failures_invalid_params"),
                "internal": crate::db::read_stat(&conn, "query_failures_internal"),
                "other": crate::db::read_stat(&conn, "query_failures_other"),
            },
            "recent": recent,
        }))
    });
    match result {
        Ok(payload) => (StatusCode::OK, Json(payload)),
        Err(error) => (StatusCode::BAD_REQUEST, Json(json!({ "error": error }))),
    }
}

#[derive(Deserialize)]
struct CleanupRequest {
    workspace_id: String,
    confirmed: bool,
}

async fn cleanup_with_kind(
    state: DaemonState,
    req: CleanupRequest,
    kind: crate::registry::CleanupKind,
) -> (StatusCode, Json<serde_json::Value>) {
    let Some(registry) = &state.registry else {
        return registry_unavailable();
    };
    let activity_id = state.activity.start(
        crate::activity::ActivityKind::CleanupJob,
        Some(req.workspace_id.clone()),
        format!("{:?}", kind),
    );
    let result = registry
        .lock()
        .map_err(|_| "registry mutex poisoned".to_string())
        .and_then(|registry| {
            registry
                .cleanup_workspace(&req.workspace_id, kind, req.confirmed)
                .map_err(|e| e.to_string())
        });
    match result {
        Ok(()) => {
            state.activity.finish(
                &activity_id,
                crate::activity::ActivityState::Completed,
                "cleanup complete".to_string(),
            );
            (StatusCode::OK, Json(json!({ "status": "ok" })))
        }
        Err(error) => {
            state.activity.finish(
                &activity_id,
                crate::activity::ActivityState::Error,
                error.clone(),
            );
            (StatusCode::BAD_REQUEST, Json(json!({ "error": error })))
        }
    }
}

async fn handle_cleanup_unregister(
    State(state): State<DaemonState>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    cleanup_with_kind(state, req, crate::registry::CleanupKind::Unregister).await
}

async fn handle_cleanup_clear_index(
    State(state): State<DaemonState>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    cleanup_with_kind(state, req, crate::registry::CleanupKind::ClearIndex).await
}

async fn handle_cleanup_delete_db(
    State(state): State<DaemonState>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    cleanup_with_kind(state, req, crate::registry::CleanupKind::DeleteDb).await
}

#[derive(Deserialize)]
struct WatchRequest {
    path: String,
    workspace_id: Option<String>,
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

    let canonical_path = match path.canonicalize() {
        Ok(path) => path,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "error": format!("path cannot be resolved: {e}") })),
            )
        }
    };

    // Security: validate path is within an approved workspace root before any
    // registry, approved-root, pool, or watcher mutation can occur.
    let is_approved =
        state.is_path_approved(&canonical_path) || state.is_path_indexed(&canonical_path).await;
    if !is_approved {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({
                "error": "path is not within an approved workspace root",
                "hint": "run ingest_repo on the workspace first"
            })),
        );
    }

    let mut registered_workspace_id = req.workspace_id.clone();
    if let Some(registry) = &state.registry {
        if let Ok(registry) = registry.lock() {
            if let Ok(entry) = registry.register_workspace(&canonical_path, None) {
                registered_workspace_id = Some(entry.workspace_id);
                state.add_approved_root(entry.workspace_root);
            }
        }
    }

    // M-10 FIX: Propagate DB/pool errors through HTTP response.
    if let Err(e) = state.pool.get_or_open(&canonical_path).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("DB pool error: {e}") })),
        );
    }
    // M-10 FIX: Propagate watcher channel send errors.
    if let Err(e) = state.watcher_tx.send(canonical_path).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": format!("Watcher channel error: {e}") })),
        );
    }
    state.activity.start(
        crate::activity::ActivityKind::WatcherJob,
        registered_workspace_id,
        format!("watching {}", req.path),
    );
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

    #[tokio::test]
    async fn watch_rejects_unapproved_path_without_registry_mutation() {
        let tmpdir = tempfile::tempdir().unwrap();
        let outside = tmpdir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();

        let state = DaemonState::new_test();
        let registry = state.registry.clone().unwrap();
        let approved_roots = state.approved_roots.clone();
        let app = build_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/watch")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({ "path": outside.to_string_lossy() }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(registry
            .lock()
            .unwrap()
            .list_workspaces()
            .unwrap()
            .is_empty());
        assert!(approved_roots.lock().unwrap().is_empty());
        assert!(!outside.join(".marrow").exists());
    }
}
