use axum::{
    body::Body,
    http::{Method, Request, StatusCode},
    Router,
};
use http_body_util::BodyExt as _;
use marrow::{
    activity::{ActivityKind, ActivityTracker},
    daemon::{
        pool::RepoPool,
        routes::{build_router, DaemonState},
    },
    registry::{Registry, WorkspaceStatus},
};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc, oneshot};
use tower::ServiceExt as _;

async fn json_request(
    app: Router,
    method: Method,
    uri: &str,
    body: Option<Value>,
) -> (StatusCode, Value) {
    let request_body = body.map_or_else(Body::empty, |value| Body::from(value.to_string()));
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "application/json")
                .body(request_body)
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn daemon_state_with_registry(registry: Registry, activity: ActivityTracker) -> DaemonState {
    let (watcher_tx, _watcher_rx) = mpsc::channel(1);
    let (dash_tx, _) = broadcast::channel(4);
    let (shutdown_tx, _shutdown_rx) = oneshot::channel();
    DaemonState {
        pool: Arc::new(RepoPool::new()),
        registry: Some(Arc::new(Mutex::new(registry))),
        activity,
        watcher_tx,
        dash_tx,
        shutdown_tx: Arc::new(Mutex::new(Some(shutdown_tx))),
        approved_roots: Arc::new(Mutex::new(Default::default())),
    }
}

#[test]
fn global_lifetime_stats_are_partial_across_registered_workspaces() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace_a = temp.path().join("workspace-a");
    let workspace_b = temp.path().join("workspace-b");
    let workspace_c = temp.path().join("workspace-c");
    std::fs::create_dir_all(workspace_a.join(".marrow")).unwrap();
    std::fs::create_dir_all(workspace_b.join(".marrow")).unwrap();
    std::fs::create_dir_all(workspace_c.join(".marrow")).unwrap();
    registry.register_workspace(&workspace_a, None).unwrap();
    registry.register_workspace(&workspace_b, None).unwrap();
    std::fs::write(workspace_c.join(".marrow").join("graph.db"), b"not sqlite").unwrap();
    registry.register_workspace(&workspace_c, None).unwrap();

    let db_a = workspace_a.join(".marrow").join("graph.db");
    let conn_a = marrow::db::init_db(db_a.to_str().unwrap()).unwrap();
    marrow::db::increment_stat(&conn_a, "total_requests", 4).unwrap();
    marrow::db::increment_stat(&conn_a, "total_tokens_saved", 80).unwrap();
    marrow::db::increment_stat(&conn_a, "total_file_tokens", 100).unwrap();
    conn_a
        .execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params!["workspace-a", workspace_a.to_string_lossy().to_string()],
        )
        .unwrap();
    conn_a.execute(
        "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
         VALUES ('workspace-a:src/a.rs:function:a:0', 'workspace-a', 'src/a.rs', 'rs', 'a', 'function', 'fn a() {}')",
        [],
    ).unwrap();

    let stats = registry.global_lifetime_stats().unwrap();
    assert_eq!(stats.total_requests, 4);
    assert_eq!(stats.total_tokens_saved, 80);
    assert_eq!(stats.total_file_tokens, 100);
    assert_eq!(stats.workspace_statuses.len(), 3);
    assert!(stats
        .workspace_statuses
        .iter()
        .any(|row| row.status == WorkspaceStatus::Available));
    assert!(stats
        .workspace_statuses
        .iter()
        .any(|row| row.status == WorkspaceStatus::MissingDb));
    assert!(stats
        .workspace_statuses
        .iter()
        .any(|row| row.status == WorkspaceStatus::Corrupt));
}

#[test]
fn selected_workspace_graph_reads_only_that_workspace_db() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace_a = temp.path().join("workspace-a");
    let workspace_b = temp.path().join("workspace-b");
    std::fs::create_dir_all(workspace_a.join(".marrow")).unwrap();
    std::fs::create_dir_all(workspace_b.join(".marrow")).unwrap();
    let entry_a = registry.register_workspace(&workspace_a, None).unwrap();
    let entry_b = registry.register_workspace(&workspace_b, None).unwrap();

    for (workspace, repo_id, symbol) in [
        (&workspace_a, "repo-a", "alpha"),
        (&workspace_b, "repo-b", "beta"),
    ] {
        let db_path = workspace.join(".marrow").join("graph.db");
        let conn = marrow::db::init_db(db_path.to_str().unwrap()).unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, workspace.to_string_lossy().to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, 'src/lib.rs', 'rs', ?3, 'function', 'fn sample() {}')",
            rusqlite::params![format!("{repo_id}:src/lib.rs:function:{symbol}:0"), repo_id, symbol],
        ).unwrap();
    }

    let graph_a = registry
        .graph_snapshot(&entry_a.workspace_id, None, 500)
        .unwrap();
    assert_eq!(graph_a.nodes.len(), 1);
    assert_eq!(graph_a.nodes[0].label, "alpha");

    let graph_b = registry
        .graph_snapshot(&entry_b.workspace_id, None, 500)
        .unwrap();
    assert_eq!(graph_b.nodes.len(), 1);
    assert_eq!(graph_b.nodes[0].label, "beta");
}

#[test]
fn workspace_graph_missing_db_does_not_create_graph_db() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(workspace.join(".marrow")).unwrap();
    let entry = registry.register_workspace(&workspace, None).unwrap();

    let graph = registry
        .graph_snapshot(&entry.workspace_id, None, 500)
        .unwrap();

    assert_eq!(graph.status, WorkspaceStatus::MissingDb);
    assert!(graph.nodes.is_empty());
    assert!(!entry.graph_db_path.exists());
}

#[tokio::test]
async fn daemon_global_routes_expose_inventory_activity_graph_and_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(workspace.join(".marrow")).unwrap();
    let entry = registry
        .register_workspace(&workspace, Some("Demo"))
        .unwrap();
    let graph_db = workspace.join(".marrow").join("graph.db");
    let conn = marrow::db::init_db(graph_db.to_str().unwrap()).unwrap();
    conn.execute(
        "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params!["repo", workspace.to_string_lossy().to_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
         VALUES ('repo:src/lib.rs:function:demo:0', 'repo', 'src/lib.rs', 'rs', 'demo', 'function', 'fn demo() {}')",
        [],
    )
    .unwrap();
    drop(conn);

    let activity = ActivityTracker::default();
    let activity_id = activity.start(
        ActivityKind::McpSession,
        Some(entry.workspace_id.clone()),
        "copilot".to_string(),
    );
    let app = build_router(daemon_state_with_registry(registry, activity));

    let (status, workspaces) =
        json_request(app.clone(), Method::GET, "/api/workspaces", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(workspaces["workspaces"].as_array().unwrap().len(), 1);

    let (status, dbs) = json_request(app.clone(), Method::GET, "/api/dbs", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(dbs["dbs"][0]["status"], "available");

    let (status, stats) = json_request(app.clone(), Method::GET, "/api/global-stats", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        stats["lifetime"]["workspace_statuses"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let graph_uri = format!("/api/workspace-graph?workspace_id={}", entry.workspace_id);
    let (status, graph) = json_request(app.clone(), Method::GET, &graph_uri, None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(graph["nodes"][0]["label"], "demo");

    let (status, activity_body) =
        json_request(app.clone(), Method::GET, "/api/activity", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(activity_body["activity"][0]["id"], activity_id);

    let (status, cleanup_error) = json_request(
        app.clone(),
        Method::POST,
        "/api/cleanup/clear-index",
        Some(serde_json::json!({ "workspace_id": entry.workspace_id, "confirmed": false })),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(cleanup_error["error"]
        .as_str()
        .unwrap()
        .contains("explicit confirmation"));

    let (status, cleanup_ok) = json_request(
        app,
        Method::POST,
        "/api/cleanup/clear-index",
        Some(serde_json::json!({ "workspace_id": entry.workspace_id, "confirmed": true })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(cleanup_ok["status"], "ok");
    let conn = rusqlite::Connection::open(graph_db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}
