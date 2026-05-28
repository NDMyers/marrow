use std::path::Path;

use marrow::registry::{Registry, WorkspaceStatus};

fn create_workspace(root: &Path) {
    std::fs::create_dir_all(root.join(".marrow")).unwrap();
}

#[test]
fn registry_open_creates_missing_parent_directory_and_migrates_schema() {
    let temp = tempfile::tempdir().unwrap();
    let registry_path = temp.path().join("home").join(".marrow").join("registry.db");

    let registry = Registry::open(&registry_path).unwrap();

    assert!(registry_path.exists());
    let version: i64 = registry
        .connection()
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 1);
    let table_count: i64 = registry
        .connection()
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'workspaces'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(table_count, 1);
}

#[test]
fn registry_registers_canonical_workspace_and_updates_last_seen() {
    let temp = tempfile::tempdir().unwrap();
    let registry_path = temp.path().join("registry.db");
    let workspace = temp.path().join("workspace");
    create_workspace(&workspace);

    let registry = Registry::open(&registry_path).unwrap();
    let first = registry.register_workspace(&workspace, None).unwrap();
    let second = registry
        .register_workspace(workspace.join("."), Some("Demo"))
        .unwrap();

    assert_eq!(first.workspace_id, second.workspace_id);
    assert_eq!(second.display_name.as_deref(), Some("Demo"));
    let expected_graph_db = workspace
        .canonicalize()
        .unwrap()
        .join(".marrow")
        .join("graph.db");
    assert_eq!(second.graph_db_path, expected_graph_db);
    assert!(second.last_seen_at >= first.registered_at);

    let workspaces = registry.list_workspaces().unwrap();
    assert_eq!(workspaces.len(), 1);
    assert_eq!(workspaces[0].status, WorkspaceStatus::MissingDb);

    let mode: String = registry
        .connection()
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    assert_eq!(mode, "wal");
    let synchronous: i64 = registry
        .connection()
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    assert_eq!(synchronous, 1);
}

#[test]
fn registry_rejects_out_of_bounds_without_confirmation() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let trusted = temp.path().join("trusted");
    let outside = temp.path().join("outside");
    create_workspace(&trusted);
    create_workspace(&outside);

    let err = registry
        .register_workspace_with_boundary(&outside, Some(&trusted), false, None)
        .unwrap_err();
    assert!(err
        .to_string()
        .contains("outside the trusted workspace boundary"));

    let entry = registry
        .register_workspace_with_boundary(&outside, Some(&trusted), true, None)
        .unwrap();
    assert!(entry.workspace_root.ends_with("outside"));
}

#[test]
fn registry_reports_db_inventory_statuses() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let available = temp.path().join("available");
    let missing = temp.path().join("missing");
    create_workspace(&available);
    create_workspace(&missing);
    registry.register_workspace(&available, None).unwrap();
    registry.register_workspace(&missing, None).unwrap();

    let graph_db = available.join(".marrow").join("graph.db");
    let conn = marrow::db::init_db(graph_db.to_str().unwrap()).unwrap();
    conn.execute(
        "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params!["available", available.to_string_lossy().to_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
         VALUES ('n1', 'available', 'src/lib.rs', 'rs', 'lib', 'function', 'fn lib() {}')",
        [],
    )
    .unwrap();

    let inventory = registry.db_inventory().unwrap();
    assert_eq!(inventory.len(), 2);
    let available_row = inventory
        .iter()
        .find(|row| row.workspace_root.ends_with("available"))
        .unwrap();
    assert_eq!(available_row.status, WorkspaceStatus::Available);
    assert_eq!(available_row.symbol_count, 1);
    let missing_row = inventory
        .iter()
        .find(|row| row.workspace_root.ends_with("missing"))
        .unwrap();
    assert_eq!(missing_row.status, WorkspaceStatus::MissingDb);
}

#[cfg(target_os = "macos")]
#[test]
fn stats_aggregate_hides_only_stale_macos_tmp_missing_db_rows() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();

    let stale_tmp = tempfile::Builder::new()
        .prefix(".tmp-stale-workspace")
        .tempdir_in(std::env::temp_dir())
        .unwrap();
    let real_missing = temp.path().join("missing");
    let nested_tmp_project = temp.path().join("projects").join(".tmp-real-project");
    let tmp_available = tempfile::Builder::new()
        .prefix(".tmp-available-workspace")
        .tempdir_in(std::env::temp_dir())
        .unwrap();

    std::fs::create_dir_all(&real_missing).unwrap();
    std::fs::create_dir_all(&nested_tmp_project).unwrap();
    registry.register_workspace(stale_tmp.path(), None).unwrap();
    registry.register_workspace(&real_missing, None).unwrap();
    registry
        .register_workspace(&nested_tmp_project, None)
        .unwrap();
    registry
        .register_workspace(tmp_available.path(), None)
        .unwrap();

    let tmp_available_db = tmp_available.path().join(".marrow").join("graph.db");
    let conn = marrow::db::init_db(tmp_available_db.to_str().unwrap()).unwrap();
    conn.execute(
        "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params![
            "tmp-available",
            tmp_available.path().to_string_lossy().to_string()
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
         VALUES ('tmp-available:src/lib.rs:function:demo:0', 'tmp-available', 'src/lib.rs', 'rs', 'demo', 'function', 'fn demo() {}')",
        [],
    )
    .unwrap();
    drop(conn);

    let raw_workspaces = registry.list_workspaces().unwrap();
    assert_eq!(raw_workspaces.len(), 4);
    assert!(raw_workspaces.iter().any(|row| row.workspace_root
        == stale_tmp.path().canonicalize().unwrap()
        && row.status == WorkspaceStatus::MissingDb));

    let raw_inventory = registry.db_inventory().unwrap();
    assert_eq!(raw_inventory.len(), 4);
    assert!(raw_inventory.iter().any(|row| row.workspace_root
        == stale_tmp.path().canonicalize().unwrap()
        && row.status == WorkspaceStatus::MissingDb));

    let aggregate = registry.stats_aggregate().unwrap();
    assert_eq!(aggregate.workspaces.len(), 3);
    assert_eq!(aggregate.lifetime.workspace_statuses.len(), 3);
    assert!(!aggregate
        .workspaces
        .iter()
        .any(|row| row.workspace_root == stale_tmp.path().canonicalize().unwrap()));
    assert!(!aggregate
        .lifetime
        .workspace_statuses
        .iter()
        .any(|row| row.workspace_root == stale_tmp.path().canonicalize().unwrap()));
    assert!(aggregate
        .workspaces
        .iter()
        .any(|row| row.workspace_root.ends_with("missing")
            && row.status == WorkspaceStatus::MissingDb));
    assert!(aggregate
        .workspaces
        .iter()
        .any(|row| row.workspace_root.ends_with(".tmp-real-project")
            && row.status == WorkspaceStatus::MissingDb));
    assert!(aggregate.workspaces.iter().any(|row| row.workspace_root
        == tmp_available.path().canonicalize().unwrap()
        && row.status == WorkspaceStatus::Available));
}

#[test]
fn registry_reports_empty_and_corrupt_workspace_db_statuses() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let empty = temp.path().join("empty workspace");
    let corrupt = temp.path().join("corrupt");
    create_workspace(&empty);
    create_workspace(&corrupt);

    let empty_db = empty.join(".marrow").join("graph.db");
    let empty_conn = marrow::db::init_db(empty_db.to_str().unwrap()).unwrap();
    drop(empty_conn);
    std::fs::write(corrupt.join(".marrow").join("graph.db"), b"not sqlite").unwrap();
    registry.register_workspace(&empty, None).unwrap();
    registry.register_workspace(&corrupt, None).unwrap();

    let inventory = registry.db_inventory().unwrap();
    let empty_row = inventory
        .iter()
        .find(|row| row.workspace_root.ends_with("empty workspace"))
        .unwrap();
    assert_eq!(empty_row.status, WorkspaceStatus::Empty);
    let corrupt_row = inventory
        .iter()
        .find(|row| row.workspace_root.ends_with("corrupt"))
        .unwrap();
    assert_eq!(corrupt_row.status, WorkspaceStatus::Corrupt);
    assert!(corrupt_row.error.is_some());
}

#[cfg(unix)]
#[test]
fn registry_deduplicates_symlinked_workspace_paths() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    let symlink = temp.path().join("workspace-link");
    create_workspace(&workspace);
    std::os::unix::fs::symlink(&workspace, &symlink).unwrap();

    let direct = registry.register_workspace(&workspace, None).unwrap();
    let linked = registry.register_workspace(&symlink, None).unwrap();

    assert_eq!(direct.workspace_id, linked.workspace_id);
    assert_eq!(registry.list_workspaces().unwrap().len(), 1);
}
