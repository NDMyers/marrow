use marrow::registry::{CleanupKind, Registry, WorkspaceStatus};

#[test]
fn cleanup_actions_are_confirmed_and_separate() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(workspace.join(".marrow")).unwrap();
    let entry = registry.register_workspace(&workspace, None).unwrap();
    let graph_db = workspace.join(".marrow").join("graph.db");
    let conn = marrow::db::init_db(graph_db.to_str().unwrap()).unwrap();
    conn.execute(
        "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params!["workspace", workspace.to_string_lossy().to_string()],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
         VALUES ('node', 'workspace', 'src/lib.rs', 'rs', 'sample', 'function', 'fn sample() {}')",
        [],
    )
    .unwrap();
    drop(conn);

    assert!(registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::ClearIndex, false)
        .is_err());
    registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::ClearIndex, true)
        .unwrap();
    assert!(
        graph_db.exists(),
        "clear index must leave graph.db in place"
    );
    assert_eq!(
        registry.list_workspaces().unwrap().len(),
        1,
        "clear index must leave registry row"
    );
    let conn = rusqlite::Connection::open(&graph_db).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    drop(conn);

    registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::DeleteDb, true)
        .unwrap();
    assert!(!graph_db.exists(), "delete db must remove only graph.db");
    assert_eq!(
        registry.list_workspaces().unwrap()[0].status,
        WorkspaceStatus::MissingDb
    );

    registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::Unregister, true)
        .unwrap();
    assert!(registry.list_workspaces().unwrap().is_empty());
}

#[test]
fn clear_index_fails_on_corrupt_graph_db_without_replacing_it() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    let marrow_dir = workspace.join(".marrow");
    std::fs::create_dir_all(&marrow_dir).unwrap();
    let graph_db = marrow_dir.join("graph.db");
    let corrupt_contents = b"not a sqlite database";
    std::fs::write(&graph_db, corrupt_contents).unwrap();
    let entry = registry.register_workspace(&workspace, None).unwrap();

    let err = registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::ClearIndex, true)
        .unwrap_err();

    assert!(err.to_string().contains("opening registered graph DB"));
    assert_eq!(std::fs::read(&graph_db).unwrap(), corrupt_contents);
}

#[cfg(unix)]
#[test]
fn cleanup_rejects_unregistered_and_symlink_escape_targets() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(workspace.join(".marrow")).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let outside_db = outside.join("graph.db");
    std::fs::write(&outside_db, b"not a registered db").unwrap();
    let link = workspace.join(".marrow").join("graph.db");
    std::os::unix::fs::symlink(&outside_db, &link).unwrap();
    let entry = registry.register_workspace(&workspace, None).unwrap();

    let err = registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::DeleteDb, true)
        .unwrap_err();
    assert!(err.to_string().contains("registered Marrow graph DB path"));

    let err = registry
        .cleanup_workspace("not-registered", CleanupKind::ClearIndex, true)
        .unwrap_err();
    assert!(err.to_string().contains("not registered"));
}

#[cfg(unix)]
#[test]
fn cleanup_rejects_symlinked_marrow_directory_escape() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    let outside = temp.path().join("outside");
    std::fs::create_dir_all(&workspace).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    std::os::unix::fs::symlink(&outside, workspace.join(".marrow")).unwrap();
    let graph_db = workspace.join(".marrow").join("graph.db");
    let conn = marrow::db::init_db(graph_db.to_str().unwrap()).unwrap();
    drop(conn);
    let entry = registry.register_workspace(&workspace, None).unwrap();

    let err = registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::DeleteDb, true)
        .unwrap_err();
    assert!(err.to_string().contains("escapes workspace root"));
    assert!(graph_db.exists());
}

#[test]
fn cleanup_rejects_registered_root_or_non_graph_db_targets() {
    let temp = tempfile::tempdir().unwrap();
    let registry = Registry::open(temp.path().join("registry.db")).unwrap();
    let workspace = temp.path().join("workspace");
    let marrow_dir = workspace.join(".marrow");
    std::fs::create_dir_all(&marrow_dir).unwrap();
    let entry = registry.register_workspace(&workspace, None).unwrap();

    registry
        .connection()
        .execute(
            "UPDATE workspaces SET graph_db_path = ?1 WHERE workspace_id = ?2",
            rusqlite::params![workspace.to_string_lossy().to_string(), entry.workspace_id],
        )
        .unwrap();
    let err = registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::DeleteDb, true)
        .unwrap_err();
    assert!(err.to_string().contains("registered Marrow graph DB path"));

    let other_db = marrow_dir.join("other.db");
    std::fs::write(&other_db, b"not graph").unwrap();
    registry
        .connection()
        .execute(
            "UPDATE workspaces SET graph_db_path = ?1 WHERE workspace_id = ?2",
            rusqlite::params![other_db.to_string_lossy().to_string(), entry.workspace_id],
        )
        .unwrap();
    let err = registry
        .cleanup_workspace(&entry.workspace_id, CleanupKind::DeleteDb, true)
        .unwrap_err();
    assert!(err.to_string().contains("registered Marrow graph DB path"));
    assert!(other_db.exists());
}
