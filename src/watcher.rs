use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
use tokio::sync::broadcast;

use crate::dashboard::{self, DashboardEvent};
use crate::ingestion;

fn indexed_repos(
    db: &Arc<Mutex<rusqlite::Connection>>,
) -> Result<Vec<(String, PathBuf)>> {
    let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
    let mut stmt = conn.prepare("SELECT id, root_path FROM repositories")?;
    let repos = stmt
        .query_map([], |row| {
            let id: String = row.get(0)?;
            let path: String = row.get(1)?;
            Ok((id, PathBuf::from(path)))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(repos)
}

/// Spawn a background file watcher that monitors all indexed repositories and
/// incrementally re-indexes changed files.  Events are broadcast directly via
/// the provided `tx` channel (no HTTP round-trip).
///
/// Returns a `JoinHandle` for the tokio task. The current workspace root is
/// always watched so the watcher can pick up repos indexed later in the same
/// server session.
pub fn spawn_watcher(
    db: Arc<Mutex<rusqlite::Connection>>,
    tx: broadcast::Sender<DashboardEvent>,
    debounce_ms: u64,
) -> Result<tokio::task::JoinHandle<()>> {
    let repos = indexed_repos(&db)?;

    let (fs_tx, mut fs_rx) = tokio::sync::mpsc::channel::<Vec<PathBuf>>(64);
    let (watch_tx, watch_rx) = std::sync::mpsc::channel::<PathBuf>();

    // Create the debounced filesystem watcher on a blocking thread
    let mut watch_paths: Vec<PathBuf> = repos.iter().map(|(_id, p)| p.clone()).collect();
    let workspace_root = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));
    if !watch_paths.iter().any(|path| path == &workspace_root) {
        watch_paths.push(workspace_root);
    }
    let debounce_dur = Duration::from_millis(debounce_ms);
    let watched_roots = Arc::new(Mutex::new(HashSet::new()));
    let watched_roots_thread = Arc::clone(&watched_roots);

    std::thread::spawn(move || {
        let rt_tx = fs_tx;
        let new_watch_rx = watch_rx;
        let mut debouncer = match new_debouncer(debounce_dur, move |res: Result<Vec<notify_debouncer_mini::DebouncedEvent>, _>| {
            if let Ok(events) = res {
                let paths: Vec<PathBuf> = events
                    .into_iter()
                    .filter(|e| e.kind == DebouncedEventKind::Any)
                    .map(|e| e.path)
                    .collect();
                if !paths.is_empty() {
                    let _ = rt_tx.blocking_send(paths);
                }
            }
        }) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Marrow watcher init error: {e}");
                return;
            }
        };

        for path in &watch_paths {
            if let Err(e) = debouncer.watcher().watch(path, notify::RecursiveMode::Recursive) {
                eprintln!("Marrow watcher: failed to watch {}: {e}", path.display());
            } else if let Ok(mut watched) = watched_roots_thread.lock() {
                watched.insert(path.clone());
            }
        }

        // Keep the watcher alive — it drops when this thread ends (which is never
        // under normal operation since the MCP server runs indefinitely).
        loop {
            while let Ok(path) = new_watch_rx.try_recv() {
                let already_watched = watched_roots_thread
                    .lock()
                    .map(|watched| watched.contains(&path))
                    .unwrap_or(false);
                if already_watched {
                    continue;
                }
                if let Err(e) = debouncer.watcher().watch(&path, notify::RecursiveMode::Recursive) {
                    eprintln!("Marrow watcher: failed to watch {}: {e}", path.display());
                } else if let Ok(mut watched) = watched_roots_thread.lock() {
                    watched.insert(path);
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    let watch_tx_task = watch_tx.clone();
    let watched_roots_task = Arc::clone(&watched_roots);
    let handle = tokio::spawn(async move {
        while let Some(paths) = fs_rx.recv().await {
            for path in paths {
                let repos = match indexed_repos(&db) {
                    Ok(repos) => repos,
                    Err(e) => {
                        eprintln!("Marrow watcher: failed to reload repos: {e}");
                        continue;
                    }
                };
                for (_, root) in &repos {
                    let already_watched = watched_roots_task
                        .lock()
                        .map(|watched| watched.contains(root))
                        .unwrap_or(false);
                    if !already_watched {
                        let _ = watch_tx_task.send(root.clone());
                    }
                }
                if let Err(e) = handle_file_change(&path, &db, &repos, &tx).await {
                    eprintln!("Marrow watcher: error handling {}: {e}", path.display());
                }
            }
        }
    });

    Ok(handle)
}

/// Process a single file change event: re-parse the file, update nodes and
/// CALLS edges, mark stale observations, and broadcast the event.
async fn handle_file_change(
    path: &Path,
    db: &Arc<Mutex<rusqlite::Connection>>,
    repos: &[(String, PathBuf)],
    tx: &broadcast::Sender<DashboardEvent>,
) -> Result<()> {
    let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    // Check if this file is parseable
    if !ingestion::is_safe_to_parse(&path) {
        return Ok(());
    }
    let ext = match path.extension().and_then(|e| e.to_str()) {
        Some(e) => e.to_string(),
        None => return Ok(()),
    };
    if ingestion::language_for_ext(&ext).is_none() {
        return Ok(());
    }

    // Find owning repo
    let (repo_id, root_path) = match repos.iter().find(|(_, root)| path.starts_with(root)) {
        Some(r) => (r.0.clone(), r.1.clone()),
        None => return Ok(()),
    };

    let rel_path = match path.strip_prefix(&root_path) {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => return Ok(()),
    };

    let file_exists = path.exists();
    let file_path_for_task = path.clone();
    let rel_path_clone = rel_path.clone();
    let repo_id_clone = repo_id.clone();
    let db_clone = Arc::clone(db);

    let symbols = tokio::task::spawn_blocking(move || -> Result<usize> {
        let conn = db_clone
            .lock()
            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;

        let tx_db = conn.unchecked_transaction()?;

        let existing_symbols: Vec<String> = tx_db
            .prepare(
                "SELECT symbol_name FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            )?
            .query_map(rusqlite::params![repo_id_clone, rel_path_clone], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        let parsed_symbols = if file_exists {
            match ingestion::parse_file(&file_path_for_task) {
                Ok((_lang, symbols)) => Some(symbols),
                Err(_) => return Ok(0),
            }
        } else {
            None
        };

        // Delete old nodes for this file
        tx_db.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id_clone, rel_path_clone],
        )?;

        // Delete edges touching this file's nodes, including cross-repo imports.
        let prefix = format!("{}:{}:", repo_id_clone, rel_path_clone);
        tx_db.execute(
            "DELETE FROM edges WHERE source_id LIKE ?1 OR target_id LIKE ?1",
            rusqlite::params![format!("{prefix}%")],
        )?;

        if let Some(symbols) = parsed_symbols {
            // Insert new nodes
            let mut count = 0;
            for sym in &symbols {
                let node_id = format!("{}:{}:{}", repo_id_clone, rel_path_clone, sym.name);
                tx_db.execute(
                    "INSERT OR REPLACE INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        node_id,
                        repo_id_clone,
                        rel_path_clone,
                        file_path_for_task.extension().and_then(|e| e.to_str()).unwrap_or(""),
                        sym.name,
                        sym.symbol_type,
                        sym.raw_text
                    ],
                )?;

                let new_hash = crate::db::hash_raw_text(&sym.raw_text);
                crate::db::mark_stale_observations(
                    &tx_db,
                    &repo_id_clone,
                    &sym.name,
                    &rel_path_clone,
                    &new_hash,
                )?;
                count += 1;
            }

            let lang_ext = file_path_for_task
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("");

            // Build CALLS edges: only load target ids for callee names used in this file.
            let mut callee_names = HashSet::new();
            for sym in &symbols {
                let callees = ingestion::extract_calls_from_symbol(&sym.raw_text, lang_ext);
                for c in callees {
                    if c != sym.name {
                        callee_names.insert(c);
                    }
                }
            }
            let name_to_ids = ingestion::build_name_to_ids_for_symbol_names(
                &tx_db,
                &repo_id_clone,
                &callee_names,
            )?;

            for sym in &symbols {
                let callees = ingestion::extract_calls_from_symbol(&sym.raw_text, lang_ext);
                let source_id = format!("{}:{}:{}", repo_id_clone, rel_path_clone, sym.name);
                for callee_name in &callees {
                    if callee_name == &sym.name {
                        continue;
                    }
                    if let Some(target_ids) = name_to_ids.get(callee_name.as_str()) {
                        for target_id in target_ids {
                            tx_db.execute(
                                "INSERT OR IGNORE INTO edges (source_id, target_id, relationship_type) \
                                 VALUES (?1, ?2, 'CALLS')",
                                rusqlite::params![source_id, target_id],
                            )?;
                        }
                    }
                }
            }

            tx_db.commit()?;
            crate::ingestion::resolve_cross_repo_edges(&conn)?;
            return Ok(count);
        }

        if !file_exists {
            for symbol_name in &existing_symbols {
                crate::db::mark_deleted_observation_stale(
                    &tx_db,
                    &repo_id_clone,
                    symbol_name,
                    &rel_path_clone,
                )?;
            }
            tx_db.commit()?;
            crate::ingestion::resolve_cross_repo_edges(&conn)?;
            return Ok(0);
        }
        Ok(0)
    })
    .await
    .map_err(|e| anyhow::anyhow!("spawn_blocking join error: {e}"))??;

    let _ = tx.send(DashboardEvent::FileReindexed {
        file_path: rel_path,
        repo_id,
        symbols,
        ts: dashboard::now_ts(),
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_handle_file_change_updates_nodes() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let db = Arc::new(Mutex::new(conn));

        let dir = std::env::temp_dir().join("marrow_watcher_test_update");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();

        // Set up repo
        {
            let c = db.lock().unwrap();
            c.execute(
                "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
                rusqlite::params!["test", dir.to_string_lossy().as_ref()],
            )
            .unwrap();
        }

        let repos = vec![("test".to_string(), dir.clone())];
        let (tx, _rx) = broadcast::channel::<DashboardEvent>(16);

        // Write a file and handle the change
        let file = dir.join("hello.py");
        std::fs::write(&file, "def greet():\n    pass\n").unwrap();

        handle_file_change(&file, &db, &repos, &tx).await.unwrap();

        // Verify node exists
        let count: i64 = {
            let c = db.lock().unwrap();
            c.query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert!(count >= 1, "expected at least 1 node, got {count}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_handle_file_change_deleted_file() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let db = Arc::new(Mutex::new(conn));

        let dir = std::env::temp_dir().join("marrow_watcher_test_delete");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();

        {
            let c = db.lock().unwrap();
            c.execute(
                "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
                rusqlite::params!["test", dir.to_string_lossy().as_ref()],
            )
            .unwrap();
        }

        let repos = vec![("test".to_string(), dir.clone())];
        let (tx, _rx) = broadcast::channel::<DashboardEvent>(16);

        // Write, index, then delete
        let file = dir.join("gone.py");
        std::fs::write(&file, "def old():\n    pass\n").unwrap();
        handle_file_change(&file, &db, &repos, &tx).await.unwrap();

        let before: i64 = {
            let c = db.lock().unwrap();
            c.query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert!(before >= 1);

        // Delete the file and handle change
        std::fs::remove_file(&file).unwrap();
        handle_file_change(&file, &db, &repos, &tx).await.unwrap();

        let after: i64 = {
            let c = db.lock().unwrap();
            c.query_row(
                "SELECT COUNT(*) FROM nodes WHERE file_path = 'gone.py' AND repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(after, 0, "nodes should be cleaned up after file deletion");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_handle_file_change_marks_deleted_observations_stale() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let db = Arc::new(Mutex::new(conn));

        let dir = std::env::temp_dir().join("marrow_watcher_test_stale");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();

        {
            let c = db.lock().unwrap();
            c.execute(
                "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
                rusqlite::params!["test", dir.to_string_lossy().as_ref()],
            )
            .unwrap();
        }

        let repos = vec![("test".to_string(), dir.clone())];
        let (tx, _rx) = broadcast::channel::<DashboardEvent>(16);

        let file = dir.join("gone.py");
        std::fs::write(&file, "def old():\n    pass\n").unwrap();
        handle_file_change(&file, &db, &repos, &tx).await.unwrap();

        {
            let c = db.lock().unwrap();
            crate::db::save_observation(&c, "test", "old", "gone.py", "watch this").unwrap();
        }

        std::fs::remove_file(&file).unwrap();
        handle_file_change(&file, &db, &repos, &tx).await.unwrap();

        let stale: i64 = {
            let c = db.lock().unwrap();
            c.query_row(
                "SELECT is_stale FROM observations WHERE repo_id = 'test' AND symbol_name = 'old' AND filepath = 'gone.py'",
                [],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(stale, 1, "deleted file observations should be marked stale");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_watcher_starts_on_empty_repos() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let db = Arc::new(Mutex::new(conn));
        let (tx, _rx) = broadcast::channel::<DashboardEvent>(16);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = spawn_watcher(db, tx, 500).unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!handle.is_finished(), "watcher should stay alive to pick up repos indexed later");
            handle.abort();
        });
    }
}
