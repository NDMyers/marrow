//! Per-repo SQLite connection pool with idle eviction.

use anyhow::{Context as _, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
// Only RwLock from tokio — connection-level locking uses std::sync::Mutex
// so pool entries can be passed directly to spawn_watcher (which requires std::sync::Mutex).
use tokio::sync::RwLock;

// ── Entry ─────────────────────────────────────────────────────────────────────

/// Connection entry in the pool — stores the connection plus last-access time.
///
/// Fully-qualified std::sync::Mutex in the struct field to avoid any ambiguity
/// (no `use std::sync::Mutex` import is needed).
#[allow(dead_code)]
pub(crate) struct PoolEntry {
    // Fully-qualified: no `use std::sync::Mutex` import is needed; Arc is from std::sync above.
    pub conn:        Arc<std::sync::Mutex<rusqlite::Connection>>,
    pub last_access: Instant,
}

// ── Pool ──────────────────────────────────────────────────────────────────────

/// Concurrency-safe connection pool.
///
/// Read-locks are used for lookups (common path).
/// Write-locks are taken only when opening a new connection.
pub struct RepoPool {
    pub(crate) inner: Arc<RwLock<HashMap<PathBuf, PoolEntry>>>,
}

impl RepoPool {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the existing connection or open a new one for `repo_root`.
    ///
    /// The database is stored at `<repo_root>/.marrow/graph.db`, which matches
    /// the existing project convention (MARROW_DB_PATH env var or `.marrow/graph.db`
    /// relative to the workspace root). Do NOT use the daemon's own cwd here —
    /// always use the caller-supplied `repo_root` so each repo gets its own DB.
    ///
    /// **IMPORTANT — mutex type:** Returns Arc<std::sync::Mutex<Connection>> to match
    /// spawn_watcher's signature in src/watcher.rs.
    #[allow(dead_code)]
    pub async fn get_or_open(&self, repo_root: &Path) -> Result<Arc<std::sync::Mutex<rusqlite::Connection>>> {
        let key = repo_root
            .canonicalize()
            .unwrap_or_else(|_| repo_root.to_path_buf());

        // Fast path: entry already exists
        {
            let map = self.inner.read().await;
            if let Some(entry) = map.get(&key) {
                return Ok(Arc::clone(&entry.conn));
            }
        }

        // Slow path: open a new connection.
        // Path convention: <repo_root>/.marrow/graph.db  (matches existing `MARROW_DB_PATH` default)
        let db_path = key.join(".marrow").join("graph.db");
        let conn = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || crate::db::init_db_or_memory(db_path.to_str().unwrap_or(":memory:"))
        })
        .await
        .context("spawn_blocking for DB open")??;

        let arc = Arc::new(std::sync::Mutex::new(conn));
        let mut map = self.inner.write().await;
        // Another task may have opened the connection while we waited for the write lock.
        let entry = map.entry(key).or_insert_with(|| PoolEntry {
            conn: Arc::clone(&arc),
            last_access: Instant::now(),
        });
        // Update last_access on every open/get
        entry.last_access = Instant::now();
        Ok(Arc::clone(&entry.conn))
    }

    /// Remove entries not accessed within `max_idle`.
    pub async fn evict_stale(&self, max_idle: Duration) {
        let now = Instant::now();
        let mut map = self.inner.write().await;
        map.retain(|_, entry| {
            now.duration_since(entry.last_access) < max_idle
        });
    }
}

/// Spawn a background task that calls `evict_stale` every `interval`.
pub fn spawn_eviction_loop(pool: Arc<RepoPool>, max_idle: Duration, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            pool.evict_stale(max_idle).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn pool_opens_new_connection_for_new_path() {
        let dir = TempDir::new().unwrap();
        let pool = RepoPool::new();
        let conn = pool.get_or_open(dir.path()).await.unwrap();
        // conn is Arc<std::sync::Mutex<Connection>> — use .lock().unwrap(), NOT .await
        let mode: String = conn.lock().unwrap()
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
    }

    #[tokio::test]
    async fn pool_returns_same_arc_for_same_path() {
        let dir = TempDir::new().unwrap();
        let pool = RepoPool::new();
        let c1 = pool.get_or_open(dir.path()).await.unwrap();
        let c2 = pool.get_or_open(dir.path()).await.unwrap();
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[tokio::test]
    async fn evict_removes_stale_entries() {
        let dir = TempDir::new().unwrap();
        let pool = RepoPool::new();
        pool.get_or_open(dir.path()).await.unwrap();

        {
            let mut map = pool.inner.write().await;
            // Pool stores canonicalized paths as keys
            let key = dir.path().canonicalize().unwrap_or_else(|_| dir.path().to_path_buf());
            if let Some(entry) = map.get_mut(&key) {
                // Backdate last_access by 61 minutes so eviction triggers
                entry.last_access = Instant::now()
                    .checked_sub(Duration::from_secs(61 * 60))
                    .unwrap_or_else(Instant::now);
            }
        }

        pool.evict_stale(Duration::from_secs(60 * 60)).await;

        let map = pool.inner.read().await;
        let key = dir.path().canonicalize().unwrap_or_else(|_| dir.path().to_path_buf());
        assert!(!map.contains_key(&key));
    }
}
