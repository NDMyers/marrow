//! Per-repo SQLite connection pool with idle eviction.

use anyhow::{Context as _, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
// Only RwLock from tokio — connection-level locking uses std::sync::Mutex
// so pool entries can be passed directly to spawn_watcher (which requires std::sync::Mutex).
use tokio::sync::RwLock;

// ── Helpers ───────────────────────────────────────────────────────────────────

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Entry ─────────────────────────────────────────────────────────────────────

/// Connection entry in the pool — stores the connection plus last-access time.
///
/// Fully-qualified std::sync::Mutex in the struct field to avoid any ambiguity
/// (no `use std::sync::Mutex` import is needed).
///
/// **Callers** are responsible for handling mutex poison on `conn.lock()`.
/// Follow the pattern in `src/watcher.rs`: `.map_err(|_| anyhow::anyhow!("DB mutex poisoned"))`.
#[allow(dead_code)]
pub(crate) struct PoolEntry {
    // Fully-qualified: no `use std::sync::Mutex` import is needed; Arc is from std::sync above.
    pub conn: Arc<std::sync::Mutex<rusqlite::Connection>>,
    /// Seconds since UNIX epoch, stored atomically so the fast read-path can
    /// update last_access without upgrading to a write lock.
    pub last_access: Arc<AtomicU64>,
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
    ///
    /// Returns an error if `repo_root` does not exist (canonicalize fails), preventing
    /// silent duplicate pool entries from different path representations of the same root.
    #[allow(dead_code)]
    pub async fn get_or_open(
        &self,
        repo_root: &Path,
    ) -> Result<Arc<std::sync::Mutex<rusqlite::Connection>>> {
        let key = repo_root
            .canonicalize()
            .with_context(|| format!("repo_root does not exist: {}", repo_root.display()))?;

        // Fast path: entry already exists — update last_access atomically (no write lock needed).
        {
            let map = self.inner.read().await;
            if let Some(entry) = map.get(&key) {
                entry.last_access.store(now_secs(), Ordering::Relaxed);
                return Ok(Arc::clone(&entry.conn));
            }
        }

        // Slow path: acquire write lock, re-check, then open if still absent.
        // We must re-check *inside* the write lock before doing the expensive
        // spawn_blocking, so that only one task ever opens the connection.
        let mut map = self.inner.write().await;

        // Re-check: another task may have opened the connection while we waited.
        if let Some(entry) = map.get_mut(&key) {
            entry.last_access.store(now_secs(), Ordering::Relaxed);
            return Ok(Arc::clone(&entry.conn));
        }

        // We hold the write lock — exactly one task will reach this point per key.
        let db_path = key.join(".marrow").join("graph.db");
        let conn = tokio::task::spawn_blocking({
            let db_path = db_path.clone();
            move || crate::db::init_db_or_memory(db_path.to_str().unwrap_or(":memory:"))
        })
        .await
        .context("spawn_blocking for DB open")??;

        let arc = Arc::new(std::sync::Mutex::new(conn));
        let last_access = Arc::new(AtomicU64::new(now_secs()));
        map.insert(
            key,
            PoolEntry {
                conn: Arc::clone(&arc),
                last_access,
            },
        );
        Ok(arc)
    }

    /// Remove entries not accessed within `max_idle`.
    pub async fn evict_stale(&self, max_idle: Duration) {
        let now = now_secs();
        let max_idle_secs = max_idle.as_secs();
        let mut map = self.inner.write().await;
        map.retain(|_, entry| {
            let accessed = entry.last_access.load(Ordering::Relaxed);
            now.saturating_sub(accessed) < max_idle_secs
        });
    }
}

/// Spawn a background task that calls `evict_stale` every `interval`.
///
/// Note: `tokio::time::interval` fires immediately at t=0. We skip the first
/// tick so eviction does not run at daemon startup before any connections exist.
pub fn spawn_eviction_loop(pool: Arc<RepoPool>, max_idle: Duration, interval: Duration) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await; // discard the immediate t=0 tick
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
        let mode: String = conn
            .lock()
            .unwrap()
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
            let map = pool.inner.read().await;
            // Pool stores canonicalized paths as keys
            let key = dir.path().canonicalize().unwrap();
            if let Some(entry) = map.get(&key) {
                // Backdate last_access by 61 minutes so eviction triggers
                let past = now_secs().saturating_sub(61 * 60);
                entry.last_access.store(past, Ordering::Relaxed);
            }
        }

        pool.evict_stale(Duration::from_secs(60 * 60)).await;

        let map = pool.inner.read().await;
        let key = dir.path().canonicalize().unwrap();
        assert!(!map.contains_key(&key));
    }
}
