use anyhow::Result;
use rusqlite::{Connection, OptionalExtension as _};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub struct IndexedRepoSnapshot {
    pub repo_id: String,
    pub root_path: String,
    pub symbol_count: i64,
    pub file_count: i64,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
pub struct DatabaseScopeSnapshot {
    pub repo_count: i64,
    pub symbol_count: i64,
    pub file_count: i64,
    pub repos: Vec<IndexedRepoSnapshot>,
}

/// Opens (or creates) the database at `db_path`.
/// If the filesystem is read-only or the directory cannot be created, falls back
/// transparently to an in-memory database so the MCP server can still function
/// inside sandboxed environments (e.g. macOS App Sandbox, read-only CWDs).
pub fn init_db_or_memory(db_path: &str) -> Result<Connection> {
    let try_disk = || -> Result<Connection> {
        if let Some(parent) = std::path::Path::new(db_path)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        init_db(db_path)
    };

    match try_disk() {
        Ok(conn) => Ok(conn),
        Err(e) => {
            eprintln!("[marrow] disk DB unavailable ({e}), falling back to :memory:");
            init_db(":memory:")
        }
    }
}

/// Tunes SQLite memory behavior for long-lived Marrow processes.
///
/// Environment (all optional):
/// - `MARROW_SQLITE_CACHE_KIB` — page cache size in **kibibytes** (SQLite `cache_size` is set to
///   the negative of this value). Default `262144` (256 MiB). Larger graphs need a larger cache
///   for throughput; smaller values reduce idle RSS.
/// - `MARROW_SQLITE_MMAP_BYTES` — passed to `PRAGMA mmap_size`. Default `0` (disable mmap).
///   SQLite’s mmap of large `graph.db` files often shows up as multi‑GB RSS in Activity Monitor
///   even when the working set is smaller. Set to a positive value (bytes) to re‑enable mmap.
pub fn apply_sqlite_memory_settings(conn: &Connection) -> Result<()> {
    // 32 MiB is sufficient for any realistic AST query workload.
    // The old 256 MiB default added ~224 MiB of permanent idle RSS per server instance.
    // Override with MARROW_SQLITE_CACHE_KIB (kibibytes) if a larger cache is needed.
    let cache_kib: i64 = std::env::var("MARROW_SQLITE_CACHE_KIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(32_768); // 32 MiB

    conn.pragma_update(None, "cache_size", -cache_kib)?;

    let mmap_bytes: i64 = std::env::var("MARROW_SQLITE_MMAP_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n >= 0)
        .unwrap_or(0);
    conn.pragma_update(None, "mmap_size", mmap_bytes)?;
    Ok(())
}

/// Opens (or creates) the database at the given path,
/// sets WAL mode and synchronous=NORMAL, then creates tables.
pub fn init_db(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA busy_timeout=30000;
         PRAGMA temp_store=FILE;
         PRAGMA auto_vacuum=INCREMENTAL;",
    )?;
    apply_sqlite_memory_settings(&conn)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS repositories (
            id        TEXT PRIMARY KEY,
            root_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS nodes (
            id          TEXT PRIMARY KEY,
            repo_id     TEXT NOT NULL REFERENCES repositories(id),
            file_path   TEXT NOT NULL,
            language    TEXT NOT NULL,
            symbol_name TEXT NOT NULL,
            symbol_type TEXT NOT NULL,
            raw_text    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS edges (
            source_id         TEXT NOT NULL REFERENCES nodes(id),
            target_id         TEXT NOT NULL REFERENCES nodes(id),
            relationship_type TEXT NOT NULL,
            PRIMARY KEY (source_id, target_id, relationship_type)
        );

        CREATE TABLE IF NOT EXISTS graph_node_degrees (
            repo_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            degree  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (repo_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS graph_degree_cache_meta (
            repo_id      TEXT PRIMARY KEY,
            node_count   INTEGER NOT NULL DEFAULT 0,
            dirty        INTEGER NOT NULL DEFAULT 1,
            refreshed_at INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_nodes_repo ON nodes(repo_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_symbol ON nodes(symbol_name);
        -- MARROW-PERF-010: composite covers repo+file and repo+symbol hot paths (ingest + callee join).
        CREATE INDEX IF NOT EXISTS idx_nodes_repo_file ON nodes(repo_id, file_path);
        CREATE INDEX IF NOT EXISTS idx_nodes_repo_symbol ON nodes(repo_id, symbol_name);
        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);
        CREATE INDEX IF NOT EXISTS idx_graph_node_degrees_repo_rank
            ON graph_node_degrees(repo_id, degree DESC, node_id);

        CREATE TABLE IF NOT EXISTS stats (
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS observations (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_id           TEXT NOT NULL DEFAULT '',
            symbol_name       TEXT NOT NULL,
            filepath          TEXT NOT NULL,
            observation_text  TEXT NOT NULL,
            timestamp         DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_known_hash   TEXT NOT NULL,
            is_stale          INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_obs_repo     ON observations(repo_id);
        CREATE INDEX IF NOT EXISTS idx_obs_symbol   ON observations(symbol_name);
        CREATE INDEX IF NOT EXISTS idx_obs_filepath ON observations(filepath);

        CREATE TABLE IF NOT EXISTS files (
            repo_id      TEXT NOT NULL REFERENCES repositories(id),
            file_path    TEXT NOT NULL,
            mtime_secs   INTEGER NOT NULL,
            content_hash TEXT NOT NULL,
            PRIMARY KEY (repo_id, file_path)
        );

        CREATE TABLE IF NOT EXISTS file_imports (
            repo_id     TEXT NOT NULL REFERENCES repositories(id),
            file_path   TEXT NOT NULL,
            import_name TEXT NOT NULL,
            PRIMARY KEY (repo_id, file_path, import_name)
        );
        CREATE INDEX IF NOT EXISTS idx_file_imports_name ON file_imports(import_name);",
    )?;
    ensure_observations_repo_id(&conn)?;
    ensure_performance_indexes(&conn)?;
    Ok(conn)
}

/// Adds indexes introduced after initial schema (idempotent).
fn ensure_performance_indexes(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_nodes_repo_file ON nodes(repo_id, file_path);
         CREATE INDEX IF NOT EXISTS idx_nodes_repo_symbol ON nodes(repo_id, symbol_name);
         CREATE TABLE IF NOT EXISTS graph_node_degrees (
            repo_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
            degree  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (repo_id, node_id)
         );
         CREATE TABLE IF NOT EXISTS graph_degree_cache_meta (
            repo_id      TEXT PRIMARY KEY,
            node_count   INTEGER NOT NULL DEFAULT 0,
            dirty        INTEGER NOT NULL DEFAULT 1,
            refreshed_at INTEGER NOT NULL DEFAULT 0
         );
         CREATE INDEX IF NOT EXISTS idx_graph_node_degrees_repo_rank
            ON graph_node_degrees(repo_id, degree DESC, node_id);",
    )?;
    Ok(())
}

pub fn mark_graph_degrees_dirty(conn: &Connection, repo_id: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO graph_degree_cache_meta (repo_id, node_count, dirty, refreshed_at)
         VALUES (?1, 0, 1, CAST(strftime('%s', 'now') AS INTEGER))
         ON CONFLICT(repo_id) DO UPDATE SET
            dirty = 1,
            refreshed_at = CAST(strftime('%s', 'now') AS INTEGER)",
        rusqlite::params![repo_id],
    )?;
    Ok(())
}

pub fn graph_degrees_are_fresh(conn: &Connection, repo_id: &str) -> Result<bool> {
    let meta = conn
        .query_row(
            "SELECT node_count, dirty FROM graph_degree_cache_meta WHERE repo_id = ?1",
            rusqlite::params![repo_id],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;

    let Some((cached_node_count, dirty)) = meta else {
        return Ok(false);
    };
    if dirty != 0 {
        return Ok(false);
    }

    let current_node_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get(0),
    )?;
    if cached_node_count != current_node_count {
        return Ok(false);
    }

    let cached_degree_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM graph_node_degrees WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get(0),
    )?;
    Ok(cached_degree_count == current_node_count)
}

pub fn ensure_graph_degrees(conn: &Connection, repo_id: &str) -> Result<bool> {
    if graph_degrees_are_fresh(conn, repo_id)? {
        return Ok(false);
    }
    refresh_graph_degrees(conn, repo_id)?;
    Ok(true)
}

pub fn refresh_graph_degrees(conn: &Connection, repo_id: &str) -> Result<()> {
    conn.execute_batch("SAVEPOINT graph_degree_refresh")?;
    let result = (|| -> Result<()> {
        conn.execute(
            "DELETE FROM graph_node_degrees WHERE repo_id = ?1",
            rusqlite::params![repo_id],
        )?;
        conn.execute(
            "INSERT INTO graph_node_degrees (repo_id, node_id, degree)
                 WITH valid_edges AS (
                     SELECT e.source_id, e.target_id, src.repo_id AS source_repo, tgt.repo_id AS target_repo
                     FROM edges e
                     JOIN nodes src ON src.id = e.source_id
                     JOIN nodes tgt ON tgt.id = e.target_id
                     WHERE src.repo_id = ?1 OR tgt.repo_id = ?1
             ),
             degree_counts AS (
                SELECT id, COUNT(*) AS degree
                FROM (
                          SELECT source_id AS id FROM valid_edges WHERE source_repo = ?1
                    UNION ALL
                          SELECT target_id AS id FROM valid_edges WHERE target_repo = ?1
                )
                GROUP BY id
             )
             SELECT n.repo_id, n.id, COALESCE(dc.degree, 0)
             FROM nodes n
             LEFT JOIN degree_counts dc ON dc.id = n.id
             WHERE n.repo_id = ?1",
            rusqlite::params![repo_id],
        )?;
        let node_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
            rusqlite::params![repo_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO graph_degree_cache_meta (repo_id, node_count, dirty, refreshed_at)
             VALUES (?1, ?2, 0, CAST(strftime('%s', 'now') AS INTEGER))
             ON CONFLICT(repo_id) DO UPDATE SET
                node_count = excluded.node_count,
                dirty = 0,
                refreshed_at = excluded.refreshed_at",
            rusqlite::params![repo_id, node_count],
        )?;
        Ok(())
    })();

    match result {
        Ok(()) => {
            conn.execute_batch("RELEASE graph_degree_refresh")?;
            Ok(())
        }
        Err(e) => {
            let _ = conn
                .execute_batch("ROLLBACK TO graph_degree_refresh; RELEASE graph_degree_refresh");
            Err(e)
        }
    }
}

/// Runs a WAL checkpoint (truncating the WAL file) and an incremental
/// vacuum pass. Used after ingest (unless skipped via env) and by `marrow maintenance`.
pub fn vacuum_and_checkpoint(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA wal_checkpoint(TRUNCATE);
         PRAGMA incremental_vacuum;",
    )?;
    Ok(())
}

/// Post-ingest maintenance: checkpoint + incremental vacuum, unless
/// `MARROW_SKIP_POST_INGEST_MAINTENANCE` is set (any non-empty value).
///
/// Skipping reduces I/O latency right after a large ingest; the DB may retain
/// more WAL space until you run [`run_graph_maintenance`] (`marrow maintenance`).
pub fn post_ingest_maintenance(conn: &Connection) -> Result<()> {
    if std::env::var_os("MARROW_SKIP_POST_INGEST_MAINTENANCE").is_some_and(|v| !v.is_empty()) {
        return Ok(());
    }
    vacuum_and_checkpoint(conn)
}

/// Explicit checkpoint + incremental vacuum (CLI `marrow maintenance`). Always runs.
pub fn run_graph_maintenance(conn: &Connection) -> Result<()> {
    vacuum_and_checkpoint(conn)
}

/// Atomically increments a scalar counter in the stats table.
pub fn increment_stat(conn: &Connection, key: &str, delta: i64) -> Result<()> {
    conn.execute(
        "INSERT INTO stats (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = value + ?2",
        rusqlite::params![key, delta],
    )?;
    Ok(())
}

fn canonicalize_best_effort(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn ensure_observations_repo_id(conn: &Connection) -> Result<()> {
    let has_repo_id = conn
        .prepare("PRAGMA table_info(observations)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .any(|name| name == "repo_id");

    if !has_repo_id {
        conn.execute(
            "ALTER TABLE observations ADD COLUMN repo_id TEXT NOT NULL DEFAULT ''",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_obs_repo ON observations(repo_id)",
            [],
        )?;
    }

    Ok(())
}

pub fn repo_id_for_root(conn: &Connection, root_path: &Path) -> Result<Option<String>> {
    let canonical = canonicalize_best_effort(root_path);
    conn.query_row(
        "SELECT id FROM repositories WHERE root_path = ?1 LIMIT 1",
        rusqlite::params![canonical.to_string_lossy().to_string()],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

#[allow(dead_code)] // retained for unit tests; production callers use is_repo_indexed_by_id
pub fn is_repo_indexed(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<bool> {
    let canonical = canonicalize_best_effort(root_path);
    let exists = conn
        .query_row(
            "SELECT 1
             FROM repositories r
             WHERE r.id = ?1
               AND r.root_path = ?2
               AND EXISTS (SELECT 1 FROM nodes n WHERE n.repo_id = r.id LIMIT 1)
             LIMIT 1",
            rusqlite::params![repo_id, canonical.to_string_lossy().to_string()],
            |_| Ok(true),
        )
        .optional()?
        .is_some();
    Ok(exists)
}

/// Returns the current value of a stats counter, or 0 if the key is absent or on error.
pub fn read_stat(conn: &Connection, key: &str) -> i64 {
    conn.query_row(
        "SELECT value FROM stats WHERE key = ?1",
        rusqlite::params![key],
        |row| row.get::<_, i64>(0),
    )
    .unwrap_or(0)
}

pub fn database_scope_snapshot(conn: &Connection) -> Result<DatabaseScopeSnapshot> {
    let symbol_count = conn.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
    let file_count = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;

    // Per-repo counts use scalar subqueries instead of joining `nodes` and `files` in one
    // GROUP BY. A double LEFT JOIN materializes a repo-sized cross product (symbols × files)
    // before DISTINCT — tens of seconds and large temp stores on ~45k+ node graphs (e.g.
    // Accrualify-scale). Subqueries keep semantics: COUNT(*) per repo_id matches
    // COUNT(DISTINCT id) / COUNT(DISTINCT file_path) given primary keys on those tables.
    let mut stmt = conn.prepare(
        "SELECT
            r.id,
            r.root_path,
            (SELECT COUNT(*) FROM nodes n WHERE n.repo_id = r.id) AS symbol_count,
            (SELECT COUNT(*) FROM files f WHERE f.repo_id = r.id) AS file_count
         FROM repositories r
         WHERE EXISTS (SELECT 1 FROM nodes n WHERE n.repo_id = r.id)
            OR EXISTS (SELECT 1 FROM files f WHERE f.repo_id = r.id)
         ORDER BY r.id ASC",
    )?;

    let repos = stmt
        .query_map([], |row| {
            Ok(IndexedRepoSnapshot {
                repo_id: row.get(0)?,
                root_path: row.get(1)?,
                symbol_count: row.get(2)?,
                file_count: row.get(3)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let repo_count = repos.len() as i64;

    Ok(DatabaseScopeSnapshot {
        repo_count,
        symbol_count,
        file_count,
        repos,
    })
}

pub fn connected_database_path(conn: &Connection) -> Result<String> {
    let mut stmt = conn.prepare("PRAGMA database_list")?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == "main" {
            let path: String = row.get(2)?;
            if path.is_empty() {
                return Ok(":memory:".to_string());
            }
            return Ok(path);
        }
    }
    Ok(":memory:".to_string())
}

pub fn clear_index_contents(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "BEGIN IMMEDIATE;
         DELETE FROM edges;
         DELETE FROM graph_node_degrees;
         DELETE FROM graph_degree_cache_meta;
         DELETE FROM nodes;
         DELETE FROM files;
         DELETE FROM file_imports;
         COMMIT;",
    )?;
    Ok(())
}

/// FNV-1a hash of raw file bytes — used for change detection in the `files` table.
pub fn hash_file_content(bytes: &[u8]) -> String {
    let mut h: u64 = 14_695_981_039_346_656_037;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    format!("{h:016x}")
}

/// FNV-1a 64-bit hash of `text`, returned as a 16-character hex string.
/// Deterministic across Rust versions — suitable for cross-run change detection.
pub fn hash_raw_text(text: &str) -> String {
    let mut h: u64 = 14_695_981_039_346_656_037;
    for b in text.bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(1_099_511_628_211);
    }
    format!("{h:016x}")
}

/// Saves an LLM observation linked to a specific indexed symbol.
///
/// Looks up the node's current `raw_text`, hashes it, and stores the
/// observation alongside that hash so staleness can be detected later.
/// Returns an error if no matching node exists in the graph.
pub fn save_observation(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: &str,
    observation_text: &str,
) -> Result<String> {
    let raw_text: Option<String> = conn
        .query_row(
            "SELECT raw_text
             FROM nodes
             WHERE repo_id = ?1 AND symbol_name = ?2 AND file_path = ?3
             LIMIT 1",
            rusqlite::params![repo_id, symbol_name, filepath],
            |row| row.get(0),
        )
        .optional()?;

    let hash = match raw_text {
        Some(ref text) => hash_raw_text(text),
        None => {
            return Err(anyhow::anyhow!(
                "No indexed node found for symbol '{}' in '{}'. \
                 Run ingest_repo first, then supply the relative file path \
                 as stored in the graph (e.g. 'src/main.rs').",
                symbol_name,
                filepath
            ))
        }
    };

    conn.execute(
        "INSERT INTO observations (repo_id, symbol_name, filepath, observation_text, last_known_hash)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![repo_id, symbol_name, filepath, observation_text, hash],
    )?;

    Ok(format!(
        "Memory saved: '{}' in '{}' (node hash: {}). Observation linked to graph.",
        symbol_name,
        filepath,
        &hash[..8]
    ))
}

/// Queries stored observations for a symbol and/or file path.
///
/// Stale observations (code changed since recording) are prefixed with a
/// prominent warning so callers know to re-verify them.
pub fn get_session_context(
    conn: &Connection,
    repo_id: Option<&str>,
    symbol_name: Option<&str>,
    filepath: Option<&str>,
) -> Result<String> {
    type Row = (String, i64, String, String, String, String);
    let mapper = |row: &rusqlite::Row<'_>| -> rusqlite::Result<Row> {
        Ok((
            row.get(0)?,
            row.get::<_, i64>(1)?,
            row.get(2)?,
            row.get(3)?,
            row.get(4)?,
            row.get(5)?,
        ))
    };

    let rows: Vec<Row> = match (repo_id, symbol_name, filepath) {
        (Some(repo), Some(sym), Some(fp)) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE repo_id = ?1 AND symbol_name = ?2 AND filepath = ?3
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![repo, sym, fp], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (Some(repo), Some(sym), None) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE repo_id = ?1 AND symbol_name = ?2
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![repo, sym], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (Some(repo), None, Some(fp)) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE repo_id = ?1 AND filepath = ?2
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![repo, fp], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (Some(repo), None, None) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE repo_id = ?1
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![repo], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (None, Some(sym), Some(fp)) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE symbol_name = ?1 AND filepath = ?2
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![sym, fp], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (None, Some(sym), None) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE symbol_name = ?1
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![sym], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (None, None, Some(fp)) => conn
            .prepare(
                "SELECT observation_text, is_stale, timestamp, symbol_name, filepath, repo_id
                 FROM observations WHERE filepath = ?1
                 ORDER BY timestamp DESC",
            )?
            .query_map(rusqlite::params![fp], mapper)?
            .filter_map(|r| r.ok())
            .collect(),

        (None, None, None) => {
            return Err(anyhow::anyhow!(
                "Provide at least one of: repo_id, symbol_name, filepath."
            ))
        }
    };

    if rows.is_empty() {
        return Ok("No session memories found for the requested context.".to_string());
    }

    let mut out = String::new();
    for (text, is_stale, ts, sym, fp, repo) in rows {
        if is_stale != 0 {
            out.push_str(&format!(
                "[STALE MEMORY WARNING: The underlying code has changed since this was \
                 recorded. Re-verify before trusting.] - {text}\n  \
                 (repo: {repo}, symbol: {sym}, file: {fp}, recorded: {ts})\n\n"
            ));
        } else {
            out.push_str(&format!(
                "{text}\n  (repo: {repo}, symbol: {sym}, file: {fp}, recorded: {ts})\n\n"
            ));
        }
    }

    Ok(out.trim_end().to_string())
}

/// Marks observations stale when the re-ingested node's hash differs from
/// `last_known_hash`. Called inside the ingestion loop after each node upsert.
pub fn mark_stale_observations(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: &str,
    new_hash: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE observations SET is_stale = 1
         WHERE repo_id = ?1 AND symbol_name = ?2 AND filepath = ?3 AND last_known_hash != ?4",
        rusqlite::params![repo_id, symbol_name, filepath, new_hash],
    )?;
    Ok(())
}

pub fn mark_deleted_observation_stale(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: &str,
) -> Result<()> {
    conn.execute(
        "UPDATE observations SET is_stale = 1
         WHERE repo_id = ?1 AND symbol_name = ?2 AND filepath = ?3",
        rusqlite::params![repo_id, symbol_name, filepath],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn init_db_applies_busy_timeout_pragma() {
        let conn = init_db(":memory:").expect("init_db failed");
        let timeout: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .expect("PRAGMA busy_timeout failed");
        assert_eq!(timeout, 30000, "busy_timeout should be 30000ms");
    }

    #[test]
    fn init_db_applies_default_memory_pragmas() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("t.db");
        let path_str = db_path.to_str().unwrap();
        let conn = init_db(path_str).expect("init_db failed");
        let cache: i64 = conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .expect("PRAGMA cache_size");
        assert_eq!(
            cache, -32_768,
            "default cache_size should be -32768 KiB (32 MiB page cache)"
        );
        let mmap: i64 = conn
            .query_row("PRAGMA mmap_size", [], |row| row.get(0))
            .expect("PRAGMA mmap_size");
        assert_eq!(mmap, 0, "default mmap_size should be 0 (mmap disabled)");
    }

    #[test]
    fn repo_index_check_is_scoped_to_repo_and_root() {
        let conn = init_db(":memory:").expect("init_db failed");
        let repo_a = tempdir().unwrap();
        let repo_b = tempdir().unwrap();
        let root_a = repo_a.path().canonicalize().unwrap();
        let root_b = repo_b.path().canonicalize().unwrap();

        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params!["repo_a", root_a.to_string_lossy().to_string()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                "repo_a:src/lib.rs:helper",
                "repo_a",
                "src/lib.rs",
                "rs",
                "helper",
                "function",
                "fn helper() {}"
            ],
        )
        .unwrap();

        assert!(is_repo_indexed(&conn, "repo_a", &root_a).unwrap());
        assert!(!is_repo_indexed(&conn, "repo_b", &root_b).unwrap());
        assert!(
            !is_repo_indexed(&conn, "repo_a", &root_b).unwrap(),
            "repo index check should not treat a different root as indexed"
        );
    }

    #[test]
    fn observations_are_scoped_to_repo() {
        let conn = init_db(":memory:").expect("init_db failed");
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params!["repo_a", "/tmp/repo_a", "repo_b", "/tmp/repo_b"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7),
                    (?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                "repo_a:src/shared.rs:helper",
                "repo_a",
                "src/shared.rs",
                "rs",
                "helper",
                "function",
                "fn helper() {}",
                "repo_b:src/shared.rs:helper",
                "repo_b",
                "src/shared.rs",
                "rs",
                "helper",
                "function",
                "fn helper() {}"
            ],
        )
        .unwrap();

        save_observation(&conn, "repo_a", "helper", "src/shared.rs", "repo a memory").unwrap();
        save_observation(&conn, "repo_b", "helper", "src/shared.rs", "repo b memory").unwrap();

        let repo_a_ctx =
            get_session_context(&conn, Some("repo_a"), Some("helper"), Some("src/shared.rs"))
                .unwrap();
        assert!(
            repo_a_ctx.contains("repo a memory"),
            "repo_a memory missing: {repo_a_ctx}"
        );
        assert!(
            !repo_a_ctx.contains("repo b memory"),
            "repo_b memory leaked: {repo_a_ctx}"
        );
    }

    #[test]
    fn files_table_exists_after_init() {
        let conn = init_db(":memory:").unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='files'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "files table should exist after init_db");
    }

    #[test]
    fn vacuum_and_checkpoint_runs_without_error() {
        let conn = init_db(":memory:").unwrap();
        vacuum_and_checkpoint(&conn).expect("vacuum_and_checkpoint should not error");
    }

    #[test]
    fn hash_file_content_is_deterministic() {
        let a = hash_file_content(b"hello world");
        let b = hash_file_content(b"hello world");
        let c = hash_file_content(b"different");
        assert_eq!(a, b, "same input must produce same hash");
        assert_ne!(a, c, "different input must produce different hash");
        assert_eq!(a.len(), 16, "hash must be 16 hex chars");
    }

    #[test]
    fn files_table_primary_key_is_repo_and_path() {
        let conn = init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('r', '/tmp/r')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (repo_id, file_path, mtime_secs, content_hash) VALUES ('r', 'a.rb', 1, 'abc')",
            [],
        ).unwrap();
        // Duplicate insert should fail due to PRIMARY KEY constraint
        let result = conn.execute(
            "INSERT INTO files (repo_id, file_path, mtime_secs, content_hash) VALUES ('r', 'a.rb', 2, 'xyz')",
            [],
        );
        assert!(
            result.is_err(),
            "duplicate (repo_id, file_path) should violate PK"
        );
    }

    #[test]
    fn database_scope_snapshot_reports_repo_aware_totals() {
        let conn = init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params!["frontend", "/tmp/frontend", "backend", "/tmp/backend"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES
                (?1, ?2, ?3, 'ts', ?4, 'function', 'export function App() {}'),
                (?5, ?6, ?7, 'ts', ?8, 'function', 'export function Header() {}'),
                (?9, ?10, ?11, 'rs', ?12, 'function', 'fn main() {}')",
            rusqlite::params![
                "frontend:src/app.ts:App",
                "frontend",
                "src/app.ts",
                "App",
                "frontend:src/header.ts:Header",
                "frontend",
                "src/header.ts",
                "Header",
                "backend:src/main.rs:main",
                "backend",
                "src/main.rs",
                "main"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (repo_id, file_path, mtime_secs, content_hash)
             VALUES
                ('frontend', 'src/app.ts', 1, 'a'),
                ('frontend', 'src/header.ts', 1, 'b'),
                ('backend', 'src/main.rs', 1, 'c')",
            [],
        )
        .unwrap();

        let snapshot = database_scope_snapshot(&conn).unwrap();
        assert_eq!(snapshot.repo_count, 2);
        assert_eq!(snapshot.symbol_count, 3);
        assert_eq!(snapshot.file_count, 3);
        assert_eq!(snapshot.repos.len(), 2);

        let repos: HashMap<_, _> = snapshot
            .repos
            .into_iter()
            .map(|repo| {
                let repo_id = repo.repo_id.clone();
                (repo_id, repo)
            })
            .collect();

        let frontend = repos.get("frontend").expect("frontend repo missing");
        assert_eq!(frontend.symbol_count, 2);
        assert_eq!(frontend.file_count, 2);
        assert_eq!(frontend.root_path, "/tmp/frontend");

        let backend = repos.get("backend").expect("backend repo missing");
        assert_eq!(backend.symbol_count, 1);
        assert_eq!(backend.file_count, 1);
        assert_eq!(backend.root_path, "/tmp/backend");
    }

    #[test]
    fn database_scope_snapshot_excludes_unindexed_repo_records() {
        let conn = init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params![
                "indexed_repo",
                "/tmp/indexed_repo",
                "empty_repo",
                "/tmp/empty_repo"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, 'rs', ?4, 'function', 'fn main() {}')",
            rusqlite::params![
                "indexed_repo:src/main.rs:main",
                "indexed_repo",
                "src/main.rs",
                "main"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (repo_id, file_path, mtime_secs, content_hash)
             VALUES ('indexed_repo', 'src/main.rs', 1, 'abc')",
            [],
        )
        .unwrap();

        let snapshot = database_scope_snapshot(&conn).unwrap();
        assert_eq!(snapshot.repo_count, 1);
        assert_eq!(snapshot.repos.len(), 1);
        assert_eq!(snapshot.repos[0].repo_id, "indexed_repo");
    }

    #[test]
    fn connected_database_path_reports_memory_for_in_memory_db() {
        let conn = init_db(":memory:").unwrap();
        assert_eq!(connected_database_path(&conn).unwrap(), ":memory:");
    }

    #[test]
    fn refresh_graph_degrees_rebuilds_rank_data_and_ignores_invalid_edges() {
        let conn = init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('repo', '/tmp/repo'), ('other', '/tmp/other')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES
                ('repo:n1', 'repo', 'src/a.rs', 'rs', 'a', 'function', 'fn a() {}'),
                ('repo:n2', 'repo', 'src/b.rs', 'rs', 'b', 'function', 'fn b() {}'),
                ('repo:n3', 'repo', 'src/c.rs', 'rs', 'c', 'function', 'fn c() {}'),
                ('other:n1', 'other', 'src/a.rs', 'rs', 'a', 'function', 'fn a() {}')",
            [],
        )
        .unwrap();
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        conn.execute(
            "INSERT INTO edges (source_id, target_id, relationship_type)
             VALUES
                ('repo:n1', 'repo:n2', 'CALLS'),
                ('repo:n1', 'repo:n3', 'CALLS'),
                ('repo:n1', 'missing:n4', 'CALLS'),
                ('repo:n1', 'other:n1', 'IMPORTS')",
            [],
        )
        .unwrap();

        mark_graph_degrees_dirty(&conn, "repo").unwrap();
        assert!(!graph_degrees_are_fresh(&conn, "repo").unwrap());
        refresh_graph_degrees(&conn, "repo").unwrap();
        assert!(graph_degrees_are_fresh(&conn, "repo").unwrap());

        let ranked: Vec<(String, i64)> = conn
            .prepare(
                "SELECT node_id, degree
                 FROM graph_node_degrees
                 WHERE repo_id = 'repo'
                 ORDER BY degree DESC, node_id ASC",
            )
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(
            ranked,
            vec![
                ("repo:n1".to_string(), 3),
                ("repo:n2".to_string(), 1),
                ("repo:n3".to_string(), 1),
            ]
        );

        refresh_graph_degrees(&conn, "other").unwrap();
        let other_degree: i64 = conn
            .query_row(
                "SELECT degree FROM graph_node_degrees WHERE repo_id = 'other' AND node_id = 'other:n1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            other_degree, 1,
            "cross-repo IMPORTS must count for the target repo"
        );

        conn.execute(
            "DELETE FROM edges WHERE source_id = 'repo:n1' AND target_id = 'repo:n3'",
            [],
        )
        .unwrap();
        mark_graph_degrees_dirty(&conn, "repo").unwrap();
        refresh_graph_degrees(&conn, "repo").unwrap();

        let degree: i64 = conn
            .query_row(
                "SELECT degree FROM graph_node_degrees WHERE repo_id = 'repo' AND node_id = 'repo:n1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(degree, 2, "degree cache must refresh after edge mutation");
    }
}
