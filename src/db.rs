use anyhow::Result;
use rusqlite::Connection;

/// Opens (or creates) the database at the given path,
/// sets WAL mode and synchronous=NORMAL, then creates tables.
pub fn init_db(db_path: &str) -> Result<Connection> {
    let conn = Connection::open(db_path)?;
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;",
    )?;
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

        CREATE INDEX IF NOT EXISTS idx_nodes_repo ON nodes(repo_id);
        CREATE INDEX IF NOT EXISTS idx_nodes_symbol ON nodes(symbol_name);
        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);

        CREATE TABLE IF NOT EXISTS stats (
            key   TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        );",
    )?;
    Ok(conn)
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

/// Returns the current value of a stats counter, or 0 if not set.
pub fn read_stat(conn: &Connection, key: &str) -> Result<i64> {
    let val = conn
        .query_row(
            "SELECT value FROM stats WHERE key = ?1",
            rusqlite::params![key],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_or(0);
    Ok(val)
}
