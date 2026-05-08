use anyhow::{anyhow, Context as _, Result};
use rusqlite::{Connection, OptionalExtension as _};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const REGISTRY_SCHEMA_VERSION: i64 = 1;
const GRAPH_INITIAL_NODE_CAP_DEFAULT: usize = 500;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStatus {
    Available,
    Empty,
    MissingDb,
    InaccessibleDb,
    Locked,
    Corrupt,
    OutOfBoundsPending,
}

impl WorkspaceStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Empty => "empty",
            Self::MissingDb => "missing_db",
            Self::InaccessibleDb => "inaccessible_db",
            Self::Locked => "locked",
            Self::Corrupt => "corrupt",
            Self::OutOfBoundsPending => "out_of_bounds_pending",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "available" => Self::Available,
            "empty" => Self::Empty,
            "missing_db" => Self::MissingDb,
            "locked" => Self::Locked,
            "corrupt" => Self::Corrupt,
            "out_of_bounds_pending" => Self::OutOfBoundsPending,
            _ => Self::InaccessibleDb,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupKind {
    Unregister,
    ClearIndex,
    DeleteDb,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkspaceEntry {
    pub workspace_id: String,
    pub workspace_root: PathBuf,
    pub graph_db_path: PathBuf,
    pub display_name: Option<String>,
    pub status: WorkspaceStatus,
    pub registered_at: u64,
    pub last_seen_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DbInventoryRow {
    pub workspace_id: String,
    pub workspace_root: PathBuf,
    pub graph_db_path: PathBuf,
    pub display_name: Option<String>,
    pub status: WorkspaceStatus,
    pub size_mb: f64,
    pub repo_count: i64,
    pub symbol_count: i64,
    pub file_count: i64,
    pub repos: Vec<crate::db::IndexedRepoSnapshot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GlobalLifetimeStats {
    pub total_requests: i64,
    pub total_tokens_saved: i64,
    pub total_file_tokens: i64,
    pub pipeline_requests: i64,
    pub direct_low_level_autorouted: i64,
    pub direct_low_level_rejected: i64,
    pub ambiguous_symbol_requests: i64,
    pub stale_capsule_prevented: i64,
    pub reduction_pct: f64,
    pub pipeline_compliance_pct: f64,
    pub workspace_statuses: Vec<DbInventoryRow>,
}

/// Combined result of a single registry scan: lifetime aggregates plus the
/// workspace list that produced them. Returned by [`Registry::stats_aggregate`]
/// to avoid the duplicate `list_workspaces()` call present in the old
/// `global_lifetime_stats()` + `list_workspaces()` pattern.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct StatsAggregate {
    pub lifetime: GlobalLifetimeStats,
    pub workspaces: Vec<WorkspaceEntry>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraphNodeDto {
    pub id: String,
    pub label: String,
    pub file_path: String,
    pub symbol_type: String,
    pub degree: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraphEdgeDto {
    pub source: String,
    pub target: String,
    pub relationship: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GraphSnapshot {
    pub nodes: Vec<GraphNodeDto>,
    pub edges: Vec<GraphEdgeDto>,
    pub truncated: bool,
    pub total_node_count: i64,
    pub workspace_id: String,
    pub repo_id: Option<String>,
    pub status: WorkspaceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub struct Registry {
    conn: Connection,
}

impl Registry {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path != Path::new(":memory:") {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
        }
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=30000;",
        )?;
        let registry = Self { conn };
        registry.migrate()?;
        Ok(registry)
    }

    pub fn open_default() -> Result<Self> {
        Self::open(default_registry_path())
    }

    #[allow(dead_code)]
    pub fn connection(&self) -> &Connection {
        &self.conn
    }

    pub fn register_workspace(
        &self,
        workspace_root: impl AsRef<Path>,
        display_name: Option<&str>,
    ) -> Result<WorkspaceEntry> {
        self.register_workspace_with_boundary(workspace_root, None, false, display_name)
    }

    pub fn register_workspace_with_boundary(
        &self,
        workspace_root: impl AsRef<Path>,
        trusted_root: Option<&Path>,
        user_confirmed: bool,
        display_name: Option<&str>,
    ) -> Result<WorkspaceEntry> {
        let root = canonicalize_existing_dir(workspace_root.as_ref())?;
        if let Some(boundary) = trusted_root {
            let trusted = canonicalize_existing_dir(boundary)?;
            if !root.starts_with(&trusted) && !user_confirmed {
                return Err(anyhow!(
                    "workspace '{}' is outside the trusted workspace boundary '{}'",
                    root.display(),
                    trusted.display()
                ));
            }
        }

        let marrow_dir = root.join(".marrow");
        std::fs::create_dir_all(&marrow_dir)?;
        let graph_db_path = marrow_dir.join("graph.db");
        let workspace_id = workspace_id_for_root(&root);
        let now = now_secs();
        let existing_registered_at: Option<i64> = self
            .conn
            .query_row(
                "SELECT registered_at FROM workspaces WHERE workspace_id = ?1",
                rusqlite::params![workspace_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        let now_i64 = now as i64;
        let registered_at = existing_registered_at.unwrap_or(now_i64);
        let status = classify_workspace_db(&graph_db_path).0;

        self.conn.execute(
            "INSERT INTO workspaces (
                workspace_id, workspace_root, graph_db_path, display_name, status, registered_at, last_seen_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(workspace_id) DO UPDATE SET
                workspace_root = excluded.workspace_root,
                graph_db_path = excluded.graph_db_path,
                display_name = COALESCE(excluded.display_name, workspaces.display_name),
                status = excluded.status,
                last_seen_at = excluded.last_seen_at",
            rusqlite::params![
                workspace_id,
                root.to_string_lossy().to_string(),
                graph_db_path.to_string_lossy().to_string(),
                display_name,
                status.as_str(),
                registered_at,
                now_i64
            ],
        )?;

        Ok(WorkspaceEntry {
            workspace_id,
            workspace_root: root,
            graph_db_path,
            display_name: display_name.map(str::to_string),
            status,
            registered_at: registered_at.max(0) as u64,
            last_seen_at: now,
            error: None,
        })
    }

    pub fn list_workspaces(&self) -> Result<Vec<WorkspaceEntry>> {
        let mut stmt = self.conn.prepare(
            "SELECT workspace_id, workspace_root, graph_db_path, display_name, status, registered_at, last_seen_at
             FROM workspaces
             ORDER BY COALESCE(display_name, workspace_root) COLLATE NOCASE ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })?;

        let mut entries = Vec::new();
        for row in rows {
            let (
                workspace_id,
                root,
                graph_db,
                display_name,
                stored_status,
                registered_at,
                last_seen_at,
            ) = row?;
            let graph_db_path = PathBuf::from(graph_db);
            let (status, error) = classify_workspace_db(&graph_db_path);
            let status = if matches!(status, WorkspaceStatus::InaccessibleDb) {
                WorkspaceStatus::from_str(&stored_status)
            } else {
                status
            };
            entries.push(WorkspaceEntry {
                workspace_id,
                workspace_root: PathBuf::from(root),
                graph_db_path,
                display_name,
                status,
                registered_at: registered_at.max(0) as u64,
                last_seen_at: last_seen_at.max(0) as u64,
                error,
            });
        }
        Ok(entries)
    }

    pub fn db_inventory(&self) -> Result<Vec<DbInventoryRow>> {
        self.list_workspaces()?
            .into_iter()
            .map(|entry| self.inventory_for_entry(entry))
            .collect()
    }

    pub fn global_lifetime_stats(&self) -> Result<GlobalLifetimeStats> {
        let inventory = self.db_inventory()?;
        let lifetime = Self::sum_lifetime_stats_from_inventory(&inventory);
        Ok(GlobalLifetimeStats {
            workspace_statuses: inventory,
            ..lifetime
        })
    }

    /// Accumulate lifetime stat counters from a pre-built inventory slice,
    /// using a single batched DB query per workspace instead of 8 sequential reads.
    fn sum_lifetime_stats_from_inventory(inventory: &[DbInventoryRow]) -> GlobalLifetimeStats {
        const STAT_KEYS: &[&str] = &[
            "total_requests",
            "total_tokens_saved",
            "total_file_tokens",
            "pipeline_requests",
            "direct_low_level_autorouted",
            "direct_low_level_rejected",
            "ambiguous_symbol_requests",
            "stale_capsule_prevented",
        ];

        let mut total_requests = 0i64;
        let mut total_tokens_saved = 0i64;
        let mut total_file_tokens = 0i64;
        let mut pipeline_requests = 0i64;
        let mut direct_low_level_autorouted = 0i64;
        let mut direct_low_level_rejected = 0i64;
        let mut ambiguous_symbol_requests = 0i64;
        let mut stale_capsule_prevented = 0i64;

        for row in inventory {
            if !matches!(
                row.status,
                WorkspaceStatus::Available | WorkspaceStatus::Empty
            ) {
                continue;
            }
            let Ok(conn) = open_graph_readonly(&row.graph_db_path) else {
                continue;
            };
            let batch = crate::db::read_stats_batch(&conn, STAT_KEYS);
            total_requests += batch.get("total_requests").copied().unwrap_or(0);
            total_tokens_saved += batch.get("total_tokens_saved").copied().unwrap_or(0);
            total_file_tokens += batch.get("total_file_tokens").copied().unwrap_or(0);
            pipeline_requests += batch.get("pipeline_requests").copied().unwrap_or(0);
            direct_low_level_autorouted += batch
                .get("direct_low_level_autorouted")
                .copied()
                .unwrap_or(0);
            direct_low_level_rejected +=
                batch.get("direct_low_level_rejected").copied().unwrap_or(0);
            ambiguous_symbol_requests +=
                batch.get("ambiguous_symbol_requests").copied().unwrap_or(0);
            stale_capsule_prevented += batch.get("stale_capsule_prevented").copied().unwrap_or(0);
        }

        let reduction_pct = if total_file_tokens == 0 {
            0.0
        } else {
            (total_tokens_saved as f64 / total_file_tokens as f64) * 100.0
        };
        let compliance_total =
            pipeline_requests + direct_low_level_autorouted + direct_low_level_rejected;
        let pipeline_compliance_pct = if compliance_total == 0 {
            0.0
        } else {
            (pipeline_requests as f64 / compliance_total as f64) * 100.0
        };

        GlobalLifetimeStats {
            total_requests,
            total_tokens_saved,
            total_file_tokens,
            pipeline_requests,
            direct_low_level_autorouted,
            direct_low_level_rejected,
            ambiguous_symbol_requests,
            stale_capsule_prevented,
            reduction_pct,
            pipeline_compliance_pct,
            workspace_statuses: Vec::new(),
        }
    }

    /// Returns aggregate lifetime stats and the workspace list in a single call,
    /// invoking `list_workspaces()` only once. Use this in performance-sensitive
    /// paths (e.g. `/stats` handler) in place of separate `global_lifetime_stats()`
    /// + `list_workspaces()` calls.
    ///
    /// Each eligible workspace graph DB is opened exactly once: scope inventory
    /// and stat batch reads share the same connection, avoiding the double-open
    /// that would occur when chaining `inventory_for_entry` → `sum_lifetime_stats_from_inventory`.
    pub fn stats_aggregate(&self) -> Result<StatsAggregate> {
        const STAT_KEYS: &[&str] = &[
            "total_requests",
            "total_tokens_saved",
            "total_file_tokens",
            "pipeline_requests",
            "direct_low_level_autorouted",
            "direct_low_level_rejected",
            "ambiguous_symbol_requests",
            "stale_capsule_prevented",
        ];

        let workspaces = self.list_workspaces()?;

        let mut total_requests = 0i64;
        let mut total_tokens_saved = 0i64;
        let mut total_file_tokens = 0i64;
        let mut pipeline_requests = 0i64;
        let mut direct_low_level_autorouted = 0i64;
        let mut direct_low_level_rejected = 0i64;
        let mut ambiguous_symbol_requests = 0i64;
        let mut stale_capsule_prevented = 0i64;

        let mut inventory = Vec::with_capacity(workspaces.len());

        for entry in &workspaces {
            let size_mb = std::fs::metadata(&entry.graph_db_path)
                .map(|meta| meta.len() as f64 / 1_048_576.0)
                .unwrap_or(0.0);
            // Reuse status/error already computed by list_workspaces(); no second
            // classify_workspace_db() call here.
            let mut row = DbInventoryRow {
                workspace_id: entry.workspace_id.clone(),
                workspace_root: entry.workspace_root.clone(),
                graph_db_path: entry.graph_db_path.clone(),
                display_name: entry.display_name.clone(),
                status: entry.status.clone(),
                size_mb,
                repo_count: 0,
                symbol_count: 0,
                file_count: 0,
                repos: Vec::new(),
                error: entry.error.clone(),
            };

            if matches!(
                row.status,
                WorkspaceStatus::Available | WorkspaceStatus::Empty
            ) {
                match open_graph_readonly(&row.graph_db_path) {
                    Ok(conn) => {
                        match crate::db::database_scope_snapshot(&conn) {
                            Ok(scope) => {
                                row.repo_count = scope.repo_count;
                                row.symbol_count = scope.symbol_count;
                                row.file_count = scope.file_count;
                                row.repos = scope.repos;
                                if row.symbol_count == 0 && row.file_count == 0 {
                                    row.status = WorkspaceStatus::Empty;
                                } else {
                                    row.status = WorkspaceStatus::Available;
                                }
                            }
                            Err(e) => {
                                let (status, _) = classify_error(&e.to_string());
                                row.status = status;
                                row.error = Some(e.to_string());
                            }
                        }
                        // Read stats on the same open connection — no second open.
                        if matches!(
                            row.status,
                            WorkspaceStatus::Available | WorkspaceStatus::Empty
                        ) {
                            let batch = crate::db::read_stats_batch(&conn, STAT_KEYS);
                            total_requests += batch.get("total_requests").copied().unwrap_or(0);
                            total_tokens_saved +=
                                batch.get("total_tokens_saved").copied().unwrap_or(0);
                            total_file_tokens +=
                                batch.get("total_file_tokens").copied().unwrap_or(0);
                            pipeline_requests +=
                                batch.get("pipeline_requests").copied().unwrap_or(0);
                            direct_low_level_autorouted += batch
                                .get("direct_low_level_autorouted")
                                .copied()
                                .unwrap_or(0);
                            direct_low_level_rejected +=
                                batch.get("direct_low_level_rejected").copied().unwrap_or(0);
                            ambiguous_symbol_requests +=
                                batch.get("ambiguous_symbol_requests").copied().unwrap_or(0);
                            stale_capsule_prevented +=
                                batch.get("stale_capsule_prevented").copied().unwrap_or(0);
                        }
                    }
                    Err(e) => {
                        let (status, error) = classify_error(&e.to_string());
                        row.status = status;
                        row.error = error;
                    }
                }
            }

            inventory.push(row);
        }

        let reduction_pct = if total_file_tokens == 0 {
            0.0
        } else {
            (total_tokens_saved as f64 / total_file_tokens as f64) * 100.0
        };
        let compliance_total =
            pipeline_requests + direct_low_level_autorouted + direct_low_level_rejected;
        let pipeline_compliance_pct = if compliance_total == 0 {
            0.0
        } else {
            (pipeline_requests as f64 / compliance_total as f64) * 100.0
        };

        let lifetime = GlobalLifetimeStats {
            total_requests,
            total_tokens_saved,
            total_file_tokens,
            pipeline_requests,
            direct_low_level_autorouted,
            direct_low_level_rejected,
            ambiguous_symbol_requests,
            stale_capsule_prevented,
            reduction_pct,
            pipeline_compliance_pct,
            workspace_statuses: inventory,
        };

        Ok(StatsAggregate {
            lifetime,
            workspaces,
        })
    }

    pub fn graph_snapshot(
        &self,
        workspace_id: &str,
        repo_id: Option<&str>,
        limit: usize,
    ) -> Result<GraphSnapshot> {
        let entry = self
            .find_workspace(workspace_id)?
            .ok_or_else(|| anyhow!("workspace '{workspace_id}' is not registered"))?;
        let (status, error) = classify_workspace_db(&entry.graph_db_path);
        if !matches!(status, WorkspaceStatus::Available | WorkspaceStatus::Empty) {
            return Ok(empty_graph_snapshot(
                entry.workspace_id,
                None,
                status,
                error,
            ));
        }
        let conn = match open_graph_readonly(&entry.graph_db_path) {
            Ok(conn) => conn,
            Err(e) => {
                let (status, error) = classify_error(&e.to_string());
                return Ok(empty_graph_snapshot(
                    entry.workspace_id,
                    None,
                    status,
                    error,
                ));
            }
        };
        let selected_repo = match repo_id {
            Some(repo) if !repo.is_empty() => Some(repo.to_string()),
            _ => match first_repo_id(&conn) {
                Ok(selected) => selected,
                Err(e) => {
                    let (status, error) = classify_error(&e.to_string());
                    return Ok(empty_graph_snapshot(
                        entry.workspace_id,
                        None,
                        status,
                        error,
                    ));
                }
            },
        };
        let Some(selected_repo_id) = selected_repo else {
            return Ok(empty_graph_snapshot(
                entry.workspace_id,
                None,
                WorkspaceStatus::Empty,
                None,
            ));
        };
        let cap = limit.max(1);
        let total_node_count: i64 = match conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
            rusqlite::params![selected_repo_id],
            |row| row.get(0),
        ) {
            Ok(count) => count,
            Err(e) => {
                let (status, error) = classify_error(&e.to_string());
                return Ok(empty_graph_snapshot(
                    entry.workspace_id,
                    Some(selected_repo_id),
                    status,
                    error,
                ));
            }
        };

        let nodes = match query_graph_nodes(&conn, &selected_repo_id, cap) {
            Ok(nodes) => nodes,
            Err(e) => {
                let (status, error) = classify_error(&e.to_string());
                return Ok(empty_graph_snapshot(
                    entry.workspace_id,
                    Some(selected_repo_id),
                    status,
                    error,
                ));
            }
        };
        let edges = match query_graph_edges(&conn, &selected_repo_id, cap) {
            Ok(edges) => edges,
            Err(e) => {
                let (status, error) = classify_error(&e.to_string());
                return Ok(empty_graph_snapshot(
                    entry.workspace_id,
                    Some(selected_repo_id),
                    status,
                    error,
                ));
            }
        };
        Ok(GraphSnapshot {
            truncated: total_node_count > cap as i64,
            total_node_count,
            nodes,
            edges,
            workspace_id: entry.workspace_id,
            repo_id: Some(selected_repo_id),
            status,
            error,
        })
    }

    pub fn cleanup_workspace(
        &self,
        workspace_id: &str,
        kind: CleanupKind,
        confirmed: bool,
    ) -> Result<()> {
        if !confirmed {
            return Err(anyhow!("cleanup action requires explicit confirmation"));
        }
        match kind {
            CleanupKind::Unregister => {
                let changed = self.conn.execute(
                    "DELETE FROM workspaces WHERE workspace_id = ?1",
                    rusqlite::params![workspace_id],
                )?;
                if changed == 0 {
                    return Err(anyhow!("workspace '{workspace_id}' is not registered"));
                }
                Ok(())
            }
            CleanupKind::ClearIndex => {
                let entry = self.validated_cleanup_entry(workspace_id, true)?;
                let conn = crate::db::init_db(&entry.graph_db_path.to_string_lossy())
                    .with_context(|| {
                        format!(
                            "opening registered graph DB {}",
                            entry.graph_db_path.display()
                        )
                    })?;
                crate::db::clear_index_contents(&conn)
            }
            CleanupKind::DeleteDb => {
                let entry = self.validated_cleanup_entry(workspace_id, true)?;
                std::fs::remove_file(&entry.graph_db_path).with_context(|| {
                    format!(
                        "deleting registered graph DB {}",
                        entry.graph_db_path.display()
                    )
                })?;
                self.conn.execute(
                    "UPDATE workspaces SET status = ?1 WHERE workspace_id = ?2",
                    rusqlite::params![WorkspaceStatus::MissingDb.as_str(), workspace_id],
                )?;
                Ok(())
            }
        }
    }

    pub fn find_workspace(&self, workspace_id: &str) -> Result<Option<WorkspaceEntry>> {
        let row = self
            .conn
            .query_row(
                "SELECT workspace_id, workspace_root, graph_db_path, display_name, status, registered_at, last_seen_at
                 FROM workspaces WHERE workspace_id = ?1",
                rusqlite::params![workspace_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .optional()?;
        let Some((workspace_id, root, graph_db, display_name, status, registered_at, last_seen_at)) =
            row
        else {
            return Ok(None);
        };
        Ok(Some(WorkspaceEntry {
            workspace_id,
            workspace_root: PathBuf::from(root),
            graph_db_path: PathBuf::from(graph_db),
            display_name,
            status: WorkspaceStatus::from_str(&status),
            registered_at: registered_at.max(0) as u64,
            last_seen_at: last_seen_at.max(0) as u64,
            error: None,
        }))
    }

    fn migrate(&self) -> Result<()> {
        let version: i64 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if version < REGISTRY_SCHEMA_VERSION {
            self.conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS workspaces (
                    workspace_id   TEXT PRIMARY KEY,
                    workspace_root TEXT NOT NULL UNIQUE,
                    graph_db_path  TEXT NOT NULL UNIQUE,
                    display_name   TEXT,
                    status         TEXT NOT NULL DEFAULT 'missing_db',
                    registered_at  INTEGER NOT NULL,
                    last_seen_at   INTEGER NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS idx_workspaces_last_seen ON workspaces(last_seen_at DESC);
                 PRAGMA user_version = 1;",
            )?;
        }
        Ok(())
    }

    fn inventory_for_entry(&self, entry: WorkspaceEntry) -> Result<DbInventoryRow> {
        let size_mb = std::fs::metadata(&entry.graph_db_path)
            .map(|meta| meta.len() as f64 / 1_048_576.0)
            .unwrap_or(0.0);
        let (status, error) = classify_workspace_db(&entry.graph_db_path);
        let mut row = DbInventoryRow {
            workspace_id: entry.workspace_id,
            workspace_root: entry.workspace_root,
            graph_db_path: entry.graph_db_path,
            display_name: entry.display_name,
            status,
            size_mb,
            repo_count: 0,
            symbol_count: 0,
            file_count: 0,
            repos: Vec::new(),
            error,
        };

        if matches!(
            row.status,
            WorkspaceStatus::Available | WorkspaceStatus::Empty
        ) {
            match open_graph_readonly(&row.graph_db_path)
                .and_then(|conn| crate::db::database_scope_snapshot(&conn))
            {
                Ok(scope) => {
                    row.repo_count = scope.repo_count;
                    row.symbol_count = scope.symbol_count;
                    row.file_count = scope.file_count;
                    row.repos = scope.repos;
                    if row.symbol_count == 0 && row.file_count == 0 {
                        row.status = WorkspaceStatus::Empty;
                    } else {
                        row.status = WorkspaceStatus::Available;
                    }
                }
                Err(e) => {
                    let (status, _) = classify_error(&e.to_string());
                    row.status = status;
                    row.error = Some(e.to_string());
                }
            }
        }
        Ok(row)
    }

    fn validated_cleanup_entry(
        &self,
        workspace_id: &str,
        must_exist: bool,
    ) -> Result<WorkspaceEntry> {
        let entry = self
            .find_workspace(workspace_id)?
            .ok_or_else(|| anyhow!("workspace '{workspace_id}' is not registered"))?;
        let root = canonicalize_existing_dir(&entry.workspace_root)?;
        let marrow_dir = root.join(".marrow");
        let canonical_marrow_dir = canonicalize_existing_dir(&marrow_dir)?;
        if !canonical_marrow_dir.starts_with(&root) {
            return Err(anyhow!(
                "registered Marrow directory escapes workspace root: {}",
                canonical_marrow_dir.display()
            ));
        }
        let expected = marrow_dir.join("graph.db");
        if entry.graph_db_path != expected {
            return Err(anyhow!(
                "cleanup target is not the registered Marrow graph DB path under {}",
                marrow_dir.display()
            ));
        }
        if entry
            .graph_db_path
            .file_name()
            .and_then(|name| name.to_str())
            != Some("graph.db")
        {
            return Err(anyhow!("cleanup target is not a Marrow graph.db file"));
        }
        if must_exist && !entry.graph_db_path.exists() {
            return Err(anyhow!(
                "registered graph DB is missing: {}",
                entry.graph_db_path.display()
            ));
        }
        if entry.graph_db_path.exists() {
            let canonical_db = entry.graph_db_path.canonicalize().with_context(|| {
                format!("canonicalizing graph DB {}", entry.graph_db_path.display())
            })?;
            let canonical_expected = expected.canonicalize()?;
            if !canonical_db.starts_with(&root)
                || !canonical_db.starts_with(&canonical_marrow_dir)
                || canonical_db != canonical_expected
            {
                return Err(anyhow!(
                    "cleanup target is not the registered Marrow graph DB path under {}",
                    canonical_marrow_dir.display()
                ));
            }
        }
        Ok(entry)
    }
}

pub fn default_registry_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("/tmp"))
        .join(".marrow")
        .join("registry.db")
}

pub fn register_workspace_best_effort(workspace_root: &Path) -> Option<WorkspaceEntry> {
    Registry::open_default()
        .ok()
        .and_then(|registry| registry.register_workspace(workspace_root, None).ok())
}

pub fn workspace_id_for_root(root: &Path) -> String {
    let root_str = root.to_string_lossy();
    let base = root
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("workspace");
    let clean_base: String = base
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let hash = fnv_hex(root_str.as_bytes());
    format!("{}-{}", clean_base, &hash[..8])
}

fn canonicalize_existing_dir(path: &Path) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalizing workspace path {}", path.display()))?;
    if !canonical.is_dir() {
        return Err(anyhow!(
            "workspace path is not a directory: {}",
            canonical.display()
        ));
    }
    Ok(canonical)
}

fn classify_workspace_db(graph_db_path: &Path) -> (WorkspaceStatus, Option<String>) {
    if !graph_db_path.exists() {
        return (WorkspaceStatus::MissingDb, None);
    }
    match open_graph_readonly(graph_db_path) {
        Ok(conn) => match graph_counts(&conn) {
            Ok((symbols, files)) if symbols == 0 && files == 0 => (WorkspaceStatus::Empty, None),
            Ok(_) => (WorkspaceStatus::Available, None),
            Err(e) => classify_error(&e.to_string()),
        },
        Err(e) => classify_error(&e.to_string()),
    }
}

fn classify_error(message: &str) -> (WorkspaceStatus, Option<String>) {
    let lower = message.to_lowercase();
    let status = if lower.contains("locked") || lower.contains("sqlite_busy") {
        WorkspaceStatus::Locked
    } else if lower.contains("malformed")
        || lower.contains("not a database")
        || lower.contains("file is not a database")
    {
        WorkspaceStatus::Corrupt
    } else {
        WorkspaceStatus::InaccessibleDb
    };
    (status, Some(message.to_string()))
}

fn open_graph_readonly(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    conn.execute_batch("PRAGMA busy_timeout=1000;")?;
    Ok(conn)
}

fn graph_counts(conn: &Connection) -> Result<(i64, i64)> {
    let symbols = conn.query_row("SELECT COUNT(*) FROM nodes", [], |row| row.get(0))?;
    let files = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;
    Ok((symbols, files))
}

fn first_repo_id(conn: &Connection) -> Result<Option<String>> {
    conn.query_row(
        "SELECT id FROM repositories
         WHERE EXISTS (SELECT 1 FROM nodes WHERE repo_id = repositories.id)
            OR EXISTS (SELECT 1 FROM files WHERE repo_id = repositories.id)
         ORDER BY id ASC LIMIT 1",
        [],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

fn query_graph_nodes(conn: &Connection, repo_id: &str, limit: usize) -> Result<Vec<GraphNodeDto>> {
    let mut stmt = conn.prepare(
        "WITH valid_edges AS (
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
         SELECT n.id, n.symbol_name, n.file_path, COALESCE(n.symbol_type, 'unknown'), COALESCE(dc.degree, 0)
         FROM nodes n
         LEFT JOIN degree_counts dc ON dc.id = n.id
         WHERE n.repo_id = ?1
         ORDER BY COALESCE(dc.degree, 0) DESC, n.file_path ASC, n.symbol_name ASC, n.id ASC
         LIMIT ?2",
    )?;
    let rows = stmt.query_map(rusqlite::params![repo_id, limit as i64], |row| {
        Ok(GraphNodeDto {
            id: row.get(0)?,
            label: row.get(1)?,
            file_path: row.get(2)?,
            symbol_type: row.get(3)?,
            degree: row.get(4)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn query_graph_edges(conn: &Connection, repo_id: &str, limit: usize) -> Result<Vec<GraphEdgeDto>> {
    let mut stmt = conn.prepare(
        "WITH valid_edges AS (
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
         ),
         top_nodes AS (
            SELECT n.id
            FROM nodes n
            LEFT JOIN degree_counts dc ON dc.id = n.id
            WHERE n.repo_id = ?1
            ORDER BY COALESCE(dc.degree, 0) DESC, n.file_path ASC, n.symbol_name ASC, n.id ASC
            LIMIT ?2
         )
         SELECT e.source_id, e.target_id, COALESCE(e.relationship_type, 'CALLS')
         FROM edges e
         JOIN top_nodes src ON src.id = e.source_id
         JOIN top_nodes tgt ON tgt.id = e.target_id
         ORDER BY e.source_id ASC, e.target_id ASC, e.relationship_type ASC",
    )?;
    let rows = stmt.query_map(rusqlite::params![repo_id, limit as i64], |row| {
        Ok(GraphEdgeDto {
            source: row.get(0)?,
            target: row.get(1)?,
            relationship: row.get(2)?,
        })
    })?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn fnv_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = 14_695_981_039_346_656_037;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1_099_511_628_211);
    }
    format!("{hash:016x}")
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub fn default_graph_limit() -> usize {
    GRAPH_INITIAL_NODE_CAP_DEFAULT
}

fn empty_graph_snapshot(
    workspace_id: String,
    repo_id: Option<String>,
    status: WorkspaceStatus,
    error: Option<String>,
) -> GraphSnapshot {
    GraphSnapshot {
        nodes: Vec::new(),
        edges: Vec::new(),
        truncated: false,
        total_node_count: 0,
        workspace_id,
        repo_id,
        status,
        error,
    }
}
