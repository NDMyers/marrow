use anyhow::{anyhow, Result};
use rusqlite::Connection;
use std::{collections::{BTreeMap, HashSet}, fmt::Write as FmtWrite, fs, path::{Path, PathBuf}};
use streaming_iterator::StreamingIterator;
use tree_sitter::{Language, Parser, Query, QueryCursor};

// ── Public types ──────────────────────────────────────────────────────────────

/// Returned by `get_context_capsule`: both strings are derived from a single
/// graph traversal, ensuring telemetry and the compare endpoint use identical
/// source data.
#[derive(Debug)]
pub struct CapsuleResult {
    /// The condensed capsule text sent to the LLM (optimized).
    pub optimized_text: String,
    /// Concatenated raw file contents of every file touched by the capsule.
    pub original_text: String,
}

#[derive(Debug)]
pub struct ContextCapsule {
    pub pivot: NodeInfo,
    pub neighbors: Vec<NeighborInfo>,
}

#[derive(Debug)]
pub struct NodeInfo {
    #[allow(dead_code)]
    pub id: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub file_path: String,
    pub language: String,
    /// Full source for the pivot; condensed body for neighbors.
    pub text: String,
}

/// Direction of an edge relative to the pivot node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeDirection {
    /// Pivot calls / imports / implements the neighbor (outbound).
    Outbound,
    /// Neighbor calls / imports / implements the pivot (inbound).
    Inbound,
}

#[derive(Debug)]
pub struct NeighborInfo {
    pub node: NodeInfo,
    /// The edge label: CALLS, IMPORTS, IMPLEMENTS, etc.
    pub relationship: String,
    /// Whether the pivot is the source (Outbound) or target (Inbound) of this edge.
    pub direction: EdgeDirection,
}

#[derive(Debug)]
pub struct ImpactNode {
    #[allow(dead_code)]
    pub id: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub file_path: String,
    pub repo_id: String,
    /// The edge type that makes this node depend on its parent in the chain.
    pub relationship_type: String,
    pub depth: i64,
}

#[derive(Debug)]
pub struct ImpactResult {
    pub pivot_id: String,
    pub affected: Vec<ImpactNode>,
    /// True when results hit `MARROW_IMPACT_MAX_ROWS` (there may be more dependents).
    pub truncated: bool,
}

type NodeRow = (String, String, String, String, String, String);

/// Maximum number of inbound callers to show before truncating in formatted output.
const MAX_INBOUND_CALLERS: usize = 10;

fn env_usize_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Max outbound neighbors loaded into a context capsule / trace (bounds RAM).
fn capsule_max_outbound_neighbors() -> usize {
    env_usize_positive("MARROW_CAPSULE_MAX_OUTBOUND", 500)
}

/// Max inbound rows fetched from SQLite (display still capped at [`MAX_INBOUND_CALLERS`]).
fn capsule_max_inbound_neighbors_load() -> usize {
    env_usize_positive("MARROW_CAPSULE_MAX_INBOUND_LOAD", 64).max(MAX_INBOUND_CALLERS)
}

/// Max rows returned by `analyze_impact` (breadth × depth cap).
pub fn impact_max_rows() -> usize {
    env_usize_positive("MARROW_IMPACT_MAX_ROWS", 5000)
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch the pivot node's full source and all depth-1 neighbors condensed.
/// Returns a `CapsuleResult` with both the optimized capsule text and the
/// concatenated raw file contents of every file the capsule touches — ensuring
/// telemetry and the compare endpoint share a single source of truth.
///
/// When `symbol_name` is ambiguous (matches multiple files), returns `Ok` with
/// a Disambiguation Payload the agent can use to retry with a specific filepath.
pub fn get_context_capsule(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> Result<CapsuleResult> {
    // Resolve or get a structured disambiguation payload.
    let (pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw) =
        match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
            SymbolResolution::Unique(row) => row,
            SymbolResolution::Ambiguous(payload) => {
                // Surface as a successful capsule — agents should parse and retry.
                return Ok(CapsuleResult {
                    optimized_text: payload.clone(),
                    original_text: payload,
                });
            }
        };

    let capsule = build_context_capsule_from_resolved(
        conn, pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw,
    )?;
    let optimized_text = format_capsule(&capsule);

    // Collect all unique file paths touched by this capsule.
    let mut touched: HashSet<String> = HashSet::new();
    touched.insert(capsule.pivot.file_path.clone());
    for n in &capsule.neighbors {
        touched.insert(n.node.file_path.clone());
    }

    // Read raw file contents from disk, sorted for deterministic output.
    let root_path: String = conn
        .query_row(
            "SELECT root_path FROM repositories WHERE id = ?1",
            rusqlite::params![repo_id],
            |row| row.get(0),
        )
        .unwrap_or_default();

    let root = if root_path.is_empty() {
        None
    } else {
        Some(PathBuf::from(&root_path))
    };

    let mut parts = Vec::new();
    for rel_path in &touched {
        let Some(root) = root.as_ref() else {
            continue;
        };
        let abs_path = match resolve_repo_file_path(root, rel_path) {
            Ok(path) => path,
            Err(_) => {
                // File was deleted or moved after ingestion — skip it.
                continue;
            }
        };
        match fs::read_to_string(&abs_path) {
            Ok(text) => parts.push(text),
            Err(_) => {
                // File unreadable (permissions, encoding) — skip it.
                continue;
            }
        }
    }
    parts.sort();
    let original_text = parts.join("\n");

    // Append any stored observations for the pivot symbol.
    let observations = query_observations_for_capsule(
        conn,
        repo_id,
        &capsule.pivot.symbol_name,
        &capsule.pivot.file_path,
    );
    let optimized_text = if observations.is_empty() {
        optimized_text
    } else {
        let mut out = optimized_text;
        out.push_str("\n── SESSION MEMORIES ─────────────────────────────────────────\n");
        for (text, is_stale, ts) in observations {
            if is_stale {
                out.push_str(&format!(
                    "[STALE MEMORY WARNING: The underlying code has changed since this was recorded. \
                     Re-verify before trusting.]\n{text}\n  (recorded: {ts})\n\n"
                ));
            } else {
                out.push_str(&format!("{text}\n  (recorded: {ts})\n\n"));
            }
        }
        out
    };

    Ok(CapsuleResult { optimized_text, original_text })
}



/// Core builder: builds a `ContextCapsule` from a pre-resolved pivot row.
/// Shared by `get_context_capsule` and `trace_logic_flow`.
fn build_context_capsule_from_resolved(
    conn: &Connection,
    pivot_id: String,
    pivot_name: String,
    pivot_type: String,
    pivot_path: String,
    pivot_lang: String,
    pivot_raw: String,
) -> Result<ContextCapsule> {
    let pivot = NodeInfo {
        id: pivot_id.clone(),
        symbol_name: pivot_name,
        symbol_type: pivot_type,
        file_path: pivot_path,
        language: pivot_lang,
        text: pivot_raw,
    };

    let out_lim = capsule_max_outbound_neighbors() as i64;
    let in_lim = capsule_max_inbound_neighbors_load() as i64;

    // ── Outbound edges: pivot → targets (things this symbol calls/imports) ──
    let mut outbound_stmt = conn.prepare(
        "SELECT n.id, n.symbol_name, n.symbol_type, n.file_path, n.language,
                n.raw_text, e.relationship_type
         FROM edges e
         JOIN nodes n ON e.source_id = ?1 AND n.id = e.target_id
         WHERE n.id != ?1
         ORDER BY n.symbol_name, n.file_path
         LIMIT ?2",
    )?;
    let outbound_rows: Vec<(String, String, String, String, String, String, String)> = outbound_stmt
        .query_map(rusqlite::params![pivot_id, out_lim], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // ── Inbound edges: sources → pivot (things that call this symbol) ──
    let mut inbound_stmt = conn.prepare(
        "SELECT n.id, n.symbol_name, n.symbol_type, n.file_path, n.language,
                n.raw_text, e.relationship_type
         FROM edges e
         JOIN nodes n ON e.target_id = ?1 AND n.id = e.source_id
         WHERE n.id != ?1
         ORDER BY n.symbol_name, n.file_path
         LIMIT ?2",
    )?;
    let inbound_rows: Vec<(String, String, String, String, String, String, String)> = inbound_stmt
        .query_map(rusqlite::params![pivot_id, in_lim], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let mut neighbors: Vec<NeighborInfo> = Vec::new();

    for (id, sym_name, sym_type, file_path, lang, raw_text, rel_type) in outbound_rows {
        neighbors.push(NeighborInfo {
            node: NodeInfo {
                id,
                symbol_name: sym_name,
                symbol_type: sym_type,
                file_path,
                language: lang.clone(),
                text: condense(&raw_text, &lang),
            },
            relationship: rel_type,
            direction: EdgeDirection::Outbound,
        });
    }

    for (id, sym_name, sym_type, file_path, lang, raw_text, rel_type) in inbound_rows {
        neighbors.push(NeighborInfo {
            node: NodeInfo {
                id,
                symbol_name: sym_name,
                symbol_type: sym_type,
                file_path,
                language: lang.clone(),
                text: condense(&raw_text, &lang),
            },
            relationship: rel_type,
            direction: EdgeDirection::Inbound,
        });
    }

    Ok(ContextCapsule { pivot, neighbors })
}

/// Format a `ContextCapsule` into the plain-text string sent to the LLM.
///
/// Outbound dependencies are listed first (bounded by `MARROW_CAPSULE_MAX_OUTBOUND` at query time).
/// Inbound callers are capped at `MAX_INBOUND_CALLERS` in this output.
fn format_capsule(capsule: &ContextCapsule) -> String {
    let mut out = String::new();
    writeln!(
        out,
        "CONTEXT CAPSULE — pivot: {} ({})",
        capsule.pivot.symbol_name, capsule.pivot.language
    ).ok();
    writeln!(out, "File : {}", capsule.pivot.file_path).ok();
    writeln!(out, "Type : {}", capsule.pivot.symbol_type).ok();
    writeln!(out, "\n── FULL SOURCE ──────────────────────────────────────────────").ok();
    writeln!(out, "{}", capsule.pivot.text).ok();

    let outbound: Vec<&NeighborInfo> = capsule.neighbors.iter()
        .filter(|n| n.direction == EdgeDirection::Outbound)
        .collect();
    let inbound: Vec<&NeighborInfo> = capsule.neighbors.iter()
        .filter(|n| n.direction == EdgeDirection::Inbound)
        .collect();

    if outbound.is_empty() && inbound.is_empty() {
        writeln!(out, "── NEIGHBORS ────────────────────────────────────────────────").ok();
        writeln!(out, "  (none — isolated symbol)").ok();
        return out;
    }

    // ── Outbound: things this symbol calls/imports (signatures only) ─────────
    if !outbound.is_empty() {
        writeln!(out, "\n── OUTBOUND DEPENDENCIES (signatures only — use read_node to expand) ──").ok();
        for n in &outbound {
            // Progressive disclosure: show only the first non-empty line (signature).
            // Full bodies are available via `run_pipeline` with `intent: "read_node"`.
            let signature = n.node.text
                .lines()
                .find(|l| !l.trim().is_empty())
                .unwrap_or(&n.node.symbol_name);
            writeln!(
                out,
                "\n  [{rel}]  {name}  ({lang})  {path}\n  {sig}",
                rel  = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
                sig  = signature,
            ).ok();
        }
    }

    let out_cap = capsule_max_outbound_neighbors();
    if outbound.len() >= out_cap {
        writeln!(
            out,
            "\n[Note: at most {out_cap} outbound neighbors loaded; set MARROW_CAPSULE_MAX_OUTBOUND to raise.]"
        )
        .ok();
    }

    // ── Inbound: things that call this symbol (capped) ───────────────────────
    if !inbound.is_empty() {
        writeln!(out, "\n── INBOUND CALLERS (who calls this) ─────────────────────────").ok();
        let shown = inbound.len().min(MAX_INBOUND_CALLERS);
        let omitted = inbound.len().saturating_sub(MAX_INBOUND_CALLERS);
        for n in &inbound[..shown] {
            writeln!(
                out,
                "  [{rel}]  {name}  ({lang})  {path}",
                rel  = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            ).ok();
        }
        if omitted > 0 {
            writeln!(out, "  [... and {omitted} more callers omitted for brevity]").ok();
        }
    }

    // ── Progressive Disclosure CTA ────────────────────────────────────────────
    writeln!(out, "\n─────────────────────────────────────────────────────────────────").ok();
    writeln!(out, "MARROW PROGRESSIVE DISCLOSURE: The neighbor code bodies above are").ok();
    writeln!(out, "intentionally condensed to signatures. To read the full source of").ok();
    writeln!(out, "any neighbor, do NOT use native file-reading tools. Instead, call:").ok();
    writeln!(out, "  run_pipeline(intent: \"read_node\", target: \"<SymbolName>\")").ok();
    writeln!(out, "─────────────────────────────────────────────────────────────────").ok();

    out
}

/// Recursively find every node that (transitively) depends on the pivot.
/// Uses a WITH RECURSIVE CTE walking edges backwards (source → pivot direction).
///
/// When `symbol_name` matches multiple files, returns `Ok` with a special
/// `ImpactResult` where `pivot_id` begins with `"DISAMBIGUATION:"` and
/// `affected` is empty. The caller must surface `pivot_id` to the agent.
pub fn analyze_impact(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> Result<ImpactResult> {
    let pivot_id = match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
        SymbolResolution::Unique(row) => row.0,
        SymbolResolution::Ambiguous(payload) => {
            return Ok(ImpactResult {
                pivot_id: format!("DISAMBIGUATION:{payload}"),
                affected: vec![],
                truncated: false,
            });
        }
    };

    // Recursive CTE: start at pivot, follow edges backwards (who calls me?).
    // `ranked` de-duplicates via ROW_NUMBER so each node appears only once
    // at its minimum depth, preserving the relationship type for that hop.
    let mut stmt = conn.prepare(
        "WITH RECURSIVE impact(node_id, rel_type, depth) AS (
             SELECT ?1, '', 0
             UNION ALL
             SELECT e.source_id, e.relationship_type, impact.depth + 1
             FROM edges e
             JOIN impact ON e.target_id = impact.node_id
             WHERE impact.depth < 10
         ),
         ranked AS (
             SELECT node_id, rel_type, depth,
                    ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY depth) AS rn
             FROM impact
             WHERE node_id != ?1
         )
         SELECT n.id, n.symbol_name, n.symbol_type, n.file_path, n.repo_id,
                r.rel_type, r.depth
         FROM ranked r
         JOIN nodes n ON n.id = r.node_id
         WHERE r.rn = 1
         ORDER BY r.depth
         LIMIT ?2",
    )?;

    let lim = impact_max_rows() as i64;
    let affected: Vec<ImpactNode> = stmt
        .query_map(rusqlite::params![pivot_id, lim], |row| {
            Ok(ImpactNode {
                id: row.get(0)?,
                symbol_name: row.get(1)?,
                symbol_type: row.get(2)?,
                file_path: row.get(3)?,
                repo_id: row.get(4)?,
                relationship_type: row.get(5)?,
                depth: row.get(6)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    let max_r = impact_max_rows();
    let truncated = affected.len() >= max_r;

    Ok(ImpactResult {
        pivot_id,
        affected,
        truncated,
    })
}

/// Trace the linear execution flow outward from a pivot symbol.
///
/// Unlike `get_context_capsule`, this function returns **only outbound edges**:
/// the exact code body of the pivot plus condensed signatures of the functions
/// it directly calls. Inbound callers and sibling nodes are excluded.
///
/// This is the AST-accurate replacement for `grep -> read_file` when an agent
/// needs to trace a specific execution path without the noise of the full
/// caller graph. Use the `trace_flow` pipeline intent to reach this function.
///
/// Returns a `CapsuleResult` for telemetry consistency with `get_context_capsule`.
pub fn trace_logic_flow(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> Result<CapsuleResult> {
    // Resolve the pivot, returning a disambiguation payload on ambiguity.
    let (pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw) =
        match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
            SymbolResolution::Unique(row) => row,
            SymbolResolution::Ambiguous(payload) => {
                return Ok(CapsuleResult {
                    optimized_text: payload.clone(),
                    original_text: payload,
                });
            }
        };

    let pivot = NodeInfo {
        id: pivot_id.clone(),
        symbol_name: pivot_name,
        symbol_type: pivot_type,
        file_path: pivot_path,
        language: pivot_lang,
        text: pivot_raw,
    };

    let out_lim = capsule_max_outbound_neighbors() as i64;
    // Query only outbound edges (what this symbol calls / imports).
    let mut stmt = conn.prepare(
        "SELECT n.id, n.symbol_name, n.symbol_type, n.file_path, n.language,
                n.raw_text, e.relationship_type
         FROM edges e
         JOIN nodes n ON e.source_id = ?1 AND n.id = e.target_id
         WHERE n.id != ?1
         ORDER BY n.symbol_name, n.file_path
         LIMIT ?2",
    )?;
    let outbound: Vec<NeighborInfo> = stmt
        .query_map(rusqlite::params![pivot_id, out_lim], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .map(|(id, sym_name, sym_type, file_path, lang, raw_text, rel_type)| NeighborInfo {
            node: NodeInfo {
                id,
                symbol_name: sym_name,
                symbol_type: sym_type,
                file_path,
                language: lang.clone(),
                text: condense(&raw_text, &lang),
            },
            relationship: rel_type,
            direction: EdgeDirection::Outbound,
        })
        .collect();

    // Format the trace output: full pivot source + outbound-only signatures.
    let mut out = String::new();
    writeln!(
        out,
        "TRACE FLOW — pivot: {} ({})",
        pivot.symbol_name, pivot.language
    ).ok();
    writeln!(out, "File : {}", pivot.file_path).ok();
    writeln!(out, "Type : {}", pivot.symbol_type).ok();
    writeln!(out, "\n── FULL SOURCE ──────────────────────────────────────────────").ok();
    writeln!(out, "{}", pivot.text).ok();

    if outbound.is_empty() {
        writeln!(out, "── DIRECT CALLEES ─────────────────────────────────────────────").ok();
        writeln!(out, "  (leaf node — no direct outbound calls)").ok();
    } else {
        writeln!(out, "\n── DIRECT CALLEES (immediate outbound dependencies) ─────────────").ok();
        if outbound.len() >= capsule_max_outbound_neighbors() {
            writeln!(
                out,
                "[Note: at most {} outbound callees loaded; set MARROW_CAPSULE_MAX_OUTBOUND to raise.]\n",
                capsule_max_outbound_neighbors()
            )
            .ok();
        }
        for n in &outbound {
            writeln!(
                out,
                "  [{rel}]  {name}  ({lang})  {path}",
                rel  = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            ).ok();
            writeln!(out, "{}", n.node.text).ok();
        }
    }

    Ok(CapsuleResult {
        optimized_text: out.clone(),
        original_text: out,
    })
}

// ── Observations ──────────────────────────────────────────────────────────

/// Query stored observations for a pivot symbol+filepath.
/// Returns `(observation_text, is_stale, timestamp)` tuples, newest first.
fn query_observations_for_capsule(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: &str,
) -> Vec<(String, bool, String)> {
    let Ok(mut stmt) = conn.prepare(
        "SELECT observation_text, is_stale, timestamp
         FROM observations
         WHERE repo_id = ?1 AND symbol_name = ?2 AND filepath = ?3
         ORDER BY timestamp DESC",
    ) else {
        return vec![];
    };
    stmt.query_map(rusqlite::params![repo_id, symbol_name, filepath], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)? != 0,
            row.get::<_, String>(2)?,
        ))
    })
    .map(|rows| rows.filter_map(|r| r.ok()).collect())
    .unwrap_or_default()
}

/// Maximum number of disambiguation candidates to surface to the agent.
const MAX_DISAMBIGUATION_ITEMS: usize = 20;

/// Result of an attempted symbol resolution.
///
/// `Unique` carries the resolved row; `Ambiguous` carries a pre-formatted
/// markdown payload the caller should return directly to the agent.
pub(crate) enum SymbolResolution {
    Unique(NodeRow),
    /// A token-efficient markdown payload listing candidate filepaths.
    Ambiguous(String),
}

/// Resolve a symbol to a single row, or produce a disambiguation payload.
///
/// Callers that receive `Ambiguous` should surface the payload string
/// directly as a successful tool result — **not** as an error — so agents
/// can parse the list and retry with the specific filepaths.
fn resolve_symbol_or_disambiguate(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> Result<SymbolResolution> {
    let candidates: Vec<NodeRow> = if let Some(fp) = filepath {
        conn.prepare(
            "SELECT id, symbol_name, symbol_type, file_path, language, raw_text
             FROM nodes
             WHERE symbol_name = ?1 AND repo_id = ?2 AND file_path = ?3
             ORDER BY file_path ASC, id ASC",
        )?
        .query_map(rusqlite::params![symbol_name, repo_id, fp], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect()
    } else {
        conn.prepare(
            "SELECT id, symbol_name, symbol_type, file_path, language, raw_text
             FROM nodes
             WHERE symbol_name = ?1 AND repo_id = ?2
             ORDER BY file_path ASC, id ASC",
        )?
        .query_map(rusqlite::params![symbol_name, repo_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect()
    };

    match candidates.len() {
        0 => Err(anyhow!("Symbol '{}' not found in repo '{}'", symbol_name, repo_id)),
        1 => Ok(SymbolResolution::Unique(
            candidates.into_iter().next().expect("single candidate must exist"),
        )),
        n => {
            let _ = crate::db::increment_stat(conn, "ambiguous_symbol_requests", 1);
            let total = n;
            let shown = candidates.len().min(MAX_DISAMBIGUATION_ITEMS);
            let omitted = total.saturating_sub(MAX_DISAMBIGUATION_ITEMS);

            let mut payload = format!(
                "Found {total} matches for '{symbol_name}'. \
                 Please call run_pipeline again with the same intent and target, \
                 but add the specific `filepath` from the list below:\n"
            );
            for (_, sym_name, sym_type, file_path, _, _) in candidates.iter().take(shown) {
                payload.push_str(&format!("- {file_path} ({sym_type}: {sym_name})\n"));
            }
            if omitted > 0 {
                payload.push_str(&format!(
                    "[... and {omitted} more matches omitted. Narrow the search with a filepath.]"
                ));
            }

            Ok(SymbolResolution::Ambiguous(payload))
        }
    }
}



fn resolve_repo_file_path(root_path: &Path, rel_path: &str) -> Result<PathBuf> {
    let rel = PathBuf::from(rel_path);
    if rel.is_absolute() {
        return Err(anyhow!(
            "Indexed file '{}' is outside the repository root and cannot be trusted.",
            rel_path
        ));
    }

    let root = root_path.canonicalize().unwrap_or_else(|_| root_path.to_path_buf());
    let candidate = root.join(&rel);
    let canonical = candidate.canonicalize().map_err(|e| {
        anyhow!("Indexed file '{}' is missing on disk: {}", rel_path, e)
    })?;
    if !canonical.starts_with(&root) {
        return Err(anyhow!(
            "Indexed file '{}' resolves outside the repository root and cannot be trusted.",
            rel_path
        ));
    }
    Ok(canonical)
}

// ── Condensation ───────────────────────────────────────────────────────────

/// Condense `raw_text` for `lang`, replacing the body with a placeholder.
/// Returns the original text unchanged if no body block is detected
/// (e.g., forward declarations, macro-defined structs, incomplete fragments).
pub fn condense(raw_text: &str, lang: &str) -> String {
    match lang {
        "cpp" | "cc" | "cxx" | "h" | "hpp" => condense_braces(
            raw_text,
            tree_sitter_cpp::LANGUAGE.into(),
            // compound_statement = function body  |  field_declaration_list = class body
            "[(compound_statement) @body (field_declaration_list) @body]",
        ),
        "py" => condense_python(raw_text),
        "ts" => condense_braces(
            raw_text,
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "[(statement_block) @body (class_body) @body]",
        ),
        "tsx" => condense_braces(
            raw_text,
            tree_sitter_typescript::LANGUAGE_TSX.into(),
            "[(statement_block) @body (class_body) @body]",
        ),
        "rs" => condense_braces(
            raw_text,
            tree_sitter_rust::LANGUAGE.into(),
            concat!(
                "[(block) @body",
                " (field_declaration_list) @body",
                " (declaration_list) @body",
                " (enum_variant_list) @body]"
            ),
        ),
        "rb" => condense_ruby(raw_text),
        _ => raw_text.to_string(),
    }
}

/// Replace the outermost `{…}` body node with `{ /* ... */ }`.
fn condense_braces(raw_text: &str, lang: Language, query_src: &str) -> String {
    match find_outermost_body(raw_text, lang, query_src) {
        Some((start, end)) => {
            format!("{}{{ /* ... */ }}{}", &raw_text[..start], &raw_text[end..])
        }
        // No body found: forward decl, macro-generated class, or parse failure.
        None => raw_text.to_string(),
    }
}

/// Replace the outermost Python `block` with an `    pass` placeholder,
/// inferring indentation from the block's first non-empty line.
fn condense_python(raw_text: &str) -> String {
    let lang: Language = tree_sitter_python::LANGUAGE.into();
    match find_outermost_body(raw_text, lang, "(block) @body") {
        Some((start, end)) => {
            let block_slice = &raw_text[start..end];
            let indent = block_slice
                .lines()
                .find(|l| !l.trim().is_empty())
                .map(|l| " ".repeat(l.len() - l.trim_start().len()))
                .unwrap_or_default();
            format!("{}{}pass{}", &raw_text[..start], indent, &raw_text[end..])
        }
        None => raw_text.to_string(),
    }
}

/// Replace the outermost Ruby body node (`body_statement` or `statements`) with
/// a `# ...` placeholder.  The closing `end` keyword lives outside the body
/// node in tree-sitter-ruby's grammar, so the byte-range replacement leaves it
/// intact, producing `def method_name(...)\n  # ...\nend` output.
fn condense_ruby(raw_text: &str) -> String {
    let lang: Language = tree_sitter_ruby::LANGUAGE.into();
    match find_outermost_body(raw_text, lang, "(body_statement) @body") {
        Some((start, end)) => {
            format!("{}  # ...{}", &raw_text[..start], &raw_text[end..])
        }
        None => raw_text.to_string(),
    }
}

/// Run a tree-sitter query on `raw_text` and return the byte range of the
/// outermost (earliest-start) captured body node.
///
/// Collecting byte ranges into a Vec before any string ops sidesteps the
/// borrow-checker conflict between the streaming iterator (which borrows
/// `cursor` and `tree`) and the subsequent `&raw_text[..]` slices.
fn find_outermost_body(
    raw_text: &str,
    lang: Language,
    query_src: &str,
) -> Option<(usize, usize)> {
    let mut parser = Parser::new();
    parser.set_language(&lang).ok()?;
    let tree = parser.parse(raw_text, None)?;
    let query = Query::new(&lang, query_src).ok()?;

    let source_bytes = raw_text.as_bytes();
    let mut cursor = QueryCursor::new();
    let mut matches = cursor.matches(&query, tree.root_node(), source_bytes);

    // Copy byte ranges out of the streaming iterator before dropping it.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    while let Some(m) = matches.next() {
        for cap in m.captures {
            ranges.push((cap.node.start_byte(), cap.node.end_byte()));
        }
    }

    // The outermost body has the smallest start byte; ties broken by largest span.
    ranges
        .into_iter()
        .min_by_key(|&(start, end)| (start, usize::MAX - end))
}

// ── Project Skeleton ──────────────────────────────────────────────────────────

const SKELETON_ROW_LIMIT: usize = 2000;

/// Return a token-efficient Markdown map of the repo's high-level symbols.
///
/// Only `function`, `class`, `struct`, `trait`, and `interface` nodes are
/// included — no variable declarations, imports, or raw text bodies.
/// If `target_dir` is provided, only nodes whose `file_path` starts with
/// that prefix are included.
pub fn get_project_skeleton(
    conn: &Connection,
    repo_id: &str,
    target_dir: Option<&str>,
) -> Result<String> {
    // Check whether there are any nodes at all.
    let total: i64 = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get(0),
    )?;
    if total == 0 {
        return Ok(
            format!(
                "No symbols found for repo '{}'. The repository has not been indexed yet.\n\
                 Run the `ingest_repo` tool to build the AST graph before using `get_skeleton`.",
                repo_id
            ),
        );
    }

    let base_sql = "SELECT file_path, symbol_type, symbol_name \
                    FROM nodes \
                    WHERE repo_id = ?1 \
                      AND (symbol_type LIKE '%function%' \
                        OR symbol_type LIKE '%class%' \
                        OR symbol_type LIKE '%struct%' \
                        OR symbol_type LIKE '%trait%' \
                        OR symbol_type LIKE '%interface%') \
                    ORDER BY file_path ASC, rowid ASC \
                    LIMIT ?2";

    let dir_sql = "SELECT file_path, symbol_type, symbol_name \
                   FROM nodes \
                   WHERE repo_id = ?1 \
                     AND (symbol_type LIKE '%function%' \
                       OR symbol_type LIKE '%class%' \
                       OR symbol_type LIKE '%struct%' \
                       OR symbol_type LIKE '%trait%' \
                       OR symbol_type LIKE '%interface%') \
                     AND (file_path = ?3 OR file_path LIKE ?4) \
                   ORDER BY file_path ASC, rowid ASC \
                   LIMIT ?2";

    let limit = SKELETON_ROW_LIMIT as i64;

    // Collect rows into a BTreeMap<file_path, Vec<(symbol_type, symbol_name)>>
    // so files appear in deterministic alphabetical order.
    let mut map: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut row_count: usize = 0;

    if let Some(dir) = target_dir {
        let exact = dir.trim_end_matches('/').to_string();
        let prefix = format!("{}/%", exact);
        let mut stmt = conn.prepare(dir_sql)?;
        let rows = stmt.query_map(rusqlite::params![repo_id, limit, exact, prefix], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        for row in rows.filter_map(|r| r.ok()) {
            map.entry(row.0).or_default().push((row.1, row.2));
            row_count += 1;
        }
    } else {
        let mut stmt = conn.prepare(base_sql)?;
        let rows = stmt.query_map(rusqlite::params![repo_id, limit], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        for row in rows.filter_map(|r| r.ok()) {
            map.entry(row.0).or_default().push((row.1, row.2));
            row_count += 1;
        }
    }

    if map.is_empty() {
        return Ok(
            "No matching symbols found for the given filter.\n\
             Try a different `target_dir` or check that the repo is indexed."
                .to_string(),
        );
    }

    let mut out = String::new();
    for (file_path, symbols) in &map {
        writeln!(out, "\u{1f4c1} {}", file_path).ok();
        for (sym_type, sym_name) in symbols {
            writeln!(out, "   - [{}] {}", sym_type, sym_name).ok();
        }
    }

    if row_count >= SKELETON_ROW_LIMIT {
        writeln!(
            out,
            "\n[WARNING: Repository map truncated for token safety. \
             Use get_context_capsule on specific files for deeper exploration.]"
        )
        .ok();
    }

    Ok(out)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::tempdir;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn make_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE repositories (id TEXT PRIMARY KEY, root_path TEXT NOT NULL);
             CREATE TABLE nodes (
                 id TEXT PRIMARY KEY, repo_id TEXT NOT NULL,
                 file_path TEXT NOT NULL, language TEXT NOT NULL,
                 symbol_name TEXT NOT NULL, symbol_type TEXT NOT NULL,
                 raw_text TEXT NOT NULL
             );
             CREATE TABLE edges (
                 source_id TEXT NOT NULL, target_id TEXT NOT NULL,
                 relationship_type TEXT NOT NULL,
                 PRIMARY KEY (source_id, target_id, relationship_type)
             );
             CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
             CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);",
        )
        .unwrap();
        conn
    }

    fn insert_node(
        conn: &Connection,
        id: &str,
        repo_id: &str,
        file_path: &str,
        lang: &str,
        name: &str,
        sym_type: &str,
        raw: &str,
    ) {
        conn.execute(
            "INSERT INTO nodes VALUES (?1,?2,?3,?4,?5,?6,?7)",
            rusqlite::params![id, repo_id, file_path, lang, name, sym_type, raw],
        )
        .unwrap();
    }

    fn insert_repo(conn: &Connection, repo_id: &str, root_path: &str) {
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, root_path],
        )
        .unwrap();
    }

    fn insert_edge(conn: &Connection, src: &str, tgt: &str, rel: &str) {
        conn.execute(
            "INSERT INTO edges VALUES (?1,?2,?3)",
            rusqlite::params![src, tgt, rel],
        )
        .unwrap();
    }

    // ── get_context_capsule ───────────────────────────────────────────────────

    #[test]
    fn capsule_pivot_has_full_text() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:foo",
            "r",
            "f.py",
            "py",
            "foo",
            "function",
            "def foo():\n    return 42\n",
        );
        let result = get_context_capsule(&conn, "foo", "r", None).unwrap();
        assert!(result.optimized_text.contains("foo"), "symbol name missing: {}", result.optimized_text);
        assert!(result.optimized_text.contains("def foo():\n    return 42\n"), "pivot text missing");
    }

    #[test]
    fn capsule_has_no_neighbors_when_isolated() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:solo",
            "r",
            "f.py",
            "py",
            "solo",
            "function",
            "def solo(): pass\n",
        );
        let result = get_context_capsule(&conn, "solo", "r", None).unwrap();
        assert!(
            result.optimized_text.contains("none — isolated symbol"),
            "isolated marker missing: {}", result.optimized_text
        );
    }

    #[test]
    fn capsule_python_neighbor_body_replaced_with_pass() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:caller",
            "r",
            "f.py",
            "py",
            "caller",
            "function",
            "def caller():\n    return bar()\n",
        );
        insert_node(
            &conn,
            "r:f.py:bar",
            "r",
            "f.py",
            "py",
            "bar",
            "function",
            "def bar():\n    x = 1\n    return x\n",
        );
        insert_edge(&conn, "r:f.py:caller", "r:f.py:bar", "CALLS");

        let result = get_context_capsule(&conn, "caller", "r", None).unwrap();
        let text = &result.optimized_text;
        // Progressive disclosure: neighbor bodies are not emitted — only the first line (signature).
        assert!(!text.contains("return x"), "neighbor body must not appear in progressive output, got: {text}");
        assert!(text.contains("def bar"), "neighbor signature must be preserved, got: {text}");
        assert!(text.contains("CALLS"), "relationship type must appear, got: {text}");
        assert!(text.contains("read_node"), "CTA for read_node must be present, got: {text}");
    }

    #[test]
    fn capsule_cpp_function_neighbor_body_replaced() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:w.cpp:main_fn",
            "r",
            "w.cpp",
            "cpp",
            "main_fn",
            "function",
            "int main_fn() {\n    return 0;\n}\n",
        );
        insert_node(
            &conn,
            "r:w.cpp:helper",
            "r",
            "w.cpp",
            "cpp",
            "helper",
            "function",
            "void helper(int x) {\n    x += 1;\n}\n",
        );
        insert_edge(&conn, "r:w.cpp:main_fn", "r:w.cpp:helper", "CALLS");

        let result = get_context_capsule(&conn, "main_fn", "r", None).unwrap();
        let text = &result.optimized_text;
        // Progressive disclosure: neighbor shows only first line (signature), no body stubs.
        assert!(!text.contains("x += 1"), "neighbor body must not appear in progressive output, got: {text}");
        assert!(text.contains("helper"), "neighbor name missing: {text}");
        assert!(text.contains("void helper"), "C++ neighbor signature must appear, got: {text}");
        assert!(text.contains("read_node"), "CTA for read_node must be present, got: {text}");
    }

    #[test]
    fn capsule_cpp_forward_decl_returns_full_text() {
        let conn = make_db();
        let fwd = "class Widget;";
        insert_node(&conn, "r:w.h:Widget", "r", "w.h", "cpp", "Widget", "class", fwd);
        insert_node(
            &conn,
            "r:w.cpp:processWidget",
            "r",
            "w.cpp",
            "cpp",
            "processWidget",
            "function",
            "void processWidget() {\n    Widget w;\n}\n",
        );
        insert_edge(&conn, "r:w.cpp:processWidget", "r:w.h:Widget", "IMPORTS");

        let result = get_context_capsule(&conn, "processWidget", "r", None).unwrap();
        assert!(
            result.optimized_text.contains(fwd),
            "forward declaration should appear verbatim in output: {}", result.optimized_text
        );
    }

    #[test]
    fn capsule_ts_function_neighbor_body_replaced() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:u.ts:entrypoint",
            "r",
            "u.ts",
            "ts",
            "entrypoint",
            "function",
            "function entrypoint(): void {\n    formatDate(new Date());\n}\n",
        );
        insert_node(
            &conn,
            "r:u.ts:formatDate",
            "r",
            "u.ts",
            "ts",
            "formatDate",
            "function",
            "function formatDate(date: Date): string {\n    return date.toISOString();\n}\n",
        );
        insert_edge(&conn, "r:u.ts:entrypoint", "r:u.ts:formatDate", "CALLS");

        let result = get_context_capsule(&conn, "entrypoint", "r", None).unwrap();
        let text = &result.optimized_text;
        assert!(text.contains("{ /* ... */ }"), "TS body should be replaced, got: {text}");
        assert!(!text.contains("toISOString"), "body leaked: {text}");
        assert!(text.contains("formatDate"), "neighbor name missing: {text}");
    }

    #[test]
    fn capsule_unknown_symbol_returns_error() {
        let conn = make_db();
        assert!(get_context_capsule(&conn, "ghost", "r", None).is_err());
    }

    #[test]
    fn capsule_duplicate_symbol_in_repo_returns_ambiguity_error() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.py:bulk_update",
            "r",
            "src/a.py",
            "py",
            "bulk_update",
            "function",
            "def bulk_update():\n    return 'a'\n",
        );
        insert_node(
            &conn,
            "r:src/b.py:bulk_update",
            "r",
            "src/b.py",
            "py",
            "bulk_update",
            "function",
            "def bulk_update():\n    return 'b'\n",
        );

        // Disambiguation is now returned as Ok(CapsuleResult) so agents can parse and retry.
        let result = get_context_capsule(&conn, "bulk_update", "r", None).unwrap();
        let msg = &result.optimized_text;
        assert!(msg.contains("Found 2 matches"), "expected disambiguation payload: {msg}");
        assert!(msg.contains("src/a.py"), "expected first candidate path: {msg}");
        assert!(msg.contains("src/b.py"), "expected second candidate path: {msg}");
        assert!(msg.contains("run_pipeline"), "payload must guide agent to retry: {msg}");
    }

    // ── analyze_impact ────────────────────────────────────────────────────────

    #[test]
    fn impact_empty_for_isolated_node() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:a.py:solo",
            "r",
            "a.py",
            "py",
            "solo",
            "function",
            "def solo(): pass\n",
        );
        let result = analyze_impact(&conn, "solo", "r", None).unwrap();
        assert!(result.affected.is_empty());
    }

    #[test]
    fn impact_finds_direct_caller_with_relationship_type() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:a.py:helper",
            "r",
            "a.py",
            "py",
            "helper",
            "function",
            "def helper(): pass\n",
        );
        insert_node(
            &conn,
            "r:a.py:caller",
            "r",
            "a.py",
            "py",
            "caller",
            "function",
            "def caller(): helper()\n",
        );
        insert_edge(&conn, "r:a.py:caller", "r:a.py:helper", "CALLS");

        let result = analyze_impact(&conn, "helper", "r", None).unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "caller");
        assert_eq!(result.affected[0].relationship_type, "CALLS");
        assert_eq!(result.affected[0].depth, 1);
    }

    #[test]
    fn impact_multi_hop_traversal_with_correct_depths() {
        let conn = make_db();
        insert_node(&conn, "r:a.py:base", "r", "a.py", "py", "base", "function", "def base(): pass\n");
        insert_node(&conn, "r:a.py:mid", "r", "a.py", "py", "mid", "function", "def mid(): base()\n");
        insert_node(&conn, "r:a.py:top", "r", "a.py", "py", "top", "function", "def top(): mid()\n");
        insert_edge(&conn, "r:a.py:mid", "r:a.py:base", "CALLS");
        insert_edge(&conn, "r:a.py:top", "r:a.py:mid", "CALLS");

        let result = analyze_impact(&conn, "base", "r", None).unwrap();
        assert_eq!(result.affected.len(), 2);
        let mid = result.affected.iter().find(|n| n.symbol_name == "mid").unwrap();
        let top = result.affected.iter().find(|n| n.symbol_name == "top").unwrap();
        assert_eq!(mid.depth, 1);
        assert_eq!(top.depth, 2);
    }

    #[test]
    fn impact_cross_repo_edge_relationship_preserved() {
        let conn = make_db();
        insert_node(
            &conn,
            "repo_b:lib.ts:ApiClient",
            "repo_b",
            "lib.ts",
            "ts",
            "ApiClient",
            "class",
            "class ApiClient {}\n",
        );
        insert_node(
            &conn,
            "repo_a:app.py:main",
            "repo_a",
            "app.py",
            "py",
            "main",
            "function",
            "def main(): pass\n",
        );
        insert_edge(&conn, "repo_a:app.py:main", "repo_b:lib.ts:ApiClient", "IMPORTS");

        let result = analyze_impact(&conn, "ApiClient", "repo_b", None).unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "main");
        assert_eq!(result.affected[0].relationship_type, "IMPORTS");
        assert_eq!(result.affected[0].repo_id, "repo_a");
    }

    #[test]
    fn impact_unknown_symbol_returns_error() {
        let conn = make_db();
        assert!(analyze_impact(&conn, "ghost", "r", None).is_err());
    }

    #[test]
    fn impact_duplicate_symbol_in_repo_returns_ambiguity_error() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.py:bulk_update",
            "r",
            "src/a.py",
            "py",
            "bulk_update",
            "function",
            "def bulk_update():\n    return 'a'\n",
        );
        insert_node(
            &conn,
            "r:src/b.py:bulk_update",
            "r",
            "src/b.py",
            "py",
            "bulk_update",
            "function",
            "def bulk_update():\n    return 'b'\n",
        );

        // Disambiguation is now returned as Ok(ImpactResult) with DISAMBIGUATION: prefix.
        let result = analyze_impact(&conn, "bulk_update", "r", None).unwrap();
        assert!(result.pivot_id.starts_with("DISAMBIGUATION:"), "expected disambig pivot_id: {}", result.pivot_id);
        let msg = &result.pivot_id["DISAMBIGUATION:".len()..];
        assert!(msg.contains("Found 2 matches"), "expected disambiguation payload: {msg}");
        assert!(msg.contains("src/a.py"), "expected first candidate path: {msg}");
        assert!(msg.contains("src/b.py"), "expected second candidate path: {msg}");
    }

    // ── get_project_skeleton ──────────────────────────────────────────────────

    #[test]
    fn skeleton_empty_db_returns_ingest_hint() {
        let conn = make_db();
        let out = get_project_skeleton(&conn, "r", None).unwrap();
        assert!(out.contains("ingest_repo"), "expected ingest hint: {out}");
    }

    #[test]
    fn skeleton_lists_functions_and_classes_grouped_by_file() {
        let conn = make_db();
        insert_node(&conn, "r:a.rs:main",   "r", "src/a.rs", "rs", "main",   "function", "fn main() {}");
        insert_node(&conn, "r:a.rs:Foo",    "r", "src/a.rs", "rs", "Foo",    "struct",   "struct Foo {}");
        insert_node(&conn, "r:b.py:helper", "r", "src/b.py", "py", "helper", "function", "def helper(): pass");
        // Variable — should NOT appear
        insert_node(&conn, "r:a.rs:X", "r", "src/a.rs", "rs", "X", "variable", "let x = 1;");

        let out = get_project_skeleton(&conn, "r", None).unwrap();
        assert!(out.contains("src/a.rs"), "a.rs missing: {out}");
        assert!(out.contains("src/b.py"), "b.py missing: {out}");
        assert!(out.contains("[function] main"),   "main missing: {out}");
        assert!(out.contains("[struct] Foo"),      "Foo missing: {out}");
        assert!(out.contains("[function] helper"), "helper missing: {out}");
        assert!(!out.contains("[variable]"),       "variable should be filtered: {out}");
    }

    #[test]
    fn skeleton_target_dir_filters_to_prefix() {
        let conn = make_db();
        insert_node(&conn, "r:a:fn1", "r", "src/api/a.rs",  "rs", "fn1", "function", "fn fn1() {}");
        insert_node(&conn, "r:b:fn2", "r", "src/core/b.rs", "rs", "fn2", "function", "fn fn2() {}");

        let out = get_project_skeleton(&conn, "r", Some("src/api")).unwrap();
        assert!(out.contains("fn1"),  "fn1 missing: {out}");
        assert!(!out.contains("fn2"), "fn2 should be filtered: {out}");
    }

    #[test]
    fn skeleton_target_dir_does_not_match_sibling_prefixes() {
        let conn = make_db();
        insert_node(&conn, "r:a:fn1", "r", "src/api/a.rs", "rs", "fn1", "function", "fn fn1() {}");
        insert_node(&conn, "r:b:fn2", "r", "src/api_old/b.rs", "rs", "fn2", "function", "fn fn2() {}");

        let out = get_project_skeleton(&conn, "r", Some("src/api")).unwrap();
        assert!(out.contains("fn1"), "expected api symbol: {out}");
        assert!(!out.contains("fn2"), "sibling prefix should not match target_dir: {out}");
    }

    #[test]
    fn skeleton_no_matching_nodes_after_filter_returns_message() {
        let conn = make_db();
        insert_node(&conn, "r:a:fn1", "r", "src/a.rs", "rs", "fn1", "function", "fn fn1() {}");

        let out = get_project_skeleton(&conn, "r", Some("src/nonexistent")).unwrap();
        assert!(out.contains("No matching symbols"), "expected no-match message: {out}");
    }

    #[test]
    fn skeleton_only_lists_symbols_for_requested_repo() {
        let conn = make_db();
        insert_node(&conn, "repo_a:a:fn1", "repo_a", "src/a.rs", "rs", "fn1", "function", "fn fn1() {}");
        insert_node(&conn, "repo_b:b:fn2", "repo_b", "src/b.rs", "rs", "fn2", "function", "fn fn2() {}");

        let out = get_project_skeleton(&conn, "repo_a", None).unwrap();
        assert!(out.contains("fn1"), "repo_a symbol missing: {out}");
        assert!(!out.contains("fn2"), "repo_b symbol leaked into repo_a output: {out}");
    }

    #[test]
    fn capsule_skips_missing_file_gracefully() {
        let conn = make_db();
        let dir = tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();
        insert_repo(&conn, "r", &root);
        insert_node(
            &conn,
            "r:src/missing.py:foo",
            "r",
            "src/missing.py",
            "py",
            "foo",
            "function",
            "def foo():\n    return 42\n",
        );

        // Missing file on disk should not crash — capsule still succeeds.
        let result = get_context_capsule(&conn, "foo", "r", None).unwrap();
        assert!(result.optimized_text.contains("foo"), "pivot should still be present: {}", result.optimized_text);
        assert!(result.original_text.is_empty(), "original_text should be empty when file is missing");
    }

    // ── condense (unit tests on raw text) ──────────────────────────────────

    #[test]
    fn condense_cpp_function_replaces_body() {
        let raw = "void process(int x) {\n    x += 1;\n    return;\n}";
        let result = condense(raw, "cpp");
        assert!(result.contains("process(int x)"), "signature lost: {result}");
        assert!(result.contains("{ /* ... */ }"), "placeholder missing: {result}");
        assert!(!result.contains("x += 1"), "body leaked: {result}");
    }

    #[test]
    fn condense_cpp_forward_decl_unchanged() {
        let raw = "class Foo;";
        assert_eq!(condense(raw, "cpp"), raw);
    }

    #[test]
    fn condense_py_function_replaces_body_with_pass() {
        let raw = "def compute(n):\n    total = 0\n    return total\n";
        let result = condense(raw, "py");
        assert!(result.contains("def compute(n):"), "signature lost: {result}");
        assert!(result.contains("pass"), "pass placeholder missing: {result}");
        assert!(!result.contains("total"), "body leaked: {result}");
    }

    #[test]
    fn condense_ts_function_replaces_body() {
        let raw = "function greet(name: string): string {\n    return `Hello ${name}`;\n}";
        let result = condense(raw, "ts");
        assert!(result.contains("greet(name: string)"), "signature lost: {result}");
        assert!(result.contains("{ /* ... */ }"), "placeholder missing: {result}");
        assert!(!result.contains("Hello"), "body leaked: {result}");
    }

    #[test]
    fn condense_rust_function_replaces_body() {
        let raw = "fn compute(n: u32) -> u32 {\n    let x = n * 2;\n    x\n}";
        let result = condense(raw, "rs");
        assert!(result.contains("fn compute(n: u32)"), "signature lost: {result}");
        assert!(result.contains("{ /* ... */ }"), "placeholder missing: {result}");
        assert!(!result.contains("n * 2"), "body leaked: {result}");
    }

    #[test]
    fn condense_ruby_method_replaces_body_preserves_end() {
        let raw = "def greet(name)\n  puts name\n  name.upcase\nend\n";
        let result = condense(raw, "rb");
        assert!(result.contains("def greet"), "signature lost: {result}");
        assert!(result.contains("end"), "`end` keyword must be preserved: {result}");
        assert!(result.contains("# ..."), "placeholder missing: {result}");
        assert!(!result.contains("puts name"), "body leaked: {result}");
    }

    #[test]
    fn condense_ruby_preserves_end_not_chopped() {
        // Verifies the byte-range replacement does NOT consume the closing `end`.
        let raw = "def foo\n  x = 1\nend\n";
        let result = condense(raw, "rb");
        assert!(result.ends_with("end\n"), "`end` must close the method: {result}");
    }

    // ── Filepath disambiguation ───────────────────────────────────────────

    #[test]
    fn capsule_disambiguates_by_filepath() {
        let conn = make_db();
        insert_node(
            &conn, "r:src/a.py:bulk_update", "r", "src/a.py", "py",
            "bulk_update", "function", "def bulk_update():\n    return 'a'\n",
        );
        insert_node(
            &conn, "r:src/b.py:bulk_update", "r", "src/b.py", "py",
            "bulk_update", "function", "def bulk_update():\n    return 'b'\n",
        );

        let result = get_context_capsule(&conn, "bulk_update", "r", Some("src/a.py")).unwrap();
        assert!(
            result.optimized_text.contains("return 'a'"),
            "should resolve to src/a.py variant: {}", result.optimized_text
        );
        assert!(
            !result.optimized_text.contains("return 'b'"),
            "src/b.py variant should not appear: {}", result.optimized_text
        );
    }

    #[test]
    fn capsule_filepath_mismatch_returns_not_found() {
        let conn = make_db();
        insert_node(
            &conn, "r:src/a.py:bulk_update", "r", "src/a.py", "py",
            "bulk_update", "function", "def bulk_update():\n    pass\n",
        );

        let err = get_context_capsule(&conn, "bulk_update", "r", Some("src/nonexistent.py")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected not-found error: {msg}");
    }

    #[test]
    fn impact_disambiguates_by_filepath() {
        let conn = make_db();
        insert_node(
            &conn, "r:src/a.py:bulk_update", "r", "src/a.py", "py",
            "bulk_update", "function", "def bulk_update():\n    return 'a'\n",
        );
        insert_node(
            &conn, "r:src/b.py:bulk_update", "r", "src/b.py", "py",
            "bulk_update", "function", "def bulk_update():\n    return 'b'\n",
        );
        insert_node(
            &conn, "r:src/a.py:caller", "r", "src/a.py", "py",
            "caller", "function", "def caller(): bulk_update()\n",
        );
        insert_edge(&conn, "r:src/a.py:caller", "r:src/a.py:bulk_update", "CALLS");

        let result = analyze_impact(&conn, "bulk_update", "r", Some("src/a.py")).unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "caller");
    }

    #[test]
    fn capsule_no_filepath_still_errors_on_ambiguity() {
        let conn = make_db();
        insert_node(
            &conn, "r:src/a.py:dup", "r", "src/a.py", "py",
            "dup", "function", "def dup(): pass\n",
        );
        insert_node(
            &conn, "r:src/b.py:dup", "r", "src/b.py", "py",
            "dup", "function", "def dup(): pass\n",
        );

        // Disambiguation without filepath is now a successful payload, not an error.
        let result = get_context_capsule(&conn, "dup", "r", None).unwrap();
        assert!(
            result.optimized_text.contains("Found 2 matches"),
            "should return disambiguation payload without filepath: {}", result.optimized_text
        );
    }
}
