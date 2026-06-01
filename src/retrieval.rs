use anyhow::{anyhow, Result};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    fmt::Write as FmtWrite,
    fs,
    path::{Path, PathBuf},
};
use streaming_iterator::StreamingIterator;
use tree_sitter::{Language, Parser, Query, QueryCursor};

// ── Public types ──────────────────────────────────────────────────────────────

/// How `get_context_capsule` fills `CapsuleResult::original_text` (env-driven).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapsuleOriginalMode {
    /// Default: do not load touched files into `original_text` (empty string on success).
    None,
    /// Legacy: concatenate full file contents of touched paths (bounded by
    /// `MARROW_CAPSULE_ORIGINAL_MAX_BYTES` when set).
    Full,
}

/// Resolved from `MARROW_CAPSULE_ORIGINAL_MODE`, with `MARROW_CAPSULE_ORIGINAL_LEGACY=1` → [`CapsuleOriginalMode::Full`].
pub fn capsule_original_mode() -> CapsuleOriginalMode {
    if env_truthy("MARROW_CAPSULE_ORIGINAL_LEGACY") {
        return CapsuleOriginalMode::Full;
    }
    match std::env::var("MARROW_CAPSULE_ORIGINAL_MODE") {
        Ok(s) if s.eq_ignore_ascii_case("full") => CapsuleOriginalMode::Full,
        Ok(s) if s.eq_ignore_ascii_case("none") => CapsuleOriginalMode::None,
        Err(_) => CapsuleOriginalMode::None,
        Ok(_) => CapsuleOriginalMode::None,
    }
}

fn env_truthy(key: &str) -> bool {
    match std::env::var(key) {
        Ok(s) => {
            let t = s.trim();
            matches!(t, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
        }
        Err(_) => false,
    }
}

/// Returned by `get_context_capsule`: both strings are derived from a single
/// graph traversal, ensuring telemetry and the compare endpoint use identical
/// source data.
#[derive(Debug)]
pub struct CapsuleResult {
    /// The condensed capsule text sent to the LLM (optimized).
    pub optimized_text: String,
    /// Raw file payload for telemetry / compare. Empty when [`CapsuleOriginalMode::None`]
    /// (default); full concatenation when [`CapsuleOriginalMode::Full`]. Disambiguation
    /// payloads mirror `optimized_text`.
    pub original_text: String,
    /// MCP / dashboard `file_tokens` heuristic (`len/4`), mode-aware (metadata sum when not full).
    pub file_tokens: usize,
    /// Bounded inspectable proof for dashboard compare. This is separate from
    /// `original_text` so normal MCP responses can stay low-cost.
    pub proof_snapshot: Option<CapsuleProofSnapshot>,
    /// Provenance labels for token baselines and proof material.
    pub provenance: CapsuleProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapsuleProofSnapshot {
    pub proof_text: String,
    pub proof_label: String,
    pub token_source: String,
    pub truncated: bool,
    pub sampled: bool,
    pub max_bytes: usize,
    pub max_files: usize,
    pub touched_file_count: usize,
    pub included_file_count: usize,
    pub omitted_file_count: usize,
    pub omitted_paths_preview: Vec<String>,
}

impl CapsuleProofSnapshot {
    pub fn without_text(&self) -> Self {
        let mut slim = self.clone();
        slim.proof_text.clear();
        slim
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CapsuleProvenance {
    // TODO: make heavy proof/provenance capture opt-in when dashboard callers request it.
    pub baseline_token_source: String,
    pub tokenizer_mode: String,
    pub original_mode: String,
    pub proof_label: String,
    pub precise_file_tokens: bool,
    pub original_max_bytes: Option<usize>,
    pub proof_max_bytes: usize,
    pub proof_max_files: usize,
    pub touched_file_count: usize,
}

impl Default for CapsuleProvenance {
    fn default() -> Self {
        Self {
            baseline_token_source: "unavailable".to_string(),
            tokenizer_mode: "unknown".to_string(),
            original_mode: "none".to_string(),
            proof_label: "unavailable".to_string(),
            precise_file_tokens: false,
            original_max_bytes: None,
            proof_max_bytes: capsule_proof_max_bytes(),
            proof_max_files: capsule_proof_max_files(),
            touched_file_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PreciseTokenMeasurement {
    pub tokens: usize,
    pub touched_file_count: usize,
    pub measured_file_count: usize,
    pub failed_paths: Vec<String>,
    pub tokenizer_mode: String,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DependencyDirection {
    Callers,
    Callees,
    Both,
}

impl DependencyDirection {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "callers" => Ok(Self::Callers),
            "callees" => Ok(Self::Callees),
            "both" => Ok(Self::Both),
            other => Err(anyhow!(
                "invalid dependency graph direction '{other}'; expected callers, callees, or both"
            )),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Callers => "callers",
            Self::Callees => "callees",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DependencyGraphOptions {
    pub depth: usize,
    pub direction: DependencyDirection,
    pub include_source: bool,
    pub max_nodes: usize,
    pub max_bytes: usize,
}

impl Default for DependencyGraphOptions {
    fn default() -> Self {
        Self {
            depth: 2,
            direction: DependencyDirection::Both,
            include_source: false,
            max_nodes: dependency_graph_max_nodes(),
            max_bytes: dependency_graph_max_bytes(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BatchIntent {
    FindSymbol,
    ExploreSymbol,
    TraceFlow,
    RefactorSymbol,
    ReadNode,
    DependencyGraph,
}

impl BatchIntent {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "find_symbol" => Some(Self::FindSymbol),
            "explore_symbol" | "capsule" => Some(Self::ExploreSymbol),
            "trace_flow" => Some(Self::TraceFlow),
            "refactor_symbol" | "analyze_impact" => Some(Self::RefactorSymbol),
            "read_node" => Some(Self::ReadNode),
            "dependency_graph" => Some(Self::DependencyGraph),
            _ => None,
        }
    }

    fn canonical_name(self) -> &'static str {
        match self {
            Self::FindSymbol => "find_symbol",
            Self::ExploreSymbol => "explore_symbol",
            Self::TraceFlow => "trace_flow",
            Self::RefactorSymbol => "refactor_symbol",
            Self::ReadNode => "read_node",
            Self::DependencyGraph => "dependency_graph",
        }
    }

    fn section_label(self) -> &'static str {
        match self {
            Self::FindSymbol => "FIND",
            Self::ExploreSymbol | Self::ReadNode => "EXPLORE",
            Self::TraceFlow => "TRACE",
            Self::RefactorSymbol => "IMPACT",
            Self::DependencyGraph => "DEPENDENCY GRAPH",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchQuery {
    pub intent: BatchIntent,
    pub target: String,
    pub filepath: Option<String>,
    pub kind: Option<String>,
    pub limit: Option<usize>,
    pub depth: Option<usize>,
    pub direction: Option<DependencyDirection>,
    pub include_source: bool,
    pub max_nodes: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct BatchOptions {
    pub repo_id: String,
    pub max_bytes: usize,
}

#[derive(Debug)]
pub struct BatchExecution {
    pub text: String,
    pub query_count: usize,
    pub truncated: bool,
    pub telemetry: Vec<BatchTelemetry>,
}

#[derive(Debug)]
pub enum BatchTelemetry {
    Capsule {
        symbol: String,
        repo: String,
        file: String,
        capsule_tokens: usize,
        file_tokens: usize,
        original_text: Option<String>,
        optimized_text: String,
        proof_snapshot: Option<Box<CapsuleProofSnapshot>>,
        provenance: Box<CapsuleProvenance>,
    },
    Impact {
        symbol: String,
        repo: String,
        affected_count: usize,
    },
    Skeleton {
        target_dir: String,
        node_count: usize,
    },
}

type NodeRow = (String, String, String, String, String, String);

#[derive(Debug, Clone)]
struct NodeBrief {
    id: String,
    symbol_name: String,
    symbol_type: String,
    file_path: String,
    language: String,
    raw_text: String,
}

impl From<NodeRow> for NodeBrief {
    fn from(row: NodeRow) -> Self {
        Self {
            id: row.0,
            symbol_name: row.1,
            symbol_type: row.2,
            file_path: row.3,
            language: row.4,
            raw_text: row.5,
        }
    }
}

/// Maximum number of inbound callers to show before truncating in formatted output.
const MAX_INBOUND_CALLERS: usize = 10;

/// Default max bytes for a pivot's full source in `format_capsule`.
/// Pivots exceeding this are auto-condensed to signatures.
/// Override with `MARROW_CAPSULE_MAX_PIVOT_BYTES`.
const DEFAULT_MAX_PIVOT_BYTES: usize = 12_000; // ~3,000 tokens

/// Max bytes for a pivot's full source before auto-condensation.
fn capsule_max_pivot_bytes() -> usize {
    env_usize_positive("MARROW_CAPSULE_MAX_PIVOT_BYTES", DEFAULT_MAX_PIVOT_BYTES)
}

fn env_usize_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Max outbound neighbors loaded into a context capsule / trace (bounds RAM).
fn capsule_max_outbound_neighbors() -> usize {
    env_usize_positive("MARROW_CAPSULE_MAX_OUTBOUND", 50)
}

/// Max inbound rows fetched from SQLite (display still capped at [`MAX_INBOUND_CALLERS`]).
fn capsule_max_inbound_neighbors_load() -> usize {
    env_usize_positive("MARROW_CAPSULE_MAX_INBOUND_LOAD", 64).max(MAX_INBOUND_CALLERS)
}

/// Max rows returned by `analyze_impact` (breadth × depth cap).
pub fn impact_max_rows() -> usize {
    env_usize_positive("MARROW_IMPACT_MAX_ROWS", 5000)
}

pub fn batch_max_bytes() -> usize {
    env_usize_positive("MARROW_BATCH_MAX_BYTES", 100_000)
}

pub fn dependency_graph_max_nodes() -> usize {
    env_usize_positive("MARROW_DEP_GRAPH_MAX_NODES", 200)
}

pub fn dependency_graph_max_bytes() -> usize {
    env_usize_positive("MARROW_DEP_GRAPH_MAX_BYTES", 100_000)
}

/// Max total bytes for `CapsuleResult::original_text` (concatenated full files touched by the
/// capsule). When set, stops reading further files once the budget would be exceeded — avoids
/// holding hundreds of large source files in RAM at once on low-memory hosts.
///
/// Unset or `0` = unlimited (legacy behavior).
fn capsule_original_text_max_bytes() -> Option<usize> {
    match std::env::var("MARROW_CAPSULE_ORIGINAL_MAX_BYTES") {
        Ok(s) => {
            let n: usize = s.parse().ok()?;
            (n > 0).then_some(n)
        }
        Err(_) => None,
    }
}

pub fn capsule_proof_max_bytes() -> usize {
    env_usize_positive("MARROW_CAPSULE_PROOF_MAX_BYTES", 16 * 1024)
}

pub fn capsule_proof_max_files() -> usize {
    env_usize_positive("MARROW_CAPSULE_PROOF_MAX_FILES", 8)
}

fn original_mode_label(mode: CapsuleOriginalMode) -> &'static str {
    match mode {
        CapsuleOriginalMode::None => "none",
        CapsuleOriginalMode::Full => "full",
    }
}

fn proof_label(sampled: bool, truncated: bool) -> &'static str {
    match (sampled, truncated) {
        (true, true) => "sampled_truncated_proof",
        (true, false) => "sampled_proof",
        (false, true) => "truncated_proof",
        (false, false) => "cached_proof",
    }
}

fn baseline_token_source(mode: CapsuleOriginalMode, truncated_full: bool) -> &'static str {
    match mode {
        CapsuleOriginalMode::None => "estimated",
        CapsuleOriginalMode::Full if truncated_full => "truncated_full",
        CapsuleOriginalMode::Full => "full",
    }
}

fn touched_paths_for_capsule(capsule: &ContextCapsule) -> HashSet<String> {
    let mut touched: HashSet<String> = HashSet::new();
    touched.insert(capsule.pivot.file_path.clone());
    for n in &capsule.neighbors {
        touched.insert(n.node.file_path.clone());
    }
    touched
}

fn prefix_by_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

fn sampled_paths<'a>(paths: &'a [&'a String], max_files: usize) -> Vec<&'a String> {
    if paths.len() <= max_files {
        return paths.to_vec();
    }
    if max_files <= 1 {
        return vec![paths[0]];
    }
    let last = paths.len() - 1;
    (0..max_files)
        .map(|i| {
            let idx = (i * last) / (max_files - 1);
            paths[idx]
        })
        .collect()
}

fn build_bounded_proof_snapshot(
    root: Option<&Path>,
    touched: &HashSet<String>,
    baseline_source: &str,
) -> Option<CapsuleProofSnapshot> {
    let root = root?;
    let max_bytes = capsule_proof_max_bytes();
    let max_files = capsule_proof_max_files();
    let mut paths: Vec<&String> = touched.iter().collect();
    paths.sort();
    if paths.is_empty() || max_bytes == 0 || max_files == 0 {
        return None;
    }

    let selected = sampled_paths(&paths, max_files);
    let sampled = paths.len() > selected.len();
    let mut used = 0usize;
    let mut text = String::new();
    let mut included = 0usize;
    let mut truncated = false;
    let mut omitted_paths_preview: Vec<String> = paths
        .iter()
        .filter(|p| !selected.iter().any(|s| *s == **p))
        .take(5)
        .map(|p| (*p).clone())
        .collect();

    for rel_path in selected {
        let header = format!("── MARROW PROOF: {rel_path} ──\n");
        if used.saturating_add(header.len()) >= max_bytes {
            truncated = true;
            if omitted_paths_preview.len() < 5 {
                omitted_paths_preview.push(rel_path.clone());
            }
            break;
        }
        let abs_path = match resolve_repo_file_path(root, rel_path) {
            Ok(path) => path,
            Err(_) => continue,
        };
        let file_text = match fs::read_to_string(&abs_path) {
            Ok(contents) => contents,
            Err(_) => continue,
        };
        let remaining = max_bytes - used - header.len();
        let body = prefix_by_bytes(&file_text, remaining);
        if body.len() < file_text.len() {
            truncated = true;
        }
        if !text.is_empty() {
            text.push('\n');
            used = used.saturating_add(1);
        }
        text.push_str(&header);
        text.push_str(body);
        used = used.saturating_add(header.len()).saturating_add(body.len());
        included += 1;
        if truncated {
            break;
        }
    }

    if text.is_empty() {
        return None;
    }

    let omitted = touched.len().saturating_sub(included);
    Some(CapsuleProofSnapshot {
        proof_text: text,
        proof_label: proof_label(sampled, truncated).to_string(),
        token_source: baseline_source.to_string(),
        truncated,
        sampled,
        max_bytes,
        max_files,
        touched_file_count: touched.len(),
        included_file_count: included,
        omitted_file_count: omitted,
        omitted_paths_preview,
    })
}

/// Sum `metadata().len() / 4` over unique touched relative paths (skip missing / unreadable).
fn file_tokens_metadata_estimate(root: Option<&Path>, touched: &HashSet<String>) -> usize {
    let mut total: usize = 0;
    let mut paths: Vec<&String> = touched.iter().collect();
    paths.sort();
    for rel_path in paths {
        let Some(root) = root else {
            continue;
        };
        let abs_path = match resolve_repo_file_path(root, rel_path) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let Ok(meta) = fs::metadata(&abs_path) else {
            continue;
        };
        total = total.saturating_add((meta.len() as usize) / 4);
    }
    total
}

/// Concatenate full file contents for touched paths in **sorted path order**.
/// With `budget` = `Some(max)`, uses `metadata().len()` before `read_to_string` and skips files
/// that would exceed the remaining budget (returns `(text, truncated, omitted_paths)`).
pub(crate) fn concat_full_original_text_sorted(
    root: Option<&Path>,
    touched: &HashSet<String>,
    budget: Option<usize>,
) -> (String, bool, Vec<String>) {
    let mut parts = Vec::new();
    let mut paths: Vec<&String> = touched.iter().collect();
    paths.sort();
    let mut truncated_original = false;
    let mut omitted_paths: Vec<String> = Vec::new();

    match budget {
        Some(max_bytes) => {
            let mut used: usize = 0;
            for rel_path in paths {
                let Some(root) = root else {
                    continue;
                };
                let abs_path = match resolve_repo_file_path(root, rel_path) {
                    Ok(path) => path,
                    Err(_) => continue,
                };
                let file_len = match fs::metadata(&abs_path) {
                    Ok(m) => m.len() as usize,
                    Err(_) => continue,
                };
                let sep = if parts.is_empty() { 0 } else { 1 };
                let next = used.saturating_add(sep).saturating_add(file_len);
                if next > max_bytes {
                    truncated_original = true;
                    omitted_paths.push(rel_path.clone());
                    continue;
                }
                let text = match fs::read_to_string(&abs_path) {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                used = used.saturating_add(sep).saturating_add(text.len());
                parts.push(text);
            }
        }
        None => {
            for rel_path in paths {
                let Some(root) = root else {
                    continue;
                };
                let abs_path = match resolve_repo_file_path(root, rel_path) {
                    Ok(path) => path,
                    Err(_) => continue,
                };
                match fs::read_to_string(&abs_path) {
                    Ok(text) => parts.push(text),
                    Err(_) => continue,
                }
            }
        }
    }

    (parts.join("\n"), truncated_original, omitted_paths)
}

/// Tiktoken (cl100k_base) token count summed per touched file, with failure provenance.
pub fn measure_precise_tokens_touched_by_capsule(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> Result<PreciseTokenMeasurement> {
    let (pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw) =
        match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
            SymbolResolution::Unique(row) => row,
            SymbolResolution::Ambiguous(_) => {
                return Ok(PreciseTokenMeasurement {
                    tokens: 0,
                    touched_file_count: 0,
                    measured_file_count: 0,
                    failed_paths: Vec::new(),
                    tokenizer_mode: "cl100k_base".to_string(),
                })
            }
        };

    let capsule = build_context_capsule_from_resolved(
        conn, pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw,
    )?;

    let touched = touched_paths_for_capsule(&capsule);

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

    let bpe = tiktoken_rs::cl100k_base().map_err(|e| anyhow!("tiktoken: {e}"))?;
    let mut paths: Vec<&String> = touched.iter().collect();
    paths.sort();
    let mut total = 0usize;
    let mut measured_file_count = 0usize;
    let mut failed_paths = Vec::new();
    for rel_path in paths {
        let Some(root) = root.as_ref() else {
            failed_paths.push(rel_path.clone());
            continue;
        };
        let abs_path = match resolve_repo_file_path(root, rel_path) {
            Ok(p) => p,
            Err(_) => {
                failed_paths.push(rel_path.clone());
                continue;
            }
        };
        let contents = match fs::read_to_string(&abs_path) {
            Ok(s) => s,
            Err(_) => {
                failed_paths.push(rel_path.clone());
                continue;
            }
        };
        total = total.saturating_add(bpe.encode_with_special_tokens(&contents).len());
        measured_file_count += 1;
    }
    Ok(PreciseTokenMeasurement {
        tokens: total,
        touched_file_count: touched.len(),
        measured_file_count,
        failed_paths,
        tokenizer_mode: "cl100k_base".to_string(),
    })
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch the pivot node's full source and all depth-1 neighbors condensed.
///
/// [`CapsuleResult::optimized_text`] is always the primary LLM payload. [`CapsuleResult::original_text`]
/// follows [`capsule_original_mode`]: default `none` leaves it empty (no concat read); `full` loads
/// full touched files (sorted paths, optional byte budget). [`CapsuleResult::file_tokens`] matches
/// dashboard/MCP telemetry (`metadata_len/4` when not `full`, else `original_text.len()/4`).
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
                let file_tokens = payload.len() / 4;
                return Ok(CapsuleResult {
                    optimized_text: payload.clone(),
                    original_text: String::new(),
                    file_tokens,
                    proof_snapshot: None,
                    provenance: CapsuleProvenance {
                        baseline_token_source: "unavailable".to_string(),
                        tokenizer_mode: "chars/4".to_string(),
                        original_mode: original_mode_label(capsule_original_mode()).to_string(),
                        proof_label: "unavailable".to_string(),
                        precise_file_tokens: false,
                        original_max_bytes: capsule_original_text_max_bytes(),
                        proof_max_bytes: capsule_proof_max_bytes(),
                        proof_max_files: capsule_proof_max_files(),
                        touched_file_count: 0,
                    },
                });
            }
        };

    let capsule = build_context_capsule_from_resolved(
        conn, pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw,
    )?;
    let optimized_text = format_capsule(&capsule);

    // Collect all unique file paths touched by this capsule.
    let touched = touched_paths_for_capsule(&capsule);

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

    let mode = capsule_original_mode();
    let budget = capsule_original_text_max_bytes();
    let mut truncated_original = false;
    let mut omitted_paths: Vec<String> = Vec::new();

    let mut original_text = if mode == CapsuleOriginalMode::None {
        String::new()
    } else {
        let (text, trunc, omit) =
            concat_full_original_text_sorted(root.as_deref(), &touched, budget);
        truncated_original = trunc;
        omitted_paths = omit;
        text
    };

    if truncated_original {
        if let Some(lim) = budget {
            let note = format!(
                "\n\n── MARROW: original_text truncated (MARROW_CAPSULE_ORIGINAL_MAX_BYTES={lim}) ──\n\
                 Omitted {} file(s) (not loaded to cap RAM). Example paths: {}\n",
                omitted_paths.len(),
                omitted_paths
                    .iter()
                    .take(5)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            original_text.push_str(&note);
        }
    }

    let file_tokens = if mode == CapsuleOriginalMode::Full {
        original_text.len() / 4
    } else {
        file_tokens_metadata_estimate(root.as_deref(), &touched)
    };
    let baseline_source = baseline_token_source(mode, truncated_original);
    let proof_snapshot = if mode == CapsuleOriginalMode::None {
        build_bounded_proof_snapshot(root.as_deref(), &touched, baseline_source)
    } else {
        None
    };
    let proof_label = proof_snapshot
        .as_ref()
        .map(|p| p.proof_label.clone())
        .unwrap_or_else(|| baseline_source.to_string());
    let provenance = CapsuleProvenance {
        baseline_token_source: baseline_source.to_string(),
        tokenizer_mode: if mode == CapsuleOriginalMode::None {
            "metadata_len/4".to_string()
        } else {
            "text_len/4".to_string()
        },
        original_mode: original_mode_label(mode).to_string(),
        proof_label,
        precise_file_tokens: false,
        original_max_bytes: budget,
        proof_max_bytes: capsule_proof_max_bytes(),
        proof_max_files: capsule_proof_max_files(),
        touched_file_count: touched.len(),
    };

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
        let any_stale = observations.iter().any(|(_, is_stale, _)| *is_stale);
        for (text, is_stale, ts) in observations {
            let compact_text = text.split_whitespace().collect::<Vec<_>>().join(" ");
            let stale_marker = if is_stale { ", STALE" } else { "" };
            out.push_str(&format!(
                "- {compact_text}  (recorded: {ts}{stale_marker})\n"
            ));
        }
        if any_stale {
            out.push_str("[Note: items marked STALE were recorded against code that has since changed — re-verify before trusting.]\n");
        }
        out
    };

    Ok(CapsuleResult {
        optimized_text,
        original_text,
        file_tokens,
        proof_snapshot,
        provenance,
    })
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
    let outbound_rows: Vec<(String, String, String, String, String, String, String)> =
        outbound_stmt
            .query_map(rusqlite::params![pivot_id, out_lim], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
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
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
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
    )
    .ok();
    writeln!(out, "File : {}", capsule.pivot.file_path).ok();
    writeln!(out, "Type : {}", capsule.pivot.symbol_type).ok();

    let max_pivot = capsule_max_pivot_bytes();
    if capsule.pivot.text.len() > max_pivot {
        // Auto-condense oversized pivots (e.g. large classes with hundreds of scopes/methods).
        let condensed = condense(&capsule.pivot.text, &capsule.pivot.language);
        writeln!(
            out,
            "\n── CONDENSED SOURCE (pivot exceeded {max_pivot}B cap) ─────────────"
        )
        .ok();
        writeln!(out, "{}", condensed).ok();
        writeln!(
            out,
            "[Note: full source ({orig}B) condensed to save tokens. \
             Use read_node or native file read for the complete body. \
             Set MARROW_CAPSULE_MAX_PIVOT_BYTES to adjust threshold.]",
            orig = capsule.pivot.text.len()
        )
        .ok();
    } else {
        writeln!(
            out,
            "\n── FULL SOURCE ──────────────────────────────────────────────"
        )
        .ok();
        writeln!(out, "{}", capsule.pivot.text).ok();
    }

    let outbound: Vec<&NeighborInfo> = capsule
        .neighbors
        .iter()
        .filter(|n| n.direction == EdgeDirection::Outbound)
        .collect();
    let inbound: Vec<&NeighborInfo> = capsule
        .neighbors
        .iter()
        .filter(|n| n.direction == EdgeDirection::Inbound)
        .collect();

    if outbound.is_empty() && inbound.is_empty() {
        writeln!(
            out,
            "── NEIGHBORS ────────────────────────────────────────────────"
        )
        .ok();
        writeln!(out, "  (none — isolated symbol)").ok();
        return out;
    }

    // ── Outbound: things this symbol calls/imports (signatures only) ─────────
    if !outbound.is_empty() {
        writeln!(
            out,
            "\n── OUTBOUND DEPENDENCIES (signatures only) ────────────────────────"
        )
        .ok();
        for n in &outbound {
            // Progressive disclosure: show only the first non-empty line (signature).
            // Full bodies are available via `run_pipeline` with `intent: "read_node"`.
            let signature = n
                .node
                .text
                .lines()
                .find(|l| !l.trim().is_empty())
                .unwrap_or(&n.node.symbol_name);
            writeln!(
                out,
                "\n  [{rel}]  {name}  ({lang})  {path}\n  {sig}",
                rel = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
                sig = signature,
            )
            .ok();
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
        writeln!(
            out,
            "\n── INBOUND CALLERS (who calls this) ─────────────────────────"
        )
        .ok();
        let shown = inbound.len().min(MAX_INBOUND_CALLERS);
        let omitted = inbound.len().saturating_sub(MAX_INBOUND_CALLERS);
        for n in &inbound[..shown] {
            writeln!(
                out,
                "  [{rel}]  {name}  ({lang})  {path}",
                rel = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            )
            .ok();
        }
        if omitted > 0 {
            writeln!(
                out,
                "  [... and {omitted} more callers omitted for brevity]"
            )
            .ok();
        }
    }

    if !outbound.is_empty() {
        writeln!(
            out,
            "\n[Expand a neighbor: run_pipeline(intent: \"read_node\", target: \"<symbol>\")]"
        )
        .ok();
    }

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

pub fn format_impact_result(result: &ImpactResult) -> String {
    let mut out = String::new();

    if let Some(payload) = result.pivot_id.strip_prefix("DISAMBIGUATION:") {
        out.push_str(payload);
        return out;
    }

    writeln!(out, "IMPACT ANALYSIS — pivot id: {}", result.pivot_id).ok();
    if result.affected.is_empty() {
        writeln!(
            out,
            "No downstream dependents found. Symbol is safe to change in isolation."
        )
        .ok();
    } else {
        writeln!(
            out,
            "{:>5}  {:>10}  {:<20}  {:<10}  {:<14}  FILE",
            "DEPTH", "REL_TYPE", "SYMBOL", "SYM_TYPE", "REPO"
        )
        .ok();
        writeln!(out, "{}", "─".repeat(80)).ok();
        for n in &result.affected {
            writeln!(
                out,
                "{depth:>5}  {rel:>10}  {sym:<20}  {typ:<10}  {repo:<14}  {file}",
                depth = n.depth,
                rel = n.relationship_type,
                sym = n.symbol_name,
                typ = n.symbol_type,
                repo = n.repo_id,
                file = n.file_path
            )
            .ok();
        }
        writeln!(out, "\n{} node(s) affected.", result.affected.len()).ok();
        if result.truncated {
            writeln!(
                out,
                "\n[Note: impact list truncated at MARROW_IMPACT_MAX_ROWS ({}); raise for more rows.]",
                impact_max_rows()
            )
            .ok();
        }
    }

    out
}

pub fn execute_batch_queries(
    conn: &Connection,
    queries: &[BatchQuery],
    options: BatchOptions,
) -> Result<BatchExecution> {
    let total = queries.len();
    let mut first_seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut sections = Vec::with_capacity(total);
    let mut telemetry = Vec::new();

    for (idx, query) in queries.iter().enumerate() {
        let ordinal = idx + 1;
        let canonical = canonical_batch_key(&options.repo_id, query);
        let body = if let Some(first) = first_seen.get(&canonical) {
            format!("Same as [{first}/{total}] above.")
        } else {
            first_seen.insert(canonical, ordinal);
            match execute_one_batch_query(conn, query, &options.repo_id) {
                Ok(executed) => {
                    if let Some(event) = executed.telemetry {
                        telemetry.push(event);
                    }
                    executed.text
                }
                Err(err) => format!("ERROR: {err}"),
            }
        };
        sections.push(format_batch_section(query, ordinal, total, &body));
    }

    let (text, truncated) = cap_batch_sections_fifo(&sections, options.max_bytes);
    Ok(BatchExecution {
        text,
        query_count: total,
        truncated,
        telemetry,
    })
}

struct ExecutedBatchQuery {
    text: String,
    telemetry: Option<BatchTelemetry>,
}

fn canonical_batch_key(repo_id: &str, query: &BatchQuery) -> String {
    format!(
        "{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        repo_id,
        query.intent.canonical_name(),
        query.target,
        query.filepath.as_deref().unwrap_or_default(),
        query.kind.as_deref().unwrap_or_default(),
        query.limit.unwrap_or(0),
        query.depth.unwrap_or(0),
        query
            .direction
            .map(DependencyDirection::label)
            .unwrap_or(""),
        query.include_source,
        query.max_nodes.unwrap_or(0)
    )
}

fn execute_one_batch_query(
    conn: &Connection,
    query: &BatchQuery,
    repo_id: &str,
) -> Result<ExecutedBatchQuery> {
    match query.intent {
        BatchIntent::FindSymbol => {
            let text = find_symbols(
                conn,
                repo_id,
                &query.target,
                query.kind.as_deref(),
                query.limit.unwrap_or(FIND_SYMBOL_DEFAULT_LIMIT),
            )?;
            let node_count = text
                .lines()
                .filter(|line| line.trim_start().starts_with("- "))
                .count();
            Ok(ExecutedBatchQuery {
                text,
                telemetry: Some(BatchTelemetry::Skeleton {
                    target_dir: format!("find:{}", query.target),
                    node_count,
                }),
            })
        }
        BatchIntent::ExploreSymbol | BatchIntent::ReadNode => {
            let result =
                get_context_capsule(conn, &query.target, repo_id, query.filepath.as_deref())?;
            let file =
                absolute_symbol_file_path(conn, repo_id, &query.target, query.filepath.as_deref());
            let capsule_tokens = result.optimized_text.len() / 4;
            let original_text = if capsule_original_mode() == CapsuleOriginalMode::None
                && result.original_text.is_empty()
            {
                None
            } else {
                Some(result.original_text)
            };
            let optimized_text = result.optimized_text;
            Ok(ExecutedBatchQuery {
                text: optimized_text.clone(),
                telemetry: Some(BatchTelemetry::Capsule {
                    symbol: query.target.clone(),
                    repo: repo_id.to_string(),
                    file,
                    capsule_tokens,
                    file_tokens: result.file_tokens,
                    original_text,
                    optimized_text,
                    proof_snapshot: result.proof_snapshot.map(Box::new),
                    provenance: Box::new(result.provenance),
                }),
            })
        }
        BatchIntent::TraceFlow => {
            let result = trace_logic_flow(conn, &query.target, repo_id, query.filepath.as_deref())?;
            let file =
                absolute_symbol_file_path(conn, repo_id, &query.target, query.filepath.as_deref());
            let capsule_tokens = result.optimized_text.len() / 4;
            let optimized_text = result.optimized_text;
            Ok(ExecutedBatchQuery {
                text: optimized_text.clone(),
                telemetry: Some(BatchTelemetry::Capsule {
                    symbol: query.target.clone(),
                    repo: repo_id.to_string(),
                    file,
                    capsule_tokens,
                    file_tokens: result.file_tokens,
                    original_text: None,
                    optimized_text,
                    proof_snapshot: None,
                    provenance: Box::new(result.provenance),
                }),
            })
        }
        BatchIntent::RefactorSymbol => {
            let result = analyze_impact(conn, &query.target, repo_id, query.filepath.as_deref())?;
            let text = format_impact_result(&result);
            let telemetry = result
                .pivot_id
                .strip_prefix("DISAMBIGUATION:")
                .is_none()
                .then(|| BatchTelemetry::Impact {
                    symbol: query.target.clone(),
                    repo: repo_id.to_string(),
                    affected_count: result.affected.len(),
                });
            Ok(ExecutedBatchQuery { text, telemetry })
        }
        BatchIntent::DependencyGraph => {
            let text = dependency_graph(
                conn,
                repo_id,
                &query.target,
                query.filepath.as_deref(),
                DependencyGraphOptions {
                    depth: query.depth.unwrap_or(2),
                    direction: query.direction.unwrap_or(DependencyDirection::Both),
                    include_source: query.include_source,
                    max_nodes: query.max_nodes.unwrap_or_else(dependency_graph_max_nodes),
                    max_bytes: dependency_graph_max_bytes(),
                },
            )?;
            let node_count = text
                .lines()
                .filter(|line| line.trim_start().starts_with("- [d"))
                .count();
            Ok(ExecutedBatchQuery {
                text,
                telemetry: Some(BatchTelemetry::Skeleton {
                    target_dir: format!("dependency_graph:{repo_id}:{}", query.target),
                    node_count,
                }),
            })
        }
    }
}

fn absolute_symbol_file_path(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: Option<&str>,
) -> String {
    if let Some(filepath) = filepath {
        return conn
            .query_row(
                "SELECT root_path FROM repositories WHERE id = ?1 LIMIT 1",
                rusqlite::params![repo_id],
                |row| {
                    let root_path: String = row.get(0)?;
                    Ok(PathBuf::from(root_path)
                        .join(filepath)
                        .to_string_lossy()
                        .to_string())
                },
            )
            .unwrap_or_else(|_| filepath.to_string());
    }

    conn.query_row(
        "SELECT n.file_path, r.root_path
         FROM nodes n
         JOIN repositories r ON r.id = n.repo_id
         WHERE n.symbol_name = ?1 AND n.repo_id = ?2
         ORDER BY n.file_path ASC, n.id ASC
         LIMIT 1",
        rusqlite::params![symbol_name, repo_id],
        |row| {
            let file_path: String = row.get(0)?;
            let root_path: String = row.get(1)?;
            Ok(PathBuf::from(root_path)
                .join(file_path)
                .to_string_lossy()
                .to_string())
        },
    )
    .unwrap_or_else(|_| symbol_name.to_string())
}

fn format_batch_section(query: &BatchQuery, ordinal: usize, total: usize, body: &str) -> String {
    let mut title = format!(
        "## [{ordinal}/{total}] {} — {}",
        query.intent.section_label(),
        query.target
    );
    if let Some(filepath) = &query.filepath {
        write!(title, " ({filepath})").ok();
    }
    format!("{title}\n{body}")
}

fn cap_batch_sections_fifo(sections: &[String], max_bytes: usize) -> (String, bool) {
    if max_bytes == 0 {
        return (String::new(), !sections.is_empty());
    }

    let mut out = String::new();
    for (idx, section) in sections.iter().enumerate() {
        let separator = if out.is_empty() { "" } else { "\n\n" };
        if out.len() + separator.len() + section.len() <= max_bytes {
            out.push_str(separator);
            out.push_str(section);
            continue;
        }

        let indexes = (idx + 1..=sections.len())
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let summary = format!(
            "\n\n[Batch output truncated at {max_bytes} bytes. Truncated/skipped query indexes: {indexes}.]"
        );
        if summary.len() >= max_bytes {
            return (prefix_by_bytes(&summary, max_bytes).to_string(), true);
        }

        out.push_str(separator);
        let remaining = max_bytes
            .saturating_sub(out.len())
            .saturating_sub(summary.len());
        out.push_str(prefix_by_bytes(section, remaining));
        if out.len() + summary.len() > max_bytes {
            let keep = max_bytes - summary.len();
            out = prefix_by_bytes(&out, keep).to_string();
        }
        out.push_str(&summary);
        return (out, true);
    }

    (out, false)
}

#[derive(Debug)]
struct GraphEntry {
    parent_index: Option<usize>,
    depth: usize,
    relationship_type: String,
    node: NodeBrief,
    already_seen: bool,
}

pub fn dependency_graph(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: Option<&str>,
    mut options: DependencyGraphOptions,
) -> Result<String> {
    if options.depth == 0 || options.depth > 5 {
        return Err(anyhow!("dependency_graph depth must be between 1 and 5"));
    }
    options.max_nodes = options.max_nodes.min(dependency_graph_max_nodes()).max(1);
    options.max_bytes = options.max_bytes.min(dependency_graph_max_bytes()).max(1);

    let root = match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
        SymbolResolution::Unique(row) => NodeBrief::from(row),
        SymbolResolution::Ambiguous(payload) => return Ok(payload),
    };

    let mut visited = HashSet::new();
    visited.insert(root.id.clone());
    let mut traversal_truncated = false;

    let (callers, caller_edges) = if matches!(
        options.direction,
        DependencyDirection::Callers | DependencyDirection::Both
    ) {
        traverse_dependency_direction(
            conn,
            &root.id,
            DependencyDirection::Callers,
            options.depth,
            options.max_nodes,
            &mut visited,
            &mut traversal_truncated,
        )?
    } else {
        (Vec::new(), 0)
    };
    let (callees, callee_edges) = if matches!(
        options.direction,
        DependencyDirection::Callees | DependencyDirection::Both
    ) {
        traverse_dependency_direction(
            conn,
            &root.id,
            DependencyDirection::Callees,
            options.depth,
            options.max_nodes,
            &mut visited,
            &mut traversal_truncated,
        )?
    } else {
        (Vec::new(), 0)
    };

    let mut unique_nodes = HashSet::new();
    unique_nodes.insert(root.id.clone());
    for entry in callers.iter().chain(callees.iter()) {
        unique_nodes.insert(entry.node.id.clone());
    }

    let mut out = String::new();
    writeln!(
        out,
        "DEPENDENCY GRAPH — {} (depth={}, direction={})",
        root.symbol_name,
        options.depth,
        options.direction.label()
    )
    .ok();
    writeln!(
        out,
        "\nROOT: {} ({}) — {}",
        root.symbol_name, root.symbol_type, root.file_path
    )
    .ok();
    if options.include_source {
        append_condensed_source(&mut out, &root, "  ");
    }
    if matches!(
        options.direction,
        DependencyDirection::Callers | DependencyDirection::Both
    ) {
        append_graph_entries(
            &mut out,
            "CALLERS (inbound):",
            &callers,
            options.include_source,
        );
    }
    if matches!(
        options.direction,
        DependencyDirection::Callees | DependencyDirection::Both
    ) {
        append_graph_entries(
            &mut out,
            "CALLEES (outbound):",
            &callees,
            options.include_source,
        );
    }

    let edge_count = caller_edges + callee_edges;
    let mut output_truncated = out.len() > options.max_bytes;
    let mut summary = format!(
        "\n{} nodes, {} edges. Traversal truncated: {}. Output truncated: {}.",
        unique_nodes.len(),
        edge_count,
        yes_no(traversal_truncated),
        yes_no(output_truncated)
    );
    if out.len() + summary.len() > options.max_bytes {
        output_truncated = true;
        summary = format!(
            "\n{} nodes, {} edges. Traversal truncated: {}. Output truncated: {}.",
            unique_nodes.len(),
            edge_count,
            yes_no(traversal_truncated),
            yes_no(output_truncated)
        );
    }
    out.push_str(&summary);

    if out.len() > options.max_bytes {
        let note = format!(
            "\n[Dependency graph output truncated at {} bytes.]{}",
            options.max_bytes, summary
        );
        if note.len() >= options.max_bytes {
            return Ok(prefix_by_bytes(&note, options.max_bytes).to_string());
        }
        let keep = options.max_bytes - note.len();
        let mut truncated = prefix_by_bytes(&out, keep).to_string();
        truncated.push_str(&note);
        return Ok(truncated);
    }

    Ok(out)
}

fn traverse_dependency_direction(
    conn: &Connection,
    root_id: &str,
    direction: DependencyDirection,
    max_depth: usize,
    max_nodes: usize,
    visited: &mut HashSet<String>,
    traversal_truncated: &mut bool,
) -> Result<(Vec<GraphEntry>, usize)> {
    let mut entries = Vec::new();
    let mut queue = VecDeque::new();
    let mut edge_count = 0usize;

    queue.push_back((root_id.to_string(), 0usize, None));

    while let Some((current_id, depth, parent_index)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }

        for (relationship_type, node) in fetch_dependency_neighbors(conn, &current_id, direction)? {
            edge_count += 1;
            let next_depth = depth + 1;
            if visited.contains(&node.id) {
                entries.push(GraphEntry {
                    parent_index,
                    depth: next_depth,
                    relationship_type,
                    node,
                    already_seen: true,
                });
                continue;
            }
            if visited.len() >= max_nodes {
                *traversal_truncated = true;
                continue;
            }
            visited.insert(node.id.clone());
            let entry_index = entries.len();
            queue.push_back((node.id.clone(), next_depth, Some(entry_index)));
            entries.push(GraphEntry {
                parent_index,
                depth: next_depth,
                relationship_type,
                node,
                already_seen: false,
            });
        }
    }

    Ok((entries, edge_count))
}

fn fetch_dependency_neighbors(
    conn: &Connection,
    current_id: &str,
    direction: DependencyDirection,
) -> Result<Vec<(String, NodeBrief)>> {
    let sql = match direction {
        DependencyDirection::Callers => {
            "SELECT e.relationship_type, n.id, n.symbol_name, n.symbol_type, n.file_path, n.language, n.raw_text
             FROM edges e
             JOIN nodes n ON n.id = e.source_id
             WHERE e.target_id = ?1
             ORDER BY e.relationship_type ASC, n.symbol_name ASC, n.file_path ASC, n.id ASC"
        }
        DependencyDirection::Callees => {
            "SELECT e.relationship_type, n.id, n.symbol_name, n.symbol_type, n.file_path, n.language, n.raw_text
             FROM edges e
             JOIN nodes n ON n.id = e.target_id
             WHERE e.source_id = ?1
             ORDER BY e.relationship_type ASC, n.symbol_name ASC, n.file_path ASC, n.id ASC"
        }
        DependencyDirection::Both => return Ok(Vec::new()),
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt
        .query_map(rusqlite::params![current_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                NodeBrief {
                    id: row.get(1)?,
                    symbol_name: row.get(2)?,
                    symbol_type: row.get(3)?,
                    file_path: row.get(4)?,
                    language: row.get(5)?,
                    raw_text: row.get(6)?,
                },
            ))
        })?
        .filter_map(|row| row.ok())
        .collect();
    Ok(rows)
}

fn append_graph_entries(
    out: &mut String,
    title: &str,
    entries: &[GraphEntry],
    include_source: bool,
) {
    writeln!(out, "\n{title}").ok();
    if entries.is_empty() {
        writeln!(out, "  (none)").ok();
        return;
    }

    for (idx, entry) in entries.iter().enumerate() {
        if entry.parent_index.is_none() {
            append_graph_entry_tree(out, entries, idx, include_source, 1);
        }
    }
}

fn append_graph_entry_tree(
    out: &mut String,
    entries: &[GraphEntry],
    index: usize,
    include_source: bool,
    level: usize,
) {
    let entry = &entries[index];
    let indent = "  ".repeat(level);
    let seen = if entry.already_seen {
        " (already seen)"
    } else {
        ""
    };
    writeln!(
        out,
        "{indent}- [d{}] [{}] {} ({}) — {}{}",
        entry.depth,
        entry.relationship_type,
        entry.node.symbol_name,
        entry.node.symbol_type,
        entry.node.file_path,
        seen
    )
    .ok();
    if include_source && !entry.already_seen {
        append_condensed_source(out, &entry.node, &format!("{indent}  "));
    }

    for (child_index, child) in entries.iter().enumerate() {
        if child.parent_index == Some(index) {
            append_graph_entry_tree(out, entries, child_index, include_source, level + 1);
        }
    }
}

fn append_condensed_source(out: &mut String, node: &NodeBrief, indent: &str) {
    let source = condense(&node.raw_text, &node.language);
    writeln!(out, "{indent}SOURCE:").ok();
    for line in source.lines() {
        writeln!(out, "{indent}{line}").ok();
    }
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "yes"
    } else {
        "no"
    }
}

pub fn map_class(
    conn: &Connection,
    repo_id: &str,
    symbol_name: &str,
    filepath: Option<&str>,
) -> Result<String> {
    let root = match resolve_symbol_or_disambiguate(conn, symbol_name, repo_id, filepath)? {
        SymbolResolution::Unique(row) => NodeBrief::from(row),
        SymbolResolution::Ambiguous(payload) => return Ok(payload),
    };
    let capsule = get_context_capsule(conn, symbol_name, repo_id, Some(&root.file_path))?;
    let graph = dependency_graph(
        conn,
        repo_id,
        symbol_name,
        Some(&root.file_path),
        DependencyGraphOptions {
            depth: 2,
            direction: DependencyDirection::Both,
            include_source: false,
            max_nodes: dependency_graph_max_nodes(),
            max_bytes: dependency_graph_max_bytes(),
        },
    )?;
    let same_file_symbols = same_file_symbols(conn, repo_id, &root.file_path, &root.id)?;
    let categories = dependency_path_categories(conn, &root.id)?;

    let mut out = String::new();
    writeln!(
        out,
        "CLASS MAP — {} ({}) — {}",
        root.symbol_name, root.symbol_type, root.file_path
    )
    .ok();
    writeln!(out, "\n## CAPSULE\n{}", capsule.optimized_text).ok();
    writeln!(out, "\n## DEPENDENCY GRAPH\n{graph}").ok();
    writeln!(out, "\n## SAME-FILE SYMBOLS").ok();
    if same_file_symbols.is_empty() {
        writeln!(out, "(none)").ok();
    } else {
        for (symbol_type, name) in same_file_symbols {
            writeln!(out, "- {symbol_type}: {name}").ok();
        }
    }
    writeln!(out, "\n## PATH CATEGORIES").ok();
    for category in [
        "controllers",
        "jobs",
        "services",
        "models",
        "tests",
        "other",
    ] {
        let paths = categories.get(category).cloned().unwrap_or_default();
        if paths.is_empty() {
            writeln!(out, "- {category}: (none)").ok();
        } else {
            writeln!(out, "- {category}: {}", paths.join(", ")).ok();
        }
    }

    Ok(out)
}

fn same_file_symbols(
    conn: &Connection,
    repo_id: &str,
    file_path: &str,
    pivot_id: &str,
) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT symbol_type, symbol_name
         FROM nodes
         WHERE repo_id = ?1 AND file_path = ?2 AND id != ?3
         ORDER BY symbol_type ASC, symbol_name ASC, id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![repo_id, file_path, pivot_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .filter_map(|row| row.ok())
        .collect();
    Ok(rows)
}

fn dependency_path_categories(
    conn: &Connection,
    root_id: &str,
) -> Result<BTreeMap<&'static str, Vec<String>>> {
    let mut categories: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    let mut stmt = conn.prepare(
        "SELECT DISTINCT n.file_path
         FROM edges e
         JOIN nodes n ON n.id = e.source_id
         WHERE e.target_id = ?1
         UNION
         SELECT DISTINCT n.file_path
         FROM edges e
         JOIN nodes n ON n.id = e.target_id
         WHERE e.source_id = ?1
         ORDER BY file_path ASC",
    )?;
    for path in stmt
        .query_map(rusqlite::params![root_id], |row| row.get::<_, String>(0))?
        .filter_map(|row| row.ok())
    {
        categories
            .entry(path_category(&path))
            .or_default()
            .push(path);
    }
    Ok(categories)
}

fn path_category(path: &str) -> &'static str {
    let lowered = path.to_ascii_lowercase();
    for (category, needle) in [
        ("controllers", "controllers"),
        ("jobs", "jobs"),
        ("services", "services"),
        ("models", "models"),
        ("tests", "tests"),
        ("tests", "spec"),
    ] {
        if lowered.split('/').any(|segment| segment == needle) {
            return category;
        }
    }
    "other"
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
                let file_tokens = payload.len() / 4;
                return Ok(CapsuleResult {
                    optimized_text: payload.clone(),
                    original_text: String::new(),
                    file_tokens,
                    proof_snapshot: None,
                    provenance: CapsuleProvenance {
                        baseline_token_source: "unavailable".to_string(),
                        tokenizer_mode: "chars/4".to_string(),
                        original_mode: "none".to_string(),
                        proof_label: "unavailable".to_string(),
                        precise_file_tokens: false,
                        original_max_bytes: None,
                        proof_max_bytes: capsule_proof_max_bytes(),
                        proof_max_files: capsule_proof_max_files(),
                        touched_file_count: 0,
                    },
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
        .map(
            |(id, sym_name, sym_type, file_path, lang, raw_text, rel_type)| NeighborInfo {
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
            },
        )
        .collect();

    // Format the trace output: full pivot source + outbound-only signatures.
    let mut out = String::new();
    writeln!(
        out,
        "TRACE FLOW — pivot: {} ({})",
        pivot.symbol_name, pivot.language
    )
    .ok();
    writeln!(out, "File : {}", pivot.file_path).ok();
    writeln!(out, "Type : {}", pivot.symbol_type).ok();
    writeln!(
        out,
        "\n── FULL SOURCE ──────────────────────────────────────────────"
    )
    .ok();
    writeln!(out, "{}", pivot.text).ok();

    if outbound.is_empty() {
        writeln!(
            out,
            "── DIRECT CALLEES ─────────────────────────────────────────────"
        )
        .ok();
        writeln!(out, "  (leaf node — no direct outbound calls)").ok();
    } else {
        writeln!(
            out,
            "\n── DIRECT CALLEES (immediate outbound dependencies) ─────────────"
        )
        .ok();
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
                rel = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            )
            .ok();
            writeln!(out, "{}", n.node.text).ok();
        }
    }

    let file_tokens = out.len() / 4;
    Ok(CapsuleResult {
        optimized_text: out.clone(),
        original_text: String::new(),
        file_tokens,
        proof_snapshot: None,
        provenance: CapsuleProvenance {
            baseline_token_source: "trace_output".to_string(),
            tokenizer_mode: "chars/4".to_string(),
            original_mode: "none".to_string(),
            proof_label: "unavailable".to_string(),
            precise_file_tokens: false,
            original_max_bytes: None,
            proof_max_bytes: capsule_proof_max_bytes(),
            proof_max_files: capsule_proof_max_files(),
            touched_file_count: 0,
        },
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
pub const FIND_SYMBOL_DEFAULT_LIMIT: usize = 50;

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
        // Fold both the caller-supplied path and the stored column to forward
        // slashes so a POSIX-style `src/context.rs` matches a Windows-stored
        // `src\context.rs` (and vice versa) without requiring a re-ingest.
        let fp = crate::db::normalize_path_separators(fp);
        conn.prepare(
            "SELECT id, symbol_name, symbol_type, file_path, language, raw_text
             FROM nodes
             WHERE symbol_name = ?1 AND repo_id = ?2
               AND REPLACE(file_path, '\\', '/') = ?3
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
        0 => Err(anyhow!(
            "Symbol '{}' not found in repo '{}'",
            symbol_name,
            repo_id
        )),
        1 => Ok(SymbolResolution::Unique(
            candidates
                .into_iter()
                .next()
                .expect("single candidate must exist"),
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

pub fn find_symbols(
    conn: &Connection,
    repo_id: &str,
    query: &str,
    kind: Option<&str>,
    limit: usize,
) -> Result<String> {
    let query = query.trim();
    if query.is_empty() {
        return Ok("No symbol matches for ''. Provide a symbol fragment.".to_string());
    }

    let limit = limit.max(1);
    let fetch_limit = limit.saturating_add(1) as i64;
    let kind_like = kind.map(like_contains_pattern);
    let rows = if let Some(fts_query) = fts_prefix_query(query) {
        if let Some(kind_like) = kind_like.as_deref() {
            conn.prepare(
                "SELECT n.file_path, n.symbol_type, n.symbol_name
                 FROM nodes_fts
                 JOIN nodes n ON n.rowid = nodes_fts.rowid
                 WHERE nodes_fts MATCH ?1
                   AND n.repo_id = ?2
                   AND n.symbol_type LIKE ?3 ESCAPE '\\'
                 ORDER BY bm25(nodes_fts) ASC, n.symbol_name ASC, n.file_path ASC, n.rowid ASC
                 LIMIT ?4",
            )?
            .query_map(
                rusqlite::params![fts_query, repo_id, kind_like, fetch_limit],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?
            .filter_map(|row| row.ok())
            .collect::<Vec<(String, String, String)>>()
        } else {
            conn.prepare(
                "SELECT n.file_path, n.symbol_type, n.symbol_name
                 FROM nodes_fts
                 JOIN nodes n ON n.rowid = nodes_fts.rowid
                 WHERE nodes_fts MATCH ?1
                   AND n.repo_id = ?2
                 ORDER BY bm25(nodes_fts) ASC, n.symbol_name ASC, n.file_path ASC, n.rowid ASC
                 LIMIT ?3",
            )?
            .query_map(rusqlite::params![fts_query, repo_id, fetch_limit], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|row| row.ok())
            .collect::<Vec<(String, String, String)>>()
        }
    } else {
        let query_like = like_contains_pattern(query);
        if let Some(kind_like) = kind_like.as_deref() {
            conn.prepare(
                "SELECT file_path, symbol_type, symbol_name
                 FROM nodes
                 WHERE repo_id = ?1
                   AND symbol_name LIKE ?2 ESCAPE '\\'
                   AND symbol_type LIKE ?3 ESCAPE '\\'
                 ORDER BY length(symbol_name) ASC, symbol_name ASC, file_path ASC, rowid ASC
                 LIMIT ?4",
            )?
            .query_map(
                rusqlite::params![repo_id, query_like, kind_like, fetch_limit],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?
            .filter_map(|row| row.ok())
            .collect::<Vec<(String, String, String)>>()
        } else {
            conn.prepare(
                "SELECT file_path, symbol_type, symbol_name
                 FROM nodes
                 WHERE repo_id = ?1
                   AND symbol_name LIKE ?2 ESCAPE '\\'
                 ORDER BY length(symbol_name) ASC, symbol_name ASC, file_path ASC, rowid ASC
                 LIMIT ?3",
            )?
            .query_map(rusqlite::params![repo_id, query_like, fetch_limit], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|row| row.ok())
            .collect::<Vec<(String, String, String)>>()
        }
    };

    format_symbol_matches(repo_id, query, rows, limit)
}

fn fts_prefix_query(query: &str) -> Option<String> {
    let mut terms = Vec::new();
    let mut current = String::new();
    for ch in query.chars() {
        if ch.is_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            terms.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        terms.push(current);
    }
    if terms.is_empty() {
        None
    } else {
        Some(
            terms
                .into_iter()
                .map(|term| format!("{term}*"))
                .collect::<Vec<_>>()
                .join(" "),
        )
    }
}

fn like_contains_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len() + 2);
    escaped.push('%');
    for ch in value.chars() {
        match ch {
            '\\' | '%' | '_' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped.push('%');
    escaped
}

fn format_symbol_matches(
    repo_id: &str,
    query: &str,
    mut rows: Vec<(String, String, String)>,
    limit: usize,
) -> Result<String> {
    if rows.is_empty() {
        return Ok(format!(
            "No symbol matches for '{query}' in repo '{repo_id}'. Try a different fragment or run analyze_repo for a broader map."
        ));
    }

    let truncated = rows.len() > limit;
    if truncated {
        rows.truncate(limit);
    }

    let mut out = format!("Found {} matches for '{query}':\n", rows.len());
    for (file_path, symbol_type, symbol_name) in rows {
        out.push_str(&format!("- {file_path} ({symbol_type}: {symbol_name})\n"));
    }
    if truncated {
        out.push_str(&format!(
            "[... capped at {limit} matches. Narrow the query or add `kind` before exploring a symbol.]"
        ));
    }
    Ok(out)
}

fn resolve_repo_file_path(root_path: &Path, rel_path: &str) -> Result<PathBuf> {
    let rel = PathBuf::from(rel_path);
    if rel.is_absolute() {
        return Err(anyhow!(
            "Indexed file '{}' is outside the repository root and cannot be trusted.",
            rel_path
        ));
    }

    let root = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());
    let candidate = root.join(&rel);
    let canonical = candidate
        .canonicalize()
        .map_err(|e| anyhow!("Indexed file '{}' is missing on disk: {}", rel_path, e))?;
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
fn find_outermost_body(raw_text: &str, lang: Language, query_src: &str) -> Option<(usize, usize)> {
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
        return Ok(format!(
            "No symbols found for repo '{}'. The repository has not been indexed yet.\n\
                 Run the `ingest_repo` tool to build the AST graph before using `get_skeleton`.",
            repo_id
        ));
    }

    let base_sql = "SELECT file_path, symbol_type, symbol_name \
                    FROM nodes \
                    WHERE repo_id = ?1 \
                      AND (symbol_type LIKE '%function%' \
                        OR symbol_type LIKE '%class%' \
                        OR symbol_type LIKE '%struct%' \
                        OR symbol_type LIKE '%trait%' \
                        OR symbol_type LIKE '%interface%' \
                        OR symbol_type LIKE '%method%' \
                        OR symbol_type LIKE '%enum%' \
                        OR symbol_type LIKE '%impl%' \
                        OR symbol_type LIKE '%module%') \
                    ORDER BY file_path ASC, rowid ASC \
                    LIMIT ?2";

    let dir_sql = "SELECT file_path, symbol_type, symbol_name \
                   FROM nodes \
                   WHERE repo_id = ?1 \
                     AND (symbol_type LIKE '%function%' \
                       OR symbol_type LIKE '%class%' \
                       OR symbol_type LIKE '%struct%' \
                       OR symbol_type LIKE '%trait%' \
                       OR symbol_type LIKE '%interface%' \
                       OR symbol_type LIKE '%method%' \
                       OR symbol_type LIKE '%enum%' \
                       OR symbol_type LIKE '%impl%' \
                       OR symbol_type LIKE '%module%') \
                     AND (REPLACE(file_path, '\\', '/') = ?3 \
                       OR REPLACE(file_path, '\\', '/') LIKE ?4) \
                   ORDER BY file_path ASC, rowid ASC \
                   LIMIT ?2";

    let limit = SKELETON_ROW_LIMIT as i64;

    // Collect rows into a BTreeMap<file_path, Vec<(symbol_type, symbol_name)>>
    // so files appear in deterministic alphabetical order.
    let mut map: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut row_count: usize = 0;

    if let Some(dir) = target_dir {
        // Compare against the forward-slash-folded column (see dir_sql) so a
        // directory filter matches Windows-stored backslash paths too.
        let exact = crate::db::normalize_path_separators(dir.trim_end_matches('/'));
        let prefix = format!("{}/%", exact);
        let mut stmt = conn.prepare(dir_sql)?;
        let rows = stmt.query_map(rusqlite::params![repo_id, limit, exact, prefix], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows.filter_map(|r| r.ok()) {
            map.entry(row.0).or_default().push((row.1, row.2));
            row_count += 1;
        }
    } else {
        let mut stmt = conn.prepare(base_sql)?;
        let rows = stmt.query_map(rusqlite::params![repo_id, limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        for row in rows.filter_map(|r| r.ok()) {
            map.entry(row.0).or_default().push((row.1, row.2));
            row_count += 1;
        }
    }

    if map.is_empty() {
        return Ok("No matching symbols found for the given filter.\n\
             Try a different `target_dir` or check that the repo is indexed."
            .to_string());
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
    use std::sync::Mutex;
    use tempfile::tempdir;

    static CAPSULE_ENV_LOCK: Mutex<()> = Mutex::new(());

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
             CREATE VIRTUAL TABLE nodes_fts USING fts5(
                 symbol_name,
                 content='nodes',
                 content_rowid='rowid',
                 tokenize='unicode61 remove_diacritics 2'
             );
             CREATE TRIGGER nodes_fts_ai AFTER INSERT ON nodes BEGIN
                 INSERT INTO nodes_fts(rowid, symbol_name) VALUES (new.rowid, new.symbol_name);
             END;
             CREATE TRIGGER nodes_fts_ad AFTER DELETE ON nodes BEGIN
                 INSERT INTO nodes_fts(nodes_fts, rowid, symbol_name) VALUES('delete', old.rowid, old.symbol_name);
             END;
             CREATE TRIGGER nodes_fts_au AFTER UPDATE ON nodes BEGIN
                 INSERT INTO nodes_fts(nodes_fts, rowid, symbol_name) VALUES('delete', old.rowid, old.symbol_name);
                 INSERT INTO nodes_fts(rowid, symbol_name) VALUES (new.rowid, new.symbol_name);
             END;
             CREATE TABLE observations (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 repo_id TEXT NOT NULL DEFAULT '',
                 symbol_name TEXT NOT NULL,
                 filepath TEXT NOT NULL,
                 observation_text TEXT NOT NULL,
                 timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                 last_known_hash TEXT NOT NULL,
                 is_stale INTEGER NOT NULL DEFAULT 0
             );
             CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
             CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);",
        )
        .unwrap();
        conn
    }

    #[allow(clippy::too_many_arguments)]
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

    fn default_batch_query(intent: BatchIntent, target: &str) -> BatchQuery {
        BatchQuery {
            intent,
            target: target.to_string(),
            filepath: None,
            kind: None,
            limit: None,
            depth: None,
            direction: None,
            include_source: false,
            max_nodes: None,
        }
    }

    fn default_graph_options() -> DependencyGraphOptions {
        DependencyGraphOptions {
            max_bytes: 100_000,
            ..DependencyGraphOptions::default()
        }
    }

    // ── compound queries ─────────────────────────────────────────────────────

    #[test]
    fn execute_batch_queries_deduplicates_exact_repeated_queries() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/lib.rs:Report",
            "r",
            "src/lib.rs",
            "rs",
            "Report",
            "struct",
            "struct Report { id: i64 }",
        );

        let queries = vec![
            default_batch_query(BatchIntent::parse("capsule").unwrap(), "Report"),
            default_batch_query(BatchIntent::ExploreSymbol, "Report"),
        ];
        let result = execute_batch_queries(
            &conn,
            &queries,
            BatchOptions {
                repo_id: "r".to_string(),
                max_bytes: 100_000,
            },
        )
        .unwrap();

        assert!(result.text.contains("CONTEXT CAPSULE"), "{}", result.text);
        assert!(
            result.text.contains("Same as [1/2] above"),
            "{}",
            result.text
        );
    }

    #[test]
    fn execute_batch_queries_deduplicates_impact_aliases() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/a.rs:A",
            "r",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() { B(); }",
        );
        insert_node(
            &conn,
            "r:src/b.rs:B",
            "r",
            "src/b.rs",
            "rs",
            "B",
            "function",
            "fn B() {}",
        );
        insert_edge(&conn, "r:src/a.rs:A", "r:src/b.rs:B", "CALLS");

        let queries = vec![
            default_batch_query(BatchIntent::parse("analyze_impact").unwrap(), "B"),
            default_batch_query(BatchIntent::RefactorSymbol, "B"),
        ];
        let result = execute_batch_queries(
            &conn,
            &queries,
            BatchOptions {
                repo_id: "r".to_string(),
                max_bytes: 100_000,
            },
        )
        .unwrap();

        assert!(result.text.contains("IMPACT"), "{}", result.text);
        assert!(
            result.text.contains("Same as [1/2] above"),
            "{}",
            result.text
        );
    }

    #[test]
    fn execute_batch_queries_keeps_distinct_graph_options_separate() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        for name in ["A", "B", "C"] {
            insert_node(
                &conn,
                &format!("r:src/{name}.rs:{name}"),
                "r",
                &format!("src/{name}.rs"),
                "rs",
                name,
                "function",
                &format!("fn {name}() {{}}"),
            );
        }
        insert_edge(&conn, "r:src/A.rs:A", "r:src/B.rs:B", "CALLS");
        insert_edge(&conn, "r:src/B.rs:B", "r:src/C.rs:C", "CALLS");

        let mut depth_one = default_batch_query(BatchIntent::DependencyGraph, "A");
        depth_one.depth = Some(1);
        let mut depth_two = default_batch_query(BatchIntent::DependencyGraph, "A");
        depth_two.depth = Some(2);
        let mut include_source = default_batch_query(BatchIntent::DependencyGraph, "A");
        include_source.depth = Some(1);
        include_source.include_source = true;
        let mut capped = default_batch_query(BatchIntent::DependencyGraph, "A");
        capped.depth = Some(1);
        capped.max_nodes = Some(2);

        let result = execute_batch_queries(
            &conn,
            &[depth_one, depth_two, include_source, capped],
            BatchOptions {
                repo_id: "r".to_string(),
                max_bytes: 100_000,
            },
        )
        .unwrap();

        assert!(!result.text.contains("Same as"), "{}", result.text);
        assert!(
            result.text.contains("## [4/4] DEPENDENCY GRAPH — A"),
            "{}",
            result.text
        );
    }

    #[test]
    fn execute_batch_queries_isolates_symbol_errors() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/a.rs:A",
            "r",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() {}",
        );
        insert_node(
            &conn,
            "r:src/b.rs:B",
            "r",
            "src/b.rs",
            "rs",
            "B",
            "function",
            "fn B() {}",
        );

        let queries = vec![
            default_batch_query(BatchIntent::ExploreSymbol, "A"),
            default_batch_query(BatchIntent::ExploreSymbol, "Missing"),
            default_batch_query(BatchIntent::ExploreSymbol, "B"),
        ];
        let result = execute_batch_queries(
            &conn,
            &queries,
            BatchOptions {
                repo_id: "r".to_string(),
                max_bytes: 100_000,
            },
        )
        .unwrap();

        assert!(result.text.contains("## [2/3] EXPLORE — Missing"));
        assert!(result.text.contains("ERROR: Symbol 'Missing' not found"));
        assert!(result.text.contains("## [3/3] EXPLORE — B"));
    }

    #[test]
    fn execute_batch_queries_truncates_fifo_at_byte_cap() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/small.rs:Small",
            "r",
            "src/small.rs",
            "rs",
            "Small",
            "function",
            "fn Small() {}",
        );
        insert_node(
            &conn,
            "r:src/large.rs:Large",
            "r",
            "src/large.rs",
            "rs",
            "Large",
            "function",
            &format!(
                "fn Large() {{\n{}\n}}",
                "let value = \"\u{2603}\";".repeat(200)
            ),
        );

        let queries = vec![
            default_batch_query(BatchIntent::ExploreSymbol, "Small"),
            default_batch_query(BatchIntent::ExploreSymbol, "Large"),
        ];
        let result = execute_batch_queries(
            &conn,
            &queries,
            BatchOptions {
                repo_id: "r".to_string(),
                max_bytes: 512,
            },
        )
        .unwrap();

        assert!(result.truncated);
        assert!(result.text.contains("Small"), "{}", result.text);
        assert!(
            result.text.contains("Truncated/skipped query indexes: 2"),
            "{}",
            result.text
        );
        assert!(std::str::from_utf8(result.text.as_bytes()).is_ok());
    }

    #[test]
    fn dependency_graph_respects_depth_and_direction() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        for name in ["A", "B", "C", "D"] {
            insert_node(
                &conn,
                &format!("r:src/{name}.rs:{name}"),
                "r",
                &format!("src/{name}.rs"),
                "rs",
                name,
                "function",
                &format!("fn {name}() {{}}"),
            );
        }
        insert_edge(&conn, "r:src/A.rs:A", "r:src/B.rs:B", "CALLS");
        insert_edge(&conn, "r:src/B.rs:B", "r:src/C.rs:C", "CALLS");
        insert_edge(&conn, "r:src/D.rs:D", "r:src/A.rs:A", "CALLS");

        let depth_one = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 1,
                direction: DependencyDirection::Callees,
                ..default_graph_options()
            },
        )
        .unwrap();
        assert!(depth_one.contains("B (function)"), "{depth_one}");
        assert!(!depth_one.contains("C (function)"), "{depth_one}");
        assert!(!depth_one.contains("D (function)"), "{depth_one}");

        let depth_two = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 2,
                direction: DependencyDirection::Callees,
                ..default_graph_options()
            },
        )
        .unwrap();
        assert!(depth_two.contains("C (function)"), "{depth_two}");

        let callers = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 1,
                direction: DependencyDirection::Callers,
                ..default_graph_options()
            },
        )
        .unwrap();
        assert!(callers.contains("D (function)"), "{callers}");
        assert!(!callers.contains("B (function)"), "{callers}");
    }

    #[test]
    fn dependency_graph_uses_shared_max_nodes_for_both_directions() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        for name in ["A", "B", "D"] {
            insert_node(
                &conn,
                &format!("r:src/{name}.rs:{name}"),
                "r",
                &format!("src/{name}.rs"),
                "rs",
                name,
                "function",
                &format!("fn {name}() {{}}"),
            );
        }
        insert_edge(&conn, "r:src/D.rs:D", "r:src/A.rs:A", "CALLS");
        insert_edge(&conn, "r:src/A.rs:A", "r:src/B.rs:B", "CALLS");

        let graph = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 1,
                direction: DependencyDirection::Both,
                max_nodes: 2,
                ..default_graph_options()
            },
        )
        .unwrap();

        let rendered_nodes = graph
            .lines()
            .filter(|line| line.trim_start().starts_with("- [d"))
            .count();
        assert_eq!(rendered_nodes, 1, "{graph}");
        assert!(graph.contains("D (function)"), "{graph}");
        assert!(!graph.contains("B (function)"), "{graph}");
        assert!(graph.contains("2 nodes"), "{graph}");
        assert!(graph.contains("Traversal truncated: yes"), "{graph}");
    }

    #[test]
    fn dependency_graph_renders_depth_two_as_nested_tree() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        for name in ["A", "B", "C", "D"] {
            insert_node(
                &conn,
                &format!("r:src/{name}.rs:{name}"),
                "r",
                &format!("src/{name}.rs"),
                "rs",
                name,
                "function",
                &format!("fn {name}() {{}}"),
            );
        }
        insert_edge(&conn, "r:src/A.rs:A", "r:src/B.rs:B", "CALLS");
        insert_edge(&conn, "r:src/B.rs:B", "r:src/C.rs:C", "CALLS");
        insert_edge(&conn, "r:src/A.rs:A", "r:src/D.rs:D", "CALLS");

        let graph = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 2,
                direction: DependencyDirection::Callees,
                ..default_graph_options()
            },
        )
        .unwrap();

        assert!(
            graph.contains("  - [d1] [CALLS] B (function) — src/B.rs"),
            "{graph}"
        );
        assert!(
            graph.contains("    - [d2] [CALLS] C (function) — src/C.rs"),
            "{graph}"
        );
        assert!(
            graph.contains("  - [d1] [CALLS] D (function) — src/D.rs"),
            "{graph}"
        );
    }

    #[test]
    fn dependency_graph_handles_cycles_without_repeating_forever() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/a.rs:A",
            "r",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() {}",
        );
        insert_node(
            &conn,
            "r:src/b.rs:B",
            "r",
            "src/b.rs",
            "rs",
            "B",
            "function",
            "fn B() {}",
        );
        insert_edge(&conn, "r:src/a.rs:A", "r:src/b.rs:B", "CALLS");
        insert_edge(&conn, "r:src/b.rs:B", "r:src/a.rs:A", "CALLS");

        let graph = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 3,
                direction: DependencyDirection::Callees,
                ..default_graph_options()
            },
        )
        .unwrap();

        assert!(graph.contains("B (function)"), "{graph}");
        assert!(graph.contains("already seen"), "{graph}");
    }

    #[test]
    fn dependency_graph_include_source_uses_condensed_text() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/a.rs:A",
            "r",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() {\n    let alpha = 1;\n    let beta = alpha + 1;\n}",
        );
        insert_node(
            &conn,
            "r:src/b.rs:B",
            "r",
            "src/b.rs",
            "rs",
            "B",
            "function",
            "fn B() {\n    let gamma = 3;\n}",
        );
        insert_edge(&conn, "r:src/a.rs:A", "r:src/b.rs:B", "CALLS");

        let graph = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 1,
                direction: DependencyDirection::Callees,
                include_source: true,
                ..default_graph_options()
            },
        )
        .unwrap();

        assert!(graph.contains("SOURCE:"), "{graph}");
        assert!(!graph.contains("let gamma = 3"), "{graph}");
    }

    #[test]
    fn dependency_graph_output_cap_is_utf8_safe_and_reports_truncation() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/a.rs:A",
            "r",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() {}",
        );
        for name in ["Wide\u{2603}One", "Wide\u{2603}Two", "Wide\u{2603}Three"] {
            insert_node(
                &conn,
                &format!("r:src/{name}.rs:{name}"),
                "r",
                &format!("src/{name}.rs"),
                "rs",
                name,
                "function",
                &format!("fn {name}() {{}}"),
            );
            insert_edge(
                &conn,
                "r:src/a.rs:A",
                &format!("r:src/{name}.rs:{name}"),
                "CALLS",
            );
        }

        let graph = dependency_graph(
            &conn,
            "r",
            "A",
            None,
            DependencyGraphOptions {
                depth: 1,
                direction: DependencyDirection::Callees,
                max_bytes: 200,
                ..default_graph_options()
            },
        )
        .unwrap();

        assert!(graph.len() <= 200, "{graph}");
        assert!(std::str::from_utf8(graph.as_bytes()).is_ok());
        assert!(
            graph.contains("Dependency graph output truncated"),
            "{graph}"
        );
    }

    #[test]
    fn map_class_combines_capsule_graph_and_same_file_symbols() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:app/models/report.rb:Report",
            "r",
            "app/models/report.rb",
            "rb",
            "Report",
            "class",
            "class Report\nend",
        );
        insert_node(
            &conn,
            "r:app/models/report.rb:create_job",
            "r",
            "app/models/report.rb",
            "rb",
            "create_job",
            "method",
            "def create_job\nend",
        );
        insert_node(
            &conn,
            "r:app/controllers/reports_controller.rb:create",
            "r",
            "app/controllers/reports_controller.rb",
            "rb",
            "create",
            "method",
            "def create\nend",
        );
        insert_node(
            &conn,
            "r:app/jobs/run_report_job.rb:RunReportJob",
            "r",
            "app/jobs/run_report_job.rb",
            "rb",
            "RunReportJob",
            "class",
            "class RunReportJob\nend",
        );
        insert_edge(
            &conn,
            "r:app/controllers/reports_controller.rb:create",
            "r:app/models/report.rb:Report",
            "CALLS",
        );
        insert_edge(
            &conn,
            "r:app/models/report.rb:Report",
            "r:app/jobs/run_report_job.rb:RunReportJob",
            "CALLS",
        );

        let out = map_class(&conn, "r", "Report", None).unwrap();
        assert!(out.contains("CLASS MAP"), "{out}");
        assert!(out.contains("## CAPSULE"), "{out}");
        assert!(out.contains("## DEPENDENCY GRAPH"), "{out}");
        assert!(out.contains("## SAME-FILE SYMBOLS"), "{out}");
        assert!(out.contains("method: create_job"), "{out}");
        assert!(
            out.contains("controllers: app/controllers/reports_controller.rb"),
            "{out}"
        );
        assert!(out.contains("jobs: app/jobs/run_report_job.rb"), "{out}");
    }

    #[test]
    fn map_class_does_not_require_framework_specific_edges() {
        let conn = make_db();
        insert_repo(&conn, "r", "/tmp/r");
        insert_node(
            &conn,
            "r:src/domain.rs:Widget",
            "r",
            "src/domain.rs",
            "rs",
            "Widget",
            "struct",
            "struct Widget { id: i64 }",
        );
        insert_node(
            &conn,
            "r:src/domain.rs:new",
            "r",
            "src/domain.rs",
            "rs",
            "new",
            "function",
            "fn new() -> Widget { Widget { id: 1 } }",
        );

        let out = map_class(&conn, "r", "Widget", None).unwrap();
        assert!(out.contains("CLASS MAP"), "{out}");
        assert!(out.contains("function: new"), "{out}");
        assert!(out.contains("other:"), "{out}");
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
        assert!(
            result.optimized_text.contains("foo"),
            "symbol name missing: {}",
            result.optimized_text
        );
        assert!(
            result
                .optimized_text
                .contains("def foo():\n    return 42\n"),
            "pivot text missing"
        );
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
            "isolated marker missing: {}",
            result.optimized_text
        );
        assert!(
            !result.optimized_text.contains("Expand a neighbor"),
            "isolated capsule should omit expansion CTA: {}",
            result.optimized_text
        );
    }

    #[test]
    fn capsule_default_outbound_cap_is_50_and_env_override_still_works() {
        let _g = CAPSULE_ENV_LOCK.lock().unwrap();
        std::env::remove_var("MARROW_CAPSULE_MAX_OUTBOUND");
        assert_eq!(capsule_max_outbound_neighbors(), 50);

        std::env::set_var("MARROW_CAPSULE_MAX_OUTBOUND", "7");
        assert_eq!(capsule_max_outbound_neighbors(), 7);
        std::env::remove_var("MARROW_CAPSULE_MAX_OUTBOUND");
    }

    #[test]
    fn capsule_session_memories_are_compact_with_shared_stale_footer() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:remembered",
            "r",
            "f.py",
            "py",
            "remembered",
            "function",
            "def remembered(): pass\n",
        );
        conn.execute(
            "INSERT INTO observations (repo_id, symbol_name, filepath, observation_text, timestamp, last_known_hash, is_stale)
             VALUES
                ('r', 'remembered', 'f.py', 'fresh note', '2026-01-01 00:00:00', 'a', 0),
                ('r', 'remembered', 'f.py', 'stale note\nwith newline', '2026-01-02 00:00:00', 'b', 1)",
            [],
        )
        .unwrap();

        let result = get_context_capsule(&conn, "remembered", "r", None).unwrap();
        let text = result.optimized_text;
        assert!(text.contains("- fresh note  (recorded: 2026-01-01 00:00:00)"));
        assert!(text.contains("- stale note with newline  (recorded: 2026-01-02 00:00:00, STALE)"));
        assert_eq!(
            text.matches("items marked STALE").count(),
            1,
            "stale footer should be emitted once: {text}"
        );
        assert!(!text.contains("[STALE MEMORY WARNING"));
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
        assert!(
            !text.contains("return x"),
            "neighbor body must not appear in progressive output, got: {text}"
        );
        assert!(
            text.contains("def bar"),
            "neighbor signature must be preserved, got: {text}"
        );
        assert!(
            text.contains("CALLS"),
            "relationship type must appear, got: {text}"
        );
        assert!(
            !text.contains("use read_node to expand"),
            "outbound section header must not include expansion guidance, got: {text}"
        );
        for forbidden in [
            "strictly forbidden",
            "forbidden from using",
            "grep",
            "read_file",
        ] {
            assert!(
                !text.contains(forbidden),
                "capsule CTA must not forbid native file reads using {forbidden:?}, got: {text}"
            );
        }
        assert_eq!(
            text.matches("read_node").count(),
            1,
            "outbound capsule must emit exactly one read_node guidance line, got: {text}"
        );
        assert!(
            text.contains(
                "[Expand a neighbor: run_pipeline(intent: \"read_node\", target: \"<symbol>\")]"
            ),
            "CTA for read_node must be present as a single line, got: {text}"
        );
    }

    #[test]
    fn capsule_inbound_only_omits_neighbor_expansion_guidance() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:caller",
            "r",
            "f.py",
            "py",
            "caller",
            "function",
            "def caller():\n    return callee()\n",
        );
        insert_node(
            &conn,
            "r:f.py:callee",
            "r",
            "f.py",
            "py",
            "callee",
            "function",
            "def callee():\n    return 42\n",
        );
        insert_edge(&conn, "r:f.py:caller", "r:f.py:callee", "CALLS");

        let result = get_context_capsule(&conn, "callee", "r", None).unwrap();
        let text = &result.optimized_text;
        assert!(
            text.contains("INBOUND CALLERS"),
            "inbound section missing, got: {text}"
        );
        assert_eq!(
            text.matches("read_node").count(),
            0,
            "capsule without outbound neighbors must omit read_node guidance, got: {text}"
        );
        assert!(
            !text.contains("Expand a neighbor"),
            "capsule without outbound neighbors must omit expansion CTA, got: {text}"
        );
    }

    #[test]
    fn capsule_at_outbound_cap_keeps_cap_note_and_single_expansion_cta() {
        let _g = CAPSULE_ENV_LOCK.lock().unwrap();
        std::env::set_var("MARROW_CAPSULE_MAX_OUTBOUND", "1");

        let conn = make_db();
        insert_node(
            &conn,
            "r:f.py:caller",
            "r",
            "f.py",
            "py",
            "caller",
            "function",
            "def caller():\n    first()\n    second()\n",
        );
        insert_node(
            &conn,
            "r:f.py:first",
            "r",
            "f.py",
            "py",
            "first",
            "function",
            "def first():\n    return 1\n",
        );
        insert_node(
            &conn,
            "r:f.py:second",
            "r",
            "f.py",
            "py",
            "second",
            "function",
            "def second():\n    return 2\n",
        );
        insert_edge(&conn, "r:f.py:caller", "r:f.py:first", "CALLS");
        insert_edge(&conn, "r:f.py:caller", "r:f.py:second", "CALLS");

        let result = get_context_capsule(&conn, "caller", "r", None).unwrap();
        std::env::remove_var("MARROW_CAPSULE_MAX_OUTBOUND");

        let text = &result.optimized_text;
        assert!(
            text.contains("[Note: at most 1 outbound neighbors loaded"),
            "outbound cap note missing: {text}"
        );
        assert_eq!(
            text.matches("Expand a neighbor").count(),
            1,
            "capped outbound capsule should still emit one expansion CTA: {text}"
        );
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
        assert!(
            !text.contains("x += 1"),
            "neighbor body must not appear in progressive output, got: {text}"
        );
        assert!(text.contains("helper"), "neighbor name missing: {text}");
        assert!(
            text.contains("void helper"),
            "C++ neighbor signature must appear, got: {text}"
        );
        assert_eq!(
            text.matches("read_node").count(),
            1,
            "outbound capsule must emit exactly one read_node guidance line, got: {text}"
        );
        assert!(
            text.contains(
                "[Expand a neighbor: run_pipeline(intent: \"read_node\", target: \"<symbol>\")]"
            ),
            "CTA for read_node must be present as a single line, got: {text}"
        );
    }

    #[test]
    fn capsule_cpp_forward_decl_returns_full_text() {
        let conn = make_db();
        let fwd = "class Widget;";
        insert_node(
            &conn,
            "r:w.h:Widget",
            "r",
            "w.h",
            "cpp",
            "Widget",
            "class",
            fwd,
        );
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
            "forward declaration should appear verbatim in output: {}",
            result.optimized_text
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
        assert!(
            text.contains("{ /* ... */ }"),
            "TS body should be replaced, got: {text}"
        );
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
        assert!(
            msg.contains("Found 2 matches"),
            "expected disambiguation payload: {msg}"
        );
        assert!(
            msg.contains("src/a.py"),
            "expected first candidate path: {msg}"
        );
        assert!(
            msg.contains("src/b.py"),
            "expected second candidate path: {msg}"
        );
        assert!(
            msg.contains("run_pipeline"),
            "payload must guide agent to retry: {msg}"
        );
    }

    #[test]
    fn resolve_symbol_matches_windows_backslash_path_with_posix_filepath() {
        // Regression: on Windows, ingest stores `file_path` with the OS
        // separator (e.g. `src\context.rs` via `Path::to_string_lossy`), but
        // agents pass POSIX-style `src/context.rs`. Exact equality used to
        // return "Symbol not found" for explore/dependency/refactor while
        // find_symbol (no filepath) still worked. Resolution must now be
        // separator-insensitive.
        let conn = make_db();
        insert_node(
            &conn,
            "marrow:src\\context.rs:compile_context_packet",
            "marrow",
            "src\\context.rs", // backslash-stored, simulating a Windows index
            "rs",
            "compile_context_packet",
            "function",
            "pub fn compile_context_packet() {}",
        );

        // POSIX-style filepath must resolve against the backslash-stored path.
        let posix = get_context_capsule(
            &conn,
            "compile_context_packet",
            "marrow",
            Some("src/context.rs"),
        )
        .expect("posix filepath must resolve against a backslash-stored path");
        assert!(
            posix.optimized_text.contains("compile_context_packet"),
            "expected the resolved capsule, got: {}",
            posix.optimized_text
        );
        assert!(
            !posix.optimized_text.contains("not found"),
            "resolution must not fall through to not-found: {}",
            posix.optimized_text
        );

        // The native (backslash) filepath must keep working too.
        let native = get_context_capsule(
            &conn,
            "compile_context_packet",
            "marrow",
            Some("src\\context.rs"),
        )
        .expect("native backslash filepath must still resolve");
        assert!(
            native.optimized_text.contains("compile_context_packet"),
            "native filepath regressed: {}",
            native.optimized_text
        );
    }

    #[test]
    fn resolve_symbol_matches_posix_stored_path_with_windows_filepath() {
        // The mirror case: a macOS/Linux index stores forward slashes, but a
        // caller on Windows (or a tool that reconstructs OS paths) passes a
        // backslash filepath. Folding both sides keeps this portable.
        let conn = make_db();
        insert_node(
            &conn,
            "marrow:src/context.rs:compile_context_packet",
            "marrow",
            "src/context.rs",
            "rs",
            "compile_context_packet",
            "function",
            "pub fn compile_context_packet() {}",
        );

        let result = get_context_capsule(
            &conn,
            "compile_context_packet",
            "marrow",
            Some("src\\context.rs"),
        )
        .expect("backslash filepath must resolve against a forward-slash index");
        assert!(
            result.optimized_text.contains("compile_context_packet"),
            "expected the resolved capsule, got: {}",
            result.optimized_text
        );
    }

    #[test]
    fn find_symbols_returns_compact_scoped_kind_filtered_matches() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.rs:process_data",
            "r",
            "src/a.rs",
            "rs",
            "process_data",
            "function",
            "fn process_data() { expensive_body(); }",
        );
        insert_node(
            &conn,
            "r:src/b.rs:process_record",
            "r",
            "src/b.rs",
            "rs",
            "process_record",
            "function",
            "fn process_record() {}",
        );
        insert_node(
            &conn,
            "r:src/c.rs:ProcessData",
            "r",
            "src/c.rs",
            "rs",
            "ProcessData",
            "class",
            "class ProcessData {}",
        );
        insert_node(
            &conn,
            "other:src/a.rs:process_other",
            "other",
            "src/a.rs",
            "rs",
            "process_other",
            "function",
            "fn process_other() {}",
        );

        let out = find_symbols(&conn, "r", "process", Some("function"), 1).unwrap();
        assert!(out.contains("Found 1 matches for 'process':"), "{out}");
        assert!(out.contains("(function: process_"), "{out}");
        assert!(
            !out.contains("ProcessData"),
            "kind filter leaked class: {out}"
        );
        assert!(
            !out.contains("process_other"),
            "repo filter leaked other repo: {out}"
        );
        assert!(!out.contains("expensive_body"), "raw source leaked: {out}");
        assert!(
            out.contains("capped at 1 matches"),
            "cap note missing: {out}"
        );
    }

    #[test]
    fn find_symbols_orders_fts_ties_deterministically() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/b.rs:process_beta",
            "r",
            "src/b.rs",
            "rs",
            "process_beta",
            "function",
            "fn process_beta() {}",
        );
        insert_node(
            &conn,
            "r:src/a.rs:process_alpha",
            "r",
            "src/a.rs",
            "rs",
            "process_alpha",
            "function",
            "fn process_alpha() {}",
        );

        let out = find_symbols(&conn, "r", "process", None, FIND_SYMBOL_DEFAULT_LIMIT).unwrap();
        let alpha = out.find("src/a.rs (function: process_alpha)").unwrap();
        let beta = out.find("src/b.rs (function: process_beta)").unwrap();
        assert!(
            alpha < beta,
            "FTS tie-breakers should be deterministic: {out}"
        );
    }

    #[test]
    fn find_symbols_falls_back_to_like_for_non_fts_query() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/op.rs:operator++",
            "r",
            "src/op.rs",
            "rs",
            "operator++",
            "function",
            "fn operator_plus_plus() {}",
        );
        insert_node(
            &conn,
            "r:src/b.rs:bb++",
            "r",
            "src/b.rs",
            "rs",
            "bb++",
            "function",
            "fn bb() {}",
        );
        insert_node(
            &conn,
            "r:src/a.rs:aa++",
            "r",
            "src/a.rs",
            "rs",
            "aa++",
            "function",
            "fn aa() {}",
        );

        let out = find_symbols(&conn, "r", "++", None, FIND_SYMBOL_DEFAULT_LIMIT).unwrap();
        assert!(out.contains("src/op.rs (function: operator++)"), "{out}");
        let aa = out.find("src/a.rs (function: aa++)").unwrap();
        let bb = out.find("src/b.rs (function: bb++)").unwrap();
        let operator = out.find("src/op.rs (function: operator++)").unwrap();
        assert!(
            aa < bb && bb < operator,
            "LIKE fallback should order by length, symbol name, path, rowid: {out}"
        );
    }

    #[test]
    fn find_symbols_duplicate_names_emit_path_type_name_shape() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.rs:render",
            "r",
            "src/a.rs",
            "rs",
            "render",
            "function",
            "fn render() { draw_a(); }",
        );
        insert_node(
            &conn,
            "r:src/b.rs:render",
            "r",
            "src/b.rs",
            "rs",
            "render",
            "method",
            "fn render() { draw_b(); }",
        );

        let out = find_symbols(&conn, "r", "render", None, FIND_SYMBOL_DEFAULT_LIMIT).unwrap();
        assert!(out.contains("- src/a.rs (function: render)"), "{out}");
        assert!(out.contains("- src/b.rs (method: render)"), "{out}");
        assert!(!out.contains("draw_a"), "raw source leaked: {out}");
        assert!(!out.contains("draw_b"), "raw source leaked: {out}");
    }

    #[test]
    fn find_symbols_no_match_is_not_a_skeleton_dump() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/lib.rs:known",
            "r",
            "src/lib.rs",
            "rs",
            "known",
            "function",
            "fn known() {}",
        );

        let out = find_symbols(&conn, "r", "missing", None, FIND_SYMBOL_DEFAULT_LIMIT).unwrap();
        assert!(out.contains("No symbol matches for 'missing'"), "{out}");
        assert!(
            !out.contains("FULL SOURCE"),
            "should not emit source: {out}"
        );
        assert!(
            !out.contains("src/lib.rs (function: known)"),
            "should not dump skeleton rows: {out}"
        );
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
        insert_node(
            &conn,
            "r:a.py:base",
            "r",
            "a.py",
            "py",
            "base",
            "function",
            "def base(): pass\n",
        );
        insert_node(
            &conn,
            "r:a.py:mid",
            "r",
            "a.py",
            "py",
            "mid",
            "function",
            "def mid(): base()\n",
        );
        insert_node(
            &conn,
            "r:a.py:top",
            "r",
            "a.py",
            "py",
            "top",
            "function",
            "def top(): mid()\n",
        );
        insert_edge(&conn, "r:a.py:mid", "r:a.py:base", "CALLS");
        insert_edge(&conn, "r:a.py:top", "r:a.py:mid", "CALLS");

        let result = analyze_impact(&conn, "base", "r", None).unwrap();
        assert_eq!(result.affected.len(), 2);
        let mid = result
            .affected
            .iter()
            .find(|n| n.symbol_name == "mid")
            .unwrap();
        let top = result
            .affected
            .iter()
            .find(|n| n.symbol_name == "top")
            .unwrap();
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
        insert_edge(
            &conn,
            "repo_a:app.py:main",
            "repo_b:lib.ts:ApiClient",
            "IMPORTS",
        );

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
        assert!(
            result.pivot_id.starts_with("DISAMBIGUATION:"),
            "expected disambig pivot_id: {}",
            result.pivot_id
        );
        let msg = &result.pivot_id["DISAMBIGUATION:".len()..];
        assert!(
            msg.contains("Found 2 matches"),
            "expected disambiguation payload: {msg}"
        );
        assert!(
            msg.contains("src/a.py"),
            "expected first candidate path: {msg}"
        );
        assert!(
            msg.contains("src/b.py"),
            "expected second candidate path: {msg}"
        );
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
        insert_node(
            &conn,
            "r:a.rs:main",
            "r",
            "src/a.rs",
            "rs",
            "main",
            "function",
            "fn main() {}",
        );
        insert_node(
            &conn,
            "r:a.rs:Foo",
            "r",
            "src/a.rs",
            "rs",
            "Foo",
            "struct",
            "struct Foo {}",
        );
        insert_node(
            &conn,
            "r:b.py:helper",
            "r",
            "src/b.py",
            "py",
            "helper",
            "function",
            "def helper(): pass",
        );
        // Variable — should NOT appear
        insert_node(
            &conn,
            "r:a.rs:X",
            "r",
            "src/a.rs",
            "rs",
            "X",
            "variable",
            "let x = 1;",
        );

        let out = get_project_skeleton(&conn, "r", None).unwrap();
        assert!(out.contains("src/a.rs"), "a.rs missing: {out}");
        assert!(out.contains("src/b.py"), "b.py missing: {out}");
        assert!(out.contains("[function] main"), "main missing: {out}");
        assert!(out.contains("[struct] Foo"), "Foo missing: {out}");
        assert!(out.contains("[function] helper"), "helper missing: {out}");
        assert!(
            !out.contains("[variable]"),
            "variable should be filtered: {out}"
        );
    }

    #[test]
    fn skeleton_target_dir_filters_to_prefix() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:a:fn1",
            "r",
            "src/api/a.rs",
            "rs",
            "fn1",
            "function",
            "fn fn1() {}",
        );
        insert_node(
            &conn,
            "r:b:fn2",
            "r",
            "src/core/b.rs",
            "rs",
            "fn2",
            "function",
            "fn fn2() {}",
        );

        let out = get_project_skeleton(&conn, "r", Some("src/api")).unwrap();
        assert!(out.contains("fn1"), "fn1 missing: {out}");
        assert!(!out.contains("fn2"), "fn2 should be filtered: {out}");
    }

    #[test]
    fn skeleton_target_dir_does_not_match_sibling_prefixes() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:a:fn1",
            "r",
            "src/api/a.rs",
            "rs",
            "fn1",
            "function",
            "fn fn1() {}",
        );
        insert_node(
            &conn,
            "r:b:fn2",
            "r",
            "src/api_old/b.rs",
            "rs",
            "fn2",
            "function",
            "fn fn2() {}",
        );

        let out = get_project_skeleton(&conn, "r", Some("src/api")).unwrap();
        assert!(out.contains("fn1"), "expected api symbol: {out}");
        assert!(
            !out.contains("fn2"),
            "sibling prefix should not match target_dir: {out}"
        );
    }

    #[test]
    fn skeleton_no_matching_nodes_after_filter_returns_message() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:a:fn1",
            "r",
            "src/a.rs",
            "rs",
            "fn1",
            "function",
            "fn fn1() {}",
        );

        let out = get_project_skeleton(&conn, "r", Some("src/nonexistent")).unwrap();
        assert!(
            out.contains("No matching symbols"),
            "expected no-match message: {out}"
        );
    }

    #[test]
    fn skeleton_only_lists_symbols_for_requested_repo() {
        let conn = make_db();
        insert_node(
            &conn,
            "repo_a:a:fn1",
            "repo_a",
            "src/a.rs",
            "rs",
            "fn1",
            "function",
            "fn fn1() {}",
        );
        insert_node(
            &conn,
            "repo_b:b:fn2",
            "repo_b",
            "src/b.rs",
            "rs",
            "fn2",
            "function",
            "fn fn2() {}",
        );

        let out = get_project_skeleton(&conn, "repo_a", None).unwrap();
        assert!(out.contains("fn1"), "repo_a symbol missing: {out}");
        assert!(
            !out.contains("fn2"),
            "repo_b symbol leaked into repo_a output: {out}"
        );
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
        assert!(
            result.optimized_text.contains("foo"),
            "pivot should still be present: {}",
            result.optimized_text
        );
        assert!(
            result.original_text.is_empty(),
            "original_text should be empty when file is missing"
        );
    }

    #[test]
    fn concat_full_original_sorted_by_path_not_by_content() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("z.txt"), "zzz").unwrap();
        fs::write(root.join("a.txt"), "aaa").unwrap();
        let mut touched = HashSet::new();
        touched.insert("z.txt".to_string());
        touched.insert("a.txt".to_string());
        let (s, trunc, omit) = concat_full_original_text_sorted(Some(root), &touched, None);
        assert!(!trunc);
        assert!(omit.is_empty());
        assert_eq!(s, "aaa\nzzz");
    }

    #[test]
    fn concat_full_original_budget_skips_using_metadata_without_full_read() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::write(root.join("a.txt"), "small").unwrap();
        fs::write(root.join("b.bin"), vec![b'y'; 100_000]).unwrap();
        let mut touched = HashSet::new();
        touched.insert("a.txt".to_string());
        touched.insert("b.bin".to_string());
        let (s, trunc, omit) = concat_full_original_text_sorted(Some(root), &touched, Some(20));
        assert!(trunc);
        assert_eq!(s, "small");
        assert!(omit.iter().any(|p| p == "b.bin"));
    }

    #[test]
    fn capsule_none_mode_empty_original_metadata_file_tokens() {
        let _g = CAPSULE_ENV_LOCK.lock().unwrap();
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MODE");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_LEGACY");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MAX_BYTES");

        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/a.py"), "aaaa").unwrap();
        fs::write(root.join("src/b.py"), "bbbbbbbb").unwrap();

        let conn = make_db();
        insert_repo(&conn, "r", &root.to_string_lossy());
        insert_node(
            &conn,
            "r:src/a.py:fa",
            "r",
            "src/a.py",
            "py",
            "fa",
            "function",
            "def fa():\n  pass\n",
        );
        insert_node(
            &conn,
            "r:src/b.py:fb",
            "r",
            "src/b.py",
            "py",
            "fb",
            "function",
            "def fb():\n  pass\n",
        );
        insert_edge(&conn, "r:src/a.py:fa", "r:src/b.py:fb", "CALLS");

        let result = get_context_capsule(&conn, "fa", "r", None).unwrap();
        assert!(result.original_text.is_empty());
        assert!(!result.optimized_text.is_empty());
        assert_eq!(result.file_tokens, 3, "4/4 + 8/4 = 1 + 2");
        assert_eq!(result.provenance.baseline_token_source, "estimated");
        assert_eq!(result.provenance.original_mode, "none");
        let proof = result
            .proof_snapshot
            .as_ref()
            .expect("default mode should include bounded proof");
        assert!(
            proof.proof_text.contains("aaaa"),
            "proof should include touched source sample: {proof:?}"
        );
        assert_eq!(proof.token_source, "estimated");

        // Same DB + `MARROW_CAPSULE_ORIGINAL_LEGACY=1` → full concat for one release.
        std::env::set_var("MARROW_CAPSULE_ORIGINAL_LEGACY", "1");
        let result_full = get_context_capsule(&conn, "fa", "r", None).unwrap();
        assert!(
            result_full.original_text.contains("aaaa"),
            "legacy full mode should load sources: {}",
            result_full.original_text
        );
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_LEGACY");
    }

    #[test]
    fn capsule_full_mode_labels_full_and_truncated_full_provenance() {
        let _g = CAPSULE_ENV_LOCK.lock().unwrap();
        std::env::set_var("MARROW_CAPSULE_ORIGINAL_MODE", "full");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_LEGACY");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MAX_BYTES");

        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(root.join("src/a.py"), "aaaa").unwrap();
        fs::write(root.join("src/b.py"), "bbbbbbbbbbbbbbbb").unwrap();

        let conn = make_db();
        insert_repo(&conn, "r", &root.to_string_lossy());
        insert_node(
            &conn,
            "r:src/a.py:fa",
            "r",
            "src/a.py",
            "py",
            "fa",
            "function",
            "def fa(): pass",
        );
        insert_node(
            &conn,
            "r:src/b.py:fb",
            "r",
            "src/b.py",
            "py",
            "fb",
            "function",
            "def fb(): pass",
        );
        insert_edge(&conn, "r:src/a.py:fa", "r:src/b.py:fb", "CALLS");

        let full = get_context_capsule(&conn, "fa", "r", None).unwrap();
        assert!(full.original_text.contains("aaaa"));
        assert!(full.original_text.contains("bbbbbbbbbbbbbbbb"));
        assert!(full.proof_snapshot.is_none());
        assert_eq!(full.provenance.baseline_token_source, "full");
        assert_eq!(full.provenance.original_mode, "full");
        assert_eq!(full.provenance.proof_label, "full");
        assert_eq!(full.provenance.original_max_bytes, None);

        std::env::set_var("MARROW_CAPSULE_ORIGINAL_MAX_BYTES", "8");
        let truncated = get_context_capsule(&conn, "fa", "r", None).unwrap();
        assert!(truncated.original_text.contains("aaaa"));
        assert!(truncated.original_text.contains("original_text truncated"));
        assert!(truncated.proof_snapshot.is_none());
        assert_eq!(truncated.provenance.baseline_token_source, "truncated_full");
        assert_eq!(truncated.provenance.proof_label, "truncated_full");
        assert_eq!(truncated.provenance.original_max_bytes, Some(8));

        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MODE");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MAX_BYTES");
    }

    #[test]
    fn precise_token_measurement_reports_failed_touched_files() {
        let dir = tempdir().unwrap();
        let root = dir.path().to_string_lossy().to_string();
        let conn = make_db();
        insert_repo(&conn, "r", &root);
        insert_node(
            &conn,
            "r:src/missing.py:fa",
            "r",
            "src/missing.py",
            "py",
            "fa",
            "function",
            "def fa(): pass",
        );

        let measured = measure_precise_tokens_touched_by_capsule(&conn, "fa", "r", None).unwrap();
        assert_eq!(measured.tokens, 0);
        assert_eq!(measured.touched_file_count, 1);
        assert_eq!(measured.measured_file_count, 0);
        assert_eq!(measured.failed_paths, vec!["src/missing.py".to_string()]);
        assert_eq!(measured.tokenizer_mode, "cl100k_base");
    }

    #[test]
    fn capsule_proof_reports_sampling_and_truncation_bounds() {
        let _g = CAPSULE_ENV_LOCK.lock().unwrap();
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_MODE");
        std::env::remove_var("MARROW_CAPSULE_ORIGINAL_LEGACY");
        std::env::set_var("MARROW_CAPSULE_PROOF_MAX_BYTES", "64");
        std::env::set_var("MARROW_CAPSULE_PROOF_MAX_FILES", "1");

        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::write(
            root.join("src/a.py"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        fs::write(root.join("src/b.py"), "bbbbbbbb").unwrap();

        let conn = make_db();
        insert_repo(&conn, "r", &root.to_string_lossy());
        insert_node(
            &conn,
            "r:src/a.py:fa",
            "r",
            "src/a.py",
            "py",
            "fa",
            "function",
            "def fa(): pass",
        );
        insert_node(
            &conn,
            "r:src/b.py:fb",
            "r",
            "src/b.py",
            "py",
            "fb",
            "function",
            "def fb(): pass",
        );
        insert_edge(&conn, "r:src/a.py:fa", "r:src/b.py:fb", "CALLS");

        let result = get_context_capsule(&conn, "fa", "r", None).unwrap();
        let proof = result.proof_snapshot.expect("proof snapshot");
        assert!(
            proof.sampled,
            "many touched files should be sampled: {proof:?}"
        );
        assert!(
            proof.truncated,
            "small proof byte cap should truncate: {proof:?}"
        );
        assert_eq!(proof.max_bytes, 64);
        assert_eq!(proof.max_files, 1);

        std::env::remove_var("MARROW_CAPSULE_PROOF_MAX_BYTES");
        std::env::remove_var("MARROW_CAPSULE_PROOF_MAX_FILES");
    }

    #[test]
    fn trace_flow_omits_original_text_payload() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.py:fa",
            "r",
            "src/a.py",
            "py",
            "fa",
            "function",
            "def fa(): pass",
        );

        let result = trace_logic_flow(&conn, "fa", "r", None).unwrap();
        assert!(result.original_text.is_empty());
        assert!(result.proof_snapshot.is_none());
        assert_eq!(result.provenance.original_mode, "none");
    }

    // ── condense (unit tests on raw text) ──────────────────────────────────

    #[test]
    fn condense_cpp_function_replaces_body() {
        let raw = "void process(int x) {\n    x += 1;\n    return;\n}";
        let result = condense(raw, "cpp");
        assert!(
            result.contains("process(int x)"),
            "signature lost: {result}"
        );
        assert!(
            result.contains("{ /* ... */ }"),
            "placeholder missing: {result}"
        );
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
        assert!(
            result.contains("def compute(n):"),
            "signature lost: {result}"
        );
        assert!(
            result.contains("pass"),
            "pass placeholder missing: {result}"
        );
        assert!(!result.contains("total"), "body leaked: {result}");
    }

    #[test]
    fn condense_ts_function_replaces_body() {
        let raw = "function greet(name: string): string {\n    return `Hello ${name}`;\n}";
        let result = condense(raw, "ts");
        assert!(
            result.contains("greet(name: string)"),
            "signature lost: {result}"
        );
        assert!(
            result.contains("{ /* ... */ }"),
            "placeholder missing: {result}"
        );
        assert!(!result.contains("Hello"), "body leaked: {result}");
    }

    #[test]
    fn condense_rust_function_replaces_body() {
        let raw = "fn compute(n: u32) -> u32 {\n    let x = n * 2;\n    x\n}";
        let result = condense(raw, "rs");
        assert!(
            result.contains("fn compute(n: u32)"),
            "signature lost: {result}"
        );
        assert!(
            result.contains("{ /* ... */ }"),
            "placeholder missing: {result}"
        );
        assert!(!result.contains("n * 2"), "body leaked: {result}");
    }

    #[test]
    fn condense_ruby_method_replaces_body_preserves_end() {
        let raw = "def greet(name)\n  puts name\n  name.upcase\nend\n";
        let result = condense(raw, "rb");
        assert!(result.contains("def greet"), "signature lost: {result}");
        assert!(
            result.contains("end"),
            "`end` keyword must be preserved: {result}"
        );
        assert!(result.contains("# ..."), "placeholder missing: {result}");
        assert!(!result.contains("puts name"), "body leaked: {result}");
    }

    #[test]
    fn condense_ruby_preserves_end_not_chopped() {
        // Verifies the byte-range replacement does NOT consume the closing `end`.
        let raw = "def foo\n  x = 1\nend\n";
        let result = condense(raw, "rb");
        assert!(
            result.ends_with("end\n"),
            "`end` must close the method: {result}"
        );
    }

    // ── Filepath disambiguation ───────────────────────────────────────────

    #[test]
    fn capsule_disambiguates_by_filepath() {
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

        let result = get_context_capsule(&conn, "bulk_update", "r", Some("src/a.py")).unwrap();
        assert!(
            result.optimized_text.contains("return 'a'"),
            "should resolve to src/a.py variant: {}",
            result.optimized_text
        );
        assert!(
            !result.optimized_text.contains("return 'b'"),
            "src/b.py variant should not appear: {}",
            result.optimized_text
        );
    }

    #[test]
    fn capsule_filepath_mismatch_returns_not_found() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.py:bulk_update",
            "r",
            "src/a.py",
            "py",
            "bulk_update",
            "function",
            "def bulk_update():\n    pass\n",
        );

        let err =
            get_context_capsule(&conn, "bulk_update", "r", Some("src/nonexistent.py")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected not-found error: {msg}");
    }

    #[test]
    fn impact_disambiguates_by_filepath() {
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
        insert_node(
            &conn,
            "r:src/a.py:caller",
            "r",
            "src/a.py",
            "py",
            "caller",
            "function",
            "def caller(): bulk_update()\n",
        );
        insert_edge(
            &conn,
            "r:src/a.py:caller",
            "r:src/a.py:bulk_update",
            "CALLS",
        );

        let result = analyze_impact(&conn, "bulk_update", "r", Some("src/a.py")).unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "caller");
    }

    #[test]
    fn capsule_auto_condenses_oversized_pivot() {
        // Build a pivot whose raw_text exceeds DEFAULT_MAX_PIVOT_BYTES.
        let conn = make_db();
        let big_body = format!(
            "class BigModel < ApplicationRecord\n{}end\n",
            "  has_many :things\n".repeat(2000)
        );
        assert!(
            big_body.len() > DEFAULT_MAX_PIVOT_BYTES,
            "test fixture must exceed cap: {} vs {}",
            big_body.len(),
            DEFAULT_MAX_PIVOT_BYTES
        );
        insert_node(
            &conn,
            "r:m.rb:BigModel",
            "r",
            "m.rb",
            "rb",
            "BigModel",
            "class",
            &big_body,
        );
        let result = get_context_capsule(&conn, "BigModel", "r", None).unwrap();
        assert!(
            result.optimized_text.contains("CONDENSED SOURCE"),
            "oversized pivot should be condensed: {}",
            &result.optimized_text[..200.min(result.optimized_text.len())]
        );
        assert!(
            result
                .optimized_text
                .contains("MARROW_CAPSULE_MAX_PIVOT_BYTES"),
            "condensed output should mention the env var"
        );
        // The condensed text should NOT contain the full repeated body.
        assert!(
            result.optimized_text.len() < big_body.len(),
            "condensed capsule ({}) should be smaller than raw pivot ({})",
            result.optimized_text.len(),
            big_body.len()
        );
    }

    #[test]
    fn capsule_small_pivot_gets_full_source() {
        let conn = make_db();
        let small_body = "def tiny():\n    return 1\n";
        assert!(small_body.len() < DEFAULT_MAX_PIVOT_BYTES);
        insert_node(
            &conn,
            "r:f.py:tiny",
            "r",
            "f.py",
            "py",
            "tiny",
            "function",
            small_body,
        );
        let result = get_context_capsule(&conn, "tiny", "r", None).unwrap();
        assert!(
            result.optimized_text.contains("FULL SOURCE"),
            "small pivot should get full source: {}",
            result.optimized_text
        );
        assert!(
            !result.optimized_text.contains("CONDENSED SOURCE"),
            "small pivot should not be condensed"
        );
    }

    #[test]
    fn capsule_no_filepath_still_errors_on_ambiguity() {
        let conn = make_db();
        insert_node(
            &conn,
            "r:src/a.py:dup",
            "r",
            "src/a.py",
            "py",
            "dup",
            "function",
            "def dup(): pass\n",
        );
        insert_node(
            &conn,
            "r:src/b.py:dup",
            "r",
            "src/b.py",
            "py",
            "dup",
            "function",
            "def dup(): pass\n",
        );

        // Disambiguation without filepath is now a successful payload, not an error.
        let result = get_context_capsule(&conn, "dup", "r", None).unwrap();
        assert!(
            result.optimized_text.contains("Found 2 matches"),
            "should return disambiguation payload without filepath: {}",
            result.optimized_text
        );
    }
}
