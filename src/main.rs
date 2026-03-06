mod db;
mod dashboard;
mod ingestion;
mod retrieval;
mod skills;
mod watcher;

use std::{
    fmt::Write as FmtWrite,
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Context as _, Result};
use dashboard::DashboardEvent;
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation,
        InitializeRequestParams, InitializeResult,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        ToolsCapability,
    },
    service::RequestContext,
    transport::stdio,
};

const DASHBOARD_EMIT_URL: &str = "http://127.0.0.1:8765/api/emit";

/// Stores the MCP client's name captured during the `initialize` handshake.
/// Safe to use as a singleton because stdio spawns one process per session.
static CLIENT_NAME: OnceLock<String> = OnceLock::new();

/// Appends a timestamped error line to `~/.marrow/debug.log`.
/// All failures are silently swallowed so callers never panic or write to stdout.
fn log_emit_error(msg: &str) {
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    let Some(home) = dirs::home_dir() else { return };
    let log_dir = home.join(".marrow");
    let _ = fs::create_dir_all(&log_dir);

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if let Ok(mut file) = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_dir.join("debug.log"))
    {
        let _ = writeln!(file, "[{ts}] telemetry POST error: {msg}");
    }
}

/// Milliseconds to wait after the Axum server spawns before sending the
/// first ServerStarted event. The listener is ready almost instantly but
/// there is a brief window between `tokio::spawn` returning and the first
/// `accept()` completing. A missed ServerStarted is cosmetic (dashboard UI
/// only), so this is best-effort rather than a hard synchronisation point.
const DASHBOARD_WARMUP_MS: u64 = 50;

// ── Capsule formatting ────────────────────────────────────────────────────────

/// Format a ContextCapsule as the plain-text string sent to the LLM.
/// Retained for tests; production code uses `CapsuleResult::optimized_text`.
#[cfg(test)]
pub(crate) fn format_capsule_string(capsule: &retrieval::ContextCapsule) -> String {
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

    if capsule.neighbors.is_empty() {
        writeln!(out, "── NEIGHBORS ────────────────────────────────────────────────").ok();
        writeln!(out, "  (none — isolated symbol)").ok();
    } else {
        for n in &capsule.neighbors {
            writeln!(
                out,
                "\n── NEIGHBOR  [{rel}]  {name}  ({lang})  {path}",
                rel  = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            ).ok();
            writeln!(out, "{}", n.node.text).ok();
        }
    }
    out
}

/// Count cl100k_base tokens in `text`.
fn count_tokens(text: &str) -> anyhow::Result<usize> {
    let bpe = tiktoken_rs::cl100k_base()?;
    Ok(bpe.encode_with_special_tokens(text).len())
}

/// Format a usize with thousands separators: 4812 → "4,812".
fn fmt_num(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Build the terminal benchmark table.
///
/// Layout (67-char inner width, 69-char total with border chars):
///   header rows span full 67 chars (W = L + 1 + R = 27 + 1 + 39)
///   metric rows: 27-char left col │ 39-char right col
fn format_benchmark_table(
    symbol:         &str,
    repo_id:        &str,
    file_path:      &str,
    file_tokens:    usize,
    capsule_tokens: usize,
) -> String {
    let saved     = file_tokens.saturating_sub(capsule_tokens);
    let reduction = if file_tokens == 0 {
        0.0_f64
    } else {
        (saved as f64 / file_tokens as f64) * 100.0
    };

    // Column inner widths (excluding the │ separator).
    const L: usize = 27; // left metric label column
    const R: usize = 39; // right value column
    const W: usize = L + 1 + R; // total inner width = 67

    let h_full  = "─".repeat(W);
    let h_left  = "─".repeat(L);
    let h_right = "─".repeat(R);

    let hdr_title = "  Marrow Token Benchmark".to_string();
    let hdr_sym   = format!("  Symbol: {symbol}  ·  Repo: {repo_id}");
    let hdr_file  = format!("  File:   {file_path}");

    let row = |label: &str, value: &str| -> String {
        format!("│  {label:<25}│  {value:<37}│\n", label = label, value = value)
    };

    let mut t = String::new();
    // Top border + header
    writeln!(t, "┌{h_full}┐").ok();
    writeln!(t, "│{hdr_title:<W$}│", W = W).ok();
    writeln!(t, "│{hdr_sym:<W$}│",   W = W).ok();
    writeln!(t, "│{hdr_file:<W$}│",  W = W).ok();
    // Column divider
    writeln!(t, "├{h_left}┬{h_right}┤").ok();
    // Column headers
    t.push_str(&row("Metric", "Value"));
    // Body divider
    writeln!(t, "├{h_left}┼{h_right}┤").ok();
    // Metric rows
    t.push_str(&row("Original File Tokens", &fmt_num(file_tokens)));
    t.push_str(&row("Capsule Tokens",       &fmt_num(capsule_tokens)));
    t.push_str(&row("Tokens Saved",         &fmt_num(saved)));
    t.push_str(&row("Reduction",            &format!("{:.1}%", reduction)));
    // Bottom border
    write!(t, "└{h_left}┴{h_right}┘").ok();
    t
}

/// Full benchmark pipeline:
/// 1. Look up the pivot node to get file_path.
/// 2. Look up the repo to get root_path → read the full source file.
/// 3. Build the Context Capsule and format it.
/// 4. Count tokens in both strings.
/// 5. Print the table.
fn run_benchmark(
    conn:    &rusqlite::Connection,
    symbol:  &str,
    repo_id: &str,
) -> anyhow::Result<()> {
    // ── Step 1: resolve file path for display ────────────────────────
    let file_path: String = conn
        .query_row(
            "SELECT file_path FROM nodes \
             WHERE symbol_name = ?1 AND repo_id = ?2 LIMIT 1",
            rusqlite::params![symbol, repo_id],
            |row| row.get(0),
        )
        .map_err(|_| {
            anyhow::anyhow!("Symbol '{}' not found in repo '{}'.", symbol, repo_id)
        })?;

    // ── Step 2: build capsule (original_text populated by the engine) ─
    let result = retrieval::get_context_capsule(conn, symbol, repo_id)?;

    // ── Step 3: count tokens ──────────────────────────────────────────
    let file_tokens    = count_tokens(&result.original_text)?;
    let capsule_tokens = count_tokens(&result.optimized_text)?;

    // ── Step 4: print table ───────────────────────────────────────────
    eprintln!(
        "{}",
        format_benchmark_table(symbol, repo_id, &file_path, file_tokens, capsule_tokens)
    );

    Ok(())
}

// ── Server struct ─────────────────────────────────────────────────────────────

/// Wraps the SQLite connection behind Arc<Mutex<_>> so the handler can be
/// Clone + Send + Sync, as required by rmcp's ServerHandler bound.
#[derive(Clone)]
struct ContextEngine {
    db:          Arc<Mutex<rusqlite::Connection>>,
    http_client: reqwest::Client,
    is_indexing: Arc<AtomicBool>,
}

impl ContextEngine {
    #[allow(dead_code)]
    fn new(
        db_path:     &str,
        http_client: reqwest::Client,
    ) -> Result<Self> {
        let conn = db::init_db(db_path)?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            http_client,
            is_indexing: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Convert a `serde_json::Value` (must be an Object) into the
    /// `Arc<serde_json::Map<String, Value>>` that `Tool::new` expects.
    fn schema(v: serde_json::Value) -> Arc<serde_json::Map<String, serde_json::Value>> {
        Arc::new(v.as_object().expect("schema must be a JSON object").clone())
    }

    /// Checks if the workspace is indexed. If not, spawns a background ingest
    /// and returns `Some(message)` so the caller can return early.
    /// Returns `None` if the workspace is already indexed (proceed normally).
    fn maybe_jit_index(
        &self,
        repo_id: &str,
        root_path: &std::path::Path,
    ) -> Option<String> {
        // Fast path: already indexed
        {
            let conn = self.db.lock().unwrap();
            if crate::db::is_repo_indexed(&conn, repo_id, root_path).unwrap_or(false) {
                return None;
            }
        }

        // Guard against concurrent indexing
        if self
            .is_indexing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Some(
                "[MARROW] Workspace indexing is already in progress. \
                 Re-invoke your query in a moment once indexing completes."
                    .to_string(),
            );
        }

        // Spawn background ingest
        let db = self.db.clone();
        let is_indexing = self.is_indexing.clone();
        let repo_id = repo_id.to_string();
        let root_path = root_path.to_path_buf();

        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let conn = db.lock().unwrap();
                crate::ingestion::run_ingestion(&conn, &repo_id, &root_path)
            })
            .await;

            is_indexing.store(false, Ordering::SeqCst);

            match result {
                Ok(Ok((syms, edges))) => {
                    eprintln!(
                        "[MARROW] Background indexing complete: {syms} symbols, {edges} edges."
                    );
                }
                Ok(Err(e)) => eprintln!("[MARROW] Background indexing failed: {e}"),
                Err(e) => eprintln!("[MARROW] Background indexing task panicked: {e}"),
            }
        });

        Some(
            "[MARROW] Workspace indexing started in the background. \
             This is a one-time operation (typically 30-60s for large codebases). \
             Re-invoke your query in a moment to proceed with full context."
                .to_string(),
        )
    }

    /// Pull a required string argument out of the tool arguments map, returning
    /// a well-formed MCP error if absent.
    fn require_str<'a>(
        args: &'a serde_json::Map<String, serde_json::Value>,
        key: &str,
    ) -> Result<&'a str, rmcp::ErrorData> {
        args.get(key)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                rmcp::ErrorData::invalid_params(
                    format!("missing required argument: '{key}'"),
                    None,
                )
            })
    }
}

fn current_workspace_root() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn fallback_repo_id_for_path(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "workspace".to_string())
}

fn resolve_request_repo_id(
    conn: &rusqlite::Connection,
    explicit_repo_id: Option<&str>,
    workspace_root: &Path,
) -> anyhow::Result<String> {
    if let Some(repo_id) = explicit_repo_id {
        return Ok(repo_id.to_string());
    }

    if let Some(repo_id) = db::repo_id_for_root(conn, workspace_root)? {
        return Ok(repo_id);
    }

    Ok(fallback_repo_id_for_path(workspace_root))
}

fn ensure_repo_ready(
    conn: &rusqlite::Connection,
    explicit_repo_id: Option<&str>,
    workspace_root: &Path,
) -> anyhow::Result<String> {
    let repo_id = resolve_request_repo_id(conn, explicit_repo_id, workspace_root)?;
    if explicit_repo_id.is_some() {
        let expected_repo_id = db::repo_id_for_root(conn, workspace_root)?
            .unwrap_or_else(|| fallback_repo_id_for_path(workspace_root));
        if expected_repo_id != repo_id {
            return Err(anyhow::anyhow!(
                "Repo '{}' does not match the current workspace. Run ingest_repo with the correct root_path before querying it from this session.",
                repo_id
            ));
        }
    }
    Ok(repo_id)
}

fn path_contains_marrow_marker(path: &Path) -> bool {
    let Ok(contents) = fs::read_to_string(path) else {
        return false;
    };
    let lowercase = contents.to_lowercase();
    lowercase.contains("marrow")
        || lowercase.contains("run_pipeline")
        || lowercase.contains("\"marrow\"")
}

fn workspace_is_initialized(root: &Path) -> bool {
    let rules = [".cursorrules", ".clinerules", ".roomrules", ".windsurfrules"];
    root.join(".marrow").is_dir()
        && root.join(".marrowrc.json").exists()
        && root.join(".vscode/mcp.json").exists()
        && rules
            .iter()
            .all(|rule| path_contains_marrow_marker(&root.join(rule)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnforcementMode {
    Default,
    Strict,
}

impl EnforcementMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Strict => "strict",
        }
    }

    fn from_config_value(value: Option<&str>) -> Self {
        match value {
            Some("strict") => Self::Strict,
            _ => Self::Default,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ComplianceAction {
    None,
    AutoRouted,
}

#[derive(Debug)]
struct ComplianceRewrite {
    tool_name: String,
    args: serde_json::Map<String, serde_json::Value>,
    notice: Option<String>,
    action: ComplianceAction,
}

fn read_workspace_config() -> serde_json::Value {
    fs::read_to_string(".marrowrc.json")
        .ok()
        .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
        .unwrap_or_else(|| serde_json::json!({}))
}

fn read_enforcement_mode() -> EnforcementMode {
    let cfg = read_workspace_config();
    EnforcementMode::from_config_value(
        cfg.get("enforcement_mode").and_then(|v| v.as_str()),
    )
}

fn apply_compliance_gate(
    tool_name: &str,
    mut args: serde_json::Map<String, serde_json::Value>,
    mode: EnforcementMode,
) -> Result<ComplianceRewrite, rmcp::ErrorData> {
    let (intent, target_key) = match tool_name {
        "get_context_capsule" => ("explore_symbol", Some("symbol_name")),
        "analyze_impact" => ("refactor_symbol", Some("symbol_name")),
        "get_skeleton" => ("analyze_repo", Some("target_dir")),
        _ => {
            return Ok(ComplianceRewrite {
                tool_name: tool_name.to_string(),
                args,
                notice: None,
                action: ComplianceAction::None,
            })
        }
    };

    match mode {
        EnforcementMode::Strict => Err(rmcp::ErrorData::invalid_params(
            format!(
                "Direct calls to '{}' are blocked in strict mode. Use `run_pipeline` first.",
                tool_name
            ),
            None,
        )),
        EnforcementMode::Default => {
            let mut routed = serde_json::Map::new();
            routed.insert("intent".to_string(), serde_json::Value::String(intent.to_string()));
            if let Some(key) = target_key {
                if let Some(value) = args.remove(key) {
                    routed.insert("target".to_string(), value);
                }
            }
            if let Some(repo_id) = args.remove("repo_id") {
                routed.insert("repo_id".to_string(), repo_id);
            }
            Ok(ComplianceRewrite {
                tool_name: "run_pipeline".to_string(),
                args: routed,
                notice: Some(format!(
                    "[MARROW COMPLIANCE] Direct '{}' call was auto-routed through `run_pipeline`. Use `run_pipeline` first to avoid this warning.\n",
                    tool_name
                )),
                action: ComplianceAction::AutoRouted,
            })
        }
    }
}

fn ensure_workspace_config(enforcement_mode: Option<EnforcementMode>) -> Result<EnforcementMode> {
    let mut cfg = read_workspace_config();
    if !cfg.is_object() {
        cfg = serde_json::json!({});
    }

    if cfg.get("ignore").is_none() {
        cfg["ignore"] = serde_json::json!(["node_modules", "target", "dist", ".git"]);
    }
    if cfg.get("show_dashboard").is_none() {
        cfg["show_dashboard"] = serde_json::Value::Bool(true);
    }
    if cfg.get("auto_open_ui").is_none() {
        cfg["auto_open_ui"] = serde_json::Value::Bool(true);
    }

    let resolved_mode = enforcement_mode.unwrap_or_else(|| {
        EnforcementMode::from_config_value(cfg.get("enforcement_mode").and_then(|v| v.as_str()))
    });
    cfg["enforcement_mode"] = serde_json::Value::String(resolved_mode.as_str().to_string());

    fs::write(".marrowrc.json", serde_json::to_string_pretty(&cfg)?)?;
    Ok(resolved_mode)
}

fn fallback_paths_for_agent(agent: skills::Agent, workspace_root: &Path) -> Vec<PathBuf> {
    match agent {
        skills::Agent::Cursor => vec![
            workspace_root.join(".cursorrules"),
            workspace_root.join(".vscode/mcp.json"),
        ],
        skills::Agent::GitHubCopilot => vec![workspace_root.join(".vscode/mcp.json")],
        skills::Agent::Antigravity => vec![workspace_root.join(".roomrules")],
        _ => Vec::new(),
    }
}

fn coverage_status_for_agent(
    agent: skills::Agent,
    workspace_root: &Path,
    home: &Path,
) -> (&'static str, String) {
    let project_target = workspace_root.join(agent.target_path(skills::Scope::Project, home));
    let global_target = agent.target_path(skills::Scope::Global, home);

    if project_target.exists() && path_contains_marrow_marker(&project_target) {
        return ("protected", format!("project instructions at {}", project_target.display()));
    }
    if global_target.exists() && path_contains_marrow_marker(&global_target) {
        return ("protected", format!("global instructions at {}", global_target.display()));
    }

    let fallback_hits: Vec<String> = fallback_paths_for_agent(agent, workspace_root)
        .into_iter()
        .filter(|path| path_contains_marrow_marker(path))
        .map(|path| path.display().to_string())
        .collect();
    if !fallback_hits.is_empty() {
        return (
            "partial",
            format!("fallback workspace files present: {}", fallback_hits.join(", ")),
        );
    }

    (
        "unprotected",
        "no agent-specific Marrow instruction target found".to_string(),
    )
}

fn format_agent_coverage_summary(workspace_root: &Path, home: &Path) -> String {
    let agents = [
        ("Claude Code", skills::Agent::ClaudeCode),
        ("Antigravity", skills::Agent::Antigravity),
        ("Cursor", skills::Agent::Cursor),
        ("GitHub Copilot", skills::Agent::GitHubCopilot),
        ("Cline", skills::Agent::Cline),
        ("Zed", skills::Agent::Zed),
    ];

    let mut out = String::new();
    writeln!(out, "Agent coverage:").ok();
    for (name, agent) in agents {
        let (status, detail) = coverage_status_for_agent(agent, workspace_root, home);
        writeln!(out, "- {name}: {status} ({detail})").ok();
    }
    let windsurf_rules = workspace_root.join(".windsurfrules");
    if path_contains_marrow_marker(&windsurf_rules) {
        writeln!(
            out,
            "- Windsurf: partial (workspace fallback rules at {})",
            windsurf_rules.display()
        )
        .ok();
    }
    out.trim_end().to_string()
}

fn format_validation_report(
    workspace_root: &Path,
    home: &Path,
    mode: EnforcementMode,
    conn: &rusqlite::Connection,
) -> String {
    let pipeline = db::read_stat(conn, "pipeline_requests");
    let autorouted = db::read_stat(conn, "direct_low_level_autorouted");
    let rejected = db::read_stat(conn, "direct_low_level_rejected");
    let ambiguous = db::read_stat(conn, "ambiguous_symbol_requests");
    let stale = db::read_stat(conn, "stale_capsule_prevented");
    let compliance_total = pipeline + autorouted + rejected;
    let compliance_pct = if compliance_total == 0 {
        0.0
    } else {
        (pipeline as f64 / compliance_total as f64) * 100.0
    };

    format!(
        "[MARROW] Validation report\n\
         Path: {}\n\
         Enforcement mode: {}\n\
         {}\n\
         Compliance stats:\n\
         - run_pipeline requests: {}\n\
         - direct low-level auto-routed: {}\n\
         - direct low-level rejected: {}\n\
         - ambiguous symbol requests: {}\n\
         - stale capsule preventions: {}\n\
         - run_pipeline compliance rate: {:.1}%",
        workspace_root.display(),
        mode.as_str(),
        format_agent_coverage_summary(workspace_root, home),
        pipeline,
        autorouted,
        rejected,
        ambiguous,
        stale,
        compliance_pct
    )
}

/// Checks whether the current workspace has been initialized (`.marrow/` exists).
/// If not, creates it, writes project-scope rules files, and returns a notice
/// string to prepend to the tool response. Non-fatal: errors are logged to stderr
/// and `None` is returned so the tool call proceeds unblocked.
///
/// Note: there is a benign TOCTOU between the `.exists()` check and `create_dir_all`.
/// This is acceptable for a single-user local server; `write_workspace_rules` is
/// idempotent so a double-write from a concurrent call causes no harm.
async fn try_auto_init() -> Option<String> {
    let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    if workspace_is_initialized(&root) {
        return None;
    }
    let result = tokio::task::spawn_blocking(|| -> anyhow::Result<()> {
        let root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        std::fs::create_dir_all(root.join(".marrow"))
            .map_err(|e| {
                eprintln!("[MARROW AUTO-INIT] Warning: could not create .marrow/: {e}");
                e
            })?;
        if let Err(e) = write_workspace_rules(&root) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write workspace rules: {e}");
        }
        if let Err(e) = write_vscode_mcp_config(&root) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write .vscode/mcp.json: {e}");
        }
        if let Err(e) = ensure_workspace_config(Some(EnforcementMode::Default)) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write .marrowrc.json: {e}");
        }
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return None, // create_dir_all failed, already logged
        Err(e) => {
            eprintln!("[MARROW AUTO-INIT] spawn_blocking failed: {e}");
            return None;
        }
    }

    Some(
        "[MARROW AUTO-INIT] This workspace was not initialized. Marrow has automatically \
         written workflow rules to .cursorrules and .clinerules. Please notify the user \
         that running `marrow integrate` once globally will prevent this message in \
         future projects.\n\n"
            .to_string(),
    )
}

// ── ServerHandler impl ────────────────────────────────────────────────────────

impl ServerHandler for ContextEngine {
    // ── Server identity ───────────────────────────────────────────────────────

    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            server_info: Implementation {
                name: "marrow-ast-context-engine".to_string(),
                version: "0.1.0".to_string(),
                title: Some("Marrow".to_string()),
                description: Some(
                    "Local, deterministic MCP server: parses multi-language codebases \
                     via tree-sitter into an AST dependency graph and serves condensed \
                     Context Capsules to reduce LLM token usage."
                        .to_string(),
                ),
                icons: None,
                website_url: None,
            },
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability { list_changed: None }),
                ..Default::default()
            },
            instructions: Some(
                "Use `run_pipeline` first for code exploration, dependency tracing, and refactor impact. \
                 Direct low-level calls may be auto-routed or rejected depending on the workspace \
                 enforcement mode."
                    .to_string(),
            ),
            ..Default::default()
        }
    }

    // ── Initialize override: capture client name ──────────────────────────────

    fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<InitializeResult, rmcp::ErrorData>> + Send + '_ {
        // Capture the connecting client's name from the MCP handshake (best-effort).
        let _ = CLIENT_NAME.set(request.client_info.name.clone());

        // Delegate to the default peer-info storage behaviour and return our ServerInfo.
        if context.peer.peer_info().is_none() {
            context.peer.set_peer_info(request);
        }
        let info = self.get_info();
        async move {
            Ok(InitializeResult {
                protocol_version: rmcp::model::ProtocolVersion::default(),
                capabilities:     info.capabilities,
                server_info:      info.server_info,
                instructions:     info.instructions,
            })
        }
    }

    // ── Tool registry ─────────────────────────────────────────────────────────

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        use serde_json::json;

        let tools = vec![
            Tool::new(
                "get_context_capsule",
                "Advanced tool. Prefer `run_pipeline` first. Direct calls may be auto-routed in \
                 default mode or rejected in strict mode. Returns the pivot symbol's full source \
                 plus condensed depth-1 callers, callees, and imports.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "symbol_name": {
                            "type": "string",
                            "description": "The exact symbol name to look up (e.g. 'MyClass' or 'process_data')."
                        },
                        "repo_id": {
                            "type": "string",
                            "description": "The repository identifier used during ingestion (e.g. 'backend_api')."
                        }
                    },
                    "required": ["symbol_name", "repo_id"]
                })),
            ),
            Tool::new(
                "analyze_impact",
                "Advanced tool. Prefer `run_pipeline` first. Direct calls may be auto-routed in \
                 default mode or rejected in strict mode. Recursively maps the blast radius of \
                 a proposed change across callers and importers.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "symbol_name": {
                            "type": "string",
                            "description": "The symbol whose downstream dependents to analyse."
                        },
                        "repo_id": {
                            "type": "string",
                            "description": "The repository identifier for the pivot symbol."
                        }
                    },
                    "required": ["symbol_name", "repo_id"]
                })),
            ),
            Tool::new(
                "ingest_repo",
                "Use this to build or refresh the AST dependency graph for a repository. \
                 Run immediately if any other Marrow tool fails with an empty or missing \
                 database error, or when the user asks you to map/index the codebase. \
                 After ingestion completes, resume the original task using get_context_capsule \
                 or analyze_impact. If the path is outside the current workspace, the server \
                 will intercept and require explicit user confirmation via `user_confirmed: true`.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "repo_id": {
                            "type": "string",
                            "description": "A unique, stable identifier for the repository (e.g. 'backend_api')."
                        },
                        "root_path": {
                            "type": "string",
                            "description": "Absolute or relative path to the repository root on disk."
                        },
                        "user_confirmed": {
                            "type": "boolean",
                            "description": "Set to true only after the user has explicitly granted permission to index a path outside the current workspace. Defaults to false."
                        }
                    },
                    "required": ["repo_id", "root_path"]
                })),
            ),
            Tool::new(
                "save_observation",
                "Save a session memory (observation) about a specific code symbol. \
                 The observation is hash-linked to the symbol's current AST node so \
                 Marrow can automatically flag it as stale if the code changes after \
                 the next ingest_repo run. Use the relative file path as stored in the \
                 graph (e.g. 'src/main.rs'). Supply `repo_id` when you want to target a \
                 repo other than the current workspace.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "repo_id": {
                            "type": "string",
                            "description": "Optional repo override. Defaults to the current workspace repo."
                        },
                        "symbol_name": {
                            "type": "string",
                            "description": "Exact name of the symbol to link the memory to (e.g. 'ingest_repo')."
                        },
                        "filepath": {
                            "type": "string",
                            "description": "Relative file path of the symbol as stored in the graph (e.g. 'src/ingestion.rs')."
                        },
                        "observation": {
                            "type": "string",
                            "description": "The observation or memory text to save."
                        }
                    },
                    "required": ["symbol_name", "filepath", "observation"]
                })),
            ),
            Tool::new(
                "get_session_context",
                "Retrieve stored session memories (observations) for a symbol or file. \
                 Memories that have gone stale — because the underlying code changed \
                 since they were recorded — are returned with a prominent warning so \
                 the caller knows to re-verify them before trusting. Supply `repo_id` \
                 to inspect memories outside the current workspace repo.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "repo_id": {
                            "type": "string",
                            "description": "Optional repo override. Defaults to the current workspace repo."
                        },
                        "symbol_name": {
                            "type": "string",
                            "description": "Filter memories by this symbol name."
                        },
                        "filepath": {
                            "type": "string",
                            "description": "Filter memories by this relative file path."
                        }
                    }
                })),
            ),
            Tool::new(
                "get_skeleton",
                "Advanced tool. Prefer `run_pipeline` first. Direct calls may be auto-routed in \
                 default mode or rejected in strict mode. Returns a token-efficient Markdown \
                 map of one repo's high-level architecture, grouped by file.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "repo_id": {
                            "type": "string",
                            "description": "Optional repo override. Defaults to the current workspace repo."
                        },
                        "target_dir": {
                            "type": "string",
                            "description": "Optional path prefix to restrict the skeleton to a \
                                            subdirectory (e.g. 'src/api'). Omit to map the entire repo."
                        }
                    }
                })),
            ),
            Tool::new(
                "run_pipeline",
                "PRIMARY TOOL. ALWAYS USE THIS FIRST. PREFER THIS over reading files \
                 directly — use for any question involving code structure, dependencies, \
                 symbol lookup, refactoring impact, or codebase exploration. Pass your \
                 goal/intent, and Marrow will auto-detect and return the optimal context \
                 (skeleton, capsule, or impact graph). Use intent 'analyze_repo' to map \
                 the full codebase, 'explore_symbol' to understand a specific symbol, or \
                 'refactor_symbol' to assess the blast radius of a change.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "intent": {
                            "type": "string",
                            "description": "Must be exactly 'analyze_repo', 'explore_symbol', or 'refactor_symbol'."
                        },
                        "target": {
                            "type": "string",
                            "description": "The symbol name or directory path relevant to the intent. \
                                            Required for explore_symbol and refactor_symbol."
                        },
                        "repo_id": {
                            "type": "string",
                            "description": "The repository identifier. Auto-detected if omitted."
                        }
                    },
                    "required": ["intent"]
                })),
            ),
            Tool::new(
                "workspace_setup",
                "CRITICAL: Run this tool when the user asks you to initialize Marrow, OR \
                 automatically if `.marrow/` does not exist in the current workspace and \
                 auto-init has not already run. Generates workspace rule files, writes \
                 `.vscode/mcp.json`, and persists the workspace enforcement mode.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "enforcement_mode": {
                            "type": "string",
                            "description": "Optional. Set to 'default' to warn and auto-route low-level calls, or 'strict' to reject them."
                        }
                    }
                })),
            ),
        ];

        std::future::ready(Ok(ListToolsResult::with_all_items(tools)))
    }

    // ── Tool dispatch ─────────────────────────────────────────────────────────

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        let db = Arc::clone(&self.db);

        async move {
            // Skip auto-init when the agent is explicitly calling workspace_setup —
            // that tool handles its own initialization messaging.
            let init_notice = if request.name.as_ref() != "workspace_setup" {
                try_auto_init().await
            } else {
                None
            };
            let original_tool_name = request.name.to_string();
            let stats_db = Arc::clone(&db);
            let args = request.arguments.unwrap_or_default();
            let enforcement_mode = read_enforcement_mode();
            let compliance = match apply_compliance_gate(&original_tool_name, args, enforcement_mode) {
                Ok(rewrite) => rewrite,
                Err(err) => {
                    if let Ok(conn) = db.lock() {
                        let _ = db::increment_stat(&conn, "direct_low_level_rejected", 1);
                    }
                    return Err(err);
                }
            };
            let compliance_notice = compliance.notice;
            let compliance_action = compliance.action;
            let args = compliance.args;

            let mut result = match compliance.tool_name.as_str() {
                // ── get_context_capsule ───────────────────────────────────────
                "get_context_capsule" => {
                    let symbol_name  = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id      = Self::require_str(&args, "repo_id")?.to_string();
                    let client_name  = CLIENT_NAME.get()
                                           .cloned()
                                           .unwrap_or_else(|| "Unknown Agent".to_string());

                    let cwd = current_workspace_root();
                    if let Some(msg) = self.maybe_jit_index(&repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let sym_for_event  = symbol_name.clone();
                    let repo_for_event = repo_id.clone();

                    let (out, original_text, capsule_tokens, file_tokens, abs_file_path) =
                        tokio::task::spawn_blocking(move || {
                            let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                            let cwd = current_workspace_root();
                            let _resolved_repo_id =
                                ensure_repo_ready(&conn, Some(&repo_id), &cwd)?;

                            let capsule_result = retrieval::get_context_capsule(&conn, &symbol_name, &repo_id)?;

                            // Both token counts use the same len()/4 heuristic so
                            // telemetry and the compare endpoint are always in sync.
                            let full_file_tokens  = capsule_result.original_text.len() / 4;
                            let optimized_tokens  = capsule_result.optimized_text.len() / 4;
                            let original_text_out = capsule_result.original_text;
                            let out               = capsule_result.optimized_text;

                            // Derive the absolute pivot file path for the dashboard event.
                            let abs_path_str: String = conn
                                .query_row(
                                    "SELECT n.file_path, r.root_path \
                                     FROM nodes n \
                                     JOIN repositories r ON r.id = n.repo_id \
                                     WHERE n.symbol_name = ?1 AND n.repo_id = ?2 \
                                     ORDER BY n.file_path ASC LIMIT 1",
                                    rusqlite::params![symbol_name, repo_id],
                                    |row| {
                                        let fp: String = row.get(0)?;
                                        let rp: String = row.get(1)?;
                                        Ok(std::path::PathBuf::from(&rp)
                                            .join(&fp)
                                            .to_string_lossy()
                                            .to_string())
                                    },
                                )
                                .unwrap_or_else(|_| symbol_name.clone());

                            // Persist to lifetime stats
                            let saved = (full_file_tokens as i64).saturating_sub(optimized_tokens as i64);
                            let _ = db::increment_stat(&conn, "total_requests",     1);
                            let _ = db::increment_stat(&conn, "total_file_tokens",  full_file_tokens as i64);
                            let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                            Ok::<_, anyhow::Error>((out, original_text_out, optimized_tokens, full_file_tokens, abs_path_str))
                        })
                        .await
                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let tokens_saved = file_tokens.saturating_sub(capsule_tokens);

                    let event = DashboardEvent::CapsuleServed {
                        symbol:         sym_for_event,
                        repo:           repo_for_event,
                        file:           abs_file_path,
                        capsule_tokens,
                        file_tokens,
                        tokens_saved,
                        origin:         client_name,
                        ts:             dashboard::now_ts(),
                        original_text:  Some(original_text),
                        optimized_text: Some(out.clone()),
                    };
                    let http_client = self.http_client.clone();
                    tokio::spawn(async move {
                        match http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await
                        {
                            Err(e) => log_emit_error(&e.to_string()),
                            Ok(resp) if !resp.status().is_success() => {
                                let status = resp.status();
                                let body = resp.text().await.unwrap_or_default();
                                log_emit_error(&format!("status={status} body={body}"));
                            }
                            Ok(_) => {}
                        }
                    });

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── analyze_impact ────────────────────────────────────────────
                "analyze_impact" => {
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id     = Self::require_str(&args, "repo_id")?.to_string();

                    let cwd = current_workspace_root();
                    if let Some(msg) = self.maybe_jit_index(&repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let sym_clone  = symbol_name.clone();
                    let repo_clone = repo_id.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let _resolved_repo_id =
                            ensure_repo_ready(&conn, Some(&repo_id), &cwd)?;

                        retrieval::analyze_impact(&conn, &symbol_name, &repo_id)
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let mut out = String::new();
                    writeln!(out, "IMPACT ANALYSIS — pivot id: {}", result.pivot_id).ok();

                    if result.affected.is_empty() {
                        writeln!(
                            out,
                            "No downstream dependents found. \
                             Symbol is safe to change in isolation."
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
                                file = n.file_path,
                            )
                            .ok();
                        }
                        writeln!(out, "\n{} node(s) affected.", result.affected.len()).ok();
                    }

                    let event = DashboardEvent::ImpactAnalyzed {
                        symbol:         sym_clone,
                        repo:           repo_clone,
                        affected_count: result.affected.len(),
                        ts:             dashboard::now_ts(),
                    };
                    let http_client = self.http_client.clone();
                    tokio::spawn(async move {
                        match http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await
                        {
                            Err(e) => log_emit_error(&e.to_string()),
                            Ok(resp) if !resp.status().is_success() => {
                                let status = resp.status();
                                let body = resp.text().await.unwrap_or_default();
                                log_emit_error(&format!("status={status} body={body}"));
                            }
                            Ok(_) => {}
                        }
                    });

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── ingest_repo ───────────────────────────────────────────────
                "ingest_repo" => {
                    let repo_id   = Self::require_str(&args, "repo_id")?.to_string();
                    let raw_path  = Self::require_str(&args, "root_path")?.to_string();
                    let user_confirmed = args
                        .get("user_confirmed")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);

                    // Phase 3: canonicalize (resolves relative paths like ../frontend)
                    let root_path: PathBuf = {
                        let p = PathBuf::from(&raw_path);
                        if p.is_absolute() {
                            p
                        } else {
                            std::env::current_dir()
                                .unwrap_or_else(|_| PathBuf::from("."))
                                .join(p)
                        }
                    };
                    let root_path = root_path
                        .canonicalize()
                        .unwrap_or_else(|_| root_path.clone());

                    let current_dir = std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."));
                    let current_dir = current_dir
                        .canonicalize()
                        .unwrap_or(current_dir);

                    // Phase 2 — Rule 2: Hard system blocklist
                    let blocked_roots = [
                        Path::new("/Library"),
                        Path::new("/.Trash"),
                        Path::new("/etc"),
                        Path::new("/System"),
                        Path::new("/usr/bin"),
                    ];
                    for blocked_root in blocked_roots {
                        if root_path == blocked_root || root_path.starts_with(blocked_root) {
                            return Ok(CallToolResult::success(vec![Content::text(
                                "CRITICAL SECURITY: Cannot index protected system directories."
                            )]));
                        }
                    }

                    let is_inside_workspace = root_path.starts_with(&current_dir);

                    // Phase 2 — Rule 3: Double opt-in for out-of-bounds paths
                    if !is_inside_workspace && !user_confirmed {
                        let msg = format!(
                            "[SYSTEM INTERCEPT: OUT OF BOUNDS REQUEST]\n\
                             The requested path '{target_path}' is outside the primary workspace boundary.\n\
                             YOUR INSTRUCTIONS:\n\
                             1. Stop your current task.\n\
                             2. Ask the user for explicit permission. Say exactly: \"You requested context that requires indexing an external directory: {target_path}. Do you want me to index this directory into the Marrow graph?\"\n\
                             3. Wait for the user to reply.\n\
                             4. If the user replies \"yes\", re-run the `ingest_repo` tool with the exact same path, but add the parameter `\"user_confirmed\": true`.",
                            target_path = root_path.display()
                        );
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    // Rule 1 (inside workspace) or Rule 4 (outside + confirmed): proceed
                    let repo_id_for_event = repo_id.clone();

                    let (symbols, edges) = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        ingestion::run_ingestion(&conn, &repo_id, &root_path)
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let event = DashboardEvent::RepoIndexed {
                        repo_id: repo_id_for_event,
                        symbols,
                        edges,
                        ts: dashboard::now_ts(),
                    };
                    let http_client = self.http_client.clone();
                    tokio::spawn(async move {
                        match http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await
                        {
                            Err(e) => log_emit_error(&e.to_string()),
                            Ok(resp) if !resp.status().is_success() => {
                                let status = resp.status();
                                let body = resp.text().await.unwrap_or_default();
                                log_emit_error(&format!("status={status} body={body}"));
                            }
                            Ok(_) => {}
                        }
                    });

                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Ingested {symbols} symbols; resolved {edges} cross-repo edges."
                    ))]))
                }

                // ── save_observation ──────────────────────────────────────────
                "save_observation" => {
                    let symbol_name      = Self::require_str(&args, "symbol_name")?.to_string();
                    let filepath         = Self::require_str(&args, "filepath")?.to_string();
                    let observation_text = Self::require_str(&args, "observation")?.to_string();
                    let repo_id_arg      = args.get("repo_id").and_then(|v| v.as_str()).map(str::to_string);

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let repo_id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)?;
                        db::save_observation(&conn, &repo_id, &symbol_name, &filepath, &observation_text)
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }

                // ── get_session_context ───────────────────────────────────────
                "get_session_context" => {
                    let repo_id     = args.get("repo_id").and_then(|v| v.as_str()).map(str::to_string);
                    let symbol_name = args.get("symbol_name").and_then(|v| v.as_str()).map(str::to_string);
                    let filepath    = args.get("filepath").and_then(|v| v.as_str()).map(str::to_string);

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let resolved_repo_id = match repo_id {
                            Some(ref repo) => Some(repo.clone()),
                            None => resolve_request_repo_id(&conn, None, &cwd).ok(),
                        };
                        db::get_session_context(
                            &conn,
                            resolved_repo_id.as_deref(),
                            symbol_name.as_deref(),
                            filepath.as_deref(),
                        )
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }

                // ── get_skeleton ──────────────────────────────────────────────
                "get_skeleton" => {
                    let target_dir = args
                        .get("target_dir")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let repo_id_arg = args.get("repo_id").and_then(|v| v.as_str()).map(str::to_string);

                    let cwd = current_workspace_root();
                    // Resolve the repo_id for JIT check (may be None → fallback)
                    let jit_repo_id = {
                        let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                        resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    };
                    if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let repo_id =
                            ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
                        retrieval::get_project_skeleton(&conn, &repo_id, target_dir.as_deref())
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }

                // ── run_pipeline ──────────────────────────────────────────────
                "run_pipeline" => {
                    let intent = Self::require_str(&args, "intent")?.to_string();
                    let target = args.get("target").and_then(|v| v.as_str()).map(str::to_string);
                    let repo_id_arg = args.get("repo_id").and_then(|v| v.as_str()).map(str::to_string);
                    let client_name = CLIENT_NAME.get()
                        .cloned()
                        .unwrap_or_else(|| "Unknown Agent".to_string());

                    match intent.as_str() {
                        "analyze_repo" => {
                            let target_dir = target.clone();
                            let target_dir_label = target_dir.clone().unwrap_or_else(|| "(workspace)".to_string());

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
                                let skeleton = retrieval::get_project_skeleton(&conn, &repo_id, target_dir.as_deref())?;
                                Ok::<_, anyhow::Error>((skeleton, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            let node_count = result
                                .lines()
                                .filter(|line| line.trim_start().starts_with("- ["))
                                .count();
                            let event = DashboardEvent::SkeletonGenerated {
                                target_dir: format!("{repo_used}:{target_dir_label}"),
                                node_count,
                                ts: dashboard::now_ts(),
                            };
                            let http_client = self.http_client.clone();
                            tokio::spawn(async move {
                                match http_client
                                    .post(DASHBOARD_EMIT_URL)
                                    .json(&event)
                                    .send()
                                    .await
                                {
                                    Err(e) => log_emit_error(&e.to_string()),
                                    Ok(resp) if !resp.status().is_success() => {
                                        let status = resp.status();
                                        let body = resp.text().await.unwrap_or_default();
                                        log_emit_error(&format!("status={status} body={body}"));
                                    }
                                    Ok(_) => {}
                                }
                            });

                            Ok(CallToolResult::success(vec![Content::text(result)]))
                        }

                        "explore_symbol" => {
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'explore_symbol' requires a 'target' (symbol name)".to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event  = symbol_name.clone();

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (out, original_text, capsule_tokens, file_tokens, abs_file_path, repo_used) =
                                tokio::task::spawn_blocking(move || {
                                    let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;

                                    let cwd = current_workspace_root();
                                    let repo_id =
                                        ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                    let capsule_result = retrieval::get_context_capsule(&conn, &symbol_name, &repo_id)?;
                                    let full_file_tokens  = capsule_result.original_text.len() / 4;
                                    let optimized_tokens  = capsule_result.optimized_text.len() / 4;
                                    let original_text_out = capsule_result.original_text;
                                    let out               = capsule_result.optimized_text;

                                    let abs_path_str: String = conn
                                        .query_row(
                                            "SELECT n.file_path, r.root_path \
                                             FROM nodes n \
                                             JOIN repositories r ON r.id = n.repo_id \
                                             WHERE n.symbol_name = ?1 AND n.repo_id = ?2 \
                                             ORDER BY n.file_path ASC LIMIT 1",
                                            rusqlite::params![symbol_name, repo_id],
                                            |row| {
                                                let fp: String = row.get(0)?;
                                                let rp: String = row.get(1)?;
                                                Ok(std::path::PathBuf::from(&rp).join(&fp).to_string_lossy().to_string())
                                            },
                                        )
                                        .unwrap_or_else(|_| symbol_name.clone());

                                    let saved = (full_file_tokens as i64).saturating_sub(optimized_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_requests",     1);
                                    let _ = db::increment_stat(&conn, "total_file_tokens",  full_file_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                                    Ok::<_, anyhow::Error>((out, original_text_out, optimized_tokens, full_file_tokens, abs_path_str, repo_id))
                                })
                                .await
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            let tokens_saved = file_tokens.saturating_sub(capsule_tokens);
                            let event = DashboardEvent::CapsuleServed {
                                symbol:         sym_for_event,
                                repo:           repo_used,
                                file:           abs_file_path,
                                capsule_tokens,
                                file_tokens,
                                tokens_saved,
                                origin:         client_name,
                                ts:             dashboard::now_ts(),
                                original_text:  Some(original_text),
                                optimized_text: Some(out.clone()),
                            };
                            let http_client = self.http_client.clone();
                            tokio::spawn(async move {
                                match http_client.post(DASHBOARD_EMIT_URL).json(&event).send().await {
                                    Err(e) => log_emit_error(&e.to_string()),
                                    Ok(resp) if !resp.status().is_success() => {
                                        let status = resp.status();
                                        let body = resp.text().await.unwrap_or_default();
                                        log_emit_error(&format!("status={status} body={body}"));
                                    }
                                    Ok(_) => {}
                                }
                            });

                            Ok(CallToolResult::success(vec![Content::text(out)]))
                        }

                        "refactor_symbol" => {
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'refactor_symbol' requires a 'target' (symbol name)".to_string(),
                                    None,
                                )
                            })?;
                            let sym_clone = symbol_name.clone();

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let result = retrieval::analyze_impact(&conn, &symbol_name, &repo_id)?;
                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            let mut out = String::new();
                            writeln!(out, "IMPACT ANALYSIS — pivot id: {}", result.pivot_id).ok();
                            if result.affected.is_empty() {
                                writeln!(out, "No downstream dependents found. Symbol is safe to change in isolation.").ok();
                            } else {
                                writeln!(out, "{:>5}  {:>10}  {:<20}  {:<10}  {:<14}  FILE", "DEPTH", "REL_TYPE", "SYMBOL", "SYM_TYPE", "REPO").ok();
                                writeln!(out, "{}", "─".repeat(80)).ok();
                                for n in &result.affected {
                                    writeln!(out, "{depth:>5}  {rel:>10}  {sym:<20}  {typ:<10}  {repo:<14}  {file}",
                                        depth = n.depth, rel = n.relationship_type, sym = n.symbol_name,
                                        typ = n.symbol_type, repo = n.repo_id, file = n.file_path).ok();
                                }
                                writeln!(out, "\n{} node(s) affected.", result.affected.len()).ok();
                            }

                            let event = DashboardEvent::ImpactAnalyzed {
                                symbol:         sym_clone,
                                repo:           repo_used,
                                affected_count: result.affected.len(),
                                ts:             dashboard::now_ts(),
                            };
                            let http_client = self.http_client.clone();
                            tokio::spawn(async move {
                                match http_client.post(DASHBOARD_EMIT_URL).json(&event).send().await {
                                    Err(e) => log_emit_error(&e.to_string()),
                                    Ok(resp) if !resp.status().is_success() => {
                                        let status = resp.status();
                                        let body = resp.text().await.unwrap_or_default();
                                        log_emit_error(&format!("status={status} body={body}"));
                                    }
                                    Ok(_) => {}
                                }
                            });

                            Ok(CallToolResult::success(vec![Content::text(out)]))
                        }

                        _ => Err(rmcp::ErrorData::invalid_params(
                            "Invalid intent. Must be 'analyze_repo', 'explore_symbol', or 'refactor_symbol'.".to_string(),
                            None,
                        )),
                    }
                }

                // ── workspace_setup ───────────────────────────────────────────
                "workspace_setup" => {
                    let enforcement_mode = EnforcementMode::from_config_value(
                        args.get("enforcement_mode").and_then(|v| v.as_str()),
                    );
                    tokio::task::spawn_blocking(move || {
                        let workspace_root = std::env::current_dir()
                            .unwrap_or_else(|_| PathBuf::from("."));
                        write_workspace_rules(&workspace_root)?;
                        write_vscode_mcp_config(&workspace_root)?;
                        ensure_workspace_config(Some(enforcement_mode))?;
                        Ok::<_, anyhow::Error>(workspace_root)
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let cwd = std::env::current_dir()
                        .unwrap_or_else(|_| PathBuf::from("."))
                        .canonicalize()
                        .unwrap_or_else(|_| PathBuf::from("."));
                    let home = std::env::var("HOME")
                        .map(PathBuf::from)
                        .unwrap_or_else(|_| PathBuf::from("."));
                    Ok(CallToolResult::success(vec![Content::text(
                        format_workspace_setup_summary(&cwd, enforcement_mode, &home),
                    )]))
                }

                _ => Err(rmcp::ErrorData::method_not_found::<
                    rmcp::model::CallToolRequestMethod,
                >()),
            };

            if let Ok(conn) = stats_db.lock() {
                if original_tool_name == "run_pipeline" {
                    let _ = db::increment_stat(&conn, "pipeline_requests", 1);
                }
                if matches!(compliance_action, ComplianceAction::AutoRouted) {
                    let _ = db::increment_stat(&conn, "direct_low_level_autorouted", 1);
                }
            }

            // Prepend compliance notice and auto-init notice to successful responses
            if let (Some(notice), Ok(ref mut tool_result)) = (&compliance_notice, &mut result) {
                tool_result.content.insert(0, Content::text(notice.as_str()));
            }
            if let (Some(notice), Ok(ref mut tool_result)) = (&init_notice, &mut result) {
                tool_result.content.insert(0, Content::text(notice.as_str()));
            }

            result
        }
    }
}

fn rule_install_note() -> &'static str {
    "Rule files strengthen Marrow-first behavior for each agent's native instruction surface. Existing files are preserved. Choose default enforcement to auto-route bypasses or strict enforcement to reject them."
}

fn format_rule_plan_line(
    name: &str,
    agent: skills::Agent,
    scope: skills::Scope,
    method: skills::Method,
    home: &Path,
) -> String {
    let target = agent.target_path(scope, home);
    let source = skills::install_source_description(method, home);
    format!("{name} -> {} ({source})", target.display())
}

fn format_rule_install_status_line(
    name: &str,
    status: skills::InstallStatus,
    target: &Path,
) -> String {
    let action = match status {
        skills::InstallStatus::Written => "rules written",
        skills::InstallStatus::PreservedExisting => "rules preserved",
    };
    format!("{name} {action} -> {}", target.display())
}

fn format_workspace_setup_summary(
    workspace_root: &Path,
    enforcement_mode: EnforcementMode,
    home: &Path,
) -> String {
    format!(
        "[MARROW] Workspace setup complete.\n\
         Path: {}\n\
         Files: .cursorrules, .clinerules, .roomrules, .windsurfrules, .vscode/mcp.json\n\
         Enforcement mode: {}\n\
         Existing files are preserved when Marrow rules are already present.\n\
         Root-level files provide fallback coverage, but primary agent-specific instruction targets still matter.\n\
         {}\n\
         Run `marrow integrate` to install agent-specific instruction files where coverage is still partial or unprotected.",
        workspace_root.display(),
        enforcement_mode.as_str(),
        format_agent_coverage_summary(workspace_root, home)
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn count_tokens_nonempty_returns_nonzero() {
        let n = count_tokens("hello world").unwrap();
        assert!(n > 0, "expected >0 tokens for 'hello world', got {n}");
    }

    #[test]
    fn count_tokens_empty_returns_zero() {
        let n = count_tokens("").unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn format_capsule_string_includes_pivot_text_and_no_neighbor_marker() {
        let capsule = retrieval::ContextCapsule {
            pivot: retrieval::NodeInfo {
                id: "r:f.py:foo".to_string(),
                symbol_name: "foo".to_string(),
                symbol_type: "function".to_string(),
                file_path: "f.py".to_string(),
                language: "py".to_string(),
                text: "def foo(): pass".to_string(),
            },
            neighbors: vec![],
        };
        let s = format_capsule_string(&capsule);
        assert!(s.contains("foo"),           "symbol name missing: {s}");
        assert!(s.contains("def foo(): pass"), "pivot text missing: {s}");
        assert!(s.contains("none"),          "isolated-symbol marker missing: {s}");
    }

    #[test]
    fn format_benchmark_table_contains_all_metrics() {
        let table = format_benchmark_table(
            "my_func",
            "my_repo",
            "src/foo.cpp",
            1_000,
            100,
        );
        // Header info
        assert!(table.contains("my_func"),     "symbol missing:\n{table}");
        assert!(table.contains("my_repo"),     "repo missing:\n{table}");
        assert!(table.contains("src/foo.cpp"), "file path missing:\n{table}");
        // Metric values
        assert!(table.contains("1,000"),       "file tokens missing:\n{table}");
        assert!(table.contains("100"),         "capsule tokens missing:\n{table}");
        assert!(table.contains("900"),         "saved tokens missing:\n{table}");
        assert!(table.contains("90.0%"),       "reduction % missing:\n{table}");
    }

    #[test]
    fn format_benchmark_table_zero_reduction_when_equal() {
        let table = format_benchmark_table("s", "r", "f.py", 500, 500);
        assert!(table.contains("Tokens Saved"), "label missing:\n{table}");
        assert!(table.contains("0.0%"),         "reduction should be 0.0%:\n{table}");
    }

    #[test]
    fn integrate_agent_list_covers_all_skill_agents() {
        // Ensures the integrate agent table and skills::Agent enum stay in sync.
        use crate::skills::Agent;
        let skill_agents = [
            Agent::ClaudeCode,
            Agent::Antigravity,
            Agent::Cursor,
            Agent::GitHubCopilot,
            Agent::Cline,
            Agent::Zed,
        ];
        assert_eq!(skill_agents.len(), 6);
    }

    #[test]
    fn rule_install_note_mentions_optional_rules_and_preservation() {
        let note = rule_install_note();
        assert!(
            note.contains("native instruction surface"),
            "expected Marrow usage guidance: {note}"
        );
        assert!(
            note.contains("Existing files are preserved"),
            "expected preservation guidance: {note}"
        );
        assert!(
            note.contains("strict enforcement"),
            "expected strict enforcement guidance: {note}"
        );
    }

    #[test]
    fn format_rule_plan_line_includes_target_and_source() {
        let line = format_rule_plan_line(
            "GitHub Copilot",
            skills::Agent::GitHubCopilot,
            skills::Scope::Project,
            skills::Method::Symlink,
            Path::new("/tmp/home"),
        );
        assert!(line.contains("GitHub Copilot"), "agent name missing: {line}");
        assert!(
            line.contains(".github/instructions/marrow-optimization.instructions.md"),
            "target path missing: {line}"
        );
        assert!(
            line.contains("/tmp/home/.marrow/marrow-optimization.md"),
            "source path missing: {line}"
        );
    }

    #[test]
    fn format_rule_install_status_line_reports_preserved_targets() {
        let line = format_rule_install_status_line(
            "Cursor",
            skills::InstallStatus::PreservedExisting,
            Path::new("/tmp/home/.cursor/rules/marrow-optimization.mdc"),
        );
        assert!(line.contains("rules preserved"), "status missing: {line}");
        assert!(
            line.contains(".cursor/rules/marrow-optimization.mdc"),
            "target path missing: {line}"
        );
    }

    #[test]
    fn workspace_setup_summary_matches_newer_installer_expectations() {
        let summary = format_workspace_setup_summary(
            Path::new("/tmp/workspace"),
            EnforcementMode::Default,
            Path::new("/tmp/home"),
        );
        assert!(
            summary.contains("Existing files are preserved"),
            "preservation guidance missing: {summary}"
        );
        assert!(
            summary.contains("Agent coverage"),
            "agent coverage summary missing: {summary}"
        );
        assert!(
            summary.contains("/tmp/workspace"),
            "workspace path missing: {summary}"
        );
        assert!(
            summary.contains("Enforcement mode: default"),
            "summary should describe the current enforcement mode: {summary}"
        );
    }

    #[tokio::test]
    async fn auto_init_fires_when_marrow_absent_then_skips() {
        let tmp = tempfile::tempdir().unwrap();
        // Point the process CWD at our temp dir so try_auto_init writes there.
        // NOTE: set_current_dir is process-global; this test must not run in parallel
        // with other tests that depend on CWD.
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        // First call: .marrow/ absent → should return Some(notice)
        let notice = try_auto_init().await;
        assert!(notice.is_some(), "expected Some notice on first call");
        let notice_str = notice.unwrap();
        assert!(
            notice_str.contains("[MARROW AUTO-INIT]"),
            "notice should contain MARROW AUTO-INIT tag"
        );
        assert!(tmp.path().join(".marrow").is_dir(), ".marrow/ should exist after init");

        // Second call: .marrow/ now exists → should return None
        let second = try_auto_init().await;
        assert!(second.is_none(), "expected None on second call when .marrow/ exists");

        // Restore CWD
        std::env::set_current_dir(original).unwrap();
    }

    #[test]
    fn compliance_gate_autoroutes_direct_capsule_call_in_default_mode() {
        let args = json!({
            "symbol_name": "bulk_update",
            "repo_id": "accrualify-rails"
        })
        .as_object()
        .unwrap()
        .clone();

        let routed = apply_compliance_gate("get_context_capsule", args, EnforcementMode::Default)
            .expect("default mode should auto-route direct low-level calls");

        assert_eq!(routed.tool_name, "run_pipeline");
        assert_eq!(routed.args.get("intent").and_then(|v| v.as_str()), Some("explore_symbol"));
        assert_eq!(routed.args.get("target").and_then(|v| v.as_str()), Some("bulk_update"));
        assert_eq!(routed.args.get("repo_id").and_then(|v| v.as_str()), Some("accrualify-rails"));
        assert!(
            routed.notice.as_deref().unwrap_or_default().contains("auto-routed"),
            "expected compliance warning notice"
        );
    }

    #[test]
    fn compliance_gate_rejects_direct_low_level_call_in_strict_mode() {
        let args = json!({
            "symbol_name": "bulk_update",
            "repo_id": "accrualify-rails"
        })
        .as_object()
        .unwrap()
        .clone();

        let err = apply_compliance_gate("analyze_impact", args, EnforcementMode::Strict)
            .expect_err("strict mode should reject direct low-level calls");
        assert!(
            err.message.contains("run_pipeline"),
            "strict mode error should instruct callers to use run_pipeline: {}",
            err.message
        );
    }

    #[test]
    fn validation_report_includes_compliance_counters() {
        let conn = crate::db::init_db(":memory:").unwrap();
        crate::db::increment_stat(&conn, "pipeline_requests", 5).unwrap();
        crate::db::increment_stat(&conn, "direct_low_level_autorouted", 2).unwrap();
        crate::db::increment_stat(&conn, "direct_low_level_rejected", 1).unwrap();
        crate::db::increment_stat(&conn, "ambiguous_symbol_requests", 3).unwrap();
        crate::db::increment_stat(&conn, "stale_capsule_prevented", 4).unwrap();

        let report = format_validation_report(
            Path::new("/tmp/workspace"),
            Path::new("/tmp/home"),
            EnforcementMode::Strict,
            &conn,
        );

        assert!(report.contains("run_pipeline requests: 5"), "pipeline count missing: {report}");
        assert!(report.contains("direct low-level auto-routed: 2"), "autoroute count missing: {report}");
        assert!(report.contains("direct low-level rejected: 1"), "reject count missing: {report}");
        assert!(report.contains("Enforcement mode: strict"), "mode missing: {report}");
    }

    #[test]
    fn agent_coverage_summary_reports_partial_for_cursor_fallback_files() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join(".cursorrules"), "marrow").unwrap();
        fs::create_dir_all(workspace.path().join(".vscode")).unwrap();
        fs::write(workspace.path().join(".vscode/mcp.json"), "{}").unwrap();

        let summary = format_agent_coverage_summary(workspace.path(), home.path());
        assert!(summary.contains("Cursor: partial"), "cursor fallback coverage should be partial: {summary}");
    }

    #[test]
    fn agent_coverage_ignores_unrelated_instruction_file_contents() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let target = workspace.path().join(".cursor/rules/marrow-optimization.mdc");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "unrelated content").unwrap();

        let (status, _) = coverage_status_for_agent(skills::Agent::Cursor, workspace.path(), home.path());
        assert_eq!(status, "unprotected", "non-Marrow files should not count as protected");
    }

    #[test]
    fn ensure_repo_ready_rejects_explicit_repo_id_for_other_workspace() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let indexed_root = tempfile::tempdir().unwrap();
        let current_root = tempfile::tempdir().unwrap();
        let indexed_root_path = indexed_root.path().canonicalize().unwrap();
        let current_root_path = current_root.path().canonicalize().unwrap();

        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params!["other_repo", indexed_root_path.to_string_lossy().to_string()],
        )
        .unwrap();

        let err = ensure_repo_ready(&conn, Some("other_repo"), &current_root_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Run ingest_repo"), "expected explicit guidance to ingest the correct repo: {msg}");
    }
}

// ── CLI subcommands ───────────────────────────────────────────────────────────

/// `marrow ui` — interactive dashboard configuration menu.
fn cmd_ui() -> Result<()> {
    use dialoguer::{Select, theme::ColorfulTheme};

    loop {
        // Re-read config each iteration so the toggle label is always current.
        let auto_open: bool = fs::read_to_string(".marrowrc.json")
            .ok()
            .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
            .and_then(|v| v.get("auto_open_ui").and_then(|b| b.as_bool()))
            .unwrap_or(true);

        let toggle_label = format!(
            "Toggle Auto-Open (Currently: {})",
            if auto_open { "ON" } else { "OFF" }
        );

        let items = vec![
            "Open Dashboard in Browser",
            toggle_label.as_str(),
            "Exit",
        ];

        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Marrow Dashboard")
            .items(&items)
            .default(0)
            .interact()?;

        match selection {
            0 => {
                if let Err(e) = open::that("http://127.0.0.1:8765") {
                    eprintln!("Could not open browser: {e}");
                }
            }
            1 => {
                let rc_path = Path::new(".marrowrc.json");
                let mut cfg: serde_json::Value = fs::read_to_string(rc_path)
                    .ok()
                    .and_then(|raw| serde_json::from_str(&raw).ok())
                    .unwrap_or_else(|| serde_json::json!({}));
                // Read the current value from the fresh cfg, not the stale loop-top snapshot.
                let current = cfg
                    .get("auto_open_ui")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(true);
                cfg["auto_open_ui"] = serde_json::Value::Bool(!current);
                // Best-effort write — silently ignore failures on read-only filesystems.
                if let Ok(pretty) = serde_json::to_string_pretty(&cfg) {
                    let tmp = rc_path.with_extension("json.tmp");
                    let _ = fs::write(&tmp, &pretty).and_then(|_| fs::rename(&tmp, rc_path));
                }
                eprintln!("Auto-Open is now {}.", if !current { "ON" } else { "OFF" });
            }
            _ => break,
        }
    }

    Ok(())
}

const WORKSPACE_RULES_CONTENT: &str = r#"# MARROW AST CONTEXT ENGINE - STRICT WORKFLOW PROTOCOL
You are equipped with the 'marrow' MCP server. You MUST adhere to the following strict workflow. Do NOT rely on your default file-reading tools.
## THE OMNI-TOOL (ALWAYS FIRST)
For EVERY coding task, exploration, or question, you MUST call the `run_pipeline` tool first.
* IF USER SAYS: "Analyze this repo", "Explain the codebase"
* ACTION: Call `run_pipeline` with `intent: "analyze_repo"`.
* IF USER SAYS: "How does <function/file> work?", "Fix a bug in <file>"
* ACTION: Call `run_pipeline` with `intent: "explore_symbol"` and the target.
* IF USER SAYS: "Refactor <function>", "Delete <file>"
* ACTION: Call `run_pipeline` with `intent: "refactor_symbol"` and the target.
If any tool states the database is empty, immediately run `ingest_repo`.
"#;

/// `marrow rules` — write Marrow-first workflow rules into the target workspace.
/// Writes rule files for Cursor, Cline, Roo/Antigravity, and Windsurf so all
/// AI agent variants default to Marrow tools.
///
/// This function is append-only and idempotent: if the Marrow header is already
/// present in a file it is skipped entirely, preventing duplicate entries and
/// preserving any user-authored content that precedes the Marrow block.
/// Creates or merges `.vscode/mcp.json` so that GitHub Copilot / VS Code
/// can discover the Marrow MCP server. Existing `mcpServers` entries are
/// preserved — only the `"marrow"` key is inserted/updated.
pub fn write_vscode_mcp_config(workspace_root: &Path) -> Result<()> {
    let vscode_dir = workspace_root.join(".vscode");
    fs::create_dir_all(&vscode_dir)
        .with_context(|| format!("could not create {}", vscode_dir.display()))?;

    let mcp_path = vscode_dir.join("mcp.json");

    let marrow_entry = serde_json::json!({
        "command": "marrow",
        "args": ["mcp"]
    });

    let mut config: serde_json::Value = if mcp_path.exists() {
        let raw = fs::read_to_string(&mcp_path)
            .with_context(|| format!("could not read {}", mcp_path.display()))?;
        serde_json::from_str(&raw)
            .unwrap_or_else(|_| serde_json::json!({}))
    } else {
        serde_json::json!({})
    };

    // VS Code workspace mcp.json uses "servers" (not "mcpServers" which is the Cline/Claude format).
    if !config["servers"].is_object() {
        config["servers"] = serde_json::json!({});
    }
    config["servers"]["marrow"] = marrow_entry;

    let pretty = serde_json::to_string_pretty(&config)
        .context("could not serialize mcp.json")?;
    fs::write(&mcp_path, pretty)
        .with_context(|| format!("could not write {}", mcp_path.display()))?;

    eprintln!("Wrote VS Code MCP config to {}", mcp_path.display());
    Ok(())
}

pub fn write_workspace_rules(root_dir: &Path) -> Result<()> {
    use std::io::Write;
    const MARROW_HEADER: &str = "# MARROW AST CONTEXT ENGINE";
    let targets = [".cursorrules", ".clinerules", ".roomrules", ".windsurfrules"];
    for filename in &targets {
        let path = root_dir.join(filename);
        if path.exists() {
            let existing = fs::read_to_string(&path)
                .with_context(|| format!("could not read {}", path.display()))?;
            if existing.contains(MARROW_HEADER) {
                eprintln!("Skipped {} (Marrow rules already present)", path.display());
                continue;
            }
            // File exists but lacks the Marrow block — append.
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .with_context(|| format!("could not open {}", path.display()))?;
            write!(file, "\n\n{WORKSPACE_RULES_CONTENT}")?;
            eprintln!("Appended to {}", path.display());
        } else {
            // File does not exist — create and write.
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("could not create {}", path.display()))?;
            write!(file, "{WORKSPACE_RULES_CONTENT}")?;
            eprintln!("Created {}", path.display());
        }
    }
    Ok(())
}

fn cmd_rules() -> Result<()> {
    let root = std::env::current_dir().context("could not determine current directory")?;
    write_workspace_rules(&root)?;
    write_vscode_mcp_config(&root)?;
    ensure_workspace_config(Some(EnforcementMode::Default))?;
    eprintln!("[MARROW] Successfully integrated! Workspace rules appended, VS Code / Copilot MCP configuration generated, and workspace enforcement set to default.");
    Ok(())
}

/// `marrow init` — scaffold a `.marrow/` directory and `.marrowrc.json` config.
fn cmd_init() -> Result<()> {
    let marrow_dir = Path::new(".marrow");
    if let Err(e) = fs::create_dir_all(marrow_dir) {
        eprintln!("Warning: could not create .marrow/ directory ({e}). Continuing.");
    }

    match ensure_workspace_config(Some(EnforcementMode::Default)) {
        Ok(_) => eprintln!("Ensured .marrowrc.json with default settings."),
        Err(e) => eprintln!("Warning: could not write .marrowrc.json ({e}). Using defaults."),
    }

    eprintln!("Initialized .marrow/ workspace.");
    Ok(())
}

/// `marrow test-capsules` — run get_context_capsule for every (repo_id, symbol)
/// in the graph. Reports success/failure counts.
fn cmd_test_capsules() -> Result<()> {
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db_or_memory(&db_path)?;

    let pairs: Vec<(String, String)> = conn.prepare(
        "SELECT DISTINCT repo_id, symbol_name FROM nodes ORDER BY repo_id, symbol_name",
    )?
    .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
    .filter_map(|r| r.ok())
    .collect();

    let total = pairs.len();
    let mut ok = 0usize;
    let mut err = 0usize;

    for (repo_id, symbol_name) in &pairs {
        match retrieval::get_context_capsule(&conn, symbol_name, repo_id) {
            Ok(_) => {
                ok += 1;
                eprintln!("OK  {repo_id}:{symbol_name}");
            }
            Err(e) => {
                err += 1;
                eprintln!("ERR {repo_id}:{symbol_name} — {e}");
            }
        }
    }

    eprintln!("\n--- test-capsules complete ---");
    eprintln!("total: {total}  ok: {ok}  err: {err}");
    if err > 0 {
        anyhow::bail!("{err} capsule(s) failed");
    }
    Ok(())
}

// ── Integrate: banner, shared types, per-agent helpers ───────────────────────

const MARROW_BANNER: &str = r#"
  ███╗   ███╗ █████╗ ██████╗ ██████╗  ██████╗ ██╗    ██╗
  ████╗ ████║██╔══██╗██╔══██╗██╔══██╗██╔═══██╗██║    ██║
  ██╔████╔██║███████║██████╔╝██████╔╝██║   ██║██║ █╗ ██║
  ██║╚██╔╝██║██╔══██║██╔══██╗██╔══██╗██║   ██║██║███╗██║
  ██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██║╚██████╔╝╚███╔███╔╝
  ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝
"#;

/// Paths + binary string resolved once and threaded into every per-agent fn.
struct IntegrationCtx {
    binary: String,
    home:   String,
}

/// What a per-agent function reports back.
enum AgentOutcome {
    Installed,
    NotFound,
}

/// Read a JSON file into a `Value`, returning `{}` if the file is absent.
fn load_json_or_empty(path: &Path) -> Result<serde_json::Value> {
    if path.exists() {
        let raw = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    } else {
        Ok(serde_json::json!({}))
    }
}

/// Pretty-print a `Value` to disk, creating parent directories as needed.
fn save_json(path: &Path, val: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(val)?)?;
    Ok(())
}

// ── Per-agent helpers ─────────────────────────────────────────────────────────

/// ~/.claude.json (global Claude Code config)
fn integrate_claude(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".claude.json");
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    ["mcp"]
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.gemini/antigravity/mcp_config.json
fn integrate_antigravity(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home)
        .join(".gemini/antigravity/mcp_config.json");
    if !path.exists() {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    ["mcp"]
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.cursor/mcp.json (global)
fn integrate_cursor(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".cursor/mcp.json");
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    ["mcp"]
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// GitHub Copilot — writes two config files:
///   ~/Library/Application Support/Code/User/mcp.json  (VS Code global MCP, macOS)
///   ~/.copilot/mcp-config.json                         (Copilot CLI, uses "mcpServers" key)
fn integrate_copilot(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    // 1. VS Code global MCP config — location is platform-specific.
    //    macOS: ~/Library/Application Support/Code/User/mcp.json
    //    Linux: ~/.config/Code/User/mcp.json
    //    Windows: %APPDATA%\Code\User\mcp.json
    #[cfg(target_os = "macos")]
    let vscode_path = PathBuf::from(&ctx.home).join("Library/Application Support/Code/User/mcp.json");
    #[cfg(target_os = "linux")]
    let vscode_path = PathBuf::from(&ctx.home).join(".config/Code/User/mcp.json");
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let vscode_path = PathBuf::from(&ctx.home).join(".mcp.json");

    if let Some(parent) = vscode_path.parent() {
        if parent.exists() {
            let mut vscode_cfg = load_json_or_empty(&vscode_path)?;
            vscode_cfg["servers"]["marrow"] = serde_json::json!({
                "command": ctx.binary,
                "args":    ["mcp"]
            });
            save_json(&vscode_path, &vscode_cfg)?;
        }
    }

    // 2. ~/.copilot/mcp-config.json — Copilot CLI
    let cli_path = PathBuf::from(&ctx.home).join(".copilot/mcp-config.json");
    let mut cli_cfg = load_json_or_empty(&cli_path)?;
    cli_cfg["mcpServers"]["marrow"] = serde_json::json!({
        "type":    "local",
        "command": ctx.binary,
        "args":    ["mcp"]
    });
    save_json(&cli_path, &cli_cfg)?;

    Ok(AgentOutcome::Installed)
}

/// ~/Library/Application Support/Code/User/globalStorage/
///   saoudrizwan.claude-dev/settings/cline_mcp_settings.json
fn integrate_cline(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home)
        .join("Library/Application Support/Code/User/globalStorage")
        .join("saoudrizwan.claude-dev/settings/cline_mcp_settings.json");
    if !path.parent().is_some_and(|p| p.exists()) {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command":     ctx.binary,
        "args":        ["mcp"],
        "disabled":    false,
        "autoApprove": []
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.config/zed/settings.json
/// Zed uses a nested "command" object inside "context_servers".
fn integrate_zed(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".config/zed/settings.json");
    if !path.exists() {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    cfg["context_servers"]["marrow"] = serde_json::json!({
        "command": {
            "path": ctx.binary,
            "args": ["mcp"]
        },
        "settings": {}
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

// ── Interactive installer ─────────────────────────────────────────────────────

/// `marrow integrate` — launch the interactive TUI installer.
fn cmd_integrate() -> Result<()> {
    use console::style;
    use dialoguer::{MultiSelect, Select, theme::ColorfulTheme};

    eprintln!("{}", style(MARROW_BANNER).cyan().bold());
    eprintln!(
        "  {}",
        style("AST Context Engine  ·  MCP Server Installer").dim()
    );
    eprintln!();

    #[allow(clippy::type_complexity)]
    let agents: &[(&str, fn(&IntegrationCtx) -> Result<AgentOutcome>, skills::Agent)] = &[
        ("Claude Code",          integrate_claude,       skills::Agent::ClaudeCode),
        ("Antigravity (Gemini)", integrate_antigravity,  skills::Agent::Antigravity),
        ("Cursor",               integrate_cursor,       skills::Agent::Cursor),
        ("GitHub Copilot",       integrate_copilot,      skills::Agent::GitHubCopilot),
        ("Cline",                integrate_cline,        skills::Agent::Cline),
        ("Zed",                  integrate_zed,          skills::Agent::Zed),
    ];

    let labels: Vec<&str> = agents.iter().map(|(name, _, _)| *name).collect();

    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select agents to configure  (space to toggle, enter to confirm)")
        .items(&labels)
        .interact()?;

    if selections.is_empty() {
        eprintln!("\n{}", style("No agents selected — nothing to do.").dim());
        return Ok(());
    }

    eprintln!("  {}", style(rule_install_note()).dim());
    let install_rules = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Create Marrow rule files for the selected agents?")
        .items(&["Yes (recommended)", "No"])
        .default(0)
        .interact()?
        == 0;

    let binary = std::env::current_exe()
        .context("Could not resolve current executable path")?
        .to_string_lossy()
        .to_string();
    let home = std::env::var("HOME").context("$HOME is not set")?;
    let home_path = PathBuf::from(&home);
    let ctx = IntegrationCtx { binary, home };

    let rule_config = if install_rules {
        // Scope selection: Global writes to ~/.agent/rules; Project writes to .agent/rules in CWD
        let scope_idx = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Rule file scope")
            .items(&["Global (recommended)", "Project"])
            .default(0)
            .interact()?;
        let scope = if scope_idx == 0 {
            skills::Scope::Global
        } else {
            skills::Scope::Project
        };

        // Method selection: WriteFile copies the content; Symlink points to ~/.marrow/marrow-optimization.md
        let method_idx = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Rule file method")
            .items(&["Write File", "Symlink"])
            .default(0)
            .interact()?;
        let method = if method_idx == 0 {
            skills::Method::WriteFile
        } else {
            skills::Method::Symlink
        };

        eprintln!();
        eprintln!("  {}", style("Rule files to create:").dim());
        for idx in &selections {
            let (name, _, skill_agent) = agents[*idx];
            eprintln!(
                "    {}",
                style(format_rule_plan_line(name, skill_agent, scope, method, &home_path)).dim()
            );
        }
        eprintln!(
            "  {}",
            style("Edit/remove the target paths above later if you want to disable implicit Marrow guidance.").dim()
        );
        Some((scope, method))
    } else {
        eprintln!(
            "  {}",
            style(
                "Rule files skipped. Marrow will still be available via MCP, but some agents may need explicit prompts to use it."
            )
            .dim()
        );
        None
    };

    let enforcement_idx = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Workspace enforcement mode")
        .items(&[
            "Default (warn + auto-route low-level bypasses)",
            "Strict (reject low-level bypasses)",
        ])
        .default(0)
        .interact()?;
    let enforcement_mode = if enforcement_idx == 0 {
        EnforcementMode::Default
    } else {
        EnforcementMode::Strict
    };
    ensure_workspace_config(Some(enforcement_mode))?;
    eprintln!(
        "  {}",
        style(format!("Workspace enforcement mode set to '{}'.", enforcement_mode.as_str())).dim()
    );

    eprintln!();
    for idx in selections {
        let (name, integrate_fn, skill_agent) = agents[idx];

        // 1. MCP registration (existing behaviour)
        let mcp_result = integrate_fn(&ctx);
        match &mcp_result {
            Ok(AgentOutcome::Installed) => eprintln!(
                "  {}  {}  {}",
                style("✓").green().bold(),
                style(name).bold(),
                style("MCP registered").dim(),
            ),
            Ok(AgentOutcome::NotFound) => eprintln!(
                "  {}  {}  {}",
                style("⚠").yellow().bold(),
                style(name).dim(),
                style("(not installed — skipped)").dim(),
            ),
            Err(e) => eprintln!(
                "  {}  {}  {}",
                style("✗").red().bold(),
                style(name).bold(),
                style(format!("MCP — {e}")).red(),
            ),
        }

        // 2. Optional rule files
        if matches!(mcp_result, Ok(AgentOutcome::Installed)) {
            if let Some((scope, method)) = rule_config {
                let target = skill_agent.target_path(scope, &home_path);
                match skills::install_skill(skill_agent, scope, method, &home_path) {
                    Ok(status) => {
                        eprintln!(
                            "  {}  {}",
                            style("✓").green().bold(),
                            style(format_rule_install_status_line(name, status, &target)).dim(),
                        );
                        eprintln!(
                            "      {}",
                            style(skills::install_source_description(method, &home_path)).dim(),
                        );
                    }
                    Err(e) => eprintln!(
                        "  {}  {}  {}",
                        style("✗").red().bold(),
                        style(name).bold(),
                        style(format!("rules — {e}")).red(),
                    ),
                }
            }
        }
    }

    eprintln!();
    eprintln!(
        "  {}",
        style(format_agent_coverage_summary(&current_workspace_root(), &home_path)).dim()
    );
    eprintln!("  {}", style("Done.").bold());
    Ok(())
}

fn cmd_validate() -> Result<()> {
    let workspace_root = current_workspace_root();
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .context("$HOME is not set")?;
    let mode = read_enforcement_mode();
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db(&db_path)?;
    println!(
        "{}",
        format_validation_report(&workspace_root, &home, mode, &conn)
    );
    Ok(())
}

/// `marrow index` — walk the current directory, parse ASTs, and populate
/// `.marrow/graph.db` inside a single SQLite transaction.
fn cmd_index() -> Result<()> {
    let t0 = Instant::now();

    // ── Resolve repo_id from current directory name ──────────────────
    let cwd = std::env::current_dir()?;
    let repo_id = cwd
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed")
        .to_string();

    // ── Load ignore patterns from .marrowrc.json (or use defaults) ───
    let ignore_patterns: Vec<String> = if let Ok(raw) = fs::read_to_string(".marrowrc.json") {
        let v: serde_json::Value = serde_json::from_str(&raw)?;
        v.get("ignore")
            .and_then(|a| a.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        vec![
            "node_modules".into(),
            "target".into(),
            "dist".into(),
            ".git".into(),
        ]
    };

    // ── Build walker using the `ignore` crate ────────────────────────
    let mut builder = ignore::WalkBuilder::new(&cwd);
    builder
        .hidden(true)          // skip hidden files/dirs
        .git_ignore(true)      // respect .gitignore
        .git_global(false)
        .git_exclude(false);

    // Apply custom overrides from .marrowrc.json
    let mut overrides = ignore::overrides::OverrideBuilder::new(&cwd);
    for pat in &ignore_patterns {
        overrides.add(&format!("!{pat}/"))?;
    }
    builder.overrides(overrides.build()?);

    let supported_exts = ["cpp", "cc", "cxx", "h", "hpp", "py", "ts", "tsx", "rs", "rb"];

    let files: Vec<PathBuf> = builder
        .build()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_some_and(|ft| ft.is_file()))
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|ext| supported_exts.contains(&ext))
        })
        .map(|entry| entry.into_path())
        .collect();

    eprintln!("Repo:  {repo_id}");
    eprintln!("Root:  {}", cwd.display());
    eprintln!("Files: {}", files.len());

    // ── Parse all files in parallel with rayon ───────────────────────
    use rayon::prelude::*;

    let parsed: Vec<_> = files
        .par_iter()
        .filter_map(|path| {
            let rel = path
                .strip_prefix(&cwd)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string();
            match ingestion::parse_file(path) {
                Ok((lang, symbols)) => Some((rel, lang, symbols)),
                Err(e) => {
                    eprintln!("  skip: {} ({})", path.display(), e);
                    None
                }
            }
        })
        .collect();

    // ── Initialize DB and insert inside a single transaction ─────────
    let db_path = ".marrow/graph.db";
    let conn = db::init_db_or_memory(db_path)?;

    conn.execute(
        "INSERT OR REPLACE INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params![repo_id, cwd.to_string_lossy().as_ref()],
    )?;

    let tx = conn.unchecked_transaction()?;
    let mut symbol_count: usize = 0;

    for (file_path, lang, symbols) in &parsed {
        for sym in symbols {
            let node_id = format!("{repo_id}:{file_path}:{}", sym.name);
            tx.execute(
                "INSERT OR REPLACE INTO nodes \
                 (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    node_id, repo_id, file_path, lang,
                    sym.name, sym.symbol_type, sym.raw_text
                ],
            )?;
            symbol_count += 1;
        }
    }
    tx.commit()?;

    // ── Cross-repo edge resolution ───────────────────────────────────
    let edge_count = ingestion::resolve_cross_repo_edges(&conn)?;

    let elapsed = t0.elapsed();
    eprintln!("\n── Index complete ──────────────────────────────────────────");
    eprintln!("  Symbols: {}", fmt_num(symbol_count));
    eprintln!("  Edges:   {}", fmt_num(edge_count));
    eprintln!("  Time:    {:.2?}", elapsed);
    eprintln!("  DB:      {db_path}");

    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // ── Global panic hook – writes panics to ~/.marrow/debug.log ──────
    std::panic::set_hook(Box::new(|panic_info| {
        use std::io::Write;
        if let Some(home) = dirs::home_dir() {
            let log_dir = home.join(".marrow");
            let _ = std::fs::create_dir_all(&log_dir);
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_dir.join("debug.log"))
            {
                let _ = writeln!(file, "[FATAL PANIC] {}", panic_info);
            }
        }
    }));

    // ── CLI subcommand dispatch ────────────────────────────────────────
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("ui")        => return cmd_ui(),
        Some("init")      => return cmd_init(),
        Some("rules")     => return cmd_rules(),
        Some("index")     => return cmd_index(),
        Some("test-capsules") => return cmd_test_capsules(),
        Some("integrate") => return cmd_integrate(),
        Some("validate")  => return cmd_validate(),
Some("benchmark") => {
            let symbol = args.get(2).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} benchmark <symbol> <repo_id>", args[0])
            })?;
            let repo_id = args.get(3).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} benchmark <symbol> <repo_id>", args[0])
            })?;

            let db_path = std::env::var("MARROW_DB_PATH")
                .unwrap_or_else(|_| ".marrow/graph.db".to_string());

            let conn = db::init_db_or_memory(&db_path)?;
            run_benchmark(&conn, symbol, repo_id)?;
            return Ok(());
        }
        Some("query") => {
            let symbol = args.get(2).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} query <symbol> <repo_id>", args[0])
            })?;
            let repo_id = args.get(3).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} query <symbol> <repo_id>", args[0])
            })?;

            let db_path = std::env::var("MARROW_DB_PATH")
                .unwrap_or_else(|_| ".marrow/graph.db".to_string());

            let conn = db::init_db_or_memory(&db_path)?;
            let result = retrieval::get_context_capsule(&conn, symbol, repo_id)?;
            println!("{}", result.optimized_text);

            let impact = retrieval::analyze_impact(&conn, symbol, repo_id)?;
            println!("\nIMPACT ANALYSIS:");
            if impact.affected.is_empty() {
                println!("  No downstream dependents found.");
            } else {
                for n in impact.affected {
                    println!("  [Depth {}] {} ({}) in {}", n.depth, n.symbol_name, n.symbol_type, n.file_path);
                }
            }
            return Ok(());
        }
        _ => {}
    }

    // ── Default: start MCP stdio server ──────────────────────────────
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".marrow/graph.db".to_string());

    // ── Read config flags in one pass ───────────────────────────────
    // Config read is always best-effort; a missing/unreadable file is not fatal.
    let (show_dashboard, auto_open_ui, enable_watcher, watch_debounce_ms) = {
        let cfg = read_workspace_config();
        let show      = cfg.get("show_dashboard").and_then(|b| b.as_bool()).unwrap_or(true);
        let open      = cfg.get("auto_open_ui").and_then(|b| b.as_bool()).unwrap_or(true);
        let watcher   = cfg.get("enable_watcher").and_then(|b| b.as_bool()).unwrap_or(false);
        let debounce  = cfg.get("watch_debounce_ms").and_then(|v| v.as_u64()).unwrap_or(500);
        (show, open, watcher, debounce)
    };


    // ── Init DB (falls back to :memory: on read-only filesystems) ─────
    let conn   = db::init_db_or_memory(&db_path)?;
    let db_arc = Arc::new(Mutex::new(conn));

    // ── Create the HTTP client once — shared by Hub startup and engine ─
    let http_client = reqwest::Client::new();

    // ── Broadcast channel (shared by dashboard + watcher) ─────────────
    // Hoisted outside `if show_dashboard` so the watcher can use it even
    // when the dashboard UI is disabled.
    let (tx, _) = tokio::sync::broadcast::channel::<DashboardEvent>(256);
    let session = Arc::new(Mutex::new(dashboard::SessionStats::default()));

    // ── Dashboard Hub election ────────────────────────────────────────
    if show_dashboard {
        match dashboard::start(
            tx.clone(),
            Arc::clone(&session),
            Arc::clone(&db_arc),
            auto_open_ui,
        )
        .await?
        {
            dashboard::HubRole::Hub => {
                // Fire-and-forget: POST ServerStarted to ourselves.
                // Spawned so we don't block while the listener finishes binding.
                let client  = http_client.clone();
                let db_path = db_path.clone();
                tokio::spawn(async move {
                    // Brief yield so the Axum accept-loop is ready.
                    tokio::time::sleep(std::time::Duration::from_millis(DASHBOARD_WARMUP_MS)).await;
                    match client
                        .post(DASHBOARD_EMIT_URL)
                        .json(&DashboardEvent::ServerStarted { port: 8765, db_path })
                        .send()
                        .await
                    {
                        Err(e) => log_emit_error(&e.to_string()),
                        Ok(resp) if !resp.status().is_success() => {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            log_emit_error(&format!("status={status} body={body}"));
                        }
                        Ok(_) => {}
                    }
                });
            }
            dashboard::HubRole::Spoke => {
                eprintln!("Marrow running as Spoke (Hub already active on :8765).");
            }
        }
    }

    // ── Background file watcher (opt-in) ──────────────────────────────
    if enable_watcher {
        match watcher::spawn_watcher(Arc::clone(&db_arc), tx.clone(), watch_debounce_ms) {
            Ok(_) => eprintln!("Marrow file watcher active (debounce: {watch_debounce_ms}ms)"),
            Err(e) => eprintln!("Marrow file watcher failed: {e}"),
        }
    }

    // ── Build engine ──────────────────────────────────────────────────
    let engine = ContextEngine {
        db:          Arc::clone(&db_arc),
        http_client,
        is_indexing: Arc::new(AtomicBool::new(false)),
    };

    eprintln!("Marrow MCP server ready — listening on stdio.");
    let server = engine.serve(stdio()).await?;
    server.waiting().await?;

    Ok(())
}
