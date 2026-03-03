mod db;
mod dashboard;
mod ingestion;
mod retrieval;

use std::{
    fmt::Write as FmtWrite,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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

/// Milliseconds to wait after the Axum server spawns before sending the
/// first ServerStarted event. The listener is ready almost instantly but
/// there is a brief window between `tokio::spawn` returning and the first
/// `accept()` completing. A missed ServerStarted is cosmetic (dashboard UI
/// only), so this is best-effort rather than a hard synchronisation point.
const DASHBOARD_WARMUP_MS: u64 = 50;

// ── Capsule formatting ────────────────────────────────────────────────────────

/// Format a ContextCapsule as the plain-text string sent to the LLM.
/// Extracted from `call_tool` so the benchmark subcommand can reuse it.
fn format_capsule_string(capsule: &retrieval::ContextCapsule) -> String {
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
    // ── Step 1: resolve file path ────────────────────────────────────
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

    // ── Step 2: resolve repo root and read the full source file ──────
    let root_path: String = conn
        .query_row(
            "SELECT root_path FROM repositories WHERE id = ?1",
            rusqlite::params![repo_id],
            |row| row.get(0),
        )
        .map_err(|_| {
            anyhow::anyhow!(
                "Repo '{}' not found in the database. Has it been ingested?",
                repo_id
            )
        })?;

    let abs_path = PathBuf::from(&root_path).join(&file_path);
    let file_content = fs::read_to_string(&abs_path)
        .with_context(|| format!(
            "Could not read source file at {}. \
             Check the file exists and is readable, or re-ingest the repo.",
            abs_path.display()
        ))?;

    // ── Step 3: build and format the capsule ─────────────────────────
    let capsule = retrieval::get_context_capsule(conn, symbol, repo_id)?;
    let capsule_str = format_capsule_string(&capsule);

    // ── Step 4: count tokens ─────────────────────────────────────────
    let file_tokens    = count_tokens(&file_content)?;
    let capsule_tokens = count_tokens(&capsule_str)?;

    // ── Step 5: print table ──────────────────────────────────────────
    println!(
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
    client_name: Arc<Mutex<String>>,
    http_client: reqwest::Client,
}

impl ContextEngine {
    #[allow(dead_code)]
    fn new(
        db_path:     &str,
        client_name: Arc<Mutex<String>>,
        http_client: reqwest::Client,
    ) -> Result<Self> {
        let conn = db::init_db(db_path)?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            client_name,
            http_client,
        })
    }

    /// Convert a `serde_json::Value` (must be an Object) into the
    /// `Arc<serde_json::Map<String, Value>>` that `Tool::new` expects.
    fn schema(v: serde_json::Value) -> Arc<serde_json::Map<String, serde_json::Value>> {
        Arc::new(v.as_object().expect("schema must be a JSON object").clone())
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
            ..Default::default()
        }
    }

    // ── Initialize override: capture client name ──────────────────────────────

    fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<InitializeResult, rmcp::ErrorData>> + Send + '_ {
        // Capture the connecting client's name from the MCP handshake.
        if let Ok(mut name) = self.client_name.lock() {
            *name = request.client_info.name.clone();
        }

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
                "Fetch the full source of a pivot symbol plus condensed signatures of its \
                 depth-1 neighbors (callers, callees, imports). Returns a Context Capsule \
                 optimised for LLM consumption.",
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
                "Map the blast radius of a proposed change. Recursively traverses the \
                 dependency graph to find every transitive caller/importer across all \
                 repos up to depth 10.",
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
                "Parse a local repository with tree-sitter and populate (or refresh) \
                 the AST dependency graph in the SQLite database. Run this before \
                 querying a repo for the first time, or after significant code changes.",
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
                        }
                    },
                    "required": ["repo_id", "root_path"]
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
            let args = request.arguments.unwrap_or_default();

            match request.name.as_ref() {
                // ── get_context_capsule ───────────────────────────────────────
                "get_context_capsule" => {
                    let symbol_name  = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id      = Self::require_str(&args, "repo_id")?.to_string();
                    let client_name  = self.client_name.lock()
                                           .map(|g| g.clone())
                                           .unwrap_or_else(|_| "Unknown Client".to_string());

                    let sym_for_event  = symbol_name.clone();
                    let repo_for_event = repo_id.clone();

                    let (out, capsule_tokens, file_tokens, file_path) =
                        tokio::task::spawn_blocking(move || {
                            let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;

                            let capsule      = retrieval::get_context_capsule(&conn, &symbol_name, &repo_id)?;
                            let file_path    = capsule.pivot.file_path.clone();
                            let out          = format_capsule_string(&capsule);
                            let capsule_toks = count_tokens(&out)?;

                            // Count full-file tokens for the savings delta
                            let file_toks: usize = conn
                                .query_row(
                                    "SELECT root_path FROM repositories WHERE id = ?1",
                                    rusqlite::params![repo_id],
                                    |row| row.get::<_, String>(0),
                                )
                                .ok()
                                .and_then(|root| {
                                    let abs = std::path::PathBuf::from(&root).join(&file_path);
                                    std::fs::read_to_string(abs).ok()
                                })
                                .and_then(|content| count_tokens(&content).ok())
                                .unwrap_or(0);

                            // Persist to lifetime stats
                            let saved = file_toks.saturating_sub(capsule_toks) as i64;
                            let _ = db::increment_stat(&conn, "total_requests",     1);
                            let _ = db::increment_stat(&conn, "total_file_tokens",  file_toks as i64);
                            let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                            Ok::<_, anyhow::Error>((out, capsule_toks, file_toks, file_path))
                        })
                        .await
                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                        .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let tokens_saved = file_tokens.saturating_sub(capsule_tokens);

                    let event = DashboardEvent::CapsuleServed {
                        symbol:         sym_for_event,
                        repo:           repo_for_event,
                        file:           file_path,
                        capsule_tokens,
                        file_tokens,
                        tokens_saved,
                        origin:         client_name,
                        ts:             dashboard::now_ts(),
                    };
                    let http_client = self.http_client.clone();
                    tokio::spawn(async move {
                        let _ = http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await;
                    });

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── analyze_impact ────────────────────────────────────────────
                "analyze_impact" => {
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id     = Self::require_str(&args, "repo_id")?.to_string();

                    let sym_clone  = symbol_name.clone();
                    let repo_clone = repo_id.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
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
                        let _ = http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await;
                    });

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── ingest_repo ───────────────────────────────────────────────
                "ingest_repo" => {
                    let repo_id   = Self::require_str(&args, "repo_id")?.to_string();
                    let root_path: PathBuf =
                        Self::require_str(&args, "root_path")?.to_string().into();

                    let repo_id_for_event = repo_id.clone();

                    let (symbols, edges) = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let symbols = ingestion::ingest_repo(&conn, &repo_id, &root_path)?;
                        let edges = ingestion::resolve_cross_repo_edges(&conn)?;
                        Ok::<_, anyhow::Error>((symbols, edges))
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
                        let _ = http_client
                            .post(DASHBOARD_EMIT_URL)
                            .json(&event)
                            .send()
                            .await;
                    });

                    Ok(CallToolResult::success(vec![Content::text(format!(
                        "Ingested {symbols} symbols; resolved {edges} cross-repo edges."
                    ))]))
                }

                _ => Err(rmcp::ErrorData::method_not_found::<
                    rmcp::model::CallToolRequestMethod,
                >()),
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

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
                let tmp = rc_path.with_extension("json.tmp");
                fs::write(&tmp, serde_json::to_string_pretty(&cfg)?)?;
                fs::rename(&tmp, rc_path)?;
                println!("Auto-Open is now {}.", if !current { "ON" } else { "OFF" });
            }
            _ => break,
        }
    }

    Ok(())
}

/// `marrow init` — scaffold a `.marrow/` directory and `.marrowrc.json` config.
fn cmd_init() -> Result<()> {
    let marrow_dir = Path::new(".marrow");
    fs::create_dir_all(marrow_dir)?;

    let rc_path = Path::new(".marrowrc.json");
    if rc_path.exists() {
        println!(".marrowrc.json already exists — skipping.");
    } else {
        let default_config = serde_json::json!({
            "ignore": ["node_modules", "target", "dist", ".git"],
            "show_dashboard": true,
            "auto_open_ui": true
        });
        fs::write(rc_path, serde_json::to_string_pretty(&default_config)?)?;
        println!("Created .marrowrc.json with default ignore patterns.");
    }

    println!("Initialized .marrow/ workspace.");
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
    binary:  String,
    db_path: String,
    home:    String,
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

/// ~/Library/Application Support/claude-code/config.json
fn integrate_claude(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home)
        .join("Library/Application Support/claude-code/config.json");
    if !path.exists() {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    [],
        "env":     { "MARROW_DB_PATH": ctx.db_path }
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.gemini/antigravity/mcp_config.json
/// The `env` block is mandatory — it bypasses the macOS sandbox (os error 30).
fn integrate_antigravity(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home)
        .join(".gemini/antigravity/mcp_config.json");
    if !path.exists() {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    [],
        "env":     { "MARROW_DB_PATH": ctx.db_path }
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
///   ~/.mcp.json          (VS Code global MCP, uses "servers" key)
///   ~/.copilot/mcp-config.json  (Copilot CLI, uses "mcpServers" key)
fn integrate_copilot(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    // 1. ~/.mcp.json — VS Code / global MCP
    let vscode_path = PathBuf::from(&ctx.home).join(".mcp.json");
    let mut vscode_cfg = load_json_or_empty(&vscode_path)?;
    vscode_cfg["servers"]["marrow"] = serde_json::json!({
        "command": ctx.binary,
        "args":    ["mcp"]
    });
    save_json(&vscode_path, &vscode_cfg)?;

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
        "args":        [],
        "env":         { "MARROW_DB_PATH": ctx.db_path },
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
            "args": [],
            "env":  { "MARROW_DB_PATH": ctx.db_path }
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
    use dialoguer::{MultiSelect, theme::ColorfulTheme};

    // ── Banner ────────────────────────────────────────────────────────
    println!("{}", style(MARROW_BANNER).cyan().bold());
    println!(
        "  {}",
        style("AST Context Engine  ·  MCP Server Installer").dim()
    );
    println!();

    // ── Agent menu ────────────────────────────────────────────────────
    #[allow(clippy::type_complexity)]
    let agents: &[(&str, fn(&IntegrationCtx) -> Result<AgentOutcome>)] = &[
        ("Claude Code",          integrate_claude),
        ("Antigravity (Gemini)", integrate_antigravity),
        ("Cursor",               integrate_cursor),
        ("GitHub Copilot",       integrate_copilot),
        ("Cline",                integrate_cline),
        ("Zed",                  integrate_zed),
    ];

    let labels: Vec<&str> = agents.iter().map(|(name, _)| *name).collect();

    let selections = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select agents to configure  (space to toggle, enter to confirm)")
        .items(&labels)
        .interact()?;

    if selections.is_empty() {
        println!("\n{}", style("No agents selected — nothing to do.").dim());
        return Ok(());
    }

    // ── Resolve shared paths once ─────────────────────────────────────
    let binary = std::env::current_exe()
        .context("Could not resolve current executable path")?
        .to_string_lossy()
        .to_string();

    let db_path = std::env::current_dir()?
        .join(".marrow/graph.db")
        .to_string_lossy()
        .to_string();

    let home = std::env::var("HOME").context("$HOME is not set")?;

    let ctx = IntegrationCtx { binary, db_path, home };

    // ── Run each selected agent ───────────────────────────────────────
    println!();
    for idx in selections {
        let (name, integrate_fn) = agents[idx];
        match integrate_fn(&ctx) {
            Ok(AgentOutcome::Installed) => println!(
                "  {}  {}",
                style("✓").green().bold(),
                style(name).bold(),
            ),
            Ok(AgentOutcome::NotFound) => println!(
                "  {}  {}  {}",
                style("⚠").yellow().bold(),
                style(name).dim(),
                style("(not installed — skipped)").dim(),
            ),
            Err(e) => println!(
                "  {}  {}  {}",
                style("✗").red().bold(),
                style(name).bold(),
                style(format!("— {e}")).red(),
            ),
        }
    }

    println!();
    println!("  {}", style("Done.").bold());
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

    let supported_exts = ["cpp", "cc", "cxx", "h", "hpp", "py", "ts", "tsx"];

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

    println!("Repo:  {repo_id}");
    println!("Root:  {}", cwd.display());
    println!("Files: {}", files.len());

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
    fs::create_dir_all(".marrow")?;
    let conn = db::init_db(db_path)?;

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
    println!("\n── Index complete ──────────────────────────────────────────");
    println!("  Symbols: {}", fmt_num(symbol_count));
    println!("  Edges:   {}", fmt_num(edge_count));
    println!("  Time:    {:.2?}", elapsed);
    println!("  DB:      {db_path}");

    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // ── CLI subcommand dispatch ────────────────────────────────────────
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("ui")   => return cmd_ui(),
        Some("init") => return cmd_init(),
        Some("index") => return cmd_index(),
        Some("integrate") => return cmd_integrate(),
        Some("benchmark") => {
            let symbol = args.get(2).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} benchmark <symbol> <repo_id>", args[0])
            })?;
            let repo_id = args.get(3).ok_or_else(|| {
                anyhow::anyhow!("Usage: {} benchmark <symbol> <repo_id>", args[0])
            })?;

            let db_path = std::env::var("MARROW_DB_PATH")
                .unwrap_or_else(|_| ".marrow/graph.db".to_string());
            let db_parent = Path::new(&db_path)
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            fs::create_dir_all(db_parent)?;

            let conn = db::init_db(&db_path)?;
            run_benchmark(&conn, symbol, repo_id)?;
            return Ok(());
        }
        _ => {}
    }

    // ── Default: start MCP stdio server ──────────────────────────────
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".marrow/graph.db".to_string());

    let db_parent = Path::new(&db_path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(db_parent)?;

    // ── Read both config flags in one pass ────────────────────────────
    let (show_dashboard, auto_open_ui) = {
        let cfg: serde_json::Value = fs::read_to_string(".marrowrc.json")
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_else(|| serde_json::json!({}));
        let show = cfg.get("show_dashboard").and_then(|b| b.as_bool()).unwrap_or(true);
        let open = cfg.get("auto_open_ui").and_then(|b| b.as_bool()).unwrap_or(true);
        (show, open)
    };

    let client_name = Arc::new(Mutex::new("Unknown Client".to_string()));

    // ── Init DB ───────────────────────────────────────────────────────
    let conn   = db::init_db(&db_path)?;
    let db_arc = Arc::new(Mutex::new(conn));

    // ── Create the HTTP client once — shared by Hub startup and engine ─
    let http_client = reqwest::Client::new();

    // ── Dashboard Hub election ────────────────────────────────────────
    if show_dashboard {
        use tokio::sync::broadcast;
        // The initial receiver is intentionally dropped: all SSE consumers call
        // tx.subscribe() dynamically when a browser connects (see sse_handler).
        // The channel stays open because AppState holds the cloned sender.
        let (tx, _) = broadcast::channel::<DashboardEvent>(256);
        let session = Arc::new(Mutex::new(dashboard::SessionStats::default()));

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
                    let _ = client
                        .post(DASHBOARD_EMIT_URL)
                        .json(&DashboardEvent::ServerStarted { port: 8765, db_path })
                        .send()
                        .await;
                });
            }
            dashboard::HubRole::Spoke => {
                eprintln!("Marrow running as Spoke (Hub already active on :8765).");
            }
        }
    }

    // ── Build engine ──────────────────────────────────────────────────
    let engine = ContextEngine {
        db:          Arc::clone(&db_arc),
        client_name: Arc::clone(&client_name),
        http_client,
    };

    eprintln!("Marrow MCP server ready — listening on stdio.");
    let server = engine.serve(stdio()).await?;
    server.waiting().await?;

    Ok(())
}
