mod db;
mod ingestion;
mod retrieval;

use std::{
    fmt::Write as FmtWrite,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::Result;
use rmcp::{
    RoleServer, ServerHandler, ServiceExt,
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation,
        ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo, Tool,
        ToolsCapability,
    },
    service::RequestContext,
    transport::stdio,
};

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

// ── Server struct ─────────────────────────────────────────────────────────────

/// Wraps the SQLite connection behind Arc<Mutex<_>> so the handler can be
/// Clone + Send + Sync, as required by rmcp's ServerHandler bound.
#[derive(Clone)]
struct ContextEngine {
    db: Arc<Mutex<rusqlite::Connection>>,
}

impl ContextEngine {
    fn new(db_path: &str) -> Result<Self> {
        let conn = db::init_db(db_path)?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
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
                     via tree-sitter into an AST dependency graph and serves skeletonized \
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
                "Fetch the full source of a pivot symbol plus skeletonized signatures of its \
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
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();

                    let capsule = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        retrieval::get_context_capsule(&conn, &symbol_name, &repo_id)
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let out = format_capsule_string(&capsule);
                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── analyze_impact ────────────────────────────────────────────
                "analyze_impact" => {
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();

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

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── ingest_repo ───────────────────────────────────────────────
                "ingest_repo" => {
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();
                    let root_path: PathBuf =
                        Self::require_str(&args, "root_path")?.to_string().into();

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
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".context_engine/graph.db".to_string());

    let db_parent = std::path::Path::new(&db_path)
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    fs::create_dir_all(db_parent)?;

    let engine = ContextEngine::new(&db_path)?;

    eprintln!("Marrow MCP server ready — listening on stdio.");
    let server = engine.serve(stdio()).await?;
    server.waiting().await?;

    Ok(())
}
