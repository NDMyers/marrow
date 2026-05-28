mod activity;
mod context;
mod daemon;
mod dashboard;
mod db;
mod ingestion;
mod ipc;
mod packaging;
mod registry;
mod retrieval;
mod service;
mod skills;
mod state;
mod ui_app;
mod watcher;

use std::{
    fmt::Write as FmtWrite,
    fs,
    io::IsTerminal,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

use anyhow::{Context as _, Result};
use dashboard::DashboardEvent;
use rmcp::{
    model::{
        CallToolRequestParams, CallToolResult, Content, Implementation, InitializeRequestParams,
        InitializeResult, ListToolsResult, PaginatedRequestParams, ServerCapabilities, ServerInfo,
        Tool, ToolsCapability,
    },
    service::RequestContext,
    transport::stdio,
    RoleServer, ServerHandler, ServiceExt,
};
use state::{set_index_state, update_index_progress, IndexState};

const DASHBOARD_EMIT_URL: &str = "http://127.0.0.1:8765/api/emit";

/// Stores the MCP client's name captured during the `initialize` handshake.
/// Safe to use as a singleton because stdio spawns one process per session.
static CLIENT_NAME: OnceLock<String> = OnceLock::new();

/// Returns true when `MARROW_TRACE=1` (or any non-empty value) is set.
/// Used to gate developer-only timing output so production stderr stays clean.
#[inline(always)]
fn trace_enabled() -> bool {
    std::env::var_os("MARROW_TRACE").is_some_and(|v| !v.is_empty())
}

/// Emit a `[MARROW TRACE]` line to stderr **iff** `MARROW_TRACE` is set.
/// Uses macro syntax so the format string is zero-cost when tracing is off.
/// Best-effort high-water RSS from `getrusage` (Darwin: bytes; Linux: KiB → bytes).
#[cfg(any(target_os = "macos", target_os = "linux"))]
fn rusage_max_rss_bytes() -> Option<u64> {
    use std::mem;
    let mut usage: libc::rusage = unsafe { mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return None;
    }
    let v = usage.ru_maxrss;
    if v == 0 {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        Some(v as u64 * 1024)
    }
    #[cfg(target_os = "macos")]
    {
        Some(v as u64)
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn rusage_max_rss_bytes() -> Option<u64> {
    None
}

macro_rules! trace {
    ($($arg:tt)*) => {
        if crate::trace_enabled() {
            eprintln!("[MARROW TRACE] {}", format!($($arg)*));
        }
    };
}

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

fn emit_dashboard_event(http_client: reqwest::Client, event: DashboardEvent) {
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
}

fn dashboard_events_from_batch_telemetry(
    telemetry: Vec<retrieval::BatchTelemetry>,
    client_name: &str,
) -> Vec<DashboardEvent> {
    telemetry
        .into_iter()
        .map(|event| match event {
            retrieval::BatchTelemetry::Capsule {
                symbol,
                repo,
                file,
                capsule_tokens,
                file_tokens,
                original_text,
                optimized_text,
                proof_snapshot,
                provenance,
            } => DashboardEvent::CapsuleServed {
                symbol,
                repo,
                file,
                capsule_tokens,
                file_tokens,
                tokens_saved: file_tokens.saturating_sub(capsule_tokens),
                origin: client_name.to_string(),
                ts: dashboard::now_ts(),
                original_text,
                optimized_text: Some(optimized_text),
                proof_snapshot,
                provenance,
                has_cached_delta: false,
            },
            retrieval::BatchTelemetry::Impact {
                symbol,
                repo,
                affected_count,
            } => DashboardEvent::ImpactAnalyzed {
                symbol,
                repo,
                affected_count,
                ts: dashboard::now_ts(),
            },
            retrieval::BatchTelemetry::Skeleton {
                target_dir,
                node_count,
            } => DashboardEvent::SkeletonGenerated {
                target_dir,
                node_count,
                ts: dashboard::now_ts(),
            },
        })
        .collect()
}

/// Milliseconds to wait after the Axum server spawns before sending the
/// first ServerStarted event. The listener is ready almost instantly but
/// there is a brief window between `tokio::spawn` returning and the first
/// `accept()` completing. A missed ServerStarted is cosmetic (dashboard UI
/// only), so this is best-effort rather than a hard synchronisation point.
#[allow(dead_code)]
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
    )
    .ok();
    writeln!(out, "File : {}", capsule.pivot.file_path).ok();
    writeln!(out, "Type : {}", capsule.pivot.symbol_type).ok();
    writeln!(
        out,
        "\n── FULL SOURCE ──────────────────────────────────────────────"
    )
    .ok();
    writeln!(out, "{}", capsule.pivot.text).ok();

    if capsule.neighbors.is_empty() {
        writeln!(
            out,
            "── NEIGHBORS ────────────────────────────────────────────────"
        )
        .ok();
        writeln!(out, "  (none — isolated symbol)").ok();
    } else {
        for n in &capsule.neighbors {
            writeln!(
                out,
                "\n── NEIGHBOR  [{rel}]  {name}  ({lang})  {path}",
                rel = n.relationship,
                name = n.node.symbol_name,
                lang = n.node.language,
                path = n.node.file_path,
            )
            .ok();
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

const BENCHMARK_REPOSITORY_LIMIT: usize = 50;
const BENCHMARK_SYMBOL_LIMIT: usize = 50;
const BENCHMARK_FILTER_LIMIT: usize = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkRepositoryChoice {
    repo_id: String,
    root_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchmarkSymbolChoice {
    repo_id: String,
    symbol_name: String,
    file_path: String,
    language: String,
    symbol_type: String,
}

struct BenchmarkMeasurement {
    file_path: String,
    file_tokens: usize,
    capsule_tokens: usize,
    provenance: retrieval::CapsuleProvenance,
    #[cfg(test)]
    optimized_text: String,
}

fn benchmark_usage(program: &str) -> String {
    format!("Usage: {program} benchmark [--precise-file-tokens] <symbol> <repo_id>")
}

fn benchmark_prompts_available() -> bool {
    std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

fn benchmark_repository_choices(
    conn: &rusqlite::Connection,
    limit: usize,
) -> anyhow::Result<Vec<BenchmarkRepositoryChoice>> {
    let mut stmt = conn.prepare(
        "SELECT id, root_path
         FROM repositories
         ORDER BY id ASC
         LIMIT ?1",
    )?;
    let rows = stmt.query_map([limit as i64], |row| {
        Ok(BenchmarkRepositoryChoice {
            repo_id: row.get(0)?,
            root_path: row.get(1)?,
        })
    })?;

    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn escape_like_pattern(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '\\' | '%' | '_' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn run_pipeline_invalid_intent_message() -> &'static str {
    "Invalid intent. Must be 'analyze_repo', 'find_symbol', 'explore_symbol', 'trace_flow', 'refactor_symbol', 'read_node', 'explore_batch', 'dependency_graph', or 'map_class'."
}

fn parse_positive_usize_value(
    value: &serde_json::Value,
    label: &str,
) -> std::result::Result<usize, rmcp::ErrorData> {
    value
        .as_u64()
        .and_then(|limit| usize::try_from(limit).ok())
        .filter(|&limit| limit > 0)
        .ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!("run_pipeline `{label}` must be a positive integer"),
                None,
            )
        })
}

fn parse_optional_positive_usize(
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> std::result::Result<Option<usize>, rmcp::ErrorData> {
    args.get(key)
        .map(|value| parse_positive_usize_value(value, key))
        .transpose()
}

fn parse_optional_bool(
    args: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> std::result::Result<bool, rmcp::ErrorData> {
    args.get(key)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                rmcp::ErrorData::invalid_params(
                    format!("run_pipeline `{key}` must be a boolean"),
                    None,
                )
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(false))
}

fn parse_dependency_direction(
    value: Option<&serde_json::Value>,
) -> std::result::Result<retrieval::DependencyDirection, rmcp::ErrorData> {
    let Some(value) = value else {
        return Ok(retrieval::DependencyDirection::Both);
    };
    let direction = value.as_str().ok_or_else(|| {
        rmcp::ErrorData::invalid_params(
            "run_pipeline `direction` must be a string".to_string(),
            None,
        )
    })?;
    retrieval::DependencyDirection::parse(direction)
        .map_err(|e| rmcp::ErrorData::invalid_params(e.to_string(), None))
}

fn parse_dependency_graph_options(
    args: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<retrieval::DependencyGraphOptions, rmcp::ErrorData> {
    let depth = parse_optional_positive_usize(args, "depth")?.unwrap_or(2);
    if depth > 5 {
        return Err(rmcp::ErrorData::invalid_params(
            "run_pipeline `depth` must be between 1 and 5".to_string(),
            None,
        ));
    }
    Ok(retrieval::DependencyGraphOptions {
        depth,
        direction: parse_dependency_direction(args.get("direction"))?,
        include_source: parse_optional_bool(args, "include_source")?,
        max_nodes: parse_optional_positive_usize(args, "max_nodes")?
            .unwrap_or_else(retrieval::dependency_graph_max_nodes),
        max_bytes: retrieval::dependency_graph_max_bytes(),
    })
}

fn parse_batch_queries(
    args: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<Vec<retrieval::BatchQuery>, rmcp::ErrorData> {
    let queries = args
        .get("queries")
        .and_then(|value| value.as_array())
        .ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                "run_pipeline explore_batch requires a `queries` array".to_string(),
                None,
            )
        })?;
    if queries.is_empty() || queries.len() > 20 {
        return Err(rmcp::ErrorData::invalid_params(
            "run_pipeline explore_batch requires 1 to 20 queries".to_string(),
            None,
        ));
    }

    let mut parsed = Vec::with_capacity(queries.len());
    for (idx, value) in queries.iter().enumerate() {
        let object = value.as_object().ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!(
                    "run_pipeline explore_batch query {} must be an object",
                    idx + 1
                ),
                None,
            )
        })?;
        let intent_name = object
            .get("intent")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                rmcp::ErrorData::invalid_params(
                    format!(
                        "run_pipeline explore_batch query {} requires `intent`",
                        idx + 1
                    ),
                    None,
                )
            })?;
        let intent = retrieval::BatchIntent::parse(intent_name).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                format!(
                    "run_pipeline explore_batch query {} has invalid intent `{intent_name}`",
                    idx + 1
                ),
                None,
            )
        })?;
        let target = object
            .get("target")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                rmcp::ErrorData::invalid_params(
                    format!(
                        "run_pipeline explore_batch query {} requires `target`",
                        idx + 1
                    ),
                    None,
                )
            })?
            .to_string();
        let depth = object
            .get("depth")
            .map(|value| parse_positive_usize_value(value, "depth"))
            .transpose()?;
        if depth.is_some_and(|value| value > 5) {
            return Err(rmcp::ErrorData::invalid_params(
                format!(
                    "run_pipeline explore_batch query {} `depth` must be between 1 and 5",
                    idx + 1
                ),
                None,
            ));
        }
        let direction = parse_dependency_direction(object.get("direction"))?;
        let include_source = parse_optional_bool(object, "include_source")?;
        parsed.push(retrieval::BatchQuery {
            intent,
            target,
            filepath: object
                .get("filepath")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            kind: object
                .get("kind")
                .and_then(|value| value.as_str())
                .map(str::to_string),
            limit: object
                .get("limit")
                .map(|value| parse_positive_usize_value(value, "limit"))
                .transpose()?,
            depth,
            direction: object.get("direction").map(|_| direction),
            include_source,
            max_nodes: object
                .get("max_nodes")
                .map(|value| parse_positive_usize_value(value, "max_nodes"))
                .transpose()?,
        });
    }
    Ok(parsed)
}

fn parse_find_symbol_limit(
    args: &serde_json::Map<String, serde_json::Value>,
) -> std::result::Result<usize, rmcp::ErrorData> {
    let Some(value) = args.get("limit") else {
        return Ok(retrieval::FIND_SYMBOL_DEFAULT_LIMIT);
    };
    value
        .as_u64()
        .and_then(|limit| usize::try_from(limit).ok())
        .filter(|&limit| limit > 0)
        .ok_or_else(|| {
            rmcp::ErrorData::invalid_params(
                "run_pipeline find_symbol `limit` must be a positive integer".to_string(),
                None,
            )
        })
}

fn dispatch_run_pipeline_find_symbol(
    conn: &rusqlite::Connection,
    repo_id: &str,
    query: &str,
    kind: Option<&str>,
    limit: usize,
) -> anyhow::Result<String> {
    retrieval::find_symbols(conn, repo_id, query, kind, limit)
}

fn benchmark_symbol_choices(
    conn: &rusqlite::Connection,
    repo_id: &str,
    search: &str,
    language: Option<&str>,
    symbol_type: Option<&str>,
    limit: usize,
) -> anyhow::Result<(Vec<BenchmarkSymbolChoice>, bool)> {
    let search = search.trim();
    let pattern = format!("%{}%", escape_like_pattern(search));
    let language = language.filter(|value| !value.is_empty());
    let symbol_type = symbol_type.filter(|value| !value.is_empty());
    let query_limit = limit.saturating_add(1) as i64;

    let mut stmt = conn.prepare(
        "SELECT repo_id, symbol_name, file_path, language, symbol_type
         FROM nodes
         WHERE repo_id = ?1
           AND (?2 = '' OR symbol_name LIKE ?3 ESCAPE '\\' OR file_path LIKE ?3 ESCAPE '\\')
           AND (?4 IS NULL OR language = ?4)
           AND (?5 IS NULL OR symbol_type = ?5)
         ORDER BY symbol_name COLLATE NOCASE ASC, file_path ASC, id ASC
         LIMIT ?6",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![repo_id, search, pattern, language, symbol_type, query_limit],
        |row| {
            Ok(BenchmarkSymbolChoice {
                repo_id: row.get(0)?,
                symbol_name: row.get(1)?,
                file_path: row.get(2)?,
                language: row.get(3)?,
                symbol_type: row.get(4)?,
            })
        },
    )?;
    let mut choices = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let limited = choices.len() > limit;
    choices.truncate(limit);
    Ok((choices, limited))
}

fn benchmark_distinct_node_values(
    conn: &rusqlite::Connection,
    repo_id: &str,
    column: &str,
    limit: usize,
) -> anyhow::Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT {column}
         FROM nodes
         WHERE repo_id = ?1
         ORDER BY {column} COLLATE NOCASE ASC
         LIMIT ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(rusqlite::params![repo_id, limit as i64], |row| row.get(0))?;
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn benchmark_repo_symbol_count(conn: &rusqlite::Connection, repo_id: &str) -> anyhow::Result<i64> {
    conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

fn benchmark_prompt_select(
    prompt: &str,
    items: &[String],
    default: usize,
) -> anyhow::Result<Option<usize>> {
    use dialoguer::{theme::ColorfulTheme, Select};

    match Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(items)
        .default(default.min(items.len().saturating_sub(1)))
        .interact_opt()
    {
        Ok(choice) => Ok(choice),
        Err(dialoguer::Error::IO(err)) if err.kind() == std::io::ErrorKind::Interrupted => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn benchmark_prompt_input(prompt: &str) -> anyhow::Result<Option<String>> {
    use dialoguer::{theme::ColorfulTheme, Input};

    match Input::<String>::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .allow_empty(true)
        .interact_text()
    {
        Ok(value) => Ok(Some(value)),
        Err(dialoguer::Error::IO(err)) if err.kind() == std::io::ErrorKind::Interrupted => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn benchmark_optional_filter(
    prompt: &str,
    values: &[String],
) -> anyhow::Result<Option<Option<String>>> {
    if values.is_empty() {
        return Ok(Some(None));
    }

    let mut items = Vec::with_capacity(values.len() + 1);
    items.push("Any".to_string());
    items.extend(values.iter().cloned());

    let Some(selection) = benchmark_prompt_select(prompt, &items, 0)? else {
        return Ok(None);
    };
    if selection == 0 {
        Ok(Some(None))
    } else {
        Ok(Some(Some(items[selection].clone())))
    }
}

fn resolve_benchmark_file_path(
    conn: &rusqlite::Connection,
    symbol: &str,
    repo_id: &str,
    filepath: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(filepath) = filepath {
        return conn
            .query_row(
                "SELECT file_path FROM nodes
                 WHERE symbol_name = ?1 AND repo_id = ?2 AND file_path = ?3
                 ORDER BY file_path ASC, id ASC
                 LIMIT 1",
                rusqlite::params![symbol, repo_id, filepath],
                |row| row.get(0),
            )
            .map_err(|_| {
                anyhow::anyhow!(
                    "Selected symbol '{}' in repo '{}' at '{}' is no longer available.",
                    symbol,
                    repo_id,
                    filepath
                )
            });
    }

    conn.query_row(
        "SELECT file_path FROM nodes
         WHERE symbol_name = ?1 AND repo_id = ?2
         ORDER BY file_path ASC, id ASC
         LIMIT 1",
        rusqlite::params![symbol, repo_id],
        |row| row.get(0),
    )
    .map_err(|_| anyhow::anyhow!("Symbol '{}' not found in repo '{}'.", symbol, repo_id))
}

/// Build the terminal benchmark table.
///
/// Layout (67-char inner width, 69-char total with border chars):
///   header rows span full 67 chars (W = L + 1 + R = 27 + 1 + 39)
///   metric rows: 27-char left col │ 39-char right col
fn format_benchmark_table(
    symbol: &str,
    repo_id: &str,
    file_path: &str,
    file_tokens: usize,
    capsule_tokens: usize,
    provenance: &retrieval::CapsuleProvenance,
) -> String {
    let saved = file_tokens.saturating_sub(capsule_tokens);
    let reduction = if file_tokens == 0 {
        0.0_f64
    } else {
        (saved as f64 / file_tokens as f64) * 100.0
    };

    // Column inner widths (excluding the │ separator).
    const L: usize = 27; // left metric label column
    const R: usize = 39; // right value column
    const W: usize = L + 1 + R; // total inner width = 67

    let h_full = "─".repeat(W);
    let h_left = "─".repeat(L);
    let h_right = "─".repeat(R);

    let hdr_title = "  Marrow Token Benchmark".to_string();
    let hdr_sym = format!("  Symbol: {symbol}  ·  Repo: {repo_id}");
    let hdr_file = format!("  File:   {file_path}");

    let row = |label: &str, value: &str| -> String {
        format!(
            "│  {label:<25}│  {value:<37}│\n",
            label = label,
            value = value
        )
    };

    let mut t = String::new();
    // Top border + header
    writeln!(t, "┌{h_full}┐").ok();
    writeln!(t, "│{hdr_title:<W$}│", W = W).ok();
    writeln!(t, "│{hdr_sym:<W$}│", W = W).ok();
    writeln!(t, "│{hdr_file:<W$}│", W = W).ok();
    // Column divider
    writeln!(t, "├{h_left}┬{h_right}┤").ok();
    // Column headers
    t.push_str(&row("Metric", "Value"));
    // Body divider
    writeln!(t, "├{h_left}┼{h_right}┤").ok();
    // Metric rows
    t.push_str(&row("Baseline Tokens", &fmt_num(file_tokens)));
    t.push_str(&row("Baseline Source", &provenance.baseline_token_source));
    t.push_str(&row("Tokenizer", &provenance.tokenizer_mode));
    t.push_str(&row("Original Mode", &provenance.original_mode));
    t.push_str(&row("Proof Mode", &provenance.proof_label));
    t.push_str(&row(
        "Precise File Tokens",
        if provenance.precise_file_tokens {
            "true"
        } else {
            "false"
        },
    ));
    t.push_str(&row(
        "Original Max Bytes",
        &provenance
            .original_max_bytes
            .map(|n| n.to_string())
            .unwrap_or_else(|| "none".to_string()),
    ));
    t.push_str(&row(
        "Proof Caps",
        &format!(
            "{} bytes / {} files",
            provenance.proof_max_bytes, provenance.proof_max_files
        ),
    ));
    t.push_str(&row("Capsule Tokens", &fmt_num(capsule_tokens)));
    t.push_str(&row("Tokens Saved", &fmt_num(saved)));
    t.push_str(&row("Reduction", &format!("{:.1}%", reduction)));
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
fn benchmark_measurement(
    conn: &rusqlite::Connection,
    symbol: &str,
    repo_id: &str,
    filepath: Option<&str>,
    precise_file_tokens: bool,
) -> anyhow::Result<BenchmarkMeasurement> {
    let file_path = resolve_benchmark_file_path(conn, symbol, repo_id, filepath)?;
    let result = retrieval::get_context_capsule(conn, symbol, repo_id, filepath)?;

    let mut provenance = result.provenance.clone();
    let file_tokens = if precise_file_tokens {
        let measured =
            retrieval::measure_precise_tokens_touched_by_capsule(conn, symbol, repo_id, filepath)?;
        if !measured.failed_paths.is_empty() {
            anyhow::bail!(
                "exact baseline unavailable: failed to tokenize touched file(s): {}",
                measured.failed_paths.join(", ")
            );
        }
        provenance.baseline_token_source = "exact".to_string();
        provenance.tokenizer_mode = measured.tokenizer_mode;
        provenance.precise_file_tokens = true;
        provenance.touched_file_count = measured.touched_file_count;
        measured.tokens
    } else {
        result.file_tokens
    };
    let capsule_tokens = count_tokens(&result.optimized_text)?;

    Ok(BenchmarkMeasurement {
        file_path,
        file_tokens,
        capsule_tokens,
        provenance,
        #[cfg(test)]
        optimized_text: result.optimized_text,
    })
}

fn run_benchmark(
    conn: &rusqlite::Connection,
    symbol: &str,
    repo_id: &str,
    filepath: Option<&str>,
    precise_file_tokens: bool,
) -> anyhow::Result<()> {
    let measurement = benchmark_measurement(conn, symbol, repo_id, filepath, precise_file_tokens)?;

    eprintln!(
        "{}",
        format_benchmark_table(
            symbol,
            repo_id,
            &measurement.file_path,
            measurement.file_tokens,
            measurement.capsule_tokens,
            &measurement.provenance
        )
    );

    Ok(())
}

fn cmd_benchmark_wizard(conn: &rusqlite::Connection) -> anyhow::Result<()> {
    let repos = benchmark_repository_choices(conn, BENCHMARK_REPOSITORY_LIMIT)?;
    if repos.is_empty() {
        eprintln!("No repositories found in the graph database. Run `marrow index` first.");
        return Ok(());
    }

    let repo_labels: Vec<String> = repos
        .iter()
        .map(|repo| format!("{}  ({})", repo.repo_id, repo.root_path))
        .collect();
    let Some(repo_index) = benchmark_prompt_select("Select repository", &repo_labels, 0)? else {
        eprintln!("Benchmark cancelled.");
        return Ok(());
    };
    let repo = &repos[repo_index];

    if benchmark_repo_symbol_count(conn, &repo.repo_id)? == 0 {
        eprintln!(
            "Repository '{}' has no symbols in the graph. Run `marrow index` for that workspace.",
            repo.repo_id
        );
        return Ok(());
    }

    let languages =
        benchmark_distinct_node_values(conn, &repo.repo_id, "language", BENCHMARK_FILTER_LIMIT)?;
    let symbol_types =
        benchmark_distinct_node_values(conn, &repo.repo_id, "symbol_type", BENCHMARK_FILTER_LIMIT)?;

    loop {
        let Some(search) = benchmark_prompt_input("Search symbol name or file path")? else {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        };
        let Some(language) = benchmark_optional_filter("Filter by language", &languages)? else {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        };
        let Some(symbol_type) = benchmark_optional_filter("Filter by symbol type", &symbol_types)?
        else {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        };

        let (symbols, limited) = benchmark_symbol_choices(
            conn,
            &repo.repo_id,
            &search,
            language.as_deref(),
            symbol_type.as_deref(),
            BENCHMARK_SYMBOL_LIMIT,
        )?;

        if symbols.is_empty() {
            eprintln!("No symbols matched. Revise the search text or filters.");
            let retry_items = vec!["Revise search/filter".to_string(), "Cancel".to_string()];
            match benchmark_prompt_select("No results", &retry_items, 0)? {
                Some(0) => continue,
                _ => {
                    eprintln!("Benchmark cancelled.");
                    return Ok(());
                }
            }
        }

        if limited {
            eprintln!(
                "Showing the first {BENCHMARK_SYMBOL_LIMIT} matching symbols. Narrow the search/filter to reach omitted results."
            );
        }

        let mut symbol_labels: Vec<String> = symbols
            .iter()
            .map(|symbol| {
                format!(
                    "{}  [{} {}]  {}",
                    symbol.symbol_name, symbol.language, symbol.symbol_type, symbol.file_path
                )
            })
            .collect();
        symbol_labels.push("Revise search/filter".to_string());
        symbol_labels.push("Cancel".to_string());

        let Some(symbol_index) = benchmark_prompt_select("Select symbol", &symbol_labels, 0)?
        else {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        };
        if symbol_index == symbols.len() {
            continue;
        }
        if symbol_index > symbols.len() {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        }

        let selected = &symbols[symbol_index];
        let modes = vec![
            "Estimated baseline (default)".to_string(),
            "Exact proof mode (--precise-file-tokens)".to_string(),
        ];
        let Some(mode_index) = benchmark_prompt_select("Select benchmark mode", &modes, 0)? else {
            eprintln!("Benchmark cancelled.");
            return Ok(());
        };

        return run_benchmark(
            conn,
            &selected.symbol_name,
            &selected.repo_id,
            Some(&selected.file_path),
            mode_index == 1,
        );
    }
}

// ── Server struct ─────────────────────────────────────────────────────────────

/// Wraps the SQLite connection behind Arc<Mutex<_>> so the handler can be
/// Clone + Send + Sync, as required by rmcp's ServerHandler bound.
#[derive(Clone)]
struct ContextEngine {
    db: Arc<Mutex<rusqlite::Connection>>,
    http_client: reqwest::Client,
}

impl ContextEngine {
    #[allow(dead_code)]
    fn new(db_path: &str, http_client: reqwest::Client) -> Result<Self> {
        let conn = db::init_db(db_path)?;
        Ok(Self {
            db: Arc::new(Mutex::new(conn)),
            http_client,
        })
    }

    /// Convert a `serde_json::Value` (must be an Object) into the
    /// `Arc<serde_json::Map<String, Value>>` that `Tool::new` expects.
    fn schema(v: serde_json::Value) -> Arc<serde_json::Map<String, serde_json::Value>> {
        Arc::new(v.as_object().expect("schema must be a JSON object").clone())
    }

    fn run_pipeline_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
        Self::schema(serde_json::json!({
            "type": "object",
            "properties": {
                "intent": {
                    "type": "string",
                    "enum": ["analyze_repo", "find_symbol", "explore_symbol", "trace_flow", "refactor_symbol", "read_node", "explore_batch", "dependency_graph", "map_class"],
                    "description": "Must be exactly 'analyze_repo', 'find_symbol', 'explore_symbol', 'trace_flow', 'refactor_symbol', 'read_node', 'explore_batch', 'dependency_graph', or 'map_class'."
                },
                "target": {
                    "type": "string",
                    "description": "The symbol name or directory path relevant to the intent. \
                                    Required for find_symbol, explore_symbol, trace_flow, refactor_symbol, dependency_graph, and map_class."
                },
                "queries": {
                    "type": "array",
                    "minItems": 1,
                    "maxItems": 20,
                    "description": "Array of exploration queries for explore_batch. Each query requires intent and target.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "intent": {
                                "type": "string",
                                "enum": ["find_symbol", "explore_symbol", "trace_flow", "refactor_symbol", "read_node", "dependency_graph", "capsule", "analyze_impact"]
                            },
                            "target": { "type": "string" },
                            "kind": { "type": "string" },
                            "limit": { "type": "integer", "minimum": 1 },
                            "filepath": { "type": "string" },
                            "depth": { "type": "integer", "minimum": 1, "maximum": 5 },
                            "direction": { "type": "string", "enum": ["callers", "callees", "both"] },
                            "include_source": { "type": "boolean" },
                            "max_nodes": { "type": "integer", "minimum": 1 }
                        },
                        "required": ["intent", "target"]
                    }
                },
                "kind": {
                    "type": "string",
                    "description": "Optional symbol_type filter for find_symbol (e.g. 'function', 'class', 'struct', 'method')."
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "default": retrieval::FIND_SYMBOL_DEFAULT_LIMIT,
                    "description": "Optional maximum number of find_symbol matches to return. Must be a positive integer."
                },
                "repo_id": {
                    "type": "string",
                    "description": "The repository identifier. Auto-detected if omitted."
                },
                "filepath": {
                    "type": "string",
                    "description": "Relative file path to disambiguate symbols with identical names across files. \
                                    ALWAYS provide this for explore_symbol, trace_flow, refactor_symbol, dependency_graph, and map_class when \
                                    you know which file the symbol is in (e.g. from the user's open editor tab, \
                                    cursor location, or a previous skeleton/search result). Required to resolve \
                                    a Disambiguation Payload — re-call run_pipeline with the same intent/target \
                                    plus the filepath from the payload instead of falling back to grep/read_file."
                },
                "depth": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5,
                    "default": 2,
                    "description": "Dependency graph traversal depth for dependency_graph or explore_batch graph queries."
                },
                "direction": {
                    "type": "string",
                    "enum": ["callers", "callees", "both"],
                    "default": "both",
                    "description": "Dependency graph traversal direction."
                },
                "include_source": {
                    "type": "boolean",
                    "default": false,
                    "description": "Include condensed source for dependency graph nodes."
                },
                "max_nodes": {
                    "type": "integer",
                    "minimum": 1,
                    "default": retrieval::dependency_graph_max_nodes(),
                    "description": "Maximum dependency graph nodes to traverse and render."
                }
            },
            "required": ["intent"]
        }))
    }

    /// Non-blocking guard for tool calls while the boot-time indexer is still
    /// building the AST graph in the background.
    fn maybe_jit_index(&self, _repo_id: &str, _fallback_root: &std::path::Path) -> Option<String> {
        state::run_pipeline_guard_message()
    }

    #[allow(dead_code)]
    fn spawn_boot_time_indexer(&self) {
        let db = Arc::clone(&self.db);
        let http_client = self.http_client.clone();
        let workspace_root = current_workspace_root();
        let repo_id = match db.lock() {
            Ok(conn) => resolve_request_repo_id(&conn, None, &workspace_root)
                .unwrap_or_else(|_| fallback_repo_id_for_path(&workspace_root)),
            Err(_) => fallback_repo_id_for_path(&workspace_root),
        };

        tokio::spawn(async move {
            let ingest_t = Instant::now();

            // Retry up to 3 times on lock contention (SQLITE_BUSY).
            // This handles the rapid-restart race where the previous server
            // process is still holding the DB write lock when we start.
            const MAX_ATTEMPTS: u32 = 3;
            const RETRY_DELAY_MS: u64 = 3_000;

            let mut final_result = None;
            for attempt in 1..=MAX_ATTEMPTS {
                let db_clone = Arc::clone(&db);
                let repo_id_clone = repo_id.clone();
                let root_clone = workspace_root.clone();

                // Use run_ingestion_with_arc so the DB mutex is released during the
                // CPU-intensive parallel parse phase, allowing concurrent tool calls
                // to proceed without being blocked for the entire indexing duration.
                let result = tokio::task::spawn_blocking(move || {
                    ingestion::run_ingestion_with_arc(
                        &db_clone,
                        &repo_id_clone,
                        &root_clone,
                        update_index_progress,
                    )
                })
                .await;

                let is_lock_error = match &result {
                    Ok(Err(e)) => {
                        let msg = e.to_string().to_lowercase();
                        msg.contains("database is locked") || msg.contains("sqlite_busy")
                    }
                    _ => false,
                };

                if is_lock_error && attempt < MAX_ATTEMPTS {
                    eprintln!(
                        "[MARROW] Boot-time indexing blocked by DB lock (attempt {attempt}/{MAX_ATTEMPTS}), \
                         retrying in {RETRY_DELAY_MS}ms…"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                    continue;
                }

                final_result = Some(result);
                break;
            }

            match final_result.unwrap() {
                Ok(Ok((symbols, edges))) => {
                    set_index_state(IndexState::Ready);
                    eprintln!(
                        "[MARROW] Boot-time indexing complete: {symbols} symbols, {edges} edges in {}ms.",
                        ingest_t.elapsed().as_millis()
                    );

                    let event = DashboardEvent::RepoIndexed {
                        repo_id,
                        symbols,
                        edges,
                        ts: dashboard::now_ts(),
                    };
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
                }
                Ok(Err(e)) => {
                    set_index_state(IndexState::Uninitialized);
                    eprintln!("[MARROW] Boot-time indexing failed: {e}");
                }
                Err(e) => {
                    set_index_state(IndexState::Uninitialized);
                    eprintln!("[MARROW] Boot-time indexing task panicked: {e}");
                }
            }
        });
    }

    /// Pull a required string argument out of the tool arguments map, returning
    /// a well-formed MCP error if absent.
    fn require_str<'a>(
        args: &'a serde_json::Map<String, serde_json::Value>,
        key: &str,
    ) -> Result<&'a str, rmcp::ErrorData> {
        args.get(key).and_then(|v| v.as_str()).ok_or_else(|| {
            rmcp::ErrorData::invalid_params(format!("missing required argument: '{key}'"), None)
        })
    }
}

/// Resolve the workspace root to the actual project directory.
///
/// Priority order:
///   1. `MARROW_WORKSPACE` env var — explicit override for edge cases (e.g. CI, custom launchers)
///   2. Walk up from `current_dir()` looking for `.marrowrc.json` (authoritative Marrow marker)
///   3. Walk up from `current_dir()` looking for `.git` (VCS root)
///   4. Fall back to `current_dir()` as-is
///
/// This fixes the VS Code MCP spawn bug: VS Code launches the server from `~` rather than the
/// open project directory, so a naive `current_dir()` resolves to the home folder and JIT
/// indexing would attempt to walk the entire filesystem.
fn current_workspace_root() -> PathBuf {
    // Tier 1: explicit env override
    if let Ok(override_path) = std::env::var("MARROW_WORKSPACE") {
        let p = PathBuf::from(&override_path);
        if let Ok(canonical) = p.canonicalize() {
            trace!(
                "current_workspace_root: MARROW_WORKSPACE override → {}",
                canonical.display()
            );
            return canonical;
        }
    }

    let cwd = std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from("."));

    // Tier 2: walk up for .marrowrc.json
    {
        let mut probe = cwd.clone();
        loop {
            if probe.join(".marrowrc.json").exists() {
                trace!(
                    "current_workspace_root: found .marrowrc.json → {}",
                    probe.display()
                );
                return probe;
            }
            if !probe.pop() {
                break;
            }
        }
    }

    // Tier 3: check for .git only at cwd — intentionally NOT walking up.
    // Walking up can hit a rogue ~/.git and cause the entire home directory
    // to be indexed (64 k+ files). The CLI is invoked from the project root,
    // so checking only the current directory is sufficient.
    if cwd.join(".git").exists() {
        trace!(
            "current_workspace_root: found .git at cwd → {}",
            cwd.display()
        );
        return cwd;
    }

    // Tier 4: fall back to cwd
    trace!(
        "current_workspace_root: no marker found, falling back to cwd → {}",
        cwd.display()
    );
    cwd
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

    // If the caller explicitly passed a repo_id, verify it exists in the DB.
    // Don't require its root_path to match the current workspace — the repo
    // may have been ingested at a child directory or from a different CWD.
    if explicit_repo_id.is_some() {
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) FROM repositories WHERE id = ?1",
                rusqlite::params![repo_id],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;
        if !exists {
            return Err(anyhow::anyhow!(
                "Repo '{}' not found in the Marrow database. Run ingest_repo first.",
                repo_id
            ));
        }
        return Ok(repo_id);
    }

    // M-8 FIX: Bounded JIT auto-indexing — when the resolved repo has no
    // symbols in the DB (first-run), synchronously ingest the workspace root
    // so the caller gets a usable graph without a manual ingest_repo step.
    let symbol_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
            rusqlite::params![repo_id],
            |row| row.get(0),
        )
        .unwrap_or(0);

    if symbol_count == 0 && workspace_root.is_dir() {
        eprintln!(
            "[MARROW] JIT auto-indexing repo '{}' at {} …",
            repo_id,
            workspace_root.display()
        );
        let t = Instant::now();
        ingestion::ingest_repo(conn, &repo_id, workspace_root)?;
        eprintln!(
            "[MARROW] JIT auto-indexing complete in {}ms.",
            t.elapsed().as_millis()
        );
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntegrationSupportTier {
    FirstClass,
    Secondary,
    CompatibilityOnly,
}

impl IntegrationSupportTier {
    #[allow(dead_code)]
    fn label(self) -> &'static str {
        match self {
            Self::FirstClass => "first-class",
            Self::Secondary => "secondary",
            Self::CompatibilityOnly => "compatibility-only",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntegrationTargetKind {
    Agent,
    Client,
    Host,
    RuntimeBackend,
}

impl IntegrationTargetKind {
    fn label(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::Client => "MCP client",
            Self::Host => "MCP host",
            Self::RuntimeBackend => "model/runtime backend",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IntegrationSetupMode {
    Automatic,
    Guided,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RuleFileSupport {
    None,
    ProjectOnly,
    ProjectAndGlobal,
}

impl RuleFileSupport {
    fn supports(self, scope: skills::Scope) -> bool {
        matches!(
            (self, scope),
            (Self::ProjectAndGlobal, _) | (Self::ProjectOnly, skills::Scope::Project)
        )
    }
}

#[derive(Clone, Copy)]
struct IntegrationTarget {
    name: &'static str,
    aliases: &'static [&'static str],
    support_tier: IntegrationSupportTier,
    kind: IntegrationTargetKind,
    setup_mode: IntegrationSetupMode,
    rule_support: RuleFileSupport,
    rule_agent: Option<skills::Agent>,
    workspace_rule_files: &'static [&'static str],
    baseline_workspace_required: bool,
    allow_config_write: bool,
    writer: Option<fn(&IntegrationCtx) -> Result<AgentOutcome>>,
}

const INTEGRATION_TARGETS: &[IntegrationTarget] = &[
    IntegrationTarget {
        name: "Claude Code",
        aliases: &["claude", "claude-code"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::ClaudeCode),
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: true,
        writer: Some(integrate_claude),
    },
    IntegrationTarget {
        name: "Antigravity",
        aliases: &["antigravity", "antigravity-gemini", "gemini-antigravity"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::Antigravity),
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: true,
        writer: Some(integrate_antigravity),
    },
    IntegrationTarget {
        name: "Cursor",
        aliases: &["cursor"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::Cursor),
        workspace_rule_files: &[".cursorrules"],
        baseline_workspace_required: true,
        allow_config_write: true,
        writer: Some(integrate_cursor),
    },
    IntegrationTarget {
        name: "GitHub Copilot",
        aliases: &["copilot", "github-copilot"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::GitHubCopilot),
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: true,
        writer: Some(integrate_copilot),
    },
    IntegrationTarget {
        name: "Cline",
        aliases: &["cline"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::Cline),
        workspace_rule_files: &[".clinerules"],
        baseline_workspace_required: true,
        allow_config_write: true,
        writer: Some(integrate_cline),
    },
    IntegrationTarget {
        name: "Zed",
        aliases: &["zed"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Automatic,
        rule_support: RuleFileSupport::ProjectAndGlobal,
        rule_agent: Some(skills::Agent::Zed),
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: true,
        writer: Some(integrate_zed),
    },
    IntegrationTarget {
        name: "Windsurf",
        aliases: &["windsurf", "codeium", "codeium-windsurf", ".windsurfrules"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::ProjectOnly,
        rule_agent: Some(skills::Agent::Windsurf),
        workspace_rule_files: &[".windsurfrules"],
        baseline_workspace_required: true,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Continue",
        aliases: &["continue", "continue-dev"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Roo Code",
        aliases: &["roo", "roo-code", "roocode", ".roomrules"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::ProjectOnly,
        rule_agent: Some(skills::Agent::RooCode),
        workspace_rule_files: &[".roomrules"],
        baseline_workspace_required: true,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Goose",
        aliases: &["goose"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "OpenHands",
        aliases: &["openhands", "open-hands"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Host,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "OpenClaw",
        aliases: &["openclaw", "open-claw"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Host,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Codex CLI",
        aliases: &["codex", "codex-cli", "openai-codex"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Gemini CLI",
        aliases: &["gemini", "gemini-cli"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "JetBrains AI Assistant",
        aliases: &["jetbrains-ai", "jetbrains-ai-assistant"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "JetBrains Junie",
        aliases: &["junie", "jetbrains-junie"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "LM Studio",
        aliases: &["lmstudio", "lm-studio"],
        support_tier: IntegrationSupportTier::FirstClass,
        kind: IntegrationTargetKind::Host,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Kilo Code",
        aliases: &["kilo", "kilo-code", "kilocode"],
        support_tier: IntegrationSupportTier::Secondary,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Sourcegraph Amp",
        aliases: &["amp", "sourcegraph-amp"],
        support_tier: IntegrationSupportTier::Secondary,
        kind: IntegrationTargetKind::Agent,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Augment Code",
        aliases: &["augment", "augment-code"],
        support_tier: IntegrationSupportTier::Secondary,
        kind: IntegrationTargetKind::Client,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Ollama",
        aliases: &["ollama"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "llama.cpp",
        aliases: &["llamacpp", "llama-cpp"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "vLLM",
        aliases: &["vllm"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "SGLang",
        aliases: &["sglang"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "LiteLLM",
        aliases: &["litellm", "lite-llm"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Ramalama",
        aliases: &["ramalama"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
    IntegrationTarget {
        name: "Docker Model Runner",
        aliases: &["docker-model-runner", "docker-models"],
        support_tier: IntegrationSupportTier::CompatibilityOnly,
        kind: IntegrationTargetKind::RuntimeBackend,
        setup_mode: IntegrationSetupMode::Guided,
        rule_support: RuleFileSupport::None,
        rule_agent: None,
        workspace_rule_files: &[],
        baseline_workspace_required: false,
        allow_config_write: false,
        writer: None,
    },
];

fn normalize_integration_name(name: &str) -> String {
    name.chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .collect()
}

fn integration_target_by_name(name: &str) -> Option<&'static IntegrationTarget> {
    let normalized = normalize_integration_name(name);
    INTEGRATION_TARGETS.iter().find(|target| {
        normalize_integration_name(target.name) == normalized
            || target
                .aliases
                .iter()
                .any(|alias| normalize_integration_name(alias) == normalized)
    })
}

fn integration_target_for_agent(agent: skills::Agent) -> Option<&'static IntegrationTarget> {
    INTEGRATION_TARGETS
        .iter()
        .find(|target| target.rule_agent == Some(agent))
}

fn integration_setup_targets() -> Vec<&'static IntegrationTarget> {
    let non_compat: Vec<&'static IntegrationTarget> = INTEGRATION_TARGETS
        .iter()
        .filter(|t| t.support_tier != IntegrationSupportTier::CompatibilityOnly)
        .collect();
    let mut result: Vec<&'static IntegrationTarget> = non_compat
        .iter()
        .copied()
        .filter(|t| t.setup_mode == IntegrationSetupMode::Automatic)
        .collect();
    result.extend(
        non_compat
            .iter()
            .copied()
            .filter(|t| t.setup_mode == IntegrationSetupMode::Guided),
    );
    result
}

fn integration_uses_universal_skills_dir(target: &IntegrationTarget) -> bool {
    integration_skill_directory(target) == UNIVERSAL_SKILLS_DIR
}

fn interactive_universal_mcp_targets() -> Vec<&'static IntegrationTarget> {
    integration_setup_targets()
        .into_iter()
        .filter(|target| integration_uses_universal_skills_dir(target))
        .collect()
}

fn interactive_additional_mcp_targets() -> Vec<&'static IntegrationTarget> {
    integration_setup_targets()
        .into_iter()
        .filter(|target| !integration_uses_universal_skills_dir(target))
        .collect()
}

#[allow(dead_code)] // Used in tests only after unified menu refactor
fn interactive_mcp_targets() -> Vec<&'static IntegrationTarget> {
    interactive_universal_mcp_targets()
        .into_iter()
        .chain(interactive_additional_mcp_targets())
        .collect()
}

fn agent_skill_target_has_mcp_integration(target: &AgentSkillTarget) -> bool {
    integration_target_by_name(target.name).is_some()
}

fn interactive_skill_only_agent_target_indices() -> Vec<usize> {
    AGENT_SKILL_TARGETS
        .iter()
        .enumerate()
        .filter(|(_, target)| !agent_skill_target_has_mcp_integration(target))
        .map(|(idx, _)| idx)
        .collect()
}

// ── Universal agents (visible upstream agents sharing .agents/skills) ─────────

/// Agents that read skills from `.agents/skills/` and are always included in
/// `marrow integrate`. These correspond to the visible (non-hidden) universal
/// entries from the Skills CLI upstream registry.
const UNIVERSAL_AGENTS: &[&str] = &[
    "Amp",
    "Antigravity",
    "Cline",
    "Codex",
    "Cursor",
    "Deep Agents",
    "Dexto",
    "Firebender",
    "Gemini CLI",
    "GitHub Copilot",
    "Kimi Code CLI",
    "OpenCode",
    "Warp",
];

const UNIVERSAL_SKILLS_DIR: &str = ".agents/skills";
const UNIVERSAL_GROUP_LABEL: &str = "Universal (.agents/skills)";
const ADDITIONAL_AGENTS_LABEL: &str = "Additional agents";

// ── Unified agent selection (for interactive flow) ────────────────────────────

enum AgentMenuEntry {
    Mcp(&'static IntegrationTarget),
    UniversalNoMcpTarget,
    SkillOnly(usize),
}

fn universal_agent_menu() -> (Vec<AgentMenuEntry>, Vec<String>) {
    let mut entries = Vec::new();
    let mut labels = Vec::new();

    for &name in UNIVERSAL_AGENTS {
        if let Some(target) = integration_target_by_name(name) {
            entries.push(AgentMenuEntry::Mcp(target));
        } else {
            entries.push(AgentMenuEntry::UniversalNoMcpTarget);
        }
        labels.push(name.to_string());
    }

    (entries, labels)
}

fn additional_agent_menu() -> (Vec<AgentMenuEntry>, Vec<String>) {
    let mut entries = Vec::new();
    let mut labels = Vec::new();

    for target in interactive_additional_mcp_targets() {
        labels.push(format_integration_menu_label(target));
        entries.push(AgentMenuEntry::Mcp(target));
    }

    for idx in interactive_skill_only_agent_target_indices() {
        let target = &AGENT_SKILL_TARGETS[idx];
        labels.push(format_skill_menu_label(target));
        entries.push(AgentMenuEntry::SkillOnly(idx));
    }

    (entries, labels)
}

fn partition_agent_menu_entries(
    selected_entries: &[&AgentMenuEntry],
) -> (Vec<&'static IntegrationTarget>, Vec<usize>, bool) {
    let mut mcp_selections = Vec::new();
    let mut skill_selections = Vec::new();
    let mut has_universal_no_mcp_target = false;

    for entry in selected_entries {
        match entry {
            AgentMenuEntry::Mcp(target) => mcp_selections.push(*target),
            AgentMenuEntry::UniversalNoMcpTarget => has_universal_no_mcp_target = true,
            AgentMenuEntry::SkillOnly(idx) => skill_selections.push(*idx),
        }
    }

    (
        mcp_selections,
        skill_selections,
        has_universal_no_mcp_target,
    )
}

// ── Agent skill targets (path-based, no MCP registration) ─────────────────────

struct AgentSkillTarget {
    /// Display name (e.g. "Aider Desk")
    name: &'static str,
    /// Aliases for direct-arg lookup (e.g. &["aider-desk"])
    aliases: &'static [&'static str],
    /// The skills directory relative to project root (e.g. ".aider-desk/skills").
    /// The installed file will be `<skills_dir>/marrow-optimization.md`.
    skills_dir: &'static str,
    /// Whether global scope is supported. Most skill-only targets are project-only.
    scope_support: RuleFileSupport,
}

const AGENT_SKILL_TARGETS: &[AgentSkillTarget] = &[
    AgentSkillTarget {
        name: "AiderDesk",
        aliases: &["aider-desk"],
        skills_dir: ".aider-desk/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Augment",
        aliases: &["augment", "augment-code"],
        skills_dir: ".augment/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "IBM Bob",
        aliases: &["bob", "ibm-bob"],
        skills_dir: ".bob/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "OpenClaw",
        aliases: &["openclaw", "open-claw"],
        skills_dir: "skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "CodeArts Agent",
        aliases: &["codearts-agent"],
        skills_dir: ".codeartsdoer/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "CodeBuddy",
        aliases: &["codebuddy"],
        skills_dir: ".codebuddy/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Codemaker",
        aliases: &["codemaker"],
        skills_dir: ".codemaker/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Code Studio",
        aliases: &["codestudio", "code-studio"],
        skills_dir: ".codestudio/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Command Code",
        aliases: &["command-code", "commandcode"],
        skills_dir: ".commandcode/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Continue",
        aliases: &["continue", "continue-dev"],
        skills_dir: ".continue/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Cortex Code",
        aliases: &["cortex", "cortex-code"],
        skills_dir: ".cortex/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Crush",
        aliases: &["crush"],
        skills_dir: ".crush/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Devin for Terminal",
        aliases: &["devin"],
        skills_dir: ".devin/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Droid",
        aliases: &["droid"],
        skills_dir: ".factory/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "ForgeCode",
        aliases: &["forgecode", "forge"],
        skills_dir: ".forge/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Goose",
        aliases: &["goose"],
        skills_dir: ".goose/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Hermes Agent",
        aliases: &["hermes-agent"],
        skills_dir: ".hermes/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Junie",
        aliases: &["junie", "jetbrains-junie"],
        skills_dir: ".junie/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "iFlow CLI",
        aliases: &["iflow-cli", "iflow"],
        skills_dir: ".iflow/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Kilo Code",
        aliases: &["kilo", "kilo-code", "kilocode"],
        skills_dir: ".kilocode/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Kiro CLI",
        aliases: &["kiro-cli", "kiro"],
        skills_dir: ".kiro/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Kode",
        aliases: &["kode"],
        skills_dir: ".kode/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "MCPJam",
        aliases: &["mcpjam"],
        skills_dir: ".mcpjam/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Mistral Vibe",
        aliases: &["mistral-vibe", "vibe"],
        skills_dir: ".vibe/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Mux",
        aliases: &["mux"],
        skills_dir: ".mux/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "OpenHands",
        aliases: &["openhands", "open-hands"],
        skills_dir: ".openhands/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Pi",
        aliases: &["pi"],
        skills_dir: ".pi/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Qoder",
        aliases: &["qoder"],
        skills_dir: ".qoder/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Qwen Code",
        aliases: &["qwen-code", "qwen"],
        skills_dir: ".qwen/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Tabnine CLI",
        aliases: &["tabnine-cli", "tabnine"],
        skills_dir: ".tabnine/agent/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    // ── Upstream non-universal targets with intentional MCP overlap ────────
    AgentSkillTarget {
        name: "Claude Code",
        aliases: &["claude-code"],
        skills_dir: ".claude/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Roo Code",
        aliases: &["roo-code", "roo"],
        skills_dir: ".roo/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Windsurf",
        aliases: &["windsurf"],
        skills_dir: ".windsurf/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    // ── Remaining upstream non-universal targets ──────────────────────────
    AgentSkillTarget {
        name: "Rovo Dev",
        aliases: &["rovodev", "rovo-dev"],
        skills_dir: ".rovodev/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Trae",
        aliases: &["trae"],
        skills_dir: ".trae/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Trae CN",
        aliases: &["trae-cn"],
        skills_dir: ".trae/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Zencoder",
        aliases: &["zencoder"],
        skills_dir: ".zencoder/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Neovate",
        aliases: &["neovate"],
        skills_dir: ".neovate/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "Pochi",
        aliases: &["pochi"],
        skills_dir: ".pochi/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
    AgentSkillTarget {
        name: "AdaL",
        aliases: &["adal"],
        skills_dir: ".adal/skills",
        scope_support: RuleFileSupport::ProjectOnly,
    },
];

fn agent_skill_target_by_name(name: &str) -> Option<&'static AgentSkillTarget> {
    let normalized = normalize_integration_name(name);
    AGENT_SKILL_TARGETS.iter().find(|target| {
        normalize_integration_name(target.name) == normalized
            || target
                .aliases
                .iter()
                .any(|alias| normalize_integration_name(alias) == normalized)
    })
}

fn combined_target_lookup(
    name: &str,
) -> (
    Option<&'static IntegrationTarget>,
    Option<&'static AgentSkillTarget>,
) {
    (
        integration_target_by_name(name),
        agent_skill_target_by_name(name),
    )
}

fn format_menu_label(name: &str, skills_dir: &str) -> String {
    format!("{name} ({skills_dir})")
}

fn is_universal_agent_name(name: &str) -> bool {
    let normalized = normalize_integration_name(name);
    UNIVERSAL_AGENTS
        .iter()
        .any(|candidate| normalize_integration_name(candidate) == normalized)
}

fn agent_skill_target_for_integration_target(
    target: &IntegrationTarget,
) -> Option<&'static AgentSkillTarget> {
    let target_names: Vec<String> = std::iter::once(target.name)
        .chain(target.aliases.iter().copied())
        .map(normalize_integration_name)
        .collect();

    AGENT_SKILL_TARGETS.iter().find(|candidate| {
        std::iter::once(candidate.name)
            .chain(candidate.aliases.iter().copied())
            .map(normalize_integration_name)
            .any(|name| target_names.contains(&name))
    })
}

fn integration_skill_directory(target: &IntegrationTarget) -> String {
    if let Some(skill_target) = agent_skill_target_for_integration_target(target) {
        return skill_target.skills_dir.to_string();
    }

    if std::iter::once(target.name)
        .chain(target.aliases.iter().copied())
        .any(is_universal_agent_name)
    {
        return UNIVERSAL_SKILLS_DIR.to_string();
    }

    let slug = target
        .aliases
        .iter()
        .copied()
        .find(|alias| !alias.starts_with('.') && !alias.contains('/'))
        .unwrap_or(target.name)
        .trim()
        .replace(' ', "-");

    format!(".{slug}/skills")
}

#[allow(dead_code)] // Used in tests only after unified menu refactor
fn universal_agent_menu_labels() -> Vec<String> {
    UNIVERSAL_AGENTS
        .iter()
        .map(|name| format_menu_label(name, UNIVERSAL_SKILLS_DIR))
        .collect()
}

fn format_skill_menu_label(target: &AgentSkillTarget) -> String {
    format_menu_label(target.name, target.skills_dir)
}

fn workspace_rule_targets() -> Vec<&'static IntegrationTarget> {
    INTEGRATION_TARGETS
        .iter()
        .filter(|target| !target.workspace_rule_files.is_empty())
        .collect()
}

const LEGACY_WORKSPACE_RULE_FILES_BY_INDEX: &[&[&str]] = &[
    &[".cursorrules"],
    &[".windsurfrules"],
    &[".clinerules", ".roomrules"],
];

fn workspace_rule_target_indices() -> Vec<usize> {
    (0..LEGACY_WORKSPACE_RULE_FILES_BY_INDEX.len()).collect()
}

fn baseline_workspace_rule_files() -> Vec<&'static str> {
    INTEGRATION_TARGETS
        .iter()
        .filter(|target| target.baseline_workspace_required)
        .flat_map(|target| target.workspace_rule_files.iter().copied())
        .collect()
}

fn format_workspace_setup_files() -> String {
    let mut files = baseline_workspace_rule_files();
    files.push(".vscode/mcp.json");
    files.join(", ")
}

fn workspace_is_initialized(root: &Path) -> bool {
    let rules = baseline_workspace_rule_files();
    root.join(".marrow").is_dir()
        && root.join(".marrowrc.json").exists()
        && root.join(".vscode/mcp.json").exists()
        && rules
            .iter()
            .all(|rule| path_contains_marrow_marker(&root.join(rule)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    SafeAppend,
    Overwrite,
    Symlink,
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

fn apply_compliance_gate(
    tool_name: &str,
    mut args: serde_json::Map<String, serde_json::Value>,
) -> ComplianceRewrite {
    let (intent, target_key) = match tool_name {
        "get_context_capsule" => ("explore_symbol", Some("symbol_name")),
        "analyze_impact" => ("refactor_symbol", Some("symbol_name")),
        "get_skeleton" => ("analyze_repo", Some("target_dir")),
        _ => {
            return ComplianceRewrite {
                tool_name: tool_name.to_string(),
                args,
                notice: None,
                action: ComplianceAction::None,
            }
        }
    };

    let mut routed = serde_json::Map::new();
    routed.insert(
        "intent".to_string(),
        serde_json::Value::String(intent.to_string()),
    );
    if let Some(key) = target_key {
        if let Some(value) = args.remove(key) {
            routed.insert("target".to_string(), value);
        }
    }
    if let Some(repo_id) = args.remove("repo_id") {
        routed.insert("repo_id".to_string(), repo_id);
    }
    if let Some(filepath) = args.remove("filepath") {
        routed.insert("filepath".to_string(), filepath);
    }
    ComplianceRewrite {
        tool_name: "run_pipeline".to_string(),
        args: routed,
        notice: Some(format!(
            "[MARROW COMPLIANCE] Direct '{}' call was auto-routed through `run_pipeline`. Use `run_pipeline` first to avoid this warning.\n",
            tool_name
        )),
        action: ComplianceAction::AutoRouted,
    }
}

fn ensure_workspace_config() -> Result<()> {
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

    // Always write "default". Normalize any legacy "strict" value on read.
    cfg["enforcement_mode"] = serde_json::Value::String("default".to_string());

    fs::write(".marrowrc.json", serde_json::to_string_pretty(&cfg)?)?;
    Ok(())
}

fn fallback_paths_for_agent(agent: skills::Agent, workspace_root: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = integration_target_for_agent(agent)
        .into_iter()
        .flat_map(|target| target.workspace_rule_files.iter().copied())
        .map(|path| workspace_root.join(path))
        .collect();

    let legacy_fallbacks: &[&str] = match agent {
        skills::Agent::Cursor => &[".cursorrules", ".vscode/mcp.json"],
        skills::Agent::GitHubCopilot => &[".vscode/mcp.json"],
        skills::Agent::Antigravity => &[".roomrules"],
        _ => &[],
    };

    for fallback in legacy_fallbacks {
        let path = workspace_root.join(fallback);
        if !paths.iter().any(|existing| existing == &path) {
            paths.push(path);
        }
    }

    paths
}

fn coverage_status_for_agent(
    agent: skills::Agent,
    workspace_root: &Path,
    home: &Path,
) -> (&'static str, String) {
    let Some(target) = integration_target_for_agent(agent) else {
        return (
            "unprotected",
            "agent is not registered as a rule-file target".to_string(),
        );
    };

    if target.rule_support.supports(skills::Scope::Project) {
        let project_target = workspace_root.join(agent.target_path(skills::Scope::Project, home));
        if project_target.exists() && path_contains_marrow_marker(&project_target) {
            return (
                "protected",
                format!("project instructions at {}", project_target.display()),
            );
        }
    }

    if target.rule_support.supports(skills::Scope::Global) {
        let global_target = agent.target_path(skills::Scope::Global, home);
        if global_target.exists() && path_contains_marrow_marker(&global_target) {
            return (
                "protected",
                format!("global instructions at {}", global_target.display()),
            );
        }
    }

    let fallback_hits: Vec<String> = fallback_paths_for_agent(agent, workspace_root)
        .into_iter()
        .filter(|path| path_contains_marrow_marker(path))
        .map(|path| path.display().to_string())
        .collect();
    if !fallback_hits.is_empty() {
        return (
            "partial",
            format!(
                "fallback workspace files present: {}",
                fallback_hits.join(", ")
            ),
        );
    }

    (
        "unprotected",
        "no agent-specific Marrow instruction target found".to_string(),
    )
}

fn format_agent_coverage_summary(workspace_root: &Path, home: &Path) -> String {
    let mut out = String::new();
    writeln!(out, "Agent coverage:").ok();
    for target in INTEGRATION_TARGETS
        .iter()
        .filter(|target| target.rule_agent.is_some())
    {
        let agent = target.rule_agent.expect("filtered above");
        let (status, detail) = coverage_status_for_agent(agent, workspace_root, home);
        writeln!(out, "- {}: {status} ({detail})", target.name).ok();
    }
    out.trim_end().to_string()
}

fn format_validation_report(
    workspace_root: &Path,
    home: &Path,
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
         {}\n\
         Compliance stats:\n\
         - run_pipeline requests: {}\n\
         - direct low-level auto-routed: {}\n\
         - direct low-level rejected: {}\n\
         - ambiguous symbol requests: {}\n\
         - stale capsule preventions: {}\n\
         - run_pipeline compliance rate: {:.1}%",
        workspace_root.display(),
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
        std::fs::create_dir_all(root.join(".marrow")).map_err(|e| {
            eprintln!("[MARROW AUTO-INIT] Warning: could not create .marrow/: {e}");
            e
        })?;
        let rule_indices = workspace_rule_target_indices();
        if let Err(e) = write_workspace_rules(
            &root,
            &rule_indices,
            WORKSPACE_RULES_CONTENT_SOFT,
            WriteMode::SafeAppend,
        ) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write workspace rules: {e}");
        }
        if let Err(e) = write_vscode_mcp_config(&root, WriteMode::SafeAppend) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write .vscode/mcp.json: {e}");
        }
        if let Err(e) = ensure_workspace_config() {
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
         written registry-backed workspace rules. Please notify the user \
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
                "Use Marrow for structural questions (callers, blast radius, repo maps, class maps), \
                 including explore_batch, dependency_graph, and map_class. Use native read/search \
                 for single-file, line-level, config/docs, or exact-search work."
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
    ) -> impl std::future::Future<Output = Result<InitializeResult, rmcp::ErrorData>> + Send + '_
    {
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
                capabilities: info.capabilities,
                server_info: info.server_info,
                instructions: info.instructions,
            })
        }
    }

    // ── Tool registry ─────────────────────────────────────────────────────────

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        use serde_json::json;

        let tools = vec![
            Tool::new(
                "get_context_capsule",
                "Returns the pivot symbol's full source plus condensed depth-1 callers, \
                 callees, and imports.",
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
                        },
                        "filepath": {
                            "type": "string",
                            "description": "Relative file path to disambiguate symbols with identical names across files. ALWAYS provide this when you know which file the symbol is in (e.g. from the user's open editor tab, cursor location, or a previous skeleton/search result). Example: 'app/controllers/invoices_controller.rb'."
                        }
                    },
                    "required": ["symbol_name", "repo_id"]
                })),
            ),
            Tool::new(
                "analyze_impact",
                "Recursively maps the blast radius of a proposed change across callers and importers.",
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
                        },
                        "filepath": {
                            "type": "string",
                            "description": "Relative file path to disambiguate symbols with identical names across files. ALWAYS provide this when you know which file the symbol is in (e.g. from the user's open editor tab, cursor location, or a previous skeleton/search result)."
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
                "Returns a token-efficient Markdown map of one repo's high-level architecture, \
                 grouped by file.",
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
                "Use for code structure, dependencies, refactor blast radius, or codebase maps. \
                 Skip for single-file lookups, line-level reads, and grep-style text search — \
                 native tools are faster there. Use `find_symbol` to discover symbol names from \
                 a partial query before calling explore_symbol. Use intent 'analyze_repo' to map \
                 the full codebase, 'find_symbol' to search indexed symbols, 'explore_symbol' to \
                 understand a specific symbol, 'trace_flow' to linearly trace a symbol's outbound \
                 execution path, 'refactor_symbol' to assess blast radius, or 'read_node' to expand \
                 a neighbor signature into its full source. Use 'explore_batch' for 3+ related \
                 symbols, 'dependency_graph' for multi-hop caller/callee maps, and 'map_class' for \
                 full class-level architecture maps.",
                Self::run_pipeline_schema(),
            ),
            Tool::new(
                "workspace_setup",
                "CRITICAL: Run this tool when the user asks you to initialize Marrow, OR \
                 automatically if `.marrow/` does not exist in the current workspace and \
                 auto-init has not already run. Generates workspace rule files, writes \
                 `.vscode/mcp.json`, and persists the workspace configuration.",
                Self::schema(json!({
                    "type": "object",
                    "properties": {}
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
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
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
            let compliance = apply_compliance_gate(&original_tool_name, args);
            let compliance_notice = compliance.notice;
            let compliance_action = compliance.action;
            let args = compliance.args;

            let mut jit_auto_indexed = false;

            let mut result = match compliance.tool_name.as_str() {
                // ── get_context_capsule ───────────────────────────────────────
                "get_context_capsule" => {
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();
                    let filepath_arg = args
                        .get("filepath")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let client_name = CLIENT_NAME
                        .get()
                        .cloned()
                        .unwrap_or_else(|| "Unknown Agent".to_string());

                    let cwd = current_workspace_root();
                    if let Some(msg) = self.maybe_jit_index(&repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let sym_for_event = symbol_name.clone();
                    let repo_for_event = repo_id.clone();

                    let (
                        out,
                        original_text,
                        capsule_tokens,
                        file_tokens,
                        abs_file_path,
                        proof_snapshot,
                        provenance,
                    ) = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let _resolved_repo_id = ensure_repo_ready(&conn, Some(&repo_id), &cwd)?;

                        let capsule_result = retrieval::get_context_capsule(
                            &conn,
                            &symbol_name,
                            &repo_id,
                            filepath_arg.as_deref(),
                        )?;

                        let full_file_tokens = capsule_result.file_tokens;
                        let optimized_tokens = capsule_result.optimized_text.len() / 4;
                        let original_text_out = capsule_result.original_text;
                        let proof_snapshot = capsule_result.proof_snapshot;
                        let provenance = capsule_result.provenance;
                        let out = capsule_result.optimized_text;

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
                        let saved =
                            (full_file_tokens as i64).saturating_sub(optimized_tokens as i64);
                        let _ = db::increment_stat(&conn, "total_requests", 1);
                        let _ =
                            db::increment_stat(&conn, "total_file_tokens", full_file_tokens as i64);
                        let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                        Ok::<_, anyhow::Error>((
                            out,
                            original_text_out,
                            optimized_tokens,
                            full_file_tokens,
                            abs_path_str,
                            proof_snapshot,
                            provenance,
                        ))
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    let tokens_saved = file_tokens.saturating_sub(capsule_tokens);

                    let original_for_emit = if retrieval::capsule_original_mode()
                        == retrieval::CapsuleOriginalMode::None
                        && original_text.is_empty()
                    {
                        None
                    } else {
                        Some(original_text)
                    };

                    let event = DashboardEvent::CapsuleServed {
                        symbol: sym_for_event,
                        repo: repo_for_event,
                        file: abs_file_path,
                        capsule_tokens,
                        file_tokens,
                        tokens_saved,
                        origin: client_name,
                        ts: dashboard::now_ts(),
                        original_text: original_for_emit,
                        optimized_text: Some(out.clone()),
                        proof_snapshot: proof_snapshot.map(Box::new),
                        provenance: Box::new(provenance),
                        has_cached_delta: false,
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
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();
                    let filepath_arg = args
                        .get("filepath")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    let cwd = current_workspace_root();
                    if let Some(msg) = self.maybe_jit_index(&repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let sym_clone = symbol_name.clone();
                    let repo_clone = repo_id.clone();

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let _resolved_repo_id = ensure_repo_ready(&conn, Some(&repo_id), &cwd)?;

                        retrieval::analyze_impact(
                            &conn,
                            &symbol_name,
                            &repo_id,
                            filepath_arg.as_deref(),
                        )
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    // Short-circuit: return the disambiguation payload if the symbol was ambiguous.
                    if let Some(payload) = result.pivot_id.strip_prefix("DISAMBIGUATION:") {
                        return Ok(CallToolResult::success(vec![Content::text(
                            payload.to_string(),
                        )]));
                    }

                    let out = retrieval::format_impact_result(&result);

                    let event = DashboardEvent::ImpactAnalyzed {
                        symbol: sym_clone,
                        repo: repo_clone,
                        affected_count: result.affected.len(),
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

                    Ok(CallToolResult::success(vec![Content::text(out)]))
                }

                // ── ingest_repo ───────────────────────────────────────────────
                "ingest_repo" => {
                    let repo_id = Self::require_str(&args, "repo_id")?.to_string();
                    let raw_path = Self::require_str(&args, "root_path")?.to_string();
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

                    // C-2 FIX: Bound against canonical workspace root, not CWD.
                    let workspace_root = current_workspace_root();

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
                                "CRITICAL SECURITY: Cannot index protected system directories.",
                            )]));
                        }
                    }

                    // C-2 FIX: Check against workspace root, not process CWD
                    let is_inside_workspace = root_path.starts_with(&workspace_root);

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

                    let mut registered_workspace_id = None;
                    let db_for_ingestion =
                        match registry::Registry::open_default().and_then(|registry| {
                            registry.register_workspace_with_boundary(
                                &root_path,
                                Some(&workspace_root),
                                user_confirmed,
                                None,
                            )
                        }) {
                            Ok(entry) => {
                                registered_workspace_id = Some(entry.workspace_id.clone());
                                let graph_db_path =
                                    entry.graph_db_path.to_string_lossy().to_string();
                                match db::init_db_or_memory(&graph_db_path) {
                                    Ok(conn) => Arc::new(Mutex::new(conn)),
                                    Err(e) => {
                                        return Err(rmcp::ErrorData::internal_error(
                                            format!("failed to open registered workspace DB: {e}"),
                                            None,
                                        ))
                                    }
                                }
                            }
                            Err(_) => Arc::clone(&db),
                        };

                    // Rule 1 (inside workspace) or Rule 4 (outside + confirmed): proceed
                    let repo_id_for_event = repo_id.clone();
                    let activity_client = ipc::default_client();
                    let activity_id = activity_client
                        .start_activity(
                            activity::ActivityKind::IndexingJob,
                            registered_workspace_id.clone(),
                            format!("indexing {}", root_path.display()),
                        )
                        .await
                        .ok()
                        .flatten();

                    let ingest_result = tokio::task::spawn_blocking(move || {
                        ingestion::run_ingestion_with_arc_and_activity(
                            &db_for_ingestion,
                            &repo_id,
                            &root_path,
                            |_| {},
                            None,
                            registered_workspace_id,
                        )
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None));
                    if let Some(activity_id) = activity_id.as_deref() {
                        match &ingest_result {
                            Ok((symbols, edges)) => {
                                let _ = activity_client
                                    .finish_activity(
                                        activity_id,
                                        activity::ActivityState::Completed,
                                        format!("indexed {symbols} symbols / {edges} edges"),
                                    )
                                    .await;
                            }
                            Err(error) => {
                                let _ = activity_client
                                    .finish_activity(
                                        activity_id,
                                        activity::ActivityState::Error,
                                        format!("{error:?}"),
                                    )
                                    .await;
                            }
                        }
                    }
                    let (symbols, edges) = ingest_result?;

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
                    let symbol_name = Self::require_str(&args, "symbol_name")?.to_string();
                    let filepath = Self::require_str(&args, "filepath")?.to_string();
                    let observation_text = Self::require_str(&args, "observation")?.to_string();
                    let repo_id_arg = args
                        .get("repo_id")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let repo_id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)?;
                        db::save_observation(
                            &conn,
                            &repo_id,
                            &symbol_name,
                            &filepath,
                            &observation_text,
                        )
                    })
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                    Ok(CallToolResult::success(vec![Content::text(result)]))
                }

                // ── get_session_context ───────────────────────────────────────
                "get_session_context" => {
                    let repo_id = args
                        .get("repo_id")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let symbol_name = args
                        .get("symbol_name")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let filepath = args
                        .get("filepath")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
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
                    let repo_id_arg = args
                        .get("repo_id")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);

                    let cwd = current_workspace_root();
                    // Resolve the repo_id for JIT check (may be None → fallback)
                    let jit_repo_id = {
                        let conn = db.lock().map_err(|_| {
                            rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None)
                        })?;
                        resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                    };
                    if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    let result = tokio::task::spawn_blocking(move || {
                        let conn = db
                            .lock()
                            .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                        let cwd = current_workspace_root();
                        let repo_id = ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
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
                    let target = args
                        .get("target")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let repo_id_arg = args
                        .get("repo_id")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let filepath_arg = args
                        .get("filepath")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let kind_arg = args
                        .get("kind")
                        .and_then(|v| v.as_str())
                        .map(str::to_string);
                    let client_name = CLIENT_NAME
                        .get()
                        .cloned()
                        .unwrap_or_else(|| "Unknown Agent".to_string());

                    if let Some(msg) = state::run_pipeline_guard_message() {
                        return Ok(CallToolResult::success(vec![Content::text(msg)]));
                    }

                    // M-13 FIX: If the graph is empty, perform bounded JIT
                    // auto-indexing so run_pipeline callers get a usable result
                    // without a manual ingest_repo step.
                    {
                        let conn = db.lock().map_err(|_| {
                            rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None)
                        })?;
                        let node_count: i64 = conn
                            .query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))
                            .unwrap_or(0);
                        if node_count == 0 {
                            let cwd = current_workspace_root();
                            if cwd.is_dir() {
                                let repo_id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                eprintln!(
                                    "[MARROW] run_pipeline JIT auto-indexing '{}' at {} …",
                                    repo_id,
                                    cwd.display()
                                );
                                let t = Instant::now();
                                ingestion::ingest_repo(&conn, &repo_id, &cwd).map_err(|e| {
                                    rmcp::ErrorData::internal_error(
                                        format!("JIT auto-indexing failed: {e}"),
                                        None,
                                    )
                                })?;
                                eprintln!(
                                    "[MARROW] run_pipeline JIT auto-indexing complete in {}ms.",
                                    t.elapsed().as_millis()
                                );
                                jit_auto_indexed = true;
                            } else {
                                return Ok(CallToolResult::success(vec![Content::text(
                                    "[SYSTEM NOTE: The Marrow graph is empty and the workspace \
                                     directory is not accessible. Run ingest_repo first.]",
                                )]));
                            }
                        }
                    }

                    match intent.as_str() {
                        "explore_batch" => {
                            let pipeline_t = Instant::now();
                            let queries = parse_batch_queries(&args)?;

                            trace!("explore_batch: start — {} queries", queries.len());

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "explore_batch: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let execution = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "explore_batch: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
                                let execution = retrieval::execute_batch_queries(
                                    &conn,
                                    &queries,
                                    retrieval::BatchOptions {
                                        repo_id,
                                        max_bytes: retrieval::batch_max_bytes(),
                                    },
                                )?;
                                db::increment_stat(&conn, "batch_requests", 1)?;
                                db::increment_stat(
                                    &conn,
                                    "batch_queries",
                                    execution.query_count as i64,
                                )?;
                                if execution.truncated {
                                    db::increment_stat(&conn, "batch_truncated", 1)?;
                                }
                                Ok::<_, anyhow::Error>(execution)
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "explore_batch: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let retrieval::BatchExecution {
                                text, telemetry, ..
                            } = execution;
                            for event in
                                dashboard_events_from_batch_telemetry(telemetry, &client_name)
                            {
                                emit_dashboard_event(self.http_client.clone(), event);
                            }

                            Ok(CallToolResult::success(vec![Content::text(text)]))
                        }

                        "dependency_graph" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'dependency_graph' requires a 'target' (symbol name)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let graph_options = parse_dependency_graph_options(&args)?;
                            let sym_for_event = symbol_name.clone();

                            trace!("dependency_graph: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "dependency_graph: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "dependency_graph: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
                                let result = retrieval::dependency_graph(
                                    &conn,
                                    &repo_id,
                                    &symbol_name,
                                    filepath_arg.as_deref(),
                                    graph_options,
                                )?;
                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "dependency_graph: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let node_count = result
                                .lines()
                                .filter(|line| line.trim_start().starts_with("- [d"))
                                .count();
                            let event = DashboardEvent::SkeletonGenerated {
                                target_dir: format!("dependency_graph:{repo_used}:{sym_for_event}"),
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

                        "map_class" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'map_class' requires a 'target' (class or symbol name)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event = symbol_name.clone();

                            trace!("map_class: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "map_class: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "map_class: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;
                                let result = retrieval::map_class(
                                    &conn,
                                    &repo_id,
                                    &symbol_name,
                                    filepath_arg.as_deref(),
                                )?;
                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "map_class: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let node_count = result
                                .lines()
                                .filter(|line| line.trim_start().starts_with("- "))
                                .count();
                            let event = DashboardEvent::SkeletonGenerated {
                                target_dir: format!("map_class:{repo_used}:{sym_for_event}"),
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

                        "analyze_repo" => {
                            let pipeline_t = Instant::now();
                            // M-18 FIX: Normalize `.` target to None (repo root).
                            let target_dir = match target.as_deref() {
                                Some(".") | Some("./") | Some("") => None,
                                other => other.map(String::from),
                            };
                            let target_dir_label = target_dir
                                .clone()
                                .unwrap_or_else(|| "(workspace)".to_string());

                            trace!("analyze_repo: start — target_dir={target_dir_label}");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "analyze_repo: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "analyze_repo: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_skel = Instant::now();
                                let skeleton = retrieval::get_project_skeleton(
                                    &conn,
                                    &repo_id,
                                    target_dir.as_deref(),
                                )?;
                                trace!(
                                    "analyze_repo: get_project_skeleton [{:?}ms] — {} chars",
                                    t_skel.elapsed().as_millis(),
                                    skeleton.len()
                                );

                                Ok::<_, anyhow::Error>((skeleton, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "analyze_repo: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

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

                        "find_symbol" => {
                            let pipeline_t = Instant::now();
                            let limit_arg = parse_find_symbol_limit(&args)?;
                            let query = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'find_symbol' requires a 'target' (symbol fragment)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let query_for_event = query.clone();

                            trace!("find_symbol: start — query='{query}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "find_symbol: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, _repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "find_symbol: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_find = Instant::now();
                                let result = dispatch_run_pipeline_find_symbol(
                                    &conn,
                                    &repo_id,
                                    &query,
                                    kind_arg.as_deref(),
                                    limit_arg,
                                )?;
                                trace!(
                                    "find_symbol: find_symbols [{:?}ms] — {} chars",
                                    t_find.elapsed().as_millis(),
                                    result.len()
                                );

                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "find_symbol: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let node_count = result
                                .lines()
                                .filter(|line| line.trim_start().starts_with("- "))
                                .count();
                            let event = DashboardEvent::SkeletonGenerated {
                                target_dir: format!("find:{query_for_event}"),
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

                        // "read_node" is a navigation alias for explore_symbol.
                        // Agents use it to "click" on a neighbor link from a
                        // previous Progressive Disclosure capsule response.
                        "explore_symbol" | "read_node" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'explore_symbol' requires a 'target' (symbol name)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event = symbol_name.clone();

                            trace!("explore_symbol: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "explore_symbol: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (out, original_text, capsule_tokens, file_tokens, abs_file_path, repo_used, proof_snapshot, provenance) =
                                tokio::task::spawn_blocking(move || {
                                    let t_lock = Instant::now();
                                    let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                    trace!("explore_symbol: db lock acquired [{:?}ms]", t_lock.elapsed().as_millis());

                                    let cwd = current_workspace_root();
                                    let repo_id =
                                        ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                    let t_cap = Instant::now();
                                    let capsule_result = retrieval::get_context_capsule(&conn, &symbol_name, &repo_id, filepath_arg.as_deref())?;
                                    let full_file_tokens  = capsule_result.file_tokens;
                                    let optimized_tokens  = capsule_result.optimized_text.len() / 4;
                                    let original_text_out = capsule_result.original_text;
                                    let proof_snapshot    = capsule_result.proof_snapshot;
                                    let provenance        = capsule_result.provenance;
                                    let out               = capsule_result.optimized_text;
                                    trace!("explore_symbol: get_context_capsule [{:?}ms] — orig={}B opt={}B", t_cap.elapsed().as_millis(), original_text_out.len(), out.len());

                                    let t_lookup = Instant::now();
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
                                    trace!("explore_symbol: abs_path lookup [{:?}ms]", t_lookup.elapsed().as_millis());

                                    let saved = (full_file_tokens as i64).saturating_sub(optimized_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_requests",     1);
                                    let _ = db::increment_stat(&conn, "total_file_tokens",  full_file_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                                    Ok::<_, anyhow::Error>((out, original_text_out, optimized_tokens, full_file_tokens, abs_path_str, repo_id, proof_snapshot, provenance))
                                })
                                .await
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "explore_symbol: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let tokens_saved = file_tokens.saturating_sub(capsule_tokens);
                            let original_for_emit = if retrieval::capsule_original_mode()
                                == retrieval::CapsuleOriginalMode::None
                                && original_text.is_empty()
                            {
                                None
                            } else {
                                Some(original_text)
                            };

                            let event = DashboardEvent::CapsuleServed {
                                symbol: sym_for_event,
                                repo: repo_used,
                                file: abs_file_path,
                                capsule_tokens,
                                file_tokens,
                                tokens_saved,
                                origin: client_name,
                                ts: dashboard::now_ts(),
                                original_text: original_for_emit,
                                optimized_text: Some(out.clone()),
                                proof_snapshot: proof_snapshot.map(Box::new),
                                provenance: Box::new(provenance),
                                has_cached_delta: false,
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

                        // ── trace_flow ────────────────────────────────────────
                        "trace_flow" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'trace_flow' requires a 'target' (symbol name)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event = symbol_name.clone();

                            trace!("trace_flow: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "trace_flow: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (
                                out,
                                capsule_tokens,
                                file_tokens,
                                abs_file_path,
                                repo_used,
                                provenance,
                            ) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "trace_flow: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_trace = Instant::now();
                                let result = retrieval::trace_logic_flow(
                                    &conn,
                                    &symbol_name,
                                    &repo_id,
                                    filepath_arg.as_deref(),
                                )?;
                                let optimized_tokens = result.optimized_text.len() / 4;
                                let file_tokens = result.file_tokens;
                                let provenance = result.provenance;
                                let out = result.optimized_text;
                                trace!(
                                    "trace_flow: trace_logic_flow [{:?}ms] — {}B",
                                    t_trace.elapsed().as_millis(),
                                    out.len()
                                );

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

                                let saved =
                                    (file_tokens as i64).saturating_sub(optimized_tokens as i64);
                                let _ = db::increment_stat(&conn, "total_requests", 1);
                                let _ = db::increment_stat(
                                    &conn,
                                    "total_file_tokens",
                                    file_tokens as i64,
                                );
                                let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                                Ok::<_, anyhow::Error>((
                                    out,
                                    optimized_tokens,
                                    file_tokens,
                                    abs_path_str,
                                    repo_id,
                                    provenance,
                                ))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "trace_flow: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            let tokens_saved = file_tokens.saturating_sub(capsule_tokens);
                            let event = DashboardEvent::CapsuleServed {
                                symbol: sym_for_event,
                                repo: repo_used,
                                file: abs_file_path,
                                capsule_tokens,
                                file_tokens,
                                tokens_saved,
                                origin: client_name,
                                ts: dashboard::now_ts(),
                                original_text: None,
                                optimized_text: Some(out.clone()),
                                proof_snapshot: None,
                                provenance: Box::new(provenance),
                                has_cached_delta: false,
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

                        "refactor_symbol" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'refactor_symbol' requires a 'target' (symbol name)"
                                        .to_string(),
                                    None,
                                )
                            })?;
                            let sym_clone = symbol_name.clone();

                            trace!("refactor_symbol: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| {
                                    rmcp::ErrorData::internal_error(
                                        "DB mutex poisoned".to_string(),
                                        None,
                                    )
                                })?;
                                let id =
                                    resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                        .map_err(|e| {
                                            rmcp::ErrorData::internal_error(e.to_string(), None)
                                        })?;
                                trace!(
                                    "refactor_symbol: resolve_repo_id='{id}' [{:?}ms]",
                                    t.elapsed().as_millis()
                                );
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db
                                    .lock()
                                    .map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!(
                                    "refactor_symbol: db lock acquired [{:?}ms]",
                                    t_lock.elapsed().as_millis()
                                );

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_impact = Instant::now();
                                let result = retrieval::analyze_impact(
                                    &conn,
                                    &symbol_name,
                                    &repo_id,
                                    filepath_arg.as_deref(),
                                )?;
                                trace!(
                                    "refactor_symbol: analyze_impact [{:?}ms] — {} affected",
                                    t_impact.elapsed().as_millis(),
                                    result.affected.len()
                                );

                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!(
                                "refactor_symbol: spawn_blocking total [{:?}ms]",
                                pipeline_t.elapsed().as_millis()
                            );

                            // Short-circuit: return the disambiguation payload if the symbol was ambiguous.
                            if let Some(payload) = result.pivot_id.strip_prefix("DISAMBIGUATION:") {
                                return Ok(CallToolResult::success(vec![Content::text(
                                    payload.to_string(),
                                )]));
                            }

                            let out = retrieval::format_impact_result(&result);

                            let event = DashboardEvent::ImpactAnalyzed {
                                symbol: sym_clone,
                                repo: repo_used,
                                affected_count: result.affected.len(),
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

                            Ok(CallToolResult::success(vec![Content::text(out)]))
                        }

                        _ => Err(rmcp::ErrorData::invalid_params(
                            run_pipeline_invalid_intent_message().to_string(),
                            None,
                        )),
                    }
                }

                // ── workspace_setup ───────────────────────────────────────────
                "workspace_setup" => {
                    tokio::task::spawn_blocking(move || {
                        let workspace_root =
                            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                        let rule_indices = workspace_rule_target_indices();
                        write_workspace_rules(
                            &workspace_root,
                            &rule_indices,
                            WORKSPACE_RULES_CONTENT_SOFT,
                            WriteMode::SafeAppend,
                        )?;
                        write_vscode_mcp_config(&workspace_root, WriteMode::SafeAppend)?;
                        ensure_workspace_config()?;
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
                        format_workspace_setup_summary(&cwd, &home),
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
                tool_result
                    .content
                    .insert(0, Content::text(notice.as_str()));
            }
            if let (Some(notice), Ok(ref mut tool_result)) = (&init_notice, &mut result) {
                tool_result
                    .content
                    .insert(0, Content::text(notice.as_str()));
            }
            // M-13 FIX: Prepend auto-indexed system note when JIT ran
            if jit_auto_indexed {
                if let Ok(ref mut tool_result) = result {
                    tool_result
                        .content
                        .insert(0, Content::text("[SYSTEM NOTE: Auto-Indexed]\n\n"));
                }
            }

            result
        }
    }
}

fn rule_install_note() -> &'static str {
    "Rule files strengthen Marrow-first behavior for each agent's native instruction surface. Managed Marrow files are updated; custom files are preserved. Auto-routing of direct low-level calls through run_pipeline is always active."
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
        skills::InstallStatus::Refreshed => "rules refreshed",
        skills::InstallStatus::PreservedExisting => "rules preserved",
    };
    format!("{name} {action} -> {}", target.display())
}

fn install_status_label(status: skills::InstallStatus) -> &'static str {
    match status {
        skills::InstallStatus::Written => "installed",
        skills::InstallStatus::Refreshed => "updated",
        skills::InstallStatus::PreservedExisting => "preserved existing",
    }
}

fn format_workspace_setup_summary(workspace_root: &Path, home: &Path) -> String {
    format!(
        "[MARROW] Workspace setup complete.\n\
         Path: {}\n\
         Files: {}\n\
         Existing files are preserved when Marrow rules are already present.\n\
         Registry-backed rule files cover Cursor, Cline, Roo Code, and Windsurf workspace guidance.\n\
         {}\n\
         Run `marrow integrate` to install agent-specific instruction files where coverage is still partial or unprotected.",
        workspace_root.display(),
        format_workspace_setup_files(),
        format_agent_coverage_summary(workspace_root, home)
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Process-global CWD is shared state; serialize every test that calls
    /// `std::env::set_current_dir` behind this mutex so they cannot race.
    static CWD_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn assert_soft_workspace_guidance(text: &str) {
        assert!(
            text.contains("MARROW AST CONTEXT ENGINE"),
            "soft guidance must retain the Marrow sentinel: {text}"
        );
        assert!(
            text.contains("find_symbol"),
            "soft guidance should document find_symbol routing: {text}"
        );
        assert!(
            text.contains("Native read/search tools are fine"),
            "soft guidance should allow native read/search for narrow lookups: {text}"
        );

        for forbidden in [
            "STRICT WORKFLOW PROTOCOL",
            "For EVERY coding task",
            "strictly forbidden",
            "Never read raw files directly",
            "forbidden from using `grep`",
            "native tool to fetch neighbor bodies",
        ] {
            assert!(
                !text.contains(forbidden),
                "soft guidance must not contain strict-only wording {forbidden:?}: {text}"
            );
        }
    }

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
    fn mcp_session_finish_state_marks_disconnect_and_error() {
        let ok: Result<()> = Ok(());
        assert_eq!(
            mcp_session_finish_state(&ok),
            activity::ActivityState::Stopped
        );

        let err: Result<()> = Err(anyhow::anyhow!("stdio failed"));
        assert_eq!(
            mcp_session_finish_state(&err),
            activity::ActivityState::Error
        );
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
        assert!(s.contains("foo"), "symbol name missing: {s}");
        assert!(s.contains("def foo(): pass"), "pivot text missing: {s}");
        assert!(s.contains("none"), "isolated-symbol marker missing: {s}");
    }

    #[test]
    fn format_benchmark_table_contains_all_metrics() {
        let provenance = retrieval::CapsuleProvenance {
            baseline_token_source: "exact".to_string(),
            tokenizer_mode: "cl100k_base".to_string(),
            original_mode: "none".to_string(),
            proof_label: "cached_proof".to_string(),
            precise_file_tokens: true,
            original_max_bytes: None,
            proof_max_bytes: 16_384,
            proof_max_files: 8,
            touched_file_count: 2,
        };
        let table =
            format_benchmark_table("my_func", "my_repo", "src/foo.cpp", 1_000, 100, &provenance);
        // Header info
        assert!(table.contains("my_func"), "symbol missing:\n{table}");
        assert!(table.contains("my_repo"), "repo missing:\n{table}");
        assert!(table.contains("src/foo.cpp"), "file path missing:\n{table}");
        // Metric values
        assert!(table.contains("1,000"), "file tokens missing:\n{table}");
        assert!(table.contains("100"), "capsule tokens missing:\n{table}");
        assert!(table.contains("900"), "saved tokens missing:\n{table}");
        assert!(table.contains("90.0%"), "reduction % missing:\n{table}");
        assert!(table.contains("exact"), "baseline source missing:\n{table}");
        assert!(table.contains("cl100k_base"), "tokenizer missing:\n{table}");
    }

    #[test]
    fn format_benchmark_table_zero_reduction_when_equal() {
        let table = format_benchmark_table(
            "s",
            "r",
            "f.py",
            500,
            500,
            &retrieval::CapsuleProvenance::default(),
        );
        assert!(table.contains("Tokens Saved"), "label missing:\n{table}");
        assert!(table.contains("0.0%"), "reduction should be 0.0%:\n{table}");
    }

    fn insert_benchmark_repo(conn: &rusqlite::Connection, id: &str, root_path: &str) {
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![id, root_path],
        )
        .unwrap();
    }

    fn insert_benchmark_node(
        conn: &rusqlite::Connection,
        repo_id: &str,
        file_path: &str,
        language: &str,
        symbol_name: &str,
        symbol_type: &str,
        raw_text: &str,
    ) {
        let id = format!("{repo_id}:{file_path}:{symbol_name}");
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![id, repo_id, file_path, language, symbol_name, symbol_type, raw_text],
        )
        .unwrap();
    }

    #[test]
    fn benchmark_repository_choices_are_bounded_and_deterministic() {
        let conn = crate::db::init_db(":memory:").unwrap();
        insert_benchmark_repo(&conn, "repo_c", "/tmp/c");
        insert_benchmark_repo(&conn, "repo_a", "/tmp/a");
        insert_benchmark_repo(&conn, "repo_b", "/tmp/b");

        let choices = benchmark_repository_choices(&conn, 2).unwrap();

        assert_eq!(choices.len(), 2);
        assert_eq!(choices[0].repo_id, "repo_a");
        assert_eq!(choices[0].root_path, "/tmp/a");
        assert_eq!(choices[1].repo_id, "repo_b");
    }

    #[test]
    fn benchmark_symbol_choices_scope_search_filter_and_bound_results() {
        let conn = crate::db::init_db(":memory:").unwrap();
        insert_benchmark_repo(&conn, "repo_a", "/tmp/a");
        insert_benchmark_repo(&conn, "repo_b", "/tmp/b");
        insert_benchmark_node(
            &conn,
            "repo_a",
            "src/foo.py",
            "py",
            "foo",
            "function",
            "def foo(): pass",
        );
        insert_benchmark_node(
            &conn,
            "repo_a",
            "src/foo_utils.ts",
            "ts",
            "helper",
            "function",
            "function helper() {}",
        );
        insert_benchmark_node(
            &conn,
            "repo_a",
            "src/widget.ts",
            "ts",
            "Widget",
            "class",
            "class Widget {}",
        );
        insert_benchmark_node(
            &conn,
            "repo_b",
            "src/foo.py",
            "py",
            "foo",
            "function",
            "def foo(): pass",
        );

        let (choices, limited) =
            benchmark_symbol_choices(&conn, "repo_a", "foo", Some("ts"), Some("function"), 10)
                .unwrap();

        assert!(!limited);
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0].repo_id, "repo_a");
        assert_eq!(choices[0].symbol_name, "helper");
        assert_eq!(choices[0].file_path, "src/foo_utils.ts");

        let (choices, limited) =
            benchmark_symbol_choices(&conn, "repo_a", "", None, None, 2).unwrap();
        assert!(limited);
        assert_eq!(choices.len(), 2);
        assert!(choices.iter().all(|choice| choice.repo_id == "repo_a"));
    }

    #[test]
    fn benchmark_measurement_uses_selected_filepath_for_duplicate_symbols() {
        let root = tempfile::tempdir().unwrap();
        fs::write(
            root.path().join("left.py"),
            "def dupe():\n    return 'left'\n",
        )
        .unwrap();
        fs::write(
            root.path().join("right.py"),
            "def dupe():\n    return 'right'\n",
        )
        .unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        insert_benchmark_repo(&conn, "repo", &root.path().to_string_lossy());
        insert_benchmark_node(
            &conn,
            "repo",
            "left.py",
            "py",
            "dupe",
            "function",
            "def dupe():\n    return 'left'\n",
        );
        insert_benchmark_node(
            &conn,
            "repo",
            "right.py",
            "py",
            "dupe",
            "function",
            "def dupe():\n    return 'right'\n",
        );

        let measurement =
            benchmark_measurement(&conn, "dupe", "repo", Some("right.py"), false).unwrap();

        assert_eq!(measurement.file_path, "right.py");
        assert!(measurement.optimized_text.contains("return 'right'"));
        assert!(!measurement.optimized_text.contains("return 'left'"));
    }

    #[test]
    fn integration_registry_classifies_first_class_secondary_and_compatibility_targets() {
        assert_eq!(
            integration_target_by_name("OpenClaw").map(|target| target.support_tier),
            Some(IntegrationSupportTier::FirstClass)
        );
        assert_eq!(
            integration_target_by_name("Sourcegraph Amp").map(|target| target.support_tier),
            Some(IntegrationSupportTier::Secondary)
        );
        assert_eq!(
            integration_target_by_name("llama.cpp").map(|target| target.support_tier),
            Some(IntegrationSupportTier::CompatibilityOnly)
        );
    }

    #[test]
    fn integration_registry_normalizes_windsurf_and_roo_aliases() {
        assert_eq!(
            integration_target_by_name("codeium windsurf").map(|target| target.name),
            Some("Windsurf")
        );
        assert_eq!(
            integration_target_by_name("roo").map(|target| target.name),
            Some("Roo Code")
        );
        assert_eq!(
            integration_target_by_name(".roomrules").map(|target| target.name),
            Some("Roo Code")
        );
    }

    #[test]
    fn integration_setup_targets_include_direct_and_secondary_but_exclude_compatibility_runtimes() {
        let names: Vec<&str> = integration_setup_targets()
            .into_iter()
            .map(|target| target.name)
            .collect();

        let unique_names: std::collections::BTreeSet<&str> = names.iter().copied().collect();
        assert_eq!(
            names.len(),
            unique_names.len(),
            "integration setup targets should not contain duplicate menu items: {names:?}"
        );

        for name in [
            "Claude Code",
            "Antigravity",
            "Cursor",
            "GitHub Copilot",
            "Cline",
            "Zed",
            "Windsurf",
            "Continue",
            "Roo Code",
            "Goose",
            "OpenHands",
            "OpenClaw",
            "Codex CLI",
            "Gemini CLI",
            "JetBrains AI Assistant",
            "JetBrains Junie",
            "LM Studio",
        ] {
            let target = integration_target_by_name(name).expect("direct target should exist");
            assert!(
                names.contains(&name),
                "{name} should be listed by marrow integrate"
            );
            assert_eq!(target.support_tier, IntegrationSupportTier::FirstClass);
            assert_ne!(target.kind, IntegrationTargetKind::RuntimeBackend);
        }

        for name in ["Kilo Code", "Sourcegraph Amp", "Augment Code"] {
            let target = integration_target_by_name(name).expect("secondary target should exist");
            assert!(
                names.contains(&name),
                "{name} should be surfaced by marrow integrate"
            );
            assert_eq!(target.support_tier, IntegrationSupportTier::Secondary);
            assert_eq!(target.setup_mode, IntegrationSetupMode::Guided);
            assert_eq!(
                format_integration_menu_label(target),
                format!("{} ({})", target.name, integration_skill_directory(target)),
                "{name} label should use the target name and integration directory"
            );
        }

        // Every automatic target must appear before every guided target.
        let targets = integration_setup_targets();
        let last_auto = targets
            .iter()
            .rposition(|t| t.setup_mode == IntegrationSetupMode::Automatic);
        let first_guided = targets
            .iter()
            .position(|t| t.setup_mode == IntegrationSetupMode::Guided);
        if let (Some(last_auto_idx), Some(first_guided_idx)) = (last_auto, first_guided) {
            assert!(
                last_auto_idx < first_guided_idx,
                "all automatic targets must appear before all guided targets in integration_setup_targets()"
            );
        }

        for name in [
            "Ollama",
            "llama.cpp",
            "vLLM",
            "SGLang",
            "LiteLLM",
            "Ramalama",
            "Docker Model Runner",
        ] {
            let target = integration_target_by_name(name).expect("runtime target should exist");
            assert!(
                !names.contains(&name),
                "{name} must not be a direct integrate target"
            );
            assert_eq!(
                target.support_tier,
                IntegrationSupportTier::CompatibilityOnly
            );
            assert_eq!(target.kind, IntegrationTargetKind::RuntimeBackend);
        }
    }

    #[test]
    fn openclaw_is_first_class_guided_self_hosted_host() {
        let target = integration_target_by_name("OpenClaw").expect("OpenClaw target should exist");

        assert_eq!(target.support_tier, IntegrationSupportTier::FirstClass);
        assert_eq!(target.kind, IntegrationTargetKind::Host);
        assert_eq!(target.setup_mode, IntegrationSetupMode::Guided);
        assert!(!target.allow_config_write);
        assert!(target.writer.is_none());
        assert!(format_integration_guidance(target).contains("No config file was written"));
    }

    #[test]
    fn guided_targets_do_not_have_config_writers() {
        for name in [
            "Continue",
            "Codex CLI",
            "Gemini CLI",
            "JetBrains AI Assistant",
            "JetBrains Junie",
            "OpenHands",
            "OpenClaw",
            "LM Studio",
            "Kilo Code",
            "Sourcegraph Amp",
            "Augment Code",
        ] {
            let target = integration_target_by_name(name).expect("target should exist");
            assert_eq!(target.setup_mode, IntegrationSetupMode::Guided);
            assert!(!target.allow_config_write, "{name} must not write config");
            assert!(target.writer.is_none(), "{name} must not have a writer");
        }
    }

    #[test]
    fn guided_registration_does_not_create_config_files() {
        let home = tempfile::tempdir().unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };

        for name in [
            "Continue",
            "Codex CLI",
            "Gemini CLI",
            "JetBrains AI Assistant",
            "JetBrains Junie",
            "OpenHands",
            "OpenClaw",
            "LM Studio",
            "Kilo Code",
            "Sourcegraph Amp",
            "Augment Code",
        ] {
            let target = integration_target_by_name(name).unwrap();
            let outcome = register_integration_target(target, &ctx).unwrap();

            assert_eq!(outcome, AgentOutcome::Guided);
            assert!(
                format_integration_guidance(target).contains("No config file was written"),
                "{name} should provide configuration guidance without claiming a write"
            );
            assert!(
                fs::read_dir(home.path()).unwrap().next().is_none(),
                "guided target {name} must not create config files"
            );
        }
    }

    #[test]
    fn automatic_writer_failure_is_not_downgraded_to_guided_setup() {
        let home_file = tempfile::NamedTempFile::new().unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home_file.path().to_string_lossy().into_owned(),
        };
        let target = integration_target_by_name("Cursor").unwrap();

        let err = register_integration_target(target, &ctx)
            .expect_err("automatic writer failures should remain errors");

        assert!(
            err.to_string().contains("Not a directory")
                || err.to_string().contains("not a directory"),
            "unexpected writer error: {err}"
        );
    }

    #[test]
    fn compatibility_runtime_guidance_reports_backend_status() {
        for name in [
            "Ollama",
            "llama.cpp",
            "vLLM",
            "SGLang",
            "LiteLLM",
            "Ramalama",
            "Docker Model Runner",
        ] {
            let target = integration_target_by_name(name).expect("runtime target should exist");
            let guidance = format_integration_guidance(target);

            assert_eq!(
                target.support_tier,
                IntegrationSupportTier::CompatibilityOnly
            );
            assert!(
                guidance.contains("model/runtime backend"),
                "runtime guidance should identify backend status for {name}: {guidance}"
            );
            assert!(
                guidance.contains("not an MCP agent/client/host destination"),
                "runtime guidance should not claim direct MCP client treatment for {name}: {guidance}"
            );
        }
    }

    #[test]
    fn integration_menu_labels_include_directory_convention() {
        for target in integration_setup_targets() {
            let label = format_integration_menu_label(target);
            assert!(
                label.starts_with(&format!("{} (", target.name)),
                "menu label for {:?} should start with the target name and directory: {label:?}",
                target.name
            );
            assert!(
                label.ends_with(')'),
                "menu label for {:?} should end with a closing parenthesis: {label:?}",
                target.name
            );
        }

        assert_eq!(
            format_integration_menu_label(integration_target_by_name("Cursor").unwrap()),
            "Cursor (.agents/skills)"
        );
        assert_eq!(
            format_integration_menu_label(integration_target_by_name("Continue").unwrap()),
            "Continue (.continue/skills)"
        );
        assert_eq!(
            format_integration_menu_label(integration_target_by_name("Sourcegraph Amp").unwrap()),
            "Sourcegraph Amp (.agents/skills)"
        );
    }

    #[test]
    fn integration_menu_labels_contain_no_legacy_taxonomy_or_file_suffixes() {
        let forbidden = [
            "guided MCP client",
            "guided agent",
            "guided host",
            "secondary, guided setup",
            "manual setup",
            "marrow-optimization.md",
            "->",
        ];

        for target in integration_setup_targets() {
            let label = format_integration_menu_label(target);

            for substr in forbidden {
                assert!(
                    !label.contains(substr),
                    "menu label for {:?} must not contain {:?}: {label:?}",
                    target.name,
                    substr
                );
            }

            // Secondary targets must not expose their tier name in the label
            if target.support_tier == IntegrationSupportTier::Secondary {
                assert!(
                    !label.to_lowercase().contains("secondary"),
                    "secondary target {:?} must not expose tier name in label: {label:?}",
                    target.name
                );
            }

            // Labels must be plain text — no ANSI escape sequences
            assert!(
                !label.contains('\x1b'),
                "menu label for {:?} must contain no ANSI control sequences: {label:?}",
                target.name
            );
        }
    }

    #[test]
    fn integration_menu_universal_group_preserves_upstream_order() {
        let labels = universal_agent_menu_labels();
        let expected: Vec<String> = UNIVERSAL_AGENTS
            .iter()
            .map(|name| format!("{name} (.agents/skills)"))
            .collect();

        assert_eq!(labels, expected);
        assert_eq!(UNIVERSAL_GROUP_LABEL, "Universal (.agents/skills)");
        assert_eq!(ADDITIONAL_AGENTS_LABEL, "Additional agents");

        for target in AGENT_SKILL_TARGETS {
            assert_ne!(
                format_skill_menu_label(target),
                format_menu_label(target.name, UNIVERSAL_SKILLS_DIR),
                "non-standard skill target '{}' must stay out of the universal block",
                target.name
            );
        }
    }

    #[test]
    fn interactive_mcp_menu_places_universal_labels_before_non_standard_labels() {
        let ordered_labels: Vec<String> = interactive_mcp_targets()
            .into_iter()
            .map(format_integration_menu_label)
            .collect();

        let first_non_standard = ordered_labels
            .iter()
            .position(|label| !label.ends_with("(.agents/skills)"))
            .expect("expected at least one non-standard MCP label");

        assert!(
            ordered_labels[..first_non_standard]
                .iter()
                .all(|label| label.ends_with("(.agents/skills)")),
            "universal .agents/skills labels must stay before non-standard MCP labels: {ordered_labels:?}"
        );
        assert!(
            ordered_labels[first_non_standard..]
                .iter()
                .all(|label| !label.ends_with("(.agents/skills)")),
            "non-standard MCP labels must only appear after the universal block: {ordered_labels:?}"
        );
    }

    #[test]
    fn additional_agents_section_contains_non_standard_mcp_labels() {
        let additional_labels: Vec<String> = interactive_additional_mcp_targets()
            .into_iter()
            .map(format_integration_menu_label)
            .collect();

        assert!(
            additional_labels.contains(&"Continue (.continue/skills)".to_string()),
            "non-standard MCP labels should appear in Additional agents: {additional_labels:?}"
        );
        assert!(
            additional_labels.contains(&"Windsurf (.windsurf/skills)".to_string()),
            "overlapping non-standard MCP labels should appear in Additional agents: {additional_labels:?}"
        );
        assert!(
            additional_labels
                .iter()
                .all(|label| !label.ends_with("(.agents/skills)")),
            "Additional agents should only contain non-standard MCP labels: {additional_labels:?}"
        );
    }

    #[test]
    fn additional_agents_prompt_contains_only_skill_only_targets() {
        let skill_only_labels: Vec<String> = interactive_skill_only_agent_target_indices()
            .into_iter()
            .map(|idx| format_skill_menu_label(&AGENT_SKILL_TARGETS[idx]))
            .collect();

        assert!(
            skill_only_labels.contains(&"AiderDesk (.aider-desk/skills)".to_string()),
            "skill-only agents should remain in the additional agents prompt: {skill_only_labels:?}"
        );
        assert!(
            skill_only_labels.contains(&"Trae (.trae/skills)".to_string()),
            "non-MCP agent skills should remain selectable: {skill_only_labels:?}"
        );
        assert!(
            !skill_only_labels.contains(&"Continue (.continue/skills)".to_string()),
            "MCP-capable overlap targets must not repeat in the additional agents prompt: {skill_only_labels:?}"
        );
        assert!(
            !skill_only_labels.contains(&"Windsurf (.windsurf/skills)".to_string()),
            "MCP-capable overlap targets must stay in the unified MCP prompt: {skill_only_labels:?}"
        );
    }

    #[test]
    fn unified_agent_menu_orders_universal_first_then_additional() {
        let (universal_entries, universal_labels) = universal_agent_menu();
        let (additional_entries, additional_labels) = additional_agent_menu();

        assert_eq!(
            universal_entries.len(),
            universal_labels.len(),
            "universal menu should have matching entry and label counts"
        );
        assert_eq!(
            additional_entries.len(),
            additional_labels.len(),
            "additional menu should have matching entry and label counts"
        );
        assert!(
            !universal_entries.is_empty(),
            "universal menu should not be empty"
        );
        assert!(
            !additional_entries.is_empty(),
            "additional menu should not be empty"
        );

        // Universal labels are just the agent name — no config suffix
        assert_eq!(
            universal_labels.len(),
            UNIVERSAL_AGENTS.len(),
            "universal menu should have one entry per UNIVERSAL_AGENTS"
        );
        assert!(
            universal_labels.contains(&"Amp".to_string()),
            "universal menu should list agents by name only: {universal_labels:?}"
        );
        assert!(
            universal_labels.contains(&"Deep Agents".to_string()),
            "universal menu should include universal-only agents by name: {universal_labels:?}"
        );
        for label in &universal_labels {
            assert!(
                !label.contains('('),
                "universal agent labels must not contain a config suffix: {label}"
            );
        }

        let mut has_universal_no_mcp = false;
        let mut has_universal_mcp = false;
        for entry in &universal_entries {
            match entry {
                AgentMenuEntry::Mcp(_) => has_universal_mcp = true,
                AgentMenuEntry::UniversalNoMcpTarget => has_universal_no_mcp = true,
                AgentMenuEntry::SkillOnly(_) => {
                    panic!("universal menu should not contain skill-only entries")
                }
            }
        }
        assert!(
            has_universal_mcp,
            "universal menu should contain at least one MCP target"
        );
        assert!(
            has_universal_no_mcp,
            "universal menu should contain at least one universal-only entry"
        );

        // Additional labels include the config suffix
        assert!(
            additional_labels.contains(&"AiderDesk (.aider-desk/skills)".to_string()),
            "additional menu should include skill-only entries with config suffix: {additional_labels:?}"
        );
        assert!(
            additional_entries
                .iter()
                .any(|e| matches!(e, AgentMenuEntry::SkillOnly(_))),
            "additional menu should contain skill-only entries like AiderDesk"
        );
    }

    #[test]
    fn unified_agent_menu_partition_handles_universal_only_empty_and_mixed_selections() {
        let (universal_entries, universal_labels) = universal_agent_menu();
        let (additional_entries, additional_labels) = additional_agent_menu();

        let universal_only_idx = universal_labels
            .iter()
            .position(|label| label == "Deep Agents")
            .expect("Deep Agents should be selectable as a universal no-MCP target");
        let selected = vec![&universal_entries[universal_only_idx]];
        let (mcp, skill, has_universal_no_mcp_target) = partition_agent_menu_entries(&selected);
        assert!(
            mcp.is_empty(),
            "universal-only selection should not add MCP targets"
        );
        assert!(
            skill.is_empty(),
            "universal-only selection should not add skill-only targets"
        );
        assert!(
            has_universal_no_mcp_target,
            "universal-only selection should prevent the no-selection early return"
        );

        let empty_selection: Vec<&AgentMenuEntry> = Vec::new();
        let (mcp, skill, has_universal_no_mcp_target) =
            partition_agent_menu_entries(&empty_selection);
        assert!(mcp.is_empty());
        assert!(skill.is_empty());
        assert!(!has_universal_no_mcp_target);

        let continue_idx = additional_labels
            .iter()
            .position(|label| label == "Continue (.continue/skills)")
            .expect("Continue should be selectable as an additional MCP target");
        let aider_idx = additional_labels
            .iter()
            .position(|label| label == "AiderDesk (.aider-desk/skills)")
            .expect("AiderDesk should be selectable as an additional skill-only target");
        let selected = vec![
            &additional_entries[continue_idx],
            &additional_entries[aider_idx],
        ];
        let (mcp, skill, has_universal_no_mcp_target) = partition_agent_menu_entries(&selected);
        assert_eq!(mcp.len(), 1, "mixed selection should preserve MCP targets");
        assert_eq!(mcp[0].name, "Continue");
        assert_eq!(
            skill.len(),
            1,
            "mixed selection should preserve skill-only targets"
        );
        assert_eq!(AGENT_SKILL_TARGETS[skill[0]].name, "AiderDesk");
        assert!(!has_universal_no_mcp_target);
    }

    #[test]
    fn cmd_integrate_renders_universal_and_additional_prompts_in_order() {
        let source = include_str!("main.rs");
        let cmd_start = source
            .rfind("\nfn cmd_integrate(args: &[String]) -> Result<()> {")
            .map(|idx| idx + 1)
            .expect("cmd_integrate should exist");
        let cmd_end = source[cmd_start..]
            .find("\nfn cmd_validate() -> Result<()> {")
            .map(|idx| cmd_start + idx)
            .expect("cmd_integrate should end before cmd_validate");
        let cmd_source = &source[cmd_start..cmd_end];

        assert_eq!(UNIVERSAL_GROUP_LABEL, "Universal (.agents/skills)");
        assert_eq!(ADDITIONAL_AGENTS_LABEL, "Additional agents");

        let count_occurrences = |needle: &str| cmd_source.match_indices(needle).count();

        let direct_branch_idx = cmd_source
            .find("if !args.is_empty()")
            .expect("cmd_integrate should preserve direct-argument branch");
        let universal_menu_idx = cmd_source
            .find("let (universal_entries, universal_labels) = universal_agent_menu();")
            .expect("cmd_integrate should build the universal agent menu");
        assert!(
            direct_branch_idx < universal_menu_idx,
            "direct-argument branch should remain before interactive prompting"
        );
        assert!(
            cmd_source[..universal_menu_idx].contains("combined_target_lookup(&name)"),
            "direct-argument branch should continue to use combined target lookup"
        );

        // Verify both section menus are built
        assert!(
            cmd_source.contains("universal_agent_menu()"),
            "cmd_integrate should build the universal agent menu"
        );
        assert!(
            cmd_source.contains("additional_agent_menu()"),
            "cmd_integrate should build the additional agent menu"
        );

        // Verify two MultiSelect prompts — one per section
        assert_eq!(
            count_occurrences("inquire::MultiSelect::new("),
            2,
            "cmd_integrate should have one MultiSelect per section (universal + additional)"
        );

        // Verify section labels are used as prompt messages
        assert!(
            cmd_source.contains("UNIVERSAL_GROUP_LABEL,"),
            "cmd_integrate should use UNIVERSAL_GROUP_LABEL as the universal prompt message"
        );
        assert!(
            cmd_source.contains("ADDITIONAL_AGENTS_LABEL,"),
            "cmd_integrate should use ADDITIONAL_AGENTS_LABEL as the additional prompt message"
        );

        assert!(
            cmd_source.contains("Interactive prompts require a terminal. Use 'marrow integrate <name>' for non-interactive installs."),
            "cmd_integrate should preserve non-TTY direct-argument guidance"
        );

        // Verify no static preview sections
        assert_eq!(
            count_occurrences("style(UNIVERSAL_GROUP_LABEL).bold()"),
            0,
            "cmd_integrate should not print static Universal group preview"
        );
        assert_eq!(
            count_occurrences("style(ADDITIONAL_AGENTS_LABEL).bold()"),
            0,
            "cmd_integrate should not print static Additional agents preview"
        );

        assert!(
            cmd_source.contains("partition_agent_menu_entries(&selected_entries)"),
            "cmd_integrate should partition selected menu entries through the shared helper"
        );

        // Verify low-level helpers are not called directly in the main flow
        assert_eq!(
            count_occurrences("interactive_mcp_targets()"),
            0,
            "cmd_integrate should use the menu helpers instead of interactive_mcp_targets()"
        );
        assert_eq!(
            count_occurrences("interactive_skill_only_agent_target_indices()"),
            0,
            "cmd_integrate should use the menu helpers instead of interactive_skill_only_agent_target_indices()"
        );
    }

    #[test]
    fn cmd_integrate_direct_mcp_branch_installs_rule_file_after_mcp_success() {
        let source = include_str!("main.rs");
        let cmd_start = source
            .rfind("\nfn cmd_integrate(args: &[String]) -> Result<()> {")
            .map(|idx| idx + 1)
            .expect("cmd_integrate should exist");
        let cmd_end = source[cmd_start..]
            .find("\nfn cmd_validate() -> Result<()> {")
            .map(|idx| cmd_start + idx)
            .expect("cmd_integrate should end before cmd_validate");
        let cmd_source = &source[cmd_start..cmd_end];
        let direct_start = cmd_source
            .find("if !args.is_empty()")
            .expect("cmd_integrate should preserve direct-argument branch");
        let direct_end = cmd_source[direct_start..]
            .find("\n        return Ok(());\n    }")
            .map(|idx| direct_start + idx)
            .expect("direct-argument branch should return before interactive prompting");
        let direct_source = &cmd_source[direct_start..direct_end];

        let mcp_registration_idx = direct_source
            .find("let mcp_result = register_integration_target(target, &ctx);")
            .expect("direct branch should register MCP targets");
        let success_guard_idx = direct_source
            .find("Ok(AgentOutcome::Installed) | Ok(AgentOutcome::Guided)")
            .expect("direct branch should gate rule install on successful or guided MCP setup");
        let rule_agent_idx = direct_source
            .find("rule_agent_for_scope(target, scope)")
            .expect("direct branch should use selected scope to resolve rule agent");
        let install_rule_idx = direct_source
            .find("skills::install_skill(skill_agent, scope, method, &home_path)")
            .expect("direct branch should install the selected MCP target rule file");
        let status_line_idx = direct_source
            .find("format_rule_install_status_line(")
            .expect("direct branch should use shared rule status formatting");
        let source_description_idx = direct_source
            .find("skills::install_source_description(method, &home_path)")
            .expect("direct branch should report the selected install source");

        assert!(
            mcp_registration_idx < success_guard_idx,
            "rule install guard should run after MCP registration"
        );
        assert!(
            success_guard_idx < rule_agent_idx,
            "rule target lookup should be inside the MCP success/guided guard"
        );
        assert!(
            rule_agent_idx < install_rule_idx,
            "rule file install should use the resolved agent"
        );
        assert!(
            install_rule_idx < status_line_idx,
            "rule install result should flow through shared status formatting"
        );
        assert!(
            status_line_idx < source_description_idx,
            "rule install output should include the install source description"
        );
    }

    // ── Agent skill target tests ──────────────────────────────────────────────

    #[test]
    fn agent_skill_targets_minimum_count() {
        assert!(
            AGENT_SKILL_TARGETS.len() >= 40,
            "expected >= 40 agent skill targets, got {}",
            AGENT_SKILL_TARGETS.len()
        );
    }

    #[test]
    fn agent_skill_targets_have_unique_names() {
        let mut seen_names = std::collections::HashSet::new();
        let mut seen_aliases = std::collections::HashSet::new();
        for target in AGENT_SKILL_TARGETS {
            assert!(
                seen_names.insert(target.name),
                "duplicate AgentSkillTarget name: {}",
                target.name
            );
            for alias in target.aliases {
                assert!(
                    seen_aliases.insert(*alias),
                    "duplicate AgentSkillTarget alias '{}' in '{}'",
                    alias,
                    target.name
                );
            }
        }
        // Intentional overlap is allowed for these agents that appear in both
        // INTEGRATION_TARGETS (MCP config) and AGENT_SKILL_TARGETS (skill path).
        let overlap_allowed: std::collections::HashSet<&str> =
            ["claudecode", "roocode", "windsurf"].into_iter().collect();
        for target in AGENT_SKILL_TARGETS {
            let normalized = normalize_integration_name(target.name);
            if overlap_allowed.contains(normalized.as_str()) {
                continue;
            }
            for it in INTEGRATION_TARGETS {
                if it.rule_agent.is_some() && normalize_integration_name(it.name) == normalized {
                    panic!(
                        "AgentSkillTarget '{}' overlaps IntegrationTarget '{}' which has rule_agent",
                        target.name, it.name
                    );
                }
            }
        }
    }

    #[test]
    fn agent_skill_targets_have_nonempty_skills_dir() {
        for target in AGENT_SKILL_TARGETS {
            assert!(
                !target.skills_dir.is_empty(),
                "AgentSkillTarget '{}' has empty skills_dir",
                target.name
            );
            assert!(
                !target.skills_dir.starts_with('/'),
                "AgentSkillTarget '{}' skills_dir must be relative: {}",
                target.name,
                target.skills_dir
            );
        }
    }

    #[test]
    fn agent_skill_targets_no_universal_dup() {
        for target in AGENT_SKILL_TARGETS {
            assert_ne!(
                target.skills_dir, ".agents/skills",
                "AgentSkillTarget '{}' must not duplicate the universal path",
                target.name
            );
        }
    }

    #[test]
    fn universal_agents_contains_expected_visible_upstream_entries() {
        assert_eq!(
            UNIVERSAL_AGENTS.len(),
            13,
            "UNIVERSAL_AGENTS should have exactly 13 visible upstream universal agents"
        );

        let expected = [
            "Amp",
            "Antigravity",
            "Cline",
            "Codex",
            "Cursor",
            "Deep Agents",
            "Dexto",
            "Firebender",
            "Gemini CLI",
            "GitHub Copilot",
            "Kimi Code CLI",
            "OpenCode",
            "Warp",
        ];
        for name in expected {
            assert!(
                UNIVERSAL_AGENTS.contains(&name),
                "UNIVERSAL_AGENTS should contain {name}"
            );
        }
    }

    #[test]
    fn universal_agents_excludes_hidden_entries() {
        for hidden in ["Replit", "Universal"] {
            assert!(
                !UNIVERSAL_AGENTS.contains(&hidden),
                "UNIVERSAL_AGENTS must not include hidden entry {hidden}"
            );
        }
    }

    #[test]
    fn agent_skill_target_by_name_finds_entries() {
        assert!(
            agent_skill_target_by_name("AiderDesk").is_some(),
            "should find AiderDesk by name"
        );
        assert!(
            agent_skill_target_by_name("aider-desk").is_some(),
            "should find AiderDesk by alias"
        );
    }

    #[test]
    fn agent_skill_target_by_name_returns_none_for_unknown() {
        assert!(agent_skill_target_by_name("Nonexistent Agent").is_none());
    }

    #[test]
    fn agent_skill_target_exhaustive_fields() {
        // Destructure without `..` so adding a new field to AgentSkillTarget
        // causes a compile error here, forcing the test to be updated.
        let AgentSkillTarget {
            name,
            aliases,
            skills_dir,
            scope_support,
        } = &AGENT_SKILL_TARGETS[0];
        assert!(!name.is_empty());
        assert!(!aliases.is_empty());
        assert!(!skills_dir.is_empty());
        // scope_support must be a valid variant (this binds it, ensuring the field exists).
        let _ = scope_support.supports(skills::Scope::Project);
    }

    #[test]
    fn combined_target_lookup_finds_skill_only() {
        let (mcp, skill) = combined_target_lookup("AiderDesk");
        assert!(mcp.is_none(), "AiderDesk should not be an MCP target");
        assert!(skill.is_some(), "AiderDesk should be a skill target");
    }

    #[test]
    fn combined_target_lookup_finds_mcp_only() {
        let (mcp, skill) = combined_target_lookup("Cursor");
        assert!(mcp.is_some(), "Cursor should be an MCP target");
        assert!(skill.is_none(), "Cursor should not be a skill target");
    }

    #[test]
    fn combined_target_lookup_finds_overlap() {
        let (mcp, skill) = combined_target_lookup("Goose");
        assert!(mcp.is_some(), "Goose should be an MCP target");
        assert!(skill.is_some(), "Goose should be a skill target");
    }

    #[test]
    fn combined_target_lookup_claude_code_dual_hit() {
        let (mcp, skill) = combined_target_lookup("Claude Code");
        assert!(mcp.is_some(), "Claude Code should be an MCP target");
        assert!(skill.is_some(), "Claude Code should also be a skill target");
        assert_eq!(skill.unwrap().skills_dir, ".claude/skills");
    }

    #[test]
    fn combined_target_lookup_roo_code_dual_hit() {
        let (mcp, skill) = combined_target_lookup("Roo Code");
        assert!(mcp.is_some(), "Roo Code should be an MCP target");
        assert!(skill.is_some(), "Roo Code should also be a skill target");
        assert_eq!(skill.unwrap().skills_dir, ".roo/skills");
    }

    #[test]
    fn combined_target_lookup_windsurf_dual_hit() {
        let (mcp, skill) = combined_target_lookup("Windsurf");
        assert!(mcp.is_some(), "Windsurf should be an MCP target");
        assert!(skill.is_some(), "Windsurf should also be a skill target");
        assert_eq!(skill.unwrap().skills_dir, ".windsurf/skills");
    }

    #[test]
    fn trae_and_trae_cn_are_distinct_entries_with_shared_path() {
        let trae = agent_skill_target_by_name("Trae");
        let trae_cn = agent_skill_target_by_name("Trae CN");
        assert!(trae.is_some(), "Trae should exist");
        assert!(trae_cn.is_some(), "Trae CN should exist");
        let trae = trae.unwrap();
        let trae_cn = trae_cn.unwrap();
        assert_ne!(trae.name, trae_cn.name, "names must differ");
        assert_eq!(
            trae.skills_dir, trae_cn.skills_dir,
            "Trae and Trae CN should share .trae/skills"
        );
        assert_eq!(trae.skills_dir, ".trae/skills");
    }

    #[test]
    fn new_upstream_targets_resolve_correctly() {
        let cases = [
            ("Rovo Dev", ".rovodev/skills"),
            ("Zencoder", ".zencoder/skills"),
            ("Neovate", ".neovate/skills"),
            ("Pochi", ".pochi/skills"),
            ("AdaL", ".adal/skills"),
        ];
        for (name, expected_dir) in cases {
            let target = agent_skill_target_by_name(name);
            assert!(
                target.is_some(),
                "{name} should be found in AGENT_SKILL_TARGETS"
            );
            assert_eq!(target.unwrap().skills_dir, expected_dir);
        }
    }

    #[test]
    fn integration_menu_skill_labels_include_directory_convention() {
        for target in AGENT_SKILL_TARGETS {
            let label = format_skill_menu_label(target);
            assert!(
                label.starts_with(&format!("{} (", target.name)),
                "skill label for '{}' must start with the display name and directory: {label:?}",
                target.name
            );
            assert!(
                label.ends_with(')'),
                "skill label for '{}' must end with a closing parenthesis: {label:?}",
                target.name
            );
        }
    }

    #[test]
    fn integration_menu_skill_labels_contain_no_taxonomy_or_file_suffixes() {
        let forbidden = [
            "guided MCP client",
            "guided agent",
            "guided host",
            "secondary, guided setup",
            "manual setup",
            "marrow-optimization.md",
            "->",
        ];

        for target in AGENT_SKILL_TARGETS {
            let label = format_skill_menu_label(target);
            for substr in forbidden {
                assert!(
                    !label.contains(substr),
                    "skill label for {:?} must not contain {:?}: {label:?}",
                    target.name,
                    substr
                );
            }
            assert!(
                !label.contains('\x1b'),
                "skill label for {:?} must contain no ANSI control sequences: {label:?}",
                target.name
            );
        }
    }

    #[test]
    fn universal_skill_path_is_agents_skills() {
        let path = std::path::PathBuf::from(".agents/skills").join("marrow-optimization.md");
        assert_eq!(
            path,
            std::path::PathBuf::from(".agents/skills/marrow-optimization.md")
        );
    }

    #[test]
    fn windsurf_and_roo_rule_files_are_first_class_coverage_evidence() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join(".windsurfrules"), "marrow").unwrap();
        fs::write(workspace.path().join(".roomrules"), "marrow").unwrap();

        let summary = format_agent_coverage_summary(workspace.path(), home.path());

        assert!(
            summary.contains("Windsurf: protected"),
            "Windsurf should be protected through registry-backed coverage: {summary}"
        );
        assert!(
            summary.contains("Roo Code: protected"),
            "Roo Code should be protected through registry-backed coverage: {summary}"
        );
        assert!(
            !summary.contains("legacy") && !summary.contains("Windsurf: partial"),
            "legacy partial Windsurf line should be gone: {summary}"
        );
        assert_eq!(summary.matches("Windsurf:").count(), 1, "{summary}");
        assert_eq!(summary.matches("Roo Code:").count(), 1, "{summary}");
    }

    #[test]
    fn workspace_initialization_uses_registry_baseline_rule_files() {
        let workspace = tempfile::tempdir().unwrap();
        fs::create_dir_all(workspace.path().join(".marrow")).unwrap();
        fs::create_dir_all(workspace.path().join(".vscode")).unwrap();
        fs::write(workspace.path().join(".marrowrc.json"), "{}").unwrap();
        fs::write(workspace.path().join(".vscode/mcp.json"), "{}").unwrap();

        assert!(
            !workspace_is_initialized(workspace.path()),
            "baseline workspace rule files should still be required"
        );

        for rule in baseline_workspace_rule_files() {
            fs::write(workspace.path().join(rule), "MARROW AST CONTEXT ENGINE").unwrap();
        }

        assert!(workspace_is_initialized(workspace.path()));
    }

    #[test]
    fn docs_separate_direct_targets_from_compatibility_only_backends() {
        let docs = [("README", include_str!("../README.md"))];

        for (label, doc) in docs {
            for direct_target in [
                "Windsurf",
                "Continue",
                "Roo Code",
                "Goose",
                "OpenHands",
                "OpenClaw",
                "Codex CLI",
                "Gemini CLI",
                "JetBrains AI Assistant",
                "JetBrains Junie",
                "LM Studio",
            ] {
                assert!(
                    doc.contains(direct_target),
                    "{label} missing {direct_target}"
                );
            }

            for secondary_target in ["Kilo Code", "Sourcegraph Amp", "Augment Code"] {
                assert!(
                    doc.contains(secondary_target),
                    "{label} missing {secondary_target}"
                );
            }

            for runtime in [
                "Ollama",
                "llama.cpp",
                "vLLM",
                "SGLang",
                "LiteLLM",
                "Ramalama",
                "Docker Model Runner",
            ] {
                assert!(doc.contains(runtime), "{label} missing {runtime}");
            }

            assert!(
                doc.contains("Compatibility-only") || doc.contains("compatibility-only"),
                "{label} should identify compatibility-only model/runtime backends"
            );
        }
    }

    #[test]
    fn github_copilot_workspace_mcp_config_counts_as_fallback_coverage() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        fs::create_dir_all(workspace.path().join(".vscode")).unwrap();
        fs::write(
            workspace.path().join(".vscode/mcp.json"),
            r#"{"servers":{"marrow":{}}}"#,
        )
        .unwrap();

        let (status, detail) =
            coverage_status_for_agent(skills::Agent::GitHubCopilot, workspace.path(), home.path());

        assert_eq!(status, "partial", "Copilot MCP fallback detail: {detail}");
        assert!(
            detail.contains(".vscode/mcp.json"),
            "Copilot fallback should cite workspace MCP config: {detail}"
        );
    }

    #[test]
    fn antigravity_roomrules_counts_as_fallback_coverage() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join(".roomrules"), "marrow").unwrap();

        let (status, detail) =
            coverage_status_for_agent(skills::Agent::Antigravity, workspace.path(), home.path());

        assert_eq!(status, "partial", "Antigravity fallback detail: {detail}");
        assert!(
            detail.contains(".roomrules"),
            "Antigravity fallback should cite .roomrules: {detail}"
        );
    }

    #[test]
    fn write_workspace_rules_preserves_legacy_numeric_indices() {
        let workspace = tempfile::tempdir().unwrap();
        let modified = write_workspace_rules(
            workspace.path(),
            &[0, 1, 2],
            WORKSPACE_RULES_CONTENT_SOFT,
            WriteMode::SafeAppend,
        )
        .unwrap();

        for filename in [
            ".cursorrules",
            ".windsurfrules",
            ".clinerules",
            ".roomrules",
        ] {
            let path = workspace.path().join(filename);
            assert!(path.exists(), "missing legacy rule file {filename}");
            let content = fs::read_to_string(path).unwrap();
            assert_soft_workspace_guidance(&content);
        }
        assert_eq!(modified.len(), 4, "legacy indices should write four files");
    }

    #[test]
    fn mcp_guidance_text_balances_structural_and_native_tool_use() {
        let engine = ContextEngine::new(":memory:", reqwest::Client::new()).unwrap();
        let info = <ContextEngine as ServerHandler>::get_info(&engine);
        let instructions = info.instructions.unwrap_or_default();

        assert!(
            instructions.contains("structural questions"),
            "{instructions}"
        );
        assert!(instructions.contains("blast radius"), "{instructions}");
        assert!(instructions.contains("repo maps"), "{instructions}");
        assert!(instructions.contains("explore_batch"), "{instructions}");
        assert!(instructions.contains("dependency_graph"), "{instructions}");
        assert!(instructions.contains("map_class"), "{instructions}");
        assert!(
            instructions.contains("native read/search"),
            "{instructions}"
        );
        assert!(instructions.contains("single-file"), "{instructions}");
        assert!(instructions.contains("line-level"), "{instructions}");
        assert!(instructions.contains("config/docs"), "{instructions}");

        let source = include_str!("main.rs");
        assert!(
            source.contains(
                "Use for code structure, dependencies, refactor blast radius, or codebase maps"
            ),
            "run_pipeline tool description should recommend structural use"
        );
        assert!(
            source.contains(
                "Skip for single-file lookups, line-level reads, and grep-style text search"
            ),
            "run_pipeline tool description should allow native narrow lookups"
        );
        assert!(
            source.contains("Use `find_symbol` to discover symbol names"),
            "run_pipeline tool description should document find_symbol discovery"
        );
        assert!(
            source.contains("Use 'explore_batch' for 3+ related"),
            "run_pipeline tool description should document batch exploration"
        );
        assert!(
            source.contains("'dependency_graph' for multi-hop"),
            "run_pipeline tool description should document dependency graph"
        );
        assert!(
            source.contains("'map_class' for"),
            "run_pipeline tool description should document class maps"
        );
    }

    #[test]
    fn run_pipeline_schema_documents_find_symbol_kind_and_limit() {
        let schema = ContextEngine::run_pipeline_schema();
        let properties = schema
            .get("properties")
            .and_then(|value| value.as_object())
            .expect("schema properties should be an object");

        let intent_enum = properties["intent"]["enum"]
            .as_array()
            .expect("intent enum should be present");
        assert!(
            intent_enum.iter().any(|value| value == "find_symbol"),
            "valid intent text should include find_symbol: {intent_enum:?}"
        );

        let kind_description = properties["kind"]["description"]
            .as_str()
            .expect("kind description should be present");
        assert!(
            kind_description.contains("find_symbol"),
            "kind should be documented as find_symbol-specific: {kind_description}"
        );

        let limit = properties
            .get("limit")
            .expect("run_pipeline schema should expose limit");
        assert_eq!(limit["type"], "integer");
        assert_eq!(limit["minimum"], 1);
        assert_eq!(
            limit["default"].as_u64(),
            Some(retrieval::FIND_SYMBOL_DEFAULT_LIMIT as u64)
        );
        assert!(
            limit["description"]
                .as_str()
                .unwrap_or_default()
                .contains("find_symbol"),
            "limit description should mention find_symbol: {limit:?}"
        );
    }

    #[test]
    fn run_pipeline_schema_documents_compound_intents() {
        let schema = ContextEngine::run_pipeline_schema();
        let properties = schema
            .get("properties")
            .and_then(|value| value.as_object())
            .expect("schema properties should be an object");

        let intent_enum = properties["intent"]["enum"]
            .as_array()
            .expect("intent enum should be present");
        for expected in ["explore_batch", "dependency_graph", "map_class"] {
            assert!(
                intent_enum.iter().any(|value| value == expected),
                "intent enum missing {expected}: {intent_enum:?}"
            );
        }

        for expected_property in [
            "queries",
            "depth",
            "direction",
            "include_source",
            "max_nodes",
        ] {
            assert!(
                properties.contains_key(expected_property),
                "schema missing {expected_property}: {properties:?}"
            );
        }
    }

    #[test]
    fn run_pipeline_invalid_intent_text_lists_find_symbol() {
        let message = run_pipeline_invalid_intent_message();
        assert!(
            message.contains("'find_symbol'"),
            "invalid intent text should list find_symbol: {message}"
        );
        for expected in ["'explore_batch'", "'dependency_graph'", "'map_class'"] {
            assert!(
                message.contains(expected),
                "invalid intent text should list {expected}: {message}"
            );
        }
    }

    fn parse_batch_error(payload: serde_json::Value) -> String {
        let args = payload.as_object().unwrap().clone();
        parse_batch_queries(&args).unwrap_err().message.to_string()
    }

    #[test]
    fn explore_batch_parser_validates_query_shape_and_count() {
        let missing_queries = parse_batch_error(json!({}));
        assert!(
            missing_queries.contains("requires a `queries` array"),
            "unexpected missing queries error: {missing_queries}"
        );

        let empty_queries = parse_batch_error(json!({ "queries": [] }));
        assert!(
            empty_queries.contains("requires 1 to 20 queries"),
            "unexpected empty queries error: {empty_queries}"
        );

        let too_many_queries = (0..21)
            .map(|idx| json!({ "intent": "explore_symbol", "target": format!("Symbol{idx}") }))
            .collect::<Vec<_>>();
        let too_many = parse_batch_error(json!({ "queries": too_many_queries }));
        assert!(
            too_many.contains("requires 1 to 20 queries"),
            "unexpected too many queries error: {too_many}"
        );

        let missing_intent = parse_batch_error(json!({
            "queries": [{ "target": "Widget" }]
        }));
        assert!(
            missing_intent.contains("query 1 requires `intent`"),
            "unexpected missing intent error: {missing_intent}"
        );

        let invalid_intent = parse_batch_error(json!({
            "queries": [{ "intent": "map_class", "target": "Widget" }]
        }));
        assert!(
            invalid_intent.contains("query 1 has invalid intent `map_class`"),
            "unexpected invalid intent error: {invalid_intent}"
        );

        let missing_target = parse_batch_error(json!({
            "queries": [{ "intent": "explore_symbol" }]
        }));
        assert!(
            missing_target.contains("query 1 requires `target`"),
            "unexpected missing target error: {missing_target}"
        );
    }

    #[test]
    fn explore_batch_parser_normalizes_aliases_and_graph_options() {
        let args = json!({
            "queries": [
                { "intent": "capsule", "target": "Widget" },
                { "intent": "analyze_impact", "target": "Widget" },
                {
                    "intent": "dependency_graph",
                    "target": "Widget",
                    "depth": 3,
                    "direction": "callees",
                    "include_source": true,
                    "max_nodes": 7
                }
            ]
        })
        .as_object()
        .unwrap()
        .clone();

        let queries = parse_batch_queries(&args).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0].intent, retrieval::BatchIntent::ExploreSymbol);
        assert_eq!(queries[1].intent, retrieval::BatchIntent::RefactorSymbol);
        assert_eq!(queries[2].intent, retrieval::BatchIntent::DependencyGraph);
        assert_eq!(queries[2].depth, Some(3));
        assert_eq!(
            queries[2].direction,
            Some(retrieval::DependencyDirection::Callees)
        );
        assert!(queries[2].include_source);
        assert_eq!(queries[2].max_nodes, Some(7));
    }

    fn parse_graph_options_error(payload: serde_json::Value) -> String {
        let args = payload.as_object().unwrap().clone();
        parse_dependency_graph_options(&args)
            .unwrap_err()
            .message
            .to_string()
    }

    #[test]
    fn dependency_graph_options_validate_bounds_and_types() {
        let valid = json!({
            "depth": 5,
            "direction": "callers",
            "include_source": true,
            "max_nodes": 3
        })
        .as_object()
        .unwrap()
        .clone();
        let options = parse_dependency_graph_options(&valid).unwrap();
        assert_eq!(options.depth, 5);
        assert_eq!(options.direction, retrieval::DependencyDirection::Callers);
        assert!(options.include_source);
        assert_eq!(options.max_nodes, 3);

        for (payload, expected) in [
            (json!({ "depth": 0 }), "`depth` must be a positive integer"),
            (json!({ "depth": 6 }), "`depth` must be between 1 and 5"),
            (json!({ "direction": 7 }), "`direction` must be a string"),
            (
                json!({ "direction": "sideways" }),
                "expected callers, callees, or both",
            ),
            (
                json!({ "include_source": "true" }),
                "`include_source` must be a boolean",
            ),
            (
                json!({ "max_nodes": 0 }),
                "`max_nodes` must be a positive integer",
            ),
        ] {
            let message = parse_graph_options_error(payload);
            assert!(
                message.contains(expected),
                "expected {expected:?} in error, got {message:?}"
            );
        }
    }

    #[test]
    fn tool_registry_keeps_compound_intents_run_pipeline_only() {
        let source = include_str!("main.rs");
        let list_tools_block = source
            .split("fn list_tools(")
            .nth(1)
            .and_then(|tail| tail.split("// ── Tool dispatch").next())
            .expect("list_tools block should be present");

        assert!(
            list_tools_block.contains("Tool::new(\n                \"run_pipeline\""),
            "run_pipeline should remain the compound intent entry point"
        );
        for forbidden_tool in ["explore_batch", "dependency_graph", "map_class"] {
            let forbidden = format!("Tool::new(\n                \"{forbidden_tool}\",");
            assert!(
                !list_tools_block.contains(&forbidden),
                "{forbidden_tool} must not be exposed as a top-level tool alias"
            );
        }
    }

    #[test]
    fn explore_batch_dispatch_records_aggregate_batch_stats() {
        let source = include_str!("main.rs");
        let dispatch_block = source
            .split("\"explore_batch\" => {")
            .nth(1)
            .and_then(|tail| tail.split("\"dependency_graph\" => {").next())
            .expect("explore_batch dispatch block should be present");

        assert!(
            dispatch_block.contains("db::increment_stat(&conn, \"batch_requests\", 1)?"),
            "batch request counter should be incremented in dispatch"
        );
        assert!(
            dispatch_block.contains("\"batch_queries\""),
            "batch query counter should be incremented in dispatch"
        );
        assert!(
            dispatch_block.contains("db::increment_stat(&conn, \"batch_truncated\", 1)?"),
            "batch truncation counter should be incremented when applicable"
        );
    }

    #[test]
    fn find_symbol_limit_parser_validates_positive_integer() {
        let valid = json!({ "intent": "find_symbol", "limit": 2 })
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(parse_find_symbol_limit(&valid).unwrap(), 2);

        let missing = json!({ "intent": "find_symbol" })
            .as_object()
            .unwrap()
            .clone();
        assert_eq!(
            parse_find_symbol_limit(&missing).unwrap(),
            retrieval::FIND_SYMBOL_DEFAULT_LIMIT
        );

        for invalid in [
            json!({ "intent": "find_symbol", "limit": 0 }),
            json!({ "intent": "find_symbol", "limit": -1 }),
            json!({ "intent": "find_symbol", "limit": "2" }),
        ] {
            let args = invalid.as_object().unwrap().clone();
            let err = parse_find_symbol_limit(&args).unwrap_err();
            assert!(
                err.message.contains("positive integer"),
                "unexpected limit validation error: {}",
                err.message
            );
        }
    }

    #[test]
    fn run_pipeline_find_symbol_dispatch_forwards_kind_and_limit() {
        let conn = crate::db::init_db(":memory:").unwrap();
        insert_benchmark_repo(&conn, "repo", "/tmp/repo");
        insert_benchmark_node(
            &conn,
            "repo",
            "src/a.rs",
            "rs",
            "process_alpha",
            "function",
            "fn process_alpha() { expensive_body(); }",
        );
        insert_benchmark_node(
            &conn,
            "repo",
            "src/b.rs",
            "rs",
            "process_beta",
            "function",
            "fn process_beta() {}",
        );
        insert_benchmark_node(
            &conn,
            "repo",
            "src/c.rs",
            "rs",
            "ProcessAlpha",
            "class",
            "class ProcessAlpha {}",
        );

        let out = dispatch_run_pipeline_find_symbol(&conn, "repo", "process", Some("function"), 1)
            .unwrap();

        assert!(out.contains("Found 1 matches for 'process':"), "{out}");
        assert!(out.contains("(function: process_"), "{out}");
        assert!(!out.contains("ProcessAlpha"), "kind filter leaked: {out}");
        assert!(!out.contains("expensive_body"), "source body leaked: {out}");
        assert!(
            out.contains("capped at 1 matches"),
            "limit was not forwarded: {out}"
        );
    }

    #[test]
    fn explore_batch_telemetry_reuses_existing_dashboard_events() {
        let conn = crate::db::init_db(":memory:").unwrap();
        insert_benchmark_repo(&conn, "repo", "/tmp/repo");
        insert_benchmark_node(
            &conn,
            "repo",
            "src/a.rs",
            "rs",
            "A",
            "function",
            "fn A() { B(); }",
        );
        insert_benchmark_node(
            &conn,
            "repo",
            "src/b.rs",
            "rs",
            "B",
            "function",
            "fn B() {}",
        );
        conn.execute(
            "INSERT INTO edges (source_id, target_id, relationship_type) VALUES (?1, ?2, ?3)",
            rusqlite::params!["repo:src/a.rs:A", "repo:src/b.rs:B", "CALLS"],
        )
        .unwrap();

        let batch_query = |intent: retrieval::BatchIntent, target: &str| retrieval::BatchQuery {
            intent,
            target: target.to_string(),
            filepath: None,
            kind: None,
            limit: None,
            depth: None,
            direction: None,
            include_source: false,
            max_nodes: None,
        };
        let execution = retrieval::execute_batch_queries(
            &conn,
            &[
                batch_query(retrieval::BatchIntent::ExploreSymbol, "A"),
                batch_query(retrieval::BatchIntent::RefactorSymbol, "B"),
                batch_query(retrieval::BatchIntent::DependencyGraph, "A"),
            ],
            retrieval::BatchOptions {
                repo_id: "repo".to_string(),
                max_bytes: 100_000,
            },
        )
        .unwrap();

        assert_eq!(execution.query_count, 3);
        assert_eq!(execution.telemetry.len(), 3);
        let events = dashboard_events_from_batch_telemetry(execution.telemetry, "batch-agent");
        assert_eq!(events.len(), 3);

        match &events[0] {
            DashboardEvent::CapsuleServed {
                symbol,
                repo,
                origin,
                optimized_text,
                ..
            } => {
                assert_eq!(symbol, "A");
                assert_eq!(repo, "repo");
                assert_eq!(origin, "batch-agent");
                assert!(optimized_text.as_deref().unwrap_or_default().contains("A"));
            }
            other => panic!("expected CapsuleServed, got {other:?}"),
        }
        match &events[1] {
            DashboardEvent::ImpactAnalyzed {
                symbol,
                repo,
                affected_count,
                ..
            } => {
                assert_eq!(symbol, "B");
                assert_eq!(repo, "repo");
                assert_eq!(*affected_count, 1);
            }
            other => panic!("expected ImpactAnalyzed, got {other:?}"),
        }
        match &events[2] {
            DashboardEvent::SkeletonGenerated {
                target_dir,
                node_count,
                ..
            } => {
                assert_eq!(target_dir, "dependency_graph:repo:A");
                assert_eq!(*node_count, 1);
            }
            other => panic!("expected SkeletonGenerated, got {other:?}"),
        }
    }

    #[test]
    fn rule_install_note_mentions_optional_rules_and_preservation() {
        let note = rule_install_note();
        assert!(
            note.contains("native instruction surface"),
            "expected Marrow usage guidance: {note}"
        );
        assert!(
            note.contains("custom files are preserved"),
            "expected preservation guidance: {note}"
        );
        assert!(
            note.contains("updated"),
            "expected refresh guidance: {note}"
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
        assert!(
            line.contains("GitHub Copilot"),
            "agent name missing: {line}"
        );
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
    fn format_rule_install_status_line_reports_refreshed_targets() {
        let line = format_rule_install_status_line(
            "GitHub Copilot",
            skills::InstallStatus::Refreshed,
            Path::new("/tmp/home/.github/instructions/marrow-optimization.instructions.md"),
        );
        assert!(line.contains("rules refreshed"), "status missing: {line}");
        assert!(
            line.contains(".github/instructions/marrow-optimization.instructions.md"),
            "target path missing: {line}"
        );
    }

    #[test]
    fn install_status_label_reports_refreshed_as_updated() {
        assert_eq!(
            install_status_label(skills::InstallStatus::Written),
            "installed"
        );
        assert_eq!(
            install_status_label(skills::InstallStatus::Refreshed),
            "updated"
        );
        assert_eq!(
            install_status_label(skills::InstallStatus::PreservedExisting),
            "preserved existing"
        );
    }

    #[test]
    fn workspace_setup_summary_matches_newer_installer_expectations() {
        let summary =
            format_workspace_setup_summary(Path::new("/tmp/workspace"), Path::new("/tmp/home"));
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
    }

    #[tokio::test]
    async fn auto_init_fires_when_marrow_absent_then_skips() {
        let tmp = tempfile::tempdir().unwrap();
        // Point the process CWD at our temp dir so try_auto_init writes there.
        // Acquire CWD_MUTEX before mutating process-global CWD; held for the full
        // test body (including .await points) to prevent races with sibling tests.
        let _cwd_guard = CWD_MUTEX.lock().await;
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
        assert!(
            tmp.path().join(".marrow").is_dir(),
            ".marrow/ should exist after init"
        );

        // Second call: .marrow/ now exists → should return None
        let second = try_auto_init().await;
        assert!(
            second.is_none(),
            "expected None on second call when .marrow/ exists"
        );

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

        let routed = apply_compliance_gate("get_context_capsule", args);

        assert_eq!(routed.tool_name, "run_pipeline");
        assert_eq!(
            routed.args.get("intent").and_then(|v| v.as_str()),
            Some("explore_symbol")
        );
        assert_eq!(
            routed.args.get("target").and_then(|v| v.as_str()),
            Some("bulk_update")
        );
        assert_eq!(
            routed.args.get("repo_id").and_then(|v| v.as_str()),
            Some("accrualify-rails")
        );
        assert!(
            routed
                .notice
                .as_deref()
                .unwrap_or_default()
                .contains("auto-routed"),
            "expected compliance warning notice"
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

        let report =
            format_validation_report(Path::new("/tmp/workspace"), Path::new("/tmp/home"), &conn);

        assert!(
            report.contains("run_pipeline requests: 5"),
            "pipeline count missing: {report}"
        );
        assert!(
            report.contains("direct low-level auto-routed: 2"),
            "autoroute count missing: {report}"
        );
        assert!(
            report.contains("direct low-level rejected: 1"),
            "reject count missing: {report}"
        );
    }

    #[test]
    fn legacy_strict_config_is_normalized_to_default() {
        let tmp = tempfile::tempdir().unwrap();
        // Acquire CWD_MUTEX before mutating process-global CWD to prevent races
        // with other tests that also call set_current_dir.
        let _cwd_guard = CWD_MUTEX.blocking_lock();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        // Pre-populate .marrowrc.json with a legacy "strict" enforcement mode.
        fs::write(".marrowrc.json", r#"{"enforcement_mode": "strict"}"#).unwrap();

        ensure_workspace_config().expect("ensure_workspace_config must succeed");

        let raw = fs::read_to_string(".marrowrc.json").unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            cfg.get("enforcement_mode").and_then(|v| v.as_str()),
            Some("default"),
            "legacy strict enforcement_mode must be normalized to default: {raw}"
        );

        std::env::set_current_dir(original).unwrap();
    }

    #[test]
    fn workspace_setup_writes_elastic_rules_regardless_of_legacy_strict_arg() {
        let tmp = tempfile::tempdir().unwrap();
        // Acquire CWD_MUTEX before mutating process-global CWD to prevent races
        // with other tests that also call set_current_dir.
        let _cwd_guard = CWD_MUTEX.blocking_lock();
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        // Simulate an old agent calling workspace_setup with {"enforcement_mode": "strict"}
        // by pre-seeding the config — the handler ignores args and always calls
        // ensure_workspace_config(), so the pre-existing file is the legacy path.
        fs::write(".marrowrc.json", r#"{"enforcement_mode": "strict"}"#).unwrap();

        let rule_indices = workspace_rule_target_indices();
        write_workspace_rules(
            Path::new("."),
            &rule_indices,
            WORKSPACE_RULES_CONTENT_SOFT,
            WriteMode::SafeAppend,
        )
        .expect("write_workspace_rules must succeed");
        ensure_workspace_config().expect("ensure_workspace_config must succeed");

        // Config must be normalized to default regardless of the pre-existing strict value.
        let raw = fs::read_to_string(".marrowrc.json").unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            cfg.get("enforcement_mode").and_then(|v| v.as_str()),
            Some("default"),
            "workspace_setup must normalize enforcement_mode to default: {raw}"
        );

        // Rule files written must carry the elastic/soft sentinel, never a strict heading.
        for rule_file in LEGACY_WORKSPACE_RULE_FILES_BY_INDEX
            .iter()
            .flat_map(|files| files.iter().copied())
        {
            let path = Path::new(".").join(rule_file);
            if path.exists() {
                let content = fs::read_to_string(&path).unwrap();
                assert!(
                    content.contains("MARROW AST CONTEXT ENGINE"),
                    "rule file {rule_file} must contain soft sentinel: {content}"
                );
                assert!(
                    !content.contains("STRICT WORKFLOW PROTOCOL"),
                    "rule file {rule_file} must not contain strict heading: {content}"
                );
            }
        }

        std::env::set_current_dir(original).unwrap();
    }

    #[test]
    fn agent_coverage_summary_reports_partial_for_cursor_fallback_files() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        fs::write(workspace.path().join(".cursorrules"), "marrow").unwrap();
        fs::create_dir_all(workspace.path().join(".vscode")).unwrap();
        fs::write(workspace.path().join(".vscode/mcp.json"), "{}").unwrap();

        let summary = format_agent_coverage_summary(workspace.path(), home.path());
        assert!(
            summary.contains("Cursor: partial"),
            "cursor fallback coverage should be partial: {summary}"
        );
    }

    #[test]
    fn agent_coverage_ignores_unrelated_instruction_file_contents() {
        let workspace = tempfile::tempdir().unwrap();
        let home = tempfile::tempdir().unwrap();
        let target = workspace
            .path()
            .join(".cursor/rules/marrow-optimization.mdc");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "unrelated content").unwrap();

        let (status, _) =
            coverage_status_for_agent(skills::Agent::Cursor, workspace.path(), home.path());
        assert_eq!(
            status, "unprotected",
            "non-Marrow files should not count as protected"
        );
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
            rusqlite::params![
                "other_repo",
                indexed_root_path.to_string_lossy().to_string()
            ],
        )
        .unwrap();

        // Repo exists in DB — should succeed even from a different workspace.
        let result = ensure_repo_ready(&conn, Some("other_repo"), &current_root_path);
        assert!(
            result.is_ok(),
            "existing repo should be accepted: {:?}",
            result.err()
        );

        // Repo does NOT exist in DB — should be rejected.
        let err =
            ensure_repo_ready(&conn, Some("nonexistent_repo"), &current_root_path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("not found"), "expected not-found error: {msg}");
        assert!(
            msg.contains("ingest_repo"),
            "expected guidance to ingest: {msg}"
        );
    }

    /// M-8: ensure_repo_ready auto-indexes when the repo has no symbols.
    #[test]
    fn ensure_repo_ready_auto_indexes_empty_repo() {
        let dir = tempfile::tempdir().unwrap();
        // Create a parseable file so ingestion produces symbols
        fs::write(dir.path().join("hello.py"), "def hello():\n    pass\n").unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        // No explicit repo_id — should resolve to the dir name and auto-index
        let repo_id = ensure_repo_ready(&conn, None, dir.path()).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
                rusqlite::params![repo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            count > 0,
            "auto-indexing should have produced symbols, got {count}"
        );
    }

    // ── launch-spec helpers ────────────────────────────────────────────────────

    #[test]
    fn mcp_launch_spec_is_portable_command_not_absolute_path() {
        let spec = mcp_launch_spec();
        let cmd = spec["command"].as_str().expect("command must be a string");
        assert_eq!(cmd, "marrow");
        assert!(
            !cmd.starts_with('/'),
            "command must not be an absolute path: {cmd}"
        );
        assert_eq!(spec["args"][0], "mcp");
    }

    #[test]
    fn mcp_shell_launch_spec_uses_absolute_shell_and_invokes_marrow_mcp() {
        let spec = mcp_shell_launch_spec();
        let cmd = spec["command"].as_str().expect("command must be a string");
        let args = spec["args"].as_array().expect("args must be an array");
        let args_str: Vec<&str> = args.iter().filter_map(|a| a.as_str()).collect();

        // Command must be an absolute path to a shell — never "marrow" itself.
        #[cfg(not(target_os = "windows"))]
        assert!(
            cmd.starts_with('/'),
            "shell must be an absolute path on Unix: {cmd}"
        );
        #[cfg(target_os = "windows")]
        assert!(
            cmd.ends_with(".exe"),
            "shell must be an .exe on Windows: {cmd}"
        );

        // The args must ultimately invoke "marrow mcp".
        let full = args_str.join(" ");
        assert!(
            full.contains("marrow mcp"),
            "args must invoke 'marrow mcp': {full}"
        );
    }

    #[test]
    fn gui_safe_path_places_binary_dir_first() {
        let path = gui_safe_path("/usr/local/bin/marrow");
        let first = path.split(':').next().unwrap();
        assert_eq!(first, "/usr/local/bin");
    }

    #[test]
    fn gui_safe_path_includes_cargo_bin() {
        let path = gui_safe_path("/some/other/marrow");
        assert!(
            path.contains(".cargo/bin"),
            "expected .cargo/bin in: {path}"
        );
    }

    #[test]
    fn gui_safe_path_has_no_duplicate_entries() {
        // /usr/local/bin is in our static list AND is the binary dir here — must not duplicate.
        let path = gui_safe_path("/usr/local/bin/marrow");
        let segments: Vec<&str> = path.split(':').collect();
        let unique: std::collections::HashSet<_> = segments.iter().collect();
        assert_eq!(
            segments.len(),
            unique.len(),
            "duplicate entries in PATH: {path}"
        );
    }

    #[test]
    fn gui_safe_path_is_non_empty_with_empty_binary() {
        let path = gui_safe_path("");
        assert!(!path.is_empty(), "PATH must never be empty");
        assert!(path.contains("/usr/local/bin"));
    }

    #[test]
    fn validate_marrow_command_error_lists_searched_dirs() {
        let result = validate_marrow_command("/dir/one:/dir/two:/dir/three");
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(msg.contains("/dir/one"), "expected dir in error: {msg}");
        assert!(msg.contains("/dir/two"), "expected dir in error: {msg}");
    }

    #[test]
    fn validate_marrow_command_fails_on_nonexistent_path() {
        let result = validate_marrow_command("/nonexistent/path:/also/nonexistent");
        assert!(result.is_err());
        let msg = format!("{:#}", result.unwrap_err());
        assert!(
            msg.contains("not found"),
            "error should mention not found: {msg}"
        );
    }

    // ── per-agent integration config tests ────────────────────────────────────

    #[test]
    fn integrate_claude_uses_shell_wrapper() {
        let home = tempfile::tempdir().unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_claude(&ctx).unwrap();
        let raw = std::fs::read_to_string(home.path().join(".claude.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        assert!(
            cmd.ends_with("zsh") || cmd.ends_with("bash"),
            "expected shell binary, got: {cmd}"
        );
        assert_eq!(cfg["mcpServers"]["marrow"]["args"][0], "-lc");
        assert!(
            cfg["mcpServers"]["marrow"]["args"][1]
                .as_str()
                .unwrap()
                .contains("marrow mcp"),
            "shell invocation must contain 'marrow mcp'"
        );
        assert!(
            !raw.contains("/absolute/path/to/marrow"),
            "binary path must not leak into config"
        );
    }

    #[test]
    fn integrate_antigravity_writes_command_marrow_with_env_path() {
        let home = tempfile::tempdir().unwrap();
        let cfg_path = home.path().join(".gemini/antigravity/mcp_config.json");
        std::fs::create_dir_all(cfg_path.parent().unwrap()).unwrap();
        std::fs::write(&cfg_path, "{}").unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_antigravity(&ctx).unwrap();
        let raw = std::fs::read_to_string(&cfg_path).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        assert_eq!(cmd, "marrow");
        assert!(cfg["mcpServers"]["marrow"]["env"]["PATH"].is_string());
    }

    #[test]
    fn integrate_cursor_uses_shell_wrapper() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".cursor")).unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_cursor(&ctx).unwrap();
        let raw = std::fs::read_to_string(home.path().join(".cursor/mcp.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        // Cursor uses shell wrapper — cmd is the shell binary, not "marrow".
        assert!(
            cmd.ends_with("zsh") || cmd.ends_with("bash"),
            "expected shell binary, got: {cmd}"
        );
        assert_eq!(cfg["mcpServers"]["marrow"]["args"][0], "-lc");
        assert!(cfg["mcpServers"]["marrow"]["args"][1]
            .as_str()
            .unwrap()
            .contains("marrow mcp"));
        // ctx.binary must not appear anywhere in the config.
        assert!(
            !raw.contains("/absolute/path/to/marrow"),
            "binary path must not leak into config"
        );
    }

    #[test]
    fn integrate_copilot_vscode_uses_shell_wrapper() {
        let home = tempfile::tempdir().unwrap();
        let vscode_dir = home.path().join("Library/Application Support/Code/User");
        std::fs::create_dir_all(&vscode_dir).unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_copilot(&ctx).unwrap();
        let raw = std::fs::read_to_string(vscode_dir.join("mcp.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["servers"]["marrow"]["command"].as_str().unwrap();
        assert!(
            cmd.ends_with("zsh") || cmd.ends_with("bash"),
            "vscode: expected shell binary, got: {cmd}"
        );
        assert_eq!(cfg["servers"]["marrow"]["args"][0], "-lc");
        assert!(!raw.contains("/absolute/path/to/marrow"));
    }

    #[test]
    fn integrate_copilot_cli_uses_shell_wrapper() {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".copilot")).unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_copilot(&ctx).unwrap();
        let raw = std::fs::read_to_string(home.path().join(".copilot/mcp-config.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        assert!(
            cmd.ends_with("zsh") || cmd.ends_with("bash"),
            "copilot cli: expected shell binary, got: {cmd}"
        );
        assert_eq!(cfg["mcpServers"]["marrow"]["args"][0], "-lc");
        assert_eq!(cfg["mcpServers"]["marrow"]["type"], "stdio");
        assert!(!raw.contains("/absolute/path/to/marrow"));
    }

    #[test]
    fn integrate_cline_uses_shell_wrapper() {
        let home = tempfile::tempdir().unwrap();
        let cline_dir = home
            .path()
            .join("Library/Application Support/Code/User/globalStorage")
            .join("saoudrizwan.claude-dev/settings");
        std::fs::create_dir_all(&cline_dir).unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_cline(&ctx).unwrap();
        let raw = std::fs::read_to_string(cline_dir.join("cline_mcp_settings.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        assert!(
            cmd.ends_with("zsh") || cmd.ends_with("bash"),
            "cline: expected shell binary, got: {cmd}"
        );
        assert_eq!(cfg["mcpServers"]["marrow"]["args"][0], "-lc");
        assert_eq!(cfg["mcpServers"]["marrow"]["disabled"], false);
        assert!(!raw.contains("/absolute/path/to/marrow"));
    }

    #[test]
    fn integrate_zed_writes_command_marrow_with_env_path() {
        let home = tempfile::tempdir().unwrap();
        let zed_dir = home.path().join(".config/zed");
        std::fs::create_dir_all(&zed_dir).unwrap();
        std::fs::write(zed_dir.join("settings.json"), "{}").unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_zed(&ctx).unwrap();
        let raw = std::fs::read_to_string(zed_dir.join("settings.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let path_val = cfg["context_servers"]["marrow"]["command"]["path"]
            .as_str()
            .unwrap();
        assert_eq!(
            path_val, "marrow",
            "Zed command.path must be 'marrow', got: {path_val}"
        );
        assert!(!path_val.starts_with('/'));
        assert!(cfg["context_servers"]["marrow"]["command"]["env"]["PATH"].is_string());
    }

    // ── regression guard ──────────────────────────────────────────────────────

    #[test]
    fn no_integrate_fn_leaks_binary_path_into_config() {
        // Regression guard: ctx.binary must never appear as a command value in any config.
        // - Antigravity/Zed: use command:"marrow" (portable name) with env.PATH.
        // - Claude/Cursor/Copilot/Cline: use shell wrapper (/bin/zsh or /bin/bash) — never the binary.
        let home = tempfile::tempdir().unwrap();
        let h = home.path();
        const BINARY: &str = "/some/absolute/path/to/marrow-unique-sentinel";

        // Pre-create all required directories/files.
        std::fs::create_dir_all(h.join(".cursor")).unwrap();
        std::fs::create_dir_all(h.join(".copilot")).unwrap();
        let ag = h.join(".gemini/antigravity/mcp_config.json");
        std::fs::create_dir_all(ag.parent().unwrap()).unwrap();
        std::fs::write(&ag, "{}").unwrap();
        let cline = h.join(
            "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings",
        );
        std::fs::create_dir_all(&cline).unwrap();
        let zed = h.join(".config/zed");
        std::fs::create_dir_all(&zed).unwrap();
        std::fs::write(zed.join("settings.json"), "{}").unwrap();
        let vscode = h.join("Library/Application Support/Code/User");
        std::fs::create_dir_all(&vscode).unwrap();

        let ctx = IntegrationCtx {
            binary: BINARY.to_string(),
            home: h.to_string_lossy().into_owned(),
        };

        integrate_claude(&ctx).unwrap();
        integrate_antigravity(&ctx).unwrap();
        integrate_cursor(&ctx).unwrap();
        integrate_copilot(&ctx).unwrap();
        integrate_cline(&ctx).unwrap();
        integrate_zed(&ctx).unwrap();

        // Every written config file must not contain the sentinel binary path.
        let config_files = [
            ".claude.json",
            ".cursor/mcp.json",
            ".copilot/mcp-config.json",
            "Library/Application Support/Code/User/mcp.json",
            "Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
            ".config/zed/settings.json",
        ];
        for rel_path in &config_files {
            let raw = std::fs::read_to_string(h.join(rel_path))
                .unwrap_or_else(|_| panic!("config not written: {rel_path}"));
            assert!(
                !raw.contains(BINARY),
                "{rel_path}: binary path leaked into config:\n{raw}"
            );
        }

        // Antigravity must use portable command name.
        // (Claude Code now uses shell wrapper — verified in the shell-wrapper loop below.)

        // Zed must use portable path name in nested command object.
        let zed_raw = std::fs::read_to_string(zed.join("settings.json")).unwrap();
        let zed_cfg: serde_json::Value = serde_json::from_str(&zed_raw).unwrap();
        assert_eq!(
            zed_cfg["context_servers"]["marrow"]["command"]["path"],
            "marrow"
        );

        // Shell-wrapper hosts must use a shell binary, not "marrow" directly.
        for (rel, ptr) in [
            (".claude.json", "/mcpServers/marrow/command"),
            (".cursor/mcp.json", "/mcpServers/marrow/command"),
            ("Library/Application Support/Code/User/mcp.json", "/servers/marrow/command"),
            ("Library/Application Support/Code/User/globalStorage/saoudrizwan.claude-dev/settings/cline_mcp_settings.json",
             "/mcpServers/marrow/command"),
        ] {
            let raw = std::fs::read_to_string(h.join(rel)).unwrap();
            let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
            let cmd = cfg.pointer(ptr).and_then(|v| v.as_str())
                .unwrap_or_else(|| panic!("command missing at {ptr} in {rel}"));
            assert!(
                cmd.ends_with("zsh") || cmd.ends_with("bash"),
                "{rel}: expected shell wrapper, got: {cmd}"
            );
        }
    }
}

// ── CLI subcommands ───────────────────────────────────────────────────────────

/// `marrow ui` — interactive dashboard configuration menu.
fn cmd_ui() -> Result<()> {
    use dialoguer::{theme::ColorfulTheme, Select};

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

        let items = vec!["Open Dashboard in Browser", toggle_label.as_str(), "Exit"];

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

const WORKSPACE_RULES_CONTENT_SOFT: &str = r#"# MARROW AST CONTEXT ENGINE - WORKFLOW GUIDANCE
You are equipped with the 'marrow' MCP server, a local deterministic AST context engine. Prefer Marrow when the task needs code structure, dependencies, symbol neighborhoods, execution traces, repo maps, class maps, or refactor blast-radius analysis. Native read/search tools are fine for single-file lookups, exact text search, line-level work, config/docs edits, and small known files.

## The Omni-Tool
Use `run_pipeline` when graph context is likely to beat raw file reads.

### Intent Routing Guide

* Repository map or architecture overview:
    * Prefer `run_pipeline` with `intent: "analyze_repo"`.

* Partial symbol name only:
    * Prefer `run_pipeline` with `intent: "find_symbol"` and `target: "fragment"` before falling back to `analyze_repo`.

* Precise execution flow for a known symbol:
    * Prefer `run_pipeline` with `intent: "trace_flow"` and the target symbol.

* Three or more related symbols in one task:
    * Prefer `run_pipeline` with `intent: "explore_batch"` and a `queries` array.

* Multi-hop caller/callee dependency map:
    * Prefer `run_pipeline` with `intent: "dependency_graph"` and the target symbol.

* Full class-level architecture map:
    * Prefer `run_pipeline` with `intent: "map_class"` and the target class.

* Symbol neighborhood, callers, callees, or local architecture:
    * Prefer `run_pipeline` with `intent: "explore_symbol"` and the target.

* Refactor, rename, delete, or API change:
    * Prefer `run_pipeline` with `intent: "refactor_symbol"` and the target.

If any tool states the database is empty, run `ingest_repo` to build the index.

### Progressive Disclosure
Marrow uses **Progressive Disclosure**: neighbor symbols in a capsule show signatures only.
To expand a neighbor into its full source, call:
  `run_pipeline(intent: "read_node", target: "<SymbolName>")`

### Handling Ambiguity
If `run_pipeline` returns a "Disambiguation Payload" stating that multiple matches were found for your target:
1. Look at the provided list of file paths in the error payload.
2. Call `run_pipeline` again, passing the exact same `intent` and `target`, but this time include the correct `filepath` parameter to disambiguate.

### Output hygiene
Do **not** add a "Made-with: Cursor" tag (or similar editor or tool attribution) to commits, pull requests, READMEs, or other artifacts unless the user explicitly asks for it.
"#;

/// `marrow rules` — write Marrow workflow guidance into the target workspace.
/// Writes rule files for Cursor, Cline, Roo/Antigravity, and Windsurf so all
/// AI agent variants default to Marrow tools.
///
/// This function is append-only and idempotent: if the Marrow header is already
/// present in a file it is skipped entirely, preventing duplicate entries and
/// preserving any user-authored content that precedes the Marrow block.
/// Creates or merges `.vscode/mcp.json` so that GitHub Copilot / VS Code
/// can discover the Marrow MCP server. Existing `mcpServers` entries are
/// preserved — only the `"marrow"` key is inserted/updated.
pub fn write_vscode_mcp_config(workspace_root: &Path, mode: WriteMode) -> Result<Option<String>> {
    let vscode_dir = workspace_root.join(".vscode");
    fs::create_dir_all(&vscode_dir)
        .with_context(|| format!("could not create {}", vscode_dir.display()))?;

    let mcp_path = vscode_dir.join("mcp.json");

    let marrow_entry = mcp_shell_launch_spec();

    // Overwrite mode discards existing config; all other modes preserve it.
    let mut config: serde_json::Value =
        if mcp_path.exists() && !matches!(mode, WriteMode::Overwrite) {
            let raw = fs::read_to_string(&mcp_path)
                .with_context(|| format!("could not read {}", mcp_path.display()))?;
            serde_json::from_str(&raw).unwrap_or_else(|_| serde_json::json!({}))
        } else {
            serde_json::json!({})
        };

    // VS Code workspace mcp.json uses "servers" (not "mcpServers" which is the Cline/Claude format).
    if !config["servers"].is_object() {
        config["servers"] = serde_json::json!({});
    }
    config["servers"]["marrow"] = marrow_entry;

    let pretty = serde_json::to_string_pretty(&config).context("could not serialize mcp.json")?;
    fs::write(&mcp_path, pretty)
        .with_context(|| format!("could not write {}", mcp_path.display()))?;

    let action = match mode {
        WriteMode::Overwrite => "overwritten",
        _ => "merged",
    };
    eprintln!(
        "Wrote VS Code MCP config to {} ({})",
        mcp_path.display(),
        action
    );
    Ok(Some(mcp_path.display().to_string()))
}

fn write_workspace_rule_files(
    root_dir: &Path,
    rule_files: &[&str],
    rules_content: &str,
    mode: WriteMode,
) -> Result<Vec<String>> {
    use std::io::Write;
    const MARROW_HEADER: &str = "# MARROW AST CONTEXT ENGINE";

    let mut modified: Vec<String> = Vec::new();

    let central_rules_path = if matches!(mode, WriteMode::Symlink) {
        let p = dirs::home_dir()
            .context("could not resolve home directory")?
            .join(".marrow")
            .join("global_rules.md");
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).ok();
        }
        if !p.exists() {
            fs::write(&p, rules_content)
                .with_context(|| format!("could not write central rules to {}", p.display()))?;
        }
        Some(p)
    } else {
        None
    };

    for &filename in rule_files {
        let path = root_dir.join(filename);
        match mode {
            WriteMode::SafeAppend => {
                if path.exists() {
                    let existing = fs::read_to_string(&path)
                        .with_context(|| format!("could not read {}", path.display()))?;
                    if existing.contains(MARROW_HEADER) {
                        eprintln!("Skipped {} (Marrow rules already present)", path.display());
                        continue;
                    }
                    let mut file = fs::OpenOptions::new()
                        .append(true)
                        .open(&path)
                        .with_context(|| format!("could not open {}", path.display()))?;
                    write!(file, "\n\n{rules_content}")?;
                    eprintln!("Appended to {}", path.display());
                } else {
                    let mut file = fs::OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&path)
                        .with_context(|| format!("could not create {}", path.display()))?;
                    write!(file, "{rules_content}")?;
                    eprintln!("Created {}", path.display());
                }
                modified.push(path.display().to_string());
            }
            WriteMode::Overwrite => {
                fs::write(&path, rules_content)
                    .with_context(|| format!("could not write {}", path.display()))?;
                eprintln!("Overwrote {}", path.display());
                modified.push(path.display().to_string());
            }
            WriteMode::Symlink => {
                let central = central_rules_path.as_ref().expect("central path set above");
                if path.exists() || path.is_symlink() {
                    fs::remove_file(&path).ok();
                }
                #[cfg(unix)]
                {
                    std::os::unix::fs::symlink(central, &path).with_context(|| {
                        format!(
                            "could not symlink {} -> {}",
                            path.display(),
                            central.display()
                        )
                    })?;
                    eprintln!("Symlinked {} -> {}", path.display(), central.display());
                    modified.push(path.display().to_string());
                }
                #[cfg(not(unix))]
                {
                    fs::write(&path, rules_content)
                        .with_context(|| format!("could not write {}", path.display()))?;
                    eprintln!(
                        "Created {} (symlink unsupported on this platform, wrote copy)",
                        path.display()
                    );
                    modified.push(path.display().to_string());
                }
            }
        }
    }

    Ok(modified)
}

/// Write Marrow rule files for the selected legacy agent groups.
///
/// `agent_indices` preserves the public legacy contract:
/// 0 => Cursor, 1 => Windsurf, 2 => Cline + Roo Code.
///
/// Returns the list of file paths that were created, appended, or symlinked.
pub fn write_workspace_rules(
    root_dir: &Path,
    agent_indices: &[usize],
    rules_content: &str,
    mode: WriteMode,
) -> Result<Vec<String>> {
    let rule_files: Vec<&str> = agent_indices
        .iter()
        .filter_map(|&idx| LEGACY_WORKSPACE_RULE_FILES_BY_INDEX.get(idx))
        .flat_map(|files| files.iter().copied())
        .collect();
    write_workspace_rule_files(root_dir, &rule_files, rules_content, mode)
}

fn cmd_rules() -> Result<()> {
    let root = std::env::current_dir().context("could not determine current directory")?;
    let rule_indices = workspace_rule_target_indices();
    write_workspace_rules(
        &root,
        &rule_indices,
        WORKSPACE_RULES_CONTENT_SOFT,
        WriteMode::SafeAppend,
    )?;
    write_vscode_mcp_config(&root, WriteMode::SafeAppend)?;
    ensure_workspace_config()?;
    eprintln!("[MARROW] Successfully integrated! Workspace rules appended and VS Code / Copilot MCP configuration generated.");
    Ok(())
}

/// `marrow init` — scaffold a `.marrow/` directory and `.marrowrc.json` config.
fn cmd_init() -> Result<()> {
    let marrow_dir = Path::new(".marrow");
    if let Err(e) = fs::create_dir_all(marrow_dir) {
        eprintln!("Warning: could not create .marrow/ directory ({e}). Continuing.");
    }

    match ensure_workspace_config() {
        Ok(_) => eprintln!("Ensured .marrowrc.json with default settings."),
        Err(e) => eprintln!("Warning: could not write .marrowrc.json ({e}). Using defaults."),
    }

    eprintln!("Initialized .marrow/ workspace.");
    Ok(())
}

/// `marrow test-capsules` — run get_context_capsule for every (repo_id, symbol)
/// in the graph. Reports success/failure counts.
fn cmd_test_capsules() -> Result<()> {
    let db_path =
        std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db_or_memory(&db_path)?;

    let pairs: Vec<(String, String)> = conn
        .prepare("SELECT DISTINCT repo_id, symbol_name FROM nodes ORDER BY repo_id, symbol_name")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let total = pairs.len();
    let mut ok = 0usize;
    let mut err = 0usize;

    for (repo_id, symbol_name) in &pairs {
        match retrieval::get_context_capsule(&conn, symbol_name, repo_id, None) {
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
    home: String,
}

/// What a per-agent function reports back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AgentOutcome {
    Installed,
    NotFound,
    Guided,
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

// ── Shared launch-spec helpers ────────────────────────────────────────────────

/// Returns the canonical, portable MCP launch spec for Marrow.
/// All integration config writers MUST use this — never write command/args by hand.
fn mcp_launch_spec() -> serde_json::Value {
    serde_json::json!({
        "command": "marrow",
        "args":    ["mcp"]
    })
}

/// Returns a shell-wrapped MCP launch spec for VS Code-based hosts (VS Code, Cursor, Cline).
///
/// These hosts use Node.js `child_process.spawn` which resolves the command using the
/// *parent process* PATH — i.e. the GUI/launchd PATH — not the `env` field in the config.
/// Injecting `env.PATH` cannot prevent ENOENT because command lookup happens before the
/// child process is started.
///
/// The fix: delegate to a login shell at a known absolute path. The shell itself never
/// ENOENTs, and its login mode (`-l`) sources the user's profile so `marrow` is on PATH
/// by the time `marrow mcp` executes.
///
/// Platform behaviour:
///   macOS   — `/bin/zsh -lc "marrow mcp"`         (zsh guaranteed since Catalina; sources ~/.zprofile)
///   Linux   — `/bin/zsh` or `/bin/bash -lc`        (prefers zsh if installed; sources ~/.profile chain)
///   Windows — `powershell.exe -NoProfile -NonInteractive -Command "marrow mcp"`
///             (PATH inherited from Windows environment; no login-profile concept needed)
///   Other   — `/bin/sh -lc "marrow mcp"`           (POSIX login-shell fallback)
fn mcp_shell_launch_spec() -> serde_json::Value {
    // macOS: zsh is the system default since Catalina and is always at /bin/zsh.
    #[cfg(target_os = "macos")]
    let spec = serde_json::json!({
        "command": "/bin/zsh",
        "args":    ["-lc", "marrow mcp"]
    });

    // Windows: PowerShell is present on all modern Windows (7+). PATH is inherited from
    // the Windows environment where installers (cargo, scoop, winget) write their entries,
    // so no profile sourcing is required — we just need a shell that handles the invocation.
    #[cfg(target_os = "windows")]
    let spec = serde_json::json!({
        "command": "powershell.exe",
        "args":    ["-NoProfile", "-NonInteractive", "-Command", "marrow mcp"]
    });

    // Linux: prefer zsh if available (common on dev machines), otherwise bash.
    // Both support `-lc` and their login mode sources /etc/profile + ~/.profile / ~/.zprofile,
    // where cargo and package managers register their PATH entries.
    #[cfg(target_os = "linux")]
    let spec = {
        let shell = if std::path::Path::new("/bin/zsh").exists() {
            "/bin/zsh"
        } else {
            "/bin/bash"
        };
        serde_json::json!({
            "command": shell,
            "args":    ["-lc", "marrow mcp"]
        })
    };

    // Generic Unix fallback (FreeBSD, OpenBSD, etc.): POSIX sh login shell.
    #[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
    let spec = serde_json::json!({
        "command": "/bin/sh",
        "args":    ["-lc", "marrow mcp"]
    });

    spec
}

/// Builds a GUI-safe PATH string for injection into generated MCP configs.
///
/// macOS IDEs launched from Finder/Dock/Spotlight inherit launchd's minimal
/// PATH, not the user's interactive shell PATH.  Injecting this value into
/// the `env.PATH` field of each MCP config entry lets the host find `marrow`
/// by command name even in that stripped environment.
///
/// Strategy: the directory containing the currently-running Marrow binary
/// wins (position 0), common package-manager locations follow, and the
/// existing process PATH appends at the end.  Duplicate entries are dropped
/// while preserving order.
fn gui_safe_path(binary_path: &str) -> String {
    let mut segments: Vec<String> = Vec::new();

    // 1. Directory of the running binary (position 0 — highest priority).
    if let Some(dir) = std::path::Path::new(binary_path).parent() {
        let s = dir.to_string_lossy();
        if !s.is_empty() {
            segments.push(s.into_owned());
        }
    }

    // 2. Common install locations for macOS and Linux package managers.
    let home = std::env::var("HOME").unwrap_or_default();
    for candidate in [
        format!("{home}/.cargo/bin"),
        format!("{home}/.local/bin"),
        "/opt/homebrew/bin".to_string(),
        "/opt/homebrew/sbin".to_string(),
        "/usr/local/bin".to_string(),
        "/usr/bin".to_string(),
        "/bin".to_string(),
    ] {
        segments.push(candidate);
    }

    // 3. Existing shell PATH last (lowest priority so our entries win).
    if let Ok(existing) = std::env::var("PATH") {
        for entry in existing.split(':') {
            if !entry.is_empty() {
                segments.push(entry.to_string());
            }
        }
    }

    // Deduplicate, preserving first occurrence.
    let mut seen = std::collections::HashSet::new();
    segments.retain(|s| seen.insert(s.clone()));
    segments.join(":")
}

/// Checks whether `marrow` is resolvable as an executable on `env_path`.
///
/// Manually walks each directory rather than spawning a subprocess.
/// Spawning would inherit the calling shell's PATH and produce false-positives
/// on machines where `marrow` is only on the interactive PATH, not the GUI PATH.
///
/// Returns `Ok(())` if marrow is found and executable; `Err` with a diagnostic
/// message (including which directories were searched) if not.
fn validate_marrow_command(env_path: &str) -> Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let found = env_path.split(':').any(|dir| {
        let candidate = std::path::Path::new(dir).join("marrow");
        if !candidate.is_file() {
            return false;
        }
        #[cfg(unix)]
        {
            candidate
                .metadata()
                .map(|m| m.permissions().mode() & 0o111 != 0)
                .unwrap_or(false)
        }
        #[cfg(not(unix))]
        {
            true // on non-Unix just check existence
        }
    });

    if found {
        Ok(())
    } else {
        let searched: Vec<&str> = env_path.split(':').take(10).collect();
        anyhow::bail!(
            "`marrow` binary not found on the generated PATH.\n\
             Searched: {}\n\
             Fix: ensure `marrow` is installed (e.g. `cargo install marrow`) \
             and is in one of the directories above.",
            searched.join(", ")
        )
    }
}

// ── Per-agent helpers ─────────────────────────────────────────────────────────

/// ~/.claude.json (global Claude Code config)
fn integrate_claude(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".claude.json");
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = mcp_shell_launch_spec();
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.gemini/antigravity/mcp_config.json
fn integrate_antigravity(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".gemini/antigravity/mcp_config.json");
    if !path.exists() {
        return Ok(AgentOutcome::NotFound);
    }
    let mut cfg = load_json_or_empty(&path)?;
    let mut spec = mcp_launch_spec();
    spec["env"] = serde_json::json!({ "PATH": gui_safe_path(&ctx.binary) });
    cfg["mcpServers"]["marrow"] = spec;
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

/// ~/.cursor/mcp.json (global)
/// Cursor uses Node.js child_process.spawn which resolves the command via the parent PATH
/// (launchd GUI PATH), not the env field. Use the shell wrapper so /bin/zsh resolves marrow
/// via the user's login PATH instead.
fn integrate_cursor(ctx: &IntegrationCtx) -> Result<AgentOutcome> {
    let path = PathBuf::from(&ctx.home).join(".cursor/mcp.json");
    let mut cfg = load_json_or_empty(&path)?;
    cfg["mcpServers"]["marrow"] = mcp_shell_launch_spec();
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
    //    Uses shell wrapper — VS Code's extension host resolves command via parent PATH,
    //    so env injection cannot prevent ENOENT for GUI-launched IDE.
    #[cfg(target_os = "macos")]
    let vscode_path =
        PathBuf::from(&ctx.home).join("Library/Application Support/Code/User/mcp.json");
    #[cfg(target_os = "linux")]
    let vscode_path = PathBuf::from(&ctx.home).join(".config/Code/User/mcp.json");
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let vscode_path = PathBuf::from(&ctx.home).join(".mcp.json");

    if let Some(parent) = vscode_path.parent() {
        if parent.exists() {
            let mut vscode_cfg = load_json_or_empty(&vscode_path)?;
            vscode_cfg["servers"]["marrow"] = mcp_shell_launch_spec();
            save_json(&vscode_path, &vscode_cfg)?;
        }
    }

    // 2. ~/.copilot/mcp-config.json — Copilot CLI
    let cli_path = PathBuf::from(&ctx.home).join(".copilot/mcp-config.json");
    let mut cli_cfg = load_json_or_empty(&cli_path)?;
    let mut spec = mcp_shell_launch_spec();
    spec["type"] = serde_json::json!("stdio");
    cli_cfg["mcpServers"]["marrow"] = spec;
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
    let mut spec = mcp_shell_launch_spec();
    spec["disabled"] = serde_json::json!(false);
    spec["autoApprove"] = serde_json::json!([]);
    cfg["mcpServers"]["marrow"] = spec;
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
            "path": "marrow",
            "args": ["mcp"],
            "env":  { "PATH": gui_safe_path(&ctx.binary) }
        },
        "settings": {}
    });
    save_json(&path, &cfg)?;
    Ok(AgentOutcome::Installed)
}

fn register_integration_target(
    target: &IntegrationTarget,
    ctx: &IntegrationCtx,
) -> Result<AgentOutcome> {
    if !target.allow_config_write || target.setup_mode == IntegrationSetupMode::Guided {
        return Ok(AgentOutcome::Guided);
    }

    let writer = target
        .writer
        .context("automatic integration target is missing a writer")?;
    writer(ctx)
}

fn rule_agent_for_scope(target: &IntegrationTarget, scope: skills::Scope) -> Option<skills::Agent> {
    target
        .rule_agent
        .filter(|agent| target.rule_support.supports(scope) && agent.supports_scope(scope))
}

fn format_integration_menu_label(target: &IntegrationTarget) -> String {
    format_menu_label(target.name, &integration_skill_directory(target))
}

fn format_integration_guidance(target: &IntegrationTarget) -> String {
    if target.support_tier == IntegrationSupportTier::CompatibilityOnly {
        return format!(
            "{} is a {} target, not an MCP agent/client/host destination. Configure Marrow through an MCP-capable agent or host, and point that tool at this backend separately.",
            target.name,
            target.kind.label()
        );
    }

    if target.setup_mode == IntegrationSetupMode::Automatic {
        return format!(
            "{} is a verified automatic {} target. Run `marrow integrate` without arguments to use the interactive installer.",
            target.name,
            target.kind.label()
        );
    }

    format!(
        "{} is supported as an MCP {} target, but no verified config file path or merge writer exists yet. Add Marrow as an MCP stdio server with command `marrow` and args [`mcp`]. No config file was written.",
        target.name,
        target.kind.label()
    )
}

// ── Interactive installer ─────────────────────────────────────────────────────

/// `marrow integrate` — launch the interactive TUI installer.
fn cmd_integrate(args: &[String]) -> Result<()> {
    use console::style;

    if !args.is_empty() {
        let name = args.join(" ");
        let (mcp_target, skill_target) = combined_target_lookup(&name);

        if mcp_target.is_none() && skill_target.is_none() {
            anyhow::bail!("unknown integration target: {name}");
        }

        let home = std::env::var("HOME").context("$HOME is not set")?;
        let home_path = PathBuf::from(&home);

        // Resolve (scope, method) pair: prompt if we have a skill target, otherwise default.
        let (scope, method) = if skill_target.is_some() {
            let scope = match inquire::Select::new(
                "Rule file scope",
                vec!["Global (recommended)", "Project"],
            )
            .prompt()
            {
                Ok(choice) => {
                    if choice.starts_with("Global") {
                        skills::Scope::Global
                    } else {
                        skills::Scope::Project
                    }
                }
                Err(inquire::InquireError::NotTTY) => skills::Scope::Project,
                Err(e) => return Err(e.into()),
            };

            let method =
                match inquire::Select::new("Rule file method", vec!["Write File", "Symlink"])
                    .prompt()
                {
                    Ok(choice) => {
                        if choice == "Symlink" {
                            skills::Method::Symlink
                        } else {
                            skills::Method::WriteFile
                        }
                    }
                    Err(inquire::InquireError::NotTTY) => skills::Method::WriteFile,
                    Err(e) => return Err(e.into()),
                };

            (scope, method)
        } else {
            (skills::Scope::Project, skills::Method::WriteFile)
        };

        ensure_workspace_config()?;

        // Install universal skill (always, regardless of target type).
        match skills::install_skill_to_dir(".agents/skills", scope, method, &home_path) {
            Ok(status) => {
                eprintln!(
                    "  {}  Universal \u{2192} .agents/skills/marrow-optimization.md ({})",
                    style("\u{2713}").green().bold(),
                    install_status_label(status),
                );
            }
            Err(e) => eprintln!(
                "  {}  Universal skill \u{2014} {}",
                style("\u{2717}").red().bold(),
                e,
            ),
        }

        // Skill-only or overlap: install skill file(s).
        if let Some(st) = skill_target {
            if st.scope_support.supports(scope) {
                match skills::install_skill_to_dir(st.skills_dir, scope, method, &home_path) {
                    Ok(status) => {
                        eprintln!(
                            "  {}  {} \u{2192} {}/marrow-optimization.md ({})",
                            style("\u{2713}").green().bold(),
                            st.name,
                            st.skills_dir,
                            install_status_label(status),
                        );
                    }
                    Err(e) => eprintln!(
                        "  {}  {} \u{2014} {}",
                        style("\u{2717}").red().bold(),
                        st.name,
                        e,
                    ),
                }
            } else {
                eprintln!(
                    "  {}  {} \u{2014} skill target does not support global scope",
                    style("i").cyan().bold(),
                    st.name,
                );
            }
        }

        // MCP-only or overlap: dispatch to register_integration_target for automatic
        // targets; print guidance for guided targets.
        if let Some(target) = mcp_target {
            let binary = std::env::current_exe()
                .context("Could not resolve current executable path")?
                .to_string_lossy()
                .to_string();
            let ctx = IntegrationCtx {
                binary,
                home: home.clone(),
            };

            let mcp_result = register_integration_target(target, &ctx);
            match &mcp_result {
                Ok(AgentOutcome::Installed) => eprintln!(
                    "  {}  {}  {}",
                    style("\u{2713}").green().bold(),
                    style(target.name).bold(),
                    style("MCP registered").dim(),
                ),
                Ok(AgentOutcome::NotFound) => eprintln!(
                    "  {}  {}  {}",
                    style("\u{26a0}").yellow().bold(),
                    style(target.name).dim(),
                    style("(not installed \u{2014} skipped)").dim(),
                ),
                Ok(AgentOutcome::Guided) => eprintln!(
                    "  {}  {}  {}",
                    style("i").cyan().bold(),
                    style(target.name).bold(),
                    style(format_integration_guidance(target)).dim(),
                ),
                Err(e) => eprintln!(
                    "  {}  {}  {}",
                    style("\u{2717}").red().bold(),
                    style(target.name).bold(),
                    style(format!("MCP \u{2014} {e}")).red(),
                ),
            }

            if matches!(
                mcp_result,
                Ok(AgentOutcome::Installed) | Ok(AgentOutcome::Guided)
            ) {
                if let Some(skill_agent) = rule_agent_for_scope(target, scope) {
                    let rule_target = skill_agent.target_path(scope, &home_path);
                    match skills::install_skill(skill_agent, scope, method, &home_path) {
                        Ok(status) => {
                            eprintln!(
                                "  {}  {}",
                                style("\u{2713}").green().bold(),
                                style(format_rule_install_status_line(
                                    target.name,
                                    status,
                                    &rule_target
                                ))
                                .dim(),
                            );
                            eprintln!(
                                "      {}",
                                style(skills::install_source_description(method, &home_path)).dim(),
                            );
                        }
                        Err(e) => eprintln!(
                            "  {}  {}  {}",
                            style("\u{2717}").red().bold(),
                            style(target.name).bold(),
                            style(format!("rules \u{2014} {e}")).red(),
                        ),
                    }
                }
            }
        }

        return Ok(());
    }

    eprintln!("{}", style(MARROW_BANNER).cyan().bold());
    eprintln!(
        "  {}",
        style("AST Context Engine  ·  MCP Server Installer").dim()
    );
    eprintln!();

    let (universal_entries, universal_labels) = universal_agent_menu();
    let (additional_entries, additional_labels) = additional_agent_menu();

    let universal_selected: Vec<&AgentMenuEntry> = match inquire::MultiSelect::new(
        UNIVERSAL_GROUP_LABEL,
        universal_labels.clone(),
    )
    .with_help_message("space to toggle, enter to confirm")
    .prompt()
    {
        Ok(chosen) => chosen
            .iter()
            .filter_map(|label| {
                universal_labels
                    .iter()
                    .position(|candidate| candidate == label)
                    .map(|idx| &universal_entries[idx])
            })
            .collect(),
        Err(inquire::InquireError::NotTTY) => {
            anyhow::bail!("Interactive prompts require a terminal. Use 'marrow integrate <name>' for non-interactive installs.");
        }
        Err(e) => return Err(e.into()),
    };

    let additional_selected: Vec<&AgentMenuEntry> = match inquire::MultiSelect::new(
        ADDITIONAL_AGENTS_LABEL,
        additional_labels.clone(),
    )
    .with_help_message("space to toggle, enter to confirm")
    .prompt()
    {
        Ok(chosen) => chosen
            .iter()
            .filter_map(|label| {
                additional_labels
                    .iter()
                    .position(|candidate| candidate == label)
                    .map(|idx| &additional_entries[idx])
            })
            .collect(),
        Err(inquire::InquireError::NotTTY) => {
            anyhow::bail!("Interactive prompts require a terminal. Use 'marrow integrate <name>' for non-interactive installs.");
        }
        Err(e) => return Err(e.into()),
    };

    let mut selected_entries: Vec<&AgentMenuEntry> = universal_selected;
    selected_entries.extend(additional_selected);

    let (mcp_selections, skill_selections, has_universal_no_mcp_target) =
        partition_agent_menu_entries(&selected_entries);

    if mcp_selections.is_empty() && skill_selections.is_empty() && !has_universal_no_mcp_target {
        eprintln!(
            "\n{}",
            style("No agents selected \u{2014} nothing to do.").dim()
        );
        return Ok(());
    }

    let has_mcp_with_rules = mcp_selections
        .iter()
        .any(|target| target.rule_agent.is_some());
    let has_skill_targets = !skill_selections.is_empty();
    let needs_rules = has_mcp_with_rules || has_skill_targets;

    let binary = std::env::current_exe()
        .context("Could not resolve current executable path")?
        .to_string_lossy()
        .to_string();
    let home = std::env::var("HOME").context("$HOME is not set")?;
    let home_path = PathBuf::from(&home);
    let ctx = IntegrationCtx {
        binary,
        home: home.clone(),
    };

    // Warn if `marrow` is not resolvable via the GUI-safe PATH we will inject.
    {
        let env_path = gui_safe_path(&ctx.binary);
        if let Err(e) = validate_marrow_command(&env_path) {
            eprintln!(
                "  {}  {}",
                style("\u{26a0}").yellow().bold(),
                style(format!("PATH warning: {e}")).yellow()
            );
            eprintln!(
                "  {}",
                style(
                    "Continuing install \u{2014} ensure `marrow` is on PATH before restarting your IDE."
                )
                .dim()
            );
            eprintln!();
        }
    }

    let (rule_scope, rule_method) = if needs_rules {
        if has_mcp_with_rules {
            eprintln!("  {}", style(rule_install_note()).dim());
        }

        let scope =
            match inquire::Select::new("Rule file scope", vec!["Global (recommended)", "Project"])
                .prompt()
            {
                Ok(choice) => {
                    if choice.starts_with("Global") {
                        skills::Scope::Global
                    } else {
                        skills::Scope::Project
                    }
                }
                Err(e) => return Err(e.into()),
            };

        let method = match inquire::Select::new("Rule file method", vec!["Write File", "Symlink"])
            .prompt()
        {
            Ok(choice) => {
                if choice == "Symlink" {
                    skills::Method::Symlink
                } else {
                    skills::Method::WriteFile
                }
            }
            Err(e) => return Err(e.into()),
        };

        eprintln!();
        eprintln!("  {}", style("Rule files to create:").dim());
        for target in &mcp_selections {
            if let Some(skill_agent) = rule_agent_for_scope(target, scope) {
                eprintln!(
                    "    {}",
                    style(format_rule_plan_line(
                        target.name,
                        skill_agent,
                        scope,
                        method,
                        &home_path
                    ))
                    .dim()
                );
            } else {
                eprintln!(
                    "    {}",
                    style(format!(
                        "{} -> guided setup only (no verified rule-file target for this scope)",
                        target.name
                    ))
                    .dim()
                );
            }
        }
        for &idx in &skill_selections {
            let st = &AGENT_SKILL_TARGETS[idx];
            eprintln!(
                "    {}",
                style(format!(
                    "{} \u{2192} {}/marrow-optimization.md",
                    st.name, st.skills_dir
                ))
                .dim()
            );
        }
        eprintln!(
            "  {}",
            style("Edit/remove the target paths above later if you want to disable implicit Marrow guidance.").dim()
        );
        (scope, method)
    } else {
        (skills::Scope::Project, skills::Method::WriteFile)
    };

    ensure_workspace_config()?;

    eprintln!();

    // Install universal skill (always, regardless of selections).
    match skills::install_skill_to_dir(".agents/skills", rule_scope, rule_method, &home_path) {
        Ok(status) => {
            eprintln!(
                "  {}  Universal \u{2192} .agents/skills/marrow-optimization.md ({})",
                style("\u{2713}").green().bold(),
                install_status_label(status),
            );
        }
        Err(e) => eprintln!(
            "  {}  Universal skill \u{2014} {}",
            style("\u{2717}").red().bold(),
            e,
        ),
    }

    // Loop MCP targets: register + optional rule files (unchanged logic).
    for target in mcp_selections {
        let mcp_result = register_integration_target(target, &ctx);
        match &mcp_result {
            Ok(AgentOutcome::Installed) => eprintln!(
                "  {}  {}  {}",
                style("\u{2713}").green().bold(),
                style(target.name).bold(),
                style("MCP registered").dim(),
            ),
            Ok(AgentOutcome::NotFound) => eprintln!(
                "  {}  {}  {}",
                style("\u{26a0}").yellow().bold(),
                style(target.name).dim(),
                style("(not installed \u{2014} skipped)").dim(),
            ),
            Ok(AgentOutcome::Guided) => eprintln!(
                "  {}  {}  {}",
                style("i").cyan().bold(),
                style(target.name).bold(),
                style(format_integration_guidance(target)).dim(),
            ),
            Err(e) => eprintln!(
                "  {}  {}  {}",
                style("\u{2717}").red().bold(),
                style(target.name).bold(),
                style(format!("MCP \u{2014} {e}")).red(),
            ),
        }

        if matches!(
            mcp_result,
            Ok(AgentOutcome::Installed) | Ok(AgentOutcome::Guided)
        ) {
            if let Some(skill_agent) = rule_agent_for_scope(target, rule_scope) {
                let rule_target = skill_agent.target_path(rule_scope, &home_path);
                match skills::install_skill(skill_agent, rule_scope, rule_method, &home_path) {
                    Ok(status) => {
                        eprintln!(
                            "  {}  {}",
                            style("\u{2713}").green().bold(),
                            style(format_rule_install_status_line(
                                target.name,
                                status,
                                &rule_target
                            ))
                            .dim(),
                        );
                        eprintln!(
                            "      {}",
                            style(skills::install_source_description(rule_method, &home_path))
                                .dim(),
                        );
                    }
                    Err(e) => eprintln!(
                        "  {}  {}  {}",
                        style("\u{2717}").red().bold(),
                        style(target.name).bold(),
                        style(format!("rules \u{2014} {e}")).red(),
                    ),
                }
            } else {
                eprintln!(
                    "  {}  {}  {}",
                    style("i").cyan().bold(),
                    style(target.name).bold(),
                    style("rules skipped \u{2014} no verified rule-file target for this scope")
                        .dim(),
                );
            }
        }
    }

    // Loop skill targets: install skill file only (no MCP registration).
    for idx in skill_selections {
        let st = &AGENT_SKILL_TARGETS[idx];
        if !st.scope_support.supports(rule_scope) {
            eprintln!(
                "  {}  {} \u{2014} skill target does not support this scope",
                style("i").cyan().bold(),
                st.name,
            );
            continue;
        }
        match skills::install_skill_to_dir(st.skills_dir, rule_scope, rule_method, &home_path) {
            Ok(status) => {
                eprintln!(
                    "  {}  {} \u{2192} {}/marrow-optimization.md ({})",
                    style("\u{2713}").green().bold(),
                    st.name,
                    st.skills_dir,
                    install_status_label(status),
                );
            }
            Err(e) => eprintln!(
                "  {}  {} \u{2014} {}",
                style("\u{2717}").red().bold(),
                st.name,
                e,
            ),
        }
    }

    eprintln!();
    eprintln!(
        "  {}",
        style(format_agent_coverage_summary(
            &current_workspace_root(),
            &home_path
        ))
        .dim()
    );
    eprintln!("  {}", style("Done.").bold());
    Ok(())
}

fn cmd_validate() -> Result<()> {
    let workspace_root = current_workspace_root();
    let home = std::env::var("HOME")
        .map(PathBuf::from)
        .context("$HOME is not set")?;
    let db_path =
        std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db(&db_path)?;
    println!(
        "{}",
        format_validation_report(&workspace_root, &home, &conn)
    );
    Ok(())
}

fn context_usage(program: &str) -> String {
    format!(
        "Usage: {program} context <task> --repo <repo_id> [--budget <tokens>] [--format markdown|json] [--profile local-8k|local-32k|cloud-cost-sensitive]"
    )
}

/// Resolve a user-supplied `--repo` value to a real `repositories.id` in the graph DB.
///
/// Accepts either a graph repo id (e.g. `Accrualify`) or a registry workspace_id
/// (e.g. `Accrualify-549bf296`). If the value does not match a row in
/// `repositories`, attempt to find the matching workspace in the registry and
/// resolve via its `workspace_root` basename. On total failure, emit a clear
/// error listing the available repo ids so the caller knows what to pass.
fn resolve_context_repo_id(conn: &rusqlite::Connection, requested: &str) -> Result<String> {
    let exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM repositories WHERE id = ?1",
            rusqlite::params![requested],
            |row| row.get(0),
        )
        .unwrap_or(0);
    if exists > 0 {
        return Ok(requested.to_string());
    }

    // Try registry resolution: caller may have passed a workspace_id whose graph
    // was ingested under a different repo_id (typically the workspace basename).
    if let Ok(registry) = registry::Registry::open_default() {
        if let Ok(Some(entry)) = registry.find_workspace(requested) {
            if let Some(basename) = entry
                .workspace_root
                .file_name()
                .and_then(|n| n.to_str())
            {
                let basename_exists: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM repositories WHERE id = ?1",
                        rusqlite::params![basename],
                        |row| row.get(0),
                    )
                    .unwrap_or(0);
                if basename_exists > 0 {
                    eprintln!(
                        "[marrow] context: resolved workspace_id '{}' to repo_id '{}' via registry",
                        requested, basename
                    );
                    return Ok(basename.to_string());
                }
            }
        }
    }

    // Strip a trailing `-<8 hex chars>` suffix and retry (workspace_id pattern).
    if let Some((prefix, suffix)) = requested.rsplit_once('-') {
        if suffix.len() == 8 && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
            let stripped_exists: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM repositories WHERE id = ?1",
                    rusqlite::params![prefix],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            if stripped_exists > 0 {
                eprintln!(
                    "[marrow] context: resolved workspace_id '{}' to repo_id '{}' (suffix stripped)",
                    requested, prefix
                );
                return Ok(prefix.to_string());
            }
        }
    }

    // Build a friendly error listing the available repo ids.
    let mut stmt =
        conn.prepare("SELECT id FROM repositories ORDER BY id")?;
    let available: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    if available.is_empty() {
        anyhow::bail!(
            "context: no repositories in graph database. Run `marrow index` first."
        );
    }
    anyhow::bail!(
        "context: --repo '{}' does not match any repo in the graph. Available: {}",
        requested,
        available.join(", ")
    );
}

fn cmd_context(program: &str, cli_args: &[String]) -> Result<()> {
    let mut task_parts = Vec::new();
    let mut repo_id: Option<String> = None;
    let mut budget_tokens: usize = 12_000;
    let mut format = context::ContextFormat::Markdown;
    let mut profile = context::ModelProfile::Local32k;

    let mut i = 0;
    while i < cli_args.len() {
        match cli_args[i].as_str() {
            "--help" | "-h" => {
                println!("{}", context_usage(program));
                return Ok(());
            }
            "--repo" | "--repo-id" => {
                i += 1;
                repo_id = Some(
                    cli_args
                        .get(i)
                        .ok_or_else(|| anyhow::anyhow!("context: --repo requires a value"))?
                        .clone(),
                );
            }
            "--budget" => {
                i += 1;
                let value = cli_args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("context: --budget requires a value"))?;
                budget_tokens = value
                    .parse::<usize>()
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or_else(|| {
                        anyhow::anyhow!("context: --budget must be a positive integer")
                    })?;
            }
            "--format" => {
                i += 1;
                let value = cli_args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("context: --format requires a value"))?;
                format = context::ContextFormat::parse(value)?;
            }
            "--profile" => {
                i += 1;
                let value = cli_args
                    .get(i)
                    .ok_or_else(|| anyhow::anyhow!("context: --profile requires a value"))?;
                profile = context::ModelProfile::parse(value)?;
            }
            other if other.starts_with('-') => {
                anyhow::bail!("context: unknown argument `{other}`");
            }
            value => task_parts.push(value.to_string()),
        }
        i += 1;
    }

    if task_parts.is_empty() {
        anyhow::bail!("{}", context_usage(program));
    }
    let repo_id = repo_id.ok_or_else(|| anyhow::anyhow!("context: --repo is required"))?;
    let task = task_parts.join(" ");
    let db_path =
        std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db_or_memory(&db_path)?;
    let repo_id = resolve_context_repo_id(&conn, &repo_id)?;
    let packet = context::compile_context_packet_for_format(
        &conn,
        context::ContextRequest {
            task,
            repo_id,
            budget_tokens,
            profile,
        },
        format,
    )?;

    match format {
        context::ContextFormat::Markdown => println!("{}", packet.to_markdown()),
        context::ContextFormat::Json => println!("{}", packet.to_json()?),
    }

    Ok(())
}

/// `marrow perf-harness` — MARROW-PERF-002: ingest via `run_ingestion`, then time capsule + impact.
fn cmd_perf_harness(cli_args: &[String]) -> Result<()> {
    if cli_args
        .first()
        .map(|s| s.as_str())
        .is_some_and(|s| s == "--help" || s == "-h")
    {
        eprintln!(
            "\
Usage: marrow perf-harness [options]

Options:
  --root <path>     Repo root to ingest (default: current directory)
  --repo-id <id>    Graph repo id (default: basename of --root)
  --db <path>       SQLite file (default: .marrow/perf-graph.db)
  --symbol <name>   Symbol for query phase (default: first symbol in graph)
  --fresh           Delete db and SQLite sidecars before ingest
    --precise-file-tokens
                                        Measure exact cl100k_base baseline tokens for touched files
  --json            Emit one JSON object on stdout; progress on stderr
  -h, --help        This message

See docs/perf-harness.md and docs/perf-baseline-runbook.md.
"
        );
        return Ok(());
    }

    let mut root = std::env::current_dir()?;
    let mut repo_id: Option<String> = None;
    let mut db_path = ".marrow/perf-graph.db".to_string();
    let mut symbol_override: Option<String> = None;
    let mut fresh = false;
    let mut json_mode = false;
    let mut precise_file_tokens = false;

    let mut i = 0usize;
    while i < cli_args.len() {
        match cli_args[i].as_str() {
            "--root" => {
                let v = cli_args
                    .get(i + 1)
                    .context("perf-harness: --root requires a path")?;
                root = PathBuf::from(v);
                i += 2;
            }
            "--repo-id" => {
                let v = cli_args
                    .get(i + 1)
                    .context("perf-harness: --repo-id requires a value")?;
                repo_id = Some(v.clone());
                i += 2;
            }
            "--db" => {
                let v = cli_args
                    .get(i + 1)
                    .context("perf-harness: --db requires a path")?;
                db_path = v.clone();
                i += 2;
            }
            "--symbol" => {
                let v = cli_args
                    .get(i + 1)
                    .context("perf-harness: --symbol requires a value")?;
                symbol_override = Some(v.clone());
                i += 2;
            }
            "--fresh" => {
                fresh = true;
                i += 1;
            }
            "--json" => {
                json_mode = true;
                i += 1;
            }
            "--precise-file-tokens" => {
                precise_file_tokens = true;
                i += 1;
            }
            other => {
                anyhow::bail!("perf-harness: unknown argument `{other}` (try --help)");
            }
        }
    }

    let root = root
        .canonicalize()
        .with_context(|| format!("perf-harness: cannot canonicalize {}", root.display()))?;

    let rid = repo_id.unwrap_or_else(|| {
        root.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unnamed")
            .to_string()
    });

    if fresh {
        let _ = fs::remove_file(&db_path);
        let _ = fs::remove_file(format!("{db_path}-wal"));
        let _ = fs::remove_file(format!("{db_path}-shm"));
    }

    if let Some(parent) = Path::new(&db_path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    eprintln!(
        "[perf-harness] repo_id={rid} root={} db={db_path}",
        root.display()
    );

    let t_ingest = Instant::now();
    let conn = db::init_db_or_memory(&db_path)?;
    let (symbols, edges) = ingestion::run_ingestion(&conn, &rid, &root)?;
    let ingest_wall_ms = t_ingest.elapsed().as_millis() as u64;

    let query_symbol = if let Some(s) = symbol_override {
        s
    } else {
        conn.query_row(
            "SELECT symbol_name FROM nodes WHERE repo_id = ?1 LIMIT 1",
            [&rid],
            |row| row.get::<_, String>(0),
        )
        .context(
            "perf-harness: no symbols in graph (empty repo or ingest produced no nodes); pass --symbol after a known ingest",
        )?
    };

    let t_query = Instant::now();
    let capsule = retrieval::get_context_capsule(&conn, &query_symbol, &rid, None)?;
    let _impact = retrieval::analyze_impact(&conn, &query_symbol, &rid, None)?;
    let query_wall_ms = t_query.elapsed().as_millis() as u64;
    let mut provenance = capsule.provenance.clone();
    let baseline_file_tokens = if precise_file_tokens {
        let measured =
            retrieval::measure_precise_tokens_touched_by_capsule(&conn, &query_symbol, &rid, None)?;
        if !measured.failed_paths.is_empty() {
            anyhow::bail!(
                "exact baseline unavailable: failed to tokenize touched file(s): {}",
                measured.failed_paths.join(", ")
            );
        }
        provenance.baseline_token_source = "exact".to_string();
        provenance.tokenizer_mode = measured.tokenizer_mode;
        provenance.precise_file_tokens = true;
        provenance.touched_file_count = measured.touched_file_count;
        measured.tokens
    } else {
        capsule.file_tokens
    };
    let capsule_tokens = count_tokens(&capsule.optimized_text)?;

    let db_file_bytes = fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
    let rss = rusage_max_rss_bytes();

    let git_head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string());

    let git_dirty = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .is_some_and(|o| !o.stdout.is_empty());

    let payload = serde_json::json!({
        "schema_version": 1u32,
        "repo_id": rid,
        "root": root.display().to_string(),
        "db_path": db_path,
        "ingest_wall_ms": ingest_wall_ms,
        "query_wall_ms": query_wall_ms,
        "symbols": symbols,
        "edges": edges,
        "query_symbol": query_symbol,
        "baseline_file_tokens": baseline_file_tokens,
        "capsule_tokens": capsule_tokens,
        "baseline_token_source": provenance.baseline_token_source,
        "tokenizer_mode": provenance.tokenizer_mode,
        "original_mode": provenance.original_mode,
        "proof_mode": provenance.proof_label,
        "precise_file_tokens": provenance.precise_file_tokens,
        "original_max_bytes": provenance.original_max_bytes,
        "proof_max_bytes": provenance.proof_max_bytes,
        "proof_max_files": provenance.proof_max_files,
        "touched_file_count": provenance.touched_file_count,
        "db_file_bytes": db_file_bytes,
        "rusage_max_rss_bytes": rss,
        "marrow_version": env!("CARGO_PKG_VERSION"),
        "marrow_git_sha": git_head,
        "marrow_git_dirty": git_dirty,
    });

    if json_mode {
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        eprintln!(
            "[perf-harness] ingest: {} ms | query: {} ms | symbols: {} | edges: {} | db_bytes: {} | ru_maxrss: {:?}",
            ingest_wall_ms,
            query_wall_ms,
            symbols,
            edges,
            db_file_bytes,
            rss
        );
        println!("{}", serde_json::to_string_pretty(&payload)?);
    }

    Ok(())
}

/// `marrow index` — same pipeline as MCP `ingest_repo` (`ingestion::run_ingestion`).
fn cmd_index(cli_args: &[String]) -> Result<()> {
    if cli_args
        .iter()
        .any(|a| a == "--help" || a == "-h" || a == "help")
    {
        println!(
            "Usage: marrow index\n\n\
             Index the current workspace using the same pipeline as MCP `ingest_repo`.\n\
             Repo id is derived from the workspace basename; the graph is written to .marrow/graph.db.\n\n\
             Options:\n  -h, --help    Show this message"
        );
        return Ok(());
    }
    if let Some(extra) = cli_args.iter().find(|a| a.starts_with('-')) {
        anyhow::bail!("index: unknown argument `{extra}` (try `marrow index --help`)");
    }
    let t0 = Instant::now();
    let cwd = std::env::current_dir()?;
    let root = cwd.canonicalize().unwrap_or_else(|_| cwd.clone());
    registry::register_workspace_best_effort(&root);
    let repo_id = root
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed")
        .to_string();

    let file_count = ingestion::collect_source_files(&root)?.len();
    eprintln!("Repo:  {repo_id}");
    eprintln!("Root:  {}", root.display());
    eprintln!("Files: {file_count} (discovered)");

    let db_path = ".marrow/graph.db";
    let conn = db::init_db_or_memory(db_path)?;
    let (symbol_count, edge_count) = ingestion::run_ingestion(&conn, &repo_id, &root)?;

    let elapsed = t0.elapsed();
    eprintln!("\n── Index complete ──────────────────────────────────────────");
    eprintln!("  Symbols: {}", fmt_num(symbol_count));
    eprintln!("  Edges:   {}", fmt_num(edge_count));
    eprintln!("  Time:    {:.2?}", elapsed);
    eprintln!("  DB:      {db_path}");

    Ok(())
}

// ── Standalone CLI commands (callable from the interactive menu) ──────────────

/// Write agent rules and VS Code Copilot config into the current workspace.
///
/// Presents an interactive sub-menu for:
///   Phase 1 — Agent selection (MultiSelect)
///   Phase 2 — Write mode (Select)
///   Phase 3 — Execution & summary
pub fn run_integrate_command(workspace_root: &Path) -> Result<()> {
    use console::style;
    use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};

    // ── Phase 1: Agent Selection ──────────────────────────────────────────────
    let workspace_targets = workspace_rule_targets();
    let mut agent_labels: Vec<String> = workspace_targets
        .iter()
        .map(|target| {
            format!(
                "{} ({})",
                target.name,
                target.workspace_rule_files.join(", ")
            )
        })
        .collect();
    let copilot_index = agent_labels.len();
    agent_labels.push("Copilot MCP (.vscode/mcp.json)".to_string());
    let defaults = vec![true; agent_labels.len()];
    let selected_agents = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Which agents do you want to integrate with?")
        .items(&agent_labels)
        .defaults(&defaults)
        .interact()?;

    if selected_agents.is_empty() {
        eprintln!(
            "{}",
            style("No agents selected. Aborting integration.").yellow()
        );
        return Ok(());
    }

    let rules_content: &str = WORKSPACE_RULES_CONTENT_SOFT;

    // ── Phase 3: Write Mode ───────────────────────────────────────────────────
    let write_options = &[
        "Safe Append  (Idempotent, preserves your existing custom rules)",
        "Overwrite    (Destructive, replaces file entirely)",
        "Symlink      (Links to a central ~/.marrow/global_rules.md)",
    ];
    let write_choice = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("How should the rule files be written?")
        .items(write_options)
        .default(0)
        .interact()?;
    let write_mode = match write_choice {
        1 => WriteMode::Overwrite,
        2 => WriteMode::Symlink,
        _ => WriteMode::SafeAppend,
    };

    // ── Phase 4: Execution ────────────────────────────────────────────────────
    eprintln!();
    let mut summary: Vec<String> = Vec::new();

    // Rule files for registry-backed workspace targets.
    let selected_rule_files: Vec<&str> = selected_agents
        .iter()
        .copied()
        .filter_map(|i| workspace_targets.get(i))
        .flat_map(|target| target.workspace_rule_files.iter().copied())
        .collect();
    if !selected_rule_files.is_empty() {
        match write_workspace_rule_files(
            workspace_root,
            &selected_rule_files,
            rules_content,
            write_mode,
        ) {
            Ok(modified) => summary.extend(modified),
            Err(e) => eprintln!("{}", style(format!("  ✗ Rule file error: {e}")).red()),
        }
    }

    // Copilot MCP config.
    if selected_agents.contains(&copilot_index) {
        match write_vscode_mcp_config(workspace_root, write_mode) {
            Ok(Some(path)) => summary.push(path),
            Ok(None) => {}
            Err(e) => eprintln!(
                "{}",
                style(format!("  ✗ Copilot MCP config error: {e}")).red()
            ),
        }
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    eprintln!();
    eprintln!(
        "{}",
        style("[Marrow] Integration complete. Files modified:")
            .green()
            .bold()
    );
    if summary.is_empty() {
        eprintln!("  {}", style("(none — all files already up to date)").dim());
    } else {
        for path in &summary {
            eprintln!("  {}  {}", style("✓").green().bold(), style(path).dim());
        }
    }
    Ok(())
}

/// Parse all source files in `workspace_root` and build the AST graph in SQLite.
/// Uses `ingestion::run_ingestion_with_progress` (same as MCP ingest).
pub fn run_index_command(workspace_root: &Path) -> Result<()> {
    use console::style;
    use indicatif::{ProgressBar, ProgressStyle};
    use std::time::Duration;

    let t0 = Instant::now();
    let root = workspace_root
        .canonicalize()
        .unwrap_or_else(|_| workspace_root.to_path_buf());
    registry::register_workspace_best_effort(&root);
    let repo_id = root
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed")
        .to_string();

    let file_count = ingestion::collect_source_files(&root)?.len();
    eprintln!(
        "{}",
        style(format!("[Marrow] Indexing {repo_id} — {file_count} files")).cyan()
    );

    let db_dir = workspace_root.join(".marrow");
    fs::create_dir_all(&db_dir)?;
    let db_path = db_dir.join("graph.db");
    let conn = db::init_db_or_memory(&db_path.to_string_lossy())?;

    let pb = ProgressBar::new(100);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {spinner:.green} [{bar:40.cyan/blue}] {pos}/100 {msg}",
        )
        .unwrap()
        .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ ")
        .progress_chars("=>-"),
    );
    pb.set_message("indexing");
    pb.enable_steady_tick(Duration::from_millis(120));

    let (symbol_count, edge_count) =
        ingestion::run_ingestion_with_progress(&conn, &repo_id, &root, |pct| {
            pb.set_position(pct as u64);
        })?;

    pb.finish_with_message("indexing complete");
    let elapsed = t0.elapsed();

    eprintln!(
        "{}",
        style(format!(
            "[Marrow] Index complete — {} symbols, {} edges in {:.2?}",
            fmt_num(symbol_count),
            fmt_num(edge_count),
            elapsed
        ))
        .green()
        .bold()
    );

    Ok(())
}

/// Perform an initial index, then watch the workspace for file saves and
/// incrementally re-index changed source files.
pub fn run_watch_command(workspace_root: &Path) -> Result<()> {
    use console::style;
    use notify::RecursiveMode;
    use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
    use std::time::Duration;

    registry::register_workspace_best_effort(workspace_root);
    eprintln!("{}", style("[Marrow] Building baseline index...").cyan());
    run_index_command(workspace_root)?;

    let db_path = workspace_root.join(".marrow/graph.db");
    let conn = Arc::new(Mutex::new(db::init_db_or_memory(
        &db_path.to_string_lossy(),
    )?));

    let (tx, rx) = std::sync::mpsc::channel();
    let mut debouncer = new_debouncer(Duration::from_millis(500), tx)?;
    debouncer
        .watcher()
        .watch(workspace_root, RecursiveMode::Recursive)?;

    eprintln!(
        "{}",
        style("[Marrow] Watching for changes. Press Ctrl+C to stop.")
            .green()
            .bold()
    );

    let supported_exts = [
        "cpp", "cc", "cxx", "h", "hpp", "py", "ts", "tsx", "rs", "rb",
    ];
    let repo_id = workspace_root
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed")
        .to_string();

    for result in rx {
        match result {
            Ok(events) => {
                for event in events {
                    if event.kind != DebouncedEventKind::Any {
                        continue;
                    }
                    let path = &event.path;
                    let ext_ok = path
                        .extension()
                        .and_then(|e| e.to_str())
                        .is_some_and(|e| supported_exts.contains(&e));
                    if !ext_ok {
                        continue;
                    }
                    let rel = path
                        .strip_prefix(workspace_root)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();
                    match ingestion::parse_file(path) {
                        Ok((lang, symbols)) => {
                            if let Ok(conn) = conn.lock() {
                                let node_id_prefix = format!("{repo_id}:{rel}:");
                                // Remove stale nodes for this file then re-insert.
                                let _ = conn.execute(
                                    "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
                                    rusqlite::params![repo_id, rel],
                                );
                                for sym in &symbols {
                                    let node_id = format!("{node_id_prefix}{}", sym.name);
                                    let _ = conn.execute(
                                        "INSERT OR REPLACE INTO nodes \
                                         (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text) \
                                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                                        rusqlite::params![
                                            node_id, repo_id, rel, lang,
                                            sym.name, sym.symbol_type, sym.raw_text
                                        ],
                                    );
                                }
                            }
                            eprintln!("{}", style(format!("[Marrow] Updated AST for {rel}")).dim());
                        }
                        Err(e) => {
                            eprintln!(
                                "{}",
                                style(format!("[Marrow] Parse error for {rel}: {e}")).yellow()
                            );
                        }
                    }
                }
            }
            Err(e) => eprintln!("{}", style(format!("[Marrow] Watch error: {e:?}")).red()),
        }
    }

    Ok(())
}

/// Interactive TUI — shown when `marrow` is run with no arguments.
fn cmd_interactive() -> Result<()> {
    use console::style;
    use dialoguer::{theme::ColorfulTheme, Select};

    // Clear terminal
    print!("\x1B[2J\x1B[1;1H");

    let art = r#"
  ███╗   ███╗ █████╗ ██████╗ ██████╗  ██████╗ ██╗    ██╗
  ████╗ ████║██╔══██╗██╔══██╗██╔══██╗██╔═══██╗██║    ██║
  ██╔████╔██║███████║██████╔╝██████╔╝██║   ██║██║ █╗ ██║
  ██║╚██╔╝██║██╔══██║██╔══██╗██╔══██╗██║   ██║██║███╗██║
  ██║ ╚═╝ ██║██║  ██║██║  ██║██║  ██║╚██████╔╝╚███╔███╔╝
  ╚═╝     ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝  ╚══╝╚══╝
"#;

    println!("{}", style(art).green().bold());
    println!("{}", style("  AST Context Engine for AI Agents\n").cyan());

    let items = [
        "1. Integrate Agents   (Configure MCP + rules)",
        "2. Index Workspace    (Build the AST graph once)",
        #[cfg(feature = "desktop")]
        "3. Desktop App        (Open native dashboard window)",
        #[cfg(feature = "desktop")]
        "4. Exit",
        #[cfg(not(feature = "desktop"))]
        "3. Exit",
    ];

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Welcome to Marrow. Select an action")
        .items(&items)
        .default(0)
        .interact()?;

    let workspace_root = current_workspace_root();

    match selection {
        0 => cmd_integrate(&[])?,
        1 => run_index_command(&workspace_root)?,
        #[cfg(feature = "desktop")]
        2 => cmd_desktop_submenu()?,
        _ => eprintln!("{}", style("Goodbye.").dim()),
    }

    Ok(())
}

/// Desktop App submenu for the interactive TUI.
#[cfg(feature = "desktop")]
fn cmd_desktop_submenu() -> Result<()> {
    use dialoguer::{theme::ColorfulTheme, Select};

    let items = [
        "Open       (Launch native dashboard window)",
        "Enable     (Register OS launcher entry)",
        "Disable    (Remove OS launcher entry)",
        "Status     (Show registration and process state)",
        "Back",
    ];

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Desktop App")
        .items(&items)
        .default(0)
        .interact()?;

    match selection {
        0 => ui_app::open_app()?,
        1 => ui_app::enable()?,
        2 => ui_app::disable()?,
        3 => ui_app::status()?,
        _ => {} // Back — return to main menu
    }

    Ok(())
}

// ── Daemon CLI helpers ────────────────────────────────────────────────────────

async fn cmd_status() -> Result<()> {
    let client = ipc::default_client();
    match client.health_check().await {
        Ok(true) => println!("[marrow] daemon is running."),
        Ok(false) => println!("[marrow] daemon is NOT running."),
        Err(e) => println!("[marrow] status check error: {e}"),
    }
    Ok(())
}

async fn cmd_stop() -> Result<()> {
    let client = ipc::default_client();
    if client.health_check().await.unwrap_or(false) {
        client.shutdown().await?;
        println!("[marrow] shutdown signal sent to daemon.");
    } else {
        println!("[marrow] daemon is not running.");
    }
    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() -> Result<()> {
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

    // Dispatch commands that MUST run on the main thread (macOS GUI requirement)
    // or that don't need async, before constructing any Tokio runtime.
    match args.get(1).map(|s| s.as_str()) {
        // Human interactive mode: no arguments → launch the TUI menu.
        None => return cmd_interactive(),
        // Help — no runtime needed.
        Some("--help") | Some("-h") | Some("help") => {
            println!("Usage: marrow [COMMAND]\n");
            println!("Commands:");
            println!("  (none)          Interactive TUI menu");
            println!("  mcp             Start MCP stdio server");
            println!("  index           Index current workspace");
            println!("  watch           Watch workspace for changes");
            println!("  init            Initialize workspace config");
            println!("  integrate       Install agent instruction files");
            println!("  validate        Check workspace setup");
            println!("  benchmark       Run token benchmark");
            println!("  context         Compile provider-neutral context packet");
            println!("  query           Query a symbol");
            println!("  maintenance     Checkpoint & vacuum database");
            println!("  daemon          Start background daemon or manage autostart");
            println!("  status          Show daemon status");
            println!("  stop            Stop daemon");
            println!("  ui              Open dashboard");
            println!("  ui-app          Desktop app (open|enable|disable|status)");
            println!("  perf-harness    Run performance benchmarks");
            println!("  service install Install daemon autostart (compatibility alias)");
            println!("\nOptions:");
            println!("  --help, -h      Show this help");
            return Ok(());
        }
        Some("ui-app") => {
            let subcmd = args.get(2).map(|s| s.as_str()).unwrap_or("open");
            match subcmd {
                "open" | "run" => return ui_app::open_app(),
                "enable" => return ui_app::enable(),
                "disable" => return ui_app::disable(),
                "status" => return ui_app::status(),
                _ => {
                    eprintln!("Usage: marrow ui-app [open|enable|disable|status]");
                    return Ok(());
                }
            }
        }
        _ => {}
    }

    // All remaining commands use async I/O — build the Tokio multithread runtime.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("Failed to build Tokio runtime")?;

    rt.block_on(async_main(args))
}

/// Async entry point for commands that need the Tokio multithread runtime.
/// Called from `main()` after GUI-related commands have been dispatched on
/// the process main thread.
async fn async_main(args: Vec<String>) -> Result<()> {
    match args.get(1).map(|s| s.as_str()) {
        // These cases are already dispatched from main() before the runtime is built.
        None | Some("--help") | Some("-h") | Some("help") | Some("ui-app") => {
            unreachable!("Dispatched from main() before Tokio runtime")
        }
        Some("ui") => return cmd_ui(),
        Some("init") => return cmd_init(),
        Some("rules") => return cmd_rules(),
        Some("index") => return cmd_index(&args[2..]),
        Some("test-capsules") => return cmd_test_capsules(),
        Some("perf-harness") => {
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            return cmd_perf_harness(&rest);
        }
        Some("context") => return cmd_context(&args[0], &args[2..]),
        Some("maintenance") => {
            let db_path =
                std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
            let conn = db::init_db_or_memory(&db_path)?;
            db::run_graph_maintenance(&conn)?;
            println!(
                "[marrow] maintenance complete (WAL checkpoint + incremental_vacuum) on {db_path}"
            );
            return Ok(());
        }
        Some("integrate") => return cmd_integrate(&args[2..]),
        Some("validate") => return cmd_validate(),
        Some("benchmark") => {
            let mut tail: Vec<String> = args.iter().skip(2).cloned().collect();
            let precise = tail
                .iter()
                .position(|a| a == "--precise-file-tokens")
                .map(|i| {
                    tail.remove(i);
                    true
                })
                .unwrap_or(false);

            if tail.is_empty() && !precise {
                let db_path = std::env::var("MARROW_DB_PATH")
                    .unwrap_or_else(|_| ".marrow/graph.db".to_string());
                if benchmark_prompts_available() {
                    let conn = db::init_db_or_memory(&db_path)?;
                    cmd_benchmark_wizard(&conn)?;
                    return Ok(());
                }
                anyhow::bail!("{}", benchmark_usage(&args[0]));
            }

            let symbol = tail
                .first()
                .ok_or_else(|| anyhow::anyhow!("{}", benchmark_usage(&args[0])))?;
            let repo_id = tail
                .get(1)
                .ok_or_else(|| anyhow::anyhow!("{}", benchmark_usage(&args[0])))?;

            let db_path =
                std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());

            let conn = db::init_db_or_memory(&db_path)?;
            run_benchmark(&conn, symbol, repo_id, None, precise)?;
            return Ok(());
        }
        Some("query") => {
            let symbol = args
                .get(2)
                .ok_or_else(|| anyhow::anyhow!("Usage: {} query <symbol> <repo_id>", args[0]))?;
            let repo_id = args
                .get(3)
                .ok_or_else(|| anyhow::anyhow!("Usage: {} query <symbol> <repo_id>", args[0]))?;

            let db_path =
                std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());

            let conn = db::init_db_or_memory(&db_path)?;
            let result = retrieval::get_context_capsule(&conn, symbol, repo_id, None)?;
            println!("{}", result.optimized_text);

            let impact = retrieval::analyze_impact(&conn, symbol, repo_id, None)?;
            println!("\nIMPACT ANALYSIS:");
            if impact.affected.is_empty() {
                println!("  No downstream dependents found.");
            } else {
                for n in impact.affected {
                    println!(
                        "  [Depth {}] {} ({}) in {}",
                        n.depth, n.symbol_name, n.symbol_type, n.file_path
                    );
                }
            }
            return Ok(());
        }
        Some("daemon") => match args.get(2).map(|s| s.as_str()) {
            None => return daemon::run().await,
            Some("install") => return service::install(),
            Some("uninstall") => return service::uninstall(),
            Some("status") => return service::status().await,
            _ => {
                eprintln!("Usage: marrow daemon [install|uninstall|status]");
                return Ok(());
            }
        },
        Some("status") => return cmd_status().await,
        Some("stop") => return cmd_stop().await,
        Some("watch") => {
            ipc::ensure_daemon_running().await?;
            let cwd = std::env::current_dir()?;
            registry::register_workspace_best_effort(&cwd);
            ipc::default_client().register_watch(&cwd).await?;
            println!("[marrow] watching {}", cwd.display());
            return Ok(());
        }
        Some("service") => {
            let subcmd = args.get(2).map(|s| s.as_str()).unwrap_or("");
            match subcmd {
                "install" => return service::install(),
                _ => {
                    eprintln!("Usage: marrow service install");
                    return Ok(());
                }
            }
        }
        // ── marrow mcp: start stdio MCP server (also boots daemon for file watching) ─
        Some("mcp") => {
            // Kick off the background daemon for file watching (best-effort; non-fatal).
            if let Err(e) = ipc::ensure_daemon_running().await {
                eprintln!("[marrow] daemon start warning (file watching unavailable): {e}");
            }
            // Fall through to the stdio MCP server below.
        }
        // Machine bypass: any unrecognised arg falls straight through to the
        // stdio server without showing the menu.
        Some(_) => {}
    }

    // ── Default: start MCP stdio server ──────────────────────────────
    let db_path =
        std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let workspace_root = current_workspace_root();
    let workspace_entry = registry::register_workspace_best_effort(&workspace_root);
    let activity_client = ipc::default_client();
    let mcp_activity_id = activity_client
        .start_activity(
            activity::ActivityKind::McpSession,
            workspace_entry.map(|entry| entry.workspace_id),
            format!("stdio {}", workspace_root.display()),
        )
        .await
        .ok()
        .flatten();

    // ── Read config flags in one pass ───────────────────────────────
    // Config read is always best-effort; a missing/unreadable file is not fatal.
    let (_show_dashboard, _auto_open_ui, enable_watcher, watch_debounce_ms) = {
        let cfg = read_workspace_config();
        let show = cfg
            .get("show_dashboard")
            .and_then(|b| b.as_bool())
            .unwrap_or(true);
        let open = cfg
            .get("auto_open_ui")
            .and_then(|b| b.as_bool())
            .unwrap_or(true);
        let watcher = cfg
            .get("enable_watcher")
            .and_then(|b| b.as_bool())
            .unwrap_or(false);
        let debounce = cfg
            .get("watch_debounce_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(500);
        (show, open, watcher, debounce)
    };

    // ── Init DB (falls back to :memory: on read-only filesystems) ─────
    let conn = db::init_db_or_memory(&db_path)?;
    let db_arc = Arc::new(Mutex::new(conn));

    // ── Create the HTTP client once — shared by Hub startup and engine ─
    let http_client = reqwest::Client::new();

    // ── Broadcast channel (shared by dashboard + watcher) ─────────────
    // Hoisted outside `if show_dashboard` so the watcher can use it even
    // when the dashboard UI is disabled.
    let (tx, _) = tokio::sync::broadcast::channel::<DashboardEvent>(256);
    let _session = Arc::new(Mutex::new(dashboard::SessionStats::default()));

    // ── Dashboard is now hosted by the daemon ──────────────────────
    // The MCP process no longer binds port 8765 or opens a browser.
    // The daemon (started via ensure_daemon_running above) serves the
    // dashboard at http://127.0.0.1:8765.

    // ── Background file watcher (opt-in) ──────────────────────────────
    if enable_watcher {
        match watcher::spawn_watcher(Arc::clone(&db_arc), tx.clone(), watch_debounce_ms) {
            Ok(_) => eprintln!("Marrow file watcher active (debounce: {watch_debounce_ms}ms)"),
            Err(e) => eprintln!("Marrow file watcher failed: {e}"),
        }
    }

    // ── Build engine ──────────────────────────────────────────────────
    let engine = ContextEngine {
        db: Arc::clone(&db_arc),
        http_client,
    };

    // Indexing is now decoupled — managed externally via `marrow index` / `marrow watch`.
    // Mark the state as Ready so run_pipeline tool calls are not blocked on MCP boot.
    set_index_state(IndexState::Ready);

    eprintln!("Marrow MCP server ready — listening on stdio.");
    let server_result: Result<()> = async {
        let server = engine.serve(stdio()).await?;
        server.waiting().await?;
        Ok(())
    }
    .await;

    if let Some(activity_id) = mcp_activity_id.as_deref() {
        let state = mcp_session_finish_state(&server_result);
        let detail = if server_result.is_ok() {
            "stdio session disconnected"
        } else {
            "stdio session error"
        };
        let _ = activity_client
            .finish_activity(activity_id, state, detail.to_string())
            .await;
    }

    server_result
}

fn mcp_session_finish_state(server_result: &Result<()>) -> activity::ActivityState {
    if server_result.is_ok() {
        activity::ActivityState::Stopped
    } else {
        activity::ActivityState::Error
    }
}
