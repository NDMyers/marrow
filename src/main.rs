mod daemon;
mod dashboard;
mod db;
mod ingestion;
mod ipc;
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

fn workspace_is_initialized(root: &Path) -> bool {
    let rules = [
        ".cursorrules",
        ".clinerules",
        ".roomrules",
        ".windsurfrules",
    ];
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

fn read_enforcement_mode() -> EnforcementMode {
    let cfg = read_workspace_config();
    EnforcementMode::from_config_value(cfg.get("enforcement_mode").and_then(|v| v.as_str()))
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
        return (
            "protected",
            format!("project instructions at {}", project_target.display()),
        );
    }
    if global_target.exists() && path_contains_marrow_marker(&global_target) {
        return (
            "protected",
            format!("global instructions at {}", global_target.display()),
        );
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
        std::fs::create_dir_all(root.join(".marrow")).map_err(|e| {
            eprintln!("[MARROW AUTO-INIT] Warning: could not create .marrow/: {e}");
            e
        })?;
        if let Err(e) = write_workspace_rules(
            &root,
            &[0, 1, 2],
            WORKSPACE_RULES_CONTENT,
            WriteMode::SafeAppend,
        ) {
            eprintln!("[MARROW AUTO-INIT] Warning: could not write workspace rules: {e}");
        }
        if let Err(e) = write_vscode_mcp_config(&root, WriteMode::SafeAppend) {
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
                 the full codebase, 'explore_symbol' to understand a specific symbol, \
                 'trace_flow' to linearly trace a symbol's outbound execution path without \
                 noisy inbound callers, 'refactor_symbol' to assess the blast radius of a change, \
                 or 'read_node' to expand a neighbor signature into its full source (use this \
                 after seeing a condensed signature in a previous explore_symbol response — \
                 never use native read_file for this).",
                Self::schema(json!({
                    "type": "object",
                    "properties": {
                        "intent": {
                            "type": "string",
                            "description": "Must be exactly 'analyze_repo', 'explore_symbol', 'trace_flow', 'refactor_symbol', or 'read_node'."
                        },
                        "target": {
                            "type": "string",
                            "description": "The symbol name or directory path relevant to the intent. \
                                            Required for explore_symbol, trace_flow, and refactor_symbol."
                        },
                        "repo_id": {
                            "type": "string",
                            "description": "The repository identifier. Auto-detected if omitted."
                        },
                        "filepath": {
                            "type": "string",
                            "description": "Relative file path to disambiguate symbols with identical names across files. \
                                            ALWAYS provide this for explore_symbol, trace_flow, and refactor_symbol when \
                                            you know which file the symbol is in (e.g. from the user's open editor tab, \
                                            cursor location, or a previous skeleton/search result). Required to resolve \
                                            a Disambiguation Payload — re-call run_pipeline with the same intent/target \
                                            plus the filepath from the payload instead of falling back to grep/read_file."
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
            let enforcement_mode = read_enforcement_mode();
            let compliance =
                match apply_compliance_gate(&original_tool_name, args, enforcement_mode) {
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

                    let mut out = String::new();

                    // Short-circuit: return the disambiguation payload if the symbol was ambiguous.
                    if let Some(payload) = result.pivot_id.strip_prefix("DISAMBIGUATION:") {
                        out.push_str(payload);
                        return Ok(CallToolResult::success(vec![Content::text(out)]));
                    }

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
                        if result.truncated {
                            writeln!(
                                out,
                                "\n[Note: impact list truncated at MARROW_IMPACT_MAX_ROWS ({}); raise for more rows.]",
                                retrieval::impact_max_rows()
                            )
                            .ok();
                        }
                    }

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

                    // Rule 1 (inside workspace) or Rule 4 (outside + confirmed): proceed
                    let repo_id_for_event = repo_id.clone();

                    let (symbols, edges) = tokio::task::spawn_blocking(move || {
                        ingestion::run_ingestion_with_arc(&db, &repo_id, &root_path, |_| {})
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
                        "analyze_repo" => {
                            let pipeline_t = Instant::now();
                            // M-18 FIX: Normalize `.` target to None (repo root).
                            let target_dir = match target.as_deref() {
                                Some(".") | Some("./") | Some("") => None,
                                other => other.map(String::from),
                            };
                            let target_dir_label = target_dir.clone().unwrap_or_else(|| "(workspace)".to_string());

                            trace!("analyze_repo: start — target_dir={target_dir_label}");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                let id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                trace!("analyze_repo: resolve_repo_id='{id}' [{:?}ms]", t.elapsed().as_millis());
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!("analyze_repo: db lock acquired [{:?}ms]", t_lock.elapsed().as_millis());

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_skel = Instant::now();
                                let skeleton = retrieval::get_project_skeleton(&conn, &repo_id, target_dir.as_deref())?;
                                trace!("analyze_repo: get_project_skeleton [{:?}ms] — {} chars", t_skel.elapsed().as_millis(), skeleton.len());

                                Ok::<_, anyhow::Error>((skeleton, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!("analyze_repo: spawn_blocking total [{:?}ms]", pipeline_t.elapsed().as_millis());

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

                        // "read_node" is a navigation alias for explore_symbol.
                        // Agents use it to "click" on a neighbor link from a
                        // previous Progressive Disclosure capsule response.
                        "explore_symbol" | "read_node" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'explore_symbol' requires a 'target' (symbol name)".to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event  = symbol_name.clone();

                            trace!("explore_symbol: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                let id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                trace!("explore_symbol: resolve_repo_id='{id}' [{:?}ms]", t.elapsed().as_millis());
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

                            trace!("explore_symbol: spawn_blocking total [{:?}ms]", pipeline_t.elapsed().as_millis());

                            let tokens_saved = file_tokens.saturating_sub(capsule_tokens);
                            let original_for_emit =
                                if retrieval::capsule_original_mode() == retrieval::CapsuleOriginalMode::None
                                    && original_text.is_empty()
                                {
                                    None
                                } else {
                                    Some(original_text)
                                };

                            let event = DashboardEvent::CapsuleServed {
                                symbol:         sym_for_event,
                                repo:           repo_used,
                                file:           abs_file_path,
                                capsule_tokens,
                                file_tokens,
                                tokens_saved,
                                origin:         client_name,
                                ts:             dashboard::now_ts(),
                                original_text:  original_for_emit,
                                optimized_text: Some(out.clone()),
                                proof_snapshot: proof_snapshot.map(Box::new),
                                provenance: Box::new(provenance),
                                has_cached_delta: false,
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

                        // ── trace_flow ────────────────────────────────────────
                        "trace_flow" => {
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'trace_flow' requires a 'target' (symbol name)".to_string(),
                                    None,
                                )
                            })?;
                            let sym_for_event = symbol_name.clone();

                            trace!("trace_flow: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                let id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                trace!("trace_flow: resolve_repo_id='{id}' [{:?}ms]", t.elapsed().as_millis());
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (out, capsule_tokens, file_tokens, abs_file_path, repo_used, provenance) =
                                tokio::task::spawn_blocking(move || {
                                    let t_lock = Instant::now();
                                    let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                    trace!("trace_flow: db lock acquired [{:?}ms]", t_lock.elapsed().as_millis());

                                    let cwd = current_workspace_root();
                                    let repo_id =
                                        ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                    let t_trace = Instant::now();
                                    let result = retrieval::trace_logic_flow(&conn, &symbol_name, &repo_id, filepath_arg.as_deref())?;
                                    let optimized_tokens = result.optimized_text.len() / 4;
                                    let file_tokens = result.file_tokens;
                                    let provenance = result.provenance;
                                    let out = result.optimized_text;
                                    trace!("trace_flow: trace_logic_flow [{:?}ms] — {}B", t_trace.elapsed().as_millis(), out.len());

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

                                    let saved = (file_tokens as i64).saturating_sub(optimized_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_requests",     1);
                                    let _ = db::increment_stat(&conn, "total_file_tokens",  file_tokens as i64);
                                    let _ = db::increment_stat(&conn, "total_tokens_saved", saved);

                                    Ok::<_, anyhow::Error>((out, optimized_tokens, file_tokens, abs_path_str, repo_id, provenance))
                                })
                                .await
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                                .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!("trace_flow: spawn_blocking total [{:?}ms]", pipeline_t.elapsed().as_millis());

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
                                original_text:  None,
                                optimized_text: Some(out.clone()),
                                proof_snapshot: None,
                                provenance: Box::new(provenance),
                                has_cached_delta: false,
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
                            let pipeline_t = Instant::now();
                            let symbol_name = target.ok_or_else(|| {
                                rmcp::ErrorData::invalid_params(
                                    "intent 'refactor_symbol' requires a 'target' (symbol name)".to_string(),
                                    None,
                                )
                            })?;
                            let sym_clone = symbol_name.clone();

                            trace!("refactor_symbol: start — symbol='{symbol_name}'");

                            let cwd = current_workspace_root();
                            let jit_repo_id = {
                                let t = Instant::now();
                                let conn = db.lock().map_err(|_| rmcp::ErrorData::internal_error("DB mutex poisoned".to_string(), None))?;
                                let id = resolve_request_repo_id(&conn, repo_id_arg.as_deref(), &cwd)
                                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                                trace!("refactor_symbol: resolve_repo_id='{id}' [{:?}ms]", t.elapsed().as_millis());
                                id
                            };
                            if let Some(msg) = self.maybe_jit_index(&jit_repo_id, &cwd) {
                                return Ok(CallToolResult::success(vec![Content::text(msg)]));
                            }

                            let (result, repo_used) = tokio::task::spawn_blocking(move || {
                                let t_lock = Instant::now();
                                let conn = db.lock().map_err(|_| anyhow::anyhow!("DB mutex poisoned"))?;
                                trace!("refactor_symbol: db lock acquired [{:?}ms]", t_lock.elapsed().as_millis());

                                let cwd = current_workspace_root();
                                let repo_id =
                                    ensure_repo_ready(&conn, repo_id_arg.as_deref(), &cwd)?;

                                let t_impact = Instant::now();
                                let result = retrieval::analyze_impact(&conn, &symbol_name, &repo_id, filepath_arg.as_deref())?;
                                trace!("refactor_symbol: analyze_impact [{:?}ms] — {} affected", t_impact.elapsed().as_millis(), result.affected.len());

                                Ok::<_, anyhow::Error>((result, repo_id))
                            })
                            .await
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?
                            .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;

                            trace!("refactor_symbol: spawn_blocking total [{:?}ms]", pipeline_t.elapsed().as_millis());

                            let mut out = String::new();

                            // Short-circuit: return the disambiguation payload if the symbol was ambiguous.
                            if let Some(payload) = result.pivot_id.strip_prefix("DISAMBIGUATION:") {
                                out.push_str(payload);
                                return Ok(CallToolResult::success(vec![Content::text(out)]));
                            }

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
                                if result.truncated {
                                    writeln!(
                                        out,
                                        "\n[Note: impact list truncated at MARROW_IMPACT_MAX_ROWS ({}); raise for more rows.]",
                                        retrieval::impact_max_rows()
                                    )
                                    .ok();
                                }
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
                            "Invalid intent. Must be 'analyze_repo', 'explore_symbol', 'trace_flow', or 'refactor_symbol'.".to_string(),
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
                        let workspace_root =
                            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                        write_workspace_rules(
                            &workspace_root,
                            &[0, 1, 2],
                            WORKSPACE_RULES_CONTENT,
                            WriteMode::SafeAppend,
                        )?;
                        write_vscode_mcp_config(&workspace_root, WriteMode::SafeAppend)?;
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

        let routed = apply_compliance_gate("get_context_capsule", args, EnforcementMode::Default)
            .expect("default mode should auto-route direct low-level calls");

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
        assert!(
            report.contains("Enforcement mode: strict"),
            "mode missing: {report}"
        );
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
    fn integrate_claude_writes_command_marrow_with_env_path() {
        let home = tempfile::tempdir().unwrap();
        let ctx = IntegrationCtx {
            binary: "/absolute/path/to/marrow".to_string(),
            home: home.path().to_string_lossy().into_owned(),
        };
        integrate_claude(&ctx).unwrap();
        let raw = std::fs::read_to_string(home.path().join(".claude.json")).unwrap();
        let cfg: serde_json::Value = serde_json::from_str(&raw).unwrap();
        let cmd = cfg["mcpServers"]["marrow"]["command"].as_str().unwrap();
        assert_eq!(cmd, "marrow", "command must be 'marrow', got: {cmd}");
        assert!(!cmd.starts_with('/'), "command must not be absolute path");
        assert!(
            cfg["mcpServers"]["marrow"]["env"]["PATH"].is_string(),
            "env.PATH must be present"
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
        // - Claude/Antigravity/Zed: use command:"marrow" (portable name) with env.PATH.
        // - Cursor/Copilot/Cline: use shell wrapper (/bin/zsh or /bin/bash) — never the binary.
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

        // Claude and Antigravity must use portable command name.
        let claude_raw = std::fs::read_to_string(h.join(".claude.json")).unwrap();
        let claude_cfg: serde_json::Value = serde_json::from_str(&claude_raw).unwrap();
        assert_eq!(claude_cfg["mcpServers"]["marrow"]["command"], "marrow");

        // Zed must use portable path name in nested command object.
        let zed_raw = std::fs::read_to_string(zed.join("settings.json")).unwrap();
        let zed_cfg: serde_json::Value = serde_json::from_str(&zed_raw).unwrap();
        assert_eq!(
            zed_cfg["context_servers"]["marrow"]["command"]["path"],
            "marrow"
        );

        // Shell-wrapper hosts must use a shell binary, not "marrow" directly.
        for (rel, ptr) in [
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

const WORKSPACE_RULES_CONTENT: &str = r#"# MARROW AST CONTEXT ENGINE - STRICT WORKFLOW PROTOCOL
You are equipped with the 'marrow' MCP server. You MUST adhere to the following strict workflow. Do NOT rely on your default file-reading tools.

## THE OMNI-TOOL (ALWAYS FIRST)
For EVERY coding task, exploration, or question, you MUST call the `run_pipeline` tool first.

### Intent Routing Guide

* IF USER SAYS: "Analyze this repo", "Explain the codebase"
  * ACTION: Call `run_pipeline` with `intent: "analyze_repo"`.

* IF USER SAYS: "Trace the execution of <symbol>", "What does <symbol> do step-by-step?", "Debug the flow of <symbol>"
  * ACTION: Call `run_pipeline` with `intent: "trace_flow"` and the target symbol.
  * (Use this for precise, linear logic tracing **without** noisy inbound callers.)

* IF USER SAYS: "What calls <symbol>?", "What is the architecture around <symbol>?"
  * ACTION: Call `run_pipeline` with `intent: "explore_symbol"` and the target.
  * (Use this to see the broader blast radius and inbound callers.)

* IF USER SAYS: "Refactor <symbol>", "Delete <symbol>"
  * ACTION: Call `run_pipeline` with `intent: "refactor_symbol"` and the target.

If any tool states the database is empty, immediately run `ingest_repo`.

### Progressive Disclosure (CRITICAL — READ THIS CAREFULLY)
Marrow uses **Progressive Disclosure** to keep context payloads small and prevent IDE truncation.

When `explore_symbol` returns a capsule:
- The **pivot symbol** (the one you asked about) contains its **full source code**.
- **Neighbor symbols** (outbound dependencies it calls) show only their **signature** (first line).
- This is intentional. You are seeing a navigation map, not the full tree.

**Rule:** If you see a neighbor signature and need its internal logic, you MUST call:
  `run_pipeline(intent: "read_node", target: "<SymbolName>")`

You are **strictly forbidden** from using `grep`, `read_file`, or any native tool to fetch neighbor bodies. This breaks the Marrow workflow and causes hallucinations from partial file reads.

### Handling Large Artifact Files (CRITICAL)
If Marrow returns a message like "Large tool result written to file: /tmp/marrow-....md":
1. You are **strictly forbidden** from guessing or summarizing from memory.
2. You MUST use your native `read_file` tool to read that specific `/tmp/marrow-....md` path.
3. Only after reading the artifact may you generate a response.

### Handling Ambiguity (CRITICAL)
If `run_pipeline` returns a "Disambiguation Payload" stating that multiple matches were found for your target:
1. DO NOT fall back to native tools like `grep` or `read_file`.
2. Look at the provided list of file paths in the error payload.
3. Immediately call `run_pipeline` again, passing the exact same `intent` and `target`, but this time include the correct `target_path` parameter to disambiguate.

### Output hygiene
Do **not** add a "Made-with: Cursor" tag (or similar editor or tool attribution) to commits, pull requests, READMEs, or other artifacts unless the user explicitly asks for it.
"#;

const WORKSPACE_RULES_CONTENT_SOFT: &str = r#"# MARROW AST CONTEXT ENGINE - WORKFLOW GUIDANCE
You are equipped with the 'marrow' MCP server. You may use the `run_pipeline` tool if you need broader context about this codebase — it provides richer, AST-aware analysis than default file-reading tools.

## THE OMNI-TOOL (OPTIONAL BUT RECOMMENDED)
For coding tasks involving exploration or refactoring, consider calling the `run_pipeline` tool for deeper context.

### Intent Routing Guide

* IF USER SAYS: "Analyze this repo", "Explain the codebase"
  * ACTION: You may call `run_pipeline` with `intent: "analyze_repo"`.

* IF USER SAYS: "Trace the execution of <symbol>", "What does <symbol> do step-by-step?", "Debug the flow of <symbol>"
  * ACTION: You may call `run_pipeline` with `intent: "trace_flow"` and the target symbol.
  * (Use this for precise, linear logic tracing **without** noisy inbound callers.)

* IF USER SAYS: "What calls <symbol>?", "What is the architecture around <symbol>?"
  * ACTION: You may call `run_pipeline` with `intent: "explore_symbol"` and the target.
  * (Use this to see the broader blast radius and inbound callers.)

* IF USER SAYS: "Refactor <symbol>", "Delete <symbol>"
  * ACTION: You may call `run_pipeline` with `intent: "refactor_symbol"` and the target.

If any tool states the database is empty, run `ingest_repo` to build the index.

### Progressive Disclosure
Marrow uses **Progressive Disclosure**: neighbor symbols in a capsule show signatures only.
To expand a neighbor into its full source, call:
  `run_pipeline(intent: "read_node", target: "<SymbolName>")`
Prefer this over using native `read_file` to stay within the Marrow context graph.

### Handling Ambiguity
If `run_pipeline` returns a "Disambiguation Payload" stating that multiple matches were found for your target:
1. Look at the provided list of file paths in the error payload.
2. Call `run_pipeline` again, passing the exact same `intent` and `target`, but this time include the correct `target_path` parameter to disambiguate.

### Output hygiene
Do **not** add a "Made-with: Cursor" tag (or similar editor or tool attribution) to commits, pull requests, READMEs, or other artifacts unless the user explicitly asks for it.
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

/// Write Marrow rule files for the selected agents.
///
/// `agent_indices` maps to:
///   0 → Cursor   (.cursorrules)
///   1 → Windsurf (.windsurfrules)
///   2 → Cline    (.clinerules, .roomrules)
///
/// Returns the list of file paths that were created, appended, or symlinked.
pub fn write_workspace_rules(
    root_dir: &Path,
    agent_indices: &[usize],
    rules_content: &str,
    mode: WriteMode,
) -> Result<Vec<String>> {
    use std::io::Write;
    const MARROW_HEADER: &str = "# MARROW AST CONTEXT ENGINE";

    // Index → file names. Cline shares .roomrules for Roo compatibility.
    const AGENT_FILES: &[&[&str]] = &[
        &[".cursorrules"],              // 0: Cursor
        &[".windsurfrules"],            // 1: Windsurf
        &[".clinerules", ".roomrules"], // 2: Cline + Roo
    ];

    let mut modified: Vec<String> = Vec::new();

    // For Symlink mode, ensure the central rules file exists once up-front.
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

    for &idx in agent_indices {
        let Some(files) = AGENT_FILES.get(idx) else {
            continue;
        };
        for &filename in *files {
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
                    // Remove existing file or stale symlink before creating the new one.
                    if path.exists() || path.is_symlink() {
                        fs::remove_file(&path).ok();
                    }
                    #[cfg(unix)]
                    {
                        std::os::unix::fs::symlink(central, &path).with_context(|| {
                            format!(
                                "could not symlink {} → {}",
                                path.display(),
                                central.display()
                            )
                        })?;
                        eprintln!("Symlinked {} → {}", path.display(), central.display());
                        modified.push(path.display().to_string());
                    }
                    #[cfg(not(unix))]
                    {
                        // Symlinks require elevated permissions on Windows; fall back to a copy.
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
    }

    Ok(modified)
}

fn cmd_rules() -> Result<()> {
    let root = std::env::current_dir().context("could not determine current directory")?;
    write_workspace_rules(
        &root,
        &[0, 1, 2],
        WORKSPACE_RULES_CONTENT,
        WriteMode::SafeAppend,
    )?;
    write_vscode_mcp_config(&root, WriteMode::SafeAppend)?;
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
    let mut spec = mcp_launch_spec();
    spec["env"] = serde_json::json!({ "PATH": gui_safe_path(&ctx.binary) });
    cfg["mcpServers"]["marrow"] = spec;
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

// ── Interactive installer ─────────────────────────────────────────────────────

/// `marrow integrate` — launch the interactive TUI installer.
fn cmd_integrate() -> Result<()> {
    use console::style;
    use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};

    eprintln!("{}", style(MARROW_BANNER).cyan().bold());
    eprintln!(
        "  {}",
        style("AST Context Engine  ·  MCP Server Installer").dim()
    );
    eprintln!();

    #[allow(clippy::type_complexity)]
    let agents: &[(
        &str,
        fn(&IntegrationCtx) -> Result<AgentOutcome>,
        skills::Agent,
    )] = &[
        ("Claude Code", integrate_claude, skills::Agent::ClaudeCode),
        (
            "Antigravity (Gemini)",
            integrate_antigravity,
            skills::Agent::Antigravity,
        ),
        ("Cursor", integrate_cursor, skills::Agent::Cursor),
        (
            "GitHub Copilot",
            integrate_copilot,
            skills::Agent::GitHubCopilot,
        ),
        ("Cline", integrate_cline, skills::Agent::Cline),
        ("Zed", integrate_zed, skills::Agent::Zed),
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

    // Warn if `marrow` is not resolvable via the GUI-safe PATH we will inject.
    // This converts a vague post-restart ENOENT into an immediate install-time diagnosis.
    {
        let env_path = gui_safe_path(&ctx.binary);
        if let Err(e) = validate_marrow_command(&env_path) {
            eprintln!(
                "  {}  {}",
                style("⚠").yellow().bold(),
                style(format!("PATH warning: {e}")).yellow()
            );
            eprintln!(
                "  {}",
                style(
                    "Continuing install — ensure `marrow` is on PATH before restarting your IDE."
                )
                .dim()
            );
            eprintln!();
        }
    }

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
                style(format_rule_plan_line(
                    name,
                    skill_agent,
                    scope,
                    method,
                    &home_path
                ))
                .dim()
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
        style(format!(
            "Workspace enforcement mode set to '{}'.",
            enforcement_mode.as_str()
        ))
        .dim()
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
    let mode = read_enforcement_mode();
    let db_path =
        std::env::var("MARROW_DB_PATH").unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let conn = db::init_db(&db_path)?;
    println!(
        "{}",
        format_validation_report(&workspace_root, &home, mode, &conn)
    );
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
fn cmd_index() -> Result<()> {
    let t0 = Instant::now();
    let cwd = std::env::current_dir()?;
    let root = cwd.canonicalize().unwrap_or_else(|_| cwd.clone());
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
///   Phase 2 — Rule strictness (Select; only when markdown-rule agents are chosen)
///   Phase 3 — Write mode (Select)
///   Phase 4 — Execution & summary
pub fn run_integrate_command(workspace_root: &Path) -> Result<()> {
    use console::style;
    use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};

    // ── Phase 1: Agent Selection ──────────────────────────────────────────────
    let agent_labels = &[
        "Cursor    (.cursorrules)",
        "Windsurf  (.windsurfrules)",
        "Cline     (.clinerules)",
        "Copilot MCP (.vscode/mcp.json)",
    ];
    let selected_agents = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Which agents do you want to integrate with?")
        .items(agent_labels)
        .defaults(&[true, true, true, true])
        .interact()?;

    if selected_agents.is_empty() {
        eprintln!(
            "{}",
            style("No agents selected. Aborting integration.").yellow()
        );
        return Ok(());
    }

    // ── Phase 2: Rule Strictness ──────────────────────────────────────────────
    // Only prompt when at least one markdown-rule agent (Cursor/Windsurf/Cline) is selected.
    let has_rule_agents = selected_agents.iter().any(|&i| i < 3);
    let rules_content: &str = if has_rule_agents {
        let strictness_options = &[
            "Strict  (Forces the agent to use Marrow's Omni-Tool exclusively)",
            "Soft    (Suggests Marrow as an optional, supplementary tool)",
        ];
        let strictness_choice = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("Select rule enforcement level")
            .items(strictness_options)
            .default(0)
            .interact()?;
        if strictness_choice == 0 {
            WORKSPACE_RULES_CONTENT
        } else {
            WORKSPACE_RULES_CONTENT_SOFT
        }
    } else {
        WORKSPACE_RULES_CONTENT
    };

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

    // Rule files for markdown-based agents (indices 0, 1, 2).
    let rule_agent_indices: Vec<usize> =
        selected_agents.iter().copied().filter(|&i| i < 3).collect();
    if !rule_agent_indices.is_empty() {
        match write_workspace_rules(
            workspace_root,
            &rule_agent_indices,
            rules_content,
            write_mode,
        ) {
            Ok(modified) => summary.extend(modified),
            Err(e) => eprintln!("{}", style(format!("  ✗ Rule file error: {e}")).red()),
        }
    }

    // Copilot MCP config (index 3).
    if selected_agents.contains(&3) {
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
        "1. Integrate Agents   (Generate rules & Copilot config)",
        "2. Index Workspace    (Build the AST graph once)",
        "3. Watch Workspace    (Index & listen for file changes)",
        "4. Start MCP Server   (Run stdio server manually)",
        #[cfg(feature = "desktop")]
        "5. Desktop App        (Open native dashboard window)",
        #[cfg(feature = "desktop")]
        "6. Exit",
        #[cfg(not(feature = "desktop"))]
        "5. Exit",
    ];

    let selection = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Welcome to Marrow. Select an action")
        .items(&items)
        .default(0)
        .interact()?;

    let workspace_root = current_workspace_root();

    match selection {
        0 => run_integrate_command(&workspace_root)?,
        1 => run_index_command(&workspace_root)?,
        2 => run_watch_command(&workspace_root)?,
        3 => {
            eprintln!(
                "{}",
                style("[Marrow] Starting MCP server... (tip: run 'marrow mcp' to bypass the menu)")
                    .yellow()
            );
            let current_exe = std::env::current_exe()?;
            std::process::Command::new(current_exe)
                .arg("mcp")
                .spawn()?
                .wait()?;
        }
        #[cfg(feature = "desktop")]
        4 => cmd_desktop_submenu()?,
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
            println!("  query           Query a symbol");
            println!("  maintenance     Checkpoint & vacuum database");
            println!("  daemon          Start background daemon");
            println!("  status          Show daemon status");
            println!("  stop            Stop daemon");
            println!("  ui              Open dashboard");
            println!("  ui-app          Desktop app (open|enable|disable|status)");
            println!("  perf-harness    Run performance benchmarks");
            println!("  service install Install as system service");
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
        Some("index") => return cmd_index(),
        Some("test-capsules") => return cmd_test_capsules(),
        Some("perf-harness") => {
            let rest: Vec<String> = args.iter().skip(2).cloned().collect();
            return cmd_perf_harness(&rest);
        }
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
        Some("integrate") => return cmd_integrate(),
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
        Some("daemon") => {
            return daemon::run().await;
        }
        Some("status") => return cmd_status().await,
        Some("stop") => return cmd_stop().await,
        Some("watch") => {
            ipc::ensure_daemon_running().await?;
            let cwd = std::env::current_dir()?;
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
    let server = engine.serve(stdio()).await?;
    server.waiting().await?;

    Ok(())
}
