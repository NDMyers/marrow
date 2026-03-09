use anyhow::{anyhow, Result};
use ignore::WalkBuilder;
use rayon::prelude::*;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use streaming_iterator::StreamingIterator;
use tree_sitter::{Language, Parser, Query, QueryCursor};

/// Parsed symbol extracted from a source file.
pub struct Symbol {
    pub name: String,
    pub symbol_type: String,
    pub raw_text: String,
}

/// Tree-sitter configuration for a supported language.
struct LangConfig {
    language: Language,
    query_src: &'static str,
}

thread_local! {
    /// Per-thread cache of (Parser, compiled symbol Query) keyed by file extension.
    /// Avoids re-compiling queries on every file parsed by rayon workers.
    static SYMBOL_PARSERS: RefCell<HashMap<String, (tree_sitter::Parser, tree_sitter::Query)>> =
        RefCell::new(HashMap::new());

    /// Per-thread cache of (Parser, compiled call Query) keyed by file extension.
    /// Avoids re-compiling call queries on every symbol during edge building.
    static CALL_PARSERS: RefCell<HashMap<String, (tree_sitter::Parser, tree_sitter::Query)>> =
        RefCell::new(HashMap::new());
}

/// Return the tree-sitter `Language` for a file extension.
/// Extracted from `lang_config_for_ext` so other modules (e.g. watcher) can
/// check parsability without needing the full query config.
pub fn language_for_ext(ext: &str) -> Option<Language> {
    match ext {
        "cpp" | "cc" | "cxx" | "h" | "hpp" => Some(tree_sitter_cpp::LANGUAGE.into()),
        "py" => Some(tree_sitter_python::LANGUAGE.into()),
        "ts" => Some(tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into()),
        "tsx" => Some(tree_sitter_typescript::LANGUAGE_TSX.into()),
        "rs" => Some(tree_sitter_rust::LANGUAGE.into()),
        "rb" => Some(tree_sitter_ruby::LANGUAGE.into()),
        _ => None,
    }
}

fn lang_config_for_ext(ext: &str) -> Option<LangConfig> {
    match ext {
        "cpp" | "cc" | "cxx" | "h" | "hpp" => Some(LangConfig {
            language: tree_sitter_cpp::LANGUAGE.into(),
            query_src: "(function_definition) @function\n(class_specifier) @class",
        }),
        "py" => Some(LangConfig {
            language: tree_sitter_python::LANGUAGE.into(),
            query_src: "(function_definition) @function\n(class_definition) @class",
        }),
        "ts" => Some(LangConfig {
            language: tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            query_src: concat!(
                "(function_declaration) @function\n",
                "(method_definition) @method\n",
                "(class_declaration) @class"
            ),
        }),
        "tsx" => Some(LangConfig {
            language: tree_sitter_typescript::LANGUAGE_TSX.into(),
            query_src: concat!(
                "(function_declaration) @function\n",
                "(method_definition) @method\n",
                "(class_declaration) @class"
            ),
        }),
        "rs" => Some(LangConfig {
            language: tree_sitter_rust::LANGUAGE.into(),
            query_src: concat!(
                "(function_item) @capture.func\n",
                "(struct_item) @capture.struct\n",
                "(trait_item) @capture.trait\n",
                "(impl_item) @capture.impl\n",
                "(enum_item) @capture.enum"
            ),
        }),
        "rb" => Some(LangConfig {
            language: tree_sitter_ruby::LANGUAGE.into(),
            query_src: concat!(
                "(method) @capture.method\n",
                "(singleton_method) @capture.method\n",
                "(class) @capture.class\n",
                "(module) @capture.module"
            ),
        }),
        _ => None,
    }
}

/// Recursively descend through C++ declarator chains to find the terminal identifier.
/// Handles chains like: function_declarator → pointer_declarator → identifier,
/// or qualified_identifier containing an identifier.
fn find_name_in_declarator<'a>(node: tree_sitter::Node<'a>, source: &[u8]) -> Option<String> {
    match node.kind() {
        "identifier" | "type_identifier" | "field_identifier" => {
            return Some(node.utf8_text(source).unwrap_or("unknown").to_string());
        }
        "qualified_identifier" => {
            // e.g. MyClass::myMethod — take the rightmost `name` field or last identifier child
            if let Some(name_node) = node.child_by_field_name("name") {
                return Some(name_node.utf8_text(source).unwrap_or("unknown").to_string());
            }
        }
        _ => {}
    }
    // Recurse into the `declarator` field if present (covers function_declarator,
    // pointer_declarator, reference_declarator, etc.)
    if let Some(inner) = node.child_by_field_name("declarator") {
        return find_name_in_declarator(inner, source);
    }
    None
}

/// Extract the symbol name from a captured AST node.
/// Tries the `name` field first, then descends into C++ declarator chains,
/// then falls back to scanning direct named children.
fn extract_symbol_name(node: &tree_sitter::Node, source: &[u8]) -> String {
    // Try the explicit "name" field (works for Python, TS, and most grammars)
    if let Some(name_node) = node.child_by_field_name("name") {
        return name_node.utf8_text(source).unwrap_or("unknown").to_string();
    }
    // C++ function_definition uses a "declarator" field instead of "name".
    // Recursively descend through the declarator chain to find the identifier.
    if let Some(decl) = node.child_by_field_name("declarator") {
        if let Some(name) = find_name_in_declarator(decl, source) {
            return name;
        }
    }
    // Final fallback: scan direct named children for identifier-like nodes
    for i in 0..node.named_child_count() {
        if let Some(child) = node.named_child(i as u32) {
            match child.kind() {
                "identifier" | "type_identifier" | "property_identifier" => {
                    return child.utf8_text(source).unwrap_or("unknown").to_string();
                }
                _ => {}
            }
        }
    }
    "anonymous".to_string()
}

/// Return a tree-sitter query string that captures call expressions for a
/// given file extension.  Each query exposes a `@callee` capture on the
/// identifier being invoked.
fn call_query_for_ext(ext: &str) -> Option<&'static str> {
    match ext {
        "py" => Some(concat!(
            "(call function: (identifier) @callee)\n",
            "(call function: (attribute attribute: (identifier) @callee))\n",
        )),
        "ts" | "tsx" => Some(concat!(
            "(call_expression function: (identifier) @callee)\n",
            "(call_expression function: (member_expression property: (property_identifier) @callee))\n",
        )),
        "cpp" | "cc" | "cxx" | "h" | "hpp" => Some(concat!(
            "(call_expression function: (identifier) @callee)\n",
            "(call_expression function: (qualified_identifier name: (identifier) @callee))\n",
            "(call_expression function: (field_expression field: (field_identifier) @callee))\n",
        )),
        "rs" => Some(concat!(
            "(call_expression function: (identifier) @callee)\n",
            "(call_expression function: (scoped_identifier name: (identifier) @callee))\n",
            "(call_expression function: (field_expression field: (field_identifier) @callee))\n",
        )),
        "rb" => Some("(call method: (identifier) @callee)\n"),
        _ => None,
    }
}

/// Parse `raw_text` with tree-sitter and extract all callee names from call
/// expressions.  Returns a deduplicated list.  Never panics — returns an
/// empty vec on parse failure or unsupported language.
pub fn extract_calls_from_symbol(raw_text: &str, lang_ext: &str) -> Vec<String> {
    let query_src = match call_query_for_ext(lang_ext) {
        Some(q) => q,
        None => return Vec::new(),
    };
    let language = match language_for_ext(lang_ext) {
        Some(l) => l,
        None => return Vec::new(),
    };

    CALL_PARSERS.with(|cache| {
        let mut map = cache.borrow_mut();
        if !map.contains_key(lang_ext) {
            let mut parser = Parser::new();
            if parser.set_language(&language).is_err() {
                return Vec::new();
            }
            let query = match Query::new(&language, query_src) {
                Ok(q) => q,
                Err(_) => return Vec::new(),
            };
            map.insert(lang_ext.to_string(), (parser, query));
        }
        let (parser, query) = map.get_mut(lang_ext).unwrap();
        parser.reset();

        let tree = match parser.parse(raw_text, None) {
            Some(t) => t,
            None => return Vec::new(),
        };

        let source_bytes = raw_text.as_bytes();
        let mut cursor = QueryCursor::new();
        let mut seen = std::collections::HashSet::new();
        let mut callees = Vec::new();
        let mut matches = cursor.matches(query, tree.root_node(), source_bytes);
        while let Some(m) = matches.next() {
            for capture in m.captures {
                if let Ok(name) = capture.node.utf8_text(source_bytes) {
                    let name = name.to_string();
                    if !name.is_empty() && seen.insert(name.clone()) {
                        callees.push(name);
                    }
                }
            }
        }
        callees
    })
}

const RAW_TEXT_MAX_BYTES: usize = 50_000; // ~50 KB (leaves room for sentinel)

/// Truncates `text` to `RAW_TEXT_MAX_BYTES` if it exceeds that threshold,
/// appending a sentinel comment so callers know the body is incomplete.
/// Full source is always available on disk via the node's `file_path`.
fn cap_raw_text(text: String) -> String {
    if text.len() <= RAW_TEXT_MAX_BYTES {
        return text;
    }
    // Truncate at a char boundary to avoid splitting UTF-8 sequences
    let end = text
        .char_indices()
        .take_while(|(i, _)| *i < RAW_TEXT_MAX_BYTES)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(RAW_TEXT_MAX_BYTES);
    format!(
        "{}\n# [MARROW: body truncated at 50KB — full source available in file]",
        &text[..end]
    )
}

/// Parse a single file and return its language tag plus extracted symbols.
pub fn parse_file(path: &Path) -> Result<(String, Vec<Symbol>)> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .ok_or_else(|| anyhow!("No file extension: {}", path.display()))?
        .to_string();

    let config =
        lang_config_for_ext(&ext).ok_or_else(|| anyhow!("Unsupported extension: {}", ext))?;

    let source = std::fs::read_to_string(path)?;
    let source_bytes = source.as_bytes();

    let symbols = SYMBOL_PARSERS.with(|cache| -> Result<Vec<Symbol>> {
        let mut map = cache.borrow_mut();
        if !map.contains_key(&ext) {
            let mut parser = Parser::new();
            parser.set_language(&config.language)?;
            let query = Query::new(&config.language, config.query_src)?;
            map.insert(ext.clone(), (parser, query));
        }
        let (parser, query) = map.get_mut(&ext).unwrap();
        parser.reset();

        let tree = parser
            .parse(&source, None)
            .ok_or_else(|| anyhow!("tree-sitter parse failed: {}", path.display()))?;

        let mut cursor = QueryCursor::new();
        let mut syms = Vec::new();
        let mut matches = cursor.matches(query, tree.root_node(), source_bytes);
        while let Some(m) = matches.next() {
            for capture in m.captures {
                let node = capture.node;
                let capture_name = query.capture_names()[capture.index as usize];
                let name = extract_symbol_name(&node, source_bytes);
                let raw_text = cap_raw_text(node.utf8_text(source_bytes).unwrap_or("").to_string());
                syms.push(Symbol {
                    name,
                    symbol_type: capture_name.to_string(),
                    raw_text,
                });
            }
        }
        Ok(syms)
    })?;

    Ok((ext, symbols))
}

/// Returns `false` for files that should never be parsed:
/// secrets/credentials, binary/minified assets, and lockfiles.
pub fn is_safe_to_parse(path: &Path) -> bool {
    let filename = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };

    // Lockfiles — exact filename match
    const LOCKFILES: &[&str] = &[
        "package-lock.json",
        "yarn.lock",
        "pnpm-lock.yaml",
        "Cargo.lock",
    ];
    if LOCKFILES.contains(&filename) {
        return false;
    }

    // Security exclusions — exact filename match
    const SECURITY_NAMES: &[&str] = &[".env", "id_rsa", "secrets.yml"];
    if SECURITY_NAMES.contains(&filename) {
        return false;
    }

    // Multi-component extension checks (e.g. foo.min.js, foo.tar.gz)
    if filename.ends_with(".min.js") || filename.ends_with(".tar.gz") {
        return false;
    }

    // Single-extension security and noise exclusions
    const BLOCKED_EXTENSIONS: &[&str] = &[
        "pem", "key", "pkcs12", "pfx", // credentials
        "map", "pdf", "png", "jpg", "jpeg", "gif", "webp", // binary/noise
        "zip", "gz", "tar",             // archives
        "sqlite", "db",                 // databases
    ];
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if BLOCKED_EXTENSIONS.contains(&ext) {
        return false;
    }

    true
}

/// Recursively collect all parseable source files under `root`, respecting
/// `.gitignore` rules and the hardcoded security/noise filter.
///
/// Uses `ignore::WalkParallel` with an mpsc channel to traverse the directory
/// tree concurrently, avoiding the bottleneck of a single-threaded walk on
/// large repositories.
pub fn collect_source_files(root: &Path) -> Vec<PathBuf> {
    use ignore::WalkState;

    let (tx, rx) = mpsc::channel();

    let walker = WalkBuilder::new(root)
        .hidden(true)       // skip dotfiles / hidden directories
        .git_ignore(true)   // respect .gitignore
        .git_exclude(true)  // respect .git/info/exclude
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            !matches!(
                name.as_ref(),
                "node_modules" | ".git" | "target" | "dist" | "build" | "vendor"
            )
        })
        .build_parallel();

    walker.run(|| {
        let tx = tx.clone();
        Box::new(move |result| {
            if let Ok(entry) = result {
                if entry.file_type().is_some_and(|ft| ft.is_file()) {
                    let path = entry.path();
                    if is_safe_to_parse(path) {
                        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                            if lang_config_for_ext(ext).is_some() {
                                let _ = tx.send(entry.into_path());
                            }
                        }
                    }
                }
            }
            WalkState::Continue
        })
    });

    // Drop the original sender so the receiver drains once all workers finish.
    drop(tx);

    rx.into_iter().collect()
}

fn maybe_emit_progress<F>(progress: &F, last_reported: &AtomicU8, next_percent: u8)
where
    F: Fn(u8) + Send + Sync,
{
    let next_percent = next_percent.min(100);
    let mut previous = last_reported.load(Ordering::SeqCst);
    while next_percent > previous {
        match last_reported.compare_exchange(
            previous,
            next_percent,
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                if last_reported.load(Ordering::SeqCst) == next_percent {
                    progress(next_percent);
                }
                break;
            }
            Err(actual) => previous = actual,
        }
    }
}

/// All data produced by the CPU-intensive parse phase.
/// Passed to the write phase so the DB lock is not held during file I/O
/// or tree-sitter work.
struct ComputedChangeset {
    /// (rel_path, lang_ext, symbols, content_hash, mtime_ns)
    parsed: Vec<(String, String, Vec<Symbol>, String, i64)>,
    /// Files whose mtime changed but content hash was identical (mtime-drift only).
    mtime_only: Vec<(String, i64)>,
    /// Relative paths of files that disappeared from disk since last index.
    removed_rels: Vec<String>,
}

// ── Phase A: brief DB read ────────────────────────────────────────────────────

/// Insert/update the repository record and return all known file metadata.
/// Holds the connection only for this short read — no I/O or CPU work.
fn load_known_files(
    conn: &Connection,
    repo_id: &str,
    root_path: &Path,
) -> Result<HashMap<String, (i64, String)>> {
    conn.execute(
        "INSERT OR REPLACE INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params![repo_id, root_path.to_string_lossy().as_ref()],
    )?;
    let mut stmt = conn.prepare(
        "SELECT file_path, mtime_secs, content_hash FROM files WHERE repo_id = ?1",
    )?;
    let rows: Vec<(String, i64, String)> = stmt
        .query_map(rusqlite::params![repo_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows.into_iter().map(|(path, mtime, hash)| (path, (mtime, hash))).collect())
}

// ── Phase B: pure CPU/IO — no DB connection held ──────────────────────────────

/// Walk the filesystem, hash changed files, and run tree-sitter in parallel.
/// No database access — safe to run while the DB mutex is released.
fn compute_changeset<F>(
    known_files: &HashMap<String, (i64, String)>,
    root_path: &Path,
    progress: &F,
    progress_state: &AtomicU8,
) -> ComputedChangeset
where
    F: Fn(u8) + Send + Sync,
{
    let disk_files = collect_source_files(root_path);
    maybe_emit_progress(progress, progress_state, 10);

    // Gather (abs_path, rel_path, mtime_ns) for every file on disk.
    // We store nanosecond precision so sub-second writes are correctly
    // detected. The column is named mtime_secs but holds nanoseconds;
    // SQLite INTEGER is 64-bit so values through year 2262 fit fine.
    let disk_meta: Vec<(PathBuf, String, i64)> = disk_files
        .iter()
        .filter_map(|path| {
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            let rel = canonical.strip_prefix(root_path).ok()?.to_string_lossy().to_string();
            let mtime = std::fs::metadata(path).ok()?.modified().ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            Some((path.clone(), rel, mtime))
        })
        .collect();

    // Determine which files need content-checking (mtime changed or new)
    let candidates: Vec<(PathBuf, String, i64)> = disk_meta
        .iter()
        .filter(|(_, rel, mtime)| match known_files.get(rel) {
            None => true,
            Some((km, _)) => km != mtime,
        })
        .cloned()
        .collect();

    // Read + hash candidates in parallel; keep only those whose content changed.
    let candidate_total = candidates.len().max(1);
    let changed: Vec<(String, PathBuf, i64, String)> = candidates
        .par_iter()
        .enumerate()
        .filter_map(|(idx, (path, rel, mtime))| {
            let bytes = std::fs::read(path).ok()?;
            let new_hash = crate::db::hash_file_content(&bytes);
            let percent = 10 + (((idx + 1) * 35) / candidate_total) as u8;
            maybe_emit_progress(progress, progress_state, percent);
            if let Some((_, known_hash)) = known_files.get(rel) {
                if *known_hash == new_hash {
                    return None; // content identical — mtime drift only
                }
            }
            Some((rel.clone(), path.clone(), *mtime, new_hash))
        })
        .collect();
    maybe_emit_progress(progress, progress_state, 45);

    // mtime-only files: candidates that didn't make it into `changed`
    let changed_rels: std::collections::HashSet<&str> =
        changed.iter().map(|(r, _, _, _)| r.as_str()).collect();
    let mtime_only: Vec<(String, i64)> = candidates
        .iter()
        .filter(|(_, rel, _)| !changed_rels.contains(rel.as_str()))
        .map(|(_, rel, mtime)| (rel.clone(), *mtime))
        .collect();

    // Parse changed files in parallel with tree-sitter
    let changed_total = changed.len().max(1);
    let parsed: Vec<(String, String, Vec<Symbol>, String, i64)> = changed
        .par_iter()
        .enumerate()
        .filter_map(|(idx, (rel, path, mtime, hash))| {
            let result = match parse_file(path) {
                Ok((lang, symbols)) => Some((rel.clone(), lang, symbols, hash.clone(), *mtime)),
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", path.display(), e);
                    None
                }
            };
            let percent = 45 + (((idx + 1) * 35) / changed_total) as u8;
            maybe_emit_progress(progress, progress_state, percent);
            result
        })
        .collect();
    maybe_emit_progress(progress, progress_state, 80);

    // Detect files removed from disk
    let disk_rels: std::collections::HashSet<&str> =
        disk_meta.iter().map(|(_, r, _)| r.as_str()).collect();
    let removed_rels: Vec<String> = known_files
        .keys()
        .filter(|fp| !disk_rels.contains(fp.as_str()))
        .cloned()
        .collect();

    ComputedChangeset { parsed, mtime_only, removed_rels }
}

// ── Phase C: brief DB write ───────────────────────────────────────────────────

/// Commit the computed changeset in a single transaction.
/// Returns (total_symbol_count, calls_edge_count).
///
/// Uses `BEGIN IMMEDIATE` so the write reservation is claimed up-front rather
/// than deferring the upgrade from read→write. This prevents the race where two
/// concurrent processes both start deferred transactions and then both try to
/// upgrade to writer simultaneously, causing `SQLITE_BUSY` for one of them.
fn write_changeset<F>(
    conn: &Connection,
    repo_id: &str,
    changeset: ComputedChangeset,
    progress: &F,
    progress_state: &AtomicU8,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    // BEGIN IMMEDIATE acquires a RESERVED lock immediately.
    // Other processes can still read but cannot write while this transaction runs.
    //
    // Retry loop: IDE MCP servers may hold a read lock between our attempts,
    // causing SQLITE_BUSY. Retry up to 20 times with 500 ms back-off before
    // giving up, rather than failing immediately.
    {
        let mut attempts = 0u32;
        loop {
            match conn.execute_batch("BEGIN IMMEDIATE") {
                Ok(_) => break,
                Err(rusqlite::Error::SqliteFailure(err, _))
                    if err.code == rusqlite::ErrorCode::DatabaseBusy =>
                {
                    attempts += 1;
                    if attempts > 20 {
                        return Err(anyhow::anyhow!(
                            "SQLite database is locked after 20 retries; \
                             another process may be holding a write lock"
                        ));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(500));
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    let result = write_changeset_body(conn, repo_id, changeset, progress, progress_state);

    match result {
        Ok(counts) => {
            conn.execute_batch("COMMIT")?;
            Ok(counts)
        }
        Err(e) => {
            let _ = conn.execute_batch("ROLLBACK");
            Err(e)
        }
    }
}

fn write_changeset_body<F>(
    conn: &Connection,
    repo_id: &str,
    changeset: ComputedChangeset,
    progress: &F,
    progress_state: &AtomicU8,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let ComputedChangeset { parsed, mtime_only, removed_rels } = changeset;

    // Remove nodes + file records for deleted files
    for file_path in &removed_rels {
        let syms: Vec<String> = {
            let mut s = conn.prepare(
                "SELECT symbol_name FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            )?;
            let collected: Vec<String> = s
                .query_map(rusqlite::params![repo_id, file_path], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();
            collected
        };
        for sym in &syms {
            crate::db::mark_deleted_observation_stale(conn, repo_id, sym, file_path)?;
        }
        conn.execute(
            "DELETE FROM edges WHERE source_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)
             OR target_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
            rusqlite::params![repo_id, file_path],
        )?;
        conn.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
        conn.execute(
            "DELETE FROM files WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
    }

    // Remove old nodes+edges for changed files (will be replaced below)
    for (file_path, _, _, _, _) in &parsed {
        conn.execute(
            "DELETE FROM edges WHERE source_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)
             OR target_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
            rusqlite::params![repo_id, file_path],
        )?;
        conn.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
    }

    // Insert fresh nodes for changed files.
    // Use prepare_cached so SQLite compiles the query plan once and reuses it
    // for every row, avoiding per-row re-compilation overhead.
    for (file_path, lang, symbols, hash, mtime) in &parsed {
        for sym in symbols {
            let node_id = format!("{}:{}:{}", repo_id, file_path, sym.name);
            conn.prepare_cached(
                "INSERT OR REPLACE INTO nodes \
                 (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?
            .execute(rusqlite::params![
                node_id, repo_id, file_path, lang,
                sym.name, sym.symbol_type, sym.raw_text
            ])?;
            let new_hash = crate::db::hash_raw_text(&sym.raw_text);
            crate::db::mark_stale_observations(conn, repo_id, &sym.name, file_path, &new_hash)?;
        }
        conn.execute(
            "INSERT OR REPLACE INTO files (repo_id, file_path, mtime_secs, content_hash)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![repo_id, file_path, mtime, hash],
        )?;
    }

    // Update mtime-only files (content unchanged, mtime drifted)
    for (rel, mtime) in &mtime_only {
        conn.execute(
            "UPDATE files SET mtime_secs = ?1 WHERE repo_id = ?2 AND file_path = ?3",
            rusqlite::params![mtime, repo_id, rel],
        )?;
    }
    maybe_emit_progress(progress, progress_state, 90);

    // Build CALLS edges for changed files
    let mut name_to_ids: HashMap<String, Vec<String>> = HashMap::new();
    for (file_path, _, symbols, _, _) in &parsed {
        for sym in symbols {
            let node_id = format!("{}:{}:{}", repo_id, file_path, sym.name);
            name_to_ids.entry(sym.name.clone()).or_default().push(node_id);
        }
    }
    // Also pull in existing unchanged nodes for cross-file call resolution
    {
        let mut stmt = conn.prepare(
            "SELECT symbol_name, id FROM nodes WHERE repo_id = ?1",
        )?;
        stmt.query_map(rusqlite::params![repo_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .for_each(|(name, id)| {
            name_to_ids.entry(name).or_default().push(id);
        });
    }

    let mut calls_edge_count = 0usize;
    for (file_path, lang, symbols, _, _) in &parsed {
        for sym in symbols {
            let callees = extract_calls_from_symbol(&sym.raw_text, lang);
            let source_id = format!("{}:{}:{}", repo_id, file_path, sym.name);
            for callee_name in &callees {
                if callee_name == &sym.name {
                    continue;
                }
                if let Some(target_ids) = name_to_ids.get(callee_name.as_str()) {
                    for target_id in target_ids {
                        conn.prepare_cached(
                            "INSERT OR IGNORE INTO edges \
                             (source_id, target_id, relationship_type) \
                             VALUES (?1, ?2, 'CALLS')",
                        )?
                        .execute(rusqlite::params![source_id, target_id])?;
                        calls_edge_count += 1;
                    }
                }
            }
        }
    }

    maybe_emit_progress(progress, progress_state, 95);

    let total: usize = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get::<_, i64>(0),
    )? as usize;

    Ok((total, calls_edge_count))
}

// ── Composed entry points ─────────────────────────────────────────────────────

fn ingest_repo_with_progress<F>(
    conn: &Connection,
    repo_id: &str,
    root_path: &Path,
    progress: &F,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let root_path = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());

    if !root_path.exists() {
        return Err(anyhow!("The specified root_path does not exist: {}", root_path.display()));
    }

    let progress_state = AtomicU8::new(0);
    maybe_emit_progress(progress, &progress_state, 5);

    let known_files = load_known_files(conn, repo_id, &root_path)?;
    let changeset = compute_changeset(&known_files, &root_path, progress, &progress_state);
    write_changeset(conn, repo_id, changeset, progress, &progress_state)
}

/// Ingest an entire repository incrementally: only re-parse files whose
/// content hash has changed since the last index run. First-time ingest
/// is a full pass. Returns `(total_symbol_count, calls_edge_count)`.
#[allow(dead_code)] // retained for tests and direct/manual ingestion entry points
pub fn ingest_repo(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<(usize, usize)> {
    ingest_repo_with_progress(conn, repo_id, root_path, &|_| {})
}

/// Combined ingestion pipeline: parse all files in `root_path` under `repo_id`,
/// then resolve cross-repo edges in a single call.
///
/// Both the explicit `ingest_repo` MCP tool handler and the JIT auto-indexer
/// call this function so the full pipeline is never duplicated.
/// Returns `(symbol_count, edge_count)`.
#[allow(dead_code)] // retained for tests and direct ingestion entry points
pub fn run_ingestion(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<(usize, usize)> {
    run_ingestion_with_progress(conn, repo_id, root_path, |_| {})
}

#[allow(dead_code)] // retained for tests and direct ingestion entry points
pub fn run_ingestion_with_progress<F>(
    conn: &Connection,
    repo_id: &str,
    root_path: &Path,
    progress: F,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let (symbols, calls_edges) = ingest_repo_with_progress(conn, repo_id, root_path, &progress)?;
    maybe_emit_progress(&progress, &AtomicU8::new(95), 95);
    let import_edges = resolve_cross_repo_edges(conn)?;
    maybe_emit_progress(&progress, &AtomicU8::new(95), 100);
    crate::db::vacuum_and_checkpoint(conn)?;
    Ok((symbols, calls_edges + import_edges))
}

/// Arc-based ingestion pipeline that releases the DB mutex between phases.
///
/// Unlike `run_ingestion_with_progress`, this function holds the lock only
/// for brief read and write windows, releasing it during the CPU/IO-intensive
/// parallel parse phase. This prevents the boot-time indexer from starving
/// concurrent tool calls that also need the DB.
pub fn run_ingestion_with_arc<F>(
    db: &Arc<Mutex<Connection>>,
    repo_id: &str,
    root_path: &Path,
    progress: F,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let root_path = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());

    if !root_path.exists() {
        return Err(anyhow!("The specified root_path does not exist: {}", root_path.display()));
    }

    let progress_state = AtomicU8::new(0);

    // Phase A: brief DB read — lock acquired, then immediately released.
    let known_files = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        maybe_emit_progress(&progress, &progress_state, 5);
        load_known_files(&conn, repo_id, &root_path)?
    };

    // Phase B: pure CPU/IO — DB mutex is NOT held.
    let changeset = compute_changeset(&known_files, &root_path, &progress, &progress_state);

    // Phase C: brief DB write — lock acquired, then released.
    let (total, calls_edges) = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        write_changeset(&conn, repo_id, changeset, &progress, &progress_state)?
    };

    // Phase D: cross-repo edges + vacuum — brief lock.
    let import_edges = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        maybe_emit_progress(&progress, &progress_state, 95);
        let edges = resolve_cross_repo_edges(&conn)?;
        crate::db::vacuum_and_checkpoint(&conn)?;
        maybe_emit_progress(&progress, &progress_state, 100);
        edges
    };

    Ok((total, calls_edges + import_edges))
}

/// Secondary pass: resolve cross-repo import edges.
/// Scans node raw_text for import-like patterns and creates IMPORTS edges
/// when the imported symbol exists in another repo's nodes.
pub fn resolve_cross_repo_edges(conn: &Connection) -> Result<usize> {
    // Load all nodes once
    let mut stmt = conn.prepare("SELECT id, repo_id, raw_text, language FROM nodes")?;
    let rows: Vec<(String, String, String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)))?
        .filter_map(|r| r.ok())
        .collect();

    if rows.is_empty() {
        return Ok(0);
    }

    // Pass 1 — collect all imports in memory
    // import_name -> Vec<(source_id, source_repo_id)>
    let mut import_map: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for (source_id, source_repo, raw_text, lang) in &rows {
        for name in extract_imports(raw_text, lang) {
            import_map
                .entry(name)
                .or_default()
                .push((source_id.clone(), source_repo.clone()));
        }
    }

    if import_map.is_empty() {
        return Ok(0);
    }

    // Pass 2 — single bulk query per 999-name chunk
    // target_name -> Vec<(node_id, repo_id)>
    let mut target_map: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();

    let all_names: Vec<&String> = import_map.keys().collect();
    for chunk in all_names.chunks(999) {
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT symbol_name, id, repo_id FROM nodes WHERE symbol_name IN ({placeholders})"
        );
        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .map(|s| rusqlite::types::Value::Text(s.to_string()))
            .collect();
        conn.prepare(&sql)?
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
            })?
            .filter_map(|r| r.ok())
            .for_each(|(name, id, repo)| {
                target_map.entry(name).or_default().push((id, repo));
            });
    }

    // Pass 3 — resolve edges in memory, insert in one transaction
    let tx = conn.unchecked_transaction()?;
    let mut edge_count = 0;

    for (import_name, sources) in &import_map {
        let Some(targets) = target_map.get(import_name) else {
            continue;
        };
        for (source_id, source_repo) in sources {
            // Only cross-repo targets
            let cross_repo: Vec<&String> = targets
                .iter()
                .filter(|(_, target_repo)| target_repo != source_repo)
                .map(|(id, _)| id)
                .collect();
            // Skip ambiguous (multiple targets across repos)
            if cross_repo.len() == 1 {
                tx.execute(
                    "INSERT OR IGNORE INTO edges (source_id, target_id, relationship_type)
                     VALUES (?1, ?2, 'IMPORTS')",
                    rusqlite::params![source_id, cross_repo[0]],
                )?;
                edge_count += 1;
            }
        }
    }

    tx.commit()?;
    Ok(edge_count)
}

/// Extract imported symbol names from raw source text based on language.
fn extract_imports(raw_text: &str, lang: &str) -> Vec<String> {
    let mut imports = Vec::new();
    for line in raw_text.lines() {
        let trimmed = line.trim();
        match lang {
            "py" => {
                if let Some(rest) = trimmed.strip_prefix("from ") {
                    // "from X import Y, Z"
                    if let Some((_module, after_import)) = rest.split_once(" import ") {
                        for name in after_import.split(',') {
                            let name = name.trim().split(" as ").next().unwrap_or("").trim();
                            if !name.is_empty() {
                                imports.push(name.to_string());
                            }
                        }
                    }
                } else if let Some(rest) = trimmed.strip_prefix("import ") {
                    // "import foo.bar" -> "bar"
                    for name in rest.split(',') {
                        let name = name.trim().split(" as ").next().unwrap_or("").trim();
                        if !name.is_empty() {
                            let last = name.rsplit('.').next().unwrap_or(name);
                            imports.push(last.to_string());
                        }
                    }
                }
            }
            "ts" | "tsx" => {
                // "import { X, Y } from '...'"
                if trimmed.starts_with("import ") {
                    if let Some(start) = trimmed.find('{') {
                        if let Some(end) = trimmed.find('}') {
                            let names = &trimmed[start + 1..end];
                            for name in names.split(',') {
                                let name = name.trim().split(" as ").next().unwrap_or("").trim();
                                if !name.is_empty() {
                                    imports.push(name.to_string());
                                }
                            }
                        }
                    }
                }
            }
            "cpp" | "cc" | "cxx" | "h" | "hpp" => {
                // #include "X.h" -> "X"
                if let Some(rest) = trimmed.strip_prefix("#include") {
                    let rest = rest.trim();
                    if let Some(inner) = rest.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                        let stem = Path::new(inner)
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .unwrap_or(inner);
                        imports.push(stem.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    imports
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_call_query_parses_all_languages() {
        let exts = ["py", "ts", "tsx", "cpp", "rs", "rb"];
        for ext in exts {
            let lang = language_for_ext(ext).expect(&format!("no language for {ext}"));
            let qsrc = call_query_for_ext(ext).expect(&format!("no call query for {ext}"));
            Query::new(&lang, qsrc).expect(&format!("query parse failed for {ext}"));
        }
    }

    #[test]
    fn test_extract_calls_python() {
        let src = r#"
def foo():
    bar()
    obj.method()
    baz(1, 2)
"#;
        let calls = extract_calls_from_symbol(src, "py");
        assert!(calls.contains(&"bar".to_string()), "missing bar: {calls:?}");
        assert!(calls.contains(&"method".to_string()), "missing method: {calls:?}");
        assert!(calls.contains(&"baz".to_string()), "missing baz: {calls:?}");
    }

    #[test]
    fn test_extract_calls_rust() {
        let src = r#"
fn foo() {
    helper();
    obj.method();
    std::io::read();
}
"#;
        let calls = extract_calls_from_symbol(src, "rs");
        assert!(calls.contains(&"helper".to_string()), "missing helper: {calls:?}");
        assert!(calls.contains(&"method".to_string()), "missing method: {calls:?}");
        assert!(calls.contains(&"read".to_string()), "missing read: {calls:?}");
    }

    #[test]
    fn test_extract_calls_typescript() {
        let src = r#"
function foo() {
    bar();
    obj.method();
}
"#;
        let calls = extract_calls_from_symbol(src, "ts");
        assert!(calls.contains(&"bar".to_string()), "missing bar: {calls:?}");
        assert!(calls.contains(&"method".to_string()), "missing method: {calls:?}");
    }

    #[test]
    fn test_ingest_creates_calls_edges() {
        let conn = crate::db::init_db(":memory:").unwrap();

        // Insert two symbols: caller calls callee
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        ).unwrap();

        // Create a temp dir with two Python files
        let dir = std::env::temp_dir().join("marrow_test_calls");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("callee.py"), "def helper():\n    pass\n").unwrap();
        std::fs::write(dir.join("caller.py"), "def main():\n    helper()\n").unwrap();

        let (syms, calls) = ingest_repo(&conn, repo_id, &dir).unwrap();
        assert!(syms >= 2, "expected at least 2 symbols, got {syms}");
        assert!(calls >= 1, "expected at least 1 CALLS edge, got {calls}");

        // Verify edge exists in DB
        let edge_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM edges WHERE relationship_type = 'CALLS'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert!(edge_count >= 1, "no CALLS edges in DB");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_reingest_clears_stale_calls_edges() {
        let conn = crate::db::init_db(":memory:").unwrap();

        let repo_id = "test";
        let dir = std::env::temp_dir().join("marrow_test_reingest");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // First ingest: caller calls helper
        std::fs::write(dir.join("callee.py"), "def helper():\n    pass\n").unwrap();
        std::fs::write(dir.join("caller.py"), "def main():\n    helper()\n").unwrap();

        let (_syms, calls1) = ingest_repo(&conn, repo_id, &dir).unwrap();
        assert!(calls1 >= 1);

        // Second ingest: caller no longer calls helper
        std::fs::write(dir.join("caller.py"), "def main():\n    pass\n").unwrap();

        let (_syms, calls2) = ingest_repo(&conn, repo_id, &dir).unwrap();
        assert_eq!(calls2, 0, "stale CALLS edge should have been cleared");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_reingest_removes_nodes_for_deleted_files() {
        let conn = crate::db::init_db(":memory:").unwrap();

        let repo_id = "test";
        let dir = std::env::temp_dir().join("marrow_test_deleted_files");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("keeper.py"), "def keep():\n    pass\n").unwrap();
        std::fs::write(dir.join("stale.py"), "def stale():\n    pass\n").unwrap();

        let (_syms, _calls) = ingest_repo(&conn, repo_id, &dir).unwrap();
        let before: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1 AND file_path = 'stale.py'",
                rusqlite::params![repo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(before, 1, "expected stale.py to be indexed before deletion");

        std::fs::remove_file(dir.join("stale.py")).unwrap();
        let (_syms, _calls) = ingest_repo(&conn, repo_id, &dir).unwrap();

        let after: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1 AND file_path = 'stale.py'",
                rusqlite::params![repo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(after, 0, "deleted file nodes should be removed on reingest");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn extract_calls_repeated_invocations_are_consistent() {
        // Validates that thread-local parser reuse doesn't corrupt state between calls.
        // Uses explicit Ruby call syntax (bar(), baz()) so tree-sitter-ruby parses them
        // as `call` nodes rather than ambiguous local-variable `identifier` nodes.
        let src = "def foo\n  bar()\n  baz()\nend\n";
        let first  = extract_calls_from_symbol(src, "rb");
        let second = extract_calls_from_symbol(src, "rb");
        let third  = extract_calls_from_symbol(src, "rb");
        assert_eq!(first, second, "repeated calls must return same result");
        assert_eq!(second, third, "repeated calls must return same result");
        assert!(first.contains(&"bar".to_string()), "bar must be detected: {first:?}");
        assert!(first.contains(&"baz".to_string()), "baz must be detected: {first:?}");
    }

    #[test]
    fn raw_text_cap_applied_to_oversized_symbols() {
        // Build a string just over 50KB
        let big_body = "x".repeat(51_200);
        let capped = cap_raw_text(big_body.clone());
        assert!(
            capped.len() < big_body.len(),
            "oversized raw_text should be truncated"
        );
        assert!(
            capped.contains("[MARROW: body truncated"),
            "truncated text should contain sentinel: {capped}"
        );
    }

    #[test]
    fn raw_text_cap_passes_small_symbols_unchanged() {
        let small = "def foo\n  42\nend\n".to_string();
        let result = cap_raw_text(small.clone());
        assert_eq!(result, small, "small raw_text should be unchanged");
    }

    #[test]
    fn second_ingest_skips_unchanged_files() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_incremental_skip");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("a.py"), "def alpha():\n    pass\n").unwrap();
        std::fs::write(dir.join("b.py"), "def beta():\n    pass\n").unwrap();

        // First ingest
        ingest_repo(&conn, "test", &dir).unwrap();

        // Count files records — should have 2
        let file_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE repo_id = 'test'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(file_count, 2, "files table should have 2 entries after first ingest");

        // Second ingest without changes — node count must be identical
        let (syms, _) = ingest_repo(&conn, "test", &dir).unwrap();
        assert_eq!(syms, 2, "second ingest should report same node count");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn modified_file_is_reindexed_on_second_ingest() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_incremental_modify");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("a.py"), "def alpha():\n    pass\n").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        // Write new content (different hash) — force mtime change too
        std::fs::write(dir.join("a.py"), "def alpha():\n    pass\ndef beta():\n    pass\n").unwrap();
        let (syms, _) = ingest_repo(&conn, "test", &dir).unwrap();
        assert_eq!(syms, 2, "modified file should result in 2 symbols (alpha + beta)");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deleted_file_nodes_removed_on_incremental_ingest() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_incremental_delete2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("keep.py"), "def keeper():\n    pass\n").unwrap();
        std::fs::write(dir.join("gone.py"), "def goner():\n    pass\n").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        std::fs::remove_file(dir.join("gone.py")).unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND file_path = 'gone.py'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 0, "nodes for deleted file should be removed");

        let files_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM files WHERE repo_id = 'test' AND file_path = 'gone.py'",
            [],
            |row| row.get(0),
        ).unwrap();
        assert_eq!(files_count, 0, "files record for deleted file should be removed");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_cross_repo_edges_skips_ambiguous_import_targets() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4), (?5, ?6)",
            rusqlite::params!["repo_a", "/tmp/repo_a", "repo_b", "/tmp/repo_b", "repo_c", "/tmp/repo_c"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7),
                    (?8, ?9, ?10, ?11, ?12, ?13, ?14),
                    (?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            rusqlite::params![
                "repo_a:main.py:main",
                "repo_a",
                "main.py",
                "py",
                "main",
                "function",
                "from vendor import SharedClient\n",
                "repo_b:client.ts:SharedClient",
                "repo_b",
                "client.ts",
                "ts",
                "SharedClient",
                "class",
                "export class SharedClient {}",
                "repo_c:client.ts:SharedClient",
                "repo_c",
                "client.ts",
                "ts",
                "SharedClient",
                "class",
                "export class SharedClient {}"
            ],
        )
        .unwrap();

        let edges = resolve_cross_repo_edges(&conn).unwrap();
        assert_eq!(edges, 0, "ambiguous cross-repo imports should be skipped");
    }

    #[test]
    fn test_ruby_symbol_extraction() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_ruby2");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(
            dir.join("test.rb"),
            "class InvoicesController < ApplicationController\n  def bulk_update\n    puts 'updating'\n  end\nend\n",
        )
        .unwrap();

        let (syms, _edges) = ingest_repo(&conn, "test", &dir).unwrap();
        
        let mut stmt = conn.prepare("SELECT symbol_name, symbol_type FROM nodes WHERE repo_id = 'test'").unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        println!("Nodes extracted: {:?}", rows);

        assert!(syms > 0, "No symbols ingested for ruby file");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_ingest_fails_for_non_existent_path() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_non_existent_path_lkjasdflkjasdf");
        let _ = std::fs::remove_dir_all(&dir);

        let result = ingest_repo(&conn, "test", &dir);
        assert!(result.is_err(), "ingest_repo should return Err if the root_path does not exist");
    }

    #[test]
    fn run_ingestion_with_progress_reports_completion() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_progress_reporting");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.py"), "def alpha():\n    pass\n").unwrap();
        std::fs::write(dir.join("b.py"), "def beta():\n    alpha()\n").unwrap();

        let progress_updates = std::sync::Mutex::new(Vec::new());
        run_ingestion_with_progress(&conn, "test", &dir, |percent| {
            progress_updates.lock().unwrap().push(percent);
        })
        .unwrap();

        let updates = progress_updates.lock().unwrap();
        assert!(!updates.is_empty(), "expected progress callback to fire");
        assert_eq!(updates.last().copied(), Some(100), "expected final progress to be 100: {updates:?}");
        assert!(
            updates.windows(2).all(|window| window[0] <= window[1]),
            "progress should be monotonic: {updates:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
