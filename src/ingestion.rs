use anyhow::{anyhow, Result};
use ignore::WalkBuilder;
use rayon::prelude::*;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
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
pub fn collect_source_files(root: &Path) -> Vec<PathBuf> {
    let walker = WalkBuilder::new(root)
        .hidden(true)       // skip dotfiles / hidden directories
        .git_ignore(true)   // respect .gitignore
        .git_exclude(true)  // respect .git/info/exclude
        .build();

    walker
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                return None;
            }
            if !is_safe_to_parse(path) {
                return None;
            }
            let ext = path.extension().and_then(|e| e.to_str())?;
            lang_config_for_ext(ext)?;
            Some(entry.into_path())
        })
        .collect()
}

/// Ingest an entire repository incrementally: only re-parse files whose
/// content hash has changed since the last index run. First-time ingest
/// is a full pass. Returns `(total_symbol_count, calls_edge_count)`.
pub fn ingest_repo(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<(usize, usize)> {
    let root_path = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());

    conn.execute(
        "INSERT OR REPLACE INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params![repo_id, root_path.to_string_lossy().as_ref()],
    )?;

    // ── Load existing file records ────────────────────────────────────────────
    let known_files: std::collections::HashMap<String, (i64, String)> = {
        let mut stmt = conn.prepare(
            "SELECT file_path, mtime_secs, content_hash FROM files WHERE repo_id = ?1",
        )?;
        let rows: Vec<(String, i64, String)> = stmt
            .query_map(rusqlite::params![repo_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?, row.get::<_, String>(2)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        rows.into_iter().map(|(path, mtime, hash)| (path, (mtime, hash))).collect()
    };

    // ── Walk all source files on disk ─────────────────────────────────────────
    let disk_files = collect_source_files(&root_path);

    // Gather (abs_path, rel_path, mtime_ns) for every file on disk.
    // We store nanosecond precision so sub-second writes (common in tests and
    // fast editors) are correctly detected as mtime changes. The column is
    // named mtime_secs but holds nanoseconds; SQLite INTEGER is 64-bit so
    // values through year 2262 fit comfortably.
    let disk_meta: Vec<(PathBuf, String, i64)> = disk_files
        .iter()
        .filter_map(|path| {
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            let rel = canonical.strip_prefix(&root_path).ok()?.to_string_lossy().to_string();
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
        .filter(|(_, rel, mtime)| {
            match known_files.get(rel) {
                None => true,                           // new file
                Some((km, _)) => km != mtime,          // mtime changed
            }
        })
        .cloned()
        .collect();

    // Read + hash candidates; keep only those whose content hash changed.
    // Returns (rel_path, abs_path, mtime, content_hash) for files to re-parse.
    let changed: Vec<(String, PathBuf, i64, String)> = candidates
        .par_iter()
        .filter_map(|(path, rel, mtime)| {
            let bytes = std::fs::read(path).ok()?;
            let new_hash = crate::db::hash_file_content(&bytes);
            // If known, compare hash; if hash is same, only mtime drifted (e.g. `touch`)
            if let Some((_, known_hash)) = known_files.get(rel) {
                if *known_hash == new_hash {
                    return None; // content identical — skip re-parse, just update mtime
                }
            }
            Some((rel.clone(), path.clone(), *mtime, new_hash))
        })
        .collect();

    // Collect rel_paths of mtime-only-changed files (candidates not in `changed`)
    let changed_rels: std::collections::HashSet<&str> =
        changed.iter().map(|(r, _, _, _)| r.as_str()).collect();
    let mtime_only: Vec<(String, i64)> = candidates
        .iter()
        .filter(|(_, rel, _)| !changed_rels.contains(rel.as_str()))
        .map(|(_, rel, mtime)| (rel.clone(), *mtime))
        .collect();

    // ── Parallel parse of changed files ──────────────────────────────────────
    let parsed: Vec<(String, String, Vec<Symbol>, String, i64)> = changed
        .par_iter()
        .filter_map(|(rel, path, mtime, hash)| {
            match parse_file(path) {
                Ok((lang, symbols)) => Some((rel.clone(), lang, symbols, hash.clone(), *mtime)),
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", path.display(), e);
                    None
                }
            }
        })
        .collect();

    // ── Detect removed files ──────────────────────────────────────────────────
    let disk_rels: std::collections::HashSet<&str> =
        disk_meta.iter().map(|(_, r, _)| r.as_str()).collect();
    let removed_rels: Vec<String> = known_files
        .keys()
        .filter(|fp| !disk_rels.contains(fp.as_str()))
        .cloned()
        .collect();

    // ── Single transaction: delete stale, insert fresh ────────────────────────
    let tx = conn.unchecked_transaction()?;

    // Remove nodes + files record for deleted files
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
            crate::db::mark_deleted_observation_stale(&tx, repo_id, sym, file_path)?;
        }
        tx.execute(
            "DELETE FROM edges WHERE source_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)
             OR target_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
            rusqlite::params![repo_id, file_path],
        )?;
        tx.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
        tx.execute(
            "DELETE FROM files WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
    }

    // Remove old nodes+edges for changed files (will be replaced)
    for (file_path, _, _, _, _) in &parsed {
        tx.execute(
            "DELETE FROM edges WHERE source_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)
             OR target_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
            rusqlite::params![repo_id, file_path],
        )?;
        tx.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
    }

    // Insert new nodes for changed files
    for (file_path, lang, symbols, hash, mtime) in &parsed {
        for sym in symbols {
            let node_id = format!("{}:{}:{}", repo_id, file_path, sym.name);
            tx.execute(
                "INSERT OR REPLACE INTO nodes \
                 (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    node_id, repo_id, file_path, lang,
                    sym.name, sym.symbol_type, sym.raw_text
                ],
            )?;
            let new_hash = crate::db::hash_raw_text(&sym.raw_text);
            crate::db::mark_stale_observations(&tx, repo_id, &sym.name, file_path, &new_hash)?;
        }
        // Upsert files record for changed file
        tx.execute(
            "INSERT OR REPLACE INTO files (repo_id, file_path, mtime_secs, content_hash)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![repo_id, file_path, mtime, hash],
        )?;
    }

    // Update mtime-only files (content unchanged, mtime drifted)
    for (rel, mtime) in &mtime_only {
        tx.execute(
            "UPDATE files SET mtime_secs = ?1 WHERE repo_id = ?2 AND file_path = ?3",
            rusqlite::params![mtime, repo_id, rel],
        )?;
    }

    tx.commit()?;

    // ── Build CALLS edges for changed files ───────────────────────────────────
    // Load full name→ids map for this repo (needed for resolution)
    let mut name_to_ids: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT symbol_name, id FROM nodes WHERE repo_id = ?1",
        )?;
        stmt.query_map(rusqlite::params![repo_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .filter_map(|r| r.ok())
        .for_each(|(name, id)| name_to_ids.entry(name).or_default().push(id));
    }

    let edge_tx = conn.unchecked_transaction()?;
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
                        edge_tx.execute(
                            "INSERT OR IGNORE INTO edges \
                             (source_id, target_id, relationship_type) \
                             VALUES (?1, ?2, 'CALLS')",
                            rusqlite::params![source_id, target_id],
                        )?;
                        calls_edge_count += 1;
                    }
                }
            }
        }
    }
    edge_tx.commit()?;

    // Total node count across the whole repo (unchanged + newly parsed)
    let total: usize = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get::<_, i64>(0),
    )? as usize;

    Ok((total, calls_edge_count))
}

/// Combined ingestion pipeline: parse all files in `root_path` under `repo_id`,
/// then resolve cross-repo edges in a single call.
///
/// Both the explicit `ingest_repo` MCP tool handler and the JIT auto-indexer
/// call this function so the full pipeline is never duplicated.
/// Returns `(symbol_count, edge_count)`.
pub fn run_ingestion(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<(usize, usize)> {
    let (symbols, calls_edges) = ingest_repo(conn, repo_id, root_path)?;
    let import_edges = resolve_cross_repo_edges(conn)?;
    crate::db::vacuum_and_checkpoint(conn)?;
    Ok((symbols, calls_edges + import_edges))
}

/// Secondary pass: resolve cross-repo import edges.
/// Scans node raw_text for import-like patterns and creates IMPORTS edges
/// when the imported symbol exists in another repo's nodes.
pub fn resolve_cross_repo_edges(conn: &Connection) -> Result<usize> {
    let mut stmt = conn.prepare("SELECT id, repo_id, raw_text, language FROM nodes")?;
    let rows: Vec<(String, String, String, String)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    let tx = conn.unchecked_transaction()?;
    let mut edge_count = 0;

    for (source_id, source_repo, raw_text, lang) in &rows {
        let imports = extract_imports(raw_text, lang);
        for imported_name in imports {
            // Look for this symbol in OTHER repos
            let mut find = conn.prepare_cached(
                "SELECT id FROM nodes WHERE symbol_name = ?1 AND repo_id != ?2 ORDER BY repo_id, file_path, id",
            )?;
            let target_ids: Vec<String> = find
                .query_map(rusqlite::params![imported_name, source_repo], |row| {
                    row.get::<_, String>(0)
                })?
                .filter_map(|r| r.ok())
                .collect();

            if target_ids.len() == 1 {
                let target_id = &target_ids[0];
                tx.execute(
                    "INSERT OR IGNORE INTO edges (source_id, target_id, relationship_type)
                     VALUES (?1, ?2, 'IMPORTS')",
                    rusqlite::params![source_id, target_id],
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
}
