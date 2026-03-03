use anyhow::{anyhow, Result};
use rayon::prelude::*;
use rusqlite::Connection;
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

/// Parse a single file and return its language tag plus extracted symbols.
pub fn parse_file(path: &Path) -> Result<(String, Vec<Symbol>)> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .ok_or_else(|| anyhow!("No file extension: {}", path.display()))?;

    let config =
        lang_config_for_ext(ext).ok_or_else(|| anyhow!("Unsupported extension: {}", ext))?;

    let source = std::fs::read_to_string(path)?;
    let source_bytes = source.as_bytes();

    let mut parser = Parser::new();
    parser.set_language(&config.language)?;

    let tree = parser
        .parse(&source, None)
        .ok_or_else(|| anyhow!("tree-sitter parse failed: {}", path.display()))?;

    let query = Query::new(&config.language, config.query_src)?;
    let mut cursor = QueryCursor::new();
    let mut symbols = Vec::new();

    let mut matches = cursor.matches(&query, tree.root_node(), source_bytes);
    while let Some(m) = matches.next() {
        for capture in m.captures {
            let node = capture.node;
            let capture_name = query.capture_names()[capture.index as usize];
            let name = extract_symbol_name(&node, source_bytes);
            let raw_text = node.utf8_text(source_bytes).unwrap_or("").to_string();
            symbols.push(Symbol {
                name,
                symbol_type: capture_name.to_string(),
                raw_text,
            });
        }
    }

    Ok((ext.to_string(), symbols))
}

/// Recursively collect all source files with supported extensions under `root`.
pub fn collect_source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_recursive(root, &mut files);
    files
}

fn collect_recursive(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            // Skip hidden dirs and common non-source directories
            if name.starts_with('.')
                || name == "node_modules"
                || name == "target"
                || name == "__pycache__"
                || name == "build"
            {
                continue;
            }
            collect_recursive(&path, out);
        } else if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
            if lang_config_for_ext(ext).is_some() {
                out.push(path);
            }
        }
    }
}

/// Ingest an entire repository: parse all files in parallel, write nodes to DB.
/// Returns the number of symbols inserted.
pub fn ingest_repo(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<usize> {
    conn.execute(
        "INSERT OR REPLACE INTO repositories (id, root_path) VALUES (?1, ?2)",
        rusqlite::params![repo_id, root_path.to_string_lossy().as_ref()],
    )?;

    let files = collect_source_files(root_path);

    // Parse all files in parallel with rayon
    let results: Vec<_> = files
        .par_iter()
        .filter_map(|path| {
            let rel_path = path
                .strip_prefix(root_path)
                .unwrap_or(path)
                .to_string_lossy()
                .to_string();
            match parse_file(path) {
                Ok((lang, symbols)) => Some((rel_path, lang, symbols)),
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", path.display(), e);
                    None
                }
            }
        })
        .collect();

    // Batch insert into SQLite (single-threaded, inside a transaction)
    let tx = conn.unchecked_transaction()?;
    let mut count = 0;
    for (file_path, lang, symbols) in &results {
        for sym in symbols {
            let node_id = format!("{}:{}:{}", repo_id, file_path, sym.name);
            tx.execute(
                "INSERT OR REPLACE INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![node_id, repo_id, file_path, lang, sym.name, sym.symbol_type, sym.raw_text],
            )?;
            count += 1;
        }
    }
    tx.commit()?;

    Ok(count)
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
                "SELECT id FROM nodes WHERE symbol_name = ?1 AND repo_id != ?2 LIMIT 1",
            )?;
            let target_id_opt = find
                .query_map(rusqlite::params![imported_name, source_repo], |row| {
                    row.get::<_, String>(0)
                })?
                .filter_map(|r| r.ok())
                .next();

            if let Some(target_id) = target_id_opt {
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
