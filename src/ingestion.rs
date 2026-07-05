use anyhow::{anyhow, Result};
use ignore::WalkBuilder;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rusqlite::Connection;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use streaming_iterator::StreamingIterator;
use tree_sitter::{Language, Parser, Query, QueryCursor};

/// Parsed symbol extracted from a source file.
pub struct Symbol {
    pub name: String,
    pub symbol_type: String,
    pub raw_text: String,
    /// Callee names extracted from this symbol's body during the parallel parse phase.
    /// Pre-computed here so the DB write phase never needs to re-run tree-sitter.
    pub callees: Vec<String>,
    /// Byte offset of the symbol's first character in the source file.
    /// Used in `make_node_id` to disambiguate same-name same-kind symbols.
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_line: usize,
    pub start_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

/// Build a stable node ID that disambiguates same-name symbols in the same file
/// (e.g. C++ `class Widget` vs constructor `Widget`, or two same-name nested functions).
/// Format: `repo_id:file_path:symbol_type:symbol_name:start_byte`
pub fn make_node_id(
    repo_id: &str,
    file_path: &str,
    symbol_type: &str,
    symbol_name: &str,
    start_byte: usize,
) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        repo_id, file_path, symbol_type, symbol_name, start_byte
    )
}

/// One indexed file’s parse output: rel path, language tag, symbols, content hash, mtime (ns).
type ParsedFileBatchRow = (String, String, Vec<Symbol>, String, i64);

/// Tree-sitter configuration for a supported language.
struct LangConfig {
    language: Language,
    query_src: &'static str,
    /// Language-specific accept/reject hook for query captures, for rules
    /// core tree-sitter queries cannot express (ancestor checks, callee
    /// allowlists — there are no ancestor/negation predicates upstream).
    /// `None` keeps every capture.
    capture_filter: Option<fn(&tree_sitter::Node, &[u8]) -> bool>,
}

/// Symbol query shared by the `.ts` and `.tsx` arms — one constant so the two
/// grammars can never drift apart silently.
///
/// The `variable_declarator` patterns index value bindings that define
/// functions: `const f = () => ...`, `const f = function () {...}`, their
/// `var` twins, and known component wrappers (`memo(() => ...)`). Locality
/// and the wrapper allowlist are enforced by `ts_capture_filter`.
const TS_SYMBOL_QUERY: &str = concat!(
    "(function_declaration) @function\n",
    "(method_definition) @method\n",
    "(class_declaration) @class\n",
    "(interface_declaration) @interface\n",
    "(type_alias_declaration) @type\n",
    "(enum_declaration) @enum\n",
    "(lexical_declaration (variable_declarator name: (identifier) value: [(arrow_function) (function_expression)]) @function)\n",
    "(variable_declaration (variable_declarator name: (identifier) value: [(arrow_function) (function_expression)]) @function)\n",
    "(lexical_declaration (variable_declarator name: (identifier) value: (call_expression function: [(identifier) (member_expression)] arguments: (arguments (arrow_function)))) @function)"
);

/// Wrapper callees whose arrow-function argument defines the bound symbol
/// (`const C = memo(() => ...)`, `React.forwardRef(...)`). Anything else —
/// `useMemo`, `debounce`, `.then` — receives a callback, not a definition.
const TS_FUNCTION_WRAPPERS: &[&str] = &["memo", "forwardRef"];

/// Accept/reject TS and TSX `variable_declarator` captures.
fn ts_capture_filter(node: &tree_sitter::Node, source: &[u8]) -> bool {
    if node.kind() != "variable_declarator" {
        return true;
    }
    // Reject function-local bindings: `const handleClick = () => {}` inside a
    // component body is implementation detail, not module surface, and every
    // component repeats the same handler names — poisoning name-based call
    // resolution. Module/namespace/export scopes have no such ancestor.
    let mut ancestor = node.parent();
    while let Some(a) = ancestor {
        if matches!(
            a.kind(),
            "arrow_function"
                | "function_declaration"
                | "function_expression"
                | "generator_function"
                | "generator_function_declaration"
                | "method_definition"
        ) {
            return false;
        }
        ancestor = a.parent();
    }
    let Some(value) = node.child_by_field_name("value") else {
        return true;
    };
    if value.kind() != "call_expression" {
        return true;
    }
    // Wrapper call: accept only the known component wrappers. The `property`
    // field covers `React.memo` / `React.forwardRef`.
    let callee_name = value
        .child_by_field_name("function")
        .and_then(|callee| match callee.kind() {
            "identifier" => callee.utf8_text(source).ok(),
            "member_expression" => callee
                .child_by_field_name("property")
                .and_then(|prop| prop.utf8_text(source).ok()),
            _ => None,
        });
    callee_name.is_some_and(|name| TS_FUNCTION_WRAPPERS.contains(&name))
}

/// Reject impl-body associated types: `impl Iterator for X { type Item = u32; }`
/// is a `type_item` too, and 50 impls would index 50 colliding `Item` symbols.
/// (Trait-body associated types parse as `associated_type` — never captured.)
fn rust_capture_filter(node: &tree_sitter::Node, _source: &[u8]) -> bool {
    if node.kind() != "type_item" {
        return true;
    }
    let mut ancestor = node.parent();
    while let Some(a) = ancestor {
        if a.kind() == "impl_item" {
            return false;
        }
        ancestor = a.parent();
    }
    true
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

/// Rayon worker count for ingestion (`read_to_string` + tree-sitter per file).
/// Unbounded parallelism multiplies peak RSS (one full source buffer per worker).
fn ingest_parse_thread_count() -> usize {
    std::env::var("MARROW_INGEST_THREADS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get().clamp(2, 8))
                .unwrap_or(4)
        })
}

/// Max parsed files buffered between Rayon workers and the DB write phase.
/// Bounded channel back-pressure limits peak RSS during large reindexes.
fn ingest_parse_queue_capacity() -> usize {
    std::env::var("MARROW_INGEST_PARSE_QUEUE")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(64)
}

/// Extensions that all share the tree-sitter-cpp grammar path.
/// The single source of truth for "is this a C/C++ file" — every ingestion
/// and retrieval decision that treats the c-family specially routes through
/// here so a new extension can never be added to one list and missed in
/// another (the `.c` rollout touched five hardcoded lists).
pub fn is_c_family_ext(ext: &str) -> bool {
    matches!(ext, "c" | "cpp" | "cc" | "cxx" | "h" | "hpp")
}

/// Return the tree-sitter `Language` for a file extension.
/// Extracted from `lang_config_for_ext` so other modules (e.g. watcher) can
/// check parsability without needing the full query config.
pub fn language_for_ext(ext: &str) -> Option<Language> {
    if is_c_family_ext(ext) {
        return Some(tree_sitter_cpp::LANGUAGE.into());
    }
    match ext {
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
        // Specifier patterns require both a name and a body: anonymous
        // specifiers (`union { ... } u;` nested in structs, top-level
        // `enum { ... };` constant blocks) would otherwise index as symbols
        // literally named "anonymous", and body-less forward declarations
        // carry no definition worth indexing (known trade-off: Pimpl-style
        // `class Impl;` headers lose their only index presence).
        // Typedef aliases are separate `@type` symbols — the tag (if named)
        // is still indexed by the specifier patterns, and `name:` is a field
        // wildcard `(_)` so template specializations (`struct hash<Foo>`)
        // stay indexed.
        _ if is_c_family_ext(ext) => Some(LangConfig {
            language: tree_sitter_cpp::LANGUAGE.into(),
            query_src: concat!(
                "(function_definition) @function\n",
                "(class_specifier name: (_) body: (field_declaration_list)) @class\n",
                "(struct_specifier name: (_) body: (field_declaration_list)) @struct\n",
                "(union_specifier name: (_) body: (field_declaration_list)) @union\n",
                "(enum_specifier name: (_) body: (enumerator_list)) @enum\n",
                "(type_definition type: (struct_specifier body: (field_declaration_list))) @type\n",
                "(type_definition type: (union_specifier body: (field_declaration_list))) @type\n",
                "(type_definition type: (enum_specifier body: (enumerator_list))) @type\n",
                "(type_definition type: (class_specifier body: (field_declaration_list))) @type"
            ),
            capture_filter: None,
        }),
        "py" => Some(LangConfig {
            language: tree_sitter_python::LANGUAGE.into(),
            query_src: concat!(
                "(function_definition) @function\n",
                "(class_definition) @class\n",
                "(type_alias_statement) @type"
            ),
            capture_filter: None,
        }),
        "ts" => Some(LangConfig {
            language: tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            query_src: TS_SYMBOL_QUERY,
            capture_filter: Some(ts_capture_filter),
        }),
        "tsx" => Some(LangConfig {
            language: tree_sitter_typescript::LANGUAGE_TSX.into(),
            query_src: TS_SYMBOL_QUERY,
            capture_filter: Some(ts_capture_filter),
        }),
        "rs" => Some(LangConfig {
            language: tree_sitter_rust::LANGUAGE.into(),
            query_src: concat!(
                "(function_item) @function\n",
                "(struct_item) @struct\n",
                "(trait_item) @trait\n",
                "(impl_item) @impl\n",
                "(enum_item) @enum\n",
                "(type_item) @type\n",
                "(union_item) @union\n",
                "(macro_definition) @macro"
            ),
            capture_filter: Some(rust_capture_filter),
        }),
        "rb" => Some(LangConfig {
            language: tree_sitter_ruby::LANGUAGE.into(),
            query_src: concat!(
                "(method) @method\n",
                "(singleton_method) @method\n",
                "(class) @class\n",
                "(module) @module"
            ),
            capture_filter: None,
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

fn find_first_identifier<'a>(node: tree_sitter::Node<'a>, source: &[u8]) -> Option<String> {
    match node.kind() {
        "identifier" | "type_identifier" | "field_identifier" | "property_identifier" => {
            return Some(node.utf8_text(source).unwrap_or("unknown").to_string());
        }
        _ => {}
    }

    for i in 0..node.named_child_count() {
        if let Some(child) = node.named_child(i as u32) {
            if let Some(name) = find_first_identifier(child, source) {
                return Some(name);
            }
        }
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
    // Python PEP 695 aliases are `type_alias_statement` nodes whose public
    // symbol lives under the `left` field, e.g. `type Vector[T] = list[T]`.
    if node.kind() == "type_alias_statement" {
        if let Some(left) = node.child_by_field_name("left") {
            if let Some(name) = find_first_identifier(left, source) {
                return name;
            }
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

/// All names bound by a captured node.
/// `type_definition` can bind several aliases at once — `typedef struct
/// { ... } A, B;` carries one `declarator` field per alias, and
/// `child_by_field_name` would silently drop everything after the first —
/// so it yields one name per declarator. Every other capture kind yields
/// exactly one name.
fn capture_symbol_names(node: &tree_sitter::Node, source: &[u8]) -> Vec<String> {
    if node.kind() == "type_definition" {
        let mut cursor = node.walk();
        let names: Vec<String> = node
            .children_by_field_name("declarator", &mut cursor)
            .filter_map(|decl| find_name_in_declarator(decl, source))
            .collect();
        if !names.is_empty() {
            return names;
        }
    }
    vec![extract_symbol_name(node, source)]
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
        "c" | "cpp" | "cc" | "cxx" | "h" | "hpp" => Some(concat!(
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

    // Guard: skip files larger than MARROW_MAX_FILE_BYTES (default 2 MiB).
    // tree-sitter builds an in-memory AST that is 3–10× the source size. With 8 parallel
    // rayon workers, a single 20 MB generated file (GraphQL schema, protobuf output,
    // API client codegen, accidentally committed bundle) creates ~1.4 GB of RSS pressure.
    // These files contribute zero architectural signal to the AST graph.
    // Full source is always available on disk via normal file reads.
    const DEFAULT_MAX_FILE_BYTES: u64 = 2 * 1024 * 1024; // 2 MiB
    let max_bytes: u64 = std::env::var("MARROW_MAX_FILE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_MAX_FILE_BYTES);
    if let Ok(meta) = std::fs::metadata(path) {
        if meta.len() > max_bytes {
            return Ok((ext, Vec::new())); // silently skip; file is on disk, not lost
        }
    }

    let source = std::fs::read_to_string(path)?;
    let path_display = path.display().to_string();
    let symbols = parse_file_inner(&source, &ext, &config, &path_display)?;

    Ok((ext, symbols))
}

/// Shared tree-sitter parse logic for a single source buffer.
fn parse_file_inner(
    source: &str,
    ext: &str,
    config: &LangConfig,
    path_display: &str,
) -> Result<Vec<Symbol>> {
    let source_bytes = source.as_bytes();

    SYMBOL_PARSERS.with(|cache| -> Result<Vec<Symbol>> {
        let mut map = cache.borrow_mut();
        if !map.contains_key(ext) {
            let mut parser = Parser::new();
            parser.set_language(&config.language)?;
            let query = Query::new(&config.language, config.query_src)?;
            map.insert(ext.to_string(), (parser, query));
        }
        let (parser, query) = map.get_mut(ext).unwrap();
        parser.reset();

        let tree = parser
            .parse(source, None)
            .ok_or_else(|| anyhow!("tree-sitter parse failed: {}", path_display))?;

        let mut cursor = QueryCursor::new();
        let mut syms = Vec::new();
        let mut matches = cursor.matches(query, tree.root_node(), source_bytes);
        while let Some(m) = matches.next() {
            for capture in m.captures {
                let node = capture.node;
                if let Some(filter) = config.capture_filter {
                    if !filter(&node, source_bytes) {
                        continue;
                    }
                }
                let capture_name = query.capture_names()[capture.index as usize];
                let names = capture_symbol_names(&node, source_bytes);
                let raw_text = cap_raw_text(node.utf8_text(source_bytes).unwrap_or("").to_string());
                let callees = extract_calls_from_symbol(&raw_text, ext);
                let start_byte = node.start_byte();
                let end_byte = node.end_byte();
                let start_position = node.start_position();
                let end_position = node.end_position();
                for name in names {
                    syms.push(Symbol {
                        name,
                        symbol_type: capture_name.to_string(),
                        raw_text: raw_text.clone(),
                        callees: callees.clone(),
                        start_byte,
                        end_byte,
                        start_line: start_position.row + 1,
                        start_column: start_position.column + 1,
                        end_line: end_position.row + 1,
                        end_column: end_position.column + 1,
                    });
                }
            }
        }
        Ok(syms)
    })
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
        "zip", "gz", "tar", // archives
        "sqlite", "db", // databases
    ];
    let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if BLOCKED_EXTENSIONS.contains(&ext) {
        return false;
    }

    true
}

/// Ignore globs from `.marrowrc.json` (same semantics as `marrow index` / TUI index).
fn marrow_ignore_patterns(root: &Path) -> Vec<String> {
    let path = root.join(".marrowrc.json");
    let Ok(raw) = fs::read_to_string(&path) else {
        return default_marrow_ignore_patterns();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&raw) else {
        return default_marrow_ignore_patterns();
    };
    v.get("ignore")
        .and_then(|a| a.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn default_marrow_ignore_patterns() -> Vec<String> {
    vec![
        "node_modules".into(),
        "target".into(),
        "dist".into(),
        ".git".into(),
        "worktrees".into(),
        ".worktrees".into(),
    ]
}

/// Configure a `WalkBuilder` like `marrow index` / `run_index_command` (marrowrc + .gitignore).
fn walk_builder_for_repo(root: &Path) -> Result<WalkBuilder> {
    let ignore_patterns = marrow_ignore_patterns(root);
    let mut builder = WalkBuilder::new(root);
    builder
        .hidden(true)
        .git_ignore(true)
        .git_global(false)
        .git_exclude(false);
    // M-7 FIX: Apply .marrowrc.json patterns as exclusions via filter_entry.
    // The previous OverrideBuilder approach used `!{pat}` which, despite the
    // ignore crate treating `!` as "ignore", did not correctly exclude
    // directories in all edge cases. Using filter_entry is explicit and testable.
    // Patterns with trailing slashes (e.g. "generated/") are stripped to match
    // directory basenames correctly.
    let normalized_patterns: Vec<String> = ignore_patterns
        .into_iter()
        .map(|p| p.trim_end_matches('/').to_string())
        .collect();
    builder.filter_entry(move |e| {
        let name = e.file_name().to_string_lossy();
        // Hardcoded exclusions for common non-source directories
        if matches!(
            name.as_ref(),
            "node_modules"
                | ".git"
                | "target"
                | "dist"
                | "build"
                | "vendor"
                | "worktrees"
                | ".worktrees"
        ) {
            return false;
        }
        // .marrowrc.json custom exclusions — match entry name against each pattern
        for pat in &normalized_patterns {
            if name == pat.as_str() {
                return false;
            }
        }
        true
    });
    Ok(builder)
}

/// Recursively collect all parseable source files under `root`, respecting
/// `.gitignore`, `.marrowrc.json` ignore rules, and the hardcoded security/noise filter.
///
/// Uses `ignore::WalkParallel` with an mpsc channel to traverse the directory
/// tree concurrently, avoiding the bottleneck of a single-threaded walk on
/// large repositories.
pub fn collect_source_files(root: &Path) -> Result<Vec<PathBuf>> {
    use ignore::WalkState;

    let (tx, rx) = mpsc::channel();
    let wb = walk_builder_for_repo(root)?;
    let walker = wb.build_parallel();

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

    drop(tx);
    Ok(rx.into_iter().collect())
}

/// Updates progress only when `next_percent` exceeds the last value **and** invokes `progress`
/// under a mutex so parallel rayon workers cannot emit callbacks out of order (e.g. 45 then 27).
fn maybe_emit_progress<F>(progress: &F, last_reported: &Mutex<u8>, next_percent: u8)
where
    F: Fn(u8) + Send + Sync,
{
    let next = next_percent.min(100);
    let Ok(mut prev) = last_reported.lock() else {
        return;
    };
    if next > *prev {
        *prev = next;
        progress(next);
    }
}

/// All data produced by the CPU-intensive parse phase.
/// Passed to the write phase so the DB lock is not held during file I/O
/// or tree-sitter work.
struct ComputedChangeset {
    /// Serialized parsed rows (workers → bounded channel → drainer thread wrote here).
    /// Tempfile is auto-cleaned when dropped.
    parsed_spill: tempfile::NamedTempFile,
    /// Files whose mtime changed but content hash was identical (mtime-drift only).
    mtime_only: Vec<(String, i64)>,
    /// Relative paths of files that disappeared from disk since last index.
    removed_rels: Vec<String>,
}

fn write_u64_be(w: &mut impl Write, v: u64) -> std::io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

fn read_u64_be(r: &mut impl Read) -> std::io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

fn write_utf8_blob(w: &mut impl Write, s: &str) -> std::io::Result<()> {
    let b = s.as_bytes();
    write_u64_be(w, b.len() as u64)?;
    w.write_all(b)
}

/// Max bytes for a single length-prefixed UTF-8 blob in the ingest spill file (DoS guard).
const MAX_INGEST_SPILL_BLOB_BYTES: u64 = 64 * 1024 * 1024;

fn read_utf8_blob(r: &mut impl Read) -> std::io::Result<String> {
    read_utf8_blob_capped(r, MAX_INGEST_SPILL_BLOB_BYTES)
}

fn read_utf8_blob_capped(r: &mut impl Read, max_len: u64) -> std::io::Result<String> {
    let len = read_u64_be(r)?;
    if len > max_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ingest spill blob length {len} exceeds cap {max_len}"),
        ));
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ingest spill: {e}"),
        )
    })
}

fn write_spill_parsed_row(w: &mut impl Write, row: &ParsedFileBatchRow) -> std::io::Result<()> {
    let (path, lang, symbols, hash, mtime) = row;
    write_utf8_blob(w, path)?;
    write_utf8_blob(w, lang)?;
    write_utf8_blob(w, hash)?;
    w.write_all(&mtime.to_be_bytes())?;
    write_u64_be(w, symbols.len() as u64)?;
    for sym in symbols {
        write_utf8_blob(w, &sym.name)?;
        write_utf8_blob(w, &sym.symbol_type)?;
        write_utf8_blob(w, &sym.raw_text)?;
        write_u64_be(w, sym.start_byte as u64)?;
        write_u64_be(w, sym.end_byte as u64)?;
        write_u64_be(w, sym.start_line as u64)?;
        write_u64_be(w, sym.start_column as u64)?;
        write_u64_be(w, sym.end_line as u64)?;
        write_u64_be(w, sym.end_column as u64)?;
        write_u64_be(w, sym.callees.len() as u64)?;
        for callee in &sym.callees {
            write_utf8_blob(w, callee)?;
        }
    }
    Ok(())
}

/// `Ok(None)` on clean EOF before the next row; `Err` on corrupt/truncated spill.
fn read_spill_parsed_row(r: &mut impl Read) -> Result<Option<ParsedFileBatchRow>> {
    let path = match read_utf8_blob(r) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let lang = read_utf8_blob(r)?;
    let hash = read_utf8_blob(r)?;
    let mut mt = [0u8; 8];
    r.read_exact(&mut mt)?;
    let mtime = i64::from_be_bytes(mt);
    let n = read_u64_be(r)? as usize;
    const MAX_SYMBOLS_PER_SPILL_ROW: u64 = 1_000_000;
    if n as u64 > MAX_SYMBOLS_PER_SPILL_ROW {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("ingest spill symbol count {n} exceeds cap {MAX_SYMBOLS_PER_SPILL_ROW}"),
        )
        .into());
    }
    let mut symbols = Vec::with_capacity(n);
    for _ in 0..n {
        let name = read_utf8_blob(r)?;
        let symbol_type = read_utf8_blob(r)?;
        let raw_text = read_utf8_blob(r)?;
        let start_byte = read_u64_be(r)? as usize;
        let end_byte = read_u64_be(r)? as usize;
        let start_line = read_u64_be(r)? as usize;
        let start_column = read_u64_be(r)? as usize;
        let end_line = read_u64_be(r)? as usize;
        let end_column = read_u64_be(r)? as usize;
        let callee_count = read_u64_be(r)? as usize;
        let mut callees = Vec::with_capacity(callee_count);
        for _ in 0..callee_count {
            callees.push(read_utf8_blob(r)?);
        }
        symbols.push(Symbol {
            name,
            symbol_type,
            raw_text,
            callees,
            start_byte,
            end_byte,
            start_line,
            start_column,
            end_line,
            end_column,
        });
    }
    Ok(Some((path, lang, symbols, hash, mtime)))
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
    let mut stmt =
        conn.prepare("SELECT file_path, mtime_secs, content_hash FROM files WHERE repo_id = ?1")?;
    let rows: Vec<(String, i64, String)> = stmt
        .query_map(rusqlite::params![repo_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows
        .into_iter()
        .map(|(path, mtime, hash)| (path, (mtime, hash)))
        .collect())
}

// ── Phase B: pure CPU/IO — no DB connection held ──────────────────────────────

/// Hash + parse candidate files in parallel. Separated so we can run it on a capped
/// rayon pool without relying on `FnOnce` twice.
fn parallel_hash_and_parse_candidates<F>(
    candidates: &[(PathBuf, String, i64)],
    known_files: &HashMap<String, (i64, String)>,
    parsed_tx: SyncSender<ParsedFileBatchRow>,
    progress: &F,
    progress_state: &Mutex<u8>,
) -> Result<Vec<(String, i64)>>
where
    F: Fn(u8) + Send + Sync,
{
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
                    return None;
                }
            }
            Some((rel.clone(), path.clone(), *mtime, new_hash))
        })
        .collect();
    maybe_emit_progress(progress, progress_state, 45);

    let changed_rels: std::collections::HashSet<&str> =
        changed.iter().map(|(r, _, _, _)| r.as_str()).collect();
    let mtime_only: Vec<(String, i64)> = candidates
        .iter()
        .filter(|(_, rel, _)| !changed_rels.contains(rel.as_str()))
        .map(|(_, rel, mtime)| (rel.clone(), *mtime))
        .collect();

    let changed_total = changed.len().max(1);
    let parse_outcome = changed.par_iter().enumerate().try_for_each(
        |(idx, (rel, path, mtime, hash))| -> Result<()> {
            let tx = parsed_tx.clone();
            let result = match parse_file(path) {
                Ok((lang, symbols)) => Some((rel.clone(), lang, symbols, hash.clone(), *mtime)),
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", path.display(), e);
                    None
                }
            };
            let percent = 45 + (((idx + 1) * 35) / changed_total) as u8;
            maybe_emit_progress(progress, progress_state, percent);
            if let Some(row) = result {
                tx.send(row).map_err(|_| {
                    anyhow!("ingest parse queue closed before write phase (receiver dropped)")
                })?;
            }
            Ok(())
        },
    );
    drop(parsed_tx);
    parse_outcome?;
    maybe_emit_progress(progress, progress_state, 80);

    Ok(mtime_only)
}

fn ingest_spill_path() -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new().prefix("marrow-spill-").tempfile()
}

fn spill_drainer_loop(
    parsed_rx: mpsc::Receiver<ParsedFileBatchRow>,
    spill_file: tempfile::NamedTempFile,
) -> Result<(tempfile::NamedTempFile, Option<anyhow::Error>)> {
    let mut w = BufWriter::new(spill_file);
    let mut first_write_err: Option<anyhow::Error> = None;
    while let Ok(row) = parsed_rx.recv() {
        if first_write_err.is_none() {
            if let Err(e) = write_spill_parsed_row(&mut w, &row) {
                first_write_err = Some(anyhow::Error::from(e));
            }
        }
    }
    w.flush()?;
    let file = w.into_inner()?;
    Ok((file, first_write_err))
}

/// Walk the filesystem, hash changed files, and run tree-sitter in parallel.
/// No database access — safe to run while the DB mutex is released.
fn compute_changeset<F>(
    known_files: &HashMap<String, (i64, String)>,
    root_path: &Path,
    progress: &F,
    progress_state: &Mutex<u8>,
) -> Result<ComputedChangeset>
where
    F: Fn(u8) + Send + Sync,
{
    let disk_files = collect_source_files(root_path)?;
    maybe_emit_progress(progress, progress_state, 10);

    // Gather (abs_path, rel_path, mtime_ns) for every file on disk.
    // We store nanosecond precision so sub-second writes are correctly
    // detected. The column is named mtime_secs but holds nanoseconds;
    // SQLite INTEGER is 64-bit so values through year 2262 fit fine.
    let disk_meta: Vec<(PathBuf, String, i64)> = disk_files
        .iter()
        .filter_map(|path| {
            let canonical = path.canonicalize().unwrap_or_else(|_| path.clone());
            // Store repo-relative paths in canonical forward-slash form so the
            // graph, freshness notes, and emitted context packets are portable
            // and stable across platforms (Windows `strip_prefix` yields `\`).
            let rel = crate::db::normalize_path_separators(
                canonical.strip_prefix(root_path).ok()?.to_string_lossy().as_ref(),
            );
            let mtime = std::fs::metadata(path)
                .ok()?
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            Some((path.clone(), rel, mtime))
        })
        .collect();

    // M-11 FIX: Hash ALL known files, not just those with changed mtime.
    // Mtime-preserving edits (e.g. git checkout, sed -i on some filesystems)
    // would otherwise skip hash verification, leaving stale graph data and
    // fresh-looking observations.
    let candidates: Vec<(PathBuf, String, i64)> = disk_meta.to_vec();

    let ingest_threads = ingest_parse_thread_count();
    let queue_cap = ingest_parse_queue_capacity();
    let (parsed_tx, parsed_rx) = mpsc::sync_channel::<ParsedFileBatchRow>(queue_cap);
    let spill_file =
        ingest_spill_path().map_err(|e| anyhow!("failed to create spill tempfile: {e}"))?;
    let drainer = std::thread::spawn(move || spill_drainer_loop(parsed_rx, spill_file));

    // Ensure `parsed_tx` is always dropped (closing the channel) even if Rayon panics,
    // so `spill_drainer_loop` cannot block forever in `recv`.
    let parse_caught =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || match ThreadPoolBuilder::new().num_threads(ingest_threads).build() {
                Ok(pool) => pool.install(|| {
                    parallel_hash_and_parse_candidates(
                        &candidates,
                        known_files,
                        parsed_tx,
                        progress,
                        progress_state,
                    )
                }),
                Err(e) => {
                    eprintln!(
                        "[marrow] ingest thread pool build failed ({e}); using default rayon pool"
                    );
                    parallel_hash_and_parse_candidates(
                        &candidates,
                        known_files,
                        parsed_tx,
                        progress,
                        progress_state,
                    )
                }
            },
        ));

    let parse_res = match parse_caught {
        Ok(r) => r,
        Err(_) => Err(anyhow!("ingest parse phase panicked")),
    };

    let drain_res = drainer
        .join()
        .map_err(|_| anyhow!("ingest spill drainer panicked"))?;

    // Extract spill file and check for write errors.
    // On error, tempfile cleans up automatically when dropped.
    let (spill_tempfile, write_err) = drain_res?;
    if let Some(e) = write_err {
        return Err(e);
    }
    if parse_res.is_err() {
        // Tempfile auto-cleanup on drop
        return parse_res.map(|_| unreachable!());
    }
    let mtime_only = parse_res?;

    // Detect files removed from disk
    let disk_rels: std::collections::HashSet<&str> =
        disk_meta.iter().map(|(_, r, _)| r.as_str()).collect();
    let removed_rels: Vec<String> = known_files
        .keys()
        .filter(|fp| !disk_rels.contains(fp.as_str()))
        .cloned()
        .collect();

    Ok(ComputedChangeset {
        parsed_spill: spill_tempfile,
        mtime_only,
        removed_rels,
    })
}

// ── Phase C: brief DB write ───────────────────────────────────────────────────

/// Whether a symbol of this kind can be the *target* of a CALLS edge.
///
/// Type-level declarations (interfaces, type aliases, unions, macro
/// definitions) are never callees; leaving them in the candidate set either
/// turns a previously-unique name ambiguous (C `struct foo` + `void foo()`
/// silently drops the edge) or steals the edge outright (TS `type Button` +
/// `const Button = () => ...` in one file). C/C++ structs and enums are
/// likewise type-only, but Rust tuple-struct constructors (`Foo(..)`) and
/// class constructors (Python/C++) are real call targets, so `struct` stays
/// callable outside the c-family and `class` stays callable everywhere.
/// `macro` is safe to exclude: Rust macro invocations parse as
/// `macro_invocation`, which the call query never captures.
pub(crate) fn is_callable_symbol_kind(symbol_type: &str, language: &str) -> bool {
    match symbol_type {
        "interface" | "type" | "union" | "macro" => false,
        "struct" | "enum" => !is_c_family_ext(language),
        _ => true,
    }
}

/// Map `symbol_name -> node id` for a bounded set of names (MARROW-PERF-009).
///
/// Uses a temp table + join so we avoid scanning every row in `nodes` and stay
/// within SQLite’s bound-parameter limits for large callee sets.
/// Only returns nodes that can actually be call targets (see
/// `is_callable_symbol_kind`) — the sole consumers are CALLS-edge resolvers.
pub(crate) fn build_name_to_ids_for_symbol_names(
    conn: &Connection,
    repo_id: &str,
    names: &HashSet<String>,
) -> Result<HashMap<String, Vec<(String, String)>>> {
    let mut map: HashMap<String, Vec<(String, String)>> = HashMap::new();
    if names.is_empty() {
        return Ok(map);
    }

    conn.execute_batch(
        "DROP TABLE IF EXISTS _marrow_callee_lookup;
         CREATE TEMP TABLE _marrow_callee_lookup (name TEXT NOT NULL PRIMARY KEY);",
    )?;

    {
        let mut ins =
            conn.prepare("INSERT OR IGNORE INTO _marrow_callee_lookup(name) VALUES (?1)")?;
        for n in names {
            ins.execute(rusqlite::params![n.as_str()])?;
        }
    }

    let mut stmt = conn.prepare(
        "SELECT n.symbol_name, n.id, n.file_path, n.symbol_type, n.language FROM nodes n
         INNER JOIN _marrow_callee_lookup c ON n.symbol_name = c.name
         WHERE n.repo_id = ?1",
    )?;
    let rows = stmt.query_map(rusqlite::params![repo_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    for r in rows {
        let (name, id, file_path, symbol_type, language) = r?;
        if !is_callable_symbol_kind(&symbol_type, &language) {
            continue;
        }
        map.entry(name).or_default().push((id, file_path));
    }

    Ok(map)
}

/// Node ids currently stored for `file_path` in `repo_id`.
fn collect_node_ids_for_file(
    conn: &Connection,
    repo_id: &str,
    file_path: &str,
) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare("SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2")?;
    let rows = stmt.query_map(rusqlite::params![repo_id, file_path], |row| row.get(0))?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Remove edges that referenced symbols removed from a file (MARROW-PERF-011).
pub(crate) fn delete_edges_touching_removed_ids(
    conn: &Connection,
    removed_ids: &[String],
) -> Result<()> {
    if removed_ids.is_empty() {
        return Ok(());
    }
    let mut stmt = conn.prepare("DELETE FROM edges WHERE source_id = ?1 OR target_id = ?1")?;
    for id in removed_ids {
        stmt.execute(rusqlite::params![id])?;
    }
    Ok(())
}

/// Batched `CALLS` inserts (MARROW-PERF-010).
fn flush_calls_edge_batch(conn: &Connection, pairs: &[(String, String)]) -> Result<usize> {
    const CHUNK: usize = 500;
    if pairs.is_empty() {
        return Ok(0);
    }
    let mut inserted = 0usize;
    for chunk in pairs.chunks(CHUNK) {
        let values_sql = chunk
            .iter()
            .map(|_| "(?, ?, 'CALLS')")
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT OR IGNORE INTO edges (source_id, target_id, relationship_type) VALUES {values_sql}"
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<rusqlite::types::Value> = chunk
            .iter()
            .flat_map(|(s, t)| {
                [
                    rusqlite::types::Value::Text(s.clone()),
                    rusqlite::types::Value::Text(t.clone()),
                ]
            })
            .collect();
        let n = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
        inserted += n;
    }
    Ok(inserted)
}

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
    progress_state: &Mutex<u8>,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    crate::db::mark_graph_degrees_dirty(conn, repo_id)?;

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

    // Tempfile will auto-cleanup when dropped; no manual remove_file needed.
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
    progress_state: &Mutex<u8>,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let ComputedChangeset {
        parsed_spill,
        mtime_only,
        removed_rels,
    } = changeset;

    // Remove nodes + file records for deleted files
    for file_path in &removed_rels {
        let syms: Vec<String> = {
            let mut s = conn
                .prepare("SELECT symbol_name FROM nodes WHERE repo_id = ?1 AND file_path = ?2")?;
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
        conn.execute(
            "DELETE FROM file_imports WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
    }

    // Apply each parsed file from the spill file (bounded channel + drainer limited parse-phase RSS).
    // While writing symbols to the DB, accumulate (source_node_id, callee_name) pairs directly
    // from the spill data — raw_text is already in memory here, so we avoid re-querying the DB
    // a second time for the CALLS edge resolution pass (eliminates two redundant full-table scans).
    let mut changed_paths: HashSet<String> = HashSet::new();
    let mut all_callee_names: HashSet<String> = HashSet::new();
    let mut pending_calls: Vec<(String, String, String)> = Vec::new();
    let spill_file = fs::File::open(parsed_spill.path())?;
    let mut spill_reader = BufReader::new(spill_file);
    while let Some((file_path, lang, symbols, hash, mtime)) =
        read_spill_parsed_row(&mut spill_reader)?
    {
        changed_paths.insert(file_path.clone());
        let old_ids = collect_node_ids_for_file(conn, repo_id, &file_path)?;
        // Outgoing CALLS from this file must be rebuilt; drop edges whose *source* is here.
        // Inbound CALLS targeting stable node ids are kept (MARROW-PERF-011).
        conn.execute(
            "DELETE FROM edges WHERE source_id IN (
                SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
            rusqlite::params![repo_id, file_path],
        )?;

        let new_ids: HashSet<String> = symbols
            .iter()
            .map(|s| make_node_id(repo_id, &file_path, &s.symbol_type, &s.name, s.start_byte))
            .collect();
        let removed: Vec<String> = old_ids.difference(&new_ids).cloned().collect();
        delete_edges_touching_removed_ids(conn, &removed)?;
        for id in &removed {
            conn.execute("DELETE FROM nodes WHERE id = ?1", rusqlite::params![id])?;
        }

        // Upsert in-place so stable `id` rows survive (FK-safe inbound edges from other files).
        for sym in &symbols {
            let node_id = make_node_id(
                repo_id,
                &file_path,
                &sym.symbol_type,
                &sym.name,
                sym.start_byte,
            );
            let new_hash = crate::db::hash_raw_text(&sym.raw_text);
            if old_ids.contains(&node_id) {
                conn.execute(
                    "UPDATE nodes SET language = ?1, symbol_name = ?2, symbol_type = ?3, raw_text = ?4, \
                     source_start_byte = ?5, source_end_byte = ?6, start_line = ?7, start_column = ?8, \
                     end_line = ?9, end_column = ?10 WHERE id = ?11",
                    rusqlite::params![
                        lang,
                        sym.name,
                        sym.symbol_type,
                        sym.raw_text,
                        sym.start_byte as i64,
                        sym.end_byte as i64,
                        sym.start_line as i64,
                        sym.start_column as i64,
                        sym.end_line as i64,
                        sym.end_column as i64,
                        node_id
                    ],
                )?;
            } else {
                conn.prepare_cached(
                    "INSERT OR REPLACE INTO nodes \
                     (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text, \
                      source_start_byte, source_end_byte, start_line, start_column, end_line, end_column)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                )?
                .execute(rusqlite::params![
                    node_id,
                    repo_id,
                    file_path,
                    lang,
                    sym.name,
                    sym.symbol_type,
                    sym.raw_text,
                    sym.start_byte as i64,
                    sym.end_byte as i64,
                    sym.start_line as i64,
                    sym.start_column as i64,
                    sym.end_line as i64,
                    sym.end_column as i64
                ])?;
            }
            crate::db::mark_stale_observations(conn, repo_id, &sym.name, &file_path, &new_hash)?;

            // Callees were pre-computed during the parallel parse phase and stored in the
            // spill file — no tree-sitter re-parse needed here.
            for callee_name in &sym.callees {
                if callee_name != &sym.name {
                    all_callee_names.insert(callee_name.clone());
                    pending_calls.push((node_id.clone(), callee_name.clone(), file_path.clone()));
                }
            }
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

    // Resolve callee names → node IDs in one bulk lookup (MARROW-PERF-009).
    // Callee names were collected during the spill-read above — no DB re-scan needed.
    let name_to_ids = build_name_to_ids_for_symbol_names(conn, repo_id, &all_callee_names)?;

    // M-5 FIX: Scope CALLS edges. Prefer same-file targets; if no same-file
    // candidate, emit only when the callee name resolves unambiguously (one
    // target in the repo). Ambiguous names produce no CALLS edge rather than
    // false-positive global links.
    let mut calls_batch: Vec<(String, String)> = Vec::new();
    for (source_id, callee_name, source_file) in &pending_calls {
        if let Some(targets) = name_to_ids.get(callee_name.as_str()) {
            let same_file: Vec<&str> = targets
                .iter()
                .filter(|(_, fp)| fp == source_file)
                .map(|(id, _)| id.as_str())
                .collect();
            if !same_file.is_empty() {
                for id in same_file {
                    calls_batch.push((source_id.clone(), id.to_string()));
                }
            } else if targets.len() == 1 {
                calls_batch.push((source_id.clone(), targets[0].0.clone()));
            }
            // else: ambiguous (multiple targets in different files), skip
        }
    }
    // M-9 FIX: Report actual inserted rows, not submitted batch size.
    let calls_edge_count = flush_calls_edge_batch(conn, &calls_batch)?;

    // M-6 FIX: Extract file-level imports from full source text (not symbol
    // raw_text) so top-level imports outside captured symbol bodies are visible
    // for cross-repo IMPORTS resolution.
    let repo_root: String = conn.query_row(
        "SELECT root_path FROM repositories WHERE id = ?1",
        rusqlite::params![repo_id],
        |row| row.get(0),
    )?;
    for file_path in &changed_paths {
        conn.execute(
            "DELETE FROM file_imports WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, file_path],
        )?;
        let abs_path = PathBuf::from(&repo_root).join(file_path);
        if let Ok(source) = fs::read_to_string(&abs_path) {
            let lang_ext = abs_path.extension().and_then(|e| e.to_str()).unwrap_or("");
            let imports = extract_imports(&source, lang_ext);
            if !imports.is_empty() {
                let mut ins = conn.prepare_cached(
                    "INSERT OR IGNORE INTO file_imports (repo_id, file_path, import_name) VALUES (?1, ?2, ?3)",
                )?;
                for name in &imports {
                    ins.execute(rusqlite::params![repo_id, file_path, name])?;
                }
            }
        }
    }

    maybe_emit_progress(progress, progress_state, 95);

    crate::db::refresh_graph_degrees(conn, repo_id)?;

    let total: usize = conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get::<_, i64>(0),
    )? as usize;

    Ok((total, calls_edge_count))
}

/// Re-index a single file in place — the shared implementation behind both
/// watch paths (`marrow watch` and the dashboard watcher).
///
/// Mirrors the per-file section of `write_changeset_body`: node ids come from
/// `make_node_id`, unchanged ids survive as UPDATEs so inbound CALLS edges
/// stay alive (MARROW-PERF-011), removed ids take their edges with them, and
/// outgoing CALLS edges are rebuilt from the symbols' pre-computed callees
/// with the same same-file-then-unambiguous scoping as full ingest.
/// Pass `None` for `parsed_symbols` when the file was deleted.
/// Returns the number of symbols written.
pub fn apply_single_file_update(
    conn: &Connection,
    repo_id: &str,
    rel_path: &str,
    lang_ext: &str,
    parsed_symbols: Option<Vec<Symbol>>,
) -> Result<usize> {
    let tx = conn.unchecked_transaction()?;
    crate::db::mark_graph_degrees_dirty(&tx, repo_id)?;

    let old_ids: HashSet<String> = tx
        .prepare("SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2")?
        .query_map(rusqlite::params![repo_id, rel_path], |row| {
            row.get::<_, String>(0)
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Outgoing CALLS from this file are rebuilt below; inbound edges onto
    // stable node ids survive (MARROW-PERF-011).
    tx.execute(
        "DELETE FROM edges WHERE source_id IN (
            SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2)",
        rusqlite::params![repo_id, rel_path],
    )?;

    let Some(symbols) = parsed_symbols else {
        // File deleted: stale-mark its observations, then drop its nodes and
        // any edges still touching them.
        let existing_symbols: Vec<String> = tx
            .prepare("SELECT symbol_name FROM nodes WHERE repo_id = ?1 AND file_path = ?2")?
            .query_map(rusqlite::params![repo_id, rel_path], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();
        for symbol_name in &existing_symbols {
            crate::db::mark_deleted_observation_stale(&tx, repo_id, symbol_name, rel_path)?;
        }
        let removed: Vec<String> = old_ids.into_iter().collect();
        delete_edges_touching_removed_ids(&tx, &removed)?;
        tx.execute(
            "DELETE FROM nodes WHERE repo_id = ?1 AND file_path = ?2",
            rusqlite::params![repo_id, rel_path],
        )?;
        tx.commit()?;
        resolve_cross_repo_after_ingest(conn, repo_id)?;
        return Ok(0);
    };

    let new_ids: HashSet<String> = symbols
        .iter()
        .map(|s| make_node_id(repo_id, rel_path, &s.symbol_type, &s.name, s.start_byte))
        .collect();
    let removed: Vec<String> = old_ids.difference(&new_ids).cloned().collect();
    delete_edges_touching_removed_ids(&tx, &removed)?;
    for id in &removed {
        tx.execute("DELETE FROM nodes WHERE id = ?1", rusqlite::params![id])?;
    }

    let mut callee_names: HashSet<String> = HashSet::new();
    for sym in &symbols {
        let node_id = make_node_id(repo_id, rel_path, &sym.symbol_type, &sym.name, sym.start_byte);
        let new_hash = crate::db::hash_raw_text(&sym.raw_text);
        if old_ids.contains(&node_id) {
            tx.execute(
                "UPDATE nodes SET language = ?1, symbol_name = ?2, symbol_type = ?3, raw_text = ?4, \
                 source_start_byte = ?5, source_end_byte = ?6, start_line = ?7, start_column = ?8, \
                 end_line = ?9, end_column = ?10 WHERE id = ?11",
                rusqlite::params![
                    lang_ext,
                    sym.name,
                    sym.symbol_type,
                    sym.raw_text,
                    sym.start_byte as i64,
                    sym.end_byte as i64,
                    sym.start_line as i64,
                    sym.start_column as i64,
                    sym.end_line as i64,
                    sym.end_column as i64,
                    node_id
                ],
            )?;
        } else {
            tx.execute(
                "INSERT OR REPLACE INTO nodes \
                 (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text, \
                  source_start_byte, source_end_byte, start_line, start_column, end_line, end_column)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                rusqlite::params![
                    node_id,
                    repo_id,
                    rel_path,
                    lang_ext,
                    sym.name,
                    sym.symbol_type,
                    sym.raw_text,
                    sym.start_byte as i64,
                    sym.end_byte as i64,
                    sym.start_line as i64,
                    sym.start_column as i64,
                    sym.end_line as i64,
                    sym.end_column as i64
                ],
            )?;
        }
        crate::db::mark_stale_observations(&tx, repo_id, &sym.name, rel_path, &new_hash)?;
        for callee_name in &sym.callees {
            if callee_name != &sym.name {
                callee_names.insert(callee_name.clone());
            }
        }
    }

    let name_to_ids = build_name_to_ids_for_symbol_names(&tx, repo_id, &callee_names)?;
    let mut calls_batch: Vec<(String, String)> = Vec::new();
    for sym in &symbols {
        let source_id = make_node_id(repo_id, rel_path, &sym.symbol_type, &sym.name, sym.start_byte);
        for callee_name in &sym.callees {
            if callee_name == &sym.name {
                continue;
            }
            if let Some(targets) = name_to_ids.get(callee_name.as_str()) {
                // M-5 scoping: prefer same-file targets; else emit only if the
                // name resolves unambiguously (single target).
                let same_file: Vec<&str> = targets
                    .iter()
                    .filter(|(_, fp)| fp == rel_path)
                    .map(|(id, _)| id.as_str())
                    .collect();
                if !same_file.is_empty() {
                    for id in same_file {
                        calls_batch.push((source_id.clone(), id.to_string()));
                    }
                } else if targets.len() == 1 {
                    calls_batch.push((source_id.clone(), targets[0].0.clone()));
                }
                // else: ambiguous cross-file, skip
            }
        }
    }
    flush_calls_edge_batch(&tx, &calls_batch)?;

    tx.commit()?;
    resolve_cross_repo_after_ingest(conn, repo_id)?;
    Ok(symbols.len())
}

// ── Composed entry points ─────────────────────────────────────────────────────

fn ingest_repo_with_progress<F>(
    conn: &Connection,
    repo_id: &str,
    root_path: &Path,
    progress: &F,
    progress_state: &Mutex<u8>,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let root_path = root_path
        .canonicalize()
        .unwrap_or_else(|_| root_path.to_path_buf());

    if !root_path.exists() {
        return Err(anyhow!(
            "The specified root_path does not exist: {}",
            root_path.display()
        ));
    }

    maybe_emit_progress(progress, progress_state, 5);

    let known_files = load_known_files(conn, repo_id, &root_path)?;
    let changeset = compute_changeset(&known_files, &root_path, progress, progress_state)?;
    write_changeset(conn, repo_id, changeset, progress, progress_state)
}

/// Ingest an entire repository incrementally: only re-parse files whose
/// content hash has changed since the last index run. First-time ingest
/// is a full pass. Returns `(total_symbol_count, calls_edge_count)`.
#[allow(dead_code)] // retained for tests and direct/manual ingestion entry points
pub fn ingest_repo(conn: &Connection, repo_id: &str, root_path: &Path) -> Result<(usize, usize)> {
    let progress_state = Mutex::new(0u8);
    ingest_repo_with_progress(conn, repo_id, root_path, &|_| {}, &progress_state)
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
    let progress_state = Mutex::new(0u8);
    let (symbols, calls_edges) =
        ingest_repo_with_progress(conn, repo_id, root_path, &progress, &progress_state)?;
    maybe_emit_progress(&progress, &progress_state, 95);
    let import_edges = resolve_cross_repo_after_ingest(conn, repo_id)?;
    maybe_emit_progress(&progress, &progress_state, 100);
    crate::db::post_ingest_maintenance(conn)?;
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
        return Err(anyhow!(
            "The specified root_path does not exist: {}",
            root_path.display()
        ));
    }

    let progress_state = Mutex::new(0u8);

    // Phase A: brief DB read — lock acquired, then immediately released.
    let known_files = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        maybe_emit_progress(&progress, &progress_state, 5);
        load_known_files(&conn, repo_id, &root_path)?
    };

    // Phase B: pure CPU/IO — DB mutex is NOT held.
    let changeset = compute_changeset(&known_files, &root_path, &progress, &progress_state)?;

    // Phase C: brief DB write — lock acquired, then released.
    let (total, calls_edges) = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        write_changeset(&conn, repo_id, changeset, &progress, &progress_state)?
    };

    // Phase D: cross-repo edges + vacuum — brief lock.
    let import_edges = {
        let conn = db.lock().map_err(|_| anyhow!("DB mutex poisoned"))?;
        maybe_emit_progress(&progress, &progress_state, 95);
        let edges = resolve_cross_repo_after_ingest(&conn, repo_id)?;
        crate::db::post_ingest_maintenance(&conn)?;
        maybe_emit_progress(&progress, &progress_state, 100);
        edges
    };

    Ok((total, calls_edges + import_edges))
}

pub fn run_ingestion_with_arc_and_activity<F>(
    db: &Arc<Mutex<Connection>>,
    repo_id: &str,
    root_path: &Path,
    progress: F,
    activity: Option<crate::activity::ActivityTracker>,
    workspace_id: Option<String>,
) -> Result<(usize, usize)>
where
    F: Fn(u8) + Send + Sync,
{
    let activity_id = activity.as_ref().map(|tracker| {
        tracker.start(
            crate::activity::ActivityKind::IndexingJob,
            workspace_id,
            format!("indexing {}", root_path.display()),
        )
    });
    let result = run_ingestion_with_arc(db, repo_id, root_path, progress);
    if let (Some(tracker), Some(id)) = (&activity, activity_id.as_deref()) {
        match &result {
            Ok((symbols, edges)) => tracker.finish(
                id,
                crate::activity::ActivityState::Completed,
                format!("indexed {symbols} symbols / {edges} edges"),
            ),
            Err(error) => {
                tracker.finish(id, crate::activity::ActivityState::Error, error.to_string())
            }
        }
    }
    result
}

/// When set to `1`/`true`/`yes`, `resolve_cross_repo_after_ingest` scans **all** repos as
/// import sources (legacy behavior). Default (unset): only the repo just ingested is
/// scanned (MARROW-PERF-012).
fn wants_full_cross_repo_import_scan() -> bool {
    matches!(
        std::env::var("MARROW_CROSS_REPO_FULL_SCAN")
            .ok()
            .as_deref()
            .map(str::trim),
        Some("1") | Some("true") | Some("yes")
    )
}

/// Run the cross-repo IMPORTS pass after indexing `repo_id`, respecting
/// `MARROW_CROSS_REPO_FULL_SCAN`.
pub fn resolve_cross_repo_after_ingest(conn: &Connection, repo_id: &str) -> Result<usize> {
    if wants_full_cross_repo_import_scan() {
        resolve_cross_repo_edges(conn, None)
    } else {
        resolve_cross_repo_edges(conn, Some(repo_id))
    }
}

/// Secondary pass: resolve cross-repo import edges.
///
/// M-6 FIX: Uses the `file_imports` table (populated from whole-file text during
/// ingestion) instead of scanning node `raw_text`. This captures top-level imports
/// that appear outside any captured symbol body.
///
/// `source_repo_scope`: when `Some(rid)`, only files with `repo_id = rid` are scanned
/// (typical after `run_ingestion`). When `None`, every file is scanned.
pub fn resolve_cross_repo_edges(
    conn: &Connection,
    source_repo_scope: Option<&str>,
) -> Result<usize> {
    // Pass 1 — collect imports from file_imports table
    // import_name -> Vec<(source_node_id, source_repo_id)>
    let fi_sql = match source_repo_scope {
        Some(_) => "SELECT import_name, repo_id, file_path FROM file_imports WHERE repo_id = ?1",
        None => "SELECT import_name, repo_id, file_path FROM file_imports",
    };
    let mut fi_stmt = conn.prepare(fi_sql)?;
    let mut fi_rows = match source_repo_scope {
        Some(rid) => fi_stmt.query(rusqlite::params![rid])?,
        None => fi_stmt.query([])?,
    };

    // Cache: (repo_id, file_path) -> first node id in that file
    let mut file_node_cache: std::collections::HashMap<(String, String), Option<String>> =
        std::collections::HashMap::new();

    let mut import_map: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();

    while let Some(row) = fi_rows.next()? {
        let import_name: String = row.get(0)?;
        let source_repo: String = row.get(1)?;
        let file_path: String = row.get(2)?;

        let cache_key = (source_repo.clone(), file_path.clone());
        let source_node = file_node_cache
            .entry(cache_key)
            .or_insert_with_key(|(repo, fp)| {
                conn.query_row(
                    "SELECT id FROM nodes WHERE repo_id = ?1 AND file_path = ?2 LIMIT 1",
                    rusqlite::params![repo, fp],
                    |row| row.get(0),
                )
                .ok()
            });

        if let Some(source_id) = source_node {
            import_map
                .entry(import_name)
                .or_default()
                .push((source_id.clone(), source_repo));
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
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .for_each(|(name, id, repo)| {
                target_map.entry(name).or_default().push((id, repo));
            });
    }

    // Pass 3 — resolve edges in memory, insert in one transaction
    let tx = conn.unchecked_transaction()?;
    let mut edge_count = 0;
    let mut touched_repos: HashSet<String> = HashSet::new();

    for (import_name, sources) in &import_map {
        let Some(targets) = target_map.get(import_name) else {
            continue;
        };
        for (source_id, source_repo) in sources {
            // Only cross-repo targets
            let cross_repo: Vec<(&String, &String)> = targets
                .iter()
                .filter(|(_, target_repo)| target_repo != source_repo)
                .map(|(id, repo)| (id, repo))
                .collect();
            // Skip ambiguous (multiple targets across repos)
            if cross_repo.len() == 1 {
                let (target_id, target_repo) = cross_repo[0];
                let inserted = tx.execute(
                    "INSERT OR IGNORE INTO edges (source_id, target_id, relationship_type)
                     VALUES (?1, ?2, 'IMPORTS')",
                    rusqlite::params![source_id, target_id],
                )?;
                if inserted > 0 {
                    touched_repos.insert(source_repo.clone());
                    touched_repos.insert(target_repo.clone());
                }
                edge_count += 1;
            }
        }
    }

    let mut touched_repo_list: Vec<String> = touched_repos.into_iter().collect();
    touched_repo_list.sort();
    for repo_id in &touched_repo_list {
        crate::db::mark_graph_degrees_dirty(&tx, repo_id)?;
    }

    tx.commit()?;
    for repo_id in touched_repo_list {
        crate::db::refresh_graph_degrees(conn, &repo_id)?;
    }
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
            "c" | "cpp" | "cc" | "cxx" | "h" | "hpp" => {
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
    use std::sync::Mutex;

    #[test]
    fn test_call_query_parses_all_languages() {
        let exts = ["py", "ts", "tsx", "c", "cpp", "rs", "rb"];
        for ext in exts {
            let lang = language_for_ext(ext).unwrap_or_else(|| panic!("no language for {ext}"));
            let qsrc = call_query_for_ext(ext).unwrap_or_else(|| panic!("no call query for {ext}"));
            Query::new(&lang, qsrc).unwrap_or_else(|_| panic!("query parse failed for {ext}"));
        }
    }

    #[test]
    fn test_symbol_query_parses_all_languages() {
        let exts = ["py", "ts", "tsx", "c", "cpp", "rs", "rb"];
        for ext in exts {
            let config = lang_config_for_ext(ext).unwrap_or_else(|| panic!("no config for {ext}"));
            Query::new(&config.language, config.query_src)
                .unwrap_or_else(|_| panic!("symbol query parse failed for {ext}"));
        }
    }

    fn symbol_pairs_from_source(ext: &str, source: &str) -> Vec<(String, String)> {
        let config = lang_config_for_ext(ext).unwrap_or_else(|| panic!("no config for {ext}"));
        parse_file_inner(source, ext, &config, "<test>")
            .unwrap()
            .into_iter()
            .map(|symbol| (symbol.name, symbol.symbol_type))
            .collect()
    }

    fn assert_has_symbol(symbols: &[(String, String)], name: &str, kind: &str) {
        assert!(
            symbols.iter().any(|(n, k)| n == name && k == kind),
            "missing {kind} {name}: {symbols:?}"
        );
    }

    fn assert_missing_symbol_name(symbols: &[(String, String)], name: &str) {
        assert!(
            !symbols.iter().any(|(n, _)| n == name),
            "unexpected symbol {name}: {symbols:?}"
        );
    }

    #[test]
    fn cpp_symbols_cover_type_specifiers_typedefs_and_c_extension() {
        let symbols = symbol_pairs_from_source(
            "h",
            r#"
struct Forward;
class Widget;
struct ggml_tensor { int n_dims; };
typedef struct { int x; } ggml_cplan;
typedef struct named_s { int y; } named_alias;
union ggml_value { int i; float f; };
enum ggml_status { GGML_OK };
"#,
        );

        // Named tags are struct/union/enum symbols — including inside typedefs
        // (ggml's `*_s` tag idiom must be findable).
        assert_has_symbol(&symbols, "ggml_tensor", "struct");
        assert_has_symbol(&symbols, "named_s", "struct");
        assert_has_symbol(&symbols, "ggml_value", "union");
        assert_has_symbol(&symbols, "ggml_status", "enum");
        // Typedef aliases are `type` symbols, distinguishable from the tag.
        assert_has_symbol(&symbols, "ggml_cplan", "type");
        assert_has_symbol(&symbols, "named_alias", "type");
        // Forward declarations and anonymous specifiers are not indexed.
        assert_missing_symbol_name(&symbols, "Forward");
        assert_missing_symbol_name(&symbols, "Widget");
        assert_missing_symbol_name(&symbols, "anonymous");

        let c_symbols = symbol_pairs_from_source("c", "struct c_file_type { int id; };\n");
        assert_has_symbol(&c_symbols, "c_file_type", "struct");
    }

    #[test]
    fn cpp_typedef_multi_declarator_pointer_and_class_variants() {
        let symbols = symbol_pairs_from_source(
            "h",
            r#"
typedef struct { int x; } A, B;
typedef struct { int y; } *ptr_t;
typedef class { public: void run(); } Runner;
typedef class Named { public: int v; } NamedAlias;
typedef enum { RED, GREEN } color_t;
"#,
        );

        assert_has_symbol(&symbols, "A", "type");
        assert_has_symbol(&symbols, "B", "type");
        assert_has_symbol(&symbols, "ptr_t", "type");
        assert_has_symbol(&symbols, "Runner", "type");
        assert_has_symbol(&symbols, "NamedAlias", "type");
        assert_has_symbol(&symbols, "Named", "class");
        assert_has_symbol(&symbols, "color_t", "type");
        assert_missing_symbol_name(&symbols, "anonymous");
    }

    /// Anonymous specifiers — nested unions (ubiquitous in ggml), top-level
    /// enum constant blocks, function-local structs — must not produce
    /// symbols named "anonymous".
    #[test]
    fn cpp_anonymous_specifiers_are_not_indexed() {
        let symbols = symbol_pairs_from_source(
            "h",
            r#"
struct outer {
    union { int i; float f; } u;
};
enum { GGML_MAX_DIMS = 4 };
void local_tmp(void) {
    struct { int a; } tmp;
    (void) tmp;
}
"#,
        );

        assert_has_symbol(&symbols, "outer", "struct");
        assert_has_symbol(&symbols, "local_tmp", "function");
        assert_missing_symbol_name(&symbols, "anonymous");
    }

    /// `name:` is a wildcard field match so template specializations keep
    /// their index presence under the written name.
    #[test]
    fn cpp_template_specialization_struct_still_indexed() {
        let symbols = symbol_pairs_from_source(
            "hpp",
            "template <typename T> struct Widget { T v; };\n\
             template <> struct Widget<int> { int v; };\n",
        );

        assert_has_symbol(&symbols, "Widget", "struct");
        assert_has_symbol(&symbols, "Widget<int>", "struct");
    }

    #[test]
    fn typescript_symbols_cover_structural_types_and_arrow_functions() {
        let symbols = symbol_pairs_from_source(
            "ts",
            r#"
interface Props { title: string }
type Handler = () => void;
enum Status { Ready }
const staticValue = 5;
export const loadData = async () => fetchData();
let transform = (value: number) => value.toString();
"#,
        );

        assert_has_symbol(&symbols, "Props", "interface");
        assert_has_symbol(&symbols, "Handler", "type");
        assert_has_symbol(&symbols, "Status", "enum");
        assert_has_symbol(&symbols, "loadData", "function");
        assert_has_symbol(&symbols, "transform", "function");
        assert_missing_symbol_name(&symbols, "staticValue");
    }

    /// T9: arrow/function-expression bindings anchor to module scope, and
    /// wrapper calls index only through the memo/forwardRef allowlist.
    #[test]
    fn typescript_arrow_anchoring_and_wrapper_allowlist() {
        let symbols = symbol_pairs_from_source(
            "ts",
            r#"
export const TopLevel = () => 1;
export const Wrapped = memo(() => 2);
export const RefComp = React.forwardRef(() => 3);
export const Memoed = React.memo(() => 4);
const expr = function () { return 5; };
var legacy = () => 6;
export const NotAComponent = useMemo(() => 7);
const debounced = debounce(() => 8);
function outer() {
    const local = () => 9;
    return local;
}
const promise = Promise.resolve().then(() => 10);
"#,
        );

        assert_has_symbol(&symbols, "TopLevel", "function");
        assert_has_symbol(&symbols, "Wrapped", "function");
        assert_has_symbol(&symbols, "RefComp", "function");
        assert_has_symbol(&symbols, "Memoed", "function");
        assert_has_symbol(&symbols, "expr", "function");
        assert_has_symbol(&symbols, "legacy", "function");
        assert_has_symbol(&symbols, "outer", "function");
        assert_missing_symbol_name(&symbols, "NotAComponent");
        assert_missing_symbol_name(&symbols, "debounced");
        assert_missing_symbol_name(&symbols, "local");
        assert_missing_symbol_name(&symbols, "promise");
    }

    #[test]
    fn tsx_wrapped_components_indexed_and_local_handlers_suppressed() {
        let symbols = symbol_pairs_from_source(
            "tsx",
            r#"
export const Memo = memo(() => <div />);
export const Card = () => {
    const handleClick = () => {};
    return <button onClick={handleClick} />;
};
"#,
        );

        assert_has_symbol(&symbols, "Memo", "function");
        assert_has_symbol(&symbols, "Card", "function");
        assert_missing_symbol_name(&symbols, "handleClick");
    }

    /// Impl-body `type Item = ...` must not become a top-level type symbol
    /// (50 impls of Iterator would index 50 colliding `Item`s), while
    /// module-level aliases stay indexed.
    #[test]
    fn rust_impl_associated_types_are_not_indexed() {
        let symbols = symbol_pairs_from_source(
            "rs",
            r#"
type TopLevel<T> = Option<T>;
struct Counter;
impl Iterator for Counter {
    type Item = u32;
    fn next(&mut self) -> Option<u32> {
        None
    }
}
"#,
        );

        assert_has_symbol(&symbols, "TopLevel", "type");
        assert_missing_symbol_name(&symbols, "Item");
    }

    #[test]
    fn tsx_symbols_cover_arrow_components() {
        let symbols = symbol_pairs_from_source(
            "tsx",
            r#"
interface Props { title: string }
export const Panel = (props: Props) => <section>{props.title}</section>;
"#,
        );

        assert_has_symbol(&symbols, "Props", "interface");
        assert_has_symbol(&symbols, "Panel", "function");
    }

    #[test]
    fn rust_symbols_cover_macros_type_aliases_and_unions() {
        let symbols = symbol_pairs_from_source(
            "rs",
            r#"
macro_rules! say { () => {}; }
type Alias<T> = Option<T>;
union Bits { i: u32, f: f32 }
"#,
        );

        assert_has_symbol(&symbols, "say", "macro");
        assert_has_symbol(&symbols, "Alias", "type");
        assert_has_symbol(&symbols, "Bits", "union");
    }

    #[test]
    fn python_symbols_cover_pep_695_type_aliases() {
        let symbols = symbol_pairs_from_source(
            "py",
            r#"
type Vector[T] = list[T]
async def af():
    pass
"#,
        );

        assert_has_symbol(&symbols, "Vector", "type");
        assert_has_symbol(&symbols, "af", "function");
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
        assert!(
            calls.contains(&"method".to_string()),
            "missing method: {calls:?}"
        );
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
        assert!(
            calls.contains(&"helper".to_string()),
            "missing helper: {calls:?}"
        );
        assert!(
            calls.contains(&"method".to_string()),
            "missing method: {calls:?}"
        );
        assert!(
            calls.contains(&"read".to_string()),
            "missing read: {calls:?}"
        );
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
        assert!(
            calls.contains(&"method".to_string()),
            "missing method: {calls:?}"
        );
    }

    #[test]
    fn test_ingest_creates_calls_edges() {
        let conn = crate::db::init_db(":memory:").unwrap();

        // Insert two symbols: caller calls callee
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

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
        let edge_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE relationship_type = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(edge_count >= 1, "no CALLS edges in DB");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T1 regression: indexing `struct foo` must not make a cross-file call to
    /// `void foo()` ambiguous — type-level twins are excluded from the CALLS
    /// candidate set, so the function keeps its edge.
    #[test]
    fn test_c_struct_name_twin_does_not_break_function_call_edge() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_kind_aware_c_twin");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.h"), "struct foo { int x; };\n").unwrap();
        std::fs::write(dir.join("foo.c"), "void foo(void) {}\n").unwrap();
        std::fs::write(dir.join("caller.c"), "void bar(void) { foo(); }\n").unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        let to_function: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes s ON s.id = e.source_id \
                 JOIN nodes t ON t.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND s.symbol_name = 'bar' \
                   AND t.symbol_name = 'foo' AND t.symbol_type = 'function'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            to_function, 1,
            "same-name struct must not turn the function callee ambiguous"
        );

        let to_struct: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes t ON t.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND t.symbol_name = 'foo' AND t.symbol_type != 'function'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(to_struct, 0, "no CALLS edge may target the struct node");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T1 regression: TS `type Button` + `const Button = () => ...` in one
    /// file — a `Button()` call must land on the function node only.
    #[test]
    fn test_ts_type_value_twin_resolves_call_to_function_not_type() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_kind_aware_ts_twin");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("button.ts"),
            "type Button = { label: string };\n\
             export const Button = () => ({ label: \"ok\" });\n\
             export const render = () => Button();\n",
        )
        .unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        let to_function: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes s ON s.id = e.source_id \
                 JOIN nodes t ON t.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND s.symbol_name = 'render' \
                   AND t.symbol_name = 'Button' AND t.symbol_type = 'function'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(to_function, 1, "call must resolve to the value binding");

        let to_type: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes t ON t.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND t.symbol_name = 'Button' AND t.symbol_type = 'type'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(to_type, 0, "no CALLS edge may target the type alias node");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T1 guard: Rust tuple-struct constructors are real call targets and must
    /// keep their edges under the kind-aware filter.
    #[test]
    fn test_rust_tuple_struct_constructor_call_still_resolves() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_kind_aware_rs_tuple");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("newtype.rs"),
            "pub struct Newtype(pub u32);\n\npub fn make() -> Newtype {\n    Newtype(1)\n}\n",
        )
        .unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        let to_struct: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes s ON s.id = e.source_id \
                 JOIN nodes t ON t.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND s.symbol_name = 'make' \
                   AND t.symbol_name = 'Newtype' AND t.symbol_type = 'struct'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(to_struct, 1, "Rust struct must stay a valid call target");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// T4: a watch-path update must produce byte-identical node ids to full
    /// ingest — same-name twins (TS `type Button` + `const Button`) collapse
    /// onto one row or dangle inbound edges otherwise.
    #[test]
    fn test_watch_update_node_ids_match_full_ingest() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_watch_id_parity");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("button.ts");
        std::fs::write(
            &file,
            "type Button = { label: string };\n\
             export const Button = () => ({ label: \"ok\" });\n",
        )
        .unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        let ids_after_ingest = |conn: &Connection| -> Vec<String> {
            let mut stmt = conn
                .prepare("SELECT id FROM nodes WHERE repo_id = 'test' ORDER BY id")
                .unwrap();
            stmt.query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok())
                .collect()
        };
        let full_ingest_ids = ids_after_ingest(&conn);
        assert_eq!(
            full_ingest_ids.len(),
            2,
            "type + const twins must be two nodes: {full_ingest_ids:?}"
        );

        // Re-index the same file through the watch path.
        let (ext, symbols) = parse_file(&file).unwrap();
        apply_single_file_update(&conn, repo_id, "button.ts", &ext, Some(symbols)).unwrap();

        let watch_ids = ids_after_ingest(&conn);
        assert_eq!(
            full_ingest_ids, watch_ids,
            "watch updates must write make_node_id-keyed rows"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Re-saving a file with unchanged content must keep every node and edge.
    /// (Regression: the old watcher path deleted all rows for the file and
    /// then UPDATEd the surviving ids — updating deleted rows no-ops, so
    /// unchanged symbols silently vanished on every re-save.)
    #[test]
    fn test_watch_resave_unchanged_file_keeps_nodes_and_edges() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_watch_resave");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("calls.py");
        std::fs::write(&file, "def helper():\n    pass\n\ndef main():\n    helper()\n").unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        for _ in 0..2 {
            let (ext, symbols) = parse_file(&file).unwrap();
            apply_single_file_update(&conn, repo_id, "calls.py", &ext, Some(symbols)).unwrap();
        }

        let nodes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(nodes, 2, "unchanged symbols must survive re-saves");

        let edges: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE relationship_type = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edges, 1, "the CALLS edge must survive re-saves");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// MARROW-PERF-009: only changed files are re-parsed; callee may live in an unchanged file.
    #[test]
    fn test_partial_reingest_resolves_calls_to_unchanged_file() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_partial_calls");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("other.py"), "def helper():\n    pass\n").unwrap();
        std::fs::write(dir.join("caller.py"), "def main():\n    helper()\n").unwrap();

        let (_syms, calls) = ingest_repo(&conn, repo_id, &dir).unwrap();
        assert!(calls >= 1, "initial ingest should create CALLS to helper");

        // Only caller.py changes; other.py stays out of the changeset.
        std::fs::write(
            dir.join("caller.py"),
            "def main():\n    helper()\n# touch\n",
        )
        .unwrap();

        let (_syms, calls2) = ingest_repo(&conn, repo_id, &dir).unwrap();
        assert!(
            calls2 >= 1,
            "narrow name_to_ids must still resolve helper in unchanged file; got calls={calls2}"
        );

        let edge_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE relationship_type = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(edge_count >= 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// MARROW-PERF-011: callee file reindexed alone; caller file unchanged — inbound CALLS kept.
    #[test]
    fn test_reingest_only_lib_preserves_calls_from_unchanged_caller() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, "/tmp/test"],
        )
        .unwrap();

        let dir = std::env::temp_dir().join("marrow_test_lib_reingest_calls");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("lib.py"), "def helper():\n    pass\n").unwrap();
        std::fs::write(dir.join("caller.py"), "def main():\n    helper()\n").unwrap();

        ingest_repo(&conn, repo_id, &dir).unwrap();

        let cross: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes src ON src.id = e.source_id \
                 JOIN nodes tgt ON tgt.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND src.file_path = 'caller.py' AND tgt.file_path = 'lib.py'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(cross >= 1, "expected CALLS from caller.py into lib.py");

        std::fs::write(dir.join("lib.py"), "def helper():\n    pass\n# touch\n").unwrap();
        ingest_repo(&conn, repo_id, &dir).unwrap();

        let cross2: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes src ON src.id = e.source_id \
                 JOIN nodes tgt ON tgt.id = e.target_id \
                 WHERE e.relationship_type = 'CALLS' \
                   AND src.file_path = 'caller.py' AND tgt.file_path = 'lib.py'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            cross2 >= 1,
            "inbound CALLS into lib.py should survive lib-only reindex; got {cross2}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_ingestion_refreshes_graph_degree_cache_after_edge_changes() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let repo_id = "test";
        let dir = std::env::temp_dir().join("marrow_test_graph_degree_refresh");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let root = dir.canonicalize().unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
            rusqlite::params![repo_id, root.to_string_lossy().to_string()],
        )
        .unwrap();

        std::fs::write(
            dir.join("main.py"),
            "def a():\n    b()\n\ndef b():\n    pass\n",
        )
        .unwrap();
        run_ingestion(&conn, repo_id, &dir).unwrap();
        assert!(crate::db::graph_degrees_are_fresh(&conn, repo_id).unwrap());
        let max_degree: i64 = conn
            .query_row(
                "SELECT MAX(degree) FROM graph_node_degrees WHERE repo_id = ?1",
                rusqlite::params![repo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(max_degree, 1);

        std::fs::write(
            dir.join("main.py"),
            "def a():\n    pass\n\ndef b():\n    pass\n",
        )
        .unwrap();
        run_ingestion(&conn, repo_id, &dir).unwrap();
        assert!(crate::db::graph_degrees_are_fresh(&conn, repo_id).unwrap());
        let max_degree: i64 = conn
            .query_row(
                "SELECT MAX(degree) FROM graph_node_degrees WHERE repo_id = ?1",
                rusqlite::params![repo_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(max_degree, 0);

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
        let first = extract_calls_from_symbol(src, "rb");
        let second = extract_calls_from_symbol(src, "rb");
        let third = extract_calls_from_symbol(src, "rb");
        assert_eq!(first, second, "repeated calls must return same result");
        assert_eq!(second, third, "repeated calls must return same result");
        assert!(
            first.contains(&"bar".to_string()),
            "bar must be detected: {first:?}"
        );
        assert!(
            first.contains(&"baz".to_string()),
            "baz must be detected: {first:?}"
        );
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
        let file_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            file_count, 2,
            "files table should have 2 entries after first ingest"
        );

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
        std::fs::write(
            dir.join("a.py"),
            "def alpha():\n    pass\ndef beta():\n    pass\n",
        )
        .unwrap();
        let (syms, _) = ingest_repo(&conn, "test", &dir).unwrap();
        assert_eq!(
            syms, 2,
            "modified file should result in 2 symbols (alpha + beta)"
        );

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

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND file_path = 'gone.py'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "nodes for deleted file should be removed");

        let files_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM files WHERE repo_id = 'test' AND file_path = 'gone.py'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            files_count, 0,
            "files record for deleted file should be removed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_resolve_cross_repo_edges_skips_ambiguous_import_targets() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4), (?5, ?6)",
            rusqlite::params![
                "repo_a",
                "/tmp/repo_a",
                "repo_b",
                "/tmp/repo_b",
                "repo_c",
                "/tmp/repo_c"
            ],
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

        let edges = resolve_cross_repo_edges(&conn, None).unwrap();
        assert_eq!(edges, 0, "ambiguous cross-repo imports should be skipped");
    }

    /// MARROW-PERF-012: scoped pass sees the same unambiguous IMPORTS as a full scan when only
    /// `repo_a` carries the import source.
    #[test]
    fn test_resolve_cross_repo_edges_scoped_matches_full_for_unambiguous_pair() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params!["repo_a", "/tmp/repo_a", "repo_b", "/tmp/repo_b"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7),
                    (?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                "repo_a:main.py:main",
                "repo_a",
                "main.py",
                "py",
                "main",
                "function",
                "from shared_vendor import UniqueWidget\n",
                "repo_b:widget.py:UniqueWidget",
                "repo_b",
                "widget.py",
                "py",
                "UniqueWidget",
                "class",
                "class UniqueWidget: pass\n"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_imports (repo_id, file_path, import_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["repo_a", "main.py", "UniqueWidget"],
        )
        .unwrap();

        let full = resolve_cross_repo_edges(&conn, None).unwrap();
        let scoped = resolve_cross_repo_edges(&conn, Some("repo_a")).unwrap();
        assert_eq!(full, scoped);
        assert_eq!(full, 1, "expected one unambiguous IMPORTS edge");
    }

    #[test]
    fn resolve_cross_repo_after_ingest_refreshes_degree_cache_after_same_node_count_import_edge() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params!["repo_a", "/tmp/repo_a", "repo_b", "/tmp/repo_b"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7),
                    (?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                "repo_a:main.py:main",
                "repo_a",
                "main.py",
                "py",
                "main",
                "function",
                "from shared_vendor import UniqueWidget\n",
                "repo_b:widget.py:UniqueWidget",
                "repo_b",
                "widget.py",
                "py",
                "UniqueWidget",
                "class",
                "class UniqueWidget: pass\n"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_imports (repo_id, file_path, import_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["repo_a", "main.py", "UniqueWidget"],
        )
        .unwrap();

        crate::db::refresh_graph_degrees(&conn, "repo_a").unwrap();
        crate::db::refresh_graph_degrees(&conn, "repo_b").unwrap();
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_a").unwrap());
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_b").unwrap());

        let edges = resolve_cross_repo_after_ingest(&conn, "repo_a").unwrap();
        assert_eq!(edges, 1, "expected one unambiguous IMPORTS edge");
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_a").unwrap());
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_b").unwrap());

        let source_degree: i64 = conn
            .query_row(
                "SELECT degree FROM graph_node_degrees WHERE repo_id = 'repo_a' AND node_id = 'repo_a:main.py:main'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let target_degree: i64 = conn
            .query_row(
                "SELECT degree FROM graph_node_degrees WHERE repo_id = 'repo_b' AND node_id = 'repo_b:widget.py:UniqueWidget'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(source_degree, 1);
        assert_eq!(target_degree, 1);
    }

    #[test]
    fn resolve_cross_repo_edges_marks_degree_cache_dirty_before_refresh_failure() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES (?1, ?2), (?3, ?4)",
            rusqlite::params!["repo_a", "/tmp/repo_a", "repo_b", "/tmp/repo_b"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7),
                    (?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            rusqlite::params![
                "repo_a:main.py:main",
                "repo_a",
                "main.py",
                "py",
                "main",
                "function",
                "from shared_vendor import UniqueWidget\n",
                "repo_b:widget.py:UniqueWidget",
                "repo_b",
                "widget.py",
                "py",
                "UniqueWidget",
                "class",
                "class UniqueWidget: pass\n"
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO file_imports (repo_id, file_path, import_name) VALUES (?1, ?2, ?3)",
            rusqlite::params!["repo_a", "main.py", "UniqueWidget"],
        )
        .unwrap();

        crate::db::refresh_graph_degrees(&conn, "repo_a").unwrap();
        crate::db::refresh_graph_degrees(&conn, "repo_b").unwrap();
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_a").unwrap());
        assert!(crate::db::graph_degrees_are_fresh(&conn, "repo_b").unwrap());

        conn.execute_batch(
            "CREATE TRIGGER fail_graph_degree_refresh
             BEFORE DELETE ON graph_node_degrees
             BEGIN
               SELECT RAISE(ABORT, 'stop graph degree refresh');
             END;",
        )
        .unwrap();

        let result = resolve_cross_repo_edges(&conn, Some("repo_a"));
        assert!(result.is_err(), "refresh failure should surface to caller");
        assert!(
            !crate::db::graph_degrees_are_fresh(&conn, "repo_a").unwrap(),
            "source repo cache must be dirty if post-commit refresh fails"
        );
        assert!(
            !crate::db::graph_degrees_are_fresh(&conn, "repo_b").unwrap(),
            "target repo cache must be dirty if post-commit refresh fails"
        );
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

        let mut stmt = conn
            .prepare("SELECT symbol_name, symbol_type FROM nodes WHERE repo_id = 'test'")
            .unwrap();
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
        assert!(
            result.is_err(),
            "ingest_repo should return Err if the root_path does not exist"
        );
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
        assert_eq!(
            updates.last().copied(),
            Some(100),
            "expected final progress to be 100: {updates:?}"
        );
        assert!(
            updates.windows(2).all(|window| window[0] <= window[1]),
            "progress should be monotonic: {updates:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_skips_bad_source_file_and_completes_progress_for_other_candidates() {
        let conn = crate::db::init_db(":memory:").unwrap();
        let dir = std::env::temp_dir().join("marrow_test_bad_source_progress");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("good.py"), "def good():\n    return 1\n").unwrap();
        std::fs::write(dir.join("bad.py"), [0xff, 0xfe, 0xfd]).unwrap();

        let progress_updates = std::sync::Mutex::new(Vec::new());
        run_ingestion_with_progress(&conn, "test", &dir, |percent| {
            progress_updates.lock().unwrap().push(percent);
        })
        .unwrap();

        let updates = progress_updates.lock().unwrap();
        assert_eq!(updates.last().copied(), Some(100));

        let node_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'good'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(node_count, 1);

        let indexed_bad_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND file_path = 'bad.py'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(indexed_bad_count, 0);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Serialize env mutation for ingestion tests (process-global).
    static ENV_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn graph_fingerprint_calls(conn: &Connection, repo_id: &str) -> (Vec<String>, Vec<String>) {
        let mut stmt = conn
            .prepare("SELECT id FROM nodes WHERE repo_id = ?1 ORDER BY id")
            .unwrap();
        let node_ids: Vec<String> = stmt
            .query_map(rusqlite::params![repo_id], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        let mut estmt = conn
            .prepare(
                "SELECT e.source_id, e.target_id FROM edges e \
                 JOIN nodes n ON n.id = e.source_id \
                 WHERE e.relationship_type = 'CALLS' AND n.repo_id = ?1 \
                 ORDER BY e.source_id, e.target_id",
            )
            .unwrap();
        let edge_keys: Vec<String> = estmt
            .query_map(rusqlite::params![repo_id], |row| {
                Ok(format!(
                    "{}->{}",
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?
                ))
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();
        (node_ids, edge_keys)
    }

    #[test]
    fn test_ingest_multiple_files_parse_queue_k_equivalence() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("marrow_test_parse_queue_k_equiv");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("a.py"), "def a_fn():\n    pass\n").unwrap();
        std::fs::write(dir.join("b.py"), "def b_fn():\n    c_fn()\n").unwrap();
        std::fs::write(dir.join("c.py"), "def c_fn():\n    a_fn()\n").unwrap();

        let root_str = dir.to_string_lossy().to_string();

        std::env::set_var("MARROW_INGEST_PARSE_QUEUE", "1");
        let conn_low = crate::db::init_db(":memory:").unwrap();
        conn_low
            .execute(
                "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
                rusqlite::params!["test", root_str.as_str()],
            )
            .unwrap();
        run_ingestion(&conn_low, "test", &dir).unwrap();
        let fp_low = graph_fingerprint_calls(&conn_low, "test");

        std::env::set_var("MARROW_INGEST_PARSE_QUEUE", "64");
        let conn_high = crate::db::init_db(":memory:").unwrap();
        conn_high
            .execute(
                "INSERT INTO repositories (id, root_path) VALUES (?1, ?2)",
                rusqlite::params!["test", root_str.as_str()],
            )
            .unwrap();
        run_ingestion(&conn_high, "test", &dir).unwrap();
        let fp_high = graph_fingerprint_calls(&conn_high, "test");

        std::env::remove_var("MARROW_INGEST_PARSE_QUEUE");

        assert_eq!(
            fp_low, fp_high,
            "CALLS graph should match for queue K=1 vs K=64"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A source file larger than the 2 MiB default must be silently skipped
    /// (returns Ok with empty symbol list, no error or panic).
    #[test]
    fn parse_file_skips_file_exceeding_default_size_limit() {
        let dir = std::env::temp_dir().join("marrow_test_parse_file_size_guard");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Write a >2 MiB Python file containing one valid function followed by comment padding.
        let big_path = dir.join("huge.py");
        let header = b"def oversize_fn():\n    pass\n";
        let padding = vec![b'#'; 3 * 1024 * 1024]; // 3 MiB of comment bytes
        let mut content = header.to_vec();
        content.extend_from_slice(&padding);
        std::fs::write(&big_path, &content).unwrap();

        let result = parse_file(&big_path);
        assert!(
            result.is_ok(),
            "parse_file should not error on oversized file"
        );
        let (_lang, symbols) = result.unwrap();
        assert!(
            symbols.is_empty(),
            "oversized file must produce zero symbols, got {}",
            symbols.len()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A file well below the 2 MiB limit is parsed normally.
    #[test]
    fn parse_file_parses_file_below_size_limit() {
        let dir = std::env::temp_dir().join("marrow_test_parse_file_normal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("small.py");
        std::fs::write(&path, b"def small_fn():\n    pass\n").unwrap();

        let (lang, symbols) = parse_file(&path).expect("parse_file should succeed for small file");
        assert_eq!(lang, "py");
        assert!(
            !symbols.is_empty(),
            "small file should produce at least one symbol"
        );
        assert!(
            symbols.iter().any(|s| s.name == "small_fn"),
            "expected 'small_fn' in symbols"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// MARROW_PARSE_TIMEOUT_MS no longer affects parse_file behavior.
    #[test]
    fn parse_file_ignores_parse_timeout_env() {
        let _guard = ENV_TEST_LOCK.lock().unwrap();
        let dir = std::env::temp_dir().join("marrow_test_parse_timeout_env_ignored");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let path = dir.join("timeout_env.py");
        std::fs::write(&path, b"def timeout_env_fn():\n    pass\n").unwrap();

        std::env::set_var("MARROW_PARSE_TIMEOUT_MS", "1");
        let result = parse_file(&path);
        std::env::remove_var("MARROW_PARSE_TIMEOUT_MS");

        let (lang, symbols) = result.expect("parse_file should ignore timeout env and parse file");
        assert_eq!(lang, "py");
        assert!(
            symbols.iter().any(|s| s.name == "timeout_env_fn"),
            "expected timeout_env_fn in symbols"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Oversized files in a repo are silently excluded from the AST graph;
    /// the ingest completes without error and no symbols from that file appear.
    #[test]
    fn ingest_silently_excludes_oversized_files_from_graph() {
        let dir = std::env::temp_dir().join("marrow_test_ingest_oversize_exclusion");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Small file that should be indexed normally.
        std::fs::write(dir.join("normal.py"), b"def normal_fn():\n    pass\n").unwrap();

        // Oversized file — 3 MiB Python file that must be silently skipped.
        let big_path = dir.join("oversize.py");
        let header = b"def oversize_fn():\n    pass\n";
        let padding = vec![b'#'; 3 * 1024 * 1024];
        let mut big_content = header.to_vec();
        big_content.extend_from_slice(&padding);
        std::fs::write(&big_path, &big_content).unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "test", &dir)
            .expect("ingest_repo must succeed even with oversized files");

        // The oversized file's symbol must not appear in the graph.
        let oversize_fn_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'oversize_fn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            oversize_fn_count, 0,
            "oversize_fn from 3 MiB file must not appear in the graph"
        );

        // The normal file's symbol must still be present.
        let normal_fn_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'normal_fn'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            normal_fn_count, 1,
            "normal_fn from small file must still be indexed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CALLS edges built via the spill-phase accumulation (new path) must be
    /// identical to what the old two-pass DB scan approach would have produced.
    /// This regression test catches any divergence in the refactored write_changeset_body.
    #[test]
    fn calls_edges_match_after_spill_phase_callee_accumulation() {
        let dir = std::env::temp_dir().join("marrow_test_calls_spill_accumulation");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(dir.join("caller.py"), b"def caller():\n    callee()\n").unwrap();
        std::fs::write(dir.join("callee.py"), b"def callee():\n    pass\n").unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "proj", &dir).expect("ingest should succeed");

        let edge_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE relationship_type = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            edge_count > 0,
            "at least one CALLS edge must exist after ingest"
        );

        // Verify the specific edge: caller → callee
        let edge_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges e \
                 JOIN nodes src ON src.id = e.source_id \
                 JOIN nodes tgt ON tgt.id = e.target_id \
                 WHERE src.symbol_name = 'caller' AND tgt.symbol_name = 'callee' \
                   AND e.relationship_type = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            edge_exists, 1,
            "CALLS edge from 'caller' to 'callee' must exist"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ingest_representative_ruby_ts_tsx_writes_graph_data_and_completes_progress() {
        let dir = std::env::temp_dir().join("marrow_test_representative_ruby_ts_tsx");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(
            dir.join("billing.rb"),
            "class BillingRun\n  def calculate\n    helper\n  end\n\n  def helper\n    1\n  end\nend\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("data.ts"),
            "export function normalizeData() {\n  return computeTotal();\n}\n\nexport function computeTotal() {\n  return 42;\n}\n",
        )
        .unwrap();
        std::fs::write(
            dir.join("Panel.tsx"),
            "export function DashboardPanel() {\n  return <section>{formatTitle()}</section>;\n}\n\nfunction formatTitle() {\n  return 'Graph';\n}\n",
        )
        .unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        let progress_updates = std::sync::Mutex::new(Vec::new());
        run_ingestion_with_progress(&conn, "test", &dir, |percent| {
            progress_updates.lock().unwrap().push(percent);
        })
        .unwrap();

        let updates = progress_updates.lock().unwrap();
        assert_eq!(
            updates.last().copied(),
            Some(100),
            "expected final progress to be 100: {updates:?}"
        );
        assert!(
            updates.windows(2).all(|window| window[0] <= window[1]),
            "progress should be monotonic: {updates:?}"
        );

        let node_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            node_count >= 6,
            "expected representative Ruby/TS/TSX nodes, got {node_count}"
        );

        let edge_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM edges", [], |row| row.get(0))
            .unwrap();
        assert!(edge_count > 0, "expected AST graph edges to be written");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Rust symbol kinds are stored as canonical names (function, struct, trait, etc.)
    /// and never as tree-sitter capture names (capture.func, capture.struct).
    #[test]
    fn rust_symbol_kinds_are_canonical() {
        let dir = std::env::temp_dir().join("marrow_test_rust_symbol_kinds");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        std::fs::write(
            dir.join("lib.rs"),
            "pub fn my_func() {}\npub struct MyStruct {}\npub trait MyTrait {}\npub enum MyEnum { A }\n",
        ).unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        let mut stmt = conn
            .prepare("SELECT symbol_name, symbol_type FROM nodes WHERE repo_id = 'test' ORDER BY symbol_name")
            .unwrap();
        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        // Verify no capture-prefixed types
        for (name, kind) in &rows {
            assert!(
                !kind.starts_with("capture."),
                "symbol '{name}' has non-canonical kind '{kind}'"
            );
        }

        // Verify expected canonical kinds exist
        let kinds: Vec<&str> = rows.iter().map(|(_, k)| k.as_str()).collect();
        assert!(
            kinds.contains(&"function"),
            "expected 'function' kind in: {kinds:?}"
        );
        assert!(
            kinds.contains(&"struct"),
            "expected 'struct' kind in: {kinds:?}"
        );
        assert!(
            kinds.contains(&"trait"),
            "expected 'trait' kind in: {kinds:?}"
        );
        assert!(
            kinds.contains(&"enum"),
            "expected 'enum' kind in: {kinds:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same-name symbols in the same file get distinct node IDs via start_byte.
    /// Tests the C++ case: `class Widget` and constructor `Widget()`.
    #[test]
    fn same_name_symbols_get_distinct_node_ids() {
        let dir = std::env::temp_dir().join("marrow_test_same_name_distinct");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // C++ file with class Widget and constructor Widget (different symbol_type)
        std::fs::write(
            dir.join("widget.cpp"),
            r#"
class Widget {
public:
    Widget() {}
    void update() {}
};

void processWidget() {}
"#,
        )
        .unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        // Check that all node IDs are unique
        let mut stmt = conn
            .prepare("SELECT id FROM nodes WHERE repo_id = 'test'")
            .unwrap();
        let ids: Vec<String> = stmt
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        let id_set: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(
            ids.len(),
            id_set.len(),
            "all node IDs must be unique — found duplicates in: {ids:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// make_node_id includes start_byte so two symbols with identical
    /// repo/file/type/name but different positions produce different IDs.
    #[test]
    fn make_node_id_includes_start_byte() {
        let id_a = make_node_id("r", "f.py", "function", "foo", 0);
        let id_b = make_node_id("r", "f.py", "function", "foo", 100);
        assert_ne!(
            id_a, id_b,
            "same-name same-kind at different positions must differ"
        );
    }

    /// .marrowrc.json ignore patterns exclude matching directories from ingestion.
    #[test]
    fn marrowrc_ignore_excludes_dirs() {
        let dir = std::env::temp_dir().join("marrow_test_marrowrc_ignore");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Create a .marrowrc.json that ignores "generated"
        std::fs::write(dir.join(".marrowrc.json"), r#"{"ignore": ["generated"]}"#).unwrap();

        // Create files in the ignored directory and a normal directory
        std::fs::create_dir_all(dir.join("generated")).unwrap();
        std::fs::write(
            dir.join("generated").join("auto.py"),
            "def auto():\n    pass\n",
        )
        .unwrap();
        std::fs::write(dir.join("real.py"), "def real():\n    pass\n").unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        // generated/auto.py should be excluded
        let auto_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'auto'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            auto_count, 0,
            "files under ignored 'generated/' dir should be excluded"
        );

        // real.py should be included
        let real_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'real'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            real_count, 1,
            "real.py outside ignored dir should be indexed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// .marrowrc.json patterns with trailing slashes are normalized and still exclude.
    #[test]
    fn marrowrc_trailing_slash_patterns_exclude_dirs() {
        let dir = std::env::temp_dir().join("marrow_test_marrowrc_trailing_slash");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Pattern uses trailing slash: "output/"
        std::fs::write(dir.join(".marrowrc.json"), r#"{"ignore": ["output/"]}"#).unwrap();

        std::fs::create_dir_all(dir.join("output")).unwrap();
        std::fs::write(dir.join("output").join("gen.py"), "def gen():\n    pass\n").unwrap();
        std::fs::write(dir.join("keep.py"), "def keep():\n    pass\n").unwrap();

        let conn = crate::db::init_db(":memory:").unwrap();
        ingest_repo(&conn, "test", &dir).unwrap();

        let gen_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'gen'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            gen_count, 0,
            "'output/' with trailing slash should still exclude"
        );

        let keep_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM nodes WHERE repo_id = 'test' AND symbol_name = 'keep'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(keep_count, 1, "keep.py should be indexed");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn default_traversal_excludes_repo_local_worktrees_dirs() {
        let dir = std::env::temp_dir().join("marrow_test_default_worktrees_ignore");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("worktrees")).unwrap();
        std::fs::create_dir_all(dir.join(".worktrees")).unwrap();

        std::fs::write(dir.join("real.py"), "def real():\n    pass\n").unwrap();
        std::fs::write(
            dir.join("worktrees").join("duplicate.py"),
            "def duplicate():\n    pass\n",
        )
        .unwrap();
        std::fs::write(
            dir.join(".worktrees").join("hidden_duplicate.py"),
            "def hidden_duplicate():\n    pass\n",
        )
        .unwrap();

        let files = collect_source_files(&dir).unwrap();
        let rels: HashSet<String> = files
            .iter()
            .map(|path| {
                path.strip_prefix(&dir)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();

        assert!(
            rels.contains("real.py"),
            "real.py should be indexed: {rels:?}"
        );
        assert!(
            !rels.contains("worktrees/duplicate.py"),
            "worktrees duplicate checkout should be excluded: {rels:?}"
        );
        assert!(
            !rels.contains(".worktrees/hidden_duplicate.py"),
            ".worktrees duplicate checkout should be excluded: {rels:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
