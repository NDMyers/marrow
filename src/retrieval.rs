use anyhow::{anyhow, Result};
use rusqlite::Connection;
use streaming_iterator::StreamingIterator;
use tree_sitter::{Language, Parser, Query, QueryCursor};

// ── Public types ──────────────────────────────────────────────────────────────

pub struct ContextCapsule {
    pub pivot: NodeInfo,
    pub neighbors: Vec<NeighborInfo>,
}

pub struct NodeInfo {
    #[allow(dead_code)]
    pub id: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub file_path: String,
    pub language: String,
    /// Full source for the pivot; skeletonized body for neighbors.
    pub text: String,
}

pub struct NeighborInfo {
    pub node: NodeInfo,
    /// The edge label: CALLS, IMPORTS, IMPLEMENTS, etc.
    pub relationship: String,
}

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

pub struct ImpactResult {
    pub pivot_id: String,
    pub affected: Vec<ImpactNode>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Fetch the pivot node's full source and all depth-1 neighbors skeletonized.
pub fn get_context_capsule(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
) -> Result<ContextCapsule> {
    let (pivot_id, pivot_name, pivot_type, pivot_path, pivot_lang, pivot_raw): (
        String,
        String,
        String,
        String,
        String,
        String,
    ) = conn
        .query_row(
            "SELECT id, symbol_name, symbol_type, file_path, language, raw_text
             FROM nodes WHERE symbol_name = ?1 AND repo_id = ?2 LIMIT 1",
            rusqlite::params![symbol_name, repo_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .map_err(|_| anyhow!("Symbol '{}' not found in repo '{}'", symbol_name, repo_id))?;

    let pivot = NodeInfo {
        id: pivot_id.clone(),
        symbol_name: pivot_name,
        symbol_type: pivot_type,
        file_path: pivot_path,
        language: pivot_lang,
        text: pivot_raw,
    };

    // Collect neighbors in both edge directions, deduplicating by node id.
    let mut stmt = conn.prepare(
        "SELECT n.id, n.symbol_name, n.symbol_type, n.file_path, n.language,
                n.raw_text, e.relationship_type
         FROM edges e
         JOIN nodes n ON (e.source_id = ?1 AND n.id = e.target_id)
                      OR (e.target_id = ?1 AND n.id = e.source_id)
         WHERE n.id != ?1",
    )?;

    let rows: Vec<(String, String, String, String, String, String, String)> = stmt
        .query_map(rusqlite::params![pivot_id], |row| {
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

    let neighbors = rows
        .into_iter()
        .map(|(id, sym_name, sym_type, file_path, lang, raw_text, rel_type)| NeighborInfo {
            node: NodeInfo {
                id,
                symbol_name: sym_name,
                symbol_type: sym_type,
                file_path,
                language: lang.clone(),
                text: skeletonize(&raw_text, &lang),
            },
            relationship: rel_type,
        })
        .collect();

    Ok(ContextCapsule { pivot, neighbors })
}

/// Recursively find every node that (transitively) depends on the pivot.
/// Uses a WITH RECURSIVE CTE walking edges backwards (source → pivot direction).
pub fn analyze_impact(
    conn: &Connection,
    symbol_name: &str,
    repo_id: &str,
) -> Result<ImpactResult> {
    let pivot_id: String = conn
        .query_row(
            "SELECT id FROM nodes WHERE symbol_name = ?1 AND repo_id = ?2 LIMIT 1",
            rusqlite::params![symbol_name, repo_id],
            |row| row.get(0),
        )
        .map_err(|_| anyhow!("Symbol '{}' not found in repo '{}'", symbol_name, repo_id))?;

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
         ORDER BY r.depth",
    )?;

    let affected = stmt
        .query_map(rusqlite::params![pivot_id], |row| {
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

    Ok(ImpactResult { pivot_id, affected })
}

// ── Skeletonization ───────────────────────────────────────────────────────────

/// Skeletonize `raw_text` for `lang`, replacing the body with a placeholder.
/// Returns the original text unchanged if no body block is detected
/// (e.g., forward declarations, macro-defined structs, incomplete fragments).
pub fn skeletonize(raw_text: &str, lang: &str) -> String {
    match lang {
        "cpp" | "cc" | "cxx" | "h" | "hpp" => skeletonize_braces(
            raw_text,
            tree_sitter_cpp::LANGUAGE.into(),
            // compound_statement = function body  |  field_declaration_list = class body
            "[(compound_statement) @body (field_declaration_list) @body]",
        ),
        "py" => skeletonize_python(raw_text),
        "ts" => skeletonize_braces(
            raw_text,
            tree_sitter_typescript::LANGUAGE_TYPESCRIPT.into(),
            "[(statement_block) @body (class_body) @body]",
        ),
        "tsx" => skeletonize_braces(
            raw_text,
            tree_sitter_typescript::LANGUAGE_TSX.into(),
            "[(statement_block) @body (class_body) @body]",
        ),
        _ => raw_text.to_string(),
    }
}

/// Replace the outermost `{…}` body node with `{ /* ... */ }`.
fn skeletonize_braces(raw_text: &str, lang: Language, query_src: &str) -> String {
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
fn skeletonize_python(raw_text: &str) -> String {
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

/// Run a tree-sitter query on `raw_text` and return the byte range of the
/// outermost (earliest-start) captured body node.
///
/// Collecting byte ranges into a Vec before any string ops sidesteps the
/// borrow-checker conflict between the streaming iterator (which borrows
/// `cursor` and `tree`) and the subsequent `&raw_text[..]` slices.
fn find_outermost_body(
    raw_text: &str,
    lang: Language,
    query_src: &str,
) -> Option<(usize, usize)> {
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

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
             CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id);
             CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id);",
        )
        .unwrap();
        conn
    }

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

    fn insert_edge(conn: &Connection, src: &str, tgt: &str, rel: &str) {
        conn.execute(
            "INSERT INTO edges VALUES (?1,?2,?3)",
            rusqlite::params![src, tgt, rel],
        )
        .unwrap();
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
        let cap = get_context_capsule(&conn, "foo", "r").unwrap();
        assert_eq!(cap.pivot.symbol_name, "foo");
        assert_eq!(cap.pivot.text, "def foo():\n    return 42\n");
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
        let cap = get_context_capsule(&conn, "solo", "r").unwrap();
        assert!(cap.neighbors.is_empty());
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

        let cap = get_context_capsule(&conn, "caller", "r").unwrap();
        assert_eq!(cap.neighbors.len(), 1);
        let neighbor_text = &cap.neighbors[0].node.text;
        assert!(
            neighbor_text.contains("pass"),
            "body should be replaced with pass, got: {neighbor_text}"
        );
        assert!(
            !neighbor_text.contains("return x"),
            "original body should be gone, got: {neighbor_text}"
        );
        assert!(
            neighbor_text.contains("def bar"),
            "signature must be preserved, got: {neighbor_text}"
        );
        assert_eq!(cap.neighbors[0].relationship, "CALLS");
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

        let cap = get_context_capsule(&conn, "main_fn", "r").unwrap();
        let neighbor_text = &cap.neighbors[0].node.text;
        assert!(
            neighbor_text.contains("{ /* ... */ }"),
            "C++ body should be replaced, got: {neighbor_text}"
        );
        assert!(
            !neighbor_text.contains("x += 1"),
            "original body should be gone, got: {neighbor_text}"
        );
        assert!(neighbor_text.contains("helper"));
    }

    #[test]
    fn capsule_cpp_forward_decl_returns_full_text() {
        let conn = make_db();
        // Forward declaration: no body block — skeletonize must return it unchanged.
        let fwd = "class Widget;";
        insert_node(&conn, "r:w.h:Widget", "r", "w.h", "cpp", "Widget", "class", fwd);
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

        let cap = get_context_capsule(&conn, "processWidget", "r").unwrap();
        let widget = cap
            .neighbors
            .iter()
            .find(|n| n.node.symbol_name == "Widget")
            .expect("Widget neighbor should be present");
        assert_eq!(
            widget.node.text, fwd,
            "forward declaration should be returned verbatim"
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

        let cap = get_context_capsule(&conn, "entrypoint", "r").unwrap();
        let neighbor_text = &cap.neighbors[0].node.text;
        assert!(
            neighbor_text.contains("{ /* ... */ }"),
            "TS body should be replaced, got: {neighbor_text}"
        );
        assert!(!neighbor_text.contains("toISOString"));
        assert!(neighbor_text.contains("formatDate"));
    }

    #[test]
    fn capsule_unknown_symbol_returns_error() {
        let conn = make_db();
        assert!(get_context_capsule(&conn, "ghost", "r").is_err());
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
        let result = analyze_impact(&conn, "solo", "r").unwrap();
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

        let result = analyze_impact(&conn, "helper", "r").unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "caller");
        assert_eq!(result.affected[0].relationship_type, "CALLS");
        assert_eq!(result.affected[0].depth, 1);
    }

    #[test]
    fn impact_multi_hop_traversal_with_correct_depths() {
        let conn = make_db();
        insert_node(&conn, "r:a.py:base", "r", "a.py", "py", "base", "function", "def base(): pass\n");
        insert_node(&conn, "r:a.py:mid", "r", "a.py", "py", "mid", "function", "def mid(): base()\n");
        insert_node(&conn, "r:a.py:top", "r", "a.py", "py", "top", "function", "def top(): mid()\n");
        insert_edge(&conn, "r:a.py:mid", "r:a.py:base", "CALLS");
        insert_edge(&conn, "r:a.py:top", "r:a.py:mid", "CALLS");

        let result = analyze_impact(&conn, "base", "r").unwrap();
        assert_eq!(result.affected.len(), 2);
        let mid = result.affected.iter().find(|n| n.symbol_name == "mid").unwrap();
        let top = result.affected.iter().find(|n| n.symbol_name == "top").unwrap();
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
        insert_edge(&conn, "repo_a:app.py:main", "repo_b:lib.ts:ApiClient", "IMPORTS");

        let result = analyze_impact(&conn, "ApiClient", "repo_b").unwrap();
        assert_eq!(result.affected.len(), 1);
        assert_eq!(result.affected[0].symbol_name, "main");
        assert_eq!(result.affected[0].relationship_type, "IMPORTS");
        assert_eq!(result.affected[0].repo_id, "repo_a");
    }

    #[test]
    fn impact_unknown_symbol_returns_error() {
        let conn = make_db();
        assert!(analyze_impact(&conn, "ghost", "r").is_err());
    }

    // ── skeletonize (unit tests on raw text) ──────────────────────────────────

    #[test]
    fn skeletonize_cpp_function_replaces_body() {
        let raw = "void process(int x) {\n    x += 1;\n    return;\n}";
        let result = skeletonize(raw, "cpp");
        assert!(result.contains("process(int x)"), "signature lost: {result}");
        assert!(result.contains("{ /* ... */ }"), "placeholder missing: {result}");
        assert!(!result.contains("x += 1"), "body leaked: {result}");
    }

    #[test]
    fn skeletonize_cpp_forward_decl_unchanged() {
        let raw = "class Foo;";
        assert_eq!(skeletonize(raw, "cpp"), raw);
    }

    #[test]
    fn skeletonize_py_function_replaces_body_with_pass() {
        let raw = "def compute(n):\n    total = 0\n    return total\n";
        let result = skeletonize(raw, "py");
        assert!(result.contains("def compute(n):"), "signature lost: {result}");
        assert!(result.contains("pass"), "pass placeholder missing: {result}");
        assert!(!result.contains("total"), "body leaked: {result}");
    }

    #[test]
    fn skeletonize_ts_function_replaces_body() {
        let raw = "function greet(name: string): string {\n    return `Hello ${name}`;\n}";
        let result = skeletonize(raw, "ts");
        assert!(result.contains("greet(name: string)"), "signature lost: {result}");
        assert!(result.contains("{ /* ... */ }"), "placeholder missing: {result}");
        assert!(!result.contains("Hello"), "body leaked: {result}");
    }
}
