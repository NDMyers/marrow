# Benchmark Subcommand Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `marrow benchmark <symbol> <repo_id>` CLI subcommand that quantifies Marrow's token reduction with a screenshot-ready terminal table.

**Architecture:** Manual `std::env::args()` dispatch at the top of `main()` — if args[1]=="benchmark", run the benchmark path and return, otherwise fall through to the existing MCP stdio server. Three private helpers are added: `format_capsule_string` (extracted from the existing `call_tool` dispatch), `format_benchmark_table` (pure formatting function), and `count_tokens` (thin tiktoken-rs wrapper). A `run_benchmark` orchestrator ties them together via DB + disk I/O.

**Tech Stack:** Rust, rusqlite, tiktoken-rs (cl100k_base), tree-sitter (unchanged), rmcp (unchanged)

---

## Task 1: Fix the `tiktoken-rs` typo in `Cargo.toml`

**Files:**
- Modify: `Cargo.toml`

### Step 1: Fix the dependency name

In `Cargo.toml`, change:
```toml
toktoken-rs = "0.5.9"
```
to:
```toml
tiktoken-rs = "0.5.9"
```

### Step 2: Verify the crate resolves

```bash
cargo check 2>&1 | head -30
```

Expected: compiles without "package not found" or "no matching package" errors.
If tiktoken-rs 0.5.9 does not exist on crates.io, run `cargo search tiktoken-rs` and use the latest available version.

### Step 3: Commit

```bash
git add Cargo.toml
git commit -m "fix: correct tiktoken-rs crate name (was toktoken-rs typo)"
```

---

## Task 2: Extract `format_capsule_string` from `call_tool`

The capsule-to-string logic is currently inline inside the `"get_context_capsule"` match arm. Extract it to a reusable private function so the benchmark can call it without going through the MCP layer.

**Files:**
- Modify: `src/main.rs`

### Step 1: Write the failing test

Add inside the existing `#[cfg(test)]` block at the bottom of `src/main.rs`:

```rust
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
```

### Step 2: Run the test — expect compile error (function doesn't exist yet)

```bash
cargo test format_capsule_string 2>&1 | tail -20
```

Expected: `error[E0425]: cannot find function 'format_capsule_string'`

### Step 3: Add the function above `impl ContextEngine`

```rust
/// Format a ContextCapsule as the plain-text string sent to the LLM.
/// Extracted from `call_tool` so the benchmark subcommand can reuse it.
fn format_capsule_string(capsule: &retrieval::ContextCapsule) -> String {
    use std::fmt::Write as FmtWrite;
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
```

### Step 4: Replace the inline formatting in `call_tool`

Find the `"get_context_capsule"` match arm. Replace the block that builds `out` (from `let mut out = String::new()` through `Ok(CallToolResult::success(...))`) with:

```rust
let out = format_capsule_string(&capsule);
Ok(CallToolResult::success(vec![Content::text(out)]))
```

Also remove the now-unused `use std::fmt::Write as FmtWrite;` at the top of `main.rs` (it is now inside `format_capsule_string`). If other places still use it, keep it.

### Step 5: Run all tests

```bash
cargo test 2>&1 | tail -20
```

Expected: all existing tests pass, new test passes.

### Step 6: Commit

```bash
git add src/main.rs
git commit -m "refactor: extract format_capsule_string from call_tool"
```

---

## Task 3: Add `count_tokens` helper

**Files:**
- Modify: `src/main.rs`

### Step 1: Write the failing tests

Add inside `#[cfg(test)]`:

```rust
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
```

### Step 2: Run — expect compile error

```bash
cargo test count_tokens 2>&1 | tail -10
```

Expected: `error[E0425]: cannot find function 'count_tokens'`

### Step 3: Add the function below `format_capsule_string`

```rust
/// Count cl100k_base tokens in `text`.
fn count_tokens(text: &str) -> anyhow::Result<usize> {
    let bpe = tiktoken_rs::cl100k_base()?;
    Ok(bpe.encode_with_special_tokens(text).len())
}
```

Add the crate import at the top of `main.rs` (with the other `use` statements):

```rust
use tiktoken_rs;
```

(tiktoken_rs does not need to be explicitly `use`d at module level; calling `tiktoken_rs::cl100k_base()` is sufficient — omit the `use` if the bare path works.)

### Step 4: Run tests

```bash
cargo test count_tokens 2>&1 | tail -10
```

Expected: both tests pass.

### Step 5: Commit

```bash
git add src/main.rs
git commit -m "feat: add count_tokens helper using tiktoken-rs cl100k_base"
```

---

## Task 4: Add `format_benchmark_table` pure formatting function

This is a pure `&str → String` function with no I/O, so it is fully unit-testable.

**Files:**
- Modify: `src/main.rs`

### Step 1: Write the failing test

Add inside `#[cfg(test)]`:

```rust
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
    assert!(table.contains("my_func"),   "symbol missing:\n{table}");
    assert!(table.contains("my_repo"),   "repo missing:\n{table}");
    assert!(table.contains("src/foo.cpp"), "file path missing:\n{table}");
    // Metric values
    assert!(table.contains("1,000"),     "file tokens missing:\n{table}");
    assert!(table.contains("100"),       "capsule tokens missing:\n{table}");
    assert!(table.contains("900"),       "saved tokens missing:\n{table}");
    assert!(table.contains("90.0%"),     "reduction % missing:\n{table}");
}

#[test]
fn format_benchmark_table_zero_reduction_when_equal() {
    let table = format_benchmark_table("s", "r", "f.py", 500, 500);
    assert!(table.contains("0"),   "saved should be 0:\n{table}");
    assert!(table.contains("0.0%"), "reduction should be 0.0%:\n{table}");
}
```

### Step 2: Run — expect compile error

```bash
cargo test format_benchmark_table 2>&1 | tail -10
```

### Step 3: Add the helper and the function

Add a small comma-formatting helper (no external deps):

```rust
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
```

Then add the table formatter. Use fixed column widths so the table is always
68 characters wide (inner), screenshot-stable regardless of inputs.

```rust
/// Build the terminal benchmark table.
///
/// Layout (68-char inner width):
///   header rows span full 68 chars
///   metric rows: 27-char left col │ 39-char right col
fn format_benchmark_table(
    symbol:         &str,
    repo_id:        &str,
    file_path:      &str,
    file_tokens:    usize,
    capsule_tokens: usize,
) -> String {
    let saved      = file_tokens.saturating_sub(capsule_tokens);
    let reduction  = if file_tokens == 0 {
        0.0_f64
    } else {
        (saved as f64 / file_tokens as f64) * 100.0
    };

    // Column inner widths (excluding the │ separator).
    const L: usize = 27; // left metric label column
    const R: usize = 39; // right value column
    const W: usize = L + 1 + R; // total inner width = 67

    let h_full  = "─".repeat(W);              // full-width horizontal
    let h_left  = "─".repeat(L);
    let h_right = "─".repeat(R);

    // Header rows (single span, left-aligned, padded to W).
    let hdr_title = format!("  Marrow Token Benchmark");
    let hdr_sym   = format!("  Symbol: {symbol}  ·  Repo: {repo_id}");
    let hdr_file  = format!("  File:   {file_path}");

    // Metric rows: "  {label:<25}" │ "  {value:<37}"
    let row = |label: &str, value: &str| -> String {
        format!("│  {label:<25}│  {value:<37}│\n", label=label, value=value)
    };

    let mut t = String::new();
    use std::fmt::Write as W_;
    // Top border + header
    writeln!(t, "┌{h_full}┐").ok();
    writeln!(t, "│{hdr_title:<W$}│", W=W).ok();
    writeln!(t, "│{hdr_sym:<W$}│",   W=W).ok();
    writeln!(t, "│{hdr_file:<W$}│",  W=W).ok();
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
```

### Step 4: Run tests

```bash
cargo test format_benchmark_table 2>&1 | tail -10
```

Expected: both tests pass.

### Step 5: Run all tests to make sure nothing broke

```bash
cargo test 2>&1 | tail -10
```

### Step 6: Commit

```bash
git add src/main.rs
git commit -m "feat: add format_benchmark_table and fmt_num helpers"
```

---

## Task 5: Add `run_benchmark` orchestrator

This function ties together DB queries, disk I/O, token counting, and table printing. It is integration-level logic and does not get its own unit test (it requires a live DB + real files). Correctness is validated by running the binary manually in Task 6.

**Files:**
- Modify: `src/main.rs`

### Step 1: Add the function below `format_benchmark_table`

```rust
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

    let abs_path = std::path::PathBuf::from(&root_path).join(&file_path);
    let file_content = fs::read_to_string(&abs_path).map_err(|_| {
        anyhow::anyhow!(
            "Source file not found at {}. Re-ingest the repo to refresh.",
            abs_path.display()
        )
    })?;

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
```

### Step 2: Verify it compiles

```bash
cargo check 2>&1 | tail -20
```

Expected: no errors.

### Step 3: Commit

```bash
git add src/main.rs
git commit -m "feat: add run_benchmark orchestrator"
```

---

## Task 6: Wire up CLI dispatch in `main()`

**Files:**
- Modify: `src/main.rs`

### Step 1: Add the dispatch block at the top of `main()`

Insert the following immediately after `let db_path = ...` and before
`let db_parent = ...`:

```rust
// ── CLI subcommand dispatch ────────────────────────────────────────
let args: Vec<String> = std::env::args().collect();
if args.get(1).map(|s| s.as_str()) == Some("benchmark") {
    let symbol = args.get(2).ok_or_else(|| {
        anyhow::anyhow!("Usage: marrow benchmark <symbol> <repo_id>")
    })?;
    let repo_id = args.get(3).ok_or_else(|| {
        anyhow::anyhow!("Usage: marrow benchmark <symbol> <repo_id>")
    })?;

    // DB must exist before benchmarking (ingest first).
    let conn = db::init_db(&db_path)?;
    run_benchmark(&conn, symbol, repo_id)?;
    return Ok(());
}
```

### Step 2: Full compile + test pass

```bash
cargo build 2>&1 | tail -20
cargo test  2>&1 | tail -20
```

Expected: binary compiles, all tests green.

### Step 3: Lint

```bash
cargo clippy -- -D warnings 2>&1 | tail -20
```

Fix any warnings before continuing.

### Step 4: Commit

```bash
git add src/main.rs
git commit -m "feat: wire benchmark subcommand dispatch in main()"
```

---

## Task 7: Smoke-test with a real ingested repo

This is a manual validation step — no DB exists in CI.

### Step 1: Ingest a repo

```bash
# Build release binary
cargo build --release

# Start MCP server in one terminal, use Claude/MCP client to call ingest_repo
# OR run the binary directly if a test fixture is available:
MARROW_DB_PATH=.context_engine/graph.db ./target/release/rust-ast-context-engine
```

If you have a test fixture with pre-populated DB (e.g., from `test_fixtures/`), use that:

```bash
MARROW_DB_PATH=test_fixtures/graph.db \
  ./target/release/rust-ast-context-engine benchmark <known_symbol> <known_repo_id>
```

### Step 2: Verify table renders correctly

Expected output format:

```
┌───────────────────────────────────────────────────────────────────┐
│  Marrow Token Benchmark                                           │
│  Symbol: <symbol>  ·  Repo: <repo_id>                            │
│  File:   src/path/to/file.cpp                                     │
├────────────────────────────┬──────────────────────────────────────┤
│  Metric                    │  Value                               │
├────────────────────────────┼──────────────────────────────────────┤
│  Original File Tokens      │  4,812                               │
│  Capsule Tokens            │  287                                 │
│  Tokens Saved              │  4,525                               │
│  Reduction                 │  94.0%                               │
└────────────────────────────┴──────────────────────────────────────┘
```

### Step 3: Verify MCP server mode is unaffected

```bash
echo '{}' | MARROW_DB_PATH=.context_engine/graph.db ./target/release/rust-ast-context-engine
```

Expected: server starts and prints `Marrow MCP server ready — listening on stdio.`

### Step 4: Final commit if any fixups were needed

```bash
git add -p
git commit -m "fix: benchmark smoke-test fixups"
```
