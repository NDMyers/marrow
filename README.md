# Marrow (AST Context Engine)

**Marrow** is a high-performance, local, and language-agnostic Model Context Protocol (MCP) server written in Rust. It is designed to drastically reduce LLM token bloat by dynamically parsing codebases into Abstract Syntax Trees (AST) and serving condensed "Context Capsules."

## Overview

Marrow operates by ingesting source code from multiple programming languages (C++, Python, TypeScript) using `tree-sitter`. It constructs a unified, cross-repository dependency graph and stores it in an optimized local SQLite database (`.marrow/graph.db`). Instead of relying on expensive vector embeddings or external graph databases, Marrow uses intelligent code condensation to provide AI agents with precise structural context and dependency insight.

## Core Capabilities

- **Frictionless Workspace Initialization:** Rapidly ingests local codebases via parallel file processing with simple `marrow init` and `marrow index` commands.
- **Universal Ingestion Pipeline:** Natively supports mapping complex symbol definitions and cross-file relationships across multiple languages.
- **Deep Impact Analysis (Blast Radius):** Employs SQLite recursive Common Table Expressions (CTEs) to map the downstream impact of a proposed code change across all files and repositories.
- **Condensed Context Capsules:** Replaces large function and class bodies with condensed signatures, preserving critical structural boundaries while minimizing token consumption.
- **Multi-Repo Edge Resolution:** Intelligently resolves and tracks cross-repo references and import edges within a shared workspace.

## Technology Stack

- **Language:** Rust (2021 edition)
- **Parser:** `tree-sitter` (Rust bindings with dynamic language loading)
- **Database:** SQLite (`rusqlite` in WAL mode) for high-throughput batch inserts and fast spatial graph queries.
- **Protocol:** Official Model Context Protocol (MCP) SDK over stdio.

## Local development

**Prerequisites:** A stable Rust toolchain (`rustup` recommended) and a working C compiler (required for `tree-sitter` native code), as on macOS with Xcode Command Line Tools.

**Build (from the repository root):**

```bash
cargo build              # debug binary: target/debug/marrow
cargo build --release    # release binary: target/release/marrow
```

**Run without installing** (same args as the installed binary):

```bash
cargo run -- mcp                    # MCP stdio server (typical for editor integration)
cargo run -- init                   # workspace setup
cargo run -- index                  # ingest current tree (same pipeline as MCP ingest_repo)
cargo run -- maintenance            # WAL checkpoint + incremental_vacuum on graph.db
cargo run -- test-capsules        # capsule validation
```

**Checks (optional):**

```bash
cargo check
cargo clippy -- -D warnings
```

**Memory tuning (SQLite + ingestion):** Marrow caps SQLite page cache and disables memory-mapped I/O by default so a large `graph.db` is less likely to show as 10+ GB in Activity Monitor. Override when needed:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MARROW_SQLITE_CACHE_KIB` | `32768` (32 MiB) | SQLite `cache_size` (negative KiB). Lower → less idle RSS; higher → faster queries. |
| `MARROW_SQLITE_MMAP_BYTES` | `0` | `PRAGMA mmap_size` in bytes; `0` disables mmap. Set positive to re-enable mmap for throughput. |
| `MARROW_MAX_FILE_BYTES` | `2097152` (2 MiB) | Skip files larger than this before tree-sitter parse. Large generated files (GraphQL schemas, protobuf outputs, bundled JS) produce ASTs 3–10× source size in each parallel worker; skipping them prevents multi-GB RSS spikes with zero loss of architectural signal. |
| `MARROW_INGEST_THREADS` | `min(8, max(2, cores))` | Rayon workers for hash/parse during ingest; fewer workers lower peak RAM during full reindex. |
| `MARROW_INGEST_PARSE_QUEUE` | `64` | Max parsed files in the bounded channel between Rayon workers and a drainer thread (serialized to a temp spill file); lower → lower peak RSS on huge reindexes, more back-pressure on workers. Spill reads cap blob size (64 MiB per field) and symbol count per row to limit corrupt-file DoS. |
| `MARROW_SKIP_POST_INGEST_MAINTENANCE` | *(unset)* | If non-empty, skip WAL checkpoint + `incremental_vacuum` after ingest (faster huge reindexes). Run `marrow maintenance` later. |
| `MARROW_CROSS_REPO_FULL_SCAN` | *(unset)* | If `1`/`true`/`yes`, scan **all** repos for cross-repo `IMPORTS` after each ingest (legacy). Default: only the repo that was just indexed — see `MARROW-PERF-012` / [`docs/perf-harness.md`](docs/perf-harness.md). |
| `MARROW_CAPSULE_MAX_OUTBOUND` | `500` | Max outbound edges loaded per capsule / trace (RAM bound). |
| `MARROW_CAPSULE_ORIGINAL_MODE` | `none` | `none` (default): do not load touched files into MCP `original_text` (saves RAM). `full`: legacy concatenation of full files (see `MARROW_CAPSULE_ORIGINAL_MAX_BYTES`). |
| `MARROW_CAPSULE_ORIGINAL_LEGACY` | *(unset)* | If `1`/`true`/`yes`, alias for `MARROW_CAPSULE_ORIGINAL_MODE=full` (one-release shim). |
| `MARROW_CAPSULE_ORIGINAL_MAX_BYTES` | *(unset)* | **Only when mode is `full`.** Cap total bytes for `original_text`. Uses file `metadata().len()` before reading; skips files that would exceed the budget. Unset = unlimited concat (can spike RAM). |
| `MARROW_CAPSULE_MAX_INBOUND_LOAD` | `64` | Max inbound rows loaded from DB (display still capped at 10). |
| `MARROW_IMPACT_MAX_ROWS` | `5000` | Max rows returned by `analyze_impact`. |

**Capsule benchmark:** `marrow benchmark <symbol> <repo_id>` uses the same `file_tokens` heuristic as the MCP tools (metadata `len/4` when `MARROW_CAPSULE_ORIGINAL_MODE` is `none`). Add `--precise-file-tokens` for cl100k_base counts summed per touched file (streams one file at a time; no full concat).

**Post-ingest DB maintenance:** After a large ingest, or if you used `MARROW_SKIP_POST_INGEST_MAINTENANCE`, run:

```bash
marrow maintenance
```

Uses `MARROW_DB_PATH` or defaults to `.marrow/graph.db`. Capsule / impact limits: [`docs/mcp-payload-limits.md`](docs/mcp-payload-limits.md).

## Rebuild & deploy

Use this whenever you pull changes or modify Marrow source code and need to get the new binary live.

### Full rebuild + install (most common)

```bash
# 1. From the marrow repo root:
cd ~/Coding/marrow

# 2. Verify it compiles cleanly:
cargo check

# 3. Run the test suite (128 tests, ~1 s):
cargo test

# 4. Build optimised release binary and install to ~/.cargo/bin/marrow:
cargo install --path .
```

`cargo install --path .` compiles with full optimisations and replaces the binary at
`~/.cargo/bin/marrow` in one step (~30 s on Apple Silicon). No separate `cp` needed.

### Pick up the new binary in your editor / agents

Marrow is launched fresh as a stdio subprocess each time an agent session starts, so
**no daemon restart is required** — just reload/restart the editor window (or the agent
session) and the next `marrow mcp` spawn will use the newly installed binary.

If the dashboard (`marrow ui`) is running as a persistent background process, restart it:

```bash
marrow stop   # stop background daemon if running
marrow ui     # re-open dashboard (optional)
```

### Quick iteration (skip install)

If you only want to test a change without overwriting the installed binary:

```bash
cargo build --release          # builds target/release/marrow
./target/release/marrow index  # run any subcommand against the uninstalled binary
```

### Lint + test only (no build)

```bash
cargo check                    # fast syntax + type check (~4 s)
cargo clippy -- -D warnings    # lint; must produce zero warnings
cargo test                     # 128 unit tests (~1 s)
```

### After a large re-index

If you just rebuilt and re-ran `marrow index` against a large codebase, run the post-ingest
maintenance pass to reclaim WAL space:

```bash
marrow maintenance
```

## Performance epic

Tracked RAM/latency work (MARROW-PERF-001–015, milestones M0–M3) is listed in [`.cursor/epics/marrow-ram-latency-epic.md`](.cursor/epics/marrow-ram-latency-epic.md); maintainers update checkboxes there as stories land.

**Baseline + harness (M0):**

- Runbook: [`docs/perf-baseline-runbook.md`](docs/perf-baseline-runbook.md)
- `marrow perf-harness`: [`docs/perf-harness.md`](docs/perf-harness.md) — `cargo build --release && ./target/release/marrow perf-harness --help`
- SQLite hot-query notes: [`docs/sqlite-query-plans.md`](docs/sqlite-query-plans.md)
- Phase C worker spike (defer): [`docs/phase-c-worker-spike.md`](docs/phase-c-worker-spike.md)
- CI perf smoke thresholds: [`ci/perf-thresholds.json`](ci/perf-thresholds.json) (used by `.github/workflows/ci.yml`)

**Global install** (puts the binary on your PATH via `~/.cargo/bin`, which must be on `PATH`):

```bash
cargo install --path .
```

This installs the **`marrow`** executable into `~/.cargo/bin` (ensure that directory is on your `PATH`).
