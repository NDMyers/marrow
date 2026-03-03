# Design: `benchmark` CLI Subcommand

**Date:** 2026-03-02
**Status:** Approved

## Problem

Marrow's core value proposition is token reduction. There was no way to
quantify that reduction for a given symbol without manually counting tokens.
The `benchmark` subcommand makes the savings concrete and screenshot-ready.

## Goal

Add `marrow benchmark <symbol> <repo_id>` to `main.rs`. The command reads
the full source file for the pivot symbol, generates its Context Capsule,
counts tokens in both using `tiktoken-rs`, and prints a formatted terminal
table showing original tokens, capsule tokens, tokens saved, and % reduction.

## Approach: Manual `std::env::args()` dispatch (Approach A)

A simple `args()` check at the top of `main()`. If the first argument is
`"benchmark"`, run benchmark mode and exit. Otherwise fall through to the
existing MCP stdio server. Zero new dependencies, matches the project's lean
dependency philosophy.

Rejected alternatives:
- **clap**: Adds a compile-time dependency for a single new subcommand.
- **Env-var flag**: Awkward UX, not what the command-line contract should be.

## Data Flow

1. Parse `args[1]=="benchmark"`, `args[2]=symbol`, `args[3]=repo_id`.
2. Open DB at `MARROW_DB_PATH` (or default).
3. Query `nodes` for the symbol → get `file_path` and `repo_id`.
4. Query `repositories` for `root_path` → construct absolute file path.
5. `fs::read_to_string(absolute_path)` → **Before** string.
6. `retrieval::get_context_capsule(&conn, symbol, repo_id)` → format into
   a `String` using the same capsule formatting logic in `call_tool` → **After** string.
7. `tiktoken_rs::cl100k_base()?.encode_with_special_tokens(&s).len()` on both.
8. Compute saved tokens and % reduction; print table to stdout.

## Dependency Fix

`Cargo.toml` currently contains a typo: `toktoken-rs = "0.5.9"`.
The correct crate name is `tiktoken-rs`. This will be corrected.

## Output Format

```
┌──────────────────────────────────────────────────────────────────┐
│  Marrow Token Benchmark                                          │
│  Symbol: process_audio  ·  Repo: juce_dsp                        │
│  File:   src/dsp/ProcessAudio.cpp                                │
├─────────────────────────┬────────────────────────────────────────┤
│  Metric                 │  Value                                 │
├─────────────────────────┼────────────────────────────────────────┤
│  Original File Tokens   │  4,812                                 │
│  Capsule Tokens         │  287                                   │
│  Tokens Saved           │  4,525                                 │
│  Reduction              │  94.0%                                 │
└─────────────────────────┴────────────────────────────────────────┘
```

Unicode box-drawing, fixed-width columns, comma-formatted numbers.
No external formatting crates.

## Error Handling

| Condition | Response |
|---|---|
| Wrong arg count | `Usage: marrow benchmark <symbol> <repo_id>` → exit 1 |
| Symbol not found in DB | `anyhow` error to stderr → exit 1 |
| File not found on disk | `"Source file not found at {path}. Re-ingest the repo to refresh."` → exit 1 |
| tiktoken BPE load failure | `anyhow` error to stderr → exit 1 |

## Files Changed

- `Cargo.toml` — fix `toktoken-rs` → `tiktoken-rs`
- `src/main.rs` — add `benchmark` arg branch in `main()` and a private
  `run_benchmark(conn, symbol, repo_id)` helper function
