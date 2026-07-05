# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `.c` files are indexed (routed through the tree-sitter-cpp grammar), and ingestion covers far more type-level constructs: C/C++ named `struct`/`union`/`enum` tags and typedef aliases, TS/TSX interfaces, type aliases, enums and arrow-function components, Rust type aliases, unions and `macro_rules!` definitions, and Python PEP 695 type aliases.

### Changed

- C++ operator overloads, conversion operators, and destructor definitions are now indexed with their real names (e.g. `operator()`, `operator bool()`, `operator<`, `~Widget`) instead of falling back to `anonymous`. Names are normalized to one canonical spelling regardless of author spacing (`operator ()` → `operator()`, `operator "" _kb` → `operator""_kb`, `from_float< int >` → `from_float<int>`), conversion operators keep their full target type (`operator std::function<void()>()`), and functions returning function pointers (`int (*get())(int)`) are named. Out-of-line definitions with multi-level qualifiers (`void ns::Widget::draw()`) now index under the bare method name `draw` (previously `Widget::draw`), matching how single-level `Widget::draw()` definitions were already indexed.
- C/C++ typedef aliases are now stored as `symbol_type: "type"` (previously the alias was indexed as a `struct`/`union`/`enum` and the named tag was skipped). Anonymous specifiers no longer produce symbols named `anonymous`, and forward declarations are not indexed. **Existing graph databases should be re-ingested** so stored `symbol_type` values match; nothing gates on an index version, so stale kinds simply persist until re-ingest. Known trade-offs: body-less forward declarations (e.g. Pimpl-style `class Impl;`) lose their only index presence, and K&R-style C function definitions are not captured.
- CALLS-edge resolution is kind-aware: type-level symbols (interfaces, type aliases, unions, macros, and c-family structs/enums) are never call-edge targets, so a same-named type no longer steals or ambiguates a function's callers.
- `marrow watch` shares the dashboard watcher's re-index implementation: `.c` saves are picked up, node ids match full ingest, and CALLS edges/observations are maintained on watched saves.

## [0.1.3] - 2026-07-03

### Added

- Query-failure telemetry: every hard MCP tool-call failure is recorded in a capped per-workspace ring buffer with its exact inputs, plus per-category counters and a `tool_calls_total` denominator. Surfaced by `marrow doctor` (counters + last 10 failures) and the new `GET /api/query-failures?workspace_id=…` daemon endpoint.

### Changed

- Upgraded the MCP SDK (`rmcp`) from 0.16 to 2.1, aligning marrow with the MCP 2025-11-25 specification. Protocol version negotiation with older clients is handled by the SDK's service layer. Resolves RUSTSEC-2026-0189 for good; the cargo-audit exception added in 0.1.2 is removed.
- Agent-facing errors now carry guidance that works when followed literally: the "Symbol not found" and `save_observation` misses point at `find_symbol` (which needs no filepath), and the empty-graph note no longer tells agents to re-run a tool that cannot succeed. Windows verbatim `\\?\` prefixes are stripped from agent-facing paths.
- Registering a workspace appends `.marrow/` to the repo's `.git/info/exclude` (local-only, never committed), so marrow no longer dirties `git status` in ingested repos.
- Removed the `time <0.3.48` constraint dependency: `time` 0.3.49+ resolved the E0119 conflict with `cookie` 0.18, so fresh `cargo install` builds get the latest `time` again.

## [0.1.2] - 2026-07-03

### Added

- Post-ingest index self-check: every ingest (MCP `ingest_repo` and `marrow index`) now resolves a sample of freshly indexed symbols back through the agent query path — with stored file paths in both separator styles — and reports the result in the ingest output, so "ingest succeeded but queries can't see it" regressions fail loudly at ingest time. `marrow index` exits non-zero if the check fails.
- `marrow doctor [repo_id]` — runs the same index self-check on demand against the workspace database (honors `MARROW_DB_PATH`).
- `marrow --version` / `-V` / `version` prints the version. Previously the flag fell through to the stdio MCP server and hung the calling shell.
- Cross-workspace query routing: MCP query tools (capsule, impact, pipeline intents, skeleton) now serve an explicitly requested `repo_id` that was ingested into another registered workspace by opening that workspace's graph DB read-only via the registry, instead of failing.

### Fixed

- Unknown CLI arguments now exit with an error and usage hint instead of silently starting the stdio MCP server.
- The "Repo not found … Run ingest_repo first" error no longer misleads agents into an ingest/query loop when the repo is indexed in a different workspace; the not-found error now names the current workspace and lists the repos that are indexed in it.

### Changed

- npm publishes to the `latest` dist-tag (was `alpha`), matching the install docs after #58; no manual dist-tag move is needed after publishing.

### Security

- Bumped `form-data` and `tar` in the npm lockfile past GHSA-hmw2-7cc7-3qxx (high) and GHSA-vmf3-w455-68vh (moderate).
- Added a documented cargo-audit exception for RUSTSEC-2026-0189: the advisory covers rmcp's Streamable HTTP server transport, which Marrow does not compile (stdio transport only). The rmcp >=1.4 upgrade that removes the exception is tracked separately.

## [0.1.1] - 2026-06-12

### Fixed

- `cargo install marrow` / `cargo install --path .` failing with E0119 in the transitive `cookie` crate: `time` 0.3.48 (published 2026-06-12) added trait impls that conflict with `cookie` 0.18's blanket `From` impl, and `cargo install` resolves dependencies fresh instead of using `Cargo.lock`. `time` is now capped below 0.3.48 (via an optional constraint dependency on the `desktop` feature) until `cookie` ships a compatible release.
- The MCP server identity and macOS `Info.plist` versions are now derived from `CARGO_PKG_VERSION` instead of hardcoded strings, so they can no longer drift from the crate version.

## [0.1.0] - 2026-06-12

### Added

- Initial public alpha release of Marrow AST context engine
- **Core Features:**
  - Deterministic AST parsing for C++, Python, TypeScript, Rust, and Ruby using `tree-sitter`
  - Local SQLite graph database with recursive CTE impact analysis
  - Multi-repository cross-file dependency tracking
  - Condensed context capsules (function/class body placeholders, preserving signatures)
- **CLI Tools:**
  - `marrow init` — Workspace initialization
  - `marrow index` — Incremental repository ingestion
  - `marrow context` — Query and generate context packets (markdown/JSON)
  - `marrow integrate` — Interactive installer that registers Marrow with MCP-capable agents and writes skill/rule files
  - `marrow benchmark` — Performance testing and profiling
  - `marrow maintenance` — WAL checkpoint and vacuum operations
  - `marrow ui-app` — Desktop app registration and launch (`enable`/`open`/`disable`)
  - `marrow daemon` — Opt-in background daemon with autostart install/uninstall
  - `marrow perf-harness` — Reproducible ingest/query performance measurements (used by CI smoke thresholds)
- **MCP Integration:**
  - Model Context Protocol (MCP) stdio server for agent integration
  - First-class support for Claude Code, Cursor, Cline, GitHub Copilot, Windsurf, Zed, and others
- **Distribution:**
  - npm installer with automatic binary download and SHA256 verification
  - Native packages for macOS (DMG), Linux (AppImage/deb), Windows (MSI)
  - Desktop dashboard for graph exploration and capsule inspection
- **Performance:**
  - Configurable SQLite cache and memory-mapping via environment variables
  - Rayon-based parallel file ingestion
  - Bounded channel spill-to-disk for large codebases

### Security

- Binary integrity verification: SHA256 hashes published and verified on all npm installs
