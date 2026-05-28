# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-05-27

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
  - `marrow benchmark` — Performance testing and profiling
  - `marrow maintenance` — WAL checkpoint and vacuum operations
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
