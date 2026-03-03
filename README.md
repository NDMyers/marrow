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
