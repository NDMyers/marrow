use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use console::style;
use dialoguer::{MultiSelect, Select, theme::ColorfulTheme};

pub const MARROW_CORE_SKILL_MD: &str = r#"---
description: "Marrow Core Skill: Optimization directives for reading files and exploring the codebase via MCP."
globs: "*"
alwaysApply: true
---
# Marrow MCP Optimization Directives

You are equipped with the Marrow Model Context Protocol (MCP) server. To maximize token efficiency and maintain focus, you MUST adhere to the following directives:

**Never read raw files directly.** Route all context gathering through the Marrow MCP tools instead of your native file-reading capabilities.

## Available Tools

### `get_context_capsule(symbol_name, repo_id)`
Fetch the full source of a pivot symbol plus condensed signatures of its depth-1 neighbors (callers, callees, imports). Returns a Context Capsule optimised for LLM consumption.

- Use this when you need to understand the implementation of a specific symbol (function, struct, class, etc.).
- `symbol_name`: The exact symbol name to look up (e.g. `MyClass` or `process_data`).
- `repo_id`: The repository identifier used during ingestion (e.g. `backend_api`).

### `analyze_impact(symbol_name, repo_id)`
Map the blast radius of a proposed change. Recursively traverses the dependency graph to find every transitive caller/importer across all repos up to depth 10.

- Use this before refactoring or deleting a symbol to understand what will break.
- `symbol_name`: The symbol whose downstream dependents you want to analyze.
- `repo_id`: The repository identifier for the pivot symbol.

### `ingest_repo(repo_id, root_path)`
Parse a local repository with tree-sitter and populate (or refresh) the AST dependency graph in the SQLite database.

- Run this before querying a repo for the first time, or after significant code changes.
- `repo_id`: A unique, stable identifier for the repository (e.g. `backend_api`).
- `root_path`: Absolute or relative path to the repository root on disk.

## Workflow

1. **First time on a repo**: Call `ingest_repo` to build the AST graph.
2. **Finding a symbol**: Call `get_context_capsule` with the exact symbol name and repo id.
3. **Before changing a symbol**: Call `analyze_impact` to see all transitive dependents.

By strictly routing your context gathering through these tools, you will operate faster and retain a much longer effective memory window.
"#;

// ── Domain types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub enum Agent {
    ClaudeCode,
    Antigravity,
    Cursor,
    GitHubCopilot,
    Cline,
    Zed,
}

impl Agent {
    fn label(self) -> &'static str {
        match self {
            Agent::ClaudeCode    => "Claude Code",
            Agent::Antigravity   => "Antigravity (Gemini)",
            Agent::Cursor        => "Cursor",
            Agent::GitHubCopilot => "GitHub Copilot",
            Agent::Cline         => "Cline",
            Agent::Zed           => "Zed",
        }
    }

    /// Resolve the target installation path from the spec's path matrix.
    pub fn target_path(self, scope: Scope, home: &Path) -> PathBuf {
        match (self, scope) {
            (Agent::ClaudeCode, Scope::Project) => {
                PathBuf::from(".claude/skills/marrow-optimization.md")
            }
            (Agent::ClaudeCode, Scope::Global) => {
                home.join(".claude/skills/marrow-optimization.md")
            }

            (Agent::Antigravity, Scope::Project) => {
                PathBuf::from(".antigravity/skills/marrow-optimization.md")
            }
            (Agent::Antigravity, Scope::Global) => {
                home.join(".antigravity/skills/marrow-optimization.md")
            }

            (Agent::Cursor, Scope::Project) => {
                PathBuf::from(".cursor/rules/marrow-optimization.mdc")
            }
            (Agent::Cursor, Scope::Global) => {
                home.join(".cursor/rules/marrow-optimization.mdc")
            }

            (Agent::GitHubCopilot, Scope::Project) => {
                PathBuf::from(".github/copilot-instructions/marrow-optimization.md")
            }
            (Agent::GitHubCopilot, Scope::Global) => {
                home.join(".copilot/skills/marrow-optimization.md")
            }

            // Cline project target is a bare file at the repo root — no subdirectory.
            (Agent::Cline, Scope::Project) => PathBuf::from(".clinerules"),
            (Agent::Cline, Scope::Global) => {
                home.join(".cline/rules/marrow-optimization.md")
            }

            // Zed project target is a bare file at the repo root — no subdirectory.
            (Agent::Zed, Scope::Project) => PathBuf::from(".rules"),
            (Agent::Zed, Scope::Global) => {
                home.join(".config/zed/rules/marrow-optimization.rules")
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Scope {
    Project,
    Global,
}

#[derive(Debug, Clone, Copy)]
pub enum Method {
    WriteFile,
    Symlink,
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Install the Marrow optimization skill for a single agent.
/// Called by `marrow integrate` after MCP registration.
pub fn install_skill(agent: Agent, scope: Scope, method: Method, home: &Path) -> Result<()> {
    let target = agent.target_path(scope, home);
    let central = home.join(".marrow/marrow-optimization.md");

    if matches!(method, Method::Symlink) {
        if let Some(parent) = central.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&central, MARROW_CORE_SKILL_MD)?;
    }

    install(&target, method, &central)
}

// ── File-system helpers ───────────────────────────────────────────────────────

pub fn install(target: &Path, method: Method, central: &Path) -> Result<()> {
    // Ensure parent directory exists (bare-root files like .clinerules have no parent dir to create).
    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    match method {
        Method::WriteFile => {
            fs::write(target, MARROW_CORE_SKILL_MD)?;
        }
        Method::Symlink => {
            // Remove any existing file or broken symlink before creating the new one.
            if target.exists() || target.symlink_metadata().is_ok() {
                fs::remove_file(target)?;
            }
            std::os::unix::fs::symlink(central, target)?;
        }
    }

    Ok(())
}
