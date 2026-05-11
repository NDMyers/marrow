use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;

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

## Output hygiene

Do **not** add a "Made-with: Cursor" tag (or similar editor or tool attribution) to commits, pull requests, READMEs, or other generated or user-facing artifacts unless the user explicitly asks for it.
"#;

// ── Domain types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Agent {
    ClaudeCode,
    Antigravity,
    Cursor,
    GitHubCopilot,
    Cline,
    Windsurf,
    RooCode,
    Zed,
}

impl Agent {
    pub fn supports_scope(self, scope: Scope) -> bool {
        !matches!(
            (self, scope),
            (Agent::Windsurf | Agent::RooCode, Scope::Global)
        )
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
            (Agent::Cursor, Scope::Global) => home.join(".cursor/rules/marrow-optimization.mdc"),

            (Agent::GitHubCopilot, Scope::Project) => {
                PathBuf::from(".github/instructions/marrow-optimization.instructions.md")
            }
            (Agent::GitHubCopilot, Scope::Global) => home.join(
                "Library/Application Support/Code/User/prompts/marrow-optimization.instructions.md",
            ),

            // Cline project target is a bare file at the repo root — no subdirectory.
            (Agent::Cline, Scope::Project) => PathBuf::from(".clinerules"),
            (Agent::Cline, Scope::Global) => home.join(".cline/rules/marrow-optimization.md"),

            (Agent::Windsurf, Scope::Project) => PathBuf::from(".windsurfrules"),
            (Agent::Windsurf, Scope::Global) => home.join(".windsurfrules"),

            (Agent::RooCode, Scope::Project) => PathBuf::from(".roomrules"),
            (Agent::RooCode, Scope::Global) => home.join(".roomrules"),

            // Zed project target is a bare file at the repo root — no subdirectory.
            (Agent::Zed, Scope::Project) => PathBuf::from(".rules"),
            (Agent::Zed, Scope::Global) => home.join(".config/zed/rules/marrow-optimization.rules"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Project,
    Global,
}

#[derive(Debug, Clone, Copy)]
pub enum Method {
    WriteFile,
    Symlink,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstallStatus {
    Written,
    PreservedExisting,
}

// ── Public entry points ───────────────────────────────────────────────────────

/// Install the Marrow optimization rule file for a single agent.
/// Called by `marrow integrate` after MCP registration.
pub fn install_skill(
    agent: Agent,
    scope: Scope,
    method: Method,
    home: &Path,
) -> Result<InstallStatus> {
    if !agent.supports_scope(scope) {
        anyhow::bail!("agent does not have a verified rule target for this scope");
    }

    let target = agent.target_path(scope, home);
    let central = install_source_path(method, home)
        .unwrap_or_else(|| home.join(".marrow/marrow-optimization.md"));

    // Respect any existing user-managed instruction file or symlink target.
    // A dangling symlink (symlink_metadata ok but exists false) is not usable —
    // remove it so we can recreate a valid one rather than silently leaving it broken.
    if target.symlink_metadata().is_ok() && !target.exists() {
        fs::remove_file(&target)?;
    } else if target.exists() {
        return Ok(InstallStatus::PreservedExisting);
    }

    if matches!(method, Method::Symlink) {
        if let Some(parent) = central.parent() {
            fs::create_dir_all(parent)?;
        }
        if !central.exists() {
            fs::write(&central, MARROW_CORE_SKILL_MD)?;
        }
    }

    install(&target, method, &central)
}

/// Install the Marrow optimization skill to a generic skills directory.
/// Used by `AgentSkillTarget` entries that don't need custom `Agent` enum logic.
pub fn install_skill_to_dir(
    skills_dir: &str,
    scope: Scope,
    method: Method,
    home: &Path,
) -> Result<InstallStatus> {
    let target = match scope {
        Scope::Project => PathBuf::from(skills_dir).join("marrow-optimization.md"),
        Scope::Global => home.join(skills_dir).join("marrow-optimization.md"),
    };
    let central = install_source_path(method, home)
        .unwrap_or_else(|| home.join(".marrow/marrow-optimization.md"));

    // Dangling symlink cleanup + existence check (reuse same logic as install_skill).
    if target.symlink_metadata().is_ok() && !target.exists() {
        fs::remove_file(&target)?;
    } else if target.exists() {
        return Ok(InstallStatus::PreservedExisting);
    }

    if matches!(method, Method::Symlink) {
        if let Some(parent) = central.parent() {
            fs::create_dir_all(parent)?;
        }
        if !central.exists() {
            fs::write(&central, MARROW_CORE_SKILL_MD)?;
        }
    }

    install(&target, method, &central)
}

pub fn install_source_path(method: Method, home: &Path) -> Option<PathBuf> {
    match method {
        Method::WriteFile => None,
        Method::Symlink => Some(home.join(".marrow/marrow-optimization.md")),
    }
}

pub fn install_source_description(method: Method, home: &Path) -> String {
    match install_source_path(method, home) {
        Some(path) => format!("source: {}", path.display()),
        None => {
            "source: embedded Marrow template; edit/remove the target file directly".to_string()
        }
    }
}

// ── File-system helpers ───────────────────────────────────────────────────────

/// Low-level filesystem primitive. Only call via [`install_skill`].
/// The `central` parameter is only meaningful for [`Method::Symlink`].
fn install(target: &Path, method: Method, central: &Path) -> Result<InstallStatus> {
    if target.symlink_metadata().is_ok() && !target.exists() {
        fs::remove_file(target)?;
    } else if target.exists() {
        return Ok(InstallStatus::PreservedExisting);
    }

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
            std::os::unix::fs::symlink(central, target)?;
        }
    }

    Ok(InstallStatus::Written)
}

#[cfg(test)]
mod tests {
    use super::{
        install, install_source_description, install_source_path, Agent, InstallStatus, Method,
        Scope,
    };
    use std::{fs, path::Path};
    use tempfile::tempdir;

    #[test]
    fn copilot_project_skill_uses_valid_instruction_file() {
        let path = Agent::GitHubCopilot.target_path(Scope::Project, Path::new("/tmp/home"));

        assert_eq!(
            path,
            Path::new(".github/instructions/marrow-optimization.instructions.md")
        );
    }

    #[test]
    fn copilot_global_skill_uses_profile_prompts_instruction_file() {
        let path = Agent::GitHubCopilot.target_path(Scope::Global, Path::new("/tmp/home"));

        assert_eq!(
            path,
            Path::new("/tmp/home/Library/Application Support/Code/User/prompts/marrow-optimization.instructions.md")
        );
    }

    #[test]
    fn windsurf_project_skill_uses_existing_workspace_rule_file() {
        let path = Agent::Windsurf.target_path(Scope::Project, Path::new("/tmp/home"));

        assert_eq!(path, Path::new(".windsurfrules"));
    }

    #[test]
    fn roo_project_skill_uses_existing_workspace_rule_file() {
        let path = Agent::RooCode.target_path(Scope::Project, Path::new("/tmp/home"));

        assert_eq!(path, Path::new(".roomrules"));
    }

    #[test]
    fn windsurf_and_roo_do_not_claim_global_rule_support() {
        assert!(!Agent::Windsurf.supports_scope(Scope::Global));
        assert!(!Agent::RooCode.supports_scope(Scope::Global));
    }

    #[test]
    fn write_file_install_preserves_existing_instruction_file() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");

        fs::write(&target, "existing content").unwrap();

        install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(fs::read_to_string(&target).unwrap(), "existing content");
    }

    #[test]
    fn symlink_install_preserves_existing_instruction_file() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");

        fs::write(&target, "existing content").unwrap();

        install(&target, Method::Symlink, &central).unwrap();

        assert_eq!(fs::read_to_string(&target).unwrap(), "existing content");
    }

    #[test]
    fn symlink_install_source_path_uses_central_marrow_file() {
        let source = install_source_path(Method::Symlink, Path::new("/tmp/home"));

        assert_eq!(
            source.as_deref(),
            Some(Path::new("/tmp/home/.marrow/marrow-optimization.md"))
        );
    }

    #[test]
    fn dangling_symlink_is_replaced_not_preserved() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("target.md");
        let nonexistent = tmp.path().join("ghost.md"); // never created

        // Create a dangling symlink pointing at a nonexistent path.
        std::os::unix::fs::symlink(&nonexistent, &target).unwrap();
        assert!(
            target.symlink_metadata().is_ok(),
            "symlink entry should exist"
        );
        assert!(!target.exists(), "dangling symlink should not resolve");

        // install should remove the dangling symlink and write the file.
        let central = tmp.path().join("central.md");
        let result = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(result, InstallStatus::Written);
        assert!(
            target.exists(),
            "file should now exist after replacing dangling symlink"
        );
        assert!(fs::read_to_string(&target)
            .unwrap()
            .to_lowercase()
            .contains("marrow"));
    }

    #[test]
    fn write_file_install_source_description_points_users_to_target() {
        let description = install_source_description(Method::WriteFile, Path::new("/tmp/home"));

        assert!(
            description.contains("edit/remove the target file directly"),
            "expected direct-management guidance: {description}"
        );
    }

    #[test]
    fn install_skill_to_dir_writes_to_project_path() {
        let original_dir = std::env::current_dir().unwrap();
        let tmp = tempdir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        let result = std::panic::catch_unwind(|| {
            let home = tmp.path().join("fake_home");
            fs::create_dir_all(&home).unwrap();

            let status = super::install_skill_to_dir(
                ".goose/skills",
                Scope::Project,
                Method::WriteFile,
                &home,
            )
            .unwrap();

            assert_eq!(status, InstallStatus::Written);
            let target = tmp.path().join(".goose/skills/marrow-optimization.md");
            assert!(target.exists(), "project-scope skill file should exist");
            assert!(fs::read_to_string(&target)
                .unwrap()
                .to_lowercase()
                .contains("marrow"));
        });

        std::env::set_current_dir(original_dir).unwrap();
        if let Err(e) = result {
            std::panic::resume_unwind(e);
        }
    }

    #[test]
    fn install_skill_to_dir_preserves_existing() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();

        let dir = home.join(".goose/skills");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("marrow-optimization.md"), "custom content").unwrap();

        let status =
            super::install_skill_to_dir(".goose/skills", Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(
            fs::read_to_string(dir.join("marrow-optimization.md")).unwrap(),
            "custom content"
        );
    }

    #[test]
    fn install_skill_to_dir_symlink_creates_central_and_link() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();

        let status =
            super::install_skill_to_dir(".forge/skills", Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        let central = home.join(".marrow/marrow-optimization.md");
        assert!(central.exists(), "central file should exist");
        let target = home.join(".forge/skills/marrow-optimization.md");
        assert!(
            target.symlink_metadata().is_ok(),
            "symlink should exist at target"
        );
    }

    #[test]
    fn install_skill_to_dir_global_scope_uses_home_prefix() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();

        let status =
            super::install_skill_to_dir(".trae/skills", Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        let target = home.join(".trae/skills/marrow-optimization.md");
        assert!(target.exists(), "global-scope skill file should exist");
    }

    #[test]
    fn install_skill_to_dir_cleans_dangling_symlink() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();

        let dir = home.join(".crush/skills");
        fs::create_dir_all(&dir).unwrap();
        let target = dir.join("marrow-optimization.md");
        let ghost = tmp.path().join("ghost.md");
        std::os::unix::fs::symlink(&ghost, &target).unwrap();
        assert!(target.symlink_metadata().is_ok());
        assert!(!target.exists());

        let status =
            super::install_skill_to_dir(".crush/skills", Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert!(target.exists());
    }
}
