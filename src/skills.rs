use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;

pub const MARROW_CORE_SKILL_MD: &str = r#"---
# marrow-generated: true
# marrow-generated-checksum: fnv1a64:aa9240036df20f67
description: "Marrow context packet and MCP guidance for codebase exploration, symbol tracing, and refactor impact analysis."
alwaysApply: true
---
# Marrow MCP Optimization Directives

Marrow is a local, deterministic AST graph engine and provider-neutral context compiler. Its fastest, cheapest path is the CLI `marrow context` packet — one command that compiles routing, source spans, freshness, and provenance before the model loop ever starts. Reach for the MCP `run_pipeline` loop only as a targeted follow-up.

## Default First Move — `marrow context`

For any broad or ambiguous multi-file task, your default first move is:

```
marrow context "<task>" --repo <repo_id> --budget <tokens> --format markdown|json --profile local-32k
```

This compiles a single provider-neutral packet up front. Prefer it over an interactive MCP loop: each `run_pipeline` call costs one extra LLM turn, and the loop has measured overhead of **+146% LLM requests** and **+53.6% input tokens** versus native tooling. Pay that cost only when a single structured response replaces 3+ sequential native file reads.

## Cold Start — Handling a `needs_index` Packet

If `marrow context` returns a routing outcome of `needs_index`, the graph for that repo is empty or stale. Recover once, then re-run:

1. Index the repository a single time: `marrow index` (or `mcp_marrow_ingest_repo` if you are already in an MCP session).
2. Re-run the same `marrow context "<task>" --repo <repo_id>` command.
3. If it still reports `needs_index`, confirm the `--repo` id matches an ingested repository before retrying.

Do not loop on `needs_index` — index once, then re-request the packet.

## Targeted Follow-Up — MCP `run_pipeline`

After the packet (or during an active agent loop on already-indexed code), use `mcp_marrow_run_pipeline` for narrow, structured graph views. It routes intents to the right view:

| Need | Intent |
|------|--------|
| Exploring unfamiliar code across **3+ files** | `explore_batch` |
| Tracing a call chain or dependency flow | `trace_flow` / `dependency_graph` |
| Mapping a class, module, or directory | `map_class` / `get_skeleton` |
| Assessing blast radius before a refactor | `refactor_symbol` |
| Orienting in an unfamiliar repository | `analyze_repo` |
| Resolving a partially-known symbol name | `find_symbol` |
| Expanding a condensed signature from a prior result | `read_node` |

Each call adds one LLM turn, so prefer it only when it replaces 3+ native reads. If a pipeline call reports a missing or stale graph, call `mcp_marrow_ingest_repo` once, then retry.

## Tools

- CLI `marrow context`: Provider-neutral markdown/json packet with routing outcome, source spans, freshness, budget, and provenance. **Default first move.**
- `mcp_marrow_run_pipeline`: Targeted follow-up — routes intents to the best graph view during an active loop.
- `mcp_marrow_get_context_capsule`: Pivot symbol source + condensed depth-1 callers, callees, imports.
- `mcp_marrow_analyze_impact`: Recursive caller/importer map for blast-radius analysis.
- `mcp_marrow_ingest_repo`: Build or refresh the AST graph for a repository.
- `mcp_marrow_get_skeleton`: Compact architecture map grouped by file or directory.
- `mcp_marrow_get_session_context`: Retrieve saved session observations and symbol-linked context.
- `mcp_marrow_save_observation`: Save a hash-linked note about a symbol for later context.
- `mcp_marrow_workspace_setup`: Initialize workspace rules and MCP config.

## When NOT To Use Marrow

- If `marrow context` returns `use_native`, follow native read/search instead of forcing Marrow.
- Single known file under ~100 lines — use `read_file` directly.
- Config, environment, documentation, or prompt files — use native tools.
- Exact text search or quick existence checks — use `grep_search` or `file_search`.

## Output hygiene

Do **not** add a "Made-with: Cursor" tag (or similar editor or tool attribution) to commits, pull requests, READMEs, or other generated or user-facing artifacts unless the user explicitly asks for it.
"#;

const GENERATED_MARKER_LINE: &str = "# marrow-generated: true";
const GENERATED_CHECKSUM_PREFIX: &str = "# marrow-generated-checksum: fnv1a64:";

const LEGACY_STALE_MARROW_CORE_SKILL_MD: &str = r#"---
description: "Marrow MCP guidance for codebase exploration, symbol tracing, and refactor impact analysis."
applyTo: "**/*.{rs,py,ts,tsx,js,jsx,c,cc,cpp,h,hpp}"
alwaysApply: false
---
# Marrow MCP Optimization Directives

Marrow is a local, deterministic AST dependency graph context engine. Prefer it when the task needs code structure, dependencies, symbol neighborhoods, execution traces, repo maps, or refactor blast-radius analysis.

Do not route every lookup through Marrow. Each MCP call adds an LLM turn and context replay, so Marrow pays off when you need roughly 3+ symbols, cross-file traces, broad repository maps, or impact analysis before changing shared code.

## Primary Workflow

1. Start with `mcp_marrow_run_pipeline` when possible; it chooses the right graph view for intents like `analyze_repo`, `explore_symbol`, `trace_flow`, `refactor_symbol`, and `read_node`.
2. If a Marrow tool reports a missing or stale graph, call `mcp_marrow_ingest_repo` and retry the original request.
3. Manually ingest after significant code changes or when explicitly refreshing an out-of-date graph. Do not make first-step ingestion routine.
4. Use `read_node` through `mcp_marrow_run_pipeline` to expand condensed signatures returned by a previous Marrow result.

## Tools

- `mcp_marrow_run_pipeline`: Primary entry point that routes high-level intents to the best Marrow context view.
- `mcp_marrow_get_context_capsule`: Returns a pivot symbol's source plus condensed depth-1 callers, callees, and imports.
- `mcp_marrow_analyze_impact`: Maps recursive callers and importers that may be affected by changing a symbol.
- `mcp_marrow_ingest_repo`: Builds or refreshes the tree-sitter AST graph for a repository.
- `mcp_marrow_get_skeleton`: Produces a compact architecture map grouped by file or directory.
- `mcp_marrow_get_session_context`: Retrieves saved session observations and symbol-linked context.
- `mcp_marrow_save_observation`: Saves a hash-linked note about a symbol for later session context.
- `mcp_marrow_workspace_setup`: Initializes Marrow workspace rules and MCP config.

## When To Use Marrow

- Understanding an unfamiliar symbol and its immediate dependencies.
- Tracing outbound flow across functions, classes, modules, or files.
- Mapping a directory or repository before nontrivial changes.
- Estimating refactor, rename, deletion, or API-change blast radius.
- Preserving useful symbol-specific observations for future work.

## When NOT To Use Marrow

- Single-file lookup where the target file is already known.
- Config, docs, YAML, README, changelog, or prompt-file edits.
- Known files under about 100 lines.
- Exact text search, simple existence checks, or quick verification.
- Reading nearby code after a precise grep or compiler diagnostic already points to it.
"#;

/// Observed older unmarked Copilot global prompt that still mentions `enforcement mode`.
/// Byte-identical to `LEGACY_STALE_MARROW_CORE_SKILL_MD` except `mcp_marrow_workspace_setup`
/// describes enforcement mode in addition to workspace rules and MCP config.
const LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD: &str = r#"---
description: "Marrow MCP guidance for codebase exploration, symbol tracing, and refactor impact analysis."
applyTo: "**/*.{rs,py,ts,tsx,js,jsx,c,cc,cpp,h,hpp}"
alwaysApply: false
---
# Marrow MCP Optimization Directives

Marrow is a local, deterministic AST dependency graph context engine. Prefer it when the task needs code structure, dependencies, symbol neighborhoods, execution traces, repo maps, or refactor blast-radius analysis.

Do not route every lookup through Marrow. Each MCP call adds an LLM turn and context replay, so Marrow pays off when you need roughly 3+ symbols, cross-file traces, broad repository maps, or impact analysis before changing shared code.

## Primary Workflow

1. Start with `mcp_marrow_run_pipeline` when possible; it chooses the right graph view for intents like `analyze_repo`, `explore_symbol`, `trace_flow`, `refactor_symbol`, and `read_node`.
2. If a Marrow tool reports a missing or stale graph, call `mcp_marrow_ingest_repo` and retry the original request.
3. Manually ingest after significant code changes or when explicitly refreshing an out-of-date graph. Do not make first-step ingestion routine.
4. Use `read_node` through `mcp_marrow_run_pipeline` to expand condensed signatures returned by a previous Marrow result.

## Tools

- `mcp_marrow_run_pipeline`: Primary entry point that routes high-level intents to the best Marrow context view.
- `mcp_marrow_get_context_capsule`: Returns a pivot symbol's source plus condensed depth-1 callers, callees, and imports.
- `mcp_marrow_analyze_impact`: Maps recursive callers and importers that may be affected by changing a symbol.
- `mcp_marrow_ingest_repo`: Builds or refreshes the tree-sitter AST graph for a repository.
- `mcp_marrow_get_skeleton`: Produces a compact architecture map grouped by file or directory.
- `mcp_marrow_get_session_context`: Retrieves saved session observations and symbol-linked context.
- `mcp_marrow_save_observation`: Saves a hash-linked note about a symbol for later session context.
- `mcp_marrow_workspace_setup`: Initializes Marrow workspace rules, MCP config, and enforcement mode.

## When To Use Marrow

- Understanding an unfamiliar symbol and its immediate dependencies.
- Tracing outbound flow across functions, classes, modules, or files.
- Mapping a directory or repository before nontrivial changes.
- Estimating refactor, rename, deletion, or API-change blast radius.
- Preserving useful symbol-specific observations for future work.

## When NOT To Use Marrow

- Single-file lookup where the target file is already known.
- Config, docs, YAML, README, changelog, or prompt-file edits.
- Known files under about 100 lines.
- Exact text search, simple existence checks, or quick verification.
- Reading nearby code after a precise grep or compiler diagnostic already points to it.
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
    Refreshed,
    PreservedExisting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingContentStatus {
    CurrentManaged,
    RefreshableManaged,
    Custom,
}

fn generated_content_for_checksum(content: &str) -> String {
    let mut generated = String::with_capacity(content.len());

    for line in content.split_inclusive('\n') {
        let marker_line = line.trim_end_matches(&['\r', '\n'][..]);
        if marker_line == GENERATED_MARKER_LINE
            || marker_line.starts_with(GENERATED_CHECKSUM_PREFIX)
        {
            continue;
        }
        generated.push_str(line);
    }

    generated
}

fn generated_checksum(content: &str) -> String {
    let generated = generated_content_for_checksum(content);
    let mut hash = 0xcbf29ce484222325u64;

    for byte in generated.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!("{hash:016x}")
}

fn generated_marker_checksum(content: &str) -> Option<&str> {
    content.lines().find_map(|line| {
        line.trim_end_matches('\r')
            .strip_prefix(GENERATED_CHECKSUM_PREFIX)
            .map(str::trim)
    })
}

fn has_generated_marker(content: &str) -> bool {
    content
        .lines()
        .any(|line| line.trim_end_matches('\r') == GENERATED_MARKER_LINE)
}

fn has_valid_generated_marker(content: &str) -> bool {
    has_generated_marker(content)
        && generated_marker_checksum(content)
            .is_some_and(|checksum| checksum == generated_checksum(content))
}

fn classify_existing_content(content: &str) -> ExistingContentStatus {
    if content == MARROW_CORE_SKILL_MD {
        return ExistingContentStatus::CurrentManaged;
    }

    if content == generated_content_for_checksum(MARROW_CORE_SKILL_MD)
        || content == LEGACY_STALE_MARROW_CORE_SKILL_MD
        || content == LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD
    {
        return ExistingContentStatus::RefreshableManaged;
    }

    if has_generated_marker(content) {
        if has_valid_generated_marker(content) {
            ExistingContentStatus::RefreshableManaged
        } else {
            ExistingContentStatus::Custom
        }
    } else {
        ExistingContentStatus::Custom
    }
}

fn refresh_existing_managed_file(path: &Path) -> Result<InstallStatus> {
    let Ok(existing) = fs::read_to_string(path) else {
        return Ok(InstallStatus::PreservedExisting);
    };

    match classify_existing_content(&existing) {
        ExistingContentStatus::CurrentManaged | ExistingContentStatus::Custom => {
            Ok(InstallStatus::PreservedExisting)
        }
        ExistingContentStatus::RefreshableManaged => {
            fs::write(path, MARROW_CORE_SKILL_MD)?;
            Ok(InstallStatus::Refreshed)
        }
    }
}

fn prepare_symlink_source(target: &Path, central: &Path) -> Result<Option<InstallStatus>> {
    if let Some(parent) = central.parent() {
        fs::create_dir_all(parent)?;
    }

    if central.exists() {
        return refresh_existing_managed_file(central).map(Some);
    }

    if !target.exists() {
        fs::write(central, MARROW_CORE_SKILL_MD)?;
        return Ok(Some(InstallStatus::Written));
    }

    Ok(None)
}

fn combine_install_status(
    target_status: InstallStatus,
    source_status: Option<InstallStatus>,
) -> InstallStatus {
    if target_status == InstallStatus::Written {
        InstallStatus::Written
    } else if target_status == InstallStatus::Refreshed
        || source_status == Some(InstallStatus::Refreshed)
    {
        InstallStatus::Refreshed
    } else {
        target_status
    }
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

    let source_status = if matches!(method, Method::Symlink) {
        prepare_symlink_source(&target, &central)?
    } else {
        None
    };
    let target_status = install(&target, method, &central)?;

    Ok(combine_install_status(target_status, source_status))
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

    let source_status = if matches!(method, Method::Symlink) {
        prepare_symlink_source(&target, &central)?
    } else {
        None
    };
    let target_status = install(&target, method, &central)?;

    Ok(combine_install_status(target_status, source_status))
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
        return match method {
            Method::WriteFile => refresh_existing_managed_file(target),
            Method::Symlink => Ok(InstallStatus::PreservedExisting),
        };
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
            #[cfg(unix)]
            {
                std::os::unix::fs::symlink(central, target)?;
            }
            #[cfg(not(unix))]
            {
                // Symlinks on Windows require elevated privileges or Developer
                // Mode; fall back to copying the central file's content so the
                // target still ends up managed and in sync.
                fs::copy(central, target)?;
            }
        }
    }

    Ok(InstallStatus::Written)
}

#[cfg(test)]
mod tests {
    use super::{
        install, install_source_description, install_source_path, Agent, InstallStatus, Method,
        Scope, LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD, LEGACY_STALE_MARROW_CORE_SKILL_MD,
        MARROW_CORE_SKILL_MD,
    };
    use std::{fs, path::Path};
    use tempfile::tempdir;

    fn marked_generated_content(unmarked_content: &str) -> String {
        let mut marked = unmarked_content.replacen(
            "---\n",
            "---\n# marrow-generated: true\n# marrow-generated-checksum: fnv1a64:0000000000000000\n",
            1,
        );
        let checksum = super::generated_checksum(&marked);
        marked = marked.replacen("0000000000000000", &checksum, 1);
        marked
    }

    #[test]
    fn marrow_core_skill_md_is_self_consistent_current_managed() {
        // Exact constant classifies as the current managed template.
        assert_eq!(
            super::classify_existing_content(MARROW_CORE_SKILL_MD),
            super::ExistingContentStatus::CurrentManaged,
        );
        // Embedded checksum marker matches the recomputed FNV-1a64 of the body.
        assert_eq!(
            super::generated_marker_checksum(MARROW_CORE_SKILL_MD),
            Some(super::generated_checksum(MARROW_CORE_SKILL_MD).as_str()),
        );
        // The marker-stripped form classifies as a refreshable managed template.
        assert_eq!(
            super::classify_existing_content(&super::generated_content_for_checksum(
                MARROW_CORE_SKILL_MD,
            )),
            super::ExistingContentStatus::RefreshableManaged,
        );
    }

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
    fn marrow_core_skill_directive_matches_current_mcp_guidance() {
        assert!(
            MARROW_CORE_SKILL_MD.lines().count() < 80,
            "directive should stay under 80 lines"
        );
        assert!(!MARROW_CORE_SKILL_MD.contains("globs: \"*\""));
        assert!(MARROW_CORE_SKILL_MD.contains("alwaysApply: true"));
        assert!(MARROW_CORE_SKILL_MD.contains("# marrow-generated: true"));
        assert!(super::has_valid_generated_marker(MARROW_CORE_SKILL_MD));

        for tool_name in [
            "mcp_marrow_run_pipeline",
            "mcp_marrow_get_context_capsule",
            "mcp_marrow_analyze_impact",
            "mcp_marrow_ingest_repo",
            "mcp_marrow_get_skeleton",
            "mcp_marrow_get_session_context",
            "mcp_marrow_save_observation",
            "mcp_marrow_workspace_setup",
        ] {
            assert!(
                MARROW_CORE_SKILL_MD.contains(tool_name),
                "directive should document {tool_name}"
            );
        }

        assert!(MARROW_CORE_SKILL_MD.contains("## When NOT To Use Marrow"));
        assert!(MARROW_CORE_SKILL_MD.contains("marrow context"));
        assert!(MARROW_CORE_SKILL_MD.contains("provider-neutral"));
        assert!(MARROW_CORE_SKILL_MD.contains("find_symbol"));
        assert!(MARROW_CORE_SKILL_MD.contains("explore_batch"));
        assert!(MARROW_CORE_SKILL_MD.contains("3+ files"));
        assert!(MARROW_CORE_SKILL_MD.contains("dependency_graph"));
        assert!(MARROW_CORE_SKILL_MD.contains("call chain"));
        assert!(MARROW_CORE_SKILL_MD.contains("map_class"));
        assert!(MARROW_CORE_SKILL_MD.contains("Config, environment"));
        assert!(MARROW_CORE_SKILL_MD.contains("Exact text search"));
        assert!(!MARROW_CORE_SKILL_MD.contains("Never read raw files directly"));
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
    fn write_file_install_preserves_custom_content_that_mentions_marrow() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let custom = "# Local workflow\nUse Marrow only after checking this repo's team notes.\n";

        fs::write(&target, custom).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), custom);
    }

    #[test]
    fn write_file_install_refreshes_current_unmarked_template() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let current_unmarked = super::generated_content_for_checksum(MARROW_CORE_SKILL_MD);

        fs::write(&target, current_unmarked).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn write_file_install_refreshes_enforcement_mode_legacy_template() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");

        fs::write(&target, LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn write_file_install_preserves_enforcement_mode_legacy_template_with_user_addition() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let edited = format!("{LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD}\nUser addition\n");

        fs::write(&target, &edited).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), edited);
    }

    #[test]
    fn write_file_install_refreshes_known_legacy_template() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");

        fs::write(&target, LEGACY_STALE_MARROW_CORE_SKILL_MD).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn write_file_install_refreshes_valid_marked_generated_file() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let marked_legacy = marked_generated_content(LEGACY_STALE_MARROW_CORE_SKILL_MD);

        fs::write(&target, marked_legacy).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn write_file_install_preserves_marked_file_with_user_edit() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let mut edited = marked_generated_content(LEGACY_STALE_MARROW_CORE_SKILL_MD);
        edited.push_str("\nUser addition\n");

        fs::write(&target, &edited).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), edited);
    }

    #[test]
    fn write_file_install_preserves_legacy_template_with_user_addition() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("existing.instructions.md");
        let central = tmp.path().join("central.md");
        let edited = format!("{LEGACY_STALE_MARROW_CORE_SKILL_MD}\nUser addition\n");

        fs::write(&target, &edited).unwrap();

        let status = install(&target, Method::WriteFile, &central).unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), edited);
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

    #[cfg(unix)]
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
            assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
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
    fn install_skill_refreshes_global_copilot_enforcement_mode_legacy_file() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::GitHubCopilot.target_path(Scope::Global, &home);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, LEGACY_ENFORCEMENT_MODE_MARROW_CORE_SKILL_MD).unwrap();

        let status = super::install_skill(
            Agent::GitHubCopilot,
            Scope::Global,
            Method::WriteFile,
            &home,
        )
        .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn install_skill_refreshes_global_copilot_legacy_file() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::GitHubCopilot.target_path(Scope::Global, &home);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, LEGACY_STALE_MARROW_CORE_SKILL_MD).unwrap();

        let status = super::install_skill(
            Agent::GitHubCopilot,
            Scope::Global,
            Method::WriteFile,
            &home,
        )
        .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn install_skill_writes_global_copilot_template_with_generated_marker() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::GitHubCopilot.target_path(Scope::Global, &home);

        let status = super::install_skill(
            Agent::GitHubCopilot,
            Scope::Global,
            Method::WriteFile,
            &home,
        )
        .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
    }

    #[test]
    fn install_skill_to_dir_refreshes_project_legacy_file() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let skills_dir = tmp.path().join(".goose/skills");
        fs::create_dir_all(&skills_dir).unwrap();
        let target = skills_dir.join("marrow-optimization.md");
        fs::write(&target, LEGACY_STALE_MARROW_CORE_SKILL_MD).unwrap();

        let status = super::install_skill_to_dir(
            skills_dir.to_str().unwrap(),
            Scope::Project,
            Method::WriteFile,
            &home,
        )
        .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&target).unwrap(), MARROW_CORE_SKILL_MD);
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

    #[cfg(unix)]
    #[test]
    fn install_skill_to_dir_symlink_refreshes_managed_central_source() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let central = home.join(".marrow/marrow-optimization.md");
        fs::create_dir_all(central.parent().unwrap()).unwrap();
        fs::write(&central, LEGACY_STALE_MARROW_CORE_SKILL_MD).unwrap();
        let target = home.join(".forge/skills/marrow-optimization.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&central, &target).unwrap();

        let status =
            super::install_skill_to_dir(".forge/skills", Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert_eq!(fs::read_to_string(&central).unwrap(), MARROW_CORE_SKILL_MD);
        assert!(target.symlink_metadata().unwrap().file_type().is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn install_skill_to_dir_symlink_preserves_custom_central_source() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let central = home.join(".marrow/marrow-optimization.md");
        fs::create_dir_all(central.parent().unwrap()).unwrap();
        fs::write(&central, "custom central content").unwrap();

        let status =
            super::install_skill_to_dir(".forge/skills", Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert_eq!(
            fs::read_to_string(&central).unwrap(),
            "custom central content"
        );
        assert!(home
            .join(".forge/skills/marrow-optimization.md")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[test]
    fn install_skill_to_dir_symlink_preserves_custom_regular_target() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = home.join(".forge/skills/marrow-optimization.md");
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "custom target content").unwrap();

        let status =
            super::install_skill_to_dir(".forge/skills", Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            "custom target content"
        );
        assert!(!target.symlink_metadata().unwrap().file_type().is_symlink());
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

    #[cfg(unix)]
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

    #[cfg(unix)]
    #[test]
    fn install_skill_to_dir_symlink_cleans_dangling_symlink() {
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
            super::install_skill_to_dir(".crush/skills", Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert!(target.exists());
        assert!(target.symlink_metadata().unwrap().file_type().is_symlink());
        assert_eq!(
            fs::read_to_string(home.join(".marrow/marrow-optimization.md")).unwrap(),
            MARROW_CORE_SKILL_MD
        );
    }
}
