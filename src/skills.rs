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
    AntigravityCli,
    Cursor,
    GitHubCopilot,
    Cline,
    Windsurf,
    RooCode,
    Zed,
}

impl Agent {
    pub fn supports_scope(self, scope: Scope) -> bool {
        // No verified on-disk global rule target:
        // - Windsurf: global rules are a single shared
        //   ~/.codeium/windsurf/memories/global_rules.md Marrow shouldn't own.
        // - Cursor: global "User Rules" live inside the app's settings store,
        //   not in a filesystem rules directory.
        // - Zed: the global Rules Library is an internal prompt database
        //   (~/.config/zed/prompts/*.mdb), not loose rule files.
        !matches!(
            (self, scope),
            (Agent::Windsurf | Agent::Cursor | Agent::Zed, Scope::Global)
        )
    }

    /// Resolve the target installation path from the spec's path matrix.
    pub fn target_path(self, scope: Scope, home: &Path) -> PathBuf {
        match (self, scope) {
            // Claude Code discovers skills only as `<skills-root>/<name>/SKILL.md`
            // directory packages; a flat `.md` dropped in `.claude/skills/` is
            // silently ignored by its skill loader.
            (Agent::ClaudeCode, Scope::Project) => {
                PathBuf::from(".claude/skills/marrow-optimization/SKILL.md")
            }
            (Agent::ClaudeCode, Scope::Global) => {
                home.join(".claude/skills/marrow-optimization/SKILL.md")
            }

            (Agent::Antigravity, Scope::Project) => {
                PathBuf::from(".antigravity/skills/marrow-optimization.md")
            }
            (Agent::Antigravity, Scope::Global) => {
                home.join(".antigravity/skills/marrow-optimization.md")
            }

            // The Antigravity CLI (`agy`) reads workspace skills from the
            // universal `.agents/skills/` directory and global skills from the
            // shared `~/.gemini/skills/<name>/SKILL.md` package layout, which
            // the Antigravity IDE also picks up.
            (Agent::AntigravityCli, Scope::Project) => {
                PathBuf::from(".agents/skills/marrow-optimization.md")
            }
            (Agent::AntigravityCli, Scope::Global) => {
                home.join(".gemini/skills/marrow-optimization/SKILL.md")
            }

            (Agent::Cursor, Scope::Project) => {
                PathBuf::from(".cursor/rules/marrow-optimization.mdc")
            }
            // Unreachable for installs (supports_scope denies Global): Cursor
            // keeps global User Rules in its app settings, not on disk.
            (Agent::Cursor, Scope::Global) => home.join(".cursor/rules/marrow-optimization.mdc"),

            (Agent::GitHubCopilot, Scope::Project) => {
                PathBuf::from(".github/instructions/marrow-optimization.instructions.md")
            }
            // VS Code reads global instruction files from <user dir>/prompts/,
            // which lives at a platform-specific location under home.
            (Agent::GitHubCopilot, Scope::Global) => {
                vscode_user_dir(home).join("prompts/marrow-optimization.instructions.md")
            }

            // Cline project target is a bare file at the repo root — no subdirectory.
            (Agent::Cline, Scope::Project) => PathBuf::from(".clinerules"),
            // Cline's global rules folder is ~/Documents/Cline/Rules on every platform.
            (Agent::Cline, Scope::Global) => {
                home.join("Documents/Cline/Rules/marrow-optimization.md")
            }

            // Windsurf reads workspace rules from the .windsurf/rules/ directory;
            // the legacy single-file .windsurfrules is deprecated and size-capped.
            (Agent::Windsurf, Scope::Project) => {
                PathBuf::from(".windsurf/rules/marrow-optimization.md")
            }
            // Unreachable for installs (supports_scope denies Global).
            (Agent::Windsurf, Scope::Global) => home.join(".windsurf/rules/marrow-optimization.md"),

            // Roo Code loads .roo/rules/ first and only falls back to a bare
            // .roorules file; the bare file is written so a user's existing
            // .roorules is never shadowed by a Marrow-created directory.
            (Agent::RooCode, Scope::Project) => PathBuf::from(".roorules"),
            // Roo Code loads global rules from every file in ~/.roo/rules/.
            (Agent::RooCode, Scope::Global) => home.join(".roo/rules/marrow-optimization.md"),

            // Zed project target is a bare file at the repo root — no subdirectory.
            (Agent::Zed, Scope::Project) => PathBuf::from(".rules"),
            // Unreachable for installs (supports_scope denies Global): Zed's
            // Rules Library is an internal prompt database, not rule files.
            (Agent::Zed, Scope::Global) => home.join(".config/zed/rules/marrow-optimization.rules"),
        }
    }
}

/// VS Code's per-user configuration directory, resolved relative to `home`.
/// Shared by the Copilot global instructions target and the VS Code / Cline
/// MCP config writers so every VS Code path is platform-gated in one place.
pub fn vscode_user_dir(home: &Path) -> PathBuf {
    #[cfg(target_os = "macos")]
    {
        home.join("Library/Application Support/Code/User")
    }
    #[cfg(target_os = "windows")]
    {
        home.join("AppData/Roaming/Code/User")
    }
    #[cfg(not(any(target_os = "macos", target_os = "windows")))]
    {
        home.join(".config/Code/User")
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

/// Rewrite the value of the `# marrow-generated-checksum:` marker line in place,
/// preserving every other line and its trailing newline exactly.
fn rewrite_checksum_marker(content: &str, checksum: &str) -> String {
    let mut out = String::with_capacity(content.len());
    for segment in content.split_inclusive('\n') {
        let line = segment.trim_end_matches(['\r', '\n']);
        if line.starts_with(GENERATED_CHECKSUM_PREFIX) {
            let newline = &segment[line.len()..];
            out.push_str(GENERATED_CHECKSUM_PREFIX);
            out.push_str(checksum);
            out.push_str(newline);
        } else {
            out.push_str(segment);
        }
    }
    out
}

/// The Copilot / VS Code `.instructions.md` flavor of the core directive.
///
/// The body is byte-identical to [`MARROW_CORE_SKILL_MD`], but the frontmatter
/// uses VS Code's `applyTo` glob (which is what actually gates auto-application
/// of an instructions file) instead of Cursor's `.mdc`-only `alwaysApply` key.
/// `applyTo: "**"` is the direct translation of `alwaysApply: true` — apply the
/// directive to every file, regardless of language, so it is never silently
/// dropped for `.rb`, `.lua`, or any other extension. The generated checksum
/// marker is recomputed so the file is still recognised as managed on refresh.
fn marrow_copilot_instructions_md() -> String {
    let swapped = MARROW_CORE_SKILL_MD.replacen("alwaysApply: true", "applyTo: \"**\"", 1);
    let checksum = generated_checksum(&swapped);
    rewrite_checksum_marker(&swapped, &checksum)
}

/// The directory-package `SKILL.md` flavor of the core directive.
///
/// Claude Code and Antigravity (IDE and the `agy` CLI) discover skills as
/// `<skills-root>/<skill-name>/SKILL.md` packages whose frontmatter is keyed
/// on `name` + `description`. The body is byte-identical to
/// [`MARROW_CORE_SKILL_MD`]; only Cursor's `.mdc`-specific `alwaysApply` key is
/// swapped for the `name` field the SKILL.md format expects. The generated
/// checksum marker is recomputed so the file is still recognised as managed on
/// refresh.
fn marrow_skill_package_md() -> String {
    let swapped =
        MARROW_CORE_SKILL_MD.replacen("alwaysApply: true", "name: marrow-optimization", 1);
    let checksum = generated_checksum(&swapped);
    rewrite_checksum_marker(&swapped, &checksum)
}

/// Classify an existing file's content against a specific managed template.
/// `classify_existing_content` is the common case keyed on the core template.
fn classify_against(content: &str, current_template: &str) -> ExistingContentStatus {
    if content == current_template {
        return ExistingContentStatus::CurrentManaged;
    }

    if content == generated_content_for_checksum(current_template)
        || content == MARROW_CORE_SKILL_MD
        || content == generated_content_for_checksum(MARROW_CORE_SKILL_MD)
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

fn classify_existing_content(content: &str) -> ExistingContentStatus {
    classify_against(content, MARROW_CORE_SKILL_MD)
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

    // Copilot/VS Code instruction files need their own `applyTo` frontmatter, so
    // they are always written as standalone files rather than symlinked to the
    // shared (Cursor-flavored) central source.
    if matches!(agent, Agent::GitHubCopilot) {
        return install_managed_file(&target, &marrow_copilot_instructions_md());
    }

    // The Antigravity CLI's global skill is a directory-based SKILL.md package
    // with its own `name` frontmatter, so it is written standalone like the
    // Copilot file. Its project target is the shared universal `.agents/skills`
    // file and takes the normal central-source path below.
    if matches!(agent, Agent::AntigravityCli) && matches!(scope, Scope::Global) {
        return install_managed_file(&target, &marrow_skill_package_md());
    }

    // Claude Code's skill is a directory-based SKILL.md package at both scopes,
    // also written standalone. Older Marrow versions wrote a flat
    // `marrow-optimization.md` that Claude Code never loaded; once the package
    // is in place, a managed copy of that flat file is removed.
    if matches!(agent, Agent::ClaudeCode) {
        let mut status = install_managed_file(&target, &marrow_skill_package_md())?;
        let legacy = match scope {
            Scope::Project => PathBuf::from(".claude/skills/marrow-optimization.md"),
            Scope::Global => home.join(".claude/skills/marrow-optimization.md"),
        };
        if remove_superseded_managed_file(&legacy)? && status == InstallStatus::PreservedExisting {
            status = InstallStatus::Refreshed;
        }
        return Ok(status);
    }

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

/// Remove a superseded legacy file if it is Marrow-managed: known template
/// content (read through a symlink, if any) or a dangling symlink. User-authored
/// content is preserved. Returns whether anything was removed.
fn remove_superseded_managed_file(path: &Path) -> Result<bool> {
    if path.symlink_metadata().is_err() {
        return Ok(false);
    }

    // A dangling symlink resolves to nothing — clear it.
    if !path.exists() {
        fs::remove_file(path)?;
        return Ok(true);
    }

    let Ok(existing) = fs::read_to_string(path) else {
        return Ok(false);
    };
    if classify_existing_content(&existing) == ExistingContentStatus::Custom {
        return Ok(false);
    }

    fs::remove_file(path)?;
    Ok(true)
}

/// Write a self-contained managed file holding exactly `content`.
///
/// Unlike [`install`], this never symlinks: the file must carry its own
/// frontmatter (Copilot's `applyTo`), which a shared central source cannot
/// provide. A user-authored file is preserved; a managed file (including a
/// symlink to the shared central, the source of the Accrualify bug) is
/// replaced in place with a standalone `content` file.
fn install_managed_file(target: &Path, content: &str) -> Result<InstallStatus> {
    let link_meta = target.symlink_metadata();

    if let Ok(meta) = &link_meta {
        let is_symlink = meta.file_type().is_symlink();

        // A dangling symlink resolves to nothing — clear it and write fresh.
        if is_symlink && !target.exists() {
            fs::remove_file(target)?;
        } else {
            // Read through the link (if any) to classify the effective content.
            let existing = fs::read_to_string(target).unwrap_or_default();
            return match classify_against(&existing, content) {
                ExistingContentStatus::CurrentManaged if !is_symlink => {
                    Ok(InstallStatus::PreservedExisting)
                }
                ExistingContentStatus::Custom => Ok(InstallStatus::PreservedExisting),
                // Managed content (or a symlink, even to current content): replace
                // with a standalone file so the frontmatter is correct and owned here.
                _ => {
                    if is_symlink {
                        fs::remove_file(target)?;
                    }
                    fs::write(target, content)?;
                    Ok(InstallStatus::Refreshed)
                }
            };
        }
    }

    if let Some(parent) = target.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(target, content)?;
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
    fn copilot_instructions_template_uses_applyto_not_alwaysapply() {
        let md = super::marrow_copilot_instructions_md();
        // VS Code / Copilot `.instructions.md` files key auto-application on
        // `applyTo` (a glob), not Cursor's `.mdc` `alwaysApply` field.
        assert!(
            md.contains("applyTo: \"**\""),
            "copilot instructions must declare applyTo: \"**\" so the directive always applies"
        );
        assert!(
            !md.contains("alwaysApply"),
            "copilot instructions must not carry Cursor's alwaysApply key"
        );
    }

    #[test]
    fn copilot_instructions_template_is_self_consistent_managed() {
        let md = super::marrow_copilot_instructions_md();
        assert!(
            super::has_valid_generated_marker(&md),
            "rendered copilot template must carry a valid generated checksum marker"
        );
        assert_eq!(
            super::classify_against(&md, &md),
            super::ExistingContentStatus::CurrentManaged,
        );
    }

    #[test]
    fn copilot_instructions_share_core_directive_body() {
        // Body (everything after the frontmatter block) must be byte-identical to
        // the core directive so guidance never drifts between agents.
        let core_body = MARROW_CORE_SKILL_MD.splitn(3, "---\n").nth(2).unwrap();
        let copilot = super::marrow_copilot_instructions_md();
        let copilot_body = copilot.splitn(3, "---\n").nth(2).unwrap();
        assert_eq!(copilot_body, core_body);
    }

    #[test]
    fn install_skill_writes_global_copilot_applyto_instructions() {
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
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_copilot_instructions_md()
        );
    }

    #[test]
    fn install_skill_preserves_user_authored_copilot_instructions() {
        // Complement, never overwrite: a user's own instruction file is left intact.
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        let target = Agent::GitHubCopilot.target_path(Scope::Project, &home);
        let target = home.join(target);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        let custom = "---\napplyTo: \"**\"\n---\n# Team rules\nAsk before indexing.\n";
        fs::write(&target, custom).unwrap();

        // Project scope writes relative to CWD, so drive install through the lower
        // level by pointing target_path at the temp home via Global semantics.
        let status =
            super::install_managed_file(&target, &super::marrow_copilot_instructions_md()).unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), custom);
    }

    #[test]
    fn install_skill_converts_legacy_core_symlink_to_standalone_applyto_file() {
        // Reproduces the Accrualify global-Copilot bug: the prompts file was a
        // symlink to the shared (Cursor-flavored) central, so it carried
        // `alwaysApply` and never auto-applied in Copilot. Install must replace
        // it with a real standalone file using `applyTo`.
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::GitHubCopilot.target_path(Scope::Global, &home);
        fs::create_dir_all(target.parent().unwrap()).unwrap();

        let central = home.join(".marrow/marrow-optimization.md");
        fs::create_dir_all(central.parent().unwrap()).unwrap();
        fs::write(&central, MARROW_CORE_SKILL_MD).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(&central, &target).unwrap();
        #[cfg(not(unix))]
        fs::copy(&central, &target).unwrap();

        let status =
            super::install_skill(Agent::GitHubCopilot, Scope::Global, Method::Symlink, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        // The target is now a regular file, not a symlink.
        assert!(!target.symlink_metadata().unwrap().file_type().is_symlink());
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_copilot_instructions_md()
        );
        // The shared central file is untouched (still core, for Cursor et al.).
        assert_eq!(fs::read_to_string(&central).unwrap(), MARROW_CORE_SKILL_MD);
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
    fn copilot_global_skill_uses_vscode_user_prompts_instruction_file() {
        let home = Path::new("/tmp/home");
        let path = Agent::GitHubCopilot.target_path(Scope::Global, home);

        assert_eq!(
            path,
            super::vscode_user_dir(home).join("prompts/marrow-optimization.instructions.md")
        );
    }

    #[test]
    fn vscode_user_dir_is_platform_specific() {
        let dir = super::vscode_user_dir(Path::new("/tmp/home"));
        #[cfg(target_os = "macos")]
        assert_eq!(
            dir,
            Path::new("/tmp/home/Library/Application Support/Code/User")
        );
        #[cfg(target_os = "windows")]
        assert_eq!(dir, Path::new("/tmp/home/AppData/Roaming/Code/User"));
        #[cfg(not(any(target_os = "macos", target_os = "windows")))]
        assert_eq!(dir, Path::new("/tmp/home/.config/Code/User"));
    }

    #[test]
    fn skill_package_md_shares_core_directive_body() {
        // Body (everything after the frontmatter block) must be byte-identical to
        // the core directive so guidance never drifts between agents.
        let core_body = MARROW_CORE_SKILL_MD.splitn(3, "---\n").nth(2).unwrap();
        let package = super::marrow_skill_package_md();
        let package_body = package.splitn(3, "---\n").nth(2).unwrap();
        assert_eq!(package_body, core_body);
        assert!(package.contains("name: marrow-optimization"));
        assert!(!package.contains("alwaysApply"));
        assert!(super::has_valid_generated_marker(&package));
    }

    #[test]
    fn install_skill_writes_global_antigravity_cli_skill_md_package() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::AntigravityCli.target_path(Scope::Global, &home);

        let status = super::install_skill(
            Agent::AntigravityCli,
            Scope::Global,
            Method::WriteFile,
            &home,
        )
        .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_skill_package_md()
        );
    }

    #[test]
    fn claude_code_skill_uses_skill_md_package_at_both_scopes() {
        assert_eq!(
            Agent::ClaudeCode.target_path(Scope::Project, Path::new("/tmp/home")),
            Path::new(".claude/skills/marrow-optimization/SKILL.md")
        );
        assert_eq!(
            Agent::ClaudeCode.target_path(Scope::Global, Path::new("/tmp/home")),
            Path::new("/tmp/home/.claude/skills/marrow-optimization/SKILL.md")
        );
    }

    #[test]
    fn install_skill_writes_global_claude_code_skill_md_package() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        fs::create_dir_all(&home).unwrap();
        let target = Agent::ClaudeCode.target_path(Scope::Global, &home);

        let status =
            super::install_skill(Agent::ClaudeCode, Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_skill_package_md()
        );
    }

    #[test]
    fn install_skill_claude_code_removes_superseded_managed_flat_file() {
        // Older Marrow wrote `.claude/skills/marrow-optimization.md`, which Claude
        // Code never loaded. Installing the SKILL.md package must clean it up.
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        let legacy = home.join(".claude/skills/marrow-optimization.md");
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(&legacy, MARROW_CORE_SKILL_MD).unwrap();

        let status =
            super::install_skill(Agent::ClaudeCode, Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert!(
            !legacy.exists(),
            "managed legacy flat file should be removed"
        );
        let target = Agent::ClaudeCode.target_path(Scope::Global, &home);
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_skill_package_md()
        );
    }

    #[test]
    fn install_skill_claude_code_reports_refreshed_when_only_legacy_file_changes() {
        // Package already current; the only on-disk change is removing the
        // superseded flat file — surface that as Refreshed, not PreservedExisting.
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        let target = Agent::ClaudeCode.target_path(Scope::Global, &home);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, super::marrow_skill_package_md()).unwrap();
        let legacy = home.join(".claude/skills/marrow-optimization.md");
        fs::write(&legacy, LEGACY_STALE_MARROW_CORE_SKILL_MD).unwrap();

        let status =
            super::install_skill(Agent::ClaudeCode, Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Refreshed);
        assert!(!legacy.exists());
    }

    #[test]
    fn install_skill_claude_code_preserves_custom_flat_file() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        let legacy = home.join(".claude/skills/marrow-optimization.md");
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        let custom = "# My own Marrow notes\nAsk before indexing.\n";
        fs::write(&legacy, custom).unwrap();

        let status =
            super::install_skill(Agent::ClaudeCode, Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::Written);
        assert_eq!(fs::read_to_string(&legacy).unwrap(), custom);
    }

    #[test]
    fn install_skill_claude_code_preserves_user_authored_skill_md() {
        let tmp = tempdir().unwrap();
        let home = tmp.path().join("fake_home");
        let target = Agent::ClaudeCode.target_path(Scope::Global, &home);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        let custom = "---\nname: marrow-optimization\n---\n# Team rules\n";
        fs::write(&target, custom).unwrap();

        let status =
            super::install_skill(Agent::ClaudeCode, Scope::Global, Method::WriteFile, &home)
                .unwrap();

        assert_eq!(status, InstallStatus::PreservedExisting);
        assert_eq!(fs::read_to_string(&target).unwrap(), custom);
    }

    #[test]
    fn antigravity_cli_project_skill_uses_universal_skills_dir() {
        let path = Agent::AntigravityCli.target_path(Scope::Project, Path::new("/tmp/home"));

        assert_eq!(path, Path::new(".agents/skills/marrow-optimization.md"));
    }

    #[test]
    fn antigravity_cli_global_skill_uses_shared_gemini_skill_package() {
        let path = Agent::AntigravityCli.target_path(Scope::Global, Path::new("/tmp/home"));

        assert_eq!(
            path,
            Path::new("/tmp/home/.gemini/skills/marrow-optimization/SKILL.md")
        );
    }

    #[test]
    fn windsurf_project_skill_uses_windsurf_rules_directory() {
        let path = Agent::Windsurf.target_path(Scope::Project, Path::new("/tmp/home"));

        assert_eq!(path, Path::new(".windsurf/rules/marrow-optimization.md"));
    }

    #[test]
    fn roo_project_skill_uses_bare_roorules_fallback_file() {
        let path = Agent::RooCode.target_path(Scope::Project, Path::new("/tmp/home"));

        // Bare .roorules (not .roo/rules/) so a Marrow-created directory never
        // shadows a user's existing .roorules file.
        assert_eq!(path, Path::new(".roorules"));
    }

    #[test]
    fn roo_global_skill_uses_roo_rules_directory() {
        let path = Agent::RooCode.target_path(Scope::Global, Path::new("/tmp/home"));

        assert_eq!(
            path,
            Path::new("/tmp/home/.roo/rules/marrow-optimization.md")
        );
        assert!(Agent::RooCode.supports_scope(Scope::Global));
    }

    #[test]
    fn cline_global_skill_uses_documents_cline_rules_directory() {
        let path = Agent::Cline.target_path(Scope::Global, Path::new("/tmp/home"));

        assert_eq!(
            path,
            Path::new("/tmp/home/Documents/Cline/Rules/marrow-optimization.md")
        );
    }

    #[test]
    fn agents_without_on_disk_global_rules_do_not_claim_global_support() {
        // Windsurf: shared global_rules.md; Cursor: in-app User Rules;
        // Zed: internal Rules Library database. None are Marrow-ownable files.
        assert!(!Agent::Windsurf.supports_scope(Scope::Global));
        assert!(!Agent::Cursor.supports_scope(Scope::Global));
        assert!(!Agent::Zed.supports_scope(Scope::Global));
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
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_copilot_instructions_md()
        );
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
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_copilot_instructions_md()
        );
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
        assert_eq!(
            fs::read_to_string(&target).unwrap(),
            super::marrow_copilot_instructions_md()
        );
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
