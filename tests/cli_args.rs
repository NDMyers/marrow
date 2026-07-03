use std::{
    path::PathBuf,
    process::{Command, Stdio},
};

fn marrow_bin() -> &'static str {
    env!("CARGO_BIN_EXE_marrow")
}

/// Scratch registry path so spawned binaries never touch the user's real
/// ~/.marrow/registry.db (HOME overrides don't redirect dirs::home_dir() on
/// Windows).
fn scratch_registry_path() -> PathBuf {
    PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("cli-args-registry.db")
}

fn run_marrow(args: &[&str]) -> std::process::Output {
    Command::new(marrow_bin())
        .args(args)
        .env("MARROW_REGISTRY_PATH", scratch_registry_path())
        // Closed stdin: if a regression sends an arg into the stdio MCP
        // server again, the process exits on EOF instead of hanging the run.
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|err| panic!("run {args:?}: {err}"))
}

fn stdout(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn version_flags_print_version_and_exit() {
    for flag in ["--version", "-V", "version"] {
        let output = run_marrow(&[flag]);
        assert!(
            output.status.success(),
            "`marrow {flag}` should exit 0: {}",
            stderr(&output)
        );
        let out = stdout(&output);
        assert!(
            out.contains(env!("CARGO_PKG_VERSION")),
            "`marrow {flag}` should print the crate version, got: {out}"
        );
        assert!(
            !stderr(&output).contains("MCP server ready"),
            "`marrow {flag}` must not start the MCP server"
        );
    }
}

#[test]
fn unknown_command_errors_instead_of_starting_mcp_server() {
    let output = run_marrow(&["definitely-not-a-command"]);
    assert!(
        !output.status.success(),
        "unknown commands should exit non-zero, got: {}",
        stdout(&output)
    );
    let err = stderr(&output);
    assert!(
        err.contains("Unknown command 'definitely-not-a-command'"),
        "error should name the unknown command: {err}"
    );
    assert!(
        !err.contains("MCP server ready"),
        "unknown commands must not fall through to the MCP server: {err}"
    );
}

#[test]
fn doctor_reports_empty_workspace_cleanly() {
    let workspace = tempfile::tempdir().unwrap();
    let output = Command::new(marrow_bin())
        .args(["doctor"])
        .current_dir(workspace.path())
        .env("MARROW_REGISTRY_PATH", scratch_registry_path())
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "doctor on an empty workspace should exit 0: {}",
        stderr(&output)
    );
    assert!(
        stdout(&output).contains("no repos indexed"),
        "doctor should say there is nothing to check: {}",
        stdout(&output)
    );
}

#[test]
fn help_mentions_version_flag() {
    let output = run_marrow(&["--help"]);
    assert!(output.status.success());
    assert!(
        stdout(&output).contains("--version"),
        "help should document --version"
    );
}
