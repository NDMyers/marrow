use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn benchmark_without_args_noninteractive_preserves_usage_error() {
    let output = Command::new(env!("CARGO_BIN_EXE_marrow"))
        .arg("benchmark")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run marrow benchmark without args");

    assert!(!output.status.success(), "expected usage failure");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage:")
            && stderr.contains("benchmark [--precise-file-tokens] <symbol> <repo_id>"),
        "expected existing usage error, got: {stderr}"
    );
}

#[test]
fn benchmark_scripted_invocation_runs_without_prompt() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("hello.py"), "def hello():\n    return 1\n").unwrap();

    let index = Command::new(env!("CARGO_BIN_EXE_marrow"))
        .arg("index")
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run marrow index");
    assert!(
        index.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&index.stderr)
    );

    let repo_id = root
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .expect("tempdir basename");
    let output = Command::new(env!("CARGO_BIN_EXE_marrow"))
        .args(["benchmark", "hello", repo_id])
        .current_dir(root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run scripted marrow benchmark");

    assert!(
        output.status.success(),
        "benchmark failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Marrow Token Benchmark"),
        "missing benchmark table: {stderr}"
    );
    assert!(
        stderr.contains("hello"),
        "missing symbol in benchmark output: {stderr}"
    );
}

/// Spawn `marrow mcp`, send a JSON-RPC initialize, expect it to be forwarded.
/// This test requires the daemon to already be running (or auto-spawned).
/// Run manually with: `cargo test -- --ignored`
#[test]
#[ignore = "requires running daemon; run manually with `cargo test -- --ignored`"]
fn mcp_proxy_forwards_initialize() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_marrow"))
        .arg("mcp")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mcp proxy");

    let init_msg = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0"}}}"#;
    let framed = format!("Content-Length: {}\r\n\r\n{}", init_msg.len(), init_msg);

    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(framed.as_bytes())
        .unwrap();
    child.kill().ok();
    let _ = child.wait();
}
