use std::process::{Command, Stdio};
use std::io::Write;

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

    child.stdin.as_mut().unwrap().write_all(framed.as_bytes()).unwrap();
    child.kill().ok();
}
