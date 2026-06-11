use std::{fs, path::PathBuf, process::Command};

use marrow::{context, db, ingestion};

/// Command for the compiled binary with the workspace registry redirected to
/// a scratch path so test runs never pollute the user's real ~/.marrow
/// registry (HOME overrides don't redirect dirs::home_dir() on Windows).
fn marrow_cmd() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_marrow"));
    command.env(
        "MARROW_REGISTRY_PATH",
        PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("context_packet-registry.db"),
    );
    command
}

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/context_packet_repo")
}

fn indexed_fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let db_path = temp.path().join("graph.db");
    let conn = db::init_db(db_path.to_str().unwrap()).unwrap();
    ingestion::run_ingestion(&conn, "fixture", &fixture_root()).unwrap();
    drop(conn);
    (temp, db_path)
}

fn compile_fixture(
    task: &str,
    budget: usize,
    profile: context::ModelProfile,
) -> context::ContextPacket {
    compile_fixture_with_format(task, budget, profile, context::ContextFormat::Markdown)
}

fn compile_fixture_with_format(
    task: &str,
    budget: usize,
    profile: context::ModelProfile,
    format: context::ContextFormat,
) -> context::ContextPacket {
    let (_temp, db_path) = indexed_fixture();
    let conn = db::init_db(db_path.to_str().unwrap()).unwrap();
    context::compile_context_packet_for_format(
        &conn,
        context::ContextRequest {
            task: task.to_string(),
            repo_id: "fixture".to_string(),
            budget_tokens: budget,
            profile,
        },
        format,
    )
    .unwrap()
}

#[test]
fn markdown_output_is_deterministic_and_contains_packet_sections() {
    let packet = compile_fixture(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
    );
    let first = packet.to_markdown();
    let second = packet.to_markdown();

    assert_eq!(first, second);
    assert!(first.contains("# Marrow Context Packet"), "{first}");
    assert!(first.contains("Task: trace request flow"), "{first}");
    assert!(first.contains("Routing:"), "{first}");
    assert!(first.contains("Ranked Context"), "{first}");
    assert!(first.contains("Token Accounting"), "{first}");
    assert!(first.contains("Freshness"), "{first}");
    assert!(first.contains("Provenance"), "{first}");
}

#[test]
fn json_output_has_stable_packet_shape() {
    let packet = compile_fixture(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
    );
    let json = packet.to_json().unwrap();
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();

    assert_eq!(value["task"], "trace request flow");
    assert_eq!(value["repo_id"], "fixture");
    assert_eq!(value["budget"]["requested_tokens"], 12_000);
    assert_eq!(value["profile"]["name"], "local-32k");
    assert!(value["routing"]["outcome"].is_string());
    assert!(value["token_accounting"]["estimated_packet_tokens"].is_number());
    assert!(value["freshness"]["index_status"].is_string());
    assert!(value["ranked_entries"].as_array().unwrap().len() >= 2);
    assert!(value["provenance"]["compiler"]
        .as_str()
        .unwrap()
        .contains("marrow context"));
}

#[test]
fn exact_entries_include_source_spans_and_condensed_entries_stay_distinct() {
    let packet = compile_fixture(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
    );
    let exact = packet
        .ranked_entries
        .iter()
        .find(|entry| entry.context_type == context::ContextEntryType::ExactSource)
        .expect("packet should include exact source");

    assert_eq!(exact.file_path, "src/request.py");
    assert_eq!(exact.symbol_name, "handle_request");
    assert_eq!(exact.symbol_type, "function");
    assert!(exact.span.as_ref().unwrap().start_byte < exact.span.as_ref().unwrap().end_byte);
    assert_eq!(exact.span.as_ref().unwrap().start_line, 1);
    assert_eq!(exact.span.as_ref().unwrap().start_column, 1);
    assert!(exact
        .source_text
        .as_ref()
        .unwrap()
        .contains("def handle_request"));

    let condensed = packet
        .ranked_entries
        .iter()
        .find(|entry| entry.context_type == context::ContextEntryType::CondensedStructure)
        .expect("packet should include condensed structural neighbors");
    assert!(condensed.source_text.is_none());
    assert!(condensed.condensed_text.as_ref().unwrap().contains("pass"));
}

#[test]
fn routing_covers_missing_broad_and_targeted_tasks() {
    let conn = db::init_db(":memory:").unwrap();
    let missing = context::compile_context_packet(
        &conn,
        context::ContextRequest {
            task: "trace request flow".to_string(),
            repo_id: "missing".to_string(),
            budget_tokens: 12_000,
            profile: context::ModelProfile::Local32k,
        },
    )
    .unwrap();
    assert_eq!(missing.routing.outcome, context::RoutingOutcome::NeedsIndex);
    assert!(missing.ranked_entries.is_empty());

    let broad = compile_fixture(
        "understand the whole codebase",
        12_000,
        context::ModelProfile::Local32k,
    );
    assert!(matches!(
        broad.routing.outcome,
        context::RoutingOutcome::UseNative | context::RoutingOutcome::Hybrid
    ));

    let targeted = compile_fixture(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
    );
    assert!(matches!(
        targeted.routing.outcome,
        context::RoutingOutcome::UseMarrow | context::RoutingOutcome::Hybrid
    ));
}

#[test]
fn model_profiles_and_budget_truncation_are_deterministic() {
    let local_8k = compile_fixture("trace request flow", 12_000, context::ModelProfile::Local8k);
    let local_32k = compile_fixture(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
    );
    assert_ne!(
        local_8k.budget.effective_tokens,
        local_32k.budget.effective_tokens
    );
    assert!(local_8k.ranked_entries.len() <= local_32k.ranked_entries.len());

    let tiny = compile_fixture("trace request flow", 20, context::ModelProfile::Local32k);
    assert!(tiny.ranked_entries.is_empty());
    assert!(tiny.provenance.truncated);
    assert!(tiny
        .provenance
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("budget")));
}

#[test]
fn changed_source_after_index_is_reported_as_stale() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    fs::create_dir_all(repo.join("src")).unwrap();
    let source = repo.join("src/request.py");
    fs::write(
        &source,
        "def handle_request(request):\n    return build_response(request)\n\ndef build_response(request):\n    return request\n",
    )
    .unwrap();

    let db_path = temp.path().join("graph.db");
    let conn = db::init_db(db_path.to_str().unwrap()).unwrap();
    ingestion::run_ingestion(&conn, "stale", &repo).unwrap();
    fs::write(
        &source,
        "def handle_request(request):\n    return {\"changed\": request}\n",
    )
    .unwrap();

    let packet = context::compile_context_packet(
        &conn,
        context::ContextRequest {
            task: "trace request flow".to_string(),
            repo_id: "stale".to_string(),
            budget_tokens: 12_000,
            profile: context::ModelProfile::Local32k,
        },
    )
    .unwrap();

    assert_eq!(packet.freshness.index_status, "stale");
    assert!(packet
        .freshness
        .notes
        .iter()
        .any(|note| note.contains("src/request.py")));
    assert!(packet
        .ranked_entries
        .iter()
        .any(|entry| entry.provenance.freshness == "stale"));
}

#[test]
fn unavailable_source_is_not_emitted_as_exact_source() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    fs::create_dir_all(repo.join("src")).unwrap();
    let source = repo.join("src/request.py");
    fs::write(
        &source,
        "def handle_request(request):\n    return build_response(request)\n\ndef build_response(request):\n    return request\n",
    )
    .unwrap();

    let db_path = temp.path().join("graph.db");
    let conn = db::init_db(db_path.to_str().unwrap()).unwrap();
    ingestion::run_ingestion(&conn, "non_utf8", &repo).unwrap();
    fs::write(&source, [0xff, 0xfe, 0xfd]).unwrap();

    let packet = context::compile_context_packet(
        &conn,
        context::ContextRequest {
            task: "trace request flow".to_string(),
            repo_id: "non_utf8".to_string(),
            budget_tokens: 12_000,
            profile: context::ModelProfile::Local32k,
        },
    )
    .unwrap();

    assert_eq!(packet.freshness.index_status, "unavailable");
    assert_eq!(packet.freshness.unavailable_file_count, 1);
    assert!(packet
        .freshness
        .notes
        .iter()
        .any(|note| note.contains("non-utf8 source")));

    let unavailable_entries = packet
        .ranked_entries
        .iter()
        .filter(|entry| entry.file_path == "src/request.py")
        .collect::<Vec<_>>();
    assert!(!unavailable_entries.is_empty());
    assert!(unavailable_entries
        .iter()
        .all(|entry| entry.context_type != context::ContextEntryType::ExactSource));
    assert!(unavailable_entries
        .iter()
        .all(|entry| entry.source_text.is_none() && entry.condensed_text.is_none()));
    assert!(unavailable_entries.iter().any(|entry| entry
        .provenance
        .rationale
        .iter()
        .any(|reason| reason.contains("provenance error"))));
}

#[test]
fn json_packet_accounting_uses_emitted_json_and_reports_budget_truncation() {
    let packet = compile_fixture_with_format(
        "trace request flow",
        12_000,
        context::ModelProfile::Local32k,
        context::ContextFormat::Json,
    );
    let json = packet.to_json().unwrap();

    assert_eq!(
        packet.token_accounting.estimated_packet_tokens,
        json.len().div_ceil(4)
    );
    assert!(packet.token_accounting.token_source.contains("json"));

    let tiny = compile_fixture_with_format(
        "trace request flow",
        300,
        context::ModelProfile::Local32k,
        context::ContextFormat::Json,
    );
    let tiny_json = tiny.to_json().unwrap();

    assert_eq!(
        tiny.token_accounting.estimated_packet_tokens,
        tiny_json.len().div_ceil(4)
    );
    assert!(tiny.provenance.truncated);
    assert!(
        tiny.token_accounting.estimated_packet_tokens <= tiny.budget.effective_tokens
            || tiny.ranked_entries.is_empty()
    );
    assert!(tiny
        .provenance
        .truncation_reasons
        .iter()
        .any(|reason| reason.contains("json packet")));
}

#[test]
fn cli_emits_markdown_and_json_packets() {
    let (_temp, db_path) = indexed_fixture();
    let markdown = marrow_cmd()
        .env("MARROW_DB_PATH", &db_path)
        .args([
            "context",
            "trace request flow",
            "--repo",
            "fixture",
            "--budget",
            "12000",
            "--format",
            "markdown",
            "--profile",
            "local-32k",
        ])
        .output()
        .unwrap();
    assert!(
        markdown.status.success(),
        "{}",
        String::from_utf8_lossy(&markdown.stderr)
    );
    let stdout = String::from_utf8(markdown.stdout).unwrap();
    assert!(stdout.contains("# Marrow Context Packet"), "{stdout}");

    let json = marrow_cmd()
        .env("MARROW_DB_PATH", &db_path)
        .args([
            "context",
            "trace request flow",
            "--repo",
            "fixture",
            "--budget",
            "12000",
            "--format",
            "json",
            "--profile",
            "local-32k",
        ])
        .output()
        .unwrap();
    assert!(
        json.status.success(),
        "{}",
        String::from_utf8_lossy(&json.stderr)
    );
    let json_stdout = String::from_utf8(json.stdout).unwrap();
    let value: serde_json::Value = serde_json::from_str(&json_stdout).unwrap();
    assert_eq!(value["routing"]["outcome"], "use_marrow");
    assert_eq!(
        value["token_accounting"]["estimated_packet_tokens"]
            .as_u64()
            .unwrap() as usize,
        json_stdout.trim_end_matches('\n').len().div_ceil(4)
    );
    assert!(value["token_accounting"]["token_source"]
        .as_str()
        .unwrap()
        .contains("json"));
}
