use marrow::activity::{ActivityKind, ActivityState, ActivityTracker};

#[test]
fn activity_tracker_lists_runtime_processes_and_completion() {
    let tracker = ActivityTracker::default();
    let mcp = tracker.start(
        ActivityKind::McpSession,
        Some("workspace-a".to_string()),
        "copilot".to_string(),
    );
    let watcher = tracker.start(
        ActivityKind::WatcherJob,
        Some("workspace-a".to_string()),
        "watch".to_string(),
    );
    let index = tracker.start(
        ActivityKind::IndexingJob,
        Some("workspace-a".to_string()),
        "index".to_string(),
    );
    let cleanup = tracker.start(
        ActivityKind::CleanupJob,
        Some("workspace-a".to_string()),
        "clear".to_string(),
    );
    let dashboard = tracker.start(ActivityKind::DashboardClient, None, "browser".to_string());
    let daemon = tracker.start(ActivityKind::DaemonJob, None, "startup".to_string());

    let active = tracker.list();
    assert_eq!(active.len(), 6);
    assert!(active
        .iter()
        .any(|row| row.id == mcp && row.kind == ActivityKind::McpSession));
    assert!(active
        .iter()
        .any(|row| row.id == watcher && row.kind == ActivityKind::WatcherJob));
    assert!(active
        .iter()
        .any(|row| row.id == index && row.kind == ActivityKind::IndexingJob));
    assert!(active
        .iter()
        .any(|row| row.id == cleanup && row.kind == ActivityKind::CleanupJob));
    assert!(active
        .iter()
        .any(|row| row.id == dashboard && row.kind == ActivityKind::DashboardClient));
    assert!(active
        .iter()
        .any(|row| row.id == daemon && row.kind == ActivityKind::DaemonJob));

    tracker.finish(&index, ActivityState::Completed, "done".to_string());
    let completed = tracker
        .list()
        .into_iter()
        .find(|row| row.id == index)
        .unwrap();
    assert_eq!(completed.state, ActivityState::Completed);
    assert!(completed.updated_at >= completed.started_at);
}

#[test]
fn activity_tracker_records_stable_ids_workspace_details_and_terminal_states() {
    let tracker = ActivityTracker::default();
    let mcp = tracker.start(
        ActivityKind::McpSession,
        Some("workspace-a".to_string()),
        "connected".to_string(),
    );
    let watcher = tracker.start(
        ActivityKind::WatcherJob,
        Some("workspace-a".to_string()),
        "watching".to_string(),
    );
    let daemon = tracker.start(ActivityKind::DaemonJob, None, "running".to_string());

    assert!(mcp.starts_with("mcp-"));
    assert!(watcher.starts_with("watcher-"));
    assert!(daemon.starts_with("daemon-"));

    tracker.finish(&mcp, ActivityState::Stopped, "disconnected".to_string());
    tracker.finish(&watcher, ActivityState::Error, "watch failed".to_string());
    tracker.finish(&daemon, ActivityState::Stale, "previous daemon".to_string());

    let records = tracker.list();
    let mcp_record = records.iter().find(|row| row.id == mcp).unwrap();
    assert_eq!(mcp_record.workspace_id.as_deref(), Some("workspace-a"));
    assert_eq!(mcp_record.state, ActivityState::Stopped);
    assert_eq!(mcp_record.detail, "disconnected");
    let watcher_record = records.iter().find(|row| row.id == watcher).unwrap();
    assert_eq!(watcher_record.state, ActivityState::Error);
    let daemon_record = records.iter().find(|row| row.id == daemon).unwrap();
    assert_eq!(daemon_record.state, ActivityState::Stale);
}
