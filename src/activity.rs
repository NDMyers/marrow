use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityKind {
    McpSession,
    DashboardClient,
    DaemonJob,
    WatcherJob,
    WatcherEvent,
    IndexingJob,
    CleanupJob,
}

impl ActivityKind {
    fn id_prefix(&self) -> &'static str {
        match self {
            Self::McpSession => "mcp",
            Self::DashboardClient => "dashboard",
            Self::DaemonJob => "daemon",
            Self::WatcherJob => "watcher",
            Self::WatcherEvent => "watch-event",
            Self::IndexingJob => "index",
            Self::CleanupJob => "cleanup",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityState {
    Active,
    Completed,
    Stale,
    Stopped,
    Error,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivityRecord {
    pub id: String,
    pub kind: ActivityKind,
    pub workspace_id: Option<String>,
    pub state: ActivityState,
    pub detail: String,
    pub started_at: u64,
    pub updated_at: u64,
}

#[derive(Clone, Default)]
pub struct ActivityTracker {
    next_id: Arc<AtomicU64>,
    records: Arc<Mutex<HashMap<String, ActivityRecord>>>,
}

impl ActivityTracker {
    pub fn start(
        &self,
        kind: ActivityKind,
        workspace_id: Option<String>,
        detail: String,
    ) -> String {
        let seq = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        let id = format!("{}-{seq}", kind.id_prefix());
        let now = now_secs();
        let record = ActivityRecord {
            id: id.clone(),
            kind,
            workspace_id,
            state: ActivityState::Active,
            detail,
            started_at: now,
            updated_at: now,
        };
        if let Ok(mut records) = self.records.lock() {
            records.insert(id.clone(), record);
        }
        id
    }

    pub fn update(&self, id: &str, state: ActivityState, detail: String) {
        if let Ok(mut records) = self.records.lock() {
            if let Some(record) = records.get_mut(id) {
                record.state = state;
                record.detail = detail;
                record.updated_at = now_secs();
            }
        }
    }

    pub fn finish(&self, id: &str, state: ActivityState, detail: String) {
        self.update(id, state, detail);
    }

    pub fn list(&self) -> Vec<ActivityRecord> {
        let mut records = self
            .records
            .lock()
            .map(|records| records.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        records.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.id.cmp(&b.id)));
        records
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
