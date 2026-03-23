use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Uninitialized,
    #[allow(dead_code)]
    Indexing { progress_percent: u8 },
    Ready,
}

pub static INDEX_STATUS: RwLock<IndexState> = RwLock::new(IndexState::Uninitialized);

pub fn read_index_state() -> IndexState {
    match INDEX_STATUS.read() {
        Ok(state) => *state,
        Err(poisoned) => *poisoned.into_inner(),
    }
}

pub fn set_index_state(next: IndexState) {
    match INDEX_STATUS.write() {
        Ok(mut state) => *state = next,
        Err(poisoned) => {
            *poisoned.into_inner() = next;
        }
    }
}

#[allow(dead_code)]
pub fn update_index_progress(progress_percent: u8) {
    set_index_state(IndexState::Indexing {
        progress_percent: progress_percent.min(100),
    });
}

pub fn run_pipeline_wait_message(progress_percent: u8) -> String {
    format!(
        "SYSTEM EVENT: Marrow is currently building the AST index in the background (Progress: {progress_percent}%). You cannot query the codebase yet. Please inform the user that you are waiting for the index to complete, and autonomously call this tool again in 5 seconds."
    )
}

pub fn run_pipeline_guard_message() -> Option<String> {
    match read_index_state() {
        IndexState::Ready => None,
        IndexState::Indexing { progress_percent } => {
            Some(run_pipeline_wait_message(progress_percent))
        }
        IndexState::Uninitialized => Some(run_pipeline_wait_message(0)),
    }
}

#[cfg(test)]
mod tests {
    use super::{IndexState, run_pipeline_guard_message, run_pipeline_wait_message, set_index_state};

    #[test]
    fn wait_message_includes_progress_and_retry_instruction() {
        let message = run_pipeline_wait_message(42);
        assert!(message.contains("Progress: 42%"), "progress missing: {message}");
        assert!(message.contains("autonomously call this tool again in 5 seconds"), "retry guidance missing: {message}");
    }

    #[test]
    fn guard_message_is_absent_only_when_index_ready() {
        set_index_state(IndexState::Indexing { progress_percent: 7 });
        assert!(run_pipeline_guard_message().is_some(), "indexing state should block queries");

        set_index_state(IndexState::Ready);
        assert!(run_pipeline_guard_message().is_none(), "ready state should allow queries");
    }
}
