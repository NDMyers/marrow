//! Daemon subcommand — long-running Axum server that owns all SQLite state.

pub mod routes;
pub mod pool;
#[allow(unused_imports)]
pub use routes::DaemonState;
#[allow(unused_imports)]
pub use pool::RepoPool;

use anyhow::Result;


/// Entry-point called from `main()` for `marrow daemon`.
/// NOTE: This is a Phase 1 stub — the complete implementation comes in Task 6.
pub async fn run() -> Result<()> {
    let (watcher_tx, _watcher_rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(64);
    let (dash_tx, _)              = tokio::sync::broadcast::channel::<crate::dashboard::DashboardEvent>(256);
    let state = routes::DaemonState::new(watcher_tx, dash_tx.clone());
    let _addr = routes::bind_address();

    #[cfg(unix)]
    {
        use tokio::net::UnixListener;
        let sock_path = crate::ipc::default_sock_path();
        let parent = sock_path.parent()
            .ok_or_else(|| anyhow::anyhow!("daemon socket path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        // Remove stale socket file from a previous (crashed) daemon.
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)?;
        eprintln!("[marrow daemon] listening on unix:{}", sock_path.display());
        axum::serve(listener, routes::build_router(state)).await?;
    }
    #[cfg(not(unix))]
    {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind(_addr).await?;
        eprintln!("[marrow daemon] listening on tcp:{_addr}");
        axum::serve(listener, routes::build_router(state)).await?;
    }
    Ok(())
}
