//! Daemon subcommand — long-running Axum server that owns all SQLite state.

pub mod pool;
pub mod routes;

#[allow(unused_imports)]
pub use pool::RepoPool;
#[allow(unused_imports)]
pub use routes::DaemonState;

use anyhow::Result;
use std::sync::Arc;

/// Filesystem event debounce window required by the spec.
const DEBOUNCE_MS: u64 = 300;

/// Entry-point called from `main()` for `marrow daemon`.
pub async fn run() -> Result<()> {
    // Create channels before DaemonState so the receiver stays in scope here.
    let (watcher_tx, mut watcher_rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(64);
    let (dash_tx, _) =
        tokio::sync::broadcast::channel::<crate::dashboard::DashboardEvent>(256);

    // Build shared state (starts eviction loop internally).
    let state = routes::DaemonState::new(watcher_tx, dash_tx.clone());

    // Spawn the global file watcher dispatcher.
    //
    // When a new workspace path arrives on `watcher_rx`, we:
    //   1. Open (or reuse) the pool connection for that repo.
    //   2. Call `spawn_watcher`, which is typed exactly as:
    //          fn spawn_watcher(db: Arc<std::sync::Mutex<Connection>>, ...) -> Result<JoinHandle>
    //      Pool::get_or_open returns the same Arc<std::sync::Mutex<Connection>> — no conversion.
    let pool_for_watcher = Arc::clone(&state.pool);
    let dash_tx_watcher = dash_tx.clone();
    tokio::spawn(async move {
        while let Some(new_path) = watcher_rx.recv().await {
            match pool_for_watcher.get_or_open(&new_path).await {
                Ok(conn) => {
                    if let Err(e) = crate::watcher::spawn_watcher(
                        conn,
                        dash_tx_watcher.clone(),
                        DEBOUNCE_MS,
                    ) {
                        eprintln!(
                            "[marrow daemon] watcher error for {}: {e}",
                            new_path.display()
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[marrow daemon] could not open pool for {}: {e}",
                        new_path.display()
                    );
                }
            }
        }
    });

    // Bind and serve.
    #[cfg(unix)]
    {
        use tokio::net::UnixListener;
        let sock_path = crate::ipc::default_sock_path();
        let parent = sock_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("daemon socket path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        // Remove stale socket from a previous (crashed) daemon.
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)?;
        eprintln!("[marrow daemon] listening on unix:{}", sock_path.display());
        axum::serve(listener, routes::build_router(state)).await?;
    }
    #[cfg(not(unix))]
    {
        use tokio::net::TcpListener;
        let addr = routes::bind_address();
        let listener = TcpListener::bind(addr).await?;
        eprintln!("[marrow daemon] listening on tcp:{addr}");
        axum::serve(listener, routes::build_router(state)).await?;
    }
    Ok(())
}
