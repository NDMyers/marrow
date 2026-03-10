//! Daemon subcommand — long-running Axum server that owns all SQLite state.

pub mod routes;
#[allow(unused_imports)]
pub use routes::DaemonState;

use anyhow::Result;


/// Entry-point called from `main()` for `marrow daemon`.
/// NOTE: This is a Phase 1 stub — the complete implementation comes in Task 6.
pub async fn run() -> Result<()> {
    let state = routes::DaemonState::new()?;

    // Bind the socket / port
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
        let addr = routes::bind_address();
        let listener = TcpListener::bind(addr).await?;
        eprintln!("[marrow daemon] listening on tcp:{addr}");
        axum::serve(listener, routes::build_router(state)).await?;
    }
    Ok(())
}
