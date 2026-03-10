//! Daemon subcommand — long-running Axum server that owns all SQLite state.

pub mod routes;

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
        std::fs::create_dir_all(sock_path.parent().unwrap())?;
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
