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

/// Dashboard TCP port.
const DASHBOARD_PORT: u16 = 8765;

/// Entry-point called from `main()` for `marrow daemon`.
pub async fn run() -> Result<()> {
    // Create channels before DaemonState so the receiver stays in scope here.
    let (watcher_tx, mut watcher_rx) = tokio::sync::mpsc::channel::<std::path::PathBuf>(64);
    let (dash_tx, _) = tokio::sync::broadcast::channel::<crate::dashboard::DashboardEvent>(256);

    // Oneshot for graceful shutdown — sent by POST /api/shutdown.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    // Build shared state (starts eviction loop internally).
    let state = routes::DaemonState::new(
        watcher_tx,
        dash_tx.clone(),
        Arc::new(std::sync::Mutex::new(Some(shutdown_tx))),
    );

    // Spawn the global file watcher dispatcher.
    //
    // When a new workspace path arrives on `watcher_rx`, we:
    //   1. Open (or reuse) the pool connection for that repo.
    //   2. Call `spawn_watcher`, which is typed exactly as:
    //          fn spawn_watcher(db: Arc<std::sync::Mutex<Connection>>, ...) -> Result<JoinHandle>
    //      Pool::get_or_open returns the same Arc<std::sync::Mutex<Connection>> — no conversion.
    // M-11 FIX: Track watched roots to prevent duplicate watcher spawns.
    let watched_repos: Arc<std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>> =
        Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));

    let pool_for_watcher = Arc::clone(&state.pool);
    let dash_tx_watcher = dash_tx.clone();
    tokio::spawn(async move {
        while let Some(new_path) = watcher_rx.recv().await {
            // M-11 FIX: Canonicalize and deduplicate by canonical repo root.
            let canonical = new_path.canonicalize().unwrap_or_else(|_| new_path.clone());
            {
                let mut watched = watched_repos.lock().unwrap_or_else(|e| e.into_inner());
                if watched.contains(&canonical) {
                    continue; // Already watching this repo root.
                }
                watched.insert(canonical.clone());
            }
            match pool_for_watcher.get_or_open(&canonical).await {
                Ok(conn) => {
                    if let Err(e) =
                        crate::watcher::spawn_watcher(conn, dash_tx_watcher.clone(), DEBOUNCE_MS)
                    {
                        eprintln!(
                            "[marrow daemon] watcher error for {}: {e}",
                            canonical.display()
                        );
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[marrow daemon] could not open pool for {}: {e}",
                        canonical.display()
                    );
                }
            }
        }
    });

    // ── Dashboard TCP listener on port 8765 ───────────────────────────
    // The daemon now hosts the dashboard routes on a second listener so that
    // the desktop app and browsers can access it at http://127.0.0.1:8765.
    let dashboard_addr = std::net::SocketAddr::from(([127, 0, 0, 1], DASHBOARD_PORT));
    let dashboard_listener = tokio::net::TcpListener::bind(dashboard_addr).await
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::AddrInUse {
                anyhow::anyhow!(
                    "Port {} is already in use. Another process may be bound to it. \
                     The daemon requires exclusive access to this port for the dashboard.",
                    DASHBOARD_PORT
                )
            } else {
                anyhow::anyhow!("Failed to bind dashboard listener on port {}: {e}", DASHBOARD_PORT)
            }
        })?;

    // Build the dashboard AppState — uses an in-memory DB connection for stats
    // since the daemon's actual repos are managed through the pool.
    let db_path = std::env::var("MARROW_DB_PATH")
        .unwrap_or_else(|_| ".marrow/graph.db".to_string());
    let dashboard_db = crate::db::init_db_or_memory(&db_path)?;
    let dashboard_session = Arc::new(std::sync::Mutex::new(
        crate::dashboard::SessionStats::default(),
    ));
    let dashboard_app_state = crate::dashboard::AppState {
        tx: dash_tx.clone(),
        session: dashboard_session,
        db: Arc::new(std::sync::Mutex::new(dashboard_db)),
    };

    let dashboard_router = routes::build_dashboard_router(state.clone(), dashboard_app_state);
    tokio::spawn(async move {
        if let Err(e) = axum::serve(dashboard_listener, dashboard_router).await {
            eprintln!("[marrow daemon] dashboard server error: {e}");
        }
    });
    eprintln!("[marrow daemon] dashboard → http://127.0.0.1:{DASHBOARD_PORT}");

    // Bind and serve IPC with graceful shutdown wired in.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        use tokio::net::UnixListener;
        let sock_path = crate::ipc::default_sock_path();
        let parent = sock_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("daemon socket path has no parent directory"))?;
        std::fs::create_dir_all(parent)?;
        // Harden socket directory permissions to 0700 (owner-only access).
        std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
        // Remove stale socket from a previous (crashed) daemon.
        let _ = std::fs::remove_file(&sock_path);
        let listener = UnixListener::bind(&sock_path)?;
        // Harden socket file permissions to 0600 (owner read/write only).
        std::fs::set_permissions(&sock_path, std::fs::Permissions::from_mode(0o600))?;
        eprintln!("[marrow daemon] listening on unix:{}", sock_path.display());
        axum::serve(listener, routes::build_router(state))
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await?;
    }
    #[cfg(not(unix))]
    {
        use tokio::net::TcpListener;
        let addr = routes::bind_address();
        let listener = TcpListener::bind(addr).await?;
        eprintln!("[marrow daemon] listening on tcp:{addr}");
        axum::serve(listener, routes::build_router(state))
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await?;
    }
    Ok(())
}
