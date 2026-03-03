use std::{
    collections::VecDeque,
    convert::Infallible,
    fs,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    Router,
    extract::State,
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::{StreamExt as _, wrappers::BroadcastStream};
use tower_http::cors::CorsLayer;

static INDEX_HTML: &str = include_str!("index.html");

// ── Event types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DashboardEvent {
    ServerStarted {
        port: u16,
        db_path: String,
    },
    CapsuleServed {
        symbol: String,
        repo: String,
        file: String,
        capsule_tokens: usize,
        file_tokens: usize,
        tokens_saved: usize,
        ts: u64,
    },
    RepoIndexed {
        repo_id: String,
        symbols: usize,
        edges: usize,
        ts: u64,
    },
    ImpactAnalyzed {
        symbol: String,
        repo: String,
        affected_count: usize,
        ts: u64,
    },
}

pub fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ── Session stats (in-memory, per-process) ────────────────────────────────────

#[derive(Default)]
pub struct SessionStats {
    pub total_requests:       usize,
    pub total_capsule_tokens: usize,
    pub total_file_tokens:    usize,
    pub total_tokens_saved:   usize,
    pub recent_events:        VecDeque<DashboardEvent>,
}

impl SessionStats {
    pub fn record_capsule(
        &mut self,
        capsule_tokens: usize,
        file_tokens:    usize,
        event:          DashboardEvent,
    ) {
        self.total_requests       += 1;
        self.total_capsule_tokens += capsule_tokens;
        self.total_file_tokens    += file_tokens;
        self.total_tokens_saved   += file_tokens.saturating_sub(capsule_tokens);
        self.recent_events.push_front(event);
        if self.recent_events.len() > 50 {
            self.recent_events.pop_back();
        }
    }
}

// ── Axum shared state ─────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub tx:      broadcast::Sender<DashboardEvent>,
    pub session: Arc<Mutex<SessionStats>>,
    pub db:      Arc<Mutex<rusqlite::Connection>>,
}

// ── Route handlers ────────────────────────────────────────────────────────────

async fn index_handler() -> impl IntoResponse {
    axum::response::Html(INDEX_HTML)
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(|msg| async move {
        let event = msg.ok()?;
        let json  = serde_json::to_string(&event).ok()?;
        Some(Ok::<Event, Infallible>(Event::default().data(json)))
    });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Serialize)]
struct StatsResponse {
    session:  SessionSnapshot,
    lifetime: LifetimeSnapshot,
}

#[derive(Serialize)]
struct SessionSnapshot {
    total_requests:       usize,
    total_capsule_tokens: usize,
    total_file_tokens:    usize,
    total_tokens_saved:   usize,
    reduction_pct:        f64,
    recent_events:        Vec<DashboardEvent>,
}

#[derive(Serialize)]
struct LifetimeSnapshot {
    total_requests:     i64,
    total_tokens_saved: i64,
    total_file_tokens:  i64,
    reduction_pct:      f64,
}

async fn stats_handler(State(state): State<AppState>) -> axum::response::Response {
    let sess = match state.session.lock() {
        Ok(g)  => g,
        Err(_) => return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response(),
    };
    let reduction_pct = if sess.total_file_tokens == 0 {
        0.0
    } else {
        (sess.total_tokens_saved as f64 / sess.total_file_tokens as f64) * 100.0
    };
    let session = SessionSnapshot {
        total_requests:       sess.total_requests,
        total_capsule_tokens: sess.total_capsule_tokens,
        total_file_tokens:    sess.total_file_tokens,
        total_tokens_saved:   sess.total_tokens_saved,
        reduction_pct,
        recent_events: sess.recent_events.iter().cloned().collect(),
    };
    drop(sess);

    let lifetime = {
        let conn = match state.db.lock() {
            Ok(g)  => g,
            Err(_) => return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response(),
        };
        let req   = crate::db::read_stat(&conn, "total_requests");
        let saved = crate::db::read_stat(&conn, "total_tokens_saved");
        let file  = crate::db::read_stat(&conn, "total_file_tokens");
        let rpct  = if file == 0 { 0.0 } else { (saved as f64 / file as f64) * 100.0 };
        LifetimeSnapshot {
            total_requests:     req,
            total_tokens_saved: saved,
            total_file_tokens:  file,
            reduction_pct:      rpct,
        }
    };

    axum::Json(StatsResponse { session, lifetime }).into_response()
}

// ── Server startup ────────────────────────────────────────────────────────────

/// Spawns the Axum dashboard on the first available port in 8765–8775.
/// Writes the bound port to `.marrow/dashboard.port`.
/// Opens the browser if `open_browser` is true.
/// Returns the bound port.
pub async fn start(
    tx:           broadcast::Sender<DashboardEvent>,
    session:      Arc<Mutex<SessionStats>>,
    db:           Arc<Mutex<rusqlite::Connection>>,
    open_browser: bool,
) -> Result<u16> {
    let state = AppState { tx, session, db };

    let router = Router::new()
        .route("/",       get(index_handler))
        .route("/stream", get(sse_handler))
        .route("/stats",  get(stats_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let (listener, port) = {
        let mut result = None;
        for port in 8765u16..=8775 {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            if let Ok(l) = TcpListener::bind(addr).await {
                result = Some((l, port));
                break;
            }
        }
        result.ok_or_else(|| anyhow::anyhow!("No ports available in 8765–8775"))?
    };

    let _ = fs::create_dir_all(".marrow")
        .and_then(|_| fs::write(".marrow/dashboard.port", port.to_string()));

    let url = format!("http://localhost:{port}");

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("Marrow dashboard error: {e}");
        }
    });

    eprintln!("Marrow dashboard → {url}");

    if open_browser {
        if let Err(e) = open::that(&url) {
            eprintln!("Could not open browser: {e}");
        }
    }

    Ok(port)
}
