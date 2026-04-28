use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    Router,
    extract::{Query, State},
    response::{
        IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::CorsLayer;

static INDEX_HTML: &str = include_str!("index.html");
static D3_JS: &str = include_str!("d3-v7.min.js");

// ── Event types ───────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
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
        origin: String,
        ts: u64,
        /// Full raw file text. Included in the telemetry POST so the Hub's
        /// compare endpoint can serve the delta modal even when running as a
        /// Spoke (different CWD / DB file). Skipped in SSE broadcasts to
        /// avoid bloating the event stream.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        original_text: Option<String>,
        /// Condensed capsule text. Same rationale as `original_text`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        optimized_text: Option<String>,
        /// Whether the original+optimized texts are cached on the Hub,
        /// enabling the client-side delta viewer button.
        #[serde(default)]
        has_cached_delta: bool,
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
    SkeletonGenerated {
        target_dir: String,
        node_count: usize,
        ts: u64,
    },
    FileReindexed {
        file_path: String,
        repo_id: String,
        symbols: usize,
        ts: u64,
    },
}

/// Result of the Hub election attempt.
#[derive(Debug)]
pub enum HubRole {
    /// This process bound 127.0.0.1:8765 and owns the Axum server.
    Hub,
    /// Port 8765 was already taken — running headless as a Spoke.
    Spoke,
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
    /// Token delta text cache keyed by `"symbol@repo@ts"`.
    /// Populated from the telemetry POST body so the compare endpoint works
    /// even when the Axum server (Hub) is running against a different DB than
    /// the process that served the capsule (Spoke).
    pub capsule_text_cache:   HashMap<String, (String, String)>,
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

        // Extract and cache texts before stripping them from the stored event.
        // The cache lets the compare endpoint serve delta views without re-querying
        // the DB — critical for Hub/Spoke scenarios where the Hub's DB is different.
        let slim_event = if let DashboardEvent::CapsuleServed {
            ref symbol,
            ref repo,
            ref original_text,
            ref optimized_text,
            ts,
            ..
        } = event {
            let mut cached = false;
            if let (Some(orig), Some(opt)) = (original_text, optimized_text) {
                if !orig.is_empty() {
                    let key = format!("{}@{}@{}", symbol, repo, ts);
                    self.capsule_text_cache.insert(key, (orig.clone(), opt.clone()));
                    cached = true;
                    // Prevent unbounded growth; a simple clear-on-overflow is fine
                    // for a local dashboard cache that holds at most ~50 entries.
                    if self.capsule_text_cache.len() > 100 {
                        self.capsule_text_cache.clear();
                    }
                }
            }
            // Strip the large text blobs before pushing into recent_events so
            // the SSE broadcast and the /stats payload stay lean.
            if let DashboardEvent::CapsuleServed {
                symbol, repo, file, capsule_tokens, file_tokens, tokens_saved, origin, ts, ..
            } = event {
                DashboardEvent::CapsuleServed {
                    symbol, repo, file, capsule_tokens, file_tokens, tokens_saved, origin, ts,
                    original_text: None,
                    optimized_text: None,
                    has_cached_delta: cached,
                }
            } else {
                unreachable!()
            }
        } else {
            event
        };

        self.recent_events.push_front(slim_event);
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

async fn d3_handler() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "application/javascript")],
        D3_JS,
    )
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.tx.subscribe();
    let stream = tokio_stream::StreamExt::filter_map(BroadcastStream::new(rx), |msg: Result<DashboardEvent, _>| {
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
    database: DatabaseSnapshot,
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
    total_requests:               i64,
    total_tokens_saved:           i64,
    total_file_tokens:            i64,
    reduction_pct:                f64,
    pipeline_requests:            i64,
    direct_low_level_autorouted:  i64,
    direct_low_level_rejected:    i64,
    ambiguous_symbol_requests:    i64,
    stale_capsule_prevented:      i64,
    pipeline_compliance_pct:      f64,
}

#[derive(Serialize)]
struct DatabaseSnapshot {
    path:         String,
    size_mb:      f64,
    repo_count:   i64,
    symbol_count: i64,
    file_count:   i64,
    repos:        Vec<IndexedRepoSnapshot>,
}

#[derive(Serialize)]
struct IndexedRepoSnapshot {
    repo_id:      String,
    root_path:    String,
    symbol_count: i64,
    file_count:   i64,
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
        let pipeline = crate::db::read_stat(&conn, "pipeline_requests");
        let autorouted = crate::db::read_stat(&conn, "direct_low_level_autorouted");
        let rejected = crate::db::read_stat(&conn, "direct_low_level_rejected");
        let ambiguous = crate::db::read_stat(&conn, "ambiguous_symbol_requests");
        let stale = crate::db::read_stat(&conn, "stale_capsule_prevented");
        let compliance_total = pipeline + autorouted + rejected;
        let compliance_pct = if compliance_total == 0 {
            0.0
        } else {
            (pipeline as f64 / compliance_total as f64) * 100.0
        };
        LifetimeSnapshot {
            total_requests:              req,
            total_tokens_saved:          saved,
            total_file_tokens:           file,
            reduction_pct:               rpct,
            pipeline_requests:           pipeline,
            direct_low_level_autorouted: autorouted,
            direct_low_level_rejected:   rejected,
            ambiguous_symbol_requests:   ambiguous,
            stale_capsule_prevented:     stale,
            pipeline_compliance_pct:     compliance_pct,
        }
    };

    let database = {
        let conn = match state.db.lock() {
            Ok(g)  => g,
            Err(_) => return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response(),
        };
        let db_path = match crate::db::connected_database_path(&conn) {
            Ok(path) => path,
            Err(e) => {
                return axum::Json(serde_json::json!({
                    "error": format!("Could not determine attached database path: {e}")
                }))
                .into_response()
            }
        };
        let size_mb = std::fs::metadata(&db_path)
            .map(|m| m.len() as f64 / 1_048_576.0)
            .unwrap_or(0.0);
        let scope = match crate::db::database_scope_snapshot(&conn) {
            Ok(scope) => scope,
            Err(e) => {
                return axum::Json(serde_json::json!({
                    "error": format!("Could not inspect attached database: {e}")
                }))
                .into_response()
            }
        };

        DatabaseSnapshot {
            path: db_path,
            size_mb,
            repo_count: scope.repo_count,
            symbol_count: scope.symbol_count,
            file_count: scope.file_count,
            repos: scope
                .repos
                .into_iter()
                .map(|repo| IndexedRepoSnapshot {
                    repo_id: repo.repo_id,
                    root_path: repo.root_path,
                    symbol_count: repo.symbol_count,
                    file_count: repo.file_count,
                })
                .collect(),
        }
    };

    axum::Json(StatsResponse { session, lifetime, database }).into_response()
}

// ── Compare handler ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CompareQuery {
    #[allow(dead_code)] // Deserialized from query params but no longer used for file reads (C-1)
    filepath:  String,
    tool_used: String,
    symbol:    Option<String>,
    repo:      Option<String>,
    ts:        Option<u64>,
}

#[derive(Serialize)]
struct CompareResponse {
    original_text:    String,
    optimized_text:   String,
    original_length:  usize,
    optimized_length: usize,
}

async fn compare_handler(
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
    Query(params): Query<CompareQuery>,
) -> axum::response::Response {
    // M-15 FIX: Reject untrusted origins before processing.
    if let Err(status) = validate_origin(&headers) {
        return status.into_response();
    }
    match params.tool_used.as_str() {
        "get_context_capsule" => {
            let symbol = match params.symbol.as_deref().filter(|s| !s.is_empty()) {
                Some(s) => s.to_string(),
                None => return axum::Json(serde_json::json!({
                    "error": "Missing 'symbol' parameter for get_context_capsule"
                })).into_response(),
            };
            let repo = match params.repo.as_deref().filter(|r| !r.is_empty()) {
                Some(r) => r.to_string(),
                None => return axum::Json(serde_json::json!({
                    "error": "Missing 'repo' parameter for get_context_capsule"
                })).into_response(),
            };

            // Check the in-memory text cache first. This is populated from the
            // telemetry POST payload and is the correct source of truth in
            // Hub/Spoke deployments where the dashboard server and the process
            // that served the capsule use different .marrow/graph.db files
            // (e.g., Cursor + Copilot both running Marrow from different CWDs).
            let cache_key = format!("{}@{}@{}", symbol, repo, params.ts.unwrap_or_default());
            if let Ok(sess) = state.session.lock() {
                if let Some((original_text, optimized_text)) = sess.capsule_text_cache.get(&cache_key) {
                    let original_length  = original_text.len() / 4;
                    let optimized_length = optimized_text.len() / 4;
                    return axum::Json(CompareResponse {
                        original_text:    original_text.clone(),
                        optimized_text:   optimized_text.clone(),
                        original_length,
                        optimized_length,
                    }).into_response();
                }
            }

            if params.ts.is_some() {
                return axum::Json(serde_json::json!({
                    "error": "This proof snapshot is no longer cached. Re-run the query to generate a fresh immutable delta."
                })).into_response();
            }

            if crate::retrieval::capsule_original_mode() == crate::retrieval::CapsuleOriginalMode::None {
                return axum::Json(serde_json::json!({
                    "error": "Compare baseline unavailable: full-file original text is not retained when MARROW_CAPSULE_ORIGINAL_MODE is none (default). Pass a snapshot timestamp from telemetry, or set MARROW_CAPSULE_ORIGINAL_MODE=full to rebuild from disk."
                })).into_response();
            }

            // Cache miss: fall back to querying the local DB. This works when
            // the Hub and the capsule-serving process share the same DB file,
            // or when the cache was evicted (>100 entries).
            let conn = match state.db.lock() {
                Ok(g)  => g,
                Err(_) => return axum::Json(serde_json::json!({"error": "DB mutex poisoned"})).into_response(),
            };
            match crate::retrieval::get_context_capsule(&conn, &symbol, &repo, None) {
                Ok(result) => {
                    let original_length  = result.file_tokens;
                    let optimized_length = result.optimized_text.len() / 4;
                    axum::Json(CompareResponse {
                        original_text:    result.original_text,
                        optimized_text:   result.optimized_text,
                        original_length,
                        optimized_length,
                    }).into_response()
                }
                Err(e) => axum::Json(serde_json::json!({
                    "error": format!("Could not build capsule for '{}': {}", symbol, e)
                })).into_response(),
            }
        }
        _ => {
            // C-1 FIX: Never read arbitrary filesystem paths. Compare is only
            // supported for get_context_capsule via the in-memory text cache.
            axum::Json(serde_json::json!({
                "error": "Compare is only supported for get_context_capsule. Use a cached snapshot timestamp."
            })).into_response()
        }
    }
}

/// Allowed dashboard origins for handler-level validation.
/// Requests with no Origin header are allowed (local/internal clients).
/// Only the dashboard's own localhost origins are trusted.
const ALLOWED_ORIGINS: &[&str] = &[
    "http://127.0.0.1:8765",
    "http://localhost:8765",
];

/// Validate the Origin header: allow if absent (internal/local) or matching
/// a trusted localhost origin. Returns `Err(403)` for untrusted origins.
fn validate_origin(headers: &axum::http::HeaderMap) -> Result<(), axum::http::StatusCode> {
    if let Some(origin) = headers.get(axum::http::header::ORIGIN) {
        let origin_str = origin.to_str().unwrap_or("");
        if !ALLOWED_ORIGINS.contains(&origin_str) {
            return Err(axum::http::StatusCode::FORBIDDEN);
        }
    }
    Ok(())
}

/// M-15 FIX: Cap emit request body size to prevent oversized metric payloads.
const EMIT_MAX_BODY_BYTES: usize = 1024 * 1024; // 1 MiB

async fn emit_handler(
    headers: axum::http::HeaderMap,
    State(state): State<AppState>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    // M-15 FIX: Reject untrusted origins before processing.
    if let Err(status) = validate_origin(&headers) {
        return status;
    }
    // M-15 FIX: Reject oversized payloads before deserialization.
    if body.len() > EMIT_MAX_BODY_BYTES {
        return axum::http::StatusCode::PAYLOAD_TOO_LARGE;
    }
    let event: DashboardEvent = match serde_json::from_slice(&body) {
        Ok(e) => e,
        Err(_) => return axum::http::StatusCode::BAD_REQUEST,
    };
    let sess_result = state.session.lock();
    match sess_result {
        Err(_) => return axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        Ok(mut sess) => {
            match &event {
                DashboardEvent::CapsuleServed { capsule_tokens, file_tokens, .. } => {
                    sess.record_capsule(*capsule_tokens, *file_tokens, event.clone());
                }
                other => {
                    sess.recent_events.push_front(other.clone());
                    if sess.recent_events.len() > 50 {
                        sess.recent_events.pop_back();
                    }
                }
            }
        }
    }
    // Strip text blobs from SSE broadcast. The full texts are already cached
    // inside SessionStats.capsule_text_cache — browsers don't need them in
    // the live event stream. Propagate has_cached_delta so the client knows
    // whether the delta viewer button should be enabled.
    let broadcast_event = match event {
        DashboardEvent::CapsuleServed {
            symbol, repo, file, capsule_tokens, file_tokens, tokens_saved, origin, ts, has_cached_delta, ..
        } => DashboardEvent::CapsuleServed {
            symbol, repo, file, capsule_tokens, file_tokens, tokens_saved, origin, ts,
            original_text:    None,
            optimized_text:   None,
            has_cached_delta,
        },
        other => other,
    };
    let _ = state.tx.send(broadcast_event);
    axum::http::StatusCode::OK
}

// ── Graph API ─────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct GraphQuery {
    repo_id: String,
}

#[derive(Deserialize)]
struct GraphNeighborsQuery {
    node_id: String,
}

#[derive(Serialize)]
struct GraphNodeDto {
    id:          String,
    label:       String,
    file_path:   String,
    symbol_type: String,
    degree:      i64,
}

#[derive(Serialize)]
struct GraphEdgeDto {
    source:       String,
    target:       String,
    relationship: String,
}

#[derive(Serialize)]
struct GraphResponse {
    nodes:            Vec<GraphNodeDto>,
    edges:            Vec<GraphEdgeDto>,
    truncated:        bool,
    total_node_count: i64,
}

#[derive(Serialize)]
struct GraphNeighborsResponse {
    nodes: Vec<GraphNodeDto>,
    edges: Vec<GraphEdgeDto>,
}

async fn graph_handler(
    State(state): State<AppState>,
    Query(params): Query<GraphQuery>,
) -> axum::response::Response {
    let conn = match state.db.lock() {
        Ok(g)  => g,
        Err(_) => return axum::Json(serde_json::json!({"error": "DB mutex poisoned"})).into_response(),
    };

    let total_node_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
            rusqlite::params![params.repo_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    // Pre-aggregate edge degrees in a single scan rather than using a
    // correlated subquery in ORDER BY (which was O(n × m)).
    let nodes: Vec<GraphNodeDto> = {
        let mut stmt = match conn.prepare(
            "WITH edge_counts AS ( \
               SELECT id, COUNT(*) AS cnt FROM ( \
                 SELECT source_id AS id FROM edges \
                 UNION ALL \
                 SELECT target_id AS id FROM edges \
               ) GROUP BY id \
             ) \
             SELECT n.id, n.symbol_name, n.file_path, \
                    COALESCE(n.symbol_type, 'unknown'), \
                    COALESCE(ec.cnt, 0) AS degree \
             FROM nodes n \
             LEFT JOIN edge_counts ec ON ec.id = n.id \
             WHERE n.repo_id = ?1 \
             ORDER BY degree DESC \
             LIMIT 500",
        ) {
            Ok(s)  => s,
            Err(e) => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        let result: Vec<GraphNodeDto> = match stmt.query_map(rusqlite::params![params.repo_id], |row| {
            Ok(GraphNodeDto {
                id:          row.get(0)?,
                label:       row.get(1)?,
                file_path:   row.get(2)?,
                symbol_type: row.get(3)?,
                degree:      row.get(4)?,
            })
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e)   => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        result
    };

    let edges: Vec<GraphEdgeDto> = {
        let mut stmt = match conn.prepare(
            "WITH edge_counts AS ( \
               SELECT id, COUNT(*) AS cnt FROM ( \
                 SELECT source_id AS id FROM edges \
                 UNION ALL \
                 SELECT target_id AS id FROM edges \
               ) GROUP BY id \
             ), \
             top_nodes AS ( \
               SELECT n.id \
               FROM nodes n \
               LEFT JOIN edge_counts ec ON ec.id = n.id \
               WHERE n.repo_id = ?1 \
               ORDER BY COALESCE(ec.cnt, 0) DESC \
               LIMIT 500 \
             ) \
             SELECT e.source_id, e.target_id, COALESCE(e.relationship_type, 'CALLS') \
             FROM edges e \
             WHERE e.source_id IN (SELECT id FROM top_nodes) \
               AND e.target_id IN (SELECT id FROM top_nodes)",
        ) {
            Ok(s)  => s,
            Err(e) => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        let result: Vec<GraphEdgeDto> = match stmt.query_map(rusqlite::params![params.repo_id], |row| {
            Ok(GraphEdgeDto {
                source:       row.get(0)?,
                target:       row.get(1)?,
                relationship: row.get(2)?,
            })
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e)   => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        result
    };

    axum::Json(GraphResponse { truncated: total_node_count > 500, total_node_count, nodes, edges })
        .into_response()
}

async fn graph_neighbors_handler(
    State(state): State<AppState>,
    Query(params): Query<GraphNeighborsQuery>,
) -> axum::response::Response {
    let conn = match state.db.lock() {
        Ok(g)  => g,
        Err(_) => return axum::Json(serde_json::json!({"error": "DB mutex poisoned"})).into_response(),
    };

    let nodes: Vec<GraphNodeDto> = {
        let mut stmt = match conn.prepare(
            "WITH neighbor_ids AS ( \
               SELECT target_id AS id FROM edges WHERE source_id = ?1 \
               UNION \
               SELECT source_id AS id FROM edges WHERE target_id = ?1 \
             ), \
             relevant AS ( \
               SELECT n.id FROM nodes n WHERE n.id = ?1 \
               UNION \
               SELECT n.id FROM nodes n WHERE n.id IN (SELECT id FROM neighbor_ids) \
             ), \
             deg AS ( \
               SELECT id, COUNT(*) AS cnt FROM ( \
                 SELECT source_id AS id FROM edges WHERE source_id IN (SELECT id FROM relevant) \
                 UNION ALL \
                 SELECT target_id AS id FROM edges WHERE target_id IN (SELECT id FROM relevant) \
               ) GROUP BY id \
             ) \
             SELECT DISTINCT n.id, n.symbol_name, n.file_path, \
                    COALESCE(n.symbol_type, 'unknown'), COALESCE(deg.cnt, 0) \
             FROM nodes n \
             LEFT JOIN deg ON deg.id = n.id \
             WHERE n.id = ?1 OR n.id IN (SELECT id FROM neighbor_ids)",
        ) {
            Ok(s)  => s,
            Err(e) => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        let result: Vec<GraphNodeDto> = match stmt.query_map(rusqlite::params![params.node_id], |row| {
            Ok(GraphNodeDto {
                id:          row.get(0)?,
                label:       row.get(1)?,
                file_path:   row.get(2)?,
                symbol_type: row.get(3)?,
                degree:      row.get(4)?,
            })
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e)   => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        result
    };

    let edges: Vec<GraphEdgeDto> = {
        let mut stmt = match conn.prepare(
            "SELECT source_id, target_id, COALESCE(relationship_type, 'CALLS') \
             FROM edges \
             WHERE source_id = ?1 OR target_id = ?1",
        ) {
            Ok(s)  => s,
            Err(e) => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        let result: Vec<GraphEdgeDto> = match stmt.query_map(rusqlite::params![params.node_id], |row| {
            Ok(GraphEdgeDto {
                source:       row.get(0)?,
                target:       row.get(1)?,
                relationship: row.get(2)?,
            })
        }) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e)   => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
        };
        result
    };

    axum::Json(GraphNeighborsResponse { nodes, edges }).into_response()
}

// ── Server startup ────────────────────────────────────────────────────────────

/// Attempts to bind strictly to 127.0.0.1:8765.
///
/// Returns `HubRole::Hub` if this process won the election and the Axum
/// server is running in the background. Returns `HubRole::Spoke` if the
/// port is already taken — the caller continues in headless mode.
pub async fn start(
    tx:           broadcast::Sender<DashboardEvent>,
    session:      Arc<Mutex<SessionStats>>,
    db:           Arc<Mutex<rusqlite::Connection>>,
    auto_open_ui: bool,
) -> Result<HubRole> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8765));
    let listener = match TcpListener::bind(addr).await {
        Ok(l)  => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            return Ok(HubRole::Spoke);
        }
        // Any other bind error (e.g. PermissionDenied) is a genuine failure
        // that should propagate — not silently downgraded to Spoke mode.
        Err(e) => return Err(e.into()),
    };

    let state = AppState { tx, session, db };

    let router = Router::new()
        .route("/",                    get(index_handler))
        .route("/d3-v7.min.js",        get(d3_handler))
        .route("/stream",              get(sse_handler))
        .route("/stats",               get(stats_handler))
        .route("/api/compare",         get(compare_handler))
        .route("/api/emit",            axum::routing::post(emit_handler))
        .route("/api/graph",           get(graph_handler))
        .route("/api/graph/neighbors", get(graph_neighbors_handler))
        // C-1 FIX: Restrict CORS to same-origin. The dashboard is served from
        // http://127.0.0.1:8765 — only allow that origin rather than wildcard *.
        // Use from_static to avoid fallible parsing and .expect() in production.
        .layer(
            CorsLayer::new()
                .allow_origin(axum::http::HeaderValue::from_static("http://127.0.0.1:8765"))
                .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
                .allow_headers(tower_http::cors::Any)
        )
        .with_state(state);

    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).await {
            eprintln!("Marrow dashboard error: {e}");
        }
    });

    eprintln!("Marrow dashboard → http://127.0.0.1:8765");

    if auto_open_ui {
        if let Err(e) = open::that("http://127.0.0.1:8765") {
            eprintln!("Could not open browser: {e}");
        }
    }

    Ok(HubRole::Hub)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// /api/emit must reject payloads larger than 1 MiB.
    #[tokio::test]
    async fn emit_rejects_oversized_payload() {
        use axum::body::Bytes;
        use axum::extract::State;

        let (tx, _rx) = broadcast::channel(4);
        let conn = crate::db::init_db(":memory:").unwrap();
        let state = AppState {
            tx,
            session: Arc::new(Mutex::new(SessionStats::default())),
            db: Arc::new(Mutex::new(conn)),
        };

        // 1 MiB + 1 byte should be rejected
        let oversized = Bytes::from(vec![0u8; EMIT_MAX_BODY_BYTES + 1]);
        let response = emit_handler(axum::http::HeaderMap::new(), State(state.clone()), oversized).await;
        let response = axum::response::IntoResponse::into_response(response);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::PAYLOAD_TOO_LARGE,
            "emit must reject payloads > 1 MiB"
        );
    }

    /// /api/emit must accept payloads at or below 1 MiB (if valid JSON).
    #[tokio::test]
    async fn emit_accepts_valid_payload_under_limit() {
        use axum::body::Bytes;
        use axum::extract::State;

        let (tx, _rx) = broadcast::channel(4);
        let conn = crate::db::init_db(":memory:").unwrap();
        let state = AppState {
            tx,
            session: Arc::new(Mutex::new(SessionStats::default())),
            db: Arc::new(Mutex::new(conn)),
        };

        let event = serde_json::json!({
            "type": "repo_indexed",
            "repo_id": "test",
            "symbols": 10,
            "edges": 5,
            "ts": 12345
        });
        let body = Bytes::from(serde_json::to_vec(&event).unwrap());
        let response = emit_handler(axum::http::HeaderMap::new(), State(state), body).await;
        let response = axum::response::IntoResponse::into_response(response);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::OK,
            "emit must accept valid payloads under 1 MiB"
        );
    }

    #[test]
    fn emit_cap_is_one_mib() {
        assert_eq!(EMIT_MAX_BODY_BYTES, 1024 * 1024, "emit cap must be exactly 1 MiB");
    }

    #[test]
    fn validate_origin_allows_no_origin() {
        let headers = axum::http::HeaderMap::new();
        assert!(validate_origin(&headers).is_ok());
    }

    #[test]
    fn validate_origin_allows_localhost_origins() {
        for origin in ALLOWED_ORIGINS {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::ORIGIN,
                axum::http::HeaderValue::from_str(origin).unwrap(),
            );
            assert!(validate_origin(&headers).is_ok(), "should allow {origin}");
        }
    }

    #[test]
    fn validate_origin_rejects_foreign_origin() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            axum::http::HeaderValue::from_static("https://evil.com"),
        );
        assert_eq!(
            validate_origin(&headers).unwrap_err(),
            axum::http::StatusCode::FORBIDDEN,
        );
    }

    /// /api/emit must reject requests from untrusted origins.
    #[tokio::test]
    async fn emit_rejects_bad_origin() {
        use axum::body::Bytes;
        use axum::extract::State;

        let (tx, _rx) = broadcast::channel(4);
        let conn = crate::db::init_db(":memory:").unwrap();
        let state = AppState {
            tx,
            session: Arc::new(Mutex::new(SessionStats::default())),
            db: Arc::new(Mutex::new(conn)),
        };

        let event = serde_json::json!({
            "type": "repo_indexed",
            "repo_id": "test",
            "symbols": 10,
            "edges": 5,
            "ts": 12345
        });
        let body = Bytes::from(serde_json::to_vec(&event).unwrap());
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            axum::http::HeaderValue::from_static("https://evil.com"),
        );
        let response = emit_handler(headers, State(state), body).await;
        let response = axum::response::IntoResponse::into_response(response);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::FORBIDDEN,
            "emit must reject untrusted origins"
        );
    }

    /// /api/compare must reject requests from untrusted origins.
    #[tokio::test]
    async fn compare_rejects_bad_origin() {
        use axum::extract::{Query, State};

        let (tx, _rx) = broadcast::channel(4);
        let conn = crate::db::init_db(":memory:").unwrap();
        let state = AppState {
            tx,
            session: Arc::new(Mutex::new(SessionStats::default())),
            db: Arc::new(Mutex::new(conn)),
        };

        let params = CompareQuery {
            filepath: "test.py".to_string(),
            tool_used: "get_context_capsule".to_string(),
            symbol: Some("foo".to_string()),
            repo: Some("test".to_string()),
            ts: None,
        };
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::ORIGIN,
            axum::http::HeaderValue::from_static("https://evil.com"),
        );
        let response = compare_handler(headers, State(state), Query(params)).await;
        assert_eq!(
            response.status(),
            axum::http::StatusCode::FORBIDDEN,
            "compare must reject untrusted origins"
        );
    }
}
