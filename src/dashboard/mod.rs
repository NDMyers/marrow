use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use axum::{
    extract::{Query, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::CorsLayer;

use crate::retrieval::{CapsuleProofSnapshot, CapsuleProvenance};

static INDEX_HTML: &str = include_str!("index.html");
static D3_JS: &str = include_str!("d3-v7.min.js");
const GRAPH_INITIAL_NODE_CAP: usize = 500;
const GRAPH_TOP_NODES_SQL: &str = "SELECT n.id, n.symbol_name, n.file_path, \
            COALESCE(n.symbol_type, 'unknown'), gd.degree \
     FROM graph_node_degrees gd \
     JOIN nodes n ON n.id = gd.node_id \
     WHERE gd.repo_id = ?1 AND n.repo_id = ?1 \
     ORDER BY gd.degree DESC, n.file_path ASC, n.symbol_name ASC, n.id ASC \
     LIMIT ?2";
const GRAPH_RETURNED_EDGES_SQL: &str = "WITH top_nodes AS ( \
        SELECT n.id \
        FROM graph_node_degrees gd \
        JOIN nodes n ON n.id = gd.node_id \
        WHERE gd.repo_id = ?1 AND n.repo_id = ?1 \
        ORDER BY gd.degree DESC, n.file_path ASC, n.symbol_name ASC, n.id ASC \
        LIMIT ?2 \
     ) \
     SELECT e.source_id, e.target_id, COALESCE(e.relationship_type, 'CALLS') \
     FROM edges e \
     JOIN top_nodes src ON src.id = e.source_id \
     JOIN top_nodes tgt ON tgt.id = e.target_id \
     ORDER BY e.source_id ASC, e.target_id ASC, e.relationship_type ASC";

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
        /// Bounded proof text plus sampling/truncation metadata. Text is
        /// stripped before SSE/stats storage; metadata is retained.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        proof_snapshot: Option<Box<CapsuleProofSnapshot>>,
        #[serde(default)]
        provenance: Box<CapsuleProvenance>,
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
#[allow(dead_code)]
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
    pub total_requests: usize,
    pub total_capsule_tokens: usize,
    pub total_file_tokens: usize,
    pub total_tokens_saved: usize,
    pub recent_events: VecDeque<DashboardEvent>,
    /// Token delta text cache keyed by `"symbol@repo@ts"`.
    /// Populated from the telemetry POST body so the compare endpoint works
    /// even when the Axum server (Hub) is running against a different DB than
    /// the process that served the capsule (Spoke).
    pub capsule_text_cache: HashMap<String, CachedCapsuleDelta>,
}

#[derive(Clone, Debug)]
pub struct CachedCapsuleDelta {
    pub baseline_text: String,
    pub optimized_text: String,
    pub original_length: usize,
    pub optimized_length: usize,
    pub proof_snapshot: Option<CapsuleProofSnapshot>,
    pub provenance: CapsuleProvenance,
}

impl SessionStats {
    pub fn record_capsule(
        &mut self,
        capsule_tokens: usize,
        file_tokens: usize,
        event: DashboardEvent,
    ) {
        self.total_requests += 1;
        self.total_capsule_tokens += capsule_tokens;
        self.total_file_tokens += file_tokens;
        self.total_tokens_saved += file_tokens.saturating_sub(capsule_tokens);

        // Extract and cache texts before stripping them from the stored event.
        // The cache lets the compare endpoint serve delta views without re-querying
        // the DB — critical for Hub/Spoke scenarios where the Hub's DB is different.
        let slim_event = if let DashboardEvent::CapsuleServed {
            ref symbol,
            ref repo,
            ref original_text,
            ref optimized_text,
            ref proof_snapshot,
            ref provenance,
            file_tokens,
            capsule_tokens,
            ts,
            ..
        } = event
        {
            let mut cached = false;
            if let Some(opt) = optimized_text {
                let baseline_text = original_text
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .cloned()
                    .or_else(|| {
                        proof_snapshot
                            .as_ref()
                            .map(|p| p.proof_text.clone())
                            .filter(|s| !s.is_empty())
                    });
                if let Some(baseline_text) = baseline_text {
                    let key = format!("{}@{}@{}", symbol, repo, ts);
                    if self.capsule_text_cache.len() >= 100 {
                        self.capsule_text_cache.clear();
                    }
                    self.capsule_text_cache.insert(
                        key,
                        CachedCapsuleDelta {
                            original_length: file_tokens,
                            optimized_length: capsule_tokens,
                            baseline_text,
                            optimized_text: opt.clone(),
                            proof_snapshot: proof_snapshot.as_deref().cloned(),
                            provenance: (**provenance).clone(),
                        },
                    );
                    cached = true;
                }
            }
            // Strip the large text blobs before pushing into recent_events so
            // the SSE broadcast and the /stats payload stay lean.
            if let DashboardEvent::CapsuleServed {
                symbol,
                repo,
                file,
                capsule_tokens,
                file_tokens,
                tokens_saved,
                origin,
                ts,
                proof_snapshot,
                provenance,
                ..
            } = event
            {
                DashboardEvent::CapsuleServed {
                    symbol,
                    repo,
                    file,
                    capsule_tokens,
                    file_tokens,
                    tokens_saved,
                    origin,
                    ts,
                    original_text: None,
                    optimized_text: None,
                    proof_snapshot: proof_snapshot
                        .as_deref()
                        .map(CapsuleProofSnapshot::without_text)
                        .map(Box::new),
                    provenance,
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
    pub tx: broadcast::Sender<DashboardEvent>,
    pub session: Arc<Mutex<SessionStats>>,
    pub db: Arc<Mutex<rusqlite::Connection>>,
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
    let stream = tokio_stream::StreamExt::filter_map(
        BroadcastStream::new(rx),
        |msg: Result<DashboardEvent, _>| {
            let event = msg.ok()?;
            let json = serde_json::to_string(&event).ok()?;
            Some(Ok::<Event, Infallible>(Event::default().data(json)))
        },
    );
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Serialize)]
struct StatsResponse {
    session: SessionSnapshot,
    lifetime: LifetimeSnapshot,
    database: DatabaseSnapshot,
}

#[derive(Serialize)]
struct SessionSnapshot {
    total_requests: usize,
    total_capsule_tokens: usize,
    total_file_tokens: usize,
    total_tokens_saved: usize,
    reduction_pct: f64,
    recent_events: Vec<DashboardEvent>,
}

#[derive(Serialize)]
struct LifetimeSnapshot {
    total_requests: i64,
    total_tokens_saved: i64,
    total_file_tokens: i64,
    reduction_pct: f64,
    pipeline_requests: i64,
    direct_low_level_autorouted: i64,
    direct_low_level_rejected: i64,
    ambiguous_symbol_requests: i64,
    stale_capsule_prevented: i64,
    pipeline_compliance_pct: f64,
}

#[derive(Serialize)]
struct DatabaseSnapshot {
    path: String,
    size_mb: f64,
    repo_count: i64,
    symbol_count: i64,
    file_count: i64,
    repos: Vec<IndexedRepoSnapshot>,
}

#[derive(Serialize)]
struct IndexedRepoSnapshot {
    repo_id: String,
    root_path: String,
    symbol_count: i64,
    file_count: i64,
}

async fn stats_handler(State(state): State<AppState>) -> axum::response::Response {
    let sess = match state.session.lock() {
        Ok(g) => g,
        Err(_) => return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response(),
    };
    let reduction_pct = if sess.total_file_tokens == 0 {
        0.0
    } else {
        (sess.total_tokens_saved as f64 / sess.total_file_tokens as f64) * 100.0
    };
    let session = SessionSnapshot {
        total_requests: sess.total_requests,
        total_capsule_tokens: sess.total_capsule_tokens,
        total_file_tokens: sess.total_file_tokens,
        total_tokens_saved: sess.total_tokens_saved,
        reduction_pct,
        recent_events: sess.recent_events.iter().cloned().collect(),
    };
    drop(sess);

    let lifetime = {
        let conn = match state.db.lock() {
            Ok(g) => g,
            Err(_) => {
                return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response()
            }
        };
        let req = crate::db::read_stat(&conn, "total_requests");
        let saved = crate::db::read_stat(&conn, "total_tokens_saved");
        let file = crate::db::read_stat(&conn, "total_file_tokens");
        let rpct = if file == 0 {
            0.0
        } else {
            (saved as f64 / file as f64) * 100.0
        };
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
            total_requests: req,
            total_tokens_saved: saved,
            total_file_tokens: file,
            reduction_pct: rpct,
            pipeline_requests: pipeline,
            direct_low_level_autorouted: autorouted,
            direct_low_level_rejected: rejected,
            ambiguous_symbol_requests: ambiguous,
            stale_capsule_prevented: stale,
            pipeline_compliance_pct: compliance_pct,
        }
    };

    let database = {
        let conn = match state.db.lock() {
            Ok(g) => g,
            Err(_) => {
                return axum::Json(serde_json::json!({"error": "lock poisoned"})).into_response()
            }
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

    axum::Json(StatsResponse {
        session,
        lifetime,
        database,
    })
    .into_response()
}

// ── Compare handler ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CompareQuery {
    #[allow(dead_code)] // Deserialized from query params but no longer used for file reads (C-1)
    filepath: String,
    tool_used: String,
    symbol: Option<String>,
    repo: Option<String>,
    ts: Option<u64>,
}

#[derive(Serialize)]
struct CompareResponse {
    original_text: String,
    optimized_text: String,
    original_length: usize,
    optimized_length: usize,
    proof_snapshot: Option<CapsuleProofSnapshot>,
    provenance: CapsuleProvenance,
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
                None => {
                    return axum::Json(serde_json::json!({
                        "error": "Missing 'symbol' parameter for get_context_capsule"
                    }))
                    .into_response()
                }
            };
            let repo = match params.repo.as_deref().filter(|r| !r.is_empty()) {
                Some(r) => r.to_string(),
                None => {
                    return axum::Json(serde_json::json!({
                        "error": "Missing 'repo' parameter for get_context_capsule"
                    }))
                    .into_response()
                }
            };

            // Check the in-memory text cache first. This is populated from the
            // telemetry POST payload and is the correct source of truth in
            // Hub/Spoke deployments where the dashboard server and the process
            // that served the capsule use different .marrow/graph.db files
            // (e.g., Cursor + Copilot both running Marrow from different CWDs).
            let cache_key = format!("{}@{}@{}", symbol, repo, params.ts.unwrap_or_default());
            if let Ok(sess) = state.session.lock() {
                if let Some(cached) = sess.capsule_text_cache.get(&cache_key) {
                    return axum::Json(CompareResponse {
                        original_text: cached.baseline_text.clone(),
                        optimized_text: cached.optimized_text.clone(),
                        original_length: cached.original_length,
                        optimized_length: cached.optimized_length,
                        proof_snapshot: cached
                            .proof_snapshot
                            .as_ref()
                            .map(CapsuleProofSnapshot::without_text),
                        provenance: cached.provenance.clone(),
                    })
                    .into_response();
                }
            }

            if params.ts.is_some() {
                return axum::Json(serde_json::json!({
                    "error": "This proof snapshot is no longer cached. Re-run the query to generate a fresh immutable delta."
                })).into_response();
            }

            if crate::retrieval::capsule_original_mode()
                == crate::retrieval::CapsuleOriginalMode::None
            {
                return axum::Json(serde_json::json!({
                    "error": "Compare baseline unavailable: full-file original text is not retained when MARROW_CAPSULE_ORIGINAL_MODE is none (default). Pass a snapshot timestamp from telemetry, or set MARROW_CAPSULE_ORIGINAL_MODE=full to rebuild from disk."
                })).into_response();
            }

            // Cache miss: fall back to querying the local DB. This works when
            // the Hub and the capsule-serving process share the same DB file,
            // or when the cache was evicted (>100 entries).
            let conn = match state.db.lock() {
                Ok(g) => g,
                Err(_) => {
                    return axum::Json(serde_json::json!({"error": "DB mutex poisoned"}))
                        .into_response()
                }
            };
            match crate::retrieval::get_context_capsule(&conn, &symbol, &repo, None) {
                Ok(result) => {
                    if result.original_text.is_empty() {
                        return axum::Json(serde_json::json!({
                            "error": "Compare baseline unavailable: no cached proof snapshot or full original text is available for this event."
                        })).into_response();
                    }
                    let original_length = result.file_tokens;
                    let optimized_length = result.optimized_text.len() / 4;
                    axum::Json(CompareResponse {
                        original_text: result.original_text,
                        optimized_text: result.optimized_text,
                        original_length,
                        optimized_length,
                        proof_snapshot: result.proof_snapshot.map(|p| p.without_text()),
                        provenance: result.provenance,
                    })
                    .into_response()
                }
                Err(e) => axum::Json(serde_json::json!({
                    "error": format!("Could not build capsule for '{}': {}", symbol, e)
                }))
                .into_response(),
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
const ALLOWED_ORIGINS: &[&str] = &["http://127.0.0.1:8765", "http://localhost:8765"];

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
        Ok(mut sess) => match &event {
            DashboardEvent::CapsuleServed {
                capsule_tokens,
                file_tokens,
                ..
            } => {
                sess.record_capsule(*capsule_tokens, *file_tokens, event.clone());
            }
            other => {
                sess.recent_events.push_front(other.clone());
                if sess.recent_events.len() > 50 {
                    sess.recent_events.pop_back();
                }
            }
        },
    }
    // Strip text blobs from SSE broadcast. The full texts are already cached
    // inside SessionStats.capsule_text_cache — browsers don't need them in
    // the live event stream. Propagate has_cached_delta so the client knows
    // whether the delta viewer button should be enabled.
    let broadcast_event = match event {
        DashboardEvent::CapsuleServed {
            symbol,
            repo,
            file,
            capsule_tokens,
            file_tokens,
            tokens_saved,
            origin,
            ts,
            has_cached_delta,
            proof_snapshot,
            provenance,
            ..
        } => DashboardEvent::CapsuleServed {
            symbol,
            repo,
            file,
            capsule_tokens,
            file_tokens,
            tokens_saved,
            origin,
            ts,
            original_text: None,
            optimized_text: None,
            proof_snapshot: proof_snapshot
                .as_deref()
                .map(CapsuleProofSnapshot::without_text)
                .map(Box::new),
            provenance,
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
    id: String,
    label: String,
    file_path: String,
    symbol_type: String,
    degree: i64,
}

#[derive(Serialize)]
struct GraphEdgeDto {
    source: String,
    target: String,
    relationship: String,
}

#[derive(Serialize)]
struct GraphResponse {
    nodes: Vec<GraphNodeDto>,
    edges: Vec<GraphEdgeDto>,
    truncated: bool,
    total_node_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    performance: Option<GraphPerformanceDto>,
}

#[derive(Serialize)]
struct GraphPerformanceDto {
    initial_node_cap: usize,
    degree_cache: &'static str,
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
        Ok(g) => g,
        Err(_) => {
            return axum::Json(serde_json::json!({"error": "DB mutex poisoned"})).into_response()
        }
    };

    let total_node_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
            rusqlite::params![params.repo_id],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let degree_cache_rebuilt = match crate::db::ensure_graph_degrees(&conn, &params.repo_id) {
        Ok(rebuilt) => rebuilt,
        Err(e) => return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response(),
    };

    let nodes: Vec<GraphNodeDto> = {
        let mut stmt = match conn.prepare(GRAPH_TOP_NODES_SQL) {
            Ok(s) => s,
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        let result: Vec<GraphNodeDto> = match stmt.query_map(
            rusqlite::params![params.repo_id, GRAPH_INITIAL_NODE_CAP as i64],
            |row| {
                Ok(GraphNodeDto {
                    id: row.get(0)?,
                    label: row.get(1)?,
                    file_path: row.get(2)?,
                    symbol_type: row.get(3)?,
                    degree: row.get(4)?,
                })
            },
        ) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        result
    };

    let edges: Vec<GraphEdgeDto> = {
        let mut stmt = match conn.prepare(GRAPH_RETURNED_EDGES_SQL) {
            Ok(s) => s,
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        let result: Vec<GraphEdgeDto> = match stmt.query_map(
            rusqlite::params![params.repo_id, GRAPH_INITIAL_NODE_CAP as i64],
            |row| {
                Ok(GraphEdgeDto {
                    source: row.get(0)?,
                    target: row.get(1)?,
                    relationship: row.get(2)?,
                })
            },
        ) {
            Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        result
    };

    axum::Json(GraphResponse {
        truncated: total_node_count > GRAPH_INITIAL_NODE_CAP as i64,
        total_node_count,
        nodes,
        edges,
        performance: Some(GraphPerformanceDto {
            initial_node_cap: GRAPH_INITIAL_NODE_CAP,
            degree_cache: if degree_cache_rebuilt {
                "rebuilt"
            } else {
                "ready"
            },
        }),
    })
    .into_response()
}

async fn graph_neighbors_handler(
    State(state): State<AppState>,
    Query(params): Query<GraphNeighborsQuery>,
) -> axum::response::Response {
    let conn = match state.db.lock() {
        Ok(g) => g,
        Err(_) => {
            return axum::Json(serde_json::json!({"error": "DB mutex poisoned"})).into_response()
        }
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
            Ok(s) => s,
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        let result: Vec<GraphNodeDto> =
            match stmt.query_map(rusqlite::params![params.node_id], |row| {
                Ok(GraphNodeDto {
                    id: row.get(0)?,
                    label: row.get(1)?,
                    file_path: row.get(2)?,
                    symbol_type: row.get(3)?,
                    degree: row.get(4)?,
                })
            }) {
                Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                Err(e) => {
                    return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
                }
            };
        result
    };

    let edges: Vec<GraphEdgeDto> = {
        let mut stmt = match conn.prepare(
            "SELECT source_id, target_id, COALESCE(relationship_type, 'CALLS') \
             FROM edges \
             WHERE source_id = ?1 OR target_id = ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
            }
        };
        let result: Vec<GraphEdgeDto> =
            match stmt.query_map(rusqlite::params![params.node_id], |row| {
                Ok(GraphEdgeDto {
                    source: row.get(0)?,
                    target: row.get(1)?,
                    relationship: row.get(2)?,
                })
            }) {
                Ok(rows) => rows.filter_map(|r| r.ok()).collect(),
                Err(e) => {
                    return axum::Json(serde_json::json!({"error": format!("{e}")})).into_response()
                }
            };
        result
    };

    axum::Json(GraphNeighborsResponse { nodes, edges }).into_response()
}

// ── Server startup ────────────────────────────────────────────────────────────

/// Build the dashboard router fragment (all dashboard routes + CORS layer).
///
/// This returns a `Router<AppState>` that can be merged into a larger application
/// or served standalone. The caller is responsible for binding a listener.
pub fn build_dashboard_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index_handler))
        .route("/d3-v7.min.js", get(d3_handler))
        .route("/stream", get(sse_handler))
        .route("/stats", get(stats_handler))
        .route("/api/compare", get(compare_handler))
        .route("/api/emit", axum::routing::post(emit_handler))
        .route("/api/graph", get(graph_handler))
        .route("/api/graph/neighbors", get(graph_neighbors_handler))
        // C-1 FIX: Restrict CORS to same-origin. The dashboard is served from
        // http://127.0.0.1:8765 — only allow that origin rather than wildcard *.
        // Use from_static to avoid fallible parsing and .expect() in production.
        .layer(
            CorsLayer::new()
                .allow_origin(axum::http::HeaderValue::from_static(
                    "http://127.0.0.1:8765",
                ))
                .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
                .allow_headers(tower_http::cors::Any),
        )
        .with_state(state)
}

/// Attempts to bind strictly to 127.0.0.1:8765.
///
/// Returns `HubRole::Hub` if this process won the election and the Axum
/// server is running in the background. Returns `HubRole::Spoke` if the
/// port is already taken — the caller continues in headless mode.
#[allow(dead_code)]
pub async fn start(
    tx: broadcast::Sender<DashboardEvent>,
    session: Arc<Mutex<SessionStats>>,
    db: Arc<Mutex<rusqlite::Connection>>,
    auto_open_ui: bool,
) -> Result<HubRole> {
    let addr = SocketAddr::from(([127, 0, 0, 1], 8765));
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            return Ok(HubRole::Spoke);
        }
        // Any other bind error (e.g. PermissionDenied) is a genuine failure
        // that should propagate — not silently downgraded to Spoke mode.
        Err(e) => return Err(e.into()),
    };

    let state = AppState { tx, session, db };
    let router = build_dashboard_router(state);

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
        let response = emit_handler(
            axum::http::HeaderMap::new(),
            State(state.clone()),
            oversized,
        )
        .await;
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
        assert_eq!(
            EMIT_MAX_BODY_BYTES,
            1024 * 1024,
            "emit cap must be exactly 1 MiB"
        );
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

    async fn compare_json(state: AppState, params: CompareQuery) -> serde_json::Value {
        use axum::body::to_bytes;
        use axum::extract::{Query, State};

        let response =
            compare_handler(axum::http::HeaderMap::new(), State(state), Query(params)).await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn cached_capsule_delta() -> CachedCapsuleDelta {
        CachedCapsuleDelta {
            baseline_text: "bounded proof text".to_string(),
            optimized_text: "optimized capsule text".to_string(),
            original_length: 101,
            optimized_length: 17,
            proof_snapshot: Some(CapsuleProofSnapshot {
                proof_text: "bounded proof text".to_string(),
                proof_label: "sampled proof".to_string(),
                token_source: "estimated".to_string(),
                truncated: true,
                sampled: true,
                max_bytes: 64,
                max_files: 2,
                touched_file_count: 3,
                included_file_count: 2,
                omitted_file_count: 1,
                omitted_paths_preview: vec!["src/omitted.rs".to_string()],
            }),
            provenance: CapsuleProvenance {
                baseline_token_source: "estimated".to_string(),
                tokenizer_mode: "text_len/4".to_string(),
                original_mode: "none".to_string(),
                proof_label: "sampled proof".to_string(),
                precise_file_tokens: false,
                original_max_bytes: None,
                proof_max_bytes: 64,
                proof_max_files: 2,
                touched_file_count: 3,
            },
        }
    }

    #[tokio::test]
    async fn compare_cached_proof_returns_bounded_text_and_provenance() {
        let state = graph_test_state(crate::db::init_db(":memory:").unwrap());
        state
            .session
            .lock()
            .unwrap()
            .capsule_text_cache
            .insert("foo@repo@777".to_string(), cached_capsule_delta());

        let data = compare_json(
            state,
            CompareQuery {
                filepath: "ignored.rs".to_string(),
                tool_used: "get_context_capsule".to_string(),
                symbol: Some("foo".to_string()),
                repo: Some("repo".to_string()),
                ts: Some(777),
            },
        )
        .await;

        assert_eq!(data["original_text"], "bounded proof text");
        assert_eq!(data["optimized_text"], "optimized capsule text");
        assert_eq!(data["original_length"], 101);
        assert_eq!(data["optimized_length"], 17);
        assert_eq!(data["proof_snapshot"]["proof_text"], "");
        assert_eq!(data["proof_snapshot"]["proof_label"], "sampled proof");
        assert_eq!(data["provenance"]["proof_label"], "sampled proof");
        assert_eq!(data["provenance"]["touched_file_count"], 3);
    }

    #[tokio::test]
    async fn compare_timestamp_cache_miss_reports_unavailable_proof() {
        let data = compare_json(
            graph_test_state(crate::db::init_db(":memory:").unwrap()),
            CompareQuery {
                filepath: "ignored.rs".to_string(),
                tool_used: "get_context_capsule".to_string(),
                symbol: Some("missing".to_string()),
                repo: Some("repo".to_string()),
                ts: Some(404),
            },
        )
        .await;

        assert!(data["error"]
            .as_str()
            .unwrap()
            .contains("proof snapshot is no longer cached"));
    }

    #[tokio::test]
    async fn compare_ignores_filepath_when_cached_proof_exists() {
        let state = graph_test_state(crate::db::init_db(":memory:").unwrap());
        state
            .session
            .lock()
            .unwrap()
            .capsule_text_cache
            .insert("foo@repo@777".to_string(), cached_capsule_delta());

        let data = compare_json(
            state,
            CompareQuery {
                filepath: "/definitely/not/a/readable/source/file.rs".to_string(),
                tool_used: "get_context_capsule".to_string(),
                symbol: Some("foo".to_string()),
                repo: Some("repo".to_string()),
                ts: Some(777),
            },
        )
        .await;

        assert_eq!(data["original_text"], "bounded proof text");
        assert!(data.get("error").is_none());
    }

    #[test]
    fn record_capsule_strips_blobs_but_retains_bounded_proof_metadata() {
        let mut stats = SessionStats::default();
        stats.record_capsule(
            17,
            101,
            DashboardEvent::CapsuleServed {
                symbol: "foo".to_string(),
                repo: "repo".to_string(),
                file: "src/foo.rs".to_string(),
                capsule_tokens: 17,
                file_tokens: 101,
                tokens_saved: 84,
                origin: "test".to_string(),
                ts: 777,
                original_text: None,
                optimized_text: Some("optimized capsule text".to_string()),
                proof_snapshot: cached_capsule_delta().proof_snapshot.map(Box::new),
                provenance: Box::new(cached_capsule_delta().provenance),
                has_cached_delta: false,
            },
        );

        let DashboardEvent::CapsuleServed {
            original_text,
            optimized_text,
            proof_snapshot,
            has_cached_delta,
            ..
        } = stats.recent_events.front().unwrap()
        else {
            panic!("expected capsule event");
        };
        assert!(original_text.is_none());
        assert!(optimized_text.is_none());
        assert!(*has_cached_delta);
        let proof = proof_snapshot
            .as_ref()
            .expect("proof metadata should remain");
        assert!(proof.proof_text.is_empty());
        assert_eq!(proof.proof_label, "sampled proof");
        assert_eq!(proof.max_bytes, 64);

        let cached = stats
            .capsule_text_cache
            .get("foo@repo@777")
            .expect("bounded proof should be cached for compare");
        assert_eq!(cached.baseline_text, "bounded proof text");
        assert_eq!(cached.provenance.proof_max_files, 2);
    }

    fn graph_test_state(conn: rusqlite::Connection) -> AppState {
        let (tx, _rx) = broadcast::channel(4);
        AppState {
            tx,
            session: Arc::new(Mutex::new(SessionStats::default())),
            db: Arc::new(Mutex::new(conn)),
        }
    }

    fn insert_graph_node(conn: &rusqlite::Connection, repo_id: &str, index: usize) -> String {
        let id = format!("{repo_id}:node:{index:03}");
        conn.execute(
            "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
             VALUES (?1, ?2, ?3, 'rs', ?4, 'function', ?5)",
            rusqlite::params![
                id,
                repo_id,
                format!("src/file_{index:03}.rs"),
                format!("sym_{index:03}"),
                format!("fn sym_{index:03}() {{}}")
            ],
        )
        .unwrap();
        format!("{repo_id}:node:{index:03}")
    }

    async fn graph_json(state: AppState, repo_id: &str) -> serde_json::Value {
        use axum::body::to_bytes;
        use axum::extract::{Query, State};

        let response = graph_handler(
            State(state),
            Query(GraphQuery {
                repo_id: repo_id.to_string(),
            }),
        )
        .await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    async fn graph_neighbors_json(state: AppState, node_id: &str) -> serde_json::Value {
        use axum::body::to_bytes;
        use axum::extract::{Query, State};

        let response = graph_neighbors_handler(
            State(state),
            Query(GraphNeighborsQuery {
                node_id: node_id.to_string(),
            }),
        )
        .await;
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn insert_many_graph_nodes(conn: &mut rusqlite::Connection, repo_id: &str, count: usize) {
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO nodes (id, repo_id, file_path, language, symbol_name, symbol_type, raw_text)
                     VALUES (?1, ?2, ?3, 'rs', ?4, 'function', ?5)",
                )
                .unwrap();
            for index in 0..count {
                let id = format!("{repo_id}:node:{index:06}");
                stmt.execute(rusqlite::params![
                    id,
                    repo_id,
                    format!("src/file_{index:06}.rs"),
                    format!("sym_{index:06}"),
                    format!("fn sym_{index:06}() {{}}")
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();
    }

    fn html_between(start_marker: &str, end_marker: &str) -> &'static str {
        let start = INDEX_HTML.find(start_marker).unwrap();
        let rest = &INDEX_HTML[start..];
        let end = rest.find(end_marker).unwrap();
        &rest[..end]
    }

    #[test]
    fn graph_top_nodes_query_uses_cached_degree_rank_data() {
        assert!(GRAPH_TOP_NODES_SQL.contains("graph_node_degrees"));
        assert!(!GRAPH_TOP_NODES_SQL.contains("edge_counts"));
        assert!(!GRAPH_TOP_NODES_SQL.contains("UNION ALL"));
    }

    #[tokio::test]
    async fn graph_handler_caps_large_repo_and_filters_edges_to_returned_nodes() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('repo', '/tmp/repo')",
            [],
        )
        .unwrap();
        let ids: Vec<String> = (0..505)
            .map(|i| insert_graph_node(&conn, "repo", i))
            .collect();
        for target in ids.iter().skip(1) {
            conn.execute(
                "INSERT INTO edges (source_id, target_id, relationship_type) VALUES (?1, ?2, 'CALLS')",
                rusqlite::params![ids[0], target],
            )
            .unwrap();
        }
        conn.pragma_update(None, "foreign_keys", "OFF").unwrap();
        conn.execute(
            "INSERT INTO edges (source_id, target_id, relationship_type) VALUES (?1, 'missing', 'CALLS')",
            rusqlite::params![ids[0]],
        )
        .unwrap();

        let data = graph_json(graph_test_state(conn), "repo").await;
        let nodes = data["nodes"].as_array().unwrap();
        let edges = data["edges"].as_array().unwrap();
        assert_eq!(nodes.len(), GRAPH_INITIAL_NODE_CAP);
        assert_eq!(data["truncated"], true);
        assert_eq!(data["total_node_count"], 505);
        assert_eq!(nodes[0]["id"], ids[0]);

        let returned: std::collections::HashSet<String> = nodes
            .iter()
            .map(|node| node["id"].as_str().unwrap().to_string())
            .collect();
        assert!(!returned.contains(&ids[500]));
        assert!(edges.iter().all(|edge| {
            returned.contains(edge["source"].as_str().unwrap())
                && returned.contains(edge["target"].as_str().unwrap())
        }));
        assert_eq!(edges.len(), GRAPH_INITIAL_NODE_CAP - 1);
    }

    #[tokio::test]
    async fn graph_handler_caps_large_synthetic_repo_with_stable_degree_ranking() {
        let mut conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('large', '/tmp/large')",
            [],
        )
        .unwrap();
        insert_many_graph_nodes(&mut conn, "large", 94_001);
        let tx = conn.transaction().unwrap();
        {
            let mut stmt = tx
                .prepare(
                    "INSERT INTO edges (source_id, target_id, relationship_type)
                     VALUES (?1, ?2, 'CALLS')",
                )
                .unwrap();
            for target in 0..40 {
                stmt.execute(rusqlite::params![
                    "large:node:093000",
                    format!("large:node:{target:06}")
                ])
                .unwrap();
            }
        }
        tx.commit().unwrap();

        let state = graph_test_state(conn);
        let first = graph_json(state.clone(), "large").await;
        let second = graph_json(state, "large").await;
        let first_nodes = first["nodes"].as_array().unwrap();
        let second_nodes = second["nodes"].as_array().unwrap();

        assert_eq!(first_nodes.len(), GRAPH_INITIAL_NODE_CAP);
        assert_eq!(first["truncated"], true);
        assert_eq!(first["total_node_count"], 94_001);
        assert_eq!(
            first["performance"]["initial_node_cap"],
            GRAPH_INITIAL_NODE_CAP
        );
        assert_eq!(first["nodes"][0]["id"], "large:node:093000");

        let first_ids: Vec<&str> = first_nodes
            .iter()
            .take(25)
            .map(|node| node["id"].as_str().unwrap())
            .collect();
        let second_ids: Vec<&str> = second_nodes
            .iter()
            .take(25)
            .map(|node| node["id"].as_str().unwrap())
            .collect();
        assert_eq!(first_ids, second_ids);
    }

    #[tokio::test]
    async fn graph_handler_rebuilds_dirty_degree_cache_before_ranking() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('repo', '/tmp/repo')",
            [],
        )
        .unwrap();
        let ids: Vec<String> = (0..505)
            .map(|i| insert_graph_node(&conn, "repo", i))
            .collect();
        for target in ids.iter().skip(1) {
            conn.execute(
                "INSERT INTO edges (source_id, target_id, relationship_type) VALUES (?1, ?2, 'CALLS')",
                rusqlite::params![ids[0], target],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO graph_node_degrees (repo_id, node_id, degree) VALUES ('repo', ?1, 9999)",
            rusqlite::params![ids[504]],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO graph_degree_cache_meta (repo_id, node_count, dirty, refreshed_at)
             VALUES ('repo', 505, 1, 0)",
            [],
        )
        .unwrap();

        let data = graph_json(graph_test_state(conn), "repo").await;
        assert_eq!(data["nodes"][0]["id"], ids[0]);
    }

    #[tokio::test]
    async fn graph_handler_returns_stable_capped_nodes_for_repo_without_edges() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('repo', '/tmp/repo')",
            [],
        )
        .unwrap();
        let ids: Vec<String> = (0..502)
            .map(|i| insert_graph_node(&conn, "repo", i))
            .collect();

        let data = graph_json(graph_test_state(conn), "repo").await;
        let nodes = data["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), GRAPH_INITIAL_NODE_CAP);
        assert_eq!(data["edges"].as_array().unwrap().len(), 0);
        assert_eq!(data["truncated"], true);
        assert_eq!(data["total_node_count"], 502);
        assert_eq!(nodes[0]["id"], ids[0]);
        assert_eq!(nodes[GRAPH_INITIAL_NODE_CAP - 1]["id"], ids[499]);
    }

    #[tokio::test]
    async fn graph_neighbors_returns_direct_neighbors_without_initial_cap() {
        let conn = crate::db::init_db(":memory:").unwrap();
        conn.execute(
            "INSERT INTO repositories (id, root_path) VALUES ('repo', '/tmp/repo')",
            [],
        )
        .unwrap();
        let ids: Vec<String> = (0..620)
            .map(|i| insert_graph_node(&conn, "repo", i))
            .collect();
        conn.execute(
            "INSERT INTO edges (source_id, target_id, relationship_type) VALUES (?1, ?2, 'CALLS'), (?1, ?3, 'CALLS')",
            rusqlite::params![ids[610], ids[611], ids[612]],
        )
        .unwrap();

        let data = graph_neighbors_json(graph_test_state(conn), &ids[610]).await;
        let returned: std::collections::HashSet<String> = data["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(returned.len(), 3);
        assert!(returned.contains(&ids[610]));
        assert!(returned.contains(&ids[611]));
        assert!(returned.contains(&ids[612]));
        assert_eq!(data["edges"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn legacy_graph_clients_can_ignore_optional_metadata() {
        #[derive(serde::Deserialize)]
        struct LegacyGraphResponse {
            nodes: Vec<serde_json::Value>,
            edges: Vec<serde_json::Value>,
            truncated: bool,
            total_node_count: i64,
        }

        let with_metadata = serde_json::json!({
            "nodes": [{"id": "n1"}],
            "edges": [],
            "truncated": true,
            "total_node_count": 2,
            "performance": {"initial_node_cap": 500, "degree_cache": "ready"},
            "future_metadata": {"ignored": true}
        });
        let without_metadata = serde_json::json!({
            "nodes": [],
            "edges": [],
            "truncated": false,
            "total_node_count": 0
        });

        let parsed_with: LegacyGraphResponse = serde_json::from_value(with_metadata).unwrap();
        let parsed_without: LegacyGraphResponse = serde_json::from_value(without_metadata).unwrap();

        assert_eq!(parsed_with.nodes.len(), 1);
        assert_eq!(parsed_with.edges.len(), 0);
        assert!(parsed_with.truncated);
        assert_eq!(parsed_with.total_node_count, 2);
        assert_eq!(parsed_without.nodes.len(), 0);
        assert_eq!(parsed_without.edges.len(), 0);
        assert!(!parsed_without.truncated);
        assert_eq!(parsed_without.total_node_count, 0);
    }

    #[test]
    fn dashboard_html_keeps_sidebar_rebuild_out_of_simulation_ticks() {
        let tick_handler = html_between("treeSim.on('tick'", "treeSim.on('end'");
        assert!(tick_handler.contains("drawFrame();"));
        assert!(!tick_handler.contains("treeRebuildDirTree"));

        let render_handler = html_between("function treeRenderPrepared", "function treeSetFocus");
        assert!(render_handler.contains("treeRebuildDirTree();"));
    }

    #[test]
    fn dashboard_html_has_progress_stale_load_and_render_failure_guards() {
        let select_repo = html_between("function treeSelectRepo", "function treeOnTabActivate");
        assert!(select_repo.contains("const loadId = ++treeLoadSeq"));
        assert!(select_repo.contains("treeLoadAbort.abort()"));
        assert!(select_repo.contains("treeSetLoadState('fetching'"));
        assert!(select_repo.contains("treeSetLoadState('preparing'"));
        assert!(select_repo.contains("treeSetLoadState('layout'"));
        assert!(select_repo.contains("if (loadId !== treeLoadSeq) return"));
        assert!(select_repo.contains("treeClearLoadState(loadId)"));
        assert!(select_repo.contains("treeShowLoadError(loadId, 'Render failed: ' + err.message)"));
        assert!(select_repo.contains("treeShowLoadError(loadId, 'Fetch failed: ' + err.message)"));

        let cache_refresh = html_between("function treeRefreshShapeCaches", "// ── Canvas init");
        assert!(cache_refresh.contains("const cadence = denseGraph && simAlpha > 0.06 ? 8 : 1"));
        assert!(cache_refresh.contains("treeFrameCounter - treeLastHullFrame < cadence"));
        assert!(cache_refresh.contains("treeLastHullFrame = treeFrameCounter"));
    }

    #[test]
    fn dashboard_html_lifetime_panel_hides_advanced_metrics_until_toggle() {
        let lifetime_panel = html_between(
            "<section id=\"panel-lifetime\"",
            "<!-- TREE VISUALIZATION panel -->",
        );
        assert!(lifetime_panel.contains("All-Time Requests"));
        assert!(lifetime_panel.contains("All-Time Tokens Saved"));
        assert!(lifetime_panel.contains("Lifetime Reduction %"));
        assert!(lifetime_panel.contains("Attached DB Size"));
        assert!(lifetime_panel.contains("Indexed Repos"));
        assert!(lifetime_panel.contains("Indexed Symbols"));
        assert!(lifetime_panel.contains("id=\"lifetime-advanced-toggle\""));
        assert!(lifetime_panel.contains("aria-expanded=\"false\""));
        assert!(lifetime_panel.contains("id=\"lifetime-advanced\" hidden"));
        assert!(lifetime_panel.contains("Pipeline Compliance %"));
        assert!(lifetime_panel.contains("Auto-Routed Bypasses"));
        assert!(lifetime_panel.contains("Rejected Bypasses"));
        assert!(lifetime_panel.contains("Ambiguous Symbol Guards"));
        assert!(lifetime_panel.contains("Stale Capsule Guards"));
        assert!(!lifetime_panel.contains("Indexed Files"));
        assert!(!lifetime_panel.contains("Root Path"));
    }

    #[test]
    fn dashboard_html_lifetime_advanced_toggle_uses_high_contrast_default_styling() {
        let toggle_style = html_between(
            "#lifetime-advanced-toggle {",
            "#lifetime-advanced-toggle:hover,",
        );
        assert!(toggle_style.contains("color: var(--surface);"));
        assert!(toggle_style.contains("font-weight: 600;"));
    }

    #[test]
    fn dashboard_html_session_table_resists_short_viewport_collapse() {
        let panel_style = html_between(".panel {", ".panel.active");
        assert!(panel_style.contains("overflow-y: auto;"));

        let table_style = html_between(".table-wrap {", ".table-wrap::-webkit-scrollbar");
        assert!(table_style.contains("flex: 1 0"));
        assert!(table_style.contains("min-height:"));
    }

    #[test]
    fn dashboard_html_compare_button_has_stable_hit_target() {
        let button_style = html_between(".view-delta-btn {", ".view-delta-btn:hover");
        assert!(button_style.contains("position: relative;"));
        assert!(button_style.contains("z-index: 1;"));
        assert!(button_style.contains("scroll-margin-top:"));

        let tool_label_style = html_between(".tool-label {", ".tool-label .sym");
        assert!(tool_label_style.contains("overflow: visible;"));
    }
}
