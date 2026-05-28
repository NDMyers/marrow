use anyhow::{anyhow, Result};
use rusqlite::{Connection, OptionalExtension as _};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs,
    path::PathBuf,
};

use crate::{db, retrieval};

const PACKET_SCHEMA_VERSION: u32 = 1;
const PACKET_ENVELOPE_TOKENS: usize = 180;
const MIN_ENTRY_TOKENS: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextFormat {
    Markdown,
    Json,
}

impl ContextFormat {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "markdown" => Ok(Self::Markdown),
            "json" => Ok(Self::Json),
            other => Err(anyhow!(
                "unknown context format `{other}`; expected markdown or json"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Markdown => "markdown",
            Self::Json => "json",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ModelProfile {
    Local8k,
    Local32k,
    CloudCostSensitive,
}

impl ModelProfile {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "local-8k" => Ok(Self::Local8k),
            "local-32k" => Ok(Self::Local32k),
            "cloud-cost-sensitive" => Ok(Self::CloudCostSensitive),
            other => Err(anyhow!(
                "unknown context profile `{other}`; expected local-8k, local-32k, or cloud-cost-sensitive"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Local8k => "local-8k",
            Self::Local32k => "local-32k",
            Self::CloudCostSensitive => "cloud-cost-sensitive",
        }
    }

    fn behavior(self) -> ProfileBehavior {
        match self {
            Self::Local8k => ProfileBehavior {
                context_window_tokens: 8_000,
                budget_percent: 70,
                exact_entry_limit: 2,
                neighbor_limit: 1,
                condensed_max_chars: 240,
            },
            Self::Local32k => ProfileBehavior {
                context_window_tokens: 32_000,
                budget_percent: 100,
                exact_entry_limit: 4,
                neighbor_limit: 3,
                condensed_max_chars: 900,
            },
            Self::CloudCostSensitive => ProfileBehavior {
                context_window_tokens: 128_000,
                budget_percent: 60,
                exact_entry_limit: 3,
                neighbor_limit: 2,
                condensed_max_chars: 420,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProfileMetadata {
    pub name: String,
    pub context_window_tokens: usize,
    pub budget_percent: usize,
    pub exact_entry_limit: usize,
    pub neighbor_limit: usize,
    pub condensed_max_chars: usize,
}

#[derive(Debug, Clone, Copy)]
struct ProfileBehavior {
    context_window_tokens: usize,
    budget_percent: usize,
    exact_entry_limit: usize,
    neighbor_limit: usize,
    condensed_max_chars: usize,
}

impl ProfileBehavior {
    fn metadata(self, profile: ModelProfile) -> ProfileMetadata {
        ProfileMetadata {
            name: profile.as_str().to_string(),
            context_window_tokens: self.context_window_tokens,
            budget_percent: self.budget_percent,
            exact_entry_limit: self.exact_entry_limit,
            neighbor_limit: self.neighbor_limit,
            condensed_max_chars: self.condensed_max_chars,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRequest {
    pub task: String,
    pub repo_id: String,
    pub budget_tokens: usize,
    pub profile: ModelProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BudgetMetadata {
    pub requested_tokens: usize,
    pub effective_tokens: usize,
    pub entry_budget_tokens: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenAccounting {
    pub estimated_packet_tokens: usize,
    pub estimated_entry_tokens: usize,
    pub estimated_source_tokens: usize,
    pub estimated_omitted_tokens: usize,
    pub token_source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FreshnessMetadata {
    pub index_status: String,
    pub repo_root: Option<String>,
    pub indexed_file_count: usize,
    pub checked_file_count: usize,
    pub stale_file_count: usize,
    pub unavailable_file_count: usize,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PacketProvenance {
    pub compiler: String,
    pub graph_source: String,
    pub deterministic: bool,
    pub network_calls: bool,
    pub embeddings: bool,
    pub provider_sdks: bool,
    pub truncated: bool,
    pub omitted_entry_count: usize,
    pub truncation_reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingOutcome {
    UseMarrow,
    UseNative,
    Hybrid,
    NeedsIndex,
}

impl RoutingOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UseMarrow => "use_marrow",
            Self::UseNative => "use_native",
            Self::Hybrid => "hybrid",
            Self::NeedsIndex => "needs_index",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutingDecision {
    pub outcome: RoutingOutcome,
    pub rationale: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceSpan {
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_line: usize,
    pub start_column: usize,
    pub end_line: usize,
    pub end_column: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContextEntryType {
    ExactSource,
    CondensedStructure,
}

impl ContextEntryType {
    fn order_key(self) -> u8 {
        match self {
            Self::ExactSource => 0,
            Self::CondensedStructure => 1,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::ExactSource => "exact_source",
            Self::CondensedStructure => "condensed_structure",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntryProvenance {
    pub sources: Vec<String>,
    pub rationale: Vec<String>,
    pub freshness: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RankedContextEntry {
    pub rank: usize,
    pub context_type: ContextEntryType,
    pub file_path: String,
    pub symbol_name: String,
    pub symbol_type: String,
    pub language: String,
    pub relationship: Option<String>,
    pub score: i64,
    pub span: Option<SourceSpan>,
    pub source_text: Option<String>,
    pub condensed_text: Option<String>,
    pub estimated_tokens: usize,
    pub provenance: EntryProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextPacket {
    pub schema_version: u32,
    pub task: String,
    pub repo_id: String,
    pub budget: BudgetMetadata,
    pub profile: ProfileMetadata,
    pub routing: RoutingDecision,
    pub ranked_entries: Vec<RankedContextEntry>,
    pub token_accounting: TokenAccounting,
    pub freshness: FreshnessMetadata,
    pub provenance: PacketProvenance,
}

impl ContextPacket {
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        writeln!(out, "# Marrow Context Packet").ok();
        writeln!(out).ok();
        writeln!(out, "Task: {}", self.task).ok();
        writeln!(out, "Repo: {}", self.repo_id).ok();
        writeln!(
            out,
            "Budget: requested={} effective={} entry_budget={}",
            self.budget.requested_tokens,
            self.budget.effective_tokens,
            self.budget.entry_budget_tokens
        )
        .ok();
        writeln!(out, "Profile: {}", self.profile.name).ok();
        writeln!(
            out,
            "Routing: {} - {}",
            self.routing.outcome.as_str(),
            self.routing.rationale
        )
        .ok();

        writeln!(out, "\n## Freshness").ok();
        writeln!(out, "- status: {}", self.freshness.index_status).ok();
        writeln!(
            out,
            "- repo_root: {}",
            self.freshness.repo_root.as_deref().unwrap_or("unavailable")
        )
        .ok();
        writeln!(
            out,
            "- files: indexed={} checked={} stale={} unavailable={}",
            self.freshness.indexed_file_count,
            self.freshness.checked_file_count,
            self.freshness.stale_file_count,
            self.freshness.unavailable_file_count
        )
        .ok();
        for note in &self.freshness.notes {
            writeln!(out, "- note: {note}").ok();
        }

        writeln!(out, "\n## Token Accounting").ok();
        writeln!(
            out,
            "- estimated_packet_tokens: {}",
            self.token_accounting.estimated_packet_tokens
        )
        .ok();
        writeln!(
            out,
            "- estimated_entry_tokens: {}",
            self.token_accounting.estimated_entry_tokens
        )
        .ok();
        writeln!(
            out,
            "- estimated_source_tokens: {}",
            self.token_accounting.estimated_source_tokens
        )
        .ok();
        writeln!(
            out,
            "- estimated_omitted_tokens: {}",
            self.token_accounting.estimated_omitted_tokens
        )
        .ok();
        writeln!(
            out,
            "- token_source: {}",
            self.token_accounting.token_source
        )
        .ok();

        writeln!(out, "\n## Ranked Context").ok();
        if self.ranked_entries.is_empty() {
            writeln!(out, "No ranked source entries included.").ok();
        }
        for entry in &self.ranked_entries {
            writeln!(
                out,
                "\n### {}. {} ({})",
                entry.rank,
                entry.symbol_name,
                entry.context_type.as_str()
            )
            .ok();
            writeln!(out, "- file: {}", entry.file_path).ok();
            writeln!(out, "- type: {}", entry.symbol_type).ok();
            writeln!(out, "- language: {}", entry.language).ok();
            if let Some(relationship) = &entry.relationship {
                writeln!(out, "- relationship: {relationship}").ok();
            }
            if let Some(span) = &entry.span {
                writeln!(out, "- bytes: {}..{}", span.start_byte, span.end_byte).ok();
                writeln!(
                    out,
                    "- lines: {}:{}..{}:{}",
                    span.start_line, span.start_column, span.end_line, span.end_column
                )
                .ok();
            }
            writeln!(out, "- freshness: {}", entry.provenance.freshness).ok();
            writeln!(
                out,
                "- rationale: {}",
                entry.provenance.rationale.join("; ")
            )
            .ok();
            let body = entry.source_text.as_ref().or(entry.condensed_text.as_ref());
            if let Some(body) = body {
                writeln!(out, "```{}", entry.language).ok();
                writeln!(out, "{body}").ok();
                writeln!(out, "```").ok();
            }
        }

        writeln!(out, "\n## Provenance").ok();
        writeln!(out, "- compiler: {}", self.provenance.compiler).ok();
        writeln!(out, "- graph_source: {}", self.provenance.graph_source).ok();
        writeln!(out, "- deterministic: {}", self.provenance.deterministic).ok();
        writeln!(out, "- network_calls: {}", self.provenance.network_calls).ok();
        writeln!(out, "- embeddings: {}", self.provenance.embeddings).ok();
        writeln!(out, "- provider_sdks: {}", self.provenance.provider_sdks).ok();
        writeln!(out, "- truncated: {}", self.provenance.truncated).ok();
        writeln!(
            out,
            "- omitted_entry_count: {}",
            self.provenance.omitted_entry_count
        )
        .ok();
        for reason in &self.provenance.truncation_reasons {
            writeln!(out, "- truncation_reason: {reason}").ok();
        }

        out
    }
}

#[derive(Debug, Clone)]
struct NodeCandidate {
    id: String,
    file_path: String,
    language: String,
    symbol_name: String,
    symbol_type: String,
    raw_text: String,
    span: SourceSpan,
    degree: i64,
    score: i64,
}

#[allow(dead_code)]
pub fn compile_context_packet(conn: &Connection, request: ContextRequest) -> Result<ContextPacket> {
    compile_context_packet_for_format(conn, request, ContextFormat::Markdown)
}

pub fn compile_context_packet_for_format(
    conn: &Connection,
    request: ContextRequest,
    output_format: ContextFormat,
) -> Result<ContextPacket> {
    let behavior = request.profile.behavior();
    let task_terms = task_terms(&request.task);
    let node_count = repo_node_count(conn, &request.repo_id)?;
    let freshness = freshness_metadata(conn, &request.repo_id, node_count)?;
    let routing = if node_count == 0 {
        RoutingDecision {
            outcome: RoutingOutcome::NeedsIndex,
            rationale: "repo has no indexed graph nodes".to_string(),
        }
    } else {
        route_task(conn, &request.repo_id, &request.task, &task_terms)?
    };

    let requested_tokens = request.budget_tokens.max(1);
    let effective_tokens = requested_tokens
        .min(behavior.context_window_tokens)
        .saturating_mul(behavior.budget_percent)
        / 100;
    let entry_budget_tokens = effective_tokens.saturating_sub(PACKET_ENVELOPE_TOKENS);
    let budget = BudgetMetadata {
        requested_tokens,
        effective_tokens,
        entry_budget_tokens,
    };

    let mut provenance = PacketProvenance {
        compiler: "marrow context v1".to_string(),
        graph_source: "sqlite:nodes,edges,files".to_string(),
        deterministic: true,
        network_calls: false,
        embeddings: false,
        provider_sdks: false,
        truncated: false,
        omitted_entry_count: 0,
        truncation_reasons: Vec::new(),
    };

    let mut candidate_entries = if routing.outcome == RoutingOutcome::NeedsIndex {
        Vec::new()
    } else {
        build_ranked_candidates(conn, &request.repo_id, &task_terms, behavior, &freshness)?
    };

    if entry_budget_tokens < MIN_ENTRY_TOKENS && !candidate_entries.is_empty() {
        provenance.truncated = true;
        provenance.omitted_entry_count = candidate_entries.len();
        provenance
            .truncation_reasons
            .push("budget below minimum packet envelope; omitted all source entries".to_string());
        candidate_entries.clear();
    }

    let (
        mut ranked_entries,
        estimated_entry_tokens,
        estimated_source_tokens,
        omitted_tokens,
        omitted_count,
    ) = enforce_entry_budget(candidate_entries, entry_budget_tokens);
    if omitted_count > 0 {
        provenance.truncated = true;
        provenance.omitted_entry_count += omitted_count;
        provenance.truncation_reasons.push(format!(
            "omitted {omitted_count} lower-ranked entries to respect budget"
        ));
    }
    for (idx, entry) in ranked_entries.iter_mut().enumerate() {
        entry.rank = idx + 1;
    }

    let mut packet = ContextPacket {
        schema_version: PACKET_SCHEMA_VERSION,
        task: request.task,
        repo_id: request.repo_id,
        budget,
        profile: behavior.metadata(request.profile),
        routing,
        ranked_entries,
        token_accounting: TokenAccounting {
            estimated_packet_tokens: 0,
            estimated_entry_tokens,
            estimated_source_tokens,
            estimated_omitted_tokens: omitted_tokens,
            token_source: format!(
                "chars/4 estimate over emitted {} packet",
                output_format.as_str()
            ),
        },
        freshness,
        provenance,
    };

    enforce_emitted_packet_budget(&mut packet, output_format)?;
    Ok(packet)
}

fn stabilize_packet_token_count(
    packet: &mut ContextPacket,
    output_format: ContextFormat,
) -> Result<()> {
    for _ in 0..4 {
        let estimate = estimate_tokens(&render_packet(packet, output_format)?);
        if packet.token_accounting.estimated_packet_tokens == estimate {
            return Ok(());
        }
        packet.token_accounting.estimated_packet_tokens = estimate;
    }
    Ok(())
}

fn render_packet(packet: &ContextPacket, output_format: ContextFormat) -> Result<String> {
    match output_format {
        ContextFormat::Markdown => Ok(packet.to_markdown()),
        ContextFormat::Json => Ok(packet.to_json()?),
    }
}

fn enforce_emitted_packet_budget(
    packet: &mut ContextPacket,
    output_format: ContextFormat,
) -> Result<()> {
    stabilize_packet_token_count(packet, output_format)?;

    let mut omitted_for_emitted_budget = false;
    while packet.token_accounting.estimated_packet_tokens > packet.budget.effective_tokens {
        let Some(omitted) = packet.ranked_entries.pop() else {
            packet.provenance.truncated = true;
            add_truncation_reason(
                &mut packet.provenance,
                format!(
                    "emitted {} packet envelope exceeds effective budget after omitting all source entries",
                    output_format.as_str()
                ),
            );
            stabilize_packet_token_count(packet, output_format)?;
            break;
        };

        packet.provenance.truncated = true;
        packet.provenance.omitted_entry_count += 1;
        packet.token_accounting.estimated_omitted_tokens = packet
            .token_accounting
            .estimated_omitted_tokens
            .saturating_add(omitted.estimated_tokens);
        refresh_entry_token_accounting(packet);
        assign_ranks(&mut packet.ranked_entries);
        if !omitted_for_emitted_budget {
            omitted_for_emitted_budget = true;
            add_truncation_reason(
                &mut packet.provenance,
                format!(
                    "omitted lower-ranked entries to respect emitted {} packet budget",
                    output_format.as_str()
                ),
            );
        }
        stabilize_packet_token_count(packet, output_format)?;
    }

    Ok(())
}

fn add_truncation_reason(provenance: &mut PacketProvenance, reason: String) {
    if !provenance
        .truncation_reasons
        .iter()
        .any(|existing| existing == &reason)
    {
        provenance.truncation_reasons.push(reason);
    }
}

fn assign_ranks(entries: &mut [RankedContextEntry]) {
    for (idx, entry) in entries.iter_mut().enumerate() {
        entry.rank = idx + 1;
    }
}

fn refresh_entry_token_accounting(packet: &mut ContextPacket) {
    let (entry_tokens, source_tokens) = entry_token_totals(&packet.ranked_entries);
    packet.token_accounting.estimated_entry_tokens = entry_tokens;
    packet.token_accounting.estimated_source_tokens = source_tokens;
}

fn entry_token_totals(entries: &[RankedContextEntry]) -> (usize, usize) {
    let entry_tokens = entries.iter().map(|entry| entry.estimated_tokens).sum();
    let source_tokens = entries
        .iter()
        .map(|entry| {
            entry
                .source_text
                .as_deref()
                .or(entry.condensed_text.as_deref())
                .map(estimate_tokens)
                .unwrap_or(0)
        })
        .sum();
    (entry_tokens, source_tokens)
}

fn repo_node_count(conn: &Connection, repo_id: &str) -> Result<usize> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM nodes WHERE repo_id = ?1",
        rusqlite::params![repo_id],
        |row| row.get::<_, i64>(0),
    )? as usize)
}

fn task_terms(task: &str) -> Vec<String> {
    let stopwords = [
        "the", "and", "for", "with", "from", "into", "this", "that", "what", "when", "where",
        "why", "how", "does", "will", "would", "should", "could", "code", "repo", "project",
    ];
    let mut seen = BTreeSet::new();
    let mut current = String::new();
    for ch in task.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            if current.len() >= 3 && !stopwords.contains(&current.as_str()) {
                seen.insert(std::mem::take(&mut current));
            } else {
                current.clear();
            }
        }
    }
    if !current.is_empty() && current.len() >= 3 && !stopwords.contains(&current.as_str()) {
        seen.insert(current);
    }
    seen.into_iter().collect()
}

fn route_task(
    conn: &Connection,
    repo_id: &str,
    task: &str,
    terms: &[String],
) -> Result<RoutingDecision> {
    let lowered = task.to_ascii_lowercase();
    let broad = [
        "whole",
        "entire",
        "all",
        "every",
        "codebase",
        "repository",
        "overview",
    ]
    .iter()
    .any(|needle| lowered.contains(needle));
    let targeted = [
        "trace", "flow", "caller", "callee", "impact", "refactor", "symbol", "function",
    ]
    .iter()
    .any(|needle| lowered.contains(needle));
    let best_score = best_task_score(conn, repo_id, terms)?;

    let (outcome, rationale) = if broad && best_score <= 3 {
        (
            RoutingOutcome::UseNative,
            "broad task with weak indexed-symbol match; native exploration is safer".to_string(),
        )
    } else if broad {
        (
            RoutingOutcome::Hybrid,
            "broad task has some graph matches, but native read/search may still be needed"
                .to_string(),
        )
    } else if targeted && best_score > 0 {
        (
            RoutingOutcome::UseMarrow,
            "targeted structural task with indexed graph coverage".to_string(),
        )
    } else if best_score > 0 {
        (
            RoutingOutcome::Hybrid,
            "lexical graph matches found, but task is not clearly structural".to_string(),
        )
    } else {
        (
            RoutingOutcome::UseNative,
            "no lexical graph-backed source match found".to_string(),
        )
    };

    Ok(RoutingDecision { outcome, rationale })
}

fn best_task_score(conn: &Connection, repo_id: &str, terms: &[String]) -> Result<i64> {
    Ok(load_nodes(conn, repo_id)?
        .into_iter()
        .map(|node| score_node(&node, terms))
        .max()
        .unwrap_or(0))
}

fn build_ranked_candidates(
    conn: &Connection,
    repo_id: &str,
    terms: &[String],
    behavior: ProfileBehavior,
    freshness: &FreshnessMetadata,
) -> Result<Vec<RankedContextEntry>> {
    let mut nodes = load_nodes(conn, repo_id)?;
    for node in &mut nodes {
        node.score = score_node(node, terms);
    }
    nodes.sort_by(|a, b| {
        b.score
            .cmp(&a.score)
            .then_with(|| b.degree.cmp(&a.degree))
            .then_with(|| a.file_path.cmp(&b.file_path))
            .then_with(|| a.symbol_name.cmp(&b.symbol_name))
            .then_with(|| a.id.cmp(&b.id))
    });

    let mut entries = BTreeMap::<String, RankedContextEntry>::new();
    for node in nodes
        .iter()
        .filter(|node| node.score > 0)
        .take(behavior.exact_entry_limit)
    {
        entries.insert(
            node.id.clone(),
            exact_entry(node, freshness_for_file(freshness, &node.file_path)),
        );
        for neighbor in load_neighbors(conn, &node.id, behavior.neighbor_limit)? {
            entries
                .entry(neighbor.id.clone())
                .and_modify(|entry| {
                    merge_rationale(entry, "also reached from graph neighbor traversal")
                })
                .or_insert_with(|| {
                    condensed_entry(
                        &neighbor,
                        freshness_for_file(freshness, &neighbor.file_path),
                        behavior.condensed_max_chars,
                    )
                });
        }
    }

    let mut ordered: Vec<_> = entries.into_values().collect();
    ordered.sort_by(|a, b| {
        a.context_type
            .order_key()
            .cmp(&b.context_type.order_key())
            .then_with(|| b.score.cmp(&a.score))
            .then_with(|| a.file_path.cmp(&b.file_path))
            .then_with(|| a.symbol_name.cmp(&b.symbol_name))
    });
    Ok(ordered)
}

fn load_nodes(conn: &Connection, repo_id: &str) -> Result<Vec<NodeCandidate>> {
    let mut stmt = conn.prepare(
        "SELECT n.id, n.file_path, n.language, n.symbol_name, n.symbol_type, n.raw_text,
                n.source_start_byte, n.source_end_byte, n.start_line, n.start_column,
                n.end_line, n.end_column, COALESCE(g.degree, 0)
         FROM nodes n
         LEFT JOIN graph_node_degrees g ON g.repo_id = n.repo_id AND g.node_id = n.id
         WHERE n.repo_id = ?1
         ORDER BY n.file_path ASC, n.symbol_name ASC, n.id ASC",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![repo_id], |row| {
            let id: String = row.get(0)?;
            let raw_text: String = row.get(5)?;
            let source_start_byte = row.get::<_, i64>(6)?.max(0) as usize;
            let source_end_byte = row.get::<_, i64>(7)?.max(0) as usize;
            let start_byte = if source_start_byte == 0 {
                parse_node_start_byte(&id).unwrap_or(0)
            } else {
                source_start_byte
            };
            let end_byte = if source_end_byte > start_byte {
                source_end_byte
            } else {
                start_byte.saturating_add(raw_text.len())
            };
            Ok(NodeCandidate {
                id,
                file_path: row.get(1)?,
                language: row.get(2)?,
                symbol_name: row.get(3)?,
                symbol_type: row.get(4)?,
                raw_text,
                span: SourceSpan {
                    start_byte,
                    end_byte,
                    start_line: row.get::<_, i64>(8)?.max(0) as usize,
                    start_column: row.get::<_, i64>(9)?.max(0) as usize,
                    end_line: row.get::<_, i64>(10)?.max(0) as usize,
                    end_column: row.get::<_, i64>(11)?.max(0) as usize,
                },
                degree: row.get::<_, i64>(12)?,
                score: 0,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn parse_node_start_byte(id: &str) -> Option<usize> {
    id.rsplit(':').next()?.parse().ok()
}

fn score_node(node: &NodeCandidate, terms: &[String]) -> i64 {
    if terms.is_empty() {
        return 0;
    }
    let symbol = node.symbol_name.to_ascii_lowercase();
    let file_path = node.file_path.to_ascii_lowercase();
    let symbol_type = node.symbol_type.to_ascii_lowercase();
    let raw_text = node.raw_text.to_ascii_lowercase();
    let mut score = 0;
    for term in terms {
        if symbol.contains(term) {
            score += 12;
        }
        if file_path.contains(term) {
            score += 6;
        }
        if symbol_type.contains(term) {
            score += 2;
        }
        score += raw_text.matches(term).count().min(5) as i64;
    }
    score
}

fn load_neighbors(conn: &Connection, node_id: &str, limit: usize) -> Result<Vec<NodeCandidate>> {
    let mut stmt = conn.prepare(
        "SELECT n.id, n.file_path, n.language, n.symbol_name, n.symbol_type, n.raw_text,
                n.source_start_byte, n.source_end_byte, n.start_line, n.start_column,
                n.end_line, n.end_column, COALESCE(g.degree, 0)
         FROM edges e
         JOIN nodes n ON n.id = CASE WHEN e.source_id = ?1 THEN e.target_id ELSE e.source_id END
         LEFT JOIN graph_node_degrees g ON g.repo_id = n.repo_id AND g.node_id = n.id
         WHERE (e.source_id = ?1 OR e.target_id = ?1) AND n.id != ?1
         ORDER BY CASE WHEN e.source_id = ?1 THEN 0 ELSE 1 END ASC,
                  e.relationship_type ASC, n.symbol_name ASC, n.file_path ASC, n.id ASC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(rusqlite::params![node_id, limit as i64], |row| {
            let id: String = row.get(0)?;
            let raw_text: String = row.get(5)?;
            let source_start_byte = row.get::<_, i64>(6)?.max(0) as usize;
            let source_end_byte = row.get::<_, i64>(7)?.max(0) as usize;
            let start_byte = if source_start_byte == 0 {
                parse_node_start_byte(&id).unwrap_or(0)
            } else {
                source_start_byte
            };
            let end_byte = if source_end_byte > start_byte {
                source_end_byte
            } else {
                start_byte.saturating_add(raw_text.len())
            };
            Ok(NodeCandidate {
                id,
                file_path: row.get(1)?,
                language: row.get(2)?,
                symbol_name: row.get(3)?,
                symbol_type: row.get(4)?,
                raw_text,
                span: SourceSpan {
                    start_byte,
                    end_byte,
                    start_line: row.get::<_, i64>(8)?.max(0) as usize,
                    start_column: row.get::<_, i64>(9)?.max(0) as usize,
                    end_line: row.get::<_, i64>(10)?.max(0) as usize,
                    end_column: row.get::<_, i64>(11)?.max(0) as usize,
                },
                degree: row.get::<_, i64>(12)?,
                score: 1,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

fn exact_entry(node: &NodeCandidate, freshness: String) -> RankedContextEntry {
    if freshness == "unavailable" {
        return metadata_only_entry(
            node,
            None,
            freshness,
            "lexical task match kept as graph metadata",
        );
    }

    let source_truncated = node.raw_text.contains("[MARROW: body truncated");
    RankedContextEntry {
        rank: 0,
        context_type: ContextEntryType::ExactSource,
        file_path: node.file_path.clone(),
        symbol_name: node.symbol_name.clone(),
        symbol_type: node.symbol_type.clone(),
        language: node.language.clone(),
        relationship: None,
        score: node.score,
        span: Some(node.span.clone()),
        source_text: Some(node.raw_text.clone()),
        condensed_text: None,
        estimated_tokens: estimate_tokens(&node.raw_text).saturating_add(32),
        provenance: EntryProvenance {
            sources: vec![
                "nodes.raw_text".to_string(),
                "nodes.source_span".to_string(),
            ],
            rationale: vec!["lexical task match ranked as exact source".to_string()],
            freshness,
            truncated: source_truncated,
        },
    }
}

fn condensed_entry(
    node: &NodeCandidate,
    freshness: String,
    max_chars: usize,
) -> RankedContextEntry {
    if freshness == "unavailable" {
        return metadata_only_entry(
            node,
            Some("graph_neighbor".to_string()),
            freshness,
            "graph neighbor kept as metadata only",
        );
    }

    let mut condensed = retrieval::condense(&node.raw_text, &node.language);
    let mut truncated = false;
    if condensed.len() > max_chars {
        condensed = prefix_by_bytes(&condensed, max_chars).to_string();
        condensed.push_str("\n[condensed structural context truncated by profile]");
        truncated = true;
    }
    let estimated_tokens = estimate_tokens(&condensed).saturating_add(24);
    RankedContextEntry {
        rank: 0,
        context_type: ContextEntryType::CondensedStructure,
        file_path: node.file_path.clone(),
        symbol_name: node.symbol_name.clone(),
        symbol_type: node.symbol_type.clone(),
        language: node.language.clone(),
        relationship: Some("graph_neighbor".to_string()),
        score: node.score,
        span: None,
        source_text: None,
        condensed_text: Some(condensed),
        estimated_tokens,
        provenance: EntryProvenance {
            sources: vec![
                "edges graph".to_string(),
                "nodes.raw_text condensed".to_string(),
            ],
            rationale: vec!["graph neighbor included as condensed structure".to_string()],
            freshness,
            truncated,
        },
    }
}

fn metadata_only_entry(
    node: &NodeCandidate,
    relationship: Option<String>,
    freshness: String,
    rationale: &str,
) -> RankedContextEntry {
    let mut sources = vec!["nodes metadata".to_string()];
    if relationship.is_some() {
        sources.insert(0, "edges graph".to_string());
    }
    RankedContextEntry {
        rank: 0,
        context_type: ContextEntryType::CondensedStructure,
        file_path: node.file_path.clone(),
        symbol_name: node.symbol_name.clone(),
        symbol_type: node.symbol_type.clone(),
        language: node.language.clone(),
        relationship,
        score: node.score,
        span: None,
        source_text: None,
        condensed_text: None,
        estimated_tokens: 24,
        provenance: EntryProvenance {
            sources,
            rationale: vec![
                rationale.to_string(),
                "provenance error: source file unavailable; source body omitted".to_string(),
            ],
            freshness,
            truncated: false,
        },
    }
}

fn merge_rationale(entry: &mut RankedContextEntry, rationale: &str) {
    if !entry
        .provenance
        .rationale
        .iter()
        .any(|existing| existing == rationale)
    {
        entry.provenance.rationale.push(rationale.to_string());
    }
}

fn enforce_entry_budget(
    candidates: Vec<RankedContextEntry>,
    entry_budget_tokens: usize,
) -> (Vec<RankedContextEntry>, usize, usize, usize, usize) {
    let mut used = 0usize;
    let mut source_tokens = 0usize;
    let mut omitted_tokens = 0usize;
    let mut omitted_count = 0usize;
    let mut entries = Vec::new();
    for entry in candidates {
        if used.saturating_add(entry.estimated_tokens) <= entry_budget_tokens {
            used += entry.estimated_tokens;
            if let Some(source_text) = &entry.source_text {
                source_tokens += estimate_tokens(source_text);
            } else if let Some(condensed_text) = &entry.condensed_text {
                source_tokens += estimate_tokens(condensed_text);
            }
            entries.push(entry);
        } else {
            omitted_tokens += entry.estimated_tokens;
            omitted_count += 1;
        }
    }
    (entries, used, source_tokens, omitted_tokens, omitted_count)
}

fn freshness_for_file(freshness: &FreshnessMetadata, file_path: &str) -> String {
    if freshness
        .notes
        .iter()
        .any(|note| file_note_matches(note, "unavailable file: ", file_path))
    {
        "unavailable".to_string()
    } else if freshness
        .notes
        .iter()
        .any(|note| file_note_matches(note, "stale file: ", file_path))
    {
        "stale".to_string()
    } else if freshness.repo_root.is_none()
        || freshness.index_status == "missing"
        || freshness.indexed_file_count == 0
    {
        freshness.index_status.clone()
    } else {
        "fresh".to_string()
    }
}

fn file_note_matches(note: &str, prefix: &str, file_path: &str) -> bool {
    note.strip_prefix(prefix)
        .is_some_and(|rest| rest == file_path || rest.starts_with(&format!("{file_path} ")))
}

fn freshness_metadata(
    conn: &Connection,
    repo_id: &str,
    node_count: usize,
) -> Result<FreshnessMetadata> {
    let repo_root = conn
        .query_row(
            "SELECT root_path FROM repositories WHERE id = ?1",
            rusqlite::params![repo_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?;

    if node_count == 0 {
        return Ok(FreshnessMetadata {
            index_status: "missing".to_string(),
            repo_root,
            indexed_file_count: 0,
            checked_file_count: 0,
            stale_file_count: 0,
            unavailable_file_count: 0,
            notes: vec!["no indexed nodes for requested repo".to_string()],
        });
    }

    let Some(root) = repo_root.clone() else {
        return Ok(FreshnessMetadata {
            index_status: "unavailable".to_string(),
            repo_root: None,
            indexed_file_count: 0,
            checked_file_count: 0,
            stale_file_count: 0,
            unavailable_file_count: 0,
            notes: vec!["repository root path is unavailable".to_string()],
        });
    };

    let root_path = PathBuf::from(&root);
    let mut stmt = conn.prepare(
        "SELECT file_path, content_hash FROM files WHERE repo_id = ?1 ORDER BY file_path ASC",
    )?;
    let files = stmt
        .query_map(rusqlite::params![repo_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let indexed_file_count = files.len();
    let mut checked_file_count = 0usize;
    let mut stale_file_count = 0usize;
    let mut unavailable_file_count = 0usize;
    let mut notes = Vec::new();
    for (file_path, expected_hash) in files {
        let abs_path = root_path.join(&file_path);
        match fs::read(&abs_path) {
            Ok(bytes) => {
                checked_file_count += 1;
                if std::str::from_utf8(&bytes).is_err() {
                    unavailable_file_count += 1;
                    notes.push(format!("unavailable file: {file_path} (non-utf8 source)"));
                    continue;
                }
                let actual_hash = db::hash_file_content(&bytes);
                if actual_hash != expected_hash {
                    stale_file_count += 1;
                    notes.push(format!("stale file: {file_path}"));
                }
            }
            Err(_) => {
                unavailable_file_count += 1;
                notes.push(format!("unavailable file: {file_path}"));
            }
        }
    }

    let index_status = if stale_file_count > 0 {
        "stale"
    } else if unavailable_file_count > 0 || indexed_file_count == 0 {
        "unavailable"
    } else {
        "fresh"
    };

    Ok(FreshnessMetadata {
        index_status: index_status.to_string(),
        repo_root: Some(root),
        indexed_file_count,
        checked_file_count,
        stale_file_count,
        unavailable_file_count,
        notes,
    })
}

fn estimate_tokens(text: &str) -> usize {
    text.len().div_ceil(4)
}

fn prefix_by_bytes(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let end = text
        .char_indices()
        .take_while(|(idx, _)| *idx < max_bytes)
        .last()
        .map(|(idx, ch)| idx + ch.len_utf8())
        .unwrap_or(0)
        .min(text.len());
    &text[..end]
}
