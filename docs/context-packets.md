# Context Packets

`marrow context` generates deterministic, structured context packets in markdown or JSON format. These packets are designed to be consumed by coding agents, IDEs, and analysis tools without requiring external vector databases, semantic search, or LLM API calls.

## Profiles

Marrow supports three built-in profiles that govern budget, condensation aggressiveness, and output verbosity:

| Profile | Token Budget | Use Case |
|---------|--------------|----------|
| `local-8k` | ~8,000 | Small local repos, quick queries, edge devices with limited memory |
| `local-32k` | ~32,000 | Standard local development, full codebase context, most IDEs |
| `cloud-cost-sensitive` | ~16,000 | Cloud deployment, cost-aware scenarios, fallback for LLM API constraints |

## Output Formats

### Markdown (`--format markdown`)

Human-readable markdown with:
- **Routing guidance:** Recommended starting points and key files
- **Source spans:** Exact file paths and line ranges for each symbol
- **Condensed neighbors:** Function/class signatures and type definitions (bodies elided)
- **Token accounting:** Estimated token consumption per section
- **Freshness metadata:** Ingest timestamp and graph version
- **Provenance:** Which repositories and ingestion runs contributed data

### JSON (`--format json`)

Structured JSON output with:
- `metadata` — packet timestamp, budget, profile, repo list
- `routing` — entry point symbols with file + line ranges
- `graph` — node and edge collections with full type information
- `tokens` — per-section and total token estimates
- `condensed_bodies` — signatures with `[...]` placeholders

## Invocation

### Basic Query

```bash
marrow context "trace request flow" \
  --repo my_repo \
  --budget 24000 \
  --format markdown \
  --profile local-32k
```

### Options

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `--repo REPO_ID` | string | Required | Repository identifier (as passed to `marrow index`) |
| `--budget TOKENS` | integer | 32000 | Token limit for the packet |
| `--format` | `markdown` \| `json` | `markdown` | Output format |
| `--profile` | `local-8k` \| `local-32k` \| `cloud-cost-sensitive` | `local-32k` | Condensation and scope preset |
| `--max-depth` | 0–10 | 3 | Maximum symbol-reference depth in the graph traversal |
| `--exclude-tests` | boolean flag | *(unset)* | Exclude test files from the context packet |

## Packet Contents

Each packet includes:

1. **Entry Points** — Symbols matching the query (ranked by relevance via symbol type and reference frequency)
2. **Condensed Definition Chain** — Symbols referenced by entry points (signatures preserved, bodies replaced with `[...]`)
3. **Import Chain** — Cross-file dependencies and module structure (if applicable to the language)
4. **Token Accounting** — Per-symbol and per-section estimates (for comparison across profiles and budgets)
5. **Metadata** — Packet generation timestamp, repository ingestion freshness, graph version, budget utilization

## Example Output (Markdown)

```markdown
# Context Packet: "trace request flow"

**Generated:** 2025-05-27 15:32:14 UTC  
**Repository:** my_repo  
**Profile:** local-32k  
**Budget:** 24,000 tokens | **Used:** 18,456 tokens | **Remaining:** 5,544 tokens

## Routing Guidance

Start here:

- **`src/api/handler.rs:RequestHandler::handle()`** — Entry point (line 42–89)
  - Processes incoming HTTP requests and dispatches to sub-modules.
  - Calls: `validate_request()`, `route_to_handler()`, `log_response()`.

## Condensed Neighbors

**`src/api/handler.rs`**

```rust
impl RequestHandler {
    pub fn handle(&mut self, req: Request) -> Result<Response> {
        // [body condensed — 120 tokens removed]
    }

    fn validate_request(&self, req: &Request) -> Result<()> {
        // [body condensed — 45 tokens removed]
    }
}
```

**`src/routing/dispatcher.rs`**

```rust
pub fn route_to_handler(task: &str) -> Box<dyn TaskHandler> {
    // [body condensed — 200 tokens removed]
}
```

## Token Breakdown

| Section | Tokens | % of Budget |
|---------|--------|-------------|
| Routing guidance | 850 | 4.6% |
| Entry point definition | 2,100 | 11.4% |
| Condensed neighbors | 12,450 | 67.4% |
| Import chain | 1,950 | 10.6% |
| Metadata | 1,106 | 6.0% |
| **Total** | **18,456** | **100%** |

## Graph Freshness

- **Last ingest:** 2025-05-27 14:58:00 UTC (34 minutes ago)
- **Files indexed:** 1,247
- **Symbols tracked:** 8,934
- **Edges:** 32,451

---

## Integration

Marrow context packets are consumed by:

- **Coding agents** (Claude Code, Cline, Cursor, GitHub Copilot) — passed to prompts as reference material
- **Custom analysis tools** — JSON format for programmatic consumption
- **Documentation generators** — markdown packets embedded in runbooks or RFCs
- **Performance profilers** — token accounting for cost estimation

No external APIs, no embeddings, no LLM calls required.
```
