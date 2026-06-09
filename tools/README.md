# `cc_audit.py` — Claude Code token & cost auditor

A Copilot-style "agent debug log" for Claude Code sessions. Reads a session
transcript and prints, per request: exact token counts (fresh input / cached-read
input / cache-write input / output), the cost using **live-fetched** current
Anthropic API pricing, and the tool-call chain — plus session totals broken down
by category, by model, and by tool.

Built for A/B benchmarking Marrow: run a conversation with Marrow on and one with
it off, then audit each session and compare the totals.

Pure Python 3 standard library — no `pip install`.

## Where the data comes from

Claude Code already writes every session to disk as JSONL at:

```
~/.claude/projects/<project-slug>/<session-uuid>.jsonl
```

The slug is the repo's absolute path with separators folded to dashes; cc_audit
derives it from the current working directory automatically (override with
`--project`). Each assistant message carries a `usage`
object (`input_tokens`, `output_tokens`, `cache_read_input_tokens`,
`cache_creation.ephemeral_5m_input_tokens` / `ephemeral_1h_input_tokens`,
`server_tool_use`) and a per-message `model`, so cost is computed per model
(the main loop is Opus; sub-agents like Explore run on Haiku).

## Usage

```bash
# List every session in this project with a summary cost
python tools/cc_audit.py --list

# Audit the most recently modified session (default)
python tools/cc_audit.py --latest

# Audit a specific session (uuid prefix is enough)
python tools/cc_audit.py --session 70fdb069

# Fuller tool-call args + write a markdown copy
python tools/cc_audit.py --session f51198f7 --verbose --markdown report.md

# Force a fresh pricing pull (ignore the 24h cache)
python tools/cc_audit.py --refresh --latest

# Compare two sessions side by side with deltas (A = baseline, B = candidate)
python tools/cc_audit.py --compare <uuidA> <uuidB>
python tools/cc_audit.py --compare a.jsonl b.jsonl --markdown diff.md

# Audit a different project, or an explicit file
python tools/cc_audit.py --project some-other-slug --list
python tools/cc_audit.py --file /path/to/session.jsonl
```

### A/B benchmark workflow

1. Run your "Marrow on" task as one Claude Code session, "Marrow off" as another.
2. `python tools/cc_audit.py --list` to find both session uuids.
3. `python tools/cc_audit.py --compare <off-uuid> <on-uuid>` for a side-by-side
   diff: tokens (fresh/cached-read/cache-write/output), cost per category and
   total, request/tool-call counts, and a per-tool frequency delta. A is the
   baseline, B the candidate; Δ% is `(B-A)/A`. Reductions render green, increases
   yellow. Add `--markdown diff.md` to save it.

   (For the full per-request drill-down on a single run, audit it directly without
   `--compare`.)

## Pricing

Two sources, merged:

1. **Live (primary):** LiteLLM's machine-readable
   `model_prices_and_context_window.json` is fetched at runtime and cached for 24h
   under `tools/.cache/` (gitignored). `--refresh` forces a re-fetch. If the network
   is down, the auditor falls back to the cache, then to bundled defaults.
2. **Override (canonical):** `tools/pricing_overrides.json` — hand-maintained from
   the official page <https://platform.claude.com/docs/en/about-claude/pricing>.
   **Values here win over the live feed**, so use it to pin official rates or fix a
   stale/missing LiteLLM entry. Keys may be a full model id (`claude-opus-4-8`) or a
   family-version (`claude-haiku-4-5`). Costs are **per token** (per-million ÷ 1e6).

Rate fields (all per token, USD):

| transcript field                       | pricing field                                | multiple |
| --------------------------------------- | --------------------------------------------- | -------- |
| `input_tokens`                          | `input_cost_per_token`                        | 1×       |
| `output_tokens`                         | `output_cost_per_token`                       | —        |
| `cache_read_input_tokens`               | `cache_read_input_token_cost`                 | ~0.1×    |
| `ephemeral_5m_input_tokens`             | `cache_creation_input_token_cost`             | 1.25×    |
| `ephemeral_1h_input_tokens`             | `cache_creation_input_token_cost` (see below) | 1.25×    |

**Cache-write pricing:** Claude Code uses a 1-hour cache TTL, so most cache-write
tokens land in `ephemeral_1h_input_tokens`. Although Anthropic's published 1h-write
tier is 2× input, Claude Code's own reported cost (`total_cost_usd`) bills these at
the standard 1.25× rate — verified by reconciling cc_audit against the CLI to within
~0.2% on single-model sessions. cc_audit therefore prices **both** cache-write
buckets at the standard `cache_creation_input_token_cost`, and still shows the
5m/1h token *counts* separately. (The `_above_1hr` override field is retained for
model resolution but not used for pricing.)

Models with no matching rate are reported explicitly (cost `?`) and excluded from
the TOTAL — they're never silently counted as $0.

## Accuracy

Validated against Claude Code's own `--output-format json` `total_cost_usd` on five
real sessions: **0.998–0.999** agreement on single-model sessions. Cross-model
sessions that spawn sub-agents (which Anthropic bills with shared caching across the
agent boundary) run slightly conservative (~0.9). Two correctness rules make this work
and are easy to get wrong:
- **De-dupe assistant messages by `message.id`.** Claude Code writes one JSONL line
  per content block, repeating full `usage` each time — naive summing over-counts 2–4×.
- **Include `<session>/subagents/*.jsonl`.** Sub-agent (Task/Explore) usage is billed
  to the session but stored separately; omitting it under-counts delegating sessions.

## Notes

- Server-side web search/fetch counts are surfaced per request and in totals; they
  are billed separately by Anthropic and are not yet folded into the dollar TOTAL.
- Costs are estimates from public per-token rates and won't match a billing invoice
  exactly (tiers, batch discounts, rounding).
- `NO_COLOR=1` disables ANSI color; output is auto-plain when piped to a file.
