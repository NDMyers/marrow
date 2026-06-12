# Benchmark: Marrow on a Production Rails Monolith

Marrow is built to cut the tokens an AI coding agent burns reading code. To prove it works
beyond toy repos, we benchmarked it against a **private, actively developed production Ruby on
Rails monolith** — a real company codebase, indexed and queried live on a developer laptop.
No code left the machine: Marrow is fully local and offline.

## The codebase

| | |
|---|---|
| Language | Ruby on Rails |
| Files | ~3,100 Ruby files |
| Lines of Ruby | ~326,000 |
| Symbols indexed | 16,539 (13.3k methods, 2.9k classes, 292 modules) |
| Call edges | ~27,000 |

## Indexing speed

Measured with `marrow perf-harness` (fresh database, exact `cl100k_base` token counts):

| Metric | Result |
|---|---|
| Full cold index of the monolith | **2.1 s** |
| Warm re-index | **0.7 s** |
| Single-symbol query | **156 ms** |
| Peak memory during ingest | 188 MB |
| Graph database on disk | 56 MB |

Hardware: Apple M2 Pro, 32 GB RAM.

## Token reduction

`marrow benchmark` compares the exact token count of every file an agent would otherwise read
(the capsule's graph neighborhood — typically ~20 files) against the Context Capsule Marrow
returns instead. Baselines are exact `cl100k_base` counts, not estimates.

| Pivot symbol | Baseline tokens | Capsule tokens | Reduction |
|---|---|---|---|
| Large API controller (~1,300 lines) | 224,548 | 4,360 | **98.1%** |
| Core domain model A | 245,785 | 4,780 | **98.1%** |
| Core domain model B | 186,549 | 4,803 | **97.4%** |
| Shared workflow concern | 149,681 | 4,623 | **96.9%** |
| Line-item model | 77,350 | 1,876 | **97.6%** |
| Authorization model | 27,536 | 3,396 | **87.7%** |

**Mean reduction across pivots: ~96%.** In a single live agent session (three capsule
requests over MCP), the dashboard recorded **383,086 tokens saved — a 95.1% session-level
reduction** — with individual requests reaching 99.4%.

## Live MCP latency

A real MCP stdio session (the same protocol Claude Code, Cursor, and other agents use) was
driven against the indexed monolith. Every `run_pipeline` response returned in well under a
quarter second:

| Intent | Latency |
|---|---|
| `explore_symbol` | 105–216 ms |
| `map_class` (269 structural nodes) | 200 ms |
| `dependency_graph` (142 nodes, depth 2) | 14 ms |
| `trace_flow` | 169 ms |

Natural-language `marrow context` packets ("How does the approval flow work end to end?")
compiled in **under 0.8 s**, correctly ranking the relevant workflow class as the #1 entry and
fitting an 8,000-token budget.

## Anatomy of a Context Capsule

What does the agent actually receive? Below is a real capsule produced during this benchmark,
pivoting on a spreadsheet-export module (identifiers lightly anonymized; the structure,
condensation, and numbers are verbatim engine output).

Without Marrow, an agent answering "how does XLSX export work?" reads the pivot file **plus
the six files its dependencies live in — 206,591 characters (~51,600 tokens)**. Marrow's
capsule delivers the same structural knowledge in **6,580 characters (~1,600 tokens): a 96.8%
reduction** — full source for the pivot, signatures only for everything it touches:

```text
CONTEXT CAPSULE — pivot: Exporters (rb)
File : app/reports/exporters/xlsx.rb:7
Type : module

── FULL SOURCE ──────────────────────────────────────────────
module Exporters
  class Xlsx < Base
    DEFAULT_SHEET_NAME = 'Sheet1'
    XLSX_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    ...

    def to_merged_file(sorted_assets: [])
      merge_batch_files(sorted_assets: sorted_assets)
    end

    def merge_batch_files(sorted_assets:)
      xlsx_tmp = Tempfile.new([TEMP_FILENAME, XLSX_FILE_EXTENSION], binmode: true)
      workbook = FastExcel.open(xlsx_tmp.path, constant_memory: true,
                                default_format: prepare_default_format)
      sheet = workbook.add_worksheet(worksheet_label)
      ...                                     # ← full 180-line module included
    end
  end
end

── OUTBOUND DEPENDENCIES (signatures only) ────────────────────────

  [CALLS]  date_filters_text   (rb)  app/reports/base_report.rb:129
  def date_filters_text

  [CALLS]  report_template     (rb)  app/models/report.rb:47
  def report_template

  [CALLS]  truncate            (rb)  app/services/payment_gateway_service.rb:576
  def truncate(string, max)

  ... 6 more neighbors, one signature line each ...

[Expand a neighbor: run_pipeline(intent: "read_node", target: "<symbol>")]
```

The agent sees every callable boundary it might need, can expand any neighbor on demand with
`read_node`, and never pays for the 126 KB test file that happens to share a method name.

## Methodology notes

- All numbers were collected on 2026-06-12 with Marrow v0.1.1 against the monolith's HEAD of
  the same day.
- Token counts use the `cl100k_base` tokenizer with `--precise-file-tokens` (exact baselines).
- The codebase is private; symbol names are anonymized here. The full unredacted run log is
  retained internally for audit.
- Reproduce on your own repo: `marrow index`, then
  `marrow benchmark --precise-file-tokens <Symbol> <repo_id>`.
