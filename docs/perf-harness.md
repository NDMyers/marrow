# `marrow perf-harness` (MARROW-PERF-002)

Deterministic driver for epic benchmarks: runs the **canonical** ingest pipeline (`ingestion::run_ingestion`), then times **MCP-equivalent** read queries (`get_context_capsule` + `analyze_impact`) on one symbol.

For ad hoc terminal exploration against an existing graph, `marrow benchmark` opens an interactive wizard when run with no arguments in a TTY. It guides repository selection, symbol search/filtering, and estimated versus exact proof mode. Keep using `marrow benchmark [--precise-file-tokens] <symbol> <repo_id>` or `marrow perf-harness ... --json` for scripted, reproducible harness runs.

## Usage

```bash
cargo build --release
./target/release/marrow perf-harness --help   # if implemented; else see flags below
```

### Flags

| Flag | Meaning |
|------|---------|
| `--root <path>` | Repository root to ingest (default: current directory). |
| `--repo-id <id>` | `repo_id` stored in DB (default: final component of `--root`). |
| `--db <path>` | SQLite path (default: `./.marrow/perf-graph.db`). |
| `--symbol <name>` | Symbol for query phase (default: first `symbol_name` in `nodes` for this `repo_id`). |
| `--fresh` | Delete `--db` before run if it exists. |
| `--precise-file-tokens` | Measure exact cl100k_base baseline tokens for files touched by the capsule. Fails the run if any touched file cannot be tokenized. |
| `--json` | Print a single JSON object on **stdout**; progress on **stderr**. |

### Example

```bash
/usr/bin/time -l ./target/release/marrow perf-harness \
  --root ~/src/Accrualify \
  --repo-id Accrualify \
  --fresh \
  --json
```

## JSON schema (`schema_version`: 1)

Fields emitted with `--json`:

- `schema_version` — `1`
- `repo_id`, `root`, `db_path`
- `ingest_wall_ms`, `query_wall_ms`
- `symbols`, `edges`
- `query_symbol` — symbol used for capsule/impact
- `baseline_file_tokens`, `capsule_tokens`
- `baseline_token_source` — `estimated`, `exact`, `full`, or `truncated_full`
- `tokenizer_mode` — `metadata_len/4`, `text_len/4`, or `cl100k_base`
- `original_mode`, `proof_mode`, `precise_file_tokens`
- `original_max_bytes`, `proof_max_bytes`, `proof_max_files`, `touched_file_count`
- `db_file_bytes` — DB file size on disk after ingest (0 if missing)
- `rusage_max_rss_bytes` — `null` on unsupported OS, else best-effort high-water from `getrusage`
- `marrow_git_dirty`, `marrow_git_sha`, `marrow_version` — build metadata when available

## Evidence-grade token claims

Use exact mode for published or skeptical-user claims:

```bash
./target/release/marrow perf-harness \
  --root ~/src/example \
  --repo-id example \
  --symbol target_symbol \
  --fresh \
  --precise-file-tokens \
  --json
```

Treat dashboard reductions and default harness runs as estimates when `baseline_token_source` is `estimated`. Treat `proof_mode` values containing `sampled` or `truncated` as partial inspectable evidence, not complete baseline text. Exact claims should quote the JSON fields needed to rerun the same measurement: `query_symbol`, `repo_id`, `tokenizer_mode`, `original_mode`, `proof_mode`, `precise_file_tokens`, `original_max_bytes`, `proof_max_bytes`, `proof_max_files`, and the git/build metadata.

## Pinning fixtures

Record the stress repo’s `git rev-parse HEAD` in [`perf-baseline-runbook.md`](./perf-baseline-runbook.md). CI may omit the full Accrualify tree; use a smaller generated fixture or gate the job behind a label.

**Multi-repo workspaces:** Each `perf-harness` run ingests one `--root`. Cross-repo `IMPORTS` are derived from **that** repo’s nodes by default (`MARROW-PERF-012`). Set `MARROW_CROSS_REPO_FULL_SCAN=1` to scan every repo after ingest (legacy; more CPU/RAM).

**CI smoke:** `.github/workflows/ci.yml` runs a two-file synthetic repo and checks `ingest_wall_ms` / `query_wall_ms` against [`ci/perf-thresholds.json`](../ci/perf-thresholds.json); read `flake_policy` there for GitHub runner variance.

## MARROW-PERF-009 — `name_to_ids` scope (CALLS targets)

**Before:** After applying node updates, CALLS resolution loaded every `(symbol_name, id)` in the repo into a `HashMap`, then inserted edges only for symbols in changed files.

**After:** Collect callee names referenced from **changed files only**, then join `nodes` against a temp table of those names (`build_name_to_ids_for_symbol_names` in `ingestion.rs`). Unchanged files are not scanned for the map. The watcher uses the same helper for single-file reindexes.

Re-run `perf-harness` on Accrualify-scale fixtures to measure ingest/rebuild phase deltas vs the baseline row in [`perf-baseline-runbook.md`](./perf-baseline-runbook.md).

## See also

- [Performance baseline runbook](./perf-baseline-runbook.md)
- [SQLite query plans](./sqlite-query-plans.md) (MARROW-PERF-010)
- Epic: [`.cursor/epics/marrow-ram-latency-epic.md`](../.cursor/epics/marrow-ram-latency-epic.md)
