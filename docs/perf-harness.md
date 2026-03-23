# `marrow perf-harness` (MARROW-PERF-002)

Deterministic driver for epic benchmarks: runs the **canonical** ingest pipeline (`ingestion::run_ingestion`), then times **MCP-equivalent** read queries (`get_context_capsule` + `analyze_impact`) on one symbol.

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
- `db_file_bytes` — DB file size on disk after ingest (0 if missing)
- `rusage_max_rss_bytes` — `null` on unsupported OS, else best-effort high-water from `getrusage`
- `git_dirty` / `marrow_version` — build metadata when available

## Pinning fixtures

Record the stress repo’s `git rev-parse HEAD` in [`perf-baseline-runbook.md`](./perf-baseline-runbook.md). CI may omit the full Accrualify tree; use a smaller generated fixture or gate the job behind a label.

## MARROW-PERF-009 — `name_to_ids` scope (CALLS targets)

**Before:** After applying node updates, CALLS resolution loaded every `(symbol_name, id)` in the repo into a `HashMap`, then inserted edges only for symbols in changed files.

**After:** Collect callee names referenced from **changed files only**, then join `nodes` against a temp table of those names (`build_name_to_ids_for_symbol_names` in `ingestion.rs`). Unchanged files are not scanned for the map. The watcher uses the same helper for single-file reindexes.

Re-run `perf-harness` on Accrualify-scale fixtures to measure ingest/rebuild phase deltas vs the baseline row in [`perf-baseline-runbook.md`](./perf-baseline-runbook.md).

## See also

- [Performance baseline runbook](./perf-baseline-runbook.md)
- Epic: [`.cursor/epics/marrow-ram-latency-epic.md`](../.cursor/epics/marrow-ram-latency-epic.md)
