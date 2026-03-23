# Marrow performance baseline (MARROW-PERF-001)

This runbook defines how we measure **wall time**, **SQLite footprint**, and **resident set** for the RAM/latency epic ([`../.cursor/epics/marrow-ram-latency-epic.md`](../.cursor/epics/marrow-ram-latency-epic.md)). Use the same steps for every regression comparison.

## What “peak RSS” means here

- **Primary (external):** On macOS **`/usr/bin/time -l`** prints two different fields:
  - **`maximum resident set size`** — use this as the epic’s headline number for “did we stay under ~5 GiB?” It should match **`rusage_max_rss_bytes`** in the harness JSON (both come from the same `getrusage` semantics on Darwin).
  - **`peak memory footprint`** — a separate kernel/accounting figure (often **lower** than max RSS on recent macOS). Log it in the Notes column if you paste full `time -l` output; do **not** substitute it for max RSS when comparing to the epic unless the team explicitly standardizes on it.
- **In-process hint:** The `perf-harness` JSON field `rusage_max_rss_bytes` comes from `getrusage(RUSAGE_SELF).ru_maxrss` (Darwin: bytes; Linux: converted from KiB). It can **differ** from Activity Monitor’s “Memory” column; treat **`maximum resident set size`** as the reference when they disagree.
- **SQLite:** A large `graph.db` with mmap enabled can inflate RSS; Marrow defaults to **`MARROW_SQLITE_MMAP_BYTES=0`**. See [README memory tuning](../README.md#local-development).

## Environment variables (record these in every baseline row)

| Variable | Role |
|----------|------|
| `MARROW_DB_PATH` | Default DB path for `mcp` / some tools (perf-harness uses `--db` or its own default). |
| `MARROW_SQLITE_CACHE_KIB` | SQLite `cache_size` (KiB); lower → lower idle RSS, slower queries. |
| `MARROW_SQLITE_MMAP_BYTES` | `mmap_size` in bytes; `0` disables mmap. |
| `MARROW_INGEST_THREADS` | Rayon worker cap during ingest. |
| `RAYON_NUM_THREADS` | Global Rayon thread pool (if set, affects parallelism). |

## Pinned stress fixture (Accrualify-scale)

Record the **exact** corpus you use so numbers are comparable:

| Field | Value (fill in) |
|-------|------------------|
| Host OS / arch | macOS (Apple Silicon); Darwin 23.x (see first baseline row Notes) |
| Repo path | `/Users/ndmyers/Accrualify` |
| Repo revision | Run `git -C ~/Accrualify rev-parse HEAD` and paste into baseline table |
| `repo_id` used with harness | `Accrualify` |

## Commands

### 1) Automated harness (ingest + capsule + impact)

From the Marrow repo root, release build recommended:

```bash
cargo build --release
```

**Peak RSS (recommended wrapper on macOS):**

```bash
/usr/bin/time -l ./target/release/marrow perf-harness \
  --root /path/to/stress/repo \
  --repo-id YOUR_REPO_ID \
  --db ./.marrow/perf-graph.db \
  --fresh \
  --json
```

 stderr shows progress; **stdout** is one JSON object (see [`perf-harness.md`](./perf-harness.md)).

### 2) Wall time only (no `time -l`)

```bash
./target/release/marrow perf-harness --root /path/to/repo --fresh --json
```

### 3) CLI ingest path note

`marrow index`, the TUI index flow, MCP `ingest_repo`, and **`perf-harness`** all use **`ingestion::run_ingestion`**. For timing comparisons, record whether **`MARROW_SKIP_POST_INGEST_MAINTENANCE`** was set; run **`marrow maintenance`** afterward if you skipped post-ingest checkpoint/vacuum.

### 4) MCP-heavy scenario (manual)

1. Point `MARROW_DB_PATH` at a DB produced by harness or MCP `ingest_repo`.
2. Start `marrow mcp` from the client.
3. Run a fixed sequence of tools (e.g. `ingest_repo` if needed, then `get_context_capsule`, then `analyze_impact` on the same symbol).
4. Record client-side timing and, if possible, wrap the **server process** with Instruments “Allocations” or sample RSS while the sequence runs.

## Baseline table (copy row per run)

| Date | Git SHA (marrow) | Fixture revision | ingest wall s | query wall s | `time -l` max RSS (B) | `rusage_max_rss_bytes` | `db_file_bytes` | symbols | edges | Notes |
|------|------------------|------------------|---------------|--------------|------------------------|-------------------------|-----------------|---------|-------|-------|
| 2026-03-23 | `ebf384204898da8e10a83ef2c7fb60a70342ccdd` (dirty) | *add `git -C ~/Accrualify rev-parse HEAD`* | 26.23 | 0.011 | 462454784 | 462454784 | 1388507136 | 31227 | 4024648 | Smoke: `perf-harness --root ~/Accrualify --fresh --json`; `time -l` real 26.32s; `peak memory footprint` 379242816 (see § above—do not conflate with max RSS). Default Marrow SQLite/ingest env unless overridden. |

## Known noise / variance

- **Cold vs warm page cache** — first run after reboot reads more from disk.
- **Parallelism** — `MARROW_INGEST_THREADS` / `RAYON_NUM_THREADS` change CPU and peak RAM.
- **Other processes** — close heavy IDEs/browsers when chasing RSS regressions.
- **WAL and checkpoints** — SQLite WAL size and `vacuum_and_checkpoint` behavior affect time and I/O spikes (see MARROW-PERF-005).
- **Antivirus / Spotlight** — can skew cold ingest on macOS.

## Accrualify-scale preset (starting point for 8–16 GiB machines)

Tune per machine; values are conservative defaults to **lower peak RSS** (may slow ingest):

```bash
export MARROW_SQLITE_CACHE_KIB=65536      # 64 MiB
export MARROW_SQLITE_MMAP_BYTES=0
export MARROW_INGEST_THREADS=4
```

Full knob reference: [README § Local development / Memory tuning](../README.md#local-development).
