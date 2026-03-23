# Phase C spike: out-of-process ingest worker (MARROW-PERF-013)

**Status:** Decision — **defer** (2026-03-23)

## Problem

If M2 (streaming ingest, scoped graph passes, SQLite tuning) still misses the Accrualify-scale **≤120 s** / **~5 GB RSS** targets, isolating parse + DB write in a separate OS process would cap RSS for the stdio MCP parent and allow harder termination of runaway parses.

## Pros

- Hard memory ceiling for the MCP/daemon process; worker can be `rlimit`-bounded.
- Crash isolation: worker panic does not take down the MCP server.
- Potential to run worker at lower priority or on a subset of CPUs.

## Cons

- IPC design (job protocol, progress, partial failure, DB locking) is non-trivial.
- Two copies of graph logic or shared library boundary; higher maintenance.
- SQLite is still a single-writer DB — a worker must own writes or serialize through a single connection; parallelism gains are limited without sharding.
- New attack surface only if network or shell is involved; a **local** pipe/socket to a child is acceptable if permissions stay umask-restricted and inputs are path-validated as today.

## Recommendation

**Defer** a worker rewrite until:

1. M2 stories are measured on the pinned Accrualify fixture (`docs/perf-baseline-runbook.md`), and  
2. Profiling shows the MCP process RSS dominated by ingest rather than retrieval or SQLite cache.

If deferred, prefer: stronger spill/queue bounds (done), scoped cross-repo pass (done), maintenance gating (done), and optional `MARROW_SQLITE_*` tuning before any process split.

## Thin prototype (if re-opened)

- Child: stdin JSON lines `{ "op": "ingest", "db_path", "repo_id", "root" }` → exit code + stderr log; no network.
- Parent: spawn with inherited or dedicated DB path; enforce single writer; timeout + kill on stall.
- Security: same path allowlists as CLI; no new listeners.
