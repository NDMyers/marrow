# Configuration Reference

Marrow is configured entirely through environment variables; no config server or account is involved. All variables are optional — the defaults are tuned so a large `graph.db` stays modest in your OS process monitor.

## Memory and ingestion tuning

| Variable | Default | Purpose |
|----------|---------|---------|
| `MARROW_DB_PATH` | `.marrow/graph.db` | Path to the SQLite graph database. |
| `MARROW_SQLITE_CACHE_KIB` | `32768` (32 MiB) | SQLite `cache_size` (negative KiB). Lower → less idle RSS; higher → faster queries. |
| `MARROW_SQLITE_MMAP_BYTES` | `0` | `PRAGMA mmap_size` in bytes; `0` disables mmap. Set positive to re-enable mmap for throughput. |
| `MARROW_MAX_FILE_BYTES` | `2097152` (2 MiB) | Skip files larger than this before tree-sitter parse. Large generated files (GraphQL schemas, protobuf outputs, bundled JS) produce ASTs 3–10× source size in each parallel worker; skipping them prevents multi-GB RSS spikes with zero loss of architectural signal. |
| `MARROW_INGEST_THREADS` | `min(8, max(2, cores))` | Rayon workers for hash/parse during ingest; fewer workers lower peak RAM during full reindex. |
| `MARROW_INGEST_PARSE_QUEUE` | `64` | Max parsed files in the bounded channel between Rayon workers and a drainer thread (serialized to a temp spill file); lower → lower peak RSS on huge reindexes, more back-pressure on workers. Spill reads cap blob size (64 MiB per field) and symbol count per row to limit corrupt-file DoS. |
| `MARROW_SKIP_POST_INGEST_MAINTENANCE` | *(unset)* | If non-empty, skip WAL checkpoint + `incremental_vacuum` after ingest (faster huge reindexes). Run `marrow maintenance` later. |
| `MARROW_CROSS_REPO_FULL_SCAN` | *(unset)* | If `1`/`true`/`yes`, scan **all** repos for cross-repo `IMPORTS` after each ingest (legacy). Default: only the repo that was just indexed. |

## Capsule and query payload limits

| Variable | Default | Purpose |
|----------|---------|---------|
| `MARROW_CAPSULE_MAX_OUTBOUND` | `500` | Max outbound edges loaded per capsule / trace (RAM bound). |
| `MARROW_CAPSULE_ORIGINAL_MODE` | `none` | `none` (default): do not load touched files into MCP `original_text` (saves RAM). `full`: legacy concatenation of full files (see `MARROW_CAPSULE_ORIGINAL_MAX_BYTES`). |
| `MARROW_CAPSULE_ORIGINAL_LEGACY` | *(unset)* | If `1`/`true`/`yes`, alias for `MARROW_CAPSULE_ORIGINAL_MODE=full` (one-release shim). |
| `MARROW_CAPSULE_ORIGINAL_MAX_BYTES` | *(unset)* | **Only when mode is `full`.** Cap total bytes for `original_text`. Uses file `metadata().len()` before reading; skips files that would exceed the budget. Unset = unlimited concat (can spike RAM). |
| `MARROW_CAPSULE_PROOF_MAX_BYTES` | `16384` | Default-mode dashboard proof snapshot cap. This bounded evidence is cached for compare; it is not returned as MCP `original_text`. |
| `MARROW_CAPSULE_PROOF_MAX_FILES` | `8` | Max touched files included in the default-mode proof snapshot. More touched files are deterministically sampled and labeled as partial. |
| `MARROW_CAPSULE_MAX_INBOUND_LOAD` | `64` | Max inbound rows loaded from DB (display still capped at 10). |
| `MARROW_IMPACT_MAX_ROWS` | `5000` | Max rows returned by `analyze_impact`. |
| `MARROW_DEP_GRAPH_MAX_BYTES` | `32000` | Response budget for `dependency_graph` output. |
| `MARROW_BATCH_MAX_BYTES` | `32000` | Response budget for `explore_batch` output. |
| `MARROW_SKELETON_MAX_BYTES` | `32000` | Response budget for `get_skeleton`; truncated output includes a `target_dir` hint to narrow the scope. |

## Token benchmarks and provenance

`marrow benchmark <symbol> <repo_id>` keeps the scriptable benchmark path and uses the same labeled `file_tokens` baseline as the MCP tools (metadata `len/4` estimate when `MARROW_CAPSULE_ORIGINAL_MODE` is `none`). Add `--precise-file-tokens` for evidence-grade cl100k_base counts summed per touched file (streams one file at a time; no full concat).

In an interactive terminal, `marrow benchmark` with no arguments opens a guided wizard for repository selection, symbol search/filtering, and benchmark mode selection. Choose estimated mode for the default provenance labels, or exact proof mode for the same behavior as `--precise-file-tokens`.

Benchmark output includes the symbol, repo ID, tokenizer mode, original/proof modes, precise-token setting, and active caps so reported reductions can be reproduced for the same graph and environment.

Dashboard reduction cards are operational estimates unless the provenance label says otherwise. Use `marrow benchmark --precise-file-tokens <symbol> <repo_id>` or `marrow perf-harness --precise-file-tokens --json` for exact, reproducible token claims.

## Post-ingest maintenance

After a large ingest, or if you set `MARROW_SKIP_POST_INGEST_MAINTENANCE`, reclaim WAL space with:

```bash
marrow maintenance
```

Uses `MARROW_DB_PATH` or defaults to `.marrow/graph.db`.
