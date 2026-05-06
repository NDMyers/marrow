# MCP payload limits (MARROW-PERF-014)

Hard caps keep `get_context_capsule`, `trace_logic_flow`, and `analyze_impact` from loading unbounded graph slices into RAM.

| Surface | Env var | Default | Notes |
|---------|---------|---------|--------|
| Capsule pivot body | `MARROW_CAPSULE_MAX_PIVOT_BYTES` | 12,000 (~3k tokens) | When the pivot's `raw_text` exceeds this, `format_capsule` auto-condenses it via `condense()` and emits a note. `trace_flow` is unaffected (always full). |
| Capsule / trace outbound edges | `MARROW_CAPSULE_MAX_OUTBOUND` | 500 | SQL `ORDER BY symbol_name, file_path` then `LIMIT`. |
| Capsule `original_text` source | `MARROW_CAPSULE_ORIGINAL_MODE` | `none` | `none`: `original_text` is empty on successful resolution (no disk read for concat). `full`: legacy full-file concat in **sorted path order**. `MARROW_CAPSULE_ORIGINAL_LEGACY=1` aliases to `full`. |
| Capsule `original_text` byte cap | `MARROW_CAPSULE_ORIGINAL_MAX_BYTES` | *(unset)* | **Only when mode is `full`.** Positive budget; uses `metadata().len()` before `read_to_string` and skips files that would exceed the remainder. Unset = unlimited. |
| Dashboard proof snapshot bytes | `MARROW_CAPSULE_PROOF_MAX_BYTES` | 16,384 | Applies only to default-mode dashboard evidence. The snapshot is cached for compare and labeled as cached, sampled, or truncated. It is not returned as MCP `original_text`. |
| Dashboard proof snapshot files | `MARROW_CAPSULE_PROOF_MAX_FILES` | 8 | Max touched files represented in the default proof snapshot. Capsules touching more files are deterministically sampled and labeled partial. |
| Capsule inbound rows loaded | `MARROW_CAPSULE_MAX_INBOUND_LOAD` | 64 | Formatted output still shows at most **10** callers (`MAX_INBOUND_CALLERS`). |
| `analyze_impact` result rows | `MARROW_IMPACT_MAX_ROWS` | 5000 | Recursive CTE then `LIMIT`; `ImpactResult.truncated` is true when the cap is hit. |
| Project skeleton | *(constant)* | 2000 rows | `SKELETON_ROW_LIMIT` in `retrieval.rs`. |

When a limit applies, the text response includes a short `[Note: …]` line. Disambiguation payloads (`MAX_DISAMBIGUATION_ITEMS` = 20) are unchanged.

Default MCP capsule responses stay low-cost: `get_context_capsule`, `run_pipeline` with `explore_symbol`, and `trace_flow` do not include full original files in normal payloads. The dashboard receives separate bounded proof metadata for served capsule events so the compare modal can show inspectable evidence without turning every MCP response into a full-file transfer.

The dashboard token baseline is labeled by provenance. `estimated` means metadata `len/4`; `cached_proof`, `sampled_proof`, and `truncated_proof` describe the bounded proof text retained for inspection; `full` and `truncated_full` require explicit `MARROW_CAPSULE_ORIGINAL_MODE=full`. Exact token claims require benchmark/report output produced with precise token measurement.

See also [README memory tuning](../README.md#local-development).
