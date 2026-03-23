# MCP payload limits (MARROW-PERF-014)

Hard caps keep `get_context_capsule`, `trace_logic_flow`, and `analyze_impact` from loading unbounded graph slices into RAM.

| Surface | Env var | Default | Notes |
|---------|---------|---------|--------|
| Capsule / trace outbound edges | `MARROW_CAPSULE_MAX_OUTBOUND` | 500 | SQL `ORDER BY symbol_name, file_path` then `LIMIT`. |
| Capsule inbound rows loaded | `MARROW_CAPSULE_MAX_INBOUND_LOAD` | 64 | Formatted output still shows at most **10** callers (`MAX_INBOUND_CALLERS`). |
| `analyze_impact` result rows | `MARROW_IMPACT_MAX_ROWS` | 5000 | Recursive CTE then `LIMIT`; `ImpactResult.truncated` is true when the cap is hit. |
| Project skeleton | *(constant)* | 2000 rows | `SKELETON_ROW_LIMIT` in `retrieval.rs`. |

When a limit applies, the text response includes a short `[Note: …]` line. Disambiguation payloads (`MAX_DISAMBIGUATION_ITEMS` = 20) are unchanged.

See also [README memory tuning](../README.md#local-development).
