# SQLite hot queries — expected plans (MARROW-PERF-010)

Captured with `EXPLAIN QUERY PLAN` against a populated graph. Plans use composite indexes added in MARROW-PERF-010 where noted.

## Ingest / CALLS

| Query | Expected use of indexes |
|-------|-------------------------|
| `SELECT id FROM nodes WHERE repo_id = ? AND file_path = ?` | `idx_nodes_repo_file` (or primary scan on `repo_id` prefix). |
| `SELECT … FROM nodes WHERE repo_id = ? AND file_path = ?` (symbol rows) | Same as above. |
| `SELECT n.symbol_name, n.id FROM nodes n INNER JOIN _marrow_callee_lookup c ON n.symbol_name = c.name WHERE n.repo_id = ?` | `idx_nodes_repo_symbol` or `idx_nodes_symbol` + `repo_id` filter (SQLite may choose either). |
| `DELETE FROM edges WHERE source_id IN (SELECT id FROM nodes WHERE repo_id = ? AND file_path = ?)` | Subquery via `idx_nodes_repo_file`; edge deletes via `idx_edges_source`. |

## Retrieval (capsule / impact)

| Query | Expected use of indexes |
|-------|-------------------------|
| `FROM edges e JOIN nodes n ON e.source_id = ? AND n.id = e.target_id` | `idx_edges_source` seek on `source_id`. |
| `FROM edges e JOIN nodes n ON e.target_id = ? AND n.id = e.source_id` | `idx_edges_target` seek on `target_id`. |
| `SELECT COUNT(*) FROM nodes WHERE repo_id = ?` | `idx_nodes_repo` or `idx_nodes_repo_file` prefix. |

Re-run `EXPLAIN QUERY PLAN` after schema changes; if the planner stops using an index, adjust statistics (`ANALYZE`) or revisit the index set.
