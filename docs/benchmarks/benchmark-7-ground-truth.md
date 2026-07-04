# Benchmark 7 Ground Truth & Protocol (pre-registered 2026-07-03, BEFORE any arm ran)

Repos: marrow @ docs/align-benchmark-claims tree (= main c0c16dc region, WITHOUT unmerged PR #61) — 1,234 symbols re-indexed today, self-check 17/17.
llama.cpp @ fdb1db877 — 16,333 symbols. DO NOT pull/switch branches in D:\Coding\marrow until the campaign completes.

Arms (subagents, Sonnet, cold-start, waves of ≤6 to limit timing contention):
- Arm A preamble: marrow available via CLI — BOTH `marrow query <symbol> <repo_id>` (capsule+impact)
  AND `marrow context "<task>" --repo <repo_id>` (task-routed context packet). Routing guidance
  mirrors shipped skill text. (Delta vs B6: `marrow context` now mentioned — B6 only exposed
  `marrow query`; disclosed.)
- Arm B preamble: native Read/Grep/Glob/shell only; no marrow, no mcp__ tools.

## Cost methodology (fixes B6 weaknesses)
- Two CONTROL runs (one per arm preamble) perform a trivial task; their token totals estimate each
  arm's fixed prompt+harness overhead. Report both absolute and MARGINAL (cell − same-arm control).
- MCP schema tax measured separately via a tools/list stdio probe against `marrow mcp`; reported as
  the additional fixed per-session cost real MCP deployments pay that CLI arm A does not.
- Wall time reported as coarse signal only (concurrent runs).
- Tool logs cross-checked against harness tool_uses.

## Cells (18 new runs + 4 reused B6 cells)

### C1 — exact-string enumeration (expected native win; boundary confirmation)
- C1-L: REUSED from B6 (T1/T3, 4 cells; native won ~11% abs tokens, ~38% time, quality parity).
- C1-M task: every direct call site of `increment_stat` in marrow src/ as file:line, excluding the
  definition (src/db.rs:458). GT = 23 call sites:
  main.rs 3161,3163,3164,3706,3707,3713,4206,4207,4208,4360,4361,4366,4574,4577,
  6795,6796,6797,6798,6799,9404,9405,9410; retrieval.rs 2340.
  TRAPS: main.rs:6427 and 6435 are string literals inside a test assertion (`dispatch_block.contains(...)`)
  — listing them is a precision error. Scoring: P/R over 23; trap inclusion = precision hit.

### C2 — symbol-level caller map + test/prod classification (marrow-native granularity)
- C2-M task: which FUNCTIONS call `ingest_repo` (crate marrow)? Name + file + classify test/production.
  GT: PRODUCTION callers (must-find): ensure_repo_ready (main.rs:1392), the run_pipeline JIT block in
  call_tool (main.rs:3638), maybe_auto_index_empty_db (main.rs:9295). TEST callers: ~22 test fns in
  ingestion.rs mod tests (sites 1832–3028), doctor.rs ingested_fixture (130), main.rs tests 7041,7138,7243.
  Scoring: 3/3 production callers found & classified (60%), test-caller coverage ≥80% & correctly
  classified (30%), no phantom callers (10%).
- C2-L task: which functions call `llama_token_to_piece`? Name + file + classify (library wrapper /
  example / tool / test). GT sites (excl. decl llama.h:1155, impl llama-vocab.cpp:4314, COMMENT TRAP
  llama-vocab.cpp:1815): common.cpp:1695,1698 (common_token_to_piece — the library wrapper, must-find);
  simple-chat:141; simple:138,190; test-backend-sampler:250,253; diffusion-cli:55; debug:173;
  batched.swift:226,230; LibLlama.swift:321,329; mtmd.cpp:787,790. 15 sites, 10 files, 2 Swift files.
  Scoring: file coverage 10/10 incl. Swift (50%), wrapper identified (20%), trap excluded (15%),
  reasonable function attribution on spot-checks (15%).

### C3 — change impact (single pair; C3-L dropped — GT cost; disclosed as 1-sample class)
- C3-M task: "If `resolve_symbol_or_disambiguate`'s return type changes, which functions must be
  updated? Name each + file, and say in one sentence what each uses it for." GT: 7 call sites =
  doctor.rs:90 (run_index_self_check) + retrieval.rs 690, 779, 1209, 1623, 1925, 2071 (function names
  verified at scoring time by reading enclosing fns; includes get_context_capsule and analyze_impact).
  FRESHNESS PROBE: doctor.rs:90 is 2 days old — missing it while listing older callers indicates
  stale-index or stale-knowledge behavior. Scoring: recall over 7 (60%), precision (20%), usage
  sentences sane (20%).

### C4 — unknown-symbol discovery (behavioral description, no name given)
- C4-M task: "Find the function that determines which files need re-parsing on an incremental ingest
  by comparing content hashes. Name + file:line." GT: compute_changeset (src/ingestion.rs:~837) full
  credit; ingest_repo_with_progress half credit if hash mechanism explained.
  ⚠ GT AMENDED POST-RUN (2026-07-03, adjudicated by reading src/ingestion.rs:748-811 after BOTH arms
  independently converged on a different answer with evidence): the hash-comparison decision
  (`known_hash == new_hash → skip`) lives in `parallel_hash_and_parse_candidates`
  (src/ingestion.rs:748, decision at 767-770); `compute_changeset` (837) is the orchestrator that
  calls it. Amended scoring: parallel_hash_and_parse_candidates = full; compute_changeset = full-minus
  (orchestrator level); ingest_repo_with_progress = half. Amendment affects both arms identically, so
  the A/B comparison is unaffected. Original registration preserved above for transparency.
- C4-L task: "Find the function that checks whether a token is an end-of-generation token.
  Name + file:line of the implementation." GT: llama_vocab_is_eog, src/llama-vocab.cpp:4121
  (decl include/llama.h acceptable as secondary cite).

### C5 — token-bounded comprehension (capsule home turf)
- C5-M task: "In ≤150 words, what does `compile_context_packet_for_format` (src/context.rs) do, and
  which functions does it directly call?" GT callees (9): behavior, task_terms, repo_node_count,
  freshness_metadata, route_task, build_ranked_candidates, enforce_entry_budget,
  enforce_emitted_packet_budget, metadata. Description rubric: budget computation, routing decision,
  candidate ranking, truncation/provenance, emitted-packet budget enforcement.
  Scoring: callee P/R (60%; `behavior`/`metadata` method-sugar misses cost half), description 40%.
- C5-L task: "In ≤150 words, what does `llama_sampler_sample` (src/llama-sampler.cpp) do, and which
  functions does it directly call?" GT callees (12): llama_get_sampled_token_ith,
  llama_get_sampled_probs_ith, llama_get_sampled_logits_ith, llama_get_sampled_candidates_ith,
  llama_get_sampled_probs_count_ith, llama_get_sampled_logits_count_ith, llama_get_model,
  llama_model_get_vocab, llama_vocab_n_tokens, llama_get_logits_ith, llama_sampler_apply,
  llama_sampler_accept. Core trio (must-find): llama_get_logits_ith, llama_sampler_apply,
  llama_sampler_accept + backend-presample early-return path in description.
  Scoring: callee P/R with core-trio weighting (60%), description incl. backend fast path (40%).

### Controls
- CTRL-A / CTRL-B: trivial task ("state the name of the top-level directory at D:\Coding\llama.cpp"),
  arm-matched preambles. Token totals = fixed overhead estimate per arm.

## Waves
1. CTRL-A, CTRL-B, C1-M ×2, C4-L ×2
2. C2-M ×2, C2-L ×2, C4-M ×2
3. C3-M ×2, C5-M ×2, C5-L ×2

## Hypotheses (registered)
- C1: native wins marginal cost & time; quality parity. (Boundary confirmation.)
- C2/C3: marrow wins quality (attribution & freshness) at comparable or better marginal cost;
  grep-only arms pay attribution overhead (extra reads) or lose caller-name accuracy.
- C4: marrow context/FTS finds by description cheaper than grep-guessing keyword roulette; risk:
  agents' prior knowledge of llama.cpp API names may let arm B shortcut C4-L (disclose if seen).
- C5: marrow wins marginal tokens materially (capsule vs reading large files); quality parity or better.
