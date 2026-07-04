# A/B Benchmark Test 6: Independent Task-Level Free-Choice A/B (llama.cpp)

**Date**: 2026-07-03
**Marrow version**: v0.1.2 + PR #60 (rmcp 2.1) + PR #61 branch (installed binary: 0.1.2)
**Substrate**: llama.cpp @ `fdb1db877` — 16,333 symbols, 1,103 C/C++/CUDA files, not authored by us, pre-indexed as `llama_cpp`
**Test subjects**: 6 fresh cold-start subagents (Claude Sonnet), zero context from the orchestrating session
**Design**: 3 tasks × 2 arms. Arm A: free tool choice with marrow available + marrow's shipped routing guidance ("marrow for structural questions; native tools for exact-text work"). Arm B: native Read/Grep/Glob/shell only. Task text byte-identical across arms.
**Ground truth**: established by the orchestrator via grep + manual verification BEFORE any arm ran ([benchmark-6-ground-truth.md](benchmark-6-ground-truth.md), committed verbatim); scoring rules fixed at the same time.

**Protocol deviation (disclosed)**: Arm A used the `marrow query <symbol> llama_cpp` CLI rather than MCP transport (MCP server not connected in the parent session, so subagents could not inherit it). CLI output was verified earlier the same day to be content-identical to the MCP capsule+impact path.

---

## Tasks

- **T1**: List every direct call site of `llama_batch_get_one` (file:line, all languages, excluding declaration + definition). Ground truth: 30 sites / 14 files, all C/C++.
- **T2**: Name the public sampling API used after `llama_decode`, cite its call site in `examples/simple/simple.cpp`, describe the token feedback loop. Ground truth: `llama_sampler_sample`, simple.cpp:182, batch_get_one(&new_token_id,1) → next decode.
- **T3**: Same shape as T1 for `llama_memory_clear`. Ground truth: 30 sites / 14 files — **including 4 Swift call sites marrow does not index**, deliberately chosen as a discriminator: full marks in arm A require combining marrow with native search.

## Results

| Task | Arm | Quality (vs ground truth) | Tool calls | Tokens | Wall time |
|---|---|---|---|---|---|
| T1 | A (marrow) | List **30/30** (100% P/R); stated total "29" — arithmetic slip in the summary line | 3 | 34,798 | 36.4s |
| T1 | B (native) | **30/30**, count correct | 3 | 31,290 | 20.2s |
| T2 | A (marrow) | **100%** (all 3 rubric parts; bonus: located the *definition* at llama-sampler.cpp:806, not just the header) | 2 | 32,923 | 12.6s |
| T2 | B (native) | **100%** | 3 | 29,183 | 13.5s |
| T3 | A (marrow) | **30/30 incl. all 4 Swift sites**, count correct | 3 | 36,435 | 50.4s |
| T3 | B (native) | **30/30 incl. all 4 Swift sites**, count correct | 2 | 32,535 | 28.3s |

**Aggregate**: Quality — arm B 300/300, arm A 295/300 (the single deduction is T1-A's miscounted summary line over a fully correct list). Tokens — A 104,156 vs B 93,008 (**native ~11% cheaper**). Wall time — A 99.4s vs B 62.0s (**native ~38% faster**). Tool calls — 8 vs 8.

## Qualitative observations (the interesting part)

1. **Free-choice arm A agents routed like marrow tells them to.** In T1 the agent ran grep first and used marrow as a cross-check; in T3 it ran marrow first and used grep to cover non-indexed languages. Both match the shipped guidance ("native tools for exact-search work"). The tool's own routing advice is empirically what capable agents converge on.
2. **Marrow's structured output prevented a precision error class.** T1-A explicitly used marrow's direct-vs-transitive caller separation to exclude depth-2+ impact entries (`common_init_from_params` callers etc.) from the "direct call sites" list — grep gives no such distinction (though the grep-only arm avoided the trap here too, since the raw string only matches direct sites).
3. **The Swift discriminator worked as designed — in both directions.** T3-A knew (or discovered) that marrow's impact list was incomplete for Swift and grepped to fill the gap, scoring 30/30. A marrow-only strategy would have scored 26/30.
4. **T2-A reached the answer in 2 calls with more depth** (definition + mechanism from one capsule), the one task shape where the capsule paid off over search.

## Verdict

**On exact-name caller enumeration — grep's home turf — native tools match marrow-assisted agents on quality and beat them modestly on cost (~11%) and latency (~38%).** This is a deliberate stress test of the task class *least* favorable to marrow: the tasks were chosen for objective scorability, which selects for exact-string search. Marrow's differentiated value measured in earlier benchmarks — condensed capsule delivery (87–98% token reduction vs reading the graph neighborhood, [Rails monolith](production-rails-monolith.md)), unknown-symbol discovery, direct-vs-transitive separation, multi-hop impact — is mostly orthogonal to this task class, with T2 and observation #2 the visible traces of it here.

**Implication for README claims**: do not state or imply that marrow beats native tooling on simple caller searches. The honest posture (now reflected in the README caveats) is: capsules win when the alternative is *reading code into context*; grep wins when the question is *where does this exact string appear*; marrow's own agent guidance already routes accordingly, and this benchmark confirms free-choice agents follow it.

## Threats to validity

- n=1 run per cell, single model (Sonnet), single repository.
- Token counts are harness-reported per-subagent totals (include prompt/system overhead identically in both arms; deltas are meaningful, absolutes overstate task cost).
- Tool logs are agent self-reports (spot-consistent with harness `tool_uses` counts in all six runs).
- Arm A's marrow access was CLI, not MCP (content-identical, transport overhead differs).
- Task selection is biased toward grep-favorable shapes by design (objective scorability); capsule-value tasks were measured in benchmarks #4 and #5 instead.
