# Benchmark 6 Ground Truth (established 2026-07-03, BEFORE any arm ran)

Repo: D:\Coding\llama.cpp @ fdb1db877 (16,333 symbols indexed as repo_id llama_cpp)

## T1 — direct call sites of `llama_batch_get_one` (excl. decl llama.h:924, def src/llama-batch.cpp:863)

examples/debug/debug.cpp:197
examples/idle/idle.cpp:60
examples/eval-callback/eval-callback.cpp:29
examples/speculative-simple/speculative-simple.cpp:133
examples/speculative-simple/speculative-simple.cpp:134
examples/speculative/speculative.cpp:185
examples/speculative/speculative.cpp:186
examples/speculative/speculative.cpp:187
tools/llama-bench/llama-bench.cpp:2109
tools/llama-bench/llama-bench.cpp:2131
examples/simple-chat/simple-chat.cpp:114
examples/simple-chat/simple-chat.cpp:151
examples/simple/simple.cpp:149
examples/simple/simple.cpp:162
examples/simple/simple.cpp:200
common/common.cpp:1420
common/common.cpp:1429
common/common.cpp:1474
common/common.cpp:1986
common/common.cpp:2018
common/common.cpp:2028
common/common.cpp:2038
tools/cvector-generator/cvector-generator.cpp:349
examples/lookup/lookup.cpp:95
examples/lookup/lookup.cpp:96
tools/completion/completion.cpp:572
examples/lookahead/lookahead.cpp:104
examples/lookahead/lookahead.cpp:105
tests/test-thread-safety.cpp:108
tests/test-thread-safety.cpp:133

TOTAL: 30 call sites, 14 files. No Swift sites.

## T2 — sampling after decode

(a) Function: `llama_sampler_sample`
(b) Call site: examples/simple/simple.cpp:182 (`new_token_id = llama_sampler_sample(smpl, ctx, -1);`)
(c) The sampled token is checked against end-of-generation, appended to output, then wrapped in a
    new single-token batch via `llama_batch_get_one(&new_token_id, 1)` (simple.cpp:200) and fed to
    the next `llama_decode` iteration.

## T3 — direct call sites of `llama_memory_clear` (excl. decl include/llama.h:715, def src/llama-context.cpp:3831)

tools/batched-bench/batched-bench.cpp:153
common/common.cpp:1431
common/common.cpp:1467
common/common.cpp:1495
examples/retrieval/retrieval.cpp:87
tests/test-save-load-state.cpp:174
tests/test-save-load-state.cpp:246
examples/idle/idle.cpp:66
examples/idle/idle.cpp:95
examples/llama.android/lib/src/main/cpp/ai_chat.cpp:175
examples/llama.android/lib/src/main/cpp/ai_chat.cpp:187
examples/llama.android/lib/src/main/cpp/ai_chat.cpp:201
examples/llama.android/lib/src/main/cpp/ai_chat.cpp:269
tools/imatrix/imatrix.cpp:849
examples/llama.swiftui/llama.cpp.swift/LibLlama.swift:213
examples/llama.swiftui/llama.cpp.swift/LibLlama.swift:226
examples/llama.swiftui/llama.cpp.swift/LibLlama.swift:245
examples/llama.swiftui/llama.cpp.swift/LibLlama.swift:295
tools/cvector-generator/cvector-generator.cpp:348
tools/completion/completion.cpp:376
tools/mtmd/mtmd-cli.cpp:498
examples/embedding/embedding.cpp:41
tools/llama-bench/llama-bench.cpp:2301
tools/llama-bench/llama-bench.cpp:2359
tools/perplexity/perplexity.cpp:367
tools/perplexity/perplexity.cpp:553
tools/perplexity/perplexity.cpp:930
tools/perplexity/perplexity.cpp:1223
tools/perplexity/perplexity.cpp:1602
tools/perplexity/perplexity.cpp:1803

TOTAL: 30 call sites, 14 files. Includes 4 Swift sites (marrow does not index Swift —
a perfect arm-A discriminator: full marks require combining marrow with native search).

## Scoring (fixed now)

- T1/T3: precision + recall of the file:line list (line off-by-few tolerated if file+context right).
- T2: (a) exact function name 50%, (b) correct site 25%, (c) feedback-loop description correct 25%.
- Cost: tool-call count from each agent's TOOL LOG + harness-reported usage if available; wall time.
- Deviation from protocol: arm A uses the marrow CLI (`marrow query <symbol> llama_cpp`), not MCP —
  MCP server not connected in the parent session; CLI output verified identical in content.
