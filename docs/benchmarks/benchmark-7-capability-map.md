# A/B Benchmark Test 7: Capability-Map Campaign (marrow + llama.cpp, five task classes)

**Date**: 2026-07-03
**Marrow**: installed v0.1.2 binary; marrow repo re-indexed same day (1,234 symbols, self-check 17/17); llama.cpp @ fdb1db877 (16,333 symbols)
**Design**: pre-registered ground truth + scoring ([benchmark-7-ground-truth.md](benchmark-7-ground-truth.md), committed verbatim) BEFORE any run; 18 fresh cold-start Sonnet subagents (+2 controls) in bounded waves; 4 cells reused from [Benchmark 6](benchmark-6-free-choice-ab.md). Arm A = free choice with marrow CLI (`marrow query`, `marrow context`) + shipped routing guidance; Arm B = native Read/Grep/Glob/shell only. Task text identical across arms.

**Cost methodology (fixes Benchmark 6 weaknesses)**: marginal tokens = cell total − same-arm control (CTRL-A 26,142; CTRL-B 25,975; the 167-token difference exactly matches the marrow preamble). MCP schema tax measured separately: a real MCP deployment injects **~2,132 tokens/session** of tool schemas (8 tools; `run_pipeline` ≈ 900 alone) that the CLI-based arm A did not pay — arm A costs are a floor for MCP deployments. Wall time reported as coarse signal only (concurrent waves).

## Results (marginal tokens; quality vs pre-registered ground truth)

| Class | Cell | Arm A (marrow) | Arm B (native) | Quality A / B | Cost verdict |
|---|---|---|---|---|---|
| C1 exact-string enumeration | C1-M `increment_stat` (23 sites + 2 string-literal traps) | 11,831 · 11 calls | 5,954 · 7 calls | 100 / 100 (both caught traps) | **native 2.0×** |
| C1 (from B6) | T1/T3 llama.cpp caller lists | — | — | ~98 / 100 | native (abs ~11%, marginal larger) |
| C2 caller map + test/prod | C2-M `ingest_repo` (3 prod + 20 test fns) | **22,576** · 19 calls | 35,537 · 17 calls | 100 / 100 | **marrow 1.6×** |
| C2 | C2-L `llama_token_to_piece` (9 fns, wrapper, comment trap, Swift) | **15,981** · 15 calls | 25,414 · 20 calls | 100 / 100 | **marrow 1.6×** |
| C3 change impact (direct callers) | C3-M `resolve_symbol_or_disambiguate` (7 callers incl. 2-day-old doctor.rs) | 12,986 · 11 calls | 11,098 · 12 calls | 100 / 100 (both passed freshness probe) | wash |
| C4 discovery by description | C4-M hash-based re-parse decision | 10,012 · 4 calls | 8,572 · 5 calls | 100 / 100 (GT amended, see below) | native 1.2× |
| C4 | C4-L end-of-generation check | 6,589 · 7 calls | 5,118 · 5 calls | 100 / 100 | native 1.3× |
| C5 single-function comprehension | C5-M `compile_context_packet_for_format` (9 callees) | 4,448 · **1 call** | 4,118 · 2 calls | 100 / ~95 (B's list complete but muddled with builtins) | wash |
| C5 | C5-L `llama_sampler_sample` (12 callees + fast path) | 5,026 · **1 call** | 3,387 · 3 calls | 100 / 100 | native 1.5× |

Quality was at ceiling almost everywhere: with a capable model, tool choice did not change answer correctness on these task shapes — it changed cost, round-trips, and latency.

## The capability map (what to advertise, truthfully)

1. **Marrow's decisive measured win: symbol-granularity caller mapping (C2)** — "which functions call X, and what kind of code are they" — **~1.6× cheaper at identical quality, replicated on both repos**. The mechanism is visible in the tool logs: the native arm pays an *attribution tax* (10+ reads and boundary checks to determine which function/test-module contains each grep hit — C2-M-B even ran `wc -l` to prove a test module spans to EOF), which is exactly the table marrow's graph already stores.
2. **Native's decisive win: exact-string enumeration (C1)** — ~1.5–2× cheaper, confirmed on both repos (three cells + B6). Grep's output *is* the deliverable. Marrow's own guidance routes this away, correctly.
3. **Comprehension of a known, short function (C5): cost wash, round-trip win for marrow.** The capsule answered in ONE call with perfect callee tables both times; native needed 2–3 targeted calls at similar marginal tokens. **Important honesty note**: the 87–98% capsule token reductions in the [Rails benchmark](production-rails-monolith.md) compare against reading the *entire graph neighborhood (~20 files)*; a skilled agent doing one surgical read of a short, named function achieves comparable marginal cost. The capsule's practical value on small targets is fewer round trips, guaranteed-complete callee/caller tables, and bounded output — not raw token mass. Reductions scale with how much code the alternative would read.
4. **Discovery by behavioral description (C4): no marrow advantage measured** (native 1.2–1.3× cheaper). `marrow context` was exercised (C4-M-A used it first) but keyword-grep reached the same functions cheaply. Caveat: C4-L was contaminated by model prior knowledge of llama.cpp's API in both arms.
5. **Small-blast-radius impact (C3): wash.** With 7 callers across 2 files, grep+read matches the graph. (B5's llama_decode data suggests the graph pulls ahead as caller counts and transitive depth grow — not re-measured here.)

## Cross-cutting observations

- **Verification paranoia dominates arm-A cost.** Diligent agents re-verify marrow's tables with native reads (C1-M-A ran BOTH full workflows — capsule and complete grep+9 reads). Where marrow won (C2), it won despite this. Guidance that capsule caller tables are exhaustive for indexed languages could cut arm-A cost further; whether agents should extend that trust is a product question (Swift-style index gaps argue for calibrated, not blind, trust).
- **Round-trips**: arm A completed C5 cells in 1 call vs 2–3, and C2 cells with materially less file I/O. In latency-sensitive or orchestration-billed settings this matters beyond tokens.
- **Real-world MCP arm A costs ~2.1k tokens/session more than measured here** (schema tax), amortized across all queries in a session.
- **Windows friction datum**: one arm-A agent's first marrow invocation failed (`cd X && marrow` in the bash tool) before succeeding via PowerShell.

## Ground-truth integrity

Two pre-registration corrections, both applied symmetrically (A/B comparison unaffected):
1. **C4-M**: both arms independently converged, with source evidence, on `parallel_hash_and_parse_candidates` (ingestion.rs:748; decision at 767–770). Adjudicated by reading: they were right — the registered `compute_changeset` is the orchestrator one level up. GT amended post-run; both arms full credit.
2. **C5-M**: `as_str` (called at context.rs:528) is a real direct callee missing from the registered 9-item list; arm B listed it. Not penalized.

## Threats to validity

- n=1 per cell (2 samples per class via the two repos, C3/C5-shape excepted); single model (Sonnet); marginal-token estimates assume control runs capture fixed overhead exactly; concurrent execution makes wall-time noisy; arm A used the CLI, not MCP transport (content-identical; costs are a floor); C4-L contaminated by prior model knowledge; task classes with objectively checkable ground truth still under-sample open-ended "explain this subsystem" work where capsule-vs-neighborhood economics (per the Rails benchmark) most favor marrow.
