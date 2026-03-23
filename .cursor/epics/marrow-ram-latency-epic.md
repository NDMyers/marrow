---
title: "Marrow RAM & Latency Hardening (Accrualify-scale)"
status: draft
---

# Epic: Marrow RAM & Latency Hardening (Accrualify-scale)

## Epic progress (in-repo)

**Legend:** `[ ]` not started · `[~]` in progress · `[x]` done

**Milestones (exit criteria from milestone table)**

**M0 — Measurement**

- [x] Benchmark harness runs on a fixed fixture (Accrualify path + `repo_id` recorded in runbook; **paste fixture `git rev-parse HEAD` in table** when convenient)
- [x] Baseline numbers + profiling artifacts checked in or documented (see `docs/perf-baseline-runbook.md` baseline row, 2026-03-23)
- [x] Stories **001–003** done

**M1 — Phase A**

- [~] Measurable improvement vs. M0 on RSS and/or ingest/MCP latency *(re-run Accrualify harness with/without `MARROW_SKIP_POST_INGEST_MAINTENANCE` to quantify)*
- [x] No new unbounded allocations on audited paths *(capsule/impact SQL `LIMIT`s + shared ingest path)*
- [x] **004–006**, **014** (audit) progressed per AC

**M2 — Phase B**

- [ ] **008**, **009–011** meet AC
- [ ] SLA trending toward ≤120 s on benchmark
- [ ] Peak RSS trend ≤ target under same fixture

**M3 — Phase C (conditional)**

- [ ] **013** spike complete with decision record (adopt / defer / reject)
- [ ] If adopted: prototype meets spike AC

**Stories (dependency-friendly order)**  
_After **002**: **004**, **005**, **006**, and **014** can run in parallel (keep **014** early per workflow). **009** and **010** can run in parallel once **002** is stable._

- [x] MARROW-PERF-001 — Baseline profiling & memory methodology
- [x] MARROW-PERF-002 — Accrualify-scale benchmark harness
- [x] MARROW-PERF-003 — Environment defaults & tuning knobs documentation
- [x] MARROW-PERF-004 — Ingest parallelism & parser cache tuning (Phase A) *(defaults documented; `MARROW_INGEST_THREADS` unchanged—re-tune vs harness as needed)*
- [x] MARROW-PERF-005 — Remove or gate `VACUUM` / heavy PRAGMA on hot path *(post-ingest = WAL checkpoint + `incremental_vacuum` only; gated by `MARROW_SKIP_POST_INGEST_MAINTENANCE`; `marrow maintenance`)*
- [x] MARROW-PERF-006 — Unify CLI vs MCP ingest paths & verify allocation shape *( `index` + TUI `run_index_command` → `run_ingestion`; walk aligns with `.marrowrc.json`)*
- [x] MARROW-PERF-014 — Capsule & MCP payload caps audit *(env-capped outbound/inbound/impact; see `docs/mcp-payload-limits.md`)*
- [x] MARROW-PERF-007 — Spec: incremental / streaming ingest *(Peter `Task`; bounded queue + spill + single txn)*
- [x] MARROW-PERF-008 — Implement incremental / streaming ingest MVP *(Linus `Task`; Cobalt follow-up: panic `catch_unwind` + spill read caps + `0600` spill on Unix; Ralph `cargo test` ALL_PASS)*
- [x] MARROW-PERF-009 — Narrow `name_to_ids` / `CALLS` rebuild scope
- [ ] MARROW-PERF-010 — SQLite indexes & batch insert tuning
- [ ] MARROW-PERF-011 — Incremental edge rebuild
- [ ] MARROW-PERF-012 — Cross-repo pass: opt-in or scoping
- [ ] MARROW-PERF-013 — Spike spec: optional worker process (Phase C)
- [ ] MARROW-PERF-015 — CI / regression gate for perf & RSS

**Last updated:** 2026-03-23 (M2: **007–009** — bounded ingest queue + spill; narrow `name_to_ids` via temp-table join + `test_partial_reingest_resolves_calls_to_unchanged_file`)

Agents and maintainers should update the checkboxes above when merging story PRs (use `[~]` on the active branch if helpful).

---

## Epic metadata

**Title:** Marrow RAM & Latency Hardening (Accrualify-scale)

**Summary:** Harden Marrow’s Rust ingestion pipeline (tree-sitter, `rusqlite`, `rayon`, MCP) so typical developer machines (8–16 GB RAM) stay under a ~5 GB peak RSS during large-repo indexing and interactive MCP/UI use, and so Accrualify-scale workloads complete full indexing plus scoped MCP operations within a **≤120 s** budget. Delivery follows the parent plan’s **Phase A quick wins → Phase B structural improvements → Phase C optional rewrite/isolation** (worker process or similar), with measurement-first gating and squad-owned stories (Peter specs, Linus implementation, Cobalt review, Ralph benchmarks/tests).

**Prior plan link (themes):** *Marrow RAM & latency development plan* — Phase A (quick wins: threading, SQLite pragmas, hot-path hygiene) → Phase B (structural: streaming/incremental ingest, narrower graph rebuilds, indexes/batching) → Phase C (optional: out-of-process worker, stronger isolation).

**Success metrics (quantitative DoD):**

1. **Peak RSS:** Under representative ingest + MCP capsule/query load, **peak RSS ≤ ~5 GB** on 8–16 GB hosts (documented repro: corpus revision, `RAYON_NUM_THREADS`, DB path, MCP calls).
2. **Latency SLA:** On the **Accrualify-scale benchmark fixture**, **cold** full ingest + defined scoped MCP/UI operations complete in **≤120 s** (wall clock; median of N runs recorded in harness).
3. **Allocation hygiene:** Hot paths for CLI and MCP ingestion **do not** fully materialize the parsed corpus in a single grow-only `Vec` (or equivalent); verified by code audit + heap profiling on the benchmark.
4. **Regression control:** CI or a scripted **regression harness** fails when RSS or latency exceeds **documented thresholds** vs. baseline commit (or emits blocking report for release branches).

---

## Milestone structure

| Milestone | Goal | Exit criteria | Duration band |
|-----------|------|---------------|---------------|
| **M0 — Measurement** | Reproducible baseline for RAM, latency, and allocation behavior. | Benchmark harness runs on a fixed fixture; baseline numbers + profiling artifacts checked in or documented; stories **001–003** done. | ~1–2 weeks |
| **M1 — Phase A** | Quick wins: safer defaults, hot-path fixes, low-risk tuning. | Measurable improvement vs. M0 on RSS and/or ingest/MCP latency; no new unbounded allocations on audited paths; **004–006, 014** (audit) progressed per AC. | ~1–3 weeks |
| **M2 — Phase B** | Structural: DB batching/indexes, narrower rebuilds, incremental/streaming ingest. | **008, 009–011** meet AC; SLA trending toward ≤120 s on benchmark; peak RSS trend ≤ target under same fixture. | ~3–6 weeks |
| **M3 — Phase C (conditional)** | Optional isolation / worker spike if M2 insufficient. | **013** spike complete with decision record (adopt / defer / reject); if adopted, prototype meets spike AC. | ~1–4 weeks |

---

## Issue / story breakdown

| ID | Title | Description | Acceptance criteria | Dependencies | Primary owner | Risk / notes |
|----|-------|-------------|---------------------|--------------|---------------|--------------|
| **MARROW-PERF-001** | Baseline profiling & memory methodology | Establish how Marrow is profiled (RSS, heap, time) on macOS Apple Silicon for ingest and MCP-heavy sessions. Align on tools (e.g. Instruments, `time`, optional `dhat`/heap tracks) and what “peak RSS” means for this epic. | 1) Written runbook: env vars, corpus path, commands for CLI ingest and MCP scenario. 2) One captured baseline table (RSS peak, wall time, DB size) for the reference workload. 3) Known noise factors listed (parallelism, cold cache). | — | Ralph → Peter (Ralph runs measurements; Peter codifies methodology if ambiguous) | Wrong workload invalidates all later comparisons. |
| **MARROW-PERF-002** | Accrualify-scale benchmark harness | Add or extend a **deterministic** benchmark harness in-repo (Rust `criterion`/`iai` or integration binary + script) that runs full ingest and a **fixed** set of MCP-like queries against a **pinned** large fixture (submodule, tarball URL, or generation script). | 1) Single command produces JSON or structured log: ingest wall time, peak RSS (if captured), query phase timing. 2) Fixture version is pinned (hash or tag in doc). 3) Harness documented in README or `docs/`. | 001 | Ralph (+ Linus for harness code) | Fixture licensing/size may block CI; use optional feature flag. |
| **MARROW-PERF-003** | Environment defaults & tuning knobs documentation | Document safe defaults for `RAYON_NUM_THREADS`, SQLite pragmas (`journal_mode`, `synchronous`, cache sizes), and MCP batch sizes for 8–16 GB machines. | 1) Doc lists each knob, default, and tradeoff. 2) “Accrualify-scale” preset section references benchmark from **002**. 3) No code change required for AC; optional follow-up issues reference this doc. | 001 | Peter → Linus (Peter outlines; Linus lands doc in repo) | Doc drift if not linked from epic. |
| **MARROW-PERF-004** | Ingest parallelism & parser cache tuning (Phase A) | Tune `rayon` parallelism, batch/chunk sizes, and any parser/query cache behavior so throughput improves without RSS spikes beyond epic budget. | 1) Before/after numbers from **002** on same fixture. 2) Default behavior on 8 GB host does not OOM; peak RSS recorded. 3) Changes are configurable or justified as new default with doc update in **003**. | 002 | Linus | Over-aggressive parallelism can regress latency on small repos. |
| **MARROW-PERF-005** | Remove or gate `VACUUM` / heavy PRAGMA on hot path | Ensure `VACUUM` (or similar full-db operations) are not triggered during normal ingest or MCP request paths; expose explicit maintenance command if needed. | 1) Code audit lists all `VACUUM`/migrate paths. 2) Ingest + MCP benchmark (**002**) shows no unexpected `VACUUM`. 3) If manual `VACUUM` remains, documented and CLI-gated. | 002 | Linus | Easy to miss dynamic SQL paths; Cobalt should review. |
| **MARROW-PERF-006** | Unify CLI vs MCP ingest paths & verify allocation shape | Single authoritative ingest pipeline for CLI and MCP; verify neither path forces full in-memory lists of all files/symbols beyond bounded batches. | 1) Architecture note: entry points call shared core (or justified exception). 2) Audit checklist signed off: no unbounded `Vec` of entire AST corpus on hot path. 3) **002** + heap sample confirms improvement or “no regression” vs. baseline. | 002 | Peter → Linus | Hidden duplication is common; spec must name files/modules. |
| **MARROW-PERF-007** | Spec: incremental / streaming ingest | Peter produces design for chunk-wise or incremental ingest (file batches, transaction boundaries, resume checkpoints) suitable for `rusqlite` + `rayon`. | 1) Spec sections: Summary, Architecture, File plan, AC, Edge cases (per squad template). 2) Explicit memory upper bound argument (bounded queues). 3) Migration/compatibility with existing DB schema stated. | 002, 006 | Peter | Ambiguity here wastes Linus cycles; Cobalt flags SPEC_DISPUTE if unclear. |
| **MARROW-PERF-008** | Implement incremental / streaming ingest MVP | Implement **007** with minimal scope: bounded batches, transactional inserts, measurable RSS reduction on **002**. | 1) All **007** AC reflected in tests or benchmark. 2) **002** shows reduced peak RSS or ≤120 s ingest (whichever was binding). 3) No `unwrap`/`expect` on production paths (project rule). | 007 | Linus | Highest integration risk in epic; Ralph runs extended tests. |
| **MARROW-PERF-009** | Narrow `name_to_ids` / `CALLS` rebuild scope | Reduce full-graph recomputation when updating symbols or edges; rebuild only affected scopes/repos where possible. | 1) Documented before/after algorithmic scope (which tables/rows touched). 2) Correctness: existing integration tests pass; new test covers partial update case. 3) **002** shows lower ingest or rebuild phase time when applicable. | 002 | Peter → Linus | Easy to introduce stale graph edges; Cobalt + Ralph critical. |
| **MARROW-PERF-010** | SQLite indexes & batch insert tuning | Add/maintain indexes for hot queries; tune batch sizes and prepared statements for `nodes`/`edges` inserts; keep WAL settings per CLAUDE.md. | 1) Query plan captured for top N hot queries (documented). 2) Insert throughput improved or RSS reduced vs. baseline on **002**. 3) Migration notes if schema changes. | 002 | Linus | Index bloat can hurt write speed; measure don’t guess. |
| **MARROW-PERF-011** | Incremental edge rebuild | After **009**/**010**, avoid rebuilding all `CALLS` (and related edges) when only a subset of files changed. | 1) Clear invalidation rules (per-file, per-repo). 2) Tests prove correctness when files added/removed/changed. 3) **002** shows measurable reduction in edge phase. | 009, 010 | Linus | Interacts with cross-repo semantics (**012**). |
| **MARROW-PERF-012** | Cross-repo pass: opt-in or scoping | Make full cross-repo graph passes explicit opt-in or scoped by default so large monorepos/multi-root don’t trigger accidental full scans. | 1) Default behavior documented; breaking changes called out. 2) MCP/CLI flags or config control scope. 3) **002** includes multi-root scenario with expected time bound. | 006 | Peter → Linus | Product behavior change; needs user-visible release note. |
| **MARROW-PERF-013** | Spike spec: optional worker process (Phase C) | If M2 misses SLA, define an out-of-process ingest/index worker with IPC boundaries and failure modes (stdio MCP stays responsive). | 1) Spike doc: pros/cons, protocol sketch, security notes (no new network attack surface without explicit scope). 2) Go/No-go recommendation. 3) If go: thin prototype plan only (no full rewrite in spike). | 002, 007 | Peter (+ Linus spike prototype optional) | Scope creep into full rewrite; keep spike time-boxed. |
| **MARROW-PERF-014** | Capsule & MCP payload caps audit | Audit `get_context_capsule`, `analyze_impact`, and related responses for max depth, max nodes, max bytes; enforce caps to protect client RAM. | 1) Table of tools, current limits, proposed limits. 2) Hard caps or streaming strategy documented. 3) Regression test: oversized request fails gracefully with clear error. | 002 | Peter → Linus | Over-capping hurts UX; tune with real Accrualify queries. |
| **MARROW-PERF-015** | CI / regression gate for perf & RSS | Wire **002** (or lightweight subset) into CI or nightly job with thresholds; fail or annotate PRs when regressions exceed tolerance. | 1) CI job definition checked in (GitHub Actions or equivalent). 2) Thresholds versioned next to baseline. 3) Documented flake policy (retries, machine variance). | 002, 004 | Ralph (+ Linus for wiring) | CI machine variance; prefer relative regression or self-hosted runner note. |

**Story count:** 15 (≥12 required).

---

## Workflow for the squad

**Execution order (DAG, prose):**

1. **M0:** Land **001** (methodology) and **002** (harness). **003** can parallel once **001** exists. No feature work without **002**.
2. **Phase A:** **004**, **005**, **006** in parallel after **002**, with **014** (caps audit) early to protect clients. **Cobalt** reviews **005**, **006**, **014** (security, perf, spec compliance) after **Linus** PRs; ingestion and DB paths are security-sensitive for DoS via pathological inputs.
3. **Phase B:** **007** (Peter) before **008**. **009** and **010** can proceed in parallel once **002** is stable; **011** follows **009**+**010**. **012** follows **006** and can overlap **008** once behavior is understood.
4. **Phase C:** **013** only if M2 exit criteria miss SLA or peak RSS; optional **Linus** prototype after Peter spike approval.
5. **Ralph:** After **002**, Ralph runs benchmark passes for every milestone exit. After **004–006, 008–012, 014**, Ralph executes targeted integration tests and benchmark comparisons. **015** runs continuously once merged.

**When to invoke Cobalt:** After each **Linus** PR touching `ingestion`, `rusqlite` schema/migrations, MCP tool handlers, or cap enforcement. After **Peter** only if resolving SPEC_DISPUTE amendments.

**When to invoke Ralph:** After M0 (baseline numbers), after M1/M2/M3 milestone exits, and on every story whose AC includes benchmark or test evidence (**001–002**, **004–006**, **008–012**, **014–015**).

---

## GitHub appendix

### Suggested labels

- `epic:perf`
- `area:ingestion`
- `area:sqlite`
- `area:mcp`
- `phase:A` | `phase:B` | `phase:C`
- `milestone:M0` … `M3`
- `agent:peter` | `agent:linus` | `agent:cobalt` | `agent:ralph`
- `type:spike`
- `risk:high`

---

### Paste: Epic issue body

~~~markdown
## Summary
Epic to reduce Marrow peak RAM and end-to-end latency on large (Accrualify-scale) codebases while keeping MCP/CLI behavior deterministic. Phased: **M0 Measurement → M1 Phase A → M2 Phase B → M3 Phase C (optional)**.

## Link to plan themes
Parent plan: **Phase A** quick wins → **Phase B** structural (streaming ingest, narrower graph rebuilds, SQLite tuning) → **Phase C** optional worker/isolation.

## Success metrics
- Peak RSS ≤ ~5GB (8–16GB machine, documented workload)
- Cold ingest + scoped MCP ops ≤ 120s on Accrualify-scale benchmark fixture
- No unbounded full-corpus materialization on hot ingest paths (audit + profile proof)
- Regression harness in CI or scripted gate

## Child issues
Track: MARROW-PERF-001 … MARROW-PERF-015 (see epic doc `.cursor/epics/marrow-ram-latency-epic.md`)

## Milestones
- M0: harness + baseline
- M1: Phase A tuning / hot-path hygiene / caps audit
- M2: Phase B streaming + SQLite + incremental graph
- M3: Phase C worker spike (conditional)
~~~

---

### Paste: Child issue — MARROW-PERF-002 (representative: harness)

~~~markdown
**Epic:** Marrow RAM & Latency Hardening  
**Milestone:** M0 Measurement  
**Labels:** `area:ingestion`, `milestone:M0`, `agent:ralph`, `agent:linus`

## Description
Add or extend a deterministic benchmark that runs full Marrow ingest and a fixed MCP-like query sequence against a pinned large fixture, emitting structured timing (and optional RSS).

## Acceptance criteria
1. One command produces structured output (JSON or log) with ingest wall time and query-phase timing.
2. Fixture revision is pinned and documented.
3. README or `docs/` explains how to obtain/run the benchmark.

## Dependencies
- MARROW-PERF-001

## Owners
- Ralph: methodology, running baseline, acceptance evidence
- Linus: implementation of harness binary/scripts

## Notes
If full fixture cannot live in CI, gate behind feature flag or nightly job.
~~~

---

### Paste: Child issue — MARROW-PERF-006 (representative: architecture / allocations)

~~~markdown
**Epic:** Marrow RAM & Latency Hardening  
**Milestone:** M1 Phase A  
**Labels:** `area:ingestion`, `area:mcp`, `phase:A`, `agent:peter`, `agent:linus`

## Description
Unify CLI and MCP ingestion around a single core pipeline and prove that hot paths do not allocate an unbounded in-memory representation of the entire parsed corpus.

## Acceptance criteria
1. Written architecture note listing shared entry points vs exceptions.
2. Allocation audit checklist completed and linked in PR.
3. Benchmark MARROW-PERF-002 + heap sample shows improvement or non-regression vs baseline.

## Dependencies
- MARROW-PERF-002

## Owners
- Peter: spec / audit criteria if ambiguous
- Linus: implementation

## Notes
Cobalt gate required before merge (DoS and perf regressions).
~~~

---

### Paste: Child issue — MARROW-PERF-015 (representative: CI / regression)

~~~markdown
**Epic:** Marrow RAM & Latency Hardening  
**Milestones:** M1–M2 (initial wiring), ongoing  
**Labels:** `epic:perf`, `area:ingestion`, `agent:ralph`, `agent:linus`

## Description
Integrate the perf benchmark (or a lightweight subset) into CI or a scheduled job with versioned thresholds to catch RSS and latency regressions.

## Acceptance criteria
1. Workflow definition checked into the repo.
2. Thresholds stored next to documented baseline.
3. Flake policy documented (retries, runner variance).

## Dependencies
- MARROW-PERF-002
- MARROW-PERF-004 (recommended so defaults stabilize before gating)

## Owners
- Ralph: defines thresholds and validates signal-to-noise
- Linus: wires CI/job

## Notes
Prefer relative regression or pinned self-hosted runner if cloud runners are too noisy.
~~~

---

*End of epic document.*
