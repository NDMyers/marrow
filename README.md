# Marrow (AST Context Engine)

![CI](https://github.com/NDMyers/marrow/actions/workflows/ci.yml/badge.svg)
![npm version](https://img.shields.io/npm/v/@nickm-swe/marrow?label=npm)
![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Install size](https://packagephobia.com/badge?p=@nickm-swe/marrow)

Marrow is a high-performance, local, and language-agnostic context compiler and Model Context Protocol (MCP) server written in Rust. It parses your codebase with `tree-sitter`, builds a cross-repository dependency graph in a local SQLite database, and serves deterministic structural context — callers, blast radius, condensed code capsules — to AI coding agents. No embeddings, no provider SDKs, no network calls.

## Install

```bash
npm install -g @nickm-swe/marrow
```

The npm installer downloads a verified (SHA256) release binary for macOS, Linux, or Windows. To build from source instead, see [Building from source](#building-from-source).

## Quick start

```bash
marrow init                                                      # workspace setup (.marrow/, .marrowrc.json)
marrow index                                                     # index the current directory
marrow context "trace request flow" --repo my_repo --format markdown
marrow integrate                                                 # wire Marrow into your editor/agent
```

## Commands

Run `marrow` with no arguments for an interactive TUI menu, or `marrow --help` for the full list.

| Command | Purpose |
|---------|---------|
| `marrow mcp` | Start the MCP stdio server (used by editor/agent integrations). |
| `marrow init` | Initialize workspace config (`.marrow/`, `.marrowrc.json`). |
| `marrow index` | Index the current workspace (same pipeline as MCP `ingest_repo`). |
| `marrow watch` | Watch the workspace for changes and re-index incrementally. |
| `marrow context <task>` | Compile a provider-neutral context packet (markdown/JSON). |
| `marrow query <symbol> <repo_id>` | Print a symbol's context capsule plus impact analysis. |
| `marrow benchmark [<symbol> <repo_id>]` | Token-reduction benchmark (interactive wizard when run bare). |
| `marrow perf-harness` | Ingest + query performance benchmark (`--json` for machine output). |
| `marrow integrate` | Write/print MCP setup for supported agent targets. |
| `marrow validate` | Check workspace setup and integration config. |
| `marrow maintenance` | WAL checkpoint + `incremental_vacuum` on `graph.db`. |
| `marrow ui` / `marrow ui-app` | Open the dashboard / manage the desktop app entry. |
| `marrow daemon [install\|uninstall\|status]` | Background daemon and autostart management. |
| `marrow status` / `marrow stop` | Show or stop the background daemon. |

## How it works

Marrow ingests source code in C++, Python, TypeScript/TSX, Rust, and Ruby using `tree-sitter`, with parallel file processing via Rayon. It constructs a unified, cross-repository dependency graph in an optimized local SQLite database (`.marrow/graph.db`, WAL mode). Instead of vector embeddings or external graph databases, Marrow answers structural questions with deterministic graph queries:

- **Impact analysis (blast radius):** recursive SQLite CTEs map the downstream impact of a proposed change across all files and repositories, with `file:line` locations on every caller row.
- **Condensed context capsules:** large function and class bodies are replaced with condensed signatures, preserving structural boundaries while minimizing token consumption.
- **Provider-neutral context packets:** `marrow context <task> --repo <repo_id> [--budget <tokens>] [--format markdown|json] [--profile local-8k|local-32k|cloud-cost-sensitive]` compiles deterministic packets with routing guidance, exact source spans, condensed neighbors, token accounting, freshness, and provenance. See [docs/context-packets.md](docs/context-packets.md).
- **Multi-repo edge resolution:** cross-repo references and import edges are resolved within a shared workspace.

All MCP tool responses are budget-capped (32 KB defaults for dependency graphs, batch exploration, and skeletons) so structural answers stay cheap to inject into an agent's context.

## Measured performance

We A/B-tested Marrow against native grep/read tooling in Claude Code on this repository (June 2026): identical structural question ("what calls `ingest_repo` and what breaks if its signature changes?"), same model (Sonnet 4.6), exact API-reported token counts. Full methodology and session IDs are in [BENCHMARK_TOKEN_COST_INVESTIGATION.md](BENCHMARK_TOKEN_COST_INVESTIGATION.md); reproduce with `tools/cc_audit.py`.

| Arm | Tool calls | Input tokens | Cost | Fact coverage |
|-----|-----------:|-------------:|-----:|:-------------:|
| Marrow (free tool choice) | 7 | 148K | $0.224 | 7/8 |
| Native grep/read | 16–18 | 368–445K | $0.34 | 4–6/8 |

With Marrow available, the agent answered with **34% lower cost, 61% fewer tool calls, and higher answer accuracy** than the native-tools baseline — one structural `analyze_impact` call replaced a multi-step grep/read hunt.

The same investigation drove output-budgeting fixes (`file:line` on all structural rows, 32 KB response caps, quieter routing notices). Re-running the identical worst-case prompt after those fixes cut its cost **59.5%** ($0.627 → $0.254) and its tool calls **81.5%** (27 → 5), with zero failed calls.

Caveats: these are small-n runs (one repository, one question per arm) measured during local development — treat them as indicative, not universal. For token-reduction claims about your own graph, run `marrow benchmark --precise-file-tokens <symbol> <repo_id>` for exact, reproducible cl100k_base counts.

## Agent integrations

`marrow integrate` uses an internal registry of MCP setup targets, in three tiers:

- **Automatic config writers** (verified merge formats, config written for you): Claude Code, Antigravity, Antigravity CLI (`agy`), Cursor, GitHub Copilot, Cline, and Zed. The Antigravity CLI writer registers Marrow in the shared `~/.gemini/config/mcp_config.json`, which the Antigravity IDE also reads.
- **Guided targets** (listed by the installer with printed setup instructions, no speculative config writes): Windsurf, Continue, Roo Code, Goose, OpenHands, OpenClaw, Codex CLI, Gemini CLI, JetBrains AI Assistant, JetBrains Junie, and LM Studio.
- **Secondary guided targets** (configuration guidance only): Kilo Code, Sourcegraph Amp, and Augment Code.

Model/runtime backends such as Ollama, llama.cpp, vLLM, SGLang, LiteLLM, Ramalama, and Docker Model Runner are not `marrow integrate` destinations — use them behind an MCP-capable agent or host that launches `marrow mcp`.

## Configuration

Marrow runs with sensible defaults; everything is tunable through environment variables (SQLite cache size, ingest parallelism, per-file size caps, capsule/impact payload limits, response budgets). See [docs/configuration.md](docs/configuration.md) for the full reference. The most commonly adjusted:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MARROW_DB_PATH` | `.marrow/graph.db` | Graph database location. |
| `MARROW_MAX_FILE_BYTES` | 2 MiB | Skip oversized (usually generated) files before parse. |
| `MARROW_INGEST_THREADS` | `min(8, max(2, cores))` | Parallel ingest workers; lower to reduce peak RAM. |
| `MARROW_IMPACT_MAX_ROWS` | `5000` | Max rows returned by `analyze_impact`. |

## Building from source

**Prerequisites:** a stable Rust toolchain (`rustup` recommended) and a working C compiler for `tree-sitter` native code:

- **macOS:** Xcode Command Line Tools (`xcode-select --install`).
- **Linux:** a C toolchain such as `build-essential` (Debian/Ubuntu) or the `gcc`/`clang` equivalents for your distro.
- **Windows:** the MSVC build tools (Visual Studio Build Tools with the "Desktop development with C++" workload) used by the default `*-pc-windows-msvc` Rust toolchain.

**Build, test, and install:**

```bash
cargo check                    # fast syntax + type check
cargo clippy -- -D warnings    # lint; must produce zero warnings
cargo test                     # full test suite
cargo install --path .         # optimized build, installed to Cargo's bin dir
```

`cargo install --path .` places the `marrow` executable in `~/.cargo/bin` (macOS/Linux) or `%USERPROFILE%\.cargo\bin` (Windows); ensure that directory is on your `PATH`.

**Quick iteration without installing:**

```bash
cargo build --release          # builds target/release/marrow(.exe)
cargo run --release -- index   # run any subcommand against the uninstalled binary
```

**Picking up a new binary:** agents launch `marrow mcp` as a fresh stdio subprocess each session, so no daemon restart is needed — reload your editor window or restart the agent session. If the dashboard daemon is running, `marrow stop` then `marrow ui` restarts it.

After re-indexing a large codebase, run `marrow maintenance` to checkpoint the WAL and reclaim space.

## Desktop app and daemon

Daemon autostart is opt-in and separate from desktop app registration:

```bash
marrow daemon install          # enable autostart
marrow daemon status
marrow daemon uninstall
```

`marrow ui-app enable` registers the desktop application entry points only; it does not enable daemon autostart. The npm package does not register desktop app entries by default. (`marrow service install` remains a one-release compatibility alias for `marrow daemon install`.)

Native package outputs are built by repository scripts and published alongside the npm tarball:

- macOS: `Marrow-{version}-aarch64-apple-darwin.dmg`, `Marrow-{version}-x86_64-apple-darwin.dmg` (`scripts/package-macos-dmg.sh`)
- Linux: `marrow_{version}_amd64.deb`, `Marrow-{version}-x86_64.AppImage` (`scripts/stage-linux-package-assets.sh`, `scripts/package-linux-appimage.sh`)
- Windows: `Marrow-{version}-x86_64-pc-windows-msvc.msi`

## License

MIT — see [LICENSE](LICENSE).
