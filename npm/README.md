# @nickm-swe/marrow

Marrow is a local, deterministic Rust MCP server and CLI. It parses your codebase with `tree-sitter`, builds a cross-repository dependency graph in a local SQLite database, and serves deterministic structural context — callers, blast radius, condensed code capsules — to AI coding agents. No embeddings, no provider SDKs, no network calls.

This npm package is a thin installer: it downloads the matching GitHub release archive for your platform, verifies it against `checksums.sha256`, extracts the `marrow` binary securely, and runs it from the package `dist/` directory.

## Install

```bash
npm install -g @nickm-swe/marrow
marrow mcp
```

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
| `marrow index` | Index the current workspace. |
| `marrow context <task>` | Compile a provider-neutral context packet (markdown/JSON). |
| `marrow query <symbol> <repo_id>` | Print a symbol's context capsule plus impact analysis. |
| `marrow integrate` | Write/print MCP setup for supported agent targets. |
| `marrow ui` / `marrow ui-app` | Open the dashboard / manage the desktop app entry. |
| `marrow daemon [install\|uninstall\|status]` | Background daemon and autostart management. |

The npm postinstall step does not register the desktop app or daemon. To add desktop app entries explicitly, run:

```bash
marrow ui-app enable
```

Daemon autostart remains separate and opt-in through `marrow daemon install`.

## Learn more

- Full README, architecture, and measured benchmarks: [github.com/NDMyers/marrow](https://github.com/NDMyers/marrow)
- Context packet reference: [docs/context-packets.md](https://github.com/NDMyers/marrow/blob/main/docs/context-packets.md)
- Environment variable / configuration reference: [docs/configuration.md](https://github.com/NDMyers/marrow/blob/main/docs/configuration.md)
- Changelog: [CHANGELOG.md](https://github.com/NDMyers/marrow/blob/main/CHANGELOG.md)
- Issues: [github.com/NDMyers/marrow/issues](https://github.com/NDMyers/marrow/issues)

## License

MIT — see [LICENSE](https://github.com/NDMyers/marrow/blob/main/LICENSE).
