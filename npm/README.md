# @nickm-swe/marrow

Marrow is a local, deterministic Rust MCP server and CLI. The npm package is a thin wrapper that downloads the matching GitHub release archive, verifies it against `checksums.sha256`, extracts the `marrow` binary securely, and runs it from the package `dist/` directory.

```bash
npm install -g @nickm-swe/marrow@alpha
marrow mcp
```

The npm postinstall step does not register the desktop app or daemon. To add desktop app entries explicitly, run:

```bash
marrow ui-app enable
```

Daemon autostart remains separate and opt-in through `marrow daemon install`.