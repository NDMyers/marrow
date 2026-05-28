# Contributing to Marrow

Thank you for your interest in contributing to Marrow!

## About Marrow

Marrow is a local, deterministic AST context engine in Rust. It is currently in **alpha** and maintained by a solo developer. We appreciate all contributions, but the scope and pace of merges may be limited during this phase.

## Reporting Bugs

Please use the [Bug Report](https://github.com/NDMyers/marrow/issues/new?template=bug_report.yml) issue template. Include:
- Marrow version (`marrow --version` or `cargo --version`)
- Operating system and platform
- Steps to reproduce
- Expected vs. actual behavior
- Any relevant logs or output

## Suggesting Features

Please use the [Feature Request](https://github.com/NDMyers/marrow/issues/new?template=feature_request.yml) issue template. Describe:
- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered
- Relevant context

## Development Setup

### Prerequisites
- **Rust:** Install via [rustup](https://rustup.rs/) (stable or nightly)
- **C Compiler:** Required for `tree-sitter` native bindings
  - **macOS:** Xcode Command Line Tools (`xcode-select --install`)
  - **Linux:** `gcc` / `clang` (usually pre-installed or via system package manager)
  - **Windows:** Visual Studio Build Tools or MinGW

### Building and Testing

```bash
# Check the codebase compiles
cargo check

# Run tests
cargo test

# Run linter checks
cargo clippy -- -D warnings

# Build release binary
cargo build --release
```

## Project Structure

**`src/`** — Core Rust implementation:
- `main.rs` — CLI entry point
- `lib.rs` — Public API surface
- `db.rs` — SQLite integration
- `ingestion.rs` — Tree-sitter parsing and graph ingestion
- `context.rs` — Context packet generation
- `ipc.rs` — MCP server protocol bridge
- `daemon/`, `dashboard/`, `ui_app.rs` — UI and daemon components

**`tests/`** — Integration tests for the CLI, MCP protocol, and graph queries.

**`npm/`** — Node.js installer and CLI wrapper:
- `bin/marrow.js` — Installer entry point
- `scripts/install.js` — Binary download and verification logic
- `package.json` — npm metadata and dependencies

**`scripts/`** — Build, packaging, and deployment scripts.

**`ci/`** — CI configuration and performance thresholds.

## Submitting a Pull Request

1. Fork the repository and create a feature branch: `git checkout -b my-feature`
2. Make your changes, following the existing code style.
3. Write tests or update existing tests to cover your changes.
4. Run `cargo test`, `cargo clippy -- -D warnings`, and `cargo check` to ensure all checks pass.
5. Commit with a clear message. **Signed commits are expected** (`git commit -S`; see [GitHub GPG key setup](https://docs.github.com/en/authentication/managing-commit-signature-verification) if needed).
6. Push to your fork and open a pull request against `main`.
7. Link any related issues in the PR description.
8. Update `CHANGELOG.md` with your changes (see [Keep a Changelog](https://keepachangelog.com/) format).

### Commit Message Conventions

- Use imperative mood: "Add feature" not "Added feature"
- Reference issues when applicable: "Fixes #123"
- Keep the first line ≤50 characters; body at ≤72 characters per line

## Code of Conduct

We are committed to providing a welcoming and inclusive experience for all contributors. Please review our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

By contributing to Marrow, you agree that your contributions will be licensed under the MIT License.
