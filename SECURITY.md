# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in Marrow, we appreciate responsible disclosure.

**Preferred method:** [GitHub Security Advisories](https://github.com/NDMyers/marrow/security/advisories/new) — this creates a private advisory channel visible only to maintainers.

**Fallback email:** 99777840+NDMyers@users.noreply.github.com

**Do not** open a public GitHub issue for security vulnerabilities.

We aim to acknowledge reports within 48 hours and provide an initial assessment within 7 days. Once a fix is prepared, we will publicly disclose the vulnerability with credit to the reporter (unless you request otherwise).

### GPG Signing

Release commits and tags are signed with the maintainer's GPG key. Public keys are available on the [maintainer's GitHub profile](https://github.com/NDMyers). You can verify signatures with:

```bash
git verify-commit <commit-hash>
git verify-tag <tag>
```

## Binary Verification

When installing Marrow via npm, the installer automatically verifies binary integrity:

1. **Checksum verification**: The installer fetches `checksums.sha256` from the GitHub release and verifies the archive SHA256 hash before extraction.
2. **Secure extraction**: The tar extraction rejects path traversal attempts, symlinks, and hardlinks.

The release workflow fails closed before npm publish unless every installer-required archive is present in the GitHub release and listed in `checksums.sha256`. Linux AppImage packaging downloads the pinned `appimagetool` release and verifies its SHA256 before use.

### Manual Verification

To manually verify a downloaded binary:

```bash
# Download the checksums file
curl -LO https://github.com/NDMyers/marrow/releases/download/vX.Y.Z/checksums.sha256

# Verify the archive
shasum -a 256 -c checksums.sha256 --ignore-missing
```

## Security Practices

- **Supply chain**: Release binaries include SHA256 checksums; npm installer verifies before extraction.
- **Release gating**: npm publishing runs only after release assets and checksums are verified.
- **Dependencies**: Cargo.lock is tracked for reproducible builds; Dependabot monitors for advisories.
- **CI gates**: `cargo audit` and `npm audit` run on every PR; failures block merge.
- **Socket permissions**: Unix daemon sockets use restrictive permissions (directory 0700, socket 0600).
- **Path validation**: Watch registration validates paths against workspace roots.
