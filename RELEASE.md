# Public Release Checklist

Use this checklist before publishing an alpha release.

- Confirm `Cargo.toml`, root `LICENSE`, npm metadata, and npm package `README.md`/`LICENSE` are current.
- Create a version tag and let the release workflow build GitHub release binaries and native packages.
- Verify the workflow generated `checksums.sha256` after all installer-required archives were uploaded.
- Confirm the npm publish job completed `npm audit`, `npm pack --dry-run --json`, and publish dry-run before the alpha publish step.
- Install the npm package in a clean environment and run `marrow --help`; run `marrow ui-app enable` only when desktop registration is desired.