# Public Release Checklist

Use this checklist before publishing an alpha release.

- Confirm CI is green on `main` (the release workflow compiles on the same runners; a red CI means a red release).
- Confirm `Cargo.toml`, root `LICENSE`, npm metadata, and npm package `README.md`/`LICENSE` are current.
- Bump versions in lockstep: the git tag (`vX.Y.Z`), `Cargo.toml`, and `npm/package.json` must all agree — the `create-release` job fails the release otherwise. Update the `CHANGELOG.md` release date at the same time.
- Create a version tag and let the release workflow build GitHub release binaries and native packages.
- Verify the workflow generated `checksums.sha256` after all installer-required archives were uploaded (all four `marrow-<target>.tar.gz`, including Windows).
- Confirm the npm publish job completed `npm audit`, `npm pack --dry-run --json`, and publish dry-run before the alpha publish step.
- Install the npm package in a clean environment with `npm install -g @nickm-swe/marrow` and run `marrow --help`; run `marrow ui-app enable` only when desktop registration is desired.
- Confirm the publish job moved the `latest` dist-tag to the new version (`npm view @nickm-swe/marrow dist-tags`). The website reads `latest` from the registry and shows the new version within an hour — no website change or deploy is needed for a release.