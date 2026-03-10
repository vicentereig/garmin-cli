# Release Guide

This repo publishes tagged releases to GitHub and can update a dedicated Homebrew tap automatically.

## One-Time Homebrew Setup

Do this before the first tagged release that should update Homebrew:

1. Create a new repository named `homebrew-garmin-cli` under `vicentereig`.
2. Give it a default branch (`main`) and at least one initial commit (a README is enough).
3. Create a fine-grained personal access token that has `Contents: Read and write` access to that tap repo.
4. Add that token to the `garmin-cli` repo as the `HOMEBREW_TAP_TOKEN` Actions secret.
5. If you use a different tap repo name, set the `HOMEBREW_TAP_REPOSITORY` repository variable in `garmin-cli` to `<owner>/<repo>`.

The workflow generates `Formula/garmin.rb` from the release `checksums.txt`, so the tap repo does not need a pre-existing formula file.

## Tagging a Release

```bash
git tag -a v1.0.0 -m "v1.0.0"
git push origin v1.0.0
```

Replace `v1.0.0` with the version you want to ship. Pushing the tag triggers `.github/workflows/release.yml`.

If the tag already exists and you later need to rerun release publishing (for example after adding the tap token), use the workflow's manual dispatch and pass that existing tag. The workflow checks out the requested tag and skips `cargo publish` on manual reruns.

## What the Release Workflow Does

For tags matching `v*`, the workflow:

1. Runs `cargo test --locked`.
2. Builds release binaries for macOS and Linux on `amd64` and `arm64`.
3. Packages each binary as `garmin-<os>-<arch>.tar.gz`.
4. Generates per-archive `.sha256` files and a merged `checksums.txt`.
5. Publishes the release artifacts to the GitHub Release page.
6. If `HOMEBREW_TAP_TOKEN` is configured, checks out the tap repo and rewrites `Formula/garmin.rb` from the release checksums.

## After the Workflow Finishes

1. Confirm the GitHub Release contains the tarballs and `checksums.txt`.
2. Confirm the Homebrew update job succeeded.
3. Verify installation:

```bash
brew install vicentereig/garmin-cli/garmin
garmin --help
```
