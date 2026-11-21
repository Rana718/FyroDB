# Changeset & Release Notes Directory

This directory stores versioned release changelogs for FyroDB.

## How Releases Work

When a new version tag (e.g. `v0.4.0` or `0.4.0`) is published to GitHub:

1. The **Release Workflow** (`.github/workflows/release.yml`) activates.
2. It parses the version from the tag (e.g., `v0.4.0` -> `0.4.0`).
3. It looks **only** for the matching changeset file in this directory:
   - `<version>-changeset.md` (e.g., `0.4.0-changeset.md`)
   - `<version>-charset.md`
   - `v<version>-changeset.md` (e.g., `v0.4.0-changeset.md`)
   - `<version>.md`
4. The workflow extracts **only that version's release notes** from the file and publishes the official GitHub Release with that content. Nothing else from older or unreleased changelogs is included.

## Preparing a New Release

To prepare release notes for version `X.Y.Z`:

1. Create a file named `.changeset/X.Y.Z-changeset.md` (e.g. `.changeset/0.4.1-changeset.md`).
2. Write the changelog specifically for version `X.Y.Z` in Markdown format:
   ```markdown
   # FyroDB X.Y.Z

   ### Added
   - Description of new features...

   ### Fixed
   - Description of bug fixes...

   ### Optimized
   - Description of performance improvements...
   ```
3. Commit this file to the repository.
4. When you push tag `vX.Y.Z`, GitHub Actions automatically publishes the release with these exact notes.
