# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.2)

- Idempotency replay fingerprints now include transaction/branch/prefer/result-format headers, preventing cross-context replay collisions.
- Branch overlay reads/writes are constrained to active branches, with stricter merge/create/delete behavior for missing/inactive branches.
- Branch update overlays now patch matching rows and preserve PK identity for branch-only materialization paths.
- REST branch read paths now enforce branch-admin authorization before query execution.
- Workspace crates and docs references were bumped to `0.27.2`.
