# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.10)

- LSP schema/completion/diagnostic state handling was hardened for workspace switching, `schema.qail` edits, and out-of-order document updates.
- LSP diagnostic/hover range handling now consistently respects UTF-16 cursor positioning, including Windows-path validation messages and non-ASCII query literals.
- Workspace crates and docs references were bumped to `0.27.10`.
