# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.3)

- Reinforced OR filter cage semantics so chained `.or_filter(...)` predicates preserve grouped `AND ( ... OR ... )` intent across execution paths.
- Added stricter parser/transpiler/encoder parity regression coverage to prevent accidental OR-to-AND behavior drift.
- Workspace crates and docs references were bumped to `0.27.3`.
