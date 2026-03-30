# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.27.4)

- The canonical PostgreSQL benchmark now measures qail-rs from native `Qail` ASTs versus `pgx` SQL strings instead of the earlier raw-SQL qail-rs surface.
- Aggregate query hot paths now use tighter buffer reuse and a dedicated four-column zero-copy receive path on the native DSL benchmark flow.
- Workspace crates and docs references were bumped to `0.27.4`.
