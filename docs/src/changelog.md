# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v1.0.0)

- Promoted QAIL to 1.0.0 Stable, declaring the API complete and production-grade.
- **gRPC Connection State Machine**: Implemented concurrent reconnection protection using a connection generation counter in the Qdrant engine.
- **Webhook Scaling**: Scaled webhook concurrency limit to 512 paired with safe timeouts.
- **Connection Pool Locking**: Replaced async-wait locks with standard library `unwrap` synchronization under heavy concurrent loads.
- **Workspace Crates**: All workspace crates, internal path dependencies, and VSCode LSP extension bumped to `1.0.0`.
