# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.26.3)

- PostgreSQL startup now requests protocol `3.2` by default, with one-shot downgrade retry to `3.0` on explicit protocol-version rejection.
- Native startup decode/handling for backend `NegotiateProtocolVersion` (`'v'`) is now implemented.
- Cancel key handling is bytes-native (`4..=256`), with public bytes-based cancel APIs for protocol 3.2 correctness.
- Legacy i32 cancel access/cancel APIs are retained as compatibility wrappers for 4-byte key behavior.
- Hardening/integration tests now cover protocol negotiation, downgrade boundaries, and startup/copy/replication harnesses under the 3.2 default.
