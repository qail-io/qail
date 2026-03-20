# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (Unreleased)

- Tenant terminology is standardized to `tenant_id` as the canonical scope identity.
- Gateway compatibility remains for legacy `operator_id` JWT/schema paths.
- Gateway policy evaluation was fixed to avoid premature deny outcomes when later policies allow access.
- Gateway handlers now consistently run optimized QAIL command execution paths.
- Analyzer diagnostics were tightened to reduce SQL/query false positives from comments and string literals.
- SDK direction is now direct-first for TypeScript, Swift, and Kotlin; Node.js native binding/WASM remain deferred.
