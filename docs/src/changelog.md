# Changelog

For the full project changelog, see the repository file:

- [`CHANGELOG.md`](https://github.com/qail-io/qail/blob/main/CHANGELOG.md)

## Current Highlights (v0.26.1)

- Patch release: fixed publish CI breakage after tenant-only cutover (`RlsContext::tenant(...)` migration in remaining examples/tests) and aligned gateway binary examples to `QWB1`.
- Tenant scope is now runtime-canonical on `tenant_id`; legacy `operator_id` compatibility aliases were removed.
- Workflow query payload runtime now enforces QAIL wire text (`QAIL-CMD/1`) and includes legacy payload detection helpers for cutover audits.
- Migration apply path is strict AST-first, with stronger hint support, post-apply verification gates, and improved backfill support for uuid/text PK cursors.
- Gateway policy evaluation no longer prematurely denies requests when later allow policies apply.
- Analyzer diagnostics are tighter, with reduced false positives from SQL comments/string literals.
- Direct SDK track is TypeScript, Swift, and Kotlin; Node.js native binding/WASM remain deferred.
